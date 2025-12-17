// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use api::region::RegionResponse;
use async_trait::async_trait;
use common_catalog::consts::FILE_ENGINE;
use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{error, info};
use object_store::ObjectStore;
use snafu::{OptionExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    CopyRegionFromRequest, CopyRegionFromResponse, RegionEngine, RegionManifestInfo, RegionRole,
    RegionScannerRef, RegionStatistic, RemapManifestsRequest, RemapManifestsResponse,
    SetRegionRoleStateResponse, SetRegionRoleStateSuccess, SettableRegionRoleState,
    SinglePartitionScanner, SyncManifestResponse,
};
use store_api::region_request::{
    AffectedRows, RegionCloseRequest, RegionCreateRequest, RegionDropRequest, RegionOpenRequest,
    RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest, SequenceNumber};
use tokio::sync::Mutex;

use crate::config::EngineConfig;
use crate::error::{
    RegionNotFoundSnafu, Result as EngineResult, UnexpectedEngineSnafu, UnsupportedSnafu,
};
use crate::region::{FileRegion, FileRegionRef};

pub struct FileRegionEngine {
    inner: EngineInnerRef,
}

impl FileRegionEngine {
    pub fn new(_config: EngineConfig, object_store: ObjectStore) -> Self {
        Self {
            inner: Arc::new(EngineInner::new(object_store)),
        }
    }

    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        self.inner
            .get_region(region_id)
            .await
            .context(RegionNotFoundSnafu { region_id })
            .map_err(BoxedError::new)?
            .query(request)
            .map_err(BoxedError::new)
    }
}

#[async_trait]
impl RegionEngine for FileRegionEngine {
    fn name(&self) -> &str {
        FILE_ENGINE
    }

    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<RegionResponse, BoxedError> {
        self.inner
            .handle_request(region_id, request)
            .await
            .map_err(BoxedError::new)
    }

    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef, BoxedError> {
        let stream = self.handle_query(region_id, request).await?;
        let metadata = self.get_metadata(region_id).await?;
        // We don't support enabling append mode for file engine.
        let scanner = Box::new(SinglePartitionScanner::new(stream, false, metadata));
        Ok(scanner)
    }

    async fn get_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef, BoxedError> {
        self.inner
            .get_region(region_id)
            .await
            .map(|r| r.metadata())
            .context(RegionNotFoundSnafu { region_id })
            .map_err(BoxedError::new)
    }

    async fn stop(&self) -> Result<(), BoxedError> {
        self.inner.stop().await.map_err(BoxedError::new)
    }

    fn region_statistic(&self, _: RegionId) -> Option<RegionStatistic> {
        None
    }

    async fn get_committed_sequence(&self, _: RegionId) -> Result<SequenceNumber, BoxedError> {
        Ok(Default::default())
    }

    fn set_region_role(&self, region_id: RegionId, role: RegionRole) -> Result<(), BoxedError> {
        self.inner
            .set_region_role(region_id, role)
            .map_err(BoxedError::new)
    }

    async fn set_region_role_state_gracefully(
        &self,
        region_id: RegionId,
        _region_role_state: SettableRegionRoleState,
    ) -> Result<SetRegionRoleStateResponse, BoxedError> {
        let exists = self.inner.get_region(region_id).await.is_some();

        if exists {
            Ok(SetRegionRoleStateResponse::success(
                SetRegionRoleStateSuccess::file(),
            ))
        } else {
            Ok(SetRegionRoleStateResponse::NotFound)
        }
    }

    async fn sync_region(
        &self,
        _region_id: RegionId,
        _manifest_info: RegionManifestInfo,
    ) -> Result<SyncManifestResponse, BoxedError> {
        // File engine doesn't need to sync region manifest.
        Ok(SyncManifestResponse::NotSupported)
    }

    async fn remap_manifests(
        &self,
        _request: RemapManifestsRequest,
    ) -> Result<RemapManifestsResponse, BoxedError> {
        Err(BoxedError::new(
            UnsupportedSnafu {
                operation: "remap_manifests",
            }
            .build(),
        ))
    }

    async fn copy_region_from(
        &self,
        _region_id: RegionId,
        _request: CopyRegionFromRequest,
    ) -> Result<CopyRegionFromResponse, BoxedError> {
        Err(BoxedError::new(
            UnsupportedSnafu {
                operation: "copy_region_from",
            }
            .build(),
        ))
    }

    fn role(&self, region_id: RegionId) -> Option<RegionRole> {
        self.inner.state(region_id)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct EngineInner {
    /// All regions opened by the engine.
    ///
    /// Writing to `regions` should also hold the `region_mutex`.
    regions: RwLock<HashMap<RegionId, FileRegionRef>>,

    /// Region mutex is used to protect the operations such as creating/opening/closing
    /// a region, to avoid things like opening the same region simultaneously.
    region_mutex: Mutex<()>,

    object_store: ObjectStore,
}

type EngineInnerRef = Arc<EngineInner>;

impl EngineInner {
    fn new(object_store: ObjectStore) -> Self {
        Self {
            regions: RwLock::new(HashMap::new()),
            region_mutex: Mutex::new(()),
            object_store,
        }
    }

    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> EngineResult<RegionResponse> {
        let result = match request {
            RegionRequest::Create(req) => self.handle_create(region_id, req).await,
            RegionRequest::Drop(req) => self.handle_drop(region_id, req).await,
            RegionRequest::Open(req) => self.handle_open(region_id, req).await,
            RegionRequest::Close(req) => self.handle_close(region_id, req).await,
            _ => UnsupportedSnafu {
                operation: request.to_string(),
            }
            .fail(),
        };
        result.map(RegionResponse::new)
    }

    async fn stop(&self) -> EngineResult<()> {
        let _lock = self.region_mutex.lock().await;
        self.regions.write().unwrap().clear();
        Ok(())
    }

    fn set_region_role(&self, _region_id: RegionId, _region_role: RegionRole) -> EngineResult<()> {
        // TODO(zhongzc): Improve the semantics and implementation of this API.
        Ok(())
    }

    fn state(&self, region_id: RegionId) -> Option<RegionRole> {
        if self.regions.read().unwrap().get(&region_id).is_some() {
            Some(RegionRole::Leader)
        } else {
            None
        }
    }
}

impl EngineInner {
    async fn handle_create(
        &self,
        region_id: RegionId,
        request: RegionCreateRequest,
    ) -> EngineResult<AffectedRows> {
        ensure!(
            request.engine == FILE_ENGINE,
            UnexpectedEngineSnafu {
                engine: request.engine
            }
        );

        if self.exists(region_id).await {
            return Ok(0);
        }

        info!("Try to create region, region_id: {}", region_id);

        let _lock = self.region_mutex.lock().await;
        // Check again after acquiring the lock
        if self.exists(region_id).await {
            return Ok(0);
        }

        let res = FileRegion::create(region_id, request, &self.object_store).await;
        let region = res.inspect_err(|err| {
            error!(
                err;
                "Failed to create region, region_id: {}",
                region_id
            );
        })?;
        self.regions.write().unwrap().insert(region_id, region);

        info!("A new region is created, region_id: {}", region_id);
        Ok(0)
    }

    async fn handle_open(
        &self,
        region_id: RegionId,
        request: RegionOpenRequest,
    ) -> EngineResult<AffectedRows> {
        if self.exists(region_id).await {
            return Ok(0);
        }

        info!("Try to open region, region_id: {}", region_id);

        let _lock = self.region_mutex.lock().await;
        // Check again after acquiring the lock
        if self.exists(region_id).await {
            return Ok(0);
        }

        let res = FileRegion::open(region_id, request, &self.object_store).await;
        let region = res.inspect_err(|err| {
            error!(
                err;
                "Failed to open region, region_id: {}",
                region_id
            );
        })?;
        self.regions.write().unwrap().insert(region_id, region);

        info!("Region opened, region_id: {}", region_id);
        Ok(0)
    }

    async fn handle_close(
        &self,
        region_id: RegionId,
        _request: RegionCloseRequest,
    ) -> EngineResult<AffectedRows> {
        let _lock = self.region_mutex.lock().await;

        let mut regions = self.regions.write().unwrap();
        if regions.remove(&region_id).is_some() {
            info!("Region closed, region_id: {}", region_id);
        }

        Ok(0)
    }

    async fn handle_drop(
        &self,
        region_id: RegionId,
        _request: RegionDropRequest,
    ) -> EngineResult<AffectedRows> {
        if !self.exists(region_id).await {
            return RegionNotFoundSnafu { region_id }.fail();
        }

        info!("Try to drop region, region_id: {}", region_id);

        let _lock = self.region_mutex.lock().await;

        let region = self.get_region(region_id).await;
        if let Some(region) = region {
            let res = FileRegion::drop(&region, &self.object_store).await;
            res.inspect_err(|err| {
                error!(
                    err;
                    "Failed to drop region, region_id: {}",
                    region_id
                );
            })?;
        }
        let _ = self.regions.write().unwrap().remove(&region_id);

        info!("Region dropped, region_id: {}", region_id);
        Ok(0)
    }

    async fn get_region(&self, region_id: RegionId) -> Option<FileRegionRef> {
        self.regions.read().unwrap().get(&region_id).cloned()
    }

    async fn exists(&self, region_id: RegionId) -> bool {
        self.regions.read().unwrap().contains_key(&region_id)
    }
}
