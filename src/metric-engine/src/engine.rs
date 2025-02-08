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

mod alter;
mod catchup;
mod close;
mod create;
mod drop;
mod flush;
mod open;
mod options;
mod put;
mod read;
mod region_metadata;
mod state;

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use api::region::RegionResponse;
use async_trait::async_trait;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use mito2::engine::MitoEngine;
pub(crate) use options::IndexOptions;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::METRIC_ENGINE_NAME;
use store_api::region_engine::{
    RegionEngine, RegionRole, RegionScannerRef, RegionStatistic, SetRegionRoleStateResponse,
    SettableRegionRoleState,
};
use store_api::region_request::{BatchRegionDdlRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use self::state::MetricEngineState;
use crate::config::EngineConfig;
use crate::data_region::DataRegion;
use crate::error::{self, Result, UnsupportedRegionRequestSnafu};
use crate::metadata_region::MetadataRegion;
use crate::row_modifier::RowModifier;
use crate::utils;

#[cfg_attr(doc, aquamarine::aquamarine)]
/// # Metric Engine
///
/// ## Regions
///
/// Regions in this metric engine has several roles. There is `PhysicalRegion`,
/// which refer to the region that actually stores the data. And `LogicalRegion`
/// that is "simulated" over physical regions. Each logical region is associated
/// with one physical region group, which is a group of two physical regions.
/// Their relationship is illustrated below:
///
/// ```mermaid
/// erDiagram
///     LogicalRegion ||--o{ PhysicalRegionGroup : corresponds
///     PhysicalRegionGroup ||--|| DataRegion : contains
///     PhysicalRegionGroup ||--|| MetadataRegion : contains
/// ```
///
/// Metric engine uses two region groups. One is for data region
/// ([METRIC_DATA_REGION_GROUP](crate::consts::METRIC_DATA_REGION_GROUP)), and the
/// other is for metadata region ([METRIC_METADATA_REGION_GROUP](crate::consts::METRIC_METADATA_REGION_GROUP)).
/// From the definition of [`RegionId`], we can convert between these two physical
/// region ids easily. Thus in the code base we usually refer to one "physical
/// region id", and convert it to the other one when necessary.
///
/// The logical region, in contrast, is a virtual region. It doesn't has dedicated
/// storage or region group. Only a region id that is allocated by meta server.
/// And all other things is shared with other logical region that are associated
/// with the same physical region group.
///
/// For more document about physical regions, please refer to [`MetadataRegion`]
/// and [`DataRegion`].
///
/// ## Operations
///
/// Both physical and logical region are accessible to user. But the operation
/// they support are different. List below:
///
/// | Operations | Logical Region | Physical Region |
/// | ---------- | -------------- | --------------- |
/// |   Create   |       ✅        |        ✅        |
/// |    Drop    |       ✅        |        ❓*       |
/// |   Write    |       ✅        |        ❌        |
/// |    Read    |       ✅        |        ✅        |
/// |   Close    |       ✅        |        ✅        |
/// |    Open    |       ✅        |        ✅        |
/// |   Alter    |       ✅        |        ❓*       |
///
/// *: Physical region can be dropped only when all related logical regions are dropped.
/// *: Alter: Physical regions only support altering region options.
///
/// ## Internal Columns
///
/// The physical data region contains two internal columns. Should
/// mention that "internal" here is for metric engine itself. Mito
/// engine will add it's internal columns to the region as well.
///
/// Their column id is registered in [`ReservedColumnId`]. And column name is
/// defined in [`DATA_SCHEMA_TSID_COLUMN_NAME`] and [`DATA_SCHEMA_TABLE_ID_COLUMN_NAME`].
///
/// Tsid is generated by hashing all tags. And table id is retrieved from logical region
/// id to distinguish data from different logical tables.
#[derive(Clone)]
pub struct MetricEngine {
    inner: Arc<MetricEngineInner>,
}

#[async_trait]
impl RegionEngine for MetricEngine {
    /// Name of this engine
    fn name(&self) -> &str {
        METRIC_ENGINE_NAME
    }

    async fn handle_batch_ddl_requests(
        &self,
        batch_request: BatchRegionDdlRequest,
    ) -> Result<RegionResponse, BoxedError> {
        match batch_request {
            BatchRegionDdlRequest::Create(requests) => {
                let mut extension_return_value = HashMap::new();
                let rows = self
                    .inner
                    .create_regions(requests, &mut extension_return_value)
                    .await
                    .map_err(BoxedError::new)?;

                Ok(RegionResponse {
                    affected_rows: rows,
                    extensions: extension_return_value,
                })
            }
            BatchRegionDdlRequest::Alter(requests) => {
                self.handle_requests(
                    requests
                        .into_iter()
                        .map(|(region_id, req)| (region_id, RegionRequest::Alter(req))),
                )
                .await
            }
            BatchRegionDdlRequest::Drop(requests) => {
                self.handle_requests(
                    requests
                        .into_iter()
                        .map(|(region_id, req)| (region_id, RegionRequest::Drop(req))),
                )
                .await
            }
        }
    }

    /// Handles non-query request to the region. Returns the count of affected rows.
    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<RegionResponse, BoxedError> {
        let mut extension_return_value = HashMap::new();

        let result = match request {
            RegionRequest::Put(put) => self.inner.put_region(region_id, put).await,
            RegionRequest::Create(create) => {
                self.inner
                    .create_region(region_id, create, &mut extension_return_value)
                    .await
            }
            RegionRequest::Drop(drop) => self.inner.drop_region(region_id, drop).await,
            RegionRequest::Open(open) => self.inner.open_region(region_id, open).await,
            RegionRequest::Close(close) => self.inner.close_region(region_id, close).await,
            RegionRequest::Alter(alter) => {
                self.inner
                    .alter_region(region_id, alter, &mut extension_return_value)
                    .await
            }
            RegionRequest::Compact(_) => {
                if self.inner.is_physical_region(region_id) {
                    self.inner
                        .mito
                        .handle_request(region_id, request)
                        .await
                        .context(error::MitoFlushOperationSnafu)
                        .map(|response| response.affected_rows)
                } else {
                    UnsupportedRegionRequestSnafu { request }.fail()
                }
            }
            RegionRequest::Flush(req) => self.inner.flush_region(region_id, req).await,
            RegionRequest::Truncate(_) => UnsupportedRegionRequestSnafu { request }.fail(),
            RegionRequest::Delete(_) => {
                if self.inner.is_physical_region(region_id) {
                    self.inner
                        .mito
                        .handle_request(region_id, request)
                        .await
                        .context(error::MitoDeleteOperationSnafu)
                        .map(|response| response.affected_rows)
                } else {
                    UnsupportedRegionRequestSnafu { request }.fail()
                }
            }
            RegionRequest::Catchup(req) => self.inner.catchup_region(region_id, req).await,
        };

        result.map_err(BoxedError::new).map(|rows| RegionResponse {
            affected_rows: rows,
            extensions: extension_return_value,
        })
    }

    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef, BoxedError> {
        self.handle_query(region_id, request).await
    }

    /// Retrieves region's metadata.
    async fn get_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef, BoxedError> {
        self.inner
            .load_region_metadata(region_id)
            .await
            .map_err(BoxedError::new)
    }

    /// Retrieves region's disk usage.
    ///
    /// Note: Returns `None` if it's a logical region.
    fn region_statistic(&self, region_id: RegionId) -> Option<RegionStatistic> {
        if self.inner.is_physical_region(region_id) {
            self.inner.mito.region_statistic(region_id)
        } else {
            None
        }
    }

    /// Stops the engine
    async fn stop(&self) -> Result<(), BoxedError> {
        // don't need to stop the underlying mito engine
        Ok(())
    }

    fn set_region_role(&self, region_id: RegionId, role: RegionRole) -> Result<(), BoxedError> {
        // ignore the region not found error
        for x in [
            utils::to_metadata_region_id(region_id),
            utils::to_data_region_id(region_id),
        ] {
            if let Err(e) = self.inner.mito.set_region_role(x, role)
                && e.status_code() != StatusCode::RegionNotFound
            {
                return Err(e);
            }
        }
        Ok(())
    }

    async fn set_region_role_state_gracefully(
        &self,
        region_id: RegionId,
        region_role_state: SettableRegionRoleState,
    ) -> std::result::Result<SetRegionRoleStateResponse, BoxedError> {
        self.inner
            .mito
            .set_region_role_state_gracefully(
                utils::to_metadata_region_id(region_id),
                region_role_state,
            )
            .await?;
        self.inner
            .mito
            .set_region_role_state_gracefully(region_id, region_role_state)
            .await
    }

    /// Returns the physical region role.
    ///
    /// Note: Returns `None` if it's a logical region.
    fn role(&self, region_id: RegionId) -> Option<RegionRole> {
        if self.inner.is_physical_region(region_id) {
            self.inner.mito.role(region_id)
        } else {
            None
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MetricEngine {
    pub fn new(mito: MitoEngine, config: EngineConfig) -> Self {
        let metadata_region = MetadataRegion::new(mito.clone());
        let data_region = DataRegion::new(mito.clone());
        Self {
            inner: Arc::new(MetricEngineInner {
                mito,
                metadata_region,
                data_region,
                state: RwLock::default(),
                config,
                row_modifier: RowModifier::new(),
            }),
        }
    }

    pub async fn logical_regions(&self, physical_region_id: RegionId) -> Result<Vec<RegionId>> {
        self.inner
            .metadata_region
            .logical_regions(physical_region_id)
            .await
    }

    /// Handles substrait query and return a stream of record batches
    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef, BoxedError> {
        self.inner
            .read_region(region_id, request)
            .await
            .map_err(BoxedError::new)
    }

    async fn handle_requests(
        &self,
        requests: impl IntoIterator<Item = (RegionId, RegionRequest)>,
    ) -> Result<RegionResponse, BoxedError> {
        let mut affected_rows = 0;
        let mut extensions = HashMap::new();
        for (region_id, request) in requests {
            let response = self.handle_request(region_id, request).await?;
            affected_rows += response.affected_rows;
            extensions.extend(response.extensions);
        }

        Ok(RegionResponse {
            affected_rows,
            extensions,
        })
    }
}

#[cfg(test)]
impl MetricEngine {
    pub async fn scan_to_stream(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<common_recordbatch::SendableRecordBatchStream, BoxedError> {
        self.inner.scan_to_stream(region_id, request).await
    }
}

struct MetricEngineInner {
    mito: MitoEngine,
    metadata_region: MetadataRegion,
    data_region: DataRegion,
    state: RwLock<MetricEngineState>,
    config: EngineConfig,
    row_modifier: RowModifier,
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use store_api::metric_engine_consts::PHYSICAL_TABLE_METADATA_KEY;
    use store_api::region_request::{RegionCloseRequest, RegionOpenRequest};

    use super::*;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn close_open_regions() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        // close physical region
        let physical_region_id = env.default_physical_region_id();
        engine
            .handle_request(
                physical_region_id,
                RegionRequest::Close(RegionCloseRequest {}),
            )
            .await
            .unwrap();

        // reopen physical region
        let physical_region_option = [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
            .into_iter()
            .collect();
        let open_request = RegionOpenRequest {
            engine: METRIC_ENGINE_NAME.to_string(),
            region_dir: env.default_region_dir(),
            options: physical_region_option,
            skip_wal_replay: false,
        };
        engine
            .handle_request(physical_region_id, RegionRequest::Open(open_request))
            .await
            .unwrap();

        // close nonexistent region won't report error
        let nonexistent_region_id = RegionId::new(12313, 12);
        engine
            .handle_request(
                nonexistent_region_id,
                RegionRequest::Close(RegionCloseRequest {}),
            )
            .await
            .unwrap();

        // open nonexistent region won't report error
        let invalid_open_request = RegionOpenRequest {
            engine: METRIC_ENGINE_NAME.to_string(),
            region_dir: env.default_region_dir(),
            options: HashMap::new(),
            skip_wal_replay: false,
        };
        engine
            .handle_request(
                nonexistent_region_id,
                RegionRequest::Open(invalid_open_request),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_role() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        let logical_region_id = env.default_logical_region_id();
        let physical_region_id = env.default_physical_region_id();

        assert!(env.metric().role(logical_region_id).is_none());
        assert!(env.metric().role(physical_region_id).is_some());
    }

    #[tokio::test]
    async fn test_region_disk_usage() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        let logical_region_id = env.default_logical_region_id();
        let physical_region_id = env.default_physical_region_id();

        assert!(env.metric().region_statistic(logical_region_id).is_none());
        assert!(env.metric().region_statistic(physical_region_id).is_some());
    }
}
