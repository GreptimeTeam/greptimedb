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

//! Mito region engine.

#[cfg(test)]
mod close_test;
#[cfg(test)]
mod create_test;
#[cfg(test)]
mod drop_test;
#[cfg(test)]
mod flush_test;
#[cfg(test)]
pub(crate) mod listener;
#[cfg(test)]
mod open_test;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_query::Output;
use common_recordbatch::SendableRecordBatchStream;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::error::{RecvSnafu, RegionNotFoundSnafu, Result};
use crate::read::scan_region::{ScanRegion, Scanner};
use crate::request::WorkerRequest;
use crate::worker::WorkerGroup;

/// Region engine implementation for timeseries data.
#[derive(Clone)]
pub struct MitoEngine {
    inner: Arc<EngineInner>,
}

impl MitoEngine {
    /// Returns a new [MitoEngine] with specific `config`, `log_store` and `object_store`.
    pub fn new<S: LogStore>(
        mut config: MitoConfig,
        log_store: Arc<S>,
        object_store: ObjectStore,
    ) -> MitoEngine {
        config.sanitize();

        MitoEngine {
            inner: Arc::new(EngineInner::new(config, log_store, object_store)),
        }
    }

    /// Stop the engine.
    ///
    /// Stopping the engine doesn't stop the underlying log store as other components might
    /// still use it.
    pub async fn stop(&self) -> Result<()> {
        self.inner.stop().await
    }

    /// Returns true if the specific region exists.
    pub fn is_region_exists(&self, region_id: RegionId) -> bool {
        self.inner.workers.is_region_exists(region_id)
    }

    fn scan(&self, region_id: RegionId, request: ScanRequest) -> Result<Scanner> {
        self.inner.handle_query(region_id, request)
    }

    #[cfg(test)]
    pub(crate) fn get_region(&self, id: RegionId) -> Option<crate::region::MitoRegionRef> {
        self.inner.workers.get_region(id)
    }
}

/// Inner struct of [MitoEngine].
struct EngineInner {
    /// Region workers group.
    workers: WorkerGroup,
}

impl EngineInner {
    /// Returns a new [EngineInner] with specific `config`, `log_store` and `object_store`.
    fn new<S: LogStore>(
        config: MitoConfig,
        log_store: Arc<S>,
        object_store: ObjectStore,
    ) -> EngineInner {
        EngineInner {
            workers: WorkerGroup::start(config, log_store, object_store),
        }
    }

    /// Stop the inner engine.
    async fn stop(&self) -> Result<()> {
        self.workers.stop().await
    }

    /// Get metadata of a region.
    ///
    /// Returns error if the region doesn't exist.
    fn get_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef> {
        // Reading a region doesn't need to go through the region worker thread.
        let region = self
            .workers
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        Ok(region.metadata())
    }

    /// Handles [RegionRequest] and return its executed result.
    async fn handle_request(&self, region_id: RegionId, request: RegionRequest) -> Result<Output> {
        let (request, receiver) = WorkerRequest::try_from_region_request(region_id, request)?;
        self.workers.submit_to_worker(region_id, request).await?;

        receiver.await.context(RecvSnafu)?
    }

    /// Handles the scan `request` and returns a [Scanner] for the `request`.
    fn handle_query(&self, region_id: RegionId, request: ScanRequest) -> Result<Scanner> {
        // Reading a region doesn't need to go through the region worker thread.
        let region = self
            .workers
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        let version = region.version();
        let scan_region = ScanRegion::new(version, region.access_layer.clone(), request);

        scan_region.scanner()
    }
}

#[async_trait]
impl RegionEngine for MitoEngine {
    fn name(&self) -> &str {
        "mito"
    }

    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> std::result::Result<Output, BoxedError> {
        self.inner
            .handle_request(region_id, request)
            .await
            .map_err(BoxedError::new)
    }

    /// Handle substrait query and return a stream of record batches
    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        self.scan(region_id, request)
            .map_err(BoxedError::new)?
            .scan()
            .await
            .map_err(BoxedError::new)
    }

    /// Retrieve region's metadata.
    async fn get_metadata(
        &self,
        region_id: RegionId,
    ) -> std::result::Result<RegionMetadataRef, BoxedError> {
        self.inner.get_metadata(region_id).map_err(BoxedError::new)
    }
}

// Tests methods.
#[cfg(test)]
impl MitoEngine {
    /// Returns a new [MitoEngine] for tests.
    pub fn new_for_test<S: LogStore>(
        mut config: MitoConfig,
        log_store: Arc<S>,
        object_store: ObjectStore,
        write_buffer_manager: crate::flush::WriteBufferManagerRef,
        listener: Option<crate::engine::listener::EventListenerRef>,
    ) -> MitoEngine {
        config.sanitize();

        MitoEngine {
            inner: Arc::new(EngineInner {
                workers: WorkerGroup::start_for_test(
                    config,
                    log_store,
                    object_store,
                    write_buffer_manager,
                    listener,
                ),
            }),
        }
    }
}
