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
mod tests;

use std::sync::Arc;

use common_query::Output;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::LogStore;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::error::{RecvSnafu, RegionNotFoundSnafu, Result};
use crate::read::scan_region::ScanRegion;
use crate::read::Scanner;
use crate::request::{RegionTask, RequestBody};
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

    /// Handle requests that modify a region.
    pub async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<Output> {
        self.inner.handle_request(region_id, request).await
    }

    /// Returns true if the specific region exists.
    pub fn is_region_exists(&self, region_id: RegionId) -> bool {
        self.inner.workers.is_region_exists(region_id)
    }

    /// Handles the scan `request` and returns a [Scanner] for the `request`.
    fn handle_query(&self, region_id: RegionId, request: ScanRequest) -> Result<Scanner> {
        self.inner.handle_query(region_id, request)
    }
}

/// Inner struct of [MitoEngine].
struct EngineInner {
    /// Region workers group.
    workers: WorkerGroup,
    /// Shared object store of all regions.
    object_store: ObjectStore,
}

impl EngineInner {
    /// Returns a new [EngineInner] with specific `config`, `log_store` and `object_store`.
    fn new<S: LogStore>(
        config: MitoConfig,
        log_store: Arc<S>,
        object_store: ObjectStore,
    ) -> EngineInner {
        EngineInner {
            workers: WorkerGroup::start(config, log_store, object_store.clone()),
            object_store,
        }
    }

    /// Stop the inner engine.
    async fn stop(&self) -> Result<()> {
        self.workers.stop().await
    }

    /// Handles [RequestBody] and return its executed result.
    async fn handle_request(&self, region_id: RegionId, request: RegionRequest) -> Result<Output> {
        // We validate and then convert the `request` into an inner `RequestBody` for ease of handling.
        let body = RequestBody::try_from_region_request(region_id, request)?;
        let (request, receiver) = RegionTask::from_request(region_id, body);
        self.workers.submit_to_worker(request).await?;

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
        let scan_region = ScanRegion::new(
            version,
            region.region_dir.clone(),
            self.object_store.clone(),
            request,
        );

        scan_region.scanner()
    }
}
