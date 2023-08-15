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

// TODO: migrate test to RegionRequest
// #[cfg(test)]
// mod tests;

use std::sync::Arc;

use common_query::Output;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::region_request::RegionRequest;
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::error::{RecvSnafu, Result};
use crate::request::RegionTask;
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

    pub async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<Output> {
        self.inner.handle_request(region_id, request).await?;
        Ok(Output::AffectedRows(0))
    }

    /// Returns true if the specific region exists.
    pub fn is_region_exists(&self, region_id: RegionId) -> bool {
        self.inner.workers.is_region_exists(region_id)
    }

    // /// Write to a region.
    // pub async fn write_region(&self, write_request: WriteRequest) -> Result<()> {
    // write_request.validate()?;
    // RequestValidator::write_request(&write_request)?;

    // TODO(yingwen): Fill default values.
    // We need to fill default values before writing it to WAL so we can get
    // the same default value after reopening the region.

    // let metadata = region.metadata();

    // write_request.fill_missing_columns(&metadata)?;
    //     self.inner
    //         .handle_request_body(RequestBody::Write(write_request))
    //         .await
    // }
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

    // TODO(yingwen): return `Output` instead of `Result<()>`.
    /// Handles [RequestBody] and return its executed result.
    async fn handle_request(&self, region_id: RegionId, request: RegionRequest) -> Result<()> {
        let (request, receiver) = RegionTask::from_request(region_id, request);
        self.workers.submit_to_worker(request).await?;

        receiver.await.context(RecvSnafu)?
    }
}
