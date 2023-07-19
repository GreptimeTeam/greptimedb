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

use std::sync::Arc;

use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::logstore::LogStore;

use crate::config::MitoConfig;
use crate::error::{RecvSnafu, Result};
pub use crate::worker::request::CreateRequest;
use crate::worker::request::{RegionRequest, RequestBody};
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
    pub async fn stop(&self) -> Result<()> {
        self.inner.stop().await
    }

    /// Creates a new region.
    pub async fn create_region(&self, request: CreateRequest) -> Result<()> {
        self.inner.create_region(request).await
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

    /// Creates a new region.
    async fn create_region(&self, create_request: CreateRequest) -> Result<()> {
        let (request, receiver) = RegionRequest::from_body(RequestBody::Create(create_request));
        self.workers.submit_to_worker(request).await?;

        receiver.await.context(RecvSnafu)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn test_engine_new_stop() {
        let env = TestEnv::new("engine-stop");
        let engine = env.create_engine(MitoConfig::default()).await;

        engine.stop().await.unwrap();
    }
}
