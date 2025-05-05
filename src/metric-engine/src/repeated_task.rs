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

use std::sync::{Arc, RwLock};
use std::time::Instant;

use common_runtime::TaskFunction;
use common_telemetry::{debug, error};
use mito2::engine::MitoEngine;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{RegionFlushRequest, RegionRequest};

use crate::engine::MetricEngineState;
use crate::error::{Error, Result};
use crate::utils;

/// Task to flush metadata regions.
///
/// This task is used to send flush requests to the metadata regions
/// periodically.
pub(crate) struct FlushMetadataRegionTask {
    pub(crate) state: Arc<RwLock<MetricEngineState>>,
    pub(crate) mito: MitoEngine,
}

#[async_trait::async_trait]
impl TaskFunction<Error> for FlushMetadataRegionTask {
    fn name(&self) -> &str {
        "FlushMetadataRegionTask"
    }

    async fn call(&mut self) -> Result<()> {
        let region_ids = {
            let state = self.state.read().unwrap();
            state
                .physical_region_states()
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };

        let num_region = region_ids.len();
        let now = Instant::now();
        for region_id in region_ids {
            let Some(role) = self.mito.role(region_id) else {
                continue;
            };
            if role == RegionRole::Follower {
                continue;
            }
            let metadata_region_id = utils::to_metadata_region_id(region_id);
            if let Err(e) = self
                .mito
                .handle_request(
                    metadata_region_id,
                    RegionRequest::Flush(RegionFlushRequest {
                        row_group_size: None,
                    }),
                )
                .await
            {
                error!(e; "Failed to flush metadata region {}", metadata_region_id);
            }
        }
        debug!(
            "Flushed {} metadata regions, elapsed: {:?}",
            num_region,
            now.elapsed()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::time::Duration;

    use store_api::region_engine::{RegionEngine, RegionManifestInfo};

    use crate::config::{EngineConfig, DEFAULT_FLUSH_METADATA_REGION_INTERVAL};
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn test_flush_metadata_region_task() {
        let env = TestEnv::with_prefix_and_config(
            "test_flush_metadata_region_task",
            EngineConfig {
                flush_metadata_region_interval: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .await;
        env.init_metric_region().await;
        let engine = env.metric();
        // Wait for flush task run
        tokio::time::sleep(Duration::from_millis(500)).await;
        let physical_region_id = env.default_physical_region_id();
        let stat = engine.region_statistic(physical_region_id).unwrap();

        assert_matches!(
            stat.manifest,
            RegionManifestInfo::Metric {
                metadata_manifest_version: 1,
                metadata_flushed_entry_id: 1,
                ..
            }
        )
    }

    #[tokio::test]
    async fn test_flush_metadata_region_task_with_long_interval() {
        let env = TestEnv::with_prefix_and_config(
            "test_flush_metadata_region_task_with_long_interval",
            EngineConfig {
                flush_metadata_region_interval: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .await;
        env.init_metric_region().await;
        let engine = env.metric();
        // Wait for flush task run, should not flush metadata region
        tokio::time::sleep(Duration::from_millis(200)).await;
        let physical_region_id = env.default_physical_region_id();
        let stat = engine.region_statistic(physical_region_id).unwrap();

        assert_matches!(
            stat.manifest,
            RegionManifestInfo::Metric {
                metadata_manifest_version: 0,
                metadata_flushed_entry_id: 0,
                ..
            }
        )
    }

    #[tokio::test]
    async fn test_flush_metadata_region_sanitize() {
        let env = TestEnv::with_prefix_and_config(
            "test_flush_metadata_region_sanitize",
            EngineConfig {
                flush_metadata_region_interval: Duration::from_secs(0),
                ..Default::default()
            },
        )
        .await;
        let metric = env.metric();
        let config = metric.config();
        assert_eq!(
            config.flush_metadata_region_interval,
            DEFAULT_FLUSH_METADATA_REGION_INTERVAL
        );
    }
}
