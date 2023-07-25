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

#[cfg(feature = "greptimedb-telemetry")]
pub mod greptimedb_telemetry {
    use std::sync::Arc;

    use async_trait::async_trait;
    use common_greptimedb_telemetry::{
        default_get_uuid, Collector, GreptimeDBTelemetry, GreptimeDBTelemetryTask,
        Mode as VersionReporterMode, TELEMETRY_INTERVAL,
    };

    use crate::cluster::MetaPeerClientRef;
    struct DistributedGreptimeDBTelemetryCollector {
        #[cfg(feature = "greptimedb-telemetry")]
        meta_peer_client: MetaPeerClientRef,
        uuid: Option<String>,
        retry: i32,
    }

    #[async_trait]
    impl Collector for DistributedGreptimeDBTelemetryCollector {
        fn get_mode(&self) -> VersionReporterMode {
            VersionReporterMode::Distributed
        }

        #[cfg(feature = "greptimedb-telemetry")]
        async fn get_nodes(&self) -> i32 {
            self.meta_peer_client
                .get_node_cnt()
                .await
                .ok()
                .unwrap_or(-1)
        }

        #[cfg(not(feature = "greptimedb-telemetry"))]
        async fn get_nodes(&self) -> i32 {
            -1
        }

        fn get_retry(&self) -> i32 {
            self.retry
        }

        fn inc_retry(&mut self) {
            self.retry += 1;
        }

        fn set_uuid_cache(&mut self, uuid: String) {
            self.uuid = Some(uuid);
        }

        fn get_uuid_cache(&self) -> Option<String> {
            self.uuid.clone()
        }
    }

    pub async fn get_greptimedb_telemetry_task(
        meta_peer_client: MetaPeerClientRef,
    ) -> Arc<GreptimeDBTelemetryTask> {
        Arc::new(GreptimeDBTelemetryTask::enable(
            TELEMETRY_INTERVAL,
            Box::new(GreptimeDBTelemetry::new(Box::new(
                DistributedGreptimeDBTelemetryCollector {
                    meta_peer_client,
                    uuid: default_get_uuid(),
                    retry: 0,
                },
            ))),
        ))
    }
}

#[cfg(not(feature = "greptimedb-telemetry"))]
pub mod greptimedb_telemetry {
    use std::sync::Arc;

    use common_greptimedb_telemetry::GreptimeDBTelemetryTask;

    use crate::cluster::MetaPeerClientRef;
    pub async fn get_greptimedb_telemetry_task(
        _: MetaPeerClientRef,
    ) -> Arc<GreptimeDBTelemetryTask> {
        Arc::new(GreptimeDBTelemetryTask::disable())
    }
}
