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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use async_trait::async_trait;
use common_greptimedb_telemetry::{
    default_get_uuid, Collector, GreptimeDBTelemetry, GreptimeDBTelemetryTask,
    Mode as VersionReporterMode, TELEMETRY_INTERVAL,
};

use crate::cluster::MetaPeerClientRef;

struct DistributedGreptimeDBTelemetryCollector {
    meta_peer_client: MetaPeerClientRef,
    uuid: Option<String>,
    retry: i32,
}

#[async_trait]
impl Collector for DistributedGreptimeDBTelemetryCollector {
    fn get_mode(&self) -> VersionReporterMode {
        VersionReporterMode::Distributed
    }

    async fn get_nodes(&self) -> Option<i32> {
        self.meta_peer_client.get_node_cnt().await.ok()
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
    working_home: Option<String>,
    meta_peer_client: MetaPeerClientRef,
    enable: bool,
) -> Arc<GreptimeDBTelemetryTask> {
    if !enable || cfg!(test) || cfg!(debug_assertions) {
        return Arc::new(GreptimeDBTelemetryTask::disable());
    }
    // Controlled by meta server state, only leader reports the info.
    let should_report = Arc::new(AtomicBool::new(false));
    Arc::new(GreptimeDBTelemetryTask::enable(
        TELEMETRY_INTERVAL,
        Box::new(GreptimeDBTelemetry::new(
            working_home.clone(),
            Box::new(DistributedGreptimeDBTelemetryCollector {
                meta_peer_client,
                uuid: default_get_uuid(&working_home),
                retry: 0,
            }),
            should_report.clone(),
        )),
        should_report,
    ))
}
