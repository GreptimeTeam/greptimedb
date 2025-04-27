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

use std::collections::BTreeMap;

use common_meta::key::flow::flow_state::FlowStat;

use crate::StreamingEngine;

impl StreamingEngine {
    pub async fn gen_state_report(&self) -> FlowStat {
        let mut full_report = BTreeMap::new();
        let mut last_exec_time_map = BTreeMap::new();
        for worker in self.worker_handles.iter() {
            match worker.get_state_size().await {
                Ok(state_size) => {
                    full_report.extend(state_size.into_iter().map(|(k, v)| (k as u32, v)));
                }
                Err(err) => {
                    common_telemetry::error!(err; "Get flow stat size error");
                }
            }

            match worker.get_last_exec_time_map().await {
                Ok(last_exec_time) => {
                    last_exec_time_map
                        .extend(last_exec_time.into_iter().map(|(k, v)| (k as u32, v)));
                }
                Err(err) => {
                    common_telemetry::error!(err; "Get last exec time error");
                }
            }
        }

        FlowStat {
            state_size: full_report,
            last_exec_time_map,
        }
    }
}
