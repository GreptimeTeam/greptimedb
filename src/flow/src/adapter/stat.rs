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
use crate::engine::FlowStatProvider;

impl FlowStatProvider for StreamingEngine {
    async fn flow_stat(&self) -> FlowStat {
        let mut state_size_map = BTreeMap::new();
        let mut last_exec_time_map = BTreeMap::new();
        let mut start_time_map = BTreeMap::new();

        for worker in self.worker_handles.iter() {
            match worker.get_full_flow_stat().await {
                Ok((sizes, exec_times, start_times)) => {
                    state_size_map.extend(sizes.into_iter().map(|(k, v)| (k as u32, v)));
                    last_exec_time_map.extend(exec_times.into_iter().map(|(k, v)| (k as u32, v)));
                    start_time_map.extend(start_times.into_iter().map(|(k, v)| (k as u32, v)));
                }
                Err(err) => {
                    common_telemetry::error!(err; "Get full flow stat error");
                }
            }
        }

        FlowStat {
            state_size: state_size_map,
            last_exec_time_map,
            start_time_map,
        }
    }
}
