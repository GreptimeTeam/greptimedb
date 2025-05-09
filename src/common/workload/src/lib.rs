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

use common_telemetry::info;
use serde::{Deserialize, Serialize};

/// The workload type of the datanode.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DatanodeWorkloadType {
    /// The datanode can handle all workloads (including ingest and query workloads).
    Hybrid = 0,
}

impl From<DatanodeWorkloadType> for api::v1::meta::DatanodeWorkloadType {
    fn from(value: DatanodeWorkloadType) -> Self {
        match value {
            DatanodeWorkloadType::Hybrid => api::v1::meta::DatanodeWorkloadType::Hybrid,
        }
    }
}

/// Sanitize the workload types.
pub fn sanitize_workload_types(workload_types: &mut Vec<DatanodeWorkloadType>) {
    info!("The Oss version only support Hybrid workload type");
    workload_types.clear();
    workload_types.push(DatanodeWorkloadType::Hybrid);
}
