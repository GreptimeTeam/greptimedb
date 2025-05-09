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
    /// The datanode can handle all workloads.
    Hybrid = 0,
}

impl DatanodeWorkloadType {
    /// Convert from `i32` to `DatanodeWorkloadType`.
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            v if v == Self::Hybrid as i32 => Some(Self::Hybrid),
            _ => None,
        }
    }

    /// Convert from `DatanodeWorkloadType` to `i32`.
    pub fn to_i32(self) -> i32 {
        self as i32
    }

    pub fn accept_ingest(&self) -> bool {
        matches!(self, Self::Hybrid)
    }
}

/// Sanitize the workload types.
pub fn sanitize_workload_types(workload_types: &mut Vec<DatanodeWorkloadType>) {
    if workload_types.is_empty() {
        info!("The workload types is empty, use Hybrid workload type");
        workload_types.push(DatanodeWorkloadType::Hybrid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_workload_types() {
        let hybrid = DatanodeWorkloadType::Hybrid;
        assert_eq!(hybrid as i32, 0);
        let hybrid_i32 = hybrid.to_i32();
        assert_eq!(hybrid_i32, 0);
        assert_eq!(DatanodeWorkloadType::from_i32(hybrid_i32), Some(hybrid));

        let unexpected_i32 = 100;
        assert_eq!(DatanodeWorkloadType::from_i32(unexpected_i32), None);
    }
}
