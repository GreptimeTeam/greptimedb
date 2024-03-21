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

//! Internal metrics of the memtable.

/// Metrics of writing the partition tree.
pub struct WriteMetrics {
    /// Size allocated by keys.
    pub key_bytes: usize,
    /// Size allocated by values.
    pub value_bytes: usize,
    /// Minimum timestamp.
    pub min_ts: i64,
    /// Maximum timestamp
    pub max_ts: i64,
}

impl Default for WriteMetrics {
    fn default() -> Self {
        Self {
            key_bytes: 0,
            value_bytes: 0,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
        }
    }
}
