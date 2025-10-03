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

use common_base::readable_size::ReadableSize;
use common_stat::{get_total_cpu_millicores, get_total_memory_readable};

/// `ResourceSpec` holds the static resource specifications of a node,
/// such as CPU cores and memory capacity. These values are fixed
/// at startup and do not change dynamically during runtime.
#[derive(Debug, Clone, Copy)]
pub struct ResourceSpec {
    pub total_cpu_millicores: i64,
    pub total_memory_bytes: Option<ReadableSize>,
}

impl Default for ResourceSpec {
    fn default() -> Self {
        Self {
            total_cpu_millicores: get_total_cpu_millicores(),
            total_memory_bytes: get_total_memory_readable(),
        }
    }
}
