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
use sysinfo::System;

/// Get the cpu cores number of system.
pub fn get_cpus() -> usize {
    let mut sys_info = System::new();
    sys_info.refresh_cpu();
    // At least 1 cpu core.
    sys_info.cpus().len().max(1)
}

/// Get the total memory of the system.
pub fn get_sys_total_memory() -> ReadableSize {
    let mut sys_info = System::new();
    sys_info.refresh_memory();
    ReadableSize(sys_info.total_memory())
}
