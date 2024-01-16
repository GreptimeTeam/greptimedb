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

pub struct SystemInfo {
    pub cpu_cores: usize,
    pub total_memory: ReadableSize,
}

/// Get the cpu cores number of system.
pub fn get_cpus() -> usize {
    // This function will check cgroups
    num_cpus::get()
}

/// Get the total memory of the system.
pub fn get_sys_total_memory() -> Option<ReadableSize> {
    if sysinfo::IS_SUPPORTED_SYSTEM {
        let mut sys_info = System::new();
        sys_info.refresh_memory();
        let mut total_memory = sys_info.total_memory();
        // Compare with cgrous memory limit, use smaller values
        if let Some(cgrous_limits) = sys_info.cgroup_limits() {
            total_memory = total_memory.min(cgrous_limits.total_memory)
        }
        return Some(ReadableSize(total_memory));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_cpus() {
        assert!(get_cpus() > 0);
    }

    #[test]
    fn test_get_sys_total_memory() {
        assert!(get_sys_total_memory().unwrap() > ReadableSize::mb(0));
    }
}
