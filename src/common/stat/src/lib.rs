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

mod cgroups;

pub use cgroups::*;
use common_base::readable_size::ReadableSize;
use sysinfo::System;

/// Get the total CPU in millicores.
pub fn get_total_cpu_millicores() -> i64 {
    // Get CPU limit from cgroups filesystem.
    if let Some(cgroup_cpu_limit) = get_cpu_limit_from_cgroups() {
        cgroup_cpu_limit
    } else {
        // Get total CPU cores from host system.
        num_cpus::get() as i64 * 1000
    }
}

/// Get the total memory in bytes.
pub fn get_total_memory_bytes() -> i64 {
    // Get memory limit from cgroups filesystem.
    if let Some(cgroup_memory_limit) = get_memory_limit_from_cgroups() {
        cgroup_memory_limit
    } else {
        // Get total memory from host system.
        if sysinfo::IS_SUPPORTED_SYSTEM {
            let mut sys_info = System::new();
            sys_info.refresh_memory();
            sys_info.total_memory() as i64
        } else {
            // If the system is not supported, return -1.
            -1
        }
    }
}

/// Get the total CPU cores. The result will be rounded to the nearest integer.
/// For example, if the total CPU is 1.5 cores(1500 millicores), the result will be 2.
pub fn get_total_cpu_cores() -> usize {
    ((get_total_cpu_millicores() as f64) / 1000.0).round() as usize
}

/// Get the total memory in readable size.
pub fn get_total_memory_readable() -> Option<ReadableSize> {
    if get_total_memory_bytes() > 0 {
        Some(ReadableSize(get_total_memory_bytes() as u64))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_total_cpu_cores() {
        assert!(get_total_cpu_cores() > 0);
    }

    #[test]
    fn test_get_total_memory_readable() {
        assert!(get_total_memory_readable().unwrap() > ReadableSize::mb(0));
    }
}
