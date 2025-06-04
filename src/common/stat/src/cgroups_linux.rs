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

use std::fs::read_to_string;
use std::path::Path;

use nix::sys::statfs;
use nix::sys::statfs::statfs;

/// `MAX_VALUE` is used to indicate that the resource is unlimited.
pub const MAX_VALUE: i64 = -1;

const CGROUP_UNIFIED_MOUNTPOINT: &str = "/sys/fs/cgroup";

const MEMORY_MAX_FILE_CGROUP_V2: &str = "memory.max";
const MEMORY_MAX_FILE_CGROUP_V1: &str = "memory.limit_in_bytes";
const CPU_MAX_FILE_CGROUP_V2: &str = "cpu.max";
const CPU_QUOTA_FILE_CGROUP_V1: &str = "cpu.cfs_quota_us";
const CPU_PERIOD_FILE_CGROUP_V1: &str = "cpu.cfs_period_us";

// `MAX_VALUE_CGROUP_V2` string in `/sys/fs/cgroup/cpu.max` and `/sys/fs/cgroup/memory.max` to indicate that the resource is unlimited.
const MAX_VALUE_CGROUP_V2: &str = "max";

// For cgroup v1, if the memory is unlimited, it will return a very large value(different from platform) that close to 2^63.
// For easier comparison, if the memory limit is larger than 1PB we consider it as unlimited.
const MAX_MEMORY_IN_BYTES: i64 = 1125899906842624; // 1PB

/// Check whether the cgroup is v2.
///
/// - Return `true` if the cgroup is v2, otherwise return `false`.
/// - Return `None` if the detection fails.
pub fn is_cgroup_v2() -> Option<bool> {
    let path = Path::new(CGROUP_UNIFIED_MOUNTPOINT);
    let fs_stat = statfs(path).ok()?;
    Some(fs_stat.filesystem_type() == statfs::CGROUP2_SUPER_MAGIC)
}

/// Get the limit of memory in bytes.
///
/// - If the memory is unlimited, return `-1`.
/// - Return `None` if it fails to read the memory limit.
pub fn get_memory_limit() -> Option<i64> {
    let memory_max_file = if is_cgroup_v2()? {
        // Read `/sys/fs/cgroup/memory.max` to get the memory limit.
        MEMORY_MAX_FILE_CGROUP_V2
    } else {
        // Read `/sys/fs/cgroup/memory.limit_in_bytes` to get the memory limit.
        MEMORY_MAX_FILE_CGROUP_V1
    };

    // For cgroup v1, it will return a very large value(different from platform) if the memory is unlimited.
    let memory_limit =
        read_value_from_file(Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(memory_max_file))?;

    // If memory limit exceeds 1PB(cgroup v1), consider it as unlimited.
    if memory_limit > MAX_MEMORY_IN_BYTES {
        return Some(MAX_VALUE);
    }

    Some(memory_limit)
}

/// Get the limit of cpu in millicores.
///
/// - If the cpu is unlimited, return `-1`.
/// - Return `None` if it fails to read the cpu limit.
pub fn get_cpu_limit() -> Option<i64> {
    if is_cgroup_v2()? {
        // Read `/sys/fs/cgroup/cpu.max` to get the cpu limit.
        get_cgroup_v2_cpu_limit(Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(CPU_MAX_FILE_CGROUP_V2))
    } else {
        // Read `/sys/fs/cgroup/cpu.cfs_quota_us` and `/sys/fs/cgroup/cpu.cfs_period_us` to get the cpu limit.
        let quota = read_value_from_file(
            Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(CPU_QUOTA_FILE_CGROUP_V1),
        )?;

        if quota == MAX_VALUE {
            return Some(MAX_VALUE);
        }

        let period = read_value_from_file(
            Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(CPU_PERIOD_FILE_CGROUP_V1),
        )?;

        // Return the cpu limit in millicores.
        Some(quota * 1000 / period)
    }
}

fn read_value_from_file<P: AsRef<Path>>(path: P) -> Option<i64> {
    let content = read_to_string(&path).ok()?;

    // If the content starts with "max", return `MAX_VALUE`.
    if content.starts_with(MAX_VALUE_CGROUP_V2) {
        return Some(MAX_VALUE);
    }

    content.trim().parse::<i64>().ok()
}

fn get_cgroup_v2_cpu_limit<P: AsRef<Path>>(path: P) -> Option<i64> {
    let content = read_to_string(&path).ok()?;

    let fields = content.trim().split(' ').collect::<Vec<&str>>();
    if fields.len() != 2 {
        return None;
    }

    // If the cpu is unlimited, it will be `-1`.
    let quota = fields[0].trim();
    if quota == MAX_VALUE_CGROUP_V2 {
        return Some(MAX_VALUE);
    }

    let quota = quota.parse::<i64>().ok()?;

    let period = fields[1].trim();
    let period = period.parse::<i64>().ok()?;

    // Return the cpu limit in millicores.
    Some(quota * 1000 / period)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_value_from_file() {
        assert_eq!(
            get_cgroup_v2_cpu_limit(Path::new("testdata/cpu.max")).unwrap(),
            1500
        );
        assert_eq!(
            get_cgroup_v2_cpu_limit(Path::new("testdata/cpu.max.unlimited")).unwrap(),
            MAX_VALUE
        );
        assert_eq!(
            get_cgroup_v2_cpu_limit(Path::new("non_existent_file")),
            None
        );
        assert_eq!(
            read_value_from_file(Path::new("testdata/memory.max")).unwrap(),
            100000
        );
        assert_eq!(
            read_value_from_file(Path::new("testdata/memory.max.unlimited")).unwrap(),
            MAX_VALUE
        );
        assert_eq!(read_value_from_file(Path::new("non_existent_file")), None);
    }
}
