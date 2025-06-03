use std::fs::read_to_string;
use std::path::Path;

use nix::sys::statfs::statfs;
use snafu::ResultExt;

use crate::error::{ParseCgroupDataSnafu, ReadCgroupMountpointSnafu, ReadFileSnafu, Result};

const CGROUP_UNIFIED_MOUNTPOINT: &str = "/sys/fs/cgroup";

const MEMORY_MAX_FILE_CGROUP_V2: &str = "memory.max";
const MEMORY_MAX_FILE_CGROUP_V1: &str = "memory.limit_in_bytes";

const CPU_MAX_FILE_CGROUP_V2: &str = "cpu.max";

const MAX_VALUE: &str = "max";

const NO_LIMIT: f64 = -1.0;

// Check whether the cgroup is v2. If not, return false.
#[cfg(target_os = "linux")]
pub fn is_cgroup_v2() -> Result<bool> {
    let path = Path::new(CGROUP_UNIFIED_MOUNTPOINT);
    Ok(statfs(path)
        .context(ReadCgroupMountpointSnafu {
            path: path.to_string_lossy().to_string(),
        })?
        .filesystem_type()
        == statfs::CGROUP2_SUPER_MAGIC)
}

// Get the memory limit.
//#[cfg(target_os = "linux")]
pub fn get_memory_limit() -> Result<f64> {
    if is_cgroup_v2() {
        // Read `/sys/fs/cgroup/memory.max` to get the memory limit.
        read_u64_from_file(&Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(MEMORY_MAX_FILE_CGROUP_V2))
    } else {
        // Read `/sys/fs/cgroup/memory.limit_in_bytes` to get the memory limit.
        let limit = read_u64_from_file(
            &Path::new(CGROUP_UNIFIED_MOUNTPOINT).join(MEMORY_MAX_FILE_CGROUP_V1),
        )?;

        // In cgroups v1, the me
    }
}

#[cfg(target_os = "linux")]
pub fn get_max_cpu_usage() -> f64 {
    // Read /sys/fs/cgroup/cpu.max
    let content = std::fs::read_to_string("./cpu.max").unwrap();
    if content.starts_with("max") {
        return -1.0;
    }

    // Get quota and period, for example: 100000 100000
    let parts = content.split(" ").collect::<Vec<&str>>();
    let quota = parts[0].parse::<u64>().unwrap();
    let period = parts[1].parse::<u64>().unwrap();
    quota as f64 / period as f64
}

fn read_u64_from_file<P: AsRef<Path>>(path: P) -> Result<f64> {
    let content = read_to_string(&path).context(ReadFileSnafu {
        path: path.as_ref().to_string_lossy().to_string(),
    })?;

    // If the content starts with "max", return NO_LIMIT.
    if content.starts_with(MAX_VALUE) {
        return Ok(NO_LIMIT);
    }

    let data = content
        .trim()
        .parse::<u64>()
        .context(ParseCgroupDataSnafu {
            path: path.as_ref().to_string_lossy().to_string(),
        })?;

    // In cgroups v1, if the memory is not
    if data == i64::MAX as u64 {
        return Ok(NO_LIMIT);
    }

    Ok(data as f64)
}
