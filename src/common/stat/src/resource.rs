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

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_runtime::JoinHandle;
use common_telemetry::info;
use sysinfo::System;
use tokio::time::sleep;

use crate::cgroups::calculate_cpu_usage;
use crate::{
    get_cpu_limit_from_cgroups, get_cpu_usage_from_cgroups, get_memory_limit_from_cgroups,
    get_memory_usage_from_cgroups,
};

/// Get the total CPU in millicores. If the CPU limit is unset, it will return the total CPU cores from host system.
pub fn get_total_cpu_millicores() -> i64 {
    // Get CPU limit from cgroups filesystem.
    if let Some(cgroup_cpu_limit) = get_cpu_limit_from_cgroups() {
        cgroup_cpu_limit
    } else {
        // Get total CPU cores from host system.
        num_cpus::get() as i64 * 1000
    }
}

/// Get the total memory in bytes. If the memory limit is unset, it will return the total memory from host system.
/// If the system is not supported to get the total host memory, it will return 0.
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
            // If the system is not supported, return 0
            0
        }
    }
}

/// Get the total CPU cores. The result will be rounded to the nearest integer.
/// For example, if the total CPU is 1.5 cores(1500 millicores), the result will be 2.
pub fn get_total_cpu_cores() -> usize {
    cpu_cores(get_total_cpu_millicores())
}

fn cpu_cores(cpu_millicores: i64) -> usize {
    ((cpu_millicores as f64) / 1_000.0).ceil() as usize
}

/// Get the total memory in readable size.
pub fn get_total_memory_readable() -> Option<ReadableSize> {
    if get_total_memory_bytes() > 0 {
        Some(ReadableSize(get_total_memory_bytes() as u64))
    } else {
        None
    }
}

/// A reference to a `ResourceStat` implementation.
pub type ResourceStatRef = Arc<dyn ResourceStat + Send + Sync>;

/// A trait for getting resource statistics.
pub trait ResourceStat {
    /// Get the total CPU in millicores.
    fn get_total_cpu_millicores(&self) -> i64;
    /// Get the total memory in bytes.
    fn get_total_memory_bytes(&self) -> i64;
    /// Get the CPU usage in millicores.
    fn get_cpu_usage_millicores(&self) -> i64;
    /// Get the memory usage in bytes.
    fn get_memory_usage_bytes(&self) -> i64;
}

/// A implementation of `ResourceStat` trait.
pub struct ResourceStatImpl {
    cpu_usage_millicores: Arc<AtomicI64>,
    last_cpu_usage_usecs: Arc<AtomicI64>,
    calculate_interval: Duration,
    handler: Option<JoinHandle<()>>,
}

impl Default for ResourceStatImpl {
    fn default() -> Self {
        Self {
            cpu_usage_millicores: Arc::new(AtomicI64::new(0)),
            last_cpu_usage_usecs: Arc::new(AtomicI64::new(0)),
            calculate_interval: Duration::from_secs(5),
            handler: None,
        }
    }
}

impl ResourceStatImpl {
    /// Start collecting CPU usage periodically. It will calculate the CPU usage in millicores based on rate of change of CPU usage usage_usec in `/sys/fs/cgroup/cpu.stat`.
    /// It ONLY works in cgroup v2 environment.
    pub fn start_collect_cpu_usage(&mut self) {
        if self.handler.is_some() {
            return;
        }

        let cpu_usage_millicores = self.cpu_usage_millicores.clone();
        let last_cpu_usage_usecs = self.last_cpu_usage_usecs.clone();
        let calculate_interval = self.calculate_interval;

        let handler = common_runtime::spawn_global(async move {
            info!(
                "Starting to collect CPU usage periodically for every {} seconds",
                calculate_interval.as_secs()
            );
            loop {
                let current_cpu_usage_usecs = get_cpu_usage_from_cgroups();
                if let Some(current_cpu_usage_usecs) = current_cpu_usage_usecs {
                    // Skip the first time to collect CPU usage.
                    if last_cpu_usage_usecs.load(Ordering::Relaxed) == 0 {
                        last_cpu_usage_usecs.store(current_cpu_usage_usecs, Ordering::Relaxed);
                        continue;
                    }
                    let cpu_usage = calculate_cpu_usage(
                        current_cpu_usage_usecs,
                        last_cpu_usage_usecs.load(Ordering::Relaxed),
                        calculate_interval.as_millis() as i64,
                    );
                    cpu_usage_millicores.store(cpu_usage, Ordering::Relaxed);
                    last_cpu_usage_usecs.store(current_cpu_usage_usecs, Ordering::Relaxed);
                }
                sleep(calculate_interval).await;
            }
        });

        self.handler = Some(handler);
    }
}

impl ResourceStat for ResourceStatImpl {
    /// Get the total CPU in millicores.
    fn get_total_cpu_millicores(&self) -> i64 {
        get_total_cpu_millicores()
    }

    /// Get the total memory in bytes.
    fn get_total_memory_bytes(&self) -> i64 {
        get_total_memory_bytes()
    }

    /// Get the CPU usage in millicores.
    fn get_cpu_usage_millicores(&self) -> i64 {
        self.cpu_usage_millicores.load(Ordering::Relaxed)
    }

    /// Get the memory usage in bytes.
    /// It ONLY works in cgroup v2 environment.
    fn get_memory_usage_bytes(&self) -> i64 {
        get_memory_usage_from_cgroups().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_total_cpu_cores() {
        assert!(get_total_cpu_cores() > 0);
        assert_eq!(cpu_cores(1), 1);
        assert_eq!(cpu_cores(100), 1);
        assert_eq!(cpu_cores(500), 1);
        assert_eq!(cpu_cores(1000), 1);
        assert_eq!(cpu_cores(1100), 2);
        assert_eq!(cpu_cores(1900), 2);
        assert_eq!(cpu_cores(10_000), 10);
    }

    #[test]
    fn test_get_total_memory_readable() {
        assert!(get_total_memory_readable().unwrap() > ReadableSize::mb(0));
    }
}
