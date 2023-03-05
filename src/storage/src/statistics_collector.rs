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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct StatisticsCollector {
    disk_usage_bytes: AtomicU64,
}

impl StatisticsCollector {
    pub fn disk_usage_bytes(&self) -> u64 {
        self.disk_usage_bytes.load(Ordering::Relaxed)
    }

    pub fn increase_disk_usage_bytes(&self, bytes: u64) {
        self.disk_usage_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn descrease_disk_usage_bytes(&self, bytes: u64) {
        self.disk_usage_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
}

pub type StatisticsCollectorRef = Arc<StatisticsCollector>;

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_disk_usage_bytes() {
        let statistics_collector = StatisticsCollectorRef::default();
        statistics_collector.increase_disk_usage_bytes(2048);
        assert_eq!(statistics_collector.disk_usage_bytes(), 2048);

        statistics_collector.descrease_disk_usage_bytes(1024);
        assert_eq!(statistics_collector.disk_usage_bytes(), 1024);
    }
}
