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

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub file_size: u64,
    pub log_file_dir: String,
    pub purge_interval: Duration,
    pub purge_threshold: u64,
    pub read_batch_size: usize,
    pub sync_write: bool,
}

impl Default for LogConfig {
    /// Default value of config stores log file into a tmp directory, which should only be used
    /// in tests.
    fn default() -> Self {
        Self {
            file_size: 1024 * 1024 * 1024,
            log_file_dir: "/tmp/greptimedb".to_string(),
            purge_interval: Duration::from_secs(10 * 60),
            purge_threshold: 1024 * 1024 * 1024 * 50,
            read_batch_size: 128,
            sync_write: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use common_telemetry::info;

    use super::*;

    #[test]
    pub fn test_default_config() {
        common_telemetry::logging::init_default_ut_logging();
        let default = LogConfig::default();
        info!("LogConfig::default(): {:?}", default);
        assert_eq!(1024 * 1024 * 1024, default.file_size);
        assert_eq!(Duration::from_secs(600), default.purge_interval);
        assert_eq!(1024 * 1024 * 1024 * 50, default.purge_threshold);
        assert_eq!(128, default.read_batch_size);
        assert!(!default.sync_write);
    }
}
