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

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InfluxdbOptions {
    pub enable: bool,

    /// If set to true, the timestamps in InfluxDB lines will be automatically converted to the
    /// time index precision of their tables. Otherwise, the timestamps are fixed to nanosecond
    /// precision. This option can be used if the InfluxDB table you created has a different
    /// time index precision than nanosecond.
    pub auto_align_precision: bool,
}

impl Default for InfluxdbOptions {
    fn default() -> Self {
        Self {
            enable: true,
            auto_align_precision: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::InfluxdbOptions;

    #[test]
    fn test_influxdb_options() {
        let default = InfluxdbOptions::default();
        assert!(default.enable);
        assert!(!default.auto_align_precision);
    }
}
