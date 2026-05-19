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
#[serde(default)]
pub struct InfluxdbOptions {
    pub enable: bool,
    pub default_merge_mode: InfluxdbMergeMode,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InfluxdbMergeMode {
    #[default]
    LastNonNull,
    LastRow,
}

impl InfluxdbMergeMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            InfluxdbMergeMode::LastNonNull => "last_non_null",
            InfluxdbMergeMode::LastRow => "last_row",
        }
    }
}

impl Default for InfluxdbOptions {
    fn default() -> Self {
        Self {
            enable: true,
            default_merge_mode: InfluxdbMergeMode::default(),
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
        assert_eq!("last_non_null", default.default_merge_mode.as_str());
    }

    #[test]
    fn test_influxdb_options_default_merge_mode() {
        let options: InfluxdbOptions = toml::from_str("default_merge_mode = 'last_row'").unwrap();
        assert_eq!("last_row", options.default_merge_mode.as_str());
    }
}
