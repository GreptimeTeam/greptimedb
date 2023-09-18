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

//! Options for a region.

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use serde_with::{serde_as, with_prefix, DisplayFromStr};
use snafu::ResultExt;

use crate::error::{Error, JsonOptionsSnafu, Result};

/// Options that affect the entire region.
///
/// Users need to specify the options while creating/opening a region.
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct RegionOptions {
    /// Region SST files TTL.
    #[serde(with = "humantime_serde")]
    pub ttl: Option<Duration>,
    /// Compaction options.
    pub compaction: CompactionOptions,
}

impl TryFrom<&HashMap<String, String>> for RegionOptions {
    type Error = Error;

    fn try_from(options_map: &HashMap<String, String>) -> Result<Self> {
        let value = options_map_to_value(options_map);
        let json = serde_json::to_string(&value).context(JsonOptionsSnafu)?;

        // #[serde(flatten)] doesn't work with #[serde(default)] so we need to parse
        // each field manually instead of using #[serde(flatten)] for `compaction`.
        // See https://github.com/serde-rs/serde/issues/1626
        let options: RegionOptionsWithoutEnum =
            serde_json::from_str(&json).context(JsonOptionsSnafu)?;
        let compaction: CompactionOptions = serde_json::from_str(&json).unwrap_or_default();

        Ok(RegionOptions {
            ttl: options.ttl,
            compaction,
        })
    }
}

/// Options for compactions
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "compaction.type")]
#[serde(rename_all = "lowercase")]
pub enum CompactionOptions {
    /// Time window compaction strategy.
    #[serde(with = "prefix_twcs")]
    Twcs(TwcsOptions),
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self::Twcs(TwcsOptions::default())
    }
}

/// Time window compaction options.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct TwcsOptions {
    /// Max num of files that can be kept in active writing time window.
    #[serde_as(as = "DisplayFromStr")]
    pub max_active_window_files: usize,
    /// Max num of files that can be kept in inactive time window.
    #[serde_as(as = "DisplayFromStr")]
    pub max_inactive_window_files: usize,
    /// Compaction time window defined when creating tables.
    #[serde(with = "humantime_serde")]
    pub time_window: Option<Duration>,
}

with_prefix!(prefix_twcs "compaction.twcs.");

impl TwcsOptions {
    /// Returns time window in second resolution.
    pub fn time_window_seconds(&self) -> Option<i64> {
        self.time_window.and_then(|window| {
            let window_secs = window.as_secs();
            if window_secs == 0 {
                None
            } else {
                window_secs.try_into().ok()
            }
        })
    }
}

impl Default for TwcsOptions {
    fn default() -> Self {
        Self {
            max_active_window_files: 4,
            max_inactive_window_files: 1,
            time_window: None,
        }
    }
}

/// We need to define a new struct without enum fields as `#[serde(default)]` does not
/// support external tagging.
#[derive(Debug, Deserialize)]
#[serde(default)]
struct RegionOptionsWithoutEnum {
    /// Region SST files TTL.
    #[serde(with = "humantime_serde")]
    ttl: Option<Duration>,
}

impl Default for RegionOptionsWithoutEnum {
    fn default() -> Self {
        let options = RegionOptions::default();
        RegionOptionsWithoutEnum { ttl: options.ttl }
    }
}

/// Converts the `options` map to a json object.
///
/// Converts all key-values to lowercase and replaces "null" strings by `null` json values.
fn options_map_to_value(options: &HashMap<String, String>) -> Value {
    let map = options
        .iter()
        .map(|(key, value)| {
            let (key, value) = (key.to_lowercase(), value.to_lowercase());

            if value == "null" {
                (key, Value::Null)
            } else {
                (key, Value::from(value))
            }
        })
        .collect();
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_map(options: &[(&str, &str)]) -> HashMap<String, String> {
        options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_empty_region_options() {
        let map = make_map(&[]);
        let options = RegionOptions::try_from(&map).unwrap();
        assert_eq!(RegionOptions::default(), options);
    }

    #[test]
    fn test_with_ttl() {
        let map = make_map(&[("ttl", "7d")]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            ttl: Some(Duration::from_secs(3600 * 24 * 7)),
            ..Default::default()
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_without_compaction_type() {
        // If `compaction.type` is not provided, we ignore all compaction
        // related options. Actually serde does not support deserialize
        // an enum without knowning its type.
        let map = make_map(&[
            ("compaction.twcs.max_active_window_files", "8"),
            ("compaction.twcs.time_window", "2h"),
        ]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions::default();
        assert_eq!(expect, options);
    }

    #[test]
    fn test_with_compaction_type() {
        let map = make_map(&[
            ("compaction.twcs.max_active_window_files", "8"),
            ("compaction.twcs.time_window", "2h"),
            ("compaction.type", "twcs"),
        ]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            compaction: CompactionOptions::Twcs(TwcsOptions {
                max_active_window_files: 8,
                time_window: Some(Duration::from_secs(3600 * 2)),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_with_all() {
        let map = make_map(&[
            ("ttl", "7d"),
            ("compaction.twcs.max_active_window_files", "8"),
            ("compaction.twcs.max_inactive_window_files", "2"),
            ("compaction.twcs.time_window", "2h"),
            ("compaction.type", "twcs"),
        ]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            ttl: Some(Duration::from_secs(3600 * 24 * 7)),
            compaction: CompactionOptions::Twcs(TwcsOptions {
                max_active_window_files: 8,
                max_inactive_window_files: 2,
                time_window: Some(Duration::from_secs(3600 * 2)),
            }),
        };
        assert_eq!(expect, options);
    }
}
