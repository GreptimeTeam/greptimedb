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
//!
//! If we add options in this mod, we also need to modify [store_api::mito_engine_options].

use std::collections::HashMap;
use std::time::Duration;

use common_wal::options::{WalOptions, WAL_OPTIONS_KEY};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use serde_with::{serde_as, with_prefix, DisplayFromStr};
use snafu::ResultExt;
use store_api::storage::ColumnId;

use crate::error::{Error, JsonOptionsSnafu, Result};

const DEFAULT_INDEX_SEGMENT_ROW_COUNT: usize = 1024;

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
    /// Custom storage. Uses default storage if it is `None`.
    pub storage: Option<String>,
    /// Wal options.
    pub wal_options: WalOptions,
    /// Index options.
    pub index_options: IndexOptions,
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

        // Tries to decode the wal options from the map or sets to the default if there's none wal options in the map.
        let wal_options = options_map.get(WAL_OPTIONS_KEY).map_or_else(
            || Ok(WalOptions::default()),
            |encoded_wal_options| {
                serde_json::from_str(encoded_wal_options).context(JsonOptionsSnafu)
            },
        )?;

        let index_options: IndexOptions = serde_json::from_str(&json).context(JsonOptionsSnafu)?;

        Ok(RegionOptions {
            ttl: options.ttl,
            compaction,
            storage: options.storage,
            wal_options,
            index_options,
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

impl CompactionOptions {
    pub(crate) fn time_window(&self) -> Option<Duration> {
        match self {
            CompactionOptions::Twcs(opts) => opts.time_window,
        }
    }
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
    storage: Option<String>,
}

impl Default for RegionOptionsWithoutEnum {
    fn default() -> Self {
        let options = RegionOptions::default();
        RegionOptionsWithoutEnum {
            ttl: options.ttl,
            storage: options.storage,
        }
    }
}

with_prefix!(prefix_inverted_index "index.inverted_index.");

/// Options for index.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default)]
pub struct IndexOptions {
    /// Options for the inverted index.
    #[serde(flatten, with = "prefix_inverted_index")]
    pub inverted_index: InvertedIndexOptions,
}

/// Options for the inverted index.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct InvertedIndexOptions {
    /// The column ids that should be ignored when building the inverted index.
    /// The column ids are separated by commas. For example, "1,2,3".
    #[serde(deserialize_with = "deserialize_ignore_column_ids")]
    pub ignore_column_ids: Vec<ColumnId>,

    /// The number of rows in a segment.
    #[serde_as(as = "DisplayFromStr")]
    pub segment_row_count: usize,
}

impl Default for InvertedIndexOptions {
    fn default() -> Self {
        Self {
            ignore_column_ids: Vec::new(),
            segment_row_count: DEFAULT_INDEX_SEGMENT_ROW_COUNT,
        }
    }
}

fn deserialize_ignore_column_ids<'de, D>(deserializer: D) -> Result<Vec<ColumnId>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let mut column_ids = Vec::new();
    for item in s.split(',') {
        let column_id = item.parse().map_err(D::Error::custom)?;
        column_ids.push(column_id);
    }
    Ok(column_ids)
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
    use common_wal::options::KafkaWalOptions;

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
    fn test_with_storage() {
        let map = make_map(&[("storage", "S3")]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            storage: Some("s3".to_string()),
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

    fn test_with_wal_options(wal_options: &WalOptions) -> bool {
        let encoded_wal_options = serde_json::to_string(&wal_options).unwrap();
        let map = make_map(&[(WAL_OPTIONS_KEY, &encoded_wal_options)]);
        let got = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            wal_options: wal_options.clone(),
            ..Default::default()
        };
        expect == got
    }

    #[test]
    fn test_with_index() {
        let map = make_map(&[
            ("index.inverted_index.ignore_column_ids", "1,2,3"),
            ("index.inverted_index.segment_row_count", "512"),
        ]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    ignore_column_ids: vec![1, 2, 3],
                    segment_row_count: 512,
                },
            },
            ..Default::default()
        };
        assert_eq!(expect, options);
    }

    // No need to add compatible tests for RegionOptions since the above tests already check for compatibility.
    #[test]
    fn test_with_any_wal_options() {
        let all_wal_options = vec![
            WalOptions::RaftEngine,
            WalOptions::Kafka(KafkaWalOptions {
                topic: "test_topic".to_string(),
            }),
        ];
        all_wal_options.iter().all(test_with_wal_options);
    }

    #[test]
    fn test_with_all() {
        let wal_options = WalOptions::Kafka(KafkaWalOptions {
            topic: "test_topic".to_string(),
        });
        let map = make_map(&[
            ("ttl", "7d"),
            ("compaction.twcs.max_active_window_files", "8"),
            ("compaction.twcs.max_inactive_window_files", "2"),
            ("compaction.twcs.time_window", "2h"),
            ("compaction.type", "twcs"),
            ("storage", "S3"),
            ("index.inverted_index.ignore_column_ids", "1,2,3"),
            ("index.inverted_index.segment_row_count", "512"),
            (
                WAL_OPTIONS_KEY,
                &serde_json::to_string(&wal_options).unwrap(),
            ),
        ]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            ttl: Some(Duration::from_secs(3600 * 24 * 7)),
            compaction: CompactionOptions::Twcs(TwcsOptions {
                max_active_window_files: 8,
                max_inactive_window_files: 2,
                time_window: Some(Duration::from_secs(3600 * 2)),
            }),
            storage: Some("s3".to_string()),
            wal_options,
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    ignore_column_ids: vec![1, 2, 3],
                    segment_row_count: 512,
                },
            },
        };
        assert_eq!(expect, options);
    }
}
