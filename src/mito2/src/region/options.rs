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

use common_base::readable_size::ReadableSize;
use common_wal::options::{WalOptions, WAL_OPTIONS_KEY};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use serde_with::{serde_as, with_prefix, DisplayFromStr, NoneAsEmptyString};
use snafu::{ensure, ResultExt};
use store_api::storage::ColumnId;
use strum::EnumString;

use crate::error::{Error, InvalidRegionOptionsSnafu, JsonOptionsSnafu, Result};
use crate::memtable::partition_tree::{DEFAULT_FREEZE_THRESHOLD, DEFAULT_MAX_KEYS_PER_SHARD};

const DEFAULT_INDEX_SEGMENT_ROW_COUNT: usize = 1024;

/// Mode to handle duplicate rows while merging.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MergeMode {
    /// Keeps the last row.
    #[default]
    LastRow,
    /// Keeps the last non-null field for each row.
    LastNonNull,
}

// Note: We need to update [store_api::mito_engine_options::is_mito_engine_option_key()]
// if we want expose the option to table options.
/// Options that affect the entire region.
///
/// Users need to specify the options while creating/opening a region.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionOptions {
    /// Region SST files TTL.
    #[serde(with = "humantime_serde")]
    pub ttl: Option<Duration>,
    /// Compaction options.
    pub compaction: CompactionOptions,
    /// Custom storage. Uses default storage if it is `None`.
    pub storage: Option<String>,
    /// If append mode is enabled, the region keeps duplicate rows.
    pub append_mode: bool,
    /// Wal options.
    pub wal_options: WalOptions,
    /// Index options.
    pub index_options: IndexOptions,
    /// Memtable options.
    pub memtable: Option<MemtableOptions>,
    /// The mode to merge duplicate rows.
    /// Only takes effect when `append_mode` is `false`.
    pub merge_mode: Option<MergeMode>,
}

impl RegionOptions {
    /// Validates options.
    pub fn validate(&self) -> Result<()> {
        if self.append_mode {
            ensure!(
                self.merge_mode.is_none(),
                InvalidRegionOptionsSnafu {
                    reason: "merge_mode is not allowed when append_mode is enabled",
                }
            );
        }
        Ok(())
    }

    /// Returns `true` if deduplication is needed.
    pub fn need_dedup(&self) -> bool {
        !self.append_mode
    }

    /// Returns the `merge_mode` if it is set, otherwise returns the default `MergeMode`.
    pub fn merge_mode(&self) -> MergeMode {
        self.merge_mode.unwrap_or_default()
    }
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
        let compaction = if validate_enum_options(options_map, "compaction.type")? {
            serde_json::from_str(&json).context(JsonOptionsSnafu)?
        } else {
            CompactionOptions::default()
        };

        // Tries to decode the wal options from the map or sets to the default if there's none wal options in the map.
        let wal_options = options_map.get(WAL_OPTIONS_KEY).map_or_else(
            || Ok(WalOptions::default()),
            |encoded_wal_options| {
                serde_json::from_str(encoded_wal_options).context(JsonOptionsSnafu)
            },
        )?;

        let index_options: IndexOptions = serde_json::from_str(&json).context(JsonOptionsSnafu)?;
        let memtable = if validate_enum_options(options_map, "memtable.type")? {
            Some(serde_json::from_str(&json).context(JsonOptionsSnafu)?)
        } else {
            None
        };

        let opts = RegionOptions {
            ttl: options.ttl,
            compaction,
            storage: options.storage,
            append_mode: options.append_mode,
            wal_options,
            index_options,
            memtable,
            merge_mode: options.merge_mode,
        };
        opts.validate()?;

        Ok(opts)
    }
}

/// Options for compactions
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "compaction.type")]
#[serde(rename_all = "snake_case")]
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

    pub(crate) fn remote_compaction(&self) -> bool {
        match self {
            CompactionOptions::Twcs(opts) => opts.remote_compaction,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TwcsOptions {
    /// Max num of sorted runs that can be kept in active writing time window.
    #[serde_as(as = "DisplayFromStr")]
    pub max_active_window_runs: usize,
    /// Max num of files in the active window.
    #[serde_as(as = "DisplayFromStr")]
    pub max_active_window_files: usize,
    /// Max num of sorted runs that can be kept in inactive time windows.
    #[serde_as(as = "DisplayFromStr")]
    pub max_inactive_window_runs: usize,
    /// Max num of files in inactive time windows.
    #[serde_as(as = "DisplayFromStr")]
    pub max_inactive_window_files: usize,
    /// Compaction time window defined when creating tables.
    #[serde(with = "humantime_serde")]
    pub time_window: Option<Duration>,
    /// Whether to use remote compaction.
    #[serde_as(as = "DisplayFromStr")]
    pub remote_compaction: bool,
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
            max_active_window_runs: 4,
            max_active_window_files: 4,
            max_inactive_window_runs: 1,
            max_inactive_window_files: 1,
            time_window: None,
            remote_compaction: false,
        }
    }
}

/// We need to define a new struct without enum fields as `#[serde(default)]` does not
/// support external tagging.
#[serde_as]
#[derive(Debug, Deserialize)]
#[serde(default)]
struct RegionOptionsWithoutEnum {
    /// Region SST files TTL.
    #[serde(with = "humantime_serde")]
    ttl: Option<Duration>,
    storage: Option<String>,
    #[serde_as(as = "DisplayFromStr")]
    append_mode: bool,
    #[serde_as(as = "NoneAsEmptyString")]
    merge_mode: Option<MergeMode>,
}

impl Default for RegionOptionsWithoutEnum {
    fn default() -> Self {
        let options = RegionOptions::default();
        RegionOptionsWithoutEnum {
            ttl: options.ttl,
            storage: options.storage,
            append_mode: options.append_mode,
            merge_mode: options.merge_mode,
        }
    }
}

with_prefix!(prefix_inverted_index "index.inverted_index.");

/// Options for index.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexOptions {
    /// Options for the inverted index.
    #[serde(flatten, with = "prefix_inverted_index")]
    pub inverted_index: InvertedIndexOptions,
}

/// Options for the inverted index.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct InvertedIndexOptions {
    /// The column ids that should be ignored when building the inverted index.
    /// The column ids are separated by commas. For example, "1,2,3".
    #[serde(deserialize_with = "deserialize_ignore_column_ids")]
    #[serde(serialize_with = "serialize_ignore_column_ids")]
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

/// Options for region level memtable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "memtable.type", rename_all = "snake_case")]
pub enum MemtableOptions {
    TimeSeries,
    #[serde(with = "prefix_partition_tree")]
    PartitionTree(PartitionTreeOptions),
}

with_prefix!(prefix_partition_tree "memtable.partition_tree.");

/// Partition tree memtable options.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PartitionTreeOptions {
    /// Max keys in an index shard.
    #[serde_as(as = "DisplayFromStr")]
    pub index_max_keys_per_shard: usize,
    /// Number of rows to freeze a data part.
    #[serde_as(as = "DisplayFromStr")]
    pub data_freeze_threshold: usize,
    /// Total bytes of dictionary to keep in fork.
    pub fork_dictionary_bytes: ReadableSize,
}

impl Default for PartitionTreeOptions {
    fn default() -> Self {
        let mut fork_dictionary_bytes = ReadableSize::mb(512);
        if let Some(sys_memory) = common_config::utils::get_sys_total_memory() {
            let adjust_dictionary_bytes = std::cmp::min(
                sys_memory / crate::memtable::partition_tree::DICTIONARY_SIZE_FACTOR,
                fork_dictionary_bytes,
            );
            if adjust_dictionary_bytes.0 > 0 {
                fork_dictionary_bytes = adjust_dictionary_bytes;
            }
        }
        Self {
            index_max_keys_per_shard: DEFAULT_MAX_KEYS_PER_SHARD,
            data_freeze_threshold: DEFAULT_FREEZE_THRESHOLD,
            fork_dictionary_bytes,
        }
    }
}

fn deserialize_ignore_column_ids<'de, D>(deserializer: D) -> Result<Vec<ColumnId>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let mut column_ids = Vec::new();
    if s.is_empty() {
        return Ok(column_ids);
    }
    for item in s.split(',') {
        let column_id = item.parse().map_err(D::Error::custom)?;
        column_ids.push(column_id);
    }
    Ok(column_ids)
}

fn serialize_ignore_column_ids<S>(column_ids: &[ColumnId], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let s = column_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    serializer.serialize_str(&s)
}

/// Converts the `options` map to a json object.
///
/// Replaces "null" strings by `null` json values.
fn options_map_to_value(options: &HashMap<String, String>) -> Value {
    let map = options
        .iter()
        .map(|(key, value)| {
            // Only convert the key to lowercase.
            if value.eq_ignore_ascii_case("null") {
                (key.to_string(), Value::Null)
            } else {
                (key.to_string(), Value::from(value.to_string()))
            }
        })
        .collect();
    Value::Object(map)
}

// `#[serde(default)]` doesn't support enum (https://github.com/serde-rs/serde/issues/1799) so we
// check the type key first.
/// Validates whether the `options_map` has valid options for specific `enum_tag_key`
/// and returns `true` if the map contains enum options.
fn validate_enum_options(
    options_map: &HashMap<String, String>,
    enum_tag_key: &str,
) -> Result<bool> {
    let enum_type = enum_tag_key.split('.').next().unwrap();
    let mut has_other_options = false;
    let mut has_tag = false;
    for key in options_map.keys() {
        if key == enum_tag_key {
            has_tag = true;
        } else if key.starts_with(enum_type) {
            has_other_options = true;
        }
    }

    // If tag is not provided, then other options for the enum should not exist.
    ensure!(
        has_tag || !has_other_options,
        InvalidRegionOptionsSnafu {
            reason: format!("missing key {} in options", enum_tag_key),
        }
    );

    Ok(has_tag)
}

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
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
            storage: Some("S3".to_string()),
            ..Default::default()
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_without_compaction_type() {
        let map = make_map(&[
            ("compaction.twcs.max_active_window_runs", "8"),
            ("compaction.twcs.time_window", "2h"),
        ]);
        let err = RegionOptions::try_from(&map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_with_compaction_type() {
        let map = make_map(&[
            ("compaction.twcs.max_active_window_runs", "8"),
            ("compaction.twcs.time_window", "2h"),
            ("compaction.type", "twcs"),
        ]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            compaction: CompactionOptions::Twcs(TwcsOptions {
                max_active_window_runs: 8,
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
        let all_wal_options = [
            WalOptions::RaftEngine,
            WalOptions::Kafka(KafkaWalOptions {
                topic: "test_topic".to_string(),
            }),
        ];
        all_wal_options.iter().all(test_with_wal_options);
    }

    #[test]
    fn test_with_memtable() {
        let map = make_map(&[("memtable.type", "time_series")]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            memtable: Some(MemtableOptions::TimeSeries),
            ..Default::default()
        };
        assert_eq!(expect, options);

        let map = make_map(&[("memtable.type", "partition_tree")]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            memtable: Some(MemtableOptions::PartitionTree(
                PartitionTreeOptions::default(),
            )),
            ..Default::default()
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_unknown_memtable_type() {
        let map = make_map(&[("memtable.type", "no_such_memtable")]);
        let err = RegionOptions::try_from(&map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_with_merge_mode() {
        let map = make_map(&[("merge_mode", "last_row")]);
        let options = RegionOptions::try_from(&map).unwrap();
        assert_eq!(MergeMode::LastRow, options.merge_mode());

        let map = make_map(&[("merge_mode", "last_non_null")]);
        let options = RegionOptions::try_from(&map).unwrap();
        assert_eq!(MergeMode::LastNonNull, options.merge_mode());

        let map = make_map(&[("merge_mode", "unknown")]);
        let err = RegionOptions::try_from(&map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_with_all() {
        let wal_options = WalOptions::Kafka(KafkaWalOptions {
            topic: "test_topic".to_string(),
        });
        let map = make_map(&[
            ("ttl", "7d"),
            ("compaction.twcs.max_active_window_runs", "8"),
            ("compaction.twcs.max_active_window_files", "11"),
            ("compaction.twcs.max_inactive_window_runs", "2"),
            ("compaction.twcs.max_inactive_window_files", "3"),
            ("compaction.twcs.time_window", "2h"),
            ("compaction.type", "twcs"),
            ("compaction.twcs.remote_compaction", "false"),
            ("storage", "S3"),
            ("append_mode", "false"),
            ("index.inverted_index.ignore_column_ids", "1,2,3"),
            ("index.inverted_index.segment_row_count", "512"),
            (
                WAL_OPTIONS_KEY,
                &serde_json::to_string(&wal_options).unwrap(),
            ),
            ("memtable.type", "partition_tree"),
            ("memtable.partition_tree.index_max_keys_per_shard", "2048"),
            ("memtable.partition_tree.data_freeze_threshold", "2048"),
            ("memtable.partition_tree.fork_dictionary_bytes", "128M"),
            ("merge_mode", "last_non_null"),
        ]);
        let options = RegionOptions::try_from(&map).unwrap();
        let expect = RegionOptions {
            ttl: Some(Duration::from_secs(3600 * 24 * 7)),
            compaction: CompactionOptions::Twcs(TwcsOptions {
                max_active_window_runs: 8,
                max_active_window_files: 11,
                max_inactive_window_runs: 2,
                max_inactive_window_files: 3,
                time_window: Some(Duration::from_secs(3600 * 2)),
                remote_compaction: false,
            }),
            storage: Some("S3".to_string()),
            append_mode: false,
            wal_options,
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    ignore_column_ids: vec![1, 2, 3],
                    segment_row_count: 512,
                },
            },
            memtable: Some(MemtableOptions::PartitionTree(PartitionTreeOptions {
                index_max_keys_per_shard: 2048,
                data_freeze_threshold: 2048,
                fork_dictionary_bytes: ReadableSize::mb(128),
            })),
            merge_mode: Some(MergeMode::LastNonNull),
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_region_options_serde() {
        let options = RegionOptions {
            ttl: Some(Duration::from_secs(3600 * 24 * 7)),
            compaction: CompactionOptions::Twcs(TwcsOptions {
                max_active_window_runs: 8,
                max_active_window_files: usize::MAX,
                max_inactive_window_runs: 2,
                max_inactive_window_files: usize::MAX,
                time_window: Some(Duration::from_secs(3600 * 2)),
                remote_compaction: false,
            }),
            storage: Some("S3".to_string()),
            append_mode: false,
            wal_options: WalOptions::Kafka(KafkaWalOptions {
                topic: "test_topic".to_string(),
            }),
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    ignore_column_ids: vec![1, 2, 3],
                    segment_row_count: 512,
                },
            },
            memtable: Some(MemtableOptions::PartitionTree(PartitionTreeOptions {
                index_max_keys_per_shard: 2048,
                data_freeze_threshold: 2048,
                fork_dictionary_bytes: ReadableSize::mb(128),
            })),
            merge_mode: Some(MergeMode::LastNonNull),
        };
        let region_options_json_str = serde_json::to_string(&options).unwrap();
        let got: RegionOptions = serde_json::from_str(&region_options_json_str).unwrap();
        assert_eq!(options, got);
    }

    #[test]
    fn test_region_options_str_serde() {
        // Notes: use empty string for `ignore_column_ids` to test the empty string case.
        let region_options_json_str = r#"{
  "ttl": "7days",
  "compaction": {
    "compaction.type": "twcs",
    "compaction.twcs.max_active_window_runs": "8",
    "compaction.twcs.max_active_window_files": "11",
    "compaction.twcs.max_inactive_window_runs": "2",
    "compaction.twcs.max_inactive_window_files": "7",
    "compaction.twcs.time_window": "2h"
  },
  "storage": "S3",
  "append_mode": false,
  "wal_options": {
    "wal.provider": "kafka",
    "wal.kafka.topic": "test_topic"
  },
  "index_options": {
    "index.inverted_index.ignore_column_ids": "",
    "index.inverted_index.segment_row_count": "512"
  },
  "memtable": {
    "memtable.type": "partition_tree",
    "memtable.partition_tree.index_max_keys_per_shard": "2048",
    "memtable.partition_tree.data_freeze_threshold": "2048",
    "memtable.partition_tree.fork_dictionary_bytes": "128MiB"
  },
  "merge_mode": "last_non_null"
}"#;
        let got: RegionOptions = serde_json::from_str(region_options_json_str).unwrap();
        let options = RegionOptions {
            ttl: Some(Duration::from_secs(3600 * 24 * 7)),
            compaction: CompactionOptions::Twcs(TwcsOptions {
                max_active_window_runs: 8,
                max_active_window_files: 11,
                max_inactive_window_runs: 2,
                max_inactive_window_files: 7,
                time_window: Some(Duration::from_secs(3600 * 2)),
                remote_compaction: false,
            }),
            storage: Some("S3".to_string()),
            append_mode: false,
            wal_options: WalOptions::Kafka(KafkaWalOptions {
                topic: "test_topic".to_string(),
            }),
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    ignore_column_ids: vec![],
                    segment_row_count: 512,
                },
            },
            memtable: Some(MemtableOptions::PartitionTree(PartitionTreeOptions {
                index_max_keys_per_shard: 2048,
                data_freeze_threshold: 2048,
                fork_dictionary_bytes: ReadableSize::mb(128),
            })),
            merge_mode: Some(MergeMode::LastNonNull),
        };
        assert_eq!(options, got);
    }
}
