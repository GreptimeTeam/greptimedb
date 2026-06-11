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
use common_telemetry::info;
use common_time::TimeToLive;
use common_wal::options::{WAL_OPTIONS_KEY, WalOptions};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use serde_with::{DisplayFromStr, NoneAsEmptyString, serde_as, with_prefix};
use snafu::{ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::{
    MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING, PRIMARY_KEY_ENCODING,
};
use store_api::mito_engine_options::COMPACTION_OVERRIDE;
use store_api::storage::{ColumnId, RegionId};
use strum::EnumString;

use crate::error::{InvalidRegionOptionsSnafu, JsonOptionsSnafu, Result};
use crate::memtable::bulk::BulkMemtableConfig;
use crate::sst::FormatType;

const DEFAULT_INDEX_SEGMENT_ROW_COUNT: usize = 1024;
const COMPACTION_TWCS_PREFIX: &str = "compaction.twcs.";
const MEMTABLE_PARTITION_TREE_PREFIX: &str = "memtable.partition_tree.";
const MEMTABLE_BULK_PREFIX: &str = "memtable.bulk.";

/// Legacy memtable type identifier accepted for backward compatibility.
/// The partition tree memtable has been removed; parsing this value falls
/// back to the default (bulk) memtable at runtime.
const LEGACY_PARTITION_TREE_MEMTABLE_TYPE: &str = "partition_tree";

pub(crate) fn parse_wal_options(
    options_map: &HashMap<String, String>,
) -> std::result::Result<WalOptions, serde_json::Error> {
    options_map
        .get(WAL_OPTIONS_KEY)
        .map_or(Ok(WalOptions::default()), |encoded_wal_options| {
            serde_json::from_str(encoded_wal_options)
        })
}

/// Mode to handle duplicate rows while merging.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumString)]
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
    pub ttl: Option<TimeToLive>,
    /// Compaction options.
    pub compaction: CompactionOptions,
    pub compaction_override: bool,
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
    /// SST format type.
    pub sst_format: Option<FormatType>,
    /// Internal primary key encoding override used by metric-engine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key_encoding: Option<PrimaryKeyEncoding>,
}

impl RegionOptions {
    /// Validates options.
    pub fn validate(&self) -> Result<()> {
        if self.append_mode {
            ensure!(
                self.merge_mode
                    .is_none_or(|mode| mode == MergeMode::LastRow),
                InvalidRegionOptionsSnafu {
                    reason: "only last_row merge_mode is allowed when append_mode is enabled",
                }
            );
        }
        Ok(())
    }

    /// Returns `true` if deduplication is needed.
    pub fn need_dedup(&self) -> bool {
        !self.append_mode
    }

    /// Returns the `merge_mode` if it is set, otherwise returns the default [`MergeMode`].
    pub fn merge_mode(&self) -> MergeMode {
        self.merge_mode.unwrap_or_default()
    }

    /// Returns the `primary_key_encoding` if it is set, otherwise returns the default [`PrimaryKeyEncoding`].
    pub fn primary_key_encoding(&self) -> PrimaryKeyEncoding {
        self.primary_key_encoding.unwrap_or_default()
    }
}

impl RegionOptions {
    /// Parses [RegionOptions] from the raw `options_map`.
    pub fn try_from_options(
        region_id: RegionId,
        options_map: &HashMap<String, String>,
    ) -> Result<Self> {
        let value = options_map_to_value(options_map);
        let json = serde_json::to_string(&value).context(JsonOptionsSnafu)?;

        // #[serde(flatten)] doesn't work with #[serde(default)] so we need to parse
        // each field manually instead of using #[serde(flatten)] for `compaction`.
        // See https://github.com/serde-rs/serde/issues/1626
        let options: RegionOptionsWithoutEnum =
            serde_json::from_str(&json).context(JsonOptionsSnafu)?;
        let has_compaction_type =
            validate_enum_options(options_map, "compaction.type", &[COMPACTION_TWCS_PREFIX])?;
        let compaction = if has_compaction_type {
            serde_json::from_str(&json).context(JsonOptionsSnafu)?
        } else {
            CompactionOptions::default()
        };

        let wal_options = parse_wal_options(options_map).context(JsonOptionsSnafu)?;

        let index_options: IndexOptions = serde_json::from_str(&json).context(JsonOptionsSnafu)?;
        let is_legacy_partition_tree = options_map
            .get("memtable.type")
            .map(|s| s.eq_ignore_ascii_case(LEGACY_PARTITION_TREE_MEMTABLE_TYPE))
            .unwrap_or(false);
        let memtable = if validate_enum_options(
            options_map,
            "memtable.type",
            &[MEMTABLE_PARTITION_TREE_PREFIX, MEMTABLE_BULK_PREFIX],
        )? {
            if is_legacy_partition_tree {
                // The partition tree memtable has been removed. Fall back to the
                // default memtable; the primary key encoding (if any) is still
                // read separately below from the legacy nested key.
                None
            } else {
                Some(serde_json::from_str(&json).context(JsonOptionsSnafu)?)
            }
        } else {
            None
        };

        // The partition tree memtable has been removed. Besides falling back to
        // the default memtable, also override the SST format to flat.
        let mut sst_format = options.sst_format;
        if is_legacy_partition_tree {
            info!(
                "Region {} specified the removed partition_tree memtable; \
                 overriding memtable to the default and SST format to flat",
                region_id
            );
            sst_format = Some(FormatType::Flat);
        }

        // Bulk memtable produces flat-encoded ranges and flushes them through
        // `put_sst()`, so the SST format must be flat to match.
        if matches!(memtable, Some(MemtableOptions::Bulk(_))) {
            if let Some(format) = sst_format
                && format != FormatType::Flat
            {
                info!(
                    "Region {} uses bulk memtable; overriding sst_format from {:?} to flat",
                    region_id, format
                );
            }
            sst_format = Some(FormatType::Flat);
        }

        let compaction_override_flag = options_map
            .get(COMPACTION_OVERRIDE)
            .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1"))
            .unwrap_or(false);
        let compaction_override = has_compaction_type || compaction_override_flag;
        let primary_key_encoding = options_map
            .get(PRIMARY_KEY_ENCODING)
            .or_else(|| options_map.get(MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING))
            .map(|v| match v.to_lowercase().as_str() {
                "dense" => Ok(PrimaryKeyEncoding::Dense),
                "sparse" => Ok(PrimaryKeyEncoding::Sparse),
                _ => Err(InvalidRegionOptionsSnafu {
                    reason: format!("Invalid primary key encoding: {v}"),
                }
                .build()),
            })
            .transpose()?;

        let opts = RegionOptions {
            ttl: options.ttl,
            compaction,
            compaction_override,
            storage: options.storage,
            append_mode: options.append_mode,
            wal_options,
            index_options,
            memtable,
            merge_mode: options.merge_mode,
            sst_format,
            primary_key_encoding,
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

    pub(crate) fn fallback_to_local(&self) -> bool {
        match self {
            CompactionOptions::Twcs(opts) => opts.fallback_to_local,
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
    /// Minimum file num in every time window to trigger a compaction.
    #[serde_as(as = "DisplayFromStr")]
    pub trigger_file_num: usize,
    /// Compaction time window defined when creating tables.
    #[serde(with = "humantime_serde")]
    pub time_window: Option<Duration>,
    /// Compaction time window defined when creating tables.
    pub max_output_file_size: Option<ReadableSize>,
    /// Whether to use remote compaction.
    #[serde_as(as = "DisplayFromStr")]
    pub remote_compaction: bool,
    /// Whether to fall back to local compaction if remote compaction fails.
    #[serde_as(as = "DisplayFromStr")]
    pub fallback_to_local: bool,
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
            trigger_file_num: 4,
            time_window: None,
            max_output_file_size: Some(ReadableSize::mb(512)),
            remote_compaction: false,
            fallback_to_local: true,
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
    ttl: Option<TimeToLive>,
    storage: Option<String>,
    #[serde_as(as = "DisplayFromStr")]
    append_mode: bool,
    #[serde_as(as = "NoneAsEmptyString")]
    merge_mode: Option<MergeMode>,
    #[serde_as(as = "NoneAsEmptyString")]
    sst_format: Option<FormatType>,
}

impl Default for RegionOptionsWithoutEnum {
    fn default() -> Self {
        let options = RegionOptions::default();
        RegionOptionsWithoutEnum {
            ttl: options.ttl,
            storage: options.storage,
            append_mode: options.append_mode,
            merge_mode: options.merge_mode,
            sst_format: options.sst_format,
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
    #[serde(with = "prefix_bulk")]
    Bulk(BulkMemtableConfig),
}

with_prefix!(prefix_bulk "memtable.bulk.");

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
                (key.clone(), Value::Null)
            } else {
                (key.clone(), Value::from(value.clone()))
            }
        })
        .collect();
    Value::Object(map)
}

// `#[serde(default)]` doesn't support enum (https://github.com/serde-rs/serde/issues/1799) so we
// check the type key first.
/// Validates whether the `options_map` has valid options for specific `enum_tag_key`
/// and returns `true` if the map contains the enum tag.
///
/// Variant options must start with one of `enum_option_prefixes`. If variant options
/// are provided, the tagged enum type key must also be provided.
fn validate_enum_options(
    options_map: &HashMap<String, String>,
    enum_tag_key: &str,
    enum_option_prefixes: &[&str],
) -> Result<bool> {
    let mut has_enum_options = false;
    let mut has_tag = false;
    for key in options_map.keys() {
        if key == enum_tag_key {
            has_tag = true;
        } else if !has_enum_options
            && enum_option_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
        {
            has_enum_options = true;
        }

        if has_tag && has_enum_options {
            break;
        }
    }

    // If tag is not provided, then other options for the enum should not exist.
    ensure!(
        has_tag || !has_enum_options,
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
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        assert_eq!(RegionOptions::default(), options);
    }

    #[test]
    fn test_with_ttl() {
        let map = make_map(&[("ttl", "7d")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        let expect = RegionOptions {
            ttl: Some(Duration::from_secs(3600 * 24 * 7).into()),
            ..Default::default()
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_with_storage() {
        let map = make_map(&[("storage", "S3")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        let expect = RegionOptions {
            storage: Some("S3".to_string()),
            ..Default::default()
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_without_compaction_type() {
        let map = make_map(&[
            ("compaction.twcs.trigger_file_num", "8"),
            ("compaction.twcs.time_window", "2h"),
        ]);
        let err = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_with_compaction_type() {
        let map = make_map(&[
            ("compaction.twcs.trigger_file_num", "8"),
            ("compaction.twcs.time_window", "2h"),
            ("compaction.type", "twcs"),
        ]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        let expect = RegionOptions {
            compaction: CompactionOptions::Twcs(TwcsOptions {
                trigger_file_num: 8,
                time_window: Some(Duration::from_secs(3600 * 2)),
                ..Default::default()
            }),
            compaction_override: true,
            ..Default::default()
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_with_compaction_override_true_without_compaction_type() {
        let map = make_map(&[(COMPACTION_OVERRIDE, "true")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        let expect = RegionOptions {
            compaction_override: true,
            ..Default::default()
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_with_compaction_override_false_without_compaction_type() {
        let map = make_map(&[(COMPACTION_OVERRIDE, "false")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        assert_eq!(RegionOptions::default(), options);
    }

    #[test]
    fn test_compaction_twcs_options_still_require_compaction_type_with_override() {
        let map = make_map(&[
            (COMPACTION_OVERRIDE, "true"),
            ("compaction.twcs.time_window", "2h"),
        ]);
        let err = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    fn test_with_wal_options(wal_options: &WalOptions) -> bool {
        let encoded_wal_options = serde_json::to_string(&wal_options).unwrap();
        let map = make_map(&[(WAL_OPTIONS_KEY, &encoded_wal_options)]);
        let got = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
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
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
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
            WalOptions::Kafka(KafkaWalOptions::new("test_topic".to_string())),
        ];
        all_wal_options.iter().all(test_with_wal_options);
    }

    #[test]
    fn test_with_memtable() {
        let map = make_map(&[("memtable.type", "time_series")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        let expect = RegionOptions {
            memtable: Some(MemtableOptions::TimeSeries),
            ..Default::default()
        };
        assert_eq!(expect, options);

        let map = make_map(&[("memtable.type", "bulk")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        let expect = RegionOptions {
            memtable: Some(MemtableOptions::Bulk(BulkMemtableConfig::default())),
            sst_format: Some(FormatType::Flat),
            ..Default::default()
        };
        assert_eq!(expect, options);

        let map = make_map(&[
            ("memtable.type", "bulk"),
            ("memtable.bulk.merge_threshold", "7"),
            ("memtable.bulk.encode_row_threshold", "11"),
            ("memtable.bulk.encode_bytes_threshold", "13"),
            ("memtable.bulk.max_merge_groups", "17"),
        ]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        let expect = RegionOptions {
            memtable: Some(MemtableOptions::Bulk(BulkMemtableConfig {
                merge_threshold: 7,
                encode_row_threshold: 11,
                encode_bytes_threshold: 13,
                max_merge_groups: 17,
            })),
            sst_format: Some(FormatType::Flat),
            ..Default::default()
        };
        assert_eq!(expect, options);

        // Legacy partition_tree memtable falls back to the default memtable and
        // overrides the SST format to flat.
        let map = make_map(&[("memtable.type", "partition_tree")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        let expect = RegionOptions {
            memtable: None,
            sst_format: Some(FormatType::Flat),
            ..Default::default()
        };
        assert_eq!(expect, options);

        // Legacy partition_tree options are tolerated alongside the type tag.
        let map = make_map(&[
            ("memtable.type", "partition_tree"),
            ("memtable.partition_tree.index_max_keys_per_shard", "2048"),
            ("memtable.partition_tree.fork_dictionary_bytes", "128M"),
        ]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        let expect = RegionOptions {
            memtable: None,
            sst_format: Some(FormatType::Flat),
            ..Default::default()
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_primary_key_encoding() {
        // New top-level key.
        let map = make_map(&[("primary_key_encoding", "sparse")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        assert_eq!(options.primary_key_encoding(), PrimaryKeyEncoding::Sparse);
        assert_eq!(
            options.primary_key_encoding,
            Some(PrimaryKeyEncoding::Sparse)
        );

        // Legacy memtable.type=partition_tree + legacy encoding.
        let map = make_map(&[
            ("memtable.type", "partition_tree"),
            ("memtable.partition_tree.primary_key_encoding", "sparse"),
        ]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        assert_eq!(options.memtable, None);
        assert_eq!(options.sst_format, Some(FormatType::Flat));
        assert_eq!(options.primary_key_encoding(), PrimaryKeyEncoding::Sparse);

        // Invalid value rejected.
        let map = make_map(&[("primary_key_encoding", "bogus")]);
        let err = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_legacy_partition_tree_overrides_sst_format() {
        // Legacy partition_tree memtable falls back to the default memtable and
        // overrides the SST format to flat, even when a different format was set.
        let map = make_map(&[
            ("memtable.type", "partition_tree"),
            ("sst_format", "primary_key"),
        ]);
        let options = RegionOptions::try_from_options(RegionId::new(1, 1), &map).unwrap();
        assert_eq!(options.memtable, None);
        assert_eq!(options.sst_format, Some(FormatType::Flat));
    }

    #[test]
    fn test_bulk_memtable_overrides_sst_format() {
        // Bulk memtable produces flat-encoded ranges, so an explicit
        // `sst_format=primary_key` must be overridden to flat to keep the
        // in-memory and on-disk encodings in sync.
        let map = make_map(&[("memtable.type", "bulk"), ("sst_format", "primary_key")]);
        let options = RegionOptions::try_from_options(RegionId::new(1, 1), &map).unwrap();
        assert_eq!(
            options.memtable,
            Some(MemtableOptions::Bulk(BulkMemtableConfig::default()))
        );
        assert_eq!(options.sst_format, Some(FormatType::Flat));
    }

    #[test]
    fn test_unknown_memtable_type() {
        let map = make_map(&[("memtable.type", "no_such_memtable")]);
        let err = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_without_memtable_type() {
        let map = make_map(&[("memtable.partition_tree.index_max_keys_per_shard", "2048")]);
        let err = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());

        let map = make_map(&[("memtable.bulk.merge_threshold", "7")]);
        let err = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_with_merge_mode() {
        let map = make_map(&[("merge_mode", "last_row")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        assert_eq!(MergeMode::LastRow, options.merge_mode());

        let map = make_map(&[("merge_mode", "last_non_null")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        assert_eq!(MergeMode::LastNonNull, options.merge_mode());

        let map = make_map(&[("merge_mode", "unknown")]);
        let err = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_append_mode_allows_last_row_merge_mode() {
        let map = make_map(&[("append_mode", "true"), ("merge_mode", "last_row")]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        assert!(options.append_mode);
        assert_eq!(MergeMode::LastRow, options.merge_mode());

        let map = make_map(&[("append_mode", "true"), ("merge_mode", "last_non_null")]);
        let err = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_with_all() {
        let wal_options = WalOptions::Kafka(KafkaWalOptions::new("test_topic".to_string()));
        let map = make_map(&[
            ("ttl", "7d"),
            ("compaction.twcs.trigger_file_num", "8"),
            ("compaction.twcs.max_output_file_size", "1GB"),
            ("compaction.twcs.time_window", "2h"),
            ("compaction.type", "twcs"),
            ("compaction.twcs.remote_compaction", "false"),
            ("compaction.twcs.fallback_to_local", "true"),
            ("storage", "S3"),
            ("append_mode", "false"),
            ("index.inverted_index.ignore_column_ids", "1,2,3"),
            ("index.inverted_index.segment_row_count", "512"),
            (
                WAL_OPTIONS_KEY,
                &serde_json::to_string(&wal_options).unwrap(),
            ),
            ("memtable.type", "bulk"),
            ("memtable.bulk.merge_threshold", "7"),
            ("memtable.bulk.encode_row_threshold", "11"),
            ("memtable.bulk.encode_bytes_threshold", "13"),
            ("memtable.bulk.max_merge_groups", "17"),
            ("merge_mode", "last_non_null"),
        ]);
        let options = RegionOptions::try_from_options(RegionId::new(0, 0), &map).unwrap();
        let expect = RegionOptions {
            ttl: Some(Duration::from_secs(3600 * 24 * 7).into()),
            compaction: CompactionOptions::Twcs(TwcsOptions {
                trigger_file_num: 8,
                time_window: Some(Duration::from_secs(3600 * 2)),
                max_output_file_size: Some(ReadableSize::gb(1)),
                remote_compaction: false,
                fallback_to_local: true,
            }),
            compaction_override: true,
            storage: Some("S3".to_string()),
            append_mode: false,
            wal_options,
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    ignore_column_ids: vec![1, 2, 3],
                    segment_row_count: 512,
                },
            },
            memtable: Some(MemtableOptions::Bulk(BulkMemtableConfig {
                merge_threshold: 7,
                encode_row_threshold: 11,
                encode_bytes_threshold: 13,
                max_merge_groups: 17,
            })),
            merge_mode: Some(MergeMode::LastNonNull),
            sst_format: Some(FormatType::Flat),
            primary_key_encoding: None,
        };
        assert_eq!(expect, options);
    }

    #[test]
    fn test_region_options_serde() {
        let options = RegionOptions {
            ttl: Some(Duration::from_secs(3600 * 24 * 7).into()),
            compaction: CompactionOptions::Twcs(TwcsOptions {
                trigger_file_num: 8,
                time_window: Some(Duration::from_secs(3600 * 2)),
                max_output_file_size: None,
                remote_compaction: false,
                fallback_to_local: true,
            }),
            compaction_override: false,
            storage: Some("S3".to_string()),
            append_mode: false,
            wal_options: WalOptions::Kafka(KafkaWalOptions::new("test_topic".to_string())),
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    ignore_column_ids: vec![1, 2, 3],
                    segment_row_count: 512,
                },
            },
            memtable: Some(MemtableOptions::Bulk(BulkMemtableConfig::default())),
            merge_mode: Some(MergeMode::LastNonNull),
            sst_format: None,
            primary_key_encoding: None,
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
    "compaction.twcs.trigger_file_num": "8",
    "compaction.twcs.max_output_file_size": "7MB",
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
    "memtable.type": "bulk"
  },
  "merge_mode": "last_non_null"
}"#;
        let got: RegionOptions = serde_json::from_str(region_options_json_str).unwrap();
        let options = RegionOptions {
            ttl: Some(Duration::from_secs(3600 * 24 * 7).into()),
            compaction: CompactionOptions::Twcs(TwcsOptions {
                trigger_file_num: 8,
                time_window: Some(Duration::from_secs(3600 * 2)),
                max_output_file_size: Some(ReadableSize::mb(7)),
                remote_compaction: false,
                fallback_to_local: true,
            }),
            compaction_override: false,
            storage: Some("S3".to_string()),
            append_mode: false,
            wal_options: WalOptions::Kafka(KafkaWalOptions::new("test_topic".to_string())),
            index_options: IndexOptions {
                inverted_index: InvertedIndexOptions {
                    ignore_column_ids: vec![],
                    segment_row_count: 512,
                },
            },
            memtable: Some(MemtableOptions::Bulk(BulkMemtableConfig::default())),
            merge_mode: Some(MergeMode::LastNonNull),
            sst_format: None,
            primary_key_encoding: None,
        };
        assert_eq!(options, got);
    }
}
