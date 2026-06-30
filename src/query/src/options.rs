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

use std::collections::HashMap;
use std::path::PathBuf;

use common_base::memory_limit::MemoryLimit;
use common_base::readable_size::ReadableSize;
use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::error::{Error, InvalidQueryContextExtensionSnafu, Result};

pub const FLOW_INCREMENTAL_AFTER_SEQS: &str = "flow.incremental_after_seqs";
pub const FLOW_INCREMENTAL_MODE: &str = "flow.incremental_mode";
pub const FLOW_RETURN_REGION_SEQ: &str = "flow.return_region_seq";
pub const FLOW_SINK_TABLE_ID: &str = "flow.sink_table_id";
/// Enable by default, set to false to explicitly disable.
pub const QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN: &str =
    "query.enable_remote_dynamic_filter_pushdown";

pub const FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY: &str = "memtable_only";

/// Query spill mode controlling disk manager behavior.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuerySpillMode {
    /// Preserve DataFusion default disk manager behavior (OS temp directory).
    Default,
    /// Explicitly configure spill path, quota, and compression.
    Custom,
    /// Explicitly disable disk spilling; temporary file creation will error.
    Disabled,
}

/// Compression for spilled data files.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuerySpillCompression {
    /// No compression (default, matches DataFusion default).
    Uncompressed,
    /// LZ4 frame compression.
    Lz4Frame,
    /// Zstandard compression.
    Zstd,
}

/// Memory pool allocation policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryMemoryPoolPolicy {
    /// Greedy memory allocation (default, preserves current behavior).
    Greedy,
    /// Fair memory allocation: share available memory evenly among operators.
    Fair,
}

/// Query engine config
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct QueryOptions {
    /// Parallelism of query engine. Default to 0, which implies the number of logical CPUs.
    pub parallelism: usize,
    /// Whether to allow query fallback when push down fails.
    pub allow_query_fallback: bool,
    /// Memory pool size for query execution. Setting it to 0 disables the limit (unbounded).
    /// Supports absolute size (e.g., "2GB") or percentage (e.g., "50%").
    /// When this limit is reached, queries will fail with ResourceExhausted error.
    pub memory_pool_size: MemoryLimit,
    /// Whether to expose per-region query load metrics.
    #[serde(skip)]
    pub enable_per_region_metrics: bool,
    /// Experimental: spill-to-disk mode.
    /// - `default`: preserve DataFusion built-in OS temp directory behavior.
    /// - `custom`: explicitly configure spill path, max directory size, and compression.
    /// - `disabled`: explicitly disable disk spilling.
    pub experimental_spill_mode: QuerySpillMode,
    /// Experimental: spill directory path. Ignored unless `experimental_spill_mode` is
    /// `"custom"`. When set, spill files are written into this directory.
    pub experimental_spill_path: Option<PathBuf>,
    /// Experimental: maximum total size of the spill directory (data written to spill files).
    /// Ignored unless `experimental_spill_mode` is `"custom"`. Default: `100GiB`.
    pub experimental_spill_max_temp_directory_size: ReadableSize,
    /// Experimental: compression algorithm applied to spilled data.
    /// Ignored unless `experimental_spill_mode` is `"custom"`. Default: `uncompressed`.
    pub experimental_spill_compression: QuerySpillCompression,
    /// Experimental: memory pool allocation policy.
    /// - `greedy` (default): preserves current behavior.
    /// - `fair`: share memory evenly among spillable operators.
    /// Only effective when `memory_pool_size` is bounded (>0).
    pub experimental_memory_pool_policy: QueryMemoryPoolPolicy,
}

#[allow(clippy::derivable_impls)]
impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            parallelism: 0,
            allow_query_fallback: false,
            memory_pool_size: MemoryLimit::default(),
            enable_per_region_metrics: false,
            experimental_spill_mode: QuerySpillMode::Default,
            experimental_spill_path: None,
            experimental_spill_max_temp_directory_size: ReadableSize::gb(100),
            experimental_spill_compression: QuerySpillCompression::Uncompressed,
            experimental_memory_pool_policy: QueryMemoryPoolPolicy::Greedy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowIncrementalMode {
    MemtableOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FlowQueryExtensions {
    /// Maps region id -> lower exclusive sequence bound for incremental reads.
    pub incremental_after_seqs: Option<HashMap<u64, u64>>,
    /// Incremental read mode requested by the caller.
    pub incremental_mode: Option<FlowIncrementalMode>,
    /// Whether the caller expects per-region watermark metadata in terminal metrics.
    pub return_region_seq: bool,
    /// Optional sink table id used to distinguish source scans from sink reads.
    pub sink_table_id: Option<TableId>,
}

impl FlowQueryExtensions {
    /// Parses flow-specific query extensions when any flow key is present.
    ///
    /// Returns `Ok(None)` for ordinary queries with no flow-related extensions,
    /// `Ok(Some(_))` when flow context is present and valid, and `Err(_)` when a
    /// flow-related extension is present but malformed or incomplete.
    pub fn parse_flow_extensions(extensions: &HashMap<String, String>) -> Result<Option<Self>> {
        let has_flow_context = extensions.contains_key(FLOW_INCREMENTAL_AFTER_SEQS)
            || extensions.contains_key(FLOW_INCREMENTAL_MODE)
            || extensions.contains_key(FLOW_RETURN_REGION_SEQ)
            || extensions.contains_key(FLOW_SINK_TABLE_ID);

        if !has_flow_context {
            return Ok(None);
        }

        let incremental_mode = extensions
            .get(FLOW_INCREMENTAL_MODE)
            .map(|value| match value.as_str() {
                v if v.eq_ignore_ascii_case(FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY) => {
                    Ok(FlowIncrementalMode::MemtableOnly)
                }
                _ => Err(invalid_query_context_extension(format!(
                    "Invalid value for {}: {}",
                    FLOW_INCREMENTAL_MODE, value
                ))),
            })
            .transpose()?;

        let incremental_after_seqs = extensions
            .get(FLOW_INCREMENTAL_AFTER_SEQS)
            .map(|value| parse_incremental_after_seqs(value.as_str()))
            .transpose()?;

        let return_region_seq = extensions
            .get(FLOW_RETURN_REGION_SEQ)
            .map(|value| parse_bool(FLOW_RETURN_REGION_SEQ, value.as_str()))
            .transpose()?
            .unwrap_or(false);

        let sink_table_id = extensions
            .get(FLOW_SINK_TABLE_ID)
            .map(|value| {
                value.parse::<TableId>().map_err(|_| {
                    invalid_query_context_extension(format!(
                        "Invalid value for {}: {}",
                        FLOW_SINK_TABLE_ID, value
                    ))
                })
            })
            .transpose()?;

        if matches!(incremental_mode, Some(FlowIncrementalMode::MemtableOnly)) {
            let after_seqs = incremental_after_seqs.as_ref().ok_or_else(|| {
                invalid_query_context_extension(format!(
                    "{} is required when {}={}.",
                    FLOW_INCREMENTAL_AFTER_SEQS,
                    FLOW_INCREMENTAL_MODE,
                    FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY
                ))
            })?;
            if after_seqs.is_empty() {
                return Err(invalid_query_context_extension(format!(
                    "{} must not be empty when {}={}.",
                    FLOW_INCREMENTAL_AFTER_SEQS,
                    FLOW_INCREMENTAL_MODE,
                    FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY
                )));
            }
        }

        Ok(Some(Self {
            incremental_after_seqs,
            incremental_mode,
            return_region_seq,
            sink_table_id,
        }))
    }

    pub fn validate_for_scan(&self, source_region_id: RegionId) -> Result<bool> {
        if self.sink_table_id.is_some() && self.sink_table_id == Some(source_region_id.table_id()) {
            return Ok(false);
        }

        if matches!(
            self.incremental_mode,
            Some(FlowIncrementalMode::MemtableOnly)
        ) {
            let after_seqs = self.incremental_after_seqs.as_ref().ok_or_else(|| {
                invalid_query_context_extension(format!(
                    "{} is required when {}=memtable_only.",
                    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE
                ))
            })?;

            if !after_seqs.contains_key(&source_region_id.as_u64()) {
                return Err(invalid_query_context_extension(format!(
                    "Missing region {} in {} when {}=memtable_only.",
                    source_region_id, FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE
                )));
            }
        }

        Ok(self.incremental_after_seqs.is_some())
    }

    pub fn should_collect_region_watermark(&self) -> bool {
        should_collect_region_watermark(
            self.return_region_seq,
            self.incremental_after_seqs.is_some(),
        )
    }
}

/// Returns whether query-level remote dynamic filter propagation is enabled.
///
/// The option defaults to enabled to preserve existing behavior. Callers may set
/// `query.enable_remote_dynamic_filter_pushdown=false` in query context
/// extensions to disable FE->DN remote dynamic filter propagation for a single
/// query.
pub fn remote_dyn_filter_pushdown_enabled_from_extensions(
    extensions: &HashMap<String, String>,
) -> Result<bool> {
    extensions
        .get(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN)
        .map(|value| parse_bool(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN, value.as_str()))
        .transpose()
        .map(|value| value.unwrap_or(true))
}

/// Returns whether raw Flow query extensions request terminal region watermark collection.
///
/// This is only an intent/presence check for transport/scan plumbing; callers that need
/// validated Flow options must still use [`FlowQueryExtensions::parse_flow_extensions`].
pub fn should_collect_region_watermark_from_extensions(
    extensions: &HashMap<String, String>,
) -> bool {
    let return_region_seq = extensions
        .get(FLOW_RETURN_REGION_SEQ)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let has_incremental_after_seqs = extensions.contains_key(FLOW_INCREMENTAL_AFTER_SEQS);

    should_collect_region_watermark(return_region_seq, has_incremental_after_seqs)
}

fn should_collect_region_watermark(
    return_region_seq: bool,
    has_incremental_after_seqs: bool,
) -> bool {
    return_region_seq || has_incremental_after_seqs
}

fn parse_incremental_after_seqs(value: &str) -> Result<HashMap<u64, u64>> {
    let raw = serde_json::from_str::<HashMap<String, serde_json::Value>>(value).map_err(|e| {
        invalid_query_context_extension(format!(
            "Invalid JSON for {}: {} ({})",
            FLOW_INCREMENTAL_AFTER_SEQS, value, e
        ))
    })?;

    raw.into_iter()
        .map(|(region_id, raw_seq)| {
            let region_id = region_id.parse::<u64>().map_err(|_| {
                invalid_query_context_extension(format!(
                    "Invalid region id in {}: {}",
                    FLOW_INCREMENTAL_AFTER_SEQS, region_id
                ))
            })?;

            let seq = match raw_seq {
                serde_json::Value::Number(num) => num.as_u64().ok_or_else(|| {
                    invalid_query_context_extension(format!(
                        "Invalid sequence value in {} for region {}: {}",
                        FLOW_INCREMENTAL_AFTER_SEQS, region_id, num
                    ))
                })?,
                serde_json::Value::String(s) => s.parse::<u64>().map_err(|_| {
                    invalid_query_context_extension(format!(
                        "Invalid sequence string in {} for region {}: {}",
                        FLOW_INCREMENTAL_AFTER_SEQS, region_id, s
                    ))
                })?,
                _ => {
                    return Err(invalid_query_context_extension(format!(
                        "Invalid sequence value type in {} for region {}",
                        FLOW_INCREMENTAL_AFTER_SEQS, region_id
                    )));
                }
            };

            Ok((region_id, seq))
        })
        .collect()
}

fn parse_bool(option_name: &str, value: &str) -> Result<bool> {
    match value {
        v if v.eq_ignore_ascii_case("true") => Ok(true),
        v if v.eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(invalid_query_context_extension(format!(
            "Invalid value for {}: {}",
            option_name, value
        ))),
    }
}

fn invalid_query_context_extension(reason: String) -> Error {
    InvalidQueryContextExtensionSnafu { reason }.build()
}

#[cfg(test)]
mod flow_extension_tests {
    use super::*;

    #[test]
    fn test_parse_flow_extensions_returns_none_for_non_flow_query() {
        let exts = HashMap::new();
        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap();

        assert_eq!(parsed, None);
    }

    #[test]
    fn test_remote_dyn_filter_pushdown_enabled_from_extensions_defaults_true() {
        assert!(remote_dyn_filter_pushdown_enabled_from_extensions(&HashMap::new()).unwrap());
    }

    #[test]
    fn test_remote_dyn_filter_pushdown_enabled_from_extensions_parses_bool() {
        let exts = HashMap::from([(
            QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN.to_string(),
            "false".to_string(),
        )]);
        assert!(!remote_dyn_filter_pushdown_enabled_from_extensions(&exts).unwrap());

        let exts = HashMap::from([(
            QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN.to_string(),
            "true".to_string(),
        )]);
        assert!(remote_dyn_filter_pushdown_enabled_from_extensions(&exts).unwrap());
    }

    #[test]
    fn test_remote_dyn_filter_pushdown_enabled_from_extensions_rejects_invalid_bool() {
        let exts = HashMap::from([(
            QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN.to_string(),
            "invalid".to_string(),
        )]);

        let err = remote_dyn_filter_pushdown_enabled_from_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN));
    }

    #[test]
    fn test_parse_flow_extensions_memtable_only_success() {
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                r#"{"1":10,"2":20}"#.to_string(),
            ),
            (FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string()),
            (FLOW_SINK_TABLE_ID.to_string(), "1024".to_string()),
        ]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed.incremental_mode,
            Some(FlowIncrementalMode::MemtableOnly)
        );
        assert_eq!(
            parsed.incremental_after_seqs.unwrap(),
            HashMap::from([(1, 10), (2, 20)])
        );
        assert!(parsed.return_region_seq);
        assert_eq!(parsed.sink_table_id, Some(1024));
    }

    #[test]
    fn test_parse_flow_extensions_mode_requires_after_seqs() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_MODE.to_string(),
            FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
        )]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS));
    }

    #[test]
    fn test_parse_flow_extensions_invalid_mode() {
        let exts = HashMap::from([(FLOW_INCREMENTAL_MODE.to_string(), "foo".to_string())]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_INCREMENTAL_MODE));
    }

    #[test]
    fn test_parse_flow_extensions_invalid_after_seqs_json() {
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                "not-json".to_string(),
            ),
        ]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS));
    }

    #[test]
    fn test_parse_flow_extensions_after_seqs_string_values() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"1":"10","2":"20"}"#.to_string(),
        )]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed.incremental_after_seqs.unwrap(),
            HashMap::from([(1, 10), (2, 20)])
        );
    }

    #[test]
    fn test_parse_flow_extensions_after_seqs_invalid_value_type() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"1":true}"#.to_string(),
        )]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS));
    }

    #[test]
    fn test_parse_flow_extensions_invalid_sink_table_id() {
        let exts = HashMap::from([(FLOW_SINK_TABLE_ID.to_string(), "x".to_string())]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_SINK_TABLE_ID));
    }

    #[test]
    fn test_validate_for_scan_missing_source_region() {
        let source_region_id = RegionId::new(100, 2);
        let existing_region_id = RegionId::new(100, 1);
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                format!(r#"{{"{}":10}}"#, existing_region_id.as_u64()),
            ),
        ]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        let err = parsed.validate_for_scan(source_region_id).unwrap_err();
        assert!(format!("{err}").contains("Missing region"));
    }

    #[test]
    fn test_validate_for_scan_sink_table_excluded() {
        let source_region_id = RegionId::new(1024, 1);
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                format!(r#"{{"{}":10}}"#, source_region_id.as_u64()),
            ),
            (FLOW_SINK_TABLE_ID.to_string(), "1024".to_string()),
        ]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        let apply_incremental = parsed.validate_for_scan(source_region_id).unwrap();
        assert!(!apply_incremental);
    }

    #[test]
    fn test_should_collect_region_watermark_defaults_false() {
        let parsed = FlowQueryExtensions::default();
        assert!(!parsed.should_collect_region_watermark());
    }

    #[test]
    fn test_should_collect_region_watermark_true_for_return_region_seq() {
        let parsed = FlowQueryExtensions {
            return_region_seq: true,
            ..Default::default()
        };
        assert!(parsed.should_collect_region_watermark());
    }

    #[test]
    fn test_should_collect_region_watermark_true_for_incremental_query() {
        let parsed = FlowQueryExtensions {
            incremental_after_seqs: Some(HashMap::from([(1, 10)])),
            ..Default::default()
        };
        assert!(parsed.should_collect_region_watermark());
    }

    #[test]
    fn test_should_collect_region_watermark_from_extensions() {
        let exts = HashMap::from([(FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string())]);
        assert!(should_collect_region_watermark_from_extensions(&exts));

        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"1":10}"#.to_string(),
        )]);
        assert!(should_collect_region_watermark_from_extensions(&exts));

        let exts = HashMap::from([(FLOW_RETURN_REGION_SEQ.to_string(), "false".to_string())]);
        assert!(!should_collect_region_watermark_from_extensions(&exts));
        assert!(!should_collect_region_watermark_from_extensions(
            &HashMap::new()
        ));
    }

    #[test]
    fn test_parse_flow_extensions_return_region_seq_only_returns_some() {
        let exts = HashMap::from([(FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string())]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert!(parsed.return_region_seq);
    }

    #[test]
    fn test_parse_flow_extensions_sink_table_only_returns_some() {
        let exts = HashMap::from([(FLOW_SINK_TABLE_ID.to_string(), "1024".to_string())]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert_eq!(parsed.sink_table_id, Some(1024));
    }

    #[test]
    fn test_parse_flow_extensions_incremental_after_seqs_only_returns_some() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"1":10}"#.to_string(),
        )]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert_eq!(
            parsed.incremental_after_seqs,
            Some(HashMap::from([(1, 10)]))
        );
    }
}

#[cfg(test)]
mod query_options_tests {
    use super::*;

    #[test]
    fn test_query_options_defaults() {
        let opts = QueryOptions::default();
        assert_eq!(opts.parallelism, 0);
        assert!(!opts.allow_query_fallback);
        assert!(opts.memory_pool_size.is_unlimited());
        assert!(!opts.enable_per_region_metrics);
        assert_eq!(opts.experimental_spill_mode, QuerySpillMode::Default);
        assert_eq!(opts.experimental_spill_path, None);
        assert_eq!(
            opts.experimental_spill_max_temp_directory_size,
            ReadableSize::gb(100)
        );
        assert_eq!(
            opts.experimental_spill_compression,
            QuerySpillCompression::Uncompressed
        );
        assert_eq!(
            opts.experimental_memory_pool_policy,
            QueryMemoryPoolPolicy::Greedy
        );
    }

    #[test]
    fn test_selected_defaults_parse() {
        let toml_str = "";
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        let def = QueryOptions::default();
        assert_eq!(opts, def);
    }

    #[test]
    fn test_parse_experimental_spill_mode() {
        let toml_str = r#"experimental_spill_mode = "custom""#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(opts.experimental_spill_mode, QuerySpillMode::Custom);

        let toml_str = r#"experimental_spill_mode = "disabled""#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(opts.experimental_spill_mode, QuerySpillMode::Disabled);

        let toml_str = r#"experimental_spill_mode = "default""#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(opts.experimental_spill_mode, QuerySpillMode::Default);
    }

    #[test]
    fn test_parse_invalid_spill_mode() {
        let toml_str = r#"experimental_spill_mode = "invalid""#;
        assert!(toml::from_str::<QueryOptions>(toml_str).is_err());
    }

    #[test]
    fn test_parse_experimental_spill_compression() {
        let toml_str = r#"experimental_spill_compression = "zstd""#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(
            opts.experimental_spill_compression,
            QuerySpillCompression::Zstd
        );

        let toml_str = r#"experimental_spill_compression = "lz4_frame""#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(
            opts.experimental_spill_compression,
            QuerySpillCompression::Lz4Frame
        );
    }

    #[test]
    fn test_parse_invalid_spill_compression() {
        let toml_str = r#"experimental_spill_compression = "gzip""#;
        assert!(toml::from_str::<QueryOptions>(toml_str).is_err());
    }

    #[test]
    fn test_parse_experimental_memory_pool_policy() {
        let toml_str = r#"experimental_memory_pool_policy = "fair""#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(
            opts.experimental_memory_pool_policy,
            QueryMemoryPoolPolicy::Fair
        );

        let toml_str = r#"experimental_memory_pool_policy = "greedy""#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(
            opts.experimental_memory_pool_policy,
            QueryMemoryPoolPolicy::Greedy
        );
    }

    #[test]
    fn test_parse_invalid_memory_pool_policy() {
        let toml_str = r#"experimental_memory_pool_policy = "none""#;
        assert!(toml::from_str::<QueryOptions>(toml_str).is_err());
    }

    #[test]
    fn test_parse_experimental_spill_path_and_quota() {
        let toml_str = r#"
experimental_spill_path = "/tmp/spill"
experimental_spill_max_temp_directory_size = "50GiB"
"#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(
            opts.experimental_spill_path,
            Some(PathBuf::from("/tmp/spill"))
        );
        assert_eq!(
            opts.experimental_spill_max_temp_directory_size,
            ReadableSize::gb(50)
        );
    }

    #[test]
    fn test_parse_experimental_spill_path_none_when_absent() {
        let toml_str = r#"experimental_spill_mode = "custom""#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(opts.experimental_spill_path, None);
    }

    /// Verify that `experimental_spill_compression` only takes effect
    /// when paired with `experimental_spill_mode = "custom"`. This test
    /// asserts the config can be parsed regardless, but the semantics
    /// in `state.rs` enforce that compression is only applied in Custom mode.
    #[test]
    fn test_parse_experimental_spill_compression_without_custom_mode() {
        let toml_str = r#"experimental_spill_compression = "zstd""#;
        // Should parse fine, even though semantically compression
        // is ignored unless mode is "custom".
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(
            opts.experimental_spill_compression,
            QuerySpillCompression::Zstd
        );
        assert_eq!(opts.experimental_spill_mode, QuerySpillMode::Default);
    }

    #[test]
    fn test_parse_experimental_spill_max_temp_directory_size_default() {
        let toml_str = "";
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(
            opts.experimental_spill_max_temp_directory_size,
            ReadableSize::gb(100)
        );
    }

    #[test]
    fn test_parse_experimental_spill_max_temp_directory_size_human_readable() {
        let toml_str = r#"experimental_spill_max_temp_directory_size = "2GB""#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(
            opts.experimental_spill_max_temp_directory_size,
            ReadableSize::gb(2)
        );
    }

    #[test]
    fn test_query_spill_mode_serde_roundtrip() {
        let modes = [
            QuerySpillMode::Default,
            QuerySpillMode::Custom,
            QuerySpillMode::Disabled,
        ];
        for mode in modes {
            let json = serde_json::to_string(&mode).unwrap();
            let roundtripped: QuerySpillMode = serde_json::from_str(&json).unwrap();
            assert_eq!(mode, roundtripped);
        }
    }

    #[test]
    fn test_query_spill_compression_serde_roundtrip() {
        let compressions = [
            QuerySpillCompression::Uncompressed,
            QuerySpillCompression::Lz4Frame,
            QuerySpillCompression::Zstd,
        ];
        for comp in compressions {
            let json = serde_json::to_string(&comp).unwrap();
            let roundtripped: QuerySpillCompression = serde_json::from_str(&json).unwrap();
            assert_eq!(comp, roundtripped);
        }
    }

    #[test]
    fn test_query_memory_pool_policy_serde_roundtrip() {
        let policies = [QueryMemoryPoolPolicy::Greedy, QueryMemoryPoolPolicy::Fair];
        for policy in policies {
            let json = serde_json::to_string(&policy).unwrap();
            let roundtripped: QueryMemoryPoolPolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(policy, roundtripped);
        }
    }
}
