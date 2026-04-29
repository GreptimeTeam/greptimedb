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

use common_base::memory_limit::MemoryLimit;
use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::error::{Error, InvalidQueryContextExtensionSnafu, Result};

pub const FLOW_INCREMENTAL_AFTER_SEQS: &str = "flow.incremental_after_seqs";
pub const FLOW_INCREMENTAL_MODE: &str = "flow.incremental_mode";
pub const FLOW_RETURN_REGION_SEQ: &str = "flow.return_region_seq";
pub const FLOW_SINK_TABLE_ID: &str = "flow.sink_table_id";

pub const FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY: &str = "memtable_only";

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
}

#[allow(clippy::derivable_impls)]
impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            parallelism: 0,
            allow_query_fallback: false,
            memory_pool_size: MemoryLimit::default(),
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
            .map(|value| parse_bool(value.as_str()))
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

fn parse_bool(value: &str) -> Result<bool> {
    match value {
        v if v.eq_ignore_ascii_case("true") => Ok(true),
        v if v.eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(invalid_query_context_extension(format!(
            "Invalid value for {}: {}",
            FLOW_RETURN_REGION_SEQ, value
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
