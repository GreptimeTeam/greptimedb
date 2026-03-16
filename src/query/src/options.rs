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
use table::metadata::TableId;

pub const FLOW_INCREMENTAL_AFTER_SEQS: &str = "flow.incremental_after_seqs";
pub const FLOW_INCREMENTAL_MODE: &str = "flow.incremental_mode";
pub const FLOW_RETURN_REGION_SEQ: &str = "flow.return_region_seq";
pub const FLOW_SINK_TABLE_ID: &str = "flow.sink_table_id";

const FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY: &str = "memtable_only";

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
    pub incremental_after_seqs: Option<HashMap<u64, u64>>,
    pub incremental_mode: Option<FlowIncrementalMode>,
    pub return_region_seq: bool,
    pub sink_table_id: Option<TableId>,
}

impl FlowQueryExtensions {
    pub fn from_extensions(extensions: &HashMap<String, String>) -> Result<Self, String> {
        let incremental_mode = extensions
            .get(FLOW_INCREMENTAL_MODE)
            .map(|value| match value.as_str() {
                v if v.eq_ignore_ascii_case(FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY) => {
                    Ok(FlowIncrementalMode::MemtableOnly)
                }
                _ => Err(format!(
                    "Invalid value for {}: {}",
                    FLOW_INCREMENTAL_MODE, value
                )),
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
                value
                    .parse::<TableId>()
                    .map_err(|_| format!("Invalid value for {}: {}", FLOW_SINK_TABLE_ID, value))
            })
            .transpose()?;

        if matches!(incremental_mode, Some(FlowIncrementalMode::MemtableOnly)) {
            let after_seqs = incremental_after_seqs.as_ref().ok_or_else(|| {
                format!(
                    "{} is required when {}={}.",
                    FLOW_INCREMENTAL_AFTER_SEQS,
                    FLOW_INCREMENTAL_MODE,
                    FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY
                )
            })?;
            if after_seqs.is_empty() {
                return Err(format!(
                    "{} must not be empty when {}={}.",
                    FLOW_INCREMENTAL_AFTER_SEQS,
                    FLOW_INCREMENTAL_MODE,
                    FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY
                ));
            }
        }

        Ok(Self {
            incremental_after_seqs,
            incremental_mode,
            return_region_seq,
            sink_table_id,
        })
    }

    pub fn validate_for_scan(
        &self,
        source_region_ids: &[u64],
        current_scan_table_id: Option<TableId>,
    ) -> Result<bool, String> {
        if self.sink_table_id.is_some() && self.sink_table_id == current_scan_table_id {
            return Ok(false);
        }

        if matches!(
            self.incremental_mode,
            Some(FlowIncrementalMode::MemtableOnly)
        ) {
            let after_seqs = self.incremental_after_seqs.as_ref().ok_or_else(|| {
                format!(
                    "{} is required when {}=memtable_only.",
                    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE
                )
            })?;

            for region_id in source_region_ids {
                if !after_seqs.contains_key(region_id) {
                    return Err(format!(
                        "Missing region {} in {} when {}=memtable_only.",
                        region_id, FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE
                    ));
                }
            }
        }

        Ok(self.incremental_after_seqs.is_some())
    }
}

fn parse_incremental_after_seqs(value: &str) -> Result<HashMap<u64, u64>, String> {
    let raw = serde_json::from_str::<HashMap<String, u64>>(value).map_err(|e| {
        format!(
            "Invalid JSON for {}: {} ({})",
            FLOW_INCREMENTAL_AFTER_SEQS, value, e
        )
    })?;

    raw.into_iter()
        .map(|(region_id, seq)| {
            region_id
                .parse::<u64>()
                .map(|region_id| (region_id, seq))
                .map_err(|_| {
                    format!(
                        "Invalid region id in {}: {}",
                        FLOW_INCREMENTAL_AFTER_SEQS, region_id
                    )
                })
        })
        .collect()
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value {
        v if v.eq_ignore_ascii_case("true") => Ok(true),
        v if v.eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(format!(
            "Invalid value for {}: {}",
            FLOW_RETURN_REGION_SEQ, value
        )),
    }
}

#[cfg(test)]
mod flow_extension_tests {
    use super::*;

    #[test]
    fn test_parse_flow_extensions_default() {
        let exts = HashMap::new();
        let parsed = FlowQueryExtensions::from_extensions(&exts).unwrap();

        assert_eq!(parsed.incremental_mode, None);
        assert_eq!(parsed.incremental_after_seqs, None);
        assert!(!parsed.return_region_seq);
        assert_eq!(parsed.sink_table_id, None);
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

        let parsed = FlowQueryExtensions::from_extensions(&exts).unwrap();
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

        let err = FlowQueryExtensions::from_extensions(&exts).unwrap_err();
        assert!(err.contains(FLOW_INCREMENTAL_AFTER_SEQS));
    }

    #[test]
    fn test_parse_flow_extensions_invalid_mode() {
        let exts = HashMap::from([(FLOW_INCREMENTAL_MODE.to_string(), "foo".to_string())]);

        let err = FlowQueryExtensions::from_extensions(&exts).unwrap_err();
        assert!(err.contains(FLOW_INCREMENTAL_MODE));
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

        let err = FlowQueryExtensions::from_extensions(&exts).unwrap_err();
        assert!(err.contains(FLOW_INCREMENTAL_AFTER_SEQS));
    }

    #[test]
    fn test_parse_flow_extensions_invalid_sink_table_id() {
        let exts = HashMap::from([(FLOW_SINK_TABLE_ID.to_string(), "x".to_string())]);

        let err = FlowQueryExtensions::from_extensions(&exts).unwrap_err();
        assert!(err.contains(FLOW_SINK_TABLE_ID));
    }

    #[test]
    fn test_validate_for_scan_missing_source_region() {
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                r#"{"1":10}"#.to_string(),
            ),
        ]);

        let parsed = FlowQueryExtensions::from_extensions(&exts).unwrap();
        let err = parsed.validate_for_scan(&[1, 2], Some(100)).unwrap_err();
        assert!(err.contains("Missing region 2"));
    }

    #[test]
    fn test_validate_for_scan_sink_table_excluded() {
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                r#"{"1":10}"#.to_string(),
            ),
            (FLOW_SINK_TABLE_ID.to_string(), "1024".to_string()),
        ]);

        let parsed = FlowQueryExtensions::from_extensions(&exts).unwrap();
        let apply_incremental = parsed.validate_for_scan(&[1, 2], Some(1024)).unwrap();
        assert!(!apply_incremental);
    }
}
