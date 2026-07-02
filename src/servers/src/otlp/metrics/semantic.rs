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

//! Per-table semantic metadata accumulated during one OTLP metrics encode pass.
//!
//! A metric emits one or more tables (histogram and summary fan out into
//! `_bucket`/`_sum`/`_count` companions). Each emitted table collects the
//! metric's scalar semantic keys. The resulting index is serialized onto the
//! `greptime.internal.semantic.per_table_index` context extension and folded
//! into each table's options at auto-create time.
//!
//! Conflict handling follows the RFC: when two sources disagree on a
//! single-valued key the value collapses to `mixed` (or `unknown` for keys whose
//! domain has no `mixed`).

use std::collections::{BTreeMap, HashMap};

use table::requests::{SEMANTIC_VALUE_MIXED, SEMANTIC_VALUE_UNKNOWN, validate_semantic_option};

/// Index of `{table_name -> {semantic_key -> value}}` built while encoding.
#[derive(Debug, Default)]
pub struct SemanticIndex {
    /// Per-table scalar keys; conflicting values collapse to `mixed`/`unknown`.
    tables: HashMap<String, BTreeMap<&'static str, String>>,
}

impl SemanticIndex {
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Records a scalar semantic key for `table`. A value conflicting with one
    /// already recorded collapses the key to `mixed`/`unknown`; once collapsed
    /// it stays collapsed.
    pub fn record_scalar(&mut self, table: &str, key: &'static str, value: &str) {
        // Avoid allocating the table name (and an empty map) on the common path
        // where the table is already present.
        if let Some(scalars) = self.tables.get_mut(table) {
            match scalars.get(key).map(String::as_str) {
                Some(existing) if existing == value => {}
                Some(SEMANTIC_VALUE_MIXED) | Some(SEMANTIC_VALUE_UNKNOWN) => {}
                Some(_) => {
                    scalars.insert(key, collapse_value(key));
                }
                None => {
                    scalars.insert(key, value.to_string());
                }
            }
        } else {
            self.tables.insert(
                table.to_string(),
                BTreeMap::from([(key, value.to_string())]),
            );
        }
    }

    /// Serializes to the JSON `{table -> {key -> value}}` carried on the context
    /// extension, or `None` when nothing was recorded.
    pub fn encode(&self) -> Option<String> {
        if self.tables.is_empty() {
            return None;
        }
        serde_json::to_string(&self.tables).ok()
    }

    #[cfg(test)]
    fn options_of(&self, table: &str) -> Option<&BTreeMap<&'static str, String>> {
        self.tables.get(table)
    }
}

/// The collapsed value for a conflicting scalar key: `mixed` when the key's
/// domain accepts it, else `unknown`. Uses the vocabulary validator as the
/// single source of truth for which keys allow `mixed`.
fn collapse_value(key: &str) -> String {
    if validate_semantic_option(key, SEMANTIC_VALUE_MIXED) {
        SEMANTIC_VALUE_MIXED.to_string()
    } else {
        SEMANTIC_VALUE_UNKNOWN.to_string()
    }
}

#[cfg(test)]
mod tests {
    use table::requests::{
        SEMANTIC_METRIC_METADATA_QUALITY, SEMANTIC_METRIC_TYPE, SEMANTIC_METRIC_UNIT,
    };

    use super::*;

    #[test]
    fn test_scalar_recording_keeps_first_then_collapses_on_conflict() {
        let mut index = SemanticIndex::default();
        index.record_scalar("t", SEMANTIC_METRIC_TYPE, "counter");
        index.record_scalar("t", SEMANTIC_METRIC_TYPE, "counter");
        assert_eq!(
            index
                .options_of("t")
                .unwrap()
                .get(SEMANTIC_METRIC_TYPE)
                .map(String::as_str),
            Some("counter")
        );

        // Conflict on a key whose domain has `mixed` collapses to `mixed`.
        index.record_scalar("t", SEMANTIC_METRIC_TYPE, "gauge");
        assert_eq!(
            index
                .options_of("t")
                .unwrap()
                .get(SEMANTIC_METRIC_TYPE)
                .map(String::as_str),
            Some("mixed")
        );
        // Further writes stay collapsed.
        index.record_scalar("t", SEMANTIC_METRIC_TYPE, "histogram");
        assert_eq!(
            index
                .options_of("t")
                .unwrap()
                .get(SEMANTIC_METRIC_TYPE)
                .map(String::as_str),
            Some("mixed")
        );
    }

    #[test]
    fn test_scalar_conflict_without_mixed_domain_collapses_to_unknown() {
        let mut index = SemanticIndex::default();
        index.record_scalar("t", SEMANTIC_METRIC_METADATA_QUALITY, "declared");
        index.record_scalar("t", SEMANTIC_METRIC_METADATA_QUALITY, "inferred");
        // metadata_quality accepts only declared/inferred/unknown, so a conflict
        // is `unknown`.
        assert_eq!(
            index
                .options_of("t")
                .unwrap()
                .get(SEMANTIC_METRIC_METADATA_QUALITY)
                .map(String::as_str),
            Some("unknown")
        );
    }

    #[test]
    fn test_encode_is_none_when_empty_and_round_trips() {
        let index = SemanticIndex::default();
        assert!(index.is_empty());
        assert_eq!(index.encode(), None);

        let mut index = SemanticIndex::default();
        index.record_scalar("metric_a", SEMANTIC_METRIC_TYPE, "counter");
        index.record_scalar("metric_a", SEMANTIC_METRIC_UNIT, "By");
        let json = index.encode().unwrap();
        let parsed: BTreeMap<String, BTreeMap<String, String>> =
            serde_json::from_str(&json).unwrap();
        let table = parsed.get("metric_a").unwrap();
        assert_eq!(
            table.get(SEMANTIC_METRIC_TYPE).map(String::as_str),
            Some("counter")
        );
        assert_eq!(
            table.get(SEMANTIC_METRIC_UNIT).map(String::as_str),
            Some("By")
        );
    }
}
