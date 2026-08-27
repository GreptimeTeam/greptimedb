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

//! Per-table semantic metadata accumulated during one ingest pass (OTLP
//! metrics, Prometheus remote write v2).
//!
//! Each written table collects the scalar semantic keys its wire metadata
//! declares (an OTLP histogram fans out into `_bucket`/`_sum`/`_count`
//! companions; a remote write series names one metric family). The resulting
//! index is serialized onto the `greptime.internal.semantic.per_table_index`
//! context extension as `{schema -> {table -> {key -> value}}}` and folded into
//! each table's options at auto-create time.
//!
//! Conflict handling follows the RFC: when two sources disagree on a
//! single-valued key the value collapses to `mixed` (or `unknown` for keys whose
//! domain has no `mixed`).

use std::collections::{BTreeMap, HashMap};

use table::requests::{SEMANTIC_VALUE_MIXED, SEMANTIC_VALUE_UNKNOWN, validate_semantic_option};

// `greptime.semantic.metric.type` values stamped per emitted table. Must stay
// within the domain accepted by `validate_semantic_option`; the drift-guard test
// asserts this.
pub const METRIC_TYPE_COUNTER: &str = "counter";
pub const METRIC_TYPE_UPDOWN_COUNTER: &str = "updown_counter";
pub const METRIC_TYPE_GAUGE: &str = "gauge";
pub const METRIC_TYPE_HISTOGRAM: &str = "histogram";
pub const METRIC_TYPE_GAUGE_HISTOGRAM: &str = "gauge_histogram";
pub const METRIC_TYPE_SUMMARY: &str = "summary";
pub const METRIC_TYPE_INFO: &str = "info";
pub const METRIC_TYPE_STATESET: &str = "stateset";

/// Maps an OpenMetrics unit name (open vocabulary full words: `seconds`,
/// `bytes`) to the UCUM code the `greptime.semantic.metric.unit` option is
/// defined in. Only the OpenMetrics base units are mapped; anything else is
/// dropped — a missing unit beats a corrupted cross-protocol vocabulary.
pub fn openmetrics_unit_to_ucum(unit: &str) -> Option<&'static str> {
    Some(match unit {
        "seconds" => "s",
        "celsius" => "Cel",
        "meters" => "m",
        "bytes" => "By",
        "ratios" => "1",
        "volts" => "V",
        "amperes" => "A",
        "joules" => "J",
        "grams" => "g",
        _ => return None,
    })
}

/// Index of `{table_name -> {semantic_key -> value}}` for one target schema.
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

    /// Serializes to the JSON `{schema -> {table -> {key -> value}}}` carried
    /// on the context extension, with every table under `schema`. `None` when
    /// nothing was recorded.
    pub fn encode(&self, schema: &str) -> Option<String> {
        if self.tables.is_empty() {
            return None;
        }
        serde_json::to_string(&BTreeMap::from([(schema, &self.tables)])).ok()
    }

    fn merge_from(&mut self, other: &SemanticIndex) {
        for (table, scalars) in &other.tables {
            for (key, value) in scalars {
                self.record_scalar(table, key, value);
            }
        }
    }

    #[cfg(test)]
    fn options_of(&self, table: &str) -> Option<&BTreeMap<&'static str, String>> {
        self.tables.get(table)
    }
}

/// Schema-aware collection of [`SemanticIndex`]es: Prometheus remote write lets
/// each series override the target schema with a special label, so one request
/// may write the same metric name into several schemas — their metadata must
/// not collapse into each other.
#[derive(Debug, Default)]
pub struct SemanticIndexes {
    /// Tables written into the request's default schema (no override).
    default: SemanticIndex,
    /// Tables written under a per-series schema override.
    overrides: HashMap<String, SemanticIndex>,
}

impl SemanticIndexes {
    pub fn is_empty(&self) -> bool {
        self.default.is_empty() && self.overrides.values().all(SemanticIndex::is_empty)
    }

    /// The index for `schema` (`None` = the request's default schema).
    pub fn index_for(&mut self, schema: Option<&str>) -> &mut SemanticIndex {
        match schema {
            None => &mut self.default,
            Some(schema) => {
                if !self.overrides.contains_key(schema) {
                    self.overrides
                        .insert(schema.to_string(), SemanticIndex::default());
                }
                self.overrides.get_mut(schema).expect("just inserted")
            }
        }
    }

    /// Serializes to the JSON `{schema -> {table -> {key -> value}}}` carried on
    /// the context extension, resolving the default index to `default_schema`.
    /// An override explicitly naming `default_schema` merges into the default
    /// with the usual conflict collapse.
    pub fn encode(&self, default_schema: &str) -> Option<String> {
        if self.is_empty() {
            return None;
        }
        let mut by_schema: BTreeMap<&str, &HashMap<String, BTreeMap<&'static str, String>>> =
            BTreeMap::new();
        let mut merged_default;
        if let Some(aliased) = self.overrides.get(default_schema) {
            merged_default = SemanticIndex::default();
            merged_default.merge_from(&self.default);
            merged_default.merge_from(aliased);
            by_schema.insert(default_schema, &merged_default.tables);
        } else if !self.default.is_empty() {
            by_schema.insert(default_schema, &self.default.tables);
        }
        for (schema, index) in &self.overrides {
            if schema != default_schema && !index.is_empty() {
                by_schema.insert(schema, &index.tables);
            }
        }
        serde_json::to_string(&by_schema).ok()
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

    type Decoded = BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>;

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
        assert_eq!(index.encode("public"), None);

        let mut index = SemanticIndex::default();
        index.record_scalar("metric_a", SEMANTIC_METRIC_TYPE, "counter");
        index.record_scalar("metric_a", SEMANTIC_METRIC_UNIT, "By");
        let json = index.encode("public").unwrap();
        let parsed: Decoded = serde_json::from_str(&json).unwrap();
        let table = parsed.get("public").unwrap().get("metric_a").unwrap();
        assert_eq!(
            table.get(SEMANTIC_METRIC_TYPE).map(String::as_str),
            Some("counter")
        );
        assert_eq!(
            table.get(SEMANTIC_METRIC_UNIT).map(String::as_str),
            Some("By")
        );
    }

    #[test]
    fn test_indexes_keep_schemas_apart_and_merge_default_alias() {
        let mut indexes = SemanticIndexes::default();
        assert!(indexes.is_empty());
        assert_eq!(indexes.encode("public"), None);

        // The same metric name in two schemas must not collapse to `mixed`.
        indexes
            .index_for(None)
            .record_scalar("cpu_usage", SEMANTIC_METRIC_TYPE, "counter");
        indexes.index_for(Some("tenant_b")).record_scalar(
            "cpu_usage",
            SEMANTIC_METRIC_TYPE,
            "gauge",
        );
        let parsed: Decoded = serde_json::from_str(&indexes.encode("public").unwrap()).unwrap();
        assert_eq!(
            parsed["public"]["cpu_usage"][SEMANTIC_METRIC_TYPE],
            "counter"
        );
        assert_eq!(
            parsed["tenant_b"]["cpu_usage"][SEMANTIC_METRIC_TYPE],
            "gauge"
        );

        // An override naming the default schema merges into it — and a real
        // conflict then collapses.
        indexes
            .index_for(Some("public"))
            .record_scalar("cpu_usage", SEMANTIC_METRIC_TYPE, "gauge");
        let parsed: Decoded = serde_json::from_str(&indexes.encode("public").unwrap()).unwrap();
        assert_eq!(parsed["public"]["cpu_usage"][SEMANTIC_METRIC_TYPE], "mixed");
    }

    #[test]
    fn test_openmetrics_unit_mapping() {
        assert_eq!(openmetrics_unit_to_ucum("seconds"), Some("s"));
        assert_eq!(openmetrics_unit_to_ucum("bytes"), Some("By"));
        assert_eq!(openmetrics_unit_to_ucum("ratios"), Some("1"));
        // Outside the OpenMetrics base set: dropped, not passed through.
        assert_eq!(openmetrics_unit_to_ucum("requests"), None);
        assert_eq!(openmetrics_unit_to_ucum(""), None);
        // No fuzzy matching: UCUM codes are not OpenMetrics names.
        assert_eq!(openmetrics_unit_to_ucum("By"), None);
    }
}
