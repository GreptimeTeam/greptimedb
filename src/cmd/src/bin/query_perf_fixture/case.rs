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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct CaseFile {
    pub(super) scenario: Scenario,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind")]
pub(super) enum Scenario {
    #[serde(rename = "direct_readable_sst")]
    DirectReadableSst(DirectReadableSstScenario),
    #[serde(rename = "prom_remote_write_then_query")]
    PromRemoteWriteThenQuery(PromRemoteWriteThenQueryScenario),
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct DirectReadableSstScenario {
    #[serde(default)]
    pub(super) seed: Option<u64>,
    pub(super) tables: Vec<TableConfig>,
    pub(super) layout: LayoutConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct PromRemoteWriteThenQueryScenario {
    #[serde(default)]
    pub(super) queries: Vec<serde_json::Value>,
    pub(super) remote_write: PromRemoteWritePlan,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct PromRemoteWritePlan {
    #[serde(default = "default_database")]
    pub(super) database: String,
    #[serde(alias = "metric_name")]
    pub(super) metric: String,
    #[serde(default = "default_physical_table")]
    pub(super) physical_table: String,
    #[serde(default = "default_series_count")]
    pub(super) series_count: u64,
    #[serde(default = "default_samples_per_series")]
    pub(super) samples_per_series: u64,
    #[serde(default = "default_start_unix_millis")]
    pub(super) start_unix_millis: i64,
    #[serde(default = "default_step_millis")]
    pub(super) step_millis: i64,
    #[serde(default = "default_chunk_series_count", alias = "batch_size")]
    pub(super) chunk_series_count: u64,
    #[serde(default = "default_timeout_seconds")]
    pub(super) timeout_seconds: u64,
    #[serde(default)]
    pub(super) sample_chunk_size: Option<u64>,
    #[serde(default = "default_flush_every_sample_chunks")]
    pub(super) flush_every_sample_chunks: u64,
    #[serde(default = "default_visibility_timeout_seconds")]
    pub(super) visibility_timeout_seconds: u64,
    #[serde(default)]
    pub(super) prom_store: PromStoreConfig,
    #[serde(default)]
    pub(super) value: ValueConfig,
    #[serde(default)]
    pub(super) storage: Option<StorageConfig>,
    #[serde(default)]
    pub(super) read_bench: Option<ReadBenchConfig>,
    #[serde(default)]
    pub(super) physical_table_setup: Option<PhysicalTableSetup>,
    #[serde(default)]
    pub(super) logical_stream_verification: Option<LogicalStreamVerification>,
}

pub(super) fn default_database() -> String {
    "public".to_string()
}
pub(super) fn default_physical_table() -> String {
    "greptime_physical_table".to_string()
}
pub(super) fn default_series_count() -> u64 {
    8
}
pub(super) fn default_samples_per_series() -> u64 {
    30
}
pub(super) fn default_start_unix_millis() -> i64 {
    1_704_067_200_000
}
pub(super) fn default_step_millis() -> i64 {
    15_000
}
pub(super) fn default_chunk_series_count() -> u64 {
    8
}
pub(super) fn default_timeout_seconds() -> u64 {
    60
}
pub(super) fn default_flush_every_sample_chunks() -> u64 {
    1
}
pub(super) fn default_visibility_timeout_seconds() -> u64 {
    30
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(try_from = "PhysicalTableSetupWire")]
pub(super) struct PhysicalTableSetup {
    pub(super) columns: Vec<PhysicalTableColumn>,
    pub(super) time_index: String,
    pub(super) engine: String,
    #[serde(default)]
    pub(super) options: BTreeMap<String, String>,
    #[serde(default)]
    pub(super) target_options: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct PhysicalTableSetupWire {
    columns: Vec<PhysicalTableColumn>,
    time_index: String,
    engine: String,
    #[serde(default)]
    options: BTreeMap<String, String>,
    #[serde(default)]
    target_options: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct PhysicalTableColumn {
    pub(super) name: String,
    #[serde(rename = "type")]
    pub(super) ty: String,
}

impl TryFrom<PhysicalTableSetupWire> for PhysicalTableSetup {
    type Error = String;

    fn try_from(value: PhysicalTableSetupWire) -> Result<Self, Self::Error> {
        if value.columns.is_empty() {
            return Err("physical_table_setup requires at least one column".to_string());
        }
        let mut columns = HashSet::new();
        for column in &value.columns {
            if !is_safe_identifier(&column.name) {
                return Err(format!(
                    "physical_table_setup column name must be a nonempty safe identifier: {}",
                    column.name
                ));
            }
            if !is_supported_sql_type(&column.ty) {
                return Err(format!(
                    "physical_table_setup column type must be a supported canonical SQL type: {}",
                    column.ty
                ));
            }
            if !columns.insert(&column.name) {
                return Err(format!(
                    "physical_table_setup has duplicate column name: {}",
                    column.name
                ));
            }
        }
        if !is_safe_identifier(&value.time_index) {
            return Err(
                "physical_table_setup time_index must be a nonempty safe identifier".to_string(),
            );
        }
        if !columns.contains(&value.time_index) {
            return Err(format!(
                "physical_table_setup time_index does not name a column: {}",
                value.time_index
            ));
        }
        if !is_safe_identifier(&value.engine) {
            return Err(
                "physical_table_setup engine must be a nonempty safe identifier".to_string(),
            );
        }
        validate_option_map("physical_table_setup options", &value.options)?;
        for (target, options) in &value.target_options {
            if target.trim().is_empty() {
                return Err(
                    "physical_table_setup target_options contains an empty target name".to_string(),
                );
            }
            validate_option_map(
                &format!("physical_table_setup target_options.{target}"),
                options,
            )?;
        }
        Ok(Self {
            columns: value.columns,
            time_index: value.time_index,
            engine: value.engine,
            options: value.options,
            target_options: value.target_options,
        })
    }
}

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn is_supported_sql_type(value: &str) -> bool {
    matches!(value, "TIMESTAMP(3)" | "DOUBLE" | "STRING")
}

fn validate_option_map(name: &str, options: &BTreeMap<String, String>) -> Result<(), String> {
    for key in options.keys() {
        if !is_safe_identifier(key) {
            return Err(format!("{name} contains an invalid option name: {key}"));
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(try_from = "LogicalStreamVerificationWire")]
pub(super) struct LogicalStreamVerification {
    pub(super) labels: Vec<String>,
    pub(super) timestamp_column: String,
    pub(super) value_column: String,
}

#[derive(Debug, Deserialize)]
struct LogicalStreamVerificationWire {
    labels: Vec<String>,
    timestamp_column: String,
    value_column: String,
}

impl TryFrom<LogicalStreamVerificationWire> for LogicalStreamVerification {
    type Error = String;

    fn try_from(value: LogicalStreamVerificationWire) -> Result<Self, Self::Error> {
        if value.labels.is_empty() {
            return Err("logical_stream_verification labels must be nonempty".to_string());
        }
        let mut labels = HashSet::new();
        for label in &value.labels {
            if !is_safe_identifier(label) {
                return Err(format!(
                    "logical_stream_verification label must be a nonempty safe identifier: {label}"
                ));
            }
            if !labels.insert(label.as_str()) {
                return Err(format!(
                    "logical_stream_verification has duplicate label: {label}"
                ));
            }
        }
        if !is_safe_identifier(&value.timestamp_column) {
            return Err(
                "logical_stream_verification timestamp_column must be a nonempty safe identifier"
                    .to_string(),
            );
        }
        if !is_safe_identifier(&value.value_column) {
            return Err(
                "logical_stream_verification value_column must be a nonempty safe identifier"
                    .to_string(),
            );
        }
        if value.timestamp_column == value.value_column
            || labels.contains(value.timestamp_column.as_str())
            || labels.contains(value.value_column.as_str())
        {
            return Err(
                "logical_stream_verification timestamp_column and value_column must be distinct from each other and all labels"
                    .to_string(),
            );
        }
        Ok(Self {
            labels: value.labels,
            timestamp_column: value.timestamp_column,
            value_column: value.value_column,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct PromStoreConfig {
    #[serde(default = "default_pending_rows_flush_interval")]
    pub(super) pending_rows_flush_interval: String,
    #[serde(default = "default_max_batch_rows")]
    pub(super) max_batch_rows: u64,
    #[serde(default = "default_max_concurrent_flushes")]
    pub(super) max_concurrent_flushes: u64,
    #[serde(default = "default_worker_channel_capacity")]
    pub(super) worker_channel_capacity: u64,
    #[serde(default = "default_max_inflight_requests")]
    pub(super) max_inflight_requests: u64,
}

impl Default for PromStoreConfig {
    fn default() -> Self {
        Self {
            pending_rows_flush_interval: default_pending_rows_flush_interval(),
            max_batch_rows: default_max_batch_rows(),
            max_concurrent_flushes: default_max_concurrent_flushes(),
            worker_channel_capacity: default_worker_channel_capacity(),
            max_inflight_requests: default_max_inflight_requests(),
        }
    }
}

pub(super) fn default_pending_rows_flush_interval() -> String {
    "1s".to_string()
}
pub(super) fn default_max_batch_rows() -> u64 {
    100000
}
pub(super) fn default_max_concurrent_flushes() -> u64 {
    256
}
pub(super) fn default_worker_channel_capacity() -> u64 {
    65526
}
pub(super) fn default_max_inflight_requests() -> u64 {
    3000
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ValueEnum, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub(super) enum ValuePattern {
    Linear,
    Constant,
    Modulo,
    Unique,
    SeededRandom,
    RunLength,
    QuantizedSignal,
    SignalWithSporadicStalls,
    MixedSignalRepeated,
    MixedSeries,
}

impl std::fmt::Display for ValuePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_value(self).unwrap().as_str().unwrap()
        )
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(try_from = "ValueConfigWire")]
pub(super) struct ValueConfig {
    #[serde(default = "default_value_pattern")]
    pub(super) pattern: ValuePattern,
    #[serde(default)]
    pub(super) base: f64,
    #[serde(default = "default_value_step")]
    pub(super) step: f64,
    #[serde(default = "default_value_cardinality")]
    pub(super) cardinality: u64,
    #[serde(default)]
    pub(super) seed: u64,
    #[serde(default = "default_value_run_length")]
    pub(super) run_length: u64,
    #[serde(default = "default_value_stall_every")]
    pub(super) stall_every: u64,
    #[serde(default = "default_value_stall_length")]
    pub(super) stall_length: u64,
    #[serde(default = "default_value_mixed_every")]
    pub(super) mixed_every: u64,
    #[serde(default = "default_value_series_mix_every")]
    pub(super) series_mix_every: u64,
    #[serde(default = "default_value_gauge_residue")]
    pub(super) gauge_residue: u64,
    #[serde(default = "default_value_fractional_step")]
    pub(super) fractional_step: f64,
}

#[derive(Debug, Deserialize)]
struct ValueConfigWire {
    #[serde(default = "default_value_pattern")]
    pattern: ValuePattern,
    #[serde(default)]
    base: f64,
    #[serde(default = "default_value_step")]
    step: f64,
    #[serde(default = "default_value_cardinality")]
    cardinality: u64,
    #[serde(default)]
    seed: u64,
    #[serde(default = "default_value_run_length")]
    run_length: u64,
    #[serde(default = "default_value_stall_every")]
    stall_every: u64,
    #[serde(default = "default_value_stall_length")]
    stall_length: u64,
    #[serde(default = "default_value_mixed_every")]
    mixed_every: u64,
    #[serde(default = "default_value_series_mix_every")]
    series_mix_every: u64,
    #[serde(default = "default_value_gauge_residue")]
    gauge_residue: u64,
    #[serde(default = "default_value_fractional_step")]
    fractional_step: f64,
}

impl TryFrom<ValueConfigWire> for ValueConfig {
    type Error = String;

    fn try_from(value: ValueConfigWire) -> Result<Self, Self::Error> {
        let value = Self {
            pattern: value.pattern,
            base: value.base,
            step: value.step,
            cardinality: value.cardinality,
            seed: value.seed,
            run_length: value.run_length,
            stall_every: value.stall_every,
            stall_length: value.stall_length,
            mixed_every: value.mixed_every,
            series_mix_every: value.series_mix_every,
            gauge_residue: value.gauge_residue,
            fractional_step: value.fractional_step,
        };
        value.validate_mixed_series_selector()?;
        Ok(value)
    }
}

impl ValueConfig {
    fn validate_mixed_series_selector(&self) -> Result<(), String> {
        if self.pattern == ValuePattern::MixedSeries {
            validate_mixed_series_selector(
                self.series_mix_every,
                self.gauge_residue,
                self.fractional_step,
            )?;
        }
        Ok(())
    }
}

pub(super) fn validate_mixed_series_selector(
    series_mix_every: u64,
    gauge_residue: u64,
    fractional_step: f64,
) -> Result<(), String> {
    if series_mix_every == 0 {
        return Err("mixed_series requires series_mix_every > 0".to_string());
    }
    if gauge_residue >= series_mix_every {
        return Err("mixed_series requires gauge_residue < series_mix_every".to_string());
    }
    if !fractional_step.is_finite() || fractional_step <= 0.0 {
        return Err("mixed_series requires fractional_step to be finite and positive".to_string());
    }
    Ok(())
}

impl Default for ValueConfig {
    fn default() -> Self {
        Self {
            pattern: default_value_pattern(),
            base: 0.0,
            step: default_value_step(),
            cardinality: default_value_cardinality(),
            seed: 0,
            run_length: default_value_run_length(),
            stall_every: default_value_stall_every(),
            stall_length: default_value_stall_length(),
            mixed_every: default_value_mixed_every(),
            series_mix_every: default_value_series_mix_every(),
            gauge_residue: default_value_gauge_residue(),
            fractional_step: default_value_fractional_step(),
        }
    }
}
pub(super) fn default_value_pattern() -> ValuePattern {
    ValuePattern::Linear
}
pub(super) fn default_value_step() -> f64 {
    0.125
}
pub(super) fn default_value_cardinality() -> u64 {
    97
}
pub(super) fn default_value_run_length() -> u64 {
    8
}
pub(super) fn default_value_stall_every() -> u64 {
    100
}
pub(super) fn default_value_stall_length() -> u64 {
    16
}
pub(super) fn default_value_mixed_every() -> u64 {
    5
}
pub(super) fn default_value_series_mix_every() -> u64 {
    20
}
pub(super) fn default_value_gauge_residue() -> u64 {
    19
}
pub(super) fn default_value_fractional_step() -> f64 {
    0.125
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_series_value_config_normalizes_all_selector_fields() {
        let value: ValueConfig = toml::from_str(
            r#"
pattern = "mixed_series"
series_mix_every = 20
gauge_residue = 19
"#,
        )
        .unwrap();

        let normalized = serde_json::to_value(value).unwrap();
        assert_eq!(normalized["pattern"], "mixed_series");
        assert_eq!(normalized["series_mix_every"], 20);
        assert_eq!(normalized["gauge_residue"], 19);
        assert_eq!(normalized["fractional_step"], 0.125);
    }

    #[test]
    fn mixed_series_value_config_rejects_invalid_selector_and_step() {
        for (config, expected) in [
            (
                r#"pattern = "mixed_series"
series_mix_every = 0
gauge_residue = 0"#,
                "series_mix_every",
            ),
            (
                r#"pattern = "mixed_series"
series_mix_every = 20
gauge_residue = 20"#,
                "gauge_residue",
            ),
            (
                r#"pattern = "mixed_series"
series_mix_every = 20
gauge_residue = 19
fractional_step = 0"#,
                "fractional_step",
            ),
        ] {
            assert!(
                toml::from_str::<ValueConfig>(config)
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }
    }

    #[test]
    fn physical_table_setup_normalizes_ordered_columns_and_target_overlays() {
        let setup: PhysicalTableSetup = toml::from_str(
            r#"
columns = [
  { name = "host", type = "STRING" },
  { name = "ts", type = "TIMESTAMP(3)" },
  { name = "value", type = "DOUBLE" },
]
time_index = "ts"
engine = "mito"
options = { compression = "zstd", append_mode = "false", physical_metric_table = "", quoted = "\"preserved\"" }
target_options = { base = { compression = "plain" }, candidate = { compression = "zstd" } }
"#,
        )
        .unwrap();

        let normalized = serde_json::to_value(setup).unwrap();
        assert_eq!(normalized["columns"][0]["name"], "host");
        assert_eq!(normalized["time_index"], "ts");
        assert_eq!(normalized["engine"], "mito");
        assert_eq!(normalized["options"]["compression"], "zstd");
        assert_eq!(normalized["options"]["append_mode"], "false");
        assert_eq!(normalized["options"]["physical_metric_table"], "");
        assert_eq!(normalized["options"]["quoted"], "\"preserved\"");
        assert_eq!(normalized["target_options"]["base"]["compression"], "plain");
    }

    #[test]
    fn physical_table_setup_rejects_noncanonical_sql_types_and_injections() {
        for ty in [
            "timestamp(3)",
            "DOUBLE ",
            "DOUBLE, injected STRING",
            "DOUBLE PRIMARY KEY",
            "DOUBLE NULL",
            "DOUBLE -- comment",
            "DOUBLE;",
            "DOUBLE trailing",
            "TIMESTAMP(3",
            "TIMESTAMP(3))",
        ] {
            let config = format!(
                r#"
columns = [{{ name = "ts", type = "TIMESTAMP(3)" }}, {{ name = "value", type = "{ty}" }}]
time_index = "ts"
engine = "metric"
"#
            );
            assert!(
                toml::from_str::<PhysicalTableSetup>(&config)
                    .unwrap_err()
                    .to_string()
                    .contains("supported canonical SQL type"),
                "type must be rejected: {ty}"
            );
        }
    }

    #[test]
    fn physical_table_setup_rejects_invalid_columns_time_index_and_target() {
        for (config, expected) in [
            (
                r#"
columns = [{ name = "ts", type = "TIMESTAMP(3)" }, { name = "ts", type = "DOUBLE" }]
time_index = "ts"
engine = "mito"
"#,
                "duplicate column",
            ),
            (
                r#"
columns = [{ name = "", type = "TIMESTAMP" }]
time_index = "ts"
engine = "mito"
"#,
                "safe identifier",
            ),
            (
                r#"
columns = [{ name = "ts", type = "TIMESTAMP(3)" }]
time_index = "missing"
engine = "mito"
"#,
                "time_index does not name a column",
            ),
            (
                r#"
columns = [{ name = "ts", type = "TIMESTAMP(3)" }]
time_index = "ts"
engine = "mito"
target_options = { "" = { compression = "zstd" } }
"#,
                "empty target name",
            ),
        ] {
            assert!(
                toml::from_str::<PhysicalTableSetup>(config)
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }
    }

    #[test]
    fn logical_stream_verification_normalizes_stable_label_order() {
        let verification: LogicalStreamVerification = toml::from_str(
            r#"
labels = ["host", "instance"]
timestamp_column = "greptime_timestamp"
value_column = "greptime_value"
"#,
        )
        .unwrap();

        assert_eq!(
            serde_json::to_value(verification).unwrap(),
            serde_json::json!({
                "labels": ["host", "instance"],
                "timestamp_column": "greptime_timestamp",
                "value_column": "greptime_value",
            })
        );
    }

    #[test]
    fn logical_stream_verification_rejects_invalid_columns_and_labels() {
        for (config, expected) in [
            (
                r#"
labels = []
timestamp_column = "greptime_timestamp"
value_column = "greptime_value"
"#,
                "labels must be nonempty",
            ),
            (
                r#"
labels = ["host", "host"]
timestamp_column = "greptime_timestamp"
value_column = "greptime_value"
"#,
                "duplicate label",
            ),
            (
                r#"
labels = ["host-name"]
timestamp_column = "greptime_timestamp"
value_column = "greptime_value"
"#,
                "label must be a nonempty safe identifier",
            ),
            (
                r#"
labels = [""]
timestamp_column = "greptime_timestamp"
value_column = "greptime_value"
"#,
                "label must be a nonempty safe identifier",
            ),
            (
                r#"
labels = ["host"]
timestamp_column = ""
value_column = "greptime_value"
"#,
                "timestamp_column must be a nonempty safe identifier",
            ),
            (
                r#"
labels = ["host"]
timestamp_column = "timestamp-column"
value_column = "greptime_value"
"#,
                "timestamp_column must be a nonempty safe identifier",
            ),
            (
                r#"
labels = ["host"]
timestamp_column = "greptime_timestamp"
value_column = ""
"#,
                "value_column must be a nonempty safe identifier",
            ),
            (
                r#"
labels = ["host"]
timestamp_column = "greptime_timestamp"
value_column = "value-column"
"#,
                "value_column must be a nonempty safe identifier",
            ),
            (
                r#"
labels = ["host"]
timestamp_column = "greptime_timestamp"
value_column = "greptime_timestamp"
"#,
                "must be distinct from each other and all labels",
            ),
            (
                r#"
labels = ["host"]
timestamp_column = "host"
value_column = "greptime_value"
"#,
                "must be distinct from each other and all labels",
            ),
            (
                r#"
labels = ["host"]
timestamp_column = "greptime_timestamp"
value_column = "host"
"#,
                "must be distinct from each other and all labels",
            ),
        ] {
            assert!(
                toml::from_str::<LogicalStreamVerification>(config)
                    .unwrap_err()
                    .to_string()
                    .contains(expected),
                "expected {expected} for:\n{config}"
            );
        }
    }

    #[test]
    fn named_read_projections_normalize_all_and_explicit_columns() {
        let read_bench: ReadBenchConfig = toml::from_str(
            r#"
projections = [
  { name = "all_columns" },
  { name = "value_only", columns = ["greptime_value"] },
]
"#,
        )
        .unwrap();

        let normalized = serde_json::to_value(read_bench).unwrap();
        assert!(normalized["projections"][0]["columns"].is_null());
        assert_eq!(
            normalized["projections"][1]["columns"],
            serde_json::json!(["greptime_value"])
        );
        assert_eq!(normalized["rounds"], 1);
    }

    #[test]
    fn read_bench_rejects_invalid_projections_rounds_and_iterations() {
        for (config, expected) in [
            (
                r#"projections = [{ name = "duplicate" }, { name = "duplicate" }]"#,
                "duplicate name",
            ),
            (
                r#"projections = [{ name = "empty_columns", columns = [] }]"#,
                "omitted or nonempty",
            ),
            (
                r#"projections = [{ name = "duplicate_columns", columns = ["value", "value"] }]"#,
                "duplicate column name",
            ),
            (
                r#"projections = [{ name = "empty_column", columns = [""] }]"#,
                "empty column name",
            ),
            (r#"projections = [{ name = "" }]"#, "empty name"),
            ("rounds = 0", "rounds must be positive"),
            ("iterations = 0", "iterations must be positive"),
        ] {
            assert!(
                toml::from_str::<ReadBenchConfig>(config)
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct StorageConfig {
    #[serde(default = "default_true")]
    pub(super) inspect: bool,
    #[serde(default = "default_storage_column")]
    pub(super) column: String,
    #[serde(default)]
    pub(super) root_suffix: Option<String>,
    #[serde(default)]
    pub(super) include_metadata_files: bool,
    #[serde(default = "default_min_files")]
    pub(super) min_files: u64,
    #[serde(default = "default_min_files")]
    pub(super) min_files_with_column: u64,
    #[serde(default)]
    pub(super) require_encodings: Vec<String>,
    #[serde(default)]
    pub(super) forbid_encodings: Vec<String>,
    #[serde(default)]
    pub(super) max_total_file_size_bytes: Option<u64>,
    #[serde(default)]
    pub(super) max_column_compressed_size_bytes: Option<u64>,
    #[serde(default)]
    pub(super) max_column_uncompressed_size_bytes: Option<u64>,
    #[serde(default)]
    pub(super) max_candidate_total_file_size_regression_pct: Option<f64>,
    #[serde(default)]
    pub(super) max_candidate_column_compressed_size_regression_pct: Option<f64>,
    #[serde(default)]
    pub(super) max_candidate_column_uncompressed_size_regression_pct: Option<f64>,
    #[serde(skip_deserializing, default)]
    pub(super) planned_thresholds: Vec<StorageThresholdPlan>,
}

#[derive(Debug, Serialize)]
pub(super) struct StorageThresholdPlan {
    pub(super) threshold: String,
    pub(super) status: String,
    pub(super) value: serde_json::Value,
}

impl StorageConfig {
    pub(super) fn populate_planned_thresholds(&mut self) {
        let mut planned = Vec::new();
        planned.push(StorageThresholdPlan::new("min_files", self.min_files));
        planned.push(StorageThresholdPlan::new(
            "min_files_with_column",
            self.min_files_with_column,
        ));
        if !self.require_encodings.is_empty() {
            planned.push(StorageThresholdPlan::new(
                "require_encodings",
                &self.require_encodings,
            ));
        }
        if !self.forbid_encodings.is_empty() {
            planned.push(StorageThresholdPlan::new(
                "forbid_encodings",
                &self.forbid_encodings,
            ));
        }
        macro_rules! push_optional {
            ($name:literal, $value:expr) => {
                if let Some(value) = $value {
                    planned.push(StorageThresholdPlan::new($name, value));
                }
            };
        }
        push_optional!("max_total_file_size_bytes", self.max_total_file_size_bytes);
        push_optional!(
            "max_column_compressed_size_bytes",
            self.max_column_compressed_size_bytes
        );
        push_optional!(
            "max_column_uncompressed_size_bytes",
            self.max_column_uncompressed_size_bytes
        );
        push_optional!(
            "max_candidate_total_file_size_regression_pct",
            self.max_candidate_total_file_size_regression_pct
        );
        push_optional!(
            "max_candidate_column_compressed_size_regression_pct",
            self.max_candidate_column_compressed_size_regression_pct
        );
        push_optional!(
            "max_candidate_column_uncompressed_size_regression_pct",
            self.max_candidate_column_uncompressed_size_regression_pct
        );
        self.planned_thresholds = planned;
    }
}

impl StorageThresholdPlan {
    fn new<T: Serialize>(threshold: &str, value: T) -> Self {
        Self {
            threshold: threshold.to_string(),
            status: "planned".to_string(),
            value: serde_json::to_value(value).expect("storage threshold value must serialize"),
        }
    }
}
pub(super) fn default_true() -> bool {
    true
}
pub(super) fn default_storage_column() -> String {
    "greptime_value".to_string()
}
pub(super) fn default_min_files() -> u64 {
    1
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(try_from = "ReadBenchConfigWire")]
pub(super) struct ReadBenchConfig {
    #[serde(default = "default_true")]
    pub(super) enabled: bool,
    #[serde(default = "default_true")]
    pub(super) parquetbench: bool,
    #[serde(default = "default_true")]
    pub(super) scanbench: bool,
    #[serde(default = "default_iterations")]
    pub(super) iterations: u64,
    #[serde(default = "default_rounds")]
    pub(super) rounds: u64,
    #[serde(default)]
    pub(super) projection: Vec<String>,
    #[serde(default)]
    pub(super) projections: Vec<NamedReadProjection>,
    #[serde(default = "default_parquet_reader")]
    pub(super) parquet_reader: String,
    #[serde(default = "default_scan_scanner")]
    pub(super) scan_scanner: String,
    #[serde(default = "default_parallelism")]
    pub(super) parallelism: u64,
    #[serde(default)]
    pub(super) max_files: Option<usize>,
    #[serde(flatten)]
    pub(super) thresholds: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ReadBenchConfigWire {
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default = "default_true")]
    parquetbench: bool,
    #[serde(default = "default_true")]
    scanbench: bool,
    #[serde(default = "default_iterations")]
    iterations: u64,
    #[serde(default = "default_rounds")]
    rounds: u64,
    #[serde(default)]
    projection: Vec<String>,
    #[serde(default)]
    projections: Vec<NamedReadProjection>,
    #[serde(default = "default_parquet_reader")]
    parquet_reader: String,
    #[serde(default = "default_scan_scanner")]
    scan_scanner: String,
    #[serde(default = "default_parallelism")]
    parallelism: u64,
    #[serde(default)]
    max_files: Option<usize>,
    #[serde(flatten)]
    thresholds: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(super) struct NamedReadProjection {
    pub(super) name: String,
    #[serde(default)]
    pub(super) columns: Option<Vec<String>>,
}

impl TryFrom<ReadBenchConfigWire> for ReadBenchConfig {
    type Error = String;

    fn try_from(value: ReadBenchConfigWire) -> Result<Self, Self::Error> {
        if value.iterations == 0 {
            return Err("scenario.remote_write.read_bench.iterations must be positive".to_string());
        }
        if value.rounds == 0 {
            return Err("scenario.remote_write.read_bench.rounds must be positive".to_string());
        }
        let mut names = HashSet::new();
        for projection in &value.projections {
            if projection.name.trim().is_empty() {
                return Err(
                    "scenario.remote_write.read_bench.projections contains an empty name"
                        .to_string(),
                );
            }
            if !names.insert(&projection.name) {
                return Err(format!(
                    "scenario.remote_write.read_bench.projections has duplicate name: {}",
                    projection.name
                ));
            }
            if let Some(columns) = &projection.columns {
                if columns.is_empty() {
                    return Err(format!(
                        "scenario.remote_write.read_bench.projections.{} columns must be omitted or nonempty",
                        projection.name
                    ));
                }
                let mut column_names = HashSet::new();
                for column in columns {
                    if column.trim().is_empty() {
                        return Err(format!(
                            "scenario.remote_write.read_bench.projections.{} contains an empty column name",
                            projection.name
                        ));
                    }
                    if !column_names.insert(column) {
                        return Err(format!(
                            "scenario.remote_write.read_bench.projections.{} has duplicate column name: {column}",
                            projection.name
                        ));
                    }
                }
            }
        }
        Ok(Self {
            enabled: value.enabled,
            parquetbench: value.parquetbench,
            scanbench: value.scanbench,
            iterations: value.iterations,
            rounds: value.rounds,
            projection: value.projection,
            projections: value.projections,
            parquet_reader: value.parquet_reader,
            scan_scanner: value.scan_scanner,
            parallelism: value.parallelism,
            max_files: value.max_files,
            thresholds: value.thresholds,
        })
    }
}

impl Default for ReadBenchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            parquetbench: true,
            scanbench: true,
            iterations: default_iterations(),
            rounds: default_rounds(),
            projection: vec![],
            projections: vec![],
            parquet_reader: default_parquet_reader(),
            scan_scanner: default_scan_scanner(),
            parallelism: default_parallelism(),
            max_files: None,
            thresholds: HashMap::new(),
        }
    }
}

pub(super) fn default_iterations() -> u64 {
    7
}
pub(super) fn default_rounds() -> u64 {
    1
}
pub(super) fn default_parquet_reader() -> String {
    "direct".to_string()
}
pub(super) fn default_scan_scanner() -> String {
    "seq".to_string()
}
pub(super) fn default_parallelism() -> u64 {
    1
}

impl Scenario {
    pub(super) fn kind(&self) -> &'static str {
        match self {
            Scenario::DirectReadableSst(_) => "direct_readable_sst",
            Scenario::PromRemoteWriteThenQuery(_) => "prom_remote_write_then_query",
        }
    }

    pub(super) fn direct_readable_sst(&self) -> &DirectReadableSstScenario {
        match self {
            Scenario::DirectReadableSst(scenario) => scenario,
            _ => panic!("scenario is not direct_readable_sst"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct TableConfig {
    pub(super) database: String,
    pub(super) name: String,
    pub(super) engine: String,
    #[serde(default)]
    pub(super) append_mode: Option<bool>,
    #[serde(default)]
    pub(super) sst_format: Option<String>,
    pub(super) primary_key: Vec<String>,
    pub(super) time_index: String,
    pub(super) columns: Vec<ColumnConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct ColumnConfig {
    pub(super) name: String,
    #[serde(rename = "type")]
    pub(super) ty: String,
    pub(super) semantic: String,
    pub(super) distribution: Option<Distribution>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind")]
pub(super) enum Distribution {
    #[serde(rename = "cardinality")]
    Cardinality {
        values: NonZeroUsize,
        prefix: String,
    },
    #[serde(rename = "deterministic_wave")]
    DeterministicWave { min: f64, max: f64 },
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct LayoutConfig {
    pub(super) regions: usize,
    pub(super) sst_count: usize,
    pub(super) rows_per_sst: usize,
    pub(super) row_group_size: usize,
    pub(super) series_count: NonZeroUsize,
    pub(super) start_unix_nanos: i64,
    pub(super) step_nanos: i64,
    pub(super) time_range_layout: String,
    pub(super) series_layout: String,
}
