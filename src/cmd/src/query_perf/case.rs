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

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::num::NonZeroU32;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};

use crate::query_perf::error::{Error, Result};

/// Current Rust-owned case schema version.
pub const CASE_SCHEMA_VERSION: u32 = 2;

/// Loads, validates, and normalizes a query performance case.
pub fn load_case(path: &Path) -> Result<ValidatedCase> {
    let text = fs::read_to_string(path)
        .map_err(|err| Error::new(format!("failed to read case {}: {err}", path.display())))?;
    let mut case = toml::from_str::<CaseFile>(&text)
        .map_err(|err| Error::new(format!("failed to parse case {}: {err}", path.display())))?;
    case.normalize()?;
    ValidatedCase::from_normalized(case)
}

/// The complete Rust-owned case document.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CaseFile {
    #[serde(default)]
    pub case: CaseMetadata,
    pub scenario: Scenario,
}

/// Non-executable report metadata.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct CaseMetadata {
    #[serde(flatten)]
    pub fields: BTreeMap<String, toml::Value>,
}

/// The executable scenario and its required query plan.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Scenario {
    #[serde(rename = "direct_readable_sst")]
    DirectReadableSst(DirectReadableSstScenario),
    #[serde(rename = "prom_remote_write_then_query")]
    PromRemoteWriteThenQuery(PromRemoteWriteThenQueryScenario),
}

/// Direct SST fixture scenario.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DirectReadableSstScenario {
    #[serde(default)]
    pub seed: Option<u64>,
    pub tables: Vec<TableConfig>,
    pub layout: LayoutConfig,
    pub queries: Vec<QueryConfig>,
}

/// Prometheus remote-write scenario.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PromRemoteWriteThenQueryScenario {
    pub queries: Vec<QueryConfig>,
    pub remote_write: PromRemoteWritePlan,
}

/// One explicitly declared query measurement.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QueryConfig {
    pub name: String,
    pub kind: QueryKind,
    #[serde(rename = "query")]
    pub text: String,
    pub database: String,
    #[serde(default)]
    pub warmup: u32,
    #[serde(default = "default_query_iterations")]
    pub iterations: NonZeroU32,
    #[serde(default)]
    pub thresholds: QueryThresholds,
}

/// Supported query protocol.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum QueryKind {
    Sql,
    Tql,
}

/// Thresholds enforced by the future runner.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryThresholds {
    #[serde(default)]
    pub max_candidate_latency_regression_pct: Option<f64>,
}

/// Remote-write fixture plan.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PromRemoteWritePlan {
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(alias = "metric_name")]
    pub metric: String,
    #[serde(default = "default_physical_table")]
    pub physical_table: String,
    #[serde(default = "default_series_count")]
    pub series_count: u64,
    #[serde(default = "default_samples_per_series")]
    pub samples_per_series: u64,
    #[serde(default = "default_start_unix_millis")]
    pub start_unix_millis: i64,
    #[serde(default = "default_step_millis")]
    pub step_millis: i64,
    #[serde(default = "default_chunk_series_count", alias = "batch_size")]
    pub chunk_series_count: u64,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub sample_chunk_size: Option<u64>,
    #[serde(default = "default_flush_every_sample_chunks")]
    pub flush_every_sample_chunks: u64,
    #[serde(default = "default_visibility_timeout_seconds")]
    pub visibility_timeout_seconds: u64,
    #[serde(default)]
    pub prom_store: PromStoreConfig,
    #[serde(default)]
    pub value: ValueConfig,
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    #[serde(default)]
    pub read_bench: Option<ReadBenchConfig>,
}

/// Defaults for a small local remote-write fixture.
pub fn default_database() -> String {
    "public".to_string()
}
pub fn default_remote_write_endpoint() -> String {
    "http://127.0.0.1:4000/v1/prometheus/write".to_string()
}
pub fn default_physical_table() -> String {
    "greptime_physical_table".to_string()
}
pub fn default_series_count() -> u64 {
    8
}
pub fn default_samples_per_series() -> u64 {
    30
}
pub fn default_start_unix_millis() -> i64 {
    1_704_067_200_000
}
pub fn default_step_millis() -> i64 {
    15_000
}
pub fn default_chunk_series_count() -> u64 {
    8
}
pub fn default_timeout_seconds() -> u64 {
    60
}
pub fn default_flush_every_sample_chunks() -> u64 {
    1
}
pub fn default_visibility_timeout_seconds() -> u64 {
    30
}
pub fn default_query_iterations() -> NonZeroU32 {
    NonZeroU32::new(1).unwrap_or(NonZeroU32::MIN)
}

/// Prometheus store tuning for a fixture target.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PromStoreConfig {
    #[serde(default = "default_pending_rows_flush_interval")]
    pub pending_rows_flush_interval: String,
    #[serde(default = "default_max_batch_rows")]
    pub max_batch_rows: u64,
    #[serde(default = "default_max_concurrent_flushes")]
    pub max_concurrent_flushes: u64,
    #[serde(default = "default_worker_channel_capacity")]
    pub worker_channel_capacity: u64,
    #[serde(default = "default_max_inflight_requests")]
    pub max_inflight_requests: u64,
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

pub fn default_pending_rows_flush_interval() -> String {
    "1s".to_string()
}
pub fn default_max_batch_rows() -> u64 {
    100_000
}
pub fn default_max_concurrent_flushes() -> u64 {
    256
}
pub fn default_worker_channel_capacity() -> u64 {
    65_526
}
pub fn default_max_inflight_requests() -> u64 {
    3_000
}

/// Deterministic remote-write value distribution.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ValuePattern {
    Linear,
    Constant,
    Modulo,
    Unique,
    SeededRandom,
    RunLength,
    QuantizedSignal,
    SignalWithSporadicStalls,
    MixedSignalRepeated,
}

impl std::fmt::Display for ValuePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Linear => "linear",
            Self::Constant => "constant",
            Self::Modulo => "modulo",
            Self::Unique => "unique",
            Self::SeededRandom => "seeded_random",
            Self::RunLength => "run_length",
            Self::QuantizedSignal => "quantized_signal",
            Self::SignalWithSporadicStalls => "signal_with_sporadic_stalls",
            Self::MixedSignalRepeated => "mixed_signal_repeated",
        })
    }
}

impl std::str::FromStr for ValuePattern {
    type Err = String;
    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "linear" => Ok(Self::Linear),
            "constant" => Ok(Self::Constant),
            "modulo" => Ok(Self::Modulo),
            "unique" => Ok(Self::Unique),
            "seeded_random" => Ok(Self::SeededRandom),
            "run_length" => Ok(Self::RunLength),
            "quantized_signal" => Ok(Self::QuantizedSignal),
            "signal_with_sporadic_stalls" => Ok(Self::SignalWithSporadicStalls),
            "mixed_signal_repeated" => Ok(Self::MixedSignalRepeated),
            _ => Err(format!("unsupported value pattern: {value}")),
        }
    }
}

/// Parameters for a remote-write value distribution.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ValueConfig {
    #[serde(default = "default_value_pattern")]
    pub pattern: ValuePattern,
    #[serde(default)]
    pub base: f64,
    #[serde(default = "default_value_step")]
    pub step: f64,
    #[serde(default = "default_value_cardinality")]
    pub cardinality: u64,
    #[serde(default)]
    pub seed: u64,
    #[serde(default = "default_value_run_length")]
    pub run_length: u64,
    #[serde(default = "default_value_stall_every")]
    pub stall_every: u64,
    #[serde(default = "default_value_stall_length")]
    pub stall_length: u64,
    #[serde(default = "default_value_mixed_every")]
    pub mixed_every: u64,
}

impl Default for ValueConfig {
    fn default() -> Self {
        Self {
            pattern: default_value_pattern(),
            base: default_value_base(),
            step: default_value_step(),
            cardinality: default_value_cardinality(),
            seed: default_value_seed(),
            run_length: default_value_run_length(),
            stall_every: default_value_stall_every(),
            stall_length: default_value_stall_length(),
            mixed_every: default_value_mixed_every(),
        }
    }
}

pub fn default_value_pattern() -> ValuePattern {
    ValuePattern::Linear
}
pub fn default_value_step() -> f64 {
    0.125
}
pub fn default_value_base() -> f64 {
    0.0
}
pub fn default_value_cardinality() -> u64 {
    97
}
pub fn default_value_run_length() -> u64 {
    8
}
pub fn default_value_stall_every() -> u64 {
    100
}
pub fn default_value_stall_length() -> u64 {
    16
}
pub fn default_value_mixed_every() -> u64 {
    5
}
pub fn default_value_seed() -> u64 {
    0
}
pub fn default_value_sample_offset() -> u64 {
    0
}

/// Typed storage-footer thresholds.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StorageConfig {
    #[serde(default = "default_true")]
    pub inspect: bool,
    #[serde(default = "default_storage_column")]
    pub column: String,
    #[serde(default)]
    pub root_suffix: Option<String>,
    #[serde(default)]
    pub include_metadata_files: bool,
    #[serde(default = "default_min_files")]
    pub min_files: u64,
    #[serde(default = "default_min_files")]
    pub min_files_with_column: u64,
    #[serde(default)]
    pub require_encodings: Vec<String>,
    #[serde(default)]
    pub forbid_encodings: Vec<String>,
    #[serde(default)]
    pub max_total_file_size_bytes: Option<u64>,
    #[serde(default)]
    pub max_column_compressed_size_bytes: Option<u64>,
    #[serde(default)]
    pub max_column_uncompressed_size_bytes: Option<u64>,
    #[serde(default)]
    pub max_candidate_total_file_size_regression_pct: Option<f64>,
    #[serde(default)]
    pub max_candidate_column_compressed_size_regression_pct: Option<f64>,
    #[serde(default)]
    pub max_candidate_column_uncompressed_size_regression_pct: Option<f64>,
    #[serde(skip_deserializing, default)]
    pub planned_thresholds: Vec<StorageThresholdPlan>,
}

/// A normalized storage threshold shown in a dry-run plan.
#[derive(Debug, Serialize)]
struct StorageThresholdPlan {
    pub threshold: String,
    pub status: String,
    pub value: serde_json::Value,
}

impl StorageConfig {
    fn populate_planned_thresholds(&mut self) {
        let mut thresholds = vec![
            StorageThresholdPlan::number("min_files", self.min_files),
            StorageThresholdPlan::number("min_files_with_column", self.min_files_with_column),
        ];
        if !self.require_encodings.is_empty() {
            thresholds.push(StorageThresholdPlan::strings(
                "require_encodings",
                &self.require_encodings,
            ));
        }
        if !self.forbid_encodings.is_empty() {
            thresholds.push(StorageThresholdPlan::strings(
                "forbid_encodings",
                &self.forbid_encodings,
            ));
        }
        for (name, value) in [
            ("max_total_file_size_bytes", self.max_total_file_size_bytes),
            (
                "max_column_compressed_size_bytes",
                self.max_column_compressed_size_bytes,
            ),
            (
                "max_column_uncompressed_size_bytes",
                self.max_column_uncompressed_size_bytes,
            ),
        ] {
            if let Some(value) = value {
                thresholds.push(StorageThresholdPlan::number(name, value));
            }
        }
        for (name, value) in [
            (
                "max_candidate_total_file_size_regression_pct",
                self.max_candidate_total_file_size_regression_pct,
            ),
            (
                "max_candidate_column_compressed_size_regression_pct",
                self.max_candidate_column_compressed_size_regression_pct,
            ),
            (
                "max_candidate_column_uncompressed_size_regression_pct",
                self.max_candidate_column_uncompressed_size_regression_pct,
            ),
        ] {
            if let Some(value) = value {
                thresholds.push(StorageThresholdPlan::float(name, value));
            }
        }
        self.planned_thresholds = thresholds;
    }
}

impl StorageThresholdPlan {
    fn number(name: &str, value: u64) -> Self {
        Self {
            threshold: name.to_string(),
            status: "planned".to_string(),
            value: value.into(),
        }
    }
    fn float(name: &str, value: f64) -> Self {
        Self {
            threshold: name.to_string(),
            status: "planned".to_string(),
            value: serde_json::json!(value),
        }
    }
    fn strings(name: &str, value: &[String]) -> Self {
        Self {
            threshold: name.to_string(),
            status: "planned".to_string(),
            value: serde_json::json!(value),
        }
    }
}

pub fn default_true() -> bool {
    true
}
pub fn default_storage_column() -> String {
    "greptime_value".to_string()
}
pub fn default_min_files() -> u64 {
    1
}

/// Typed read-benchmark configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReadBenchConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub parquetbench: bool,
    #[serde(default = "default_true")]
    pub scanbench: bool,
    #[serde(default = "default_iterations")]
    pub iterations: u64,
    #[serde(default)]
    pub projection: Vec<String>,
    #[serde(default = "default_parquet_reader")]
    pub parquet_reader: String,
    #[serde(default = "default_scan_scanner")]
    pub scan_scanner: String,
    #[serde(default = "default_parallelism")]
    pub parallelism: u64,
    #[serde(default)]
    pub max_files: Option<usize>,
}

impl Default for ReadBenchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            parquetbench: true,
            scanbench: true,
            iterations: default_iterations(),
            projection: vec![],
            parquet_reader: default_parquet_reader(),
            scan_scanner: default_scan_scanner(),
            parallelism: default_parallelism(),
            max_files: None,
        }
    }
}

pub fn default_iterations() -> u64 {
    7
}
pub fn default_parquet_reader() -> String {
    "direct".to_string()
}
pub fn default_scan_scanner() -> String {
    "seq".to_string()
}
pub fn default_parallelism() -> u64 {
    1
}

/// Fixture table schema.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TableConfig {
    pub database: String,
    pub name: String,
    pub engine: String,
    #[serde(default)]
    pub append_mode: Option<bool>,
    #[serde(default)]
    pub sst_format: Option<String>,
    pub primary_key: Vec<String>,
    pub time_index: String,
    pub columns: Vec<ColumnConfig>,
}

/// Fixture column schema and generator distribution.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub semantic: String,
    #[serde(default)]
    pub distribution: Option<Distribution>,
}

/// Supported deterministic fixture distributions.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Distribution {
    #[serde(rename = "cardinality")]
    Cardinality { values: u64, prefix: String },
    #[serde(rename = "deterministic_wave")]
    DeterministicWave { min: f64, max: f64 },
}

/// Direct-SST physical layout.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LayoutConfig {
    pub regions: usize,
    pub sst_count: usize,
    pub rows_per_sst: usize,
    pub row_group_size: usize,
    pub series_count: usize,
    pub start_unix_nanos: i64,
    pub step_nanos: i64,
    pub time_range_layout: String,
    pub series_layout: String,
}

/// A case that has passed normalization and validation. Parsed TOML types stay
/// private so fixture generators cannot bypass those checks.
#[derive(Debug)]
pub struct ValidatedCase {
    scenario: ValidatedScenario,
}

#[derive(Debug)]
pub enum ValidatedScenario {
    DirectReadableSst(ValidatedDirectSstScenario),
    PromRemoteWriteThenQuery(ValidatedRemoteWriteScenario),
}

#[derive(Debug)]
pub struct ValidatedDirectSstScenario {
    seed: Option<u64>,
    tables: Vec<ValidatedTable>,
    layout: ValidatedLayout,
    queries: Vec<ValidatedQuery>,
    plan: serde_json::Value,
}

#[derive(Debug)]
pub struct ValidatedRemoteWriteScenario {
    queries: Vec<ValidatedQuery>,
    remote_write: ValidatedRemoteWritePlan,
    plan: serde_json::Value,
}

#[derive(Debug)]
pub struct ValidatedQuery {
    identity: ResolvedQueryIdentity,
    text: String,
    warmup: u32,
    iterations: NonZeroU32,
    max_candidate_latency_regression_pct: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct ResolvedQueryIdentity {
    name: String,
    database: String,
    kind: ValidatedQueryKind,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidatedQueryKind {
    Sql,
    Tql,
}

#[derive(Debug)]
pub struct ValidatedTable {
    database: String,
    name: String,
    engine: String,
    append_mode: Option<bool>,
    sst_format: ValidatedSstFormat,
    sst_format_explicit: bool,
    primary_key: Vec<String>,
    time_index: String,
    columns: Vec<ValidatedColumn>,
}

#[derive(Debug, Clone, Copy)]
pub enum ValidatedSstFormat {
    Flat,
    PrimaryKey,
}

#[derive(Debug)]
pub struct ValidatedColumn {
    name: String,
    ty: ValidatedColumnType,
    semantic: ValidatedSemanticType,
    distribution: Option<ValidatedDistribution>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ValidatedColumnType {
    String,
    Float64,
    Uint64,
    TimestampNanosecond,
    TimestampMillisecond,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ValidatedSemanticType {
    Tag,
    Field,
    Timestamp,
}

#[derive(Debug)]
pub enum ValidatedDistribution {
    Cardinality { values: u64, prefix: String },
    DeterministicWave { min: f64, max: f64 },
}

#[derive(Debug)]
pub struct ValidatedLayout {
    regions: usize,
    sst_count: usize,
    rows_per_sst: usize,
    row_group_size: usize,
    series_count: usize,
    start_unix_nanos: i64,
    step_nanos: i64,
    series_layout: ValidatedSeriesLayout,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ValidatedSeriesLayout {
    RoundRobin,
    TimestampMajor,
    PerSst,
}

#[derive(Debug)]
pub struct ValidatedRemoteWritePlan {
    database: String,
    metric: String,
    physical_table: String,
    series_count: u64,
    samples_per_series: u64,
    start_unix_millis: i64,
    step_millis: i64,
    chunk_series_count: u64,
    timeout_seconds: u64,
    sample_chunk_size: Option<u64>,
    flush_every_sample_chunks: u64,
    visibility_timeout_seconds: u64,
    value: ValidatedRemoteWriteValue,
    storage_requested: bool,
    read_bench_requested: bool,
    read_bench_parquet: bool,
    read_bench_scan: bool,
    storage_thresholds: Option<StorageThresholds>,
}

/// Server-visible fixture identity derived exclusively from a validated
/// remote-write workload. `logical_metric_table` is the Prometheus metric table
/// exposed to queries; `physical_table` is the distinct table used by the
/// remote-write storage contract.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RemoteWriteFixtureExpectation {
    database: String,
    logical_metric_table: String,
    physical_table: String,
    logical_summary: ExpectedLogicalFixtureSummary,
}

/// Logical summary shared by all direct-SST tables. The direct fixture generator
/// applies one validated layout to every declared table, so this summary is
/// deliberately per table rather than a sum across the case's tables.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ExpectedLogicalFixtureSummary {
    rows: u64,
    series: u64,
    time_start_unix_nanos: i64,
    time_end_unix_nanos: i64,
}

/// Exact, normalized remote-write value parameters for the endpoint runner.
#[derive(Debug, Clone)]
pub struct ValidatedRemoteWriteValue {
    pattern: ValuePattern,
    base: f64,
    step: f64,
    cardinality: u64,
    seed: u64,
    run_length: u64,
    stall_every: u64,
    stall_length: u64,
    mixed_every: u64,
}

/// Typed footer thresholds consumed by the controller-result evaluator.
#[derive(Debug, Clone)]
pub struct StorageThresholds {
    min_files: u64,
    min_files_with_column: u64,
    require_encodings: Vec<String>,
    forbid_encodings: Vec<String>,
    max_total_file_size_bytes: Option<u64>,
    max_column_compressed_size_bytes: Option<u64>,
    max_column_uncompressed_size_bytes: Option<u64>,
    max_candidate_total_file_size_regression_pct: Option<f64>,
    max_candidate_column_compressed_size_regression_pct: Option<f64>,
    max_candidate_column_uncompressed_size_regression_pct: Option<f64>,
}

impl ValidatedCase {
    fn from_normalized(case: CaseFile) -> Result<Self> {
        let scenario = match case.scenario {
            Scenario::DirectReadableSst(raw) => {
                let mut plan = serde_json::to_value(&raw).map_err(|err| {
                    Error::new(format!("failed to serialize direct SST plan: {err}"))
                })?;
                plan.as_object_mut()
                    .ok_or_else(|| Error::new("direct SST plan must serialize as an object"))?
                    .insert("kind".to_string(), serde_json::json!("direct_readable_sst"));
                ValidatedScenario::DirectReadableSst(ValidatedDirectSstScenario {
                    seed: raw.seed,
                    tables: raw
                        .tables
                        .into_iter()
                        .map(ValidatedTable::from_raw)
                        .collect::<Result<_>>()?,
                    layout: ValidatedLayout::from_raw(raw.layout)?,
                    queries: raw
                        .queries
                        .into_iter()
                        .map(ValidatedQuery::from_raw)
                        .collect(),
                    plan,
                })
            }
            Scenario::PromRemoteWriteThenQuery(raw) => {
                let mut plan = serde_json::to_value(&raw).map_err(|err| {
                    Error::new(format!("failed to serialize remote-write plan: {err}"))
                })?;
                plan.as_object_mut()
                    .ok_or_else(|| Error::new("remote-write plan must serialize as an object"))?
                    .insert(
                        "kind".to_string(),
                        serde_json::json!("prom_remote_write_then_query"),
                    );
                ValidatedScenario::PromRemoteWriteThenQuery(ValidatedRemoteWriteScenario {
                    queries: raw
                        .queries
                        .into_iter()
                        .map(ValidatedQuery::from_raw)
                        .collect(),
                    remote_write: ValidatedRemoteWritePlan::from_raw(raw.remote_write),
                    plan,
                })
            }
        };
        Ok(Self { scenario })
    }
    pub fn kind(&self) -> &'static str {
        match self.scenario {
            ValidatedScenario::DirectReadableSst(_) => "direct_readable_sst",
            ValidatedScenario::PromRemoteWriteThenQuery(_) => "prom_remote_write_then_query",
        }
    }
    pub fn direct_readable_sst(&self) -> Result<&ValidatedDirectSstScenario> {
        match &self.scenario {
            ValidatedScenario::DirectReadableSst(scenario) => Ok(scenario),
            _ => Err(Error::new("scenario is not direct_readable_sst")),
        }
    }
    pub fn scenario_plan(&self) -> serde_json::Value {
        match &self.scenario {
            ValidatedScenario::DirectReadableSst(value) => value.plan.clone(),
            ValidatedScenario::PromRemoteWriteThenQuery(value) => value.plan.clone(),
        }
    }
    /// Returns the exact, normalized query plan without exposing parsed TOML.
    pub fn queries(&self) -> &[ValidatedQuery] {
        match &self.scenario {
            ValidatedScenario::DirectReadableSst(value) => value.queries(),
            ValidatedScenario::PromRemoteWriteThenQuery(value) => value.queries(),
        }
    }
    /// Converts the executable query plan to its canonical manifest identity.
    pub fn manifest_queries(
        &self,
    ) -> crate::query_perf::error::Result<Vec<crate::query_perf::manifest::ResolvedQueryIdentity>>
    {
        self.queries()
            .iter()
            .map(ValidatedQuery::manifest_identity)
            .collect()
    }
    /// Returns the API-write plan only for a remote-write scenario.
    pub fn remote_write(&self) -> Option<&ValidatedRemoteWritePlan> {
        match &self.scenario {
            ValidatedScenario::PromRemoteWriteThenQuery(value) => Some(value.remote_write()),
            ValidatedScenario::DirectReadableSst(_) => None,
        }
    }
    /// Returns controller observation kinds explicitly requested by the normalized case.
    pub fn observation_kinds(&self) -> Vec<crate::query_perf::manifest::ObservationKind> {
        match self.remote_write() {
            Some(plan) => {
                let mut kinds = Vec::new();
                if plan.storage_requested() {
                    kinds.push(crate::query_perf::manifest::ObservationKind::Footer);
                }
                if plan.read_bench_requested() {
                    kinds.push(crate::query_perf::manifest::ObservationKind::ReadBench);
                }
                kinds
            }
            None => vec![],
        }
    }
    pub fn storage_thresholds(&self) -> Option<&StorageThresholds> {
        self.remote_write()
            .and_then(ValidatedRemoteWritePlan::storage_thresholds)
    }
    /// Derives the exact server-visible identity required by an API-write fixture.
    pub fn remote_write_fixture_expectation(&self) -> Result<RemoteWriteFixtureExpectation> {
        self.remote_write()
            .ok_or_else(|| Error::new("case is not a remote-write fixture"))?
            .fixture_expectation()
    }
    /// Derives the one per-table direct-SST logical layout. The fixture generator
    /// uses this same layout for each validated table, which is why the manifest
    /// can retain a global logical summary without hiding table-specific values.
    pub fn direct_fixture_logical_summary(&self) -> Result<ExpectedLogicalFixtureSummary> {
        let direct = self.direct_readable_sst()?;
        direct.layout().logical_summary()
    }
    /// Digest of every normalized executable input, including workload and queries.
    pub fn executable_digest(&self) -> Result<crate::query_perf::manifest::Sha256Digest> {
        let body = serde_json::json!({
            "schema_version": CASE_SCHEMA_VERSION,
            "scenario": self.scenario_plan(),
        });
        serde_json::to_vec(&body)
            .map(crate::query_perf::manifest::Sha256Digest::compute)
            .map_err(|err| Error::new(format!("failed to serialize executable case: {err}")))
    }
    /// Digest of the normalized fixture workload, excluding only query execution.
    pub fn fixture_plan_digest(&self) -> Result<crate::query_perf::manifest::Sha256Digest> {
        let mut scenario = self.scenario_plan();
        scenario
            .as_object_mut()
            .ok_or_else(|| Error::new("normalized scenario must be an object"))?
            .remove("queries");
        serde_json::to_vec(&serde_json::json!({
            "schema_version": CASE_SCHEMA_VERSION,
            "scenario": scenario,
        }))
        .map(crate::query_perf::manifest::Sha256Digest::compute)
        .map_err(|err| Error::new(format!("failed to serialize fixture plan: {err}")))
    }
}
impl ValidatedDirectSstScenario {
    pub fn seed(&self) -> Option<u64> {
        self.seed
    }
    pub fn tables(&self) -> &[ValidatedTable] {
        &self.tables
    }
    pub fn layout(&self) -> &ValidatedLayout {
        &self.layout
    }
    pub fn queries(&self) -> &[ValidatedQuery] {
        &self.queries
    }
}
impl ValidatedRemoteWriteScenario {
    pub fn queries(&self) -> &[ValidatedQuery] {
        &self.queries
    }
    pub fn remote_write(&self) -> &ValidatedRemoteWritePlan {
        &self.remote_write
    }
}
impl ValidatedQuery {
    fn from_raw(raw: QueryConfig) -> Self {
        Self {
            identity: ResolvedQueryIdentity {
                name: raw.name,
                database: raw.database,
                kind: match raw.kind {
                    QueryKind::Sql => ValidatedQueryKind::Sql,
                    QueryKind::Tql => ValidatedQueryKind::Tql,
                },
            },
            text: raw.text,
            warmup: raw.warmup,
            iterations: raw.iterations,
            max_candidate_latency_regression_pct: raw
                .thresholds
                .max_candidate_latency_regression_pct,
        }
    }
    pub fn resolved_identity(&self) -> &ResolvedQueryIdentity {
        &self.identity
    }
    pub fn text(&self) -> &str {
        &self.text
    }
    pub fn warmup(&self) -> u32 {
        self.warmup
    }
    pub fn iterations(&self) -> NonZeroU32 {
        self.iterations
    }
    pub fn max_candidate_latency_regression_pct(&self) -> Option<f64> {
        self.max_candidate_latency_regression_pct
    }
    pub fn manifest_identity(&self) -> Result<crate::query_perf::manifest::ResolvedQueryIdentity> {
        Ok(crate::query_perf::manifest::ResolvedQueryIdentity {
            name: self.identity.name.clone(),
            kind: match self.identity.kind {
                ValidatedQueryKind::Sql => QueryKind::Sql,
                ValidatedQueryKind::Tql => QueryKind::Tql,
            },
            database: self.identity.database.clone(),
            query: self.text.clone(),
            warmup: self.warmup,
            iterations: self.iterations.get(),
        })
    }
}
impl ResolvedQueryIdentity {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn database(&self) -> &str {
        &self.database
    }
    pub fn kind(&self) -> ValidatedQueryKind {
        self.kind
    }
}
impl Serialize for ResolvedQueryIdentity {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Identity<'a> {
            name: &'a str,
            database: &'a str,
            kind: ValidatedQueryKind,
        }
        Identity {
            name: &self.name,
            database: &self.database,
            kind: self.kind,
        }
        .serialize(serializer)
    }
}
impl ValidatedTable {
    fn from_raw(raw: TableConfig) -> Result<Self> {
        let sst_format = match raw.sst_format.as_deref() {
            None | Some("flat") => ValidatedSstFormat::Flat,
            Some("primary_key" | "primary-key") => ValidatedSstFormat::PrimaryKey,
            Some(value) => return Err(Error::new(format!("unsupported sst_format: {value}"))),
        };
        let sst_format_explicit = raw.sst_format.is_some();
        Ok(Self {
            database: raw.database,
            name: raw.name,
            engine: raw.engine,
            append_mode: raw.append_mode,
            sst_format,
            sst_format_explicit,
            primary_key: raw.primary_key,
            time_index: raw.time_index,
            columns: raw
                .columns
                .into_iter()
                .map(ValidatedColumn::from_raw)
                .collect::<Result<_>>()?,
        })
    }
    pub fn database(&self) -> &str {
        &self.database
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn engine(&self) -> &str {
        &self.engine
    }
    pub fn append_mode(&self) -> Option<bool> {
        self.append_mode
    }
    pub fn sst_format(&self) -> ValidatedSstFormat {
        self.sst_format
    }
    pub fn sst_format_option(&self) -> Option<&'static str> {
        self.sst_format_explicit.then_some(match self.sst_format {
            ValidatedSstFormat::Flat => "flat",
            ValidatedSstFormat::PrimaryKey => "primary_key",
        })
    }
    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }
    pub fn time_index(&self) -> &str {
        &self.time_index
    }
    pub fn columns(&self) -> &[ValidatedColumn] {
        &self.columns
    }
}
impl ValidatedColumn {
    fn from_raw(raw: ColumnConfig) -> Result<Self> {
        let ty = match raw.ty.as_str() {
            "STRING" => ValidatedColumnType::String,
            "DOUBLE" => ValidatedColumnType::Float64,
            "UINT64" => ValidatedColumnType::Uint64,
            "TIMESTAMP(9)" => ValidatedColumnType::TimestampNanosecond,
            "TIMESTAMP(3)" => ValidatedColumnType::TimestampMillisecond,
            value => return Err(Error::new(format!("unsupported column type {value}"))),
        };
        let semantic = match raw.semantic.as_str() {
            "tag" => ValidatedSemanticType::Tag,
            "field" => ValidatedSemanticType::Field,
            "timestamp" => ValidatedSemanticType::Timestamp,
            value => return Err(Error::new(format!("unsupported semantic {value}"))),
        };
        let distribution = match raw.distribution {
            Some(Distribution::Cardinality { values, prefix }) => {
                Some(ValidatedDistribution::Cardinality { values, prefix })
            }
            Some(Distribution::DeterministicWave { min, max }) => {
                Some(ValidatedDistribution::DeterministicWave { min, max })
            }
            None => None,
        };
        Ok(Self {
            name: raw.name,
            ty,
            semantic,
            distribution,
        })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn ty(&self) -> ValidatedColumnType {
        self.ty
    }
    pub fn semantic(&self) -> ValidatedSemanticType {
        self.semantic
    }
    pub fn distribution(&self) -> Option<&ValidatedDistribution> {
        self.distribution.as_ref()
    }
}
impl ValidatedLayout {
    fn from_raw(raw: LayoutConfig) -> Result<Self> {
        let series_layout = match raw.series_layout.as_str() {
            "round_robin" => ValidatedSeriesLayout::RoundRobin,
            "timestamp_major" => ValidatedSeriesLayout::TimestampMajor,
            "per_sst" => ValidatedSeriesLayout::PerSst,
            value => return Err(Error::new(format!("unsupported series_layout: {value}"))),
        };
        Ok(Self {
            regions: raw.regions,
            sst_count: raw.sst_count,
            rows_per_sst: raw.rows_per_sst,
            row_group_size: raw.row_group_size,
            series_count: raw.series_count,
            start_unix_nanos: raw.start_unix_nanos,
            step_nanos: raw.step_nanos,
            series_layout,
        })
    }
    pub fn sst_count(&self) -> usize {
        self.sst_count
    }
    pub fn regions(&self) -> usize {
        self.regions
    }
    pub fn rows_per_sst(&self) -> usize {
        self.rows_per_sst
    }
    pub fn row_group_size(&self) -> usize {
        self.row_group_size
    }
    pub fn series_count(&self) -> usize {
        self.series_count
    }
    pub fn start_unix_nanos(&self) -> i64 {
        self.start_unix_nanos
    }
    pub fn step_nanos(&self) -> i64 {
        self.step_nanos
    }
    pub fn series_layout(&self) -> ValidatedSeriesLayout {
        self.series_layout
    }
    pub fn logical_summary(&self) -> Result<ExpectedLogicalFixtureSummary> {
        let rows = u64::try_from(self.sst_count)
            .map_err(|_| Error::new("layout.sst_count exceeds u64"))?
            .checked_mul(
                u64::try_from(self.rows_per_sst)
                    .map_err(|_| Error::new("layout.rows_per_sst exceeds u64"))?,
            )
            .ok_or_else(|| Error::new("layout.sst_count * rows_per_sst overflows"))?;
        let series = u64::try_from(self.series_count)
            .map_err(|_| Error::new("layout.series_count exceeds u64"))?;
        let time_steps = match self.series_layout {
            ValidatedSeriesLayout::TimestampMajor => rows
                .checked_div(series)
                .ok_or_else(|| Error::new("layout series count must be positive"))?,
            _ => rows,
        };
        let end_offset = i64::try_from(
            time_steps
                .checked_sub(1)
                .ok_or_else(|| Error::new("layout has no generated rows"))?,
        )
        .map_err(|_| Error::new("layout timestamp step exceeds i64"))?
        .checked_mul(self.step_nanos)
        .ok_or_else(|| Error::new("layout timestamp multiplication overflows"))?;
        Ok(ExpectedLogicalFixtureSummary {
            rows,
            series,
            time_start_unix_nanos: self.start_unix_nanos,
            time_end_unix_nanos: self
                .start_unix_nanos
                .checked_add(end_offset)
                .ok_or_else(|| Error::new("layout timestamp range overflows"))?,
        })
    }
}
impl ValidatedRemoteWritePlan {
    fn from_raw(raw: PromRemoteWritePlan) -> Self {
        Self {
            database: raw.database,
            metric: raw.metric,
            physical_table: raw.physical_table,
            series_count: raw.series_count,
            samples_per_series: raw.samples_per_series,
            start_unix_millis: raw.start_unix_millis,
            step_millis: raw.step_millis,
            chunk_series_count: raw.chunk_series_count,
            timeout_seconds: raw.timeout_seconds,
            sample_chunk_size: raw.sample_chunk_size,
            flush_every_sample_chunks: raw.flush_every_sample_chunks,
            visibility_timeout_seconds: raw.visibility_timeout_seconds,
            value: ValidatedRemoteWriteValue {
                pattern: raw.value.pattern,
                base: raw.value.base,
                step: raw.value.step,
                cardinality: raw.value.cardinality,
                seed: raw.value.seed,
                run_length: raw.value.run_length,
                stall_every: raw.value.stall_every,
                stall_length: raw.value.stall_length,
                mixed_every: raw.value.mixed_every,
            },
            storage_requested: raw.storage.as_ref().is_some_and(|value| value.inspect),
            read_bench_requested: raw.read_bench.as_ref().is_some_and(|value| value.enabled),
            read_bench_parquet: raw
                .read_bench
                .as_ref()
                .is_some_and(|value| value.enabled && value.parquetbench),
            read_bench_scan: raw
                .read_bench
                .as_ref()
                .is_some_and(|value| value.enabled && value.scanbench),
            storage_thresholds: raw.storage.as_ref().map(|storage| StorageThresholds {
                min_files: storage.min_files,
                min_files_with_column: storage.min_files_with_column,
                require_encodings: storage.require_encodings.clone(),
                forbid_encodings: storage.forbid_encodings.clone(),
                max_total_file_size_bytes: storage.max_total_file_size_bytes,
                max_column_compressed_size_bytes: storage.max_column_compressed_size_bytes,
                max_column_uncompressed_size_bytes: storage.max_column_uncompressed_size_bytes,
                max_candidate_total_file_size_regression_pct: storage
                    .max_candidate_total_file_size_regression_pct,
                max_candidate_column_compressed_size_regression_pct: storage
                    .max_candidate_column_compressed_size_regression_pct,
                max_candidate_column_uncompressed_size_regression_pct: storage
                    .max_candidate_column_uncompressed_size_regression_pct,
            }),
        }
    }
    pub fn database(&self) -> &str {
        &self.database
    }
    pub fn metric(&self) -> &str {
        &self.metric
    }
    pub fn physical_table(&self) -> &str {
        &self.physical_table
    }
    pub fn series_count(&self) -> u64 {
        self.series_count
    }
    pub fn samples_per_series(&self) -> u64 {
        self.samples_per_series
    }
    pub fn start_unix_millis(&self) -> i64 {
        self.start_unix_millis
    }
    pub fn step_millis(&self) -> i64 {
        self.step_millis
    }
    pub fn chunk_series_count(&self) -> u64 {
        self.chunk_series_count
    }
    pub fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }
    pub fn sample_chunk_size(&self) -> Option<u64> {
        self.sample_chunk_size
    }
    pub fn flush_every_sample_chunks(&self) -> u64 {
        self.flush_every_sample_chunks
    }
    pub fn visibility_timeout_seconds(&self) -> u64 {
        self.visibility_timeout_seconds
    }
    pub fn value(&self) -> &ValidatedRemoteWriteValue {
        &self.value
    }
    pub fn storage_requested(&self) -> bool {
        self.storage_requested
    }
    pub fn read_bench_requested(&self) -> bool {
        self.read_bench_requested
    }
    pub fn read_bench_parquet(&self) -> bool {
        self.read_bench_parquet
    }
    pub fn read_bench_scan(&self) -> bool {
        self.read_bench_scan
    }
    pub fn storage_thresholds(&self) -> Option<&StorageThresholds> {
        self.storage_thresholds.as_ref()
    }
    /// The time bounds are inclusive: the final sample is at
    /// `start + (samples_per_series - 1) * step`.
    pub fn fixture_expectation(&self) -> Result<RemoteWriteFixtureExpectation> {
        let rows = self
            .series_count
            .checked_mul(self.samples_per_series)
            .ok_or_else(|| Error::new("series_count * samples_per_series overflows"))?;
        let start = self
            .start_unix_millis
            .checked_mul(1_000_000)
            .ok_or_else(|| Error::new("remote_write start timestamp overflows nanoseconds"))?;
        let end_offset = i64::try_from(
            self.samples_per_series
                .checked_sub(1)
                .ok_or_else(|| Error::new("samples_per_series must be positive"))?,
        )
        .map_err(|_| Error::new("samples_per_series exceeds i64"))?
        .checked_mul(self.step_millis)
        .and_then(|millis| millis.checked_mul(1_000_000))
        .ok_or_else(|| Error::new("remote_write end timestamp overflows nanoseconds"))?;
        Ok(RemoteWriteFixtureExpectation {
            database: self.database.clone(),
            logical_metric_table: self.metric.clone(),
            physical_table: self.physical_table.clone(),
            logical_summary: ExpectedLogicalFixtureSummary {
                rows,
                series: self.series_count,
                time_start_unix_nanos: start,
                time_end_unix_nanos: start.checked_add(end_offset).ok_or_else(|| {
                    Error::new("remote_write end timestamp overflows nanoseconds")
                })?,
            },
        })
    }
}
impl RemoteWriteFixtureExpectation {
    pub fn database(&self) -> &str {
        &self.database
    }
    pub fn logical_metric_table(&self) -> &str {
        &self.logical_metric_table
    }
    pub fn physical_table(&self) -> &str {
        &self.physical_table
    }
    pub fn logical_summary(&self) -> ExpectedLogicalFixtureSummary {
        self.logical_summary
    }
}
impl ExpectedLogicalFixtureSummary {
    pub fn rows(&self) -> u64 {
        self.rows
    }
    pub fn series(&self) -> u64 {
        self.series
    }
    pub fn time_start_unix_nanos(&self) -> i64 {
        self.time_start_unix_nanos
    }
    pub fn time_end_unix_nanos(&self) -> i64 {
        self.time_end_unix_nanos
    }
}
impl ValidatedRemoteWriteValue {
    pub fn pattern(&self) -> ValuePattern {
        self.pattern
    }
    pub fn base(&self) -> f64 {
        self.base
    }
    pub fn step(&self) -> f64 {
        self.step
    }
    pub fn cardinality(&self) -> u64 {
        self.cardinality
    }
    pub fn seed(&self) -> u64 {
        self.seed
    }
    pub fn run_length(&self) -> u64 {
        self.run_length
    }
    pub fn stall_every(&self) -> u64 {
        self.stall_every
    }
    pub fn stall_length(&self) -> u64 {
        self.stall_length
    }
    pub fn mixed_every(&self) -> u64 {
        self.mixed_every
    }
}
impl StorageThresholds {
    pub fn min_files(&self) -> u64 {
        self.min_files
    }
    pub fn min_files_with_column(&self) -> u64 {
        self.min_files_with_column
    }
    pub fn require_encodings(&self) -> &[String] {
        &self.require_encodings
    }
    pub fn forbid_encodings(&self) -> &[String] {
        &self.forbid_encodings
    }
    pub fn max_total_file_size_bytes(&self) -> Option<u64> {
        self.max_total_file_size_bytes
    }
    pub fn max_column_compressed_size_bytes(&self) -> Option<u64> {
        self.max_column_compressed_size_bytes
    }
    pub fn max_column_uncompressed_size_bytes(&self) -> Option<u64> {
        self.max_column_uncompressed_size_bytes
    }
    pub fn max_candidate_total_file_size_regression_pct(&self) -> Option<f64> {
        self.max_candidate_total_file_size_regression_pct
    }
    pub fn max_candidate_column_compressed_size_regression_pct(&self) -> Option<f64> {
        self.max_candidate_column_compressed_size_regression_pct
    }
    pub fn max_candidate_column_uncompressed_size_regression_pct(&self) -> Option<f64> {
        self.max_candidate_column_uncompressed_size_regression_pct
    }
}

impl CaseFile {
    /// Validates every executable field and applies derived plan defaults.
    pub fn normalize(&mut self) -> Result<()> {
        match &mut self.scenario {
            Scenario::DirectReadableSst(scenario) => {
                validate_direct_sst(scenario)?;
                validate_queries(&scenario.queries)?;
                for query in &scenario.queries {
                    if !scenario
                        .tables
                        .iter()
                        .any(|table| table.database == query.database)
                    {
                        return Err(Error::new(format!(
                            "query {} database {} is not declared by a table",
                            query.name, query.database
                        )));
                    }
                }
            }
            Scenario::PromRemoteWriteThenQuery(scenario) => {
                validate_queries(&scenario.queries)?;
                validate_remote_write(&mut scenario.remote_write)?;
                if scenario
                    .queries
                    .iter()
                    .any(|query| query.database != scenario.remote_write.database)
                {
                    return Err(Error::new(
                        "remote-write query database must match remote_write.database",
                    ));
                }
            }
        }
        Ok(())
    }
}

fn validate_queries(queries: &[QueryConfig]) -> Result<()> {
    if queries.is_empty() {
        return Err(Error::new("scenario.queries must be non-empty"));
    }
    let mut names = HashSet::new();
    for query in queries {
        nonempty("scenario.queries[].name", &query.name)?;
        nonempty("scenario.queries[].text", &query.text)?;
        validate_identifier("scenario.queries[].database", &query.database)?;
        if !names.insert(query.name.as_str()) {
            return Err(Error::new(format!("duplicate query name: {}", query.name)));
        }
        if let Some(limit) = query.thresholds.max_candidate_latency_regression_pct {
            finite_nonnegative("max_candidate_latency_regression_pct", limit)?;
        }
    }
    Ok(())
}

fn validate_direct_sst(scenario: &DirectReadableSstScenario) -> Result<()> {
    if scenario.tables.is_empty() {
        return Err(Error::new("scenario.tables must be non-empty"));
    }
    let layout = &scenario.layout;
    for (name, value) in [
        ("regions", layout.regions),
        ("sst_count", layout.sst_count),
        ("rows_per_sst", layout.rows_per_sst),
        ("row_group_size", layout.row_group_size),
        ("series_count", layout.series_count),
    ] {
        positive(name, value as u64)?;
    }
    if layout.regions != 1 {
        return Err(Error::new(
            "direct_readable_sst requires layout.regions = 1",
        ));
    }
    if layout.row_group_size > layout.rows_per_sst {
        return Err(Error::new(
            "layout.row_group_size must not exceed rows_per_sst",
        ));
    }
    if layout.step_nanos <= 0 {
        return Err(Error::new("layout.step_nanos must be positive"));
    }
    match layout.time_range_layout.as_str() {
        "non_overlapping_per_sst" => {}
        other => {
            return Err(Error::new(format!(
                "unsupported time_range_layout: {other}"
            )));
        }
    }
    match layout.series_layout.as_str() {
        "round_robin" | "timestamp_major" | "per_sst" => {}
        other => return Err(Error::new(format!("unsupported series_layout: {other}"))),
    }
    if layout.series_layout == "timestamp_major" && layout.rows_per_sst % layout.series_count != 0 {
        return Err(Error::new(
            "timestamp_major requires rows_per_sst divisible by series_count",
        ));
    }
    let rows = layout
        .sst_count
        .checked_mul(layout.rows_per_sst)
        .ok_or_else(|| Error::new("layout.sst_count * rows_per_sst overflows"))?;
    let steps = if layout.series_layout == "timestamp_major" {
        rows / layout.series_count
    } else {
        rows
    };
    let nanos = i64::try_from(
        steps
            .checked_sub(1)
            .ok_or_else(|| Error::new("layout has no generated rows"))?,
    )
    .map_err(|_| Error::new("layout timestamp step exceeds i64"))?
    .checked_mul(layout.step_nanos)
    .ok_or_else(|| Error::new("layout timestamp multiplication overflows"))?;
    layout
        .start_unix_nanos
        .checked_add(nanos)
        .ok_or_else(|| Error::new("layout timestamp range overflows"))?;
    let mut names = HashSet::new();
    let mut pairs = HashSet::new();
    for table in &scenario.tables {
        validate_identifier("table.database", &table.database)?;
        validate_identifier("table.name", &table.name)?;
        nonempty("table.engine", &table.engine)?;
        if !names.insert(table.name.as_str()) {
            return Err(Error::new(format!("duplicate table name: {}", table.name)));
        }
        if !pairs.insert((&table.database, &table.name)) {
            return Err(Error::new(format!(
                "duplicate table: {}.{}",
                table.database, table.name
            )));
        }
        validate_table(table)?;
    }
    Ok(())
}

fn validate_table(table: &TableConfig) -> Result<()> {
    if table.columns.is_empty() {
        return Err(Error::new(format!("table {} has no columns", table.name)));
    }
    if table.engine != "mito" {
        return Err(Error::new(format!(
            "unsupported table engine: {}",
            table.engine
        )));
    }
    if let Some(format) = &table.sst_format
        && !matches!(format.as_str(), "flat" | "primary_key" | "primary-key")
    {
        return Err(Error::new(format!("unsupported sst_format: {format}")));
    }
    let mut columns = HashSet::new();
    let mut timestamp_count = 0;
    for column in &table.columns {
        nonempty("column.name", &column.name)?;
        nonempty("column.type", &column.ty)?;
        nonempty("column.semantic", &column.semantic)?;
        if !columns.insert(column.name.as_str()) {
            return Err(Error::new(format!(
                "duplicate column {} in {}",
                column.name, table.name
            )));
        }
        if column.semantic == "timestamp" {
            timestamp_count += 1;
        }
        validate_column_generator(column)?;
        match &column.distribution {
            Some(Distribution::Cardinality { values, prefix }) => {
                positive("column.distribution.values", *values)?;
                nonempty("column.distribution.prefix", prefix)?;
            }
            Some(Distribution::DeterministicWave { min, max }) => {
                finite_nonnegative("column.distribution.min", *min)?;
                finite_nonnegative("column.distribution.max", *max)?;
                if min > max {
                    return Err(Error::new("deterministic_wave min must not exceed max"));
                }
            }
            None => {}
        }
    }
    if timestamp_count != 1
        || !table
            .columns
            .iter()
            .any(|column| column.name == table.time_index && column.semantic == "timestamp")
    {
        return Err(Error::new(format!(
            "time_index {} must name a timestamp column",
            table.time_index
        )));
    }
    if table.primary_key.is_empty() {
        return Err(Error::new(format!(
            "table {} primary_key must be non-empty",
            table.name
        )));
    }
    let mut keys = HashSet::new();
    for key in &table.primary_key {
        if !columns.contains(key.as_str()) {
            return Err(Error::new(format!("primary key column {key} is missing")));
        }
        if !keys.insert(key.as_str()) {
            return Err(Error::new(format!("duplicate primary key column {key}")));
        }
        let column = table
            .columns
            .iter()
            .find(|column| column.name == *key)
            .ok_or_else(|| Error::new(format!("primary key column {key} is missing")))?;
        if column.semantic != "tag" || column.ty != "STRING" {
            return Err(Error::new(format!(
                "primary key column {key} must be a STRING tag"
            )));
        }
    }
    Ok(())
}

fn validate_remote_write(remote: &mut PromRemoteWritePlan) -> Result<()> {
    for (name, value) in [
        ("remote_write.database", &remote.database),
        ("remote_write.metric", &remote.metric),
        ("remote_write.physical_table", &remote.physical_table),
    ] {
        validate_identifier(name, value)?;
    }
    if remote.metric.eq_ignore_ascii_case(&remote.physical_table) {
        return Err(Error::new(
            "remote_write.metric (logical query metric) must differ from physical_table (storage table)",
        ));
    }
    for (name, value) in [
        ("series_count", remote.series_count),
        ("samples_per_series", remote.samples_per_series),
        ("chunk_series_count", remote.chunk_series_count),
        ("timeout_seconds", remote.timeout_seconds),
        (
            "flush_every_sample_chunks",
            remote.flush_every_sample_chunks,
        ),
        (
            "visibility_timeout_seconds",
            remote.visibility_timeout_seconds,
        ),
        ("value.cardinality", remote.value.cardinality),
        ("value.run_length", remote.value.run_length),
        ("value.stall_every", remote.value.stall_every),
        ("value.stall_length", remote.value.stall_length),
        ("value.mixed_every", remote.value.mixed_every),
    ] {
        positive(name, value)?;
    }
    if let Some(chunk) = remote.sample_chunk_size {
        positive("sample_chunk_size", chunk)?;
        if chunk > remote.samples_per_series {
            return Err(Error::new(
                "sample_chunk_size must not exceed samples_per_series",
            ));
        }
    }
    if remote.step_millis <= 0 {
        return Err(Error::new("remote_write.step_millis must be positive"));
    }
    validate_remote_write_totals(
        remote.series_count,
        remote.samples_per_series,
        0,
        remote.samples_per_series,
    )?;
    let span = i64::try_from(remote.samples_per_series - 1)
        .map_err(|_| Error::new("samples_per_series exceeds i64"))?
        .checked_mul(remote.step_millis)
        .ok_or_else(|| Error::new("remote_write timestamp multiplication overflows"))?;
    remote
        .start_unix_millis
        .checked_add(span)
        .ok_or_else(|| Error::new("remote_write timestamp range overflows"))?;
    let start_nanos = remote
        .start_unix_millis
        .checked_mul(1_000_000)
        .ok_or_else(|| Error::new("remote_write start timestamp overflows nanoseconds"))?;
    span.checked_mul(1_000_000)
        .and_then(|offset| start_nanos.checked_add(offset))
        .ok_or_else(|| Error::new("remote_write end timestamp overflows nanoseconds"))?;
    for (name, value) in [
        ("value.base", remote.value.base),
        ("value.step", remote.value.step),
    ] {
        if !value.is_finite() {
            return Err(Error::new(format!("{name} must be finite")));
        }
    }
    for (name, value) in [
        ("max_batch_rows", remote.prom_store.max_batch_rows),
        (
            "max_concurrent_flushes",
            remote.prom_store.max_concurrent_flushes,
        ),
        (
            "worker_channel_capacity",
            remote.prom_store.worker_channel_capacity,
        ),
        (
            "max_inflight_requests",
            remote.prom_store.max_inflight_requests,
        ),
    ] {
        positive(name, value)?;
    }
    nonempty(
        "pending_rows_flush_interval",
        &remote.prom_store.pending_rows_flush_interval,
    )?;
    if remote
        .storage
        .as_ref()
        .is_some_and(|storage| storage.inspect)
        && remote.read_bench.is_none()
    {
        remote.read_bench = Some(ReadBenchConfig::default());
    }
    match (&mut remote.storage, &mut remote.read_bench) {
        (Some(storage), bench) => {
            if !storage.inspect && bench.as_ref().is_some_and(|value| value.enabled) {
                return Err(Error::new("read_bench requires storage.inspect = true"));
            }
            validate_storage(storage)?;
            if storage.inspect {
                if let Some(bench) = bench {
                    validate_read_bench(bench, &storage.column)?;
                }
            }
        }
        (None, Some(bench)) if bench.enabled => {
            return Err(Error::new("read_bench requires storage"));
        }
        _ => {}
    }
    Ok(())
}

/// Validates every remote-write row and ordinal total before data generation.
/// Both the case normalizer and the direct CLI adapter use this helper so that
/// overflow is rejected at the public boundary, not only by the generator.
pub fn validate_remote_write_totals(
    series_count: u64,
    samples_per_series: u64,
    sample_offset: u64,
    total_samples_per_series: u64,
) -> Result<()> {
    positive("series_count", series_count)?;
    positive("samples_per_series", samples_per_series)?;
    positive("total_samples_per_series", total_samples_per_series)?;
    let end = sample_offset
        .checked_add(samples_per_series)
        .ok_or_else(|| Error::new("remote-write sample offset overflows"))?;
    if end > total_samples_per_series {
        return Err(Error::new(
            "value offset and samples must fit within total_samples_per_series",
        ));
    }
    series_count
        .checked_mul(samples_per_series)
        .ok_or_else(|| Error::new("series_count * samples_per_series overflows"))?;
    series_count
        .checked_sub(1)
        .and_then(|last_series| last_series.checked_mul(total_samples_per_series))
        .and_then(|first_ordinal| first_ordinal.checked_add(end))
        .ok_or_else(|| Error::new("remote-write ordinal overflows"))?;
    Ok(())
}

/// Allows only semantic/type/distribution triples implemented by the fixture
/// generator. Keeping this list exhaustive prevents a case from declaring a
/// distribution which the generator silently ignores.
fn validate_column_generator(column: &ColumnConfig) -> Result<()> {
    let supported = matches!(
        (
            column.semantic.as_str(),
            column.ty.as_str(),
            &column.distribution
        ),
        ("tag", "STRING", Some(Distribution::Cardinality { .. }))
            | (
                "field",
                "DOUBLE",
                None | Some(Distribution::DeterministicWave { .. })
            )
            | ("field", "UINT64", None)
            | ("timestamp", "TIMESTAMP(9)" | "TIMESTAMP(3)", None)
    );
    if supported {
        Ok(())
    } else {
        Err(Error::new(format!(
            "unsupported column generator combination: semantic={} type={}",
            column.semantic, column.ty
        )))
    }
}

fn validate_storage(storage: &mut StorageConfig) -> Result<()> {
    if !storage.inspect {
        return Err(Error::new(
            "storage.inspect = false is not a storage contract",
        ));
    }
    nonempty("storage.column", &storage.column)?;
    positive("storage.min_files", storage.min_files)?;
    positive(
        "storage.min_files_with_column",
        storage.min_files_with_column,
    )?;
    if let Some(path) = &storage.root_suffix {
        validate_relative_path("storage.root_suffix", path)?;
    }
    let required: HashSet<_> = storage
        .require_encodings
        .iter()
        .map(String::as_str)
        .collect();
    if required.len() != storage.require_encodings.len()
        || storage
            .forbid_encodings
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != storage.forbid_encodings.len()
    {
        return Err(Error::new("storage encodings must be unique"));
    }
    for encoding in storage
        .require_encodings
        .iter()
        .chain(&storage.forbid_encodings)
    {
        nonempty("storage encoding", encoding)?;
    }
    if storage
        .forbid_encodings
        .iter()
        .any(|encoding| required.contains(encoding.as_str()))
    {
        return Err(Error::new(
            "storage encoding cannot be both required and forbidden",
        ));
    }
    for value in [
        storage.max_candidate_total_file_size_regression_pct,
        storage.max_candidate_column_compressed_size_regression_pct,
        storage.max_candidate_column_uncompressed_size_regression_pct,
    ]
    .into_iter()
    .flatten()
    {
        finite_nonnegative("storage regression threshold", value)?;
    }
    storage.populate_planned_thresholds();
    Ok(())
}

fn validate_read_bench(bench: &mut ReadBenchConfig, column: &str) -> Result<()> {
    if !bench.enabled {
        return Ok(());
    }
    if !bench.parquetbench && !bench.scanbench {
        return Err(Error::new(
            "read_bench must enable parquetbench or scanbench",
        ));
    }
    positive("read_bench.iterations", bench.iterations)?;
    positive("read_bench.parallelism", bench.parallelism)?;
    if let Some(max_files) = bench.max_files {
        if max_files == 0 {
            return Err(Error::new("read_bench.max_files must be positive"));
        }
    }
    nonempty("read_bench.parquet_reader", &bench.parquet_reader)?;
    nonempty("read_bench.scan_scanner", &bench.scan_scanner)?;
    if bench.projection.is_empty() {
        bench.projection.push(column.to_string());
    }
    let mut names = HashSet::new();
    for value in &bench.projection {
        nonempty("read_bench.projection", value)?;
        if !names.insert(value.as_str()) {
            return Err(Error::new(format!(
                "duplicate read_bench projection {value}"
            )));
        }
    }
    Ok(())
}

/// Validates a user supplied path is a relative, non-traversing path.
pub fn validate_relative_path(field: &str, value: &str) -> Result<()> {
    nonempty(field, value)?;
    let path = Path::new(value);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(Error::new(format!(
            "{field} must be a relative non-traversing path"
        )));
    }
    Ok(())
}

fn nonempty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(Error::new(format!("{field} must be non-empty")))
    } else {
        Ok(())
    }
}
fn validate_identifier(field: &str, value: &str) -> Result<()> {
    nonempty(field, value)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(Error::new(format!("{field} must be an identifier")));
    }
    Ok(())
}
fn positive(field: &str, value: u64) -> Result<()> {
    if value == 0 {
        Err(Error::new(format!("{field} must be positive")))
    } else {
        Ok(())
    }
}
fn finite_nonnegative(field: &str, value: f64) -> Result<()> {
    if !value.is_finite() || value < 0.0 {
        Err(Error::new(format!(
            "{field} must be finite and non-negative"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIRECT: &str = r#"
[case]
name = "unit"
[scenario]
kind = "direct_readable_sst"
seed = 1
[[scenario.tables]]
database = "public"
name = "unit"
engine = "mito"
primary_key = ["host"]
time_index = "ts"
[[scenario.tables.columns]]
name = "host"
type = "STRING"
semantic = "tag"
distribution = { kind = "cardinality", values = 1, prefix = "host" }
[[scenario.tables.columns]]
name = "value"
type = "DOUBLE"
semantic = "field"
distribution = { kind = "deterministic_wave", min = 0.0, max = 1.0 }
[[scenario.tables.columns]]
name = "ts"
type = "TIMESTAMP(9)"
semantic = "timestamp"
[scenario.layout]
regions = 1
sst_count = 1
rows_per_sst = 1
row_group_size = 1
series_count = 1
start_unix_nanos = 0
step_nanos = 1
time_range_layout = "non_overlapping_per_sst"
series_layout = "round_robin"
[[scenario.queries]]
name = "q"
kind = "sql"
database = "public"
query = "select 1"
iterations = 1
"#;

    const REMOTE: &str = r#"
[scenario]
kind = "prom_remote_write_then_query"
[scenario.remote_write]
metric = "m"
series_count = 2
samples_per_series = 3
sample_chunk_size = 2
chunk_series_count = 2
timeout_seconds = 1
visibility_timeout_seconds = 2
step_millis = 1
[scenario.remote_write.value]
base = 1.0
step = 0.5
[scenario.remote_write.storage]
column = "greptime_value"
min_files = 1
min_files_with_column = 1
max_total_file_size_bytes = 10
[scenario.remote_write.read_bench]
parquetbench = true
scanbench = true
iterations = 1
parallelism = 1
[[scenario.queries]]
name = "q"
kind = "sql"
database = "public"
query = "select 1"
iterations = 1
[scenario.queries.thresholds]
max_candidate_latency_regression_pct = 10
"#;

    fn normalized(text: &str) -> Result<CaseFile> {
        let mut case: CaseFile = toml::from_str(text).map_err(|err| Error::new(err.to_string()))?;
        case.normalize()?;
        Ok(case)
    }

    fn validated(text: &str) -> Result<ValidatedCase> {
        ValidatedCase::from_normalized(normalized(text)?)
    }

    #[test]
    fn executable_digest_mutations_change_and_stale_digest_fails() {
        let baseline = validated(REMOTE).unwrap_or_else(|err| panic!("case: {err}"));
        let digest = baseline
            .executable_digest()
            .unwrap_or_else(|err| panic!("digest: {err}"));
        let mutations = [
            (
                "direct layout",
                DIRECT,
                "rows_per_sst = 1",
                "rows_per_sst = 2",
            ),
            (
                "series count",
                REMOTE,
                "series_count = 2",
                "series_count = 3",
            ),
            (
                "sample count",
                REMOTE,
                "samples_per_series = 3",
                "samples_per_series = 4",
            ),
            (
                "sample chunk",
                REMOTE,
                "sample_chunk_size = 2",
                "sample_chunk_size = 1",
            ),
            (
                "chunk series",
                REMOTE,
                "chunk_series_count = 2",
                "chunk_series_count = 1",
            ),
            (
                "visibility timeout",
                REMOTE,
                "visibility_timeout_seconds = 2",
                "visibility_timeout_seconds = 3",
            ),
            ("value", REMOTE, "base = 1.0", "base = 2.0"),
            (
                "query threshold",
                REMOTE,
                "max_candidate_latency_regression_pct = 10",
                "max_candidate_latency_regression_pct = 11",
            ),
            (
                "storage threshold",
                REMOTE,
                "max_total_file_size_bytes = 10",
                "max_total_file_size_bytes = 11",
            ),
            (
                "read-bench mode",
                REMOTE,
                "scanbench = true",
                "scanbench = false",
            ),
        ];
        for (name, source, old, new) in mutations {
            let changed = validated(&source.replacen(old, new, 1))
                .unwrap_or_else(|err| panic!("{name}: {err}"));
            assert_ne!(
                changed
                    .executable_digest()
                    .unwrap_or_else(|err| panic!("{name}: {err}")),
                if source == REMOTE {
                    digest.clone()
                } else {
                    validated(source)
                        .unwrap_or_else(|err| panic!("{name}: {err}"))
                        .executable_digest()
                        .unwrap_or_else(|err| panic!("{name}: {err}"))
                },
                "{name} must invalidate executable identity"
            );
        }
        assert!(digest.verify(b"different executable contract").is_err());
    }

    #[test]
    fn fixture_digest_tracks_fixture_inputs_but_not_query_reporting() {
        let baseline = validated(REMOTE).unwrap_or_else(|err| panic!("case: {err}"));
        let digest = baseline
            .fixture_plan_digest()
            .unwrap_or_else(|err| panic!("digest: {err}"));
        for (name, old, new) in [
            ("series count", "series_count = 2", "series_count = 3"),
            (
                "sample count",
                "samples_per_series = 3",
                "samples_per_series = 4",
            ),
            (
                "sample chunk",
                "sample_chunk_size = 2",
                "sample_chunk_size = 1",
            ),
            (
                "chunk series",
                "chunk_series_count = 2",
                "chunk_series_count = 1",
            ),
            (
                "visibility timeout",
                "visibility_timeout_seconds = 2",
                "visibility_timeout_seconds = 3",
            ),
            ("value", "base = 1.0", "base = 2.0"),
            ("read-bench mode", "scanbench = true", "scanbench = false"),
        ] {
            let changed = validated(&REMOTE.replacen(old, new, 1))
                .unwrap_or_else(|err| panic!("{name}: {err}"));
            assert_ne!(
                changed
                    .fixture_plan_digest()
                    .unwrap_or_else(|err| panic!("{name}: {err}")),
                digest,
                "{name} must invalidate fixture identity"
            );
        }
        let direct = validated(DIRECT).unwrap_or_else(|err| panic!("direct: {err}"));
        let changed_direct = validated(&DIRECT.replacen("rows_per_sst = 1", "rows_per_sst = 2", 1))
            .unwrap_or_else(|err| panic!("direct layout: {err}"));
        assert_ne!(
            direct
                .fixture_plan_digest()
                .unwrap_or_else(|err| panic!("direct digest: {err}")),
            changed_direct
                .fixture_plan_digest()
                .unwrap_or_else(|err| panic!("changed direct digest: {err}")),
            "direct layout must invalidate fixture identity"
        );
        for (name, old, new) in [
            ("query text", "query = \"select 1\"", "query = \"select 2\""),
            (
                "query threshold",
                "max_candidate_latency_regression_pct = 10",
                "max_candidate_latency_regression_pct = 11",
            ),
        ] {
            let changed = validated(&REMOTE.replacen(old, new, 1))
                .unwrap_or_else(|err| panic!("{name}: {err}"));
            assert_eq!(
                changed
                    .fixture_plan_digest()
                    .unwrap_or_else(|err| panic!("{name}: {err}")),
                digest,
                "{name} must remain report-only"
            );
        }
    }

    #[test]
    fn rejects_missing_empty_and_duplicate_queries() {
        assert!(normalized(&DIRECT.replace("[[scenario.queries]]\nname = \"q\"\nkind = \"sql\"\ndatabase = \"public\"\nquery = \"select 1\"\niterations = 1\n", "")).is_err());
        assert!(normalized(&DIRECT.replace("query = \"select 1\"", "query = \" \" ")).is_err());
        assert!(normalized(&DIRECT.replace("iterations = 1", "iterations = 0")).is_err());
        assert!(normalized(&DIRECT.replace("iterations = 1", "iterations = 1\n[[scenario.queries]]\nname = \"q\"\nkind = \"sql\"\ndatabase = \"public\"\nquery = \"select 2\"\niterations = 1")).is_err());
    }

    #[test]
    fn rejects_unknown_executable_fields_and_invalid_layout() {
        assert!(toml::from_str::<LayoutConfig>("regions=1\nsst_count=1\nrows_per_sst=1\nrow_group_size=1\nseries_count=1\nstart_unix_nanos=0\nstep_nanos=1\ntime_range_layout='non_overlapping_per_sst'\nseries_layout='round_robin'\nunexpected=1").is_err());
        assert!(toml::from_str::<QueryThresholds>("unsupported=1").is_err());
        assert!(normalized(&DIRECT.replace("rows_per_sst = 1", "rows_per_sst = 0")).is_err());
        assert!(
            normalized(
                &DIRECT
                    .replace(
                        "start_unix_nanos = 0",
                        "start_unix_nanos = 9223372036854775807"
                    )
                    .replace("sst_count = 1", "sst_count = 2")
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_invalid_distribution_primary_key_and_query_database() {
        assert!(
            normalized(&DIRECT.replace(
                "distribution = { kind = \"cardinality\", values = 1, prefix = \"host\" }",
                "distribution = { kind = \"cardinality\", values = 0, prefix = \"host\" }",
            ))
            .is_err()
        );
        assert!(
            normalized(&DIRECT.replace(
                "distribution = { kind = \"deterministic_wave\", min = 0.0, max = 1.0 }",
                "distribution = { kind = \"cardinality\", values = 1, prefix = \"value\" }",
            ))
            .is_err()
        );
        assert!(normalized(&DIRECT.replace(
            "type = \"DOUBLE\"\nsemantic = \"field\"\ndistribution = { kind = \"deterministic_wave\", min = 0.0, max = 1.0 }",
            "type = \"UINT64\"\nsemantic = \"field\"\ndistribution = { kind = \"deterministic_wave\", min = 0.0, max = 1.0 }",
        ))
        .is_err());
        assert!(normalized(&DIRECT.replace(
            "name = \"ts\"\ntype = \"TIMESTAMP(9)\"\nsemantic = \"timestamp\"",
            "name = \"ts\"\ntype = \"TIMESTAMP(9)\"\nsemantic = \"timestamp\"\ndistribution = { kind = \"deterministic_wave\", min = 0.0, max = 1.0 }",
        ))
        .is_err());
        assert!(
            normalized(&DIRECT.replace("primary_key = [\"host\"]", "primary_key = [\"value\"]"))
                .is_err()
        );
        assert!(
            normalized(&DIRECT.replace(
                "database = \"public\"\nquery = \"select 1\"",
                "database = \"other\"\nquery = \"select 1\""
            ))
            .is_err()
        );
    }

    #[test]
    fn rejects_table_paths_and_storage_read_bench_conflicts() {
        assert!(validate_relative_path("path", "../escape").is_err());
        assert!(validate_relative_path("path", "/absolute").is_err());
        let remote = r#"
[scenario]
kind = "prom_remote_write_then_query"
[[scenario.queries]]
name = "q"
kind = "sql"
database = "public"
query = "select 1"
[scenario.remote_write]
metric = "m"
series_count = 1
samples_per_series = 1
chunk_series_count = 1
timeout_seconds = 1
step_millis = 1
[scenario.remote_write.storage]
inspect = false
[scenario.remote_write.read_bench]
enabled = true
"#;
        assert!(normalized(remote).is_err());
        assert!(
            normalized(&REMOTE.replace(
                "max_total_file_size_bytes = 10",
                "require_encodings = [\"PLAIN\", \"PLAIN\"]",
            ))
            .is_err()
        );
    }

    #[test]
    fn rejects_direct_total_rows_and_remote_totals_overflow() {
        assert!(
            normalized(
                &DIRECT
                    .replace("sst_count = 1", "sst_count = 18446744073709551615")
                    .replace("rows_per_sst = 1", "rows_per_sst = 2"),
            )
            .is_err()
        );
        assert!(validate_remote_write_totals(u64::MAX, 2, 0, 2).is_err());
    }

    #[test]
    fn rejects_remote_metric_equal_to_storage_physical_table() {
        for metric in ["greptime_physical_table", "GREPTIME_PHYSICAL_TABLE"] {
            assert!(
                normalized(&REMOTE.replace("metric = \"m\"", &format!("metric = \"{metric}\""),))
                    .is_err()
            );
        }
    }

    #[test]
    fn remote_write_fixture_expectation_is_checked_and_inclusive() {
        let case = validated(REMOTE).unwrap_or_else(|err| panic!("case: {err}"));
        let expected = case
            .remote_write_fixture_expectation()
            .unwrap_or_else(|err| panic!("expectation: {err}"));
        assert_eq!(expected.database(), "public");
        assert_eq!(expected.logical_metric_table(), "m");
        assert_eq!(expected.physical_table(), "greptime_physical_table");
        assert_eq!(expected.logical_summary().rows(), 6);
        assert_eq!(expected.logical_summary().series(), 2);
        assert_eq!(
            expected.logical_summary().time_start_unix_nanos(),
            1_704_067_200_000_000_000
        );
        assert_eq!(
            expected.logical_summary().time_end_unix_nanos(),
            1_704_067_200_002_000_000,
            "the end is the final included sample, not an exclusive bound"
        );
    }

    #[test]
    fn every_builtin_case_normalizes() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/perf/query_cases");
        for entry in fs::read_dir(root).unwrap_or_else(|err| panic!("read test cases: {err}")) {
            let path = entry
                .unwrap_or_else(|err| panic!("read test case entry: {err}"))
                .path()
                .join("case.toml");
            assert!(load_case(&path).is_ok(), "{}", path.display());
        }
    }
}
