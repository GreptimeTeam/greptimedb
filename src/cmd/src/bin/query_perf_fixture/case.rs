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
pub(super) struct ReadBenchConfig {
    #[serde(default = "default_true")]
    pub(super) enabled: bool,
    #[serde(default = "default_true")]
    pub(super) parquetbench: bool,
    #[serde(default = "default_true")]
    pub(super) scanbench: bool,
    #[serde(default = "default_iterations")]
    pub(super) iterations: u64,
    #[serde(default)]
    pub(super) projection: Vec<String>,
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
            thresholds: HashMap::new(),
        }
    }
}

pub(super) fn default_iterations() -> u64 {
    7
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
