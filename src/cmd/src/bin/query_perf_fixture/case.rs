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
use std::num::{NonZeroU64, NonZeroUsize};

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
    #[serde(rename = "otlp_trace_load")]
    OtlpTraceLoad(OtlpTraceLoadScenario),
    #[serde(rename = "write_throughput")]
    WriteThroughput(WriteThroughputScenario),
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct DirectReadableSstScenario {
    #[serde(default)]
    pub(super) seed: Option<u64>,
    #[serde(default)]
    pub(super) queries: Vec<serde_json::Value>,
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
pub(super) struct OtlpTraceLoadScenario {
    pub(super) load: OtlpTraceLoadPlan,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct OtlpTraceLoadPlan {
    pub(super) database: String,
    pub(super) table: String,
    pub(super) pipeline: String,
    pub(super) duration_seconds: NonZeroU64,
    pub(super) warmup_seconds: u64,
    pub(super) rate: NonZeroU64,
    pub(super) workers: NonZeroUsize,
    pub(super) exporter_shards: NonZeroUsize,
    pub(super) workload: String,
    pub(super) visibility_timeout_seconds: NonZeroU64,
    pub(super) thresholds: OtlpTraceLoadThresholds,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct OtlpTraceLoadThresholds {
    pub(super) max_candidate_throughput_regression_pct: f64,
    pub(super) max_candidate_mean_latency_regression_pct: f64,
    pub(super) max_failure_count: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WriteThroughputScenario {
    pub(super) remote_write: PromRemoteWritePlan,
    pub(super) write_measure: WriteMeasureConfig,
    /// Workload-scheduler configuration injected into the datanode via
    /// environment variables. When absent the runner leaves both targets on
    /// the datanode default (scheduler disabled). `enable` is not used by the
    /// runner for derivation: the base target always runs with the scheduler
    /// disabled and the candidate target always runs with it enabled when this
    /// section is present.
    #[serde(default)]
    pub(super) scheduler: Option<WorkloadSchedulerCaseConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkloadSchedulerCaseConfig {
    /// Enables policy-controlled query and write task spawning. Mirrors the
    /// datanode `runtime.experimental_workload_scheduler.enable` option; the
    /// runner derives the per-target value (base = false, candidate = true)
    /// and ignores this field for derivation.
    #[serde(default)]
    pub(super) enable: bool,
    /// Maximum polls admitted to Tokio at once. Zero uses four times
    /// `global_rt_size`, consistent with the runtime default.
    #[serde(default)]
    pub(super) max_concurrent_polls: u64,
    /// Relative share for query polls while writes are also backlogged.
    #[serde(default = "default_scheduler_query_weight")]
    pub(super) query_weight: u64,
    /// Relative share for write polls while queries are also backlogged.
    #[serde(default = "default_scheduler_write_weight")]
    pub(super) write_weight: u64,
}

pub(super) fn default_scheduler_query_weight() -> u64 {
    2
}
pub(super) fn default_scheduler_write_weight() -> u64 {
    8
}

impl WorkloadSchedulerCaseConfig {
    pub(super) fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("query_weight", self.query_weight),
            ("write_weight", self.write_weight),
        ] {
            if value == 0 {
                return Err(format!("scenario.scheduler.{name} must be positive"));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WriteMeasureConfig {
    /// Nominal measurement window in wall-clock seconds. `window_seconds` must
    /// divide it; the runner buckets per-window RPS over the first
    /// `duration_seconds` of the ingestion timeline.
    pub(super) duration_seconds: NonZeroU64,
    /// Per-window RPS bucket size in seconds.
    pub(super) window_seconds: NonZeroU64,
    /// Optional target ingest rate in rows/second; 0 = max throughput
    /// (the generator writes as fast as it can and the achieved rate is
    /// measured). Pacing is not implemented; this value is validated,
    /// normalized, and reported for future use.
    #[serde(default)]
    pub(super) target_rps: f64,
    pub(super) thresholds: WriteThroughputThresholds,
    /// Optional concurrent read+write ("mix") measurement. When present the
    /// runner runs the remote-write ingestion in a background thread while a
    /// query loop hammers the same frontend for `duration_seconds`, so query
    /// and write tasks genuinely contend on the datanode runtime under a dual
    /// backlog.
    #[serde(default)]
    pub(super) mix: Option<MixMeasureConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MixMeasureConfig {
    /// SQL for the concurrent query loop. Defaults to a `count(*)` over the
    /// remote-write physical table (`greptime_physical_table`), which scans
    /// every ingested row and is therefore a real datanode-runtime contender.
    #[serde(default)]
    pub(super) query_sql: Option<String>,
    /// Wall-clock interval between queries per loop thread, in milliseconds.
    #[serde(default = "default_mix_query_interval_ms")]
    pub(super) query_interval_ms: NonZeroU64,
    /// Number of concurrent query-loop threads.
    #[serde(default = "default_mix_query_parallelism")]
    pub(super) query_parallelism: NonZeroU64,
    pub(super) thresholds: MixMeasureThresholds,
}

pub(super) fn default_mix_query_interval_ms() -> NonZeroU64 {
    NonZeroU64::new(100).expect("100 is non-zero")
}
pub(super) fn default_mix_query_parallelism() -> NonZeroU64 {
    NonZeroU64::new(1).expect("1 is non-zero")
}

impl MixMeasureConfig {
    pub(super) fn validate(&self) -> Result<(), String> {
        if let Some(sql) = &self.query_sql
            && sql.trim().is_empty()
        {
            return Err(
                "scenario.write_measure.mix.query_sql must be a non-empty SQL string".to_string(),
            );
        }
        self.thresholds.validate()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MixMeasureThresholds {
    /// Max fraction of failed query attempts per target, in [0, 1] (0.05 = 5%).
    pub(super) max_query_failure_rate: f64,
    /// Max candidate-vs-base query p99 latency regression percent:
    /// `(candidate_p99_ms - base_p99_ms) / base_p99_ms * 100`.
    pub(super) max_query_p99_regression_pct: f64,
}

impl MixMeasureThresholds {
    fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("max_query_failure_rate", self.max_query_failure_rate),
            (
                "max_query_p99_regression_pct",
                self.max_query_p99_regression_pct,
            ),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(format!(
                    "scenario.write_measure.mix.thresholds.{name} must be a finite non-negative number"
                ));
            }
        }
        if self.max_query_failure_rate > 1.0 {
            return Err(
                "scenario.write_measure.mix.thresholds.max_query_failure_rate must be <= 1.0 (a rate in [0, 1])"
                    .to_string(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WriteThroughputThresholds {
    /// Max fraction of failed write chunks per target, in [0, 1] (0.05 = 5%).
    pub(super) max_failure_rate: f64,
    /// Max candidate-vs-base mean RPS regression percent:
    /// `(base_mean_rps - candidate_mean_rps) / base_mean_rps * 100`.
    pub(super) max_mean_rps_regression_pct: f64,
    /// Max candidate-vs-base write-request p99 latency regression percent:
    /// `(candidate_p99_ms - base_p99_ms) / base_p99_ms * 100`.
    pub(super) max_p99_latency_regression_pct: f64,
    /// Optional absolute floor on each target's mean RPS.
    #[serde(default)]
    pub(super) min_rps_absolute: Option<f64>,
}

impl WriteMeasureConfig {
    pub(super) fn validate(&self) -> Result<(), String> {
        if self.duration_seconds.get() % self.window_seconds.get() != 0 {
            return Err(
                "scenario.write_measure.duration_seconds must be a positive multiple of window_seconds"
                    .to_string(),
            );
        }
        if !self.target_rps.is_finite() || self.target_rps < 0.0 {
            return Err(
                "scenario.write_measure.target_rps must be a finite non-negative number"
                    .to_string(),
            );
        }
        if let Some(mix) = &self.mix {
            mix.validate()?;
        }
        self.thresholds.validate()
    }
}

impl WriteThroughputThresholds {
    fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("max_failure_rate", self.max_failure_rate),
            (
                "max_mean_rps_regression_pct",
                self.max_mean_rps_regression_pct,
            ),
            (
                "max_p99_latency_regression_pct",
                self.max_p99_latency_regression_pct,
            ),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(format!(
                    "scenario.write_measure.thresholds.{name} must be a finite non-negative number"
                ));
            }
        }
        if self.max_failure_rate > 1.0 {
            return Err(
                "scenario.write_measure.thresholds.max_failure_rate must be <= 1.0 (a rate in [0, 1])"
                    .to_string(),
            );
        }
        if let Some(min_rps) = self.min_rps_absolute
            && (!min_rps.is_finite() || min_rps < 0.0)
        {
            return Err(
                "scenario.write_measure.thresholds.min_rps_absolute must be a finite non-negative number"
                    .to_string(),
            );
        }
        Ok(())
    }
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
            Scenario::OtlpTraceLoad(_) => "otlp_trace_load",
            Scenario::WriteThroughput(_) => "write_throughput",
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;

    /// Repository root, derived from the cmd crate manifest (`src/cmd`).
    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("cmd crate lives at src/cmd")
            .parent()
            .expect("repo root is the parent of src")
            .to_path_buf()
    }

    fn builtin_write_throughput_case() -> PathBuf {
        repo_root().join("tests/perf/query_cases/write_throughput_scheduler/case.toml")
    }

    fn builtin_write_read_mixed_case() -> PathBuf {
        repo_root().join("tests/perf/query_cases/write_read_mixed_scheduler/case.toml")
    }

    fn parse_case(text: &str) -> Result<CaseFile, toml::de::Error> {
        toml::from_str(text)
    }

    fn write_throughput_scenario(text: &str) -> WriteThroughputScenario {
        let case = parse_case(text).expect("case must parse");
        match case.scenario {
            Scenario::WriteThroughput(scenario) => scenario,
            other => panic!("expected write_throughput scenario, got {:?}", other.kind()),
        }
    }

    const MINIMAL_CASE: &str = r#"
[scenario]
kind = "write_throughput"

[scenario.remote_write]
metric = "write_throughput_test"

[scenario.write_measure]
duration_seconds = 60
window_seconds = 5

[scenario.write_measure.thresholds]
max_failure_rate = 0.05
max_mean_rps_regression_pct = 10.0
max_p99_latency_regression_pct = 10.0
"#;

    const SCHEDULER_CASE: &str = r#"
[scenario]
kind = "write_throughput"

[scenario.remote_write]
metric = "write_throughput_test"

[scenario.write_measure]
duration_seconds = 60
window_seconds = 5

[scenario.write_measure.thresholds]
max_failure_rate = 0.05
max_mean_rps_regression_pct = 10.0
max_p99_latency_regression_pct = 10.0

[scenario.scheduler]
max_concurrent_polls = 16
query_weight = 2
write_weight = 8
"#;

    const MIXED_CASE: &str = r#"
[scenario]
kind = "write_throughput"

[scenario.remote_write]
metric = "write_read_mixed_test"

[scenario.write_measure]
duration_seconds = 60
window_seconds = 5

[scenario.write_measure.thresholds]
max_failure_rate = 0.05
max_mean_rps_regression_pct = 10.0
max_p99_latency_regression_pct = 10.0

[scenario.write_measure.mix]
query_interval_ms = 100
query_parallelism = 2

[scenario.write_measure.mix.thresholds]
max_query_failure_rate = 0.05
max_query_p99_regression_pct = 10.0
"#;

    #[test]
    fn parses_builtin_write_throughput_case() {
        let text = std::fs::read_to_string(builtin_write_throughput_case())
            .expect("built-in write_throughput case.toml must exist");
        let case = parse_case(&text).expect("built-in write_throughput case must parse");
        assert_eq!(case.scenario.kind(), "write_throughput");
        let scenario = write_throughput_scenario(&text);
        assert_eq!(scenario.remote_write.series_count, 2048);
        assert_eq!(scenario.remote_write.samples_per_series, 3600);
        assert_eq!(scenario.write_measure.duration_seconds.get(), 60);
        assert_eq!(scenario.write_measure.window_seconds.get(), 5);
        assert_eq!(scenario.write_measure.target_rps, 0.0);
        assert_eq!(scenario.write_measure.thresholds.max_failure_rate, 0.05);
        assert_eq!(
            scenario
                .write_measure
                .thresholds
                .max_mean_rps_regression_pct,
            10.0
        );
        assert_eq!(
            scenario
                .write_measure
                .thresholds
                .max_p99_latency_regression_pct,
            10.0
        );
        let scheduler = scenario
            .scheduler
            .expect("built-in case must declare a scheduler section");
        assert!(!scheduler.enable);
        assert_eq!(scheduler.max_concurrent_polls, 16);
        assert_eq!(scheduler.query_weight, 2);
        assert_eq!(scheduler.write_weight, 8);
        scheduler
            .validate()
            .expect("built-in scheduler must validate");
        scenario
            .write_measure
            .validate()
            .expect("built-in case must validate");
    }

    #[test]
    fn minimal_case_uses_defaults_and_validates() {
        let scenario = write_throughput_scenario(MINIMAL_CASE);
        assert_eq!(scenario.remote_write.series_count, 8);
        assert_eq!(scenario.remote_write.samples_per_series, 30);
        assert_eq!(scenario.write_measure.target_rps, 0.0);
        assert!(scenario.scheduler.is_none());
        assert!(scenario.write_measure.thresholds.min_rps_absolute.is_none());
        scenario
            .write_measure
            .validate()
            .expect("minimal case must validate");
    }

    #[test]
    fn write_throughput_parses_scheduler_section() {
        let scenario = write_throughput_scenario(SCHEDULER_CASE);
        let scheduler = scenario
            .scheduler
            .expect("scheduler section must parse into Some");
        assert!(!scheduler.enable);
        assert_eq!(scheduler.max_concurrent_polls, 16);
        assert_eq!(scheduler.query_weight, 2);
        assert_eq!(scheduler.write_weight, 8);
        scheduler
            .validate()
            .expect("scheduler config must validate");
    }

    #[test]
    fn write_throughput_rejects_invalid_scheduler_weights() {
        let zero_query = SCHEDULER_CASE.replace("query_weight = 2", "query_weight = 0");
        let scenario = write_throughput_scenario(&zero_query);
        let err = scenario
            .scheduler
            .unwrap()
            .validate()
            .expect_err("zero query_weight must be rejected");
        assert!(err.contains("query_weight"), "{err}");

        let zero_write = SCHEDULER_CASE.replace("write_weight = 8", "write_weight = 0");
        let scenario = write_throughput_scenario(&zero_write);
        let err = scenario
            .scheduler
            .unwrap()
            .validate()
            .expect_err("zero write_weight must be rejected");
        assert!(err.contains("write_weight"), "{err}");
    }

    #[test]
    fn write_throughput_rejects_unknown_scheduler_fields() {
        let unknown = SCHEDULER_CASE.replace(
            "max_concurrent_polls = 16",
            "max_concurrent_polls = 16\nbogus_scheduler_field = 1",
        );
        let err = parse_case(&unknown).expect_err("unknown scheduler field must be rejected");
        assert!(err.to_string().contains("bogus_scheduler_field"), "{err}");
    }

    #[test]
    fn rejects_bad_types() {
        // window_seconds must be an integer, not a string.
        let bad_type = MINIMAL_CASE.replace("window_seconds = 5", "window_seconds = \"five\"");
        assert!(parse_case(&bad_type).is_err());

        // duration_seconds must be positive; NonZeroU64 rejects zero.
        let zero_duration = MINIMAL_CASE.replace("duration_seconds = 60", "duration_seconds = 0");
        assert!(parse_case(&zero_duration).is_err());

        // duration_seconds must be an integer, not a float.
        let float_duration =
            MINIMAL_CASE.replace("duration_seconds = 60", "duration_seconds = 60.5");
        assert!(parse_case(&float_duration).is_err());
    }

    #[test]
    fn rejects_unknown_fields() {
        let unknown = MINIMAL_CASE.replace(
            "duration_seconds = 60",
            "duration_seconds = 60\nbogus_field = 1",
        );
        let err = parse_case(&unknown).expect_err("unknown field must be rejected");
        assert!(err.to_string().contains("bogus_field"));
    }

    #[test]
    fn rejects_inconsistent_window_and_duration() {
        let inconsistent = MINIMAL_CASE.replace("window_seconds = 5", "window_seconds = 7");
        let scenario = write_throughput_scenario(&inconsistent);
        let err = scenario
            .write_measure
            .validate()
            .expect_err("window_seconds must divide duration_seconds");
        assert!(err.contains("window_seconds"), "{err}");
    }

    #[test]
    fn rejects_negative_or_non_finite_thresholds() {
        let negative_rps = MINIMAL_CASE.replace(
            "max_mean_rps_regression_pct = 10.0",
            "max_mean_rps_regression_pct = -1.0",
        );
        let scenario = write_throughput_scenario(&negative_rps);
        let err = scenario
            .write_measure
            .validate()
            .expect_err("negative regression limit must be rejected");
        assert!(err.contains("max_mean_rps_regression_pct"), "{err}");

        // max_failure_rate is a rate in [0, 1].
        let oversized_rate =
            MINIMAL_CASE.replace("max_failure_rate = 0.05", "max_failure_rate = 1.5");
        let scenario = write_throughput_scenario(&oversized_rate);
        let err = scenario
            .write_measure
            .validate()
            .expect_err("max_failure_rate above 1.0 must be rejected");
        assert!(err.contains("max_failure_rate"), "{err}");
    }

    #[test]
    fn parses_builtin_write_read_mixed_case() {
        let text = std::fs::read_to_string(builtin_write_read_mixed_case())
            .expect("built-in write_read_mixed case.toml must exist");
        let case = parse_case(&text).expect("built-in write_read_mixed case must parse");
        assert_eq!(case.scenario.kind(), "write_throughput");
        let scenario = write_throughput_scenario(&text);
        assert_eq!(scenario.remote_write.series_count, 2048);
        assert_eq!(scenario.remote_write.samples_per_series, 3600);
        assert_eq!(scenario.write_measure.duration_seconds.get(), 60);
        assert_eq!(scenario.write_measure.window_seconds.get(), 5);
        let mix = scenario
            .write_measure
            .mix
            .as_ref()
            .expect("built-in mixed case must declare a mix section");
        assert_eq!(mix.query_interval_ms.get(), 100);
        assert_eq!(mix.query_parallelism.get(), 2);
        assert_eq!(mix.thresholds.max_query_failure_rate, 0.05);
        assert_eq!(mix.thresholds.max_query_p99_regression_pct, 10.0);
        let scheduler = scenario
            .scheduler
            .as_ref()
            .expect("built-in mixed case must declare a scheduler section");
        assert_eq!(scheduler.query_weight, 2);
        assert_eq!(scheduler.write_weight, 8);
        scenario
            .write_measure
            .validate()
            .expect("built-in mixed case must validate");
        scheduler
            .validate()
            .expect("built-in scheduler must validate");
    }

    #[test]
    fn write_throughput_mix_uses_defaults_and_validates() {
        // Defaults apply only for keys the case does not set explicitly; drop
        // the explicit query_parallelism to exercise the default.
        let defaults_case = MIXED_CASE.replace("query_parallelism = 2\n", "");
        let scenario = write_throughput_scenario(&defaults_case);
        let mix = scenario
            .write_measure
            .mix
            .as_ref()
            .expect("mix section must parse into Some");
        assert!(mix.query_sql.is_none());
        assert_eq!(mix.query_interval_ms.get(), 100);
        assert_eq!(mix.query_parallelism.get(), 1);
        mix.validate().expect("mix config must validate");
        scenario
            .write_measure
            .validate()
            .expect("write_measure with mix must validate");
    }

    #[test]
    fn write_throughput_mix_parses_explicit_values() {
        let explicit = MIXED_CASE.replace(
            "query_interval_ms = 100\nquery_parallelism = 2",
            "query_sql = \"SELECT count(*) FROM greptime_physical_table\"\nquery_interval_ms = 250\nquery_parallelism = 4",
        );
        let scenario = write_throughput_scenario(&explicit);
        let mix = scenario
            .write_measure
            .mix
            .expect("mix section must parse into Some");
        assert_eq!(
            mix.query_sql.as_deref(),
            Some("SELECT count(*) FROM greptime_physical_table")
        );
        assert_eq!(mix.query_interval_ms.get(), 250);
        assert_eq!(mix.query_parallelism.get(), 4);
        mix.validate().expect("explicit mix config must validate");
    }

    #[test]
    fn write_throughput_mix_rejects_zero_interval_or_parallelism() {
        let zero_interval = MIXED_CASE.replace("query_interval_ms = 100", "query_interval_ms = 0");
        assert!(
            parse_case(&zero_interval).is_err(),
            "zero query_interval_ms must be rejected"
        );

        let zero_parallelism = MIXED_CASE.replace("query_parallelism = 2", "query_parallelism = 0");
        assert!(
            parse_case(&zero_parallelism).is_err(),
            "zero query_parallelism must be rejected"
        );
    }

    #[test]
    fn write_throughput_mix_rejects_unknown_fields() {
        let unknown = MIXED_CASE.replace(
            "query_interval_ms = 100",
            "query_interval_ms = 100\nbogus_mix_field = 1",
        );
        let err = parse_case(&unknown).expect_err("unknown mix field must be rejected");
        assert!(err.to_string().contains("bogus_mix_field"), "{err}");
    }

    #[test]
    fn write_throughput_mix_rejects_empty_query_sql() {
        let empty_sql = MIXED_CASE.replace(
            "query_interval_ms = 100",
            "query_sql = \"   \"\nquery_interval_ms = 100",
        );
        let scenario = write_throughput_scenario(&empty_sql);
        let mix = scenario
            .write_measure
            .mix
            .expect("mix section must parse into Some");
        let err = mix
            .validate()
            .expect_err("blank query_sql must be rejected");
        assert!(err.contains("query_sql"), "{err}");
    }

    #[test]
    fn write_throughput_mix_rejects_invalid_thresholds() {
        let negative_limit = MIXED_CASE.replace(
            "max_query_p99_regression_pct = 10.0",
            "max_query_p99_regression_pct = -1.0",
        );
        let scenario = write_throughput_scenario(&negative_limit);
        let mix = scenario
            .write_measure
            .mix
            .expect("mix section must parse into Some");
        let err = mix
            .validate()
            .expect_err("negative query p99 regression limit must be rejected");
        assert!(err.contains("max_query_p99_regression_pct"), "{err}");

        let oversized_rate = MIXED_CASE.replace(
            "max_query_failure_rate = 0.05",
            "max_query_failure_rate = 1.5",
        );
        let scenario = write_throughput_scenario(&oversized_rate);
        let mix = scenario
            .write_measure
            .mix
            .expect("mix section must parse into Some");
        let err = mix
            .validate()
            .expect_err("query failure rate above 1.0 must be rejected");
        assert!(err.contains("max_query_failure_rate"), "{err}");
    }
}
