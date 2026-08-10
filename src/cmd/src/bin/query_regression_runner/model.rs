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

use object_store::config::ObjectStoreConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Deserialize)]
pub(super) struct DestinationConfig {
    pub(super) data_home: String,
    pub(super) object_store: ObjectStoreConfig,
}

#[derive(Debug, Deserialize)]
pub(super) struct FixtureSummary {
    pub(super) region_dir: String,
}

#[derive(Debug, Default, Serialize)]
pub(super) struct CopyCounts {
    pub(super) files: u64,
    pub(super) bytes: u64,
}

#[derive(Debug, Serialize)]
pub(super) struct MaterializeResult {
    pub(super) region_dir: String,
    pub(super) object_store: CopyCounts,
    pub(super) manifest: CopyCounts,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
pub(super) enum Scenario {
    #[serde(rename = "direct_readable_sst")]
    DirectReadableSst {
        tables: Vec<Table>,
        layout: Layout,
        #[serde(default)]
        queries: Vec<Query>,
    },
    #[serde(rename = "prom_remote_write_then_query")]
    PromRemoteWriteThenQuery {
        remote_write: RemoteWrite,
        #[serde(default)]
        queries: Vec<Query>,
    },
    #[serde(rename = "otlp_trace_load")]
    OtlpTraceLoad { load: OtlpTraceLoad },
    #[serde(rename = "write_throughput")]
    WriteThroughput {
        remote_write: RemoteWrite,
        write_measure: WriteMeasure,
        #[serde(default)]
        scheduler: Option<WorkloadSchedulerConfig>,
    },
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct WriteMeasure {
    pub(super) duration_seconds: u64,
    pub(super) window_seconds: u64,
    #[serde(default)]
    pub(super) target_rps: f64,
    pub(super) thresholds: WriteThroughputThresholds,
    #[serde(default)]
    pub(super) mix: Option<MixMeasure>,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct WriteThroughputThresholds {
    pub(super) max_failure_rate: f64,
    pub(super) max_mean_rps_regression_pct: f64,
    pub(super) max_p99_latency_regression_pct: f64,
    #[serde(default)]
    pub(super) min_rps_absolute: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct MixMeasure {
    #[serde(default)]
    pub(super) query_sql: Option<String>,
    #[serde(default)]
    pub(super) query_interval_ms: u64,
    #[serde(default)]
    pub(super) query_parallelism: u64,
    pub(super) thresholds: MixMeasureThresholds,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct MixMeasureThresholds {
    pub(super) max_query_failure_rate: f64,
    pub(super) max_query_p99_regression_pct: f64,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct WorkloadSchedulerConfig {
    /// Mirrored from the case for report parity; the runner derives the
    /// per-target enable flag instead of reading this field.
    #[serde(default)]
    #[allow(dead_code)]
    pub(super) enable: bool,
    #[serde(default)]
    pub(super) max_concurrent_polls: u64,
    #[serde(default)]
    pub(super) query_weight: u64,
    #[serde(default)]
    pub(super) write_weight: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct Layout {
    pub(super) regions: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct Table {
    pub(super) database: String,
    pub(super) name: String,
    #[serde(default)]
    pub(super) engine: String,
    #[serde(default)]
    pub(super) columns: Vec<Column>,
    #[serde(default)]
    pub(super) primary_key: Vec<String>,
    #[serde(default)]
    pub(super) time_index: Option<String>,
    #[serde(default)]
    pub(super) append_mode: Option<bool>,
    #[serde(default)]
    pub(super) sst_format: Option<String>,
    #[serde(default = "default_show_create_engine")]
    pub(super) validate_show_create_engine: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct Column {
    pub(super) name: String,
    #[serde(rename = "type")]
    pub(super) data_type: String,
}

const fn default_show_create_engine() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct RemoteWrite {
    pub(super) database: String,
    pub(super) metric: String,
    pub(super) physical_table: String,
    pub(super) series_count: u64,
    pub(super) samples_per_series: u64,
    pub(super) start_unix_millis: i64,
    pub(super) step_millis: i64,
    pub(super) chunk_series_count: u64,
    pub(super) timeout_seconds: u64,
    pub(super) sample_chunk_size: Option<u64>,
    pub(super) flush_every_sample_chunks: u64,
    pub(super) visibility_timeout_seconds: u64,
    pub(super) prom_store: PromStore,
    pub(super) value: RemoteValue,
    pub(super) storage: Option<StorageConfig>,
    pub(super) read_bench: Option<ReadBenchConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct PromStore {
    pub(super) pending_rows_flush_interval: String,
    pub(super) max_batch_rows: u64,
    pub(super) max_concurrent_flushes: u64,
    pub(super) worker_channel_capacity: u64,
    pub(super) max_inflight_requests: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct RemoteValue {
    pub(super) pattern: String,
    pub(super) base: f64,
    pub(super) step: f64,
    pub(super) cardinality: u64,
    pub(super) seed: u64,
    pub(super) run_length: u64,
    pub(super) stall_every: u64,
    pub(super) stall_length: u64,
    pub(super) mixed_every: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct OtlpTraceLoad {
    pub(super) load: OtlpLoad,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct OtlpLoad {
    pub(super) database: String,
    pub(super) table: String,
    pub(super) pipeline: String,
    pub(super) duration_seconds: u64,
    pub(super) warmup_seconds: u64,
    pub(super) rate: u64,
    pub(super) workers: usize,
    pub(super) exporter_shards: usize,
    pub(super) workload: String,
    pub(super) visibility_timeout_seconds: u64,
    pub(super) thresholds: OtlpThresholds,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct OtlpThresholds {
    pub(super) max_candidate_throughput_regression_pct: f64,
    pub(super) max_candidate_mean_latency_regression_pct: f64,
    pub(super) max_failure_count: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct StorageConfig {
    pub(super) inspect: bool,
    pub(super) column: String,
    pub(super) root_suffix: Option<String>,
    pub(super) include_metadata_files: bool,
    pub(super) min_files: u64,
    pub(super) min_files_with_column: u64,
    pub(super) require_encodings: Vec<String>,
    pub(super) forbid_encodings: Vec<String>,
    pub(super) max_total_file_size_bytes: Option<u64>,
    pub(super) max_column_compressed_size_bytes: Option<u64>,
    pub(super) max_column_uncompressed_size_bytes: Option<u64>,
    pub(super) max_candidate_total_file_size_regression_pct: Option<f64>,
    pub(super) max_candidate_column_compressed_size_regression_pct: Option<f64>,
    pub(super) max_candidate_column_uncompressed_size_regression_pct: Option<f64>,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct ReadBenchConfig {
    pub(super) enabled: bool,
    pub(super) parquetbench: bool,
    pub(super) scanbench: bool,
    pub(super) iterations: u64,
    pub(super) projection: Vec<String>,
    pub(super) parquet_reader: String,
    pub(super) scan_scanner: String,
    pub(super) parallelism: u64,
    pub(super) max_files: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct Query {
    #[serde(default)]
    pub(super) name: Option<String>,
    #[serde(default)]
    pub(super) kind: Option<String>,
    pub(super) query: String,
    #[serde(default)]
    pub(super) warmup: usize,
    #[serde(default = "one")]
    pub(super) iterations: usize,
    #[serde(default)]
    pub(super) thresholds: Map<String, Value>,
}

const fn one() -> usize {
    1
}

#[derive(Debug, Serialize)]
pub(super) struct QueryResult {
    pub(super) validation: Vec<Value>,
    pub(super) validation_errors: Vec<Value>,
    pub(super) measurements: Vec<Measurement>,
    pub(super) status: String,
}

#[derive(Debug, Serialize)]
pub(super) struct Measurement {
    pub(super) name: Option<String>,
    pub(super) kind: Option<String>,
    pub(super) iterations: usize,
    pub(super) samples: Vec<Value>,
    pub(super) latency_ms_median: Option<f64>,
    pub(super) latency_ms_p95: Option<f64>,
    pub(super) status: String,
}
