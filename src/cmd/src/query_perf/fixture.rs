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

mod direct_sst;
mod inspect_footer;
mod prom_remote_write;

use std::path::PathBuf;

pub use direct_sst::run_direct_sst;
pub use inspect_footer::inspect_footer;
pub use prom_remote_write::run_prom_remote_write;

use crate::query_perf::case::{ValidatedCase, ValuePattern};
use crate::query_perf::error::Result;

/// Typed, already-adapted request for direct SST generation.
#[derive(Debug)]
pub struct DirectSstRequest {
    pub case: ValidatedCase,
    pub case_name: String,
    pub out_dir: PathBuf,
    pub region_id: u64,
    pub table_dir: Option<String>,
    pub table: Option<String>,
    pub checkpoint_version: u64,
    pub allow_large: bool,
    pub dry_run: bool,
}

#[derive(Debug, serde::Serialize)]
pub struct DirectSstSummary {
    pub case: String,
    pub table: String,
    pub database: String,
    pub sst_count: usize,
    pub rows_per_sst: usize,
    pub dry_run: bool,
}

/// Typed remote-write request. CLI defaults belong in the binary adapter.
#[derive(Debug, Clone)]
pub struct RemoteWriteRequest {
    pub endpoint: String,
    pub database: String,
    pub metric: String,
    pub physical_table: String,
    pub series_count: u64,
    pub samples_per_series: u64,
    pub start_unix_millis: i64,
    pub step_millis: i64,
    pub chunk_series_count: u64,
    pub timeout_seconds: u64,
    pub value_pattern: ValuePattern,
    pub value_base: f64,
    pub value_step: f64,
    pub value_cardinality: u64,
    pub value_seed: u64,
    pub value_run_length: u64,
    pub value_stall_every: u64,
    pub value_stall_length: u64,
    pub value_mixed_every: u64,
    pub value_sample_offset: u64,
    pub value_total_samples_per_series: Option<u64>,
}

#[derive(Debug, serde::Serialize)]
pub struct RemoteWriteSummary {
    pub endpoint: String,
    pub database: String,
    pub metric: String,
    pub physical_table: String,
    pub series_count: u64,
    pub samples_per_series: u64,
    pub rows: u64,
    pub batches: u64,
    pub elapsed_seconds: f64,
    pub http_statuses: Vec<u16>,
}

#[derive(Debug)]
pub struct FooterRequest {
    pub root: PathBuf,
    pub column: String,
    pub include_metadata_files: bool,
}

#[derive(Debug, serde::Serialize)]
pub struct FooterObservation {
    pub root: String,
    pub summary: FooterSummary,
    pub files: Vec<FooterFileObservation>,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct FooterSummary {
    pub column: String,
    pub file_count: usize,
    pub files_with_column: usize,
    pub total_file_size: u64,
    pub total_rows: i64,
    pub column_compressed_size: i64,
    pub column_uncompressed_size: i64,
    pub unique_encodings: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct FooterFileObservation {
    pub path: String,
    pub relative_path: String,
    pub file_size: u64,
    pub num_rows: i64,
    pub num_row_groups: usize,
    pub columns: Vec<FooterColumnChunkObservation>,
}

#[derive(Debug, serde::Serialize)]
pub struct FooterColumnChunkObservation {
    pub row_group_index: usize,
    pub column_index: usize,
    pub column_path: String,
    pub encodings: Vec<String>,
    pub compression: String,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
    pub num_values: i64,
}

pub fn validate_footer_request(request: &FooterRequest) -> Result<()> {
    if request.column.trim().is_empty() {
        return Err(crate::query_perf::error::Error::new(
            "footer column must be non-empty",
        ));
    }
    Ok(())
}
