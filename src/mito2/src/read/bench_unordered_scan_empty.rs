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

//! Micro-benchmark: demonstrates that `UnorderedScan` still iterates over
//! all per-row-group file ranges even when every SST file is pruned by
//! manifest-level time-range pruning.

use std::sync::Arc;
use std::time::Instant;

use common_time::Timestamp;
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, col, lit};
use futures::StreamExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionScanner;
use store_api::storage::{FileId, RegionId};

use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::scan_region::{PredicateGroup, ScanInput};
use crate::read::stream::ScanBatch;
use crate::read::unordered_scan::UnorderedScan;
use crate::sst::file::{FileHandle, FileMeta};
use crate::test_util::memtable_util::metadata_with_primary_key;
use crate::test_util::new_noop_file_purger;
use crate::test_util::scheduler_util::SchedulerEnv;

/// This test constructs many `FileHandle`s, each with `num_row_groups > 0`.
/// All files have a manifest time range of `[0ms, 1000ms]`. A predicate
/// `ts > 10_000ms` (future timestamp) is pushed down so that
/// `ScanInput::prune_file()`'s manifest-level pruning marks every file as
/// pruned. The benchmark then runs `UnorderedScan::scan_all_partitions()`
/// and drains the stream to EOF, measuring the wall time wasted on the
/// already-pruned range loop.
///
/// Run with:
/// ```text
/// MITO_EMPTY_PRUNE_FILES=100 MITO_EMPTY_PRUNE_ROW_GROUPS=100 \
///   cargo test -p mito2 --features test \
///   bench_unordered_scan_empty_pruned_file_ranges \
///   -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore]
async fn bench_unordered_scan_empty_pruned_file_ranges() {
    let num_files: usize = std::env::var("MITO_EMPTY_PRUNE_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let row_groups_per_file: u64 = std::env::var("MITO_EMPTY_PRUNE_ROW_GROUPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let expected_file_ranges = num_files * row_groups_per_file as usize;

    eprintln!("--- bench_unordered_scan_empty_pruned_file_ranges ---");
    eprintln!("  files:               {num_files}");
    eprintln!("  row_groups_per_file: {row_groups_per_file}");
    eprintln!("  expected_file_ranges:{expected_file_ranges}");

    // ── Build file handles with time range [0ms, 1000ms] ──────────────
    let file_purger = new_noop_file_purger();
    let files: Vec<FileHandle> = (0..num_files)
        .map(|_| {
            let meta = FileMeta {
                region_id: RegionId::new(123, 456),
                file_id: FileId::random(),
                time_range: (
                    Timestamp::new_millisecond(0),
                    Timestamp::new_millisecond(1000),
                ),
                num_row_groups: row_groups_per_file,
                num_rows: row_groups_per_file * 1024,
                level: 0,
                ..Default::default()
            };
            FileHandle::new(meta, file_purger.clone())
        })
        .collect();

    // ── Metadata & mapper (k0, k1, ts, v0, v1) ───────────────────────
    let metadata: RegionMetadataRef = Arc::new(metadata_with_primary_key(vec![0, 1], false));
    let mapper = FlatProjectionMapper::new(&metadata, [0, 2, 3]).unwrap();

    // ── Predicate: ts > 10_000 ms (all files are in the past) ────────
    let predicate_exprs: Vec<Expr> =
        vec![col("ts").gt(lit(ScalarValue::TimestampMillisecond(Some(10_000), None)))];
    let predicate = PredicateGroup::new(&metadata, &predicate_exprs).unwrap();

    // ── ScanInput (append mode to use UnorderedScan range layout) ────
    let env = SchedulerEnv::new().await;
    let input = ScanInput::new(env.access_layer.clone(), mapper)
        .with_files(files)
        .with_predicate(predicate)
        .with_append_mode(true);

    let total_rows = input.total_rows();
    let num_files_in_input = input.num_files();
    eprintln!("  total_rows:          {total_rows}");
    eprintln!("  num_files_in_input:  {num_files_in_input}");

    // ── Phase 1: UnorderedScan::new (range expansion) ──────────────
    let start_new = Instant::now();
    let scanner = UnorderedScan::new(input);
    let new_cost = start_new.elapsed();
    let partition_count = scanner.properties().num_partitions();
    eprintln!("  partition_count:     {partition_count}");

    // ── Phase 2: create scan stream ─────────────────────────────────
    let start_stream = Instant::now();
    let batch_stream = match scanner.scan_all_partitions() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  ERROR: scan_all_partitions failed: {e}");
            return;
        }
    };
    let stream_cost = start_stream.elapsed();

    // ── Phase 3: drain to EOF ───────────────────────────────────────
    let start_drain = Instant::now();
    let mut output_rows: usize = 0;
    futures::pin_mut!(batch_stream);
    while let Some(item) = batch_stream.next().await {
        match item {
            Ok(ScanBatch::RecordBatch(rb)) => {
                output_rows += rb.num_rows();
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("  ERROR scanning partition: {e}");
            }
        }
    }
    let drain_cost = start_drain.elapsed();
    let total_elapsed = new_cost + stream_cost + drain_cost;

    // ── Report ───────────────────────────────────────────────────────
    eprintln!("  new_cost:            {new_cost:?} (range expansion + Sprunner spawn)");
    eprintln!("  stream_cost:         {stream_cost:?} (scan_all_partitions)");
    eprintln!("  drain_cost:          {drain_cost:?} (consume to EOF)");
    eprintln!("  total_elapsed:       {total_elapsed:?}");
    eprintln!("  output_rows:         {output_rows}");
    eprintln!("  expected empty: all {expected_file_ranges} file ranges should be manifest-pruned");
}
