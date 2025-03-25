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

//! Utilities for scanners.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_telemetry::debug;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};
use futures::Stream;
use prometheus::IntGauge;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::metrics::{
    IN_PROGRESS_SCAN, PRECISE_FILTER_ROWS_TOTAL, READ_BATCHES_RETURN, READ_ROWS_IN_ROW_GROUP_TOTAL,
    READ_ROWS_RETURN, READ_ROW_GROUPS_TOTAL, READ_STAGE_ELAPSED,
};
use crate::read::range::{RangeBuilderList, RowGroupIndex};
use crate::read::scan_region::StreamContext;
use crate::read::{Batch, ScannerMetrics, Source};
use crate::sst::file::FileTimeRange;
use crate::sst::parquet::reader::ReaderMetrics;

/// Verbose scan metrics for a partition.
#[derive(Debug, Default)]
struct ScanMetricsSet {
    /// Duration to prepare the scan task.
    prepare_scan_cost: Duration,
    /// Duration to build the (merge) reader.
    build_reader_cost: Duration,
    /// Duration to scan data.
    scan_cost: Duration,
    /// Duration to convert batches.
    convert_cost: Duration,
    /// Duration while waiting for `yield`.
    yield_cost: Duration,
    /// Duration of the scan.
    total_cost: Duration,
    /// Number of rows returned.
    num_rows: usize,
    /// Number of batches returned.
    num_batches: usize,
    /// Number of mem ranges scanned.
    num_mem_ranges: usize,
    /// Number of file ranges scanned.
    num_file_ranges: usize,

    // SST related metrics:
    /// Duration to build file ranges.
    build_parts_cost: Duration,
    /// Number of row groups before filtering.
    rg_total: usize,
    /// Number of row groups filtered by fulltext index.
    rg_fulltext_filtered: usize,
    /// Number of row groups filtered by inverted index.
    rg_inverted_filtered: usize,
    /// Number of row groups filtered by min-max index.
    rg_minmax_filtered: usize,
    /// Number of row groups filtered by bloom filter index.
    rg_bloom_filtered: usize,
    /// Number of rows in row group before filtering.
    rows_before_filter: usize,
    /// Number of rows in row group filtered by fulltext index.
    rows_fulltext_filtered: usize,
    /// Number of rows in row group filtered by inverted index.
    rows_inverted_filtered: usize,
    /// Number of rows in row group filtered by bloom filter index.
    rows_bloom_filtered: usize,
    /// Number of rows filtered by precise filter.
    rows_precise_filtered: usize,
    /// Number of record batches read from SST.
    num_sst_record_batches: usize,
    /// Number of batches decoded from SST.
    num_sst_batches: usize,
    /// Number of rows read from SST.
    num_sst_rows: usize,

    /// Elapsed time before the first poll operation.
    first_poll: Duration,
}

impl ScanMetricsSet {
    /// Merges the local scanner metrics.
    fn merge_scanner_metrics(&mut self, other: &ScannerMetrics) {
        self.prepare_scan_cost += other.prepare_scan_cost;
        self.build_reader_cost += other.build_reader_cost;
        self.scan_cost += other.scan_cost;
        self.convert_cost += other.convert_cost;
        self.yield_cost += other.yield_cost;
        self.num_rows += other.num_rows;
        self.num_batches += other.num_batches;
        self.num_mem_ranges += other.num_mem_ranges;
        self.num_file_ranges += other.num_file_ranges;
    }

    /// Merges the local reader metrics.
    fn merge_reader_metrics(&mut self, other: &ReaderMetrics) {
        self.build_parts_cost += other.build_cost;

        self.rg_total += other.filter_metrics.rg_total;
        self.rg_fulltext_filtered += other.filter_metrics.rg_fulltext_filtered;
        self.rg_inverted_filtered += other.filter_metrics.rg_inverted_filtered;
        self.rg_minmax_filtered += other.filter_metrics.rg_minmax_filtered;
        self.rg_bloom_filtered += other.filter_metrics.rg_bloom_filtered;

        self.rows_before_filter += other.filter_metrics.rows_total;
        self.rows_fulltext_filtered += other.filter_metrics.rows_fulltext_filtered;
        self.rows_inverted_filtered += other.filter_metrics.rows_inverted_filtered;
        self.rows_bloom_filtered += other.filter_metrics.rows_bloom_filtered;
        self.rows_precise_filtered += other.filter_metrics.rows_precise_filtered;

        self.num_sst_record_batches += other.num_record_batches;
        self.num_sst_batches += other.num_batches;
        self.num_sst_rows += other.num_rows;
    }

    /// Observes metrics.
    fn observe_metrics(&self) {
        READ_STAGE_ELAPSED
            .with_label_values(&["prepare_scan"])
            .observe(self.prepare_scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["build_reader"])
            .observe(self.build_reader_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["convert_rb"])
            .observe(self.convert_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["scan"])
            .observe(self.scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["yield"])
            .observe(self.yield_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["total"])
            .observe(self.total_cost.as_secs_f64());
        READ_ROWS_RETURN.observe(self.num_rows as f64);
        READ_BATCHES_RETURN.observe(self.num_batches as f64);

        READ_STAGE_ELAPSED
            .with_label_values(&["build_parts"])
            .observe(self.build_parts_cost.as_secs_f64());

        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(self.rg_total as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["fulltext_index_filtered"])
            .inc_by(self.rg_fulltext_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(self.rg_inverted_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["minmax_index_filtered"])
            .inc_by(self.rg_minmax_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["bloom_filter_index_filtered"])
            .inc_by(self.rg_bloom_filtered as u64);

        PRECISE_FILTER_ROWS_TOTAL
            .with_label_values(&["parquet"])
            .inc_by(self.rows_precise_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(self.rows_before_filter as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["fulltext_index_filtered"])
            .inc_by(self.rows_fulltext_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(self.rows_inverted_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["bloom_filter_index_filtered"])
            .inc_by(self.rows_bloom_filtered as u64);
    }
}

struct PartitionMetricsInner {
    region_id: RegionId,
    /// Index of the partition to scan.
    partition: usize,
    /// Label to distinguish different scan operation.
    scanner_type: &'static str,
    /// Query start time.
    query_start: Instant,
    /// Verbose scan metrics that only log to debug logs by default.
    metrics: Mutex<ScanMetricsSet>,
    in_progress_scan: IntGauge,

    // Normal metrics that always report to the [ExecutionPlanMetricsSet]:
    /// Duration to build file ranges.
    build_parts_cost: Time,
    /// Duration to build the (merge) reader.
    build_reader_cost: Time,
    /// Duration to scan data.
    scan_cost: Time,
    /// Duration while waiting for `yield`.
    yield_cost: Time,
}

impl PartitionMetricsInner {
    fn on_finish(&self) {
        let mut metrics = self.metrics.lock().unwrap();
        if metrics.total_cost.is_zero() {
            metrics.total_cost = self.query_start.elapsed();
        }
    }
}

impl Drop for PartitionMetricsInner {
    fn drop(&mut self) {
        self.on_finish();
        let metrics = self.metrics.lock().unwrap();
        metrics.observe_metrics();
        self.in_progress_scan.dec();

        debug!(
            "{} finished, region_id: {}, partition: {}, metrics: {:?}",
            self.scanner_type, self.region_id, self.partition, metrics
        );
    }
}

/// Metrics while reading a partition.
#[derive(Clone)]
pub(crate) struct PartitionMetrics(Arc<PartitionMetricsInner>);

impl PartitionMetrics {
    pub(crate) fn new(
        region_id: RegionId,
        partition: usize,
        scanner_type: &'static str,
        query_start: Instant,
        metrics_set: &ExecutionPlanMetricsSet,
    ) -> Self {
        let partition_str = partition.to_string();
        let in_progress_scan = IN_PROGRESS_SCAN.with_label_values(&[scanner_type, &partition_str]);
        in_progress_scan.inc();
        let mut metrics = ScanMetricsSet::default();
        metrics.prepare_scan_cost = query_start.elapsed();
        let inner = PartitionMetricsInner {
            region_id,
            partition,
            scanner_type,
            query_start,
            metrics: Mutex::new(metrics),
            in_progress_scan,
            build_parts_cost: MetricBuilder::new(metrics_set)
                .subset_time("build_parts_cost", partition),
            build_reader_cost: MetricBuilder::new(metrics_set)
                .subset_time("build_reader_cost", partition),
            scan_cost: MetricBuilder::new(metrics_set).subset_time("scan_cost", partition),
            yield_cost: MetricBuilder::new(metrics_set).subset_time("yield_cost", partition),
        };
        Self(Arc::new(inner))
    }

    pub(crate) fn on_first_poll(&self) {
        let mut metrics = self.0.metrics.lock().unwrap();
        metrics.first_poll = self.0.query_start.elapsed();
    }

    pub(crate) fn inc_num_mem_ranges(&self, num: usize) {
        let mut metrics = self.0.metrics.lock().unwrap();
        metrics.num_mem_ranges += num;
    }

    pub(crate) fn inc_num_file_ranges(&self, num: usize) {
        let mut metrics = self.0.metrics.lock().unwrap();
        metrics.num_file_ranges += num;
    }

    /// Merges `build_reader_cost`.
    pub(crate) fn inc_build_reader_cost(&self, cost: Duration) {
        self.0.build_reader_cost.add_duration(cost);

        let mut metrics = self.0.metrics.lock().unwrap();
        metrics.build_reader_cost += cost;
    }

    /// Merges [ScannerMetrics], `build_reader_cost`, `scan_cost` and `yield_cost`.
    pub(crate) fn merge_metrics(&self, metrics: &ScannerMetrics) {
        self.0
            .build_reader_cost
            .add_duration(metrics.build_reader_cost);
        self.0.scan_cost.add_duration(metrics.scan_cost);
        self.0.yield_cost.add_duration(metrics.yield_cost);

        let mut metrics_set = self.0.metrics.lock().unwrap();
        metrics_set.merge_scanner_metrics(metrics);
    }

    /// Merges [ReaderMetrics] and `build_reader_cost`.
    pub(crate) fn merge_reader_metrics(&self, metrics: &ReaderMetrics) {
        self.0.build_parts_cost.add_duration(metrics.build_cost);

        let mut metrics_set = self.0.metrics.lock().unwrap();
        metrics_set.merge_reader_metrics(metrics);
    }

    pub(crate) fn on_finish(&self) {
        self.0.on_finish();
    }
}

/// Scans memtable ranges at `index`.
pub(crate) fn scan_mem_ranges(
    stream_ctx: Arc<StreamContext>,
    part_metrics: PartitionMetrics,
    index: RowGroupIndex,
    time_range: FileTimeRange,
) -> impl Stream<Item = Result<Batch>> {
    try_stream! {
        let ranges = stream_ctx.input.build_mem_ranges(index);
        part_metrics.inc_num_mem_ranges(ranges.len());
        for range in ranges {
            let build_reader_start = Instant::now();
            let iter = range.build_iter(time_range)?;
            part_metrics.inc_build_reader_cost(build_reader_start.elapsed());

            let mut source = Source::Iter(iter);
            while let Some(batch) = source.next_batch().await? {
                yield batch;
            }
        }
    }
}

/// Scans file ranges at `index`.
pub(crate) fn scan_file_ranges(
    stream_ctx: Arc<StreamContext>,
    part_metrics: PartitionMetrics,
    index: RowGroupIndex,
    read_type: &'static str,
    range_builder: Arc<RangeBuilderList>,
) -> impl Stream<Item = Result<Batch>> {
    try_stream! {
        let mut reader_metrics = ReaderMetrics::default();
        let ranges = range_builder.build_file_ranges(&stream_ctx.input, index, &mut reader_metrics).await?;
        part_metrics.inc_num_file_ranges(ranges.len());

        for range in ranges {
            let build_reader_start = Instant::now();
            let reader = range.reader(stream_ctx.input.series_row_selector).await?;
            let build_cost = build_reader_start.elapsed();
            part_metrics.inc_build_reader_cost(build_cost);
            let compat_batch = range.compat_batch();
            let mut source = Source::PruneReader(reader);
            while let Some(mut batch) = source.next_batch().await? {
                if let Some(compact_batch) = compat_batch {
                    batch = compact_batch.compat_batch(batch)?;
                }
                yield batch;
            }
            if let Source::PruneReader(reader) = source {
                let prune_metrics = reader.metrics();
                reader_metrics.merge_from(&prune_metrics);
            }
        }

        // Reports metrics.
        reader_metrics.observe_rows(read_type);
        part_metrics.merge_reader_metrics(&reader_metrics);
    }
}
