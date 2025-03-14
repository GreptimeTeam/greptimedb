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

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_telemetry::debug;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, Time};
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

/// Scan metrics to report to the execution metrics.
struct ScanMetricsSet {
    /// Duration to prepare the scan task.
    prepare_scan_cost: Time,
    /// Duration to build the (merge) reader.
    build_reader_cost: Time,
    /// Duration to scan data.
    scan_cost: Time,
    /// Duration to convert batches.
    convert_cost: Time,
    /// Duration while waiting for `yield`.
    yield_cost: Time,
    /// Duration of the scan.
    total_cost: Time,
    /// Number of rows returned.
    num_rows: Count,
    /// Number of batches returned.
    num_batches: Count,
    /// Number of mem ranges scanned.
    num_mem_ranges: Count,
    /// Number of file ranges scanned.
    num_file_ranges: Count,

    // SST related metrics:
    /// Duration to build file ranges.
    build_parts_cost: Time,
    /// Number of row groups before filtering.
    rg_total: Count,
    /// Number of row groups filtered by fulltext index.
    rg_fulltext_filtered: Count,
    /// Number of row groups filtered by inverted index.
    rg_inverted_filtered: Count,
    /// Number of row groups filtered by min-max index.
    rg_minmax_filtered: Count,
    /// Number of row groups filtered by bloom filter index.
    rg_bloom_filtered: Count,
    /// Number of rows in row group before filtering.
    rows_before_filter: Count,
    /// Number of rows in row group filtered by fulltext index.
    rows_fulltext_filtered: Count,
    /// Number of rows in row group filtered by inverted index.
    rows_inverted_filtered: Count,
    /// Number of rows in row group filtered by bloom filter index.
    rows_bloom_filtered: Count,
    /// Number of rows filtered by precise filter.
    rows_precise_filtered: Count,
    /// Number of record batches read from SST.
    num_sst_record_batches: Count,
    /// Number of batches decoded from SST.
    num_sst_batches: Count,
    /// Number of rows read from SST.
    num_sst_rows: Count,

    /// Elapsed time before the first poll operation.
    first_poll: Time,
}

impl std::fmt::Debug for ScanMetricsSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanMetricsSet")
            .field(
                "prepare_scan_cost",
                &Duration::from_nanos(self.prepare_scan_cost.value() as u64),
            )
            .field(
                "build_reader_cost",
                &Duration::from_nanos(self.build_reader_cost.value() as u64),
            )
            .field(
                "scan_cost",
                &Duration::from_nanos(self.scan_cost.value() as u64),
            )
            .field(
                "convert_cost",
                &Duration::from_nanos(self.convert_cost.value() as u64),
            )
            .field(
                "yield_cost",
                &Duration::from_nanos(self.yield_cost.value() as u64),
            )
            .field(
                "total_cost",
                &Duration::from_nanos(self.total_cost.value() as u64),
            )
            .field("num_rows", &self.num_rows.value())
            .field("num_batches", &self.num_batches.value())
            .field("num_mem_ranges", &self.num_mem_ranges.value())
            .field("num_file_ranges", &self.num_file_ranges.value())
            .field(
                "build_parts_cost",
                &Duration::from_nanos(self.build_parts_cost.value() as u64),
            )
            .field("rg_total", &self.rg_total.value())
            .field("rg_fulltext_filtered", &self.rg_fulltext_filtered.value())
            .field("rg_inverted_filtered", &self.rg_inverted_filtered.value())
            .field("rg_minmax_filtered", &self.rg_minmax_filtered.value())
            .field("rg_bloom_filtered", &self.rg_bloom_filtered.value())
            .field("rows_before_filter", &self.rows_before_filter.value())
            .field(
                "rows_fulltext_filtered",
                &self.rows_fulltext_filtered.value(),
            )
            .field(
                "rows_inverted_filtered",
                &self.rows_inverted_filtered.value(),
            )
            .field("rows_bloom_filtered", &self.rows_bloom_filtered.value())
            .field("rows_precise_filtered", &self.rows_precise_filtered.value())
            .field(
                "num_sst_record_batches",
                &self.num_sst_record_batches.value(),
            )
            .field("num_sst_batches", &self.num_sst_batches.value())
            .field("num_sst_rows", &self.num_sst_rows.value())
            .field(
                "first_poll",
                &Duration::from_nanos(self.first_poll.value() as u64),
            )
            .finish()
    }
}

impl ScanMetricsSet {
    /// Creates a metrics set from an execution metrics.
    fn new(metrics_set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            prepare_scan_cost: MetricBuilder::new(metrics_set)
                .subset_time("prepare_scan_cost", partition),
            build_parts_cost: MetricBuilder::new(metrics_set)
                .subset_time("build_parts_cost", partition),
            build_reader_cost: MetricBuilder::new(metrics_set)
                .subset_time("build_reader_cost", partition),
            scan_cost: MetricBuilder::new(metrics_set).subset_time("scan_cost", partition),
            convert_cost: MetricBuilder::new(metrics_set).subset_time("convert_cost", partition),
            yield_cost: MetricBuilder::new(metrics_set).subset_time("yield_cost", partition),
            total_cost: MetricBuilder::new(metrics_set).subset_time("total_cost", partition),
            num_rows: MetricBuilder::new(metrics_set).counter("num_rows", partition),
            num_batches: MetricBuilder::new(metrics_set).counter("num_batches", partition),
            num_mem_ranges: MetricBuilder::new(metrics_set).counter("num_mem_ranges", partition),
            num_file_ranges: MetricBuilder::new(metrics_set).counter("num_file_ranges", partition),
            rg_total: MetricBuilder::new(metrics_set).counter("rg_total", partition),
            rg_fulltext_filtered: MetricBuilder::new(metrics_set)
                .counter("rg_fulltext_filtered", partition),
            rg_inverted_filtered: MetricBuilder::new(metrics_set)
                .counter("rg_inverted_filtered", partition),
            rg_minmax_filtered: MetricBuilder::new(metrics_set)
                .counter("rg_minmax_filtered", partition),
            rg_bloom_filtered: MetricBuilder::new(metrics_set)
                .counter("rg_bloom_filtered", partition),
            rows_before_filter: MetricBuilder::new(metrics_set)
                .counter("rows_before_filter", partition),
            rows_fulltext_filtered: MetricBuilder::new(metrics_set)
                .counter("rows_fulltext_filtered", partition),
            rows_inverted_filtered: MetricBuilder::new(metrics_set)
                .counter("rows_inverted_filtered", partition),
            rows_bloom_filtered: MetricBuilder::new(metrics_set)
                .counter("rows_bloom_filtered", partition),
            rows_precise_filtered: MetricBuilder::new(metrics_set)
                .counter("rows_precise_filtered", partition),
            num_sst_record_batches: MetricBuilder::new(metrics_set)
                .counter("num_sst_record_batches", partition),
            num_sst_batches: MetricBuilder::new(metrics_set).counter("num_sst_batches", partition),
            num_sst_rows: MetricBuilder::new(metrics_set).counter("num_sst_rows", partition),
            first_poll: MetricBuilder::new(metrics_set).subset_time("first_poll", partition),
        }
    }

    /// Merges the local scanner metrics.
    fn merge_scanner_metrics(&self, other: &ScannerMetrics) {
        self.prepare_scan_cost.add_duration(other.prepare_scan_cost);
        self.build_reader_cost.add_duration(other.build_reader_cost);
        self.scan_cost.add_duration(other.scan_cost);
        self.convert_cost.add_duration(other.convert_cost);
        self.yield_cost.add_duration(other.yield_cost);
        self.num_rows.add(other.num_rows);
        self.num_batches.add(other.num_batches);
        self.num_mem_ranges.add(other.num_mem_ranges);
        self.num_file_ranges.add(other.num_file_ranges);
    }

    /// Merges the local reader metrics.
    fn merge_reader_metrics(&self, other: &ReaderMetrics) {
        self.build_parts_cost.add_duration(other.build_cost);

        self.rg_total.add(other.filter_metrics.rg_total);
        self.rg_fulltext_filtered
            .add(other.filter_metrics.rg_fulltext_filtered);
        self.rg_inverted_filtered
            .add(other.filter_metrics.rg_inverted_filtered);
        self.rg_minmax_filtered
            .add(other.filter_metrics.rg_minmax_filtered);
        self.rg_bloom_filtered
            .add(other.filter_metrics.rg_bloom_filtered);

        self.rows_before_filter.add(other.filter_metrics.rows_total);
        self.rows_fulltext_filtered
            .add(other.filter_metrics.rows_fulltext_filtered);
        self.rows_inverted_filtered
            .add(other.filter_metrics.rows_inverted_filtered);
        self.rows_bloom_filtered
            .add(other.filter_metrics.rows_bloom_filtered);
        self.rows_precise_filtered
            .add(other.filter_metrics.rows_precise_filtered);

        self.num_sst_record_batches.add(other.num_record_batches);
        self.num_sst_batches.add(other.num_batches);
        self.num_sst_rows.add(other.num_rows);
    }

    /// Observes metrics.
    fn observe_metrics(&self) {
        READ_STAGE_ELAPSED
            .with_label_values(&["prepare_scan"])
            .observe(time_to_secs_f64(&self.prepare_scan_cost));
        READ_STAGE_ELAPSED
            .with_label_values(&["build_reader"])
            .observe(time_to_secs_f64(&self.build_reader_cost));
        READ_STAGE_ELAPSED
            .with_label_values(&["convert_rb"])
            .observe(time_to_secs_f64(&self.convert_cost));
        READ_STAGE_ELAPSED
            .with_label_values(&["scan"])
            .observe(time_to_secs_f64(&self.scan_cost));
        READ_STAGE_ELAPSED
            .with_label_values(&["yield"])
            .observe(time_to_secs_f64(&self.yield_cost));
        READ_STAGE_ELAPSED
            .with_label_values(&["total"])
            .observe(time_to_secs_f64(&self.total_cost));
        READ_ROWS_RETURN.observe(self.num_rows.value() as f64);
        READ_BATCHES_RETURN.observe(self.num_batches.value() as f64);

        READ_STAGE_ELAPSED
            .with_label_values(&["build_parts"])
            .observe(time_to_secs_f64(&self.build_parts_cost));

        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(self.rg_total.value() as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["fulltext_index_filtered"])
            .inc_by(self.rg_fulltext_filtered.value() as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(self.rg_inverted_filtered.value() as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["minmax_index_filtered"])
            .inc_by(self.rg_minmax_filtered.value() as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["bloom_filter_index_filtered"])
            .inc_by(self.rg_bloom_filtered.value() as u64);

        PRECISE_FILTER_ROWS_TOTAL
            .with_label_values(&["parquet"])
            .inc_by(self.rows_precise_filtered.value() as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(self.rows_before_filter.value() as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["fulltext_index_filtered"])
            .inc_by(self.rows_fulltext_filtered.value() as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(self.rows_inverted_filtered.value() as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["bloom_filter_index_filtered"])
            .inc_by(self.rows_bloom_filtered.value() as u64);
    }
}

fn time_to_secs_f64(cost: &Time) -> f64 {
    Duration::from_nanos(cost.value() as u64).as_secs_f64()
}

struct PartitionMetricsInner {
    region_id: RegionId,
    /// Index of the partition to scan.
    partition: usize,
    /// Label to distinguish different scan operation.
    scanner_type: &'static str,
    /// Query start time.
    query_start: Instant,
    metrics: ScanMetricsSet,
    in_progress_scan: IntGauge,
}

impl PartitionMetricsInner {
    fn on_finish(&self) {
        if self.metrics.total_cost.value() == 0 {
            self.metrics.total_cost.add_elapsed(self.query_start);
        }
    }
}

impl Drop for PartitionMetricsInner {
    fn drop(&mut self) {
        self.on_finish();
        self.metrics.observe_metrics();
        self.in_progress_scan.dec();

        debug!(
            "{} finished, region_id: {}, partition: {}, metrics: {:?}",
            self.scanner_type, self.region_id, self.partition, self.metrics
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
        let inner = PartitionMetricsInner {
            region_id,
            partition,
            scanner_type,
            query_start,
            metrics: ScanMetricsSet::new(metrics_set, partition),
            in_progress_scan,
        };
        inner.metrics.prepare_scan_cost.add_elapsed(query_start);
        Self(Arc::new(inner))
    }

    pub(crate) fn on_first_poll(&self) {
        self.0.metrics.first_poll.add_elapsed(self.0.query_start);
    }

    pub(crate) fn inc_num_mem_ranges(&self, num: usize) {
        self.0.metrics.num_mem_ranges.add(num);
    }

    pub(crate) fn inc_num_file_ranges(&self, num: usize) {
        self.0.metrics.num_file_ranges.add(num);
    }

    pub(crate) fn inc_build_reader_cost(&self, cost: Duration) {
        self.0.metrics.build_reader_cost.add_duration(cost);
    }

    pub(crate) fn merge_metrics(&self, metrics: &ScannerMetrics) {
        self.0.metrics.merge_scanner_metrics(metrics);
    }

    pub(crate) fn merge_reader_metrics(&self, metrics: &ReaderMetrics) {
        self.0.metrics.merge_reader_metrics(metrics);
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
