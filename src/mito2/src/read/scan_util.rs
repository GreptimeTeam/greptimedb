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

use std::fmt;
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
use crate::sst::parquet::reader::{ReaderFilterMetrics, ReaderMetrics};

/// Verbose scan metrics for a partition.
#[derive(Default)]
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

    /// Number of send timeout in SeriesScan.
    num_series_send_timeout: usize,
    /// Number of rows the series distributor scanned.
    num_distributor_rows: usize,
    /// Number of batches the series distributor scanned.
    num_distributor_batches: usize,
    /// Duration of the series distributor to scan.
    distributor_scan_cost: Duration,
    /// Duration of the series distributor to yield.
    distributor_yield_cost: Duration,
}

impl fmt::Debug for ScanMetricsSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ScanMetricsSet {
            prepare_scan_cost,
            build_reader_cost,
            scan_cost,
            convert_cost,
            yield_cost,
            total_cost,
            num_rows,
            num_batches,
            num_mem_ranges,
            num_file_ranges,
            build_parts_cost,
            rg_total,
            rg_fulltext_filtered,
            rg_inverted_filtered,
            rg_minmax_filtered,
            rg_bloom_filtered,
            rows_before_filter,
            rows_fulltext_filtered,
            rows_inverted_filtered,
            rows_bloom_filtered,
            rows_precise_filtered,
            num_sst_record_batches,
            num_sst_batches,
            num_sst_rows,
            first_poll,
            num_series_send_timeout,
            num_distributor_rows,
            num_distributor_batches,
            distributor_scan_cost,
            distributor_yield_cost,
        } = self;

        write!(
            f,
            "{{prepare_scan_cost={prepare_scan_cost:?}, \
            build_reader_cost={build_reader_cost:?}, \
            scan_cost={scan_cost:?}, \
            convert_cost={convert_cost:?}, \
            yield_cost={yield_cost:?}, \
            total_cost={total_cost:?}, \
            num_rows={num_rows}, \
            num_batches={num_batches}, \
            num_mem_ranges={num_mem_ranges}, \
            num_file_ranges={num_file_ranges}, \
            build_parts_cost={build_parts_cost:?}, \
            rg_total={rg_total}, \
            rg_fulltext_filtered={rg_fulltext_filtered}, \
            rg_inverted_filtered={rg_inverted_filtered}, \
            rg_minmax_filtered={rg_minmax_filtered}, \
            rg_bloom_filtered={rg_bloom_filtered}, \
            rows_before_filter={rows_before_filter}, \
            rows_fulltext_filtered={rows_fulltext_filtered}, \
            rows_inverted_filtered={rows_inverted_filtered}, \
            rows_bloom_filtered={rows_bloom_filtered}, \
            rows_precise_filtered={rows_precise_filtered}, \
            num_sst_record_batches={num_sst_record_batches}, \
            num_sst_batches={num_sst_batches}, \
            num_sst_rows={num_sst_rows}, \
            first_poll={first_poll:?}, \
            num_series_send_timeout={num_series_send_timeout}, \
            num_distributor_rows={num_distributor_rows}, \
            num_distributor_batches={num_distributor_batches}, \
            distributor_scan_cost={distributor_scan_cost:?}, \
            distributor_yield_cost={distributor_yield_cost:?}}},"
        )
    }
}
impl ScanMetricsSet {
    /// Attaches the `prepare_scan_cost` to the metrics set.
    fn with_prepare_scan_cost(mut self, cost: Duration) -> Self {
        self.prepare_scan_cost += cost;
        self
    }

    /// Merges the local scanner metrics.
    fn merge_scanner_metrics(&mut self, other: &ScannerMetrics) {
        let ScannerMetrics {
            prepare_scan_cost,
            build_reader_cost,
            scan_cost,
            convert_cost,
            yield_cost,
            num_batches,
            num_rows,
            num_mem_ranges,
            num_file_ranges,
        } = other;

        self.prepare_scan_cost += *prepare_scan_cost;
        self.build_reader_cost += *build_reader_cost;
        self.scan_cost += *scan_cost;
        self.convert_cost += *convert_cost;
        self.yield_cost += *yield_cost;
        self.num_rows += *num_rows;
        self.num_batches += *num_batches;
        self.num_mem_ranges += *num_mem_ranges;
        self.num_file_ranges += *num_file_ranges;
    }

    /// Merges the local reader metrics.
    fn merge_reader_metrics(&mut self, other: &ReaderMetrics) {
        let ReaderMetrics {
            build_cost,
            filter_metrics:
                ReaderFilterMetrics {
                    rg_total,
                    rg_fulltext_filtered,
                    rg_inverted_filtered,
                    rg_minmax_filtered,
                    rg_bloom_filtered,
                    rows_total,
                    rows_fulltext_filtered,
                    rows_inverted_filtered,
                    rows_bloom_filtered,
                    rows_precise_filtered,
                },
            num_record_batches,
            num_batches,
            num_rows,
            scan_cost: _,
        } = other;

        self.build_parts_cost += *build_cost;

        self.rg_total += *rg_total;
        self.rg_fulltext_filtered += *rg_fulltext_filtered;
        self.rg_inverted_filtered += *rg_inverted_filtered;
        self.rg_minmax_filtered += *rg_minmax_filtered;
        self.rg_bloom_filtered += *rg_bloom_filtered;

        self.rows_before_filter += *rows_total;
        self.rows_fulltext_filtered += *rows_fulltext_filtered;
        self.rows_inverted_filtered += *rows_inverted_filtered;
        self.rows_bloom_filtered += *rows_bloom_filtered;
        self.rows_precise_filtered += *rows_precise_filtered;

        self.num_sst_record_batches += *num_record_batches;
        self.num_sst_batches += *num_batches;
        self.num_sst_rows += *num_rows;
    }

    /// Sets distributor metrics.
    fn set_distributor_metrics(&mut self, distributor_metrics: &SeriesDistributorMetrics) {
        let SeriesDistributorMetrics {
            num_series_send_timeout,
            num_rows,
            num_batches,
            scan_cost,
            yield_cost,
        } = distributor_metrics;

        self.num_series_send_timeout += *num_series_send_timeout;
        self.num_distributor_rows += *num_rows;
        self.num_distributor_batches += *num_batches;
        self.distributor_scan_cost += *scan_cost;
        self.distributor_yield_cost += *yield_cost;
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

/// List of PartitionMetrics.
#[derive(Default)]
pub(crate) struct PartitionMetricsList(Mutex<Vec<Option<PartitionMetrics>>>);

impl PartitionMetricsList {
    /// Sets a new [PartitionMetrics] at the specified partition.
    pub(crate) fn set(&self, partition: usize, metrics: PartitionMetrics) {
        let mut list = self.0.lock().unwrap();
        if list.len() <= partition {
            list.resize(partition + 1, None);
        }
        list[partition] = Some(metrics);
    }

    /// Format verbose metrics for each partition for explain.
    pub(crate) fn format_verbose_metrics(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let list = self.0.lock().unwrap();
        write!(f, ", metrics_per_partition: ")?;
        f.debug_list()
            .entries(list.iter().filter_map(|p| p.as_ref()))
            .finish()
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
        let metrics = ScanMetricsSet::default().with_prepare_scan_cost(query_start.elapsed());
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

    /// Finishes the query.
    pub(crate) fn on_finish(&self) {
        self.0.on_finish();
    }

    /// Sets the distributor metrics.
    pub(crate) fn set_distributor_metrics(&self, metrics: &SeriesDistributorMetrics) {
        let mut metrics_set = self.0.metrics.lock().unwrap();
        metrics_set.set_distributor_metrics(metrics);
    }
}

impl fmt::Debug for PartitionMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let metrics = self.0.metrics.lock().unwrap();
        write!(f, "[partition={}, {:?}]", self.0.partition, metrics)
    }
}

/// Metrics for the series distributor.
#[derive(Default)]
pub(crate) struct SeriesDistributorMetrics {
    /// Number of send timeout in SeriesScan.
    pub(crate) num_series_send_timeout: usize,
    /// Number of rows the series distributor scanned.
    pub(crate) num_rows: usize,
    /// Number of batches the series distributor scanned.
    pub(crate) num_batches: usize,
    /// Duration of the series distributor to scan.
    pub(crate) scan_cost: Duration,
    /// Duration of the series distributor to yield.
    pub(crate) yield_cost: Duration,
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
        reader_metrics.filter_metrics.observe();
        part_metrics.merge_reader_metrics(&reader_metrics);
    }
}
