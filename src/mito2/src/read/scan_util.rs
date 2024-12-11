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
use futures::Stream;
use prometheus::IntGauge;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::metrics::IN_PROGRESS_SCAN;
use crate::read::range::{RangeBuilderList, RowGroupIndex};
use crate::read::scan_region::StreamContext;
use crate::read::{Batch, ScannerMetrics, Source};
use crate::sst::file::FileTimeRange;
use crate::sst::parquet::reader::ReaderMetrics;

struct PartitionMetricsInner {
    region_id: RegionId,
    /// Index of the partition to scan.
    partition: usize,
    /// Label to distinguish different scan operation.
    scanner_type: &'static str,
    /// Query start time.
    query_start: Instant,
    /// Elapsed time before the first poll operation.
    first_poll: Duration,
    metrics: ScannerMetrics,
    reader_metrics: ReaderMetrics,
    in_progress_scan: IntGauge,
}

impl PartitionMetricsInner {
    fn on_finish(&mut self) {
        if self.metrics.total_cost.is_zero() {
            self.metrics.total_cost = self.query_start.elapsed();
        }
        self.metrics.build_parts_cost = self.reader_metrics.build_cost;
    }
}

impl Drop for PartitionMetricsInner {
    fn drop(&mut self) {
        self.on_finish();
        self.metrics.observe_metrics();
        self.in_progress_scan.dec();

        debug!(
            "{} finished, region_id: {}, partition: {}, first_poll: {:?}, metrics: {:?}, reader_metrics: {:?}",
            self.scanner_type, self.region_id, self.partition, self.first_poll, self.metrics, self.reader_metrics
        );
    }
}

/// Metrics while reading a partition.
#[derive(Clone)]
pub(crate) struct PartitionMetrics(Arc<Mutex<PartitionMetricsInner>>);

impl PartitionMetrics {
    pub(crate) fn new(
        region_id: RegionId,
        partition: usize,
        scanner_type: &'static str,
        query_start: Instant,
        metrics: ScannerMetrics,
    ) -> Self {
        let partition_str = partition.to_string();
        let in_progress_scan = IN_PROGRESS_SCAN.with_label_values(&[scanner_type, &partition_str]);
        let inner = PartitionMetricsInner {
            region_id,
            partition,
            scanner_type,
            query_start,
            first_poll: Duration::default(),
            metrics,
            reader_metrics: ReaderMetrics::default(),
            in_progress_scan,
        };
        Self(Arc::new(Mutex::new(inner)))
    }

    pub(crate) fn on_first_poll(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.first_poll = inner.query_start.elapsed();
    }

    pub(crate) fn inc_num_mem_ranges(&self, num: usize) {
        let mut inner = self.0.lock().unwrap();
        inner.metrics.num_mem_ranges += num;
    }

    pub(crate) fn inc_num_file_ranges(&self, num: usize) {
        let mut inner = self.0.lock().unwrap();
        inner.metrics.num_file_ranges += num;
    }

    pub(crate) fn inc_build_reader_cost(&self, cost: Duration) {
        let mut inner = self.0.lock().unwrap();
        inner.metrics.build_reader_cost += cost;
    }

    pub(crate) fn merge_metrics(&self, metrics: &ScannerMetrics) {
        let mut inner = self.0.lock().unwrap();
        inner.metrics.merge_from(metrics);
    }

    pub(crate) fn merge_reader_metrics(&self, metrics: &ReaderMetrics) {
        let mut inner = self.0.lock().unwrap();
        inner.reader_metrics.merge_from(metrics);
    }

    pub(crate) fn on_finish(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.on_finish();
    }
}

/// Scans memtable ranges at `index`.
pub(crate) fn scan_mem_ranges(
    stream_ctx: Arc<StreamContext>,
    part_metrics: PartitionMetrics,
    index: RowGroupIndex,
    time_range: FileTimeRange,
    range_builder_list: Arc<RangeBuilderList>,
) -> impl Stream<Item = Result<Batch>> {
    try_stream! {
        let ranges = range_builder_list.build_mem_ranges(&stream_ctx.input, index);
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
