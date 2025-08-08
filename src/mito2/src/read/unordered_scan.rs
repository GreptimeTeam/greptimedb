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

//! Unordered scanner.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_stream::{stream, try_stream};
use common_error::ext::BoxedError;
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use futures::{Stream, StreamExt};
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
};

use crate::error::{PartitionOutOfRangeSnafu, Result};
use crate::read::range::RangeBuilderList;
use crate::read::scan_region::{ScanInput, StreamContext};
use crate::read::scan_util::{
    scan_file_ranges, scan_mem_ranges, PartitionMetrics, PartitionMetricsList,
};
use crate::read::stream::{ConvertBatchStream, ScanBatch, ScanBatchStream};
use crate::read::{scan_util, Batch, ScannerMetrics};

/// Scans a region without providing any output ordering guarantee.
///
/// Only an append only table should use this scanner.
pub struct UnorderedScan {
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Context of streams.
    stream_ctx: Arc<StreamContext>,
    /// Metrics for each partition.
    metrics_list: PartitionMetricsList,
}

impl UnorderedScan {
    /// Creates a new [UnorderedScan].
    pub(crate) fn new(input: ScanInput) -> Self {
        let mut properties = ScannerProperties::default()
            .with_append_mode(input.append_mode)
            .with_total_rows(input.total_rows());
        let stream_ctx = Arc::new(StreamContext::unordered_scan_ctx(input));
        properties.partitions = vec![stream_ctx.partition_ranges()];

        Self {
            properties,
            stream_ctx,
            metrics_list: PartitionMetricsList::default(),
        }
    }

    /// Scans the region and returns a stream.
    pub(crate) async fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let metrics_set = ExecutionPlanMetricsSet::new();
        let part_num = self.properties.num_partitions();
        let streams = (0..part_num)
            .map(|i| self.scan_partition(&QueryScanContext::default(), &metrics_set, i))
            .collect::<Result<Vec<_>, BoxedError>>()?;
        let stream = stream! {
            for mut stream in streams {
                while let Some(rb) = stream.next().await {
                    yield rb;
                }
            }
        };
        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.schema(),
            Box::pin(stream),
        ));
        Ok(stream)
    }

    /// Scans a [PartitionRange] by its `identifier` and returns a stream.
    fn scan_partition_range(
        stream_ctx: Arc<StreamContext>,
        part_range_id: usize,
        part_metrics: PartitionMetrics,
        range_builder_list: Arc<RangeBuilderList>,
    ) -> impl Stream<Item = Result<Batch>> {
        try_stream! {
            // Gets range meta.
            let range_meta = &stream_ctx.ranges[part_range_id];
            for index in &range_meta.row_group_indices {
                if stream_ctx.is_mem_range_index(*index) {
                    let stream = scan_mem_ranges(
                        stream_ctx.clone(),
                        part_metrics.clone(),
                        *index,
                        range_meta.time_range,
                    );
                    for await batch in stream {
                        yield batch?;
                    }
                } else if stream_ctx.is_file_range_index(*index) {
                    let stream = scan_file_ranges(
                        stream_ctx.clone(),
                        part_metrics.clone(),
                        *index,
                        "unordered_scan_files",
                        range_builder_list.clone(),
                    ).await?;
                    for await batch in stream {
                        yield batch?;
                    }
                } else {
                    let stream = scan_util::maybe_scan_other_ranges(
                        &stream_ctx,
                        *index,
                        &part_metrics,
                    ).await?;
                    for await batch in stream {
                        yield batch?;
                    }
                }
            }
        }
    }

    /// Scan [`Batch`] in all partitions one by one.
    pub(crate) fn scan_all_partitions(&self) -> Result<ScanBatchStream> {
        let metrics_set = ExecutionPlanMetricsSet::new();

        let streams = (0..self.properties.partitions.len())
            .map(|partition| {
                let metrics = self.partition_metrics(false, partition, &metrics_set);
                self.scan_batch_in_partition(partition, metrics)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::pin(futures::stream::iter(streams).flatten()))
    }

    fn partition_metrics(
        &self,
        explain_verbose: bool,
        partition: usize,
        metrics_set: &ExecutionPlanMetricsSet,
    ) -> PartitionMetrics {
        let part_metrics = PartitionMetrics::new(
            self.stream_ctx.input.mapper.metadata().region_id,
            partition,
            "UnorderedScan",
            self.stream_ctx.query_start,
            explain_verbose,
            metrics_set,
        );
        self.metrics_list.set(partition, part_metrics.clone());
        part_metrics
    }

    fn scan_partition_impl(
        &self,
        ctx: &QueryScanContext,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        if ctx.explain_verbose {
            common_telemetry::info!(
                "UnorderedScan partition {}, region_id: {}",
                partition,
                self.stream_ctx.input.region_metadata().region_id
            );
        }

        let metrics = self.partition_metrics(ctx.explain_verbose, partition, metrics_set);

        let batch_stream = self.scan_batch_in_partition(partition, metrics.clone())?;

        let input = &self.stream_ctx.input;
        let record_batch_stream = ConvertBatchStream::new(
            batch_stream,
            input.mapper.clone(),
            input.cache_strategy.clone(),
            metrics,
        );

        Ok(Box::pin(RecordBatchStreamWrapper::new(
            input.mapper.output_schema(),
            Box::pin(record_batch_stream),
        )))
    }

    fn scan_batch_in_partition(
        &self,
        partition: usize,
        part_metrics: PartitionMetrics,
    ) -> Result<ScanBatchStream> {
        ensure!(
            partition < self.properties.partitions.len(),
            PartitionOutOfRangeSnafu {
                given: partition,
                all: self.properties.partitions.len(),
            }
        );

        let stream_ctx = self.stream_ctx.clone();
        let part_ranges = self.properties.partitions[partition].clone();
        let distinguish_range = self.properties.distinguish_partition_range;

        let stream = try_stream! {
            part_metrics.on_first_poll();

            let range_builder_list = Arc::new(RangeBuilderList::new(
                stream_ctx.input.num_memtables(),
                stream_ctx.input.num_files(),
            ));
            // Scans each part.
            for part_range in part_ranges {
                let mut metrics = ScannerMetrics::default();
                let mut fetch_start = Instant::now();
                #[cfg(debug_assertions)]
                let mut checker = crate::read::BatchChecker::default()
                    .with_start(Some(part_range.start))
                    .with_end(Some(part_range.end));

                let stream = Self::scan_partition_range(
                    stream_ctx.clone(),
                    part_range.identifier,
                    part_metrics.clone(),
                    range_builder_list.clone(),
                );
                for await batch in stream {
                    let batch = batch?;
                    metrics.scan_cost += fetch_start.elapsed();
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();

                    debug_assert!(!batch.is_empty());
                    if batch.is_empty() {
                        continue;
                    }

                    #[cfg(debug_assertions)]
                    checker.ensure_part_range_batch(
                        "UnorderedScan",
                        stream_ctx.input.mapper.metadata().region_id,
                        partition,
                        part_range,
                        &batch,
                    );

                    let yield_start = Instant::now();
                    yield ScanBatch::Normal(batch);
                    metrics.yield_cost += yield_start.elapsed();

                    fetch_start = Instant::now();
                }

                // Yields an empty part to indicate this range is terminated.
                // The query engine can use this to optimize some queries.
                if distinguish_range {
                    let yield_start = Instant::now();
                    yield ScanBatch::Normal(Batch::empty());
                    metrics.yield_cost += yield_start.elapsed();
                }

                metrics.scan_cost += fetch_start.elapsed();
                part_metrics.merge_metrics(&metrics);
            }

            part_metrics.on_finish();
        };
        Ok(Box::pin(stream))
    }
}

impl RegionScanner for UnorderedScan {
    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.stream_ctx.input.mapper.output_schema()
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.stream_ctx.input.mapper.metadata().clone()
    }

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.properties.prepare(request);
        // UnorderedScan only scans one row group per partition so the resource requirement won't be too high.
        Ok(())
    }

    fn scan_partition(
        &self,
        ctx: &QueryScanContext,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        self.scan_partition_impl(ctx, metrics_set, partition)
            .map_err(BoxedError::new)
    }

    fn has_predicate(&self) -> bool {
        let predicate = self.stream_ctx.input.predicate();
        predicate.map(|p| !p.exprs().is_empty()).unwrap_or(false)
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }
}

impl DisplayAs for UnorderedScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "UnorderedScan: region={}, ",
            self.stream_ctx.input.mapper.metadata().region_id
        )?;
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                self.stream_ctx.format_for_explain(false, f)
            }
            DisplayFormatType::Verbose => {
                self.stream_ctx.format_for_explain(true, f)?;
                self.metrics_list.format_verbose_metrics(f)
            }
        }
    }
}

impl fmt::Debug for UnorderedScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnorderedScan")
            .field("num_ranges", &self.stream_ctx.ranges.len())
            .finish()
    }
}

#[cfg(test)]
impl UnorderedScan {
    /// Returns the input.
    pub(crate) fn input(&self) -> &ScanInput {
        &self.stream_ctx.input
    }
}
