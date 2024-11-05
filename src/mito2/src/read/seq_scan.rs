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

//! Sequential scan.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::util::ChainedRecordBatchStream;
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::tracing;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{PartitionRange, RegionScanner, ScannerProperties};
use store_api::storage::TimeSeriesRowSelector;
use tokio::sync::Semaphore;

use crate::error::{PartitionOutOfRangeSnafu, Result};
use crate::read::dedup::{DedupReader, LastNonNull, LastRow};
use crate::read::last_row::LastRowReader;
use crate::read::merge::MergeReaderBuilder;
use crate::read::scan_region::{ScanInput, StreamContext};
use crate::read::scan_util::{scan_file_ranges, scan_mem_ranges, PartitionMetrics};
use crate::read::{BatchReader, BoxedBatchReader, ScannerMetrics, Source};
use crate::region::options::MergeMode;

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary keys, time index` inside every
/// [`PartitionRange`]. Each "partition" may contains many [`PartitionRange`]s.
pub struct SeqScan {
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Context of streams.
    stream_ctx: Arc<StreamContext>,
    /// Semaphore to control scan parallelism of files.
    /// Streams created by the scanner share the same semaphore.
    semaphore: Arc<Semaphore>,
    /// The scanner is used for compaction.
    compaction: bool,
}

impl SeqScan {
    /// Creates a new [SeqScan].
    pub(crate) fn new(input: ScanInput) -> Self {
        // TODO(yingwen): Set permits according to partition num. But we need to support file
        // level parallelism.
        let parallelism = input.parallelism.parallelism.max(1);
        let mut properties = ScannerProperties::default()
            .with_append_mode(input.append_mode)
            .with_total_rows(input.total_rows());
        let stream_ctx = Arc::new(StreamContext::seq_scan_ctx(input));
        properties.partitions = vec![stream_ctx.partition_ranges()];

        Self {
            properties,
            stream_ctx,
            semaphore: Arc::new(Semaphore::new(parallelism)),
            compaction: false,
        }
    }

    /// Sets the scanner to be used for compaction.
    pub(crate) fn with_compaction(mut self) -> Self {
        self.compaction = true;
        self
    }

    /// Builds a stream for the query.
    ///
    /// The returned stream is not partitioned and will contains all the data. If want
    /// partitioned scan, use [`RegionScanner::scan_partition`].
    pub fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let streams = (0..self.properties.partitions.len())
            .map(|partition: usize| self.scan_partition(partition))
            .collect::<Result<Vec<_>, _>>()?;

        let aggr_stream = ChainedRecordBatchStream::new(streams).map_err(BoxedError::new)?;
        Ok(Box::pin(aggr_stream))
    }

    /// Builds a [BoxedBatchReader] from sequential scan for compaction.
    pub async fn build_reader(&self) -> Result<BoxedBatchReader> {
        let part_metrics = PartitionMetrics::new(
            self.stream_ctx.input.mapper.metadata().region_id,
            0,
            get_scanner_type(self.compaction),
            self.stream_ctx.query_start,
            ScannerMetrics {
                prepare_scan_cost: self.stream_ctx.query_start.elapsed(),
                ..Default::default()
            },
        );
        debug_assert_eq!(1, self.properties.partitions.len());
        let partition_ranges = &self.properties.partitions[0];

        let reader = Self::build_all_merge_reader(
            &self.stream_ctx,
            partition_ranges,
            self.semaphore.clone(),
            self.compaction,
            &part_metrics,
        )
        .await?;
        Ok(Box::new(reader))
    }

    /// Builds a merge reader that reads all data.
    async fn build_all_merge_reader(
        stream_ctx: &Arc<StreamContext>,
        partition_ranges: &[PartitionRange],
        semaphore: Arc<Semaphore>,
        compaction: bool,
        part_metrics: &PartitionMetrics,
    ) -> Result<BoxedBatchReader> {
        let mut sources = Vec::new();
        for part_range in partition_ranges {
            build_sources(
                stream_ctx,
                part_range,
                compaction,
                part_metrics,
                &mut sources,
            );
        }
        Self::build_reader_from_sources(stream_ctx, sources, semaphore).await
    }

    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    async fn build_reader_from_sources(
        stream_ctx: &StreamContext,
        mut sources: Vec<Source>,
        semaphore: Arc<Semaphore>,
    ) -> Result<BoxedBatchReader> {
        if stream_ctx.input.parallelism.parallelism > 1 {
            // Read sources in parallel. We always spawn a task so we can control the parallelism
            // by the semaphore.
            sources = stream_ctx
                .input
                .create_parallel_sources(sources, semaphore.clone())?;
        }

        let mut builder = MergeReaderBuilder::from_sources(sources);
        let reader = builder.build().await?;

        let dedup = !stream_ctx.input.append_mode;
        let reader = if dedup {
            match stream_ctx.input.merge_mode {
                MergeMode::LastRow => Box::new(DedupReader::new(
                    reader,
                    LastRow::new(stream_ctx.input.filter_deleted),
                )) as _,
                MergeMode::LastNonNull => Box::new(DedupReader::new(
                    reader,
                    LastNonNull::new(stream_ctx.input.filter_deleted),
                )) as _,
            }
        } else {
            Box::new(reader) as _
        };

        let reader = match &stream_ctx.input.series_row_selector {
            Some(TimeSeriesRowSelector::LastRow) => Box::new(LastRowReader::new(reader)) as _,
            None => reader,
        };

        Ok(reader)
    }

    /// Scans the given partition when the part list is set properly.
    /// Otherwise the returned stream might not contains any data.
    fn scan_partition_impl(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        if partition >= self.properties.partitions.len() {
            return Err(BoxedError::new(
                PartitionOutOfRangeSnafu {
                    given: partition,
                    all: self.properties.partitions.len(),
                }
                .build(),
            ));
        }

        let stream_ctx = self.stream_ctx.clone();
        let semaphore = self.semaphore.clone();
        let partition_ranges = self.properties.partitions[partition].clone();
        let compaction = self.compaction;
        let distinguish_range = self.properties.distinguish_partition_range();
        let part_metrics = PartitionMetrics::new(
            self.stream_ctx.input.mapper.metadata().region_id,
            partition,
            get_scanner_type(self.compaction),
            stream_ctx.query_start,
            ScannerMetrics {
                prepare_scan_cost: self.stream_ctx.query_start.elapsed(),
                ..Default::default()
            },
        );

        let stream = try_stream! {
            part_metrics.on_first_poll();

            // Scans each part.
            for part_range in partition_ranges {
                let mut sources = Vec::new();
                build_sources(&stream_ctx, &part_range, compaction, &part_metrics, &mut sources);

                let mut reader =
                    Self::build_reader_from_sources(&stream_ctx, sources, semaphore.clone())
                        .await
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?;
                let cache = stream_ctx.input.cache_manager.as_deref();
                let mut metrics = ScannerMetrics::default();
                let mut fetch_start = Instant::now();
                #[cfg(debug_assertions)]
                let mut checker = crate::read::BatchChecker::default()
                    .with_start(Some(part_range.start))
                    .with_end(Some(part_range.end));

                while let Some(batch) = reader
                    .next_batch()
                    .await
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?
                {
                    metrics.scan_cost += fetch_start.elapsed();
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();

                    debug_assert!(!batch.is_empty());
                    if batch.is_empty() {
                        continue;
                    }

                    #[cfg(debug_assertions)]
                    checker.ensure_part_range_batch(
                        "SeqScan",
                        stream_ctx.input.mapper.metadata().region_id,
                        partition,
                        part_range,
                        &batch,
                    );

                    let convert_start = Instant::now();
                    let record_batch = stream_ctx.input.mapper.convert(&batch, cache)?;
                    metrics.convert_cost += convert_start.elapsed();
                    let yield_start = Instant::now();
                    yield record_batch;
                    metrics.yield_cost += yield_start.elapsed();

                    fetch_start = Instant::now();
                }

                // Yields an empty part to indicate this range is terminated.
                // The query engine can use this to optimize some queries.
                if distinguish_range {
                    let yield_start = Instant::now();
                    yield stream_ctx.input.mapper.empty_record_batch();
                    metrics.yield_cost += yield_start.elapsed();
                }

                metrics.scan_cost += fetch_start.elapsed();
                part_metrics.merge_metrics(&metrics);
            }

            part_metrics.on_finish();
        };

        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.stream_ctx.input.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }
}

impl RegionScanner for SeqScan {
    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.stream_ctx.input.mapper.output_schema()
    }

    fn scan_partition(&self, partition: usize) -> Result<SendableRecordBatchStream, BoxedError> {
        self.scan_partition_impl(partition)
    }

    fn prepare(
        &mut self,
        ranges: Vec<Vec<PartitionRange>>,
        distinguish_partition_range: bool,
    ) -> Result<(), BoxedError> {
        self.properties.partitions = ranges;
        self.properties.distinguish_partition_range = distinguish_partition_range;
        Ok(())
    }

    fn has_predicate(&self) -> bool {
        let predicate = self.stream_ctx.input.predicate();
        predicate.map(|p| !p.exprs().is_empty()).unwrap_or(false)
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.stream_ctx.input.mapper.metadata().clone()
    }
}

impl DisplayAs for SeqScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SeqScan: region={}, ",
            self.stream_ctx.input.mapper.metadata().region_id
        )?;
        self.stream_ctx.format_for_explain(f)
    }
}

impl fmt::Debug for SeqScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeqScan")
            .field("num_ranges", &self.stream_ctx.ranges.len())
            .finish()
    }
}

/// Builds sources for the partition range.
fn build_sources(
    stream_ctx: &Arc<StreamContext>,
    part_range: &PartitionRange,
    compaction: bool,
    part_metrics: &PartitionMetrics,
    sources: &mut Vec<Source>,
) {
    // Gets range meta.
    let range_meta = &stream_ctx.ranges[part_range.identifier];
    sources.reserve(range_meta.row_group_indices.len());
    for index in &range_meta.row_group_indices {
        let stream = if stream_ctx.is_mem_range_index(*index) {
            let stream = scan_mem_ranges(
                part_metrics.partition(),
                stream_ctx.clone(),
                part_metrics.clone(),
                *index,
                range_meta.time_range,
            );
            Box::pin(stream) as _
        } else {
            let read_type = if compaction {
                "compaction"
            } else {
                "seq_scan_files"
            };
            let stream = scan_file_ranges(
                part_metrics.partition(),
                stream_ctx.clone(),
                part_metrics.clone(),
                *index,
                read_type,
            );
            Box::pin(stream) as _
        };
        sources.push(Source::Stream(stream));
    }
}

#[cfg(test)]
impl SeqScan {
    /// Returns the input.
    pub(crate) fn input(&self) -> &ScanInput {
        &self.stream_ctx.input
    }
}

/// Returns the scanner type.
fn get_scanner_type(compaction: bool) -> &'static str {
    if compaction {
        "SeqScan(compaction)"
    } else {
        "SeqScan"
    }
}
