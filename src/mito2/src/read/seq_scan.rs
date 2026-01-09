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
use common_recordbatch::util::ChainedRecordBatchStream;
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::{tracing, warn};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use snafu::{OptionExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    PartitionRange, PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
};
use store_api::storage::TimeSeriesRowSelector;
use tokio::sync::Semaphore;

use crate::error::{PartitionOutOfRangeSnafu, Result, TooManyFilesToReadSnafu, UnexpectedSnafu};
use crate::read::dedup::{DedupReader, LastNonNull, LastRow};
use crate::read::flat_dedup::{FlatDedupReader, FlatLastNonNull, FlatLastRow};
use crate::read::flat_merge::FlatMergeReader;
use crate::read::last_row::LastRowReader;
use crate::read::merge::MergeReaderBuilder;
use crate::read::pruner::{PartitionPruner, Pruner};
use crate::read::range::RangeMeta;
use crate::read::scan_region::{ScanInput, StreamContext};
use crate::read::scan_util::{
    PartitionMetrics, PartitionMetricsList, SplitRecordBatchStream, scan_file_ranges,
    scan_flat_file_ranges, scan_flat_mem_ranges, scan_mem_ranges,
    should_split_flat_batches_for_merge,
};
use crate::read::stream::{ConvertBatchStream, ScanBatch, ScanBatchStream};
use crate::read::{
    Batch, BatchReader, BoxedBatchReader, BoxedRecordBatchStream, ScannerMetrics, Source, scan_util,
};
use crate::region::options::MergeMode;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary keys, time index` inside every
/// [`PartitionRange`]. Each "partition" may contains many [`PartitionRange`]s.
pub struct SeqScan {
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Context of streams.
    stream_ctx: Arc<StreamContext>,
    /// Shared pruner for file range building.
    pruner: Arc<Pruner>,
    /// Metrics for each partition.
    /// The scanner only sets in query and keeps it empty during compaction.
    metrics_list: PartitionMetricsList,
}

impl SeqScan {
    /// Creates a new [SeqScan] with the given input.
    /// If `input.compaction` is true, the scanner will not attempt to split ranges.
    pub(crate) fn new(input: ScanInput) -> Self {
        let mut properties = ScannerProperties::default()
            .with_append_mode(input.append_mode)
            .with_total_rows(input.total_rows());
        let stream_ctx = Arc::new(StreamContext::seq_scan_ctx(input));
        properties.partitions = vec![stream_ctx.partition_ranges()];

        // Create the shared pruner with number of workers equal to CPU cores.
        let num_workers = common_stat::get_total_cpu_cores().max(1);
        let pruner = Arc::new(Pruner::new(stream_ctx.clone(), num_workers));

        Self {
            properties,
            stream_ctx,
            pruner,
            metrics_list: PartitionMetricsList::default(),
        }
    }

    /// Builds a stream for the query.
    ///
    /// The returned stream is not partitioned and will contains all the data. If want
    /// partitioned scan, use [`RegionScanner::scan_partition`].
    #[tracing::instrument(
        skip_all,
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
    pub fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let metrics_set = ExecutionPlanMetricsSet::new();
        let streams = (0..self.properties.partitions.len())
            .map(|partition: usize| {
                self.scan_partition(&QueryScanContext::default(), &metrics_set, partition)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let aggr_stream = ChainedRecordBatchStream::new(streams).map_err(BoxedError::new)?;
        Ok(Box::pin(aggr_stream))
    }

    /// Scan [`Batch`] in all partitions one by one.
    pub(crate) fn scan_all_partitions(&self) -> Result<ScanBatchStream> {
        let metrics_set = ExecutionPlanMetricsSet::new();

        let streams = (0..self.properties.partitions.len())
            .map(|partition| {
                let metrics = self.new_partition_metrics(false, &metrics_set, partition);
                self.scan_batch_in_partition(partition, metrics)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::pin(futures::stream::iter(streams).flatten()))
    }

    /// Builds a [BoxedBatchReader] from sequential scan for compaction.
    ///
    /// # Panics
    /// Panics if the compaction flag is not set.
    pub async fn build_reader_for_compaction(&self) -> Result<BoxedBatchReader> {
        assert!(self.stream_ctx.input.compaction);

        let metrics_set = ExecutionPlanMetricsSet::new();
        let part_metrics = self.new_partition_metrics(false, &metrics_set, 0);
        debug_assert_eq!(1, self.properties.partitions.len());
        let partition_ranges = &self.properties.partitions[0];

        let reader = Self::merge_all_ranges_for_compaction(
            &self.stream_ctx,
            partition_ranges,
            &part_metrics,
            self.pruner.clone(),
        )
        .await?;
        Ok(Box::new(reader))
    }

    /// Builds a [BoxedRecordBatchStream] from sequential scan for flat format compaction.
    ///
    /// # Panics
    /// Panics if the compaction flag is not set.
    pub async fn build_flat_reader_for_compaction(&self) -> Result<BoxedRecordBatchStream> {
        assert!(self.stream_ctx.input.compaction);

        let metrics_set = ExecutionPlanMetricsSet::new();
        let part_metrics = self.new_partition_metrics(false, &metrics_set, 0);
        debug_assert_eq!(1, self.properties.partitions.len());
        let partition_ranges = &self.properties.partitions[0];

        let reader = Self::merge_all_flat_ranges_for_compaction(
            &self.stream_ctx,
            partition_ranges,
            &part_metrics,
            self.pruner.clone(),
        )
        .await?;
        Ok(reader)
    }

    /// Builds a merge reader that reads all ranges.
    /// Callers MUST not split ranges before calling this method.
    async fn merge_all_ranges_for_compaction(
        stream_ctx: &Arc<StreamContext>,
        partition_ranges: &[PartitionRange],
        part_metrics: &PartitionMetrics,
        pruner: Arc<Pruner>,
    ) -> Result<BoxedBatchReader> {
        pruner.add_partition_ranges(partition_ranges);
        let partition_pruner = Arc::new(PartitionPruner::new(pruner, partition_ranges));

        let mut sources = Vec::new();
        for part_range in partition_ranges {
            build_sources(
                stream_ctx,
                part_range,
                true,
                part_metrics,
                partition_pruner.clone(),
                &mut sources,
                None,
            )
            .await?;
        }

        common_telemetry::debug!(
            "Build reader to read all parts, region_id: {}, num_part_ranges: {}, num_sources: {}",
            stream_ctx.input.mapper.metadata().region_id,
            partition_ranges.len(),
            sources.len()
        );
        Self::build_reader_from_sources(stream_ctx, sources, None, None).await
    }

    /// Builds a merge reader that reads all flat ranges.
    /// Callers MUST not split ranges before calling this method.
    async fn merge_all_flat_ranges_for_compaction(
        stream_ctx: &Arc<StreamContext>,
        partition_ranges: &[PartitionRange],
        part_metrics: &PartitionMetrics,
        pruner: Arc<Pruner>,
    ) -> Result<BoxedRecordBatchStream> {
        pruner.add_partition_ranges(partition_ranges);
        let partition_pruner = Arc::new(PartitionPruner::new(pruner, partition_ranges));

        let mut sources = Vec::new();
        for part_range in partition_ranges {
            build_flat_sources(
                stream_ctx,
                part_range,
                true,
                part_metrics,
                partition_pruner.clone(),
                &mut sources,
                None,
            )
            .await?;
        }

        common_telemetry::debug!(
            "Build flat reader to read all parts, region_id: {}, num_part_ranges: {}, num_sources: {}",
            stream_ctx.input.mapper.metadata().region_id,
            partition_ranges.len(),
            sources.len()
        );
        Self::build_flat_reader_from_sources(stream_ctx, sources, None, None).await
    }

    /// Builds a reader to read sources. If `semaphore` is provided, reads sources in parallel
    /// if possible.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub(crate) async fn build_reader_from_sources(
        stream_ctx: &StreamContext,
        mut sources: Vec<Source>,
        semaphore: Option<Arc<Semaphore>>,
        part_metrics: Option<&PartitionMetrics>,
    ) -> Result<BoxedBatchReader> {
        if let Some(semaphore) = semaphore.as_ref() {
            // Read sources in parallel.
            if sources.len() > 1 {
                sources = stream_ctx
                    .input
                    .create_parallel_sources(sources, semaphore.clone())?;
            }
        }

        let mut builder = MergeReaderBuilder::from_sources(sources);
        if let Some(metrics) = part_metrics {
            builder.with_metrics_reporter(Some(metrics.merge_metrics_reporter()));
        }
        let reader = builder.build().await?;

        let dedup = !stream_ctx.input.append_mode;
        let dedup_metrics_reporter = part_metrics.map(|m| m.dedup_metrics_reporter());
        let reader = if dedup {
            match stream_ctx.input.merge_mode {
                MergeMode::LastRow => Box::new(DedupReader::new(
                    reader,
                    LastRow::new(stream_ctx.input.filter_deleted),
                    dedup_metrics_reporter,
                )) as _,
                MergeMode::LastNonNull => Box::new(DedupReader::new(
                    reader,
                    LastNonNull::new(stream_ctx.input.filter_deleted),
                    dedup_metrics_reporter,
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

    /// Builds a flat reader to read sources that returns RecordBatch. If `semaphore` is provided, reads sources in parallel
    /// if possible.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub(crate) async fn build_flat_reader_from_sources(
        stream_ctx: &StreamContext,
        mut sources: Vec<BoxedRecordBatchStream>,
        semaphore: Option<Arc<Semaphore>>,
        part_metrics: Option<&PartitionMetrics>,
    ) -> Result<BoxedRecordBatchStream> {
        if let Some(semaphore) = semaphore.as_ref() {
            // Read sources in parallel.
            if sources.len() > 1 {
                sources = stream_ctx
                    .input
                    .create_parallel_flat_sources(sources, semaphore.clone())?;
            }
        }

        let mapper = stream_ctx.input.mapper.as_flat().unwrap();
        let schema = mapper.input_arrow_schema(stream_ctx.input.compaction);

        let metrics_reporter = part_metrics.map(|m| m.merge_metrics_reporter());
        let reader =
            FlatMergeReader::new(schema, sources, DEFAULT_READ_BATCH_SIZE, metrics_reporter)
                .await?;

        let dedup = !stream_ctx.input.append_mode;
        let dedup_metrics_reporter = part_metrics.map(|m| m.dedup_metrics_reporter());
        let reader = if dedup {
            match stream_ctx.input.merge_mode {
                MergeMode::LastRow => Box::pin(
                    FlatDedupReader::new(
                        reader.into_stream().boxed(),
                        FlatLastRow::new(stream_ctx.input.filter_deleted),
                        dedup_metrics_reporter,
                    )
                    .into_stream(),
                ) as _,
                MergeMode::LastNonNull => Box::pin(
                    FlatDedupReader::new(
                        reader.into_stream().boxed(),
                        FlatLastNonNull::new(
                            mapper.field_column_start(),
                            stream_ctx.input.filter_deleted,
                        ),
                        dedup_metrics_reporter,
                    )
                    .into_stream(),
                ) as _,
            }
        } else {
            Box::pin(reader.into_stream()) as _
        };

        Ok(reader)
    }

    /// Scans the given partition when the part list is set properly.
    /// Otherwise the returned stream might not contains any data.
    fn scan_partition_impl(
        &self,
        ctx: &QueryScanContext,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        if ctx.explain_verbose {
            common_telemetry::info!(
                "SeqScan partition {}, region_id: {}",
                partition,
                self.stream_ctx.input.region_metadata().region_id
            );
        }

        let metrics = self.new_partition_metrics(ctx.explain_verbose, metrics_set, partition);
        let input = &self.stream_ctx.input;

        let batch_stream = if input.flat_format {
            // Use flat scan for bulk memtables
            self.scan_flat_batch_in_partition(partition, metrics.clone())?
        } else {
            // Use regular batch scan for normal memtables
            self.scan_batch_in_partition(partition, metrics.clone())?
        };
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

    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.stream_ctx.input.mapper.metadata().region_id,
            partition = partition
        )
    )]
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

        if self.properties.partitions[partition].is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let stream_ctx = self.stream_ctx.clone();
        let semaphore = self.new_semaphore();
        let partition_ranges = self.properties.partitions[partition].clone();
        let compaction = self.stream_ctx.input.compaction;
        let distinguish_range = self.properties.distinguish_partition_range;
        let file_scan_semaphore = if compaction { None } else { semaphore.clone() };
        let pruner = self.pruner.clone();
        // Initializes ref counts for the pruner.
        // If we call scan_batch_in_partition() multiple times but don't read all batches from the stream,
        // then the ref count won't be decremented.
        // This is a rare case and keeping all remaining entries still uses less memory than a per partition cache.
        pruner.add_partition_ranges(&partition_ranges);
        let partition_pruner = Arc::new(PartitionPruner::new(pruner, &partition_ranges));

        let stream = try_stream! {
            part_metrics.on_first_poll();
            // Start fetch time before building sources so scan cost contains
            // build part cost.
            let mut fetch_start = Instant::now();

            let _mapper = stream_ctx.input.mapper.as_primary_key().context(UnexpectedSnafu {
                reason: "Unexpected format",
            })?;
            // Scans each part.
            for part_range in partition_ranges {
                let mut sources = Vec::new();
                build_sources(
                    &stream_ctx,
                    &part_range,
                    compaction,
                    &part_metrics,
                    partition_pruner.clone(),
                    &mut sources,
                    file_scan_semaphore.clone(),
                ).await?;

                let mut reader =
                    Self::build_reader_from_sources(&stream_ctx, sources, semaphore.clone(), Some(&part_metrics))
                        .await?;
                #[cfg(debug_assertions)]
                let mut checker = crate::read::BatchChecker::default()
                    .with_start(Some(part_range.start))
                    .with_end(Some(part_range.end));

                let mut metrics = ScannerMetrics {
                    scan_cost: fetch_start.elapsed(),
                    ..Default::default()
                };
                fetch_start = Instant::now();

                while let Some(batch) = reader.next_batch().await? {
                    metrics.scan_cost += fetch_start.elapsed();
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();

                    debug_assert!(!batch.is_empty());
                    if batch.is_empty() {
                        fetch_start = Instant::now();
                        continue;
                    }

                    #[cfg(debug_assertions)]
                    checker.ensure_part_range_batch(
                        "SeqScan",
                        _mapper.metadata().region_id,
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
                fetch_start = Instant::now();
                part_metrics.merge_metrics(&metrics);
            }

            part_metrics.on_finish();
        };
        Ok(Box::pin(stream))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.stream_ctx.input.mapper.metadata().region_id,
            partition = partition
        )
    )]
    fn scan_flat_batch_in_partition(
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

        if self.properties.partitions[partition].is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let stream_ctx = self.stream_ctx.clone();
        let semaphore = self.new_semaphore();
        let partition_ranges = self.properties.partitions[partition].clone();
        let compaction = self.stream_ctx.input.compaction;
        let file_scan_semaphore = if compaction { None } else { semaphore.clone() };
        let pruner = self.pruner.clone();
        // Initializes ref counts for the pruner.
        // If we call scan_batch_in_partition() multiple times but don't read all batches from the stream,
        // then the ref count won't be decremented.
        // This is a rare case and keeping all remaining entries still uses less memory than a per partition cache.
        pruner.add_partition_ranges(&partition_ranges);
        let partition_pruner = Arc::new(PartitionPruner::new(pruner, &partition_ranges));

        let stream = try_stream! {
            part_metrics.on_first_poll();
            // Start fetch time before building sources so scan cost contains
            // build part cost.
            let mut fetch_start = Instant::now();

            // Scans each part.
            for part_range in partition_ranges {
                let mut sources = Vec::new();
                build_flat_sources(
                    &stream_ctx,
                    &part_range,
                    compaction,
                    &part_metrics,
                    partition_pruner.clone(),
                    &mut sources,
                    file_scan_semaphore.clone(),
                ).await?;

                let mut reader =
                    Self::build_flat_reader_from_sources(&stream_ctx, sources, semaphore.clone(), Some(&part_metrics))
                        .await?;

                let mut metrics = ScannerMetrics {
                    scan_cost: fetch_start.elapsed(),
                    ..Default::default()
                };
                fetch_start = Instant::now();

                while let Some(record_batch) = reader.try_next().await? {
                    metrics.scan_cost += fetch_start.elapsed();
                    metrics.num_batches += 1;
                    metrics.num_rows += record_batch.num_rows();

                    debug_assert!(record_batch.num_rows() > 0);
                    if record_batch.num_rows() == 0 {
                        fetch_start = Instant::now();
                        continue;
                    }

                    let yield_start = Instant::now();
                    yield ScanBatch::RecordBatch(record_batch);
                    metrics.yield_cost += yield_start.elapsed();

                    fetch_start = Instant::now();
                }

                metrics.scan_cost += fetch_start.elapsed();
                fetch_start = Instant::now();
                part_metrics.merge_metrics(&metrics);
            }

            part_metrics.on_finish();
        };
        Ok(Box::pin(stream))
    }

    fn new_semaphore(&self) -> Option<Arc<Semaphore>> {
        if self.properties.target_partitions() > self.properties.num_partitions() {
            // We can use additional tasks to read the data if we have more target partitions than actual partitions.
            // This semaphore is partition level.
            // We don't use a global semaphore to avoid a partition waiting for others. The final concurrency
            // of tasks usually won't exceed the target partitions a lot as compaction can reduce the number of
            // files in a part range.
            Some(Arc::new(Semaphore::new(
                self.properties.target_partitions() - self.properties.num_partitions() + 1,
            )))
        } else {
            None
        }
    }

    /// Creates a new partition metrics instance.
    /// Sets the partition metrics for the given partition if it is not for compaction.
    fn new_partition_metrics(
        &self,
        explain_verbose: bool,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> PartitionMetrics {
        let metrics = PartitionMetrics::new(
            self.stream_ctx.input.mapper.metadata().region_id,
            partition,
            get_scanner_type(self.stream_ctx.input.compaction),
            self.stream_ctx.query_start,
            explain_verbose,
            metrics_set,
        );

        if !self.stream_ctx.input.compaction {
            self.metrics_list.set(partition, metrics.clone());
        }

        metrics
    }

    /// Finds the maximum number of files to read in a single partition range.
    fn max_files_in_partition(ranges: &[RangeMeta], partition_ranges: &[PartitionRange]) -> usize {
        partition_ranges
            .iter()
            .map(|part_range| {
                let range_meta = &ranges[part_range.identifier];
                range_meta.indices.len()
            })
            .max()
            .unwrap_or(0)
    }

    /// Checks resource limit for the scanner.
    pub(crate) fn check_scan_limit(&self) -> Result<()> {
        // Check max file count limit for all partitions since we scan them in parallel.
        let total_max_files: usize = self
            .properties
            .partitions
            .iter()
            .map(|partition| Self::max_files_in_partition(&self.stream_ctx.ranges, partition))
            .sum();

        let max_concurrent_files = self.stream_ctx.input.max_concurrent_scan_files;
        if total_max_files > max_concurrent_files {
            return TooManyFilesToReadSnafu {
                actual: total_max_files,
                max: max_concurrent_files,
            }
            .fail();
        }

        Ok(())
    }
}

impl RegionScanner for SeqScan {
    fn name(&self) -> &str {
        "SeqScan"
    }

    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.stream_ctx.input.mapper.output_schema()
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.stream_ctx.input.mapper.metadata().clone()
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

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.properties.prepare(request);

        self.check_scan_limit().map_err(BoxedError::new)?;

        Ok(())
    }

    fn has_predicate_without_region(&self) -> bool {
        let predicate = self
            .stream_ctx
            .input
            .predicate_group()
            .predicate_without_region();
        predicate.map(|p| !p.exprs().is_empty()).unwrap_or(false)
    }

    fn update_predicate_with_dyn_filter(
        &mut self,
        filter_exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    ) -> Vec<bool> {
        self.stream_ctx
            .update_predicate_with_dyn_filter(filter_exprs)
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }
}

impl DisplayAs for SeqScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SeqScan: region={}, ",
            self.stream_ctx.input.mapper.metadata().region_id
        )?;
        match t {
            // TODO(LFC): Implement all the "TreeRender" display format.
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

impl fmt::Debug for SeqScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeqScan")
            .field("num_ranges", &self.stream_ctx.ranges.len())
            .finish()
    }
}

/// Builds sources for the partition range and push them to the `sources` vector.
pub(crate) async fn build_sources(
    stream_ctx: &Arc<StreamContext>,
    part_range: &PartitionRange,
    compaction: bool,
    part_metrics: &PartitionMetrics,
    partition_pruner: Arc<PartitionPruner>,
    sources: &mut Vec<Source>,
    semaphore: Option<Arc<Semaphore>>,
) -> Result<()> {
    // Gets range meta.
    let range_meta = &stream_ctx.ranges[part_range.identifier];
    #[cfg(debug_assertions)]
    if compaction {
        // Compaction expects input sources are not been split.
        debug_assert_eq!(range_meta.indices.len(), range_meta.row_group_indices.len());
        for (i, row_group_idx) in range_meta.row_group_indices.iter().enumerate() {
            // It should scan all row groups.
            debug_assert_eq!(
                -1, row_group_idx.row_group_index,
                "Expect {} range scan all row groups, given: {}",
                i, row_group_idx.row_group_index,
            );
        }
    }

    let read_type = if compaction {
        "compaction"
    } else {
        "seq_scan_files"
    };
    let num_indices = range_meta.row_group_indices.len();
    if num_indices == 0 {
        return Ok(());
    }

    sources.reserve(num_indices);
    let mut ordered_sources = Vec::with_capacity(num_indices);
    ordered_sources.resize_with(num_indices, || None);
    let mut file_scan_tasks = Vec::new();

    for (position, index) in range_meta.row_group_indices.iter().enumerate() {
        if stream_ctx.is_mem_range_index(*index) {
            let stream = scan_mem_ranges(
                stream_ctx.clone(),
                part_metrics.clone(),
                *index,
                range_meta.time_range,
            );
            ordered_sources[position] = Some(Source::Stream(Box::pin(stream) as _));
        } else if stream_ctx.is_file_range_index(*index) {
            if let Some(semaphore_ref) = semaphore.as_ref() {
                // run in parallel, controlled by semaphore
                let stream_ctx = stream_ctx.clone();
                let part_metrics = part_metrics.clone();
                let partition_pruner = partition_pruner.clone();
                let semaphore = Arc::clone(semaphore_ref);
                let row_group_index = *index;
                file_scan_tasks.push(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let stream = scan_file_ranges(
                        stream_ctx,
                        part_metrics,
                        row_group_index,
                        read_type,
                        partition_pruner,
                    )
                    .await?;
                    Ok((position, Source::Stream(Box::pin(stream) as _)))
                });
            } else {
                // no semaphore, run sequentially
                let stream = scan_file_ranges(
                    stream_ctx.clone(),
                    part_metrics.clone(),
                    *index,
                    read_type,
                    partition_pruner.clone(),
                )
                .await?;
                ordered_sources[position] = Some(Source::Stream(Box::pin(stream) as _));
            }
        } else {
            let stream =
                scan_util::maybe_scan_other_ranges(stream_ctx, *index, part_metrics).await?;
            ordered_sources[position] = Some(Source::Stream(stream));
        }
    }

    if !file_scan_tasks.is_empty() {
        let results = futures::future::try_join_all(file_scan_tasks).await?;
        for (position, source) in results {
            ordered_sources[position] = Some(source);
        }
    }

    for source in ordered_sources.into_iter().flatten() {
        sources.push(source);
    }
    Ok(())
}

/// Builds flat sources for the partition range and push them to the `sources` vector.
pub(crate) async fn build_flat_sources(
    stream_ctx: &Arc<StreamContext>,
    part_range: &PartitionRange,
    compaction: bool,
    part_metrics: &PartitionMetrics,
    partition_pruner: Arc<PartitionPruner>,
    sources: &mut Vec<BoxedRecordBatchStream>,
    semaphore: Option<Arc<Semaphore>>,
) -> Result<()> {
    // Gets range meta.
    let range_meta = &stream_ctx.ranges[part_range.identifier];
    #[cfg(debug_assertions)]
    if compaction {
        // Compaction expects input sources are not been split.
        debug_assert_eq!(range_meta.indices.len(), range_meta.row_group_indices.len());
        for (i, row_group_idx) in range_meta.row_group_indices.iter().enumerate() {
            // It should scan all row groups.
            debug_assert_eq!(
                -1, row_group_idx.row_group_index,
                "Expect {} range scan all row groups, given: {}",
                i, row_group_idx.row_group_index,
            );
        }
    }

    let read_type = if compaction {
        "compaction"
    } else {
        "seq_scan_files"
    };
    let num_indices = range_meta.row_group_indices.len();
    if num_indices == 0 {
        return Ok(());
    }

    let should_split = should_split_flat_batches_for_merge(stream_ctx, range_meta);
    sources.reserve(num_indices);
    let mut ordered_sources = Vec::with_capacity(num_indices);
    ordered_sources.resize_with(num_indices, || None);
    let mut file_scan_tasks = Vec::new();

    for (position, index) in range_meta.row_group_indices.iter().enumerate() {
        if stream_ctx.is_mem_range_index(*index) {
            let stream = scan_flat_mem_ranges(stream_ctx.clone(), part_metrics.clone(), *index);
            ordered_sources[position] = Some(Box::pin(stream) as _);
        } else if stream_ctx.is_file_range_index(*index) {
            if let Some(semaphore_ref) = semaphore.as_ref() {
                // run in parallel, controlled by semaphore
                let stream_ctx = stream_ctx.clone();
                let part_metrics = part_metrics.clone();
                let partition_pruner = partition_pruner.clone();
                let semaphore = Arc::clone(semaphore_ref);
                let row_group_index = *index;
                file_scan_tasks.push(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let stream = scan_flat_file_ranges(
                        stream_ctx,
                        part_metrics,
                        row_group_index,
                        read_type,
                        partition_pruner,
                    )
                    .await?;
                    Ok((position, Box::pin(stream) as _))
                });
            } else {
                // no semaphore, run sequentially
                let stream = scan_flat_file_ranges(
                    stream_ctx.clone(),
                    part_metrics.clone(),
                    *index,
                    read_type,
                    partition_pruner.clone(),
                )
                .await?;
                ordered_sources[position] = Some(Box::pin(stream) as _);
            }
        } else {
            let stream =
                scan_util::maybe_scan_flat_other_ranges(stream_ctx, *index, part_metrics).await?;
            ordered_sources[position] = Some(stream);
        }
    }

    if !file_scan_tasks.is_empty() {
        let results = futures::future::try_join_all(file_scan_tasks).await?;
        for (position, stream) in results {
            ordered_sources[position] = Some(stream);
        }
    }

    for stream in ordered_sources.into_iter().flatten() {
        if should_split {
            sources.push(Box::pin(SplitRecordBatchStream::new(stream)));
        } else {
            sources.push(stream);
        }
    }

    if should_split {
        common_telemetry::debug!(
            "Splitting record batches, region: {}, sources: {}, part_range: {:?}",
            stream_ctx.input.region_metadata().region_id,
            sources.len(),
            part_range,
        );
    }

    Ok(())
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
