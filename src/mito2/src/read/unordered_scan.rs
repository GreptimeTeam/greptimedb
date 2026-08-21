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
use common_telemetry::tracing;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use futures::StreamExt;
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
};
use tokio::sync::Semaphore;

use crate::error::{PartitionOutOfRangeSnafu, Result};
use crate::read::pruner::{PartitionPruner, Pruner};
use crate::read::scan_region::{ScanInput, StreamContext};
use crate::read::scan_util::{
    PartitionMetrics, PartitionMetricsList, compute_parallel_channel_size, scan_flat_file_ranges,
    scan_flat_mem_ranges,
};
use crate::read::stream::{ConvertBatchStream, ScanBatch, ScanBatchStream};
use crate::read::{BoxedRecordBatchStream, ScannerMetrics, scan_util};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;

/// Maximum concurrent per-file scan tasks within one [UnorderedScan] partition.
///
/// The shared pruner caps its worker pool at the same magnitude, so additional
/// permits would only add task/memory churn without extra pruning parallelism.
const MAX_FILE_SCAN_CONCURRENCY: usize = 16;

/// Scans a region without providing any output ordering guarantee.
///
/// Only an append only table should use this scanner.
pub struct UnorderedScan {
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Context of streams.
    stream_ctx: Arc<StreamContext>,
    /// Shared pruner for file range building.
    pruner: Arc<Pruner>,
    /// Metrics for each partition.
    metrics_list: PartitionMetricsList,
}

impl UnorderedScan {
    /// Creates a new [UnorderedScan].
    pub(crate) fn new(input: ScanInput) -> Self {
        let mut properties = ScannerProperties::default()
            .with_append_mode(input.append_mode)
            .with_total_rows(input.total_rows());
        if let Some(counters) = input.query_stat_counters.clone() {
            properties.set_query_stat_counters(counters);
        }
        let stream_ctx = Arc::new(StreamContext::unordered_scan_ctx(input));
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

    /// Scans the region and returns a stream.
    #[tracing::instrument(
        skip_all,
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
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

    /// Builds the ordered scan sources for one partition range.
    ///
    /// Sources are returned in row-group order. File sources are scanned
    /// through [scan_flat_file_ranges]; the per-file parallelism itself is
    /// driven by the caller (see [Self::scan_flat_batch_in_partition]), which
    /// spawns one task per part range behind a semaphore and then consumes the
    /// ordered sources. This mirrors how [SeqScan](crate::read::seq_scan::SeqScan)
    /// builds per-file sources and re-orders them into `ordered_sources`.
    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %stream_ctx.input.region_metadata().region_id,
            part_range_id = part_range_id
        )
    )]
    async fn build_flat_partition_range_sources(
        stream_ctx: &Arc<StreamContext>,
        part_range_id: usize,
        part_metrics: &PartitionMetrics,
        partition_pruner: Arc<PartitionPruner>,
    ) -> Result<Vec<BoxedRecordBatchStream>> {
        // Gets range meta.
        let range_meta = &stream_ctx.ranges[part_range_id];
        let part_range = range_meta.new_partition_range(part_range_id);
        let pre_filter_mode = stream_ctx.range_pre_filter_mode(&part_range);

        let num_indices = range_meta.row_group_indices.len();
        if num_indices == 0 {
            return Ok(Vec::new());
        }

        let mut ordered_sources = Vec::with_capacity(num_indices);
        ordered_sources.resize_with(num_indices, || None);
        for (position, index) in range_meta.row_group_indices.iter().enumerate() {
            if stream_ctx.is_mem_range_index(*index) {
                let stream = scan_flat_mem_ranges(
                    stream_ctx.clone(),
                    part_metrics.clone(),
                    *index,
                    range_meta.time_range,
                );
                ordered_sources[position] = Some(Box::pin(stream) as _);
            } else if stream_ctx.is_file_range_index(*index) {
                // Common manifest-level fast-skip shared by UnorderedScan and SeqScan.
                if partition_pruner.try_skip_manifest_pruned_file_range(*index, part_metrics) {
                    continue;
                }
                let stream = scan_flat_file_ranges(
                    stream_ctx.clone(),
                    part_metrics.clone(),
                    *index,
                    "unordered_scan_files",
                    partition_pruner.clone(),
                )
                .await?;
                ordered_sources[position] = Some(Box::pin(stream) as _);
            } else {
                let stream = scan_util::maybe_scan_flat_other_ranges(
                    stream_ctx,
                    *index,
                    part_metrics,
                    pre_filter_mode,
                )
                .await?;
                ordered_sources[position] = Some(stream);
            }
        }

        Ok(ordered_sources.into_iter().flatten().collect())
    }

    /// Creates a semaphore bounding concurrent file scans for one partition.
    ///
    /// [UnorderedScan] provides no output ordering guarantee, so file ranges
    /// can be scanned concurrently as long as results are emitted in row-group
    /// order. The permit count is derived from the query's target partitions
    /// and capped to bound per-partition memory/backpressure. Scans that
    /// explicitly request a single target partition stay sequential.
    fn new_file_scan_semaphore(&self) -> Option<Arc<Semaphore>> {
        let target_partitions = self.properties.target_partitions();
        if target_partitions > 1 {
            Some(Arc::new(Semaphore::new(
                target_partitions.min(MAX_FILE_SCAN_CONCURRENCY),
            )))
        } else {
            None
        }
    }

    /// Scan [`Batch`] in all partitions one by one.
    pub(crate) fn scan_all_partitions(&self) -> Result<ScanBatchStream> {
        let metrics_set = ExecutionPlanMetricsSet::new();

        let streams = (0..self.properties.partitions.len())
            .map(|partition| {
                let metrics = self.partition_metrics(false, partition, &metrics_set);
                self.scan_flat_batch_in_partition(partition, metrics)
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

    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.stream_ctx.input.mapper.metadata().region_id,
            partition = partition
        )
    )]
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
        let input = &self.stream_ctx.input;

        let batch_stream = self.scan_flat_batch_in_partition(partition, metrics.clone())?;

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

        let stream_ctx = self.stream_ctx.clone();
        let part_ranges = self.properties.partitions[partition].clone();
        let pruner = self.pruner.clone();
        // Initializes ref counts for the pruner.
        // If we call scan_batch_in_partition() multiple times but don't read all batches from the stream,
        // then the ref count won't be decremented.
        // This is a rare case and keeping all remaining entries still uses less memory than a per partition cache.
        pruner.add_partition_ranges(&part_ranges);
        let partition_pruner = Arc::new(PartitionPruner::new(pruner, &part_ranges));
        let file_scan_semaphore = self.new_file_scan_semaphore();

        let stream = try_stream! {
            part_metrics.on_first_poll();

            // Builds the scan sources of all part ranges in parallel behind the
            // semaphore, preserving part-range order. UnorderedScan yields no
            // output ordering guarantee, but keeping row-group order matches the
            // previous sequential behavior exactly (append mode has no dedup).
            let mut ordered_sources: Vec<Option<Vec<BoxedRecordBatchStream>>> =
                Vec::with_capacity(part_ranges.len());
            ordered_sources.resize_with(part_ranges.len(), || None);
            let mut part_scan_tasks = Vec::new();
            for (position, part_range) in part_ranges.iter().enumerate() {
                if let Some(semaphore) = file_scan_semaphore.as_ref() {
                    // Run in parallel, controlled by the semaphore.
                    let stream_ctx = stream_ctx.clone();
                    let part_metrics = part_metrics.clone();
                    let partition_pruner = partition_pruner.clone();
                    let semaphore = Arc::clone(semaphore);
                    let part_range = *part_range;
                    part_scan_tasks.push(async move {
                        let _permit = semaphore.acquire().await.unwrap();
                        let sources = Self::build_flat_partition_range_sources(
                            &stream_ctx,
                            part_range.identifier,
                            &part_metrics,
                            partition_pruner,
                        )
                        .await?;
                        Ok::<(usize, Vec<BoxedRecordBatchStream>), crate::error::Error>((
                            position,
                            sources,
                        ))
                    });
                } else {
                    let sources = Self::build_flat_partition_range_sources(
                        &stream_ctx,
                        part_range.identifier,
                        &part_metrics,
                        partition_pruner.clone(),
                    )
                    .await?;
                    ordered_sources[position] = Some(sources);
                }
            }
            if !part_scan_tasks.is_empty() {
                let results = futures::future::try_join_all(part_scan_tasks).await?;
                for (position, sources) in results {
                    ordered_sources[position] = Some(sources);
                }
            }

            // Reads batches from the ordered sources, fetching from multiple
            // files concurrently behind the semaphore with bounded channels.
            let sources: Vec<BoxedRecordBatchStream> =
                ordered_sources.into_iter().flatten().flatten().collect();
            let sources = if let Some(semaphore) = file_scan_semaphore.as_ref() {
                if sources.len() > 1 {
                    stream_ctx.input.create_parallel_flat_sources(
                        sources,
                        semaphore.clone(),
                        compute_parallel_channel_size(DEFAULT_READ_BATCH_SIZE),
                    )?
                } else {
                    sources
                }
            } else {
                sources
            };

            let mut metrics = ScannerMetrics::default();
            let mut fetch_start = Instant::now();
            for mut source in sources {
                while let Some(record_batch) = source.next().await {
                    let record_batch = record_batch?;
                    metrics.scan_cost += fetch_start.elapsed();
                    metrics.num_batches += 1;
                    metrics.num_rows += record_batch.num_rows();

                    debug_assert!(record_batch.num_rows() > 0);
                    if record_batch.num_rows() == 0 {
                        continue;
                    }

                    let yield_start = Instant::now();
                    yield ScanBatch::RecordBatch(record_batch);
                    metrics.yield_cost += yield_start.elapsed();

                    fetch_start = Instant::now();
                }
            }

            metrics.scan_cost += fetch_start.elapsed();
            part_metrics.merge_metrics(&metrics);

            part_metrics.on_finish();
        };
        Ok(Box::pin(stream))
    }
}

impl RegionScanner for UnorderedScan {
    fn name(&self) -> &str {
        "UnorderedScan"
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

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.properties.prepare(request);

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

    /// If this scanner have predicate other than region partition exprs
    fn has_predicate_without_region(&self) -> bool {
        let predicate = self
            .stream_ctx
            .input
            .predicate_group()
            .predicate_without_region();
        predicate.is_some()
    }

    fn add_dyn_filter_to_predicate(
        &mut self,
        filter_exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    ) -> Vec<bool> {
        self.stream_ctx.add_dyn_filter_to_predicate(filter_exprs)
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }

    fn set_query_load_region_id(&mut self, region_id: store_api::storage::RegionId) {
        self.properties.set_query_load_region_id(region_id);
    }

    fn snapshot_sequence(&self) -> Option<u64> {
        self.stream_ctx.input.snapshot_sequence
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use api::v1::Rows;
    use common_recordbatch::RecordBatches;
    use datatypes::arrow::array::TimestampMillisecondArray;
    use store_api::region_engine::{PrepareRequest, RegionEngine, RegionScanner};
    use store_api::region_request::RegionRequest;
    use store_api::storage::{RegionId, ScanRequest};

    use super::*;
    use crate::config::MitoConfig;
    use crate::read::scan_region::Scanner;
    use crate::test_util::{
        CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
    };

    /// Flushes `num_files` non-overlapping, ts-ascending SSTs and scans them
    /// with [UnorderedScan], asserting that all rows are returned and that the
    /// row-group (file) order is preserved both on the sequential path and on
    /// the parallel path enabled by a raised `target_partitions`.
    #[tokio::test]
    async fn test_parallel_file_scan_preserves_rows_and_order() {
        common_telemetry::init_default_ut_logging();

        let mut env = TestEnv::new().await;
        let engine = env
            .create_engine(MitoConfig {
                default_flat_format: true,
                ..Default::default()
            })
            .await;

        let region_id = RegionId::new(1, 1);
        let request = CreateRequestBuilder::new()
            .insert_option("append_mode", "true")
            .build();
        let column_schemas = rows_schema(&request);
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();

        // Flush 4 non-overlapping ts ranges (5 rows each) into 4 SSTs. Each
        // range is one row group, so 4 part ranges -> 4 files.
        let ranges = [0..5, 5..10, 10..15, 15..20];
        for range in &ranges {
            let rows = Rows {
                schema: column_schemas.clone(),
                rows: build_rows(range.start, range.end),
            };
            put_rows(&engine, region_id, rows).await;
            flush_region(&engine, region_id, None).await;
        }

        // Scans both with and without the parallel file-scan path. UnorderedScan
        // makes no ordering guarantee, so the invariant is that the parallel
        // path returns exactly the same rows in exactly the same row-group
        // (version file) order as the sequential path.
        let mut outputs = Vec::with_capacity(2);
        for with_parallel in [false, true] {
            let scanner = engine
                .scanner(region_id, ScanRequest::default())
                .await
                .unwrap();
            let Scanner::Unordered(mut unordered_scan) = scanner else {
                panic!("expected unordered scan for an append-mode flat region");
            };
            if with_parallel {
                // Enables the file-scan semaphore (target partitions > 1).
                unordered_scan
                    .prepare(PrepareRequest::default().with_target_partitions(4))
                    .unwrap();
            }
            let stream = unordered_scan.build_stream().await.unwrap();
            let batches = RecordBatches::try_collect(stream).await.unwrap();

            let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                20, num_rows,
                "all rows must be returned (with_parallel={with_parallel})"
            );

            // Collect the ts values in scan (row-group) order.
            let schema = unordered_scan.schema();
            let ts_idx = schema
                .column_schemas()
                .iter()
                .position(|col| col.name == "ts")
                .expect("ts column must exist in the output schema");
            let mut ts_values = Vec::with_capacity(20);
            for batch in batches.iter() {
                let ts_arr = batch
                    .column(ts_idx)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("ts column must be timestamp millis");
                ts_values.extend(ts_arr.iter().flatten());
            }
            outputs.push(ts_values);
        }
        assert_eq!(
            outputs[0], outputs[1],
            "parallel file scan must preserve the sequential row-group order"
        );
        assert_eq!(20, outputs[0].len());
    }

    /// Micro-benchmark: full [UnorderedScan] over the same set of SSTs with the
    /// file-scan semaphore disabled (sequential) vs enabled (parallel). Both
    /// paths must return identical rows; the reported medians isolate the
    /// parallel-scan effect from all other scanner overhead because only the
    /// semaphore differs. Run with `-- --nocapture` to see the timings.
    #[allow(clippy::print_stdout)]
    #[tokio::test]
    async fn bench_parallel_vs_sequential_file_scan() {
        common_telemetry::init_default_ut_logging();

        let mut env = TestEnv::new().await;
        let engine = env
            .create_engine(MitoConfig {
                default_flat_format: true,
                ..Default::default()
            })
            .await;

        let region_id = RegionId::new(1, 1);
        let request = CreateRequestBuilder::new()
            .insert_option("append_mode", "true")
            .build();
        let column_schemas = rows_schema(&request);
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();

        // 16 non-overlapping ts ranges (4096 rows each) -> 16 SSTs, exercising
        // the same many-files append-mode scan shape as the perf case.
        const NUM_FILES: usize = 16;
        const ROWS_PER_FILE: usize = 4096;
        for i in 0..NUM_FILES {
            let start = i * ROWS_PER_FILE;
            let rows = Rows {
                schema: column_schemas.clone(),
                rows: build_rows(start, start + ROWS_PER_FILE),
            };
            put_rows(&engine, region_id, rows).await;
            flush_region(&engine, region_id, None).await;
        }
        let expected_rows = NUM_FILES * ROWS_PER_FILE;

        async fn scan_all(
            engine: &crate::engine::MitoEngine,
            region_id: RegionId,
            target_partitions: usize,
            expected_rows: usize,
        ) -> Duration {
            let scanner = engine
                .scanner(region_id, ScanRequest::default())
                .await
                .unwrap();
            let Scanner::Unordered(mut unordered_scan) = scanner else {
                panic!("expected unordered scan for an append-mode flat region");
            };
            unordered_scan
                .prepare(PrepareRequest::default().with_target_partitions(target_partitions))
                .unwrap();
            let start = Instant::now();
            let stream = unordered_scan.build_stream().await.unwrap();
            let batches = RecordBatches::try_collect(stream).await.unwrap();
            let elapsed = start.elapsed();
            let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                expected_rows, num_rows,
                "all rows must be returned with target_partitions={target_partitions}"
            );
            elapsed
        }

        async fn median_of(
            engine: &crate::engine::MitoEngine,
            region_id: RegionId,
            target_partitions: usize,
            expected_rows: usize,
            iterations: usize,
        ) -> Duration {
            let mut samples = Vec::with_capacity(iterations);
            for _ in 0..iterations {
                samples.push(scan_all(engine, region_id, target_partitions, expected_rows).await);
            }
            samples.sort();
            samples[samples.len() / 2]
        }

        // Warmup both paths (object store caches, JIT, etc.).
        let _ = scan_all(&engine, region_id, 1, expected_rows).await;
        let _ = scan_all(&engine, region_id, NUM_FILES, expected_rows).await;

        let sequential = median_of(&engine, region_id, 1, expected_rows, 5).await;
        let parallel = median_of(&engine, region_id, NUM_FILES, expected_rows, 5).await;

        println!(
            "unordered_scan micro-bench ({} files x {} rows): sequential={:?} parallel={:?} speedup={:.2}x",
            NUM_FILES,
            ROWS_PER_FILE,
            sequential,
            parallel,
            sequential.as_secs_f64() / parallel.as_secs_f64().max(f64::EPSILON),
        );
        // Both paths must be correct; no timing assertion (machine-dependent).
        assert!(sequential > Duration::ZERO);
        assert!(parallel > Duration::ZERO);
    }
}
