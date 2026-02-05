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

//! Per-series scan implementation.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::util::ChainedRecordBatchStream;
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::tracing::{self, Instrument};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::arrow::array::BinaryArray;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use smallvec::{SmallVec, smallvec};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    PartitionRange, PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::error::{
    Error, InvalidSenderSnafu, PartitionOutOfRangeSnafu, Result, ScanMultiTimesSnafu,
    ScanSeriesSnafu, TooManyFilesToReadSnafu,
};
use crate::read::pruner::{PartitionPruner, Pruner};
use crate::read::scan_region::{ScanInput, StreamContext};
use crate::read::scan_util::{PartitionMetrics, PartitionMetricsList, SeriesDistributorMetrics};
use crate::read::seq_scan::{SeqScan, build_flat_sources, build_sources};
use crate::read::stream::{ConvertBatchStream, ScanBatch, ScanBatchStream};
use crate::read::{Batch, ScannerMetrics};
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::format::PrimaryKeyArray;

/// Timeout to send a batch to a sender.
const SEND_TIMEOUT: Duration = Duration::from_micros(100);

/// List of receivers.
type ReceiverList = Vec<Option<Receiver<Result<SeriesBatch>>>>;

/// Scans a region and returns sorted rows of a series in the same partition.
///
/// The output order is always order by `(primary key, time index)` inside every
/// partition.
/// Always returns the same series (primary key) to the same partition.
pub struct SeriesScan {
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Context of streams.
    stream_ctx: Arc<StreamContext>,
    /// Shared pruner for file range building.
    pruner: Arc<Pruner>,
    /// Receivers of each partition.
    receivers: Mutex<ReceiverList>,
    /// Metrics for each partition.
    /// The scanner only sets in query and keeps it empty during compaction.
    metrics_list: Arc<PartitionMetricsList>,
}

impl SeriesScan {
    /// Creates a new [SeriesScan].
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
            receivers: Mutex::new(Vec::new()),
            metrics_list: Arc::new(PartitionMetricsList::default()),
        }
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
        let metrics = new_partition_metrics(
            &self.stream_ctx,
            ctx.explain_verbose,
            metrics_set,
            partition,
            &self.metrics_list,
        );

        let batch_stream =
            self.scan_batch_in_partition(ctx, partition, metrics.clone(), metrics_set)?;

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

    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.stream_ctx.input.mapper.metadata().region_id,
            partition = partition
        )
    )]
    fn scan_batch_in_partition(
        &self,
        ctx: &QueryScanContext,
        partition: usize,
        part_metrics: PartitionMetrics,
        metrics_set: &ExecutionPlanMetricsSet,
    ) -> Result<ScanBatchStream> {
        if ctx.explain_verbose {
            common_telemetry::info!(
                "SeriesScan partition {}, region_id: {}",
                partition,
                self.stream_ctx.input.region_metadata().region_id
            );
        }

        ensure!(
            partition < self.properties.num_partitions(),
            PartitionOutOfRangeSnafu {
                given: partition,
                all: self.properties.num_partitions(),
            }
        );

        self.maybe_start_distributor(metrics_set, &self.metrics_list);

        let mut receiver = self.take_receiver(partition)?;
        let stream = try_stream! {
            part_metrics.on_first_poll();

            let mut fetch_start = Instant::now();
            while let Some(series) = receiver.recv().await {
                let series = series?;

                let mut metrics = ScannerMetrics::default();
                metrics.scan_cost += fetch_start.elapsed();
                fetch_start = Instant::now();

                metrics.num_batches += series.num_batches();
                metrics.num_rows += series.num_rows();

                let yield_start = Instant::now();
                yield ScanBatch::Series(series);
                metrics.yield_cost += yield_start.elapsed();

                part_metrics.merge_metrics(&metrics);
            }

            part_metrics.on_finish();
        };
        Ok(Box::pin(stream))
    }

    /// Takes the receiver for the partition.
    fn take_receiver(&self, partition: usize) -> Result<Receiver<Result<SeriesBatch>>> {
        let mut rx_list = self.receivers.lock().unwrap();
        rx_list[partition]
            .take()
            .context(ScanMultiTimesSnafu { partition })
    }

    /// Starts the distributor if the receiver list is empty.
    #[tracing::instrument(
        skip(self, metrics_set, metrics_list),
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
    fn maybe_start_distributor(
        &self,
        metrics_set: &ExecutionPlanMetricsSet,
        metrics_list: &Arc<PartitionMetricsList>,
    ) {
        let mut rx_list = self.receivers.lock().unwrap();
        if !rx_list.is_empty() {
            return;
        }

        let (senders, receivers) = new_channel_list(self.properties.num_partitions());
        let mut distributor = SeriesDistributor {
            stream_ctx: self.stream_ctx.clone(),
            semaphore: Some(Arc::new(Semaphore::new(self.properties.num_partitions()))),
            partitions: self.properties.partitions.clone(),
            pruner: self.pruner.clone(),
            senders,
            metrics_set: metrics_set.clone(),
            metrics_list: metrics_list.clone(),
        };
        let region_id = distributor.stream_ctx.input.mapper.metadata().region_id;
        let span = tracing::info_span!("SeriesScan::distributor", region_id = %region_id);
        common_runtime::spawn_global(
            async move {
                distributor.execute().await;
            }
            .instrument(span),
        );

        *rx_list = receivers;
    }

    /// Scans the region and returns a stream.
    #[tracing::instrument(
        skip_all,
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
    pub(crate) async fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let part_num = self.properties.num_partitions();
        let metrics_set = ExecutionPlanMetricsSet::default();
        let streams = (0..part_num)
            .map(|i| self.scan_partition(&QueryScanContext::default(), &metrics_set, i))
            .collect::<Result<Vec<_>, BoxedError>>()?;
        let chained_stream = ChainedRecordBatchStream::new(streams).map_err(BoxedError::new)?;
        Ok(Box::pin(chained_stream))
    }

    /// Scan [`Batch`] in all partitions one by one.
    pub(crate) fn scan_all_partitions(&self) -> Result<ScanBatchStream> {
        let metrics_set = ExecutionPlanMetricsSet::new();

        let streams = (0..self.properties.partitions.len())
            .map(|partition| {
                let metrics = new_partition_metrics(
                    &self.stream_ctx,
                    false,
                    &metrics_set,
                    partition,
                    &self.metrics_list,
                );

                self.scan_batch_in_partition(
                    &QueryScanContext::default(),
                    partition,
                    metrics,
                    &metrics_set,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::pin(futures::stream::iter(streams).flatten()))
    }

    /// Checks resource limit for the scanner.
    pub(crate) fn check_scan_limit(&self) -> Result<()> {
        // Sum the total number of files across all partitions
        let total_files: usize = self
            .properties
            .partitions
            .iter()
            .flat_map(|partition| partition.iter())
            .map(|part_range| {
                let range_meta = &self.stream_ctx.ranges[part_range.identifier];
                range_meta.indices.len()
            })
            .sum();

        let max_concurrent_files = self.stream_ctx.input.max_concurrent_scan_files;
        if total_files > max_concurrent_files {
            return TooManyFilesToReadSnafu {
                actual: total_files,
                max: max_concurrent_files,
            }
            .fail();
        }

        Ok(())
    }
}

fn new_channel_list(num_partitions: usize) -> (SenderList, ReceiverList) {
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..num_partitions)
        .map(|_| {
            let (sender, receiver) = mpsc::channel(1);
            (Some(sender), Some(receiver))
        })
        .unzip();
    (SenderList::new(senders), receivers)
}

impl RegionScanner for SeriesScan {
    fn name(&self) -> &str {
        "SeriesScan"
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

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }
}

impl DisplayAs for SeriesScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SeriesScan: region={}, ",
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

impl fmt::Debug for SeriesScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeriesScan")
            .field("num_ranges", &self.stream_ctx.ranges.len())
            .finish()
    }
}

#[cfg(test)]
impl SeriesScan {
    /// Returns the input.
    pub(crate) fn input(&self) -> &ScanInput {
        &self.stream_ctx.input
    }
}

/// The distributor scans series and distributes them to different partitions.
struct SeriesDistributor {
    /// Context for the scan stream.
    stream_ctx: Arc<StreamContext>,
    /// Optional semaphore for limiting the number of concurrent scans.
    semaphore: Option<Arc<Semaphore>>,
    /// Partition ranges to scan.
    partitions: Vec<Vec<PartitionRange>>,
    /// Shared pruner for file range building.
    pruner: Arc<Pruner>,
    /// Senders of all partitions.
    senders: SenderList,
    /// Metrics set to report.
    /// The distributor report the metrics as an additional partition.
    /// This may double the scan cost of the [SeriesScan] metrics. We can
    /// get per-partition metrics in verbose mode to see the metrics of the
    /// distributor.
    metrics_set: ExecutionPlanMetricsSet,
    metrics_list: Arc<PartitionMetricsList>,
}

impl SeriesDistributor {
    /// Executes the distributor.
    #[tracing::instrument(
        skip_all,
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
    async fn execute(&mut self) {
        let result = if self.stream_ctx.input.flat_format {
            self.scan_partitions_flat().await
        } else {
            self.scan_partitions().await
        };

        if let Err(e) = result {
            self.senders.send_error(e).await;
        }
    }

    /// Scans all parts in flat format using FlatSeriesBatchDivider.
    #[tracing::instrument(
        skip_all,
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
    async fn scan_partitions_flat(&mut self) -> Result<()> {
        // Initialize reference counts for all partition ranges.
        for partition_ranges in &self.partitions {
            self.pruner.add_partition_ranges(partition_ranges);
        }

        // Create PartitionPruner covering all partitions
        let all_partition_ranges: Vec<_> = self.partitions.iter().flatten().cloned().collect();
        let partition_pruner = Arc::new(PartitionPruner::new(
            self.pruner.clone(),
            &all_partition_ranges,
        ));

        let part_metrics = new_partition_metrics(
            &self.stream_ctx,
            false,
            &self.metrics_set,
            self.partitions.len(),
            &self.metrics_list,
        );
        part_metrics.on_first_poll();
        // Start fetch time before building sources so scan cost contains
        // build part cost.
        let mut fetch_start = Instant::now();

        // Scans all parts.
        let mut sources = Vec::with_capacity(self.partitions.len());
        for partition in &self.partitions {
            sources.reserve(partition.len());
            for part_range in partition {
                build_flat_sources(
                    &self.stream_ctx,
                    part_range,
                    false,
                    &part_metrics,
                    partition_pruner.clone(),
                    &mut sources,
                    self.semaphore.clone(),
                )
                .await?;
            }
        }

        // Builds a flat reader that merge sources from all parts.
        let mut reader = SeqScan::build_flat_reader_from_sources(
            &self.stream_ctx,
            sources,
            self.semaphore.clone(),
            Some(&part_metrics),
        )
        .await?;
        let mut metrics = SeriesDistributorMetrics::default();

        let mut divider = FlatSeriesBatchDivider::default();
        while let Some(record_batch) = reader.try_next().await? {
            metrics.scan_cost += fetch_start.elapsed();
            metrics.num_batches += 1;
            metrics.num_rows += record_batch.num_rows();

            debug_assert!(record_batch.num_rows() > 0);
            if record_batch.num_rows() == 0 {
                fetch_start = Instant::now();
                continue;
            }

            // Use divider to split series
            let divider_start = Instant::now();
            let series_batch = divider.push(record_batch);
            metrics.divider_cost += divider_start.elapsed();
            if let Some(series_batch) = series_batch {
                let yield_start = Instant::now();
                self.senders
                    .send_batch(SeriesBatch::Flat(series_batch))
                    .await?;
                metrics.yield_cost += yield_start.elapsed();
            }
            fetch_start = Instant::now();
        }

        // Send any remaining batch in the divider
        let divider_start = Instant::now();
        let series_batch = divider.finish();
        metrics.divider_cost += divider_start.elapsed();
        if let Some(series_batch) = series_batch {
            let yield_start = Instant::now();
            self.senders
                .send_batch(SeriesBatch::Flat(series_batch))
                .await?;
            metrics.yield_cost += yield_start.elapsed();
        }

        metrics.scan_cost += fetch_start.elapsed();
        metrics.num_series_send_timeout = self.senders.num_timeout;
        metrics.num_series_send_full = self.senders.num_full;
        part_metrics.set_distributor_metrics(&metrics);

        part_metrics.on_finish();

        Ok(())
    }

    /// Scans all parts.
    #[tracing::instrument(
        skip_all,
        fields(region_id = %self.stream_ctx.input.mapper.metadata().region_id)
    )]
    async fn scan_partitions(&mut self) -> Result<()> {
        // Initialize reference counts for all partition ranges.
        for partition_ranges in &self.partitions {
            self.pruner.add_partition_ranges(partition_ranges);
        }

        // Create PartitionPruner covering all partitions
        let all_partition_ranges: Vec<_> = self.partitions.iter().flatten().cloned().collect();
        let partition_pruner = Arc::new(PartitionPruner::new(
            self.pruner.clone(),
            &all_partition_ranges,
        ));

        let part_metrics = new_partition_metrics(
            &self.stream_ctx,
            false,
            &self.metrics_set,
            self.partitions.len(),
            &self.metrics_list,
        );
        part_metrics.on_first_poll();
        // Start fetch time before building sources so scan cost contains
        // build part cost.
        let mut fetch_start = Instant::now();

        // Scans all parts.
        let mut sources = Vec::with_capacity(self.partitions.len());
        for partition in &self.partitions {
            sources.reserve(partition.len());
            for part_range in partition {
                build_sources(
                    &self.stream_ctx,
                    part_range,
                    false,
                    &part_metrics,
                    partition_pruner.clone(),
                    &mut sources,
                    self.semaphore.clone(),
                )
                .await?;
            }
        }

        // Builds a reader that merge sources from all parts.
        let mut reader = SeqScan::build_reader_from_sources(
            &self.stream_ctx,
            sources,
            self.semaphore.clone(),
            Some(&part_metrics),
        )
        .await?;
        let mut metrics = SeriesDistributorMetrics::default();

        let mut current_series = PrimaryKeySeriesBatch::default();
        while let Some(batch) = reader.next_batch().await? {
            metrics.scan_cost += fetch_start.elapsed();
            metrics.num_batches += 1;
            metrics.num_rows += batch.num_rows();

            debug_assert!(!batch.is_empty());
            if batch.is_empty() {
                fetch_start = Instant::now();
                continue;
            }

            let Some(last_key) = current_series.current_key() else {
                current_series.push(batch);
                fetch_start = Instant::now();
                continue;
            };

            if last_key == batch.primary_key() {
                current_series.push(batch);
                fetch_start = Instant::now();
                continue;
            }

            // We find a new series, send the current one.
            let to_send =
                std::mem::replace(&mut current_series, PrimaryKeySeriesBatch::single(batch));
            let yield_start = Instant::now();
            self.senders
                .send_batch(SeriesBatch::PrimaryKey(to_send))
                .await?;
            metrics.yield_cost += yield_start.elapsed();
            fetch_start = Instant::now();
        }

        if !current_series.is_empty() {
            let yield_start = Instant::now();
            self.senders
                .send_batch(SeriesBatch::PrimaryKey(current_series))
                .await?;
            metrics.yield_cost += yield_start.elapsed();
        }

        metrics.scan_cost += fetch_start.elapsed();
        metrics.num_series_send_timeout = self.senders.num_timeout;
        metrics.num_series_send_full = self.senders.num_full;
        part_metrics.set_distributor_metrics(&metrics);

        part_metrics.on_finish();

        Ok(())
    }
}

/// Batches of the same series in primary key format.
#[derive(Default, Debug)]
pub struct PrimaryKeySeriesBatch {
    pub batches: SmallVec<[Batch; 4]>,
}

impl PrimaryKeySeriesBatch {
    /// Creates a new [PrimaryKeySeriesBatch] from a single [Batch].
    fn single(batch: Batch) -> Self {
        Self {
            batches: smallvec![batch],
        }
    }

    fn current_key(&self) -> Option<&[u8]> {
        self.batches.first().map(|batch| batch.primary_key())
    }

    fn push(&mut self, batch: Batch) {
        self.batches.push(batch);
    }

    /// Returns true if there is no batch.
    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// Batches of the same series.
#[derive(Debug)]
pub enum SeriesBatch {
    PrimaryKey(PrimaryKeySeriesBatch),
    Flat(FlatSeriesBatch),
}

impl SeriesBatch {
    /// Returns the number of batches.
    pub fn num_batches(&self) -> usize {
        match self {
            SeriesBatch::PrimaryKey(primary_key_batch) => primary_key_batch.batches.len(),
            SeriesBatch::Flat(flat_batch) => flat_batch.batches.len(),
        }
    }

    /// Returns the total number of rows across all batches.
    pub fn num_rows(&self) -> usize {
        match self {
            SeriesBatch::PrimaryKey(primary_key_batch) => {
                primary_key_batch.batches.iter().map(|x| x.num_rows()).sum()
            }
            SeriesBatch::Flat(flat_batch) => flat_batch.batches.iter().map(|x| x.num_rows()).sum(),
        }
    }
}

/// Batches of the same series in flat format.
#[derive(Default, Debug)]
pub struct FlatSeriesBatch {
    pub batches: SmallVec<[RecordBatch; 4]>,
}

/// List of senders.
struct SenderList {
    senders: Vec<Option<Sender<Result<SeriesBatch>>>>,
    /// Number of None senders.
    num_nones: usize,
    /// Index of the current partition to send.
    sender_idx: usize,
    /// Number of timeout.
    num_timeout: usize,
    /// Number of full senders.
    num_full: usize,
}

impl SenderList {
    fn new(senders: Vec<Option<Sender<Result<SeriesBatch>>>>) -> Self {
        let num_nones = senders.iter().filter(|sender| sender.is_none()).count();
        Self {
            senders,
            num_nones,
            sender_idx: 0,
            num_timeout: 0,
            num_full: 0,
        }
    }

    /// Finds a partition and tries to send the batch to the partition.
    /// Returns None if it sends successfully.
    fn try_send_batch(&mut self, mut batch: SeriesBatch) -> Result<Option<SeriesBatch>> {
        for _ in 0..self.senders.len() {
            ensure!(self.num_nones < self.senders.len(), InvalidSenderSnafu);

            let sender_idx = self.fetch_add_sender_idx();
            let Some(sender) = &self.senders[sender_idx] else {
                continue;
            };

            match sender.try_send(Ok(batch)) {
                Ok(()) => return Ok(None),
                Err(TrySendError::Full(res)) => {
                    self.num_full += 1;
                    // Safety: we send Ok.
                    batch = res.unwrap();
                }
                Err(TrySendError::Closed(res)) => {
                    self.senders[sender_idx] = None;
                    self.num_nones += 1;
                    // Safety: we send Ok.
                    batch = res.unwrap();
                }
            }
        }

        Ok(Some(batch))
    }

    /// Finds a partition and sends the batch to the partition.
    async fn send_batch(&mut self, mut batch: SeriesBatch) -> Result<()> {
        // Sends the batch without blocking first.
        match self.try_send_batch(batch)? {
            Some(b) => {
                // Unable to send batch to partition.
                batch = b;
            }
            None => {
                return Ok(());
            }
        }

        loop {
            ensure!(self.num_nones < self.senders.len(), InvalidSenderSnafu);

            let sender_idx = self.fetch_add_sender_idx();
            let Some(sender) = &self.senders[sender_idx] else {
                continue;
            };
            // Adds a timeout to avoid blocking indefinitely and sending
            // the batch in a round-robin fashion when some partitions
            // don't poll their inputs. This may happen if we have a
            // node like sort merging. But it is rare when we are using SeriesScan.
            match sender.send_timeout(Ok(batch), SEND_TIMEOUT).await {
                Ok(()) => break,
                Err(SendTimeoutError::Timeout(res)) => {
                    self.num_timeout += 1;
                    // Safety: we send Ok.
                    batch = res.unwrap();
                }
                Err(SendTimeoutError::Closed(res)) => {
                    self.senders[sender_idx] = None;
                    self.num_nones += 1;
                    // Safety: we send Ok.
                    batch = res.unwrap();
                }
            }
        }

        Ok(())
    }

    async fn send_error(&self, error: Error) {
        let error = Arc::new(error);
        for sender in self.senders.iter().flatten() {
            let result = Err(error.clone()).context(ScanSeriesSnafu);
            let _ = sender.send(result).await;
        }
    }

    fn fetch_add_sender_idx(&mut self) -> usize {
        let sender_idx = self.sender_idx;
        self.sender_idx = (self.sender_idx + 1) % self.senders.len();
        sender_idx
    }
}

fn new_partition_metrics(
    stream_ctx: &StreamContext,
    explain_verbose: bool,
    metrics_set: &ExecutionPlanMetricsSet,
    partition: usize,
    metrics_list: &PartitionMetricsList,
) -> PartitionMetrics {
    let metrics = PartitionMetrics::new(
        stream_ctx.input.mapper.metadata().region_id,
        partition,
        "SeriesScan",
        stream_ctx.query_start,
        explain_verbose,
        metrics_set,
    );

    metrics_list.set(partition, metrics.clone());
    metrics
}

/// A divider to split flat record batches by time series.
///
/// It only ensures rows of the same series are returned in the same [FlatSeriesBatch].
/// However, a [FlatSeriesBatch] may contain rows from multiple series.
#[derive(Default)]
struct FlatSeriesBatchDivider {
    buffer: FlatSeriesBatch,
}

impl FlatSeriesBatchDivider {
    /// Pushes a record batch into the divider.
    ///
    /// Returns a [FlatSeriesBatch] if we ensure the batch contains all rows of the series in it.
    fn push(&mut self, batch: RecordBatch) -> Option<FlatSeriesBatch> {
        // If buffer is empty
        if self.buffer.batches.is_empty() {
            self.buffer.batches.push(batch);
            return None;
        }

        // Gets the primary key column from the incoming batch.
        let pk_column_idx = primary_key_column_index(batch.num_columns());
        let batch_pk_column = batch.column(pk_column_idx);
        let batch_pk_array = batch_pk_column
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        let batch_pk_values = batch_pk_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        // Gets the last primary key of the incoming batch.
        let batch_last_pk =
            primary_key_at(batch_pk_array, batch_pk_values, batch_pk_array.len() - 1);
        // Gets the last primary key of the buffer.
        // Safety: the buffer is not empty.
        let buffer_last_batch = self.buffer.batches.last().unwrap();
        let buffer_pk_column = buffer_last_batch.column(pk_column_idx);
        let buffer_pk_array = buffer_pk_column
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        let buffer_pk_values = buffer_pk_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let buffer_last_pk =
            primary_key_at(buffer_pk_array, buffer_pk_values, buffer_pk_array.len() - 1);

        // If last primary key in the batch is the same as last primary key in the buffer.
        if batch_last_pk == buffer_last_pk {
            self.buffer.batches.push(batch);
            return None;
        }
        // Otherwise, the batch must have a different primary key, we find the first offset of the
        // changed primary key.
        let batch_pk_keys = batch_pk_array.keys();
        let pk_indices = batch_pk_keys.values();
        let mut change_offset = 0;
        for (i, &key) in pk_indices.iter().enumerate() {
            let batch_pk = batch_pk_values.value(key as usize);

            if buffer_last_pk != batch_pk {
                change_offset = i;
                break;
            }
        }

        // Splits the batch at the change offset
        let (first_part, remaining_part) = if change_offset > 0 {
            let first_part = batch.slice(0, change_offset);
            let remaining_part = batch.slice(change_offset, batch.num_rows() - change_offset);
            (Some(first_part), Some(remaining_part))
        } else {
            (None, Some(batch))
        };

        // Creates the result from current buffer + first part of new batch
        let mut result = std::mem::take(&mut self.buffer);
        if let Some(first_part) = first_part {
            result.batches.push(first_part);
        }

        // Pushes remaining part to the buffer if it exists
        if let Some(remaining_part) = remaining_part {
            self.buffer.batches.push(remaining_part);
        }

        Some(result)
    }

    /// Returns the final [FlatSeriesBatch].
    fn finish(&mut self) -> Option<FlatSeriesBatch> {
        if self.buffer.batches.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.buffer))
        }
    }
}

/// Helper function to extract primary key bytes at a specific index from [PrimaryKeyArray].
fn primary_key_at<'a>(
    primary_key: &PrimaryKeyArray,
    primary_key_values: &'a BinaryArray,
    index: usize,
) -> &'a [u8] {
    let key = primary_key.keys().value(index);
    primary_key_values.value(key as usize)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::OpType;
    use datatypes::arrow::array::{
        ArrayRef, BinaryDictionaryBuilder, Int64Array, StringDictionaryBuilder,
        TimestampMillisecondArray, UInt8Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;

    use super::*;

    fn new_test_record_batch(
        primary_keys: &[&[u8]],
        timestamps: &[i64],
        sequences: &[u64],
        op_types: &[OpType],
        fields: &[u64],
    ) -> RecordBatch {
        let num_rows = timestamps.len();
        debug_assert_eq!(sequences.len(), num_rows);
        debug_assert_eq!(op_types.len(), num_rows);
        debug_assert_eq!(fields.len(), num_rows);
        debug_assert_eq!(primary_keys.len(), num_rows);

        let columns: Vec<ArrayRef> = vec![
            build_test_pk_string_dict_array(primary_keys),
            Arc::new(Int64Array::from_iter(
                fields.iter().map(|v| Some(*v as i64)),
            )),
            Arc::new(TimestampMillisecondArray::from_iter_values(
                timestamps.iter().copied(),
            )),
            build_test_pk_array(primary_keys),
            Arc::new(UInt64Array::from_iter_values(sequences.iter().copied())),
            Arc::new(UInt8Array::from_iter_values(
                op_types.iter().map(|v| *v as u8),
            )),
        ];

        RecordBatch::try_new(build_test_flat_schema(), columns).unwrap()
    }

    fn build_test_pk_string_dict_array(primary_keys: &[&[u8]]) -> ArrayRef {
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        for &pk in primary_keys {
            let pk_str = std::str::from_utf8(pk).unwrap();
            builder.append(pk_str).unwrap();
        }
        Arc::new(builder.finish())
    }

    fn build_test_pk_array(primary_keys: &[&[u8]]) -> ArrayRef {
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for &pk in primary_keys {
            builder.append(pk).unwrap();
        }
        Arc::new(builder.finish())
    }

    fn build_test_flat_schema() -> SchemaRef {
        let fields = vec![
            Field::new(
                "k0",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("field0", DataType::Int64, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ];
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_empty_buffer_first_push() {
        let mut divider = FlatSeriesBatchDivider::default();
        let result = divider.finish();
        assert!(result.is_none());

        let mut divider = FlatSeriesBatchDivider::default();
        let batch = new_test_record_batch(
            &[b"series1", b"series1"],
            &[1000, 2000],
            &[1, 2],
            &[OpType::Put, OpType::Put],
            &[10, 20],
        );
        let result = divider.push(batch);
        assert!(result.is_none());
        assert_eq!(divider.buffer.batches.len(), 1);
    }

    #[test]
    fn test_same_series_accumulation() {
        let mut divider = FlatSeriesBatchDivider::default();

        let batch1 = new_test_record_batch(
            &[b"series1", b"series1"],
            &[1000, 2000],
            &[1, 2],
            &[OpType::Put, OpType::Put],
            &[10, 20],
        );

        let batch2 = new_test_record_batch(
            &[b"series1", b"series1"],
            &[3000, 4000],
            &[3, 4],
            &[OpType::Put, OpType::Put],
            &[30, 40],
        );

        divider.push(batch1);
        let result = divider.push(batch2);
        assert!(result.is_none());
        let series_batch = divider.finish().unwrap();
        assert_eq!(series_batch.batches.len(), 2);
    }

    #[test]
    fn test_series_boundary_detection() {
        let mut divider = FlatSeriesBatchDivider::default();

        let batch1 = new_test_record_batch(
            &[b"series1", b"series1"],
            &[1000, 2000],
            &[1, 2],
            &[OpType::Put, OpType::Put],
            &[10, 20],
        );

        let batch2 = new_test_record_batch(
            &[b"series2", b"series2"],
            &[3000, 4000],
            &[3, 4],
            &[OpType::Put, OpType::Put],
            &[30, 40],
        );

        divider.push(batch1);
        let series_batch = divider.push(batch2).unwrap();
        assert_eq!(series_batch.batches.len(), 1);

        assert_eq!(divider.buffer.batches.len(), 1);
    }

    #[test]
    fn test_series_boundary_within_batch() {
        let mut divider = FlatSeriesBatchDivider::default();

        let batch1 = new_test_record_batch(
            &[b"series1", b"series1"],
            &[1000, 2000],
            &[1, 2],
            &[OpType::Put, OpType::Put],
            &[10, 20],
        );

        let batch2 = new_test_record_batch(
            &[b"series1", b"series2"],
            &[3000, 4000],
            &[3, 4],
            &[OpType::Put, OpType::Put],
            &[30, 40],
        );

        divider.push(batch1);
        let series_batch = divider.push(batch2).unwrap();
        assert_eq!(series_batch.batches.len(), 2);
        assert_eq!(series_batch.batches[0].num_rows(), 2);
        assert_eq!(series_batch.batches[1].num_rows(), 1);

        assert_eq!(divider.buffer.batches.len(), 1);
        assert_eq!(divider.buffer.batches[0].num_rows(), 1);
    }

    #[test]
    fn test_series_splitting() {
        let mut divider = FlatSeriesBatchDivider::default();

        let batch1 = new_test_record_batch(&[b"series1"], &[1000], &[1], &[OpType::Put], &[10]);

        let batch2 = new_test_record_batch(
            &[b"series1", b"series2", b"series2", b"series3"],
            &[2000, 3000, 4000, 5000],
            &[2, 3, 4, 5],
            &[OpType::Put, OpType::Put, OpType::Put, OpType::Put],
            &[20, 30, 40, 50],
        );

        divider.push(batch1);
        let series_batch = divider.push(batch2).unwrap();
        assert_eq!(series_batch.batches.len(), 2);

        let total_rows: usize = series_batch.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let final_batch = divider.finish().unwrap();
        assert_eq!(final_batch.batches.len(), 1);
        assert_eq!(final_batch.batches[0].num_rows(), 3);
    }
}
