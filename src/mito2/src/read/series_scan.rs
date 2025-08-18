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
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use futures::StreamExt;
use smallvec::{smallvec, SmallVec};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    PartitionRange, PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
};
use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Semaphore;

use crate::error::{
    Error, InvalidSenderSnafu, PartitionOutOfRangeSnafu, Result, ScanMultiTimesSnafu,
    ScanSeriesSnafu, TooManyFilesToReadSnafu,
};
use crate::read::range::RangeBuilderList;
use crate::read::scan_region::{ScanInput, StreamContext};
use crate::read::scan_util::{PartitionMetrics, PartitionMetricsList, SeriesDistributorMetrics};
use crate::read::seq_scan::{build_sources, SeqScan};
use crate::read::stream::{ConvertBatchStream, ScanBatch, ScanBatchStream};
use crate::read::{Batch, ScannerMetrics};

/// Timeout to send a batch to a sender.
const SEND_TIMEOUT: Duration = Duration::from_millis(10);

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
        let stream_ctx = Arc::new(StreamContext::seq_scan_ctx(input, false));
        properties.partitions = vec![stream_ctx.partition_ranges()];

        Self {
            properties,
            stream_ctx,
            receivers: Mutex::new(Vec::new()),
            metrics_list: Arc::new(PartitionMetricsList::default()),
        }
    }

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

                metrics.num_batches += series.batches.len();
                metrics.num_rows += series.batches.iter().map(|x| x.num_rows()).sum::<usize>();

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
            senders,
            metrics_set: metrics_set.clone(),
            metrics_list: metrics_list.clone(),
        };
        common_runtime::spawn_global(async move {
            distributor.execute().await;
        });

        *rx_list = receivers;
    }

    /// Scans the region and returns a stream.
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

    fn has_predicate(&self) -> bool {
        let predicate = self.stream_ctx.input.predicate();
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
    async fn execute(&mut self) {
        if let Err(e) = self.scan_partitions().await {
            self.senders.send_error(e).await;
        }
    }

    /// Scans all parts.
    async fn scan_partitions(&mut self) -> Result<()> {
        let part_metrics = new_partition_metrics(
            &self.stream_ctx,
            false,
            &self.metrics_set,
            self.partitions.len(),
            &self.metrics_list,
        );
        part_metrics.on_first_poll();

        let range_builder_list = Arc::new(RangeBuilderList::new(
            self.stream_ctx.input.num_memtables(),
            self.stream_ctx.input.num_files(),
        ));
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
                    range_builder_list.clone(),
                    &mut sources,
                )
                .await?;
            }
        }

        // Builds a reader that merge sources from all parts.
        let mut reader =
            SeqScan::build_reader_from_sources(&self.stream_ctx, sources, self.semaphore.clone())
                .await?;
        let mut metrics = SeriesDistributorMetrics::default();
        let mut fetch_start = Instant::now();

        let mut current_series = SeriesBatch::default();
        while let Some(batch) = reader.next_batch().await? {
            metrics.scan_cost += fetch_start.elapsed();
            fetch_start = Instant::now();
            metrics.num_batches += 1;
            metrics.num_rows += batch.num_rows();

            debug_assert!(!batch.is_empty());
            if batch.is_empty() {
                continue;
            }

            let Some(last_key) = current_series.current_key() else {
                current_series.push(batch);
                continue;
            };

            if last_key == batch.primary_key() {
                current_series.push(batch);
                continue;
            }

            // We find a new series, send the current one.
            let to_send = std::mem::replace(&mut current_series, SeriesBatch::single(batch));
            let yield_start = Instant::now();
            self.senders.send_batch(to_send).await?;
            metrics.yield_cost += yield_start.elapsed();
        }

        if !current_series.is_empty() {
            let yield_start = Instant::now();
            self.senders.send_batch(current_series).await?;
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

/// Batches of the same series.
#[derive(Default)]
pub struct SeriesBatch {
    pub batches: SmallVec<[Batch; 4]>,
}

impl SeriesBatch {
    /// Creates a new [SeriesBatch] from a single [Batch].
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
