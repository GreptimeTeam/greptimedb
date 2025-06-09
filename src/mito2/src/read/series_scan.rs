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
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::util::ChainedRecordBatchStream;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::compute::concat_batches;
use datatypes::schema::SchemaRef;
use smallvec::{smallvec, SmallVec};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{PartitionRange, PrepareRequest, RegionScanner, ScannerProperties};
use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Semaphore;

use crate::error::{
    ComputeArrowSnafu, Error, InvalidSenderSnafu, PartitionOutOfRangeSnafu, Result,
    ScanMultiTimesSnafu, ScanSeriesSnafu,
};
use crate::read::range::RangeBuilderList;
use crate::read::scan_region::{ScanInput, StreamContext};
use crate::read::scan_util::{PartitionMetrics, PartitionMetricsList, SeriesDistributorMetrics};
use crate::read::seq_scan::{build_sources, SeqScan};
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
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        if partition >= self.properties.num_partitions() {
            return Err(BoxedError::new(
                PartitionOutOfRangeSnafu {
                    given: partition,
                    all: self.properties.num_partitions(),
                }
                .build(),
            ));
        }

        self.maybe_start_distributor(metrics_set, &self.metrics_list);

        let part_metrics =
            new_partition_metrics(&self.stream_ctx, metrics_set, partition, &self.metrics_list);
        let mut receiver = self.take_receiver(partition).map_err(BoxedError::new)?;
        let stream_ctx = self.stream_ctx.clone();

        let stream = try_stream! {
            part_metrics.on_first_poll();

            let cache = &stream_ctx.input.cache_strategy;
            let mut df_record_batches = Vec::new();
            let mut fetch_start = Instant::now();
            while let Some(result) = receiver.recv().await {
                let mut metrics = ScannerMetrics::default();
                let series = result.map_err(BoxedError::new).context(ExternalSnafu)?;
                metrics.scan_cost += fetch_start.elapsed();
                fetch_start = Instant::now();

                let convert_start = Instant::now();
                df_record_batches.reserve(series.batches.len());
                for batch in series.batches {
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();

                    let record_batch = stream_ctx.input.mapper.convert(&batch, cache)?;
                    df_record_batches.push(record_batch.into_df_record_batch());
                }

                let output_schema = stream_ctx.input.mapper.output_schema();
                let df_record_batch =
                    concat_batches(output_schema.arrow_schema(), &df_record_batches)
                        .context(ComputeArrowSnafu)
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?;
                df_record_batches.clear();
                let record_batch =
                    RecordBatch::try_from_df_record_batch(output_schema, df_record_batch)?;
                metrics.convert_cost += convert_start.elapsed();

                let yield_start = Instant::now();
                yield record_batch;
                metrics.yield_cost += yield_start.elapsed();

                part_metrics.merge_metrics(&metrics);
            }
        };

        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.stream_ctx.input.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
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
            .map(|i| self.scan_partition(&metrics_set, i))
            .collect::<Result<Vec<_>, BoxedError>>()?;
        let chained_stream = ChainedRecordBatchStream::new(streams).map_err(BoxedError::new)?;
        Ok(Box::pin(chained_stream))
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
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        self.scan_partition_impl(metrics_set, partition)
    }

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.properties.prepare(request);
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
            DisplayFormatType::Default => self.stream_ctx.format_for_explain(false, f),
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
                );
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
        part_metrics.set_distributor_metrics(&metrics);

        part_metrics.on_finish();

        Ok(())
    }
}

/// Batches of the same series.
#[derive(Default)]
struct SeriesBatch {
    batches: SmallVec<[Batch; 4]>,
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
}

impl SenderList {
    fn new(senders: Vec<Option<Sender<Result<SeriesBatch>>>>) -> Self {
        let num_nones = senders.iter().filter(|sender| sender.is_none()).count();
        Self {
            senders,
            num_nones,
            sender_idx: 0,
            num_timeout: 0,
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
    metrics_set: &ExecutionPlanMetricsSet,
    partition: usize,
    metrics_list: &PartitionMetricsList,
) -> PartitionMetrics {
    let metrics = PartitionMetrics::new(
        stream_ctx.input.mapper.metadata().region_id,
        partition,
        "SeriesScan",
        stream_ctx.query_start,
        metrics_set,
    );

    metrics_list.set(partition, metrics.clone());
    metrics
}
