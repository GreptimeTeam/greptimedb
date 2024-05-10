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

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::debug;
use snafu::ResultExt;
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;

use crate::cache::CacheManager;
use crate::error::Result;
use crate::metrics::{READ_BATCHES_RETURN, READ_ROWS_RETURN, READ_STAGE_ELAPSED};
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::ScanInput;
use crate::read::Source;

/// Scans a region without providing any output ordering guarantee.
///
/// Only an append only table should use this scanner.
pub struct UnorderedScan {
    input: ScanInput,
}

impl UnorderedScan {
    /// Creates a new [UnorderedScan].
    pub(crate) fn new(input: ScanInput) -> Self {
        Self { input }
    }

    /// Scans the region and returns a stream.
    pub async fn build_stream(&self) -> Result<SendableRecordBatchStream> {
        let enable_parallel = self.enable_parallel_scan();
        if enable_parallel {
            self.scan_in_parallel().await
        } else {
            self.scan_sources().await
        }
    }

    /// Scans all sources one by one.
    async fn scan_sources(&self) -> Result<SendableRecordBatchStream> {
        let mut metrics = Metrics::default();
        let build_start = Instant::now();
        let query_start = self.input.query_start.unwrap_or(build_start);
        metrics.prepare_scan_cost = query_start.elapsed();

        // Scans all memtables and SSTs.
        let sources = self.input.build_sources().await?;
        metrics.build_source_cost = build_start.elapsed();
        Self::observe_metrics_on_start(&metrics);

        let mapper = self.input.mapper.clone();
        let cache_manager = self.input.cache_manager.clone();
        let stream = try_stream! {
            for mut source in sources {
                let cache = cache_manager.as_deref();
                while let Some(batch) = Self::fetch_from_source(&mut source, &mapper, cache, &mut metrics).await? {
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();
                    yield batch;
                }
            }

            metrics.total_cost = query_start.elapsed();
            Self::observe_metrics_on_finish(&metrics);
            debug!("Unordered scan finished, region_id: {}, metrics: {:?}", mapper.metadata().region_id, metrics);
        };
        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.input.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }

    /// Scans all sources in parallel.
    async fn scan_in_parallel(&self) -> Result<SendableRecordBatchStream> {
        debug_assert!(self.input.parallelism.allow_parallel_scan());

        let mut metrics = Metrics::default();
        let build_start = Instant::now();
        let query_start = self.input.query_start.unwrap_or(build_start);
        metrics.prepare_scan_cost = query_start.elapsed();

        // Scans all memtables and SSTs.
        let sources = self.input.build_sources().await?;
        metrics.build_source_cost = build_start.elapsed();
        Self::observe_metrics_on_start(&metrics);

        let (sender, receiver) = mpsc::channel(self.input.parallelism.channel_size);
        let semaphore = Arc::new(Semaphore::new(self.input.parallelism.parallelism));
        // Spawn a task for each source.
        for source in sources {
            self.input
                .spawn_scan_task(source, semaphore.clone(), sender.clone());
        }
        let stream = Box::pin(ReceiverStream::new(receiver));

        let mapper = self.input.mapper.clone();
        let cache_manager = self.input.cache_manager.clone();
        // For simplicity, we wrap the receiver into a stream to reuse code. We can use the channel directly if it
        // becomes a bottleneck.
        let mut source = Source::Stream(stream);
        let stream = try_stream! {
            let cache = cache_manager.as_deref();
            while let Some(batch) = Self::fetch_from_source(&mut source, &mapper, cache, &mut metrics).await? {
                metrics.num_batches += 1;
                metrics.num_rows += batch.num_rows();
                yield batch;
            }

            metrics.total_cost = query_start.elapsed();
            Self::observe_metrics_on_finish(&metrics);
            debug!("Unordered scan in parallel finished, region_id: {}, metrics: {:?}", mapper.metadata().region_id, metrics);
        };
        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.input.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }

    /// Returns whether to scan in parallel.
    fn enable_parallel_scan(&self) -> bool {
        self.input.parallelism.allow_parallel_scan()
            && (self.input.files.len() + self.input.memtables.len()) > 1
    }

    /// Fetch a batch from the source and convert it into a record batch.
    async fn fetch_from_source(
        source: &mut Source,
        mapper: &ProjectionMapper,
        cache: Option<&CacheManager>,
        metrics: &mut Metrics,
    ) -> common_recordbatch::error::Result<Option<RecordBatch>> {
        let start = Instant::now();

        let Some(batch) = source
            .next_batch()
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?
        else {
            metrics.scan_cost += start.elapsed();

            return Ok(None);
        };

        let convert_start = Instant::now();
        let record_batch = mapper.convert(&batch, cache)?;
        metrics.convert_cost += convert_start.elapsed();
        metrics.scan_cost += start.elapsed();

        Ok(Some(record_batch))
    }

    fn observe_metrics_on_start(metrics: &Metrics) {
        READ_STAGE_ELAPSED
            .with_label_values(&["prepare_scan"])
            .observe(metrics.prepare_scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["build_source"])
            .observe(metrics.build_source_cost.as_secs_f64());
    }

    fn observe_metrics_on_finish(metrics: &Metrics) {
        READ_STAGE_ELAPSED
            .with_label_values(&["convert_rb"])
            .observe(metrics.convert_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["scan"])
            .observe(metrics.scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["total"])
            .observe(metrics.total_cost.as_secs_f64());
        READ_ROWS_RETURN.observe(metrics.num_rows as f64);
        READ_BATCHES_RETURN.observe(metrics.num_batches as f64);
    }
}

#[cfg(test)]
impl UnorderedScan {
    /// Returns the input.
    pub(crate) fn input(&self) -> &ScanInput {
        &self.input
    }
}

/// Metrics for [UnorderedScan].
#[derive(Debug, Default)]
struct Metrics {
    /// Duration to prepare the scan task.
    prepare_scan_cost: Duration,
    /// Duration to build sources.
    build_source_cost: Duration,
    /// Duration to scan data.
    scan_cost: Duration,
    /// Duration to convert batches.
    convert_cost: Duration,
    /// Duration of the scan.
    total_cost: Duration,
    /// Number of batches returned.
    num_batches: usize,
    /// Number of rows returned.
    num_rows: usize,
}
