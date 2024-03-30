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

use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::{debug, tracing};
use snafu::ResultExt;

use crate::cache::CacheManager;
use crate::error::Result;
use crate::metrics::{READ_BATCHES_RETURN, READ_ROWS_RETURN, READ_STAGE_ELAPSED};
use crate::read::merge::MergeReaderBuilder;
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::ScanInput;
use crate::read::{BatchReader, BoxedBatchReader};

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary key, time index`.
pub struct SeqScan {
    input: ScanInput,
}

impl SeqScan {
    /// Creates a new [SeqScan].
    #[must_use]
    pub(crate) fn new(input: ScanInput) -> SeqScan {
        SeqScan { input }
    }

    /// Builds a stream for the query.
    pub async fn build_stream(&self) -> Result<SendableRecordBatchStream> {
        let mut metrics = Metrics::default();
        let build_start = Instant::now();
        let query_start = self.input.query_start.unwrap_or(build_start);
        metrics.prepare_scan_cost = query_start.elapsed();
        let use_parallel = self.use_parallel_reader();
        // Scans all memtables and SSTs. Builds a merge reader to merge results.
        let mut reader = if use_parallel {
            self.build_parallel_reader().await?
        } else {
            self.build_reader().await?
        };
        metrics.build_reader_cost = build_start.elapsed();
        READ_STAGE_ELAPSED
            .with_label_values(&["prepare_scan"])
            .observe(metrics.prepare_scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["build_reader"])
            .observe(metrics.build_reader_cost.as_secs_f64());

        // Creates a stream to poll the batch reader and convert batch into record batch.
        let mapper = self.input.mapper.clone();
        let cache_manager = self.input.cache_manager.clone();
        let parallelism = self.input.parallelism.parallelism;
        let stream = try_stream! {
            let cache = cache_manager.as_ref().map(|cache| cache.as_ref());
            while let Some(batch) =
                Self::fetch_record_batch(&mut reader, &mapper, cache, &mut metrics).await?
            {
                metrics.num_batches += 1;
                metrics.num_rows += batch.num_rows();
                yield batch;
            }

            // Update metrics.
            metrics.total_cost = query_start.elapsed();
            READ_STAGE_ELAPSED.with_label_values(&["convert_rb"]).observe(metrics.convert_cost.as_secs_f64());
            READ_STAGE_ELAPSED.with_label_values(&["scan"]).observe(metrics.scan_cost.as_secs_f64());
            READ_STAGE_ELAPSED.with_label_values(&["total"]).observe(metrics.total_cost.as_secs_f64());
            READ_ROWS_RETURN.observe(metrics.num_rows as f64);
            READ_BATCHES_RETURN.observe(metrics.num_batches as f64);
            debug!(
                "Seq scan finished, region_id: {:?}, metrics: {:?}, use_parallel: {}, parallelism: {}",
                mapper.metadata().region_id, metrics, use_parallel, parallelism,
            );
        };
        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.input.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }

    /// Builds a [BoxedBatchReader] from sequential scan.
    pub async fn build_reader(&self) -> Result<BoxedBatchReader> {
        // Scans all memtables and SSTs. Builds a merge reader to merge results.
        let sources = self.input.build_sources().await?;
        let dedup = !self.input.append_mode;
        let mut builder = MergeReaderBuilder::from_sources(sources, dedup);
        let reader = builder.build().await?;
        Ok(Box::new(reader))
    }

    /// Builds a [BoxedBatchReader] that can scan memtables and SSTs in parallel.
    async fn build_parallel_reader(&self) -> Result<BoxedBatchReader> {
        let sources = self.input.build_parallel_sources().await?;
        let dedup = !self.input.append_mode;
        let mut builder = MergeReaderBuilder::from_sources(sources, dedup);
        let reader = builder.build().await?;
        Ok(Box::new(reader))
    }

    /// Returns whether to use a parallel reader.
    fn use_parallel_reader(&self) -> bool {
        self.input.parallelism.allow_parallel_scan()
            && (self.input.files.len() + self.input.memtables.len()) > 1
    }

    /// Fetch a batch from the reader and convert it into a record batch.
    #[tracing::instrument(skip_all, level = "trace")]
    async fn fetch_record_batch(
        reader: &mut dyn BatchReader,
        mapper: &ProjectionMapper,
        cache: Option<&CacheManager>,
        metrics: &mut Metrics,
    ) -> common_recordbatch::error::Result<Option<RecordBatch>> {
        let start = Instant::now();

        let Some(batch) = reader
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
}

/// Metrics for [SeqScan].
#[derive(Debug, Default)]
struct Metrics {
    /// Duration to prepare the scan task.
    prepare_scan_cost: Duration,
    /// Duration to build the reader.
    build_reader_cost: Duration,
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

#[cfg(test)]
impl SeqScan {
    /// Returns the input.
    pub(crate) fn input(&self) -> &ScanInput {
        &self.input
    }
}
