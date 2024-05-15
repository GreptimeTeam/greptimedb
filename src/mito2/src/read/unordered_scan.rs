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
use std::time::{Duration, Instant};

use async_stream::{stream, try_stream};
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::debug;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use futures::StreamExt;
use snafu::ResultExt;
use store_api::region_engine::{RegionScanner, ScannerPartitioning, ScannerProperties};

use crate::cache::CacheManager;
use crate::error::Result;
use crate::metrics::{READ_BATCHES_RETURN, READ_ROWS_RETURN, READ_STAGE_ELAPSED};
use crate::read::compat::CompatBatch;
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::{ScanInput, ScanPart};
use crate::read::Source;
use crate::sst::parquet::reader::ReaderMetrics;

/// Scans a region without providing any output ordering guarantee.
///
/// Only an append only table should use this scanner.
pub struct UnorderedScan {
    /// Input memtables and files.
    input: ScanInput,
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Parts to scan.
    parts: Vec<ScanPart>,

    // Metrics:
    /// The start time of the query.
    query_start: Instant,
    /// Time elapsed before creating the scanner.
    prepare_scan_cost: Duration,
    /// Duration to build parts to scan.
    build_parts_cost: Duration,
}

impl UnorderedScan {
    /// Creates a new [UnorderedScan].
    pub(crate) async fn new(input: ScanInput) -> Result<Self> {
        let build_start = Instant::now();
        let query_start = input.query_start.unwrap_or(build_start);
        let prepare_scan_cost = query_start.elapsed();
        let parts = input.build_parts().await?;
        let build_parts_cost = build_start.elapsed();

        // Observes metrics.
        READ_STAGE_ELAPSED
            .with_label_values(&["prepare_scan"])
            .observe(prepare_scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["build_parts"])
            .observe(build_parts_cost.as_secs_f64());

        Ok(Self {
            input,
            properties: ScannerProperties::new(ScannerPartitioning::Unknown(parts.len())),
            parts,
            query_start,
            prepare_scan_cost,
            build_parts_cost,
        })
    }

    /// Scans the region and returns a stream.
    pub(crate) async fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let streams = (0..self.parts.len())
            .map(|i| self.scan_partition(i))
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

    /// Fetch a batch from the source and convert it into a record batch.
    async fn fetch_from_source(
        source: &mut Source,
        mapper: &ProjectionMapper,
        cache: Option<&CacheManager>,
        compat_batch: Option<&CompatBatch>,
        metrics: &mut Metrics,
    ) -> common_recordbatch::error::Result<Option<RecordBatch>> {
        let start = Instant::now();

        let Some(mut batch) = source
            .next_batch()
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?
        else {
            metrics.scan_cost += start.elapsed();

            return Ok(None);
        };

        if let Some(compat) = compat_batch {
            batch = compat
                .compat_batch(batch)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
        }

        let convert_start = Instant::now();
        let record_batch = mapper.convert(&batch, cache)?;
        metrics.convert_cost += convert_start.elapsed();
        metrics.scan_cost += start.elapsed();

        Ok(Some(record_batch))
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

impl RegionScanner for UnorderedScan {
    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.input.mapper.output_schema()
    }

    fn scan_partition(&self, partition: usize) -> Result<SendableRecordBatchStream, BoxedError> {
        let part = &self.parts[partition];
        let mut metrics = Metrics {
            prepare_scan_cost: self.prepare_scan_cost,
            build_source_cost: self.build_parts_cost,
            ..Default::default()
        };
        let mapper = self.input.mapper.clone();
        let cache_manager = self.input.cache_manager.clone();
        let memtable_sources = part
            .memtables
            .iter()
            .map(|mem| {
                let iter = mem.iter(Some(mapper.column_ids()), self.input.predicate.clone())?;
                Ok(Source::Iter(iter))
            })
            .collect::<Result<Vec<_>>>()
            .map_err(BoxedError::new)?;
        let file_ranges = part.file_ranges.clone();
        let query_start = self.query_start;
        let stream = try_stream! {
            let cache = cache_manager.as_deref();
            // Scans memtables first.
            for mut source in memtable_sources {
                while let Some(batch) = Self::fetch_from_source(&mut source, &mapper, cache, None, &mut metrics).await? {
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();
                    yield batch;
                }
            }
            // Then scans file ranges.
            let mut reader_metrics = ReaderMetrics::default();
            for file_range in file_ranges {
                let reader = file_range.reader().await.map_err(BoxedError::new).context(ExternalSnafu)?;
                let compat_batch = file_range.compat_batch();
                let mut source = Source::RowGroupReader(reader);
                while let Some(batch) = Self::fetch_from_source(&mut source, &mapper, cache, compat_batch, &mut metrics).await? {
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();
                    yield batch;
                }
                if let Source::RowGroupReader(reader) = source {
                    reader_metrics.merge_from(reader.metrics());
                }
            }

            metrics.total_cost = query_start.elapsed();
            Self::observe_metrics_on_finish(&metrics);
            debug!(
                "Unordered scan partition {} finished, region_id: {}, metrics: {:?}, reader_metrics: {:?}",
                partition, mapper.metadata().region_id, metrics, reader_metrics
            );
        };
        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.input.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }
}

impl DisplayAs for UnorderedScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnorderedScan: [{:?}]", self.parts)
    }
}

impl fmt::Debug for UnorderedScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnorderedScan")
            .field("parts", &self.parts)
            .field("prepare_scan_cost", &self.prepare_scan_cost)
            .field("build_parts_cost", &self.build_parts_cost)
            .finish()
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
// We print all fields in logs so we disable the dead_code lint.
#[allow(dead_code)]
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
