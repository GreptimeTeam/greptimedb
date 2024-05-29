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
use tokio::sync::Mutex;

use crate::cache::CacheManager;
use crate::error::Result;
use crate::memtable::MemtableRef;
use crate::metrics::{READ_BATCHES_RETURN, READ_ROWS_RETURN, READ_STAGE_ELAPSED};
use crate::read::compat::CompatBatch;
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::{FileRangeCollector, ScanInput, ScanPart};
use crate::read::Source;
use crate::sst::file::FileMeta;
use crate::sst::parquet::file_range::FileRange;
use crate::sst::parquet::reader::ReaderMetrics;

/// Scans a region without providing any output ordering guarantee.
///
/// Only an append only table should use this scanner.
pub struct UnorderedScan {
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Context of streams.
    stream_ctx: Arc<StreamContext>,
}

impl UnorderedScan {
    /// Creates a new [UnorderedScan].
    pub(crate) fn new(input: ScanInput) -> Self {
        let query_start = input.query_start.unwrap_or_else(Instant::now);
        let prepare_scan_cost = query_start.elapsed();
        let properties =
            ScannerProperties::new(ScannerPartitioning::Unknown(input.parallelism.parallelism));

        // Observes metrics.
        READ_STAGE_ELAPSED
            .with_label_values(&["prepare_scan"])
            .observe(prepare_scan_cost.as_secs_f64());

        let stream_ctx = Arc::new(StreamContext {
            input,
            parts: Mutex::new(ScanPartList::default()),
            query_start,
            prepare_scan_cost,
        });

        Self {
            properties,
            stream_ctx,
        }
    }

    /// Scans the region and returns a stream.
    pub(crate) async fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let part_num = self.properties.partitioning().num_partitions();
        let streams = (0..part_num)
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
        self.stream_ctx.input.mapper.output_schema()
    }

    fn scan_partition(&self, partition: usize) -> Result<SendableRecordBatchStream, BoxedError> {
        let mut metrics = Metrics {
            prepare_scan_cost: self.stream_ctx.prepare_scan_cost,
            ..Default::default()
        };
        let stream_ctx = self.stream_ctx.clone();
        let stream = try_stream! {
            let mut parts = stream_ctx.parts.lock().await;
            parts
                .maybe_init_parts(&stream_ctx.input, &mut metrics)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            let Some(part) = parts.get_part(partition) else {
                return;
            };

            let mapper = &stream_ctx.input.mapper;
            let memtable_sources = part
                .memtables
                .iter()
                .map(|mem| {
                    let iter = mem.iter(
                        Some(mapper.column_ids()),
                        stream_ctx.input.predicate.clone(),
                    )?;
                    Ok(Source::Iter(iter))
                })
                .collect::<Result<Vec<_>>>()
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            let query_start = stream_ctx.query_start;
            let cache = stream_ctx.input.cache_manager.as_deref();
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
            for file_range in &part.file_ranges {
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
            self.stream_ctx.input.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }
}

impl DisplayAs for UnorderedScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnorderedScan: [{:?}]", self.stream_ctx.parts)
    }
}

impl fmt::Debug for UnorderedScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnorderedScan")
            .field("parts", &self.stream_ctx.parts)
            .field("prepare_scan_cost", &self.stream_ctx.prepare_scan_cost)
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

/// List of [ScanPart]s.
#[derive(Debug, Default)]
struct ScanPartList(Option<Vec<ScanPart>>);

impl ScanPartList {
    /// Initializes parts if they are not built yet.
    async fn maybe_init_parts(&mut self, input: &ScanInput, metrics: &mut Metrics) -> Result<()> {
        if self.0.is_none() {
            let now = Instant::now();
            let mut distributor = UnorderedDistributor::default();
            input.prune_file_ranges(&mut distributor).await?;
            self.0 = Some(distributor.build_parts(&input.memtables, input.parallelism.parallelism));

            metrics.build_parts_cost = now.elapsed();
            READ_STAGE_ELAPSED
                .with_label_values(&["build_parts"])
                .observe(metrics.build_parts_cost.as_secs_f64());
        }
        Ok(())
    }

    /// Gets the part by index, returns None if the index is out of bound.
    /// # Panics
    /// Panics if parts are not initialized.
    fn get_part(&mut self, index: usize) -> Option<&ScanPart> {
        let parts = self.0.as_ref().unwrap();
        parts.get(index)
    }
}

/// Context shared by different streams.
/// It contains the input and distributes input to multiple parts
/// to scan.
struct StreamContext {
    /// Input memtables and files.
    input: ScanInput,
    /// Parts to scan.
    /// The scanner builds parts to scan from the input lazily.
    /// The mutex is used to ensure the parts are only built once.
    parts: Mutex<ScanPartList>,

    // Metrics:
    /// The start time of the query.
    query_start: Instant,
    /// Time elapsed before creating the scanner.
    prepare_scan_cost: Duration,
}

/// Metrics for [UnorderedScan].
// We print all fields in logs so we disable the dead_code lint.
#[allow(dead_code)]
#[derive(Debug, Default)]
struct Metrics {
    /// Duration to prepare the scan task.
    prepare_scan_cost: Duration,
    /// Duration to build parts.
    build_parts_cost: Duration,
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

/// Builds [ScanPart]s without preserving order. It distributes file ranges and memtables
/// across partitions. Each partition scans a subset of memtables and file ranges. There
/// is no output ordering guarantee of each partition.
#[derive(Default)]
struct UnorderedDistributor {
    file_ranges: Vec<FileRange>,
}

impl FileRangeCollector for UnorderedDistributor {
    fn append_file_ranges(
        &mut self,
        _file_meta: &FileMeta,
        file_ranges: impl Iterator<Item = FileRange>,
    ) {
        self.file_ranges.extend(file_ranges);
    }
}

impl UnorderedDistributor {
    /// Distributes file ranges and memtables across partitions according to the `parallelism`.
    /// The output number of parts may be `<= parallelism`.
    fn build_parts(self, memtables: &[MemtableRef], parallelism: usize) -> Vec<ScanPart> {
        if parallelism <= 1 {
            // Returns a single part.
            let part = ScanPart {
                memtables: memtables.to_vec(),
                file_ranges: self.file_ranges,
            };
            return vec![part];
        }

        let mems_per_part = ((memtables.len() + parallelism - 1) / parallelism).max(1);
        let ranges_per_part = ((self.file_ranges.len() + parallelism - 1) / parallelism).max(1);
        common_telemetry::debug!(
                "Parallel scan is enabled, parallelism: {}, {} memtables, {} file_ranges, mems_per_part: {}, ranges_per_part: {}",
                parallelism,
                memtables.len(),
                self.file_ranges.len(),
                mems_per_part,
                ranges_per_part
            );
        let mut scan_parts = memtables
            .chunks(mems_per_part)
            .map(|mems| ScanPart {
                memtables: mems.to_vec(),
                file_ranges: Vec::new(),
            })
            .collect::<Vec<_>>();
        for (i, ranges) in self.file_ranges.chunks(ranges_per_part).enumerate() {
            if i == scan_parts.len() {
                scan_parts.push(ScanPart {
                    memtables: Vec::new(),
                    file_ranges: ranges.to_vec(),
                });
            } else {
                scan_parts[i].file_ranges = ranges.to_vec();
            }
        }

        scan_parts
    }
}
