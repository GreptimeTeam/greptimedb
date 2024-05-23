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
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::{debug, tracing};
use common_time::Timestamp;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use snafu::ResultExt;
use store_api::region_engine::{RegionScanner, ScannerPartitioning, ScannerProperties};
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::cache::CacheManager;
use crate::error::Result;
use crate::memtable::MemtableRef;
use crate::metrics::{READ_BATCHES_RETURN, READ_ROWS_RETURN, READ_STAGE_ELAPSED};
use crate::read::merge::MergeReaderBuilder;
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::{ScanInput, ScanPart, ScanPartBuilder};
use crate::read::{BatchReader, BoxedBatchReader, Source};
use crate::sst::file::FileMeta;
use crate::sst::parquet::file_range::FileRange;

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary key, time index`.
pub struct SeqScan {
    /// Input memtable and files.
    input: ScanInput,
    /// Parts to scan.
    parts: Vec<PartsInTimeRange>,
    /// Properties of the scanner.
    properties: ScannerProperties,
}

impl SeqScan {
    /// Creates a new [SeqScan].
    #[must_use]
    pub(crate) async fn new(input: ScanInput) -> Result<SeqScan> {
        let parts = input.build_parts(SeqPartBuilder::default()).await?;
        let parts = group_parts(parts);
        let properties = ScannerProperties::new(ScannerPartitioning::Unknown(parts.len()));

        Ok(SeqScan {
            input,
            parts,
            properties,
        })
    }

    /// Builds a stream for the query.
    pub async fn build_stream(&self) -> Result<SendableRecordBatchStream> {
        let mut sources = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            part.build_sources(
                Some(self.input.mapper.column_ids()),
                self.input.predicate.as_ref(),
                &mut sources,
            )?;
        }
        let stream = self.sources_to_stream(sources);
        Ok(stream)
    }

    fn sources_to_stream(&self, sources: Vec<Source>) -> SendableRecordBatchStream {
        let dedup = !self.input.append_mode;
        let mut builder =
            MergeReaderBuilder::from_sources(sources, dedup, self.input.filter_deleted);
        let mapper = self.input.mapper.clone();
        let cache_manager = self.input.cache_manager.clone();
        let stream = try_stream! {
            let mut reader = builder
                .build()
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            let cache = cache_manager.as_deref();
            while let Some(batch) = reader
                .next_batch()
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
            {
                let record_batch = mapper.convert(&batch, cache)?;
                yield record_batch;
            }
        };

        Box::pin(RecordBatchStreamWrapper::new(
            self.input.mapper.output_schema(),
            Box::pin(stream),
        ))
    }

    // /// Builds a stream for the query.
    // pub async fn build_stream(&self) -> Result<SendableRecordBatchStream> {
    //     let mut metrics = Metrics::default();
    //     let build_start = Instant::now();
    //     let query_start = self.input.query_start.unwrap_or(build_start);
    //     metrics.prepare_scan_cost = query_start.elapsed();
    //     let use_parallel = self.use_parallel_reader();
    //     // Scans all memtables and SSTs. Builds a merge reader to merge results.
    //     let mut reader = if use_parallel {
    //         self.build_parallel_reader().await?
    //     } else {
    //         self.build_reader().await?
    //     };
    //     metrics.build_reader_cost = build_start.elapsed();
    //     READ_STAGE_ELAPSED
    //         .with_label_values(&["prepare_scan"])
    //         .observe(metrics.prepare_scan_cost.as_secs_f64());
    //     READ_STAGE_ELAPSED
    //         .with_label_values(&["build_reader"])
    //         .observe(metrics.build_reader_cost.as_secs_f64());

    //     // Creates a stream to poll the batch reader and convert batch into record batch.
    //     let mapper = self.input.mapper.clone();
    //     let cache_manager = self.input.cache_manager.clone();
    //     let parallelism = self.input.parallelism.parallelism;
    //     let stream = try_stream! {
    //         let cache = cache_manager.as_ref().map(|cache| cache.as_ref());
    //         while let Some(batch) =
    //             Self::fetch_record_batch(&mut reader, &mapper, cache, &mut metrics).await?
    //         {
    //             metrics.num_batches += 1;
    //             metrics.num_rows += batch.num_rows();
    //             yield batch;
    //         }

    //         // Update metrics.
    //         metrics.total_cost = query_start.elapsed();
    //         READ_STAGE_ELAPSED.with_label_values(&["convert_rb"]).observe(metrics.convert_cost.as_secs_f64());
    //         READ_STAGE_ELAPSED.with_label_values(&["scan"]).observe(metrics.scan_cost.as_secs_f64());
    //         READ_STAGE_ELAPSED.with_label_values(&["total"]).observe(metrics.total_cost.as_secs_f64());
    //         READ_ROWS_RETURN.observe(metrics.num_rows as f64);
    //         READ_BATCHES_RETURN.observe(metrics.num_batches as f64);
    //         debug!(
    //             "Seq scan finished, region_id: {:?}, metrics: {:?}, use_parallel: {}, parallelism: {}",
    //             mapper.metadata().region_id, metrics, use_parallel, parallelism,
    //         );
    //     };
    //     let stream = Box::pin(RecordBatchStreamWrapper::new(
    //         self.input.mapper.output_schema(),
    //         Box::pin(stream),
    //     ));

    //     Ok(stream)
    // }

    /// Builds a [BoxedBatchReader] from sequential scan.
    pub async fn build_reader(&self) -> Result<BoxedBatchReader> {
        // Scans all memtables and SSTs. Builds a merge reader to merge results.
        let sources = self.input.build_sources().await?;
        let dedup = !self.input.append_mode;
        let mut builder =
            MergeReaderBuilder::from_sources(sources, dedup, self.input.filter_deleted);
        let reader = builder.build().await?;
        Ok(Box::new(reader))
    }

    // /// Builds a [BoxedBatchReader] that can scan memtables and SSTs in parallel.
    // async fn build_parallel_reader(&self) -> Result<BoxedBatchReader> {
    //     let sources = self.input.build_parallel_sources().await?;
    //     let dedup = !self.input.append_mode;
    //     let mut builder =
    //         MergeReaderBuilder::from_sources(sources, dedup, self.input.filter_deleted);
    //     let reader = builder.build().await?;
    //     Ok(Box::new(reader))
    // }

    // /// Returns whether to use a parallel reader.
    // fn use_parallel_reader(&self) -> bool {
    //     self.input.parallelism.allow_parallel_scan()
    //         && (self.input.files.len() + self.input.memtables.len()) > 1
    // }

    // /// Fetch a batch from the reader and convert it into a record batch.
    // #[tracing::instrument(skip_all, level = "trace")]
    // async fn fetch_record_batch(
    //     reader: &mut dyn BatchReader,
    //     mapper: &ProjectionMapper,
    //     cache: Option<&CacheManager>,
    //     metrics: &mut Metrics,
    // ) -> common_recordbatch::error::Result<Option<RecordBatch>> {
    //     let start = Instant::now();

    //     let Some(batch) = reader
    //         .next_batch()
    //         .await
    //         .map_err(BoxedError::new)
    //         .context(ExternalSnafu)?
    //     else {
    //         metrics.scan_cost += start.elapsed();

    //         return Ok(None);
    //     };

    //     let convert_start = Instant::now();
    //     let record_batch = mapper.convert(&batch, cache)?;
    //     metrics.convert_cost += convert_start.elapsed();
    //     metrics.scan_cost += start.elapsed();

    //     Ok(Some(record_batch))
    // }
}

impl RegionScanner for SeqScan {
    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.input.mapper.output_schema()
    }

    fn scan_partition(&self, partition: usize) -> Result<SendableRecordBatchStream, BoxedError> {
        let part = &self.parts[partition];
        let mut sources = Vec::new();
        part.build_sources(
            Some(self.input.mapper.column_ids()),
            self.input.predicate.as_ref(),
            &mut sources,
        )
        .map_err(BoxedError::new)?;
        let stream = self.sources_to_stream(sources);
        Ok(stream)
    }
}

impl DisplayAs for SeqScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SeqScan: [{:?}]", self.parts)
    }
}

impl fmt::Debug for SeqScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeqScan")
            .field("parts", &self.parts)
            .finish()
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

/// Builds [ScanPart]s that preserves order.
#[derive(Default)]
pub(crate) struct SeqPartBuilder {
    parts: Vec<ScanPart>,
}

impl ScanPartBuilder for SeqPartBuilder {
    fn set_parallelism(&mut self, _parallelism: usize) {}

    fn append_file_ranges(
        &mut self,
        file_meta: &FileMeta,
        file_ranges: impl Iterator<Item = FileRange>,
    ) {
        // Creates a [ScanPart] for each file.
        let part = ScanPart {
            memtables: Vec::new(),
            file_ranges: file_ranges.collect(),
            time_range: Some(file_meta.time_range),
        };
        if part.file_ranges.is_empty() {
            // No ranges to read.
            return;
        }
        self.parts.push(part);
    }

    fn build_parts(mut self, memtables: &[MemtableRef]) -> Vec<ScanPart> {
        // Creates a part for each memtable.
        for mem in memtables {
            let stats = mem.stats();
            let part = ScanPart {
                memtables: vec![mem.clone()],
                file_ranges: vec![],
                time_range: stats.time_range(),
            };
            self.parts.push(part);
        }

        self.parts
    }
}

/// Parts to scan in the same time range.
#[derive(Debug, Default)]
struct PartsInTimeRange {
    /// Parts to scan. Each [ScanPart] scans a file or a memtable.
    parts: Vec<ScanPart>,
    /// Inclusive time range of parts.
    time_range: Option<(Timestamp, Timestamp)>,
}

impl PartsInTimeRange {
    fn overlaps(&self, part: &ScanPart) -> bool {
        let (Some(current_range), Some(part_range)) = (self.time_range, part.time_range) else {
            return true;
        };

        overlaps(&current_range, &part_range)
    }

    fn merge(&mut self, part: ScanPart) {
        let Some(current_range) = self.time_range else {
            self.time_range = part.time_range;
            self.parts.push(part);
            return;
        };
        let part_range = part.time_range.unwrap();
        let start = current_range.0.min(part_range.0);
        let end = current_range.1.max(part_range.1);
        self.time_range = Some((start, end));
        self.parts.push(part);
    }

    fn build_sources(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: Option<&Predicate>,
        sources: &mut Vec<Source>,
    ) -> Result<()> {
        sources.reserve(self.parts.len());
        for part in &self.parts {
            if let Some(mem) = part.memtables.first() {
                // Each part only has one memtable.
                let iter = mem.iter(projection, predicate.cloned())?;
                sources.push(Source::Iter(iter));
                continue;
            }

            let ranges = part.file_ranges.clone();
            let stream = try_stream! {
                // TODO(yingwen): metrics.
                for range in ranges {
                    let mut reader = range.reader().await?;
                    let compat_batch = range.compat_batch();
                    while let Some(mut batch) = reader.next_batch().await? {
                        if let Some(compat) = compat_batch {
                            batch = compat
                                .compat_batch(batch)?;
                        }

                        yield batch;
                    }
                }
            };
            let stream = Box::pin(stream);
            sources.push(Source::Stream(stream));
        }
        Ok(())
    }
}

/// Groups parts by time range.
/// All time ranges are not None.
fn group_parts(mut parts: Vec<ScanPart>) -> Vec<PartsInTimeRange> {
    if parts.is_empty() {
        return Vec::new();
    }

    // Groups parts by time range.
    parts.sort_unstable_by(|a, b| {
        // Safety: time ranges of parts from [SeqPartBuilder] are not None.
        let a = a.time_range.unwrap();
        let b = b.time_range.unwrap();
        a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1))
    });
    let mut parts_in_range = PartsInTimeRange::default();
    let mut exclusive_parts = Vec::new();
    for part in parts {
        if parts_in_range.parts.is_empty() {
            parts_in_range.time_range = part.time_range;
            parts_in_range.parts.push(part);
            continue;
        }

        if parts_in_range.overlaps(&part) {
            parts_in_range.merge(part);
        } else {
            exclusive_parts.push(parts_in_range);
            let part_range = part.time_range;
            parts_in_range = PartsInTimeRange {
                parts: vec![part],
                time_range: part_range,
            };
        }
    }

    // TODO(yingwen): split/merge parts according to parallelsim

    exclusive_parts
}

// FIXME(yingwen): Use overlaps() in twcs mod.
/// Checks if two inclusive timestamp ranges overlap with each other.
fn overlaps(l: &(Timestamp, Timestamp), r: &(Timestamp, Timestamp)) -> bool {
    let (l, r) = if l.0 <= r.0 { (l, r) } else { (r, l) };
    let (_, l_end) = l;
    let (r_start, _) = r;

    r_start <= l_end
}
