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
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::debug;
use common_time::Timestamp;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use smallvec::smallvec;
use snafu::ResultExt;
use store_api::region_engine::{RegionScanner, ScannerPartitioning, ScannerProperties};
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::MemtableRef;
use crate::read::merge::MergeReaderBuilder;
use crate::read::scan_region::{FileRangeCollector, ScanInput, ScanPart};
use crate::read::{BatchReader, BoxedBatchReader, ScannerMetrics, Source};
use crate::sst::file::FileMeta;
use crate::sst::parquet::file_range::FileRange;
use crate::sst::parquet::reader::ReaderMetrics;

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary key, time index`.
pub struct SeqScan {
    /// Input memtable and files.
    input: ScanInput,
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Parts to scan.
    parts: Vec<PartsInTimeRange>,

    // Metrics:
    /// The start time of the query.
    query_start: Instant,
    /// Time elapsed before creating the scanner.
    prepare_scan_cost: Duration,
    /// Duration to build parts to scan.
    build_parts_cost: Duration,
}

impl SeqScan {
    /// Creates a new [SeqScan].
    pub(crate) async fn new(input: ScanInput) -> Result<SeqScan> {
        let build_start = Instant::now();
        let query_start = input.query_start.unwrap_or(build_start);
        let prepare_scan_cost = query_start.elapsed();
        let mut distributor = SeqDistributor::default();
        input.prune_file_ranges(&mut distributor).await?;
        let parts = group_parts_by_range(distributor.build_parts(&input.memtables));
        let parts = maybe_split_parts(parts, input.parallelism.parallelism);
        let properties = ScannerProperties::new(ScannerPartitioning::Unknown(parts.len()));
        let build_parts_cost = build_start.elapsed();

        // Observes metrics.
        ScannerMetrics::observe_metrics_on_create(&prepare_scan_cost, &build_parts_cost);

        Ok(SeqScan {
            input,
            properties,
            parts,
            query_start,
            prepare_scan_cost,
            build_parts_cost,
        })
    }

    /// Builds a stream for the query.
    pub async fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let mut sources = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            part.build_sources(
                Some(self.input.mapper.column_ids()),
                self.input.predicate.as_ref(),
                &mut sources,
            )
            .map_err(BoxedError::new)?;
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
        let mut metrics = ScannerMetrics {
            prepare_scan_cost: self.prepare_scan_cost,
            build_parts_cost: self.build_parts_cost,
            ..Default::default()
        };
        let query_start = self.query_start;
        let stream = try_stream! {
            let mut reader = builder
                .build()
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            let cache = cache_manager.as_deref();
            let mut fetch_start = Instant::now();
            while let Some(batch) = reader
                .next_batch()
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
            {
                metrics.scan_cost += fetch_start.elapsed();
                metrics.num_batches += 1;
                metrics.num_rows += batch.num_rows();

                let convert_start = Instant::now();
                let record_batch = mapper.convert(&batch, cache)?;
                metrics.convert_cost += convert_start.elapsed();
                yield record_batch;

                fetch_start = Instant::now();
            }
            metrics.scan_cost += fetch_start.elapsed();
            metrics.total_cost = query_start.elapsed();
            metrics.observe_metrics_on_finish();

            debug!(
                "Seq scan finished, region_id: {:?}, metrics: {:?}",
                mapper.metadata().region_id, metrics,
            );
        };

        Box::pin(RecordBatchStreamWrapper::new(
            self.input.mapper.output_schema(),
            Box::pin(stream),
        ))
    }

    // TODO(yingwen): Remove this.
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

#[cfg(test)]
impl SeqScan {
    /// Returns the input.
    pub(crate) fn input(&self) -> &ScanInput {
        &self.input
    }
}

/// Builds [ScanPart]s that preserves order.
#[derive(Default)]
pub(crate) struct SeqDistributor {
    parts: Vec<ScanPart>,
}

impl FileRangeCollector for SeqDistributor {
    fn append_file_ranges(
        &mut self,
        file_meta: &FileMeta,
        file_ranges: impl Iterator<Item = FileRange>,
    ) {
        // Creates a [ScanPart] for each file.
        let ranges: Vec<_> = file_ranges.collect();
        if ranges.is_empty() {
            // No ranges to read.
            return;
        }
        let part = ScanPart {
            memtables: Vec::new(),
            file_ranges: smallvec![ranges],
            time_range: Some(file_meta.time_range),
        };
        self.parts.push(part);
    }
}

impl SeqDistributor {
    fn build_parts(mut self, memtables: &[MemtableRef]) -> Vec<ScanPart> {
        // Creates a part for each memtable.
        for mem in memtables {
            let stats = mem.stats();
            let part = ScanPart {
                memtables: vec![mem.clone()],
                file_ranges: smallvec![],
                time_range: stats.time_range(),
            };
            self.parts.push(part);
        }

        self.parts
    }
}

/// Builds sources to merge from a `part`.
fn build_sources_to_merge(
    part: &ScanPart,
    projection: Option<&[ColumnId]>,
    predicate: Option<&Predicate>,
    sources: &mut Vec<Source>,
) -> Result<()> {
    sources.reserve(part.memtables.len() + part.file_ranges.len());
    // Read memtables.
    for mem in &part.memtables {
        let iter = mem.iter(projection, predicate.cloned())?;
        sources.push(Source::Iter(iter));
    }
    // Read files.
    for file in &part.file_ranges {
        if file.is_empty() {
            continue;
        }

        // Creates a stream to read the file.
        let ranges = file.clone();
        let stream = try_stream! {
            let mut reader_metrics = ReaderMetrics::default();
            let file_id = ranges[0].file_handle().file_id();
            let region_id = ranges[0].file_handle().region_id();
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
                reader_metrics.merge_from(reader.metrics());
            }
            debug!(
                "Seq scan region {} file {} ranges finished, metrics: {:?}",
                region_id, file_id, reader_metrics
            );
        };
        let stream = Box::pin(stream);
        sources.push(Source::Stream(stream));
    }

    Ok(())
}

/// Groups parts by time range. It may generate parts more than parallelism.
/// All time ranges are not None.
fn group_parts_by_range(mut parts: Vec<ScanPart>) -> Vec<ScanPart> {
    if parts.is_empty() {
        return Vec::new();
    }

    // Sorts parts by time range.
    parts.sort_unstable_by(|a, b| {
        // Safety: time ranges of parts from [SeqPartBuilder] are not None.
        let a = a.time_range.unwrap();
        let b = b.time_range.unwrap();
        a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1))
    });
    let mut part_in_range = None;
    // Parts with exclusive time ranges.
    let mut part_groups = Vec::new();
    for part in parts {
        let Some(mut prev_part) = part_in_range.take() else {
            part_in_range = Some(part);
            continue;
        };

        if prev_part.overlaps(&part) {
            prev_part.merge(part);
            part_in_range = Some(prev_part);
        } else {
            // A new group.
            part_groups.push(prev_part);
            part_in_range = Some(part);
        }
    }
    if let Some(part) = part_in_range {
        part_groups.push(part);
    }

    part_groups
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::timestamp::TimeUnit;

    use super::*;
    use crate::memtable::MemtableId;
    use crate::test_util::memtable_util::EmptyMemtable;

    type Output = (Vec<MemtableId>, i64, i64);

    fn run_group_parts_test(input: &[(MemtableId, i64, i64)], expect: &[Output]) {
        let parts = input
            .iter()
            .map(|(id, start, end)| {
                let range = (
                    Timestamp::new(*start, TimeUnit::Second),
                    Timestamp::new(*end, TimeUnit::Second),
                );
                ScanPart {
                    memtables: vec![Arc::new(
                        EmptyMemtable::new(*id).with_time_range(Some(range)),
                    )],
                    file_ranges: smallvec![],
                    time_range: Some(range),
                }
            })
            .collect();
        let output = group_parts_by_range(parts);
        let actual: Vec<_> = output
            .iter()
            .map(|part| {
                let ids: Vec<_> = part.memtables.iter().map(|mem| mem.id()).collect();
                let range = part.time_range.unwrap();
                (ids, range.0.value(), range.1.value())
            })
            .collect();
        assert_eq!(expect, actual);
    }

    #[test]
    fn test_group_parts() {
        // Group 1 part.
        run_group_parts_test(&[(1, 0, 2000)], &[(vec![1], 0, 2000)]);

        // 1, 2, 3, 4 => [3, 1, 4], [2]
        run_group_parts_test(
            &[
                (1, 1000, 2000),
                (2, 6000, 7000),
                (3, 0, 1500),
                (4, 1500, 3000),
            ],
            &[(vec![3, 1, 4], 0, 3000), (vec![2], 6000, 7000)],
        );

        // 1, 2, 3 => [3], [1], [2],
        run_group_parts_test(
            &[(1, 3000, 4000), (2, 4001, 6000), (3, 0, 1000)],
            &[
                (vec![3], 0, 1000),
                (vec![1], 3000, 4000),
                (vec![2], 4001, 6000),
            ],
        );
    }
}
