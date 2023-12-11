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

//! Scans a region according to the scan request.

use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::debug;
use common_time::range::TimestampRange;
use store_api::storage::ScanRequest;
use table::predicate::{Predicate, TimeRangePredicateBuilder};

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::error::Result;
use crate::read::projection::ProjectionMapper;
use crate::read::seq_scan::SeqScan;
use crate::region::version::VersionRef;
use crate::sst::file::FileHandle;

/// A scanner scans a region and returns a [SendableRecordBatchStream].
pub(crate) enum Scanner {
    /// Sequential scan.
    Seq(SeqScan),
    // TODO(yingwen): Support windowed scan and chained scan.
}

impl Scanner {
    /// Returns a [SendableRecordBatchStream] to retrieve scan results.
    pub(crate) async fn scan(&self) -> Result<SendableRecordBatchStream> {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.build_stream().await,
        }
    }
}

#[cfg(test)]
impl Scanner {
    /// Returns number of files to scan.
    pub(crate) fn num_files(&self) -> usize {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.num_files(),
        }
    }

    /// Returns number of memtables to scan.
    pub(crate) fn num_memtables(&self) -> usize {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.num_memtables(),
        }
    }

    /// Returns SST file ids to scan.
    pub(crate) fn file_ids(&self) -> Vec<crate::sst::file::FileId> {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.file_ids(),
        }
    }
}

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Helper to scans a region by [ScanRequest].
///
/// [ScanRegion] collects SSTs and memtables to scan without actually reading them. It
/// creates a [Scanner] to actually scan these targets in [Scanner::scan()].
///
/// ```mermaid
/// classDiagram
/// class ScanRegion {
///     -VersionRef version
///     -ScanRequest request
///     ~scanner() Scanner
///     ~seq_scan() SeqScan
/// }
/// class Scanner {
///     <<enumeration>>
///     SeqScan
///     +scan() SendableRecordBatchStream
/// }
/// class SeqScan {
///     -ProjectionMapper mapper
///     -Option~TimeRange~ time_range
///     -Option~Predicate~ predicate
///     -Vec~MemtableRef~ memtables
///     -Vec~FileHandle~ files
///     +build() SendableRecordBatchStream
/// }
/// class ProjectionMapper {
///     ~output_schema() SchemaRef
///     ~convert(Batch) RecordBatch
/// }
/// ScanRegion -- Scanner
/// ScanRegion o-- ScanRequest
/// Scanner o-- SeqScan
/// Scanner -- SendableRecordBatchStream
/// SeqScan o-- ProjectionMapper
/// SeqScan -- SendableRecordBatchStream
/// ```
pub(crate) struct ScanRegion {
    /// Version of the region at scan.
    version: VersionRef,
    /// Access layer of the region.
    access_layer: AccessLayerRef,
    /// Scan request.
    request: ScanRequest,
    /// Cache.
    cache_manager: Option<CacheManagerRef>,
    /// Parallelism to scan.
    parallelism: ScanParallism,
}

impl ScanRegion {
    /// Creates a [ScanRegion].
    pub(crate) fn new(
        version: VersionRef,
        access_layer: AccessLayerRef,
        request: ScanRequest,
        cache_manager: Option<CacheManagerRef>,
    ) -> ScanRegion {
        ScanRegion {
            version,
            access_layer,
            request,
            cache_manager,
            parallelism: ScanParallism::default(),
        }
    }

    /// Sets parallelism.
    #[must_use]
    pub(crate) fn with_parallelism(mut self, parallelism: ScanParallism) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Returns a [Scanner] to scan the region.
    pub(crate) fn scanner(self) -> Result<Scanner> {
        self.seq_scan().map(Scanner::Seq)
    }

    /// Scan sequentially.
    pub(crate) fn seq_scan(self) -> Result<SeqScan> {
        let time_range = self.build_time_range_predicate();

        let ssts = &self.version.ssts;
        let mut total_ssts = 0;
        let mut files = Vec::new();
        for level in ssts.levels() {
            total_ssts += level.files.len();

            for file in level.files.values() {
                // Finds SST files in range.
                if file_in_range(file, &time_range) {
                    files.push(file.clone());
                }
            }
        }

        let memtables = self.version.memtables.list_memtables();
        // Skip empty memtables and memtables out of time range.
        let memtables: Vec<_> = memtables
            .into_iter()
            .filter(|mem| {
                if mem.is_empty() {
                    return false;
                }
                let stats = mem.stats();
                let Some((start, end)) = stats.time_range() else {
                    return true;
                };

                // The time range of the memtable is inclusive.
                let memtable_range = TimestampRange::new_inclusive(Some(start), Some(end));
                memtable_range.intersects(&time_range)
            })
            .collect();

        debug!(
            "Seq scan region {}, request: {:?}, memtables: {}, ssts_to_read: {}, total_ssts: {}",
            self.version.metadata.region_id,
            self.request,
            memtables.len(),
            files.len(),
            total_ssts
        );

        let predicate = Predicate::new(self.request.filters.clone());
        // The mapper always computes projected column ids as the schema of SSTs may change.
        let mapper = match &self.request.projection {
            Some(p) => ProjectionMapper::new(&self.version.metadata, p.iter().copied())?,
            None => ProjectionMapper::all(&self.version.metadata)?,
        };

        let seq_scan = SeqScan::new(self.access_layer.clone(), mapper)
            .with_time_range(Some(time_range))
            .with_predicate(Some(predicate))
            .with_memtables(memtables)
            .with_files(files)
            .with_cache(self.cache_manager)
            .with_parallelism(self.parallelism);

        Ok(seq_scan)
    }

    /// Build time range predicate from filters.
    fn build_time_range_predicate(&self) -> TimestampRange {
        let time_index = self.version.metadata.time_index_column();
        let unit = time_index
            .column_schema
            .data_type
            .as_timestamp()
            .expect("Time index must have timestamp-compatible type")
            .unit();
        TimeRangePredicateBuilder::new(&time_index.column_schema.name, unit, &self.request.filters)
            .build()
    }
}

/// Config for parallel scan.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ScanParallism {
    /// Number of tasks expect to spawn to read data.
    pub(crate) parallelism: usize,
    /// Channel size to send batches. Only takes effect when the parallelism > 1.
    pub(crate) channel_size: usize,
}

impl ScanParallism {
    /// Returns true if we allow parallel scan.
    pub(crate) fn allow_parallel_scan(&self) -> bool {
        self.parallelism > 1
    }
}

/// Returns true if the time range of a SST `file` matches the `predicate`.
fn file_in_range(file: &FileHandle, predicate: &TimestampRange) -> bool {
    if predicate == &TimestampRange::min_to_max() {
        return true;
    }
    // end timestamp of a SST is inclusive.
    let (start, end) = file.time_range();
    let file_ts_range = TimestampRange::new_inclusive(Some(start), Some(end));
    file_ts_range.intersects(predicate)
}
