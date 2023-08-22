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

use common_telemetry::debug;
use common_time::range::TimestampRange;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;
use store_api::storage::{ColumnId, ScanRequest};
use table::predicate::{Predicate, TimeRangePredicateBuilder};

use crate::error::{BuildPredicateSnafu, Result};
use crate::read::seq_scan::SeqScan;
use crate::region::version::VersionRef;
use crate::sst::file::FileHandle;

/// Helper to scans a region by [ScanRequest].
pub(crate) struct ScanRegion {
    /// Version of the region at scan.
    version: VersionRef,
    /// Directory of SST files.
    file_dir: String,
    /// Object store that stores SST files.
    object_store: ObjectStore,
    /// Scan request.
    request: ScanRequest,
}

impl ScanRegion {
    /// Creates a [ScanRegion].
    pub(crate) fn new(
        version: VersionRef,
        file_dir: String,
        object_store: ObjectStore,
        request: ScanRequest,
    ) -> ScanRegion {
        ScanRegion {
            version,
            file_dir,
            object_store,
            request,
        }
    }

    /// Scan sequentailly.
    pub(crate) fn seq_scan(&self) -> Result<SeqScan> {
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

        debug!(
            "Seq scan region {}, memtables: {}, ssts_to_read: {}, total_ssts: {}",
            self.version.metadata.region_id,
            memtables.len(),
            files.len(),
            total_ssts
        );

        let predicate = Predicate::try_new(
            self.request.filters.clone(),
            self.version.metadata.schema.clone(),
        )
        .context(BuildPredicateSnafu)?;
        let projection = self
            .request
            .projection
            .as_ref()
            .map(|p| projection_indices_to_ids(&self.version.metadata, p))
            .transpose()?;

        let seq_scan = SeqScan::new(
            self.version.metadata.clone(),
            &self.file_dir,
            self.object_store.clone(),
        )
        .with_projection(projection)
        .with_time_range(Some(time_range))
        .with_predicate(Some(predicate))
        .with_memtables(memtables)
        .with_files(files);

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

// TODO(yingwen): Remove this once scan
/// Map projection indices to column ids.
fn projection_indices_to_ids(
    metadata: &RegionMetadata,
    projection: &[usize],
) -> Result<Vec<ColumnId>> {
    todo!()
}
