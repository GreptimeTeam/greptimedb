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

use common_recordbatch::SendableRecordBatchStream;
use common_time::range::TimestampRange;
use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::memtable::MemtableRef;
use crate::sst::file::FileHandle;

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary key, time index`.
pub struct SeqScan {
    /// Metadata of the region to scan.
    metadata: RegionMetadataRef,
    /// Directory of SST files.
    file_dir: String,
    /// Object store that stores SST files.
    object_store: ObjectStore,

    /// Projection to push down.
    projection: Option<Vec<ColumnId>>,
    /// Time range filter for time index.
    time_range: Option<TimestampRange>,
    /// Predicate to push down.
    predicate: Option<Predicate>,
    /// Memtables to scan.
    memtables: Vec<MemtableRef>,
    /// Handles to SST files to scan.
    files: Vec<FileHandle>,
}

impl SeqScan {
    /// Creates a new [SeqScan].
    pub fn new(metadata: RegionMetadataRef, file_dir: &str, object_store: ObjectStore) -> SeqScan {
        SeqScan {
            metadata,
            file_dir: file_dir.to_string(),
            object_store,
            projection: None,
            time_range: None,
            predicate: None,
            memtables: Vec::new(),
            files: Vec::new(),
        }
    }

    /// Set projection.
    pub fn with_projection(mut self, projection: Option<Vec<ColumnId>>) -> Self {
        self.projection = projection;
        self
    }

    /// Set time range filter for time index.
    pub fn with_time_range(mut self, time_range: Option<TimestampRange>) -> Self {
        self.time_range = time_range;
        self
    }

    /// Set predicate to push down.
    pub fn with_predicate(mut self, predicate: Option<Predicate>) -> Self {
        self.predicate = predicate;
        self
    }

    /// Set memtables to read.
    pub fn with_memtables(mut self, memtables: Vec<MemtableRef>) -> Self {
        self.memtables = memtables;
        self
    }

    /// Set files to read.
    pub fn with_files(mut self, files: Vec<FileHandle>) -> Self {
        self.files = files;
        self
    }

    /// Builds a stream for the query.
    pub fn build(&self) -> SendableRecordBatchStream {
        unimplemented!()
    }
}
