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

//! Provides a builder to build a stream to query the engine.

use common_query::logical_plan::Expr;
use common_recordbatch::{OrderOption, SendableRecordBatchStream};
use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::memtable::MemtableRef;
use crate::sst::file::FileHandle;

/// Builder to construct a [SendableRecordBatchStream] for a query.
pub struct RecordBatchStreamBuilder {
    /// Metadata of the region when the builder created.
    metadata: RegionMetadataRef,
    /// Projection to push down.
    projection: Option<Vec<ColumnId>>,
    /// Filters to push down.
    filters: Vec<Expr>,
    /// Required output ordering.
    output_ordering: Option<Vec<OrderOption>>,
    /// Memtables to scan.
    memtables: Vec<MemtableRef>,
    /// Handles to SST files to scan.
    files: Vec<FileHandle>,
    /// Directory of SST files.
    file_dir: String,
    /// Object store that stores SST files.
    object_store: ObjectStore,
}

impl RecordBatchStreamBuilder {
    /// Creates a new builder.
    pub fn new(
        metadata: RegionMetadataRef,
        file_dir: &str,
        object_store: ObjectStore,
    ) -> RecordBatchStreamBuilder {
        RecordBatchStreamBuilder {
            metadata,
            projection: None,
            filters: Vec::new(),
            output_ordering: None,
            memtables: Vec::new(),
            files: Vec::new(),
            file_dir: file_dir.to_string(),
            object_store,
        }
    }

    /// Pushes down projection
    pub fn projection(&mut self, projection: Option<Vec<ColumnId>>) -> &mut Self {
        self.projection = projection;
        self
    }

    /// Pushes down filters.
    pub fn filters(&mut self, filters: Vec<Expr>) -> &mut Self {
        self.filters = filters;
        self
    }

    /// Set required output ordering.
    pub fn output_ordering(&mut self, ordering: Option<Vec<OrderOption>>) -> &mut Self {
        self.output_ordering = ordering;
        self
    }

    /// Set memtables to read.
    pub fn memtables(&mut self, memtables: Vec<MemtableRef>) -> &mut Self {
        self.memtables = memtables;
        self
    }

    /// Set files to read.
    pub fn files(&mut self, files: Vec<FileHandle>) -> &mut Self {
        self.files = files;
        self
    }

    /// Builds a stream for the query.
    pub fn build(&mut self) -> SendableRecordBatchStream {
        unimplemented!()
    }
}
