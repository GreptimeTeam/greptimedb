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

use std::sync::Arc;

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::SendableRecordBatchStream;
use common_time::range::TimestampRange;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::ScanRequest;
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::MemtableRef;
use crate::read::merge::MergeReaderBuilder;
use crate::read::stream::{ProjectionMapper, StreamImpl};
use crate::read::BatchReader;
use crate::sst::file::FileHandle;
use crate::sst::parquet::reader::ParquetReaderBuilder;

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary key, time index`.
pub struct SeqScan {
    /// Directory of SST files.
    file_dir: String,
    /// Object store that stores SST files.
    object_store: ObjectStore,
    /// Maps projected Batches to RecordBatches.
    mapper: Arc<ProjectionMapper>,
    /// Original scan request to scan memtable.
    // TODO(yingwen): Remove this if memtable::iter() takes another struct.
    request: ScanRequest,

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
    #[must_use]
    pub(crate) fn new(
        file_dir: String,
        object_store: ObjectStore,
        mapper: ProjectionMapper,
        request: ScanRequest,
    ) -> SeqScan {
        SeqScan {
            file_dir,
            object_store,
            mapper: Arc::new(mapper),
            time_range: None,
            predicate: None,
            memtables: Vec::new(),
            files: Vec::new(),
            request,
        }
    }

    /// Set time range filter for time index.
    #[must_use]
    pub(crate) fn with_time_range(mut self, time_range: Option<TimestampRange>) -> Self {
        self.time_range = time_range;
        self
    }

    /// Set predicate to push down.
    #[must_use]
    pub(crate) fn with_predicate(mut self, predicate: Option<Predicate>) -> Self {
        self.predicate = predicate;
        self
    }

    /// Set memtables to read.
    #[must_use]
    pub(crate) fn with_memtables(mut self, memtables: Vec<MemtableRef>) -> Self {
        self.memtables = memtables;
        self
    }

    /// Set files to read.
    #[must_use]
    pub(crate) fn with_files(mut self, files: Vec<FileHandle>) -> Self {
        self.files = files;
        self
    }

    /// Builds a stream for the query.
    #[must_use]
    pub async fn build(&self) -> Result<SendableRecordBatchStream> {
        // Scans all memtables and SSTs. Builds a merge reader to merge results.
        let mut builder = MergeReaderBuilder::new();
        for mem in &self.memtables {
            let iter = mem.iter(self.request.clone());
            builder.push_batch_iter(iter);
        }
        for file in &self.files {
            let reader = ParquetReaderBuilder::new(
                self.file_dir.clone(),
                file.clone(),
                self.object_store.clone(),
            )
            .predicate(self.predicate.clone())
            .time_range(self.time_range.clone())
            .projection(Some(self.mapper.column_ids().to_vec()))
            .build()
            .await?;
            builder.push_batch_reader(Box::new(reader));
        }
        let mut reader = builder.build().await?;
        // Creates a stream to poll the batch reader and convert batch into record batch.
        let mapper = self.mapper.clone();
        let stream = try_stream! {
            while let Some(batch) = reader.next_batch().await.map_err(BoxedError::new).context(ExternalSnafu)? {
                yield mapper.convert(&batch)?;
            }
        };
        let stream = Box::pin(StreamImpl::new(
            Box::pin(stream),
            self.mapper.output_schema(),
        ));

        Ok(stream)
    }
}
