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
use common_recordbatch::{RecordBatchStreamAdaptor, SendableRecordBatchStream};
use common_time::range::TimestampRange;
use snafu::ResultExt;
use table::predicate::Predicate;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::error::Result;
use crate::memtable::MemtableRef;
use crate::read::compat::{self, CompatReader};
use crate::read::merge::MergeReaderBuilder;
use crate::read::projection::ProjectionMapper;
use crate::read::{BatchReader, BoxedBatchReader};
use crate::sst::file::FileHandle;

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary key, time index`.
pub struct SeqScan {
    /// Region SST access layer.
    access_layer: AccessLayerRef,
    /// Maps projected Batches to RecordBatches.
    mapper: Arc<ProjectionMapper>,
    /// Time range filter for time index.
    time_range: Option<TimestampRange>,
    /// Predicate to push down.
    predicate: Option<Predicate>,
    /// Memtables to scan.
    memtables: Vec<MemtableRef>,
    /// Handles to SST files to scan.
    files: Vec<FileHandle>,
    /// Cache.
    cache_manager: Option<CacheManagerRef>,
}

impl SeqScan {
    /// Creates a new [SeqScan].
    #[must_use]
    pub(crate) fn new(access_layer: AccessLayerRef, mapper: ProjectionMapper) -> SeqScan {
        SeqScan {
            access_layer,
            mapper: Arc::new(mapper),
            time_range: None,
            predicate: None,
            memtables: Vec::new(),
            files: Vec::new(),
            cache_manager: None,
        }
    }

    /// Sets time range filter for time index.
    #[must_use]
    pub(crate) fn with_time_range(mut self, time_range: Option<TimestampRange>) -> Self {
        self.time_range = time_range;
        self
    }

    /// Sets predicate to push down.
    #[must_use]
    pub(crate) fn with_predicate(mut self, predicate: Option<Predicate>) -> Self {
        self.predicate = predicate;
        self
    }

    /// Sets memtables to read.
    #[must_use]
    pub(crate) fn with_memtables(mut self, memtables: Vec<MemtableRef>) -> Self {
        self.memtables = memtables;
        self
    }

    /// Sets files to read.
    #[must_use]
    pub(crate) fn with_files(mut self, files: Vec<FileHandle>) -> Self {
        self.files = files;
        self
    }

    /// Sets cache for this query.
    pub(crate) fn with_cache(mut self, cache: Option<CacheManagerRef>) -> Self {
        self.cache_manager = cache;
        self
    }

    /// Builds a stream for the query.
    pub async fn build_stream(&self) -> Result<SendableRecordBatchStream> {
        // Scans all memtables and SSTs. Builds a merge reader to merge results.
        let mut reader = self.build_reader().await?;

        // Creates a stream to poll the batch reader and convert batch into record batch.
        let mapper = self.mapper.clone();
        let cache_manager = self.cache_manager.clone();
        let stream = try_stream! {
            let cache = cache_manager.as_ref().map(|cache| cache.as_ref());
            while let Some(batch) = reader
                .next_batch()
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
            {
                yield mapper.convert(&batch, cache)?;
            }
        };
        let stream = Box::pin(RecordBatchStreamAdaptor::new(
            self.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }

    /// Builds a [BoxedBatchReader] from sequential scan.
    pub async fn build_reader(&self) -> Result<BoxedBatchReader> {
        // Scans all memtables and SSTs. Builds a merge reader to merge results.
        let mut builder = MergeReaderBuilder::new();
        for mem in &self.memtables {
            // TODO(hl): pass filters once memtable supports filter pushdown.
            let iter = mem.iter(Some(self.mapper.column_ids()), self.predicate.clone());
            builder.push_batch_iter(iter);
        }
        for file in &self.files {
            let reader = self
                .access_layer
                .read_sst(file.clone())
                .predicate(self.predicate.clone())
                .time_range(self.time_range)
                .projection(Some(self.mapper.column_ids().to_vec()))
                .cache(self.cache_manager.clone())
                .build()
                .await?;
            if compat::has_same_columns(self.mapper.metadata(), reader.metadata()) {
                builder.push_batch_reader(Box::new(reader));
            } else {
                // They have different schema. We need to adapt the batch first so the
                // mapper can convert the it.
                let compat_reader =
                    CompatReader::new(&self.mapper, reader.metadata().clone(), reader)?;
                builder.push_batch_reader(Box::new(compat_reader));
            }
        }
        Ok(Box::new(builder.build().await?))
    }
}

#[cfg(test)]
impl SeqScan {
    /// Returns number of memtables to scan.
    pub(crate) fn num_memtables(&self) -> usize {
        self.memtables.len()
    }

    /// Returns number of SST files to scan.
    pub(crate) fn num_files(&self) -> usize {
        self.files.len()
    }

    /// Returns SST file ids to scan.
    pub(crate) fn file_ids(&self) -> Vec<crate::sst::file::FileId> {
        self.files.iter().map(|file| file.file_id()).collect()
    }
}
