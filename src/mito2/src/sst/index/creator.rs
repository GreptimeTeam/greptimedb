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

mod statistics;
mod temp_provider;

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use common_telemetry::warn;
use index::inverted_index::create::sort::external_sort::ExternalSorter;
use index::inverted_index::create::sort_create::SortIndexCreator;
use index::inverted_index::create::InvertedIndexCreator;
use index::inverted_index::format::writer::InvertedIndexBlobWriter;
use object_store::ObjectStore;
use puffin::file_format::writer::{Blob, PuffinAsyncWriter, PuffinFileWriter};
use snafu::{ensure, ResultExt};
use store_api::metadata::RegionMetadataRef;
use tokio::io::duplex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::error::{
    BiSnafu, IndexFinishSnafu, OperateAbortedIndexSnafu, PuffinAddBlobSnafu, PuffinFinishSnafu,
    PushIndexValueSnafu, Result,
};
use crate::metrics::{
    INDEX_PUFFIN_FLUSH_OP_TOTAL, INDEX_PUFFIN_WRITE_BYTES_TOTAL, INDEX_PUFFIN_WRITE_OP_TOTAL,
};
use crate::read::Batch;
use crate::sst::file::FileId;
use crate::sst::index::codec::{ColumnId, IndexValueCodec, IndexValuesCodec};
use crate::sst::index::creator::statistics::Statistics;
use crate::sst::index::creator::temp_provider::TempFileProvider;
use crate::sst::index::intermediate::{IntermediateLocation, IntermediateManager};
use crate::sst::index::store::InstrumentedStore;
use crate::sst::index::{
    INDEX_BLOB_TYPE, MIN_MEMORY_USAGE_THRESHOLD, PIPE_BUFFER_SIZE_FOR_SENDING_BLOB,
};

type ByteCount = usize;
type RowCount = usize;

/// Creates SST index.
pub struct SstIndexCreator {
    /// Path of index file to write.
    file_path: String,
    /// The store to write index files.
    store: InstrumentedStore,
    /// The index creator.
    index_creator: Box<dyn InvertedIndexCreator>,
    /// The provider of intermediate files.
    temp_file_provider: Arc<TempFileProvider>,

    /// Codec for decoding primary keys.
    codec: IndexValuesCodec,
    /// Reusable buffer for encoding index values.
    value_buf: Vec<u8>,

    /// Statistics of index creation.
    stats: Statistics,
    /// Whether the index creation is aborted.
    aborted: bool,

    /// Ignore column IDs for index creation.
    ignore_column_ids: HashSet<ColumnId>,
}

impl SstIndexCreator {
    /// Creates a new `SstIndexCreator`.
    /// Should ensure that the number of tag columns is greater than 0.
    pub fn new(
        file_path: String,
        sst_file_id: FileId,
        metadata: &RegionMetadataRef,
        index_store: ObjectStore,
        intermediate_manager: IntermediateManager,
        memory_usage_threshold: Option<usize>,
        segment_row_count: NonZeroUsize,
    ) -> Self {
        // `memory_usage_threshold` is the total memory usage threshold of the index creation,
        // so we need to divide it by the number of columns
        let memory_threshold = memory_usage_threshold.map(|threshold| {
            (threshold / metadata.primary_key.len()).max(MIN_MEMORY_USAGE_THRESHOLD)
        });
        let temp_file_provider = Arc::new(TempFileProvider::new(
            IntermediateLocation::new(&metadata.region_id, &sst_file_id),
            intermediate_manager,
        ));
        let sorter = ExternalSorter::factory(temp_file_provider.clone() as _, memory_threshold);
        let index_creator = Box::new(SortIndexCreator::new(sorter, segment_row_count));

        let codec = IndexValuesCodec::from_tag_columns(metadata.primary_key_columns());
        Self {
            file_path,
            store: InstrumentedStore::new(index_store),
            codec,
            index_creator,
            temp_file_provider,

            value_buf: vec![],

            stats: Statistics::default(),
            aborted: false,

            ignore_column_ids: HashSet::default(),
        }
    }

    /// Sets the write buffer size of the store.
    pub fn with_buffer_size(mut self, write_buffer_size: Option<usize>) -> Self {
        self.store = self.store.with_write_buffer_size(write_buffer_size);
        self
    }

    /// Sets the ignore column IDs for index creation.
    pub fn with_ignore_column_ids(mut self, ignore_column_ids: HashSet<ColumnId>) -> Self {
        self.ignore_column_ids = ignore_column_ids;
        self
    }

    /// Updates index with a batch of rows.
    /// Garbage will be cleaned up if failed to update.
    pub async fn update(&mut self, batch: &Batch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if batch.is_empty() {
            return Ok(());
        }

        if let Err(update_err) = self.do_update(batch).await {
            // clean up garbage if failed to update
            if let Err(err) = self.do_cleanup().await {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to clean up index creator, file_path: {}, err: {}",
                        self.file_path, err
                    );
                } else {
                    warn!(err; "Failed to clean up index creator, file_path: {}", self.file_path);
                }
            }
            return Err(update_err);
        }

        Ok(())
    }

    /// Finishes index creation and cleans up garbage.
    /// Returns the number of rows and bytes written.
    pub async fn finish(&mut self) -> Result<(RowCount, ByteCount)> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if self.stats.row_count() == 0 {
            // no IO is performed, no garbage to clean up, just return
            return Ok((0, 0));
        }

        let finish_res = self.do_finish().await;
        // clean up garbage no matter finish successfully or not
        if let Err(err) = self.do_cleanup().await {
            if cfg!(any(test, feature = "test")) {
                panic!(
                    "Failed to clean up index creator, file_path: {}, err: {}",
                    self.file_path, err
                );
            } else {
                warn!(err; "Failed to clean up index creator, file_path: {}", self.file_path);
            }
        }

        finish_res.map(|_| (self.stats.row_count(), self.stats.byte_count()))
    }

    /// Aborts index creation and clean up garbage.
    pub async fn abort(&mut self) -> Result<()> {
        if self.aborted {
            return Ok(());
        }
        self.aborted = true;

        self.do_cleanup().await
    }

    async fn do_update(&mut self, batch: &Batch) -> Result<()> {
        let mut guard = self.stats.record_update();

        let n = batch.num_rows();
        guard.inc_row_count(n);

        for (column_id, field, value) in self.codec.decode(batch.primary_key())? {
            if self.ignore_column_ids.contains(column_id) {
                continue;
            }

            if let Some(value) = value.as_ref() {
                self.value_buf.clear();
                IndexValueCodec::encode_value(value.as_value_ref(), field, &mut self.value_buf)?;
            }

            // non-null value -> Some(encoded_bytes), null value -> None
            let value = value.is_some().then_some(self.value_buf.as_slice());
            self.index_creator
                .push_with_name_n(column_id, value, n)
                .await
                .context(PushIndexValueSnafu)?;
        }

        Ok(())
    }

    /// Data flow of finishing index:
    ///
    /// ```text
    ///                               (In Memory Buffer)
    ///                                    ┌──────┐
    ///  ┌─────────────┐                   │ PIPE │
    ///  │             │ write index data  │      │
    ///  │ IndexWriter ├──────────────────►│ tx   │
    ///  │             │                   │      │
    ///  └─────────────┘                   │      │
    ///                  ┌─────────────────┤ rx   │
    ///  ┌─────────────┐ │ read as blob    └──────┘
    ///  │             │ │
    ///  │ PuffinWriter├─┤
    ///  │             │ │ copy to file    ┌──────┐
    ///  └─────────────┘ └────────────────►│ File │
    ///                                    └──────┘
    /// ```
    async fn do_finish(&mut self) -> Result<()> {
        let mut guard = self.stats.record_finish();

        let file_writer = self
            .store
            .writer(
                &self.file_path,
                &INDEX_PUFFIN_WRITE_BYTES_TOTAL,
                &INDEX_PUFFIN_WRITE_OP_TOTAL,
                &INDEX_PUFFIN_FLUSH_OP_TOTAL,
            )
            .await?;
        let mut puffin_writer = PuffinFileWriter::new(file_writer);

        let (tx, rx) = duplex(PIPE_BUFFER_SIZE_FOR_SENDING_BLOB);
        let blob = Blob {
            blob_type: INDEX_BLOB_TYPE.to_string(),
            data: rx.compat(),
            properties: HashMap::default(),
        };
        let mut index_writer = InvertedIndexBlobWriter::new(tx.compat_write());

        let (index_finish, puffin_add_blob) = futures::join!(
            self.index_creator.finish(&mut index_writer),
            puffin_writer.add_blob(blob)
        );

        match (
            puffin_add_blob.context(PuffinAddBlobSnafu),
            index_finish.context(IndexFinishSnafu),
        ) {
            (Err(e1), Err(e2)) => BiSnafu {
                first: Box::new(e1),
                second: Box::new(e2),
            }
            .fail()?,

            (Ok(_), e @ Err(_)) => e?,
            (e @ Err(_), Ok(_)) => e?,
            _ => {}
        }

        let byte_count = puffin_writer.finish().await.context(PuffinFinishSnafu)?;
        guard.inc_byte_count(byte_count);
        Ok(())
    }

    async fn do_cleanup(&mut self) -> Result<()> {
        let _guard = self.stats.record_cleanup();

        self.temp_file_provider.cleanup().await
    }
}

#[cfg(test)]
mod tests {
    // TODO(zhongzc): This PR has grown quite large, and the SstIndexCreator deserves
    // a significant number of unit tests. These unit tests are substantial enough to
    // make up a large PR on their own. I will bring them in with the next PR.
}
