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

pub mod builder;

use std::collections::BTreeSet;

use futures::{AsyncRead, AsyncSeek};
use index::inverted_index::format::reader::InvertedIndexBlobReader;
use index::inverted_index::search::index_apply::{
    IndexApplier, IndexNotFoundStrategy, SearchContext,
};
use object_store::ObjectStore;
use puffin::file_format::reader::{PuffinAsyncReader, PuffinFileReader};
use snafu::{OptionExt, ResultExt};

use crate::error::{
    ApplyIndexSnafu, PuffinBlobTypeNotFoundSnafu, PuffinReadBlobSnafu, PuffinReadMetadataSnafu,
    Result,
};
use crate::metrics::{
    INDEX_APPLY_ELAPSED, INDEX_APPLY_MEMORY_USAGE, INDEX_PUFFIN_READ_BYTES_TOTAL,
    INDEX_PUFFIN_READ_OP_TOTAL, INDEX_PUFFIN_SEEK_OP_TOTAL,
};
use crate::sst::file::FileId;
use crate::sst::index::store::InstrumentedStore;
use crate::sst::index::INDEX_BLOB_TYPE;
use crate::sst::location;

/// The [`SstIndexApplier`] is responsible for applying predicates to the provided SST files
/// and returning the relevant row group ids for further scan.
pub struct SstIndexApplier {
    /// The root directory of the region.
    region_dir: String,

    /// Store responsible for accessing SST files.
    store: InstrumentedStore,

    /// Predefined index applier used to apply predicates to index files
    /// and return the relevant row group ids for further scan.
    index_applier: Box<dyn IndexApplier>,
}

impl SstIndexApplier {
    /// Creates a new [`SstIndexApplier`].
    pub fn new(
        region_dir: String,
        object_store: ObjectStore,
        index_applier: Box<dyn IndexApplier>,
    ) -> Self {
        INDEX_APPLY_MEMORY_USAGE.add(index_applier.memory_usage() as i64);

        Self {
            region_dir,
            store: InstrumentedStore::new(object_store),
            index_applier,
        }
    }

    /// Applies predicates to the provided SST file id and returns the relevant row group ids
    pub async fn apply(&self, file_id: FileId) -> Result<BTreeSet<usize>> {
        let _timer = INDEX_APPLY_ELAPSED.start_timer();

        let mut puffin_reader = self.puffin_reader(file_id).await?;
        let blob_reader = Self::index_blob_reader(&mut puffin_reader).await?;
        let mut index_reader = InvertedIndexBlobReader::new(blob_reader);

        let context = SearchContext {
            // Encountering a non-existing column indicates that it doesn't match predicates.
            index_not_found_strategy: IndexNotFoundStrategy::ReturnEmpty,
        };
        self.index_applier
            .apply(context, &mut index_reader)
            .await
            .context(ApplyIndexSnafu)
    }

    /// Helper function to create a [`PuffinFileReader`] for the provided SST file id.
    async fn puffin_reader(
        &self,
        file_id: FileId,
    ) -> Result<PuffinFileReader<impl AsyncRead + AsyncSeek>> {
        let file_path = location::index_file_path(&self.region_dir, file_id);
        let file_reader = self
            .store
            .reader(
                &file_path,
                &INDEX_PUFFIN_READ_BYTES_TOTAL,
                &INDEX_PUFFIN_READ_OP_TOTAL,
                &INDEX_PUFFIN_SEEK_OP_TOTAL,
            )
            .await?;
        Ok(PuffinFileReader::new(file_reader))
    }

    /// Helper function to create a [`PuffinBlobReader`] for the index blob of the provided index file reader.
    async fn index_blob_reader(
        puffin_reader: &mut PuffinFileReader<impl AsyncRead + AsyncSeek + Unpin + Send>,
    ) -> Result<impl AsyncRead + AsyncSeek + '_> {
        let file_meta = puffin_reader
            .metadata()
            .await
            .context(PuffinReadMetadataSnafu)?;
        let blob_meta = file_meta
            .blobs
            .iter()
            .find(|blob| blob.blob_type == INDEX_BLOB_TYPE)
            .context(PuffinBlobTypeNotFoundSnafu {
                blob_type: INDEX_BLOB_TYPE,
            })?;
        puffin_reader
            .blob_reader(blob_meta)
            .context(PuffinReadBlobSnafu)
    }
}

impl Drop for SstIndexApplier {
    fn drop(&mut self) {
        INDEX_APPLY_MEMORY_USAGE.sub(self.index_applier.memory_usage() as i64);
    }
}

#[cfg(test)]
mod tests {
    use futures::io::Cursor;
    use index::inverted_index::search::index_apply::MockIndexApplier;
    use object_store::services::Memory;
    use puffin::file_format::writer::{Blob, PuffinAsyncWriter, PuffinFileWriter};

    use super::*;
    use crate::error::Error;

    #[tokio::test]
    async fn test_index_applier_apply_basic() {
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let file_id = FileId::random();
        let region_dir = "region_dir".to_string();
        let path = location::index_file_path(&region_dir, file_id);

        let mut puffin_writer = PuffinFileWriter::new(object_store.writer(&path).await.unwrap());
        puffin_writer
            .add_blob(Blob {
                blob_type: INDEX_BLOB_TYPE.to_string(),
                data: Cursor::new(vec![]),
                properties: Default::default(),
            })
            .await
            .unwrap();
        puffin_writer.finish().await.unwrap();

        let mut mock_index_applier = MockIndexApplier::new();
        mock_index_applier.expect_memory_usage().returning(|| 100);
        mock_index_applier
            .expect_apply()
            .returning(|_, _| Ok(BTreeSet::from_iter([1, 2, 3])));

        let sst_index_applier = SstIndexApplier::new(
            region_dir.clone(),
            object_store,
            Box::new(mock_index_applier),
        );
        let ids = sst_index_applier.apply(file_id).await.unwrap();
        assert_eq!(ids, BTreeSet::from_iter([1, 2, 3]));
    }

    #[tokio::test]
    async fn test_index_applier_apply_invalid_blob_type() {
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let file_id = FileId::random();
        let region_dir = "region_dir".to_string();
        let path = location::index_file_path(&region_dir, file_id);

        let mut puffin_writer = PuffinFileWriter::new(object_store.writer(&path).await.unwrap());
        puffin_writer
            .add_blob(Blob {
                blob_type: "invalid_blob_type".to_string(),
                data: Cursor::new(vec![]),
                properties: Default::default(),
            })
            .await
            .unwrap();
        puffin_writer.finish().await.unwrap();

        let mut mock_index_applier = MockIndexApplier::new();
        mock_index_applier.expect_memory_usage().returning(|| 100);
        mock_index_applier.expect_apply().never();

        let sst_index_applier = SstIndexApplier::new(
            region_dir.clone(),
            object_store,
            Box::new(mock_index_applier),
        );
        let res = sst_index_applier.apply(file_id).await;
        assert!(matches!(res, Err(Error::PuffinBlobTypeNotFound { .. })));
    }
}
