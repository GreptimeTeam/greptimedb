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

use std::sync::Arc;
use std::time::Instant;

use futures::{AsyncRead, AsyncSeek};
use index::inverted_index::format::reader::cache::{
    CachedInvertedIndexBlobReader, InvertedIndexCache,
};
use index::inverted_index::format::reader::InvertedIndexBlobReader;
use index::inverted_index::metrics::INDEX_CACHE_STATS;
use index::inverted_index::search::index_apply::{
    ApplyOutput, IndexApplier, IndexNotFoundStrategy, SearchContext,
};
use object_store::ObjectStore;
use puffin::file_format::reader::{PuffinAsyncReader, PuffinFileReader};
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
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
pub(crate) struct SstIndexApplier {
    /// The root directory of the region.
    region_dir: String,

    /// Region ID.
    region_id: RegionId,

    /// Store responsible for accessing remote index files.
    store: InstrumentedStore,

    /// The cache of index files.
    file_cache: Option<FileCacheRef>,

    /// Predefined index applier used to apply predicates to index files
    /// and return the relevant row group ids for further scan.
    index_applier: Box<dyn IndexApplier>,

    index_cache: Option<Arc<InvertedIndexCache>>,
}

pub(crate) type SstIndexApplierRef = Arc<SstIndexApplier>;

impl SstIndexApplier {
    /// Creates a new [`SstIndexApplier`].
    pub fn new(
        region_dir: String,
        region_id: RegionId,
        object_store: ObjectStore,
        file_cache: Option<FileCacheRef>,
        index_cache: Option<Arc<InvertedIndexCache>>,
        index_applier: Box<dyn IndexApplier>,
    ) -> Self {
        INDEX_APPLY_MEMORY_USAGE.add(index_applier.memory_usage() as i64);

        Self {
            region_dir,
            region_id,
            store: InstrumentedStore::new(object_store),
            file_cache,
            index_applier,
            index_cache,
        }
    }

    /// Applies predicates to the provided SST file id and returns the relevant row group ids
    pub async fn apply(&self, file_id: FileId) -> Result<ApplyOutput> {
        let _timer = INDEX_APPLY_ELAPSED.start_timer();

        let context = SearchContext {
            // Encountering a non-existing column indicates that it doesn't match predicates.
            index_not_found_strategy: IndexNotFoundStrategy::ReturnEmpty,
        };

        match self.cached_puffin_reader(file_id).await? {
            Some(mut puffin_reader) => {
                let start = Instant::now();
                let blob_reader =
                    Self::index_blob_reader(&mut puffin_reader, file_id, &self.index_cache).await?;
                common_telemetry::debug!("Build index blob reader cost: {:?}", start.elapsed());

                if let Some(index_cache) = &self.index_cache {
                    let mut index_reader = CachedInvertedIndexBlobReader::new(
                        file_id.into(),
                        InvertedIndexBlobReader::new(blob_reader),
                        index_cache.clone(),
                    );
                    self.index_applier
                        .apply(context, &mut index_reader)
                        .await
                        .context(ApplyIndexSnafu)
                } else {
                    let mut index_reader = InvertedIndexBlobReader::new(blob_reader);
                    self.index_applier
                        .apply(context, &mut index_reader)
                        .await
                        .context(ApplyIndexSnafu)
                }
            }
            None => {
                let start = Instant::now();
                let mut puffin_reader = self.remote_puffin_reader(file_id).await?;
                let blob_reader =
                    Self::index_blob_reader(&mut puffin_reader, file_id, &self.index_cache).await?;
                common_telemetry::debug!("Build index blob reader cost: {:?}", start.elapsed());

                if let Some(index_cache) = &self.index_cache {
                    let mut index_reader = CachedInvertedIndexBlobReader::new(
                        file_id.into(),
                        InvertedIndexBlobReader::new(blob_reader),
                        index_cache.clone(),
                    );
                    self.index_applier
                        .apply(context, &mut index_reader)
                        .await
                        .context(ApplyIndexSnafu)
                } else {
                    let mut index_reader = InvertedIndexBlobReader::new(blob_reader);
                    self.index_applier
                        .apply(context, &mut index_reader)
                        .await
                        .context(ApplyIndexSnafu)
                }
            }
        }
    }

    /// Helper function to create a [`PuffinFileReader`] from the cached index file.
    async fn cached_puffin_reader(
        &self,
        file_id: FileId,
    ) -> Result<Option<PuffinFileReader<impl AsyncRead + AsyncSeek>>> {
        let Some(file_cache) = &self.file_cache else {
            return Ok(None);
        };

        let Some(indexed_value) = file_cache
            .get(IndexKey::new(self.region_id, file_id, FileType::Puffin))
            .await
        else {
            return Ok(None);
        };

        Ok(file_cache
            .reader(IndexKey::new(self.region_id, file_id, FileType::Puffin))
            .await
            .map(|v| v.into_futures_async_read(0..indexed_value.file_size as u64))
            .map(PuffinFileReader::new))
    }

    /// Helper function to create a [`PuffinFileReader`] from the remote index file.
    async fn remote_puffin_reader(
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
    async fn index_blob_reader<'a>(
        puffin_reader: &'a mut PuffinFileReader<impl AsyncRead + AsyncSeek + Unpin + Send>,
        file_id: FileId,
        cache: &Option<Arc<InvertedIndexCache>>,
    ) -> Result<impl AsyncRead + AsyncSeek + 'a> {
        let file_meta = if let Some(cache) = cache.as_ref() {
            if let Some(cached_metadata) = cache.get_file_metadata(file_id.into()) {
                INDEX_CACHE_STATS
                    .with_label_values(&["puffin_metadata", "hit"])
                    .inc();
                cached_metadata
            } else {
                INDEX_CACHE_STATS
                    .with_label_values(&["puffin_metadata", "hit"])
                    .inc();
                let file_meta = Arc::new(
                    puffin_reader
                        .metadata()
                        .await
                        .context(PuffinReadMetadataSnafu)?,
                );
                cache.put_file_metadata(file_id.into(), file_meta.clone());
                file_meta
            }
        } else {
            Arc::new(
                puffin_reader
                    .metadata()
                    .await
                    .context(PuffinReadMetadataSnafu)?,
            )
        };

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
    use common_base::BitVec;
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

        let mut puffin_writer = PuffinFileWriter::new(
            object_store
                .writer(&path)
                .await
                .unwrap()
                .into_futures_async_write(),
        );
        puffin_writer
            .add_blob(Blob {
                blob_type: INDEX_BLOB_TYPE.to_string(),
                compressed_data: Cursor::new(vec![]),
                properties: Default::default(),
                compression_codec: None,
            })
            .await
            .unwrap();
        puffin_writer.finish().await.unwrap();

        let mut mock_index_applier = MockIndexApplier::new();
        mock_index_applier.expect_memory_usage().returning(|| 100);
        mock_index_applier.expect_apply().returning(|_, _| {
            Ok(ApplyOutput {
                matched_segment_ids: BitVec::EMPTY,
                total_row_count: 100,
                segment_row_count: 10,
            })
        });

        let sst_index_applier = SstIndexApplier::new(
            region_dir.clone(),
            RegionId::new(0, 0),
            object_store,
            None,
            None,
            Box::new(mock_index_applier),
        );
        let output = sst_index_applier.apply(file_id).await.unwrap();
        assert_eq!(
            output,
            ApplyOutput {
                matched_segment_ids: BitVec::EMPTY,
                total_row_count: 100,
                segment_row_count: 10,
            }
        );
    }

    #[tokio::test]
    async fn test_index_applier_apply_invalid_blob_type() {
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let file_id = FileId::random();
        let region_dir = "region_dir".to_string();
        let path = location::index_file_path(&region_dir, file_id);
        let mut puffin_writer = PuffinFileWriter::new(
            object_store
                .writer(&path)
                .await
                .unwrap()
                .into_futures_async_write(),
        );
        puffin_writer
            .add_blob(Blob {
                blob_type: "invalid_blob_type".to_string(),
                compressed_data: Cursor::new(vec![]),
                properties: Default::default(),
                compression_codec: None,
            })
            .await
            .unwrap();
        puffin_writer.finish().await.unwrap();

        let mut mock_index_applier = MockIndexApplier::new();
        mock_index_applier.expect_memory_usage().returning(|| 100);
        mock_index_applier.expect_apply().never();

        let sst_index_applier = SstIndexApplier::new(
            region_dir.clone(),
            RegionId::new(0, 0),
            object_store,
            None,
            None,
            Box::new(mock_index_applier),
        );
        let res = sst_index_applier.apply(file_id).await;
        assert!(matches!(res, Err(Error::PuffinBlobTypeNotFound { .. })));
    }
}
