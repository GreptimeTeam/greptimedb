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

use std::sync::Arc;

use object_store::services::Fs;
use object_store::util::{join_dir, with_instrument_layers};
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;

use crate::cache::write_cache::SstUploadRequest;
use crate::cache::CacheManagerRef;
use crate::config::{FulltextIndexConfig, InvertedIndexConfig};
use crate::error::{CleanDirSnafu, DeleteIndexSnafu, DeleteSstSnafu, OpenDalSnafu, Result};
use crate::read::Source;
use crate::region::options::IndexOptions;
use crate::sst::file::{FileHandle, FileId, FileMeta};
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::sst::index::IndexerBuilder;
use crate::sst::location;
use crate::sst::parquet::reader::ParquetReaderBuilder;
use crate::sst::parquet::writer::ParquetWriter;
use crate::sst::parquet::{SstInfo, WriteOptions};

pub type AccessLayerRef = Arc<AccessLayer>;

/// A layer to access SST files under the same directory.
pub struct AccessLayer {
    region_dir: String,
    /// Target object store.
    object_store: ObjectStore,
    /// Puffin manager factory for index.
    puffin_manager_factory: PuffinManagerFactory,
    /// Intermediate manager for inverted index.
    intermediate_manager: IntermediateManager,
}

impl std::fmt::Debug for AccessLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessLayer")
            .field("region_dir", &self.region_dir)
            .finish()
    }
}

impl AccessLayer {
    /// Returns a new [AccessLayer] for specific `region_dir`.
    pub fn new(
        region_dir: impl Into<String>,
        object_store: ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        intermediate_manager: IntermediateManager,
    ) -> AccessLayer {
        AccessLayer {
            region_dir: region_dir.into(),
            object_store,
            puffin_manager_factory,
            intermediate_manager,
        }
    }

    /// Returns the directory of the region.
    pub fn region_dir(&self) -> &str {
        &self.region_dir
    }

    /// Returns the object store of the layer.
    pub fn object_store(&self) -> &ObjectStore {
        &self.object_store
    }

    /// Returns the puffin manager factory.
    pub fn puffin_manager_factory(&self) -> &PuffinManagerFactory {
        &self.puffin_manager_factory
    }

    /// Deletes a SST file (and its index file if it has one) with given file id.
    pub(crate) async fn delete_sst(&self, file_meta: &FileMeta) -> Result<()> {
        let path = location::sst_file_path(&self.region_dir, file_meta.file_id);
        self.object_store
            .delete(&path)
            .await
            .context(DeleteSstSnafu {
                file_id: file_meta.file_id,
            })?;

        let path = location::index_file_path(&self.region_dir, file_meta.file_id);
        self.object_store
            .delete(&path)
            .await
            .context(DeleteIndexSnafu {
                file_id: file_meta.file_id,
            })?;

        Ok(())
    }

    /// Returns a reader builder for specific `file`.
    pub(crate) fn read_sst(&self, file: FileHandle) -> ParquetReaderBuilder {
        ParquetReaderBuilder::new(self.region_dir.clone(), file, self.object_store.clone())
    }

    /// Writes a SST with specific `file_id` and `metadata` to the layer.
    ///
    /// Returns the info of the SST. If no data written, returns None.
    pub(crate) async fn write_sst(
        &self,
        request: SstWriteRequest,
        write_opts: &WriteOptions,
    ) -> Result<Option<SstInfo>> {
        let file_path = location::sst_file_path(&self.region_dir, request.file_id);
        let index_file_path = location::index_file_path(&self.region_dir, request.file_id);
        let region_id = request.metadata.region_id;
        let file_id = request.file_id;
        let cache_manager = request.cache_manager.clone();

        let sst_info = if let Some(write_cache) = cache_manager.write_cache() {
            // Write to the write cache.
            write_cache
                .write_and_upload_sst(
                    request,
                    SstUploadRequest {
                        upload_path: file_path,
                        index_upload_path: index_file_path,
                        remote_store: self.object_store.clone(),
                    },
                    write_opts,
                )
                .await?
        } else {
            // Write cache is disabled.
            let store = self.object_store.clone();
            let indexer = IndexerBuilder {
                op_type: request.op_type,
                file_id,
                file_path: index_file_path,
                metadata: &request.metadata,
                row_group_size: write_opts.row_group_size,
                puffin_manager: self.puffin_manager_factory.build(store),
                intermediate_manager: self.intermediate_manager.clone(),
                index_options: request.index_options,
                inverted_index_config: request.inverted_index_config,
                fulltext_index_config: request.fulltext_index_config,
            }
            .build()
            .await;
            let mut writer = ParquetWriter::new_with_object_store(
                self.object_store.clone(),
                file_path,
                request.metadata,
                indexer,
            );
            writer.write_all(request.source, write_opts).await?
        };

        // Put parquet metadata to cache manager.
        if let Some(sst_info) = &sst_info {
            if let Some(parquet_metadata) = &sst_info.file_metadata {
                cache_manager.put_parquet_meta_data(region_id, file_id, parquet_metadata.clone())
            }
        }

        Ok(sst_info)
    }
    /// Returns whether the file exists in the object store.
    pub(crate) async fn is_exist(&self, file_meta: &FileMeta) -> Result<bool> {
        let path = location::sst_file_path(&self.region_dir, file_meta.file_id);
        self.object_store
            .is_exist(&path)
            .await
            .context(OpenDalSnafu)
    }
}

/// `OperationType` represents the origin of the `SstWriteRequest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum OperationType {
    Flush,
    Compact,
}

/// Contents to build a SST.
pub(crate) struct SstWriteRequest {
    pub(crate) op_type: OperationType,
    pub(crate) file_id: FileId,
    pub(crate) metadata: RegionMetadataRef,
    pub(crate) source: Source,
    pub(crate) cache_manager: CacheManagerRef,
    #[allow(dead_code)]
    pub(crate) storage: Option<String>,

    /// Configs for index
    pub(crate) index_options: IndexOptions,
    pub(crate) inverted_index_config: InvertedIndexConfig,
    pub(crate) fulltext_index_config: FulltextIndexConfig,
}

pub(crate) async fn new_fs_cache_store(root: &str) -> Result<ObjectStore> {
    let atomic_write_dir = join_dir(root, ".tmp/");
    clean_dir(&atomic_write_dir).await?;

    let builder = Fs::default().root(root).atomic_write_dir(&atomic_write_dir);
    let store = ObjectStore::new(builder).context(OpenDalSnafu)?.finish();

    Ok(with_instrument_layers(store, false))
}

/// Clean the directory.
async fn clean_dir(dir: &str) -> Result<()> {
    if tokio::fs::try_exists(dir)
        .await
        .context(CleanDirSnafu { dir })?
    {
        tokio::fs::remove_dir_all(dir)
            .await
            .context(CleanDirSnafu { dir })?;
    }

    Ok(())
}
