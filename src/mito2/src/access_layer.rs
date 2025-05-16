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
use object_store::{ErrorKind, ObjectStore};
use smallvec::SmallVec;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{RegionId, SequenceNumber};

use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::write_cache::SstUploadRequest;
use crate::cache::CacheManagerRef;
use crate::config::{BloomFilterConfig, FulltextIndexConfig, InvertedIndexConfig};
use crate::error::{CleanDirSnafu, DeleteIndexSnafu, DeleteSstSnafu, OpenDalSnafu, Result};
use crate::read::Source;
use crate::region::options::IndexOptions;
use crate::sst::file::{FileHandle, FileId, FileMeta};
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::sst::index::IndexerBuilderImpl;
use crate::sst::location;
use crate::sst::parquet::reader::ParquetReaderBuilder;
use crate::sst::parquet::writer::ParquetWriter;
use crate::sst::parquet::{SstInfo, WriteOptions};

pub type AccessLayerRef = Arc<AccessLayer>;
/// SST write results.
pub type SstInfoArray = SmallVec<[SstInfo; 2]>;

pub const ATOMIC_WRITE_DIR: &str = "tmp/";
/// For compatibility. Remove this after a major version release.
pub const OLD_ATOMIC_WRITE_DIR: &str = ".tmp/";

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
    pub async fn write_sst(
        &self,
        request: SstWriteRequest,
        write_opts: &WriteOptions,
    ) -> Result<SstInfoArray> {
        let region_id = request.metadata.region_id;
        let cache_manager = request.cache_manager.clone();

        let sst_info = if let Some(write_cache) = cache_manager.write_cache() {
            // Write to the write cache.
            write_cache
                .write_and_upload_sst(
                    request,
                    SstUploadRequest {
                        dest_path_provider: RegionFilePathFactory {
                            region_dir: self.region_dir.clone(),
                        },
                        remote_store: self.object_store.clone(),
                    },
                    write_opts,
                )
                .await?
        } else {
            // Write cache is disabled.
            let store = self.object_store.clone();
            let path_provider = RegionFilePathFactory::new(self.region_dir.clone());
            let indexer_builder = IndexerBuilderImpl {
                op_type: request.op_type,
                metadata: request.metadata.clone(),
                row_group_size: write_opts.row_group_size,
                puffin_manager: self
                    .puffin_manager_factory
                    .build(store, path_provider.clone()),
                intermediate_manager: self.intermediate_manager.clone(),
                index_options: request.index_options,
                inverted_index_config: request.inverted_index_config,
                fulltext_index_config: request.fulltext_index_config,
                bloom_filter_index_config: request.bloom_filter_index_config,
            };
            // We disable write cache on file system but we still use atomic write.
            // TODO(yingwen): If we support other non-fs stores without the write cache, then
            // we may have find a way to check whether we need the cleaner.
            let cleaner = TempFileCleaner::new(region_id, self.object_store.clone());
            let mut writer = ParquetWriter::new_with_object_store(
                self.object_store.clone(),
                request.metadata,
                indexer_builder,
                path_provider,
            )
            .await
            .with_file_cleaner(cleaner);
            writer
                .write_all(request.source, request.max_sequence, write_opts)
                .await?
        };

        // Put parquet metadata to cache manager.
        if !sst_info.is_empty() {
            for sst in &sst_info {
                if let Some(parquet_metadata) = &sst.file_metadata {
                    cache_manager.put_parquet_meta_data(
                        region_id,
                        sst.file_id,
                        parquet_metadata.clone(),
                    )
                }
            }
        }

        Ok(sst_info)
    }
}

/// `OperationType` represents the origin of the `SstWriteRequest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    Flush,
    Compact,
}

/// Contents to build a SST.
pub struct SstWriteRequest {
    pub op_type: OperationType,
    pub metadata: RegionMetadataRef,
    pub source: Source,
    pub cache_manager: CacheManagerRef,
    #[allow(dead_code)]
    pub storage: Option<String>,
    pub max_sequence: Option<SequenceNumber>,

    /// Configs for index
    pub index_options: IndexOptions,
    pub inverted_index_config: InvertedIndexConfig,
    pub fulltext_index_config: FulltextIndexConfig,
    pub bloom_filter_index_config: BloomFilterConfig,
}

/// Cleaner to remove temp files on the atomic write dir.
pub(crate) struct TempFileCleaner {
    region_id: RegionId,
    object_store: ObjectStore,
}

impl TempFileCleaner {
    /// Constructs the cleaner for the region and store.
    pub(crate) fn new(region_id: RegionId, object_store: ObjectStore) -> Self {
        Self {
            region_id,
            object_store,
        }
    }

    /// Removes the SST and index file from the local atomic dir by the file id.
    pub(crate) async fn clean_by_file_id(&self, file_id: FileId) {
        let sst_key = IndexKey::new(self.region_id, file_id, FileType::Parquet).to_string();
        let index_key = IndexKey::new(self.region_id, file_id, FileType::Puffin).to_string();

        Self::clean_atomic_dir_files(&self.object_store, &[&sst_key, &index_key]).await;
    }

    /// Removes the files from the local atomic dir by their names.
    pub(crate) async fn clean_atomic_dir_files(
        local_store: &ObjectStore,
        names_to_remove: &[&str],
    ) {
        // We don't know the actual suffix of the file under atomic dir, so we have
        // to list the dir. The cost should be acceptable as there won't be to many files.
        let Ok(entries) = local_store.list(ATOMIC_WRITE_DIR).await.inspect_err(|e| {
            if e.kind() != ErrorKind::NotFound {
                common_telemetry::error!(e; "Failed to list tmp files for {:?}", names_to_remove)
            }
        }) else {
            return;
        };

        // In our case, we can ensure the file id is unique so it is safe to remove all files
        // with the same file id under the atomic write dir.
        let actual_files: Vec<_> = entries
            .into_iter()
            .filter_map(|entry| {
                if entry.metadata().is_dir() {
                    return None;
                }

                // Remove name that matches files_to_remove.
                let should_remove = names_to_remove
                    .iter()
                    .any(|file| entry.name().starts_with(file));
                if should_remove {
                    Some(entry.path().to_string())
                } else {
                    None
                }
            })
            .collect();

        common_telemetry::warn!(
            "Clean files {:?} under atomic write dir for {:?}",
            actual_files,
            names_to_remove
        );

        if let Err(e) = local_store.delete_iter(actual_files).await {
            common_telemetry::error!(e; "Failed to delete tmp file for {:?}", names_to_remove);
        }
    }
}

pub(crate) async fn new_fs_cache_store(root: &str) -> Result<ObjectStore> {
    let atomic_write_dir = join_dir(root, ATOMIC_WRITE_DIR);
    clean_dir(&atomic_write_dir).await?;

    // Compatible code. Remove this after a major release.
    let old_atomic_temp_dir = join_dir(root, OLD_ATOMIC_WRITE_DIR);
    clean_dir(&old_atomic_temp_dir).await?;

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

/// Path provider for SST file and index file.
pub trait FilePathProvider: Send + Sync {
    /// Creates index file path of given file id.
    fn build_index_file_path(&self, file_id: FileId) -> String;

    /// Creates SST file path of given file id.
    fn build_sst_file_path(&self, file_id: FileId) -> String;
}

/// Path provider that builds paths in local write cache.
#[derive(Clone)]
pub(crate) struct WriteCachePathProvider {
    region_id: RegionId,
    file_cache: FileCacheRef,
}

impl WriteCachePathProvider {
    /// Creates a new `WriteCachePathProvider` instance.
    pub fn new(region_id: RegionId, file_cache: FileCacheRef) -> Self {
        Self {
            region_id,
            file_cache,
        }
    }
}

impl FilePathProvider for WriteCachePathProvider {
    fn build_index_file_path(&self, file_id: FileId) -> String {
        let puffin_key = IndexKey::new(self.region_id, file_id, FileType::Puffin);
        self.file_cache.cache_file_path(puffin_key)
    }

    fn build_sst_file_path(&self, file_id: FileId) -> String {
        let parquet_file_key = IndexKey::new(self.region_id, file_id, FileType::Parquet);
        self.file_cache.cache_file_path(parquet_file_key)
    }
}

/// Path provider that builds paths in region storage path.
#[derive(Clone, Debug)]
pub(crate) struct RegionFilePathFactory {
    region_dir: String,
}

impl RegionFilePathFactory {
    /// Creates a new `RegionFilePathFactory` instance.
    pub fn new(region_dir: String) -> Self {
        Self { region_dir }
    }
}

impl FilePathProvider for RegionFilePathFactory {
    fn build_index_file_path(&self, file_id: FileId) -> String {
        location::index_file_path(&self.region_dir, file_id)
    }

    fn build_sst_file_path(&self, file_id: FileId) -> String {
        location::sst_file_path(&self.region_dir, file_id)
    }
}
