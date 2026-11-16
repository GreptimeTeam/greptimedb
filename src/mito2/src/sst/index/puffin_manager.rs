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

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use object_store::{FuturesAsyncWriter, ObjectStore, Service, ServiceInfo, Servicer};
use puffin::error::{self as puffin_error, Result as PuffinResult};
use puffin::puffin_manager::file_accessor::PuffinFileAccessor;
use puffin::puffin_manager::file_key::{PuffinFileKey, PuffinStorageScope};
use puffin::puffin_manager::fs_puffin_manager::FsPuffinManager;
use puffin::puffin_manager::stager::{BoundedStager, Stager};
use puffin::puffin_manager::{BlobGuard, PuffinManager, PuffinReader};
use snafu::ResultExt;

use crate::access_layer::FilePathProvider;
use crate::error::{PuffinInitStagerSnafu, PuffinPurgeStagerSnafu, Result};
use crate::metrics::{
    INDEX_PUFFIN_FLUSH_OP_TOTAL, INDEX_PUFFIN_READ_BYTES_TOTAL, INDEX_PUFFIN_READ_OP_TOTAL,
    INDEX_PUFFIN_WRITE_BYTES_TOTAL, INDEX_PUFFIN_WRITE_OP_TOTAL, StagerMetrics,
};
use crate::sst::file::RegionIndexId;
use crate::sst::index::store::{self, InstrumentedStore};

type InstrumentedRangeReader = store::InstrumentedRangeReader<'static>;
type InstrumentedAsyncWrite = store::InstrumentedAsyncWrite<'static, FuturesAsyncWriter>;

pub type SstPuffinManager =
    FsPuffinManager<Arc<BoundedStager<RegionIndexId>>, ObjectStorePuffinFileAccessor>;
pub type SstPuffinReader = <SstPuffinManager as PuffinManager>::Reader;
pub type SstPuffinWriter = <SstPuffinManager as PuffinManager>::Writer;
pub type SstPuffinBlob = <SstPuffinReader as PuffinReader>::Blob;
pub type SstPuffinDir = <SstPuffinReader as PuffinReader>::Dir;
pub type BlobReader = <SstPuffinBlob as BlobGuard>::Reader;

const STAGING_DIR: &str = "staging";

/// Reuses a scope across managers built from the same backend instance.
/// Keeping the service alive prevents pointer reuse from aliasing a cached identity.
/// Entries live with the factory and grow with distinct backend instances it encounters.
#[derive(Default)]
struct BackendScopes {
    entries: Mutex<Vec<BackendScope>>,
}

struct BackendScope {
    service: Servicer,
    location: ServiceInfo,
    scope: Arc<PuffinStorageScope>,
}

impl BackendScopes {
    fn get(&self, store: &ObjectStore) -> Arc<PuffinStorageScope> {
        let service = store.base_service();
        let location = store.service().info();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = entries
            .iter()
            .find(|entry| Arc::ptr_eq(&entry.service, service) && entry.location == location)
        {
            return entry.scope.clone();
        }
        let scope = Arc::new(PuffinStorageScope::unique(
            location.scheme().to_string(),
            location.name().to_string(),
            location.root().to_string(),
        ));
        entries.push(BackendScope {
            service: service.clone(),
            location,
            scope: scope.clone(),
        });
        scope
    }
}

/// A factory for creating `SstPuffinManager` instances.
#[derive(Clone)]
pub struct PuffinManagerFactory {
    /// The stager used by the puffin manager.
    stager: Arc<BoundedStager<RegionIndexId>>,

    /// The size of the write buffer used to create object store.
    write_buffer_size: Option<usize>,
    backend_scopes: Arc<BackendScopes>,
}

impl PuffinManagerFactory {
    /// Creates a new `PuffinManagerFactory` instance.
    pub async fn new(
        aux_path: impl AsRef<Path>,
        staging_capacity: u64,
        write_buffer_size: Option<usize>,
        staging_ttl: Option<Duration>,
    ) -> Result<Self> {
        let staging_dir = aux_path.as_ref().join(STAGING_DIR);
        let stager = BoundedStager::new(
            staging_dir,
            staging_capacity,
            Some(Arc::new(StagerMetrics::default())),
            staging_ttl,
        )
        .await
        .context(PuffinInitStagerSnafu)?;
        Ok(Self {
            stager: Arc::new(stager),
            write_buffer_size,
            backend_scopes: Arc::default(),
        })
    }

    /// Overrides only this task's upload buffers, sharing the existing stager.
    pub(crate) fn with_write_buffer_size(mut self, bytes: usize) -> Self {
        self.write_buffer_size = Some(bytes);
        self
    }

    pub fn build(
        &self,
        store: ObjectStore,
        path_provider: impl FilePathProvider + 'static,
    ) -> SstPuffinManager {
        let scope = self.backend_scopes.get(&store);
        let store = InstrumentedStore::new(store).with_write_buffer_size(self.write_buffer_size);
        let puffin_file_accessor =
            ObjectStorePuffinFileAccessor::new(store, Arc::new(path_provider), scope);
        SstPuffinManager::new(self.stager.clone(), puffin_file_accessor)
    }

    /// Builds a reader manager for an exact local replica of an origin file.
    ///
    /// `origin` must come from this factory or one of its clones so both managers
    /// share the same stager. The replica must contain the origin's exact bytes.
    /// Reads use the replica location; cache keys retain the origin's identity.
    pub(crate) fn build_cached(
        &self,
        origin: &SstPuffinManager,
        store: ObjectStore,
        path_provider: impl FilePathProvider + 'static,
    ) -> SstPuffinManager {
        let origin = origin.file_accessor();
        let accessor = ObjectStorePuffinFileAccessor {
            object_store: InstrumentedStore::new(store)
                .with_write_buffer_size(self.write_buffer_size),
            path_provider: Arc::new(path_provider),
            cache_path_provider: origin.cache_path_provider.clone(),
            cache_scope: origin.cache_scope.clone(),
        };
        SstPuffinManager::new(self.stager.clone(), accessor)
    }

    pub(crate) async fn purge_stager(&self, file_id: RegionIndexId) -> Result<()> {
        self.stager
            .purge(&file_id)
            .await
            .context(PuffinPurgeStagerSnafu)
    }
}

#[cfg(test)]
impl PuffinManagerFactory {
    pub(crate) async fn new_for_test_async(
        prefix: &str,
    ) -> (common_test_util::temp_dir::TempDir, Self) {
        let tempdir = common_test_util::temp_dir::create_temp_dir(prefix);
        let factory = Self::new(tempdir.path().to_path_buf(), 1024, None, None)
            .await
            .unwrap();
        (tempdir, factory)
    }

    pub(crate) fn new_for_test_block(prefix: &str) -> (common_test_util::temp_dir::TempDir, Self) {
        let tempdir = common_test_util::temp_dir::create_temp_dir(prefix);

        let f = Self::new(tempdir.path().to_path_buf(), 1024, None, None);
        let factory = common_runtime::block_on_global(f).unwrap();

        (tempdir, factory)
    }
}

/// A `PuffinFileAccessor` implementation that uses an object store as the underlying storage.
#[derive(Clone)]
pub struct ObjectStorePuffinFileAccessor {
    object_store: InstrumentedStore,
    path_provider: Arc<dyn FilePathProvider>,
    cache_path_provider: Arc<dyn FilePathProvider>,
    cache_scope: Arc<PuffinStorageScope>,
}

impl ObjectStorePuffinFileAccessor {
    pub(crate) fn new(
        object_store: InstrumentedStore,
        path_provider: Arc<dyn FilePathProvider>,
        cache_scope: Arc<PuffinStorageScope>,
    ) -> Self {
        Self {
            object_store,
            cache_path_provider: path_provider.clone(),
            path_provider,
            cache_scope,
        }
    }

    pub(crate) fn store(&self) -> &InstrumentedStore {
        &self.object_store
    }
}

#[async_trait]
impl PuffinFileAccessor for ObjectStorePuffinFileAccessor {
    type Reader = InstrumentedRangeReader;
    type Writer = InstrumentedAsyncWrite;
    type FileHandle = RegionIndexId;

    fn file_key(&self, handle: &RegionIndexId) -> PuffinFileKey<RegionIndexId> {
        PuffinFileKey::new(
            self.cache_scope.clone(),
            self.cache_path_provider
                .build_index_file_path_with_version(*handle),
            *handle,
        )
    }

    async fn reader(&self, handle: &RegionIndexId) -> PuffinResult<Self::Reader> {
        let file_path = self
            .path_provider
            .build_index_file_path_with_version(*handle);
        self.object_store
            .range_reader(
                &file_path,
                &INDEX_PUFFIN_READ_BYTES_TOTAL,
                &INDEX_PUFFIN_READ_OP_TOTAL,
            )
            .await
            .map_err(BoxedError::new)
            .context(puffin_error::ExternalSnafu)
    }

    async fn writer(&self, handle: &RegionIndexId) -> PuffinResult<Self::Writer> {
        let file_path = self
            .path_provider
            .build_index_file_path_with_version(*handle);
        self.object_store
            .writer(
                &file_path,
                &INDEX_PUFFIN_WRITE_BYTES_TOTAL,
                &INDEX_PUFFIN_WRITE_OP_TOTAL,
                &INDEX_PUFFIN_FLUSH_OP_TOTAL,
            )
            .await
            .map_err(BoxedError::new)
            .context(puffin_error::ExternalSnafu)
    }
}

#[cfg(test)]
mod tests {

    use common_base::range_read::RangeReader;
    use common_test_util::temp_dir::create_temp_dir;
    use futures::io::Cursor;
    use object_store::services::Memory;
    use puffin::blob_metadata::CompressionCodec;
    use puffin::puffin_manager::{PuffinManager, PuffinReader, PuffinWriter, PutOptions};
    use store_api::storage::FileId;

    use super::*;
    use crate::sst::file::{RegionFileId, RegionIndexId};

    struct TestFilePathProvider;

    impl FilePathProvider for TestFilePathProvider {
        fn build_index_file_path(&self, file_id: RegionFileId) -> String {
            file_id.file_id().to_string()
        }

        fn build_index_file_path_with_version(&self, index_id: RegionIndexId) -> String {
            index_id.file_id.file_id().to_string()
        }

        fn build_sst_file_path(&self, file_id: RegionFileId) -> String {
            file_id.file_id().to_string()
        }
    }

    #[tokio::test]
    async fn test_file_key_tracks_storage_path_and_version() {
        let (_dir, factory) = PuffinManagerFactory::new_for_test_async("test_file_key_").await;
        let store = ObjectStore::new(Memory::default()).unwrap();
        let index_id = RegionIndexId::new(RegionFileId::new(0.into(), FileId::random()), 0);
        let manager = factory.build(store.clone(), TestFilePathProvider);
        let rebuilt = factory.build(store, TestFilePathProvider);
        assert_eq!(manager.file_key(&index_id), rebuilt.file_key(&index_id));
        // Even a path provider that omits the version cannot alias generations.
        let next = RegionIndexId::new(index_id.file_id, 1);
        assert_ne!(manager.file_key(&index_id), manager.file_key(&next));

        let other_store = ObjectStore::new(Memory::default()).unwrap();
        let other = factory.build(other_store, TestFilePathProvider);
        assert_ne!(manager.file_key(&index_id), other.file_key(&index_id));

        struct PrefixedPath;
        impl FilePathProvider for PrefixedPath {
            fn build_index_file_path(&self, id: RegionFileId) -> String {
                format!("other/{}", id.file_id())
            }
            fn build_index_file_path_with_version(&self, id: RegionIndexId) -> String {
                format!("other/{}", id.file_id())
            }
            fn build_sst_file_path(&self, id: RegionFileId) -> String {
                format!("other/{}", id.file_id())
            }
        }
        let same_store = manager.file_accessor().store().store().clone();
        let moved = factory.build(same_store, PrefixedPath);
        assert_ne!(manager.file_key(&index_id), moved.file_key(&index_id));
    }

    #[tokio::test]
    async fn test_cached_replica_reuses_origin_metadata_and_reads_local_bytes() {
        use api::v1::index::InvertedIndexMetas;
        use puffin::puffin_manager::cache::PuffinMetadataCache;
        use store_api::region_request::PathType;

        use crate::access_layer::RegionFilePathFactory;
        use crate::cache::index::inverted_index::InvertedIndexCache;
        use crate::metrics::CACHE_BYTES;

        let (_dir, factory) = PuffinManagerFactory::new_for_test_async("test_replica_key_").await;
        let remote_store = ObjectStore::new(Memory::default()).unwrap();
        let local_store = ObjectStore::new(Memory::default()).unwrap();
        let metadata_cache = Arc::new(PuffinMetadataCache::new(1024 * 1024, &CACHE_BYTES));
        let manager = factory
            .build(remote_store.clone(), TestFilePathProvider)
            .with_puffin_metadata_cache(Some(metadata_cache.clone()));
        let replica = factory
            .build_cached(
                &manager,
                local_store.clone(),
                RegionFilePathFactory::new("cached/".into(), PathType::Bare),
            )
            .with_puffin_metadata_cache(Some(metadata_cache));
        // Hold the local bytes and path fixed; only cache identity differs.
        let isolated_replica = factory.build(
            local_store.clone(),
            RegionFilePathFactory::new("cached/".into(), PathType::Bare),
        );
        let id = RegionIndexId::new(RegionFileId::new(0.into(), FileId::random()), 0);
        assert_ne!(manager.file_key(&id), isolated_replica.file_key(&id));
        assert_eq!(manager.file_key(&id), replica.file_key(&id));
        assert_ne!(
            manager.file_key(&id),
            replica.file_key(&RegionIndexId::new(id.file_id, 1))
        );

        let bytes = b"read these bytes from the local replica";
        let mut writer = manager.writer(&id).await.unwrap();
        writer
            .put_blob(
                "blob",
                Cursor::new(bytes),
                PutOptions::default(),
                Default::default(),
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();
        let remote_path = TestFilePathProvider.build_index_file_path_with_version(id);
        let local_path = RegionFilePathFactory::new("cached/".into(), PathType::Bare)
            .build_index_file_path_with_version(id);
        assert_ne!(remote_path, local_path);
        local_store
            .write(&local_path, remote_store.read(&remote_path).await.unwrap())
            .await
            .unwrap();

        let metadata = manager.reader(&id).await.unwrap().metadata().await.unwrap();
        remote_store.delete(&remote_path).await.unwrap();
        let reader = replica.reader(&id).await.unwrap();
        assert!(Arc::ptr_eq(&metadata, &reader.metadata().await.unwrap()));
        let blob = reader.blob("blob").await.unwrap().reader().await.unwrap();
        assert_eq!(&*blob.read(0..bytes.len() as u64).await.unwrap(), bytes);

        let index_cache = InvertedIndexCache::new(1024 * 1024, 1024 * 1024, 1024);
        let index_metadata = Arc::new(InvertedIndexMetas::default());
        index_cache
            .put_metadata(manager.file_key(&id), index_metadata.clone())
            .await;
        assert!(
            index_cache
                .get_metadata(isolated_replica.file_key(&id))
                .await
                .is_none()
        );
        assert!(Arc::ptr_eq(
            &index_metadata,
            &index_cache
                .get_metadata(replica.file_key(&id))
                .await
                .unwrap()
        ));
    }

    #[tokio::test]
    async fn test_file_key_isolates_s3_endpoints_without_io() {
        let (_dir, factory) = PuffinManagerFactory::new_for_test_async("test_s3_scope_").await;
        let build = |endpoint| {
            ObjectStore::new(
                object_store::services::S3::default()
                    .bucket("same-bucket")
                    .root("same-root")
                    .region("us-east-1")
                    .endpoint(endpoint),
            )
            .unwrap()
        };
        let first = build("http://127.0.0.1:19001");
        let second = build("http://127.0.0.1:19002");
        assert_eq!(first.info().scheme(), second.info().scheme());
        assert_eq!(first.info().name(), second.info().name());
        assert_eq!(first.info().root(), second.info().root());
        let index_id = RegionIndexId::new(RegionFileId::new(0.into(), FileId::random()), 0);
        let manager = factory.build(first.clone(), TestFilePathProvider);
        let reconstructed = factory.clone().build(first.clone(), TestFilePathProvider);
        let other = factory.build(second, TestFilePathProvider);
        assert_eq!(
            manager.file_key(&index_id),
            reconstructed.file_key(&index_id)
        );
        assert_ne!(manager.file_key(&index_id), other.file_key(&index_id));
        let (_other_dir, other_factory) =
            PuffinManagerFactory::new_for_test_async("test_other_scope_").await;
        let independent = other_factory.build(first, TestFilePathProvider);
        assert_ne!(manager.file_key(&index_id), independent.file_key(&index_id));
    }

    #[tokio::test]
    async fn test_puffin_manager_factory() {
        let (_dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_puffin_manager_factory_").await;

        let object_store = ObjectStore::new(Memory::default()).unwrap();
        let manager = factory.build(object_store, TestFilePathProvider);

        let file_id = RegionIndexId::new(RegionFileId::new(0.into(), FileId::random()), 0);
        let blob_key = "blob-key";
        let dir_key = "dir-key";
        let raw_data = b"hello world!";

        let mut writer = manager.writer(&file_id).await.unwrap();
        writer
            .put_blob(
                blob_key,
                Cursor::new(raw_data),
                PutOptions::default(),
                Default::default(),
            )
            .await
            .unwrap();
        let dir_data = create_temp_dir("test_puffin_manager_factory_dir_data_");
        tokio::fs::write(dir_data.path().join("hello"), raw_data)
            .await
            .unwrap();
        writer
            .put_dir(
                dir_key,
                dir_data.path().into(),
                PutOptions {
                    compression: Some(CompressionCodec::Zstd),
                },
                Default::default(),
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let reader = manager.reader(&file_id).await.unwrap();
        let blob_guard = reader.blob(blob_key).await.unwrap();
        let blob_reader = blob_guard.reader().await.unwrap();
        let meta = blob_reader.metadata().await.unwrap();
        let bs = blob_reader.read(0..meta.content_length).await.unwrap();
        assert_eq!(&*bs, raw_data);

        let (dir_guard, _metrics) = reader.dir(dir_key).await.unwrap();
        let file = dir_guard.path().join("hello");
        let data = tokio::fs::read(file).await.unwrap();
        assert_eq!(data, raw_data);
    }
}
