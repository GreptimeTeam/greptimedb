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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use object_store::{FuturesAsyncWriter, ObjectStore};
use puffin::error::{self as puffin_error, Result as PuffinResult};
use puffin::puffin_manager::file_accessor::PuffinFileAccessor;
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

pub(crate) type SstPuffinManager =
    FsPuffinManager<Arc<BoundedStager<RegionIndexId>>, ObjectStorePuffinFileAccessor>;
pub(crate) type SstPuffinReader = <SstPuffinManager as PuffinManager>::Reader;
pub(crate) type SstPuffinWriter = <SstPuffinManager as PuffinManager>::Writer;
pub(crate) type SstPuffinBlob = <SstPuffinReader as PuffinReader>::Blob;
pub(crate) type SstPuffinDir = <SstPuffinReader as PuffinReader>::Dir;
pub(crate) type BlobReader = <SstPuffinBlob as BlobGuard>::Reader;

const STAGING_DIR: &str = "staging";

/// A factory for creating `SstPuffinManager` instances.
#[derive(Clone)]
pub struct PuffinManagerFactory {
    /// The stager used by the puffin manager.
    stager: Arc<BoundedStager<RegionIndexId>>,

    /// The size of the write buffer used to create object store.
    write_buffer_size: Option<usize>,
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
        })
    }

    pub(crate) fn build(
        &self,
        store: ObjectStore,
        path_provider: impl FilePathProvider + 'static,
    ) -> SstPuffinManager {
        let store = InstrumentedStore::new(store).with_write_buffer_size(self.write_buffer_size);
        let puffin_file_accessor =
            ObjectStorePuffinFileAccessor::new(store, Arc::new(path_provider));
        SstPuffinManager::new(self.stager.clone(), puffin_file_accessor)
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
pub(crate) struct ObjectStorePuffinFileAccessor {
    object_store: InstrumentedStore,
    path_provider: Arc<dyn FilePathProvider>,
}

impl ObjectStorePuffinFileAccessor {
    pub fn new(object_store: InstrumentedStore, path_provider: Arc<dyn FilePathProvider>) -> Self {
        Self {
            object_store,
            path_provider,
        }
    }
}

#[async_trait]
impl PuffinFileAccessor for ObjectStorePuffinFileAccessor {
    type Reader = InstrumentedRangeReader;
    type Writer = InstrumentedAsyncWrite;
    type FileHandle = RegionIndexId;

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
    async fn test_puffin_manager_factory() {
        let (_dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_puffin_manager_factory_").await;

        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
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
