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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use object_store::{FuturesAsyncReader, FuturesAsyncWriter, ObjectStore};
use puffin::error::{self as puffin_error, Result as PuffinResult};
use puffin::puffin_manager::file_accessor::PuffinFileAccessor;
use puffin::puffin_manager::fs_puffin_manager::FsPuffinManager;
use puffin::puffin_manager::stager::BoundedStager;
use puffin::puffin_manager::{BlobGuard, PuffinManager, PuffinReader};
use snafu::ResultExt;

use crate::error::{PuffinInitStagerSnafu, Result};
use crate::metrics::{
    INDEX_PUFFIN_FLUSH_OP_TOTAL, INDEX_PUFFIN_READ_BYTES_TOTAL, INDEX_PUFFIN_READ_OP_TOTAL,
    INDEX_PUFFIN_SEEK_OP_TOTAL, INDEX_PUFFIN_WRITE_BYTES_TOTAL, INDEX_PUFFIN_WRITE_OP_TOTAL,
};
use crate::sst::index::store::{self, InstrumentedStore};

type InstrumentedAsyncRead = store::InstrumentedAsyncRead<'static, FuturesAsyncReader>;
type InstrumentedAsyncWrite = store::InstrumentedAsyncWrite<'static, FuturesAsyncWriter>;

pub(crate) type SstPuffinManager =
    FsPuffinManager<Arc<BoundedStager>, ObjectStorePuffinFileAccessor>;
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
    stager: Arc<BoundedStager>,

    /// The size of the write buffer used to create object store.
    write_buffer_size: Option<usize>,
}

impl PuffinManagerFactory {
    /// Creates a new `PuffinManagerFactory` instance.
    pub async fn new(
        aux_path: impl AsRef<Path>,
        staging_capacity: u64,
        write_buffer_size: Option<usize>,
    ) -> Result<Self> {
        let staging_dir = aux_path.as_ref().join(STAGING_DIR);
        let stager = BoundedStager::new(staging_dir, staging_capacity)
            .await
            .context(PuffinInitStagerSnafu)?;
        Ok(Self {
            stager: Arc::new(stager),
            write_buffer_size,
        })
    }

    pub(crate) fn build(&self, store: ObjectStore) -> SstPuffinManager {
        let store = InstrumentedStore::new(store).with_write_buffer_size(self.write_buffer_size);
        let puffin_file_accessor = ObjectStorePuffinFileAccessor::new(store);
        SstPuffinManager::new(self.stager.clone(), puffin_file_accessor)
    }
}

#[cfg(test)]
impl PuffinManagerFactory {
    pub(crate) async fn new_for_test_async(
        prefix: &str,
    ) -> (common_test_util::temp_dir::TempDir, Self) {
        let tempdir = common_test_util::temp_dir::create_temp_dir(prefix);
        let factory = Self::new(tempdir.path().to_path_buf(), 1024, None)
            .await
            .unwrap();
        (tempdir, factory)
    }

    pub(crate) fn new_for_test_block(prefix: &str) -> (common_test_util::temp_dir::TempDir, Self) {
        let tempdir = common_test_util::temp_dir::create_temp_dir(prefix);

        let f = Self::new(tempdir.path().to_path_buf(), 1024, None);
        let factory = common_runtime::block_on_global(f).unwrap();

        (tempdir, factory)
    }
}

/// A `PuffinFileAccessor` implementation that uses an object store as the underlying storage.
#[derive(Clone)]
pub(crate) struct ObjectStorePuffinFileAccessor {
    object_store: InstrumentedStore,
}

impl ObjectStorePuffinFileAccessor {
    pub fn new(object_store: InstrumentedStore) -> Self {
        Self { object_store }
    }
}

#[async_trait]
impl PuffinFileAccessor for ObjectStorePuffinFileAccessor {
    type Reader = InstrumentedAsyncRead;
    type Writer = InstrumentedAsyncWrite;

    async fn reader(&self, puffin_file_name: &str) -> PuffinResult<Self::Reader> {
        self.object_store
            .reader(
                puffin_file_name,
                &INDEX_PUFFIN_READ_BYTES_TOTAL,
                &INDEX_PUFFIN_READ_OP_TOTAL,
                &INDEX_PUFFIN_SEEK_OP_TOTAL,
            )
            .await
            .map_err(BoxedError::new)
            .context(puffin_error::ExternalSnafu)
    }

    async fn writer(&self, puffin_file_name: &str) -> PuffinResult<Self::Writer> {
        self.object_store
            .writer(
                puffin_file_name,
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
    use common_test_util::temp_dir::create_temp_dir;
    use futures::io::Cursor;
    use futures::AsyncReadExt;
    use object_store::services::Memory;
    use puffin::blob_metadata::CompressionCodec;
    use puffin::puffin_manager::{DirGuard, PuffinManager, PuffinReader, PuffinWriter, PutOptions};

    use super::*;

    #[tokio::test]
    async fn test_puffin_manager_factory() {
        let (_dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_puffin_manager_factory_").await;

        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let manager = factory.build(object_store);

        let file_name = "my-puffin-file";
        let blob_key = "blob-key";
        let dir_key = "dir-key";
        let raw_data = b"hello world!";

        let mut writer = manager.writer(file_name).await.unwrap();
        writer
            .put_blob(blob_key, Cursor::new(raw_data), PutOptions::default())
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
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let reader = manager.reader(file_name).await.unwrap();
        let blob_guard = reader.blob(blob_key).await.unwrap();
        let mut blob_reader = blob_guard.reader().await.unwrap();
        let mut buf = Vec::new();
        blob_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, raw_data);

        let dir_guard = reader.dir(dir_key).await.unwrap();
        let file = dir_guard.path().join("hello");
        let data = tokio::fs::read(file).await.unwrap();
        assert_eq!(data, raw_data);
    }
}
