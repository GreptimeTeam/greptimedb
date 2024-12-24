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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_telemetry::warn;
use futures::{AsyncRead, AsyncWrite};
use index::error as index_error;
use index::error::Result as IndexResult;
use index::external_provider::ExternalTempFileProvider;
use snafu::ResultExt;

use crate::error::Result;
use crate::metrics::{
    INDEX_INTERMEDIATE_FLUSH_OP_TOTAL, INDEX_INTERMEDIATE_READ_BYTES_TOTAL,
    INDEX_INTERMEDIATE_READ_OP_TOTAL, INDEX_INTERMEDIATE_SEEK_OP_TOTAL,
    INDEX_INTERMEDIATE_WRITE_BYTES_TOTAL, INDEX_INTERMEDIATE_WRITE_OP_TOTAL,
};
use crate::sst::index::intermediate::{IntermediateLocation, IntermediateManager};

/// `TempFileProvider` implements `ExternalTempFileProvider`.
/// It uses `InstrumentedStore` to create and read intermediate files.
pub(crate) struct TempFileProvider {
    /// Provides the location of intermediate files.
    location: IntermediateLocation,
    /// Provides store to access to intermediate files.
    manager: IntermediateManager,
}

#[async_trait]
impl ExternalTempFileProvider for TempFileProvider {
    async fn create(
        &self,
        file_group: &str,
        file_id: &str,
    ) -> IndexResult<Box<dyn AsyncWrite + Unpin + Send>> {
        let path = self.location.file_path(file_group, file_id);
        let writer = self
            .manager
            .store()
            .writer(
                &path,
                &INDEX_INTERMEDIATE_WRITE_BYTES_TOTAL,
                &INDEX_INTERMEDIATE_WRITE_OP_TOTAL,
                &INDEX_INTERMEDIATE_FLUSH_OP_TOTAL,
            )
            .await
            .map_err(BoxedError::new)
            .context(index_error::ExternalSnafu)?;
        Ok(Box::new(writer))
    }

    async fn read_all(
        &self,
        file_group: &str,
    ) -> IndexResult<Vec<(String, Box<dyn AsyncRead + Unpin + Send>)>> {
        let file_group_path = self.location.file_group_path(file_group);
        let entries = self
            .manager
            .store()
            .list(&file_group_path)
            .await
            .map_err(BoxedError::new)
            .context(index_error::ExternalSnafu)?;
        let mut readers = Vec::with_capacity(entries.len());

        for entry in entries {
            if entry.metadata().is_dir() {
                warn!("Unexpected entry in index creation dir: {:?}", entry.path());
                continue;
            }

            let im_file_id = self.location.im_file_id_from_path(entry.path());

            let reader = self
                .manager
                .store()
                .reader(
                    entry.path(),
                    &INDEX_INTERMEDIATE_READ_BYTES_TOTAL,
                    &INDEX_INTERMEDIATE_READ_OP_TOTAL,
                    &INDEX_INTERMEDIATE_SEEK_OP_TOTAL,
                )
                .await
                .map_err(BoxedError::new)
                .context(index_error::ExternalSnafu)?;
            readers.push((im_file_id, Box::new(reader) as _));
        }

        Ok(readers)
    }
}

impl TempFileProvider {
    /// Creates a new `TempFileProvider`.
    pub fn new(location: IntermediateLocation, manager: IntermediateManager) -> Self {
        Self { location, manager }
    }

    /// Removes all intermediate files.
    pub async fn cleanup(&self) -> Result<()> {
        self.manager
            .store()
            .remove_all(self.location.dir_to_cleanup())
            .await
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir;
    use futures::{AsyncReadExt, AsyncWriteExt};
    use store_api::storage::RegionId;

    use super::*;
    use crate::sst::file::FileId;

    #[tokio::test]
    async fn test_temp_file_provider_basic() {
        let temp_dir = temp_dir::create_temp_dir("intermediate");
        let path = temp_dir.path().display().to_string();

        let location = IntermediateLocation::new(&RegionId::new(0, 0), &FileId::random());
        let store = IntermediateManager::init_fs(path).await.unwrap();
        let provider = TempFileProvider::new(location.clone(), store);

        let file_group = "tag0";
        let file_id = "0000000010";
        let mut writer = provider.create(file_group, file_id).await.unwrap();
        writer.write_all(b"hello").await.unwrap();
        writer.flush().await.unwrap();
        writer.close().await.unwrap();

        let file_id = "0000000100";
        let mut writer = provider.create(file_group, file_id).await.unwrap();
        writer.write_all(b"world").await.unwrap();
        writer.flush().await.unwrap();
        writer.close().await.unwrap();

        let file_group = "tag1";
        let file_id = "0000000010";
        let mut writer = provider.create(file_group, file_id).await.unwrap();
        writer.write_all(b"foo").await.unwrap();
        writer.flush().await.unwrap();
        writer.close().await.unwrap();

        let readers = provider.read_all("tag0").await.unwrap();
        assert_eq!(readers.len(), 2);
        for (_, mut reader) in readers {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert!(matches!(buf.as_slice(), b"hello" | b"world"));
        }
        let readers = provider.read_all("tag1").await.unwrap();
        assert_eq!(readers.len(), 1);
        let mut reader = readers.into_iter().map(|x| x.1).next().unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"foo");

        provider.cleanup().await.unwrap();

        assert!(provider
            .manager
            .store()
            .list(location.dir_to_cleanup())
            .await
            .unwrap()
            .is_empty());
    }
}
