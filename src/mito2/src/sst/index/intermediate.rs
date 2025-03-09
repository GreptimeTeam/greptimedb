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

use std::path::PathBuf;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_telemetry::{debug, warn};
use futures::{AsyncRead, AsyncWrite};
use index::error as index_error;
use index::error::Result as IndexResult;
use index::external_provider::ExternalTempFileProvider;
use object_store::util::{self, normalize_dir};
use snafu::ResultExt;
use store_api::storage::{ColumnId, RegionId};
use uuid::Uuid;

use crate::access_layer::new_fs_cache_store;
use crate::error::Result;
use crate::metrics::{
    INDEX_INTERMEDIATE_FLUSH_OP_TOTAL, INDEX_INTERMEDIATE_READ_BYTES_TOTAL,
    INDEX_INTERMEDIATE_READ_OP_TOTAL, INDEX_INTERMEDIATE_SEEK_OP_TOTAL,
    INDEX_INTERMEDIATE_WRITE_BYTES_TOTAL, INDEX_INTERMEDIATE_WRITE_OP_TOTAL,
};
use crate::sst::file::FileId;
use crate::sst::index::store::InstrumentedStore;

const INTERMEDIATE_DIR: &str = "__intm";

/// `IntermediateManager` provides store to access to intermediate files.
#[derive(Clone)]
pub struct IntermediateManager {
    base_dir: PathBuf,
    store: InstrumentedStore,
}

impl IntermediateManager {
    /// Create a new `IntermediateManager` with the given root path.
    /// It will clean up all garbage intermediate files from previous runs.
    pub async fn init_fs(aux_path: impl AsRef<str>) -> Result<Self> {
        common_telemetry::info!(
            "Initializing intermediate manager, aux_path: {}",
            aux_path.as_ref()
        );

        let store = new_fs_cache_store(&normalize_dir(aux_path.as_ref())).await?;
        let store = InstrumentedStore::new(store);

        // Remove all garbage intermediate files from previous runs.
        if let Err(err) = store.remove_all(INTERMEDIATE_DIR).await {
            warn!(err; "Failed to remove garbage intermediate files");
        }

        Ok(Self {
            base_dir: PathBuf::from(aux_path.as_ref()),
            store,
        })
    }

    /// Set the write buffer size for the store.
    pub fn with_buffer_size(mut self, write_buffer_size: Option<usize>) -> Self {
        self.store = self.store.with_write_buffer_size(write_buffer_size);
        self
    }

    /// Returns the store to access to intermediate files.
    pub(crate) fn store(&self) -> &InstrumentedStore {
        &self.store
    }

    /// Returns the intermediate directory path for building fulltext index.
    /// The format is `{aux_path}/__intm/{region_id}/{sst_file_id}/fulltext-{column_id}-{uuid}`.
    pub(crate) fn fulltext_path(
        &self,
        region_id: &RegionId,
        sst_file_id: &FileId,
        column_id: ColumnId,
    ) -> PathBuf {
        let uuid = Uuid::new_v4();
        self.base_dir
            .join(INTERMEDIATE_DIR)
            .join(region_id.as_u64().to_string())
            .join(sst_file_id.to_string())
            .join(format!("fulltext-{column_id}-{uuid}"))
    }
}

/// `IntermediateLocation` produces paths for intermediate files
/// during external sorting.
#[derive(Debug, Clone)]
pub struct IntermediateLocation {
    files_dir: String,
}

impl IntermediateLocation {
    /// Create a new `IntermediateLocation`. Set the root directory to
    /// `__intm/{region_id}/{sst_file_id}/{uuid}/`, incorporating
    /// uuid to differentiate active sorting files from orphaned data due to unexpected
    /// process termination.
    pub fn new(region_id: &RegionId, sst_file_id: &FileId) -> Self {
        let region_id = region_id.as_u64();
        let uuid = Uuid::new_v4();
        Self {
            files_dir: format!("{INTERMEDIATE_DIR}/{region_id}/{sst_file_id}/{uuid}/"),
        }
    }

    /// Returns the directory to clean up when the sorting is done
    pub fn dir_to_cleanup(&self) -> &str {
        &self.files_dir
    }

    /// Returns the path of the directory for intermediate files associated with the `file_group`:
    /// `__intm/{region_id}/{sst_file_id}/{uuid}/{file_group}/`
    pub fn file_group_path(&self, file_group: &str) -> String {
        util::join_path(&self.files_dir, &format!("{file_group}/"))
    }

    /// Returns the path of the intermediate file with the given `file_group` and `im_file_id`:
    /// `__intm/{region_id}/{sst_file_id}/{uuid}/{file_group}/{im_file_id}.im`
    pub fn file_path(&self, file_group: &str, im_file_id: &str) -> String {
        util::join_path(
            &self.file_group_path(file_group),
            &format!("{im_file_id}.im"),
        )
    }

    /// Returns the intermediate file id from the path.
    pub fn im_file_id_from_path(&self, path: &str) -> String {
        path.rsplit('/')
            .next()
            .and_then(|s| s.strip_suffix(".im"))
            .unwrap_or_default()
            .to_string()
    }
}

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
                // todo(hl): we can keep this warning once we find a way to filter self in list result.
                debug!("Unexpected entry in index creation dir: {:?}", entry.path());
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
    use std::ffi::OsStr;

    use common_test_util::temp_dir;
    use futures::{AsyncReadExt, AsyncWriteExt};
    use regex::Regex;
    use store_api::storage::RegionId;

    use super::*;
    use crate::sst::file::FileId;

    #[tokio::test]
    async fn test_manager() {
        let temp_dir = temp_dir::create_temp_dir("index_intermediate");
        let path = temp_dir.path().to_str().unwrap();

        // write a garbage file
        tokio::fs::create_dir_all(format!("{path}/{INTERMEDIATE_DIR}"))
            .await
            .unwrap();
        tokio::fs::write(format!("{path}/{INTERMEDIATE_DIR}/garbage.im"), "blahblah")
            .await
            .unwrap();

        let _manager = IntermediateManager::init_fs(path).await.unwrap();

        // cleaned up by `init_fs`
        assert!(!tokio::fs::try_exists(format!("{path}/{INTERMEDIATE_DIR}"))
            .await
            .unwrap());
    }

    #[test]
    fn test_intermediate_location() {
        let sst_file_id = FileId::random();
        let location = IntermediateLocation::new(&RegionId::new(0, 0), &sst_file_id);

        let re = Regex::new(&format!(
            "{INTERMEDIATE_DIR}/0/{sst_file_id}/{}/",
            r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}"
        ))
        .unwrap();
        assert!(re.is_match(&location.files_dir));

        let uuid = location.files_dir.split('/').nth(3).unwrap();

        let file_group = "1";
        assert_eq!(
            location.file_group_path(file_group),
            format!("{INTERMEDIATE_DIR}/0/{sst_file_id}/{uuid}/{file_group}/")
        );

        let im_file_id = "000000000010";
        let file_path = location.file_path(file_group, im_file_id);
        assert_eq!(
            file_path,
            format!("{INTERMEDIATE_DIR}/0/{sst_file_id}/{uuid}/{file_group}/{im_file_id}.im")
        );

        assert_eq!(location.im_file_id_from_path(&file_path), im_file_id);
    }

    #[tokio::test]
    async fn test_fulltext_intm_path() {
        let temp_dir = temp_dir::create_temp_dir("test_fulltext_intm_path_");
        let aux_path = temp_dir.path().to_string_lossy().to_string();

        let manager = IntermediateManager::init_fs(&aux_path).await.unwrap();
        let region_id = RegionId::new(0, 0);
        let sst_file_id = FileId::random();
        let column_id = 1;
        let fulltext_path = manager.fulltext_path(&region_id, &sst_file_id, column_id);

        let mut pi = fulltext_path.iter();
        for a in temp_dir.path().iter() {
            assert_eq!(a, pi.next().unwrap());
        }
        assert_eq!(pi.next().unwrap(), INTERMEDIATE_DIR);
        assert_eq!(pi.next().unwrap(), "0"); // region id
        assert_eq!(pi.next().unwrap(), OsStr::new(&sst_file_id.to_string())); // sst file id
        assert!(Regex::new(r"fulltext-1-\w{8}-\w{4}-\w{4}-\w{4}-\w{12}")
            .unwrap()
            .is_match(&pi.next().unwrap().to_string_lossy())); // fulltext path
        assert!(pi.next().is_none());
    }

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
