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

use common_telemetry::warn;
use object_store::util::{self, normalize_dir};
use store_api::storage::{ColumnId, RegionId};
use uuid::Uuid;

use crate::access_layer::new_fs_cache_store;
use crate::error::Result;
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

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use common_test_util::temp_dir;
    use regex::Regex;

    use super::*;

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
}
