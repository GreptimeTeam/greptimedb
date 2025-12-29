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

use common_datasource::compression::CompressionType;
use common_telemetry::debug;
use object_store::{Lister, ObjectStore, util};
use snafu::ResultExt;
use store_api::ManifestVersion;

use crate::error::{OpenDalSnafu, Result};
use crate::manifest::storage::delta::DeltaStorage;
use crate::manifest::storage::size_tracker::NoopTracker;
use crate::manifest::storage::utils::sort_manifests;
use crate::manifest::storage::{file_version, is_delta_file};

/// A simple key-value storage for arbitrary data in the staging directory.
///
/// This is primarily used during repartition operations to store generated
/// manifests that will be consumed by other regions via [`ApplyStagingManifestRequest`](store_api::region_request::ApplyStagingManifestRequest).
/// The data is stored in `{staging_path}/data/` directory.
#[derive(Debug, Clone)]
pub(crate) struct StagingDataStorage {
    object_store: ObjectStore,
    path: String,
}

impl StagingDataStorage {
    pub fn new(path: String, object_store: ObjectStore) -> Self {
        let path = util::normalize_dir(&format!("{}/data/", path));
        common_telemetry::debug!(
            "Staging data storage path: {}, root: {}",
            path,
            object_store.info().root()
        );
        Self { object_store, path }
    }

    /// Put the bytes to the data storage.
    pub async fn put(&self, path: &str, bytes: Vec<u8>) -> Result<()> {
        let path = format!("{}{}", self.path, path);
        common_telemetry::debug!(
            "Putting data to staging data storage, path: {}, root: {}, bytes: {}",
            path,
            self.object_store.info().root(),
            bytes.len()
        );
        self.object_store
            .write(&path, bytes)
            .await
            .context(OpenDalSnafu)?;
        Ok(())
    }

    /// Get the bytes from the data storage.
    pub async fn get(&self, path: &str) -> Result<Vec<u8>> {
        let path = format!("{}{}", self.path, path);
        common_telemetry::debug!(
            "Reading data from staging data storage, path: {}, root: {}",
            path,
            self.object_store.info().root()
        );
        let bytes = self.object_store.read(&path).await.context(OpenDalSnafu)?;

        Ok(bytes.to_vec())
    }
}

/// Storage for staging manifest files and data during repartition operations.
///
/// Contains:
/// - `delta_storage`: Stores incremental manifest delta files for the staging region.
/// - `data_storage`: Stores arbitrary data (e.g., generated manifests for regions).
#[derive(Debug, Clone)]
pub(crate) struct StagingStorage {
    delta_storage: DeltaStorage<NoopTracker>,
    data_storage: StagingDataStorage,
}

/// Returns the staging path from the manifest path.
///
/// # Example
/// - Input: `"data/table/region_0001/manifest/"`
/// - Output: `"data/table/region_0001/staging/manifest/"`
pub fn staging_path(manifest_path: &str) -> String {
    let parent_dir = manifest_path
        .trim_end_matches("manifest/")
        .trim_end_matches('/');
    util::normalize_dir(&format!("{}/staging/manifest", parent_dir))
}

impl StagingStorage {
    pub fn new(path: String, object_store: ObjectStore, compress_type: CompressionType) -> Self {
        let staging_path = staging_path(&path);
        let data_storage = StagingDataStorage::new(staging_path.clone(), object_store.clone());
        let delta_storage = DeltaStorage::new(
            staging_path.clone(),
            object_store.clone(),
            compress_type,
            // StagingStorage does not use a manifest cache; set to None.
            None,
            // StagingStorage does not track file sizes, since all staging files are
            // deleted after exiting staging mode.
            Arc::new(NoopTracker),
        );

        Self {
            delta_storage,
            data_storage,
        }
    }

    /// Returns the data storage.
    pub(crate) fn data_storage(&self) -> &StagingDataStorage {
        &self.data_storage
    }

    /// Returns an iterator of manifests from staging directory.
    pub(crate) async fn manifest_lister(&self) -> Result<Option<Lister>> {
        self.delta_storage.manifest_lister().await
    }

    /// Fetch all staging manifest files and return them as (version, action_list) pairs.
    pub(crate) async fn fetch_manifests(&self) -> Result<Vec<(ManifestVersion, Vec<u8>)>> {
        let manifest_entries = self
            .delta_storage
            .get_paths(|entry| {
                let file_name = entry.name();
                if is_delta_file(file_name) {
                    let version = file_version(file_name);
                    Some((version, entry))
                } else {
                    None
                }
            })
            .await?;

        let mut sorted_entries = manifest_entries;
        sort_manifests(&mut sorted_entries);

        self.delta_storage
            .fetch_manifests_from_entries(sorted_entries)
            .await
    }

    /// Save the delta manifest file.
    pub(crate) async fn save(&mut self, version: ManifestVersion, bytes: &[u8]) -> Result<()> {
        self.delta_storage.save(version, bytes).await
    }

    /// Clean all staging manifest files.
    pub(crate) async fn clear(&self) -> Result<()> {
        self.delta_storage
            .object_store()
            .remove_all(self.delta_storage.path())
            .await
            .context(OpenDalSnafu)?;

        debug!(
            "Cleared all staging manifest files from {}",
            self.delta_storage.path()
        );

        Ok(())
    }
}

#[cfg(test)]
impl StagingStorage {
    pub fn set_compress_type(&mut self, compress_type: CompressionType) {
        self.delta_storage.set_compress_type(compress_type);
    }
}

#[cfg(test)]
mod tests {
    use crate::manifest::storage::staging::staging_path;

    #[test]
    fn test_staging_path() {
        let path = "/data/table/region_0001/manifest/";
        let expected = "/data/table/region_0001/staging/manifest/";
        assert_eq!(staging_path(path), expected);
    }
}
