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

#[derive(Debug, Clone)]
pub(crate) struct StagingStorage {
    delta_storage: DeltaStorage,
}

impl StagingStorage {
    pub fn new(path: String, object_store: ObjectStore, compress_type: CompressionType) -> Self {
        let staging_path = {
            // Convert "region_dir/manifest/" to "region_dir/staging/manifest/"
            let parent_dir = path.trim_end_matches("manifest/").trim_end_matches('/');
            util::normalize_dir(&format!("{}/staging/manifest", parent_dir))
        };

        let delta_storage = DeltaStorage::new(
            staging_path.clone(),
            object_store.clone(),
            compress_type,
            None,
            Arc::new(NoopTracker),
        );
        Self { delta_storage }
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
