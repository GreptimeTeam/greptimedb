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

use common_datasource::compression::CompressionType;
use common_telemetry::debug;
use futures::TryStreamExt;
use futures::future::try_join_all;
use object_store::{Entry, ErrorKind, Lister, ObjectStore};
use snafu::{ResultExt, ensure};
use store_api::ManifestVersion;
use store_api::storage::RegionId;
use tokio::sync::Semaphore;

use crate::cache::manifest_cache::ManifestCache;
use crate::error::{
    CompressObjectSnafu, DecompressObjectSnafu, InvalidScanIndexSnafu, OpenDalSnafu, Result,
};
use crate::manifest::storage::size_tracker::TrackerRef;
use crate::manifest::storage::utils::{
    get_from_cache, put_to_cache, sort_manifests, write_and_put_cache,
};
use crate::manifest::storage::{
    FETCH_MANIFEST_PARALLELISM, delta_file, file_compress_type, file_version, gen_path,
    is_delta_file,
};

#[derive(Debug, Clone)]
pub(crate) struct DeltaStorage {
    object_store: ObjectStore,
    compress_type: CompressionType,
    path: String,
    delta_tracker: TrackerRef,
    manifest_cache: Option<ManifestCache>,
}

impl DeltaStorage {
    pub(crate) fn new(
        path: String,
        object_store: ObjectStore,
        compress_type: CompressionType,
        manifest_cache: Option<ManifestCache>,
        delta_tracker: TrackerRef,
    ) -> Self {
        Self {
            object_store,
            compress_type,
            path,
            delta_tracker,
            manifest_cache,
        }
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn object_store(&self) -> &ObjectStore {
        &self.object_store
    }

    fn delta_file_path(&self, version: ManifestVersion) -> String {
        gen_path(&self.path, &delta_file(version), self.compress_type)
    }

    /// Returns an iterator of manifests from path directory.
    pub(crate) async fn manifest_lister(&self) -> Result<Option<Lister>> {
        match self.object_store.lister_with(&self.path).await {
            Ok(streamer) => Ok(Some(streamer)),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                debug!("Manifest directory does not exist: {}", self.path);
                Ok(None)
            }
            Err(e) => Err(e).context(OpenDalSnafu)?,
        }
    }

    /// Return all `R`s in the directory that meet the `filter` conditions (that is, the `filter` closure returns `Some(R)`),
    /// and discard `R` that does not meet the conditions (that is, the `filter` closure returns `None`)
    /// Return an empty vector when directory is not found.
    pub async fn get_paths<F, R>(&self, filter: F) -> Result<Vec<R>>
    where
        F: Fn(Entry) -> Option<R>,
    {
        let Some(streamer) = self.manifest_lister().await? else {
            return Ok(vec![]);
        };

        streamer
            .try_filter_map(|e| async { Ok(filter(e)) })
            .try_collect::<Vec<_>>()
            .await
            .context(OpenDalSnafu)
    }

    /// Scans the manifest files in the range of [start, end) and return all manifest entries.
    pub async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<Vec<(ManifestVersion, Entry)>> {
        ensure!(start <= end, InvalidScanIndexSnafu { start, end });

        let mut entries: Vec<(ManifestVersion, Entry)> = self
            .get_paths(|entry| {
                let file_name = entry.name();
                if is_delta_file(file_name) {
                    let version = file_version(file_name);
                    if start <= version && version < end {
                        return Some((version, entry));
                    }
                }
                None
            })
            .await?;

        sort_manifests(&mut entries);

        Ok(entries)
    }

    /// Fetches manifests in range [start_version, end_version).
    ///
    /// This functions is guaranteed to return manifests from the `start_version` strictly (must contain `start_version`).
    pub async fn fetch_manifests_strict_from(
        &self,
        start_version: ManifestVersion,
        end_version: ManifestVersion,
        region_id: RegionId,
    ) -> Result<Vec<(ManifestVersion, Vec<u8>)>> {
        let mut manifests = self.fetch_manifests(start_version, end_version).await?;
        let start_index = manifests.iter().position(|(v, _)| *v == start_version);
        debug!(
            "Fetches manifests in range [{},{}), start_index: {:?}, region_id: {}, manifests: {:?}",
            start_version,
            end_version,
            start_index,
            region_id,
            manifests.iter().map(|(v, _)| *v).collect::<Vec<_>>()
        );
        if let Some(start_index) = start_index {
            Ok(manifests.split_off(start_index))
        } else {
            Ok(vec![])
        }
    }

    /// Common implementation for fetching manifests from entries in parallel.
    pub(crate) async fn fetch_manifests_from_entries(
        &self,
        entries: Vec<(ManifestVersion, Entry)>,
    ) -> Result<Vec<(ManifestVersion, Vec<u8>)>> {
        if entries.is_empty() {
            return Ok(vec![]);
        }

        // TODO(weny): Make it configurable.
        let semaphore = Semaphore::new(FETCH_MANIFEST_PARALLELISM);

        let tasks = entries.iter().map(|(v, entry)| async {
            // Safety: semaphore must exist.
            let _permit = semaphore.acquire().await.unwrap();

            let cache_key = entry.path();
            // Try to get from cache first
            if let Some(data) = get_from_cache(self.manifest_cache.as_ref(), cache_key).await {
                return Ok((*v, data));
            }

            // Fetch from remote object store
            let compress_type = file_compress_type(entry.name());
            let bytes = self
                .object_store
                .read(entry.path())
                .await
                .context(OpenDalSnafu)?;
            let data = compress_type
                .decode(bytes)
                .await
                .context(DecompressObjectSnafu {
                    compress_type,
                    path: entry.path(),
                })?;

            // Add to cache
            put_to_cache(self.manifest_cache.as_ref(), cache_key.to_string(), &data).await;

            Ok((*v, data))
        });

        try_join_all(tasks).await
    }

    /// Fetch all manifests in concurrent, and return the manifests in range [start_version, end_version)
    ///
    /// **Notes**: This function is no guarantee to return manifests from the `start_version` strictly.
    /// Uses [fetch_manifests_strict_from](DeltaStorage::fetch_manifests_strict_from) to get manifests from the `start_version`.
    pub async fn fetch_manifests(
        &self,
        start_version: ManifestVersion,
        end_version: ManifestVersion,
    ) -> Result<Vec<(ManifestVersion, Vec<u8>)>> {
        let manifests = self.scan(start_version, end_version).await?;
        self.fetch_manifests_from_entries(manifests).await
    }

    /// Save the delta manifest file.
    pub async fn save(&mut self, version: ManifestVersion, bytes: &[u8]) -> Result<()> {
        let path = self.delta_file_path(version);
        debug!("Save log to manifest storage, version: {}", version);
        let data = self
            .compress_type
            .encode(bytes)
            .await
            .context(CompressObjectSnafu {
                compress_type: self.compress_type,
                path: &path,
            })?;
        let delta_size = data.len();

        write_and_put_cache(
            &self.object_store,
            self.manifest_cache.as_ref(),
            &path,
            data,
        )
        .await?;
        self.delta_tracker.record(version, delta_size as u64);

        Ok(())
    }
}

#[cfg(test)]
impl DeltaStorage {
    pub fn set_compress_type(&mut self, compress_type: CompressionType) {
        self.compress_type = compress_type;
    }
}
