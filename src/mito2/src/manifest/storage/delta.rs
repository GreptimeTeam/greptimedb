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
use common_telemetry::{debug, warn};
use futures::TryStreamExt;
use futures::future::try_join_all;
use object_store::{Buffer, Entry, ErrorKind, Lister, ObjectStore};
use snafu::{ResultExt, ensure};
use store_api::ManifestVersion;
use store_api::storage::RegionId;
use tokio::sync::Semaphore;

use crate::cache::manifest_cache::ManifestCache;
use crate::error::{
    CompressObjectSnafu, DecompressObjectSnafu, InvalidScanIndexSnafu, ManifestDeltaNotFoundSnafu,
    ManifestVersionConflictSnafu, OpenDalSnafu, Result,
};
use crate::manifest::storage::size_tracker::Tracker;
use crate::manifest::storage::utils::{
    get_from_cache_decoded, sort_manifests, write_and_put_cache,
};
use crate::manifest::storage::{
    FETCH_MANIFEST_PARALLELISM, delta_file, file_compress_type, file_version, gen_path,
    is_delta_file, list_start_after,
};

#[derive(Debug, Clone)]
pub(crate) struct DeltaStorage<T: Tracker> {
    object_store: ObjectStore,
    compress_type: CompressionType,
    path: String,
    delta_tracker: Arc<T>,
    manifest_cache: Option<ManifestCache>,
}

impl<T: Tracker> DeltaStorage<T> {
    pub(crate) fn new(
        path: String,
        object_store: ObjectStore,
        compress_type: CompressionType,
        manifest_cache: Option<ManifestCache>,
        delta_tracker: Arc<T>,
    ) -> Self {
        Self {
            object_store,
            compress_type,
            path,
            delta_tracker,
            manifest_cache,
        }
    }

    fn delta_file_path(&self, version: ManifestVersion) -> String {
        gen_path(&self.path, &delta_file(version), self.compress_type)
    }

    /// Returns an iterator of manifests from path directory.
    ///
    /// If `start_after` is `Some`, the lister will skip entries whose name is
    /// lexicographically less than or equal to it (see OpenDAL's `start_after`).
    pub(crate) async fn manifest_lister(
        &self,
        start_after: Option<&str>,
    ) -> Result<Option<Lister>> {
        let mut builder = self.object_store.lister_with(&self.path);
        if let Some(s) = start_after {
            builder = builder.start_after(s);
        }
        match builder.await {
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
    ///
    /// `start_after` is forwarded to the underlying lister to skip entries
    /// whose name is lexicographically less than or equal to it.
    pub async fn get_paths<F, R>(&self, start_after: Option<&str>, mut filter: F) -> Result<Vec<R>>
    where
        F: FnMut(Entry) -> Option<R>,
    {
        let Some(streamer) = self.manifest_lister(start_after).await? else {
            return Ok(vec![]);
        };

        streamer
            .try_filter_map(|e| {
                let result = filter(e);
                async { Ok(result) }
            })
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

        // Push the version lower bound into the list request via
        // `list_start_after`; skip the hint when `start == 0` (nothing to skip).
        let start_after = (start > 0).then(|| list_start_after(&self.path, start));
        let mut total_paths = 0;
        let mut entries: Vec<(ManifestVersion, Entry)> = self
            .get_paths(start_after.as_deref(), |entry| {
                total_paths += 1;
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

        common_telemetry::debug!(
            "DeltaStorage get paths for {}, start: {}, end: {}, start_after: {:?}, total_paths: {}, entries: {}",
            self.path,
            start,
            end,
            start_after,
            total_paths,
            entries.len()
        );

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
            let compress_type = file_compress_type(entry.name());
            // Try to get from cache first
            if let Some(data) = get_from_cache_decoded(self.manifest_cache.as_ref(), cache_key, compress_type).await {
                let size = entry.metadata().content_length();
                if size > 0 {
                    self.delta_tracker.record(*v, size);
                } else {
                    match self.object_store.stat(entry.path()).await {
                        Ok(metadata) => self.delta_tracker.record(*v, metadata.content_length()),
                        Err(error) => warn!(error; "Failed to account for cached manifest delta {}", entry.path()),
                    }
                }
                return Ok((*v, data));
            }

            // Fetch from remote object store
            let bytes = match self.object_store.read(entry.path()).await {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    return Err(error).context(ManifestDeltaNotFoundSnafu {
                        version: *v,
                        path: entry.path(),
                    });
                }
                Err(error) => return Err(error).context(OpenDalSnafu),
            };
            self.delta_tracker.record(*v, bytes.len() as u64);
            let data = compress_type
                .decode(bytes.clone())
                .await
                .context(DecompressObjectSnafu {
                    compress_type,
                    path: entry.path(),
                })?;

            // Add to cache
            if let Some(cache) = &self.manifest_cache {
                cache.put_file(cache_key.to_string(), bytes.to_vec()).await;
            }

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

    /// Creates the delta of `version` under its fixed `.json.gz` name, so that only one writer can
    /// commit the version.
    ///
    /// An existing object with the same decoded content is a retry of an earlier write that
    /// landed. Any other existing object fails with a version conflict.
    pub(crate) async fn save_conditional(
        &mut self,
        version: ManifestVersion,
        bytes: &[u8],
    ) -> Result<()> {
        let path = gen_path(&self.path, &delta_file(version), CompressionType::Gzip);
        // Every writer has to use one object name, so `compress_manifest = false` is honoured
        // with gzip level 0, which stores the bytes without compressing them.
        let data = if self.compress_type.is_compressed() {
            CompressionType::Gzip.encode(bytes).await
        } else {
            CompressionType::encode_gzip_stored(bytes).await
        }
        .context(CompressObjectSnafu {
            compress_type: CompressionType::Gzip,
            path: &path,
        })?;
        let data = Buffer::from(data);

        let stored_size = match self
            .object_store
            .write_with(&path, data.clone())
            .if_not_exists(true)
            .await
        {
            Ok(_) => data.len(),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
                ) =>
            {
                self.ensure_same_delta(version, &path, bytes).await?
            }
            Err(error) => return Err(error).context(OpenDalSnafu),
        };

        self.delta_tracker.record(version, stored_size as u64);
        if let Some(cache) = &self.manifest_cache {
            cache.put_file(path, data.to_vec()).await;
        }

        Ok(())
    }

    /// Checks that the object at `path` decodes to `bytes`, reading it directly from storage.
    /// Returns the size of the stored object.
    async fn ensure_same_delta(
        &self,
        version: ManifestVersion,
        path: &str,
        bytes: &[u8],
    ) -> Result<usize> {
        let conflict = |reason: String| {
            ManifestVersionConflictSnafu {
                version,
                manifest_dir: &self.path,
                reason,
            }
            .build()
        };
        let (stored_size, existing) = self
            .read_delta_at(path, CompressionType::Gzip)
            .await
            .map_err(|e| conflict(format!("cannot read the existing delta: {e}")))?
            .ok_or_else(|| conflict("the existing delta is gone".to_string()))?;
        if existing != bytes {
            return Err(conflict(
                "the existing delta has different content".to_string(),
            ));
        }
        Ok(stored_size)
    }

    /// Reads the delta of `version` that [Self::save] writes directly from storage, and returns
    /// whether it holds `bytes` (then recording its size), or `None` if it does not exist.
    pub(crate) async fn holds_delta(
        &self,
        version: ManifestVersion,
        bytes: &[u8],
    ) -> Result<Option<bool>> {
        let path = self.delta_file_path(version);
        let delta = self.read_delta_at(&path, self.compress_type).await?;
        Ok(delta.map(|(size, data)| {
            let same = data == bytes;
            if same {
                self.delta_tracker.record(version, size as u64);
            }
            same
        }))
    }

    /// Reads and decodes the object at `path`, bypassing the cache. Returns its stored size and
    /// content, or `None` if it does not exist.
    async fn read_delta_at(
        &self,
        path: &str,
        compress_type: CompressionType,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        let stored = match self.object_store.read(path).await {
            Ok(stored) => stored,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).context(OpenDalSnafu),
        };
        let stored_size = stored.len();
        let data = compress_type
            .decode(stored)
            .await
            .context(DecompressObjectSnafu {
                compress_type,
                path,
            })?;
        Ok(Some((stored_size, data)))
    }
}

#[cfg(test)]
impl<T: Tracker> DeltaStorage<T> {
    pub fn set_compress_type(&mut self, compress_type: CompressionType) {
        self.compress_type = compress_type;
    }
}
