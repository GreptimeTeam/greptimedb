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

pub(crate) mod checkpoint;
pub(crate) mod delta;
pub(crate) mod size_tracker;
pub(crate) mod staging;
pub(crate) mod utils;

use std::iter::Iterator;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use common_datasource::compression::CompressionType;
use common_error::ext::ErrorExt;
use common_telemetry::{debug, warn};
use crc32fast::Hasher;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use object_store::util::join_dir;
use object_store::{Lister, ObjectStore, util};
use regex::Regex;
use snafu::{ResultExt, ensure};
use store_api::storage::RegionId;
use store_api::{MIN_VERSION, ManifestVersion};

use crate::cache::manifest_cache::ManifestCache;
use crate::error::{
    ChecksumMismatchSnafu, ManifestVersionConflictSnafu, OpenDalSnafu, Result,
    UnsupportedOperationSnafu,
};
use crate::manifest::storage::checkpoint::CheckpointStorage;
use crate::manifest::storage::delta::DeltaStorage;
use crate::manifest::storage::size_tracker::{CheckpointTracker, DeltaTracker, SizeTracker};
use crate::manifest::storage::staging::StagingStorage;
use crate::manifest::storage::utils::remove_from_cache;
use crate::metrics::manifest_size::RegionManifestSizeMetric;

lazy_static! {
    static ref DELTA_RE: Regex = Regex::new("^\\d+\\.json").unwrap();
    static ref CHECKPOINT_RE: Regex = Regex::new("^\\d+\\.checkpoint").unwrap();
}

pub const LAST_CHECKPOINT_FILE: &str = "_last_checkpoint";
const DEFAULT_MANIFEST_COMPRESSION_TYPE: CompressionType = CompressionType::Gzip;
/// Due to backward compatibility, it is possible that the user's manifest file has not been compressed.
/// So when we encounter problems, we need to fall back to `FALL_BACK_COMPRESS_TYPE` for processing.
pub(crate) const FALL_BACK_COMPRESS_TYPE: CompressionType = CompressionType::Uncompressed;
const FETCH_MANIFEST_PARALLELISM: usize = 16;

/// Returns the directory to the manifest files.
pub fn manifest_dir(region_dir: &str) -> String {
    join_dir(region_dir, "manifest")
}

/// Returns the [CompressionType] according to whether to compress manifest files.
pub const fn manifest_compress_type(compress: bool) -> CompressionType {
    if compress {
        DEFAULT_MANIFEST_COMPRESSION_TYPE
    } else {
        FALL_BACK_COMPRESS_TYPE
    }
}

pub fn delta_file(version: ManifestVersion) -> String {
    format!("{version:020}.json")
}

pub fn checkpoint_file(version: ManifestVersion) -> String {
    format!("{version:020}.checkpoint")
}

/// Returns a lexicographic `start_after` key for an object-store `list`
/// request over the manifest directory at `path`.
///
/// `path` must be the same directory prefix passed to `lister_with(path)`
/// and must end with `/`. OpenDAL resolves `start_after` against the
/// operator root, not relative to the listed path, so the caller must
/// supply the full prefix — otherwise the bound is compared against keys
/// that already share a longer prefix and is silently a no-op.
pub(crate) fn list_start_after(path: &str, version: ManifestVersion) -> String {
    debug_assert!(
        path.ends_with('/'),
        "list_start_after: path must end with '/', got {path:?}",
    );
    // Manifest files are named `{version:020}.{json,checkpoint}[.gz]` and sort lexicographically;
    // `{path}{version:020}` is a strict prefix of `{path}{version:020}.{json,checkpoint}[.gz]`.
    format!("{path}{version:020}")
}

pub fn gen_path(path: &str, file: &str, compress_type: CompressionType) -> String {
    if compress_type == CompressionType::Uncompressed {
        format!("{}{}", path, file)
    } else {
        format!("{}{}.{}", path, file, compress_type.file_extension())
    }
}

pub(crate) fn checkpoint_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

pub(crate) fn verify_checksum(data: &[u8], wanted: Option<u32>) -> Result<()> {
    if let Some(checksum) = wanted {
        let calculated_checksum = checkpoint_checksum(data);
        ensure!(
            checksum == calculated_checksum,
            ChecksumMismatchSnafu {
                actual: calculated_checksum,
                expected: checksum,
            }
        );
    }
    Ok(())
}

/// Return's the file manifest version from path
///
/// # Panics
/// If the file path is not a valid delta or checkpoint file.
pub fn file_version(path: &str) -> ManifestVersion {
    let s = path.split('.').next().unwrap();
    s.parse().unwrap_or_else(|_| panic!("Invalid file: {path}"))
}

/// Return's the file compress algorithm by file extension.
///
/// for example file
/// `00000000000000000000.json.gz` -> `CompressionType::GZIP`
pub fn file_compress_type(path: &str) -> CompressionType {
    let s = path.rsplit('.').next().unwrap_or("");
    CompressionType::from_str(s).unwrap_or(CompressionType::Uncompressed)
}

pub fn is_delta_file(file_name: &str) -> bool {
    DELTA_RE.is_match(file_name)
}

pub fn is_checkpoint_file(file_name: &str) -> bool {
    CHECKPOINT_RE.is_match(file_name)
}

#[derive(Clone, Debug)]
pub struct ManifestObjectStore {
    object_store: ObjectStore,
    path: String,
    /// Optional manifest cache for local caching.
    manifest_cache: Option<ManifestCache>,
    // Tracks the size of each file in the manifest directory.
    size_tracker: SizeTracker,
    // The checkpoint file storage.
    checkpoint_storage: CheckpointStorage<CheckpointTracker>,
    // The delta file storage.
    delta_storage: DeltaStorage<DeltaTracker>,
    /// The staging file storage.
    staging_storage: StagingStorage,
    /// Whether deltas are created conditionally and recovery ignores the checkpoint pointer.
    conditional_publication: bool,
}

impl ManifestObjectStore {
    pub fn new(
        path: &str,
        object_store: ObjectStore,
        compress_type: CompressionType,
        total_manifest_size: Arc<AtomicU64>,
        manifest_cache: Option<ManifestCache>,
    ) -> Self {
        Self::new_with_metric(
            path,
            object_store,
            compress_type,
            total_manifest_size,
            None,
            manifest_cache,
        )
    }

    /// Constructs a store sharing the region instance's publication state.
    pub(crate) fn new_with_metric(
        path: &str,
        object_store: ObjectStore,
        compress_type: CompressionType,
        total_manifest_size: Arc<AtomicU64>,
        metric: Option<RegionManifestSizeMetric>,
        manifest_cache: Option<ManifestCache>,
    ) -> Self {
        common_telemetry::info!("Create manifest store, cache: {}", manifest_cache.is_some());

        let path = util::normalize_dir(path);
        let size_tracker = SizeTracker::new(total_manifest_size, metric);
        let checkpoint_tracker = Arc::new(size_tracker.checkpoint_tracker());
        let delta_tracker = Arc::new(size_tracker.manifest_tracker());
        let checkpoint_storage = CheckpointStorage::new(
            path.clone(),
            object_store.clone(),
            compress_type,
            manifest_cache.clone(),
            checkpoint_tracker,
        );
        let delta_storage = DeltaStorage::new(
            path.clone(),
            object_store.clone(),
            compress_type,
            manifest_cache.clone(),
            delta_tracker,
        );
        let staging_storage =
            StagingStorage::new(path.clone(), object_store.clone(), compress_type);

        Self {
            object_store,
            path,
            manifest_cache,
            size_tracker,
            checkpoint_storage,
            delta_storage,
            staging_storage,
            conditional_publication: false,
        }
    }

    /// Sets whether this store publishes deltas by conditional create and recovers from the
    /// highest checkpoint found by LIST.
    ///
    /// Returns an error if the mode is enabled on a backend without conditional create.
    pub(crate) fn with_conditional_publication(mut self, enabled: bool) -> Result<Self> {
        let capability = self.object_store.info().capability();
        ensure!(
            !enabled || capability.write_with_if_not_exists,
            UnsupportedOperationSnafu {
                err_msg: format!(
                    "conditional create is required for manifest {} of a region whose writes need a lease",
                    self.path
                ),
            }
        );
        self.conditional_publication = enabled;
        Ok(self)
    }

    /// Returns whether this store publishes deltas by conditional create.
    pub(crate) fn is_conditional_publication(&self) -> bool {
        self.conditional_publication
    }

    /// Returns the manifest dir
    pub(crate) fn manifest_dir(&self) -> &str {
        &self.path
    }

    /// Returns an iterator of manifests from normal or staging directory.
    ///
    /// `start_after` is forwarded to the non-staging lister to skip entries
    /// whose name is lexicographically less than or equal to it. It is
    /// ignored for the staging directory.
    pub(crate) async fn manifest_lister(
        &self,
        is_staging: bool,
        start_after: Option<&str>,
    ) -> Result<Option<Lister>> {
        if is_staging {
            self.staging_storage.manifest_lister().await
        } else {
            self.delta_storage.manifest_lister(start_after).await
        }
    }

    /// Fetches manifests in range [start_version, end_version).
    /// This functions is guaranteed to return manifests from the `start_version` strictly (must contain `start_version`).
    pub async fn fetch_manifests_strict_from(
        &self,
        start_version: ManifestVersion,
        end_version: ManifestVersion,
        region_id: RegionId,
    ) -> Result<Vec<(ManifestVersion, Vec<u8>)>> {
        self.delta_storage
            .fetch_manifests_strict_from(start_version, end_version, region_id)
            .await
    }

    /// Fetch all manifests in concurrent, and return the manifests in range [start_version, end_version)
    ///
    /// **Notes**: This function is no guarantee to return manifests from the `start_version` strictly.
    /// Uses [fetch_manifests_strict_from](ManifestObjectStore::fetch_manifests_strict_from) to get manifests from the `start_version`.
    pub async fn fetch_manifests(
        &self,
        start_version: ManifestVersion,
        end_version: ManifestVersion,
    ) -> Result<Vec<(ManifestVersion, Vec<u8>)>> {
        self.delta_storage
            .fetch_manifests(start_version, end_version)
            .await
    }

    /// Delete manifest files that version < end.
    /// If keep_last_checkpoint is true, the last checkpoint file will be kept.
    /// ### Return
    /// The number of files whose deletion was acknowledged by the object store.
    pub async fn delete_until(
        &self,
        end: ManifestVersion,
        keep_last_checkpoint: bool,
    ) -> Result<usize> {
        // Stores (entry, is_checkpoint, version) in a Vec.
        //
        // `start_after` is intentionally `None` here: a previous deletion
        // may have been interrupted and left stale files at versions below
        // the current checkpoint; we need the lister to surface them so
        // cleanup can finish.
        let entries: Vec<_> = self
            .delta_storage
            .get_paths(None, |entry| {
                let file_name = entry.name();
                let is_checkpoint = is_checkpoint_file(file_name);
                if is_delta_file(file_name) || is_checkpoint_file(file_name) {
                    let version = file_version(file_name);
                    if version < end {
                        return Some((entry, is_checkpoint, version));
                    }
                }
                None
            })
            .await?;
        let checkpoint_version = if keep_last_checkpoint {
            // Note that the order of entries is unspecific.
            entries
                .iter()
                .filter_map(
                    |(_e, is_checkpoint, version)| {
                        if *is_checkpoint { Some(version) } else { None }
                    },
                )
                .max()
        } else {
            None
        };
        let del_entries: Vec<_> = entries
            .iter()
            .filter(|(_e, is_checkpoint, version)| {
                if let Some(max_version) = checkpoint_version {
                    if *is_checkpoint {
                        // We need to keep the checkpoint file.
                        version < max_version
                    } else {
                        // We can delete the log file with max_version as the checkpoint
                        // file contains the log file's content.
                        version <= max_version
                    }
                } else {
                    true
                }
            })
            .collect();
        let paths = del_entries
            .iter()
            .map(|(e, _, _)| e.path().to_string())
            .collect::<Vec<_>>();
        let total = paths.len();

        debug!(
            "Deleting {} logs from manifest storage path {} until {}, checkpoint_version: {:?}, paths: {:?}",
            total, self.path, end, checkpoint_version, paths,
        );

        // Remove from cache first
        for (entry, _, _) in &del_entries {
            remove_from_cache(self.manifest_cache.as_ref(), entry.path()).await;
        }

        // Try batch delete first. On failure, fall back to per-file deletes.
        // This is a workaround for S3-compatible object stores that do not support batch delete. See issue #7986.
        let mut deleted = 0;
        match self.object_store.delete_iter(paths.clone()).await {
            Ok(()) => deleted = total,
            Err(batch_err) => {
                warn!(
                    batch_err;
                    "Batch delete failed for manifest path {}, falling back to per-file delete for {} paths",
                    self.path, total,
                );
                for (i, path) in paths.iter().enumerate() {
                    if let Err(e) = self.object_store.delete(path).await {
                        warn!(
                            e;
                            "Failed to delete manifest file {} under {}, aborting fallback, {} files will be retried on next checkpoint",
                            path, self.path, total - i,
                        );
                        break;
                    }
                    deleted += 1;
                }
            }
        }

        Ok(deleted)
    }

    /// Save the delta manifest file.
    pub async fn save(
        &mut self,
        version: ManifestVersion,
        bytes: &[u8],
        is_staging: bool,
    ) -> Result<()> {
        if is_staging {
            self.staging_storage.save(version, bytes).await
        } else if self.conditional_publication {
            self.delta_storage.save_conditional(version, bytes).await
        } else {
            self.delta_storage.save(version, bytes).await
        }
    }

    /// Fails with a version conflict unless LIST proves that no checkpoint at or above `version`
    /// exists.
    ///
    /// Cleanup deletes a delta only after a checkpoint that covers it is saved, and never deletes
    /// the highest checkpoint. So a late create that landed on a deleted name always finds one.
    pub(crate) async fn ensure_version_not_checkpointed(
        &self,
        version: ManifestVersion,
    ) -> Result<()> {
        let reason = match self.list_highest_checkpoint(version).await {
            Ok(None) => return Ok(()),
            Ok(Some((checkpoint, _))) => format!("covered by checkpoint {checkpoint}"),
            Err(e) => format!("cannot list the checkpoints: {}", e.output_msg()),
        };
        ManifestVersionConflictSnafu {
            version,
            manifest_dir: &self.path,
            reason,
        }
        .fail()
    }

    /// Save the checkpoint manifest file.
    pub(crate) async fn save_checkpoint(
        &self,
        version: ManifestVersion,
        bytes: &[u8],
    ) -> Result<()> {
        self.checkpoint_storage
            .save_checkpoint(version, bytes)
            .await
    }

    /// Load the latest checkpoint.
    /// Return manifest version and the raw [RegionCheckpoint](crate::manifest::action::RegionCheckpoint) content if any
    /// Also applies the checkpoint retention rule to the tracked size.
    pub async fn load_last_checkpoint(&mut self) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        let checkpoint = if self.conditional_publication {
            match self.list_highest_checkpoint(MIN_VERSION).await {
                Ok(Some((version, compress_type))) => {
                    self.checkpoint_storage
                        .load_listed_checkpoint(version, compress_type)
                        .await?
                }
                Ok(None) => None,
                // A region that does not exist yet can have no manifest directory.
                Err(e) if e.is_object_not_found() => None,
                Err(e) => return Err(e),
            }
        } else {
            self.checkpoint_storage.load_last_checkpoint().await?
        };
        if let Some((version, _)) = &checkpoint {
            self.retain_manifest_size_after(*version);
        }
        Ok(checkpoint)
    }

    /// Compute the size(Byte) in manifest size map.
    pub(crate) fn total_manifest_size(&self) -> u64 {
        self.size_tracker.total()
    }

    /// Accounts for the latest checkpoint already covered by this follower's replay.
    /// Only a changed or previously unaccounted checkpoint needs a stat.
    pub(crate) async fn update_manifest_size(
        &self,
        replayed_version: ManifestVersion,
    ) -> Result<()> {
        let Some(metadata) = self
            .checkpoint_storage
            .read_last_checkpoint_metadata()
            .await?
        else {
            return Ok(());
        };
        if metadata.version > replayed_version {
            // Pruning now would stop counting deltas still in this follower's recovery set.
            // Retention applies once replay covers this version or sync installs the checkpoint.
            return Ok(());
        }
        if !self.size_tracker.contains_checkpoint(metadata.version) {
            self.checkpoint_storage
                .record_checkpoint_size(metadata.version)
                .await?;
        }
        self.retain_manifest_size_after(metadata.version);
        Ok(())
    }

    /// Prunes logical accounting independently of physical cleanup success.
    pub(crate) fn retain_manifest_size_after(&self, version: ManifestVersion) {
        self.size_tracker.retain_after(version);
    }

    /// Fetch all staging manifest files and return them as (version, action_list) pairs.
    pub async fn fetch_staging_manifests(&self) -> Result<Vec<(ManifestVersion, Vec<u8>)>> {
        self.staging_storage.fetch_manifests().await
    }

    /// Clear all staging manifest files.
    pub async fn clear_staging_manifests(&mut self) -> Result<()> {
        self.staging_storage.clear().await
    }

    /// Returns the staging storage.
    pub(crate) fn staging_storage(&self) -> &StagingStorage {
        &self.staging_storage
    }

    /// Returns the version and compression of the highest checkpoint at or above `min_version`
    /// found by LIST.
    async fn list_highest_checkpoint(
        &self,
        min_version: ManifestVersion,
    ) -> Result<Option<(ManifestVersion, CompressionType)>> {
        let mut lister = self
            .object_store
            .lister_with(&self.path)
            .start_after(&list_start_after(&self.path, min_version))
            .await
            .context(OpenDalSnafu)?;
        let mut highest = None;
        while let Some(entry) = lister.try_next().await.context(OpenDalSnafu)? {
            let file_name = entry.name();
            if is_checkpoint_file(file_name) {
                let version = file_version(file_name);
                if version >= min_version && highest.is_none_or(|(v, _)| v < version) {
                    highest = Some((version, file_compress_type(file_name)));
                }
            }
        }
        Ok(highest)
    }
}

#[cfg(test)]
impl ManifestObjectStore {
    pub async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        self.object_store
            .read(path)
            .await
            .context(OpenDalSnafu)
            .map(|v| v.to_vec())
    }

    pub(crate) fn checkpoint_storage(&self) -> &CheckpointStorage<CheckpointTracker> {
        &self.checkpoint_storage
    }

    pub(crate) fn delta_storage(&self) -> &DeltaStorage<DeltaTracker> {
        &self.delta_storage
    }

    pub(crate) fn set_compress_type(&mut self, compress_type: CompressionType) {
        self.checkpoint_storage.set_compress_type(compress_type);
        self.delta_storage.set_compress_type(compress_type);
        self.staging_storage.set_compress_type(compress_type);
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::ObjectStore;
    use object_store::services::Fs;

    use super::*;
    use crate::manifest::storage::checkpoint::CheckpointMetadata;

    fn new_test_manifest_store() -> ManifestObjectStore {
        new_test_manifest_store_at("/")
    }

    fn new_test_manifest_store_at(path: &str) -> ManifestObjectStore {
        common_telemetry::init_default_ut_logging();
        let tmp_dir = create_temp_dir("test_manifest_log_store");
        let builder = Fs::default().root(&tmp_dir.path().to_string_lossy());
        let object_store = ObjectStore::new(builder).unwrap();
        ManifestObjectStore::new(
            path,
            object_store,
            CompressionType::Uncompressed,
            Default::default(),
            None,
        )
    }

    fn new_checkpoint_metadata_with_version(version: ManifestVersion) -> CheckpointMetadata {
        CheckpointMetadata {
            size: 0,
            version,
            checksum: None,
            extend_metadata: Default::default(),
        }
    }

    #[test]
    // Define this test mainly to prevent future unintentional changes may break the backward compatibility.
    fn test_compress_file_path_generation() {
        let path = "/foo/bar/";
        let version: ManifestVersion = 0;
        let file_path = gen_path(path, &delta_file(version), CompressionType::Gzip);
        assert_eq!(file_path.as_str(), "/foo/bar/00000000000000000000.json.gz")
    }

    #[tokio::test]
    async fn test_manifest_log_store_uncompress() {
        let mut log_store = new_test_manifest_store();
        log_store.set_compress_type(CompressionType::Uncompressed);
        test_manifest_log_store_case(log_store).await;
    }

    #[tokio::test]
    async fn test_manifest_log_store_compress() {
        let mut log_store = new_test_manifest_store();
        log_store.set_compress_type(CompressionType::Gzip);
        test_manifest_log_store_case(log_store).await;
    }

    async fn test_manifest_log_store_case(mut log_store: ManifestObjectStore) {
        for v in 0..5 {
            log_store
                .save(v, format!("hello, {v}").as_bytes(), false)
                .await
                .unwrap();
        }

        let manifests = log_store.fetch_manifests(1, 4).await.unwrap();
        let mut it = manifests.into_iter();
        for v in 1..4 {
            let (version, bytes) = it.next().unwrap();
            assert_eq!(v, version);
            assert_eq!(format!("hello, {v}").as_bytes(), bytes);
        }
        assert!(it.next().is_none());

        let manifests = log_store.fetch_manifests(0, 11).await.unwrap();
        let mut it = manifests.into_iter();
        for v in 0..5 {
            let (version, bytes) = it.next().unwrap();
            assert_eq!(v, version);
            assert_eq!(format!("hello, {v}").as_bytes(), bytes);
        }
        assert!(it.next().is_none());

        // test checkpoint
        assert!(log_store.load_last_checkpoint().await.unwrap().is_none());
        log_store
            .save_checkpoint(3, "checkpoint".as_bytes())
            .await
            .unwrap();

        let (v, checkpoint) = log_store.load_last_checkpoint().await.unwrap().unwrap();
        assert_eq!(checkpoint, "checkpoint".as_bytes());
        assert_eq!(3, v);

        //delete (,4) logs and keep checkpoint 3.
        let _ = log_store.delete_until(4, true).await.unwrap();
        let _ = log_store
            .checkpoint_storage
            .load_checkpoint(new_checkpoint_metadata_with_version(3))
            .await
            .unwrap()
            .unwrap();
        let _ = log_store.load_last_checkpoint().await.unwrap().unwrap();
        let manifests = log_store.fetch_manifests(0, 11).await.unwrap();
        let mut it = manifests.into_iter();

        let (version, bytes) = it.next().unwrap();
        assert_eq!(4, version);
        assert_eq!("hello, 4".as_bytes(), bytes);
        assert!(it.next().is_none());

        // delete all logs and checkpoints
        let _ = log_store.delete_until(11, false).await.unwrap();
        assert!(
            log_store
                .checkpoint_storage
                .load_checkpoint(new_checkpoint_metadata_with_version(3))
                .await
                .unwrap()
                .is_none()
        );
        assert!(log_store.load_last_checkpoint().await.unwrap().is_none());
        let manifests = log_store.fetch_manifests(0, 11).await.unwrap();
        let mut it = manifests.into_iter();

        assert!(it.next().is_none());
    }

    #[tokio::test]
    // test ManifestObjectStore can read/delete previously uncompressed data correctly
    async fn test_compress_backward_compatible() {
        let mut log_store = new_test_manifest_store();

        // write uncompress data to stimulate previously uncompressed data
        log_store.set_compress_type(CompressionType::Uncompressed);
        for v in 0..5 {
            log_store
                .save(v, format!("hello, {v}").as_bytes(), false)
                .await
                .unwrap();
        }
        log_store
            .save_checkpoint(5, "checkpoint_uncompressed".as_bytes())
            .await
            .unwrap();

        // change compress type
        log_store.set_compress_type(CompressionType::Gzip);

        // test load_last_checkpoint work correctly for previously uncompressed data
        let (v, checkpoint) = log_store.load_last_checkpoint().await.unwrap().unwrap();
        assert_eq!(v, 5);
        assert_eq!(checkpoint, "checkpoint_uncompressed".as_bytes());

        // write compressed data to stimulate compress algorithm take effect
        for v in 5..10 {
            log_store
                .save(v, format!("hello, {v}").as_bytes(), false)
                .await
                .unwrap();
        }
        log_store
            .save_checkpoint(10, "checkpoint_compressed".as_bytes())
            .await
            .unwrap();

        // test data reading
        let manifests = log_store.fetch_manifests(0, 10).await.unwrap();
        let mut it = manifests.into_iter();

        for v in 0..10 {
            let (version, bytes) = it.next().unwrap();
            assert_eq!(v, version);
            assert_eq!(format!("hello, {v}").as_bytes(), bytes);
        }
        let (v, checkpoint) = log_store
            .checkpoint_storage
            .load_checkpoint(new_checkpoint_metadata_with_version(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v, 5);
        assert_eq!(checkpoint, "checkpoint_uncompressed".as_bytes());
        let (v, checkpoint) = log_store.load_last_checkpoint().await.unwrap().unwrap();
        assert_eq!(v, 10);
        assert_eq!(checkpoint, "checkpoint_compressed".as_bytes());

        // Delete util 10, contain uncompressed/compressed data
        // log 0, 1, 2, 7, 8, 9 will be delete
        assert_eq!(11, log_store.delete_until(10, false).await.unwrap());
        let manifests = log_store.fetch_manifests(0, 10).await.unwrap();
        let mut it = manifests.into_iter();
        assert!(it.next().is_none());
    }

    #[tokio::test]
    async fn test_file_version() {
        let version = file_version("00000000000000000007.checkpoint");
        assert_eq!(version, 7);

        let name = delta_file(version);
        assert_eq!(name, "00000000000000000007.json");

        let name = checkpoint_file(version);
        assert_eq!(name, "00000000000000000007.checkpoint");
    }

    #[tokio::test]
    async fn test_uncompressed_manifest_files_size() {
        let mut log_store = new_test_manifest_store();
        // write 5 manifest files with uncompressed（8B per file）
        log_store.set_compress_type(CompressionType::Uncompressed);
        for v in 0..5 {
            log_store
                .save(v, format!("hello, {v}").as_bytes(), false)
                .await
                .unwrap();
        }
        // write 1 checkpoint file with uncompressed（23B）
        log_store
            .save_checkpoint(5, "checkpoint_uncompressed".as_bytes())
            .await
            .unwrap();

        // manifest files size
        assert_eq!(log_store.total_manifest_size(), 63);

        // delete 3 manifest files
        assert_eq!(log_store.delete_until(3, false).await.unwrap(), 3);

        // Physical deletion does not define the logical recovery set.
        assert_eq!(log_store.total_manifest_size(), 63);
        log_store.retain_manifest_size_after(5);
        assert_eq!(log_store.total_manifest_size(), 23);

        // delete all manifest files
        assert_eq!(
            log_store
                .delete_until(ManifestVersion::MAX, false)
                .await
                .unwrap(),
            3
        );

        assert_eq!(log_store.total_manifest_size(), 23);
    }

    #[tokio::test]
    async fn test_compressed_manifest_files_size() {
        let mut log_store = new_test_manifest_store();
        // Test with compressed manifest files
        log_store.set_compress_type(CompressionType::Gzip);
        // write 5 manifest files
        for v in 0..5 {
            log_store
                .save(v, format!("hello, {v}").as_bytes(), false)
                .await
                .unwrap();
        }
        log_store
            .save_checkpoint(5, "checkpoint_compressed".as_bytes())
            .await
            .unwrap();

        // manifest files size
        assert_eq!(log_store.total_manifest_size(), 181);

        // delete 3 manifest files
        assert_eq!(log_store.delete_until(3, false).await.unwrap(), 3);

        // Physical deletion does not define the logical recovery set.
        assert_eq!(log_store.total_manifest_size(), 181);
        log_store.retain_manifest_size_after(5);
        assert_eq!(log_store.total_manifest_size(), 41);

        // delete all manifest files
        assert_eq!(
            log_store
                .delete_until(ManifestVersion::MAX, false)
                .await
                .unwrap(),
            3
        );

        assert_eq!(log_store.total_manifest_size(), 41);
    }

    #[tokio::test]
    async fn test_scan_with_start_after_uncompress() {
        let mut log_store = new_test_manifest_store();
        log_store.set_compress_type(CompressionType::Uncompressed);
        test_scan_with_start_after_case(log_store).await;
    }

    #[tokio::test]
    async fn test_scan_with_start_after_compress() {
        let mut log_store = new_test_manifest_store();
        log_store.set_compress_type(CompressionType::Gzip);
        test_scan_with_start_after_case(log_store).await;
    }

    // OpenDAL resolves `start_after` against the operator
    // root, so the bound must embed the manifest directory prefix. Running the
    // same assertions against a non-root path exercises that composition.
    #[tokio::test]
    async fn test_scan_with_start_after_nested_path() {
        let mut log_store = new_test_manifest_store_at("/nested/region-1/");
        log_store.set_compress_type(CompressionType::Uncompressed);
        test_scan_with_start_after_case(log_store).await;
    }

    async fn test_scan_with_start_after_case(mut log_store: ManifestObjectStore) {
        for v in 0..10 {
            log_store
                .save(v, format!("hello, {v}").as_bytes(), false)
                .await
                .unwrap();
        }
        // A checkpoint at version 5 shares the directory; scan must still
        // return only delta files in range.
        log_store
            .save_checkpoint(5, "checkpoint".as_bytes())
            .await
            .unwrap();

        // start > 0: `start_after` must skip pre-start deltas without losing any.
        let entries = log_store.delta_storage.scan(3, 10).await.unwrap();
        let versions: Vec<_> = entries.iter().map(|(v, _)| *v).collect();
        assert_eq!(versions, vec![3, 4, 5, 6, 7, 8, 9]);

        // start == 0: `start_after` is skipped; every delta is returned.
        let entries = log_store.delta_storage.scan(0, 10).await.unwrap();
        let versions: Vec<_> = entries.iter().map(|(v, _)| *v).collect();
        assert_eq!(versions, (0..10).collect::<Vec<_>>());

        // Upper bound exclusive.
        let entries = log_store.delta_storage.scan(7, 9).await.unwrap();
        let versions: Vec<_> = entries.iter().map(|(v, _)| *v).collect();
        assert_eq!(versions, vec![7, 8]);

        // Start beyond any existing file returns empty.
        let entries = log_store
            .delta_storage
            .scan(10, ManifestVersion::MAX)
            .await
            .unwrap();
        assert!(entries.is_empty());
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CacheSource {
        WriteThrough,
        RemoteFill,
        LegacyDecoded,
        Truncated,
    }

    #[derive(Debug, Clone, Copy)]
    enum PayloadKind {
        Delta,
        Checkpoint { checksum: bool },
    }

    // Exercise both representations left by older versions, as well as new writes
    // and remote fills. Truncation only applies to compression envelopes.
    #[rstest::rstest]
    #[case::write_through_uncompressed(CacheSource::WriteThrough, CompressionType::Uncompressed)]
    #[case::write_through_gzip(CacheSource::WriteThrough, CompressionType::Gzip)]
    #[case::write_through_zstd(CacheSource::WriteThrough, CompressionType::Zstd)]
    #[case::write_through_xz(CacheSource::WriteThrough, CompressionType::Xz)]
    #[case::write_through_bzip2(CacheSource::WriteThrough, CompressionType::Bzip2)]
    #[case::remote_fill_uncompressed(CacheSource::RemoteFill, CompressionType::Uncompressed)]
    #[case::remote_fill_gzip(CacheSource::RemoteFill, CompressionType::Gzip)]
    #[case::remote_fill_zstd(CacheSource::RemoteFill, CompressionType::Zstd)]
    #[case::remote_fill_xz(CacheSource::RemoteFill, CompressionType::Xz)]
    #[case::remote_fill_bzip2(CacheSource::RemoteFill, CompressionType::Bzip2)]
    #[case::legacy_decoded_uncompressed(CacheSource::LegacyDecoded, CompressionType::Uncompressed)]
    #[case::legacy_decoded_gzip(CacheSource::LegacyDecoded, CompressionType::Gzip)]
    #[case::legacy_decoded_zstd(CacheSource::LegacyDecoded, CompressionType::Zstd)]
    #[case::legacy_decoded_xz(CacheSource::LegacyDecoded, CompressionType::Xz)]
    #[case::legacy_decoded_bzip2(CacheSource::LegacyDecoded, CompressionType::Bzip2)]
    #[case::truncated_gzip(CacheSource::Truncated, CompressionType::Gzip)]
    #[case::truncated_zstd(CacheSource::Truncated, CompressionType::Zstd)]
    #[case::truncated_xz(CacheSource::Truncated, CompressionType::Xz)]
    #[case::truncated_bzip2(CacheSource::Truncated, CompressionType::Bzip2)]
    #[tokio::test]
    async fn test_manifest_cache_payload(
        #[case] source: CacheSource,
        #[case] compression: CompressionType,
        #[values(PayloadKind::Delta, PayloadKind::Checkpoint { checksum: false }, PayloadKind::Checkpoint { checksum: true })]
        payload: PayloadKind,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use common_base::readable_size::ReadableSize;
        use object_store::layers::mock::MockLayerBuilder;

        let remote_dir = create_temp_dir("manifest-cache-remote");
        let cache_dir = create_temp_dir("manifest-cache-local");
        let remote =
            ObjectStore::new(Fs::default().root(remote_dir.path().to_str().unwrap())).unwrap();
        let local =
            ObjectStore::new(Fs::default().root(cache_dir.path().to_str().unwrap())).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let remote = remote.layer(
            MockLayerBuilder::default()
                .reader_factory(Arc::new(move |_, _, reader| {
                    observed.fetch_add(1, Ordering::Relaxed);
                    reader
                }))
                .build()
                .unwrap(),
        );
        let mut cache = ManifestCache::new(local.clone(), ReadableSize::mb(32), None, true).await;
        let mut store = ManifestObjectStore::new(
            "region/manifest/",
            remote.clone(),
            compression,
            Default::default(),
            (source != CacheSource::RemoteFill).then(|| cache.clone()),
        );
        let data = br#"{"manifest":"persistent cache payload"}"#;
        let key = gen_path(
            "region/manifest/",
            &if matches!(payload, PayloadKind::Checkpoint { .. }) {
                checkpoint_file(3)
            } else {
                delta_file(3)
            },
            compression,
        );
        if matches!(payload, PayloadKind::Checkpoint { .. }) {
            store.save_checkpoint(3, data).await.unwrap();
        } else {
            store.save(3, data, false).await.unwrap();
        }
        let encoded = remote.read(&key).await.unwrap().to_vec();
        if source == CacheSource::LegacyDecoded {
            cache.put_file(key.clone(), data.to_vec()).await;
        } else if source == CacheSource::Truncated {
            cache
                .put_file(key.clone(), encoded[..encoded.len() / 2].to_vec())
                .await;
        }
        drop(store);
        for iteration in 0..2 {
            drop(cache);
            cache = ManifestCache::new(local.clone(), ReadableSize::mb(32), None, true).await;
            if source != CacheSource::RemoteFill || iteration > 0 {
                assert!(
                    cache.get(&key).await.is_some(),
                    "cache recovery missed {key}"
                );
            }
            store = ManifestObjectStore::new(
                "region/manifest/",
                remote.clone(),
                compression,
                Default::default(),
                Some(cache.clone()),
            );
            let before = reads.load(Ordering::Relaxed);
            let actual = if let PayloadKind::Checkpoint { checksum } = payload {
                let metadata = CheckpointMetadata {
                    size: data.len(),
                    version: 3,
                    checksum: checksum.then(|| checkpoint_checksum(data)),
                    extend_metadata: Default::default(),
                };
                store
                    .checkpoint_storage
                    .load_checkpoint(metadata)
                    .await
                    .unwrap()
                    .unwrap()
            } else {
                let mut deltas = store.fetch_manifests(3, 4).await.unwrap();
                assert_eq!(1, deltas.len());
                deltas.remove(0)
            };
            assert_eq!((3, data.to_vec()), actual);
            // A successful roundtrip alone could hide a decoded refill that causes
            // repeated cache misses. Check both the bytes and remote read count.
            assert_eq!(encoded, cache.get_file(&key).await.unwrap());
            let must_refill = iteration == 0
                && (source == CacheSource::RemoteFill
                    || (compression != CompressionType::Uncompressed
                        && (source == CacheSource::LegacyDecoded
                            || source == CacheSource::Truncated)));
            assert_eq!(
                usize::from(must_refill),
                reads.load(Ordering::Relaxed) - before
            );
            drop(store);
        }
    }

    #[rstest::rstest]
    #[case::checksum(false)]
    #[case::decompression(true)]
    #[tokio::test]
    async fn test_manifest_cache_corrupted_remote(#[case] invalid_encoding: bool) {
        use crate::error::Error;
        use common_base::readable_size::ReadableSize;

        let remote_dir = create_temp_dir("manifest-cache-remote");
        let cache_dir = create_temp_dir("manifest-cache-local");
        let remote =
            ObjectStore::new(Fs::default().root(remote_dir.path().to_str().unwrap())).unwrap();
        let local =
            ObjectStore::new(Fs::default().root(cache_dir.path().to_str().unwrap())).unwrap();
        let cache = ManifestCache::new(local, ReadableSize::mb(32), None, true).await;
        let mut store = ManifestObjectStore::new(
            "region/",
            remote.clone(),
            CompressionType::Gzip,
            Default::default(),
            Some(cache.clone()),
        );
        store.save_checkpoint(3, b"checkpoint").await.unwrap();
        let key = gen_path("region/", &checkpoint_file(3), CompressionType::Gzip);
        let corrupted = if invalid_encoding {
            b"invalid gzip".to_vec()
        } else {
            CompressionType::Gzip
                .encode(b"other".as_slice())
                .await
                .unwrap()
        };
        remote.write(&key, corrupted).await.unwrap();
        // A legacy decoded cache entry must not hide the remote error on refill.
        cache.put_file(key.clone(), b"checkpoint".to_vec()).await;
        let error = store.load_last_checkpoint().await.unwrap_err();
        if invalid_encoding {
            assert!(matches!(error, Error::DecompressObject { .. }), "{error}");
        } else {
            assert!(matches!(error, Error::ChecksumMismatch { .. }), "{error}");
        }
        assert!(cache.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_manifest_cache_uncompressed_fallback() {
        use common_base::readable_size::ReadableSize;

        let remote_dir = create_temp_dir("manifest-cache-remote");
        let cache_dir = create_temp_dir("manifest-cache-local");
        let remote =
            ObjectStore::new(Fs::default().root(remote_dir.path().to_str().unwrap())).unwrap();
        let local =
            ObjectStore::new(Fs::default().root(cache_dir.path().to_str().unwrap())).unwrap();
        let cache = ManifestCache::new(local.clone(), ReadableSize::mb(32), None, true).await;
        let store = ManifestObjectStore::new(
            "region/",
            remote.clone(),
            CompressionType::Uncompressed,
            Default::default(),
            Some(cache.clone()),
        );
        store.save_checkpoint(3, b"checkpoint").await.unwrap();
        drop(store);
        drop(cache);
        let cache = ManifestCache::new(local, ReadableSize::mb(32), None, true).await;
        assert!(
            cache
                .get("region/00000000000000000003.checkpoint")
                .await
                .is_some()
        );
        // Keep _last_checkpoint, but remove its payload to prove the fallback hit.
        remote
            .delete("region/00000000000000000003.checkpoint")
            .await
            .unwrap();
        let mut store = ManifestObjectStore::new(
            "region/",
            remote,
            CompressionType::Gzip,
            Default::default(),
            Some(cache),
        );
        assert_eq!(
            Some((3, b"checkpoint".to_vec())),
            store.load_last_checkpoint().await.unwrap()
        );
    }
}
