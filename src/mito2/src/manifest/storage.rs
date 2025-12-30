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
use common_telemetry::debug;
use crc32fast::Hasher;
use lazy_static::lazy_static;
use object_store::util::join_dir;
use object_store::{Lister, ObjectStore, util};
use regex::Regex;
use snafu::{ResultExt, ensure};
use store_api::ManifestVersion;
use store_api::storage::RegionId;

use crate::cache::manifest_cache::ManifestCache;
use crate::error::{ChecksumMismatchSnafu, OpenDalSnafu, Result};
use crate::manifest::storage::checkpoint::CheckpointStorage;
use crate::manifest::storage::delta::DeltaStorage;
use crate::manifest::storage::size_tracker::{CheckpointTracker, DeltaTracker, SizeTracker};
use crate::manifest::storage::staging::StagingStorage;
use crate::manifest::storage::utils::remove_from_cache;

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
}

impl ManifestObjectStore {
    pub fn new(
        path: &str,
        object_store: ObjectStore,
        compress_type: CompressionType,
        total_manifest_size: Arc<AtomicU64>,
        manifest_cache: Option<ManifestCache>,
    ) -> Self {
        common_telemetry::info!("Create manifest store, cache: {}", manifest_cache.is_some());

        let path = util::normalize_dir(path);
        let size_tracker = SizeTracker::new(total_manifest_size);
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
        }
    }

    /// Returns the manifest dir
    pub(crate) fn manifest_dir(&self) -> &str {
        &self.path
    }

    /// Returns an iterator of manifests from normal or staging directory.
    pub(crate) async fn manifest_lister(&self, is_staging: bool) -> Result<Option<Lister>> {
        if is_staging {
            self.staging_storage.manifest_lister().await
        } else {
            self.delta_storage.manifest_lister().await
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
    /// The number of deleted files.
    pub async fn delete_until(
        &self,
        end: ManifestVersion,
        keep_last_checkpoint: bool,
    ) -> Result<usize> {
        // Stores (entry, is_checkpoint, version) in a Vec.
        let entries: Vec<_> = self
            .delta_storage
            .get_paths(|entry| {
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
        let ret = paths.len();

        debug!(
            "Deleting {} logs from manifest storage path {} until {}, checkpoint_version: {:?}, paths: {:?}",
            ret, self.path, end, checkpoint_version, paths,
        );

        // Remove from cache first
        for (entry, _, _) in &del_entries {
            remove_from_cache(self.manifest_cache.as_ref(), entry.path()).await;
        }

        self.object_store
            .delete_iter(paths)
            .await
            .context(OpenDalSnafu)?;

        // delete manifest sizes
        for (_, is_checkpoint, version) in &del_entries {
            if *is_checkpoint {
                self.size_tracker
                    .remove(&size_tracker::FileKey::Checkpoint(*version));
            } else {
                self.size_tracker
                    .remove(&size_tracker::FileKey::Delta(*version));
            }
        }

        Ok(ret)
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
        } else {
            self.delta_storage.save(version, bytes).await
        }
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
    pub async fn load_last_checkpoint(&mut self) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        self.checkpoint_storage.load_last_checkpoint().await
    }

    /// Compute the size(Byte) in manifest size map.
    pub(crate) fn total_manifest_size(&self) -> u64 {
        self.size_tracker.total()
    }

    /// Resets the size of all files.
    pub(crate) fn reset_manifest_size(&mut self) {
        self.size_tracker.reset();
    }

    /// Set the size of the delta file by delta version.
    pub(crate) fn set_delta_file_size(&mut self, version: ManifestVersion, size: u64) {
        self.size_tracker.record_delta(version, size);
    }

    /// Set the size of the checkpoint file by checkpoint version.
    pub(crate) fn set_checkpoint_file_size(&self, version: ManifestVersion, size: u64) {
        self.size_tracker.record_checkpoint(version, size);
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
        common_telemetry::init_default_ut_logging();
        let tmp_dir = create_temp_dir("test_manifest_log_store");
        let builder = Fs::default().root(&tmp_dir.path().to_string_lossy());
        let object_store = ObjectStore::new(builder).unwrap().finish();
        ManifestObjectStore::new(
            "/",
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

        // manifest files size after delete
        assert_eq!(log_store.total_manifest_size(), 39);

        // delete all manifest files
        assert_eq!(
            log_store
                .delete_until(ManifestVersion::MAX, false)
                .await
                .unwrap(),
            3
        );

        assert_eq!(log_store.total_manifest_size(), 0);
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

        // manifest files size after delete
        assert_eq!(log_store.total_manifest_size(), 97);

        // delete all manifest files
        assert_eq!(
            log_store
                .delete_until(ManifestVersion::MAX, false)
                .await
                .unwrap(),
            3
        );

        assert_eq!(log_store.total_manifest_size(), 0);
    }
}
