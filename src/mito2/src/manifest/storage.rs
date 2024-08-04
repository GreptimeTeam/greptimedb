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

use std::collections::HashMap;
use std::iter::Iterator;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use common_datasource::compression::CompressionType;
use common_telemetry::debug;
use crc32fast::Hasher;
use futures::future::try_join_all;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use object_store::{util, Entry, ErrorKind, Lister, ObjectStore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::manifest::ManifestVersion;
use tokio::sync::Semaphore;

use crate::error::{
    ChecksumMismatchSnafu, CompressObjectSnafu, DecompressObjectSnafu, InvalidScanIndexSnafu,
    OpenDalSnafu, Result, SerdeJsonSnafu, Utf8Snafu,
};

lazy_static! {
    static ref DELTA_RE: Regex = Regex::new("^\\d+\\.json").unwrap();
    static ref CHECKPOINT_RE: Regex = Regex::new("^\\d+\\.checkpoint").unwrap();
}

const LAST_CHECKPOINT_FILE: &str = "_last_checkpoint";
const DEFAULT_MANIFEST_COMPRESSION_TYPE: CompressionType = CompressionType::Gzip;
/// Due to backward compatibility, it is possible that the user's manifest file has not been compressed.
/// So when we encounter problems, we need to fall back to `FALL_BACK_COMPRESS_TYPE` for processing.
const FALL_BACK_COMPRESS_TYPE: CompressionType = CompressionType::Uncompressed;
const FETCH_MANIFEST_PARALLELISM: usize = 16;

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

fn checkpoint_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn verify_checksum(data: &[u8], wanted: Option<u32>) -> Result<()> {
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

/// Key to identify a manifest file.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
enum FileKey {
    /// A delta file (`.json`).
    Delta(ManifestVersion),
    /// A checkpoint file (`.checkpoint`).
    Checkpoint(ManifestVersion),
}

#[derive(Clone, Debug)]
pub struct ManifestObjectStore {
    object_store: ObjectStore,
    compress_type: CompressionType,
    path: String,
    /// Stores the size of each manifest file.
    manifest_size_map: Arc<RwLock<HashMap<FileKey, u64>>>,
    total_manifest_size: Arc<AtomicU64>,
}

impl ManifestObjectStore {
    pub fn new(
        path: &str,
        object_store: ObjectStore,
        compress_type: CompressionType,
        total_manifest_size: Arc<AtomicU64>,
    ) -> Self {
        Self {
            object_store,
            compress_type,
            path: util::normalize_dir(path),
            manifest_size_map: Arc::new(RwLock::new(HashMap::new())),
            total_manifest_size,
        }
    }

    /// Returns the delta file path under the **current** compression algorithm
    fn delta_file_path(&self, version: ManifestVersion) -> String {
        gen_path(&self.path, &delta_file(version), self.compress_type)
    }

    /// Returns the checkpoint file path under the **current** compression algorithm
    fn checkpoint_file_path(&self, version: ManifestVersion) -> String {
        gen_path(&self.path, &checkpoint_file(version), self.compress_type)
    }

    /// Returns the last checkpoint path, because the last checkpoint is not compressed,
    /// so its path name has nothing to do with the compression algorithm used by `ManifestObjectStore`
    pub(crate) fn last_checkpoint_path(&self) -> String {
        format!("{}{}", self.path, LAST_CHECKPOINT_FILE)
    }

    /// Returns the manifest dir
    pub(crate) fn manifest_dir(&self) -> &str {
        &self.path
    }

    /// Returns a iterator of manifests.
    pub(crate) async fn manifest_lister(&self) -> Result<Option<Lister>> {
        match self.object_store.lister_with(&self.path).await {
            Ok(streamer) => Ok(Some(streamer)),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                debug!("Manifest directory does not exists: {}", self.path);
                Ok(None)
            }
            Err(e) => Err(e).context(OpenDalSnafu)?,
        }
    }

    /// Return all `R`s in the root directory that meet the `filter` conditions (that is, the `filter` closure returns `Some(R)`),
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

    /// Sorts the manifest files.
    fn sort_manifests(entries: &mut [(ManifestVersion, Entry)]) {
        entries.sort_unstable_by(|(v1, _), (v2, _)| v1.cmp(v2));
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

        Self::sort_manifests(&mut entries);

        Ok(entries)
    }

    /// Fetch all manifests in concurrent.
    pub async fn fetch_manifests(
        &self,
        start_version: ManifestVersion,
        end_version: ManifestVersion,
    ) -> Result<Vec<(ManifestVersion, Vec<u8>)>> {
        let manifests = self.scan(start_version, end_version).await?;

        // TODO(weny): Make it configurable.
        let semaphore = Semaphore::new(FETCH_MANIFEST_PARALLELISM);

        let tasks = manifests.iter().map(|(v, entry)| async {
            // Safety: semaphore must exist.
            let _permit = semaphore.acquire().await.unwrap();

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
            Ok((*v, data))
        });

        try_join_all(tasks).await
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
                        if *is_checkpoint {
                            Some(version)
                        } else {
                            None
                        }
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
            ret,
            self.path,
            end,
            checkpoint_version,
            paths,
        );

        self.object_store
            .remove(paths)
            .await
            .context(OpenDalSnafu)?;

        // delete manifest sizes
        for (_, is_checkpoint, version) in &del_entries {
            if *is_checkpoint {
                self.unset_file_size(&FileKey::Checkpoint(*version));
            } else {
                self.unset_file_size(&FileKey::Delta(*version));
            }
        }

        Ok(ret)
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
        self.object_store
            .write(&path, data)
            .await
            .context(OpenDalSnafu)?;
        self.set_delta_file_size(version, delta_size as u64);
        Ok(())
    }

    /// Save the checkpoint manifest file.
    pub(crate) async fn save_checkpoint(
        &self,
        version: ManifestVersion,
        bytes: &[u8],
    ) -> Result<()> {
        let path = self.checkpoint_file_path(version);
        let data = self
            .compress_type
            .encode(bytes)
            .await
            .context(CompressObjectSnafu {
                compress_type: self.compress_type,
                path: &path,
            })?;
        let checkpoint_size = data.len();
        let checksum = checkpoint_checksum(bytes);
        self.object_store
            .write(&path, data)
            .await
            .context(OpenDalSnafu)?;
        self.set_checkpoint_file_size(version, checkpoint_size as u64);

        // Because last checkpoint file only contain size and version, which is tiny, so we don't compress it.
        let last_checkpoint_path = self.last_checkpoint_path();

        let checkpoint_metadata = CheckpointMetadata {
            size: bytes.len(),
            version,
            checksum: Some(checksum),
            extend_metadata: HashMap::new(),
        };

        debug!(
            "Save checkpoint in path: {},  metadata: {:?}",
            last_checkpoint_path, checkpoint_metadata
        );

        let bytes = checkpoint_metadata.encode()?;
        self.object_store
            .write(&last_checkpoint_path, bytes)
            .await
            .context(OpenDalSnafu)?;

        Ok(())
    }

    async fn load_checkpoint(
        &mut self,
        metadata: CheckpointMetadata,
    ) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        let version = metadata.version;
        let path = self.checkpoint_file_path(version);
        // Due to backward compatibility, it is possible that the user's checkpoint not compressed,
        // so if we don't find file by compressed type. fall back to checkpoint not compressed find again.
        let checkpoint_data =
            match self.object_store.read(&path).await {
                Ok(checkpoint) => {
                    let checkpoint_size = checkpoint.len();
                    let decompress_data = self.compress_type.decode(checkpoint).await.context(
                        DecompressObjectSnafu {
                            compress_type: self.compress_type,
                            path,
                        },
                    )?;
                    verify_checksum(&decompress_data, metadata.checksum)?;
                    // set the checkpoint size
                    self.set_checkpoint_file_size(version, checkpoint_size as u64);
                    Ok(Some(decompress_data))
                }
                Err(e) => {
                    if e.kind() == ErrorKind::NotFound {
                        if self.compress_type != FALL_BACK_COMPRESS_TYPE {
                            let fall_back_path = gen_path(
                                &self.path,
                                &checkpoint_file(version),
                                FALL_BACK_COMPRESS_TYPE,
                            );
                            debug!(
                                "Failed to load checkpoint from path: {}, fall back to path: {}",
                                path, fall_back_path
                            );
                            match self.object_store.read(&fall_back_path).await {
                                Ok(checkpoint) => {
                                    let checkpoint_size = checkpoint.len();
                                    let decompress_data = FALL_BACK_COMPRESS_TYPE
                                        .decode(checkpoint)
                                        .await
                                        .context(DecompressObjectSnafu {
                                            compress_type: FALL_BACK_COMPRESS_TYPE,
                                            path,
                                        })?;
                                    verify_checksum(&decompress_data, metadata.checksum)?;
                                    self.set_checkpoint_file_size(version, checkpoint_size as u64);
                                    Ok(Some(decompress_data))
                                }
                                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                                Err(e) => Err(e).context(OpenDalSnafu),
                            }
                        } else {
                            Ok(None)
                        }
                    } else {
                        Err(e).context(OpenDalSnafu)
                    }
                }
            }?;
        Ok(checkpoint_data.map(|data| (version, data)))
    }

    /// Load the latest checkpoint.
    /// Return manifest version and the raw [RegionCheckpoint](crate::manifest::action::RegionCheckpoint) content if any
    pub async fn load_last_checkpoint(&mut self) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        let last_checkpoint_path = self.last_checkpoint_path();
        let last_checkpoint_data = match self.object_store.read(&last_checkpoint_path).await {
            Ok(data) => data,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(e) => {
                return Err(e).context(OpenDalSnafu)?;
            }
        };

        let checkpoint_metadata = CheckpointMetadata::decode(&last_checkpoint_data.to_vec())?;

        debug!(
            "Load checkpoint in path: {}, metadata: {:?}",
            last_checkpoint_path, checkpoint_metadata
        );

        self.load_checkpoint(checkpoint_metadata).await
    }

    #[cfg(test)]
    pub async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        self.object_store
            .read(path)
            .await
            .context(OpenDalSnafu)
            .map(|v| v.to_vec())
    }

    #[cfg(test)]
    pub async fn write_last_checkpoint(
        &mut self,
        version: ManifestVersion,
        bytes: &[u8],
    ) -> Result<()> {
        let path = self.checkpoint_file_path(version);
        let data = self
            .compress_type
            .encode(bytes)
            .await
            .context(CompressObjectSnafu {
                compress_type: self.compress_type,
                path: &path,
            })?;

        let checkpoint_size = data.len();

        self.object_store
            .write(&path, data)
            .await
            .context(OpenDalSnafu)?;

        self.set_checkpoint_file_size(version, checkpoint_size as u64);

        let last_checkpoint_path = self.last_checkpoint_path();
        let checkpoint_metadata = CheckpointMetadata {
            size: bytes.len(),
            version,
            checksum: Some(1218259706),
            extend_metadata: HashMap::new(),
        };

        debug!(
            "Rewrite checkpoint in path: {},  metadata: {:?}",
            last_checkpoint_path, checkpoint_metadata
        );

        let bytes = checkpoint_metadata.encode()?;

        // Overwrite the last checkpoint with the modified content
        self.object_store
            .write(&last_checkpoint_path, bytes.clone())
            .await
            .context(OpenDalSnafu)?;
        Ok(())
    }

    /// Compute the size(Byte) in manifest size map.
    pub(crate) fn total_manifest_size(&self) -> u64 {
        self.manifest_size_map.read().unwrap().values().sum()
    }

    /// Set the size of the delta file by delta version.
    pub(crate) fn set_delta_file_size(&mut self, version: ManifestVersion, size: u64) {
        let mut m = self.manifest_size_map.write().unwrap();
        m.insert(FileKey::Delta(version), size);

        self.inc_total_manifest_size(size);
    }

    /// Set the size of the checkpoint file by checkpoint version.
    fn set_checkpoint_file_size(&self, version: ManifestVersion, size: u64) {
        let mut m = self.manifest_size_map.write().unwrap();
        m.insert(FileKey::Checkpoint(version), size);

        self.inc_total_manifest_size(size);
    }

    fn unset_file_size(&self, key: &FileKey) {
        let mut m = self.manifest_size_map.write().unwrap();
        if let Some(val) = m.remove(key) {
            self.dec_total_manifest_size(val);
        }
    }

    fn inc_total_manifest_size(&self, val: u64) {
        self.total_manifest_size.fetch_add(val, Ordering::Relaxed);
    }

    fn dec_total_manifest_size(&self, val: u64) {
        self.total_manifest_size.fetch_sub(val, Ordering::Relaxed);
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct CheckpointMetadata {
    pub size: usize,
    /// The latest version this checkpoint contains.
    pub version: ManifestVersion,
    pub checksum: Option<u32>,
    pub extend_metadata: HashMap<String, String>,
}

impl CheckpointMetadata {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_string(self)
            .context(SerdeJsonSnafu)?
            .into_bytes())
    }

    fn decode(bs: &[u8]) -> Result<Self> {
        let data = std::str::from_utf8(bs).context(Utf8Snafu)?;

        serde_json::from_str(data).context(SerdeJsonSnafu)
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;
    use object_store::ObjectStore;

    use super::*;

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
        log_store.compress_type = CompressionType::Uncompressed;
        test_manifest_log_store_case(log_store).await;
    }

    #[tokio::test]
    async fn test_manifest_log_store_compress() {
        let mut log_store = new_test_manifest_store();
        log_store.compress_type = CompressionType::Gzip;
        test_manifest_log_store_case(log_store).await;
    }

    async fn test_manifest_log_store_case(mut log_store: ManifestObjectStore) {
        for v in 0..5 {
            log_store
                .save(v, format!("hello, {v}").as_bytes())
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
        assert!(log_store
            .load_checkpoint(new_checkpoint_metadata_with_version(3))
            .await
            .unwrap()
            .is_none());
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
        log_store.compress_type = CompressionType::Uncompressed;
        for v in 0..5 {
            log_store
                .save(v, format!("hello, {v}").as_bytes())
                .await
                .unwrap();
        }
        log_store
            .save_checkpoint(5, "checkpoint_uncompressed".as_bytes())
            .await
            .unwrap();

        // change compress type
        log_store.compress_type = CompressionType::Gzip;

        // test load_last_checkpoint work correctly for previously uncompressed data
        let (v, checkpoint) = log_store.load_last_checkpoint().await.unwrap().unwrap();
        assert_eq!(v, 5);
        assert_eq!(checkpoint, "checkpoint_uncompressed".as_bytes());

        // write compressed data to stimulate compress algorithm take effect
        for v in 5..10 {
            log_store
                .save(v, format!("hello, {v}").as_bytes())
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
        log_store.compress_type = CompressionType::Uncompressed;
        for v in 0..5 {
            log_store
                .save(v, format!("hello, {v}").as_bytes())
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
        log_store.compress_type = CompressionType::Gzip;
        // write 5 manifest files
        for v in 0..5 {
            log_store
                .save(v, format!("hello, {v}").as_bytes())
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
