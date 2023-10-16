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

use common_datasource::compression::CompressionType;
use common_telemetry::debug;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use object_store::{util, Entry, ErrorKind, ObjectStore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::manifest::ManifestVersion;

use crate::error::{
    CompressObjectSnafu, DecompressObjectSnafu, InvalidScanIndexSnafu, OpenDalSnafu, Result,
    SerdeJsonSnafu, Utf8Snafu,
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

#[inline]
pub const fn manifest_compress_type(compress: bool) -> CompressionType {
    if compress {
        DEFAULT_MANIFEST_COMPRESSION_TYPE
    } else {
        FALL_BACK_COMPRESS_TYPE
    }
}

#[inline]
pub fn delta_file(version: ManifestVersion) -> String {
    format!("{version:020}.json")
}

#[inline]
pub fn checkpoint_file(version: ManifestVersion) -> String {
    format!("{version:020}.checkpoint")
}

#[inline]
pub fn gen_path(path: &str, file: &str, compress_type: CompressionType) -> String {
    if compress_type == CompressionType::Uncompressed {
        format!("{}{}", path, file)
    } else {
        format!("{}{}.{}", path, file, compress_type.file_extension())
    }
}

/// Return's the file manifest version from path
///
/// # Panics
/// Panics if the file path is not a valid delta or checkpoint file.
#[inline]
pub fn file_version(path: &str) -> ManifestVersion {
    let s = path.split('.').next().unwrap();
    s.parse().unwrap_or_else(|_| panic!("Invalid file: {path}"))
}

/// Return's the file name from path
/// Just use for .json and .checkpoint file
/// ### Panics
/// Panics if the file path is not a valid delta or checkpoint file.
#[inline]
pub fn file_name(path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or("").to_string();
    if !is_checkpoint_file(&name) && !is_delta_file(&name) {
        panic!("Invalid file: {path}")
    }
    name
}

/// Return's the file compress algorithm by file extension.
///
/// for example file
/// `00000000000000000000.json.gz` -> `CompressionType::GZIP`
#[inline]
pub fn file_compress_type(path: &str) -> CompressionType {
    let s = path.rsplit('.').next().unwrap_or("");
    CompressionType::from_str(s).unwrap_or(CompressionType::Uncompressed)
}

#[inline]
pub fn is_delta_file(file_name: &str) -> bool {
    DELTA_RE.is_match(file_name)
}

#[inline]
pub fn is_checkpoint_file(file_name: &str) -> bool {
    CHECKPOINT_RE.is_match(file_name)
}

pub struct ObjectStoreLogIterator {
    object_store: ObjectStore,
    iter: Box<dyn Iterator<Item = (ManifestVersion, Entry)> + Send + Sync>,
}

impl ObjectStoreLogIterator {
    pub async fn next_log(&mut self) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        match self.iter.next() {
            Some((v, entry)) => {
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
                Ok(Some((v, data)))
            }
            None => Ok(None),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ManifestObjectStore {
    object_store: ObjectStore,
    compress_type: CompressionType,
    path: String,
    /// Stores the size of each manifest file.
    /// K is the manifest file name(such as "0000000000000000006.checkpoint"
    /// or "0000000000000000006.json"), and V is the file size.
    manifest_size_map: HashMap<String, u64>,
}

impl ManifestObjectStore {
    pub fn new(path: &str, object_store: ObjectStore, compress_type: CompressionType) -> Self {
        Self {
            object_store,
            compress_type,
            path: util::normalize_dir(path),
            manifest_size_map: HashMap::new(),
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

    /// Return all `R`s in the root directory that meet the `filter` conditions (that is, the `filter` closure returns `Some(R)`),
    /// and discard `R` that does not meet the conditions (that is, the `filter` closure returns `None`)
    /// Return an empty vector when directory is not found.
    pub async fn get_paths<F, R>(&self, filter: F) -> Result<Vec<R>>
    where
        F: Fn(Entry) -> Option<R>,
    {
        let streamer = match self.object_store.lister_with(&self.path).await {
            Ok(streamer) => streamer,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                debug!("Manifest directory does not exists: {}", self.path);
                return Ok(vec![]);
            }
            Err(e) => Err(e).context(OpenDalSnafu)?,
        };

        streamer
            .try_filter_map(|e| async { Ok(filter(e)) })
            .try_collect::<Vec<_>>()
            .await
            .context(OpenDalSnafu)
    }

    /// Scan the manifest files in the range of [start, end) and return the iterator.
    pub async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<ObjectStoreLogIterator> {
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

        entries.sort_unstable_by(|(v1, _), (v2, _)| v1.cmp(v2));

        Ok(ObjectStoreLogIterator {
            object_store: self.object_store.clone(),
            iter: Box::new(entries.into_iter()),
        })
    }

    /// Delete manifest files that version < end.
    /// If keep_last_checkpoint is true, the last checkpoint file will be kept.
    /// ### Return
    /// The number of deleted files.
    pub async fn delete_until(
        &mut self,
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
        let paths: Vec<_> = entries
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
            .map(|e| e.0.path().to_string())
            .collect();
        let ret = paths.len();

        debug!(
            "Deleting {} logs from manifest storage path {} until {}, checkpoint_version: {:?}, paths: {:?}",
            ret,
            self.path,
            end,
            checkpoint_version,
            paths,
        );

        // delete the manifest'size in paths
        paths.iter().for_each(|path| {
            let name = file_name(path);
            self.manifest_size_map.remove(&name);
        });

        self.object_store
            .remove(paths)
            .await
            .context(OpenDalSnafu)?;

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
        self.set_manifest_size_by_path(&path, data.len() as u64);
        self.object_store
            .write(&path, data)
            .await
            .context(OpenDalSnafu)
    }

    /// Save the checkpoint manifest file.
    pub async fn save_checkpoint(&mut self, version: ManifestVersion, bytes: &[u8]) -> Result<()> {
        let path = self.checkpoint_file_path(version);
        let data = self
            .compress_type
            .encode(bytes)
            .await
            .context(CompressObjectSnafu {
                compress_type: self.compress_type,
                path: &path,
            })?;
        self.set_manifest_size_by_path(&path, data.len() as u64);
        self.object_store
            .write(&path, data)
            .await
            .context(OpenDalSnafu)?;

        // Because last checkpoint file only contain size and version, which is tiny, so we don't compress it.
        let last_checkpoint_path = self.last_checkpoint_path();

        let checkpoint_metadata = CheckpointMetadata {
            size: bytes.len(),
            version,
            checksum: None,
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

    pub async fn load_checkpoint(
        &mut self,
        version: ManifestVersion,
    ) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        let path = self.checkpoint_file_path(version);
        // Due to backward compatibility, it is possible that the user's checkpoint not compressed,
        // so if we don't find file by compressed type. fall back to checkpoint not compressed find again.
        let checkpoint_data = match self.object_store.read(&path).await {
            Ok(checkpoint) => {
                let decompress_data =
                    self.compress_type
                        .decode(checkpoint)
                        .await
                        .context(DecompressObjectSnafu {
                            compress_type: self.compress_type,
                            path: path.clone(),
                        })?;
                self.set_manifest_size_by_path(&path, decompress_data.len() as u64);
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
                                let decompress_data = FALL_BACK_COMPRESS_TYPE
                                    .decode(checkpoint)
                                    .await
                                    .context(DecompressObjectSnafu {
                                        compress_type: FALL_BACK_COMPRESS_TYPE,
                                        path: path.clone(),
                                    })?;
                                self.set_manifest_size_by_path(&path, decompress_data.len() as u64);
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

        let checkpoint_metadata = CheckpointMetadata::decode(&last_checkpoint_data)?;

        debug!(
            "Load checkpoint in path: {}, metadata: {:?}",
            last_checkpoint_path, checkpoint_metadata
        );

        self.load_checkpoint(checkpoint_metadata.version).await
    }

    #[cfg(test)]
    pub async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        self.object_store.read(path).await.context(OpenDalSnafu)
    }

    /// Get the size(Byte) of the delta file by delta version.
    /// If the delta file does not exist, return None.
    pub fn delta_file_size(&self, version: ManifestVersion) -> Option<u64> {
        self.manifest_size_map.get(&delta_file(version)).copied()
    }

    /// Get the size(Byte) of the checkpoint file by checkpoint version.
    /// If the checkpoint file does not exist, return None.
    pub fn checkpoint_file_size(&self, version: ManifestVersion) -> Option<u64> {
        self.manifest_size_map
            .get(&checkpoint_file(version))
            .copied()
    }

    /// Compute the size(Byte) in manifest size map.
    pub fn get_total_manifest_size(&self) -> u64 {
        self.manifest_size_map.values().sum()
    }

    /// Count the total size(Byte) of exist manifest files which satisfy:
    /// delta file version <= end and checkpoint file version < end.
    /// Notice: this function will read files from object store.
    pub async fn set_manifest_size_until(&mut self, end: ManifestVersion) -> Result<()> {
        let entries = self
            .get_paths(|entry| {
                let file_name = entry.name();
                if is_delta_file(file_name) || is_checkpoint_file(file_name) {
                    let file_version = file_version(file_name);
                    if file_version < end || (file_version == end && is_delta_file(file_name)) {
                        return Some(entry);
                    }
                }
                None
            })
            .await?;
        for entry in entries {
            let bytes = self
                .object_store
                .read(entry.path())
                .await
                .context(OpenDalSnafu)?;
            self.set_manifest_size_by_path(entry.path(), bytes.len() as u64);
        }

        Ok(())
    }

    /// Set the size of the manifest file by path.
    pub fn set_manifest_size_by_path(&mut self, path: &str, size: u64) {
        self.manifest_size_map.insert(file_name(path), size);
    }

    /// Set the size of the delta file by delta version.
    pub fn set_delta_file_size(&mut self, version: ManifestVersion, size: u64) {
        self.manifest_size_map.insert(delta_file(version), size);
    }

    /// Set the size of the checkpoint file by checkpoint version.
    pub fn set_checkpoint_file_size(&mut self, version: ManifestVersion, size: u64) {
        self.manifest_size_map
            .insert(checkpoint_file(version), size);
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct CheckpointMetadata {
    pub size: usize,
    /// The latest version this checkpoint contains.
    pub version: ManifestVersion,
    pub checksum: Option<String>,
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
        let mut builder = Fs::default();
        let _ = builder.root(&tmp_dir.path().to_string_lossy());
        let object_store = ObjectStore::new(builder).unwrap().finish();
        ManifestObjectStore::new("/", object_store, CompressionType::Uncompressed)
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

        let mut it = log_store.scan(1, 4).await.unwrap();
        for v in 1..4 {
            let (version, bytes) = it.next_log().await.unwrap().unwrap();
            assert_eq!(v, version);
            assert_eq!(format!("hello, {v}").as_bytes(), bytes);
        }
        assert!(it.next_log().await.unwrap().is_none());

        let mut it = log_store.scan(0, 11).await.unwrap();
        for v in 0..5 {
            let (version, bytes) = it.next_log().await.unwrap().unwrap();
            assert_eq!(v, version);
            assert_eq!(format!("hello, {v}").as_bytes(), bytes);
        }
        assert!(it.next_log().await.unwrap().is_none());

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
        let _ = log_store.load_checkpoint(3).await.unwrap().unwrap();
        let _ = log_store.load_last_checkpoint().await.unwrap().unwrap();
        let mut it = log_store.scan(0, 11).await.unwrap();
        let (version, bytes) = it.next_log().await.unwrap().unwrap();
        assert_eq!(4, version);
        assert_eq!("hello, 4".as_bytes(), bytes);
        assert!(it.next_log().await.unwrap().is_none());

        // delete all logs and checkpoints
        let _ = log_store.delete_until(11, false).await.unwrap();
        assert!(log_store.load_checkpoint(3).await.unwrap().is_none());
        assert!(log_store.load_last_checkpoint().await.unwrap().is_none());
        let mut it = log_store.scan(0, 11).await.unwrap();
        assert!(it.next_log().await.unwrap().is_none());
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

        // write compressed data to stimulate compress alogorithom take effect
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
        let mut it = log_store.scan(0, 10).await.unwrap();
        for v in 0..10 {
            let (version, bytes) = it.next_log().await.unwrap().unwrap();
            assert_eq!(v, version);
            assert_eq!(format!("hello, {v}").as_bytes(), bytes);
        }
        let (v, checkpoint) = log_store.load_checkpoint(5).await.unwrap().unwrap();
        assert_eq!(v, 5);
        assert_eq!(checkpoint, "checkpoint_uncompressed".as_bytes());
        let (v, checkpoint) = log_store.load_last_checkpoint().await.unwrap().unwrap();
        assert_eq!(v, 10);
        assert_eq!(checkpoint, "checkpoint_compressed".as_bytes());

        // Delete util 10, contain uncompressed/compressed data
        // log 0, 1, 2, 7, 8, 9 will be delete
        assert_eq!(11, log_store.delete_until(10, false).await.unwrap());
        let mut it = log_store.scan(0, 10).await.unwrap();
        assert!(it.next_log().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_file_name() {
        let name = file_name(
            "data/greptime/public/1054/1054_0000000000/manifest/00000000000000000007.json",
        );
        assert_eq!(name, "00000000000000000007.json");

        let name = file_name(
            "/data/greptime/public/1054/1054_0000000000/manifest/00000000000000000007.checkpoint",
        );
        assert_eq!(name, "00000000000000000007.checkpoint");

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
        // write 5 manifest files
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
        // single delta file size
        assert_eq!(log_store.delta_file_size(0), Some(8));

        // single checkpoint file size
        assert_eq!(log_store.checkpoint_file_size(5), Some(23));

        // manifest files size
        assert_eq!(log_store.get_total_manifest_size(), 63);

        // delete 3 manifest files
        assert_eq!(log_store.delete_until(3, false).await.unwrap(), 3);

        // manifest files size after delete
        assert_eq!(log_store.get_total_manifest_size(), 39);

        // delete all manifest files
        assert_eq!(
            log_store
                .delete_until(ManifestVersion::MAX, false)
                .await
                .unwrap(),
            3
        );

        assert_eq!(log_store.get_total_manifest_size(), 0);
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
        assert_eq!(log_store.get_total_manifest_size(), 181);

        // delete 3 manifest files
        assert_eq!(log_store.delete_until(3, false).await.unwrap(), 3);

        // manifest files size after delete
        assert_eq!(log_store.get_total_manifest_size(), 97);

        // delete all manifest files
        assert_eq!(
            log_store
                .delete_until(ManifestVersion::MAX, false)
                .await
                .unwrap(),
            3
        );

        assert_eq!(log_store.get_total_manifest_size(), 0);
    }
}
