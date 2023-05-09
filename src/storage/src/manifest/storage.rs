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

use async_trait::async_trait;
use common_datasource::compression::CompressionType;
use common_telemetry::logging;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use object_store::{raw_normalize_path, util, Entry, ErrorKind, ObjectStore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::manifest::{LogIterator, ManifestLogStorage, ManifestVersion};

use crate::error::{
    CompressObjectSnafu, DecodeJsonSnafu, DecompressObjectSnafu, DeleteObjectSnafu,
    EncodeJsonSnafu, Error, InvalidScanIndexSnafu, ListObjectsSnafu, ReadObjectSnafu, Result,
    Utf8Snafu, WriteObjectSnafu,
};

lazy_static! {
    static ref DELTA_RE: Regex = Regex::new("^\\d+\\.json").unwrap();
    static ref CHECKPOINT_RE: Regex = Regex::new("^\\d+\\.checkpoint").unwrap();
}

const LAST_CHECKPOINT_FILE: &str = "_last_checkpoint";
const DEFAULT_COMPRESSION_TYPE: CompressionType = CompressionType::GZIP;

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
    if compress_type == CompressionType::UNCOMPRESSED {
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

/// Return's the file compress algorithm by file extension.
///
/// for example file
/// `00000000000000000000.json.GZ` -> `CompressionType::GZIP`
#[inline]
pub fn file_compress_type(path: &str) -> CompressionType {
    let s = path.rsplit('.').next().unwrap_or("");
    CompressionType::from_str(s).unwrap_or(CompressionType::UNCOMPRESSED)
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

#[async_trait]
impl LogIterator for ObjectStoreLogIterator {
    type Error = Error;

    async fn next_log(&mut self) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        match self.iter.next() {
            Some((v, entry)) => {
                let compress_type = file_compress_type(entry.name());
                let bytes = self
                    .object_store
                    .read(entry.path())
                    .await
                    .context(ReadObjectSnafu { path: entry.path() })?;
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
}

impl ManifestObjectStore {
    pub fn new(path: &str, object_store: ObjectStore) -> Self {
        Self {
            object_store,
            compress_type: DEFAULT_COMPRESSION_TYPE,
            path: util::normalize_dir(path),
        }
    }

    #[inline]
    /// Returns the delta file path under the **current** compression algorithm
    fn delta_file_path(&self, version: ManifestVersion) -> String {
        gen_path(&self.path, &delta_file(version), self.compress_type)
    }

    #[inline]
    /// Returns the checkpoint file path under the **current** compression algorithm
    fn checkpoint_file_path(&self, version: ManifestVersion) -> String {
        gen_path(&self.path, &checkpoint_file(version), self.compress_type)
    }

    #[inline]
    /// Returns the last checkpoint path, because the last checkpoint is not compressed,
    /// so its path name has nothing to do with the compression algorithm used by `ManifestObjectStore`
    fn last_checkpoint_path(&self) -> String {
        format!("{}{}", self.path, LAST_CHECKPOINT_FILE)
    }

    /// Return all `R`s in the root directory that meet the `filter` conditions (that is, the `filter` closure returns `Some(R)`),
    /// and discard `R` that does not meet the conditions (that is, the `filter` closure returns `None`)
    async fn get_paths<F, R>(&self, filter: F) -> Result<Vec<R>>
    where
        F: Fn(Entry) -> Option<R>,
    {
        let streamer = self
            .object_store
            .list(&self.path)
            .await
            .context(ListObjectsSnafu { path: &self.path })?;
        streamer
            .try_filter_map(|e| async { Ok(filter(e)) })
            .try_collect::<Vec<_>>()
            .await
            .context(ListObjectsSnafu { path: &self.path })
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct CheckpointMetadata {
    pub size: usize,
    /// The latest version this checkpoint contains.
    pub version: ManifestVersion,
    pub checksum: Option<String>,
    pub extend_metadata: Option<HashMap<String, String>>,
}

impl CheckpointMetadata {
    fn encode(&self) -> Result<impl AsRef<[u8]>> {
        serde_json::to_string(self).context(EncodeJsonSnafu)
    }

    fn decode(bs: &[u8]) -> Result<Self> {
        let data = std::str::from_utf8(bs).context(Utf8Snafu)?;

        serde_json::from_str(data).context(DecodeJsonSnafu)
    }
}

#[async_trait]
impl ManifestLogStorage for ManifestObjectStore {
    type Error = Error;
    type Iter = ObjectStoreLogIterator;

    async fn scan(
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

    async fn delete_until(
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

        logging::debug!(
            "Deleting {} logs from manifest storage path {} until {}, checkpoint: {:?}, paths: {:?}",
            ret,
            self.path,
            end,
            checkpoint_version,
            paths,
        );

        self.object_store
            .remove(paths)
            .await
            .with_context(|_| DeleteObjectSnafu {
                path: self.path.clone(),
            })?;

        Ok(ret)
    }

    async fn save(&self, version: ManifestVersion, bytes: &[u8]) -> Result<()> {
        let path = self.delta_file_path(version);
        logging::debug!("Save log to manifest storage, version: {}", version);
        let data = self
            .compress_type
            .encode(bytes)
            .await
            .context(CompressObjectSnafu {
                compress_type: self.compress_type,
                path: &path,
            })?;
        self.object_store
            .write(&path, data)
            .await
            .context(WriteObjectSnafu { path })
    }

    async fn delete(&self, start: ManifestVersion, end: ManifestVersion) -> Result<()> {
        ensure!(start <= end, InvalidScanIndexSnafu { start, end });

        // Due to backward compatibility, it is possible that the user's log between start and end has not been compressed,
        // so we need to delete the uncompressed file corresponding to that version, even if the uncompressed file in that version do not exist.
        let mut paths = Vec::with_capacity(((end - start) * 2) as usize);
        for version in start..end {
            paths.push(raw_normalize_path(&self.delta_file_path(version)));
            paths.push(raw_normalize_path(&gen_path(
                &self.path,
                &delta_file(version),
                CompressionType::UNCOMPRESSED,
            )));
        }

        logging::debug!(
            "Deleting logs from manifest storage, start: {}, end: {}",
            start,
            end
        );

        self.object_store
            .remove(paths.clone())
            .await
            .with_context(|_| DeleteObjectSnafu {
                path: paths.join(","),
            })?;

        Ok(())
    }

    async fn save_checkpoint(&self, version: ManifestVersion, bytes: &[u8]) -> Result<()> {
        let path = self.checkpoint_file_path(version);
        let data = self
            .compress_type
            .encode(bytes)
            .await
            .context(CompressObjectSnafu {
                compress_type: self.compress_type,
                path: &path,
            })?;
        self.object_store
            .write(&path, data)
            .await
            .context(WriteObjectSnafu { path })?;

        // Because last checkpoint file only contain size and version, which is tiny, so we don't compress it.
        let last_checkpoint_path = self.last_checkpoint_path();

        let checkpoint_metadata = CheckpointMetadata {
            size: bytes.len(),
            version,
            checksum: None,
            extend_metadata: None,
        };

        logging::debug!(
            "Save checkpoint in path: {},  metadata: {:?}",
            last_checkpoint_path,
            checkpoint_metadata
        );

        let bs = checkpoint_metadata.encode()?;
        self.object_store
            .write(&last_checkpoint_path, bs.as_ref().to_vec())
            .await
            .context(WriteObjectSnafu {
                path: last_checkpoint_path,
            })?;

        Ok(())
    }

    async fn load_checkpoint(
        &self,
        version: ManifestVersion,
    ) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        let mut path = self.checkpoint_file_path(version);
        // Due to backward compatibility, it is possible that the user's checkpoint not compressed,
        // so if we don't find file by compressed type. fall back to checkpoint not compressed find again.
        let checkpoint_data = match self.object_store.read(&path).await {
            Ok(checkpoint) => Ok(Some(checkpoint)),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                path = gen_path(
                    &self.path,
                    &checkpoint_file(version),
                    CompressionType::UNCOMPRESSED,
                );
                match self.object_store.read(&path).await {
                    Ok(checkpoint) => Ok(Some(checkpoint)),
                    Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e).context(ReadObjectSnafu { path: &path }),
                }
            }
            Err(e) => Err(e).context(ReadObjectSnafu { path: &path }),
        }?;
        if let Some(data) = checkpoint_data {
            let decompress_data =
                self.compress_type
                    .decode(data)
                    .await
                    .context(DecompressObjectSnafu {
                        compress_type: self.compress_type,
                        path,
                    })?;
            Ok(Some((version, decompress_data)))
        } else {
            Ok(None)
        }
    }

    async fn delete_checkpoint(&self, version: ManifestVersion) -> Result<()> {
        // Due to backward compatibility, it is possible that the user's checkpoint file has not been compressed,
        // so we need to delete the uncompressed checkpoint file corresponding to that version, even if the uncompressed checkpoint file in that version do not exist.
        let paths = vec![
            raw_normalize_path(&self.checkpoint_file_path(version)),
            raw_normalize_path(&gen_path(
                &self.path,
                &checkpoint_file(version),
                CompressionType::UNCOMPRESSED,
            )),
        ];

        self.object_store
            .remove(paths.clone())
            .await
            .context(DeleteObjectSnafu {
                path: paths.join(","),
            })?;
        Ok(())
    }

    async fn load_last_checkpoint(&self) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        let last_checkpoint_path = self.last_checkpoint_path();
        let last_checkpoint_data = match self.object_store.read(&last_checkpoint_path).await {
            Ok(data) => data,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(e) => {
                return Err(e).context(ReadObjectSnafu {
                    path: last_checkpoint_path,
                });
            }
        };

        let checkpoint_metadata = CheckpointMetadata::decode(&last_checkpoint_data)?;

        logging::debug!(
            "Load checkpoint in path: {}, metadata: {:?}",
            last_checkpoint_path,
            checkpoint_metadata
        );

        self.load_checkpoint(checkpoint_metadata.version).await
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;
    use object_store::ObjectStore;

    use super::*;

    #[test]
    // Define this test mainly to prevent future unintentional changes may break the backward compatibility.
    fn test_compress_file_path_generation() {
        let path = "/foo/bar/";
        let version: ManifestVersion = 0;
        let file_path = gen_path(path, &delta_file(version), CompressionType::GZIP);
        assert_eq!(file_path.as_str(), "/foo/bar/00000000000000000000.json.gz")
    }

    #[tokio::test]
    async fn test_manifest_log_store() {
        common_telemetry::init_default_ut_logging();
        let tmp_dir = create_temp_dir("test_manifest_log_store");
        let mut builder = Fs::default();
        builder.root(&tmp_dir.path().to_string_lossy());
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let log_store = ManifestObjectStore::new("/", object_store);

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

        // Delete [0, 3)
        log_store.delete(0, 3).await.unwrap();

        // [3, 5) remains
        let mut it = log_store.scan(0, 11).await.unwrap();
        for v in 3..5 {
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
        log_store.delete_until(4, true).await.unwrap();
        assert!(log_store.load_checkpoint(3).await.unwrap().is_some());
        assert!(log_store.load_last_checkpoint().await.unwrap().is_some());
        let mut it = log_store.scan(0, 11).await.unwrap();
        let (version, bytes) = it.next_log().await.unwrap().unwrap();
        assert_eq!(4, version);
        assert_eq!("hello, 4".as_bytes(), bytes);
        assert!(it.next_log().await.unwrap().is_none());

        // delete all logs and checkpoints
        log_store.delete_until(11, false).await.unwrap();
        assert!(log_store.load_checkpoint(3).await.unwrap().is_none());
        assert!(log_store.load_last_checkpoint().await.unwrap().is_none());
        let mut it = log_store.scan(0, 11).await.unwrap();
        assert!(it.next_log().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_compress_backward_compatible() {
        // test ManifestObjectStore can read previously uncompressed data correctly
        common_telemetry::init_default_ut_logging();
        let tmp_dir = create_temp_dir("test_manifest_log_store");
        let mut builder = Fs::default();
        builder.root(&tmp_dir.path().to_string_lossy());
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let log_store = ManifestObjectStore::new("/", object_store.clone());

        // write uncompress data directly to stimulate previously uncompressed data
        for v in 0..5 {
            let path = format!("{}{}", log_store.path(), delta_file(v));
            object_store
                .write(&path, format!("hello, {v}"))
                .await
                .unwrap();
        }

        // write different compressed data directly to stimulate previously change compress alogorithom
        for v in 5..10 {
            let path = format!(
                "{}{}.{}",
                log_store.path(),
                delta_file(v),
                CompressionType::GZIP
            );
            let data = format!("hello, {v}");
            let compress_data = CompressionType::GZIP.encode(data).await.unwrap();
            object_store.write(&path, compress_data).await.unwrap();
        }
        for v in 10..15 {
            let path = format!(
                "{}{}.{}",
                log_store.path(),
                delta_file(v),
                CompressionType::BZIP2
            );
            let data = format!("hello, {v}");
            let compress_data = CompressionType::BZIP2.encode(data).await.unwrap();
            object_store.write(&path, compress_data).await.unwrap();
        }
        for v in 15..20 {
            let path = format!(
                "{}{}.{}",
                log_store.path(),
                delta_file(v),
                CompressionType::XZ
            );
            let data = format!("hello, {v}");
            let compress_data = CompressionType::XZ.encode(data).await.unwrap();
            object_store.write(&path, compress_data).await.unwrap();
        }

        // write compress data
        for v in 20..25 {
            log_store
                .save(v, format!("hello, {v}").as_bytes())
                .await
                .unwrap();
        }

        let mut it = log_store.scan(0, 25).await.unwrap();
        for v in 0..25 {
            let (version, bytes) = it.next_log().await.unwrap().unwrap();
            assert_eq!(v, version);
            assert_eq!(format!("hello, {v}").as_bytes(), bytes);
        }
    }
}
