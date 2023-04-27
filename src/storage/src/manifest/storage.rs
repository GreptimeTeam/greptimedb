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

use async_trait::async_trait;
use common_telemetry::logging;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use object_store::{raw_normalize_path, util, Entry, ErrorKind, ObjectStore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::manifest::{LogIterator, ManifestLogStorage, ManifestVersion};

use crate::error::{
    DecodeJsonSnafu, DeleteObjectSnafu, EncodeJsonSnafu, Error, InvalidScanIndexSnafu,
    ListObjectsSnafu, ReadObjectSnafu, Result, Utf8Snafu, WriteObjectSnafu,
};

lazy_static! {
    static ref DELTA_RE: Regex = Regex::new("^\\d+\\.json$").unwrap();
    static ref CHECKPOINT_RE: Regex = Regex::new("^\\d+\\.checkpoint").unwrap();
}

const LAST_CHECKPOINT_FILE: &str = "_last_checkpoint";

#[inline]
pub fn delta_file(version: ManifestVersion) -> String {
    format!("{version:020}.json")
}

#[inline]
pub fn checkpoint_file(version: ManifestVersion) -> String {
    format!("{version:020}.checkpoint")
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
                let bytes = self
                    .object_store
                    .read(entry.path())
                    .await
                    .context(ReadObjectSnafu { path: entry.path() })?;
                Ok(Some((v, bytes)))
            }
            None => Ok(None),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ManifestObjectStore {
    object_store: ObjectStore,
    path: String,
}

impl ManifestObjectStore {
    pub fn new(path: &str, object_store: ObjectStore) -> Self {
        Self {
            object_store,
            path: util::normalize_dir(path),
        }
    }

    #[inline]
    fn delta_file_path(&self, version: ManifestVersion) -> String {
        format!("{}{}", self.path, delta_file(version))
    }

    #[inline]
    fn checkpoint_file_path(&self, version: ManifestVersion) -> String {
        format!("{}{}", self.path, checkpoint_file(version))
    }

    #[inline]
    fn last_checkpoint_path(&self) -> String {
        format!("{}{}", self.path, LAST_CHECKPOINT_FILE)
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

        let streamer = self
            .object_store
            .list(&self.path)
            .await
            .context(ListObjectsSnafu { path: &self.path })?;

        let mut entries: Vec<(ManifestVersion, Entry)> = streamer
            .try_filter_map(|e| async move {
                let file_name = e.name();
                if is_delta_file(file_name) {
                    let version = file_version(file_name);
                    if version >= start && version < end {
                        Ok(Some((version, e)))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            })
            .try_collect::<Vec<_>>()
            .await
            .context(ListObjectsSnafu { path: &self.path })?;

        entries.sort_unstable_by(|(v1, _), (v2, _)| v1.cmp(v2));

        Ok(ObjectStoreLogIterator {
            object_store: self.object_store.clone(),
            iter: Box::new(entries.into_iter()),
        })
    }

    async fn delete_until(&self, end: ManifestVersion) -> Result<usize> {
        let streamer = self
            .object_store
            .list(&self.path)
            .await
            .context(ListObjectsSnafu { path: &self.path })?;

        let paths: Vec<_> = streamer
            .try_filter_map(|e| async move {
                let file_name = e.name();
                if is_delta_file(file_name) || is_checkpoint_file(file_name) {
                    let version = file_version(file_name);
                    if version < end {
                        Ok(Some(e.path().to_string()))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            })
            .try_collect::<Vec<_>>()
            .await
            .context(ListObjectsSnafu { path: &self.path })?;
        let ret = paths.len();

        logging::debug!(
            "Deleting {} logs from manifest storage path {} until end {}.",
            ret,
            self.path,
            end,
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

        self.object_store
            .write(&path, bytes.to_vec())
            .await
            .context(WriteObjectSnafu { path })
    }

    async fn delete(&self, start: ManifestVersion, end: ManifestVersion) -> Result<()> {
        let raw_paths = (start..end)
            .map(|v| self.delta_file_path(v))
            .collect::<Vec<_>>();

        logging::debug!(
            "Deleting logs from manifest storage, start: {}, end: {}",
            start,
            end
        );

        let paths = raw_paths
            .iter()
            .map(|p| raw_normalize_path(p))
            .collect::<Vec<_>>();

        self.object_store
            .remove(paths)
            .await
            .with_context(|_| DeleteObjectSnafu {
                path: raw_paths.join(","),
            })?;

        Ok(())
    }

    async fn save_checkpoint(&self, version: ManifestVersion, bytes: &[u8]) -> Result<()> {
        let path = self.checkpoint_file_path(version);
        self.object_store
            .write(&path, bytes.to_vec())
            .await
            .context(WriteObjectSnafu { path })?;

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
        let path = self.checkpoint_file_path(version);
        match self.object_store.read(&path).await {
            Ok(checkpoint) => Ok(Some((version, checkpoint))),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).context(ReadObjectSnafu { path }),
        }
    }

    async fn delete_checkpoint(&self, version: ManifestVersion) -> Result<()> {
        let path = self.checkpoint_file_path(version);
        self.object_store
            .delete(&path)
            .await
            .context(DeleteObjectSnafu { path })?;
        Ok(())
    }

    async fn load_last_checkpoint(&self) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        let last_checkpoint_path = self.last_checkpoint_path();

        let last_checkpoint_data = match self.object_store.read(&last_checkpoint_path).await {
            Ok(last_checkpoint_data) => last_checkpoint_data,
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

        //delete (,4) logs and checkpoints
        log_store.delete_until(4).await.unwrap();
        assert!(log_store.load_checkpoint(3).await.unwrap().is_none());
        assert!(log_store.load_last_checkpoint().await.unwrap().is_none());
        let mut it = log_store.scan(0, 11).await.unwrap();
        let (version, bytes) = it.next_log().await.unwrap().unwrap();
        assert_eq!(4, version);
        assert_eq!("hello, 4".as_bytes(), bytes);
        assert!(it.next_log().await.unwrap().is_none());

        // delete all logs and checkpoints
        log_store.delete_until(11).await.unwrap();
        assert!(log_store.load_checkpoint(3).await.unwrap().is_none());
        assert!(log_store.load_last_checkpoint().await.unwrap().is_none());
        let mut it = log_store.scan(0, 11).await.unwrap();
        assert!(it.next_log().await.unwrap().is_none());
    }
}
