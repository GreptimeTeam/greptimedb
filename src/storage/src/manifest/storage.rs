use std::collections::HashMap;
use std::iter::Iterator;

use async_trait::async_trait;
use common_telemetry::logging;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use object_store::{util, DirEntry, ObjectStore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::manifest::{LogIterator, ManifestLogStorage, ManifestVersion};

use crate::error::{
    DecodeJsonSnafu, DeleteObjectSnafu, EncodeJsonSnafu, Error, InvalidScanIndexSnafu,
    ListObjectsSnafu, ReadObjectSnafu, Result, Utf8Snafu, WriteObjectSnafu,
};

lazy_static! {
    static ref RE: Regex = Regex::new("^\\d+\\.json$").unwrap();
}

const LAST_CHECKPOINT_FILE: &str = "_last_checkpoint";

#[inline]
pub fn delta_file(version: ManifestVersion) -> String {
    format!("{:020}.json", version)
}

#[inline]
pub fn checkpoint_file(version: ManifestVersion) -> String {
    format!("{:020}.checkpoint", version)
}

/// Return's the delta file version from path
///
/// # Panics
/// Panics if the file path is not a valid delta file.
#[inline]
pub fn delta_version(path: &str) -> ManifestVersion {
    let s = path.split('.').next().unwrap();
    s.parse()
        .unwrap_or_else(|_| panic!("Invalid delta file: {}", path))
}

#[inline]
pub fn is_delta_file(file_name: &str) -> bool {
    RE.is_match(file_name)
}

pub struct ObjectStoreLogIterator {
    iter: Box<dyn Iterator<Item = (ManifestVersion, DirEntry)> + Send + Sync>,
}

#[async_trait]
impl LogIterator for ObjectStoreLogIterator {
    type Error = Error;

    async fn next_log(&mut self) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        match self.iter.next() {
            Some((v, e)) => {
                let object = e.into_object();
                let bytes = object.read().await.context(ReadObjectSnafu {
                    path: object.path(),
                })?;

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

    fn delta_file_path(&self, version: ManifestVersion) -> String {
        format!("{}{}", self.path, delta_file(version))
    }

    fn checkpoint_file_path(&self, version: ManifestVersion) -> String {
        format!("{}{}", self.path, checkpoint_file(version))
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct CheckpointMetadata {
    pub size: usize,
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

        let dir = self.object_store.object(&self.path);
        let dir_exists = dir
            .is_exist()
            .await
            .context(ReadObjectSnafu { path: &self.path })?;

        if !dir_exists {
            return Ok(ObjectStoreLogIterator {
                iter: Box::new(Vec::default().into_iter()),
            });
        }

        let streamer = dir
            .list()
            .await
            .context(ListObjectsSnafu { path: &self.path })?;

        let mut entries: Vec<(ManifestVersion, DirEntry)> = streamer
            .try_filter_map(|e| async move {
                let file_name = e.name();
                if is_delta_file(file_name) {
                    let version = delta_version(file_name);
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
            iter: Box::new(entries.into_iter()),
        })
    }

    async fn save(&self, version: ManifestVersion, bytes: &[u8]) -> Result<()> {
        let object = self.object_store.object(&self.delta_file_path(version));
        object.write(bytes).await.context(WriteObjectSnafu {
            path: object.path(),
        })?;

        Ok(())
    }

    async fn delete(&self, start: ManifestVersion, end: ManifestVersion) -> Result<()> {
        //TODO(dennis): delete in batch or concurrently?
        for v in start..end {
            let object = self.object_store.object(&self.delta_file_path(v));
            object.delete().await.context(DeleteObjectSnafu {
                path: object.path(),
            })?;
        }

        Ok(())
    }

    async fn save_checkpoint(&self, version: ManifestVersion, bytes: &[u8]) -> Result<()> {
        let object = self
            .object_store
            .object(&self.checkpoint_file_path(version));
        object.write(bytes).await.context(WriteObjectSnafu {
            path: object.path(),
        })?;

        let last_checkpoint = self
            .object_store
            .object(&format!("{}{}", self.path, LAST_CHECKPOINT_FILE));

        let checkpoint_metadata = CheckpointMetadata {
            size: bytes.len(),
            version,
            checksum: None,
            extend_metadata: None,
        };

        logging::debug!(
            "Save checkpoint in path: {},  metadata: {:?}",
            last_checkpoint.path(),
            checkpoint_metadata
        );

        let bs = checkpoint_metadata.encode()?;
        last_checkpoint.write(bs).await.context(WriteObjectSnafu {
            path: last_checkpoint.path(),
        })?;

        Ok(())
    }

    async fn load_checkpoint(&self) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        let last_checkpoint = self
            .object_store
            .object(&format!("{}{}", self.path, LAST_CHECKPOINT_FILE));

        let checkpoint_exists = last_checkpoint.is_exist().await.context(ReadObjectSnafu {
            path: last_checkpoint.path(),
        })?;

        if checkpoint_exists {
            let bytes = last_checkpoint.read().await.context(ReadObjectSnafu {
                path: last_checkpoint.path(),
            })?;

            let checkpoint_metadata = CheckpointMetadata::decode(&bytes)?;

            logging::debug!(
                "Load checkpoint in path: {},  metadata: {:?}",
                last_checkpoint.path(),
                checkpoint_metadata
            );

            let checkpoint = self
                .object_store
                .object(&self.checkpoint_file_path(checkpoint_metadata.version));

            Ok(Some((
                checkpoint_metadata.version,
                checkpoint.read().await.context(ReadObjectSnafu {
                    path: checkpoint.path(),
                })?,
            )))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::{backend::fs, ObjectStore};
    use tempdir::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_manifest_log_store() {
        common_telemetry::init_default_ut_logging();
        let tmp_dir = TempDir::new("test_manifest_log_store").unwrap();
        let object_store = ObjectStore::new(
            fs::Backend::build()
                .root(&tmp_dir.path().to_string_lossy())
                .finish()
                .await
                .unwrap(),
        );

        let log_store = ManifestObjectStore::new("/", object_store);

        for v in 0..5 {
            log_store
                .save(v, format!("hello, {}", v).as_bytes())
                .await
                .unwrap();
        }

        let mut it = log_store.scan(1, 4).await.unwrap();
        for v in 1..4 {
            let (version, bytes) = it.next_log().await.unwrap().unwrap();
            assert_eq!(v, version);
            assert_eq!(format!("hello, {}", v).as_bytes(), bytes);
        }
        assert!(it.next_log().await.unwrap().is_none());

        let mut it = log_store.scan(0, 11).await.unwrap();
        for v in 0..5 {
            let (version, bytes) = it.next_log().await.unwrap().unwrap();
            assert_eq!(v, version);
            assert_eq!(format!("hello, {}", v).as_bytes(), bytes);
        }
        assert!(it.next_log().await.unwrap().is_none());

        // Delete [0, 3)
        log_store.delete(0, 3).await.unwrap();

        // [3, 5) remains
        let mut it = log_store.scan(0, 11).await.unwrap();
        for v in 3..5 {
            let (version, bytes) = it.next_log().await.unwrap().unwrap();
            assert_eq!(v, version);
            assert_eq!(format!("hello, {}", v).as_bytes(), bytes);
        }
        assert!(it.next_log().await.unwrap().is_none());

        // test checkpoint
        assert!(log_store.load_checkpoint().await.unwrap().is_none());
        log_store
            .save_checkpoint(3, "checkpoint".as_bytes())
            .await
            .unwrap();

        let (v, checkpoint) = log_store.load_checkpoint().await.unwrap().unwrap();
        assert_eq!(checkpoint, "checkpoint".as_bytes());
        assert_eq!(3, v);
    }
}
