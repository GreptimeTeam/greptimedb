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

use common_datasource::compression::CompressionType;
use common_telemetry::debug;
use object_store::{ErrorKind, ObjectStore};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::ManifestVersion;

use crate::cache::manifest_cache::ManifestCache;
use crate::error::{
    CompressObjectSnafu, DecompressObjectSnafu, OpenDalSnafu, Result, SerdeJsonSnafu, Utf8Snafu,
};
use crate::manifest::storage::size_tracker::TrackerRef;
use crate::manifest::storage::utils::{get_from_cache, put_to_cache, write_and_put_cache};
use crate::manifest::storage::{
    FALL_BACK_COMPRESS_TYPE, LAST_CHECKPOINT_FILE, checkpoint_checksum, checkpoint_file, gen_path,
    verify_checksum,
};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct CheckpointMetadata {
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

/// Handle checkpoint storage operations.
#[derive(Debug, Clone)]
pub(crate) struct CheckpointStorage {
    object_store: ObjectStore,
    compress_type: CompressionType,
    path: String,
    manifest_cache: Option<ManifestCache>,
    size_tracker: TrackerRef,
}

impl CheckpointStorage {
    pub fn new(
        path: String,
        object_store: ObjectStore,
        compress_type: CompressionType,
        manifest_cache: Option<ManifestCache>,
        size_tracker: TrackerRef,
    ) -> Self {
        Self {
            object_store,
            compress_type,
            path,
            manifest_cache,
            size_tracker,
        }
    }

    /// Returns the last checkpoint path, because the last checkpoint is not compressed,
    /// so its path name has nothing to do with the compression algorithm used by `ManifestObjectStore`
    pub(crate) fn last_checkpoint_path(&self) -> String {
        format!("{}{}", self.path, LAST_CHECKPOINT_FILE)
    }

    /// Returns the checkpoint file path under the **current** compression algorithm
    fn checkpoint_file_path(&self, version: ManifestVersion) -> String {
        gen_path(&self.path, &checkpoint_file(version), self.compress_type)
    }

    pub(crate) async fn load_checkpoint(
        &mut self,
        metadata: CheckpointMetadata,
    ) -> Result<Option<(ManifestVersion, Vec<u8>)>> {
        let version = metadata.version;
        let path = self.checkpoint_file_path(version);

        // Try to get from cache first
        if let Some(data) = get_from_cache(self.manifest_cache.as_ref(), &path).await {
            verify_checksum(&data, metadata.checksum)?;
            return Ok(Some((version, data)));
        }

        // Due to backward compatibility, it is possible that the user's checkpoint not compressed,
        // so if we don't find file by compressed type. fall back to checkpoint not compressed find again.
        let checkpoint_data = match self.object_store.read(&path).await {
            Ok(checkpoint) => {
                let checkpoint_size = checkpoint.len();
                let decompress_data =
                    self.compress_type
                        .decode(checkpoint)
                        .await
                        .with_context(|_| DecompressObjectSnafu {
                            compress_type: self.compress_type,
                            path: path.clone(),
                        })?;
                verify_checksum(&decompress_data, metadata.checksum)?;
                // set the checkpoint size
                self.size_tracker.record(version, checkpoint_size as u64);
                // Add to cache
                put_to_cache(self.manifest_cache.as_ref(), path, &decompress_data).await;
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

                        // Try to get fallback from cache first
                        if let Some(data) =
                            get_from_cache(self.manifest_cache.as_ref(), &fall_back_path).await
                        {
                            verify_checksum(&data, metadata.checksum)?;
                            return Ok(Some((version, data)));
                        }

                        match self.object_store.read(&fall_back_path).await {
                            Ok(checkpoint) => {
                                let checkpoint_size = checkpoint.len();
                                let decompress_data = FALL_BACK_COMPRESS_TYPE
                                    .decode(checkpoint)
                                    .await
                                    .with_context(|_| DecompressObjectSnafu {
                                        compress_type: FALL_BACK_COMPRESS_TYPE,
                                        path: fall_back_path.clone(),
                                    })?;
                                verify_checksum(&decompress_data, metadata.checksum)?;
                                self.size_tracker.record(version, checkpoint_size as u64);
                                // Add fallback to cache
                                put_to_cache(
                                    self.manifest_cache.as_ref(),
                                    fall_back_path,
                                    &decompress_data,
                                )
                                .await;
                                Ok(Some(decompress_data))
                            }
                            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                            Err(e) => return Err(e).context(OpenDalSnafu),
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

        // Fetch from remote object store without cache
        let last_checkpoint_data = match self.object_store.read(&last_checkpoint_path).await {
            Ok(data) => data.to_vec(),
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

        self.load_checkpoint(checkpoint_metadata).await
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

        write_and_put_cache(
            &self.object_store,
            self.manifest_cache.as_ref(),
            &path,
            data,
        )
        .await?;
        self.size_tracker.record(version, checkpoint_size as u64);

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
}

#[cfg(test)]
impl CheckpointStorage {
    pub async fn write_last_checkpoint(
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

        self.object_store
            .write(&path, data)
            .await
            .context(OpenDalSnafu)?;

        self.size_tracker.record(version, checkpoint_size as u64);

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

    pub fn set_compress_type(&mut self, compress_type: CompressionType) {
        self.compress_type = compress_type;
    }
}
