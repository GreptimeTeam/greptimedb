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

use common_telemetry::debug;
use object_store::{ObjectStore, util};
use snafu::ResultExt;

use crate::error::{OpenDalSnafu, Result};
use crate::manifest::storage::utils::write_and_put_cache;

/// A simple blob storage for arbitrary binary data in the staging directory.
///
/// This is primarily used during repartition operations to store generated
/// manifests that will be consumed by other regions via [`ApplyStagingManifestRequest`](store_api::region_request::ApplyStagingManifestRequest).
/// The blobs are stored in `{region_dir}/staging/blob/` directory.
#[derive(Debug, Clone)]
pub(crate) struct StagingBlobStorage {
    object_store: ObjectStore,
    path: String,
}

/// Returns the staging path from the blob path.
///
/// # Example
/// - Input: `"data/table/region_0001/manifest/"`
/// - Output: `"data/table/region_0001/staging/blob/"`
pub fn staging_blob_path(manifest_path: &str) -> String {
    let parent_dir = manifest_path
        .trim_end_matches("manifest/")
        .trim_end_matches('/');
    util::normalize_dir(&format!("{}/staging/blob", parent_dir))
}

impl StagingBlobStorage {
    pub fn new(path: String, object_store: ObjectStore) -> Self {
        let path = util::normalize_dir(&path);
        common_telemetry::debug!(
            "Staging blob storage path: {}, root: {}",
            path,
            object_store.info().root()
        );
        Self { object_store, path }
    }

    /// Returns the path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the object store.
    pub fn object_store(&self) -> &ObjectStore {
        &self.object_store
    }

    /// Put the bytes to the blob storage.
    pub async fn put(&self, path: &str, bytes: Vec<u8>) -> Result<()> {
        let path = format!("{}{}", self.path, path);
        common_telemetry::debug!(
            "Putting blob to staging blob storage, path: {}, root: {}, bytes: {}",
            path,
            self.object_store.info().root(),
            bytes.len()
        );
        write_and_put_cache(&self.object_store, None, &path, bytes).await?;
        Ok(())
    }

    /// Get the bytes from the blob storage.
    pub async fn get(&self, path: &str) -> Result<Vec<u8>> {
        let path = format!("{}{}", self.path, path);
        common_telemetry::debug!(
            "Reading blob from staging blob storage, path: {}, root: {}",
            path,
            self.object_store.info().root()
        );
        let bytes = self.object_store.read(&path).await.context(OpenDalSnafu)?;

        Ok(bytes.to_vec())
    }
}

/// Storage for the staging directory used during repartition operations.
///
/// Directory structure:
/// - `{region_dir}/staging/manifest/` — manifest deltas that older versions staged. It is only
///   cleared.
/// - `{region_dir}/staging/blob/` — for arbitrary blobs (e.g., generated region manifests).
#[derive(Debug, Clone)]
pub(crate) struct StagingStorage {
    object_store: ObjectStore,
    manifest_path: String,
    blob_storage: StagingBlobStorage,
}

/// Returns the staging path from the manifest path.
///
/// # Example
/// - Input: `"data/table/region_0001/manifest/"`
/// - Output: `"data/table/region_0001/staging/manifest/"`
pub fn staging_manifest_path(manifest_path: &str) -> String {
    let parent_dir = manifest_path
        .trim_end_matches("manifest/")
        .trim_end_matches('/');
    util::normalize_dir(&format!("{}/staging/manifest", parent_dir))
}

impl StagingStorage {
    pub fn new(path: String, object_store: ObjectStore) -> Self {
        let blob_storage = StagingBlobStorage::new(staging_blob_path(&path), object_store.clone());

        Self {
            object_store,
            manifest_path: staging_manifest_path(&path),
            blob_storage,
        }
    }

    /// Returns the blob storage.
    pub(crate) fn blob_storage(&self) -> &StagingBlobStorage {
        &self.blob_storage
    }

    /// Clean all staging manifest files.
    pub(crate) async fn clear(&self) -> Result<()> {
        self.object_store
            .delete_with(&self.manifest_path)
            .recursive(true)
            .await
            .context(OpenDalSnafu)?;

        self.blob_storage
            .object_store()
            .delete_with(self.blob_storage.path())
            .recursive(true)
            .await
            .context(OpenDalSnafu)?;

        debug!(
            "Cleared all staging manifest files from {}, blob directory: {}",
            self.manifest_path,
            self.blob_storage.path()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use common_test_util::temp_dir::create_temp_dir;
    use futures::TryStreamExt;
    use object_store::layers::mock::{MockLayerBuilder, ObservedWriter, WriterObservations, oio};
    use object_store::services::Fs;
    use object_store::{ErrorKind, ObjectStore};

    use super::{StagingBlobStorage, StagingStorage, staging_blob_path, staging_manifest_path};

    // A test-only per-write limit, not a storage provider's default limit.
    const WRITE_LIMIT: usize = 64 * 1024 * 1024;

    struct LimitedWriter {
        inner: oio::Writer,
    }

    impl oio::Write for LimitedWriter {
        async fn write(&mut self, bytes: object_store::Buffer) -> object_store::Result<()> {
            if bytes.len() > WRITE_LIMIT {
                return Err(object_store::Error::new(
                    ErrorKind::Unexpected,
                    "test write size limit exceeded",
                ));
            }
            self.inner.write(bytes).await
        }

        async fn close(&mut self) -> object_store::Result<object_store::layers::mock::Metadata> {
            self.inner.close().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.inner.abort().await
        }
    }

    #[tokio::test]
    async fn test_staging_blob_put_chunked() {
        let tmp_dir = create_temp_dir("test_staging_blob_put_chunked");
        let observations = Arc::new(Mutex::new(WriterObservations::default()));
        let captured = observations.clone();
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |_, _, inner| {
                Box::new(ObservedWriter::new(
                    Box::new(LimitedWriter { inner }),
                    captured.clone(),
                ))
            }))
            .build()
            .unwrap();
        let store = ObjectStore::new(Fs::default().root(&tmp_dir.path().to_string_lossy()))
            .unwrap()
            .layer(layer);
        let storage = StagingBlobStorage::new("staging/blob/".to_string(), store);

        for size in [17, WRITE_LIMIT, WRITE_LIMIT + 17] {
            let bytes = (0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>();
            *observations.lock().unwrap() = WriterObservations::default();
            storage.put("manifest", bytes.clone()).await.unwrap();
            assert_eq!(storage.get("manifest").await.unwrap(), bytes);
            let observed = observations.lock().unwrap();
            assert_eq!(observed.write_sizes.iter().sum::<usize>(), size);
            assert!(observed.write_sizes.iter().all(|size| *size <= WRITE_LIMIT));
            assert_eq!(observed.write_sizes.len(), size.div_ceil(WRITE_LIMIT));
            assert_eq!(observed.close_calls, 1);
        }
    }

    #[test]
    fn test_staging_path() {
        let path = "/data/table/region_0001/manifest/";
        let expected = "/data/table/region_0001/staging/manifest/";
        assert_eq!(staging_manifest_path(path), expected);
    }

    #[test]
    fn test_staging_blob_path() {
        let path = "/data/table/region_0001/manifest/";
        let expected = "/data/table/region_0001/staging/blob/";
        assert_eq!(staging_blob_path(path), expected);
    }

    async fn count_entries(object_store: &ObjectStore, path: &str) -> usize {
        match object_store.lister_with(path).await {
            Ok(lister) => lister.try_collect::<Vec<_>>().await.unwrap().len(),
            Err(err) if err.kind() == ErrorKind::NotFound => 0,
            Err(err) => panic!("failed to list {path}: {err}"),
        }
    }

    #[tokio::test]
    async fn test_staging_clear_removes_blobs() {
        common_telemetry::init_default_ut_logging();

        let tmp_dir = create_temp_dir("test_staging_storage_clear");
        let builder = Fs::default().root(&tmp_dir.path().to_string_lossy());
        let object_store = ObjectStore::new(builder).unwrap();
        let manifest_path = "/data/table/region_0001/manifest/";
        let storage = StagingStorage::new(manifest_path.to_string(), object_store.clone());
        let manifest_dir = staging_manifest_path(manifest_path);
        let blob_dir = staging_blob_path(manifest_path);

        // A delta staged by an older version.
        object_store
            .write(
                &format!("{manifest_dir}00000000000000000000.json"),
                b"manifest".to_vec(),
            )
            .await
            .unwrap();
        storage
            .blob_storage()
            .put("region_0002_manifest", b"blob".to_vec())
            .await
            .unwrap();

        assert!(count_entries(&object_store, &manifest_dir).await > 0);
        assert!(count_entries(&object_store, &blob_dir).await > 0);

        storage.clear().await.unwrap();

        assert_eq!(count_entries(&object_store, &manifest_dir).await, 0);
        assert_eq!(count_entries(&object_store, &blob_dir).await, 0);
    }
}
