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

use common_config::KvBackendConfig;
use common_error::ext::BoxedError;
use common_meta::kv_backend::KvBackendRef;
use common_meta::snapshot::MetadataSnapshotManager;
use common_telemetry::tracing::warn;
use common_telemetry::{error, info};
use log_store::raft_engine::RaftEngineBackend;
use object_store::manager::ObjectStoreManagerRef;
use snafu::ResultExt;

use crate::error::{
    CheckTargetSourceCleanSnafu, OpenMetadataKvBackendSnafu, RestoreMetadataSnapshotSnafu, Result,
};

/// Builds the metadata kvbackend.
pub fn build_metadata_kvbackend(dir: String, config: &KvBackendConfig) -> Result<KvBackendRef> {
    info!(
        "Creating metadata kvbackend with dir: {}, config: {:?}",
        dir, config
    );
    let kv_backend = RaftEngineBackend::try_open_with_cfg(dir, config)
        .map_err(BoxedError::new)
        .context(OpenMetadataKvBackendSnafu)?;

    Ok(Arc::new(kv_backend))
}

/// Attempts to restore metadata from a snapshot file if the target source is clean.
///
/// This initializes the metadata store from a snapshot file. Will not overwrite existing
/// data unless the store is confirmed empty/clean. If the target store is not clean,
/// initialization is skipped and a warning is logged.
pub async fn restore_metadata_from_snapshot(
    kv_backend: &KvBackendRef,
    object_store_manager: &ObjectStoreManagerRef,
    file_path: &str,
    ignore_error: bool,
) -> Result<()> {
    let object_store = object_store_manager.default_object_store();
    let manager = MetadataSnapshotManager::new(kv_backend.clone(), object_store.clone());
    let is_empty = manager
        .check_target_source_clean()
        .await
        .map_err(BoxedError::new)
        .context(CheckTargetSourceCleanSnafu)?;

    if !is_empty {
        warn!(
            "The target metadata store is not empty; skipping restoration from metadata snapshot."
        );
        return Ok(());
    }

    let result = manager
        .restore(file_path)
        .await
        .map_err(BoxedError::new)
        .context(RestoreMetadataSnapshotSnafu);

    match result {
        Ok(count) => {
            info!(
                "Restored {} key-value pairs from metadata snapshot({}).",
                count, file_path,
            );
        }
        Err(err) => {
            if ignore_error {
                error!(
                    err;
                    "Failed to restore metadata from snapshot({}), but ignore the error.",
                    file_path
                );
            } else {
                return Err(err);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use common_meta::kv_backend::KvBackend;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::rpc::store::PutRequest;
    use datanode::config::StorageConfig;
    use datanode::datanode::DatanodeBuilder;
    use object_store::ObjectStore;
    use object_store::config::{FileConfig, ObjectStoreConfig};

    use super::*;

    async fn setup_backup_file(object_store: ObjectStore, file_path: &str) {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = MetadataSnapshotManager::new(kv_backend.clone(), object_store);
        // Put some data into the kv backend
        kv_backend
            .put(
                PutRequest::new()
                    .with_key(b"test".to_vec())
                    .with_value(b"test".to_vec()),
            )
            .await
            .unwrap();
        manager.dump(file_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_restore_metadata_from_snapshot() {
        common_telemetry::init_default_ut_logging();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage_config = StorageConfig {
            data_home: format!("{}/home", temp_dir.path().display()),
            store: ObjectStoreConfig::File(FileConfig {}),
            providers: vec![],
        };
        let object_store_manager = DatanodeBuilder::build_object_store_manager(&storage_config)
            .await
            .unwrap();

        setup_backup_file(
            object_store_manager.default_object_store().clone(),
            "/metadata_backup/metakv.metadata.fb",
        )
        .await;
        let kv_backend = Arc::new(MemoryKvBackend::default());
        restore_metadata_from_snapshot(
            &(kv_backend.clone() as _),
            &object_store_manager,
            "/metadata_backup/metakv.metadata.fb",
            false,
        )
        .await
        .unwrap();
        let value = kv_backend.get(b"test").await.unwrap().unwrap();
        assert_eq!(value.value, b"test");
    }

    #[tokio::test]
    async fn test_restore_metadata_from_snapshot_with_existing_data() {
        common_telemetry::init_default_ut_logging();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage_config = StorageConfig {
            data_home: format!("{}/home", temp_dir.path().display()),
            store: ObjectStoreConfig::File(FileConfig {}),
            providers: vec![],
        };
        let object_store_manager = DatanodeBuilder::build_object_store_manager(&storage_config)
            .await
            .unwrap();

        setup_backup_file(
            object_store_manager.default_object_store().clone(),
            "/metadata_backup/metakv.metadata.fb",
        )
        .await;
        let kv_backend = Arc::new(MemoryKvBackend::default());
        kv_backend
            .put(
                PutRequest::new()
                    .with_key(b"test".to_vec())
                    .with_value(b"foo".to_vec()),
            )
            .await
            .unwrap();

        restore_metadata_from_snapshot(
            &(kv_backend.clone() as _),
            &object_store_manager,
            "/metadata_backup/metakv.metadata.fb",
            false,
        )
        .await
        .unwrap();
        let value = kv_backend.get(b"test").await.unwrap().unwrap();
        assert_eq!(value.value, b"foo");
    }

    #[tokio::test]
    async fn test_restore_metadata_from_snapshot_ignore_error() {
        common_telemetry::init_default_ut_logging();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage_config = StorageConfig {
            data_home: format!("{}/home", temp_dir.path().display()),
            store: ObjectStoreConfig::File(FileConfig {}),
            providers: vec![],
        };
        let object_store_manager = DatanodeBuilder::build_object_store_manager(&storage_config)
            .await
            .unwrap();

        // Should fail to restore metadata from snapshot without ignore error
        let kv_backend = Arc::new(MemoryKvBackend::default());
        restore_metadata_from_snapshot(
            &(kv_backend.clone() as _),
            &object_store_manager,
            "/metadata_backup/metakv.metadata.fb",
            false,
        )
        .await
        .unwrap_err();

        // Restore metadata from snapshot with ignore error
        let kv_backend = Arc::new(MemoryKvBackend::default());
        restore_metadata_from_snapshot(
            &(kv_backend.clone() as _),
            &object_store_manager,
            "/metadata_backup/metakv.metadata.fb",
            true,
        )
        .await
        .unwrap();
    }
}
