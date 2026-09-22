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

use std::path::Path;

use async_trait::async_trait;
use clap::{Parser, Subcommand};
use common_error::ext::BoxedError;
use common_meta::snapshot::MetadataSnapshotManager;
use object_store::ObjectStore;
use snafu::OptionExt;

use crate::Tool;
use crate::common::{ObjectStoreConfig, StoreConfig, new_fs_object_store};
use crate::error::InvalidFilePathSnafu;
use crate::utils::resolve_relative_path_with_current_dir;

/// Subcommand for metadata snapshot operations, including saving snapshots, restoring from snapshots, and viewing snapshot information.
#[derive(Subcommand)]
pub enum SnapshotCommand {
    /// Save a snapshot of the current metadata state to a specified location.
    Save(SaveCommand),
    /// Restore metadata from a snapshot.
    Restore(RestoreCommand),
    /// Explore metadata from a snapshot.
    Info(InfoCommand),
}

impl SnapshotCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        match self {
            SnapshotCommand::Save(cmd) => Ok(Box::new(cmd.build().await?)),
            SnapshotCommand::Restore(cmd) => Ok(Box::new(cmd.build().await?)),
            SnapshotCommand::Info(cmd) => Ok(Box::new(cmd.build().await?)),
        }
    }
}

/// Export metadata snapshot tool.
/// This tool is used to export metadata snapshot from etcd, pg or mysql.
/// It will dump the metadata snapshot to local file or s3 bucket.
/// The snapshot file will be in binary format.
#[derive(Debug, Default, Parser)]
pub struct SaveCommand {
    /// The store configuration.
    #[clap(flatten)]
    store: StoreConfig,
    /// The object store configuration.
    #[clap(flatten)]
    object_store: ObjectStoreConfig,
    /// The path of the target snapshot file.
    #[clap(
        long,
        default_value = "metadata_snapshot.metadata.fb",
        alias = "file_name"
    )]
    file_path: String,
    /// Specifies the root directory used for I/O operations.
    #[clap(long, default_value = "/", alias = "output_dir")]
    dir: String,
}

impl SaveCommand {
    async fn build(&self) -> Result<MetaSnapshotTool, BoxedError> {
        let kvbackend = self.store.build().await?;
        let (object_store, file_path) = build_object_store_and_resolve_file_path(
            self.object_store.clone(),
            &self.dir,
            &self.file_path,
        )?;
        let tool = MetaSnapshotTool {
            inner: MetadataSnapshotManager::new(kvbackend, object_store),
            file_path,
        };
        Ok(tool)
    }
}

struct MetaSnapshotTool {
    inner: MetadataSnapshotManager,
    file_path: String,
}

#[async_trait]
impl Tool for MetaSnapshotTool {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.inner
            .dump(&self.file_path)
            .await
            .map_err(BoxedError::new)?;
        Ok(())
    }
}

/// Restore metadata from a snapshot file.
///
/// This command restores the metadata state from a previously saved snapshot.
/// The snapshot can be loaded from either a local file system or an S3 bucket,
/// depending on the provided configuration.
#[derive(Debug, Default, Parser)]
pub struct RestoreCommand {
    /// The store configuration.
    #[clap(flatten)]
    store: StoreConfig,
    /// The object store config.
    #[clap(flatten)]
    object_store: ObjectStoreConfig,
    /// The path of the target snapshot file.
    #[clap(
        long,
        default_value = "metadata_snapshot.metadata.fb",
        alias = "file_name"
    )]
    file_path: String,
    /// Specifies the root directory used for I/O operations.
    #[clap(long, default_value = "/", alias = "input_dir")]
    dir: String,
    #[clap(long, default_value = "false")]
    force: bool,
}

impl RestoreCommand {
    async fn build(&self) -> Result<MetaRestoreTool, BoxedError> {
        let kvbackend = self.store.build().await?;
        let (object_store, file_path) = build_object_store_and_resolve_file_path(
            self.object_store.clone(),
            &self.dir,
            &self.file_path,
        )
        .map_err(BoxedError::new)?;
        let tool = MetaRestoreTool::new(
            MetadataSnapshotManager::new(kvbackend, object_store),
            file_path,
            self.force,
        );
        Ok(tool)
    }
}

struct MetaRestoreTool {
    inner: MetadataSnapshotManager,
    file_path: String,
    force: bool,
}

impl MetaRestoreTool {
    pub fn new(inner: MetadataSnapshotManager, file_path: String, force: bool) -> Self {
        Self {
            inner,
            file_path,
            force,
        }
    }
}

#[async_trait]
impl Tool for MetaRestoreTool {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        let clean = self
            .inner
            .check_target_source_clean()
            .await
            .map_err(BoxedError::new)?;
        if clean {
            common_telemetry::info!(
                "The target source is clean, we will restore the metadata snapshot."
            );
            self.inner
                .restore(&self.file_path)
                .await
                .map_err(BoxedError::new)?;
            Ok(())
        } else if !self.force {
            common_telemetry::warn!(
                "The target source is not clean, if you want to restore the metadata snapshot forcefully, please use --force option."
            );
            Ok(())
        } else {
            common_telemetry::info!(
                "The target source is not clean, We will restore the metadata snapshot with --force."
            );
            self.inner
                .restore(&self.file_path)
                .await
                .map_err(BoxedError::new)?;
            Ok(())
        }
    }
}

/// Explore metadata from a snapshot file.
///
/// This command allows filtering the metadata by a specific key and limiting the number of results.
/// It prints the filtered metadata to the console.
#[derive(Debug, Default, Parser)]
pub struct InfoCommand {
    /// The object store config.
    #[clap(flatten)]
    object_store: ObjectStoreConfig,
    /// The path of the target snapshot file.
    #[clap(
        long,
        default_value = "metadata_snapshot.metadata.fb",
        alias = "file_name"
    )]
    file_path: String,
    /// Specifies the root directory used for I/O operations.
    #[clap(long, default_value = "/", alias = "input_dir")]
    dir: String,
    /// The query string to filter the metadata.
    #[clap(long, default_value = "*")]
    inspect_key: String,
    /// The limit of the metadata to query.
    #[clap(long)]
    limit: Option<usize>,
}

struct MetaInfoTool {
    inner: ObjectStore,
    file_path: String,
    inspect_key: String,
    limit: Option<usize>,
}

#[async_trait]
impl Tool for MetaInfoTool {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        let result = MetadataSnapshotManager::info(
            &self.inner,
            &self.file_path,
            &self.inspect_key,
            self.limit,
        )
        .await
        .map_err(BoxedError::new)?;
        for item in result {
            println!("{}", item);
        }
        Ok(())
    }
}

impl InfoCommand {
    async fn build(&self) -> Result<MetaInfoTool, BoxedError> {
        let (object_store, file_path) = build_object_store_and_resolve_file_path(
            self.object_store.clone(),
            &self.dir,
            &self.file_path,
        )?;
        let tool = MetaInfoTool {
            inner: object_store,
            file_path,
            inspect_key: self.inspect_key.clone(),
            limit: self.limit,
        };
        Ok(tool)
    }
}

/// Builds the object store and resolves the file path.
fn build_object_store_and_resolve_file_path(
    object_store: ObjectStoreConfig,
    fs_root: &str,
    file_path: &str,
) -> Result<(ObjectStore, String), BoxedError> {
    if let Some(object_store) = object_store.build().map_err(BoxedError::new)? {
        return Ok((object_store, file_path.to_string()));
    }

    if fs_root != "/" {
        let root = resolve_relative_path_with_current_dir(fs_root).map_err(BoxedError::new)?;
        let path = Path::new(file_path);
        let key = if path.is_absolute() {
            path.strip_prefix(&root).map_err(|_| {
                BoxedError::new(
                    InvalidFilePathSnafu {
                        msg: format!("Snapshot path is outside root {root}: {file_path}"),
                    }
                    .build(),
                )
            })?
        } else {
            path
        };
        let key = key.to_string_lossy().into_owned();
        #[cfg(windows)]
        let key = key.replace('\\', "/");
        return Ok((new_fs_object_store(&root)?, key));
    }

    let path = resolve_relative_path_with_current_dir(file_path).map_err(BoxedError::new)?;
    let path = Path::new(&path);
    let parent = path
        .parent()
        .with_context(|| InvalidFilePathSnafu {
            msg: format!("Snapshot path has no parent: {}", path.display()),
        })
        .map_err(BoxedError::new)?;
    let file_name = path
        .file_name()
        .with_context(|| InvalidFilePathSnafu {
            msg: format!("Snapshot path has no file name: {}", path.display()),
        })
        .map_err(BoxedError::new)?;
    let object_store = new_fs_object_store(&parent.to_string_lossy())?;
    Ok((object_store, file_name.to_string_lossy().into_owned()))
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use clap::Parser;
    use common_meta::kv_backend::KvBackend;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::rpc::store::PutRequest;
    use object_store::ObjectStore;

    use super::*;
    use crate::metadata::snapshot::RestoreCommand;

    fn create_raftengine_url(path: &std::path::Path) -> String {
        let mut path = path.to_string_lossy().replace('\\', "/");
        if !path.starts_with('/') {
            path = format!("/{}", path);
        }
        format!("raftengine://{}", path)
    }

    #[tokio::test]
    async fn test_cmd_resolve_file_path() {
        let name = "metadata_snapshot.metadata.fb";
        let current_dir = std::env::current_dir().unwrap();
        let absolute = current_dir.join(name);
        for file_path in [name, absolute.to_str().unwrap()] {
            let cmd = RestoreCommand::parse_from([
                "",
                "--file_name",
                file_path,
                "--backend",
                "memory-store",
                "--store-addrs",
                "memory://",
            ]);
            let tool = cmd.build().await.unwrap();
            assert_eq!(tool.file_path, name);
        }

        let dir = tempfile::tempdir().unwrap();
        for file_path in [
            "backup/snapshot.fb".to_string(),
            dir.path()
                .join("backup/snapshot.fb")
                .to_string_lossy()
                .into_owned(),
        ] {
            let (store, key) = build_object_store_and_resolve_file_path(
                ObjectStoreConfig::default(),
                dir.path().to_str().unwrap(),
                &file_path,
            )
            .unwrap();
            assert_eq!(key, "backup/snapshot.fb");
            store.write(&key, "snapshot").await.unwrap();
            assert_eq!(
                std::fs::read(dir.path().join("backup/snapshot.fb")).unwrap(),
                b"snapshot"
            );
        }
        assert!(
            build_object_store_and_resolve_file_path(
                ObjectStoreConfig::default(),
                dir.path().to_str().unwrap(),
                absolute.to_str().unwrap(),
            )
            .is_err()
        );
    }

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
    async fn test_restore_raft_engine_store() {
        common_telemetry::init_default_ut_logging();
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path().display().to_string();
        let object_store = new_fs_object_store(&root).unwrap();
        setup_backup_file(object_store, "/backup/metadata_snapshot.metadata.fb").await;
        let metadata_path = temp_dir.path().join("metadata");
        {
            let cmd = RestoreCommand::parse_from([
                "",
                "--file_name",
                temp_dir
                    .path()
                    .join("backup")
                    .join("metadata_snapshot.metadata.fb")
                    .to_str()
                    .unwrap(),
                "--backend",
                "raft-engine-store",
                "--store-addrs",
                &create_raftengine_url(&metadata_path),
            ]);
            let tool = cmd.build().await.unwrap();
            tool.do_work().await.unwrap();
        }
        // Waits for the raft engine release the file lock.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let kv = standalone::build_metadata_kvbackend(
            metadata_path.display().to_string(),
            Default::default(),
        )
        .unwrap();

        let value = kv.get(b"test").await.unwrap().unwrap().value;
        assert_eq!(value, b"test");
    }

    #[tokio::test]
    async fn test_save_raft_engine_store() {
        common_telemetry::init_default_ut_logging();
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path().display().to_string();
        let metadata_path = temp_dir.path().join("metadata");
        {
            let kv = standalone::build_metadata_kvbackend(
                metadata_path.to_string_lossy().to_string(),
                Default::default(),
            )
            .unwrap();
            kv.put(
                PutRequest::new()
                    .with_key(b"test".to_vec())
                    .with_value(b"test".to_vec()),
            )
            .await
            .unwrap();
        }
        // Waits for the raft engine release the file lock.
        tokio::time::sleep(Duration::from_secs(1)).await;
        {
            let cmd = SaveCommand::parse_from([
                "",
                "--file_name",
                temp_dir
                    .path()
                    .join("backup")
                    .join("metadata_snapshot.metadata.fb")
                    .to_str()
                    .unwrap(),
                "--backend",
                "raft-engine-store",
                "--store-addrs",
                &create_raftengine_url(&metadata_path),
            ]);
            let tool = cmd.build().await.unwrap();
            tool.do_work().await.unwrap();
        }

        // Reads the snapshot file from the object store.
        let object_store = new_fs_object_store(&root).unwrap();
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = MetadataSnapshotManager::new(kv_backend.clone(), object_store);
        manager
            .restore("/backup/metadata_snapshot.metadata.fb")
            .await
            .unwrap();
        let value = kv_backend.get(b"test").await.unwrap().unwrap().value;
        assert_eq!(value, b"test");
    }

    #[test]
    fn test_path() {
        let path = "C:\\Users\\user\\AppData\\Local\\Temp\\.tmpuPiVuB\\metadata";
        let path = Path::new(path);
        let url = create_raftengine_url(path);
        assert_eq!(
            url,
            "raftengine:///C:/Users/user/AppData/Local/Temp/.tmpuPiVuB/metadata"
        );
        let url = url::Url::parse(&url).unwrap();
        assert_eq!(
            url.path(),
            "/C:/Users/user/AppData/Local/Temp/.tmpuPiVuB/metadata"
        );
    }
}
