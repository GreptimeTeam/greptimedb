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
use crate::common::{ObjectStoreConfig, StoreConfig};
use crate::error::UnexpectedSnafu;

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
            SnapshotCommand::Save(cmd) => cmd.build().await,
            SnapshotCommand::Restore(cmd) => cmd.build().await,
            SnapshotCommand::Info(cmd) => cmd.build().await,
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
    /// The name of the target snapshot file. we will add the file extension automatically.
    #[clap(long, default_value = "metadata_snapshot")]
    file_name: String,
    /// The directory to store the snapshot file.
    #[clap(long, default_value = "", alias = "output_dir")]
    dir: String,
}

impl SaveCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kvbackend = self.store.build().await?;
        let object_store = self.object_store.build().map_err(BoxedError::new)?;
        let tool = MetaSnapshotTool {
            inner: MetadataSnapshotManager::new(kvbackend, object_store),
            path: self.dir.clone(),
            file_name: self.file_name.clone(),
        };
        Ok(Box::new(tool))
    }
}

struct MetaSnapshotTool {
    inner: MetadataSnapshotManager,
    path: String,
    file_name: String,
}

#[async_trait]
impl Tool for MetaSnapshotTool {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.inner
            .dump(&self.path, &self.file_name)
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
    /// The name of the target snapshot file.
    #[clap(long, default_value = "metadata_snapshot.metadata.fb")]
    file_name: String,
    /// The directory to store the snapshot file.
    #[clap(long, default_value = ".", alias = "input_dir")]
    dir: String,
    #[clap(long, default_value = "false")]
    force: bool,
}

impl RestoreCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kvbackend = self.store.build().await?;
        let input_dir = &self.dir;
        let file_path = Path::new(input_dir).join(&self.file_name);
        let file_path = file_path
            .to_str()
            .context(UnexpectedSnafu {
                msg: format!(
                    "Invalid file path, input dir: {}, file name: {}",
                    input_dir, &self.file_name
                ),
            })
            .map_err(BoxedError::new)?;

        let object_store = self.object_store.build().map_err(BoxedError::new)?;
        let tool = MetaRestoreTool::new(
            MetadataSnapshotManager::new(kvbackend, object_store),
            file_path.to_string(),
            self.force,
        );
        Ok(Box::new(tool))
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
    /// The name of the target snapshot file. we will add the file extension automatically.
    #[clap(long, default_value = "metadata_snapshot")]
    file_name: String,
    /// The directory to store the snapshot file.
    #[clap(long, default_value = ".", alias = "input_dir")]
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
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let object_store = self.object_store.build().map_err(BoxedError::new)?;
        let file_path = Path::new(&self.dir).join(&self.file_name);
        let file_path = file_path
            .to_str()
            .context(UnexpectedSnafu {
                msg: format!(
                    "Invalid file path, input dir: {}, file name: {}",
                    &self.dir, &self.file_name
                ),
            })
            .map_err(BoxedError::new)?;
        let tool = MetaInfoTool {
            inner: object_store,
            file_path: file_path.to_string(),
            inspect_key: self.inspect_key.clone(),
            limit: self.limit,
        };
        Ok(Box::new(tool))
    }
}
