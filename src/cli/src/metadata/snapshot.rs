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
use common_base::secrets::{ExposeSecret, SecretString};
use common_error::ext::BoxedError;
use common_meta::snapshot::MetadataSnapshotManager;
use object_store::services::{Fs, S3};
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};

use crate::error::{InvalidFilePathSnafu, OpenDalSnafu, S3ConfigNotSetSnafu};
use crate::metadata::common::StoreConfig;
use crate::Tool;

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

// TODO(qtang): Abstract a generic s3 config for export import meta snapshot restore
#[derive(Debug, Default, Parser)]
struct S3Config {
    /// whether to use s3 as the output directory. default is false.
    #[clap(long, default_value = "false")]
    s3: bool,
    /// The s3 bucket name.
    #[clap(long)]
    s3_bucket: Option<String>,
    /// The s3 region.
    #[clap(long)]
    s3_region: Option<String>,
    /// The s3 access key.
    #[clap(long)]
    s3_access_key: Option<SecretString>,
    /// The s3 secret key.
    #[clap(long)]
    s3_secret_key: Option<SecretString>,
    /// The s3 endpoint. we will automatically use the default s3 decided by the region if not set.
    #[clap(long)]
    s3_endpoint: Option<String>,
}

impl S3Config {
    pub fn build(&self, root: &str) -> Result<Option<ObjectStore>, BoxedError> {
        if !self.s3 {
            Ok(None)
        } else {
            if self.s3_region.is_none()
                || self.s3_access_key.is_none()
                || self.s3_secret_key.is_none()
                || self.s3_bucket.is_none()
            {
                return S3ConfigNotSetSnafu.fail().map_err(BoxedError::new);
            }
            // Safety, unwrap is safe because we have checked the options above.
            let mut config = S3::default()
                .bucket(self.s3_bucket.as_ref().unwrap())
                .region(self.s3_region.as_ref().unwrap())
                .access_key_id(self.s3_access_key.as_ref().unwrap().expose_secret())
                .secret_access_key(self.s3_secret_key.as_ref().unwrap().expose_secret());

            if !root.is_empty() && root != "." {
                config = config.root(root);
            }

            if let Some(endpoint) = &self.s3_endpoint {
                config = config.endpoint(endpoint);
            }
            Ok(Some(
                ObjectStore::new(config)
                    .context(OpenDalSnafu)
                    .map_err(BoxedError::new)?
                    .finish(),
            ))
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
    /// The s3 config.
    #[clap(flatten)]
    s3_config: S3Config,
    /// The name of the target snapshot file. we will add the file extension automatically.
    #[clap(long, default_value = "metadata_snapshot")]
    file_name: String,
    /// The directory to store the snapshot file.
    /// if target output is s3 bucket, this is the root directory in the bucket.
    /// if target output is local file, this is the local directory.
    #[clap(long, default_value = "")]
    output_dir: String,
}

fn create_local_file_object_store(root: &str) -> Result<ObjectStore, BoxedError> {
    let root = if root.is_empty() { "." } else { root };
    let object_store = ObjectStore::new(Fs::default().root(root))
        .context(OpenDalSnafu)
        .map_err(BoxedError::new)?
        .finish();
    Ok(object_store)
}

impl SaveCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kvbackend = self.store.build().await?;
        let output_dir = &self.output_dir;
        let object_store = self.s3_config.build(output_dir).map_err(BoxedError::new)?;
        if let Some(store) = object_store {
            let tool = MetaSnapshotTool {
                inner: MetadataSnapshotManager::new(kvbackend, store),
                target_file: self.file_name.clone(),
            };
            Ok(Box::new(tool))
        } else {
            let object_store = create_local_file_object_store(output_dir)?;
            let tool = MetaSnapshotTool {
                inner: MetadataSnapshotManager::new(kvbackend, object_store),
                target_file: self.file_name.clone(),
            };
            Ok(Box::new(tool))
        }
    }
}

struct MetaSnapshotTool {
    inner: MetadataSnapshotManager,
    target_file: String,
}

#[async_trait]
impl Tool for MetaSnapshotTool {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.inner
            .dump("", &self.target_file)
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
    /// The s3 config.
    #[clap(flatten)]
    s3_config: S3Config,
    /// The name of the target snapshot file.
    #[clap(long, default_value = "metadata_snapshot.metadata.fb")]
    file_name: String,
    /// The directory to store the snapshot file.
    #[clap(long, default_value = ".")]
    input_dir: String,
    #[clap(long, default_value = "false")]
    force: bool,
}

impl RestoreCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kvbackend = self.store.build().await?;
        let input_dir = &self.input_dir;
        let object_store = self.s3_config.build(input_dir).map_err(BoxedError::new)?;
        if let Some(store) = object_store {
            let tool = MetaRestoreTool::new(
                MetadataSnapshotManager::new(kvbackend, store),
                self.file_name.clone(),
                self.force,
            );
            Ok(Box::new(tool))
        } else {
            let object_store = create_local_file_object_store(input_dir)?;
            let tool = MetaRestoreTool::new(
                MetadataSnapshotManager::new(kvbackend, object_store),
                self.file_name.clone(),
                self.force,
            );
            Ok(Box::new(tool))
        }
    }
}

struct MetaRestoreTool {
    inner: MetadataSnapshotManager,
    source_file: String,
    force: bool,
}

impl MetaRestoreTool {
    pub fn new(inner: MetadataSnapshotManager, source_file: String, force: bool) -> Self {
        Self {
            inner,
            source_file,
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
                .restore(&self.source_file)
                .await
                .map_err(BoxedError::new)?;
            Ok(())
        } else if !self.force {
            common_telemetry::warn!(
                 "The target source is not clean, if you want to restore the metadata snapshot forcefully, please use --force option."
             );
            Ok(())
        } else {
            common_telemetry::info!("The target source is not clean, We will restore the metadata snapshot with --force.");
            self.inner
                .restore(&self.source_file)
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
    /// The s3 config.
    #[clap(flatten)]
    s3_config: S3Config,
    /// The name of the target snapshot file. we will add the file extension automatically.
    #[clap(long, default_value = "metadata_snapshot")]
    file_name: String,
    /// The query string to filter the metadata.
    #[clap(long, default_value = "*")]
    inspect_key: String,
    /// The limit of the metadata to query.
    #[clap(long)]
    limit: Option<usize>,
}

struct MetaInfoTool {
    inner: ObjectStore,
    source_file: String,
    inspect_key: String,
    limit: Option<usize>,
}

#[async_trait]
impl Tool for MetaInfoTool {
    #[allow(clippy::print_stdout)]
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        let result = MetadataSnapshotManager::info(
            &self.inner,
            &self.source_file,
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
    fn decide_object_store_root_for_local_store(
        file_path: &str,
    ) -> Result<(&str, &str), BoxedError> {
        let path = Path::new(file_path);
        let parent = path
            .parent()
            .and_then(|p| p.to_str())
            .context(InvalidFilePathSnafu { msg: file_path })
            .map_err(BoxedError::new)?;
        let file_name = path
            .file_name()
            .and_then(|f| f.to_str())
            .context(InvalidFilePathSnafu { msg: file_path })
            .map_err(BoxedError::new)?;
        let root = if parent.is_empty() { "." } else { parent };
        Ok((root, file_name))
    }

    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let object_store = self.s3_config.build("").map_err(BoxedError::new)?;
        if let Some(store) = object_store {
            let tool = MetaInfoTool {
                inner: store,
                source_file: self.file_name.clone(),
                inspect_key: self.inspect_key.clone(),
                limit: self.limit,
            };
            Ok(Box::new(tool))
        } else {
            let (root, file_name) =
                Self::decide_object_store_root_for_local_store(&self.file_name)?;
            let object_store = create_local_file_object_store(root)?;
            let tool = MetaInfoTool {
                inner: object_store,
                source_file: file_name.to_string(),
                inspect_key: self.inspect_key.clone(),
                limit: self.limit,
            };
            Ok(Box::new(tool))
        }
    }
}
