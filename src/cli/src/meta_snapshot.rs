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

use async_trait::async_trait;
use clap::Parser;
use common_base::secrets::{ExposeSecret, SecretString};
use common_error::ext::BoxedError;
use common_meta::kv_backend::chroot::ChrootKvBackend;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::rds::{MySqlStore, PgStore};
use common_meta::kv_backend::{KvBackendRef, DEFAULT_META_TABLE_NAME};
use common_meta::snapshot::MetadataSnapshotManager;
use meta_srv::bootstrap::{create_etcd_client, create_mysql_pool, create_postgres_pool};
use meta_srv::metasrv::BackendImpl;
use object_store::services::{Fs, S3};
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{KvBackendNotSetSnafu, OpenDalSnafu, S3ConfigNotSetSnafu};
use crate::Tool;
#[derive(Debug, Default, Parser)]
struct MetaConnection {
    /// The endpoint of store. one of etcd, pg or mysql.
    #[clap(long, alias = "store-addr", value_delimiter = ',', num_args = 1..)]
    store_addrs: Vec<String>,
    /// The database backend.
    #[clap(long, value_enum)]
    backend: Option<BackendImpl>,
    #[clap(long, default_value = "")]
    store_key_prefix: String,
    #[clap(long,default_value = DEFAULT_META_TABLE_NAME)]
    meta_table_name: String,
    #[clap(long, default_value = "128")]
    max_txn_ops: usize,
}

impl MetaConnection {
    pub async fn build(&self) -> Result<KvBackendRef, BoxedError> {
        let max_txn_ops = self.max_txn_ops;
        let table_name = &self.meta_table_name;
        let store_addrs = &self.store_addrs;
        if store_addrs.is_empty() {
            KvBackendNotSetSnafu { backend: "all" }
                .fail()
                .map_err(BoxedError::new)
        } else {
            let kvbackend = match self.backend {
                Some(BackendImpl::EtcdStore) => {
                    let etcd_client = create_etcd_client(store_addrs)
                        .await
                        .map_err(BoxedError::new)?;
                    Ok(EtcdStore::with_etcd_client(etcd_client, max_txn_ops))
                }
                Some(BackendImpl::PostgresStore) => {
                    let pool = create_postgres_pool(store_addrs)
                        .await
                        .map_err(BoxedError::new)?;
                    Ok(PgStore::with_pg_pool(pool, table_name, max_txn_ops)
                        .await
                        .map_err(BoxedError::new)?)
                }
                Some(BackendImpl::MysqlStore) => {
                    let pool = create_mysql_pool(store_addrs)
                        .await
                        .map_err(BoxedError::new)?;
                    Ok(MySqlStore::with_mysql_pool(pool, table_name, max_txn_ops)
                        .await
                        .map_err(BoxedError::new)?)
                }
                _ => KvBackendNotSetSnafu { backend: "all" }
                    .fail()
                    .map_err(BoxedError::new),
            };
            if self.store_key_prefix.is_empty() {
                kvbackend
            } else {
                let chroot_kvbackend =
                    ChrootKvBackend::new(self.store_key_prefix.as_bytes().to_vec(), kvbackend?);
                Ok(Arc::new(chroot_kvbackend))
            }
        }
    }
}

#[derive(Debug, Default, Parser)]
struct S3Config {
    /// weather to use s3 as the output directory. default is false.
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
    /// The s3 endpoint.
    #[clap(long)]
    s3_endpoint: Option<String>,
    /// The s3 root path.
    #[clap(long)]
    s3_root: Option<String>,
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
                .secret_access_key(self.s3_secret_key.as_ref().unwrap().expose_secret())
                .root(root);

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
pub struct MetaSnapshotCommand {
    /// The connection to the metadata store.
    #[clap(flatten)]
    connection: MetaConnection,
    /// The s3 config.
    #[clap(flatten)]
    s3_config: S3Config,
    /// The name of the target snapshot file.
    #[clap(long, default_value = "metadata_snapshot")]
    file_name: String,
    /// The directory to store the snapshot file.
    #[clap(long, default_value = ".")]
    output_dir: String,
}

fn create_local_file_object_store(root: &str) -> Result<ObjectStore, BoxedError> {
    let object_store = ObjectStore::new(Fs::default().root(root))
        .context(OpenDalSnafu)
        .map_err(BoxedError::new)?
        .finish();
    Ok(object_store)
}

impl MetaSnapshotCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kvbackend = self.connection.build().await?;
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

pub struct MetaSnapshotTool {
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

/// Restore metadata snapshot tool.
/// This tool is used to restore metadata snapshot from etcd, pg or mysql.
/// It will restore the metadata snapshot from local file or s3 bucket.
#[derive(Debug, Default, Parser)]
pub struct MetaRestoreCommand {
    /// The connection to the metadata store.
    #[clap(flatten)]
    connection: MetaConnection,
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

impl MetaRestoreCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kvbackend = self.connection.build().await?;
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

pub struct MetaRestoreTool {
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
        if !self.force {
            let clean = self
                .inner
                .check_target_srouce_clean()
                .await
                .map_err(BoxedError::new)?;
            if clean {
                common_telemetry::info!(
                    "The target source is clean, we will restore the metadata snapshot."
                );
            } else {
                common_telemetry::info!("The target source is not clean, restore will be skipped. you can use --force to force restore.");
                return Ok(());
            }
        }
        self.inner
            .restore(&self.source_file)
            .await
            .map_err(BoxedError::new)?;
        Ok(())
    }
}
