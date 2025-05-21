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

use async_trait::async_trait;
use clap::Parser;
use common_error::ext::BoxedError;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::rds::{MySqlStore, PgStore};
use common_meta::kv_backend::DEFAULT_META_TABLE_NAME;
use common_meta::snapshot::MetadataSnapshotManager;
use object_store::services::{Fs, S3};
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{KVBackendNotSetSnafu, OpenDalSnafu};
use crate::Tool;

/// Export metadata snapshot tool.
/// This tool is used to export metadata snapshot from etcd, pg or mysql.
/// It will dump the metadata snapshot to local file or s3 bucket.
/// The snapshot file will be in binary format.
/// The snapshot file will be in the format of `backup.bin`.
#[derive(Debug, Default, Parser)]
pub struct MetaSnapshotCommand {
    /// Etcd address, e.g. "127.0.0.1:2379"
    #[clap(long)]
    etcd_addr: Option<String>,
    /// Postgres address, e.g. "password=password dbname=postgres user=postgres host=localhost port=5432"
    #[clap(long)]
    pg_addr: Option<String>,
    /// MySQL address, e.g. "mysql://user:password@ip:port/dbname"
    #[clap(long)]
    mysql_addr: Option<String>,
    /// The table name to store metadata. available for pg and mysql.
    #[clap(long)]
    meta_table_name: Option<String>,
    #[clap(long, default_value = "128")]
    max_txn_ops: Option<usize>,
    /// The name of the target snapshot file.
    #[clap(long, default_value = "backup.bin")]
    file_name: Option<String>,
    /// The directory to store the snapshot file.
    #[clap(long, default_value = ".")]
    output_dir: Option<String>,
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
    s3_access_key: Option<String>,
    /// The s3 secret key.
    #[clap(long)]
    s3_secret_key: Option<String>,
    /// The s3 endpoint.
    #[clap(long)]
    s3_endpoint: Option<String>,
    /// The s3 root path.
    #[clap(long)]
    s3_root: Option<String>,
}

impl MetaSnapshotCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let max_txn_ops = self.max_txn_ops.unwrap_or(128);
        let table_name = self
            .meta_table_name
            .as_ref()
            .map(|n| n.as_str())
            .unwrap_or(DEFAULT_META_TABLE_NAME);
        let kvbackend = if let Some(etcd_addr) = &self.etcd_addr {
            let addr = etcd_addr.split(',').collect::<Vec<_>>();
            if addr.is_empty() {
                return KVBackendNotSetSnafu { backend: "etcd" }
                    .fail()
                    .map_err(BoxedError::new);
            }
            EtcdStore::with_endpoints(addr, max_txn_ops)
                .await
                .map_err(BoxedError::new)?
        } else if let Some(pg_addr) = &self.pg_addr {
            // Initialize PgStore
            // ...
            PgStore::with_url(pg_addr, table_name, max_txn_ops)
                .await
                .map_err(BoxedError::new)?
        } else if let Some(mysql_addr) = &self.mysql_addr {
            MySqlStore::with_url(mysql_addr, table_name, max_txn_ops)
                .await
                .map_err(BoxedError::new)?
        } else {
            return KVBackendNotSetSnafu { backend: "etcd" }
                .fail()
                .map_err(BoxedError::new);
        };

        let output_dir = self.output_dir.clone().unwrap_or_else(|| ".".to_string());
        let tool = if self.s3 {
            let operator = ObjectStore::new(S3::default().root(&output_dir))
                .context(OpenDalSnafu)
                .map_err(BoxedError::new)?
                .finish();
            Box::new(MetaSnapshotTool {
                inner: MetadataSnapshotManager::new(kvbackend, operator),
            })
        } else {
            let operator = ObjectStore::new(Fs::default().root(&output_dir))
                .context(OpenDalSnafu)
                .map_err(BoxedError::new)?
                .finish();
            Box::new(MetaSnapshotTool {
                inner: MetadataSnapshotManager::new(kvbackend, operator),
            })
        };
        Ok(tool)
    }
}

pub struct MetaSnapshotTool {
    inner: MetadataSnapshotManager,
}

#[async_trait]
impl Tool for MetaSnapshotTool {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.inner.dump("/", "").await.map_err(BoxedError::new)?;
        Ok(())
    }
}
