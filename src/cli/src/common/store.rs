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

use clap::{Parser, ValueEnum};
use common_error::ext::BoxedError;
use common_meta::kv_backend::KvBackendRef;
use common_meta::kv_backend::chroot::ChrootKvBackend;
use common_meta::kv_backend::etcd::EtcdStore;
use meta_srv::metasrv::BackendClientOptions;
use meta_srv::utils::etcd::create_etcd_client_with_tls;
use serde::{Deserialize, Serialize};
use servers::tls::{TlsMode, TlsOption};
use snafu::OptionExt;

use crate::error::{EmptyStoreAddrsSnafu, InvalidArgumentsSnafu};

// The datastores that implements metadata kvbackend.
#[derive(Clone, Debug, PartialEq, Serialize, Default, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum BackendImpl {
    // Etcd as metadata storage.
    #[default]
    EtcdStore,
    // In memory metadata storage - mostly used for testing.
    MemoryStore,
    #[cfg(feature = "pg_kvbackend")]
    // Postgres as metadata storage.
    PostgresStore,
    #[cfg(feature = "mysql_kvbackend")]
    // MySql as metadata storage.
    MysqlStore,
    // RaftEngine as metadata storage.
    RaftEngineStore,
}

#[derive(Debug, Default, Parser)]
pub struct StoreConfig {
    /// The endpoint of store. one of etcd, postgres or mysql.
    ///
    /// For postgres store, the format is:
    /// "password=password dbname=postgres user=postgres host=localhost port=5432"
    ///
    /// For etcd store, the format is:
    /// "127.0.0.1:2379"
    ///
    /// For mysql store, the format is:
    /// "mysql://user:password@ip:port/dbname"
    #[clap(long, alias = "store-addr", value_delimiter = ',', num_args = 1..)]
    pub store_addrs: Vec<String>,

    /// The maximum number of operations in a transaction. Only used when using [etcd-store].
    #[clap(long, default_value = "128")]
    pub max_txn_ops: usize,

    /// The metadata store backend.
    #[clap(long, value_enum, default_value = "etcd-store")]
    pub backend: BackendImpl,

    /// The key prefix of the metadata store.
    #[clap(long, default_value = "")]
    pub store_key_prefix: String,

    /// The table name in RDS to store metadata. Only used when using [postgres-store] or [mysql-store].
    #[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
    #[clap(long, default_value = common_meta::kv_backend::DEFAULT_META_TABLE_NAME)]
    pub meta_table_name: String,

    /// Optional PostgreSQL schema for metadata table (defaults to current search_path if unset).
    #[cfg(feature = "pg_kvbackend")]
    #[clap(long)]
    pub meta_schema_name: Option<String>,

    /// Automatically create PostgreSQL schema if it doesn't exist (default: true).
    #[cfg(feature = "pg_kvbackend")]
    #[clap(long, default_value_t = true)]
    pub auto_create_schema: bool,

    /// TLS mode for backend store connections (etcd, PostgreSQL, MySQL)
    #[clap(long = "backend-tls-mode", value_enum, default_value = "disable")]
    pub backend_tls_mode: TlsMode,

    /// Path to TLS certificate file for backend store connections
    #[clap(long = "backend-tls-cert-path", default_value = "")]
    pub backend_tls_cert_path: String,

    /// Path to TLS private key file for backend store connections
    #[clap(long = "backend-tls-key-path", default_value = "")]
    pub backend_tls_key_path: String,

    /// Path to TLS CA certificate file for backend store connections
    #[clap(long = "backend-tls-ca-cert-path", default_value = "")]
    pub backend_tls_ca_cert_path: String,

    /// Enable watching TLS certificate files for changes
    #[clap(long = "backend-tls-watch")]
    pub backend_tls_watch: bool,
}

impl StoreConfig {
    pub fn tls_config(&self) -> Option<TlsOption> {
        if self.backend_tls_mode != TlsMode::Disable {
            Some(TlsOption {
                mode: self.backend_tls_mode.clone(),
                cert_path: self.backend_tls_cert_path.clone(),
                key_path: self.backend_tls_key_path.clone(),
                ca_cert_path: self.backend_tls_ca_cert_path.clone(),
                watch: self.backend_tls_watch,
            })
        } else {
            None
        }
    }

    /// Builds a [`KvBackendRef`] from the store configuration.
    pub async fn build(&self) -> Result<KvBackendRef, BoxedError> {
        let max_txn_ops = self.max_txn_ops;
        let store_addrs = &self.store_addrs;
        if store_addrs.is_empty() {
            EmptyStoreAddrsSnafu.fail().map_err(BoxedError::new)
        } else {
            common_telemetry::info!(
                "Building kvbackend with store addrs: {:?}, backend: {:?}",
                store_addrs,
                self.backend
            );
            let kvbackend = match self.backend {
                BackendImpl::EtcdStore => {
                    let tls_config = self.tls_config();
                    let etcd_client = create_etcd_client_with_tls(
                        store_addrs,
                        &BackendClientOptions::default(),
                        tls_config.as_ref(),
                    )
                    .await
                    .map_err(BoxedError::new)?;
                    Ok(EtcdStore::with_etcd_client(etcd_client, max_txn_ops))
                }
                #[cfg(feature = "pg_kvbackend")]
                BackendImpl::PostgresStore => {
                    let table_name = &self.meta_table_name;
                    let tls_config = self.tls_config();
                    let pool = meta_srv::utils::postgres::create_postgres_pool(
                        store_addrs,
                        None,
                        tls_config,
                    )
                    .await
                    .map_err(BoxedError::new)?;
                    let schema_name = self.meta_schema_name.as_deref();
                    Ok(common_meta::kv_backend::rds::PgStore::with_pg_pool(
                        pool,
                        schema_name,
                        table_name,
                        max_txn_ops,
                        self.auto_create_schema,
                    )
                    .await
                    .map_err(BoxedError::new)?)
                }
                #[cfg(feature = "mysql_kvbackend")]
                BackendImpl::MysqlStore => {
                    let table_name = &self.meta_table_name;
                    let tls_config = self.tls_config();
                    let pool =
                        meta_srv::utils::mysql::create_mysql_pool(store_addrs, tls_config.as_ref())
                            .await
                            .map_err(BoxedError::new)?;
                    Ok(common_meta::kv_backend::rds::MySqlStore::with_mysql_pool(
                        pool,
                        table_name,
                        max_txn_ops,
                    )
                    .await
                    .map_err(BoxedError::new)?)
                }
                #[cfg(not(test))]
                BackendImpl::MemoryStore => {
                    use crate::error::UnsupportedMemoryBackendSnafu;

                    UnsupportedMemoryBackendSnafu
                        .fail()
                        .map_err(BoxedError::new)
                }
                #[cfg(test)]
                BackendImpl::MemoryStore => {
                    use common_meta::kv_backend::memory::MemoryKvBackend;

                    Ok(Arc::new(MemoryKvBackend::default()) as _)
                }
                BackendImpl::RaftEngineStore => {
                    let url = store_addrs
                        .first()
                        .context(InvalidArgumentsSnafu {
                            msg: "empty store addresses".to_string(),
                        })
                        .map_err(BoxedError::new)?;
                    let kvbackend =
                        standalone::build_metadata_kv_from_url(url).map_err(BoxedError::new)?;

                    Ok(kvbackend)
                }
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
