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

use clap::Parser;
use common_error::ext::BoxedError;
use common_meta::kv_backend::chroot::ChrootKvBackend;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::KvBackendRef;
use meta_srv::bootstrap::create_etcd_client;
use meta_srv::metasrv::{BackendClientOptions, BackendImpl};

use crate::error::{EmptyStoreAddrsSnafu, UnsupportedMemoryBackendSnafu};

#[derive(Debug, Default, Parser)]
pub(crate) struct StoreConfig {
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
    store_addrs: Vec<String>,

    /// The maximum number of operations in a transaction. Only used when using [etcd-store].
    #[clap(long, default_value = "128")]
    max_txn_ops: usize,

    /// The metadata store backend.
    #[clap(long, value_enum, default_value = "etcd-store")]
    backend: BackendImpl,

    /// The key prefix of the metadata store.
    #[clap(long, default_value = "")]
    store_key_prefix: String,

    /// The table name in RDS to store metadata. Only used when using [postgres-store] or [mysql-store].
    #[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
    #[clap(long, default_value = common_meta::kv_backend::DEFAULT_META_TABLE_NAME)]
    meta_table_name: String,
}

impl StoreConfig {
    /// Builds a [`KvBackendRef`] from the store configuration.
    pub async fn build(&self) -> Result<KvBackendRef, BoxedError> {
        let max_txn_ops = self.max_txn_ops;
        let store_addrs = &self.store_addrs;
        if store_addrs.is_empty() {
            EmptyStoreAddrsSnafu.fail().map_err(BoxedError::new)
        } else {
            let kvbackend = match self.backend {
                BackendImpl::EtcdStore => {
                    let etcd_client =
                        create_etcd_client(store_addrs, &BackendClientOptions::default())
                            .await
                            .map_err(BoxedError::new)?;
                    Ok(EtcdStore::with_etcd_client(etcd_client, max_txn_ops))
                }
                #[cfg(feature = "pg_kvbackend")]
                BackendImpl::PostgresStore => {
                    let table_name = &self.meta_table_name;
                    let pool = meta_srv::bootstrap::create_postgres_pool(store_addrs, None)
                        .await
                        .map_err(BoxedError::new)?;
                    Ok(common_meta::kv_backend::rds::PgStore::with_pg_pool(
                        pool,
                        table_name,
                        max_txn_ops,
                    )
                    .await
                    .map_err(BoxedError::new)?)
                }
                #[cfg(feature = "mysql_kvbackend")]
                BackendImpl::MysqlStore => {
                    let table_name = &self.meta_table_name;
                    let pool = meta_srv::bootstrap::create_mysql_pool(store_addrs)
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
                BackendImpl::MemoryStore => UnsupportedMemoryBackendSnafu
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
