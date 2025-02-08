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

use std::marker::PhantomData;
use std::sync::Arc;

use deadpool_postgres::{Config, Pool, Runtime};
use snafu::ResultExt;
use tokio_postgres::types::ToSql;
use tokio_postgres::{IsolationLevel, NoTls, Row};

use crate::error::{
    CreatePostgresPoolSnafu, GetPostgresConnectionSnafu, PostgresExecutionSnafu,
    PostgresTransactionSnafu, Result,
};
use crate::kv_backend::rds::{
    DefaultQueryExecutor, ExecutorFactory, RdsStore, SqlTemplateFactory, TxnQueryExecutor,
    RDS_STORE_TXN_RETRY_COUNT,
};
use crate::kv_backend::KvBackendRef;
use crate::rpc::KeyValue;

type PgClient = deadpool::managed::Object<deadpool_postgres::Manager>;
type TxnPgClient<'a> = deadpool_postgres::Transaction<'a>;

/// Converts a row to a [`KeyValue`].
fn key_value_from_row(r: Row) -> KeyValue {
    KeyValue {
        key: r.get(0),
        value: r.get(1),
    }
}

#[async_trait::async_trait]
impl DefaultQueryExecutor for PgClient {
    type TxnExecutor<'a>
        = TxnPgClient<'a>
    where
        Self: 'a;

    fn name() -> &'static str {
        "Postgres"
    }

    async fn default_query(&self, query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>> {
        let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as _).collect();
        let rows = self
            .query(query, &params)
            .await
            .context(PostgresExecutionSnafu { sql: query })?;
        Ok(rows.into_iter().map(key_value_from_row).collect())
    }

    async fn txn_executor<'a>(&'a mut self) -> Result<Self::TxnExecutor<'a>> {
        let txn = self
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .context(PostgresTransactionSnafu {
                operation: "start".to_string(),
            })?;
        Ok(txn)
    }
}

#[async_trait::async_trait]
impl<'a> TxnQueryExecutor<'a> for TxnPgClient<'a> {
    async fn txn_query(&self, query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>> {
        let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as _).collect();
        let rows = self
            .query(query, &params)
            .await
            .context(PostgresExecutionSnafu { sql: query })?;
        Ok(rows.into_iter().map(key_value_from_row).collect())
    }

    async fn txn_commit(self) -> Result<()> {
        self.commit().await.context(PostgresTransactionSnafu {
            operation: "commit",
        })?;
        Ok(())
    }
}

pub struct PgExecutorFactory {
    pool: Pool,
}

impl PgExecutorFactory {
    async fn client(&self) -> Result<PgClient> {
        match self.pool.get().await {
            Ok(client) => Ok(client),
            Err(e) => GetPostgresConnectionSnafu {
                reason: e.to_string(),
            }
            .fail(),
        }
    }
}

#[async_trait::async_trait]
impl ExecutorFactory<PgClient> for PgExecutorFactory {
    async fn default_executor(&self) -> Result<PgClient> {
        self.client().await
    }

    async fn txn_executor<'a>(
        &self,
        default_executor: &'a mut PgClient,
    ) -> Result<TxnPgClient<'a>> {
        default_executor.txn_executor().await
    }
}

/// A PostgreSQL-backed key-value store for metasrv.
/// It uses deadpool-postgres as the connection pool for RdsStore.
pub type PgStore = RdsStore<PgClient, PgExecutorFactory>;

impl PgStore {
    /// Create pgstore impl of KvBackendRef from url.
    pub async fn with_url(url: &str, table_name: &str, max_txn_ops: usize) -> Result<KvBackendRef> {
        let mut cfg = Config::new();
        cfg.url = Some(url.to_string());
        // TODO(weny, CookiePie): add tls support
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(CreatePostgresPoolSnafu)?;
        Self::with_pg_pool(pool, table_name, max_txn_ops).await
    }

    /// Create pgstore impl of KvBackendRef from tokio-postgres client.
    pub async fn with_pg_pool(
        pool: Pool,
        table_name: &str,
        max_txn_ops: usize,
    ) -> Result<KvBackendRef> {
        // This step ensures the postgres metadata backend is ready to use.
        // We check if greptime_metakv table exists, and we will create a new table
        // if it does not exist.
        let client = match pool.get().await {
            Ok(client) => client,
            Err(e) => {
                return GetPostgresConnectionSnafu {
                    reason: e.to_string(),
                }
                .fail();
            }
        };
        let template_factory = SqlTemplateFactory::new(table_name);
        let sql_template_set = template_factory.build();
        client
            .execute(&sql_template_set.create_table_statement, &[])
            .await
            .with_context(|_| PostgresExecutionSnafu {
                sql: sql_template_set.create_table_statement.to_string(),
            })?;
        Ok(Arc::new(Self {
            max_txn_ops,
            sql_template_set,
            txn_retry_count: RDS_STORE_TXN_RETRY_COUNT,
            executor_factory: PgExecutorFactory { pool },
            _phantom: PhantomData,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_backend::test::{
        prepare_kv_with_prefix, test_kv_batch_delete_with_prefix, test_kv_batch_get_with_prefix,
        test_kv_compare_and_put_with_prefix, test_kv_delete_range_with_prefix,
        test_kv_put_with_prefix, test_kv_range_2_with_prefix, test_kv_range_with_prefix,
        test_txn_compare_equal, test_txn_compare_greater, test_txn_compare_less,
        test_txn_compare_not_equal, test_txn_one_compare_op, text_txn_multi_compare_op,
        unprepare_kv,
    };

    async fn build_pg_kv_backend(table_name: &str) -> Option<PgStore> {
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap_or_default();
        if endpoints.is_empty() {
            return None;
        }

        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(CreatePostgresPoolSnafu)
            .unwrap();
        let client = pool.get().await.unwrap();
        let template_factory = SqlTemplateFactory::new(table_name);
        let sql_templates = template_factory.build();
        client
            .execute(&sql_templates.create_table_statement, &[])
            .await
            .context(PostgresExecutionSnafu {
                sql: sql_templates.create_table_statement.to_string(),
            })
            .unwrap();
        Some(PgStore {
            max_txn_ops: 128,
            sql_template_set: sql_templates,
            txn_retry_count: RDS_STORE_TXN_RETRY_COUNT,
            executor_factory: PgExecutorFactory { pool },
            _phantom: PhantomData,
        })
    }

    #[tokio::test]
    async fn test_pg_put() {
        if let Some(kv_backend) = build_pg_kv_backend("put_test").await {
            let prefix = b"put/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_put_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_pg_range() {
        if let Some(kv_backend) = build_pg_kv_backend("range_test").await {
            let prefix = b"range/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_range_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_pg_range_2() {
        if let Some(kv_backend) = build_pg_kv_backend("range2_test").await {
            let prefix = b"range2/";
            test_kv_range_2_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_pg_batch_get() {
        if let Some(kv_backend) = build_pg_kv_backend("batch_get_test").await {
            let prefix = b"batch_get/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_batch_get_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_pg_batch_delete() {
        if let Some(kv_backend) = build_pg_kv_backend("batch_delete_test").await {
            let prefix = b"batch_delete/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_delete_range_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_pg_batch_delete_with_prefix() {
        if let Some(kv_backend) = build_pg_kv_backend("batch_delete_prefix_test").await {
            let prefix = b"batch_delete/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_batch_delete_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_pg_delete_range() {
        if let Some(kv_backend) = build_pg_kv_backend("delete_range_test").await {
            let prefix = b"delete_range/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_delete_range_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_pg_compare_and_put() {
        if let Some(kv_backend) = build_pg_kv_backend("compare_and_put_test").await {
            let prefix = b"compare_and_put/";
            let kv_backend = Arc::new(kv_backend);
            test_kv_compare_and_put_with_prefix(kv_backend.clone(), prefix.to_vec()).await;
        }
    }

    #[tokio::test]
    async fn test_pg_txn() {
        if let Some(kv_backend) = build_pg_kv_backend("txn_test").await {
            test_txn_one_compare_op(&kv_backend).await;
            text_txn_multi_compare_op(&kv_backend).await;
            test_txn_compare_equal(&kv_backend).await;
            test_txn_compare_greater(&kv_backend).await;
            test_txn_compare_less(&kv_backend).await;
            test_txn_compare_not_equal(&kv_backend).await;
        }
    }
}
