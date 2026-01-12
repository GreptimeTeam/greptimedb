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

use common_telemetry::debug;
use snafu::ResultExt;
use sqlx::mysql::MySqlRow;
use sqlx::pool::Pool;
use sqlx::{MySql, MySqlPool, Row, Transaction as MySqlTransaction};
use strum::AsRefStr;

use crate::error::{CreateMySqlPoolSnafu, MySqlExecutionSnafu, MySqlTransactionSnafu, Result};
use crate::kv_backend::rds::{
    Executor, ExecutorFactory, ExecutorImpl, KvQueryExecutor, RdsStore, Transaction,
    RDS_STORE_OP_BATCH_DELETE, RDS_STORE_OP_BATCH_GET, RDS_STORE_OP_BATCH_PUT,
    RDS_STORE_OP_RANGE_DELETE, RDS_STORE_OP_RANGE_QUERY, RDS_STORE_TXN_RETRY_COUNT,
};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

const MYSQL_STORE_NAME: &str = "mysql_store";

type MySqlClient = Arc<Pool<MySql>>;
pub struct MySqlTxnClient(MySqlTransaction<'static, MySql>);

fn key_value_from_row(row: MySqlRow) -> KeyValue {
    // Safety: key and value are the first two columns in the row
    KeyValue {
        key: row.get_unchecked(0),
        value: row.get_unchecked(1),
    }
}

const EMPTY: &[u8] = &[0];

/// Type of range template.
#[derive(Debug, Clone, Copy, AsRefStr)]
enum RangeTemplateType {
    Point,
    Range,
    Full,
    LeftBounded,
    Prefix,
}

/// Builds params for the given range template type.
impl RangeTemplateType {
    /// Builds the parameters for the given range template type.
    /// You can check out the conventions at [RangeRequest]
    fn build_params(&self, mut key: Vec<u8>, range_end: Vec<u8>) -> Vec<Vec<u8>> {
        match self {
            RangeTemplateType::Point => vec![key],
            RangeTemplateType::Range => vec![key, range_end],
            RangeTemplateType::Full => vec![],
            RangeTemplateType::LeftBounded => vec![key],
            RangeTemplateType::Prefix => {
                key.push(b'%');
                vec![key]
            }
        }
    }
}

/// Templates for range request.
#[derive(Debug, Clone)]
struct RangeTemplate {
    point: String,
    range: String,
    full: String,
    left_bounded: String,
    prefix: String,
}

impl RangeTemplate {
    /// Gets the template for the given type.
    fn get(&self, typ: RangeTemplateType) -> &str {
        match typ {
            RangeTemplateType::Point => &self.point,
            RangeTemplateType::Range => &self.range,
            RangeTemplateType::Full => &self.full,
            RangeTemplateType::LeftBounded => &self.left_bounded,
            RangeTemplateType::Prefix => &self.prefix,
        }
    }

    /// Adds limit to the template.
    fn with_limit(template: &str, limit: i64) -> String {
        if limit == 0 {
            return format!("{};", template);
        }
        format!("{} LIMIT {};", template, limit)
    }
}

fn is_prefix_range(start: &[u8], end: &[u8]) -> bool {
    if start.len() != end.len() {
        return false;
    }
    let l = start.len();
    let same_prefix = start[0..l - 1] == end[0..l - 1];
    if let (Some(rhs), Some(lhs)) = (start.last(), end.last()) {
        return same_prefix && (*rhs + 1) == *lhs;
    }
    false
}

/// Determine the template type for range request.
fn range_template(key: &[u8], range_end: &[u8]) -> RangeTemplateType {
    match (key, range_end) {
        (_, &[]) => RangeTemplateType::Point,
        (EMPTY, EMPTY) => RangeTemplateType::Full,
        (_, EMPTY) => RangeTemplateType::LeftBounded,
        (start, end) => {
            if is_prefix_range(start, end) {
                RangeTemplateType::Prefix
            } else {
                RangeTemplateType::Range
            }
        }
    }
}

/// Generate in placeholders for MySQL.
fn mysql_generate_in_placeholders(from: usize, to: usize) -> Vec<String> {
    (from..=to).map(|_| "?".to_string()).collect()
}

/// Factory for building sql templates.
struct MySqlTemplateFactory<'a> {
    table_name: &'a str,
}

impl<'a> MySqlTemplateFactory<'a> {
    /// Creates a new [`SqlTemplateFactory`] with the given table name.
    fn new(table_name: &'a str) -> Self {
        Self { table_name }
    }

    /// Builds the template set for the given table name.
    fn build(&self) -> MySqlTemplateSet {
        let table_name = self.table_name;
        // Some of queries don't end with `;`, because we need to add `LIMIT` clause.
        MySqlTemplateSet {
            table_name: table_name.to_string(),
            create_table_statement: format!(
                // Cannot be more than 3072 bytes in PRIMARY KEY
                "CREATE TABLE IF NOT EXISTS `{table_name}`(k VARBINARY(3072) PRIMARY KEY, v BLOB);",
            ),
            range_template: RangeTemplate {
                point: format!("SELECT k, v FROM `{table_name}` WHERE k = ?"),
                range: format!("SELECT k, v FROM `{table_name}` WHERE k >= ? AND k < ? ORDER BY k"),
                full: format!("SELECT k, v FROM `{table_name}` ORDER BY k"),
                left_bounded: format!("SELECT k, v FROM `{table_name}` WHERE k >= ? ORDER BY k"),
                prefix: format!("SELECT k, v FROM `{table_name}` WHERE k LIKE ? ORDER BY k"),
            },
            delete_template: RangeTemplate {
                point: format!("DELETE FROM `{table_name}` WHERE k = ?;"),
                range: format!("DELETE FROM `{table_name}` WHERE k >= ? AND k < ?;"),
                full: format!("DELETE FROM `{table_name}`"),
                left_bounded: format!("DELETE FROM `{table_name}` WHERE k >= ?;"),
                prefix: format!("DELETE FROM `{table_name}` WHERE k LIKE ?;"),
            },
        }
    }
}

/// Templates for the given table name.
#[derive(Debug, Clone)]
pub struct MySqlTemplateSet {
    table_name: String,
    create_table_statement: String,
    range_template: RangeTemplate,
    delete_template: RangeTemplate,
}

impl MySqlTemplateSet {
    /// Generates the sql for batch get.
    fn generate_batch_get_query(&self, key_len: usize) -> String {
        let table_name = &self.table_name;
        let in_clause = mysql_generate_in_placeholders(1, key_len).join(", ");
        format!(
            "SELECT k, v FROM `{table_name}` WHERE k in ({});",
            in_clause
        )
    }

    /// Generates the sql for batch delete.
    fn generate_batch_delete_query(&self, key_len: usize) -> String {
        let table_name = &self.table_name;
        let in_clause = mysql_generate_in_placeholders(1, key_len).join(", ");
        format!("DELETE FROM `{table_name}` WHERE k in ({});", in_clause)
    }

    /// Generates the sql for batch upsert.
    /// For MySQL, it also generates a select query to get the previous values.
    fn generate_batch_upsert_query(&self, kv_len: usize) -> (String, String) {
        let table_name = &self.table_name;
        let in_placeholders: Vec<String> = (1..=kv_len).map(|_| "?".to_string()).collect();
        let in_clause = in_placeholders.join(", ");
        let mut values_placeholders = Vec::new();
        for _ in 0..kv_len {
            values_placeholders.push("(?, ?)".to_string());
        }
        let values_clause = values_placeholders.join(", ");

        (
            format!(r#"SELECT k, v FROM `{table_name}` WHERE k IN ({in_clause})"#,),
            format!(
                r#"INSERT INTO `{table_name}` (k, v) VALUES {values_clause} ON DUPLICATE KEY UPDATE v = VALUES(v);"#,
            ),
        )
    }
}

#[async_trait::async_trait]
impl Executor for MySqlClient {
    type Transaction<'a>
        = MySqlTxnClient
    where
        Self: 'a;

    fn name() -> &'static str {
        "MySql"
    }

    async fn query(&mut self, raw_query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>> {
        let query = sqlx::query(raw_query);
        let query = params.iter().fold(query, |query, param| query.bind(param));
        let rows = query
            .fetch_all(&**self)
            .await
            .context(MySqlExecutionSnafu { sql: raw_query })?;
        Ok(rows.into_iter().map(key_value_from_row).collect())
    }

    async fn execute(&mut self, raw_query: &str, params: &[&Vec<u8>]) -> Result<()> {
        let query = sqlx::query(raw_query);
        let query = params.iter().fold(query, |query, param| query.bind(param));
        query
            .execute(&**self)
            .await
            .context(MySqlExecutionSnafu { sql: raw_query })?;
        Ok(())
    }

    async fn txn_executor<'a>(&'a mut self) -> Result<Self::Transaction<'a>> {
        // sqlx has no isolation level support for now, so we have to set it manually.
        // TODO(CookiePie): Waiting for https://github.com/launchbadge/sqlx/pull/3614 and remove this.
        sqlx::query("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&**self)
            .await
            .context(MySqlExecutionSnafu {
                sql: "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            })?;
        let txn = self
            .begin()
            .await
            .context(MySqlExecutionSnafu { sql: "begin" })?;
        Ok(MySqlTxnClient(txn))
    }
}

#[async_trait::async_trait]
impl Transaction<'_> for MySqlTxnClient {
    async fn query(&mut self, raw_query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>> {
        let query = sqlx::query(raw_query);
        let query = params.iter().fold(query, |query, param| query.bind(param));
        // As said in https://docs.rs/sqlx/latest/sqlx/trait.Executor.html, we need a `&mut *transaction`. Weird.
        let rows = query
            .fetch_all(&mut *(self.0))
            .await
            .context(MySqlExecutionSnafu { sql: raw_query })?;
        Ok(rows.into_iter().map(key_value_from_row).collect())
    }

    async fn execute(&mut self, raw_query: &str, params: &[&Vec<u8>]) -> Result<()> {
        let query = sqlx::query(raw_query);
        let query = params.iter().fold(query, |query, param| query.bind(param));
        // As said in https://docs.rs/sqlx/latest/sqlx/trait.Executor.html, we need a `&mut *transaction`. Weird.
        query
            .execute(&mut *(self.0))
            .await
            .context(MySqlExecutionSnafu { sql: raw_query })?;
        Ok(())
    }

    /// Caution: sqlx will stuck on the query if two transactions conflict with each other.
    /// Don't know if it's a feature or it depends on the database. Be careful.
    async fn commit(self) -> Result<()> {
        self.0.commit().await.context(MySqlTransactionSnafu {
            operation: "commit",
        })?;
        Ok(())
    }
}

pub struct MySqlExecutorFactory {
    pool: Arc<Pool<MySql>>,
}

#[async_trait::async_trait]
impl ExecutorFactory<MySqlClient> for MySqlExecutorFactory {
    async fn default_executor(&self) -> Result<MySqlClient> {
        Ok(self.pool.clone())
    }

    async fn txn_executor<'a>(
        &self,
        default_executor: &'a mut MySqlClient,
    ) -> Result<MySqlTxnClient> {
        default_executor.txn_executor().await
    }
}

/// A MySQL-backed key-value store.
/// It uses [sqlx::Pool<MySql>] as the connection pool for [RdsStore].
pub type MySqlStore = RdsStore<MySqlClient, MySqlExecutorFactory, MySqlTemplateSet>;

#[async_trait::async_trait]
impl KvQueryExecutor<MySqlClient> for MySqlStore {
    async fn range_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, MySqlClient>,
        req: RangeRequest,
    ) -> Result<RangeResponse> {
        let template_type = range_template(&req.key, &req.range_end);
        let template = self.sql_template_set.range_template.get(template_type);
        let params = template_type.build_params(req.key, req.range_end);
        let params_ref = params.iter().collect::<Vec<_>>();
        // Always add 1 to limit to check if there is more data
        let query =
            RangeTemplate::with_limit(template, if req.limit == 0 { 0 } else { req.limit + 1 });
        let limit = req.limit as usize;
        debug!("query: {:?}, params: {:?}", query, params);
        let mut kvs = crate::record_rds_sql_execute_elapsed!(
            query_executor.query(&query, &params_ref).await,
            MYSQL_STORE_NAME,
            RDS_STORE_OP_RANGE_QUERY,
            template_type.as_ref()
        )?;
        if req.keys_only {
            kvs.iter_mut().for_each(|kv| kv.value = vec![]);
        }
        // If limit is 0, we always return all data
        if limit == 0 || kvs.len() <= limit {
            return Ok(RangeResponse { kvs, more: false });
        }
        // If limit is greater than the number of rows, we remove the last row and set more to true
        let removed = kvs.pop();
        debug_assert!(removed.is_some());
        Ok(RangeResponse { kvs, more: true })
    }

    async fn batch_put_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, MySqlClient>,
        req: BatchPutRequest,
    ) -> Result<BatchPutResponse> {
        let mut in_params = Vec::with_capacity(req.kvs.len() * 3);
        let mut values_params = Vec::with_capacity(req.kvs.len() * 2);

        for kv in &req.kvs {
            let processed_key = &kv.key;
            in_params.push(processed_key);

            let processed_value = &kv.value;
            values_params.push(processed_key);
            values_params.push(processed_value);
        }
        let in_params = in_params.iter().map(|x| x as _).collect::<Vec<_>>();
        let values_params = values_params.iter().map(|x| x as _).collect::<Vec<_>>();
        let (select, update) = self
            .sql_template_set
            .generate_batch_upsert_query(req.kvs.len());

        // Fast path: if we don't need previous kvs, we can just upsert the keys.
        if !req.prev_kv {
            crate::record_rds_sql_execute_elapsed!(
                query_executor.execute(&update, &values_params).await,
                MYSQL_STORE_NAME,
                RDS_STORE_OP_BATCH_PUT,
                ""
            )?;
            return Ok(BatchPutResponse::default());
        }
        // Should use transaction to ensure atomicity.
        if let ExecutorImpl::Default(query_executor) = query_executor {
            let txn = query_executor.txn_executor().await?;
            let mut txn = ExecutorImpl::Txn(txn);
            let res = self.batch_put_with_query_executor(&mut txn, req).await;
            txn.commit().await?;
            return res;
        }
        let prev_kvs = crate::record_rds_sql_execute_elapsed!(
            query_executor.query(&select, &in_params).await,
            MYSQL_STORE_NAME,
            RDS_STORE_OP_BATCH_PUT,
            ""
        )?;
        query_executor.execute(&update, &values_params).await?;
        Ok(BatchPutResponse { prev_kvs })
    }

    async fn batch_get_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, MySqlClient>,
        req: BatchGetRequest,
    ) -> Result<BatchGetResponse> {
        if req.keys.is_empty() {
            return Ok(BatchGetResponse { kvs: vec![] });
        }
        let query = self
            .sql_template_set
            .generate_batch_get_query(req.keys.len());
        let params = req.keys.iter().map(|x| x as _).collect::<Vec<_>>();
        let kvs = crate::record_rds_sql_execute_elapsed!(
            query_executor.query(&query, &params).await,
            MYSQL_STORE_NAME,
            RDS_STORE_OP_BATCH_GET,
            ""
        )?;
        Ok(BatchGetResponse { kvs })
    }

    async fn delete_range_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, MySqlClient>,
        req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse> {
        // Since we need to know the number of deleted keys, we have no fast path here.
        // Should use transaction to ensure atomicity.
        if let ExecutorImpl::Default(query_executor) = query_executor {
            let txn = query_executor.txn_executor().await?;
            let mut txn = ExecutorImpl::Txn(txn);
            let res = self.delete_range_with_query_executor(&mut txn, req).await;
            txn.commit().await?;
            return res;
        }
        let range_get_req = RangeRequest {
            key: req.key.clone(),
            range_end: req.range_end.clone(),
            limit: 0,
            keys_only: false,
        };
        let prev_kvs = self
            .range_with_query_executor(query_executor, range_get_req)
            .await?
            .kvs;
        let template_type = range_template(&req.key, &req.range_end);
        let template = self.sql_template_set.delete_template.get(template_type);
        let params = template_type.build_params(req.key, req.range_end);
        let params_ref = params.iter().map(|x| x as _).collect::<Vec<_>>();
        crate::record_rds_sql_execute_elapsed!(
            query_executor.execute(template, &params_ref).await,
            MYSQL_STORE_NAME,
            RDS_STORE_OP_RANGE_DELETE,
            template_type.as_ref()
        )?;
        let mut resp = DeleteRangeResponse::new(prev_kvs.len() as i64);
        if req.prev_kv {
            resp.with_prev_kvs(prev_kvs);
        }
        Ok(resp)
    }

    async fn batch_delete_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, MySqlClient>,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse> {
        if req.keys.is_empty() {
            return Ok(BatchDeleteResponse::default());
        }
        let query = self
            .sql_template_set
            .generate_batch_delete_query(req.keys.len());
        let params = req.keys.iter().map(|x| x as _).collect::<Vec<_>>();
        // Fast path: if we don't need previous kvs, we can just delete the keys.
        if !req.prev_kv {
            crate::record_rds_sql_execute_elapsed!(
                query_executor.execute(&query, &params).await,
                MYSQL_STORE_NAME,
                RDS_STORE_OP_BATCH_DELETE,
                ""
            )?;
            return Ok(BatchDeleteResponse::default());
        }
        // Should use transaction to ensure atomicity.
        if let ExecutorImpl::Default(query_executor) = query_executor {
            let txn = query_executor.txn_executor().await?;
            let mut txn = ExecutorImpl::Txn(txn);
            let res = self.batch_delete_with_query_executor(&mut txn, req).await;
            txn.commit().await?;
            return res;
        }
        // Should get previous kvs first
        let batch_get_req = BatchGetRequest {
            keys: req.keys.clone(),
        };
        let prev_kvs = self
            .batch_get_with_query_executor(query_executor, batch_get_req)
            .await?
            .kvs;
        // Pure `DELETE` has no return value, so we need to use `execute` instead of `query`.
        crate::record_rds_sql_execute_elapsed!(
            query_executor.execute(&query, &params).await,
            MYSQL_STORE_NAME,
            RDS_STORE_OP_BATCH_DELETE,
            ""
        )?;
        if req.prev_kv {
            Ok(BatchDeleteResponse { prev_kvs })
        } else {
            Ok(BatchDeleteResponse::default())
        }
    }
}

impl MySqlStore {
    /// Create [MySqlStore] impl of [KvBackendRef] from url.
    pub async fn with_url(url: &str, table_name: &str, max_txn_ops: usize) -> Result<KvBackendRef> {
        let pool = MySqlPool::connect(url)
            .await
            .context(CreateMySqlPoolSnafu)?;
        Self::with_mysql_pool(pool, table_name, max_txn_ops).await
    }

    /// Create [MySqlStore] impl of [KvBackendRef] from [sqlx::Pool<MySql>].
    pub async fn with_mysql_pool(
        pool: Pool<MySql>,
        table_name: &str,
        max_txn_ops: usize,
    ) -> Result<KvBackendRef> {
        // This step ensures the mysql metadata backend is ready to use.
        // We check if greptime_metakv table exists, and we will create a new table
        // if it does not exist.
        let sql_template_set = MySqlTemplateFactory::new(table_name).build();
        sqlx::query(&sql_template_set.create_table_statement)
            .execute(&pool)
            .await
            .context(MySqlExecutionSnafu {
                sql: sql_template_set.create_table_statement.to_string(),
            })?;
        Ok(Arc::new(MySqlStore {
            max_txn_ops,
            sql_template_set,
            txn_retry_count: RDS_STORE_TXN_RETRY_COUNT,
            executor_factory: MySqlExecutorFactory {
                pool: Arc::new(pool),
            },
            _phantom: PhantomData,
        }))
    }
}

#[cfg(test)]
mod tests {
    use common_telemetry::init_default_ut_logging;

    use super::*;
    use crate::kv_backend::test::{
        prepare_kv_with_prefix, test_kv_batch_delete_with_prefix, test_kv_batch_get_with_prefix,
        test_kv_compare_and_put_with_prefix, test_kv_delete_range_with_prefix,
        test_kv_put_with_prefix, test_kv_range_2_with_prefix, test_kv_range_with_prefix,
        test_simple_kv_range, test_txn_compare_equal, test_txn_compare_greater,
        test_txn_compare_less, test_txn_compare_not_equal, test_txn_one_compare_op,
        text_txn_multi_compare_op, unprepare_kv,
    };
    use crate::maybe_skip_mysql_integration_test;

    async fn build_mysql_kv_backend(table_name: &str) -> Option<MySqlStore> {
        init_default_ut_logging();
        let endpoints = std::env::var("GT_MYSQL_ENDPOINTS").unwrap_or_default();
        if endpoints.is_empty() {
            return None;
        }
        let pool = MySqlPool::connect(&endpoints).await.unwrap();
        let sql_templates = MySqlTemplateFactory::new(table_name).build();
        sqlx::query(&sql_templates.create_table_statement)
            .execute(&pool)
            .await
            .unwrap();
        Some(MySqlStore {
            max_txn_ops: 128,
            sql_template_set: sql_templates,
            txn_retry_count: RDS_STORE_TXN_RETRY_COUNT,
            executor_factory: MySqlExecutorFactory {
                pool: Arc::new(pool),
            },
            _phantom: PhantomData,
        })
    }

    #[tokio::test]
    async fn test_mysql_put() {
        maybe_skip_mysql_integration_test!();
        let kv_backend = build_mysql_kv_backend("put-test").await.unwrap();
        let prefix = b"put/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_put_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_mysql_range() {
        maybe_skip_mysql_integration_test!();
        let kv_backend = build_mysql_kv_backend("range-test").await.unwrap();
        let prefix = b"range/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_range_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_mysql_range_2() {
        maybe_skip_mysql_integration_test!();
        let kv_backend = build_mysql_kv_backend("range2-test").await.unwrap();
        let prefix = b"range2/";
        test_kv_range_2_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_mysql_all_range() {
        maybe_skip_mysql_integration_test!();
        let kv_backend = build_mysql_kv_backend("simple_range-test").await.unwrap();
        let prefix = b"";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_simple_kv_range(&kv_backend).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_mysql_batch_get() {
        maybe_skip_mysql_integration_test!();
        let kv_backend = build_mysql_kv_backend("batch_get-test").await.unwrap();
        let prefix = b"batch_get/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_batch_get_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_mysql_batch_delete() {
        maybe_skip_mysql_integration_test!();
        let kv_backend = build_mysql_kv_backend("batch_delete-test").await.unwrap();
        let prefix = b"batch_delete/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_delete_range_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_mysql_batch_delete_with_prefix() {
        maybe_skip_mysql_integration_test!();
        let kv_backend = build_mysql_kv_backend("batch_delete_with_prefix-test")
            .await
            .unwrap();
        let prefix = b"batch_delete/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_batch_delete_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_mysql_delete_range() {
        maybe_skip_mysql_integration_test!();
        let kv_backend = build_mysql_kv_backend("delete_range-test").await.unwrap();
        let prefix = b"delete_range/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_delete_range_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_mysql_compare_and_put() {
        maybe_skip_mysql_integration_test!();
        let kv_backend = build_mysql_kv_backend("compare_and_put-test")
            .await
            .unwrap();
        let prefix = b"compare_and_put/";
        let kv_backend = Arc::new(kv_backend);
        test_kv_compare_and_put_with_prefix(kv_backend.clone(), prefix.to_vec()).await;
    }

    #[tokio::test]
    async fn test_mysql_txn() {
        maybe_skip_mysql_integration_test!();
        let kv_backend = build_mysql_kv_backend("txn-test").await.unwrap();
        test_txn_one_compare_op(&kv_backend).await;
        text_txn_multi_compare_op(&kv_backend).await;
        test_txn_compare_equal(&kv_backend).await;
        test_txn_compare_greater(&kv_backend).await;
        test_txn_compare_less(&kv_backend).await;
        test_txn_compare_not_equal(&kv_backend).await;
    }
}
