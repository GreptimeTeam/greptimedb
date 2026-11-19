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

use common_telemetry::{debug, info, warn};
use lazy_static::lazy_static;
use regex::Regex;
use snafu::{OptionExt, ResultExt};
use sqlx::mysql::MySqlRow;
use sqlx::pool::Pool;
use sqlx::{AssertSqlSafe, MySql, MySqlPool, Row, Transaction as MySqlTransaction};
use strum::AsRefStr;

use crate::error::{
    CreateMySqlPoolSnafu, MySqlExecutionSnafu, MySqlTransactionSnafu, Result, UnexpectedSnafu,
};
use crate::kv_backend::ClosableKvBackendRef;
use crate::kv_backend::rds::{
    Executor, ExecutorFactory, ExecutorImpl, KvQueryExecutor, RDS_BATCH_SIZE,
    RDS_STORE_OP_BATCH_DELETE, RDS_STORE_OP_BATCH_GET, RDS_STORE_OP_BATCH_PUT,
    RDS_STORE_OP_RANGE_DELETE, RDS_STORE_OP_RANGE_QUERY, RDS_STORE_TXN_RETRY_COUNT, RdsStore,
    Transaction, ensure_rustls_crypto_provider_installed,
};
use crate::rpc::KeyValue;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, RangeRequest, RangeResponse,
};

const MYSQL_STORE_NAME: &str = "mysql_store";

// MySQL prepared statements accept at most 65,535 parameters.
const MAX_BATCH_KEYS: usize = u16::MAX as usize;
const MAX_BATCH_PUT_KVS: usize = MAX_BATCH_KEYS / 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueBlobType {
    Blob,
    MediumBlob,
    LongBlob,
}

lazy_static! {
    static ref VALUE_COLUMN_BLOB_TYPE_RE: Regex =
        Regex::new(r#"(?i)(?:\(|,)\s*[`"]?v[`"]?\s+(longblob|mediumblob|blob)\b"#).unwrap();
}

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
                "CREATE TABLE IF NOT EXISTS `{table_name}`(k VARBINARY(3072) PRIMARY KEY, v MEDIUMBLOB);",
            ),
            show_create_table_statement: format!("SHOW CREATE TABLE `{table_name}`"),
            alter_value_column_statement: format!(
                "ALTER TABLE `{table_name}` MODIFY COLUMN v MEDIUMBLOB;"
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
    show_create_table_statement: String,
    alter_value_column_statement: String,
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
        // Safety: `raw_query` is generated by `MySqlTemplateSet`, and all dynamic data
        // is passed via bind parameters.
        let query = sqlx::query(AssertSqlSafe(raw_query));
        let query = params.iter().fold(query, |query, param| query.bind(param));
        let rows = query
            .fetch_all(&**self)
            .await
            .context(MySqlExecutionSnafu { sql: raw_query })?;
        Ok(rows.into_iter().map(key_value_from_row).collect())
    }

    async fn execute(&mut self, raw_query: &str, params: &[&Vec<u8>]) -> Result<()> {
        // Safety: `raw_query` is generated by `MySqlTemplateSet`, and all dynamic data
        // is passed via bind parameters.
        let query = sqlx::query(AssertSqlSafe(raw_query));
        let query = params.iter().fold(query, |query, param| query.bind(param));
        query
            .execute(&**self)
            .await
            .context(MySqlExecutionSnafu { sql: raw_query })?;
        Ok(())
    }

    async fn txn_executor<'a>(&'a mut self) -> Result<Self::Transaction<'a>> {
        // MySQL requires SET TRANSACTION separately from BEGIN. Execute both on
        // the connection acquired by begin_with so the isolation level applies.
        let txn = self
            .begin_with("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN")
            .await
            .context(MySqlTransactionSnafu { operation: "begin" })?;
        Ok(MySqlTxnClient(txn))
    }

    async fn read_only_txn_executor<'a>(&'a mut self) -> Result<Self::Transaction<'a>> {
        let txn = self
            .begin_with("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY; BEGIN")
            .await
            .context(MySqlTransactionSnafu {
                operation: "begin read-only",
            })?;
        Ok(MySqlTxnClient(txn))
    }
}

#[async_trait::async_trait]
impl Transaction<'_> for MySqlTxnClient {
    async fn query(&mut self, raw_query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>> {
        // Safety: `raw_query` is generated by `MySqlTemplateSet`, and all dynamic data
        // is passed via bind parameters.
        let query = sqlx::query(AssertSqlSafe(raw_query));
        let query = params.iter().fold(query, |query, param| query.bind(param));
        // As said in https://docs.rs/sqlx/latest/sqlx/trait.Executor.html, we need a `&mut *transaction`. Weird.
        let rows = query
            .fetch_all(&mut *(self.0))
            .await
            .context(MySqlExecutionSnafu { sql: raw_query })?;
        Ok(rows.into_iter().map(key_value_from_row).collect())
    }

    async fn execute(&mut self, raw_query: &str, params: &[&Vec<u8>]) -> Result<()> {
        // Safety: `raw_query` is generated by `MySqlTemplateSet`, and all dynamic data
        // is passed via bind parameters.
        let query = sqlx::query(AssertSqlSafe(raw_query));
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
    async fn close(&self) {
        self.pool.close().await;
    }

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
        let kvs = &req.kvs;
        if kvs.is_empty() {
            return Ok(BatchPutResponse::default());
        }
        if (req.prev_kv || kvs.len() > (*RDS_BATCH_SIZE).min(MAX_BATCH_PUT_KVS))
            && let ExecutorImpl::Default(executor) = query_executor
        {
            let mut txn = ExecutorImpl::Txn(executor.txn_executor().await?);
            let response = self.put_chunks(&mut txn, kvs, req.prev_kv).await?;
            txn.commit().await?;
            return Ok(response);
        }
        self.put_chunks(query_executor, kvs, req.prev_kv).await
    }

    async fn batch_get_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, MySqlClient>,
        req: BatchGetRequest,
    ) -> Result<BatchGetResponse> {
        let mut keys = req.keys;
        keys.sort_unstable();
        keys.dedup();
        let chunk_size = *RDS_BATCH_SIZE;
        if keys.len() > chunk_size
            && let ExecutorImpl::Default(client) = query_executor
        {
            // Share one snapshot without serializable read conflicts or row locks.
            let mut transaction = ExecutorImpl::Txn(client.read_only_txn_executor().await?);
            let response = self.get_chunks(&mut transaction, &keys, chunk_size).await?;
            transaction.commit().await?;
            return Ok(response);
        }
        self.get_chunks(query_executor, &keys, chunk_size).await
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
        let keys = req.keys;
        if keys.is_empty() {
            return Ok(BatchDeleteResponse::default());
        }
        if (req.prev_kv || keys.len() > *RDS_BATCH_SIZE)
            && let ExecutorImpl::Default(executor) = query_executor
        {
            let mut txn = ExecutorImpl::Txn(executor.txn_executor().await?);
            let response = self.delete_chunks(&mut txn, &keys, req.prev_kv).await?;
            txn.commit().await?;
            return Ok(response);
        }
        self.delete_chunks(query_executor, &keys, req.prev_kv).await
    }
}

impl MySqlStore {
    async fn get_chunks(
        &self,
        query_executor: &mut ExecutorImpl<'_, MySqlClient>,
        keys: &[Vec<u8>],
        chunk_size: usize,
    ) -> Result<BatchGetResponse> {
        let mut kvs = Vec::new();
        let mut params = Vec::with_capacity(keys.len().min(chunk_size));
        for keys in keys.chunks(chunk_size) {
            let query = self.sql_template_set.generate_batch_get_query(keys.len());
            params.clear();
            params.extend(keys);
            kvs.extend(crate::record_rds_sql_execute_elapsed!(
                query_executor.query(&query, &params).await,
                MYSQL_STORE_NAME,
                RDS_STORE_OP_BATCH_GET,
                ""
            )?);
        }
        Ok(BatchGetResponse { kvs })
    }

    async fn put_chunks(
        &self,
        executor: &mut ExecutorImpl<'_, MySqlClient>,
        kvs: &[KeyValue],
        prev_kv: bool,
    ) -> Result<BatchPutResponse> {
        let mut prev_kvs = Vec::new();
        let chunk_size = (*RDS_BATCH_SIZE).min(MAX_BATCH_PUT_KVS);
        let mut params = Vec::with_capacity(kvs.len().min(chunk_size) * 2);
        for chunk in kvs.chunks(chunk_size) {
            let (select, update) = self
                .sql_template_set
                .generate_batch_upsert_query(chunk.len());
            if prev_kv {
                params.clear();
                params.extend(chunk.iter().map(|kv| &kv.key));
                prev_kvs.extend(crate::record_rds_sql_execute_elapsed!(
                    executor.query(&select, &params).await,
                    MYSQL_STORE_NAME,
                    RDS_STORE_OP_BATCH_PUT,
                    ""
                )?);
            }
            params.clear();
            params.extend(chunk.iter().flat_map(|kv| [&kv.key, &kv.value]));
            crate::record_rds_sql_execute_elapsed!(
                executor.execute(&update, &params).await,
                MYSQL_STORE_NAME,
                RDS_STORE_OP_BATCH_PUT,
                ""
            )?;
        }
        Ok(BatchPutResponse { prev_kvs })
    }

    async fn delete_chunks(
        &self,
        executor: &mut ExecutorImpl<'_, MySqlClient>,
        keys: &[Vec<u8>],
        prev_kv: bool,
    ) -> Result<BatchDeleteResponse> {
        let mut prev_kvs = Vec::new();
        let mut params = Vec::with_capacity(keys.len().min(*RDS_BATCH_SIZE));
        for chunk in keys.chunks(*RDS_BATCH_SIZE) {
            params.clear();
            params.extend(chunk);
            if prev_kv {
                let select = self.sql_template_set.generate_batch_get_query(chunk.len());
                prev_kvs.extend(crate::record_rds_sql_execute_elapsed!(
                    executor.query(&select, &params).await,
                    MYSQL_STORE_NAME,
                    RDS_STORE_OP_BATCH_DELETE,
                    ""
                )?);
            }
            let delete = self
                .sql_template_set
                .generate_batch_delete_query(chunk.len());
            crate::record_rds_sql_execute_elapsed!(
                executor.execute(&delete, &params).await,
                MYSQL_STORE_NAME,
                RDS_STORE_OP_BATCH_DELETE,
                ""
            )?;
        }
        Ok(BatchDeleteResponse { prev_kvs })
    }

    /// Reads the current table definition for best-effort schema upgrades.
    async fn fetch_create_table_sql(
        pool: &Pool<MySql>,
        sql_template_set: &MySqlTemplateSet,
    ) -> Result<Option<String>> {
        let row = sqlx::query(AssertSqlSafe(
            sql_template_set.show_create_table_statement.as_str(),
        ))
        .fetch_optional(pool)
        .await
        .with_context(|_| MySqlExecutionSnafu {
            sql: sql_template_set.show_create_table_statement.clone(),
        })?;
        Ok(row.map(|row| row.get(1)))
    }

    /// Parses the blob type of the `v` column from `SHOW CREATE TABLE` output.
    fn parse_value_column_blob_type(create_table_sql: &str) -> Option<ValueBlobType> {
        // `SHOW CREATE TABLE` returns MySQL-specific DDL. A minimal parser keeps the
        // upgrade check small and avoids introducing a SQL parser just for one column.
        let captures = VALUE_COLUMN_BLOB_TYPE_RE.captures(create_table_sql)?;
        match captures.get(1)?.as_str().to_ascii_lowercase().as_str() {
            "blob" => Some(ValueBlobType::Blob),
            "mediumblob" => Some(ValueBlobType::MediumBlob),
            "longblob" => Some(ValueBlobType::LongBlob),
            _ => None,
        }
    }

    /// Upgrades the metadata value column to `MEDIUMBLOB` when an old table still uses `BLOB`.
    async fn maybe_upgrade_value_column_to_mediumblob(
        pool: &Pool<MySql>,
        sql_template_set: &MySqlTemplateSet,
    ) -> Result<()> {
        let table_name = &sql_template_set.table_name;
        let create_table_sql = Self::fetch_create_table_sql(pool, sql_template_set)
            .await?
            .context(UnexpectedSnafu {
                err_msg: format!("Failed to fetch CREATE TABLE SQL for `{table_name}`"),
            })?;

        match Self::parse_value_column_blob_type(&create_table_sql) {
            Some(ValueBlobType::Blob) => {
                sqlx::query(AssertSqlSafe(
                    sql_template_set.alter_value_column_statement.as_str(),
                ))
                .execute(pool)
                .await
                .with_context(|_| MySqlExecutionSnafu {
                    sql: sql_template_set.alter_value_column_statement.clone(),
                })?;
                info!("Upgraded MySQL metadata value column to MEDIUMBLOB for `{table_name}`");
            }
            Some(ValueBlobType::MediumBlob | ValueBlobType::LongBlob) => {
                debug!("MySQL metadata value column for `{table_name}` is already compatible");
            }
            None => {
                warn!(
                    "Failed to determine MySQL metadata value column type from table definition for `{table_name}`, skip automatic MEDIUMBLOB upgrade"
                );
            }
        }

        Ok(())
    }

    /// Create [MySqlStore] impl of [ClosableKvBackendRef] from url.
    pub async fn with_url(
        url: &str,
        table_name: &str,
        max_txn_ops: usize,
    ) -> Result<ClosableKvBackendRef> {
        ensure_rustls_crypto_provider_installed()?;
        let pool = MySqlPool::connect(url)
            .await
            .context(CreateMySqlPoolSnafu)?;
        Self::with_mysql_pool(pool, table_name, max_txn_ops).await
    }

    /// Create [MySqlStore] impl of [ClosableKvBackendRef] from [sqlx::Pool<MySql>].
    pub async fn with_mysql_pool(
        pool: Pool<MySql>,
        table_name: &str,
        max_txn_ops: usize,
    ) -> Result<ClosableKvBackendRef> {
        // This step ensures the mysql metadata backend is ready to use.
        // We check if greptime_metakv table exists, and we will create a new table
        // if it does not exist.
        let sql_template_set = MySqlTemplateFactory::new(table_name).build();
        sqlx::query(AssertSqlSafe(
            sql_template_set.create_table_statement.as_str(),
        ))
        .execute(&pool)
        .await
        .with_context(|_| MySqlExecutionSnafu {
            sql: sql_template_set.create_table_statement.clone(),
        })?;
        Self::maybe_upgrade_value_column_to_mediumblob(&pool, &sql_template_set).await?;
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
    use sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
    use uuid::Uuid;

    use super::*;
    use crate::kv_backend::KvBackend;
    use crate::kv_backend::test::{
        prepare_kv_with_prefix, test_kv_batch_delete_with_prefix,
        test_kv_batch_get_concurrent_updates, test_kv_batch_get_with_prefix,
        test_kv_compare_and_put_with_prefix, test_kv_delete_range_with_prefix,
        test_kv_put_with_prefix, test_kv_range_2_with_prefix, test_kv_range_with_prefix,
        test_simple_kv_range, test_txn_compare_equal, test_txn_compare_greater,
        test_txn_compare_less, test_txn_compare_not_equal, test_txn_one_compare_op,
        text_txn_multi_compare_op, unprepare_kv,
    };
    use crate::maybe_skip_mysql_integration_test;
    use crate::rpc::store::{PutRequest, RangeRequest};
    use crate::test_util::test_certs_dir;

    #[tokio::test]
    async fn test_close_backend() {
        let pool =
            sqlx::mysql::MySqlPoolOptions::new().connect_lazy_with(MySqlConnectOptions::new());
        let backend: ClosableKvBackendRef = Arc::new(MySqlStore {
            max_txn_ops: 128,
            txn_retry_count: RDS_STORE_TXN_RETRY_COUNT,
            executor_factory: MySqlExecutorFactory {
                pool: Arc::new(pool.clone()),
            },
            sql_template_set: MySqlTemplateFactory::new("close_test").build(),
            _phantom: PhantomData,
        });
        let shared_backend = backend.clone();
        assert!(!pool.is_closed());
        backend.close().await.unwrap();
        assert!(pool.is_closed());
        shared_backend.close().await.unwrap();
        assert!(matches!(pool.acquire().await, Err(sqlx::Error::PoolClosed)));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            shared_backend.put(crate::rpc::store::PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                prev_kv: false,
            }),
        )
        .await
        .expect("closed backend must reject writes promptly");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_close_backend_waits_for_checked_out_connection() {
        maybe_skip_mysql_integration_test!();
        let pool = mysql_pool().await.unwrap();
        let table = new_test_table_name("close_held");
        let backend = MySqlStore::with_mysql_pool(pool.clone(), &table, 128)
            .await
            .unwrap();
        let connection = pool.acquire().await.unwrap();
        let close = backend.close();
        tokio::pin!(close);

        assert!(futures::poll!(close.as_mut()).is_pending());
        assert!(pool.is_closed());
        assert!(matches!(pool.acquire().await, Err(sqlx::Error::PoolClosed)));
        drop(connection);
        tokio::time::timeout(std::time::Duration::from_secs(5), close)
            .await
            .expect("close must finish after the connection is returned")
            .unwrap();
        backend.close().await.unwrap();
    }

    fn new_test_table_name(prefix: &str) -> String {
        let uuid = Uuid::new_v4().simple().to_string();
        let max_prefix_len = 63usize.saturating_sub(uuid.len() + 1);
        let prefix = &prefix[..prefix.len().min(max_prefix_len)];
        format!("{prefix}_{uuid}")
    }

    async fn mysql_pool() -> Option<MySqlPool> {
        init_default_ut_logging();
        let endpoints = std::env::var("GT_MYSQL_ENDPOINTS").unwrap_or_default();
        if endpoints.is_empty() {
            return None;
        }
        ensure_rustls_crypto_provider_installed().unwrap();
        Some(MySqlPool::connect(&endpoints).await.unwrap())
    }

    async fn show_create_table(pool: &MySqlPool, table_name: &str) -> String {
        let sql = format!("SHOW CREATE TABLE `{table_name}`");
        let row = sqlx::query(AssertSqlSafe(sql.as_str()))
            .fetch_one(pool)
            .await
            .unwrap();
        row.get::<String, _>(1)
    }

    async fn create_legacy_blob_table(pool: &MySqlPool, table_name: &str) {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS `{table_name}`(k VARBINARY(3072) PRIMARY KEY, v BLOB);"
        );
        sqlx::query(AssertSqlSafe(sql.as_str()))
            .execute(pool)
            .await
            .unwrap();
    }

    async fn drop_table(pool: &MySqlPool, table_name: &str) {
        let sql = format!("DROP TABLE IF EXISTS `{table_name}`;");
        sqlx::query(AssertSqlSafe(sql.as_str()))
            .execute(pool)
            .await
            .unwrap();
    }

    async fn build_mysql_kv_backend(table_name: &str) -> Option<MySqlStore> {
        let pool = mysql_pool().await?;
        let sql_templates = MySqlTemplateFactory::new(table_name).build();
        sqlx::query(AssertSqlSafe(sql_templates.create_table_statement.as_str()))
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
    async fn test_mysql_batch_parameter_boundaries() {
        maybe_skip_mysql_integration_test!();
        let table = new_test_table_name("batch_parameter_boundaries");
        let store = build_mysql_kv_backend(&table).await.unwrap();
        let client = store.executor_factory.default_executor().await.unwrap();
        let mut executor = ExecutorImpl::Default(client.clone());
        for size in [
            0,
            MAX_BATCH_PUT_KVS - 1,
            MAX_BATCH_PUT_KVS,
            MAX_BATCH_PUT_KVS + 1,
            MAX_BATCH_KEYS - 1,
            MAX_BATCH_KEYS,
            MAX_BATCH_KEYS + 1,
        ] {
            let kvs: Vec<_> = (0..size)
                .map(|index| KeyValue {
                    key: format!("key-{index:06}").into_bytes(),
                    value: b"original".to_vec(),
                })
                .collect();
            let keys: Vec<_> = kvs.iter().map(|kv| kv.key.clone()).collect();
            let response = store
                .batch_put_with_query_executor(
                    &mut executor,
                    BatchPutRequest {
                        kvs: kvs.clone(),
                        prev_kv: false,
                    },
                )
                .await
                .unwrap();
            assert!(response.prev_kvs.is_empty());
            let mut fetched = store
                .batch_get_with_query_executor(
                    &mut executor,
                    BatchGetRequest { keys: keys.clone() },
                )
                .await
                .unwrap()
                .kvs;
            fetched.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            assert_eq!(fetched, kvs);
            let updated = kvs
                .iter()
                .map(|kv| KeyValue {
                    key: kv.key.clone(),
                    value: b"updated".to_vec(),
                })
                .collect::<Vec<_>>();
            let mut previous = store
                .batch_put_with_query_executor(
                    &mut executor,
                    BatchPutRequest {
                        kvs: updated.clone(),
                        prev_kv: true,
                    },
                )
                .await
                .unwrap()
                .prev_kvs;
            previous.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            assert_eq!(previous, kvs);
            let mut deleted = store
                .batch_delete_with_query_executor(
                    &mut executor,
                    BatchDeleteRequest {
                        keys: keys.clone(),
                        prev_kv: true,
                    },
                )
                .await
                .unwrap()
                .prev_kvs;
            deleted.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            assert_eq!(deleted, updated);
            assert!(
                store
                    .batch_get_with_query_executor(&mut executor, BatchGetRequest { keys })
                    .await
                    .unwrap()
                    .kvs
                    .is_empty()
            );
        }
        drop_table(&store.executor_factory.pool, &table).await;
    }

    #[tokio::test]
    async fn test_mysql_batch_get_concurrent_updates() {
        maybe_skip_mysql_integration_test!();
        let table = new_test_table_name("batch_get_consistent");
        let backend = build_mysql_kv_backend(&table).await.unwrap();
        test_kv_batch_get_concurrent_updates(&backend, *RDS_BATCH_SIZE + 1).await;
        drop_table(&backend.executor_factory.pool, &table).await;
    }

    #[tokio::test]
    async fn test_mysql_transaction_isolation_preserves_session_defaults() {
        maybe_skip_mysql_integration_test!();
        ensure_rustls_crypto_provider_installed().unwrap();
        // A single connection forces every subsequent borrower to reuse it.
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&std::env::var("GT_MYSQL_ENDPOINTS").unwrap())
            .await
            .unwrap();
        let table = new_test_table_name("transaction_scope");
        let backend = build_mysql_kv_backend(&table).await.unwrap();
        let mut writer = backend.executor_factory.pool.acquire().await.unwrap();
        sqlx::query("SET SESSION innodb_lock_wait_timeout = 1")
            .execute(&mut *writer)
            .await
            .unwrap();
        let reset = format!(
            "INSERT INTO `{table}` (k,v) VALUES ('a','old') ON DUPLICATE KEY UPDATE v='old'"
        );
        let update = format!("UPDATE `{table}` SET v='new' WHERE k='a'");
        let select = backend.sql_template_set.generate_batch_get_query(1);
        let key = b"a".to_vec();
        let mut client = Arc::new(pool);
        for read_only in [false, true] {
            for finish in ["commit", "rollback", "drop"] {
                sqlx::query(AssertSqlSafe(reset.as_str()))
                    .execute(&mut *writer)
                    .await
                    .unwrap();
                let mut txn = if read_only {
                    client.read_only_txn_executor().await.unwrap()
                } else {
                    client.txn_executor().await.unwrap()
                };
                assert_eq!(txn.query(&select, &[&key]).await.unwrap()[0].value, b"old");
                let write = sqlx::query(AssertSqlSafe(update.as_str()))
                    .execute(&mut *writer)
                    .await;
                if read_only {
                    // RR permits the writer to commit, but retains the first snapshot.
                    write.unwrap();
                    assert_eq!(txn.query(&select, &[&key]).await.unwrap()[0].value, b"old");
                    let error = sqlx::query(AssertSqlSafe(update.as_str()))
                        .execute(&mut *txn.0)
                        .await
                        .unwrap_err();
                    assert_eq!(
                        error.as_database_error().unwrap().code().as_deref(),
                        Some("25006")
                    );
                } else {
                    // SERIALIZABLE plain reads hold locks; a session-default RC
                    // transaction would let this update succeed.
                    let error = write.unwrap_err();
                    assert_eq!(
                        error
                            .as_database_error()
                            .unwrap()
                            .downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                            .number(),
                        1205
                    );
                }
                match finish {
                    "commit" => txn.0.commit().await.unwrap(),
                    "rollback" => txn.0.rollback().await.unwrap(),
                    "drop" => drop(txn),
                    _ => unreachable!(),
                }
                let mut borrowed = client.acquire().await.unwrap();
                let row = sqlx::query(
                    "SHOW SESSION VARIABLES WHERE Variable_name IN ('transaction_isolation', 'tx_isolation')",
                ).fetch_one(&mut *borrowed).await.unwrap();
                assert_eq!(row.get::<String, _>(1), "READ-COMMITTED");
                drop(borrowed);

                // Verify the next ordinary transaction inherits RC and read/write
                // access, rather than either override used above.
                let mut next = client.begin().await.unwrap();
                sqlx::query(AssertSqlSafe(reset.as_str()))
                    .execute(&mut *writer)
                    .await
                    .unwrap();
                let first = sqlx::query(AssertSqlSafe(select.as_str()))
                    .bind(&key)
                    .fetch_one(&mut *next)
                    .await
                    .unwrap();
                assert_eq!(first.get::<Vec<u8>, _>(1), b"old");
                sqlx::query(AssertSqlSafe(update.as_str()))
                    .execute(&mut *writer)
                    .await
                    .unwrap();
                let second = sqlx::query(AssertSqlSafe(select.as_str()))
                    .bind(&key)
                    .fetch_one(&mut *next)
                    .await
                    .unwrap();
                assert_eq!(second.get::<Vec<u8>, _>(1), b"new");
                sqlx::query(AssertSqlSafe(reset.as_str()))
                    .execute(&mut *next)
                    .await
                    .unwrap();
                next.rollback().await.unwrap();
            }
        }
        drop(writer);
        drop_table(&backend.executor_factory.pool, &table).await;
        client.close().await;
    }

    #[tokio::test]
    async fn test_mysql_batch_get_does_not_wait_for_uncommitted_write() {
        maybe_skip_mysql_integration_test!();
        let table = new_test_table_name("snapshot_locked_writer");
        let mut backend = build_mysql_kv_backend(&table).await.unwrap();
        // Force the reader to time out quickly if it accidentally uses locking reads.
        let reader_pool = sqlx::mysql::MySqlPoolOptions::new()
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET SESSION innodb_lock_wait_timeout = 1")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&std::env::var("GT_MYSQL_ENDPOINTS").unwrap())
            .await
            .unwrap();
        let writer_pool = backend.executor_factory.pool.clone();
        backend.executor_factory.pool = Arc::new(reader_pool);
        let kvs = vec![
            KeyValue {
                key: b"a".to_vec(),
                value: b"old".to_vec(),
            },
            KeyValue {
                key: b"z".to_vec(),
                value: b"old".to_vec(),
            },
        ];
        backend
            .batch_put(BatchPutRequest {
                kvs: kvs.clone(),
                prev_kv: false,
            })
            .await
            .unwrap();
        let mut writer = writer_pool.begin().await.unwrap();
        let update = format!("UPDATE `{table}` SET v='new' WHERE k='z'");
        sqlx::query(AssertSqlSafe(update.as_str()))
            .execute(&mut *writer)
            .await
            .unwrap();
        let mut keys = vec![b"a".to_vec()];
        keys.extend((0..*RDS_BATCH_SIZE - 1).map(|i| format!("m{i:06}").into_bytes()));
        keys.push(b"z".to_vec());
        let mut actual = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            backend.batch_get(BatchGetRequest { keys }),
        )
        .await
        .unwrap()
        .unwrap()
        .kvs;
        actual.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(actual, kvs);
        writer.rollback().await.unwrap();
        drop_table(&writer_pool, &table).await;
    }

    #[tokio::test]
    async fn test_mysql_batch_get_deduplicates_keys_before_chunking() {
        maybe_skip_mysql_integration_test!();
        let table = new_test_table_name("get_duplicate_keys");
        let store = build_mysql_kv_backend(&table).await.unwrap();
        let client = store.executor_factory.default_executor().await.unwrap();
        let mut executor = ExecutorImpl::Default(client.clone());
        let kvs = vec![KeyValue {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }];
        store
            .batch_put_with_query_executor(
                &mut executor,
                BatchPutRequest {
                    kvs: kvs.clone(),
                    prev_kv: false,
                },
            )
            .await
            .unwrap();
        for count in [1, *RDS_BATCH_SIZE, *RDS_BATCH_SIZE + 1] {
            let response = store
                .batch_get_with_query_executor(
                    &mut executor,
                    BatchGetRequest {
                        keys: vec![b"key".to_vec(); count],
                    },
                )
                .await
                .unwrap();
            assert_eq!(response.kvs, kvs);
        }
        drop_table(&store.executor_factory.pool, &table).await;
    }

    #[tokio::test]
    async fn test_mysql_chunks_reuse_existing_transaction() {
        maybe_skip_mysql_integration_test!();
        let table = new_test_table_name("existing_chunk_transaction");
        let store = build_mysql_kv_backend(&table).await.unwrap();
        let mut client = store.executor_factory.default_executor().await.unwrap();
        let kvs: Vec<_> = (0..=*RDS_BATCH_SIZE)
            .map(|index| KeyValue {
                key: format!("key-{index:06}").into_bytes(),
                value: b"value".to_vec(),
            })
            .collect();
        let keys = kvs.iter().map(|kv| kv.key.clone()).collect::<Vec<_>>();
        {
            let mut txn = ExecutorImpl::Txn(client.txn_executor().await.unwrap());
            store
                .batch_put_with_query_executor(
                    &mut txn,
                    BatchPutRequest {
                        kvs,
                        prev_kv: false,
                    },
                )
                .await
                .unwrap();
            let inside = store
                .batch_get_with_query_executor(&mut txn, BatchGetRequest { keys: keys.clone() })
                .await
                .unwrap();
            assert_eq!(inside.kvs.len(), keys.len());
            // The caller owns this transaction and deliberately does not commit it.
        }
        let mut executor = ExecutorImpl::Default(client.clone());
        let outside = store
            .batch_get_with_query_executor(&mut executor, BatchGetRequest { keys })
            .await
            .unwrap();
        assert!(outside.kvs.is_empty());
        drop_table(&store.executor_factory.pool, &table).await;
    }

    #[tokio::test]
    async fn test_mysql_later_put_chunk_failure_rolls_back() {
        maybe_skip_mysql_integration_test!();
        let table = new_test_table_name("put_chunk_rollback");
        let store = build_mysql_kv_backend(&table).await.unwrap();
        let trigger_name = new_test_table_name("put_failure");
        let trigger = format!(
            "CREATE TRIGGER `{trigger_name}` BEFORE INSERT ON `{table}` \
            FOR EACH ROW BEGIN IF NEW.v = X'6661696c' THEN \
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected put failure'; END IF; END"
        );
        sqlx::raw_sql(AssertSqlSafe(trigger.as_str()))
            .execute(store.executor_factory.pool.as_ref())
            .await
            .unwrap();
        let client = store.executor_factory.default_executor().await.unwrap();
        let mut executor = ExecutorImpl::Default(client.clone());
        for prev_kv in [false, true] {
            let mut kvs: Vec<_> = (0..(*RDS_BATCH_SIZE).min(MAX_BATCH_PUT_KVS))
                .map(|index| KeyValue {
                    key: format!("key-{index:06}").into_bytes(),
                    value: b"ok".to_vec(),
                })
                .collect();
            kvs.push(KeyValue {
                key: b"last".to_vec(),
                value: b"fail".to_vec(),
            });
            assert!(
                store
                    .batch_put_with_query_executor(&mut executor, BatchPutRequest { kvs, prev_kv })
                    .await
                    .is_err()
            );
            let remaining = store
                .range_with_query_executor(
                    &mut executor,
                    RangeRequest::new().with_range(vec![0], vec![0]),
                )
                .await
                .unwrap();
            assert!(
                remaining.kvs.is_empty(),
                "successful earlier chunks must roll back"
            );
        }
        drop_table(&store.executor_factory.pool, &table).await;
    }

    #[tokio::test]
    async fn test_mysql_later_delete_chunk_failure_rolls_back() {
        maybe_skip_mysql_integration_test!();
        let table = new_test_table_name("delete_chunk_rollback");
        let store = build_mysql_kv_backend(&table).await.unwrap();
        let client = store.executor_factory.default_executor().await.unwrap();
        let mut executor = ExecutorImpl::Default(client.clone());
        let kvs: Vec<_> = (0..=*RDS_BATCH_SIZE)
            .map(|index| KeyValue {
                key: format!("key-{index:06}").into_bytes(),
                value: b"original".to_vec(),
            })
            .collect();
        let keys = kvs.iter().map(|kv| kv.key.clone()).collect::<Vec<_>>();
        store
            .batch_put_with_query_executor(
                &mut executor,
                BatchPutRequest {
                    kvs: kvs.clone(),
                    prev_kv: false,
                },
            )
            .await
            .unwrap();
        let last_key = String::from_utf8(keys.last().unwrap().clone()).unwrap();
        let trigger_name = new_test_table_name("delete_failure");
        let trigger = format!(
            "CREATE TRIGGER `{trigger_name}` BEFORE DELETE ON `{table}` \
            FOR EACH ROW BEGIN IF OLD.k = '{last_key}' THEN \
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected delete failure'; END IF; END"
        );
        sqlx::raw_sql(AssertSqlSafe(trigger.as_str()))
            .execute(store.executor_factory.pool.as_ref())
            .await
            .unwrap();
        for prev_kv in [false, true] {
            assert!(
                store
                    .batch_delete_with_query_executor(
                        &mut executor,
                        BatchDeleteRequest {
                            keys: keys.clone(),
                            prev_kv
                        }
                    )
                    .await
                    .is_err()
            );
            let mut remaining = store
                .batch_get_with_query_executor(
                    &mut executor,
                    BatchGetRequest { keys: keys.clone() },
                )
                .await
                .unwrap()
                .kvs;
            remaining.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            assert_eq!(remaining, kvs, "successful earlier chunks must roll back");
        }
        drop_table(&store.executor_factory.pool, &table).await;
    }

    #[test]
    fn test_parse_value_column_blob_type() {
        let sql = r#"CREATE TABLE `greptime_metakv` (
  `k` varbinary(3072) NOT NULL,
  `v` MEDIUMBLOB,
  PRIMARY KEY (`k`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#;
        assert_eq!(
            Some(ValueBlobType::MediumBlob),
            MySqlStore::parse_value_column_blob_type(sql)
        );

        let sql = r#"CREATE TABLE `greptime_metakv` (`k` varbinary(3072) NOT NULL, `v` blob, PRIMARY KEY (`k`))"#;
        assert_eq!(
            Some(ValueBlobType::Blob),
            MySqlStore::parse_value_column_blob_type(sql)
        );

        let sql = r#"CREATE TABLE `greptime_metakv` (`k` varbinary(3072) NOT NULL, `v` longblob, PRIMARY KEY (`k`))"#;
        assert_eq!(
            Some(ValueBlobType::LongBlob),
            MySqlStore::parse_value_column_blob_type(sql)
        );

        let sql = "CREATE TABLE `greptime_metakv` (`k` varbinary(3072) NOT NULL, `v`    BLOB NOT NULL, PRIMARY KEY (`k`))";
        assert_eq!(
            Some(ValueBlobType::Blob),
            MySqlStore::parse_value_column_blob_type(sql)
        );

        let sql = "CREATE TABLE `greptime_metakv` (\n  `k` varbinary(3072) NOT NULL,\n  \"v\" MediumBlob,\n  PRIMARY KEY (`k`)\n)";
        assert_eq!(
            Some(ValueBlobType::MediumBlob),
            MySqlStore::parse_value_column_blob_type(sql)
        );

        let sql = "CREATE TABLE `greptime_metakv` (`k` varbinary(3072) NOT NULL, vv blob, `v` longblob, PRIMARY KEY (`k`))";
        assert_eq!(
            Some(ValueBlobType::LongBlob),
            MySqlStore::parse_value_column_blob_type(sql)
        );

        let sql = "CREATE TABLE `greptime_metakv` (`k` varbinary(3072) NOT NULL, value blob, PRIMARY KEY (`k`))";
        assert_eq!(None, MySqlStore::parse_value_column_blob_type(sql));

        let sql = "CREATE TABLE `greptime_metakv` (`k` varbinary(3072) NOT NULL, `v` varchar(255), PRIMARY KEY (`k`))";
        assert_eq!(None, MySqlStore::parse_value_column_blob_type(sql));

        let sql =
            "CREATE TABLE `greptime_metakv` (`k` varbinary(3072) NOT NULL, PRIMARY KEY (`k`))";
        assert_eq!(None, MySqlStore::parse_value_column_blob_type(sql));
    }

    #[tokio::test]
    async fn test_mysql_new_metadata_table_uses_mediumblob() {
        maybe_skip_mysql_integration_test!();
        let pool = mysql_pool().await.unwrap();
        let table_name = new_test_table_name("test_mysql_mediumblob_schema");

        MySqlStore::with_mysql_pool(pool.clone(), &table_name, 128)
            .await
            .unwrap();

        let create_table = show_create_table(&pool, &table_name).await;
        assert!(create_table.to_ascii_uppercase().contains("MEDIUMBLOB"));

        drop_table(&pool, &table_name).await;
    }

    #[tokio::test]
    async fn test_mysql_legacy_blob_metadata_table_is_upgraded() {
        maybe_skip_mysql_integration_test!();
        let pool = mysql_pool().await.unwrap();
        let table_name = new_test_table_name("test_mysql_legacy_blob_upgrade");

        create_legacy_blob_table(&pool, &table_name).await;
        MySqlStore::with_mysql_pool(pool.clone(), &table_name, 128)
            .await
            .unwrap();

        let create_table = show_create_table(&pool, &table_name).await;
        assert!(create_table.to_ascii_uppercase().contains("MEDIUMBLOB"));

        drop_table(&pool, &table_name).await;
    }

    #[tokio::test]
    async fn test_mysql_metadata_table_stores_large_values() {
        maybe_skip_mysql_integration_test!();
        let pool = mysql_pool().await.unwrap();
        let table_name = new_test_table_name("test_mysql_large_metadata_value");
        let kv_backend = MySqlStore::with_mysql_pool(pool.clone(), &table_name, 128)
            .await
            .unwrap();
        let key = b"large-value".to_vec();
        let value = vec![b'x'; 70 * 1024];

        kv_backend
            .put(
                PutRequest::new()
                    .with_key(key.clone())
                    .with_value(value.clone()),
            )
            .await
            .unwrap();
        let response = kv_backend
            .range(RangeRequest::new().with_key(key.clone()))
            .await
            .unwrap();

        assert_eq!(1, response.kvs.len());
        assert_eq!(key, response.kvs[0].key);
        assert_eq!(value, response.kvs[0].value);

        drop_table(&pool, &table_name).await;
    }

    #[tokio::test]
    async fn test_mysql_upgraded_metadata_table_stores_large_values() {
        maybe_skip_mysql_integration_test!();
        let pool = mysql_pool().await.unwrap();
        let table_name = new_test_table_name("test_mysql_upgraded_large_metadata_value");

        create_legacy_blob_table(&pool, &table_name).await;
        let kv_backend = MySqlStore::with_mysql_pool(pool.clone(), &table_name, 128)
            .await
            .unwrap();
        let key = b"large-value".to_vec();
        let value = vec![b'y'; 70 * 1024];

        kv_backend
            .put(
                PutRequest::new()
                    .with_key(key.clone())
                    .with_value(value.clone()),
            )
            .await
            .unwrap();
        let response = kv_backend
            .range(RangeRequest::new().with_key(key.clone()))
            .await
            .unwrap();

        assert_eq!(1, response.kvs.len());
        assert_eq!(key, response.kvs[0].key);
        assert_eq!(value, response.kvs[0].value);

        drop_table(&pool, &table_name).await;
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

    #[tokio::test]
    async fn test_mysql_with_tls() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_mysql_integration_test!();
        ensure_rustls_crypto_provider_installed().unwrap();
        let endpoint = std::env::var("GT_MYSQL_ENDPOINTS").unwrap();

        let opts = endpoint
            .parse::<MySqlConnectOptions>()
            .unwrap()
            .ssl_mode(MySqlSslMode::Required);
        let pool = MySqlPool::connect_with(opts).await.unwrap();
        sqlx::query("SELECT 1").execute(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_mysql_with_mtls() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_mysql_integration_test!();
        ensure_rustls_crypto_provider_installed().unwrap();
        let endpoint = std::env::var("GT_MYSQL_ENDPOINTS").unwrap();
        let certs_dir = test_certs_dir();

        let opts = endpoint
            .parse::<MySqlConnectOptions>()
            .unwrap()
            .ssl_mode(MySqlSslMode::Required)
            .ssl_client_cert(certs_dir.join("client.crt").to_string_lossy().to_string())
            .ssl_client_key(certs_dir.join("client.key").to_string_lossy().to_string());
        let pool = MySqlPool::connect_with(opts).await.unwrap();
        sqlx::query("SELECT 1").execute(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_mysql_with_tls_verify_ca() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_mysql_integration_test!();
        ensure_rustls_crypto_provider_installed().unwrap();
        let endpoint = std::env::var("GT_MYSQL_ENDPOINTS").unwrap();
        let certs_dir = test_certs_dir();

        let opts = endpoint
            .parse::<MySqlConnectOptions>()
            .unwrap()
            .ssl_mode(MySqlSslMode::VerifyCa)
            .ssl_ca(certs_dir.join("root.crt").to_string_lossy().to_string())
            .ssl_client_cert(certs_dir.join("client.crt").to_string_lossy().to_string())
            .ssl_client_key(certs_dir.join("client.key").to_string_lossy().to_string());
        let pool = MySqlPool::connect_with(opts).await.unwrap();
        sqlx::query("SELECT 1").execute(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_mysql_with_tls_verify_ident() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_mysql_integration_test!();
        ensure_rustls_crypto_provider_installed().unwrap();
        let endpoint = std::env::var("GT_MYSQL_ENDPOINTS").unwrap();
        let certs_dir = test_certs_dir();

        let opts = endpoint
            .parse::<MySqlConnectOptions>()
            .unwrap()
            .ssl_mode(MySqlSslMode::VerifyIdentity)
            .ssl_ca(certs_dir.join("root.crt").to_string_lossy().to_string())
            .ssl_client_cert(certs_dir.join("client.crt").to_string_lossy().to_string())
            .ssl_client_key(certs_dir.join("client.key").to_string_lossy().to_string());
        let pool = MySqlPool::connect_with(opts).await.unwrap();
        sqlx::query("SELECT 1").execute(&pool).await.unwrap();
    }
}
