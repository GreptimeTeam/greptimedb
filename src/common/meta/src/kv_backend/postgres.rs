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

use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use common_telemetry::debug;
use deadpool_postgres::{Config, Pool, Runtime};
use snafu::ResultExt;
use tokio_postgres::types::ToSql;
use tokio_postgres::{IsolationLevel, NoTls, Row};

use crate::error::{
    CreatePostgresPoolSnafu, Error, GetPostgresConnectionSnafu, PostgresExecutionSnafu,
    PostgresTransactionRetryFailedSnafu, PostgresTransactionSnafu, Result,
};
use crate::kv_backend::txn::{
    Compare, Txn as KvTxn, TxnOp, TxnOpResponse, TxnResponse as KvTxnResponse,
};
use crate::kv_backend::{KvBackend, KvBackendRef, TxnService};
use crate::metrics::METRIC_META_TXN_REQUEST;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

type PgClient = deadpool::managed::Object<deadpool_postgres::Manager>;

enum PgQueryExecutor<'a> {
    Client(PgClient),
    Transaction(deadpool_postgres::Transaction<'a>),
}

impl PgQueryExecutor<'_> {
    async fn query(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>> {
        match self {
            PgQueryExecutor::Client(client) => {
                let stmt = client
                    .prepare_cached(query)
                    .await
                    .context(PostgresExecutionSnafu { sql: query })?;
                client
                    .query(&stmt, params)
                    .await
                    .context(PostgresExecutionSnafu { sql: query })
            }
            PgQueryExecutor::Transaction(txn) => {
                let stmt = txn
                    .prepare_cached(query)
                    .await
                    .context(PostgresExecutionSnafu { sql: query })?;
                txn.query(&stmt, params)
                    .await
                    .context(PostgresExecutionSnafu { sql: query })
            }
        }
    }

    async fn commit(self) -> Result<()> {
        match self {
            PgQueryExecutor::Client(_) => Ok(()),
            PgQueryExecutor::Transaction(txn) => {
                txn.commit().await.context(PostgresTransactionSnafu {
                    operation: "commit".to_string(),
                })
            }
        }
    }
}

const PG_STORE_TXN_RETRY_COUNT: usize = 3;

/// Posgres backend store for metasrv
pub struct PgStore<T> {
    pool: Pool,
    max_txn_ops: usize,
    sql_template_set: SqlTemplateSet<T>,
    txn_retry_count: usize,
}

const EMPTY: &[u8] = &[0];

/// Factory for building sql templates.
struct SqlTemplateFactory<'a, T> {
    table_name: &'a str,
    _phantom: PhantomData<T>,
}

impl<'a, T> SqlTemplateFactory<'a, T> {
    /// Creates a new [`SqlTemplateFactory`] with the given table name.
    fn new(table_name: &'a str) -> Self {
        Self {
            table_name,
            _phantom: PhantomData,
        }
    }

    /// Builds the template set for the given table name.
    fn build(
        &self,
        key_value_from_row: fn(Row) -> T,
        key_value_from_row_key_only: fn(Row) -> T,
    ) -> SqlTemplateSet<T> {
        let table_name = self.table_name;
        SqlTemplateSet {
            table_name: table_name.to_string(),
            create_table_statement: format!(
                "CREATE TABLE IF NOT EXISTS {table_name}(k bytea PRIMARY KEY, v bytea)",
            ),
            range_template: RangeTemplate {
                point: format!("SELECT k, v FROM {table_name} WHERE k = $1"),
                range: format!("SELECT k, v FROM {table_name} WHERE k >= $1 AND k < $2 ORDER BY k"),
                full: format!("SELECT k, v FROM {table_name} $1 ORDER BY k"),
                left_bounded: format!("SELECT k, v FROM {table_name} WHERE k >= $1 ORDER BY k"),
                prefix: format!("SELECT k, v FROM {table_name} WHERE k LIKE $1 ORDER BY k"),
            },
            delete_template: RangeTemplate {
                point: format!("DELETE FROM {table_name} WHERE k = $1 RETURNING k,v;"),
                range: format!("DELETE FROM {table_name} WHERE k >= $1 AND k < $2 RETURNING k,v;"),
                full: format!("DELETE FROM {table_name} RETURNING k,v"),
                left_bounded: format!("DELETE FROM {table_name} WHERE k >= $1 RETURNING k,v;"),
                prefix: format!("DELETE FROM {table_name} WHERE k LIKE $1 RETURNING k,v;"),
            },
            key_value_from_row,
            key_value_from_row_key_only,
        }
    }
}

/// Templates for the given table name.
#[derive(Debug, Clone)]
pub struct SqlTemplateSet<T> {
    table_name: String,
    create_table_statement: String,
    range_template: RangeTemplate,
    delete_template: RangeTemplate,
    key_value_from_row: fn(Row) -> T,
    key_value_from_row_key_only: fn(Row) -> T,
}

impl<T: Sync + Send> SqlTemplateSet<T> {
    /// Converts a row to a [`KeyValue`] with options.
    fn key_value_from_row_with_opts(&self, keys_only: bool) -> impl Fn(Row) -> T {
        if keys_only {
            self.key_value_from_row_key_only
        } else {
            self.key_value_from_row
        }
    }

    /// Generates the sql for batch get.
    fn generate_batch_get_query(&self, key_len: usize) -> String {
        let table_name = &self.table_name;
        let in_clause = generate_in_placeholders(1, key_len).join(", ");
        format!("SELECT k, v FROM {table_name} WHERE k in ({});", in_clause)
    }

    /// Generates the sql for batch delete.
    fn generate_batch_delete_query(&self, key_len: usize) -> String {
        let table_name = &self.table_name;
        let in_clause = generate_in_placeholders(1, key_len).join(", ");
        format!(
            "DELETE FROM {table_name} WHERE k in ({}) RETURNING k,v;",
            in_clause
        )
    }

    /// Generates the sql for batch upsert.
    fn generate_batch_upsert_query(&self, kv_len: usize) -> String {
        let table_name = &self.table_name;
        let in_placeholders: Vec<String> = (1..=kv_len).map(|i| format!("${}", i)).collect();
        let in_clause = in_placeholders.join(", ");
        let mut param_index = kv_len + 1;
        let mut values_placeholders = Vec::new();
        for _ in 0..kv_len {
            values_placeholders.push(format!("(${0}, ${1})", param_index, param_index + 1));
            param_index += 2;
        }
        let values_clause = values_placeholders.join(", ");

        format!(
            r#"
    WITH prev AS (
        SELECT k,v FROM {table_name} WHERE k IN ({in_clause})
    ), update AS (
    INSERT INTO {table_name} (k, v) VALUES
        {values_clause}
    ON CONFLICT (
        k
    ) DO UPDATE SET
        v = excluded.v
    )

    SELECT k, v FROM prev;
    "#
        )
    }
}

/// Default sql template set for [`KeyValue`].
pub type DefaultSqlTemplateSet = SqlTemplateSet<KeyValue>;

/// Default pg store for [`KeyValue`].
pub type DefaultPgStore = PgStore<KeyValue>;

impl<T> PgStore<T> {
    async fn client(&self) -> Result<PgClient> {
        match self.pool.get().await {
            Ok(client) => Ok(client),
            Err(e) => GetPostgresConnectionSnafu {
                reason: e.to_string(),
            }
            .fail(),
        }
    }

    async fn client_executor(&self) -> Result<PgQueryExecutor<'_>> {
        let client = self.client().await?;
        Ok(PgQueryExecutor::Client(client))
    }

    async fn txn_executor<'a>(&self, client: &'a mut PgClient) -> Result<PgQueryExecutor<'a>> {
        let txn = client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .context(PostgresTransactionSnafu {
                operation: "start".to_string(),
            })?;
        Ok(PgQueryExecutor::Transaction(txn))
    }
}

impl DefaultPgStore {
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
        let sql_template_set =
            template_factory.build(key_value_from_row, key_value_from_row_key_only);
        client
            .execute(&sql_template_set.create_table_statement, &[])
            .await
            .with_context(|_| PostgresExecutionSnafu {
                sql: sql_template_set.create_table_statement.to_string(),
            })?;
        Ok(Arc::new(Self {
            pool,
            max_txn_ops,
            sql_template_set,
            txn_retry_count: PG_STORE_TXN_RETRY_COUNT,
        }))
    }
}

/// Type of range template.
#[derive(Debug, Clone, Copy)]
enum RangeTemplateType {
    Point,
    Range,
    Full,
    LeftBounded,
    Prefix,
}

/// Builds params for the given range template type.
impl RangeTemplateType {
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

/// Generate in placeholders for sql.
fn generate_in_placeholders(from: usize, to: usize) -> Vec<String> {
    (from..=to).map(|i| format!("${}", i)).collect()
}

#[async_trait::async_trait]
impl KvBackend for DefaultPgStore {
    fn name(&self) -> &str {
        "Postgres"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        let client = self.client_executor().await?;
        self.range_with_query_executor(&client, req).await
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let client = self.client_executor().await?;
        self.put_with_query_executor(&client, req).await
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let client = self.client_executor().await?;
        self.batch_put_with_query_executor(&client, req).await
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        let client = self.client_executor().await?;
        self.batch_get_with_query_executor(&client, req).await
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let client = self.client_executor().await?;
        self.delete_range_with_query_executor(&client, req).await
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        let client = self.client_executor().await?;
        self.batch_delete_with_query_executor(&client, req).await
    }
}

/// Converts a row to a [`KeyValue`] with key only.
fn key_value_from_row_key_only(r: Row) -> KeyValue {
    KeyValue {
        key: r.get(0),
        value: vec![],
    }
}

/// Converts a row to a [`KeyValue`].
fn key_value_from_row(r: Row) -> KeyValue {
    KeyValue {
        key: r.get(0),
        value: r.get(1),
    }
}

impl DefaultPgStore {
    async fn range_with_query_executor(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        req: RangeRequest,
    ) -> Result<RangeResponse> {
        let template_type = range_template(&req.key, &req.range_end);
        let template = self.sql_template_set.range_template.get(template_type);
        let params = template_type.build_params(req.key, req.range_end);
        let params_ref = params.iter().map(|x| x as _).collect::<Vec<_>>();
        // Always add 1 to limit to check if there is more data
        let query =
            RangeTemplate::with_limit(template, if req.limit == 0 { 0 } else { req.limit + 1 });
        let limit = req.limit as usize;
        debug!("query: {:?}, params: {:?}", query, params);
        let res = query_executor.query(&query, &params_ref).await?;
        let mut kvs: Vec<KeyValue> = res
            .into_iter()
            .map(
                self.sql_template_set
                    .key_value_from_row_with_opts(req.keys_only),
            )
            .collect();
        // If limit is 0, we always return all data
        if limit == 0 || kvs.len() <= limit {
            return Ok(RangeResponse { kvs, more: false });
        }
        // If limit is greater than the number of rows, we remove the last row and set more to true
        let removed = kvs.pop();
        debug_assert!(removed.is_some());
        Ok(RangeResponse { kvs, more: true })
    }

    async fn put_with_query_executor(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        req: PutRequest,
    ) -> Result<PutResponse> {
        let kv = KeyValue {
            key: req.key,
            value: req.value,
        };
        let mut res = self
            .batch_put_with_query_executor(
                query_executor,
                BatchPutRequest {
                    kvs: vec![kv],
                    prev_kv: req.prev_kv,
                },
            )
            .await?;

        if !res.prev_kvs.is_empty() {
            debug_assert!(req.prev_kv);
            return Ok(PutResponse {
                prev_kv: Some(res.prev_kvs.remove(0)),
            });
        }
        Ok(PutResponse::default())
    }

    async fn batch_put_with_query_executor(
        &self,
        query_executor: &PgQueryExecutor<'_>,
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
        in_params.extend(values_params);
        let params = in_params.iter().map(|x| x as _).collect::<Vec<_>>();
        let query = self
            .sql_template_set
            .generate_batch_upsert_query(req.kvs.len());
        let res = query_executor.query(&query, &params).await?;
        if req.prev_kv {
            Ok(BatchPutResponse {
                prev_kvs: res
                    .into_iter()
                    .map(self.sql_template_set.key_value_from_row)
                    .collect(),
            })
        } else {
            Ok(BatchPutResponse::default())
        }
    }

    /// Batch get with certain client. It's needed for a client with transaction.
    async fn batch_get_with_query_executor(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        req: BatchGetRequest,
    ) -> Result<BatchGetResponse> {
        if req.keys.is_empty() {
            return Ok(BatchGetResponse { kvs: vec![] });
        }
        let query = self
            .sql_template_set
            .generate_batch_get_query(req.keys.len());
        let params = req.keys.iter().map(|x| x as _).collect::<Vec<_>>();
        let res = query_executor.query(&query, &params).await?;
        Ok(BatchGetResponse {
            kvs: res
                .into_iter()
                .map(self.sql_template_set.key_value_from_row)
                .collect(),
        })
    }

    async fn delete_range_with_query_executor(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse> {
        let template_type = range_template(&req.key, &req.range_end);
        let template = self.sql_template_set.delete_template.get(template_type);
        let params = template_type.build_params(req.key, req.range_end);
        let params_ref = params.iter().map(|x| x as _).collect::<Vec<_>>();
        let res = query_executor.query(template, &params_ref).await?;
        let mut resp = DeleteRangeResponse::new(res.len() as i64);
        if req.prev_kv {
            resp.with_prev_kvs(
                res.into_iter()
                    .map(self.sql_template_set.key_value_from_row)
                    .collect(),
            );
        }
        Ok(resp)
    }

    async fn batch_delete_with_query_executor(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse> {
        if req.keys.is_empty() {
            return Ok(BatchDeleteResponse::default());
        }
        let query = self
            .sql_template_set
            .generate_batch_delete_query(req.keys.len());
        let params = req.keys.iter().map(|x| x as _).collect::<Vec<_>>();
        let res = query_executor.query(&query, &params).await?;
        if req.prev_kv {
            Ok(BatchDeleteResponse {
                prev_kvs: res
                    .into_iter()
                    .map(self.sql_template_set.key_value_from_row)
                    .collect(),
            })
        } else {
            Ok(BatchDeleteResponse::default())
        }
    }

    async fn execute_txn_cmp(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        cmp: &[Compare],
    ) -> Result<bool> {
        let batch_get_req = BatchGetRequest {
            keys: cmp.iter().map(|c| c.key.clone()).collect(),
        };
        let res = self
            .batch_get_with_query_executor(query_executor, batch_get_req)
            .await?;
        debug!("batch get res: {:?}", res);
        let res_map = res
            .kvs
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<HashMap<Vec<u8>, Vec<u8>>>();
        for c in cmp {
            let value = res_map.get(&c.key);
            if !c.compare_value(value) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Execute a batch of transaction operations. This function is only used for transactions with the same operation type.
    async fn try_batch_txn(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        txn_ops: &[TxnOp],
    ) -> Result<Option<Vec<TxnOpResponse>>> {
        if !check_txn_ops(txn_ops)? {
            return Ok(None);
        }
        // Safety: txn_ops is not empty
        match txn_ops.first().unwrap() {
            TxnOp::Delete(_) => self.handle_batch_delete(query_executor, txn_ops).await,
            TxnOp::Put(_, _) => self.handle_batch_put(query_executor, txn_ops).await,
            TxnOp::Get(_) => self.handle_batch_get(query_executor, txn_ops).await,
        }
    }

    async fn handle_batch_delete(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        txn_ops: &[TxnOp],
    ) -> Result<Option<Vec<TxnOpResponse>>> {
        let mut batch_del_req = BatchDeleteRequest {
            keys: vec![],
            prev_kv: true,
        };
        for op in txn_ops {
            if let TxnOp::Delete(key) = op {
                batch_del_req.keys.push(key.clone());
            }
        }
        let res = self
            .batch_delete_with_query_executor(query_executor, batch_del_req)
            .await?;
        let res_map = res
            .prev_kvs
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<HashMap<Vec<u8>, Vec<u8>>>();
        let mut resps = Vec::with_capacity(txn_ops.len());
        for op in txn_ops {
            if let TxnOp::Delete(key) = op {
                let value = res_map.get(key);
                resps.push(TxnOpResponse::ResponseDelete(DeleteRangeResponse {
                    deleted: if value.is_some() { 1 } else { 0 },
                    prev_kvs: vec![],
                }));
            }
        }
        Ok(Some(resps))
    }

    async fn handle_batch_put(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        txn_ops: &[TxnOp],
    ) -> Result<Option<Vec<TxnOpResponse>>> {
        let mut batch_put_req = BatchPutRequest {
            kvs: vec![],
            prev_kv: false,
        };
        for op in txn_ops {
            if let TxnOp::Put(key, value) = op {
                batch_put_req.kvs.push(KeyValue {
                    key: key.clone(),
                    value: value.clone(),
                });
            }
        }
        let _ = self
            .batch_put_with_query_executor(query_executor, batch_put_req)
            .await?;
        let mut resps = Vec::with_capacity(txn_ops.len());
        for op in txn_ops {
            if let TxnOp::Put(_, _) = op {
                resps.push(TxnOpResponse::ResponsePut(PutResponse { prev_kv: None }));
            }
        }
        Ok(Some(resps))
    }

    async fn handle_batch_get(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        txn_ops: &[TxnOp],
    ) -> Result<Option<Vec<TxnOpResponse>>> {
        let mut batch_get_req = BatchGetRequest { keys: vec![] };
        for op in txn_ops {
            if let TxnOp::Get(key) = op {
                batch_get_req.keys.push(key.clone());
            }
        }
        let res = self
            .batch_get_with_query_executor(query_executor, batch_get_req)
            .await?;
        let res_map = res
            .kvs
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<HashMap<Vec<u8>, Vec<u8>>>();
        let mut resps = Vec::with_capacity(txn_ops.len());
        for op in txn_ops {
            if let TxnOp::Get(key) = op {
                let value = res_map.get(key);
                resps.push(TxnOpResponse::ResponseGet(RangeResponse {
                    kvs: value
                        .map(|v| {
                            vec![KeyValue {
                                key: key.clone(),
                                value: v.clone(),
                            }]
                        })
                        .unwrap_or_default(),
                    more: false,
                }));
            }
        }
        Ok(Some(resps))
    }

    async fn execute_txn_op(
        &self,
        query_executor: &PgQueryExecutor<'_>,
        op: &TxnOp,
    ) -> Result<TxnOpResponse> {
        match op {
            TxnOp::Put(key, value) => {
                let res = self
                    .put_with_query_executor(
                        query_executor,
                        PutRequest {
                            key: key.clone(),
                            value: value.clone(),
                            prev_kv: false,
                        },
                    )
                    .await?;
                Ok(TxnOpResponse::ResponsePut(res))
            }
            TxnOp::Get(key) => {
                let res = self
                    .range_with_query_executor(
                        query_executor,
                        RangeRequest {
                            key: key.clone(),
                            range_end: vec![],
                            limit: 1,
                            keys_only: false,
                        },
                    )
                    .await?;
                Ok(TxnOpResponse::ResponseGet(res))
            }
            TxnOp::Delete(key) => {
                let res = self
                    .delete_range_with_query_executor(
                        query_executor,
                        DeleteRangeRequest {
                            key: key.clone(),
                            range_end: vec![],
                            prev_kv: false,
                        },
                    )
                    .await?;
                Ok(TxnOpResponse::ResponseDelete(res))
            }
        }
    }

    async fn txn_inner(&self, txn: &KvTxn) -> Result<KvTxnResponse> {
        let mut client = self.client().await?;
        let pg_txn = self.txn_executor(&mut client).await?;
        let mut success = true;
        if txn.c_when {
            success = self.execute_txn_cmp(&pg_txn, &txn.req.compare).await?;
        }
        let mut responses = vec![];
        if success && txn.c_then {
            match self.try_batch_txn(&pg_txn, &txn.req.success).await? {
                Some(res) => responses.extend(res),
                None => {
                    for txnop in &txn.req.success {
                        let res = self.execute_txn_op(&pg_txn, txnop).await?;
                        responses.push(res);
                    }
                }
            }
        } else if !success && txn.c_else {
            match self.try_batch_txn(&pg_txn, &txn.req.failure).await? {
                Some(res) => responses.extend(res),
                None => {
                    for txnop in &txn.req.failure {
                        let res = self.execute_txn_op(&pg_txn, txnop).await?;
                        responses.push(res);
                    }
                }
            }
        }

        pg_txn.commit().await?;
        Ok(KvTxnResponse {
            responses,
            succeeded: success,
        })
    }
}

#[async_trait::async_trait]
impl TxnService for DefaultPgStore {
    type Error = Error;

    async fn txn(&self, txn: KvTxn) -> Result<KvTxnResponse> {
        let _timer = METRIC_META_TXN_REQUEST
            .with_label_values(&["postgres", "txn"])
            .start_timer();

        let mut backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_millis(200))
            .with_max_times(self.txn_retry_count)
            .build();

        loop {
            match self.txn_inner(&txn).await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    if e.is_serialization_error() {
                        let d = backoff.next();
                        if let Some(d) = d {
                            tokio::time::sleep(d).await;
                            continue;
                        }
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        PostgresTransactionRetryFailedSnafu {}.fail()
    }

    fn max_txn_ops(&self) -> usize {
        self.max_txn_ops
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

/// Checks if the transaction operations are the same type.
fn check_txn_ops(txn_ops: &[TxnOp]) -> Result<bool> {
    if txn_ops.is_empty() {
        return Ok(false);
    }
    let same = txn_ops.windows(2).all(|a| {
        matches!(
            (&a[0], &a[1]),
            (TxnOp::Put(_, _), TxnOp::Put(_, _))
                | (TxnOp::Get(_), TxnOp::Get(_))
                | (TxnOp::Delete(_), TxnOp::Delete(_))
        )
    });
    Ok(same)
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

    async fn build_pg_kv_backend(table_name: &str) -> Option<DefaultPgStore> {
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
        let sql_templates = template_factory.build(key_value_from_row, key_value_from_row_key_only);
        client
            .execute(&sql_templates.create_table_statement, &[])
            .await
            .context(PostgresExecutionSnafu {
                sql: sql_templates.create_table_statement.to_string(),
            })
            .unwrap();
        Some(PgStore {
            pool,
            max_txn_ops: 128,
            sql_template_set: sql_templates,
            txn_retry_count: PG_STORE_TXN_RETRY_COUNT,
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
