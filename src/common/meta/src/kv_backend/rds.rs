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
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use common_telemetry::debug;

use crate::error::{Error, RdsTransactionRetryFailedSnafu, Result};
use crate::kv_backend::txn::{
    Compare, Txn as KvTxn, TxnOp, TxnOpResponse, TxnResponse as KvTxnResponse,
};
use crate::kv_backend::{KvBackend, TxnService};
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
const RDS_STORE_TXN_RETRY_COUNT: usize = 3;

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

/// Generate in placeholders for sql.
fn generate_in_placeholders(from: usize, to: usize) -> Vec<String> {
    (from..=to).map(|i| format!("${}", i)).collect()
}

/// Factory for building sql templates.
struct SqlTemplateFactory<'a> {
    table_name: &'a str,
}

impl<'a> SqlTemplateFactory<'a> {
    /// Creates a new [`SqlTemplateFactory`] with the given table name.
    fn new(table_name: &'a str) -> Self {
        Self { table_name }
    }

    /// Builds the template set for the given table name.
    fn build(&self) -> SqlTemplateSet {
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
        }
    }
}

/// Templates for the given table name.
#[derive(Debug, Clone)]
pub struct SqlTemplateSet {
    table_name: String,
    create_table_statement: String,
    range_template: RangeTemplate,
    delete_template: RangeTemplate,
}

impl SqlTemplateSet {
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

/// Query executor for rds. It can execute queries or generate a transaction executor.
#[async_trait::async_trait]
pub trait DefaultQueryExecutor: Send + Sync {
    type TxnExecutor<'a>: 'a + TxnQueryExecutor<'a>
    where
        Self: 'a;

    fn name() -> &'static str;

    async fn default_query(&self, query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>>;

    async fn txn_executor<'a>(&'a mut self) -> Result<Self::TxnExecutor<'a>>;
}

/// Transaction query executor for rds. It can execute queries in transaction or commit the transaction.
#[async_trait::async_trait]
pub trait TxnQueryExecutor<'a>: Send + Sync {
    async fn txn_query(&self, query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>>;

    async fn txn_commit(self) -> Result<()>;
}

/// Factory for creating default and transaction query executors.
#[async_trait::async_trait]
pub trait ExecutorFactory<T: DefaultQueryExecutor>: Send + Sync {
    async fn default_executor(&self) -> Result<T>;

    async fn txn_executor<'a>(&self, default_executor: &'a mut T) -> Result<T::TxnExecutor<'a>>;
}

/// Rds backed store for metsrv
pub struct RdsStore<T, S>
where
    T: DefaultQueryExecutor + Send + Sync,
    S: ExecutorFactory<T> + Send + Sync,
{
    max_txn_ops: usize,
    sql_template_set: SqlTemplateSet,
    txn_retry_count: usize,
    executor_factory: S,
    _phantom: PhantomData<T>,
}

pub enum RdsQueryExecutor<'a, T: DefaultQueryExecutor + 'a> {
    Default(T),
    Txn(T::TxnExecutor<'a>),
}

impl<T: DefaultQueryExecutor> RdsQueryExecutor<'_, T> {
    async fn query(&self, query: &str, params: &Vec<&Vec<u8>>) -> Result<Vec<KeyValue>> {
        match self {
            Self::Default(executor) => executor.default_query(query, params).await,
            Self::Txn(executor) => executor.txn_query(query, params).await,
        }
    }

    async fn commit(self) -> Result<()> {
        match self {
            Self::Txn(executor) => executor.txn_commit().await,
            _ => Ok(()),
        }
    }
}

impl<T, S> RdsStore<T, S>
where
    T: DefaultQueryExecutor + Send + Sync,
    S: ExecutorFactory<T> + Send + Sync,
{
    async fn range_with_query_executor(
        &self,
        query_executor: &RdsQueryExecutor<'_, T>,
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
        let mut kvs = query_executor.query(&query, &params_ref).await?;
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

    async fn put_with_query_executor(
        &self,
        query_executor: &RdsQueryExecutor<'_, T>,
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
        query_executor: &RdsQueryExecutor<'_, T>,
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
        let kvs = query_executor.query(&query, &params).await?;
        if req.prev_kv {
            Ok(BatchPutResponse { prev_kvs: kvs })
        } else {
            Ok(BatchPutResponse::default())
        }
    }

    /// Batch get with certain client. It's needed for a client with transaction.
    async fn batch_get_with_query_executor(
        &self,
        query_executor: &RdsQueryExecutor<'_, T>,
        req: BatchGetRequest,
    ) -> Result<BatchGetResponse> {
        if req.keys.is_empty() {
            return Ok(BatchGetResponse { kvs: vec![] });
        }
        let query = self
            .sql_template_set
            .generate_batch_get_query(req.keys.len());
        let params = req.keys.iter().map(|x| x as _).collect::<Vec<_>>();
        let kvs = query_executor.query(&query, &params).await?;
        Ok(BatchGetResponse { kvs })
    }

    async fn delete_range_with_query_executor(
        &self,
        query_executor: &RdsQueryExecutor<'_, T>,
        req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse> {
        let template_type = range_template(&req.key, &req.range_end);
        let template = self.sql_template_set.delete_template.get(template_type);
        let params = template_type.build_params(req.key, req.range_end);
        let params_ref = params.iter().map(|x| x as _).collect::<Vec<_>>();
        let kvs = query_executor.query(template, &params_ref).await?;
        let mut resp = DeleteRangeResponse::new(kvs.len() as i64);
        if req.prev_kv {
            resp.with_prev_kvs(kvs);
        }
        Ok(resp)
    }

    async fn batch_delete_with_query_executor(
        &self,
        query_executor: &RdsQueryExecutor<'_, T>,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse> {
        if req.keys.is_empty() {
            return Ok(BatchDeleteResponse::default());
        }
        let query = self
            .sql_template_set
            .generate_batch_delete_query(req.keys.len());
        let params = req.keys.iter().map(|x| x as _).collect::<Vec<_>>();
        let kvs = query_executor.query(&query, &params).await?;
        if req.prev_kv {
            Ok(BatchDeleteResponse { prev_kvs: kvs })
        } else {
            Ok(BatchDeleteResponse::default())
        }
    }

    async fn execute_txn_cmp(
        &self,
        query_executor: &RdsQueryExecutor<'_, T>,
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
        query_executor: &RdsQueryExecutor<'_, T>,
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
        query_executor: &RdsQueryExecutor<'_, T>,
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
        query_executor: &RdsQueryExecutor<'_, T>,
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
        query_executor: &RdsQueryExecutor<'_, T>,
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
        query_executor: &RdsQueryExecutor<'_, T>,
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
        let mut default_executor = self.executor_factory.default_executor().await?;
        let txn_executor = RdsQueryExecutor::Txn(
            self.executor_factory
                .txn_executor(&mut default_executor)
                .await?,
        );
        let mut success = true;
        if txn.c_when {
            success = self
                .execute_txn_cmp(&txn_executor, &txn.req.compare)
                .await?;
        }
        let mut responses = vec![];
        if success && txn.c_then {
            match self.try_batch_txn(&txn_executor, &txn.req.success).await? {
                Some(res) => responses.extend(res),
                None => {
                    for txnop in &txn.req.success {
                        let res = self.execute_txn_op(&txn_executor, txnop).await?;
                        responses.push(res);
                    }
                }
            }
        } else if !success && txn.c_else {
            match self.try_batch_txn(&txn_executor, &txn.req.failure).await? {
                Some(res) => responses.extend(res),
                None => {
                    for txnop in &txn.req.failure {
                        let res = self.execute_txn_op(&txn_executor, txnop).await?;
                        responses.push(res);
                    }
                }
            }
        }

        txn_executor.commit().await?;
        Ok(KvTxnResponse {
            responses,
            succeeded: success,
        })
    }
}

#[async_trait::async_trait]
impl<T, S> KvBackend for RdsStore<T, S>
where
    T: DefaultQueryExecutor + 'static,
    S: ExecutorFactory<T> + 'static,
{
    fn name(&self) -> &str {
        T::name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        let client = self.executor_factory.default_executor().await?;
        let query_executor = RdsQueryExecutor::Default(client);
        self.range_with_query_executor(&query_executor, req).await
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let client = self.executor_factory.default_executor().await?;
        let query_executor = RdsQueryExecutor::Default(client);
        self.put_with_query_executor(&query_executor, req).await
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let client = self.executor_factory.default_executor().await?;
        let query_executor = RdsQueryExecutor::Default(client);
        self.batch_put_with_query_executor(&query_executor, req)
            .await
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        let client = self.executor_factory.default_executor().await?;
        let query_executor = RdsQueryExecutor::Default(client);
        self.batch_get_with_query_executor(&query_executor, req)
            .await
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let client = self.executor_factory.default_executor().await?;
        let query_executor = RdsQueryExecutor::Default(client);
        self.delete_range_with_query_executor(&query_executor, req)
            .await
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        let client = self.executor_factory.default_executor().await?;
        let query_executor = RdsQueryExecutor::Default(client);
        self.batch_delete_with_query_executor(&query_executor, req)
            .await
    }
}

#[async_trait::async_trait]
impl<T, S> TxnService for RdsStore<T, S>
where
    T: DefaultQueryExecutor + 'static,
    S: ExecutorFactory<T> + 'static,
{
    type Error = Error;

    async fn txn(&self, txn: KvTxn) -> Result<KvTxnResponse> {
        let _timer = METRIC_META_TXN_REQUEST
            .with_label_values(&[T::name(), "txn"])
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

        RdsTransactionRetryFailedSnafu {}.fail()
    }

    fn max_txn_ops(&self) -> usize {
        self.max_txn_ops
    }
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
