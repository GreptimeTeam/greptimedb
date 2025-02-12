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

mod postgres;

pub use postgres::PgStore;

const RDS_STORE_TXN_RETRY_COUNT: usize = 3;

/// Query executor for rds. It can execute queries or generate a transaction executor.
#[async_trait::async_trait]
pub trait DefaultQueryExecutor: Send + Sync {
    type TxnExecutor<'a>: 'a + TxnQueryExecutor<'a>
    where
        Self: 'a;

    fn name() -> &'static str;

    async fn default_query(&mut self, query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>>;

    /// Some queries don't need to return any result, such as `DELETE`.
    async fn default_execute(&mut self, query: &str, params: &[&Vec<u8>]) -> Result<()> {
        self.default_query(query, params).await?;
        Ok(())
    }

    async fn txn_executor<'a>(&'a mut self) -> Result<Self::TxnExecutor<'a>>;
}

/// Transaction query executor for rds. It can execute queries in transaction or commit the transaction.
#[async_trait::async_trait]
pub trait TxnQueryExecutor<'a>: Send + Sync {
    async fn txn_query(&mut self, query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>>;

    async fn txn_execute(&mut self, query: &str, params: &[&Vec<u8>]) -> Result<()> {
        self.txn_query(query, params).await?;
        Ok(())
    }

    async fn txn_commit(self) -> Result<()>;
}

/// Factory for creating default and transaction query executors.
#[async_trait::async_trait]
pub trait ExecutorFactory<T: DefaultQueryExecutor>: Send + Sync {
    async fn default_executor(&self) -> Result<T>;

    async fn txn_executor<'a>(&self, default_executor: &'a mut T) -> Result<T::TxnExecutor<'a>>;
}

/// Rds backed store for metsrv
pub struct RdsStore<T, S, R>
where
    T: DefaultQueryExecutor + Send + Sync,
    S: ExecutorFactory<T> + Send + Sync,
{
    max_txn_ops: usize,
    txn_retry_count: usize,
    executor_factory: S,
    sql_template_set: R,
    _phantom: PhantomData<T>,
}

pub enum RdsQueryExecutor<'a, T: DefaultQueryExecutor + 'a> {
    Default(T),
    Txn(T::TxnExecutor<'a>),
}

impl<T: DefaultQueryExecutor> RdsQueryExecutor<'_, T> {
    async fn query(&mut self, query: &str, params: &Vec<&Vec<u8>>) -> Result<Vec<KeyValue>> {
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

#[async_trait::async_trait]
pub trait KvQueryExecutor<T: DefaultQueryExecutor> {
    async fn range_with_query_executor(
        &self,
        query_executor: &mut RdsQueryExecutor<'_, T>,
        req: RangeRequest,
    ) -> Result<RangeResponse>;

    async fn put_with_query_executor(
        &self,
        query_executor: &mut RdsQueryExecutor<'_, T>,
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
        query_executor: &mut RdsQueryExecutor<'_, T>,
        req: BatchPutRequest,
    ) -> Result<BatchPutResponse>;

    /// Batch get with certain client. It's needed for a client with transaction.
    async fn batch_get_with_query_executor(
        &self,
        query_executor: &mut RdsQueryExecutor<'_, T>,
        req: BatchGetRequest,
    ) -> Result<BatchGetResponse>;

    async fn delete_range_with_query_executor(
        &self,
        query_executor: &mut RdsQueryExecutor<'_, T>,
        req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse>;

    async fn batch_delete_with_query_executor(
        &self,
        query_executor: &mut RdsQueryExecutor<'_, T>,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse>;
}

impl<T, S, R> RdsStore<T, S, R>
where
    Self: KvQueryExecutor<T> + Send + Sync,
    T: DefaultQueryExecutor + Send + Sync,
    S: ExecutorFactory<T> + Send + Sync,
{
    async fn execute_txn_cmp(
        &self,
        query_executor: &mut RdsQueryExecutor<'_, T>,
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
        query_executor: &mut RdsQueryExecutor<'_, T>,
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
        query_executor: &mut RdsQueryExecutor<'_, T>,
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
        query_executor: &mut RdsQueryExecutor<'_, T>,
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
        query_executor: &mut RdsQueryExecutor<'_, T>,
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
        query_executor: &mut RdsQueryExecutor<'_, T>,
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
        let mut txn_executor = RdsQueryExecutor::Txn(
            self.executor_factory
                .txn_executor(&mut default_executor)
                .await?,
        );
        let mut success = true;
        if txn.c_when {
            success = self
                .execute_txn_cmp(&mut txn_executor, &txn.req.compare)
                .await?;
        }
        let mut responses = vec![];
        if success && txn.c_then {
            match self
                .try_batch_txn(&mut txn_executor, &txn.req.success)
                .await?
            {
                Some(res) => responses.extend(res),
                None => {
                    for txnop in &txn.req.success {
                        let res = self.execute_txn_op(&mut txn_executor, txnop).await?;
                        responses.push(res);
                    }
                }
            }
        } else if !success && txn.c_else {
            match self
                .try_batch_txn(&mut txn_executor, &txn.req.failure)
                .await?
            {
                Some(res) => responses.extend(res),
                None => {
                    for txnop in &txn.req.failure {
                        let res = self.execute_txn_op(&mut txn_executor, txnop).await?;
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
impl<T, S, R> KvBackend for RdsStore<T, S, R>
where
    R: 'static,
    Self: KvQueryExecutor<T> + Send + Sync,
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
        let mut query_executor = RdsQueryExecutor::Default(client);
        self.range_with_query_executor(&mut query_executor, req)
            .await
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let client = self.executor_factory.default_executor().await?;
        let mut query_executor = RdsQueryExecutor::Default(client);
        self.put_with_query_executor(&mut query_executor, req).await
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let client = self.executor_factory.default_executor().await?;
        let mut query_executor = RdsQueryExecutor::Default(client);
        self.batch_put_with_query_executor(&mut query_executor, req)
            .await
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        let client = self.executor_factory.default_executor().await?;
        let mut query_executor = RdsQueryExecutor::Default(client);
        self.batch_get_with_query_executor(&mut query_executor, req)
            .await
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let client = self.executor_factory.default_executor().await?;
        let mut query_executor = RdsQueryExecutor::Default(client);
        self.delete_range_with_query_executor(&mut query_executor, req)
            .await
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        let client = self.executor_factory.default_executor().await?;
        let mut query_executor = RdsQueryExecutor::Default(client);
        self.batch_delete_with_query_executor(&mut query_executor, req)
            .await
    }
}

#[async_trait::async_trait]
impl<T, S, R> TxnService for RdsStore<T, S, R>
where
    Self: KvQueryExecutor<T> + Send + Sync,
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
