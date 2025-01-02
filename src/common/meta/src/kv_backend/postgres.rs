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
use std::borrow::Cow;
use std::sync::Arc;

use deadpool_postgres::{Config, Pool, Runtime};
use snafu::ResultExt;
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;

use crate::error::{
    CreatePostgresPoolSnafu, Error, GetPostgresConnectionSnafu, PostgresExecutionSnafu, Result,
    StrFromUtf8Snafu,
};
use crate::kv_backend::txn::{Txn as KvTxn, TxnResponse as KvTxnResponse};
use crate::kv_backend::{KvBackend, KvBackendRef, TxnService};
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

/// Posgres backend store for metasrv
pub struct PgStore {
    pool: Pool,
}

const EMPTY: &[u8] = &[0];

// TODO: allow users to configure metadata table name.
const METADKV_CREATION: &str =
    "CREATE TABLE IF NOT EXISTS greptime_metakv(k varchar PRIMARY KEY, v varchar)";

const FULL_TABLE_SCAN: &str = "SELECT k, v FROM greptime_metakv $1 ORDER BY K";

const POINT_GET: &str = "SELECT k, v FROM greptime_metakv WHERE k = $1";

const PREFIX_SCAN: &str = "SELECT k, v FROM greptime_metakv WHERE k LIKE $1 ORDER BY K";

const RANGE_SCAN_LEFT_BOUNDED: &str = "SELECT k, v FROM greptime_metakv WHERE k >= $1 ORDER BY K";

const RANGE_SCAN_FULL_RANGE: &str =
    "SELECT k, v FROM greptime_metakv WHERE k >= $1 AND K < $2 ORDER BY K";

const FULL_TABLE_DELETE: &str = "DELETE FROM greptime_metakv RETURNING k,v";

const POINT_DELETE: &str = "DELETE FROM greptime_metakv WHERE K = $1 RETURNING k,v;";

const PREFIX_DELETE: &str = "DELETE FROM greptime_metakv WHERE k LIKE $1 RETURNING k,v;";

const RANGE_DELETE_LEFT_BOUNDED: &str = "DELETE FROM greptime_metakv WHERE k >= $1 RETURNING k,v;";

const RANGE_DELETE_FULL_RANGE: &str =
    "DELETE FROM greptime_metakv WHERE k >= $1 AND K < $2 RETURNING k,v;";

const CAS: &str = r#"
WITH prev AS (
    SELECT k,v FROM greptime_metakv WHERE k = $1 AND v = $2
), update AS (
UPDATE greptime_metakv
SET k=$1,
v=$2
WHERE 
    k=$1 AND v=$3
)

SELECT k, v FROM prev;
"#;

const PUT_IF_NOT_EXISTS: &str = r#"    
WITH prev AS (
    select k,v from greptime_metakv where k = $1
), insert AS (
    INSERT INTO greptime_metakv
    VALUES ($1, $2)
    ON CONFLICT (k) DO NOTHING
)

SELECT k, v FROM prev;"#;

impl PgStore {
    /// Create pgstore impl of KvBackendRef from url.
    pub async fn with_url(url: &str) -> Result<KvBackendRef> {
        let mut cfg = Config::new();
        cfg.url = Some(url.to_string());
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(CreatePostgresPoolSnafu)?;
        Self::with_pg_pool(pool).await
    }

    /// Create pgstore impl of KvBackendRef from tokio-postgres client.
    pub async fn with_pg_pool(pool: Pool) -> Result<KvBackendRef> {
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
        client
            .execute(METADKV_CREATION, &[])
            .await
            .context(PostgresExecutionSnafu)?;
        Ok(Arc::new(Self { pool }))
    }

    async fn get_client(&self) -> Result<deadpool::managed::Object<deadpool_postgres::Manager>> {
        match self.pool.get().await {
            Ok(client) => Ok(client),
            Err(e) => GetPostgresConnectionSnafu {
                reason: e.to_string(),
            }
            .fail(),
        }
    }

    async fn put_if_not_exists(&self, key: &str, value: &str) -> Result<bool> {
        let res = self
            .get_client()
            .await?
            .query(PUT_IF_NOT_EXISTS, &[&key, &value])
            .await
            .context(PostgresExecutionSnafu)?;
        Ok(res.is_empty())
    }
}

fn select_range_template(req: &RangeRequest) -> &str {
    if req.range_end.is_empty() {
        return POINT_GET;
    }
    if req.key == EMPTY && req.range_end == EMPTY {
        FULL_TABLE_SCAN
    } else if req.range_end == EMPTY {
        RANGE_SCAN_LEFT_BOUNDED
    } else if is_prefix_range(&req.key, &req.range_end) {
        PREFIX_SCAN
    } else {
        RANGE_SCAN_FULL_RANGE
    }
}

fn select_range_delete_template(req: &DeleteRangeRequest) -> &str {
    if req.range_end.is_empty() {
        return POINT_DELETE;
    }
    if req.key == EMPTY && req.range_end == EMPTY {
        FULL_TABLE_DELETE
    } else if req.range_end == EMPTY {
        RANGE_DELETE_LEFT_BOUNDED
    } else if is_prefix_range(&req.key, &req.range_end) {
        PREFIX_DELETE
    } else {
        RANGE_DELETE_FULL_RANGE
    }
}

// Generate dynamic parameterized sql for batch get.
fn generate_batch_get_query(key_len: usize) -> String {
    let in_placeholders: Vec<String> = (1..=key_len).map(|i| format!("${}", i)).collect();
    let in_clause = in_placeholders.join(", ");
    format!(
        "SELECT k, v FROM greptime_metakv WHERE k in ({});",
        in_clause
    )
}

// Generate dynamic parameterized sql for batch delete.
fn generate_batch_delete_query(key_len: usize) -> String {
    let in_placeholders: Vec<String> = (1..=key_len).map(|i| format!("${}", i)).collect();
    let in_clause = in_placeholders.join(", ");
    format!(
        "DELETE FROM greptime_metakv WHERE k in ({}) RETURNING k, v;",
        in_clause
    )
}

// Generate dynamic parameterized sql for batch upsert.
fn generate_batch_upsert_query(kv_len: usize) -> String {
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
        SELECT k,v FROM greptime_metakv WHERE k IN ({in_clause})
    ), update AS (
    INSERT INTO greptime_metakv (k, v) VALUES
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

//  Trim null byte at the end and convert bytes to string.
fn process_bytes<'a>(data: &'a [u8], name: &str) -> Result<&'a str> {
    let mut len = data.len();
    // remove trailing null bytes to avoid error in postgres encoding.
    while len > 0 && data[len - 1] == 0 {
        len -= 1;
    }
    let res = std::str::from_utf8(&data[0..len]).context(StrFromUtf8Snafu { name })?;
    Ok(res)
}

#[async_trait::async_trait]
impl KvBackend for PgStore {
    fn name(&self) -> &str {
        "Postgres"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        let mut params = vec![];
        let template = select_range_template(&req);
        if req.key != EMPTY {
            let key = process_bytes(&req.key, "rangeKey")?;
            if template == PREFIX_SCAN {
                let prefix = format!("{key}%");
                params.push(Cow::Owned(prefix))
            } else {
                params.push(Cow::Borrowed(key))
            }
        }
        if template == RANGE_SCAN_FULL_RANGE && req.range_end != EMPTY {
            let range_end = process_bytes(&req.range_end, "rangeEnd")?;
            params.push(Cow::Borrowed(range_end));
        }
        let limit = req.limit as usize;
        let limit_cause = match limit > 0 {
            true => format!(" LIMIT {};", limit + 1),
            false => ";".to_string(),
        };
        let template = format!("{}{}", template, limit_cause);
        let params: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|x| match x {
                Cow::Borrowed(borrowed) => borrowed as &(dyn ToSql + Sync),
                Cow::Owned(owned) => owned as &(dyn ToSql + Sync),
            })
            .collect();
        let res = self
            .get_client()
            .await?
            .query(&template, &params)
            .await
            .context(PostgresExecutionSnafu)?;
        let kvs: Vec<KeyValue> = res
            .into_iter()
            .map(|r| {
                let key: String = r.get(0);
                if req.keys_only {
                    return KeyValue {
                        key: key.into_bytes(),
                        value: vec![],
                    };
                }
                let value: String = r.get(1);
                KeyValue {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                }
            })
            .collect();
        if limit == 0 || limit > kvs.len() {
            return Ok(RangeResponse { kvs, more: false });
        }
        let (filtered_kvs, _) = kvs.split_at(limit);
        Ok(RangeResponse {
            kvs: filtered_kvs.to_vec(),
            more: kvs.len() > limit,
        })
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let kv = KeyValue {
            key: req.key,
            value: req.value,
        };
        let mut res = self
            .batch_put(BatchPutRequest {
                kvs: vec![kv],
                prev_kv: req.prev_kv,
            })
            .await?;

        if !res.prev_kvs.is_empty() {
            return Ok(PutResponse {
                prev_kv: Some(res.prev_kvs.remove(0)),
            });
        }
        Ok(PutResponse { prev_kv: None })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let mut in_params = Vec::with_capacity(req.kvs.len());
        let mut values_params = Vec::with_capacity(req.kvs.len() * 2);

        for kv in &req.kvs {
            let processed_key = process_bytes(&kv.key, "BatchPutRequestKey")?;
            in_params.push(processed_key);

            let processed_value = process_bytes(&kv.value, "BatchPutRequestValue")?;
            values_params.push(processed_key);
            values_params.push(processed_value);
        }
        in_params.extend(values_params);
        let params: Vec<&(dyn ToSql + Sync)> =
            in_params.iter().map(|x| x as &(dyn ToSql + Sync)).collect();

        let query = generate_batch_upsert_query(req.kvs.len());

        let res = self
            .get_client()
            .await?
            .query(&query, &params)
            .await
            .context(PostgresExecutionSnafu)?;
        if req.prev_kv {
            let kvs: Vec<KeyValue> = res
                .into_iter()
                .map(|r| {
                    let key: String = r.get(0);
                    let value: String = r.get(1);
                    KeyValue {
                        key: key.into_bytes(),
                        value: value.into_bytes(),
                    }
                })
                .collect();
            if !kvs.is_empty() {
                return Ok(BatchPutResponse { prev_kvs: kvs });
            }
        }
        Ok(BatchPutResponse { prev_kvs: vec![] })
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        if req.keys.is_empty() {
            return Ok(BatchGetResponse { kvs: vec![] });
        }
        let query = generate_batch_get_query(req.keys.len());
        let value_params = req
            .keys
            .iter()
            .map(|k| process_bytes(k, "BatchGetRequestKey"))
            .collect::<Result<Vec<&str>>>()?;
        let params: Vec<&(dyn ToSql + Sync)> = value_params
            .iter()
            .map(|x| x as &(dyn ToSql + Sync))
            .collect();

        let res = self
            .get_client()
            .await?
            .query(&query, &params)
            .await
            .context(PostgresExecutionSnafu)?;
        let kvs: Vec<KeyValue> = res
            .into_iter()
            .map(|r| {
                let key: String = r.get(0);
                let value: String = r.get(1);
                KeyValue {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                }
            })
            .collect();
        Ok(BatchGetResponse { kvs })
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let mut params = vec![];
        let template = select_range_delete_template(&req);
        if req.key != EMPTY {
            let key = process_bytes(&req.key, "deleteRangeKey")?;
            if template == PREFIX_DELETE {
                let prefix = format!("{key}%");
                params.push(Cow::Owned(prefix));
            } else {
                params.push(Cow::Borrowed(key));
            }
        }
        if template == RANGE_DELETE_FULL_RANGE && req.range_end != EMPTY {
            let range_end = process_bytes(&req.range_end, "deleteRangeEnd")?;
            params.push(Cow::Borrowed(range_end));
        }
        let params: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|x| match x {
                Cow::Borrowed(borrowed) => borrowed as &(dyn ToSql + Sync),
                Cow::Owned(owned) => owned as &(dyn ToSql + Sync),
            })
            .collect();

        let res = self
            .get_client()
            .await?
            .query(template, &params)
            .await
            .context(PostgresExecutionSnafu)?;
        let deleted = res.len() as i64;
        if !req.prev_kv {
            return Ok({
                DeleteRangeResponse {
                    deleted,
                    prev_kvs: vec![],
                }
            });
        }
        let kvs: Vec<KeyValue> = res
            .into_iter()
            .map(|r| {
                let key: String = r.get(0);
                let value: String = r.get(1);
                KeyValue {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                }
            })
            .collect();
        Ok(DeleteRangeResponse {
            deleted,
            prev_kvs: kvs,
        })
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        if req.keys.is_empty() {
            return Ok(BatchDeleteResponse { prev_kvs: vec![] });
        }
        let query = generate_batch_delete_query(req.keys.len());
        let value_params = req
            .keys
            .iter()
            .map(|k| process_bytes(k, "BatchDeleteRequestKey"))
            .collect::<Result<Vec<&str>>>()?;
        let params: Vec<&(dyn ToSql + Sync)> = value_params
            .iter()
            .map(|x| x as &(dyn ToSql + Sync))
            .collect();

        let res = self
            .get_client()
            .await?
            .query(&query, &params)
            .await
            .context(PostgresExecutionSnafu)?;
        if !req.prev_kv {
            return Ok(BatchDeleteResponse { prev_kvs: vec![] });
        }
        let kvs: Vec<KeyValue> = res
            .into_iter()
            .map(|r| {
                let key: String = r.get(0);
                let value: String = r.get(1);
                KeyValue {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                }
            })
            .collect();
        Ok(BatchDeleteResponse { prev_kvs: kvs })
    }

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        let key = process_bytes(&req.key, "CASKey")?;
        let value = process_bytes(&req.value, "CASValue")?;
        if req.expect.is_empty() {
            let put_res = self.put_if_not_exists(key, value).await?;
            return Ok(CompareAndPutResponse {
                success: put_res,
                prev_kv: None,
            });
        }
        let expect = process_bytes(&req.expect, "CASExpect")?;

        let res = self
            .get_client()
            .await?
            .query(CAS, &[&key, &value, &expect])
            .await
            .context(PostgresExecutionSnafu)?;
        match res.is_empty() {
            true => Ok(CompareAndPutResponse {
                success: false,
                prev_kv: None,
            }),
            false => {
                let mut kvs: Vec<KeyValue> = res
                    .into_iter()
                    .map(|r| {
                        let key: String = r.get(0);
                        let value: String = r.get(1);
                        KeyValue {
                            key: key.into_bytes(),
                            value: value.into_bytes(),
                        }
                    })
                    .collect();
                Ok(CompareAndPutResponse {
                    success: true,
                    prev_kv: Some(kvs.remove(0)),
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl TxnService for PgStore {
    type Error = Error;

    async fn txn(&self, _txn: KvTxn) -> Result<KvTxnResponse> {
        // TODO: implement txn for pg kv backend.
        unimplemented!()
    }

    fn max_txn_ops(&self) -> usize {
        unreachable!("postgres backend does not support max_txn_ops!")
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_backend::test::{
        prepare_kv_with_prefix, test_kv_batch_delete_with_prefix, test_kv_batch_get_with_prefix,
        test_kv_compare_and_put_with_prefix, test_kv_delete_range_with_prefix,
        test_kv_put_with_prefix, test_kv_range_2_with_prefix, test_kv_range_with_prefix,
        unprepare_kv,
    };

    async fn build_pg_kv_backend() -> Option<PgStore> {
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
        client
            .execute(METADKV_CREATION, &[])
            .await
            .context(PostgresExecutionSnafu)
            .unwrap();
        Some(PgStore { pool })
    }

    #[tokio::test]
    async fn test_put() {
        if let Some(kv_backend) = build_pg_kv_backend().await {
            let prefix = b"put/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_put_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_range() {
        if let Some(kv_backend) = build_pg_kv_backend().await {
            let prefix = b"range/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_range_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_range_2() {
        if let Some(kv_backend) = build_pg_kv_backend().await {
            test_kv_range_2_with_prefix(kv_backend, b"range2/".to_vec()).await;
        }
    }

    #[tokio::test]
    async fn test_batch_get() {
        if let Some(kv_backend) = build_pg_kv_backend().await {
            let prefix = b"batchGet/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_batch_get_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compare_and_put() {
        if let Some(kv_backend) = build_pg_kv_backend().await {
            let kv_backend = Arc::new(kv_backend);
            test_kv_compare_and_put_with_prefix(kv_backend, b"compareAndPut/".to_vec()).await;
        }
    }

    #[tokio::test]
    async fn test_delete_range() {
        if let Some(kv_backend) = build_pg_kv_backend().await {
            let prefix = b"deleteRange/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_delete_range_with_prefix(kv_backend, prefix.to_vec()).await;
        }
    }

    #[tokio::test]
    async fn test_batch_delete() {
        if let Some(kv_backend) = build_pg_kv_backend().await {
            let prefix = b"batchDelete/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_batch_delete_with_prefix(kv_backend, prefix.to_vec()).await;
        }
    }
}
