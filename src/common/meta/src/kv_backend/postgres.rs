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
use std::sync::Arc;

use snafu::ResultExt;
use tokio_postgres::{Client, NoTls};

use super::{KvBackend, TxnService};
use crate::error::{ConnectPostgresSnafu, Error, FromUtf8Snafu, PostgresFailedSnafu, Result};
use crate::kv_backend::txn::{Txn as KvTxn, TxnResponse as KvTxnResponse};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

pub struct PgStore {
    client: Client,
}

const METADKV_CREATION: &str =
    "CREATE TABLE IF NOT EXISTS metakv(k varchar PRIMARY KEY, v varchar);";

impl PgStore {
    pub async fn with_url(url: &str) -> Result<KvBackendRef> {
        let (client, conn) = tokio_postgres::connect(url, NoTls)
            .await
            .context(ConnectPostgresSnafu)?;
        tokio::spawn(async move { conn.await.context(ConnectPostgresSnafu) });
        Self::with_pg_client(client).await
    }

    pub async fn with_pg_client(client: Client) -> Result<KvBackendRef> {
        // This step ensures the postgres metadata backend is ready to use.
        // We check if metakv table exists, and we will create a new table
        // if it does not exist.
        client
            .execute(METADKV_CREATION, &[])
            .await
            .context(PostgresFailedSnafu)?;
        Ok(Arc::new(Self { client }))
    }
}

/*
SELECT k, v FROM metakv
WHERE k >= start_key and k <= end_key
*/
fn generate_range_scan_query(req: RangeRequest) -> Result<String> {
    let mut sql = String::new();
    sql.push_str("SELECT k");
    if !req.keys_only {
        sql.push_str(",v");
    }
    sql.push_str(" FROM metakv");
    if req.key != b"\0" || req.range_end != b"\0" {
        let start_key = process_bytes(req.key.clone(), "GetRequestKey")?;
        match req.range_end.is_empty() {
            true => sql.push_str(&format!(" WHERE k = '{start_key}'")),
            false => match is_prefix_range(req.key.clone(), &req.range_end) {
                true => {
                    sql.push_str(&format!(" WHERE k LIKE '{start_key}%'"));
                }
                false => {
                    sql.push_str(&format!(" WHERE k >= '{start_key}'"));
                    if req.range_end != b"\0" {
                        let end_key = process_bytes(req.range_end, "GetRequestRangeEnd")?;
                        sql.push_str(&format!(" AND k < '{end_key}'"));
                    }
                }
            },
        }
    }
    // This is a hack that we fetch limit + 1 and compare the result rows against request's limit
    // to see if there are more data to fetch.
    if req.limit > 0 {
        sql.push_str(&format!(" LIMIT {}", req.limit + 1));
    }
    sql.push(';');
    Ok(sql)
}

/*
SELECT k, v FROM metakv
WHERE k IN ('k1', 'k2', 'k3')
*/
fn generate_batch_scan_query(req: BatchGetRequest) -> Result<String> {
    let keys = req
        .keys
        .iter()
        .map(|k| process_bytes(k.clone(), "BatchGetRequestKey"))
        .collect::<Result<Vec<String>>>()?;
    let mut sql = String::new();
    sql.push_str("SELECT k, v FROM metakv ");
    let predicate = keys
        .iter()
        .map(|k| format!("'{k}'"))
        .collect::<Vec<String>>()
        .join(",");
    sql.push_str(&format!("WHERE k IN ({predicate})"));
    sql.push(';');
    Ok(sql)
}

/*
WITH prev AS (
    select k,v from metakv where k in ({keys_sql})
), update AS (
INSERT INTO metakv (k, v) VALUES
    {}
ON CONFLICT (
    k
) DO UPDATE SET
    v = excluded.v
)

SELECT k, v FROM prev;
*/
fn generate_batch_upsert_query(kvs: Vec<KeyValue>) -> Result<String> {
    let keys = kvs
        .iter()
        .map(|kv| process_bytes(kv.key.clone(), "BatchPutRequestKey"))
        .collect::<Result<Vec<String>>>()?;
    let vals = kvs
        .iter()
        .map(|kv| process_bytes(kv.value.clone(), "BatchPutRequestValue"))
        .collect::<Result<Vec<String>>>()?;
    let keys_sql = keys
        .iter()
        .map(|k| format!("'{k}'"))
        .collect::<Vec<String>>()
        .join(",");
    let kvs_sql = keys
        .iter()
        .zip(vals.iter())
        .map(|kv| format!("('{}', '{}')", kv.0, kv.1))
        .collect::<Vec<String>>()
        .join(",");

    Ok(format!(
        r#"
    WITH prev AS (
        select k,v from metakv where k in ({keys_sql})
    ), update AS (
    INSERT INTO metakv (k, v) VALUES
        {kvs_sql}
    ON CONFLICT (
        k
    ) DO UPDATE SET
        v = excluded.v
    )

    SELECT k, v FROM prev;
    "#
    ))
}

/*
DELETE FROM metakv WHERE k >= start_key and k < end_key RETURNING k, v;
*/
fn generate_range_delete_query(req: DeleteRangeRequest) -> Result<String> {
    let mut sql = String::new();
    sql.push_str("DELETE FROM metakv");
    if req.key != b"\0" || req.range_end != b"\0" {
        let start_key = process_bytes(req.key.clone(), "DeleteRangeRequestKey")?;
        match req.range_end.is_empty() {
            true => sql.push_str(&format!(" WHERE k = '{start_key}'")),
            false => match is_prefix_range(req.key.clone(), &req.range_end) {
                true => sql.push_str(&format!(" WHERE k like '{start_key}%'")),
                false => {
                    sql.push_str(&format!(" WHERE k >= '{start_key}'"));
                    if req.range_end != b"\0" {
                        let end_key = process_bytes(req.range_end, "DeleteRangeRequestRangeEnd")?;
                        sql.push_str(&format!(" AND k <= '{end_key}'"));
                    }
                }
            },
        }
    }
    sql.push_str(" RETURNING k, v");
    sql.push(';');
    Ok(sql)
}

/*
DELETE FROM metakv WHERE k IN (kvs) RETURNING K, V;
*/
fn generate_batch_delete_query(req: BatchDeleteRequest) -> Result<String> {
    let keys = req
        .keys
        .iter()
        .map(|k| process_bytes(k.clone(), "BatchDeleteRequestKey"))
        .collect::<Result<Vec<String>>>()?;
    let mut sql = String::new();
    sql.push_str("DELETE FROM metakv ");
    let predicate = keys
        .iter()
        .map(|k| format!("'{k}'"))
        .collect::<Vec<String>>()
        .join(",");
    sql.push_str(&format!("WHERE k IN ({predicate})"));
    sql.push_str(" RETURNING k, v");
    sql.push(';');
    Ok(sql)
}

/*
WITH prev AS (
        select k,v from metakv where k = '{key}' AND v = '{expect}'
    ), update AS (
    UPDATE metakv
    SET k='{key}',
    v='{value}'
    WHERE
        k='{key}'
        AND v='{expected}'
    )

    SELECT k, v FROM prev;
*/
fn generate_compare_and_put_query(req: CompareAndPutRequest) -> Result<String> {
    let key = process_bytes(req.key, "CompareAndPutRequestKey")?;
    let expect = process_bytes(req.expect, "CompareAndPutRequestExpect")?;
    let value = process_bytes(req.value, "CompareAndPutRequestValue")?;

    Ok(format!(
        r#"
    WITH prev AS (
        select k,v from metakv where k = '{key}' AND v = '{expect}'
    ), update AS (
    UPDATE metakv
    SET k='{key}',
    v='{value}'
    WHERE 
        k='{key}' AND v = '{expect}'
    )

    SELECT k, v FROM prev;
    "#
    ))
}

fn generate_put_if_not_exist_query(key: Vec<u8>, value: Vec<u8>) -> Result<String> {
    let key = process_bytes(key, "PutIfNotExistKey")?;
    let value = process_bytes(value, "PutIfNotExistValue")?;
    Ok(format!(
        r#"    
    WITH prev AS (
        select k,v from metakv where k = '{key}'
    ), insert AS (
        INSERT INTO metakv
        VALUES ('{key}', '{value}')
        ON CONFLICT (k) DO NOTHING
    )

    SELECT k, v FROM prev;"#
    ))
}

//  Trim null byte at the end and convert bytes to string.
fn process_bytes(mut data: Vec<u8>, name: &str) -> Result<String> {
    let mut len = data.len();
    // remove trailing null bytes to avoid error in postgres encoding.
    while len > 0 && data[len - 1] == 0 {
        len -= 1;
    }
    data.truncate(len);
    let res = String::from_utf8(data).context(FromUtf8Snafu { name })?;
    Ok(res)
}

impl PgStore {
    async fn put_if_not_exists(&self, key: Vec<u8>, value: Vec<u8>) -> Result<bool> {
        let query = generate_put_if_not_exist_query(key, value)?;
        let res = self
            .client
            .query(&query, &[])
            .await
            .context(PostgresFailedSnafu)?;
        Ok(res.is_empty())
    }
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
        let key_only = req.keys_only;
        let limit = req.limit as usize;
        let query = generate_range_scan_query(req)?;
        let res = self
            .client
            .query(&query, &[])
            .await
            .context(PostgresFailedSnafu)?;
        let kvs: Vec<KeyValue> = res
            .into_iter()
            .map(|r| {
                let key: String = r.get(0);
                if key_only {
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
        let kvs = KeyValue {
            key: req.key,
            value: req.value,
        };
        let query = generate_batch_upsert_query(vec![kvs])?;
        let res = self
            .client
            .query(&query, &[])
            .await
            .context(PostgresFailedSnafu)?;
        if req.prev_kv {
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
            if !kvs.is_empty() {
                return Ok(PutResponse {
                    prev_kv: Some(kvs.remove(0)),
                });
            }
        }
        Ok(PutResponse { prev_kv: None })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let query = generate_batch_upsert_query(req.kvs)?;
        let res = self
            .client
            .query(&query, &[])
            .await
            .context(PostgresFailedSnafu)?;
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
        let query = generate_batch_scan_query(req)?;
        let res = self
            .client
            .query(&query, &[])
            .await
            .context(PostgresFailedSnafu)?;
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
        let prev_kv = req.prev_kv;
        let query = generate_range_delete_query(req)?;
        let res = self
            .client
            .query(&query, &[])
            .await
            .context(PostgresFailedSnafu)?;
        let deleted = res.len() as i64;
        if !prev_kv {
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
        let prev_kv = req.prev_kv;
        let query = generate_batch_delete_query(req)?;
        let res = self
            .client
            .query(&query, &[])
            .await
            .context(PostgresFailedSnafu)?;
        if !prev_kv {
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
        if req.expect.is_empty() {
            let put_res = self.put_if_not_exists(req.key, req.value).await?;
            return Ok(CompareAndPutResponse {
                success: put_res,
                prev_kv: None,
            });
        }
        let query = generate_compare_and_put_query(req)?;
        let res = self
            .client
            .query(&query, &[])
            .await
            .context(PostgresFailedSnafu)?;
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
        unimplemented!()
    }

    fn max_txn_ops(&self) -> usize {
        unimplemented!()
    }
}

fn is_prefix_range(mut start: Vec<u8>, end: &[u8]) -> bool {
    if let Some(last_byte) = start.last_mut() {
        *last_byte += 1;
    }
    start == end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_range_scan_query() {
        // case 1: key and end range both present
        let req = RangeRequest {
            key: b"test_key".to_vec(),
            range_end: b"test_range_end".to_vec(),
            limit: 64,
            keys_only: true,
        };
        let query = generate_range_scan_query(req).unwrap();
        assert_eq!(
            "SELECT k FROM metakv WHERE k >= 'test_key' AND k < 'test_range_end' LIMIT 65;",
            query
        );

        // case 2: range_end == \0 we scan >= key
        let req = RangeRequest {
            key: b"test_key".to_vec(),
            range_end: vec![b'\0'],
            limit: 64,
            keys_only: true,
        };
        let query = generate_range_scan_query(req).unwrap();
        assert_eq!(
            "SELECT k FROM metakv WHERE k >= 'test_key' LIMIT 65;",
            query
        );

        // case 3: full table scan key == \0 and end range == \0 no limit
        let req = RangeRequest {
            key: vec![b'\0'],
            range_end: vec![b'\0'],
            limit: 0,
            keys_only: true,
        };
        let query = generate_range_scan_query(req).unwrap();
        assert_eq!("SELECT k FROM metakv;", query);

        // case 4: prefix scan
        let start_key = b"a";
        let post_key = b"b";
        assert!(is_prefix_range(start_key.clone().to_vec(), post_key));
        let req = RangeRequest {
            key: start_key.to_vec(),
            range_end: post_key.to_vec(),
            limit: 0,
            keys_only: true,
        };
        let query = generate_range_scan_query(req).unwrap();
        assert_eq!("SELECT k FROM metakv WHERE k LIKE 'a%';", query);

        // case 5: point get
        let req = RangeRequest {
            key: b"test_key".to_vec(),
            range_end: vec![],
            limit: 64,
            keys_only: true,
        };
        let query = generate_range_scan_query(req).unwrap();
        assert_eq!("SELECT k FROM metakv WHERE k = 'test_key' LIMIT 65;", query);

        // case 6: key and end range both present
        let req = RangeRequest {
            key: b"test_key".to_vec(),
            range_end: b"test_range_end".to_vec(),
            limit: 64,
            keys_only: false,
        };
        let query = generate_range_scan_query(req).unwrap();
        assert_eq!(
            "SELECT k,v FROM metakv WHERE k >= 'test_key' AND k < 'test_range_end' LIMIT 65;",
            query
        );
    }

    #[test]
    fn test_generate_batch_upsert_query() {
        // case 1: single upsert
        let kvs = vec![KeyValue {
            key: b"a".to_vec(),
            value: b"b".to_vec(),
        }];
        let mut query = generate_batch_upsert_query(kvs).unwrap();
        query = query.trim().to_string();
        let expected = r#"WITH prev AS (
        select k,v from metakv where k in ('a')
    ), update AS (
    INSERT INTO metakv (k, v) VALUES
        ('a', 'b')
    ON CONFLICT (
        k
    ) DO UPDATE SET
        v = excluded.v
    )

    SELECT k, v FROM prev;"#;
        assert_eq!(expected, query);

        // case 2: multi-upsert
        let kvs = vec![
            KeyValue {
                key: b"a".to_vec(),
                value: b"b".to_vec(),
            },
            KeyValue {
                key: b"c".to_vec(),
                value: b"d".to_vec(),
            },
        ];
        let mut query = generate_batch_upsert_query(kvs).unwrap();
        query = query.trim().to_string();
        let expected = r#"WITH prev AS (
        select k,v from metakv where k in ('a','c')
    ), update AS (
    INSERT INTO metakv (k, v) VALUES
        ('a', 'b'),('c', 'd')
    ON CONFLICT (
        k
    ) DO UPDATE SET
        v = excluded.v
    )

    SELECT k, v FROM prev;"#;
        assert_eq!(expected, query);
    }

    #[test]
    fn test_generate_range_delete_query() {
        // case 1: delete range
        let req = DeleteRangeRequest {
            key: b"test_key".to_vec(),
            range_end: b"test_range_end".to_vec(),
            prev_kv: true,
        };
        let query = generate_range_delete_query(req).unwrap();
        assert_eq!(
            "DELETE FROM metakv WHERE k >= 'test_key' AND k <= 'test_range_end' RETURNING k, v;",
            query
        );

        // case 2: range_end == \0 we delete >= key
        let req = DeleteRangeRequest {
            key: b"test_key".to_vec(),
            range_end: vec![b'\0'],
            prev_kv: true,
        };
        let query = generate_range_delete_query(req).unwrap();
        assert_eq!(
            "DELETE FROM metakv WHERE k >= 'test_key' RETURNING k, v;",
            query
        );

        // case 3: prefix delete
        let start_key = b"a";
        let post_key = b"b";
        assert!(is_prefix_range(start_key.clone().to_vec(), post_key));
        let req = DeleteRangeRequest {
            key: start_key.to_vec(),
            range_end: post_key.to_vec(),
            prev_kv: true,
        };
        let query = generate_range_delete_query(req).unwrap();
        assert_eq!(
            "DELETE FROM metakv WHERE k like 'a%' RETURNING k, v;",
            query
        );

        // case 4: point delete
        let req = DeleteRangeRequest {
            key: b"test_key".to_vec(),
            range_end: vec![],
            prev_kv: true,
        };
        let query = generate_range_delete_query(req).unwrap();
        assert_eq!(
            "DELETE FROM metakv WHERE k = 'test_key' RETURNING k, v;",
            query
        );

        // case 4: full table delete
        let req = DeleteRangeRequest {
            key: vec![b'\0'],
            range_end: vec![b'\0'],
            prev_kv: true,
        };
        let query = generate_range_delete_query(req).unwrap();
        assert_eq!("DELETE FROM metakv RETURNING k, v;", query);
    }

    #[test]
    fn test_generate_batch_delete_query() {
        let keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let req = BatchDeleteRequest {
            keys,
            prev_kv: true,
        };
        let query = generate_batch_delete_query(req).unwrap();
        assert_eq!(
            "DELETE FROM metakv WHERE k IN ('a','b','c') RETURNING k, v;",
            query
        );
    }

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

        let (client, connection) = tokio_postgres::connect(&endpoints, NoTls).await.unwrap();
        tokio::spawn(connection);
        let _ = client.execute(METADKV_CREATION, &[]).await;
        Some(PgStore { client })
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
