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

//! [KvBackend] implementation based on [raft_engine::Engine].

use std::any::Any;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::Path;
use std::sync::RwLock;

use common_error::ext::BoxedError;
use common_meta::error as meta_error;
use common_meta::kv_backend::txn::{Txn, TxnOp, TxnOpResponse, TxnRequest, TxnResponse};
use common_meta::kv_backend::{KvBackend, TxnService};
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse,
};
use common_meta::rpc::KeyValue;
use common_meta::util::get_next_prefix_key;
use raft_engine::{Config, Engine, LogBatch};
use snafu::{IntoError, ResultExt};

use crate::error::{self, IoSnafu, RaftEngineSnafu};

pub(crate) const SYSTEM_NAMESPACE: u64 = 0;

/// RaftEngine based [KvBackend] implementation.
pub struct RaftEngineBackend {
    engine: RwLock<Engine>,
}

fn ensure_dir(dir: &str) -> error::Result<()> {
    let io_context = |err| {
        IoSnafu {
            path: dir.to_string(),
        }
        .into_error(err)
    };

    let path = Path::new(dir);
    if !path.exists() {
        // create the directory to ensure the permission
        return std::fs::create_dir_all(path).map_err(io_context);
    }

    let metadata = std::fs::metadata(path).map_err(io_context)?;
    if !metadata.is_dir() {
        return Err(io_context(std::io::ErrorKind::NotADirectory.into()));
    }

    Ok(())
}

impl RaftEngineBackend {
    pub fn try_open_with_cfg(config: Config) -> error::Result<Self> {
        ensure_dir(&config.dir)?;
        if let Some(spill_dir) = &config.spill_dir {
            ensure_dir(spill_dir)?;
        }

        let engine = Engine::open(config).context(RaftEngineSnafu)?;
        Ok(Self {
            engine: RwLock::new(engine),
        })
    }
}

#[async_trait::async_trait]
impl TxnService for RaftEngineBackend {
    type Error = meta_error::Error;

    async fn txn(&self, txn: Txn) -> meta_error::Result<TxnResponse> {
        let TxnRequest {
            compare,
            success,
            failure,
        } = txn.into();

        let mut succeeded = true;
        let engine = self.engine.write().unwrap();
        for cmp in compare {
            let existing_value = engine_get(&engine, &cmp.key)?.map(|kv| kv.value);
            if !cmp.compare_value(existing_value.as_ref()) {
                succeeded = false;
                break;
            }
        }

        let mut batch = LogBatch::default();
        let do_txn = |txn_op| match txn_op {
            TxnOp::Put(key, value) => {
                batch
                    .put(SYSTEM_NAMESPACE, key, value)
                    .context(RaftEngineSnafu)
                    .map_err(BoxedError::new)
                    .context(meta_error::ExternalSnafu)?;
                Ok(TxnOpResponse::ResponsePut(PutResponse { prev_kv: None }))
            }

            TxnOp::Get(key) => {
                let value = engine_get(&engine, &key)?.map(|kv| kv.value);
                let kvs = value
                    .map(|value| KeyValue { key, value })
                    .into_iter()
                    .collect();
                Ok(TxnOpResponse::ResponseGet(RangeResponse {
                    kvs,
                    more: false,
                }))
            }

            TxnOp::Delete(key) => {
                let prev = engine_get(&engine, &key)?;
                batch.delete(SYSTEM_NAMESPACE, key);
                let deleted = if prev.is_some() { 1 } else { 0 };
                Ok(TxnOpResponse::ResponseDelete(DeleteRangeResponse {
                    deleted,
                    prev_kvs: vec![],
                }))
            }
        };

        let responses = if succeeded { success } else { failure }
            .into_iter()
            .map(do_txn)
            .collect::<meta_error::Result<_>>()?;

        engine
            .write(&mut batch, false)
            .context(RaftEngineSnafu)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;

        Ok(TxnResponse {
            succeeded,
            responses,
        })
    }

    fn max_txn_ops(&self) -> usize {
        usize::MAX
    }
}

#[async_trait::async_trait]
impl KvBackend for RaftEngineBackend {
    fn name(&self) -> &str {
        "RaftEngineBackend"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse, Self::Error> {
        let mut res = vec![];
        let (start, end) = req.range();
        let RangeRequest {
            keys_only, limit, ..
        } = req;

        let (start_key, end_key) = match (start, end) {
            (Included(start), Included(end)) => (Some(start), Some(get_next_prefix_key(&end))),
            (Unbounded, Unbounded) => (None, None),
            (Included(start), Excluded(end)) => (Some(start), Some(end)),
            (Included(start), Unbounded) => (Some(start), None),
            _ => unreachable!(),
        };
        let mut more = false;
        let mut iter = 0;

        self.engine
            .read()
            .unwrap()
            .scan_raw_messages(
                SYSTEM_NAMESPACE,
                start_key.as_deref(),
                end_key.as_deref(),
                false,
                |key, value| {
                    let take = limit == 0 || iter != limit;
                    iter += 1;
                    more = limit > 0 && iter > limit;

                    if take {
                        res.push(KeyValue {
                            key: key.to_vec(),
                            value: if keys_only { vec![] } else { value.to_vec() },
                        });
                    }

                    take
                },
            )
            .context(RaftEngineSnafu)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        Ok(RangeResponse { kvs: res, more })
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse, Self::Error> {
        let PutRequest {
            key,
            value,
            prev_kv,
        } = req;

        let mut prev = None;
        // Engine::write assures that one batch is written atomically. The read/write lock is
        // just to prevent race condition between put and txn.
        let engine = self.engine.read().unwrap();
        if prev_kv {
            prev = engine_get(&engine, &key)?;
        }
        engine_put(&engine, key, value)?;
        Ok(PutResponse { prev_kv: prev })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse, Self::Error> {
        let BatchPutRequest { kvs, prev_kv } = req;
        let mut batch = LogBatch::with_capacity(kvs.len());

        let mut prev_kvs = if prev_kv {
            Vec::with_capacity(kvs.len())
        } else {
            vec![]
        };

        let engine = self.engine.read().unwrap();
        for kv in kvs {
            if prev_kv && let Some(kv) = engine_get(&engine, &kv.key)? {
                prev_kvs.push(kv);
            }
            batch
                .put(SYSTEM_NAMESPACE, kv.key, kv.value)
                .context(RaftEngineSnafu)
                .map_err(BoxedError::new)
                .context(meta_error::ExternalSnafu)?;
        }

        engine
            .write(&mut batch, false)
            .context(RaftEngineSnafu)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;

        Ok(BatchPutResponse { prev_kvs })
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse, Self::Error> {
        let mut response = BatchGetResponse {
            kvs: Vec::with_capacity(req.keys.len()),
        };
        for key in req.keys {
            let Some(value) = self.engine.read().unwrap().get(SYSTEM_NAMESPACE, &key) else {
                continue;
            };
            response.kvs.push(KeyValue { key, value });
        }
        Ok(response)
    }

    async fn delete_range(
        &self,
        req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, Self::Error> {
        let DeleteRangeRequest {
            key,
            range_end,
            prev_kv,
        } = req;

        let range = RangeRequest {
            key,
            range_end,
            limit: 0,
            keys_only: false,
        };
        let range_resp = self.range(range).await?;

        let mut prev_kvs = vec![];
        let mut deleted = 0;

        let engine = self.engine.read().unwrap();
        for kv in range_resp.kvs {
            engine_delete(&engine, &kv.key)?;
            if prev_kv {
                prev_kvs.push(kv);
            }
            deleted += 1;
        }

        Ok(DeleteRangeResponse { deleted, prev_kvs })
    }

    async fn batch_delete(
        &self,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse, Self::Error> {
        let BatchDeleteRequest { keys, prev_kv } = req;

        let mut prev_kvs = if prev_kv {
            Vec::with_capacity(keys.len())
        } else {
            vec![]
        };
        let mut batch = LogBatch::with_capacity(keys.len());
        let engine = self.engine.read().unwrap();
        for key in keys {
            if prev_kv && let Some(prev) = engine_get(&engine, &key)? {
                prev_kvs.push(prev);
            }
            batch.delete(SYSTEM_NAMESPACE, key);
        }
        engine
            .write(&mut batch, false)
            .context(RaftEngineSnafu)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        Ok(BatchDeleteResponse { prev_kvs })
    }

    async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
        engine_get(&self.engine.read().unwrap(), key)
    }

    async fn exists(&self, key: &[u8]) -> Result<bool, Self::Error> {
        Ok(engine_get(&self.engine.read().unwrap(), key)?.is_some())
    }

    async fn delete(&self, key: &[u8], prev_kv: bool) -> Result<Option<KeyValue>, Self::Error> {
        let engine = self.engine.read().unwrap();
        let prev = if prev_kv {
            engine_get(&engine, key)?
        } else {
            None
        };
        engine_delete(&engine, key)?;
        Ok(prev)
    }
}

fn engine_get(engine: &Engine, key: &[u8]) -> meta_error::Result<Option<KeyValue>> {
    let res = engine.get(SYSTEM_NAMESPACE, key);
    Ok(res.map(|value| KeyValue {
        key: key.to_vec(),
        value,
    }))
}

fn engine_put(engine: &Engine, key: Vec<u8>, value: Vec<u8>) -> meta_error::Result<()> {
    let mut batch = LogBatch::with_capacity(1);
    batch
        .put(SYSTEM_NAMESPACE, key, value)
        .context(RaftEngineSnafu)
        .map_err(BoxedError::new)
        .context(meta_error::ExternalSnafu)?;
    engine
        .write(&mut batch, false)
        .context(RaftEngineSnafu)
        .map_err(BoxedError::new)
        .context(meta_error::ExternalSnafu)?;
    Ok(())
}

fn engine_delete(engine: &Engine, key: &[u8]) -> meta_error::Result<()> {
    let mut batch = LogBatch::with_capacity(1);
    batch.delete(SYSTEM_NAMESPACE, key.to_vec());
    engine
        .write(&mut batch, false)
        .context(RaftEngineSnafu)
        .map_err(BoxedError::new)
        .context(meta_error::ExternalSnafu)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use common_meta::kv_backend::test::{
        prepare_kv, test_kv_batch_delete, test_kv_batch_get, test_kv_compare_and_put,
        test_kv_delete_range, test_kv_put, test_kv_range, test_kv_range_2,
    };
    use common_meta::rpc::store::{CompareAndPutRequest, CompareAndPutResponse};
    use common_test_util::temp_dir::create_temp_dir;
    use raft_engine::{Config, ReadableSize, RecoveryMode};

    use super::*;

    fn build_kv_backend(dir: String) -> RaftEngineBackend {
        let config = Config {
            dir,
            spill_dir: None,
            recovery_mode: RecoveryMode::AbsoluteConsistency,
            target_file_size: ReadableSize::mb(4),
            purge_threshold: ReadableSize::mb(16),
            ..Default::default()
        };
        let engine = RwLock::new(Engine::open(config).unwrap());
        RaftEngineBackend { engine }
    }

    #[tokio::test]
    async fn test_raft_engine_kv() {
        let dir = create_temp_dir("raft-engine-kv");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());
        assert!(backend.get(b"hello").await.unwrap().is_none());

        let response = backend
            .put(PutRequest {
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
                prev_kv: false,
            })
            .await
            .unwrap();
        assert!(response.prev_kv.is_none());
        assert_eq!(
            b"world".as_slice(),
            &backend.get(b"hello").await.unwrap().unwrap().value
        );
    }

    #[tokio::test]
    async fn test_compare_and_put() {
        let dir = create_temp_dir("compare_and_put");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());

        let key = b"hello".to_vec();
        backend
            .put(PutRequest {
                key: key.clone(),
                value: b"word".to_vec(),
                prev_kv: false,
            })
            .await
            .unwrap();

        let CompareAndPutResponse { success, prev_kv } = backend
            .compare_and_put(CompareAndPutRequest {
                key: key.clone(),
                expect: b"world".to_vec(),
                value: b"whatever".to_vec(),
            })
            .await
            .unwrap();
        assert!(!success);
        assert_eq!(b"word".as_slice(), &prev_kv.unwrap().value);

        let CompareAndPutResponse { success, prev_kv } = backend
            .compare_and_put(CompareAndPutRequest {
                key: key.clone(),
                expect: b"word".to_vec(),
                value: b"world".to_vec(),
            })
            .await
            .unwrap();
        assert!(success);
        // Do not return prev_kv on success
        assert!(prev_kv.is_none());

        assert_eq!(
            b"world".as_slice(),
            &backend.get(b"hello").await.unwrap().unwrap().value
        );
    }

    fn build_batch_key_values(start: usize, end: usize) -> Vec<KeyValue> {
        (start..end)
            .map(|idx| {
                let bytes = idx.to_ne_bytes().to_vec();
                KeyValue {
                    key: bytes.clone(),
                    value: bytes,
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn test_compare_and_put_empty() {
        let dir = create_temp_dir("compare_and_put_empty");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());
        let CompareAndPutResponse { success, prev_kv } = backend
            .compare_and_put(CompareAndPutRequest {
                key: b"hello".to_vec(),
                expect: vec![],
                value: b"world".to_vec(),
            })
            .await
            .unwrap();
        assert!(success);
        assert!(prev_kv.is_none());

        assert_eq!(
            b"world".as_slice(),
            &backend.get(b"hello").await.unwrap().unwrap().value
        );
    }

    #[tokio::test]
    async fn test_batch_put_and_scan_delete() {
        let dir = create_temp_dir("compare_and_put");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());

        // put 0..10
        let BatchPutResponse { prev_kvs } = backend
            .batch_put(BatchPutRequest {
                kvs: build_batch_key_values(0, 10),
                prev_kv: false,
            })
            .await
            .unwrap();
        assert_eq!(0, prev_kvs.len());

        let BatchPutResponse { prev_kvs } = backend
            .batch_put(BatchPutRequest {
                kvs: build_batch_key_values(5, 15),
                prev_kv: true,
            })
            .await
            .unwrap();
        let prev_kvs = prev_kvs
            .into_iter()
            .map(|kv| kv.key)
            .collect::<HashSet<_>>();
        assert_eq!(
            build_batch_key_values(5, 10)
                .into_iter()
                .map(|kv| kv.key)
                .collect::<HashSet<_>>(),
            prev_kvs
        );

        // range 2..10
        let RangeResponse { kvs, more } = backend
            .range(RangeRequest {
                key: 2usize.to_ne_bytes().to_vec(),
                range_end: 10usize.to_ne_bytes().to_vec(),
                limit: 0,
                keys_only: false,
            })
            .await
            .unwrap();
        assert!(!more);
        assert_eq!(
            build_batch_key_values(2, 10)
                .into_iter()
                .map(|kv| kv.key)
                .collect::<HashSet<_>>(),
            kvs.into_iter().map(|kv| kv.key).collect::<HashSet<_>>()
        );

        //raneg 0..1000
        let RangeResponse { kvs, more } = backend
            .range(RangeRequest {
                key: 0usize.to_ne_bytes().to_vec(),
                range_end: 1000usize.to_ne_bytes().to_vec(),
                limit: 0,
                keys_only: false,
            })
            .await
            .unwrap();
        assert!(!more);
        assert_eq!(
            build_batch_key_values(0, 15)
                .into_iter()
                .map(|kv| kv.key)
                .collect::<HashSet<_>>(),
            kvs.into_iter().map(|kv| kv.key).collect::<HashSet<_>>()
        );

        // then delete 3..7
        let BatchDeleteResponse { prev_kvs } = backend
            .batch_delete(BatchDeleteRequest {
                keys: build_batch_key_values(3, 7)
                    .into_iter()
                    .map(|kv| kv.key)
                    .collect(),
                prev_kv: true,
            })
            .await
            .unwrap();
        assert_eq!(
            build_batch_key_values(3, 7)
                .into_iter()
                .map(|kv| kv.key)
                .collect::<HashSet<_>>(),
            prev_kvs
                .into_iter()
                .map(|kv| kv.key)
                .collect::<HashSet<_>>()
        );

        // finally assert existing keys to be 0..3 ∪ 7..15
        let RangeResponse { kvs, more } = backend
            .range(RangeRequest {
                key: 0usize.to_ne_bytes().to_vec(),
                range_end: 1000usize.to_ne_bytes().to_vec(),
                limit: 0,
                keys_only: false,
            })
            .await
            .unwrap();
        assert!(!more);

        let keys = kvs.into_iter().map(|kv| kv.key).collect::<HashSet<_>>();
        assert_eq!(
            build_batch_key_values(0, 3)
                .into_iter()
                .chain(build_batch_key_values(7, 15))
                .map(|kv| kv.key)
                .collect::<HashSet<_>>(),
            keys
        );
    }

    #[tokio::test]
    async fn test_range() {
        let dir = create_temp_dir("range");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());
        prepare_kv(&backend).await;

        test_kv_range(&backend).await;
    }

    #[tokio::test]
    async fn test_range_2() {
        let dir = create_temp_dir("range2");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());

        test_kv_range_2(backend).await;
    }

    #[tokio::test]
    async fn test_put() {
        let dir = create_temp_dir("put");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());
        prepare_kv(&backend).await;

        test_kv_put(&backend).await;
    }

    #[tokio::test]
    async fn test_batch_get() {
        let dir = create_temp_dir("batch_get");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());
        prepare_kv(&backend).await;

        test_kv_batch_get(&backend).await;
    }

    #[tokio::test]
    async fn test_batch_delete() {
        let dir = create_temp_dir("batch_delete");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());
        prepare_kv(&backend).await;

        test_kv_batch_delete(backend).await;
    }

    #[tokio::test]
    async fn test_delete_range() {
        let dir = create_temp_dir("delete_range");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());
        prepare_kv(&backend).await;

        test_kv_delete_range(backend).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compare_and_put_2() {
        let dir = create_temp_dir("compare_and_put");
        let backend = build_kv_backend(dir.path().to_str().unwrap().to_string());
        prepare_kv(&backend).await;

        test_kv_compare_and_put(Arc::new(backend)).await;
    }
}
