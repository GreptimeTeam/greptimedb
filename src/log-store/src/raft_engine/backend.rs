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
use std::sync::RwLock;

use common_meta::kv_backend::txn::{Txn, TxnResponse};
use common_meta::kv_backend::{KvBackend, TxnService};
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, MoveValueRequest, MoveValueResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse,
};
use common_meta::rpc::KeyValue;
use raft_engine::{Engine, LogBatch};
use snafu::{ensure, ResultExt};

use crate::error::{RaftEngineSnafu, UnsupportedOperationSnafu};

pub(crate) const SYSTEM_NAMESPACE: u64 = 0;

pub struct RaftEngineBackend {
    engine: RwLock<Engine>,
}

#[async_trait::async_trait]
impl TxnService for RaftEngineBackend {
    type Error = crate::error::Error;

    async fn txn(&self, txn: Txn) -> Result<TxnResponse, Self::Error> {
        todo!()
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
        self.engine
            .read()
            .unwrap()
            .scan_raw_messages(
                SYSTEM_NAMESPACE,
                Some(&req.key),
                Some(&req.range_end),
                false,
                |key, value| {
                    res.push(KeyValue {
                        key: key.to_vec(),
                        value: value.to_vec(),
                    });
                    true
                },
            )
            .context(RaftEngineSnafu)?;
        Ok(RangeResponse {
            kvs: res,
            more: false,
        })
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse, Self::Error> {
        let PutRequest {
            key,
            value,
            prev_kv,
        } = req;

        ensure!(
            !prev_kv,
            UnsupportedOperationSnafu {
                reason: "put with prev_kv set"
            }
        );

        let mut batch = LogBatch::with_capacity(1);
        batch
            .put(SYSTEM_NAMESPACE, key, value)
            .context(RaftEngineSnafu)?;
        self.engine
            .read()
            .unwrap()
            .write(&mut batch, false)
            .context(RaftEngineSnafu)?;
        Ok(PutResponse { prev_kv: None })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse, Self::Error> {
        let BatchPutRequest { kvs, prev_kv } = req;
        ensure!(
            !prev_kv,
            UnsupportedOperationSnafu {
                reason: "batch_put with prev_kv set"
            }
        );

        let mut batch = LogBatch::with_capacity(kvs.len());
        for kv in kvs {
            batch
                .put(SYSTEM_NAMESPACE, kv.key, kv.value)
                .context(RaftEngineSnafu)?;
        }

        self.engine
            .read()
            .unwrap()
            .write(&mut batch, false)
            .context(RaftEngineSnafu)?;

        Ok(BatchPutResponse { prev_kvs: vec![] })
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse, Self::Error> {
        let mut response = BatchGetResponse {
            kvs: Vec::with_capacity(req.keys.len()),
        };
        let engine = self.engine.read().unwrap();
        for key in req.keys {
            let value = engine.get(SYSTEM_NAMESPACE, &key).unwrap();
            response.kvs.push(KeyValue { key, value });
        }
        Ok(response)
    }

    async fn compare_and_put(
        &self,
        _req: CompareAndPutRequest,
    ) -> Result<CompareAndPutResponse, Self::Error> {
        todo!()
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
        ensure!(
            !prev_kv,
            UnsupportedOperationSnafu {
                reason: "delete_range with prev_kv set"
            }
        );
        let range = RangeRequest {
            key,
            range_end,
            limit: 0,
            keys_only: true,
        };
        let range_resp = self.range(range).await?;

        let mut deleted = 0;
        for KeyValue { key, .. } in range_resp.kvs {
            self.delete(&key, false).await?;
            deleted += 1;
        }

        Ok(DeleteRangeResponse {
            deleted,
            prev_kvs: vec![],
        })
    }

    async fn batch_delete(
        &self,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse, Self::Error> {
        let BatchDeleteRequest { keys, prev_kv } = req;
        ensure!(
            !prev_kv,
            UnsupportedOperationSnafu {
                reason: "batch_put with prev_kv set"
            }
        );

        for key in keys {
            self.delete(&key, false).await?;
        }
        Ok(BatchDeleteResponse { prev_kvs: vec![] })
    }

    async fn move_value(&self, _req: MoveValueRequest) -> Result<MoveValueResponse, Self::Error> {
        todo!()
    }

    async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
        let res = self.engine.read().unwrap().get(SYSTEM_NAMESPACE, key);
        Ok(res.map(|value| KeyValue {
            key: key.to_vec(),
            value,
        }))
    }

    async fn exists(&self, key: &[u8]) -> Result<bool, Self::Error> {
        self.get(key).await.map(|o| o.is_some())
    }

    async fn delete(&self, key: &[u8], prev_kv: bool) -> Result<Option<KeyValue>, Self::Error> {
        ensure!(
            !prev_kv,
            UnsupportedOperationSnafu {
                reason: "delete with prev_kv set"
            }
        );

        let prev = self.get(key).await?;
        let mut batch = LogBatch::with_capacity(1);
        batch.delete(SYSTEM_NAMESPACE, key.to_vec());
        self.engine
            .read()
            .unwrap()
            .write(&mut batch, false)
            .context(RaftEngineSnafu)?;
        Ok(prev)
    }
}

#[cfg(test)]
mod tests {
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
}
