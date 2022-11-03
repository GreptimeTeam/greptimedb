use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use api::v1::meta::BatchPutRequest;
use api::v1::meta::BatchPutResponse;
use api::v1::meta::CompareAndPutRequest;
use api::v1::meta::CompareAndPutResponse;
use api::v1::meta::DeleteRangeRequest;
use api::v1::meta::DeleteRangeResponse;
use api::v1::meta::KeyValue;
use api::v1::meta::PutRequest;
use api::v1::meta::PutResponse;
use api::v1::meta::RangeRequest;
use api::v1::meta::RangeResponse;
use api::v1::meta::ResponseHeader;
use tokio::sync::RwLock;

use super::kv::KvStore;
use crate::error::Result;

/// Only for mock test
#[derive(Clone)]
pub struct MemStore {
    inner: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Default::default())),
        }
    }
}

#[async_trait::async_trait]
impl KvStore for MemStore {
    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        let RangeRequest {
            header,
            key,
            range_end,
            limit,
            keys_only,
        } = req;

        let memory = self.inner.read().await;

        let mut kvs = if range_end.is_empty() {
            memory.get_key_value(&key).map_or(vec![], |(k, v)| {
                vec![KeyValue {
                    key: k.clone(),
                    value: if keys_only { vec![] } else { v.clone() },
                }]
            })
        } else {
            let range = Range {
                start: key,
                end: range_end,
            };
            memory
                .range(range)
                .map(|kv| KeyValue {
                    key: kv.0.clone(),
                    value: if keys_only { vec![] } else { kv.1.clone() },
                })
                .collect::<Vec<_>>()
        };

        let more = if limit > 0 {
            kvs.truncate(limit as usize);
            true
        } else {
            false
        };

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(RangeResponse { header, kvs, more })
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let PutRequest {
            header,
            key,
            value,
            prev_kv,
        } = req;

        let mut memory = self.inner.write().await;
        let prev_value = memory.insert(key.clone(), value);
        let prev_kv = if prev_kv {
            prev_value.map(|value| KeyValue { key, value })
        } else {
            None
        };

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(PutResponse { header, prev_kv })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let BatchPutRequest {
            header,
            kvs,
            prev_kv,
        } = req;

        let mut memory = self.inner.write().await;
        let prev_kvs = if prev_kv {
            kvs.into_iter()
                .map(|kv| (kv.key.clone(), memory.insert(kv.key, kv.value)))
                .filter(|(_, v)| v.is_some())
                .map(|(key, value)| KeyValue {
                    key,
                    value: value.unwrap(),
                })
                .collect()
        } else {
            for kv in kvs.into_iter() {
                memory.insert(kv.key, kv.value);
            }
            vec![]
        };

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(BatchPutResponse { header, prev_kvs })
    }

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        let CompareAndPutRequest {
            header,
            key,
            expect,
            value,
        } = req;

        let mut memory = self.inner.write().await;
        let (success, prev_kv) = if expect.is_empty() {
            (
                false,
                Some(KeyValue {
                    key: key.clone(),
                    value: vec![],
                }),
            )
        } else {
            let prev_val = memory.get(&key);
            let success = prev_val
                .map(|v| expect.cmp(v) == Ordering::Equal)
                .unwrap_or(false);
            (
                success,
                prev_val.map(|v| KeyValue {
                    key: key.clone(),
                    value: v.clone(),
                }),
            )
        };

        if success {
            memory.insert(key, value);
        }

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(CompareAndPutResponse {
            header,
            success,
            prev_kv,
        })
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let DeleteRangeRequest {
            header,
            key,
            range_end,
            prev_kv,
        } = req;

        let mut memory = self.inner.write().await;

        let prev_kvs = if range_end.is_empty() {
            let prev_val = memory.remove(&key);
            prev_val.map_or(vec![], |value| vec![KeyValue { key, value }])
        } else {
            let range = Range {
                start: key,
                end: range_end,
            };
            memory
                .drain_filter(|key, _| range.contains(key))
                .map(|(key, value)| KeyValue { key, value })
                .collect::<Vec<_>>()
        };

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(DeleteRangeResponse {
            header,
            deleted: prev_kvs.len() as i64,
            prev_kvs: if prev_kv {
                prev_kvs
            } else {
                Default::default()
            },
        })
    }
}
