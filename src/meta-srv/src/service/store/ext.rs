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

use api::v1::meta::{KeyValue, RangeRequest};
use snafu::ensure;

use super::kv::KvStore;
use crate::error::{self, Result};

#[async_trait::async_trait]
pub trait KvStoreExt {
    async fn get(&self, key: Vec<u8>) -> Result<Option<KeyValue>>;

    async fn batch_get(&self, key: Vec<Vec<u8>>) -> Result<Vec<KeyValue>>;
}

#[async_trait::async_trait]
impl<T> KvStoreExt for T
where
    T: KvStore + ?Sized,
{
    async fn get(&self, key: Vec<u8>) -> Result<Option<KeyValue>> {
        let req = RangeRequest {
            key,
            ..Default::default()
        };

        let mut kvs = self.range(req).await?.kvs;

        if kvs.is_empty() {
            return Ok(None);
        }

        ensure!(
            kvs.len() == 1,
            error::InvalidKvsLengthSnafu { expect: 1_usize }
        );

        // Safety: the length check has been performed before using unwrap()
        Ok(Some(kvs.pop().unwrap()))
    }

    async fn batch_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<KeyValue>> {
        let mut kvs = Vec::with_capacity(keys.len());

        for key in keys {
            if let Some(kv) = self.get(key).await? {
                kvs.push(kv);
            }
        }

        Ok(kvs)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::PutRequest;

    use crate::service::store::ext::KvStoreExt;
    use crate::service::store::kv::KvStoreRef;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_get() {
        let mut in_mem = Arc::new(MemStore::new()) as KvStoreRef;

        put_stats_to_store(&mut in_mem).await;

        let kv = in_mem
            .get("test_key1".as_bytes().to_vec())
            .await
            .unwrap()
            .unwrap();

        assert_eq!("test_key1".as_bytes(), kv.key);
        assert_eq!("test_val1".as_bytes(), kv.value);

        let kv = in_mem
            .get("test_key2".as_bytes().to_vec())
            .await
            .unwrap()
            .unwrap();

        assert_eq!("test_key2".as_bytes(), kv.key);
        assert_eq!("test_val2".as_bytes(), kv.value);

        let may_kv = in_mem.get("test_key3".as_bytes().to_vec()).await.unwrap();

        assert!(may_kv.is_none());
    }

    #[tokio::test]
    async fn test_batch_get() {
        let mut in_mem = Arc::new(MemStore::new()) as KvStoreRef;

        put_stats_to_store(&mut in_mem).await;

        let keys = vec![
            "test_key1".as_bytes().to_vec(),
            "test_key1".as_bytes().to_vec(),
            "test_key2".as_bytes().to_vec(),
        ];

        let kvs = in_mem.batch_get(keys).await.unwrap();

        assert_eq!(3, kvs.len());

        assert_eq!("test_key1".as_bytes(), kvs[0].key);
        assert_eq!("test_key1".as_bytes(), kvs[1].key);
        assert_eq!("test_key2".as_bytes(), kvs[2].key);

        assert_eq!("test_val1".as_bytes(), kvs[0].value);
        assert_eq!("test_val1".as_bytes(), kvs[1].value);
        assert_eq!("test_val2".as_bytes(), kvs[2].value);
    }

    async fn put_stats_to_store(store: &mut KvStoreRef) {
        store
            .put(PutRequest {
                key: "test_key1".as_bytes().to_vec(),
                value: "test_val1".as_bytes().to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        store
            .put(PutRequest {
                key: "test_key2".as_bytes().to_vec(),
                value: "test_val2".as_bytes().to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
}
