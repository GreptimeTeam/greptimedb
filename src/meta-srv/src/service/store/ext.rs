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

use api::v1::meta::{DeleteRangeRequest, KeyValue, RangeRequest};

use crate::error::Result;
use crate::service::store::kv::KvStore;

#[async_trait::async_trait]
pub trait KvStoreExt {
    /// Get the value by the given key.
    async fn get(&self, key: Vec<u8>) -> Result<Option<KeyValue>>;

    /// Check if a key exists, it does not return the value.
    async fn exists(&self, key: Vec<u8>) -> Result<bool>;

    /// Delete the value by the given key. If prev_kv is true,
    /// gets the previous key-value pairs before deleting it.
    /// The previous key-value pairs will be returned.
    async fn delete(&self, key: Vec<u8>, prev_kv: bool) -> Result<Option<KeyValue>>;
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

        Ok(kvs.pop())
    }

    async fn exists(&self, key: Vec<u8>) -> Result<bool> {
        let req = RangeRequest {
            key,
            keys_only: true,
            ..Default::default()
        };

        let kvs = self.range(req).await?.kvs;

        Ok(!kvs.is_empty())
    }

    async fn delete(&self, key: Vec<u8>, prev_kv: bool) -> Result<Option<KeyValue>> {
        let req = DeleteRangeRequest {
            key,
            prev_kv,
            ..Default::default()
        };

        let mut prev_kvs = self.delete_range(req).await?.prev_kvs;

        Ok(prev_kvs.pop())
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
    async fn test_exists() {
        let mut in_mem = Arc::new(MemStore::new()) as KvStoreRef;

        put_stats_to_store(&mut in_mem).await;

        assert!(in_mem
            .exists("test_key1".as_bytes().to_vec())
            .await
            .unwrap());
        assert!(in_mem
            .exists("test_key2".as_bytes().to_vec())
            .await
            .unwrap());
        assert!(!in_mem
            .exists("test_key3".as_bytes().to_vec())
            .await
            .unwrap());
        assert!(!in_mem.exists("test_key".as_bytes().to_vec()).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete() {
        let mut in_mem = Arc::new(MemStore::new()) as KvStoreRef;

        let mut prev_kv = in_mem
            .delete("test_key1".as_bytes().to_vec(), true)
            .await
            .unwrap();
        assert!(prev_kv.is_none());

        put_stats_to_store(&mut in_mem).await;

        assert!(in_mem
            .exists("test_key1".as_bytes().to_vec())
            .await
            .unwrap());

        prev_kv = in_mem
            .delete("test_key1".as_bytes().to_vec(), true)
            .await
            .unwrap();
        assert!(prev_kv.is_some());
        assert_eq!("test_key1".as_bytes(), prev_kv.unwrap().key);
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
