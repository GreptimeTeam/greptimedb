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

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use super::{KvBackend, *};
use crate::error::Error;
use crate::rpc::store::{BatchGetRequest, PutRequest};
use crate::rpc::KeyValue;
use crate::util;

pub fn mock_kvs() -> Vec<KeyValue> {
    vec![
        KeyValue {
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
        },
        KeyValue {
            key: b"key2".to_vec(),
            value: b"val2".to_vec(),
        },
        KeyValue {
            key: b"key3".to_vec(),
            value: b"val3".to_vec(),
        },
    ]
}

pub async fn prepare_kv(kv_backend: &impl KvBackend) {
    let kvs = mock_kvs();
    assert!(kv_backend
        .batch_put(BatchPutRequest {
            kvs,
            ..Default::default()
        })
        .await
        .is_ok());

    assert!(kv_backend
        .put(PutRequest {
            key: b"key11".to_vec(),
            value: b"val11".to_vec(),
            ..Default::default()
        })
        .await
        .is_ok());
}

pub async fn test_kv_put(kv_backend: impl KvBackend) {
    let resp = kv_backend
        .put(PutRequest {
            key: b"key11".to_vec(),
            value: b"val12".to_vec(),
            prev_kv: false,
        })
        .await
        .unwrap();
    assert!(resp.prev_kv.is_none());

    let resp = kv_backend
        .put(PutRequest {
            key: b"key11".to_vec(),
            value: b"val13".to_vec(),
            prev_kv: true,
        })
        .await
        .unwrap();
    let prev_kv = resp.prev_kv.unwrap();
    assert_eq!(b"key11", prev_kv.key());
    assert_eq!(b"val12", prev_kv.value());
}

pub async fn test_kv_range(kv_backend: impl KvBackend) {
    let key = b"key1".to_vec();
    let range_end = util::get_prefix_end_key(b"key1");

    let resp = kv_backend
        .range(RangeRequest {
            key: key.clone(),
            range_end: range_end.clone(),
            limit: 0,
            keys_only: false,
        })
        .await
        .unwrap();

    assert_eq!(2, resp.kvs.len());
    assert_eq!(b"key1", resp.kvs[0].key());
    assert_eq!(b"val1", resp.kvs[0].value());
    assert_eq!(b"key11", resp.kvs[1].key());
    assert_eq!(b"val11", resp.kvs[1].value());

    let resp = kv_backend
        .range(RangeRequest {
            key: key.clone(),
            range_end: range_end.clone(),
            limit: 0,
            keys_only: true,
        })
        .await
        .unwrap();

    assert_eq!(2, resp.kvs.len());
    assert_eq!(b"key1", resp.kvs[0].key());
    assert_eq!(b"", resp.kvs[0].value());
    assert_eq!(b"key11", resp.kvs[1].key());
    assert_eq!(b"", resp.kvs[1].value());

    let resp = kv_backend
        .range(RangeRequest {
            key: key.clone(),
            limit: 0,
            keys_only: false,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(1, resp.kvs.len());
    assert_eq!(b"key1", resp.kvs[0].key());
    assert_eq!(b"val1", resp.kvs[0].value());

    let resp = kv_backend
        .range(RangeRequest {
            key,
            range_end,
            limit: 1,
            keys_only: false,
        })
        .await
        .unwrap();

    assert_eq!(1, resp.kvs.len());
    assert_eq!(b"key1", resp.kvs[0].key());
    assert_eq!(b"val1", resp.kvs[0].value());
}

pub async fn test_kv_range_2(kv_backend: impl KvBackend) {
    kv_backend
        .put(PutRequest::new().with_key("atest").with_value("value"))
        .await
        .unwrap();

    kv_backend
        .put(PutRequest::new().with_key("test").with_value("value"))
        .await
        .unwrap();

    // If both key and range_end are ‘\0’, then range represents all keys.
    let result = kv_backend
        .range(RangeRequest::new().with_range(b"\0".to_vec(), b"\0".to_vec()))
        .await
        .unwrap();

    assert_eq!(result.kvs.len(), 2);
    assert!(!result.more);

    // If range_end is ‘\0’, the range is all keys greater than or equal to the key argument.
    let result = kv_backend
        .range(RangeRequest::new().with_range(b"a".to_vec(), b"\0".to_vec()))
        .await
        .unwrap();

    assert_eq!(result.kvs.len(), 2);

    let result = kv_backend
        .range(RangeRequest::new().with_range(b"b".to_vec(), b"\0".to_vec()))
        .await
        .unwrap();

    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].key, b"test");

    // Fetches the keys >= "a", set limit to 1, the `more` should be true.
    let result = kv_backend
        .range(
            RangeRequest::new()
                .with_range(b"a".to_vec(), b"\0".to_vec())
                .with_limit(1),
        )
        .await
        .unwrap();
    assert_eq!(result.kvs.len(), 1);
    assert!(result.more);

    // Fetches the keys >= "a", set limit to 2, the `more` should be false.
    let result = kv_backend
        .range(
            RangeRequest::new()
                .with_range(b"a".to_vec(), b"\0".to_vec())
                .with_limit(2),
        )
        .await
        .unwrap();
    assert_eq!(result.kvs.len(), 2);
    assert!(!result.more);

    // Fetches the keys >= "a", set limit to 3, the `more` should be false.
    let result = kv_backend
        .range(
            RangeRequest::new()
                .with_range(b"a".to_vec(), b"\0".to_vec())
                .with_limit(3),
        )
        .await
        .unwrap();
    assert_eq!(result.kvs.len(), 2);
    assert!(!result.more);
}

pub async fn test_kv_batch_get(kv_backend: impl KvBackend) {
    let keys = vec![];
    let resp = kv_backend
        .batch_get(BatchGetRequest { keys })
        .await
        .unwrap();

    assert!(resp.kvs.is_empty());

    let keys = vec![b"key10".to_vec()];
    let resp = kv_backend
        .batch_get(BatchGetRequest { keys })
        .await
        .unwrap();

    assert!(resp.kvs.is_empty());

    let keys = vec![b"key1".to_vec(), b"key3".to_vec(), b"key4".to_vec()];
    let resp = kv_backend
        .batch_get(BatchGetRequest { keys })
        .await
        .unwrap();

    assert_eq!(2, resp.kvs.len());
    assert_eq!(b"key1", resp.kvs[0].key());
    assert_eq!(b"val1", resp.kvs[0].value());
    assert_eq!(b"key3", resp.kvs[1].key());
    assert_eq!(b"val3", resp.kvs[1].value());
}

pub async fn test_kv_compare_and_put(kv_backend: Arc<dyn KvBackend<Error = Error>>) {
    let success = Arc::new(AtomicU8::new(0));

    let mut joins = vec![];
    for _ in 0..20 {
        let kv_backend_clone = kv_backend.clone();
        let success_clone = success.clone();
        let join = tokio::spawn(async move {
            let req = CompareAndPutRequest {
                key: b"key".to_vec(),
                expect: vec![],
                value: b"val_new".to_vec(),
            };
            let resp = kv_backend_clone.compare_and_put(req).await.unwrap();
            if resp.success {
                success_clone.fetch_add(1, Ordering::SeqCst);
            }
        });
        joins.push(join);
    }

    for join in joins {
        join.await.unwrap();
    }

    assert_eq!(1, success.load(Ordering::SeqCst));
}

pub async fn test_kv_delete_range(kv_backend: impl KvBackend) {
    let req = DeleteRangeRequest {
        key: b"key3".to_vec(),
        range_end: vec![],
        prev_kv: true,
    };

    let resp = kv_backend.delete_range(req).await.unwrap();
    assert_eq!(1, resp.prev_kvs.len());
    assert_eq!(1, resp.deleted);
    assert_eq!(b"key3", resp.prev_kvs[0].key());
    assert_eq!(b"val3", resp.prev_kvs[0].value());

    let resp = kv_backend.get(b"key3").await.unwrap();
    assert!(resp.is_none());

    let req = DeleteRangeRequest {
        key: b"key2".to_vec(),
        range_end: vec![],
        prev_kv: false,
    };

    let resp = kv_backend.delete_range(req).await.unwrap();
    assert_eq!(1, resp.deleted);
    assert!(resp.prev_kvs.is_empty());

    let resp = kv_backend.get(b"key2").await.unwrap();
    assert!(resp.is_none());

    let key = b"key1".to_vec();
    let range_end = util::get_prefix_end_key(b"key1");

    let req = DeleteRangeRequest {
        key: key.clone(),
        range_end: range_end.clone(),
        prev_kv: true,
    };
    let resp = kv_backend.delete_range(req).await.unwrap();
    assert_eq!(2, resp.prev_kvs.len());

    let req = RangeRequest {
        key,
        range_end,
        ..Default::default()
    };
    let resp = kv_backend.range(req).await.unwrap();
    assert!(resp.kvs.is_empty());
}

pub async fn test_kv_batch_delete(kv_backend: impl KvBackend) {
    assert!(kv_backend.get(b"key1").await.unwrap().is_some());
    assert!(kv_backend.get(b"key100").await.unwrap().is_none());

    let req = BatchDeleteRequest {
        keys: vec![b"key1".to_vec(), b"key100".to_vec()],
        prev_kv: true,
    };
    let resp = kv_backend.batch_delete(req).await.unwrap();
    assert_eq!(1, resp.prev_kvs.len());
    assert_eq!(
        vec![KeyValue {
            key: b"key1".to_vec(),
            value: b"val1".to_vec()
        }],
        resp.prev_kvs
    );
    assert!(kv_backend.get(b"key1").await.unwrap().is_none());

    assert!(kv_backend.get(b"key2").await.unwrap().is_some());
    assert!(kv_backend.get(b"key3").await.unwrap().is_some());

    let req = BatchDeleteRequest {
        keys: vec![b"key2".to_vec(), b"key3".to_vec()],
        prev_kv: false,
    };
    let resp = kv_backend.batch_delete(req).await.unwrap();
    assert!(resp.prev_kvs.is_empty());

    assert!(kv_backend.get(b"key2").await.unwrap().is_none());
    assert!(kv_backend.get(b"key3").await.unwrap().is_none());
}
