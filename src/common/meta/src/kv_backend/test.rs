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

use txn::{Compare, CompareOp, TxnOp};

use super::{KvBackend, *};
use crate::error::Error;
use crate::rpc::store::{BatchGetRequest, PutRequest};
use crate::rpc::KeyValue;
use crate::util;

pub fn mock_kvs(prefix: Vec<u8>) -> Vec<KeyValue> {
    vec![
        KeyValue {
            key: [prefix.clone(), b"key1".to_vec()].concat(),
            value: b"val1".to_vec(),
        },
        KeyValue {
            key: [prefix.clone(), b"key2".to_vec()].concat(),
            value: b"val2".to_vec(),
        },
        KeyValue {
            key: [prefix.clone(), b"key3".to_vec()].concat(),
            value: b"val3".to_vec(),
        },
        KeyValue {
            key: [prefix.clone(), b"key11".to_vec()].concat(),
            value: b"val11".to_vec(),
        },
    ]
}

pub async fn prepare_kv(kv_backend: &impl KvBackend) {
    prepare_kv_with_prefix(kv_backend, vec![]).await;
}

pub async fn prepare_kv_with_prefix(kv_backend: &impl KvBackend, prefix: Vec<u8>) {
    let kvs = mock_kvs(prefix);
    assert!(kv_backend
        .batch_put(BatchPutRequest {
            kvs,
            ..Default::default()
        })
        .await
        .is_ok());
}

pub async fn unprepare_kv(kv_backend: &impl KvBackend, prefix: &[u8]) {
    let range_end = util::get_prefix_end_key(prefix);
    assert!(
        kv_backend
            .delete_range(DeleteRangeRequest {
                key: prefix.to_vec(),
                range_end,
                ..Default::default()
            })
            .await
            .is_ok(),
        "prefix: {:?}",
        std::str::from_utf8(prefix).unwrap()
    );
}

pub async fn test_kv_put(kv_backend: &impl KvBackend) {
    test_kv_put_with_prefix(kv_backend, vec![]).await;
}

pub async fn test_kv_put_with_prefix(kv_backend: &impl KvBackend, prefix: Vec<u8>) {
    let put_key = [prefix.clone(), b"key11".to_vec()].concat();
    let resp = kv_backend
        .put(PutRequest {
            key: put_key.clone(),
            value: b"val12".to_vec(),
            prev_kv: false,
        })
        .await
        .unwrap();
    assert!(resp.prev_kv.is_none());

    let resp = kv_backend
        .put(PutRequest {
            key: put_key.clone(),
            value: b"val13".to_vec(),
            prev_kv: true,
        })
        .await
        .unwrap();
    let prev_kv = resp.prev_kv.unwrap();
    assert_eq!(put_key, prev_kv.key());
    assert_eq!(b"val12", prev_kv.value());
}

pub async fn test_kv_range(kv_backend: &impl KvBackend) {
    test_kv_range_with_prefix(kv_backend, vec![]).await;
}

pub async fn test_kv_range_with_prefix(kv_backend: &impl KvBackend, prefix: Vec<u8>) {
    let key = [prefix.clone(), b"key1".to_vec()].concat();
    let key11 = [prefix.clone(), b"key11".to_vec()].concat();
    let range_end = util::get_prefix_end_key(&key);

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
    assert_eq!(key, resp.kvs[0].key);
    assert_eq!(b"val1", resp.kvs[0].value());
    assert_eq!(key11, resp.kvs[1].key);
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
    assert_eq!(key, resp.kvs[0].key);
    assert_eq!(b"", resp.kvs[0].value());
    assert_eq!(key11, resp.kvs[1].key);
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
    assert_eq!(key, resp.kvs[0].key);
    assert_eq!(b"val1", resp.kvs[0].value());

    let resp = kv_backend
        .range(RangeRequest {
            key: key.clone(),
            range_end,
            limit: 1,
            keys_only: false,
        })
        .await
        .unwrap();

    assert_eq!(1, resp.kvs.len());
    assert_eq!(key, resp.kvs[0].key);
    assert_eq!(b"val1", resp.kvs[0].value());
}

pub async fn test_kv_range_2(kv_backend: &impl KvBackend) {
    test_kv_range_2_with_prefix(kv_backend, vec![]).await;
}

pub async fn test_kv_range_2_with_prefix(kv_backend: &impl KvBackend, prefix: Vec<u8>) {
    let atest = [prefix.clone(), b"atest".to_vec()].concat();
    let test = [prefix.clone(), b"test".to_vec()].concat();

    kv_backend
        .put(
            PutRequest::new()
                .with_key(atest.clone())
                .with_value("value"),
        )
        .await
        .unwrap();

    kv_backend
        .put(PutRequest::new().with_key(test.clone()).with_value("value"))
        .await
        .unwrap();

    // If both key and range_end are ‘\0’, then range represents all keys.
    let all_start = [prefix.clone(), b"\0".to_vec()].concat();
    let all_end = if prefix.is_empty() {
        b"\0".to_vec()
    } else {
        util::get_prefix_end_key(&prefix)
    };
    let result = kv_backend
        .range(RangeRequest::new().with_range(all_start, all_end.clone()))
        .await
        .unwrap();

    assert_eq!(result.kvs.len(), 2);
    assert!(!result.more);

    // If range_end is ‘\0’, the range is all keys greater than or equal to the key argument.
    let a_start = [prefix.clone(), b"a".to_vec()].concat();
    let result = kv_backend
        .range(RangeRequest::new().with_range(a_start.clone(), all_end.clone()))
        .await
        .unwrap();

    assert_eq!(result.kvs.len(), 2);

    let b_start = [prefix.clone(), b"b".to_vec()].concat();
    let result = kv_backend
        .range(RangeRequest::new().with_range(b_start, all_end.clone()))
        .await
        .unwrap();

    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].key, test);

    // Fetches the keys >= "a", set limit to 1, the `more` should be true.
    let result = kv_backend
        .range(
            RangeRequest::new()
                .with_range(a_start.clone(), all_end.clone())
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
                .with_range(a_start.clone(), all_end.clone())
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
                .with_range(a_start.clone(), all_end.clone())
                .with_limit(3),
        )
        .await
        .unwrap();
    assert_eq!(result.kvs.len(), 2);
    assert!(!result.more);

    let req = BatchDeleteRequest {
        keys: vec![atest, test],
        prev_kv: false,
    };
    let resp = kv_backend.batch_delete(req).await.unwrap();
    assert!(resp.prev_kvs.is_empty());
}

pub async fn test_kv_batch_get(kv_backend: &impl KvBackend) {
    test_kv_batch_get_with_prefix(kv_backend, vec![]).await;
}

pub async fn test_kv_batch_get_with_prefix(kv_backend: &impl KvBackend, prefix: Vec<u8>) {
    let keys = vec![];
    let resp = kv_backend
        .batch_get(BatchGetRequest { keys })
        .await
        .unwrap();

    assert!(resp.kvs.is_empty());

    let key10 = [prefix.clone(), b"key10".to_vec()].concat();
    let keys = vec![key10];
    let resp = kv_backend
        .batch_get(BatchGetRequest { keys })
        .await
        .unwrap();

    assert!(resp.kvs.is_empty());

    let key1 = [prefix.clone(), b"key1".to_vec()].concat();
    let key3 = [prefix.clone(), b"key3".to_vec()].concat();
    let key4 = [prefix.clone(), b"key4".to_vec()].concat();
    let keys = vec![key1.clone(), key3.clone(), key4];
    let resp = kv_backend
        .batch_get(BatchGetRequest { keys })
        .await
        .unwrap();

    assert_eq!(2, resp.kvs.len());
    assert_eq!(key1, resp.kvs[0].key);
    assert_eq!(b"val1", resp.kvs[0].value());
    assert_eq!(key3, resp.kvs[1].key);
    assert_eq!(b"val3", resp.kvs[1].value());
}

pub async fn test_kv_compare_and_put(kv_backend: Arc<dyn KvBackend<Error = Error>>) {
    test_kv_compare_and_put_with_prefix(kv_backend, vec![]).await;
}

pub async fn test_kv_compare_and_put_with_prefix(
    kv_backend: Arc<dyn KvBackend<Error = Error>>,
    prefix: Vec<u8>,
) {
    let success = Arc::new(AtomicU8::new(0));
    let key = [prefix.clone(), b"key".to_vec()].concat();

    let mut joins = vec![];
    for _ in 0..20 {
        let kv_backend_clone = kv_backend.clone();
        let success_clone = success.clone();
        let key_clone = key.clone();

        let join = tokio::spawn(async move {
            let req = CompareAndPutRequest {
                key: key_clone,
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

    let resp = kv_backend.delete(&key, false).await.unwrap();
    assert!(resp.is_none());
}

pub async fn test_kv_delete_range(kv_backend: &impl KvBackend) {
    test_kv_delete_range_with_prefix(kv_backend, vec![]).await;
}

pub async fn test_kv_delete_range_with_prefix(kv_backend: &impl KvBackend, prefix: Vec<u8>) {
    let key3 = [prefix.clone(), b"key3".to_vec()].concat();
    let req = DeleteRangeRequest {
        key: key3.clone(),
        range_end: vec![],
        prev_kv: true,
    };

    let resp = kv_backend.delete_range(req).await.unwrap();
    assert_eq!(1, resp.prev_kvs.len());
    assert_eq!(1, resp.deleted);
    assert_eq!(key3, resp.prev_kvs[0].key);
    assert_eq!(b"val3", resp.prev_kvs[0].value());

    let resp = kv_backend.get(&key3).await.unwrap();
    assert!(resp.is_none());

    let key2 = [prefix.clone(), b"key2".to_vec()].concat();
    let req = DeleteRangeRequest {
        key: key2.clone(),
        range_end: vec![],
        prev_kv: false,
    };

    let resp = kv_backend.delete_range(req).await.unwrap();
    assert_eq!(1, resp.deleted);
    assert!(resp.prev_kvs.is_empty());

    let resp = kv_backend.get(&key2).await.unwrap();
    assert!(resp.is_none());

    let key = [prefix.clone(), b"key1".to_vec()].concat();
    let range_end = util::get_prefix_end_key(&key);

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

pub async fn test_kv_batch_delete(kv_backend: &impl KvBackend) {
    test_kv_batch_delete_with_prefix(kv_backend, vec![]).await;
}

pub async fn test_kv_batch_delete_with_prefix(kv_backend: &impl KvBackend, prefix: Vec<u8>) {
    let key1 = [prefix.clone(), b"key1".to_vec()].concat();
    let key100 = [prefix.clone(), b"key100".to_vec()].concat();
    assert!(kv_backend.get(&key1).await.unwrap().is_some());
    assert!(kv_backend.get(&key100).await.unwrap().is_none());

    let req = BatchDeleteRequest {
        keys: vec![key1.clone(), key100.clone()],
        prev_kv: true,
    };
    let resp = kv_backend.batch_delete(req).await.unwrap();
    assert_eq!(1, resp.prev_kvs.len());
    assert_eq!(
        vec![KeyValue {
            key: key1.clone(),
            value: b"val1".to_vec()
        }],
        resp.prev_kvs
    );
    assert!(kv_backend.get(&key1).await.unwrap().is_none());

    let key2 = [prefix.clone(), b"key2".to_vec()].concat();
    let key3 = [prefix.clone(), b"key3".to_vec()].concat();
    let key11 = [prefix.clone(), b"key11".to_vec()].concat();
    assert!(kv_backend.get(&key2).await.unwrap().is_some());
    assert!(kv_backend.get(&key3).await.unwrap().is_some());
    assert!(kv_backend.get(&key11).await.unwrap().is_some());

    let req = BatchDeleteRequest {
        keys: vec![key2.clone(), key3.clone(), key11.clone()],
        prev_kv: false,
    };
    let resp = kv_backend.batch_delete(req).await.unwrap();
    assert!(resp.prev_kvs.is_empty());

    assert!(kv_backend.get(&key2).await.unwrap().is_none());
    assert!(kv_backend.get(&key3).await.unwrap().is_none());
    assert!(kv_backend.get(&key11).await.unwrap().is_none());
}

pub async fn test_txn_one_compare_op(kv_backend: &impl KvBackend) {
    let _ = kv_backend
        .put(PutRequest {
            key: vec![11],
            value: vec![3],
            ..Default::default()
        })
        .await
        .unwrap();

    let txn = Txn::new()
        .when(vec![Compare::with_value(
            vec![11],
            CompareOp::Greater,
            vec![1],
        )])
        .and_then(vec![TxnOp::Put(vec![11], vec![1])])
        .or_else(vec![TxnOp::Put(vec![11], vec![2])]);

    let txn_response = kv_backend.txn(txn).await.unwrap();

    assert!(txn_response.succeeded);
    assert_eq!(txn_response.responses.len(), 1);
}

pub async fn text_txn_multi_compare_op(kv_backend: &impl KvBackend) {
    for i in 1..3 {
        let _ = kv_backend
            .put(PutRequest {
                key: vec![i],
                value: vec![i],
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let when: Vec<_> = (1..3u8)
        .map(|i| Compare::with_value(vec![i], CompareOp::Equal, vec![i]))
        .collect();

    let txn = Txn::new()
        .when(when)
        .and_then(vec![
            TxnOp::Put(vec![1], vec![10]),
            TxnOp::Put(vec![2], vec![20]),
        ])
        .or_else(vec![TxnOp::Put(vec![1], vec![11])]);

    let txn_response = kv_backend.txn(txn).await.unwrap();

    assert!(txn_response.succeeded);
    assert_eq!(txn_response.responses.len(), 2);
}

pub async fn test_txn_compare_equal(kv_backend: &impl KvBackend) {
    let key = vec![101u8];
    kv_backend.delete(&key, false).await.unwrap();

    let txn = Txn::new()
        .when(vec![Compare::with_value_not_exists(
            key.clone(),
            CompareOp::Equal,
        )])
        .and_then(vec![TxnOp::Put(key.clone(), vec![1])])
        .or_else(vec![TxnOp::Put(key.clone(), vec![2])]);
    let txn_response = kv_backend.txn(txn.clone()).await.unwrap();
    assert!(txn_response.succeeded);

    let txn_response = kv_backend.txn(txn).await.unwrap();
    assert!(!txn_response.succeeded);

    let txn = Txn::new()
        .when(vec![Compare::with_value(
            key.clone(),
            CompareOp::Equal,
            vec![2],
        )])
        .and_then(vec![TxnOp::Put(key.clone(), vec![3])])
        .or_else(vec![TxnOp::Put(key, vec![4])]);
    let txn_response = kv_backend.txn(txn).await.unwrap();
    assert!(txn_response.succeeded);
}

pub async fn test_txn_compare_greater(kv_backend: &impl KvBackend) {
    let key = vec![102u8];
    kv_backend.delete(&key, false).await.unwrap();

    let txn = Txn::new()
        .when(vec![Compare::with_value_not_exists(
            key.clone(),
            CompareOp::Greater,
        )])
        .and_then(vec![TxnOp::Put(key.clone(), vec![1])])
        .or_else(vec![TxnOp::Put(key.clone(), vec![2])]);
    let txn_response = kv_backend.txn(txn.clone()).await.unwrap();
    assert!(!txn_response.succeeded);

    let txn_response = kv_backend.txn(txn).await.unwrap();
    assert!(txn_response.succeeded);

    let txn = Txn::new()
        .when(vec![Compare::with_value(
            key.clone(),
            CompareOp::Greater,
            vec![1],
        )])
        .and_then(vec![TxnOp::Put(key.clone(), vec![3])])
        .or_else(vec![TxnOp::Get(key.clone())]);
    let mut txn_response = kv_backend.txn(txn).await.unwrap();
    assert!(!txn_response.succeeded);
    let res = txn_response.responses.pop().unwrap();
    assert_eq!(
        res,
        TxnOpResponse::ResponseGet(RangeResponse {
            kvs: vec![KeyValue {
                key,
                value: vec![1]
            }],
            more: false,
        })
    );
}

pub async fn test_txn_compare_less(kv_backend: &impl KvBackend) {
    let key = vec![103u8];
    kv_backend.delete(&[3], false).await.unwrap();

    let txn = Txn::new()
        .when(vec![Compare::with_value_not_exists(
            key.clone(),
            CompareOp::Less,
        )])
        .and_then(vec![TxnOp::Put(key.clone(), vec![1])])
        .or_else(vec![TxnOp::Put(key.clone(), vec![2])]);
    let txn_response = kv_backend.txn(txn.clone()).await.unwrap();
    assert!(!txn_response.succeeded);

    let txn_response = kv_backend.txn(txn).await.unwrap();
    assert!(!txn_response.succeeded);

    let txn = Txn::new()
        .when(vec![Compare::with_value(
            key.clone(),
            CompareOp::Less,
            vec![2],
        )])
        .and_then(vec![TxnOp::Put(key.clone(), vec![3])])
        .or_else(vec![TxnOp::Get(key.clone())]);
    let mut txn_response = kv_backend.txn(txn).await.unwrap();
    assert!(!txn_response.succeeded);
    let res = txn_response.responses.pop().unwrap();
    assert_eq!(
        res,
        TxnOpResponse::ResponseGet(RangeResponse {
            kvs: vec![KeyValue {
                key,
                value: vec![2]
            }],
            more: false,
        })
    );
}

pub async fn test_txn_compare_not_equal(kv_backend: &impl KvBackend) {
    let key = vec![104u8];
    kv_backend.delete(&key, false).await.unwrap();

    let txn = Txn::new()
        .when(vec![Compare::with_value_not_exists(
            key.clone(),
            CompareOp::NotEqual,
        )])
        .and_then(vec![TxnOp::Put(key.clone(), vec![1])])
        .or_else(vec![TxnOp::Put(key.clone(), vec![2])]);
    let txn_response = kv_backend.txn(txn.clone()).await.unwrap();
    assert!(!txn_response.succeeded);

    let txn_response = kv_backend.txn(txn).await.unwrap();
    assert!(txn_response.succeeded);

    let txn = Txn::new()
        .when(vec![Compare::with_value(
            key.clone(),
            CompareOp::Equal,
            vec![2],
        )])
        .and_then(vec![TxnOp::Put(key.clone(), vec![3])])
        .or_else(vec![TxnOp::Get(key.clone())]);
    let mut txn_response = kv_backend.txn(txn).await.unwrap();
    assert!(!txn_response.succeeded);
    let res = txn_response.responses.pop().unwrap();
    assert_eq!(
        res,
        TxnOpResponse::ResponseGet(RangeResponse {
            kvs: vec![KeyValue {
                key,
                value: vec![1]
            }],
            more: false,
        })
    );
}
