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

use common_error::ext::ErrorExt;

use crate::rpc::store::{DeleteRangeResponse, PutResponse, RangeResponse};

mod etcd;

#[async_trait::async_trait]
pub trait TxnService: Sync + Send {
    type Error: ErrorExt;

    async fn txn(&self, _txn: Txn) -> Result<TxnResponse, Self::Error> {
        unimplemented!("txn is not implemented")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompareOp {
    Equal,
    Greater,
    Less,
    NotEqual,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Compare {
    pub key: Vec<u8>,
    pub cmp: CompareOp,
    /// None means the key does not exist.
    pub target: Option<Vec<u8>>,
}

impl Compare {
    pub fn new(key: Vec<u8>, cmp: CompareOp, target: Option<Vec<u8>>) -> Self {
        Self { key, cmp, target }
    }

    pub fn with_value(key: Vec<u8>, cmp: CompareOp, target: Vec<u8>) -> Self {
        Self::new(key, cmp, Some(target))
    }

    pub fn with_not_exist_value(key: Vec<u8>, cmp: CompareOp) -> Self {
        Self::new(key, cmp, None)
    }

    pub fn compare_with_value(&self, value: Option<&Vec<u8>>) -> bool {
        match (value, &self.target) {
            (Some(value), Some(target)) => match self.cmp {
                CompareOp::Equal => *value == *target,
                CompareOp::Greater => *value > *target,
                CompareOp::Less => *value < *target,
                CompareOp::NotEqual => *value != *target,
            },
            (Some(_), None) => match self.cmp {
                CompareOp::Equal => false,
                CompareOp::Greater => true,
                CompareOp::Less => false,
                CompareOp::NotEqual => true,
            },
            (None, Some(_)) => match self.cmp {
                CompareOp::Equal => false,
                CompareOp::Greater => false,
                CompareOp::Less => true,
                CompareOp::NotEqual => true,
            },
            (None, None) => match self.cmp {
                CompareOp::Equal => true,
                CompareOp::Greater => false,
                CompareOp::Less => false,
                CompareOp::NotEqual => false,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TxnOp {
    Put(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TxnRequest {
    pub compare: Vec<Compare>,
    pub success: Vec<TxnOp>,
    pub failure: Vec<TxnOp>,
}

impl TxnRequest {
    pub fn extend(&mut self, other: TxnRequest) {
        self.compare.extend(other.compare);
        self.success.extend(other.success);
        self.failure.extend(other.failure);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TxnOpResponse {
    ResponsePut(PutResponse),
    ResponseGet(RangeResponse),
    ResponseDelete(DeleteRangeResponse),
}

pub struct TxnResponse {
    pub succeeded: bool,
    pub responses: Vec<TxnOpResponse>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Txn {
    // HACK - chroot would modify this field
    pub(super) req: TxnRequest,
    c_when: bool,
    c_then: bool,
    c_else: bool,
}

impl Txn {
    pub fn merge_all<T: IntoIterator<Item = Txn>>(values: T) -> Self {
        values
            .into_iter()
            .reduce(|acc, e| acc.merge(e))
            .unwrap_or_default()
    }

    pub fn merge(mut self, other: Txn) -> Self {
        self.c_when |= other.c_when;
        self.c_then |= other.c_then;
        self.c_else |= other.c_else;

        self.req.extend(other.req);

        self
    }

    pub fn new() -> Self {
        Txn::default()
    }

    /// Takes a list of comparison. If all comparisons passed in succeed,
    /// the operations passed into `and_then()` will be executed. Or the operations
    /// passed into `or_else()` will be executed.
    #[inline]
    pub fn when(mut self, compares: impl Into<Vec<Compare>>) -> Self {
        assert!(!self.c_when, "cannot call `when` twice");
        assert!(!self.c_then, "cannot call `when` after `and_then`");
        assert!(!self.c_else, "cannot call `when` after `or_else`");

        self.c_when = true;
        self.req.compare = compares.into();
        self
    }

    /// Takes a list of operations. The operations list will be executed, if the
    /// comparisons passed into `when()` succeed.
    #[inline]
    pub fn and_then(mut self, operations: impl Into<Vec<TxnOp>>) -> Self {
        assert!(!self.c_then, "cannot call `and_then` twice");
        assert!(!self.c_else, "cannot call `and_then` after `or_else`");

        self.c_then = true;
        self.req.success = operations.into();
        self
    }

    /// Takes a list of operations. The operations list will be executed, if the
    /// comparisons passed into `when()` fail.
    #[inline]
    pub fn or_else(mut self, operations: impl Into<Vec<TxnOp>>) -> Self {
        assert!(!self.c_else, "cannot call `or_else` twice");

        self.c_else = true;
        self.req.failure = operations.into();
        self
    }
}

impl From<Txn> for TxnRequest {
    fn from(txn: Txn) -> Self {
        txn.req
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::error::Error;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::KvBackendRef;
    use crate::rpc::store::PutRequest;
    use crate::rpc::KeyValue;

    #[test]
    fn test_compare() {
        // Equal
        let compare = Compare::with_value(vec![1], CompareOp::Equal, vec![1]);
        assert!(compare.compare_with_value(Some(&vec![1])));
        assert!(!compare.compare_with_value(None));
        let compare = Compare::with_not_exist_value(vec![1], CompareOp::Equal);
        assert!(compare.compare_with_value(None));

        // Greater
        let compare = Compare::with_value(vec![1], CompareOp::Greater, vec![1]);
        assert!(compare.compare_with_value(Some(&vec![2])));
        assert!(!compare.compare_with_value(None));
        let compare = Compare::with_not_exist_value(vec![1], CompareOp::Greater);
        assert!(!compare.compare_with_value(None));
        assert!(compare.compare_with_value(Some(&vec![1])));

        // Less
        let compare = Compare::with_value(vec![1], CompareOp::Less, vec![1]);
        assert!(compare.compare_with_value(Some(&vec![0])));
        assert!(compare.compare_with_value(None));
        let compare = Compare::with_not_exist_value(vec![1], CompareOp::Less);
        assert!(!compare.compare_with_value(None));
        assert!(!compare.compare_with_value(Some(&vec![1])));

        // NotEqual
        let compare = Compare::with_value(vec![1], CompareOp::NotEqual, vec![1]);
        assert!(!compare.compare_with_value(Some(&vec![1])));
        assert!(compare.compare_with_value(Some(&vec![2])));
        assert!(compare.compare_with_value(None));
        let compare = Compare::with_not_exist_value(vec![1], CompareOp::NotEqual);
        assert!(!compare.compare_with_value(None));
        assert!(compare.compare_with_value(Some(&vec![1])));
    }

    #[test]
    fn test_txn() {
        let txn = Txn::new()
            .when(vec![Compare::with_value(
                vec![1],
                CompareOp::Equal,
                vec![1],
            )])
            .and_then(vec![TxnOp::Put(vec![1], vec![1])])
            .or_else(vec![TxnOp::Put(vec![1], vec![2])]);

        assert_eq!(
            txn,
            Txn {
                req: TxnRequest {
                    compare: vec![Compare::with_value(vec![1], CompareOp::Equal, vec![1])],
                    success: vec![TxnOp::Put(vec![1], vec![1])],
                    failure: vec![TxnOp::Put(vec![1], vec![2])],
                },
                c_when: true,
                c_then: true,
                c_else: true,
            }
        );
    }

    #[tokio::test]
    async fn test_txn_one_compare_op() {
        let kv_backend = create_kv_backend().await;

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

    #[tokio::test]
    async fn test_txn_multi_compare_op() {
        let kv_backend = create_kv_backend().await;

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

    #[tokio::test]
    async fn test_txn_compare_equal() {
        let kv_backend = create_kv_backend().await;
        let key = vec![101u8];
        kv_backend.delete(&key, false).await.unwrap();

        let txn = Txn::new()
            .when(vec![Compare::with_not_exist_value(
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

    #[tokio::test]
    async fn test_txn_compare_greater() {
        let kv_backend = create_kv_backend().await;
        let key = vec![102u8];
        kv_backend.delete(&key, false).await.unwrap();

        let txn = Txn::new()
            .when(vec![Compare::with_not_exist_value(
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

    #[tokio::test]
    async fn test_txn_compare_less() {
        let kv_backend = create_kv_backend().await;
        let key = vec![103u8];
        kv_backend.delete(&[3], false).await.unwrap();

        let txn = Txn::new()
            .when(vec![Compare::with_not_exist_value(
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

    #[tokio::test]
    async fn test_txn_compare_not_equal() {
        let kv_backend = create_kv_backend().await;
        let key = vec![104u8];
        kv_backend.delete(&key, false).await.unwrap();

        let txn = Txn::new()
            .when(vec![Compare::with_not_exist_value(
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

    async fn create_kv_backend() -> KvBackendRef {
        Arc::new(MemoryKvBackend::<Error>::new())
        // TODO(jiachun): Add a feature to test against etcd in github CI
        //
        // The same test can be run against etcd by uncommenting the following line
        // crate::service::store::etcd::EtcdStore::with_endpoints(["127.0.0.1:2379"])
        //     .await
        //     .unwrap()
    }
}
