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

use api::v1::meta::{DeleteRangeResponse, PutResponse, RangeResponse};

use crate::error::Result;

mod etcd;

#[async_trait::async_trait]
pub trait TxnService: Sync + Send {
    async fn txn(&self, _txn: Txn) -> Result<TxnResponse> {
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
    pub target: Vec<u8>,
}

impl Compare {
    pub fn new(key: Vec<u8>, cmp: CompareOp, target: Vec<u8>) -> Self {
        Self { key, cmp, target }
    }

    pub fn compare_with_value(&self, value: &Vec<u8>) -> bool {
        match self.cmp {
            CompareOp::Equal => *value == self.target,
            CompareOp::Greater => *value > self.target,
            CompareOp::Less => *value < self.target,
            CompareOp::NotEqual => *value != self.target,
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
    req: TxnRequest,
    c_when: bool,
    c_then: bool,
    c_else: bool,
}

impl Txn {
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

    use api::v1::meta::PutRequest;

    use super::*;
    use crate::service::store::kv::KvStoreRef;
    use crate::service::store::memory::MemStore;

    #[test]
    fn test_compare() {
        let compare = Compare::new(vec![1], CompareOp::Equal, vec![1]);
        assert!(compare.compare_with_value(&vec![1]));
        assert!(!compare.compare_with_value(&vec![]));

        let compare = Compare::new(vec![1], CompareOp::Equal, vec![]);
        assert!(compare.compare_with_value(&vec![]));

        let compare = Compare::new(vec![1], CompareOp::Greater, vec![1]);
        assert!(compare.compare_with_value(&vec![2]));
        assert!(!compare.compare_with_value(&vec![]));

        let compare = Compare::new(vec![1], CompareOp::Less, vec![1]);
        assert!(compare.compare_with_value(&vec![0]));
        assert!(compare.compare_with_value(&vec![]));

        let compare = Compare::new(vec![1], CompareOp::NotEqual, vec![1]);
        assert!(compare.compare_with_value(&vec![2]));
        assert!(compare.compare_with_value(&vec![0]));
    }

    #[test]
    fn test_txn() {
        let txn = Txn::new()
            .when(vec![Compare::new(vec![1], CompareOp::Equal, vec![1])])
            .and_then(vec![TxnOp::Put(vec![1], vec![1])])
            .or_else(vec![TxnOp::Put(vec![1], vec![2])]);

        assert_eq!(
            txn,
            Txn {
                req: TxnRequest {
                    compare: vec![Compare::new(vec![1], CompareOp::Equal, vec![1])],
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
        let kv_store = create_kv_store().await;

        let _ = kv_store
            .put(PutRequest {
                key: vec![11],
                value: vec![3],
                ..Default::default()
            })
            .await
            .unwrap();

        let txn = Txn::new()
            .when(vec![Compare::new(vec![11], CompareOp::Greater, vec![1])])
            .and_then(vec![TxnOp::Put(vec![11], vec![1])])
            .or_else(vec![TxnOp::Put(vec![11], vec![2])]);

        let txn_response = kv_store.txn(txn).await.unwrap();

        assert!(txn_response.succeeded);
        assert_eq!(txn_response.responses.len(), 1);
    }

    #[tokio::test]
    async fn test_txn_multi_compare_op() {
        let kv_store = create_kv_store().await;

        for i in 1..3 {
            let _ = kv_store
                .put(PutRequest {
                    key: vec![i],
                    value: vec![i],
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        let when: Vec<_> = (1..3u8)
            .map(|i| Compare::new(vec![i], CompareOp::Equal, vec![i]))
            .collect();

        let txn = Txn::new()
            .when(when)
            .and_then(vec![
                TxnOp::Put(vec![1], vec![10]),
                TxnOp::Put(vec![2], vec![20]),
            ])
            .or_else(vec![TxnOp::Put(vec![1], vec![11])]);

        let txn_response = kv_store.txn(txn).await.unwrap();

        assert!(txn_response.succeeded);
        assert_eq!(txn_response.responses.len(), 2);
    }

    async fn create_kv_store() -> KvStoreRef {
        Arc::new(MemStore::new())
    }
}
