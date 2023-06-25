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
pub trait TxnService: Send + Sync {
    async fn txn(&self, txn: Txn) -> Result<TxnResponse>;
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
    pub target: Vec<u8>,
    pub op: CompareOp,
}

impl Compare {
    pub fn cmp_with_value(&self, value: &Vec<u8>) -> bool {
        match self.op {
            CompareOp::Equal => *value == self.target,
            CompareOp::Greater => *value > self.target,
            CompareOp::Less => *value < self.target,
            CompareOp::NotEqual => *value != self.target,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TxnOp {
    Put(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Debug, Clone, Default)]
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

#[derive(Debug, Clone, Default)]
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
