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

use etcd_client::{
    Compare as EtcdCompare, CompareOp as EtcdCompareOp, Txn as EtcdTxn, TxnOp as EtcdTxnOp,
    TxnOpResponse as EtcdTxnOpResponse, TxnResponse as EtcdTxnResponse,
};

use crate::error::{self, Result};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp, TxnOpResponse, TxnResponse};
use crate::rpc::store::{DeleteRangeResponse, PutResponse, RangeResponse};
use crate::rpc::KeyValue;

impl From<Txn> for EtcdTxn {
    fn from(txn: Txn) -> Self {
        let mut etcd_txn = EtcdTxn::new();
        if txn.c_when {
            let compares = txn
                .req
                .compare
                .into_iter()
                .map(EtcdCompare::from)
                .collect::<Vec<_>>();
            etcd_txn = etcd_txn.when(compares);
        }
        if txn.c_then {
            let success = txn
                .req
                .success
                .into_iter()
                .map(EtcdTxnOp::from)
                .collect::<Vec<_>>();
            etcd_txn = etcd_txn.and_then(success);
        }
        if txn.c_else {
            let failure = txn
                .req
                .failure
                .into_iter()
                .map(EtcdTxnOp::from)
                .collect::<Vec<_>>();
            etcd_txn = etcd_txn.or_else(failure);
        }
        etcd_txn
    }
}

impl From<Compare> for EtcdCompare {
    fn from(cmp: Compare) -> Self {
        let etcd_cmp = match cmp.cmp {
            CompareOp::Equal => EtcdCompareOp::Equal,
            CompareOp::Greater => EtcdCompareOp::Greater,
            CompareOp::Less => EtcdCompareOp::Less,
            CompareOp::NotEqual => EtcdCompareOp::NotEqual,
        };
        match cmp.target {
            Some(target) => EtcdCompare::value(cmp.key, etcd_cmp, target),
            // create revision 0 means key does not exist
            None => EtcdCompare::create_revision(cmp.key, etcd_cmp, 0),
        }
    }
}

impl From<TxnOp> for EtcdTxnOp {
    fn from(op: TxnOp) -> Self {
        match op {
            TxnOp::Put(key, value) => EtcdTxnOp::put(key, value, None),
            TxnOp::Get(key) => EtcdTxnOp::get(key, None),
            TxnOp::Delete(key) => EtcdTxnOp::delete(key, None),
        }
    }
}

impl TryFrom<EtcdTxnOpResponse> for TxnOpResponse {
    type Error = error::Error;

    fn try_from(op_resp: EtcdTxnOpResponse) -> Result<Self> {
        match op_resp {
            EtcdTxnOpResponse::Put(mut res) => {
                let prev_kv = res.take_prev_key().map(KeyValue::from);
                Ok(TxnOpResponse::ResponsePut(PutResponse { prev_kv }))
            }
            EtcdTxnOpResponse::Get(mut res) => {
                let kvs = res.take_kvs().into_iter().map(KeyValue::from).collect();
                Ok(TxnOpResponse::ResponseGet(RangeResponse {
                    kvs,
                    more: false,
                }))
            }
            EtcdTxnOpResponse::Delete(mut res) => {
                let deleted = res.deleted();
                let prev_kvs = res
                    .take_prev_kvs()
                    .into_iter()
                    .map(KeyValue::from)
                    .collect::<Vec<_>>();
                Ok(TxnOpResponse::ResponseDelete(DeleteRangeResponse {
                    deleted,
                    prev_kvs,
                }))
            }
            EtcdTxnOpResponse::Txn(_) => error::EtcdTxnOpResponseSnafu {
                err_msg: "nested txn is not supported",
            }
            .fail(),
        }
    }
}

impl TryFrom<EtcdTxnResponse> for TxnResponse {
    type Error = error::Error;

    fn try_from(resp: EtcdTxnResponse) -> Result<Self> {
        let succeeded = resp.succeeded();
        let responses = resp
            .op_responses()
            .into_iter()
            .map(TxnOpResponse::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            succeeded,
            responses,
        })
    }
}
