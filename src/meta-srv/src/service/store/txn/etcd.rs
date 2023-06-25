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
use etcd_client::{
    Compare as EtcdCompare, CompareOp as EtcdCompareOp, Txn as EtcdTxn, TxnOp as EtcdTxnOp,
    TxnOpResponse as EtcdTxnOpResponse, TxnResponse as EtcdTxnResponse,
};

use crate::service::store::etcd_util::KvPair;
use crate::service::store::txn::{Compare, CompareOp, Txn, TxnOp, TxnOpResponse, TxnResponse};

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
        match cmp.op {
            CompareOp::Equal => EtcdCompare::value(cmp.key, EtcdCompareOp::Equal, cmp.target),
            CompareOp::Greater => EtcdCompare::value(cmp.key, EtcdCompareOp::Greater, cmp.target),
            CompareOp::Less => EtcdCompare::value(cmp.key, EtcdCompareOp::Less, cmp.target),
            CompareOp::NotEqual => EtcdCompare::value(cmp.key, EtcdCompareOp::NotEqual, cmp.target),
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

impl From<EtcdTxnOpResponse> for TxnOpResponse {
    fn from(op_resp: EtcdTxnOpResponse) -> Self {
        match op_resp {
            EtcdTxnOpResponse::Put(res) => {
                let prev_kv = res.prev_key().map(KvPair::from_etcd_kv);
                let put_res = PutResponse {
                    prev_kv,
                    ..Default::default()
                };
                TxnOpResponse::ResponsePut(put_res)
            }
            EtcdTxnOpResponse::Get(res) => {
                let kvs = res.kvs().iter().map(KvPair::from_etcd_kv).collect();
                let range_res = RangeResponse {
                    kvs,
                    ..Default::default()
                };
                TxnOpResponse::ResponseGet(range_res)
            }
            EtcdTxnOpResponse::Delete(res) => {
                let prev_kvs = res
                    .prev_kvs()
                    .iter()
                    .map(KvPair::from_etcd_kv)
                    .collect::<Vec<_>>();
                let delete_res = DeleteRangeResponse {
                    prev_kvs,
                    deleted: res.deleted(),
                    ..Default::default()
                };
                TxnOpResponse::ResponseDelete(delete_res)
            }
            EtcdTxnOpResponse::Txn(_) => unimplemented!("nested txn is not supported"),
        }
    }
}

impl From<EtcdTxnResponse> for TxnResponse {
    fn from(resp: EtcdTxnResponse) -> Self {
        let succeeded = resp.succeeded();
        let responses = resp
            .op_responses()
            .into_iter()
            .map(TxnOpResponse::from)
            .collect::<Vec<_>>();
        Self {
            succeeded,
            responses,
        }
    }
}
