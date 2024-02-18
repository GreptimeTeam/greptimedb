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

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::Result;
use crate::key::{DeserializedValueWithBytes, TableMetaValue};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp, TxnOpResponse};

pub(crate) fn build_txn_response_decoder_fn<T>(
    raw_key: Vec<u8>,
) -> impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<DeserializedValueWithBytes<T>>>
where
    T: Serialize + DeserializeOwned + TableMetaValue,
{
    move |txn_res: &Vec<TxnOpResponse>| {
        txn_res
            .iter()
            .filter_map(|resp| {
                if let TxnOpResponse::ResponseGet(r) = resp {
                    Some(r)
                } else {
                    None
                }
            })
            .flat_map(|r| &r.kvs)
            .find(|kv| kv.key == raw_key)
            .map(|kv| DeserializedValueWithBytes::from_inner_slice(&kv.value))
            .transpose()
    }
}

pub(crate) fn build_put_if_absent_txn(key: Vec<u8>, value: Vec<u8>) -> Txn {
    Txn::new()
        .when(vec![Compare::with_not_exist_value(
            key.clone(),
            CompareOp::Equal,
        )])
        .and_then(vec![TxnOp::Put(key.clone(), value)])
        .or_else(vec![TxnOp::Get(key)])
}

pub(crate) fn build_compare_and_put_txn(key: Vec<u8>, old_value: Vec<u8>, value: Vec<u8>) -> Txn {
    Txn::new()
        .when(vec![Compare::with_value(
            key.clone(),
            CompareOp::Equal,
            old_value,
        )])
        .and_then(vec![TxnOp::Put(key.clone(), value)])
        .or_else(vec![TxnOp::Get(key)])
}
