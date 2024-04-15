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
use crate::rpc::KeyValue;

/// The response set of [TxnOpResponse::ResponseGet]
pub(crate) struct TxnOpGetResponseSet(Vec<KeyValue>);

impl TxnOpGetResponseSet {
    /// Returns a [TxnOp] to retrieve the value corresponding `key` and
    /// a filter to consume corresponding [KeyValue] from [TxnOpGetResponseSet].
    pub(crate) fn build_get_op<T: Into<Vec<u8>>>(
        key: T,
    ) -> (
        TxnOp,
        impl FnMut(&'_ mut TxnOpGetResponseSet) -> Option<Vec<u8>>,
    ) {
        let key = key.into();
        (TxnOp::Get(key.clone()), TxnOpGetResponseSet::filter(key))
    }

    /// Returns a filter to consume a [KeyValue] where the key equals `key`.
    pub(crate) fn filter(key: Vec<u8>) -> impl FnMut(&mut TxnOpGetResponseSet) -> Option<Vec<u8>> {
        move |set| {
            let pos = set.0.iter().position(|kv| kv.key == key);
            match pos {
                Some(pos) => Some(set.0.remove(pos).value),
                None => None,
            }
        }
    }

    /// Returns a decoder to decode bytes to `DeserializedValueWithBytes<T>`.
    pub(crate) fn decode_with<F, T>(
        mut f: F,
    ) -> impl FnMut(&mut TxnOpGetResponseSet) -> Result<Option<DeserializedValueWithBytes<T>>>
    where
        F: FnMut(&mut TxnOpGetResponseSet) -> Option<Vec<u8>>,
        T: Serialize + DeserializeOwned + TableMetaValue,
    {
        move |set| {
            f(set)
                .map(|value| DeserializedValueWithBytes::from_inner_slice(&value))
                .transpose()
        }
    }
}

impl From<&mut Vec<TxnOpResponse>> for TxnOpGetResponseSet {
    fn from(value: &mut Vec<TxnOpResponse>) -> Self {
        let value = value
            .extract_if(|resp| matches!(resp, TxnOpResponse::ResponseGet(_)))
            .flat_map(|resp| {
                // Safety: checked
                let TxnOpResponse::ResponseGet(r) = resp else {
                    unreachable!()
                };

                r.kvs
            })
            .collect::<Vec<_>>();
        TxnOpGetResponseSet(value)
    }
}

// TODO(weny): using `TxnOpGetResponseSet`.
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
