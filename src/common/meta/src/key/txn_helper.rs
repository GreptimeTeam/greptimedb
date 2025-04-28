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
use crate::key::{DeserializedValueWithBytes, MetadataValue};
use crate::kv_backend::txn::TxnOpResponse;
use crate::rpc::KeyValue;

/// The response set of [TxnOpResponse::ResponseGet]
pub struct TxnOpGetResponseSet(Vec<KeyValue>);

impl TxnOpGetResponseSet {
    /// Returns a filter to consume a [KeyValue] where the key equals `key`.
    pub fn filter(key: Vec<u8>) -> impl FnMut(&mut TxnOpGetResponseSet) -> Option<Vec<u8>> {
        move |set| {
            let pos = set.0.iter().position(|kv| kv.key == key);
            match pos {
                Some(pos) => Some(set.0.remove(pos).value),
                None => None,
            }
        }
    }

    /// Returns a decoder to decode bytes to `DeserializedValueWithBytes<T>`.
    pub fn decode_with<F, T>(
        mut f: F,
    ) -> impl FnMut(&mut TxnOpGetResponseSet) -> Result<Option<DeserializedValueWithBytes<T>>>
    where
        F: FnMut(&mut TxnOpGetResponseSet) -> Option<Vec<u8>>,
        T: Serialize + DeserializeOwned + MetadataValue,
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
            .extract_if(.., |resp| matches!(resp, TxnOpResponse::ResponseGet(_)))
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
