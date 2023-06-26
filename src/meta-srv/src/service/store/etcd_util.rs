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

use api::v1::meta::KeyValue;

pub struct KvPair<'a>(&'a etcd_client::KeyValue);

impl<'a> KvPair<'a> {
    /// Creates a `KvPair` from etcd KeyValue
    #[inline]
    pub fn new(kv: &'a etcd_client::KeyValue) -> Self {
        Self(kv)
    }

    #[inline]
    pub fn from_etcd_kv(kv: &etcd_client::KeyValue) -> KeyValue {
        KeyValue::from(KvPair::new(kv))
    }
}

impl<'a> From<KvPair<'a>> for KeyValue {
    fn from(kv: KvPair<'a>) -> Self {
        Self {
            key: kv.0.key().to_vec(),
            value: kv.0.value().to_vec(),
        }
    }
}
