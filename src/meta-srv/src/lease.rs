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

use api::v1::meta::RangeRequest;

use crate::error::Result;
use crate::keys::{DnLeaseKey, LeaseValue, DN_LEASE_PREFIX};
use crate::service::store::kv::KvStoreRef;
use crate::util;

pub async fn alive_datanodes<P>(
    cluster_id: u64,
    kv_store: &KvStoreRef,
    predicate: P,
) -> Result<Vec<(DnLeaseKey, LeaseValue)>>
where
    P: Fn(&DnLeaseKey, &LeaseValue) -> bool,
{
    let key = get_lease_prefix(cluster_id);
    let range_end = util::get_prefix_end_key(&key);
    let req = RangeRequest {
        key,
        range_end,
        ..Default::default()
    };

    let res = kv_store.range(req).await?;

    let kvs = res.kvs;
    let mut lease_kvs = vec![];
    for kv in kvs {
        let lease_key: DnLeaseKey = kv.key.try_into()?;
        let lease_value: LeaseValue = kv.value.try_into()?;
        if !predicate(&lease_key, &lease_value) {
            continue;
        }
        lease_kvs.push((lease_key, lease_value));
    }

    Ok(lease_kvs)
}

#[inline]
pub fn get_lease_prefix(cluster_id: u64) -> Vec<u8> {
    format!("{DN_LEASE_PREFIX}-{cluster_id}").into_bytes()
}
