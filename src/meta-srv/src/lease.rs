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

use std::collections::HashMap;

use common_meta::util;
use common_time::util as time_util;

use crate::cluster::MetaPeerClientRef;
use crate::error::Result;
use crate::keys::{LeaseKey, LeaseValue, DN_LEASE_PREFIX};

pub async fn alive_datanodes(
    cluster_id: u64,
    meta_peer_client: &MetaPeerClientRef,
    lease_secs: u64,
) -> Result<HashMap<LeaseKey, LeaseValue>> {
    let lease_filter = |_: &LeaseKey, v: &LeaseValue| {
        ((time_util::current_time_millis() - v.timestamp_millis) as u64) < lease_secs * 1000
    };

    filter_datanodes(cluster_id, meta_peer_client, lease_filter).await
}

pub async fn filter_datanodes<P>(
    cluster_id: u64,
    meta_peer_client: &MetaPeerClientRef,
    predicate: P,
) -> Result<HashMap<LeaseKey, LeaseValue>>
where
    P: Fn(&LeaseKey, &LeaseValue) -> bool,
{
    let key = get_lease_prefix(cluster_id);
    let range_end = util::get_prefix_end_key(&key);

    let kvs = meta_peer_client.range(key, range_end, false).await?;
    let mut lease_kvs = HashMap::new();
    for kv in kvs {
        let lease_key: LeaseKey = kv.key.try_into()?;
        let lease_value: LeaseValue = kv.value.try_into()?;
        if !predicate(&lease_key, &lease_value) {
            continue;
        }
        let _ = lease_kvs.insert(lease_key, lease_value);
    }

    Ok(lease_kvs)
}

#[inline]
pub fn get_lease_prefix(cluster_id: u64) -> Vec<u8> {
    format!("{DN_LEASE_PREFIX}-{cluster_id}").into_bytes()
}
