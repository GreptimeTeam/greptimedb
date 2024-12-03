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
use std::hash::Hash;

use common_error::ext::BoxedError;
use common_meta::kv_backend::KvBackend;
use common_meta::peer::{Peer, PeerLookupService};
use common_meta::{utils, ClusterId, DatanodeId, FlownodeId};
use common_time::util as time_util;
use snafu::ResultExt;

use crate::cluster::MetaPeerClientRef;
use crate::error::{Error, Result};
use crate::key::{DatanodeLeaseKey, FlownodeLeaseKey, LeaseValue};

fn build_lease_filter(lease_secs: u64) -> impl Fn(&LeaseValue) -> bool {
    move |v: &LeaseValue| {
        ((time_util::current_time_millis() - v.timestamp_millis) as u64)
            < lease_secs.checked_mul(1000).unwrap_or(u64::MAX)
    }
}

/// look up [`Peer`] given [`ClusterId`] and [`DatanodeId`], will only return if it's alive under given `lease_secs`
pub async fn lookup_datanode_peer(
    cluster_id: ClusterId,
    datanode_id: DatanodeId,
    meta_peer_client: &MetaPeerClientRef,
    lease_secs: u64,
) -> Result<Option<Peer>> {
    let lease_filter = build_lease_filter(lease_secs);
    let lease_key = DatanodeLeaseKey {
        cluster_id,
        node_id: datanode_id,
    };
    let lease_key_bytes: Vec<u8> = lease_key.clone().try_into()?;
    let Some(kv) = meta_peer_client.get(&lease_key_bytes).await? else {
        return Ok(None);
    };
    let lease_value: LeaseValue = kv.value.try_into()?;
    let is_alive = lease_filter(&lease_value);
    if is_alive {
        Ok(Some(Peer {
            id: lease_key.node_id,
            addr: lease_value.node_addr,
        }))
    } else {
        Ok(None)
    }
}

/// Find all alive datanodes
pub async fn alive_datanodes(
    cluster_id: ClusterId,
    meta_peer_client: &MetaPeerClientRef,
    lease_secs: u64,
) -> Result<HashMap<DatanodeLeaseKey, LeaseValue>> {
    let predicate = build_lease_filter(lease_secs);
    filter(
        DatanodeLeaseKey::prefix_key_by_cluster(cluster_id),
        meta_peer_client,
        |v| predicate(v),
    )
    .await
}

/// look up [`Peer`] given [`ClusterId`] and [`DatanodeId`], only return if it's alive under given `lease_secs`
pub async fn lookup_flownode_peer(
    cluster_id: ClusterId,
    flownode_id: FlownodeId,
    meta_peer_client: &MetaPeerClientRef,
    lease_secs: u64,
) -> Result<Option<Peer>> {
    let lease_filter = build_lease_filter(lease_secs);
    let lease_key = FlownodeLeaseKey {
        cluster_id,
        node_id: flownode_id,
    };
    let lease_key_bytes: Vec<u8> = lease_key.clone().try_into()?;
    let Some(kv) = meta_peer_client.get(&lease_key_bytes).await? else {
        return Ok(None);
    };
    let lease_value: LeaseValue = kv.value.try_into()?;

    let is_alive = lease_filter(&lease_value);
    if is_alive {
        Ok(Some(Peer {
            id: lease_key.node_id,
            addr: lease_value.node_addr,
        }))
    } else {
        Ok(None)
    }
}

/// Find all alive flownodes
pub async fn alive_flownodes(
    cluster_id: ClusterId,
    meta_peer_client: &MetaPeerClientRef,
    lease_secs: u64,
) -> Result<HashMap<FlownodeLeaseKey, LeaseValue>> {
    let predicate = build_lease_filter(lease_secs);
    filter(
        FlownodeLeaseKey::prefix_key_by_cluster(cluster_id),
        meta_peer_client,
        |v| predicate(v),
    )
    .await
}

pub async fn filter<P, K>(
    key: Vec<u8>,
    meta_peer_client: &MetaPeerClientRef,
    predicate: P,
) -> Result<HashMap<K, LeaseValue>>
where
    P: Fn(&LeaseValue) -> bool,
    K: Eq + Hash + TryFrom<Vec<u8>, Error = Error>,
{
    let range_end = utils::get_prefix_end_key(&key);
    let range_req = common_meta::rpc::store::RangeRequest {
        key,
        range_end,
        keys_only: false,
        ..Default::default()
    };
    let kvs = meta_peer_client.range(range_req).await?.kvs;
    let mut lease_kvs = HashMap::new();
    for kv in kvs {
        let lease_key: K = kv.key.try_into()?;
        let lease_value: LeaseValue = kv.value.try_into()?;
        if !predicate(&lease_value) {
            continue;
        }
        let _ = lease_kvs.insert(lease_key, lease_value);
    }

    Ok(lease_kvs)
}

#[derive(Clone)]
pub struct MetaPeerLookupService {
    pub meta_peer_client: MetaPeerClientRef,
}

impl MetaPeerLookupService {
    pub fn new(meta_peer_client: MetaPeerClientRef) -> Self {
        Self { meta_peer_client }
    }
}

#[async_trait::async_trait]
impl PeerLookupService for MetaPeerLookupService {
    async fn datanode(
        &self,
        cluster_id: ClusterId,
        id: DatanodeId,
    ) -> common_meta::error::Result<Option<Peer>> {
        lookup_datanode_peer(cluster_id, id, &self.meta_peer_client, u64::MAX)
            .await
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)
    }
    async fn flownode(
        &self,
        cluster_id: ClusterId,
        id: FlownodeId,
    ) -> common_meta::error::Result<Option<Peer>> {
        lookup_flownode_peer(cluster_id, id, &self.meta_peer_client, u64::MAX)
            .await
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)
    }
}
