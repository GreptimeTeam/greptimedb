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
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::task::{Context, Poll};

use api::v1::meta::heartbeat_request::NodeWorkloads;
use common_error::ext::BoxedError;
use common_meta::kv_backend::{KvBackend, ResettableKvBackendRef};
use common_meta::peer::{Peer, PeerLookupService};
use common_meta::{util, DatanodeId, FlownodeId};
use common_time::util as time_util;
use common_workload::DatanodeWorkloadType;
use snafu::ResultExt;

use crate::cluster::MetaPeerClientRef;
use crate::error::{Error, KvBackendSnafu, Result};
use crate::key::{DatanodeLeaseKey, FlownodeLeaseKey, LeaseValue};

fn build_lease_filter(lease_secs: u64) -> impl Fn(&LeaseValue) -> bool {
    move |v: &LeaseValue| {
        ((time_util::current_time_millis() - v.timestamp_millis) as u64)
            < lease_secs.saturating_mul(1000)
    }
}

/// Returns true if the datanode can accept ingest workload based on its workload types.
///
/// A datanode is considered to accept ingest workload if it supports either:
/// - Hybrid workload (both ingest and query workloads)
/// - Ingest workload (only ingest workload)
pub fn is_datanode_accept_ingest_workload(lease_value: &LeaseValue) -> bool {
    match &lease_value.workloads {
        NodeWorkloads::Datanode(workloads) => workloads
            .types
            .iter()
            .filter_map(|w| DatanodeWorkloadType::from_i32(*w))
            .any(|w| w.accept_ingest()),
        _ => false,
    }
}

/// Returns the lease value of the given datanode id, if the datanode is not found, returns None.
pub async fn find_datanode_lease_value(
    datanode_id: DatanodeId,
    in_memory_key: &ResettableKvBackendRef,
) -> Result<Option<LeaseValue>> {
    let lease_key = DatanodeLeaseKey {
        node_id: datanode_id,
    };
    let lease_key_bytes: Vec<u8> = lease_key.try_into()?;
    let Some(kv) = in_memory_key
        .get(&lease_key_bytes)
        .await
        .context(KvBackendSnafu)?
    else {
        return Ok(None);
    };

    let lease_value: LeaseValue = kv.value.try_into()?;

    Ok(Some(lease_value))
}

/// look up [`Peer`] given [`ClusterId`] and [`DatanodeId`], will only return if it's alive under given `lease_secs`
pub async fn lookup_datanode_peer(
    datanode_id: DatanodeId,
    meta_peer_client: &MetaPeerClientRef,
    lease_secs: u64,
) -> Result<Option<Peer>> {
    let lease_filter = build_lease_filter(lease_secs);
    let lease_key = DatanodeLeaseKey {
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

type LeaseFilterFuture<'a, K> =
    Pin<Box<dyn Future<Output = Result<HashMap<K, LeaseValue>>> + Send + 'a>>;

pub struct LeaseFilter<'a, K>
where
    K: Eq + Hash + TryFrom<Vec<u8>, Error = Error> + 'a,
{
    lease_secs: u64,
    key_prefix: Vec<u8>,
    meta_peer_client: &'a MetaPeerClientRef,
    condition: Option<fn(&LeaseValue) -> bool>,
    inner_future: Option<LeaseFilterFuture<'a, K>>,
}

impl<'a, K> LeaseFilter<'a, K>
where
    K: Eq + Hash + TryFrom<Vec<u8>, Error = Error> + 'a,
{
    pub fn new(
        lease_secs: u64,
        key_prefix: Vec<u8>,
        meta_peer_client: &'a MetaPeerClientRef,
    ) -> Self {
        Self {
            lease_secs,
            key_prefix,
            meta_peer_client,
            condition: None,
            inner_future: None,
        }
    }

    /// Set the condition for the lease filter.
    pub fn with_condition(mut self, condition: fn(&LeaseValue) -> bool) -> Self {
        self.condition = Some(condition);
        self
    }
}

impl<'a, K> Future for LeaseFilter<'a, K>
where
    K: Eq + Hash + TryFrom<Vec<u8>, Error = Error> + 'a,
{
    type Output = Result<HashMap<K, LeaseValue>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if this.inner_future.is_none() {
            let lease_filter = build_lease_filter(this.lease_secs);
            let condition = this.condition;
            let key_prefix = std::mem::take(&mut this.key_prefix);
            let fut = filter(key_prefix, this.meta_peer_client, move |v| {
                lease_filter(v) && condition.unwrap_or(|_| true)(v)
            });

            this.inner_future = Some(Box::pin(fut));
        }

        let fut = this.inner_future.as_mut().unwrap();
        let result = futures::ready!(fut.as_mut().poll(cx))?;

        Poll::Ready(Ok(result))
    }
}

/// Find all alive datanodes
pub fn alive_datanodes(
    meta_peer_client: &MetaPeerClientRef,
    lease_secs: u64,
) -> LeaseFilter<'_, DatanodeLeaseKey> {
    LeaseFilter::new(lease_secs, DatanodeLeaseKey::prefix_key(), meta_peer_client)
}

/// look up [`Peer`] given [`ClusterId`] and [`DatanodeId`], only return if it's alive under given `lease_secs`
pub async fn lookup_flownode_peer(
    flownode_id: FlownodeId,
    meta_peer_client: &MetaPeerClientRef,
    lease_secs: u64,
) -> Result<Option<Peer>> {
    let lease_filter = build_lease_filter(lease_secs);
    let lease_key = FlownodeLeaseKey {
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
pub fn alive_flownodes(
    meta_peer_client: &MetaPeerClientRef,
    lease_secs: u64,
) -> LeaseFilter<'_, FlownodeLeaseKey> {
    LeaseFilter::new(
        lease_secs,
        FlownodeLeaseKey::prefix_key_by_cluster(),
        meta_peer_client,
    )
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
    let range_end = util::get_prefix_end_key(&key);
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
    async fn datanode(&self, id: DatanodeId) -> common_meta::error::Result<Option<Peer>> {
        lookup_datanode_peer(id, &self.meta_peer_client, u64::MAX)
            .await
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)
    }
    async fn flownode(&self, id: FlownodeId) -> common_meta::error::Result<Option<Peer>> {
        lookup_flownode_peer(id, &self.meta_peer_client, u64::MAX)
            .await
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::heartbeat_request::NodeWorkloads;
    use api::v1::meta::DatanodeWorkloads;
    use common_meta::kv_backend::ResettableKvBackendRef;
    use common_meta::rpc::store::PutRequest;
    use common_time::util::current_time_millis;
    use common_workload::DatanodeWorkloadType;

    use crate::key::{DatanodeLeaseKey, LeaseValue};
    use crate::lease::{alive_datanodes, is_datanode_accept_ingest_workload};
    use crate::test_util::create_meta_peer_client;

    async fn put_lease_value(
        kv_backend: &ResettableKvBackendRef,
        key: DatanodeLeaseKey,
        value: LeaseValue,
    ) {
        kv_backend
            .put(PutRequest {
                key: key.try_into().unwrap(),
                value: value.try_into().unwrap(),
                prev_kv: false,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_alive_datanodes() {
        let client = create_meta_peer_client();
        let in_memory = client.memory_backend();
        let lease_secs = 10;

        // put a stale lease value for node 1
        let key = DatanodeLeaseKey { node_id: 1 };
        let value = LeaseValue {
            // 20s ago
            timestamp_millis: current_time_millis() - lease_secs * 2 * 1000,
            node_addr: "127.0.0.1:20201".to_string(),
            workloads: NodeWorkloads::Datanode(DatanodeWorkloads {
                types: vec![DatanodeWorkloadType::Hybrid as i32],
            }),
        };
        put_lease_value(&in_memory, key, value).await;

        // put a fresh lease value for node 2
        let key = DatanodeLeaseKey { node_id: 2 };
        let value = LeaseValue {
            timestamp_millis: current_time_millis(),
            node_addr: "127.0.0.1:20202".to_string(),
            workloads: NodeWorkloads::Datanode(DatanodeWorkloads {
                types: vec![DatanodeWorkloadType::Hybrid as i32],
            }),
        };
        put_lease_value(&in_memory, key.clone(), value.clone()).await;
        let leases = alive_datanodes(&client, lease_secs as u64).await.unwrap();
        assert_eq!(leases.len(), 1);
        assert_eq!(leases.get(&key), Some(&value));
    }

    #[tokio::test]
    async fn test_alive_datanodes_with_condition() {
        let client = create_meta_peer_client();
        let in_memory = client.memory_backend();
        let lease_secs = 10;

        // put a lease value for node 1 without mode info
        let key = DatanodeLeaseKey { node_id: 1 };
        let value = LeaseValue {
            // 20s ago
            timestamp_millis: current_time_millis() - 20 * 1000,
            node_addr: "127.0.0.1:20201".to_string(),
            workloads: NodeWorkloads::Datanode(DatanodeWorkloads {
                types: vec![DatanodeWorkloadType::Hybrid as i32],
            }),
        };
        put_lease_value(&in_memory, key, value).await;

        // put a lease value for node 2 with mode info
        let key = DatanodeLeaseKey { node_id: 2 };
        let value = LeaseValue {
            timestamp_millis: current_time_millis(),
            node_addr: "127.0.0.1:20202".to_string(),
            workloads: NodeWorkloads::Datanode(DatanodeWorkloads {
                types: vec![DatanodeWorkloadType::Hybrid as i32],
            }),
        };
        put_lease_value(&in_memory, key, value).await;

        // put a lease value for node 3 with mode info
        let key = DatanodeLeaseKey { node_id: 3 };
        let value = LeaseValue {
            timestamp_millis: current_time_millis(),
            node_addr: "127.0.0.1:20203".to_string(),
            workloads: NodeWorkloads::Datanode(DatanodeWorkloads {
                types: vec![i32::MAX],
            }),
        };
        put_lease_value(&in_memory, key, value).await;

        // put a lease value for node 3 with mode info
        let key = DatanodeLeaseKey { node_id: 4 };
        let value = LeaseValue {
            timestamp_millis: current_time_millis(),
            node_addr: "127.0.0.1:20204".to_string(),
            workloads: NodeWorkloads::Datanode(DatanodeWorkloads {
                types: vec![i32::MAX],
            }),
        };
        put_lease_value(&in_memory, key, value).await;

        let leases = alive_datanodes(&client, lease_secs as u64)
            .with_condition(is_datanode_accept_ingest_workload)
            .await
            .unwrap();
        assert_eq!(leases.len(), 1);
        assert!(leases.contains_key(&DatanodeLeaseKey { node_id: 2 }));
    }
}
