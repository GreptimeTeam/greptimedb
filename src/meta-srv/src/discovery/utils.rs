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
use std::time::Duration;

use api::v1::meta::heartbeat_request::NodeWorkloads;
use common_meta::DatanodeId;
use common_meta::cluster::NodeInfo;
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_time::util::{DefaultSystemTimer, SystemTimer};
use common_workload::DatanodeWorkloadType;
use futures::TryStreamExt;
use snafu::ResultExt;

use crate::discovery::lease::{LeaseValueAccessor, LeaseValueType};
use crate::discovery::node_info::{NodeInfoAccessor, NodeInfoType};
use crate::error::{KvBackendSnafu, Result};
use crate::key::{DatanodeLeaseKey, LeaseValue};

impl LastActiveTs for LeaseValue {
    fn last_active_ts(&self) -> i64 {
        self.timestamp_millis
    }
}

impl LastActiveTs for NodeInfo {
    fn last_active_ts(&self) -> i64 {
        self.last_activity_ts
    }
}

/// Trait for types that have a last active timestamp.
pub trait LastActiveTs {
    /// Returns the last active timestamp in milliseconds.
    fn last_active_ts(&self) -> i64;
}

/// Builds a filter closure that checks whether a [`LastActiveTs`] item
/// is still within the specified active duration, relative to the
/// current time provided by the given [`SystemTimer`].
///
/// The returned closure uses the timestamp at the time of building,
/// so the "now" reference point is fixed when this function is called.
pub fn build_active_filter<T: LastActiveTs>(
    timer: impl SystemTimer,
    active_duration: Duration,
) -> impl Fn(&T) -> bool {
    let now = timer.current_time_millis();
    let active_duration = active_duration.as_millis() as u64;
    move |item: &T| {
        let elapsed = now.saturating_sub(item.last_active_ts()) as u64;
        elapsed < active_duration
    }
}

/// Returns the alive datanode lease values.
pub async fn alive_datanode_lease_values(
    lister: &impl LeaseValueAccessor,
    active_duration: Duration,
    condition: Option<fn(&NodeWorkloads) -> bool>,
) -> Result<HashMap<u64, LeaseValue>> {
    let active_filter = build_active_filter(DefaultSystemTimer, active_duration);
    lister
        .lease_values(LeaseValueType::Datanode)
        .try_filter(|(_, lease_value)| {
            futures::future::ready(
                active_filter(lease_value) && condition.unwrap_or(|_| true)(&lease_value.workloads),
            )
        })
        .try_collect::<HashMap<_, _>>()
        .await
}

/// Returns the alive datanodes.
pub async fn alive_datanodes(
    lister: &impl LeaseValueAccessor,
    active_duration: Duration,
    condition: Option<fn(&NodeWorkloads) -> bool>,
) -> Result<Vec<Peer>> {
    let active_filter = build_active_filter(DefaultSystemTimer, active_duration);
    lister
        .lease_values(LeaseValueType::Datanode)
        .try_filter(|(_, lease_value)| {
            futures::future::ready(
                active_filter(lease_value) && condition.unwrap_or(|_| true)(&lease_value.workloads),
            )
        })
        .map_ok(|(peer_id, lease_value)| Peer::new(peer_id, lease_value.node_addr))
        .try_collect::<Vec<_>>()
        .await
}

/// Returns the alive flownodes.
pub async fn alive_flownodes(
    lister: &impl LeaseValueAccessor,
    active_duration: Duration,
    condition: Option<fn(&NodeWorkloads) -> bool>,
) -> Result<Vec<Peer>> {
    let active_filter = build_active_filter(DefaultSystemTimer, active_duration);
    lister
        .lease_values(LeaseValueType::Flownode)
        .try_filter(|(_, lease_value)| {
            futures::future::ready(
                active_filter(lease_value) && condition.unwrap_or(|_| true)(&lease_value.workloads),
            )
        })
        .map_ok(|(peer_id, lease_value)| Peer::new(peer_id, lease_value.node_addr))
        .try_collect::<Vec<_>>()
        .await
}

/// Returns the alive frontends.
pub async fn alive_frontends(
    lister: &impl NodeInfoAccessor,
    active_duration: Duration,
) -> Result<Vec<Peer>> {
    let active_filter = build_active_filter(DefaultSystemTimer, active_duration);
    lister
        .node_infos(NodeInfoType::Frontend)
        .try_filter(|(_, node_info)| futures::future::ready(active_filter(node_info)))
        .map_ok(|(_, node_info)| node_info.peer)
        .try_collect::<Vec<_>>()
        .await
}

/// Returns the alive datanode peer.
pub async fn alive_datanode(
    lister: &impl LeaseValueAccessor,
    peer_id: u64,
    active_duration: Duration,
) -> Result<Option<Peer>> {
    let active_filter = build_active_filter(DefaultSystemTimer, active_duration);
    let v = lister
        .lease_value(LeaseValueType::Datanode, peer_id)
        .await?
        .filter(|(_, lease)| active_filter(lease))
        .map(|(peer_id, lease)| Peer::new(peer_id, lease.node_addr));

    Ok(v)
}

/// Returns true if the datanode can accept ingest workload based on its workload types.
///
/// A datanode is considered to accept ingest workload if it supports either:
/// - Hybrid workload (both ingest and query workloads)
/// - Ingest workload (only ingest workload)
pub fn is_datanode_accept_ingest_workload(datanode_workloads: &NodeWorkloads) -> bool {
    match &datanode_workloads {
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
    in_memory: &KvBackendRef,
    datanode_id: DatanodeId,
) -> Result<Option<LeaseValue>> {
    let lease_key = DatanodeLeaseKey {
        node_id: datanode_id,
    };
    let lease_key_bytes: Vec<u8> = lease_key.try_into()?;
    let Some(kv) = in_memory
        .get(&lease_key_bytes)
        .await
        .context(KvBackendSnafu)?
    else {
        return Ok(None);
    };

    let lease_value: LeaseValue = kv.value.try_into()?;
    Ok(Some(lease_value))
}
