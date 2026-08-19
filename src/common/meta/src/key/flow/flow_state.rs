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

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::error::{self, Result};
use crate::key::flow::FlowScoped;
use crate::key::{FlowId, MetadataKey, MetadataValue};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{PutRequest, RangeRequest};

/// The entire FlowId to Flow Size's Map is stored directly in the value part of the key.
pub const FLOW_STATE_KEY: &str = "state";

/// The inner prefix (under `state/`) of the per-flownode flow state keys.
pub const FLOW_STATE_NODE_KEY_PREFIX: &str = "node";

/// The key of flow state.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FlowStateKeyInner;

impl FlowStateKeyInner {
    pub fn new() -> Self {
        Self
    }
}

impl<'a> MetadataKey<'a, FlowStateKeyInner> for FlowStateKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        FLOW_STATE_KEY.as_bytes().to_vec()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowStateKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidMetadataSnafu {
                err_msg: format!(
                    "FlowInfoKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        if key != FLOW_STATE_KEY {
            return Err(error::InvalidMetadataSnafu {
                err_msg: format!("Invalid FlowStateKeyInner '{key}'"),
            }
            .build());
        }
        Ok(FlowStateKeyInner::new())
    }
}

/// The key stores the state size of the flow.
///
/// The layout: `__flow/state`.
pub struct FlowStateKey(FlowScoped<FlowStateKeyInner>);

impl FlowStateKey {
    /// Returns the [FlowStateKey].
    pub fn new() -> FlowStateKey {
        let inner = FlowStateKeyInner::new();
        FlowStateKey(FlowScoped::new(inner))
    }
}

impl Default for FlowStateKey {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> MetadataKey<'a, FlowStateKey> for FlowStateKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowStateKey> {
        Ok(FlowStateKey(FlowScoped::<FlowStateKeyInner>::from_bytes(
            bytes,
        )?))
    }
}

/// The inner key of a per-flownode flow state entry: `state/node/{node_id}`.
///
/// `node_id` is the operator-configured flownode id (unique within the
/// cluster; a flownode requires `node_id` in its config, see
/// `src/cmd/src/flownode.rs`). It is the same value reported as
/// `HeartbeatRequest.header.member_id` — the canonical identity metasrv uses
/// for flownodes, see `get_node_id` in `src/meta-srv/src/service/heartbeat.rs`
/// — and as `HeartbeatRequest.peer.id`.
#[derive(Debug, Clone, PartialEq)]
struct FlowStateNodeKeyInner {
    node_id: u64,
}

impl FlowStateNodeKeyInner {
    pub fn new(node_id: u64) -> Self {
        Self { node_id }
    }
}

impl<'a> MetadataKey<'a, FlowStateNodeKeyInner> for FlowStateNodeKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!(
            "{FLOW_STATE_KEY}/{FLOW_STATE_NODE_KEY_PREFIX}/{}",
            self.node_id
        )
        .into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowStateNodeKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidMetadataSnafu {
                err_msg: format!(
                    "FlowStateNodeKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let prefix = format!("{FLOW_STATE_KEY}/{FLOW_STATE_NODE_KEY_PREFIX}/");
        let Some(node_id) = key.strip_prefix(&prefix) else {
            return Err(error::InvalidMetadataSnafu {
                err_msg: format!("Invalid FlowStateNodeKeyInner '{key}'"),
            }
            .build());
        };
        let node_id = node_id.parse::<u64>().map_err(|_| {
            error::InvalidMetadataSnafu {
                err_msg: format!("Invalid node id '{node_id}' in FlowStateNodeKeyInner '{key}'"),
            }
            .build()
        })?;
        Ok(FlowStateNodeKeyInner::new(node_id))
    }
}

/// The key stores the per-flownode flow state report.
///
/// The layout: `__flow/state/node/{node_id}`.
///
/// Per-node keys live in the in-memory KV (same as the global `__flow/state`
/// key), so they are automatically cleared when metasrv resets the in-memory
/// KV on leader change; no separate cleanup is needed.
pub struct FlowStateNodeKey(FlowScoped<FlowStateNodeKeyInner>);

impl FlowStateNodeKey {
    /// Returns the [FlowStateNodeKey] of the given node.
    pub fn new(node_id: u64) -> FlowStateNodeKey {
        FlowStateNodeKey(FlowScoped::new(FlowStateNodeKeyInner::new(node_id)))
    }

    /// Returns the full key prefix of all per-node flow state keys:
    /// `__flow/state/node/`.
    pub fn prefix() -> Vec<u8> {
        format!(
            "{}{FLOW_STATE_KEY}/{FLOW_STATE_NODE_KEY_PREFIX}/",
            FlowScoped::<FlowStateNodeKeyInner>::PREFIX
        )
        .into_bytes()
    }
}

impl<'a> MetadataKey<'a, FlowStateNodeKey> for FlowStateNodeKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowStateNodeKey> {
        Ok(FlowStateNodeKey(
            FlowScoped::<FlowStateNodeKeyInner>::from_bytes(bytes)?,
        ))
    }
}

/// The value of flow state size
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct FlowStateValue {
    /// For each key, the bytes of the state in memory
    pub state_size: BTreeMap<FlowId, usize>,
    /// For each key, the last execution time of flow in unix timestamp milliseconds.
    pub last_exec_time_map: BTreeMap<FlowId, i64>,
}

impl FlowStateValue {
    pub fn new(
        state_size: BTreeMap<FlowId, usize>,
        last_exec_time_map: BTreeMap<FlowId, i64>,
    ) -> Self {
        Self {
            state_size,
            last_exec_time_map,
        }
    }
}

pub type FlowStateManagerRef = Arc<FlowStateManager>;

/// The manager of [FlowStateKey]. Since state size changes frequently, we store it in memory.
///
/// This is only used in distributed mode. When meta-srv use heartbeat to update the flow stat report
/// and frontned use get to get the latest flow stat report.
///
/// Per-flownode reports are stored under `__flow/state/node/{node_id}` keys in
/// the in-memory KV (not in a separate in-process map), so a metasrv leader
/// change — which resets the in-memory KV — automatically clears all per-node
/// state without leaving stale entries behind.
pub struct FlowStateManager {
    in_memory: KvBackendRef,
    /// Serializes the critical section of [`FlowStateManager::merge`]
    /// (write per-node key -> scan -> aggregate -> write global key). It holds
    /// no long-lived state; per-node reports live in the in-memory KV.
    merge_lock: Mutex<()>,
}

impl FlowStateManager {
    pub fn new(in_memory: KvBackendRef) -> Self {
        Self {
            in_memory,
            merge_lock: Mutex::new(()),
        }
    }

    pub async fn get(&self) -> Result<Option<FlowStateValue>> {
        let key = FlowStateKey::new().to_bytes();
        self.in_memory
            .get(&key)
            .await?
            .map(|x| FlowStateValue::try_from_raw_value(&x.value))
            .transpose()
    }

    pub async fn put(&self, value: FlowStateValue) -> Result<()> {
        let key = FlowStateKey::new().to_bytes();
        let value = value.try_as_raw_value()?;
        let req = PutRequest::new().with_key(key).with_value(value);
        self.in_memory.put(req).await?;
        Ok(())
    }

    /// Merges a flow state report from a single flownode into the global view.
    ///
    /// `node_id` is the operator-configured flownode id (unique within the
    /// cluster; reported as `HeartbeatRequest.header.member_id`, the canonical
    /// metasrv identity of a flownode, and equal to `peer.id`).
    ///
    /// Reports are tracked per node under `__flow/state/node/{node_id}` in the
    /// in-memory KV. A new report from the same node unconditionally replaces
    /// that node's previous entry: reports are processed strictly in arrival
    /// order, so no epoch comparison is made and a wall-clock rollback after a
    /// node restart cannot permanently drop the node's later reports. The
    /// global `FlowStateValue` is then aggregated over all per-node entries:
    /// for the same flow, `last_exec_time_map` takes the max reported
    /// timestamp and `state_size` takes the max reported size across nodes (a
    /// flow normally runs on a single active flownode, so max is a safe
    /// approximation). The aggregated value is written into the in-memory KV
    /// under the global key `__flow/state` via the same path as `put`, keeping
    /// `get()` behavior unchanged.
    ///
    /// Per-node keys are cleared automatically when the in-memory KV is reset
    /// on a leader change. Known limitation: entries of dropped flows are not
    /// proactively removed here. Once a flow is dropped its metadata is gone,
    /// so the flows table join simply can't see the stale entry; it becomes
    /// user-invisible until the owning flownode reports again (or stops
    /// heartbeating forever, in which case the stale flow id lingers in the
    /// aggregate but is never joined against any flow metadata).
    pub async fn merge(&self, node_id: u64, incoming: FlowStateValue) -> Result<()> {
        let _guard = self.merge_lock.lock().await;

        // 1. Store this node's latest report under its per-node key.
        let node_key = FlowStateNodeKey::new(node_id).to_bytes();
        let value = incoming.try_as_raw_value()?;
        let req = PutRequest::new().with_key(node_key).with_value(value);
        self.in_memory.put(req).await?;

        // 2. Read back every per-node report and aggregate them.
        let req = RangeRequest::new().with_prefix(FlowStateNodeKey::prefix());
        let resp = self.in_memory.range(req).await?;
        let mut state_size = BTreeMap::new();
        let mut last_exec_time_map = BTreeMap::new();
        for kv in resp.kvs {
            let state = FlowStateValue::try_from_raw_value(&kv.value)?;
            for (flow_id, size) in state.state_size {
                state_size
                    .entry(flow_id)
                    .and_modify(|v: &mut usize| *v = (*v).max(size))
                    .or_insert(size);
            }
            for (flow_id, ts) in state.last_exec_time_map {
                last_exec_time_map
                    .entry(flow_id)
                    .and_modify(|v: &mut i64| *v = (*v).max(ts))
                    .or_insert(ts);
            }
        }

        // 3. Write the aggregated value to the global key.
        let aggregated = FlowStateValue::new(state_size, last_exec_time_map);
        let key = FlowStateKey::new().to_bytes();
        let value = aggregated.try_as_raw_value()?;
        let req = PutRequest::new().with_key(key).with_value(value);
        self.in_memory.put(req).await?;
        Ok(())
    }
}

/// Flow's state report, send regularly through heartbeat message
#[derive(Debug, Clone, Default)]
pub struct FlowStat {
    /// For each key, the bytes of the state in memory
    pub state_size: BTreeMap<u32, usize>,
    /// For each key, the last execution time of flow in unix timestamp milliseconds.
    pub last_exec_time_map: BTreeMap<FlowId, i64>,
}

impl From<FlowStateValue> for FlowStat {
    fn from(value: FlowStateValue) -> Self {
        Self {
            state_size: value.state_size,
            last_exec_time_map: value.last_exec_time_map,
        }
    }
}

impl From<FlowStat> for FlowStateValue {
    fn from(value: FlowStat) -> Self {
        Self {
            state_size: value.state_size,
            last_exec_time_map: value.last_exec_time_map,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::key::FlowId;
    use crate::key::flow::flow_state::FlowStateValue;
    use crate::kv_backend::memory::MemoryKvBackend;

    fn state(last_exec_time_map: BTreeMap<FlowId, i64>) -> FlowStateValue {
        FlowStateValue::new(BTreeMap::new(), last_exec_time_map)
    }

    #[tokio::test]
    async fn test_merge_keeps_reports_from_different_nodes() {
        let manager = FlowStateManager::new(Arc::new(MemoryKvBackend::default()));

        // Node A reports flow 1 executed at t1.
        manager
            .merge(1, state(BTreeMap::from([(1, 100)])))
            .await
            .unwrap();
        // Node B reports flow 2 executed at t2. Before the per-node merge this
        // would have wiped out node A's flow 1 entry.
        manager
            .merge(2, state(BTreeMap::from([(2, 200)])))
            .await
            .unwrap();
        // Node A reports flow 1 executed at t3.
        manager
            .merge(1, state(BTreeMap::from([(1, 300)])))
            .await
            .unwrap();

        let value = manager.get().await.unwrap().unwrap();
        // flow 1 and flow 2 are both present, and flow 1 takes max(t1, t3).
        assert_eq!(value.last_exec_time_map.get(&1), Some(&300));
        assert_eq!(value.last_exec_time_map.get(&2), Some(&200));
    }

    #[tokio::test]
    async fn test_merge_replaces_same_node_state() {
        let manager = FlowStateManager::new(Arc::new(MemoryKvBackend::default()));

        manager
            .merge(1, state(BTreeMap::from([(1, 100)])))
            .await
            .unwrap();
        // A new report from the same node replaces the previous one. There is
        // no epoch comparison: arrival order alone decides, so this is also
        // what a restarted node (new epoch) hits.
        manager
            .merge(1, state(BTreeMap::from([(2, 200)])))
            .await
            .unwrap();

        let value = manager.get().await.unwrap().unwrap();
        assert!(!value.last_exec_time_map.contains_key(&1));
        assert_eq!(value.last_exec_time_map.get(&2), Some(&200));
    }

    #[tokio::test]
    async fn test_merge_accepts_clock_rollback_from_same_node() {
        let manager = FlowStateManager::new(Arc::new(MemoryKvBackend::default()));

        // Simulates a flownode restart whose wall clock rolled back: the node
        // first reports flow 1 at t1, then (after restart) reports a *smaller*
        // timestamp t0. Because reports are processed strictly in arrival
        // order (no epoch comparison), the later report must win instead of
        // being permanently rejected.
        manager
            .merge(1, state(BTreeMap::from([(1, 100)])))
            .await
            .unwrap();
        manager
            .merge(1, state(BTreeMap::from([(1, 50)])))
            .await
            .unwrap();

        let value = manager.get().await.unwrap().unwrap();
        assert_eq!(value.last_exec_time_map.get(&1), Some(&50));
    }

    #[tokio::test]
    async fn test_merge_aggregates_state_size_and_last_exec_time_by_max() {
        let manager = FlowStateManager::new(Arc::new(MemoryKvBackend::default()));

        // Both nodes report the same flow; the aggregate must take the max of
        // state_size and last_exec_time_map across nodes.
        manager
            .merge(
                1,
                FlowStateValue::new(BTreeMap::from([(1, 1024)]), BTreeMap::from([(1, 100)])),
            )
            .await
            .unwrap();
        manager
            .merge(
                2,
                FlowStateValue::new(BTreeMap::from([(1, 2048)]), BTreeMap::from([(1, 50)])),
            )
            .await
            .unwrap();

        let value = manager.get().await.unwrap().unwrap();
        assert_eq!(value.state_size.get(&1), Some(&2048));
        assert_eq!(value.last_exec_time_map.get(&1), Some(&100));
    }

    #[tokio::test]
    async fn test_merge_concurrent_reports_no_lost_update() {
        let manager = Arc::new(FlowStateManager::new(Arc::new(MemoryKvBackend::default())));

        // Two nodes report concurrently; the merge lock must serialize the
        // read-modify-write so neither node's report is lost.
        let m1 = manager.clone();
        let h1 = tokio::spawn(async move {
            m1.merge(1, state(BTreeMap::from([(1, 100)])))
                .await
                .unwrap();
        });
        let m2 = manager.clone();
        let h2 = tokio::spawn(async move {
            m2.merge(2, state(BTreeMap::from([(2, 200)])))
                .await
                .unwrap();
        });
        h1.await.unwrap();
        h2.await.unwrap();

        let value = manager.get().await.unwrap().unwrap();
        assert_eq!(value.last_exec_time_map.get(&1), Some(&100));
        assert_eq!(value.last_exec_time_map.get(&2), Some(&200));
    }

    #[tokio::test]
    async fn test_merge_empty_report_removes_own_flows_keeps_others() {
        let manager = FlowStateManager::new(Arc::new(MemoryKvBackend::default()));

        // Node A reports flow 1, node B reports flow 2.
        manager
            .merge(1, state(BTreeMap::from([(1, 100)])))
            .await
            .unwrap();
        manager
            .merge(2, state(BTreeMap::from([(2, 200)])))
            .await
            .unwrap();

        // Node A reports an empty map: its own flow 1 disappears from the
        // aggregate while node B's flow 2 is retained.
        manager.merge(1, state(BTreeMap::new())).await.unwrap();

        let value = manager.get().await.unwrap().unwrap();
        assert!(!value.last_exec_time_map.contains_key(&1));
        assert_eq!(value.last_exec_time_map.get(&2), Some(&200));
    }

    #[tokio::test]
    async fn test_merge_state_cleared_on_in_memory_kv_reset() {
        let backend = Arc::new(MemoryKvBackend::default());
        let manager = FlowStateManager::new(backend.clone());

        manager
            .merge(1, state(BTreeMap::from([(1, 100)])))
            .await
            .unwrap();
        assert!(manager.get().await.unwrap().is_some());

        // Simulate a metasrv leader change, which clears the in-memory KV
        // (including the per-node keys, since they live in the same KV).
        backend.clear();
        assert!(manager.get().await.unwrap().is_none());

        // A fresh report works again after the reset.
        manager
            .merge(2, state(BTreeMap::from([(2, 200)])))
            .await
            .unwrap();
        let value = manager.get().await.unwrap().unwrap();
        assert_eq!(value.last_exec_time_map.get(&2), Some(&200));
    }

    #[tokio::test]
    async fn test_merge_writes_global_key() {
        let backend = Arc::new(MemoryKvBackend::default());
        let manager = FlowStateManager::new(backend.clone());

        manager
            .merge(1, state(BTreeMap::from([(1, 100)])))
            .await
            .unwrap();

        // The global key is still `__flow/state` and holds the serialized
        // FlowStateValue, so existing get()/client readers are unchanged.
        let dump = backend.dump();
        let global_key = "__flow/state".as_bytes().to_vec();
        assert!(dump.contains_key(&global_key));
        let value = FlowStateValue::try_from_raw_value(dump.get(&global_key).unwrap()).unwrap();
        assert_eq!(value.last_exec_time_map.get(&1), Some(&100));
    }
}
