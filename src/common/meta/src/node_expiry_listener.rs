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
use std::sync::Mutex;
use std::time::Duration;

use common_telemetry::{debug, error, info, warn};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};

use crate::cluster::{NodeInfo, NodeInfoKey, Role};
use crate::error;
use crate::kv_backend::ResettableKvBackendRef;
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::leadership_notifier::LeadershipChangeListener;
use crate::rpc::KeyValue;
use crate::rpc::store::RangeRequest;

/// [NodeExpiryListener] periodically checks all node info in memory and removes
/// expired node info to prevent memory leak.
pub struct NodeExpiryListener {
    handle: Mutex<Option<JoinHandle<()>>>,
    max_idle_time: Duration,
    role_idle_limits: HashMap<Role, Duration>,
    in_memory: ResettableKvBackendRef,
}

impl Drop for NodeExpiryListener {
    fn drop(&mut self) {
        self.stop();
    }
}

impl NodeExpiryListener {
    pub fn new(max_idle_time: Duration, in_memory: ResettableKvBackendRef) -> Self {
        Self {
            handle: Mutex::new(None),
            max_idle_time,
            role_idle_limits: HashMap::new(),
            in_memory,
        }
    }

    /// Caps the idle time for registrations of the given role.
    pub fn with_role_idle_limit(mut self, role: Role, limit: Duration) -> Self {
        self.role_idle_limits
            .insert(role, limit.min(self.max_idle_time));
        self
    }

    /// Starts periodic cleanup on the active metasrv.
    pub fn start(&self) {
        let mut handle = self.handle.lock().unwrap();
        if handle.is_none() {
            let in_memory = self.in_memory.clone();

            let max_idle_time = self.max_idle_time;
            let role_idle_limits = self.role_idle_limits.clone();
            let ticker_loop = tokio::spawn(async move {
                // Run clean task every minute.
                let mut interval = interval(Duration::from_secs(60));
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if let Err(e) = Self::clean_expired_nodes(
                        &in_memory,
                        max_idle_time,
                        &role_idle_limits,
                        common_time::util::current_time_millis(),
                    )
                    .await
                    {
                        error!(e; "Failed to clean expired node");
                    }
                }
            });
            *handle = Some(ticker_loop);
        }
    }

    /// Stops periodic cleanup.
    pub fn stop(&self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.abort();
            info!("Node expiry listener stopped")
        }
    }

    /// Cleans expired nodes from memory.
    async fn clean_expired_nodes(
        in_memory: &ResettableKvBackendRef,
        max_idle_time: Duration,
        role_idle_limits: &HashMap<Role, Duration>,
        now: i64,
    ) -> error::Result<()> {
        let req = RangeRequest::new().with_prefix(NodeInfoKey::key_prefix());
        for KeyValue { key, value } in in_memory.range(req).await?.kvs {
            let Ok(node_key) = NodeInfoKey::try_from(key.clone()).inspect_err(|e| {
                warn!(e; "Unrecognized node info key");
            }) else {
                continue;
            };
            let Ok(info) = serde_json::from_slice::<NodeInfo>(&value).inspect_err(|e| {
                warn!(e; "Unrecognized node info value");
            }) else {
                continue;
            };
            let limit = role_idle_limits
                .get(&node_key.role)
                .unwrap_or(&max_idle_time);
            let idle_ms = now.saturating_sub(info.last_activity_ts).max(0) as u64;
            if Duration::from_millis(idle_ms) > *limit {
                match in_memory.txn(delete_if_unchanged(key, value)).await {
                    Ok(resp) if resp.succeeded => {
                        debug!("Deleted expired node key: {:?}", node_key)
                    }
                    Err(e) => warn!(e; "Failed to delete expired node: {:?}", node_key),
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

// A heartbeat may have refreshed the registration since the scan.
fn delete_if_unchanged(key: Vec<u8>, value: Vec<u8>) -> Txn {
    Txn::new()
        .when(vec![Compare::with_value(
            key.clone(),
            CompareOp::Equal,
            value,
        )])
        .and_then(vec![TxnOp::Delete(key)])
}

#[async_trait::async_trait]
impl LeadershipChangeListener for NodeExpiryListener {
    fn name(&self) -> &str {
        "NodeExpiryListener"
    }

    async fn on_leader_start(&self) -> error::Result<()> {
        self.start();
        info!(
            "On leader start, node expiry listener started with max idle time: {:?}",
            self.max_idle_time
        );
        Ok(())
    }

    async fn on_leader_stop(&self) -> error::Result<()> {
        self.stop();
        info!("On leader stop, node expiry listener stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::rpc::store::PutRequest;

    #[tokio::test]
    async fn test_cleanup_preserves_refreshed_registration() {
        let backend: ResettableKvBackendRef = Arc::new(MemoryKvBackend::new());
        let key = Vec::from(&NodeInfoKey {
            role: Role::Datanode,
            node_id: 1,
        });
        let scanned = b"first heartbeat".to_vec();
        let refreshed = b"next heartbeat".to_vec();
        for value in [&scanned, &refreshed] {
            backend
                .put(
                    PutRequest::new()
                        .with_key(key.clone())
                        .with_value(value.clone()),
                )
                .await
                .unwrap();
        }
        let response = backend
            .txn(delete_if_unchanged(key.clone(), scanned))
            .await
            .unwrap();
        assert!(!response.succeeded);
        assert_eq!(backend.get(&key).await.unwrap().unwrap().value, refreshed);
    }

    #[tokio::test]
    async fn test_role_idle_limit() {
        let now = 100_000_000;
        let cases = [
            (86_400, None, vec![1, 2, 3, 4]),
            (86_400, Some(60), vec![1, 3, 4]),
            (30, Some(60), vec![4]),
        ];
        for (global_secs, datanode_secs, expected) in cases {
            let backend: ResettableKvBackendRef = Arc::new(MemoryKvBackend::new());
            let mut listener =
                NodeExpiryListener::new(Duration::from_secs(global_secs), backend.clone());
            if let Some(limit) = datanode_secs {
                listener =
                    listener.with_role_idle_limit(Role::Datanode, Duration::from_secs(limit));
            }
            let mut keys = Vec::new();
            for (node_id, role, idle_ms) in [
                (1, Role::Datanode, 60_000),
                (2, Role::Datanode, 60_001),
                (3, Role::Frontend, 86_400_000),
                (4, Role::Flownode, 30_000),
                (5, Role::Flownode, 86_400_001),
            ] {
                let key = Vec::from(&NodeInfoKey { role, node_id });
                let value = serde_json::json!({
                    "peer": {"id": node_id, "addr": "localhost"},
                    "last_activity_ts": now - idle_ms,
                    "status": "Standalone",
                    "version": "", "git_commit": "", "start_time_ms": 0
                });
                backend
                    .put(
                        PutRequest::new()
                            .with_key(key.clone())
                            .with_value(value.to_string()),
                    )
                    .await
                    .unwrap();
                keys.push((node_id, key));
            }

            NodeExpiryListener::clean_expired_nodes(
                &backend,
                listener.max_idle_time,
                &listener.role_idle_limits,
                now,
            )
            .await
            .unwrap();
            let mut remaining = Vec::new();
            for (id, key) in keys {
                if backend.get(&key).await.unwrap().is_some() {
                    remaining.push(id);
                }
            }
            assert_eq!(remaining, expected);
        }
    }
}
