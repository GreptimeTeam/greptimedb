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

use std::sync::Mutex;
use std::time::Duration;

use common_telemetry::{debug, error, info, warn};
use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};

use crate::cluster::{NodeInfo, NodeInfoKey};
use crate::error;
use crate::kv_backend::ResettableKvBackendRef;
use crate::leadership_notifier::LeadershipChangeListener;
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;

/// [NodeExpiryListener] periodically checks all node info in memory and removes
/// expired node info to prevent memory leak.
pub struct NodeExpiryListener {
    handle: Mutex<Option<JoinHandle<()>>>,
    max_idle_time: Duration,
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
            in_memory,
        }
    }

    async fn start(&self) {
        let mut handle = self.handle.lock().unwrap();
        if handle.is_none() {
            let in_memory = self.in_memory.clone();

            let max_idle_time = self.max_idle_time;
            let ticker_loop = tokio::spawn(async move {
                // Run clean task every minute.
                let mut interval = interval(Duration::from_secs(60));
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if let Err(e) = Self::clean_expired_nodes(&in_memory, max_idle_time).await {
                        error!(e; "Failed to clean expired node");
                    }
                }
            });
            *handle = Some(ticker_loop);
        }
    }

    fn stop(&self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.abort();
            info!("Node expiry listener stopped")
        }
    }

    /// Cleans expired nodes from memory.
    async fn clean_expired_nodes(
        in_memory: &ResettableKvBackendRef,
        max_idle_time: Duration,
    ) -> error::Result<()> {
        let node_keys = Self::list_expired_nodes(in_memory, max_idle_time).await?;
        for key in node_keys {
            let key_bytes: Vec<u8> = (&key).into();
            if let Err(e) = in_memory.delete(&key_bytes, false).await {
                warn!(e; "Failed to delete expired node: {:?}", key_bytes);
            } else {
                debug!("Deleted expired node key: {:?}", key);
            }
        }
        Ok(())
    }

    /// Lists expired nodes that have been inactive more than `max_idle_time`.
    async fn list_expired_nodes(
        in_memory: &ResettableKvBackendRef,
        max_idle_time: Duration,
    ) -> error::Result<impl Iterator<Item = NodeInfoKey>> {
        let prefix = NodeInfoKey::key_prefix_with_cluster_id(0);
        let req = RangeRequest::new().with_prefix(prefix);
        let current_time_millis = common_time::util::current_time_millis();
        let resp = in_memory.range(req).await?;
        Ok(resp
            .kvs
            .into_iter()
            .filter_map(move |KeyValue { key, value }| {
                let Ok(info) = NodeInfo::try_from(value).inspect_err(|e| {
                    warn!(e; "Unrecognized node info value");
                }) else {
                    return None;
                };
                if (current_time_millis - info.last_activity_ts) > max_idle_time.as_millis() as i64
                {
                    NodeInfoKey::try_from(key)
                        .inspect_err(|e| {
                            warn!(e; "Unrecognized node info key: {:?}", info.peer);
                        })
                        .ok()
                        .inspect(|node_key| {
                            debug!("Found expired node: {:?}", node_key);
                        })
                } else {
                    None
                }
            }))
    }
}

#[async_trait::async_trait]
impl LeadershipChangeListener for NodeExpiryListener {
    fn name(&self) -> &str {
        "NodeExpiryListener"
    }

    async fn on_leader_start(&self) -> error::Result<()> {
        self.start().await;
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
