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

use crate::cluster::{NodeInfo, NodeInfoKey, Role};
use crate::error;
use crate::kv_backend::ResettableKvBackendRef;
use crate::leadership_notifier::LeadershipChangeListener;
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;

pub struct FrontendExpiryListener {
    handle: Mutex<Option<JoinHandle<()>>>,
    tick_interval: Duration,
    expire_interval: Duration,
    in_memory: ResettableKvBackendRef,
}

impl Drop for FrontendExpiryListener {
    fn drop(&mut self) {
        self.stop();
    }
}

impl FrontendExpiryListener {
    pub fn new(
        tick_interval: Duration,
        expire_interval: Duration,
        in_memory: ResettableKvBackendRef,
    ) -> Self {
        Self {
            handle: Mutex::new(None),
            tick_interval,
            expire_interval,
            in_memory,
        }
    }

    async fn start(&self) {
        let mut handle = self.handle.lock().unwrap();
        if handle.is_none() {
            let tick_interval = self.tick_interval;
            let in_memory = self.in_memory.clone();

            let expire_interval = self.expire_interval;
            let ticker_loop = tokio::spawn(async move {
                let mut interval = interval(tick_interval);
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if let Err(e) =
                        Self::clean_expired_frontend_node(&in_memory, expire_interval).await
                    {
                        error!(e; "Failed to clean expired frontend node");
                    }
                }
            });
            *handle = Some(ticker_loop);
        }
    }

    fn stop(&self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.abort();
            info!("Frontend expiry ticker stopped")
        }
    }

    async fn clean_expired_frontend_node(
        in_memory: &ResettableKvBackendRef,
        expire_interval: Duration,
    ) -> error::Result<()> {
        let node_keys = Self::list_expired_nodes(in_memory, expire_interval).await?;
        for key in node_keys {
            let key_bytes: Vec<u8> = (&key).into();
            if let Err(e) = in_memory.delete(&key_bytes, false).await {
                warn!(e;"Failed to delete expired frontend node: {:?}", key_bytes);
            } else {
                debug!("Deleted expired frontend node key: {:?}", key);
            }
        }
        Ok(())
    }

    async fn list_expired_nodes(
        in_memory: &ResettableKvBackendRef,
        expire_interval: Duration,
    ) -> error::Result<impl Iterator<Item = NodeInfoKey>> {
        let prefix = NodeInfoKey::key_prefix_with_role(0, Role::Frontend);
        let req = RangeRequest::new().with_prefix(prefix);
        let current_time_millis = common_time::util::current_time_millis();
        let resp = in_memory.range(req).await?;
        let expire_interval = expire_interval;
        Ok(resp
            .kvs
            .into_iter()
            .filter_map(move |KeyValue { key, value }| {
                let Ok(info) = NodeInfo::try_from(value).inspect_err(|e| {
                    warn!(e; "Unrecognized frontend node info value");
                }) else {
                    return None;
                };
                if (current_time_millis - info.last_activity_ts)
                    > expire_interval.as_millis() as i64
                {
                    NodeInfoKey::try_from(key)
                        .inspect_err(|e| {
                            warn!(e; "Unrecognized node info key");
                        })
                        .ok()
                } else {
                    None
                }
            }))
    }
}

#[async_trait::async_trait]
impl LeadershipChangeListener for FrontendExpiryListener {
    fn name(&self) -> &str {
        "FrontendExpiryListener"
    }

    async fn on_leader_start(&self) -> error::Result<()> {
        self.start().await;
        info!("On leader start, frontend expiry listener started");
        Ok(())
    }

    async fn on_leader_stop(&self) -> error::Result<()> {
        self.stop();
        info!("On leader stop, frontend expiry listener stopped");
        Ok(())
    }
}
