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

use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use api::v1::meta::heartbeat_client::HeartbeatClient;
use api::v1::meta::{AskLeaderRequest, RequestHeader, Role};
use async_trait::async_trait;
use common_grpc::channel_manager::ChannelManager;
use common_meta::distributed_time_constants::META_KEEP_ALIVE_INTERVAL_SECS;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::warn;
use rand::seq::SliceRandom;
use snafu::ResultExt;
use tokio::time::timeout;
use tonic::transport::Channel;

use crate::client::Id;
use crate::error;
use crate::error::Result;

pub type LeaderProviderRef = Arc<dyn LeaderProvider>;

/// Provide [`MetaClient`] a Metasrv leader's address.
#[async_trait]
pub trait LeaderProvider: Debug + Send + Sync {
    /// Get the leader of the Metasrv. If it returns `None`, or the leader is outdated,
    /// you can use `ask_leader` to find a new one.
    fn leader(&self) -> Option<String>;

    /// Find the current leader of the Metasrv.
    async fn ask_leader(&self) -> Result<String>;
}

pub type LeaderProviderFactoryRef = Arc<dyn LeaderProviderFactory>;

/// A factory for creating [`LeaderProvider`] instances.
pub trait LeaderProviderFactory: Send + Sync + Debug {
    fn create(&self, peers: &[&str]) -> LeaderProviderRef;
}

#[derive(Debug)]
struct LeadershipGroup {
    leader: Option<String>,
    peers: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct AskLeader {
    id: Id,
    role: Role,
    leadership_group: Arc<RwLock<LeadershipGroup>>,
    channel_manager: ChannelManager,
    max_retry: usize,
}

impl AskLeader {
    pub fn new(
        id: Id,
        role: Role,
        peers: impl Into<Vec<String>>,
        channel_manager: ChannelManager,
        max_retry: usize,
    ) -> Self {
        let leadership_group = Arc::new(RwLock::new(LeadershipGroup {
            leader: None,
            peers: peers.into(),
        }));
        Self {
            id,
            role,
            leadership_group,
            channel_manager,
            max_retry,
        }
    }

    pub fn get_leader(&self) -> Option<String> {
        self.leadership_group.read().unwrap().leader.clone()
    }

    async fn ask_leader_inner(&self) -> Result<String> {
        let mut peers = {
            let leadership_group = self.leadership_group.read().unwrap();
            leadership_group.peers.clone()
        };
        peers.shuffle(&mut rand::rng());

        let req = AskLeaderRequest {
            header: Some(RequestHeader::new(
                self.id,
                self.role,
                TracingContext::from_current_span().to_w3c(),
            )),
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel(peers.len());
        let channel_manager = self.channel_manager.clone();

        for addr in &peers {
            let mut client = self.create_asker(addr)?;
            let tx_clone = tx.clone();
            let req = req.clone();
            let addr = addr.clone();
            let channel_manager = channel_manager.clone();
            tokio::spawn(async move {
                match client.ask_leader(req).await {
                    Ok(res) => {
                        if let Some(endpoint) = res.into_inner().leader {
                            let _ = tx_clone.send(endpoint.addr).await;
                        } else {
                            warn!("No leader from: {addr}");
                        };
                    }
                    Err(status) => {
                        // Reset cached channel even on generic errors: the VIP may keep us on a dead
                        // backend, so forcing a reconnect gives us a chance to hit a healthy peer.
                        Self::reset_channels_with_manager(
                            &channel_manager,
                            std::slice::from_ref(&addr),
                        );
                        warn!("Failed to ask leader from: {addr}, {status}");
                    }
                }
            });
        }

        let leader = match timeout(
            self.channel_manager
                .config()
                .timeout
                .unwrap_or(Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS)),
            rx.recv(),
        )
        .await
        {
            Ok(Some(leader)) => leader,
            Ok(None) => return error::NoLeaderSnafu.fail(),
            Err(e) => {
                // All peers timed out. Reset channels to force reconnection,
                // which may help escape dead backends in VIP/LB scenarios.
                Self::reset_channels_with_manager(&self.channel_manager, &peers);
                return Err(e).context(error::AskLeaderTimeoutSnafu);
            }
        };

        let mut leadership_group = self.leadership_group.write().unwrap();
        leadership_group.leader = Some(leader.clone());

        Ok(leader)
    }

    pub async fn ask_leader(&self) -> Result<String> {
        let mut times = 0;
        while times < self.max_retry {
            match self.ask_leader_inner().await {
                Ok(res) => {
                    return Ok(res);
                }
                Err(err) => {
                    warn!("Failed to ask leader, source: {err}, retry {times} times");
                    times += 1;
                    continue;
                }
            }
        }

        error::RetryTimesExceededSnafu {
            msg: "Failed to ask leader",
            times: self.max_retry,
        }
        .fail()
    }

    fn create_asker(&self, addr: impl AsRef<str>) -> Result<HeartbeatClient<Channel>> {
        Ok(HeartbeatClient::new(
            self.channel_manager
                .get(addr)
                .context(error::CreateChannelSnafu)?,
        ))
    }

    /// Drop cached channels for the given peers so a fresh connection is used next time.
    fn reset_channels_with_manager(channel_manager: &ChannelManager, peers: &[String]) {
        if peers.is_empty() {
            return;
        }

        channel_manager.retain_channel(|addr, _| !peers.iter().any(|peer| peer == addr));
    }
}

#[async_trait]
impl LeaderProvider for AskLeader {
    fn leader(&self) -> Option<String> {
        self.get_leader()
    }

    async fn ask_leader(&self) -> Result<String> {
        self.ask_leader().await
    }
}

/// A factory for creating [`LeaderProvider`] instances.
#[derive(Clone, Debug)]
pub struct LeaderProviderFactoryImpl {
    id: Id,
    role: Role,
    max_retry: usize,
    channel_manager: ChannelManager,
}

impl LeaderProviderFactoryImpl {
    pub fn new(id: Id, role: Role, max_retry: usize, channel_manager: ChannelManager) -> Self {
        Self {
            id,
            role,
            max_retry,
            channel_manager,
        }
    }
}

impl LeaderProviderFactory for LeaderProviderFactoryImpl {
    fn create(&self, peers: &[&str]) -> LeaderProviderRef {
        Arc::new(AskLeader::new(
            self.id,
            self.role,
            peers.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
            self.channel_manager.clone(),
            self.max_retry,
        ))
    }
}
