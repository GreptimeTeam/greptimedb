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

use std::sync::{Arc, RwLock};
use std::time::Duration;

use api::v1::meta::heartbeat_client::HeartbeatClient;
use api::v1::meta::{AskLeaderRequest, RequestHeader, Role};
use common_grpc::channel_manager::ChannelManager;
use common_meta::distributed_time_constants::META_KEEP_ALIVE_INTERVAL_SECS;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::warn;
use rand::seq::SliceRandom;
use snafu::{OptionExt, ResultExt};
use tokio::time::timeout;
use tonic::transport::Channel;

use crate::client::Id;
use crate::error;
use crate::error::Result;

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
        peers.shuffle(&mut rand::thread_rng());

        let req = AskLeaderRequest {
            header: Some(RequestHeader::new(
                self.id,
                self.role,
                TracingContext::from_current_span().to_w3c(),
            )),
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel(peers.len());

        for addr in &peers {
            let mut client = self.create_asker(addr)?;
            let tx_clone = tx.clone();
            let req = req.clone();
            let addr = addr.to_string();
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
                        warn!("Failed to ask leader from: {addr}, {status}");
                    }
                }
            });
        }

        let leader = timeout(
            self.channel_manager
                .config()
                .timeout
                .unwrap_or(Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS)),
            rx.recv(),
        )
        .await
        .context(error::AskLeaderTimeoutSnafu)?
        .context(error::NoLeaderSnafu)?;

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
}
