use std::sync::{Arc, RwLock};

use api::v1::meta::heartbeat_client::HeartbeatClient;
use api::v1::meta::{AskLeaderRequest, RequestHeader, Role};
use common_grpc::channel_manager::ChannelManager;
use common_telemetry::warn;
use rand::seq::SliceRandom;
use snafu::{OptionExt, ResultExt};
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
}

impl AskLeader {
    pub fn new(
        id: Id,
        role: Role,
        peers: impl Into<Vec<String>>,
        channel_manager: ChannelManager,
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
        }
    }

    pub fn get_leader(&self) -> Option<String> {
        self.leadership_group.read().unwrap().leader.clone()
    }

    pub async fn ask_leader(&self) -> Result<String> {
        let (prev_leader, mut peers) = {
            let leadership_group = self.leadership_group.read().unwrap();
            let leader = leadership_group.leader.clone();
            let peers = leadership_group.peers.clone();
            (leader, peers)
        };
        peers.shuffle(&mut rand::thread_rng());

        let header = RequestHeader::new(self.id, self.role);
        let mut leader = None;
        for addr in &peers {
            let req = AskLeaderRequest {
                header: Some(header.clone()),
            };
            let mut client = self.create_asker(addr)?;
            match client.ask_leader(req).await {
                Ok(res) => {
                    let Some(endpoint) = res.into_inner().leader else {
                        warn!("No leader from: {addr}");
                        continue;
                    };
                    leader = Some(endpoint.addr);
                    break;
                }
                Err(status) => {
                    warn!("Failed to ask leader from: {addr}, {status}");
                }
            }
        }

        let leader = leader.context(error::NoLeaderSnafu)?;
        if prev_leader.as_ref() != Some(&leader) {
            let mut leadership_group = self.leadership_group.write().unwrap();
            leadership_group.leader = Some(leader.clone());
        }
        Ok(leader)
    }

    fn create_asker(&self, addr: impl AsRef<str>) -> Result<HeartbeatClient<Channel>> {
        Ok(HeartbeatClient::new(
            self.channel_manager
                .get(addr)
                .context(error::CreateChannelSnafu)?,
        ))
    }
}
