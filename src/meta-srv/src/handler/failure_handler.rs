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

use api::v1::meta::{HeartbeatRequest, Role};
use async_trait::async_trait;
use common_telemetry::info;

use crate::error::Result;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;
use crate::region::supervisor::{DatanodeHeartbeat, HeartbeatAcceptor, RegionSupervisor};

pub struct RegionFailureHandler {
    heartbeat_acceptor: HeartbeatAcceptor,
}

impl RegionFailureHandler {
    pub(crate) fn new(
        mut region_supervisor: RegionSupervisor,
        heartbeat_acceptor: HeartbeatAcceptor,
    ) -> Self {
        info!("Starting region supervisor");
        common_runtime::spawn_global(async move { region_supervisor.run().await });
        Self { heartbeat_acceptor }
    }
}

#[async_trait]
impl HeartbeatHandler for RegionFailureHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        _: &HeartbeatRequest,
        _ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let Some(stat) = acc.stat.as_ref() else {
            return Ok(HandleControl::Continue);
        };

        self.heartbeat_acceptor
            .accept(DatanodeHeartbeat::from(stat))
            .await;

        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::HeartbeatRequest;
    use common_catalog::consts::default_engine;
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;
    use tokio::sync::oneshot;

    use crate::handler::failure_handler::RegionFailureHandler;
    use crate::handler::node_stat::{RegionStat, Stat};
    use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
    use crate::metasrv::builder::MetasrvBuilder;
    use crate::region::supervisor::tests::new_test_supervisor;
    use crate::region::supervisor::{Event, HeartbeatAcceptor};

    #[tokio::test]
    async fn test_handle_heartbeat() {
        let (supervisor, sender) = new_test_supervisor();
        let heartbeat_acceptor = HeartbeatAcceptor::new(sender.clone());
        let handler = RegionFailureHandler::new(supervisor, heartbeat_acceptor);
        let req = &HeartbeatRequest::default();
        let builder = MetasrvBuilder::new();
        let metasrv = builder.build().await.unwrap();
        let mut ctx = metasrv.new_ctx();
        let acc = &mut HeartbeatAccumulator::default();
        fn new_region_stat(region_id: u64) -> RegionStat {
            RegionStat {
                id: RegionId::from_u64(region_id),
                rcus: 0,
                wcus: 0,
                approximate_bytes: 0,
                engine: default_engine().to_string(),
                role: RegionRole::Follower,
            }
        }
        acc.stat = Some(Stat {
            cluster_id: 1,
            id: 42,
            region_stats: vec![new_region_stat(1), new_region_stat(2), new_region_stat(3)],
            timestamp_millis: 1000,
            ..Default::default()
        });

        handler.handle(req, &mut ctx, acc).await.unwrap();
        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        let detector = rx.await.unwrap();
        assert_eq!(detector.iter().collect::<Vec<_>>().len(), 3);
    }
}
