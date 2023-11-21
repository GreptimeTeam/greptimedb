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

mod runner;

use std::sync::Arc;

use api::v1::meta::{HeartbeatRequest, Role};
use async_trait::async_trait;
use common_catalog::consts::default_engine;
use common_meta::RegionIdent;

use crate::error::Result;
use crate::failure_detector::PhiAccrualFailureDetectorOptions;
use crate::handler::failure_handler::runner::{FailureDetectControl, FailureDetectRunner};
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::{Context, ElectionRef};
use crate::procedure::region_failover::RegionFailoverManager;

pub(crate) struct DatanodeHeartbeat {
    region_idents: Vec<RegionIdent>,
    heartbeat_time: i64,
}

pub struct RegionFailureHandler {
    failure_detect_runner: FailureDetectRunner,
}

impl RegionFailureHandler {
    pub(crate) async fn try_new(
        election: Option<ElectionRef>,
        region_failover_manager: Arc<RegionFailoverManager>,
        failure_detector_options: PhiAccrualFailureDetectorOptions,
    ) -> Result<Self> {
        region_failover_manager.try_start()?;

        let mut failure_detect_runner = FailureDetectRunner::new(
            election,
            region_failover_manager.clone(),
            failure_detector_options,
        );
        failure_detect_runner.start().await;

        Ok(Self {
            failure_detect_runner,
        })
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
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        if ctx.is_infancy {
            self.failure_detect_runner
                .send_control(FailureDetectControl::Purge)
                .await;
        }

        let Some(stat) = acc.stat.as_ref() else {
            return Ok(HandleControl::Continue);
        };

        let heartbeat = DatanodeHeartbeat {
            region_idents: stat
                .region_stats
                .iter()
                .map(|x| {
                    let region_id = x.id;
                    RegionIdent {
                        cluster_id: stat.cluster_id,
                        datanode_id: stat.id,
                        table_id: region_id.table_id(),
                        region_number: region_id.region_number(),
                        // TODO(LFC): Use the actual table engine (maybe retrieve from heartbeat).
                        engine: default_engine().to_string(),
                    }
                })
                .collect(),
            heartbeat_time: stat.timestamp_millis,
        };

        self.failure_detect_runner.send_heartbeat(heartbeat).await;

        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;

    use super::*;
    use crate::handler::node_stat::{RegionStat, Stat};
    use crate::metasrv::builder::MetaSrvBuilder;
    use crate::test_util::create_region_failover_manager;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_heartbeat() {
        let region_failover_manager = create_region_failover_manager();
        let failure_detector_options = PhiAccrualFailureDetectorOptions::default();
        let handler =
            RegionFailureHandler::try_new(None, region_failover_manager, failure_detector_options)
                .await
                .unwrap();

        let req = &HeartbeatRequest::default();

        let builder = MetaSrvBuilder::new();
        let metasrv = builder.build().await.unwrap();
        let mut ctx = metasrv.new_ctx();
        ctx.is_infancy = false;

        let acc = &mut HeartbeatAccumulator::default();
        fn new_region_stat(region_id: u64) -> RegionStat {
            RegionStat {
                id: RegionId::from_u64(region_id),
                rcus: 0,
                wcus: 0,
                approximate_bytes: 0,
                approximate_rows: 0,
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

        let dump = handler.failure_detect_runner.dump().await;
        assert_eq!(dump.iter().collect::<Vec<_>>().len(), 3);

        // infancy makes heartbeats re-accumulated
        ctx.is_infancy = true;
        acc.stat = None;
        handler.handle(req, &mut ctx, acc).await.unwrap();
        let dump = handler.failure_detect_runner.dump().await;
        assert_eq!(dump.iter().collect::<Vec<_>>().len(), 0);
    }
}
