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

use api::v1::meta::{HeartbeatRequest, Role};
use async_trait::async_trait;

use crate::error::Result;
use crate::handler::failure_handler::runner::{FailureDetectControl, FailureDetectRunner};
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::{Context, ElectionRef};

#[derive(Eq, Hash, PartialEq, Clone)]
pub(crate) struct RegionIdent {
    catalog: String,
    schema: String,
    table: String,
    region_id: u64,
}

// TODO(LFC): TBC
pub(crate) struct DatanodeHeartbeat {
    #[allow(dead_code)]
    cluster_id: u64,
    #[allow(dead_code)]
    node_id: u64,
    region_idents: Vec<RegionIdent>,
    heartbeat_time: i64,
}

pub struct RegionFailureHandler {
    failure_detect_runner: FailureDetectRunner,
}

impl RegionFailureHandler {
    pub fn new(election: Option<ElectionRef>) -> Self {
        Self {
            failure_detect_runner: FailureDetectRunner::new(election),
        }
    }

    pub async fn start(&mut self) {
        self.failure_detect_runner.start().await;
    }
}

#[async_trait]
impl HeartbeatHandler for RegionFailureHandler {
    fn is_acceptable(&self, role: Option<Role>) -> bool {
        role.map_or(false, |r| r == Role::Datanode)
    }

    async fn handle(
        &self,
        _: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if ctx.is_infancy {
            self.failure_detect_runner
                .send_control(FailureDetectControl::Purge)
                .await;
        }

        if ctx.is_skip_all() {
            return Ok(());
        }

        let Some(stat) = acc.stat.as_ref() else { return Ok(()) };

        let heartbeat = DatanodeHeartbeat {
            cluster_id: stat.cluster_id,
            node_id: stat.id,
            region_idents: stat
                .region_stats
                .iter()
                .map(|x| RegionIdent {
                    catalog: x.catalog.clone(),
                    schema: x.schema.clone(),
                    table: x.table.clone(),
                    region_id: x.id,
                })
                .collect(),
            heartbeat_time: stat.timestamp_millis,
        };

        self.failure_detect_runner.send_heartbeat(heartbeat).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::node_stat::{RegionStat, Stat};
    use crate::metasrv::builder::MetaSrvBuilder;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_heartbeat() {
        let mut handler = RegionFailureHandler::new(None);
        handler.start().await;

        let req = &HeartbeatRequest::default();

        let builder = MetaSrvBuilder::new();
        let metasrv = builder.build().await;
        let mut ctx = metasrv.new_ctx();
        ctx.is_infancy = false;

        let acc = &mut HeartbeatAccumulator::default();
        fn new_region_stat(region_id: u64) -> RegionStat {
            RegionStat {
                id: region_id,
                catalog: "a".to_string(),
                schema: "b".to_string(),
                table: "c".to_string(),
                rcus: 0,
                wcus: 0,
                approximate_bytes: 0,
                approximate_rows: 0,
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
