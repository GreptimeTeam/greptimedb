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

        // Never blocks: the region lease has already been decided earlier in the
        // chain, so a busy supervisor must not withhold the heartbeat response.
        self.heartbeat_acceptor
            .accept(DatanodeHeartbeat::from(stat));

        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use api::v1::meta::HeartbeatRequest;
    use common_catalog::consts::default_engine;
    use common_meta::datanode::{RegionManifestInfo, RegionStat, Stat};
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;
    use tokio::sync::oneshot;

    use crate::handler::failure_handler::RegionFailureHandler;
    use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
    use crate::metasrv::builder::MetasrvBuilder;
    use crate::region::supervisor::tests::new_test_supervisor;
    use crate::region::supervisor::{DatanodeHeartbeat, Event, HeartbeatAcceptor};

    fn new_region_stat(region_id: u64) -> RegionStat {
        RegionStat {
            id: RegionId::from_u64(region_id),
            rcus: 0,
            wcus: 0,
            approximate_bytes: 0,
            engine: default_engine().to_string(),
            role: RegionRole::Follower,
            num_rows: 0,
            memtable_size: 0,
            manifest_size: 0,
            sst_size: 0,
            sst_num: 0,
            index_size: 0,
            region_manifest: RegionManifestInfo::Mito {
                manifest_version: 0,
                flushed_entry_id: 0,
                file_removed_cnt: 0,
            },
            data_topic_latest_entry_id: 0,
            metadata_topic_latest_entry_id: 0,
            written_bytes: 0,
            query_cpu_time: 0,
            query_scanned_bytes: 0,
            min_timestamp: None,
            max_timestamp: None,
        }
    }

    fn new_stat() -> Stat {
        Stat {
            id: 42,
            region_stats: vec![new_region_stat(1), new_region_stat(2), new_region_stat(3)],
            timestamp_millis: 1000,
            ..Default::default()
        }
    }

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
        acc.stat = Some(new_stat());

        handler.handle(req, &mut ctx, acc).await.unwrap();
        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        let detector = rx.await.unwrap();
        assert_eq!(detector.iter().collect::<Vec<_>>().len(), 3);
    }

    // Regression test for issue #9419: this handler used to await a blocking send on the
    // bounded supervisor queue, so a busy supervisor parked the heartbeat response path and
    // withheld region lease grants from every datanode.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_handle_heartbeat_does_not_block_on_full_supervisor_queue() {
        let (supervisor, sender) = new_test_supervisor();
        // The handler is built directly rather than through `RegionFailureHandler::new`, which
        // spawns the supervisor: an undrained queue is what fills up here.
        let handler = RegionFailureHandler {
            heartbeat_acceptor: HeartbeatAcceptor::new(sender.clone()),
        };
        // Holds the receiver alive; otherwise the queue reports `Closed` instead of `Full`.
        let _supervisor = supervisor;

        let stat = new_stat();
        let mut queued = 0;
        while sender
            .try_send(Event::HeartbeatArrived(DatanodeHeartbeat::from(&stat)))
            .is_ok()
        {
            queued += 1;
        }
        assert_eq!(queued, sender.max_capacity());

        let builder = MetasrvBuilder::new();
        let metasrv = builder.build().await.unwrap();
        let mut ctx = metasrv.new_ctx();
        let acc = &mut HeartbeatAccumulator::default();
        // Without a stat the handler returns before reaching the acceptor, which would make
        // this test pass on the unfixed code as well.
        acc.stat = Some(stat);

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            handler.handle(&HeartbeatRequest::default(), &mut ctx, acc),
        )
        .await;
        assert!(result.is_ok());
    }
}
