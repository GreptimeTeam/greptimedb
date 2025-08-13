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

use crate::error::Result;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct CollectTopicStatsHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for CollectTopicStatsHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let Some(current_stat) = acc.stat.as_ref() else {
            return Ok(HandleControl::Continue);
        };

        ctx.topic_stats_registry.add_stats(
            current_stat.id,
            &current_stat.topic_stats,
            current_stat.timestamp_millis,
        );

        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use common_meta::datanode::{Stat, TopicStat};
    use common_meta::distributed_time_constants::TOPIC_STATS_REPORT_INTERVAL_SECS;
    use common_time::util::current_time_millis;

    use super::*;
    use crate::handler::test_utils::TestEnv;

    #[tokio::test]
    async fn test_handle_collect_topic_stats() {
        let env = TestEnv::new();
        let ctx = env.ctx();

        let handler = CollectTopicStatsHandler;
        let timestamp_millis = current_time_millis();
        let aligned_ts = timestamp_millis - timestamp_millis % 1000;
        handle_request_many_times(ctx.clone(), &handler, 1, timestamp_millis, 10).await;
        handle_request_many_times(ctx.clone(), &handler, 2, timestamp_millis, 10).await;

        // trigger the next window
        let next_timestamp_millis =
            timestamp_millis + (TOPIC_STATS_REPORT_INTERVAL_SECS * 1000) as i64;
        handle_request_many_times(ctx.clone(), &handler, 1, next_timestamp_millis, 10).await;

        let latest_entry_id = ctx
            .topic_stats_registry
            .get_latest_entry_id("test")
            .unwrap();
        assert_eq!(latest_entry_id, (15, aligned_ts));
        let latest_entry_id = ctx
            .topic_stats_registry
            .get_latest_entry_id("test2")
            .unwrap();
        assert_eq!(latest_entry_id, (10, aligned_ts));
        assert!(ctx
            .topic_stats_registry
            .get_latest_entry_id("test3")
            .is_none());
    }

    async fn handle_request_many_times(
        mut ctx: Context,
        handler: &CollectTopicStatsHandler,
        datanode_id: u64,
        timestamp_millis: i64,
        loop_times: i32,
    ) {
        let req = HeartbeatRequest::default();
        for i in 1..=loop_times {
            let mut acc = HeartbeatAccumulator {
                stat: Some(Stat {
                    id: datanode_id,
                    region_num: i as _,
                    timestamp_millis,
                    topic_stats: vec![
                        TopicStat {
                            topic: "test".to_string(),
                            latest_entry_id: 15,
                            record_size: 1024,
                            record_num: 2,
                        },
                        TopicStat {
                            topic: "test2".to_string(),
                            latest_entry_id: 10,
                            record_size: 1024,
                            record_num: 2,
                        },
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            };
            handler.handle(&req, &mut ctx, &mut acc).await.unwrap();
        }
    }
}
