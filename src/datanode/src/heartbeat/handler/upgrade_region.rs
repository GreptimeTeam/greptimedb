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

use common_meta::instruction::{
    InstructionReply, UpgradeRegion, UpgradeRegionReply, UpgradeRegionsReply,
};
use common_telemetry::{info, warn};
use futures::future::join_all;
use store_api::region_request::{RegionCatchupRequest, RegionRequest, ReplayCheckpoint};

use crate::heartbeat::handler::{HandlerContext, InstructionHandler};
use crate::heartbeat::task_tracker::WaitResult;

#[derive(Debug, Clone, Copy, Default)]
pub struct UpgradeRegionsHandler;

impl UpgradeRegionsHandler {
    // Handles uprade regions instruction.
    // 
    // Returns batch of upgrade region replies, the order of the replies is not guaranteed.
    async fn handle_upgrade_regions(
        &self,
        ctx: &HandlerContext,
        upgrade_regions: Vec<UpgradeRegion>,
    ) -> Vec<UpgradeRegionReply> {
        let mut replies = Vec::with_capacity(upgrade_regions.len());
        let mut catchup_request = Vec::with_capacity(upgrade_regions.len());

        for upgrade_region in upgrade_regions {
            let Some(writable) = ctx.region_server.is_region_leader(upgrade_region.region_id)
            else {
                replies.push(UpgradeRegionReply {
                    region_id: upgrade_region.region_id,
                    ready: false,
                    exists: false,
                    error: None,
                });
                continue;
            };

            if writable {
                replies.push(UpgradeRegionReply {
                    region_id: upgrade_region.region_id,
                    ready: true,
                    exists: true,
                    error: None,
                });
            } else {
                let UpgradeRegion {
                    last_entry_id,
                    metadata_last_entry_id,
                    location_id,
                    replay_entry_id,
                    metadata_replay_entry_id,
                    replay_timeout,
                    ..
                } = upgrade_region;

                let checkpoint = match (replay_entry_id, metadata_replay_entry_id) {
                    (Some(entry_id), metadata_entry_id) => Some(ReplayCheckpoint {
                        entry_id,
                        metadata_entry_id,
                    }),
                    _ => None,
                };

                catchup_request.push((
                    upgrade_region.region_id,
                    replay_timeout.unwrap_or_default(),
                    RegionCatchupRequest {
                        set_writable: true,
                        entry_id: last_entry_id,
                        metadata_entry_id: metadata_last_entry_id,
                        location_id,
                        checkpoint,
                    },
                ));
            }
        }

        let mut wait_results = Vec::with_capacity(catchup_request.len());

        for (region_id, replay_timeout, catchup_request) in catchup_request {
            let region_server_moved = ctx.region_server.clone();
            // TODO(weny): parallelize the catchup tasks.
            let result = ctx
                .catchup_tasks
                .try_register(
                    region_id,
                    Box::pin(async move {
                        info!(
                            "Executing region: {region_id} catchup to: last entry id {:?}",
                            catchup_request.entry_id
                        );
                        region_server_moved
                            .handle_request(region_id, RegionRequest::Catchup(catchup_request))
                            .await?;
                        Ok(())
                    }),
                )
                .await;

            if result.is_busy() {
                warn!("Another catchup task is running for the region: {region_id}");
            }

            // We don't care that it returns a newly registered or running task.
            let mut watcher = result.into_watcher();
            wait_results.push((
                region_id,
                ctx.catchup_tasks.wait(&mut watcher, replay_timeout).await,
            ));
        }

        let results = join_all(
            wait_results
                .into_iter()
                .map(|(region_id, result)| async move {
                    match result {
                        WaitResult::Timeout => UpgradeRegionReply {
                            region_id,
                            ready: false,
                            exists: true,
                            error: None,
                        },
                        WaitResult::Finish(Ok(_)) => UpgradeRegionReply {
                            region_id,
                            ready: true,
                            exists: true,
                            error: None,
                        },
                        WaitResult::Finish(Err(err)) => UpgradeRegionReply {
                            region_id,
                            ready: false,
                            exists: true,
                            error: Some(format!("{err:?}")),
                        },
                    }
                }),
        )
        .await;

        replies.extend(results.into_iter());
        replies
    }
}

#[async_trait::async_trait]
impl InstructionHandler for UpgradeRegionsHandler {
    type Instruction = Vec<UpgradeRegion>;

    async fn handle(
        &self,
        ctx: &HandlerContext,
        upgrade_regions: Self::Instruction,
    ) -> Option<InstructionReply> {
        let replies = self.handle_upgrade_regions(ctx, upgrade_regions).await;

        Some(InstructionReply::UpgradeRegions(UpgradeRegionsReply::new(
            replies,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_meta::instruction::UpgradeRegion;
    use mito2::engine::MITO_ENGINE_NAME;
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;
    use tokio::time::Instant;

    use crate::error;
    use crate::heartbeat::handler::upgrade_region::UpgradeRegionsHandler;
    use crate::heartbeat::handler::{HandlerContext, InstructionHandler};
    use crate::tests::{MockRegionEngine, mock_region_server};

    #[tokio::test]
    async fn test_region_not_exist() {
        let mut mock_region_server = mock_region_server();
        let (mock_engine, _) = MockRegionEngine::new(MITO_ENGINE_NAME);
        mock_region_server.register_engine(mock_engine);

        let handler_context = HandlerContext::new_for_test(mock_region_server);

        let region_id = RegionId::new(1024, 1);
        let waits = vec![None, Some(Duration::from_millis(100u64))];

        for replay_timeout in waits {
            let reply = UpgradeRegionsHandler
                .handle(
                    &handler_context,
                    vec![UpgradeRegion {
                        region_id,
                        replay_timeout,
                        ..Default::default()
                    }],
                )
                .await;

            let reply = &reply.unwrap().expect_upgrade_regions_reply()[0];
            assert!(!reply.exists);
            assert!(reply.error.is_none());
        }
    }

    #[tokio::test]
    async fn test_region_writable() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);

        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Leader));
                region_engine.handle_request_mock_fn = Some(Box::new(|_, _| {
                    // Should be unreachable.
                    unreachable!();
                }));
            });
        mock_region_server.register_test_region(region_id, mock_engine);

        let handler_context = HandlerContext::new_for_test(mock_region_server);

        let waits = vec![None, Some(Duration::from_millis(100u64))];

        for replay_timeout in waits {
            let reply = UpgradeRegionsHandler
                .handle(
                    &handler_context,
                    vec![UpgradeRegion {
                        region_id,
                        replay_timeout,
                        ..Default::default()
                    }],
                )
                .await;

            let reply = &reply.unwrap().expect_upgrade_regions_reply()[0];
            assert!(reply.ready);
            assert!(reply.exists);
            assert!(reply.error.is_none());
        }
    }

    #[tokio::test]
    async fn test_region_not_ready() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);

        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                // Region is not ready.
                region_engine.mock_role = Some(Some(RegionRole::Follower));
                region_engine.handle_request_mock_fn = Some(Box::new(|_, _| Ok(0)));
                // Note: Don't change.
                region_engine.handle_request_delay = Some(Duration::from_secs(100));
            });
        mock_region_server.register_test_region(region_id, mock_engine);

        let handler_context = HandlerContext::new_for_test(mock_region_server);

        let waits = vec![None, Some(Duration::from_millis(100u64))];

        for replay_timeout in waits {
            let reply = UpgradeRegionsHandler
                .handle(
                    &handler_context,
                    vec![UpgradeRegion {
                        region_id,
                        replay_timeout,
                        ..Default::default()
                    }],
                )
                .await;

            let reply = &reply.unwrap().expect_upgrade_regions_reply()[0];
            assert!(!reply.ready);
            assert!(reply.exists);
            assert!(reply.error.is_none());
        }
    }

    #[tokio::test]
    async fn test_region_not_ready_with_retry() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);

        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                // Region is not ready.
                region_engine.mock_role = Some(Some(RegionRole::Follower));
                region_engine.handle_request_mock_fn = Some(Box::new(|_, _| Ok(0)));
                // Note: Don't change.
                region_engine.handle_request_delay = Some(Duration::from_millis(300));
            });
        mock_region_server.register_test_region(region_id, mock_engine);

        let waits = vec![
            Some(Duration::from_millis(100u64)),
            Some(Duration::from_millis(100u64)),
        ];

        let handler_context = HandlerContext::new_for_test(mock_region_server);

        for replay_timeout in waits {
            let reply = UpgradeRegionsHandler
                .handle(
                    &handler_context,
                    vec![UpgradeRegion {
                        region_id,
                        replay_timeout,
                        ..Default::default()
                    }],
                )
                .await;

            let reply = &reply.unwrap().expect_upgrade_regions_reply()[0];
            assert!(!reply.ready);
            assert!(reply.exists);
            assert!(reply.error.is_none());
        }

        let timer = Instant::now();
        let reply = UpgradeRegionsHandler
            .handle(
                &handler_context,
                vec![UpgradeRegion {
                    region_id,
                    replay_timeout: Some(Duration::from_millis(500)),
                    ..Default::default()
                }],
            )
            .await;
        // Must less than 300 ms.
        assert!(timer.elapsed().as_millis() < 300);

        let reply = &reply.unwrap().expect_upgrade_regions_reply()[0];
        assert!(reply.ready);
        assert!(reply.exists);
        assert!(reply.error.is_none());
    }

    #[tokio::test]
    async fn test_region_error() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);

        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                // Region is not ready.
                region_engine.mock_role = Some(Some(RegionRole::Follower));
                region_engine.handle_request_mock_fn = Some(Box::new(|_, _| {
                    error::UnexpectedSnafu {
                        violated: "mock_error".to_string(),
                    }
                    .fail()
                }));
                // Note: Don't change.
                region_engine.handle_request_delay = Some(Duration::from_millis(100));
            });
        mock_region_server.register_test_region(region_id, mock_engine);

        let handler_context = HandlerContext::new_for_test(mock_region_server);

        let reply = UpgradeRegionsHandler
            .handle(
                &handler_context,
                vec![UpgradeRegion {
                    region_id,
                    ..Default::default()
                }],
            )
            .await;

        // It didn't wait for handle returns; it had no idea about the error.
        let reply = &reply.unwrap().expect_upgrade_regions_reply()[0];
        assert!(!reply.ready);
        assert!(reply.exists);
        assert!(reply.error.is_none());

        let reply = UpgradeRegionsHandler
            .handle(
                &handler_context,
                vec![UpgradeRegion {
                    region_id,
                    replay_timeout: Some(Duration::from_millis(200)),
                    ..Default::default()
                }],
            )
            .await;

        let reply = &reply.unwrap().expect_upgrade_regions_reply()[0];
        assert!(!reply.ready);
        assert!(reply.exists);
        assert!(reply.error.is_some());
        assert!(reply.error.as_ref().unwrap().contains("mock_error"));
    }
}
