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

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_meta::instruction::{
    InstructionReply, UpgradeRegion, UpgradeRegionReply, UpgradeRegionsReply,
};
use common_telemetry::{debug, info, warn};
use store_api::region_request::{RegionCatchupRequest, ReplayCheckpoint};
use store_api::storage::RegionId;

use crate::error::Result;
use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

#[derive(Debug, Clone, Copy, Default)]
pub struct UpgradeRegionsHandler {
    pub upgrade_region_parallelism: usize,
}

#[cfg(test)]
impl UpgradeRegionsHandler {
    fn new_test() -> UpgradeRegionsHandler {
        UpgradeRegionsHandler {
            upgrade_region_parallelism: 8,
        }
    }
}

impl UpgradeRegionsHandler {
    fn convert_responses_to_replies(
        responses: Result<Vec<(RegionId, std::result::Result<(), BoxedError>)>>,
        catchup_regions: &[RegionId],
    ) -> Vec<UpgradeRegionReply> {
        match responses {
            Ok(responses) => responses
                .into_iter()
                .map(|(region_id, result)| match result {
                    Ok(()) => UpgradeRegionReply {
                        region_id,
                        ready: true,
                        exists: true,
                        error: None,
                    },
                    Err(err) => {
                        if err.status_code() == StatusCode::RegionNotFound {
                            UpgradeRegionReply {
                                region_id,
                                ready: false,
                                exists: false,
                                error: Some(format!("{err:?}")),
                            }
                        } else {
                            UpgradeRegionReply {
                                region_id,
                                ready: false,
                                exists: true,
                                error: Some(format!("{err:?}")),
                            }
                        }
                    }
                })
                .collect::<Vec<_>>(),
            Err(err) => catchup_regions
                .iter()
                .map(|region_id| UpgradeRegionReply {
                    region_id: *region_id,
                    ready: false,
                    exists: true,
                    error: Some(format!("{err:?}")),
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl UpgradeRegionsHandler {
    // Handles upgrade regions instruction.
    //
    // Returns batch of upgrade region replies, the order of the replies is not guaranteed.
    async fn handle_upgrade_regions(
        &self,
        ctx: &HandlerContext,
        upgrade_regions: Vec<UpgradeRegion>,
    ) -> Vec<UpgradeRegionReply> {
        let num_upgrade_regions = upgrade_regions.len();
        let mut replies = Vec::with_capacity(num_upgrade_regions);
        let mut catchup_requests = Vec::with_capacity(num_upgrade_regions);
        let mut catchup_regions = Vec::with_capacity(num_upgrade_regions);
        let mut timeout = None;

        for upgrade_region in upgrade_regions {
            let Some(writable) = ctx.region_server.is_region_leader(upgrade_region.region_id)
            else {
                // Region is not found.
                debug!("Region {} is not found", upgrade_region.region_id);
                replies.push(UpgradeRegionReply {
                    region_id: upgrade_region.region_id,
                    ready: false,
                    exists: false,
                    error: None,
                });
                continue;
            };

            // Ignores the catchup requests for writable regions.
            if writable {
                warn!(
                    "Region {} is writable, ignores the catchup request",
                    upgrade_region.region_id
                );
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
                match timeout {
                    Some(timeout) => {
                        debug_assert_eq!(timeout, replay_timeout);
                    }
                    None => {
                        // TODO(weny): required the replay_timeout.
                        timeout = Some(replay_timeout);
                    }
                }

                let checkpoint = match (replay_entry_id, metadata_replay_entry_id) {
                    (Some(entry_id), metadata_entry_id) => Some(ReplayCheckpoint {
                        entry_id,
                        metadata_entry_id,
                    }),
                    _ => None,
                };

                catchup_regions.push(upgrade_region.region_id);
                catchup_requests.push((
                    upgrade_region.region_id,
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

        let Some(timeout) = timeout else {
            // No replay timeout, so we don't need to catchup the regions.
            info!("All regions are writable, no need to catchup");
            debug_assert_eq!(replies.len(), num_upgrade_regions);
            return replies;
        };

        match tokio::time::timeout(
            timeout,
            ctx.region_server
                .handle_batch_catchup_requests(self.upgrade_region_parallelism, catchup_requests),
        )
        .await
        {
            Ok(responses) => {
                replies.extend(
                    Self::convert_responses_to_replies(responses, &catchup_regions).into_iter(),
                );
            }
            Err(_) => {
                replies.extend(catchup_regions.iter().map(|region_id| UpgradeRegionReply {
                    region_id: *region_id,
                    ready: false,
                    exists: true,
                    error: None,
                }));
            }
        }

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
        let region_id2 = RegionId::new(1024, 2);
        let replay_timeout = Duration::from_millis(100u64);
        let reply = UpgradeRegionsHandler::new_test()
            .handle(
                &handler_context,
                vec![
                    UpgradeRegion {
                        region_id,
                        replay_timeout,
                        ..Default::default()
                    },
                    UpgradeRegion {
                        region_id: region_id2,
                        replay_timeout,
                        ..Default::default()
                    },
                ],
            )
            .await;

        let replies = &reply.unwrap().expect_upgrade_regions_reply();
        assert_eq!(replies[0].region_id, region_id);
        assert_eq!(replies[1].region_id, region_id2);
        for reply in replies {
            assert!(!reply.exists);
            assert!(reply.error.is_none());
        }
    }

    #[tokio::test]
    async fn test_region_writable() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let region_id2 = RegionId::new(1024, 2);

        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Leader));
                region_engine.handle_request_mock_fn = Some(Box::new(|_, _| {
                    // Should be unreachable.
                    unreachable!();
                }));
            });
        mock_region_server.register_test_region(region_id, mock_engine.clone());
        mock_region_server.register_test_region(region_id2, mock_engine);
        let handler_context = HandlerContext::new_for_test(mock_region_server);
        let replay_timeout = Duration::from_millis(100u64);
        let reply = UpgradeRegionsHandler::new_test()
            .handle(
                &handler_context,
                vec![
                    UpgradeRegion {
                        region_id,
                        replay_timeout,
                        ..Default::default()
                    },
                    UpgradeRegion {
                        region_id: region_id2,
                        replay_timeout,
                        ..Default::default()
                    },
                ],
            )
            .await;

        let replies = &reply.unwrap().expect_upgrade_regions_reply();
        assert_eq!(replies[0].region_id, region_id);
        assert_eq!(replies[1].region_id, region_id2);
        for reply in replies {
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
        let replay_timeout = Duration::from_millis(100u64);
        let reply = UpgradeRegionsHandler::new_test()
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
        assert!(reply.error.is_none(), "error: {:?}", reply.error);
    }

    #[tokio::test]
    async fn test_region_not_ready_with_retry() {
        common_telemetry::init_default_ut_logging();
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);

        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                // Region is not ready.
                region_engine.mock_role = Some(Some(RegionRole::Follower));
                region_engine.handle_request_mock_fn = Some(Box::new(|_, _| Ok(0)));
                region_engine.handle_request_delay = Some(Duration::from_millis(300));
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let waits = vec![Duration::from_millis(100u64), Duration::from_millis(100u64)];
        let handler_context = HandlerContext::new_for_test(mock_region_server);
        for replay_timeout in waits {
            let reply = UpgradeRegionsHandler::new_test()
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
            assert!(reply.error.is_none(), "error: {:?}", reply.error);
        }

        let reply = UpgradeRegionsHandler::new_test()
            .handle(
                &handler_context,
                vec![UpgradeRegion {
                    region_id,
                    replay_timeout: Duration::from_millis(500),
                    ..Default::default()
                }],
            )
            .await;
        let reply = &reply.unwrap().expect_upgrade_regions_reply()[0];
        assert!(reply.ready);
        assert!(reply.exists);
        assert!(reply.error.is_none(), "error: {:?}", reply.error);
    }

    #[tokio::test]
    async fn test_region_error() {
        common_telemetry::init_default_ut_logging();
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
        let reply = UpgradeRegionsHandler::new_test()
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

        let reply = UpgradeRegionsHandler::new_test()
            .handle(
                &handler_context,
                vec![UpgradeRegion {
                    region_id,
                    replay_timeout: Duration::from_millis(200),
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
