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
    DowngradeRegion, DowngradeRegionReply, DowngradeRegionsReply, InstructionReply,
};
use common_telemetry::tracing::info;
use common_telemetry::{error, warn};
use futures::future::join_all;
use store_api::region_engine::{SetRegionRoleStateResponse, SettableRegionRoleState};
use store_api::region_request::{RegionFlushRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::heartbeat::handler::{HandlerContext, InstructionHandler};
use crate::heartbeat::task_tracker::WaitResult;

#[derive(Debug, Clone, Copy, Default)]
pub struct DowngradeRegionsHandler;

impl DowngradeRegionsHandler {
    async fn handle_downgrade_region(
        ctx: &HandlerContext,
        DowngradeRegion {
            region_id,
            flush_timeout,
        }: DowngradeRegion,
    ) -> DowngradeRegionReply {
        let Some(writable) = ctx.region_server.is_region_leader(region_id) else {
            warn!("Region: {region_id} is not found");
            return DowngradeRegionReply {
                region_id,
                last_entry_id: None,
                metadata_last_entry_id: None,
                exists: false,
                error: None,
            };
        };

        let region_server_moved = ctx.region_server.clone();

        // Ignores flush request
        if !writable {
            warn!(
                "Region: {region_id} is not writable, flush_timeout: {:?}",
                flush_timeout
            );
            return ctx.downgrade_to_follower_gracefully(region_id).await;
        }

        // If flush_timeout is not set, directly convert region to follower.
        let Some(flush_timeout) = flush_timeout else {
            return ctx.downgrade_to_follower_gracefully(region_id).await;
        };

        // Sets region to downgrading,
        // the downgrading region will reject all write requests.
        // However, the downgrading region will still accept read, flush requests.
        match ctx
            .region_server
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
            .await
        {
            Ok(SetRegionRoleStateResponse::Success { .. }) => {}
            Ok(SetRegionRoleStateResponse::NotFound) => {
                warn!("Region: {region_id} is not found");
                return DowngradeRegionReply {
                    region_id,
                    last_entry_id: None,
                    metadata_last_entry_id: None,
                    exists: false,
                    error: None,
                };
            }
            Ok(SetRegionRoleStateResponse::InvalidTransition(err)) => {
                error!(err; "Failed to convert region to downgrading leader - invalid transition");
                return DowngradeRegionReply {
                    region_id,
                    last_entry_id: None,
                    metadata_last_entry_id: None,
                    exists: true,
                    error: Some(format!("{err:?}")),
                };
            }
            Err(err) => {
                error!(err; "Failed to convert region to downgrading leader");
                return DowngradeRegionReply {
                    region_id,
                    last_entry_id: None,
                    metadata_last_entry_id: None,
                    exists: true,
                    error: Some(format!("{err:?}")),
                };
            }
        }

        let register_result = ctx
            .downgrade_tasks
            .try_register(
                region_id,
                Box::pin(async move {
                    info!("Flush region: {region_id} before converting region to follower");
                    region_server_moved
                        .handle_request(
                            region_id,
                            RegionRequest::Flush(RegionFlushRequest {
                                row_group_size: None,
                            }),
                        )
                        .await?;

                    Ok(())
                }),
            )
            .await;

        if register_result.is_busy() {
            warn!("Another flush task is running for the region: {region_id}");
        }

        let mut watcher = register_result.into_watcher();
        let result = ctx.downgrade_tasks.wait(&mut watcher, flush_timeout).await;

        match result {
            WaitResult::Timeout => DowngradeRegionReply {
                region_id,
                last_entry_id: None,
                metadata_last_entry_id: None,
                exists: true,
                error: Some(format!(
                    "Flush region timeout, region: {region_id}, timeout: {:?}",
                    flush_timeout
                )),
            },
            WaitResult::Finish(Ok(_)) => ctx.downgrade_to_follower_gracefully(region_id).await,
            WaitResult::Finish(Err(err)) => DowngradeRegionReply {
                region_id,
                last_entry_id: None,
                metadata_last_entry_id: None,
                exists: true,
                error: Some(format!("{err:?}")),
            },
        }
    }
}

#[async_trait::async_trait]
impl InstructionHandler for DowngradeRegionsHandler {
    type Instruction = Vec<DowngradeRegion>;

    async fn handle(
        &self,
        ctx: &HandlerContext,
        downgrade_regions: Self::Instruction,
    ) -> Option<InstructionReply> {
        let futures = downgrade_regions
            .into_iter()
            .map(|downgrade_region| Self::handle_downgrade_region(ctx, downgrade_region));
        // Join all futures; parallelism is governed by the underlying flush scheduler.
        let results = join_all(futures).await;

        Some(InstructionReply::DowngradeRegions(
            DowngradeRegionsReply::new(results),
        ))
    }
}

impl HandlerContext {
    async fn downgrade_to_follower_gracefully(&self, region_id: RegionId) -> DowngradeRegionReply {
        match self
            .region_server
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
            .await
        {
            Ok(SetRegionRoleStateResponse::Success(success)) => DowngradeRegionReply {
                region_id,
                last_entry_id: success.last_entry_id(),
                metadata_last_entry_id: success.metadata_last_entry_id(),
                exists: true,
                error: None,
            },
            Ok(SetRegionRoleStateResponse::NotFound) => {
                warn!("Region: {region_id} is not found");
                DowngradeRegionReply {
                    region_id,
                    last_entry_id: None,
                    metadata_last_entry_id: None,
                    exists: false,
                    error: None,
                }
            }
            Ok(SetRegionRoleStateResponse::InvalidTransition(err)) => {
                error!(err; "Failed to convert region to follower - invalid transition");
                DowngradeRegionReply {
                    region_id,
                    last_entry_id: None,
                    metadata_last_entry_id: None,
                    exists: true,
                    error: Some(format!("{err:?}")),
                }
            }
            Err(err) => {
                error!(err; "Failed to convert region to {}", SettableRegionRoleState::Follower);
                DowngradeRegionReply {
                    region_id,
                    last_entry_id: None,
                    metadata_last_entry_id: None,
                    exists: true,
                    error: Some(format!("{err:?}")),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;
    use std::time::Duration;

    use common_meta::heartbeat::handler::{HandleControl, HeartbeatResponseHandler};
    use common_meta::heartbeat::mailbox::MessageMeta;
    use common_meta::instruction::{DowngradeRegion, Instruction};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use mito2::config::MitoConfig;
    use mito2::engine::MITO_ENGINE_NAME;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use store_api::region_engine::{
        RegionEngine, RegionRole, SetRegionRoleStateResponse, SetRegionRoleStateSuccess,
    };
    use store_api::region_request::RegionRequest;
    use store_api::storage::RegionId;
    use tokio::time::Instant;

    use crate::error;
    use crate::heartbeat::handler::downgrade_region::DowngradeRegionsHandler;
    use crate::heartbeat::handler::tests::HeartbeatResponseTestEnv;
    use crate::heartbeat::handler::{
        HandlerContext, InstructionHandler, RegionHeartbeatResponseHandler,
    };
    use crate::tests::{MockRegionEngine, mock_region_server};

    #[tokio::test]
    async fn test_region_not_exist() {
        let mut mock_region_server = mock_region_server();
        let (mock_engine, _) = MockRegionEngine::new(MITO_ENGINE_NAME);
        mock_region_server.register_engine(mock_engine);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);
        let region_id = RegionId::new(1024, 1);
        let waits = vec![None, Some(Duration::from_millis(100u64))];

        for flush_timeout in waits {
            let reply = DowngradeRegionsHandler
                .handle(
                    &handler_context,
                    vec![DowngradeRegion {
                        region_id,
                        flush_timeout,
                    }],
                )
                .await;

            let reply = &reply.unwrap().expect_downgrade_regions_reply()[0];
            assert!(!reply.exists);
            assert!(reply.error.is_none());
            assert!(reply.last_entry_id.is_none());
        }
    }

    #[tokio::test]
    async fn test_region_readonly() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Follower));
                region_engine.handle_request_mock_fn = Some(Box::new(|_, req| {
                    if let RegionRequest::Flush(_) = req {
                        // Should be unreachable.
                        unreachable!();
                    };

                    Ok(0)
                }));
                region_engine.handle_set_readonly_gracefully_mock_fn = Some(Box::new(|_| {
                    Ok(SetRegionRoleStateResponse::success(
                        SetRegionRoleStateSuccess::mito(1024),
                    ))
                }))
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);

        let waits = vec![None, Some(Duration::from_millis(100u64))];
        for flush_timeout in waits {
            let reply = DowngradeRegionsHandler
                .handle(
                    &handler_context,
                    vec![DowngradeRegion {
                        region_id,
                        flush_timeout,
                    }],
                )
                .await;

            let reply = &reply.unwrap().expect_downgrade_regions_reply()[0];
            assert!(reply.exists);
            assert!(reply.error.is_none());
            assert_eq!(reply.last_entry_id.unwrap(), 1024);
        }
    }

    #[tokio::test]
    async fn test_region_flush_timeout() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Leader));
                region_engine.handle_request_delay = Some(Duration::from_secs(100));
                region_engine.handle_set_readonly_gracefully_mock_fn = Some(Box::new(|_| {
                    Ok(SetRegionRoleStateResponse::success(
                        SetRegionRoleStateSuccess::mito(1024),
                    ))
                }))
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);

        let flush_timeout = Duration::from_millis(100);
        let reply = DowngradeRegionsHandler
            .handle(
                &handler_context,
                vec![DowngradeRegion {
                    region_id,
                    flush_timeout: Some(flush_timeout),
                }],
            )
            .await;

        let reply = &reply.unwrap().expect_downgrade_regions_reply()[0];
        assert!(reply.exists);
        assert!(reply.error.as_ref().unwrap().contains("timeout"));
        assert!(reply.last_entry_id.is_none());
    }

    #[tokio::test]
    async fn test_region_flush_timeout_and_retry() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Leader));
                region_engine.handle_request_delay = Some(Duration::from_millis(300));
                region_engine.handle_set_readonly_gracefully_mock_fn = Some(Box::new(|_| {
                    Ok(SetRegionRoleStateResponse::success(
                        SetRegionRoleStateSuccess::mito(1024),
                    ))
                }))
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);

        let waits = vec![
            Some(Duration::from_millis(100u64)),
            Some(Duration::from_millis(100u64)),
        ];

        for flush_timeout in waits {
            let reply = DowngradeRegionsHandler
                .handle(
                    &handler_context,
                    vec![DowngradeRegion {
                        region_id,
                        flush_timeout,
                    }],
                )
                .await;

            let reply = &reply.unwrap().expect_downgrade_regions_reply()[0];
            assert!(reply.exists);
            assert!(reply.error.as_ref().unwrap().contains("timeout"));
            assert!(reply.last_entry_id.is_none());
        }
        let timer = Instant::now();
        let reply = DowngradeRegionsHandler
            .handle(
                &handler_context,
                vec![DowngradeRegion {
                    region_id,
                    flush_timeout: Some(Duration::from_millis(500)),
                }],
            )
            .await;
        // Must less than 300 ms.
        assert!(timer.elapsed().as_millis() < 300);

        let reply = &reply.unwrap().expect_downgrade_regions_reply()[0];
        assert!(reply.exists);
        assert!(reply.error.is_none());
        assert_eq!(reply.last_entry_id.unwrap(), 1024);
    }

    #[tokio::test]
    async fn test_region_flush_timeout_and_retry_error() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Leader));
                region_engine.handle_request_delay = Some(Duration::from_millis(300));
                region_engine.handle_request_mock_fn = Some(Box::new(|_, _| {
                    error::UnexpectedSnafu {
                        violated: "mock flush failed",
                    }
                    .fail()
                }));
                region_engine.handle_set_readonly_gracefully_mock_fn = Some(Box::new(|_| {
                    Ok(SetRegionRoleStateResponse::success(
                        SetRegionRoleStateSuccess::mito(1024),
                    ))
                }))
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);

        let waits = vec![
            Some(Duration::from_millis(100u64)),
            Some(Duration::from_millis(100u64)),
        ];

        for flush_timeout in waits {
            let reply = DowngradeRegionsHandler
                .handle(
                    &handler_context,
                    vec![DowngradeRegion {
                        region_id,
                        flush_timeout,
                    }],
                )
                .await;
            let reply = &reply.unwrap().expect_downgrade_regions_reply()[0];
            assert!(reply.exists);
            assert!(reply.error.as_ref().unwrap().contains("timeout"));
            assert!(reply.last_entry_id.is_none());
        }
        let timer = Instant::now();
        let reply = DowngradeRegionsHandler
            .handle(
                &handler_context,
                vec![DowngradeRegion {
                    region_id,
                    flush_timeout: Some(Duration::from_millis(500)),
                }],
            )
            .await;
        // Must less than 300 ms.
        assert!(timer.elapsed().as_millis() < 300);
        let reply = &reply.unwrap().expect_downgrade_regions_reply()[0];
        assert!(reply.exists);
        assert!(reply.error.as_ref().unwrap().contains("flush failed"));
        assert!(reply.last_entry_id.is_none());
    }

    #[tokio::test]
    async fn test_set_region_readonly_not_found() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Leader));
                region_engine.handle_set_readonly_gracefully_mock_fn =
                    Some(Box::new(|_| Ok(SetRegionRoleStateResponse::NotFound)));
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);
        let reply = DowngradeRegionsHandler
            .handle(
                &handler_context,
                vec![DowngradeRegion {
                    region_id,
                    flush_timeout: None,
                }],
            )
            .await;
        let reply = &reply.unwrap().expect_downgrade_regions_reply()[0];
        assert!(!reply.exists);
        assert!(reply.error.is_none());
        assert!(reply.last_entry_id.is_none());
    }

    #[tokio::test]
    async fn test_set_region_readonly_error() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Leader));
                region_engine.handle_set_readonly_gracefully_mock_fn = Some(Box::new(|_| {
                    error::UnexpectedSnafu {
                        violated: "Failed to set region to readonly",
                    }
                    .fail()
                }));
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let handler_context = HandlerContext::new_for_test(mock_region_server, kv_backend);
        let reply = DowngradeRegionsHandler
            .handle(
                &handler_context,
                vec![DowngradeRegion {
                    region_id,
                    flush_timeout: None,
                }],
            )
            .await;
        let reply = &reply.unwrap().expect_downgrade_regions_reply()[0];
        assert!(reply.exists);
        assert!(
            reply
                .error
                .as_ref()
                .unwrap()
                .contains("Failed to set region to readonly")
        );
        assert!(reply.last_entry_id.is_none());
    }

    #[tokio::test]
    async fn test_downgrade_regions() {
        common_telemetry::init_default_ut_logging();

        let mut region_server = mock_region_server();
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let heartbeat_handler =
            RegionHeartbeatResponseHandler::new(region_server.clone(), kv_backend);
        let mut engine_env = TestEnv::with_prefix("downgrade-regions").await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine.clone()));
        let region_id = RegionId::new(1024, 1);
        let region_id1 = RegionId::new(1024, 2);
        let builder = CreateRequestBuilder::new();
        let create_req = builder.build();
        region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap();
        let create_req1 = builder.build();
        region_server
            .handle_request(region_id1, RegionRequest::Create(create_req1))
            .await
            .unwrap();
        let meta = MessageMeta::new_test(1, "test", "dn-1", "meta-0");
        let instruction = Instruction::DowngradeRegions(vec![
            DowngradeRegion {
                region_id,
                flush_timeout: Some(Duration::from_secs(1)),
            },
            DowngradeRegion {
                region_id: region_id1,
                flush_timeout: Some(Duration::from_secs(1)),
            },
        ]);
        let mut heartbeat_env = HeartbeatResponseTestEnv::new();
        let mut ctx = heartbeat_env.create_handler_ctx((meta, instruction));
        let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
        assert_matches!(control, HandleControl::Continue);

        let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();
        let reply = reply.expect_downgrade_regions_reply();
        assert_eq!(reply[0].region_id, region_id);
        assert!(reply[0].exists);
        assert!(reply[0].error.is_none());
        assert_eq!(reply[0].last_entry_id, Some(0));
        assert_eq!(reply[1].region_id, region_id1);
        assert!(reply[1].exists);
        assert!(reply[1].error.is_none());
        assert_eq!(reply[1].last_entry_id, Some(0));

        assert_eq!(engine.role(region_id).unwrap(), RegionRole::Follower);
        assert_eq!(engine.role(region_id1).unwrap(), RegionRole::Follower);
    }
}
