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

use common_meta::instruction::{DowngradeRegion, DowngradeRegionReply, InstructionReply};
use common_telemetry::tracing::info;
use common_telemetry::warn;
use futures_util::future::BoxFuture;
use store_api::region_engine::SetReadonlyResponse;
use store_api::region_request::{RegionFlushRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::heartbeat::handler::HandlerContext;
use crate::heartbeat::task_tracker::WaitResult;

impl HandlerContext {
    async fn set_readonly_gracefully(&self, region_id: RegionId) -> InstructionReply {
        match self.region_server.set_readonly_gracefully(region_id).await {
            Ok(SetReadonlyResponse::Success { last_entry_id }) => {
                InstructionReply::DowngradeRegion(DowngradeRegionReply {
                    last_entry_id,
                    exists: true,
                    error: None,
                })
            }
            Ok(SetReadonlyResponse::NotFound) => {
                InstructionReply::DowngradeRegion(DowngradeRegionReply {
                    last_entry_id: None,
                    exists: false,
                    error: None,
                })
            }
            Err(err) => InstructionReply::DowngradeRegion(DowngradeRegionReply {
                last_entry_id: None,
                exists: true,
                error: Some(format!("{err:?}")),
            }),
        }
    }

    pub(crate) fn handle_downgrade_region_instruction(
        self,
        DowngradeRegion {
            region_id,
            wait_for_flush_timeout,
        }: DowngradeRegion,
    ) -> BoxFuture<'static, InstructionReply> {
        Box::pin(async move {
            let Some(writable) = self.region_server.is_writable(region_id) else {
                return InstructionReply::DowngradeRegion(DowngradeRegionReply {
                    last_entry_id: None,
                    exists: false,
                    error: None,
                });
            };

            // Ignores flush request
            if !writable {
                return self.set_readonly_gracefully(region_id).await;
            }

            let region_server_moved = self.region_server.clone();
            if let Some(flush_timeout) = wait_for_flush_timeout {
                let register_result = self
                    .downgrade_tasks
                    .try_register(
                        region_id,
                        Box::pin(async move {
                            info!("Flush region: {region_id} before downgrading region");
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
                let result = self.catchup_tasks.wait(&mut watcher, flush_timeout).await;

                match result {
                    WaitResult::Timeout => {
                        InstructionReply::DowngradeRegion(DowngradeRegionReply {
                            last_entry_id: None,
                            exists: true,
                            error: Some(format!(
                                "Flush region: {region_id} before downgrading region is timeout"
                            )),
                        })
                    }
                    WaitResult::Finish(Ok(_)) => self.set_readonly_gracefully(region_id).await,
                    WaitResult::Finish(Err(err)) => {
                        InstructionReply::DowngradeRegion(DowngradeRegionReply {
                            last_entry_id: None,
                            exists: true,
                            error: Some(format!("{err:?}")),
                        })
                    }
                }
            } else {
                self.set_readonly_gracefully(region_id).await
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::time::Duration;

    use common_meta::instruction::{DowngradeRegion, InstructionReply};
    use mito2::engine::MITO_ENGINE_NAME;
    use store_api::region_engine::{RegionRole, SetReadonlyResponse};
    use store_api::region_request::RegionRequest;
    use store_api::storage::RegionId;
    use tokio::time::Instant;

    use crate::heartbeat::handler::HandlerContext;
    use crate::tests::{mock_region_server, MockRegionEngine};

    #[tokio::test]
    async fn test_region_not_exist() {
        let mut mock_region_server = mock_region_server();
        let (mock_engine, _) = MockRegionEngine::new(MITO_ENGINE_NAME);
        mock_region_server.register_engine(mock_engine);
        let handler_context = HandlerContext::new_for_test(mock_region_server);
        let region_id = RegionId::new(1024, 1);
        let waits = vec![None, Some(Duration::from_millis(100u64))];

        for wait_for_flush_timeout in waits {
            let reply = handler_context
                .clone()
                .handle_downgrade_region_instruction(DowngradeRegion {
                    region_id,
                    wait_for_flush_timeout,
                })
                .await;
            assert_matches!(reply, InstructionReply::DowngradeRegion(_));

            if let InstructionReply::DowngradeRegion(reply) = reply {
                assert!(!reply.exists);
                assert!(reply.error.is_none());
                assert!(reply.last_entry_id.is_none());
            }
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
                region_engine.handle_set_readonly_gracefully_mock_fn =
                    Some(Box::new(|_| Ok(SetReadonlyResponse::success(Some(1024)))))
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let handler_context = HandlerContext::new_for_test(mock_region_server);

        let waits = vec![None, Some(Duration::from_millis(100u64))];
        for wait_for_flush_timeout in waits {
            let reply = handler_context
                .clone()
                .handle_downgrade_region_instruction(DowngradeRegion {
                    region_id,
                    wait_for_flush_timeout,
                })
                .await;
            assert_matches!(reply, InstructionReply::DowngradeRegion(_));

            if let InstructionReply::DowngradeRegion(reply) = reply {
                assert!(reply.exists);
                assert!(reply.error.is_none());
                assert_eq!(reply.last_entry_id.unwrap(), 1024);
            }
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
                region_engine.handle_set_readonly_gracefully_mock_fn =
                    Some(Box::new(|_| Ok(SetReadonlyResponse::success(Some(1024)))))
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let handler_context = HandlerContext::new_for_test(mock_region_server);

        let wait_for_flush_timeout = Duration::from_millis(100);
        let reply = handler_context
            .clone()
            .handle_downgrade_region_instruction(DowngradeRegion {
                region_id,
                wait_for_flush_timeout: Some(wait_for_flush_timeout),
            })
            .await;
        assert_matches!(reply, InstructionReply::DowngradeRegion(_));

        if let InstructionReply::DowngradeRegion(reply) = reply {
            assert!(reply.exists);
            assert!(reply.error.unwrap().contains("timeout"));
            assert!(reply.last_entry_id.is_none());
        }
    }

    #[tokio::test]
    async fn test_region_flush_timeout_and_retry() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Leader));
                region_engine.handle_request_delay = Some(Duration::from_millis(300));
                region_engine.handle_set_readonly_gracefully_mock_fn =
                    Some(Box::new(|_| Ok(SetReadonlyResponse::success(Some(1024)))))
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let handler_context = HandlerContext::new_for_test(mock_region_server);

        let waits = vec![
            Some(Duration::from_millis(100u64)),
            Some(Duration::from_millis(100u64)),
        ];

        for wait_for_flush_timeout in waits {
            let reply = handler_context
                .clone()
                .handle_downgrade_region_instruction(DowngradeRegion {
                    region_id,
                    wait_for_flush_timeout,
                })
                .await;
            assert_matches!(reply, InstructionReply::DowngradeRegion(_));
            if let InstructionReply::DowngradeRegion(reply) = reply {
                assert!(reply.exists);
                assert!(reply.error.unwrap().contains("timeout"));
                assert!(reply.last_entry_id.is_none());
            }
        }
        let timer = Instant::now();
        let reply = handler_context
            .handle_downgrade_region_instruction(DowngradeRegion {
                region_id,
                wait_for_flush_timeout: Some(Duration::from_millis(500)),
            })
            .await;
        assert_matches!(reply, InstructionReply::DowngradeRegion(_));
        // Must less than 300 ms.
        assert!(timer.elapsed().as_millis() < 300);

        if let InstructionReply::DowngradeRegion(reply) = reply {
            assert!(reply.exists);
            assert!(reply.error.is_none());
            assert_eq!(reply.last_entry_id.unwrap(), 1024);
        }
    }

    #[tokio::test]
    async fn test_set_region_readonly_not_found() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);
        let (mock_engine, _) =
            MockRegionEngine::with_custom_apply_fn(MITO_ENGINE_NAME, |region_engine| {
                region_engine.mock_role = Some(Some(RegionRole::Leader));
                region_engine.handle_set_readonly_gracefully_mock_fn =
                    Some(Box::new(|_| Ok(SetReadonlyResponse::NotFound)));
            });
        mock_region_server.register_test_region(region_id, mock_engine);
        let handler_context = HandlerContext::new_for_test(mock_region_server);
        let reply = handler_context
            .clone()
            .handle_downgrade_region_instruction(DowngradeRegion {
                region_id,
                wait_for_flush_timeout: None,
            })
            .await;
        assert_matches!(reply, InstructionReply::DowngradeRegion(_));
        if let InstructionReply::DowngradeRegion(reply) = reply {
            assert!(!reply.exists);
            assert!(reply.error.is_none());
            assert!(reply.last_entry_id.is_none());
        }
    }
}
