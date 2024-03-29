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

use common_error::ext::ErrorExt;
use common_meta::instruction::{InstructionReply, UpgradeRegion, UpgradeRegionReply};
use common_telemetry::{info, warn};
use futures_util::future::BoxFuture;
use store_api::region_request::{RegionCatchupRequest, RegionRequest};

use crate::heartbeat::handler::HandlerContext;
use crate::heartbeat::task_tracker::WaitResult;

impl HandlerContext {
    pub(crate) fn handle_upgrade_region_instruction(
        self,
        UpgradeRegion {
            region_id,
            last_entry_id,
            wait_for_replay_timeout,
        }: UpgradeRegion,
    ) -> BoxFuture<'static, InstructionReply> {
        Box::pin(async move {
            let Some(writable) = self.region_server.is_writable(region_id) else {
                return InstructionReply::UpgradeRegion(UpgradeRegionReply {
                    ready: false,
                    exists: false,
                    error: None,
                });
            };

            if writable {
                return InstructionReply::UpgradeRegion(UpgradeRegionReply {
                    ready: true,
                    exists: true,
                    error: None,
                });
            }

            let region_server_moved = self.region_server.clone();

            // The catchup task is almost zero cost if the inside region is writable.
            // Therefore, it always registers a new catchup task.
            let register_result = self
                .catchup_tasks
                .try_register(
                    region_id,
                    Box::pin(async move {
                        info!("Executing region: {region_id} catchup to: last entry id {last_entry_id:?}");
                        region_server_moved
                            .handle_request(
                                region_id,
                                RegionRequest::Catchup(RegionCatchupRequest {
                                    set_writable: true,
                                    entry_id: last_entry_id,
                                }),
                            )
                            .await?;

                        Ok(())
                    }),
                )
                .await;

            if register_result.is_busy() {
                warn!("Another catchup task is running for the region: {region_id}");
            }

            // Returns immediately
            let Some(wait_for_replay_timeout) = wait_for_replay_timeout else {
                return InstructionReply::UpgradeRegion(UpgradeRegionReply {
                    ready: false,
                    exists: true,
                    error: None,
                });
            };

            // We don't care that it returns a newly registered or running task.
            let mut watcher = register_result.into_watcher();
            let result = self
                .catchup_tasks
                .wait(&mut watcher, wait_for_replay_timeout)
                .await;

            match result {
                WaitResult::Timeout => InstructionReply::UpgradeRegion(UpgradeRegionReply {
                    ready: false,
                    exists: true,
                    error: None,
                }),
                WaitResult::Finish(Ok(_)) => InstructionReply::UpgradeRegion(UpgradeRegionReply {
                    ready: true,
                    exists: true,
                    error: None,
                }),
                WaitResult::Finish(Err(err)) => {
                    InstructionReply::UpgradeRegion(UpgradeRegionReply {
                        ready: false,
                        exists: true,
                        error: Some(err.output_msg()),
                    })
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::time::Duration;

    use common_meta::instruction::{InstructionReply, UpgradeRegion};
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;
    use tokio::time::Instant;

    use crate::error;
    use crate::heartbeat::handler::HandlerContext;
    use crate::heartbeat::task_tracker::TaskTracker;
    use crate::tests::{mock_region_server, MockRegionEngine};

    #[tokio::test]
    async fn test_region_not_exist() {
        let mut mock_region_server = mock_region_server();
        let (mock_engine, _) = MockRegionEngine::new();
        mock_region_server.register_engine(mock_engine);

        let handler_context = HandlerContext {
            region_server: mock_region_server,
            catchup_tasks: TaskTracker::new(),
        };

        let region_id = RegionId::new(1024, 1);
        let waits = vec![None, Some(Duration::from_millis(100u64))];

        for wait_for_replay_timeout in waits {
            let reply = handler_context
                .clone()
                .handle_upgrade_region_instruction(UpgradeRegion {
                    region_id,
                    last_entry_id: None,
                    wait_for_replay_timeout,
                })
                .await;
            assert_matches!(reply, InstructionReply::UpgradeRegion(_));

            if let InstructionReply::UpgradeRegion(reply) = reply {
                assert!(!reply.exists);
                assert!(reply.error.is_none());
            }
        }
    }

    #[tokio::test]
    async fn test_region_writable() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);

        let (mock_engine, _) = MockRegionEngine::with_custom_apply_fn(|region_engine| {
            region_engine.mock_role = Some(Some(RegionRole::Leader));
            region_engine.handle_request_mock_fn = Some(Box::new(|_, _| {
                // Should be unreachable.
                unreachable!();
            }));
        });
        mock_region_server.register_test_region(region_id, mock_engine);

        let handler_context = HandlerContext {
            region_server: mock_region_server,
            catchup_tasks: TaskTracker::new(),
        };

        let waits = vec![None, Some(Duration::from_millis(100u64))];

        for wait_for_replay_timeout in waits {
            let reply = handler_context
                .clone()
                .handle_upgrade_region_instruction(UpgradeRegion {
                    region_id,
                    last_entry_id: None,
                    wait_for_replay_timeout,
                })
                .await;
            assert_matches!(reply, InstructionReply::UpgradeRegion(_));

            if let InstructionReply::UpgradeRegion(reply) = reply {
                assert!(reply.ready);
                assert!(reply.exists);
                assert!(reply.error.is_none());
            }
        }
    }

    #[tokio::test]
    async fn test_region_not_ready() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);

        let (mock_engine, _) = MockRegionEngine::with_custom_apply_fn(|region_engine| {
            // Region is not ready.
            region_engine.mock_role = Some(Some(RegionRole::Follower));
            region_engine.handle_request_mock_fn = Some(Box::new(|_, _| Ok(0)));
            // Note: Don't change.
            region_engine.handle_request_delay = Some(Duration::from_secs(100));
        });
        mock_region_server.register_test_region(region_id, mock_engine);

        let handler_context = HandlerContext {
            region_server: mock_region_server,
            catchup_tasks: TaskTracker::new(),
        };

        let waits = vec![None, Some(Duration::from_millis(100u64))];

        for wait_for_replay_timeout in waits {
            let reply = handler_context
                .clone()
                .handle_upgrade_region_instruction(UpgradeRegion {
                    region_id,
                    last_entry_id: None,
                    wait_for_replay_timeout,
                })
                .await;
            assert_matches!(reply, InstructionReply::UpgradeRegion(_));

            if let InstructionReply::UpgradeRegion(reply) = reply {
                assert!(!reply.ready);
                assert!(reply.exists);
                assert!(reply.error.is_none());
            }
        }
    }

    #[tokio::test]
    async fn test_region_not_ready_with_retry() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);

        let (mock_engine, _) = MockRegionEngine::with_custom_apply_fn(|region_engine| {
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

        let handler_context = HandlerContext {
            region_server: mock_region_server,
            catchup_tasks: TaskTracker::new(),
        };

        for wait_for_replay_timeout in waits {
            let reply = handler_context
                .clone()
                .handle_upgrade_region_instruction(UpgradeRegion {
                    region_id,
                    last_entry_id: None,
                    wait_for_replay_timeout,
                })
                .await;
            assert_matches!(reply, InstructionReply::UpgradeRegion(_));

            if let InstructionReply::UpgradeRegion(reply) = reply {
                assert!(!reply.ready);
                assert!(reply.exists);
                assert!(reply.error.is_none());
            }
        }

        let timer = Instant::now();
        let reply = handler_context
            .handle_upgrade_region_instruction(UpgradeRegion {
                region_id,
                last_entry_id: None,
                wait_for_replay_timeout: Some(Duration::from_millis(500)),
            })
            .await;
        assert_matches!(reply, InstructionReply::UpgradeRegion(_));
        // Must less than 300 ms.
        assert!(timer.elapsed().as_millis() < 300);

        if let InstructionReply::UpgradeRegion(reply) = reply {
            assert!(reply.ready);
            assert!(reply.exists);
            assert!(reply.error.is_none());
        }
    }

    #[tokio::test]
    async fn test_region_error() {
        let mock_region_server = mock_region_server();
        let region_id = RegionId::new(1024, 1);

        let (mock_engine, _) = MockRegionEngine::with_custom_apply_fn(|region_engine| {
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

        let handler_context = HandlerContext {
            region_server: mock_region_server,
            catchup_tasks: TaskTracker::new(),
        };

        let reply = handler_context
            .clone()
            .handle_upgrade_region_instruction(UpgradeRegion {
                region_id,
                last_entry_id: None,
                wait_for_replay_timeout: None,
            })
            .await;
        assert_matches!(reply, InstructionReply::UpgradeRegion(_));

        // It didn't wait for handle returns; it had no idea about the error.
        if let InstructionReply::UpgradeRegion(reply) = reply {
            assert!(!reply.ready);
            assert!(reply.exists);
            assert!(reply.error.is_none());
        }

        let reply = handler_context
            .clone()
            .handle_upgrade_region_instruction(UpgradeRegion {
                region_id,
                last_entry_id: None,
                wait_for_replay_timeout: Some(Duration::from_millis(200)),
            })
            .await;
        assert_matches!(reply, InstructionReply::UpgradeRegion(_));

        if let InstructionReply::UpgradeRegion(reply) = reply {
            assert!(!reply.ready);
            assert!(reply.exists);
            assert!(reply.error.is_some());
            assert!(reply.error.unwrap().contains("mock_error"));
        }
    }
}
