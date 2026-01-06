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

use std::any::Any;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use api::v1::meta::MailboxMessage;
use common_meta::instruction::{
    EnterStagingRegionReply, EnterStagingRegionsReply, Instruction, InstructionReply,
};
use common_meta::peer::Peer;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};

use crate::error::{self, Error, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::repartition::group::remap_manifest::RemapManifest;
use crate::procedure::repartition::group::utils::{
    HandleMultipleResult, group_region_routes_by_peer, handle_multiple_results,
};
use crate::procedure::repartition::group::{Context, GroupPrepareResult, State};
use crate::procedure::repartition::plan::RegionDescriptor;
use crate::service::mailbox::{Channel, MailboxRef};

#[derive(Debug, Serialize, Deserialize)]
pub struct EnterStagingRegion;

#[async_trait::async_trait]
#[typetag::serde]
impl State for EnterStagingRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        self.enter_staging_regions(ctx).await?;

        Ok((Box::new(RemapManifest), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnterStagingRegion {
    fn build_enter_staging_instructions(
        prepare_result: &GroupPrepareResult,
        targets: &[RegionDescriptor],
    ) -> Result<HashMap<Peer, Vec<common_meta::instruction::EnterStagingRegion>>> {
        let target_partition_expr_by_region = targets
            .iter()
            .map(|target| {
                Ok((
                    target.region_id,
                    target
                        .partition_expr
                        .as_json_str()
                        .context(error::SerializePartitionExprSnafu)?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        // Safety: `leader_peer` is set for all region routes, checked in `repartition_start`.
        let target_region_routes_by_peer =
            group_region_routes_by_peer(&prepare_result.target_routes);
        let mut instructions = HashMap::with_capacity(target_region_routes_by_peer.len());
        for (peer, region_ids) in target_region_routes_by_peer {
            let enter_staging_regions = region_ids
                .into_iter()
                .map(|region_id| common_meta::instruction::EnterStagingRegion {
                    region_id,
                    // Safety: the target_routes is constructed from the targets, so the region_id is always present in the map.
                    partition_expr: target_partition_expr_by_region[&region_id].clone(),
                })
                .collect();
            instructions.insert(peer.clone(), enter_staging_regions);
        }

        Ok(instructions)
    }

    #[allow(dead_code)]
    async fn enter_staging_regions(&self, ctx: &mut Context) -> Result<()> {
        let table_id = ctx.persistent_ctx.table_id;
        let group_id = ctx.persistent_ctx.group_id;
        // Safety: the group prepare result is set in the RepartitionStart state.
        let prepare_result = ctx.persistent_ctx.group_prepare_result.as_ref().unwrap();
        let targets = &ctx.persistent_ctx.targets;
        let instructions = Self::build_enter_staging_instructions(prepare_result, targets)?;
        let operation_timeout =
            ctx.next_operation_timeout()
                .context(error::ExceededDeadlineSnafu {
                    operation: "Enter staging regions",
                })?;
        let (peers, tasks): (Vec<_>, Vec<_>) = instructions
            .iter()
            .map(|(peer, enter_staging_regions)| {
                (
                    peer,
                    Self::enter_staging_region(
                        &ctx.mailbox,
                        &ctx.server_addr,
                        peer,
                        enter_staging_regions,
                        operation_timeout,
                    ),
                )
            })
            .unzip();
        info!(
            "Sent enter staging regions instructions to peers: {:?} for repartition table {}, group id {}",
            peers, table_id, group_id
        );

        let format_err_msg = |idx: usize, error: &Error| {
            let peer = peers[idx];
            format!(
                "Failed to enter staging regions on datanode {:?}, error: {:?}",
                peer, error
            )
        };
        // Waits for all tasks to complete.
        let results = join_all(tasks).await;
        let result = handle_multiple_results(&results);
        match result {
            HandleMultipleResult::AllSuccessful => Ok(()),
            HandleMultipleResult::AllRetryable(retryable_errors) => error::RetryLaterSnafu {
                reason: format!(
                    "All retryable errors during entering staging regions for repartition table {}, group id {}: {:?}",
                    table_id, group_id,
                    retryable_errors
                        .iter()
                        .map(|(idx, error)| format_err_msg(*idx, error))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            }
            .fail(),
            HandleMultipleResult::AllNonRetryable(non_retryable_errors) => error::UnexpectedSnafu {
                violated: format!(
                    "All non retryable errors during entering staging regions for repartition table {}, group id {}: {:?}",
                    table_id, group_id,
                    non_retryable_errors
                        .iter()
                        .map(|(idx, error)| format_err_msg(*idx, error))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            }
            .fail(),
            HandleMultipleResult::PartialRetryable {
                retryable_errors,
                non_retryable_errors,
            } => error::UnexpectedSnafu {
                violated: format!(
                    "Partial retryable errors during entering staging regions for repartition table {}, group id {}: {:?}, non retryable errors: {:?}",
                    table_id, group_id,
                    retryable_errors
                        .iter()
                        .map(|(idx, error)| format_err_msg(*idx, error))
                        .collect::<Vec<_>>()
                        .join(","),
                    non_retryable_errors
                        .iter()
                        .map(|(idx, error)| format_err_msg(*idx, error))
                        .collect::<Vec<_>>()
                        .join(","),
                ),
            }
            .fail(),
        }
    }

    /// Enter staging region on a datanode.
    ///
    /// Retry:
    /// - Pusher is not found.
    /// - Mailbox timeout.
    ///
    /// Abort(non-retry):
    /// - Unexpected instruction reply.
    /// - Exceeded deadline of enter staging regions instruction.
    /// - Target region doesn't exist on the datanode.
    async fn enter_staging_region(
        mailbox: &MailboxRef,
        server_addr: &str,
        peer: &Peer,
        enter_staging_regions: &[common_meta::instruction::EnterStagingRegion],
        timeout: Duration,
    ) -> Result<()> {
        let ch = Channel::Datanode(peer.id);
        let instruction = Instruction::EnterStagingRegions(enter_staging_regions.to_vec());
        let message = MailboxMessage::json_message(
            &format!(
                "Enter staging regions: {:?}",
                enter_staging_regions
                    .iter()
                    .map(|r| r.region_id)
                    .collect::<Vec<_>>()
            ),
            &format!("Metasrv@{}", server_addr),
            &format!("Datanode-{}@{}", peer.id, peer.addr),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;
        let now = Instant::now();
        let receiver = mailbox.send(&ch, message, timeout).await;

        let receiver = match receiver {
            Ok(receiver) => receiver,
            Err(error::Error::PusherNotFound { .. }) => error::RetryLaterSnafu {
                reason: format!(
                    "Pusher not found for enter staging regions on datanode {:?}, elapsed: {:?}",
                    peer,
                    now.elapsed()
                ),
            }
            .fail()?,
            Err(err) => {
                return Err(err);
            }
        };

        match receiver.await {
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                info!(
                    "Received enter staging regions reply: {:?}, elapsed: {:?}",
                    reply,
                    now.elapsed()
                );
                let InstructionReply::EnterStagingRegions(EnterStagingRegionsReply { replies }) =
                    reply
                else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect enter staging regions reply",
                    }
                    .fail();
                };
                for reply in replies {
                    Self::handle_enter_staging_region_reply(&reply, &now, peer)?;
                }

                Ok(())
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for enter staging regions on datanode {:?}, elapsed: {:?}",
                    peer,
                    now.elapsed()
                );
                error::RetryLaterSnafu { reason }.fail()
            }
            Err(err) => Err(err),
        }
    }

    fn handle_enter_staging_region_reply(
        EnterStagingRegionReply {
            region_id,
            ready,
            exists,
            error,
        }: &EnterStagingRegionReply,
        now: &Instant,
        peer: &Peer,
    ) -> Result<()> {
        ensure!(
            exists,
            error::UnexpectedSnafu {
                violated: format!(
                    "Region {} doesn't exist on datanode {:?}, elapsed: {:?}",
                    region_id,
                    peer,
                    now.elapsed()
                )
            }
        );

        if error.is_some() {
            return error::RetryLaterSnafu {
                reason: format!(
                    "Failed to enter staging region {} on datanode {:?}, error: {:?}, elapsed: {:?}",
                    region_id, peer, error, now.elapsed()
                ),
            }
            .fail();
        }

        ensure!(
            ready,
            error::RetryLaterSnafu {
                reason: format!(
                    "Region {} is still entering staging state on datanode {:?}, elapsed: {:?}",
                    region_id,
                    peer,
                    now.elapsed()
                ),
            }
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::time::Duration;

    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::RegionId;

    use crate::error::{self, Error};
    use crate::procedure::repartition::group::GroupPrepareResult;
    use crate::procedure::repartition::group::enter_staging_region::EnterStagingRegion;
    use crate::procedure::repartition::plan::RegionDescriptor;
    use crate::procedure::repartition::test_util::{
        TestingEnv, new_persistent_context, range_expr,
    };
    use crate::procedure::test_util::{
        new_close_region_reply, new_enter_staging_region_reply, send_mock_reply,
    };
    use crate::service::mailbox::Channel;

    #[test]
    fn test_build_enter_staging_instructions() {
        let table_id = 1024;
        let prepare_result = GroupPrepareResult {
            source_routes: vec![RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 1),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            }],
            target_routes: vec![
                RegionRoute {
                    region: Region {
                        id: RegionId::new(table_id, 1),
                        ..Default::default()
                    },
                    leader_peer: Some(Peer::empty(1)),
                    ..Default::default()
                },
                RegionRoute {
                    region: Region {
                        id: RegionId::new(table_id, 2),
                        ..Default::default()
                    },
                    leader_peer: Some(Peer::empty(2)),
                    ..Default::default()
                },
            ],
            central_region: RegionId::new(table_id, 1),
            central_region_datanode: Peer::empty(1),
        };
        let targets = test_targets();
        let instructions =
            EnterStagingRegion::build_enter_staging_instructions(&prepare_result, &targets)
                .unwrap();

        assert_eq!(instructions.len(), 2);
        let instruction_1 = instructions.get(&Peer::empty(1)).unwrap().clone();
        assert_eq!(
            instruction_1,
            vec![common_meta::instruction::EnterStagingRegion {
                region_id: RegionId::new(table_id, 1),
                partition_expr: range_expr("x", 0, 10).as_json_str().unwrap(),
            }]
        );
        let instruction_2 = instructions.get(&Peer::empty(2)).unwrap().clone();
        assert_eq!(
            instruction_2,
            vec![common_meta::instruction::EnterStagingRegion {
                region_id: RegionId::new(table_id, 2),
                partition_expr: range_expr("x", 10, 20).as_json_str().unwrap(),
            }]
        );
    }

    #[tokio::test]
    async fn test_datanode_is_unreachable() {
        let env = TestingEnv::new();
        let server_addr = "localhost";
        let peer = Peer::empty(1);
        let enter_staging_regions = vec![common_meta::instruction::EnterStagingRegion {
            region_id: RegionId::new(1024, 1),
            partition_expr: range_expr("x", 0, 10).as_json_str().unwrap(),
        }];
        let timeout = Duration::from_secs(10);

        let err = EnterStagingRegion::enter_staging_region(
            env.mailbox_ctx.mailbox(),
            server_addr,
            &peer,
            &enter_staging_regions,
            timeout,
        )
        .await
        .unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_enter_staging_region_exceeded_deadline() {
        let mut env = TestingEnv::new();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
            .await;
        let server_addr = "localhost";
        let peer = Peer::empty(1);
        let enter_staging_regions = vec![common_meta::instruction::EnterStagingRegion {
            region_id: RegionId::new(1024, 1),
            partition_expr: range_expr("x", 0, 10).as_json_str().unwrap(),
        }];
        let timeout = Duration::from_secs(10);

        // Sends a timeout error.
        send_mock_reply(env.mailbox_ctx.mailbox().clone(), rx, |id| {
            Err(error::MailboxTimeoutSnafu { id }.build())
        });

        let err = EnterStagingRegion::enter_staging_region(
            env.mailbox_ctx.mailbox(),
            server_addr,
            &peer,
            &enter_staging_regions,
            timeout,
        )
        .await
        .unwrap_err();
        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_unexpected_instruction_reply() {
        let mut env = TestingEnv::new();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let server_addr = "localhost";
        let peer = Peer::empty(1);
        let enter_staging_regions = vec![common_meta::instruction::EnterStagingRegion {
            region_id: RegionId::new(1024, 1),
            partition_expr: range_expr("x", 0, 10).as_json_str().unwrap(),
        }];
        let timeout = Duration::from_secs(10);

        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
            .await;
        // Sends an incorrect reply.
        send_mock_reply(env.mailbox_ctx.mailbox().clone(), rx, |id| {
            Ok(new_close_region_reply(id))
        });

        let err = EnterStagingRegion::enter_staging_region(
            env.mailbox_ctx.mailbox(),
            server_addr,
            &peer,
            &enter_staging_regions,
            timeout,
        )
        .await
        .unwrap_err();
        assert_matches!(err, Error::UnexpectedInstructionReply { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_enter_staging_region_failed_to_enter_staging_state() {
        let mut env = TestingEnv::new();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
            .await;
        let server_addr = "localhost";
        let peer = Peer::empty(1);
        let enter_staging_regions = vec![common_meta::instruction::EnterStagingRegion {
            region_id: RegionId::new(1024, 1),
            partition_expr: range_expr("x", 0, 10).as_json_str().unwrap(),
        }];
        let timeout = Duration::from_secs(10);

        // Sends a failed reply.
        send_mock_reply(env.mailbox_ctx.mailbox().clone(), rx, |id| {
            Ok(new_enter_staging_region_reply(
                id,
                RegionId::new(1024, 1),
                false,
                true,
                Some("test mocked".to_string()),
            ))
        });

        let err = EnterStagingRegion::enter_staging_region(
            env.mailbox_ctx.mailbox(),
            server_addr,
            &peer,
            &enter_staging_regions,
            timeout,
        )
        .await
        .unwrap_err();
        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
            .await;
        // Region doesn't exist on the datanode.
        send_mock_reply(env.mailbox_ctx.mailbox().clone(), rx, |id| {
            Ok(new_enter_staging_region_reply(
                id,
                RegionId::new(1024, 1),
                false,
                false,
                None,
            ))
        });

        let err = EnterStagingRegion::enter_staging_region(
            env.mailbox_ctx.mailbox(),
            server_addr,
            &peer,
            &enter_staging_regions,
            timeout,
        )
        .await
        .unwrap_err();
        assert_matches!(err, Error::Unexpected { .. });
        assert!(!err.is_retryable());
    }

    fn test_prepare_result(table_id: u32) -> GroupPrepareResult {
        GroupPrepareResult {
            source_routes: vec![],
            target_routes: vec![
                RegionRoute {
                    region: Region {
                        id: RegionId::new(table_id, 1),
                        ..Default::default()
                    },
                    leader_peer: Some(Peer::empty(1)),
                    ..Default::default()
                },
                RegionRoute {
                    region: Region {
                        id: RegionId::new(table_id, 2),
                        ..Default::default()
                    },
                    leader_peer: Some(Peer::empty(2)),
                    ..Default::default()
                },
            ],
            central_region: RegionId::new(table_id, 1),
            central_region_datanode: Peer::empty(1),
        }
    }

    fn test_targets() -> Vec<RegionDescriptor> {
        vec![
            RegionDescriptor {
                region_id: RegionId::new(1024, 1),
                partition_expr: range_expr("x", 0, 10),
            },
            RegionDescriptor {
                region_id: RegionId::new(1024, 2),
                partition_expr: range_expr("x", 10, 20),
            },
        ]
    }

    #[tokio::test]
    async fn test_enter_staging_regions_all_successful() {
        let mut env = TestingEnv::new();
        let table_id = 1024;
        let targets = test_targets();
        let mut persistent_context = new_persistent_context(table_id, vec![], targets);
        persistent_context.group_prepare_result = Some(test_prepare_result(table_id));

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
            .await;
        send_mock_reply(env.mailbox_ctx.mailbox().clone(), rx, |id| {
            Ok(new_enter_staging_region_reply(
                id,
                RegionId::new(1024, 1),
                true,
                true,
                None,
            ))
        });
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(2), tx)
            .await;
        send_mock_reply(env.mailbox_ctx.mailbox().clone(), rx, |id| {
            Ok(new_enter_staging_region_reply(
                id,
                RegionId::new(1024, 2),
                true,
                true,
                None,
            ))
        });
        let mut ctx = env.create_context(persistent_context);
        EnterStagingRegion
            .enter_staging_regions(&mut ctx)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_enter_staging_region_retryable() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let targets = test_targets();
        let mut persistent_context = new_persistent_context(table_id, vec![], targets);
        persistent_context.group_prepare_result = Some(test_prepare_result(table_id));
        let mut ctx = env.create_context(persistent_context);
        let err = EnterStagingRegion
            .enter_staging_regions(&mut ctx)
            .await
            .unwrap_err();
        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_enter_staging_regions_non_retryable() {
        let mut env = TestingEnv::new();
        let table_id = 1024;
        let targets = test_targets();
        let mut persistent_context = new_persistent_context(table_id, vec![], targets);
        persistent_context.group_prepare_result = Some(test_prepare_result(table_id));
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
            .await;
        // Sends an incorrect reply.
        send_mock_reply(env.mailbox_ctx.mailbox().clone(), rx, |id| {
            Ok(new_close_region_reply(id))
        });

        let mut ctx = env.create_context(persistent_context.clone());
        // Datanode 1 returns unexpected reply.
        // Datanode 2 is unreachable.
        let err = EnterStagingRegion
            .enter_staging_regions(&mut ctx)
            .await
            .unwrap_err();
        assert_matches!(err, Error::Unexpected { .. });
        assert!(!err.is_retryable());

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(2), tx)
            .await;
        // Sends an incorrect reply.
        send_mock_reply(env.mailbox_ctx.mailbox().clone(), rx, |id| {
            Ok(new_close_region_reply(id))
        });
        let mut ctx = env.create_context(persistent_context);
        // Datanode 1 returns unexpected reply.
        // Datanode 2 returns unexpected reply.
        let err = EnterStagingRegion
            .enter_staging_regions(&mut ctx)
            .await
            .unwrap_err();
        assert_matches!(err, Error::Unexpected { .. });
        assert!(!err.is_retryable());
    }
}
