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
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use api::v1::meta::MailboxMessage;
use common_meta::instruction::{
    Instruction, InstructionReply, MetadataCompaction, MetadataSyncAttempt, SyncRegionReply,
    SyncRegionsReply,
};
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{info, warn};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::region_engine::SyncRegionFromRequest;
use store_api::region_request::RegionFlushReason;
use store_api::storage::RegionId;

use crate::error::{self, Error, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
use crate::procedure::repartition::group::utils::{
    HandleMultipleResult, group_region_routes_by_peer, handle_multiple_results,
};
use crate::procedure::repartition::group::{Context, State};
use crate::procedure::utils::{ErrorStrategy, instruction_error_result};
use crate::service::mailbox::{Channel, MailboxRef};

const DEFAULT_SYNC_REGION_PARALLELISM: usize = 3;

/// The state of syncing regions for a repartition group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRegion {
    pub region_routes: Vec<RegionRoute>,
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for SyncRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        Self::flush_central_region(ctx).await?;
        self.sync_with_paused_source(ctx).await?;

        Ok((
            Box::new(UpdateMetadata::ApplyStaging),
            Status::executing(true),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl SyncRegion {
    async fn flush_central_region(ctx: &mut Context) -> Result<()> {
        let operation_timeout =
            ctx.next_operation_timeout()
                .context(error::ExceededDeadlineSnafu {
                    operation: "Flush central region",
                })?;
        let prepare_result = ctx.persistent_ctx.group_prepare_result.as_ref().unwrap();

        crate::procedure::utils::flush_region(
            &ctx.mailbox,
            &ctx.server_addr,
            &[prepare_result.central_region],
            &prepare_result.central_region_datanode,
            operation_timeout,
            ErrorStrategy::Retry,
            Some(RegionFlushReason::Repartition),
        )
        .await
    }

    async fn sync_with_paused_source(&self, ctx: &mut Context) -> Result<()> {
        let prepare = ctx.persistent_ctx.group_prepare_result.as_ref().unwrap();
        let source = prepare.central_region_datanode.clone();
        let attempt = MetadataSyncAttempt {
            region_id: prepare.central_region,
            attempt_id: uuid::Uuid::new_v4(),
        };
        let remaining = ctx
            .next_operation_timeout()
            .context(error::ExceededDeadlineSnafu {
                operation: "Pause source metadata compaction",
            })?;
        let pause =
            Instruction::MetadataCompaction(MetadataCompaction::Pause { attempt, remaining });
        let reply =
            Self::request(&ctx.mailbox, &ctx.server_addr, &source, pause, remaining).await?;
        let InstructionReply::MetadataCompaction(reply) = reply else {
            return error::UnexpectedInstructionReplySnafu {
                mailbox_message: reply.to_string(),
                reason: "expected source metadata compaction pause acknowledgement",
            }
            .fail();
        };
        if let Some(err) = reply.error {
            return instruction_error_result(
                &err,
                format!(
                    "Failed to pause compaction for {}: {}",
                    attempt.region_id, err.message
                ),
            );
        }
        ensure!(
            reply.result,
            error::RetryLaterSnafu {
                reason: format!(
                    "Source {} did not confirm metadata compaction pause",
                    attempt.region_id
                )
            }
        );

        // A lost reply may leave a target copying. The source releases its guard
        // at the deadline if we cannot confirm that every target finished.
        let mut all_replied = true;
        let result = self.sync_regions(ctx, &mut all_replied).await;
        if all_replied {
            let release = Instruction::MetadataCompaction(MetadataCompaction::Release(attempt));
            if let Err(err) = Self::release_source(ctx, &source, release).await {
                warn!(err; "Failed to release source metadata compaction for {attempt:?}");
            }
        }
        result
    }

    async fn release_source(ctx: &Context, source: &Peer, release: Instruction) -> Result<()> {
        let msg = Self::message(&ctx.server_addr, source, &release)?;
        let channel = Channel::Datanode(source.id);
        tokio::time::timeout(
            ctx.next_operation_timeout().unwrap_or_default(),
            ctx.mailbox.send_oneway(&channel, msg),
        )
        .await
        .ok()
        .context(error::ExceededDeadlineSnafu {
            operation: "Release source metadata compaction",
        })?
    }

    /// Builds instructions to sync regions on datanodes.
    fn build_sync_region_instructions(
        central_region: RegionId,
        region_routes: &[RegionRoute],
    ) -> HashMap<Peer, Vec<common_meta::instruction::SyncRegion>> {
        let target_region_routes_by_peer = group_region_routes_by_peer(region_routes);
        let mut instructions = HashMap::with_capacity(target_region_routes_by_peer.len());

        for (peer, region_ids) in target_region_routes_by_peer {
            let sync_regions = region_ids
                .into_iter()
                .map(|region_id| {
                    let request = SyncRegionFromRequest::FromRegion {
                        source_region_id: central_region,
                        parallelism: DEFAULT_SYNC_REGION_PARALLELISM,
                    };
                    common_meta::instruction::SyncRegion { region_id, request }
                })
                .collect();
            instructions.insert((*peer).clone(), sync_regions);
        }

        instructions
    }

    /// Syncs regions on datanodes.
    async fn sync_regions(&self, ctx: &mut Context, all_replied: &mut bool) -> Result<()> {
        let table_id = ctx.persistent_ctx.table_id;
        let prepare_result = ctx.persistent_ctx.group_prepare_result.as_ref().unwrap();
        let instructions = Self::build_sync_region_instructions(
            prepare_result.central_region,
            &self.region_routes,
        );
        let operation_timeout =
            ctx.next_operation_timeout()
                .context(error::ExceededDeadlineSnafu {
                    operation: "Sync regions",
                })?;

        let (peers, tasks): (Vec<_>, Vec<_>) = instructions
            .iter()
            .map(|(peer, sync_regions)| {
                (
                    peer,
                    Self::request(
                        &ctx.mailbox,
                        &ctx.server_addr,
                        peer,
                        Instruction::SyncRegions(sync_regions.clone()),
                        operation_timeout,
                    ),
                )
            })
            .unzip();

        info!(
            "Sent sync regions instructions to peers: {:?} for repartition table {}",
            peers, table_id
        );

        let format_err_msg = |idx: usize, error: &Error| {
            let peer = peers[idx];
            format!(
                "Failed to sync regions on datanode {:?}, error: {:?}",
                peer, error
            )
        };

        let now = Instant::now();
        let replies = join_all(tasks).await;
        let results = replies
            .into_iter()
            .zip(&peers)
            .map(|(reply, peer)| match reply {
                Ok(InstructionReply::SyncRegions(SyncRegionsReply { replies }))
                    if Self::received_all(&instructions[*peer], &replies) =>
                {
                    replies
                        .iter()
                        .try_for_each(|reply| Self::handle_sync_region_reply(reply, &now, peer))
                }
                Ok(reply) => {
                    *all_replied = false;
                    error::UnexpectedInstructionReplySnafu {
                        mailbox_message: reply.to_string(),
                        reason: "expected replies from all requested sync regions",
                    }
                    .fail()
                }
                Err(err) => {
                    *all_replied = false;
                    Err(err)
                }
            })
            .collect::<Vec<_>>();
        let result = handle_multiple_results(&results);

        match result {
            HandleMultipleResult::AllSuccessful => Ok(()),
            HandleMultipleResult::AllRetryable(retryable_errors) => error::RetryLaterSnafu {
                reason: format!(
                    "All retryable errors during syncing regions for repartition table {}: {:?}",
                    table_id,
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
                    "All non retryable errors during syncing regions for repartition table {}: {:?}",
                    table_id,
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
                    "Partial retryable errors during syncing regions for repartition table {}: {:?}, non retryable errors: {:?}",
                    table_id,
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

    fn message(
        server_addr: &str,
        peer: &Peer,
        instruction: &Instruction,
    ) -> Result<MailboxMessage> {
        MailboxMessage::json_message(
            &instruction.to_string(),
            &format!("Metasrv@{server_addr}"),
            &format!("Datanode-{}@{}", peer.id, peer.addr),
            common_time::util::current_time_millis(),
            instruction,
            Some(TracingContext::from_current_span().to_w3c()),
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })
    }

    async fn request(
        mailbox: &MailboxRef,
        server_addr: &str,
        peer: &Peer,
        instruction: Instruction,
        timeout: Duration,
    ) -> Result<InstructionReply> {
        let ch = Channel::Datanode(peer.id);
        let message = Self::message(server_addr, peer, &instruction)?;
        let now = std::time::Instant::now();
        let receiver = mailbox.send(&ch, message, timeout).await;

        let receiver = match receiver {
            Ok(receiver) => receiver,
            Err(error::Error::PusherNotFound { .. }) => error::RetryLaterSnafu {
                reason: format!(
                    "Pusher not found for sync regions on datanode {:?}, elapsed: {:?}",
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
            Ok(msg) => HeartbeatMailbox::json_reply(&msg),
            Err(error::Error::MailboxChannelClosed { .. }) => error::RetryLaterSnafu {
                reason: format!(
                    "Mailbox closed when sending sync region to datanode {:?}, elapsed: {:?}",
                    peer,
                    now.elapsed()
                ),
            }
            .fail()?,
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for sync regions on datanode {:?}, elapsed: {:?}",
                    peer,
                    now.elapsed()
                );
                error::RetryLaterSnafu { reason }.fail()
            }
            Err(err) => Err(err),
        }
    }

    fn received_all(
        requests: &[common_meta::instruction::SyncRegion],
        replies: &[SyncRegionReply],
    ) -> bool {
        if requests.len() != replies.len() {
            return false;
        }
        let ids: HashSet<_> = replies.iter().map(|reply| reply.region_id).collect();
        requests
            .iter()
            .all(|request| ids.contains(&request.region_id))
    }

    fn handle_sync_region_reply(
        SyncRegionReply {
            region_id,
            ready,
            exists,
            error,
        }: &SyncRegionReply,
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

        if let Some(error) = error {
            return instruction_error_result(
                error,
                format!(
                    "Failed to sync region {} on datanode {:?}, error: {:?}, elapsed: {:?}",
                    region_id,
                    peer,
                    error,
                    now.elapsed()
                ),
            );
        }

        ensure!(
            ready,
            error::RetryLaterSnafu {
                reason: format!(
                    "Region {} failed to sync on datanode {:?}, elapsed: {:?}",
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
    use std::assert_matches;

    use common_error::ext::RetryHint;
    use common_error::status_code::StatusCode;
    use common_meta::instruction::InstructionError;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::region_engine::SyncRegionFromRequest;
    use store_api::storage::RegionId;

    use crate::error::Error;
    use crate::procedure::repartition::group::GroupPrepareResult;
    use crate::procedure::repartition::group::sync_region::SyncRegion;
    use crate::procedure::repartition::test_util::{TestingEnv, new_persistent_context};
    use crate::procedure::test_util::{
        new_sync_region_reply, new_sync_region_reply_with_error, send_mock_reply,
    };
    use crate::service::mailbox::Channel;

    #[test]
    fn test_build_sync_region_instructions() {
        let table_id = 1024;
        let central_region = RegionId::new(table_id, 1);
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(table_id, 3),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];

        let instructions =
            SyncRegion::build_sync_region_instructions(central_region, &region_routes);
        assert_eq!(instructions.len(), 1);
        let peer_instructions = instructions.get(&Peer::empty(1)).unwrap();
        assert_eq!(peer_instructions.len(), 1);
        assert_eq!(peer_instructions[0].region_id, RegionId::new(table_id, 3));
        let SyncRegionFromRequest::FromRegion {
            source_region_id, ..
        } = &peer_instructions[0].request
        else {
            panic!("expect from region request");
        };
        assert_eq!(*source_region_id, central_region);
    }

    fn test_prepare_result(table_id: u32) -> GroupPrepareResult {
        GroupPrepareResult {
            source_routes: vec![],
            target_routes: vec![],
            central_region: RegionId::new(table_id, 1),
            central_region_datanode: Peer::empty(1),
        }
    }

    #[tokio::test]
    async fn test_source_pause_release_follows_target_reply() {
        use std::time::Duration;

        use crate::handler::HeartbeatMailbox;
        use common_meta::instruction::{
            Instruction, InstructionReply, MetadataCompaction, SimpleReply,
        };

        for outcome in ["success", "region_error", "lost_reply", "pause_rejected"] {
            let mut env = TestingEnv::new();
            let mut persistent = new_persistent_context(1024, vec![], vec![]);
            persistent.group_prepare_result = Some(test_prepare_result(1024));
            let budget = persistent.timeout;
            let (source_tx, mut source_rx) = tokio::sync::mpsc::channel(2);
            let (target_tx, mut target_rx) = tokio::sync::mpsc::channel(1);
            env.mailbox_ctx
                .insert_heartbeat_response_receiver(Channel::Datanode(1), source_tx)
                .await;
            env.mailbox_ctx
                .insert_heartbeat_response_receiver(Channel::Datanode(2), target_tx)
                .await;
            let mut ctx = env.create_context(persistent);
            let mailbox = ctx.mailbox.clone();
            let target = RegionId::new(1024, 3);
            let sync = SyncRegion {
                region_routes: vec![RegionRoute {
                    region: Region {
                        id: target,
                        ..Default::default()
                    },
                    leader_peer: Some(Peer::empty(2)),
                    ..Default::default()
                }],
            };
            let peers = async {
                let msg = source_rx
                    .recv()
                    .await
                    .unwrap()
                    .unwrap()
                    .mailbox_message
                    .unwrap();
                let Instruction::MetadataCompaction(MetadataCompaction::Pause {
                    attempt,
                    remaining,
                }) = HeartbeatMailbox::json_instruction(&msg).unwrap()
                else {
                    panic!("expected pause")
                };
                assert_eq!(attempt.region_id, RegionId::new(1024, 1));
                assert!(remaining > Duration::ZERO && remaining <= budget);
                assert!(target_rx.try_recv().is_err());
                let mut ack = msg.clone();
                ack.payload = Some(api::v1::meta::mailbox_message::Payload::Json(
                    serde_json::to_string(&InstructionReply::MetadataCompaction(SimpleReply {
                        result: outcome != "pause_rejected",
                        error: None,
                    }))
                    .unwrap(),
                ));
                mailbox.on_recv(msg.id, Ok(ack)).await.unwrap();
                if outcome == "pause_rejected" {
                    return attempt;
                }
                let msg = target_rx
                    .recv()
                    .await
                    .unwrap()
                    .unwrap()
                    .mailbox_message
                    .unwrap();
                assert!(matches!(
                    HeartbeatMailbox::json_instruction(&msg).unwrap(),
                    Instruction::SyncRegions(_)
                ));
                assert!(source_rx.try_recv().is_err());
                let reply = match outcome {
                    "lost_reply" => crate::error::MailboxTimeoutSnafu { id: msg.id }.fail(),
                    "region_error" => Ok(new_sync_region_reply(
                        msg.id,
                        target,
                        false,
                        true,
                        Some("sync failed".into()),
                    )),
                    _ => Ok(new_sync_region_reply(msg.id, target, true, true, None)),
                };
                mailbox.on_recv(msg.id, reply).await.unwrap();
                attempt
            };
            let (attempt, result) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(peers, sync.sync_with_paused_source(&mut ctx))
            })
            .await
            .unwrap();
            assert_eq!(result.is_ok(), outcome == "success");
            if outcome == "pause_rejected" {
                assert!(target_rx.try_recv().is_err());
            }
            if matches!(outcome, "lost_reply" | "pause_rejected") {
                assert!(source_rx.try_recv().is_err());
            } else {
                let msg = source_rx
                    .try_recv()
                    .unwrap()
                    .unwrap()
                    .mailbox_message
                    .unwrap();
                assert_eq!(
                    HeartbeatMailbox::json_instruction(&msg).unwrap(),
                    Instruction::MetadataCompaction(MetadataCompaction::Release(attempt))
                );
            }
        }
    }

    #[tokio::test]
    async fn test_sync_regions_all_successful() {
        let mut env = TestingEnv::new();
        let table_id = 1024;
        let mut persistent_context = new_persistent_context(table_id, vec![], vec![]);
        persistent_context.group_prepare_result = Some(test_prepare_result(table_id));

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
            .await;
        send_mock_reply(env.mailbox_ctx.mailbox().clone(), rx, |id| {
            Ok(new_sync_region_reply(
                id,
                RegionId::new(1024, 3),
                true,
                true,
                None,
            ))
        });

        let mut ctx = env.create_context(persistent_context);
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(table_id, 3),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];
        let sync_region = SyncRegion { region_routes };

        sync_region.sync_regions(&mut ctx, &mut true).await.unwrap();
    }

    #[tokio::test]
    async fn test_sync_regions_retryable() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut persistent_context = new_persistent_context(table_id, vec![], vec![]);
        persistent_context.group_prepare_result = Some(test_prepare_result(table_id));

        let mut ctx = env.create_context(persistent_context);
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(table_id, 3),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];
        let sync_region = SyncRegion { region_routes };

        let err = sync_region
            .sync_regions(&mut ctx, &mut true)
            .await
            .unwrap_err();
        assert_matches!(err, Error::RetryLater { .. });
    }

    #[tokio::test]
    async fn test_sync_regions_retryable_instruction_error() {
        let mut env = TestingEnv::new();
        let table_id = 1024;
        let region_id = RegionId::new(table_id, 3);
        let mut persistent_context = new_persistent_context(table_id, vec![], vec![]);
        persistent_context.group_prepare_result = Some(test_prepare_result(table_id));

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
            .await;
        send_mock_reply(env.mailbox_ctx.mailbox().clone(), rx, move |id| {
            Ok(new_sync_region_reply_with_error(
                id,
                region_id,
                false,
                true,
                Some(InstructionError {
                    code: StatusCode::StorageUnavailable,
                    message: "manifest delta disappeared".to_string(),
                    retry_hint: RetryHint::Retryable,
                }),
            ))
        });

        let mut ctx = env.create_context(persistent_context);
        let sync_region = SyncRegion {
            region_routes: vec![RegionRoute {
                region: Region {
                    id: region_id,
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            }],
        };

        let err = sync_region
            .sync_regions(&mut ctx, &mut true)
            .await
            .unwrap_err();
        assert_matches!(err, Error::RetryLater { .. });
    }
}
