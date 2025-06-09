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
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_error::ext::BoxedError;
use common_meta::distributed_time_constants::REGION_LEASE_SECS;
use common_meta::instruction::{
    DowngradeRegion, DowngradeRegionReply, Instruction, InstructionReply,
};
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::{error, info, warn};
use common_time::util::current_time_millis;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use tokio::time::{sleep, Instant};

use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::lease::find_datanode_lease_value;
use crate::procedure::region_migration::update_metadata::UpdateMetadata;
use crate::procedure::region_migration::upgrade_candidate_region::UpgradeCandidateRegion;
use crate::procedure::region_migration::{Context, State};
use crate::service::mailbox::Channel;

#[derive(Debug, Serialize, Deserialize)]
pub struct DowngradeLeaderRegion {
    // The optimistic retry times.
    optimistic_retry: usize,
    // The retry initial interval.
    retry_initial_interval: Duration,
}

impl Default for DowngradeLeaderRegion {
    fn default() -> Self {
        Self {
            optimistic_retry: 3,
            retry_initial_interval: Duration::from_millis(500),
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for DowngradeLeaderRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let now = Instant::now();
        // Ensures the `leader_region_lease_deadline` must exist after recovering.
        ctx.volatile_ctx
            .set_leader_region_lease_deadline(Duration::from_secs(REGION_LEASE_SECS));

        match self.downgrade_region_with_retry(ctx).await {
            Ok(_) => {
                // Do nothing
                info!(
                    "Downgraded region leader success, region: {}",
                    ctx.persistent_ctx.region_id
                );
            }
            Err(error::Error::ExceededDeadline { .. }) => {
                info!(
                    "Downgrade region leader exceeded deadline, region: {}",
                    ctx.persistent_ctx.region_id
                );
                // Rollbacks the metadata if procedure is timeout
                return Ok((Box::new(UpdateMetadata::Rollback), Status::executing(false)));
            }
            Err(err) => {
                error!(err; "Occurs non-retryable error, region: {}", ctx.persistent_ctx.region_id);
                if let Some(deadline) = ctx.volatile_ctx.leader_region_lease_deadline.as_ref() {
                    info!(
                        "Running into the downgrade region leader slow path, region: {}, sleep until {:?}",
                        ctx.persistent_ctx.region_id, deadline
                    );
                    tokio::time::sleep_until(*deadline).await;
                } else {
                    warn!(
                        "Leader region lease deadline is not set, region: {}",
                        ctx.persistent_ctx.region_id
                    );
                }
            }
        }
        ctx.update_downgrade_leader_region_elapsed(now);

        Ok((
            Box::new(UpgradeCandidateRegion::default()),
            Status::executing(false),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl DowngradeLeaderRegion {
    /// Builds downgrade region instruction.
    fn build_downgrade_region_instruction(
        &self,
        ctx: &Context,
        flush_timeout: Duration,
    ) -> Instruction {
        let pc = &ctx.persistent_ctx;
        let region_id = pc.region_id;
        Instruction::DowngradeRegion(DowngradeRegion {
            region_id,
            flush_timeout: Some(flush_timeout),
        })
    }

    /// Tries to downgrade a leader region.
    ///
    /// Retry:
    /// - [MailboxTimeout](error::Error::MailboxTimeout), Timeout.
    /// - Failed to downgrade region on the Datanode.
    ///
    /// Abort:
    /// - [PusherNotFound](error::Error::PusherNotFound), The datanode is unreachable.
    /// - [PushMessage](error::Error::PushMessage), The receiver is dropped.
    /// - [MailboxReceiver](error::Error::MailboxReceiver), The sender is dropped without sending (impossible).
    /// - [UnexpectedInstructionReply](error::Error::UnexpectedInstructionReply).
    /// - [ExceededDeadline](error::Error::ExceededDeadline)
    /// - Invalid JSON.
    async fn downgrade_region(&self, ctx: &mut Context) -> Result<()> {
        let region_id = ctx.persistent_ctx.region_id;
        let operation_timeout =
            ctx.next_operation_timeout()
                .context(error::ExceededDeadlineSnafu {
                    operation: "Downgrade region",
                })?;
        let downgrade_instruction = self.build_downgrade_region_instruction(ctx, operation_timeout);

        let leader = &ctx.persistent_ctx.from_peer;
        let msg = MailboxMessage::json_message(
            &format!("Downgrade leader region: {}", region_id),
            &format!("Metasrv@{}", ctx.server_addr()),
            &format!("Datanode-{}@{}", leader.id, leader.addr),
            common_time::util::current_time_millis(),
            &downgrade_instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: downgrade_instruction.to_string(),
        })?;

        let ch = Channel::Datanode(leader.id);
        let now = Instant::now();
        let receiver = ctx.mailbox.send(&ch, msg, operation_timeout).await?;

        match receiver.await {
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                info!(
                    "Received downgrade region reply: {:?}, region: {}, elapsed: {:?}",
                    reply,
                    region_id,
                    now.elapsed()
                );
                let InstructionReply::DowngradeRegion(DowngradeRegionReply {
                    last_entry_id,
                    metadata_last_entry_id,
                    exists,
                    error,
                }) = reply
                else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect downgrade region reply",
                    }
                    .fail();
                };

                if error.is_some() {
                    return error::RetryLaterSnafu {
                        reason: format!(
                            "Failed to downgrade the region {} on datanode {:?}, error: {:?}, elapsed: {:?}",
                            region_id, leader, error, now.elapsed()
                        ),
                    }
                    .fail();
                }

                if !exists {
                    warn!(
                        "Trying to downgrade the region {} on datanode {:?}, but region doesn't exist!, elapsed: {:?}",
                        region_id, leader, now.elapsed()
                    );
                } else {
                    info!(
                        "Region {} leader is downgraded on datanode {:?}, last_entry_id: {:?}, metadata_last_entry_id: {:?}, elapsed: {:?}",
                        region_id,
                        leader,
                        last_entry_id,
                        metadata_last_entry_id,
                        now.elapsed()
                    );
                }

                if let Some(last_entry_id) = last_entry_id {
                    ctx.volatile_ctx.set_last_entry_id(last_entry_id);
                }

                if let Some(metadata_last_entry_id) = metadata_last_entry_id {
                    ctx.volatile_ctx
                        .set_metadata_last_entry_id(metadata_last_entry_id);
                }

                Ok(())
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for downgrade leader region {region_id} on datanode {:?}, elapsed: {:?}", 
                    leader,
                    now.elapsed()
                );
                error::RetryLaterSnafu { reason }.fail()
            }
            Err(err) => Err(err),
        }
    }

    async fn update_leader_region_lease_deadline(&self, ctx: &mut Context) {
        let leader = &ctx.persistent_ctx.from_peer;

        let last_connection_at = match find_datanode_lease_value(leader.id, &ctx.in_memory).await {
            Ok(lease_value) => lease_value.map(|lease_value| lease_value.timestamp_millis),
            Err(err) => {
                error!(err; "Failed to find datanode lease value for datanode: {}, during region migration, region: {}", leader, ctx.persistent_ctx.region_id);
                return;
            }
        };

        if let Some(last_connection_at) = last_connection_at {
            let now = current_time_millis();
            let elapsed = now - last_connection_at;
            let region_lease = Duration::from_secs(REGION_LEASE_SECS);

            // It's safe to update the region leader lease deadline here because:
            // 1. The old region leader has already been marked as downgraded in metadata,
            //    which means any attempts to renew its lease will be rejected.
            // 2. The pusher disconnect time record only gets removed when the datanode (from_peer)
            //    establishes a new heartbeat connection stream.
            if elapsed >= (REGION_LEASE_SECS * 1000) as i64 {
                ctx.volatile_ctx.reset_leader_region_lease_deadline();
                info!(
                    "Datanode {}({}) has been disconnected for longer than the region lease period ({:?}), reset leader region lease deadline to None, region: {}", 
                    leader,
                    last_connection_at,
                    region_lease,
                    ctx.persistent_ctx.region_id
                );
            } else if elapsed > 0 {
                // `now - last_connection_at` < REGION_LEASE_SECS * 1000
                let lease_timeout =
                    region_lease - Duration::from_millis((now - last_connection_at) as u64);
                ctx.volatile_ctx.reset_leader_region_lease_deadline();
                ctx.volatile_ctx
                    .set_leader_region_lease_deadline(lease_timeout);
                info!(
                    "Datanode {}({}) last connected {:?} ago, updated leader region lease deadline to {:?}, region: {}",
                    leader, last_connection_at, elapsed, ctx.volatile_ctx.leader_region_lease_deadline, ctx.persistent_ctx.region_id
                );
            } else {
                warn!(
                    "Datanode {} has invalid last connection timestamp: {} (which is after current time: {}), region: {}",
                    leader, last_connection_at, now, ctx.persistent_ctx.region_id
                )
            }
        } else {
            warn!(
                "Failed to find last connection time for datanode {}, unable to update region lease deadline, region: {}",
                leader, ctx.persistent_ctx.region_id
            )
        }
    }

    /// Downgrades a leader region.
    ///
    /// Fast path:
    /// - Waits for the reply of downgrade instruction.
    ///
    /// Slow path:
    /// - Waits for the lease of the leader region expired.
    ///
    /// Abort:
    /// - ExceededDeadline
    async fn downgrade_region_with_retry(&self, ctx: &mut Context) -> Result<()> {
        let mut retry = 0;

        loop {
            let timer = Instant::now();
            if let Err(err) = self.downgrade_region(ctx).await {
                ctx.update_operations_elapsed(timer);
                retry += 1;
                // Throws the error immediately if the procedure exceeded the deadline.
                if matches!(err, error::Error::ExceededDeadline { .. }) {
                    error!(err; "Failed to downgrade region leader, region: {}, exceeded deadline", ctx.persistent_ctx.region_id);
                    return Err(err);
                } else if matches!(err, error::Error::PusherNotFound { .. }) {
                    // Throws the error immediately if the datanode is unreachable.
                    error!(err; "Failed to downgrade region leader, region: {}, datanode({}) is unreachable(PusherNotFound)", ctx.persistent_ctx.region_id, ctx.persistent_ctx.from_peer.id);
                    self.update_leader_region_lease_deadline(ctx).await;
                    return Err(err);
                } else if err.is_retryable() && retry < self.optimistic_retry {
                    error!(err; "Failed to downgrade region leader, region: {}, retry later", ctx.persistent_ctx.region_id);
                    sleep(self.retry_initial_interval).await;
                } else {
                    return Err(BoxedError::new(err)).context(error::DowngradeLeaderSnafu {
                        region_id: ctx.persistent_ctx.region_id,
                    })?;
                }
            } else {
                ctx.update_operations_elapsed(timer);
                // Resets the deadline.
                ctx.volatile_ctx.reset_leader_region_lease_deadline();
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::RegionId;
    use tokio::time::Instant;

    use super::*;
    use crate::error::Error;
    use crate::procedure::region_migration::test_util::{new_procedure_context, TestingEnv};
    use crate::procedure::region_migration::{ContextFactory, PersistentContext};
    use crate::procedure::test_util::{
        new_close_region_reply, new_downgrade_region_reply, send_mock_reply,
    };

    fn new_persistent_context() -> PersistentContext {
        PersistentContext {
            catalog: "greptime".into(),
            schema: "public".into(),
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            region_id: RegionId::new(1024, 1),
            timeout: Duration::from_millis(1000),
        }
    }

    async fn prepare_table_metadata(ctx: &Context, wal_options: HashMap<u32, String>) {
        let table_info =
            new_test_table_info(ctx.persistent_ctx.region_id.table_id(), vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(ctx.persistent_ctx.region_id),
            leader_peer: Some(ctx.persistent_ctx.from_peer.clone()),
            follower_peers: vec![ctx.persistent_ctx.to_peer.clone()],
            ..Default::default()
        }];
        ctx.table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(region_routes),
                wal_options,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_datanode_is_unreachable() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::PusherNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_pusher_dropped() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        drop(rx);

        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::PushMessage { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_procedure_exceeded_deadline() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        ctx.volatile_ctx.metrics.operations_elapsed =
            ctx.persistent_ctx.timeout + Duration::from_secs(1);

        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::ExceededDeadline { .. });
        assert!(!err.is_retryable());

        let err = state
            .downgrade_region_with_retry(&mut ctx)
            .await
            .unwrap_err();
        assert_matches!(err, Error::ExceededDeadline { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_unexpected_instruction_reply() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        // Sends an incorrect reply.
        send_mock_reply(mailbox, rx, |id| Ok(new_close_region_reply(id)));

        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::UnexpectedInstructionReply { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_instruction_exceeded_deadline() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Err(error::MailboxTimeoutSnafu { id }.build())
        });

        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_downgrade_region_failed() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_downgrade_region_reply(
                id,
                None,
                false,
                Some("test mocked".to_string()),
            ))
        });

        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
        assert!(format!("{err:?}").contains("test mocked"), "err: {err:?}",);
    }

    #[tokio::test]
    async fn test_downgrade_region_with_retry_fast_path() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        common_runtime::spawn_global(async move {
            // retry: 0.
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Err(error::MailboxTimeoutSnafu { id: reply_id }.build()),
                )
                .await
                .unwrap();

            // retry: 1.
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Ok(new_downgrade_region_reply(reply_id, Some(1), true, None)),
                )
                .await
                .unwrap();
        });

        state.downgrade_region_with_retry(&mut ctx).await.unwrap();
        assert_eq!(ctx.volatile_ctx.leader_region_last_entry_id, Some(1));
        assert!(ctx.volatile_ctx.leader_region_lease_deadline.is_none());
    }

    #[tokio::test]
    async fn test_downgrade_region_with_retry_slow_path() {
        let state = DowngradeLeaderRegion {
            optimistic_retry: 3,
            retry_initial_interval: Duration::from_millis(100),
        };
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        common_runtime::spawn_global(async move {
            for _ in 0..3 {
                let resp = rx.recv().await.unwrap().unwrap();
                let reply_id = resp.mailbox_message.unwrap().id;
                mailbox
                    .on_recv(
                        reply_id,
                        Err(error::MailboxTimeoutSnafu { id: reply_id }.build()),
                    )
                    .await
                    .unwrap();
            }
        });

        ctx.volatile_ctx
            .set_leader_region_lease_deadline(Duration::from_secs(5));
        let expected_deadline = ctx.volatile_ctx.leader_region_lease_deadline.unwrap();
        let err = state
            .downgrade_region_with_retry(&mut ctx)
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::DowngradeLeader { .. });
        assert_eq!(ctx.volatile_ctx.leader_region_last_entry_id, None);
        // Should remain no change.
        assert_eq!(
            ctx.volatile_ctx.leader_region_lease_deadline.unwrap(),
            expected_deadline
        )
    }

    #[tokio::test]
    async fn test_next_upgrade_candidate_state() {
        let mut state = Box::<DowngradeLeaderRegion>::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_downgrade_region_reply(id, Some(1), true, None))
        });

        let timer = Instant::now();
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();
        let elapsed = timer.elapsed().as_secs();
        assert!(elapsed < REGION_LEASE_SECS / 2);
        assert_eq!(ctx.volatile_ctx.leader_region_last_entry_id, Some(1));
        assert!(ctx.volatile_ctx.leader_region_lease_deadline.is_none());

        let _ = next
            .as_any()
            .downcast_ref::<UpgradeCandidateRegion>()
            .unwrap();
    }

    #[tokio::test]
    async fn test_downgrade_region_procedure_exceeded_deadline() {
        let mut state = Box::<UpgradeCandidateRegion>::default();
        state.retry_initial_interval = Duration::from_millis(100);
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();
        ctx.volatile_ctx.metrics.operations_elapsed =
            ctx.persistent_ctx.timeout + Duration::from_secs(1);

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_downgrade_region_reply(id, None, true, None))
        });
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();
        let update_metadata = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();
        assert_matches!(update_metadata, UpdateMetadata::Rollback);
    }
}
