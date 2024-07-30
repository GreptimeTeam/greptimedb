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
use common_meta::distributed_time_constants::{MAILBOX_RTT_SECS, REGION_LEASE_SECS};
use common_meta::instruction::{
    DowngradeRegion, DowngradeRegionReply, Instruction, InstructionReply,
};
use common_procedure::Status;
use common_telemetry::{info, warn};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio::time::sleep;

use super::upgrade_candidate_region::UpgradeCandidateRegion;
use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::region_migration::{Context, State};
use crate::service::mailbox::Channel;

const DOWNGRADE_LEADER_REGION_TIMEOUT: Duration = Duration::from_secs(MAILBOX_RTT_SECS);

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
    async fn next(&mut self, ctx: &mut Context) -> Result<(Box<dyn State>, Status)> {
        let replay_timeout = ctx.persistent_ctx.replay_timeout;
        // Ensures the `leader_region_lease_deadline` must exist after recovering.
        ctx.volatile_ctx
            .set_leader_region_lease_deadline(Duration::from_secs(REGION_LEASE_SECS));
        self.downgrade_region_with_retry(ctx).await;

        if let Some(deadline) = ctx.volatile_ctx.leader_region_lease_deadline.as_ref() {
            info!(
                "Running into the downgrade leader slow path, sleep until {:?}",
                deadline
            );
            tokio::time::sleep_until(*deadline).await;
        }

        Ok((
            Box::new(UpgradeCandidateRegion {
                replay_timeout,
                ..Default::default()
            }),
            Status::executing(false),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl DowngradeLeaderRegion {
    /// Builds downgrade region instruction.
    fn build_downgrade_region_instruction(&self, ctx: &Context) -> Instruction {
        let pc = &ctx.persistent_ctx;
        let region_id = pc.region_id;
        Instruction::DowngradeRegion(DowngradeRegion { region_id })
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
    /// - Invalid JSON.
    async fn downgrade_region(
        &self,
        ctx: &mut Context,
        downgrade_instruction: &Instruction,
    ) -> Result<()> {
        let pc = &ctx.persistent_ctx;
        let region_id = pc.region_id;
        let leader = &pc.from_peer;

        let msg = MailboxMessage::json_message(
            &format!("Downgrade leader region: {}", region_id),
            &format!("Meta@{}", ctx.server_addr()),
            &format!("Datanode-{}@{}", leader.id, leader.addr),
            common_time::util::current_time_millis(),
            downgrade_instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: downgrade_instruction.to_string(),
        })?;

        let ch = Channel::Datanode(leader.id);
        let receiver = ctx
            .mailbox
            .send(&ch, msg, DOWNGRADE_LEADER_REGION_TIMEOUT)
            .await?;

        match receiver.await? {
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                let InstructionReply::DowngradeRegion(DowngradeRegionReply {
                    last_entry_id,
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
                            "Failed to downgrade the region {} on Datanode {:?}, error: {:?}",
                            region_id, leader, error
                        ),
                    }
                    .fail();
                }

                if !exists {
                    warn!(
                        "Trying to downgrade the region {} on Datanode {}, but region doesn't exist!",
                        region_id, leader
                    );
                }

                if let Some(last_entry_id) = last_entry_id {
                    ctx.volatile_ctx.set_last_entry_id(last_entry_id);
                }

                Ok(())
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for downgrade leader region {region_id} on datanode {:?}", 
                    leader,
                );
                error::RetryLaterSnafu { reason }.fail()
            }
            Err(err) => Err(err),
        }
    }

    /// Downgrades a leader region.
    ///
    /// Fast path:
    /// - Waits for the reply of downgrade instruction.
    ///
    /// Slow path:
    /// - Waits for the lease of the leader region expired.
    async fn downgrade_region_with_retry(&self, ctx: &mut Context) {
        let instruction = self.build_downgrade_region_instruction(ctx);

        let mut retry = 0;

        loop {
            if let Err(err) = self.downgrade_region(ctx, &instruction).await {
                retry += 1;
                if err.is_retryable() && retry < self.optimistic_retry {
                    warn!("Failed to downgrade region, error: {err:?}, retry later");
                    sleep(self.retry_initial_interval).await;
                } else {
                    break;
                }
            } else {
                // Resets the deadline.
                ctx.volatile_ctx.reset_leader_region_lease_deadline();
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_meta::peer::Peer;
    use store_api::storage::RegionId;
    use tokio::time::Instant;

    use super::*;
    use crate::error::Error;
    use crate::procedure::region_migration::test_util::{
        new_close_region_reply, new_downgrade_region_reply, send_mock_reply, TestingEnv,
    };
    use crate::procedure::region_migration::{ContextFactory, PersistentContext};

    fn new_persistent_context() -> PersistentContext {
        PersistentContext {
            catalog: "greptime".into(),
            schema: "public".into(),
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            region_id: RegionId::new(1024, 1),
            cluster_id: 0,
            replay_timeout: Duration::from_millis(1000),
        }
    }

    #[tokio::test]
    async fn test_datanode_is_unreachable() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let instruction = &state.build_downgrade_region_instruction(&ctx);
        let err = state
            .downgrade_region(&mut ctx, instruction)
            .await
            .unwrap_err();

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
        let mailbox_ctx = env.mailbox_context();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        drop(rx);

        let instruction = &state.build_downgrade_region_instruction(&ctx);
        let err = state
            .downgrade_region(&mut ctx, instruction)
            .await
            .unwrap_err();

        assert_matches!(err, Error::PushMessage { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_unexpected_instruction_reply() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        // Sends an incorrect reply.
        send_mock_reply(mailbox, rx, |id| Ok(new_close_region_reply(id)));

        let instruction = &state.build_downgrade_region_instruction(&ctx);
        let err = state
            .downgrade_region(&mut ctx, instruction)
            .await
            .unwrap_err();

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
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Err(error::MailboxTimeoutSnafu { id }.build())
        });

        let instruction = &state.build_downgrade_region_instruction(&ctx);
        let err = state
            .downgrade_region(&mut ctx, instruction)
            .await
            .unwrap_err();

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

        let instruction = &state.build_downgrade_region_instruction(&ctx);
        let err = state
            .downgrade_region(&mut ctx, instruction)
            .await
            .unwrap_err();

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

        state.downgrade_region_with_retry(&mut ctx).await;
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
        state.downgrade_region_with_retry(&mut ctx).await;
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
        let (next, _) = state.next(&mut ctx).await.unwrap();
        let elapsed = timer.elapsed().as_secs();
        assert!(elapsed < REGION_LEASE_SECS / 2);
        assert_eq!(ctx.volatile_ctx.leader_region_last_entry_id, Some(1));
        assert!(ctx.volatile_ctx.leader_region_lease_deadline.is_none());

        let _ = next
            .as_any()
            .downcast_ref::<UpgradeCandidateRegion>()
            .unwrap();
    }
}
