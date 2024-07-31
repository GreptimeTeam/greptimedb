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
use common_meta::distributed_time_constants::MAILBOX_RTT_SECS;
use common_meta::instruction::{Instruction, InstructionReply, UpgradeRegion, UpgradeRegionReply};
use common_procedure::Status;
use common_telemetry::warn;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use tokio::time::sleep;

use super::update_metadata::UpdateMetadata;
use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::region_migration::{Context, State};
use crate::service::mailbox::Channel;

#[derive(Debug, Serialize, Deserialize)]
pub struct UpgradeCandidateRegion {
    // The optimistic retry times.
    pub(crate) optimistic_retry: usize,
    // The retry initial interval.
    pub(crate) retry_initial_interval: Duration,
    // The replay timeout of a instruction.
    pub(crate) replay_timeout: Duration,
    // If it's true it requires the candidate region MUST replay the WAL to the latest entry id.
    // Otherwise, it will rollback to the old leader region.
    pub(crate) require_ready: bool,
}

impl Default for UpgradeCandidateRegion {
    fn default() -> Self {
        Self {
            optimistic_retry: 3,
            retry_initial_interval: Duration::from_millis(500),
            replay_timeout: Duration::from_millis(1000),
            require_ready: true,
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for UpgradeCandidateRegion {
    async fn next(&mut self, ctx: &mut Context) -> Result<(Box<dyn State>, Status)> {
        if self.upgrade_region_with_retry(ctx).await {
            Ok((Box::new(UpdateMetadata::Upgrade), Status::executing(false)))
        } else {
            Ok((Box::new(UpdateMetadata::Rollback), Status::executing(false)))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl UpgradeCandidateRegion {
    const UPGRADE_CANDIDATE_REGION_RTT: Duration = Duration::from_secs(MAILBOX_RTT_SECS);

    /// Returns the timeout of the upgrade candidate region.
    ///
    /// Equals `replay_timeout` + RTT
    fn send_upgrade_candidate_region_timeout(&self) -> Duration {
        self.replay_timeout + UpgradeCandidateRegion::UPGRADE_CANDIDATE_REGION_RTT
    }

    /// Builds upgrade region instruction.
    fn build_upgrade_region_instruction(&self, ctx: &Context) -> Instruction {
        let pc = &ctx.persistent_ctx;
        let region_id = pc.region_id;
        let last_entry_id = ctx.volatile_ctx.leader_region_last_entry_id;

        Instruction::UpgradeRegion(UpgradeRegion {
            region_id,
            last_entry_id,
            wait_for_replay_timeout: Some(self.replay_timeout),
        })
    }

    /// Tries to upgrade a candidate region.
    ///
    /// Retry:
    /// - If `require_ready` is true, but the candidate region returns `ready` is false.
    /// - [MailboxTimeout](error::Error::MailboxTimeout), Timeout.
    ///
    /// Abort:
    /// - The candidate region doesn't exist.
    /// - [PusherNotFound](error::Error::PusherNotFound), The datanode is unreachable.
    /// - [PushMessage](error::Error::PushMessage), The receiver is dropped.
    /// - [MailboxReceiver](error::Error::MailboxReceiver), The sender is dropped without sending (impossible).
    /// - [UnexpectedInstructionReply](error::Error::UnexpectedInstructionReply) (impossible).
    /// - Invalid JSON (impossible).
    async fn upgrade_region(&self, ctx: &Context, upgrade_instruction: &Instruction) -> Result<()> {
        let pc = &ctx.persistent_ctx;
        let region_id = pc.region_id;
        let candidate = &pc.to_peer;

        let msg = MailboxMessage::json_message(
            &format!("Upgrade candidate region: {}", region_id),
            &format!("Meta@{}", ctx.server_addr()),
            &format!("Datanode-{}@{}", candidate.id, candidate.addr),
            common_time::util::current_time_millis(),
            upgrade_instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: upgrade_instruction.to_string(),
        })?;

        let ch = Channel::Datanode(candidate.id);
        let receiver = ctx
            .mailbox
            .send(&ch, msg, self.send_upgrade_candidate_region_timeout())
            .await?;

        match receiver.await? {
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                let InstructionReply::UpgradeRegion(UpgradeRegionReply {
                    ready,
                    exists,
                    error,
                }) = reply
                else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "Unexpected reply of the upgrade region instruction",
                    }
                    .fail();
                };

                // Notes: The order of handling is important.
                if error.is_some() {
                    return error::RetryLaterSnafu {
                        reason: format!(
                            "Failed to upgrade the region {} on datanode {:?}, error: {:?}",
                            region_id, candidate, error
                        ),
                    }
                    .fail();
                }

                ensure!(
                    exists,
                    error::UnexpectedSnafu {
                        violated: format!(
                            "Expected region {} doesn't exist on datanode {:?}",
                            region_id, candidate
                        )
                    }
                );

                if self.require_ready && !ready {
                    return error::RetryLaterSnafu {
                        reason: format!(
                            "Candidate region {} still replaying the wal on datanode {:?}",
                            region_id, candidate
                        ),
                    }
                    .fail();
                }

                Ok(())
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                        "Mailbox received timeout for upgrade candidate region {region_id} on datanode {:?}", 
                        candidate,
                    );
                error::RetryLaterSnafu { reason }.fail()
            }
            Err(err) => Err(err),
        }
    }

    /// Upgrades a candidate region.
    ///
    /// Returns true if the candidate region is upgraded successfully.
    async fn upgrade_region_with_retry(&self, ctx: &Context) -> bool {
        let upgrade_instruction = self.build_upgrade_region_instruction(ctx);

        let mut retry = 0;
        let mut upgraded = false;

        loop {
            if let Err(err) = self.upgrade_region(ctx, &upgrade_instruction).await {
                retry += 1;
                if err.is_retryable() && retry < self.optimistic_retry {
                    warn!("Failed to upgrade region, error: {err:?}, retry later");
                    sleep(self.retry_initial_interval).await;
                } else {
                    break;
                }
            } else {
                upgraded = true;
                break;
            }
        }

        upgraded
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_meta::peer::Peer;
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;
    use crate::procedure::region_migration::test_util::{
        new_close_region_reply, new_upgrade_region_reply, send_mock_reply, TestingEnv,
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
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let ctx = env.context_factory().new_context(persistent_context);

        let instruction = &state.build_upgrade_region_instruction(&ctx);
        let err = state.upgrade_region(&ctx, instruction).await.unwrap_err();

        assert_matches!(err, Error::PusherNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_pusher_dropped() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        drop(rx);

        let instruction = &state.build_upgrade_region_instruction(&ctx);
        let err = state.upgrade_region(&ctx, instruction).await.unwrap_err();

        assert_matches!(err, Error::PushMessage { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_unexpected_instruction_reply() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| Ok(new_close_region_reply(id)));

        let instruction = &state.build_upgrade_region_instruction(&ctx);
        let err = state.upgrade_region(&ctx, instruction).await.unwrap_err();
        assert_matches!(err, Error::UnexpectedInstructionReply { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_upgrade_region_failed() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        // A reply contains an error.
        send_mock_reply(mailbox, rx, |id| {
            Ok(new_upgrade_region_reply(
                id,
                true,
                true,
                Some("test mocked".to_string()),
            ))
        });

        let instruction = &state.build_upgrade_region_instruction(&ctx);
        let err = state.upgrade_region(&ctx, instruction).await.unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
        assert!(format!("{err:?}").contains("test mocked"));
    }

    #[tokio::test]
    async fn test_upgrade_region_not_found() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_upgrade_region_reply(id, true, false, None))
        });

        let instruction = &state.build_upgrade_region_instruction(&ctx);
        let err = state.upgrade_region(&ctx, instruction).await.unwrap_err();

        assert_matches!(err, Error::Unexpected { .. });
        assert!(!err.is_retryable());
        assert!(err.to_string().contains("doesn't exist"));
    }

    #[tokio::test]
    async fn test_upgrade_region_require_ready() {
        let mut state = UpgradeCandidateRegion {
            require_ready: true,
            ..Default::default()
        };

        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_upgrade_region_reply(id, false, true, None))
        });

        let instruction = &state.build_upgrade_region_instruction(&ctx);
        let err = state.upgrade_region(&ctx, instruction).await.unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
        assert!(format!("{err:?}").contains("still replaying the wal"));

        // Sets the `require_ready` to false.
        state.require_ready = false;

        let mailbox = mailbox_ctx.mailbox().clone();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_upgrade_region_reply(id, false, true, None))
        });

        let instruction = &state.build_upgrade_region_instruction(&ctx);
        state.upgrade_region(&ctx, instruction).await.unwrap();
    }

    #[tokio::test]
    async fn test_upgrade_region_with_retry_ok() {
        let mut state = Box::<UpgradeCandidateRegion>::default();
        state.retry_initial_interval = Duration::from_millis(100);
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        common_runtime::spawn_global(async move {
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Err(error::MailboxTimeoutSnafu { id: reply_id }.build()),
                )
                .await
                .unwrap();

            // retry: 1
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Ok(new_upgrade_region_reply(reply_id, false, true, None)),
                )
                .await
                .unwrap();

            // retry: 2
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Ok(new_upgrade_region_reply(reply_id, true, true, None)),
                )
                .await
                .unwrap();
        });

        let (next, _) = state.next(&mut ctx).await.unwrap();

        let update_metadata = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();

        assert_matches!(update_metadata, UpdateMetadata::Upgrade);
    }

    #[tokio::test]
    async fn test_upgrade_region_with_retry_failed() {
        let mut state = Box::<UpgradeCandidateRegion>::default();
        state.retry_initial_interval = Duration::from_millis(100);
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        common_runtime::spawn_global(async move {
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Err(error::MailboxTimeoutSnafu { id: reply_id }.build()),
                )
                .await
                .unwrap();

            // retry: 1
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Ok(new_upgrade_region_reply(reply_id, false, true, None)),
                )
                .await
                .unwrap();

            // retry: 2
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Ok(new_upgrade_region_reply(reply_id, false, false, None)),
                )
                .await
                .unwrap();
        });

        let (next, _) = state.next(&mut ctx).await.unwrap();

        let update_metadata = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();
        assert_matches!(update_metadata, UpdateMetadata::Rollback);
    }
}
