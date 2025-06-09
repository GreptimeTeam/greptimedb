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

use api::v1::meta::MailboxMessage;
use common_meta::instruction::{Instruction, InstructionReply, SimpleReply};
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::{info, warn};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use tokio::time::Instant;

use crate::error::{self, Error, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::region_migration::update_metadata::UpdateMetadata;
use crate::procedure::region_migration::{Context, State};
use crate::service::mailbox::Channel;

/// Flushes the leader region before downgrading it.
///
/// This can minimize the time window where the region is not writable.
#[derive(Debug, Serialize, Deserialize)]
pub struct PreFlushRegion;

#[async_trait::async_trait]
#[typetag::serde]
impl State for PreFlushRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let timer = Instant::now();
        self.flush_region(ctx).await?;
        ctx.update_flush_leader_region_elapsed(timer);
        // We intentionally don't update `operations_elapsed` here to prevent
        // the `next_operation_timeout` from being reduced by the flush operation.
        // This ensures sufficient time for subsequent critical operations.

        Ok((
            Box::new(UpdateMetadata::Downgrade),
            Status::executing(false),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PreFlushRegion {
    /// Builds flush leader region instruction.
    fn build_flush_leader_region_instruction(&self, ctx: &Context) -> Instruction {
        let pc = &ctx.persistent_ctx;
        let region_id = pc.region_id;
        Instruction::FlushRegion(region_id)
    }

    /// Tries to flush a leader region.
    ///
    /// Ignore:
    /// - [PusherNotFound](error::Error::PusherNotFound), The datanode is unreachable.
    /// - [PushMessage](error::Error::PushMessage), The receiver is dropped.
    /// - Failed to flush region on the Datanode.
    ///
    /// Abort:
    /// - [MailboxTimeout](error::Error::MailboxTimeout), Timeout.
    /// - [MailboxReceiver](error::Error::MailboxReceiver), The sender is dropped without sending (impossible).
    /// - [UnexpectedInstructionReply](error::Error::UnexpectedInstructionReply).
    /// - [ExceededDeadline](error::Error::ExceededDeadline)
    /// - Invalid JSON.
    async fn flush_region(&self, ctx: &mut Context) -> Result<()> {
        let operation_timeout =
            ctx.next_operation_timeout()
                .context(error::ExceededDeadlineSnafu {
                    operation: "Flush leader region",
                })?;
        let flush_instruction = self.build_flush_leader_region_instruction(ctx);
        let region_id = ctx.persistent_ctx.region_id;
        let leader = &ctx.persistent_ctx.from_peer;

        let msg = MailboxMessage::json_message(
            &format!("Flush leader region: {}", region_id),
            &format!("Metasrv@{}", ctx.server_addr()),
            &format!("Datanode-{}@{}", leader.id, leader.addr),
            common_time::util::current_time_millis(),
            &flush_instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: flush_instruction.to_string(),
        })?;

        let ch = Channel::Datanode(leader.id);
        let now = Instant::now();
        let result = ctx.mailbox.send(&ch, msg, operation_timeout).await;

        match result {
            Ok(receiver) => match receiver.await {
                Ok(msg) => {
                    let reply = HeartbeatMailbox::json_reply(&msg)?;
                    info!(
                        "Received flush leader region reply: {:?}, region: {}, elapsed: {:?}",
                        reply,
                        region_id,
                        now.elapsed()
                    );

                    let InstructionReply::FlushRegion(SimpleReply { result, error }) = reply else {
                        return error::UnexpectedInstructionReplySnafu {
                            mailbox_message: msg.to_string(),
                            reason: "expect flush region reply",
                        }
                        .fail();
                    };

                    if error.is_some() {
                        warn!(
                            "Failed to flush leader region {} on datanode {:?}, error: {:?}. Skip flush operation.",
                            region_id, leader, error
                        );
                    } else if result {
                        info!(
                            "The flush leader region {} on datanode {:?} is successful, elapsed: {:?}",
                            region_id,
                            leader,
                            now.elapsed()
                        );
                    }

                    Ok(())
                }
                Err(Error::MailboxTimeout { .. }) => error::ExceededDeadlineSnafu {
                    operation: "Flush leader region",
                }
                .fail(),
                Err(err) => Err(err),
            },
            Err(Error::PusherNotFound { .. }) => {
                warn!(
                    "Failed to flush leader region({}), the datanode({}) is unreachable(PusherNotFound). Skip flush operation.",
                    region_id,
                    leader
                );
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use store_api::storage::RegionId;

    use super::*;
    use crate::procedure::region_migration::test_util::{self, new_procedure_context, TestingEnv};
    use crate::procedure::region_migration::{ContextFactory, PersistentContext};
    use crate::procedure::test_util::{
        new_close_region_reply, new_flush_region_reply, send_mock_reply,
    };

    fn new_persistent_context() -> PersistentContext {
        test_util::new_persistent_context(1, 2, RegionId::new(1024, 1))
    }

    #[tokio::test]
    async fn test_datanode_is_unreachable() {
        let state = PreFlushRegion;
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        // Should be ok, if leader region is unreachable. it will skip flush operation.
        state.flush_region(&mut ctx).await.unwrap();
    }

    #[tokio::test]
    async fn test_unexpected_instruction_reply() {
        common_telemetry::init_default_ut_logging();
        let state = PreFlushRegion;
        // from_peer: 1
        // to_peer: 2
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
        let err = state.flush_region(&mut ctx).await.unwrap_err();
        assert_matches!(err, Error::UnexpectedInstructionReply { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_instruction_exceeded_deadline() {
        let state = PreFlushRegion;
        // from_peer: 1
        // to_peer: 2
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
        // Sends an timeout error.
        send_mock_reply(mailbox, rx, |id| {
            Err(error::MailboxTimeoutSnafu { id }.build())
        });

        let err = state.flush_region(&mut ctx).await.unwrap_err();
        assert_matches!(err, Error::ExceededDeadline { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_flush_region_failed() {
        common_telemetry::init_default_ut_logging();
        let state = PreFlushRegion;
        // from_peer: 1
        // to_peer: 2
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
            Ok(new_flush_region_reply(
                id,
                false,
                Some("test mocked".to_string()),
            ))
        });
        // Should be ok, if flush leader region failed. it will skip flush operation.
        state.flush_region(&mut ctx).await.unwrap();
    }

    #[tokio::test]
    async fn test_next_update_metadata_downgrade_state() {
        common_telemetry::init_default_ut_logging();
        let mut state = PreFlushRegion;
        // from_peer: 1
        // to_peer: 2
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
        send_mock_reply(mailbox, rx, |id| Ok(new_flush_region_reply(id, true, None)));
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();

        let update_metadata = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();
        assert_matches!(update_metadata, UpdateMetadata::Downgrade);
    }
}
