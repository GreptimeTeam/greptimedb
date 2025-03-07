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

use std::time::{Duration, Instant};

use api::v1::meta::MailboxMessage;
use common_error::ext::BoxedError;
use common_meta::distributed_time_constants::REGION_LEASE_SECS;
use common_meta::instruction::{Instruction, InstructionReply, OpenRegion, SimpleReply};
use common_meta::key::datanode_table::{DatanodeTableKey, RegionInfo};
use common_meta::peer::Peer;
use common_meta::RegionIdent;
use common_telemetry::info;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use super::Context;
use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::service::mailbox::Channel;

/// Uses lease time of a region as the timeout of opening a candidate region.
const OPEN_REGION_FOLLOWER_TIMEOUT: Duration = Duration::from_secs(REGION_LEASE_SECS);

pub(crate) struct CreateFollower {
    region_id: RegionId,
    // The peer of the datanode to add region follower.
    peer: Peer,
}

impl CreateFollower {
    pub fn new(region_id: RegionId, peer: Peer) -> Self {
        Self { region_id, peer }
    }

    /// Builds the open region instruction for the region follower.
    pub(crate) async fn build_open_region_instruction(&self, ctx: &Context) -> Result<Instruction> {
        let datanode_id = self.peer.id;
        let table_id = self.region_id.table_id();
        let region_number = self.region_id.region_number();
        let datanode_table_key = DatanodeTableKey {
            datanode_id,
            table_id,
        };

        let datanode_table_value = ctx
            .table_metadata_manager
            .datanode_table_manager()
            .get(&datanode_table_key)
            .await
            .context(error::TableMetadataManagerSnafu)
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!("Failed to get DatanodeTable: ({datanode_id},{table_id})"),
            })?
            .context(error::DatanodeTableNotFoundSnafu {
                table_id,
                datanode_id,
            })?;

        let RegionInfo {
            region_storage_path,
            region_options,
            region_wal_options,
            engine,
        } = datanode_table_value.region_info;

        let region_ident = RegionIdent {
            datanode_id,
            table_id,
            region_number,
            engine,
        };

        let open_instruction = Instruction::OpenRegion(OpenRegion::new(
            region_ident,
            &region_storage_path,
            region_options,
            region_wal_options,
            true,
        ));

        Ok(open_instruction)
    }

    /// Sends the open region instruction to the datanode.
    pub(crate) async fn send_open_region_instruction(
        &self,
        ctx: &Context,
        instruction: Instruction,
    ) -> Result<()> {
        let msg = MailboxMessage::json_message(
            &format!("Open a follower region: {}", self.region_id),
            &format!("Metasrv@{}", ctx.server_addr),
            &format!("Datanode-{}@{}", self.peer.id, self.peer.addr),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        let ch = Channel::Datanode(self.peer.id);
        let now = Instant::now();
        let receiver = ctx
            .mailbox
            .send(&ch, msg, OPEN_REGION_FOLLOWER_TIMEOUT)
            .await?;

        match receiver.await? {
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                info!(
                    "Received open region follower reply: {:?}, region: {}, elapsed: {:?}",
                    reply,
                    self.region_id,
                    now.elapsed()
                );
                let InstructionReply::OpenRegion(SimpleReply { result, error }) = reply else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect open region follower reply",
                    }
                    .fail();
                };

                if result {
                    Ok(())
                } else {
                    error::RetryLaterSnafu {
                        reason: format!(
                            "Region {} is not opened by datanode {:?}, error: {error:?}, elapsed: {:?}",
                            self.region_id,
                            &self.peer,
                            now.elapsed()
                        ),
                    }
                    .fail()
                }
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for open region follower {} on datanode {:?}, elapsed: {:?}",
                    self.region_id,
                    &self.peer,
                    now.elapsed()
                );
                error::RetryLaterSnafu { reason }.fail()
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_meta::DatanodeId;

    use super::*;
    use crate::error::Error;
    use crate::procedure::region_follower::test_util::TestingEnv;
    use crate::procedure::test_util::{new_close_region_reply, send_mock_reply};

    #[tokio::test]
    async fn test_datanode_table_not_found() {
        let env = TestingEnv::new();
        let ctx = env.new_context();

        let create_follower =
            CreateFollower::new(RegionId::new(1, 1), Peer::new(1, "127.0.0.1:8080"));
        let instruction = create_follower
            .build_open_region_instruction(&ctx)
            .await
            .unwrap();
        let err = create_follower
            .send_open_region_instruction(&ctx, instruction)
            .await
            .unwrap_err();
        assert_matches!(err, Error::DatanodeTableNotFound { .. });
    }

    #[tokio::test]
    async fn test_datanode_is_unreachable() {
        let env = TestingEnv::new();
        let ctx = env.new_context();

        let region_id = RegionId::new(1, 1);
        let peer = Peer::new(1, "127.0.0.1:8080");
        let create_follower = CreateFollower::new(region_id, peer.clone());
        let instruction = mock_open_region_instruction(peer.id, region_id);
        let err = create_follower
            .send_open_region_instruction(&ctx, instruction)
            .await
            .unwrap_err();
        assert_matches!(err, Error::PusherNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_unexpected_instruction_reply() {
        let mut env = TestingEnv::new();
        let ctx = env.new_context();
        let mailbox_ctx = env.mailbox_context_mut();
        let mailbox = mailbox_ctx.mailbox().clone();

        let region_id = RegionId::new(1, 1);
        let peer = Peer::new(1, "127.0.0.1:8080");

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(peer.id), tx)
            .await;

        // Sends an timeout error.
        send_mock_reply(mailbox, rx, |id| Ok(new_close_region_reply(id)));

        let create_follower = CreateFollower::new(region_id, peer.clone());
        let instruction = mock_open_region_instruction(peer.id, region_id);
        let err = create_follower
            .send_open_region_instruction(&ctx, instruction)
            .await
            .unwrap_err();
        assert_matches!(err, Error::UnexpectedInstructionReply { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_instruction_exceeded_deadline() {
        let mut env = TestingEnv::new();
        let ctx = env.new_context();
        let mailbox_ctx = env.mailbox_context_mut();
        let mailbox = mailbox_ctx.mailbox().clone();

        let region_id = RegionId::new(1, 1);
        let peer = Peer::new(1, "127.0.0.1:8080");

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(peer.id), tx)
            .await;

        // Sends an timeout error.
        send_mock_reply(mailbox, rx, |id| {
            Err(error::MailboxTimeoutSnafu { id }.build())
        });

        let create_follower = CreateFollower::new(region_id, peer.clone());
        let instruction = mock_open_region_instruction(peer.id, region_id);
        let err = create_follower
            .send_open_region_instruction(&ctx, instruction)
            .await
            .unwrap_err();
        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
    }

    fn mock_open_region_instruction(datanode_id: DatanodeId, region_id: RegionId) -> Instruction {
        Instruction::OpenRegion(OpenRegion {
            region_ident: RegionIdent {
                datanode_id,
                table_id: region_id.table_id(),
                region_number: region_id.region_number(),
                engine: "mito2".to_string(),
            },
            region_storage_path: "/tmp".to_string(),
            region_options: Default::default(),
            region_wal_options: Default::default(),
            skip_wal_replay: true,
        })
    }
}
