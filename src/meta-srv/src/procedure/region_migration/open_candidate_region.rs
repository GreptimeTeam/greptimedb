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
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_meta::ddl::utils::region_storage_path;
use common_meta::distributed_time_constants::MAILBOX_RTT_SECS;
use common_meta::instruction::{Instruction, InstructionReply, OpenRegion, SimpleReply};
use common_meta::RegionIdent;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::region_migration::downgrade_leader_region::DowngradeLeaderRegion;
use crate::procedure::region_migration::{Context, State};
use crate::service::mailbox::Channel;

const OPEN_CANDIDATE_REGION_TIMEOUT: Duration = Duration::from_secs(MAILBOX_RTT_SECS);

#[derive(Debug, Serialize, Deserialize)]
pub struct OpenCandidateRegion;

#[async_trait::async_trait]
#[typetag::serde]
impl State for OpenCandidateRegion {
    async fn next(&mut self, ctx: &mut Context) -> Result<Box<dyn State>> {
        let instruction = self.build_open_region_instruction(ctx).await?;
        self.open_candidate_region(ctx, instruction).await?;

        Ok(Box::<DowngradeLeaderRegion>::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl OpenCandidateRegion {
    /// Builds open region instructions
    ///
    /// Abort(non-retry):
    /// - Table Info is not found.
    async fn build_open_region_instruction(&self, ctx: &mut Context) -> Result<Instruction> {
        let pc = &ctx.persistent_ctx;
        let cluster_id = pc.cluster_id;
        let table_id = pc.region_id.table_id();
        let region_number = pc.region_id.region_number();
        let candidate_id = pc.to_peer.id;

        let table_info_value = ctx.get_table_info_value().await?;
        let table_info = &table_info_value.table_info;

        // The region storage path is immutable after the region is created.
        // Therefore, it's safe to store it in `VolatileContext` for future use.
        let region_storage_path =
            region_storage_path(&table_info.catalog_name, &table_info.schema_name);

        let engine = table_info.meta.engine.clone();
        let region_options: HashMap<String, String> = (&table_info.meta.options).into();

        let open_instruction = Instruction::OpenRegion(OpenRegion::new(
            RegionIdent {
                cluster_id,
                datanode_id: candidate_id,
                table_id,
                region_number,
                engine,
            },
            &region_storage_path,
            region_options,
        ));

        Ok(open_instruction)
    }

    /// Opens the candidate region.
    ///
    /// Abort(non-retry):
    /// - The Datanode is unreachable(e.g., Candidate pusher is not found).
    /// - Unexpected instruction reply.
    /// - Another procedure is opening the candidate region.
    ///
    /// Retry:
    /// - Exceeded deadline of open instruction.
    /// - Datanode failed to open the candidate region.
    async fn open_candidate_region(
        &self,
        ctx: &mut Context,
        open_instruction: Instruction,
    ) -> Result<()> {
        let pc = &ctx.persistent_ctx;
        let vc = &mut ctx.volatile_ctx;
        let region_id = pc.region_id;
        let candidate = &pc.to_peer;

        // Registers the opening region.
        let guard = ctx
            .opening_region_keeper
            .register(candidate.id, region_id)
            .context(error::RegionOpeningRaceSnafu {
                peer_id: candidate.id,
                region_id,
            })?;

        debug_assert!(vc.opening_region_guard.is_none());
        vc.opening_region_guard = Some(guard);

        let msg = MailboxMessage::json_message(
            &format!("Open candidate region: {}", region_id),
            &format!("Meta@{}", ctx.server_addr()),
            &format!("Datanode-{}@{}", candidate.id, candidate.addr),
            common_time::util::current_time_millis(),
            &open_instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: open_instruction.to_string(),
        })?;

        let ch = Channel::Datanode(candidate.id);
        let receiver = ctx
            .mailbox
            .send(&ch, msg, OPEN_CANDIDATE_REGION_TIMEOUT)
            .await?;

        match receiver.await? {
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                let InstructionReply::OpenRegion(SimpleReply { result, error }) = reply else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect open region reply",
                    }
                    .fail();
                };

                if result {
                    Ok(())
                } else {
                    error::RetryLaterSnafu {
                        reason: format!(
                            "Region {region_id} is not opened by datanode {:?}, error: {error:?}",
                            candidate,
                        ),
                    }
                    .fail()
                }
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for open candidate region {region_id} on datanode {:?}", 
                    candidate,
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

    use api::v1::meta::mailbox_message::Payload;
    use common_catalog::consts::MITO2_ENGINE;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::DatanodeId;
    use common_time::util::current_time_millis;
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;
    use crate::procedure::region_migration::downgrade_leader_region::DowngradeLeaderRegion;
    use crate::procedure::region_migration::test_util::{
        self, new_close_region_reply, send_mock_reply, TestingEnv,
    };
    use crate::procedure::region_migration::{ContextFactory, PersistentContext};

    fn new_persistent_context() -> PersistentContext {
        test_util::new_persistent_context(1, 2, RegionId::new(1024, 1))
    }

    fn new_mock_open_instruction(datanode_id: DatanodeId, region_id: RegionId) -> Instruction {
        Instruction::OpenRegion(OpenRegion {
            region_ident: RegionIdent {
                cluster_id: 0,
                datanode_id,
                table_id: region_id.table_id(),
                region_number: region_id.region_number(),
                engine: MITO2_ENGINE.to_string(),
            },
            region_storage_path: "/bar/foo/region/".to_string(),
            options: Default::default(),
        })
    }

    fn new_open_region_reply(id: u64, result: bool, error: Option<String>) -> MailboxMessage {
        MailboxMessage {
            id,
            subject: "mock".to_string(),
            from: "datanode".to_string(),
            to: "meta".to_string(),
            timestamp_millis: current_time_millis(),
            payload: Some(Payload::Json(
                serde_json::to_string(&InstructionReply::OpenRegion(SimpleReply { result, error }))
                    .unwrap(),
            )),
        }
    }

    #[tokio::test]
    async fn test_table_info_is_not_found_error() {
        let state = OpenCandidateRegion;
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let err = state
            .build_open_region_instruction(&mut ctx)
            .await
            .unwrap_err();

        assert_matches!(err, Error::TableInfoNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_datanode_is_unreachable() {
        let state = OpenCandidateRegion;
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let region_id = persistent_context.region_id;
        let to_peer_id = persistent_context.to_peer.id;
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let open_instruction = new_mock_open_instruction(to_peer_id, region_id);
        let err = state
            .open_candidate_region(&mut ctx, open_instruction)
            .await
            .unwrap_err();

        assert_matches!(err, Error::PusherNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_candidate_region_opening_error() {
        let state = OpenCandidateRegion;
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let region_id = persistent_context.region_id;
        let to_peer_id = persistent_context.to_peer.id;

        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let opening_region_keeper = env.opening_region_keeper();
        let _guard = opening_region_keeper
            .register(to_peer_id, region_id)
            .unwrap();

        let open_instruction = new_mock_open_instruction(to_peer_id, region_id);
        let err = state
            .open_candidate_region(&mut ctx, open_instruction)
            .await
            .unwrap_err();

        assert_matches!(err, Error::RegionOpeningRace { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_unexpected_instruction_reply() {
        let state = OpenCandidateRegion;
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let region_id = persistent_context.region_id;
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        // Sends an incorrect reply.
        send_mock_reply(mailbox, rx, |id| Ok(new_close_region_reply(id)));

        let open_instruction = new_mock_open_instruction(to_peer_id, region_id);
        let err = state
            .open_candidate_region(&mut ctx, open_instruction)
            .await
            .unwrap_err();

        assert_matches!(err, Error::UnexpectedInstructionReply { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_instruction_exceeded_deadline() {
        let state = OpenCandidateRegion;
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let region_id = persistent_context.region_id;
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        // Sends an timeout error.
        send_mock_reply(mailbox, rx, |id| {
            Err(error::MailboxTimeoutSnafu { id }.build())
        });

        let open_instruction = new_mock_open_instruction(to_peer_id, region_id);
        let err = state
            .open_candidate_region(&mut ctx, open_instruction)
            .await
            .unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_open_candidate_region_failed() {
        let state = OpenCandidateRegion;
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let region_id = persistent_context.region_id;
        let to_peer_id = persistent_context.to_peer.id;
        let mut env = TestingEnv::new();

        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_open_region_reply(
                id,
                false,
                Some("test mocked".to_string()),
            ))
        });

        let open_instruction = new_mock_open_instruction(to_peer_id, region_id);
        let err = state
            .open_candidate_region(&mut ctx, open_instruction)
            .await
            .unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
        assert!(err.to_string().contains("test mocked"));
    }

    #[tokio::test]
    async fn test_next_downgrade_leader_region_state() {
        let mut state = Box::new(OpenCandidateRegion);
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let region_id = persistent_context.region_id;
        let to_peer_id = persistent_context.to_peer.id;
        let mut env = TestingEnv::new();

        // Prepares table
        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(persistent_context.region_id),
            leader_peer: Some(Peer::empty(3)),
            ..Default::default()
        }];

        env.table_metadata_manager()
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| Ok(new_open_region_reply(id, true, None)));

        let next = state.next(&mut ctx).await.unwrap();
        let vc = ctx.volatile_ctx;
        assert_eq!(
            vc.opening_region_guard.unwrap().info(),
            (to_peer_id, region_id)
        );

        let _ = next
            .as_any()
            .downcast_ref::<DowngradeLeaderRegion>()
            .unwrap();
    }
}
