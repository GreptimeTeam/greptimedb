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
use std::ops::Div;

use api::v1::meta::MailboxMessage;
use common_meta::RegionIdent;
use common_meta::distributed_time_constants::default_distributed_time_constants;
use common_meta::instruction::{Instruction, InstructionReply, OpenRegion, SimpleReply};
use common_meta::key::datanode_table::RegionInfo;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use tokio::time::Instant;

use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::region_migration::flush_leader_region::PreFlushRegion;
use crate::procedure::region_migration::{Context, State};
use crate::service::mailbox::Channel;

#[derive(Debug, Serialize, Deserialize)]
pub struct OpenCandidateRegion;

#[async_trait::async_trait]
#[typetag::serde]
impl State for OpenCandidateRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let instruction = self.build_open_region_instruction(ctx).await?;
        let now = Instant::now();
        self.open_candidate_region(ctx, instruction).await?;
        ctx.update_open_candidate_region_elapsed(now);

        Ok((Box::new(PreFlushRegion), Status::executing(false)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl OpenCandidateRegion {
    /// Builds open region instructions
    ///
    /// Abort(non-retry):
    /// - Datanode Table is not found.
    async fn build_open_region_instruction(&self, ctx: &mut Context) -> Result<Instruction> {
        let region_ids = ctx.persistent_ctx.region_ids.clone();
        let from_peer_id = ctx.persistent_ctx.from_peer.id;
        let to_peer_id = ctx.persistent_ctx.to_peer.id;
        let datanode_table_values = ctx.get_from_peer_datanode_table_values().await?;
        let mut open_regions = Vec::with_capacity(region_ids.len());

        for region_id in region_ids {
            let table_id = region_id.table_id();
            let region_number = region_id.region_number();
            let datanode_table_value = datanode_table_values.get(&table_id).context(
                error::DatanodeTableNotFoundSnafu {
                    table_id,
                    datanode_id: from_peer_id,
                },
            )?;
            let RegionInfo {
                region_storage_path,
                region_options,
                region_wal_options,
                engine,
            } = datanode_table_value.region_info.clone();

            open_regions.push(OpenRegion::new(
                RegionIdent {
                    datanode_id: to_peer_id,
                    table_id,
                    region_number,
                    engine,
                },
                &region_storage_path,
                region_options,
                region_wal_options,
                true,
            ));
        }

        Ok(Instruction::OpenRegions(open_regions))
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
        let region_ids = &pc.region_ids;
        let candidate = &pc.to_peer;

        // This method might be invoked multiple times.
        // Only registers the guard if `opening_region_guard` is absent.
        if vc.opening_region_guards.is_empty() {
            for region_id in region_ids {
                // Registers the opening region.
                let guard = ctx
                    .opening_region_keeper
                    .register(candidate.id, *region_id)
                    .context(error::RegionOpeningRaceSnafu {
                        peer_id: candidate.id,
                        region_id: *region_id,
                    })?;
                vc.opening_region_guards.push(guard);
            }
        }

        let msg = MailboxMessage::json_message(
            &format!("Open candidate regions: {:?}", region_ids),
            &format!("Metasrv@{}", ctx.server_addr()),
            &format!("Datanode-{}@{}", candidate.id, candidate.addr),
            common_time::util::current_time_millis(),
            &open_instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: open_instruction.to_string(),
        })?;

        let operation_timeout =
            ctx.next_operation_timeout()
                .context(error::ExceededDeadlineSnafu {
                    operation: "Open candidate region",
                })?;
        let operation_timeout = operation_timeout
            .div(2)
            .max(default_distributed_time_constants().region_lease);
        let ch = Channel::Datanode(candidate.id);
        let now = Instant::now();
        let receiver = ctx.mailbox.send(&ch, msg, operation_timeout).await?;

        match receiver.await {
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                info!(
                    "Received open region reply: {:?}, region: {:?}, elapsed: {:?}",
                    reply,
                    region_ids,
                    now.elapsed()
                );
                let InstructionReply::OpenRegions(SimpleReply { result, error }) = reply else {
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
                            "Region {region_ids:?} is not opened by datanode {:?}, error: {error:?}, elapsed: {:?}",
                            candidate,
                            now.elapsed()
                        ),
                    }
                    .fail()
                }
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for open candidate region {region_ids:?} on datanode {:?}, elapsed: {:?}",
                    candidate,
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
    use std::collections::HashMap;

    use common_catalog::consts::MITO2_ENGINE;
    use common_meta::DatanodeId;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;
    use crate::procedure::region_migration::test_util::{self, TestingEnv, new_procedure_context};
    use crate::procedure::region_migration::{ContextFactory, PersistentContext};
    use crate::procedure::test_util::{
        new_close_region_reply, new_open_region_reply, send_mock_reply,
    };

    fn new_persistent_context() -> PersistentContext {
        test_util::new_persistent_context(1, 2, RegionId::new(1024, 1))
    }

    fn new_mock_open_instruction(datanode_id: DatanodeId, region_id: RegionId) -> Instruction {
        Instruction::OpenRegions(vec![OpenRegion {
            region_ident: RegionIdent {
                datanode_id,
                table_id: region_id.table_id(),
                region_number: region_id.region_number(),
                engine: MITO2_ENGINE.to_string(),
            },
            region_storage_path: "/bar/foo/region/".to_string(),
            region_options: Default::default(),
            region_wal_options: Default::default(),
            skip_wal_replay: true,
        }])
    }

    #[tokio::test]
    async fn test_datanode_table_is_not_found_error() {
        let state = OpenCandidateRegion;
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let err = state
            .build_open_region_instruction(&mut ctx)
            .await
            .unwrap_err();

        assert_matches!(err, Error::DatanodeTableNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_datanode_is_unreachable() {
        let state = OpenCandidateRegion;
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let region_id = persistent_context.region_ids[0];
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
        let region_id = persistent_context.region_ids[0];
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
        let region_id = persistent_context.region_ids[0];
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
        let region_id = persistent_context.region_ids[0];
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
        let region_id = persistent_context.region_ids[0];
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
        assert!(format!("{err:?}").contains("test mocked"));
    }

    #[tokio::test]
    async fn test_next_flush_leader_region_state() {
        let mut state = Box::new(OpenCandidateRegion);
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;
        let region_id = persistent_context.region_ids[0];
        let to_peer_id = persistent_context.to_peer.id;
        let mut env = TestingEnv::new();

        // Prepares table
        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(from_peer_id)),
            ..Default::default()
        }];

        env.table_metadata_manager()
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(region_routes),
                HashMap::default(),
            )
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
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();
        let vc = ctx.volatile_ctx;
        assert_eq!(vc.opening_region_guards[0].info(), (to_peer_id, region_id));

        let flush_leader_region = next.as_any().downcast_ref::<PreFlushRegion>().unwrap();
        assert_matches!(flush_leader_region, PreFlushRegion);
    }
}
