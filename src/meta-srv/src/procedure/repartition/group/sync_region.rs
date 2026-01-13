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
    FlushRegions, Instruction, InstructionReply, SyncRegionReply, SyncRegionsReply,
};
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::{info, warn};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::region_engine::SyncRegionFromRequest;
use store_api::storage::RegionId;

use crate::error::{self, Error, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
use crate::procedure::repartition::group::utils::{
    HandleMultipleResult, group_region_routes_by_peer, handle_multiple_results,
};
use crate::procedure::repartition::group::{Context, State};
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
        self.sync_regions(ctx).await?;

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
        let flush_instruction =
            Instruction::FlushRegions(FlushRegions::sync_single(prepare_result.central_region));
        let msg = MailboxMessage::json_message(
            &format!("Flush central region: {:?}", prepare_result.central_region),
            &format!("Metasrv@{}", ctx.server_addr),
            &format!(
                "Datanode-{}@{}",
                prepare_result.central_region_datanode.id,
                prepare_result.central_region_datanode.addr
            ),
            common_time::util::current_time_millis(),
            &flush_instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: flush_instruction.to_string(),
        })?;

        let ch = Channel::Datanode(prepare_result.central_region_datanode.id);
        let now = Instant::now();
        let result = ctx.mailbox.send(&ch, msg, operation_timeout).await;

        match result {
            Ok(receiver) => match receiver.await {
                Ok(msg) => {
                    let reply = HeartbeatMailbox::json_reply(&msg)?;
                    info!(
                        "Received flush central region reply: {:?}, region: {}, elapsed: {:?}",
                        reply,
                        prepare_result.central_region,
                        now.elapsed()
                    );
                    let reply_result = match reply {
                        InstructionReply::FlushRegions(flush_reply) => {
                            if flush_reply.results.len() != 1 {
                                return error::UnexpectedInstructionReplySnafu {
                                    mailbox_message: msg.to_string(),
                                    reason: format!(
                                        "expect {} region flush result, but got {}",
                                        1,
                                        flush_reply.results.len()
                                    ),
                                }
                                .fail();
                            }

                            match flush_reply.overall_success {
                                true => (true, None),
                                false => (
                                    false,
                                    Some(
                                        flush_reply
                                            .results
                                            .iter()
                                            .filter_map(|(region_id, result)| match result {
                                                Ok(_) => None,
                                                Err(e) => Some(format!("{}: {}", region_id, e)),
                                            })
                                            .collect::<Vec<String>>()
                                            .join("; "),
                                    ),
                                ),
                            }
                        }
                        _ => {
                            return error::UnexpectedInstructionReplySnafu {
                                mailbox_message: msg.to_string(),
                                reason: "expect flush region reply",
                            }
                            .fail();
                        }
                    };
                    let (result, error) = reply_result;

                    if let Some(error) = error {
                        warn!(
                            "Failed to flush central region {:?} on datanode {:?}, error: {}. Skip flush operation.",
                            prepare_result.central_region,
                            prepare_result.central_region_datanode,
                            &error
                        );
                    } else if result {
                        info!(
                            "The flush central region {:?} on datanode {:?} is successful, elapsed: {:?}",
                            prepare_result.central_region,
                            prepare_result.central_region_datanode,
                            now.elapsed()
                        );
                    }

                    Ok(())
                }
                Err(Error::MailboxTimeout { .. }) => error::ExceededDeadlineSnafu {
                    operation: "Flush central region",
                }
                .fail(),
                Err(err) => Err(err),
            },
            Err(Error::PusherNotFound { .. }) => {
                warn!(
                    "Failed to flush central region({:?}), the datanode({}) is unreachable(PusherNotFound). Skip flush operation.",
                    prepare_result.central_region, prepare_result.central_region_datanode,
                );
                Ok(())
            }
            Err(err) => Err(err),
        }
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
    async fn sync_regions(&self, ctx: &mut Context) -> Result<()> {
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
                    Self::sync_region(
                        &ctx.mailbox,
                        &ctx.server_addr,
                        peer,
                        sync_regions,
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

        let results = join_all(tasks).await;
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

    /// Syncs regions on a datanode.
    async fn sync_region(
        mailbox: &MailboxRef,
        server_addr: &str,
        peer: &Peer,
        sync_regions: &[common_meta::instruction::SyncRegion],
        timeout: Duration,
    ) -> Result<()> {
        let ch = Channel::Datanode(peer.id);
        let instruction = Instruction::SyncRegions(sync_regions.to_vec());
        let message = MailboxMessage::json_message(
            &format!(
                "Sync regions: {:?}",
                sync_regions.iter().map(|r| r.region_id).collect::<Vec<_>>()
            ),
            &format!("Metasrv@{}", server_addr),
            &format!("Datanode-{}@{}", peer.id, peer.addr),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

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
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                info!(
                    "Received sync regions reply: {:?}, elapsed: {:?}",
                    reply,
                    now.elapsed()
                );
                let InstructionReply::SyncRegions(SyncRegionsReply { replies }) = reply else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect sync regions reply",
                    }
                    .fail();
                };
                for reply in replies {
                    Self::handle_sync_region_reply(&reply, &now, peer)?;
                }
                Ok(())
            }
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
            return error::RetryLaterSnafu {
                reason: format!(
                    "Failed to sync region {} on datanode {:?}, error: {:?}, elapsed: {:?}",
                    region_id,
                    peer,
                    error,
                    now.elapsed()
                ),
            }
            .fail();
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
    use std::assert_matches::assert_matches;

    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::region_engine::SyncRegionFromRequest;
    use store_api::storage::RegionId;

    use crate::error::Error;
    use crate::procedure::repartition::group::GroupPrepareResult;
    use crate::procedure::repartition::group::sync_region::SyncRegion;
    use crate::procedure::repartition::test_util::{TestingEnv, new_persistent_context};
    use crate::procedure::test_util::{new_sync_region_reply, send_mock_reply};
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

        sync_region.sync_regions(&mut ctx).await.unwrap();
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

        let err = sync_region.sync_regions(&mut ctx).await.unwrap_err();
        assert_matches!(err, Error::RetryLater { .. });
    }
}
