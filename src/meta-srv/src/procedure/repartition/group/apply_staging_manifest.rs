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
    ApplyStagingManifestReply, ApplyStagingManifestsReply, Instruction, InstructionReply,
};
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::RegionId;

use crate::error::{self, Error, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
use crate::procedure::repartition::group::utils::{
    HandleMultipleResult, group_region_routes_by_peer, handle_multiple_results,
};
use crate::procedure::repartition::group::{Context, State};
use crate::procedure::repartition::plan::RegionDescriptor;
use crate::service::mailbox::{Channel, MailboxRef};

#[derive(Debug, Serialize, Deserialize)]
pub struct ApplyStagingManifest;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ApplyStagingManifest {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let timer = Instant::now();
        self.apply_staging_manifests(ctx).await?;
        ctx.update_apply_staging_manifest_elapsed(timer.elapsed());

        Ok((
            Box::new(UpdateMetadata::ExitStaging),
            Status::executing(true),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct ApplyStagingManifestInstructions {
    instructions: HashMap<Peer, Vec<common_meta::instruction::ApplyStagingManifest>>,
    central_region_instruction: Option<(Peer, common_meta::instruction::ApplyStagingManifest)>,
}

impl ApplyStagingManifest {
    fn build_apply_staging_manifest_instructions(
        staging_manifest_paths: &HashMap<RegionId, String>,
        target_routes: &[RegionRoute],
        targets: &[RegionDescriptor],
        central_region_id: RegionId,
    ) -> Result<ApplyStagingManifestInstructions> {
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
        let target_region_routes_by_peer = group_region_routes_by_peer(target_routes);
        let mut instructions = HashMap::with_capacity(target_region_routes_by_peer.len());

        let mut central_region_instruction = None;
        for (peer, mut region_ids) in target_region_routes_by_peer {
            // If the central region is in the target region ids,
            // remove it and build the instruction for the central region.
            if region_ids.contains(&central_region_id) {
                region_ids.retain(|r| *r != central_region_id);
                central_region_instruction = Some((
                    peer.clone(),
                    common_meta::instruction::ApplyStagingManifest {
                        region_id: central_region_id,
                        partition_expr: target_partition_expr_by_region[&central_region_id].clone(),
                        central_region_id,
                        manifest_path: staging_manifest_paths[&central_region_id].clone(),
                    },
                ));
                if region_ids.is_empty() {
                    continue;
                }
            }
            let apply_staging_manifests = region_ids
                .into_iter()
                .map(|region_id| common_meta::instruction::ApplyStagingManifest {
                    region_id,
                    partition_expr: target_partition_expr_by_region[&region_id].clone(),
                    central_region_id,
                    manifest_path: staging_manifest_paths[&region_id].clone(),
                })
                .collect();
            instructions.insert(peer.clone(), apply_staging_manifests);
        }

        Ok(ApplyStagingManifestInstructions {
            instructions,
            central_region_instruction,
        })
    }

    async fn apply_staging_manifests(&self, ctx: &mut Context) -> Result<()> {
        let table_id = ctx.persistent_ctx.table_id;
        let group_id = ctx.persistent_ctx.group_id;
        let staging_manifest_paths = &ctx.persistent_ctx.staging_manifest_paths;
        // Safety: the group prepare result is set in the RepartitionStart state.
        let prepare_result = ctx.persistent_ctx.group_prepare_result.as_ref().unwrap();
        let targets = &ctx.persistent_ctx.targets;
        let target_routes = &prepare_result.target_routes;
        let central_region_id = prepare_result.central_region;
        let ApplyStagingManifestInstructions {
            instructions,
            central_region_instruction,
        } = Self::build_apply_staging_manifest_instructions(
            staging_manifest_paths,
            target_routes,
            targets,
            central_region_id,
        )?;
        let operation_timeout =
            ctx.next_operation_timeout()
                .context(error::ExceededDeadlineSnafu {
                    operation: "Apply staging manifests",
                })?;

        let instruction_region_count: usize = instructions.values().map(|v| v.len()).sum();
        let (peers, tasks): (Vec<_>, Vec<_>) = instructions
            .iter()
            .map(|(peer, apply_staging_manifests)| {
                (
                    peer,
                    Self::apply_staging_manifest(
                        &ctx.mailbox,
                        &ctx.server_addr,
                        peer,
                        apply_staging_manifests,
                        operation_timeout,
                    ),
                )
            })
            .unzip();
        info!(
            "Sent apply staging manifests instructions, table_id: {}, group_id: {}, peers: {}, regions: {}",
            table_id,
            group_id,
            peers.len(),
            instruction_region_count
        );

        let format_err_msg = |idx: usize, error: &Error| {
            let peer = peers[idx];
            format!(
                "Failed to apply staging manifests on datanode {:?}, error: {:?}",
                peer, error
            )
        };
        // Waits for all tasks to complete.
        let results = join_all(tasks).await;
        let result = handle_multiple_results(&results);
        match result {
            HandleMultipleResult::AllSuccessful => {
                // Coninute
            }
            HandleMultipleResult::AllRetryable(retryable_errors) => return error::RetryLaterSnafu {
                reason: format!(
                    "All retryable errors during applying staging manifests for repartition table {}, group id {}: {:?}",
                    table_id, group_id,
                    retryable_errors
                        .iter()
                        .map(|(idx, error)| format_err_msg(*idx, error))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            }
            .fail(),
            HandleMultipleResult::AllNonRetryable(non_retryable_errors) => return error::UnexpectedSnafu {
                violated: format!(
                    "All non retryable errors during applying staging manifests for repartition table {}, group id {}: {:?}",
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
            } => return error::UnexpectedSnafu {
                violated: format!(
                    "Partial retryable errors during applying staging manifests for repartition table {}, group id {}: {:?}, non retryable errors: {:?}",
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

        if let Some((peer, instruction)) = central_region_instruction {
            info!("Applying staging manifest for central region: {:?}", peer);
            Self::apply_staging_manifest(
                &ctx.mailbox,
                &ctx.server_addr,
                &peer,
                &[instruction],
                operation_timeout,
            )
            .await?;
        }

        Ok(())
    }

    async fn apply_staging_manifest(
        mailbox: &MailboxRef,
        server_addr: &str,
        peer: &Peer,
        apply_staging_manifests: &[common_meta::instruction::ApplyStagingManifest],
        timeout: Duration,
    ) -> Result<()> {
        let ch = Channel::Datanode(peer.id);
        let instruction = Instruction::ApplyStagingManifests(apply_staging_manifests.to_vec());
        let message = MailboxMessage::json_message(
            &format!(
                "Apply staging manifests for regions: {:?}",
                apply_staging_manifests
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
                    "Pusher not found for apply staging manifests on datanode {:?}, elapsed: {:?}",
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
                let elapsed = now.elapsed();
                let InstructionReply::ApplyStagingManifests(ApplyStagingManifestsReply { replies }) =
                    reply
                else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect apply staging manifests reply",
                    }
                    .fail();
                };
                let total = replies.len();
                let (mut ready, mut not_ready, mut with_error) = (0, 0, 0);
                let region_ids = replies.iter().map(|r| r.region_id).collect::<Vec<_>>();
                for reply in replies {
                    if reply.error.is_some() {
                        with_error += 1;
                    } else if reply.ready {
                        ready += 1;
                    } else {
                        not_ready += 1;
                    }
                    Self::handle_apply_staging_manifest_reply(&reply, &now, peer)?;
                }
                info!(
                    "Received apply staging manifests reply, peer: {:?}, total_regions: {}, regions:{:?}, ready: {}, not_ready: {}, with_error: {}, elapsed: {:?}",
                    peer, total, region_ids, ready, not_ready, with_error, elapsed
                );

                Ok(())
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for apply staging manifests on datanode {:?}, elapsed: {:?}",
                    peer,
                    now.elapsed()
                );
                error::RetryLaterSnafu { reason }.fail()
            }
            Err(err) => Err(err),
        }
    }

    fn handle_apply_staging_manifest_reply(
        ApplyStagingManifestReply {
            region_id,
            ready,
            exists,
            error,
        }: &ApplyStagingManifestReply,
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
                    "Failed to apply staging manifest on datanode {:?}, error: {:?}, elapsed: {:?}",
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
                    "Region {} is still applying staging manifest on datanode {:?}, elapsed: {:?}",
                    region_id,
                    peer,
                    now.elapsed()
                ),
            }
        );

        Ok(())
    }
}
