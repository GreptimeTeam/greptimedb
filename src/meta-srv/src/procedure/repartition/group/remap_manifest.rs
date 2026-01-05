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
use common_meta::instruction::{Instruction, InstructionReply, RemapManifestReply};
use common_meta::peer::Peer;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::{info, warn};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::repartition::group::apply_staging_manifest::ApplyStagingManifest;
use crate::procedure::repartition::group::{Context, State};
use crate::procedure::repartition::plan::RegionDescriptor;
use crate::service::mailbox::{Channel, MailboxRef};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RemapManifest;

#[async_trait::async_trait]
#[typetag::serde]
impl State for RemapManifest {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let prepare_result = ctx.persistent_ctx.group_prepare_result.as_ref().unwrap();
        let remap = Self::build_remap_manifest_instructions(
            &ctx.persistent_ctx.sources,
            &ctx.persistent_ctx.targets,
            &ctx.persistent_ctx.region_mapping,
            prepare_result.central_region,
        )?;
        let operation_timeout =
            ctx.next_operation_timeout()
                .context(error::ExceededDeadlineSnafu {
                    operation: "Remap manifests",
                })?;
        let manifest_paths = Self::remap_manifests(
            &ctx.mailbox,
            &ctx.server_addr,
            &prepare_result.central_region_datanode,
            &remap,
            operation_timeout,
        )
        .await?;
        let table_id = ctx.persistent_ctx.table_id;
        let group_id = ctx.persistent_ctx.group_id;

        if manifest_paths.len() != ctx.persistent_ctx.targets.len() {
            warn!(
                "Mismatch in manifest paths count: expected {}, got {}. This occurred during remapping manifests for group {} and table {}.",
                ctx.persistent_ctx.targets.len(),
                manifest_paths.len(),
                group_id,
                table_id
            );
        }

        ctx.persistent_ctx.staging_manifest_paths = manifest_paths;

        Ok((Box::new(ApplyStagingManifest), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl RemapManifest {
    fn build_remap_manifest_instructions(
        source_regions: &[RegionDescriptor],
        target_regions: &[RegionDescriptor],
        region_mapping: &HashMap<RegionId, Vec<RegionId>>,
        central_region_id: RegionId,
    ) -> Result<common_meta::instruction::RemapManifest> {
        let new_partition_exprs = target_regions
            .iter()
            .map(|r| {
                Ok((
                    r.region_id,
                    r.partition_expr
                        .as_json_str()
                        .context(error::SerializePartitionExprSnafu)?,
                ))
            })
            .collect::<Result<HashMap<RegionId, String>>>()?;

        Ok(common_meta::instruction::RemapManifest {
            region_id: central_region_id,
            input_regions: source_regions.iter().map(|r| r.region_id).collect(),
            region_mapping: region_mapping.clone(),
            new_partition_exprs,
        })
    }

    async fn remap_manifests(
        mailbox: &MailboxRef,
        server_addr: &str,
        peer: &Peer,
        remap: &common_meta::instruction::RemapManifest,
        timeout: Duration,
    ) -> Result<HashMap<RegionId, String>> {
        let ch = Channel::Datanode(peer.id);
        let instruction = Instruction::RemapManifest(remap.clone());
        let message = MailboxMessage::json_message(
            &format!(
                "Remap manifests, central region: {}, input regions: {:?}",
                remap.region_id, remap.input_regions
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
                    "Pusher not found for remap manifests on datanode {:?}, elapsed: {:?}",
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
                    "Received remap manifest reply: {:?}, elapsed: {:?}",
                    reply,
                    now.elapsed()
                );
                let InstructionReply::RemapManifest(reply) = reply else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect remap manifest reply",
                    }
                    .fail();
                };

                Self::handle_remap_manifest_reply(remap.region_id, reply, &now, peer)
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for remap manifests on datanode {:?}, elapsed: {:?}",
                    peer,
                    now.elapsed()
                );
                error::RetryLaterSnafu { reason }.fail()
            }
            Err(err) => Err(err),
        }
    }

    fn handle_remap_manifest_reply(
        region_id: RegionId,
        RemapManifestReply {
            exists,
            manifest_paths,
            error,
        }: RemapManifestReply,
        now: &Instant,
        peer: &Peer,
    ) -> Result<HashMap<RegionId, String>> {
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
                    "Failed to remap manifest on datanode {:?}, error: {:?}, elapsed: {:?}",
                    peer,
                    error,
                    now.elapsed()
                ),
            }
            .fail();
        }

        Ok(manifest_paths)
    }
}
