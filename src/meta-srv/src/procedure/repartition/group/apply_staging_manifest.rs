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
use common_telemetry::tracing_context::TracingContext;
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
use crate::procedure::repartition::plan::TargetRegionDescriptor;
use crate::procedure::utils::instruction_error_result;
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
        targets: &[TargetRegionDescriptor],
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
        let tracing_ctx = TracingContext::from_current_span();
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
            Some(tracing_ctx.to_w3c()),
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
            },
            Err(error::Error::MailboxChannelClosed {..})=> error::RetryLaterSnafu {
                reason: format!(
                    "Mailbox closed when sending apply staging manifests to datanode {:?}, elapsed: {:?}",
                    peer,
                    now.elapsed()
                ),
            }.fail()?,
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

        if let Some(error) = error {
            return instruction_error_result(
                error,
                format!(
                    "Failed to apply staging manifest on datanode {:?}, error: {:?}, elapsed: {:?}",
                    peer,
                    error,
                    now.elapsed()
                ),
            );
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_meta::instruction::ApplyStagingManifest as ApplyStagingManifestInstruction;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_procedure::Procedure;
    use store_api::storage::RegionId;

    use crate::procedure::repartition::group::apply_staging_manifest::ApplyStagingManifest;
    use crate::procedure::repartition::group::{
        GroupPrepareResult, PersistentContext, RepartitionGroupProcedure,
    };
    use crate::procedure::repartition::plan::{SourceRegionDescriptor, TargetRegionDescriptor};
    use crate::procedure::repartition::test_util::{
        TestingEnv, new_persistent_context, range_expr,
    };

    /// Pins the remap -> apply handoff across a persisted boundary: a remap has
    /// finished and recorded `staging_manifest_paths` in the group's
    /// `PersistentContext`, and a (new) metasrv resumes the group at
    /// [`ApplyStagingManifest`]. The state is dumped and restored exactly as a
    /// restarted leader restores it, and the resumed step must address every target
    /// with the path the remap recorded rather than computing its own.
    ///
    /// Version skew: an old (pre-`owned_range`) node that finishes a remap records
    /// `staging_manifest_paths` whose entries name manifest files without an
    /// `owned_range`. Such output must not be applied by a node that requires the
    /// field, and `mito2` refuses it at install time
    /// (`test_remap_install_without_an_owned_range_is_refused` in
    /// `src/mito2/src/worker/handle_apply_staging.rs`; not duplicated here). The
    /// recorded paths outlive the remapping node, so a repartition resumed after a
    /// partial upgrade reaches exactly that refusal - which is why every node must be
    /// upgraded before a repartition is resumed.
    #[test]
    fn test_resumed_apply_staging_manifest_uses_the_persisted_paths() {
        let table_id = 1024;
        let central_region = RegionId::new(table_id, 1);
        let targets = vec![
            TargetRegionDescriptor {
                region_id: central_region,
                partition_expr: range_expr("x", 0, 10),
            },
            TargetRegionDescriptor {
                region_id: RegionId::new(table_id, 2),
                partition_expr: range_expr("x", 10, 20),
            },
            TargetRegionDescriptor {
                region_id: RegionId::new(table_id, 3),
                partition_expr: range_expr("x", 20, 30),
            },
        ];
        // The central region stays on peer 1, the two new regions land on peers 2
        // and 3.
        let target_routes = targets
            .iter()
            .enumerate()
            .map(|(idx, target)| RegionRoute {
                region: Region {
                    id: target.region_id,
                    partition_expr: target.partition_expr.as_json_str().unwrap(),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(idx as u64 + 1)),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let source_routes = vec![RegionRoute {
            region: Region {
                id: central_region,
                partition_expr: range_expr("x", 0, 30).as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];

        // The boundary an old node leaves behind once its remap finished: the paths
        // exist only in the persisted group context.
        let staging_manifest_paths = HashMap::from([
            (central_region, "manifests/region-1.manifest".to_string()),
            (
                RegionId::new(table_id, 2),
                "manifests/region-2.manifest".to_string(),
            ),
            (
                RegionId::new(table_id, 3),
                "manifests/region-3.manifest".to_string(),
            ),
        ]);
        let mut persistent_ctx = new_persistent_context(
            table_id,
            vec![SourceRegionDescriptor::partitioned(
                central_region,
                range_expr("x", 0, 30),
            )],
            targets.clone(),
        );
        persistent_ctx.group_prepare_result = Some(GroupPrepareResult {
            source_routes,
            target_routes: target_routes.clone(),
            central_region,
            central_region_datanode: Peer::empty(1),
        });
        persistent_ctx.staging_manifest_paths = staging_manifest_paths.clone();

        // The context decodes back to the same paths, so what the apply step reads is
        // what the remap wrote.
        let encoded = serde_json::to_string(&persistent_ctx).unwrap();
        let decoded: PersistentContext = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, persistent_ctx);
        assert_eq!(decoded.staging_manifest_paths, staging_manifest_paths);

        // A restarted metasrv loads the persisted group and resumes at this state.
        let env = TestingEnv::new();
        let procedure = RepartitionGroupProcedure {
            state: Box::new(ApplyStagingManifest),
            context: env.create_context(persistent_ctx),
        };
        let resumed =
            RepartitionGroupProcedure::from_json(&procedure.dump().unwrap(), |persistent_ctx| {
                env.create_context(persistent_ctx)
            })
            .unwrap();
        assert!(resumed.state.as_any().is::<ApplyStagingManifest>());
        assert_eq!(
            resumed.context.persistent_ctx.staging_manifest_paths,
            staging_manifest_paths
        );

        let resumed_ctx = &resumed.context.persistent_ctx;
        // Safety: set by the `RepartitionStart` state, restored above.
        let prepare_result = resumed_ctx.group_prepare_result.as_ref().unwrap();
        let built = ApplyStagingManifest::build_apply_staging_manifest_instructions(
            &resumed_ctx.staging_manifest_paths,
            &prepare_result.target_routes,
            &resumed_ctx.targets,
            prepare_result.central_region,
        )
        .unwrap();

        // The central region is addressed on its own, every other target through the
        // per-peer list.
        let mut by_region: HashMap<RegionId, (Peer, ApplyStagingManifestInstruction)> =
            HashMap::with_capacity(resumed_ctx.targets.len());
        for (peer, instructions) in &built.instructions {
            assert!(!instructions.is_empty());
            for instruction in instructions {
                assert_ne!(instruction.region_id, central_region);
                assert!(
                    by_region
                        .insert(instruction.region_id, (peer.clone(), instruction.clone()))
                        .is_none(),
                    "region {} received more than one apply instruction",
                    instruction.region_id
                );
            }
        }
        let (central_peer, central_instruction) = built
            .central_region_instruction
            .expect("the central region is a target, so it is addressed separately");
        assert_eq!(central_instruction.region_id, central_region);
        assert!(
            by_region
                .insert(central_region, (central_peer, central_instruction))
                .is_none()
        );

        // One instruction per target, each carrying the path the remap recorded, each
        // sent to the peer that leads the region.
        assert_eq!(by_region.len(), resumed_ctx.targets.len());
        for target in &resumed_ctx.targets {
            let (peer, instruction) = &by_region[&target.region_id];
            assert_eq!(
                instruction.manifest_path,
                staging_manifest_paths[&target.region_id]
            );
            assert_eq!(
                instruction.partition_expr,
                target.partition_expr.as_json_str().unwrap()
            );
            assert_eq!(instruction.central_region_id, central_region);
            let route = target_routes
                .iter()
                .find(|route| route.region.id == target.region_id)
                .unwrap();
            assert_eq!(Some(peer), route.leader_peer.as_ref());
        }
    }
}
