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

use std::collections::HashMap;
use std::time::Instant;

use api::v1::meta::MailboxMessage;
use common_error::ext::ErrorExt;
use common_meta::instruction::{Instruction, InstructionReply};
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::warn;
use common_time::util as time_util;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::error::{self, Error, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::repartition::group::Context;
use crate::service::mailbox::{Channel, MailboxRef};

/// Groups the region routes by the leader peer.
///
/// # Panics
///
/// Panics if the leader peer is not set for any of the region routes.
pub(crate) fn group_region_routes_by_peer(
    region_routes: &[RegionRoute],
) -> HashMap<&Peer, Vec<RegionId>> {
    let mut map: HashMap<&Peer, Vec<RegionId>> = HashMap::new();
    for region_route in region_routes {
        map.entry(region_route.leader_peer.as_ref().unwrap())
            .or_default()
            .push(region_route.region.id);
    }
    map
}

/// Returns `true` if all results are successful.
fn all_successful(results: &[Result<()>]) -> bool {
    results.iter().all(Result::is_ok)
}

pub enum HandleMultipleResult<'a> {
    AllSuccessful,
    AllRetryable(Vec<(usize, &'a Error)>),
    PartialRetryable {
        retryable_errors: Vec<(usize, &'a Error)>,
        non_retryable_errors: Vec<(usize, &'a Error)>,
    },
    AllNonRetryable(Vec<(usize, &'a Error)>),
}

/// Evaluates results from multiple operations and categorizes errors by retryability.
///
/// If all operations succeed, returns `AllSuccessful`.
/// If all errors are retryable, returns `AllRetryable`.
/// If all errors are non-retryable, returns `AllNonRetryable`.
/// Otherwise, returns `PartialRetryable` with separate collections for retryable and non-retryable errors.
pub(crate) fn handle_multiple_results<'a>(results: &'a [Result<()>]) -> HandleMultipleResult<'a> {
    if all_successful(results) {
        return HandleMultipleResult::AllSuccessful;
    }

    let mut retryable_errors = Vec::new();
    let mut non_retryable_errors = Vec::new();
    for (index, result) in results.iter().enumerate() {
        if let Err(error) = result {
            if error.is_retryable() {
                retryable_errors.push((index, error));
            } else {
                non_retryable_errors.push((index, error));
            }
        }
    }

    match (retryable_errors.is_empty(), non_retryable_errors.is_empty()) {
        (true, false) => HandleMultipleResult::AllNonRetryable(non_retryable_errors),
        (false, true) => HandleMultipleResult::AllRetryable(retryable_errors),
        (false, false) => HandleMultipleResult::PartialRetryable {
            retryable_errors,
            non_retryable_errors,
        },
        // Should not happen, but include for completeness
        (true, true) => HandleMultipleResult::AllSuccessful,
    }
}

/// Verifies that every source of a **merge** holds only the files it created itself, before the
/// procedure publishes the new routes.
///
/// The remap-time guard in mito2 refuses the same state, but it fires inside `RemapManifest`,
/// which runs *after* `UpdateMetadata::ApplyStaging` has already published the target write
/// routes: an acknowledged write could then land in a region the rollback no longer routes
/// correctly. This asks each merge source leader first, with a validate-only remap that only
/// reads the region's persisted manifest, so the refusal happens while the old routes still apply.
///
/// A one-to-many split is exempt: every target receives only that source's files and no target
/// combines copies, so the check is skipped unless two or more sources feed the same target.
pub(crate) async fn validate_merge_sources(ctx: &Context) -> Result<()> {
    // Only a target fed by two or more distinct sources can publish one object twice, so only the
    // sources that feed such a target are checked. The remap-time guard exempts exactly the other
    // shapes through the same predicate, so a repartition this check lets through cannot be
    // refused after its routes are published.
    let checked_sources =
        store_api::region_engine::sources_sharing_a_target(&ctx.persistent_ctx.region_mapping);
    if checked_sources.is_empty() {
        return Ok(());
    }
    let Some(prepare_result) = ctx.persistent_ctx.group_prepare_result.as_ref() else {
        // The prepare result is captured by `RepartitionStart` before any edge into
        // `UpdateMetadata::ApplyStaging`, so it is always present here.
        return Ok(());
    };

    let operation_timeout = ctx
        .next_operation_timeout()
        .context(error::ExceededDeadlineSnafu {
            operation: "Validate merge sources",
        })?;

    let mut requests = Vec::with_capacity(checked_sources.len());
    for &region_id in &checked_sources {
        // A source without a route would silently go unchecked, so refuse to guess.
        let route = prepare_result
            .source_routes
            .iter()
            .find(|route| route.region.id == region_id)
            .context(error::UnexpectedSnafu {
                violated: format!("No source route for region: {region_id}"),
            })?;
        let peer = route.leader_peer.clone().context(error::UnexpectedSnafu {
            violated: format!("Leader peer is not set for source region: {region_id}"),
        })?;
        requests.push((region_id, peer));
    }

    let mut pending = Vec::with_capacity(requests.len());
    for (region_id, peer) in requests {
        let instruction = build_validate_only_instruction(ctx, region_id)?;
        pending.push(send_validate_only(
            ctx.mailbox.clone(),
            ctx.server_addr.clone(),
            peer,
            instruction,
            operation_timeout,
        ));
    }
    let results = futures::future::join_all(pending).await;

    let mut offenders = Vec::new();
    for (region_id, result) in checked_sources.into_iter().zip(results) {
        match result {
            Ok(()) => {}
            // A retryable failure - a mailbox timeout, a channel that closed, a region that is
            // not a leader yet - says nothing about which region owns what. It reaches the
            // procedure as retryable, so it is retried instead of turning into a terminal
            // "compact the region" refusal.
            Err(err) if err.retry_hint().is_retryable() => return Err(err),
            Err(err) => offenders.push(format!("region {region_id}: {err}")),
        }
    }

    if offenders.is_empty() {
        return Ok(());
    }
    error::UnexpectedSnafu {
        violated: format!(
            "The repartition refuses to merge regions that hold files another region created, \
             before the new routes are published: {}. Compact the offending regions and retry.",
            offenders.join("; ")
        ),
    }
    .fail()
}

/// Builds the validate-only remap instruction for one source region.
///
/// It carries the real target expressions so that an *older* datanode, which ignores
/// [`common_meta::instruction::RemapManifest::validate_only`], performs an ordinary remap: that
/// requires the region to be in staging state and fails with the structured stage-required
/// error, so metasrv sees an error instead of a silent success.
fn build_validate_only_instruction(
    ctx: &Context,
    region_id: RegionId,
) -> Result<common_meta::instruction::RemapManifest> {
    let new_partition_exprs = ctx
        .persistent_ctx
        .targets
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
        .collect::<Result<HashMap<RegionId, String>>>()?;
    // The mapping is the one the remap will use, so the datanode is asked about the exact shape
    // it would be asked to publish; only `validate_only` keeps it from writing.
    let targets = ctx
        .persistent_ctx
        .region_mapping
        .get(&region_id)
        .cloned()
        .context(error::UnexpectedSnafu {
            violated: format!("No target regions in the mapping for source region: {region_id}"),
        })?;
    Ok(common_meta::instruction::RemapManifest {
        region_id,
        input_regions: vec![region_id],
        region_mapping: HashMap::from([(region_id, targets)]),
        new_partition_exprs,
        validate_only: true,
    })
}

/// Sends one validate-only remap to `peer` and reduces its reply to `Ok(())` or an error carrying
/// the datanode's own message.
async fn send_validate_only(
    mailbox: MailboxRef,
    server_addr: String,
    peer: Peer,
    instruction: common_meta::instruction::RemapManifest,
    timeout: std::time::Duration,
) -> Result<()> {
    let region_id = instruction.region_id;
    let ch = Channel::Datanode(peer.id);
    let instruction = Instruction::RemapManifest(instruction);
    let tracing_ctx = TracingContext::from_current_span();
    let message = MailboxMessage::json_message(
        &format!("Validate merge source, region: {region_id}"),
        &format!("Metasrv@{server_addr}"),
        &format!("Datanode-{}@{}", peer.id, peer.addr),
        time_util::current_time_millis(),
        &instruction,
        Some(tracing_ctx.to_w3c()),
    )
    .with_context(|_| error::SerializeToJsonSnafu {
        input: instruction.to_string(),
    })?;
    let now = Instant::now();
    let receiver = mailbox.send(&ch, message, timeout).await.map_err(|e| {
        error::RetryLaterSnafu {
            reason: format!(
                "Failed to validate source region {region_id} on datanode {peer:?}: {e}"
            ),
        }
        .build()
    })?;
    // Waiting for the reply can time out or find its channel closed. Neither says anything
    // about which region owns what, so both stay retryable instead of becoming a refusal.
    let msg = receiver.await.map_err(|e| {
        error::RetryLaterSnafu {
            reason: format!(
                "Source region {region_id} on datanode {peer:?} did not answer the validation: {e}"
            ),
        }
        .build()
    })?;
    let reply = HeartbeatMailbox::json_reply(&msg)?;
    let InstructionReply::RemapManifest(reply) = reply else {
        return error::UnexpectedInstructionReplySnafu {
            mailbox_message: msg.to_string(),
            reason: "expect remap manifest reply",
        }
        .fail();
    };
    if !reply.exists {
        return error::UnexpectedSnafu {
            violated: format!(
                "Source region {region_id} does not exist on datanode {peer:?}, elapsed: {:?}",
                now.elapsed()
            ),
        }
        .fail();
    }
    if let Some(error) = reply.error {
        if error.retry_hint.is_retryable() {
            return error::RetryLaterSnafu {
                reason: format!(
                    "Failed to validate merge source {region_id} on datanode {peer:?}: {error}"
                ),
            }
            .fail();
        }
        warn!(
            "Refusing to merge source region {} on datanode {:?}: {}",
            region_id, peer, error
        );
        return error::UnexpectedSnafu {
            violated: error.message,
        }
        .fail();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::meta::MailboxMessage;
    use api::v1::meta::mailbox_message::Payload;
    use common_error::ext::{ErrorExt, RetryHint};
    use common_error::status_code::StatusCode;
    use common_meta::instruction::{
        Instruction, InstructionError, InstructionReply, RemapManifestReply,
    };
    use common_meta::peer::Peer;
    use common_meta::rpc::router::RegionRoute;
    use common_time::util::current_time_millis;
    use store_api::storage::RegionId;

    use crate::handler::HeartbeatMailbox;
    use crate::procedure::repartition::group::GroupPrepareResult;
    use crate::procedure::repartition::group::utils::validate_merge_sources;
    use crate::procedure::repartition::plan::{SourceRegionDescriptor, TargetRegionDescriptor};
    use crate::procedure::repartition::test_util::{
        TestingEnv, new_persistent_context, range_expr, test_region_route,
    };
    use crate::service::mailbox::Channel;

    const TABLE_ID: u32 = 1024;

    /// A merge of two source regions, each a leader on its own datanode, into one target.
    /// The `region_mapping` production builds from the plan's `transition_map`
    /// (`dispatch::build_region_mapping`): for each source, the targets whose domains it overlaps.
    fn mapping(
        pairs: &[(RegionId, Vec<RegionId>)],
    ) -> std::collections::HashMap<RegionId, Vec<RegionId>> {
        pairs.iter().cloned().collect()
    }

    fn merge_sources() -> (
        Vec<SourceRegionDescriptor>,
        Vec<TargetRegionDescriptor>,
        GroupPrepareResult,
    ) {
        let left = RegionId::new(TABLE_ID, 1);
        let right = RegionId::new(TABLE_ID, 2);
        let target = RegionId::new(TABLE_ID, 3);
        let sources = vec![
            SourceRegionDescriptor::partitioned(left, range_expr("x", 0, 50)),
            SourceRegionDescriptor::partitioned(right, range_expr("x", 50, 100)),
        ];
        let targets = vec![TargetRegionDescriptor {
            region_id: target,
            partition_expr: range_expr("x", 0, 100),
        }];
        let prepare_result = GroupPrepareResult {
            source_routes: vec![
                RegionRoute {
                    leader_peer: Some(Peer::empty(1)),
                    ..test_region_route(left, "col(x) >= 0 AND col(x) < 50")
                },
                RegionRoute {
                    leader_peer: Some(Peer::empty(2)),
                    ..test_region_route(right, "col(x) >= 50 AND col(x) < 100")
                },
            ],
            target_routes: vec![test_region_route(target, "col(x) >= 0 AND col(x) < 100")],
            central_region: left,
            central_region_datanode: Peer::empty(1),
        };
        (sources, targets, prepare_result)
    }

    fn remap_manifest_reply(id: u64, error: Option<InstructionError>) -> MailboxMessage {
        MailboxMessage {
            id,
            subject: "mock".to_string(),
            from: "datanode".to_string(),
            to: "meta".to_string(),
            timestamp_millis: current_time_millis(),
            payload: Some(Payload::Json(
                serde_json::to_string(&InstructionReply::RemapManifest(RemapManifestReply {
                    exists: true,
                    manifest_paths: HashMap::new(),
                    error,
                }))
                .unwrap(),
            )),
            header: None,
        }
    }

    #[test]
    fn test_validate_only_instruction_is_judged_with_the_target_expressions() {
        let env = TestingEnv::new();
        let (sources, targets, prepare_result) = merge_sources();
        let mut persistent = new_persistent_context(TABLE_ID, sources, targets.clone());
        persistent.group_prepare_result = Some(prepare_result);
        persistent.region_mapping = mapping(&[
            (RegionId::new(TABLE_ID, 1), vec![targets[0].region_id]),
            (RegionId::new(TABLE_ID, 2), vec![targets[0].region_id]),
        ]);
        let ctx = env.create_context(persistent);

        let instruction =
            super::build_validate_only_instruction(&ctx, RegionId::new(TABLE_ID, 1)).unwrap();
        assert!(instruction.validate_only);
        assert_eq!(instruction.region_id, RegionId::new(TABLE_ID, 1));
        assert_eq!(instruction.input_regions, vec![RegionId::new(TABLE_ID, 1)]);
        let expected_expr = targets[0].partition_expr.as_json_str().unwrap();
        assert_eq!(
            instruction.new_partition_exprs.get(&targets[0].region_id),
            Some(&expected_expr)
        );
    }

    #[tokio::test]
    async fn test_validate_merge_sources_refuses_before_the_routes_are_published() {
        // A source holds a file another region created. The check runs before the procedure
        // publishes the new routes, so on failure the published route - the one that still
        // accepts writes - has to be untouched, and the error has to name the offending region.
        let mut env = TestingEnv::new();
        let (sources, targets, prepare_result) = merge_sources();
        let mapping = mapping(&[
            (RegionId::new(TABLE_ID, 1), vec![RegionId::new(TABLE_ID, 3)]),
            (RegionId::new(TABLE_ID, 2), vec![RegionId::new(TABLE_ID, 3)]),
        ]);
        let old_routes = prepare_result.source_routes.clone();
        env.create_physical_table_metadata(TABLE_ID, old_routes.clone())
            .await;

        let (left_tx, mut left_rx) = tokio::sync::mpsc::channel(2);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), left_tx)
            .await;
        let (right_tx, mut right_rx) = tokio::sync::mpsc::channel(2);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(2), right_tx)
            .await;

        let mut persistent = new_persistent_context(TABLE_ID, sources, targets);
        persistent.group_prepare_result = Some(prepare_result);
        persistent.region_mapping = mapping;
        let ctx = env.create_context(persistent);
        let mailbox = ctx.mailbox.clone();

        let offender_file = "0a1b2c3d-0000-4000-8000-000000000001";
        let datanodes = async {
            let mut messages = Vec::new();
            messages.push((1u32, left_rx.recv().await.unwrap().unwrap()));
            messages.push((2u32, right_rx.recv().await.unwrap().unwrap()));
            for (region_number, message) in messages {
                let msg = message.mailbox_message.unwrap();
                let Instruction::RemapManifest(instruction) =
                    HeartbeatMailbox::json_instruction(&msg).unwrap()
                else {
                    panic!("expected a remap manifest instruction");
                };
                // Every source is asked about its own region, and only validates.
                assert!(instruction.validate_only);
                assert_eq!(
                    instruction.region_id,
                    RegionId::new(TABLE_ID, region_number)
                );
                let error = (region_number == 1).then(|| InstructionError {
                    code: StatusCode::IllegalState,
                    message: format!(
                        "Region {}({region_number}, 1) holds file {offender_file} that another region created",
                        TABLE_ID
                    ),
                    retry_hint: RetryHint::NonRetryable,
                });
                let ack = remap_manifest_reply(msg.id, error);
                mailbox.on_recv(msg.id, Ok(ack)).await.unwrap();
            }
        };

        let (result, ()) = tokio::join!(validate_merge_sources(&ctx), datanodes);
        let err = result.unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains(&RegionId::new(TABLE_ID, 1).to_string()),
            "{message}"
        );
        assert!(message.contains(offender_file), "{message}");
        assert!(
            message.contains("before the new routes are published"),
            "{message}"
        );

        let published = ctx.get_table_route_value().await.unwrap();
        assert_eq!(published.into_inner().region_routes().unwrap(), &old_routes);
    }

    #[tokio::test]
    async fn test_validate_merge_sources_keeps_a_timeout_retryable() {
        // The datanodes never answer: delivery times out. A timeout says nothing about region
        // ownership, so the procedure has to retry - advising the operator to compact a region
        // would be wrong advice and a terminal failure.
        let mut env = TestingEnv::new();
        let (sources, targets, prepare_result) = merge_sources();
        let (left_tx, mut left_rx) = tokio::sync::mpsc::channel(2);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), left_tx)
            .await;
        let (right_tx, mut right_rx) = tokio::sync::mpsc::channel(2);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(2), right_tx)
            .await;

        let mut persistent = new_persistent_context(TABLE_ID, sources, targets);
        persistent.group_prepare_result = Some(prepare_result);
        persistent.region_mapping = mapping(&[
            (RegionId::new(TABLE_ID, 1), vec![RegionId::new(TABLE_ID, 3)]),
            (RegionId::new(TABLE_ID, 2), vec![RegionId::new(TABLE_ID, 3)]),
        ]);
        let ctx = env.create_context(persistent);
        let mailbox = ctx.mailbox.clone();

        let datanodes = async {
            for rx in [&mut left_rx, &mut right_rx] {
                let msg = rx.recv().await.unwrap().unwrap().mailbox_message.unwrap();
                mailbox
                    .on_recv(
                        msg.id,
                        Err(crate::error::MailboxTimeoutSnafu { id: msg.id }.build()),
                    )
                    .await
                    .unwrap();
            }
        };

        let (result, ()) = tokio::join!(validate_merge_sources(&ctx), datanodes);
        let err = result.unwrap_err();
        assert!(
            err.retry_hint().is_retryable(),
            "a mailbox timeout must stay retryable: {err}"
        );
        let message = err.to_string();
        assert!(
            !message.contains("Compact the offending regions"),
            "a timeout is not an ownership refusal: {message}"
        );
    }

    #[tokio::test]
    async fn test_validate_merge_sources_asks_each_source_of_a_shared_target_once() {
        // A many-to-one group: both sources feed the same target, so both are asked - exactly
        // once each - and a clean answer lets the group through.
        let mut env = TestingEnv::new();
        let (sources, targets, prepare_result) = merge_sources();
        let (left_tx, mut left_rx) = tokio::sync::mpsc::channel(2);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(1), left_tx)
            .await;
        let (right_tx, mut right_rx) = tokio::sync::mpsc::channel(2);
        env.mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(2), right_tx)
            .await;

        let mut persistent = new_persistent_context(TABLE_ID, sources, targets);
        persistent.group_prepare_result = Some(prepare_result);
        persistent.region_mapping = mapping(&[
            (RegionId::new(TABLE_ID, 1), vec![RegionId::new(TABLE_ID, 3)]),
            (RegionId::new(TABLE_ID, 2), vec![RegionId::new(TABLE_ID, 3)]),
        ]);
        let ctx = env.create_context(persistent);
        let mailbox = ctx.mailbox.clone();

        let datanodes = async {
            let mut asked = Vec::new();
            for rx in [&mut left_rx, &mut right_rx] {
                let msg = rx.recv().await.unwrap().unwrap().mailbox_message.unwrap();
                let Instruction::RemapManifest(instruction) =
                    HeartbeatMailbox::json_instruction(&msg).unwrap()
                else {
                    panic!("expected a remap manifest instruction");
                };
                assert!(instruction.validate_only);
                asked.push(instruction.region_id);
                // The instruction carries the shape the remap would use: this source -> the
                // shared target.
                assert_eq!(
                    instruction.region_mapping[&instruction.region_id],
                    vec![RegionId::new(TABLE_ID, 3)]
                );
                let ack = remap_manifest_reply(msg.id, None);
                mailbox.on_recv(msg.id, Ok(ack)).await.unwrap();
            }
            asked
        };

        let (result, asked) = tokio::join!(validate_merge_sources(&ctx), datanodes);
        result.unwrap();
        assert_eq!(
            asked,
            vec![RegionId::new(TABLE_ID, 1), RegionId::new(TABLE_ID, 2)],
            "one request per source of the shared target"
        );
        // Nothing further is asked.
        assert!(left_rx.try_recv().is_err());
        assert!(right_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_validate_merge_sources_skips_shapes_no_target_combines() {
        // A split (one source, several targets) and a one-to-one repartition (one source, one
        // target, region id reused) both hand every target a single source's files: no target
        // combines two file sets, so the procedure asks nothing. The remap guard exempts the same
        // shapes through the same predicate, so neither can be refused after publication.
        for shape in ["split", "one-to-one"] {
            let mut env = TestingEnv::new();
            let source = RegionId::new(TABLE_ID, 1);
            let second = RegionId::new(TABLE_ID, 2);
            let (_, _, prepare_result) = merge_sources();
            let (targets, mapping) = if shape == "split" {
                (
                    vec![
                        TargetRegionDescriptor {
                            region_id: second,
                            partition_expr: range_expr("x", 0, 50),
                        },
                        TargetRegionDescriptor {
                            region_id: RegionId::new(TABLE_ID, 3),
                            partition_expr: range_expr("x", 50, 100),
                        },
                    ],
                    mapping(&[(source, vec![second, RegionId::new(TABLE_ID, 3)])]),
                )
            } else {
                (
                    vec![TargetRegionDescriptor {
                        region_id: source,
                        partition_expr: range_expr("x", 0, 100),
                    }],
                    mapping(&[(source, vec![source])]),
                )
            };
            let mut persistent = new_persistent_context(
                TABLE_ID,
                vec![SourceRegionDescriptor::partitioned(
                    source,
                    range_expr("x", 0, 100),
                )],
                targets,
            );
            persistent.group_prepare_result = Some(GroupPrepareResult {
                source_routes: prepare_result.source_routes[..1].to_vec(),
                ..prepare_result
            });
            persistent.region_mapping = mapping;
            let (tx, mut rx) = tokio::sync::mpsc::channel(2);
            env.mailbox_ctx
                .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
                .await;
            let ctx = env.create_context(persistent);

            validate_merge_sources(&ctx)
                .await
                .unwrap_or_else(|e| panic!("{shape} must not be checked: {e}"));
            assert!(
                rx.try_recv().is_err(),
                "{shape} must not send an instruction"
            );
        }
    }
}
