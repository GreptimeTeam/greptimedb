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
use common_error::ext::BoxedError;
use common_meta::distributed_time_constants::default_distributed_time_constants;
use common_meta::instruction::{
    DowngradeRegion, DowngradeRegionReply, DowngradeRegionsReply, Instruction, InstructionReply,
};
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use tokio::time::{Instant, sleep};

use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::region_migration::update_metadata::UpdateMetadata;
use crate::procedure::region_migration::upgrade_candidate_region::UpgradeCandidateRegion;
use crate::procedure::region_migration::{Context, State};
use crate::procedure::utils::instruction_error_result;
use crate::service::mailbox::Channel;

#[derive(Debug, Serialize, Deserialize)]
pub struct DowngradeLeaderRegion {
    // The optimistic retry times.
    optimistic_retry: usize,
    // The retry initial interval.
    retry_initial_interval: Duration,
}

impl Default for DowngradeLeaderRegion {
    fn default() -> Self {
        Self {
            optimistic_retry: 3,
            retry_initial_interval: Duration::from_millis(500),
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for DowngradeLeaderRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let now = Instant::now();
        // Ensures the `leader_region_lease_deadline` must exist after recovering.
        ctx.volatile_ctx
            .set_leader_region_lease_deadline(default_distributed_time_constants().region_lease);

        match self.downgrade_region_with_retry(ctx).await {
            Ok(_) => {
                // Do nothing
                info!(
                    "Downgraded region leader success, region: {:?}",
                    ctx.persistent_ctx.region_ids
                );
            }
            Err(error::Error::ExceededDeadline { .. }) => {
                info!(
                    "Downgrade region leader exceeded deadline, region: {:?}",
                    ctx.persistent_ctx.region_ids
                );
                // Rollbacks the metadata if procedure is timeout
                return Ok((Box::new(UpdateMetadata::Rollback), Status::executing(false)));
            }
            Err(err) => {
                error!(err; "Occurs non-retryable error, region: {:?}", ctx.persistent_ctx.region_ids);
                if let Some(deadline) = ctx.volatile_ctx.leader_region_lease_deadline.as_ref() {
                    info!(
                        "Running into the downgrade region leader slow path, region: {:?}, sleep until {:?}",
                        ctx.persistent_ctx.region_ids, deadline
                    );
                    tokio::time::sleep_until(*deadline).await;
                } else {
                    warn!(
                        "Leader region lease deadline is not set, region: {:?}",
                        ctx.persistent_ctx.region_ids
                    );
                }
            }
        }
        ctx.update_downgrade_leader_region_elapsed(now);

        Ok((
            Box::new(UpgradeCandidateRegion::default()),
            Status::executing(false),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl DowngradeLeaderRegion {
    /// Builds downgrade region instruction.
    fn build_downgrade_region_instruction(
        &self,
        ctx: &Context,
        flush_timeout: Duration,
    ) -> Instruction {
        let region_ids = &ctx.persistent_ctx.region_ids;
        let mut downgrade_regions = Vec::with_capacity(region_ids.len());
        for region_id in region_ids {
            downgrade_regions.push(DowngradeRegion {
                region_id: *region_id,
                flush_timeout: Some(flush_timeout),
            });
        }

        Instruction::DowngradeRegions(downgrade_regions)
    }

    fn handle_downgrade_region_reply(
        &self,
        ctx: &mut Context,
        reply: &DowngradeRegionReply,
        now: &Instant,
    ) -> Result<()> {
        let leader = &ctx.persistent_ctx.from_peer;
        let DowngradeRegionReply {
            region_id,
            last_entry_id,
            metadata_last_entry_id,
            exists,
            error,
        } = reply;

        if let Some(error) = error {
            return instruction_error_result(
                error,
                format!(
                    "Failed to downgrade the region {} on datanode {:?}, error: {:?}, elapsed: {:?}",
                    region_id,
                    leader,
                    error,
                    now.elapsed()
                ),
            );
        }

        if !exists {
            warn!(
                "Trying to downgrade the region {} on datanode {:?}, but region doesn't exist!, elapsed: {:?}",
                region_id,
                leader,
                now.elapsed()
            );
        } else {
            info!(
                "Region {} leader is downgraded on datanode {:?}, last_entry_id: {:?}, metadata_last_entry_id: {:?}, elapsed: {:?}",
                region_id,
                leader,
                last_entry_id,
                metadata_last_entry_id,
                now.elapsed()
            );
        }

        if let Some(last_entry_id) = last_entry_id {
            debug!(
                "set last_entry_id: {:?}, region_id: {:?}",
                last_entry_id, region_id
            );
            ctx.volatile_ctx
                .set_last_entry_id(*region_id, *last_entry_id);
        }

        if let Some(metadata_last_entry_id) = metadata_last_entry_id {
            ctx.volatile_ctx
                .set_metadata_last_entry_id(*region_id, *metadata_last_entry_id);
        }

        Ok(())
    }

    /// Tries to downgrade a leader region.
    ///
    /// Retry:
    /// - [MailboxTimeout](error::Error::MailboxTimeout), Timeout.
    /// - Failed to downgrade region on the Datanode.
    ///
    /// Abort:
    /// - [PusherNotFound](error::Error::PusherNotFound), The datanode is unreachable.
    /// - [PushMessage](error::Error::PushMessage), The receiver is dropped.
    /// - [MailboxReceiver](error::Error::MailboxReceiver), The sender is dropped without sending (impossible).
    /// - [UnexpectedInstructionReply](error::Error::UnexpectedInstructionReply).
    /// - [ExceededDeadline](error::Error::ExceededDeadline)
    /// - Invalid JSON.
    async fn downgrade_region(&self, ctx: &mut Context) -> Result<()> {
        let region_ids = &ctx.persistent_ctx.region_ids;
        let operation_timeout =
            ctx.next_operation_timeout()
                .context(error::ExceededDeadlineSnafu {
                    operation: "Downgrade region",
                })?;
        let downgrade_instruction = self.build_downgrade_region_instruction(ctx, operation_timeout);

        let leader = &ctx.persistent_ctx.from_peer;
        let tracing_ctx = TracingContext::from_current_span();
        let msg = MailboxMessage::json_message(
            &format!("Downgrade leader regions: {:?}", region_ids),
            &format!("Metasrv@{}", ctx.server_addr()),
            &format!("Datanode-{}@{}", leader.id, leader.addr),
            common_time::util::current_time_millis(),
            &downgrade_instruction,
            Some(tracing_ctx.to_w3c()),
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: downgrade_instruction.to_string(),
        })?;

        let ch = Channel::Datanode(leader.id);
        let now = Instant::now();
        let receiver = ctx.mailbox.send(&ch, msg, operation_timeout).await?;

        match receiver.await {
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                info!(
                    "Received downgrade region reply: {:?}, region: {:?}, elapsed: {:?}",
                    reply,
                    region_ids,
                    now.elapsed()
                );
                let InstructionReply::DowngradeRegions(DowngradeRegionsReply { replies }) = reply
                else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect downgrade region reply",
                    }
                    .fail();
                };

                for reply in replies {
                    self.handle_downgrade_region_reply(ctx, &reply, &now)?;
                }
                Ok(())
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for downgrade leader region {region_ids:?} on datanode {:?}, elapsed: {:?}",
                    leader,
                    now.elapsed()
                );
                error::RetryLaterSnafu { reason }.fail()
            }
            Err(err) => Err(err),
        }
    }

    /// Sets the deadline to wait for the region leases of the unreachable leader to expire.
    ///
    /// They expire one lease after this metasrv last granted them. A former metasrv leader is
    /// assumed to grant none later than [`META_LEASE_SECS`](common_meta::distributed_time_constants::META_LEASE_SECS)
    /// after this metasrv became the leader, which the election backends do not enforce (see
    /// `LeaseGrantTracker`).
    fn update_leader_region_lease_deadline(&self, ctx: &mut Context) {
        let leader = &ctx.persistent_ctx.from_peer;
        let region_lease = default_distributed_time_constants().region_lease;
        let lease_expiry = ctx
            .lease_grant_tracker
            .lease_expiry(leader.id, region_lease);
        let lease_timeout = lease_expiry.saturating_duration_since(Instant::now());
        ctx.volatile_ctx.reset_leader_region_lease_deadline();
        ctx.volatile_ctx
            .set_leader_region_lease_deadline(lease_timeout);
        info!(
            "Datanode {} is unreachable, waits {:?} for its region leases to expire, region: {:?}",
            leader, lease_timeout, ctx.persistent_ctx.region_ids
        );
    }

    /// Extends the deadline to wait for, after the downgrade failed, to when the Leader grants
    /// of former metasrv leaders have expired.
    ///
    /// The leader may not have applied the downgrade. The deadline is already one lease after
    /// this step started, which covers this metasrv's Leader grants: it records a grant before
    /// it reads the route the grant comes from, so every Leader grant computed from the route
    /// before the region was marked as downgrading was recorded before this step started. Later
    /// grants are DowngradingLeader, which do not extend a region's Leader deadline.
    fn extend_leader_region_lease_deadline(&self, ctx: &mut Context) {
        let region_lease = default_distributed_time_constants().region_lease;
        let former_leader_expiry = ctx
            .lease_grant_tracker
            .former_leader_lease_expiry(region_lease);
        if let Some(deadline) = ctx.volatile_ctx.leader_region_lease_deadline.as_mut() {
            *deadline = (*deadline).max(former_leader_expiry);
        }
    }

    /// Downgrades a leader region.
    ///
    /// Fast path:
    /// - Waits for the reply of downgrade instruction.
    ///
    /// Slow path:
    /// - Waits for the lease of the leader region expired.
    ///
    /// Abort:
    /// - ExceededDeadline
    async fn downgrade_region_with_retry(&self, ctx: &mut Context) -> Result<()> {
        let mut retry = 0;

        loop {
            let timer = Instant::now();
            if let Err(err) = self.downgrade_region(ctx).await {
                ctx.update_operations_elapsed(timer);
                retry += 1;
                // Throws the error immediately if the procedure exceeded the deadline.
                if matches!(err, error::Error::ExceededDeadline { .. }) {
                    error!(err; "Failed to downgrade region leader, regions: {:?}, exceeded deadline", ctx.persistent_ctx.region_ids);
                    return Err(err);
                } else if matches!(err, error::Error::PusherNotFound { .. }) {
                    // Throws the error immediately if the datanode is unreachable.
                    error!(err; "Failed to downgrade region leader, regions: {:?}, datanode({}) is unreachable(PusherNotFound)", ctx.persistent_ctx.region_ids, ctx.persistent_ctx.from_peer.id);
                    self.update_leader_region_lease_deadline(ctx);
                    return Err(err);
                } else if err.is_retryable() && retry < self.optimistic_retry {
                    error!(err; "Failed to downgrade region leader, regions: {:?}, retry later", ctx.persistent_ctx.region_ids);
                    sleep(self.retry_initial_interval).await;
                } else {
                    self.extend_leader_region_lease_deadline(ctx);
                    return Err(BoxedError::new(err)).context(error::DowngradeLeaderSnafu {
                        // TODO(weny): handle multiple regions.
                        region_id: ctx.persistent_ctx.region_ids[0],
                    })?;
                }
            } else {
                ctx.update_operations_elapsed(timer);
                // Resets the deadline.
                ctx.volatile_ctx.reset_leader_region_lease_deadline();
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::collections::HashMap;

    use common_meta::distributed_time_constants::META_LEASE_SECS;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::wal_provider::RegionWalOptions;
    use store_api::storage::RegionId;
    use tokio::time::Instant;

    use super::*;
    use crate::error::Error;
    use crate::procedure::region_migration::test_util::{TestingEnv, new_procedure_context};
    use crate::procedure::region_migration::{
        ContextFactory, PersistentContext, RegionMigrationTriggerReason,
    };
    use crate::procedure::test_util::{
        new_close_region_reply, new_downgrade_region_reply, send_mock_reply,
    };

    fn new_persistent_context() -> PersistentContext {
        PersistentContext::new(
            vec![("greptime".into(), "public".into())],
            Peer::empty(1),
            Peer::empty(2),
            vec![RegionId::new(1024, 1)],
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Unknown,
        )
    }

    async fn prepare_table_metadata(ctx: &Context, wal_options: RegionWalOptions) {
        let region_id = ctx.persistent_ctx.region_ids[0];
        let table_info = new_test_table_info(region_id.table_id());
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(ctx.persistent_ctx.from_peer.clone()),
            follower_peers: vec![ctx.persistent_ctx.to_peer.clone()],
            ..Default::default()
        }];
        ctx.table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(region_routes),
                wal_options,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_datanode_is_unreachable() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::PusherNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_pusher_dropped() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        drop(rx);

        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::PushMessage { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_procedure_exceeded_deadline() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        ctx.volatile_ctx.metrics.operations_elapsed =
            ctx.persistent_ctx.timeout + Duration::from_secs(1);

        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::ExceededDeadline { .. });
        assert!(!err.is_retryable());

        let err = state
            .downgrade_region_with_retry(&mut ctx)
            .await
            .unwrap_err();
        assert_matches!(err, Error::ExceededDeadline { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_unexpected_instruction_reply() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        // Sends an incorrect reply.
        send_mock_reply(mailbox, rx, |id| Ok(new_close_region_reply(id)));

        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::UnexpectedInstructionReply { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_instruction_exceeded_deadline() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Err(error::MailboxTimeoutSnafu { id }.build())
        });

        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_downgrade_region_failed() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_downgrade_region_reply(
                id,
                None,
                false,
                Some("test mocked".to_string()),
            ))
        });

        let err = state.downgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
        assert!(format!("{err:?}").contains("test mocked"), "err: {err:?}",);
    }

    #[tokio::test]
    async fn test_downgrade_region_with_retry_fast_path() {
        let state = DowngradeLeaderRegion::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        common_runtime::spawn_global(async move {
            // retry: 0.
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Err(error::MailboxTimeoutSnafu { id: reply_id }.build()),
                )
                .await
                .unwrap();

            // retry: 1.
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Ok(new_downgrade_region_reply(reply_id, Some(1), true, None)),
                )
                .await
                .unwrap();
        });

        state.downgrade_region_with_retry(&mut ctx).await.unwrap();
        assert_eq!(
            ctx.volatile_ctx
                .leader_region_last_entry_ids
                .get(&RegionId::new(0, 0))
                .cloned(),
            Some(1)
        );
        assert!(ctx.volatile_ctx.leader_region_lease_deadline.is_none());
    }

    #[tokio::test]
    async fn test_downgrade_region_with_retry_slow_path() {
        let state = DowngradeLeaderRegion {
            optimistic_retry: 3,
            retry_initial_interval: Duration::from_millis(100),
        };
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        common_runtime::spawn_global(async move {
            for _ in 0..3 {
                let resp = rx.recv().await.unwrap().unwrap();
                let reply_id = resp.mailbox_message.unwrap().id;
                mailbox
                    .on_recv(
                        reply_id,
                        Err(error::MailboxTimeoutSnafu { id: reply_id }.build()),
                    )
                    .await
                    .unwrap();
            }
        });

        // Later than the expiry of the leases granted to the leader.
        ctx.volatile_ctx
            .set_leader_region_lease_deadline(Duration::from_secs(60));
        let expected_deadline = ctx.volatile_ctx.leader_region_lease_deadline.unwrap();
        let err = state
            .downgrade_region_with_retry(&mut ctx)
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::DowngradeLeader { .. });
        // assert_eq!(ctx.volatile_ctx.leader_region_last_entry_id, None);
        // Should remain no change.
        assert_eq!(
            ctx.volatile_ctx.leader_region_lease_deadline.unwrap(),
            expected_deadline
        )
    }

    #[tokio::test]
    async fn test_next_upgrade_candidate_state() {
        let mut state = Box::<DowngradeLeaderRegion>::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_downgrade_region_reply(id, Some(1), true, None))
        });

        let timer = Instant::now();
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();
        let elapsed = timer.elapsed().as_secs();
        let region_lease = default_distributed_time_constants().region_lease.as_secs();
        assert!(elapsed < region_lease / 2);
        assert_eq!(
            ctx.volatile_ctx
                .leader_region_last_entry_ids
                .get(&RegionId::new(0, 0))
                .cloned(),
            Some(1)
        );
        assert!(ctx.volatile_ctx.leader_region_lease_deadline.is_none());

        let _ = next
            .as_any()
            .downcast_ref::<UpgradeCandidateRegion>()
            .unwrap();
    }

    #[tokio::test]
    async fn test_downgrade_region_procedure_exceeded_deadline() {
        let mut state = Box::<UpgradeCandidateRegion>::default();
        state.retry_initial_interval = Duration::from_millis(100);
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();
        ctx.volatile_ctx.metrics.operations_elapsed =
            ctx.persistent_ctx.timeout + Duration::from_secs(1);

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_downgrade_region_reply(id, None, true, None))
        });
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();
        let update_metadata = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();
        assert_matches!(update_metadata, UpdateMetadata::Rollback);
    }

    /// Downgrades the leader, which is unreachable, and returns the deadline to wait until.
    async fn unreachable_leader_lease_deadline(ctx: &mut Context) -> Instant {
        let state = DowngradeLeaderRegion::default();
        let err = state.downgrade_region_with_retry(ctx).await.unwrap_err();
        assert_matches!(err, Error::PusherNotFound { .. });
        ctx.volatile_ctx.leader_region_lease_deadline.unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn test_unreachable_leader_waits_for_last_grant() {
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let region_lease = default_distributed_time_constants().region_lease;

        // The wait counts from the monotonic instant of the last grant.
        tokio::time::advance(Duration::from_secs(60)).await;
        ctx.lease_grant_tracker.record_grant(from_peer_id);
        let granted_at = Instant::now();
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(
            granted_at + region_lease,
            unreachable_leader_lease_deadline(&mut ctx).await
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_unreachable_leader_waits_after_leader_change() {
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let region_lease = default_distributed_time_constants().region_lease;

        ctx.lease_grant_tracker.record_grant(from_peer_id);
        // Another metasrv leads for a while, then this one is elected again. The other leader
        // may still grant leases for `META_LEASE_SECS` after the election.
        tokio::time::advance(Duration::from_secs(60)).await;
        ctx.lease_grant_tracker.reset();
        let leader_since = Instant::now();
        assert_eq!(
            leader_since + Duration::from_secs(META_LEASE_SECS) + region_lease,
            unreachable_leader_lease_deadline(&mut ctx).await
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_failed_downgrade_waits_after_leader_change() {
        let mut state = Box::<DowngradeLeaderRegion>::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;
        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let region_lease = default_distributed_time_constants().region_lease;

        // The leader has a heartbeat stream to this metasrv, but the downgrade instruction does
        // not reach it.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        env.mailbox_context()
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;
        drop(rx);

        // This metasrv has just become the leader. The former leader may still grant leases to
        // the leader for `META_LEASE_SECS`.
        ctx.lease_grant_tracker.reset();
        let leader_since = Instant::now();
        let (next, _) = state
            .next(&mut ctx, &new_procedure_context())
            .await
            .unwrap();
        assert_eq!(
            leader_since + Duration::from_secs(META_LEASE_SECS) + region_lease,
            Instant::now()
        );
        let _ = next
            .as_any()
            .downcast_ref::<UpgradeCandidateRegion>()
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_failed_downgrade_ignores_grants_after_step_start() {
        let mut state = Box::<DowngradeLeaderRegion>::default();
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;
        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let region_lease = default_distributed_time_constants().region_lease;

        // The leader keeps sending heartbeats while its downgrade fails. The region is already
        // marked as downgrading, so they are granted DowngradingLeader, which does not extend
        // its Leader deadline.
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(from_peer_id), tx)
            .await;
        let tracker = ctx.lease_grant_tracker.clone();
        tokio::spawn(async move {
            while let Some(Ok(resp)) = rx.recv().await {
                tracker.record_grant(from_peer_id);
                let reply_id = resp.mailbox_message.unwrap().id;
                let reply =
                    new_downgrade_region_reply(reply_id, None, true, Some("test mocked".into()));
                mailbox.on_recv(reply_id, Ok(reply)).await.unwrap();
            }
        });

        // This metasrv has been the leader for longer than `META_LEASE_SECS`.
        tokio::time::advance(Duration::from_secs(60)).await;
        let step_start = Instant::now();
        let (next, _) = state
            .next(&mut ctx, &new_procedure_context())
            .await
            .unwrap();
        assert_eq!(step_start + region_lease, Instant::now());
        let _ = next
            .as_any()
            .downcast_ref::<UpgradeCandidateRegion>()
            .unwrap();
    }
}
