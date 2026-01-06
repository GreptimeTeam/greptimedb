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
use std::collections::HashSet;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_meta::ddl::utils::parse_region_wal_options;
use common_meta::instruction::{
    Instruction, InstructionReply, UpgradeRegion, UpgradeRegionReply, UpgradeRegionsReply,
};
use common_meta::key::topic_region::TopicRegionKey;
use common_meta::lock_key::RemoteWalLock;
use common_meta::wal_options_allocator::extract_topic_from_wal_options;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::{error, info};
use common_wal::options::WalOptions;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use tokio::time::{Instant, sleep};

use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::region_migration::update_metadata::UpdateMetadata;
use crate::procedure::region_migration::{Context, State};
use crate::service::mailbox::Channel;

#[derive(Debug, Serialize, Deserialize)]
pub struct UpgradeCandidateRegion {
    // The optimistic retry times.
    pub(crate) optimistic_retry: usize,
    // The retry initial interval.
    pub(crate) retry_initial_interval: Duration,
    // If it's true it requires the candidate region MUST replay the WAL to the latest entry id.
    // Otherwise, it will rollback to the old leader region.
    pub(crate) require_ready: bool,
}

impl Default for UpgradeCandidateRegion {
    fn default() -> Self {
        Self {
            optimistic_retry: 3,
            retry_initial_interval: Duration::from_millis(500),
            require_ready: true,
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for UpgradeCandidateRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let now = Instant::now();

        let topics = self.get_kafka_topics(ctx).await?;
        if self
            .upgrade_region_with_retry(ctx, procedure_ctx, topics)
            .await
        {
            ctx.update_upgrade_candidate_region_elapsed(now);
            Ok((Box::new(UpdateMetadata::Upgrade), Status::executing(false)))
        } else {
            ctx.update_upgrade_candidate_region_elapsed(now);
            Ok((Box::new(UpdateMetadata::Rollback), Status::executing(false)))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl UpgradeCandidateRegion {
    async fn get_kafka_topics(&self, ctx: &mut Context) -> Result<HashSet<String>> {
        let table_regions = ctx.persistent_ctx.table_regions();
        let datanode_table_values = ctx.get_from_peer_datanode_table_values().await?;
        let mut topics = HashSet::new();
        for (table_id, regions) in table_regions {
            let Some(datanode_table_value) = datanode_table_values.get(&table_id) else {
                continue;
            };

            let region_wal_options =
                parse_region_wal_options(&datanode_table_value.region_info.region_wal_options)
                    .context(error::ParseWalOptionsSnafu)?;

            for region_id in regions {
                let Some(WalOptions::Kafka(kafka_wal_options)) =
                    region_wal_options.get(&region_id.region_number())
                else {
                    continue;
                };
                if !topics.contains(&kafka_wal_options.topic) {
                    topics.insert(kafka_wal_options.topic.clone());
                }
            }
        }

        Ok(topics)
    }

    /// Builds upgrade region instruction.
    async fn build_upgrade_region_instruction(
        &self,
        ctx: &mut Context,
        replay_timeout: Duration,
    ) -> Result<Instruction> {
        let region_ids = ctx.persistent_ctx.region_ids.clone();
        let datanode_table_values = ctx.get_from_peer_datanode_table_values().await?;
        let mut region_topic = Vec::with_capacity(region_ids.len());
        for region_id in region_ids.iter() {
            let table_id = region_id.table_id();
            if let Some(datanode_table_value) = datanode_table_values.get(&table_id)
                && let Some(topic) = extract_topic_from_wal_options(
                    *region_id,
                    &datanode_table_value.region_info.region_wal_options,
                )
            {
                region_topic.push((*region_id, topic));
            }
        }

        let replay_checkpoints = ctx
            .get_replay_checkpoints(
                region_topic
                    .iter()
                    .map(|(region_id, topic)| TopicRegionKey::new(*region_id, topic))
                    .collect(),
            )
            .await?;
        // Build upgrade regions instruction.
        let mut upgrade_regions = Vec::with_capacity(region_ids.len());
        for region_id in region_ids {
            let last_entry_id = ctx
                .volatile_ctx
                .leader_region_last_entry_ids
                .get(&region_id)
                .copied();
            let metadata_last_entry_id = ctx
                .volatile_ctx
                .leader_region_metadata_last_entry_ids
                .get(&region_id)
                .copied();
            let checkpoint = replay_checkpoints.get(&region_id).copied();
            upgrade_regions.push(UpgradeRegion {
                region_id,
                last_entry_id,
                metadata_last_entry_id,
                replay_timeout,
                location_id: Some(ctx.persistent_ctx.from_peer.id),
                replay_entry_id: checkpoint.map(|c| c.entry_id),
                metadata_replay_entry_id: checkpoint.and_then(|c| c.metadata_entry_id),
            });
        }

        Ok(Instruction::UpgradeRegions(upgrade_regions))
    }

    fn handle_upgrade_region_reply(
        &self,
        ctx: &mut Context,
        UpgradeRegionReply {
            region_id,
            ready,
            exists,
            error,
        }: &UpgradeRegionReply,
        now: &Instant,
    ) -> Result<()> {
        let candidate = &ctx.persistent_ctx.to_peer;
        if error.is_some() {
            return error::RetryLaterSnafu {
                reason: format!(
                    "Failed to upgrade the region {} on datanode {:?}, error: {:?}, elapsed: {:?}",
                    region_id,
                    candidate,
                    error,
                    now.elapsed()
                ),
            }
            .fail();
        }

        ensure!(
            exists,
            error::UnexpectedSnafu {
                violated: format!(
                    "Candidate region {} doesn't exist on datanode {:?}",
                    region_id, candidate
                )
            }
        );

        if self.require_ready && !ready {
            return error::RetryLaterSnafu {
                reason: format!(
                    "Candidate region {} still replaying the wal on datanode {:?}, elapsed: {:?}",
                    region_id,
                    candidate,
                    now.elapsed()
                ),
            }
            .fail();
        }

        Ok(())
    }

    /// Tries to upgrade a candidate region.
    ///
    /// Retry:
    /// - If `require_ready` is true, but the candidate region returns `ready` is false.
    /// - [MailboxTimeout](error::Error::MailboxTimeout), Timeout.
    ///
    /// Abort:
    /// - The candidate region doesn't exist.
    /// - [PusherNotFound](error::Error::PusherNotFound), The datanode is unreachable.
    /// - [PushMessage](error::Error::PushMessage), The receiver is dropped.
    /// - [MailboxReceiver](error::Error::MailboxReceiver), The sender is dropped without sending (impossible).
    /// - [UnexpectedInstructionReply](error::Error::UnexpectedInstructionReply) (impossible).
    /// - [ExceededDeadline](error::Error::ExceededDeadline)
    /// - Invalid JSON (impossible).
    async fn upgrade_region(&self, ctx: &mut Context) -> Result<()> {
        let operation_timeout =
            ctx.next_operation_timeout()
                .context(error::ExceededDeadlineSnafu {
                    operation: "Upgrade region",
                })?;
        let upgrade_instruction = self
            .build_upgrade_region_instruction(ctx, operation_timeout)
            .await?;

        let pc = &ctx.persistent_ctx;
        let region_ids = &pc.region_ids;
        let candidate = &pc.to_peer;

        let msg = MailboxMessage::json_message(
            &format!("Upgrade candidate regions: {:?}", region_ids),
            &format!("Metasrv@{}", ctx.server_addr()),
            &format!("Datanode-{}@{}", candidate.id, candidate.addr),
            common_time::util::current_time_millis(),
            &upgrade_instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: upgrade_instruction.to_string(),
        })?;

        let ch = Channel::Datanode(candidate.id);
        let receiver = ctx.mailbox.send(&ch, msg, operation_timeout).await?;

        let now = Instant::now();
        match receiver.await {
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                info!(
                    "Received upgrade region reply: {:?}, regions: {:?}, elapsed: {:?}",
                    reply,
                    region_ids,
                    now.elapsed()
                );
                let InstructionReply::UpgradeRegions(UpgradeRegionsReply { replies }) = reply
                else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "Unexpected reply of the upgrade region instruction",
                    }
                    .fail();
                };
                for reply in replies {
                    self.handle_upgrade_region_reply(ctx, &reply, &now)?;
                }
                Ok(())
            }
            Err(error::Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for upgrade candidate regions {region_ids:?} on datanode {:?}, elapsed: {:?}",
                    candidate,
                    now.elapsed()
                );
                error::RetryLaterSnafu { reason }.fail()
            }
            Err(err) => Err(err),
        }
    }

    /// Upgrades a candidate region.
    ///
    /// Returns true if the candidate region is upgraded successfully.
    async fn upgrade_region_with_retry(
        &self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
        topics: HashSet<String>,
    ) -> bool {
        let mut retry = 0;
        let mut upgraded = false;

        let mut guards = Vec::with_capacity(topics.len());
        loop {
            let timer = Instant::now();
            // If using Kafka WAL, acquire a read lock on the topic to prevent WAL pruning during the upgrade.
            for topic in &topics {
                guards.push(
                    procedure_ctx
                        .provider
                        .acquire_lock(&(RemoteWalLock::Read(topic.clone()).into()))
                        .await,
                );
            }

            if let Err(err) = self.upgrade_region(ctx).await {
                retry += 1;
                ctx.update_operations_elapsed(timer);
                if matches!(err, error::Error::ExceededDeadline { .. }) {
                    error!("Failed to upgrade region, exceeded deadline");
                    break;
                } else if err.is_retryable() && retry < self.optimistic_retry {
                    error!("Failed to upgrade region, error: {err:?}, retry later");
                    sleep(self.retry_initial_interval).await;
                } else {
                    error!("Failed to upgrade region, error: {err:?}");
                    break;
                }
            } else {
                ctx.update_operations_elapsed(timer);
                upgraded = true;
                break;
            }
        }

        upgraded
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;
    use crate::procedure::region_migration::manager::RegionMigrationTriggerReason;
    use crate::procedure::region_migration::test_util::{TestingEnv, new_procedure_context};
    use crate::procedure::region_migration::{ContextFactory, PersistentContext};
    use crate::procedure::test_util::{
        new_close_region_reply, new_upgrade_region_reply, send_mock_reply,
    };

    fn new_persistent_context() -> PersistentContext {
        PersistentContext::new(
            vec![("greptime".into(), "public".into())],
            Peer::empty(1),
            Peer::empty(2),
            vec![RegionId::new(1024, 1)],
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Manual,
        )
    }

    async fn prepare_table_metadata(ctx: &Context, wal_options: HashMap<u32, String>) {
        let region_id = ctx.persistent_ctx.region_ids[0];
        let table_info = new_test_table_info(region_id.table_id()).into();
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
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let err = state.upgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::PusherNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_pusher_dropped() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        drop(rx);

        let err = state.upgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::PushMessage { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_procedure_exceeded_deadline() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        ctx.volatile_ctx.metrics.operations_elapsed =
            ctx.persistent_ctx.timeout + Duration::from_secs(1);

        let err = state.upgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::ExceededDeadline { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_unexpected_instruction_reply() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| Ok(new_close_region_reply(id)));

        let err = state.upgrade_region(&mut ctx).await.unwrap_err();
        assert_matches!(err, Error::UnexpectedInstructionReply { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_upgrade_region_failed() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        // A reply contains an error.
        send_mock_reply(mailbox, rx, |id| {
            Ok(new_upgrade_region_reply(
                id,
                true,
                true,
                Some("test mocked".to_string()),
            ))
        });

        let err = state.upgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
        assert!(format!("{err:?}").contains("test mocked"));
    }

    #[tokio::test]
    async fn test_upgrade_region_not_found() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_upgrade_region_reply(id, true, false, None))
        });

        let err = state.upgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::Unexpected { .. });
        assert!(!err.is_retryable());
        assert!(err.to_string().contains("doesn't exist"));
    }

    #[tokio::test]
    async fn test_upgrade_region_require_ready() {
        let mut state = UpgradeCandidateRegion {
            require_ready: true,
            ..Default::default()
        };

        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_upgrade_region_reply(id, false, true, None))
        });

        let err = state.upgrade_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
        assert!(format!("{err:?}").contains("still replaying the wal"));

        // Sets the `require_ready` to false.
        state.require_ready = false;

        let mailbox = mailbox_ctx.mailbox().clone();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_upgrade_region_reply(id, false, true, None))
        });

        state.upgrade_region(&mut ctx).await.unwrap();
    }

    #[tokio::test]
    async fn test_upgrade_region_with_retry_ok() {
        let mut state = Box::<UpgradeCandidateRegion>::default();
        state.retry_initial_interval = Duration::from_millis(100);
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        common_runtime::spawn_global(async move {
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Err(error::MailboxTimeoutSnafu { id: reply_id }.build()),
                )
                .await
                .unwrap();

            // retry: 1
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Ok(new_upgrade_region_reply(reply_id, false, true, None)),
                )
                .await
                .unwrap();

            // retry: 2
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Ok(new_upgrade_region_reply(reply_id, true, true, None)),
                )
                .await
                .unwrap();
        });

        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();

        let update_metadata = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();

        assert_matches!(update_metadata, UpdateMetadata::Upgrade);
    }

    #[tokio::test]
    async fn test_upgrade_region_with_retry_failed() {
        let mut state = Box::<UpgradeCandidateRegion>::default();
        state.retry_initial_interval = Duration::from_millis(100);
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        common_runtime::spawn_global(async move {
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Err(error::MailboxTimeoutSnafu { id: reply_id }.build()),
                )
                .await
                .unwrap();

            // retry: 1
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Ok(new_upgrade_region_reply(reply_id, false, true, None)),
                )
                .await
                .unwrap();

            // retry: 2
            let resp = rx.recv().await.unwrap().unwrap();
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox
                .on_recv(
                    reply_id,
                    Ok(new_upgrade_region_reply(reply_id, false, false, None)),
                )
                .await
                .unwrap();
        });
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();

        let update_metadata = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();
        assert_matches!(update_metadata, UpdateMetadata::Rollback);
    }

    #[tokio::test]
    async fn test_upgrade_region_procedure_exceeded_deadline() {
        let mut state = Box::<UpgradeCandidateRegion>::default();
        state.retry_initial_interval = Duration::from_millis(100);
        let persistent_context = new_persistent_context();
        let to_peer_id = persistent_context.to_peer.id;

        let mut env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        prepare_table_metadata(&ctx, HashMap::default()).await;
        let mailbox_ctx = env.mailbox_context();
        let mailbox = mailbox_ctx.mailbox().clone();
        ctx.volatile_ctx.metrics.operations_elapsed =
            ctx.persistent_ctx.timeout + Duration::from_secs(1);

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;

        send_mock_reply(mailbox, rx, |id| {
            Ok(new_upgrade_region_reply(id, false, true, None))
        });
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();
        let update_metadata = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();
        assert_matches!(update_metadata, UpdateMetadata::Rollback);
    }
}
