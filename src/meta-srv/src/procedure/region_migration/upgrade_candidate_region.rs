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
use common_meta::instruction::{
    Instruction, InstructionReply, UpgradeRegion, UpgradeRegionReply, UpgradeRegionsReply,
};
use common_meta::lock_key::RemoteWalLock;
use common_meta::wal_provider::extract_topic_from_wal_options;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{error, info};
use common_wal::options::WalOptions;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::METRIC_ENGINE_NAME;
use tokio::time::{Instant, sleep};

use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::region_migration::update_metadata::UpdateMetadata;
use crate::procedure::region_migration::{Context, State};
use crate::procedure::utils::instruction_error_result;
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

            let region_wal_options = &datanode_table_value.region_info.region_wal_options;

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
                let is_metric_engine =
                    datanode_table_value.region_info.engine == METRIC_ENGINE_NAME;
                region_topic.push((*region_id, topic, is_metric_engine));
            }
        }

        let replay_checkpoints = ctx
            .get_replay_checkpoints_with_topic_pruned_entry_ids(&region_topic)
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
        if let Some(error) = error {
            return instruction_error_result(
                error,
                format!(
                    "Failed to upgrade the region {} on datanode {:?}, error: {:?}, elapsed: {:?}",
                    region_id,
                    candidate,
                    error,
                    now.elapsed()
                ),
            );
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

        let tracing_ctx = TracingContext::from_current_span();
        let msg = MailboxMessage::json_message(
            &format!("Upgrade candidate regions: {:?}", region_ids),
            &format!("Metasrv@{}", ctx.server_addr()),
            &format!("Datanode-{}@{}", candidate.id, candidate.addr),
            common_time::util::current_time_millis(),
            &upgrade_instruction,
            Some(tracing_ctx.to_w3c()),
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

        // If using Kafka WAL, hold a read lock on each topic for all attempts to prevent WAL
        // pruning during the upgrade. Acquire once: re-acquiring a held read lock on retry
        // queues behind a waiting `WalPruneProcedure` writer, which waits on the first guard.
        let mut guards = Vec::with_capacity(topics.len());
        for topic in &topics {
            guards.push(
                procedure_ctx
                    .provider
                    .acquire_lock(&(RemoteWalLock::Read(topic.clone()).into()))
                    .await,
            );
        }

        loop {
            let timer = Instant::now();
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
    use std::assert_matches;
    use std::collections::HashMap;

    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::key::topic_name::TopicNameKey;
    use common_meta::key::topic_region::{ReplayCheckpoint, TopicRegionKey, TopicRegionValue};
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::wal_provider::RegionWalOptions;
    use common_wal::options::KafkaWalOptions;
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;
    use crate::procedure::region_migration::test_util::{TestingEnv, new_procedure_context};
    use crate::procedure::region_migration::{
        ContextFactory, PersistentContext, RegionMigrationTriggerReason,
    };
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
            RegionMigrationTriggerReason::Unknown,
        )
    }

    fn kafka_wal_options(topic: &str) -> RegionWalOptions {
        RegionWalOptions::from([(
            1,
            WalOptions::Kafka(KafkaWalOptions::new(topic.to_string())),
        )])
    }

    async fn prepare_table_metadata(ctx: &Context, wal_options: RegionWalOptions) {
        prepare_table_metadata_with_engine(ctx, wal_options, "engine").await;
    }

    async fn prepare_table_metadata_with_engine(
        ctx: &Context,
        wal_options: RegionWalOptions,
        engine: &str,
    ) {
        let region_id = ctx.persistent_ctx.region_ids[0];
        let mut table_info = new_test_table_info(region_id.table_id());
        table_info.meta.engine = engine.to_string();
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
    async fn test_build_upgrade_region_instruction_merges_topic_pruned_entry_id() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let region_id = ctx.persistent_ctx.region_ids[0];
        let topic = "test_topic";
        prepare_table_metadata(&ctx, kafka_wal_options(topic)).await;
        ctx.table_metadata_manager
            .topic_region_manager()
            .batch_put(&[(
                TopicRegionKey::new(region_id, topic),
                Some(TopicRegionValue::new(Some(ReplayCheckpoint::new(10, None)))),
            )])
            .await
            .unwrap();
        ctx.table_metadata_manager
            .topic_name_manager()
            .batch_put(vec![TopicNameKey::new(topic)])
            .await
            .unwrap();
        let prev = ctx
            .table_metadata_manager
            .topic_name_manager()
            .get(topic)
            .await
            .unwrap();
        ctx.table_metadata_manager
            .topic_name_manager()
            .update(topic, 20, prev)
            .await
            .unwrap();

        let instruction = state
            .build_upgrade_region_instruction(&mut ctx, Duration::from_secs(1))
            .await
            .unwrap();
        let Instruction::UpgradeRegions(upgrade_regions) = instruction else {
            unreachable!()
        };

        assert_eq!(upgrade_regions.len(), 1);
        assert_eq!(upgrade_regions[0].replay_entry_id, Some(20));
        assert_eq!(upgrade_regions[0].metadata_replay_entry_id, None);
    }

    #[tokio::test]
    async fn test_build_upgrade_region_instruction_merges_metric_metadata_pruned_entry_id() {
        let state = UpgradeCandidateRegion::default();
        let persistent_context = new_persistent_context();
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let region_id = ctx.persistent_ctx.region_ids[0];
        let topic = "test_topic";
        prepare_table_metadata_with_engine(&ctx, kafka_wal_options(topic), METRIC_ENGINE_NAME)
            .await;
        ctx.table_metadata_manager
            .topic_region_manager()
            .batch_put(&[(
                TopicRegionKey::new(region_id, topic),
                Some(TopicRegionValue::new(Some(ReplayCheckpoint::new(
                    10,
                    Some(5),
                )))),
            )])
            .await
            .unwrap();
        ctx.table_metadata_manager
            .topic_name_manager()
            .batch_put(vec![TopicNameKey::new(topic)])
            .await
            .unwrap();
        let prev = ctx
            .table_metadata_manager
            .topic_name_manager()
            .get(topic)
            .await
            .unwrap();
        ctx.table_metadata_manager
            .topic_name_manager()
            .update(topic, 20, prev)
            .await
            .unwrap();

        let instruction = state
            .build_upgrade_region_instruction(&mut ctx, Duration::from_secs(1))
            .await
            .unwrap();
        let Instruction::UpgradeRegions(upgrade_regions) = instruction else {
            unreachable!()
        };

        assert_eq!(upgrade_regions.len(), 1);
        assert_eq!(upgrade_regions[0].replay_entry_id, Some(20));
        assert_eq!(upgrade_regions[0].metadata_replay_entry_id, Some(20));
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

/// Regression tests for a deadlock between the upgrade step's per-retry `RemoteWalLock::Read`
/// (now acquired once before the retry loop) and `WalPruneProcedure`'s `RemoteWalLock::Write`
/// on the same topic.
///
/// Mirrors a datanode rolling restart: failover sends the upgrade instruction to a candidate
/// datanode that never answers, the mailbox times out (retryable), and the periodic WAL prune
/// for the region's topic starts while the upgrade is waiting. Both procedures share one
/// `ContextProvider`, as they do inside metasrv's procedure manager.
///
/// Requires a Kafka broker: `GT_KAFKA_ENDPOINTS=127.0.0.1:9092`.
#[cfg(test)]
mod wal_prune_deadlock_tests {
    use std::assert_matches;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::region_registry::LeaderRegionRegistry;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::wal_provider::{RegionWalOptions, build_kafka_client};
    use common_procedure::Procedure;
    use common_wal::config::kafka::common::KafkaConnectionConfig;
    use common_wal::maybe_skip_kafka_integration_test;
    use common_wal::options::KafkaWalOptions;
    use common_wal::test_util::get_kafka_endpoints;
    use store_api::storage::RegionId;
    use tokio::time::timeout;

    use super::*;
    use crate::procedure::region_migration::test_util::{TestingEnv, new_procedure_context};
    use crate::procedure::region_migration::{
        ContextFactory, PersistentContext, RegionMigrationTriggerReason,
    };
    use crate::procedure::wal_prune::{Context as WalPruneContext, WalPruneProcedure};

    struct Scenario {
        env: TestingEnv,
        ctx: Context,
        topic: String,
        upgrade_instructions: Arc<AtomicUsize>,
    }

    /// One region on a Kafka topic, failing over to a datanode that receives the upgrade
    /// instruction but never replies (it is restarting).
    async fn setup(name: &str) -> Scenario {
        let topic = format!("{name}_{}", uuid::Uuid::new_v4());
        let region_id = RegionId::new(1024, 1);
        let persistent_ctx = PersistentContext::new(
            vec![("greptime".into(), "public".into())],
            Peer::empty(1),
            Peer::empty(2),
            vec![region_id],
            // The migration timeout; the first upgrade attempt consumes all of it.
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Failover,
        );
        let to_peer_id = persistent_ctx.to_peer.id;

        let mut env = TestingEnv::new();
        let ctx = env.context_factory().new_context(persistent_ctx);
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(ctx.persistent_ctx.from_peer.clone()),
            follower_peers: vec![ctx.persistent_ctx.to_peer.clone()],
            ..Default::default()
        }];
        let wal_options = RegionWalOptions::from([(
            region_id.region_number(),
            WalOptions::Kafka(KafkaWalOptions::new(topic.clone())),
        )]);
        ctx.table_metadata_manager
            .create_table_metadata(
                new_test_table_info(region_id.table_id()),
                TableRouteValue::physical(region_routes),
                wal_options,
            )
            .await
            .unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        env.mailbox_context()
            .insert_heartbeat_response_receiver(Channel::Datanode(to_peer_id), tx)
            .await;
        let upgrade_instructions = Arc::new(AtomicUsize::new(0));
        let counter = upgrade_instructions.clone();
        common_runtime::spawn_global(async move {
            while let Some(Ok(_)) = rx.recv().await {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });

        Scenario {
            env,
            ctx,
            topic,
            upgrade_instructions,
        }
    }

    async fn wait_for_first_instruction(counter: &AtomicUsize) {
        timeout(Duration::from_secs(5), async {
            while counter.load(Ordering::SeqCst) == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_upgrade_retry_does_not_deadlock_with_wal_prune() {
        maybe_skip_kafka_integration_test!();
        let Scenario {
            env,
            mut ctx,
            topic,
            upgrade_instructions,
        } = setup("upgrade_wal_prune_deadlock").await;
        // Metasrv shares one provider (and dynamic key lock) across all procedures.
        let procedure_ctx = new_procedure_context();

        let migration_procedure_ctx = procedure_ctx.clone();
        let migration = tokio::spawn(async move {
            let mut state = Box::<UpgradeCandidateRegion>::default();
            state.next(&mut ctx, &migration_procedure_ctx).await
        });
        // The upgrade step now holds `RemoteWalLock::Read(topic)` and waits on the datanode.
        wait_for_first_instruction(&upgrade_instructions).await;

        // The periodic WAL prune for the region's topic starts.
        let client = build_kafka_client(&KafkaConnectionConfig {
            broker_endpoints: get_kafka_endpoints(),
            ..Default::default()
        })
        .await
        .unwrap();
        client
            .controller_client()
            .unwrap()
            .create_topic(&topic, 1, 1, 5000)
            .await
            .unwrap();
        let mut wal_prune = WalPruneProcedure::new(
            WalPruneContext {
                client: Arc::new(client),
                table_metadata_manager: env.table_metadata_manager().clone(),
                leader_region_registry: Arc::new(LeaderRegionRegistry::new()),
            },
            None,
            topic,
            0,
            false,
        );
        let wal_prune_procedure_ctx = procedure_ctx.clone();
        let wal_prune =
            tokio::spawn(async move { wal_prune.execute(&wal_prune_procedure_ctx).await });

        // The mailbox times out after ~1s (retryable). The retry keeps the read lock it already
        // holds, fails fast with `ExceededDeadline`, and the migration rolls back. Releasing the
        // read lock then lets the queued WAL prune run.
        let (next, _) = timeout(Duration::from_secs(10), migration)
            .await
            .expect("upgrade step should not deadlock")
            .unwrap()
            .unwrap();
        let update_metadata = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();
        assert_matches!(update_metadata, UpdateMetadata::Rollback);
        assert_eq!(1, upgrade_instructions.load(Ordering::SeqCst));

        timeout(Duration::from_secs(10), wal_prune)
            .await
            .expect("WAL prune should not deadlock")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_upgrade_retry_without_wal_prune_rolls_back() {
        maybe_skip_kafka_integration_test!();
        let Scenario {
            mut ctx,
            upgrade_instructions,
            ..
        } = setup("upgrade_wal_prune_control").await;
        let procedure_ctx = new_procedure_context();

        let mut state = Box::<UpgradeCandidateRegion>::default();
        let (next, _) = timeout(
            Duration::from_secs(10),
            state.next(&mut ctx, &procedure_ctx),
        )
        .await
        .expect("upgrade step should finish without WAL prune")
        .unwrap();
        let update_metadata = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();
        assert_matches!(update_metadata, UpdateMetadata::Rollback);
        assert_eq!(1, upgrade_instructions.load(Ordering::SeqCst));
    }
}
