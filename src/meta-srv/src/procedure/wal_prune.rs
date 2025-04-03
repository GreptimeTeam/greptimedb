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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_error::ext::BoxedError;
use common_meta::distributed_time_constants::MAILBOX_RTT_SECS;
use common_meta::instruction::{FlushRegions, Instruction};
use common_meta::key::TableMetadataManagerRef;
use common_meta::lock_key::RemoteWalLock;
use common_meta::peer::Peer;
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_procedure::error::ToJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status, StringKey,
};
use common_telemetry::warn;
use itertools::{Itertools, MinMaxResult};
use log_store::kafka::DEFAULT_PARTITION;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::Client;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::logstore::EntryId;
use store_api::storage::RegionId;

use crate::error::{
    self, BuildPartitionClientSnafu, DeleteRecordSnafu, TableMetadataManagerSnafu,
    UpdateMinEntryIdSnafu,
};
use crate::service::mailbox::{Channel, MailboxRef};
use crate::Result;

type KafkaClientRef = Arc<Client>;

const FLUSH_TIMEOUT: Duration = Duration::from_secs(MAILBOX_RTT_SECS);
const DELETE_RECORDS_TIMEOUT: i32 = 1000;

/// The state of WAL pruning.
#[derive(Debug, Serialize, Deserialize)]
pub enum WalPruneState {
    Prepare,
    SendingFlushRequest,
    Prune,
}

pub struct Context {
    /// The Kafka client.
    client: KafkaClientRef,
    /// The table metadata manager.
    table_metadata_manager: TableMetadataManagerRef,
    leader_region_registry: LeaderRegionRegistryRef,
    server_addr: String,
    mailbox: MailboxRef,
}

/// The data of WAL pruning.
#[derive(Serialize, Deserialize)]
pub struct WalPruneData {
    /// The topic name to prune.
    pub topic: String,
    /// The minimum flush entry id for topic, which is used to prune the WAL.
    pub min_flushed_entry_id: EntryId,
    pub regions_to_flush: Vec<RegionId>,
    /// If `flushed_entry_id` + `threshold` < `max_flushed_entry_id`, send a flush request to the region.
    pub threshold: u64,
    /// The state.
    pub state: WalPruneState,
}

/// The procedure to prune WAL.
pub struct WalPruneProcedure {
    pub data: WalPruneData,
    pub context: Context,
}

impl WalPruneProcedure {
    const TYPE_NAME: &'static str = "metasrv-procedure::WalPrune";

    pub fn new(topic: String, context: Context, threshold: u64) -> Self {
        Self {
            data: WalPruneData {
                topic,
                min_flushed_entry_id: 0,
                threshold,
                regions_to_flush: vec![],
                state: WalPruneState::Prepare,
            },
            context,
        }
    }

    pub fn from_json(json: &str, context: Context) -> ProcedureResult<Self> {
        let data: WalPruneData = serde_json::from_str(json).context(ToJsonSnafu)?;
        Ok(Self { data, context })
    }

    async fn build_flush_region_instruction(
        &self,
        ctx: &Context,
        region_ids: &[RegionId],
    ) -> Result<Vec<(Peer, Instruction)>> {
        let mut table_ids = HashSet::new();
        for region_id in region_ids {
            let table_id = region_id.table_id();
            table_ids.insert(table_id);
        }
        let table_id_vec = table_ids.into_iter().collect::<Vec<_>>();
        let table_ids_table_routes_map = ctx
            .table_metadata_manager
            .table_route_manager()
            .batch_get_physical_table_routes(&table_id_vec)
            .await
            .context(TableMetadataManagerSnafu)?;

        let mut peer_region_ids_map = HashMap::new();
        for region_id in region_ids {
            let table_id = region_id.table_id();
            let table_route = match table_ids_table_routes_map.get(&table_id) {
                Some(route) => route,
                None => {
                    warn!("Failed to get table route for region: {region_id}");
                    continue;
                }
            };
            for region_route in &table_route.region_routes {
                if region_route.region.id != *region_id {
                    continue;
                }
                if let Some(peer) = &region_route.leader_peer {
                    peer_region_ids_map
                        .entry(peer)
                        .or_insert_with(Vec::new)
                        .push(*region_id);
                }
            }
        }

        let peer_and_instructions = peer_region_ids_map
            .into_iter()
            .map(|(peer, region_ids)| {
                let flush_instruction = Instruction::FlushRegion(FlushRegions {
                    datanode_id: peer.id,
                    region_ids,
                });
                (peer.clone(), flush_instruction)
            })
            .collect();
        Ok(peer_and_instructions)
    }

    /// Prepare the entry id to prune and regions to flush.
    ///
    /// Retry:
    /// - Failed to retrieve any metadata.
    pub async fn on_prepare(&mut self) -> Result<Status> {
        let region_ids = self
            .context
            .table_metadata_manager
            .topic_region_manager()
            .regions(&self.data.topic)
            .await
            .context(TableMetadataManagerSnafu)
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: "Failed to get topic-region map",
            })?;
        let flush_entry_ids_map: HashMap<_, _> = self
            .context
            .leader_region_registry
            .batch_get(region_ids.iter().cloned())
            .into_iter()
            .map(|(region_id, region)| {
                let flushed_entry_id = region.manifest.min_flushed_entry_id();
                (region_id, flushed_entry_id)
            })
            .collect();

        // Check if the `flush_entry_ids_map` contains all region ids.
        let non_collected_region_ids =
            check_heartbeat_collected_region_ids(&region_ids, &flush_entry_ids_map);
        if !non_collected_region_ids.is_empty() {
            // The heartbeat collected region ids do not contain all region ids in the topic-region map.
            // In this case, we should not prune the WAL.
            warn!("The heartbeat collected region ids do not contain all region ids in the topic-region map. Aborting the WAL prune procedure.
            topic: {}, non-collected region ids: {:?}", self.data.topic, non_collected_region_ids);
            return Ok(Status::done());
        }

        let min_max_result = flush_entry_ids_map.values().minmax();
        let max_flushed_entry_id = match min_max_result {
            MinMaxResult::NoElements => {
                return Ok(Status::done());
            }
            MinMaxResult::OneElement(flushed_entry_id) => {
                self.data.min_flushed_entry_id = *flushed_entry_id;
                *flushed_entry_id
            }
            MinMaxResult::MinMax(min_flushed_entry_id, max_flushed_entry_id) => {
                self.data.min_flushed_entry_id = *min_flushed_entry_id;
                *max_flushed_entry_id
            }
        };
        for (region_id, flushed_entry_id) in flush_entry_ids_map {
            if flushed_entry_id + self.data.threshold < max_flushed_entry_id {
                self.data.regions_to_flush.push(region_id);
            }
        }

        self.data.state = WalPruneState::SendingFlushRequest;
        Ok(Status::executing(true))
    }

    /// Send the flush request to regions with low flush entry id.
    pub async fn on_sending_flush_request(&mut self) -> Result<Status> {
        // Safety: regions_to_flush is loaded in on_prepare.
        let flush_instructions = self
            .build_flush_region_instruction(&self.context, &self.data.regions_to_flush)
            .await?;
        for (peer, flush_instruction) in flush_instructions.into_iter() {
            let msg = MailboxMessage::json_message(
                &format!("Flush regions: {}", flush_instruction),
                &format!("Metasrv@{}", self.context.server_addr),
                &format!("Datanode-{}@{}", peer.id, peer.addr),
                common_time::util::current_time_millis(),
                &flush_instruction,
            )
            .with_context(|_| error::SerializeToJsonSnafu {
                input: flush_instruction.to_string(),
            })?;
            let ch = Channel::Datanode(peer.id);
            let _ = self.context.mailbox.send(&ch, msg, FLUSH_TIMEOUT).await?;
        }
        self.data.state = WalPruneState::Prune;
        Ok(Status::executing(true))
    }

    /// Prune the WAL and persist the minimum flushed entry id.
    pub async fn on_prune(&mut self) -> Result<Status> {
        // Safety: last_entry_ids are loaded in on_prepare.
        let partition_client = self
            .context
            .client
            .partition_client(
                self.data.topic.clone(),
                DEFAULT_PARTITION,
                UnknownTopicHandling::Retry,
            )
            .await
            .context(BuildPartitionClientSnafu {
                topic: self.data.topic.clone(),
                partition: DEFAULT_PARTITION,
            })?;

        self.context
            .table_metadata_manager
            .topic_name_manager()
            .put(&self.data.topic, self.data.min_flushed_entry_id)
            .await
            .context(UpdateMinEntryIdSnafu {
                topic: self.data.topic.clone(),
            })?;
        partition_client
            .delete_records(
                self.data.min_flushed_entry_id as i64,
                DELETE_RECORDS_TIMEOUT,
            )
            .await
            .context(DeleteRecordSnafu {
                topic: self.data.topic.clone(),
                partition: DEFAULT_PARTITION,
                offset: self.data.min_flushed_entry_id,
            })
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: "Failed to delete records",
            })?;
        Ok(Status::done())
    }
}

#[async_trait::async_trait]
impl Procedure for WalPruneProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    fn rollback_supported(&self) -> bool {
        false
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        match state {
            WalPruneState::Prepare => self.on_prepare().await,
            WalPruneState::SendingFlushRequest => self.on_sending_flush_request().await,
            WalPruneState::Prune => self.on_prune().await,
        }
        .map_err(|e| {
            if e.is_retryable() {
                ProcedureError::retry_later(e)
            } else {
                ProcedureError::external(e)
            }
        })
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    /// WAL prune procedure will read the topic-region map from the table metadata manager,
    /// which are modified by `DROP [TABLE|DATABASE]` and `CREATE [TABLE]` operations.
    /// But the modifications are atomic, so it does not conflict with the procedure.
    /// It only abort the procedure sometimes since the `check_heartbeat_collected_region_ids` fails.
    fn lock_key(&self) -> LockKey {
        let lock_key: StringKey = RemoteWalLock::Write(self.data.topic.clone()).into();
        LockKey::new(vec![lock_key])
    }
}

/// Check if the heartbeat collected region ids contains all region ids in the topic-region map.
fn check_heartbeat_collected_region_ids(
    region_ids: &[RegionId],
    heartbeat_collected_region_ids: &HashMap<RegionId, u64>,
) -> Vec<RegionId> {
    let mut non_collected_region_ids = Vec::new();
    for region_id in region_ids {
        if !heartbeat_collected_region_ids.contains_key(region_id) {
            non_collected_region_ids.push(*region_id);
        }
    }
    non_collected_region_ids
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use api::v1::meta::HeartbeatResponse;
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::region_registry::LeaderRegionRegistry;
    use common_meta::sequence::SequenceBuilder;
    use common_meta::wal_options_allocator::build_kafka_topic_creator;
    use common_wal::config::kafka::common::{KafkaConnectionConfig, KafkaTopicConfig};
    use common_wal::config::kafka::MetasrvKafkaConfig;
    use common_wal::test_util::run_test_with_kafka_wal;
    use rskafka::record::Record;
    use tokio::sync::mpsc::Receiver;

    use super::*;
    use crate::handler::HeartbeatMailbox;
    use crate::procedure::test_util::{new_wal_prune_metadata, MailboxContext};

    struct TestEnv {
        table_metadata_manager: TableMetadataManagerRef,
        leader_region_registry: LeaderRegionRegistryRef,
        mailbox: MailboxContext,
        server_addr: String,
    }

    impl TestEnv {
        fn new() -> Self {
            let kv_backend = Arc::new(MemoryKvBackend::new());
            let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
            let leader_region_registry = Arc::new(LeaderRegionRegistry::new());
            let mailbox_sequence =
                SequenceBuilder::new("test_heartbeat_mailbox", kv_backend.clone()).build();

            let mailbox_ctx = MailboxContext::new(mailbox_sequence);

            Self {
                table_metadata_manager,
                leader_region_registry,
                mailbox: mailbox_ctx,
                server_addr: "localhost".to_string(),
            }
        }

        fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
            &self.table_metadata_manager
        }

        fn leader_region_registry(&self) -> &LeaderRegionRegistryRef {
            &self.leader_region_registry
        }

        fn mailbox_context(&self) -> &MailboxContext {
            &self.mailbox
        }

        fn server_addr(&self) -> &str {
            &self.server_addr
        }
    }

    /// Mock a test env for testing.
    /// Including:
    /// 1. Create a test env with a mailbox, a table metadata manager and a in-memory kv backend.
    /// 2. Prepare some data in the table metadata manager and in-memory kv backend.
    /// 3. Generate a `WalPruneProcedure` with the test env.
    /// 4. Return the test env, the procedure, the minimum last entry id to prune and the regions to flush.
    async fn mock_test_env(
        topic: String,
        broker_endpoints: Vec<String>,
        env: &TestEnv,
    ) -> (WalPruneProcedure, u64, Vec<RegionId>) {
        // Creates a topic manager.
        let kafka_topic = KafkaTopicConfig {
            replication_factor: broker_endpoints.len() as i16,
            ..Default::default()
        };
        let config = MetasrvKafkaConfig {
            connection: KafkaConnectionConfig {
                broker_endpoints,
                ..Default::default()
            },
            kafka_topic,
            ..Default::default()
        };
        let topic_creator = build_kafka_topic_creator(&config).await.unwrap();
        let table_metadata_manager = env.table_metadata_manager().clone();
        let leader_region_registry = env.leader_region_registry().clone();
        let mailbox = env.mailbox_context().mailbox().clone();
        let offsets = mock_wal_entries(topic_creator.client().clone(), &topic, 10).await;
        let threshold = 10;

        let (min_last_entry_id, regions_to_flush) = new_wal_prune_metadata(
            table_metadata_manager.clone(),
            leader_region_registry.clone(),
            10,
            5,
            &offsets,
            threshold,
            topic.clone(),
        )
        .await;

        let context = Context {
            client: topic_creator.client().clone(),
            table_metadata_manager,
            leader_region_registry,
            mailbox,
            server_addr: env.server_addr().to_string(),
        };

        let wal_prune_procedure = WalPruneProcedure::new(topic, context, threshold);
        (wal_prune_procedure, min_last_entry_id, regions_to_flush)
    }

    fn record(i: usize) -> Record {
        let key = format!("key_{i}");
        let value = format!("value_{i}");
        Record {
            key: Some(key.into()),
            value: Some(value.into()),
            timestamp: chrono::Utc::now(),
            headers: Default::default(),
        }
    }

    async fn mock_wal_entries(
        client: KafkaClientRef,
        topic_name: &str,
        n_entries: usize,
    ) -> Vec<i64> {
        let controller_client = client.controller_client().unwrap();
        let _ = controller_client
            .create_topic(topic_name, 1, 1, 5_000)
            .await;
        let partition_client = client
            .partition_client(topic_name, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap();
        let mut offsets = Vec::with_capacity(n_entries);
        for i in 0..n_entries {
            let record = vec![record(i)];
            let offset = partition_client
                .produce(
                    record,
                    rskafka::client::partition::Compression::NoCompression,
                )
                .await
                .unwrap();
            offsets.extend(offset);
        }
        offsets
    }

    async fn check_entry_id_existence(
        client: KafkaClientRef,
        topic_name: &str,
        entry_id: i64,
    ) -> bool {
        let partition_client = client
            .partition_client(topic_name, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap();
        let (records, _high_watermark) = partition_client
            .fetch_records(entry_id, 0..10001, 5_000)
            .await
            .unwrap();
        !records.is_empty()
    }

    async fn delete_topic(client: KafkaClientRef, topic_name: &str) {
        let controller_client = client.controller_client().unwrap();
        controller_client
            .delete_topic(topic_name, 5_000)
            .await
            .unwrap();
    }

    async fn check_flush_request(
        rx: &mut Receiver<std::result::Result<HeartbeatResponse, tonic::Status>>,
        region_ids: &[RegionId],
    ) {
        let resp = rx.recv().await.unwrap().unwrap();
        let msg = resp.mailbox_message.unwrap();
        let flush_instruction = HeartbeatMailbox::json_instruction(&msg).unwrap();
        let mut flush_requested_region_ids = match flush_instruction {
            Instruction::FlushRegion(FlushRegions { region_ids, .. }) => region_ids,
            _ => unreachable!(),
        };
        let sorted_region_ids = region_ids
            .iter()
            .cloned()
            .sorted_by(|a, b| a.as_u64().cmp(&b.as_u64()))
            .collect::<Vec<_>>();
        flush_requested_region_ids.sort_by(|a, b| a.as_u64().cmp(&b.as_u64()));
        assert_eq!(flush_requested_region_ids, sorted_region_ids);
    }

    #[tokio::test]
    async fn test_procedure_execution() {
        run_test_with_kafka_wal(|broker_endpoints| {
            Box::pin(async {
                common_telemetry::init_default_ut_logging();
                let topic_name = "greptime_test_topic".to_string();
                let mut env = TestEnv::new();
                let (mut procedure, min_last_entry_id, regions_to_flush) =
                    mock_test_env(topic_name.clone(), broker_endpoints, &env).await;

                // Step 1: Test `on_prepare`.
                let status = procedure.on_prepare().await.unwrap();
                assert_matches!(status, Status::Executing { persist: true });
                assert_matches!(procedure.data.state, WalPruneState::SendingFlushRequest);
                assert_eq!(procedure.data.min_flushed_entry_id, min_last_entry_id);
                assert_eq!(procedure.data.regions_to_flush, regions_to_flush);

                // Step 2: Test `on_sending_flush_request`.
                let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                env.mailbox
                    .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
                    .await;
                let status = procedure.on_sending_flush_request().await.unwrap();
                check_flush_request(&mut rx, &regions_to_flush).await;
                assert_matches!(status, Status::Executing { persist: true });
                assert_matches!(procedure.data.state, WalPruneState::Prune);

                // Step 3: Test `on_prune`.
                let status = procedure.on_prune().await.unwrap();
                assert_matches!(status, Status::Done { output: None });
                // Check if the entry ids after `min_flushed_entry_id` still exist.
                assert!(
                    check_entry_id_existence(
                        procedure.context.client.clone(),
                        &topic_name,
                        procedure.data.min_flushed_entry_id as i64,
                    )
                    .await
                );
                // Check if the entry s before `min_flushed_entry_id` are deleted.
                assert!(
                    procedure.data.min_flushed_entry_id == 0
                        || !check_entry_id_existence(
                            procedure.context.client.clone(),
                            &topic_name,
                            procedure.data.min_flushed_entry_id as i64 - 1,
                        )
                        .await
                );

                let min_entry_id = env
                    .table_metadata_manager()
                    .topic_name_manager()
                    .get(&topic_name)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    min_entry_id.pruned_entry_id,
                    procedure.data.min_flushed_entry_id
                );

                // `check_heartbeat_collected_region_ids` fails.
                // Should log a warning and return `Status::Done`.
                procedure.context.leader_region_registry.reset();
                let status = procedure.on_prepare().await.unwrap();
                assert_matches!(status, Status::Done { output: None });

                // Clean up the topic.
                delete_topic(procedure.context.client, &topic_name).await;
            })
        })
        .await;
    }
}
