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

pub(crate) mod manager;
#[cfg(test)]
mod test_util;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_error::ext::BoxedError;
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
use common_telemetry::{info, warn};
use itertools::{Itertools, MinMaxResult};
use log_store::kafka::DEFAULT_PARTITION;
use manager::{WalPruneProcedureGuard, WalPruneProcedureTracker};
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::Client;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::logstore::EntryId;
use store_api::storage::RegionId;

use crate::error::{
    self, BuildPartitionClientSnafu, DeleteRecordsSnafu, TableMetadataManagerSnafu,
    UpdateTopicNameValueSnafu,
};
use crate::service::mailbox::{Channel, MailboxRef};
use crate::Result;

pub type KafkaClientRef = Arc<Client>;

const DELETE_RECORDS_TIMEOUT: Duration = Duration::from_secs(1);

/// The state of WAL pruning.
#[derive(Debug, Serialize, Deserialize)]
pub enum WalPruneState {
    Prepare,
    FlushRegion,
    Prune,
}

#[derive(Clone)]
pub struct Context {
    /// The Kafka client.
    pub client: KafkaClientRef,
    /// The table metadata manager.
    pub table_metadata_manager: TableMetadataManagerRef,
    /// The leader region registry.
    pub leader_region_registry: LeaderRegionRegistryRef,
    /// Server address of metasrv.
    pub server_addr: String,
    /// The mailbox to send messages.
    pub mailbox: MailboxRef,
}

/// The data of WAL pruning.
#[derive(Serialize, Deserialize)]
pub struct WalPruneData {
    /// The topic name to prune.
    pub topic: String,
    /// The minimum flush entry id for topic, which is used to prune the WAL.
    pub prunable_entry_id: EntryId,
    pub regions_to_flush: Vec<RegionId>,
    /// If `prunable_entry_id` + `trigger_flush_threshold` < `max_prunable_entry_id`, send a flush request to the region.
    /// If `None`, never send flush requests.
    pub trigger_flush_threshold: u64,
    /// The state.
    pub state: WalPruneState,
}

/// The procedure to prune WAL.
pub struct WalPruneProcedure {
    pub data: WalPruneData,
    pub context: Context,
    pub _guard: Option<WalPruneProcedureGuard>,
}

impl WalPruneProcedure {
    const TYPE_NAME: &'static str = "metasrv-procedure::WalPrune";

    pub fn new(
        topic: String,
        context: Context,
        trigger_flush_threshold: u64,
        guard: Option<WalPruneProcedureGuard>,
    ) -> Self {
        Self {
            data: WalPruneData {
                topic,
                prunable_entry_id: 0,
                trigger_flush_threshold,
                regions_to_flush: vec![],
                state: WalPruneState::Prepare,
            },
            context,
            _guard: guard,
        }
    }

    pub fn from_json(
        json: &str,
        context: &Context,
        tracker: WalPruneProcedureTracker,
    ) -> ProcedureResult<Self> {
        let data: WalPruneData = serde_json::from_str(json).context(ToJsonSnafu)?;
        let guard = tracker.insert_running_procedure(data.topic.clone());
        Ok(Self {
            data,
            context: context.clone(),
            _guard: guard,
        })
    }

    async fn build_peer_to_region_ids_map(
        &self,
        ctx: &Context,
        region_ids: &[RegionId],
    ) -> Result<HashMap<Peer, Vec<RegionId>>> {
        let table_ids = region_ids
            .iter()
            .map(|region_id| region_id.table_id())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let table_ids_table_routes_map = ctx
            .table_metadata_manager
            .table_route_manager()
            .batch_get_physical_table_routes(&table_ids)
            .await
            .context(TableMetadataManagerSnafu)?;

        let mut peer_region_ids_map = HashMap::new();
        for region_id in region_ids {
            let table_id = region_id.table_id();
            let table_route = match table_ids_table_routes_map.get(&table_id) {
                Some(route) => route,
                None => return error::TableRouteNotFoundSnafu { table_id }.fail(),
            };
            for region_route in &table_route.region_routes {
                if region_route.region.id != *region_id {
                    continue;
                }
                if let Some(peer) = &region_route.leader_peer {
                    peer_region_ids_map
                        .entry(peer.clone())
                        .or_insert_with(Vec::new)
                        .push(*region_id);
                }
            }
        }
        Ok(peer_region_ids_map)
    }

    fn build_flush_region_instruction(
        &self,
        peer_region_ids_map: HashMap<Peer, Vec<RegionId>>,
    ) -> Result<Vec<(Peer, Instruction)>> {
        let peer_and_instructions = peer_region_ids_map
            .into_iter()
            .map(|(peer, region_ids)| {
                let flush_instruction = Instruction::FlushRegion(FlushRegions { region_ids });
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
        let prunable_entry_ids_map: HashMap<_, _> = self
            .context
            .leader_region_registry
            .batch_get(region_ids.iter().cloned())
            .into_iter()
            .map(|(region_id, region)| {
                let prunable_entry_id = region.manifest.prunable_entry_id();
                (region_id, prunable_entry_id)
            })
            .collect();

        // Check if the `prunable_entry_ids_map` contains all region ids.
        let non_collected_region_ids =
            check_heartbeat_collected_region_ids(&region_ids, &prunable_entry_ids_map);
        if !non_collected_region_ids.is_empty() {
            // The heartbeat collected region ids do not contain all region ids in the topic-region map.
            // In this case, we should not prune the WAL.
            warn!("The heartbeat collected region ids do not contain all region ids in the topic-region map. Aborting the WAL prune procedure.
            topic: {}, non-collected region ids: {:?}", self.data.topic, non_collected_region_ids);
            return Ok(Status::done());
        }

        let min_max_result = prunable_entry_ids_map.values().minmax();
        let max_prunable_entry_id = match min_max_result {
            MinMaxResult::NoElements => {
                return Ok(Status::done());
            }
            MinMaxResult::OneElement(prunable_entry_id) => {
                self.data.prunable_entry_id = *prunable_entry_id;
                *prunable_entry_id
            }
            MinMaxResult::MinMax(min_prunable_entry_id, max_prunable_entry_id) => {
                self.data.prunable_entry_id = *min_prunable_entry_id;
                *max_prunable_entry_id
            }
        };
        if self.data.trigger_flush_threshold != 0 {
            for (region_id, prunable_entry_id) in prunable_entry_ids_map {
                if prunable_entry_id + self.data.trigger_flush_threshold < max_prunable_entry_id {
                    self.data.regions_to_flush.push(region_id);
                }
            }
            self.data.state = WalPruneState::FlushRegion;
        } else {
            self.data.state = WalPruneState::Prune;
        }
        Ok(Status::executing(true))
    }

    /// Send the flush request to regions with low flush entry id.
    ///
    /// Retry:
    /// - Failed to build peer to region ids map. It means failure in retrieving metadata.
    pub async fn on_sending_flush_request(&mut self) -> Result<Status> {
        let peer_to_region_ids_map = self
            .build_peer_to_region_ids_map(&self.context, &self.data.regions_to_flush)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: "Failed to build peer to region ids map",
            })?;
        let flush_instructions = self.build_flush_region_instruction(peer_to_region_ids_map)?;
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
            self.context.mailbox.send_oneway(&ch, msg).await?;
        }
        self.data.state = WalPruneState::Prune;
        Ok(Status::executing(true))
    }

    /// Prune the WAL and persist the minimum prunable entry id.
    ///
    /// Retry:
    /// - Failed to update the minimum prunable entry id in kvbackend.
    /// - Failed to delete records.
    pub async fn on_prune(&mut self) -> Result<Status> {
        // Safety: `prunable_entry_id`` are loaded in on_prepare.
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

        // Should update the min prunable entry id in the kv backend before deleting records.
        // Otherwise, when a datanode restarts, it will not be able to find the wal entries.
        let prev = self
            .context
            .table_metadata_manager
            .topic_name_manager()
            .get(&self.data.topic)
            .await
            .context(TableMetadataManagerSnafu)
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!("Failed to get TopicNameValue, topic: {}", self.data.topic),
            })?;
        self.context
            .table_metadata_manager
            .topic_name_manager()
            .update(&self.data.topic, self.data.prunable_entry_id, prev)
            .await
            .context(UpdateTopicNameValueSnafu {
                topic: &self.data.topic,
            })
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "Failed to update pruned entry id for topic: {}",
                    self.data.topic
                ),
            })?;
        partition_client
            .delete_records(
                (self.data.prunable_entry_id + 1) as i64,
                DELETE_RECORDS_TIMEOUT.as_millis() as i32,
            )
            .await
            .context(DeleteRecordsSnafu {
                topic: &self.data.topic,
                partition: DEFAULT_PARTITION,
                offset: (self.data.prunable_entry_id + 1),
            })
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "Failed to delete records for topic: {}, partition: {}, offset: {}",
                    self.data.topic,
                    DEFAULT_PARTITION,
                    self.data.prunable_entry_id + 1
                ),
            })?;
        info!(
            "Successfully pruned WAL for topic: {}, entry id: {}",
            self.data.topic, self.data.prunable_entry_id
        );
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
            WalPruneState::FlushRegion => self.on_sending_flush_request().await,
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
    use common_wal::test_util::run_test_with_kafka_wal;
    use rskafka::record::Record;
    use tokio::sync::mpsc::Receiver;

    use super::*;
    use crate::handler::HeartbeatMailbox;
    use crate::procedure::test_util::new_wal_prune_metadata;
    // Fix this import to correctly point to the test_util module
    use crate::procedure::wal_prune::test_util::TestEnv;

    /// Mock a test env for testing.
    /// Including:
    /// 1. Prepare some data in the table metadata manager and in-memory kv backend.
    /// 2. Return the procedure, the minimum last entry id to prune and the regions to flush.
    async fn mock_test_data(procedure: &WalPruneProcedure) -> (u64, Vec<RegionId>) {
        let n_region = 10;
        let n_table = 5;
        // 5 entries per region.
        let offsets = mock_wal_entries(
            procedure.context.client.clone(),
            &procedure.data.topic,
            (n_region * n_table * 5) as usize,
        )
        .await;
        let (prunable_entry_id, regions_to_flush) = new_wal_prune_metadata(
            procedure.context.table_metadata_manager.clone(),
            procedure.context.leader_region_registry.clone(),
            n_region,
            n_table,
            &offsets,
            procedure.data.trigger_flush_threshold,
            procedure.data.topic.clone(),
        )
        .await;
        (prunable_entry_id, regions_to_flush)
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
        expect_success: bool,
    ) {
        let partition_client = client
            .partition_client(topic_name, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap();
        let res = partition_client
            .fetch_records(entry_id, 0..10001, 5_000)
            .await;
        if expect_success {
            assert!(res.is_ok());
            let (record, _high_watermark) = res.unwrap();
            assert!(!record.is_empty());
        } else {
            let err = res.unwrap_err();
            // The error is in a private module so we check it through `to_string()`.
            assert!(err.to_string().contains("OffsetOutOfRange"));
        }
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
            .sorted_by_key(|a| a.as_u64())
            .collect::<Vec<_>>();
        flush_requested_region_ids.sort_by_key(|a| a.as_u64());
        assert_eq!(flush_requested_region_ids, sorted_region_ids);
    }

    #[tokio::test]
    async fn test_procedure_execution() {
        run_test_with_kafka_wal(|broker_endpoints| {
            Box::pin(async {
                common_telemetry::init_default_ut_logging();
                let mut topic_name = uuid::Uuid::new_v4().to_string();
                // Topic should start with a letter.
                topic_name = format!("test_procedure_execution-{}", topic_name);
                let mut env = TestEnv::new();
                let context = env.build_wal_prune_context(broker_endpoints).await;
                let mut procedure = WalPruneProcedure::new(topic_name.clone(), context, 10, None);

                // Before any data in kvbackend is mocked, should return a retryable error.
                let result = procedure.on_prune().await;
                assert_matches!(result, Err(e) if e.is_retryable());

                let (prunable_entry_id, regions_to_flush) = mock_test_data(&procedure).await;

                // Step 1: Test `on_prepare`.
                let status = procedure.on_prepare().await.unwrap();
                assert_matches!(
                    status,
                    Status::Executing {
                        persist: true,
                        clean_poisons: false
                    }
                );
                assert_matches!(procedure.data.state, WalPruneState::FlushRegion);
                assert_eq!(procedure.data.prunable_entry_id, prunable_entry_id);
                assert_eq!(
                    procedure.data.regions_to_flush.len(),
                    regions_to_flush.len()
                );
                for region_id in &regions_to_flush {
                    assert!(procedure.data.regions_to_flush.contains(region_id));
                }

                // Step 2: Test `on_sending_flush_request`.
                let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                env.mailbox
                    .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
                    .await;
                let status = procedure.on_sending_flush_request().await.unwrap();
                check_flush_request(&mut rx, &regions_to_flush).await;
                assert_matches!(
                    status,
                    Status::Executing {
                        persist: true,
                        clean_poisons: false
                    }
                );
                assert_matches!(procedure.data.state, WalPruneState::Prune);

                // Step 3: Test `on_prune`.
                let status = procedure.on_prune().await.unwrap();
                assert_matches!(status, Status::Done { output: None });
                // Check if the entry ids after `prunable_entry_id` still exist.
                check_entry_id_existence(
                    procedure.context.client.clone(),
                    &topic_name,
                    procedure.data.prunable_entry_id as i64 + 1,
                    true,
                )
                .await;
                // Check if the entry s before `prunable_entry_id` are deleted.
                check_entry_id_existence(
                    procedure.context.client.clone(),
                    &topic_name,
                    procedure.data.prunable_entry_id as i64,
                    false,
                )
                .await;

                let value = env
                    .table_metadata_manager
                    .topic_name_manager()
                    .get(&topic_name)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(value.pruned_entry_id, procedure.data.prunable_entry_id);

                // Step 4: Test `on_prepare`, `check_heartbeat_collected_region_ids` fails.
                // Should log a warning and return `Status::Done`.
                procedure.context.leader_region_registry.reset();
                let status = procedure.on_prepare().await.unwrap();
                assert_matches!(status, Status::Done { output: None });

                // Step 5: Test `on_prepare`, don't flush regions.
                procedure.data.trigger_flush_threshold = 0;
                procedure.on_prepare().await.unwrap();
                assert_matches!(procedure.data.state, WalPruneState::Prune);
                assert_eq!(value.pruned_entry_id, procedure.data.prunable_entry_id);

                // Clean up the topic.
                delete_topic(procedure.context.client, &topic_name).await;
            })
        })
        .await;
    }
}
