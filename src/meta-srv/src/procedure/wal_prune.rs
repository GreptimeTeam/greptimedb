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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_error::ext::BoxedError;
use common_meta::key::TableMetadataManagerRef;
use common_meta::lock_key::RemoteWalLock;
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
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use rskafka::client::Client;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::logstore::EntryId;
use store_api::storage::RegionId;

use crate::error::{
    self, BuildPartitionClientSnafu, DeleteRecordsSnafu, TableMetadataManagerSnafu,
    UpdateTopicNameValueSnafu,
};
use crate::Result;

pub type KafkaClientRef = Arc<Client>;

const DELETE_RECORDS_TIMEOUT: Duration = Duration::from_secs(5);

/// The state of WAL pruning.
#[derive(Debug, Serialize, Deserialize)]
pub enum WalPruneState {
    Prepare,
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
}

/// The data of WAL pruning.
#[derive(Serialize, Deserialize)]
pub struct WalPruneData {
    /// The topic name to prune.
    pub topic: String,
    /// The minimum flush entry id for topic, which is used to prune the WAL.
    pub prunable_entry_id: EntryId,
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

    pub fn new(topic: String, context: Context, guard: Option<WalPruneProcedureGuard>) -> Self {
        Self {
            data: WalPruneData {
                topic,
                prunable_entry_id: 0,
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
        match min_max_result {
            MinMaxResult::NoElements => {
                return Ok(Status::done());
            }
            MinMaxResult::OneElement(prunable_entry_id) => {
                self.data.prunable_entry_id = *prunable_entry_id;
            }
            MinMaxResult::MinMax(min_prunable_entry_id, _) => {
                self.data.prunable_entry_id = *min_prunable_entry_id;
            }
        };
        self.data.state = WalPruneState::Prune;
        Ok(Status::executing(false))
    }

    /// Prune the WAL and persist the minimum prunable entry id.
    ///
    /// Retry:
    /// - Failed to update the minimum prunable entry id in kvbackend.
    /// - Failed to delete records.
    pub async fn on_prune(&mut self) -> Result<Status> {
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
        let earliest_offset = partition_client
            .get_offset(OffsetAt::Earliest)
            .await
            .context(error::GetOffsetSnafu {
                topic: self.data.topic.clone(),
            })?;
        let latest_offset = partition_client
            .get_offset(OffsetAt::Latest)
            .await
            .context(error::GetOffsetSnafu {
                topic: self.data.topic.clone(),
            })?;
        if self.data.prunable_entry_id <= earliest_offset as u64 {
            warn!(
                "The prunable entry id is less or equal to the earliest offset, topic: {}, prunable entry id: {}, earliest offset: {}",
                self.data.topic, self.data.prunable_entry_id, earliest_offset as u64
            );
            return Ok(Status::done());
        }

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
                // notice here no "+1" is needed because the offset arg is exclusive, and it's defensive programming just in case somewhere else have a off by one error, see https://kafka.apache.org/36/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#endOffsets(java.util.Collection) which we use to get the end offset from high watermark
                self.data.prunable_entry_id as i64,
                DELETE_RECORDS_TIMEOUT.as_millis() as i32,
            )
            .await
            .context(DeleteRecordsSnafu {
                topic: &self.data.topic,
                partition: DEFAULT_PARTITION,
                offset: self.data.prunable_entry_id,
            })
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "Failed to delete records for topic: {}, prunable entry id: {}, latest offset: {}",
                    self.data.topic, self.data.prunable_entry_id, latest_offset as u64
                ),
            })?;

        info!(
            "Successfully pruned WAL for topic: {}, prunable entry id: {}, latest offset: {}",
            self.data.topic, self.data.prunable_entry_id, latest_offset as u64
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

    use common_wal::maybe_skip_kafka_integration_test;
    use common_wal::test_util::get_kafka_endpoints;
    use rskafka::record::Record;

    use super::*;
    use crate::procedure::test_util::new_wal_prune_metadata;
    // Fix this import to correctly point to the test_util module
    use crate::procedure::wal_prune::test_util::TestEnv;

    /// Mock a test env for testing.
    /// Including:
    /// 1. Prepare some data in the table metadata manager and in-memory kv backend.
    /// 2. Return the procedure, the minimum last entry id to prune and the regions to flush.
    async fn mock_test_data(procedure: &WalPruneProcedure) -> u64 {
        let n_region = 10;
        let n_table = 5;
        // 5 entries per region.
        let offsets = mock_wal_entries(
            procedure.context.client.clone(),
            &procedure.data.topic,
            (n_region * n_table * 5) as usize,
        )
        .await;
        let prunable_entry_id = new_wal_prune_metadata(
            procedure.context.table_metadata_manager.clone(),
            procedure.context.leader_region_registry.clone(),
            n_region,
            n_table,
            &offsets,
            procedure.data.topic.clone(),
        )
        .await;
        prunable_entry_id
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
                .unwrap()
                .offsets;
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

    #[tokio::test]
    async fn test_procedure_execution() {
        maybe_skip_kafka_integration_test!();
        let broker_endpoints = get_kafka_endpoints();

        common_telemetry::init_default_ut_logging();
        let mut topic_name = uuid::Uuid::new_v4().to_string();
        // Topic should start with a letter.
        topic_name = format!("test_procedure_execution-{}", topic_name);
        let env = TestEnv::new();
        let context = env.build_wal_prune_context(broker_endpoints).await;
        TestEnv::prepare_topic(&context.client, &topic_name).await;
        let mut procedure = WalPruneProcedure::new(topic_name.clone(), context, None);

        let prunable_entry_id = mock_test_data(&procedure).await;

        // Step 1: Test `on_prepare`.
        let status = procedure.on_prepare().await.unwrap();
        assert_matches!(
            status,
            Status::Executing {
                persist: false,
                clean_poisons: false
            }
        );
        assert_matches!(procedure.data.state, WalPruneState::Prune);
        assert_eq!(procedure.data.prunable_entry_id, prunable_entry_id);

        // Step 2: Test `on_prune`.
        let status = procedure.on_prune().await.unwrap();
        assert_matches!(status, Status::Done { output: None });
        // Check if the entry ids after(include) `prunable_entry_id` still exist.
        check_entry_id_existence(
            procedure.context.client.clone(),
            &topic_name,
            procedure.data.prunable_entry_id as i64,
            true,
        )
        .await;
        // Check if the entry ids before `prunable_entry_id` are deleted.
        check_entry_id_existence(
            procedure.context.client.clone(),
            &topic_name,
            procedure.data.prunable_entry_id as i64 - 1,
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

        // Step 3: Test `on_prepare`, `check_heartbeat_collected_region_ids` fails.
        // Should log a warning and return `Status::Done`.
        procedure.context.leader_region_registry.reset();
        let status = procedure.on_prepare().await.unwrap();
        assert_matches!(status, Status::Done { output: None });

        // Clean up the topic.
        delete_topic(procedure.context.client, &topic_name).await;
    }
}
