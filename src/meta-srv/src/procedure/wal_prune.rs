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
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_meta::key::TableMetadataManagerRef;
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_procedure::error::ToJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use log_store::kafka::DEFAULT_PARTITION;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::Client;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::logstore::EntryId;
use store_api::storage::RegionId;

use crate::error::{self, BuildPartitionClientSnafu, DeleteRecordSnafu, TableMetadataManagerSnafu};
use crate::Result;

type KafkaClientRef = Arc<Client>;

const TIMEOUT: i32 = 100;

/// The state of WAL pruning.
#[derive(Debug, Serialize, Deserialize)]
pub enum WalPruneState {
    Prepare,
    Prune,
}

pub struct Context {
    /// The Kafka client.
    client: KafkaClientRef,
    /// The table metadata manager.
    table_metadata_manager: TableMetadataManagerRef,
    leader_region_registry: LeaderRegionRegistryRef,
}

/// The data of WAL pruning.
#[derive(Serialize, Deserialize)]
pub struct WalPruneData {
    /// The topic name to prune.
    pub topic: String,
    /// The minimum flush entry id for topic, which is used to prune the WAL.
    /// If the topic has no region, the value is set to `None`.
    pub min_flush_entry_id: EntryId,
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

    pub fn new(topic: String, context: Context) -> Self {
        Self {
            data: WalPruneData {
                topic,
                min_flush_entry_id: 0,
                state: WalPruneState::Prepare,
            },
            context,
        }
    }

    pub fn from_json(json: &str, context: Context) -> ProcedureResult<Self> {
        let data: WalPruneData = serde_json::from_str(json).context(ToJsonSnafu)?;
        Ok(Self { data, context })
    }

    /// Calculate the last entry id to prune for each topic.
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
        if !check_heartbeat_collected_region_ids(&region_ids, &flush_entry_ids_map)
            || region_ids.is_empty()
        {
            return Ok(Status::done());
        }

        // Safety: `flush_entry_ids_map` are not empty.
        self.data.min_flush_entry_id = *(flush_entry_ids_map.values().min().unwrap());
        self.data.state = WalPruneState::Prune;
        Ok(Status::executing(true))
    }

    /// Prune the WAL.
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

        partition_client
            .delete_records(self.data.min_flush_entry_id as i64, TIMEOUT)
            .await
            .context(DeleteRecordSnafu {
                topic: self.data.topic.clone(),
                partition: DEFAULT_PARTITION,
                offset: self.data.min_flush_entry_id,
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
        LockKey::new(vec![])
    }
}

/// Check if the heartbeat collected region ids contains all region ids in the topic-region map.
fn check_heartbeat_collected_region_ids(
    region_ids: &[RegionId],
    heartbeat_collected_region_ids: &HashMap<RegionId, u64>,
) -> bool {
    region_ids
        .iter()
        .all(|region_id| heartbeat_collected_region_ids.contains_key(region_id))
}
