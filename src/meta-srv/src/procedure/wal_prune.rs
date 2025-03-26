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

use std::sync::Arc;

use common_error::ext::BoxedError;
use common_meta::key::TableMetadataManagerRef;
use common_meta::lock_key::RemoteWalLock;
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
    pub table_metadata_manager: TableMetadataManagerRef,
}

/// The data of WAL pruning.
#[derive(Serialize, Deserialize)]
pub struct WalPruneData {
    /// The topic names to prune.
    pub topics: Vec<String>,
    /// The last entry id for each region.
    pub last_entry_ids: Option<Vec<Option<EntryId>>>,
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

    pub fn new(topics: Vec<String>, context: Context) -> Self {
        Self {
            data: WalPruneData {
                topics,
                last_entry_ids: None,
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
        let topic_region_map = self
            .context
            .table_metadata_manager
            .topic_region_manager()
            .get_regions_by_topics(&self.data.topics)
            .await
            .context(TableMetadataManagerSnafu)
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: "Failed to get topic-region map",
            })?;
        let regions = topic_region_map
            .values()
            .flatten()
            .copied()
            .collect::<Vec<RegionId>>();
        let last_entry_ids_map = self
            .context
            .table_metadata_manager
            .topic_region_manager()
            .get_region_last_entry_ids(regions)
            .await;
        // Map last entry id to each topic
        let mut last_entry_ids = Vec::with_capacity(self.data.topics.len());
        for topic in &self.data.topics {
            // Safety: the topic must exist in the map.
            let region_ids = topic_region_map.get(topic).unwrap();
            // `None` means no region for the topic.
            if region_ids.is_empty() {
                last_entry_ids.push(None);
                continue;
            }
            let mut max_last_entry_id = 0;
            for region_id in region_ids {
                let last_entry_id = last_entry_ids_map.get(region_id).copied();
                if let Some(last_entry_id) = last_entry_id {
                    // We should use the biggest last entry id.
                    max_last_entry_id = max_last_entry_id.max(last_entry_id);
                }
            }
            if max_last_entry_id == 0 {
                last_entry_ids.push(None);
            } else {
                last_entry_ids.push(Some(max_last_entry_id));
            }
        }
        self.data.last_entry_ids = Some(last_entry_ids);
        self.data.state = WalPruneState::Prune;
        Ok(Status::executing(true))
    }

    /// Prune the WAL.
    pub async fn on_prune(&mut self) -> Result<Status> {
        // Safety: last_entry_ids are loaded in on_prepare.
        for (topic, last_entry_id) in self
            .data
            .topics
            .iter()
            .zip(self.data.last_entry_ids.as_ref().unwrap())
        {
            if let Some(last_entry_id) = last_entry_id {
                let partition_client = self
                    .context
                    .client
                    .partition_client(topic, DEFAULT_PARTITION, UnknownTopicHandling::Retry)
                    .await
                    .context(BuildPartitionClientSnafu {
                        topic,
                        partition: DEFAULT_PARTITION,
                    })?;

                partition_client
                    .delete_records((*last_entry_id) as i64, TIMEOUT)
                    .await
                    .context(DeleteRecordSnafu {
                        topic,
                        partition: DEFAULT_PARTITION,
                        offset: *last_entry_id,
                    })?;
            }
        }
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

    fn lock_key(&self) -> LockKey {
        let lock_key = vec![RemoteWalLock::Read.into()];
        LockKey::new(lock_key)
    }
}
