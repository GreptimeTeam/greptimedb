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

use common_telemetry::{debug, warn};
use common_wal::config::kafka::DatanodeKafkaConfig;
use futures::StreamExt;
use itertools::Itertools;
use rskafka::client::consumer::{StartOffset, StreamConsumer, StreamConsumerBuilder};
use rskafka::client::partition::OffsetAt;
use rskafka::record::Record;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::EntryId;

use crate::error::{
    CastEntryIdSnafu, CommitNothingSnafu, DecodeJsonSnafu, GetLatestOffsetSnafu, PullRecordSnafu,
    Result,
};
use crate::kafka::client_manager::ClientManager;
use crate::kafka::util::{EntryInner, RecordMeta};
use crate::kafka::{EntryImpl, NamespaceImpl};

/// Manages accesses on consumers. Each consumer pulls records from a specific topic and commits them in a batching manner.
#[derive(Debug)]
pub struct ConsumerManager {
    client_manager: Arc<ClientManager>,
    max_batch_size: i32,
    max_wait_ms: i32,
}

impl ConsumerManager {
    /// Creates a consumer manager.
    pub fn new(config: &DatanodeKafkaConfig, client_manager: Arc<ClientManager>) -> Self {
        Self {
            client_manager,
            max_batch_size: config.max_batch_size.as_bytes() as i32,
            max_wait_ms: config.consumer_wait_timeout.as_millis() as i32,
        }
    }

    /// Returns a new consumer to pull records starting from the given offset.
    pub async fn consumer(
        &self,
        topic: String,
        region_id: Option<u64>,
        start_entry_id: EntryId,
    ) -> Result<Option<Consumer>> {
        // The returned offset is the offset of the next upcoming record. Therefore, the actual last offset should be minus one.
        let client = self.client_manager.get_or_insert(&topic).await?;
        let last_offset = client
            .get_offset(OffsetAt::Latest)
            .await
            .with_context(|_| GetLatestOffsetSnafu {
                topic: topic.clone(),
            })?
            - 1;

        let start_offset = i64::try_from(start_entry_id).context(CastEntryIdSnafu)?;
        if start_offset > last_offset {
            debug!(
                "Abort comsumption, there're no new logs. topic = {}, region = {:?}, start offset = {}, last offset = {}",
                topic, region_id, start_offset, last_offset
            );
            return Ok(None);
        }

        let consumer = StreamConsumerBuilder::new(client, StartOffset::At(start_offset))
            .with_max_batch_size(self.max_batch_size)
            .with_max_wait_ms(self.max_wait_ms)
            .build();
        Ok(Some(Consumer {
            inner: consumer,
            curr_offset: start_offset,
            last_offset,
            topic,
            region_id,
            entry_stage: HashMap::new(),
        }))
    }
}

/// A consumer that pulls records from a specific topic and commits them in a batching manner.
pub struct Consumer {
    /// A consumer that pulls records in a streaming manner.
    inner: StreamConsumer,
    /// The offset of the latest pulled record.
    curr_offset: i64,
    /// The offset the last record the consumer should pull.
    last_offset: i64,
    /// The topic to pull records from.
    topic: String,
    /// If a region id is provided, a record is filtered out if it does not contains entries associated with the given region id.
    region_id: Option<u64>,
    /// Stages entries by timestamp.
    entry_stage: HashMap<i64, Vec<EntryInner>>,
}

enum DecodeResult {
    NormalEntries(Vec<EntryInner>),
    CommitRecord(i64),
    Filtered,
}

impl Consumer {
    pub async fn next(&mut self) -> Result<Vec<EntryImpl>> {
        if self.curr_offset >= self.last_offset {
            return Ok(vec![]);
        }

        while let Some(result) = self.inner.next().await {
            let (record_and_offset, _) = result.with_context(|_| PullRecordSnafu {
                topic: self.topic.clone(),
            })?;
            let record = record_and_offset.record;
            self.curr_offset = record_and_offset.offset;
            debug!(
                "Pulled a record. topic = {}, offset = {}",
                self.topic, self.curr_offset
            );

            match self.decode_record(record)? {
                DecodeResult::NormalEntries(entries) => {
                    // Safety: decode_record guarantees the entries to be not empty.
                    self.entry_stage
                        .entry(entries[0].timestamp)
                        .or_default()
                        .extend(entries);
                }
                DecodeResult::CommitRecord(timestamp) => {
                    let entries: Vec<EntryImpl> = self
                        .entry_stage
                        .get_mut(&timestamp)
                        .context(CommitNothingSnafu { timestamp })?
                        .drain(..)
                        .filter(|entry| entry.timestamp == timestamp)
                        .group_by(|entry| entry.id)
                        .into_iter()
                        .map(|(_, group)| {
                            let entries = group
                                .sorted_by(|a, b| a.seq.cmp(&b.seq))
                                .collect::<Vec<_>>();
                            EntryImpl {
                                // Overwrites the entry id with the Kafka offset since the entry id affects wal replay.
                                id: self.curr_offset as u64,
                                // Safety: a group is guaranteed to be not empty.
                                ns: NamespaceImpl {
                                    region_id: entries[0].region_id,
                                    topic: self.topic.clone(),
                                },
                                data: entries.into_iter().flat_map(|entry| entry.data).collect(),
                            }
                        })
                        .collect::<Vec<_>>();
                    debug!(
                        "Commit {} entries. topic = {}, regions = {:?}, timestamp = {}",
                        entries.len(),
                        self.topic,
                        entries
                            .iter()
                            .map(|entry| entry.ns.region_id)
                            .collect::<Vec<_>>(),
                        timestamp,
                    );
                    return Ok(entries);
                }
                DecodeResult::Filtered => {}
            }

            if self.curr_offset >= self.last_offset {
                break;
            }
        }
        Ok(vec![])
    }

    fn decode_record(&self, record: Record) -> Result<DecodeResult> {
        // A record with None key is regarded as a no-op record and is filtered out.
        let Some(key) = record.key else {
            return Ok(DecodeResult::Filtered);
        };

        let timestamp = record.timestamp.timestamp_millis();
        // A record with Some key but None value is regarded as a commit record.
        let Some(data) = record.value else {
            let regions = serde_json::from_slice::<Vec<u64>>(&key).context(DecodeJsonSnafu)?;
            // Conditionally filter out the commit record if a region id is provided.
            if let Some(region_id) = self.region_id
                && regions.into_iter().all(|id| id != region_id)
            {
                return Ok(DecodeResult::Filtered);
            }
            return Ok(DecodeResult::CommitRecord(timestamp));
        };

        let meta = serde_json::from_slice::<RecordMeta>(&key).context(DecodeJsonSnafu)?;
        let mut entries = Vec::new();
        let mut offset = 0;
        for entry_meta in meta.entry_metas {
            // Conditionally filter out entries by region id.
            if let Some(region_id) = self.region_id
                && entry_meta.region_id != region_id
            {
                offset += entry_meta.length;
                continue;
            }
            entries.push(EntryInner {
                data: data[offset..offset + entry_meta.length].to_vec(),
                id: entry_meta.id,
                seq: entry_meta.seq,
                region_id: entry_meta.region_id,
                timestamp,
            });
            offset += entry_meta.length;
        }
        if entries.is_empty() {
            return Ok(DecodeResult::Filtered);
        }
        debug!(
            "Cached {} entries. topic = {}, regions = {:?}, timestamp = {}",
            entries.len(),
            self.topic,
            entries
                .iter()
                .map(|entry| entry.region_id)
                .collect::<Vec<_>>(),
            timestamp,
        );
        Ok(DecodeResult::NormalEntries(entries))
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        // Alerts if there're any entry leftovers.
        // Leftovers are generated due to partial failure of producing entries.
        for (timestamp, entries) in self.entry_stage.iter() {
            if !entries.is_empty() {
                warn!(
                    "Found entry leftovers. len = {}, topic = {}, regions = {:?}, timestamp = {}",
                    entries.len(),
                    self.topic,
                    entries
                        .iter()
                        .map(|entry| entry.region_id)
                        .collect::<Vec<_>>(),
                    timestamp,
                );
            }
        }
    }
}
