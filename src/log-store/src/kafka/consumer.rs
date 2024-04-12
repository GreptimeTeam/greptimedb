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

use std::cmp::Ordering;
use std::sync::Arc;

use common_wal::config::kafka::DatanodeKafkaConfig;
use futures::StreamExt;
use itertools::Itertools;
use rskafka::client::consumer::{StartOffset, StreamConsumer, StreamConsumerBuilder};
use rskafka::client::partition::OffsetAt;
use rskafka::record::Record;
use snafu::ResultExt;
use store_api::logstore::EntryId;

use crate::error::{ConsumeRecordSnafu, DecodeJsonSnafu, GetOffsetSnafu, Result};
use crate::kafka::client_manager::ClientManager;
use crate::kafka::producer::{EntryInner, RecordMeta};
use crate::kafka::{EntryImpl, NamespaceImpl};

#[derive(Debug)]
pub struct ConsumerManager {
    client_manager: Arc<ClientManager>,
    max_batch_size: i32,
    max_wait_ms: i32,
}

impl ConsumerManager {
    pub fn new(config: &DatanodeKafkaConfig, client_manager: Arc<ClientManager>) -> Self {
        Self {
            client_manager,
            max_batch_size: config.max_batch_size.as_bytes() as i32,
            max_wait_ms: config.consumer_wait_timeout.as_millis() as i32,
        }
    }

    pub async fn consumer(
        &self,
        ns: NamespaceImpl,
        start_entry_id: EntryId,
    ) -> Result<Option<Consumer>> {
        // FIXME(niebayes): handle cast error.
        let start_offset = i64::try_from(start_entry_id).unwrap();
        let client = self.client_manager.get_or_insert(&ns.topic).await?;
        let last_offset = client
            .get_offset(OffsetAt::Latest)
            .await
            .context(GetOffsetSnafu { ns: ns.clone() })?
            - 1;
        if start_offset >= last_offset {
            return Ok(None);
        }

        let consumer = StreamConsumerBuilder::new(client, StartOffset::At(start_offset))
            .with_max_batch_size(self.max_batch_size)
            .with_max_wait_ms(self.max_wait_ms)
            .build();
        Ok(Some(Consumer {
            inner: consumer,
            last_offset,
            ns,
            entry_stage: Vec::new(),
        }))
    }
}

pub struct Consumer {
    inner: StreamConsumer,
    last_offset: i64,
    ns: NamespaceImpl,
    entry_stage: Vec<EntryInner>,
}

enum DecodeResult {
    NormalEntries(Vec<EntryInner>),
    Checkpoint(i64),
    NoOp,
    Filtered,
}

impl Consumer {
    pub async fn next(&mut self) -> Result<Vec<EntryImpl>> {
        while let Some(result) = self.inner.next().await {
            let (record_and_offset, _) = result.with_context(|_| ConsumeRecordSnafu {
                ns: self.ns.clone(),
            })?;
            let record = record_and_offset.record;
            let offset = record_and_offset.offset;
            match self.decode_record(record)? {
                DecodeResult::NormalEntries(entries) => self.entry_stage.extend(entries),
                DecodeResult::Checkpoint(timestamp) => {
                    let entries = self
                        .entry_stage
                        .drain(..)
                        .filter(|entry| entry.timestamp == timestamp)
                        .sorted_by(|a, b| match a.id.cmp(&b.id) {
                            Ordering::Equal => a.seq.cmp(&b.seq),
                            other => other,
                        })
                        .map(|entry| EntryImpl {
                            data: entry.data,
                            id: offset as u64,
                            ns: entry.ns,
                        })
                        .collect();
                    return Ok(entries);
                }
                DecodeResult::NoOp | DecodeResult::Filtered => {}
            }
            if offset >= self.last_offset {
                break;
            }
        }
        Ok(vec![])
    }

    fn decode_record(&self, record: Record) -> Result<DecodeResult> {
        if record.key.is_none() {
            return Ok(DecodeResult::NoOp);
        }

        let meta =
            serde_json::from_slice::<RecordMeta>(&record.key.unwrap()).context(DecodeJsonSnafu)?;
        let timestamp = record.timestamp.timestamp_millis();

        let mut entries = Vec::new();
        let mut offset = 0;
        for entry_meta in meta.entry_metas {
            if entry_meta.ns != self.ns {
                offset += entry_meta.length;
                continue;
            }
            let Some(data) = record.value.as_ref() else {
                return Ok(DecodeResult::Checkpoint(timestamp));
            };
            entries.push(EntryInner {
                data: data[offset..offset + entry_meta.length].to_vec(),
                id: entry_meta.id,
                seq: entry_meta.seq,
                ns: entry_meta.ns,
                timestamp,
            });
            offset += entry_meta.length;
        }
        if entries.is_empty() {
            return Ok(DecodeResult::Filtered);
        }
        Ok(DecodeResult::NormalEntries(entries))
    }
}
