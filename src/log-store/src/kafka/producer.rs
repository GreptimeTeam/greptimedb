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
use std::time::Duration;

use chrono::DateTime;
use common_wal::config::kafka::DatanodeKafkaConfig;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use rskafka::BackoffConfig;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::logstore::EntryId;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::error::{
    BuildClientSnafu, BuildPartitionClientSnafu, EncodeJsonSnafu, EntryTooLargeSnafu, FlushSnafu,
    ProduceSnafu, Result,
};
use crate::kafka::{EntryImpl, NamespaceImpl};

type Sequence = u8;

const RECORD_VERSION: u32 = 0;
const DEFAULT_PARTITION: i32 = 0;

#[derive(Debug, Serialize, Deserialize)]
pub struct EntryInner {
    data: Vec<u8>,
    id: EntryId,
    seq: Sequence,
    pub ns: NamespaceImpl,
    timestamp: i64,
}

pub fn maybe_split_entry(
    entry: EntryImpl,
    max_entry_size: usize,
    timestamp: i64,
) -> Vec<EntryInner> {
    if entry.data.len() <= max_entry_size {
        return vec![EntryInner {
            data: entry.data,
            id: entry.id,
            seq: 0,
            ns: entry.ns,
            timestamp,
        }];
    }

    entry
        .data
        .chunks(max_entry_size)
        .enumerate()
        .map(|(i, chunk)| EntryInner {
            data: chunk.to_vec(),
            id: entry.id,
            seq: i as u8,
            ns: entry.ns.clone(),
            timestamp,
        })
        .collect()
}

#[derive(Debug)]
pub struct ProducerManager {
    producers: RwLock<HashMap<String, Producer>>,
    client_factory: Client,
    buffer_capacity: usize,
    linger: Duration,
    compression: Compression,
}

impl ProducerManager {
    pub async fn try_new(config: &DatanodeKafkaConfig) -> Result<Self> {
        let client = ClientBuilder::new(config.broker_endpoints.clone())
            .backoff_config(BackoffConfig {
                init_backoff: config.backoff.init,
                max_backoff: config.backoff.max,
                base: config.backoff.base as f64,
                deadline: config.backoff.deadline,
            })
            .build()
            .await
            .with_context(|_| BuildClientSnafu {
                broker_endpoints: config.broker_endpoints.clone(),
            })?;
        Ok(Self {
            producers: RwLock::new(HashMap::new()),
            client_factory: client,
            buffer_capacity: config.max_batch_size.as_bytes() as usize,
            linger: config.linger,
            compression: config.compression,
        })
    }

    pub async fn get_or_insert(&self, topic: &str) -> Result<Producer> {
        {
            let producer_map = self.producers.read().await;
            if let Some(producer) = producer_map.get(topic) {
                return Ok(producer.clone());
            }
        }

        let mut producer_map = self.producers.write().await;
        match producer_map.get(topic) {
            Some(producer) => Ok(producer.clone()),
            None => {
                let client = self
                    .client_factory
                    .partition_client(topic, DEFAULT_PARTITION, UnknownTopicHandling::Retry)
                    .await
                    .with_context(|_| BuildPartitionClientSnafu {
                        topic,
                        partition: DEFAULT_PARTITION,
                    })?;
                let producer = Producer {
                    linger: self.linger,
                    inner: Arc::new(parking_lot::Mutex::new(ProducerInner::new(
                        client,
                        self.buffer_capacity,
                        self.compression,
                    ))),
                };
                producer_map.insert(topic.to_string(), producer.clone());
                Ok(producer)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Producer {
    linger: Duration,
    inner: Arc<parking_lot::Mutex<ProducerInner>>,
}

impl Producer {
    pub async fn produce(&self, entry: EntryInner) -> Result<i64> {
        let job = {
            let mut inner = self.inner.lock();
            inner.produce(entry)?
        };
        let result = match job {
            CallerJob::Wait(notifier) => notifier.wait().await,
            CallerJob::Linger {
                notifier,
                buffer_id,
            } => {
                let linger = tokio::spawn({
                    let linger = self.linger;
                    let inner = self.inner.clone();
                    async move {
                        tokio::time::sleep(linger).await;
                        inner.lock().flush(Some(buffer_id))
                    }
                });

                tokio::select! {
                    // FIXME(niebayes): handle join error.
                    linger_result = linger => {
                        linger_result.unwrap()?;
                        notifier.wait().await
                    }
                    produce_result = notifier.wait() => produce_result,
                }
            }
        };
        result.map_err(|e| ProduceSnafu { error: e }.build())
    }
}

#[derive(Debug)]
struct ProducerInner {
    buffer: Option<EntryBuffer>,
    buffer_id: usize,
    has_linger_waiter: bool,
    pending_flush_tasks: Vec<JoinHandle<Result<()>>>,
    client: Arc<PartitionClient>,
    compression: Compression,
}

enum CallerJob {
    /// Caller should wait for the produce result.
    Wait(Arc<ProduceResultNotifier>),
    /// Caller should spawn a task to sleep during the linger and then try to flush the buffer with the associated buffer id.
    Linger {
        notifier: Arc<ProduceResultNotifier>,
        buffer_id: usize,
    },
}

type ProduceResult = std::result::Result<i64, String>;

#[derive(Debug, Default)]
pub struct ProduceResultNotifier {
    result: RwLock<Option<ProduceResult>>,
    notify: tokio::sync::Notify,
}

impl ProduceResultNotifier {
    async fn wait(&self) -> ProduceResult {
        if let Some(result) = self.result.read().await.as_ref() {
            return result.clone();
        }

        self.notify.notified().await;
        self.result.read().await.as_ref().unwrap().clone()
    }

    async fn notify_waiters(&self, result: ProduceResult) {
        let mut result_guard = self.result.write().await;
        *result_guard = Some(result);
        self.notify.notify_waiters();
    }
}

impl ProducerInner {
    fn new(client: PartitionClient, buffer_capacity: usize, compression: Compression) -> Self {
        Self {
            buffer: Some(EntryBuffer::new(buffer_capacity)),
            buffer_id: 0,
            pending_flush_tasks: Vec::new(),
            has_linger_waiter: false,
            client: Arc::new(client),
            compression,
        }
    }

    fn produce(&mut self, entry: EntryInner) -> Result<CallerJob> {
        let notifier = match self.buffer.as_mut().unwrap().try_push(entry)? {
            PushResult::Ok(notifier) => notifier,
            PushResult::NoCapacity(entry) => {
                self.flush(None)?;

                match self.buffer.as_mut().unwrap().try_push(entry).unwrap() {
                    PushResult::Ok(notifier) => notifier,
                    PushResult::NoCapacity(_) => unreachable!(),
                }
            }
        };

        if self.has_linger_waiter {
            return Ok(CallerJob::Wait(notifier));
        }

        self.has_linger_waiter = true;
        Ok(CallerJob::Linger {
            notifier,
            buffer_id: self.buffer_id,
        })
    }

    fn flush(&mut self, buffer_id: Option<usize>) -> Result<()> {
        if let Some(buffer_id) = buffer_id
            && buffer_id != self.buffer_id
        {
            return Ok(());
        }

        self.pending_flush_tasks
            .retain_mut(|task| !task.is_finished());

        let flush_task = self
            .buffer
            .replace(EntryBuffer::new(self.buffer.as_ref().unwrap().capacity))
            .unwrap()
            .start_background_flush(self.client.clone(), self.compression);
        self.pending_flush_tasks.push(flush_task);

        self.buffer_id = self.buffer_id.wrapping_add(1);
        self.has_linger_waiter = false;
        Ok(())
    }
}

impl Drop for ProducerInner {
    fn drop(&mut self) {
        self.pending_flush_tasks
            .drain(..)
            .for_each(|task| task.abort());
    }
}

pub enum PushResult {
    Ok(Arc<ProduceResultNotifier>),
    NoCapacity(EntryInner),
}

#[derive(Debug)]
pub struct EntryBuffer {
    entries: Vec<EntryInner>,
    accumulated: usize,
    capacity: usize,
    notifier: Arc<ProduceResultNotifier>,
}

impl EntryBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Vec::new(),
            accumulated: 0,
            capacity,
            notifier: Arc::new(ProduceResultNotifier::default()),
        }
    }

    pub fn try_push(&mut self, entry: EntryInner) -> Result<PushResult> {
        let entry_size = entry.data.len();
        ensure!(
            entry_size <= self.capacity,
            EntryTooLargeSnafu {
                entry_size,
                capacity: self.capacity
            }
        );

        if self.accumulated + entry_size > self.capacity {
            return Ok(PushResult::NoCapacity(entry));
        }

        self.entries.push(entry);
        self.accumulated += entry_size;
        Ok(PushResult::Ok(self.notifier.clone()))
    }

    pub fn start_background_flush(
        self,
        client: Arc<PartitionClient>,
        compression: Compression,
    ) -> JoinHandle<Result<()>> {
        tokio::spawn({
            async move {
                let record = Self::build_record(self.entries)?;
                // TODO(niebayes): the max batch size in the client side is not identical with that in the server side.
                let result = client
                    .produce(vec![record], compression)
                    .await
                    .map(|offsets| offsets[0])
                    .with_context(|_| FlushSnafu {
                        topic: client.topic(),
                    })
                    .map_err(|e| e.to_string());
                self.notifier.notify_waiters(result).await;
                Ok(())
            }
        })
    }

    fn build_record(entries: Vec<EntryInner>) -> Result<Record> {
        let mut builder = RecordBuilder::new(entries.len(), RECORD_VERSION, entries[0].timestamp);
        entries.into_iter().for_each(|entry| builder.push(entry));
        builder.try_build()
    }
}

#[derive(Serialize, Deserialize)]
struct EntryMeta {
    id: EntryId,
    seq: Sequence,
    ns: NamespaceImpl,
    length: usize,
}

#[derive(Default, Serialize, Deserialize)]
struct RecordMeta {
    version: u32,
    entry_metas: Vec<EntryMeta>,
}

struct RecordBuilder {
    timestamp: i64,
    meta: RecordMeta,
    data: Vec<Vec<u8>>,
}

impl RecordBuilder {
    fn new(capacity: usize, version: u32, timestamp: i64) -> Self {
        Self {
            timestamp,
            meta: RecordMeta {
                version,
                entry_metas: Vec::with_capacity(capacity),
            },
            data: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, entry: EntryInner) {
        self.meta.entry_metas.push(EntryMeta {
            id: entry.id,
            seq: entry.seq,
            ns: entry.ns,
            length: entry.data.len(),
        });
        self.data.push(entry.data);
    }

    fn try_build(self) -> Result<Record> {
        let encoded_meta = serde_json::to_vec(&self.meta).context(EncodeJsonSnafu)?;
        Ok(Record {
            key: Some(encoded_meta),
            value: Some(self.data.into_iter().flatten().collect()),
            headers: Default::default(),
            timestamp: DateTime::from_timestamp_millis(self.timestamp).unwrap(),
        })
    }
}
