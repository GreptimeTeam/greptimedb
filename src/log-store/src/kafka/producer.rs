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

use common_telemetry::debug;
use common_wal::config::kafka::DatanodeKafkaConfig;
use rskafka::client::partition::{Compression, PartitionClient};
use rskafka::record::Record;
use snafu::{ensure, OptionExt, ResultExt};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use super::util::maybe_split_entry;
use crate::error::{
    CastTimestampSnafu, EncodeJsonSnafu, EntryTooLargeSnafu, JoinTaskSnafu, ProduceEntriesSnafu,
    ProduceRecordSnafu, Result,
};
use crate::kafka::client_manager::ClientManager;
use crate::kafka::util::{EntryInner, RecordBuilder};
use crate::kafka::EntryImpl;

/// Manages accesses on producers. Each producer appends entries in a batching manner to a specific topic.
#[derive(Debug)]
pub struct ProducerManager {
    producers: RwLock<HashMap<String, Producer>>,
    max_entry_size: usize,
    linger: Duration,
    compression: Compression,
    client_manager: Arc<ClientManager>,
}

impl ProducerManager {
    /// Creates a producer manager.
    pub fn new(config: &DatanodeKafkaConfig, client_manager: Arc<ClientManager>) -> Self {
        Self {
            producers: RwLock::new(HashMap::new()),
            max_entry_size: config.max_batch_size.as_bytes() as usize,
            linger: config.linger,
            compression: config.compression,
            client_manager,
        }
    }

    /// Gets the producer associated with given topic. Constructs the producer if it does not exist yet.
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
                let producer = Producer {
                    inner: Arc::new(parking_lot::Mutex::new(ProducerInner::new(
                        self.client_manager.get_or_insert(topic).await?,
                        self.max_entry_size,
                        self.compression,
                    ))),
                    linger: self.linger,
                    max_entry_size: self.max_entry_size,
                };
                producer_map.insert(topic.to_string(), producer.clone());
                Ok(producer)
            }
        }
    }
}

/// A producer appends entries to a specific topic in a batching manner.
#[derive(Debug, Clone)]
pub struct Producer {
    inner: Arc<parking_lot::Mutex<ProducerInner>>,
    max_entry_size: usize,
    linger: Duration,
}

impl Producer {
    /// Produces a batch of entries belong to the same topic.
    /// Returns the regions the entries associated with and the offset of the commit record.
    pub async fn produce(&self, entries: Vec<EntryImpl>) -> Result<(Vec<u64>, i64)> {
        // Appends entries to the topic. A large entry will be split into smaller ones.
        // The timestamp is used to identify entries produced in the same batch.
        let timestamp = chrono::Utc::now().timestamp_millis();
        let (regions, tasks): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .flat_map(|entry| maybe_split_entry(entry, self.max_entry_size, timestamp))
            .map(|entry| (entry.region_id, self.produce_entry(entry)))
            .unzip();
        let _ = futures::future::try_join_all(tasks).await?;

        // Appends a commit record as a mark of successful production.
        // A consumer will buffer entries until a commit record arrives.
        let (client, compression) = {
            let inner = self.inner.lock();
            (inner.client.clone(), inner.compression)
        };
        let offset = client
            .produce(
                vec![Self::build_commit_record(&regions, timestamp)?],
                compression,
            )
            .await
            .map(|offsets| offsets[0])
            .with_context(|_| ProduceRecordSnafu {
                topic: client.topic(),
            })?;
        debug!(
            "Produced entries belong to regions {:?}. offset = {}, timestamp = {}",
            regions, offset, timestamp
        );
        Ok((regions, offset))
    }

    async fn produce_entry(&self, entry: EntryInner) -> Result<()> {
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
                // Starts a linger and tries to flush the buffer when it expires.
                let linger = tokio::spawn({
                    let linger = self.linger;
                    let inner = self.inner.clone();
                    async move {
                        tokio::time::sleep(linger).await;
                        inner.lock().flush(Some(buffer_id))
                    }
                });
                // We have to wait for both the linger and the produce results
                // since the buffer may be flushed before the linger expires.
                tokio::select! {
                    linger_result = linger => {
                        let _ = linger_result.context(JoinTaskSnafu)?;
                        notifier.wait().await
                    }
                    produce_result = notifier.wait() => produce_result,
                }
            }
        };
        result.map_err(|e| ProduceEntriesSnafu { error: e }.build())
    }

    fn build_commit_record(regions: &[u64], timestamp: i64) -> Result<Record> {
        let regions = serde_json::to_vec(regions).context(EncodeJsonSnafu)?;
        Ok(Record {
            key: Some(regions),
            value: None,
            headers: Default::default(),
            timestamp: chrono::DateTime::from_timestamp_millis(timestamp)
                .context(CastTimestampSnafu)?,
        })
    }
}

enum CallerJob {
    /// Caller should wait for the produce result.
    Wait(Arc<ProduceResultNotifier>),
    /// Caller should start a linger and try to flush the buffer when the linger expires.
    Linger {
        notifier: Arc<ProduceResultNotifier>,
        buffer_id: usize,
    },
}

type ProduceResult = std::result::Result<(), String>;

/// Each waiter is able to wait for the produce result through the notifier.
/// The produce result is ready to be observed when the buffer is flused.
#[derive(Debug, Default)]
struct ProduceResultNotifier {
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

#[derive(Debug)]
struct ProducerInner {
    /// A buffer that caches entries belong to the same topic.
    /// It's wrapped in an Option to support replacing it with a new instance and flusing the old one concurrently.
    buffer: Option<EntryBuffer>,
    /// The id of the current buffer. The buffer is flushed only when the requester holds a consistent buffer id.
    buffer_id: usize,
    /// The first writer of the buffer is responsible for starting a linger and flushing the buffer when the linger expires.
    /// The subsequent writers have to wait for the produce result.
    has_linger: bool,
    /// The handles of the flush tasks that are not finished yet. Those tasks will be forced to abort when the producer is dropped.
    pending_flush_tasks: Vec<JoinHandle<Result<()>>>,
    /// The kafka client.
    client: Arc<PartitionClient>,
    /// The compression algorithm to compress the entries.
    compression: Compression,
}

impl ProducerInner {
    fn new(client: Arc<PartitionClient>, buffer_capacity: usize, compression: Compression) -> Self {
        Self {
            buffer: Some(EntryBuffer::new(buffer_capacity)),
            buffer_id: 0,
            pending_flush_tasks: Vec::new(),
            has_linger: false,
            client,
            compression,
        }
    }

    fn produce(&mut self, entry: EntryInner) -> Result<CallerJob> {
        // Tries to push the entry to the buffer. An error occurs when the entry is too large.
        let notifier = match self.buffer.as_mut().unwrap().try_push(entry)? {
            PushResult::Ok(notifier) => notifier,
            PushResult::NoCapacity(entry) => {
                // The buffer has no room for the entry. Flush the buffer and then push the entry to the new buffer instance.
                self.flush(None)?;
                // Safety: the push has to succeed since the buffer instance is totally new and the entry is guaranteed to be
                // fit into the buffer or an error would occur on the first trail of push.
                let PushResult::Ok(notifier) =
                    self.buffer.as_mut().unwrap().try_push(entry).unwrap()
                else {
                    unreachable!()
                };
                notifier
            }
        };

        if self.has_linger {
            return Ok(CallerJob::Wait(notifier));
        }

        self.has_linger = true;
        Ok(CallerJob::Linger {
            notifier,
            buffer_id: self.buffer_id,
        })
    }

    /// Tries to flush the buffer.
    /// If the given buffer id is not consistent with the id of the current buffer, the flush aborts.
    /// If no buffer id provided, the flush is forced to be executed.
    fn flush(&mut self, buffer_id: Option<usize>) -> Result<()> {
        if let Some(buffer_id) = buffer_id
            && buffer_id != self.buffer_id
        {
            debug!(
                "Abort flushing the buffer. given buffer id = {}, curr buffer id = {}",
                buffer_id, self.buffer_id
            );
            return Ok(());
        }

        // Clears finished flush tasks.
        self.pending_flush_tasks
            .retain_mut(|task| !task.is_finished());

        // Replaces the buffer with a new instance and flushes the old buffer in the backgroud.
        let flush_task = self
            .buffer
            .replace(EntryBuffer::new(self.buffer.as_ref().unwrap().capacity))
            .unwrap()
            .start_background_flush(self.client.clone(), self.compression);
        self.pending_flush_tasks.push(flush_task);

        self.buffer_id = self.buffer_id.wrapping_add(1);
        self.has_linger = false;
        Ok(())
    }
}

impl Drop for ProducerInner {
    fn drop(&mut self) {
        // Clears pending tasks to avoid leaking tasks.
        self.pending_flush_tasks
            .drain(..)
            .for_each(|task| task.abort());
    }
}

enum PushResult {
    Ok(Arc<ProduceResultNotifier>),
    NoCapacity(EntryInner),
}

#[derive(Debug)]
struct EntryBuffer {
    entries: Vec<EntryInner>,
    accumulated: usize,
    capacity: usize,
    notifier: Arc<ProduceResultNotifier>,
}

impl EntryBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            entries: Vec::new(),
            accumulated: 0,
            capacity,
            notifier: Arc::new(ProduceResultNotifier::default()),
        }
    }

    // FIXME(niebayes): we need to provide a conservative value for the record meta so that the encoded record is guaranteed to be fit
    // into a kafka message.
    fn try_push(&mut self, entry: EntryInner) -> Result<PushResult> {
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

    fn start_background_flush(
        self,
        client: Arc<PartitionClient>,
        compression: Compression,
    ) -> JoinHandle<Result<()>> {
        tokio::spawn({
            async move {
                let record = RecordBuilder::build_record(self.entries)?;
                // Stringify the error to support clone it.
                let result = client
                    .produce(vec![record], compression)
                    .await
                    .map(|_| ())
                    .with_context(|_| ProduceRecordSnafu {
                        topic: client.topic(),
                    })
                    .map_err(|e| e.to_string());
                self.notifier.notify_waiters(result).await;
                Ok(())
            }
        })
    }
}
