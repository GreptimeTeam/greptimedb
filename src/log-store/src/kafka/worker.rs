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

pub(crate) mod dump_index;
pub(crate) mod flush;
pub(crate) mod produce;
pub(crate) mod update_high_watermark;

use std::sync::Arc;

use common_telemetry::debug;
use dashmap::DashMap;
use futures::future::try_join_all;
use rskafka::client::partition::Compression;
use rskafka::record::Record;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::provider::KafkaProvider;
use store_api::logstore::EntryId;
use store_api::storage::RegionId;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::{self};

use crate::error::{self, NoMaxValueSnafu, Result};
use crate::kafka::index::{IndexCollector, IndexEncoder};
use crate::kafka::producer::ProducerClient;

pub(crate) enum WorkerRequest {
    Produce(ProduceRequest),
    TruncateIndex(TruncateIndexRequest),
    DumpIndex(DumpIndexRequest),
    UpdateHighWatermark,
}

impl WorkerRequest {
    pub(crate) fn new_produce_request(
        region_id: RegionId,
        batch: Vec<Record>,
    ) -> (WorkerRequest, ProduceResultHandle) {
        let (tx, rx) = oneshot::channel();

        (
            WorkerRequest::Produce(ProduceRequest {
                region_id,
                batch,
                sender: tx,
            }),
            ProduceResultHandle { receiver: rx },
        )
    }
}

pub(crate) struct DumpIndexRequest {
    encoder: Arc<dyn IndexEncoder>,
    sender: oneshot::Sender<()>,
}

impl DumpIndexRequest {
    pub fn new(encoder: Arc<dyn IndexEncoder>) -> (DumpIndexRequest, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            DumpIndexRequest {
                encoder,
                sender: tx,
            },
            rx,
        )
    }
}

pub(crate) struct TruncateIndexRequest {
    region_id: RegionId,
    entry_id: EntryId,
}

impl TruncateIndexRequest {
    pub fn new(region_id: RegionId, entry_id: EntryId) -> Self {
        Self {
            region_id,
            entry_id,
        }
    }
}

pub(crate) struct ProduceRequest {
    region_id: RegionId,
    batch: Vec<Record>,
    sender: oneshot::Sender<ProduceResultReceiver>,
}

/// Receives the committed offsets when data has been committed to Kafka
/// or an unrecoverable error has been encountered.
pub(crate) struct ProduceResultHandle {
    receiver: oneshot::Receiver<ProduceResultReceiver>,
}

impl ProduceResultHandle {
    /// Waits for the data has been committed to Kafka.
    /// Returns the **max** committed offsets.
    pub(crate) async fn wait(self) -> Result<u64> {
        self.receiver
            .await
            .context(error::WaitProduceResultReceiverSnafu)?
            .wait()
            .await
    }
}

#[derive(Default)]
pub(crate) struct ProduceResultReceiver {
    receivers: Vec<oneshot::Receiver<Result<Vec<i64>>>>,
}

impl ProduceResultReceiver {
    fn add_receiver(&mut self, receiver: oneshot::Receiver<Result<Vec<i64>>>) {
        self.receivers.push(receiver)
    }

    async fn wait(self) -> Result<u64> {
        Ok(try_join_all(self.receivers)
            .await
            .into_iter()
            .flatten()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .max()
            .context(NoMaxValueSnafu)? as u64)
    }
}

pub(crate) struct PendingRequest {
    batch: Vec<Record>,
    region_ids: Vec<RegionId>,
    size: usize,
    sender: oneshot::Sender<Result<Vec<i64>>>,
}

pub(crate) struct BackgroundProducerWorker {
    pub(crate) provider: Arc<KafkaProvider>,
    /// The [`ProducerClient`].
    pub(crate) client: Arc<dyn ProducerClient>,
    // The compression configuration.
    pub(crate) compression: Compression,
    /// Receiver of [ProduceRequest].
    pub(crate) receiver: Receiver<WorkerRequest>,
    /// Max batch size for a worker to handle requests.
    pub(crate) request_batch_size: usize,
    /// Max bytes size for a single flush.
    pub(crate) max_batch_bytes: usize,
    /// Collecting ids of WAL entries.
    pub(crate) index_collector: Box<dyn IndexCollector>,
    /// High watermark for all topics.
    pub(crate) high_watermark: Arc<DashMap<Arc<KafkaProvider>, u64>>,
}

impl BackgroundProducerWorker {
    pub(crate) async fn run(&mut self) {
        let mut buffer = Vec::with_capacity(self.request_batch_size);
        loop {
            match self.receiver.recv().await {
                Some(req) => {
                    buffer.clear();
                    buffer.push(req);
                    for _ in 1..self.request_batch_size {
                        match self.receiver.try_recv() {
                            Ok(req) => buffer.push(req),
                            Err(_) => break,
                        }
                    }
                    self.handle_requests(&mut buffer).await;
                }
                None => {
                    debug!("The sender is dropped, BackgroundProducerWorker exited");
                    // Exits the loop if the `sender` is dropped.
                    break;
                }
            }
        }
    }

    async fn handle_requests(&mut self, buffer: &mut Vec<WorkerRequest>) {
        let mut produce_requests = Vec::with_capacity(buffer.len());
        for req in buffer.drain(..) {
            match req {
                WorkerRequest::Produce(req) => produce_requests.push(req),
                WorkerRequest::TruncateIndex(TruncateIndexRequest {
                    region_id,
                    entry_id,
                }) => self.index_collector.truncate(region_id, entry_id),
                WorkerRequest::DumpIndex(req) => self.dump_index(req).await,
                WorkerRequest::UpdateHighWatermark => {
                    self.update_high_watermark().await;
                }
            }
        }

        let pending_requests = self.aggregate_records(&mut produce_requests, self.max_batch_bytes);
        self.try_flush_pending_requests(pending_requests).await;
    }
}
