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

pub(crate) mod flush;
pub(crate) mod produce;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common_telemetry::debug;
use futures::future::try_join_all;
use rskafka::client::partition::Compression;
use rskafka::client::producer::ProducerClient;
use rskafka::record::Record;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

use super::collector::IndexCollector;
use crate::error::{self, NoMaxValueSnafu, Result};

pub struct ProduceRequest {
    pub(crate) region_id: RegionId,
    pub(crate) batch: Vec<Record>,
    pub(crate) sender: oneshot::Sender<ProduceResultReceiver>,
}

/// Receives the committed offsets when data has been committed to Kafka
/// or an unrecoverable error has been encountered.
pub(crate) struct ProduceResultHandle {
    pub(crate) receiver: oneshot::Receiver<ProduceResultReceiver>,
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
    /// The [`ProducerClient`].
    pub(crate) client: Arc<dyn ProducerClient>,
    // The compression configuration.
    pub(crate) compression: Compression,
    // The running flag.
    pub(crate) running: Arc<AtomicBool>,
    /// Receiver of [ProduceRequest].
    pub(crate) receiver: Receiver<ProduceRequest>,
    /// Max batch size for a worker to handle requests.
    pub(crate) request_batch_size: usize,
    /// Max bytes size for a single flush.
    pub(crate) max_batch_bytes: usize,
    /// The [PendingRequest]s.
    pub(crate) pending_requests: Vec<PendingRequest>,
    /// Collecting ids of WAL entries.
    pub(crate) index_collector: Box<dyn IndexCollector>,
}

impl BackgroundProducerWorker {
    pub(crate) async fn run(&mut self) {
        let mut buffer = Vec::with_capacity(self.request_batch_size);
        while self.running.load(Ordering::Relaxed) {
            self.try_flush_pending_requests().await;
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
                    self.pending_requests =
                        self.handle_produce_requests(&mut buffer, self.max_batch_bytes);
                }
                None => {
                    debug!("The sender is dropped, BackgroundProducerWorker exited");
                    // Exits the loop if the `sender` is dropped.
                    break;
                }
            }
        }
    }
}
