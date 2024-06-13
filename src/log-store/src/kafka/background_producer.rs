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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common_runtime::JoinHandle;
use common_telemetry::{debug, warn};
use futures::future::try_join_all;
use rskafka::client::partition::Compression;
use rskafka::client::producer::ProducerClient;
use rskafka::record::Record;
use snafu::ResultExt;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

use crate::error::{self, Result};

struct ProduceRequest {
    batch: Vec<Record>,
    sender: oneshot::Sender<ProduceResultReceiver>,
}

#[derive(Default)]
pub(crate) struct ProduceResultReceiver {
    receivers: Vec<oneshot::Receiver<Result<Vec<i64>>>>,
}

impl ProduceResultReceiver {
    fn add_receiver(&mut self, receiver: oneshot::Receiver<Result<Vec<i64>>>) {
        self.receivers.push(receiver)
    }

    pub(crate) async fn wait(self) -> Result<Vec<i64>> {
        Ok(try_join_all(self.receivers)
            .await
            .into_iter()
            .flatten()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>())
    }
}

struct BackgroundProducerWorker {
    /// The [`ProducerClient`].
    client: Arc<dyn ProducerClient>,
    // The compression configuration.
    compression: Compression,
    // The running flag.
    running: Arc<AtomicBool>,
    /// Receiver of [ProduceRequest].
    receiver: Receiver<ProduceRequest>,
    /// Max batch size for a worker to handle requests.
    request_batch_size: usize,
    /// Max size for a single flush.
    max_flush_size: usize,
    /// The [PendingRequest]s.
    pending_requests: Vec<PendingRequest>,
}

struct PendingRequest {
    batch: Vec<Record>,
    size: usize,
    sender: oneshot::Sender<Result<Vec<i64>>>,
}

struct AggregatedPendingRequest {
    batch: Vec<PendingRequest>,
}

fn handle_produce_requests(
    requests: &mut Vec<ProduceRequest>,
    max_batch_size: usize,
) -> Vec<PendingRequest> {
    let mut records_buffer = vec![];
    let mut batch_size = 0;
    let mut pending_requests = vec![];

    for ProduceRequest { batch, sender } in std::mem::take(requests) {
        let mut receiver = ProduceResultReceiver::default();
        for record in batch {
            if batch_size + record.approximate_size() > max_batch_size {
                // TODO(weny): ensure records_buffer.len() > 0
                assert!(!records_buffer.is_empty());
                let (tx, rx) = oneshot::channel();
                pending_requests.push(PendingRequest {
                    batch: std::mem::take(&mut records_buffer),
                    size: batch_size,
                    sender: tx,
                });
                batch_size = 0;
                receiver.add_receiver(rx);

                // TODO(weny): ensure record.len() < max_batch_size
                batch_size += record.approximate_size();
                records_buffer.push(record);
            } else {
                batch_size += record.approximate_size();
                records_buffer.push(record);
            }
        }
        // Remaining records.
        if batch_size > 0 {
            let (tx, rx) = oneshot::channel();
            pending_requests.push(PendingRequest {
                batch: std::mem::take(&mut records_buffer),
                size: batch_size,
                sender: tx,
            });
            batch_size = 0;
            receiver.add_receiver(rx);
        }

        let _ = sender.send(receiver);
    }
    pending_requests
}

async fn do_flush(
    client: &Arc<dyn ProducerClient>,
    PendingRequest {
        batch,
        sender,
        size: _size,
    }: PendingRequest,
    compression: Compression,
) {
    let result = client
        .produce(batch, compression)
        .await
        .context(error::BatchProduceSnafu);

    if let Err(err) = sender.send(result) {
        warn!(err; "BatchFlushState Receiver is dropped");
    }
}

impl BackgroundProducerWorker {
    async fn run(&mut self) {
        let mut buffer = Vec::with_capacity(self.request_batch_size);
        while self.running.load(Ordering::Relaxed) {
            // Processes pending requests first.
            if !self.pending_requests.is_empty() {
                for req in std::mem::take(&mut self.pending_requests) {
                    do_flush(&self.client, req, self.compression).await
                }
            } else {
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
                            handle_produce_requests(&mut buffer, self.max_flush_size);
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
}

pub(crate) struct OrderedBatchProducer {
    sender: Sender<ProduceRequest>,
    /// Used to control the [`BackgroundProducerWorker`].
    running: Arc<AtomicBool>,
    /// The handle of [`BackgroundProducerWorker`].
    handle: JoinHandle<()>,
}

impl Drop for OrderedBatchProducer {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

pub(crate) struct ProduceResultHandle {
    receiver: oneshot::Receiver<ProduceResultReceiver>,
}

impl ProduceResultHandle {
    pub(crate) async fn wait(self) -> Result<Vec<i64>> {
        self.receiver.await.unwrap().wait().await
    }
}

impl OrderedBatchProducer {
    pub(crate) fn new(
        client: Arc<dyn ProducerClient>,
        compression: Compression,
        request_channel_size: usize,
        request_batch_size: usize,
        max_flush_size: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(request_channel_size);
        let running = Arc::new(AtomicBool::new(true));

        let mut worker = BackgroundProducerWorker {
            client,
            compression,
            running: running.clone(),
            receiver: rx,
            request_batch_size,
            max_flush_size,
            pending_requests: vec![],
        };

        Self {
            sender: tx,
            running,
            handle: tokio::spawn(async move { worker.run().await }),
        }
    }

    pub(crate) async fn produce(&self, batch: Vec<Record>) -> Result<ProduceResultHandle> {
        let receiver = {
            let (tx, rx) = oneshot::channel();
            self.sender
                .send(ProduceRequest { batch, sender: tx })
                .await
                .expect("worker panic");
            rx
        };

        Ok(ProduceResultHandle { receiver })
    }
}
