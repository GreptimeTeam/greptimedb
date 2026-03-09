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

//! Per-subject batch producer for the NATS JetStream WAL.
//!
//! Each active WAL subject gets one [`NatsBatchProducer`] whose background
//! task drains a channel of publish requests, publishes them sequentially to
//! the NATS subject, and returns the committed sequence numbers via oneshot
//! channels.

use std::sync::Arc;
use std::time::Duration;

use common_telemetry::warn;
use snafu::ResultExt;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

use crate::error::{NatsBatchProducerStoppedSnafu, NatsPublishSnafu, Result};
use crate::nats::client::NatsClientRef;
use crate::nats::record::NatsRecord;

pub(crate) type NatsBatchProducerRef = Arc<NatsBatchProducer>;

/// Maximum number of pending publish requests in the channel.
const CHANNEL_SIZE: usize = 128;

/// A request sent from the LogStore to the background producer worker.
pub(crate) struct PublishRequest {
    /// NATS records to publish.
    pub(crate) records: Vec<NatsRecord>,
    /// Oneshot sender that receives the sequence number of the last published
    /// record once all records have been committed.
    pub(crate) result_tx: oneshot::Sender<Result<u64>>,
}

/// Handle to a pending publish result.
pub(crate) struct ProduceResultHandle {
    rx: oneshot::Receiver<Result<u64>>,
}

impl ProduceResultHandle {
    /// Wait for the committed NATS sequence number.
    pub(crate) async fn wait(self) -> Result<u64> {
        self.rx.await.unwrap_or_else(|_| {
            NatsBatchProducerStoppedSnafu.fail()
        })
    }
}

/// Background worker that publishes WAL records for a single NATS subject.
///
/// The worker serialises all publishes to preserve per-subject ordering
/// (required by JetStream for deduplication / sequence correctness).
struct ProducerWorker {
    subject: String,
    client: NatsClientRef,
    receiver: Receiver<PublishRequest>,
    publish_ack_timeout: Duration,
}

impl ProducerWorker {
    async fn run(mut self) {
        while let Some(req) = self.receiver.recv().await {
            let result = self.handle_request(req.records).await;
            let _ = req.result_tx.send(result);
        }
    }

    async fn handle_request(&self, records: Vec<NatsRecord>) -> Result<u64> {
        let mut last_seq: u64 = 0;
        for record in records {
            let (headers, payload) = record.to_nats_message()?;
            let ack_future = self
                .client
                .jetstream
                .publish_with_headers(self.subject.clone(), headers, payload)
                .await
                .context(NatsPublishSnafu {
                    subject: self.subject.clone(),
                })?;

            let ack_result = tokio::time::timeout(self.publish_ack_timeout, ack_future).await;
            let ack = match ack_result {
                Ok(Ok(ack)) => ack,
                Ok(Err(e)) => {
                    return Err(crate::error::Error::NatsPublishAck {
                        subject: self.subject.clone(),
                        msg: e.to_string(),
                        location: snafu::location!(),
                    });
                }
                Err(_) => {
                    return Err(crate::error::Error::NatsPublishAck {
                        subject: self.subject.clone(),
                        msg: "publish ack timeout".to_string(),
                        location: snafu::location!(),
                    });
                }
            };

            last_seq = ack.sequence;
        }
        Ok(last_seq)
    }
}

/// Per-subject publish aggregator.
///
/// Callers send [`PublishRequest`]s through the channel; a background Tokio
/// task processes them sequentially and responds via the oneshot sender.
#[derive(Debug)]
pub(crate) struct NatsBatchProducer {
    sender: Sender<PublishRequest>,
}

impl NatsBatchProducer {
    /// Creates a new `NatsBatchProducer` for the given `subject`.
    pub(crate) fn new(
        subject: String,
        client: NatsClientRef,
        publish_ack_timeout: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        let worker = ProducerWorker {
            subject,
            client,
            receiver: rx,
            publish_ack_timeout,
        };
        tokio::spawn(worker.run());
        Self { sender: tx }
    }

    /// Enqueues `records` for publishing.
    ///
    /// Returns a [`ProduceResultHandle`] that resolves to the NATS sequence
    /// number of the last committed record.
    pub(crate) async fn produce(&self, records: Vec<NatsRecord>) -> Result<ProduceResultHandle> {
        let (result_tx, result_rx) = oneshot::channel();
        let req = PublishRequest { records, result_tx };
        if self.sender.send(req).await.is_err() {
            warn!("NatsBatchProducer is already stopped");
            return NatsBatchProducerStoppedSnafu.fail();
        }
        Ok(ProduceResultHandle { rx: result_rx })
    }
}
