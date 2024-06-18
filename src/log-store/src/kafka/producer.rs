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

use common_telemetry::{debug, warn};
use futures::future::try_join_all;
use rskafka::client::partition::Compression;
use rskafka::client::producer::ProducerClient;
use rskafka::record::Record;
use snafu::{OptionExt, ResultExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

use crate::error::{self, NoMaxValueSnafu, Result};

pub struct ProduceRequest {
    batch: Vec<Record>,
    sender: oneshot::Sender<ProduceResultReceiver>,
}

#[derive(Default)]
struct ProduceResultReceiver {
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
    /// Max bytes size for a single flush.
    max_batch_bytes: usize,
    /// The [PendingRequest]s.
    pending_requests: Vec<PendingRequest>,
}

struct PendingRequest {
    batch: Vec<Record>,
    size: usize,
    sender: oneshot::Sender<Result<Vec<i64>>>,
}

/// ## Panic
/// Panic if any [Record]'s `approximate_size` > `max_batch_bytes`.
fn handle_produce_requests(
    requests: &mut Vec<ProduceRequest>,
    max_batch_bytes: usize,
) -> Vec<PendingRequest> {
    let mut records_buffer = vec![];
    let mut batch_size = 0;
    let mut pending_requests = Vec::with_capacity(requests.len());

    for ProduceRequest { batch, sender } in requests.drain(..) {
        let mut receiver = ProduceResultReceiver::default();
        for record in batch {
            assert!(record.approximate_size() <= max_batch_bytes);
            // Yields the `PendingRequest` if buffer is full.
            if batch_size + record.approximate_size() > max_batch_bytes {
                let (tx, rx) = oneshot::channel();
                pending_requests.push(PendingRequest {
                    batch: std::mem::take(&mut records_buffer),
                    size: batch_size,
                    sender: tx,
                });
                batch_size = 0;
                receiver.add_receiver(rx);
            }

            batch_size += record.approximate_size();
            records_buffer.push(record);
        }
        // The remaining records.
        if batch_size > 0 {
            // Yields `PendingRequest`
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
                // TODO(weny): Considering merge `PendingRequest`s.
                for req in self.pending_requests.drain(..) {
                    do_flush(&self.client, req, self.compression).await
                }
            }
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
                        handle_produce_requests(&mut buffer, self.max_batch_bytes);
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

pub type OrderedBatchProducerRef = Arc<OrderedBatchProducer>;

/// [`OrderedBatchProducer`] attempts to aggregate multiple produce requests together
#[derive(Debug)]
pub(crate) struct OrderedBatchProducer {
    sender: Sender<ProduceRequest>,
    /// Used to control the [`BackgroundProducerWorker`].
    running: Arc<AtomicBool>,
}

impl Drop for OrderedBatchProducer {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
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

impl OrderedBatchProducer {
    /// Constructs a new [`OrderedBatchProducer`].
    pub(crate) fn new(
        client: Arc<dyn ProducerClient>,
        compression: Compression,
        channel_size: usize,
        request_batch_size: usize,
        max_batch_bytes: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(channel_size);
        let running = Arc::new(AtomicBool::new(true));
        let mut worker = BackgroundProducerWorker {
            client,
            compression,
            running: running.clone(),
            receiver: rx,
            request_batch_size,
            max_batch_bytes,
            pending_requests: vec![],
        };
        tokio::spawn(async move { worker.run().await });
        Self {
            sender: tx,
            running,
        }
    }

    /// Writes `data` to the [`OrderedBatchProducer`].
    ///
    /// Returns the [ProduceResultHandle], which will receive a result when data has been committed to Kafka
    /// or an unrecoverable error has been encountered.
    ///
    /// ## Panic
    /// Panic if any [Record]'s `approximate_size` > `max_batch_bytes`.
    pub(crate) async fn produce(&self, batch: Vec<Record>) -> Result<ProduceResultHandle> {
        let receiver = {
            let (tx, rx) = oneshot::channel();
            self.sender
                .send(ProduceRequest { batch, sender: tx })
                .await
                .context(error::SendProduceRequestSnafu)?;
            rx
        };

        Ok(ProduceResultHandle { receiver })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use chrono::{TimeZone, Utc};
    use common_base::readable_size::ReadableSize;
    use common_telemetry::debug;
    use futures::future::BoxFuture;
    use futures::stream::FuturesUnordered;
    use futures::{FutureExt, StreamExt};
    use rskafka::client::error::{Error as ClientError, RequestContext};
    use rskafka::client::partition::Compression;
    use rskafka::client::producer::ProducerClient;
    use rskafka::protocol::error::Error as ProtocolError;
    use rskafka::record::Record;

    use crate::kafka::producer::OrderedBatchProducer;

    #[derive(Debug)]
    struct MockClient {
        error: Option<ProtocolError>,
        panic: Option<String>,
        delay: Duration,
        batch_sizes: Mutex<Vec<usize>>,
    }

    impl ProducerClient for MockClient {
        fn produce(
            &self,
            records: Vec<Record>,
            _compression: Compression,
        ) -> BoxFuture<'_, Result<Vec<i64>, ClientError>> {
            Box::pin(async move {
                tokio::time::sleep(self.delay).await;

                if let Some(e) = self.error {
                    return Err(ClientError::ServerError {
                        protocol_error: e,
                        error_message: None,
                        request: RequestContext::Partition("foo".into(), 1),
                        response: None,
                        is_virtual: false,
                    });
                }

                if let Some(p) = self.panic.as_ref() {
                    panic!("{}", p);
                }

                let mut batch_sizes = self.batch_sizes.lock().unwrap();
                let offset_base = batch_sizes.iter().sum::<usize>();
                let offsets = (0..records.len())
                    .map(|x| (x + offset_base) as i64)
                    .collect();
                batch_sizes.push(records.len());
                debug!("Return offsets: {offsets:?}");
                Ok(offsets)
            })
        }
    }

    fn record() -> Record {
        Record {
            key: Some(vec![0; 4]),
            value: Some(vec![0; 6]),
            headers: Default::default(),
            timestamp: Utc.timestamp_millis_opt(320).unwrap(),
        }
    }

    #[tokio::test]
    async fn test_producer() {
        common_telemetry::init_default_ut_logging();
        let record = record();
        let delay = Duration::from_secs(0);
        let client = Arc::new(MockClient {
            error: None,
            panic: None,
            delay,
            batch_sizes: Default::default(),
        });

        let producer = OrderedBatchProducer::new(
            client.clone(),
            Compression::NoCompression,
            128,
            64,
            ReadableSize((record.approximate_size() * 2) as u64).as_bytes() as usize,
        );

        // Produces 3 records
        let handle = producer
            .produce(vec![record.clone(), record.clone(), record.clone()])
            .await
            .unwrap();
        assert_eq!(handle.wait().await.unwrap(), 2);
        assert_eq!(client.batch_sizes.lock().unwrap().as_slice(), &[2, 1]);

        // Produces 2 records
        let handle = producer
            .produce(vec![record.clone(), record.clone()])
            .await
            .unwrap();
        assert_eq!(handle.wait().await.unwrap(), 4);
        assert_eq!(client.batch_sizes.lock().unwrap().as_slice(), &[2, 1, 2]);

        // Produces 1 records
        let handle = producer.produce(vec![record.clone()]).await.unwrap();
        assert_eq!(handle.wait().await.unwrap(), 5);
        assert_eq!(client.batch_sizes.lock().unwrap().as_slice(), &[2, 1, 2, 1]);
    }

    #[tokio::test]
    async fn test_producer_client_error() {
        let record = record();
        let client = Arc::new(MockClient {
            error: Some(ProtocolError::NetworkException),
            panic: None,
            delay: Duration::from_millis(1),
            batch_sizes: Default::default(),
        });

        let producer = OrderedBatchProducer::new(
            client.clone(),
            Compression::NoCompression,
            128,
            64,
            ReadableSize((record.approximate_size() * 2) as u64).as_bytes() as usize,
        );

        let mut futures = FuturesUnordered::new();
        futures.push(
            producer
                .produce(vec![record.clone(), record.clone(), record.clone()])
                .await
                .unwrap()
                .wait(),
        );
        futures.push(
            producer
                .produce(vec![record.clone(), record.clone()])
                .await
                .unwrap()
                .wait(),
        );
        futures.push(producer.produce(vec![record.clone()]).await.unwrap().wait());

        futures.next().await.unwrap().unwrap_err();
        futures.next().await.unwrap().unwrap_err();
        futures.next().await.unwrap().unwrap_err();
    }

    #[tokio::test]
    async fn test_producer_cancel() {
        let record = record();
        let client = Arc::new(MockClient {
            error: None,
            panic: None,
            delay: Duration::from_millis(1),
            batch_sizes: Default::default(),
        });

        let producer = OrderedBatchProducer::new(
            client.clone(),
            Compression::NoCompression,
            128,
            64,
            ReadableSize((record.approximate_size() * 2) as u64).as_bytes() as usize,
        );

        let a = producer
            .produce(vec![record.clone(), record.clone(), record.clone()])
            .await
            .unwrap()
            .wait()
            .fuse();

        let b = producer.produce(vec![record]).await.unwrap().wait().fuse();

        let mut b = Box::pin(b);

        {
            // Cancel a when it exits this block
            let mut a = Box::pin(a);

            // Select biased to encourage `a` to be the one with the linger that
            // expires first and performs the produce operation
            futures::select_biased! {
                _ = &mut a => panic!("a should not have flushed"),
                _ = &mut b => panic!("b should not have flushed"),
                _ = tokio::time::sleep(Duration::from_millis(1)).fuse() => {},
            }
        }

        // But `b` should still complete successfully
        tokio::time::timeout(Duration::from_secs(1), b)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            client
                .batch_sizes
                .lock()
                .unwrap()
                .as_slice()
                .iter()
                .sum::<usize>(),
            4
        );
    }
}
