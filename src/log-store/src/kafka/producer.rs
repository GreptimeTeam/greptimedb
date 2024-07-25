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

use rskafka::client::partition::Compression;
use rskafka::client::producer::ProducerClient;
use rskafka::record::Record;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;

use super::worker::{BackgroundProducerWorker, ProduceResultHandle};
use crate::error::{self, Result};
use crate::kafka::collector::NoopCollector;
use crate::kafka::worker::ProduceRequest;

pub type OrderedBatchProducerRef = Arc<OrderedBatchProducer>;

/// [`OrderedBatchProducer`] attempts to aggregate multiple produce requests together
#[derive(Debug)]
pub(crate) struct OrderedBatchProducer {
    pub(crate) sender: Sender<ProduceRequest>,
    /// Used to control the [`BackgroundProducerWorker`].
    running: Arc<AtomicBool>,
}

impl Drop for OrderedBatchProducer {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
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
            index_collector: Box::new(NoopCollector),
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
    pub(crate) async fn produce(
        &self,
        region_id: RegionId,
        batch: Vec<Record>,
    ) -> Result<ProduceResultHandle> {
        let receiver = {
            let (tx, rx) = oneshot::channel();
            self.sender
                .send(ProduceRequest {
                    region_id,
                    batch,
                    sender: tx,
                })
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
    use store_api::storage::RegionId;

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

        let region_id = RegionId::new(1, 1);
        // Produces 3 records
        let handle = producer
            .produce(
                region_id,
                vec![record.clone(), record.clone(), record.clone()],
            )
            .await
            .unwrap();
        assert_eq!(handle.wait().await.unwrap(), 2);
        assert_eq!(client.batch_sizes.lock().unwrap().as_slice(), &[2, 1]);

        // Produces 2 records
        let handle = producer
            .produce(region_id, vec![record.clone(), record.clone()])
            .await
            .unwrap();
        assert_eq!(handle.wait().await.unwrap(), 4);
        assert_eq!(client.batch_sizes.lock().unwrap().as_slice(), &[2, 1, 2]);

        // Produces 1 records
        let handle = producer
            .produce(region_id, vec![record.clone()])
            .await
            .unwrap();
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

        let region_id = RegionId::new(1, 1);
        let mut futures = FuturesUnordered::new();
        futures.push(
            producer
                .produce(
                    region_id,
                    vec![record.clone(), record.clone(), record.clone()],
                )
                .await
                .unwrap()
                .wait(),
        );
        futures.push(
            producer
                .produce(region_id, vec![record.clone(), record.clone()])
                .await
                .unwrap()
                .wait(),
        );

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

        let region_id = RegionId::new(1, 1);
        let a = producer
            .produce(
                region_id,
                vec![record.clone(), record.clone(), record.clone()],
            )
            .await
            .unwrap()
            .wait()
            .fuse();

        let b = producer
            .produce(region_id, vec![record])
            .await
            .unwrap()
            .wait()
            .fuse();

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
