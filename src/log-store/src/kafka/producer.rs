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

use std::sync::Arc;

use common_telemetry::warn;
use dashmap::DashMap;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient};
use rskafka::record::Record;
use store_api::logstore::provider::KafkaProvider;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::error::{self, Result};
use crate::kafka::index::IndexCollector;
use crate::kafka::worker::{BackgroundProducerWorker, ProduceResultHandle, WorkerRequest};
use crate::metrics::{
    METRIC_KAFKA_CLIENT_BYTES_TOTAL, METRIC_KAFKA_CLIENT_PRODUCE_ELAPSED,
    METRIC_KAFKA_CLIENT_TRAFFIC_TOTAL,
};

pub type OrderedBatchProducerRef = Arc<OrderedBatchProducer>;

// Max batch size for a `OrderedBatchProducer` to handle requests.
const REQUEST_BATCH_SIZE: usize = 64;

// Producer channel size
const PRODUCER_CHANNEL_SIZE: usize = REQUEST_BATCH_SIZE * 2;

/// [`OrderedBatchProducer`] attempts to aggregate multiple produce requests together
#[derive(Debug)]
pub(crate) struct OrderedBatchProducer {
    pub(crate) sender: Sender<WorkerRequest>,
}

impl OrderedBatchProducer {
    pub(crate) fn channel() -> (Sender<WorkerRequest>, Receiver<WorkerRequest>) {
        mpsc::channel(PRODUCER_CHANNEL_SIZE)
    }

    /// Constructs a new [`OrderedBatchProducer`].
    pub(crate) fn new(
        (tx, rx): (Sender<WorkerRequest>, Receiver<WorkerRequest>),
        provider: Arc<KafkaProvider>,
        client: Arc<dyn ProducerClient>,
        compression: Compression,
        max_batch_bytes: usize,
        index_collector: Box<dyn IndexCollector>,
        high_watermark: Arc<DashMap<Arc<KafkaProvider>, u64>>,
    ) -> Self {
        let mut worker = BackgroundProducerWorker {
            provider,
            client,
            compression,
            receiver: rx,
            request_batch_size: REQUEST_BATCH_SIZE,
            max_batch_bytes,
            index_collector,
            high_watermark,
        };
        tokio::spawn(async move { worker.run().await });
        Self { sender: tx }
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
        let (req, handle) = WorkerRequest::new_produce_request(region_id, batch);
        if self.sender.send(req).await.is_err() {
            warn!("OrderedBatchProducer is already exited");
            return error::OrderedBatchProducerStoppedSnafu {}.fail();
        }

        Ok(handle)
    }

    /// Sends an [WorkerRequest::UpdateHighWatermark] request to the producer.
    /// This is used to update the high watermark for the topic.
    pub(crate) async fn update_high_watermark(&self) -> Result<()> {
        if self
            .sender
            .send(WorkerRequest::UpdateHighWatermark)
            .await
            .is_err()
        {
            warn!("OrderedBatchProducer is already exited");
            return error::OrderedBatchProducerStoppedSnafu {}.fail();
        }
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait ProducerClient: std::fmt::Debug + Send + Sync {
    async fn produce(
        &self,
        records: Vec<Record>,
        compression: Compression,
    ) -> rskafka::client::error::Result<Vec<i64>>;

    async fn get_offset(&self, at: OffsetAt) -> rskafka::client::error::Result<i64>;
}

#[async_trait::async_trait]
impl ProducerClient for PartitionClient {
    async fn produce(
        &self,
        records: Vec<Record>,
        compression: Compression,
    ) -> rskafka::client::error::Result<Vec<i64>> {
        let total_size = records.iter().map(|r| r.approximate_size()).sum::<usize>();
        let partition = self.partition().to_string();
        METRIC_KAFKA_CLIENT_BYTES_TOTAL
            .with_label_values(&[self.topic(), &partition])
            .inc_by(total_size as u64);
        METRIC_KAFKA_CLIENT_TRAFFIC_TOTAL
            .with_label_values(&[self.topic(), &partition])
            .inc();
        let _timer = METRIC_KAFKA_CLIENT_PRODUCE_ELAPSED
            .with_label_values(&[self.topic(), &partition])
            .start_timer();

        self.produce(records, compression).await
    }

    async fn get_offset(&self, at: OffsetAt) -> rskafka::client::error::Result<i64> {
        self.get_offset(at).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use common_base::readable_size::ReadableSize;
    use common_telemetry::debug;
    use futures::stream::FuturesUnordered;
    use futures::{FutureExt, StreamExt};
    use rskafka::client::error::{Error as ClientError, RequestContext};
    use rskafka::client::partition::Compression;
    use rskafka::protocol::error::Error as ProtocolError;
    use rskafka::record::Record;
    use store_api::storage::RegionId;

    use super::*;
    use crate::kafka::index::NoopCollector;
    use crate::kafka::producer::OrderedBatchProducer;
    use crate::kafka::test_util::record;

    #[derive(Debug)]
    struct MockClient {
        error: Option<ProtocolError>,
        panic: Option<String>,
        delay: Duration,
        batch_sizes: Mutex<Vec<usize>>,
    }

    #[async_trait::async_trait]
    impl ProducerClient for MockClient {
        async fn produce(
            &self,
            records: Vec<Record>,
            _compression: Compression,
        ) -> rskafka::client::error::Result<Vec<i64>> {
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
        }

        async fn get_offset(&self, _at: OffsetAt) -> rskafka::client::error::Result<i64> {
            todo!()
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
        let provider = Arc::new(KafkaProvider::new(String::new()));
        let producer = OrderedBatchProducer::new(
            OrderedBatchProducer::channel(),
            provider,
            client.clone(),
            Compression::NoCompression,
            ReadableSize((record.approximate_size() * 2) as u64).as_bytes() as usize,
            Box::new(NoopCollector),
            Arc::new(DashMap::new()),
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
        let provider = Arc::new(KafkaProvider::new(String::new()));
        let producer = OrderedBatchProducer::new(
            OrderedBatchProducer::channel(),
            provider,
            client.clone(),
            Compression::NoCompression,
            ReadableSize((record.approximate_size() * 2) as u64).as_bytes() as usize,
            Box::new(NoopCollector),
            Arc::new(DashMap::new()),
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
        futures.push(
            producer
                .produce(region_id, vec![record.clone()])
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

        let provider = Arc::new(KafkaProvider::new(String::new()));
        let producer = OrderedBatchProducer::new(
            OrderedBatchProducer::channel(),
            provider,
            client.clone(),
            Compression::NoCompression,
            ReadableSize((record.approximate_size() * 2) as u64).as_bytes() as usize,
            Box::new(NoopCollector),
            Arc::new(DashMap::new()),
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
