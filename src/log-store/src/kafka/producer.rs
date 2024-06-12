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

pub(crate) mod batch;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use batch::{AggregatedStatus, BatchBuilder, BatchFlushState, FlushRequest, ResultHandle};
use common_runtime::JoinHandle;
use common_telemetry::{debug, error, warn};
use rskafka::client::partition::Compression;
use rskafka::client::producer::aggregator::{self, Aggregator, TryPush};
use rskafka::client::producer::ProducerClient;
use snafu::ResultExt;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, Mutex};

use crate::error::{self, Result};

struct FlushingRequest {
    handle: JoinHandle<()>,
    receiver: oneshot::Receiver<()>,
}

struct BackgroundProducerWorker<A: Aggregator> {
    /// The [`ProducerClient`].
    client: Arc<dyn ProducerClient>,
    // The compression configuration.
    compression: Compression,
    /// Receives [`FlushRequest`] from [`OrderedBatchProducerInner`].
    receiver: Receiver<FlushRequest<A>>,
    // The running flag.
    running: Arc<AtomicBool>,
    // Current flushing request.
    flushing: Option<FlushingRequest>,
}

async fn do_flush<A: Aggregator>(
    client: Arc<dyn ProducerClient>,
    FlushRequest {
        batch,
        status_deagg,
        sender,
    }: FlushRequest<A>,
    compression: Compression,
) {
    if let Err(err) = match client
        .produce(batch, compression)
        .await
        .context(error::ProduceBatchSnafu)
    {
        Ok(aggregated_status) => sender.send(BatchFlushState::Result(Arc::new(AggregatedStatus {
            aggregated_status,
            status_deagg,
        }))),
        Err(err) => sender.send(BatchFlushState::Err(Arc::new(err))),
    } {
        warn!(err; "BatchFlushState Receiver is dropped");
    }
}

impl<A: Aggregator> BackgroundProducerWorker<A> {
    async fn run(&mut self) {
        while self.running.load(Ordering::Relaxed) {
            match self.flushing.take() {
                Some(current) => {
                    if let Err(err) = current.receiver.await {
                        error!(err; "Flushing task is panicked");
                    };
                }
                None => match self.receiver.recv().await {
                    Some(request) => {
                        let (tx, rx) = oneshot::channel();
                        let client = self.client.clone();
                        let compression = self.compression;
                        self.flushing = Some(FlushingRequest {
                            handle: tokio::spawn(async move {
                                do_flush(client, request, compression).await;
                                // Ignores the error if the loop is exited.
                                let _ = tx.send(());
                            }),
                            receiver: rx,
                        });
                    }
                    None => {
                        debug!("The sender is dropped, BackgroundProducerWorker exited");
                        // Exits the loop if the `sender` is dropped.
                        break;
                    }
                },
            }
        }
    }
}

pub(crate) enum CallerRole<A: Aggregator> {
    /// The caller has no additional book-keeping to perform, and can wait on
    /// the result handle for the batched write result.
    Waiter(ResultHandle<A>),

    /// This caller has been selected to perform the "linger" timeout to drive
    /// timely flushing of the batch.
    ///
    /// The caller MUST wait for the configured linger time before calling
    /// [`OrderedBatchProducerInner::try_enqueue()`] with the provided `flush_token`.
    Leader {
        handle: ResultHandle<A>,
        flush_token: usize,
    },
}

/// [`ResultReceiver`] will receive the result when data has been committed to Kafka
/// or an unrecoverable error has been encountered.
pub enum ResultReceiver<A: Aggregator> {
    Waiter {
        handle: ResultHandle<A>,
    },
    Leader {
        handle: ResultHandle<A>,
        flush_token: usize,
        linger: Duration,
        inner: Arc<Mutex<OrderedBatchProducerInner<A>>>,
    },
}

impl<A: Aggregator> ResultReceiver<A> {
    async fn wait(self) -> Result<<A as aggregator::AggregatorStatus>::Status> {
        match self {
            ResultReceiver::Waiter { mut handle } => {
                let status = handle.wait().await?;
                handle.result(status)
            }
            ResultReceiver::Leader {
                mut handle,
                flush_token,
                inner,
                linger,
            } => {
                // This caller has been selected to wait for the linger
                // duration, and then attempt to flush the batch of writes.
                //
                // Spawn a task for the linger to ensure cancellation safety.
                let linger = tokio::spawn({
                    async move {
                        tokio::time::sleep(linger).await;

                        // The linger has expired, attempt to conditionally flush the
                        // batch using the provided token to ensure only the correct
                        // batch is flushed.
                        inner.lock().await.try_flush(Some(flush_token)).await?;
                        Ok(())
                    }
                });

                // The batch may be flushed before the linger period expires if
                // the aggregator becomes full, so watch for both outcomes.
                tokio::select! {
                    res = linger => res.expect("linger panic")?,
                    r = handle.wait() => return handle.result(r?),
                }

                // The linger expired & completed.
                //
                // Wait for the result of the flush to be published.
                let status = handle.wait().await?;
                // And demux the status for this caller.
                handle.result(status)
            }
        }
    }
}

pub(crate) struct OrderedBatchProducerInner<A: Aggregator> {
    /// It's wrapped in an [`Option`] to allow the [`OrderedProducerInner`]
    /// to consume the [`BatchBuilder`] and yield the next [`BatchBuilder`].    
    batch_builder: Option<BatchBuilder<A>>,
    sender: Sender<FlushRequest<A>>,

    /// Used to track if a [`CallerRole::Leader`] has been returned for the
    /// current batch_builder.
    has_leader: bool,

    /// A logical clock to enable a conditional flush of a specific
    /// [`BatchBuilder`] instance.
    flush_clock: usize,
}

impl<A: Aggregator> OrderedBatchProducerInner<A> {
    pub(crate) fn new(aggregator: A, sender: Sender<FlushRequest<A>>) -> Self {
        Self {
            batch_builder: Some(BatchBuilder::new(aggregator)),
            sender,
            has_leader: false,
            flush_clock: 0,
        }
    }

    pub(crate) async fn try_push(&mut self, data: A::Input) -> Result<CallerRole<A>> {
        let handle = match self.batch_builder.as_mut().unwrap().try_push(data)? {
            TryPush::Aggregated(handle) => handle,
            TryPush::NoCapacity(data) => {
                self.try_flush(None).await?;
                match self.batch_builder.as_mut().unwrap().try_push(data)? {
                    TryPush::Aggregated(handle) => handle,
                    TryPush::NoCapacity(_) => {
                        return error::AggregatorInputTooLargeSnafu {}.fail();
                    }
                }
            }
        };

        if self.has_leader {
            return Ok(CallerRole::Waiter(handle));
        }

        // This caller is the first writer to this batch,
        // and it should wait for the `linger` time before flushing.
        self.has_leader = true;
        Ok(CallerRole::Leader {
            handle,
            flush_token: self.flush_clock,
        })
    }

    /// Sends [FlushRequest] to [OrderedProducerLoop], and yields the next [BatchBuilder].
    pub(crate) async fn try_flush(&mut self, flush_token: Option<usize>) -> Result<()> {
        if let Some(flush_token) = flush_token {
            if self.flush_clock != flush_token {
                // The flush has been triggered.
                return Ok(());
            }
        }
        let builder = self.batch_builder.take().expect("no batch builder");

        let (builder, maybe_flush_request) = builder.next_batch();
        self.batch_builder = Some(builder);
        self.has_leader = false;
        self.flush_clock = self.flush_clock.wrapping_add(1);

        match maybe_flush_request {
            Ok(Some(flush_request)) => {
                self.sender.send(flush_request).await.map_err(|err| {
                    error::SendFlushRequestSnafu {
                        reason: err.to_string(),
                    }
                    .build()
                })?;
            }
            Ok(None) => {
                // Nothing to flush
            }
            Err(err) => return Err(err),
        }

        Ok(())
    }
}

/// [`OrderedBatchProducer`] attempts to aggregate multiple produce requests together
/// using the provided [`Aggregator`].
///
/// It will buffer up records until either the linger time expires or
/// [`Aggregator`] cannot accommodate another record.
///
/// At this point it will flush the [`Aggregator`].
///
/// The flush will respect the order of aggregated data.
pub struct OrderedBatchProducer<A: Aggregator> {
    /// The timeout of performing aggregating.
    linger: Duration,
    inner: Arc<Mutex<OrderedBatchProducerInner<A>>>,
    /// Used to control the [`BackgroundProducerWorker`].
    running: Arc<AtomicBool>,
    /// The handle of [`BackgroundProducerWorker`].
    handle: JoinHandle<()>,
}

impl<A: Aggregator> Drop for OrderedBatchProducer<A> {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

impl<A: Aggregator> OrderedBatchProducer<A> {
    /// Constructs a new [`OrderedBatchProducer`].
    pub(crate) fn new(
        aggregator: A,
        client: Arc<dyn ProducerClient>,
        linger: Duration,
        compression: Compression,
        flush_queue_size: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(flush_queue_size);
        let running = Arc::new(AtomicBool::new(true));
        let mut worker = BackgroundProducerWorker {
            client,
            receiver: rx,
            compression,
            running: running.clone(),
            flushing: None,
        };

        Self {
            linger,
            inner: Arc::new(Mutex::new(OrderedBatchProducerInner::new(aggregator, tx))),
            running,
            handle: tokio::spawn(async move { worker.run().await }),
        }
    }

    /// Writes `data` to the [`OrderedBatchProducer`].
    ///
    /// Returns the [ResultReceiver], which will receive a result when data has been committed to Kafka
    /// or an unrecoverable error has been encountered.
    pub(crate) async fn produce(&self, data: A::Input) -> Result<ResultReceiver<A>> {
        let role = {
            let mut inner = self.inner.lock().await;
            inner.try_push(data).await?
        };

        match role {
            CallerRole::Waiter(handle) => Ok(ResultReceiver::Waiter { handle }),
            CallerRole::Leader {
                handle,
                flush_token,
            } => Ok(ResultReceiver::Leader {
                handle,
                flush_token,
                linger: self.linger,
                inner: self.inner.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use chrono::{TimeZone, Utc};
    use common_telemetry::debug;
    use futures::future::BoxFuture;
    use futures::stream::{FuturesOrdered, FuturesUnordered};
    use futures::StreamExt;
    use rskafka::client::error::{Error as ClientError, RequestContext};
    use rskafka::client::partition::Compression;
    use rskafka::client::producer::aggregator::RecordAggregator;
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
        let linger = Duration::from_millis(100);
        let delay = Duration::from_secs(0);
        let client = Arc::new(MockClient {
            error: None,
            panic: None,
            delay,
            batch_sizes: Default::default(),
        });
        let aggregator = RecordAggregator::new(record.approximate_size() * 2);
        let producer = OrderedBatchProducer::new(
            aggregator,
            client.clone(),
            linger,
            Compression::NoCompression,
            128,
        );
        let mut futures = FuturesOrdered::new();

        // Produces 3 records
        futures.push_back(producer.produce(record.clone()).await.unwrap().wait());
        futures.push_back(producer.produce(record.clone()).await.unwrap().wait());
        futures.push_back(producer.produce(record.clone()).await.unwrap().wait());

        assert_eq!(
            tokio::time::timeout(Duration::from_millis(10), futures.next())
                .await
                .expect("no timeout")
                .expect("future left")
                .expect("no error"),
            0,
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(10), futures.next())
                .await
                .expect("no timeout")
                .expect("future left")
                .expect("no error"),
            1,
        );

        // Third should leader
        tokio::time::timeout(Duration::from_millis(10), futures.next())
            .await
            .expect_err("timeout");

        assert_eq!(client.batch_sizes.lock().unwrap().as_slice(), &[2]);

        // Should publish third record after linger expires
        assert_eq!(
            tokio::time::timeout(linger * 2, futures.next())
                .await
                .expect("no timeout")
                .expect("future left")
                .expect("no error"),
            2,
        );
        assert_eq!(client.batch_sizes.lock().unwrap().as_slice(), &[2, 1]);
    }

    #[tokio::test]
    async fn test_producer_client_error() {
        let record = record();
        let linger = Duration::from_millis(5);
        let client = Arc::new(MockClient {
            error: Some(ProtocolError::NetworkException),
            panic: None,
            delay: Duration::from_millis(1),
            batch_sizes: Default::default(),
        });

        let aggregator = RecordAggregator::new(record.approximate_size() * 2);
        let producer = OrderedBatchProducer::new(
            aggregator,
            client.clone(),
            linger,
            Compression::NoCompression,
            128,
        );

        let mut futures = FuturesUnordered::new();
        futures.push(producer.produce(record.clone()).await.unwrap().wait());
        futures.push(producer.produce(record.clone()).await.unwrap().wait());

        futures.next().await.unwrap().unwrap_err();
        futures.next().await.unwrap().unwrap_err();
    }
}
