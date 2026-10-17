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

use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use common_telemetry::{info, warn};
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::record::Record;

use crate::kafka::consumer::{FetchClient, FetchResult};
use crate::kafka::producer::{ProduceResult, ProducerClient};

/// The number of TLS records written to a connection after which the connection is replaced.
///
/// rustls caps an AES-GCM key at 2^24 TLS records. TLS 1.2 has no KeyUpdate, so rustls closes the
/// connection once the cap is reached, and the in-flight requests may hang until they time out
/// (see <https://github.com/GreptimeTeam/greptimedb/issues/9681>). Half of the cap leaves room for
/// the estimation error of [`estimate_tls_records`].
pub(crate) const DEFAULT_TLS_RECORD_LIMIT: u64 = 1 << 23;

/// The max plaintext size of a TLS record.
const MAX_TLS_RECORD_PAYLOAD: usize = 16 * 1024;

/// The default send buffer limit of rustls. A large write is accepted in chunks of at most this
/// size, and each chunk may end with a partially filled record.
const RUSTLS_BUFFER_LIMIT: usize = 64 * 1024;

/// Estimates the TLS records needed to write a request whose body is `request_size` bytes.
///
/// rskafka writes the 4-byte length prefix and the body separately, so the prefix always takes
/// its own record.
fn estimate_tls_records(request_size: usize) -> u64 {
    let body = request_size.max(1);
    (1 + body.div_ceil(MAX_TLS_RECORD_PAYLOAD) + body.div_ceil(RUSTLS_BUFFER_LIMIT)) as u64
}

/// Tracks the TLS records written to a connection.
#[derive(Debug)]
struct TlsRecordBudget {
    limit: u64,
    written: AtomicU64,
    rotating: AtomicBool,
}

impl TlsRecordBudget {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            written: AtomicU64::new(0),
            rotating: AtomicBool::new(false),
        }
    }

    /// Adds `records` to the written records.
    ///
    /// Returns true if the limit is reached and the caller is responsible for the rotation.
    fn consume(&self, records: u64) -> bool {
        let written = self.written.fetch_add(records, Ordering::Relaxed) + records;
        written >= self.limit
            && self
                .rotating
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }

    /// Ends the rotation started by [`TlsRecordBudget::consume`].
    fn finish_rotation(&self, rotated: bool) {
        if rotated {
            self.written.store(0, Ordering::Relaxed);
        }
        self.rotating.store(false, Ordering::Release);
    }
}

#[derive(Debug)]
struct Inner {
    client: Arc<rskafka::client::Client>,
    topic: String,
    partition: i32,
    current: RwLock<Arc<PartitionClient>>,
    /// `None` if the connection is never rotated, e.g., the connection is not encrypted.
    budget: Option<TlsRecordBudget>,
}

/// A [`PartitionClient`] whose broker connection is replaced before it writes too many TLS records.
///
/// Every request adds its estimated TLS records to a budget. Once the budget is exhausted, a new
/// [`PartitionClient`], which owns a new broker connection, is created in the background and
/// replaces the current one. Requests keep using the current connection until the replacement
/// is ready, and in-flight requests on the old connection are not affected.
#[derive(Debug)]
pub(crate) struct RotatingPartitionClient {
    inner: Arc<Inner>,
}

impl RotatingPartitionClient {
    /// Creates a [`RotatingPartitionClient`] for the partition of the `client`.
    ///
    /// The connection is rotated after writing `tls_record_limit` TLS records, or never if it is `None`.
    pub(crate) fn new(
        client: Arc<rskafka::client::Client>,
        partition_client: PartitionClient,
        tls_record_limit: Option<u64>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                client,
                topic: partition_client.topic().to_string(),
                partition: partition_client.partition(),
                current: RwLock::new(Arc::new(partition_client)),
                budget: tls_record_limit.map(TlsRecordBudget::new),
            }),
        }
    }

    fn current(&self) -> Arc<PartitionClient> {
        self.inner.current.read().unwrap().clone()
    }

    /// Accounts `records` TLS records written, and starts a rotation if the budget is exhausted.
    fn consume(&self, records: u64) {
        let Some(budget) = &self.inner.budget else {
            return;
        };
        if budget.consume(records) {
            let inner = self.inner.clone();
            tokio::spawn(async move { inner.rotate().await });
        }
    }

    /// Gets the offset of the partition. See [`PartitionClient::get_offset`].
    pub(crate) async fn get_offset(&self, at: OffsetAt) -> rskafka::client::error::Result<i64> {
        let result = self.current().get_offset(at).await;
        self.consume(estimate_tls_records(0));
        result
    }
}

impl Inner {
    async fn rotate(&self) {
        // Safety: only called when the budget exists.
        let budget = self.budget.as_ref().unwrap();
        info!(
            "Rotating the broker connection of topic: {}, partition: {}, written TLS records: {}",
            self.topic,
            self.partition,
            budget.written.load(Ordering::Relaxed)
        );
        // The topic is known to exist, so there is no need to wait for it.
        let result = self
            .client
            .partition_client(
                self.topic.as_str(),
                self.partition,
                UnknownTopicHandling::Error,
            )
            .await;
        match result {
            Ok(partition_client) => {
                *self.current.write().unwrap() = Arc::new(partition_client);
                budget.finish_rotation(true);
                info!(
                    "Rotated the broker connection of topic: {}, partition: {}",
                    self.topic, self.partition
                );
            }
            Err(e) => {
                // Keeps the current connection; the next request retries the rotation.
                budget.finish_rotation(false);
                warn!(
                    e; "Failed to rotate the broker connection of topic: {}, partition: {}",
                    self.topic, self.partition
                );
            }
        }
    }
}

#[async_trait::async_trait]
impl ProducerClient for RotatingPartitionClient {
    async fn produce(
        &self,
        records: Vec<Record>,
        compression: Compression,
    ) -> rskafka::client::error::Result<ProduceResult> {
        let approximate_size = records.iter().map(Record::approximate_size).sum();
        let result = ProducerClient::produce(&*self.current(), records, compression).await;
        let request_size = result
            .as_ref()
            .map_or(approximate_size, |r| r.encoded_request_size);
        self.consume(estimate_tls_records(request_size));
        result
    }

    async fn get_offset(&self, at: OffsetAt) -> rskafka::client::error::Result<i64> {
        RotatingPartitionClient::get_offset(self, at).await
    }
}

#[async_trait::async_trait]
impl FetchClient for RotatingPartitionClient {
    async fn fetch_records(
        &self,
        offset: i64,
        bytes: Range<i32>,
        max_wait_ms: i32,
    ) -> rskafka::client::error::Result<FetchResult> {
        let result = FetchClient::fetch_records(&*self.current(), offset, bytes, max_wait_ms).await;
        self.consume(estimate_tls_records(0));
        result
    }

    fn topic(&self) -> &str {
        &self.inner.topic
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_wal::maybe_skip_kafka_integration_test;
    use common_wal::test_util::get_kafka_endpoints;
    use rskafka::client::ClientBuilder;

    use super::*;
    use crate::kafka::test_util::{create_topics, record};

    #[test]
    fn test_estimate_tls_records() {
        assert_eq!(estimate_tls_records(0), 3);
        assert_eq!(estimate_tls_records(100), 3);
        assert_eq!(estimate_tls_records(MAX_TLS_RECORD_PAYLOAD + 1), 4);
        // 1 prefix record + 5 full records + 2 partial records of the 64 KiB chunks.
        assert_eq!(estimate_tls_records(RUSTLS_BUFFER_LIMIT + 1), 8);
    }

    #[test]
    fn test_tls_record_budget() {
        let budget = TlsRecordBudget::new(10);
        assert!(!budget.consume(9));
        assert!(budget.consume(1));
        // Only one caller rotates at a time.
        assert!(!budget.consume(1));

        // A failed rotation keeps the written records, so the next request retries.
        budget.finish_rotation(false);
        assert!(budget.consume(1));

        // A successful rotation resets the written records.
        budget.finish_rotation(true);
        assert!(!budget.consume(9));
        assert!(budget.consume(1));
    }

    #[tokio::test]
    async fn test_rotate_connection() {
        maybe_skip_kafka_integration_test!();
        let broker_endpoints = get_kafka_endpoints();
        let topic = create_topics(
            1,
            |_| format!("test_rotate_connection_{}", uuid::Uuid::new_v4()),
            &broker_endpoints,
        )
        .await
        .remove(0);
        let client = Arc::new(ClientBuilder::new(broker_endpoints).build().await.unwrap());
        let partition_client = client
            .partition_client(topic.as_str(), 0, UnknownTopicHandling::Retry)
            .await
            .unwrap();
        // Every request exhausts the budget.
        let client = RotatingPartitionClient::new(client, partition_client, Some(1));

        let budget = client.inner.budget.as_ref().unwrap();
        for i in 0..3 {
            let previous = client.current();
            let result =
                ProducerClient::produce(&client, vec![record()], Compression::NoCompression)
                    .await
                    .unwrap();
            assert_eq!(result.offsets, vec![i]);

            // Waits for the rotation to finish.
            tokio::time::timeout(Duration::from_secs(10), async {
                while Arc::ptr_eq(&previous, &client.current())
                    || budget.rotating.load(Ordering::Acquire)
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
        assert_eq!(client.get_offset(OffsetAt::Latest).await.unwrap(), 3);
    }
}
