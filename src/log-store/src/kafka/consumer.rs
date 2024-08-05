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

use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_telemetry::debug;
use futures::future::{BoxFuture, Fuse, FusedFuture};
use futures::{FutureExt, Stream};
use pin_project::pin_project;
use rskafka::client::partition::PartitionClient;
use rskafka::record::RecordAndOffset;

use super::index::{NextBatchHint, RegionWalIndexIterator};

#[async_trait::async_trait]
pub trait FetchClient: std::fmt::Debug + Send + Sync {
    /// Fetch records.
    ///
    /// Arguments are identical to [`PartitionClient::fetch_records`].
    async fn fetch_records(
        &self,
        offset: i64,
        bytes: Range<i32>,
        max_wait_ms: i32,
    ) -> rskafka::client::error::Result<(Vec<RecordAndOffset>, i64)>;
}

#[async_trait::async_trait]
impl FetchClient for PartitionClient {
    async fn fetch_records(
        &self,
        offset: i64,
        bytes: Range<i32>,
        max_wait_ms: i32,
    ) -> rskafka::client::error::Result<(Vec<RecordAndOffset>, i64)> {
        self.fetch_records(offset, bytes, max_wait_ms).await
    }
}

struct FetchResult {
    records_and_offsets: Vec<RecordAndOffset>,
    batch_size: usize,
    fetch_bytes: i32,
    watermark: i64,
    used_offset: i64,
}

/// The [`Consumer`] struct represents a Kafka consumer that fetches messages from
/// a Kafka cluster. Yielding records respecting the [`RegionWalIndexIterator`].
#[pin_project]
pub struct Consumer {
    last_high_watermark: i64,

    /// The client is used to fetch records from kafka topic.
    client: Arc<dyn FetchClient>,

    /// The max batch size in a single fetch request.
    max_batch_size: usize,

    /// The max wait milliseconds.
    max_wait_ms: u32,

    /// The avg record size
    avg_record_size: usize,

    /// Termination flag
    terminated: bool,

    /// The buffer of records.
    buffer: RecordsBuffer,

    /// The fetch future.
    fetch_fut: Fuse<BoxFuture<'static, rskafka::client::error::Result<FetchResult>>>,
}

struct RecordsBuffer {
    buffer: VecDeque<RecordAndOffset>,

    index: Box<dyn RegionWalIndexIterator>,
}

impl RecordsBuffer {
    fn pop_front(&mut self) -> Option<RecordAndOffset> {
        while let Some(index) = self.index.peek() {
            if let Some(record_and_offset) = self.buffer.pop_front() {
                if index == record_and_offset.offset as u64 {
                    self.index.next();
                    return Some(record_and_offset);
                }
            } else {
                return None;
            }
        }

        self.buffer.clear();
        None
    }

    fn extend(&mut self, records: Vec<RecordAndOffset>) {
        if let (Some(first), Some(index)) = (records.first(), self.index.peek()) {
            // TODO(weny): throw an error?
            assert!(
                index <= first.offset as u64,
                "index: {index}, first offset: {}",
                first.offset
            );
        }
        self.buffer.extend(records);
    }
}

impl Stream for Consumer {
    type Item = rskafka::client::error::Result<(RecordAndOffset, i64)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        loop {
            if *this.terminated {
                return Poll::Ready(None);
            }

            if this.buffer.index.peek().is_none() {
                return Poll::Ready(None);
            }

            if let Some(x) = this.buffer.pop_front() {
                debug!("Yielding record with offset: {}", x.offset);
                return Poll::Ready(Some(Ok((x, *this.last_high_watermark))));
            }

            if this.fetch_fut.is_terminated() {
                match this.buffer.index.peek() {
                    Some(next_offset) => {
                        let client = Arc::clone(this.client);
                        let max_wait_ms = *this.max_wait_ms as i32;
                        let offset = next_offset as i64;
                        let NextBatchHint { bytes, len } = this
                            .buffer
                            .index
                            .next_batch_hint(*this.avg_record_size)
                            .unwrap_or(NextBatchHint {
                                bytes: *this.avg_record_size,
                                len: 1,
                            });

                        let fetch_range =
                            1i32..(bytes.saturating_add(1).min(*this.max_batch_size) as i32);
                        *this.fetch_fut = FutureExt::fuse(Box::pin(async move {
                            let (records_and_offsets, watermark) = client
                                .fetch_records(offset, fetch_range, max_wait_ms)
                                .await?;

                            Ok(FetchResult {
                                records_and_offsets,
                                watermark,
                                used_offset: offset,
                                fetch_bytes: bytes as i32,
                                batch_size: len,
                            })
                        }));
                    }
                    None => {
                        return Poll::Ready(None);
                    }
                }
            }

            let data = futures::ready!(this.fetch_fut.poll_unpin(cx));

            match data {
                Ok(FetchResult {
                    mut records_and_offsets,
                    watermark,
                    used_offset,
                    fetch_bytes,
                    batch_size,
                }) => {
                    // Sort records by offset in case they aren't in order
                    records_and_offsets.sort_unstable_by_key(|x| x.offset);
                    *this.last_high_watermark = watermark;
                    if !records_and_offsets.is_empty() {
                        *this.avg_record_size = fetch_bytes as usize / records_and_offsets.len();
                        debug!("set avg_record_size: {}", *this.avg_record_size);
                    }

                    debug!(
                        "Fetch result: {:?}, used_offset: {used_offset}, max_batch_size: {fetch_bytes}, expected batch_num: {batch_size}, actual batch_num: {}",
                        records_and_offsets
                            .iter()
                            .map(|record| record.offset)
                            .collect::<Vec<_>>(),
                        records_and_offsets
                            .len()
                    );
                    this.buffer.extend(records_and_offsets);
                    continue;
                }
                Err(e) => {
                    *this.terminated = true;

                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::ops::Range;
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use futures::future::Fuse;
    use futures::TryStreamExt;
    use rskafka::record::{Record, RecordAndOffset};

    use super::FetchClient;
    use crate::kafka::consumer::{Consumer, RecordsBuffer};
    use crate::kafka::index::{MultipleRegionWalIndexIterator, RegionWalRange, RegionWalVecIndex};

    #[derive(Debug)]
    struct MockFetchClient {
        record: Record,
    }

    #[async_trait::async_trait]
    impl FetchClient for MockFetchClient {
        async fn fetch_records(
            &self,
            offset: i64,
            bytes: Range<i32>,
            _max_wait_ms: i32,
        ) -> rskafka::client::error::Result<(Vec<RecordAndOffset>, i64)> {
            let record_size = self.record.approximate_size();
            let num = (bytes.end.unsigned_abs() as usize / record_size).max(1);

            let records = (0..num)
                .map(|idx| RecordAndOffset {
                    record: self.record.clone(),
                    offset: offset + idx as i64,
                })
                .collect::<Vec<_>>();
            let max_offset = offset + records.len() as i64;
            Ok((records, max_offset))
        }
    }

    fn test_record() -> Record {
        Record {
            key: Some(vec![0; 4]),
            value: Some(vec![0; 6]),
            headers: Default::default(),
            timestamp: Utc.timestamp_millis_opt(1337).unwrap(),
        }
    }

    #[tokio::test]
    async fn test_consumer_with_index() {
        common_telemetry::init_default_ut_logging();
        let record = test_record();
        let record_size = record.approximate_size();
        let mock_client = MockFetchClient {
            record: record.clone(),
        };
        let index = RegionWalVecIndex::new([1, 3, 4, 8, 10, 12], record_size * 3);
        let consumer = Consumer {
            last_high_watermark: -1,
            client: Arc::new(mock_client),
            max_batch_size: usize::MAX,
            max_wait_ms: 500,
            avg_record_size: record_size,
            terminated: false,
            buffer: RecordsBuffer {
                buffer: VecDeque::new(),
                index: Box::new(index),
            },
            fetch_fut: Fuse::terminated(),
        };

        let records = consumer.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            records
                .into_iter()
                .map(|(x, _)| x.offset)
                .collect::<Vec<_>>(),
            vec![1, 3, 4, 8, 10, 12]
        )
    }

    #[tokio::test]
    async fn test_consumer_without_index() {
        common_telemetry::init_default_ut_logging();
        let record = test_record();
        let mock_client = MockFetchClient {
            record: record.clone(),
        };
        let index = RegionWalRange::new(0..30, 1024);
        let consumer = Consumer {
            last_high_watermark: -1,
            client: Arc::new(mock_client),
            max_batch_size: usize::MAX,
            max_wait_ms: 500,
            avg_record_size: record.approximate_size(),
            terminated: false,
            buffer: RecordsBuffer {
                buffer: VecDeque::new(),
                index: Box::new(index),
            },
            fetch_fut: Fuse::terminated(),
        };

        let records = consumer.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            records
                .into_iter()
                .map(|(x, _)| x.offset)
                .collect::<Vec<_>>(),
            (0..30).collect::<Vec<_>>()
        )
    }

    #[tokio::test]
    async fn test_consumer_with_multiple_index() {
        common_telemetry::init_default_ut_logging();
        let record = test_record();
        let mock_client = MockFetchClient {
            record: record.clone(),
        };

        let iter0 = Box::new(RegionWalRange::new(0..0, 1024)) as _;
        let iter1 = Box::new(RegionWalVecIndex::new(
            [0, 1, 2, 7, 8, 11],
            record.approximate_size() * 4,
        )) as _;
        let iter2 = Box::new(RegionWalRange::new(12..12, 1024)) as _;
        let iter3 = Box::new(RegionWalRange::new(1024..1028, 1024)) as _;
        let iter = MultipleRegionWalIndexIterator::new([iter0, iter1, iter2, iter3]);

        let consumer = Consumer {
            last_high_watermark: -1,
            client: Arc::new(mock_client),
            max_batch_size: usize::MAX,
            max_wait_ms: 500,
            avg_record_size: record.approximate_size(),
            terminated: false,
            buffer: RecordsBuffer {
                buffer: VecDeque::new(),
                index: Box::new(iter),
            },
            fetch_fut: Fuse::terminated(),
        };

        let records = consumer.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            records
                .into_iter()
                .map(|(x, _)| x.offset)
                .collect::<Vec<_>>(),
            [0, 1, 2, 7, 8, 11, 1024, 1025, 1026, 1027]
        )
    }
}
