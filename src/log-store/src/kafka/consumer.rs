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

use std::collections::{BTreeSet, VecDeque};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_telemetry::info;
use futures::future::{BoxFuture, Fuse, FusedFuture};
use futures::{FutureExt, Stream};
use pin_project::pin_project;
use rskafka::client::partition::PartitionClient;
use rskafka::record::RecordAndOffset;
use store_api::storage::RegionId;

pub trait WalIndexReader: Send + Sync {
    fn find(&mut self, region_id: RegionId) -> Vec<u64>;
}

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
    watermark: i64,
    used_offset: i64,
}

#[pin_project]
pub struct Consumer {
    pruner: RecordPruner,

    last_high_watermark: i64,

    client: Arc<dyn FetchClient>,

    start_offset: i64,

    next_offset: i64,

    record_bytes: usize,

    end_offset: i64,

    max_wait_ms: i32,

    terminated: bool,

    fetch_fut: Fuse<BoxFuture<'static, rskafka::client::error::Result<FetchResult>>>,
}

impl Consumer {
    pub fn new(
        client: Arc<dyn FetchClient>,
        mut start_offset: u64,
        end_offset: u64,
        mut index: BTreeSet<u64>,
    ) -> Self {
        let index = index.split_off(&start_offset);
        info!("Start offset: {start_offset}, end offset: {end_offset}, index: {index:?}");
        if let Some(first_index) = index.first() {
            info!("Using first index: {first_index}, original start offset: {start_offset}");
            start_offset = start_offset.max(*first_index);
        }
        Self {
            pruner: RecordPruner {
                index: index.into_iter().collect::<VecDeque<_>>(),
                buffer: VecDeque::new(),
            },
            last_high_watermark: 0,
            client,
            start_offset: start_offset as i64,
            next_offset: start_offset as i64,
            record_bytes: 512 * 1024,
            end_offset: end_offset as i64,
            max_wait_ms: 1000,
            terminated: false,
            fetch_fut: Fuse::terminated(),
        }
    }
}

struct RecordPruner {
    index: VecDeque<u64>,

    buffer: VecDeque<RecordAndOffset>,
}

impl RecordPruner {
    pub fn next_offset(&self) -> Option<u64> {
        self.index.front().cloned()
    }

    pub fn pop_front(&mut self) -> Option<RecordAndOffset> {
        if let Some(index) = self.index.front() {
            if let Some(record_and_offset) = self.buffer.pop_front() {
                if *index == record_and_offset.offset as u64 {
                    self.index.pop_front();
                    Some(record_and_offset)
                } else {
                    self.pop_front()
                }
            } else {
                None
            }
        } else {
            self.buffer.pop_front()
        }
    }

    pub fn extend(&mut self, records: Vec<RecordAndOffset>) {
        if let (Some(first), Some(index)) = (records.first(), self.index.front()) {
            assert!(
                *index <= first.offset as u64,
                "index: {index}, first offset: {}",
                first.offset
            );
        }
        self.buffer.extend(records);
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
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
            if this.pruner.is_empty() && this.next_offset > this.end_offset {
                return Poll::Ready(None);
            }

            if let Some(x) = this.pruner.pop_front() {
                info!("Yielding record with offset: {}", x.offset);
                let next_offset = if let Some(next_offset) = this.pruner.next_offset() {
                    next_offset as i64
                } else {
                    x.offset + 1_i64
                };
                if *this.next_offset < next_offset {
                    info!("set next offset: {}", next_offset);
                    *this.next_offset = next_offset;
                }

                if x.offset <= *this.end_offset {
                    return Poll::Ready(Some(Ok((x, *this.last_high_watermark))));
                } else {
                    return Poll::Ready(None);
                }
            }

            if this.fetch_fut.is_terminated() {
                let client = Arc::clone(this.client);
                let bytes = 1i32..(*this.record_bytes as i32);
                let max_wait_ms = *this.max_wait_ms;
                let offset = *this.next_offset;

                *this.fetch_fut = FutureExt::fuse(Box::pin(async move {
                    let (records_and_offsets, watermark) =
                        client.fetch_records(offset, bytes, max_wait_ms).await?;

                    Ok(FetchResult {
                        records_and_offsets,
                        watermark,
                        used_offset: offset,
                    })
                }));
            }

            let data = futures::ready!(this.fetch_fut.poll_unpin(cx));

            match data {
                Ok(FetchResult {
                    mut records_and_offsets,
                    watermark,
                    used_offset,
                }) => {
                    // Remember used offset (might be overwritten if there was any data) so we don't refetch the
                    // earliest / latest offset for every try. Also fetching the latest offset might be racy otherwise,
                    // since we'll never be in a position where the latest one can actually be fetched.
                    *this.next_offset = used_offset;

                    // Sort records by offset in case they aren't in order
                    records_and_offsets.sort_by_key(|x| x.offset);
                    *this.last_high_watermark = watermark;
                    if let Some(x) = records_and_offsets.last() {
                        *this.next_offset = x.offset + 1;
                        info!(
                            "Fetch result: {:?}, used_offset: {used_offset}",
                            records_and_offsets
                                .iter()
                                .map(|record| record.offset)
                                .collect::<Vec<_>>()
                        );
                        this.pruner.extend(records_and_offsets)
                    }
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
    use crate::kafka::consumer::{Consumer, RecordPruner};

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

    #[tokio::test]
    async fn test_consumer_with_index() {
        common_telemetry::init_default_ut_logging();
        let record = Record {
            key: Some(vec![0; 4]),
            value: Some(vec![0; 6]),
            headers: Default::default(),
            timestamp: Utc.timestamp_millis_opt(1337).unwrap(),
        };

        for record_size in [record.approximate_size(), record.approximate_size() * 3] {
            let mock_client = MockFetchClient {
                record: record.clone(),
            };
            let consumer = Consumer {
                pruner: RecordPruner {
                    index: VecDeque::from([5, 7, 8, 10, 12]),
                    buffer: VecDeque::new(),
                },
                last_high_watermark: 0,
                client: Arc::new(mock_client),
                start_offset: 5,
                next_offset: 5,
                record_bytes: record_size,
                end_offset: 15,
                max_wait_ms: 100,
                terminated: false,
                fetch_fut: Fuse::terminated(),
            };
            let records = consumer.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(
                records
                    .into_iter()
                    .map(|(x, _)| x.offset)
                    .collect::<Vec<_>>(),
                vec![5, 7, 8, 10, 12, 13, 14, 15]
            )
        }
    }

    #[tokio::test]
    async fn test_consumer_without_index() {
        common_telemetry::init_default_ut_logging();
        let record = Record {
            key: Some(vec![0; 4]),
            value: Some(vec![0; 6]),
            headers: Default::default(),
            timestamp: Utc.timestamp_millis_opt(1337).unwrap(),
        };
        for record_size in [record.approximate_size(), record.approximate_size() * 3] {
            let mock_client = MockFetchClient {
                record: record.clone(),
            };
            let consumer = Consumer {
                pruner: RecordPruner {
                    index: VecDeque::new(),
                    buffer: VecDeque::new(),
                },
                last_high_watermark: 0,
                client: Arc::new(mock_client),
                start_offset: 5,
                next_offset: 5,
                record_bytes: record_size,
                end_offset: 15,
                max_wait_ms: 100,
                terminated: false,
                fetch_fut: Fuse::terminated(),
            };
            let records = consumer.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(
                records
                    .into_iter()
                    .map(|(x, _)| x.offset)
                    .collect::<Vec<_>>(),
                (5..=15).collect::<Vec<_>>()
            )
        }
    }
}
