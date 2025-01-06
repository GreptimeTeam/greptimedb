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

use std::collections::HashMap;
use std::sync::Arc;

use async_stream::stream;
use common_telemetry::{debug, error};
use futures::future::join_all;
use snafu::OptionExt;
use store_api::logstore::entry::Entry;
use store_api::logstore::provider::Provider;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

use crate::error::{self, Result};
use crate::wal::entry_reader::{decode_raw_entry, WalEntryReader};
use crate::wal::raw_entry_reader::RawEntryReader;
use crate::wal::{EntryId, WalEntryStream};

/// [WalEntryDistributor] distributes Wal entries to specific [WalEntryReceiver]s based on [RegionId].
pub(crate) struct WalEntryDistributor {
    raw_wal_reader: Arc<dyn RawEntryReader>,
    provider: Provider,
    /// Sends [Entry] to receivers based on [RegionId]
    senders: HashMap<RegionId, Sender<Entry>>,
    /// Waits for the arg from the [WalEntryReader].
    arg_receivers: Vec<(RegionId, oneshot::Receiver<EntryId>)>,
}

impl WalEntryDistributor {
    /// Distributes entries to specific [WalEntryReceiver]s based on [RegionId].
    pub async fn distribute(mut self) -> Result<()> {
        let arg_futures = self
            .arg_receivers
            .iter_mut()
            .map(|(region_id, receiver)| async { (*region_id, receiver.await.ok()) });
        let args = join_all(arg_futures)
            .await
            .into_iter()
            .filter_map(|(region_id, start_id)| start_id.map(|start_id| (region_id, start_id)))
            .collect::<Vec<_>>();

        // No subscribers
        if args.is_empty() {
            return Ok(());
        }
        // Safety: must exist
        let min_start_id = args.iter().map(|(_, start_id)| *start_id).min().unwrap();
        let receivers: HashMap<_, _> = args
            .into_iter()
            .map(|(region_id, start_id)| {
                (
                    region_id,
                    EntryReceiver {
                        start_id,
                        sender: self.senders[&region_id].clone(),
                    },
                )
            })
            .collect();

        let mut stream = self.raw_wal_reader.read(&self.provider, min_start_id)?;
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            let entry_id = entry.entry_id();
            let region_id = entry.region_id();

            if let Some(EntryReceiver { sender, start_id }) = receivers.get(&region_id) {
                if entry_id >= *start_id {
                    if let Err(err) = sender.send(entry).await {
                        error!(err; "Failed to distribute raw entry, entry_id:{}, region_id: {}", entry_id, region_id);
                    }
                }
            } else {
                debug!("Subscriber not found, region_id: {}", region_id);
            }
        }

        Ok(())
    }
}

/// Receives the Wal entries from [WalEntryDistributor].
#[derive(Debug)]
pub(crate) struct WalEntryReceiver {
    /// Receives the [Entry] from the [WalEntryDistributor].
    entry_receiver: Option<Receiver<Entry>>,
    /// Sends the `start_id` to the [WalEntryDistributor].
    arg_sender: Option<oneshot::Sender<EntryId>>,
}

impl WalEntryReceiver {
    pub fn new(entry_receiver: Receiver<Entry>, arg_sender: oneshot::Sender<EntryId>) -> Self {
        Self {
            entry_receiver: Some(entry_receiver),
            arg_sender: Some(arg_sender),
        }
    }
}

impl WalEntryReader for WalEntryReceiver {
    fn read(&mut self, _provider: &Provider, start_id: EntryId) -> Result<WalEntryStream<'static>> {
        let arg_sender =
            self.arg_sender
                .take()
                .with_context(|| error::InvalidWalReadRequestSnafu {
                    reason: format!("Call WalEntryReceiver multiple time, start_id: {start_id}"),
                })?;
        // Safety: check via arg_sender
        let mut entry_receiver = self.entry_receiver.take().unwrap();

        if arg_sender.send(start_id).is_err() {
            return error::InvalidWalReadRequestSnafu {
                reason: format!(
                    "WalEntryDistributor is dropped, failed to send arg, start_id: {start_id}"
                ),
            }
            .fail();
        }

        let stream = stream! {
            let mut buffered_entry = None;
            while let Some(next_entry) = entry_receiver.recv().await {
                match buffered_entry.take() {
                    Some(entry) => {
                        yield decode_raw_entry(entry);
                        buffered_entry = Some(next_entry);
                    },
                    None => {
                        buffered_entry = Some(next_entry);
                    }
                };
            }
            if let Some(entry) = buffered_entry {
                // Ignores tail corrupted data.
                if entry.is_complete() {
                    yield decode_raw_entry(entry);
                }
            }
        };

        Ok(Box::pin(stream))
    }
}

struct EntryReceiver {
    start_id: EntryId,
    sender: Sender<Entry>,
}

/// The default buffer size of the [Entry] receiver.
pub const DEFAULT_ENTRY_RECEIVER_BUFFER_SIZE: usize = 2048;

/// Returns [WalEntryDistributor] and batch [WalEntryReceiver]s.
///
/// ### Note:
/// Ensures `receiver.read` is called before the `distributor.distribute` in the same thread.
///
/// ```text
/// let (distributor, receivers) = build_wal_entry_distributor_and_receivers(..);
///  Thread 1                        |
///                                  |
/// // may deadlock                  |
/// distributor.distribute().await;  |
///                                  |  
///                                  |
/// receivers[0].read().await        |
/// ```
///
pub fn build_wal_entry_distributor_and_receivers(
    provider: Provider,
    raw_wal_reader: Arc<dyn RawEntryReader>,
    region_ids: &[RegionId],
    buffer_size: usize,
) -> (WalEntryDistributor, Vec<WalEntryReceiver>) {
    let mut senders = HashMap::with_capacity(region_ids.len());
    let mut readers = Vec::with_capacity(region_ids.len());
    let mut arg_receivers = Vec::with_capacity(region_ids.len());

    for &region_id in region_ids {
        let (entry_sender, entry_receiver) = mpsc::channel(buffer_size);
        let (arg_sender, arg_receiver) = oneshot::channel();

        senders.insert(region_id, entry_sender);
        arg_receivers.push((region_id, arg_receiver));
        readers.push(WalEntryReceiver::new(entry_receiver, arg_sender));
    }

    (
        WalEntryDistributor {
            provider,
            raw_wal_reader,
            senders,
            arg_receivers,
        },
        readers,
    )
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use api::v1::{Mutation, OpType, WalEntry};
    use futures::{stream, TryStreamExt};
    use prost::Message;
    use store_api::logstore::entry::{Entry, MultiplePartEntry, MultiplePartHeader, NaiveEntry};

    use super::*;
    use crate::test_util::wal_util::generate_tail_corrupted_stream;
    use crate::wal::raw_entry_reader::{EntryStream, RawEntryReader};
    use crate::wal::EntryId;

    struct MockRawEntryReader {
        entries: Vec<Entry>,
    }

    impl MockRawEntryReader {
        pub fn new(entries: Vec<Entry>) -> MockRawEntryReader {
            Self { entries }
        }
    }

    impl RawEntryReader for MockRawEntryReader {
        fn read(&self, _provider: &Provider, _start_id: EntryId) -> Result<EntryStream<'static>> {
            let stream = stream::iter(self.entries.clone().into_iter().map(Ok));
            Ok(Box::pin(stream))
        }
    }

    #[tokio::test]
    async fn test_wal_entry_distributor_without_receivers() {
        let provider = Provider::kafka_provider("my_topic".to_string());
        let reader = Arc::new(MockRawEntryReader::new(vec![Entry::Naive(NaiveEntry {
            region_id: RegionId::new(1024, 1),
            provider: provider.clone(),
            entry_id: 1,
            data: vec![1],
        })]));

        let (distributor, receivers) = build_wal_entry_distributor_and_receivers(
            provider,
            reader,
            &[RegionId::new(1024, 1), RegionId::new(1025, 1)],
            128,
        );

        // Drops all receivers
        drop(receivers);
        // Returns immediately
        distributor.distribute().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_entry_distributor() {
        common_telemetry::init_default_ut_logging();
        let provider = Provider::kafka_provider("my_topic".to_string());
        let reader = Arc::new(MockRawEntryReader::new(vec![
            Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: RegionId::new(1024, 1),
                entry_id: 1,
                data: WalEntry {
                    mutations: vec![Mutation {
                        op_type: OpType::Put as i32,
                        sequence: 1u64,
                        rows: None,
                        write_hint: 0,
                    }],
                }
                .encode_to_vec(),
            }),
            Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: RegionId::new(1024, 2),
                entry_id: 2,
                data: WalEntry {
                    mutations: vec![Mutation {
                        op_type: OpType::Put as i32,
                        sequence: 2u64,
                        rows: None,
                        write_hint: 0,
                    }],
                }
                .encode_to_vec(),
            }),
            Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: RegionId::new(1024, 3),
                entry_id: 3,
                data: WalEntry {
                    mutations: vec![Mutation {
                        op_type: OpType::Put as i32,
                        sequence: 3u64,
                        rows: None,
                        write_hint: 0,
                    }],
                }
                .encode_to_vec(),
            }),
        ]));

        // Builds distributor and receivers
        let (distributor, mut receivers) = build_wal_entry_distributor_and_receivers(
            provider.clone(),
            reader,
            &[
                RegionId::new(1024, 1),
                RegionId::new(1024, 2),
                RegionId::new(1024, 3),
            ],
            128,
        );
        assert_eq!(receivers.len(), 3);

        // Should be okay if one of receiver is dropped.
        let last = receivers.pop().unwrap();
        drop(last);

        let mut streams = receivers
            .iter_mut()
            .map(|receiver| receiver.read(&provider, 0).unwrap())
            .collect::<Vec<_>>();
        distributor.distribute().await.unwrap();
        let entries = streams
            .get_mut(0)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            entries,
            vec![(
                1,
                WalEntry {
                    mutations: vec![Mutation {
                        op_type: OpType::Put as i32,
                        sequence: 1u64,
                        rows: None,
                        write_hint: 0,
                    }],
                }
            )]
        );
        let entries = streams
            .get_mut(1)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            entries,
            vec![(
                2,
                WalEntry {
                    mutations: vec![Mutation {
                        op_type: OpType::Put as i32,
                        sequence: 2u64,
                        rows: None,
                        write_hint: 0,
                    }],
                }
            )]
        );
    }

    #[tokio::test]
    async fn test_tail_corrupted_stream() {
        let mut entries = vec![];
        let region1 = RegionId::new(1, 1);
        let region1_expected_wal_entry = WalEntry {
            mutations: vec![Mutation {
                op_type: OpType::Put as i32,
                sequence: 1u64,
                rows: None,
                write_hint: 0,
            }],
        };
        let region2 = RegionId::new(1, 2);
        let region2_expected_wal_entry = WalEntry {
            mutations: vec![Mutation {
                op_type: OpType::Put as i32,
                sequence: 3u64,
                rows: None,
                write_hint: 0,
            }],
        };
        let region3 = RegionId::new(1, 3);
        let region3_expected_wal_entry = WalEntry {
            mutations: vec![Mutation {
                op_type: OpType::Put as i32,
                sequence: 3u64,
                rows: None,
                write_hint: 0,
            }],
        };
        let provider = Provider::kafka_provider("my_topic".to_string());
        entries.extend(generate_tail_corrupted_stream(
            provider.clone(),
            region1,
            &region1_expected_wal_entry,
            3,
        ));
        entries.extend(generate_tail_corrupted_stream(
            provider.clone(),
            region2,
            &region2_expected_wal_entry,
            2,
        ));
        entries.extend(generate_tail_corrupted_stream(
            provider.clone(),
            region3,
            &region3_expected_wal_entry,
            4,
        ));

        let corrupted_stream = MockRawEntryReader { entries };
        // Builds distributor and receivers
        let (distributor, mut receivers) = build_wal_entry_distributor_and_receivers(
            provider.clone(),
            Arc::new(corrupted_stream),
            &[region1, region2, region3],
            128,
        );
        assert_eq!(receivers.len(), 3);
        let mut streams = receivers
            .iter_mut()
            .map(|receiver| receiver.read(&provider, 0).unwrap())
            .collect::<Vec<_>>();
        distributor.distribute().await.unwrap();

        assert_eq!(
            streams
                .get_mut(0)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![(0, region1_expected_wal_entry)]
        );

        assert_eq!(
            streams
                .get_mut(1)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![(0, region2_expected_wal_entry)]
        );

        assert_eq!(
            streams
                .get_mut(2)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![(0, region3_expected_wal_entry)]
        );
    }

    #[tokio::test]
    async fn test_part_corrupted_stream() {
        let mut entries = vec![];
        let region1 = RegionId::new(1, 1);
        let region1_expected_wal_entry = WalEntry {
            mutations: vec![Mutation {
                op_type: OpType::Put as i32,
                sequence: 1u64,
                rows: None,
                write_hint: 0,
            }],
        };
        let region2 = RegionId::new(1, 2);
        let provider = Provider::kafka_provider("my_topic".to_string());
        entries.extend(generate_tail_corrupted_stream(
            provider.clone(),
            region1,
            &region1_expected_wal_entry,
            3,
        ));
        entries.extend(vec![
            // The corrupted data.
            Entry::MultiplePart(MultiplePartEntry {
                provider: provider.clone(),
                region_id: region2,
                entry_id: 0,
                headers: vec![MultiplePartHeader::First],
                parts: vec![vec![1; 100]],
            }),
            Entry::MultiplePart(MultiplePartEntry {
                provider: provider.clone(),
                region_id: region2,
                entry_id: 0,
                headers: vec![MultiplePartHeader::First],
                parts: vec![vec![1; 100]],
            }),
        ]);

        let corrupted_stream = MockRawEntryReader { entries };
        // Builds distributor and receivers
        let (distributor, mut receivers) = build_wal_entry_distributor_and_receivers(
            provider.clone(),
            Arc::new(corrupted_stream),
            &[region1, region2],
            128,
        );
        assert_eq!(receivers.len(), 2);
        let mut streams = receivers
            .iter_mut()
            .map(|receiver| receiver.read(&provider, 0).unwrap())
            .collect::<Vec<_>>();
        distributor.distribute().await.unwrap();
        assert_eq!(
            streams
                .get_mut(0)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![(0, region1_expected_wal_entry)]
        );

        assert_matches!(
            streams
                .get_mut(1)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err(),
            error::Error::CorruptedEntry { .. }
        );
    }

    #[tokio::test]
    async fn test_wal_entry_receiver_start_id() {
        let provider = Provider::kafka_provider("my_topic".to_string());
        let reader = Arc::new(MockRawEntryReader::new(vec![
            Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: RegionId::new(1024, 1),
                entry_id: 1,
                data: WalEntry {
                    mutations: vec![Mutation {
                        op_type: OpType::Put as i32,
                        sequence: 1u64,
                        rows: None,
                        write_hint: 0,
                    }],
                }
                .encode_to_vec(),
            }),
            Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: RegionId::new(1024, 2),
                entry_id: 2,
                data: WalEntry {
                    mutations: vec![Mutation {
                        op_type: OpType::Put as i32,
                        sequence: 2u64,
                        rows: None,
                        write_hint: 0,
                    }],
                }
                .encode_to_vec(),
            }),
            Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: RegionId::new(1024, 1),
                entry_id: 3,
                data: WalEntry {
                    mutations: vec![Mutation {
                        op_type: OpType::Put as i32,
                        sequence: 3u64,
                        rows: None,
                        write_hint: 0,
                    }],
                }
                .encode_to_vec(),
            }),
            Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: RegionId::new(1024, 2),
                entry_id: 4,
                data: WalEntry {
                    mutations: vec![Mutation {
                        op_type: OpType::Put as i32,
                        sequence: 4u64,
                        rows: None,
                        write_hint: 0,
                    }],
                }
                .encode_to_vec(),
            }),
        ]));

        // Builds distributor and receivers
        let (distributor, mut receivers) = build_wal_entry_distributor_and_receivers(
            provider.clone(),
            reader,
            &[RegionId::new(1024, 1), RegionId::new(1024, 2)],
            128,
        );
        assert_eq!(receivers.len(), 2);
        let mut streams = receivers
            .iter_mut()
            .map(|receiver| receiver.read(&provider, 4).unwrap())
            .collect::<Vec<_>>();
        distributor.distribute().await.unwrap();

        assert_eq!(
            streams
                .get_mut(1)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![(
                4,
                WalEntry {
                    mutations: vec![Mutation {
                        op_type: OpType::Put as i32,
                        sequence: 4u64,
                        rows: None,
                        write_hint: 0,
                    }],
                }
            )]
        );
    }
}
