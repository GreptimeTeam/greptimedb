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

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_wal::options::{KafkaWalOptions, WalOptions};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::logstore::entry::Entry;
use store_api::logstore::provider::{KafkaProvider, Provider, RaftEngineProvider};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio_stream::StreamExt;

use crate::error::{self, Result};
use crate::wal::EntryId;

/// A stream that yields [Entry].
pub type EntryStream<'a> = BoxStream<'a, Result<Entry>>;

/// [RawEntryReader] provides the ability to read [Entry] from the underlying [LogStore].
pub(crate) trait RawEntryReader: Send + Sync {
    fn read(&self, provider: &Provider, start_id: EntryId) -> Result<EntryStream<'static>>;
}

/// Implement the [RawEntryReader] for the [LogStore].
pub struct LogStoreRawEntryReader<S> {
    store: Arc<S>,
}

impl<S: LogStore> LogStoreRawEntryReader<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    fn read_region(
        &self,
        ns: RaftEngineProvider,
        start_id: EntryId,
    ) -> Result<EntryStream<'static>> {
        let region_id = RegionId::from_u64(ns.id);
        let store = self.store.clone();

        let stream = try_stream!({
            let mut stream = store
                .read(&Provider::RaftEngine(ns), start_id)
                .await
                .map_err(BoxedError::new)
                .context(error::ReadWalSnafu { region_id })?;

            while let Some(entries) = stream.next().await {
                let entries = entries
                    .map_err(BoxedError::new)
                    .context(error::ReadWalSnafu { region_id })?;

                for entry in entries {
                    yield entry
                }
            }
        });

        Ok(Box::pin(stream))
    }

    fn read_topic(
        &self,
        ns: Arc<KafkaProvider>,
        start_id: EntryId,
    ) -> Result<EntryStream<'static>> {
        let store = self.store.clone();
        let stream = try_stream!({
            let mut stream = store
                .read(&Provider::Kafka(ns.clone()), start_id)
                .await
                .map_err(BoxedError::new)
                .context(error::ReadKafkaWalSnafu { topic: &ns.topic })?;
            while let Some(entries) = stream.next().await {
                let entries = entries
                    .map_err(BoxedError::new)
                    .context(error::ReadKafkaWalSnafu { topic: &ns.topic })?;

                for entry in entries {
                    yield entry
                }
            }
        });

        Ok(Box::pin(stream))
    }
}

impl<S: LogStore> RawEntryReader for LogStoreRawEntryReader<S> {
    fn read(&self, ctx: &Provider, start_id: EntryId) -> Result<EntryStream<'static>> {
        let stream = match ctx {
            Provider::RaftEngine(ns) => self.read_region(*ns, start_id)?,
            Provider::Kafka(ns) => self.read_topic(ns.clone(), start_id)?,
        };

        Ok(Box::pin(stream))
    }
}

/// A [RawEntryReader] reads [RawEntry] belongs to a specific region.
pub struct RegionRawEntryReader<R> {
    reader: R,
    region_id: RegionId,
}

impl<R> RegionRawEntryReader<R>
where
    R: RawEntryReader,
{
    pub fn new(reader: R, region_id: RegionId) -> Self {
        Self { reader, region_id }
    }
}

impl<R> RawEntryReader for RegionRawEntryReader<R>
where
    R: RawEntryReader,
{
    fn read(&self, ctx: &Provider, start_id: EntryId) -> Result<EntryStream<'static>> {
        let mut stream = self.reader.read(ctx, start_id)?;
        let region_id = self.region_id;

        let stream = try_stream!({
            while let Some(entry) = stream.next().await {
                let entry = entry?;
                if entry.region_id() == region_id {
                    yield entry
                }
            }
        });

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_wal::options::WalOptions;
    use futures::stream;
    use store_api::logstore::entry::{Entry, NaiveEntry};
    use store_api::logstore::entry_stream::SendableEntryStream;
    use store_api::logstore::{AppendBatchResponse, AppendResponse, EntryId, LogStore};
    use store_api::storage::RegionId;

    use super::*;
    use crate::error;

    #[derive(Debug)]
    struct MockLogStore {
        entries: Vec<Entry>,
    }

    #[async_trait::async_trait]
    impl LogStore for MockLogStore {
        type Error = error::Error;

        async fn stop(&self) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn append_batch(
            &self,
            entries: Vec<Entry>,
        ) -> Result<AppendBatchResponse, Self::Error> {
            unreachable!()
        }

        async fn read(
            &self,
            provider: &Provider,
            id: EntryId,
        ) -> Result<SendableEntryStream<'static, Entry, Self::Error>, Self::Error> {
            Ok(Box::pin(stream::iter(vec![Ok(self.entries.clone())])))
        }

        async fn create_namespace(&self, ns: &Provider) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn delete_namespace(&self, ns: &Provider) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn list_namespaces(&self) -> Result<Vec<Provider>, Self::Error> {
            unreachable!()
        }

        async fn obsolete(
            &self,
            provider: &Provider,
            entry_id: EntryId,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn entry(
            &self,
            data: &mut Vec<u8>,
            entry_id: EntryId,
            region_id: RegionId,
            provider: &Provider,
        ) -> Result<Entry, Self::Error> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_raw_entry_reader() {
        let provider = Provider::raft_engine_provider(RegionId::new(1024, 1).as_u64());
        let expected_entries = vec![Entry::Naive(NaiveEntry {
            provider: provider.clone(),
            region_id: RegionId::new(1024, 1),
            entry_id: 1,
            data: vec![1],
        })];
        let store = MockLogStore {
            entries: expected_entries.clone(),
        };

        let reader = LogStoreRawEntryReader::new(Arc::new(store));
        let entries = reader
            .read(&provider, 0)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(expected_entries, entries);
    }

    #[tokio::test]
    async fn test_raw_entry_reader_filter() {
        let provider = Provider::raft_engine_provider(RegionId::new(1024, 1).as_u64());
        let all_entries = vec![
            Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: RegionId::new(1024, 1),
                entry_id: 1,
                data: vec![1],
            }),
            Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: RegionId::new(1024, 2),
                entry_id: 2,
                data: vec![2],
            }),
            Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: RegionId::new(1024, 3),
                entry_id: 3,
                data: vec![3],
            }),
        ];
        let store = MockLogStore {
            entries: all_entries.clone(),
        };

        let expected_region_id = RegionId::new(1024, 3);
        let reader = RegionRawEntryReader::new(
            LogStoreRawEntryReader::new(Arc::new(store)),
            expected_region_id,
        );
        let entries = reader
            .read(&provider, 0)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            all_entries
                .into_iter()
                .filter(|entry| entry.region_id() == expected_region_id)
                .collect::<Vec<_>>(),
            entries
        );
    }
}
