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
use store_api::logstore::entry::{Entry, RawEntry};
use store_api::logstore::namespace::{KafkaNamespace, LogStoreNamespace, RaftEngineNamespace};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio_stream::StreamExt;

use crate::error::{self, Result};
use crate::wal::EntryId;

/// A stream that yields [RawEntry].
pub type RawEntryStream<'a> = BoxStream<'a, Result<RawEntry>>;

/// [RawEntryReader] provides the ability to read [RawEntry] from the underlying [LogStore].
pub(crate) trait RawEntryReader: Send + Sync {
    fn read(&self, ns: &LogStoreNamespace, start_id: EntryId) -> Result<RawEntryStream<'static>>;
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
        ns: RaftEngineNamespace,
        start_id: EntryId,
    ) -> Result<RawEntryStream<'static>> {
        let region_id = RegionId::from_u64(ns.id);
        let store = self.store.clone();

        let stream = try_stream!({
            let mut stream = store
                .read(&LogStoreNamespace::RaftEngine(ns), start_id)
                .await
                .map_err(BoxedError::new)
                .context(error::ReadWalSnafu { region_id })?;

            while let Some(entries) = stream.next().await {
                let entries = entries
                    .map_err(BoxedError::new)
                    .context(error::ReadWalSnafu { region_id })?;

                for entry in entries {
                    yield entry.into_raw_entry()
                }
            }
        });

        Ok(Box::pin(stream))
    }

    fn read_topic(
        &self,
        ns: Arc<KafkaNamespace>,
        start_id: EntryId,
    ) -> Result<RawEntryStream<'static>> {
        let store = self.store.clone();
        let stream = try_stream!({
            let mut stream = store
                .read(&LogStoreNamespace::Kafka(ns.clone()), start_id)
                .await
                .map_err(BoxedError::new)
                .context(error::ReadKafkaWalSnafu { topic: &ns.topic })?;
            while let Some(entries) = stream.next().await {
                let entries = entries
                    .map_err(BoxedError::new)
                    .context(error::ReadKafkaWalSnafu { topic: &ns.topic })?;

                for entry in entries {
                    yield entry.into_raw_entry()
                }
            }
        });

        Ok(Box::pin(stream))
    }
}

impl<S: LogStore> RawEntryReader for LogStoreRawEntryReader<S> {
    fn read(&self, ctx: &LogStoreNamespace, start_id: EntryId) -> Result<RawEntryStream<'static>> {
        let stream = match ctx {
            LogStoreNamespace::RaftEngine(ns) => self.read_region(*ns, start_id)?,
            LogStoreNamespace::Kafka(ns) => self.read_topic(ns.clone(), start_id)?,
        };

        Ok(Box::pin(stream))
    }
}

/// A filter implement the [RawEntryReader]
pub struct RawEntryReaderFilter<R, F> {
    reader: R,
    filter: Arc<F>,
}

impl<R, F> RawEntryReaderFilter<R, F>
where
    R: RawEntryReader,
    F: Fn(&RawEntry) -> bool + Sync + Send,
{
    pub fn new(reader: R, filter: F) -> Self {
        Self {
            reader,
            filter: Arc::new(filter),
        }
    }
}

impl<R, F> RawEntryReader for RawEntryReaderFilter<R, F>
where
    R: RawEntryReader,
    F: Fn(&RawEntry) -> bool + Sync + Send + 'static,
{
    fn read(&self, ctx: &LogStoreNamespace, start_id: EntryId) -> Result<RawEntryStream<'static>> {
        let mut stream = self.reader.read(ctx, start_id)?;
        let filter = self.filter.clone();

        let stream = try_stream!({
            while let Some(entry) = stream.next().await {
                let entry = entry?;
                if filter(&entry) {
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
    use store_api::logstore::entry::{Entry, RawEntry};
    use store_api::logstore::entry_stream::SendableEntryStream;
    use store_api::logstore::namespace::Namespace;
    use store_api::logstore::{
        AppendBatchResponse, AppendResponse, EntryId, LogStore, NamespaceId,
    };
    use store_api::storage::RegionId;

    use super::*;
    use crate::error;

    #[derive(Debug)]
    struct MockLogStore {
        entries: Vec<RawEntry>,
    }

    #[derive(Debug, Eq, PartialEq, Clone, Copy, Default, Hash)]
    struct MockNamespace;

    impl Namespace for MockNamespace {
        fn id(&self) -> NamespaceId {
            0
        }
    }

    #[async_trait::async_trait]
    impl LogStore for MockLogStore {
        type Entry = RawEntry;
        type Error = error::Error;

        async fn stop(&self) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn append(&self, entry: Self::Entry) -> Result<AppendResponse, Self::Error> {
            unreachable!()
        }

        async fn append_batch(
            &self,
            entries: Vec<Self::Entry>,
        ) -> Result<AppendBatchResponse, Self::Error> {
            unreachable!()
        }

        async fn read(
            &self,
            ns: &LogStoreNamespace,
            id: EntryId,
        ) -> Result<SendableEntryStream<'static, Self::Entry, Self::Error>, Self::Error> {
            Ok(Box::pin(stream::iter(vec![Ok(self.entries.clone())])))
        }

        async fn create_namespace(&self, ns: &LogStoreNamespace) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn delete_namespace(&self, ns: &LogStoreNamespace) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn list_namespaces(&self) -> Result<Vec<LogStoreNamespace>, Self::Error> {
            unreachable!()
        }

        async fn obsolete(
            &self,
            ns: &LogStoreNamespace,
            entry_id: EntryId,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn entry(
            &self,
            data: &mut Vec<u8>,
            entry_id: EntryId,
            region_id: RegionId,
            ns: &LogStoreNamespace,
        ) -> Self::Entry {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_raw_entry_reader() {
        let expected_entries = vec![RawEntry {
            region_id: RegionId::new(1024, 1),
            entry_id: 1,
            data: vec![],
        }];
        let store = MockLogStore {
            entries: expected_entries.clone(),
        };

        let reader = LogStoreRawEntryReader::new(Arc::new(store));
        let entries = reader
            .read(
                &LogStoreNamespace::raft_engine_namespace(RegionId::new(1024, 1).as_u64()),
                0,
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(expected_entries, entries);
    }

    #[tokio::test]
    async fn test_raw_entry_reader_filter() {
        let all_entries = vec![
            RawEntry {
                region_id: RegionId::new(1024, 1),
                entry_id: 1,
                data: vec![1],
            },
            RawEntry {
                region_id: RegionId::new(1024, 2),
                entry_id: 2,
                data: vec![2],
            },
            RawEntry {
                region_id: RegionId::new(1024, 3),
                entry_id: 3,
                data: vec![3],
            },
        ];
        let store = MockLogStore {
            entries: all_entries.clone(),
        };

        let expected_region_id = RegionId::new(1024, 3);
        let reader =
            RawEntryReaderFilter::new(LogStoreRawEntryReader::new(Arc::new(store)), move |entry| {
                entry.region_id == expected_region_id
            });
        let entries = reader
            .read(
                &LogStoreNamespace::raft_engine_namespace(RegionId::new(1024, 1).as_u64()),
                0,
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            all_entries
                .into_iter()
                .filter(|entry| entry.region_id == expected_region_id)
                .collect::<Vec<_>>(),
            entries
        );
    }
}
