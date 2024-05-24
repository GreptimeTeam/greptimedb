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
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio_stream::StreamExt;

use crate::error::{self, Result};
use crate::wal::EntryId;

/// A stream that yields [RawEntry].
pub type RawEntryStream<'a> = BoxStream<'a, Result<RawEntry>>;

// The namespace of kafka log store
pub struct KafkaNamespace<'a> {
    topic: &'a str,
}

// The namespace of raft engine log store
pub struct RaftEngineNamespace {
    region_id: RegionId,
}

/// The namespace of [RawEntryReader].
pub(crate) enum LogStoreNamespace<'a> {
    RaftEngine(RaftEngineNamespace),
    Kafka(KafkaNamespace<'a>),
}

/// [RawEntryReader] provides the ability to read [RawEntry] from the underlying [LogStore].
pub(crate) trait RawEntryReader: Send + Sync {
    fn read<'a>(
        &'a self,
        ctx: LogStoreNamespace<'a>,
        start_id: EntryId,
    ) -> Result<RawEntryStream<'a>>;
}

/// Implement the [RawEntryReader] for the [LogStore].
pub struct LogStoreRawEntryReader<S> {
    store: Arc<S>,
}

impl<S: LogStore> LogStoreRawEntryReader<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    fn read_region(&self, ns: RaftEngineNamespace, start_id: EntryId) -> Result<RawEntryStream> {
        let region_id = ns.region_id;
        let stream = try_stream!({
            // TODO(weny): refactor the `namespace` method.
            let namespace = self.store.namespace(region_id.into(), &Default::default());
            let mut stream = self
                .store
                .read(&namespace, start_id)
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

    fn read_topic<'a>(
        &'a self,
        ns: KafkaNamespace<'a>,
        start_id: EntryId,
    ) -> Result<RawEntryStream> {
        let topic = ns.topic;
        let stream = try_stream!({
            // TODO(weny): refactor the `namespace` method.
            let namespace = self.store.namespace(
                RegionId::from_u64(0).into(),
                &WalOptions::Kafka(KafkaWalOptions {
                    topic: topic.to_string(),
                }),
            );

            let mut stream = self
                .store
                .read(&namespace, start_id)
                .await
                .map_err(BoxedError::new)
                .context(error::ReadKafkaWalSnafu { topic })?;

            while let Some(entries) = stream.next().await {
                let entries = entries
                    .map_err(BoxedError::new)
                    .context(error::ReadKafkaWalSnafu { topic })?;

                for entry in entries {
                    yield entry.into_raw_entry()
                }
            }
        });

        Ok(Box::pin(stream))
    }
}

impl<S: LogStore> RawEntryReader for LogStoreRawEntryReader<S> {
    fn read<'a>(
        &'a self,
        ctx: LogStoreNamespace<'a>,
        start_id: EntryId,
    ) -> Result<RawEntryStream<'a>> {
        let stream = match ctx {
            LogStoreNamespace::RaftEngine(ns) => self.read_region(ns, start_id)?,
            LogStoreNamespace::Kafka(ns) => self.read_topic(ns, start_id)?,
        };

        Ok(Box::pin(stream))
    }
}

/// A filter implement the [RawEntryReader]
pub struct RawEntryReaderFilter<R, F> {
    reader: R,
    filter: F,
}

impl<R, F> RawEntryReader for RawEntryReaderFilter<R, F>
where
    R: RawEntryReader,
    F: Fn(&RawEntry) -> bool + Sync + Send,
{
    fn read<'a>(
        &'a self,
        ctx: LogStoreNamespace<'a>,
        start_id: EntryId,
    ) -> Result<RawEntryStream<'a>> {
        let mut stream = self.reader.read(ctx, start_id)?;
        let filter = &(self.filter);
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
