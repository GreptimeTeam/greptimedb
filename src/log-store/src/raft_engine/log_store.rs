// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_stream::{stream, try_stream};
use async_trait::async_trait;
use futures_util::StreamExt;
use raft_engine::{Engine, LogBatch, MessageExt};
use snafu::ResultExt;
use store_api::logstore::entry::Id;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::{AppendResponse, LogStore};

use crate::error::{AddEntryLogBatchSnafu, Error, ScanEntriesSnafu, WriteBatchSnafu};
use crate::raft_engine::protos::logstore::{Entry, Namespace};

const NAMESPACE_PREFIX: &str = "__sys_namespace_";

pub struct RaftEngineLogstore {
    engine: Arc<Engine>,
}

impl Debug for RaftEngineLogstore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RaftEngineLogstore")
    }
}

#[async_trait::async_trait]
impl LogStore for RaftEngineLogstore {
    type Error = Error;
    type Namespace = Namespace;
    type Entry = Entry;

    async fn start(&self) -> Result<(), Self::Error> {
        todo!()
    }

    async fn stop(&self) -> Result<(), Self::Error> {
        todo!()
    }

    async fn append(&self, e: Self::Entry) -> Result<AppendResponse, Self::Error> {
        let entry_id = e.id;
        let mut batch = LogBatch::with_capacity(1);
        batch
            .add_entries::<MessageType>(e.namespace_id, &[e])
            .context(AddEntryLogBatchSnafu)?;
        self.engine
            .write(&mut batch, false)
            .context(WriteBatchSnafu)?;
        Ok(AppendResponse { entry_id })
    }

    async fn append_batch(
        &self,
        ns: &Self::Namespace,
        entries: Vec<Self::Entry>,
    ) -> Result<Vec<Id>, Self::Error> {
        let entry_ids = entries.iter().map(Entry::get_id).collect::<Vec<_>>();
        let mut batch = LogBatch::with_capacity(entries.len());
        batch
            .add_entries::<MessageType>(ns.id, &entries)
            .context(AddEntryLogBatchSnafu)?;
        self.engine
            .write(&mut batch, false)
            .context(WriteBatchSnafu)?;
        Ok(entry_ids)
    }

    async fn read(
        &self,
        ns: &Self::Namespace,
        id: Id,
    ) -> Result<SendableEntryStream<'_, Self::Entry, Self::Error>, Self::Error> {
        let last_index = self.engine.last_index(ns.id).unwrap();

        let engine = self.engine.clone();
        let max_batch_size = 128;

        let (tx, mut rx) = tokio::sync::mpsc::channel(max_batch_size);
        let ns = ns.clone();
        common_runtime::spawn_read(async move {
            let mut start_idx = id;
            while start_idx < last_index {
                let mut vec = Vec::with_capacity(max_batch_size);
                match engine
                    .fetch_entries_to::<MessageType>(
                        ns.id,
                        start_idx,
                        last_index + 1,
                        Some(max_batch_size),
                        &mut vec,
                    )
                    .context(ScanEntriesSnafu)
                {
                    Ok(_) => {
                        if let Some(last_entry) = vec.last() {
                            start_idx = last_entry.id;
                        }
                        tx.send(Ok(vec));
                    }
                    Err(e) => {
                        tx.send(Err(e));
                        return;
                    }
                }
            }
        });

        let s = stream!({
            while let Some(res) = rx.recv().await {
                yield res;
            }
        });
        Ok(Box::pin(s))
    }

    async fn create_namespace(&mut self, ns: &Self::Namespace) -> Result<(), Self::Error> {
        todo!()
    }

    async fn delete_namespace(&mut self, ns: &Self::Namespace) -> Result<(), Self::Error> {
        todo!()
    }

    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error> {
        todo!()
    }

    fn entry<D: AsRef<[u8]>>(&self, data: D, id: Id, ns: Self::Namespace) -> Self::Entry {
        todo!()
    }

    fn namespace(&self, id: store_api::logstore::namespace::Id) -> Self::Namespace {
        todo!()
    }

    async fn obsolete(&self, namespace: Self::Namespace, id: Id) -> Result<(), Self::Error> {
        todo!()
    }
}

#[derive(Debug, Clone)]
struct MessageType;

impl MessageExt for MessageType {
    type Entry = Entry;

    fn index(e: &Self::Entry) -> u64 {
        e.id
    }
}
