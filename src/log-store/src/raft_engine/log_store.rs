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

use async_stream::stream;
use common_telemetry::info;
use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize, RecoveryMode};
use snafu::{OptionExt, ResultExt};
use store_api::logstore::entry::Id;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::Namespace as NamespaceTrait;
use store_api::logstore::{AppendResponse, LogStore};
use tokio::sync::RwLock;

use crate::error::{AddEntryLogBatchSnafu, Error, IllegalStateSnafu, RaftEngineSnafu};
use crate::raft_engine::protos::logstore::{Entry, Namespace};

const NAMESPACE_PREFIX: &str = "__sys_namespace_";
const SYSTEM_NAMESPACE: u64 = 0;

pub struct RaftEngineLogstore {
    path: String,
    engine: RwLock<Option<Arc<Engine>>>,
}

impl RaftEngineLogstore {
    pub fn new(path: impl AsRef<str>) -> Self {
        Self {
            path: path.as_ref().to_string(),
            engine: RwLock::new(None),
        }
    }
}

impl Debug for RaftEngineLogstore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RaftEngineLogstore {{ path: {} }}", self.path)
    }
}

#[async_trait::async_trait]
impl LogStore for RaftEngineLogstore {
    type Error = Error;
    type Namespace = Namespace;
    type Entry = Entry;

    async fn start(&self) -> Result<(), Self::Error> {
        let mut engine = self.engine.write().await;
        let config = Config {
            dir: self.path.clone(),
            purge_threshold: ReadableSize::gb(50), // TODO(hl): set according to available disk space
            recovery_mode: RecoveryMode::TolerateTailCorruption,
            batch_compression_threshold: ReadableSize::kb(8),
            target_file_size: ReadableSize::gb(1),
            ..Default::default()
        };
        *engine = Some(Arc::new(
            Engine::open(config.clone()).context(RaftEngineSnafu)?,
        ));
        info!("RaftEngineLogstore started with config: {config:?}");
        Ok(())
    }

    async fn stop(&self) -> Result<(), Self::Error> {
        let mut engine = self.engine.write().await;
        *engine = None;
        info!("RaftEngineLogstore stopped");
        Ok(())
    }

    async fn append(&self, e: Self::Entry) -> Result<AppendResponse, Self::Error> {
        let entry_id = e.id;
        let mut batch = LogBatch::with_capacity(1);
        batch
            .add_entries::<MessageType>(e.namespace_id, &[e])
            .context(AddEntryLogBatchSnafu)?;
        let guard = self.engine.read().await;
        let engine = guard.as_ref().context(IllegalStateSnafu)?;
        engine.write(&mut batch, false).context(RaftEngineSnafu)?;
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
            .read()
            .await
            .as_ref()
            .context(IllegalStateSnafu)?
            .write(&mut batch, false)
            .context(RaftEngineSnafu)?;
        Ok(entry_ids)
    }

    async fn read(
        &self,
        ns: &Self::Namespace,
        id: Id,
    ) -> Result<SendableEntryStream<'_, Self::Entry, Self::Error>, Self::Error> {
        let engine = self
            .engine
            .read()
            .await
            .as_ref()
            .context(IllegalStateSnafu)?
            .clone();
        let last_index = engine.last_index(ns.id).unwrap();

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
                    .context(RaftEngineSnafu)
                {
                    Ok(_) => {
                        if let Some(last_entry) = vec.last() {
                            start_idx = last_entry.id;
                        }
                        // reader side closed, cancel following reads
                        if let Err(_) = tx.send(Ok(vec)).await {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
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
        let key = format!("{}{}", NAMESPACE_PREFIX, ns.id).as_bytes().to_vec();
        let mut batch = LogBatch::with_capacity(1);
        batch
            .put_message::<Namespace>(SYSTEM_NAMESPACE, key, ns)
            .context(RaftEngineSnafu)?;
        self.engine
            .read()
            .await
            .as_ref()
            .context(IllegalStateSnafu)?
            .write(&mut batch, true)
            .context(RaftEngineSnafu)?;
        Ok(())
    }

    async fn delete_namespace(&mut self, ns: &Self::Namespace) -> Result<(), Self::Error> {
        let key = format!("{}{}", NAMESPACE_PREFIX, ns.id).as_bytes().to_vec();
        let mut batch = LogBatch::with_capacity(1);
        batch.delete(SYSTEM_NAMESPACE, key);
        self.engine
            .read()
            .await
            .as_ref()
            .context(IllegalStateSnafu)?
            .write(&mut batch, true)
            .context(RaftEngineSnafu)?;
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error> {
        let mut namespaces: Vec<Namespace> = vec![];
        self.engine
            .read()
            .await
            .as_ref()
            .context(IllegalStateSnafu)?
            .scan_messages::<Namespace, _>(
                SYSTEM_NAMESPACE,
                Some(NAMESPACE_PREFIX.as_bytes()),
                None,
                false,
                |_k, v| {
                    namespaces.push(v);
                    true
                },
            )
            .context(RaftEngineSnafu)?;
        Ok(namespaces)
    }

    fn entry<D: AsRef<[u8]>>(&self, data: D, id: Id, ns: Self::Namespace) -> Self::Entry {
        Entry {
            id,
            data: data.as_ref().to_vec(),
            namespace_id: ns.id(),
            ..Default::default()
        }
    }

    fn namespace(&self, id: store_api::logstore::namespace::Id) -> Self::Namespace {
        Namespace {
            id,
            ..Default::default()
        }
    }

    async fn obsolete(&self, namespace: Self::Namespace, id: Id) -> Result<(), Self::Error> {
        self.engine
            .read()
            .await
            .as_ref()
            .context(IllegalStateSnafu)?
            .compact_to(namespace.id(), id);
        Ok(())
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
