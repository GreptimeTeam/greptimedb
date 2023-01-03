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
use std::time::Duration;

use async_stream::stream;
use common_telemetry::{error, info};
use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize, RecoveryMode};
use snafu::{OptionExt, ResultExt};
use store_api::logstore::entry::Id;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::Namespace as NamespaceTrait;
use store_api::logstore::{AppendResponse, LogStore};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{
    AddEntryLogBatchSnafu, Error, IllegalStateSnafu, RaftEngineSnafu, WaitGcTaskStopSnafu,
};
use crate::raft_engine::protos::logstore::{Entry, Namespace};

const NAMESPACE_PREFIX: &str = "__sys_namespace_";
const SYSTEM_NAMESPACE: u64 = 0;

pub struct RaftEngineLogstore {
    path: String,
    engine: RwLock<Option<Arc<Engine>>>,
    cancel_token: Mutex<Option<CancellationToken>>,
    gc_task_handle: Mutex<Option<JoinHandle<()>>>,
}

impl RaftEngineLogstore {
    pub fn new(path: impl AsRef<str>) -> Self {
        Self {
            path: path.as_ref().to_string(),
            engine: RwLock::new(None),
            cancel_token: Mutex::new(None),
            gc_task_handle: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub fn new_with_engine(engine: Arc<Engine>) -> Self {
        Self {
            path: "".to_string(),
            engine: RwLock::new(Some(engine.clone())),
            cancel_token: Mutex::new(None),
            gc_task_handle: Mutex::new(None),
        }
    }
}

impl Debug for RaftEngineLogstore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftEngineLogstore")
            .field("path", &self.path)
            .finish()
    }
}

#[async_trait::async_trait]
impl LogStore for RaftEngineLogstore {
    type Error = Error;
    type Namespace = Namespace;
    type Entry = Entry;

    async fn start(&self) -> Result<(), Self::Error> {
        let mut engine = self.engine.write().await;
        if engine.is_none() {
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
        }
        let engine_clone = engine.as_ref().unwrap().clone(); // Safety: engine's presence is checked above
        let interval = Duration::from_secs(60 * 10);
        let token = CancellationToken::new();
        let child = token.child_token();
        let handle = common_runtime::spawn_bg(async move {
            loop {
                if let Err(e) = engine_clone.purge_expired_files().context(RaftEngineSnafu) {
                    error!(e; "Failed to purge files in logstore");
                }
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = child.cancelled() => {
                        info!("LogStore gc task has been cancelled");
                        return;
                    }
                }
            }
        });
        *self.cancel_token.lock().await = Some(token);
        *self.gc_task_handle.lock().await = Some(handle);
        Ok(())
    }

    async fn stop(&self) -> Result<(), Self::Error> {
        let handle = self
            .gc_task_handle
            .lock()
            .await
            .take()
            .context(IllegalStateSnafu)?;
        let token = self
            .cancel_token
            .lock()
            .await
            .take()
            .context(IllegalStateSnafu)?;
        token.cancel();
        handle.await.context(WaitGcTaskStopSnafu)?;
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
            loop {
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
                            start_idx = last_entry.id + 1;
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

                if start_idx >= last_index {
                    break;
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use futures_util::StreamExt;
    use raft_engine::{Config, Engine, ReadableSize};
    use store_api::logstore::namespace::Namespace as NamespaceTrait;
    use store_api::logstore::LogStore;
    use tempdir::TempDir;

    use crate::raft_engine::log_store::RaftEngineLogstore;
    use crate::raft_engine::protos::logstore::{Entry, Namespace};

    #[tokio::test]
    async fn test_open_logstore() {
        let dir = TempDir::new("raft-engine-logstore-test").unwrap();
        let logstore = RaftEngineLogstore::new(dir.path().to_str().unwrap());
        logstore.start().await.unwrap();
        let namespaces = logstore.list_namespaces().await.unwrap();
        assert_eq!(0, namespaces.len());
    }

    #[tokio::test]
    async fn test_manage_namespace() {
        let dir = TempDir::new("raft-engine-logstore-test").unwrap();
        let mut logstore = RaftEngineLogstore::new(dir.path().to_str().unwrap());
        logstore.start().await.unwrap();
        assert!(logstore.list_namespaces().await.unwrap().is_empty());

        logstore
            .create_namespace(&Namespace::with_id(42))
            .await
            .unwrap();
        let namespaces = logstore.list_namespaces().await.unwrap();
        assert_eq!(1, namespaces.len());
        assert_eq!(Namespace::with_id(42), namespaces[0]);

        logstore
            .delete_namespace(&Namespace::with_id(42))
            .await
            .unwrap();
        assert!(logstore.list_namespaces().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_create_namespace_and_reopen_logstore() {
        let dir = TempDir::new("raft-engine-logstore-test").unwrap();
        {
            let mut logstore = RaftEngineLogstore::new(dir.path().to_str().unwrap());
            logstore.start().await.unwrap();
            assert!(logstore.list_namespaces().await.unwrap().is_empty());

            logstore
                .create_namespace(&Namespace::with_id(42))
                .await
                .unwrap();
            let namespaces = logstore.list_namespaces().await.unwrap();
            assert_eq!(1, namespaces.len());
            assert_eq!(Namespace::with_id(42), namespaces[0]);
        }
        let logstore = RaftEngineLogstore::new(dir.path().to_str().unwrap());
        logstore.start().await.unwrap();
        assert_eq!(
            &Namespace::with_id(42),
            logstore.list_namespaces().await.unwrap().first().unwrap()
        );
    }

    #[tokio::test]
    async fn test_append_and_read() {
        let dir = TempDir::new("raft-engine-logstore-test").unwrap();
        let logstore = RaftEngineLogstore::new(dir.path().to_str().unwrap());
        logstore.start().await.unwrap();

        let namespace = Namespace::with_id(1);
        let cnt = 1024;
        for i in 0..cnt {
            let response = logstore
                .append(Entry::create(
                    i,
                    namespace.id,
                    i.to_string().as_bytes().to_vec(),
                ))
                .await
                .unwrap();
            assert_eq!(i, response.entry_id);
        }
        let mut entries = HashSet::with_capacity(1024);
        let mut s = logstore.read(&Namespace::with_id(1), 0).await.unwrap();
        while let Some(r) = s.next().await {
            let vec = r.unwrap();
            entries.extend(vec.into_iter().map(|e| e.id));
        }
        assert_eq!(cnt as usize, entries.len());
    }

    async fn wal_dir_usage(path: impl AsRef<str>) -> usize {
        let mut size: usize = 0;
        let mut read_dir = tokio::fs::read_dir(path.as_ref()).await.unwrap();
        while let Ok(dir_entry) = read_dir.next_entry().await {
            let Some(entry) = dir_entry else {
                break;
            };
            if entry.file_type().await.unwrap().is_file() {
                size += entry.metadata().await.unwrap().len() as usize;
            }
        }
        size
    }

    #[tokio::test]
    async fn test_compaction() {
        let dir = TempDir::new("raft-engine-logstore-test").unwrap();
        let engine = Arc::new(
            Engine::open(Config {
                dir: dir.path().to_str().unwrap().to_string(),
                target_file_size: ReadableSize::mb(2),
                purge_threshold: ReadableSize::mb(4),
                ..Default::default()
            })
            .unwrap(),
        );
        let logstore = RaftEngineLogstore::new_with_engine(engine.clone());
        let namespace = Namespace::with_id(42);

        for id in 0..4096 {
            let entry = Entry::create(id, namespace.id(), [b'x'; 4096].to_vec());
            logstore.append(entry).await.unwrap();
        }

        println!(
            "usage: {:?}\n",
            wal_dir_usage(dir.path().to_str().unwrap()).await
        );
        logstore.obsolete(namespace, 4000).await.unwrap();
        engine.purge_expired_files().unwrap();

        println!(
            "usage: {:?}\n",
            wal_dir_usage(dir.path().to_str().unwrap()).await
        );
    }
}
