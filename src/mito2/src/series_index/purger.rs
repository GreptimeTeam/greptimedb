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

//! Deferred deletion of series and range index files.

use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common_telemetry::warn;
use object_store::{ErrorKind, ObjectStore};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::metrics::SERIES_INDEX_FILE_OPERATION_TOTAL;
use crate::series_index::catalog::{range_index_path, series_index_path};
use crate::sst::file::RegionFileId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IndexFileType {
    Range,
    Series,
}

impl IndexFileType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::Series => "series",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PurgeRequest {
    pub(crate) file_id: RegionFileId,
    pub(crate) kind: IndexFileType,
}

impl PurgeRequest {
    pub(crate) fn path(self) -> String {
        match self.kind {
            IndexFileType::Range => {
                range_index_path(self.file_id.region_id(), self.file_id.file_id())
            }
            IndexFileType::Series => {
                series_index_path(self.file_id.region_id(), self.file_id.file_id())
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct IndexFilePurger {
    store: ObjectStore,
    sender: UnboundedSender<PurgeRequest>,
    pending: Arc<Mutex<HashSet<String>>>,
}

impl Debug for IndexFilePurger {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexFilePurger").finish_non_exhaustive()
    }
}

impl IndexFilePurger {
    /// Blocks reuse immediately, even while an old reader still owns the file.
    pub(crate) fn retire(&self, request: PurgeRequest) {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(request.path());
    }

    pub(crate) fn is_retired(&self, path: &str) -> bool {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(path)
    }

    pub(crate) fn purge(&self, request: PurgeRequest) {
        self.retire(request);
        if let Err(error) = self.sender.send(request) {
            let store = self.store.clone();
            let pending = self.pending.clone();
            common_runtime::spawn_compact(async move {
                while !purge_file(&store, error.0, &pending).await {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
        }
    }

    pub(crate) fn run(
        &self,
        receiver: UnboundedReceiver<PurgeRequest>,
    ) -> impl std::future::Future<Output = ()> + Send + use<> {
        run_purge_task(self.store.clone(), receiver, self.pending.clone())
    }

    pub(crate) fn start(store: ObjectStore) -> Self {
        let (purger, receiver) = series_index_channel(store.clone());
        common_runtime::spawn_compact(purger.run(receiver));
        purger
    }
}

pub(crate) fn file_operation(index_type: IndexFileType, operation: &str, result: &str) {
    SERIES_INDEX_FILE_OPERATION_TOTAL
        .with_label_values(&[index_type.as_str(), operation, result])
        .inc();
}

async fn run_purge_task(
    store: ObjectStore,
    mut receiver: UnboundedReceiver<PurgeRequest>,
    pending: Arc<Mutex<HashSet<String>>>,
) {
    let mut retries = Vec::new();
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            request = receiver.recv() => {
                let Some(request) = request else { break; };
                if !purge_file(&store, request, &pending).await { retries.push(request); }
            }
            _ = interval.tick(), if !retries.is_empty() => {
                let mut failed = Vec::new();
                for request in retries.drain(..) {
                    if !purge_file(&store, request, &pending).await { failed.push(request); }
                }
                retries = failed;
            }
        }
    }
    // Drain ready deletions on shutdown; startup recovery removes any remaining orphans.
    for request in retries {
        purge_file(&store, request, &pending).await;
    }
}

async fn purge_file(
    store: &ObjectStore,
    request: PurgeRequest,
    pending: &Mutex<HashSet<String>>,
) -> bool {
    let path = request.path();
    match store.delete(&path).await {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            file_operation(request.kind, "delete", "failure");
            warn!(error; "Failed to delete index, path: {path}");
            return false;
        }
    }
    pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&path);
    file_operation(request.kind, "delete", "success");
    true
}

pub(crate) fn series_index_channel(
    store: ObjectStore,
) -> (IndexFilePurger, UnboundedReceiver<PurgeRequest>) {
    let (sender, receiver) = unbounded_channel();
    (
        IndexFilePurger {
            store,
            sender,
            pending: Arc::default(),
        },
        receiver,
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use object_store::ObjectStore;
    use object_store::services::Memory;
    use store_api::storage::{FileId, RegionId};

    use super::*;

    #[tokio::test]
    async fn test_purge_drains_queue_after_last_sender_drops() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, receiver) = series_index_channel(store.clone());
        let mut paths = Vec::new();
        for _ in 0..3 {
            let file_id = RegionFileId::new(RegionId::new(1, 1), FileId::random());
            let path = series_index_path(file_id.region_id(), file_id.file_id());
            store.write(&path, "index").await.unwrap();
            paths.push(path);
            purger.purge(PurgeRequest {
                file_id,
                kind: IndexFileType::Series,
            });
        }
        let task = purger.run(receiver);
        drop(purger);
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap();
        for path in paths {
            assert!(!store.exists(&path).await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_purge_falls_back_after_receiver_drops() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, receiver) = series_index_channel(store.clone());
        drop(receiver);
        let file_id = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        let path = series_index_path(file_id.region_id(), file_id.file_id());
        store.write(&path, "index").await.unwrap();
        purger.purge(PurgeRequest {
            file_id,
            kind: IndexFileType::Series,
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.exists(&path).await.unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    struct FailingDeleter {
        inner: object_store::layers::mock::oio::Deleter,
        failing: Arc<std::sync::atomic::AtomicBool>,
        attempted: Arc<std::sync::atomic::AtomicBool>,
    }
    impl object_store::layers::mock::oio::Delete for FailingDeleter {
        async fn delete(
            &mut self,
            path: &str,
            args: object_store::layers::mock::OpDelete,
        ) -> object_store::layers::mock::Result<()> {
            self.attempted
                .store(true, std::sync::atomic::Ordering::Release);
            if self.failing.load(std::sync::atomic::Ordering::Acquire) {
                return Err(object_store::layers::mock::Error::new(
                    ErrorKind::Unexpected,
                    "injected delete failure",
                ));
            }
            self.inner.delete(path, args).await
        }
        async fn close(&mut self) -> object_store::layers::mock::Result<()> {
            self.inner.close().await
        }
    }

    #[tokio::test]
    async fn test_range_path_reuse_waits_for_readers_and_successful_delete() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use crate::series_index::catalog::IndexFileMetadata;
        use crate::series_index::version::IndexFileHandle;
        let failing = Arc::new(AtomicBool::new(true));
        let attempted = Arc::new(AtomicBool::new(false));
        let failed = failing.clone();
        let observed = attempted.clone();
        let layer = object_store::layers::mock::MockLayerBuilder::default()
            .deleter_factory(Arc::new(move |inner| {
                Box::new(FailingDeleter {
                    inner,
                    failing: failed.clone(),
                    attempted: observed.clone(),
                })
            }))
            .build()
            .unwrap();
        let store = ObjectStore::new(Memory::default()).unwrap().layer(layer);
        let purger = IndexFilePurger::start(store.clone());
        let region = RegionId::new(1, 1);
        let id = FileId::random();
        let path = range_index_path(region, id);
        store.write(&path, "old index").await.unwrap();
        let handle = IndexFileHandle::new(
            region,
            id,
            IndexFileType::Range,
            IndexFileMetadata {
                file_size: 9,
                min_timestamp: common_time::Timestamp::new_second(0),
            },
            purger.clone(),
        );
        let reader = handle.clone();
        handle.mark_deleted();
        drop(handle);
        assert!(purger.is_retired(&path));
        assert!(store.exists(&path).await.unwrap());
        assert!(!attempted.load(Ordering::Acquire));
        drop(reader);
        super::super::tests::wait_for(|| attempted.load(Ordering::Acquire)).await;
        assert!(purger.is_retired(&path));
        assert!(store.exists(&path).await.unwrap());
        failing.store(false, Ordering::Release);
        super::super::tests::wait_for(|| !purger.is_retired(&path)).await;
        assert!(!store.exists(&path).await.unwrap());
        store.write(&path, "replacement").await.unwrap();
        assert_eq!(
            b"replacement".as_slice(),
            store.read(&path).await.unwrap().to_bytes().as_ref()
        );
    }
}
