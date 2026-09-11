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

//! Deferred deletion of aggregate series-index files.

use std::fmt::{self, Debug, Formatter};

use common_telemetry::{info, warn};
use object_store::{ErrorKind, ObjectStore};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::metrics::SERIES_INDEX_FILE_OPERATION_TOTAL;
use crate::series_index::catalog::series_index_path;
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
}

#[derive(Clone)]
pub(crate) struct IndexFilePurger {
    store: ObjectStore,
    sender: UnboundedSender<PurgeRequest>,
}

impl Debug for IndexFilePurger {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexFilePurger").finish_non_exhaustive()
    }
}

impl IndexFilePurger {
    pub(crate) fn purge(&self, request: PurgeRequest) {
        if let Err(error) = self.sender.send(request) {
            let store = self.store.clone();
            common_runtime::spawn_global(async move {
                purge_file(&store, error.0).await;
            });
        }
    }
}

pub(crate) fn file_operation(index_type: IndexFileType, operation: &str, result: &str) {
    SERIES_INDEX_FILE_OPERATION_TOTAL
        .with_label_values(&[index_type.as_str(), operation, result])
        .inc();
}

/// Processes queued deletions once each, independently of periodic maintenance.
pub(crate) async fn run_index_purge_task(
    worker_id: u32,
    store: ObjectStore,
    mut receiver: UnboundedReceiver<PurgeRequest>,
) {
    info!("Start series-index purge task, worker: {worker_id}");
    while let Some(request) = receiver.recv().await {
        purge_file(&store, request).await;
    }
    info!("Stop series-index purge task, worker: {worker_id}");
}

async fn purge_file(store: &ObjectStore, request: PurgeRequest) {
    let path = series_index_path(request.file_id.region_id(), request.file_id.file_id());
    match store.delete(&path).await {
        Ok(()) => {
            file_operation(IndexFileType::Series, "delete", "success");
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            file_operation(IndexFileType::Series, "delete", "success");
        }
        Err(error) => {
            file_operation(IndexFileType::Series, "delete", "failure");
            warn!(error; "Failed to delete series index, index_type: {}, path: {}, phase: deletion", IndexFileType::Series.as_str(), path);
        }
    }
}

pub(crate) fn series_index_channel(
    store: ObjectStore,
) -> (IndexFilePurger, UnboundedReceiver<PurgeRequest>) {
    let (sender, receiver) = unbounded_channel();
    (IndexFilePurger { store, sender }, receiver)
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
            purger.purge(PurgeRequest { file_id });
        }
        drop(purger);
        tokio::time::timeout(
            Duration::from_secs(10),
            run_index_purge_task(0, store.clone(), receiver),
        )
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
        purger.purge(PurgeRequest { file_id });
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.exists(&path).await.unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
