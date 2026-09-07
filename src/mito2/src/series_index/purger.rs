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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common_telemetry::{info, warn};
use object_store::{ErrorKind, ObjectStore};
use tokio::sync::{Notify, mpsc};

use super::catalog::series_index_path;
use crate::metrics::SERIES_INDEX_FILE_OPERATION_TOTAL;
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
    sender: mpsc::UnboundedSender<PurgeRequest>,
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
                let _ = purge_file(&store, error.0).await;
            });
        }
    }
}

pub(crate) fn file_operation(index_type: IndexFileType, operation: &str, result: &str) {
    SERIES_INDEX_FILE_OPERATION_TOTAL
        .with_label_values(&[index_type.as_str(), operation, result])
        .inc();
}

pub(crate) async fn purge_file(store: &ObjectStore, request: PurgeRequest) -> bool {
    let path = series_index_path(request.file_id.region_id(), request.file_id.file_id());
    match store.delete(&path).await {
        Ok(()) => {
            file_operation(IndexFileType::Series, "delete", "success");
            true
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            file_operation(IndexFileType::Series, "delete", "success");
            true
        }
        Err(error) => {
            file_operation(IndexFileType::Series, "delete", "failure");
            warn!(error; "Failed to delete series index, index_type: {}, path: {}, phase: deletion, retry: true", IndexFileType::Series.as_str(), path);
            false
        }
    }
}

pub(crate) fn series_index_channel(
    store: ObjectStore,
) -> (IndexFilePurger, mpsc::UnboundedReceiver<PurgeRequest>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (IndexFilePurger { store, sender }, receiver)
}

/// Shared lifecycle state for a worker's series-index task.
#[derive(Debug)]
pub(crate) struct SeriesIndexTaskState {
    running: AtomicBool,
    notify: Notify,
}

impl SeriesIndexTaskState {
    pub(crate) fn new() -> Self {
        Self {
            running: AtomicBool::new(true),
            notify: Notify::new(),
        }
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn stop(&self) {
        self.running.store(false, Ordering::Release);
        // notify_one() retains a permit if the task has not started waiting yet.
        self.notify.notify_one();
    }

    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }
}

/// Consumes deferred series-file deletions and retries failures until the worker stops.
pub(crate) async fn run_index_purge_task(
    worker_id: u32,
    store: ObjectStore,
    state: Arc<SeriesIndexTaskState>,
    mut purge_receiver: mpsc::UnboundedReceiver<PurgeRequest>,
) {
    info!("Start series-index purge task, worker: {worker_id}");
    let mut retry_purges = Vec::new();
    while state.is_running() {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5 * 60)) => {}
            _ = state.notified() => {}
            Some(request) = purge_receiver.recv() => {
                if !purge_file(&store, request).await {
                    retry_purges.push(request);
                }
                continue;
            }
        }
        if !state.is_running() {
            break;
        }
        for request in std::mem::take(&mut retry_purges) {
            if !purge_file(&store, request).await {
                retry_purges.push(request);
            }
        }
    }
    while let Ok(request) = purge_receiver.try_recv() {
        retry_purges.push(request);
    }
    for request in retry_purges {
        let _ = purge_file(&store, request).await;
    }
    info!("Stop series-index purge task, worker: {worker_id}");
}
