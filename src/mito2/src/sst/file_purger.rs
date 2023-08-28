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

use common_telemetry::{error, info};
use store_api::storage::RegionId;

use crate::access_layer::AccessLayerRef;
use crate::schedule::scheduler::{LocalScheduler, Scheduler};
use crate::sst::file::FileId;

/// Request to remove a file.
#[derive(Debug)]
pub struct PurgeRequest {
    /// Region id of the file.
    pub region_id: RegionId,
    /// Id of the file.
    pub file_id: FileId,
}

/// A worker to delete files in background.
pub trait FilePurger: Send + Sync {
    /// Send a purge request to the background worker.
    fn send_request(&self, request: PurgeRequest);
}

pub type FilePurgerRef = Arc<dyn FilePurger>;

pub struct LocalFilePurger {
    scheduler: Arc<LocalScheduler>,

    sst_layer: AccessLayerRef,
}

impl LocalFilePurger {
    pub fn new(scheduler: Arc<LocalScheduler>, sst_layer: AccessLayerRef) -> Self {
        Self {
            scheduler,
            sst_layer,
        }
    }
}

impl FilePurger for LocalFilePurger {
    fn send_request(&self, request: PurgeRequest) {
        let file_id = request.file_id;
        let region_id = request.region_id;
        let sst_layer = self.sst_layer.clone();

        if let Err(e) = self.scheduler.schedule(Box::pin(async move {
            if let Err(e) = sst_layer.delete_sst(file_id).await {
                error!(e; "Failed to delete SST file, file: {}, region: {}", 
                    file_id.as_parquet(), region_id);
            } else {
                info!(
                    "Successfully deleted SST file: {}, region: {}",
                    file_id.as_parquet(),
                    region_id
                );
            }
        })) {
            error!(e; "Failed to schedule the file purge request");
        }
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;
    use object_store::{util, ObjectStore};

    use super::*;
    use crate::access_layer::AccessLayer;
    use crate::schedule::scheduler::LocalScheduler;
    use crate::sst::file::{FileHandle, FileId, FileMeta, FileTimeRange};

    #[tokio::test]
    async fn test_file_purge() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("file-purge");
        let mut builder = Fs::default();
        let _ = builder.root(dir.path().to_str().unwrap());
        let object_store = ObjectStore::new(builder).unwrap().finish();
        let sst_file_id = FileId::random();
        let sst_dir = "table1";
        let path = util::join_path(sst_dir, &sst_file_id.as_parquet());

        object_store.write(&path, vec![0; 4096]).await.unwrap();

        let scheduler = Arc::new(LocalScheduler::new(3));
        let layer = Arc::new(AccessLayer::new(sst_dir, object_store.clone()));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler.clone(), layer));

        {
            let handle = FileHandle::new(
                FileMeta {
                    region_id: 0.into(),
                    file_id: sst_file_id,
                    time_range: FileTimeRange::default(),
                    level: 0,
                    file_size: 4096,
                },
                file_purger,
            );
            // mark file as deleted and drop the handle, we expect the file is deleted.
            handle.mark_deleted();
        }

        scheduler.stop(true).await.unwrap();

        assert!(!object_store
            .is_exist(&format!("{}/{}", sst_dir, sst_file_id.as_parquet()))
            .await
            .unwrap());
    }
}
