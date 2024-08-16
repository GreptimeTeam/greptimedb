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

use object_store::ObjectStore;
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::RegionId;
use tokio::sync::Barrier;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::manifest::action::RegionEdit;
use crate::region::MitoRegionRef;
use crate::sst::file::{FileId, FileMeta};
use crate::test_util::{CreateRequestBuilder, TestEnv};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_edit_region_concurrently() {
    const EDITS_PER_TASK: usize = 10;
    let tasks_count = 10;

    // A task that creates SST files and edits the region with them.
    struct Task {
        region: MitoRegionRef,
        ssts: Vec<FileMeta>,
    }

    impl Task {
        async fn create_ssts(&mut self, object_store: &ObjectStore) {
            for _ in 0..EDITS_PER_TASK {
                let file = FileMeta {
                    region_id: self.region.region_id,
                    file_id: FileId::random(),
                    level: 0,
                    ..Default::default()
                };
                object_store
                    .write(
                        &format!("{}/{}.parquet", self.region.region_dir(), file.file_id),
                        b"x".as_slice(),
                    )
                    .await
                    .unwrap();
                self.ssts.push(file);
            }
        }

        async fn edit_region(self, engine: MitoEngine) {
            for sst in self.ssts {
                let edit = RegionEdit {
                    files_to_add: vec![sst],
                    files_to_remove: vec![],
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                };
                engine
                    .edit_region(self.region.region_id, edit)
                    .await
                    .unwrap();
            }
        }
    }

    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();

    let mut tasks = Vec::with_capacity(tasks_count);
    let object_store = env.get_object_store().unwrap();
    for _ in 0..tasks_count {
        let mut task = Task {
            region: region.clone(),
            ssts: Vec::new(),
        };
        task.create_ssts(&object_store).await;
        tasks.push(task);
    }

    let mut futures = Vec::with_capacity(tasks_count);
    let barrier = Arc::new(Barrier::new(tasks_count));
    for task in tasks {
        futures.push(tokio::spawn({
            let barrier = barrier.clone();
            let engine = engine.clone();
            async move {
                barrier.wait().await;
                task.edit_region(engine).await;
            }
        }));
    }
    futures::future::join_all(futures).await;

    assert_eq!(
        region.version().ssts.levels()[0].files.len(),
        tasks_count * EDITS_PER_TASK
    );
}
