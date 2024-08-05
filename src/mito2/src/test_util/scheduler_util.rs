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

//! Utilities to mock flush and compaction schedulers.

use std::sync::{Arc, Mutex};

use common_base::Plugins;
use common_datasource::compression::CompressionType;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use object_store::services::Fs;
use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;
use tokio::sync::mpsc::Sender;

use crate::access_layer::{AccessLayer, AccessLayerRef};
use crate::cache::CacheManager;
use crate::compaction::CompactionScheduler;
use crate::config::MitoConfig;
use crate::error::Result;
use crate::flush::FlushScheduler;
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::region::{ManifestContext, ManifestContextRef, RegionState};
use crate::request::WorkerRequest;
use crate::schedule::scheduler::{Job, LocalScheduler, Scheduler, SchedulerRef};
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::worker::WorkerListener;

/// Scheduler mocker.
pub(crate) struct SchedulerEnv {
    #[allow(unused)]
    path: TempDir,
    /// Mock access layer for test.
    pub(crate) access_layer: AccessLayerRef,
    scheduler: Option<SchedulerRef>,
}

impl SchedulerEnv {
    /// Creates a new mocker.
    pub(crate) async fn new() -> SchedulerEnv {
        let path = create_temp_dir("");
        let path_str = path.path().display().to_string();
        let builder = Fs::default().root(&path_str);

        let index_aux_path = path.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();
        let object_store = ObjectStore::new(builder).unwrap().finish();
        let access_layer = Arc::new(AccessLayer::new(
            "",
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        SchedulerEnv {
            path,
            access_layer,
            scheduler: None,
        }
    }

    /// Set scheduler.
    pub(crate) fn scheduler(mut self, scheduler: SchedulerRef) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    /// Creates a new compaction scheduler.
    pub(crate) fn mock_compaction_scheduler(
        &self,
        request_sender: Sender<WorkerRequest>,
    ) -> CompactionScheduler {
        let scheduler = self.get_scheduler();

        CompactionScheduler::new(
            scheduler,
            request_sender,
            Arc::new(CacheManager::default()),
            Arc::new(MitoConfig::default()),
            WorkerListener::default(),
            Plugins::new(),
        )
    }

    /// Creates a new flush scheduler.
    pub(crate) fn mock_flush_scheduler(&self) -> FlushScheduler {
        let scheduler = self.get_scheduler();

        FlushScheduler::new(scheduler)
    }

    /// Creates a new manifest context.
    pub(crate) async fn mock_manifest_context(
        &self,
        metadata: RegionMetadataRef,
    ) -> ManifestContextRef {
        Arc::new(ManifestContext::new(
            RegionManifestManager::new(
                metadata,
                RegionManifestOptions {
                    manifest_dir: "".to_string(),
                    object_store: self.access_layer.object_store().clone(),
                    compress_type: CompressionType::Uncompressed,
                    checkpoint_distance: 10,
                },
                Default::default(),
            )
            .await
            .unwrap(),
            RegionState::Writable,
        ))
    }

    fn get_scheduler(&self) -> SchedulerRef {
        self.scheduler
            .clone()
            .unwrap_or_else(|| Arc::new(LocalScheduler::new(1)))
    }
}

#[derive(Default)]
pub struct VecScheduler {
    jobs: Mutex<Vec<Job>>,
}

impl VecScheduler {
    pub fn num_jobs(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }
}

#[async_trait::async_trait]
impl Scheduler for VecScheduler {
    fn schedule(&self, job: Job) -> Result<()> {
        self.jobs.lock().unwrap().push(job);
        Ok(())
    }

    async fn stop(&self, _await_termination: bool) -> Result<()> {
        Ok(())
    }
}
