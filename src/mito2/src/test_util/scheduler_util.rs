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

use std::sync::Arc;

use common_test_util::temp_dir::{create_temp_dir, TempDir};
use object_store::services::Fs;
use object_store::ObjectStore;
use tokio::sync::mpsc::Sender;

use crate::access_layer::{AccessLayer, AccessLayerRef};
use crate::cache::CacheManager;
use crate::compaction::CompactionScheduler;
use crate::flush::FlushScheduler;
use crate::request::WorkerRequest;
use crate::schedule::scheduler::{LocalScheduler, SchedulerRef};

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
    pub(crate) fn new() -> SchedulerEnv {
        let path = create_temp_dir("");
        let mut builder = Fs::default();
        builder.root(path.path().to_str().unwrap());
        let object_store = ObjectStore::new(builder).unwrap().finish();
        let access_layer = Arc::new(AccessLayer::new("", object_store.clone()));

        SchedulerEnv {
            path: create_temp_dir(""),
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
            Arc::new(CacheManager::new(0, 0, 0)),
        )
    }

    /// Creates a new flush scheduler.
    pub(crate) fn mock_flush_scheduler(&self) -> FlushScheduler {
        let scheduler = self.get_scheduler();

        FlushScheduler::new(scheduler)
    }

    fn get_scheduler(&self) -> SchedulerRef {
        self.scheduler
            .clone()
            .unwrap_or_else(|| Arc::new(LocalScheduler::new(1)))
    }
}
