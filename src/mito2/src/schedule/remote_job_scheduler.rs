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

use serde::{Deserialize, Serialize};

use crate::compaction::compactor::CompactionRegion;
use crate::compaction::picker::PickerOutput;
use crate::error::Result;

pub type RemoteJobSchedulerRef = Arc<dyn RemoteJobScheduler>;

/// RemoteJobScheduler is a trait that defines the API to schedule remote jobs.
#[async_trait::async_trait]
pub trait RemoteJobScheduler: Send + Sync + 'static {
    /// Sends a job to the scheduler and returns a unique identifier for the job.
    async fn schedule(&self, job: RemoteJob) -> Result<JobId>;
}

/// JobId is a unique identifier for a remote job and allocated by the scheduler.
#[derive(Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobId(u64);

impl JobId {
    /// Returns the JobId as a u64.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// RemoteJob is a job that can be executed remotely. For example, a remote compaction job.
#[derive(Clone)]
#[allow(dead_code)]
pub enum RemoteJob {
    CompactionJob(CompactionJob),
}

/// CompactionJob is a remote job that compacts a set of files in a compaction service.
#[derive(Clone)]
#[allow(dead_code)]
pub struct CompactionJob {
    pub compaction_region: CompactionRegion,
    pub picker_output: PickerOutput,
}

/// RemoteJobSchedulerOption is an option to create a RemoteJobScheduler.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RemoteJobSchedulerOption {
    pub addr: String,
}
