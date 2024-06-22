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
use std::time::Instant;

use common_telemetry::error;
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;

use crate::compaction::compactor::CompactionRegion;
use crate::compaction::picker::PickerOutput;
use crate::error::{Error, Result};
use crate::manifest::action::RegionEdit;
use crate::request::{BackgroundNotify, CompactionFailed, CompactionFinished, WorkerRequest};

pub type RemoteJobSchedulerRef = Arc<dyn RemoteJobScheduler>;

/// RemoteJobScheduler is a trait that defines the API to schedule remote jobs.
#[async_trait::async_trait]
pub trait RemoteJobScheduler: Send + Sync + 'static {
    /// Sends a job to the scheduler and returns a unique identifier for the job.
    async fn schedule(&self, job: RemoteJob, notifier: Arc<dyn Notifier>) -> Result<JobId>;
}

/// Notifier is used to notify the mito engine when a remote job is completed.
#[async_trait::async_trait]
pub trait Notifier: Send + Sync + 'static {
    /// Notify the mito engine that a remote job is completed.
    async fn notify(&self, result: RemoteJobResult);
}

/// JobId is a unique identifier for a remote job and allocated by the scheduler.
#[derive(Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobId(u64);

impl JobId {
    /// Returns the JobId as a u64.
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Construct a new [JobId] from u64.
    pub const fn from_u64(id: u64) -> JobId {
        JobId(id)
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
    pub start_time: Instant,
}

/// RemoteJobResult is the result of a remote job.
#[allow(dead_code)]
pub enum RemoteJobResult {
    CompactionJobResult(CompactionJobResult),
}

/// CompactionJobResult is the result of a compaction job.
#[allow(dead_code)]
pub struct CompactionJobResult {
    pub job_id: JobId,
    pub region_id: RegionId,
    pub start_time: Instant,
    pub region_edit: Option<RegionEdit>,
    pub err: Option<Error>,
}

/// DefaultNotifier is a default implementation of Notifier that sends WorkerRequest to the mito engine.
pub(crate) struct DefaultNotifier {
    pub(crate) request_sender: Sender<WorkerRequest>,
}

#[async_trait::async_trait]
impl Notifier for DefaultNotifier {
    async fn notify(&self, result: RemoteJobResult) {
        match result {
            RemoteJobResult::CompactionJobResult(result) => {
                let notify = {
                    if let Some(err) = result.err {
                        BackgroundNotify::CompactionFailed(CompactionFailed {
                            region_id: result.region_id,
                            err: Arc::new(err),
                        })
                    } else if let Some(region_edit) = result.region_edit {
                        BackgroundNotify::CompactionFinished(CompactionFinished {
                            region_id: result.region_id,
                            senders: None,
                            start_time: result.start_time,
                            edit: region_edit,
                        })
                    } else {
                        // Do nothing if region_edit is None and there is no error.
                        return;
                    }
                };

                if let Err(e) = self
                    .request_sender
                    .send(WorkerRequest::Background {
                        region_id: result.region_id,
                        notify,
                    })
                    .await
                {
                    error!(
                        "Failed to notify compaction job status for region {}, request: {:?}",
                        result.region_id, e.0
                    );
                }
            }
        }
    }
}
