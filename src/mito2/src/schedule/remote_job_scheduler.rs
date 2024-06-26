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
use crate::error::Result;
use crate::manifest::action::RegionEdit;
use crate::request::{
    BackgroundNotify, CompactionFailed, CompactionFinished, OutputTx, WorkerRequest,
};

pub type RemoteJobSchedulerRef = Arc<dyn RemoteJobScheduler>;

/// RemoteJobScheduler is a trait that defines the API to schedule remote jobs.
#[async_trait::async_trait]
pub trait RemoteJobScheduler: Send + Sync + 'static {
    /// Sends a job to the scheduler and returns a UUID for the job.
    async fn schedule(&self, job: RemoteJob, notifier: Arc<dyn Notifier>) -> Result<String>;
}

/// Notifier is used to notify the mito engine when a remote job is completed.
#[async_trait::async_trait]
pub trait Notifier: Send + Sync + 'static {
    /// Notify the mito engine that a remote job is completed.
    async fn notify(&mut self, result: RemoteJobResult);
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
    pub job_id: String,
    pub region_id: RegionId,
    pub start_time: Instant,
    pub region_edit: Result<RegionEdit>,
}

/// DefaultNotifier is a default implementation of Notifier that sends WorkerRequest to the mito engine.
pub(crate) struct DefaultNotifier {
    pub(crate) request_sender: Sender<WorkerRequest>,
    pub(crate) waiters: Vec<OutputTx>,
}

#[async_trait::async_trait]
impl Notifier for DefaultNotifier {
    async fn notify(&mut self, result: RemoteJobResult) {
        match result {
            RemoteJobResult::CompactionJobResult(result) => {
                let notify = {
                    match result.region_edit {
                        Ok(edit) => BackgroundNotify::CompactionFinished(CompactionFinished {
                            region_id: result.region_id,
                            senders: std::mem::take(&mut self.waiters),
                            start_time: result.start_time,
                            edit,
                        }),
                        Err(err) => BackgroundNotify::CompactionFailed(CompactionFailed {
                            region_id: result.region_id,
                            err: Arc::new(err),
                        }),
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
