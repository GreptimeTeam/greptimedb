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

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use common_telemetry::error;
use serde::{Deserialize, Serialize};
use snafu::{Location, ResultExt, Snafu};
use store_api::manifest::ManifestVersion;
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::compaction::compactor::CompactionRegion;
use crate::compaction::picker::PickerOutput;
use crate::error::{CompactRegionSnafu, Error, ParseJobIdSnafu, Result};
use crate::manifest::action::RegionEdit;
use crate::metrics::{COMPACTION_FAILURE_COUNT, INFLIGHT_COMPACTION_COUNT};
use crate::request::{
    BackgroundNotify, CompactionFailed, CompactionFinished, OutputTx, WorkerRequest,
};

pub type RemoteJobSchedulerRef = Arc<dyn RemoteJobScheduler>;

#[cfg_attr(doc, aquamarine::aquamarine)]
/// RemoteJobScheduler is a trait that defines the API to schedule remote jobs.
/// For example, a compaction job can be scheduled remotely as the following workflow:
/// ```mermaid
///   participant User
///   participant MitoEngine
///   participant CompactionScheduler
///   participant Plugins
///   participant RemoteJobScheduler
///
///   User->>MitoEngine: Initiates compaction
///   MitoEngine->>CompactionScheduler: schedule_compaction()
///   CompactionScheduler->>Plugins: Handle plugins
///   CompactionScheduler->>RemoteJobScheduler: schedule(CompactionJob)
///   RemoteJobScheduler-->>CompactionScheduler: Returns Job UUID
///   CompactionScheduler-->>MitoEngine: Task scheduled with Job UUID
///   MitoEngine-->>User: Compaction task scheduled
/// ```
#[async_trait::async_trait]
pub trait RemoteJobScheduler: Send + Sync + 'static {
    /// Sends a job to the scheduler and returns a UUID for the job.
    async fn schedule(
        &self,
        job: RemoteJob,
        notifier: Box<dyn Notifier>,
    ) -> Result<JobId, RemoteJobSchedulerError>;
}

#[derive(Snafu, Debug)]
#[snafu(display("Internal error occurred in remote job scheduler: {}", reason))]
pub struct RemoteJobSchedulerError {
    #[snafu(implicit)]
    pub location: Location,
    pub reason: String,
    // Keep the waiters in the error so that we can notify them when fallback to the local compaction.
    pub waiters: Vec<OutputTx>,
}

/// Notifier is used to notify the mito engine when a remote job is completed.
#[async_trait::async_trait]
pub trait Notifier: Send + Sync + 'static {
    /// Notify the mito engine that a remote job is completed.
    async fn notify(&self, result: RemoteJobResult, waiters: Vec<OutputTx>);
}

/// Unique id for a remote job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(Uuid);

impl JobId {
    /// Parses job id from string.
    pub fn parse_str(input: &str) -> Result<JobId> {
        Uuid::parse_str(input).map(JobId).context(ParseJobIdSnafu)
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// RemoteJob is a job that can be executed remotely. For example, a remote compaction job.
#[allow(dead_code)]
pub enum RemoteJob {
    CompactionJob(CompactionJob),
}

/// CompactionJob is a remote job that compacts a set of files in a compaction service.
#[allow(dead_code)]
pub struct CompactionJob {
    pub compaction_region: CompactionRegion,
    pub picker_output: PickerOutput,
    pub start_time: Instant,
    /// Send the result of the compaction job to these waiters.
    pub waiters: Vec<OutputTx>,
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
    pub region_edit: Result<(ManifestVersion, RegionEdit)>,
}

/// DefaultNotifier is a default implementation of Notifier that sends WorkerRequest to the mito engine.
pub(crate) struct DefaultNotifier {
    /// The sender to send WorkerRequest to the mito engine. This is used to notify the mito engine when a remote job is completed.
    pub(crate) request_sender: Sender<WorkerRequest>,
}

impl DefaultNotifier {
    fn on_failure(&self, err: Arc<Error>, region_id: RegionId, mut waiters: Vec<OutputTx>) {
        COMPACTION_FAILURE_COUNT.inc();
        for waiter in waiters.drain(..) {
            waiter.send(Err(err.clone()).context(CompactRegionSnafu { region_id }));
        }
    }
}

#[async_trait::async_trait]
impl Notifier for DefaultNotifier {
    async fn notify(&self, result: RemoteJobResult, waiters: Vec<OutputTx>) {
        INFLIGHT_COMPACTION_COUNT.dec();
        match result {
            RemoteJobResult::CompactionJobResult(result) => {
                let notify = {
                    match result.region_edit {
                        Ok((manifest_version, edit)) => {
                            BackgroundNotify::CompactionFinished(CompactionFinished {
                                region_id: result.region_id,
                                senders: waiters,
                                start_time: result.start_time,
                                edit,
                                manifest_version,
                            })
                        }
                        Err(err) => {
                            error!(
                                "Compaction failed for region {}: {:?}",
                                result.region_id, err
                            );
                            let err = Arc::new(err);
                            self.on_failure(err.clone(), result.region_id, waiters);
                            BackgroundNotify::CompactionFailed(CompactionFailed {
                                region_id: result.region_id,
                                err,
                            })
                        }
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
                        "Failed to notify compaction job status for region {}, error: {:?}",
                        result.region_id, e
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_id() {
        let id = Uuid::new_v4().to_string();
        let job_id = JobId::parse_str(&id).unwrap();
        assert_eq!(job_id.to_string(), id);
    }
}
