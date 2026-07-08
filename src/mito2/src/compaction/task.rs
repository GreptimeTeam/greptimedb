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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Instant;

use common_memory_manager::OnExhaustedPolicy;
use common_telemetry::{error, info, warn};
use itertools::Itertools;
use snafu::ResultExt;
use store_api::ManifestVersion;
use tokio::sync::mpsc;

use crate::compaction::LocalCompactionState;
use crate::compaction::compactor::{CompactionRegion, Compactor, MergeOutput};
use crate::compaction::memory_manager::{CompactionMemoryGuard, CompactionMemoryManager};
use crate::compaction::picker::{CompactionTask, PickerOutput};
use crate::error::{CompactRegionSnafu, CompactionMemoryExhaustedSnafu};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::metrics::{COMPACTION_FAILURE_COUNT, COMPACTION_MEMORY_WAIT, COMPACTION_STAGE_ELAPSED};
use crate::region::RegionRoleState;
use crate::request::{
    BackgroundNotify, CompactionCancelled, CompactionFailed, CompactionFinished, OutputTx,
    RegionEditResult, Waiters, WorkerRequest, WorkerRequestWithTime,
};
use crate::sst::file::FileMeta;
use crate::worker::WorkerListener;
use crate::{error, metrics};

/// Maximum number of compaction tasks in parallel.
pub const MAX_PARALLEL_COMPACTION: usize = 1;

pub(crate) struct CompactionTaskImpl {
    /// Shared local-compaction state for cooperative cancellation.
    pub(crate) state: LocalCompactionState,
    pub compaction_region: CompactionRegion,
    /// Request sender to notify the worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequestWithTime>,
    /// Senders that are used to notify waiters waiting for pending compaction tasks.
    pub waiters: Vec<OutputTx>,
    /// Start time of compaction task
    pub start_time: Instant,
    /// Event listener.
    pub(crate) listener: WorkerListener,
    /// Compactor to handle compaction.
    pub(crate) compactor: Arc<dyn Compactor>,
    /// Output of the picker.
    pub(crate) picker_output: PickerOutput,
    /// Memory manager to acquire memory budget.
    pub(crate) memory_manager: Arc<CompactionMemoryManager>,
    /// Policy when memory is exhausted.
    pub(crate) memory_policy: OnExhaustedPolicy,
    /// Estimated memory bytes needed for this compaction.
    pub(crate) estimated_memory_bytes: u64,
}

impl Debug for CompactionTaskImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwcsCompactionTask")
            .field("region_id", &self.compaction_region.region_id)
            .field("picker_output", &self.picker_output)
            .field(
                "append_mode",
                &self.compaction_region.region_options.append_mode,
            )
            .finish()
    }
}

impl Drop for CompactionTaskImpl {
    fn drop(&mut self) {
        self.mark_files_compacting(false)
    }
}

impl CompactionTaskImpl {
    fn mark_files_compacting(&self, compacting: bool) {
        self.picker_output
            .outputs
            .iter()
            .for_each(|o| o.inputs.iter().for_each(|f| f.set_compacting(compacting)));
    }

    /// Acquires memory budget based on the configured policy.
    ///
    /// Returns an error if memory cannot be acquired according to the policy.
    async fn acquire_memory_with_policy(&self) -> error::Result<CompactionMemoryGuard> {
        let region_id = self.compaction_region.region_id;
        let requested_bytes = self.estimated_memory_bytes;
        let policy = self.memory_policy;

        let _timer = COMPACTION_MEMORY_WAIT.start_timer();
        self.memory_manager
            .acquire_with_policy(requested_bytes, policy)
            .await
            .context(CompactionMemoryExhaustedSnafu {
                region_id,
                policy: format!("{policy:?}"),
            })
    }

    /// Remove expired ssts files, update manifest immediately
    /// and apply the edit to region version.
    ///
    /// This function logs errors but does not stop the compaction process if removal fails.
    async fn remove_expired(
        &self,
        compaction_region: &CompactionRegion,
        expired_files: Vec<FileMeta>,
    ) {
        let region_id = compaction_region.region_id;
        let expired_files_str = expired_files.iter().map(|f| f.file_id).join(",");
        let (expire_delete_sender, expire_delete_listener) = tokio::sync::oneshot::channel();
        // Update manifest to remove expired SSTs
        let edit = RegionEdit {
            files_to_add: Vec::new(),
            files_to_remove: expired_files,
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        };

        // 1. Update manifest
        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        let RegionRoleState::Leader(current_region_state) =
            compaction_region.manifest_ctx.current_state()
        else {
            warn!(
                "Region {} not in leader state, skip removing expired files",
                region_id
            );
            return;
        };
        if let Err(e) = compaction_region
            .manifest_ctx
            .update_manifest(current_region_state, action_list, false)
            .await
        {
            warn!(
                e;
                "Failed to update manifest for expired files removal, region: {region_id}, files: [{expired_files_str}]. Compaction will continue."
            );
            return;
        }

        // 2. Notify region worker loop to remove expired files from region version.
        self.send_to_worker(WorkerRequest::Background {
            region_id,
            notify: BackgroundNotify::RegionEdit(RegionEditResult {
                region_id,
                waiters: Waiters::one(expire_delete_sender),
                edit,
                result: Ok(()),
                update_region_state: false,
                is_staging: false,
            }),
        })
        .await;

        if let Err(e) = expire_delete_listener
            .await
            .context(error::RecvSnafu)
            .flatten()
        {
            warn!(
                e;
                "Failed to remove expired files from region version, region: {region_id}, files: [{expired_files_str}]. Compaction will continue."
            );
            return;
        }

        info!(
            "Successfully removed expired files, region: {region_id}, files: [{expired_files_str}]"
        );
    }

    async fn handle_expiration(&mut self) {
        // 1. In case of local compaction, we can delete expired ssts in advance.
        if !self.picker_output.expired_ssts.is_empty() {
            let remove_timer = COMPACTION_STAGE_ELAPSED
                .with_label_values(&["remove_expired"])
                .start_timer();
            let expired_ssts = self
                .picker_output
                .expired_ssts
                .drain(..)
                .map(|f| f.meta_ref().clone())
                .collect();
            // remove_expired logs errors but doesn't stop compaction
            self.remove_expired(&self.compaction_region, expired_ssts)
                .await;
            remove_timer.observe_duration();
        }
    }

    async fn handle_compaction(&mut self) -> error::Result<MergeOutput> {
        // 2. Merge inputs
        let merge_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["merge"])
            .start_timer();

        let compaction_result = match self
            .compactor
            .merge_ssts(&self.compaction_region, self.picker_output.clone())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!(e; "Failed to compact region: {}", self.compaction_region.region_id);
                merge_timer.stop_and_discard();
                return Err(e);
            }
        };
        let merge_time = merge_timer.stop_and_record();

        metrics::COMPACTION_INPUT_BYTES.inc_by(compaction_result.input_file_size() as f64);
        metrics::COMPACTION_OUTPUT_BYTES.inc_by(compaction_result.output_file_size() as f64);
        info!(
            "Compacted SST files, region_id: {}, input: {:?}, output: {:?}, window: {:?}, waiter_num: {}, merge_time: {}s",
            self.compaction_region.region_id,
            compaction_result.files_to_remove,
            compaction_result.files_to_add,
            compaction_result.compaction_time_window,
            self.waiters.len(),
            merge_time,
        );

        self.listener
            .on_merge_ssts_finished(self.compaction_region.region_id)
            .await;

        Ok(compaction_result)
    }

    async fn update_manifest(
        &self,
        compaction_result: crate::compaction::compactor::MergeOutput,
    ) -> error::Result<(RegionEdit, ManifestVersion)> {
        let _manifest_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["write_manifest"])
            .start_timer();

        self.compactor
            .update_manifest(&self.compaction_region, compaction_result)
            .await
    }

    /// Handles compaction failure, notifies all waiters.
    pub(crate) fn on_failure(&mut self, err: Arc<error::Error>) {
        COMPACTION_FAILURE_COUNT.inc();
        for waiter in self.waiters.drain(..) {
            waiter.send(Err(err.clone()).context(CompactRegionSnafu {
                region_id: self.compaction_region.region_id,
            }));
        }
    }

    /// Notifies region worker to handle post-compaction tasks.
    async fn send_to_worker(&self, request: WorkerRequest) {
        if let Err(e) = self
            .request_sender
            .send(WorkerRequestWithTime::new(request))
            .await
        {
            error!(
                "Failed to notify compaction job status for region {}, request: {:?}",
                self.compaction_region.region_id, e.0
            );
        }
    }

    async fn invoke_sst_hook(&self, merge_output: &MergeOutput) {
        self.compaction_region.invoke_sst_hook(merge_output).await;
    }
}

#[async_trait::async_trait]
impl CompactionTask for CompactionTaskImpl {
    async fn run(&mut self) {
        // Acquire memory budget before starting compaction
        let _memory_guard = match self.acquire_memory_with_policy().await {
            Ok(guard) => guard,
            Err(e) => {
                error!(e; "Failed to acquire memory for compaction, region id: {}", self.compaction_region.region_id);
                let err = Arc::new(e);
                self.on_failure(err.clone());
                let notify = BackgroundNotify::CompactionFailed(CompactionFailed {
                    region_id: self.compaction_region.region_id,
                    err,
                });
                self.send_to_worker(WorkerRequest::Background {
                    region_id: self.compaction_region.region_id,
                    notify,
                })
                .await;
                return;
            }
        };

        // Marks files compacting before compaction and unmark after compaction (even if compaction is cancelled or failed), so that they won't be picked by other compaction tasks.
        self.mark_files_compacting(true);
        self.handle_expiration().await;

        let notify = match self.handle_compaction().await {
            Ok(merge_output) => {
                // Stop accepting cancellation once we are about to publish the compaction edit.
                if merge_output.files_to_add.is_empty() && !self.state.mark_commit_started() {
                    let senders = std::mem::take(&mut self.waiters);
                    BackgroundNotify::CompactionCancelled(CompactionCancelled {
                        region_id: self.compaction_region.region_id,
                        senders,
                    })
                } else {
                    if !merge_output.files_to_add.is_empty() {
                        self.state.mark_commit_started_after_output();
                    }
                    self.invoke_sst_hook(&merge_output).await;
                    match self.update_manifest(merge_output).await {
                        Ok((edit, _manifest_version)) => {
                            let senders = std::mem::take(&mut self.waiters);
                            BackgroundNotify::CompactionFinished(CompactionFinished {
                                region_id: self.compaction_region.region_id,
                                senders,
                                start_time: self.start_time,
                                edit,
                            })
                        }
                        Err(e) => {
                            error!(e; "Failed to compact region, region id: {}", self.compaction_region.region_id);
                            let err = Arc::new(e);
                            self.on_failure(err.clone());
                            BackgroundNotify::CompactionFailed(CompactionFailed {
                                region_id: self.compaction_region.region_id,
                                err,
                            })
                        }
                    }
                }
            }
            Err(e) => {
                error!(e; "Failed to compact region, region id: {}", self.compaction_region.region_id);
                let err = Arc::new(e);
                // notify compaction waiters
                self.on_failure(err.clone());
                BackgroundNotify::CompactionFailed(CompactionFailed {
                    region_id: self.compaction_region.region_id,
                    err,
                })
            }
        };

        self.send_to_worker(WorkerRequest::Background {
            region_id: self.compaction_region.region_id,
            notify,
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use common_base::Plugins;
    use common_base::cancellation::CancellationHandle;
    use common_memory_manager::OnExhaustedPolicy;
    use store_api::ManifestVersion;
    use store_api::metadata::RegionMetadataRef;
    use store_api::storage::{FileId, RegionId};
    use tokio::sync::{Notify, mpsc};
    use tokio::time::timeout;

    use super::CompactionTaskImpl;
    use crate::cache::CacheManager;
    use crate::compaction::compactor::{
        CompactionRegion, CompactionVersion, Compactor, MergeOutput,
    };
    use crate::compaction::memory_manager::new_compaction_memory_manager;
    use crate::compaction::picker::{CompactionTask, PickerOutput};
    use crate::compaction::test_util::new_file_handle;
    use crate::compaction::{LocalCompactionState, RequestCancelResult};
    use crate::config::MitoConfig;
    use crate::engine::region_hook::{RegionHook, RegionHookRef, SstFileInfo};
    use crate::error::Result;
    use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
    use crate::region::options::RegionOptions;
    use crate::request::{BackgroundNotify, WorkerRequest};
    use crate::sst::file::FileMeta;
    use crate::sst::version::SstVersion;
    use crate::test_util::memtable_util::metadata_for_test;
    use crate::test_util::scheduler_util::SchedulerEnv;
    use crate::worker::WorkerListener;

    #[test]
    fn test_picker_output_with_expired_ssts() {
        // Test that PickerOutput correctly includes expired_ssts
        // This verifies that expired SSTs are properly identified and included
        // in the picker output, which is then handled by handle_expiration()

        let file_ids = (0..3).map(|_| FileId::random()).collect::<Vec<_>>();
        let expired_ssts = vec![
            new_file_handle(file_ids[0], 0, 999, 0),
            new_file_handle(file_ids[1], 1000, 1999, 0),
        ];

        let picker_output = PickerOutput {
            outputs: vec![],
            expired_ssts: expired_ssts.clone(),
            time_window_size: 3600,
            max_file_size: None,
        };

        // Verify expired_ssts are included
        assert_eq!(picker_output.expired_ssts.len(), 2);
        assert_eq!(
            picker_output.expired_ssts[0].file_id(),
            expired_ssts[0].file_id()
        );
        assert_eq!(
            picker_output.expired_ssts[1].file_id(),
            expired_ssts[1].file_id()
        );
    }

    #[test]
    fn test_picker_output_without_expired_ssts() {
        // Test that PickerOutput works correctly when there are no expired SSTs
        let picker_output = PickerOutput {
            outputs: vec![],
            expired_ssts: vec![],
            time_window_size: 3600,
            max_file_size: None,
        };

        // Verify empty expired_ssts
        assert!(picker_output.expired_ssts.is_empty());
    }

    fn dummy_file_meta(region_id: RegionId) -> FileMeta {
        FileMeta {
            region_id,
            file_id: FileId::random(),
            file_size: 1024,
            ..Default::default()
        }
    }

    async fn new_test_compaction_region(hook: RegionHookRef) -> CompactionRegion {
        let env = SchedulerEnv::new().await;
        let metadata = metadata_for_test();
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        let plugins = Plugins::new();
        plugins.insert(hook);

        CompactionRegion {
            region_id: metadata.region_id,
            region_options: RegionOptions::default(),
            engine_config: Arc::new(MitoConfig::default()),
            region_metadata: metadata.clone(),
            cache_manager: Arc::new(CacheManager::default()),
            access_layer: env.access_layer.clone(),
            manifest_ctx,
            current_version: CompactionVersion {
                metadata,
                options: RegionOptions::default(),
                ssts: Arc::new(SstVersion::new()),
                compaction_time_window: None,
            },
            file_purger: None,
            ttl: None,
            max_parallelism: 1,
            plugins,
        }
    }

    struct PausingSstHook {
        reached: Arc<Notify>,
        release: Arc<Notify>,
        observed_files: Arc<Mutex<Vec<FileId>>>,
    }

    impl fmt::Debug for PausingSstHook {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("PausingSstHook").finish()
        }
    }

    #[async_trait::async_trait]
    impl RegionHook for PausingSstHook {
        async fn on_sst_files_written(
            &self,
            _region_id: RegionId,
            _region_metadata: &RegionMetadataRef,
            files: &[SstFileInfo<'_>],
        ) {
            self.observed_files
                .lock()
                .unwrap()
                .extend(files.iter().map(|file| file.file_meta.file_id));
            self.reached.notify_one();
            self.release.notified().await;
        }
    }

    struct NoopRegionHook;

    impl fmt::Debug for NoopRegionHook {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("NoopRegionHook").finish()
        }
    }

    #[async_trait::async_trait]
    impl RegionHook for NoopRegionHook {}

    struct WrittenOutputCompactor {
        output_file: FileMeta,
        update_manifest_calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Compactor for WrittenOutputCompactor {
        async fn merge_ssts(
            &self,
            _compaction_region: &CompactionRegion,
            _picker_output: PickerOutput,
        ) -> Result<MergeOutput> {
            Ok(MergeOutput {
                files_to_add: vec![self.output_file.clone()],
                files_to_remove: Vec::new(),
                compaction_time_window: Some(3600),
                sst_infos: Vec::new(),
            })
        }

        async fn update_manifest(
            &self,
            compaction_region: &CompactionRegion,
            merge_output: MergeOutput,
        ) -> Result<(RegionEdit, ManifestVersion)> {
            self.update_manifest_calls.fetch_add(1, Ordering::SeqCst);

            let edit = RegionEdit {
                files_to_add: merge_output.files_to_add,
                files_to_remove: merge_output.files_to_remove,
                timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            };
            let action_list =
                RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
            let manifest_version = compaction_region
                .manifest_ctx
                .update_manifest_for_compaction(action_list)
                .await?;

            Ok((edit, manifest_version))
        }
    }

    struct PausingOutputCompactor {
        output_file: FileMeta,
        output_ready: Arc<Notify>,
        release_output: Arc<Notify>,
        update_manifest_calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Compactor for PausingOutputCompactor {
        async fn merge_ssts(
            &self,
            _compaction_region: &CompactionRegion,
            _picker_output: PickerOutput,
        ) -> Result<MergeOutput> {
            self.output_ready.notify_one();
            self.release_output.notified().await;

            Ok(MergeOutput {
                files_to_add: vec![self.output_file.clone()],
                files_to_remove: Vec::new(),
                compaction_time_window: Some(3600),
                sst_infos: Vec::new(),
            })
        }

        async fn update_manifest(
            &self,
            compaction_region: &CompactionRegion,
            merge_output: MergeOutput,
        ) -> Result<(RegionEdit, ManifestVersion)> {
            self.update_manifest_calls.fetch_add(1, Ordering::SeqCst);

            let edit = RegionEdit {
                files_to_add: merge_output.files_to_add,
                files_to_remove: merge_output.files_to_remove,
                timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            };
            let action_list =
                RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
            let manifest_version = compaction_region
                .manifest_ctx
                .update_manifest_for_compaction(action_list)
                .await?;

            Ok((edit, manifest_version))
        }
    }

    #[tokio::test]
    async fn test_cancel_after_merge_output_is_too_late_and_commits_manifest() {
        common_telemetry::init_default_ut_logging();

        let hook_reached = Arc::new(Notify::new());
        let hook_release = Arc::new(Notify::new());
        let observed_files = Arc::new(Mutex::new(Vec::new()));
        let hook: RegionHookRef = Arc::new(PausingSstHook {
            reached: hook_reached.clone(),
            release: hook_release.clone(),
            observed_files: observed_files.clone(),
        });

        let compaction_region = new_test_compaction_region(hook).await;
        let region_id = compaction_region.region_id;
        let manifest_ctx = compaction_region.manifest_ctx.clone();
        let output_file = dummy_file_meta(region_id);
        let output_file_id = output_file.file_id;
        let update_manifest_calls = Arc::new(AtomicUsize::new(0));
        let state = LocalCompactionState::new(Arc::new(CancellationHandle::default()));
        let (request_sender, mut request_receiver) = mpsc::channel(4);
        let mut task = CompactionTaskImpl {
            state: state.clone(),
            compaction_region,
            request_sender,
            waiters: Vec::new(),
            start_time: Instant::now(),
            listener: WorkerListener::default(),
            compactor: Arc::new(WrittenOutputCompactor {
                output_file,
                update_manifest_calls: update_manifest_calls.clone(),
            }),
            picker_output: PickerOutput {
                outputs: Vec::new(),
                expired_ssts: Vec::new(),
                time_window_size: 3600,
                max_file_size: None,
            },
            memory_manager: Arc::new(new_compaction_memory_manager(1024 * 1024)),
            memory_policy: OnExhaustedPolicy::Fail,
            estimated_memory_bytes: 1,
        };

        let runner = tokio::spawn(async move {
            task.run().await;
        });

        timeout(Duration::from_secs(5), hook_reached.notified())
            .await
            .expect("compaction should pause after observing written SST files");

        assert_eq!(observed_files.lock().unwrap().clone(), vec![output_file_id]);
        assert_eq!(state.request_cancel(), RequestCancelResult::TooLateToCancel);
        hook_release.notify_one();

        let worker_request = timeout(Duration::from_secs(5), request_receiver.recv())
            .await
            .expect("compaction should notify the worker after manifest commit")
            .expect("worker request sender should stay open");

        match worker_request.request {
            WorkerRequest::Background {
                region_id: notified_region,
                notify: BackgroundNotify::CompactionFinished(finished),
            } => {
                assert_eq!(notified_region, region_id);
                assert_eq!(finished.region_id, region_id);
                assert_eq!(finished.edit.files_to_add.len(), 1);
                assert_eq!(finished.edit.files_to_add[0].file_id, output_file_id);
            }
            other => panic!("expected finished compaction notification, got {other:?}"),
        }

        runner
            .await
            .expect("compaction task should finish after committing manifest");

        assert_eq!(update_manifest_calls.load(Ordering::SeqCst), 1);
        let manifest = manifest_ctx.manifest_manager.read().await.manifest();
        assert!(
            manifest.files.contains_key(&output_file_id),
            "compaction output should be manifest-owned after cancellation is too late"
        );
    }

    #[tokio::test]
    async fn test_cancel_while_merge_holds_output_still_commits_manifest() {
        common_telemetry::init_default_ut_logging();

        let hook: RegionHookRef = Arc::new(NoopRegionHook);
        let compaction_region = new_test_compaction_region(hook).await;
        let region_id = compaction_region.region_id;
        let manifest_ctx = compaction_region.manifest_ctx.clone();
        let output_file = dummy_file_meta(region_id);
        let output_file_id = output_file.file_id;
        let output_ready = Arc::new(Notify::new());
        let release_output = Arc::new(Notify::new());
        let update_manifest_calls = Arc::new(AtomicUsize::new(0));
        let state = LocalCompactionState::new(Arc::new(CancellationHandle::default()));
        let (request_sender, mut request_receiver) = mpsc::channel(4);
        let mut task = CompactionTaskImpl {
            state: state.clone(),
            compaction_region,
            request_sender,
            waiters: Vec::new(),
            start_time: Instant::now(),
            listener: WorkerListener::default(),
            compactor: Arc::new(PausingOutputCompactor {
                output_file,
                output_ready: output_ready.clone(),
                release_output: release_output.clone(),
                update_manifest_calls: update_manifest_calls.clone(),
            }),
            picker_output: PickerOutput {
                outputs: Vec::new(),
                expired_ssts: Vec::new(),
                time_window_size: 3600,
                max_file_size: None,
            },
            memory_manager: Arc::new(new_compaction_memory_manager(1024 * 1024)),
            memory_policy: OnExhaustedPolicy::Fail,
            estimated_memory_bytes: 1,
        };

        let runner = tokio::spawn(async move {
            task.run().await;
        });

        timeout(Duration::from_secs(5), output_ready.notified())
            .await
            .expect("compaction should pause while holding a written output");

        assert_eq!(state.request_cancel(), RequestCancelResult::CancelIssued);
        release_output.notify_one();

        let worker_request = timeout(Duration::from_secs(5), request_receiver.recv())
            .await
            .expect("compaction should notify the worker after manifest commit")
            .expect("worker request sender should stay open");

        match worker_request.request {
            WorkerRequest::Background {
                region_id: notified_region,
                notify: BackgroundNotify::CompactionFinished(finished),
            } => {
                assert_eq!(notified_region, region_id);
                assert_eq!(finished.region_id, region_id);
                assert_eq!(finished.edit.files_to_add.len(), 1);
                assert_eq!(finished.edit.files_to_add[0].file_id, output_file_id);
            }
            other => panic!("expected finished compaction notification, got {other:?}"),
        }

        runner
            .await
            .expect("compaction task should finish after committing manifest");

        assert_eq!(update_manifest_calls.load(Ordering::SeqCst), 1);
        let manifest = manifest_ctx.manifest_manager.read().await.manifest();
        assert!(
            manifest.files.contains_key(&output_file_id),
            "compaction output should be manifest-owned even if cancellation was issued first"
        );
    }

    // Note: Testing remove_expired() directly requires extensive mocking of:
    // - manifest_ctx (ManifestContext)
    // - request_sender (mpsc::Sender<WorkerRequestWithTime>)
    // - WorkerRequest handling
    //
    // The behavior is tested indirectly through integration tests:
    // - remove_expired() logs errors but doesn't stop compaction
    // - handle_expiration() continues even if remove_expired() encounters errors
    // - The expiration stage is designed to be non-blocking for compaction
}
