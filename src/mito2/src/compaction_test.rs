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

use std::assert_matches;
use std::time::Duration;

use api::v1::region::StrictWindow;
use common_datasource::compression::CompressionType;
use common_meta::key::schema_name::SchemaNameValue;
use common_time::DatabaseTimeToLive;
use store_api::storage::FileId;
use tokio::sync::{Barrier, oneshot};

use super::*;
use crate::compaction::memory_manager::{CompactionMemoryGuard, new_compaction_memory_manager};
use crate::compaction::test_util::new_file_handle;
use crate::error::InvalidSchedulerStateSnafu;
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::region::ManifestContext;
use crate::schedule::scheduler::{Job, Scheduler};
use crate::sst::FormatType;
use crate::test_util::mock_schema_metadata_manager;
use crate::test_util::scheduler_util::{SchedulerEnv, VecScheduler};
use crate::test_util::version_util::{VersionControlBuilder, apply_edit};

struct FailingScheduler;

struct FailingRemoteScheduler;

#[derive(Default)]
struct HoldingRemoteScheduler {
    notifier: Mutex<Option<Box<dyn crate::schedule::remote_job_scheduler::Notifier>>>,
}

impl HoldingRemoteScheduler {
    fn drop_notifier(&self) {
        self.notifier.lock().unwrap().take();
    }
}

#[async_trait::async_trait]
impl crate::schedule::remote_job_scheduler::RemoteJobScheduler for FailingRemoteScheduler {
    async fn schedule(
        &self,
        job: RemoteJob,
        _notifier: Box<dyn crate::schedule::remote_job_scheduler::Notifier>,
    ) -> std::result::Result<
        crate::schedule::remote_job_scheduler::JobId,
        crate::schedule::remote_job_scheduler::RemoteJobSchedulerError,
    > {
        let RemoteJob::CompactionJob(job) = job;
        Err(
            crate::schedule::remote_job_scheduler::RemoteJobSchedulerError {
                location: snafu::location!(),
                reason: "remote scheduler rejected job".to_string(),
                waiters: job.waiters,
            },
        )
    }
}

#[async_trait::async_trait]
impl crate::schedule::remote_job_scheduler::RemoteJobScheduler for HoldingRemoteScheduler {
    async fn schedule(
        &self,
        _job: RemoteJob,
        notifier: Box<dyn crate::schedule::remote_job_scheduler::Notifier>,
    ) -> std::result::Result<
        crate::schedule::remote_job_scheduler::JobId,
        crate::schedule::remote_job_scheduler::RemoteJobSchedulerError,
    > {
        self.notifier.lock().unwrap().replace(notifier);
        Ok(crate::schedule::remote_job_scheduler::JobId::parse_str(
            "00000000-0000-0000-0000-000000000002",
        )
        .unwrap())
    }
}

fn compactable_version() -> VersionControlRef {
    let mut builder = VersionControlBuilder::new();
    let end = 1000 * 1000;
    Arc::new(
        builder
            .push_l0_file(0, end)
            .push_l0_file(10, end)
            .push_l0_file(50, end)
            .push_l0_file(80, end)
            .push_l0_file(90, end)
            .build(),
    )
}

async fn begin_pick_result(
    env: &SchedulerEnv,
    scheduler: &mut CompactionScheduler,
    rx: &mut mpsc::Receiver<WorkerRequestWithTime>,
    version_control: &VersionControlRef,
) -> (
    CompactionPickFinished,
    ManifestContextRef,
    SchemaMetadataManagerRef,
) {
    let region_id = version_control.current().version.metadata.region_id;
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    assert!(
        scheduler
            .schedule_compaction(
                region_id,
                Options::Regular(Default::default()),
                version_control,
                &env.access_layer,
                OptionOutputTx::none(),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
            )
            .unwrap()
    );
    let finished = recv_compaction_pick_finished(rx).await;
    assert!(matches!(
        &finished.result,
        CompactionPlanningResult::Prepared(_)
    ));
    (finished, manifest_ctx, schema_metadata_manager)
}

fn selected_files(finished: &CompactionPickFinished) -> Vec<FileHandle> {
    let CompactionPlanningResult::Prepared(prepared) = &finished.result else {
        panic!("expected prepared compaction");
    };
    prepared
        .picker_output
        .outputs
        .iter()
        .flat_map(|output| output.inputs.iter().cloned())
        .chain(prepared.picker_output.expired_ssts.iter().cloned())
        .collect()
}

fn use_remote_compaction(finished: &mut CompactionPickFinished) {
    let CompactionPlanningResult::Prepared(prepared) = &mut finished.result else {
        panic!("expected prepared compaction");
    };
    let crate::region::options::CompactionOptions::Twcs(options) =
        &mut prepared.compaction_region.region_options.compaction;
    options.remote_compaction = true;
    options.fallback_to_local = false;
}

fn picker_output_with_files(
    output_files: Vec<FileHandle>,
    expired_ssts: Vec<FileHandle>,
) -> PickerOutput {
    PickerOutput {
        outputs: vec![CompactionOutput {
            output_level: 1,
            inputs: output_files,
            filter_deleted: false,
            output_time_range: None,
        }],
        expired_ssts,
        ..Default::default()
    }
}

#[async_trait::async_trait]
impl Scheduler for FailingScheduler {
    fn schedule(&self, _job: Job) -> Result<()> {
        InvalidSchedulerStateSnafu.fail()
    }

    async fn stop(&self, _await_termination: bool) -> Result<()> {
        Ok(())
    }
}

async fn recv_compaction_pick_finished(
    rx: &mut mpsc::Receiver<WorkerRequestWithTime>,
) -> CompactionPickFinished {
    let request = rx.recv().await.expect("worker request channel closed");
    match request.request {
        WorkerRequest::Background {
            notify: BackgroundNotify::CompactionPickFinished(finished),
            ..
        } => finished,
        other => panic!("unexpected worker request: {other:?}"),
    }
}

fn assert_compaction_lifecycle_error(err: Error, lifecycle: &str) {
    let Error::CompactRegion { source, .. } = err else {
        panic!("expected compact-region error, got {err:?}");
    };
    let matches_lifecycle = matches!(
        (lifecycle, source.as_ref()),
        ("close", Error::RegionClosed { .. })
            | ("drop", Error::RegionDropped { .. })
            | ("truncate", Error::RegionTruncated { .. })
    );
    assert!(
        matches_lifecycle,
        "unexpected {lifecycle} error source: {source:?}"
    );
}

#[test]
fn test_picking_compacting_files_rolls_back_on_conflict() {
    let first = new_file_handle(FileId::random(), 0, 10, 0);
    let conflicting = new_file_handle(FileId::random(), 0, 10, 0);
    conflicting.set_compacting(true);
    let output = picker_output_with_files(vec![first.clone(), conflicting.clone()], vec![]);

    assert!(CompactingFiles::try_new(&output).is_none());
    assert!(!first.compacting());
    assert!(conflicting.compacting());
}



#[test]
fn test_pick_result_rejects_ambiguous_file_id_across_levels() {
    let purger = crate::test_util::new_noop_file_purger();
    let selected = new_file_handle(FileId::random(), 0, 10, 1);
    let mut wrong_level = selected.meta_ref().clone();
    wrong_level.level = 0;
    let current_level = selected.meta_ref().clone();
    let mut current = crate::sst::version::SstVersion::new();
    current.add_files(purger.clone(), std::iter::once(wrong_level));
    let output = picker_output_with_files(vec![selected], Vec::new());

    assert!(refresh_picker_output(output.clone(), &current).is_none());
    current.add_files(purger, std::iter::once(current_level));
    assert!(refresh_picker_output(output, &current).is_none());
}

#[tokio::test]
async fn test_find_compaction_options_db_level() {
    let builder = VersionControlBuilder::new();
    let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
    let region_id = builder.region_id();
    let table_id = region_id.table_id();
    // Register table without ttl but with db-level compaction options
    let mut schema_value = SchemaNameValue {
        ttl: Some(DatabaseTimeToLive::default()),
        ..Default::default()
    };
    schema_value
        .extra_options
        .insert("compaction.type".to_string(), "twcs".to_string());
    schema_value
        .extra_options
        .insert("compaction.twcs.time_window".to_string(), "2h".to_string());
    schema_metadata_manager
        .register_region_table_info(
            table_id,
            "t",
            "c",
            "s",
            Some(schema_value),
            kv_backend.clone(),
        )
        .await;

    let version_control = Arc::new(builder.build());
    let region_opts = version_control.current().version.options.clone();
    let (opts, _) = find_dynamic_options(region_id, &region_opts, &schema_metadata_manager)
        .await
        .unwrap();
    match opts {
        crate::region::options::CompactionOptions::Twcs(t) => {
            assert_eq!(t.time_window_seconds(), Some(2 * 3600));
        }
    }
}

#[tokio::test]
async fn test_find_compaction_options_priority() {
    fn schema_value_with_twcs(time_window: &str) -> SchemaNameValue {
        let mut schema_value = SchemaNameValue {
            ttl: Some(DatabaseTimeToLive::default()),
            ..Default::default()
        };
        schema_value
            .extra_options
            .insert("compaction.type".to_string(), "twcs".to_string());
        schema_value.extra_options.insert(
            "compaction.twcs.time_window".to_string(),
            time_window.to_string(),
        );
        schema_value
    }

    let cases = [
        (
            "db options set and table override set",
            Some(schema_value_with_twcs("2h")),
            true,
            Some(Duration::from_secs(5 * 3600)),
            Some(5 * 3600),
        ),
        (
            "db options set and table override not set",
            Some(schema_value_with_twcs("2h")),
            false,
            None,
            Some(2 * 3600),
        ),
        (
            "db options not set and table override set",
            None,
            true,
            Some(Duration::from_secs(4 * 3600)),
            Some(4 * 3600),
        ),
        (
            "db options not set and table override not set",
            None,
            false,
            None,
            None,
        ),
    ];

    for (case_name, schema_value, override_set, table_window, expected_window) in cases {
        let builder = VersionControlBuilder::new();
        let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
        let region_id = builder.region_id();
        let table_id = region_id.table_id();
        schema_metadata_manager
            .register_region_table_info(
                table_id,
                "t",
                "c",
                "s",
                schema_value,
                kv_backend.clone(),
            )
            .await;

        let version_control = Arc::new(builder.build());
        let mut region_opts = version_control.current().version.options.clone();
        region_opts.compaction_override = override_set;
        if let Some(window) = table_window {
            let crate::region::options::CompactionOptions::Twcs(twcs) =
                &mut region_opts.compaction;
            twcs.time_window = Some(window);
        }

        let (opts, _) = find_dynamic_options(region_id, &region_opts, &schema_metadata_manager)
            .await
            .unwrap();
        match opts {
            crate::region::options::CompactionOptions::Twcs(t) => {
                assert_eq!(t.time_window_seconds(), expected_window, "{case_name}");
            }
        }
    }
}

#[tokio::test]
async fn test_schedule_empty() {
    let env = SchedulerEnv::new().await;
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let mut builder = VersionControlBuilder::new();
    let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
    schema_metadata_manager
        .register_region_table_info(
            builder.region_id().table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            kv_backend,
        )
        .await;
    // Nothing to compact.
    let version_control = Arc::new(builder.build());
    let (output_tx, output_rx) = oneshot::channel();
    let waiter = OptionOutputTx::from(output_tx);
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let scheduled = scheduler
        .schedule_compaction(
            builder.region_id(),
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            waiter,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();
    assert!(scheduled);
    let finished = recv_compaction_pick_finished(&mut rx).await;
    assert!(matches!(&finished.result, CompactionPlanningResult::NoPlan));
    scheduler
        .handle_compaction_pick_finished(
            finished,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .await;
    let output = output_rx.await.unwrap().unwrap();
    assert_eq!(output, 0);
    assert!(scheduler.region_status.is_empty());

    // Only one file, picker won't compact it.
    let version_control = Arc::new(builder.push_l0_file(0, 1000).build());
    let (output_tx, output_rx) = oneshot::channel();
    let waiter = OptionOutputTx::from(output_tx);
    let scheduled = scheduler
        .schedule_compaction(
            builder.region_id(),
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            waiter,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();
    assert!(scheduled);
    let finished = recv_compaction_pick_finished(&mut rx).await;
    assert!(matches!(&finished.result, CompactionPlanningResult::NoPlan));
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;
    let output = output_rx.await.unwrap().unwrap();
    assert_eq!(output, 0);
    assert!(scheduler.region_status.is_empty());
}

#[tokio::test]
async fn test_schedule_compaction_returns_true_when_task_scheduled() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let mut builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let end = 1000 * 1000;
    // Five overlapping L0 files are enough for the regular picker to create a task.
    let version_control = Arc::new(
        builder
            .push_l0_file(0, end)
            .push_l0_file(10, end)
            .push_l0_file(50, end)
            .push_l0_file(80, end)
            .push_l0_file(90, end)
            .build(),
    );
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
    schema_metadata_manager
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            kv_backend,
        )
        .await;

    let scheduled = scheduler
        .schedule_compaction(
            region_id,
            Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::none(),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();

    // The boolean result is what the worker uses to decide whether to update
    // last_schedule_compaction_millis.
    assert!(scheduled);
    assert_eq!(0, job_scheduler.num_jobs());
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;
    assert_eq!(1, job_scheduler.num_jobs());
    assert!(scheduler.region_status.contains_key(&region_id));
}



#[tokio::test]
async fn test_planning_error_completion_clears_picking_and_notifies_waiter_once() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    let (waiter_tx, waiter_rx) = oneshot::channel();
    let mut status =
        CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
    status.merge_waiter(OptionOutputTx::from(waiter_tx));
    status.start_picking(9);
    scheduler.region_status.insert(region_id, status);

    let pending_ddls = scheduler
        .handle_compaction_pick_finished(
            CompactionPickFinished {
                region_id,
                plan_id: 9,
                version_control,
                result: CompactionPlanningResult::Error(Arc::new(
                    InvalidSchedulerStateSnafu.build(),
                )),
            },
            &manifest_ctx,
            schema_metadata_manager,
        )
        .await;

    assert!(pending_ddls.is_empty());
    assert!(waiter_rx.await.unwrap().is_err());
    assert!(!scheduler.region_status.contains_key(&region_id));
}

#[tokio::test]
async fn test_planning_panic_still_sends_pick_finished() {
    let builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let (tx, mut rx) = mpsc::channel(4);

    CompactionScheduler::notify_planning_result(region_id, 7, version_control, tx, async {
        panic!("planning boom")
    })
    .await;

    let finished = recv_compaction_pick_finished(&mut rx).await;
    assert_eq!(region_id, finished.region_id);
    assert_eq!(7, finished.plan_id);
    let CompactionPlanningResult::Error(err) = finished.result else {
        panic!("expected planning error, got {:?}", finished.result);
    };
    assert!(
        err.to_string().contains("planning boom"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_picking_lifecycle_close_drop_truncate_fail_waiters_and_ignore_completion() {
    for lifecycle in ["close", "drop", "truncate"] {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        let (manual_tx, manual_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
        status.merge_waiter(OptionOutputTx::from(first_tx));
        status.set_pending_request(PendingCompaction {
            options: compact_request::Options::StrictWindow(StrictWindow {
                window_seconds: 60,
            }),
            waiter: OptionOutputTx::from(manual_tx),
            max_parallelism: 1,
        });
        status.start_picking(7);
        status.merge_regular_trigger(OptionOutputTx::from(second_tx));
        scheduler.region_status.insert(region_id, status);

        match lifecycle {
            "close" => scheduler.on_region_closed(region_id),
            "drop" => scheduler.on_region_dropped(region_id),
            "truncate" => scheduler.on_region_truncated(region_id),
            _ => unreachable!(),
        }

        assert!(!scheduler.region_status.contains_key(&region_id));
        for result in [
            first_rx.await.unwrap(),
            second_rx.await.unwrap(),
            manual_rx.await.unwrap(),
        ] {
            assert_compaction_lifecycle_error(result.unwrap_err(), lifecycle);
        }
        let pending_ddls = scheduler
            .accept_compaction_pick_finished(
                CompactionPickFinished {
                    region_id,
                    plan_id: 7,
                    version_control: version_control.clone(),
                    result: CompactionPlanningResult::NoPlan,
                },
                &version_control,
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await;
        assert!(pending_ddls.is_empty());
        assert!(!scheduler.region_status.contains_key(&region_id));
    }
}




#[tokio::test]
async fn test_ddl_fence_prevents_repeated_regular_followups() {
    let env = SchedulerEnv::new().await;
    let (tx, mut rx) = mpsc::channel(8);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    let (current_tx, current_rx) = oneshot::channel();
    let mut status =
        CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
    status.merge_waiter(OptionOutputTx::from(current_tx));
    status.start_picking(7);
    scheduler.region_status.insert(region_id, status);

    let (pre_fence_tx, pre_fence_rx) = oneshot::channel();
    assert!(
        !scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                OptionOutputTx::from(pre_fence_tx),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
            )
            .unwrap()
    );
    let (ddl_tx, mut ddl_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(ddl_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    let pending_ddls = scheduler
        .accept_compaction_pick_finished(
            CompactionPickFinished {
                region_id,
                plan_id: 7,
                version_control: version_control.clone(),
                result: CompactionPlanningResult::NoPlan,
            },
            &version_control,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .await;
    assert!(pending_ddls.is_empty());
    assert_eq!(current_rx.await.unwrap().unwrap(), 0);
    assert_matches!(ddl_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));

    let mut followup_finished = tokio::time::timeout(
        Duration::from_secs(5),
        recv_compaction_pick_finished(&mut rx),
    )
    .await
    .expect("pre-fence regular follow-up was not planned");
    let mut postfence_waiters = Vec::new();
    for _ in 0..3 {
        let (waiter_tx, waiter_rx) = oneshot::channel();
        assert!(
            !scheduler
                .schedule_compaction(
                    region_id,
                    compact_request::Options::Regular(Default::default()),
                    &version_control,
                    &env.access_layer,
                    OptionOutputTx::from(waiter_tx),
                    &manifest_ctx,
                    schema_metadata_manager.clone(),
                    1,
                )
                .unwrap()
        );
        postfence_waiters.push(waiter_rx);
    }
    assert!(
        !scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                OptionOutputTx::none(),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
            )
            .unwrap()
    );

    followup_finished.result = CompactionPlanningResult::NoPlan;
    let pending_ddls = scheduler
        .accept_compaction_pick_finished(
            followup_finished,
            &version_control,
            &manifest_ctx,
            schema_metadata_manager,
        )
        .await;
    assert_eq!(pending_ddls.len(), 1);
    assert_eq!(pre_fence_rx.await.unwrap().unwrap(), 0);
    for waiter in postfence_waiters {
        assert_eq!(waiter.await.unwrap().unwrap(), 0);
    }
    assert!(!scheduler.region_status.contains_key(&region_id));
    assert!(rx.try_recv().is_err());
    assert_matches!(ddl_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
}

#[tokio::test]
async fn test_picking_lifecycle_scheduler_drop_notifies_pending_requests() {
    let env = SchedulerEnv::new().await;
    let (waiter_rx, manual_rx, ddl_rx) = {
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let (waiter_tx, waiter_rx) = oneshot::channel();
        let (manual_tx, manual_rx) = oneshot::channel();
        let (ddl_tx, ddl_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control, env.access_layer.clone());
        status.merge_waiter(OptionOutputTx::from(waiter_tx));
        status.set_pending_request(PendingCompaction {
            options: compact_request::Options::StrictWindow(StrictWindow {
                window_seconds: 60,
            }),
            waiter: OptionOutputTx::from(manual_tx),
            max_parallelism: 1,
        });
        status.pending_ddl_requests.push(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(ddl_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });
        status.start_picking(7);
        scheduler.region_status.insert(region_id, status);
        (waiter_rx, manual_rx, ddl_rx)
    };

    for result in [
        waiter_rx.await.unwrap(),
        manual_rx.await.unwrap(),
        ddl_rx.await.unwrap(),
    ] {
        assert_compaction_lifecycle_error(result.unwrap_err(), "close");
    }
}


#[tokio::test]
async fn test_pick_result_mismatched_token_keeps_status_and_waiter_untouched() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let version_control = compactable_version();
    let region_id = version_control.current().version.metadata.region_id;
    let (mut finished, manifest_ctx, schema_metadata_manager) =
        begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
    let (waiter_tx, mut waiter_rx) = oneshot::channel();
    scheduler
        .region_status
        .get_mut(&region_id)
        .unwrap()
        .merge_waiter(OptionOutputTx::from(waiter_tx));
    finished.plan_id += 1;

    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;

    assert_eq!(job_scheduler.num_jobs(), 0);
    assert!(scheduler.region_status[&region_id].is_busy());
    assert_eq!(scheduler.region_status[&region_id].waiters.len(), 1);
    assert_matches!(
        waiter_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
}



#[tokio::test]
async fn test_pick_result_accepts_unrelated_concurrent_flush() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let version_control = compactable_version();
    let (finished, manifest_ctx, schema_metadata_manager) =
        begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
    let selected = selected_files(&finished);
    apply_edit(
        &version_control,
        &[(2_000_000, 3_000_000)],
        &[],
        selected[0].file_purger(),
    );

    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;

    assert_eq!(job_scheduler.num_jobs(), 1);
    assert!(selected.iter().all(FileHandle::compacting));
}

#[tokio::test]
async fn test_pick_result_rejects_removed_selected_file_cleanly() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let version_control = compactable_version();
    let (finished, manifest_ctx, schema_metadata_manager) =
        begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
    let selected = selected_files(&finished);
    let (waiter_tx, waiter_rx) = oneshot::channel();
    let region_id = finished.region_id;
    scheduler
        .region_status
        .get_mut(&region_id)
        .unwrap()
        .merge_waiter(OptionOutputTx::from(waiter_tx));
    apply_edit(
        &version_control,
        &[],
        &[selected[0].meta_ref().clone()],
        selected[0].file_purger(),
    );

    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;

    assert_eq!(job_scheduler.num_jobs(), 0);
    assert_eq!(waiter_rx.await.unwrap().unwrap(), 0);
    assert!(!scheduler.region_status.contains_key(&region_id));
}

#[tokio::test]
async fn test_pick_result_refreshes_replaced_selected_file() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let version_control = compactable_version();
    let (finished, manifest_ctx, schema_metadata_manager) =
        begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
    let selected = selected_files(&finished);
    let stale = selected[0].clone();
    let mut replacement = stale.meta_ref().clone();
    replacement.index_version = 1;
    replacement.index_file_size = 128;
    version_control.apply_edit(
        Some(crate::manifest::action::RegionEdit {
            files_to_add: vec![replacement],
            files_to_remove: Vec::new(),
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        }),
        &[],
        stale.file_purger(),
    );
    let current = version_control
        .current()
        .version
        .ssts
        .file_for_compaction(&stale)
        .unwrap()
        .clone();

    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;

    assert_eq!(job_scheduler.num_jobs(), 1);
    assert!(!stale.compacting());
    assert!(current.compacting());
    assert_eq!(current.meta_ref().index_version, 1);
}

#[tokio::test]
async fn test_pick_result_rejects_deleted_or_compacting_current_file() {
    for deleted in [true, false] {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version_control = compactable_version();
        let (finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        let selected = selected_files(&finished);
        if deleted {
            selected[0].mark_deleted();
        } else {
            selected[0].set_compacting(true);
        }

        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;

        assert_eq!(job_scheduler.num_jobs(), 0);
        assert!(scheduler.region_status.is_empty());
        if !deleted {
            assert!(selected[0].compacting());
            selected[0].set_compacting(false);
        }
    }
}

#[tokio::test]
async fn test_pick_result_local_submission_failure_releases_and_notifies_once() {
    let env = SchedulerEnv::new()
        .await
        .scheduler(Arc::new(FailingScheduler));
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let version_control = compactable_version();
    let region_id = version_control.current().version.metadata.region_id;
    let (finished, manifest_ctx, schema_metadata_manager) =
        begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
    let selected = selected_files(&finished);
    let (waiter_tx, waiter_rx) = oneshot::channel();
    scheduler
        .region_status
        .get_mut(&region_id)
        .unwrap()
        .merge_waiter(OptionOutputTx::from(waiter_tx));

    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;

    assert!(waiter_rx.await.unwrap().is_err());
    assert!(selected.iter().all(|file| !file.compacting()));
    assert!(!scheduler.region_status.contains_key(&region_id));
}




#[tokio::test]
async fn test_pick_result_remote_submission_failure_releases_and_notifies_once() {
    let env = SchedulerEnv::new().await;
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    scheduler
        .plugins
        .insert::<RemoteJobSchedulerRef>(Arc::new(FailingRemoteScheduler));
    let version_control = compactable_version();
    let region_id = version_control.current().version.metadata.region_id;
    let (mut finished, manifest_ctx, schema_metadata_manager) =
        begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
    use_remote_compaction(&mut finished);
    let selected = selected_files(&finished);
    let (waiter_tx, waiter_rx) = oneshot::channel();
    scheduler
        .region_status
        .get_mut(&region_id)
        .unwrap()
        .merge_waiter(OptionOutputTx::from(waiter_tx));

    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;

    assert!(waiter_rx.await.unwrap().is_err());
    assert!(selected.iter().all(|file| !file.compacting()));
    assert!(!scheduler.region_status.contains_key(&region_id));
}

#[tokio::test]
async fn test_local_reservations_survive_status_removal_until_execution_drops() {
    let selected;
    {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version_control = compactable_version();
        let region_id = version_control.current().version.metadata.region_id;
        let (finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        selected = selected_files(&finished);
        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;
        assert!(selected.iter().all(FileHandle::compacting));

        scheduler.on_region_closed(region_id);

        assert!(selected.iter().all(FileHandle::compacting));
    }
    assert!(selected.iter().all(|file| !file.compacting()));
}

#[tokio::test]
async fn test_remote_reservations_survive_status_removal_until_notifier_drops() {
    let env = SchedulerEnv::new().await;
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let remote_scheduler = Arc::new(HoldingRemoteScheduler::default());
    scheduler
        .plugins
        .insert::<RemoteJobSchedulerRef>(remote_scheduler.clone());
    let version_control = compactable_version();
    let region_id = version_control.current().version.metadata.region_id;
    let (mut finished, manifest_ctx, schema_metadata_manager) =
        begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
    use_remote_compaction(&mut finished);
    let selected = selected_files(&finished);
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;
    assert!(selected.iter().all(FileHandle::compacting));

    scheduler.on_region_closed(region_id);

    assert!(selected.iter().all(FileHandle::compacting));
    remote_scheduler.drop_notifier();
    assert!(selected.iter().all(|file| !file.compacting()));
}

#[tokio::test]
async fn test_stale_local_success_does_not_clear_replacement_phase() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let stale_version_control = compactable_version();
    let replacement_version_control = compactable_version();
    let region_id = replacement_version_control
        .current()
        .version
        .metadata
        .region_id;
    assert!(!Arc::ptr_eq(
        &stale_version_control,
        &replacement_version_control
    ));
    let manifest_ctx = env
        .mock_manifest_context(
            replacement_version_control
                .current()
                .version
                .metadata
                .clone(),
        )
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    let stale_execution =
        CompactionExecution::for_test(stale_version_control, CompactionExecutionKind::Local);
    let mut status = CompactionStatus::new(
        region_id,
        replacement_version_control.clone(),
        env.access_layer.clone(),
    );
    status.start_local_task();
    scheduler.region_status.insert(region_id, status);
    let files_before = replacement_version_control
        .current()
        .version
        .ssts
        .owned_num_files(region_id);
    if scheduler.is_current_region_execution(
        region_id,
        &replacement_version_control,
        &stale_execution,
    ) {
        apply_edit(
            &replacement_version_control,
            &[(2_000_000, 3_000_000)],
            &[],
            crate::test_util::new_noop_file_purger(),
        );
    }

    scheduler
        .on_execution_finished(
            region_id,
            &stale_execution,
            &manifest_ctx,
            schema_metadata_manager,
        )
        .await;

    assert!(scheduler.region_status[&region_id].is_busy());
    assert_eq!(
        replacement_version_control
            .current()
            .version
            .ssts
            .owned_num_files(region_id),
        files_before
    );
}


#[tokio::test]
async fn test_stale_local_failure_does_not_remove_replacement_status_or_waiter() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let stale_version_control = compactable_version();
    let replacement_version_control = compactable_version();
    let region_id = replacement_version_control
        .current()
        .version
        .metadata
        .region_id;
    let stale_execution =
        CompactionExecution::for_test(stale_version_control, CompactionExecutionKind::Local);
    let (waiter_tx, mut waiter_rx) = oneshot::channel();
    let mut status = CompactionStatus::new(
        region_id,
        replacement_version_control,
        env.access_layer.clone(),
    );
    status.start_local_task();
    status.merge_waiter(OptionOutputTx::from(waiter_tx));
    scheduler.region_status.insert(region_id, status);

    scheduler.on_execution_failed(
        region_id,
        &stale_execution,
        Arc::new(InvalidSchedulerStateSnafu.build()),
    );

    assert!(scheduler.region_status.contains_key(&region_id));
    assert_matches!(
        waiter_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
}

#[tokio::test]
async fn test_stale_remote_success_does_not_clear_replacement_phase() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let stale_version_control = compactable_version();
    let replacement_version_control = compactable_version();
    let region_id = replacement_version_control
        .current()
        .version
        .metadata
        .region_id;
    let stale_execution =
        CompactionExecution::for_test(stale_version_control, CompactionExecutionKind::Remote);
    let manifest_ctx = env
        .mock_manifest_context(
            replacement_version_control
                .current()
                .version
                .metadata
                .clone(),
        )
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    let mut status = CompactionStatus::new(
        region_id,
        replacement_version_control.clone(),
        env.access_layer.clone(),
    );
    status.start_remote_task();
    scheduler.region_status.insert(region_id, status);
    let files_before = replacement_version_control
        .current()
        .version
        .ssts
        .owned_num_files(region_id);
    if scheduler.is_current_region_execution(
        region_id,
        &replacement_version_control,
        &stale_execution,
    ) {
        apply_edit(
            &replacement_version_control,
            &[(2_000_000, 3_000_000)],
            &[],
            crate::test_util::new_noop_file_purger(),
        );
    }

    scheduler
        .on_execution_finished(
            region_id,
            &stale_execution,
            &manifest_ctx,
            schema_metadata_manager,
        )
        .await;

    assert!(scheduler.region_status[&region_id].is_busy());
    assert_eq!(
        replacement_version_control
            .current()
            .version
            .ssts
            .owned_num_files(region_id),
        files_before
    );
}


#[tokio::test]
async fn test_schedule_compaction_skips_task_exceeding_memory_limit() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    scheduler.memory_manager = Arc::new(new_compaction_memory_manager(1024 * 1024));

    let mut builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let end = 1000 * 1000;
    let version_control = Arc::new(
        builder
            .push_l0_file_with_max_row_group_size(0, end, 1024 * 1024)
            .push_l0_file_with_max_row_group_size(10, end, 1024 * 1024)
            .push_l0_file_with_max_row_group_size(50, end, 1024 * 1024)
            .push_l0_file_with_max_row_group_size(80, end, 1024 * 1024)
            .push_l0_file_with_max_row_group_size(90, end, 1024 * 1024)
            .build(),
    );
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
    schema_metadata_manager
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            kv_backend,
        )
        .await;
    let (output_tx, output_rx) = oneshot::channel();
    let rejected = COMPACTION_MEMORY_REJECTED.with_label_values(&["oversized"]);
    let rejected_before = rejected.get();

    let scheduled = scheduler
        .schedule_compaction(
            region_id,
            Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::from(output_tx),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();

    assert!(scheduled);
    let finished = recv_compaction_pick_finished(&mut rx).await;
    let selected = selected_files(&finished);
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;
    assert_eq!(output_rx.await.unwrap().unwrap(), 0);
    assert_eq!(rejected_before + 1, rejected.get());
    assert_eq!(0, job_scheduler.num_jobs());
    assert!(!scheduler.region_status.contains_key(&region_id));
    assert!(selected.iter().all(|file| !file.compacting()));
}

#[tokio::test]
async fn test_schedule_on_finished() {
    common_telemetry::init_default_ut_logging();
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let mut builder = VersionControlBuilder::new();
    let purger = builder.file_purger();
    let region_id = builder.region_id();

    let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
    schema_metadata_manager
        .register_region_table_info(
            builder.region_id().table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            kv_backend,
        )
        .await;

    // 5 files to compact.
    let end = 1000 * 1000;
    let version_control = Arc::new(
        builder
            .push_l0_file(0, end)
            .push_l0_file(10, end)
            .push_l0_file(50, end)
            .push_l0_file(80, end)
            .push_l0_file(90, end)
            .build(),
    );
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let scheduled = scheduler
        .schedule_compaction(
            region_id,
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::none(),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();
    // Should schedule 1 compaction.
    assert!(scheduled);
    assert_eq!(1, scheduler.region_status.len());
    assert_eq!(0, job_scheduler.num_jobs());
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(
            finished,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .await;
    assert_eq!(1, job_scheduler.num_jobs());
    let data = version_control.current();
    let file_metas: Vec<_> = data.version.ssts.levels()[0]
        .files
        .values()
        .map(|file| file.meta_ref().clone())
        .collect();

    // 5 files for next compaction and removes old files.
    apply_edit(
        &version_control,
        &[(0, end), (20, end), (40, end), (60, end), (80, end)],
        &file_metas,
        purger.clone(),
    );
    // The task is pending.
    let (tx, _rx) = oneshot::channel();
    let scheduled = scheduler
        .schedule_compaction(
            region_id,
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::new(Some(OutputTx::new(tx))),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();
    assert!(!scheduled);
    assert_eq!(1, scheduler.region_status.len());
    assert_eq!(1, job_scheduler.num_jobs());
    assert!(
        !scheduler
            .region_status
            .get(&builder.region_id())
            .unwrap()
            .waiters
            .is_empty()
    );

    // On compaction finished and schedule next compaction.
    scheduler
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    let scheduled = scheduler.schedule_next_compaction(
        region_id,
        &manifest_ctx,
        schema_metadata_manager.clone(),
    );
    assert!(scheduled);
    assert_eq!(1, scheduler.region_status.len());
    assert_eq!(1, job_scheduler.num_jobs());
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(
            finished,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .await;
    assert_eq!(2, job_scheduler.num_jobs());

    // 5 files for next compaction.
    apply_edit(
        &version_control,
        &[(0, end), (20, end), (40, end), (60, end), (80, end)],
        &[],
        purger.clone(),
    );
    let (tx, _rx) = oneshot::channel();
    // The task is pending.
    let scheduled = scheduler
        .schedule_compaction(
            region_id,
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::new(Some(OutputTx::new(tx))),
            &manifest_ctx,
            schema_metadata_manager,
            1,
        )
        .unwrap();
    assert!(!scheduled);
    assert_eq!(2, job_scheduler.num_jobs());
    assert!(
        !scheduler
            .region_status
            .get(&builder.region_id())
            .unwrap()
            .waiters
            .is_empty()
    );
}


#[tokio::test]
async fn test_finished_compaction_idle_status_blocks_scheduling_until_removed() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let mut builder = VersionControlBuilder::new();
    let region_id = builder.region_id();

    let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
    schema_metadata_manager
        .register_region_table_info(
            builder.region_id().table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            kv_backend,
        )
        .await;

    let end = 1000 * 1000;
    let version_control = Arc::new(
        builder
            .push_l0_file(0, end)
            .push_l0_file(10, end)
            .push_l0_file(50, end)
            .push_l0_file(80, end)
            .push_l0_file(90, end)
            .build(),
    );
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;

    // Schedules a compaction.
    let scheduled = scheduler
        .schedule_compaction(
            region_id,
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::none(),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();
    assert!(scheduled);

    // Picks a plan and submits the job.
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(
            finished,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .await;
    assert_eq!(1, job_scheduler.num_jobs());

    // The compaction finishes with no pending requests, leaving an idle
    // status behind.
    let pending_ddls = scheduler
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    assert!(pending_ddls.is_empty());
    assert!(scheduler.region_status.contains_key(&region_id));
    assert!(!scheduler.is_compacting(region_id));

    // The idle status swallows future compaction triggers.
    let scheduled = scheduler
        .schedule_compaction(
            region_id,
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::none(),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();
    assert!(!scheduled);
    assert_eq!(1, job_scheduler.num_jobs());

    // Removing the idle status lets the region schedule compactions again.
    scheduler.remove_idle_status(region_id);
    let scheduled = scheduler
        .schedule_compaction(
            region_id,
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::none(),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();
    assert!(scheduled);
}


#[tokio::test]
async fn test_manual_compaction_when_compaction_in_progress() {
    common_telemetry::init_default_ut_logging();
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let mut builder = VersionControlBuilder::new();
    let purger = builder.file_purger();
    let region_id = builder.region_id();

    let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
    schema_metadata_manager
        .register_region_table_info(
            builder.region_id().table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            kv_backend,
        )
        .await;

    // 5 files to compact.
    let end = 1000 * 1000;
    let version_control = Arc::new(
        builder
            .push_l0_file(0, end)
            .push_l0_file(10, end)
            .push_l0_file(50, end)
            .push_l0_file(80, end)
            .push_l0_file(90, end)
            .build(),
    );
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;

    let file_metas: Vec<_> = version_control.current().version.ssts.levels()[0]
        .files
        .values()
        .map(|file| file.meta_ref().clone())
        .collect();

    // 5 files for next compaction and removes old files.
    apply_edit(
        &version_control,
        &[(0, end), (20, end), (40, end), (60, end), (80, end)],
        &file_metas,
        purger.clone(),
    );

    scheduler
        .schedule_compaction(
            region_id,
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::none(),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();
    // Should schedule 1 compaction.
    assert_eq!(1, scheduler.region_status.len());
    assert_eq!(0, job_scheduler.num_jobs());
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(
            finished,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .await;
    assert_eq!(1, job_scheduler.num_jobs());
    assert!(
        scheduler
            .region_status
            .get(&region_id)
            .unwrap()
            .pending_request
            .is_none()
    );

    // Schedule another manual compaction.
    let (tx, _rx) = oneshot::channel();
    scheduler
        .schedule_compaction(
            region_id,
            compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
            &version_control,
            &env.access_layer,
            OptionOutputTx::new(Some(OutputTx::new(tx))),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
        )
        .unwrap();
    assert_eq!(1, scheduler.region_status.len());
    // Current job num should be 1 since compaction is in progress.
    assert_eq!(1, job_scheduler.num_jobs());
    let status = scheduler.region_status.get(&builder.region_id()).unwrap();
    assert!(status.pending_request.is_some());

    // On compaction finished and schedule next compaction.
    scheduler
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    assert_eq!(1, scheduler.region_status.len());
    assert_eq!(1, job_scheduler.num_jobs());
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(
            finished,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .await;
    assert_eq!(2, job_scheduler.num_jobs());

    let status = scheduler.region_status.get(&builder.region_id()).unwrap();
    assert!(status.pending_request.is_none());
}

#[tokio::test]
async fn test_compaction_bypass_in_staging_mode() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);

    // Create version control and manifest context for staging mode
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = version_control.current().version.metadata.region_id;

    // Create staging manifest context using the same pattern as SchedulerEnv
    let staging_manifest_ctx = {
        let manager = RegionManifestManager::new(
            version_control.current().version.metadata.clone(),
            0,
            RegionManifestOptions {
                manifest_dir: "".to_string(),
                object_store: env.access_layer.object_store().clone(),
                compress_type: CompressionType::Uncompressed,
                checkpoint_distance: 10,
                remove_file_options: Default::default(),
                manifest_cache: None,
            },
            FormatType::PrimaryKey,
            &Default::default(),
        )
        .await
        .unwrap();
        Arc::new(ManifestContext::new(
            manager,
            RegionRoleState::Leader(RegionLeaderState::Staging),
            None,
        ))
    };

    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

    // Test regular compaction bypass in staging mode
    let (tx, rx) = oneshot::channel();
    scheduler
        .schedule_compaction(
            region_id,
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::new(Some(OutputTx::new(tx))),
            &staging_manifest_ctx,
            schema_metadata_manager,
            1,
        )
        .unwrap();

    let result = rx.await.unwrap();
    assert_eq!(result.unwrap(), 0); // is there a better way to check this?
    assert_eq!(0, scheduler.region_status.len());
}

#[tokio::test]
async fn test_add_ddl_request_to_pending() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();

    scheduler.region_status.insert(
        region_id,
        CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
    );
    scheduler
        .region_status
        .get_mut(&region_id)
        .unwrap()
        .start_local_task();

    let (output_tx, _output_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(output_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    assert!(scheduler.has_pending_ddls(region_id));
}

#[tokio::test]
async fn test_pending_ddl_rejects_later_manual_compaction() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

    let mut status =
        CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
    status.start_local_task();
    scheduler.region_status.insert(region_id, status);

    let (first_manual_tx, mut first_manual_rx) = oneshot::channel();
    assert!(
        !scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
                &version_control,
                &env.access_layer,
                OptionOutputTx::from(first_manual_tx),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
            )
            .unwrap()
    );

    let (ddl_tx, _ddl_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(ddl_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    let (later_manual_tx, mut later_manual_rx) = oneshot::channel();
    assert!(
        !scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::StrictWindow(StrictWindow {
                    window_seconds: 120,
                }),
                &version_control,
                &env.access_layer,
                OptionOutputTx::from(later_manual_tx),
                &manifest_ctx,
                schema_metadata_manager,
                1,
            )
            .unwrap()
    );

    let later_result = later_manual_rx
        .try_recv()
        .expect("manual compaction queued after DDL was not rejected");
    assert_matches!(later_result.unwrap_err(), Error::CompactionCancelled { .. });
    assert_matches!(
        first_manual_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
    let pending_request = scheduler.region_status[&region_id]
        .pending_request
        .as_ref()
        .expect("manual compaction queued before DDL was removed");
    assert_matches!(
        &pending_request.options,
        compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 })
    );
}

#[tokio::test]
async fn test_request_cancel_state_transitions() {
    let env = SchedulerEnv::new().await;
    let builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let mut status =
        CompactionStatus::new(region_id, version_control, env.access_layer.clone());
    let state = status.start_local_task();

    assert_eq!(status.request_cancel(), RequestCancelResult::CancelIssued);
    assert!(state.cancel_handle().is_cancelled());
    assert_eq!(
        status.request_cancel(),
        RequestCancelResult::AlreadyCancelling
    );

    assert!(!state.mark_commit_started());
    assert_eq!(
        status.request_cancel(),
        RequestCancelResult::AlreadyCancelling
    );

    assert!(status.clear_running_task());
    assert_eq!(status.request_cancel(), RequestCancelResult::NotRunning);
}

#[tokio::test]
async fn test_request_cancel_remote_compaction_is_too_late() {
    let env = SchedulerEnv::new().await;
    let builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let mut status =
        CompactionStatus::new(region_id, version_control, env.access_layer.clone());

    status.start_remote_task();

    assert_eq!(
        status.request_cancel(),
        RequestCancelResult::TooLateToCancel
    );
    assert!(status.is_busy());
}

#[tokio::test]
async fn test_on_compaction_cancelled_returns_pending_ddl_requests() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();
    let _manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (_schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

    let (regular_tx, regular_rx) = oneshot::channel();
    let mut status =
        CompactionStatus::new(region_id, version_control, env.access_layer.clone());
    status.start_picking(7);
    status.merge_regular_trigger(OptionOutputTx::from(regular_tx));
    status.start_local_task();
    scheduler.region_status.insert(region_id, status);

    let (output_tx, _output_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(output_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    let pending_ddls = scheduler.on_compaction_cancelled(region_id).await;

    assert_eq!(pending_ddls.len(), 1);
    assert!(!scheduler.has_pending_ddls(region_id));
    assert!(!scheduler.region_status.contains_key(&region_id));
    assert_eq!(job_scheduler.num_jobs(), 0);
    assert!(regular_rx.await.unwrap().is_err());
}

#[tokio::test]
async fn test_on_compaction_cancelled_prioritizes_pending_ddls_over_pending_compaction() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();
    let _manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (_schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

    scheduler.region_status.insert(
        region_id,
        CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
    );
    let status = scheduler.region_status.get_mut(&region_id).unwrap();
    status.start_local_task();
    let (manual_tx, manual_rx) = oneshot::channel();
    status.set_pending_request(PendingCompaction {
        options: compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
        waiter: OptionOutputTx::from(manual_tx),
        max_parallelism: 1,
    });

    let (output_tx, _output_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(output_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    let pending_ddls = scheduler.on_compaction_cancelled(region_id).await;

    assert_eq!(pending_ddls.len(), 1);
    assert!(!scheduler.region_status.contains_key(&region_id));
    assert_eq!(job_scheduler.num_jobs(), 0);
    assert_matches!(manual_rx.await.unwrap(), Err(_));
}

#[tokio::test]
async fn test_pending_ddl_request_failed_on_compaction_failed() {
    let env = SchedulerEnv::new().await;
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();

    let (regular_tx, regular_rx) = oneshot::channel();
    let mut status =
        CompactionStatus::new(region_id, version_control, env.access_layer.clone());
    status.start_picking(7);
    status.merge_regular_trigger(OptionOutputTx::from(regular_tx));
    status.start_local_task();
    scheduler.region_status.insert(region_id, status);

    let (output_tx, output_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(output_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    assert!(scheduler.has_pending_ddls(region_id));
    scheduler
        .on_compaction_failed(region_id, Arc::new(RegionClosedSnafu { region_id }.build()));

    assert!(!scheduler.has_pending_ddls(region_id));
    let result = output_rx.await.unwrap();
    assert_matches!(result, Err(_));
    assert!(regular_rx.await.unwrap().is_err());
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_pending_ddl_request_failed_on_region_closed() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();

    scheduler.region_status.insert(
        region_id,
        CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
    );

    let (output_tx, output_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(output_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    assert!(scheduler.has_pending_ddls(region_id));
    scheduler.on_region_closed(region_id);

    assert!(!scheduler.has_pending_ddls(region_id));
    let result = output_rx.await.unwrap();
    assert_matches!(result, Err(_));
}

#[tokio::test]
async fn test_pending_ddl_request_failed_on_region_dropped() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();

    scheduler.region_status.insert(
        region_id,
        CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
    );

    let (output_tx, output_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(output_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    assert!(scheduler.has_pending_ddls(region_id));
    scheduler.on_region_dropped(region_id);

    assert!(!scheduler.has_pending_ddls(region_id));
    let result = output_rx.await.unwrap();
    assert_matches!(result, Err(_));
}

#[tokio::test]
async fn test_pending_ddl_request_failed_on_region_truncated() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();

    scheduler.region_status.insert(
        region_id,
        CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
    );

    let (output_tx, output_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(output_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    assert!(scheduler.has_pending_ddls(region_id));
    scheduler.on_region_truncated(region_id);

    assert!(!scheduler.has_pending_ddls(region_id));
    let result = output_rx.await.unwrap();
    assert_matches!(result, Err(_));
}

#[tokio::test]
async fn test_on_compaction_finished_returns_pending_ddl_requests() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

    scheduler.region_status.insert(
        region_id,
        CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
    );
    scheduler
        .region_status
        .get_mut(&region_id)
        .unwrap()
        .start_local_task();

    let (output_tx, _output_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(output_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    let pending_ddls = scheduler
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager)
        .await;

    assert_eq!(pending_ddls.len(), 1);
    assert!(!scheduler.has_pending_ddls(region_id));
    assert!(!scheduler.region_status.contains_key(&region_id));
    assert_eq!(job_scheduler.num_jobs(), 0);
}

#[tokio::test]
async fn test_on_compaction_finished_replays_pending_ddl_after_manual_noop() {
    let env = SchedulerEnv::new().await;
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

    let (manual_tx, manual_rx) = oneshot::channel();
    let mut status =
        CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
    status.start_local_task();
    status.set_pending_request(PendingCompaction {
        options: compact_request::Options::Regular(Default::default()),
        waiter: OptionOutputTx::from(manual_tx),
        max_parallelism: 1,
    });
    scheduler.region_status.insert(region_id, status);

    let (ddl_tx, _ddl_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(ddl_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    let pending_ddls = scheduler
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager.clone())
        .await;

    assert!(pending_ddls.is_empty());
    let finished = recv_compaction_pick_finished(&mut rx).await;
    let pending_ddls = scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;
    assert_eq!(pending_ddls.len(), 1);
    assert!(!scheduler.region_status.contains_key(&region_id));
    assert_eq!(manual_rx.await.unwrap().unwrap(), 0);
}

#[tokio::test]
async fn test_on_compaction_finished_returns_empty_when_region_absent() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

    let pending_ddls = scheduler
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager)
        .await;

    assert!(pending_ddls.is_empty());
}

#[tokio::test]
async fn test_on_compaction_finished_manual_schedule_error_cleans_status() {
    let env = SchedulerEnv::new()
        .await
        .scheduler(Arc::new(FailingScheduler));
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let mut builder = VersionControlBuilder::new();
    let end = 1000 * 1000;
    let version_control = Arc::new(
        builder
            .push_l0_file(0, end)
            .push_l0_file(10, end)
            .push_l0_file(50, end)
            .push_l0_file(80, end)
            .push_l0_file(90, end)
            .build(),
    );
    let region_id = builder.region_id();
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

    let (manual_tx, manual_rx) = oneshot::channel();
    let mut status =
        CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
    status.start_local_task();
    status.set_pending_request(PendingCompaction {
        options: compact_request::Options::Regular(Default::default()),
        waiter: OptionOutputTx::from(manual_tx),
        max_parallelism: 1,
    });
    scheduler.region_status.insert(region_id, status);

    let (ddl_tx, ddl_rx) = oneshot::channel();
    scheduler.add_ddl_request_to_pending(SenderDdlRequest {
        region_id,
        sender: OptionOutputTx::from(ddl_tx),
        request: crate::request::DdlRequest::EnterStaging(
            store_api::region_request::EnterStagingRequest {
                partition_directive:
                    store_api::region_request::StagingPartitionDirective::RejectAllWrites,
            },
        ),
    });

    let pending_ddls = scheduler
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager.clone())
        .await;

    assert!(pending_ddls.is_empty());
    let finished = recv_compaction_pick_finished(&mut rx).await;
    let pending_ddls = scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;
    assert!(pending_ddls.is_empty());
    assert!(!scheduler.region_status.contains_key(&region_id));
    assert_matches!(manual_rx.await.unwrap(), Err(_));
    assert_matches!(ddl_rx.await.unwrap(), Err(_));
}

#[tokio::test]
async fn test_on_compaction_finished_next_schedule_noop_removes_status() {
    let env = SchedulerEnv::new().await;
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let region_id = builder.region_id();
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

    scheduler.region_status.insert(
        region_id,
        CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
    );
    scheduler
        .region_status
        .get_mut(&region_id)
        .unwrap()
        .start_local_task();

    let pending_ddls = scheduler
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager)
        .await;

    assert!(pending_ddls.is_empty());
    assert!(scheduler.region_status.contains_key(&region_id));

    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    // With no compactable files, next scheduling returns false and removes
    // the status without creating a background task.
    let scheduled = scheduler.schedule_next_compaction(
        region_id,
        &manifest_ctx,
        schema_metadata_manager.clone(),
    );
    assert!(scheduled);
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;
    assert!(!scheduler.region_status.contains_key(&region_id));
}

#[tokio::test]
async fn test_on_compaction_finished_next_schedule_error_cleans_status() {
    let env = SchedulerEnv::new()
        .await
        .scheduler(Arc::new(FailingScheduler));
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let mut builder = VersionControlBuilder::new();
    let end = 1000 * 1000;
    let version_control = Arc::new(
        builder
            .push_l0_file(0, end)
            .push_l0_file(10, end)
            .push_l0_file(50, end)
            .push_l0_file(80, end)
            .push_l0_file(90, end)
            .build(),
    );
    let region_id = builder.region_id();
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

    scheduler.region_status.insert(
        region_id,
        CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
    );
    scheduler
        .region_status
        .get_mut(&region_id)
        .unwrap()
        .start_local_task();

    let pending_ddls = scheduler
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager)
        .await;

    assert!(pending_ddls.is_empty());
    assert!(scheduler.region_status.contains_key(&region_id));

    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    // The failing scheduler simulates a submit error; callers must see false.
    let scheduled = scheduler.schedule_next_compaction(
        region_id,
        &manifest_ctx,
        schema_metadata_manager.clone(),
    );
    assert!(scheduled);
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;
    assert!(!scheduler.region_status.contains_key(&region_id));
}

#[tokio::test]
async fn test_concurrent_memory_competition() {
    let manager = Arc::new(new_compaction_memory_manager(3 * 1024 * 1024)); // 3MB
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = vec![];

    // Spawn 3 tasks competing for memory, each trying to acquire 2MB
    for _i in 0..3 {
        let mgr = manager.clone();
        let bar = barrier.clone();
        let handle = tokio::spawn(async move {
            bar.wait().await; // Synchronize start
            mgr.try_acquire(2 * 1024 * 1024)
        });
        handles.push(handle);
    }

    let results: Vec<Option<CompactionMemoryGuard>> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Only 1 should succeed (3MB limit, 2MB request, can only fit one)
    let succeeded = results.iter().filter(|r| r.is_some()).count();
    let failed = results.iter().filter(|r| r.is_none()).count();

    assert_eq!(succeeded, 1, "Expected exactly 1 task to acquire memory");
    assert_eq!(failed, 2, "Expected 2 tasks to fail");

    // Clean up
    drop(results);
    assert_eq!(manager.used_bytes(), 0);
}
