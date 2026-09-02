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
use api::v1::region::compact_request::Options;
use common_datasource::compression::CompressionType;
use common_meta::key::schema_name::SchemaNameValue;
use common_time::{DatabaseTimeToLive, Timestamp};
use store_api::mito_engine_options::{
    COMPACTION_TYPE, TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM, TWCS_TRIGGER_FILE_NUM,
};
use store_api::storage::FileId;
use tokio::sync::{Barrier, mpsc, oneshot};

use crate::compaction::memory_manager::{CompactionMemoryGuard, new_compaction_memory_manager};
use crate::compaction::picker::PickerOutput;
use crate::compaction::scheduler::planning::CompactionPlanningResult;
use crate::compaction::scheduler::state::{CompactingFiles, CompactionPhase};
use crate::compaction::scheduler::*;
use crate::compaction::test_util::new_file_handle;
use crate::compaction::{CompactionOutput, find_dynamic_options};
use crate::error::InvalidSchedulerStateSnafu;
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::metrics::COMPACTION_MEMORY_REJECTED;
use crate::region::ManifestContext;
use crate::request::{BackgroundNotify, OutputTx, WorkerRequest};
use crate::schedule::remote_job_scheduler::{RemoteJob, RemoteJobSchedulerRef};
use crate::schedule::scheduler::{Job, Scheduler};
use crate::sst::FormatType;
use crate::sst::file::FileHandle;
use crate::test_util::mock_schema_metadata_manager;
use crate::test_util::scheduler_util::{SchedulerEnv, VecScheduler};
use crate::test_util::version_util::{VersionControlBuilder, apply_edit};

struct FailingScheduler;

struct FailingRemoteScheduler;

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
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    assert!(
        scheduler
            .schedule_automatic_compaction(
                Options::Regular(Default::default()),
                version_control,
                &env.access_layer,
                &manifest_ctx,
                schema_metadata_manager.clone(),
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

fn use_remote_compaction(finished: &mut CompactionPickFinished, fallback_to_local: bool) {
    let CompactionPlanningResult::Prepared(prepared) = &mut finished.result else {
        panic!("expected prepared compaction");
    };
    let crate::region::options::CompactionOptions::Twcs(options) =
        &mut prepared.compaction_region.region_options.compaction;
    options.remote_compaction = true;
    options.fallback_to_local = fallback_to_local;
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
    schema_value.extra_options.insert(
        "compaction.twcs.active_window.l1_merge_trigger".to_string(),
        "12".to_string(),
    );
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
            assert_eq!(t.active_window_l1_merge_trigger, 12);
        }
    }
}

#[tokio::test]
async fn test_find_compaction_options_db_level_prefers_canonical_trigger_alias() {
    let builder = VersionControlBuilder::new();
    let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
    let region_id = builder.region_id();
    let table_id = region_id.table_id();
    let mut schema_value = SchemaNameValue::default();
    schema_value
        .extra_options
        .insert(COMPACTION_TYPE.to_string(), "twcs".to_string());
    schema_value
        .extra_options
        .insert(TWCS_TRIGGER_FILE_NUM.to_string(), "7".to_string());
    schema_value.extra_options.insert(
        TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM.to_string(),
        "9".to_string(),
    );
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

    let crate::region::options::CompactionOptions::Twcs(twcs) = opts;
    assert_eq!(twcs.trigger_file_num, 9);
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
            .register_region_table_info(table_id, "t", "c", "s", schema_value, kv_backend.clone())
            .await;

        let version_control = Arc::new(builder.build());
        let mut region_opts = version_control.current().version.options.clone();
        region_opts.compaction_override = override_set;
        if let Some(window) = table_window {
            let crate::region::options::CompactionOptions::Twcs(twcs) = &mut region_opts.compaction;
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
        .schedule_manual_compaction(
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            waiter,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
            None,
        )
        .unwrap();
    assert!(scheduled);
    let finished = recv_compaction_pick_finished(&mut rx).await;
    assert!(matches!(&finished.result, CompactionPlanningResult::NoPlan));
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    let output = output_rx.await.unwrap().unwrap();
    assert_eq!(output, 0);
    assert!(scheduler.region_status.is_empty());

    // Only one file, picker won't compact it.
    let version_control = Arc::new(builder.push_l0_file(0, 1000).build());
    let (output_tx, output_rx) = oneshot::channel();
    let waiter = OptionOutputTx::from(output_tx);
    let scheduled = scheduler
        .schedule_manual_compaction(
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            waiter,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
            None,
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
        .schedule_automatic_compaction(
            Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            &manifest_ctx,
            schema_metadata_manager.clone(),
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
async fn test_planning_followup_reports_automatic_schedule() {
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
    let mut status =
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone());
    status.start_picking(7);
    status.mark_automatic_trigger();
    scheduler.region_status.insert(region_id, status);

    let transition = scheduler
        .handle_compaction_pick_finished(
            CompactionPickFinished {
                region_id,
                plan_id: 7,
                result: CompactionPlanningResult::NoPlan,
            },
            &manifest_ctx,
            schema_metadata_manager,
        )
        .await;

    assert_matches!(transition, CompactionTransition::AutomaticFollowupScheduled);
}

#[tokio::test]
async fn test_planning_panic_notifies_and_clears_status() {
    let env = SchedulerEnv::new().await;
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx.clone());
    let builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
    let (waiter_tx, waiter_rx) = oneshot::channel();
    let mut status =
        CompactionStatus::for_test(region_id, version_control.clone(), env.access_layer.clone());
    status.start_picking(7);
    status.merge_waiter(OptionOutputTx::from(waiter_tx));
    scheduler.region_status.insert(region_id, status);

    CompactionScheduler::notify_planning_result(region_id, 7, tx, async {
        panic!("planning boom")
    })
    .await;

    let finished = recv_compaction_pick_finished(&mut rx).await;
    let CompactionPlanningResult::Error(err) = &finished.result else {
        panic!("expected planning error, got {:?}", &finished.result);
    };
    assert!(err.to_string().contains("planning boom"));
    let pending_ddls = scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;

    assert!(pending_ddls.is_empty());
    assert!(waiter_rx.await.unwrap().is_err());
    assert!(!scheduler.region_status.contains_key(&region_id));
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
    let mut status =
        CompactionStatus::for_test(region_id, version_control.clone(), env.access_layer.clone());
    status.start_picking(7);
    scheduler.region_status.insert(region_id, status);

    let (pre_fence_tx, pre_fence_rx) = oneshot::channel();
    assert!(
        !scheduler
            .schedule_manual_compaction(
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                OptionOutputTx::from(pre_fence_tx),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
                None,
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

    let pending_ddls = scheduler
        .handle_compaction_pick_finished(
            CompactionPickFinished {
                region_id,
                plan_id: 7,
                result: CompactionPlanningResult::NoPlan,
            },
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .await;
    assert!(pending_ddls.is_empty());

    let mut followup_finished = tokio::time::timeout(
        Duration::from_secs(5),
        recv_compaction_pick_finished(&mut rx),
    )
    .await
    .expect("pre-fence regular follow-up was not planned");
    let (post_fence_tx, post_fence_rx) = oneshot::channel();
    assert!(
        !scheduler
            .schedule_manual_compaction(
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                OptionOutputTx::from(post_fence_tx),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
                None,
            )
            .unwrap()
    );
    assert_matches!(
        post_fence_rx.await.unwrap().unwrap_err(),
        Error::CompactionCancelled { .. }
    );

    followup_finished.result = CompactionPlanningResult::NoPlan;
    let pending_ddls = scheduler
        .handle_compaction_pick_finished(followup_finished, &manifest_ctx, schema_metadata_manager)
        .await;
    assert_eq!(pending_ddls.len(), 1);
    assert_eq!(pre_fence_rx.await.unwrap().unwrap(), 0);
    assert!(!scheduler.region_status.contains_key(&region_id));
    assert!(rx.try_recv().is_err());
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
    assert_eq!(scheduler.region_status[&region_id].active.waiters.len(), 1);
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
    use_remote_compaction(&mut finished, false);
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
async fn test_remote_fallback_uses_new_execution_plan_id() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    scheduler
        .plugins
        .insert::<RemoteJobSchedulerRef>(Arc::new(FailingRemoteScheduler));
    let version_control = compactable_version();
    let region_id = version_control.current().version.metadata.region_id;
    let (mut finished, manifest_ctx, schema_metadata_manager) =
        begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
    let remote_plan_id = finished.plan_id;
    use_remote_compaction(&mut finished, true);

    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
        .await;

    assert_eq!(job_scheduler.num_jobs(), 1);
    assert!(matches!(
        &scheduler.region_status[&region_id].active.phase,
        CompactionPhase::Local { .. }
    ));
    assert!(
        !scheduler.region_status[&region_id]
            .matches_execution(&CompactionExecution::for_test(remote_plan_id))
    );
}

#[tokio::test]
async fn test_stale_plan_execution_does_not_affect_replacement_status() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let stale_execution = CompactionExecution::for_test(1);
    let replacement_version_control = compactable_version();
    let region_id = replacement_version_control
        .current()
        .version
        .metadata
        .region_id;
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
    let (waiter_tx, mut waiter_rx) = oneshot::channel();
    let mut status = CompactionStatus::for_test(
        region_id,
        replacement_version_control,
        env.access_layer.clone(),
    );
    status.start_local_task();
    status.merge_waiter(OptionOutputTx::from(waiter_tx));
    scheduler.region_status.insert(region_id, status);

    let pending_ddls = scheduler
        .on_execution_finished(
            region_id,
            &stale_execution,
            &manifest_ctx,
            schema_metadata_manager,
            true,
        )
        .await;
    assert!(pending_ddls.is_empty());
    assert!(scheduler.region_status.contains_key(&region_id));
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
        .schedule_manual_compaction(
            Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::from(output_tx),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
            None,
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
async fn test_execution_finished_drains_until_no_plan() {
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
        .schedule_automatic_compaction(
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .unwrap();
    // Should schedule 1 compaction.
    assert!(scheduled);
    assert_eq!(1, scheduler.region_status.len());
    assert_eq!(0, job_scheduler.num_jobs());
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager.clone())
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
    // A completed execution that reduced the file count chains another pick even
    // without an explicit trigger, because the layout is still compactable.
    let transition = scheduler
        .on_compaction_finished(
            region_id,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            true,
        )
        .await;
    assert_matches!(transition, CompactionTransition::AutomaticFollowupScheduled);
    assert!(scheduler.region_status.contains_key(&region_id));
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    assert_eq!(2, job_scheduler.num_jobs());

    // Replace the layout with a single file: the drain must stop once the picker
    // finds nothing to do.
    let data = version_control.current();
    let file_metas: Vec<_> = data.version.ssts.levels()[0]
        .files
        .values()
        .map(|file| file.meta_ref().clone())
        .collect();
    apply_edit(&version_control, &[(0, end)], &file_metas, purger.clone());
    let transition = scheduler
        .on_compaction_finished(
            region_id,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            true,
        )
        .await;
    assert_matches!(transition, CompactionTransition::AutomaticFollowupScheduled);
    let finished = recv_compaction_pick_finished(&mut rx).await;
    let transition = scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    assert_matches!(transition, CompactionTransition::NoAction);
    assert!(scheduler.region_status.is_empty());
    assert_eq!(2, job_scheduler.num_jobs());
}

#[tokio::test]
async fn test_execution_finished_without_progress_removes_status() {
    common_telemetry::init_default_ut_logging();
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
        .schedule_automatic_compaction(
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .unwrap();
    assert!(scheduled);
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    assert_eq!(1, job_scheduler.num_jobs());

    // The execution rewrote files without reducing the file count (e.g. its output
    // was split into more files than its input). Chaining would loop without making
    // progress, so the lifecycle ends here; the next flush trigger resumes compaction.
    let transition = scheduler
        .on_compaction_finished(
            region_id,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            false,
        )
        .await;
    assert_matches!(transition, CompactionTransition::NoAction);
    assert!(scheduler.region_status.is_empty());
    assert_eq!(1, job_scheduler.num_jobs());
}

#[tokio::test]
async fn test_time_range_compaction_when_compaction_in_progress() {
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

    // 40 files to compact. The first task picks 32, leaving 8 for the pending request.
    let end = 1000 * 1000;
    for offset in 0..40 {
        builder.push_l0_file(offset * 10, end);
    }
    let version_control = Arc::new(builder.build());
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;

    let file_metas: Vec<_> = version_control.current().version.ssts.levels()[0]
        .files
        .values()
        .map(|file| file.meta_ref().clone())
        .collect();

    // Replaces the files before scheduling the first compaction.
    let next_files = (0..40).map(|offset| (offset * 20, end)).collect::<Vec<_>>();
    apply_edit(&version_control, &next_files, &file_metas, purger.clone());

    scheduler
        .schedule_automatic_compaction(
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .unwrap();
    // Should schedule 1 compaction.
    assert_eq!(1, scheduler.region_status.len());
    assert_eq!(0, job_scheduler.num_jobs());
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager.clone())
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

    // Schedule another manual compaction with a time range.
    let time_range = TimestampRange::new(
        Timestamp::new_millisecond(0),
        Timestamp::new_millisecond(end + 1),
    )
    .unwrap();
    let (tx, _rx) = oneshot::channel();
    scheduler
        .schedule_manual_compaction(
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::new(Some(OutputTx::new(tx))),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
            Some(time_range),
        )
        .unwrap();
    assert_eq!(1, scheduler.region_status.len());
    // Current job num should be 1 since compaction is in progress.
    assert_eq!(1, job_scheduler.num_jobs());
    let status = scheduler.region_status.get(&builder.region_id()).unwrap();
    assert_eq!(
        Some(time_range),
        status
            .pending_request
            .as_ref()
            .and_then(|pending| pending.time_range)
    );

    // On compaction finished and schedule next compaction.
    scheduler
        .on_compaction_finished(
            region_id,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            true,
        )
        .await;
    assert_eq!(1, scheduler.region_status.len());
    assert!(
        scheduler
            .region_status
            .get(&region_id)
            .unwrap()
            .is_manual_compaction()
    );
    assert_eq!(1, job_scheduler.num_jobs());
    let finished = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    assert_eq!(2, job_scheduler.num_jobs());

    let status = scheduler.region_status.get(&builder.region_id()).unwrap();
    assert!(status.pending_request.is_none());
}

#[tokio::test]
async fn test_automatic_compaction_merges_with_manual_compaction() {
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

    scheduler
        .schedule_manual_compaction(
            compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
            &version_control,
            &env.access_layer,
            OptionOutputTx::none(),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
            None,
        )
        .unwrap();
    scheduler
        .schedule_automatic_compaction(
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            &manifest_ctx,
            schema_metadata_manager,
        )
        .unwrap();

    let status = scheduler.region_status.get(&region_id).unwrap();
    assert!(status.is_manual_compaction());
    assert!(status.pending_request.is_none());
    assert!(status.active.automatic_followup_required);
}

#[tokio::test]
async fn test_manual_compaction_rejects_another_manual_compaction() {
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

    scheduler
        .schedule_manual_compaction(
            compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
            &version_control,
            &env.access_layer,
            OptionOutputTx::none(),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
            None,
        )
        .unwrap();

    let (second_tx, mut second_rx) = oneshot::channel();
    scheduler
        .schedule_manual_compaction(
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::from(second_tx),
            &manifest_ctx,
            schema_metadata_manager,
            1,
            None,
        )
        .unwrap();

    let err = second_rx
        .try_recv()
        .expect("a concurrent manual compaction should fail immediately")
        .expect_err("a concurrent manual compaction should return an error");
    assert_matches!(
        err,
        crate::error::Error::ManualCompactionAlreadyRunning {
            region_id: running_region_id
        } if running_region_id == region_id
    );
    assert!(
        scheduler
            .region_status
            .get(&region_id)
            .unwrap()
            .pending_request
            .is_none()
    );
}

#[tokio::test]
async fn test_ranged_compaction_stops_without_followup() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);

    let mut builder = VersionControlBuilder::new();
    for offset in [0, 10, 20, 30] {
        builder.push_l0_file(offset, 1_000);
    }
    for offset in [0, 10, 20, 30] {
        builder.push_l0_file(2 * 3_600_000 + offset, 2 * 3_600_000 + 1_000);
    }
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
    let mut schema_value = SchemaNameValue::default();
    schema_value
        .extra_options
        .insert("compaction.type".to_string(), "twcs".to_string());
    schema_value
        .extra_options
        .insert("compaction.twcs.time_window".to_string(), "1h".to_string());
    schema_metadata_manager
        .register_region_table_info(
            region_id.table_id(),
            "t",
            "c",
            "s",
            Some(schema_value),
            kv_backend,
        )
        .await;
    let time_range = TimestampRange::new(
        Timestamp::new_millisecond(0),
        Timestamp::new_millisecond(3_600_000),
    )
    .unwrap();

    scheduler
        .schedule_manual_compaction(
            compact_request::Options::StrictWindow(StrictWindow {
                window_seconds: 3_600,
            }),
            &version_control,
            &env.access_layer,
            OptionOutputTx::none(),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
            Some(time_range),
        )
        .unwrap();
    let first = recv_compaction_pick_finished(&mut rx).await;
    assert!(
        selected_files(&first)
            .iter()
            .all(|file| file.time_range().1 < Timestamp::new_millisecond(3_600_000))
    );
    scheduler
        .handle_compaction_pick_finished(first, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    assert_eq!(1, job_scheduler.num_jobs());

    scheduler
        .on_compaction_finished(
            region_id,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            true,
        )
        .await;
    // The execution reduced the file count and the remaining window is still
    // compactable, so the scheduler drains it with an unrestricted follow-up.
    assert!(scheduler.region_status.contains_key(&region_id));
    let followup = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(followup, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    assert_eq!(2, job_scheduler.num_jobs());
}

#[tokio::test]
async fn test_automatic_trigger_during_execution_clears_continuation_scope() {
    let job_scheduler = Arc::new(VecScheduler::default());
    let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);

    let mut builder = VersionControlBuilder::new();
    for offset in [0, 10, 20, 30] {
        builder.push_l0_file(offset, 1_000);
    }
    for offset in [0, 10, 20, 30] {
        builder.push_l0_file(2 * 3_600_000 + offset, 2 * 3_600_000 + 1_000);
    }
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let manifest_ctx = env
        .mock_manifest_context(version_control.current().version.metadata.clone())
        .await;
    let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
    let mut schema_value = SchemaNameValue::default();
    schema_value
        .extra_options
        .insert("compaction.type".to_string(), "twcs".to_string());
    schema_value
        .extra_options
        .insert("compaction.twcs.time_window".to_string(), "1h".to_string());
    schema_metadata_manager
        .register_region_table_info(
            region_id.table_id(),
            "t",
            "c",
            "s",
            Some(schema_value),
            kv_backend,
        )
        .await;
    let time_range = TimestampRange::new(
        Timestamp::new_millisecond(0),
        Timestamp::new_millisecond(3_600_000),
    )
    .unwrap();

    scheduler
        .schedule_manual_compaction(
            compact_request::Options::StrictWindow(StrictWindow {
                window_seconds: 3_600,
            }),
            &version_control,
            &env.access_layer,
            OptionOutputTx::none(),
            &manifest_ctx,
            schema_metadata_manager.clone(),
            1,
            Some(time_range),
        )
        .unwrap();
    let first = recv_compaction_pick_finished(&mut rx).await;
    scheduler
        .handle_compaction_pick_finished(first, &manifest_ctx, schema_metadata_manager.clone())
        .await;
    // The manual compaction is now in the local execution phase.
    assert_eq!(1, job_scheduler.num_jobs());

    // An unrestricted automatic trigger (e.g. caused by a flush) arrives during execution.
    scheduler
        .schedule_automatic_compaction(
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            &manifest_ctx,
            schema_metadata_manager.clone(),
        )
        .unwrap();

    // Finishing the manual execution must chain an unrestricted follow-up instead of
    // continuing with the manual request's time range.
    scheduler
        .on_compaction_finished(
            region_id,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            true,
        )
        .await;

    let continuation = recv_compaction_pick_finished(&mut rx).await;
    let CompactionPlanningResult::Prepared(prepared) = continuation.result else {
        panic!("expected the unrestricted follow-up to select out-of-range windows");
    };
    assert!(
        prepared
            .picker_output
            .outputs
            .iter()
            .flat_map(|output| &output.inputs)
            .any(|file| file.time_range().1 >= Timestamp::new_millisecond(2 * 3_600_000))
    );
}

#[tokio::test]
async fn test_compaction_bypass_in_staging_mode() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);

    // Create version control and manifest context for staging mode
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());

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
        .schedule_manual_compaction(
            compact_request::Options::Regular(Default::default()),
            &version_control,
            &env.access_layer,
            OptionOutputTx::new(Some(OutputTx::new(tx))),
            &staging_manifest_ctx,
            schema_metadata_manager,
            1,
            None,
        )
        .unwrap();

    let result = rx.await.unwrap();
    assert_eq!(result.unwrap(), 0);
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
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone()),
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
async fn test_pending_ddl_fences_later_compaction_triggers() {
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

    let (first_manual_tx, mut first_manual_rx) = oneshot::channel();
    let mut status =
        CompactionStatus::for_test(region_id, version_control.clone(), env.access_layer.clone());
    status.start_local_task();
    status.set_pending_request(PendingCompaction {
        options: compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
        waiter: OptionOutputTx::from(first_manual_tx),
        max_parallelism: 1,
        time_range: None,
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

    // Automatic regular triggers have no waiter and are ignored by the DDL fence.
    assert!(
        !scheduler
            .schedule_automatic_compaction(
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .unwrap()
    );
    let active = &scheduler.region_status[&region_id].active;
    assert!(active.waiters.is_empty());
    assert!(!active.automatic_followup_required);

    // Explicit regular and strict-window requests both have waiters and are rejected.
    for options in [
        compact_request::Options::Regular(Default::default()),
        compact_request::Options::StrictWindow(StrictWindow {
            window_seconds: 120,
        }),
    ] {
        let (later_tx, later_rx) = oneshot::channel();
        assert!(
            !scheduler
                .schedule_manual_compaction(
                    options,
                    &version_control,
                    &env.access_layer,
                    OptionOutputTx::from(later_tx),
                    &manifest_ctx,
                    schema_metadata_manager.clone(),
                    1,
                    None,
                )
                .unwrap()
        );
        assert_matches!(
            later_rx.await.unwrap().unwrap_err(),
            Error::CompactionCancelled { .. }
        );
    }

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
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone());
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
}

#[tokio::test]
async fn test_request_cancel_remote_compaction_is_too_late() {
    let env = SchedulerEnv::new().await;
    let builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let mut status =
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone());

    status.start_remote_task();

    assert_eq!(
        status.request_cancel(),
        RequestCancelResult::TooLateToCancel
    );
}

#[tokio::test]
async fn test_try_cancel_and_add_ddl_returns_request_when_not_running() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let region_id = RegionId::new(1, 1);
    let (ddl_tx, ddl_rx) = oneshot::channel();

    let result =
        scheduler.try_cancel_and_add_ddl(region_id, OptionOutputTx::from(ddl_tx), 42_u64, |_| {
            crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            )
        });

    let Err((sender, payload)) = result else {
        panic!("DDL was queued without a running compaction");
    };
    assert_eq!(payload, 42);
    sender.send(Ok(0));
    assert_eq!(ddl_rx.await.unwrap().unwrap(), 0);
}

#[tokio::test]
async fn test_try_cancel_and_add_ddl_cancels_and_queues_atomically() {
    let env = SchedulerEnv::new().await;
    let (tx, _rx) = mpsc::channel(4);
    let mut scheduler = env.mock_compaction_scheduler(tx);
    let builder = VersionControlBuilder::new();
    let region_id = builder.region_id();
    let version_control = Arc::new(builder.build());
    let mut status =
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone());
    status.start_picking(7);
    scheduler.region_status.insert(region_id, status);
    let (ddl_tx, _ddl_rx) = oneshot::channel();

    let result =
        scheduler.try_cancel_and_add_ddl(region_id, OptionOutputTx::from(ddl_tx), (), |_| {
            crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            )
        });

    assert!(result.is_ok());
    assert!(scheduler.has_pending_ddls(region_id));
    assert_eq!(
        scheduler.request_cancel(region_id),
        RequestCancelResult::AlreadyCancelling
    );
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

    let mut status =
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone());
    status.start_picking(7);
    status.mark_automatic_trigger();
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
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone()),
    );
    let status = scheduler.region_status.get_mut(&region_id).unwrap();
    status.start_local_task();
    let (manual_tx, manual_rx) = oneshot::channel();
    status.set_pending_request(PendingCompaction {
        options: compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
        waiter: OptionOutputTx::from(manual_tx),
        max_parallelism: 1,
        time_range: None,
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

    let mut status =
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone());
    status.start_picking(7);
    status.mark_automatic_trigger();
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
    scheduler.on_compaction_failed(region_id, Arc::new(RegionClosedSnafu { region_id }.build()));

    assert!(!scheduler.has_pending_ddls(region_id));
    let result = output_rx.await.unwrap();
    assert_matches!(result, Err(_));
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
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone()),
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
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone()),
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
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone()),
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
        CompactionStatus::for_test(region_id, version_control, env.access_layer.clone()),
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
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager, true)
        .await;

    assert_eq!(pending_ddls.len(), 1);
    assert!(!scheduler.has_pending_ddls(region_id));
    assert!(!scheduler.region_status.contains_key(&region_id));
    assert_eq!(job_scheduler.num_jobs(), 0);
}

#[tokio::test]
async fn test_planning_terminal_prioritizes_pending_ddl_over_automatic_followup() {
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
        CompactionStatus::for_test(region_id, version_control.clone(), env.access_layer.clone());
    let state = status.start_local_task();
    assert!(state.mark_commit_started());
    status.mark_automatic_trigger();
    status.set_pending_request(PendingCompaction {
        options: compact_request::Options::Regular(Default::default()),
        waiter: OptionOutputTx::from(manual_tx),
        max_parallelism: 1,
        time_range: None,
    });
    scheduler.region_status.insert(region_id, status);

    let (ddl_tx, _ddl_rx) = oneshot::channel();
    let result =
        scheduler.try_cancel_and_add_ddl(region_id, OptionOutputTx::from(ddl_tx), (), |_| {
            crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            )
        });
    assert!(result.is_ok());

    let pending_ddls = scheduler
        .on_compaction_finished(
            region_id,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            true,
        )
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
async fn test_on_compaction_finished_dispatches_pending_ddl_before_chained_regular() {
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

    // An automatic trigger was recorded while picking and the region is now
    // executing; a DDL queued behind the task must be dispatched as soon
    // as the task finishes instead of waiting for a whole extra cycle.
    let mut status =
        CompactionStatus::for_test(region_id, version_control.clone(), env.access_layer.clone());
    status.start_picking(7);
    status.mark_automatic_trigger();
    status.start_local_task();
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
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager, true)
        .await;

    assert_eq!(pending_ddls.len(), 1);
    assert!(!scheduler.region_status.contains_key(&region_id));
    assert!(rx.try_recv().is_err());
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
        .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager, true)
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
        CompactionStatus::for_test(region_id, version_control.clone(), env.access_layer.clone());
    status.start_local_task();
    status.set_pending_request(PendingCompaction {
        options: compact_request::Options::Regular(Default::default()),
        waiter: OptionOutputTx::from(manual_tx),
        max_parallelism: 1,
        time_range: None,
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
        .on_compaction_finished(
            region_id,
            &manifest_ctx,
            schema_metadata_manager.clone(),
            true,
        )
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
