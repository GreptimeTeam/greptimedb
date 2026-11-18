// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! Independent shared-L0 scheduling, reusing the ordinary scheduler and file leases.
use super::*;

// Plans and units share the existing monotonic counter. Separate ID ranges let
// existing worker notifications reach the right scheduler without wire changes.
const SHARED_PLAN: u64 = 1 << 63;

pub(super) fn is_shared_plan(id: u64) -> bool {
    id & SHARED_PLAN != 0
}

impl CompactionScheduler {
    pub(super) fn shared_scheduler(&mut self) -> &mut Self {
        if self.shared.is_none() {
            let mut child = Self::new(
                self.scheduler.clone(),
                self.request_sender.clone(),
                self.cache_manager.clone(),
                self.engine_config.clone(),
                self.listener.clone(),
                self.plugins.clone(),
                self.memory_manager.clone(),
                self.memory_policy,
            );
            child.shared_lane = true;
            child.next_plan_id = SHARED_PLAN;
            child.disable_compaction = self.disable_compaction;
            self.shared = Some(Box::new(child));
        }
        self.shared.as_mut().unwrap()
    }

    pub(super) fn shared_running(&self, region_id: RegionId) -> bool {
        self.shared
            .as_ref()
            .and_then(|s| s.region_status.get(&region_id))
            .is_some_and(CompactionStatus::is_compacting)
    }

    /// A shared publication wakes L1 selection immediately, even while other
    /// shared units remain active. A remote L1 execution coalesces this wakeup.
    pub(super) fn after_shared_event(
        &mut self,
        region_id: RegionId,
        manifest: &ManifestContextRef,
        schemas: SchemaMetadataManagerRef,
        transition: CompactionTransition,
    ) -> CompactionTransition {
        let Some(status) = self.region_status.get(&region_id) else {
            return transition;
        };
        if status.has_ddl() {
            if !status.is_compacting() && !self.shared_running(region_id) {
                return CompactionTransition::from_pending_ddls(
                    self.take_ready_ddls(region_id).unwrap_or_default(),
                );
            }
            return transition;
        }
        let version = status.version_control.clone();
        let access = status.access_layer.clone();
        if let Err(err) = self.schedule_compaction(
            CompactionTrigger::Automatic,
            compact_request::Options::Regular(Default::default()),
            &version,
            &access,
            OptionOutputTx::none(),
            manifest,
            schemas,
            crate::compaction::compactor::experiment::automatic_parallelism(),
            None,
        ) {
            warn!(err; "Failed to schedule L1 after shared publication for region {}", region_id);
        }
        transition
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::scheduler::planning::CompactionPlanningResult;
    use crate::test_util::version_util::VersionControlBuilder;
    use crate::test_util::{mock_schema_metadata_manager, scheduler_util::SchedulerEnv};
    use tokio::sync::{mpsc, oneshot};

    #[tokio::test]
    async fn shared_planning_finishes_while_remote_l1_stays_active() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(8);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version = Arc::new(VersionControlBuilder::new().build());
        let id = version.region_id();
        scheduler.register_region(&version, &env.access_layer);
        scheduler
            .region_status
            .get_mut(&id)
            .unwrap()
            .start_remote_task();
        let remote = CompactionExecution::for_test(0);
        let shared = scheduler.shared_scheduler();
        shared.register_region(&version, &env.access_layer);
        shared
            .region_status
            .get_mut(&id)
            .unwrap()
            .start_picking(SHARED_PLAN);
        assert!(scheduler.shared_running(id));
        assert!(scheduler.is_current_execution(id, &remote));
        let manifest = env
            .mock_manifest_context(version.current().version.metadata.clone())
            .await;
        let (schemas, _) = mock_schema_metadata_manager();
        scheduler
            .handle_compaction_pick_finished(
                CompactionPickFinished {
                    region_id: id,
                    plan_id: SHARED_PLAN,
                    result: CompactionPlanningResult::NoPlan,
                },
                &manifest,
                schemas,
            )
            .await;
        assert!(!scheduler.shared_running(id));
        assert!(
            scheduler.is_current_execution(id, &remote),
            "shared completion must not clear L1"
        );
    }

    #[tokio::test]
    async fn remote_completion_does_not_end_shared_work_and_close_clears_both() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(8);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version = Arc::new(VersionControlBuilder::new().build());
        let id = version.region_id();
        scheduler.register_region(&version, &env.access_layer);
        scheduler
            .region_status
            .get_mut(&id)
            .unwrap()
            .start_remote_task();
        let shared = scheduler.shared_scheduler();
        shared.register_region(&version, &env.access_layer);
        shared
            .region_status
            .get_mut(&id)
            .unwrap()
            .start_picking(SHARED_PLAN);
        let manifest = env
            .mock_manifest_context(version.current().version.metadata.clone())
            .await;
        let (schemas, _) = mock_schema_metadata_manager();
        let remote = CompactionExecution::for_test(0);
        scheduler
            .on_execution_finished(id, &remote, &manifest, schemas, false)
            .await;
        assert!(scheduler.shared_running(id));
        assert!(!scheduler.is_current_execution(id, &remote));
        scheduler.on_region_closed(id);
        assert!(!scheduler.region_status.contains_key(&id));
        assert!(
            !scheduler
                .shared
                .as_ref()
                .unwrap()
                .region_status
                .contains_key(&id)
        );
        assert!(!scheduler.can_apply_unit(id, SHARED_PLAN));
    }

    #[tokio::test]
    async fn ddl_waits_for_both_lanes_without_blocking_their_terminal_callbacks() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(8);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version = Arc::new(VersionControlBuilder::new().build());
        let id = version.region_id();
        scheduler.register_region(&version, &env.access_layer);
        scheduler
            .region_status
            .get_mut(&id)
            .unwrap()
            .start_remote_task();
        let shared = scheduler.shared_scheduler();
        shared.register_region(&version, &env.access_layer);
        shared
            .region_status
            .get_mut(&id)
            .unwrap()
            .start_picking(SHARED_PLAN);
        let (reply, _response) = oneshot::channel();
        assert!(
            scheduler
                .try_cancel_and_add_ddl(id, reply.into(), (), |_| {
                    DdlRequest::Truncate(store_api::region_request::RegionTruncateRequest::All)
                })
                .is_ok()
        );
        let manifest = env
            .mock_manifest_context(version.current().version.metadata.clone())
            .await;
        let (schemas, _) = mock_schema_metadata_manager();
        let first = scheduler
            .on_execution_finished(
                id,
                &CompactionExecution::for_test(0),
                &manifest,
                schemas.clone(),
                false,
            )
            .await;
        assert!(
            first.is_empty(),
            "DDL must not execute while shared planning still owns inputs"
        );
        let last = scheduler
            .handle_compaction_pick_finished(
                CompactionPickFinished {
                    region_id: id,
                    plan_id: SHARED_PLAN,
                    result: CompactionPlanningResult::NoPlan,
                },
                &manifest,
                schemas,
            )
            .await;
        assert_eq!(last.len(), 1);
        assert!(scheduler.shared.as_ref().unwrap().ddl_blocked.contains(&id));
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::*;
    use crate::compaction::scheduler::planning::CompactionPlanningResult;
    use crate::test_util::version_util::VersionControlBuilder;
    use crate::test_util::{mock_schema_metadata_manager, scheduler_util::SchedulerEnv};
    use tokio::sync::{mpsc, oneshot};

    #[tokio::test]
    async fn retry_reselects_without_a_write_and_keeps_remote_l1_active() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(8);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version = Arc::new(VersionControlBuilder::new().build());
        let id = version.region_id();
        scheduler.register_region(&version, &env.access_layer);
        scheduler
            .region_status
            .get_mut(&id)
            .unwrap()
            .start_remote_task();
        let shared = scheduler.shared_scheduler();
        shared.register_region(&version, &env.access_layer);
        shared
            .region_status
            .get_mut(&id)
            .unwrap()
            .start_picking(SHARED_PLAN);
        shared.next_plan_id = SHARED_PLAN + 1;
        let manifest = env
            .mock_manifest_context(version.current().version.metadata.clone())
            .await;
        let (schemas, _) = mock_schema_metadata_manager();
        let transition = scheduler
            .handle_compaction_pick_finished(
                CompactionPickFinished {
                    region_id: id,
                    plan_id: SHARED_PLAN,
                    result: CompactionPlanningResult::RetryShared,
                },
                &manifest,
                schemas,
            )
            .await;
        assert!(matches!(
            transition,
            CompactionTransition::AutomaticFollowupScheduled
        ));
        assert!(scheduler.shared.as_ref().unwrap().region_status[&id].is_picking(SHARED_PLAN + 1));
        assert!(scheduler.is_current_execution(id, &CompactionExecution::for_test(0)));
    }

    #[tokio::test]
    async fn retry_releases_ddl_instead_of_restarting_canceled_selection() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(8);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version = Arc::new(VersionControlBuilder::new().build());
        let id = version.region_id();
        scheduler.register_region(&version, &env.access_layer);
        let shared = scheduler.shared_scheduler();
        shared.register_region(&version, &env.access_layer);
        shared
            .region_status
            .get_mut(&id)
            .unwrap()
            .start_picking(SHARED_PLAN);
        let (reply, _response) = oneshot::channel();
        assert!(
            scheduler
                .try_cancel_and_add_ddl(id, reply.into(), (), |_| {
                    DdlRequest::Truncate(store_api::region_request::RegionTruncateRequest::All)
                })
                .is_ok()
        );
        let manifest = env
            .mock_manifest_context(version.current().version.metadata.clone())
            .await;
        let (schemas, _) = mock_schema_metadata_manager();
        let transition = scheduler
            .handle_compaction_pick_finished(
                CompactionPickFinished {
                    region_id: id,
                    plan_id: SHARED_PLAN,
                    result: CompactionPlanningResult::RetryShared,
                },
                &manifest,
                schemas,
            )
            .await;
        assert_eq!(transition.len(), 1);
        assert!(!scheduler.shared_running(id));
    }

    #[tokio::test]
    async fn stale_retry_cannot_restart_a_closed_and_reopened_region() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(8);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version = Arc::new(VersionControlBuilder::new().build());
        let id = version.region_id();
        scheduler.register_region(&version, &env.access_layer);
        let shared = scheduler.shared_scheduler();
        shared.register_region(&version, &env.access_layer);
        shared
            .region_status
            .get_mut(&id)
            .unwrap()
            .start_picking(SHARED_PLAN);
        scheduler.on_region_closed(id);
        scheduler.register_region(&version, &env.access_layer);
        scheduler
            .shared_scheduler()
            .register_region(&version, &env.access_layer);
        let manifest = env
            .mock_manifest_context(version.current().version.metadata.clone())
            .await;
        let (schemas, _) = mock_schema_metadata_manager();
        let transition = scheduler
            .handle_compaction_pick_finished(
                CompactionPickFinished {
                    region_id: id,
                    plan_id: SHARED_PLAN,
                    result: CompactionPlanningResult::RetryShared,
                },
                &manifest,
                schemas,
            )
            .await;
        assert!(matches!(transition, CompactionTransition::NoAction));
        assert!(!scheduler.shared_running(id));
        assert!(!scheduler.region_status[&id].is_compacting());
    }
}
