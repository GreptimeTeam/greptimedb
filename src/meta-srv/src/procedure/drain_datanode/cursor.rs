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

use std::any::Any;

use common_meta::key::table_route::TableRouteValue;
use common_meta::peer::Peer;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::procedure::drain_datanode::dispatch::Dispatch;
use crate::procedure::drain_datanode::drain_datanode_end::DrainDatanodeEnd;
use crate::procedure::drain_datanode::utils::batch_aggregator::BatchAggregator;
use crate::procedure::drain_datanode::{Context, MAX_RESCAN_ROUNDS, State, utils};
use crate::procedure::region_migration::DEFAULT_REGION_MIGRATION_TIMEOUT;
use crate::procedure::region_migration::manager::{
    RegionMigrationProcedureTask, RegionMigrationTriggerReason,
};
use crate::procedure::region_migration::utils::RegionMigrationTaskBatch;

/// Scans region groups, prepares migrations, and confirms completion with a fresh scan.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct DrainDatanodeCursor {
    /// The group being prepared, retained when preparation needs a retry.
    #[serde(skip)]
    region_group: Vec<RegionId>,
    /// Batches prepared for this round, retained when a later scan or preparation fails.
    #[serde(skip)]
    batches: Vec<RegionMigrationTaskBatch>,
}

impl DrainDatanodeCursor {
    /// Starts a fresh scan of the authoritative table routes.
    ///
    /// Scans are bounded by [`MAX_RESCAN_ROUNDS`]: a datanode whose leader regions keep
    /// coming back must not be rescanned forever.
    fn start_scan(&mut self, ctx: &mut Context) -> Result<()> {
        let node_id = ctx.persistent_ctx.node_id;
        if ctx.volatile_ctx.rescan_round >= MAX_RESCAN_ROUNDS {
            return error::DrainDatanodeRescanExceededSnafu {
                node_id,
                max_rounds: MAX_RESCAN_ROUNDS,
            }
            .fail();
        }

        ctx.volatile_ctx.rescan_round += 1;
        ctx.volatile_ctx.scan = Some(utils::scan::RegionScan::new(
            &ctx.table_metadata_manager,
            node_id,
            ctx.persistent_ctx.parallelism,
            ctx.persistent_ctx.parallelism * 4,
        ));
        ctx.volatile_ctx.follower_count = 0;

        Ok(())
    }

    /// Reads only the next bounded window. A read failure invalidates the scan so
    /// retry starts from authoritative metadata instead of losing buffered entries.
    async fn read_next_group(
        &mut self,
        ctx: &mut Context,
        max_regions: usize,
    ) -> Result<Option<Vec<RegionId>>> {
        let Some(mut scan) = ctx.volatile_ctx.scan.take() else {
            return Ok(None);
        };
        let mut next = scan
            .next_group(
                |region_id| {
                    ctx.volatile_ctx.failed_regions.contains(&region_id)
                        || ctx.region_migration_manager.tracker().contains(region_id)
                },
                max_regions,
            )
            .await?;
        // A rebuilt scan may encounter regions already prepared before a read error.
        if let Some(group) = &mut next {
            group.retain(|region| {
                !self
                    .batches
                    .iter()
                    .any(|batch| batch.region_ids.contains(region))
            });
        }
        ctx.volatile_ctx.follower_count = scan.follower_count();
        ctx.volatile_ctx.scan = Some(scan);
        Ok(next)
    }

    /// Fills this round's region budget, retaining partial work on retryable errors.
    async fn prepare_batches(&mut self, ctx: &mut Context) -> Result<()> {
        loop {
            utils::ensure_drain_deadline(ctx)?;
            if !self.region_group.is_empty() {
                let batches = self.prepare_migrations(ctx).await?;
                self.batches.extend(batches);
                self.region_group.clear();
            }
            let prepared_regions = self
                .batches
                .iter()
                .map(|batch| batch.region_ids.len())
                .sum::<usize>();
            if prepared_regions >= ctx.persistent_ctx.parallelism {
                break;
            }
            if ctx.volatile_ctx.scan.is_none() {
                self.start_scan(ctx)?;
            }
            let Some(group) = self
                .read_next_group(ctx, ctx.persistent_ctx.parallelism - prepared_regions)
                .await?
            else {
                break;
            };
            self.region_group = group;
        }
        utils::ensure_drain_deadline(ctx)
    }

    /// Selects destinations within the current group and prepares complete migration batches.
    async fn prepare_migrations(
        &mut self,
        ctx: &mut Context,
    ) -> Result<Vec<RegionMigrationTaskBatch>> {
        let regions = self.region_group.clone();
        let mut tasks = Vec::new();
        for region_id in regions {
            if ctx.volatile_ctx.failed_regions.contains(&region_id)
                || ctx.region_migration_manager.tracker().contains(region_id)
            {
                continue;
            }
            match self.prepare_migration_task(ctx, region_id).await {
                Ok(Some(task)) => tasks.push(task),
                Ok(None) => {}
                Err(e) => ctx.handle_migration_error(e, &[region_id])?,
            }
        }
        let groups = BatchAggregator::group(tasks.into_iter().map(|task| (task.to_peer.id, task)));
        let batches = groups
            .into_iter()
            .map(|(_, tasks)| {
                let first = &tasks[0];
                RegionMigrationTaskBatch {
                    region_ids: tasks.iter().map(|task| task.region_id).collect(),
                    from_peer: first.from_peer.clone(),
                    to_peer: first.to_peer.clone(),
                    // Downgrade and catchup share this batch budget; downgrade requests run
                    // concurrently rather than receiving a separate budget per region.
                    timeout: first.timeout,
                    trigger_reason: first.trigger_reason,
                }
            })
            .collect();
        Ok(batches)
    }

    async fn prepare_migration_task(
        &self,
        ctx: &mut Context,
        region_id: RegionId,
    ) -> Result<Option<RegionMigrationProcedureTask>> {
        let node_id = ctx.persistent_ctx.node_id;
        let Some(table_route) = ctx
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(region_id.table_id())
            .await
            .context(error::TableMetadataManagerSnafu)?
        else {
            return Ok(None);
        };
        let TableRouteValue::Physical(table_route) = table_route else {
            return Ok(None);
        };
        let Some(region_route) = table_route
            .region_routes
            .iter()
            .find(|region_route| region_route.region.id == region_id)
        else {
            return Ok(None);
        };
        if region_route.leader_peer.as_ref().map(|peer| peer.id) != Some(node_id) {
            return Ok(None);
        }

        let to_peer = utils::select_migration_target(
            &ctx.selector_ctx,
            node_id,
            ctx.persistent_ctx.target_node_id,
            region_id,
            &region_route.follower_peers,
        )
        .await?;

        Ok(Some(RegionMigrationProcedureTask::new(
            region_id,
            Peer::empty(node_id),
            to_peer,
            DEFAULT_REGION_MIGRATION_TIMEOUT,
            RegionMigrationTriggerReason::Manual,
        )))
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for DrainDatanodeCursor {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        loop {
            self.prepare_batches(ctx).await?;
            if !self.batches.is_empty() {
                let batches = std::mem::take(&mut self.batches);
                return Ok((Box::new(Dispatch::new(batches)), Status::executing(false)));
            }
            let produced_group = ctx
                .volatile_ctx
                .scan
                .take()
                .is_some_and(|scan| scan.has_produced_group());
            if produced_group {
                // Confirm completion after all work from this scan has settled.
                continue;
            }
            info!(
                "No more migrations to dispatch for datanode {}, migrated regions: {}, follower regions: {}",
                ctx.persistent_ctx.node_id,
                ctx.volatile_ctx.migrated,
                ctx.volatile_ctx.follower_count
            );
            return Ok((Box::new(DrainDatanodeEnd), Status::executing(false)));
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use api::v1::meta::heartbeat_request::NodeWorkloads;
    use common_meta::cluster::NodeInfo;
    use common_meta::error::Result as MetaResult;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::{Peer, PeerDiscovery};
    use common_procedure::Status;
    use common_time::util::current_time_millis;
    use common_wal::options::{KafkaWalOptions, WalOptions};
    use store_api::storage::RegionId;

    use crate::error;
    use crate::key::NodeSchedulingRole;
    use crate::procedure::drain_datanode::cursor::DrainDatanodeCursor;
    use crate::procedure::drain_datanode::dispatch::Dispatch;
    use crate::procedure::drain_datanode::drain_datanode_end::DrainDatanodeEnd;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, create_kafka_table, leader_region, new_persistent_context,
    };
    use crate::procedure::drain_datanode::{DrainDatanodeStrategy, MAX_RESCAN_ROUNDS, State};
    use crate::procedure::region_migration::DEFAULT_REGION_MIGRATION_TIMEOUT;
    use crate::procedure::region_migration::manager::{
        RegionMigrationProcedureTask, RegionMigrationTriggerReason,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;
    use crate::procedure::region_migration::utils::RegionMigrationTaskBatch;

    fn dispatch_regions(state: &dyn State) -> Vec<RegionId> {
        state
            .as_any()
            .downcast_ref::<Dispatch>()
            .unwrap()
            .batches
            .iter()
            .flat_map(|batch| batch.region_ids.iter().copied())
            .collect()
    }

    async fn wait_until_deadline(deadline_ms: i64) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while current_time_millis() < deadline_ms {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the deadline should elapse");
    }

    struct EmptyDiscovery {
        deadline_ms: i64,
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl PeerDiscovery for EmptyDiscovery {
        async fn active_frontends(&self) -> MetaResult<Vec<NodeInfo>> {
            unreachable!()
        }

        async fn active_datanodes(
            &self,
            _filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
        ) -> MetaResult<Vec<NodeInfo>> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            if call == 1 {
                wait_until_deadline(self.deadline_ms).await;
            }
            Ok(vec![])
        }

        async fn active_flownodes(
            &self,
            _filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
        ) -> MetaResult<Vec<NodeInfo>> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_cursor_checks_deadline_after_preparation_failures() {
        let env = TestEnv::new().await;
        let regions = (0..4).map(|i| RegionId::new(1024, i)).collect::<Vec<_>>();
        env.create_table(regions.iter().map(|id| leader_region(*id, 1)).collect())
            .await;
        let mut persistent = new_persistent_context(None, DrainDatanodeStrategy::TryAll);
        persistent.parallelism = 1;
        persistent.deadline_ms = current_time_millis() + 1000;
        let discovery = Arc::new(EmptyDiscovery {
            deadline_ms: persistent.deadline_ms,
            calls: AtomicUsize::new(0),
        });
        let mut ctx = env.context(persistent);
        ctx.selector_ctx.peer_discovery = discovery.clone();
        let error = DrainDatanodeCursor::default()
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::ExceededDeadline { .. }));
        assert_eq!(2, discovery.calls.load(Ordering::Relaxed));
        assert_eq!(&regions[..2], ctx.volatile_ctx.failed_regions);
        assert_eq!(0, ctx.volatile_ctx.migrated);
    }

    #[tokio::test]
    async fn test_cursor_checks_deadline_after_empty_scan() {
        use common_meta::key::TableMetadataManager;
        use common_meta::kv_backend::test_util::MockKvBackendBuilder;

        let env = TestEnv::new().await;
        let mut ctx = env.context(new_persistent_context(None, DrainDatanodeStrategy::TryAll));
        let deadline_ms = current_time_millis() + 1000;
        ctx.persistent_ctx.deadline_ms = deadline_ms;
        ctx.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(
            MockKvBackendBuilder::default()
                .range_async_fn(Arc::new(move |_| {
                    Box::pin(async move {
                        wait_until_deadline(deadline_ms).await;
                        Ok(common_meta::rpc::store::RangeResponse {
                            kvs: vec![],
                            more: false,
                        })
                    })
                }))
                .build()
                .unwrap(),
        )));
        let error = DrainDatanodeCursor::default()
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::ExceededDeadline { .. }));
        assert_eq!(1, ctx.volatile_ctx.rescan_round);
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_cursor_excludes_running_migrations() {
        for has_pending_region in [false, true] {
            let env = TestEnv::new().await;
            let running = RegionId::new(1024, 1);
            let pending = RegionId::new(1024, 2);
            let failed = RegionId::new(1024, 3);
            let mut routes = vec![leader_region(running, 1), leader_region(failed, 1)];
            if has_pending_region {
                routes.push(leader_region(pending, 1));
            }
            env.create_table(routes).await;
            let guard = env
                .region_migration_manager
                .tracker()
                .insert_running_procedure(&RegionMigrationProcedureTask::new(
                    running,
                    Peer::empty(1),
                    Peer::empty(2),
                    Duration::from_secs(10),
                    RegionMigrationTriggerReason::Manual,
                ))
                .unwrap();
            let mut ctx = env.context(new_persistent_context(
                Some(2),
                DrainDatanodeStrategy::TryAll,
            ));
            ctx.volatile_ctx.failed_regions.push(failed);
            let procedure_ctx = TestingEnv::procedure_context();

            let (next, status) = DrainDatanodeCursor::default()
                .next(&mut ctx, &procedure_ctx)
                .await
                .unwrap();
            assert!(matches!(status, Status::Executing { persist: false, .. }));
            assert_eq!(vec![failed], ctx.volatile_ctx.failed_regions);
            assert_eq!(0, ctx.volatile_ctx.migrated);
            if has_pending_region {
                assert!(next.as_any().is::<Dispatch>());
                assert_eq!(vec![pending], dispatch_regions(next.as_ref()));
            } else {
                assert!(next.as_any().is::<DrainDatanodeEnd>());
            }

            // Once the migration releases its guard, an unchanged route needs a new child.
            drop(guard);

            ctx.volatile_ctx.scan = None;
            let (next, _) = DrainDatanodeCursor::default()
                .next(&mut ctx, &procedure_ctx)
                .await
                .unwrap();
            assert!(next.as_any().is::<Dispatch>());
            let expected = if has_pending_region {
                vec![running, pending]
            } else {
                vec![running]
            };
            assert_eq!(expected, dispatch_regions(next.as_ref()));
        }
    }

    #[tokio::test]
    async fn test_drain_datanode_cursor_transitions_to_end() {
        let env = TestEnv::new().await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = DrainDatanodeCursor::default()
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
        assert!(matches!(status, Status::Executing { persist: false, .. }));
    }

    #[tokio::test]
    async fn test_drain_datanode_cursor_rescan_requeues() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        // A fresh scan discovers regions still led by the source.
        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = DrainDatanodeCursor::default()
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
        assert_eq!(vec![region_id], dispatch_regions(next.as_ref()));
        assert_eq!(1, ctx.volatile_ctx.rescan_round);
    }

    #[tokio::test]
    async fn test_drain_datanode_cursor_exceeded_rounds() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        ctx.volatile_ctx.rescan_round = MAX_RESCAN_ROUNDS;
        let procedure_ctx = TestingEnv::procedure_context();

        let error = DrainDatanodeCursor::default()
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            error::Error::DrainDatanodeRescanExceeded { .. }
        ));
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn test_drain_datanode_cursor_ignores_failed_regions() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(None, DrainDatanodeStrategy::TryAll));
        ctx.volatile_ctx.failed_regions = vec![region_id];
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = DrainDatanodeCursor::default()
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
    }

    #[tokio::test]
    async fn test_cursor_skips_region_moved_away() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        // The current batch lists the region, but the table route says another datanode leads it.
        env.create_table(vec![leader_region(region_id, 2)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(3),
            DrainDatanodeStrategy::FailFast,
        ));
        let mut cursor = DrainDatanodeCursor {
            region_group: vec![region_id],
            ..Default::default()
        };
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = cursor.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().is::<DrainDatanodeEnd>());
    }

    #[tokio::test]
    async fn test_drain_datanode_skips_region_whose_migration_is_running() {
        for scan_before_migration in [false, true] {
            let env = TestEnv::new().await;
            let region_id = RegionId::new(1024, 1);
            env.create_table(vec![leader_region(region_id, 1)]).await;

            let mut ctx = env.context(new_persistent_context(
                Some(2),
                DrainDatanodeStrategy::FailFast,
            ));
            let mut cursor = DrainDatanodeCursor::default();
            if scan_before_migration {
                cursor.start_scan(&mut ctx).unwrap();
                cursor.region_group = cursor.read_next_group(&mut ctx, 1).await.unwrap().unwrap();
                assert_eq!(vec![region_id], cursor.region_group);
            }
            let procedure_ctx = TestingEnv::procedure_context();

            // A manual migration of the same region is already running. Building it keeps the
            // region in the running set of the migration manager until it is dropped.
            let _child = env
                .region_migration_manager
                .build_subprocedure(
                    common_procedure::ProcedureContext::default(),
                    RegionMigrationProcedureTask::new(
                        region_id,
                        Peer::empty(1),
                        Peer::empty(2),
                        Duration::from_secs(10),
                        RegionMigrationTriggerReason::Manual,
                    ),
                )
                .await
                .expect("the manual migration should be buildable");

            let (next, status) = cursor.next(&mut ctx, &procedure_ctx).await.unwrap();

            // Both scanning and preparation skip regions owned by another migration.
            assert!(matches!(status, Status::Executing { .. }));
            assert!(next.as_any().is::<DrainDatanodeEnd>());

            assert!(ctx.volatile_ctx.failed_regions.is_empty());
            assert_eq!(0, ctx.volatile_ctx.migrated);
            assert!(env.region_migration_manager.tracker().contains(region_id));
        }
    }

    #[tokio::test]
    async fn test_drain_datanode_fail_fast_on_unschedulable_target() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;
        env.node_scheduling_manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 3)
            .await
            .unwrap();

        let mut ctx = env.context(new_persistent_context(
            Some(3),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let mut cursor = DrainDatanodeCursor::default();
        let error = cursor.next(&mut ctx, &procedure_ctx).await.unwrap_err();

        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
        assert!(!error.is_retryable());
        assert_eq!(vec![region_id], cursor.region_group);
    }

    #[tokio::test]
    async fn test_same_topic_splits_by_eligible_destination() {
        use common_meta::peer::Peer;
        let env = TestEnv::new().await;
        let first = RegionId::new(1024, 0);
        let second = RegionId::new(1024, 1);
        let mut first_route = leader_region(first, 1);
        first_route.follower_peers = vec![Peer::empty(3)];
        let mut second_route = leader_region(second, 1);
        second_route.follower_peers = vec![Peer::empty(2)];
        env.table_metadata_manager
            .create_table_metadata(
                new_test_table_info(1024),
                TableRouteValue::physical(vec![first_route, second_route]),
                HashMap::from([
                    (0, WalOptions::Kafka(KafkaWalOptions::new("topic".into()))),
                    (1, WalOptions::Kafka(KafkaWalOptions::new("topic".into()))),
                ]),
            )
            .await
            .unwrap();
        let mut persistent = new_persistent_context(None, DrainDatanodeStrategy::FailFast);
        persistent.parallelism = 2;
        let mut ctx = env.context(persistent);
        let (dispatch, status) = DrainDatanodeCursor::default()
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();
        assert!(matches!(status, Status::Executing { persist: false, .. }));
        let dispatch = dispatch.as_any().downcast_ref::<Dispatch>().unwrap();
        let destinations = dispatch
            .batches
            .iter()
            .map(|batch| (batch.to_peer.id, batch.region_ids.clone()))
            .collect::<Vec<_>>();
        assert_eq!(vec![(2, vec![first]), (3, vec![second])], destinations);
        // The cursor prepares inputs; registration happens only when Dispatch runs.
        assert!(!env.region_migration_manager.tracker().contains(first));
        assert!(!env.region_migration_manager.tracker().contains(second));
    }

    #[tokio::test]
    async fn test_cursor_preserves_prepared_batches_on_scan_retry() {
        use common_meta::key::TableMetadataManager;
        use common_meta::kv_backend::test_util::MockKvBackendBuilder;

        let env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 3, 1).await;
        let mut persistent = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent.parallelism = 3;
        let mut ctx = env.context(persistent);
        let first = RegionId::new(1024, 0);
        let mut cursor = DrainDatanodeCursor {
            batches: vec![RegionMigrationTaskBatch {
                region_ids: vec![first],
                from_peer: Peer::empty(1),
                to_peer: Peer::empty(2),
                timeout: Duration::from_secs(10),
                trigger_reason: RegionMigrationTriggerReason::Manual,
            }],
            ..Default::default()
        };
        ctx.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(
            MockKvBackendBuilder::default()
                .range_fn(Arc::new(|_| {
                    common_meta::error::ElectionNoLeaderSnafu.fail()
                }))
                .build()
                .unwrap(),
        )));
        let procedure_ctx = TestingEnv::procedure_context();
        let error = cursor.next(&mut ctx, &procedure_ctx).await.unwrap_err();
        assert!(error.is_retryable());
        assert!(ctx.volatile_ctx.scan.is_none());
        assert_eq!(vec![first], cursor.batches[0].region_ids);
        ctx.table_metadata_manager = env.table_metadata_manager.clone();
        let (dispatch, _) = cursor.next(&mut ctx, &procedure_ctx).await.unwrap();
        let dispatch = dispatch.as_any().downcast_ref::<Dispatch>().unwrap();
        let mut regions = dispatch
            .batches
            .iter()
            .flat_map(|batch| batch.region_ids.iter().copied())
            .collect::<Vec<_>>();
        regions.sort_unstable();
        assert_eq!(
            (0..3).map(|i| RegionId::new(1024, i)).collect::<Vec<_>>(),
            regions
        );
        assert_eq!(2, ctx.volatile_ctx.rescan_round);
    }

    #[tokio::test]
    async fn test_cursor_fills_region_budget_for_kafka_and_local_wal() {
        for kafka in [false, true] {
            let env = TestEnv::new().await;
            if kafka {
                create_kafka_table(&env, 1024, 8, 2).await;
            } else {
                env.create_table(
                    (0..8)
                        .map(|i| leader_region(RegionId::new(1024, i), 1))
                        .collect(),
                )
                .await;
            }
            let mut persistent = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
            persistent.parallelism = 3;
            let mut ctx = env.context(persistent);
            let (dispatch, status) = DrainDatanodeCursor::default()
                .next(&mut ctx, &TestingEnv::procedure_context())
                .await
                .unwrap();
            assert!(matches!(status, Status::Executing { persist: false, .. }));
            let batches = &dispatch
                .as_any()
                .downcast_ref::<Dispatch>()
                .unwrap()
                .batches;
            assert_eq!(
                3,
                batches
                    .iter()
                    .map(|batch| batch.region_ids.len())
                    .sum::<usize>()
            );
            assert_eq!(if kafka { 1 } else { 3 }, batches.len());
            assert!(
                batches
                    .iter()
                    .all(|batch| batch.timeout == DEFAULT_REGION_MIGRATION_TIMEOUT)
            );
            for region in batches.iter().flat_map(|batch| &batch.region_ids) {
                assert!(!env.region_migration_manager.tracker().contains(*region));
            }
        }
    }
    #[tokio::test]
    async fn test_cursor_retains_group_and_batches_on_preparation_retry() {
        use common_meta::key::TableMetadataManager;
        use common_meta::kv_backend::test_util::MockKvBackendBuilder;

        let env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 2, 1).await;
        let first = RegionId::new(1024, 0);
        let second = RegionId::new(1024, 1);
        let mut persistent = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent.parallelism = 2;
        let mut ctx = env.context(persistent);
        let mut cursor = DrainDatanodeCursor {
            region_group: vec![second],
            batches: vec![RegionMigrationTaskBatch {
                region_ids: vec![first],
                from_peer: Peer::empty(1),
                to_peer: Peer::empty(2),
                timeout: Duration::from_secs(10),
                trigger_reason: RegionMigrationTriggerReason::Manual,
            }],
        };
        ctx.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(
            MockKvBackendBuilder::default()
                .range_fn(Arc::new(|_| {
                    common_meta::error::ElectionNoLeaderSnafu.fail()
                }))
                .build()
                .unwrap(),
        )));
        let procedure_ctx = TestingEnv::procedure_context();
        let error = cursor.next(&mut ctx, &procedure_ctx).await.unwrap_err();
        assert!(error.is_retryable());
        assert_eq!(vec![second], cursor.region_group);
        assert_eq!(vec![first], cursor.batches[0].region_ids);
        ctx.table_metadata_manager = env.table_metadata_manager.clone();
        let (dispatch, _) = cursor.next(&mut ctx, &procedure_ctx).await.unwrap();
        let batches = &dispatch
            .as_any()
            .downcast_ref::<Dispatch>()
            .unwrap()
            .batches;
        assert_eq!(
            vec![first, second],
            batches
                .iter()
                .flat_map(|batch| batch.region_ids.iter().copied())
                .collect::<Vec<_>>()
        );
    }
}
