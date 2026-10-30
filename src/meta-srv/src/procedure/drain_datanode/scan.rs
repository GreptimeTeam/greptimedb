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
use std::sync::Mutex;

use common_meta::DatanodeId;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::datanode_table::DatanodeTableValue;
use common_meta::key::table_route::TableRouteValue;
use common_meta::rpc::router::RegionRoute;
use common_meta::wal_provider::extract_topic_from_wal_options;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::procedure::drain_datanode::TopicBatch;
use crate::procedure::region_migration::manager::RegionMigrationProcedureTracker;
use crate::utils::batch_aggregator::BatchAggregator;

/// A paused scan owns at most a metadata page, one table's routes/WAL options,
/// and the bounded topic accumulator. It never materializes the node's full plan.
pub(crate) struct RegionScan {
    tables: Mutex<BoxStream<'static, common_meta::error::Result<DatanodeTableValue>>>,
    current: Option<(DatanodeTableValue, std::vec::IntoIter<RegionRoute>)>,
    batches: BatchAggregator<Option<String>, RegionId>,
    exhausted: bool,
    /// Whether this scan has emitted any topic batch.
    pub(crate) produced_batch: bool,
    pub(crate) follower_count: usize,
}

impl fmt::Debug for RegionScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegionScan")
            .field("exhausted", &self.exhausted)
            .field("follower_count", &self.follower_count)
            .finish_non_exhaustive()
    }
}

impl RegionScan {
    /// Starts a paused scan with a region budget per emitted topic batch.
    pub(crate) fn new(
        metadata: &TableMetadataManagerRef,
        node_id: DatanodeId,
        parallelism: usize,
    ) -> Self {
        Self {
            // The stream is Send but not Sync. Access is exclusively through &mut
            // self/get_mut, without holding a lock across an await.
            tables: Mutex::new(metadata.datanode_table_manager().tables(node_id)),
            current: None,
            batches: BatchAggregator::new(parallelism, parallelism * 4),
            exhausted: false,
            produced_batch: false,
            follower_count: 0,
        }
    }

    /// Reads through full groups and skips failed or tracked regions. Returns `None`
    /// only after the metadata stream and all partial groups have been consumed.
    pub(crate) async fn next_batch(
        &mut self,
        metadata: &TableMetadataManagerRef,
        node_id: DatanodeId,
        failed: &[RegionId],
        tracker: &RegionMigrationProcedureTracker,
    ) -> Result<Option<TopicBatch>> {
        loop {
            if let Some((value, routes)) = &mut self.current {
                if let Some(route) = routes.next() {
                    if route.follower_peers.iter().any(|peer| peer.id == node_id) {
                        self.follower_count += 1;
                    }
                    if route.leader_peer.as_ref().map(|peer| peer.id) != Some(node_id)
                        || failed.contains(&route.region.id)
                        || tracker.contains(route.region.id)
                    {
                        continue;
                    }
                    let region_id = route.region.id;
                    let topic = extract_topic_from_wal_options(
                        region_id,
                        &value.region_info.region_wal_options,
                    );
                    if let Some(batch) = self.batches.push(topic, region_id) {
                        self.produced_batch = true;
                        return Ok(Some(Self::build_batch(batch)));
                    }
                    continue;
                }
                self.current = None;
            }

            if self.exhausted {
                let batch = self.batches.flush().map(Self::build_batch);
                self.produced_batch |= batch.is_some();
                return Ok(batch);
            }
            let Some(value) = self
                .tables
                .get_mut()
                .unwrap()
                .try_next()
                .await
                .context(error::TableMetadataManagerSnafu)?
            else {
                self.exhausted = true;
                continue;
            };
            let route = metadata
                .table_route_manager()
                .table_route_storage()
                .get(value.table_id)
                .await
                .context(error::TableMetadataManagerSnafu)?;
            if let Some(TableRouteValue::Physical(route)) = route {
                self.current = Some((value, route.region_routes.into_iter()));
            }
        }
    }

    fn build_batch((topic, mut leader_regions): (Option<String>, Vec<RegionId>)) -> TopicBatch {
        leader_regions.sort_unstable();
        leader_regions.dedup();
        TopicBatch {
            leader_regions,
            topic,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_procedure::{Status, SubprocedureRecovery};
    use common_wal::options::{KafkaWalOptions, WalOptions};
    use store_api::storage::RegionId;

    use super::RegionScan;
    use crate::procedure::drain_datanode::DrainDatanodeStrategy;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, create_kafka_table, dispatch_next, leader_region, new_persistent_context,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;

    #[tokio::test]
    async fn test_topic_scan_pauses_and_preserves_interleaved_regions() {
        let env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 9, 2).await;
        create_kafka_table(&env, 1025, 3, 2).await;
        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 2);
        let tracker = env.region_migration_manager.tracker();
        let first = scan
            .next_batch(&env.table_metadata_manager, 1, &[], tracker)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            vec![RegionId::new(1024, 0), RegionId::new(1024, 2)],
            first.leader_regions
        );
        // The first child can be dispatched before even the first table is exhausted.
        assert_eq!(6, scan.current.as_ref().unwrap().1.len());
        assert!(!scan.exhausted);
        let mut seen = first.leader_regions;
        while let Some(plan) = scan
            .next_batch(&env.table_metadata_manager, 1, &[], tracker)
            .await
            .unwrap()
        {
            assert!(plan.leader_regions.len() <= 2);
            let first = plan.leader_regions[0];
            let expected_topic = format!("topic-{}", first.region_number() % 2);
            assert_eq!(Some(expected_topic.as_str()), plan.topic.as_deref());
            assert!(
                plan.leader_regions
                    .iter()
                    .all(|id| id.region_number() % 2 == first.region_number() % 2)
            );
            seen.extend(plan.leader_regions);
        }
        seen.sort_unstable();
        let expected = (0..9)
            .map(|i| RegionId::new(1024, i))
            .chain((0..3).map(|i| RegionId::new(1025, i)))
            .collect::<Vec<_>>();
        assert_eq!(expected, seen);
    }

    #[tokio::test]
    async fn test_topic_batch_owns_all_guards_and_respects_region_budget() {
        let env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 8, 2).await;
        let mut persistent = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent.parallelism = 3;
        let mut ctx = env.context(persistent);
        ctx.start_scan().unwrap();
        ctx.read_next_topic().await.unwrap();
        let regions = ctx.volatile_ctx.leader_regions().to_vec();
        assert_eq!(3, regions.len());
        let (_, status) = dispatch_next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();
        let Status::Suspended {
            subprocedures,
            recovery,
            persist,
        } = status
        else {
            panic!("expected a detached batch");
        };
        assert!(matches!(recovery, SubprocedureRecovery::Detach));
        assert!(!persist);
        assert_eq!(1, subprocedures.len());
        assert_eq!(
            regions,
            ctx.volatile_ctx.inflight_subprocedures[0].region_ids
        );
        let tracker = env.region_migration_manager.tracker();
        for region_id in &regions {
            assert!(tracker.contains(*region_id));
        }
        drop(subprocedures);
        for region_id in regions {
            assert!(!tracker.contains(region_id));
        }
    }
    #[tokio::test]
    async fn test_topic_batch_reduces_child_count() {
        common_telemetry::init_default_ut_logging();
        let env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 8, 1).await;
        for trial in 0..3 {
            for grouped in [false, true] {
                let mut persistent =
                    new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
                persistent.parallelism = 8;
                let mut ctx = env.context(persistent);
                ctx.start_scan().unwrap();
                ctx.read_next_topic().await.unwrap();
                if !grouped {
                    ctx.volatile_ctx.topic_batch.as_mut().unwrap().topic = None;
                }
                let (_, status) = dispatch_next(&mut ctx, &TestingEnv::procedure_context())
                    .await
                    .unwrap();
                let Status::Suspended { subprocedures, .. } = status else {
                    panic!("expected children");
                };
                let count = subprocedures.len();
                assert_eq!(if grouped { 1 } else { 8 }, count);
                let mut regions = ctx
                    .volatile_ctx
                    .inflight_subprocedures
                    .iter()
                    .flat_map(|meta| meta.region_ids.iter().copied())
                    .collect::<Vec<_>>();
                regions.sort_unstable();
                assert_eq!(
                    (0..8).map(|i| RegionId::new(1024, i)).collect::<Vec<_>>(),
                    regions
                );
                common_telemetry::info!(
                    "trial={trial}, grouped={grouped}, regions={}, children={count}",
                    regions.len()
                );
                drop(subprocedures);
                assert!(
                    regions
                        .iter()
                        .all(|id| !env.region_migration_manager.tracker().contains(*id))
                );
            }
        }
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
        ctx.start_scan().unwrap();
        ctx.read_next_topic().await.unwrap();
        let (_, status) = dispatch_next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();
        let Status::Suspended { subprocedures, .. } = status else {
            panic!("expected children");
        };
        assert_eq!(2, subprocedures.len());
        let destinations = subprocedures
            .iter()
            .map(|child| {
                let data: serde_json::Value =
                    serde_json::from_str(&child.procedure.dump().unwrap()).unwrap();
                (
                    data["persistent_ctx"]["to_peer"]["id"].as_u64().unwrap(),
                    data["persistent_ctx"]["region_ids"]
                        .as_array()
                        .unwrap()
                        .len(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![(2, 1), (3, 1)], destinations);
    }
    #[tokio::test]
    async fn test_reload_rebuilds_scan_and_excludes_every_region_in_running_batch() {
        use crate::procedure::drain_datanode::DrainDatanodeProcedure;
        use crate::procedure::drain_datanode::manager::DrainDatanodeProcedureTracker;
        use common_procedure::Procedure;

        let env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 8, 1).await;
        let mut persistent = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent.parallelism = 3;
        let mut ctx = env.context(persistent);
        ctx.start_scan().unwrap();
        ctx.read_next_topic().await.unwrap();
        let running = ctx.volatile_ctx.leader_regions().to_vec();
        let (_, status) = dispatch_next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();
        let Status::Suspended { subprocedures, .. } = status else {
            panic!("expected children");
        };
        let original = DrainDatanodeProcedure::new(ctx, vec![]);
        let data = original.dump().unwrap();
        let tracker = DrainDatanodeProcedureTracker::default();
        let mut restored = DrainDatanodeProcedure::from_json(&data, &tracker, |persistent| {
            env.context(persistent)
        })
        .unwrap();
        assert!(restored.context.volatile_ctx.scan.is_none());
        restored.context.start_scan().unwrap();
        restored.context.read_next_topic().await.unwrap();
        assert_eq!(
            vec![
                RegionId::new(1024, 3),
                RegionId::new(1024, 4),
                RegionId::new(1024, 5)
            ],
            restored.context.volatile_ctx.leader_regions()
        );
        assert!(
            running
                .iter()
                .all(|id| env.region_migration_manager.tracker().contains(*id))
        );
        drop(subprocedures);
        restored.context.start_scan().unwrap();
        restored.context.read_next_topic().await.unwrap();
        assert_eq!(running, restored.context.volatile_ctx.leader_regions());
    }
    #[tokio::test]
    async fn test_scan_retry_rebuilds_buffered_partial_topics() {
        use common_meta::key::TableMetadataManager;
        use common_meta::kv_backend::test_util::MockKvBackendBuilder;
        use std::sync::Arc;

        let env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 3, 2).await;
        create_kafka_table(&env, 1025, 1, 1).await;
        let mut persistent = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent.parallelism = 2;
        let mut ctx = env.context(persistent);
        ctx.start_scan().unwrap();
        ctx.read_next_topic().await.unwrap();
        assert_eq!(
            vec![RegionId::new(1024, 0), RegionId::new(1024, 2)],
            ctx.volatile_ctx.leader_regions()
        );
        for region_id in ctx.volatile_ctx.leader_regions().to_vec() {
            ctx.forget_leader_region(region_id);
        }
        ctx.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(
            MockKvBackendBuilder::default()
                .range_fn(Arc::new(|_| {
                    common_meta::error::ElectionNoLeaderSnafu.fail()
                }))
                .build()
                .unwrap(),
        )));
        let error = ctx.read_next_topic().await.unwrap_err();
        assert!(error.is_retryable());
        assert!(ctx.volatile_ctx.scan.is_none());
        ctx.table_metadata_manager = env.table_metadata_manager.clone();
        ctx.start_scan().unwrap();
        ctx.read_next_topic().await.unwrap();
        let mut seen = ctx.volatile_ctx.leader_regions().to_vec();
        while ctx.volatile_ctx.topic_batch.is_some() {
            ctx.read_next_topic().await.unwrap();
            seen.extend_from_slice(ctx.volatile_ctx.leader_regions());
        }
        seen.sort_unstable();
        assert_eq!(
            vec![
                RegionId::new(1024, 0),
                RegionId::new(1024, 1),
                RegionId::new(1024, 2),
                RegionId::new(1025, 0)
            ],
            seen
        );
        assert_eq!(2, ctx.volatile_ctx.rescan_round);
    }
}
