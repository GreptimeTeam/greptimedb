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
use crate::procedure::drain_datanode::batch_aggregator::BatchAggregator;
use crate::procedure::region_migration::manager::RegionMigrationProcedureTracker;

/// A paused scan owns at most a metadata page, one table's routes/WAL options,
/// and the bounded topic accumulator. It never materializes the node's full plan.
pub(crate) struct RegionScan {
    tables: BoxStream<'static, common_meta::error::Result<DatanodeTableValue>>,
    current_table: Option<(DatanodeTableValue, std::vec::IntoIter<RegionRoute>)>,
    batches: BatchAggregator<String, RegionId>,
    remaining_group: Vec<RegionId>,
    exhausted: bool,
    /// Whether this scan has emitted any region group.
    pub(crate) produced_group: bool,
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
    /// Creates a bounded scan, grouping regions that can share a migration procedure.
    pub(crate) fn new(
        metadata: &TableMetadataManagerRef,
        node_id: DatanodeId,
        parallelism: usize,
    ) -> Self {
        Self {
            tables: metadata.datanode_table_manager().tables(node_id),
            current_table: None,
            batches: BatchAggregator::new(parallelism, parallelism * 4),
            remaining_group: vec![],
            exhausted: false,
            produced_group: false,
            follower_count: 0,
        }
    }

    /// Returns at most `max_regions` regions that can share a migration procedure.
    /// Retains the rest of the group for the next call. A zero limit behaves as one.
    /// Returns `None` only after the metadata stream and buffered groups are exhausted.
    pub(crate) async fn next_group(
        &mut self,
        metadata: &TableMetadataManagerRef,
        node_id: DatanodeId,
        failed_regions: &[RegionId],
        tracker: &RegionMigrationProcedureTracker,
        max_regions: usize,
    ) -> Result<Option<Vec<RegionId>>> {
        loop {
            if !self.remaining_group.is_empty() {
                let count = max_regions.max(1).min(self.remaining_group.len());
                self.produced_group = true;
                return Ok(Some(self.remaining_group.drain(..count).collect()));
            }
            if let Some((value, routes)) = &mut self.current_table {
                if let Some(route) = routes.next() {
                    if route.follower_peers.iter().any(|peer| peer.id == node_id) {
                        self.follower_count += 1;
                    }
                    if route.leader_peer.as_ref().map(|peer| peer.id) != Some(node_id)
                        || failed_regions.contains(&route.region.id)
                        || tracker.contains(route.region.id)
                    {
                        continue;
                    }
                    let region_id = route.region.id;
                    let Some(topic) = extract_topic_from_wal_options(
                        region_id,
                        &value.region_info.region_wal_options,
                    ) else {
                        // Regions without a shared WAL topic must migrate independently.
                        self.produced_group = true;
                        return Ok(Some(vec![region_id]));
                    };
                    if let Some((_, mut regions)) = self.batches.push(topic, region_id) {
                        regions.sort_unstable();
                        regions.dedup();
                        self.remaining_group = regions;
                    }
                    continue;
                }
                self.current_table = None;
            }

            if self.exhausted {
                let Some((_, mut regions)) = self.batches.flush() else {
                    return Ok(None);
                };
                regions.sort_unstable();
                regions.dedup();
                self.remaining_group = regions;
                continue;
            }
            let Some(value) = self
                .tables
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
                self.current_table = Some((value, route.region_routes.into_iter()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::RegionId;

    use super::RegionScan;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, create_kafka_table, follower_region, leader_region,
    };

    #[tokio::test]
    async fn test_topic_scan_pauses_and_preserves_interleaved_regions() {
        let env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 9, 2).await;
        create_kafka_table(&env, 1025, 3, 2).await;
        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 2);
        let tracker = env.region_migration_manager.tracker();
        let first = scan
            .next_group(&env.table_metadata_manager, 1, &[], tracker, 2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(vec![RegionId::new(1024, 0), RegionId::new(1024, 2)], first);
        // The first child can be dispatched before even the first table is exhausted.
        assert_eq!(6, scan.current_table.as_ref().unwrap().1.len());
        assert!(!scan.exhausted);
        let mut seen = first;
        while let Some(group) = scan
            .next_group(&env.table_metadata_manager, 1, &[], tracker, 2)
            .await
            .unwrap()
        {
            assert!(group.len() <= 2);
            let first = group[0];
            assert!(
                group
                    .iter()
                    .all(|id| id.region_number() % 2 == first.region_number() % 2)
            );
            seen.extend(group);
        }
        seen.sort_unstable();
        let expected = (0..9)
            .map(|i| RegionId::new(1024, i))
            .chain((0..3).map(|i| RegionId::new(1025, i)))
            .collect::<Vec<_>>();
        assert_eq!(expected, seen);
    }

    #[tokio::test]
    async fn test_scan_leaders_and_count_followers() {
        let env = TestEnv::new().await;
        let first = RegionId::new(1024, 1);
        let second = RegionId::new(1024, 2);
        env.create_table(vec![leader_region(first, 1), follower_region(second, 2, 1)])
            .await;

        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 4);
        let mut leaders = vec![];
        while let Some(group) = scan
            .next_group(
                &env.table_metadata_manager,
                1,
                &[],
                env.region_migration_manager.tracker(),
                4,
            )
            .await
            .unwrap()
        {
            leaders.extend(group);
        }
        assert_eq!(vec![first], leaders);
        assert_eq!(1, scan.follower_count);
    }

    #[tokio::test]
    async fn test_scan_empty_datanode() {
        let env = TestEnv::new().await;

        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 4);
        let mut leaders = vec![];
        while let Some(group) = scan
            .next_group(
                &env.table_metadata_manager,
                1,
                &[],
                env.region_migration_manager.tracker(),
                4,
            )
            .await
            .unwrap()
        {
            leaders.extend(group);
        }
        assert!(leaders.is_empty());
        assert_eq!(0, scan.follower_count);
    }

    #[tokio::test]
    async fn test_scan_uses_authoritative_table_route() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        // The datanode is listed as a follower of the region, so it is not a leader even
        // though the table index of the datanode contains the table.
        env.create_table(vec![follower_region(region_id, 2, 1)])
            .await;

        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 4);
        let mut leaders = vec![];
        while let Some(group) = scan
            .next_group(
                &env.table_metadata_manager,
                1,
                &[],
                env.region_migration_manager.tracker(),
                4,
            )
            .await
            .unwrap()
        {
            leaders.extend(group);
        }
        assert!(leaders.is_empty());
        assert_eq!(1, scan.follower_count);
    }
    #[tokio::test]
    async fn test_scan_retains_group_remainder_across_budgets() {
        let env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 4, 1).await;
        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 4);
        let tracker = env.region_migration_manager.tracker();
        for (limit, expected) in [(1, vec![0]), (2, vec![1, 2]), (1, vec![3])] {
            let group = scan
                .next_group(&env.table_metadata_manager, 1, &[], tracker, limit)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                expected
                    .into_iter()
                    .map(|i| RegionId::new(1024, i))
                    .collect::<Vec<_>>(),
                group
            );
        }
        assert!(
            scan.next_group(&env.table_metadata_manager, 1, &[], tracker, 4)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_scan_keeps_local_wal_regions_separate() {
        let env = TestEnv::new().await;
        env.create_table(
            (0..3)
                .map(|i| leader_region(RegionId::new(1024, i), 1))
                .collect(),
        )
        .await;
        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 4);
        let tracker = env.region_migration_manager.tracker();
        for i in 0..3 {
            assert_eq!(
                Some(vec![RegionId::new(1024, i)]),
                scan.next_group(&env.table_metadata_manager, 1, &[], tracker, 4)
                    .await
                    .unwrap()
            );
        }
        assert!(
            scan.next_group(&env.table_metadata_manager, 1, &[], tracker, 4)
                .await
                .unwrap()
                .is_none()
        );
    }
}
