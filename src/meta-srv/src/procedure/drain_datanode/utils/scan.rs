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
use crate::procedure::drain_datanode::utils::batch_aggregator::BatchAggregator;

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
        batch_size: usize,
        buffer_limit: usize,
    ) -> Self {
        Self {
            tables: metadata.datanode_table_manager().tables(node_id),
            current_table: None,
            batches: BatchAggregator::new(batch_size, buffer_limit),
            remaining_group: vec![],
            exhausted: false,
            produced_group: false,
            follower_count: 0,
        }
    }

    /// Returns at most `max_regions` regions that can share a migration procedure.
    /// Applies `should_skip` to source leaders before buffering them.
    /// Retains the rest of the group for the next call. A zero limit behaves as one.
    /// Returns `None` only after the metadata stream and buffered groups are exhausted.
    pub(crate) async fn next_group(
        &mut self,
        metadata: &TableMetadataManagerRef,
        node_id: DatanodeId,
        mut should_skip: impl FnMut(RegionId) -> bool,
        max_regions: usize,
    ) -> Result<Option<Vec<RegionId>>> {
        loop {
            if !self.remaining_group.is_empty() {
                let count = max_regions.max(1).min(self.remaining_group.len());
                self.produced_group = true;
                return Ok(Some(self.remaining_group.drain(..count).collect()));
            }
            if let Some(regions) = self.scan_current_table(node_id, &mut should_skip) {
                self.remaining_group = regions;
                continue;
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
            self.read_next_table(metadata).await?;
        }
    }

    /// Scans the current table until a group is ready or all its routes are consumed.
    fn scan_current_table(
        &mut self,
        node_id: DatanodeId,
        should_skip: &mut impl FnMut(RegionId) -> bool,
    ) -> Option<Vec<RegionId>> {
        let (value, routes) = self.current_table.as_mut()?;
        for route in routes.by_ref() {
            if route.follower_peers.iter().any(|peer| peer.id == node_id) {
                self.follower_count += 1;
            }
            if route.leader_peer.as_ref().map(|peer| peer.id) != Some(node_id)
                || should_skip(route.region.id)
            {
                continue;
            }
            let region_id = route.region.id;
            let Some(topic) =
                extract_topic_from_wal_options(region_id, &value.region_info.region_wal_options)
            else {
                // Regions without a shared WAL topic must migrate independently.
                return Some(vec![region_id]);
            };
            if let Some((_, mut regions)) = self.batches.push(topic, region_id) {
                regions.sort_unstable();
                regions.dedup();
                return Some(regions);
            }
        }
        self.current_table = None;
        None
    }

    /// Loads the next table's authoritative routes, skipping dropped and logical tables.
    async fn read_next_table(&mut self, metadata: &TableMetadataManagerRef) -> Result<()> {
        let Some(value) = self
            .tables
            .try_next()
            .await
            .context(error::TableMetadataManagerSnafu)?
        else {
            self.exhausted = true;
            return Ok(());
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
        Ok(())
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
        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 2, 8);
        let first = scan
            .next_group(&env.table_metadata_manager, 1, |_| false, 2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(vec![RegionId::new(1024, 0), RegionId::new(1024, 2)], first);
        // The first child can be dispatched before even the first table is exhausted.
        assert_eq!(6, scan.current_table.as_ref().unwrap().1.len());
        assert!(!scan.exhausted);
        let mut seen = first;
        while let Some(group) = scan
            .next_group(&env.table_metadata_manager, 1, |_| false, 2)
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

        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 4, 16);
        let mut leaders = vec![];
        while let Some(group) = scan
            .next_group(&env.table_metadata_manager, 1, |_| false, 4)
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

        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 4, 16);
        let mut leaders = vec![];
        while let Some(group) = scan
            .next_group(&env.table_metadata_manager, 1, |_| false, 4)
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

        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 4, 16);
        let mut leaders = vec![];
        while let Some(group) = scan
            .next_group(&env.table_metadata_manager, 1, |_| false, 4)
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
        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 4, 16);
        for (limit, expected) in [(1, vec![0]), (2, vec![1, 2]), (1, vec![3])] {
            let group = scan
                .next_group(&env.table_metadata_manager, 1, |_| false, limit)
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
            scan.next_group(&env.table_metadata_manager, 1, |_| false, 4)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_scan_skips_regions_selected_by_predicate() {
        for kafka in [false, true] {
            let env = TestEnv::new().await;
            if kafka {
                create_kafka_table(&env, 1024, 6, 1).await;
            } else {
                env.create_table(
                    (0..6)
                        .map(|i| leader_region(RegionId::new(1024, i), 1))
                        .collect(),
                )
                .await;
            }
            let skipped = [
                RegionId::new(1024, 1),
                RegionId::new(1024, 3),
                RegionId::new(1024, 5),
            ];
            let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 2, 8);
            let mut checked = Vec::new();
            let mut regions = Vec::new();
            while let Some(group) = scan
                .next_group(
                    &env.table_metadata_manager,
                    1,
                    |region| {
                        checked.push(region);
                        skipped.contains(&region)
                    },
                    1,
                )
                .await
                .unwrap()
            {
                regions.extend(group);
            }
            assert_eq!(
                (0..6).map(|i| RegionId::new(1024, i)).collect::<Vec<_>>(),
                checked
            );
            assert_eq!(
                vec![
                    RegionId::new(1024, 0),
                    RegionId::new(1024, 2),
                    RegionId::new(1024, 4)
                ],
                regions
            );
        }
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
        let mut scan = RegionScan::new(&env.table_metadata_manager, 1, 4, 16);
        for i in 0..3 {
            assert_eq!(
                Some(vec![RegionId::new(1024, i)]),
                scan.next_group(&env.table_metadata_manager, 1, |_| false, 4)
                    .await
                    .unwrap()
            );
        }
        assert!(
            scan.next_group(&env.table_metadata_manager, 1, |_| false, 4)
                .await
                .unwrap()
                .is_none()
        );
    }
}
