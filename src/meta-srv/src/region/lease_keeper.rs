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

pub mod file;
pub mod mito;
pub mod utils;

use std::collections::{HashMap, HashSet};

use common_catalog::consts::{FILE_ENGINE, MITO2_ENGINE};
use common_meta::key::TableMetadataManagerRef;
use futures::future::try_join_all;
use snafu::ResultExt;
use store_api::region_engine::RegionRole;
use store_api::storage::{RegionId, TableId};

use crate::error::{self, Result};

pub struct RegionLeaseKeeper {
    table_metadata_manager: TableMetadataManagerRef,
}

impl RegionLeaseKeeper {
    pub fn new(table_metadata_manager: TableMetadataManagerRef) -> Self {
        Self {
            table_metadata_manager,
        }
    }
}

impl RegionLeaseKeeper {
    /// Retains active mito regions(`datanode_regions`), returns inactive regions.
    ///
    /// **For mito regions:**
    ///
    /// - It removes a leader region if the `node_id` isn't the corresponding leader peer in `region_routes`.
    /// - It removes a follower region if the `node_id` isn't one of the peers in `region_routes`.
    ///
    /// **For file regions:**
    ///
    /// - It removes a follower region if the `node_id` isn't one of the peers in `region_routes`.
    ///
    /// **Common behaviors:**
    ///
    /// - It removes a region if the region's table metadata is not found.
    pub async fn retain_active_regions(
        &self,
        _cluster_id: u64,
        node_id: u64,
        engine: &str,
        datanode_regions: &mut Vec<(RegionId, RegionRole)>,
    ) -> Result<HashSet<RegionId>> {
        let table_route_manager = self.table_metadata_manager.table_route_manager();

        let handler = match engine {
            MITO2_ENGINE => mito::retain_active_regions,
            FILE_ENGINE => file::retain_active_regions,
            _ => {
                return error::UnexpectedSnafu {
                    violated: format!("Unknown engine: {engine}"),
                }
                .fail()
            }
        };

        let mut tables = HashMap::<TableId, Vec<(RegionId, RegionRole)>>::new();

        // Group by `table_id`.
        for (region_id, role) in datanode_regions.iter() {
            let table = tables.entry(region_id.table_id()).or_default();
            table.push((*region_id, *role));
        }

        let table_ids: Vec<TableId> = tables.keys().cloned().collect::<Vec<_>>();
        let metadata = try_join_all(
            table_ids
                .iter()
                .map(|table_id| table_route_manager.get(*table_id)),
        )
        .await
        .context(error::TableMetadataManagerSnafu)?;

        // All tables' metadata.
        let tables_metadata = table_ids
            .into_iter()
            .zip(metadata)
            .collect::<HashMap<TableId, Option<_>>>();

        let mut inactive_regions = HashSet::new();

        // Removes inactive regions.
        for (table_id, metadata) in tables_metadata {
            if let Some(metadata) = metadata {
                // Safety: Value must exist.
                let regions = tables.get_mut(&table_id).unwrap();
                let region_routes = &metadata.region_routes;

                inactive_regions.extend(handler(node_id, regions, region_routes));
            } else {
                // Removes table if metadata is not found.
                // Safety: Value must exist.
                let regions = tables
                    .remove(&table_id)
                    .unwrap()
                    .into_iter()
                    .map(|(region_id, _)| region_id);

                inactive_regions.extend(regions);
            }
        }

        let _ = datanode_regions
            .extract_if(|(region_id, _)| inactive_regions.contains(region_id))
            .collect::<Vec<_>>();

        Ok(inactive_regions)
    }

    #[cfg(test)]
    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_catalog::consts::MITO2_ENGINE;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::key::TableMetadataManager;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;

    use super::RegionLeaseKeeper;
    use crate::service::store::kv::KvBackendAdapter;
    use crate::service::store::memory::MemStore;

    fn new_test_keeper() -> RegionLeaseKeeper {
        let store = KvBackendAdapter::wrap(Arc::new(MemStore::new()));

        let table_metadata_manager = Arc::new(TableMetadataManager::new(store));

        RegionLeaseKeeper::new(table_metadata_manager)
    }

    #[tokio::test]
    async fn test_empty_table_routes() {
        let node_id = 1;
        let engine = MITO2_ENGINE;
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);

        let keeper = new_test_keeper();

        let mut datanode_regions = vec![(region_id, RegionRole::Leader)];

        let removed = keeper
            .retain_active_regions(0, node_id, engine, &mut datanode_regions)
            .await
            .unwrap();

        assert!(datanode_regions.is_empty());
        assert_eq!(removed.len(), 1);
        assert!(removed.contains(&region_id));
    }

    #[tokio::test]
    async fn test_retain_active_regions_simple() {
        let node_id = 1;
        let engine = MITO2_ENGINE;
        let region_number = 1u32;
        let table_id = 10;
        let region_id = RegionId::new(table_id, region_number);
        let peer = Peer::empty(node_id);
        let table_info = new_test_table_info(table_id, vec![region_number]).into();

        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(peer.clone()),
            ..Default::default()
        }];

        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();

        table_metadata_manager
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        // `inactive_regions` should be vec![region_id].
        let mut datanode_regions = vec![(region_id, RegionRole::Leader)];

        let inactive_regions = keeper
            .retain_active_regions(0, node_id, engine, &mut datanode_regions)
            .await
            .unwrap();

        assert!(inactive_regions.is_empty());
        assert_eq!(datanode_regions.len(), 1);

        // `inactive_regions` should be empty, because the `region_id` is a potential leader.
        let mut datanode_regions = vec![(region_id, RegionRole::Follower)];

        let inactive_regions = keeper
            .retain_active_regions(0, node_id, engine, &mut datanode_regions)
            .await
            .unwrap();

        assert!(inactive_regions.is_empty());
    }

    #[tokio::test]
    async fn test_retain_active_regions_2() {
        let node_id = 1;
        let engine = MITO2_ENGINE;
        let region_number = 1u32;
        let table_id = 10;
        let region_id = RegionId::new(table_id, region_number);
        let another_region_id = RegionId::new(table_id, region_number + 1);
        let unknown_region_id = RegionId::new(table_id + 1, region_number);

        let peer = Peer::empty(node_id);
        let another_peer = Peer::empty(node_id + 1);

        let table_info =
            new_test_table_info(table_id, vec![region_number, region_number + 1]).into();

        let region_routes = vec![
            RegionRoute {
                region: Region::new_test(region_id),
                leader_peer: Some(peer.clone()),
                ..Default::default()
            },
            RegionRoute {
                region: Region::new_test(another_region_id),
                leader_peer: None,
                follower_peers: vec![another_peer.clone()],
            },
        ];

        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();

        table_metadata_manager
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        // `inactive_regions` should be vec![unknown_region_id].
        let mut datanode_regions = vec![
            (region_id, RegionRole::Leader),
            (unknown_region_id, RegionRole::Follower),
        ];

        let inactive_regions = keeper
            .retain_active_regions(0, node_id, engine, &mut datanode_regions)
            .await
            .unwrap();

        assert_eq!(inactive_regions.len(), 1);
        assert!(inactive_regions.contains(&unknown_region_id));
        assert_eq!(datanode_regions.len(), 1);

        // `inactive_regions` should be vec![another_region_id], because the `another_region_id` is a active region of `another_peer`.
        let mut datanode_regions = vec![
            (region_id, RegionRole::Follower),
            (another_region_id, RegionRole::Follower),
        ];

        let inactive_regions = keeper
            .retain_active_regions(0, node_id, engine, &mut datanode_regions)
            .await
            .unwrap();

        assert_eq!(inactive_regions.len(), 1);
        assert!(inactive_regions.contains(&another_region_id));
        assert_eq!(datanode_regions.len(), 1);

        // `inactive_regions` should be vec![another_region_id], because the `another_region_id` is a active region of `another_peer`.
        let mut datanode_regions = vec![
            (region_id, RegionRole::Follower),
            (another_region_id, RegionRole::Leader),
        ];

        let inactive_regions = keeper
            .retain_active_regions(0, node_id, engine, &mut datanode_regions)
            .await
            .unwrap();

        assert_eq!(inactive_regions.len(), 1);
        assert!(inactive_regions.contains(&another_region_id));
        assert_eq!(datanode_regions.len(), 1);
    }
}
