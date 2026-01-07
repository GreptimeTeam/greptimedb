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

use std::sync::Arc;

use common_telemetry::debug;
use store_api::storage::{RegionId, TableId};

use crate::error::Result;
use crate::peer::PeerAllocator;
use crate::rpc::router::{Region, RegionRoute};

pub type TableRouteAllocatorRef = Arc<dyn TableRouteAllocator>;

#[async_trait::async_trait]
pub trait TableRouteAllocator: Send + Sync {
    async fn allocate(
        &self,
        table_id: TableId,
        partition_exprs: &[&str],
        next_region_number: u32,
    ) -> Result<Vec<RegionRoute>>;
}

#[async_trait::async_trait]
impl<T: PeerAllocator> TableRouteAllocator for T {
    async fn allocate(
        &self,
        table_id: TableId,
        partition_exprs: &[&str],
        next_region_number: u32,
    ) -> Result<Vec<RegionRoute>> {
        let regions = partition_exprs.len().max(1);
        let peers = self.alloc(regions).await?;
        debug!(
            "Allocated peers {:?} for table {}, next region number: {}",
            peers, table_id, next_region_number
        );

        let mut region_routes = partition_exprs
            .iter()
            .enumerate()
            .map(|(i, partition)| {
                let region_number = next_region_number + i as u32;
                let region = Region {
                    id: RegionId::new(table_id, region_number),
                    partition_expr: partition.to_string(),
                    ..Default::default()
                };

                let peer = peers[i % peers.len()].clone();

                RegionRoute {
                    region,
                    leader_peer: Some(peer),
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();

        // If the table has no partitions, we need to create a default region.
        if region_routes.is_empty() {
            region_routes.push(RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, next_region_number),
                    ..Default::default()
                },
                leader_peer: Some(peers[0].clone()),
                ..Default::default()
            });
        }

        Ok(region_routes)
    }
}
