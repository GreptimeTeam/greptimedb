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
use std::collections::{HashMap, HashSet};

use common_meta::ddl::drop_table::executor::DropTableExecutor;
use common_meta::lock_key::TableLock;
use common_meta::node_manager::NodeManagerRef;
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_meta::rpc::router::RegionRoute;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::{info, warn};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};
use table::table_name::TableName;
use table::table_reference::TableReference;
use tokio::time::Instant;

use crate::error::{self, Result};
use crate::procedure::repartition::group::region_routes;
use crate::procedure::repartition::repartition_end::RepartitionEnd;
use crate::procedure::repartition::{Context, State};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeallocateRegion;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DeallocateRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let timer = Instant::now();
        let region_to_deallocate = ctx
            .persistent_ctx
            .plans
            .iter()
            .map(|p| p.pending_deallocate_region_ids.len())
            .sum::<usize>();
        if region_to_deallocate == 0 {
            ctx.update_deallocate_region_elapsed(timer.elapsed());
            return Ok((Box::new(RepartitionEnd), Status::done()));
        }

        let table_id = ctx.persistent_ctx.table_id;
        let pending_deallocate_region_ids = ctx
            .persistent_ctx
            .plans
            .iter()
            .flat_map(|p| p.pending_deallocate_region_ids.iter())
            .cloned()
            .collect::<HashSet<_>>();
        let dealloc_count = pending_deallocate_region_ids.len();
        info!(
            "Deallocating regions for repartition, table_id: {}, count: {}, regions: {:?}",
            table_id, dealloc_count, pending_deallocate_region_ids
        );

        let table_lock = TableLock::Write(table_id).into();
        let _guard = procedure_ctx.provider.acquire_lock(&table_lock).await;
        let table_route_value = ctx.get_table_route_value().await?;
        let deallocating_regions = {
            let region_routes = region_routes(table_id, &table_route_value)?;
            Self::filter_deallocatable_region_routes(
                table_id,
                region_routes,
                &pending_deallocate_region_ids,
            )
        };

        let table_ref = TableReference::full(
            &ctx.persistent_ctx.catalog_name,
            &ctx.persistent_ctx.schema_name,
            &ctx.persistent_ctx.table_name,
        );
        // Deallocates the regions on datanodes.
        Self::deallocate_regions(
            &ctx.node_manager,
            &ctx.leader_region_registry,
            table_ref.into(),
            table_id,
            &deallocating_regions,
        )
        .await?;

        // Safety: the table route must be physical, so we can safely unwrap the region routes.
        let region_routes = table_route_value.region_routes().unwrap();
        let new_region_routes =
            Self::generate_region_routes(region_routes, &pending_deallocate_region_ids);
        ctx.update_table_route(&table_route_value, new_region_routes, HashMap::new())
            .await?;
        ctx.invalidate_table_cache().await?;

        ctx.update_deallocate_region_elapsed(timer.elapsed());
        Ok((Box::new(RepartitionEnd), Status::executing(false)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl DeallocateRegion {
    async fn deallocate_regions(
        node_manager: &NodeManagerRef,
        leader_region_registry: &LeaderRegionRegistryRef,
        table: TableName,
        table_id: TableId,
        region_routes: &[RegionRoute],
    ) -> Result<()> {
        let executor = DropTableExecutor::new(table, table_id, false);
        // Note: Consider adding an option to forcefully drop the physical region,
        // which would involve dropping all logical regions associated with that physical region.
        executor
            .on_drop_regions(
                node_manager,
                leader_region_registry,
                region_routes,
                false,
                true,
            )
            .await
            .context(error::DeallocateRegionsSnafu { table_id })?;

        Ok(())
    }

    fn filter_deallocatable_region_routes(
        table_id: TableId,
        region_routes: &[RegionRoute],
        pending_deallocate_region_ids: &HashSet<RegionId>,
    ) -> Vec<RegionRoute> {
        let region_routes_map = region_routes
            .iter()
            .map(|r| (r.region.id, r.clone()))
            .collect::<HashMap<_, _>>();
        pending_deallocate_region_ids
            .iter()
            .filter_map(|region_id| match region_routes_map.get(region_id) {
                Some(region_route) => Some(region_route.clone()),
                None => {
                    warn!(
                        "Region {} not found during deallocate regions for table {:?}",
                        region_id, table_id
                    );
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    fn generate_region_routes(
        region_routes: &[RegionRoute],
        pending_deallocate_region_ids: &HashSet<RegionId>,
    ) -> Vec<RegionRoute> {
        // Safety: the table route must be physical, so we can safely unwrap the region routes.
        region_routes
            .iter()
            .filter(|r| !pending_deallocate_region_ids.contains(&r.region.id))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::{RegionId, TableId};

    use crate::procedure::repartition::deallocate_region::DeallocateRegion;

    fn test_region_routes(table_id: TableId) -> Vec<RegionRoute> {
        vec![
            RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 1),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            },
            RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 2),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(2)),
                ..Default::default()
            },
        ]
    }

    #[test]
    fn test_filter_deallocatable_region_routes() {
        let table_id = 1024;
        let region_routes = test_region_routes(table_id);
        let pending_deallocate_region_ids = HashSet::from([RegionId::new(table_id, 1)]);
        let deallocatable_region_routes = DeallocateRegion::filter_deallocatable_region_routes(
            table_id,
            &region_routes,
            &pending_deallocate_region_ids,
        );
        assert_eq!(deallocatable_region_routes.len(), 1);
        assert_eq!(
            deallocatable_region_routes[0].region.id,
            RegionId::new(table_id, 1)
        );
    }

    #[test]
    fn test_generate_region_routes() {
        let table_id = 1024;
        let region_routes = test_region_routes(table_id);
        let pending_deallocate_region_ids = HashSet::from([RegionId::new(table_id, 1)]);
        let new_region_routes = DeallocateRegion::generate_region_routes(
            &region_routes,
            &pending_deallocate_region_ids,
        );
        assert_eq!(new_region_routes.len(), 1);
        assert_eq!(new_region_routes[0].region.id, RegionId::new(table_id, 2));
    }
}
