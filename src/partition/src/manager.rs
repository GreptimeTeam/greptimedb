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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::Rows;
use common_meta::cache::{TableRoute, TableRouteCacheRef};
use common_meta::key::table_route::{PhysicalTableRouteValue, TableRouteManager};
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::{self, RegionRoute};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::{TableId, TableInfo};

use crate::cache::{CachedPartitionInfo, PartitionInfoCacheRef, PhysicalPartitionInfo};
use crate::error::{FindLeaderSnafu, Result};
use crate::expr::PartitionExpr;
use crate::multi_dim::MultiDimPartitionRule;
use crate::splitter::RowSplitter;
use crate::{PartitionRuleRef, error};

pub type PartitionRuleManagerRef = Arc<PartitionRuleManager>;

/// PartitionRuleManager manages the table routes and partition rules.
/// It provides methods to find regions by:
/// - values (in case of insertion)
/// - filters (in case of select, deletion and update)
pub struct PartitionRuleManager {
    table_route_manager: TableRouteManager,
    table_route_cache: TableRouteCacheRef,
    partition_info_cache: PartitionInfoCacheRef,
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub id: RegionId,
    pub partition_expr: Option<PartitionExpr>,
}

#[derive(Debug, Clone)]
pub struct PartitionInfoWithVersion {
    pub id: RegionId,
    pub partition_expr: Option<PartitionExpr>,
    pub partition_expr_version: Option<u64>,
}

impl PartitionRuleManager {
    pub fn new(
        kv_backend: KvBackendRef,
        table_route_cache: TableRouteCacheRef,
        partition_info_cache: PartitionInfoCacheRef,
    ) -> Self {
        Self {
            table_route_manager: TableRouteManager::new(kv_backend),
            table_route_cache,
            partition_info_cache,
        }
    }

    pub async fn find_physical_table_route(
        &self,
        table_id: TableId,
    ) -> Result<Arc<PhysicalTableRouteValue>> {
        match self
            .table_route_cache
            .get(table_id)
            .await
            .context(error::TableRouteManagerSnafu)?
            .context(error::TableRouteNotFoundSnafu { table_id })?
            .as_ref()
        {
            TableRoute::Physical(physical_table_route) => Ok(physical_table_route.clone()),
            TableRoute::Logical(logical_table_route) => {
                let physical_table_id = logical_table_route.physical_table_id();
                let physical_table_route = self
                    .table_route_cache
                    .get(physical_table_id)
                    .await
                    .context(error::TableRouteManagerSnafu)?
                    .context(error::TableRouteNotFoundSnafu { table_id })?;

                let physical_table_route = physical_table_route
                    .as_physical_table_route_ref()
                    .context(error::UnexpectedSnafu{
                        err_msg: format!(
                            "Expected the physical table route, but got logical table route, table: {table_id}"
                        ),
                    })?;

                Ok(physical_table_route.clone())
            }
        }
    }

    pub async fn batch_find_region_routes(
        &self,
        table_ids: &[TableId],
    ) -> Result<HashMap<TableId, Vec<RegionRoute>>> {
        let table_routes = self
            .table_route_manager
            .batch_get_physical_table_routes(table_ids)
            .await
            .context(error::TableRouteManagerSnafu)?;

        let mut table_region_routes = HashMap::with_capacity(table_routes.len());

        for (table_id, table_route) in table_routes {
            let region_routes = table_route.region_routes;
            table_region_routes.insert(table_id, region_routes);
        }

        Ok(table_region_routes)
    }

    /// Returns the physical partition info with version.
    pub async fn find_physical_partition_info(
        &self,
        table_id: TableId,
    ) -> Result<Arc<PhysicalPartitionInfo>> {
        let cached = self
            .partition_info_cache
            .get(table_id)
            .await
            .context(error::GetPartitionInfoSnafu)?
            .context(error::TableRouteNotFoundSnafu { table_id })?;
        match cached {
            CachedPartitionInfo::Physical(info) => Ok(info),
            CachedPartitionInfo::Logical(physical_table_id) => {
                let cached = self
                    .partition_info_cache
                    .get(physical_table_id)
                    .await
                    .context(error::GetPartitionInfoSnafu)?
                    .context(error::TableRouteNotFoundSnafu {
                        table_id: physical_table_id,
                    })?;
                let info = cached.into_physical().context(error::UnexpectedSnafu{
                        err_msg: format!(
                            "Expected the physical partition info, but got logical partable route, table: {physical_table_id}"
                        )
                    })?;

                Ok(info)
            }
        }
    }

    pub async fn batch_find_table_partitions(
        &self,
        table_ids: &[TableId],
    ) -> Result<HashMap<TableId, Vec<PartitionInfo>>> {
        let batch_region_routes = self.batch_find_region_routes(table_ids).await?;

        let mut results = HashMap::with_capacity(table_ids.len());

        for (table_id, region_routes) in batch_region_routes {
            results.insert(
                table_id,
                create_partitions_from_region_routes(table_id, &region_routes)?,
            );
        }

        Ok(results)
    }

    pub async fn find_table_partition_rule(
        &self,
        table_info: &TableInfo,
    ) -> Result<(PartitionRuleRef, HashMap<RegionNumber, Option<u64>>)> {
        let partition_columns = table_info
            .meta
            .partition_column_names()
            .cloned()
            .collect::<Vec<_>>();

        let partition_info = self
            .find_physical_partition_info(table_info.table_id())
            .await?;
        let partition_versions = partition_info
            .partitions
            .iter()
            .map(|r| (r.id.region_number(), r.partition_expr_version))
            .collect::<HashMap<RegionNumber, Option<u64>>>();
        let regions = partition_info
            .partitions
            .iter()
            .map(|x| x.id.region_number())
            .collect::<Vec<RegionNumber>>();
        let exprs = partition_info
            .partitions
            .iter()
            .filter_map(|x| x.partition_expr.as_ref())
            .cloned()
            .collect::<Vec<_>>();
        let partition_rule = Arc::new(MultiDimPartitionRule::try_new(
            partition_columns,
            regions,
            exprs,
            false,
        )?) as _;
        Ok((partition_rule, partition_versions))
    }

    /// Find the leader of the region.
    pub async fn find_region_leader(&self, region_id: RegionId) -> Result<Peer> {
        let region_routes = &self
            .find_physical_table_route(region_id.table_id())
            .await?
            .region_routes;

        router::find_region_leader(region_routes, region_id.region_number()).context(
            FindLeaderSnafu {
                region_id,
                table_id: region_id.table_id(),
            },
        )
    }

    pub async fn split_rows(
        &self,
        table_info: &TableInfo,
        rows: Rows,
    ) -> Result<HashMap<RegionNumber, (Rows, Option<u64>)>> {
        let (partition_rule, partition_versions) =
            self.find_table_partition_rule(table_info).await?;

        let result = RowSplitter::new(partition_rule)
            .split(rows)?
            .into_iter()
            .map(|(region_number, rows)| {
                (
                    region_number,
                    (
                        rows,
                        partition_versions
                            .get(&region_number)
                            .copied()
                            .unwrap_or_default(),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();

        Ok(result)
    }
}

/// Creates partitions from region routes.
pub fn create_partitions_from_region_routes(
    table_id: TableId,
    region_routes: &[RegionRoute],
) -> Result<Vec<PartitionInfo>> {
    let mut partitions = Vec::with_capacity(region_routes.len());
    for r in region_routes {
        let partition_expr = PartitionExpr::from_json_str(&r.region.partition_expr())?;

        // The region routes belong to the physical table but are shared among all logical tables.
        // That it to say, the region id points to the physical table, so we need to use the actual
        // table id (which may be a logical table) to renew the region id.
        let id = RegionId::new(table_id, r.region.id.region_number());
        partitions.push(PartitionInfo { id, partition_expr });
    }

    Ok(partitions)
}
