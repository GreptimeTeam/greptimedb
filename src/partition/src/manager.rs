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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::v1::Rows;
use common_meta::cache::{TableRoute, TableRouteCacheRef};
use common_meta::key::table_route::{PhysicalTableRouteValue, TableRouteManager};
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::{self, RegionRoute};
use datafusion_expr::{BinaryExpr, Expr, Operator};
use datatypes::prelude::Value;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

use crate::columns::RangeColumnsPartitionRule;
use crate::error::{FindLeaderSnafu, Result};
use crate::multi_dim::MultiDimPartitionRule;
use crate::partition::{PartitionBound, PartitionDef, PartitionExpr};
use crate::range::RangePartitionRule;
use crate::splitter::RowSplitter;
use crate::{error, PartitionRuleRef};

#[async_trait::async_trait]
pub trait TableRouteCacheInvalidator: Send + Sync {
    async fn invalidate_table_route(&self, table: TableId);
}

pub type TableRouteCacheInvalidatorRef = Arc<dyn TableRouteCacheInvalidator>;

pub type PartitionRuleManagerRef = Arc<PartitionRuleManager>;

/// PartitionRuleManager manages the table routes and partition rules.
/// It provides methods to find regions by:
/// - values (in case of insertion)
/// - filters (in case of select, deletion and update)
pub struct PartitionRuleManager {
    table_route_manager: TableRouteManager,
    table_route_cache: TableRouteCacheRef,
}

#[derive(Debug)]
pub struct PartitionInfo {
    pub id: RegionId,
    pub partition: PartitionDef,
}

impl PartitionRuleManager {
    pub fn new(kv_backend: KvBackendRef, table_route_cache: TableRouteCacheRef) -> Self {
        Self {
            table_route_manager: TableRouteManager::new(kv_backend),
            table_route_cache,
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

    pub async fn find_table_partitions(&self, table_id: TableId) -> Result<Vec<PartitionInfo>> {
        let region_routes = &self
            .find_physical_table_route(table_id)
            .await?
            .region_routes;
        ensure!(
            !region_routes.is_empty(),
            error::FindTableRoutesSnafu { table_id }
        );

        create_partitions_from_region_routes(table_id, region_routes)
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

    pub async fn find_table_partition_rule(&self, table_id: TableId) -> Result<PartitionRuleRef> {
        let partitions = self.find_table_partitions(table_id).await?;

        let partition_columns = partitions[0].partition.partition_columns();

        let regions = partitions
            .iter()
            .map(|x| x.id.region_number())
            .collect::<Vec<RegionNumber>>();

        let exprs = partitions
            .iter()
            .filter_map(|x| match &x.partition.partition_bounds()[0] {
                PartitionBound::Expr(e) => Some(e.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        let rule = MultiDimPartitionRule::try_new(partition_columns.clone(), regions, exprs)?;
        Ok(Arc::new(rule) as _)
    }

    /// Get partition rule of given table.
    pub async fn find_table_partition_rule_deprecated(
        &self,
        table_id: TableId,
    ) -> Result<PartitionRuleRef> {
        let partitions = self.find_table_partitions(table_id).await?;

        debug_assert!(!partitions.is_empty());

        let partition_columns = partitions[0].partition.partition_columns();

        let regions = partitions
            .iter()
            .map(|x| x.id.region_number())
            .collect::<Vec<RegionNumber>>();

        // TODO(LFC): Serializing and deserializing partition rule is ugly, must find a much more elegant way.
        let partition_rule: PartitionRuleRef = match partition_columns.len() {
            1 => {
                // Omit the last "MAXVALUE".
                let bounds = partitions
                    .iter()
                    .filter_map(|info| match &info.partition.partition_bounds()[0] {
                        PartitionBound::Value(v) => Some(v.clone()),
                        PartitionBound::MaxValue => None,
                        PartitionBound::Expr(_) => None,
                    })
                    .collect::<Vec<Value>>();
                Arc::new(RangePartitionRule::new(
                    partition_columns[0].clone(),
                    bounds,
                    regions,
                )) as _
            }
            _ => {
                let bounds = partitions
                    .iter()
                    .map(|x| x.partition.partition_bounds().clone())
                    .collect::<Vec<Vec<PartitionBound>>>();
                Arc::new(RangeColumnsPartitionRule::new(
                    partition_columns.clone(),
                    bounds,
                    regions,
                )) as _
            }
        };
        Ok(partition_rule)
    }

    /// Find regions in partition rule by filters.
    pub fn find_regions_by_filters(
        &self,
        partition_rule: PartitionRuleRef,
        filters: &[Expr],
    ) -> Result<Vec<RegionNumber>> {
        let regions = if let Some((first, rest)) = filters.split_first() {
            let mut target = find_regions0(partition_rule.clone(), first)?;
            for filter in rest {
                let regions = find_regions0(partition_rule.clone(), filter)?;

                // When all filters are provided as a collection, it often implicitly states that
                // "all filters must be satisfied". So we join all the results here.
                target.retain(|x| regions.contains(x));

                // Failed fast, empty collection join any is empty.
                if target.is_empty() {
                    break;
                }
            }
            target.into_iter().collect::<Vec<_>>()
        } else {
            partition_rule.find_regions_by_exprs(&[])?
        };
        ensure!(
            !regions.is_empty(),
            error::FindRegionsSnafu {
                filters: filters.to_vec()
            }
        );
        Ok(regions)
    }

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
        table_id: TableId,
        rows: Rows,
    ) -> Result<HashMap<RegionNumber, Rows>> {
        let partition_rule = self.find_table_partition_rule(table_id).await?;
        RowSplitter::new(partition_rule).split(rows)
    }
}

fn create_partitions_from_region_routes(
    table_id: TableId,
    region_routes: &[RegionRoute],
) -> Result<Vec<PartitionInfo>> {
    let mut partitions = Vec::with_capacity(region_routes.len());
    for r in region_routes {
        let partition = r
            .region
            .partition
            .as_ref()
            .context(error::FindRegionRoutesSnafu {
                region_id: r.region.id,
                table_id,
            })?;
        let partition_def = PartitionDef::try_from(partition)?;

        // The region routes belong to the physical table but are shared among all logical tables.
        // That it to say, the region id points to the physical table, so we need to use the actual
        // table id (which may be a logical table) to renew the region id.
        let id = RegionId::new(table_id, r.region.id.region_number());
        partitions.push(PartitionInfo {
            id,
            partition: partition_def,
        });
    }
    partitions.sort_by(|a, b| {
        a.partition
            .partition_bounds()
            .cmp(b.partition.partition_bounds())
    });

    ensure!(
        partitions
            .windows(2)
            .all(|w| w[0].partition.partition_columns() == w[1].partition.partition_columns()),
        error::InvalidTableRouteDataSnafu {
            table_id,
            err_msg: "partition columns of all regions are not the same"
        }
    );

    Ok(partitions)
}

fn find_regions0(partition_rule: PartitionRuleRef, filter: &Expr) -> Result<HashSet<RegionNumber>> {
    match filter {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if op.supports_propagation() => {
            let column_op_value = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(v)) => Some((&c.name, *op, v)),
                (Expr::Literal(v), Expr::Column(c)) => Some((
                    &c.name,
                    // Safety: previous branch ensures this is a comparison operator
                    op.swap().unwrap(),
                    v,
                )),
                _ => None,
            };
            if let Some((column, op, scalar)) = column_op_value {
                let value = Value::try_from(scalar.clone()).with_context(|_| {
                    error::ConvertScalarValueSnafu {
                        value: scalar.clone(),
                    }
                })?;
                return Ok(partition_rule
                    .find_regions_by_exprs(&[PartitionExpr::new(column, op, value)])?
                    .into_iter()
                    .collect::<HashSet<RegionNumber>>());
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(op, Operator::And | Operator::Or) =>
        {
            let left_regions = find_regions0(partition_rule.clone(), &left.clone())?;
            let right_regions = find_regions0(partition_rule.clone(), &right.clone())?;
            let regions = match op {
                Operator::And => left_regions
                    .intersection(&right_regions)
                    .cloned()
                    .collect::<HashSet<RegionNumber>>(),
                Operator::Or => left_regions
                    .union(&right_regions)
                    .cloned()
                    .collect::<HashSet<RegionNumber>>(),
                _ => unreachable!(),
            };
            return Ok(regions);
        }
        _ => (),
    }

    // Returns all regions for not supported partition expr as a safety hatch.
    Ok(partition_rule
        .find_regions_by_exprs(&[])?
        .into_iter()
        .collect::<HashSet<RegionNumber>>())
}
