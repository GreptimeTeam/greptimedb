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
use datatypes::arrow::array::UInt32Array;
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME,
};
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::{TableId, TableInfo};

use crate::cache::{CachedPartitionInfo, PartitionInfoCacheRef, PhysicalPartitionInfo};
use crate::error::{FindLeaderSnafu, Result};
use crate::expr::PartitionExpr;
use crate::multi_dim::MultiDimPartitionRule;
use crate::splitter::RowSplitter;
use crate::{PartitionRule, PartitionRuleRef, error};

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

/// A single captured routing snapshot of one table.
///
/// The rule, the expression versions and the identity route all come from the
/// same route metadata, so a request built from one snapshot stays consistent
/// while the table is repartitioned.
pub struct TablePartitionRule {
    pub rule: PartitionRuleRef,
    pub versions: HashMap<RegionNumber, Option<u64>>,
    /// Region that receives every row of this table, when its partition rule
    /// routes by the table's own identity instead of by the payload.
    pub identity_region: Option<RegionNumber>,
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
        Ok(self.find_physical_table_route_with_id(table_id).await?.1)
    }

    /// Returns the resolved physical table id and its route.
    pub async fn find_physical_table_route_with_id(
        &self,
        table_id: TableId,
    ) -> Result<(TableId, Arc<PhysicalTableRouteValue>)> {
        match self
            .table_route_cache
            .get(table_id)
            .await
            .context(error::TableRouteManagerSnafu)?
            .context(error::TableRouteNotFoundSnafu { table_id })?
            .as_ref()
        {
            TableRoute::Physical(physical_table_route) => {
                Ok((table_id, physical_table_route.clone()))
            }
            TableRoute::Logical(logical_table_route) => {
                let physical_table_id = logical_table_route.physical_table_id();
                let physical_table_route = self
                    .table_route_cache
                    .get(physical_table_id)
                    .await
                    .context(error::TableRouteManagerSnafu)?
                    .context(error::TableRouteNotFoundSnafu {
                        table_id: physical_table_id,
                    })?;

                let physical_table_route = physical_table_route
                    .as_physical_table_route_ref()
                    .context(error::UnexpectedSnafu{
                        err_msg: format!(
                            "Expected the physical table route, but got logical table route, table: {physical_table_id}"
                        ),
                    })?;

                Ok((physical_table_id, physical_table_route.clone()))
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
    ) -> Result<TablePartitionRule> {
        let mut partition_columns = table_info
            .meta
            .partition_column_names()
            .cloned()
            .collect::<Vec<_>>();

        let partition_info = self
            .find_physical_partition_info(table_info.table_id())
            .await?;
        let versions = partition_info
            .partitions
            .iter()
            .map(|r| (r.id.region_number(), r.partition_expr_version))
            .collect::<HashMap<RegionNumber, Option<u64>>>();
        let (mut regions, exprs): (Vec<_>, Vec<_>) = partition_info
            .partitions
            .iter()
            .filter_map(|partition| {
                partition
                    .partition_expr
                    .as_ref()
                    .map(|expr| (partition.id.region_number(), expr.clone()))
            })
            .unzip();
        // Empty expressions belong to the default region. Keep it after the
        // explicit regions so expression indices still identify the right region.
        regions.extend(
            partition_info
                .partitions
                .iter()
                .filter(|partition| partition.partition_expr.is_none())
                .map(|partition| partition.id.region_number()),
        );

        // A logical table's rows never carry the physical schema's internal
        // columns, so the rule has to learn the identity column from the table.
        let identity = routes_by_identity(table_info, &exprs)?;
        if identity
            && !partition_columns
                .iter()
                .any(|c| c == DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
        {
            partition_columns.push(DATA_SCHEMA_TABLE_ID_COLUMN_NAME.into());
        }
        let rule: PartitionRuleRef = Arc::new(MultiDimPartitionRule::try_new(
            partition_columns,
            regions,
            exprs,
            false,
        )?);
        let identity_region = if identity {
            Some(find_identity_region(
                rule.as_ref(),
                table_info.table_id(),
                &partition_info,
            )?)
        } else {
            None
        };

        Ok(TablePartitionRule {
            rule,
            versions,
            identity_region,
        })
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
        let partition = self.find_table_partition_rule(table_info).await?;
        if let Some(region) = partition.identity_region {
            if rows.rows.is_empty() {
                return Ok(HashMap::new());
            }
            let version = partition.versions.get(&region).copied().unwrap_or_default();
            return Ok(HashMap::from([(region, (rows, version))]));
        }

        let result = RowSplitter::new(partition.rule)
            .split(rows)?
            .into_iter()
            .map(|(region_number, rows)| {
                (
                    region_number,
                    (
                        rows,
                        partition
                            .versions
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

/// Whether the table's own identity is the only way to evaluate its rule.
///
/// A logical metric table stores its rows under a physical table whose internal
/// [`DATA_SCHEMA_TABLE_ID_COLUMN_NAME`] column records the owning logical table,
/// while the logical schema and its write payloads have no such column. Once the
/// physical table is partitioned on that column, the rule can only be evaluated
/// against the identity itself.
///
/// Mixed rules are rejected here rather than routed with a guessed value, and
/// the value is never taken from a payload column that happens to share the name.
fn routes_by_identity(table_info: &TableInfo, exprs: &[PartitionExpr]) -> Result<bool> {
    let logical_metric = table_info.meta.engine == METRIC_ENGINE_NAME
        && table_info
            .meta
            .options
            .extra_options
            .contains_key(LOGICAL_TABLE_METADATA_KEY);
    if !logical_metric {
        return Ok(false);
    }

    let mut referenced = HashSet::new();
    for expr in exprs {
        expr.collect_column_names(&mut referenced);
    }
    if !referenced.contains(DATA_SCHEMA_TABLE_ID_COLUMN_NAME) {
        return Ok(false);
    }
    ensure!(
        referenced.len() == 1,
        error::UnsupportedTablePartitionSnafu {
            reason: format!(
                "logical table {} is partitioned on {} together with other columns: {:?}",
                table_info.table_id(),
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                referenced
            ),
        }
    );

    Ok(true)
}

/// Resolves the identity row to the single region that owns the whole table.
///
/// The row is evaluated by the rule itself so the expression keeps the same type
/// coordination, hash support and default handling as any other write.
///
/// Repartitioning validates the new expressions with `PartitionChecker`, which
/// rejects gaps and overlaps, so a rule built by it selects exactly one region for
/// any identity. The checks below therefore only reject corrupt route metadata: the
/// rule sends rows no expression matched to a fallback region, which must not be
/// mistaken for the region that owns the table.
fn find_identity_region(
    rule: &dyn PartitionRule,
    table_id: TableId,
    partition_info: &PhysicalPartitionInfo,
) -> Result<RegionNumber> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
        DataType::UInt32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(UInt32Array::from(vec![table_id]))])
        .context(error::ComputeArrowKernelSnafu)?;
    let masks = rule.split_record_batch(&batch)?;

    let mut selected = masks
        .iter()
        .filter(|(_, mask)| mask.selected_rows() > 0)
        .map(|(region, _)| region);
    let region = selected.next().copied().context(error::UnexpectedSnafu {
        err_msg: format!("no partition selected the identity of table {table_id}"),
    })?;
    ensure!(
        selected.next().is_none(),
        error::UnsupportedTablePartitionSnafu {
            reason: format!("multiple partitions select the identity of table {table_id}"),
        }
    );
    // Only a region with its own expression can own the table. Otherwise the rule
    // had no match and routed the row to its fallback region instead.
    ensure!(
        partition_info.partitions.iter().any(|partition| {
            partition.id.region_number() == region && partition.partition_expr.is_some()
        }),
        error::UnsupportedTablePartitionSnafu {
            reason: format!("no partition expression owns region {region} of table {table_id}"),
        }
    );

    Ok(region)
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
