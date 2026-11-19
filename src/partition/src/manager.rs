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

    /// Returns the partition rule of the physical table that backs `table_info`, together
    /// with the partition expression version of each region.
    ///
    /// The rule's expressions and regions come from that physical table, while its
    /// [`partition_columns`](crate::PartitionRule::partition_columns) are the columns
    /// declared by `table_info` **union** every column those expressions reference: a
    /// logical table needn't carry the column the physical table is partitioned on.
    /// Callers must therefore not assume the returned columns equal
    /// `table_info.meta.partition_column_names()`, nor that every one of them exists in
    /// `table_info`'s schema.
    pub async fn find_table_partition_rule(
        &self,
        table_info: &TableInfo,
    ) -> Result<(PartitionRuleRef, HashMap<RegionNumber, Option<u64>>)> {
        let partition_info = self
            .find_physical_partition_info(table_info.table_id())
            .await?;

        build_table_partition_rule(
            table_info.meta.partition_column_names().cloned(),
            &partition_info,
        )
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

/// Builds the partition rule of a physical table from declared columns and partition info.
///
/// `declared_partition_columns` are the partition key columns the calling table declares,
/// while `partition_info` holds the physical table's partition expressions and regions. The
/// caller's table may be a logical table sharing the physical one, and it doesn't have
/// to carry every column the physical table is partitioned on, so the declared columns
/// are extended with the columns the expressions reference - declared order first, the
/// remaining names sorted. Without them the expressions can't be resolved: rows are
/// rejected with `UndefinedColumn`, or, when the declared list is empty, every row takes
/// the unpartitioned early return and is sent to region 0 without evaluating anything.
/// A table that owns every referenced column gets exactly its declared list back.
fn build_table_partition_rule(
    declared_partition_columns: impl IntoIterator<Item = String>,
    partition_info: &PhysicalPartitionInfo,
) -> Result<(PartitionRuleRef, HashMap<RegionNumber, Option<u64>>)> {
    let mut partition_columns = declared_partition_columns.into_iter().collect::<Vec<_>>();

    let partition_versions = partition_info
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

    // The expressions come from the physical table, so they may reference columns the
    // logical table doesn't declare. Append the missing ones in a stable order and keep
    // the declared order untouched so a table owning every column keeps its old rule.
    let mut referenced_columns = HashSet::new();
    for expr in &exprs {
        expr.collect_column_names(&mut referenced_columns);
    }
    let mut referenced_columns = referenced_columns.into_iter().collect::<Vec<_>>();
    referenced_columns.sort();
    let mut columns_in_rule = partition_columns
        .iter()
        .cloned()
        .collect::<HashSet<String>>();
    for column in referenced_columns {
        if columns_in_rule.insert(column.clone()) {
            partition_columns.push(column);
        }
    }

    let partition_rule = Arc::new(MultiDimPartitionRule::try_new(
        partition_columns,
        regions,
        exprs,
        false,
    )?) as _;
    Ok((partition_rule, partition_versions))
}

#[cfg(test)]
mod tests {
    use api::v1::helper::{field_column_schema, tag_column_schema};
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row, Rows};
    use datatypes::arrow::array::{RecordBatch, StringArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::value::Value;

    use super::*;
    use crate::expr::col;

    fn mock_physical_partition_info(
        partitions: impl IntoIterator<Item = (RegionNumber, Option<PartitionExpr>)>,
    ) -> PhysicalPartitionInfo {
        PhysicalPartitionInfo {
            partitions: partitions
                .into_iter()
                .map(|(region_number, partition_expr)| PartitionInfoWithVersion {
                    id: RegionId::new(1, region_number),
                    partition_expr,
                    partition_expr_version: None,
                })
                .collect(),
        }
    }

    fn rows_with_i32(column: &str, values: &[i32]) -> Rows {
        Rows {
            schema: vec![tag_column_schema(column, ColumnDataType::Int32)],
            rows: values
                .iter()
                .map(|v| Row {
                    values: vec![ValueData::I32Value(*v).into()],
                })
                .collect(),
        }
    }

    fn split_regions(rule: PartitionRuleRef, rows: Rows) -> HashMap<RegionNumber, Rows> {
        RowSplitter::new(rule).split(rows).unwrap()
    }

    fn sorted_regions(splits: &HashMap<RegionNumber, Rows>) -> Vec<RegionNumber> {
        let mut regions = splits.keys().copied().collect::<Vec<_>>();
        regions.sort_unstable();
        regions
    }

    /// A logical table that declares a subset of the physical partition columns must
    /// still get an evaluable rule.
    #[test]
    fn test_rule_covers_physical_only_partition_column() {
        // `a` is declared by the logical table, `b` only exists in the physical rule.
        let partition_info = mock_physical_partition_info([
            (1, Some(col("a").lt(Value::Int32(10)))),
            (
                2,
                Some(
                    col("a")
                        .gt_eq(Value::Int32(10))
                        .and(col("b").gt(Value::Int32(50))),
                ),
            ),
        ]);

        let (rule, _) = build_table_partition_rule(["a".to_string()], &partition_info).unwrap();
        assert_eq!(&["a", "b"], rule.partition_columns());

        // A table that declares every referenced column keeps its columns and order.
        let (rule, _) =
            build_table_partition_rule(["a".to_string(), "b".to_string()], &partition_info)
                .unwrap();
        assert_eq!(&["a", "b"], rule.partition_columns());

        // A row carrying only `a` is routed by the expression on `a` instead of
        // failing with `UndefinedColumn`.
        let (rule, _) = build_table_partition_rule(["a".to_string()], &partition_info).unwrap();
        let splits = split_regions(rule, rows_with_i32("a", &[5]));
        assert_eq!(vec![1], sorted_regions(&splits));

        // A row that carries `b` is routed by the physical expression on `b`.
        let (rule, _) = build_table_partition_rule(["a".to_string()], &partition_info).unwrap();
        let rows = Rows {
            schema: vec![
                tag_column_schema("a", ColumnDataType::Int32),
                tag_column_schema("b", ColumnDataType::Int32),
            ],
            rows: vec![Row {
                values: vec![
                    ValueData::I32Value(100).into(),
                    ValueData::I32Value(60).into(),
                ],
            }],
        };
        let splits = split_regions(rule, rows);
        assert_eq!(vec![2], sorted_regions(&splits));

        // Without `b` the value is NULL, which fails `b > 50`, so no expression
        // accepts the row and it keeps taking the default region, as today.
        let (rule, _) = build_table_partition_rule(["a".to_string()], &partition_info).unwrap();
        let splits = split_regions(rule, rows_with_i32("a", &[100]));
        assert_eq!(vec![0], sorted_regions(&splits));
    }

    /// A logical table without partition columns must not degenerate into "no
    /// partition" routing when the physical rule has expressions.
    #[test]
    fn test_rule_covers_expression_columns_without_declared_partition_columns() {
        let partition_info = mock_physical_partition_info([
            (1, Some(col("b").lt(Value::Int32(10)))),
            // Default region.
            (2, None),
        ]);

        let (rule, _) =
            build_table_partition_rule(std::iter::empty::<String>(), &partition_info).unwrap();
        assert_eq!(&["b"], rule.partition_columns());

        // Before the fix an empty column list made `RowSplitter` send every row to
        // region 0 without evaluating an expression. Now `b` is read from the row.
        let splits = split_regions(rule, rows_with_i32("b", &[5]));
        assert_eq!(vec![1], sorted_regions(&splits));

        // A row that doesn't carry `b` is routed as NULL instead of being rejected.
        // `Value` orders NULL below any other value, so `b < 10` holds for it.
        let (rule, _) =
            build_table_partition_rule(std::iter::empty::<String>(), &partition_info).unwrap();
        let splits = split_regions(rule, rows_with_i32("other", &[5]));
        assert_eq!(vec![1], sorted_regions(&splits));
    }

    /// Rows that carry no `host` column at all.
    fn rows_without_host(cpu: &[f64]) -> Rows {
        Rows {
            schema: vec![field_column_schema("cpu", ColumnDataType::Float64)],
            rows: cpu
                .iter()
                .map(|v| Row {
                    values: vec![ValueData::F64Value(*v).into()],
                })
                .collect(),
        }
    }

    /// A batch that has the partition column, but every one of its values is NULL.
    fn all_null_host_batch(num_rows: usize) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![None::<&str>; num_rows]))],
        )
        .unwrap()
    }

    /// Regression for the issue's second example: a row whose physical partition column
    /// is missing must land in the region whose expression accepts `NULL` (region 1 for
    /// `host < 'm'`), not in the fallback region 0 that the degenerate "no partition
    /// columns" rule used to return without evaluating anything. Stands in for a logical
    /// table that declares no partition columns and has no `host` column at all.
    #[test]
    fn test_null_partition_value_routes_to_expression_region() {
        let partition_info = mock_physical_partition_info([
            (0, Some(col("host").gt_eq(Value::from("m")))),
            (1, Some(col("host").lt(Value::from("m")))),
        ]);
        let (rule, _) =
            build_table_partition_rule(std::iter::empty::<String>(), &partition_info).unwrap();

        // Every row lacks `host`, so every row is routed by the `host < 'm'` region.
        let rows = rows_without_host(&[1.0, 2.0, 3.0]);
        let num_rows = rows.rows.len();
        let splits = split_regions(rule.clone(), rows);
        assert_eq!(vec![1], sorted_regions(&splits));
        assert_eq!(num_rows, splits[&1].rows.len());
        assert_eq!(&["host"], rule.partition_columns());

        // Region 1's own predicate selects the NULL rows; region 0's one rejects them.
        let masks = rule
            .split_record_batch(&all_null_host_batch(num_rows))
            .unwrap();
        assert!(
            !masks.contains_key(&0),
            "the `host >= 'm'` region must not select NULL hosts"
        );
        assert!(masks.get(&1).expect("region 1 accepts NULL").select_all());
    }

    /// Control: with the predicate order reversed region 0 owns the NULLs, and an
    /// unpartitioned physical table keeps the "no partition" early return. Both route
    /// exactly as they did before the rule stopped being degenerate, so only routing
    /// is asserted here - the rule's column list is expected to change. Like its
    /// sibling, the rule is built for a table that declares no partition columns.
    #[test]
    fn test_null_partition_value_control_routing_unchanged() {
        let partition_info = mock_physical_partition_info([
            (0, Some(col("host").lt(Value::from("m")))),
            (1, Some(col("host").gt_eq(Value::from("m")))),
        ]);
        let (rule, _) =
            build_table_partition_rule(std::iter::empty::<String>(), &partition_info).unwrap();
        let rows = rows_without_host(&[1.0, 2.0]);
        let num_rows = rows.rows.len();
        let splits = split_regions(rule, rows);
        assert_eq!(vec![0], sorted_regions(&splits));
        assert_eq!(num_rows, splits[&0].rows.len());

        // No expression at all: the rule keeps no partition columns and `RowSplitter`
        // keeps its early return, so every row still goes to region 0.
        let unpartitioned = mock_physical_partition_info([(0, None)]);
        let (rule, _) =
            build_table_partition_rule(std::iter::empty::<String>(), &unpartitioned).unwrap();
        assert!(rule.partition_columns().is_empty());
        let rows = rows_without_host(&[1.0, 2.0, 3.0]);
        let num_rows = rows.rows.len();
        let splits = split_regions(rule, rows);
        assert_eq!(vec![0], sorted_regions(&splits));
        assert_eq!(num_rows, splits[&0].rows.len());
    }
}
