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

//! [ExtensionPlanner] implementation for distributed planner

use std::sync::Arc;

use ahash::HashMap;
use arrow_schema::SortOptions;
use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_telemetry::debug;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{DataFusionError, TableReference};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use partition::expr::{Operand, PartitionExpr, RestrictedOp};
use partition::function::IcebergHash;
use partition::manager::{
    PartitionInfo, PartitionRuleManagerRef, create_partitions_from_region_routes,
};
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use store_api::metric_engine_consts::{DATA_SCHEMA_TABLE_ID_COLUMN_NAME, METRIC_ENGINE_NAME};
use store_api::storage::{RegionId, TableId};
use table::TableRef;
use table::metadata::{TableInfo, TableType};
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::dist_plan::PredicateExtractor;
use crate::dist_plan::merge_scan::{MergeScanExec, MergeScanLogicalPlan};
use crate::dist_plan::merge_sort::{MergeSortExec, MergeSortLogicalPlan};
use crate::dist_plan::region_pruner::ConstraintPruner;
use crate::error::{CatalogSnafu, PartitionRuleManagerSnafu, TableNotFoundSnafu};
use crate::optimizer::expr_depth;
use crate::region_query::RegionQueryHandlerRef;

/// Planner for converting merge sort logical plan to physical plan.
///
/// `MergeSortExec` always represents the distributed merge stage. It declares
/// the required input ordering to DataFusion, so `EnforceSorting` inserts a
/// `SortExec` below it when the input `MergeScanExec` cannot preserve per-region
/// ordering, for example when one output partition may merge multiple region
/// streams.
pub struct MergeSortExtensionPlanner {}

impl MergeSortExtensionPlanner {
    fn ordering(
        planner: &dyn PhysicalPlanner,
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
        merge_sort: &MergeSortLogicalPlan,
    ) -> Result<LexOrdering> {
        let ordering = merge_sort
            .expr
            .iter()
            .map(|sort_expr| {
                let physical_expr = planner.create_physical_expr(
                    &sort_expr.expr,
                    merge_sort.input.schema(),
                    session,
                    planning_ctx,
                )?;
                Ok(PhysicalSortExpr::new(
                    physical_expr,
                    SortOptions {
                        descending: !sort_expr.asc,
                        nulls_first: sort_expr.nulls_first,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        LexOrdering::new(ordering).ok_or_else(|| {
            DataFusionError::Internal(
                "Expect MergeSort to have non-empty sort expressions".to_string(),
            )
        })
    }
}

#[async_trait]
impl ExtensionPlanner for MergeSortExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(merge_sort) = node.as_any().downcast_ref::<MergeSortLogicalPlan>() {
            if let LogicalPlan::Extension(ext) = &merge_sort.input.as_ref()
                && ext
                    .node
                    .as_any()
                    .downcast_ref::<MergeScanLogicalPlan>()
                    .is_some()
            {
                let input = physical_inputs.first().cloned().ok_or_else(|| {
                    DataFusionError::Internal(
                        "Expect MergeSort to have one physical input".to_string(),
                    )
                })?;
                if input.downcast_ref::<MergeScanExec>().is_none() {
                    return Err(DataFusionError::Internal(format!(
                        "Expect MergeSort's input is a MergeScanExec, found {:?}",
                        physical_inputs
                    )));
                }

                let ordering = Self::ordering(planner, session, planning_ctx, merge_sort)?;
                Ok(Some(Arc::new(MergeSortExec::new(
                    ordering,
                    input,
                    merge_sort.fetch,
                ))))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

pub struct DistExtensionPlanner {
    catalog_manager: CatalogManagerRef,
    partition_rule_manager: PartitionRuleManagerRef,
    region_query_handler: RegionQueryHandlerRef,
    enable_per_region_metrics: bool,
}

impl DistExtensionPlanner {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_rule_manager: PartitionRuleManagerRef,
        region_query_handler: RegionQueryHandlerRef,
        enable_per_region_metrics: bool,
    ) -> Self {
        Self {
            catalog_manager,
            partition_rule_manager,
            region_query_handler,
            enable_per_region_metrics,
        }
    }
}

#[async_trait]
impl ExtensionPlanner for DistExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(merge_scan) = node.as_any().downcast_ref::<MergeScanLogicalPlan>() else {
            return Ok(None);
        };

        let input_plan = merge_scan.input();
        let fallback = |logical_plan| async move {
            let optimized_plan = self.optimize_input_logical_plan(session, logical_plan)?;
            planner
                .create_physical_plan(&optimized_plan, session)
                .await
                .map(Some)
        };

        if merge_scan.is_placeholder() {
            // ignore placeholder
            return fallback(input_plan).await;
        }

        let optimized_plan = input_plan;
        let direct_raw = matches!(input_plan, LogicalPlan::Extension(extension)
            if extension.node.as_any().is::<promql::extension_plan::RawSeriesSource>());
        let Some(table_name) = Self::extract_full_table_name(input_plan)? else {
            if direct_raw {
                return Err(DataFusionError::Plan(
                    "raw series source has no region route table".into(),
                ));
            }
            // no relation found in input plan, going to execute them locally
            return fallback(optimized_plan).await;
        };

        let (regions, series_disjoint) = match self.get_regions(&table_name, input_plan).await {
            Ok(regions) => regions,
            Err(error) if direct_raw => {
                return Err(error);
            }
            // Existing non-raw plans may still execute locally when no peers are found.
            Err(_) => return fallback(optimized_plan).await,
        };

        // TODO(ruihang): generate different execution plans for different variant merge operation
        let schema = merge_scan.schema().as_arrow();
        let session_state = session
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "MergeScan requires a SessionState for physical planning".to_string(),
                )
            })?;
        let query_ctx = session_state
            .config()
            .get_extension()
            .unwrap_or_else(QueryContext::arc);
        let merge_scan_plan = MergeScanExec::new(
            session_state,
            table_name,
            regions,
            input_plan.clone(),
            schema,
            self.region_query_handler.clone(),
            query_ctx,
            session.config().target_partitions(),
            merge_scan.remote_dyn_filter_producer_id(),
            self.enable_per_region_metrics,
        )?
        .with_raw_series_disjoint(series_disjoint)?;
        let merge_scan_plan = Arc::new(merge_scan_plan) as Arc<dyn ExecutionPlan>;
        // Regions answer their own stored series; a source that merges label sets does so here,
        // where every region's answer arrives.
        if let LogicalPlan::Extension(extension) = input_plan
            && let Some(source) = extension
                .node
                .as_any()
                .downcast_ref::<promql::extension_plan::RawSeriesSource>()
        {
            return source.merged_by_label_set(merge_scan_plan).map(Some);
        }
        Ok(Some(merge_scan_plan))
    }
}

impl DistExtensionPlanner {
    /// Extract fully resolved table name from logical plan
    fn extract_full_table_name(plan: &LogicalPlan) -> Result<Option<TableName>> {
        let mut extractor = TableScanExtractor::default();
        let _ = plan.visit(&mut extractor)?;
        Ok(extractor.table_name)
    }

    /// Selects the regions of the ordinary distributed plan.
    async fn get_regions(
        &self,
        table_name: &TableName,
        logical_plan: &LogicalPlan,
    ) -> Result<(Vec<RegionId>, bool)> {
        let mut extractor = TableScanExtractor::default();
        let _ = logical_plan.visit(&mut extractor)?;
        // Resolving by name again could bind an authorized scan to a replacement table.
        let captured_table = extractor.captured_table;
        let table = match &captured_table {
            Some(table) => table.clone(),
            None => self
                .catalog_manager
                .table(
                    &table_name.catalog_name,
                    &table_name.schema_name,
                    &table_name.table_name,
                    None,
                )
                .await
                .context(CatalogSnafu)?
                .with_context(|| TableNotFoundSnafu {
                    table: table_name.to_string(),
                })?,
        };

        let table_info = table.table_info();
        let (physical_table_id, physical_table_route) = self
            .partition_rule_manager
            .find_physical_table_route_with_id(table_info.table_id())
            .await
            .context(PartitionRuleManagerSnafu)?;
        let all_regions = physical_table_route
            .region_routes
            .iter()
            .map(|r| RegionId::new(table_info.table_id(), r.region.id.region_number()))
            .collect::<Vec<_>>();
        let logical_partition_columns = partition_column_types(&table_info);
        let partition_columns = logical_partition_columns
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        debug!(
            "DistExtensionPlanner: loaded table partition metadata, table: {}, table_id: {}, partition_key_indices: {:?}, partition_columns: {:?}, all_regions: {:?}",
            table_name,
            table_info.table_id(),
            table_info.meta.partition_key_indices,
            partition_columns,
            all_regions,
        );
        let partitioned_by_table_id = physical_table_id != table_info.table_id()
            && table_info.meta.engine == METRIC_ENGINE_NAME
            && table.partition_rules().is_some_and(|rules| {
                rules
                    .extra_phy_cols_not_in_logical_table
                    .iter()
                    .any(|name| name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            });
        let regions = 'pruning: {
            let partition_expressions = if partition_columns.is_empty() {
                Vec::new()
            } else {
                PredicateExtractor::extract_partition_expressions(logical_plan, &partition_columns)
                    .unwrap_or_else(|err| {
                        debug!(
                            "Failed to extract partition predicates for table {table_name}: {err}"
                        );
                        Vec::new()
                    })
            };

            if partition_expressions.is_empty()
                && (!partitioned_by_table_id || all_regions.len() == 1)
            {
                break 'pruning all_regions;
            }
            let partitions = match create_partitions_from_region_routes(
                table_info.table_id(),
                &physical_table_route.region_routes,
            ) {
                Ok(partitions) => partitions,
                Err(err) => {
                    debug!("Failed to load partitions for table {table_name}: {err}");
                    break 'pruning all_regions;
                }
            };

            let (mut regions, partitions) = if partitioned_by_table_id {
                let Some(candidates) =
                    simplify_metric_partitions(table_info.table_id(), partitions)
                else {
                    debug!("Could not simplify partition rules for table {table_name}");
                    break 'pruning all_regions;
                };
                candidates
            } else {
                (Vec::new(), partitions)
            };

            // Rules that simplify to true impose no constraints on the SQL predicates.
            // Send only the residual rules to the constraint pruner.
            if !partitions.is_empty()
                && !partition_expressions.is_empty()
                && let Some(column_types) = self
                    .partition_column_types_for_pruning(
                        table_name,
                        table_info.as_ref(),
                        physical_table_id,
                        &partition_expressions,
                    )
                    .await
            {
                match ConstraintPruner::prune_regions(
                    &partition_expressions,
                    &partitions,
                    column_types,
                ) {
                    Ok(pruned) => regions.extend(pruned),
                    Err(err) => {
                        debug!("Failed to prune partitions for table {table_name}: {err}");
                        regions.extend(partitions.iter().map(|partition| partition.id));
                    }
                }
            } else {
                regions.extend(partitions.iter().map(|partition| partition.id));
            }

            debug!(
                "Region pruning for table {table_name}: {} -> {} regions",
                all_regions.len(),
                regions.len()
            );
            regions
        };

        // A selected region is trivially disjoint. For several regions the proof must
        // use the captured scan and the physical route's table metadata, not a logical
        // metric table's possibly incomplete partition columns.
        let series_disjoint = if regions.len() <= 1 {
            true
        } else if captured_table.is_none() {
            false
        } else {
            let physical_info = if physical_table_id == table_info.table_id() {
                Some(table_info.clone())
            } else {
                self.catalog_manager
                    .table_info_by_id(physical_table_id)
                    .await
                    .ok()
                    .flatten()
            };
            physical_info.is_some_and(|info| {
                physical_regions_disjoint(&info, &physical_table_route.region_routes, &regions)
            })
        };
        Ok((regions, series_disjoint))
    }

    /// Resolves the partition-column types that are safe to use for region pruning.
    ///
    /// A logical metric table may not contain every physical partition column, either for backward
    /// compatibility or because its physical table was repartitioned after the logical table was
    /// created. Predicate extraction must remain bounded by the logical schema, while pruning
    /// needs the physical datatypes to evaluate route expressions. Any lookup failure or
    /// logical/physical datatype mismatch returns `None`, preserving the current candidate regions.
    async fn partition_column_types_for_pruning(
        &self,
        table_name: &TableName,
        logical_table_info: &TableInfo,
        physical_table_id: u32,
        partition_expressions: &[PartitionExpr],
    ) -> Option<HashMap<String, ConcreteDataType>> {
        let physical_partition_columns = if physical_table_id == logical_table_info.table_id() {
            partition_column_types(logical_table_info)
        } else {
            match self
                .catalog_manager
                .table_info_by_id(physical_table_id)
                .await
            {
                Ok(Some(physical_table_info)) => {
                    partition_column_types(physical_table_info.as_ref())
                }
                Ok(None) => {
                    debug!(
                        "DistExtensionPlanner: physical table info not found for table {} (id: {})",
                        table_name, physical_table_id
                    );
                    return None;
                }
                Err(err) => {
                    debug!(
                        "DistExtensionPlanner: failed to load physical table info for table {} (id: {}): {}",
                        table_name, physical_table_id, err
                    );
                    return None;
                }
            }
        };
        let physical_column_types = physical_partition_columns
            .into_iter()
            .collect::<HashMap<_, _>>();
        let logical_column_types = partition_column_types(logical_table_info)
            .into_iter()
            .collect::<HashMap<_, _>>();
        let mut predicate_column_names = std::collections::HashSet::new();
        for expression in partition_expressions {
            expression.collect_column_names(&mut predicate_column_names);
        }
        if predicate_column_names
            .iter()
            .any(|name| logical_column_types.get(name) != physical_column_types.get(name))
        {
            debug!(
                "DistExtensionPlanner: logical and physical partition metadata mismatch for table {} (physical id: {})",
                table_name, physical_table_id
            );
            return None;
        }

        Some(physical_column_types)
    }

    /// Input logical plan is analyzed. Thus only call logical optimizer to optimize it.
    fn optimize_input_logical_plan(
        &self,
        session: &dyn Session,
        plan: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        expr_depth::validate_plan(plan)?;
        let session_state = session
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "MergeScan requires a SessionState for logical optimization".to_string(),
                )
            })?;

        session_state
            .optimizer()
            .optimize(plan.clone(), session_state, |_, _| {})
    }
}

enum SimplifiedPartitionExpr {
    Constant(bool),
    Residual(PartitionExpr),
}

/// Returns unconditional regions and regions with residual partition rules.
/// Incomplete or inconsistent routing rules leave the original route in use.
fn simplify_metric_partitions(
    table_id: TableId,
    partitions: Vec<PartitionInfo>,
) -> Option<(Vec<RegionId>, Vec<PartitionInfo>)> {
    let table_id = Value::UInt32(table_id);
    let hash = IcebergHash::evaluate_value(&table_id).ok()?;
    let mut unconditional = Vec::new();
    let mut residual = Vec::new();
    for PartitionInfo { id, partition_expr } in partitions {
        match simplify_metric_partition_expr(partition_expr?, &table_id, &hash)? {
            SimplifiedPartitionExpr::Constant(false) => {}
            SimplifiedPartitionExpr::Constant(true) => unconditional.push(id),
            SimplifiedPartitionExpr::Residual(expr) => residual.push(PartitionInfo {
                id,
                partition_expr: Some(expr),
            }),
        }
    }
    // A complete set of physical rules covers every logical table ID.
    if unconditional.is_empty() && residual.is_empty() {
        return None;
    }
    Some((unconditional, residual))
}

fn simplify_metric_partition_expr(
    expr: PartitionExpr,
    table_id: &Value,
    hash: &Value,
) -> Option<SimplifiedPartitionExpr> {
    use SimplifiedPartitionExpr::{Constant, Residual};

    // Route deserialization canonicalizes comparisons to dimension op literal.
    match (expr.op, *expr.lhs, *expr.rhs) {
        (op @ (RestrictedOp::And | RestrictedOp::Or), Operand::Expr(lhs), Operand::Expr(rhs)) => {
            let lhs = simplify_metric_partition_expr(lhs, table_id, hash)?;
            let rhs = simplify_metric_partition_expr(rhs, table_id, hash)?;
            let absorbing = op == RestrictedOp::Or;
            match (lhs, rhs) {
                (Constant(lhs), Constant(rhs)) => {
                    Some(Constant(if absorbing { lhs || rhs } else { lhs && rhs }))
                }
                (Constant(value), residual) | (residual, Constant(value)) => {
                    Some(if value == absorbing {
                        Constant(value)
                    } else {
                        residual
                    })
                }
                (Residual(lhs), Residual(rhs)) => Some(Residual(PartitionExpr {
                    lhs: Box::new(Operand::Expr(lhs)),
                    op,
                    rhs: Box::new(Operand::Expr(rhs)),
                })),
            }
        }
        (RestrictedOp::And | RestrictedOp::Or, _, _) => None,
        (op, dimension @ (Operand::Column(_) | Operand::IcebergHash(_)), Operand::Value(bound)) => {
            let known = match &dimension {
                Operand::Column(name) if name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME => table_id,
                Operand::IcebergHash(function)
                    if function.column() == DATA_SCHEMA_TABLE_ID_COLUMN_NAME =>
                {
                    hash
                }
                _ => return Some(Residual(PartitionExpr::new(dimension, op, bound.into()))),
            };

            // DDL types the bound to the dimension. Partition routing orders NULL
            // before non-NULL values. A mismatched bound cannot safely be folded.
            if !matches!(bound, Value::Null) && known.data_type() != bound.data_type() {
                return None;
            }
            let ordering = known.cmp(&bound);
            let matches = match op {
                RestrictedOp::Eq => ordering.is_eq(),
                RestrictedOp::NotEq => !ordering.is_eq(),
                RestrictedOp::Lt => ordering.is_lt(),
                RestrictedOp::LtEq => ordering.is_le(),
                RestrictedOp::Gt => ordering.is_gt(),
                RestrictedOp::GtEq => ordering.is_ge(),
                RestrictedOp::And | RestrictedOp::Or => return None,
            };
            Some(Constant(matches))
        }
        _ => None,
    }
}

/// Prove that every physical routing column belongs to the complete raw primary key.
/// Unknown indices, missing route expressions, or route columns not represented in physical
/// partition metadata cannot establish disjointness. Reserved-looking names have no special status.
fn physical_regions_disjoint(
    table_info: &TableInfo,
    routes: &[common_meta::rpc::router::RegionRoute],
    regions: &[RegionId],
) -> bool {
    let columns = table_info.meta.schema.column_schemas();
    let partition_keys = &table_info.meta.partition_key_indices;
    // Routing by any nonempty subset of the complete key keeps equal complete keys together.
    // The reverse inclusion is not required: (host, rack) partitioned by host is disjoint.
    if partition_keys.is_empty()
        || table_info.meta.primary_key_indices.is_empty()
        || partition_keys.iter().any(|index| {
            columns.get(*index).is_none_or(|column| {
                column.is_time_index() || !table_info.meta.primary_key_indices.contains(index)
            })
        })
        || table_info
            .meta
            .primary_key_indices
            .iter()
            .any(|index| columns.get(*index).is_none())
    {
        return false;
    }
    let partition_names = partition_keys
        .iter()
        .map(|index| columns[*index].name.as_str())
        .collect::<std::collections::HashSet<_>>();
    for region in regions {
        let mut matching = routes
            .iter()
            .filter(|route| route.region.id.region_number() == region.region_number());
        let Some(route) = matching.next() else {
            return false;
        };
        if matching.next().is_some() {
            return false;
        }
        let Ok(Some(expression)) = PartitionExpr::from_json_str(&route.region.partition_expr())
        else {
            return false;
        };
        let mut names = std::collections::HashSet::new();
        expression.collect_column_names(&mut names);
        if names.is_empty()
            || names
                .iter()
                .any(|name| !partition_names.contains(name.as_str()))
        {
            return false;
        }
    }
    true
}

fn partition_column_types(table_info: &TableInfo) -> Vec<(String, ConcreteDataType)> {
    table_info
        .meta
        .partition_columns()
        .map(|column| (column.name.clone(), column.data_type.clone()))
        .collect()
}

/// Extract the scan name and captured table identity from a logical plan.
#[derive(Default)]
struct TableScanExtractor {
    pub table_name: Option<TableName>,
    captured_table: Option<TableRef>,
}

impl TreeNodeVisitor<'_> for TableScanExtractor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::TableScan(scan) => {
                if let Some(source) = scan.source.downcast_ref::<DefaultTableSource>()
                    && let Some(provider) = source
                        .table_provider
                        .downcast_ref::<DfTableProviderAdapter>()
                {
                    if provider.table().table_type() == TableType::Base {
                        self.captured_table = Some(provider.table());
                        let info = provider.table().table_info();
                        self.table_name = Some(TableName::new(
                            info.catalog_name.clone(),
                            info.schema_name.clone(),
                            info.name.clone(),
                        ));
                    }
                    return Ok(TreeNodeRecursion::Stop);
                }
                match &scan.table_name {
                    TableReference::Full {
                        catalog,
                        schema,
                        table,
                    } => {
                        self.table_name = Some(TableName::new(
                            catalog.to_string(),
                            schema.to_string(),
                            table.to_string(),
                        ));
                        Ok(TreeNodeRecursion::Stop)
                    }
                    // TODO(ruihang): Maybe the following two cases should not be valid
                    TableReference::Partial { schema, table } => {
                        self.table_name = Some(TableName::new(
                            DEFAULT_CATALOG_NAME.to_string(),
                            schema.to_string(),
                            table.to_string(),
                        ));
                        Ok(TreeNodeRecursion::Stop)
                    }
                    TableReference::Bare { table } => {
                        self.table_name = Some(TableName::new(
                            DEFAULT_CATALOG_NAME.to_string(),
                            DEFAULT_SCHEMA_NAME.to_string(),
                            table.to_string(),
                        ));
                        Ok(TreeNodeRecursion::Stop)
                    }
                }
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::region::{RemoteDynFilterUnregister, RemoteDynFilterUpdate};
    use async_trait::async_trait;
    use catalog::memory::MemoryCatalogManager;
    use catalog::{CatalogManagerRef, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::cache::new_table_route_cache;
    use common_meta::key::TableMetadataManager;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use datafusion::datasource::DefaultTableSource;
    use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, col as df_col, lit};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::value::Value;
    use moka::future::CacheBuilder;
    use partition::cache::new_partition_info_cache;
    use partition::expr::{Operand, PartitionExpr, RestrictedOp, col as partition_col};
    use partition::function::IcebergHash;
    use partition::manager::PartitionRuleManager;
    use session::ReadPreference;
    use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
    use store_api::storage::RegionId;
    use table::dist_table::DistTable;
    use table::metadata::{TableInfo, TableInfoBuilder, TableMeta, TableType};
    use table::table::PartitionRules;
    use table::table::adapter::DfTableProviderAdapter;
    use table::table_name::TableName;
    use table::test_util::EmptyTable;

    use super::{DistExtensionPlanner, physical_regions_disjoint};
    use crate::region_query::{RegionQueryHandler, RegionQueryTarget};

    const LOGICAL_TABLE_ID: u32 = 1024;
    const PHYSICAL_TABLE_ID: u32 = 2048;

    struct UnusedRegionQueryHandler;

    #[async_trait]
    impl RegionQueryHandler for UnusedRegionQueryHandler {
        async fn select_target(
            &self,
            _read_preference: ReadPreference,
            _region_id: RegionId,
        ) -> crate::error::Result<RegionQueryTarget> {
            unreachable!("get_regions does not select region query targets")
        }

        async fn do_get(
            &self,
            _target: &RegionQueryTarget,
            _request: QueryRequest,
        ) -> crate::error::Result<SendableRecordBatchStream> {
            unreachable!("get_regions does not query regions")
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _target: &RegionQueryTarget,
            _query_id: String,
            _update: RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unreachable!("get_regions does not update dynamic filters")
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _target: &RegionQueryTarget,
            _query_id: String,
            _unregister: RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unreachable!("get_regions does not unregister dynamic filters")
        }
    }

    fn table_info(
        table_id: u32,
        name: &str,
        columns: &[&str],
        partition_keys: Vec<usize>,
    ) -> TableInfo {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|name| {
                    if *name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME {
                        ColumnSchema::new(*name, ConcreteDataType::uint32_datatype(), false)
                    } else {
                        ColumnSchema::new(*name, ConcreteDataType::string_datatype(), true)
                    }
                })
                .collect(),
        ));
        let meta = TableMeta {
            schema,
            primary_key_indices: vec![],
            value_indices: vec![],
            engine: "metric".to_string(),
            next_column_id: columns.len() as u32,
            options: Default::default(),
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices: partition_keys,
            column_ids: (0..columns.len() as u32).collect(),
        };
        TableInfoBuilder::default()
            .table_id(table_id)
            .table_version(0)
            .name(name.to_string())
            .catalog_name(DEFAULT_CATALOG_NAME.to_string())
            .schema_name(DEFAULT_SCHEMA_NAME.to_string())
            .desc(None)
            .table_type(TableType::Base)
            .meta(meta)
            .build()
            .unwrap()
    }

    fn region_route(region_number: u32, expression: Option<PartitionExpr>) -> RegionRoute {
        RegionRoute {
            region: Region {
                id: RegionId::new(PHYSICAL_TABLE_ID, region_number),
                partition_expr: expression
                    .map(|expression| expression.as_json_str().unwrap())
                    .unwrap_or_default(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    async fn planner_and_plan(
        physical_partition_keys: Vec<usize>,
        expressions: Vec<Option<PartitionExpr>>,
    ) -> (DistExtensionPlanner, LogicalPlan, TableName) {
        let logical_info = table_info(LOGICAL_TABLE_ID, "logical", &["host"], vec![0]);
        let physical_info = table_info(
            PHYSICAL_TABLE_ID,
            "physical",
            &["host", "rack", DATA_SCHEMA_TABLE_ID_COLUMN_NAME],
            physical_partition_keys,
        );
        let hidden_columns: Vec<_> = physical_info
            .meta
            .partition_column_names()
            .filter(|name| {
                logical_info
                    .meta
                    .schema
                    .column_index_by_name(name)
                    .is_none()
            })
            .cloned()
            .collect();
        let partition_rules = (!hidden_columns.is_empty()).then_some(PartitionRules {
            extra_phy_cols_not_in_logical_table: hidden_columns,
        });
        let logical_table =
            DistTable::table_partitioned(Arc::new(logical_info.clone()), partition_rules);
        let physical_table = EmptyTable::from_table_info(&physical_info);
        let catalog_manager = MemoryCatalogManager::with_default_setup();
        for table in [&logical_table, &physical_table] {
            let info = table.table_info();
            catalog_manager
                .register_table_sync(RegisterTableRequest {
                    catalog: info.catalog_name.clone(),
                    schema: info.schema_name.clone(),
                    table_name: info.name.clone(),
                    table_id: info.table_id(),
                    table: table.clone(),
                })
                .unwrap();
        }

        let backend = Arc::new(MemoryKvBackend::default());
        let metadata_manager = TableMetadataManager::new(backend.clone());
        let routes = expressions
            .into_iter()
            .enumerate()
            .map(|(index, expression)| region_route(index as u32 + 1, expression))
            .collect();
        metadata_manager
            .create_table_metadata(
                physical_info,
                TableRouteValue::physical(routes),
                HashMap::new(),
            )
            .await
            .unwrap();
        metadata_manager
            .create_table_metadata(
                logical_info,
                TableRouteValue::logical(PHYSICAL_TABLE_ID),
                HashMap::new(),
            )
            .await
            .unwrap();

        let table_route_cache = Arc::new(new_table_route_cache(
            "planner-test-routes".to_string(),
            CacheBuilder::new(16).build(),
            backend.clone(),
        ));
        let partition_info_cache = Arc::new(new_partition_info_cache(
            "planner-test-partitions".to_string(),
            CacheBuilder::new(16).build(),
            table_route_cache.clone(),
        ));
        let partition_rule_manager = Arc::new(PartitionRuleManager::new(
            backend,
            table_route_cache,
            partition_info_cache,
        ));
        let (resolved_physical_id, physical_route) = partition_rule_manager
            .find_physical_table_route_with_id(PHYSICAL_TABLE_ID)
            .await
            .unwrap();
        assert_eq!(PHYSICAL_TABLE_ID, resolved_physical_id);
        let (resolved_logical_id, logical_route) = partition_rule_manager
            .find_physical_table_route_with_id(LOGICAL_TABLE_ID)
            .await
            .unwrap();
        assert_eq!(PHYSICAL_TABLE_ID, resolved_logical_id);
        assert_eq!(physical_route.region_routes, logical_route.region_routes);
        let catalog_manager: CatalogManagerRef = catalog_manager;
        let planner = DistExtensionPlanner::new(
            catalog_manager,
            partition_rule_manager,
            Arc::new(UnusedRegionQueryHandler),
            false,
        );
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(logical_table),
        )));
        let plan = LogicalPlanBuilder::scan_with_filters("logical", table_source, None, vec![])
            .unwrap()
            .filter(df_col("host").eq(lit("a")))
            .unwrap()
            .build()
            .unwrap();
        (
            planner,
            plan,
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "logical"),
        )
    }

    fn physical_partition_expressions() -> Vec<Option<PartitionExpr>> {
        vec![
            Some(partition_col("host").lt(Value::String("m".into()))),
            Some(
                partition_col("host")
                    .gt_eq(Value::String("m".into()))
                    .and(partition_col("rack").lt(Value::String("n".into()))),
            ),
            Some(
                partition_col("host")
                    .gt_eq(Value::String("m".into()))
                    .and(partition_col("rack").gt_eq(Value::String("n".into()))),
            ),
        ]
    }

    #[tokio::test]
    async fn region_routing_uses_captured_table() {
        let (mut planner, plan, table_name) = planner_and_plan(vec![0], vec![None]).await;
        planner.catalog_manager = MemoryCatalogManager::with_default_setup();
        let (regions, _) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(vec![RegionId::new(LOGICAL_TABLE_ID, 1)], regions);
    }

    #[tokio::test]
    async fn logical_table_pruning_uses_physical_partition_datatypes() {
        let (planner, plan, table_name) =
            planner_and_plan(vec![0, 1], physical_partition_expressions()).await;

        let (regions, _) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(vec![RegionId::new(LOGICAL_TABLE_ID, 1)], regions);
    }

    #[tokio::test]
    async fn missing_physical_partition_datatype_falls_back_to_all_logical_regions() {
        let (planner, plan, table_name) =
            planner_and_plan(vec![0], physical_partition_expressions()).await;

        let (regions, _) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_all_logical_regions(regions);
    }

    #[tokio::test]
    async fn missing_route_partition_expression_falls_back_to_all_logical_regions() {
        let mut expressions = physical_partition_expressions();
        expressions[1] = None;
        let (planner, plan, table_name) = planner_and_plan(vec![0, 1], expressions).await;

        let (regions, _) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_all_logical_regions(regions);
    }

    #[tokio::test]
    async fn unfiltered_tag_partitioned_scan_keeps_all_regions() {
        // The captured table still describes tag partitioning, even if a newer
        // route already contains rules for __table_id.
        let id = partition_col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME);
        let bound = Value::UInt32(LOGICAL_TABLE_ID);
        for tag_keys in [vec![0], vec![0, 1]] {
            let rules = vec![
                Some(id.clone().lt(bound.clone())),
                Some(id.clone().gt_eq(bound.clone())),
            ];
            let (planner, plan, table) = planner_and_plan(tag_keys, rules).await;
            let (regions, _) = planner.get_regions(&table, plan.inputs()[0]).await.unwrap();
            assert_eq!(
                regions,
                vec![
                    RegionId::new(LOGICAL_TABLE_ID, 1),
                    RegionId::new(LOGICAL_TABLE_ID, 2)
                ]
            );
        }
    }

    #[tokio::test]
    async fn prunes_raw_and_hashed_table_id_without_sql_predicates() {
        let id = Value::UInt32(LOGICAL_TABLE_ID);
        let hash = IcebergHash::evaluate_value(&id).unwrap();
        for (dimension, bound) in [
            (partition_col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME), id),
            (
                Operand::IcebergHash(IcebergHash::new(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)),
                hash,
            ),
        ] {
            let rules = vec![
                Some(dimension.clone().lt(bound.clone())),
                Some(dimension.gt_eq(bound)),
            ];
            let (planner, plan, table) = planner_and_plan(vec![2], rules).await;
            let (regions, _) = planner.get_regions(&table, plan.inputs()[0]).await.unwrap();
            assert_eq!(regions, vec![RegionId::new(LOGICAL_TABLE_ID, 2)]);
        }
    }

    #[tokio::test]
    async fn prunes_mixed_table_id_and_host_rules() {
        let id = partition_col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME);
        let host = partition_col("host");
        let boundary = Value::UInt32(LOGICAL_TABLE_ID);
        let rules = vec![
            Some(id.clone().lt(boundary.clone())),
            Some(
                id.clone()
                    .gt_eq(boundary.clone())
                    .and(host.clone().lt(Value::String("m".into()))),
            ),
            Some(
                id.gt_eq(boundary)
                    .and(host.gt_eq(Value::String("m".into()))),
            ),
        ];
        let (mut planner, plan, table) = planner_and_plan(vec![0, 2], rules).await;
        let candidates = vec![
            RegionId::new(LOGICAL_TABLE_ID, 2),
            RegionId::new(LOGICAL_TABLE_ID, 3),
        ];
        let (regions, _) = planner.get_regions(&table, plan.inputs()[0]).await.unwrap();
        assert_eq!(regions, candidates);
        let (regions, _) = planner.get_regions(&table, &plan).await.unwrap();
        assert_eq!(regions, vec![RegionId::new(LOGICAL_TABLE_ID, 2)]);

        planner.catalog_manager = MemoryCatalogManager::with_default_setup();
        let (regions, _) = planner.get_regions(&table, &plan).await.unwrap();
        assert_eq!(regions, candidates);
    }

    #[tokio::test]
    async fn folds_or_in_table_id_partition_rule() {
        let id = partition_col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME);
        let host = partition_col("host");
        for (boundary, expected) in [(LOGICAL_TABLE_ID, 2), (LOGICAL_TABLE_ID + 1, 1)] {
            let boundary = Value::UInt32(boundary);
            let rules = vec![
                Some(PartitionExpr::new(
                    Operand::Expr(id.clone().lt(boundary.clone())),
                    RestrictedOp::Or,
                    Operand::Expr(host.clone().gt_eq(Value::String("m".into()))),
                )),
                Some(
                    id.clone()
                        .gt_eq(boundary)
                        .and(host.clone().lt(Value::String("m".into()))),
                ),
            ];
            let (planner, plan, table) = planner_and_plan(vec![0, 2], rules).await;
            let (regions, _) = planner.get_regions(&table, &plan).await.unwrap();
            assert_eq!(regions, vec![RegionId::new(LOGICAL_TABLE_ID, expected)]);
        }
    }

    #[tokio::test]
    async fn incomplete_table_id_rules_keep_the_full_route() {
        let id = partition_col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME);
        for rule in [
            None,
            Some(id.clone().lt(Value::UInt32(0))),
            Some(id.clone().gt_eq(Value::Int64(0))),
        ] {
            let rules = vec![Some(id.clone().lt(Value::UInt32(LOGICAL_TABLE_ID))), rule];
            let (planner, plan, table) = planner_and_plan(vec![2], rules).await;
            let (regions, _) = planner.get_regions(&table, plan.inputs()[0]).await.unwrap();
            assert_eq!(
                regions,
                vec![
                    RegionId::new(LOGICAL_TABLE_ID, 1),
                    RegionId::new(LOGICAL_TABLE_ID, 2)
                ]
            );
        }
    }

    #[test]
    fn disjoint_requires_raw_key_routing_and_valid_unique_routes() {
        let regions = vec![
            RegionId::new(LOGICAL_TABLE_ID, 1),
            RegionId::new(LOGICAL_TABLE_ID, 2),
        ];
        let routes = vec![
            region_route(1, Some(partition_col("host").lt(Value::String("m".into())))),
            region_route(
                2,
                Some(partition_col("host").gt_eq(Value::String("m".into()))),
            ),
        ];
        let mut info = table_info(PHYSICAL_TABLE_ID, "physical", &["host", "rack"], vec![0]);
        info.meta.primary_key_indices = vec![0];
        assert!(physical_regions_disjoint(&info, &routes, &regions));
        info.meta.primary_key_indices = vec![0, 1];
        assert!(physical_regions_disjoint(&info, &routes, &regions));
        info.meta.partition_key_indices = vec![0, 1];
        assert!(physical_regions_disjoint(&info, &routes, &regions));
        info.meta.partition_key_indices = vec![0, 2];
        assert!(!physical_regions_disjoint(&info, &routes, &regions));
        info.meta.partition_key_indices = vec![0];
        info.meta.primary_key_indices = vec![0];
        let mut duplicate = routes.clone();
        duplicate.push(routes[0].clone());
        assert!(!physical_regions_disjoint(&info, &duplicate, &regions));
        let mut missing = routes.clone();
        missing[1] = region_route(2, None);
        assert!(!physical_regions_disjoint(&info, &missing, &regions));
        let mut unknown = routes.clone();
        unknown[1] = region_route(2, Some(partition_col("rack").lt(Value::String("m".into()))));
        assert!(!physical_regions_disjoint(&info, &unknown, &regions));
        let mut malformed = routes.clone();
        malformed[1].region.partition_expr = "{invalid".into();
        assert!(!physical_regions_disjoint(&info, &malformed, &regions));
        assert!(!physical_regions_disjoint(&info, &routes[..1], &regions));

        info.meta.schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ]));
        info.meta.partition_key_indices = vec![0, 1];
        info.meta.primary_key_indices = vec![0, 1];
        assert!(!physical_regions_disjoint(&info, &routes, &regions));
    }

    #[tokio::test]
    async fn disjoint_proof_uses_physical_not_logical_metadata() {
        let (planner, plan, table_name) =
            planner_and_plan(vec![0, 1], physical_partition_expressions()).await;
        // The host predicate is selective; one selected region is always disjoint.
        let (regions, disjoint) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(regions.len(), 1);
        assert!(disjoint);
        // With no pruning the fixture's physical metadata has no PK, so it cannot prove
        // disjointness even though the logical partition keys name host.
        let LogicalPlan::Filter(filter) = plan else {
            panic!("expected filter")
        };
        let (regions, disjoint) = planner
            .get_regions(&table_name, &filter.input)
            .await
            .unwrap();
        assert_eq!(regions.len(), 3);
        assert!(!disjoint);
    }

    fn assert_all_logical_regions(mut regions: Vec<RegionId>) {
        regions.sort_unstable();
        assert_eq!(
            vec![
                RegionId::new(LOGICAL_TABLE_ID, 1),
                RegionId::new(LOGICAL_TABLE_ID, 2),
                RegionId::new(LOGICAL_TABLE_ID, 3),
            ],
            regions
        );
    }
}
