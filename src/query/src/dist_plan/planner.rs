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

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use arrow_schema::SortOptions;
use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE, MITO2_ENGINE,
};
use common_meta::rpc::router::RegionRoute;
use common_telemetry::debug;
use common_time::timestamp::TimeUnit;
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
use partition::expr::PartitionExpr;
use partition::manager::{PartitionRuleManagerRef, create_partitions_from_region_routes};
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::TableRef;
use table::metadata::TableInfo;
pub use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::dist_plan::PredicateExtractor;
use crate::dist_plan::merge_scan::{MergeScanExec, MergeScanLogicalPlan};
use crate::dist_plan::merge_sort::{MergeSortExec, MergeSortLogicalPlan};
use crate::dist_plan::raw_fan_in::{NativeRawFanInExec, bind_raw_series_table};
use crate::dist_plan::region_pruner::ConstraintPruner;
use crate::error::{CatalogSnafu, PartitionRuleManagerSnafu, TableNotFoundSnafu};
use crate::optimizer::expr_depth;
use crate::region_query::{RegionQueryHandlerRef, RegionQueryTarget};
use datatypes::series::SeriesOrdering;
use promql::extension_plan::{FusedInputBinding, FusedSeries};

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
    /// Gates capture of native raw routes; ordinary region selection is unchanged.
    capture_native_raw_route: bool,
}

impl DistExtensionPlanner {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_rule_manager: PartitionRuleManagerRef,
        region_query_handler: RegionQueryHandlerRef,
        enable_per_region_metrics: bool,
        capture_native_raw_route: bool,
    ) -> Self {
        Self {
            catalog_manager,
            partition_rule_manager,
            region_query_handler,
            enable_per_region_metrics,
            capture_native_raw_route,
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
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(fused) = node.as_any().downcast_ref::<FusedSeries>() {
            if fused.children().len() != 1 || physical_inputs.len() != 1 {
                return Ok(None);
            }
            let Some(scan) = physical_inputs[0].downcast_ref::<MergeScanExec>() else {
                // LocalRegionScan and other local sources keep their existing physical planning.
                return Ok(None);
            };
            let Some(source) = scan.native_raw_source() else {
                return Ok(None);
            };
            let binding = fused.binding();
            let metric_name = binding.metric_name.as_deref().ok_or_else(|| {
                DataFusionError::Plan("native raw FusedSeries has no metric name".into())
            })?;
            let tag_names = binding
                .tags
                .iter()
                .filter(|name| name.as_str() != "__name__")
                .cloned()
                .collect::<Vec<_>>();
            let field_names = fused.raw_source_fields();
            let (read, expected, decode) = bind_raw_series_table(
                &source.table_info,
                metric_name,
                &tag_names,
                &field_names,
                source
                    .route
                    .targets
                    .first()
                    .map(|(region, _)| *region)
                    // This ID is only a template: an empty route never dispatches it.
                    .unwrap_or_else(|| RegionId::new(source.table_info.table_id(), 0)),
            )?;
            let regions = source.route.targets.clone();
            let input = Arc::new(NativeRawFanInExec::try_new(
                read,
                Arc::new(expected),
                decode.clone(),
                regions,
                source.plan.clone(),
                Arc::clone(source.region_query_handler),
                Arc::clone(source.query_ctx),
            )?);
            return fused
                .to_execution_plan(vec![(
                    input,
                    FusedInputBinding::Remote(decode),
                    SeriesOrdering::KeyThenTime,
                )])
                .map(Some);
        }

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
        let Some(table_name) = Self::extract_full_table_name(input_plan)? else {
            // no relation found in input plan, going to execute them locally
            return fallback(optimized_plan).await;
        };

        let Ok((regions, native_raw_route)) = self.get_regions(&table_name, input_plan).await
        else {
            // no peers found, going to execute them locally
            return fallback(optimized_plan).await;
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
        )?;
        // Captured provenance only: ordinary execution of `MergeScanExec` never
        // reads it, so a missing capture keeps the plan exactly as before.
        let merge_scan_plan = match native_raw_route {
            Some((native_raw_route, table_info)) => {
                merge_scan_plan.with_native_raw_route(native_raw_route, table_info)
            }
            None => merge_scan_plan,
        };
        Ok(Some(Arc::new(merge_scan_plan) as _))
    }
}

impl DistExtensionPlanner {
    /// Extract fully resolved table name from logical plan
    fn extract_full_table_name(plan: &LogicalPlan) -> Result<Option<TableName>> {
        let mut extractor = TableScanExtractor::default();
        let _ = plan.visit(&mut extractor)?;
        Ok(extractor.table_name)
    }

    /// Selects the regions of the ordinary distributed plan and captures the
    /// authorized native raw route for the same selection when available.
    ///
    /// Every ordinary selection path (pruned and all fallbacks) returns the
    /// regions a `MergeScanExec` would use, plus the frozen region targets
    /// captured in that exact order. The capture is provenance for a native raw
    /// consumer and never changes ordinary region selection or its error handling.
    async fn get_regions(
        &self,
        table_name: &TableName,
        logical_plan: &LogicalPlan,
    ) -> Result<(
        Vec<RegionId>,
        Option<(CaptureNativeRawRoute, Arc<TableInfo>)>,
    )> {
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
        // Ordinary region selection. Every fallback keeps all regions, exactly
        // as before the native raw route existed.
        let regions = 'pruning: {
            if partition_columns.is_empty() {
                break 'pruning all_regions;
            }
            // Extract predicates from logical plan
            let partition_expressions = match PredicateExtractor::extract_partition_expressions(
                logical_plan,
                &partition_columns,
            ) {
                Ok(expressions) => expressions,
                Err(err) => {
                    common_telemetry::debug!(
                        "Failed to extract partition expressions for table {} (id: {}), using all regions: {:?}",
                        table_name,
                        table.table_info().table_id(),
                        err
                    );
                    break 'pruning all_regions;
                }
            };

            if partition_expressions.is_empty() {
                break 'pruning all_regions;
            }

            let Some(partition_column_types) = self
                .partition_column_types_for_pruning(
                    table_name,
                    table_info.as_ref(),
                    physical_table_id,
                    &partition_expressions,
                    &all_regions,
                )
                .await
            else {
                break 'pruning all_regions;
            };

            // Get partition information for the table if partition rule manager is available
            let partitions = match create_partitions_from_region_routes(
                table_info.table_id(),
                &physical_table_route.region_routes,
            ) {
                Ok(partitions) => partitions,
                Err(err) => {
                    common_telemetry::debug!(
                        "Failed to get partition information for table {}, using all regions: {:?}",
                        table_name,
                        err
                    );
                    break 'pruning all_regions;
                }
            };
            if partitions.is_empty() {
                break 'pruning all_regions;
            }
            // Apply region pruning based on partition rules
            let pruned_regions = match ConstraintPruner::prune_regions(
                &partition_expressions,
                &partitions,
                partition_column_types,
            ) {
                Ok(regions) => regions,
                Err(err) => {
                    common_telemetry::debug!(
                        "Failed to prune regions for table {}, using all regions: {:?}",
                        table_name,
                        err
                    );
                    break 'pruning all_regions;
                }
            };

            common_telemetry::debug!(
                "Region pruning for table {}: {} partition expressions applied, pruned from {} to {} regions",
                table_name,
                partition_expressions.len(),
                all_regions.len(),
                pruned_regions.len()
            );

            pruned_regions
        };

        // Captured from the same physical route that selected the regions above,
        // in the same order. This never mutates or reorders `regions`; the
        // experimental gate only skips the capture.
        let native_raw_route = if self.capture_native_raw_route {
            capture_native_raw_route(
                captured_table.as_ref(),
                &table_info,
                physical_table_id,
                &physical_table_route.region_routes,
                &regions,
            )
        } else {
            None
        };
        Ok((
            regions,
            native_raw_route.and_then(|route| {
                captured_table
                    .as_ref()
                    .map(|table| (route, table.table_info()))
            }),
        ))
    }

    /// Resolves the partition-column types that are safe to use for region pruning.
    ///
    /// A logical metric table may not contain every physical partition column, either for backward
    /// compatibility or because its physical table was repartitioned after the logical table was
    /// created. Predicate extraction must remain bounded by the logical schema, while pruning
    /// needs the physical datatypes to evaluate route expressions. Any lookup failure or
    /// logical/physical datatype mismatch returns `None`, causing the caller to scan all regions.
    async fn partition_column_types_for_pruning(
        &self,
        table_name: &TableName,
        logical_table_info: &TableInfo,
        physical_table_id: u32,
        partition_expressions: &[PartitionExpr],
        all_regions: &[RegionId],
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
                        "DistExtensionPlanner: physical table info not found for table {} (id: {}), using all regions: {:?}",
                        table_name, physical_table_id, all_regions
                    );
                    return None;
                }
                Err(err) => {
                    debug!(
                        "DistExtensionPlanner: failed to load physical table info for table {} (id: {}): {}, using all regions: {:?}",
                        table_name, physical_table_id, err, all_regions
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
                "DistExtensionPlanner: logical and physical partition metadata mismatch for table {} (physical id: {}), using all regions: {:?}",
                table_name, physical_table_id, all_regions
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

fn partition_column_types(table_info: &TableInfo) -> Vec<(String, ConcreteDataType)> {
    table_info
        .meta
        .partition_columns()
        .map(|column| (column.name.clone(), column.data_type.clone()))
        .collect()
}

/// Frozen route provenance of an ordinary distributed plan for a native raw
/// consumer.
///
/// `targets` pairs each region selected by the ordinary plan with the leader
/// target of the physical route that selected it, in the exact order the
/// ordinary `MergeScanExec` uses. It is captured for selected regions on the
/// table authorized by the scan; ordinary execution never reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CaptureNativeRawRoute {
    /// `targets[i]` is the frozen leader target of the i-th selected region.
    pub(crate) targets: Vec<(RegionId, RegionQueryTarget)>,
}

/// Freezes the leader target of every `selected_regions` entry from
/// `region_routes`, the physical route that selected them.
///
/// Returns `None` when the table or selected routes cannot safely identify
/// their physical leader targets. Declining never changes ordinary region
/// selection or its error handling.
fn capture_native_raw_route(
    captured_table: Option<&TableRef>,
    table_info: &TableInfo,
    physical_table_id: u32,
    region_routes: &[RegionRoute],
    selected_regions: &[RegionId],
) -> Option<CaptureNativeRawRoute> {
    // Only the table captured from the authorized scan can prove the route: a
    // name-only lookup may have bound the plan to a replacement table.
    let captured_table = captured_table?;
    if captured_table.table_type() != TableType::Base
        || !matches!(table_info.meta.engine.as_str(), MITO_ENGINE | MITO2_ENGINE)
        // A logical table shares another physical table's route, so its region
        // ids do not identify the routes below.
        || table_info.table_id() != physical_table_id
    {
        return None;
    }
    let timestamp_column = table_info.meta.schema.timestamp_column()?;
    // The native raw consumer currently supports millisecond timestamps only.
    if timestamp_column.data_type.as_timestamp()?.unit() != TimeUnit::Millisecond {
        return None;
    }

    let mut route_by_region_number = HashMap::with_capacity(region_routes.len());
    for route in region_routes {
        // Two routes for one region number cannot be mapped one-to-one.
        if route_by_region_number
            .insert(route.region.id.region_number(), route)
            .is_some()
        {
            return None;
        }
    }

    let mut seen = HashSet::with_capacity(selected_regions.len());
    let mut targets = Vec::with_capacity(selected_regions.len());
    for region_id in selected_regions {
        // A duplicated region would be dispatched twice.
        if !seen.insert(*region_id) {
            return None;
        }
        let route = route_by_region_number.get(&region_id.region_number())?;
        // The lookup is by region number, but only the full region id identifies
        // this table's route: another table's route can carry the same region
        // number, and freezing its target would dispatch a foreign region.
        if route.region.id != *region_id {
            return None;
        }
        // Reads go to the region leader, so a staging or downgrading leader
        // must not be frozen as a query target.
        if route.is_leader_staging() || route.is_leader_downgrading() {
            return None;
        }
        let leader_peer = route.leader_peer.clone()?;
        targets.push((*region_id, RegionQueryTarget::new(leader_peer)));
    }

    Some(CaptureNativeRawRoute { targets })
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
    use common_catalog::consts::{
        DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE, MITO2_ENGINE,
    };
    use common_meta::cache::new_table_route_cache;
    use common_meta::key::TableMetadataManager;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{LeaderState, Region, RegionRoute};
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use common_time::timestamp::Timestamp;
    use datafusion::datasource::DefaultTableSource;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, TableSource, col as df_col, lit};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::value::Value;
    use moka::future::CacheBuilder;
    use partition::cache::new_partition_info_cache;
    use partition::expr::{PartitionExpr, col as partition_col};
    use partition::manager::PartitionRuleManager;
    use session::ReadPreference;
    use store_api::storage::RegionId;
    use table::metadata::{TableInfo, TableInfoBuilder, TableMeta, TableType};
    use table::table::adapter::DfTableProviderAdapter;
    use table::table_name::TableName;
    use table::test_util::EmptyTable;

    use super::{CaptureNativeRawRoute, DistExtensionPlanner, capture_native_raw_route};
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
                .map(|name| ColumnSchema::new(*name, ConcreteDataType::string_datatype(), true))
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
            &["host", "rack"],
            physical_partition_keys,
        );
        let logical_table = EmptyTable::from_table_info(&logical_info);
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
        let (regions, native_raw_route) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(vec![RegionId::new(LOGICAL_TABLE_ID, 1)], regions);
        // The captured table is not the physical route owner, so its region ids
        // cannot identify these route targets.
        assert!(native_raw_route.is_none());
    }

    #[tokio::test]
    async fn logical_table_pruning_uses_physical_partition_datatypes() {
        let (planner, plan, table_name) =
            planner_and_plan(vec![0, 1], physical_partition_expressions()).await;

        let (regions, native_raw_route) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(vec![RegionId::new(LOGICAL_TABLE_ID, 1)], regions);
        assert!(native_raw_route.is_none());
    }

    #[tokio::test]
    async fn missing_physical_partition_datatype_falls_back_to_all_logical_regions() {
        let (planner, plan, table_name) =
            planner_and_plan(vec![0], physical_partition_expressions()).await;

        let (regions, native_raw_route) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_all_logical_regions(regions);
        assert!(native_raw_route.is_none());
    }

    #[tokio::test]
    async fn missing_route_partition_expression_falls_back_to_all_logical_regions() {
        let mut expressions = physical_partition_expressions();
        expressions[1] = None;
        let (planner, plan, table_name) = planner_and_plan(vec![0, 1], expressions).await;

        let (regions, native_raw_route) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_all_logical_regions(regions);
        assert!(native_raw_route.is_none());
    }

    // ------------------------- native raw route capture ----------------------

    /// Table id of the base table used by the native raw route tests.
    const NATIVE_TABLE_ID: u32 = 4096;
    /// Timestamp split used by the native route tests.
    const NATIVE_SPLIT_MS: i64 = 1_700_000_000_000;

    /// Table info of a base table with an explicit timestamp column.
    fn native_table_info_with(
        table_id: u32,
        name: &str,
        engine: &str,
        timestamp_type: ConcreteDataType,
        timestamp_nullable: bool,
        partition_key_indices: Vec<usize>,
    ) -> TableInfo {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("ts", timestamp_type, timestamp_nullable).with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
        ]));
        let meta = TableMeta {
            schema,
            primary_key_indices: vec![],
            value_indices: vec![],
            engine: engine.to_string(),
            next_column_id: 2,
            options: Default::default(),
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices,
            column_ids: vec![0, 1],
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

    /// Table info of a base mito table partitioned on a millisecond timestamp.
    fn native_table_info(table_id: u32, name: &str, engine: &str, ts_nullable: bool) -> TableInfo {
        native_table_info_with(
            table_id,
            name,
            engine,
            ConcreteDataType::timestamp_millisecond_datatype(),
            ts_nullable,
            vec![0],
        )
    }

    fn native_region_route(
        region_number: u32,
        expression: Option<PartitionExpr>,
        leader_peer_id: Option<u64>,
    ) -> RegionRoute {
        native_region_route_for_table(NATIVE_TABLE_ID, region_number, expression, leader_peer_id)
    }

    /// A route whose region id belongs to `table_id`, which may differ from the
    /// table the capture is asked about.
    fn native_region_route_for_table(
        table_id: u32,
        region_number: u32,
        expression: Option<PartitionExpr>,
        leader_peer_id: Option<u64>,
    ) -> RegionRoute {
        RegionRoute {
            region: Region {
                id: RegionId::new(table_id, region_number),
                partition_expr: expression
                    .map(|expression| expression.as_json_str().unwrap())
                    .unwrap_or_default(),
                ..Default::default()
            },
            leader_peer: leader_peer_id.map(|id| Peer::new(id, format!("127.0.0.1:{id}"))),
            ..Default::default()
        }
    }

    fn native_target(peer_id: u64) -> RegionQueryTarget {
        RegionQueryTarget::new(Peer::new(peer_id, format!("127.0.0.1:{peer_id}")))
    }

    /// Routes of the canonical explicit half-domain split.
    fn native_half_domain_routes() -> Vec<RegionRoute> {
        vec![
            native_region_route(
                1,
                Some(
                    partition_col("ts").lt(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(11),
            ),
            native_region_route(
                2,
                Some(
                    partition_col("ts").gt_eq(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(22),
            ),
        ]
    }

    fn native_region_ids() -> Vec<RegionId> {
        vec![
            RegionId::new(NATIVE_TABLE_ID, 1),
            RegionId::new(NATIVE_TABLE_ID, 2),
        ]
    }

    /// Builds a planner whose scan captures `info` over `routes`, the physical
    /// region routes of that same table.
    ///
    /// `ts_filter` adds a filter on the timestamp column, which routes the
    /// selection through pruning. `by_name_scan` scans a DataFusion provider
    /// instead of a [`DfTableProviderAdapter`], which forces the ordinary name
    /// lookup and leaves the scan without a captured table.
    async fn native_planner_and_plan(
        info: TableInfo,
        routes: Vec<RegionRoute>,
        ts_filter: Option<i64>,
        by_name_scan: bool,
    ) -> (DistExtensionPlanner, LogicalPlan, TableName) {
        let table = EmptyTable::from_table_info(&info);
        let catalog_manager = MemoryCatalogManager::with_default_setup();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: info.catalog_name.clone(),
                schema: info.schema_name.clone(),
                table_name: info.name.clone(),
                table_id: info.table_id(),
                table: table.clone(),
            })
            .unwrap();

        let backend = Arc::new(MemoryKvBackend::default());
        let metadata_manager = TableMetadataManager::new(backend.clone());
        metadata_manager
            .create_table_metadata(
                info.clone(),
                TableRouteValue::physical(routes),
                HashMap::new(),
            )
            .await
            .unwrap();

        let table_route_cache = Arc::new(new_table_route_cache(
            "native-raw-route-routes".to_string(),
            CacheBuilder::new(16).build(),
            backend.clone(),
        ));
        let partition_info_cache = Arc::new(new_partition_info_cache(
            "native-raw-route-partitions".to_string(),
            CacheBuilder::new(16).build(),
            table_route_cache.clone(),
        ));
        let partition_rule_manager = Arc::new(PartitionRuleManager::new(
            backend,
            table_route_cache,
            partition_info_cache,
        ));
        let catalog_manager: CatalogManagerRef = catalog_manager;
        let planner = DistExtensionPlanner::new(
            catalog_manager,
            partition_rule_manager,
            Arc::new(UnusedRegionQueryHandler),
            false,
            // The native raw route fixtures exercise the enabled capture.
            true,
        );
        let table_source: Arc<dyn TableSource> = if by_name_scan {
            Arc::new(DefaultTableSource::new(Arc::new(
                datafusion::datasource::empty::EmptyTable::new(
                    info.meta.schema.arrow_schema().clone(),
                ),
            )))
        } else {
            Arc::new(DefaultTableSource::new(Arc::new(
                DfTableProviderAdapter::new(table),
            )))
        };
        let mut builder =
            LogicalPlanBuilder::scan_with_filters(info.name.clone(), table_source, None, vec![])
                .unwrap();
        if let Some(split_ms) = ts_filter {
            builder = builder
                .filter(
                    df_col("ts").gt(lit(ScalarValue::TimestampMillisecond(Some(split_ms), None))),
                )
                .unwrap();
        }
        let plan = builder.build().unwrap();
        let table_name =
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, info.name.clone());
        (planner, plan, table_name)
    }

    /// Captures `selected` regions of `routes` without going through a logical plan.
    fn capture_native_selected(
        info: &TableInfo,
        routes: &[RegionRoute],
        selected: &[RegionId],
    ) -> Option<CaptureNativeRawRoute> {
        let table = EmptyTable::from_table_info(info);
        capture_native_raw_route(Some(&table), info, info.table_id(), routes, selected)
    }

    #[tokio::test]
    async fn native_raw_route_captures_explicit_half_domains() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let (mut planner, plan, table_name) =
            native_planner_and_plan(info, native_half_domain_routes(), None, false).await;
        // The capture must come from the table captured by the scan: an empty
        // catalog cannot rebind the authorized scan to a replacement table.
        planner.catalog_manager = MemoryCatalogManager::with_default_setup();

        let (regions, native_raw_route) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(native_region_ids(), regions);
        let native_raw_route = native_raw_route.expect("the selected routes are captured");
        assert_eq!(
            vec![
                (RegionId::new(NATIVE_TABLE_ID, 1), native_target(11)),
                (RegionId::new(NATIVE_TABLE_ID, 2), native_target(22)),
            ],
            native_raw_route.0.targets
        );
    }

    #[tokio::test]
    async fn native_raw_route_captures_on_pruning_path() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let (planner, plan, table_name) = native_planner_and_plan(
            info,
            native_half_domain_routes(),
            Some(NATIVE_SPLIT_MS),
            false,
        )
        .await;

        let (regions, native_raw_route) = planner.get_regions(&table_name, &plan).await.unwrap();
        // Timestamp predicates are not pruned yet, so the ordinary plan keeps
        // every region; the capture still describes exactly those regions.
        assert_eq!(native_region_ids(), regions);
        let native_raw_route = native_raw_route.expect("the pruning fallback captures the route");
        assert_eq!(2, native_raw_route.0.targets.len());
        assert_eq!(
            (RegionId::new(NATIVE_TABLE_ID, 1), native_target(11)),
            native_raw_route.0.targets[0]
        );
        assert_eq!(
            (RegionId::new(NATIVE_TABLE_ID, 2), native_target(22)),
            native_raw_route.0.targets[1]
        );
    }

    #[tokio::test]
    async fn native_raw_route_captures_pruned_subset_without_changing_it() {
        // A table partitioned on `host`: the pruner returns a proper subset;
        // capture must follow precisely the same selected routes.
        let info = native_table_info_with(
            NATIVE_TABLE_ID,
            "native",
            MITO_ENGINE,
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
            vec![1],
        );
        let routes = vec![
            native_region_route(
                1,
                Some(partition_col("host").lt(Value::String("m".into()))),
                Some(11),
            ),
            native_region_route(
                2,
                Some(partition_col("host").gt_eq(Value::String("m".into()))),
                Some(22),
            ),
        ];
        let (planner, plan, table_name) = native_planner_and_plan(info, routes, None, false).await;
        let plan = LogicalPlanBuilder::from(plan)
            .filter(df_col("host").eq(lit("a")))
            .unwrap()
            .build()
            .unwrap();

        let (regions, native_raw_route) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(vec![RegionId::new(NATIVE_TABLE_ID, 1)], regions);
        assert_eq!(
            vec![(RegionId::new(NATIVE_TABLE_ID, 1), native_target(11))],
            native_raw_route.expect("pruned subset capture").0.targets
        );
    }

    #[test]
    fn native_raw_route_preserves_selected_subset_and_order() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let routes = vec![
            native_region_route(
                1,
                Some(
                    partition_col("ts").lt(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(11),
            ),
            native_region_route(
                2,
                Some(
                    partition_col("ts").eq(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(22),
            ),
            native_region_route(
                3,
                Some(
                    partition_col("ts").gt(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(33),
            ),
        ];

        // A pruned selection in pruning order, which is not route order.
        let selected = vec![
            RegionId::new(NATIVE_TABLE_ID, 3),
            RegionId::new(NATIVE_TABLE_ID, 1),
        ];
        let captured = capture_native_selected(&info, &routes, &selected)
            .expect("selected routes are capturable");
        assert_eq!(
            vec![
                (RegionId::new(NATIVE_TABLE_ID, 3), native_target(33)),
                (RegionId::new(NATIVE_TABLE_ID, 1), native_target(11)),
            ],
            captured.targets
        );

        // The complete selection keeps route order.
        let all = vec![
            RegionId::new(NATIVE_TABLE_ID, 1),
            RegionId::new(NATIVE_TABLE_ID, 2),
            RegionId::new(NATIVE_TABLE_ID, 3),
        ];
        let captured = capture_native_selected(&info, &routes, &all)
            .expect("the complete selection is capturable");
        assert_eq!(
            vec![
                (RegionId::new(NATIVE_TABLE_ID, 1), native_target(11)),
                (RegionId::new(NATIVE_TABLE_ID, 2), native_target(22)),
                (RegionId::new(NATIVE_TABLE_ID, 3), native_target(33)),
            ],
            captured.targets
        );
    }

    #[test]
    fn native_raw_route_captures_middle_domain_conjunction() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let middle = partition_col("ts")
            .gt(Value::Timestamp(Timestamp::new_millisecond(
                NATIVE_SPLIT_MS,
            )))
            .and(
                partition_col("ts").lt(Value::Timestamp(Timestamp::new_millisecond(
                    NATIVE_SPLIT_MS + 2_000,
                ))),
            );
        let routes = vec![
            native_region_route(
                1,
                Some(
                    partition_col("ts").lt(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(11),
            ),
            native_region_route(2, Some(middle), Some(22)),
            native_region_route(
                3,
                Some(
                    partition_col("ts").gt_eq(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS + 2_000,
                    ))),
                ),
                Some(33),
            ),
        ];
        let captured = capture_native_selected(
            &info,
            &routes,
            &[
                RegionId::new(NATIVE_TABLE_ID, 1),
                RegionId::new(NATIVE_TABLE_ID, 2),
                RegionId::new(NATIVE_TABLE_ID, 3),
            ],
        )
        .expect("raw merge accepts a bounded timestamp conjunction");
        assert_eq!(
            vec![
                (RegionId::new(NATIVE_TABLE_ID, 1), native_target(11)),
                (RegionId::new(NATIVE_TABLE_ID, 2), native_target(22)),
                (RegionId::new(NATIVE_TABLE_ID, 3), native_target(33)),
            ],
            captured.targets
        );
    }

    #[test]
    fn native_raw_route_captures_unpartitioned_table() {
        let info = native_table_info_with(
            NATIVE_TABLE_ID,
            "native",
            MITO_ENGINE,
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
            vec![],
        );
        let routes = vec![native_region_route(1, None, Some(11))];
        assert_eq!(
            Some(CaptureNativeRawRoute {
                targets: vec![(RegionId::new(NATIVE_TABLE_ID, 1), native_target(11))],
            }),
            capture_native_selected(&info, &routes, &[RegionId::new(NATIVE_TABLE_ID, 1)])
        );
    }

    #[test]
    fn native_raw_route_captures_empty_selection() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        assert_eq!(
            Some(CaptureNativeRawRoute { targets: vec![] }),
            capture_native_selected(&info, &[], &[])
        );
    }

    #[test]
    fn native_raw_route_declines_foreign_table_route_with_same_region_number() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let foreign_table_id = NATIVE_TABLE_ID + 1;
        // The routes belong to another table but carry the same region numbers,
        // so only the full region id can reject them.
        let routes = vec![
            native_region_route_for_table(
                foreign_table_id,
                1,
                Some(
                    partition_col("ts").lt(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(11),
            ),
            native_region_route_for_table(
                foreign_table_id,
                2,
                Some(
                    partition_col("ts").gt_eq(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(22),
            ),
        ];

        assert!(capture_native_selected(&info, &routes, &native_region_ids()).is_none());
    }

    #[tokio::test]
    async fn native_raw_route_declines_foreign_table_route_without_changing_ordinary_ids() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let foreign_table_id = NATIVE_TABLE_ID + 1;
        let routes = vec![
            native_region_route_for_table(
                foreign_table_id,
                1,
                Some(
                    partition_col("ts").lt(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(11),
            ),
            native_region_route_for_table(
                foreign_table_id,
                2,
                Some(
                    partition_col("ts").gt_eq(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(22),
            ),
        ];
        let (planner, plan, table_name) = native_planner_and_plan(info, routes, None, false).await;

        let (regions, native_raw_route) = planner.get_regions(&table_name, &plan).await.unwrap();
        // The ordinary plan still selects its own region ids from the route, and
        // only the capture declines.
        assert_eq!(native_region_ids(), regions);
        assert!(native_raw_route.is_none());
    }

    #[test]
    fn native_raw_route_requires_millisecond_timestamp_column() {
        let second_column_info = native_table_info_with(
            NATIVE_TABLE_ID,
            "native",
            MITO_ENGINE,
            ConcreteDataType::timestamp_second_datatype(),
            false,
            vec![0],
        );
        let routes = vec![native_region_route(1, None, Some(11))];
        assert!(
            capture_native_selected(
                &second_column_info,
                &routes,
                &[RegionId::new(NATIVE_TABLE_ID, 1)]
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn native_raw_route_captures_default_region() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let routes = vec![
            native_region_route(
                1,
                Some(
                    partition_col("ts").lt(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(11),
            ),
            // The default region has no explicit expression at all.
            native_region_route(2, None, Some(22)),
        ];
        let (planner, plan, table_name) = native_planner_and_plan(info, routes, None, false).await;

        let (regions, native_raw_route) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(native_region_ids(), regions);
        assert_eq!(
            vec![
                (RegionId::new(NATIVE_TABLE_ID, 1), native_target(11)),
                (RegionId::new(NATIVE_TABLE_ID, 2), native_target(22)),
            ],
            native_raw_route
                .expect("default selected region is capturable")
                .0
                .targets
        );
    }

    #[test]
    fn native_raw_route_declines_missing_leader() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let routes = vec![
            native_region_route(
                1,
                Some(
                    partition_col("ts").lt(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(11),
            ),
            native_region_route(
                2,
                Some(
                    partition_col("ts").gt_eq(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                None,
            ),
        ];

        assert!(capture_native_selected(&info, &routes, &native_region_ids()).is_none());
    }

    #[test]
    fn native_raw_route_declines_staging_or_downgrading_leader() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let mut routes = native_half_domain_routes();
        assert!(capture_native_selected(&info, &routes, &native_region_ids()).is_some());

        routes[1].leader_state = Some(LeaderState::Staging);
        assert!(capture_native_selected(&info, &routes, &native_region_ids()).is_none());

        routes[1].leader_state = Some(LeaderState::Downgrading);
        assert!(capture_native_selected(&info, &routes, &native_region_ids()).is_none());
    }

    #[test]
    fn native_raw_route_declines_duplicate_regions() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let routes = native_half_domain_routes();

        let duplicated_selection = [
            RegionId::new(NATIVE_TABLE_ID, 1),
            RegionId::new(NATIVE_TABLE_ID, 1),
        ];
        assert!(capture_native_selected(&info, &routes, &duplicated_selection).is_none());

        // Two routes for one region number cannot be mapped one-to-one.
        let duplicated_routes = vec![
            native_region_route(
                1,
                Some(
                    partition_col("ts").lt(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(11),
            ),
            native_region_route(
                1,
                Some(
                    partition_col("ts").gt_eq(Value::Timestamp(Timestamp::new_millisecond(
                        NATIVE_SPLIT_MS,
                    ))),
                ),
                Some(22),
            ),
        ];
        assert!(
            capture_native_selected(
                &info,
                &duplicated_routes,
                &[RegionId::new(NATIVE_TABLE_ID, 1)]
            )
            .is_none()
        );
    }

    #[test]
    fn native_raw_route_declines_unprovable_tables() {
        let routes = native_half_domain_routes();
        let selected = native_region_ids();

        // A logical table whose route resolves to another physical table.
        let logical_info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let table = EmptyTable::from_table_info(&logical_info);
        assert!(
            capture_native_raw_route(
                Some(&table),
                &logical_info,
                PHYSICAL_TABLE_ID,
                &routes,
                &selected,
            )
            .is_none()
        );

        // A non-mito engine.
        let metric_info = native_table_info(NATIVE_TABLE_ID, "native", "metric", false);
        assert!(capture_native_selected(&metric_info, &routes, &selected).is_none());
        let mito2_info = native_table_info(NATIVE_TABLE_ID, "native", MITO2_ENGINE, false);
        assert!(capture_native_selected(&mito2_info, &routes, &selected).is_some());

        // Timestamp nullability is irrelevant to identity/time duplicate-conflict merging.
        let nullable_info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, true);
        assert!(capture_native_selected(&nullable_info, &routes, &selected).is_some());

        // Without a captured table the name lookup may bind a replacement.
        assert!(
            capture_native_raw_route(None, &logical_info, NATIVE_TABLE_ID, &routes, &selected,)
                .is_none()
        );
    }

    #[tokio::test]
    async fn native_raw_route_declines_name_lookup_without_captured_table() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let (planner, plan, table_name) =
            native_planner_and_plan(info, native_half_domain_routes(), None, true).await;

        // The ordinary plan still resolves the table by name and keeps every
        // region; only the capture is skipped.
        let (regions, native_raw_route) = planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(native_region_ids(), regions);
        assert!(native_raw_route.is_none());
    }

    #[tokio::test]
    async fn native_raw_route_capture_follows_experimental_flag() {
        let info = native_table_info(NATIVE_TABLE_ID, "native", MITO_ENGINE, false);
        let (mut planner, plan, table_name) =
            native_planner_and_plan(info, native_half_domain_routes(), None, false).await;

        // Enabled: capture follows the ordinary selected region order.
        let (enabled_regions, enabled_route) =
            planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(native_region_ids(), enabled_regions);
        assert_eq!(
            vec![
                (RegionId::new(NATIVE_TABLE_ID, 1), native_target(11)),
                (RegionId::new(NATIVE_TABLE_ID, 2), native_target(22)),
            ],
            enabled_route
                .expect("the enabled fixture captures")
                .0
                .targets
        );

        // Disabled: the same regions in the same order, and no capture at all.
        planner.capture_native_raw_route = false;
        let (disabled_regions, disabled_route) =
            planner.get_regions(&table_name, &plan).await.unwrap();
        assert_eq!(enabled_regions, disabled_regions);
        assert!(disabled_route.is_none());
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
