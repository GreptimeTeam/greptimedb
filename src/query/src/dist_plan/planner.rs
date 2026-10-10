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
use catalog::kvbackend::KvBackendCatalogManager;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::datanode::RegionStat;
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
use partition::expr::PartitionExpr;
use partition::manager::{PartitionRuleManagerRef, create_partitions_from_region_routes};
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use store_api::region_engine::RegionRole;
use store_api::storage::RegionId;
use table::TableRef;
use table::metadata::TableInfo;
pub use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::dist_plan::PredicateExtractor;
use crate::dist_plan::dist_join_planner::{expected_region_ids, nested_broadcast_join_probe_input};
use crate::dist_plan::merge_scan::{MergeScanExec, MergeScanLogicalPlan};
use crate::dist_plan::merge_sort::{MergeSortExec, MergeSortLogicalPlan};
use crate::dist_plan::region_pruner::ConstraintPruner;
use crate::error::{CatalogSnafu, PartitionRuleManagerSnafu, TableNotFoundSnafu};
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
        // Region pruning clears collected predicates at a multi-input join, so routing the
        // nested payload by the join would read every large-side region. The local large-side
        // input preserves its predicate pruning; the full payload is still dispatched there.
        let routing_plan = nested_broadcast_join_probe_input(input_plan).unwrap_or(input_plan);
        let Some(table_name) = Self::extract_full_table_name(routing_plan)? else {
            // no relation found in input plan, going to execute them locally
            return fallback(optimized_plan).await;
        };

        let Ok(regions) = self.get_regions(&table_name, routing_plan).await else {
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
        let row_estimate = self.ordinary_scan_row_estimate(input_plan, &regions).await;
        let merge_scan_plan = MergeScanExec::new(
            session_state,
            table_name,
            regions,
            input_plan.clone(),
            schema,
            self.region_query_handler.clone(),
            query_ctx,
            session.config().target_partitions(),
            merge_scan.partition_cols().clone(),
            merge_scan.remote_dyn_filter_producer_id(),
            self.enable_per_region_metrics,
        )?;
        let merge_scan_plan = match row_estimate {
            Some(estimate) => merge_scan_plan.with_row_estimate(estimate),
            None => merge_scan_plan,
        };
        Ok(Some(Arc::new(merge_scan_plan) as _))
    }
}

/// Sums positive ordinary-leader row counts for exactly the selected regions.
fn aggregate_region_rows(selected_regions: &[RegionId], reports: &[RegionStat]) -> Option<usize> {
    let selected = selected_regions
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    if selected_regions.is_empty() || selected.len() != selected_regions.len() {
        return None;
    }

    let mut rows = 0u64;
    for region in selected_regions {
        let mut leaders = reports
            .iter()
            .filter(|report| report.id == *region && report.role == RegionRole::Leader);
        let report = leaders.next()?;
        if leaders.next().is_some() || report.num_rows == 0 {
            return None;
        }
        rows = rows.checked_add(report.num_rows)?;
    }
    usize::try_from(rows).ok()
}

impl DistExtensionPlanner {
    /// Returns a row estimate only for a simple scan of the captured physical base table.
    async fn ordinary_scan_row_estimate(
        &self,
        plan: &LogicalPlan,
        selected_regions: &[RegionId],
    ) -> Option<usize> {
        fn captured_base_table_id(plan: &LogicalPlan) -> Option<u32> {
            match plan {
                LogicalPlan::TableScan(scan) => {
                    let source = scan.source.downcast_ref::<DefaultTableSource>()?;
                    let provider = source
                        .table_provider
                        .downcast_ref::<DfTableProviderAdapter>()?;
                    (provider.table().table_type() == TableType::Base)
                        .then(|| provider.table().table_info().table_id())
                }
                LogicalPlan::Filter(filter) => captured_base_table_id(&filter.input),
                LogicalPlan::Projection(projection) => captured_base_table_id(&projection.input),
                LogicalPlan::SubqueryAlias(alias) => captured_base_table_id(&alias.input),
                _ => None,
            }
        }

        let table_id = captured_base_table_id(plan)?;
        if selected_regions.is_empty() {
            return None;
        }
        let selected = selected_regions
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        if selected.len() != selected_regions.len()
            || selected.iter().any(|region| region.table_id() != table_id)
        {
            return None;
        }
        let catalog_manager = self
            .catalog_manager
            .as_any()
            .downcast_ref::<KvBackendCatalogManager>()?;
        let (physical_table_id, route) = self
            .partition_rule_manager
            .find_physical_table_route_with_id(table_id)
            .await
            .ok()?;
        if physical_table_id != table_id {
            return None;
        }
        let routed_region_ids = expected_region_ids(physical_table_id, &route.region_routes)?;
        let routed_regions = routed_region_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        if routed_regions.is_empty()
            || routed_regions.len() != routed_region_ids.len()
            || !selected.is_subset(&routed_regions)
        {
            return None;
        }

        let reports = catalog_manager
            .information_extension()
            .region_stats()
            .await
            .ok()?;
        aggregate_region_rows(selected_regions, &reports)
    }

    /// Extract fully resolved table name from logical plan
    fn extract_full_table_name(plan: &LogicalPlan) -> Result<Option<TableName>> {
        let mut extractor = TableScanExtractor::default();
        let _ = plan.visit(&mut extractor)?;
        Ok(extractor.table_name)
    }

    async fn get_regions(
        &self,
        table_name: &TableName,
        logical_plan: &LogicalPlan,
    ) -> Result<Vec<RegionId>> {
        let mut extractor = TableScanExtractor::default();
        let _ = logical_plan.visit(&mut extractor)?;
        // Resolving by name again could bind an authorized scan to a replacement table.
        let table = match extractor.captured_table {
            Some(table) => table,
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
        if partition_columns.is_empty() {
            return Ok(all_regions);
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
                return Ok(all_regions);
            }
        };

        if partition_expressions.is_empty() {
            return Ok(all_regions);
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
            return Ok(all_regions);
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
                return Ok(all_regions);
            }
        };
        if partitions.is_empty() {
            return Ok(all_regions);
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
                return Ok(all_regions);
            }
        };

        common_telemetry::debug!(
            "Region pruning for table {}: {} partition expressions applied, pruned from {} to {} regions",
            table_name,
            partition_expressions.len(),
            all_regions.len(),
            pruned_regions.len()
        );

        Ok(pruned_regions)
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

/// Returns the resolved name of the first base table found in `plan`.
#[cfg(test)]
pub(crate) fn table_name_of(plan: &LogicalPlan) -> Option<TableName> {
    let mut extractor = TableScanExtractor::default();
    let _ = plan.visit(&mut extractor).ok()?;

    extractor.table_name
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
    use common_meta::datanode::RegionStat;
    use common_meta::key::TableMetadataManager;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use datafusion::datasource::DefaultTableSource;
    use datafusion_expr::{
        JoinType, LogicalPlan, LogicalPlanBuilder, Projection, col as df_col, lit,
    };
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::value::Value;
    use moka::future::CacheBuilder;
    use partition::cache::new_partition_info_cache;
    use partition::expr::{PartitionExpr, col as partition_col};
    use partition::manager::PartitionRuleManager;
    use session::ReadPreference;
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;
    use table::metadata::{TableInfo, TableInfoBuilder, TableMeta, TableType};
    use table::table::adapter::DfTableProviderAdapter;
    use table::table_name::TableName;
    use table::test_util::EmptyTable;

    use super::nested_broadcast_join_probe_input;
    use super::{DistExtensionPlanner, aggregate_region_rows};
    use crate::dist_plan::merge_scan::MergeScanLogicalPlan;
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
        assert_eq!(
            vec![RegionId::new(LOGICAL_TABLE_ID, 1)],
            planner.get_regions(&table_name, &plan).await.unwrap()
        );
    }

    #[tokio::test]
    async fn logical_table_pruning_uses_physical_partition_datatypes() {
        let (planner, plan, table_name) =
            planner_and_plan(vec![0, 1], physical_partition_expressions()).await;

        assert_eq!(
            vec![RegionId::new(LOGICAL_TABLE_ID, 1)],
            planner.get_regions(&table_name, &plan).await.unwrap()
        );
    }

    /// Region pruning clears the collected predicates at a node with several inputs, so a
    /// nested broadcast join must be routed by its local large-side input: its predicate
    /// prunes the large-table regions, while routing by the whole payload scans all regions.
    #[tokio::test]
    async fn nested_broadcast_join_routes_by_large_input() {
        let (planner, large_plan, table_name) =
            planner_and_plan(vec![0, 1], physical_partition_expressions()).await;
        // The small build side can use the same table under another qualifier; only the
        // large-side predicate must decide which regions are selected.
        let build_plan =
            MergeScanLogicalPlan::new(build_side_plan(&large_plan), false, Default::default())
                .into_logical_plan();
        let join_plan = LogicalPlanBuilder::from(build_plan)
            .join(
                large_plan.clone(),
                JoinType::Inner,
                (vec!["host"], vec!["host"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        assert!(
            planner
                .ordinary_scan_row_estimate(&join_plan, &[RegionId::new(LOGICAL_TABLE_ID, 1)])
                .await
                .is_none(),
            "a join payload must not inherit a base table's row count"
        );

        // The whole payload clears the large-side predicate at the join and reads all regions.
        assert_all_logical_regions(planner.get_regions(&table_name, &join_plan).await.unwrap());

        // The local large input keeps its predicate pruning: one of three regions.
        let routing_plan = nested_broadcast_join_probe_input(&join_plan)
            .expect("the supported nested shape must route by its large-side input");
        assert_eq!(large_plan.to_string(), routing_plan.to_string());
        assert_eq!(
            vec![RegionId::new(LOGICAL_TABLE_ID, 1)],
            planner
                .get_regions(&table_name, routing_plan)
                .await
                .unwrap()
        );

        let mut full_payload = join_plan.clone();
        for _ in 0..4 {
            let schema = full_payload.schema().clone();
            full_payload = LogicalPlan::Projection(Projection::new_from_schema(
                Arc::new(full_payload),
                schema,
            ));
        }
        assert!(
            planner
                .ordinary_scan_row_estimate(&full_payload, &[RegionId::new(LOGICAL_TABLE_ID, 1)],)
                .await
                .is_none(),
            "a join under projection wrappers is not an ordinary scan"
        );
        let large_input = nested_broadcast_join_probe_input(&full_payload)
            .expect("projection-wrapped join should retain its large-side routing input");
        assert_eq!(
            vec![RegionId::new(LOGICAL_TABLE_ID, 1)],
            planner.get_regions(&table_name, large_input).await.unwrap()
        );
    }

    /// Scans the table of `plan` under the qualifier `build` with the same probe predicate,
    /// so a join of both sides has distinct field qualifiers.
    fn build_side_plan(plan: &LogicalPlan) -> LogicalPlan {
        let LogicalPlan::Filter(filter) = plan else {
            panic!("expected the probe plan to be a filter, got: {plan}");
        };
        let LogicalPlan::TableScan(scan) = filter.input.as_ref() else {
            panic!("expected the probe plan to scan a table, got: {plan}");
        };
        LogicalPlanBuilder::scan_with_filters("build", scan.source.clone(), None, vec![])
            .unwrap()
            .filter(filter.predicate.clone())
            .unwrap()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn missing_physical_partition_datatype_falls_back_to_all_logical_regions() {
        let (planner, plan, table_name) =
            planner_and_plan(vec![0], physical_partition_expressions()).await;

        assert_all_logical_regions(planner.get_regions(&table_name, &plan).await.unwrap());
    }

    #[tokio::test]
    async fn missing_route_partition_expression_falls_back_to_all_logical_regions() {
        let mut expressions = physical_partition_expressions();
        expressions[1] = None;
        let (planner, plan, table_name) = planner_and_plan(vec![0, 1], expressions).await;

        assert_all_logical_regions(planner.get_regions(&table_name, &plan).await.unwrap());
    }

    fn region_stat(id: RegionId, role: RegionRole, num_rows: u64) -> RegionStat {
        RegionStat {
            id,
            rcus: 0,
            wcus: 0,
            approximate_bytes: 1,
            engine: "mito".to_string(),
            role,
            num_rows,
            memtable_size: 0,
            manifest_size: 0,
            sst_size: 1,
            sst_num: 1,
            index_size: 0,
            region_manifest: common_meta::datanode::RegionManifestInfo::Mito {
                manifest_version: 0,
                flushed_entry_id: 0,
                file_removed_cnt: 0,
            },
            written_bytes: 0,
            query_cpu_time: 0,
            query_scanned_bytes: 0,
            data_topic_latest_entry_id: 0,
            metadata_topic_latest_entry_id: 0,
            min_timestamp: None,
            max_timestamp: None,
        }
    }

    #[test]
    fn aggregate_region_rows_requires_one_positive_ordinary_leader_per_selected_region() {
        let first = RegionId::new(LOGICAL_TABLE_ID, 1);
        let second = RegionId::new(LOGICAL_TABLE_ID, 2);
        let unrelated = RegionId::new(LOGICAL_TABLE_ID, 3);

        assert_eq!(
            Some(30),
            aggregate_region_rows(
                &[first, second],
                &[
                    region_stat(first, RegionRole::Leader, 10),
                    region_stat(second, RegionRole::Leader, 20),
                    region_stat(unrelated, RegionRole::Leader, 999),
                ],
            )
        );
        assert_eq!(
            Some(10),
            aggregate_region_rows(
                &[first],
                &[
                    region_stat(first, RegionRole::Leader, 10),
                    region_stat(second, RegionRole::Leader, 20),
                ],
            ),
            "pruned regions must not contribute to the selected scan estimate"
        );
        assert_eq!(
            Some(10),
            aggregate_region_rows(
                &[first],
                &[
                    region_stat(first, RegionRole::Follower, 100),
                    region_stat(first, RegionRole::Leader, 10),
                ],
            )
        );

        let invalid_cases = [
            (
                vec![first, second],
                vec![region_stat(first, RegionRole::Leader, 10)],
            ),
            (vec![first], vec![region_stat(first, RegionRole::Leader, 0)]),
            (
                vec![first],
                vec![
                    region_stat(first, RegionRole::Leader, 10),
                    region_stat(first, RegionRole::Leader, 0),
                ],
            ),
            (
                vec![first],
                vec![region_stat(first, RegionRole::Follower, 10)],
            ),
            (
                vec![first],
                vec![region_stat(first, RegionRole::DowngradingLeader, 10)],
            ),
            (
                vec![first, second],
                vec![
                    region_stat(first, RegionRole::Leader, u64::MAX),
                    region_stat(second, RegionRole::Leader, 1),
                ],
            ),
        ];
        for (selected, reports) in invalid_cases {
            assert_eq!(None, aggregate_region_rows(&selected, &reports));
        }
        assert_eq!(None, aggregate_region_rows(&[], &[]));
        assert_eq!(
            None,
            aggregate_region_rows(
                &[first, first],
                &[region_stat(first, RegionRole::Leader, 10)]
            )
        );
        assert_eq!(
            None,
            aggregate_region_rows(
                &[first],
                &[region_stat(RegionId::new(9999, 1), RegionRole::Leader, 10)]
            )
        );
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
