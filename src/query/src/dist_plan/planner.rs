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
use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use datafusion::common::Result;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{DataFusionError, TableReference};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
pub use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::dist_plan::PredicateExtractor;
use crate::dist_plan::merge_scan::{MergeScanExec, MergeScanLogicalPlan};
use crate::dist_plan::merge_sort::MergeSortLogicalPlan;
use crate::dist_plan::region_pruner::ConstraintPruner;
use crate::error::{CatalogSnafu, PartitionRuleManagerSnafu, TableNotFoundSnafu};
use crate::region_query::RegionQueryHandlerRef;

/// Planner for convert merge sort logical plan to physical plan
///
/// it is currently a fallback to sort, and doesn't change the execution plan:
/// `MergeSort(MergeScan) -> Sort(MergeScan) - to physical plan -> ...`
/// It should be applied after `DistExtensionPlanner`
///
/// (Later when actually impl this merge sort)
///
/// We should ensure the number of partition is not smaller than the number of region at present. Otherwise this would result in incorrect output.
pub struct MergeSortExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for MergeSortExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(merge_sort) = node.as_any().downcast_ref::<MergeSortLogicalPlan>() {
            if let LogicalPlan::Extension(ext) = &merge_sort.input.as_ref()
                && ext
                    .node
                    .as_any()
                    .downcast_ref::<MergeScanLogicalPlan>()
                    .is_some()
            {
                let merge_scan_exec = physical_inputs
                    .first()
                    .and_then(|p| p.as_any().downcast_ref::<MergeScanExec>())
                    .ok_or(DataFusionError::Internal(format!(
                        "Expect MergeSort's input is a MergeScanExec, found {:?}",
                        physical_inputs
                    )))?;

                let partition_cnt = merge_scan_exec.partition_count();
                let region_cnt = merge_scan_exec.region_count();
                // if partition >= region, we know that every partition stream of merge scan is ordered
                // and we only need to do a merge sort, otherwise fallback to quick sort
                let can_merge_sort = partition_cnt >= region_cnt;
                if can_merge_sort {
                    // TODO(discord9): use `SortPreservingMergeExec here`
                }
                // for now merge sort only exist in logical plan, and have the same effect as `Sort`
                // doesn't change the execution plan, this will change in the future
                let ret = planner
                    .create_physical_plan(&merge_sort.clone().into_sort(), session_state)
                    .await?;
                Ok(Some(ret))
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
}

impl DistExtensionPlanner {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_rule_manager: PartitionRuleManagerRef,
        region_query_handler: RegionQueryHandlerRef,
    ) -> Self {
        Self {
            catalog_manager,
            partition_rule_manager,
            region_query_handler,
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
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(merge_scan) = node.as_any().downcast_ref::<MergeScanLogicalPlan>() else {
            return Ok(None);
        };

        let input_plan = merge_scan.input();
        let fallback = |logical_plan| async move {
            let optimized_plan = self.optimize_input_logical_plan(session_state, logical_plan)?;
            planner
                .create_physical_plan(&optimized_plan, session_state)
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

        let Ok(regions) = self.get_regions(&table_name, input_plan).await else {
            // no peers found, going to execute them locally
            return fallback(optimized_plan).await;
        };

        // TODO(ruihang): generate different execution plans for different variant merge operation
        let schema = optimized_plan.schema().as_arrow();
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
            session_state.config().target_partitions(),
            merge_scan.partition_cols().clone(),
        )?;
        Ok(Some(Arc::new(merge_scan_plan) as _))
    }
}

impl DistExtensionPlanner {
    /// Extract fully resolved table name from logical plan
    fn extract_full_table_name(plan: &LogicalPlan) -> Result<Option<TableName>> {
        let mut extractor = TableNameExtractor::default();
        let _ = plan.visit(&mut extractor)?;
        Ok(extractor.table_name)
    }

    async fn get_regions(
        &self,
        table_name: &TableName,
        logical_plan: &LogicalPlan,
    ) -> Result<Vec<RegionId>> {
        let table = self
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
            })?;

        let table_info = table.table_info();
        let physical_table_route = self
            .partition_rule_manager
            .find_physical_table_route(table_info.table_id())
            .await
            .context(PartitionRuleManagerSnafu)?;
        let all_regions = physical_table_route
            .region_routes
            .iter()
            .map(|r| RegionId::new(table_info.table_id(), r.region.id.region_number()))
            .collect::<Vec<_>>();
        // Extract partition columns
        let partition_columns: Vec<String> =
            table_info.meta.partition_column_names().cloned().collect();
        if partition_columns.is_empty() {
            return Ok(all_regions);
        }
        let partition_column_types = partition_columns
            .iter()
            .map(|col_name| {
                let data_type = table_info
                    .meta
                    .schema
                    .column_schema_by_name(col_name)
                    // Safety: names are retrieved above from the same table
                    .unwrap()
                    .data_type
                    .clone();
                (col_name.clone(), data_type)
            })
            .collect::<HashMap<_, _>>();

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

        // Get partition information for the table if partition rule manager is available
        let partitions = match self
            .partition_rule_manager
            .find_table_partitions(table.table_info().table_id())
            .await
        {
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

    /// Input logical plan is analyzed. Thus only call logical optimizer to optimize it.
    fn optimize_input_logical_plan(
        &self,
        session_state: &SessionState,
        plan: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        let state = session_state.clone();
        state.optimizer().optimize(plan.clone(), &state, |_, _| {})
    }
}

/// Visitor to extract table name from logical plan (TableScan node)
#[derive(Default)]
struct TableNameExtractor {
    pub table_name: Option<TableName>,
}

impl TreeNodeVisitor<'_> for TableNameExtractor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::TableScan(scan) => {
                if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>()
                    && let Some(provider) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DfTableProviderAdapter>()
                {
                    if provider.table().table_type() == TableType::Base {
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
