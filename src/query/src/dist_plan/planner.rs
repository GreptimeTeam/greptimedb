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

use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::table_name::TableName;
use datafusion::common::Result;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion_common::TableReference;
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion_optimizer::analyzer::Analyzer;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
pub use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;

use crate::dist_plan::merge_scan::{MergeScanExec, MergeScanLogicalPlan};
use crate::error;
use crate::error::{CatalogSnafu, TableNotFoundSnafu};
use crate::region_query::RegionQueryHandlerRef;

pub struct DistExtensionPlanner {
    catalog_manager: CatalogManagerRef,
    region_query_handler: RegionQueryHandlerRef,
}

impl DistExtensionPlanner {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        region_query_handler: RegionQueryHandlerRef,
    ) -> Self {
        Self {
            catalog_manager,
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
            planner
                .create_physical_plan(logical_plan, session_state)
                .await
                .map(Some)
        };

        if merge_scan.is_placeholder() {
            // ignore placeholder
            return fallback(input_plan).await;
        }

        let optimized_plan = self.optimize_input_logical_plan(session_state, input_plan)?;
        let Some(table_name) = Self::extract_full_table_name(input_plan)? else {
            // no relation found in input plan, going to execute them locally
            return fallback(&optimized_plan).await;
        };

        let Ok(regions) = self.get_regions(&table_name).await else {
            // no peers found, going to execute them locally
            return fallback(&optimized_plan).await;
        };

        // TODO(ruihang): generate different execution plans for different variant merge operation
        let schema = optimized_plan.schema().as_ref().into();
        // Pass down the original plan, allow execution nodes to do their optimization
        let amended_plan = Self::plan_with_full_table_name(input_plan.clone(), &table_name)?;
        let substrait_plan = DFLogicalSubstraitConvertor
            .encode(&amended_plan)
            .context(error::EncodeSubstraitLogicalPlanSnafu)?
            .into();
        let merge_scan_plan = MergeScanExec::new(
            table_name,
            regions,
            substrait_plan,
            &schema,
            self.region_query_handler.clone(),
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

    /// Apply the fully resolved table name to the TableScan plan
    fn plan_with_full_table_name(plan: LogicalPlan, name: &TableName) -> Result<LogicalPlan> {
        plan.transform(&|plan| TableNameRewriter::rewrite_table_name(plan, name))
    }

    async fn get_regions(&self, table_name: &TableName) -> Result<Vec<RegionId>> {
        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table: table_name.to_string(),
            })?;
        Ok(table.table_info().region_ids())
    }

    // TODO(ruihang): find a more elegant way to optimize input logical plan
    fn optimize_input_logical_plan(
        &self,
        session_state: &SessionState,
        plan: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        let state = session_state.clone();
        let analyzer = Analyzer::default();
        let state = state.with_analyzer_rules(analyzer.rules);
        state.optimize(plan)
    }
}

/// Visitor to extract table name from logical plan (TableScan node)
#[derive(Default)]
struct TableNameExtractor {
    pub table_name: Option<TableName>,
}

impl TreeNodeVisitor for TableNameExtractor {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> Result<VisitRecursion> {
        match node {
            LogicalPlan::TableScan(scan) => {
                if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>() {
                    if let Some(provider) = source
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
                        return Ok(VisitRecursion::Stop);
                    }
                }
                match &scan.table_name {
                    TableReference::Full {
                        catalog,
                        schema,
                        table,
                    } => {
                        self.table_name = Some(TableName::new(
                            catalog.clone(),
                            schema.clone(),
                            table.clone(),
                        ));
                        Ok(VisitRecursion::Stop)
                    }
                    // TODO(ruihang): Maybe the following two cases should not be valid
                    TableReference::Partial { schema, table } => {
                        self.table_name = Some(TableName::new(
                            DEFAULT_CATALOG_NAME.to_string(),
                            schema.clone(),
                            table.clone(),
                        ));
                        Ok(VisitRecursion::Stop)
                    }
                    TableReference::Bare { table } => {
                        self.table_name = Some(TableName::new(
                            DEFAULT_CATALOG_NAME.to_string(),
                            DEFAULT_SCHEMA_NAME.to_string(),
                            table.clone(),
                        ));
                        Ok(VisitRecursion::Stop)
                    }
                }
            }
            _ => Ok(VisitRecursion::Continue),
        }
    }
}

struct TableNameRewriter;

impl TableNameRewriter {
    fn rewrite_table_name(
        plan: LogicalPlan,
        name: &TableName,
    ) -> datafusion_common::Result<Transformed<LogicalPlan>> {
        Ok(match plan {
            LogicalPlan::TableScan(mut table_scan) => {
                table_scan.table_name = TableReference::full(
                    name.catalog_name.clone(),
                    name.schema_name.clone(),
                    name.table_name.clone(),
                );
                Transformed::Yes(LogicalPlan::TableScan(table_scan))
            }
            _ => Transformed::No(plan),
        })
    }
}
