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
use client::client_manager::DatanodeClients;
use common_base::bytes::Bytes;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::peer::Peer;
use common_meta::table_name::TableName;
use datafusion::common::Result;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::planner::ExtensionPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use datafusion_common::tree_node::{TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion_common::{DataFusionError, TableReference};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use partition::manager::PartitionRuleManager;
use snafu::ResultExt;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::adapter::DfTableProviderAdapter;

use crate::dist_plan::merge_scan::{MergeScanExec, MergeScanLogicalPlan};
use crate::error;

pub struct DistExtensionPlanner {
    partition_manager: Arc<PartitionRuleManager>,
    clients: Arc<DatanodeClients>,
}

impl DistExtensionPlanner {
    pub fn new(
        partition_manager: Arc<PartitionRuleManager>,
        clients: Arc<DatanodeClients>,
    ) -> Self {
        Self {
            partition_manager,
            clients,
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
        let maybe_merge_scan = { node.as_any().downcast_ref::<MergeScanLogicalPlan>() };
        if let Some(merge_scan) = maybe_merge_scan {
            if merge_scan.is_placeholder() {
                // ignore placeholder
                planner
                    .create_physical_plan(merge_scan.input(), session_state)
                    .await
                    .map(Some)
            } else {
                // TODO(ruihang): generate different execution plans for different variant merge operation
                let input_plan = merge_scan.input();
                println!("input: {:?}", input_plan);
                let Some(table_name) = self.get_table_name(input_plan)? else {
                    println!("table name not found, ignore this merge scan");
                    // no relation found in input plan, going to execute them locally 
                    return planner
                        .create_physical_plan(input_plan, session_state)
                        .await
                        .map(Some);
                };
                let input_schema = input_plan.schema().clone();
                let substrait_plan: Bytes = DFLogicalSubstraitConvertor
                    .encode(input_plan.clone())
                    .context(error::EncodeSubstraitLogicalPlanSnafu)?
                    .into();
                let peers = self.get_peers(&table_name).await;
                match peers {
                    Ok(peers) => {
                        let exec = MergeScanExec::new(
                            table_name,
                            peers,
                            substrait_plan,
                            Arc::new(input_schema.as_ref().into()),
                            self.clients.clone(),
                        );

                        Ok(Some(Arc::new(exec) as _))
                    }
                    Err(_) => planner
                        .create_physical_plan(input_plan, session_state)
                        .await
                        .map(Some),
                }
            }
        } else {
            Ok(None)
        }
    }
}

impl DistExtensionPlanner {
    /// Extract table name from logical plan
    fn get_table_name(&self, plan: &LogicalPlan) -> Result<Option<TableName>> {
        let mut extractor = TableNameExtractor::default();
        let _ = plan.visit(&mut extractor)?;
        Ok(extractor.table_name)
    }

    async fn get_peers(&self, table_name: &TableName) -> Result<Vec<Peer>> {
        self.partition_manager
            .find_table_region_leaders(table_name)
            .await
            .with_context(|_| error::RoutePartitionSnafu {
                table: table_name.clone(),
            })
            .map_err(|e| DataFusionError::External(Box::new(e)))
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
                        let info = provider.table().table_info();
                        self.table_name = Some(TableName::new(
                            info.catalog_name.clone(),
                            info.schema_name.clone(),
                            info.name.clone(),
                        ));
                        println!("retrieved table name: {:?}", self.table_name);
                        return Ok(VisitRecursion::Stop);
                    }
                }
                println!("downcast failed");
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
