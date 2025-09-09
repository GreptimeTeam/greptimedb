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

//! Fallback dist plan analyzer, which will only push down table scan node
//! This is used when `PlanRewriter` produce errors when trying to rewrite the plan
//! This is a temporary solution, and will be removed once we have a more robust plan rewriter
//!

use std::collections::BTreeSet;

use common_telemetry::debug;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::Result as DfResult;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_expr::LogicalPlan;
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;

use crate::dist_plan::MergeScanLogicalPlan;
use crate::dist_plan::analyzer::HashableAliasMapping;

/// FallbackPlanRewriter is a plan rewriter that will only push down table scan node
/// This is used when `PlanRewriter` produce errors when trying to rewrite the plan
/// This is a temporary solution, and will be removed once we have a more robust plan rewriter
/// It will traverse the logical plan and rewrite table scan node to merge scan node
#[derive(Debug, Clone, Default)]
pub struct FallbackPlanRewriter;

impl TreeNodeRewriter for FallbackPlanRewriter {
    type Node = LogicalPlan;

    fn f_down(
        &mut self,
        plan: Self::Node,
    ) -> DfResult<datafusion_common::tree_node::Transformed<Self::Node>> {
        if let LogicalPlan::TableScan(table_scan) = &plan {
            let partition_cols = if let Some(source) = table_scan
                .source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
            {
                if let Some(provider) = source
                    .table_provider
                    .as_any()
                    .downcast_ref::<DfTableProviderAdapter>()
                {
                    if provider.table().table_type() == TableType::Base {
                        let info = provider.table().table_info();
                        let partition_key_indices = info.meta.partition_key_indices.clone();
                        let schema = info.meta.schema.clone();
                        let partition_cols = partition_key_indices
                            .into_iter()
                            .map(|index| schema.column_name_by_index(index).to_string())
                            .collect::<Vec<String>>();
                        debug!(
                            "FallbackPlanRewriter: table {} has partition columns: {:?}",
                            info.name, partition_cols
                        );
                        Some(partition_cols
                                .into_iter()
                                .map(|c| {
                                    let index =
                                        plan.schema().index_of_column_by_name(None, &c).ok_or_else(|| {
                                            datafusion_common::DataFusionError::Internal(
                                                format!(
                                                    "PlanRewriter: maybe_set_partitions: column {c} not found in schema of plan: {plan}"
                                                ),
                                            )
                                        })?;
                                    let column = plan.schema().columns().get(index).cloned().ok_or_else(|| {
                                        datafusion_common::DataFusionError::Internal(format!(
                                            "PlanRewriter: maybe_set_partitions: column index {index} out of bounds in schema of plan: {plan}"
                                        ))
                                    })?;
                                    Ok((c.clone(), BTreeSet::from([column])))
                                })
                                .collect::<DfResult<HashableAliasMapping>>()?)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };
            let node = MergeScanLogicalPlan::new(
                plan,
                false,
                // at this stage, the partition cols should be set
                // treat it as non-partitioned if None
                partition_cols.clone().unwrap_or_default(),
            )
            .into_logical_plan();
            Ok(Transformed::yes(node))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}
