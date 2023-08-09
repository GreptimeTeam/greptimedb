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

use std::sync::{Arc, Mutex};

use datafusion::datasource::DefaultTableSource;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion_expr::{Extension, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;

use crate::dist_plan::commutativity::{
    partial_commutative_transformer, Categorizer, Commutativity,
};
use crate::dist_plan::merge_scan::MergeScanLogicalPlan;
use crate::dist_plan::utils;

pub struct DistPlannerAnalyzer;

impl AnalyzerRule for DistPlannerAnalyzer {
    fn name(&self) -> &str {
        "DistPlannerAnalyzer"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<LogicalPlan> {
        // (1) transform up merge scan
        let mut visitor = CommutativeVisitor::new();
        let _ = plan.visit(&mut visitor)?;
        let state = ExpandState::new();
        let plan = plan.transform_down(&|plan| Self::expand(plan, &visitor, &state))?;

        // (2) remove placeholder merge scan
        let plan = plan.transform(&Self::remove_placeholder_merge_scan)?;

        Ok(plan)
    }
}

impl DistPlannerAnalyzer {
    /// Add [MergeScanLogicalPlan] before the table scan
    #[allow(dead_code)]
    fn add_merge_scan(plan: LogicalPlan) -> datafusion_common::Result<Transformed<LogicalPlan>> {
        Ok(match plan {
            LogicalPlan::TableScan(table_scan) => {
                let ext_plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(MergeScanLogicalPlan::new(
                        LogicalPlan::TableScan(table_scan),
                        true,
                    )),
                });
                Transformed::Yes(ext_plan)
            }
            _ => Transformed::No(plan),
        })
    }

    /// Remove placeholder [MergeScanLogicalPlan]
    fn remove_placeholder_merge_scan(
        plan: LogicalPlan,
    ) -> datafusion_common::Result<Transformed<LogicalPlan>> {
        Ok(match &plan {
            LogicalPlan::Extension(extension)
                if extension.node.name() == MergeScanLogicalPlan::name() =>
            {
                let merge_scan = extension
                    .node
                    .as_any()
                    .downcast_ref::<MergeScanLogicalPlan>()
                    .unwrap();
                if merge_scan.is_placeholder() {
                    Transformed::Yes(merge_scan.input().clone())
                } else {
                    Transformed::No(plan)
                }
            }
            _ => Transformed::No(plan),
        })
    }

    /// Expand stages on the stop node
    fn expand(
        mut plan: LogicalPlan,
        visitor: &CommutativeVisitor,
        state: &ExpandState,
    ) -> datafusion_common::Result<Transformed<LogicalPlan>> {
        if state.is_transformed() {
            // only transform once
            return Ok(Transformed::No(plan));
        }
        if let Some(stop_node) = visitor.stop_node && utils::hash_plan(&plan) != stop_node {
            // only act with the stop node or the root (the first node seen by this closure) if no stop node
            return Ok(Transformed::No(plan));
        }

        if visitor.stop_node.is_some() {
            // insert merge scan between the stop node and its child
            let children = plan.inputs();
            let mut new_children = Vec::with_capacity(children.len());
            for child in children {
                let mut new_child =
                    MergeScanLogicalPlan::new(child.clone(), false).into_logical_plan();
                // expand stages
                for new_stage in &visitor.next_stage {
                    new_child = new_stage.with_new_inputs(&[new_child])?
                }
                new_children.push(new_child);
            }
            plan = plan.with_new_inputs(&new_children)?;
        } else {
            // otherwise add merge scan as the new root
            plan = MergeScanLogicalPlan::new(plan, false).into_logical_plan();
            // expand stages
            for new_stage in &visitor.next_stage {
                plan = new_stage.with_new_inputs(&[plan])?
            }
        }

        state.set_transformed();
        Ok(Transformed::Yes(plan))
    }
}

struct ExpandState {
    transformed: Mutex<bool>,
}

impl ExpandState {
    pub fn new() -> Self {
        Self {
            transformed: Mutex::new(false),
        }
    }

    pub fn is_transformed(&self) -> bool {
        *self.transformed.lock().unwrap()
    }

    /// Set the state to transformed
    pub fn set_transformed(&self) {
        *self.transformed.lock().unwrap() = true;
    }
}

struct CommutativeVisitor {
    next_stage: Vec<LogicalPlan>,
    // hash of the stop node
    stop_node: Option<u64>,
    /// Partition columns of current visiting table
    current_partition_cols: Option<Vec<String>>,
}

impl TreeNodeVisitor for CommutativeVisitor {
    type N = LogicalPlan;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> datafusion_common::Result<VisitRecursion> {
        // find the first merge scan and stop traversing down
        // todo: check if it works for join
        Ok(match plan {
            LogicalPlan::TableScan(table_scan) => {
                // TODO(ruihang): spawn a sub visitor to retrieve partition columns
                if let Some(source) = table_scan
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
                            self.current_partition_cols = Some(partition_cols);
                        }
                    }
                }
                VisitRecursion::Continue
            }
            _ => VisitRecursion::Continue,
        })
    }

    fn post_visit(&mut self, plan: &LogicalPlan) -> datafusion_common::Result<VisitRecursion> {
        if DFLogicalSubstraitConvertor.encode(plan).is_err() {
            common_telemetry::info!(
                "substrait error: {:?}",
                DFLogicalSubstraitConvertor.encode(plan)
            );
            self.stop_node = Some(utils::hash_plan(plan));
            return Ok(VisitRecursion::Stop);
        }

        match Categorizer::check_plan(plan) {
            Commutativity::Commutative => {}
            Commutativity::PartialCommutative => {
                if let Some(plan) = partial_commutative_transformer(plan) {
                    self.next_stage.push(plan)
                }
            }
            Commutativity::ConditionalCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan) {
                    self.next_stage.push(plan)
                }
            },
            Commutativity::TransformedCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan) {
                    self.next_stage.push(plan)
                }
            },
            Commutativity::CheckPartition => {
                if let Some(partition_cols) = &self.current_partition_cols
                    && partition_cols.is_empty() {
                    // no partition columns, and can be encoded skip
                    return Ok(VisitRecursion::Continue);
                } else {
                    self.stop_node = Some(utils::hash_plan(plan));
                    return Ok(VisitRecursion::Stop);
                }
            },
            Commutativity::NonCommutative
            | Commutativity::Unimplemented
            | Commutativity::Unsupported => {
                self.stop_node = Some(utils::hash_plan(plan));
                return Ok(VisitRecursion::Stop);
            }
        }

        Ok(VisitRecursion::Continue)
    }
}

impl CommutativeVisitor {
    pub fn new() -> Self {
        Self {
            next_stage: vec![],
            stop_node: None,
            current_partition_cols: None,
        }
    }
}

#[cfg(test)]
mod test {
    use datafusion::datasource::DefaultTableSource;
    use datafusion_expr::{avg, col, lit, Expr, LogicalPlanBuilder};
    use table::table::adapter::DfTableProviderAdapter;
    use table::table::numbers::NumbersTable;

    use super::*;

    #[ignore = "Projection is disabled for https://github.com/apache/arrow-datafusion/issues/6489"]
    #[test]
    fn transform_simple_projection_filter() {
        let numbers_table = Arc::new(NumbersTable::new(0)) as _;
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(numbers_table),
        )));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .filter(col("number").lt(lit(10)))
            .unwrap()
            .project(vec![col("number")])
            .unwrap()
            .distinct()
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
        let expected = vec![
            "Distinct:",
            "  MergeScan [is_placeholder=false]",
            "    Distinct:",
            "      Projection: t.number",
            "        Filter: t.number < Int32(10)",
            "          TableScan: t",
        ]
        .join("\n");
        assert_eq!(expected, format!("{:?}", result));
    }

    #[test]
    fn transform_aggregator() {
        let numbers_table = Arc::new(NumbersTable::new(0)) as _;
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(numbers_table),
        )));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .aggregate(Vec::<Expr>::new(), vec![avg(col("number"))])
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
        let expected = vec![
            "Aggregate: groupBy=[[]], aggr=[[AVG(t.number)]]",
            "  MergeScan [is_placeholder=false]",
        ]
        .join("\n");
        assert_eq!(expected, format!("{:?}", result));
    }

    #[test]
    fn transform_distinct_order() {
        let numbers_table = Arc::new(NumbersTable::new(0)) as _;
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(numbers_table),
        )));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .distinct()
            .unwrap()
            .sort(vec![col("number").sort(true, false)])
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
        let expected = vec![
            "Sort: t.number ASC NULLS LAST",
            "  Distinct:",
            "    MergeScan [is_placeholder=false]",
        ]
        .join("\n");
        assert_eq!(expected, format!("{:?}", result));
    }

    #[test]
    fn transform_single_limit() {
        let numbers_table = Arc::new(NumbersTable::new(0)) as _;
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(numbers_table),
        )));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .limit(0, Some(1))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
        let expected = vec![
            "Limit: skip=0, fetch=1",
            "  MergeScan [is_placeholder=false]",
        ].join("\n");
        assert_eq!(expected, format!("{:?}", result));
    }
}
