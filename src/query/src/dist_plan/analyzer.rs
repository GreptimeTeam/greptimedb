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

use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result as DfResult;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{RewriteRecursion, TreeNode, TreeNodeRewriter};
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::analyzer::AnalyzerRule;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;

use crate::dist_plan::commutativity::{
    partial_commutative_transformer, Categorizer, Commutativity,
};
use crate::dist_plan::merge_scan::MergeScanLogicalPlan;

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
        let mut rewriter = MagicRewriter::default();
        plan.rewrite(&mut rewriter)
    }
}

/// Status of the rewriter to mark if the current pass is expanded
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
enum RewriterStatus {
    #[default]
    Unexpanded,
    Expanded,
}

#[derive(Debug, Default)]
struct MagicRewriter {
    /// Current level in the tree
    level: usize,
    /// Simulated stack for the `rewrite` recursion
    stack: Vec<(LogicalPlan, usize)>,
    /// Stages to be expanded
    stage: Vec<LogicalPlan>,
    status: RewriterStatus,
    /// Partition columns of the table in current pass
    partition_cols: Option<Vec<String>>,
}

impl MagicRewriter {
    fn get_parent(&self) -> Option<&LogicalPlan> {
        // level starts from 1, it's safe to minus by 1
        self.stack
            .iter()
            .rev()
            .find(|(_, level)| *level == self.level - 1)
            .map(|(node, _)| node)
    }

    /// Return true if should stop and expand. The input plan is the parent node of current node
    fn should_expand(&mut self, plan: &LogicalPlan) -> bool {
        if DFLogicalSubstraitConvertor.encode(plan).is_err() {
            common_telemetry::info!(
                "substrait error: {:?}",
                DFLogicalSubstraitConvertor.encode(plan)
            );
            return true;
        }

        match Categorizer::check_plan(plan) {
            Commutativity::Commutative => {}
            Commutativity::PartialCommutative => {
                if let Some(plan) = partial_commutative_transformer(plan) {
                    self.stage.push(plan)
                }
            }
            Commutativity::ConditionalCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan) {
                    self.stage.push(plan)
                }
            },
            Commutativity::TransformedCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan) {
                    self.stage.push(plan)
                }
            },
            Commutativity::CheckPartition
            | Commutativity::NonCommutative
            | Commutativity::Unimplemented
            | Commutativity::Unsupported => {
                return true;
            }
        }

        false
    }

    fn is_expanded(&self) -> bool {
        self.status == RewriterStatus::Expanded
    }

    fn set_expanded(&mut self) {
        self.status = RewriterStatus::Expanded;
    }

    fn set_unexpanded(&mut self) {
        self.status = RewriterStatus::Unexpanded;
    }

    fn maybe_set_partitions(&mut self, plan: &LogicalPlan) {
        if self.partition_cols.is_some() {
            // only need to set once
            return;
        }

        if let LogicalPlan::TableScan(table_scan) = plan {
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
                        self.partition_cols = Some(partition_cols);
                    }
                }
            }
        }
    }

    /// pop one stack item and reduce the level by 1
    fn pop_stack(&mut self) {
        self.level -= 1;
        self.stack.pop();
    }
}

impl TreeNodeRewriter for MagicRewriter {
    type N = LogicalPlan;

    /// descend
    fn pre_visit<'a>(&'a mut self, node: &'a Self::N) -> DfResult<RewriteRecursion> {
        self.level += 1;
        self.stack.push((node.clone(), self.level));
        // decendening will clear the stage
        self.stage.clear();
        self.set_unexpanded();
        self.partition_cols = None;
        Ok(RewriteRecursion::Continue)
    }

    /// ascend
    ///
    /// Besure to call `pop_stack` before returning
    fn mutate(&mut self, node: Self::N) -> DfResult<Self::N> {
        // only expand once on each ascending
        if self.is_expanded() {
            self.pop_stack();
            return Ok(node);
        }

        self.maybe_set_partitions(&node);

        let Some(parent) = self.get_parent() else {
            // add merge scan as the new root
            let mut node = MergeScanLogicalPlan::new(node, false).into_logical_plan();
            // expand stages
            for new_stage in self.stage.drain(..) {
                node = new_stage.with_new_inputs(&[node])?
            }
            self.set_expanded();

            self.pop_stack();
            return Ok(node);
        };

        // TODO(ruihang): avoid this clone
        if self.should_expand(&parent.clone()) {
            // TODO(ruihang): does this work for nodes with multiple children?;
            // replace the current node with expanded one
            let mut node = MergeScanLogicalPlan::new(node, false).into_logical_plan();
            // expand stages
            for new_stage in self.stage.drain(..) {
                node = new_stage.with_new_inputs(&[node])?
            }
            self.set_expanded();

            self.pop_stack();
            return Ok(node);
        }

        self.pop_stack();
        Ok(node)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::datasource::DefaultTableSource;
    use datafusion_expr::{avg, col, lit, Expr, LogicalPlanBuilder};
    use table::table::adapter::DfTableProviderAdapter;
    use table::table::numbers::NumbersTable;

    use super::*;

    #[ignore = "Projection is disabled for https://github.com/apache/arrow-datafusion/issues/6489"]
    #[test]
    fn transform_simple_projection_filter() {
        let numbers_table = NumbersTable::table(0);
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
        let expected = [
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
        let numbers_table = NumbersTable::table(0);
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
        let expected = [
            "Aggregate: groupBy=[[]], aggr=[[AVG(t.number)]]",
            "  MergeScan [is_placeholder=false]",
        ]
        .join("\n");
        assert_eq!(expected, format!("{:?}", result));
    }

    #[test]
    fn transform_distinct_order() {
        let numbers_table = NumbersTable::table(0);
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
        let expected = [
            "Sort: t.number ASC NULLS LAST",
            "  Distinct:",
            "    MergeScan [is_placeholder=false]",
        ]
        .join("\n");
        assert_eq!(expected, format!("{:?}", result));
    }

    #[test]
    fn transform_single_limit() {
        let numbers_table = NumbersTable::table(0);
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
        let expected = [
            "Limit: skip=0, fetch=1",
            "  MergeScan [is_placeholder=false]",
        ]
        .join("\n");
        assert_eq!(expected, format!("{:?}", result));
    }
}
