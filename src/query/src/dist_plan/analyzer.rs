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

use std::sync::Arc;

use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result as DfResult;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{RewriteRecursion, Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::expr::{Exists, InSubquery};
use datafusion_expr::{col, Expr, LogicalPlan, LogicalPlanBuilder, Subquery};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_optimizer::simplify_expressions::SimplifyExpressions;
use datafusion_optimizer::{OptimizerContext, OptimizerRule};
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
        // preprocess the input plan
        let optimizer_context = OptimizerContext::new();
        let plan = SimplifyExpressions::new()
            .try_optimize(&plan, &optimizer_context)?
            .unwrap_or(plan);

        let plan = plan.transform(&Self::inspect_plan_with_subquery)?;
        let mut rewriter = PlanRewriter::default();
        let result = plan.rewrite(&mut rewriter)?;

        Ok(result)
    }
}

impl DistPlannerAnalyzer {
    fn inspect_plan_with_subquery(plan: LogicalPlan) -> DfResult<Transformed<LogicalPlan>> {
        let exprs = plan
            .expressions()
            .into_iter()
            .map(|e| e.transform(&Self::transform_subquery))
            .collect::<DfResult<Vec<_>>>()?;

        let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
        Ok(Transformed::Yes(plan.with_new_exprs(exprs, &inputs)?))
    }

    fn transform_subquery(expr: Expr) -> DfResult<Transformed<Expr>> {
        match expr {
            Expr::Exists(exists) => Ok(Transformed::Yes(Expr::Exists(Exists {
                subquery: Self::handle_subquery(exists.subquery)?,
                negated: exists.negated,
            }))),
            Expr::InSubquery(in_subquery) => Ok(Transformed::Yes(Expr::InSubquery(InSubquery {
                expr: in_subquery.expr,
                subquery: Self::handle_subquery(in_subquery.subquery)?,
                negated: in_subquery.negated,
            }))),
            Expr::ScalarSubquery(scalar_subquery) => Ok(Transformed::Yes(Expr::ScalarSubquery(
                Self::handle_subquery(scalar_subquery)?,
            ))),

            _ => Ok(Transformed::No(expr)),
        }
    }

    fn handle_subquery(subquery: Subquery) -> DfResult<Subquery> {
        let mut rewriter = PlanRewriter::default();
        let mut rewrote_subquery = subquery.subquery.as_ref().clone().rewrite(&mut rewriter)?;
        // Workaround. DF doesn't support the first plan in subquery to be an Extension
        if matches!(rewrote_subquery, LogicalPlan::Extension(_)) {
            let output_schema = rewrote_subquery.schema().clone();
            let project_exprs = output_schema
                .fields()
                .iter()
                .map(|f| col(f.name()))
                .collect::<Vec<_>>();
            rewrote_subquery = LogicalPlanBuilder::from(rewrote_subquery)
                .project(project_exprs)?
                .build()?;
        }

        Ok(Subquery {
            subquery: Arc::new(rewrote_subquery),
            outer_ref_columns: subquery.outer_ref_columns,
        })
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
struct PlanRewriter {
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

impl PlanRewriter {
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
            return true;
        }

        match Categorizer::check_plan(plan, self.partition_cols.clone()) {
            Commutativity::Commutative => {}
            Commutativity::PartialCommutative => {
                if let Some(plan) = partial_commutative_transformer(plan) {
                    self.stage.push(plan)
                }
            }
            Commutativity::ConditionalCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan)
                {
                    self.stage.push(plan)
                }
            }
            Commutativity::TransformedCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan)
                {
                    self.stage.push(plan)
                }
            }
            Commutativity::NonCommutative
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

impl TreeNodeRewriter for PlanRewriter {
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

        // only expand when the leaf is table scan
        if node.inputs().is_empty() && !matches!(node, LogicalPlan::TableScan(_)) {
            self.set_expanded();
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
    use datafusion_common::JoinType;
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
        let expected = "MergeScan [is_placeholder=false]";
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
        let expected = "MergeScan [is_placeholder=false]";
        assert_eq!(expected, format!("{:?}", result));
    }

    #[test]
    fn transform_unalighed_join_with_alias() {
        let left = NumbersTable::table(0);
        let right = NumbersTable::table(1);
        let left_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(left),
        )));
        let right_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(right),
        )));

        let right_plan = LogicalPlanBuilder::scan_with_filters("t", right_source, None, vec![])
            .unwrap()
            .alias("right")
            .unwrap()
            .build()
            .unwrap();

        let plan = LogicalPlanBuilder::scan_with_filters("t", left_source, None, vec![])
            .unwrap()
            .join_using(right_plan, JoinType::LeftSemi, vec!["number"])
            .unwrap()
            .limit(0, Some(1))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
        let expected = "Limit: skip=0, fetch=1\
            \n  LeftSemi Join: Using t.number = right.number\
            \n    MergeScan [is_placeholder=false]\
            \n    SubqueryAlias: right\
            \n      MergeScan [is_placeholder=false]";
        assert_eq!(expected, format!("{:?}", result));
    }
}
