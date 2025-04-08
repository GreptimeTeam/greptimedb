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

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result as DfResult;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::Column;
use datafusion_expr::expr::{Exists, InSubquery};
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{col as col_fn, Expr, LogicalPlan, LogicalPlanBuilder, Subquery};
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
use crate::plan::ExtractExpr;
use crate::query_engine::DefaultSerializer;

#[derive(Debug)]
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
            .rewrite(plan, &optimizer_context)?
            .data;

        let plan = plan.transform(&Self::inspect_plan_with_subquery)?;
        let mut rewriter = PlanRewriter::default();
        let result = plan.data.rewrite(&mut rewriter)?.data;

        Ok(result)
    }
}

impl DistPlannerAnalyzer {
    fn inspect_plan_with_subquery(plan: LogicalPlan) -> DfResult<Transformed<LogicalPlan>> {
        // Workaround for https://github.com/GreptimeTeam/greptimedb/issues/5469 and https://github.com/GreptimeTeam/greptimedb/issues/5799
        // FIXME(yingwen): Remove the `Limit` plan once we update DataFusion.
        if let LogicalPlan::Limit(_) | LogicalPlan::Distinct(_) = &plan {
            return Ok(Transformed::no(plan));
        }

        let exprs = plan
            .expressions_consider_join()
            .into_iter()
            .map(|e| e.transform(&Self::transform_subquery).map(|x| x.data))
            .collect::<DfResult<Vec<_>>>()?;

        // Some plans that are special treated (should not call `with_new_exprs` on them)
        if !matches!(plan, LogicalPlan::Unnest(_)) {
            let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
            Ok(Transformed::yes(plan.with_new_exprs(exprs, inputs)?))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    fn transform_subquery(expr: Expr) -> DfResult<Transformed<Expr>> {
        match expr {
            Expr::Exists(exists) => Ok(Transformed::yes(Expr::Exists(Exists {
                subquery: Self::handle_subquery(exists.subquery)?,
                negated: exists.negated,
            }))),
            Expr::InSubquery(in_subquery) => Ok(Transformed::yes(Expr::InSubquery(InSubquery {
                expr: in_subquery.expr,
                subquery: Self::handle_subquery(in_subquery.subquery)?,
                negated: in_subquery.negated,
            }))),
            Expr::ScalarSubquery(scalar_subquery) => Ok(Transformed::yes(Expr::ScalarSubquery(
                Self::handle_subquery(scalar_subquery)?,
            ))),

            _ => Ok(Transformed::no(expr)),
        }
    }

    fn handle_subquery(subquery: Subquery) -> DfResult<Subquery> {
        let mut rewriter = PlanRewriter::default();
        let mut rewrote_subquery = subquery
            .subquery
            .as_ref()
            .clone()
            .rewrite(&mut rewriter)?
            .data;
        // Workaround. DF doesn't support the first plan in subquery to be an Extension
        if matches!(rewrote_subquery, LogicalPlan::Extension(_)) {
            let output_schema = rewrote_subquery.schema().clone();
            let project_exprs = output_schema
                .fields()
                .iter()
                .map(|f| col_fn(f.name()))
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
    column_requirements: HashSet<Column>,
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
        if DFLogicalSubstraitConvertor
            .encode(plan, DefaultSerializer)
            .is_err()
        {
            return true;
        }
        match Categorizer::check_plan(plan, self.partition_cols.clone()) {
            Commutativity::Commutative => {}
            Commutativity::PartialCommutative => {
                if let Some(plan) = partial_commutative_transformer(plan) {
                    self.update_column_requirements(&plan);
                    self.stage.push(plan)
                }
            }
            Commutativity::ConditionalCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan)
                {
                    self.update_column_requirements(&plan);
                    self.stage.push(plan)
                }
            }
            Commutativity::TransformedCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan)
                {
                    self.update_column_requirements(&plan);
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

    fn update_column_requirements(&mut self, plan: &LogicalPlan) {
        let mut container = HashSet::new();
        for expr in plan.expressions() {
            // this method won't fail
            let _ = expr_to_columns(&expr, &mut container);
        }

        for col in container {
            self.column_requirements.insert(col);
        }
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

    fn expand(&mut self, mut on_node: LogicalPlan) -> DfResult<LogicalPlan> {
        // store schema before expand
        let schema = on_node.schema().clone();
        let mut rewriter = EnforceDistRequirementRewriter {
            column_requirements: std::mem::take(&mut self.column_requirements),
        };
        on_node = on_node.rewrite(&mut rewriter)?.data;

        // add merge scan as the new root
        let mut node = MergeScanLogicalPlan::new(
            on_node,
            false,
            // at this stage, the partition cols should be set
            // treat it as non-partitioned if None
            self.partition_cols.clone().unwrap_or_default(),
        )
        .into_logical_plan();

        // expand stages
        for new_stage in self.stage.drain(..) {
            node = new_stage
                .with_new_exprs(new_stage.expressions_consider_join(), vec![node.clone()])?;
        }
        self.set_expanded();

        // recover the schema
        let node = LogicalPlanBuilder::from(node)
            .project(schema.iter().map(|(qualifier, field)| {
                Expr::Column(Column::new(qualifier.cloned(), field.name()))
            }))?
            .build()?;

        Ok(node)
    }
}

/// Implementation of the [`TreeNodeRewriter`] trait which is responsible for rewriting
/// logical plans to enforce various requirement for distributed query.
///
/// Requirements enforced by this rewriter:
/// - Enforce column requirements for `LogicalPlan::Projection` nodes. Makes sure the
///   required columns are available in the sub plan.
struct EnforceDistRequirementRewriter {
    column_requirements: HashSet<Column>,
}

impl TreeNodeRewriter for EnforceDistRequirementRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        if let LogicalPlan::Projection(ref projection) = node {
            let mut column_requirements = std::mem::take(&mut self.column_requirements);
            if column_requirements.is_empty() {
                return Ok(Transformed::no(node));
            }

            for expr in &projection.expr {
                let (qualifier, name) = expr.qualified_name();
                let column = Column::new(qualifier, name);
                column_requirements.remove(&column);
            }
            if column_requirements.is_empty() {
                return Ok(Transformed::no(node));
            }

            let mut new_exprs = projection.expr.clone();
            for col in &column_requirements {
                new_exprs.push(Expr::Column(col.clone()));
            }
            let new_node =
                node.with_new_exprs(new_exprs, node.inputs().into_iter().cloned().collect())?;
            return Ok(Transformed::yes(new_node));
        }

        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }
}

impl TreeNodeRewriter for PlanRewriter {
    type Node = LogicalPlan;

    /// descend
    fn f_down<'a>(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        self.level += 1;
        self.stack.push((node.clone(), self.level));
        // decendening will clear the stage
        self.stage.clear();
        self.set_unexpanded();
        self.partition_cols = None;
        Ok(Transformed::no(node))
    }

    /// ascend
    ///
    /// Besure to call `pop_stack` before returning
    fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        // only expand once on each ascending
        if self.is_expanded() {
            self.pop_stack();
            return Ok(Transformed::no(node));
        }

        // only expand when the leaf is table scan
        if node.inputs().is_empty() && !matches!(node, LogicalPlan::TableScan(_)) {
            self.set_expanded();
            self.pop_stack();
            return Ok(Transformed::no(node));
        }

        self.maybe_set_partitions(&node);

        let Some(parent) = self.get_parent() else {
            let node = self.expand(node)?;
            self.pop_stack();
            return Ok(Transformed::yes(node));
        };

        // TODO(ruihang): avoid this clone
        if self.should_expand(&parent.clone()) {
            // TODO(ruihang): does this work for nodes with multiple children?;
            let node = self.expand(node)?;
            self.pop_stack();
            return Ok(Transformed::yes(node));
        }

        self.pop_stack();
        Ok(Transformed::no(node))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::datasource::DefaultTableSource;
    use datafusion::functions_aggregate::expr_fn::avg;
    use datafusion_common::JoinType;
    use datafusion_expr::{col, lit, Expr, LogicalPlanBuilder};
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
        assert_eq!(expected, result.to_string());
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
        let expected = "Projection: avg(t.number)\
        \n  MergeScan [is_placeholder=false]";
        assert_eq!(expected, result.to_string());
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
        let expected = ["Projection: t.number", "  MergeScan [is_placeholder=false]"].join("\n");
        assert_eq!(expected, result.to_string());
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
        let expected = "Projection: t.number\
        \n  MergeScan [is_placeholder=false]";
        assert_eq!(expected, result.to_string());
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
            .join_on(
                right_plan,
                JoinType::LeftSemi,
                vec![col("t.number").eq(col("right.number"))],
            )
            .unwrap()
            .limit(0, Some(1))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
        let expected = [
            "Limit: skip=0, fetch=1",
            "  LeftSemi Join:  Filter: t.number = right.number",
            "    Projection: t.number",
            "      MergeScan [is_placeholder=false]",
            "    SubqueryAlias: right",
            "      Projection: t.number",
            "        MergeScan [is_placeholder=false]",
        ]
        .join("\n");
        assert_eq!(expected, result.to_string());
    }
}
