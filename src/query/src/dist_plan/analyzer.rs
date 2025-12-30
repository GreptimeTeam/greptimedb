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

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use common_telemetry::debug;
use datafusion::config::{ConfigExtension, ExtensionOptions};
use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result as DfResult;
use datafusion_common::Column;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::expr::{Exists, InSubquery};
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Subquery, col as col_fn};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_optimizer::simplify_expressions::SimplifyExpressions;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use promql::extension_plan::SeriesDivide;
use store_api::storage::VectorSearchRequest;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;

use crate::dist_plan::VectorScanLogicalPlan;
use crate::dist_plan::analyzer::utils::{aliased_columns_for, rewrite_merge_sort_exprs};
use crate::dist_plan::commutativity::{
    Categorizer, Commutativity, partial_commutative_transformer,
};
use crate::dist_plan::merge_scan::MergeScanLogicalPlan;
use crate::dist_plan::merge_sort::MergeSortLogicalPlan;
use crate::metrics::PUSH_DOWN_FALLBACK_ERRORS_TOTAL;
use crate::plan::ExtractExpr;
use crate::query_engine::DefaultSerializer;

#[cfg(test)]
mod test;

mod fallback;
mod utils;

pub(crate) use utils::AliasMapping;

/// Placeholder for other physical partition columns that are not in logical table
const OTHER_PHY_PART_COL_PLACEHOLDER: &str = "__OTHER_PHYSICAL_PART_COLS_PLACEHOLDER__";

#[derive(Debug, Clone)]
pub struct DistPlannerOptions {
    pub allow_query_fallback: bool,
}

impl ConfigExtension for DistPlannerOptions {
    const PREFIX: &'static str = "dist_planner";
}

impl ExtensionOptions for DistPlannerOptions {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> DfResult<()> {
        Err(datafusion_common::DataFusionError::NotImplemented(format!(
            "DistPlannerOptions does not support set key: {key} with value: {value}"
        )))
    }

    fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
        vec![datafusion::config::ConfigEntry {
            key: "allow_query_fallback".to_string(),
            value: Some(self.allow_query_fallback.to_string()),
            description: "Allow query fallback to fallback plan rewriter",
        }]
    }
}

#[derive(Debug)]
pub struct DistPlannerAnalyzer;

impl AnalyzerRule for DistPlannerAnalyzer {
    fn name(&self) -> &str {
        "DistPlannerAnalyzer"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        config: &ConfigOptions,
    ) -> datafusion_common::Result<LogicalPlan> {
        let mut config = config.clone();
        // Aligned with the behavior in `datafusion_optimizer::OptimizerContext::new()`.
        config.optimizer.filter_null_join_keys = true;
        let config = Arc::new(config);

        // The `ConstEvaluator` in `SimplifyExpressions` might evaluate some UDFs early in the
        // planning stage, by executing them directly. For example, the `database()` function.
        // So the `ConfigOptions` here (which is set from the session context) should be present
        // in the UDF's `ScalarFunctionArgs`. However, the default implementation in DataFusion
        // seems to lost track on it: the `ConfigOptions` is recreated with its default values again.
        // So we create a custom `OptimizerConfig` with the desired `ConfigOptions`
        // to walk around the issue.
        // TODO(LFC): Maybe use DataFusion's `OptimizerContext` again
        //   once https://github.com/apache/datafusion/pull/17742 is merged.
        struct OptimizerContext {
            inner: datafusion_optimizer::OptimizerContext,
            config: Arc<ConfigOptions>,
        }

        impl OptimizerConfig for OptimizerContext {
            fn query_execution_start_time(&self) -> DateTime<Utc> {
                self.inner.query_execution_start_time()
            }

            fn alias_generator(&self) -> &Arc<AliasGenerator> {
                self.inner.alias_generator()
            }

            fn options(&self) -> Arc<ConfigOptions> {
                self.config.clone()
            }
        }

        let optimizer_context = OptimizerContext {
            inner: datafusion_optimizer::OptimizerContext::new(),
            config: config.clone(),
        };

        let plan = SimplifyExpressions::new()
            .rewrite(plan, &optimizer_context)?
            .data;

        let opt = config.extensions.get::<DistPlannerOptions>();
        let allow_fallback = opt.map(|o| o.allow_query_fallback).unwrap_or(false);

        let result = match self.try_push_down(plan.clone()) {
            Ok(plan) => plan,
            Err(err) => {
                if allow_fallback {
                    common_telemetry::warn!(err; "Failed to push down plan, using fallback plan rewriter for plan: {plan}");
                    // if push down failed, use fallback plan rewriter
                    PUSH_DOWN_FALLBACK_ERRORS_TOTAL.inc();
                    self.use_fallback(plan)?
                } else {
                    return Err(err);
                }
            }
        };

        Ok(result)
    }
}

impl DistPlannerAnalyzer {
    /// Try push down as many nodes as possible
    fn try_push_down(&self, plan: LogicalPlan) -> DfResult<LogicalPlan> {
        let plan = plan.transform(&Self::inspect_plan_with_subquery)?;
        let mut rewriter = PlanRewriter::default();
        let result = plan.data.rewrite(&mut rewriter)?.data;
        Ok(result)
    }

    /// Use fallback plan rewriter to rewrite the plan and only push down table scan nodes
    fn use_fallback(&self, plan: LogicalPlan) -> DfResult<LogicalPlan> {
        let mut rewriter = fallback::FallbackPlanRewriter;
        let result = plan.rewrite(&mut rewriter)?.data;
        Ok(result)
    }

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
            spans: Default::default(),
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
    /// Stages to be expanded, will be added as parent node of merge scan one by one
    stage: Vec<LogicalPlan>,
    status: RewriterStatus,
    /// Partition columns of the table in current pass
    partition_cols: Option<AliasMapping>,
    /// use stack count as scope to determine column requirements is needed or not
    /// i.e for a logical plan like:
    /// ```ignore
    /// 1: Projection: t.number
    /// 2: Sort: t.pk1+t.pk2
    /// 3. Projection: t.number, t.pk1, t.pk2
    /// ```
    /// `Sort` will make a column requirement for `t.pk1` at level 2.
    /// Which making `Projection` at level 1 need to add a ref to `t.pk1` as well.
    /// So that the expanded plan will be
    /// ```ignore
    /// Projection: t.number
    ///   MergeSort: t.pk1
    ///     MergeScan: remote_input=
    /// Projection: t.number, "t.pk1+t.pk2" <--- the original `Projection` at level 1 get added with `t.pk1+t.pk2`
    ///  Sort: t.pk1+t.pk2
    ///    Projection: t.number, t.pk1, t.pk2
    /// ```
    /// Making `MergeSort` can have `t.pk1` as input.
    /// Meanwhile `Projection` at level 3 doesn't need to add any new column because 3 > 2
    /// and col requirements at level 2 is not applicable for level 3.
    ///
    /// see more details in test `expand_proj_step_aggr` and `expand_proj_sort_proj`
    ///
    /// TODO(discord9): a simpler solution to track column requirements for merge scan
    column_requirements: Vec<(HashSet<Column>, usize)>,
    /// Whether to expand on next call
    /// This is used to handle the case where a plan is transformed, but need to be expanded from it's
    /// parent node. For example a Aggregate plan is split into two parts in frontend and datanode, and need
    /// to be expanded from the parent node of the Aggregate plan.
    expand_on_next_call: bool,
    /// Expanding on next partial/conditional/transformed commutative plan
    /// This is used to handle the case where a plan is transformed, but still
    /// need to push down as many node as possible before next partial/conditional/transformed commutative
    /// plan. I.e.
    /// ```ignore
    /// Limit:
    ///     Sort:
    /// ```
    /// where `Limit` is partial commutative, and `Sort` is conditional commutative.
    /// In this case, we need to expand the `Limit` plan,
    /// so that we can push down the `Sort` plan as much as possible.
    expand_on_next_part_cond_trans_commutative: bool,
    new_child_plan: Option<LogicalPlan>,
    /// Vector search request extracted from the table adapter.
    /// This is used to wrap the plan with `VectorScanLogicalPlan` for distributed execution.
    vector_search_request: Option<VectorSearchRequest>,
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
    fn should_expand(&mut self, plan: &LogicalPlan) -> DfResult<bool> {
        debug!(
            "Check should_expand at level: {}  with Stack:\n{}, ",
            self.level,
            self.stack
                .iter()
                .map(|(p, l)| format!("{l}:{}{}", "  ".repeat(l - 1), p.display()))
                .collect::<Vec<String>>()
                .join("\n"),
        );
        if let Err(e) = DFLogicalSubstraitConvertor.encode(plan, DefaultSerializer) {
            debug!(
                "PlanRewriter: plan cannot be converted to substrait with error={e:?}, expanding now: {plan}"
            );
            return Ok(true);
        }

        if self.expand_on_next_call {
            self.expand_on_next_call = false;
            debug!("PlanRewriter: expand_on_next_call is true, expanding now");
            return Ok(true);
        }

        if self.expand_on_next_part_cond_trans_commutative {
            let comm = Categorizer::check_plan(plan, self.partition_cols.clone())?;
            match comm {
                Commutativity::PartialCommutative => {
                    // a small difference is that for partial commutative, we still need to
                    // push down it(so `Limit` can be pushed down)

                    // notice how limit needed to be expanded as well to make sure query is correct
                    // i.e. `Limit fetch=10` need to be pushed down to the leaf node
                    self.expand_on_next_part_cond_trans_commutative = false;
                    self.expand_on_next_call = true;
                }
                Commutativity::ConditionalCommutative(_)
                | Commutativity::TransformedCommutative { .. } => {
                    // again a new node that can be push down, we should just
                    // do push down now and avoid further expansion
                    self.expand_on_next_part_cond_trans_commutative = false;
                    debug!(
                        "PlanRewriter: meet a new conditional/transformed commutative plan, expanding now: {plan}"
                    );
                    return Ok(true);
                }
                _ => (),
            }
        }

        match Categorizer::check_plan(plan, self.partition_cols.clone())? {
            Commutativity::Commutative => {
                // PATCH: we should reconsider SORT's commutativity instead of doing this trick.
                // explain: for a fully commutative SeriesDivide, its child Sort plan only serves it. I.e., that
                //   Sort plan is also fully commutative, instead of conditional commutative. So we can remove
                //   the generated MergeSort from stage safely.
                if let LogicalPlan::Extension(ext_a) = plan
                    && ext_a.node.name() == SeriesDivide::name()
                    && let Some(LogicalPlan::Extension(ext_b)) = self.stage.last()
                    && ext_b.node.name() == MergeSortLogicalPlan::name()
                {
                    // revert last `ConditionalCommutative` result for Sort plan in this case.
                    // `update_column_requirements` left unchanged because Sort won't generate
                    // new columns or remove existing columns.
                    self.stage.pop();
                    self.expand_on_next_part_cond_trans_commutative = false;
                }
            }
            Commutativity::PartialCommutative => {
                if let Some(plan) = partial_commutative_transformer(plan) {
                    // notice this plan is parent of current node, so `self.level - 1` when updating column requirements
                    self.update_column_requirements(&plan, self.level - 1);
                    self.expand_on_next_part_cond_trans_commutative = true;
                    self.stage.push(plan)
                }
            }
            Commutativity::ConditionalCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan)
                {
                    // notice this plan is parent of current node, so `self.level - 1` when updating column requirements
                    self.update_column_requirements(&plan, self.level - 1);
                    self.expand_on_next_part_cond_trans_commutative = true;
                    self.stage.push(plan)
                }
            }
            Commutativity::TransformedCommutative { transformer } => {
                if let Some(transformer) = transformer {
                    let transformer_actions = transformer(plan)?;
                    debug!(
                        "PlanRewriter: transformed plan: {}\n from {plan}",
                        transformer_actions
                            .extra_parent_plans
                            .iter()
                            .enumerate()
                            .map(|(i, p)| format!(
                                "Extra {i}-th parent plan from parent to child = {}",
                                p.display()
                            ))
                            .collect::<Vec<_>>()
                            .join("\n")
                    );
                    if let Some(new_child_plan) = &transformer_actions.new_child_plan {
                        debug!("PlanRewriter: new child plan: {}", new_child_plan);
                    }
                    if let Some(last_stage) = transformer_actions.extra_parent_plans.last() {
                        // update the column requirements from the last stage
                        // notice current plan's parent plan is where we need to apply the column requirements
                        self.update_column_requirements(last_stage, self.level - 1);
                    }
                    self.stage
                        .extend(transformer_actions.extra_parent_plans.into_iter().rev());
                    self.expand_on_next_call = true;
                    self.new_child_plan = transformer_actions.new_child_plan;
                }
            }
            Commutativity::NonCommutative
            | Commutativity::Unimplemented
            | Commutativity::Unsupported => {
                debug!("PlanRewriter: meet a non-commutative plan, expanding now: {plan}");
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Update the column requirements for the current plan, plan_level is the level of the plan
    /// in the stack, which is used to determine if the column requirements are applicable
    /// for other plans in the stack.
    fn update_column_requirements(&mut self, plan: &LogicalPlan, plan_level: usize) {
        debug!(
            "PlanRewriter: update column requirements for plan: {plan}\n with old column_requirements: {:?}",
            self.column_requirements
        );
        let mut container = HashSet::new();
        for expr in plan.expressions() {
            // this method won't fail
            let _ = expr_to_columns(&expr, &mut container);
        }

        self.column_requirements.push((container, plan_level));
        debug!(
            "PlanRewriter: updated column requirements: {:?}",
            self.column_requirements
        );
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

    fn maybe_set_partitions(&mut self, plan: &LogicalPlan) -> DfResult<()> {
        if let Some(part_cols) = &mut self.partition_cols {
            // update partition alias
            let child = plan.inputs().first().cloned().ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "PlanRewriter: maybe_set_partitions: plan has no child: {plan}"
                ))
            })?;

            for (_col_name, alias_set) in part_cols.iter_mut() {
                let aliased_cols = aliased_columns_for(
                    &alias_set.clone().into_iter().collect(),
                    plan,
                    Some(child),
                )?;
                *alias_set = aliased_cols.into_values().flatten().collect();
            }

            debug!(
                "PlanRewriter: maybe_set_partitions: updated partition columns: {:?} at plan: {}",
                part_cols,
                plan.display()
            );

            return Ok(());
        }

        if let LogicalPlan::TableScan(table_scan) = plan
            && let Some(source) = table_scan
                .source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
            && let Some(provider) = source
                .table_provider
                .as_any()
                .downcast_ref::<DfTableProviderAdapter>()
        {
            // Extract vector search hint from the adapter if present.
            if self.vector_search_request.is_none() {
                self.vector_search_request = provider.get_vector_search_hint();
            }

            let table = provider.table();
            if table.table_type() == TableType::Base {
                let info = table.table_info();
                let partition_key_indices = info.meta.partition_key_indices.clone();
                let schema = info.meta.schema.clone();
                let mut partition_cols = partition_key_indices
                    .into_iter()
                    .map(|index| schema.column_name_by_index(index).to_string())
                    .collect::<Vec<String>>();

                let partition_rules = table.partition_rules();
                let exist_phy_part_cols_not_in_logical_table = partition_rules
                    .map(|r| !r.extra_phy_cols_not_in_logical_table.is_empty())
                    .unwrap_or(false);

                if exist_phy_part_cols_not_in_logical_table && partition_cols.is_empty() {
                    // there are other physical partition columns that are not in logical table and part cols are empty
                    // so we need to add a placeholder for it to prevent certain optimization
                    // this is used to make sure the final partition columns(that optimizer see) are not empty
                    // notice if originally partition_cols is not empty, then there is no need to add this place holder,
                    // as subset of phy part cols can still be used for certain optimization, and it works as if
                    // those columns are always null
                    // This helps with distinguishing between non-partitioned table and partitioned table with all phy part cols not in logical table
                    partition_cols.push(OTHER_PHY_PART_COL_PLACEHOLDER.to_string());
                }
                self.partition_cols = Some(
                            partition_cols
                                .into_iter()
                                .map(|c| {
                                    if c == OTHER_PHY_PART_COL_PLACEHOLDER {
                                        // for placeholder, just return a empty alias
                                        return Ok((c.clone(), BTreeSet::new()));
                                    }
                                    let index =
                                        if let Some(c) = plan.schema().index_of_column_by_name(None, &c){
                                            c
                                        } else {
                                            // the `projection` field of `TableScan` doesn't contain the partition columns,
                                            // this is similar to not having a alias, hence return empty alias set
                                            return Ok((c.clone(), BTreeSet::new()))
                                        };
                                    let column = plan.schema().columns().get(index).cloned().ok_or_else(|| {
                                        datafusion_common::DataFusionError::Internal(format!(
                                            "PlanRewriter: maybe_set_partitions: column index {index} out of bounds in schema of plan: {plan}"
                                        ))
                                    })?;
                                    Ok((c.clone(), BTreeSet::from([column])))
                                })
                                .collect::<DfResult<AliasMapping>>()?,
                        );
            }
        }

        Ok(())
    }

    /// pop one stack item and reduce the level by 1
    fn pop_stack(&mut self) {
        self.level -= 1;
        self.stack.pop();
    }

    fn expand(&mut self, mut on_node: LogicalPlan) -> DfResult<LogicalPlan> {
        // store schema before expand, new child plan might have a different schema, so not using it
        let schema = on_node.schema().clone();
        if let Some(new_child_plan) = self.new_child_plan.take() {
            // if there is a new child plan, use it as the new root
            on_node = new_child_plan;
        }
        let mut rewriter = EnforceDistRequirementRewriter::new(
            std::mem::take(&mut self.column_requirements),
            self.level,
        );
        debug!(
            "PlanRewriter: enforce column requirements for node: {on_node} with rewriter: {rewriter:?}"
        );
        on_node = on_node.rewrite(&mut rewriter)?.data;
        debug!(
            "PlanRewriter: after enforced column requirements with rewriter: {rewriter:?} for node:\n{on_node}"
        );

        debug!(
            "PlanRewriter: expand on node: {on_node} with partition col alias mapping: {:?}",
            self.partition_cols
        );

        // If there's a vector search request, wrap the plan with VectorScanLogicalPlan.
        // This allows the hint to be serialized via Substrait and transmitted to datanodes.
        let inner_plan = if let Some(request) = self.vector_search_request.take() {
            debug!(
                "PlanRewriter: wrapping plan with VectorScanLogicalPlan for vector search: column_id={}, k={}",
                request.column_id, request.k
            );
            VectorScanLogicalPlan::new(
                on_node.clone(),
                request.column_id,
                request.query_vector,
                request.k,
                request.metric,
            )
            .into_logical_plan()
        } else {
            on_node.clone()
        };

        // add merge scan as the new root
        let mut node = MergeScanLogicalPlan::new(
            inner_plan,
            false,
            // at this stage, the partition cols should be set
            // treat it as non-partitioned if None
            self.partition_cols.clone().unwrap_or_default(),
        )
        .into_logical_plan();

        // expand stages
        for new_stage in self.stage.drain(..) {
            // tracking alias for merge sort's sort exprs
            let new_stage = if let LogicalPlan::Extension(ext) = &new_stage
                && let Some(merge_sort) = ext.node.as_any().downcast_ref::<MergeSortLogicalPlan>()
            {
                // TODO(discord9): change `on_node` to `node` once alias tracking is supported for merge scan
                rewrite_merge_sort_exprs(merge_sort, &on_node)?
            } else {
                new_stage
            };
            node = new_stage
                .with_new_exprs(new_stage.expressions_consider_join(), vec![node.clone()])?;
        }
        self.set_expanded();

        // recover the schema, this make sure after expand the schema is the same as old node
        // because after expand the raw top node might have extra columns i.e. sorting columns for `Sort` node
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
///
#[derive(Debug)]
struct EnforceDistRequirementRewriter {
    /// only enforce column requirements after the expanding node in question,
    /// meaning only for node with `cur_level` <= `level` will consider adding those column requirements
    /// TODO(discord9): a simpler solution to track column requirements for merge scan
    column_requirements: Vec<(HashSet<Column>, usize)>,
    /// only apply column requirements >= `cur_level`
    /// this is used to avoid applying column requirements that are not needed
    /// for the current node, i.e. the node is not in the scope of the column requirements
    /// i.e, for this plan:
    /// ```ignore
    /// Aggregate: min(t.number)
    ///   Projection: t.number
    /// ```
    /// when on `Projection` node, we don't need to apply the column requirements of `Aggregate` node
    /// because the `Projection` node is not in the scope of the `Aggregate` node
    cur_level: usize,
    plan_per_level: BTreeMap<usize, LogicalPlan>,
}

impl EnforceDistRequirementRewriter {
    fn new(column_requirements: Vec<(HashSet<Column>, usize)>, cur_level: usize) -> Self {
        Self {
            column_requirements,
            cur_level,
            plan_per_level: BTreeMap::new(),
        }
    }

    /// Return a mapping from (original column, level) to aliased columns in current node of all
    /// applicable column requirements
    /// i.e. only column requirements with level >= `cur_level` will be considered
    fn get_current_applicable_column_requirements(
        &self,
        node: &LogicalPlan,
    ) -> DfResult<BTreeMap<(Column, usize), BTreeSet<Column>>> {
        let col_req_per_level = self
            .column_requirements
            .iter()
            .filter(|(_, level)| *level >= self.cur_level)
            .collect::<Vec<_>>();

        // track alias for columns and use aliased columns instead
        // aliased col reqs at current level
        let mut result_alias_mapping = BTreeMap::new();
        let Some(child) = node.inputs().first().cloned() else {
            return Ok(Default::default());
        };
        for (col_req, level) in col_req_per_level {
            if let Some(original) = self.plan_per_level.get(level) {
                // query for alias in current plan
                let aliased_cols =
                    aliased_columns_for(&col_req.iter().cloned().collect(), node, Some(original))?;
                for original_col in col_req {
                    let aliased_cols = aliased_cols.get(original_col).cloned();
                    if let Some(cols) = aliased_cols
                        && !cols.is_empty()
                    {
                        result_alias_mapping.insert((original_col.clone(), *level), cols);
                    } else {
                        // if no aliased column found in current node, there should be alias in child node as promised by enforce col reqs
                        // because it should insert required columns in child node
                        // so we can find the alias in child node
                        // if not found, it's an internal error
                        let aliases_in_child = aliased_columns_for(
                            &[original_col.clone()].into(),
                            child,
                            Some(original),
                        )?;
                        let Some(aliases) = aliases_in_child
                            .get(original_col)
                            .cloned()
                            .filter(|a| !a.is_empty())
                        else {
                            return Err(datafusion_common::DataFusionError::Internal(format!(
                                "EnforceDistRequirementRewriter: no alias found for required column {original_col} in child plan {child} from original plan {original}",
                            )));
                        };

                        result_alias_mapping.insert((original_col.clone(), *level), aliases);
                    }
                }
            }
        }
        Ok(result_alias_mapping)
    }
}

impl TreeNodeRewriter for EnforceDistRequirementRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        // check that node doesn't have multiple children, i.e. join/subquery
        if node.inputs().len() > 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "EnforceDistRequirementRewriter: node with multiple inputs is not supported"
                    .to_string(),
            ));
        }
        self.plan_per_level.insert(self.cur_level, node.clone());
        self.cur_level += 1;
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        self.cur_level -= 1;
        // first get all applicable column requirements

        // make sure all projection applicable scope has the required columns
        if let LogicalPlan::Projection(ref projection) = node {
            let mut applicable_column_requirements =
                self.get_current_applicable_column_requirements(&node)?;

            debug!(
                "EnforceDistRequirementRewriter: applicable column requirements at level {} = {:?} for node {}",
                self.cur_level,
                applicable_column_requirements,
                node.display()
            );

            for expr in &projection.expr {
                let (qualifier, name) = expr.qualified_name();
                let column = Column::new(qualifier, name);
                applicable_column_requirements.retain(|_col_level, alias_set| {
                    // remove all columns that are already in the projection exprs
                    !alias_set.contains(&column)
                });
            }
            if applicable_column_requirements.is_empty() {
                return Ok(Transformed::no(node));
            }

            let mut new_exprs = projection.expr.clone();
            for (col, alias_set) in &applicable_column_requirements {
                // use the first alias in alias set as the column to add
                new_exprs.push(Expr::Column(alias_set.first().cloned().ok_or_else(
                    || {
                        datafusion_common::DataFusionError::Internal(
                            format!("EnforceDistRequirementRewriter: alias set is empty, for column {col:?} in node {node}"),
                        )
                    },
                )?));
            }
            let new_node =
                node.with_new_exprs(new_exprs, node.inputs().into_iter().cloned().collect())?;
            debug!(
                "EnforceDistRequirementRewriter: added missing columns {:?} to projection node from old node: \n{node}\n Making new node: \n{new_node}",
                applicable_column_requirements
            );

            // update plan for later use
            self.plan_per_level.insert(self.cur_level, new_node.clone());

            // still need to continue for next projection if applicable
            return Ok(Transformed::yes(new_node));
        }
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

        self.maybe_set_partitions(&node)?;

        let Some(parent) = self.get_parent() else {
            debug!("Plan Rewriter: expand now for no parent found for node: {node}");
            let node = self.expand(node);
            debug!(
                "PlanRewriter: expanded plan: {}",
                match &node {
                    Ok(n) => n.to_string(),
                    Err(e) => format!("Error expanding plan: {e}"),
                }
            );
            let node = node?;
            self.pop_stack();
            return Ok(Transformed::yes(node));
        };

        let parent = parent.clone();

        if self.should_expand(&parent)? {
            // TODO(ruihang): does this work for nodes with multiple children?;
            debug!(
                "PlanRewriter: should expand child:\n {node}\n Of Parent: {}",
                parent.display()
            );
            let node = self.expand(node);
            debug!(
                "PlanRewriter: expanded plan: {}",
                match &node {
                    Ok(n) => n.to_string(),
                    Err(e) => format!("Error expanding plan: {e}"),
                }
            );
            let node = node?;
            self.pop_stack();
            return Ok(Transformed::yes(node));
        }

        self.pop_stack();
        Ok(Transformed::no(node))
    }
}
