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

use datafusion::config::ConfigOptions;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, Sort};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore, lit};
use promql::extension_plan::{InstantManipulate, SeriesDivide, SeriesNormalize};
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;

use crate::QueryEngineContext;
use crate::optimizer::ExtensionAnalyzerRule;

/// Rewrites `count(<presence-preserving-agg>(<vector_selector>) by (...))` into a presence-based
/// group count.
///
/// This stays intentionally narrow:
/// - the outer aggregate must be plain `count`
/// - the inner aggregate must be a plain aggregate whose result existence is equivalent to input
///   group existence
/// - the inner input must be the direct instant-vector-selector plan
/// - the outer count must only group by the evaluation timestamp
#[derive(Debug)]
pub struct CountNestAggrRule;

impl ExtensionAnalyzerRule for CountNestAggrRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &QueryEngineContext,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform_down(&Self::rewrite_plan).map(|x| x.data)
    }
}

impl CountNestAggrRule {
    fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Sort(sort) = plan else {
            return Ok(Transformed::no(plan));
        };

        if let Some(rewritten) = Self::try_rewrite_sort(&sort)? {
            Ok(Transformed::yes(rewritten))
        } else {
            Ok(Transformed::no(LogicalPlan::Sort(sort)))
        }
    }

    fn try_rewrite_sort(sort: &Sort) -> Result<Option<LogicalPlan>> {
        if sort.fetch.is_some() {
            return Ok(None);
        }

        let LogicalPlan::Aggregate(outer_agg) = sort.input.as_ref() else {
            return Ok(None);
        };
        if outer_agg.group_expr.len() != 1 || outer_agg.aggr_expr.len() != 1 {
            return Ok(None);
        }
        let outer_time_expr = outer_agg.group_expr[0].clone();
        let outer_count_arg =
            match Self::aggregate_if(&outer_agg.aggr_expr[0], |name| name == "count") {
                Some((_, arg)) => arg,
                None => return Ok(None),
            };

        let LogicalPlan::Sort(inner_sort) = outer_agg.input.as_ref() else {
            return Ok(None);
        };
        if inner_sort.fetch.is_some() {
            return Ok(None);
        }

        let LogicalPlan::Aggregate(inner_agg) = inner_sort.input.as_ref() else {
            return Ok(None);
        };
        if inner_agg.aggr_expr.len() != 1 || inner_agg.group_expr.is_empty() {
            return Ok(None);
        }
        let (inner_is_count, inner_value_expr) =
            match Self::aggregate_if(&inner_agg.aggr_expr[0], |name| {
                Self::is_supported_inner_aggregate(name)
            }) {
                Some((name, arg)) => (name == "count", arg),
                None => return Ok(None),
            };
        let Expr::Column(_) = inner_value_expr else {
            return Ok(None);
        };

        let Expr::Column(outer_count_column) = outer_count_arg else {
            return Ok(None);
        };
        let inner_output_field = inner_agg.schema.field(inner_agg.group_expr.len());
        if outer_count_column.name != *inner_output_field.name() {
            return Ok(None);
        }

        if !Self::is_projection_chain_to_instant(inner_agg.input.as_ref()) {
            return Ok(None);
        }

        if !inner_agg
            .group_expr
            .iter()
            .all(|expr| matches!(expr, Expr::Column(_)))
        {
            return Ok(None);
        }

        let Some(time_expr_pos) = inner_agg
            .group_expr
            .iter()
            .position(|expr| expr == &outer_time_expr)
        else {
            return Ok(None);
        };

        let mut presence_group_exprs = Vec::with_capacity(inner_agg.group_expr.len());
        presence_group_exprs.push(outer_time_expr.clone());
        presence_group_exprs.extend(
            inner_agg
                .group_expr
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx != time_expr_pos)
                .map(|(_, expr)| expr.clone()),
        );

        let mut required_input_columns =
            Self::collect_required_input_columns(&presence_group_exprs, inner_value_expr);
        required_input_columns.extend(Self::collect_required_instant_columns(
            inner_agg.input.as_ref(),
        ));
        let presence_source = Self::rebuild_projection_chain_to_instant(
            inner_agg.input.as_ref(),
            &required_input_columns,
        )?;

        let outer_value_name = outer_agg
            .schema
            .field(outer_agg.group_expr.len())
            .name()
            .clone();
        let mut presence_input = LogicalPlanBuilder::from(presence_source);
        if !inner_is_count {
            presence_input = presence_input.filter(inner_value_expr.clone().is_not_null())?;
        }
        let presence_input = presence_input
            .project(presence_group_exprs.clone())?
            .distinct()?
            .build()?;

        let rewritten = LogicalPlanBuilder::from(presence_input)
            .aggregate(
                outer_agg.group_expr.clone(),
                vec![count_udaf().call(vec![lit(1_i64)]).alias(outer_value_name)],
            )?
            .sort(sort.expr.clone())?
            .build()?;

        Ok(Some(rewritten))
    }

    fn collect_required_input_columns(group_exprs: &[Expr], value_expr: &Expr) -> HashSet<String> {
        let mut required = HashSet::new();

        for expr in group_exprs {
            if let Expr::Column(column) = expr {
                required.insert(column.name.clone());
            }
        }
        if let Expr::Column(column) = value_expr {
            // Keep the value column in the pruned instant input so `InstantManipulate`
            // can still perform stale-NaN filtering before we project down to keys.
            required.insert(column.name.clone());
        }

        required
    }

    fn collect_required_instant_columns(plan: &LogicalPlan) -> HashSet<String> {
        let mut required = HashSet::new();
        Self::collect_required_instant_columns_into(plan, &mut required);
        required
    }

    fn collect_required_instant_columns_into(plan: &LogicalPlan, required: &mut HashSet<String>) {
        match plan {
            LogicalPlan::Projection(projection) => {
                for expr in &projection.expr {
                    required.extend(
                        expr.column_refs()
                            .into_iter()
                            .map(|column| column.name.clone()),
                    );
                }
                Self::collect_required_instant_columns_into(projection.input.as_ref(), required);
            }
            LogicalPlan::Extension(extension) => {
                for expr in extension.node.expressions() {
                    if let Expr::Column(column) = expr {
                        required.insert(column.name);
                    }
                }

                if extension.node.as_any().is::<SeriesDivide>()
                    && extension.node.inputs()[0]
                        .schema()
                        .fields()
                        .iter()
                        .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
                {
                    required.insert(DATA_SCHEMA_TSID_COLUMN_NAME.to_string());
                }

                if let Some(input) = extension.node.inputs().into_iter().next() {
                    Self::collect_required_instant_columns_into(input, required);
                }
            }
            _ => {}
        }
    }

    fn aggregate_if<F>(expr: &Expr, accept_name: F) -> Option<(&str, &Expr)>
    where
        F: FnOnce(&str) -> bool,
    {
        let Expr::AggregateFunction(func) = expr else {
            return None;
        };
        let name = func.func.name();
        if !accept_name(name)
            || func.params.filter.is_some()
            || func.params.distinct
            || !func.params.order_by.is_empty()
            || func.params.args.len() != 1
        {
            return None;
        }

        Some((name, &func.params.args[0]))
    }

    fn is_supported_inner_aggregate(name: &str) -> bool {
        matches!(
            name,
            "count" | "sum" | "avg" | "min" | "max" | "stddev_pop" | "var_pop"
        )
    }

    fn is_projection_chain_to_instant(plan: &LogicalPlan) -> bool {
        let mut current = plan;
        loop {
            match current {
                LogicalPlan::Projection(projection) => current = projection.input.as_ref(),
                LogicalPlan::Extension(ext) => {
                    return ext.node.as_any().is::<InstantManipulate>();
                }
                _ => return false,
            }
        }
    }

    fn rebuild_projection_chain_to_instant(
        plan: &LogicalPlan,
        required_columns: &HashSet<String>,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection(projection) => {
                let input = Self::rebuild_projection_chain_to_instant(
                    projection.input.as_ref(),
                    required_columns,
                )?;
                LogicalPlanBuilder::from(input)
                    .project(projection.expr.clone())?
                    .build()
            }
            LogicalPlan::Extension(extension) => {
                if let Some(instant) = extension.node.as_any().downcast_ref::<InstantManipulate>() {
                    let input =
                        Self::prune_instant_input(extension.node.inputs()[0], required_columns)?;
                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(instant.with_exprs_and_inputs(vec![], vec![input])?),
                    }));
                }

                Ok(plan.clone())
            }
            _ => Ok(plan.clone()),
        }
    }

    fn prune_instant_input(
        plan: &LogicalPlan,
        required_columns: &HashSet<String>,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Extension(extension) => {
                if let Some(normalize) = extension.node.as_any().downcast_ref::<SeriesNormalize>() {
                    let input =
                        Self::prune_instant_input(extension.node.inputs()[0], required_columns)?;
                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(normalize.with_exprs_and_inputs(vec![], vec![input])?),
                    }));
                }

                if let Some(divide) = extension.node.as_any().downcast_ref::<SeriesDivide>() {
                    let divide_input = extension.node.inputs()[0].clone();

                    let projection_exprs = divide_input
                        .schema()
                        .fields()
                        .iter()
                        .filter(|field| required_columns.contains(field.name()))
                        .map(|field| {
                            Expr::Column(datafusion_common::Column::from_name(field.name().clone()))
                        })
                        .collect::<Vec<_>>();
                    let projected_input = LogicalPlanBuilder::from(divide_input)
                        .project(projection_exprs)?
                        .build()?;

                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(
                            divide.with_exprs_and_inputs(vec![], vec![projected_input])?,
                        ),
                    }));
                }

                Ok(plan.clone())
            }
            _ => Ok(plan.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::EmptyRelation;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_common::{Column, DFSchema};
    use datafusion_expr::col;
    use datatypes::arrow::datatypes::{DataType, Field, TimeUnit};

    use super::*;

    /// Unmarked source: plain arrow fields, no region metadata. It carries the
    /// columns the fixtures reference plus `unused`, which nothing reads.
    fn source_plan() -> LogicalPlan {
        let fields = vec![
            (
                None,
                Arc::new(Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )),
            ),
            (None, Arc::new(Field::new("tag", DataType::Utf8, true))),
            (
                None,
                Arc::new(Field::new("value", DataType::Float64, false)),
            ),
            (None, Arc::new(Field::new("aux", DataType::Float64, true))),
            (
                None,
                Arc::new(Field::new("unused", DataType::Float64, true)),
            ),
        ];
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::new_with_metadata(fields, HashMap::new()).unwrap()),
        })
    }

    /// `SeriesDivide(tag, ts) -> InstantManipulate(ts, value)`.
    fn instant_chain() -> LogicalPlan {
        let divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                vec!["tag".to_string()],
                "ts".to_string(),
                source_plan(),
            )),
        });
        LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                0,
                1000,
                1000,
                1000,
                0,
                "ts".to_string(),
                vec!["tag".to_string()],
                Some("value".to_string()),
                divide,
            )),
        })
    }

    /// Builds the oracle shape `Sort <- count <- Sort <- sum <- <input>` and runs
    /// [`CountNestAggrRule::try_rewrite_sort`] on it.
    ///
    /// The rule only matches bare aggregate expressions, so both aggregates are
    /// built without aliases, and the outer `count` argument is a column named
    /// exactly like the inner aggregate's output field. That name is read back
    /// from the inner schema the same way the rule's own name check does.
    fn try_rewrite(input: LogicalPlan, inner_value: Expr) -> Result<Option<LogicalPlan>> {
        let inner_agg_plan = LogicalPlanBuilder::from(input)
            .aggregate(
                vec![col("ts"), col("tag")],
                vec![sum_udaf().call(vec![inner_value])],
            )
            .unwrap()
            .build()
            .unwrap();
        let inner_output_column = {
            let LogicalPlan::Aggregate(inner_agg) = &inner_agg_plan else {
                unreachable!("plan builder must produce an Aggregate node");
            };
            Column::new_unqualified(
                inner_agg
                    .schema
                    .field(inner_agg.group_expr.len())
                    .name()
                    .clone(),
            )
        };
        let inner_sort = LogicalPlanBuilder::from(inner_agg_plan)
            .sort(vec![col("ts").sort(true, true)])
            .unwrap()
            .build()
            .unwrap();
        let outer_agg = LogicalPlanBuilder::from(inner_sort)
            .aggregate(
                vec![col("ts")],
                vec![count_udaf().call(vec![Expr::Column(inner_output_column)])],
            )
            .unwrap()
            .build()
            .unwrap();
        let outer_sort = LogicalPlanBuilder::from(outer_agg)
            .sort(vec![col("ts").sort(true, true)])
            .unwrap()
            .build()
            .unwrap();

        let LogicalPlan::Sort(sort) = outer_sort else {
            unreachable!("plan builder must produce a Sort node");
        };
        CountNestAggrRule::try_rewrite_sort(&sort)
    }

    /// Strings of every projection expression in `plan`, in plan order.
    fn projection_exprs(plan: &LogicalPlan) -> Vec<String> {
        let mut exprs = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::Projection(projection) = node {
                exprs.extend(projection.expr.iter().map(|expr| expr.to_string()));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        exprs
    }

    /// Column names the `SeriesDivide` node reads after pruning.
    fn series_divide_input_columns(plan: &LogicalPlan) -> Vec<String> {
        let mut columns = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::Extension(extension) = node
                && extension.node.as_any().is::<SeriesDivide>()
            {
                columns = extension.node.inputs()[0]
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect();
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        columns
    }

    /// Asserts the shared post-rewrite invariants for one fixture case: the
    /// rewrite really happened, pruning below `SeriesDivide` kept `aux` (the
    /// retained projection still reads it) and dropped `unused`, and every
    /// `kept` expression fragment is still printed in the projection chain.
    fn assert_rewritten(rewritten: &LogicalPlan, case: &str, kept: &[&str]) {
        assert!(
            has_distinct(rewritten),
            "{case}: rewrite must occur: {rewritten}"
        );
        assert_eq!(
            series_divide_input_columns(rewritten),
            ["ts", "tag", "value", "aux"],
            "{case}: expected aux retained and unused pruned: {rewritten}"
        );
        let exprs = projection_exprs(rewritten);
        for &fragment in kept {
            assert!(
                exprs.iter().any(|expr| expr.contains(fragment)),
                "{case}: expected a projection expression containing {fragment:?}: {rewritten}"
            );
        }
    }

    fn has_distinct(plan: &LogicalPlan) -> bool {
        let mut found = false;
        plan.apply(|node| {
            if matches!(node, LogicalPlan::Distinct(_)) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        found
    }

    #[test]
    fn rewrite_retains_plain_aux_column_for_retained_projection() {
        let projection = LogicalPlanBuilder::from(instant_chain())
            .project(vec![col("ts"), col("tag"), col("value"), col("aux")])
            .unwrap()
            .build()
            .unwrap();

        let rewritten = try_rewrite(projection, col("value"))
            .expect("rewrite must not fail")
            .expect("rule must fire");
        assert_rewritten(&rewritten, "plain aux", &["aux"]);
    }

    #[test]
    fn rewrite_retains_computed_aux_alias_for_retained_projection() {
        let projection = LogicalPlanBuilder::from(instant_chain())
            .project(vec![
                col("ts"),
                col("tag"),
                col("value"),
                (col("aux") + lit(1.0)).alias("aux_plus_one"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let rewritten = try_rewrite(projection, col("value"))
            .expect("rewrite must not fail")
            .expect("rule must fire");
        assert_rewritten(&rewritten, "computed aux alias", &["aux +", "aux_plus_one"]);
    }

    #[test]
    fn rewrite_retains_aux_through_two_alias_layers() {
        let layer = LogicalPlanBuilder::from(instant_chain())
            .project(vec![
                col("ts"),
                col("tag"),
                col("value").alias("v1"),
                col("aux").alias("aux1"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let projection = LogicalPlanBuilder::from(layer)
            .project(vec![
                col("ts"),
                col("tag"),
                col("v1").alias("v2"),
                col("aux1"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let rewritten = try_rewrite(projection, col("v2"))
            .expect("rewrite must not fail")
            .expect("rule must fire");
        assert_rewritten(&rewritten, "two alias layers", &["aux AS aux1", "aux1"]);
    }
}
