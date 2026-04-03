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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::datasource::DefaultTableSource;
use datafusion::sql::TableReference;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column, DFSchema, Result as DataFusionResult};
use datafusion_expr::expr_rewriter::normalize_cols;
use datafusion_expr::logical_plan::{Aggregate, Join, Projection, TableScan};
use datafusion_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use table::table::adapter::DfTableProviderAdapter;

use crate::dist_plan::MergeScanLogicalPlan;
use crate::dummy_catalog::DummyTableProvider;

#[derive(Debug)]
pub struct JoinReduceRule;

impl OptimizerRule for JoinReduceRule {
    fn name(&self) -> &str {
        "JoinReduceRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(Self::rewrite_plan)
    }
}

impl JoinReduceRule {
    fn rewrite_plan(plan: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = &plan else {
            return Ok(Transformed::no(plan));
        };

        if let Some(rewritten) = Self::try_reduce_projection(projection)? {
            Ok(Transformed::yes(rewritten))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    fn try_reduce_projection(projection: &Projection) -> DataFusionResult<Option<LogicalPlan>> {
        let LogicalPlan::Join(outer_join) = projection.input.as_ref() else {
            return Ok(None);
        };

        if let Some(rewritten) = Self::try_reduce_join_side(projection, outer_join, true)? {
            return Ok(Some(rewritten));
        }

        Self::try_reduce_join_side(projection, outer_join, false)
    }

    fn try_reduce_join_side(
        projection: &Projection,
        outer_join: &Join,
        projection_side_is_left: bool,
    ) -> DataFusionResult<Option<LogicalPlan>> {
        if outer_join.join_type != JoinType::Inner || outer_join.filter.is_some() {
            return Ok(None);
        }

        let projection_side_plan = if projection_side_is_left {
            outer_join.left.as_ref()
        } else {
            outer_join.right.as_ref()
        };
        let repeated_outer_plan = if projection_side_is_left {
            outer_join.right.as_ref()
        } else {
            outer_join.left.as_ref()
        };

        let Some(inner_projection) =
            Self::strip_subquery_aliases(projection_side_plan).as_projection()
        else {
            return Ok(None);
        };
        let Some(inner_join) =
            Self::strip_subquery_aliases(inner_projection.input.as_ref()).as_join()
        else {
            return Ok(None);
        };
        if inner_join.join_type != JoinType::Inner || inner_join.filter.is_some() {
            return Ok(None);
        }

        let Some((inner_repeated_plan, repeated_on_left)) =
            Self::find_matching_inner_side(inner_join, repeated_outer_plan)
        else {
            return Ok(None);
        };
        let reduced_repeated_plan = Self::relabel_repeated_plan(
            repeated_outer_plan,
            Self::preferred_passthrough_relation(projection).as_ref(),
        )?;
        let Some(inner_repeated_to_outer) =
            Self::build_column_map_by_name(inner_repeated_plan, &reduced_repeated_plan)
        else {
            return Ok(None);
        };
        let Some(outer_repeated_to_reduced) =
            Self::build_column_map_by_name(repeated_outer_plan, &reduced_repeated_plan)
        else {
            return Ok(None);
        };

        let outer_name_counts = Self::column_name_counts(outer_join.schema.columns());
        let projection_output_map = Self::build_projection_output_map(
            projection_side_plan,
            inner_projection,
            &inner_repeated_to_outer,
            &outer_name_counts,
        )?;
        let Some(outer_key_columns) = Self::validate_outer_join_keys(
            inner_join,
            outer_join,
            projection_side_is_left,
            &projection_output_map,
            repeated_outer_plan,
        ) else {
            return Ok(None);
        };

        if !Self::is_unique_on(repeated_outer_plan, &outer_key_columns) {
            return Ok(None);
        }

        let Some(reduced_input) = Self::replace_inner_join_side(
            inner_join,
            repeated_on_left,
            &reduced_repeated_plan,
            &inner_repeated_to_outer,
        ) else {
            return Ok(None);
        };
        let reduced_input = LogicalPlan::Join(reduced_input);

        let original_projection_exprs =
            normalize_cols(projection.expr.clone(), projection.input.as_ref())?;
        let rewritten_exprs = original_projection_exprs
            .iter()
            .cloned()
            .map(|expr| {
                let expr = Self::replace_columns_by_name(expr, &outer_repeated_to_reduced)?;
                Self::replace_columns_by_name(expr, &projection_output_map)
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        let rewritten_exprs = normalize_cols(rewritten_exprs, &reduced_input)?;
        let rewritten_exprs =
            Self::preserve_projection_output_columns(rewritten_exprs, projection, &reduced_input);
        let rewritten_exprs =
            Self::preserve_projection_output_names(rewritten_exprs, projection.schema.as_ref());

        let validated =
            Projection::try_new(rewritten_exprs.clone(), Arc::new(reduced_input.clone()))?;
        if !Self::same_schema_types(validated.schema.as_ref(), projection.schema.as_ref()) {
            return Ok(None);
        }

        Ok(Some(LogicalPlan::Projection(validated)))
    }

    fn strip_subquery_aliases(mut plan: &LogicalPlan) -> &LogicalPlan {
        while let LogicalPlan::SubqueryAlias(alias) = plan {
            plan = alias.input.as_ref();
        }
        plan
    }

    fn strip_passthrough_aliases_and_projections(mut plan: &LogicalPlan) -> &LogicalPlan {
        loop {
            match plan {
                LogicalPlan::SubqueryAlias(alias) => {
                    plan = alias.input.as_ref();
                }
                LogicalPlan::Projection(projection)
                    if Self::is_passthrough_projection(projection) =>
                {
                    plan = projection.input.as_ref();
                }
                _ => return plan,
            }
        }
    }

    fn same_repeated_source(left: &LogicalPlan, right: &LogicalPlan) -> bool {
        let left = Self::strip_passthrough_aliases_and_projections(left);
        let right = Self::strip_passthrough_aliases_and_projections(right);

        match (left, right) {
            (LogicalPlan::Filter(left), LogicalPlan::Filter(right)) => {
                Self::same_filter_predicate(&left.predicate, &right.predicate)
                    && Self::same_repeated_source(left.input.as_ref(), right.input.as_ref())
            }
            (LogicalPlan::Sort(left), LogicalPlan::Sort(right)) => {
                left.expr == right.expr
                    && left.fetch == right.fetch
                    && Self::same_repeated_source(left.input.as_ref(), right.input.as_ref())
            }
            (LogicalPlan::Repartition(left), LogicalPlan::Repartition(right)) => {
                left.partitioning_scheme == right.partitioning_scheme
                    && Self::same_repeated_source(left.input.as_ref(), right.input.as_ref())
            }
            (LogicalPlan::Limit(left), LogicalPlan::Limit(right)) => {
                left.skip == right.skip
                    && left.fetch == right.fetch
                    && Self::same_repeated_source(left.input.as_ref(), right.input.as_ref())
            }
            (LogicalPlan::Extension(left), LogicalPlan::Extension(right)) => {
                if let (Some(left_merge_scan), Some(right_merge_scan)) = (
                    left.node.as_any().downcast_ref::<MergeScanLogicalPlan>(),
                    right.node.as_any().downcast_ref::<MergeScanLogicalPlan>(),
                ) {
                    left_merge_scan.is_placeholder() == right_merge_scan.is_placeholder()
                        && left_merge_scan.partition_cols() == right_merge_scan.partition_cols()
                        && Self::same_repeated_source(
                            left_merge_scan.input(),
                            right_merge_scan.input(),
                        )
                } else {
                    left.node.name() == right.node.name()
                        && left.node.expressions() == right.node.expressions()
                        && match (
                            left.node.inputs().as_slice(),
                            right.node.inputs().as_slice(),
                        ) {
                            ([left_input], [right_input]) => {
                                Self::same_repeated_source(left_input, right_input)
                            }
                            _ => left == right,
                        }
                }
            }
            (LogicalPlan::TableScan(left), LogicalPlan::TableScan(right)) => {
                left.table_name == right.table_name
                    && left.filters == right.filters
                    && left.fetch == right.fetch
            }
            _ => left == right,
        }
    }

    fn is_passthrough_projection(projection: &Projection) -> bool {
        projection
            .expr
            .iter()
            .all(|expr| matches!(expr.clone().unalias(), Expr::Column(_)))
    }

    fn find_matching_inner_side<'a>(
        inner_join: &'a Join,
        repeated_outer_plan: &LogicalPlan,
    ) -> Option<(&'a LogicalPlan, bool)> {
        if Self::same_repeated_source(inner_join.left.as_ref(), repeated_outer_plan) {
            Some((inner_join.left.as_ref(), true))
        } else if Self::same_repeated_source(inner_join.right.as_ref(), repeated_outer_plan) {
            Some((inner_join.right.as_ref(), false))
        } else {
            None
        }
    }

    fn build_column_map_by_name(
        input_plan: &LogicalPlan,
        output_plan: &LogicalPlan,
    ) -> Option<HashMap<String, Expr>> {
        let output_columns = output_plan
            .schema()
            .columns()
            .into_iter()
            .map(|column| (column.name.clone(), column))
            .collect::<HashMap<_, _>>();
        let input_columns = input_plan.schema().columns();
        let input_name_counts = Self::column_name_counts(input_columns.clone());

        input_columns
            .into_iter()
            .try_fold(HashMap::new(), |mut mappings, input| {
                let output = output_columns.get(&input.name)?.clone();
                let expr = Expr::Column(output);
                mappings.insert(input.flat_name(), expr.clone());
                if input_name_counts
                    .get(&input.name)
                    .copied()
                    .unwrap_or_default()
                    == 1
                {
                    mappings.insert(input.name.clone(), expr);
                }
                Some(mappings)
            })
    }

    fn preferred_passthrough_relation(projection: &Projection) -> Option<TableReference> {
        let mut relations = projection
            .expr
            .iter()
            .zip(projection.schema.columns())
            .filter_map(|(expr, output_column)| {
                matches!(expr.clone().unalias(), Expr::Column(_))
                    .then_some(output_column.relation)
                    .flatten()
            });
        let first = relations.next()?;
        relations.all(|relation| relation == first).then_some(first)
    }

    fn relabel_repeated_plan(
        plan: &LogicalPlan,
        relation: Option<&TableReference>,
    ) -> DataFusionResult<LogicalPlan> {
        let Some(relation) = relation else {
            return Ok(plan.clone());
        };
        LogicalPlanBuilder::from(plan.clone())
            .alias(relation.clone())?
            .build()
    }

    fn replace_inner_join_side(
        inner_join: &Join,
        repeated_on_left: bool,
        repeated_outer_plan: &LogicalPlan,
        inner_repeated_to_outer: &HashMap<String, Expr>,
    ) -> Option<Join> {
        let rewritten_on = inner_join
            .on
            .iter()
            .map(|(left, right)| {
                Some((
                    Self::replace_columns_by_name(left.clone(), inner_repeated_to_outer).ok()?,
                    Self::replace_columns_by_name(right.clone(), inner_repeated_to_outer).ok()?,
                ))
            })
            .collect::<Option<Vec<_>>>()?;

        let replaced = Join::try_new(
            if repeated_on_left {
                Arc::new(repeated_outer_plan.clone())
            } else {
                inner_join.left.clone()
            },
            if repeated_on_left {
                inner_join.right.clone()
            } else {
                Arc::new(repeated_outer_plan.clone())
            },
            rewritten_on,
            inner_join.filter.clone(),
            inner_join.join_type,
            inner_join.join_constraint,
            inner_join.null_equality,
            inner_join.null_aware,
        )
        .ok()?;

        Self::join_exprs_match_inputs(&replaced).then_some(replaced)
    }

    fn join_exprs_match_inputs(join: &Join) -> bool {
        join.on.iter().all(|(left, right)| {
            left.column_refs()
                .iter()
                .all(|column| join.left.schema().has_column(column))
                && right
                    .column_refs()
                    .iter()
                    .all(|column| join.right.schema().has_column(column))
        })
    }

    fn build_projection_output_map(
        output_plan: &LogicalPlan,
        projection: &Projection,
        inner_repeated_to_outer: &HashMap<String, Expr>,
        outer_name_counts: &HashMap<String, usize>,
    ) -> DataFusionResult<HashMap<String, Expr>> {
        output_plan
            .schema()
            .columns()
            .into_iter()
            .zip(projection.expr.iter())
            .try_fold(HashMap::new(), |mut mappings, (column, expr)| {
                let expr =
                    Self::replace_columns_by_name(expr.clone().unalias(), inner_repeated_to_outer)?;
                mappings.insert(column.flat_name(), expr.clone());
                if outer_name_counts
                    .get(&column.name)
                    .copied()
                    .unwrap_or_default()
                    == 1
                {
                    mappings.insert(column.name.clone(), expr);
                }
                Ok(mappings)
            })
    }

    fn validate_outer_join_keys(
        inner_join: &Join,
        outer_join: &Join,
        projection_side_is_left: bool,
        projection_output_map: &HashMap<String, Expr>,
        repeated_outer_plan: &LogicalPlan,
    ) -> Option<Vec<Column>> {
        let repeated_by_name = repeated_outer_plan
            .schema()
            .columns()
            .into_iter()
            .map(|column| (column.name.clone(), column))
            .collect::<HashMap<_, _>>();
        let mut repeated_outer_key_columns = Vec::with_capacity(outer_join.on.len());

        for (left, right) in &outer_join.on {
            let projection_expr = if projection_side_is_left { left } else { right }
                .clone()
                .unalias();
            let repeated_expr = if projection_side_is_left { right } else { left }
                .clone()
                .unalias();

            let Expr::Column(projection_column) = projection_expr else {
                return None;
            };
            let Expr::Column(repeated_column) = repeated_expr else {
                return None;
            };

            let mapped = projection_output_map
                .get(&projection_column.flat_name())
                .cloned()
                .or_else(|| projection_output_map.get(&projection_column.name).cloned())?
                .unalias();
            let Expr::Column(reduced_join_column) = mapped else {
                return None;
            };
            let expected_repeated_column = repeated_by_name.get(&reduced_join_column.name)?;
            if *expected_repeated_column != repeated_column
                && !Self::join_equates_columns(inner_join, &reduced_join_column, &repeated_column)
            {
                return None;
            }

            repeated_outer_key_columns.push(repeated_column);
        }

        Some(repeated_outer_key_columns)
    }

    fn is_unique_on(plan: &LogicalPlan, requested_columns: &[Column]) -> bool {
        let requested_columns = Self::dedup_columns(requested_columns);
        match plan {
            LogicalPlan::SubqueryAlias(alias) => {
                Self::map_same_schema_columns(&requested_columns, plan, alias.input.as_ref())
                    .is_some_and(|mapped| Self::is_unique_on(alias.input.as_ref(), &mapped))
            }
            LogicalPlan::Projection(projection) => {
                Self::map_projection_columns(projection, plan, &requested_columns)
                    .is_some_and(|mapped| Self::is_unique_on(projection.input.as_ref(), &mapped))
            }
            LogicalPlan::Filter(filter) => {
                let mut augmented_columns = requested_columns.clone();
                augmented_columns.extend(Self::constant_filtered_columns(&filter.predicate));
                let augmented_columns = Self::dedup_columns(&augmented_columns);
                Self::is_unique_on(filter.input.as_ref(), &augmented_columns)
            }
            LogicalPlan::Sort(sort) => Self::is_unique_on(sort.input.as_ref(), &requested_columns),
            LogicalPlan::Repartition(repartition) => {
                Self::is_unique_on(repartition.input.as_ref(), &requested_columns)
            }
            LogicalPlan::Limit(limit) => {
                Self::is_unique_on(limit.input.as_ref(), &requested_columns)
            }
            LogicalPlan::Aggregate(aggregate) => {
                Self::aggregate_is_unique_on(aggregate, &requested_columns)
            }
            LogicalPlan::Extension(extension) => {
                if let Some(merge_scan) = extension
                    .node
                    .as_any()
                    .downcast_ref::<MergeScanLogicalPlan>()
                    && let Some(mapped) =
                        Self::map_same_schema_columns(&requested_columns, plan, merge_scan.input())
                {
                    return Self::is_unique_on(merge_scan.input(), &mapped);
                }

                if extension.node.name() == "SeriesDivide" {
                    let required = extension
                        .node
                        .expressions()
                        .into_iter()
                        .filter_map(|expr| expr.unalias().try_as_col().cloned())
                        .collect::<Vec<_>>();
                    return Self::requested_contains_all(&requested_columns, &required);
                }

                if extension.node.name() == "InstantManipulate"
                    && let Some(input) = extension.node.inputs().into_iter().next()
                    && let Some(mapped) =
                        Self::map_same_schema_columns(&requested_columns, plan, input)
                {
                    return Self::is_unique_on(input, &mapped);
                }

                false
            }
            LogicalPlan::TableScan(table_scan) => Self::table_scan_unique_key(table_scan)
                .is_some_and(|required| {
                    Self::requested_contains_all(&requested_columns, &required)
                }),
            _ => false,
        }
    }

    fn aggregate_is_unique_on(aggregate: &Aggregate, requested_columns: &[Column]) -> bool {
        aggregate
            .group_expr
            .iter()
            .map(|expr| expr.clone().unalias())
            .map(|expr| match expr {
                Expr::Column(column) => Some(column),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .is_some_and(|required| Self::requested_contains_all(requested_columns, &required))
    }

    fn constant_filtered_columns(predicate: &Expr) -> Vec<Column> {
        let mut columns = Vec::new();
        Self::collect_constant_filtered_columns(&predicate.clone().unalias(), &mut columns);
        columns
    }

    fn collect_constant_filtered_columns(expr: &Expr, columns: &mut Vec<Column>) {
        match expr {
            Expr::BinaryExpr(binary) if matches!(binary.op, datafusion_expr::Operator::And) => {
                Self::collect_constant_filtered_columns(binary.left.as_ref(), columns);
                Self::collect_constant_filtered_columns(binary.right.as_ref(), columns);
            }
            Expr::BinaryExpr(binary) if matches!(binary.op, datafusion_expr::Operator::Eq) => {
                match (binary.left.as_ref(), binary.right.as_ref()) {
                    (Expr::Column(column), Expr::Literal(_, _))
                    | (Expr::Literal(_, _), Expr::Column(column)) => {
                        columns.push(column.clone());
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    fn same_filter_predicate(left: &Expr, right: &Expr) -> bool {
        let mut left_terms = Vec::new();
        let mut right_terms = Vec::new();
        Self::collect_conjunct_terms(&left.clone().unalias(), &mut left_terms);
        Self::collect_conjunct_terms(&right.clone().unalias(), &mut right_terms);
        left_terms.sort_unstable();
        right_terms.sort_unstable();
        left_terms == right_terms
    }

    fn collect_conjunct_terms(expr: &Expr, terms: &mut Vec<String>) {
        match expr {
            Expr::BinaryExpr(binary) if matches!(binary.op, datafusion_expr::Operator::And) => {
                Self::collect_conjunct_terms(binary.left.as_ref(), terms);
                Self::collect_conjunct_terms(binary.right.as_ref(), terms);
            }
            _ => terms.push(expr.to_string()),
        }
    }

    fn map_projection_columns(
        projection: &Projection,
        output_plan: &LogicalPlan,
        requested_columns: &[Column],
    ) -> Option<Vec<Column>> {
        let output_map = output_plan
            .schema()
            .columns()
            .into_iter()
            .zip(projection.expr.iter())
            .map(|(output_column, expr)| (output_column.flat_name(), expr.clone().unalias()))
            .collect::<HashMap<_, _>>();

        requested_columns
            .iter()
            .map(|column| {
                let expr = output_map.get(&column.flat_name())?.clone();
                match expr {
                    Expr::Column(inner_column) => Some(inner_column),
                    _ => None,
                }
            })
            .collect()
    }

    fn map_same_schema_columns(
        requested_columns: &[Column],
        output_plan: &LogicalPlan,
        input_plan: &LogicalPlan,
    ) -> Option<Vec<Column>> {
        let output_columns = output_plan.schema().columns();
        let input_columns = input_plan.schema().columns();
        if output_columns.len() != input_columns.len() {
            return None;
        }

        let replace_map = output_columns
            .into_iter()
            .zip(input_columns)
            .collect::<HashMap<_, _>>();
        requested_columns
            .iter()
            .map(|column| replace_map.get(column).cloned())
            .collect()
    }

    fn table_scan_unique_key(table_scan: &TableScan) -> Option<Vec<Column>> {
        let source = table_scan
            .source
            .as_any()
            .downcast_ref::<DefaultTableSource>()?;

        if let Some(provider) = source
            .table_provider
            .as_any()
            .downcast_ref::<DummyTableProvider>()
        {
            let metadata = provider.region_metadata();
            let mut key_names = metadata
                .primary_key_columns()
                .map(|column| column.column_schema.name.clone())
                .collect::<Vec<_>>();
            key_names.push(metadata.time_index_column().column_schema.name.clone());
            return Self::schema_columns_by_names(
                table_scan.projected_schema.columns(),
                &key_names,
            );
        }

        if let Some(provider) = source
            .table_provider
            .as_any()
            .downcast_ref::<DfTableProviderAdapter>()
        {
            let table = provider.table();
            let mut key_names = table
                .primary_key_columns()
                .map(|column| column.name)
                .collect::<Vec<_>>();
            key_names.push(table.schema().timestamp_column()?.name.clone());
            return Self::schema_columns_by_names(
                table_scan.projected_schema.columns(),
                &key_names,
            );
        }

        None
    }

    fn schema_columns_by_names(
        schema_columns: Vec<Column>,
        key_names: &[String],
    ) -> Option<Vec<Column>> {
        let by_name = schema_columns
            .into_iter()
            .map(|column| (column.name.clone(), column))
            .collect::<HashMap<_, _>>();
        key_names
            .iter()
            .map(|name| by_name.get(name).cloned())
            .collect()
    }

    fn requested_contains_all(requested_columns: &[Column], required_columns: &[Column]) -> bool {
        let requested_flat = requested_columns
            .iter()
            .map(Column::flat_name)
            .collect::<HashSet<_>>();
        let requested_names = requested_columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<HashSet<_>>();
        required_columns.iter().all(|column| {
            requested_flat.contains(&column.flat_name()) || requested_names.contains(&column.name)
        })
    }

    fn replace_columns_by_name(
        expr: Expr,
        replace_map: &HashMap<String, Expr>,
    ) -> DataFusionResult<Expr> {
        expr.transform_up(|expr| {
            Ok(if let Expr::Column(column) = &expr {
                if let Some(replacement) = replace_map.get(&column.flat_name()) {
                    Transformed::yes(replacement.clone())
                } else {
                    Transformed::no(expr)
                }
            } else {
                Transformed::no(expr)
            })
        })
        .data()
    }

    fn column_name_counts(columns: Vec<Column>) -> HashMap<String, usize> {
        let mut counts = HashMap::with_capacity(columns.len());
        for column in columns {
            *counts.entry(column.name).or_insert(0) += 1;
        }
        counts
    }

    fn dedup_columns(columns: &[Column]) -> Vec<Column> {
        let mut unique = HashMap::with_capacity(columns.len());
        for column in columns {
            unique
                .entry(column.flat_name())
                .or_insert_with(|| column.clone());
        }
        unique.into_values().collect()
    }

    fn same_schema_types(left: &DFSchema, right: &DFSchema) -> bool {
        left.fields().len() == right.fields().len()
            && left
                .fields()
                .iter()
                .zip(right.fields().iter())
                .all(|(left, right)| {
                    left.data_type() == right.data_type()
                        && left.is_nullable() == right.is_nullable()
                })
    }

    fn join_equates_columns(join: &Join, left: &Column, right: &Column) -> bool {
        join.on.iter().any(|(join_left, join_right)| {
            matches!(
                (join_left.clone().unalias(), join_right.clone().unalias()),
                (Expr::Column(join_left), Expr::Column(join_right))
                    if (join_left == *left && join_right == *right)
                        || (join_left == *right && join_right == *left)
            )
        })
    }

    fn preserve_projection_output_names(exprs: Vec<Expr>, schema: &DFSchema) -> Vec<Expr> {
        exprs
            .into_iter()
            .zip(schema.fields().iter())
            .map(|(expr, field)| {
                if matches!(expr.clone().unalias(), Expr::Column(_)) {
                    return expr;
                }

                let output_name = field.name();
                if expr.schema_name().to_string() == *output_name {
                    expr
                } else {
                    Expr::Alias(datafusion_expr::expr::Alias::new(
                        expr,
                        None::<String>,
                        output_name.clone(),
                    ))
                }
            })
            .collect()
    }

    fn preserve_projection_output_columns(
        exprs: Vec<Expr>,
        projection: &Projection,
        input: &LogicalPlan,
    ) -> Vec<Expr> {
        let original_output_columns = projection.schema.columns();
        exprs
            .into_iter()
            .zip(projection.expr.iter())
            .zip(original_output_columns)
            .map(|((expr, original_expr), output_column)| {
                if matches!(original_expr.clone().unalias(), Expr::Column(_))
                    && input.schema().has_column(&output_column)
                {
                    Expr::Column(output_column)
                } else {
                    expr
                }
            })
            .collect()
    }
}

trait LogicalPlanExt {
    fn as_join(&self) -> Option<&Join>;
    fn as_projection(&self) -> Option<&Projection>;
}

impl LogicalPlanExt for LogicalPlan {
    fn as_join(&self) -> Option<&Join> {
        match self {
            LogicalPlan::Join(join) => Some(join),
            _ => None,
        }
    }

    fn as_projection(&self) -> Option<&Projection> {
        match self {
            LogicalPlan::Projection(projection) => Some(projection),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::datasource::provider_as_source;
    use datafusion_common::NullEquality;
    use datafusion_expr::expr::{Alias, BinaryExpr};
    use datafusion_expr::{JoinConstraint, LogicalPlanBuilder, Operator, col, lit};
    use datafusion_optimizer::OptimizerContext;

    use super::*;
    use crate::dist_plan::MergeScanLogicalPlan;
    use crate::optimizer::test_util::{mock_table_provider, mock_table_provider_with_tsid};

    fn optimize(plan: LogicalPlan) -> LogicalPlan {
        JoinReduceRule
            .rewrite(plan, &OptimizerContext::new())
            .unwrap()
            .data
    }

    fn build_scan(table_name: &str, with_tsid: bool) -> LogicalPlan {
        let provider = if with_tsid {
            Arc::new(mock_table_provider_with_tsid(1.into())) as _
        } else {
            Arc::new(mock_table_provider(1.into())) as _
        };

        LogicalPlanBuilder::scan(table_name, provider_as_source(provider), None)
            .unwrap()
            .build()
            .unwrap()
    }

    fn alias_plan(plan: LogicalPlan, alias: &str) -> LogicalPlan {
        LogicalPlanBuilder::from(plan)
            .alias(alias)
            .unwrap()
            .build()
            .unwrap()
    }

    fn merge_scan(plan: LogicalPlan) -> LogicalPlan {
        MergeScanLogicalPlan::new(plan, false, Default::default()).into_logical_plan()
    }

    fn join_on(
        left: LogicalPlan,
        right: LogicalPlan,
        left_keys: &[&str],
        right_keys: &[&str],
    ) -> LogicalPlan {
        let on = left_keys
            .iter()
            .zip(right_keys.iter())
            .map(|(left_key, right_key)| (col(*left_key), col(*right_key)))
            .collect::<Vec<_>>();
        LogicalPlan::Join(
            Join::try_new(
                Arc::new(left),
                Arc::new(right),
                on,
                None,
                JoinType::Inner,
                JoinConstraint::On,
                NullEquality::NullEqualsNull,
                false,
            )
            .unwrap(),
        )
    }

    #[test]
    fn reduces_redundant_join_with_unique_scan_key() {
        let repeated = alias_plan(build_scan("a", false), "a");
        let other = alias_plan(build_scan("b", false), "b");
        let inner_join = join_on(
            repeated.clone(),
            other,
            &["a.k0", "a.ts"],
            &["b.k0", "b.ts"],
        );
        let inner_projection = LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    col("a.k0"),
                    col("a.ts"),
                    Expr::Alias(Alias::new(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(col("a.v0")),
                            op: Operator::Minus,
                            right: Box::new(col("b.v0")),
                        }),
                        None::<String>,
                        "delta".to_string(),
                    )),
                ],
                Arc::new(inner_join),
            )
            .unwrap(),
        );
        let outer_join = join_on(
            alias_plan(inner_projection, "delta"),
            repeated,
            &["delta.k0", "delta.ts"],
            &["a.k0", "a.ts"],
        );
        let outer_projection = LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    col("a.k0"),
                    col("a.ts"),
                    Expr::Alias(Alias::new(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(col("delta")),
                            op: Operator::Divide,
                            right: Box::new(col("a.v0")),
                        }),
                        None::<String>,
                        "ratio".to_string(),
                    )),
                ],
                Arc::new(outer_join),
            )
            .unwrap(),
        );

        let optimized = optimize(outer_projection);
        let formatted = optimized.display_indent_schema().to_string();
        assert_eq!(formatted.matches("Inner Join").count(), 1, "{formatted}");
        assert!(formatted.contains("ratio"), "{formatted}");
    }

    #[test]
    fn keeps_outer_join_when_repeated_side_is_not_unique() {
        let repeated = alias_plan(build_scan("a", false), "a");
        let other = alias_plan(build_scan("b", false), "b");
        let inner_join = join_on(repeated.clone(), other, &["a.k0"], &["b.k0"]);
        let inner_projection = LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    col("a.k0"),
                    Expr::Alias(Alias::new(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(col("a.v0")),
                            op: Operator::Minus,
                            right: Box::new(col("b.v0")),
                        }),
                        None::<String>,
                        "delta".to_string(),
                    )),
                ],
                Arc::new(inner_join),
            )
            .unwrap(),
        );
        let outer_join = join_on(
            alias_plan(inner_projection, "delta"),
            repeated,
            &["delta.k0"],
            &["a.k0"],
        );
        let outer_projection = LogicalPlan::Projection(
            Projection::try_new(vec![col("delta"), col("a.v0")], Arc::new(outer_join)).unwrap(),
        );

        let optimized = optimize(outer_projection);
        let formatted = optimized.display_indent_schema().to_string();
        assert_eq!(formatted.matches("Inner Join").count(), 2, "{formatted}");
    }

    #[test]
    fn reduces_join_when_projection_uses_other_side_keys() {
        let repeated = alias_plan(build_scan("a", false), "a");
        let other = alias_plan(build_scan("b", false), "b");
        let inner_join = join_on(
            repeated.clone(),
            other,
            &["a.k0", "a.ts"],
            &["b.k0", "b.ts"],
        );
        let inner_projection = LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    col("b.k0"),
                    col("b.ts"),
                    Expr::Alias(Alias::new(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(col("a.v0")),
                            op: Operator::Minus,
                            right: Box::new(col("b.v0")),
                        }),
                        None::<String>,
                        "delta".to_string(),
                    )),
                ],
                Arc::new(inner_join),
            )
            .unwrap(),
        );
        let outer_join = join_on(
            alias_plan(inner_projection, "delta"),
            repeated,
            &["delta.k0", "delta.ts"],
            &["a.k0", "a.ts"],
        );
        let outer_projection = LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    col("a.k0"),
                    col("a.ts"),
                    Expr::Alias(Alias::new(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(col("delta")),
                            op: Operator::Divide,
                            right: Box::new(col("a.v0")),
                        }),
                        None::<String>,
                        "ratio".to_string(),
                    )),
                ],
                Arc::new(outer_join),
            )
            .unwrap(),
        );

        let optimized = optimize(outer_projection);
        let formatted = optimized.display_indent_schema().to_string();
        assert_eq!(formatted.matches("Inner Join").count(), 1, "{formatted}");
    }

    #[test]
    fn reduces_join_when_repeated_filters_reorder_conjuncts() {
        let repeated = alias_plan(build_scan("a", false), "a");
        let repeated_left = LogicalPlanBuilder::from(repeated.clone())
            .filter(col("a.k0").eq(lit("x")).and(col("a.ts").eq(lit(1_i64))))
            .unwrap()
            .build()
            .unwrap();
        let repeated_right = LogicalPlanBuilder::from(repeated)
            .filter(col("a.ts").eq(lit(1_i64)).and(col("a.k0").eq(lit("x"))))
            .unwrap()
            .build()
            .unwrap();
        let other = alias_plan(build_scan("b", false), "b");
        let inner_join = join_on(repeated_left, other, &["a.k0", "a.ts"], &["b.k0", "b.ts"]);
        let inner_projection = LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    col("b.k0"),
                    col("b.ts"),
                    Expr::Alias(Alias::new(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(col("a.v0")),
                            op: Operator::Minus,
                            right: Box::new(col("b.v0")),
                        }),
                        None::<String>,
                        "delta".to_string(),
                    )),
                ],
                Arc::new(inner_join),
            )
            .unwrap(),
        );
        let outer_join = join_on(
            alias_plan(inner_projection, "delta"),
            repeated_right,
            &["delta.k0", "delta.ts"],
            &["a.k0", "a.ts"],
        );
        let outer_projection = LogicalPlan::Projection(
            Projection::try_new(vec![col("delta"), col("a.v0")], Arc::new(outer_join)).unwrap(),
        );

        let optimized = optimize(outer_projection);
        let formatted = optimized.display_indent_schema().to_string();
        assert_eq!(formatted.matches("Inner Join").count(), 1, "{formatted}");
    }

    #[test]
    fn reduces_join_through_merge_scan_with_reordered_filters() {
        let repeated = alias_plan(build_scan("a", false), "a");
        let repeated_left = merge_scan(
            LogicalPlanBuilder::from(repeated.clone())
                .filter(col("a.k0").eq(lit("x")).and(col("a.ts").eq(lit(1_i64))))
                .unwrap()
                .build()
                .unwrap(),
        );
        let repeated_right = merge_scan(
            LogicalPlanBuilder::from(repeated)
                .filter(col("a.ts").eq(lit(1_i64)).and(col("a.k0").eq(lit("x"))))
                .unwrap()
                .build()
                .unwrap(),
        );
        let other = alias_plan(build_scan("b", false), "b");
        let inner_join = join_on(repeated_left, other, &["a.k0", "a.ts"], &["b.k0", "b.ts"]);
        let inner_projection = LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    col("a.k0"),
                    col("a.ts"),
                    Expr::Alias(Alias::new(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(col("a.v0")),
                            op: Operator::Minus,
                            right: Box::new(col("b.v0")),
                        }),
                        None::<String>,
                        "delta".to_string(),
                    )),
                ],
                Arc::new(inner_join),
            )
            .unwrap(),
        );
        let outer_join = join_on(
            alias_plan(inner_projection, "delta"),
            repeated_right,
            &["delta.k0", "delta.ts"],
            &["a.k0", "a.ts"],
        );
        let outer_projection = LogicalPlan::Projection(
            Projection::try_new(vec![col("delta"), col("a.v0")], Arc::new(outer_join)).unwrap(),
        );

        let optimized = optimize(outer_projection);
        let formatted = optimized.display_indent_schema().to_string();
        assert_eq!(formatted.matches("Inner Join").count(), 1, "{formatted}");
    }

    #[test]
    fn reduces_join_through_merge_scan_wrappers() {
        let repeated = alias_plan(build_scan("a", false), "a");
        let repeated_left = merge_scan(
            LogicalPlanBuilder::from(repeated.clone())
                .filter(col("a.k0").eq(lit("x")).and(col("a.ts").eq(lit(1_i64))))
                .unwrap()
                .build()
                .unwrap(),
        );
        let repeated_right = merge_scan(
            LogicalPlanBuilder::from(repeated)
                .filter(col("a.ts").eq(lit(1_i64)).and(col("a.k0").eq(lit("x"))))
                .unwrap()
                .build()
                .unwrap(),
        );
        let repeated_left = LogicalPlanBuilder::from(repeated_left)
            .project(vec![col("a.v0"), col("a.k0"), col("a.ts")])
            .unwrap()
            .build()
            .unwrap();
        let other = merge_scan(alias_plan(build_scan("b", false), "b"));
        let other = LogicalPlanBuilder::from(other)
            .project(vec![col("b.v0"), col("b.k0"), col("b.ts")])
            .unwrap()
            .build()
            .unwrap();

        let inner_join = join_on(repeated_left, other, &["a.k0", "a.ts"], &["b.k0", "b.ts"]);
        let inner_projection = LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    col("b.k0"),
                    col("b.ts"),
                    Expr::Alias(Alias::new(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(col("a.v0")),
                            op: Operator::Minus,
                            right: Box::new(col("b.v0")),
                        }),
                        None::<String>,
                        "delta".to_string(),
                    )),
                ],
                Arc::new(inner_join),
            )
            .unwrap(),
        );
        let outer_join = join_on(
            alias_plan(inner_projection, "delta"),
            repeated_right,
            &["delta.k0", "delta.ts"],
            &["a.k0", "a.ts"],
        );
        let outer_projection = LogicalPlan::Projection(
            Projection::try_new(vec![col("delta"), col("a.v0")], Arc::new(outer_join)).unwrap(),
        );

        let optimized = optimize(outer_projection);
        let formatted = optimized.display_indent_schema().to_string();
        assert_eq!(formatted.matches("Inner Join").count(), 1, "{formatted}");
    }

    #[test]
    fn recognizes_series_divide_uniqueness() {
        let repeated = alias_plan(build_scan("metric", true), "metric");
        let divide = LogicalPlan::Extension(datafusion_expr::Extension {
            node: Arc::new(promql::extension_plan::SeriesDivide::new(
                vec!["__tsid".to_string()],
                "ts".to_string(),
                repeated.clone(),
            )),
        });
        let other = alias_plan(build_scan("other", true), "other");
        let inner_join = join_on(
            divide.clone(),
            other,
            &["metric.__tsid", "metric.ts"],
            &["other.__tsid", "other.ts"],
        );
        let inner_projection = LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    col("metric.__tsid"),
                    col("metric.ts"),
                    Expr::Alias(Alias::new(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(col("metric.v0")),
                            op: Operator::Minus,
                            right: Box::new(col("other.v0")),
                        }),
                        None::<String>,
                        "delta".to_string(),
                    )),
                ],
                Arc::new(inner_join),
            )
            .unwrap(),
        );
        let outer_join = join_on(
            alias_plan(inner_projection, "delta"),
            divide,
            &["delta.__tsid", "delta.ts"],
            &["metric.__tsid", "metric.ts"],
        );
        let outer_projection = LogicalPlan::Projection(
            Projection::try_new(vec![col("delta"), col("metric.v0")], Arc::new(outer_join))
                .unwrap(),
        );

        let optimized = optimize(outer_projection);
        let formatted = optimized.display_indent_schema().to_string();
        assert_eq!(formatted.matches("Inner Join").count(), 1, "{formatted}");
    }
}
