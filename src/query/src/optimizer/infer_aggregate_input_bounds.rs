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

use arrow_schema::DataType;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, Result, ScalarValue};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{Aggregate, Expr, ExprSchemable, Filter, LogicalPlan, Operator};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

use crate::dist_plan::MergeScanLogicalPlan;

/// Derives an input bound from an existential HAVING condition and MIN/MAX.
///
/// For example, `MIN(x), SUM(CASE WHEN p AND x < u THEN 1 ELSE 0 END)` with
/// HAVING `SUM(...) > 0` can discard rows outside `x < u`: they contribute zero
/// to SUM, and every surviving group has a witness below u, preserving its MIN.
/// MAX permits the symmetric lower bound. The original HAVING must remain.
///
/// This deliberately requires exactly these two aggregates and ordinary column
/// grouping. Extra aggregates, grouping sets, floating-point extrema and arbitrary
/// expressions need separate preservation proofs. It must run before MergeScan
/// hides remote inputs, as well as in the non-distributed logical optimizer.
#[derive(Debug)]
pub(crate) struct InferAggregateInputBoundsRule;

impl OptimizerRule for InferAggregateInputBoundsRule {
    fn name(&self) -> &str {
        "infer_aggregate_input_bounds"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(mut having) = plan else {
            return Ok(Transformed::no(plan));
        };
        let LogicalPlan::Aggregate(aggregate) = having.input.as_ref() else {
            return Ok(Transformed::no(LogicalPlan::Filter(having)));
        };
        // The pre-MergeScan pass already handled remote inputs. Their filters
        // are hidden from this pass, so re-inference can add useless local work.
        if aggregate.input.exists(|plan| {
            Ok(matches!(plan, LogicalPlan::Extension(extension)
                if extension.node.as_any().is::<MergeScanLogicalPlan>()))
        })? {
            return Ok(Transformed::no(LogicalPlan::Filter(having)));
        }
        let Some(bound) = infer_bound(aggregate, &having.predicate)? else {
            return Ok(Transformed::no(LogicalPlan::Filter(having)));
        };
        if already_filtered(&aggregate.input, &bound) {
            return Ok(Transformed::no(LogicalPlan::Filter(having)));
        }

        let mut aggregate = aggregate.clone();
        aggregate.input = Arc::new(LogicalPlan::Filter(Filter::try_new(
            bound,
            aggregate.input,
        )?));
        having.input = Arc::new(LogicalPlan::Aggregate(aggregate));
        Ok(Transformed::yes(LogicalPlan::Filter(having)))
    }
}

fn infer_bound(aggregate: &Aggregate, having: &Expr) -> Result<Option<Expr>> {
    if aggregate.aggr_expr.len() != 2
        || aggregate.group_expr.is_empty()
        || !aggregate
            .group_expr
            .iter()
            .all(|e| matches!(e, Expr::Column(_)))
        || !safe_predicate(having, aggregate.schema.as_ref())?
    {
        return Ok(None);
    }

    for conjunct in split_conjunction(having) {
        let Some(sum_index) = positive_sum_index(conjunct, aggregate) else {
            continue;
        };
        let Some(sum) = plain_aggregate(&aggregate.aggr_expr[sum_index]) else {
            continue;
        };
        let Some(extremum) = plain_aggregate(&aggregate.aggr_expr[1 - sum_index]) else {
            continue;
        };
        if let Some(bound) = bound_from_aggregates(sum, extremum, &aggregate.input)? {
            return Ok(Some(bound));
        }
    }
    Ok(None)
}

fn positive_sum_index(predicate: &Expr, aggregate: &Aggregate) -> Option<usize> {
    let Expr::BinaryExpr(comparison) = predicate else {
        return None;
    };
    let (output, threshold, operator) = match (comparison.left.as_ref(), comparison.right.as_ref())
    {
        (Expr::Column(column), literal) => (column, literal, comparison.op),
        (literal, Expr::Column(column)) => (column, literal, comparison.op.swap()?),
        _ => return None,
    };
    if !((operator == Operator::Gt && integer_literal(threshold) == Some(0))
        || (operator == Operator::GtEq && integer_literal(threshold) == Some(1)))
    {
        return None;
    }
    let index = aggregate.schema.index_of_column(output).ok()?;
    index
        .checked_sub(aggregate.group_expr.len())
        .filter(|&i| i < 2)
}

fn plain_aggregate(expr: &Expr) -> Option<&AggregateFunction> {
    let expr = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        _ => expr,
    };
    let Expr::AggregateFunction(function) = expr else {
        return None;
    };
    let params = &function.params;
    (params.args.len() == 1
        && !params.distinct
        && params.filter.is_none()
        && params.order_by.is_empty()
        && params.null_treatment.is_none())
    .then_some(function)
}

fn bound_from_aggregates(
    sum: &AggregateFunction,
    extremum: &AggregateFunction,
    input: &LogicalPlan,
) -> Result<Option<Expr>> {
    let schema = input.schema();
    // Compare function identities, not names: a user-defined "sum" need not
    // have the built-in aggregate's zero-contribution semantics.
    if sum.func != sum_udaf() || (extremum.func != min_udaf() && extremum.func != max_udaf()) {
        return Ok(None);
    }
    let argument = &extremum.params.args[0];
    let argument_type = argument.get_type(schema)?;
    if !(argument_type.is_integer() || matches!(argument_type, DataType::Timestamp(..)))
        || !safe_operand(argument, schema)?
    {
        return Ok(None);
    }
    let Some(condition) = indicator_condition(&sum.params.args[0]) else {
        return Ok(None);
    };
    if !safe_predicate(condition, schema)?
        || !columns_are_from_scan(argument, input)?
        || !columns_are_from_scan(condition, input)?
    {
        return Ok(None);
    }
    Ok(split_conjunction(condition)
        .into_iter()
        .find_map(|predicate| extremum_bound(predicate, argument, extremum, &argument_type)))
}

fn indicator_condition(expr: &Expr) -> Option<&Expr> {
    let Expr::Case(case) = expr else {
        return None;
    };
    let [(condition, contribution)] = case.when_then_expr.as_slice() else {
        return None;
    };
    (case.expr.is_none()
        && integer_literal(contribution) == Some(1)
        && case.else_expr.as_deref().and_then(integer_literal) == Some(0))
    .then_some(condition)
}

fn extremum_bound(
    predicate: &Expr,
    argument: &Expr,
    extremum: &AggregateFunction,
    argument_type: &DataType,
) -> Option<Expr> {
    let Expr::BinaryExpr(comparison) = predicate else {
        return None;
    };
    let (literal, operator) = if comparison.left.as_ref() == argument {
        (comparison.right.as_ref(), comparison.op)
    } else if comparison.right.as_ref() == argument {
        (comparison.left.as_ref(), comparison.op.swap()?)
    } else {
        return None;
    };
    let Expr::Literal(bound, _) = literal else {
        return None;
    };
    if bound.is_null() || &bound.data_type() != argument_type {
        return None;
    }
    let preserves_extremum = if extremum.func == min_udaf() {
        matches!(operator, Operator::Lt | Operator::LtEq)
    } else {
        matches!(operator, Operator::Gt | Operator::GtEq)
    };
    preserves_extremum.then(|| predicate.clone())
}

fn integer_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(integer), _) => *integer,
        Expr::Literal(ScalarValue::Int32(integer), _) => integer.map(i64::from),
        _ => None,
    }
}

/// Rejects explicitly volatile, fallible or subquery expressions. Column
/// references additionally require input-plan provenance checks.
fn safe_predicate(expr: &Expr, schema: &DFSchema) -> Result<bool> {
    match expr {
        Expr::BinaryExpr(binary) => Ok(matches!(
            binary.op,
            Operator::And
                | Operator::Or
                | Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        ) && safe_predicate(&binary.left, schema)?
            && safe_predicate(&binary.right, schema)?),
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => safe_operand(expr, schema),
        _ => safe_operand(expr, schema),
    }
}

fn safe_operand(expr: &Expr, schema: &DFSchema) -> Result<bool> {
    match expr {
        Expr::Column(_) | Expr::Literal(..) => Ok(true),
        // Timestamp-to-Int64 preserves its native unit and cannot fail. Keep
        // the cast intact rather than assuming timestamps are nanoseconds.
        Expr::Cast(cast) => Ok(cast.field.data_type() == &DataType::Int64
            && matches!(cast.expr.as_ref(), Expr::Column(_))
            && matches!(cast.expr.get_type(schema)?, DataType::Timestamp(..))),
        _ => Ok(false),
    }
}

/// A projected column can hide division, a fallible cast or a volatile UDF.
/// PushDownFilter may inline it below the WHERE that made evaluation safe.
fn columns_are_from_scan(expr: &Expr, input: &LogicalPlan) -> Result<bool> {
    for column in expr.column_refs() {
        if !column_is_from_scan(column, input)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn column_is_from_scan(column: &Column, input: &LogicalPlan) -> Result<bool> {
    let index = input.schema().index_of_column(column)?;
    match input {
        LogicalPlan::TableScan(_) => Ok(true),
        LogicalPlan::Filter(filter) => column_is_from_scan(column, &filter.input),
        LogicalPlan::SubqueryAlias(alias) => column_at_is_from_scan(index, &alias.input),
        LogicalPlan::Projection(projection) => {
            let expr = match &projection.expr[index] {
                Expr::Alias(alias) => alias.expr.as_ref(),
                expr => expr,
            };
            Ok(safe_operand(expr, projection.input.schema())?
                && columns_are_from_scan(expr, &projection.input)?)
        }
        LogicalPlan::Join(join) => {
            match (
                join.left.schema().has_column(column),
                join.right.schema().has_column(column),
            ) {
                (true, false) => column_is_from_scan(column, &join.left),
                (false, true) => column_is_from_scan(column, &join.right),
                _ => Ok(false),
            }
        }
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                if !column_at_is_from_scan(index, input)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        // Unknown plan nodes may synthesize columns or hide their origin.
        _ => Ok(false),
    }
}

fn column_at_is_from_scan(index: usize, input: &LogicalPlan) -> Result<bool> {
    let (qualifier, field) = input.schema().qualified_field(index);
    column_is_from_scan(&Column::new(qualifier.cloned(), field.name()), input)
}

fn already_filtered(input: &LogicalPlan, bound: &Expr) -> bool {
    match input {
        LogicalPlan::Filter(filter) => {
            split_conjunction(&filter.predicate).contains(&bound)
                || already_filtered(&filter.input, bound)
        }
        LogicalPlan::TableScan(scan) => scan
            .filters
            .iter()
            .any(|predicate| split_conjunction(predicate).contains(&bound)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt32Array,
    };
    use arrow::datatypes::UInt32Type;
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::SessionContext;
    use datafusion_optimizer::OptimizerContext;
    use datafusion_optimizer::optimizer::Optimizer;
    use datafusion_optimizer::push_down_filter::PushDownFilter;
    use datafusion_optimizer::simplify_expressions::SimplifyExpressions;

    use super::*;

    fn context() -> SessionContext {
        // Covers an earlier nonmatching row, both interval endpoints, NULL
        // values/predicates/group keys, and groups with no matching rows.
        let keys = vec![
            Some("a"),
            Some("a"),
            Some("a"),
            Some("a"),
            Some("a"),
            Some("b"),
            Some("c"),
            Some("c"),
            None,
            None,
            None,
        ];
        let numbers = vec![
            Some(5),
            Some(10),
            Some(20),
            Some(25),
            None,
            Some(25),
            Some(15),
            Some(4),
            Some(2),
            Some(12),
            Some(22),
        ];
        let flags = vec![
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            None,
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ];
        let services = DictionaryArray::<UInt32Type>::try_new(
            UInt32Array::from(
                flags
                    .iter()
                    .map(|flag| flag.map(|f| u32::from(!f)))
                    .collect::<Vec<_>>(),
            ),
            Arc::new(StringArray::from(vec!["cart", "other"])),
        )
        .unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(StringArray::from(keys)) as _),
            ("service", Arc::new(services) as _),
            ("v", Arc::new(Int64Array::from(numbers.clone())) as _),
            (
                "f",
                Arc::new(Float64Array::from(vec![1.0; numbers.len()])) as _,
            ),
            (
                "ts",
                Arc::new(TimestampNanosecondArray::from(numbers.clone())) as _,
            ),
            (
                "ts_ms",
                Arc::new(TimestampMillisecondArray::from(numbers)) as _,
            ),
            ("flag", Arc::new(BooleanArray::from(flags)) as _),
        ])
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    async fn analyzed(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await.unwrap();
        let plan = state
            .analyzer()
            .execute_and_check(plan, state.config_options(), |_, _| {})
            .unwrap();
        Optimizer::with_rules(vec![Arc::new(SimplifyExpressions::new())])
            .optimize(plan, &OptimizerContext::new(), |_, _| {})
            .unwrap()
    }

    fn optimize(plan: LogicalPlan) -> LogicalPlan {
        Optimizer::with_rules(vec![Arc::new(InferAggregateInputBoundsRule)])
            .optimize(plan, &OptimizerContext::new(), |_, _| {})
            .unwrap()
    }

    fn aggregate_input_bound(plan: &LogicalPlan) -> Option<&Expr> {
        if let LogicalPlan::Aggregate(aggregate) = plan
            && let LogicalPlan::Filter(filter) = aggregate.input.as_ref()
        {
            return Some(&filter.predicate);
        }
        plan.inputs().into_iter().find_map(aggregate_input_bound)
    }

    async fn rows(ctx: &SessionContext, plan: LogicalPlan) -> String {
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }

    #[tokio::test]
    async fn existential_having_preserves_extrema_and_conditional_sum() {
        let ctx = context();
        for (extremum, argument, condition, having, expected_bound) in [
            (
                "MIN",
                "v",
                "service = 'cart' AND v >= 10 AND v < 20",
                "n > 0",
                "t.v < Int64(20)",
            ),
            ("MIN", "v", "flag AND v <= 20", "n >= 1", "t.v <= Int64(20)"),
            (
                "MAX",
                "v",
                "flag AND v >= 10 AND v < 20",
                "0 < n",
                "t.v >= Int64(10)",
            ),
            ("MAX", "v", "flag AND 10 < v", "1 <= n", "t.v > Int64(10)"),
            (
                "MIN",
                "CAST(ts AS BIGINT)",
                "flag AND CAST(ts AS BIGINT) < 20",
                "n > 0",
                "CAST(t.ts AS Int64) < Int64(20)",
            ),
            (
                "MIN",
                "CAST(ts_ms AS BIGINT)",
                "flag AND CAST(ts_ms AS BIGINT) < 20",
                "n > 0",
                "CAST(t.ts_ms AS Int64) < Int64(20)",
            ),
            (
                "MIN",
                "ts",
                "flag AND ts < TIMESTAMP '1970-01-01 00:00:00.000000020'",
                "n > 0",
                "t.ts < TimestampNanosecond(20, None)",
            ),
        ] {
            let sql = format!(
                "SELECT k, {extremum}({argument}) AS endpoint,
                SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) AS n
                FROM t GROUP BY k HAVING {having} ORDER BY k NULLS FIRST"
            );
            let original = analyzed(&ctx, &sql).await;
            let optimized = optimize(original.clone());
            assert_eq!(
                aggregate_input_bound(&optimized).unwrap().to_string(),
                expected_bound,
                "{sql}"
            );
            assert_eq!(
                optimized,
                optimize(optimized.clone()),
                "rule must be idempotent: {sql}"
            );
            assert_eq!(
                rows(&ctx, original).await,
                rows(&ctx, optimized).await,
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn guarded_derived_operands_are_not_pruned() {
        let ctx = SessionContext::new();
        let batch = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(StringArray::from(vec!["a", "a", "b"])) as _),
            ("v", Arc::new(Int64Array::from(vec![12, 5, 25])) as _),
            ("d", Arc::new(Int64Array::from(vec![0, 1, 1])) as _),
            (
                "raw",
                Arc::new(StringArray::from(vec!["bad", "5", "25"])) as _,
            ),
        ])
        .unwrap();
        ctx.register_batch("guarded", batch).unwrap();
        ctx.sql("CREATE VIEW guarded_view AS SELECT k, v / d AS r FROM guarded WHERE abs(d) > 0")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        for source in [
            "SELECT k, v / d AS r FROM guarded WHERE abs(d) > 0",
            "SELECT k, quotient AS r FROM (SELECT k, v / d AS quotient FROM guarded WHERE abs(d) > 0) q",
            "SELECT k, CAST(raw AS BIGINT) AS r FROM guarded WHERE abs(d) > 0",
            "SELECT k, r FROM guarded_view",
            "SELECT k, v AS r FROM guarded UNION ALL SELECT k, v / d AS r FROM guarded WHERE abs(d) > 0",
            "SELECT l.k, l.r FROM guarded_view l JOIN guarded q ON l.k = q.k",
        ] {
            let sql = format!(
                "SELECT k, MIN(r) FROM ({source}) s GROUP BY k
                HAVING SUM(CASE WHEN r < 20 THEN 1 ELSE 0 END) > 0 ORDER BY k"
            );
            let original = analyzed(&ctx, &sql).await;
            let optimized = optimize(original.clone());
            assert_eq!(
                rows(&ctx, original.clone()).await,
                rows(&ctx, optimized.clone()).await,
                "{sql}"
            );
            assert_eq!(original, optimized, "{sql}");
        }
    }

    #[tokio::test]
    async fn scan_column_aliases_still_allow_bounds() {
        let ctx = context();
        for source in [
            "SELECT k, v AS r FROM t",
            "SELECT k, renamed AS r FROM (SELECT k, v AS renamed FROM t) q",
            "SELECT k, CAST(ts AS BIGINT) AS r FROM t",
            "SELECT k, v AS r FROM t UNION ALL SELECT k, v AS r FROM t",
            "SELECT l.k, l.v AS r FROM t l JOIN t r ON l.k = r.k",
        ] {
            let sql = format!(
                "SELECT k, MIN(r) FROM ({source}) s GROUP BY k
                HAVING SUM(CASE WHEN r < 20 THEN 1 ELSE 0 END) > 0 ORDER BY k NULLS FIRST"
            );
            let original = analyzed(&ctx, &sql).await;
            let optimized = optimize(original.clone());
            assert!(aggregate_input_bound(&optimized).is_some(), "{sql}");
            assert_eq!(
                rows(&ctx, original).await,
                rows(&ctx, optimized).await,
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn unsafe_or_unsupported_aggregates_are_not_pruned() {
        let ctx = context();
        let indicator = "SUM(CASE WHEN flag AND v < 20 THEN 1 ELSE 0 END)";
        for sql in [
            format!("SELECT k, MIN(v), COUNT(*) FROM t GROUP BY k HAVING {indicator}>0"),
            format!("SELECT k, MIN(v), MAX(v) FROM t GROUP BY k HAVING {indicator}>0"),
            format!("SELECT k, MIN(f) FROM t GROUP BY k HAVING {indicator}>0"),
            format!("SELECT k, MAX(v) FROM t GROUP BY k HAVING {indicator}>0"),
            format!("SELECT MIN(v) FROM t HAVING {indicator}>0"),
            format!("SELECT k, MIN(v) FROM t GROUP BY ROLLUP(k) HAVING {indicator}>0"),
            format!("SELECT k, MIN(v) FROM t GROUP BY k HAVING {indicator}=0"),
            format!("SELECT k, MIN(v) FROM t GROUP BY k HAVING {indicator}>0 OR MIN(v)>0"),
            format!("SELECT k, MIN(v) FILTER (WHERE flag) FROM t GROUP BY k HAVING {indicator}>0"),
            "SELECT k, MIN(v) FROM t GROUP BY k HAVING SUM(CASE WHEN v<20 THEN 1 ELSE 2 END)>0".into(),
            "SELECT k, MIN(v) FROM t GROUP BY k HAVING SUM(CASE WHEN v<20 THEN 1 END)>0".into(),
            "SELECT k, MIN(v) FROM t GROUP BY k HAVING SUM(DISTINCT CASE WHEN v<20 THEN 1 ELSE 0 END)>0".into(),
            "SELECT k, MIN(v) FROM t GROUP BY k HAVING SUM(CASE WHEN v<20 OR flag THEN 1 ELSE 0 END)>0".into(),
            "SELECT k, MIN(v) FROM t GROUP BY k HAVING SUM(CASE WHEN random()>0.5 AND v<20 THEN 1 ELSE 0 END)>0".into(),
            "SELECT k, MIN(CAST(k AS BIGINT)) FROM t GROUP BY k HAVING SUM(CASE WHEN CAST(k AS BIGINT)<20 THEN 1 ELSE 0 END)>0".into(),
            "SELECT k, MIN(v) FROM t GROUP BY k HAVING SUM(CASE WHEN v<20 THEN 1 WHEN flag THEN 1 ELSE 0 END)>0".into(),
            "SELECT k, MIN(f) FROM t GROUP BY k HAVING SUM(CASE WHEN f<20.0 THEN 1 ELSE 0 END)>0".into(),
            "SELECT k, MIN(v) FROM t GROUP BY k HAVING SUM(CASE WHEN v<20 THEN 1 ELSE 0 END)>0 AND SUM(CASE WHEN v<100 THEN 1 ELSE 0 END)>0".into(),
        ] {
            let original = analyzed(&ctx, &sql).await;
            assert_eq!(original, optimize(original.clone()), "{sql}");
        }
    }

    #[tokio::test]
    async fn nonpositive_thresholds_preserve_zero_match_groups() {
        let ctx = context();
        let expected = [
            "+---+----------+",
            "| k | endpoint |",
            "+---+----------+",
            "|   | 2        |",
            "| a | 5        |",
            "| b | 25       |",
            "| c | 4        |",
            "+---+----------+",
        ]
        .join("\n");
        for threshold in [">= 0", "> -1"] {
            let sql = format!(
                "SELECT k, MIN(v) AS endpoint FROM t GROUP BY k
                HAVING SUM(CASE WHEN flag AND v < 20 THEN 1 ELSE 0 END) {threshold}
                ORDER BY k NULLS FIRST"
            );
            let original = analyzed(&ctx, &sql).await;
            let optimized = optimize(original.clone());
            assert_eq!(original, optimized, "{sql}");
            assert_eq!(expected, rows(&ctx, optimized).await, "{sql}");
        }
    }

    #[tokio::test]
    async fn bound_survives_filter_pushdown_without_being_added_again() {
        let ctx = context();
        let original = analyzed(
            &ctx,
            "SELECT k, MIN(v) FROM t GROUP BY k
            HAVING SUM(CASE WHEN flag AND v<20 THEN 1 ELSE 0 END)>0",
        )
        .await;
        let optimizer = Optimizer::with_rules(vec![
            Arc::new(InferAggregateInputBoundsRule),
            Arc::new(PushDownFilter::new()),
        ]);
        let optimized = optimizer
            .optimize(original, &OptimizerContext::new(), |_, _| {})
            .unwrap();
        let repeated = optimizer
            .optimize(optimized.clone(), &OptimizerContext::new(), |_, _| {})
            .unwrap();
        assert_eq!(optimized, repeated);
        assert!(aggregate_input_bound(&optimized).is_some());
    }
}
