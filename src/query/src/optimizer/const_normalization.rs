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

use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};
use datafusion::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion_common::{DFSchemaRef, Result, ScalarValue};
use datafusion_expr::expr::{Cast, InList, Like, TryCast};
use datafusion_expr::{Between, BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, lit};
use datafusion_expr_common::casts::try_cast_literal_to_type;
use datafusion_optimizer::analyzer::AnalyzerRule;

use crate::plan::ExtractExpr;

/// ConstNormalizationRule rewrites castable constants against their
/// non-constant comparison operand ahead of filter pushdown.
#[derive(Debug)]
pub struct ConstNormalizationRule;

impl AnalyzerRule for ConstNormalizationRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform(|plan| match plan {
            LogicalPlan::Filter(filter) => {
                let schema = filter.input.schema().clone();
                rewrite_plan_exprs(LogicalPlan::Filter(filter), schema)
            }
            LogicalPlan::TableScan(scan) => {
                let schema = scan.projected_schema.clone();
                rewrite_plan_exprs(LogicalPlan::TableScan(scan), schema)
            }
            _ => Ok(Transformed::no(plan)),
        })
        .map(|x| x.data)
    }

    fn name(&self) -> &str {
        "ConstNormalizationRule"
    }
}

fn rewrite_plan_exprs(plan: LogicalPlan, schema: DFSchemaRef) -> Result<Transformed<LogicalPlan>> {
    let mut rewriter = ConstNormalizationRewriter {
        schema,
        transformed: false,
    };
    let exprs = plan
        .expressions_consider_join()
        .into_iter()
        .map(|expr| expr.rewrite(&mut rewriter).map(|rewritten| rewritten.data))
        .collect::<Result<Vec<_>>>()?;
    if !rewriter.transformed {
        return Ok(Transformed::no(plan));
    }

    let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
    plan.with_new_exprs(exprs, inputs).map(Transformed::yes)
}

struct ConstNormalizationRewriter {
    schema: DFSchemaRef,
    transformed: bool,
}

impl TreeNodeRewriter for ConstNormalizationRewriter {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let recursion = if matches!(
            expr,
            Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
        ) {
            TreeNodeRecursion::Jump
        } else {
            TreeNodeRecursion::Continue
        };

        Ok(Transformed::new(expr, false, recursion))
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let rewritten = rewrite_expr_node(expr, &self.schema)?;
        self.transformed |= rewritten.transformed;
        Ok(rewritten)
    }
}

fn rewrite_expr_node(expr: Expr, schema: &DFSchemaRef) -> Result<Transformed<Expr>> {
    match expr {
        Expr::BinaryExpr(binary) => match rewrite_binary_expr(binary.clone(), schema)? {
            Some(expr) => Ok(Transformed::yes(expr)),
            None => Ok(Transformed::no(Expr::BinaryExpr(binary))),
        },
        Expr::Between(between) => match rewrite_between_expr(between.clone(), schema)? {
            Some(expr) => Ok(Transformed::yes(expr)),
            None => Ok(Transformed::no(Expr::Between(between))),
        },
        Expr::InList(in_list) => match rewrite_in_list_expr(in_list.clone(), schema)? {
            Some(expr) => Ok(Transformed::yes(expr)),
            None => Ok(Transformed::no(Expr::InList(in_list))),
        },
        Expr::Like(like) => rewrite_like_expr(like, PatternMatchKind::Like, schema),
        Expr::SimilarTo(like) => rewrite_like_expr(like, PatternMatchKind::SimilarTo, schema),
        expr => Ok(Transformed::no(expr)),
    }
}

fn rewrite_between_expr(between: Between, schema: &DFSchemaRef) -> Result<Option<Expr>> {
    let Between {
        expr,
        negated,
        low,
        high,
    } = between;
    let expr = *expr;
    let low_expr = *low;
    let high_expr = *high;
    let Some((target, constants)) =
        extract_rewrite_operands(&expr, &[low_expr.clone(), high_expr.clone()], schema)?
    else {
        return Ok(None);
    };

    if let Some(mut constants) = target.normalize_constants(&constants) {
        let high = constants
            .pop()
            .expect("between normalization expects high constant");
        let low = constants
            .pop()
            .expect("between normalization expects low constant");
        return Ok(Some(Expr::Between(Between {
            expr: Box::new(target.expr.clone()),
            negated,
            low: Box::new(lit(low)),
            high: Box::new(lit(high)),
        })));
    }

    Ok((!negated)
        .then(|| target.normalize_timestamp_between(&constants[0], &constants[1]))
        .flatten())
}

fn rewrite_in_list_expr(in_list: InList, schema: &DFSchemaRef) -> Result<Option<Expr>> {
    let InList {
        expr,
        list,
        negated,
    } = in_list;
    let expr = *expr;
    let Some((target, constants)) = extract_rewrite_operands(&expr, &list, schema)? else {
        return Ok(None);
    };

    Ok(target.normalize_constants(&constants).map(|constants| {
        target
            .expr
            .clone()
            .in_list(constants.into_iter().map(lit).collect(), negated)
    }))
}

fn rewrite_like_expr(
    like: Like,
    kind: PatternMatchKind,
    schema: &DFSchemaRef,
) -> Result<Transformed<Expr>> {
    let original = match kind {
        PatternMatchKind::Like => Expr::Like(like.clone()),
        PatternMatchKind::SimilarTo => Expr::SimilarTo(like.clone()),
    };
    let Like {
        negated,
        expr,
        pattern,
        escape_char,
        case_insensitive,
    } = like;
    let expr = *expr;
    let pattern = *pattern;
    let Some((target, constants)) =
        extract_rewrite_operands(&expr, std::slice::from_ref(&pattern), schema)?
    else {
        return Ok(Transformed::no(original));
    };
    let Some(mut constants) = target.normalize_constants(&constants) else {
        return Ok(Transformed::no(original));
    };

    let pattern = lit(constants
        .pop()
        .expect("pattern normalization expects one constant"));
    let like = Like::new(
        negated,
        Box::new(target.expr.clone()),
        Box::new(pattern),
        escape_char,
        case_insensitive,
    );
    let rewritten = match kind {
        PatternMatchKind::Like => Expr::Like(like),
        PatternMatchKind::SimilarTo => Expr::SimilarTo(like),
    };
    Ok(Transformed::yes(rewritten))
}

fn rewrite_binary_expr(binary: BinaryExpr, schema: &DFSchemaRef) -> Result<Option<Expr>> {
    if !binary.op.supports_propagation() {
        return Ok(None);
    }

    let BinaryExpr { left, op, right } = binary;
    let left = *left;
    let right = *right;
    if let Some(expr) = rewrite_binary_side(left.clone(), op, right.clone(), schema)? {
        return Ok(Some(expr));
    }

    let Some(swapped_op) = op.swap() else {
        return Ok(None);
    };

    rewrite_binary_side(right, swapped_op, left, schema)
}

fn rewrite_binary_side(
    target_expr: Expr,
    op: Operator,
    constant_expr: Expr,
    schema: &DFSchemaRef,
) -> Result<Option<Expr>> {
    let Some((target, constants)) =
        extract_rewrite_operands(&target_expr, std::slice::from_ref(&constant_expr), schema)?
    else {
        return Ok(None);
    };

    if let Some(mut constants) = target.normalize_constants(&constants) {
        let constant = constants
            .pop()
            .expect("binary normalization expects one constant");
        return Ok(Some(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(target.expr.clone()),
            op,
            right: Box::new(lit(constant)),
        })));
    }

    Ok(target.normalize_timestamp_binary(op, &constants[0]))
}

fn extract_rewrite_operands(
    target_expr: &Expr,
    constant_exprs: &[Expr],
    schema: &DFSchemaRef,
) -> Result<Option<(NormalizationTarget, Vec<ScalarValue>)>> {
    let Some(target) = extract_normalization_target(target_expr, schema)? else {
        return Ok(None);
    };

    extract_constant_scalars(constant_exprs)
        .map(|constants| constants.map(|constants| (target, constants)))
}

#[derive(Clone)]
struct NormalizationTarget {
    expr: Expr,
    data_type: DataType,
    kind: NormalizationKind,
}

#[derive(Clone)]
enum NormalizationKind {
    /// The cast preserves every source value exactly, so literals can be cast directly.
    Lossless,
    /// The cast drops timestamp precision and must widen predicate bounds to preserve semantics.
    TimestampDowncast {
        source_unit: ArrowTimeUnit,
        target_unit: ArrowTimeUnit,
        timezone: Option<Arc<str>>,
    },
}

impl NormalizationTarget {
    fn normalize_constants(&self, constants: &[ScalarValue]) -> Option<Vec<ScalarValue>> {
        constants
            .iter()
            .map(|constant| self.normalize_constant(constant))
            .collect()
    }

    fn normalize_constant(&self, constant: &ScalarValue) -> Option<ScalarValue> {
        match self.kind {
            NormalizationKind::TimestampDowncast { .. } => None,
            NormalizationKind::Lossless => cast_literal_losslessly(constant, &self.data_type),
        }
    }

    fn normalize_timestamp_binary(&self, op: Operator, constant: &ScalarValue) -> Option<Expr> {
        let NormalizationKind::TimestampDowncast {
            source_unit,
            target_unit,
            timezone,
        } = &self.kind
        else {
            return None;
        };

        let constant = constant
            .cast_to(&DataType::Timestamp(*target_unit, timezone.clone()))
            .ok()?;
        let value = timestamp_scalar_value(&constant)?;
        let bound = match op {
            Operator::GtEq => lower_bound_for_ge(value, *source_unit, *target_unit)?,
            Operator::Gt => lower_bound_for_ge(value.checked_add(1)?, *source_unit, *target_unit)?,
            Operator::Lt => lower_bound_for_ge(value, *source_unit, *target_unit)?,
            Operator::LtEq => {
                lower_bound_for_ge(value.checked_add(1)?, *source_unit, *target_unit)?
            }
            _ => return None,
        };

        let normalized_op = match op {
            Operator::GtEq | Operator::Gt => Operator::GtEq,
            Operator::Lt | Operator::LtEq => Operator::Lt,
            _ => return None,
        };

        Some(match normalized_op {
            Operator::GtEq => self.expr.clone().gt_eq(lit(timestamp_scalar(
                *source_unit,
                timezone.clone(),
                bound,
            ))),
            Operator::Lt => {
                self.expr
                    .clone()
                    .lt(lit(timestamp_scalar(*source_unit, timezone.clone(), bound)))
            }
            _ => unreachable!("timestamp normalization only rewrites to >= or <"),
        })
    }

    fn normalize_timestamp_between(&self, low: &ScalarValue, high: &ScalarValue) -> Option<Expr> {
        let NormalizationKind::TimestampDowncast {
            source_unit,
            target_unit,
            timezone,
        } = &self.kind
        else {
            return None;
        };

        let target_type = DataType::Timestamp(*target_unit, timezone.clone());
        let low = low.cast_to(&target_type).ok()?;
        let high = high.cast_to(&target_type).ok()?;
        let low = timestamp_scalar_value(&low)?;
        let high = timestamp_scalar_value(&high)?;

        let lower = lower_bound_for_ge(low, *source_unit, *target_unit)?;
        let upper = lower_bound_for_ge(high.checked_add(1)?, *source_unit, *target_unit)?;

        Some(
            self.expr
                .clone()
                .gt_eq(lit(timestamp_scalar(*source_unit, timezone.clone(), lower)))
                .and(self.expr.clone().lt(lit(timestamp_scalar(
                    *source_unit,
                    timezone.clone(),
                    upper,
                )))),
        )
    }
}

/// Returns the non-constant side we should normalize against.
///
/// Plain expressions normalize literals to their own type. Cast expressions only participate when
/// the cast is lossless or when timestamp downcasts can be rewritten as wider source-side bounds.
fn extract_normalization_target(
    expr: &Expr,
    schema: &DFSchemaRef,
) -> Result<Option<NormalizationTarget>> {
    if extract_constant_scalar(expr)?.is_some() {
        return Ok(None);
    }

    let Some((_, source_expr, target_type)) = extract_cast_input(expr) else {
        return Ok(Some(NormalizationTarget {
            expr: expr.clone(),
            data_type: expr.get_type(schema)?,
            kind: NormalizationKind::Lossless,
        }));
    };

    let data_type = source_expr.get_type(schema)?;
    let Some(kind) = classify_normalization_kind(&data_type, target_type) else {
        return Ok(None);
    };

    Ok(Some(NormalizationTarget {
        expr: source_expr.clone(),
        data_type,
        kind,
    }))
}

fn classify_normalization_kind(
    source_type: &DataType,
    target_type: &DataType,
) -> Option<NormalizationKind> {
    if is_lossless_cast(source_type, target_type) {
        return Some(NormalizationKind::Lossless);
    }

    match (source_type, target_type) {
        (
            DataType::Timestamp(source_unit, source_tz),
            DataType::Timestamp(target_unit, target_tz),
        ) if source_tz == target_tz
            && time_unit_rank(*source_unit) > time_unit_rank(*target_unit) =>
        {
            Some(NormalizationKind::TimestampDowncast {
                source_unit: *source_unit,
                target_unit: *target_unit,
                timezone: source_tz.clone(),
            })
        }
        _ => None,
    }
}

/// Returns whether every value of `source_type` is representable in `target_type`.
fn is_lossless_cast(source_type: &DataType, target_type: &DataType) -> bool {
    match (source_type, target_type) {
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (DataType::Int16, DataType::Int32 | DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::UInt8, DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt8, DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (DataType::UInt16, DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt16, DataType::Int32 | DataType::Int64)
        | (DataType::UInt32, DataType::UInt64 | DataType::Int64)
        | (DataType::Utf8, DataType::Utf8View | DataType::LargeUtf8) => true,
        (
            DataType::Timestamp(source_unit, source_tz),
            DataType::Timestamp(target_unit, target_tz),
        ) => source_tz == target_tz && time_unit_rank(*source_unit) <= time_unit_rank(*target_unit),
        _ => false,
    }
}

#[derive(Clone, Copy)]
enum PatternMatchKind {
    Like,
    SimilarTo,
}

fn extract_constant_scalars(exprs: &[Expr]) -> Result<Option<Vec<ScalarValue>>> {
    let mut values = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let Some(value) = extract_constant_scalar(expr)? else {
            return Ok(None);
        };
        values.push(value);
    }

    Ok(Some(values))
}

fn extract_constant_scalar(expr: &Expr) -> Result<Option<ScalarValue>> {
    if let Some(value) = expr.as_literal() {
        return Ok(Some(value.clone()));
    }

    let Some((kind, expr, data_type)) = extract_cast_input(expr) else {
        return Ok(None);
    };

    match kind {
        CastInputKind::Cast => extract_constant_scalar(expr)?
            .map(|value| value.cast_to(data_type))
            .transpose(),
        CastInputKind::TryCast => {
            Ok(extract_constant_scalar(expr)?.and_then(|value| value.cast_to(data_type).ok()))
        }
    }
}

fn cast_literal_losslessly(value: &ScalarValue, target_type: &DataType) -> Option<ScalarValue> {
    try_cast_literal_to_type(value, target_type)
}

#[derive(Clone, Copy)]
enum CastInputKind {
    Cast,
    TryCast,
}

fn extract_cast_input(expr: &Expr) -> Option<(CastInputKind, &Expr, &DataType)> {
    match expr {
        Expr::Cast(Cast { expr, data_type }) => {
            Some((CastInputKind::Cast, expr.as_ref(), data_type))
        }
        Expr::TryCast(TryCast { expr, data_type }) => {
            Some((CastInputKind::TryCast, expr.as_ref(), data_type))
        }
        _ => None,
    }
}

fn time_unit_rank(unit: ArrowTimeUnit) -> usize {
    match unit {
        ArrowTimeUnit::Second => 0,
        ArrowTimeUnit::Millisecond => 1,
        ArrowTimeUnit::Microsecond => 2,
        ArrowTimeUnit::Nanosecond => 3,
    }
}

fn time_unit_scale(unit: ArrowTimeUnit) -> i64 {
    match unit {
        ArrowTimeUnit::Second => 1,
        ArrowTimeUnit::Millisecond => 1_000,
        ArrowTimeUnit::Microsecond => 1_000_000,
        ArrowTimeUnit::Nanosecond => 1_000_000_000,
    }
}

fn finer_to_coarser_ratio(source_unit: ArrowTimeUnit, target_unit: ArrowTimeUnit) -> Option<i64> {
    let source_scale = time_unit_scale(source_unit);
    let target_scale = time_unit_scale(target_unit);
    (source_scale >= target_scale).then_some(source_scale / target_scale)
}

/// Returns the smallest source-unit timestamp whose downcast is greater than or equal to
/// `target_value`.
///
/// DataFusion timestamp downcasts truncate toward zero. For non-positive buckets that means the
/// bucket starts before `target_value * ratio`, so `<= x` can be rewritten as `< lower_bound(x+1)`
/// without dropping rows near zero or across negative boundaries.
fn lower_bound_for_ge(
    target_value: i64,
    source_unit: ArrowTimeUnit,
    target_unit: ArrowTimeUnit,
) -> Option<i64> {
    let ratio = finer_to_coarser_ratio(source_unit, target_unit)?;
    let base = target_value.checked_mul(ratio)?;
    if target_value <= 0 {
        base.checked_sub(ratio - 1)
    } else {
        Some(base)
    }
}

fn timestamp_scalar_value(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampSecond(Some(value), _)
        | ScalarValue::TimestampMillisecond(Some(value), _)
        | ScalarValue::TimestampMicrosecond(Some(value), _)
        | ScalarValue::TimestampNanosecond(Some(value), _) => Some(*value),
        _ => None,
    }
}

fn timestamp_scalar(unit: ArrowTimeUnit, timezone: Option<Arc<str>>, value: i64) -> ScalarValue {
    match unit {
        ArrowTimeUnit::Second => ScalarValue::TimestampSecond(Some(value), timezone),
        ArrowTimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), timezone),
        ArrowTimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), timezone),
        ArrowTimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), timezone),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};
    use async_trait::async_trait;
    use common_time::Timestamp;
    use common_time::range::TimestampRange;
    use common_time::timestamp::TimeUnit;
    use datafusion::catalog::Session;
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::{TableProvider, provider_as_source};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_common::arrow::datatypes::Field;
    use datafusion_common::{DFSchema, ScalarValue, ToDFSchema};
    use datafusion_expr::expr::{Between, Like};
    use datafusion_expr::expr_fn::{cast, col, try_cast};
    use datafusion_expr::{
        Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableScan, TableSource,
        TableType, lit,
    };
    use datafusion_optimizer::analyzer::AnalyzerRule;
    use datafusion_optimizer::optimizer::{Optimizer, OptimizerContext};
    use datafusion_optimizer::push_down_filter::PushDownFilter;
    use table::predicate::build_time_range_predicate;

    use super::{
        ConstNormalizationRule, PatternMatchKind, cast_literal_losslessly, lower_bound_for_ge,
    };

    #[test]
    fn test_normalize_direct_integer_cast_comparison() {
        assert_filter_plan(
            vec![Field::new("v", DataType::Int32, false)],
            cast(col("v"), DataType::Int64).gt_eq(lit(42_i64)),
            "Filter: t.v >= Int32(42)\n  TableScan: t",
        );
    }

    #[test]
    fn test_normalize_non_column_operand() {
        assert_filter_plan(
            vec![Field::new("v", DataType::Int32, false)],
            cast(col("v") + lit(1_i32), DataType::Int64).gt_eq(lit(42_i64)),
            "Filter: t.v + Int32(1) >= Int32(42)\n  TableScan: t",
        );
    }

    #[test]
    fn test_normalize_swapped_binary_comparison() {
        assert_filter_plan(
            vec![Field::new("v", DataType::Int16, false)],
            lit(42_i64).lt_eq(cast(col("v"), DataType::Int64)),
            "Filter: t.v >= Int16(42)\n  TableScan: t",
        );
    }

    #[test]
    fn test_normalize_try_cast_target() {
        assert_filter_plan(
            vec![Field::new("v", DataType::Int16, false)],
            try_cast(col("v"), DataType::Int64).gt_eq(lit(42_i64)),
            "Filter: t.v >= Int16(42)\n  TableScan: t",
        );
    }

    #[test]
    fn test_normalize_casted_constants() {
        let fields = vec![Field::new("v", DataType::Int16, false)];
        let cases = [
            (
                col("v").gt_eq(cast(lit(42_i8), DataType::Int64)),
                "Filter: t.v >= Int16(42)\n  TableScan: t",
            ),
            (
                col("v").in_list(
                    vec![
                        cast(lit(1_i8), DataType::Int64),
                        try_cast(lit(2_i8), DataType::Int64),
                    ],
                    false,
                ),
                "Filter: t.v IN ([Int16(1), Int16(2)])\n  TableScan: t",
            ),
        ];

        for (predicate, expected) in cases {
            assert_filter_plan(fields.clone(), predicate, expected);
        }
    }

    #[test]
    fn test_normalize_plain_integer_literals() {
        let fields = vec![Field::new("v", DataType::Int16, false)];
        let cases = [
            (
                col("v").gt_eq(lit(42_i64)),
                "Filter: t.v >= Int16(42)\n  TableScan: t",
            ),
            (
                col("v").in_list(vec![lit(1_i64), lit(2_i64)], false),
                "Filter: t.v IN ([Int16(1), Int16(2)])\n  TableScan: t",
            ),
            (
                col("v").between(lit(3_i64), lit(5_i64)),
                "Filter: t.v BETWEEN Int16(3) AND Int16(5)\n  TableScan: t",
            ),
        ];

        for (predicate, expected) in cases {
            assert_filter_plan(fields.clone(), predicate, expected);
        }
    }

    #[test]
    fn test_normalize_unsigned_to_signed_literals() {
        let cases = [
            (
                vec![Field::new("v", DataType::UInt8, false)],
                cast(col("v"), DataType::Int16).lt_eq(lit(255_i16)),
                "Filter: t.v <= UInt8(255)\n  TableScan: t",
            ),
            (
                vec![Field::new("v", DataType::UInt16, false)],
                cast(col("v"), DataType::Int32).gt_eq(lit(42_i32)),
                "Filter: t.v >= UInt16(42)\n  TableScan: t",
            ),
            (
                vec![Field::new("v", DataType::UInt32, false)],
                cast(col("v"), DataType::Int64).between(lit(3_i64), lit(5_i64)),
                "Filter: t.v BETWEEN UInt32(3) AND UInt32(5)\n  TableScan: t",
            ),
        ];

        for (fields, predicate, expected) in cases {
            assert_filter_plan(fields, predicate, expected);
        }
    }

    #[test]
    fn test_normalize_in_list_and_between() {
        let fields = vec![Field::new("v", DataType::Int16, false)];
        let cases = [
            (
                cast(col("v"), DataType::Int64).in_list(vec![lit(1_i64), lit(2_i64)], false),
                "Filter: t.v IN ([Int16(1), Int16(2)])\n  TableScan: t",
            ),
            (
                cast(col("v"), DataType::Int64).between(lit(3_i64), lit(5_i64)),
                "Filter: t.v BETWEEN Int16(3) AND Int16(5)\n  TableScan: t",
            ),
        ];

        for (predicate, expected) in cases {
            assert_filter_plan(fields.clone(), predicate, expected);
        }
    }

    #[test]
    fn test_keep_non_lossless_literal_unchanged() {
        assert_filter_plan(
            vec![Field::new("v", DataType::Int16, false)],
            col("v").gt_eq(lit(100_000_i64)),
            "Filter: t.v >= Int64(100000)\n  TableScan: t",
        );
    }

    #[test]
    fn test_normalize_scan_filters() {
        let scan = build_scan_plan(test_schema(vec![Field::new("v", DataType::Int16, false)]));
        let LogicalPlan::TableScan(scan) = scan else {
            panic!("expected table scan");
        };
        let plan = LogicalPlan::TableScan(TableScan {
            filters: vec![cast(col("v"), DataType::Int64).gt_eq(lit(42_i64))],
            ..scan
        });

        let analyzed = analyze_plan(plan);

        assert_eq!(
            vec![col("v").gt_eq(lit(42_i16))],
            extract_scan_filters(&analyzed)
        );
    }

    #[test]
    fn test_normalize_negated_between() {
        assert_filter_plan(
            vec![Field::new("v", DataType::Int16, false)],
            Expr::Between(Between {
                expr: Box::new(cast(col("v"), DataType::Int64)),
                negated: true,
                low: Box::new(lit(3_i64)),
                high: Box::new(lit(5_i64)),
            }),
            "Filter: t.v NOT BETWEEN Int16(3) AND Int16(5)\n  TableScan: t",
        );
    }

    #[test]
    fn test_normalize_like_literal() {
        assert_pattern_match_plan(
            PatternMatchKind::Like,
            ScalarValue::LargeUtf8(Some("api%".to_string())),
            "Filter: t.s LIKE Utf8(\"api%\")\n  TableScan: t",
        );
    }

    #[test]
    fn test_normalize_similar_to_literal() {
        assert_pattern_match_plan(
            PatternMatchKind::SimilarTo,
            ScalarValue::LargeUtf8(Some("api.*".to_string())),
            "Filter: t.s SIMILAR TO Utf8(\"api.*\")\n  TableScan: t",
        );
    }

    #[test]
    fn test_normalize_direct_timestamp_filter() {
        assert_timestamp_pushdown(
            vec![
                Field::new(
                    "ts",
                    DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("tag", DataType::Utf8, true),
            ],
            ts_cast_to_ms()
                .gt_eq(ts_ms_literal(-299_999))
                .and(ts_cast_to_ms().lt_eq(ts_ms_literal(10_000)))
                .and(col("tag").eq(lit("api"))),
            "Filter: t.ts >= TimestampNanosecond(-299999999999, None) AND t.ts < TimestampNanosecond(10001000000, None) AND t.tag = Utf8(\"api\")\n  TableScan: t",
            "TableScan: t, full_filters=[t.ts >= TimestampNanosecond(-299999999999, None), t.ts < TimestampNanosecond(10001000000, None), t.tag = Utf8(\"api\")]",
            TimestampRange::new_inclusive(
                Some(Timestamp::new_nanosecond(-299_999_999_999)),
                Some(Timestamp::new_nanosecond(10_000_999_999)),
            ),
        );
    }

    #[test]
    fn test_normalize_timestamp_between_filter() {
        assert_timestamp_pushdown(
            vec![Field::new(
                "ts",
                DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
                false,
            )],
            ts_cast_to_ms().between(ts_ms_literal(-299_999), ts_ms_literal(10_000)),
            "Filter: t.ts >= TimestampNanosecond(-299999999999, None) AND t.ts < TimestampNanosecond(10001000000, None)\n  TableScan: t",
            "TableScan: t, full_filters=[t.ts >= TimestampNanosecond(-299999999999, None), t.ts < TimestampNanosecond(10001000000, None)]",
            TimestampRange::new_inclusive(
                Some(Timestamp::new_nanosecond(-299_999_999_999)),
                Some(Timestamp::new_nanosecond(10_000_999_999)),
            ),
        );
    }

    #[test]
    fn test_normalize_strict_timestamp_filter() {
        assert_timestamp_pushdown(
            vec![Field::new(
                "ts",
                DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
                false,
            )],
            ts_cast_to_ms()
                .gt(ts_ms_literal(10_000))
                .and(ts_cast_to_ms().lt(ts_ms_literal(20_000))),
            "Filter: t.ts >= TimestampNanosecond(10001000000, None) AND t.ts < TimestampNanosecond(20000000000, None)\n  TableScan: t",
            "TableScan: t, full_filters=[t.ts >= TimestampNanosecond(10001000000, None), t.ts < TimestampNanosecond(20000000000, None)]",
            TimestampRange::new_inclusive(
                Some(Timestamp::new_nanosecond(10_001_000_000)),
                Some(Timestamp::new_nanosecond(19_999_999_999)),
            ),
        );
    }

    #[test]
    fn test_normalize_zero_boundary_timestamp_filter() {
        let fields = vec![Field::new(
            "ts",
            DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
            false,
        )];

        assert_timestamp_pushdown(
            fields.clone(),
            ts_cast_to_ms().gt_eq(ts_ms_literal(0)),
            "Filter: t.ts >= TimestampNanosecond(-999999, None)\n  TableScan: t",
            "TableScan: t, full_filters=[t.ts >= TimestampNanosecond(-999999, None)]",
            TimestampRange::from_start(Timestamp::new_nanosecond(-999_999)),
        );

        assert_timestamp_pushdown(
            fields.clone(),
            ts_cast_to_ms().lt(ts_ms_literal(0)),
            "Filter: t.ts < TimestampNanosecond(-999999, None)\n  TableScan: t",
            "TableScan: t, full_filters=[t.ts < TimestampNanosecond(-999999, None)]",
            TimestampRange::until_end(Timestamp::new_nanosecond(-999_999), false),
        );

        assert_timestamp_pushdown(
            fields,
            ts_cast_to_ms().between(ts_ms_literal(0), ts_ms_literal(0)),
            "Filter: t.ts >= TimestampNanosecond(-999999, None) AND t.ts < TimestampNanosecond(1000000, None)\n  TableScan: t",
            "TableScan: t, full_filters=[t.ts >= TimestampNanosecond(-999999, None), t.ts < TimestampNanosecond(1000000, None)]",
            TimestampRange::new_inclusive(
                Some(Timestamp::new_nanosecond(-999_999)),
                Some(Timestamp::new_nanosecond(999_999)),
            ),
        );
    }

    #[test]
    fn test_timestamp_downcast_contract_matches_datafusion_casts() {
        let cases = [
            (-1_000_001, -1),
            (-1_000_000, -1),
            (-999_999, 0),
            (-1, 0),
            (0, 0),
            (999_999, 0),
            (1_000_000, 1),
        ];

        for (source, expected) in cases {
            let casted = cast_literal_losslessly(
                &ScalarValue::TimestampNanosecond(Some(source), None),
                &DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            )
            .unwrap();
            assert_eq!(
                ScalarValue::TimestampMillisecond(Some(expected), None),
                casted
            );
        }

        assert_eq!(
            Some(-1_999_999),
            lower_bound_for_ge(-1, ArrowTimeUnit::Nanosecond, ArrowTimeUnit::Millisecond)
        );
        assert_eq!(
            Some(-999_999),
            lower_bound_for_ge(0, ArrowTimeUnit::Nanosecond, ArrowTimeUnit::Millisecond)
        );
        assert_eq!(
            Some(1_000_000),
            lower_bound_for_ge(1, ArrowTimeUnit::Nanosecond, ArrowTimeUnit::Millisecond)
        );
    }

    #[test]
    fn test_normalize_plain_timestamp_literals() {
        assert_timestamp_pushdown(
            vec![Field::new(
                "ts",
                DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
                false,
            )],
            col("ts")
                .gt_eq(ts_ms_literal(-299_999))
                .and(col("ts").lt_eq(ts_ms_literal(10_000))),
            "Filter: t.ts >= TimestampNanosecond(-299999000000, None) AND t.ts <= TimestampNanosecond(10000000000, None)\n  TableScan: t",
            "TableScan: t, full_filters=[t.ts >= TimestampNanosecond(-299999000000, None), t.ts <= TimestampNanosecond(10000000000, None)]",
            TimestampRange::new_inclusive(
                Some(Timestamp::new_nanosecond(-299_999_000_000)),
                Some(Timestamp::new_nanosecond(10_000_000_000)),
            ),
        );
    }

    fn assert_pattern_match_plan(kind: PatternMatchKind, pattern: ScalarValue, expected: &str) {
        let predicate = match kind {
            PatternMatchKind::Like => Expr::Like(Like::new(
                false,
                Box::new(cast(col("s"), DataType::LargeUtf8)),
                Box::new(lit(pattern)),
                None,
                false,
            )),
            PatternMatchKind::SimilarTo => Expr::SimilarTo(Like::new(
                false,
                Box::new(cast(col("s"), DataType::LargeUtf8)),
                Box::new(lit(pattern)),
                None,
                false,
            )),
        };

        assert_filter_plan(
            vec![Field::new("s", DataType::Utf8, false)],
            predicate,
            expected,
        );
    }

    fn assert_filter_plan(fields: Vec<Field>, predicate: Expr, expected: &str) {
        assert_eq!(expected, analyze_filter(fields, predicate).to_string());
    }

    fn assert_timestamp_pushdown(
        fields: Vec<Field>,
        predicate: Expr,
        expected_analyzed: &str,
        expected_pushed: &str,
        expected_range: TimestampRange,
    ) {
        let analyzed = analyze_filter(fields, predicate);
        assert_eq!(expected_analyzed, analyzed.to_string());

        let pushed = push_down_filters(analyzed);
        assert_eq!(expected_pushed, pushed.to_string());

        let range =
            build_time_range_predicate("ts", TimeUnit::Nanosecond, &extract_scan_filters(&pushed));
        assert_eq!(expected_range, range);
    }

    fn analyze_filter(fields: Vec<Field>, predicate: Expr) -> LogicalPlan {
        analyze_plan(build_filter_plan(test_schema(fields), predicate))
    }

    fn analyze_plan(plan: LogicalPlan) -> LogicalPlan {
        ConstNormalizationRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap()
    }

    fn build_filter_plan(schema: Arc<DFSchema>, predicate: Expr) -> LogicalPlan {
        LogicalPlanBuilder::scan("t", test_source(schema), None)
            .unwrap()
            .filter(predicate)
            .unwrap()
            .build()
            .unwrap()
    }

    fn build_scan_plan(schema: Arc<DFSchema>) -> LogicalPlan {
        LogicalPlanBuilder::scan("t", test_source(schema), None)
            .unwrap()
            .build()
            .unwrap()
    }

    fn push_down_filters(plan: LogicalPlan) -> LogicalPlan {
        Optimizer::with_rules(vec![Arc::new(PushDownFilter::new())])
            .optimize(plan, &OptimizerContext::new(), |_, _| {})
            .unwrap()
    }

    fn ts_cast_to_ms() -> Expr {
        cast(
            col("ts"),
            DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
        )
    }

    fn ts_ms_literal(value: i64) -> Expr {
        lit(ScalarValue::TimestampMillisecond(Some(value), None))
    }

    fn extract_scan_filters(plan: &LogicalPlan) -> Vec<Expr> {
        match plan {
            LogicalPlan::TableScan(scan) => scan.filters.clone(),
            _ => plan
                .inputs()
                .into_iter()
                .flat_map(extract_scan_filters)
                .collect(),
        }
    }

    fn test_schema(fields: Vec<Field>) -> Arc<DFSchema> {
        arrow_schema::Schema::new(fields).to_dfschema_ref().unwrap()
    }

    fn test_source(schema: Arc<DFSchema>) -> Arc<dyn TableSource> {
        let table = ExactPushdownProvider {
            schema: Arc::new(schema.as_ref().as_arrow().clone()),
        };
        provider_as_source(Arc::new(table))
    }

    #[derive(Debug)]
    struct ExactPushdownProvider {
        schema: arrow_schema::SchemaRef,
    }

    #[async_trait]
    impl TableProvider for ExactPushdownProvider {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> arrow_schema::SchemaRef {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            unreachable!("scan should not be called in const_normalization tests")
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        }
    }
}
