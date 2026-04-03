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
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchemaRef, Result, ScalarValue};
use datafusion_expr::expr::{Cast, InList, Like, TryCast};
use datafusion_expr::{
    Between, BinaryExpr, Expr, ExprSchemable, Filter, LogicalPlan, Operator, TableScan, lit,
};
use datafusion_expr_common::casts::try_cast_literal_to_type;
use datafusion_optimizer::analyzer::AnalyzerRule;

/// ConstNormalizationRule rewrites castable constants against their
/// non-constant comparison operand ahead of filter pushdown.
#[derive(Debug)]
pub struct ConstNormalizationRule;

impl AnalyzerRule for ConstNormalizationRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform(|plan| match plan {
            LogicalPlan::Filter(filter) => normalize_filter_plan(filter),
            LogicalPlan::TableScan(scan) => normalize_scan_plan(scan),
            _ => Ok(Transformed::no(plan)),
        })
        .map(|x| x.data)
    }

    fn name(&self) -> &str {
        "ConstNormalizationRule"
    }
}

fn normalize_filter_plan(filter: Filter) -> Result<Transformed<LogicalPlan>> {
    let predicate = normalize_filter_expr(filter.predicate.clone(), filter.input.schema().clone())?;
    if predicate != filter.predicate {
        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
            predicate,
            filter.input,
        )?)))
    } else {
        Ok(Transformed::no(LogicalPlan::Filter(filter)))
    }
}

fn normalize_scan_plan(scan: TableScan) -> Result<Transformed<LogicalPlan>> {
    if scan.filters.is_empty() {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    }

    let filters = scan
        .filters
        .iter()
        .map(|expr| normalize_filter_expr(expr.clone(), scan.projected_schema.clone()))
        .collect::<Result<Vec<_>>>()?;

    if filters != scan.filters {
        Ok(Transformed::yes(LogicalPlan::TableScan(TableScan {
            table_name: scan.table_name,
            source: scan.source,
            projection: scan.projection,
            projected_schema: scan.projected_schema,
            filters,
            fetch: scan.fetch,
        })))
    } else {
        Ok(Transformed::no(LogicalPlan::TableScan(scan)))
    }
}

fn normalize_filter_expr(expr: Expr, schema: DFSchemaRef) -> Result<Expr> {
    expr.transform_up(|expr| normalize_expr_node(expr, &schema))
        .map(|x| x.data)
}

fn normalize_expr_node(expr: Expr, schema: &DFSchemaRef) -> Result<Transformed<Expr>> {
    match expr {
        Expr::BinaryExpr(binary) => normalize_binary_expr(binary, schema),
        Expr::Between(between) => normalize_between_expr(between, schema),
        Expr::InList(in_list) => normalize_in_list_expr(in_list, schema),
        Expr::Like(like) => normalize_like_expr(like, false, schema),
        Expr::SimilarTo(like) => normalize_like_expr(like, true, schema),
        expr => Ok(Transformed::no(expr)),
    }
}

fn normalize_binary_expr(
    BinaryExpr { left, op, right }: BinaryExpr,
    schema: &DFSchemaRef,
) -> Result<Transformed<Expr>> {
    let left = *left;
    let right = *right;

    if op.supports_propagation() {
        if let Some(expr) = normalize_binary_side(left.clone(), op, right.clone(), schema)? {
            return Ok(Transformed::yes(expr));
        }

        if let Some(swapped_op) = op.swap()
            && let Some(expr) =
                normalize_binary_side(right.clone(), swapped_op, left.clone(), schema)?
        {
            return Ok(Transformed::yes(expr));
        }
    }

    Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })))
}

fn normalize_binary_side(
    target_expr: Expr,
    op: Operator,
    constant_expr: Expr,
    schema: &DFSchemaRef,
) -> Result<Option<Expr>> {
    let Some((target, constants)) =
        extract_target_and_constants(&target_expr, std::slice::from_ref(&constant_expr), schema)?
    else {
        return Ok(None);
    };

    if let Some(expr) = normalize_constant_operands(&target, &constants, |target, mut constants| {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(target),
            op,
            right: Box::new(
                constants
                    .pop()
                    .expect("binary normalization expects one constant"),
            ),
        })
    }) {
        return Ok(Some(expr));
    }

    Ok(normalize_timestamp_downcast_binary(
        &target,
        op,
        &constants[0],
    ))
}

fn normalize_between_expr(
    Between {
        expr,
        negated,
        low,
        high,
    }: Between,
    schema: &DFSchemaRef,
) -> Result<Transformed<Expr>> {
    let expr = *expr;
    let low = *low;
    let high = *high;

    if let Some((target, constants)) =
        extract_target_and_constants(&expr, &[low.clone(), high.clone()], schema)?
    {
        if let Some(expr) =
            normalize_constant_operands(&target, &constants, |target, mut constants| {
                let high = constants
                    .pop()
                    .expect("between normalization expects high constant");
                let low = constants
                    .pop()
                    .expect("between normalization expects low constant");
                Expr::Between(Between {
                    expr: Box::new(target),
                    negated,
                    low: Box::new(low),
                    high: Box::new(high),
                })
            })
        {
            return Ok(Transformed::yes(expr));
        }

        if !negated
            && let Some(expr) =
                normalize_timestamp_downcast_between(&target, &constants[0], &constants[1])
        {
            return Ok(Transformed::yes(expr));
        }
    }

    Ok(Transformed::no(Expr::Between(Between {
        expr: Box::new(expr),
        negated,
        low: Box::new(low),
        high: Box::new(high),
    })))
}

fn normalize_in_list_expr(
    InList {
        expr,
        list,
        negated,
    }: InList,
    schema: &DFSchemaRef,
) -> Result<Transformed<Expr>> {
    let expr = *expr;

    let Some((target, constants)) = extract_target_and_constants(&expr, &list, schema)? else {
        return Ok(Transformed::no(Expr::InList(InList {
            expr: Box::new(expr),
            list,
            negated,
        })));
    };

    let Some(expr) = normalize_constant_operands(&target, &constants, |target, constants| {
        Expr::InList(InList {
            expr: Box::new(target),
            list: constants,
            negated,
        })
    }) else {
        return Ok(Transformed::no(Expr::InList(InList {
            expr: Box::new(expr),
            list,
            negated,
        })));
    };

    Ok(Transformed::yes(expr))
}

fn normalize_like_expr(
    Like {
        negated,
        expr,
        pattern,
        escape_char,
        case_insensitive,
    }: Like,
    similar_to: bool,
    schema: &DFSchemaRef,
) -> Result<Transformed<Expr>> {
    let expr = *expr;
    let pattern = *pattern;

    let Some((target, constants)) =
        extract_target_and_constants(&expr, std::slice::from_ref(&pattern), schema)?
    else {
        return Ok(Transformed::no(rebuild_like_expr(
            expr,
            pattern,
            negated,
            escape_char,
            case_insensitive,
            similar_to,
        )));
    };

    let Some(expr) = normalize_constant_operands(&target, &constants, |target, mut constants| {
        rebuild_like_expr(
            target,
            constants
                .pop()
                .expect("like normalization expects one constant"),
            negated,
            escape_char,
            case_insensitive,
            similar_to,
        )
    }) else {
        return Ok(Transformed::no(rebuild_like_expr(
            expr,
            pattern,
            negated,
            escape_char,
            case_insensitive,
            similar_to,
        )));
    };

    Ok(Transformed::yes(expr))
}

fn rebuild_like_expr(
    expr: Expr,
    pattern: Expr,
    negated: bool,
    escape_char: Option<char>,
    case_insensitive: bool,
    similar_to: bool,
) -> Expr {
    let like = Like::new(
        negated,
        Box::new(expr),
        Box::new(pattern),
        escape_char,
        case_insensitive,
    );
    if similar_to {
        Expr::SimilarTo(like)
    } else {
        Expr::Like(like)
    }
}

fn extract_target_and_constants(
    target_expr: &Expr,
    constant_exprs: &[Expr],
    schema: &DFSchemaRef,
) -> Result<Option<(NormalizationTarget, Vec<ScalarValue>)>> {
    let Some(target) = extract_normalization_target(target_expr, schema)? else {
        return Ok(None);
    };
    let Some(constants) = extract_constant_scalars(constant_exprs)? else {
        return Ok(None);
    };
    Ok(Some((target, constants)))
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

fn normalize_constant_operands<F>(
    target: &NormalizationTarget,
    constants: &[ScalarValue],
    rebuild: F,
) -> Option<Expr>
where
    F: FnOnce(Expr, Vec<Expr>) -> Expr,
{
    let normalized = normalize_constants_losslessly(target, constants)?;
    Some(rebuild(
        target.normalized_expr.clone(),
        normalized.into_iter().map(lit).collect(),
    ))
}

fn normalize_constants_losslessly(
    target: &NormalizationTarget,
    constants: &[ScalarValue],
) -> Option<Vec<ScalarValue>> {
    constants
        .iter()
        .map(|constant| normalize_lossless_literal(target, constant))
        .collect()
}

#[derive(Clone)]
struct NormalizationTarget {
    normalized_expr: Expr,
    normalized_type: DataType,
    kind: CastKind,
}

#[derive(Clone)]
enum CastKind {
    Direct,
    RemovableCast,
    TimestampDowncast {
        source_unit: ArrowTimeUnit,
        target_unit: ArrowTimeUnit,
        timezone: Option<Arc<str>>,
    },
}

fn extract_normalization_target(
    expr: &Expr,
    schema: &DFSchemaRef,
) -> Result<Option<NormalizationTarget>> {
    if extract_constant_scalar(expr)?.is_some() {
        return Ok(None);
    }

    if extract_cast_input(expr).is_none() {
        return Ok(Some(NormalizationTarget {
            normalized_expr: expr.clone(),
            normalized_type: expr.get_type(schema)?,
            kind: CastKind::Direct,
        }));
    }

    let Some((_, source_expr, target_type)) = extract_cast_input(expr) else {
        return Ok(None);
    };

    if extract_constant_scalar(source_expr)?.is_some() {
        return Ok(None);
    }

    let normalized_type = source_expr.get_type(schema)?;
    let Some(kind) = classify_cast_kind(&normalized_type, target_type) else {
        return Ok(None);
    };

    Ok(Some(NormalizationTarget {
        normalized_expr: source_expr.clone(),
        normalized_type,
        kind,
    }))
}

fn classify_cast_kind(source_type: &DataType, target_type: &DataType) -> Option<CastKind> {
    if is_lossless_cast(source_type, target_type) {
        return Some(CastKind::RemovableCast);
    }

    match (source_type, target_type) {
        (
            DataType::Timestamp(source_unit, source_tz),
            DataType::Timestamp(target_unit, target_tz),
        ) if source_tz == target_tz
            && time_unit_rank(*source_unit) > time_unit_rank(*target_unit) =>
        {
            Some(CastKind::TimestampDowncast {
                source_unit: *source_unit,
                target_unit: *target_unit,
                timezone: source_tz.clone(),
            })
        }
        _ => None,
    }
}

fn is_lossless_cast(source_type: &DataType, target_type: &DataType) -> bool {
    match (source_type, target_type) {
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (DataType::Int16, DataType::Int32 | DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::UInt8, DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt16, DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt32, DataType::UInt64)
        | (DataType::Utf8, DataType::Utf8View | DataType::LargeUtf8) => true,
        (
            DataType::Timestamp(source_unit, source_tz),
            DataType::Timestamp(target_unit, target_tz),
        ) => source_tz == target_tz && time_unit_rank(*source_unit) <= time_unit_rank(*target_unit),
        _ => false,
    }
}

fn normalize_lossless_literal(
    target: &NormalizationTarget,
    constant: &ScalarValue,
) -> Option<ScalarValue> {
    matches!(target.kind, CastKind::Direct | CastKind::RemovableCast)
        .then_some(())
        .and_then(|_| cast_literal_losslessly(constant, &target.normalized_type))
}

fn normalize_timestamp_downcast_binary(
    target: &NormalizationTarget,
    op: Operator,
    constant: &ScalarValue,
) -> Option<Expr> {
    let CastKind::TimestampDowncast {
        source_unit,
        target_unit,
        timezone,
    } = &target.kind
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
        Operator::LtEq => lower_bound_for_ge(value.checked_add(1)?, *source_unit, *target_unit)?,
        _ => return None,
    };

    let normalized_op = match op {
        Operator::GtEq | Operator::Gt => Operator::GtEq,
        Operator::Lt | Operator::LtEq => Operator::Lt,
        _ => return None,
    };

    Some(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(target.normalized_expr.clone()),
        op: normalized_op,
        right: Box::new(lit(timestamp_scalar(*source_unit, timezone.clone(), bound))),
    }))
}

fn normalize_timestamp_downcast_between(
    target: &NormalizationTarget,
    low: &ScalarValue,
    high: &ScalarValue,
) -> Option<Expr> {
    let CastKind::TimestampDowncast {
        source_unit,
        target_unit,
        timezone,
    } = &target.kind
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
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(target.normalized_expr.clone()),
            op: Operator::GtEq,
            right: Box::new(lit(timestamp_scalar(*source_unit, timezone.clone(), lower))),
        })
        .and(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(target.normalized_expr.clone()),
            op: Operator::Lt,
            right: Box::new(lit(timestamp_scalar(*source_unit, timezone.clone(), upper))),
        })),
    )
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
        .or_else(|| cast_literal_by_round_trip(value, target_type))
}

fn cast_literal_by_round_trip(value: &ScalarValue, target_type: &DataType) -> Option<ScalarValue> {
    let casted = value.cast_to(target_type).ok()?;
    let round_trip = casted.cast_to(&value.data_type()).ok()?;
    (round_trip == *value).then_some(casted)
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

fn lower_bound_for_ge(
    target_value: i64,
    source_unit: ArrowTimeUnit,
    target_unit: ArrowTimeUnit,
) -> Option<i64> {
    let ratio = finer_to_coarser_ratio(source_unit, target_unit)?;
    let base = target_value.checked_mul(ratio)?;
    if target_value < 0 {
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
    use datafusion_expr::expr_fn::{cast, col};
    use datafusion_expr::{
        Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableSource, TableType,
        lit,
    };
    use datafusion_optimizer::analyzer::AnalyzerRule;
    use datafusion_optimizer::optimizer::{Optimizer, OptimizerContext};
    use datafusion_optimizer::push_down_filter::PushDownFilter;
    use table::predicate::build_time_range_predicate;

    use super::ConstNormalizationRule;

    #[test]
    fn test_normalize_direct_integer_cast_comparison() {
        let schema = test_schema(vec![Field::new("v", DataType::Int32, false)]);
        let plan = LogicalPlanBuilder::scan("t", test_source(schema.clone()), None)
            .unwrap()
            .filter(cast(col("v"), DataType::Int64).gt_eq(lit(42_i64)))
            .unwrap()
            .build()
            .unwrap();

        let result = ConstNormalizationRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();

        assert_eq!(
            "Filter: t.v >= Int32(42)\n  TableScan: t",
            result.to_string()
        );
    }

    #[test]
    fn test_normalize_non_column_operand() {
        let schema = test_schema(vec![Field::new("v", DataType::Int32, false)]);
        let plan = LogicalPlanBuilder::scan("t", test_source(schema), None)
            .unwrap()
            .filter(cast(col("v") + lit(1_i32), DataType::Int64).gt_eq(lit(42_i64)))
            .unwrap()
            .build()
            .unwrap();

        let result = ConstNormalizationRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();

        assert_eq!(
            "Filter: t.v + Int32(1) >= Int32(42)\n  TableScan: t",
            result.to_string()
        );
    }

    #[test]
    fn test_normalize_plain_integer_literals() {
        let schema = test_schema(vec![Field::new("v", DataType::Int16, false)]);
        let comparison_plan = LogicalPlanBuilder::scan("t", test_source(schema.clone()), None)
            .unwrap()
            .filter(col("v").gt_eq(lit(42_i64)))
            .unwrap()
            .build()
            .unwrap();

        let in_list_plan = LogicalPlanBuilder::scan("t", test_source(schema.clone()), None)
            .unwrap()
            .filter(col("v").in_list(vec![lit(1_i64), lit(2_i64)], false))
            .unwrap()
            .build()
            .unwrap();

        let between_plan = LogicalPlanBuilder::scan("t", test_source(schema), None)
            .unwrap()
            .filter(col("v").between(lit(3_i64), lit(5_i64)))
            .unwrap()
            .build()
            .unwrap();

        let comparison = ConstNormalizationRule
            .analyze(comparison_plan, &ConfigOptions::default())
            .unwrap();
        let in_list = ConstNormalizationRule
            .analyze(in_list_plan, &ConfigOptions::default())
            .unwrap();
        let between = ConstNormalizationRule
            .analyze(between_plan, &ConfigOptions::default())
            .unwrap();

        assert_eq!(
            "Filter: t.v >= Int16(42)\n  TableScan: t",
            comparison.to_string()
        );
        assert_eq!(
            "Filter: t.v IN ([Int16(1), Int16(2)])\n  TableScan: t",
            in_list.to_string()
        );
        assert_eq!(
            "Filter: t.v BETWEEN Int16(3) AND Int16(5)\n  TableScan: t",
            between.to_string()
        );
    }

    #[test]
    fn test_normalize_in_list_and_between() {
        let schema = test_schema(vec![Field::new("v", DataType::Int16, false)]);
        let in_list_plan = LogicalPlanBuilder::scan("t", test_source(schema.clone()), None)
            .unwrap()
            .filter(cast(col("v"), DataType::Int64).in_list(vec![lit(1_i64), lit(2_i64)], false))
            .unwrap()
            .build()
            .unwrap();

        let between_plan = LogicalPlanBuilder::scan("t", test_source(schema), None)
            .unwrap()
            .filter(cast(col("v"), DataType::Int64).between(lit(3_i64), lit(5_i64)))
            .unwrap()
            .build()
            .unwrap();

        let in_list = ConstNormalizationRule
            .analyze(in_list_plan, &ConfigOptions::default())
            .unwrap();
        let between = ConstNormalizationRule
            .analyze(between_plan, &ConfigOptions::default())
            .unwrap();

        assert_eq!(
            "Filter: t.v IN ([Int16(1), Int16(2)])\n  TableScan: t",
            in_list.to_string()
        );
        assert_eq!(
            "Filter: t.v BETWEEN Int16(3) AND Int16(5)\n  TableScan: t",
            between.to_string()
        );
    }

    #[test]
    fn test_normalize_negated_between() {
        let schema = test_schema(vec![Field::new("v", DataType::Int16, false)]);
        let plan = LogicalPlanBuilder::scan("t", test_source(schema), None)
            .unwrap()
            .filter(Expr::Between(Between {
                expr: Box::new(cast(col("v"), DataType::Int64)),
                negated: true,
                low: Box::new(lit(3_i64)),
                high: Box::new(lit(5_i64)),
            }))
            .unwrap()
            .build()
            .unwrap();

        let result = ConstNormalizationRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();

        assert_eq!(
            "Filter: t.v NOT BETWEEN Int16(3) AND Int16(5)\n  TableScan: t",
            result.to_string()
        );
    }

    #[test]
    fn test_normalize_like_literal() {
        let schema = test_schema(vec![Field::new("s", DataType::Utf8, false)]);
        let plan = LogicalPlanBuilder::scan("t", test_source(schema), None)
            .unwrap()
            .filter(Expr::Like(Like::new(
                false,
                Box::new(cast(col("s"), DataType::LargeUtf8)),
                Box::new(lit(ScalarValue::LargeUtf8(Some("api%".to_string())))),
                None,
                false,
            )))
            .unwrap()
            .build()
            .unwrap();

        let result = ConstNormalizationRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();

        assert_eq!(
            "Filter: t.s LIKE Utf8(\"api%\")\n  TableScan: t",
            result.to_string()
        );
    }

    #[test]
    fn test_normalize_direct_timestamp_filter() {
        let schema = test_schema(vec![
            Field::new(
                "ts",
                DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("tag", DataType::Utf8, true),
        ]);
        let plan = LogicalPlanBuilder::scan("t", test_source(schema), None)
            .unwrap()
            .filter(
                cast(
                    col("ts"),
                    DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                )
                .gt_eq(lit(ScalarValue::TimestampMillisecond(Some(-299_999), None)))
                .and(
                    cast(
                        col("ts"),
                        DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                    )
                    .lt_eq(lit(ScalarValue::TimestampMillisecond(Some(10_000), None))),
                )
                .and(col("tag").eq(lit("api"))),
            )
            .unwrap()
            .build()
            .unwrap();

        let analyzed = ConstNormalizationRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();
        let expected = "\
\nFilter: t.ts >= TimestampNanosecond(-299999999999, None) AND t.ts < TimestampNanosecond(10001000000, None) AND t.tag = Utf8(\"api\")\
\n  TableScan: t";
        assert_eq!(expected.trim_start(), analyzed.to_string());

        let pushed = Optimizer::with_rules(vec![Arc::new(PushDownFilter::new())])
            .optimize(analyzed, &OptimizerContext::new(), |_, _| {})
            .unwrap();
        let expected = "\
\nTableScan: t, full_filters=[t.ts >= TimestampNanosecond(-299999999999, None), t.ts < TimestampNanosecond(10001000000, None), t.tag = Utf8(\"api\")]";
        assert_eq!(expected.trim_start(), pushed.to_string());

        let filters = extract_scan_filters(&pushed);
        let range = build_time_range_predicate("ts", TimeUnit::Nanosecond, &filters);
        assert_eq!(
            TimestampRange::new_inclusive(
                Some(Timestamp::new_nanosecond(-299_999_999_999)),
                Some(Timestamp::new_nanosecond(10_000_999_999))
            ),
            range
        );
    }

    #[test]
    fn test_normalize_plain_timestamp_literals() {
        let schema = test_schema(vec![Field::new(
            "ts",
            DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
            false,
        )]);
        let plan = LogicalPlanBuilder::scan("t", test_source(schema), None)
            .unwrap()
            .filter(
                col("ts")
                    .gt_eq(lit(ScalarValue::TimestampMillisecond(Some(-299_999), None)))
                    .and(
                        col("ts").lt_eq(lit(ScalarValue::TimestampMillisecond(Some(10_000), None))),
                    ),
            )
            .unwrap()
            .build()
            .unwrap();

        let analyzed = ConstNormalizationRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();
        let expected = "\
\nFilter: t.ts >= TimestampNanosecond(-299999000000, None) AND t.ts <= TimestampNanosecond(10000000000, None)\
\n  TableScan: t";
        assert_eq!(expected.trim_start(), analyzed.to_string());

        let pushed = Optimizer::with_rules(vec![Arc::new(PushDownFilter::new())])
            .optimize(analyzed, &OptimizerContext::new(), |_, _| {})
            .unwrap();
        let expected = "\
\nTableScan: t, full_filters=[t.ts >= TimestampNanosecond(-299999000000, None), t.ts <= TimestampNanosecond(10000000000, None)]";
        assert_eq!(expected.trim_start(), pushed.to_string());

        let filters = extract_scan_filters(&pushed);
        let range = build_time_range_predicate("ts", TimeUnit::Nanosecond, &filters);
        assert_eq!(
            TimestampRange::new_inclusive(
                Some(Timestamp::new_nanosecond(-299_999_000_000)),
                Some(Timestamp::new_nanosecond(10_000_000_000))
            ),
            range
        );
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
