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
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{DFSchemaRef, Result, ScalarValue};
use datafusion_expr::expr::{Cast, InList, TryCast};
use datafusion_expr::{
    Between, BinaryExpr, Expr, ExprSchemable, Filter, LogicalPlan, Operator, TableScan,
};
use datafusion_optimizer::analyzer::AnalyzerRule;

/// ConstNormalizationRule removes casts on column operands in filters when the
/// constant side can be normalized to the source column type ahead of filter
/// pushdown.
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
    expr.rewrite(&mut ConstNormalizer { schema })
        .map(|x| x.data)
}

struct ConstNormalizer {
    schema: DFSchemaRef,
}

impl TreeNodeRewriter for ConstNormalizer {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let original = expr.clone();
        let new_expr = match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                normalize_binary_expr(*left, op, *right, &self.schema)?
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => normalize_between_expr(*expr, negated, *low, *high, &self.schema)?,
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => normalize_in_list_expr(*expr, list, negated, &self.schema)?,
            expr => expr,
        };

        if new_expr != original {
            Ok(Transformed::yes(new_expr))
        } else {
            Ok(Transformed::no(new_expr))
        }
    }
}

fn normalize_binary_expr(
    left: Expr,
    op: Operator,
    right: Expr,
    schema: &DFSchemaRef,
) -> Result<Expr> {
    if let Some(expr) =
        normalize_binary_with_casted_column(left.clone(), op, right.clone(), schema)?
    {
        return Ok(expr);
    }

    if let Some(swapped_op) = op.swap()
        && let Some(expr) =
            normalize_binary_with_casted_column(right.clone(), swapped_op, left.clone(), schema)?
    {
        return Ok(expr);
    }

    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}

fn normalize_binary_with_casted_column(
    casted_column: Expr,
    op: Operator,
    constant: Expr,
    schema: &DFSchemaRef,
) -> Result<Option<Expr>> {
    let Some(casted_column) = extract_casted_column(&casted_column, schema)? else {
        return Ok(None);
    };
    let Some(constant) = extract_constant_scalar(&constant)? else {
        return Ok(None);
    };

    if let Some(expr) = normalize_lossless_binary(&casted_column, op, &constant) {
        return Ok(Some(expr));
    }

    Ok(normalize_timestamp_downcast_binary(
        &casted_column,
        op,
        &constant,
    ))
}

fn normalize_between_expr(
    expr: Expr,
    negated: bool,
    low: Expr,
    high: Expr,
    schema: &DFSchemaRef,
) -> Result<Expr> {
    if negated {
        return Ok(Expr::Between(Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        }));
    }

    let Some(casted_column) = extract_casted_column(&expr, schema)? else {
        return Ok(Expr::Between(Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        }));
    };
    let Some(low_value) = extract_constant_scalar(&low)? else {
        return Ok(Expr::Between(Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        }));
    };
    let Some(high_value) = extract_constant_scalar(&high)? else {
        return Ok(Expr::Between(Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        }));
    };

    if let (Some(low), Some(high)) = (
        normalize_lossless_literal(&casted_column, &low_value),
        normalize_lossless_literal(&casted_column, &high_value),
    ) {
        return Ok(Expr::Between(Between {
            expr: Box::new(casted_column.source_expr.clone()),
            negated,
            low: Box::new(Expr::Literal(low, None)),
            high: Box::new(Expr::Literal(high, None)),
        }));
    }

    if let Some(expr) =
        normalize_timestamp_downcast_between(&casted_column, &low_value, &high_value)
    {
        return Ok(expr);
    }

    Ok(Expr::Between(Between {
        expr: Box::new(expr),
        negated,
        low: Box::new(low),
        high: Box::new(high),
    }))
}

fn normalize_in_list_expr(
    expr: Expr,
    list: Vec<Expr>,
    negated: bool,
    schema: &DFSchemaRef,
) -> Result<Expr> {
    let Some(casted_column) = extract_casted_column(&expr, schema)? else {
        return Ok(Expr::InList(InList {
            expr: Box::new(expr),
            list,
            negated,
        }));
    };

    let mut new_list = Vec::with_capacity(list.len());
    for item in &list {
        let Some(value) = extract_constant_scalar(item)? else {
            return Ok(Expr::InList(InList {
                expr: Box::new(expr),
                list,
                negated,
            }));
        };
        let Some(normalized) = normalize_lossless_literal(&casted_column, &value) else {
            return Ok(Expr::InList(InList {
                expr: Box::new(expr),
                list,
                negated,
            }));
        };
        new_list.push(Expr::Literal(normalized, None));
    }

    Ok(Expr::InList(InList {
        expr: Box::new(casted_column.source_expr.clone()),
        list: new_list,
        negated,
    }))
}

#[derive(Clone)]
struct CastedColumn {
    source_expr: Expr,
    source_type: DataType,
    kind: CastKind,
}

#[derive(Clone)]
enum CastKind {
    Lossless,
    TimestampDowncast {
        source_unit: ArrowTimeUnit,
        target_unit: ArrowTimeUnit,
        timezone: Option<Arc<str>>,
    },
}

fn extract_casted_column(expr: &Expr, schema: &DFSchemaRef) -> Result<Option<CastedColumn>> {
    let (source_expr, target_type) = match expr {
        Expr::Cast(Cast { expr, data_type }) | Expr::TryCast(TryCast { expr, data_type }) => {
            (expr.as_ref(), data_type)
        }
        _ => return Ok(None),
    };

    if !matches!(source_expr, Expr::Column(_)) {
        return Ok(None);
    }

    let source_type = source_expr.get_type(schema)?;
    let Some(kind) = classify_cast_kind(&source_type, target_type) else {
        return Ok(None);
    };

    Ok(Some(CastedColumn {
        source_expr: source_expr.clone(),
        source_type,
        kind,
    }))
}

fn classify_cast_kind(source_type: &DataType, target_type: &DataType) -> Option<CastKind> {
    if is_lossless_column_cast(source_type, target_type) {
        return Some(CastKind::Lossless);
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

fn is_lossless_column_cast(source_type: &DataType, target_type: &DataType) -> bool {
    match (source_type, target_type) {
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (DataType::Int16, DataType::Int32 | DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::UInt8, DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt16, DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt32, DataType::UInt64) => true,
        (
            DataType::Timestamp(source_unit, source_tz),
            DataType::Timestamp(target_unit, target_tz),
        ) => source_tz == target_tz && time_unit_rank(*source_unit) <= time_unit_rank(*target_unit),
        _ => false,
    }
}

fn normalize_lossless_binary(
    casted_column: &CastedColumn,
    op: Operator,
    constant: &ScalarValue,
) -> Option<Expr> {
    let normalized = normalize_lossless_literal(casted_column, constant)?;
    Some(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(casted_column.source_expr.clone()),
        op,
        right: Box::new(Expr::Literal(normalized, None)),
    }))
}

fn normalize_lossless_literal(
    casted_column: &CastedColumn,
    constant: &ScalarValue,
) -> Option<ScalarValue> {
    matches!(casted_column.kind, CastKind::Lossless)
        .then_some(())
        .and_then(|_| cast_literal_losslessly(constant, &casted_column.source_type))
}

fn normalize_timestamp_downcast_binary(
    casted_column: &CastedColumn,
    op: Operator,
    constant: &ScalarValue,
) -> Option<Expr> {
    let CastKind::TimestampDowncast {
        source_unit,
        target_unit,
        timezone,
    } = &casted_column.kind
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
        left: Box::new(casted_column.source_expr.clone()),
        op: normalized_op,
        right: Box::new(Expr::Literal(
            timestamp_scalar(*source_unit, timezone.clone(), bound),
            None,
        )),
    }))
}

fn normalize_timestamp_downcast_between(
    casted_column: &CastedColumn,
    low: &ScalarValue,
    high: &ScalarValue,
) -> Option<Expr> {
    let CastKind::TimestampDowncast {
        source_unit,
        target_unit,
        timezone,
    } = &casted_column.kind
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
            left: Box::new(casted_column.source_expr.clone()),
            op: Operator::GtEq,
            right: Box::new(Expr::Literal(
                timestamp_scalar(*source_unit, timezone.clone(), lower),
                None,
            )),
        })
        .and(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(casted_column.source_expr.clone()),
            op: Operator::Lt,
            right: Box::new(Expr::Literal(
                timestamp_scalar(*source_unit, timezone.clone(), upper),
                None,
            )),
        })),
    )
}

fn extract_constant_scalar(expr: &Expr) -> Result<Option<ScalarValue>> {
    match expr {
        Expr::Literal(value, _) => Ok(Some(value.clone())),
        Expr::Cast(Cast { expr, data_type }) => extract_constant_scalar(expr)?
            .map(|value| value.cast_to(data_type))
            .transpose(),
        Expr::TryCast(TryCast { expr, data_type }) => {
            Ok(extract_constant_scalar(expr)?.and_then(|value| value.cast_to(data_type).ok()))
        }
        _ => Ok(None),
    }
}

fn cast_literal_losslessly(value: &ScalarValue, target_type: &DataType) -> Option<ScalarValue> {
    let casted = value.cast_to(target_type).ok()?;
    let round_trip = casted.cast_to(&value.data_type()).ok()?;
    (round_trip == *value).then_some(casted)
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
