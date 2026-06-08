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

use common_time::Timezone;
use common_time::timestamp::{TimeUnit, Timestamp};
use datafusion::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::InList;
use datafusion_expr::{
    Between, BinaryExpr, Expr, ExprSchemable, Filter, Literal, LogicalPlan, Operator, TableScan,
};
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
use session::context::QueryContextRef;

use crate::QueryEngineContext;
use crate::optimizer::ExtensionAnalyzerRule;
use crate::plan::ExtractExpr;

/// TypeConversionRule converts some literal values in logical plan to other types according
/// to data type of corresponding columns.
/// Specifically:
/// - string literal of timestamp is converted to `Expr::Literal(ScalarValue::TimestampMillis)`
/// - string literal of boolean is converted to `Expr::Literal(ScalarValue::Boolean)`
pub struct TypeConversionRule;

impl ExtensionAnalyzerRule for TypeConversionRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        ctx: &QueryEngineContext,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform(&|plan| match plan {
            LogicalPlan::Filter(filter) => {
                let mut converter = TypeConverter {
                    schema: filter.input.schema().clone(),
                    query_ctx: ctx.query_ctx(),
                };
                let rewritten = filter.predicate.clone().rewrite(&mut converter)?.data;
                Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                    rewritten,
                    filter.input,
                )?)))
            }
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                fetch,
            }) => {
                let mut converter = TypeConverter {
                    schema: projected_schema.clone(),
                    query_ctx: ctx.query_ctx(),
                };
                let rewrite_filters = filters
                    .into_iter()
                    .map(|e| e.rewrite(&mut converter).map(|x| x.data))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Transformed::yes(LogicalPlan::TableScan(TableScan {
                    table_name: table_name.clone(),
                    source: source.clone(),
                    projection,
                    projected_schema,
                    filters: rewrite_filters,
                    fetch,
                })))
            }
            LogicalPlan::Projection { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Analyze { .. } => {
                let mut converter = TypeConverter {
                    schema: plan.schema().clone(),
                    query_ctx: ctx.query_ctx(),
                };
                let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
                let expr = plan
                    .expressions_consider_join()
                    .into_iter()
                    .map(|e| e.rewrite(&mut converter).map(|x| x.data))
                    .collect::<Result<Vec<_>>>()?;

                plan.with_new_exprs(expr, inputs).map(Transformed::yes)
            }

            LogicalPlan::Distinct { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Subquery { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::SubqueryAlias { .. }
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Join { .. } => Ok(Transformed::no(plan)),
        })
        .map(|x| x.data)
    }
}

struct TypeConverter {
    query_ctx: QueryContextRef,
    schema: DFSchemaRef,
}

impl TypeConverter {
    fn column_type(&self, expr: &Expr) -> Option<DataType> {
        if let Expr::Column(_) = expr
            && let Ok(v) = expr.get_type(&self.schema)
        {
            return Some(v);
        }
        None
    }

    fn cast_scalar_value(
        &self,
        value: &ScalarValue,
        target_type: &DataType,
    ) -> Result<ScalarValue> {
        match (target_type, value) {
            (DataType::Timestamp(_, _), ScalarValue::Utf8(Some(v))) => {
                let parsed = string_to_timestamp_scalar(v, Some(&self.query_ctx.timezone()))?;
                self.cast_scalar_value(&parsed, target_type)
            }
            (DataType::Boolean, ScalarValue::Utf8(Some(v))) => match v.to_lowercase().as_str() {
                "true" => Ok(ScalarValue::Boolean(Some(true))),
                "false" => Ok(ScalarValue::Boolean(Some(false))),
                _ => Ok(ScalarValue::Boolean(None)),
            },
            (target_type, value) => {
                let value_arr = value.to_array()?;
                let arr = compute::cast(&value_arr, target_type)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                ScalarValue::try_from_array(
                    &arr,
                    0, // index: Converts a value in `array` at `index` into a ScalarValue
                )
            }
        }
    }

    fn convert_type<'b>(&self, left: &'b Expr, right: &'b Expr) -> Result<(Expr, Expr)> {
        let left_type = self.column_type(left);
        let right_type = self.column_type(right);

        let target_type = match (&left_type, &right_type) {
            (Some(v), None) => v,
            (None, Some(v)) => v,
            _ => return Ok((left.clone(), right.clone())),
        };

        // only try to convert timestamp or boolean types
        if !matches!(target_type, DataType::Timestamp(_, _) | DataType::Boolean) {
            return Ok((left.clone(), right.clone()));
        }

        match (left, right) {
            (Expr::Column(col), Expr::Literal(value, _)) => {
                let casted_right = self.cast_scalar_value(value, target_type)?;
                if casted_right.is_null() {
                    return Err(DataFusionError::Plan(format!(
                        "column:{col:?}. Casting value:{value:?} to {target_type:?} is invalid",
                    )));
                }
                Ok((left.clone(), Expr::Literal(casted_right, None)))
            }
            (Expr::Literal(value, _), Expr::Column(col)) => {
                let casted_left = self.cast_scalar_value(value, target_type)?;
                if casted_left.is_null() {
                    return Err(DataFusionError::Plan(format!(
                        "column:{col:?}. Casting value:{value:?} to {target_type:?} is invalid",
                    )));
                }
                Ok((Expr::Literal(casted_left, None), right.clone()))
            }
            _ => Ok((left.clone(), right.clone())),
        }
    }

    fn convert_comparison_expr(&self, left: Expr, op: Operator, right: Expr) -> Result<Expr> {
        if let Some(expr) = self.convert_timestamp_string_comparison(&left, op, &right)? {
            return Ok(expr);
        }

        let Some(swapped_op) = op.swap() else {
            let (left, right) = self.convert_type(&left, &right)?;
            return Ok(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }));
        };

        if let Some(expr) = self.convert_timestamp_string_comparison(&right, swapped_op, &left)? {
            return Ok(expr);
        }

        let (left, right) = self.convert_type(&left, &right)?;
        Ok(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }))
    }

    fn convert_timestamp_string_comparison(
        &self,
        column_expr: &Expr,
        op: Operator,
        literal_expr: &Expr,
    ) -> Result<Option<Expr>> {
        let Some(DataType::Timestamp(target_arrow_unit, target_timezone)) =
            self.column_type(column_expr)
        else {
            return Ok(None);
        };
        let Expr::Literal(ScalarValue::Utf8(Some(value)), _) = literal_expr else {
            return Ok(None);
        };

        let literal = Timestamp::from_str(value, Some(&self.query_ctx.timezone()))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let target_unit = TimeUnit::from(target_arrow_unit);

        if literal.unit().factor() >= target_unit.factor() {
            let casted = timestamp_to_scalar(literal, target_unit, target_timezone)?;
            return Ok(Some(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column_expr.clone()),
                op,
                right: Box::new(casted.lit()),
            })));
        }

        let floor = literal
            .convert_to(target_unit)
            .ok_or_else(|| DataFusionError::Plan("timestamp literal overflow".to_string()))?;
        let ceil = literal
            .convert_to_ceil(target_unit)
            .ok_or_else(|| DataFusionError::Plan("timestamp literal overflow".to_string()))?;
        let is_exact = floor == ceil;
        let literal =
            |value| timestamp_scalar(target_arrow_unit, target_timezone.clone(), value).lit();
        let expr = match op {
            Operator::Eq if is_exact => column_expr.clone().eq(literal(floor.value())),
            Operator::Eq => false.lit(),
            Operator::NotEq if is_exact => column_expr.clone().not_eq(literal(floor.value())),
            Operator::NotEq => true.lit(),
            Operator::Lt => {
                column_expr
                    .clone()
                    .lt_eq(literal(ceil.value().checked_sub(1).ok_or_else(|| {
                        DataFusionError::Plan("timestamp literal overflow".to_string())
                    })?))
            }
            Operator::LtEq => column_expr.clone().lt_eq(literal(floor.value())),
            Operator::Gt => {
                column_expr
                    .clone()
                    .gt_eq(literal(floor.value().checked_add(1).ok_or_else(|| {
                        DataFusionError::Plan("timestamp literal overflow".to_string())
                    })?))
            }
            Operator::GtEq => column_expr.clone().gt_eq(literal(ceil.value())),
            _ => return Ok(None),
        };

        Ok(Some(expr))
    }

    fn convert_timestamp_string_between(
        &self,
        expr: &Expr,
        negated: bool,
        low: &Expr,
        high: &Expr,
    ) -> Result<Option<Expr>> {
        let (low_op, high_op) = if negated {
            (Operator::Lt, Operator::Gt)
        } else {
            (Operator::GtEq, Operator::LtEq)
        };
        let Some(low) = self.convert_timestamp_string_comparison(expr, low_op, low)? else {
            return Ok(None);
        };
        let Some(high) = self.convert_timestamp_string_comparison(expr, high_op, high)? else {
            return Ok(None);
        };

        Ok(Some(if negated { low.or(high) } else { low.and(high) }))
    }

    fn convert_timestamp_string_in_list(
        &self,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
    ) -> Result<Option<Expr>> {
        let Some(DataType::Timestamp(target_arrow_unit, target_timezone)) = self.column_type(expr)
        else {
            return Ok(None);
        };
        let target_unit = TimeUnit::from(target_arrow_unit);
        let mut converted = Vec::with_capacity(list.len());

        for item in list {
            let Expr::Literal(ScalarValue::Utf8(Some(value)), _) = item else {
                return Ok(None);
            };
            let literal = Timestamp::from_str(value, Some(&self.query_ctx.timezone()))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            if literal.unit().factor() >= target_unit.factor() {
                converted.push(
                    timestamp_to_scalar(literal, target_unit, target_timezone.clone())?.lit(),
                );
                continue;
            }

            let floor = literal
                .convert_to(target_unit)
                .ok_or_else(|| DataFusionError::Plan("timestamp literal overflow".to_string()))?;
            let ceil = literal
                .convert_to_ceil(target_unit)
                .ok_or_else(|| DataFusionError::Plan("timestamp literal overflow".to_string()))?;
            if floor == ceil {
                converted.push(
                    timestamp_scalar(target_arrow_unit, target_timezone.clone(), floor.value())
                        .lit(),
                );
            }
        }

        Ok(Some(if converted.is_empty() {
            negated.lit()
        } else {
            expr.clone().in_list(converted, negated)
        }))
    }
}

impl TreeNodeRewriter for TypeConverter {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let new_expr = match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => self.convert_comparison_expr(*left, op, *right)?,
                _ => Expr::BinaryExpr(BinaryExpr { left, op, right }),
            },
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                if let Some(expr) =
                    self.convert_timestamp_string_between(&expr, negated, &low, &high)?
                {
                    return Ok(Transformed::yes(expr));
                }
                let (expr, low) = self.convert_type(&expr, &low)?;
                let (expr, high) = self.convert_type(&expr, &high)?;
                Expr::Between(Between {
                    expr: Box::new(expr),
                    negated,
                    low: Box::new(low),
                    high: Box::new(high),
                })
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                if let Some(expr) = self.convert_timestamp_string_in_list(&expr, &list, negated)? {
                    return Ok(Transformed::yes(expr));
                }
                let mut list_expr = Vec::with_capacity(list.len());
                for e in list {
                    let (_, expr_conversion) = self.convert_type(&expr, &e)?;
                    list_expr.push(expr_conversion);
                }
                Expr::InList(InList {
                    expr,
                    list: list_expr,
                    negated,
                })
            }
            Expr::Literal(value, _) => Expr::Literal(value, None),
            expr => expr,
        };
        Ok(Transformed::yes(new_expr))
    }
}

fn string_to_timestamp_scalar(string: &str, timezone: Option<&Timezone>) -> Result<ScalarValue> {
    let ts = Timestamp::from_str(string, timezone)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let value = Some(ts.value());
    let scalar = match ts.unit() {
        TimeUnit::Second => ScalarValue::TimestampSecond(value, None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, None),
    };
    Ok(scalar)
}

fn timestamp_to_scalar(
    timestamp: Timestamp,
    target_unit: TimeUnit,
    timezone: Option<Arc<str>>,
) -> Result<ScalarValue> {
    let timestamp = timestamp
        .convert_to(target_unit)
        .ok_or_else(|| DataFusionError::Plan("timestamp literal overflow".to_string()))?;
    Ok(timestamp_scalar(
        timestamp.unit().as_arrow_time_unit(),
        timezone,
        timestamp.value(),
    ))
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion_common::arrow::datatypes::Field;
    use datafusion_common::{Column, DFSchema};
    use datafusion_expr::{Literal, LogicalPlanBuilder};
    use datafusion_sql::TableReference;
    use session::context::QueryContext;

    use super::*;

    #[test]
    fn test_string_to_timestamp_scalar() {
        assert_eq!(
            string_to_timestamp_scalar("2022-02-02 19:00:00+08:00", None).unwrap(),
            ScalarValue::TimestampSecond(Some(1643799600), None)
        );
        assert_eq!(
            string_to_timestamp_scalar("2009-02-13 23:31:30Z", None).unwrap(),
            ScalarValue::TimestampSecond(Some(1234567890), None)
        );

        assert_eq!(
            string_to_timestamp_scalar(
                "2009-02-13 23:31:30",
                Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap())
            )
            .unwrap(),
            ScalarValue::TimestampSecond(Some(1234567890 - 8 * 3600), None)
        );

        assert_eq!(
            string_to_timestamp_scalar(
                "2009-02-13 23:31:30",
                Some(&Timezone::from_tz_string("-8:00").unwrap())
            )
            .unwrap(),
            ScalarValue::TimestampSecond(Some(1234567890 + 8 * 3600), None)
        );
    }

    #[test]
    fn test_convert_timestamp_str() {
        use datatypes::arrow::datatypes::TimeUnit as ArrowTimeUnit;

        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![(
                    None::<TableReference>,
                    Arc::new(Field::new(
                        "ts",
                        DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                        true,
                    )),
                )],
                HashMap::new(),
            )
            .unwrap(),
        );
        let mut converter = TypeConverter {
            schema,
            query_ctx: QueryContext::arc(),
        };

        assert_eq!(
            Expr::Column(Column::from_name("ts")).gt(ScalarValue::TimestampMillisecond(
                Some(1599514949000),
                None
            )
            .lit()),
            converter
                .f_up(Expr::Column(Column::from_name("ts")).gt("2020-09-08T05:42:29+08:00".lit()))
                .unwrap()
                .data
        );
    }

    #[test]
    fn test_convert_timestamp_string_comparison_preserves_literal_precision() {
        use datatypes::arrow::datatypes::TimeUnit as ArrowTimeUnit;

        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![(
                    None::<TableReference>,
                    Arc::new(Field::new(
                        "ts",
                        DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                        true,
                    )),
                )],
                HashMap::new(),
            )
            .unwrap(),
        );
        let mut converter = TypeConverter {
            schema,
            query_ctx: QueryContext::arc(),
        };
        let ts_col = Expr::Column(Column::from_name("ts"));

        assert_eq!(
            ts_col
                .clone()
                .lt_eq(ScalarValue::TimestampMillisecond(Some(1780372200195), None).lit()),
            converter
                .f_up(ts_col.clone().lt("2026-06-02 03:50:00.195100+00:00".lit()))
                .unwrap()
                .data
        );

        assert_eq!(
            false.lit(),
            converter
                .f_up(ts_col.clone().eq("2026-06-02 03:50:00.195100+00:00".lit()))
                .unwrap()
                .data
        );

        assert_eq!(
            ts_col
                .clone()
                .gt_eq(ScalarValue::TimestampMillisecond(Some(1780372200196), None).lit()),
            converter
                .f_up(ts_col.gt_eq("2026-06-02 03:50:00.195100+00:00".lit()))
                .unwrap()
                .data
        );
    }

    #[test]
    fn test_convert_timestamp_string_between_and_in_list_preserve_literal_precision() {
        use datatypes::arrow::datatypes::TimeUnit as ArrowTimeUnit;

        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![(
                    None::<TableReference>,
                    Arc::new(Field::new(
                        "ts",
                        DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                        true,
                    )),
                )],
                HashMap::new(),
            )
            .unwrap(),
        );
        let mut converter = TypeConverter {
            schema,
            query_ctx: QueryContext::arc(),
        };
        let ts_col = Expr::Column(Column::from_name("ts"));

        assert_eq!(
            ts_col
                .clone()
                .gt_eq(ScalarValue::TimestampMillisecond(Some(1780372200001), None).lit())
                .and(
                    ts_col
                        .clone()
                        .lt_eq(ScalarValue::TimestampMillisecond(Some(1780372200195), None).lit()),
                ),
            converter
                .f_up(Expr::Between(Between::new(
                    Box::new(ts_col.clone()),
                    false,
                    Box::new("2026-06-02 03:50:00.000100+00:00".lit()),
                    Box::new("2026-06-02 03:50:00.195100+00:00".lit()),
                )))
                .unwrap()
                .data
        );

        assert_eq!(
            false.lit(),
            converter
                .f_up(ts_col.in_list(vec!["2026-06-02 03:50:00.195100+00:00".lit()], false,))
                .unwrap()
                .data
        );
    }

    #[test]
    fn test_convert_bool() {
        let col_name = "is_valid";
        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![(
                    None::<TableReference>,
                    Arc::new(Field::new(col_name, DataType::Boolean, false)),
                )],
                HashMap::new(),
            )
            .unwrap(),
        );
        let mut converter = TypeConverter {
            schema,
            query_ctx: QueryContext::arc(),
        };

        assert_eq!(
            Expr::Column(Column::from_name(col_name)).eq(true.lit()),
            converter
                .f_up(Expr::Column(Column::from_name(col_name)).eq("true".lit()))
                .unwrap()
                .data
        );
    }

    #[test]
    fn test_retrieve_type_from_aggr_plan() {
        let plan = LogicalPlanBuilder::values(vec![vec![
            ScalarValue::Int64(Some(1)).lit(),
            ScalarValue::Float64(Some(1.0)).lit(),
            ScalarValue::TimestampMillisecond(Some(1), None).lit(),
        ]])
        .unwrap()
        .filter(Expr::Column(Column::from_name("column3")).gt("1970-01-01 00:00:00+08:00".lit()))
        .unwrap()
        .filter(
            "1970-01-01 00:00:00+08:00"
                .lit()
                .lt_eq(Expr::Column(Column::from_name("column3"))),
        )
        .unwrap()
        .aggregate(
            Vec::<Expr>::new(),
            vec![Expr::AggregateFunction(
                datafusion_expr::expr::AggregateFunction::new_udf(
                    datafusion::functions_aggregate::count::count_udaf(),
                    vec![Expr::Column(Column::from_name("column1"))],
                    false,
                    None,
                    vec![],
                    None,
                ),
            )],
        )
        .unwrap()
        .build()
        .unwrap();
        let context = QueryEngineContext::mock();

        let transformed_plan = TypeConversionRule
            .analyze(plan, &context, &ConfigOptions::default())
            .unwrap();
        let expected = String::from(
            "Aggregate: groupBy=[[]], aggr=[[count(column1)]]\
            \n  Filter: column3 >= TimestampMillisecond(-28800000, None)\
            \n    Filter: column3 > TimestampMillisecond(-28800000, None)\
            \n      Values: (Int64(1), Float64(1), TimestampMillisecond(1, None))",
        );
        assert_eq!(format!("{}", transformed_plan.display_indent()), expected);
    }

    #[test]
    fn test_reverse_non_ts_type() {
        let context = QueryEngineContext::mock();

        let plan = LogicalPlanBuilder::values(vec![vec![1.0f64.lit()]])
            .unwrap()
            .filter(Expr::Column(Column::from_name("column1")).gt_eq("1.2345".lit()))
            .unwrap()
            .filter(
                "1.2345"
                    .lit()
                    .lt(Expr::Column(Column::from_name("column1"))),
            )
            .unwrap()
            .build()
            .unwrap();
        let transformed_plan = TypeConversionRule
            .analyze(plan, &context, &ConfigOptions::default())
            .unwrap();
        let expected = String::from(
            "Filter: Utf8(\"1.2345\") < column1\
            \n  Filter: column1 >= Utf8(\"1.2345\")\
            \n    Values: (Float64(1))",
        );
        assert_eq!(format!("{}", transformed_plan.display_indent()), expected);
    }
}
