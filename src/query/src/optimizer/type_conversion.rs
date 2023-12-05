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

use std::str::FromStr;

use common_time::timestamp::{TimeUnit, Timestamp};
use datafusion::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::InList;
use datafusion_expr::{
    Between, BinaryExpr, Expr, ExprSchemable, Filter, LogicalPlan, Operator, TableScan,
};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::DataType;

/// TypeConversionRule converts some literal values in logical plan to other types according
/// to data type of corresponding columns.
/// Specifically:
/// - string literal of timestamp is converted to `Expr::Literal(ScalarValue::TimestampMillis)`
/// - string literal of boolean is converted to `Expr::Literal(ScalarValue::Boolean)`
pub struct TypeConversionRule;

impl AnalyzerRule for TypeConversionRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform(&|plan| match plan {
            LogicalPlan::Filter(filter) => {
                let mut converter = TypeConverter {
                    schema: filter.input.schema().clone(),
                };
                let rewritten = filter.predicate.clone().rewrite(&mut converter)?;
                Ok(Transformed::Yes(LogicalPlan::Filter(Filter::try_new(
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
                };
                let rewrite_filters = filters
                    .into_iter()
                    .map(|e| e.rewrite(&mut converter))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Transformed::Yes(LogicalPlan::TableScan(TableScan {
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
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossJoin { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Analyze { .. } => {
                let mut converter = TypeConverter {
                    schema: plan.schema().clone(),
                };
                let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
                let expr = plan
                    .expressions()
                    .into_iter()
                    .map(|e| e.rewrite(&mut converter))
                    .collect::<Result<Vec<_>>>()?;

                plan.with_new_exprs(expr, &inputs).map(Transformed::Yes)
            }

            LogicalPlan::Subquery { .. }
            | LogicalPlan::SubqueryAlias { .. }
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Prepare(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_) => Ok(Transformed::No(plan)),
        })
    }

    fn name(&self) -> &str {
        "TypeConversionRule"
    }
}

struct TypeConverter {
    schema: DFSchemaRef,
}

impl TypeConverter {
    fn column_type(&self, expr: &Expr) -> Option<DataType> {
        if let Expr::Column(_) = expr {
            if let Ok(v) = expr.get_type(&self.schema) {
                return Some(v);
            }
        }
        None
    }

    fn cast_scalar_value(value: &ScalarValue, target_type: &DataType) -> Result<ScalarValue> {
        match (target_type, value) {
            (DataType::Timestamp(_, _), ScalarValue::Utf8(Some(v))) => string_to_timestamp_ms(v),
            (DataType::Boolean, ScalarValue::Utf8(Some(v))) => match v.to_lowercase().as_str() {
                "true" => Ok(ScalarValue::Boolean(Some(true))),
                "false" => Ok(ScalarValue::Boolean(Some(false))),
                _ => Ok(ScalarValue::Boolean(None)),
            },
            (target_type, value) => {
                let value_arr = value.to_array();
                let arr =
                    compute::cast(&value_arr, target_type).map_err(DataFusionError::ArrowError)?;

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
            (Expr::Column(col), Expr::Literal(value)) => {
                let casted_right = Self::cast_scalar_value(value, target_type)?;
                if casted_right.is_null() {
                    return Err(DataFusionError::Plan(format!(
                        "column:{col:?}. Casting value:{value:?} to {target_type:?} is invalid",
                    )));
                }
                Ok((left.clone(), Expr::Literal(casted_right)))
            }
            (Expr::Literal(value), Expr::Column(col)) => {
                let casted_left = Self::cast_scalar_value(value, target_type)?;
                if casted_left.is_null() {
                    return Err(DataFusionError::Plan(format!(
                        "column:{col:?}. Casting value:{value:?} to {target_type:?} is invalid",
                    )));
                }
                Ok((Expr::Literal(casted_left), right.clone()))
            }
            _ => Ok((left.clone(), right.clone())),
        }
    }
}

impl TreeNodeRewriter for TypeConverter {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        let new_expr = match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {
                    let (left, right) = self.convert_type(&left, &right)?;
                    Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    })
                }
                _ => Expr::BinaryExpr(BinaryExpr { left, op, right }),
            },
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
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
            Expr::Literal(value) => match value {
                ScalarValue::TimestampSecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(i, TimeUnit::Second)
                }
                ScalarValue::TimestampMillisecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(i, TimeUnit::Millisecond)
                }
                ScalarValue::TimestampMicrosecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(i, TimeUnit::Microsecond)
                }
                ScalarValue::TimestampNanosecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(i, TimeUnit::Nanosecond)
                }
                _ => Expr::Literal(value),
            },
            expr => expr,
        };
        Ok(new_expr)
    }
}

fn timestamp_to_timestamp_ms_expr(val: i64, unit: TimeUnit) -> Expr {
    let timestamp = match unit {
        TimeUnit::Second => val * 1_000,
        TimeUnit::Millisecond => val,
        TimeUnit::Microsecond => val / 1_000,
        TimeUnit::Nanosecond => val / 1_000 / 1_000,
    };

    Expr::Literal(ScalarValue::TimestampMillisecond(Some(timestamp), None))
}

fn string_to_timestamp_ms(string: &str) -> Result<ScalarValue> {
    let ts = Timestamp::from_str(string).map_err(|e| DataFusionError::External(Box::new(e)))?;

    let value = Some(ts.value());
    let scalar = match ts.unit() {
        TimeUnit::Second => ScalarValue::TimestampSecond(value, None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, None),
    };
    Ok(scalar)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::logical_expr::expr::AggregateFunction as AggrExpr;
    use datafusion_common::{Column, DFField, DFSchema};
    use datafusion_expr::{AggregateFunction, LogicalPlanBuilder};
    use datafusion_sql::TableReference;

    use super::*;

    #[test]
    fn test_string_to_timestamp_ms() {
        assert_eq!(
            string_to_timestamp_ms("2022-02-02 19:00:00+08:00").unwrap(),
            ScalarValue::TimestampSecond(Some(1643799600), None)
        );
        assert_eq!(
            string_to_timestamp_ms("2009-02-13 23:31:30Z").unwrap(),
            ScalarValue::TimestampSecond(Some(1234567890), None)
        );
    }

    #[test]
    fn test_timestamp_to_timestamp_ms_expr() {
        assert_eq!(
            timestamp_to_timestamp_ms_expr(123, TimeUnit::Second),
            Expr::Literal(ScalarValue::TimestampMillisecond(Some(123000), None))
        );

        assert_eq!(
            timestamp_to_timestamp_ms_expr(123, TimeUnit::Millisecond),
            Expr::Literal(ScalarValue::TimestampMillisecond(Some(123), None))
        );

        assert_eq!(
            timestamp_to_timestamp_ms_expr(123, TimeUnit::Microsecond),
            Expr::Literal(ScalarValue::TimestampMillisecond(Some(0), None))
        );

        assert_eq!(
            timestamp_to_timestamp_ms_expr(1230, TimeUnit::Microsecond),
            Expr::Literal(ScalarValue::TimestampMillisecond(Some(1), None))
        );

        assert_eq!(
            timestamp_to_timestamp_ms_expr(123000, TimeUnit::Microsecond),
            Expr::Literal(ScalarValue::TimestampMillisecond(Some(123), None))
        );

        assert_eq!(
            timestamp_to_timestamp_ms_expr(1230, TimeUnit::Nanosecond),
            Expr::Literal(ScalarValue::TimestampMillisecond(Some(0), None))
        );
        assert_eq!(
            timestamp_to_timestamp_ms_expr(123_000_000, TimeUnit::Nanosecond),
            Expr::Literal(ScalarValue::TimestampMillisecond(Some(123), None))
        );
    }

    #[test]
    fn test_convert_timestamp_str() {
        use datatypes::arrow::datatypes::TimeUnit as ArrowTimeUnit;

        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![DFField::new(
                    None::<TableReference>,
                    "ts",
                    DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                    true,
                )],
                HashMap::new(),
            )
            .unwrap(),
        );
        let mut converter = TypeConverter { schema };

        assert_eq!(
            Expr::Column(Column::from_name("ts")).gt(Expr::Literal(ScalarValue::TimestampSecond(
                Some(1599514949),
                None
            ))),
            converter
                .mutate(
                    Expr::Column(Column::from_name("ts")).gt(Expr::Literal(ScalarValue::Utf8(
                        Some("2020-09-08T05:42:29+08:00".to_string()),
                    )))
                )
                .unwrap()
        );
    }

    #[test]
    fn test_convert_bool() {
        let col_name = "is_valid";
        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![DFField::new(
                    None::<TableReference>,
                    col_name,
                    DataType::Boolean,
                    false,
                )],
                HashMap::new(),
            )
            .unwrap(),
        );
        let mut converter = TypeConverter { schema };

        assert_eq!(
            Expr::Column(Column::from_name(col_name))
                .eq(Expr::Literal(ScalarValue::Boolean(Some(true)))),
            converter
                .mutate(
                    Expr::Column(Column::from_name(col_name))
                        .eq(Expr::Literal(ScalarValue::Utf8(Some("true".to_string()))))
                )
                .unwrap()
        );
    }

    #[test]
    fn test_retrieve_type_from_aggr_plan() {
        let plan =
            LogicalPlanBuilder::values(vec![vec![
                Expr::Literal(ScalarValue::Int64(Some(1))),
                Expr::Literal(ScalarValue::Float64(Some(1.0))),
                Expr::Literal(ScalarValue::TimestampMillisecond(Some(1), None)),
            ]])
            .unwrap()
            .filter(Expr::Column(Column::from_name("column3")).gt(Expr::Literal(
                ScalarValue::Utf8(Some("1970-01-01 00:00:00+08:00".to_string())),
            )))
            .unwrap()
            .filter(
                Expr::Literal(ScalarValue::Utf8(Some(
                    "1970-01-01 00:00:00+08:00".to_string(),
                )))
                .lt_eq(Expr::Column(Column::from_name("column3"))),
            )
            .unwrap()
            .aggregate(
                Vec::<Expr>::new(),
                vec![Expr::AggregateFunction(AggrExpr {
                    fun: AggregateFunction::Count,
                    args: vec![Expr::Column(Column::from_name("column1"))],
                    distinct: false,
                    filter: None,
                    order_by: None,
                })],
            )
            .unwrap()
            .build()
            .unwrap();

        let transformed_plan = TypeConversionRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();
        let expected = String::from(
            "Aggregate: groupBy=[[]], aggr=[[COUNT(column1)]]\
            \n  Filter: TimestampSecond(-28800, None) <= column3\
            \n    Filter: column3 > TimestampSecond(-28800, None)\
            \n      Values: (Int64(1), Float64(1), TimestampMillisecond(1, None))",
        );
        assert_eq!(format!("{}", transformed_plan.display_indent()), expected);
    }

    #[test]
    fn test_reverse_non_ts_type() {
        let plan =
            LogicalPlanBuilder::values(vec![vec![Expr::Literal(ScalarValue::Float64(Some(1.0)))]])
                .unwrap()
                .filter(
                    Expr::Column(Column::from_name("column1"))
                        .gt_eq(Expr::Literal(ScalarValue::Utf8(Some("1.2345".to_string())))),
                )
                .unwrap()
                .filter(
                    Expr::Literal(ScalarValue::Utf8(Some("1.2345".to_string())))
                        .lt(Expr::Column(Column::from_name("column1"))),
                )
                .unwrap()
                .build()
                .unwrap();
        let transformed_plan = TypeConversionRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();
        let expected = String::from(
            "Filter: Utf8(\"1.2345\") < column1\
            \n  Filter: column1 >= Utf8(\"1.2345\")\
            \n    Values: (Float64(1))",
        );
        assert_eq!(format!("{}", transformed_plan.display_indent()), expected);
    }
}
