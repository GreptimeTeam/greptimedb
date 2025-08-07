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

use common_time::timestamp::{TimeUnit, Timestamp};
use common_time::Timezone;
use datafusion::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::InList;
use datafusion_expr::{
    Between, BinaryExpr, Expr, ExprSchemable, Filter, LogicalPlan, Operator, TableScan,
};
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::DataType;
use session::context::QueryContextRef;

use crate::optimizer::ExtensionAnalyzerRule;
use crate::plan::ExtractExpr;
use crate::QueryEngineContext;

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
            | LogicalPlan::Join { .. }
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
            | LogicalPlan::RecursiveQuery(_) => Ok(Transformed::no(plan)),
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
        if let Expr::Column(_) = expr {
            if let Ok(v) = expr.get_type(&self.schema) {
                return Some(v);
            }
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
                string_to_timestamp_ms(v, Some(&self.query_ctx.timezone()))
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
            Expr::Literal(value, _) => match value {
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
                _ => Expr::Literal(value, None),
            },
            expr => expr,
        };
        Ok(Transformed::yes(new_expr))
    }
}

fn timestamp_to_timestamp_ms_expr(val: i64, unit: TimeUnit) -> Expr {
    let timestamp = match unit {
        TimeUnit::Second => val * 1_000,
        TimeUnit::Millisecond => val,
        TimeUnit::Microsecond => val / 1_000,
        TimeUnit::Nanosecond => val / 1_000 / 1_000,
    };

    Expr::Literal(
        ScalarValue::TimestampMillisecond(Some(timestamp), None),
        None,
    )
}

fn string_to_timestamp_ms(string: &str, timezone: Option<&Timezone>) -> Result<ScalarValue> {
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
    fn test_string_to_timestamp_ms() {
        assert_eq!(
            string_to_timestamp_ms("2022-02-02 19:00:00+08:00", None).unwrap(),
            ScalarValue::TimestampSecond(Some(1643799600), None)
        );
        assert_eq!(
            string_to_timestamp_ms("2009-02-13 23:31:30Z", None).unwrap(),
            ScalarValue::TimestampSecond(Some(1234567890), None)
        );

        assert_eq!(
            string_to_timestamp_ms(
                "2009-02-13 23:31:30",
                Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap())
            )
            .unwrap(),
            ScalarValue::TimestampSecond(Some(1234567890 - 8 * 3600), None)
        );

        assert_eq!(
            string_to_timestamp_ms(
                "2009-02-13 23:31:30",
                Some(&Timezone::from_tz_string("-8:00").unwrap())
            )
            .unwrap(),
            ScalarValue::TimestampSecond(Some(1234567890 + 8 * 3600), None)
        );
    }

    #[test]
    fn test_timestamp_to_timestamp_ms_expr() {
        assert_eq!(
            timestamp_to_timestamp_ms_expr(123, TimeUnit::Second),
            ScalarValue::TimestampMillisecond(Some(123000), None).lit()
        );

        assert_eq!(
            timestamp_to_timestamp_ms_expr(123, TimeUnit::Millisecond),
            ScalarValue::TimestampMillisecond(Some(123), None).lit()
        );

        assert_eq!(
            timestamp_to_timestamp_ms_expr(123, TimeUnit::Microsecond),
            ScalarValue::TimestampMillisecond(Some(0), None).lit()
        );

        assert_eq!(
            timestamp_to_timestamp_ms_expr(1230, TimeUnit::Microsecond),
            ScalarValue::TimestampMillisecond(Some(1), None).lit()
        );

        assert_eq!(
            timestamp_to_timestamp_ms_expr(123000, TimeUnit::Microsecond),
            ScalarValue::TimestampMillisecond(Some(123), None).lit()
        );

        assert_eq!(
            timestamp_to_timestamp_ms_expr(1230, TimeUnit::Nanosecond),
            ScalarValue::TimestampMillisecond(Some(0), None).lit()
        );
        assert_eq!(
            timestamp_to_timestamp_ms_expr(123_000_000, TimeUnit::Nanosecond),
            ScalarValue::TimestampMillisecond(Some(123), None).lit()
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
            Expr::Column(Column::from_name("ts")).gt(ScalarValue::TimestampSecond(
                Some(1599514949),
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
            \n  Filter: TimestampSecond(-28800, None) <= column3\
            \n    Filter: column3 > TimestampSecond(-28800, None)\
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
