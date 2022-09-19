use std::str::FromStr;
use std::sync::Arc;

use arrow::compute;
use arrow::compute::cast::CastOptions;
use arrow::datatypes::DataType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::plan::Filter;
use datafusion::logical_plan::{
    Expr, ExprRewritable, ExprRewriter, ExprSchemable, LogicalPlan, Operator, TableScan,
};
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::optimizer::utils;
use datafusion_common::{DFSchemaRef, DataFusionError, ScalarValue};

mod type_conversion;
use common_telemetry::debug;
use common_time::timestamp::{TimeUnit, Timestamp};
use datafusion_common::Result;
pub struct TypeConversionRule;

impl OptimizerRule for TypeConversionRule {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        let mut converter = TypeConverter {
            schemas: plan.all_schemas(),
        };

        match plan {
            LogicalPlan::Filter(Filter { predicate, input }) => Ok(LogicalPlan::Filter(Filter {
                predicate: predicate.clone().rewrite(&mut converter)?,
                input: Arc::new(self.optimize(input, execution_props)?),
            })),
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                limit,
            }) => {
                let rewrite_filters = filters
                    .clone()
                    .into_iter()
                    .map(|e| e.rewrite(&mut converter))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::TableScan(TableScan {
                    table_name: table_name.clone(),
                    source: source.clone(),
                    projection: projection.clone(),
                    projected_schema: projected_schema.clone(),
                    filters: rewrite_filters,
                    limit: *limit,
                }))
            }
            LogicalPlan::Projection { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossJoin { .. }
            | LogicalPlan::CreateMemoryTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Analyze { .. } => {
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(plan, execution_props))
                    .collect::<Result<Vec<_>>>()?;

                let expr = plan
                    .expressions()
                    .into_iter()
                    .map(|e| e.rewrite(&mut converter))
                    .collect::<Result<Vec<_>>>()?;

                utils::from_plan(plan, &expr, &new_inputs)
            }

            LogicalPlan::EmptyRelation { .. } => Ok(plan.clone()),
        }
    }

    fn name(&self) -> &str {
        "TypeConversionRule"
    }
}

struct TypeConverter<'a> {
    schemas: Vec<&'a DFSchemaRef>,
}

impl<'a> TypeConverter<'a> {
    fn column_data_type(&self, expr: &Expr) -> Option<DataType> {
        if let Expr::Column(_) = expr {
            for schema in &self.schemas {
                if let Ok(v) = expr.get_type(schema) {
                    return Some(v);
                }
            }
        }
        None
    }

    fn cast_scalar_value(value: &ScalarValue, data_type: &DataType) -> Result<ScalarValue> {
        if let DataType::Timestamp(_, _) = data_type {
            if let ScalarValue::Utf8(Some(v)) = value {
                return string_to_timestamp_ms(v);
            }
        }

        if let DataType::Boolean = data_type {
            if let ScalarValue::Utf8(Some(v)) = value {
                return match v.to_lowercase().as_str() {
                    "true" => Ok(ScalarValue::Boolean(Some(true))),
                    "false" => Ok(ScalarValue::Boolean(Some(false))),
                    _ => Ok(ScalarValue::Boolean(None)),
                };
            }
        }

        let value_arr = value.to_array();

        let arr = compute::cast::cast(value_arr.as_ref(), data_type, CastOptions::default())
            .map_err(DataFusionError::ArrowError)?;

        ScalarValue::try_from_array(
            &Arc::from(arr), // index: Converts a value in `array` at `index` into a ScalarValue
            0,
        )
    }

    fn convert_type<'b>(&self, mut left: &'b Expr, mut right: &'b Expr) -> Result<(Expr, Expr)> {
        let left_type = self.column_data_type(left);
        let right_type = self.column_data_type(right);

        let mut reverse = false;
        let left_type = match (&left_type, &right_type) {
            (Some(v), None) => v,
            (None, Some(v)) => {
                reverse = true;
                std::mem::swap(&mut left, &mut right);
                v
            }
            _ => return Ok((left.clone(), right.clone())),
        };

        match (left, right) {
            (Expr::Column(col), Expr::Literal(value)) => {
                let casted_right = Self::cast_scalar_value(value, left_type)?;
                debug!(
                    "TypeRewriter convert type, origin_left:{:?}, type:{:?}, right:{:?}, casted_right:{:?}",
                    col, left_type, value, casted_right
                );
                if casted_right.is_null() {
                    return Err(DataFusionError::Plan(format!(
                        "column:{:?} value:{:?} is invalid",
                        col, value
                    )));
                }
                if reverse {
                    Ok((Expr::Literal(casted_right), left.clone()))
                } else {
                    Ok((left.clone(), Expr::Literal(casted_right)))
                }
            }
            _ => Ok((left.clone(), right.clone())),
        }
    }
}

impl<'a> ExprRewriter for TypeConverter<'a> {
    fn mutate(&mut self, expr: Expr) -> datafusion_common::Result<Expr> {
        let new_expr = match expr {
            Expr::BinaryExpr { left, op, right } => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {
                    let (left, right) = self.convert_type(&left, &right)?;
                    Expr::BinaryExpr {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    }
                }
                _ => Expr::BinaryExpr { left, op, right },
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let (expr, low) = self.convert_type(&expr, &low)?;
                let (expr, high) = self.convert_type(&expr, &high)?;
                Expr::Between {
                    expr: Box::new(expr),
                    negated,
                    low: Box::new(low),
                    high: Box::new(high),
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut list_expr = Vec::with_capacity(list.len());
                for e in list {
                    let (_, expr_conversion) = self.convert_type(&expr, &e)?;
                    list_expr.push(expr_conversion);
                }
                Expr::InList {
                    expr,
                    list: list_expr,
                    negated,
                }
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
    Ok(ScalarValue::TimestampMillisecond(
        Some(
            Timestamp::from_str(string)
                .map(|t| t.value() / 1_000_000)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ),
        None,
    ))
}
