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

use datafusion_common::Result;
use datafusion_expr::expr::{Alias, Cast};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, Projection, Union};
use datatypes::arrow::datatypes::DataType;
use session::context::QueryContextRef;

use crate::optimizer::type_conversion::cast_string_to_timestamp;
use crate::plan::ExtractExpr;

/// Rewrites string literals that feed timestamp columns at an INSERT boundary.
///
/// The rewrite follows output columns through relational nodes but does not
/// evaluate source expressions. This keeps explicit casts and other query
/// expressions on DataFusion's existing path.
pub(super) fn rewrite_insert_assignments(
    plan: LogicalPlan,
    query_ctx: QueryContextRef,
) -> Result<LogicalPlan> {
    let LogicalPlan::Projection(assignment) = plan else {
        return Ok(plan);
    };

    let converter = InsertAssignmentConverter { query_ctx };
    let mut input = assignment.input.as_ref().clone();
    let mut changed = false;
    for (output_idx, expr) in assignment.expr.iter().enumerate() {
        let target_type = assignment.schema.field(output_idx).data_type();
        if !matches!(target_type, DataType::Timestamp(_, _)) {
            continue;
        }

        let Some(input_idx) = assignment_input_index(expr, input.schema()) else {
            continue;
        };
        if let Some(rewritten) = converter.rewrite_output_column(&input, input_idx, target_type)? {
            input = rewritten;
            changed = true;
        }
    }

    if !changed {
        return Ok(LogicalPlan::Projection(assignment));
    }
    Projection::try_new(assignment.expr, Arc::new(input)).map(LogicalPlan::Projection)
}

struct InsertAssignmentConverter {
    query_ctx: QueryContextRef,
}

impl InsertAssignmentConverter {
    /// Rewrites one output column. `Some` always contains a changed plan;
    /// `None` keeps the original plan.
    fn rewrite_output_column(
        &self,
        plan: &LogicalPlan,
        output_idx: usize,
        target_type: &DataType,
    ) -> Result<Option<LogicalPlan>> {
        if output_idx >= plan.schema().fields().len() {
            return Ok(None);
        }

        match plan {
            // DataFusion pushes INSERT assignment casts into Values, so inspect
            // them even though the Values schema already has the target type.
            LogicalPlan::Values(values) => self.rewrite_values(values, output_idx, target_type),
            // Once the source query has produced the target type, its casts and
            // coercions belong to the source query rather than INSERT assignment.
            _ if plan.schema().field(output_idx).data_type() == target_type => Ok(None),
            LogicalPlan::Projection(projection) => {
                self.rewrite_projection(projection, output_idx, target_type)
            }
            LogicalPlan::Union(union) => self.rewrite_union(union, output_idx, target_type),
            // Filter, Sort, and Distinct are not passthrough nodes here: changing
            // an input type could change the source query before assignment.
            LogicalPlan::Limit(_) | LogicalPlan::SubqueryAlias(_) => {
                self.rewrite_passthrough(plan, output_idx, target_type)
            }
            _ => Ok(None),
        }
    }

    fn rewrite_projection(
        &self,
        projection: &Projection,
        output_idx: usize,
        target_type: &DataType,
    ) -> Result<Option<LogicalPlan>> {
        let expr = &projection.expr[output_idx];
        let Some((rewritten_expr, rewritten_input, expr_changed)) =
            self.rewrite_projection_expr(expr, projection.input.as_ref(), target_type)?
        else {
            return Ok(None);
        };

        let mut exprs = projection.expr.clone();
        exprs[output_idx] = if expr_changed && !matches!(rewritten_expr, Expr::Alias(_)) {
            let (qualifier, field) = projection.schema.qualified_field(output_idx);
            rewritten_expr.alias_qualified(qualifier.cloned(), field.name())
        } else {
            rewritten_expr
        };

        Projection::try_new(exprs, Arc::new(rewritten_input))
            .map(LogicalPlan::Projection)
            .map(Some)
    }

    fn rewrite_projection_expr(
        &self,
        expr: &Expr,
        input: &LogicalPlan,
        target_type: &DataType,
    ) -> Result<Option<(Expr, LogicalPlan, bool)>> {
        match expr {
            Expr::Alias(alias) => {
                let Some((rewritten, input, changed)) =
                    self.rewrite_projection_expr(&alias.expr, input, target_type)?
                else {
                    return Ok(None);
                };
                let mut alias = alias.clone();
                alias.expr = Box::new(rewritten);
                Ok(Some((Expr::Alias(alias), input, changed)))
            }
            Expr::Literal(value, _) => {
                let Some(rewritten) = self.convert_literal(value, target_type) else {
                    return Ok(None);
                };
                Ok(Some((rewritten, input.clone(), true)))
            }
            Expr::Column(column) => {
                let Some(input_idx) = input.schema().maybe_index_of_column(column) else {
                    return Ok(None);
                };
                let Some(rewritten_input) =
                    self.rewrite_output_column(input, input_idx, target_type)?
                else {
                    return Ok(None);
                };
                Ok(Some((expr.clone(), rewritten_input, false)))
            }
            _ => Ok(None),
        }
    }

    fn rewrite_values(
        &self,
        values: &datafusion_expr::Values,
        output_idx: usize,
        target_type: &DataType,
    ) -> Result<Option<LogicalPlan>> {
        let mut rewritten = values.clone();
        let mut changed = false;
        for row in &mut rewritten.values {
            let Some(expr) = row.get_mut(output_idx) else {
                return Ok(None);
            };

            if let Expr::Cast(Cast {
                expr: inner,
                data_type,
            }) = expr
                && data_type == target_type
                && let Expr::Literal(value, _) = inner.as_ref()
                && let Some(value) = self.convert_literal(value, target_type)
            {
                *expr = value;
                changed = true;
                continue;
            }

            if expr.get_type(values.schema.as_ref()).ok().as_ref() != Some(target_type) {
                return Ok(None);
            }
        }

        Ok(changed.then_some(LogicalPlan::Values(rewritten)))
    }

    fn rewrite_union(
        &self,
        union: &Union,
        output_idx: usize,
        target_type: &DataType,
    ) -> Result<Option<LogicalPlan>> {
        let mut inputs = Vec::with_capacity(union.inputs.len());
        // A union has one shared schema, so rewrite it only when the same output
        // column is an INSERT-assigned literal in every branch.
        for input in &union.inputs {
            let Some(rewritten) = self.rewrite_output_column(input, output_idx, target_type)?
            else {
                return Ok(None);
            };
            inputs.push(Arc::new(rewritten));
        }

        Union::try_new(inputs).map(LogicalPlan::Union).map(Some)
    }

    fn rewrite_passthrough(
        &self,
        plan: &LogicalPlan,
        output_idx: usize,
        target_type: &DataType,
    ) -> Result<Option<LogicalPlan>> {
        let inputs = plan.inputs();
        let [input] = inputs.as_slice() else {
            return Ok(None);
        };
        let Some(rewritten_input) = self.rewrite_output_column(input, output_idx, target_type)?
        else {
            return Ok(None);
        };

        plan.with_new_exprs(plan.expressions_consider_join(), vec![rewritten_input])
            .map(Some)
    }

    fn convert_literal(
        &self,
        value: &datafusion_common::ScalarValue,
        target_type: &DataType,
    ) -> Option<Expr> {
        let datafusion_common::ScalarValue::Utf8(Some(value)) = value else {
            return None;
        };
        cast_string_to_timestamp(value, target_type, Some(&self.query_ctx.timezone()))
            .ok()
            .filter(|value| !value.is_null())
            .map(|value| Expr::Literal(value, None))
    }
}

fn assignment_input_index(
    expr: &Expr,
    input_schema: &datafusion_common::DFSchema,
) -> Option<usize> {
    let expr = match expr {
        Expr::Alias(Alias { expr, .. }) => expr.as_ref(),
        expr => expr,
    };
    let expr = match expr {
        Expr::Cast(Cast { expr, .. }) => expr.as_ref(),
        expr => expr,
    };
    let Expr::Column(column) = expr else {
        return None;
    };
    input_schema.maybe_index_of_column(column)
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use datafusion_common::arrow::datatypes::TimeUnit;
    use datafusion_expr::Literal;
    use session::context::QueryContext;

    use super::*;

    #[test]
    fn test_convert_literal_uses_query_timezone_and_target_precision() {
        let query_ctx = QueryContext::arc();
        query_ctx.set_timezone(common_time::Timezone::from_tz_string("Asia/Shanghai").unwrap());
        let converter = InsertAssignmentConverter { query_ctx };
        let target_type = DataType::Timestamp(TimeUnit::Nanosecond, None);

        assert_eq!(
            converter.convert_literal(
                &ScalarValue::Utf8(Some("2009-02-13 23:31:30.123456789".to_string())),
                &target_type,
            ),
            Some(ScalarValue::TimestampNanosecond(Some(1_234_539_090_123_456_789), None).lit())
        );
    }

    #[test]
    fn test_convert_literal_falls_back_for_unsupported_literal() {
        let converter = InsertAssignmentConverter {
            query_ctx: QueryContext::arc(),
        };
        let target_type = DataType::Timestamp(TimeUnit::Nanosecond, None);

        for literal in ["1970-01-01", "-8-01-01 00:00:01.5"] {
            assert_eq!(
                converter
                    .convert_literal(&ScalarValue::Utf8(Some(literal.to_string())), &target_type,),
                None
            );
        }
    }
}
