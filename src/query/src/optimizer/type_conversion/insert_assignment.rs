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

use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::expr::{Alias, Cast};
use datafusion_expr::{Distinct, Expr, ExprSchemable, LogicalPlan, Projection, Union, Values};
use datatypes::arrow::datatypes::DataType;
use session::context::QueryContextRef;

use crate::optimizer::type_conversion::cast_string_to_timestamp;
use crate::plan::ExtractExpr;

/// Rewrites string literals that feed timestamp columns at an INSERT boundary.
///
/// A column that carries the same literal in every row is folded into the
/// assignment expression, which leaves the source query untouched. `VALUES`
/// rows and `UNION` branches carry per-row values, so those nodes are rewritten
/// in place instead. Source expressions are never evaluated, which keeps
/// explicit casts on DataFusion's existing path.
pub(super) fn rewrite_insert_assignments(
    plan: LogicalPlan,
    query_ctx: QueryContextRef,
) -> Result<LogicalPlan> {
    let LogicalPlan::Projection(assignment) = plan else {
        return Ok(plan);
    };

    let converter = InsertAssignmentConverter { query_ctx };
    let mut exprs = assignment.expr.clone();
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

        let literal = lineage_literal(&input, input_idx).cloned();
        if let Some(literal) = literal
            && let Some(folded) = converter.convert_literal(&literal, target_type)
        {
            let (qualifier, field) = assignment.schema.qualified_field(output_idx);
            exprs[output_idx] = folded.alias_qualified(qualifier.cloned(), field.name());
            changed = true;
            continue;
        }

        if let Some(rewritten) = converter.rewrite_output_column(&input, input_idx, target_type)? {
            input = rewritten;
            changed = true;
        }
    }

    if !changed {
        return Ok(LogicalPlan::Projection(assignment));
    }
    Projection::try_new(exprs, Arc::new(input)).map(LogicalPlan::Projection)
}

/// Resolves the literal that `output_idx` carries in every row of `plan`.
///
/// Only nodes that keep a row's value intact are followed, so the caller can
/// treat the result as a constant of the whole relation.
fn lineage_literal(plan: &LogicalPlan, output_idx: usize) -> Option<&ScalarValue> {
    if output_idx >= plan.schema().fields().len() {
        return None;
    }

    match plan {
        LogicalPlan::Projection(projection) => match unalias(&projection.expr[output_idx]) {
            Expr::Literal(value, _) => Some(value),
            Expr::Column(column) => {
                let input_idx = projection.input.schema().maybe_index_of_column(column)?;
                lineage_literal(projection.input.as_ref(), input_idx)
            }
            _ => None,
        },
        // These nodes drop, reorder or deduplicate rows without touching the
        // value a surviving row carries, and their schema stays positional.
        LogicalPlan::Filter(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Distinct(Distinct::All(_)) => {
            let inputs = plan.inputs();
            let [input] = inputs.as_slice() else {
                return None;
            };
            lineage_literal(input, output_idx)
        }
        _ => None,
    }
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
            // Filter, Sort and Distinct are excluded here: retyping a column
            // they read would change the source query. Constants still reach
            // the assignment through `lineage_literal`, which mutates nothing.
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
        match unalias(&projection.expr[output_idx]) {
            Expr::Literal(value, _) => {
                let Some(converted) = self.convert_literal(value, target_type) else {
                    return Ok(None);
                };
                let (qualifier, field) = projection.schema.qualified_field(output_idx);
                let mut exprs = projection.expr.clone();
                exprs[output_idx] = converted.alias_qualified(qualifier.cloned(), field.name());

                Projection::try_new(exprs, projection.input.clone())
                    .map(LogicalPlan::Projection)
                    .map(Some)
            }
            Expr::Column(column) => {
                // Retyping the input column affects every output column reading
                // it, so only follow lineage with a single consumer.
                if projection
                    .expr
                    .iter()
                    .enumerate()
                    .any(|(idx, other)| idx != output_idx && other.column_refs().contains(column))
                {
                    return Ok(None);
                }

                let input = projection.input.as_ref();
                let Some(input_idx) = input.schema().maybe_index_of_column(column) else {
                    return Ok(None);
                };
                let Some(rewritten) = self.rewrite_output_column(input, input_idx, target_type)?
                else {
                    return Ok(None);
                };

                Projection::try_new(projection.expr.clone(), Arc::new(rewritten))
                    .map(LogicalPlan::Projection)
                    .map(Some)
            }
            _ => Ok(None),
        }
    }

    fn rewrite_values(
        &self,
        values: &Values,
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

    fn convert_literal(&self, value: &ScalarValue, target_type: &DataType) -> Option<Expr> {
        let ScalarValue::Utf8(Some(value)) = value else {
            return None;
        };
        cast_string_to_timestamp(value, target_type, Some(&self.query_ctx.timezone()))
            .ok()
            .filter(|value| !value.is_null())
            .map(|value| Expr::Literal(value, None))
    }
}

fn assignment_input_index(expr: &Expr, input_schema: &DFSchema) -> Option<usize> {
    let expr = match unalias(expr) {
        Expr::Cast(Cast { expr, .. }) => expr.as_ref(),
        expr => expr,
    };
    let Expr::Column(column) = expr else {
        return None;
    };
    input_schema.maybe_index_of_column(column)
}

fn unalias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(Alias { expr, .. }) => unalias(expr),
        expr => expr,
    }
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
