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
use datafusion::config::ConfigOptions;
use datafusion_common::{DFSchemaRef, Result, ScalarValue};
use datafusion_expr::expr::{Alias, Cast};
use datafusion_expr::{Distinct, Expr, ExprSchemable, LogicalPlan, Projection, Values};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_optimizer::analyzer::type_coercion::TypeCoercion;
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use session::context::QueryContextRef;

use crate::optimizer::type_conversion::cast_string_to_timestamp;

/// Interprets strings assigned to timestamp columns at an `INSERT` boundary
/// using the session timezone. `plan` is the assignment projection under a
/// `WriteOp::Insert`.
///
/// DataFusion plans `INSERT` as a projection casting each source column to its
/// target column type, and that cast reads a naive string as UTC. Arrow does
/// apply a timezone when the cast target carries one, so the assignment is
/// routed through `Timestamp(unit, Some(tz))` and back. Stripping the timezone
/// afterwards is value-preserving — arrow only shifts values in the opposite
/// direction.
///
/// The source query is left untouched. Reinterpreting a value where it is
/// *produced* would change what the source query means: pushing the conversion
/// below a `UNION`'s `DISTINCT`, for instance, moves the dedup key from the raw
/// strings to parsed instants and silently drops rows.
///
/// # Why `TypeCoercion` runs here
///
/// The rewrite reads source types, and those are only settled once a `UNION`'s
/// branch types have been reconciled: before coercion a union carries its loose
/// schema (the first branch's types), so a mixed
/// `SELECT 'string' UNION ALL SELECT CAST(.. AS TIMESTAMP)` still looks like a
/// string. Retargeting that cast would leave `Timestamp(None) ->
/// Timestamp(Some(tz))` behind once coercion retypes the union — the one
/// direction in which arrow shifts the value instead of relabelling it.
///
/// Coercing here rather than deferring to the analyzer is forced by where an
/// INSERT is still identifiable: `exec_dml_statement` strips the `Dml` node and
/// executes its input, so by the time the analyzer runs, an assignment
/// projection is indistinguishable from any other projection.
///
/// Explicit casts stay out of this: the SQL layer turns a user's
/// `CAST(x AS TIMESTAMP)` into an `arrow_cast` call, which only becomes an
/// `Expr::Cast` in the optimizer's `SimplifyExpressions`. Assignment casts are
/// therefore the only `Expr::Cast` reaching a timestamp column here.
///
/// # Reach
///
/// The emitted cast only carries its timezone where the expression is evaluated
/// on this node. Substrait drops the timezone name when a plan is pushed down —
/// it encodes any zoned timestamp as `PrecisionTimestampTz` and decodes it back
/// as UTC — so a source reading from a table falls back to UTC, the behaviour it
/// had before this rule existed. Sources that never leave this node (literals,
/// `VALUES`, and `UNION`s of them) keep the session timezone, and those are what
/// an INSERT's timestamp assignment is in practice.
pub(crate) fn rewrite_insert_assignments(
    plan: LogicalPlan,
    query_ctx: &QueryContextRef,
    config: &ConfigOptions,
) -> Result<LogicalPlan> {
    let Some(timezone) = session_timezone(query_ctx) else {
        return Ok(plan);
    };

    let plan = TypeCoercion::new().analyze(plan, config)?;
    rewrite_assignment(plan, &timezone)
}

/// Session timezone, in both forms the rewrite needs.
struct SessionTimezone {
    /// Parses literals, matching the plain `INSERT ... VALUES` path.
    parsed: Timezone,
    /// Names the intermediate arrow cast target.
    name: Arc<str>,
}

fn session_timezone(query_ctx: &QueryContextRef) -> Option<SessionTimezone> {
    let parsed = query_ctx.timezone();

    // A UTC session already gets UTC semantics from the plain assignment cast.
    if parsed.is_utc() {
        return None;
    }

    Some(SessionTimezone {
        name: Arc::from(parsed.to_string()),
        parsed,
    })
}

fn rewrite_assignment(plan: LogicalPlan, timezone: &SessionTimezone) -> Result<LogicalPlan> {
    let LogicalPlan::Projection(assignment) = plan else {
        return Ok(plan);
    };

    let mut exprs = assignment.expr.clone();
    let mut changed = false;
    for expr in &mut exprs {
        changed |= retarget_assignment_cast(
            expr,
            assignment.input.schema(),
            Some(assignment.input.as_ref()),
            timezone,
        )?;
    }

    // The planner types `VALUES` against the target table, so the assignment
    // cast lands inside the `Values` rows instead of on the projection above.
    let mut input = assignment.input.clone();
    if let LogicalPlan::Values(values) = assignment.input.as_ref()
        && let Some(rewritten) = rewrite_values(values, timezone)?
    {
        input = Arc::new(LogicalPlan::Values(rewritten));
        changed = true;
    }

    if !changed {
        return Ok(LogicalPlan::Projection(assignment));
    }
    Projection::try_new(exprs, input).map(LogicalPlan::Projection)
}

fn rewrite_values(values: &Values, timezone: &SessionTimezone) -> Result<Option<Values>> {
    let mut rewritten = values.clone();
    let mut changed = false;
    for row in &mut rewritten.values {
        for expr in row.iter_mut() {
            changed |= retarget_assignment_cast(expr, &values.schema, None, timezone)?;
        }
    }

    Ok(changed.then_some(rewritten))
}

/// Reinterprets one assignment cast, returning whether it was rewritten.
///
/// `source_plan` is the projection's input, used to resolve a literal behind a
/// column reference; `Values` rows carry their expression inline and pass `None`.
fn retarget_assignment_cast(
    expr: &mut Expr,
    schema: &DFSchemaRef,
    source_plan: Option<&LogicalPlan>,
    timezone: &SessionTimezone,
) -> Result<bool> {
    let expr = unalias_mut(expr);
    let Expr::Cast(Cast {
        expr: source,
        field,
    }) = expr
    else {
        return Ok(false);
    };
    let DataType::Timestamp(unit, None) = field.data_type() else {
        return Ok(false);
    };
    let unit = *unit;

    if !matches!(
        source.get_type(schema)?,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Ok(false);
    }

    // Fold literals with the same parser the plain `INSERT ... VALUES` path
    // uses, so a given string means the same thing however it reaches a column.
    // The parsers disagree on ambiguous local times: this one resolves them,
    // arrow rejects them.
    let folded = source_literal(source.as_ref(), source_plan)
        .and_then(|literal| convert_literal(&literal, unit, &timezone.parsed));
    if let Some(folded) = folded {
        *expr = folded;
        return Ok(true);
    }

    let source = source.as_ref().clone();
    *expr = Expr::Cast(Cast::new(
        Box::new(Expr::Cast(Cast::new(
            Box::new(source),
            DataType::Timestamp(unit, Some(timezone.name.clone())),
        ))),
        DataType::Timestamp(unit, None),
    ));
    Ok(true)
}

fn source_literal(source: &Expr, source_plan: Option<&LogicalPlan>) -> Option<ScalarValue> {
    match source {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Column(column) => {
            let plan = source_plan?;
            let index = plan.schema().maybe_index_of_column(column)?;
            lineage_literal(plan, index).cloned()
        }
        _ => None,
    }
}

/// Resolves a literal when every row carries the same value at `output_idx`.
///
/// Read-only: the literal is folded into the assignment above, so nodes that
/// drop, reorder or deduplicate rows can be traversed — none of them changes
/// the value a surviving row carries, and folding above them leaves their keys
/// on the original strings.
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

fn convert_literal(value: &ScalarValue, unit: TimeUnit, timezone: &Timezone) -> Option<Expr> {
    let ScalarValue::Utf8(Some(value)) = value else {
        return None;
    };
    cast_string_to_timestamp(value, &DataType::Timestamp(unit, None), Some(timezone))
        .ok()
        .filter(|value| !value.is_null())
        .map(|value| Expr::Literal(value, None))
}

fn unalias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(Alias { expr, .. }) => unalias(expr),
        expr => expr,
    }
}

fn unalias_mut(expr: &mut Expr) -> &mut Expr {
    match expr {
        Expr::Alias(Alias { expr, .. }) => unalias_mut(expr),
        expr => expr,
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::DFSchema;
    use datafusion_expr::expr::Placeholder;

    use super::*;

    fn shanghai() -> SessionTimezone {
        let parsed = Timezone::from_tz_string("Asia/Shanghai").unwrap();
        SessionTimezone {
            name: Arc::from(parsed.to_string()),
            parsed,
        }
    }

    /// A prepared `INSERT ... VALUES (?)` arrives here as a cast over an untyped
    /// placeholder, which must survive for parameter substitution.
    #[test]
    fn test_untyped_placeholder_assignment_is_left_alone() {
        let schema = Arc::new(DFSchema::empty());
        let mut expr = Expr::Cast(Cast::new(
            Box::new(Expr::Placeholder(Placeholder::new_with_field(
                "$1".to_string(),
                None,
            ))),
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ));
        let original = expr.clone();

        let changed = retarget_assignment_cast(&mut expr, &schema, None, &shanghai()).unwrap();

        assert!(!changed);
        assert_eq!(expr, original);
    }
}
