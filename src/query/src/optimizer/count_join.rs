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

//! Shape-based `AVG(value * weight)` and `SUM(value * weight)`
//! join-aggregation rewrites.
//!
//! Every rewrite turns `... JOIN ... GROUP BY host, area` with a single
//! aggregate into a join of two partial aggregates computed on one side of the
//! join each, with a projection above the join reconstructing the aggregate.
//! Admission is decided by the plan shape alone: the rule never inspects the
//! data its inputs will read.
//!
//! `AVG(value * weight)` / `SUM(value * weight)`: the aggregate over the
//! pairwise products is replaced by `SUM(value) * SUM(weight)` divided by
//! `COUNT(value) * COUNT(weight)` (`AVG`) or used as it is (`SUM`).
//!
//! # Approximation contract
//!
//! The payload rewrite is opt-in rule state and **shape-based**: it does not
//! inspect the payloads, so it cannot know how well they factorize.
//!
//! - For well-conditioned inputs the factorized result matches the unrewritten
//!   pairwise baseline to within ordinary floating-point rounding, roughly
//!   `1e-9` relative.
//! - On inputs with heavy cancellation, intermediate overflow, or
//!   subnormal/extreme magnitudes the factorized `AVG`/`SUM` can differ from the
//!   baseline arbitrarily, in magnitude and in sign.
//!
//! That difference is accepted in this opt-in experiment mode: the opt-in
//! switch and this documentation are the guard, not an admission classifier.
//! Queries whose payloads may be ill-conditioned should keep the switch off.
//!
//! One difference is removed for free: a zero numerator is normalized to `+0.0`
//! (a `CASE` in the reconstruction), so a cancellation the baseline evaluates to
//! `+0.0` cannot surface here as `-0.0`.
//!
//! The rule is intentionally narrow: it matches only the join and group shapes
//! described by `matching_payload_shape`. It is not
//! registered in the default optimizer list and gated by a session extension.

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::sum::{Sum, sum_udaf};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Column, DFSchema, NullEquality, Result, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, BinaryExpr, Case, Cast};
use datafusion_expr::logical_plan::{Aggregate, Join, JoinConstraint, JoinType, Projection};
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, Operator};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

/// Rewrites supported AVG/SUM payload products into partial aggregates.
#[derive(Debug, Default)]
pub(crate) struct CountJoinRewrite;

impl OptimizerRule for CountJoinRewrite {
    fn name(&self) -> &str {
        "factorized_join_agg_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !config.options().extensions.get::<crate::options::FactorizedJoinAggOptions>().is_some_and(|options| options.enabled) {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::Aggregate(agg) = &plan else {
            return Ok(Transformed::no(plan));
        };
        // The payload rewrites are shape-based: they need neither the pair bound
        // nor any data certificate, only the matched join/group shape, and they
        // never materialize the pair count. A single aggregate can match at most
        // one of the two functions.
        for function in [PayloadFunction::Avg, PayloadFunction::Sum] {
            if let Some(matched) = matching_payload_shape(agg, function) {
                return rewrite_payload(agg, &matched, function, config);
            }
        }
        Ok(Transformed::no(plan))
    }
}

/// Rewrites the matched `AVG(value * weight)` or `SUM(value * weight)`
/// aggregate into a join of two partial aggregates whose projection
/// reconstructs the payload function from them.
///
/// The projection computes, from the branch `nf`/`nv`/`sv` (fact) and
/// `nd`/`nw`/`sw` (dim) outputs:
///
/// ```text
/// valid = nv * nw
/// total = CASE WHEN sv * sw = 0.0 THEN 0.0 ELSE sv * sw END
/// AVG   = CASE WHEN valid = 0 THEN NULL ELSE total / CAST(valid AS Float64) END
/// SUM   = CASE WHEN valid = 0 THEN NULL ELSE total END
/// ```
///
/// Both reconstructions return `NULL` for a group without a single non-null
/// product, which is exactly when `valid` is zero: `nv` and `nw` are payload
/// counts, so their product is zero iff one side has no non-null payload and the
/// join therefore has no non-null product. `AVG` over an empty pair set is
/// `NULL` and so is `SUM`.
///
/// `total` normalizes a zero numerator to `+0.0` because the pairwise numerator
/// it must match can only cancel to `+0.0`: for facts `[1, -1]` with weight
/// `[-1]`, the baseline sums the products `-1` and `+1` into `+0.0`, while
/// `SUM(value)` is `+0.0`, `SUM(weight)` is `-1.0` and their naive product is
/// `-0.0`, which would divide into `-0.0` instead of the baseline's `+0.0`. The
/// normalization is free and shape-level: it removes that mismatch without
/// looking at any data. Every other difference between the factorized and the
/// pairwise result is the approximation the module documentation accepts.
fn rewrite_payload(
    agg: &Aggregate,
    matched: &PayloadMatch<'_>,
    function: PayloadFunction,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    let PayloadMatch {
        join,
        left_key,
        right_key,
        right_group,
        fact_payload,
        dim_payload,
    } = matched;

    let fact_rows_name = config.alias_generator().next("partial_nf");
    let fact_payloads_name = config.alias_generator().next("partial_nv");
    let fact_sum_name = config.alias_generator().next("partial_sv");
    let dim_rows_name = config.alias_generator().next("partial_nd");
    let dim_payloads_name = config.alias_generator().next("partial_nw");
    let dim_sum_name = config.alias_generator().next("partial_sw");

    let left = Aggregate::try_new(
        Arc::clone(&join.left),
        vec![Expr::Column(left_key.clone())],
        vec![
            partial_aggregate(count_udaf(), vec![Expr::Literal(datafusion_common::utils::expr::COUNT_STAR_EXPANSION, None)], &fact_rows_name),
            partial_aggregate(
                count_udaf(),
                vec![Expr::Column(fact_payload.clone())],
                &fact_payloads_name,
            ),
            partial_aggregate(
                sum_udaf(),
                vec![Expr::Column(fact_payload.clone())],
                &fact_sum_name,
            ),
        ],
    )?;
    let right = Aggregate::try_new(
        Arc::clone(&join.right),
        vec![
            Expr::Column(right_key.clone()),
            Expr::Column(right_group.clone()),
        ],
        vec![
            partial_aggregate(count_udaf(), vec![Expr::Literal(datafusion_common::utils::expr::COUNT_STAR_EXPANSION, None)], &dim_rows_name),
            partial_aggregate(
                count_udaf(),
                vec![Expr::Column(dim_payload.clone())],
                &dim_payloads_name,
            ),
            partial_aggregate(
                sum_udaf(),
                vec![Expr::Column(dim_payload.clone())],
                &dim_sum_name,
            ),
        ],
    )?;

    let left = Arc::new(LogicalPlan::Aggregate(left));
    let right = Arc::new(LogicalPlan::Aggregate(right));
    let right_area = left.schema().fields().len() + 1;
    let join_keys = (column_at(left.schema(), 0), column_at(right.schema(), 0));
    let rewritten_join = Join::try_new(
        left,
        right,
        vec![(Expr::Column(join_keys.0), Expr::Column(join_keys.1))],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?;

    let partial = |name: &str| Expr::Column(Column::new_unqualified(name));
    let valid = partial(&fact_payloads_name) * partial(&dim_payloads_name);
    let product = partial(&fact_sum_name) * partial(&dim_sum_name);
    // `total`: the signed-zero normalization described on this function.
    let total = Expr::Case(Case::new(
        None,
        vec![(
            Box::new(is_zero(product.clone(), float_literal(0.0))),
            Box::new(float_literal(0.0)),
        )],
        Some(Box::new(product)),
    ));
    let numerator = Expr::Case(Case::new(
        None,
        vec![(
            Box::new(is_zero(valid.clone(), int_literal(0))),
            Box::new(Expr::Literal(ScalarValue::Float64(None), None)),
        )],
        Some(Box::new(match function {
            PayloadFunction::Avg => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(total),
                Operator::Divide,
                Box::new(Expr::Cast(Cast::new(Box::new(valid), DataType::Float64))),
            )),
            PayloadFunction::Sum => total,
        })),
    ));
    let output = vec![
        Expr::Column(column_at(&rewritten_join.schema, 0)),
        Expr::Column(column_at(&rewritten_join.schema, right_area)),
        numerator.alias(agg.schema.field(2).name()),
    ];
    let projection = Projection::try_new(output, Arc::new(LogicalPlan::Join(rewritten_join)))?;
    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
}

fn partial_aggregate(udaf: Arc<AggregateUDF>, args: Vec<Expr>, alias: &str) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        udaf,
        args,
        false,
        None,
        vec![],
        None,
    ))
    .alias(alias)
}

fn int_literal(value: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(Some(value)), None)
}

fn float_literal(value: f64) -> Expr {
    Expr::Literal(ScalarValue::Float64(Some(value)), None)
}

/// `<lhs> = <rhs>`, used to detect the zero and NULL cases of the `AVG`
/// reconstruction.
fn is_zero(lhs: Expr, rhs: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(lhs), Operator::Eq, Box::new(rhs)))
}

fn column_at(schema: &DFSchema, index: usize) -> Column {
    let (qualifier, field) = schema.qualified_field(index);
    Column::new(qualifier.cloned(), field.name())
}

/// The payload aggregate a rewrite reconstructs from the branch statistics.
///
/// Both functions share the same shape, the same two payload columns and the
/// same branch statistics; they differ only in what the projection computes from
/// the payload sum product: `AVG` divides it by the payload count product, `SUM`
/// is the product itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PayloadFunction {
    /// `AVG(<fact payload> * <dim payload>)`.
    Avg,
    /// `SUM(<fact payload> * <dim payload>)`.
    Sum,
}

/// The pieces of the `AVG`/`SUM(<fact payload> * <dim payload>)` shape the
/// rewrite needs.
struct PayloadMatch<'a> {
    join: &'a Join,
    left_key: Column,
    right_key: Column,
    right_group: Column,
    fact_payload: Column,
    dim_payload: Column,
}

/// Matches the narrow supported `AVG(payload * payload)` or
/// `SUM(payload * payload)` shape and returns the join plus everything the
/// rewrite needs from it.
///
/// Required shape: the same inner `ON` equi-join on `host` with a `fact` table
/// or subquery alias on the left and a `dim` one on the right (so the payload
/// columns are the `fact.value` and `dim.weight` columns the rewrite reads),
/// grouped by the left `host` and the right `area`, aggregated by a single
/// `AVG` — or, for [`PayloadFunction::Sum`], `SUM` — of the product of those two
/// columns, with no `DISTINCT`, `FILTER`, `ORDER BY`, null treatment or extra
/// argument. The product operands may be given in either order and may reach the
/// aggregate through a projection that repeats them.
fn matching_payload_shape(agg: &Aggregate, function: PayloadFunction) -> Option<PayloadMatch<'_>> {
    if agg.group_expr.len() != 2 || agg.aggr_expr.len() != 1 {
        return None;
    }
    let (join, projection) = match agg.input.as_ref() {
        LogicalPlan::Join(join) => (join, None),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Join(join) => (join, Some(projection)),
            _ => return None,
        },
        _ => return None,
    };
    if join.join_type != JoinType::Inner
        || join.join_constraint != JoinConstraint::On
        || join.null_equality != NullEquality::NullEqualsNothing
    {
        return None;
    }
    let (left_key, right_key) = equality_keys(join)?;
    if left_key.name != "host" || right_key.name != "host" {
        return None;
    }
    let aggregate = match &agg.aggr_expr[0] {
        Expr::AggregateFunction(aggregate) => aggregate,
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::AggregateFunction(aggregate) => aggregate,
            _ => return None,
        },
        _ => return None,
    };
    let params = &aggregate.params;
    let inner = aggregate.func.inner();
    let matches_function = match function {
        PayloadFunction::Avg => inner.downcast_ref::<Avg>().is_some(),
        PayloadFunction::Sum => inner.downcast_ref::<Sum>().is_some(),
    };
    if !matches_function
        || params.distinct
        || params.filter.is_some()
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
        || params.args.len() != 1
    {
        return None;
    }
    let (Expr::Column(left_group), Expr::Column(right_group)) =
        (&agg.group_expr[0], &agg.group_expr[1])
    else {
        return None;
    };
    if left_group != left_key
        || !right_group.name.eq_ignore_ascii_case("area")
        || right_group.relation != right_key.relation
        || !fixture_scan(&join.left, "fact")
        || !fixture_scan(&join.right, "dim")
    {
        return None;
    }
    let (first, second) = product_operands(&params.args[0], projection)?;
    Some(PayloadMatch {
        join,
        left_key: left_key.clone(),
        right_key: right_key.clone(),
        right_group: right_group.clone(),
        fact_payload: payload_column(&join.left, "value", first)?,
        dim_payload: payload_column(&join.right, "weight", second)?,
    })
}

/// The two operands of a payload aggregate's argument, which must be a
/// multiplication of two columns of the joined inputs, in either order.
///
/// The projection that may sit between the aggregate and the join either passes
/// the product's operands through under their original names (the argument is
/// then the multiplication itself) or computes the product on its own, in which
/// case the argument is a column of the projection and the multiplication is
/// looked up there.
fn product_operands<'a>(
    argument: &'a Expr,
    projection: Option<&'a Projection>,
) -> Option<(&'a Expr, &'a Expr)> {
    let entry = match argument {
        Expr::BinaryExpr(binary) => binary,
        Expr::Column(column) => {
            let projection = projection?;
            let index = projection
                .schema
                .maybe_index_of_column(column)
                .or_else(|| {
                    projection
                        .schema
                        .index_of_column_by_name(None, &column.name)
                })?;
            let entry = projection.expr.get(index)?;
            match entry {
                Expr::BinaryExpr(binary) => binary,
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::BinaryExpr(binary) => binary,
                    _ => return None,
                },
                _ => return None,
            }
        }
        _ => return None,
    };
    if entry.op != Operator::Multiply {
        return None;
    }
    Some((entry.left.as_ref(), entry.right.as_ref()))
}

/// Resolves `operand` to the `name` column of the input it must come from, so a
/// payload from the wrong side of the join — or any expression other than a
/// plain column — cannot be rewritten.
fn payload_column(input: &LogicalPlan, name: &str, operand: &Expr) -> Option<Column> {
    let Expr::Column(column) = operand else {
        return None;
    };
    if !column.name.eq(name) || !input.schema().has_column(column) {
        return None;
    }
    Some(column.clone())
}

fn equality_keys(join: &Join) -> Option<(&Column, &Column)> {
    if join.on.len() != 1 || join.filter.is_some() {
        return None;
    }
    let (Expr::Column(left), Expr::Column(right)) = (&join.on[0].0, &join.on[0].1) else {
        return None;
    };
    Some((left, right))
}

fn fixture_scan(plan: &LogicalPlan, table: &str) -> bool {
    match plan {
        LogicalPlan::TableScan(scan) => scan.table_name.to_string().eq_ignore_ascii_case(table),
        LogicalPlan::SubqueryAlias(alias) => match alias.input.as_ref() {
            LogicalPlan::TableScan(scan) => scan.table_name.to_string().eq_ignore_ascii_case(table),
            _ => false,
        },
        _ => false,
    }
}
