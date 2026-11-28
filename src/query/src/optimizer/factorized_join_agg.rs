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
//! A side that distributed planning wrapped in a merge scan moves its partial
//! aggregate into the region plan as `__<f>_state`, with the matching
//! `__<f>_merge` left on the frontend: every contributing region then sends one
//! row per group instead of every raw row. See [`branch_aggregate`].
//!
//! Shipping a region-complete partial aggregate instead - one row per group
//! rather than one per group per region - would be cheaper, but nothing a
//! logical rule can read ties the catalog's partition key to the route the query
//! is executed against: a repartition publishes the new key indices before the
//! regions are allocated, and while the two disagree one group can live in
//! several regions. The state/merge split needs no locality argument at all -
//! merging the states is what produces one row per group, whatever the layout -
//! so it is the only construction a plan shape can justify.
//!
//! `AVG(value * weight)` / `SUM(value * weight)`: the aggregate over the
//! pairwise products is replaced by `SUM(value) * SUM(weight)` divided by
//! `COUNT(value) * COUNT(weight)` (`AVG`) or used as it is (`SUM`). The counts
//! are checked individually for zero; for `AVG` they are cast to `Float64`
//! before multiplication to avoid overflowing their `Int64` representation.
//!
//! `COUNT(value * weight)`: a product is `NULL` exactly when one of its
//! operands is, so the number of non-`NULL` products in a group is the product
//! of the two payload counts, `COUNT(value) * COUNT(weight)` — the very count
//! product `AVG` divides by. No payload sum is computed for it, and the
//! reconstructed count is exact, so the approximation contract below does not
//! apply to it. `COUNT(*)` is not part of this rewrite.
//!
//! All three require `Float64` payloads. `AVG`/`SUM` need them for the
//! reconstruction to be exact, and `COUNT` needs them because it never evaluates
//! the product the flat plan evaluates: for a payload type whose product can
//! fail — a `DECIMAL(38, 0)` product is out of range at `10^30 * 10^30` — the
//! rewrite would answer a group the baseline errors on. `Float64` multiplication
//! is total, so it is the one payload type all three can agree on.
//!
//! # Approximation contract
//!
//! The payload rewrite is opt-in rule state and **shape-based**: it does not
//! inspect the payloads, so it cannot know how well they factorize.
//!
//! - Floating-point reassociation means the factorized result can differ from
//!   the pairwise baseline; on inputs with heavy cancellation, intermediate
//!   overflow, or subnormal/extreme magnitudes, it can differ arbitrarily, in
//!   magnitude and in sign.
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
//! described by `matching_payload_shape`. It is registered in the optimizer but
//! remains inert unless enabled by config/session extension state.

use std::collections::HashSet;
use std::sync::Arc;

use crate::dist_plan::MergeScanLogicalPlan;
use arrow_schema::DataType;
use common_function::aggrs::aggr_wrapper::StateMergeHelper;
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::count::{Count, count_udaf};
use datafusion::functions_aggregate::sum::{Sum, sum_udaf};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Column, DFSchema, NullEquality, Result, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, BinaryExpr, Case, Cast};
use datafusion_expr::logical_plan::{Aggregate, Join, JoinConstraint, JoinType, Projection};
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, Operator, UserDefinedLogicalNode};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::query_engine::DefaultSerializer;

/// Rewrites supported AVG/SUM/COUNT payload products into partial aggregates.
#[derive(Debug, Default)]
pub(crate) struct FactorizedJoinAggRule;

impl OptimizerRule for FactorizedJoinAggRule {
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
        if !config
            .options()
            .extensions
            .get::<crate::options::FactorizedJoinAggOptions>()
            .is_some_and(|options| options.enabled)
        {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::Aggregate(agg) = &plan else {
            return Ok(Transformed::no(plan));
        };
        // The payload rewrites are shape-based: they need neither the pair bound
        // nor any data certificate, only the matched join/group shape. A single
        // aggregate can match at most one of the three functions.
        for function in [
            PayloadFunction::Avg,
            PayloadFunction::Sum,
            PayloadFunction::Count,
        ] {
            if let Some(matched) = matching_payload_shape(agg, function) {
                return rewrite_payload(agg, &matched, function, config);
            }
        }
        Ok(Transformed::no(plan))
    }
}

/// Rewrites the matched `AVG(value * weight)`, `SUM(value * weight)` or
/// `COUNT(value * weight)` aggregate into a join of two partial aggregates
/// whose projection reconstructs the payload function from them.
///
/// The projection computes, from the branch `nv`/`sv` (fact) and `nw`/`sw`
/// (dim) outputs:
///
/// ```text
/// no_products = (nv = 0) OR (nw = 0)
/// total       = CASE WHEN sv * sw = 0.0 THEN 0.0 ELSE sv * sw END
/// AVG         = CASE WHEN no_products THEN NULL ELSE total / (CAST(nv AS Float64) * CAST(nw AS Float64)) END
/// SUM         = CASE WHEN no_products THEN NULL ELSE total END
/// COUNT       = nv * nw
/// ```
///
/// `AVG` and `SUM` return `NULL` for a group without a single non-null
/// product, which is exactly when either payload count is zero. `AVG` over an
/// empty pair set is `NULL` and so is `SUM`. Counts are never multiplied as
/// `Int64` there: `AVG` casts each count before multiplication, and `SUM` does
/// not compute a count product.
///
/// `COUNT` needs neither guard: it counts the pairs whose product is non-`NULL`,
/// which is exactly the pairs whose *both* operands are non-`NULL`, i.e.
/// `nv * nw` ([`checked_count_product`]); a group without such a pair counts
/// `0`, never `NULL`. It also needs no division, so it does not reuse the
/// `AVG`/`SUM` reconstruction expression.
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

    let (fact_payloads_name, fact_sum_name, dim_payloads_name, dim_sum_name) =
        partial_aliases(agg, join, config);

    // The two payload counts drive every reconstruction. `AVG` and `SUM` also
    // need the payload sums; `COUNT` does not build them: its payload operands
    // are not constrained to a type, and a payload type without a `SUM` (say a
    // string) would not even plan.
    let mut fact_partials = vec![partial_aggregate(
        count_udaf(),
        vec![Expr::Column(fact_payload.clone())],
        &fact_payloads_name,
    )];
    let mut dim_partials = vec![partial_aggregate(
        count_udaf(),
        vec![Expr::Column(dim_payload.clone())],
        &dim_payloads_name,
    )];
    if function != PayloadFunction::Count {
        fact_partials.push(partial_aggregate(
            sum_udaf(),
            vec![Expr::Column(fact_payload.clone())],
            &fact_sum_name,
        ));
        dim_partials.push(partial_aggregate(
            sum_udaf(),
            vec![Expr::Column(dim_payload.clone())],
            &dim_sum_name,
        ));
    }

    let left = branch_aggregate(
        &join.left,
        &[Expr::Column(left_key.clone())],
        &fact_partials,
    )?;
    let right = branch_aggregate(
        &join.right,
        &[
            Expr::Column(right_key.clone()),
            Expr::Column(right_group.clone()),
        ],
        &dim_partials,
    )?;

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
    let fact_count = partial(&fact_payloads_name);
    let dim_count = partial(&dim_payloads_name);
    let reconstruction = match function {
        // `COUNT(a * b)`: a product is non-`NULL` exactly when both of its
        // operands are, so `nv * nw` pairs carry a non-`NULL` product. No zero
        // guard and no division: `COUNT` of a group without such a pair is `0`
        // rather than `NULL`, and a group only exists here when the join
        // produced at least one pair.
        PayloadFunction::Count => checked_count_product(fact_count, dim_count),
        PayloadFunction::Avg | PayloadFunction::Sum => {
            let no_products = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(is_zero(fact_count.clone(), int_literal(0))),
                Operator::Or,
                Box::new(is_zero(dim_count.clone(), int_literal(0))),
            ));
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
            let value = if function == PayloadFunction::Avg {
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(total),
                    Operator::Divide,
                    Box::new(
                        Expr::Cast(Cast::new(Box::new(fact_count), DataType::Float64))
                            * Expr::Cast(Cast::new(Box::new(dim_count), DataType::Float64)),
                    ),
                ))
            } else {
                total
            };
            Expr::Case(Case::new(
                None,
                vec![(
                    Box::new(no_products),
                    Box::new(Expr::Literal(ScalarValue::Float64(None), None)),
                )],
                Some(Box::new(value)),
            ))
        }
    };
    // The aggregate's own output field and its slot in the aggregate schema are
    // preserved: only the expression producing it changes. The qualifier is part
    // of the field - a parent may read `q.pairs` - so it is carried over too.
    let (output_qualifier, output_field) = agg.schema.qualified_field(2);
    let output = vec![
        Expr::Column(column_at(&rewritten_join.schema, 0)),
        Expr::Column(column_at(&rewritten_join.schema, right_area)),
        reconstruction.alias_qualified(output_qualifier.cloned(), output_field.name().clone()),
    ];
    let projection = Projection::try_new(output, Arc::new(LogicalPlan::Join(rewritten_join)))?;
    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
}

/// One branch of the rewritten join: the branch aggregate over `input`, with its
/// region-side state work moved into the merge scan's remote plan.
///
/// `MergeScanExec` concatenates the streams the regions return and re-aggregates
/// nothing, so a group whose rows live in two regions arrives as two state rows.
/// The `__<f>_merge` aggregate above the scan is what collapses them into one row
/// per group ([`state_merge_aggregate`]), which is why this placement needs no
/// locality proof: see the module documentation for why the whole aggregate
/// cannot take its place.
///
/// A branch the helper cannot split keeps its aggregate above the merge scan,
/// where it sees the merged rows of all regions.
fn branch_aggregate(
    input: &Arc<LogicalPlan>,
    group_expr: &[Expr],
    aggr_expr: &[Expr],
) -> Result<Arc<LogicalPlan>> {
    let local = Aggregate::try_new(Arc::clone(input), group_expr.to_vec(), aggr_expr.to_vec())?;
    if let Some(remote) = remote_aggregate(input, &local) {
        return Ok(remote);
    }
    Ok(Arc::new(LogicalPlan::Aggregate(local)))
}

/// The region-side state work `local` can do, as a merge aggregate over the
/// rebuilt merge scan that outputs `local`'s schema, or `None` when the
/// aggregate has to stay on the frontend.
fn remote_aggregate(input: &LogicalPlan, local: &Aggregate) -> Option<Arc<LogicalPlan>> {
    let LogicalPlan::Extension(extension) = input else {
        return None;
    };
    let scan = extension
        .node
        .as_any()
        .downcast_ref::<MergeScanLogicalPlan>()?;
    // The branch aggregate is written against the columns the scan exposes to
    // the local stage; a scan that exposes anything else keeps it outside.
    if scan.is_placeholder() || scan.schema() != scan.input().schema() {
        return None;
    }
    let remote = Aggregate::try_new(
        Arc::new(scan.input().clone()),
        local.group_expr.clone(),
        local.aggr_expr.clone(),
    )
    .ok()?;
    state_merge_aggregate(scan, remote, local)
}

/// `remote` split into `__<f>_state` inside the region plan and `__<f>_merge`
/// above the merge scan, both still below the join.
///
/// Each region returns one state row per group it holds rows for; the merge
/// aggregate turns those into one row per group.
///
/// [`StateMergeHelper::split_aggr_node`] owns the pair, the function names and
/// the merge semantics; a function it cannot split leaves the branch on the
/// frontend. The merge aggregate cannot be shipped - it carries a UDAF built for
/// these input types - which is why only the state plan goes into the region.
fn state_merge_aggregate(
    scan: &MergeScanLogicalPlan,
    remote: Aggregate,
    local: &Aggregate,
) -> Option<Arc<LogicalPlan>> {
    let split = StateMergeHelper::split_aggr_node(remote).ok()?;
    // The state plan is what a region runs; it has to encode like any other
    // region plan.
    DFLogicalSubstraitConvertor
        .encode(&split.lower_state, DefaultSerializer)
        .ok()?;
    let LogicalPlan::Aggregate(merge) = split.upper_merge else {
        return None;
    };
    let merged = Aggregate::try_new(
        rebuild_merge_scan(scan, split.lower_state),
        merge.group_expr,
        merge.aggr_expr,
    )
    .ok()?;
    // The join above binds the branch by position as well as by name, and the
    // helper only compares its own pair: check the merge aggregate re-bound to
    // the scan that now feeds it against the aggregate it replaces.
    if merged.schema != local.schema {
        return None;
    }
    Some(Arc::new(LogicalPlan::Aggregate(merged)))
}

/// `plan` as the remote plan of a merge scan that keeps `scan`'s identity.
fn rebuild_merge_scan(scan: &MergeScanLogicalPlan, plan: LogicalPlan) -> Arc<LogicalPlan> {
    let mut pushed = MergeScanLogicalPlan::new(plan, false);
    if let Some(id) = scan.remote_dyn_filter_producer_id() {
        pushed = pushed.with_remote_dyn_filter_producer_id(id);
    }
    Arc::new(pushed.into_logical_plan())
}

/// The four partial-aggregate output names, made collision-free against every
/// field name the rewritten aggregates can still see.
///
/// `OptimizerConfig::alias_generator` only keeps generated aliases apart from
/// each other, not from the input fields. A column literally named like a
/// generated alias (say `partial_nv_1`) would then land next to the qualified
/// grouping key of the same name in a partial aggregate's output schema, and
/// `DFSchema` rejects that plan as ambiguous: `Schema contains qualified field
/// name m.partial_nv_1 and unqualified field name partial_nv_1 which would be
/// ambiguous`. Prepending underscores until the name is unused keeps the names
/// deterministic and mutually distinct; the visible column set is finite, so
/// the loop terminates.
fn partial_aliases(
    agg: &Aggregate,
    join: &Join,
    config: &dyn OptimizerConfig,
) -> (String, String, String, String) {
    // `agg.input` is the optional projection above the join, so it already
    // covers both join inputs; the join schemas are collected as well so the
    // used set does not depend on whether that projection is present.
    let mut used: HashSet<String> = HashSet::new();
    for schema in [join.left.schema(), join.right.schema(), agg.input.schema()] {
        used.extend(schema.fields().iter().map(|field| field.name().clone()));
    }
    let mut unique = |prefix: &str| {
        let mut name = config.alias_generator().next(prefix);
        while !used.insert(name.clone()) {
            name = format!("_{name}");
        }
        name
    };
    (
        unique("partial_nv"),
        unique("partial_sv"),
        unique("partial_nw"),
        unique("partial_sw"),
    )
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

/// `nv * nw`, the pair count `COUNT(<fact payload> * <dim payload>)`
/// reconstructs, as a checked `Int64` product.
///
/// DataFusion multiplies integers with `mul_wrapping`, so a plain `Int64`
/// `nv * nw` would wrap an unrepresentable count around to a small or negative
/// value and answer with a silently wrong count. The multiplication is therefore
/// done on `Decimal128(38, 0)`, where the product of two counts is exact (both
/// are non-negative `Int64`s, so the product is below `2^126 < 10^38`), and the
/// exact product is narrowed back to `Int64` by the default, non-safe cast: a
/// product above `i64::MAX` fails the query with an out-of-range cast error
/// instead of wrapping.
fn checked_count_product(fact_count: Expr, dim_count: Expr) -> Expr {
    let exact = |expr: Expr| Expr::Cast(Cast::new(Box::new(expr), DataType::Decimal128(38, 0)));
    let product = exact(fact_count) * exact(dim_count);
    Expr::Cast(Cast::new(Box::new(product), DataType::Int64))
}

fn column_at(schema: &DFSchema, index: usize) -> Column {
    let (qualifier, field) = schema.qualified_field(index);
    Column::new(qualifier.cloned(), field.name())
}

/// The payload aggregate a rewrite reconstructs from the branch statistics.
///
/// The three functions share the same shape, the same two payload columns and
/// the same branch statistics; they differ only in what the projection computes
/// from them: `AVG` divides the payload sum product by the payload count
/// product, `SUM` is the sum product itself, and `COUNT` is the count product
/// alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PayloadFunction {
    /// `AVG(<fact payload> * <dim payload>)`.
    Avg,
    /// `SUM(<fact payload> * <dim payload>)`.
    Sum,
    /// `COUNT(<fact payload> * <dim payload>)`.
    ///
    /// `COUNT(*)` is a separate case: it counts join pairs rather than
    /// non-`NULL` products, so it would need a row count per side, and it keeps
    /// the plain aggregate plan.
    Count,
}

/// The pieces of the `AVG`/`SUM`/`COUNT(<fact payload> * <dim payload>)` shape the
/// rewrite needs.
struct PayloadMatch<'a> {
    join: &'a Join,
    left_key: Column,
    right_key: Column,
    right_group: Column,
    fact_payload: Column,
    dim_payload: Column,
}

/// Matches the narrow supported `AVG(payload * payload)`,
/// `SUM(payload * payload)` or `COUNT(payload * payload)` shape and returns the
/// join plus everything the rewrite needs from it.
///
/// Required shape: an inner `ON` equi-join whose group and payload columns map directly
/// to the respective join sides,
/// grouped by the left `host` and the right `area`, aggregated by a single
/// `AVG` — or, for [`PayloadFunction::Sum`], `SUM`, or for
/// [`PayloadFunction::Count`], `COUNT` — of the product of those two columns,
/// with no `DISTINCT`, `FILTER`, `ORDER BY`, null treatment or extra argument.
/// The product operands may be given in either order and may reach the aggregate
/// through a projection that repeats them.
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
    // Distributed planning wraps each scan in a MergeScan extension and then
    // restores the original scan schema with a projection. Keep the wrapper in
    // the rewritten branches, but validate its hidden remote input is a scan.
    if !is_scan_input(&join.left) || !is_scan_input(&join.right) {
        return None;
    }
    if join.join_type != JoinType::Inner
        || join.join_constraint != JoinConstraint::On
        || join.null_equality != NullEquality::NullEqualsNothing
    {
        return None;
    }
    let (left_key, right_key) = equality_keys(join)?;

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
        PayloadFunction::Count => inner.downcast_ref::<Count>().is_some(),
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
    let left_group = resolve_through_projection(left_group, projection)?;
    let right_group = resolve_through_projection(right_group, projection)?;
    let left_key = resolve_through_projection(left_key, None)?;
    let right_key = resolve_through_projection(right_key, None)?;
    if left_group != left_key
        || right_group == right_key
        || !join.left.schema().has_column(&left_group)
        || !join.right.schema().has_column(&right_group)
    {
        return None;
    }
    // `COUNT(*)` is a separate case: it counts join pairs, so it would need a
    // row count per side instead of the payload counts built here. It plans as
    // `count(<literal or a single column>)`, which the product check below
    // declines, so it keeps the plain aggregate.
    let (first, second, operands_from_projection_expr) =
        product_operands(&params.args[0], projection)?;
    let (first, second) = match operands_from_projection_expr {
        true => (column_expr(first)?, column_expr(second)?),
        false => (
            resolve_expr_through_projection(first, projection)?,
            resolve_expr_through_projection(second, projection)?,
        ),
    };
    let (fact_payload, dim_payload) =
        if join.left.schema().has_column(&first) && join.right.schema().has_column(&second) {
            (first, second)
        } else if join.left.schema().has_column(&second) && join.right.schema().has_column(&first) {
            (second, first)
        } else {
            return None;
        };
    // The payload types have to make the flat plan's product total, so that a
    // row the baseline answers cannot reach a rewrite that never evaluates it.
    // `COUNT` consumes only the payloads' nullness and never multiplies a value,
    // so it cannot fail where the baseline fails, and would answer `1` for a
    // group the baseline errors on: a `DECIMAL(38, 0)` payload product of
    // `10^30 * 10^30` is out of range there. Integer products wrap or error
    // depending on `BinaryExpr::fail_on_overflow`, which is not this rule's to
    // depend on. `Float64` multiplication is total - it follows IEEE 754 and
    // saturates to infinity instead of failing - and is the payload type the
    // value-consuming `AVG`/`SUM` reconstructions are exact for, so all three
    // functions share one requirement.
    if field_type(join.left.schema(), &fact_payload)? != &DataType::Float64
        || field_type(join.right.schema(), &dim_payload)? != &DataType::Float64
    {
        return None;
    }
    Some(PayloadMatch {
        join,
        left_key,
        right_key,
        right_group,
        fact_payload,
        dim_payload,
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
) -> Option<(&'a Expr, &'a Expr, bool)> {
    let (entry, operands_from_projection_expr) = match argument {
        Expr::BinaryExpr(binary) => (binary, false),
        Expr::Column(column) => {
            let projection = projection?;
            let index = projection.schema.maybe_index_of_column(column)?;
            let entry = projection.expr.get(index)?;
            let binary = match entry {
                Expr::BinaryExpr(binary) => binary,
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::BinaryExpr(binary) => binary,
                    _ => return None,
                },
                _ => return None,
            };
            (binary, true)
        }
        _ => return None,
    };
    if entry.op != Operator::Multiply {
        return None;
    }
    Some((
        entry.left.as_ref(),
        entry.right.as_ref(),
        operands_from_projection_expr,
    ))
}

fn is_scan_input(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => is_scan_input(&alias.input),
        // The distributed planner wraps each scan in a MergeScan extension and
        // restores the original schema with a projection; a plain column
        // selection may additionally sit between that wrapper and the join.
        LogicalPlan::Projection(projection) => is_scan_input(&projection.input),
        LogicalPlan::Extension(ext) => ext
            .node
            .as_any()
            .downcast_ref::<MergeScanLogicalPlan>()
            // The merge scan's input can itself carry the wrapped stages (a
            // projection or filter over the original scan), so recurse.
            .is_some_and(|scan| is_scan_input(scan.input())),
        LogicalPlan::TableScan(_) => true,
        LogicalPlan::Filter(filter) => is_scan_input(&filter.input),
        _ => false,
    }
}

/// Resolve columns through projections only when they are exact pass-throughs.
fn column_expr(expr: &Expr) -> Option<Column> {
    let Expr::Column(column) = expr else {
        return None;
    };
    Some(column.clone())
}

fn resolve_expr_through_projection(expr: &Expr, projection: Option<&Projection>) -> Option<Column> {
    let Expr::Column(column) = expr else {
        return None;
    };
    resolve_through_projection(column, projection)
}
fn resolve_through_projection(column: &Column, projection: Option<&Projection>) -> Option<Column> {
    if let Some(projection) = projection {
        let idx = projection.schema.maybe_index_of_column(column)?;
        let expr = projection.expr.get(idx)?;
        let Expr::Column(c) = expr else {
            return None;
        };
        return Some(c.clone());
    }
    Some(column.clone())
}
fn field_type<'a>(schema: &'a DFSchema, column: &Column) -> Option<&'a DataType> {
    schema
        .iter()
        .find(|(q, f)| {
            f.name().as_str() == column.name.as_str()
                && column
                    .relation
                    .as_ref()
                    .is_none_or(|relation| *q == Some(relation))
        })
        .map(|(_, f)| f.data_type())
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
