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

use arc_swap::ArcSwap;
use chrono::{Datelike, Months, NaiveDate};
use common_telemetry::{debug, warn};
use common_time::Timestamp;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use datafusion::common::ScalarValue;
use datafusion::physical_optimizer::pruning::PruningPredicateBuilder;
use datafusion_common::ToDFSchema;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::expr::{Expr, InList};
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{Between, BinaryExpr, Operator};
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::expressions::{
    BinaryExpr as PhysicalBinaryExpr, DynamicFilterPhysicalExpr, is_null,
};
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use datatypes::arrow;
use datatypes::value::scalar_value_to_timestamp;
use snafu::ResultExt;

use crate::error;

#[cfg(test)]
mod stats;

/// Assert the scalar value is not utf8. Returns `None` if it's utf8.
/// In theory, it should be converted to a timestamp scalar value by `TypeConversionRule`.
macro_rules! return_none_if_utf8 {
    ($lit: ident) => {
        if is_string_timestamp_literal($lit) {
            warn!(
                "Unexpected ScalarValue::Utf8 in time range predicate: {:?}. Maybe it's an implicit bug, please report it to https://github.com/GreptimeTeam/greptimedb/issues",
                $lit
            );

            // Make the predicate ineffective.
            return None;
        }
    };
}

pub fn is_string_timestamp_literal(scalar: &ScalarValue) -> bool {
    matches!(
        scalar,
        ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) | ScalarValue::Utf8View(_)
    )
}

/// Reference-counted pointer to a list of logical exprs and a list of dynamic filter physical exprs.
#[derive(Debug, Clone, Default)]
pub struct Predicate {
    /// logical exprs
    exprs: Arc<Vec<Expr>>,
    /// dynamic filter physical exprs, only useful if dynamic filtering is enabled
    ///
    /// They are usually from `TopK` or `Join` operators, and can dynamically filter data during query execution by using current runtime information to further reduce data scanning
    dyn_filters: Arc<ArcSwap<Vec<Arc<DynamicFilterPhysicalExpr>>>>,
}

impl Predicate {
    /// Creates a new `Predicate` by converting logical exprs to physical exprs that can be
    /// evaluated against record batches.
    /// Returns error when failed to convert exprs.
    pub fn new(exprs: Vec<Expr>) -> Self {
        Self {
            exprs: Arc::new(exprs),
            dyn_filters: Arc::new(ArcSwap::new(Arc::new(vec![]))),
        }
    }

    pub fn with_dyn_filters(
        exprs: Vec<Expr>,
        dyn_filters: Vec<Arc<DynamicFilterPhysicalExpr>>,
    ) -> Self {
        Self {
            exprs: Arc::new(exprs),
            dyn_filters: Arc::new(ArcSwap::new(Arc::new(dyn_filters))),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.exprs.is_empty() && self.dyn_filters.load().is_empty()
    }

    /// Adds dynamic filter physical exprs to the existing list.
    pub fn add_dyn_filters(&self, dyn_filters: Vec<Arc<DynamicFilterPhysicalExpr>>) {
        self.dyn_filters.rcu(|existing| {
            let mut new_filters = existing.as_ref().clone();
            new_filters.extend(dyn_filters.clone());
            Arc::new(new_filters)
        });
    }

    /// Removes dynamic filters while preserving the static logical expressions.
    pub fn clear_dyn_filters(&self) {
        self.dyn_filters.store(Arc::new(vec![]));
    }

    /// Returns the logical exprs.
    pub fn exprs(&self) -> &[Expr] {
        &self.exprs
    }

    /// Returns the dynamic filter physical exprs. Notice this return a live dynamic filters which
    /// can change during query execution.
    pub fn dyn_filters(&self) -> Arc<Vec<Arc<DynamicFilterPhysicalExpr>>> {
        self.dyn_filters.load_full()
    }

    /// Returns the dynamic filter as physical exprs. Notice this return a "snapshot" of
    /// dynamic filters at the time of calling this method.
    pub fn dyn_filter_phy_exprs(&self) -> error::Result<Vec<Arc<dyn PhysicalExpr>>> {
        self.dyn_filters
            .load()
            .iter()
            .map(|e| {
                // Pruning must preserve NULL inputs just like decoded dynamic filtering.
                e.children()
                    .into_iter()
                    .try_fold(e.current()?, |expr, child| {
                        Ok(Arc::new(PhysicalBinaryExpr::new(
                            expr,
                            Operator::Or,
                            is_null(child.clone())?,
                        )) as Arc<dyn PhysicalExpr>)
                    })
            })
            .collect::<Result<Vec<_>, _>>()
            .context(error::DatafusionSnafu)
    }

    /// Builds a single physical expr according to provided schema.
    pub fn to_physical_expr(
        expr: &Expr,
        schema: &arrow::datatypes::SchemaRef,
    ) -> error::Result<Arc<dyn PhysicalExpr>> {
        let df_schema = schema
            .clone()
            .to_dfschema_ref()
            .context(error::DatafusionSnafu)?;

        // TODO(hl): `execution_props` provides variables required by evaluation.
        // we may reuse the `execution_props` from `SessionState` once we support
        // registering variables.
        let execution_props = &ExecutionProps::new();

        create_physical_expr(
            expr,
            df_schema.as_ref(),
            execution_props,
            &PhysicalPlanningContext::default(),
        )
        .context(error::DatafusionSnafu)
    }

    /// Builds physical exprs according to provided schema.
    pub fn to_physical_exprs(
        &self,
        schema: &arrow::datatypes::SchemaRef,
    ) -> error::Result<Vec<Arc<dyn PhysicalExpr>>> {
        let dyn_filters = self.dyn_filter_phy_exprs()?;

        Ok(self
            .exprs
            .iter()
            .filter_map(|expr| Self::to_physical_expr(expr, schema).ok())
            .chain(dyn_filters)
            .collect::<Vec<_>>())
    }

    /// Evaluates the predicate against the `stats`.
    /// Returns a vector of boolean values, among which `false` means the row group can be skipped.
    pub fn prune_with_stats<S: PruningStatistics>(
        &self,
        stats: &S,
        schema: &arrow::datatypes::SchemaRef,
    ) -> Vec<bool> {
        let mut res = vec![true; stats.num_containers()];
        let physical_exprs = match self.to_physical_exprs(schema) {
            Ok(expr) => expr,
            Err(e) => {
                warn!(e; "Failed to build physical expr from predicates: {:?}", &self.exprs);
                return res;
            }
        };

        for expr in &physical_exprs {
            match PruningPredicateBuilder::new()
                .with_file_schema(schema.clone())
                .try_build(expr.clone())
            {
                Ok(p) => match p.prune(stats) {
                    Ok(r) => {
                        for (curr_val, res) in r.into_iter().zip(res.iter_mut()) {
                            *res &= curr_val
                        }
                    }
                    Err(e) => {
                        warn!(e; "Failed to prune row groups");
                    }
                },
                Err(e) => {
                    // since dynamic filter exprs could be complex, it's possible that the pruning predicate builder fails to prove anything from it. In that case, we just log it and skip pruning with this expr.
                    debug!("Failed to create pruning predicate for expr: {e:?}");
                }
            }
        }
        res
    }
}

// tests for `build_time_range_predicate` locates in src/query/tests/time_range_filter_test.rs
// since it requires query engine to convert sql to filters.
/// `build_time_range_predicate` extracts time range from logical exprs to facilitate fast
/// time range pruning.
pub fn build_time_range_predicate(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    filters: &[Expr],
) -> TimestampRange {
    let mut res = TimestampRange::min_to_max();
    for expr in filters {
        if let Some(range) = extract_time_range_from_expr(ts_col_name, ts_col_unit, expr) {
            res = res.and(&range);
        }
    }
    res
}

/// The outcome of strictly extracting the time range of `ts_col_name` from a
/// scan's filters. Unlike [`build_time_range_predicate`], which quietly widens
/// to `min_to_max` on anything it cannot parse (fine for pruning), this
/// distinguishes "the column is not filtered at all" from "it is filtered in a
/// way that cannot be safely turned into a range" — for callers whose contract
/// forbids silently falling back to a default window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeRangeExtraction {
    /// No filter references the column.
    Absent,
    /// Every filter referencing the column was folded into this range, which
    /// over-approximates the filters' satisfying set.
    Extracted(TimestampRange),
    /// At least one filter references the column in a shape that cannot be
    /// safely extracted (e.g. under `OR`/`NOT`, or compared to a non-literal).
    Unsupported,
}

/// Strictly extracts the time range of `ts_col_name` from the (implicitly
/// AND-ed) scan filters. See [`TimeRangeExtraction`].
pub fn extract_time_range_strict(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    filters: &[Expr],
) -> TimeRangeExtraction {
    let mut range: Option<TimestampRange> = None;
    for expr in filters {
        if !expr
            .column_refs()
            .iter()
            .any(|column| column.name == ts_col_name)
        {
            continue;
        }
        // Disjunctive shapes (`OR`, `IN`) collapse disjoint ranges into their
        // convex hull, which is not exactly representable as one contiguous
        // range; callers deriving synthetic timestamps from the bounds would
        // emit points inside the gaps.
        if contains_disjunction_over_column(expr, ts_col_name) {
            return TimeRangeExtraction::Unsupported;
        }
        // `Some` from the lenient extractor over-approximates the expression's
        // satisfying set (an `AND` side it cannot parse is dropped, which only
        // widens), so intersecting extracted conjuncts stays an
        // over-approximation.
        match extract_time_range_from_expr(ts_col_name, ts_col_unit, expr) {
            Some(extracted) => {
                range = Some(match range {
                    Some(acc) => acc.and(&extracted),
                    None => extracted,
                });
            }
            None => return TimeRangeExtraction::Unsupported,
        }
    }
    match range {
        Some(range) => TimeRangeExtraction::Extracted(range),
        None => TimeRangeExtraction::Absent,
    }
}

fn contains_disjunction_over_column(expr: &Expr, ts_col_name: &str) -> bool {
    let is_disjunction = |expr: &Expr| {
        matches!(
            expr,
            Expr::BinaryExpr(BinaryExpr {
                op: Operator::Or,
                ..
            }) | Expr::InList(_)
        ) && expr
            .column_refs()
            .iter()
            .any(|column| column.name == ts_col_name)
    };
    expr.exists(|expr| Ok(is_disjunction(expr))).unwrap_or(true)
}

/// Extract time range filter from `WHERE`/`IN (...)`/`BETWEEN` clauses.
/// Return None if no time range can be found in expr.
pub fn extract_time_range_from_expr(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    expr: &Expr,
) -> Option<TimestampRange> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            extract_from_binary_expr(ts_col_name, ts_col_unit, left, op, right)
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => extract_from_between_expr(ts_col_name, ts_col_unit, expr, negated, low, high),
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => extract_from_in_list_expr(ts_col_name, expr, *negated, list),
        _ => None,
    }
}

fn extract_from_binary_expr(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    left: &Expr,
    op: &Operator,
    right: &Expr,
) -> Option<TimestampRange> {
    // `date_trunc(unit, ts_col) <op> literal` does not compare the time index
    // directly, so it needs its own extraction rule.
    if let Some(range) =
        extract_from_date_trunc_comparison(ts_col_name, ts_col_unit, left, op, right)
    {
        return Some(range);
    }

    match op {
        Operator::Eq => get_timestamp_filter(ts_col_name, left, right)
            .and_then(|(ts, _)| ts.convert_to(ts_col_unit))
            .map(TimestampRange::single),
        Operator::Lt => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] < ts_col
                let ts_val = ts.convert_to(ts_col_unit)?.value();
                Some(TimestampRange::from_start(Timestamp::new(
                    ts_val + 1,
                    ts_col_unit,
                )))
            } else {
                // ts_col < [lit]
                ts.convert_to_ceil(ts_col_unit)
                    .map(|ts| TimestampRange::until_end(ts, false))
            }
        }
        Operator::LtEq => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] <= ts_col
                ts.convert_to_ceil(ts_col_unit)
                    .map(TimestampRange::from_start)
            } else {
                // ts_col <= [lit]
                ts.convert_to(ts_col_unit)
                    .map(|ts| TimestampRange::until_end(ts, true))
            }
        }
        Operator::Gt => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] > ts_col
                ts.convert_to_ceil(ts_col_unit)
                    .map(|t| TimestampRange::until_end(t, false))
            } else {
                // ts_col > [lit]
                let ts_val = ts.convert_to(ts_col_unit)?.value();
                Some(TimestampRange::from_start(Timestamp::new(
                    ts_val + 1,
                    ts_col_unit,
                )))
            }
        }
        Operator::GtEq => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] >= ts_col
                ts.convert_to(ts_col_unit)
                    .map(|t| TimestampRange::until_end(t, true))
            } else {
                // ts_col >= [lit]
                ts.convert_to_ceil(ts_col_unit)
                    .map(TimestampRange::from_start)
            }
        }
        Operator::And => {
            // instead of return none when failed to extract time range from left/right, we unwrap the none into
            // `TimestampRange::min_to_max`.
            let left = extract_time_range_from_expr(ts_col_name, ts_col_unit, left)
                .unwrap_or_else(TimestampRange::min_to_max);
            let right = extract_time_range_from_expr(ts_col_name, ts_col_unit, right)
                .unwrap_or_else(TimestampRange::min_to_max);
            Some(left.and(&right))
        }
        Operator::Or => {
            let left = extract_time_range_from_expr(ts_col_name, ts_col_unit, left)?;
            let right = extract_time_range_from_expr(ts_col_name, ts_col_unit, right)?;
            Some(left.or(&right))
        }
        _ => None,
    }
}

fn get_timestamp_filter(ts_col_name: &str, left: &Expr, right: &Expr) -> Option<(Timestamp, bool)> {
    let (col, lit, reverse) = match (left, right) {
        (Expr::Column(column), Expr::Literal(scalar, _)) => (column, scalar, false),
        (Expr::Literal(scalar, _), Expr::Column(column)) => (column, scalar, true),
        _ => {
            return None;
        }
    };
    if col.name != ts_col_name {
        return None;
    }

    return_none_if_utf8!(lit);
    scalar_value_to_timestamp(lit, None).map(|t| (t, reverse))
}

/// Granularity of a `date_trunc` call. Mirrors the units accepted by
/// DataFusion's `date_trunc`, which parses the unit case-insensitively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateTruncUnit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl DateTruncUnit {
    fn parse(unit: &str) -> Option<Self> {
        match unit.to_ascii_lowercase().as_str() {
            "microsecond" => Some(Self::Microsecond),
            "millisecond" => Some(Self::Millisecond),
            "second" => Some(Self::Second),
            "minute" => Some(Self::Minute),
            "hour" => Some(Self::Hour),
            "day" => Some(Self::Day),
            "week" => Some(Self::Week),
            "month" => Some(Self::Month),
            "quarter" => Some(Self::Quarter),
            "year" => Some(Self::Year),
            _ => None,
        }
    }

    /// Width of one bucket in nanoseconds for fixed-width units. Returns `None`
    /// for calendar units, whose width depends on where the bucket falls.
    fn fixed_width_in_nanos(&self) -> Option<i64> {
        const SECOND: i64 = 1_000_000_000;
        Some(match self {
            Self::Microsecond => 1_000,
            Self::Millisecond => 1_000_000,
            Self::Second => SECOND,
            Self::Minute => 60 * SECOND,
            Self::Hour => 60 * 60 * SECOND,
            Self::Day => 24 * 60 * 60 * SECOND,
            Self::Week => 7 * 24 * 60 * 60 * SECOND,
            Self::Month | Self::Quarter | Self::Year => return None,
        })
    }
}

/// Matches `date_trunc(<unit literal>, <ts_col>)` and returns the unit.
/// Returns `None` for any other shape, so unrelated expressions keep their
/// existing extraction result.
fn date_trunc_unit_of(expr: &Expr, ts_col_name: &str) -> Option<DateTruncUnit> {
    let Expr::ScalarFunction(scalar_function) = expr else {
        return None;
    };
    let func_name = scalar_function.func.name();
    if !func_name.eq_ignore_ascii_case("date_trunc") {
        return None;
    }
    // DataFusion's `date_trunc` takes exactly `(unit, timestamp)`: there is no
    // three-argument (timezone) form to handle.
    let [unit, timestamp] = scalar_function.args.as_slice() else {
        return None;
    };
    let Expr::Literal(unit, _) = unit else {
        return None;
    };
    let unit = match unit {
        ScalarValue::Utf8(Some(unit))
        | ScalarValue::Utf8View(Some(unit))
        | ScalarValue::LargeUtf8(Some(unit)) => unit,
        _ => return None,
    };
    let unit = DateTruncUnit::parse(unit)?;
    // Any other argument says nothing about the time index.
    match timestamp {
        Expr::Column(column) if column.name == ts_col_name => Some(unit),
        _ => None,
    }
}

/// Extracts the time range implied by `date_trunc(unit, ts) <op> literal`
/// (also accepts the literal on the left-hand side).
///
/// # Range math
///
/// Let `f(ts) = date_trunc(unit, ts)`, so `f(ts) <= ts < next_boundary(f(ts))`.
/// GreptimeDB's time index is a timezone-less `Timestamp` interpreted as UTC,
/// which matches the UTC semantics `date_trunc` applies to timestamps without
/// timezone metadata.
///
/// - `f >= b` and `f > b` imply `ts >= b`, so `b` is used as an inclusive
///   lower bound (rounding up can only widen the range).
/// - `f <= b` and `f < b` imply `ts < next_boundary_after(b)`, the first bucket
///   boundary strictly after `b`. This is exact for `<=` and over-approximates
///   `<` by at most one bucket, which only widens the extracted range.
/// - `f = b` implies the bucket `[b, next_boundary_after(b))`.
///
/// Everything else (`!=`, non-literal bound, non-time-index column, unsupported
/// unit, ...) returns `None` so the caller falls back to its previous behavior.
fn extract_from_date_trunc_comparison(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    left: &Expr,
    op: &Operator,
    right: &Expr,
) -> Option<TimestampRange> {
    // Normalize to `date_trunc(...) <op> literal`.
    let (unit, lit, op) = match (left, right) {
        (expr, Expr::Literal(lit, _)) => (date_trunc_unit_of(expr, ts_col_name)?, lit, *op),
        (Expr::Literal(lit, _), expr) => (
            date_trunc_unit_of(expr, ts_col_name)?,
            lit,
            // `None` for operators that are not order comparisons, and those
            // are not supported below anyway.
            op.swap()?,
        ),
        _ => return None,
    };

    return_none_if_utf8!(lit);
    let bound = scalar_value_to_timestamp(lit, None)?;

    match op {
        Operator::Gt | Operator::GtEq => bound
            .convert_to_ceil(ts_col_unit)
            .map(TimestampRange::from_start),
        Operator::Lt | Operator::LtEq => date_trunc_next_boundary_after(bound, unit, ts_col_unit)
            .map(|end| TimestampRange::until_end(end, false)),
        Operator::Eq => {
            let start = bound.convert_to_ceil(ts_col_unit)?;
            let end = date_trunc_next_boundary_after(bound, unit, ts_col_unit)?;
            TimestampRange::new(start, end)
        }
        _ => None,
    }
}

/// Returns an exclusive upper bound for `date_trunc(unit, ts) <= bound`, rounded
/// up to `ts_col_unit`.
///
/// Fixed-width units use `bound + one bucket`, which is safe even when `bound`
/// is not aligned to a bucket boundary; calendar units compute the first instant
/// of the next bucket exactly. Rounding up (and adding a whole bucket for an
/// unaligned `bound`) can only widen the extracted range: the result never cuts
/// off a row that satisfies the filter.
fn date_trunc_next_boundary_after(
    bound: Timestamp,
    unit: DateTruncUnit,
    ts_col_unit: TimeUnit,
) -> Option<Timestamp> {
    if let Some(width) = unit.fixed_width_in_nanos() {
        // Adding one bucket width is safe even when `bound` is not aligned to a
        // bucket boundary.
        let nanos = bound
            .convert_to(TimeUnit::Nanosecond)?
            .value()
            .checked_add(width)?;
        return Timestamp::new(nanos, TimeUnit::Nanosecond).convert_to_ceil(ts_col_unit);
    }

    // Calendar units: the width depends on the month, so compute the first
    // instant of the next bucket in the UTC calendar.
    let date = bound.to_chrono_datetime()?.date();
    let (first_month, months_per_bucket) = match unit {
        DateTruncUnit::Month => (date.month(), 1),
        DateTruncUnit::Quarter => (((date.month() - 1) / 3) * 3 + 1, 3),
        DateTruncUnit::Year => (1, 12),
        _ => return None,
    };
    let bucket_start =
        NaiveDate::from_ymd_opt(date.year(), first_month, 1)?.and_hms_opt(0, 0, 0)?;
    let next = bucket_start.checked_add_months(Months::new(months_per_bucket))?;
    Timestamp::from_chrono_datetime(next)?.convert_to_ceil(ts_col_unit)
}

fn extract_from_between_expr(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    expr: &Expr,
    negated: &bool,
    low: &Expr,
    high: &Expr,
) -> Option<TimestampRange> {
    if *negated {
        return None;
    }

    if let Some(unit) = date_trunc_unit_of(expr, ts_col_name) {
        // `date_trunc(unit, ts) BETWEEN low AND high` covers
        // `[low, next_boundary_after(high))`. A side that cannot be extracted
        // widens to `min_to_max`, mirroring the plain column case below.
        let (Expr::Literal(low, _), Expr::Literal(high, _)) = (low, high) else {
            return None;
        };
        return_none_if_utf8!(low);
        return_none_if_utf8!(high);

        let start = scalar_value_to_timestamp(low, None)
            .and_then(|ts| ts.convert_to_ceil(ts_col_unit))
            .map_or_else(TimestampRange::min_to_max, TimestampRange::from_start);
        let end = scalar_value_to_timestamp(high, None)
            .and_then(|ts| date_trunc_next_boundary_after(ts, unit, ts_col_unit))
            .map_or_else(TimestampRange::min_to_max, |ts| {
                TimestampRange::until_end(ts, false)
            });
        return Some(start.and(&end));
    }

    let Expr::Column(col) = expr else {
        return None;
    };
    if col.name != ts_col_name {
        return None;
    }

    match (low, high) {
        (Expr::Literal(low, _), Expr::Literal(high, _)) => {
            return_none_if_utf8!(low);
            return_none_if_utf8!(high);

            let low_opt =
                scalar_value_to_timestamp(low, None).and_then(|ts| ts.convert_to(ts_col_unit));
            let high_opt = scalar_value_to_timestamp(high, None)
                .and_then(|ts| ts.convert_to_ceil(ts_col_unit));
            Some(TimestampRange::new_inclusive(low_opt, high_opt))
        }
        _ => None,
    }
}

/// Extract time range filter from `IN (...)` expr.
fn extract_from_in_list_expr(
    ts_col_name: &str,
    expr: &Expr,
    negated: bool,
    list: &[Expr],
) -> Option<TimestampRange> {
    if negated {
        return None;
    }
    let Expr::Column(col) = expr else {
        return None;
    };
    if col.name != ts_col_name {
        return None;
    }

    if list.is_empty() {
        return Some(TimestampRange::empty());
    }
    let mut init_range = TimestampRange::empty();
    for expr in list {
        if let Expr::Literal(scalar, _) = expr {
            return_none_if_utf8!(scalar);
            // TODO(hl): maybe we should raise an error here since cannot parse
            // timestamp value from in list expr
            let timestamp = scalar_value_to_timestamp(scalar, None)?;
            init_range = init_range.or(&TimestampRange::single(timestamp));
        }
    }
    Some(init_range)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_test_util::temp_dir::{TempDir, create_temp_dir};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{BinaryExpr, Literal, Operator, col, lit};
    use datatypes::arrow::array::Int32Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::arrow_array::StringArray;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::predicate::stats::RowGroupPruningStatistics;

    fn check_build_predicate(expr: Expr, expect: TimestampRange) {
        assert_eq!(
            expect,
            build_time_range_predicate("ts", TimeUnit::Millisecond, &[expr])
        );
    }

    #[test]
    fn test_gt() {
        // ts > 1ms
        check_build_predicate(
            col("ts").gt(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1ms > ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).gt(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), false),
        );

        // 1001us > ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).gt(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // ts > 1001us
        check_build_predicate(
            col("ts").gt(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1s > ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).gt(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), false),
        );

        // ts > 1s
        check_build_predicate(
            col("ts").gt(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(1001)),
        );
    }

    #[test]
    fn test_gt_eq() {
        // ts >= 1ms
        check_build_predicate(
            col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(1)),
        );

        // 1ms >= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).gt_eq(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1001us >= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).gt_eq(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // ts >= 1001us
        check_build_predicate(
            col("ts").gt_eq(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1s >= ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).gt_eq(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), true),
        );

        // ts >= 1s
        check_build_predicate(
            col("ts").gt_eq(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(1000)),
        );
    }

    #[test]
    fn test_lt() {
        // ts < 1ms
        check_build_predicate(
            col("ts").lt(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), false),
        );

        // 1ms < ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).lt(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1001us < ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).lt(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // ts < 1001us
        check_build_predicate(
            col("ts").lt(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1s < ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).lt(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(1001)),
        );

        // ts < 1s
        check_build_predicate(
            col("ts").lt(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), false),
        );
    }

    #[test]
    fn test_lt_eq() {
        // ts <= 1ms
        check_build_predicate(
            col("ts").lt_eq(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1ms <= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).lt_eq(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(1)),
        );

        // 1001us <= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).lt_eq(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // ts <= 1001us
        check_build_predicate(
            col("ts").lt_eq(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1s <= ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).lt_eq(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(1000)),
        );

        // ts <= 1s
        check_build_predicate(
            col("ts").lt_eq(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), true),
        );
    }

    /// Builds `date_trunc(<unit>, <ts_col>)` from the real DataFusion UDF, so the
    /// shape under test matches what the planner produces.
    fn date_trunc_expr(unit: &str, ts_col: &str) -> Expr {
        datafusion::functions::datetime::date_trunc().call(vec![
            lit(ScalarValue::Utf8(Some(unit.to_string()))),
            col(ts_col),
        ])
    }

    /// A UTC instant in millisecond unit, the unit used by the tests below.
    fn utc_ms(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> Timestamp {
        Timestamp::from_chrono_datetime(
            NaiveDate::from_ymd_opt(year, month, day)
                .unwrap()
                .and_hms_opt(hour, minute, second)
                .unwrap(),
        )
        .unwrap()
        .convert_to(TimeUnit::Millisecond)
        .unwrap()
    }

    fn ts_ms(ts: Timestamp) -> Expr {
        lit(ScalarValue::TimestampMillisecond(Some(ts.value()), None))
    }

    fn check_date_trunc_predicate(expr: Expr, expect: TimestampRange) {
        assert_eq!(
            expect,
            build_time_range_predicate("ts", TimeUnit::Millisecond, &[expr])
        );
    }

    #[test]
    fn test_date_trunc_minute_comparisons() {
        let b = utc_ms(2024, 5, 15, 0, 0, 0);
        let next_minute = utc_ms(2024, 5, 15, 0, 1, 0);

        // date_trunc('minute', ts) >= b
        check_date_trunc_predicate(
            date_trunc_expr("minute", "ts").gt_eq(ts_ms(b)),
            TimestampRange::from_start(b),
        );
        // date_trunc('minute', ts) > b: `b` stays a safe inclusive lower bound.
        check_date_trunc_predicate(
            date_trunc_expr("minute", "ts").gt(ts_ms(b)),
            TimestampRange::from_start(b),
        );
        // b <= date_trunc('minute', ts)
        check_date_trunc_predicate(
            ts_ms(b).lt_eq(date_trunc_expr("minute", "ts")),
            TimestampRange::from_start(b),
        );
        // b < date_trunc('minute', ts)
        check_date_trunc_predicate(
            ts_ms(b).lt(date_trunc_expr("minute", "ts")),
            TimestampRange::from_start(b),
        );

        // date_trunc('minute', ts) < b
        check_date_trunc_predicate(
            date_trunc_expr("minute", "ts").lt(ts_ms(b)),
            TimestampRange::until_end(next_minute, false),
        );
        // date_trunc('minute', ts) <= b
        check_date_trunc_predicate(
            date_trunc_expr("minute", "ts").lt_eq(ts_ms(b)),
            TimestampRange::until_end(next_minute, false),
        );
        // b > date_trunc('minute', ts)
        check_date_trunc_predicate(
            ts_ms(b).gt(date_trunc_expr("minute", "ts")),
            TimestampRange::until_end(next_minute, false),
        );
        // b >= date_trunc('minute', ts)
        check_date_trunc_predicate(
            ts_ms(b).gt_eq(date_trunc_expr("minute", "ts")),
            TimestampRange::until_end(next_minute, false),
        );
    }

    #[test]
    fn test_date_trunc_minute_eq() {
        let b = utc_ms(2024, 5, 15, 0, 0, 0);
        let expect = TimestampRange::new(b, utc_ms(2024, 5, 15, 0, 1, 0)).unwrap();
        check_date_trunc_predicate(date_trunc_expr("minute", "ts").eq(ts_ms(b)), expect);
        // The literal may also be on the left-hand side.
        check_date_trunc_predicate(ts_ms(b).eq(date_trunc_expr("minute", "ts")), expect);
    }

    #[test]
    fn test_date_trunc_between() {
        let low = utc_ms(2024, 5, 15, 0, 0, 0);
        let high = utc_ms(2024, 5, 15, 0, 2, 0);
        // [low, high + one minute).
        check_date_trunc_predicate(
            date_trunc_expr("minute", "ts").between(ts_ms(low), ts_ms(high)),
            TimestampRange::new(low, utc_ms(2024, 5, 15, 0, 3, 0)).unwrap(),
        );
    }

    #[test]
    fn test_date_trunc_and_with_time_range() {
        let start = utc_ms(2024, 5, 15, 0, 0, 0);
        let end = utc_ms(2024, 5, 17, 0, 0, 0);
        // The day-truncated lower bound and an ordinary time range are combined with AND.
        check_date_trunc_predicate(
            date_trunc_expr("day", "ts")
                .gt_eq(ts_ms(start))
                .and(col("ts").lt(ts_ms(end))),
            TimestampRange::new(start, end).unwrap(),
        );
    }

    #[test]
    fn test_date_trunc_neq_returns_none() {
        let b = utc_ms(2024, 5, 15, 0, 0, 0);
        check_date_trunc_predicate(
            date_trunc_expr("day", "ts").not_eq(ts_ms(b)),
            TimestampRange::min_to_max(),
        );
    }

    #[test]
    fn test_date_trunc_not_between_returns_none() {
        let b = utc_ms(2024, 5, 15, 0, 0, 0);
        let not_between = Expr::Between(Between {
            expr: Box::new(date_trunc_expr("day", "ts")),
            negated: true,
            low: Box::new(ts_ms(b)),
            high: Box::new(ts_ms(b)),
        });
        check_date_trunc_predicate(not_between, TimestampRange::min_to_max());
    }

    #[test]
    fn test_date_trunc_day_week_upper_bound() {
        // A bound that is not aligned to a bucket: the exclusive end is widened
        // by one bucket (a superset of the satisfying rows, never narrower).
        let b = utc_ms(2023, 12, 31, 12, 0, 0);
        check_date_trunc_predicate(
            date_trunc_expr("day", "ts").lt_eq(ts_ms(b)),
            TimestampRange::until_end(utc_ms(2024, 1, 1, 12, 0, 0), false),
        );

        // A week is a fixed 7 days, so a mid-week bound is widened by a week.
        let b = utc_ms(2023, 12, 27, 6, 0, 0);
        check_date_trunc_predicate(
            date_trunc_expr("week", "ts").lt(ts_ms(b)),
            TimestampRange::until_end(utc_ms(2024, 1, 3, 6, 0, 0), false),
        );
    }

    #[test]
    fn test_date_trunc_month_quarter_year_upper_bound() {
        // month: first instant of the next calendar month (December -> January).
        let b = utc_ms(2023, 12, 15, 10, 30, 0);
        check_date_trunc_predicate(
            date_trunc_expr("month", "ts").lt_eq(ts_ms(b)),
            TimestampRange::until_end(utc_ms(2024, 1, 1, 0, 0, 0), false),
        );

        // quarter: 2023-11-15 belongs to Q4, which ends at 2024-01-01.
        let b = utc_ms(2023, 11, 15, 10, 30, 0);
        check_date_trunc_predicate(
            date_trunc_expr("quarter", "ts").lt(ts_ms(b)),
            TimestampRange::until_end(utc_ms(2024, 1, 1, 0, 0, 0), false),
        );

        // year: 2023 ends at 2024-01-01.
        let b = utc_ms(2023, 6, 1, 8, 0, 0);
        check_date_trunc_predicate(
            date_trunc_expr("year", "ts").lt_eq(ts_ms(b)),
            TimestampRange::until_end(utc_ms(2024, 1, 1, 0, 0, 0), false),
        );

        // An upper bound inside the last month of the representable range must
        // not overflow into a wrong bound: it is simply not extracted.
        let huge = lit(ScalarValue::TimestampNanosecond(Some(i64::MAX), None));
        assert_eq!(
            None,
            extract_time_range_from_expr(
                "ts",
                TimeUnit::Nanosecond,
                &date_trunc_expr("year", "ts").lt_eq(huge)
            )
        );
    }

    #[test]
    fn test_date_trunc_upper_bound_is_superset() {
        // For an unaligned bound the exact first bucket boundary after `b` is the
        // next midnight; the extracted end must not be earlier than that.
        let b = utc_ms(2024, 5, 15, 12, 0, 0);
        let range = build_time_range_predicate(
            "ts",
            TimeUnit::Millisecond,
            &[date_trunc_expr("day", "ts").lt(ts_ms(b))],
        );

        assert!(*range.end() >= Some(utc_ms(2024, 5, 16, 0, 0, 0)));
        // Rows admitted by the filter stay inside the extracted range.
        assert!(range.contains(&utc_ms(2024, 5, 15, 23, 59, 59)));
        assert!(range.contains(&utc_ms(2024, 5, 15, 12, 0, 1)));
        // A row of the following day is definitely outside.
        assert!(!range.contains(&utc_ms(2024, 5, 17, 0, 0, 0)));
    }

    #[test]
    fn test_date_trunc_eq_matches_exact_bucket() {
        // An aligned bound extracts exactly the bucket of that day, so the range
        // is not narrower than the hand-computed set of matching rows.
        let day = utc_ms(2024, 5, 15, 0, 0, 0);
        let next_day = utc_ms(2024, 5, 16, 0, 0, 0);
        let range = build_time_range_predicate(
            "ts",
            TimeUnit::Millisecond,
            &[date_trunc_expr("day", "ts").eq(ts_ms(day))],
        );

        assert_eq!(TimestampRange::new(day, next_day).unwrap(), range);
        assert!(range.contains(&utc_ms(2024, 5, 15, 0, 0, 0)));
        assert!(range.contains(&utc_ms(2024, 5, 15, 23, 59, 59)));
        assert!(!range.contains(&next_day));
        assert!(!range.contains(&utc_ms(2024, 5, 14, 23, 59, 59)));
    }

    #[test]
    fn test_date_trunc_uppercase_unit() {
        let day = utc_ms(2024, 5, 15, 0, 0, 0);
        // DataFusion parses the unit case-insensitively, so `'DAY'` behaves like `'day'`.
        check_date_trunc_predicate(
            date_trunc_expr("DAY", "ts").eq(ts_ms(day)),
            TimestampRange::new(day, utc_ms(2024, 5, 16, 0, 0, 0)).unwrap(),
        );
    }

    #[test]
    fn test_date_trunc_unsupported_shapes_return_none() {
        let b = utc_ms(2024, 5, 15, 0, 0, 0);

        // Truncating a column that is not the time index.
        check_date_trunc_predicate(
            date_trunc_expr("day", "other_ts").gt_eq(ts_ms(b)),
            TimestampRange::min_to_max(),
        );

        // Truncating an expression rather than the bare column.
        let truncated_expr = datafusion::functions::datetime::date_trunc().call(vec![
            lit(ScalarValue::Utf8(Some("day".to_string()))),
            date_trunc_expr("hour", "ts"),
        ]);
        check_date_trunc_predicate(truncated_expr.gt_eq(ts_ms(b)), TimestampRange::min_to_max());

        // A non-literal unit.
        let dynamic_unit =
            datafusion::functions::datetime::date_trunc().call(vec![col("unit"), col("ts")]);
        check_date_trunc_predicate(dynamic_unit.gt_eq(ts_ms(b)), TimestampRange::min_to_max());

        // An unknown unit.
        check_date_trunc_predicate(
            date_trunc_expr("fortnight", "ts").gt_eq(ts_ms(b)),
            TimestampRange::min_to_max(),
        );

        // Three arguments: this DataFusion version has no timezone form.
        let three_args = datafusion::functions::datetime::date_trunc().call(vec![
            lit(ScalarValue::Utf8(Some("day".to_string()))),
            col("ts"),
            lit(ScalarValue::Utf8(Some("UTC".to_string()))),
        ]);
        check_date_trunc_predicate(three_args.gt_eq(ts_ms(b)), TimestampRange::min_to_max());

        // A non-literal bound.
        check_date_trunc_predicate(
            date_trunc_expr("day", "ts").gt_eq(col("t2")),
            TimestampRange::min_to_max(),
        );

        // A string-typed bound is left to the type-conversion rule.
        check_date_trunc_predicate(
            date_trunc_expr("day", "ts").gt_eq(lit("2024-05-15")),
            TimestampRange::min_to_max(),
        );

        // BETWEEN with a non-literal side is not extracted.
        check_date_trunc_predicate(
            date_trunc_expr("day", "ts").between(col("t2"), col("t3")),
            TimestampRange::min_to_max(),
        );
    }

    #[test]
    fn test_extract_time_range_strict() {
        fn ts_lit(ms: i64) -> Expr {
            lit(ScalarValue::TimestampMillisecond(Some(ms), None))
        }
        let extract =
            |filters: &[Expr]| extract_time_range_strict("ts", TimeUnit::Millisecond, filters);
        let range = |start: i64, end: i64| {
            TimestampRange::new(
                Timestamp::new_millisecond(start),
                Timestamp::new_millisecond(end),
            )
            .unwrap()
        };

        // No filter references the column.
        assert_eq!(extract(&[]), TimeRangeExtraction::Absent);
        assert_eq!(
            extract(&[col("host").eq(lit("a"))]),
            TimeRangeExtraction::Absent
        );

        // Both bounds across conjuncts; unrelated filters are ignored.
        assert_eq!(
            extract(&[
                col("ts").gt_eq(ts_lit(1000)),
                col("ts").lt(ts_lit(2000)),
                col("host").eq(lit("a")),
            ]),
            TimeRangeExtraction::Extracted(range(1000, 2000))
        );

        // Lower bound only / upper bound only.
        assert_eq!(
            extract(&[col("ts").gt_eq(ts_lit(1000))]),
            TimeRangeExtraction::Extracted(TimestampRange::from_start(Timestamp::new_millisecond(
                1000
            )))
        );
        assert_eq!(
            extract(&[col("ts").lt(ts_lit(2000))]),
            TimeRangeExtraction::Extracted(TimestampRange::until_end(
                Timestamp::new_millisecond(2000),
                false
            ))
        );

        // BETWEEN is inclusive on both ends.
        assert_eq!(
            extract(&[col("ts").between(ts_lit(1000), ts_lit(2000))]),
            TimeRangeExtraction::Extracted(range(1000, 2001))
        );

        // Equality pins a single point.
        assert_eq!(
            extract(&[col("ts").eq(ts_lit(1500))]),
            TimeRangeExtraction::Extracted(TimestampRange::single(Timestamp::new_millisecond(
                1500
            )))
        );

        // An unparsable side under AND only widens; the extraction stays safe.
        assert_eq!(
            extract(&[col("ts").gt_eq(ts_lit(1000)).and(col("ts").lt(col("t2")))]),
            TimeRangeExtraction::Extracted(TimestampRange::from_start(Timestamp::new_millisecond(
                1000
            )))
        );

        // Contradictory bounds collapse to the empty range, not an error.
        let TimeRangeExtraction::Extracted(empty) =
            extract(&[col("ts").gt_eq(ts_lit(2000)), col("ts").lt(ts_lit(1000))])
        else {
            panic!("expected an extraction");
        };
        assert!(empty.is_empty());

        // Shapes that could widen the satisfying set beyond what is extractable
        // must be refused: OR with an unparsable side, NOT, non-literal bounds.
        assert_eq!(
            extract(&[col("ts").gt(ts_lit(1000)).or(col("host").eq(lit("a")))]),
            TimeRangeExtraction::Unsupported
        );
        assert_eq!(
            extract(&[!col("ts").gt(ts_lit(1000))]),
            TimeRangeExtraction::Unsupported
        );
        assert_eq!(
            extract(&[col("ts").gt_eq(col("t2"))]),
            TimeRangeExtraction::Unsupported
        );
    }

    async fn gen_test_parquet_file(dir: &TempDir, cnt: usize) -> (String, Arc<Schema>) {
        let path = dir
            .path()
            .join("test-prune.parquet")
            .to_string_lossy()
            .to_string();

        let name_field = Field::new("name", DataType::Utf8, true);
        let count_field = Field::new("cnt", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![name_field, count_field]));

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.clone())
            .unwrap();

        let write_props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(10))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(write_props)).unwrap();

        for i in (0..cnt).step_by(10) {
            let name_array = Arc::new(StringArray::from(
                (i..(i + 10).min(cnt))
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>(),
            )) as Arc<_>;
            let count_array = Arc::new(Int32Array::from(
                (i..(i + 10).min(cnt)).map(|i| i as i32).collect::<Vec<_>>(),
            )) as Arc<_>;
            let rb = RecordBatch::try_new(schema.clone(), vec![name_array, count_array]).unwrap();
            writer.write(&rb).unwrap();
        }
        let _ = writer.close().unwrap();
        (path, schema)
    }

    async fn assert_prune(array_cnt: usize, filters: Vec<Expr>, expect: Vec<bool>) {
        let dir = create_temp_dir("prune_parquet");
        let (path, arrow_schema) = gen_test_parquet_file(&dir, array_cnt).await;
        let schema = Arc::new(datatypes::schema::Schema::try_from(arrow_schema.clone()).unwrap());
        let arrow_predicate = Predicate::new(filters);
        let builder = ParquetRecordBatchStreamBuilder::new(
            tokio::fs::OpenOptions::new()
                .read(true)
                .open(path)
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let metadata = builder.metadata().clone();
        let row_groups = metadata.row_groups();

        let stats = RowGroupPruningStatistics::new(row_groups, &schema);
        let res = arrow_predicate.prune_with_stats(&stats, &arrow_schema);
        assert_eq!(expect, res);
    }

    #[test]
    fn test_clear_dyn_filters_preserves_static_predicates() {
        use datafusion_physical_expr::expressions::lit as physical_lit;

        let static_exprs = vec![col("a").eq(lit(1_i32))];
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(vec![], physical_lit(true)));
        let predicate =
            Predicate::with_dyn_filters(static_exprs.clone(), vec![dynamic_filter.clone()]);

        predicate.clear_dyn_filters();
        // An update from the old producer must not put its wrapper back into this execution.
        dynamic_filter.update(physical_lit(false)).unwrap();

        assert_eq!(predicate.exprs(), static_exprs);
        assert!(predicate.dyn_filters().is_empty());
        assert!(predicate.dyn_filter_phy_exprs().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_dynamic_pruning_keeps_null_row_group() {
        use datafusion_physical_expr::expressions::{
            Column as PhysicalColumn, lit as physical_lit,
        };

        let dir = create_temp_dir("dynamic_pruning_nulls");
        let path = dir.path().join("nullable.parquet");
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), None).unwrap();
        for values in [[None, Some(1)], [Some(1), Some(1)], [None, None]] {
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int32Array::from(values.to_vec()))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        let builder =
            ParquetRecordBatchStreamBuilder::new(tokio::fs::File::open(path).await.unwrap())
                .await
                .unwrap();
        let schema = Arc::new(datatypes::schema::Schema::try_from(arrow_schema.clone()).unwrap());
        let stats = RowGroupPruningStatistics::new(builder.metadata().row_groups(), &schema);
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(PhysicalColumn::new("a", 0))],
            physical_lit(true),
        ));
        let predicate = Predicate::with_dyn_filters(vec![], vec![filter.clone()]);
        assert_eq!(
            predicate.prune_with_stats(&stats, &arrow_schema),
            vec![true; 3]
        );
        filter
            .update(
                Predicate::to_physical_expr(
                    &col("a").gt_eq(lit(10_i32)).and(col("a").lt_eq(lit(10_i32))),
                    &arrow_schema,
                )
                .unwrap(),
            )
            .unwrap();
        // NULL inputs must reach decoded filtering; non-NULL misses can still be pruned.
        assert_eq!(
            predicate.prune_with_stats(&stats, &arrow_schema),
            vec![true, false, true],
        );
    }

    #[tokio::test]
    async fn test_clear_dyn_filters_restores_static_row_group_pruning() {
        use datafusion_physical_expr::expressions::{
            Column as PhysicalColumn, lit as physical_lit,
        };

        let dir = create_temp_dir("dynamic_pruning_reset");
        let (path, arrow_schema) = gen_test_parquet_file(&dir, 30).await;
        let schema = Arc::new(datatypes::schema::Schema::try_from(arrow_schema.clone()).unwrap());
        let builder =
            ParquetRecordBatchStreamBuilder::new(tokio::fs::File::open(path).await.unwrap())
                .await
                .unwrap();
        let stats = RowGroupPruningStatistics::new(builder.metadata().row_groups(), &schema);
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(PhysicalColumn::new("cnt", 1))],
            physical_lit(true),
        ));
        let predicate = Predicate::with_dyn_filters(
            vec![col("cnt").gt_eq(lit(10_i32))],
            vec![dynamic_filter.clone()],
        );

        dynamic_filter
            .update(
                Predicate::to_physical_expr(&col("cnt").gt(lit(100_i32)), &arrow_schema).unwrap(),
            )
            .unwrap();
        assert_eq!(
            predicate.prune_with_stats(&stats, &arrow_schema),
            vec![false; 3]
        );

        // Reset after the prior stream is dropped: no dynamic filter remains, while the
        // static predicate still excludes only the first row group.
        predicate.clear_dyn_filters();
        assert_eq!(
            predicate.prune_with_stats(&stats, &arrow_schema),
            vec![false, true, true]
        );
    }

    fn gen_predicate(max_val: i32, op: Operator) -> Vec<Expr> {
        vec![datafusion_expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(datafusion_expr::Expr::Column(Column::from_name("cnt"))),
            op,
            right: Box::new(max_val.lit()),
        })]
    }

    #[tokio::test]
    async fn test_prune_empty() {
        assert_prune(3, vec![], vec![true]).await;
    }

    #[tokio::test]
    async fn test_prune_all_match() {
        let p = gen_predicate(3, Operator::Gt);
        assert_prune(2, p, vec![false]).await;
    }

    #[tokio::test]
    async fn test_prune_gt() {
        let p = gen_predicate(29, Operator::Gt);
        assert_prune(
            100,
            p,
            vec![
                false, false, false, true, true, true, true, true, true, true,
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_prune_eq_expr() {
        let p = gen_predicate(30, Operator::Eq);
        assert_prune(40, p, vec![false, false, false, true]).await;
    }

    #[tokio::test]
    async fn test_prune_neq_expr() {
        let p = gen_predicate(30, Operator::NotEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_gteq_expr() {
        let p = gen_predicate(29, Operator::GtEq);
        assert_prune(40, p, vec![false, false, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_lt_expr() {
        let p = gen_predicate(30, Operator::Lt);
        assert_prune(40, p, vec![true, true, true, false]).await;
    }

    #[tokio::test]
    async fn test_prune_lteq_expr() {
        let p = gen_predicate(30, Operator::LtEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_between_expr() {
        let p = gen_predicate(30, Operator::LtEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_or() {
        // cnt > 30 or cnt < 20
        let e = datafusion_expr::Expr::Column(Column::from_name("cnt"))
            .gt(30.lit())
            .or(datafusion_expr::Expr::Column(Column::from_name("cnt")).lt(20.lit()));
        assert_prune(40, vec![e], vec![true, true, false, true]).await;
    }

    #[tokio::test]
    async fn test_to_physical_expr() {
        let predicate = Predicate::new(vec![
            col("host").eq(lit("host_a")),
            col("ts").gt(lit(ScalarValue::TimestampMicrosecond(Some(123), None))),
        ]);

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "host",
            arrow::datatypes::DataType::Utf8,
            false,
        )]));

        let predicates = predicate.to_physical_exprs(&schema).unwrap();
        assert!(!predicates.is_empty());

        let physical_expr = Predicate::to_physical_expr(&col("host").eq(lit("host_a")), &schema);
        assert!(physical_expr.is_ok());
    }
}
