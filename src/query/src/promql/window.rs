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

//! A planner-agnostic evaluation-window helper for incremental rollups.
//!
//! This module deliberately does not know about the legacy or native PromQL
//! planner, a cache, a materialized sink, or Flow. It only turns a span of
//! indices on an already parsed statement's grid into a smaller [`EvalStmt`].

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use promql_parser::parser::{
    AggregateExpr, Call, EvalStmt, Expr as PromExpr, MatrixSelector, ParenExpr, VectorSelector,
    token,
};

use crate::promql::label_values::signed_millis_since_epoch;

/// A statement span that cannot be turned into an evaluation window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowError {
    /// The expression is outside the deliberately small v1 allowlist.
    NotEligible,
    /// The requested grid span is empty or lies outside the original grid.
    InvalidSpan,
    /// The grid arithmetic cannot be represented.
    Overflow,
}

impl fmt::Display for WindowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotEligible => f.write_str("the PromQL expression is not windowable"),
            Self::InvalidSpan => f.write_str("the requested evaluation span is invalid"),
            Self::Overflow => f.write_str("the evaluation span overflows the time grid"),
        }
    }
}

impl std::error::Error for WindowError {}

/// Returns an [`EvalStmt`] for the output grid points whose indices are in
/// `[first, last)`.
///
/// The grid is the one the planners evaluate on: both floor `start` and `end` to
/// whole milliseconds since the epoch ([`signed_millis_since_epoch`]) and lay
/// `start_ms + i * step_ms` out from there, so an instant with a sub-millisecond
/// field is not a grid point of either. The returned statement therefore starts
/// and ends on that integer grid, as whole-millisecond [`SystemTime`]s, and
/// keeps the expression, step, and lookback unchanged. It does not round to
/// wall-clock buckets.
///
/// Every millisecond value planning derives from the statement is validated the
/// way planning validates it, so a statement planning would refuse never yields
/// a window: the step and the lookback have to survive the narrowing to `i64`
/// milliseconds, and the range of an admitted `rate` has to be a positive whole
/// number of them.
///
/// The caller remains responsible for making sure that the evaluator reads the
/// input history each selected point needs (for example the complete range of
/// `rate(x[5m])`); selecting output points does not narrow that history.
pub fn windowed_eval_stmt(
    original: &EvalStmt,
    first: usize,
    last: usize,
) -> Result<EvalStmt, WindowError> {
    EvalGrid::of(original)?.window(original, first, last)
}

/// Whether `expr` is in the deliberately small v1 allowlist.
///
/// Admitted:
/// - a named instant vector selector with no `@` and no `offset`;
/// - `rate(<named matrix selector>)` with no `@` and no `offset`, however the
///   argument is parenthesised;
/// - `sum(<admitted expr>)`, with or without a `by (...)`/`without (...)`
///   grouping clause, however the argument is parenthesised. Grouping chooses
///   which groups a point's value belongs to, not which points the value is
///   computed for, so it does not change what a window of output points
///   recomputes.
///
/// Everything else fails closed: every other aggregate, binaries, subqueries,
/// other functions, dynamic metric discovery, and any `@`/`offset` modifier.
///
/// `sum` is the one aggregate admitted, and the reason is not popularity: it is
/// the shape whose value at an output timestamp folds the complete candidate
/// vector of that timestamp, so recomputing one timestamp's fold answers what
/// the unsplit statement answers there. That is exactly the shape a Prometheus
/// dashboard panel is written in (`sum(rate(x[5m]))`), so it is the one worth
/// admitting first. Everything else stays out until someone makes the same
/// argument for it: a ranking aggregate such as `topk` or `quantile` answers
/// with a subset of the candidate vector rather than a fold of it, and "the
/// fold of a window equals the fold of the whole vector" - the property a caller
/// of this helper checks - is not the property it would need.
pub fn is_windowable(expr: &PromExpr) -> bool {
    admit(expr).is_some()
}

/// The named source table of an expression admitted by the window allowlist.
///
/// Selectors name the table directly; `rate` uses its matrix selector's name,
/// `sum` uses its child's name, and parentheses are transparent. Multi-input,
/// unnamed, and other expressions outside the allowlist fail closed.
pub fn windowed_source_table(expr: &PromExpr) -> Result<&str, WindowError> {
    admit(expr)
        .map(|admitted| admitted.source_table)
        .ok_or(WindowError::NotEligible)
}

/// The maximum input range of an admitted expression, in milliseconds.
///
/// Named selectors return zero; their caller must account for the evaluation
/// lookback separately. A `rate` returns its matrix range, and parentheses and
/// `sum` do not change it. Expressions outside the window allowlist, and ranges
/// that narrow to zero or overflow, fail closed.
pub fn max_input_range_ms(expr: &PromExpr) -> Result<i64, WindowError> {
    let admitted = admit(expr).ok_or(WindowError::NotEligible)?;
    match admitted.matrix {
        Some(matrix) => range_millis(matrix.range),
        None => Ok(0),
    }
}

/// What the v1 allowlist admits, beyond the expression being windowable at all.
struct Admitted<'a> {
    /// The named selector's source table.
    source_table: &'a str,
    /// The range of an admitted `rate`'s matrix selector, which planning narrows
    /// to milliseconds before it folds a window of them.
    matrix: Option<&'a MatrixSelector>,
}

/// The allowlist, in one place: `None` outside it.
fn admit(expr: &PromExpr) -> Option<Admitted<'_>> {
    match expr {
        PromExpr::Paren(ParenExpr { expr }) => admit(expr),
        PromExpr::VectorSelector(selector) => Some(Admitted {
            source_table: named_selector(selector)?,
            matrix: None,
        }),
        PromExpr::Call(Call { func, args }) if func.name == "rate" && args.args.len() == 1 => {
            match unparen(&args.args[0]) {
                PromExpr::MatrixSelector(selector) => Some(Admitted {
                    source_table: named_selector(&selector.vs)?,
                    matrix: Some(selector),
                }),
                _ => None,
            }
        }
        // `sum` without a parameter: the parser gives `topk` and `quantile` the
        // parameter that makes them ranking aggregates, so requiring `None` here
        // keeps the admitted aggregate the folding one it means to be. The
        // grouping modifier is passed over rather than inspected: `by` and
        // `without` move a value between groups, they do not decide which
        // timestamps have one.
        PromExpr::Aggregate(AggregateExpr {
            op,
            expr,
            param: None,
            ..
        }) if op.id() == token::T_SUM => admit(expr),
        _ => None,
    }
}

/// The expression a parenthesised one wraps, however many parentheses were written.
///
/// Parentheses group, so they say nothing about the expression inside them:
/// `rate((x[5m]))` asks for what `rate(x[5m])` asks for. Both planners look
/// through them, so refusing one on the way it was typed would refuse a
/// statement they evaluate.
fn unparen(mut expr: &PromExpr) -> &PromExpr {
    while let PromExpr::Paren(ParenExpr { expr: inner }) = expr {
        expr = inner;
    }
    expr
}

fn named_selector(selector: &VectorSelector) -> Option<&str> {
    if selector.at.is_some()
        || selector.offset.is_some()
        || !selector.matchers.or_matchers.is_empty()
        || selector
            .matchers
            .matchers
            .iter()
            .any(|matcher| matcher.name == "__name__")
    {
        return None;
    }
    selector.name.as_deref()
}

/// A step as the positive whole number of milliseconds planning lays its grid out with.
///
/// The step is the grid, so a step carrying a sub-millisecond part is refused rather
/// than narrowed: the narrowed step would lay out a different grid than the statement
/// asked for, and every point of a window would move with it.
fn step_millis(duration: Duration) -> Result<i64, WindowError> {
    if !duration.subsec_nanos().is_multiple_of(1_000_000) {
        return Err(WindowError::InvalidSpan);
    }
    let millis = i64::try_from(duration.as_millis()).map_err(|_| WindowError::Overflow)?;
    if millis <= 0 {
        return Err(WindowError::InvalidSpan);
    }
    Ok(millis)
}

/// A range selector's window as the positive millisecond count planning narrows it to.
///
/// Planning narrows a range by truncation, so a fractional millisecond folds what its
/// whole milliseconds fold; a positive range that narrows to zero folds nothing and
/// selects nothing, which is not a window this helper can split.
fn range_millis(duration: Duration) -> Result<i64, WindowError> {
    let millis = i64::try_from(duration.as_millis()).map_err(|_| WindowError::Overflow)?;
    if millis <= 0 {
        return Err(WindowError::InvalidSpan);
    }
    Ok(millis)
}

/// The evaluation grid of a statement, in the integers both planners evaluate on.
///
/// A planner-agnostic reader of the grid [`windowed_eval_stmt`] slices: the same
/// integer points, exposed so a caller that has to place spans of its own (a cache
/// of materialized windows, for example) can read them instead of laying out a
/// second grid. It carries no cache, sink, or planner knowledge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvalGrid {
    /// First grid point, in milliseconds since the epoch.
    start_ms: i64,
    /// Positive length of one step, in milliseconds.
    step_ms: i64,
    /// How many points the statement's own `[start, end]` spans.
    points: u64,
    /// Last grid point, in milliseconds since the epoch.
    last_ms: i64,
}

impl EvalGrid {
    /// The grid `stmt` is evaluated on.
    ///
    /// Every millisecond value planning derives from the statement is validated
    /// the way planning validates it, exactly as [`windowed_eval_stmt`] does, so a
    /// statement planning would refuse never yields a grid.
    pub fn of(stmt: &EvalStmt) -> Result<Self, WindowError> {
        max_input_range_ms(&stmt.expr)?;
        let step_ms = step_millis(stmt.interval)?;
        // A lookback is not narrowed further than planning narrows it: both routes
        // keep it in milliseconds, so a value that does not survive that conversion
        // is one they cannot evaluate with.
        i64::try_from(stmt.lookback_delta.as_millis()).map_err(|_| WindowError::Overflow)?;

        Self::layout(stmt, step_ms)
    }

    /// First grid point, in milliseconds since the epoch.
    pub fn start_ms(&self) -> i64 {
        self.start_ms
    }

    /// Positive length of one step, in milliseconds.
    pub fn step_ms(&self) -> i64 {
        self.step_ms
    }

    /// How many points the grid holds.
    pub fn points(&self) -> u64 {
        self.points
    }

    /// Last grid point, in milliseconds since the epoch.
    pub fn last_ms(&self) -> i64 {
        self.last_ms
    }

    /// The millisecond just past the last point, which is the exclusive upper
    /// bound of the span the whole grid covers.
    pub fn end_ms_exclusive(&self) -> Result<i64, WindowError> {
        self.last_ms
            .checked_add(self.step_ms)
            .ok_or(WindowError::Overflow)
    }

    /// The phase of the grid, in milliseconds: the distance from the last
    /// step-aligned instant at or before its first point.
    ///
    /// Two statements with the same step evaluate on the same grid exactly when
    /// their phases are equal, so this is what makes spans laid out from one
    /// statement's grid reusable by another.
    pub fn phase_ms(&self) -> i64 {
        self.start_ms.rem_euclid(self.step_ms)
    }

    /// The timestamp of grid index `index`, in milliseconds since the epoch.
    ///
    /// An index at or past [`EvalGrid::points`] continues the grid rather than
    /// failing, so a caller can name the instant just past its last point.
    pub fn point_ms(&self, index: u64) -> Result<i64, WindowError> {
        let offset = i128::from(index) * i128::from(self.step_ms);
        i64::try_from(i128::from(self.start_ms) + offset).map_err(|_| WindowError::Overflow)
    }

    fn layout(original: &EvalStmt, step_ms: i64) -> Result<Self, WindowError> {
        let start_ms = millis(original.start)?;
        let end_ms = millis(original.end)?;
        if end_ms < start_ms {
            return Err(WindowError::InvalidSpan);
        }
        // `(end - start) / step + 1`, the span an inclusive grid of these
        // endpoints holds, as the planners' own grid builds it.
        let points = (i128::from(end_ms) - i128::from(start_ms)) / i128::from(step_ms) + 1;
        let points = u64::try_from(points).map_err(|_| WindowError::Overflow)?;
        // A grid whose last point does not fit is one no planner builds, so no
        // window of it can be evaluated either.
        let last_ms = Self {
            start_ms,
            step_ms,
            points,
            last_ms: start_ms,
        }
        .point_ms(points - 1)?;
        Ok(Self {
            start_ms,
            step_ms,
            points,
            last_ms,
        })
    }

    fn window(
        &self,
        original: &EvalStmt,
        first: usize,
        last: usize,
    ) -> Result<EvalStmt, WindowError> {
        if first >= last {
            return Err(WindowError::InvalidSpan);
        }
        let first = u64::try_from(first).map_err(|_| WindowError::Overflow)?;
        let last = u64::try_from(last).map_err(|_| WindowError::Overflow)?;
        if last > self.points {
            return Err(WindowError::InvalidSpan);
        }

        Ok(EvalStmt {
            expr: original.expr.clone(),
            start: system_time(self.point_ms(first)?)?,
            end: system_time(self.point_ms(last - 1)?)?,
            interval: original.interval,
            lookback_delta: original.lookback_delta,
        })
    }
}

/// A time as the millisecond the planners read for it.
fn millis(time: SystemTime) -> Result<i64, WindowError> {
    signed_millis_since_epoch(time).map_err(|_| WindowError::Overflow)
}

/// The [`SystemTime`] of a whole millisecond, on either side of the epoch.
///
/// Both planners floor a time to milliseconds, so a window bound carrying a
/// sub-millisecond field would be read as the millisecond below it. Building
/// every bound here keeps the window on the grid instead.
fn system_time(millis: i64) -> Result<SystemTime, WindowError> {
    let magnitude = Duration::from_millis(millis.unsigned_abs());
    let time = if millis >= 0 {
        UNIX_EPOCH.checked_add(magnitude)
    } else {
        UNIX_EPOCH.checked_sub(magnitude)
    };
    time.ok_or(WindowError::Overflow)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use promql_parser::parser::parse;

    use super::*;

    fn stmt(expr: &str) -> EvalStmt {
        let expr = parse(expr).expect("the expression parses");
        EvalStmt {
            expr,
            start: UNIX_EPOCH + Duration::from_secs(1),
            end: UNIX_EPOCH + Duration::from_secs(1) + Duration::from_secs(37 * 4),
            interval: Duration::from_secs(37),
            lookback_delta: Duration::from_secs(300),
        }
    }

    fn stmt_between(expr: &str, start: SystemTime, end: SystemTime, step: Duration) -> EvalStmt {
        EvalStmt {
            start,
            end,
            interval: step,
            ..stmt(expr)
        }
    }

    /// [`stmt`] with the range of the matrix selector its `rate` folds over replaced.
    fn stmt_with_range(expr: &str, range: Duration) -> EvalStmt {
        let mut original = stmt(expr);
        let PromExpr::Call(call) = &mut original.expr else {
            panic!("{expr} is a call")
        };
        let PromExpr::MatrixSelector(matrix) = &mut *call.args.args[0] else {
            panic!("{expr} folds a matrix selector")
        };
        matrix.range = range;
        original
    }

    fn millis(time: SystemTime) -> u128 {
        time.duration_since(UNIX_EPOCH).unwrap().as_millis()
    }

    #[test]
    fn slices_on_the_original_grid_without_rounding() {
        let original = stmt("rate(x[5m])");
        let window = windowed_eval_stmt(&original, 2, 5).unwrap();

        assert_eq!(millis(window.start), 1_000 + 2 * 37_000);
        assert_eq!(millis(window.end), 1_000 + 4 * 37_000);
        assert_eq!(window.interval, original.interval);
        assert_eq!(window.lookback_delta, original.lookback_delta);
        assert_eq!(window.expr, original.expr);
        assert_eq!(millis(original.start), 1_000);
        assert_eq!(millis(original.end), 1_000 + 4 * 37_000);
    }

    #[test]
    fn rejects_empty_or_out_of_grid_spans() {
        let original = stmt("x");
        assert_eq!(
            windowed_eval_stmt(&original, 3, 3).unwrap_err(),
            WindowError::InvalidSpan
        );
        assert_eq!(
            windowed_eval_stmt(&original, 0, 6).unwrap_err(),
            WindowError::InvalidSpan
        );
        assert_eq!(
            windowed_eval_stmt(&original, 5, 6).unwrap_err(),
            WindowError::InvalidSpan
        );

        let mut zero = original.clone();
        zero.interval = Duration::ZERO;
        assert_eq!(
            windowed_eval_stmt(&zero, 0, 1).unwrap_err(),
            WindowError::InvalidSpan
        );

        let mut sub_millisecond = original.clone();
        sub_millisecond.interval = Duration::from_nanos(1);
        assert_eq!(
            windowed_eval_stmt(&sub_millisecond, 0, 1).unwrap_err(),
            WindowError::InvalidSpan
        );

        let mut fractional_millisecond = original.clone();
        fractional_millisecond.interval = Duration::from_nanos(1_500_000);
        assert_eq!(
            windowed_eval_stmt(&fractional_millisecond, 0, 1).unwrap_err(),
            WindowError::InvalidSpan
        );
    }

    /// The grid is the integer one both planners build from their own
    /// millisecond endpoints, not one laid out from the `SystemTime`s: a start
    /// with a sub-millisecond field is not a grid point of either.
    #[test]
    fn windows_start_on_the_planners_millisecond_grid() {
        // `[epoch .. epoch + 2ms]` at `1ms`: planning floors the start to `0` and
        // holds three points, at 0ms, 1ms and 2ms.
        let original = stmt_between(
            "x",
            UNIX_EPOCH + Duration::from_micros(900),
            UNIX_EPOCH + Duration::from_millis(2),
            Duration::from_millis(1),
        );
        let third = windowed_eval_stmt(&original, 2, 3).unwrap();
        assert_eq!(millis(third.start), 2);
        assert_eq!(millis(third.end), 2);
        assert_eq!(third.start, UNIX_EPOCH + Duration::from_millis(2));

        // The first window starts on the floored point, not on `epoch + 900µs`.
        let first = windowed_eval_stmt(&original, 0, 1).unwrap();
        assert_eq!(first.start, UNIX_EPOCH);
        assert_eq!(first.end, UNIX_EPOCH);

        // An end that is not on the grid keeps the last point it can hold.
        let original = stmt_between(
            "x",
            UNIX_EPOCH + Duration::from_micros(900),
            UNIX_EPOCH + Duration::from_micros(3_600),
            Duration::from_millis(1),
        );
        let window = windowed_eval_stmt(&original, 1, 3).unwrap();
        assert_eq!(millis(window.start), 1);
        assert_eq!(millis(window.end), 2);
    }

    /// Times before the epoch floor the other way, and a window keeps that grid.
    #[test]
    fn windows_keep_the_pre_epoch_grid() {
        // `-1.5ms .. 1.5ms` floor to `-2ms .. 1ms`, four points at 1ms.
        let original = stmt_between(
            "x",
            UNIX_EPOCH - Duration::from_micros(1_500),
            UNIX_EPOCH + Duration::from_micros(1_500),
            Duration::from_millis(1),
        );
        let window = windowed_eval_stmt(&original, 1, 3).unwrap();
        assert_eq!(window.start, UNIX_EPOCH - Duration::from_millis(1));
        assert_eq!(window.end, UNIX_EPOCH);

        let last = windowed_eval_stmt(&original, 3, 4).unwrap();
        assert_eq!(last.start, UNIX_EPOCH + Duration::from_millis(1));
        assert_eq!(last.end, UNIX_EPOCH + Duration::from_millis(1));
    }

    /// A step the planners cannot narrow to `i64` milliseconds is one they refuse:
    /// a singleton span must not report a window it would evaluate with a wrapped step.
    #[test]
    fn rejects_a_step_planning_cannot_represent() {
        let mut original = stmt("x");
        original.end = original.start;
        original.interval = Duration::from_millis(i64::MAX as u64 + 1);
        assert_eq!(
            windowed_eval_stmt(&original, 0, 1).unwrap_err(),
            WindowError::Overflow
        );

        // The largest representable step still windows a singleton.
        original.interval = Duration::from_millis(i64::MAX as u64);
        let window = windowed_eval_stmt(&original, 0, 1).unwrap();
        assert_eq!(window.start, window.end);
    }

    #[test]
    fn rejects_a_lookback_planning_cannot_represent() {
        let mut original = stmt("x");
        original.lookback_delta = Duration::from_millis(i64::MAX as u64 + 1);
        assert_eq!(
            windowed_eval_stmt(&original, 0, 1).unwrap_err(),
            WindowError::Overflow
        );
    }

    /// The range a `rate` folds over has to survive planning's narrowing to a
    /// positive millisecond count: a positive range that narrows to zero selects
    /// nothing, so no window of it is one the planners would evaluate.
    #[test]
    fn rejects_a_rate_range_that_narrows_to_no_millisecond() {
        for range in [
            Duration::from_micros(500),
            Duration::from_nanos(1),
            Duration::ZERO,
        ] {
            assert_eq!(
                windowed_eval_stmt(&stmt_with_range("rate(x[5m])", range), 0, 1).unwrap_err(),
                WindowError::InvalidSpan,
                "{range:?}"
            );
        }

        assert_eq!(
            windowed_eval_stmt(
                &stmt_with_range("rate(x[5m])", Duration::from_millis(i64::MAX as u64 + 1)),
                0,
                1
            )
            .unwrap_err(),
            WindowError::Overflow
        );

        // Planning narrows a range by truncation, and the window keeps the range the
        // statement wrote, so a fractional millisecond folds what its whole
        // milliseconds fold rather than being refused here.
        for range in [
            Duration::from_millis(1),
            Duration::from_micros(1_500),
            Duration::from_secs(300),
        ] {
            assert!(
                windowed_eval_stmt(&stmt_with_range("rate(x[5m])", range), 0, 1).is_ok(),
                "{range:?}"
            );
        }
    }

    #[test]
    fn a_grid_reads_the_points_the_windows_are_cut_from() {
        let original = stmt("rate(x[5m])");
        let grid = EvalGrid::of(&original).unwrap();

        assert_eq!(grid.start_ms(), 1_000);
        assert_eq!(grid.step_ms(), 37_000);
        assert_eq!(grid.points(), 5);
        assert_eq!(grid.last_ms(), 1_000 + 4 * 37_000);
        assert_eq!(grid.end_ms_exclusive().unwrap(), 1_000 + 5 * 37_000);
        assert_eq!(grid.phase_ms(), 1_000);

        for index in 0..grid.points() {
            let window = windowed_eval_stmt(&original, index as usize, index as usize + 1)
                .expect("every point of the grid windows");
            assert_eq!(
                grid.point_ms(index).unwrap(),
                i64::try_from(millis(window.start)).unwrap()
            );
        }
        // An index past the last point continues the grid instead of failing, so a
        // caller can name the instant just past the last point.
        assert_eq!(
            grid.point_ms(grid.points()).unwrap(),
            grid.end_ms_exclusive().unwrap()
        );
    }

    #[test]
    fn a_grid_refuses_what_planning_refuses() {
        assert_eq!(
            EvalGrid::of(&stmt("avg(rate(x[5m]))")).unwrap_err(),
            WindowError::NotEligible
        );

        // The admitted `sum` changes nothing about the grid itself: it is the same
        // integer points, read off the same statement.
        assert!(EvalGrid::of(&stmt("sum(rate(x[5m]))")).is_ok());

        let mut backwards = stmt("x");
        backwards.end = backwards.start - Duration::from_secs(1);
        assert_eq!(
            EvalGrid::of(&backwards).unwrap_err(),
            WindowError::InvalidSpan
        );

        let mut sub_millisecond = stmt("x");
        sub_millisecond.interval = Duration::from_micros(1_500);
        assert_eq!(
            EvalGrid::of(&sub_millisecond).unwrap_err(),
            WindowError::InvalidSpan
        );
    }

    /// A grid that does not start on a step boundary keeps its residue as its
    /// phase, which is what makes spans cut from it reusable by another window of
    /// the same step.
    #[test]
    fn a_grid_reports_the_phase_of_an_off_step_start() {
        let original = stmt_between(
            "x",
            UNIX_EPOCH + Duration::from_millis(1_001),
            UNIX_EPOCH + Duration::from_millis(1_001 + 4 * 37_000),
            Duration::from_secs(37),
        );
        let grid = EvalGrid::of(&original).unwrap();
        assert_eq!(grid.phase_ms(), 1_001);
        assert_eq!(grid.points(), 5);
    }

    #[test]
    fn admits_named_selectors_and_rate() {
        assert!(is_windowable(&stmt("x").expr));
        assert!(is_windowable(&stmt("rate(x[5m])").expr));
        assert!(is_windowable(&stmt("((x))").expr));
    }

    #[test]
    fn input_range_follows_the_window_allowlist() {
        for (query, expected) in [
            ("x", 0),
            ("((x))", 0),
            ("sum((sum(x)))", 0),
            ("rate(x[10m])", 600_000),
            ("sum(rate(x[10m]))", 600_000),
            ("((sum((sum(rate(((x[10m]))))))))", 600_000),
            ("sum by (host) (rate(x[10m]))", 600_000),
        ] {
            assert_eq!(
                max_input_range_ms(&stmt(query).expr),
                Ok(expected),
                "{query}"
            );
        }

        for query in [
            "avg(rate(x[10m]))",
            "x offset 1m",
            "rate(x[10m] @ end())",
            "rate(x[10m]) + rate(y[20m])",
            "sum(rate(x[10m]) + rate(y[20m]))",
        ] {
            assert_eq!(
                max_input_range_ms(&stmt(query).expr),
                Err(WindowError::NotEligible),
                "{query}"
            );
        }
    }

    #[test]
    fn input_range_rejects_zero_and_overflow_after_narrowing() {
        for range in [Duration::ZERO, Duration::from_micros(999)] {
            let original = stmt_with_range("rate(x[10m])", range);
            assert_eq!(
                max_input_range_ms(&original.expr),
                Err(WindowError::InvalidSpan)
            );
        }
        let overflow = stmt_with_range("rate(x[10m])", Duration::from_millis(i64::MAX as u64 + 1));
        assert_eq!(
            max_input_range_ms(&overflow.expr),
            Err(WindowError::Overflow)
        );

        let largest = stmt_with_range("rate(x[10m])", Duration::from_millis(i64::MAX as u64));
        assert_eq!(max_input_range_ms(&largest.expr), Ok(i64::MAX));
        let fractional = stmt_with_range("rate(x[10m])", Duration::from_micros(1_500));
        assert_eq!(max_input_range_ms(&fractional.expr), Ok(1));
    }

    #[test]
    fn source_table_follows_the_window_allowlist() {
        for (query, expected) in [
            ("cpu", "cpu"),
            ("cpu{host=\"a\"}", "cpu"),
            ("rate(requests_total[5m])", "requests_total"),
            ("rate(((requests_total[5m])))", "requests_total"),
            ("sum(cpu)", "cpu"),
            ("sum by (host) (rate(requests_total[5m]))", "requests_total"),
            ("((sum((sum(rate(((cpu[5m]))))))))", "cpu"),
        ] {
            assert_eq!(
                windowed_source_table(&stmt(query).expr),
                Ok(expected),
                "{query}"
            );
        }

        for query in [
            "cpu + memory",
            "sum(rate(cpu[5m]) + rate(memory[5m]))",
            "avg(cpu)",
            "abs(cpu)",
            "{host=\"a\"}",
            "{__name__=\"cpu\"}",
            "cpu offset 1m",
            "rate(cpu[5m] @ end())",
            "rate(cpu[5m:30s])",
        ] {
            assert_eq!(
                windowed_source_table(&stmt(query).expr),
                Err(WindowError::NotEligible),
                "{query}"
            );
        }
    }

    /// Parentheses group, so they say nothing about the expression inside them:
    /// both planners look through them around a `rate` argument too.
    #[test]
    fn admits_parenthesised_rate_arguments() {
        for query in ["rate((x[5m]))", "rate(((x[5m])))", "((rate((x[5m]))))"] {
            assert!(is_windowable(&stmt(query).expr), "{query}");
            assert!(
                windowed_eval_stmt(&stmt(query), 0, 1).is_ok(),
                "{query} is windowable and its first point is in the grid"
            );
        }
    }

    #[test]
    fn rejects_modifiers_and_non_allowlisted_expressions() {
        for query in [
            "rate(x[5m] offset 1m)",
            "rate(x[5m] @ end())",
            "rate(x[5m:30s])",
            "rate((x[5m] @ end()))",
            "rate((x[5m] offset 1m))",
            // The one admitted aggregate admits only a windowable child, and only with no
            // parameter of its own.
            "avg(rate(x[5m]))",
            "min(rate(x[5m]))",
            "max(rate(x[5m]))",
            "count(rate(x[5m]))",
            "quantile(0.9, rate(x[5m]))",
            "sum(topk(5, x))",
            "sum(rate(x[5m] @ end()))",
            "sum(rate(x[5m] offset 1m))",
            "sum(x offset 1m)",
            "sum(x @ 123)",
            "sum(rate(x[5m])) + 1",
            "topk(5, x)",
            "{host=\"a\"}",
            "{__name__=\"x\"}",
            "x @ 123",
            "x offset 1m",
        ] {
            assert!(!is_windowable(&stmt(query).expr), "{query}");
            assert_eq!(
                windowed_eval_stmt(&stmt(query), 0, 1).unwrap_err(),
                WindowError::NotEligible,
                "{query}"
            );
        }
    }

    /// `sum` folds the complete candidate vector of each output timestamp, so a window of
    /// output timestamps recomputes what the unsplit statement would have folded there. The
    /// grouping modifier moves a value between groups; it does not decide which timestamps
    /// have one, so `by`/`without` stay admitted.
    #[test]
    fn admits_sum_of_windowable_children() {
        for query in [
            "sum(rate(x[5m]))",
            "sum by (host) (rate(x[5m]))",
            "sum without (host) (rate(x[5m]))",
            "sum((rate(x[5m])))",
            "sum(((rate((x[5m])))))",
            "((sum by (host) ((rate(x[5m])))))",
            "sum(x)",
        ] {
            assert!(is_windowable(&stmt(query).expr), "{query}");
            assert!(
                windowed_eval_stmt(&stmt(query), 0, 1).is_ok(),
                "{query} is windowable and its first point is in the grid"
            );
        }
    }
}
