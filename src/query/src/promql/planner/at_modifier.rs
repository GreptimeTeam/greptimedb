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

//! Anchored selector planning and replay for the PromQL `@` modifier.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{Column, Expr as DfExpr};
use promql::extension_plan::{InstantManipulate, Millisecond, SeriesDivide};
use promql_parser::parser::{
    AtModifier, Call, Expr as PromExpr, MatrixSelector, Offset, ParenExpr,
};
use snafu::{OptionExt, ResultExt, ensure};

use crate::promql::error::{
    AtModifierTimestampOutOfRangeSnafu, DataFusionPlanningSnafu, Result, TimeIndexNotFoundSnafu,
};
use crate::promql::planner::PromPlanner;
use crate::query_engine::QueryEngineState;

impl PromPlanner {
    /// Resolve the `@` modifier of a vector or matrix selector into the timestamp its sample
    /// window is anchored at, in milliseconds since the Unix epoch.
    ///
    /// Prometheus semantics:
    /// - `@ <unix_ts>` anchors at the given timestamp,
    /// - `@ start()` / `@ end()` anchor at the evaluation range of the whole statement,
    /// - `offset` shifts the anchor backwards: the window ends at `anchor - offset`.
    ///
    /// Returns `None` when the selector has no `@` modifier.
    fn at_ref_time(
        &self,
        at: &Option<AtModifier>,
        offset: &Option<Offset>,
    ) -> Result<Option<Millisecond>> {
        let anchor = match at {
            None => return Ok(None),
            Some(AtModifier::Start) => self.ctx.stmt_start,
            Some(AtModifier::End) => self.ctx.stmt_end,
            Some(AtModifier::At(time)) => Self::system_time_to_millis(time)?,
        };
        Ok(Some(Self::anchor_sub(anchor, Self::offset_millis(offset))?))
    }

    /// Subtracts `rhs` from `lhs` on the millisecond timeline of an `@` anchor.
    ///
    /// A negative result is valid: `@` and `offset` accept timestamps before the Unix epoch. A
    /// result outside the representable millisecond range is rejected like an unrepresentable
    /// anchor ([`Self::system_time_to_millis`]) instead of clamping it, so the same class of
    /// input always gets the same answer.
    pub(super) fn anchor_sub(lhs: Millisecond, rhs: Millisecond) -> Result<Millisecond> {
        lhs.checked_sub(rhs)
            .with_context(|| AtModifierTimestampOutOfRangeSnafu {
                timestamp: format!("{}ms - {}ms", lhs, rhs),
            })
    }

    /// The offset a selector with an `@` modifier is evaluated with.
    ///
    /// Prometheus anchors such a selector by rewriting its offset to `eval_time - anchor`
    /// (`setOffsetForAtModifier`), so that the selector always selects its samples around `anchor`
    /// regardless of the step being evaluated. `eval_time` is the start of the evaluation the
    /// selector belongs to, which is [`Self::start`].
    ///
    /// Returns `None` when the selector has no `@` modifier.
    pub(super) fn at_modifier_offset(
        &self,
        at: &Option<AtModifier>,
        offset: &Option<Offset>,
    ) -> Result<Option<Millisecond>> {
        let Some(anchor) = self.at_ref_time(at, offset)? else {
            return Ok(None);
        };
        Ok(Some(Self::anchor_sub(self.ctx.start, anchor)?))
    }

    /// Whether `expr` is a call that has to be evaluated once for the whole grid, because it folds
    /// a range selector anchored by `@` — the range argument of the call's parser signature, which
    /// is a [`MatrixSelector`] here; only a call with such an argument can take this path, so no
    /// function-name registry is involved.
    ///
    /// This is the shape that needs Prometheus' `StepInvariantExpr` the most: a range function such
    /// as `rate` derives its result from the evaluation instant it is called at, so folding the
    /// window once per step would let the outer evaluation grid change the result of a window that
    /// `@` fixed. Evaluated once, at the start of the grid, the rewritten offset of
    /// [`Self::at_modifier_offset`] places the anchor at that instant, and [`Self::replay_over_grid`]
    /// reports the result at every step.
    ///
    /// Unlike Prometheus, which wraps the whole step-invariant subtree (`preprocessExprHelper`),
    /// only the call itself is promoted here. The operators above it are not: they are still planned
    /// at every step over the replayed result, which is safe for the row-wise ones and keeps the
    /// promotion root narrow. The promotion root has to stay a call over one range selector, because
    /// the replay needs one series per batch ([`Self::series_divide_plan`]) and only such a call
    /// guarantees that the rows it emits still describe the series it was divided by. The operators
    /// left out — an aggregation, a join, or a label rewriting call such as `label_join` — mix or
    /// re-label the rows of different series, so they are unsafe as promotion roots even though
    /// evaluating them after the replay is fine. A call whose input is an anchored *instant*
    /// selector (`abs(some_metric @ 300)`) needs no promotion either: the selector anchors and
    /// replays its sample per series on its own, and the call above it is row-wise.
    ///
    /// `predict_linear` is the exception among the range functions: it predicts from the evaluation
    /// instant of each step ([`Self::create_range_eval_ts_expr`]), so it has to stay outside the
    /// promoted subtree and follow the grid. The remaining arguments of the call have to be
    /// literals, since the replay of the promoted result has no second vector input to divide.
    /// Parentheses around the range argument are transparent (`rate((m[5m] @ 300))`), so they are
    /// looked through and the call is promoted as if they were absent. Nothing else of the subtree
    /// is unwrapped, so an outer parenthesis promotes no operator above the call.
    fn promotes_anchored_range_call(expr: &PromExpr) -> bool {
        let PromExpr::Call(Call { func, args }) = expr else {
            return false;
        };
        // See the doc comment: the regression of `predict_linear` follows the evaluation step.
        if func.name == "predict_linear" {
            return false;
        }
        let mut anchored_range = false;
        for arg in &args.args {
            // Parentheses around the range argument are transparent, so the call is promoted the
            // same way for `rate((m[5m] @ 300))` as for `rate(m[5m] @ 300)`. Only the parentheses
            // directly around this one argument are looked through here: the promotion stays
            // confined to a call over one anchored range selector instead of descending into an
            // arbitrary parenthesized subtree.
            let mut arg = arg.as_ref();
            while let PromExpr::Paren(ParenExpr { expr }) = arg {
                arg = expr;
            }
            match arg {
                // The window is pinned by `@`, so every step folds the same samples.
                PromExpr::MatrixSelector(MatrixSelector { vs, .. }) if vs.at.is_some() => {
                    if anchored_range {
                        return false;
                    }
                    anchored_range = true;
                }
                // A literal argument is the same value at every step.
                arg if Self::try_build_literal_expr(arg).is_some() => {}
                _ => return false,
            }
        }
        anchored_range
    }

    /// Plans the anchored range call `prom_expr` ([`Self::promotes_anchored_range_call`]) as a
    /// step-invariant subtree: the call is evaluated on a single evaluation instant (`grid_start`,
    /// the start of the outer evaluation) and its result is then reported at every step of
    /// `[grid_start, ctx.end]` by [`Self::replay_over_grid`].
    ///
    /// This is the planner's counterpart of Prometheus' `StepInvariantExpr` for the one shape it
    /// promotes. Evaluating the call once matters for the functions that derive their result from
    /// the step being evaluated: `rate(m[5m] @ 300)` folds its window around the anchor once, and
    /// the extrapolation boundaries of `rate` must be derived from that same window at every step
    /// instead of following the outer evaluation timestamp.
    ///
    /// Only the call itself is promoted; the operators above it are planned as usual over the
    /// replayed result. The result of the promoted call is split into one series per batch before it
    /// is replayed ([`Self::series_divide_plan`]), because the row-wise projection of the call does
    /// not preserve the batch layout of the selector.
    ///
    /// Returns `None` when `prom_expr` is not such a call, so that the caller plans it as usual.
    /// The selector inside the promoted call keeps its own `@` anchoring (see
    /// [`Self::at_modifier_offset`]), and planning it with `ctx.end == ctx.start` folds its window
    /// once for that single instant instead of expanding it over the grid, which the replay of the
    /// call result above already does.
    pub(super) async fn promote_anchored_range_call(
        &mut self,
        prom_expr: &PromExpr,
        timestamp_fn: bool,
        query_engine_state: &QueryEngineState,
    ) -> Result<Option<LogicalPlan>> {
        let grid_start = self.ctx.start;
        let grid_end = self.ctx.end;
        // An instant query evaluates a single step, so there is nothing to promote.
        if grid_start == grid_end || !Self::promotes_anchored_range_call(prom_expr) {
            return Ok(None);
        }

        // Plan the subtree on a single evaluation instant: every selector below still anchors its
        // window through `@`, and the functions above them derive their result from that one
        // instant. The planner is single-use, so `ctx.end` needs no restore-on-error.
        self.ctx.end = grid_start;
        let anchored = self
            .prom_expr_to_plan_inner(prom_expr, timestamp_fn, query_engine_state)
            .await?;
        self.ctx.end = grid_end;

        let time_index_column =
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: self.ctx.table_name.clone().unwrap_or_default(),
                })?;
        // The replay reads one series per batch; see [`Self::series_divide_plan`] for why the
        // layout of the selector below the promoted call does not survive it.
        let anchored = self.series_divide_plan(anchored, &time_index_column)?;
        Ok(Some(self.replay_over_grid(
            anchored,
            grid_start,
            grid_end,
            time_index_column,
        )))
    }

    /// Convert the timestamp of an `@` modifier into milliseconds since the Unix epoch.
    fn system_time_to_millis(time: &SystemTime) -> Result<Millisecond> {
        let (millis, negative) = match time.duration_since(UNIX_EPOCH) {
            Ok(duration) => (duration.as_millis(), false),
            // The `@` modifier accepts timestamps before the Unix epoch, e.g. `@ -1`.
            Err(err) => (err.duration().as_millis(), true),
        };
        ensure!(
            millis <= i64::MAX as u128,
            AtModifierTimestampOutOfRangeSnafu {
                timestamp: time
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| format!("+{}ms", duration.as_millis()))
                    .unwrap_or_else(|err| format!("-{}ms", err.duration().as_millis())),
            }
        );
        let millis = millis as Millisecond;
        Ok(if negative { -millis } else { millis })
    }

    /// Report the samples of `anchored` at every step of the evaluation grid
    /// `[grid_start, grid_end]`.
    ///
    /// A selector with an `@` modifier is anchored: the sample window is selected once, around the
    /// anchor timestamp, and every evaluation step reports that same window. Prometheus does this
    /// by rewriting the selector's offset to `eval_time - anchor` and only fetching the samples on
    /// the first step (`setOffsetForAtModifier` plus the `refetch` shortcut in `rangeEval`).
    ///
    /// The expansion reuses [`InstantManipulate`] with a lookback that spans the whole grid: every
    /// step then picks the same sample (or the same already computed value, when `anchored` ends
    /// with a function call such as `rate`) and stamps it with the step's timestamp.
    ///
    /// Every input batch of `anchored` must hold exactly one series, because [`InstantManipulate`]
    /// takes a batch as one timeline. A leaf-level replay (`m @ 300`) consumes the [`SeriesDivide`]
    /// of its selector directly. A promoted call is guaranteed that layout by
    /// [`Self::series_divide_plan`], which is why [`Self::promote_anchored_range_call`] splits
    /// its result before calling this method.
    pub(super) fn replay_over_grid(
        &self,
        anchored: LogicalPlan,
        grid_start: Millisecond,
        grid_end: Millisecond,
        time_index_column: String,
    ) -> LogicalPlan {
        if grid_start == grid_end {
            // A single evaluation step: `anchored` is already stamped with that timestamp.
            return anchored;
        }

        let series_key_columns = self.series_key_columns_for_schema(anchored.schema());
        let replayed = InstantManipulate::new(
            grid_start,
            grid_end,
            // The lookback must keep the single anchored sample eligible for every step.
            grid_end - grid_start + 1,
            self.ctx.interval,
            0,
            time_index_column,
            series_key_columns,
            self.ctx.field_columns.first().cloned(),
            anchored,
        );
        LogicalPlan::Extension(Extension {
            node: Arc::new(replayed),
        })
    }

    /// Sorts `input` by its series key and time index and splits it into one batch per series.
    ///
    /// [`InstantManipulate`] reads every input batch as one series (it takes the timeline of the
    /// batch and reports the row selected at every step), so a batch holding several series would
    /// lose all but one of them. A selector establishes that layout with its own [`SeriesDivide`],
    /// but the per-series distribution requirement does not reach a promoted subtree above it:
    /// the row-wise projection of a call sits in between, so the batch boundaries of the selector
    /// are not preserved — in a distributed plan the promoted result can be delivered as one batch
    /// holding every series. Sorting and dividing here restores the layout, exactly like
    /// [`Self::prom_matrix_selector_to_plan`] does for the input of a range function.
    ///
    /// Series keys that are not present in `input` are dropped, since `ctx.tag_columns` may have
    /// drifted from the actual output schema. A plan without any series key column is returned
    /// unchanged: there is nothing to divide by.
    fn series_divide_plan(
        &self,
        input: LogicalPlan,
        time_index_column: &str,
    ) -> Result<LogicalPlan> {
        let series_key_columns = self.series_key_columns_for_schema(input.schema());
        if series_key_columns.is_empty() {
            return Ok(input);
        }

        let mut sort_exprs = series_key_columns
            .iter()
            .map(|name| DfExpr::Column(Column::from_name(name)).sort(true, true))
            .collect::<Vec<_>>();
        sort_exprs.push(DfExpr::Column(Column::from_name(time_index_column)).sort(true, true));
        let sort_plan = LogicalPlanBuilder::from(input)
            .sort(sort_exprs)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                series_key_columns,
                time_index_column.to_string(),
                sort_plan,
            )),
        }))
    }
}
