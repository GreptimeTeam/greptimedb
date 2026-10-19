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

//! Compiles a PromQL AST into a [`NativeTree`].
//!
//! This is the front half of the native evaluation model: it reads the parsed statement and writes
//! down what it means as bound nodes. It runs nothing, and nothing calls it yet, so the subset it
//! accepts is deliberately one shape:
//!
//! ```text
//! <range function>(<selector>[<range>])   e.g. rate(x[5m])
//! ```
//!
//! which becomes `Input` (raw samples) → `Window` (the range function over the statement's grid).
//! Everything else is a [`DataFusionError::NotImplemented`] that names what was found and quotes
//! the shape above, because rejecting a query the compiler does not understand is the only honest
//! alternative to compiling it into something else.
//!
//! The statement's grid and the selector's window are the only numbers derived here; the legality
//! of the tree is not checked here. [`NativeTree::try_new`] is the single authority on that, so a
//! node added later cannot be legal in one place and illegal in another.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::SeriesSchema;
use datatypes::series::time::{EvalGrid, GridSpec};
use promql::native::plan::{
    BoundInput, BoundWindow, NativeExpr, NativeNode, NativeTree, NativeType,
};
use promql::range_eval::{RangeFunction, RangeWindow};
use promql_parser::parser::{Call, EvalStmt, Expr as PromExpr, MatrixSelector, Offset};

use crate::promql::label_values::signed_millis_since_epoch;

/// The one shape this compiler accepts, quoted in every rejection.
const SUPPORTED: &str = "`<range function>(<selector>[<range>])`, for example `rate(x[5m])`";

/// Compiles `stmt` into a tree that reads `input` and folds it with the statement's one window.
///
/// `input` is the raw series schema the caller bound from the table: the AST alone cannot produce
/// it, because it says which series the query reads but not which fields they carry. `ordering` is
/// what the caller's scan guarantees, and the tree records it rather than assuming one.
pub fn compile(
    stmt: &EvalStmt,
    input: Arc<SeriesSchema>,
    ordering: SeriesOrdering,
) -> Result<NativeTree> {
    let PromExpr::Call(Call { func, args }) = &stmt.expr else {
        return Err(not_implemented(&format!("the expression `{}`", stmt.expr)));
    };
    let Some(function) = range_function(func.name) else {
        return Err(not_implemented(&format!(
            "a call to `{}`, which has no native range kernel",
            func.name
        )));
    };
    let [argument] = args.args.as_slice() else {
        return Err(not_implemented(&format!(
            "a call to `{}` with {} argument(s)",
            func.name,
            args.args.len()
        )));
    };
    let PromExpr::MatrixSelector(MatrixSelector { vs, range }) = argument.as_ref() else {
        return Err(not_implemented(&format!(
            "a call to `{}` whose argument is `{}`",
            func.name, argument
        )));
    };
    // An `@`-anchored selector is step-invariant: every step folds the same samples, which the
    // native model would have to replay over the grid. That replay is not part of this subset, so
    // the anchor is refused rather than dropped.
    if vs.at.is_some() {
        return Err(not_implemented(&format!(
            "a call to `{}` over an `@`-anchored selector, whose window is fixed rather than \
             following the evaluation grid and would need the step-invariant replay the native \
             compiler does not implement yet",
            func.name
        )));
    }
    let range_ms = range_millis(range.as_millis())?;
    let offset_ms = selector_offset_ms(&vs.offset)?;
    // The grid is the last thing built: a query refused above, or a window that cannot exist,
    // never pays for a grid nothing would read.
    let output_grid = statement_grid(stmt)?;

    // A range function reports one value per input field, so the layout is the input's.
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::clone(&input.fields),
        eval_grid: Some(Arc::clone(&output_grid)),
    }));

    // One allocation shared by the binding and the node's output: `NativeTree::try_new` compares
    // them by identity, because a compiler that meant one schema has no reason to allocate it
    // twice.
    let raw = NativeType::Samples(input);

    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: raw.clone(),
                    ordering,
                }),
                output: raw,
            }),
            op: BoundWindow {
                function,
                window: RangeWindow {
                    range_ms,
                    offset_ms,
                    // Only `@` sets an anchor, and an anchored selector is rejected above.
                    anchor_ms: None,
                },
                // The input is raw samples, so it has no grid of its own.
                input_grid: None,
                output_grid,
            },
        },
        output,
    })
}

/// The grid the statement is evaluated on: `(end - start) / step + 1` points, the last of which
/// does not pass `end`.
fn statement_grid(stmt: &EvalStmt) -> Result<Arc<EvalGrid>> {
    let start_ms = signed_millis_since_epoch(stmt.start)?;
    let end_ms = signed_millis_since_epoch(stmt.end)?;
    // The step is narrowed before [`GridSpec`] sees it, so an interval to which no millisecond
    // step corresponds is reported as its own value rather than as a step of zero.
    let step_ms = millis(stmt.interval.as_millis()).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "the statement's evaluation step of {}ms is out of range",
            stmt.interval.as_millis()
        ))
    })?;
    // A positive step, ordered endpoints and an inclusive length that fits are the grid's contract,
    // so [`GridSpec`] owns them; re-deriving any of them here would be a second copy of it.
    let spec = GridSpec::try_from_range(start_ms, end_ms, step_ms).map_err(|error| {
        DataFusionError::Plan(format!(
            "the statement's grid of {start_ms}ms..{end_ms}ms at {step_ms}ms per step is invalid: \
             {error}"
        ))
    })?;
    Ok(Arc::new(EvalGrid::try_new(spec).map_err(grid_error)?))
}

/// The window length of a range selector, which has to be longer than zero to select anything.
fn range_millis(range: u128) -> Result<i64> {
    let range_ms = millis(range).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "the range selector's window of {range}ms is out of range"
        ))
    })?;
    if range_ms <= 0 {
        return Err(DataFusionError::Plan(format!(
            "the range selector's window is {range_ms}ms, and a window has to be longer than zero"
        )));
    }
    Ok(range_ms)
}

/// The selector's offset in milliseconds; positive reads earlier samples.
fn selector_offset_ms(offset: &Option<Offset>) -> Result<i64> {
    let Some(offset) = offset else {
        return Ok(0);
    };
    let (magnitude, sign) = match offset {
        Offset::Pos(duration) => (duration.as_millis(), 1),
        Offset::Neg(duration) => (duration.as_millis(), -1),
    };
    millis(magnitude)
        .and_then(|magnitude| magnitude.checked_mul(sign))
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "the selector's offset of {magnitude}ms is out of range"
            ))
        })
}

/// A millisecond magnitude as an `i64`, or `None` when it does not fit.
fn millis(millis: u128) -> Option<i64> {
    i64::try_from(millis).ok()
}

fn grid_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::Plan(format!("the statement's grid is invalid: {error}"))
}

/// The range function a PromQL name denotes, or `None` when this compiler has no kernel for it.
///
/// The map is explicit on purpose: a name that is not listed here is rejected, so supporting one
/// more function is a decision made in one place rather than a consequence of a name looking
/// familiar.
fn range_function(name: &str) -> Option<RangeFunction> {
    match name {
        "rate" => Some(RangeFunction::Rate),
        "increase" => Some(RangeFunction::Increase),
        "delta" => Some(RangeFunction::Delta),
        "irate" => Some(RangeFunction::Irate),
        "idelta" => Some(RangeFunction::Idelta),
        "avg_over_time" => Some(RangeFunction::AvgOverTime),
        "min_over_time" => Some(RangeFunction::MinOverTime),
        "max_over_time" => Some(RangeFunction::MaxOverTime),
        "sum_over_time" => Some(RangeFunction::SumOverTime),
        "count_over_time" => Some(RangeFunction::CountOverTime),
        "last_over_time" => Some(RangeFunction::LastOverTime),
        "present_over_time" => Some(RangeFunction::PresentOverTime),
        "stddev_over_time" => Some(RangeFunction::StddevOverTime),
        "stdvar_over_time" => Some(RangeFunction::StdvarOverTime),
        "resets" => Some(RangeFunction::Resets),
        "changes" => Some(RangeFunction::Changes),
        "deriv" => Some(RangeFunction::Deriv),
        _ => None,
    }
}

/// A rejection of a shape outside [`SUPPORTED`].
fn not_implemented(found: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native PromQL compiler supports only {SUPPORTED} for now, but found {found}"
    ))
}

#[cfg(test)]
mod tests;
