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
//! accepts is deliberately three shapes:
//!
//! ```text
//! <selector>                                       e.g. x
//! <range function>(<selector>[<range>])            e.g. rate(x[5m])
//! <range function>(<number>, <selector>[<range>])  e.g. quantile_over_time(0.9, x[5m])
//! ```
//!
//! which become `Input` (raw samples) → `Instant` (the last sample within the statement's lookback)
//! and `Input` (raw samples) → `Window` (the range function over the statement's grid). A selector
//! may carry `offset` in either shape and, in the window shape, an `@` anchor. Everything else is a
//! [`DataFusionError::NotImplemented`] that names what was found and quotes the shapes above,
//! because rejecting a query the compiler does not understand is the only honest alternative to
//! compiling it into something else.
//!
//! The statement's grid, the selector's lookback, window, offset and anchor are the only numbers
//! derived here; the legality of the tree is not checked here. [`NativeTree::try_new`] is the
//! single authority on that, so a node added later cannot be legal in one place and illegal in
//! another.

use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::SeriesSchema;
use datatypes::series::time::{EvalGrid, GridSpec};
use promql::native::plan::{
    BoundInput, BoundInstant, BoundWindow, NativeExpr, NativeNode, NativeTree, NativeType,
};
use promql::range_eval::{RangeFunction, RangeWindow};
use promql_parser::parser::{
    AtModifier, Call, EvalStmt, Expr as PromExpr, MatrixSelector, NumberLiteral, Offset,
    VectorSelector,
};

use crate::promql::label_values::signed_millis_since_epoch;

/// The shapes this compiler accepts, quoted in every rejection.
const SUPPORTED: &str = "a bare selector, a range function over a selector — \
                         `<range function>(<selector>[<range>])` — or a parameterised one, \
                         `<range function>(<number>, <selector>[<range>])`, for example `x`, \
                         `rate(x[5m])` or `quantile_over_time(0.9, x[5m])`";

/// Compiles `stmt` into a tree that reads `input` and either selects from it or folds it with the
/// statement's one window.
///
/// `input` is the raw series schema the caller bound from the table, and the caller has to apply the
/// selector's matchers before binding it: `__field__`, `__schema__` and `__database__` select rows
/// and fields the same way they do in the existing planner, and the AST cannot restate that
/// afterwards. `input.fields` is therefore the post-`__field__` field set, not the table's; a caller
/// that binds every field of the table instead compiles a different, wrong window, because
/// `rate(x{__field__="a"}[5m])` over fields `[a, b]` would fold two fields where the planner folds
/// one.
///
/// `ordering` is what the caller's scan guarantees, and the tree records it rather than assuming
/// one. Only `Contiguous` and `KeyThenTime` are accepted: a native range window consumes one series
/// to its last chunk before the next, so chunks of different series may not interleave, and an
/// `Interleaved` declaration is refused here — which is where the refusal belongs, since a tree that
/// declares it under a window cannot be built by this compiler at all. [`NativeTree::validate`]'s
/// window branch checks the same thing, so if both land this refusal comes first and the tree check
/// is a backstop for a tree built some other way.
///
/// The tree carries no metric name and no matchers, so `rate(x{job="a"}[5m])` and `rate(y[5m])`
/// compile to identical trees, and an `EXPLAIN` built on this display cannot tell them apart. That
/// is fine while a tree has one selector; a tree with two inputs will need each `Input` to name its
/// selector.
pub fn compile(
    stmt: &EvalStmt,
    input: Arc<SeriesSchema>,
    ordering: SeriesOrdering,
) -> Result<NativeTree> {
    // A bare selector is the other shape: an instant selection over the same raw input.
    if let PromExpr::VectorSelector(selector) = unparen(&stmt.expr) {
        return instant_selection(stmt, selector, input, ordering);
    }
    let PromExpr::Call(Call { func, args }) = unparen(&stmt.expr) else {
        return Err(not_implemented(&format!("the expression `{}`", stmt.expr)));
    };
    let Some(signature) = range_signature(func.name) else {
        return Err(not_implemented(&format!(
            "a call to `{}`, which has no native range kernel",
            func.name
        )));
    };
    // The count is the signature's, so the rejection names what this function takes rather than
    // what one argument happens to be.
    let expected = signature.parameters() + 1;
    if args.args.len() != expected {
        return Err(not_implemented(&format!(
            "a call to `{}` with {} argument(s), where it takes {expected}: {}",
            func.name,
            args.args.len(),
            signature.arguments()
        )));
    }
    // The parameters come first and the range selector is last, for both signatures: a function
    // without parameters has no `parameters` to read.
    let (parameters, selector) = args.args.split_at(signature.parameters());
    let function = match signature {
        RangeSignature::Selector(function) => function,
        RangeSignature::Scalar(bind) => bind(scalar_parameter(func.name, &parameters[0])?),
    };
    let argument = &selector[0];
    let PromExpr::MatrixSelector(MatrixSelector { vs, range }) = unparen(argument) else {
        return Err(not_implemented(&format!(
            "a call to `{}` whose argument is `{}`",
            func.name, argument
        )));
    };
    // A native range window reads one series to its last chunk before it reads the next, so raw
    // samples whose chunks of different series interleave cannot be folded by it. The declaration
    // is refused here rather than recorded onto an input the tree is built over and fails on.
    if ordering == SeriesOrdering::Interleaved {
        return Err(not_implemented(
            "raw samples whose series interleave, which this compiler cannot consume: its range \
             window reads one series to its last chunk before the next, so chunks of different \
             series may not interleave",
        ));
    }
    window_selection(stmt, function, vs, range, input, ordering)
}

/// Binds `function` over the range selector `vs[range]` into a window over the raw input.
///
/// Both shapes reach this: the range, the offset, the anchor, the grid and the output are the same
/// whichever function reads them, so a parameterised function only decides *what* folds the window,
/// never *which* window is folded.
fn window_selection(
    stmt: &EvalStmt,
    function: RangeFunction,
    vs: &VectorSelector,
    range: &Duration,
    input: Arc<SeriesSchema>,
    ordering: SeriesOrdering,
) -> Result<NativeTree> {
    // An `@` anchor is step-invariant: every step folds the same samples, which the window's
    // `anchor_ms` tells `RangeEvaluator` to replay over the grid.
    let range_ms = range_millis(range.as_millis())?;
    let offset_ms = selector_offset_ms(&vs.offset)?;
    let anchor_ms = selector_anchor_ms(&vs.at, stmt)?;
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
                    anchor_ms,
                },
                // The input is raw samples, so it has no grid of its own.
                input_grid: None,
                output_grid,
            },
        },
        output,
    })
}

/// Compiles a bare selector into an instant selection over the raw input.
///
/// The lookback, the offset and the grid are the whole of an instant selection: it reports the last
/// sample at or before each grid point, within the statement's lookback.
fn instant_selection(
    stmt: &EvalStmt,
    selector: &VectorSelector,
    input: Arc<SeriesSchema>,
    ordering: SeriesOrdering,
) -> Result<NativeTree> {
    // An `@` anchor is bound on a range window, which carries `anchor_ms`; an instant selection has
    // no such field, so the anchor has nowhere to go. Refusing it is the only alternative to
    // dropping it, which would silently select around every grid point instead of one anchor.
    if selector.at.is_some() {
        return Err(not_implemented(&format!(
            "the `@`-anchored selector `{selector}`, whose anchor the instant selection node has \
             no field for: an instant selection binds a lookback, an offset and a grid, while \
             anchoring is bound on a range window, as in `rate(x[5m] @ 1234)`"
        )));
    }
    let lookback_ms = lookback_millis(stmt.lookback_delta.as_millis())?;
    let offset_ms = selector_offset_ms(&selector.offset)?;
    let output_grid = statement_grid(stmt)?;

    // An instant selection reports one value per input field, so the layout is the input's.
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::clone(&input.fields),
        eval_grid: Some(Arc::clone(&output_grid)),
    }));

    // One allocation shared by the binding and the node's output, as in the window shape.
    let raw = NativeType::Samples(input);

    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: raw.clone(),
                    ordering,
                }),
                output: raw,
            }),
            op: BoundInstant {
                lookback_ms,
                offset_ms,
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

/// The statement's lookback in milliseconds, which is how far back an instant selection reads.
fn lookback_millis(lookback: u128) -> Result<i64> {
    millis(lookback).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "the statement's lookback of {lookback}ms is out of range"
        ))
    })
}

/// The selector's `@` anchor in milliseconds since the Unix epoch, or `None` when it has none.
///
/// `start()` and `end()` stay symbolic in the AST, because only the statement knows which evaluation
/// range they name, so they are resolved against `stmt.start` and `stmt.end` here. The offset is not
/// folded in: [`RangeWindow`] keeps anchor and offset apart and ends the window at
/// `anchor_ms - offset_ms` at run time.
///
/// An anchor decouples the window's read range from the statement's: it reads
/// `(anchor_ms - offset_ms - range_ms, anchor_ms - offset_ms]`, which for an anchor like `@ 1234` can
/// lie entirely outside `[stmt.start - range_ms, stmt.end]`. Whoever binds the scan therefore has to
/// derive its range from the window's anchor, offset and range rather than from `stmt.start` and
/// `stmt.end`; a scan bound to the statement alone leaves such a window with no samples, and the
/// query silently returns empty instead of failing.
fn selector_anchor_ms(at: &Option<AtModifier>, stmt: &EvalStmt) -> Result<Option<i64>> {
    match at {
        None => Ok(None),
        Some(AtModifier::Start) => Ok(Some(signed_millis_since_epoch(stmt.start)?)),
        Some(AtModifier::End) => Ok(Some(signed_millis_since_epoch(stmt.end)?)),
        Some(AtModifier::At(time)) => Ok(Some(signed_millis_since_epoch(*time)?)),
    }
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

/// What a call to a range function has to be: the parameters it reads, and how to bind them.
#[derive(Clone, Copy)]
enum RangeSignature {
    /// A function that reads only its range selector, as in `rate(x[5m])`.
    Selector(RangeFunction),
    /// A function that reads one number literal before its range selector, and the function that
    /// number binds, as in `quantile_over_time(0.9, x[5m])`.
    ///
    /// The number is kept out of the variant name because only the call site knows it.
    Scalar(fn(f64) -> RangeFunction),
}

impl RangeSignature {
    /// How many scalar parameters the function reads before its range selector.
    fn parameters(self) -> usize {
        match self {
            Self::Selector(_) => 0,
            Self::Scalar(_) => 1,
        }
    }

    /// What the call's arguments are, for the arity rejection to name them.
    fn arguments(self) -> &'static str {
        match self {
            Self::Selector(_) => "its range selector",
            Self::Scalar(_) => "a number and its range selector",
        }
    }
}

/// The signature the PromQL name calls, or `None` when this compiler has no kernel for it.
///
/// The map is explicit on purpose, as in [`range_function`]: a name that is not listed is rejected,
/// so supporting one more function is a decision made in one place rather than a consequence of a
/// name looking familiar. A parameterised function is named here rather than in the map, because
/// its name alone cannot produce the variant the tree binds.
fn range_signature(name: &str) -> Option<RangeSignature> {
    match name {
        "quantile_over_time" => Some(RangeSignature::Scalar(RangeFunction::QuantileOverTime)),
        _ => range_function(name).map(RangeSignature::Selector),
    }
}

/// The number a parameterised range function was given, as the literal it was written as.
///
/// A number literal is the only parameter this compiler binds. A parameter whose value is only
/// known at run time, `time()` being the real case, would make the kernel's constant a query-time
/// value, which the tree has no field for.
///
/// A parameter written as an expression the existing planner folds to a literal is refused here
/// and is a gap this stage leaves open on purpose: `quantile_over_time(1 - 0.1, x[5m])` and
/// `quantile_over_time(-(0.9), x[5m])` plan today through `try_build_literal_expr`, which folds
/// `Paren`, `Unary` and literal-only `Binary`, and reach this function still unfolded. They are
/// refused rather than answered from the wrong number, and folding them here would be the work
/// to close the gap.
///
/// A quantile outside `[0, 1]` is not refused here: Prometheus binds it and the kernel reports
/// `-Infinity` below zero and `+Infinity` above one, so refusing it would be this compiler
/// inventing a rule the query engine does not have.
fn scalar_parameter(name: &str, parameter: &PromExpr) -> Result<f64> {
    match unparen(parameter) {
        PromExpr::NumberLiteral(NumberLiteral { val }) => Ok(*val),
        found => Err(not_implemented(&format!(
            "a call to `{name}` whose first argument is `{found}`, where it has to be a number \
             literal"
        ))),
    }
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

/// The expression a parenthesised one wraps, however many parentheses were written.
///
/// Parentheses group, so they say nothing about the expression inside them: `rate((x[5m]))` asks
/// for exactly what `rate(x[5m])` asks for. Refusing it would reject a statement on the way it was
/// typed rather than on what it means.
fn unparen(mut expr: &PromExpr) -> &PromExpr {
    while let PromExpr::Paren(paren) = expr {
        expr = &paren.expr;
    }
    expr
}

/// A rejection of a shape outside [`SUPPORTED`].
fn not_implemented(found: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native PromQL compiler supports only {SUPPORTED} for now, but found {found}"
    ))
}

#[cfg(test)]
mod tests;
