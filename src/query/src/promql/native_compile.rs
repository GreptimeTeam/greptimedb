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
//! accepts is one recursive rule:
//!
//! ```text
//! <selector>                                        e.g. x
//! <range function>(<selector>[<range>])             e.g. rate(x[5m])
//! <range function>(<constant>, <selector>[<range>]) e.g. quantile_over_time(0.9, x[5m])
//! <math function>(<expression>[, <constant>]*)      e.g. abs(rate(x[5m])), clamp_min(x, 0)
//! ```
//!
//! which become `Input` (raw samples) → `Instant` (the last sample within the statement's lookback),
//! `Input` (raw samples) → `Window` (the range function over the statement's grid) and
//! `<expression>` → `Map` (a pointwise math function over what its argument evaluates to). A
//! selector may carry `offset` in either selection shape and, in the window shape, an `@` anchor.
//! Everything else is a [`DataFusionError::NotImplemented`] that names what was found and quotes the
//! shapes above, because rejecting a query the compiler does not understand is the only honest
//! alternative to compiling it into something else.
//!
//! Two decisions keep the recursion from growing a branch per query. A constant argument — a
//! quantile, a rounding step, a clamp bound — is folded out of the statement by [`constant_value`],
//! so `quantile_over_time(1 - 0.1, x[5m])` binds the number it means rather than the expression it
//! was written as. A math function is bound by [`math_op`] from a list of names, so
//! `clamp_min(abs(rate(x[5m])), 0)` is two maps over one window because the arguments nest, not
//! because the compiler knows the query. Arithmetic inside a fold is [`ArithmeticOp::calculate`]'s
//! alone, so the number this compiler binds and the number the kernel computes cannot disagree
//! about `%`, `^` or `atan2`.
//!
//! The statement's grid, the selector's lookback, window, offset and anchor are the only numbers
//! derived here; the legality of the tree is not checked here. [`NativeTree::try_new`] is the
//! single authority on that, so a node added later cannot be legal in one place and illegal in
//! another. What is refused before a tree exists is the scan's shape: no shape below can consume
//! chunks of different series that interleave, so an `Interleaved` ordering is refused once, at the
//! entry, and [`NativeNode::validate`] refuses the same ordering for a tree built some other way.

use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::SeriesSchema;
use datatypes::series::time::{EvalGrid, GridSpec};
use promql::map::{ArithmeticOp, MathOp};
use promql::map_time::DateFunction;
use promql::native::plan::{
    BoundInput, BoundInstant, BoundMap, BoundWindow, NativeExpr, NativeNode, NativeTree, NativeType,
};
use promql::range_eval::{RangeFunction, RangeWindow};
use promql_parser::parser::token::TokenType;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AtModifier, Call, EvalStmt, Expr as PromExpr, MatrixSelector, NumberLiteral, Offset,
    VectorSelector, token,
};

use crate::promql::label_values::signed_millis_since_epoch;

/// The shapes this compiler accepts, quoted in every rejection.
const SUPPORTED: &str = "a selector, a range function over a range selector — \
                         `<range function>(<selector>[<range>])`, with its constants in front of \
                         the selector as in `quantile_over_time(0.9, x[5m])` — or a math function \
                         over any of them, as in `abs(rate(x[5m]))`: for example `x`, \
                         `rate(x[5m])`, `quantile_over_time(0.9, x[5m])` or `abs(rate(x[5m]))`";

/// Compiles `stmt` into a tree that reads `input` and evaluates the statement's expression over it.
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
/// one. Only `Contiguous` and `KeyThenTime` are accepted: every shape this compiler accepts reads
/// one series to its last chunk before the next, so an `Interleaved` declaration is refused at the
/// entry, before any shape is built — the bare selector included, which returns before a window
/// exists — and a shape added later inherits the refusal instead of restating it.
/// [`NativeNode::validate`] refuses the same ordering in its `Instant` and `Window` branches, as a
/// backstop for a tree built some other way.
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
    // Every shape this compiler accepts reads one series to its last chunk before the next, so raw
    // samples whose chunks of different series interleave cannot be fed to any of them. The refusal
    // is here rather than in a shape's branch so that a shape added later inherits it.
    if ordering == SeriesOrdering::Interleaved {
        return Err(not_implemented(
            "raw samples whose series interleave, which this compiler cannot consume: a native \
             evaluation reads one series to its last chunk before the next, so chunks of different \
             series may not interleave",
        ));
    }
    let context = Context {
        stmt,
        input,
        ordering,
    };
    // The whole tree is validated once, here: a node compiled as another node's input is checked as
    // part of the tree rather than on its own, so the same node cannot be legal in one place and
    // illegal in another.
    NativeTree::try_new(compile_expr(&context, &stmt.expr)?)
}

/// What every node of one compilation derives from the statement rather than from the expression.
///
/// The three are the same for the whole tree: one statement decides the grid, the lookback and the
/// anchors, one caller binds the raw input, and one scan guarantees the ordering. Passing them as
/// one value is what lets [`compile_expr`] recurse without restating what it was handed.
struct Context<'a> {
    /// The statement being compiled.
    stmt: &'a EvalStmt,
    /// The raw series the caller bound; shared by every input node rather than copied per node.
    input: Arc<SeriesSchema>,
    /// The ordering the caller's scan guarantees.
    ordering: SeriesOrdering,
}

/// Compiles `expr` into the node it denotes, recursing into whatever it is made of.
///
/// One rule per kind of expression and nothing per query: a math function is compiled by compiling
/// its argument, so a query nests as deeply as it was written. A kind this compiler has no node for
/// is refused with what was found, which is why the rejection is useful without a coverage list.
fn compile_expr(context: &Context, expr: &PromExpr) -> Result<NativeNode> {
    match expr {
        // Parentheses group, so they change how a statement was typed rather than what it asks for:
        // the expression inside them is the one that compiles.
        PromExpr::Paren(paren) => compile_expr(context, &paren.expr),
        PromExpr::Call(call) => compile_call(context, call),
        PromExpr::VectorSelector(selector) => instant_selection(context, selector),
        found => Err(not_implemented(&format!("the expression `{found}`"))),
    }
}

/// Compiles a call: a range function opens a window over the selector it is given, and a math
/// function maps the value of its first argument.
///
/// The two name lists are disjoint, and a range function is decided first because it binds its own
/// window rather than its argument's value. A name in neither list is refused here, which is where
/// the difference between "this compiler cannot fold this function" and "this name means nothing"
/// is still visible.
fn compile_call(context: &Context, call: &Call) -> Result<NativeNode> {
    let Call { func, args } = call;
    if let Some(signature) = range_signature(func.name) {
        return window_call(context, func.name, signature, &args.args);
    }
    // The name is looked up before the call's arguments are read, so a name this compiler has no
    // kernel for is refused as such whichever arguments it was given: `pi()` and `time()` have no
    // vector to map, and the rejection names the name rather than describing a vector they never
    // wrote.
    let Some((op, vector)) = math_op(func.name, &args.args)? else {
        return Err(not_implemented(&format!(
            "a call to `{}`, which has no native kernel",
            func.name
        )));
    };
    map_call(context, func.name, op, vector)
}

/// Binds a range function over the range selector it is given, and the window that selector opens.
///
/// The parameters come first and the range selector is last, for both signatures: a function without
/// parameters has no `parameters` to read.
fn window_call(
    context: &Context,
    name: &str,
    signature: RangeSignature,
    args: &[Box<PromExpr>],
) -> Result<NativeNode> {
    // The count is the signature's, so the rejection names what this function takes rather than
    // what one argument happens to be.
    let expected = signature.parameters() + 1;
    if args.len() != expected {
        return Err(not_implemented(&format!(
            "a call to `{name}` with {} argument(s), where it takes {expected}: {}",
            args.len(),
            signature.arguments()
        )));
    }
    let (parameters, selector) = args.split_at(signature.parameters());
    let function = match signature {
        RangeSignature::Selector(function) => function,
        RangeSignature::Scalar(bind) => bind(constant_argument(name, &parameters[0], 0)?),
    };
    let argument = &selector[0];
    let PromExpr::MatrixSelector(MatrixSelector { vs, range }) = unparen(argument) else {
        return Err(not_implemented(&format!(
            "a call to `{name}` whose argument is `{argument}`"
        )));
    };
    window_selection(context, function, vs, range)
}

/// Wraps `op` over the value of `vector`, which is the first argument of a math function.
///
/// The output is the input's own type: a pointwise function moves no value to another grid point
/// and reports one value per field it was given, so the map's schema is the input's allocation
/// rather than a copy that merely agrees with it. [`NativeTree::validate`] does not require that
/// sharing: it compares the map's grid with its input's by [`EvalGrid::spec`] and its field count
/// with its input's, so a map that allocated an equal grid of its own would validate the same way.
/// The only comparison by identity is an input node's, whose output has to be the very binding it
/// was bound to.
fn map_call(context: &Context, name: &str, op: MathOp, vector: &PromExpr) -> Result<NativeNode> {
    // A math function is pointwise over a vector, so its first argument has to be one. The parser
    // checks the same thing and the tree's validation checks the compiled input's type again, but
    // neither can name the argument the client wrote: this is the check that can.
    if vector.value_type() != ValueType::Vector {
        return Err(not_implemented(&format!(
            "a call to `{name}` whose first argument is `{vector}`, which is a {}, where the \
             function takes a vector",
            vector.value_type()
        )));
    }
    let input = compile_expr(context, vector)?;
    let output = input.output.clone();
    Ok(NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(input),
            op: BoundMap {
                ops: Arc::from([op]),
            },
        },
        output,
    })
}

/// Binds `function` over the range selector `vs[range]` into a window over the raw input.
///
/// Both shapes reach this: the range, the offset, the anchor, the grid and the output are the same
/// whichever function reads them, so a parameterised function only decides *what* folds the window,
/// never *which* window is folded.
fn window_selection(
    context: &Context,
    function: RangeFunction,
    vs: &VectorSelector,
    range: &Duration,
) -> Result<NativeNode> {
    // An `@` anchor is step-invariant: every step folds the same samples, which the window's
    // `anchor_ms` tells `RangeEvaluator` to replay over the grid.
    let range_ms = range_millis(range.as_millis())?;
    let offset_ms = selector_offset_ms(&vs.offset)?;
    let anchor_ms = selector_anchor_ms(&vs.at, context.stmt)?;
    // The grid is the last thing built: a query refused above, or a window that cannot exist,
    // never pays for a grid nothing would read.
    let output_grid = statement_grid(context.stmt)?;

    // A range function reports one value per input field, so the layout is the input's.
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::clone(&context.input.fields),
        eval_grid: Some(Arc::clone(&output_grid)),
    }));

    // One allocation shared by the binding and the node's output: `NativeTree::try_new` compares
    // them by identity, because a compiler that meant one schema has no reason to allocate it
    // twice.
    let raw = NativeType::Samples(Arc::clone(&context.input));

    Ok(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: raw.clone(),
                    ordering: context.ordering,
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
fn instant_selection(context: &Context, selector: &VectorSelector) -> Result<NativeNode> {
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
    let lookback_ms = lookback_millis(context.stmt.lookback_delta.as_millis())?;
    let offset_ms = selector_offset_ms(&selector.offset)?;
    let output_grid = statement_grid(context.stmt)?;

    // An instant selection reports one value per input field, so the layout is the input's.
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::clone(&context.input.fields),
        eval_grid: Some(Arc::clone(&output_grid)),
    }));

    // One allocation shared by the binding and the node's output, as in the window shape.
    let raw = NativeType::Samples(Arc::clone(&context.input));

    Ok(NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: raw.clone(),
                    ordering: context.ordering,
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
    /// A function that reads one constant before its range selector, and the function that constant
    /// binds, as in `quantile_over_time(0.9, x[5m])`.
    ///
    /// The constant is kept out of the variant name because only the call site knows it.
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

/// The signature the PromQL name calls, or `None` when this compiler has no range kernel for it.
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

/// What a call to a math function has to be: the constants it reads after its vector, and how to
/// bind them.
///
/// The shapes are separated by how many constants the function reads, because that is the only
/// thing the variants differ in: a name maps to one kernel, and the constants are bound from the
/// call. `round` is the one function whose constant may be left out, so it is its own shape: the
/// default is a decision about the function rather than about the constant.
#[derive(Clone, Copy)]
enum MathSignature {
    /// A function of its vector alone, as in `abs`.
    Unary(MathOp),
    /// A function of its vector and one constant, as in `clamp_min`.
    OneConstant(fn(f64) -> MathOp),
    /// A function of its vector and one constant it may be called without, as in `round`.
    DefaultedConstant {
        default: f64,
        bind: fn(f64) -> MathOp,
    },
    /// A function of its vector and two constants, as in `clamp`.
    TwoConstants(fn(f64, f64) -> MathOp),
}

impl MathSignature {
    /// How many constants the function reads when every one of them is written.
    fn parameters(self) -> usize {
        match self {
            Self::Unary(_) => 0,
            Self::OneConstant(_) | Self::DefaultedConstant { .. } => 1,
            Self::TwoConstants(_) => 2,
        }
    }

    /// How many constants the function has to be given.
    fn required(self) -> usize {
        match self {
            Self::DefaultedConstant { .. } => 0,
            _ => self.parameters(),
        }
    }

    /// What the call's arguments are, for the arity rejection to name them.
    fn arguments(self) -> &'static str {
        match self {
            Self::Unary(_) => "its vector",
            Self::OneConstant(_) => "its vector and one constant",
            Self::DefaultedConstant { .. } => "its vector and one optional constant",
            Self::TwoConstants(_) => "its vector and two constants",
        }
    }
}

/// The signature the PromQL name calls, or `None` when this compiler has no math kernel for it.
///
/// The map is explicit on purpose, as in [`range_function`]: a name that is not listed is refused
/// rather than treated as math because it looks mathematical, so adding one more function is a
/// decision made in one place. A name in this list is a promise that its kernel exists, which is why
/// arity is a property of the variant rather than of the caller.
fn math_signature(name: &str) -> Option<MathSignature> {
    match name {
        "abs" => Some(MathSignature::Unary(MathOp::Abs)),
        "ceil" => Some(MathSignature::Unary(MathOp::Ceil)),
        "floor" => Some(MathSignature::Unary(MathOp::Floor)),
        "sqrt" => Some(MathSignature::Unary(MathOp::Sqrt)),
        "exp" => Some(MathSignature::Unary(MathOp::Exp)),
        "ln" => Some(MathSignature::Unary(MathOp::Ln)),
        "log2" => Some(MathSignature::Unary(MathOp::Log2)),
        "log10" => Some(MathSignature::Unary(MathOp::Log10)),
        "sin" => Some(MathSignature::Unary(MathOp::Sin)),
        "cos" => Some(MathSignature::Unary(MathOp::Cos)),
        "tan" => Some(MathSignature::Unary(MathOp::Tan)),
        "asin" => Some(MathSignature::Unary(MathOp::Asin)),
        "acos" => Some(MathSignature::Unary(MathOp::Acos)),
        "atan" => Some(MathSignature::Unary(MathOp::Atan)),
        "sinh" => Some(MathSignature::Unary(MathOp::Sinh)),
        "cosh" => Some(MathSignature::Unary(MathOp::Cosh)),
        "tanh" => Some(MathSignature::Unary(MathOp::Tanh)),
        "asinh" => Some(MathSignature::Unary(MathOp::Asinh)),
        "acosh" => Some(MathSignature::Unary(MathOp::Acosh)),
        "atanh" => Some(MathSignature::Unary(MathOp::Atanh)),
        "deg" => Some(MathSignature::Unary(MathOp::Deg)),
        "rad" => Some(MathSignature::Unary(MathOp::Rad)),
        "sgn" => Some(MathSignature::Unary(MathOp::Sgn)),
        // A date function takes the vector its values are read from; without one it reads the
        // evaluation time, which has no input to map and is refused here.
        "day_of_month" => Some(MathSignature::Unary(MathOp::Date(DateFunction::DayOfMonth))),
        "day_of_week" => Some(MathSignature::Unary(MathOp::Date(DateFunction::DayOfWeek))),
        "day_of_year" => Some(MathSignature::Unary(MathOp::Date(DateFunction::DayOfYear))),
        "days_in_month" => Some(MathSignature::Unary(MathOp::Date(
            DateFunction::DaysInMonth,
        ))),
        "hour" => Some(MathSignature::Unary(MathOp::Date(DateFunction::Hour))),
        "minute" => Some(MathSignature::Unary(MathOp::Date(DateFunction::Minute))),
        "month" => Some(MathSignature::Unary(MathOp::Date(DateFunction::Month))),
        "year" => Some(MathSignature::Unary(MathOp::Date(DateFunction::Year))),
        // `round`'s nearest defaults to one, so `round(x)` is `round(x, 1)` and its variant always
        // carries the number it rounds to.
        "round" => Some(MathSignature::DefaultedConstant {
            default: 1.0,
            bind: |nearest| MathOp::Round { nearest },
        }),
        "clamp" => Some(MathSignature::TwoConstants(|min, max| MathOp::Clamp {
            min,
            max,
        })),
        "clamp_min" => Some(MathSignature::OneConstant(|min| MathOp::ClampMin { min })),
        "clamp_max" => Some(MathSignature::OneConstant(|max| MathOp::ClampMax { max })),
        _ => None,
    }
}

/// The pointwise kernel `name` denotes over its vector, and that vector, or `None` when this
/// compiler has no math kernel for that name.
///
/// A name that is not in [`math_signature`] is not math: the caller reports that it has no kernel
/// instead of compiling it into a kernel that would compute something else. `arguments` is the
/// call's whole argument list, because the vector is its first argument: the arity is the
/// signature's and counts the vector in, so a call with no argument at all is refused as a call
/// that wrote none rather than as a shape this compiler cannot bind. The arguments after the vector
/// are the call's constants, in the order they were written, and each is folded to the number it
/// means by [`constant_value`], because the tree binds a kernel's constants and has no field for an
/// expression.
fn math_op<'a>(
    name: &str,
    arguments: &'a [Box<PromExpr>],
) -> Result<Option<(MathOp, &'a PromExpr)>> {
    let Some(signature) = math_signature(name) else {
        return Ok(None);
    };
    // The vector is the function's first argument, so a call that wrote no argument at all is
    // refused by the same arity check as one that wrote too few constants.
    let Some((vector, parameters)) = arguments.split_first() else {
        return Err(arity_error(name, signature, 0));
    };
    let given = parameters.len() + 1;
    if given < signature.required() + 1 || given > signature.parameters() + 1 {
        return Err(arity_error(name, signature, given));
    }
    // The vector is the first argument, so the constants are read from the second onwards.
    let op = match signature {
        MathSignature::Unary(op) => op,
        MathSignature::OneConstant(bind) => bind(constant_argument(name, &parameters[0], 1)?),
        MathSignature::DefaultedConstant { default, bind } => bind(match parameters.first() {
            Some(parameter) => constant_argument(name, parameter, 1)?,
            None => default,
        }),
        MathSignature::TwoConstants(bind) => bind(
            constant_argument(name, &parameters[0], 1)?,
            constant_argument(name, &parameters[1], 2)?,
        ),
    };
    Ok(Some((op, vector)))
}

/// The rejection of a math call that wrote `given` arguments where `signature` takes another count.
///
/// The count is the signature's, so the message says what this function takes rather than what one
/// argument happens to be. The vector is counted in: it is the function's first argument, and a
/// reader counting the call's arguments should not have to add it themselves.
fn arity_error(name: &str, signature: MathSignature, given: usize) -> DataFusionError {
    not_implemented(&format!(
        "a call to `{name}` with {given} argument(s), where it takes {}",
        signature.arguments()
    ))
}

/// The constant `argument` folds to, or a rejection naming the argument and where it was read.
///
/// A parameter is a constant of the statement: a quantile, a rounding step or a clamp bound is a
/// field of the tree, so a parameter whose value is only known at run time — `time()` being the real
/// case — has nowhere to go. It is refused rather than answered from the wrong number, and the
/// rejection names the expression, because that is what the query has to change.
///
/// A quantile outside `[0, 1]` is not refused here: Prometheus binds it and the kernel reports
/// `-Infinity` below zero and `+Infinity` above one, so refusing it would be this compiler inventing
/// a rule the query engine does not have.
fn constant_argument(name: &str, argument: &PromExpr, position: usize) -> Result<f64> {
    constant_value(argument).ok_or_else(|| {
        not_implemented(&format!(
            "a call to `{name}` whose {} is `{argument}`, where it has to be a constant expression \
             this compiler folds to a number literal",
            argument_position(position)
        ))
    })
}

/// How a rejection names the argument a parameter was read from.
///
/// The three positions are the ones a signature can read: the vector, and the two constants behind
/// it. Every caller passes the literal position its signature reads, so there is no fourth position
/// to name and no default to invent one for.
fn argument_position(position: usize) -> &'static str {
    const POSITIONS: [&str; 3] = ["first argument", "second argument", "third argument"];
    POSITIONS[position]
}

/// The number `expr` folds to, or `None` when its value is not the statement's to know.
///
/// Only what the statement itself decides is folded: a number literal, a group around one, a negated
/// one, and arithmetic whose two operands both fold. A call, a selector, a range selector, an
/// aggregate and a subquery are `None`, because their value comes from the data or from the
/// evaluation time, and a kernel's constant is a field of the tree rather than something bound while
/// it runs. Returning `None` rather than an error is what lets the caller report where the
/// expression was found, which is the useful half of the refusal.
///
/// Arithmetic is [`ArithmeticOp::calculate`]'s and nothing else's: that is where Prometheus'
/// semantics for `%`, `^` and `atan2` live, the last two through `go_math`, so defining them again
/// here would be a second answer for the two to disagree over. An operator with no
/// [`ArithmeticOp`] — a comparison, `and`, `or`, `unless` — is not arithmetic and does not fold.
fn constant_value(expr: &PromExpr) -> Option<f64> {
    match expr {
        PromExpr::NumberLiteral(NumberLiteral { val }) => Some(*val),
        PromExpr::Paren(paren) => constant_value(&paren.expr),
        PromExpr::Unary(unary) => constant_value(&unary.expr).map(|value| -value),
        PromExpr::Binary(binary) => {
            let op = arithmetic_op(&binary.op)?;
            let left = constant_value(&binary.lhs)?;
            let right = constant_value(&binary.rhs)?;
            Some(op.calculate(left, right))
        }
        // Calls (`time()`, `scalar(v)`), selectors, range selectors, aggregates and subqueries all
        // read something this compiler cannot know before the query runs.
        _ => None,
    }
}

/// The arithmetic operator a PromQL token denotes, or `None` when the token is not arithmetic.
///
/// The token is matched by its id, which is stable and is what the rest of the planner matches on,
/// rather than by the text it displays as: a symbol this compiler does not recognise is not
/// arithmetic here rather than a wrong operator. A comparison and a set operator are the cases that
/// reach this and fold nothing, which is this batch's boundary rather than an accident of the map.
fn arithmetic_op(op: &TokenType) -> Option<ArithmeticOp> {
    match op.id() {
        token::T_ADD => Some(ArithmeticOp::Add),
        token::T_SUB => Some(ArithmeticOp::Subtract),
        token::T_MUL => Some(ArithmeticOp::Multiply),
        token::T_DIV => Some(ArithmeticOp::Divide),
        token::T_MOD => Some(ArithmeticOp::Modulo),
        token::T_POW => Some(ArithmeticOp::Power),
        token::T_ATAN2 => Some(ArithmeticOp::Atan2),
        _ => None,
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
/// typed rather than on what it means. A range selector is not an expression this compiler
/// compiles on its own, so the call that reads one looks through the grouping itself.
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
