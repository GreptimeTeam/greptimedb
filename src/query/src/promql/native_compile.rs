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
//! <expression>                                     one of, recursively:
//!   <selector>                                     e.g. x, x offset -5m
//!   <range function>(<matrix>)                     e.g. rate(x[5m]), rate(x[5m:1m] @ 1234)
//!   quantile_over_time(<constant>, <matrix>)       e.g. quantile_over_time(0.9, x[5m])
//!   predict_linear(<matrix>, <constant>)           e.g. predict_linear(x[5m], 300)
//!   double_exponential_smoothing(<matrix>, <constant>, <constant>)
//!                                                  e.g. double_exponential_smoothing(x[5m], .5, .1)
//!   <math function>(<expression>[, <constant>]*)   e.g. abs(rate(x[5m])), clamp_min(x, 0)
//!   <sort>(<expression>[, <string>]*)              e.g. sort_desc(x), sort_by_label(x, "job")
//!   absent(<expression>)                           e.g. absent(x{job="a"})
//!   <histogram>(<constant>+, <expression>)         e.g. histogram_quantile(0.9, rate(x[5m]))
//!   <expression> <operator> <constant>             e.g. x * 100, x > bool 0
//!   <constant> <operator> <expression>             e.g. 1 / x, 0 < x
//!   -<expression>                                  e.g. -x
//!   <aggregator> [(by|without)(<labels>)] (<expression>)
//!                                                  e.g. avg by(job)(rate(x[5m]) * 100)
//!   <aggregator> (<expression>) [(by|without)(<labels>)]
//!                                                  e.g. count (x) by(job)
//!   quantile(<constant>, <expression>) [(by|without)(<labels>)]
//!                                                  e.g. quantile(0.9, x) by(job)
//!
//! <matrix>    <selector>[<range>]<modifiers>       a range selector: raw samples
//!           | <expression>[<range>:<step>]<modifiers>
//!                                                  a subquery: <expression> evaluated on a grid;
//!                                                  the step may be omitted as `[<range>:]`
//! <modifiers>                                      nothing, `offset <duration>`, `@ <anchor>`, or
//!                                                  both, in either order — e.g. `x[5m] @ 1234
//!                                                  offset 1m`
//! ```
//!
//! where `<aggregator>` is `sum`, `avg`, `count`, `min`, `max`, `group`, `stddev`, `stdvar` or
//! `quantile`, the function names are [`range_signature`]'s and [`math_signature`]'s, and which side
//! of the matrix a range function's constants go on is [`RangeSignature`]'s: before it for
//! `quantile_over_time`, after it for `predict_linear` and `double_exponential_smoothing`. A math
//! function's constants always follow its vector.
//!
//! Parentheses are transparent wherever they appear — `rate((x[5m]))` is `rate(x[5m])` — because
//! they say how a statement was typed rather than what it asks for.
//!
//! A subquery's `<expression>` is any shape above, so a subquery nests inside a subquery on a grid
//! of its own, as in `rate(rate(x[5m:1m])[10m:30s])`; a `<step>` left out means Prometheus's default
//! evaluation interval, one minute. Its `offset` and its `@` anchor are its own, not the inner
//! expression's, so `rate(x[5m:1m] @ end() offset 1m)` reads one fixed interval. A `<selector>`
//! outside a `<matrix>` takes an `offset` and no anchor: the instant shape has no field for one, so
//! `x @ 100` is refused and `rate(x[5m] @ 100)` is not.
//!
//! Those shapes become `Input` (raw samples) → `Instant` (the last sample within the statement's
//! lookback), `Input` (raw samples) → `Window` (the range function over the statement's grid),
//! `<expression>` → `Window` whose input is evaluated (the subquery, whose input grid is its own),
//! `<expression>` → `Map` (a pointwise math function over what its argument evaluates to) and, over
//! any of those, `Binary` (an arithmetic or comparison operation with a constant on one side),
//! `Scalar` (that constant) and `Aggregate` (one of the nine aggregators that fold a group's values
//! into one value per grid point, optionally grouped). Everything else is a
//! [`DataFusionError::NotImplemented`] that names what was found and quotes the shapes above,
//! because rejecting a query the compiler does not understand is the only honest alternative to
//! compiling it into something else.
//!
//! The compiler binds a subquery's child grid and `RangeWindow` independently of runtime execution:
//! compilation describes the anchored interval, while runtime support determines how that bound
//! plan is consumed.
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
//! Two more are about what a node means rather than about which node to build. A binary operation
//! keeps its operands in the order they were written, because most operators are not commutative:
//! `1 / x` is not `x / 1`, and which side the constant is on is the only thing that tells them
//! apart. `return_bool` is carried as its own field, so `x > 0` (which filters) and `x > bool 0`
//! (which reports zero or one) are one node with one field changed. A negation then needs no node of
//! its own: `-x` binds as the multiplication by `-1` that it is, which the multiplication kernel
//! already computes.
//!
//! What a tree cannot yet express is deliberate too. The one input binding carries the ordering it
//! guarantees and what it produces, but not the selector it reads, so a tree with two selectors
//! cannot tell its inputs apart and an operation between two vectors is refused. A statement whose
//! value is a scalar has nothing to read, so it is refused as well: a constant is bound next to a
//! vector that gives it something to broadcast over.
//!
//! An aggregate is bound as [`AggregateStage::Complete`], which is a claim about the whole execution
//! rather than about the node: it says that every contribution to every group is in this execution's
//! input. The caller has to keep that claim. One tree per partition does not: handing every
//! partition the same `Complete` tree sums each partition's part and reports it as the group's
//! whole, so a partitioned caller has to feed whole groups to one execution or split the aggregation
//! into `Partial` and `Final` instead. What a `Complete` node guarantees here is only the arithmetic
//! — that the values it folded are all the values it was given.
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
use datatypes::series::samples::SeriesSchema;
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{MetricName, SeriesOrdering};
use promql::histogram_eval::HistogramFunction;
use promql::map::{ArithmeticOp, ComparisonOp, MathOp};
use promql::map_time::DateFunction;
use promql::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundVectorOp, BoundWindow, Grouping, NativeExpr, NativeNode, NativeTree, NativeType,
};
use promql::range_eval::{RangeFunction, RangeWindow, subquery_grid};
use promql::select::{LabelMatcher, MatchOp, SortOrder, absent_labels};
use promql::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};
use promql_parser::label::MatchOp as ParserMatchOp;
use promql_parser::parser::token::TokenType;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, AtModifier, BinModifier, BinaryExpr, Call, EvalStmt, Expr as PromExpr,
    LabelModifier, MatrixSelector, NumberLiteral, Offset, StringLiteral, SubqueryExpr,
    VectorMatchCardinality, VectorMatchFillValues, VectorSelector, token,
};

use crate::promql::label_values::signed_millis_since_epoch;

/// The shapes this compiler accepts, quoted in every rejection.
///
/// It is one sentence of grammar rather than a list of examples, because it is what a refused query
/// reads: the shapes have to be true of every query the compiler binds, and the examples only show
/// them. The two matrix shapes and the recursion through them are stated first, because that
/// recursion is what a reader is most likely to get wrong.
const SUPPORTED: &str = "a selector — `x`, optionally with an `offset` — a range function over \
                         a matrix, where a matrix is `<selector>[<range>]` as in `rate(x[5m])` or \
                         `<expression>[<range>:<step>]` as in `rate(x[5m:1m])`, with the step \
                         optionally omitted as `[<range>:]`, where the subquery's `<expression>` \
                         is any shape here, so a subquery nests on a grid of its own as in \
                         `rate(rate(x[5m:1m])[10m:30s])`, and where either matrix shape carries the \
                         `offset` and `@` anchor written on it as in `rate(x[5m] @ 1234)`; a math \
                         function over any of them (`abs(rate(x[5m]))`, `clamp_min(x, 0)`), a binary \
                         operation between one of them and a constant (`2 / (x + 1)`, \
                         `x > bool 0`), a negation of one of them (`-x`), or one of the aggregators \
                         that folds a group's values into one value per grid point — `sum`, `avg`, \
                         `count`, `min`, `max`, `group`, `stddev`, `stdvar` and \
                         `quantile(<constant>, <expression>)` — over any of them, optionally \
                         grouped by `by` or `without` as in `avg by(job)(rate(x[5m]) * 100)`; a \
                         sort, `absent` or `histogram_quantile` / `histogram_fraction` over any of \
                         them (`sort_desc(x)`, `absent(x{job=\"a\"})`, \
                         `histogram_quantile(0.9, rate(x[5m]))`); a range function's own \
                         constants sit where that function takes them, before \
                         the matrix for `quantile_over_time(0.9, x[5m])` and after it for \
                         `predict_linear(x[5m], 300)` and \
                         `double_exponential_smoothing(x[5m], 0.5, 0.1)`";

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
    // A tree reads a scan and reports evaluated samples, so an expression whose value is not a
    // vector has nothing to compile into: `1 + 2` is a scalar, and the scalar node is bound as a
    // constant next to a vector that gives it something to broadcast over rather than on its own.
    // The check is on the statement's type rather than on the compiled node, so it does not depend on
    // which kinds of expression happen to compile to a scalar today.
    if stmt.expr.value_type() != ValueType::Vector {
        return Err(not_implemented(&format!(
            "the expression `{}`, which is a {} where a tree reports evaluated samples",
            stmt.expr,
            stmt.expr.value_type()
        )));
    }
    let context = Context {
        stmt,
        input,
        ordering,
        // The grid is built once for the whole compilation rather than per node: every node of a
        // tree without a subquery lands on this one, and a subquery replaces it for its own subtree.
        grid: statement_grid(stmt)?,
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
    /// The grid the expression being compiled is evaluated on.
    ///
    /// It is the statement's grid for everything except the inside of a subquery, which opens a grid
    /// of its own: `rate(x[5m:1m])` evaluates `x` at one-minute steps over the five-minute lookback,
    /// not at the statement's steps. Carrying it here is what lets [`compile_expr`] recurse into a
    /// subquery without every node builder having to ask which grid it is on.
    grid: Arc<EvalGrid>,
}

/// Compiles `expr` into the node it denotes, recursing into whatever it is made of.
///
/// One rule per kind of expression and nothing per query: a math function is compiled by compiling
/// its argument, so a query nests as deeply as it was written. A kind this compiler has no node for
/// is refused with what was found, which is why the rejection is useful without a coverage list.
fn compile_expr(context: &Context, expr: &PromExpr) -> Result<NativeNode> {
    // A constant is a value rather than a computation, so it binds as the number it folds to, by the
    // same fold a call's parameters use: `x * (1 + 2)` carries `3` rather than an addition the tree
    // would have to know how to run. The fold is checked first so that no shape below has to say
    // what it does with a constant operand.
    if let Some(value) = constant_value(expr) {
        return Ok(scalar_node(context, value));
    }
    match expr {
        // Parentheses group, so they change how a statement was typed rather than what it asks for:
        // the expression inside them is the one that compiles.
        PromExpr::Paren(paren) => compile_expr(context, &paren.expr),
        PromExpr::Call(call) => compile_call(context, call),
        PromExpr::VectorSelector(selector) => instant_selection(context, selector),
        PromExpr::Unary(unary) => negate(context, &unary.expr),
        PromExpr::Binary(binary) => binary_operation(context, binary),
        PromExpr::Aggregate(aggregate) => aggregate_expr(context, aggregate),
        found => Err(not_implemented(&format!("the expression `{found}`"))),
    }
}

/// Binds `value` as a constant scalar on the grid of the expression being compiled.
///
/// The grid is the context's rather than the constant's own: a constant has no timestamps, so the
/// only positions it can be broadcast over are the ones the expression it sits next to is evaluated
/// at — the statement's grid, or a subquery's inside one. [`NativeTree::validate`] compares a binary
/// operation's two grids by spec rather than by allocation.
///
/// Nothing here can fail: the grid is the one [`Context`] already carries, so a caller needs no `?`.
fn scalar_node(context: &Context, value: f64) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(Arc::clone(&context.grid)),
    }
}

/// Compiles a negation as the multiplication it is: `-x` is `-1 * x`.
///
/// The tree has no negate node and needs none: broadcasting a constant over the expression's value
/// is the same arithmetic, and the multiplication kernel is the one that already computes it. A
/// dedicated negate kernel would be an optimization of this node's work rather than a different
/// meaning, so binding the multiplication does not bind anything the query did not ask for.
fn negate(context: &Context, operand: &PromExpr) -> Result<NativeNode> {
    // The constant goes on the left, where the minus was: `-x` subtracts nothing, but a reader of
    // the tree should see the same order the query was written in.
    Ok(broadcast(
        scalar_node(context, -1.0),
        compile_expr(context, operand)?,
        BinaryOp::Mul,
        false,
    ))
}

/// Compiles a binary operation whose operands are not both vectors.
///
/// The tree holds one input binding, and that binding carries the ordering it guarantees and what it
/// produces but not the selector it reads, so two vectors cannot be told apart: `a + b` and `a + a`
/// would compile to the same tree. That is why a vector–vector operation is refused until an input
/// names its selector, and why an operation over one vector and one scalar is the shape the tree can
/// bind: a scalar carries no labels, so broadcasting it is unambiguous.
fn binary_operation(context: &Context, binary: &BinaryExpr) -> Result<NativeNode> {
    // A set operator combines two vectors' series rather than their values, and the tree has no node
    // for it at all. Its name is reported before the operands are looked at, so a set operation
    // between two vectors is refused as the set operation it is rather than as a binary operation
    // the compiler has no node for.
    if binary.op.is_set_operator() {
        return Err(not_implemented(&format!(
            "the set operator `{}` in `{binary}`, which has no native node: it combines two \
             vectors' series, and every binary node here is an arithmetic or comparison operation \
             over values",
            binary.op
        )));
    }
    let Some(op) = binary_op(&binary.op) else {
        return Err(not_implemented(&format!(
            "the binary operator `{}` in `{binary}`",
            binary.op
        )));
    };
    // Two vectors are refused before the modifier is looked at, so a vector–vector query with
    // matching says what is actually missing. The refusal names the missing identity rather than the
    // query, because that is what whoever adds it has to add.
    if binary.lhs.value_type() == ValueType::Vector && binary.rhs.value_type() == ValueType::Vector
    {
        return Err(not_implemented(&format!(
            "the binary operation `{binary}` between two vectors, which needs each input to name \
             the selector it reads: an input binding carries its ordering and what it produces, but \
             not which selector it was bound to"
        )));
    }
    // What is left has a constant on one side, and a modifier is refused rather than dropped. This
    // is deliberately narrower than Prometheus, which drops an `on` or `ignoring` written on a
    // scalar rather than refusing it; refusing is the conservative choice here, because the tree's
    // matching has no field that could carry the modifier, so binding the operation would compute
    // something the query did not ask for. A fill value is refused for the same reason: the tree's
    // matching has no field for one, and dropping it would compute the operation without the fill
    // the query asked for.
    if refuses_modifier(binary.modifier.as_ref()) {
        return Err(not_implemented(&format!(
            "the vector matching in `{binary}`, which a constant operand has nothing to match: \
             `on`, `ignoring`, `group_left` and `group_right` name labels of a vector, and a fill \
             value fills in a match group that could miss"
        )));
    }
    // Two constants are a value rather than an operation over a vector. A `bool` comparison of two
    // of them folded to its number before this point, so what arrives here is a comparison without
    // `bool`, which Prometheus refuses to parse at all. Binding it would produce a node that only
    // fails when it is run, so it is refused here with what was found.
    if binary.lhs.value_type() == ValueType::Scalar && binary.rhs.value_type() == ValueType::Scalar
    {
        return Err(not_implemented(&format!(
            "the binary operation `{binary}` between two constants, which reports no value: a \
             comparison of two scalars is written with `bool` to say whether it holds, and one \
             written that way is folded to the number it means"
        )));
    }
    // Both operands are compiled before the node is built, so `2 / (x + 1)` nests. The constant side
    // is bound by [`compile_expr`]'s fold, which is what makes it a scalar node rather than an
    // expression the tree would have to run.
    Ok(broadcast(
        compile_expr(context, &binary.lhs)?,
        compile_expr(context, &binary.rhs)?,
        op,
        binary.return_bool(),
    ))
}

/// Wraps two compiled operands in the binary node that applies `op` to them.
///
/// The output is the vector side's own binding, because a scalar carries no fields: a broadcast keeps
/// the vector's fields and the grid it is evaluated on, which is the statement's grid on both sides.
/// Two scalars are possible as a nested operand — a pair that [`constant_value`] refuses to fold, such
/// as a comparison between two constants — and the node reports the left one's grid, which is the
/// statement's too. The operands keep the order they were written in, because a non-commutative
/// operator means what that order says.
fn broadcast(left: NativeNode, right: NativeNode, op: BinaryOp, return_bool: bool) -> NativeNode {
    // `compile_expr` binds a constant to a scalar node and a selection or a function to a vector, so
    // this is where the side the operation reports is decided.
    let output = if matches!(left.output, NativeType::Scalar(_)) {
        right.output.clone()
    } else {
        left.output.clone()
    };
    NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: scalar_matching(op, return_bool),
        },
        output,
    }
}

/// The matching a vector-and-constant operation records.
///
/// Nothing is matched: a scalar carries no labels, so [`NativeTree::validate`] broadcasts it and
/// reads neither `labels` nor `cardinality`. They carry the default a vector–vector operation of the
/// same operator would have, which is also the only matching PromQL allows here — it rejects
/// `on`, `ignoring`, `group_left` and `group_right` on a scalar–vector operation. `return_bool` is
/// the one modifier that changes what this operation computes: `x > 0` filters the vector and
/// `x > bool 0` reports zero or one for each of its values.
fn scalar_matching(op: BinaryOp, return_bool: bool) -> BinaryMatching {
    BinaryMatching {
        op,
        return_bool,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        // A fill value fills in a match group that could miss, which a broadcast over a constant never
        // does: the constant exists at every grid point, so there is no missing side to fill.
        fill: FillValues::default(),
    }
}

/// The binary operator a PromQL token denotes, or `None` when there is no node for it.
///
/// A comparison is bound like an arithmetic operator, because the node carries `return_bool`
/// separately: `x > 0` and `x > bool 0` are one operator with one field changed. [`arithmetic_op`]
/// is not reused because it decides what a *fold* may do at compile time and a comparison folds
/// nothing, while this map decides what a *node* computes once the query runs.
fn binary_op(op: &TokenType) -> Option<BinaryOp> {
    match op.id() {
        token::T_ADD => Some(BinaryOp::Add),
        token::T_SUB => Some(BinaryOp::Sub),
        token::T_MUL => Some(BinaryOp::Mul),
        token::T_DIV => Some(BinaryOp::Div),
        token::T_MOD => Some(BinaryOp::Mod),
        token::T_POW => Some(BinaryOp::Pow),
        token::T_ATAN2 => Some(BinaryOp::Atan2),
        token::T_EQLC => Some(BinaryOp::Eq),
        token::T_NEQ => Some(BinaryOp::Ne),
        token::T_GTR => Some(BinaryOp::Gt),
        token::T_LSS => Some(BinaryOp::Lt),
        token::T_GTE => Some(BinaryOp::Ge),
        token::T_LTE => Some(BinaryOp::Le),
        _ => None,
    }
}

/// Compiles an aggregation into an aggregate node over the expression it groups.
///
/// Every aggregator that folds a group's values into one value per grid point binds, whether or not
/// its kernel exists yet: `sum`, `avg`, `count`, `min`, `max`, `group`, `stddev`, `stdvar` and
/// `quantile`. Binding a shape the compiler can describe and letting the runtime refuse a kind it
/// cannot execute keeps this the one place that says what an aggregate means, so a kernel landing
/// later changes no binding.
///
/// The rest of the parser's aggregates are a different shape, and are refused with the reason rather
/// than folded into a kind that would compute something else: `topk`, `bottomk`, `limitk` and
/// `limit_ratio` select some of the input's series, so they do not report one value per group per
/// grid point, and `count_values` writes one output label per distinct value rather than folding a
/// group's values at all.
///
/// The grouping is bound in the tree's own terms, because `by` and `without` are not each other's
/// inverse: `by(job)` keeps `job` and drops every other label, while `without(instance)` keeps every
/// label it does not name and drops `__name__` as well, so a list of the surviving labels could not
/// say the same thing. A bare `sum(x)` groups by no label at all, which is Prometheus's reading of
/// "no modifier": every label is dropped, `__name__` included, and every series of the input falls
/// into one group.
///
/// The stage is [`AggregateStage::Complete`], which says that every contribution to each group is in
/// this execution's input. The module documentation says what keeping that claim costs a caller.
fn aggregate_expr(context: &Context, aggregate: &AggregateExpr) -> Result<NativeNode> {
    let AggregateExpr {
        op,
        expr,
        param,
        modifier,
    } = aggregate;
    let kind = match op.id() {
        token::T_SUM => AggregateKind::Sum,
        token::T_AVG => AggregateKind::Avg,
        token::T_COUNT => AggregateKind::Count,
        token::T_MIN => AggregateKind::Min,
        token::T_MAX => AggregateKind::Max,
        token::T_GROUP => AggregateKind::Group,
        token::T_STDDEV => AggregateKind::Stddev,
        token::T_STDVAR => AggregateKind::Stdvar,
        token::T_QUANTILE => {
            // `quantile` is the only one of the nine with a parameter, and the parser always gives
            // it one: a `quantile` with no quantile written is refused before the compiler sees it.
            // The quantile is the call's first argument, so a parameter that is not a constant is
            // refused under that name rather than answered from a number bound too early.
            let param = param.as_ref().ok_or_else(|| {
                not_implemented(&format!(
                    "the aggregate `{op}` in `{aggregate}`, which takes its quantile as its first \
                     argument"
                ))
            })?;
            AggregateKind::Quantile {
                quantile: constant_argument(&op.to_string(), param, 0)?,
            }
        }
        // A ranking aggregate keeps some of the input's series, so what it reports is not one value
        // per group per grid point: `topk(3, x)` answers with three series, not with the groups'
        // folded values.
        token::T_TOPK | token::T_BOTTOMK | token::T_LIMITK | token::T_LIMIT_RATIO => {
            return Err(not_implemented(&format!(
                "the aggregate `{op}` in `{aggregate}`, which selects some of its input's series \
                 rather than folding each group's values into one value per grid point"
            )));
        }
        // `count_values` folds nothing: it writes one output label per distinct value it saw, which
        // is a shape no folded value can spell.
        token::T_COUNT_VALUES => {
            return Err(not_implemented(&format!(
                "the aggregate `{op}` in `{aggregate}`, which writes one output label per distinct \
                 value rather than folding each group's values into one value per grid point"
            )));
        }
        // The parser's `aggregate_op` rule lists exactly the fourteen names above, so this arm is
        // an operator this compiler was not taught rather than one it means to fold.
        _ => {
            return Err(not_implemented(&format!(
                "the aggregate `{op}` in `{aggregate}`"
            )));
        }
    };
    let input = compile_expr(context, expr)?;
    // A complete aggregate folds each group's values and reports them on its input's grid, one value
    // per input field, so its layout is its input's.
    let output = input.output.clone();
    Ok(NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(input),
            op: BoundAggregate {
                kind,
                group: grouping(modifier.as_ref()),
                stage: AggregateStage::Complete,
            },
        },
        output,
    })
}

/// How a `by` or `without` modifier identifies a group, in the tree's own terms.
///
/// The labels keep the order they were written in: a grouping is a set, and the tree records the
/// query rather than a key it derived from one.
fn grouping(modifier: Option<&LabelModifier>) -> Grouping {
    /// The written labels, as the tree stores them.
    fn labels(labels: &[String]) -> Arc<[Arc<str>]> {
        labels
            .iter()
            .map(|label| Arc::<str>::from(label.as_str()))
            .collect()
    }
    match modifier {
        // No modifier and `by()` ask for the same grouping: no label is kept, so every series of the
        // input falls into one group.
        None => Grouping::By(Arc::new([])),
        Some(LabelModifier::Include(by)) => Grouping::By(labels(&by.labels)),
        Some(LabelModifier::Exclude(without)) => Grouping::Without(labels(&without.labels)),
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
    if let Some((op, vector)) = vector_op(func.name, &args.args)? {
        return vector_call(context, func.name, op, vector);
    }
    // The name is looked up before the call's arguments are read, so a name this compiler has no
    // kernel for is refused as such whichever argument it was given: `absent(x)` is refused as a
    // name with no kernel, not as a selector this compiler could not compile.
    let Some((op, vector)) = math_op(func.name, &args.args)? else {
        return Err(not_implemented(&format!(
            "a call to `{}`, which has no native kernel",
            func.name
        )));
    };
    map_call(context, func.name, op, vector)
}

/// Binds a range function over the matrix it is given, and the window that matrix opens.
///
/// Where the matrix sits is the signature's, not a convention: `quantile_over_time(0.9, x[5m])` writes
/// its constant before the matrix, while `predict_linear(x[5m], 300)` and
/// `double_exponential_smoothing(x[5m], 0.5, 0.1)` write theirs after it, and a function with no
/// constants at all reads the matrix first. [`RangeSignature::selector_index`] is that position, and
/// everything else the call wrote is a constant — which keeps the position it was written at, so a
/// rejection names the argument the query wrote rather than the constant's offset among the constants.
///
/// The matrix is either shape: a range selector, which reads raw samples, or a subquery, which
/// evaluates its own expression on a grid of its own.
fn window_call(
    context: &Context,
    name: &str,
    signature: RangeSignature,
    args: &[Box<PromExpr>],
) -> Result<NativeNode> {
    // The count is the signature's, so the rejection names what this function takes rather than
    // what one argument happens to be.
    let expected = signature.arity();
    if args.len() != expected {
        return Err(not_implemented(&format!(
            "a call to `{name}` with {} argument(s), where it takes {expected}: {}",
            args.len(),
            signature.arguments()
        )));
    }
    // The matrix sits where the signature says it does, and everything else is a constant. Reading it
    // by position rather than by "the last argument" is what lets `predict_linear(x[5m], 300)` and
    // `quantile_over_time(0.9, x[5m])` share one path.
    let selector_index = signature.selector_index();
    // The selector stays in its original argument position for signature binding, while either
    // matrix form can provide the samples folded by the range function.
    let argument = &args[selector_index];
    let function = signature.bind(name, args)?;
    match unparen(argument) {
        PromExpr::MatrixSelector(MatrixSelector { vs, range }) => {
            window_selection(context, function, vs, range)
        }
        PromExpr::Subquery(subquery) => subquery_window(context, name, function, subquery),
        other => Err(not_implemented(&format!(
            "a call to `{name}` whose matrix is `{other}`, where it has to be a range \
             selector like `x[5m]` or a subquery like `x[5m:1m]`"
        ))),
    }
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

/// Wraps the vector operation `op` over the value of `vector`, which keeps its type as a map does.
fn vector_call(
    context: &Context,
    name: &str,
    op: BoundVectorOp,
    vector: &PromExpr,
) -> Result<NativeNode> {
    if vector.value_type() != ValueType::Vector {
        return Err(not_implemented(&format!(
            "a call to `{name}` whose vector argument is `{vector}`, which is a {}, where the \
             function takes a vector",
            vector.value_type()
        )));
    }
    let input = compile_expr(context, vector)?;
    let output = input.output.clone();
    Ok(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(input),
            op,
        },
        output,
    })
}

/// The vector operation a sort, `absent` or a classic histogram function binds, and the vector it reads,
/// or `None` when `name` is none of them.
fn vector_op<'a>(
    name: &str,
    arguments: &'a [Box<PromExpr>],
) -> Result<Option<(BoundVectorOp, &'a PromExpr)>> {
    let order = match name {
        "sort" => Some(SortOrder::Asc),
        "sort_desc" => Some(SortOrder::Desc),
        "sort_by_label" | "sort_by_label_desc" => None,
        "histogram_quantile" | "histogram_fraction" => {
            let Some((vector, constants)) = arguments.split_last() else {
                return Err(not_implemented(&format!(
                    "a call to `{name}` with no argument"
                )));
            };
            let function = match constants {
                [quantile] if name == "histogram_quantile" => {
                    HistogramFunction::Quantile(constant_argument(name, quantile, 0)?)
                }
                [lower, upper] if name == "histogram_fraction" => HistogramFunction::Fraction {
                    lower: constant_argument(name, lower, 0)?,
                    upper: constant_argument(name, upper, 1)?,
                },
                _ => {
                    return Err(not_implemented(&format!(
                        "a call to `{name}` with {} argument(s)",
                        arguments.len()
                    )));
                }
            };
            return Ok(Some((BoundVectorOp::Histogram(function), vector)));
        }
        "absent" => {
            let [vector] = arguments else {
                return Err(not_implemented(&format!(
                    "a call to `absent` with {} argument(s)",
                    arguments.len()
                )));
            };
            let labels = absent_labels_of(vector)?;
            return Ok(Some((BoundVectorOp::Absent { labels }, vector)));
        }
        _ => return Ok(None),
    };
    let Some((vector, strings)) = arguments.split_first() else {
        return Err(not_implemented(&format!(
            "a call to `{name}` with no argument"
        )));
    };
    let order = match order {
        Some(order) if strings.is_empty() => order,
        Some(_) => {
            return Err(not_implemented(&format!(
                "a call to `{name}` with {} argument(s)",
                arguments.len()
            )));
        }
        None => {
            let labels = strings
                .iter()
                .map(|argument| match unparen(argument) {
                    PromExpr::StringLiteral(StringLiteral { val }) => Ok(Arc::from(val.as_str())),
                    other => Err(not_implemented(&format!(
                        "a call to `{name}` whose argument `{other}` is not a string literal"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            if name == "sort_by_label" {
                SortOrder::ByLabel(labels)
            } else {
                SortOrder::ByLabelDesc(labels)
            }
        }
    };
    Ok(Some((BoundVectorOp::Sort(order), vector)))
}

/// The labels `absent(expr)` answers with, as Prometheus `createLabelsForAbsentFunction` derives
/// them: from the matchers of a vector selector, and none for any other expression.
///
/// `__field__`, `__schema__` and `__database__` choose what is read rather than match a label, so
/// they are not labels of the answer, as in the existing planner.
fn absent_labels_of(expr: &PromExpr) -> Result<MetricName> {
    let PromExpr::VectorSelector(selector) = unparen(expr) else {
        return absent_labels(&[]);
    };
    if !selector.matchers.or_matchers.is_empty() {
        return Err(not_implemented(&format!(
            "`absent` over `{selector}`, whose matchers are alternatives"
        )));
    }
    let matchers = selector
        .matchers
        .matchers
        .iter()
        .filter(|matcher| {
            !matches!(
                matcher.name.as_str(),
                "__field__" | "__schema__" | "__database__"
            )
        })
        .map(|matcher| LabelMatcher {
            name: Arc::from(matcher.name.as_str()),
            op: match matcher.op {
                ParserMatchOp::Equal => MatchOp::Equal,
                ParserMatchOp::NotEqual => MatchOp::NotEqual,
                ParserMatchOp::Re(_) => MatchOp::Regex,
                ParserMatchOp::NotRe(_) => MatchOp::NotRegex,
            },
            value: Arc::from(matcher.value.as_str()),
            matches_empty: matcher.is_match(""),
        })
        .collect::<Vec<_>>();
    absent_labels(&matchers)
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
    let output_grid = Arc::clone(&context.grid);

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

/// The step a subquery names when it names none, as in `x[5m:]`.
///
/// Prometheus falls back to the global evaluation interval, whose default is a minute, rather than to
/// the statement's own step. The existing planner folds the same default; the constant is restated
/// here because that one is private to the planner, and it is a Prometheus protocol constant rather
/// than a number either caller may choose.
const DEFAULT_SUBQUERY_STEP_MS: i64 = 60_000;

/// Binds `function` over `subquery`, whose inner expression is evaluated on a grid of its own.
///
/// A range function folds a matrix, and PromQL writes a matrix two ways. A range selector reads raw
/// samples from the scan; a subquery builds one by evaluating an expression. That is why this node
/// declares `input_grid`: `rate(x[5m:1m])` samples `x` at one-minute steps over the five-minute
/// lookback, and the window folds those points rather than raw samples. The inner expression is
/// compiled against the subquery's grid for the same reason — a selector inside it has to land where
/// the window will read it.
///
/// An `@` anchor makes the subquery step-invariant: every output point reads the same interval,
/// `(anchor - offset - range, anchor - offset]`. The child grid is built for that interval, while
/// the output grid remains the statement's grid; the anchor, range and offset are bound together in
/// the window.
fn subquery_window(
    context: &Context,
    name: &str,
    function: RangeFunction,
    subquery: &SubqueryExpr,
) -> Result<NativeNode> {
    let SubqueryExpr {
        expr,
        range,
        step,
        offset,
        at,
    } = subquery;
    // The anchor is resolved by the same function a range selector's is, so `start()`, `end()` and a
    // literal — including a negative one — mean the same thing in both shapes.
    let anchor_ms = selector_anchor_ms(at, context.stmt)?;
    // A subquery evaluates an expression and hands the window one value per grid point per field, so
    // the expression inside it has to be a vector: a scalar has no fields for the window to fold.
    if expr.value_type() != ValueType::Vector {
        return Err(not_implemented(&format!(
            "a call to `{name}` over the subquery `{subquery}`, whose expression is a {}, where a \
             subquery has to evaluate a vector",
            expr.value_type()
        )));
    }
    let range_ms = range_millis(range.as_millis())?;
    // A subquery step of zero names no evaluation point at all, so it is reported as its own value
    // rather than as an empty grid.
    let step_ms = match step {
        Some(step) => millis(step.as_millis()).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "the subquery's step of {}ms is out of range",
                step.as_millis()
            ))
        })?,
        None => DEFAULT_SUBQUERY_STEP_MS,
    };
    if step_ms <= 0 {
        return Err(DataFusionError::Plan(format!(
            "the subquery's step is {step_ms}ms, and a step has to be longer than zero"
        )));
    }
    let offset_ms = selector_offset_ms(offset)?;
    let window = RangeWindow {
        range_ms,
        offset_ms,
        anchor_ms,
    };
    let child_spec = subquery_grid(context.grid.spec(), window, step_ms)?.ok_or_else(|| {
        not_implemented(&format!(
            "the subquery over {range_ms}ms at {step_ms}ms per step, which names no evaluation point \
             in its anchored interval or the statement's range"
        ))
    })?;
    let child_grid = Arc::new(EvalGrid::try_deferred(child_spec).map_err(grid_error)?);
    let inner = compile_expr(
        &Context {
            stmt: context.stmt,
            input: Arc::clone(&context.input),
            ordering: context.ordering,
            grid: Arc::clone(&child_grid),
        },
        expr,
    )?;

    // A range function reports one value per input field, and the inner expression reports one per
    // field of the same raw input, so the layout is the input's.
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::clone(&context.input.fields),
        eval_grid: Some(Arc::clone(&context.grid)),
    }));

    Ok(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function,
                window,
                // The one field that tells this window from a raw one: its input was evaluated on the
                // subquery's grid rather than read from the scan.
                input_grid: Some(child_grid),
                output_grid: Arc::clone(&context.grid),
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
    let output_grid = Arc::clone(&context.grid);

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
///
/// Visible to the planner because the binding a [`NativeTree`] is planned against has to name the
/// same grid the tree itself was compiled on, and [`compile`]'s callers therefore read it here
/// rather than deriving the grid a second time.
pub(crate) fn statement_grid(stmt: &EvalStmt) -> Result<Arc<EvalGrid>> {
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
    Ok(Arc::new(EvalGrid::try_deferred(spec).map_err(grid_error)?))
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

/// What a call to a range function has to be: where its range selector sits, how many constants it
/// reads, and how they bind.
///
/// The selector's position is a variant rather than a convention because PromQL does not put it in
/// one place: `quantile_over_time(0.9, x[5m])` writes its quantile before the selector, while
/// `predict_linear(x[5m], 300)` and `double_exponential_smoothing(x[5m], 0.5, 0.1)` write theirs
/// after it. A single "parameters come first" rule would read the selector as a constant in the
/// second group and reject an ordinary query, so each shape states its own layout.
#[derive(Clone, Copy)]
enum RangeSignature {
    /// A function that reads only its range selector, as in `rate(x[5m])`.
    Selector(RangeFunction),
    /// One constant, then the range selector, as in `quantile_over_time(0.9, x[5m])`.
    ConstantBeforeSelector(fn(f64) -> RangeFunction),
    /// The range selector, then one constant, as in `predict_linear(x[5m], 300)`.
    ConstantAfterSelector(fn(f64) -> RangeFunction),
    /// The range selector, then two constants, as in
    /// `double_exponential_smoothing(x[5m], 0.5, 0.1)`.
    TwoConstantsAfterSelector(fn(f64, f64) -> RangeFunction),
}

impl RangeSignature {
    /// The position of the range selector among the call's arguments.
    fn selector_index(self) -> usize {
        match self {
            Self::Selector(_)
            | Self::ConstantAfterSelector(_)
            | Self::TwoConstantsAfterSelector(_) => 0,
            Self::ConstantBeforeSelector(_) => 1,
        }
    }

    /// How many constants the function reads.
    fn constants(self) -> usize {
        match self {
            Self::Selector(_) => 0,
            Self::ConstantBeforeSelector(_) | Self::ConstantAfterSelector(_) => 1,
            Self::TwoConstantsAfterSelector(_) => 2,
        }
    }

    /// How many arguments the call writes, its range selector counted in.
    fn arity(self) -> usize {
        self.constants() + 1
    }

    /// What the call's arguments are, for the arity rejection to name them.
    fn arguments(self) -> &'static str {
        match self {
            Self::Selector(_) => "its range selector",
            Self::ConstantBeforeSelector(_) => "a number and its range selector",
            Self::ConstantAfterSelector(_) => "its range selector and a number",
            Self::TwoConstantsAfterSelector(_) => "its range selector and two numbers",
        }
    }

    /// Binds the function from the call's arguments, which have already passed the arity check.
    fn bind(self, name: &str, args: &[Box<PromExpr>]) -> Result<RangeFunction> {
        Ok(match self {
            Self::Selector(function) => function,
            Self::ConstantBeforeSelector(bind) => bind(constant_argument(name, &args[0], 0)?),
            Self::ConstantAfterSelector(bind) => bind(constant_argument(name, &args[1], 1)?),
            Self::TwoConstantsAfterSelector(bind) => bind(
                constant_argument(name, &args[1], 1)?,
                constant_argument(name, &args[2], 2)?,
            ),
        })
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
        "quantile_over_time" => Some(RangeSignature::ConstantBeforeSelector(
            RangeFunction::QuantileOverTime,
        )),
        // These two write their constants after the selector, which is the opposite order to
        // `quantile_over_time`.
        "predict_linear" => Some(RangeSignature::ConstantAfterSelector(
            RangeFunction::PredictLinear,
        )),
        "double_exponential_smoothing" => Some(RangeSignature::TwoConstantsAfterSelector(
            |smoothing, trend| RangeFunction::DoubleExponentialSmoothing { smoothing, trend },
        )),
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
        // evaluation time, which has no input to map and is refused by `math_op`.
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
        if let MathSignature::Unary(MathOp::Date(_)) = signature {
            return Err(not_implemented(&format!(
                "a call to `{name}` without an argument, which reads the evaluation time and has \
                 no native kernel"
            )));
        }
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
/// `name` is whoever reads the argument: a call's function name, or an aggregator's, since
/// `quantile(0.9, x)` reads a constant too. The rejection therefore names that and the position
/// rather than calling everything a call, which would be false for an aggregate.
///
/// A quantile outside `[0, 1]` is not refused here: Prometheus binds it and the kernel reports
/// `-Infinity` below zero and `+Infinity` above one, so refusing it would be this compiler inventing
/// a rule the query engine does not have.
fn constant_argument(name: &str, argument: &PromExpr, position: usize) -> Result<f64> {
    constant_value(argument).ok_or_else(|| {
        not_implemented(&format!(
            "`{name}`, whose {} is `{argument}`, where it has to be a constant expression this \
             compiler folds to a number literal",
            argument_position(position)
        ))
    })
}

/// How a rejection names the argument a parameter was read from.
///
/// The three positions are the ones a supported call can write: a math function's vector and the two
/// constants behind it, or a range function's selector and the constants on either side of it. Every
/// caller passes the literal position it read, so there is no fourth position to name and no default
/// to invent one for.
fn argument_position(position: usize) -> &'static str {
    const POSITIONS: [&str; 3] = ["first argument", "second argument", "third argument"];
    POSITIONS[position]
}

/// Whether a binary operation carries a modifier this compiler refuses to bind.
///
/// A scalar carries no labels, so there is nothing to match or name, and the tree's matching has no
/// field for a fill value: binding the operation would compute it without the modifier the query
/// asked for. The fold and [`binary_operation`] share this test rather than restating it, because a
/// fold that computed a constant without reading its modifier would answer a query the dispatch
/// below refuses.
fn refuses_modifier(modifier: Option<&BinModifier>) -> bool {
    modifier.is_some_and(|modifier| {
        modifier.matching.is_some()
            || modifier.card != VectorMatchCardinality::OneToOne
            || modifier.fill_values != VectorMatchFillValues::default()
    })
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
            // A modifier is read before the operands are folded: `x * (1 + fill(0) 2)` asks for a
            // fill, and folding the inner addition would answer `x * 3` with the fill gone. The
            // expression is left unfolded so the dispatch below refuses it with the reason it
            // writes, which is also why this is a `None` rather than an error here.
            if refuses_modifier(binary.modifier.as_ref()) {
                return None;
            }
            let left = constant_value(&binary.lhs)?;
            let right = constant_value(&binary.rhs)?;
            if let Some(op) = arithmetic_op(&binary.op) {
                return Some(op.calculate(left, right));
            }
            // A comparison of two constants is the number it reports when it is written with
            // `bool`: Prometheus reads `1 > bool 2` as `0`, and [`ComparisonOp::apply`] is the same
            // answer the kernel gives. Without `bool` there is no number to report -- `1 > 2` is a
            // parse error in Prometheus -- so it is left unfolded and the dispatch refuses it.
            if binary.return_bool()
                && let Some(op) = comparison_op(&binary.op)
            {
                return Some(f64::from(op.apply(left, right)));
            }
            None
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
/// arithmetic here rather than a wrong operator. A comparison is `None` here and is folded by
/// [`comparison_op`] instead, because only a `bool` comparison of two constants has a number to
/// report; a set operator folds nothing at all.
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

/// The comparison operator a PromQL token denotes, or `None` when the token is not a comparison.
///
/// This is the folding half of [`binary_op`], which binds the same tokens to a node: the fold and
/// the node have to agree on which token is which comparison, so the two maps sit beside each other.
fn comparison_op(op: &TokenType) -> Option<ComparisonOp> {
    match op.id() {
        token::T_EQLC => Some(ComparisonOp::Equal),
        token::T_NEQ => Some(ComparisonOp::NotEqual),
        token::T_GTR => Some(ComparisonOp::Greater),
        token::T_LSS => Some(ComparisonOp::Less),
        token::T_GTE => Some(ComparisonOp::GreaterEqual),
        token::T_LTE => Some(ComparisonOp::LessEqual),
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
        // These functions are supported unconditionally by this native compiler.
        "first_over_time" => Some(RangeFunction::FirstOverTime),
        "mad_over_time" => Some(RangeFunction::MadOverTime),
        "ts_of_first_over_time" => Some(RangeFunction::TsOfFirstOverTime),
        "ts_of_last_over_time" => Some(RangeFunction::TsOfLastOverTime),
        "ts_of_min_over_time" => Some(RangeFunction::TsOfMinOverTime),
        "ts_of_max_over_time" => Some(RangeFunction::TsOfMaxOverTime),
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
