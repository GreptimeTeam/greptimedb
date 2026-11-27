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
//! Each expression kind binds to one node, recursively: a selector to `Input` → `Instant`, a
//! range function to `Window` over raw samples or over an evaluated subquery on its own grid,
//! functions to `Map` or `Vector`, operators to `Binary` or `Set`, aggregators to `Aggregate` or
//! `Vector`.
//! Constant arguments are folded by [`constant_value`]. Any other shape is a
//! [`DataFusionError::NotImplemented`] that quotes the accepted shapes.
//!
//! Aggregates are bound as [`AggregateStage::Complete`]: the caller must feed every contribution to
//! a group into one execution, or split the aggregation into `Partial` and `Final`.
//!
//! Tree legality is checked by [`NativeTree::try_new`], not here. An `Interleaved` scan ordering is
//! refused at the entry because no node can consume interleaved series.

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{SeriesLabels, SeriesOrdering};
use promql::extension_plan::FieldKind;
use promql::histogram_eval::{HistogramFunction, NativeHistogramFunction};
use promql::info::InfoSelector;
use promql::map::MathOp;
use promql::map_label::LabelRewrite;
use promql::map_time::DateFunction;
use promql::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundSetOp, BoundVectorOp, BoundWindow, Grouping, InstantValue, MapOp, NativeExpr, NativeNode,
    NativeTree, NativeType, ParameterizedOp,
};
use promql::range_eval::{RangeFunction, RangeWindow, subquery_grid};
use promql::select::{LabelMatcher, MatchOp, SortOrder, absent_labels};
use promql::vector_match::{
    BinaryKind, BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels, SetOp,
    binary_output_schema,
};
use promql_parser::label::{MatchOp as ParserMatchOp, Matcher, Matchers};
use promql_parser::parser::token::TokenType;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, AtModifier, BinModifier, BinaryExpr, Call, EvalStmt, Expr as PromExpr,
    LabelModifier, MatrixSelector, NumberLiteral, Offset, StringLiteral, SubqueryExpr,
    VectorMatchCardinality, VectorMatchFillValues, VectorSelector, token,
};

use crate::promql::label_values::signed_millis_since_epoch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectorTiming {
    Instant { lookback_ms: i64 },
    Window { range_ms: i64 },
}

#[derive(Clone, Debug)]
pub struct SelectorRequirement {
    pub metric: Option<String>,
    pub matchers: Matchers,
    pub offset_ms: i64,
    pub at_ms: Option<i64>,
    pub grid_start_ms: i64,
    pub grid_end_ms: i64,
    pub timing: SelectorTiming,
}

impl SelectorRequirement {
    /// Returns `(start_ms, end_ms, include_start)` for the scan hull covering this selector.
    ///
    /// The end is inclusive; the start is inclusive only for a zero-lookback instant selector.
    /// `grid_end_ms` is the grid's last point, not the statement's end.
    pub fn raw_scan_bounds(&self) -> Result<(i64, i64, bool)> {
        let end = self
            .at_ms
            .unwrap_or(self.grid_end_ms)
            .checked_sub(self.offset_ms)
            .ok_or_else(|| DataFusionError::Plan("selector scan end overflows".into()))?;
        let lower = self
            .at_ms
            .unwrap_or(self.grid_start_ms)
            .checked_sub(self.offset_ms)
            .ok_or_else(|| DataFusionError::Plan("selector scan start overflows".into()))?;
        let (duration, inclusive) = match self.timing {
            SelectorTiming::Instant { lookback_ms } => (lookback_ms, lookback_ms == 0),
            SelectorTiming::Window { range_ms } => (range_ms, false),
        };
        let start = lower
            .checked_sub(duration)
            .ok_or_else(|| DataFusionError::Plan("selector scan start overflows".into()))?;
        Ok((start, end, inclusive))
    }
}

/// One compiled selector occurrence and the scan layout bound for it.
#[derive(Clone, Debug)]
pub struct CompiledInput {
    /// Occurrence number in compiler traversal order, starting at zero. Several occurrences may map
    /// to one scan.
    pub source_index: usize,
    pub requirement: SelectorRequirement,
    pub schema: Arc<SeriesSchema>,
    pub ordering: SeriesOrdering,
}

#[derive(Clone, Debug)]
pub struct CompiledTree {
    pub tree: NativeTree,
    pub inputs: Arc<[CompiledInput]>,
}

/// The shapes this compiler accepts, quoted in every rejection.
const SUPPORTED: &str = "a selector — `x`, optionally with an `offset` — a range function over \
                         a matrix, where a matrix is `<selector>[<range>]` as in `rate(x[5m])` or \
                         `<expression>[<range>:<step>]` as in `rate(x[5m:1m])`, with the step \
                         optionally omitted as `[<range>:]`, where the subquery's `<expression>` \
                         is any shape here, so a subquery nests on a grid of its own as in \
                         `rate(rate(x[5m:1m])[10m:30s])`, and where either matrix shape carries the \
                         `offset` and `@` anchor written on it as in `rate(x[5m] @ 1234)`; a math \
                         function over any of them (`abs(rate(x[5m]))`, `clamp_min(x, 0)`), with \
                         `round` and the `clamp` functions also taking scalars \
                         (`round(x, time() / 10)`), a native \
                         histogram function over any of them (`histogram_count(x)`), a label \
                         function over any of them \
                         (`label_replace(x, \"a\", \"$1\", \"b\", \"(.*)\")`), a binary \
                         operation between one of them and a scalar (`2 / (x + 1)`, \
                         `x > bool time()`, `time() - timestamp(x)`), a scalar — a constant, \
                         `time()`, `scalar(<expression>)` or a binary operation between \
                         scalars — \
                         `vector(<scalar>)`, a date function without an argument (`hour()`), a \
                         negation of one of them (`-x`), a set operation between two of them \
                         (`x and on(job) y`, `x or y`, `x unless y`), `info` over any of them with \
                         one info metric (`info(x, {version=~\".+\"})`), or one of the \
                         aggregators that folds a group's values into one value per grid point — \
                         `sum`, `avg`, `count`, `min`, `max`, `group`, `stddev`, `stdvar` and \
                         `quantile(<scalar>, <expression>)` — over any of them, optionally \
                         grouped by `by` or `without` as in `avg by(job)(rate(x[5m]) * 100)`; \
                         `topk`, `bottomk`, `limitk`, `limit_ratio` or `count_values` over any of \
                         them, as in `topk(3, x) by(job)` and `count_values(\"v\", x)`; a \
                         sort, `absent`, `absent_over_time` or `histogram_quantile` / \
                         `histogram_fraction` over any of them (`sort_desc(x)`, \
                         `absent(x{job=\"a\"})`, `absent_over_time(x{job=\"a\"}[5m])`, \
                         `histogram_quantile(0.9, rate(x[5m]))`); a range function's own \
                         constants sit where that function takes them, before \
                         the matrix for `quantile_over_time(0.9, x[5m])` and after it for \
                         `predict_linear(x[5m], 300)` and \
                         `double_exponential_smoothing(x[5m], 0.5, 0.1)`";

/// Compiles a single-selector `stmt` into a tree that reads `input`, bound as floats.
///
/// `input` must be the schema after the selector's `__field__`, `__schema__` and `__database__`
/// matchers are applied; binding every field of the table folds fields the query did not select.
/// `ordering` is what the scan guarantees; `Interleaved` is refused. A statement without exactly one
/// selector is refused; use [`compile_selectors`] for several.
pub fn compile(
    stmt: &EvalStmt,
    input: Arc<SeriesSchema>,
    ordering: SeriesOrdering,
) -> Result<NativeTree> {
    // The planner passes the table's field kind through `compile_selectors_with_grid`.
    let compiled = compile_selectors(
        stmt,
        &|_| Ok(Arc::clone(&input)),
        ordering,
        Some(FieldKind::Float),
    )?;
    if compiled.inputs.len() != 1 {
        return Err(not_implemented(&format!(
            "`compile` with {} selector inputs; use `compile_selectors` to bind each input",
            compiled.inputs.len()
        )));
    }
    Ok(compiled.tree)
}

fn data_free_value_field(stmt: &EvalStmt) -> Arc<str> {
    fn visit(expr: &PromExpr, targets: &mut Vec<String>) {
        match expr {
            PromExpr::Aggregate(aggregate) => {
                if aggregate.op.id() == token::T_COUNT_VALUES
                    && let Some(param) = &aggregate.param
                    && let PromExpr::StringLiteral(value) = unparen(param)
                {
                    targets.push(value.val.clone());
                }
                if let Some(modifier) = &aggregate.modifier {
                    match modifier {
                        LabelModifier::Include(labels) | LabelModifier::Exclude(labels) => {
                            targets.extend(labels.labels.iter().cloned());
                        }
                    }
                }
                // `topk` and friends take an expression parameter, whose groupings count too: an
                // aggregate refuses to group by the field it folds.
                if let Some(param) = &aggregate.param {
                    visit(param, targets);
                }
                visit(&aggregate.expr, targets);
            }
            PromExpr::Unary(unary) => visit(&unary.expr, targets),
            PromExpr::Binary(binary) => {
                if let Some(modifier) = &binary.modifier {
                    if let Some(matching) = &modifier.matching {
                        targets.extend(matching.labels().labels.iter().cloned());
                    }
                    match &modifier.card {
                        VectorMatchCardinality::OneToMany(labels)
                        | VectorMatchCardinality::ManyToOne(labels) => {
                            targets.extend(labels.labels.iter().cloned());
                        }
                        _ => {}
                    }
                }
                visit(&binary.lhs, targets);
                visit(&binary.rhs, targets);
            }
            PromExpr::Paren(paren) => visit(&paren.expr, targets),
            PromExpr::Subquery(subquery) => visit(&subquery.expr, targets),
            PromExpr::Call(call) => call.args.args.iter().for_each(|arg| visit(arg, targets)),
            _ => {}
        }
    }
    let mut targets = Vec::new();
    visit(&stmt.expr, &mut targets);
    if !targets.iter().any(|name| name == VECTOR_FIELD) {
        return Arc::from(VECTOR_FIELD);
    }
    (0usize..)
        .map(|index| format!("native_value_{index}"))
        .find(|candidate| !targets.iter().any(|name| name == candidate))
        .map(Arc::from)
        .expect("the value field candidate sequence is unbounded")
}

/// Compiles a statement that reads no data (a constant, `time()`, `vector(time())`, `hour()`) on
/// the statement's grid. A statement with a selector is refused.
pub fn compile_statement(stmt: &EvalStmt) -> Result<NativeTree> {
    let output_field = data_free_value_field(stmt);
    let compiled = compile_selectors_with_value_field(
        stmt,
        &|_| {
            Err(not_implemented(&format!(
                "a selector in `{}`, which is compiled as a statement that reads no data",
                stmt.expr
            )))
        },
        SeriesOrdering::Contiguous,
        output_field,
        // No source, so no source kind; the walk reads it only at an input node.
        None,
    )?;
    Ok(compiled.tree)
}

/// Compiles a data-free `stmt` on `grid` instead of the grid its bounds derive.
///
/// Used for a subquery root's inner expression and for statements that read no data; `@ start()`
/// and `@ end()` still resolve against the statement's bounds.
pub(crate) fn compile_statement_with_grid(
    stmt: &EvalStmt,
    grid: Arc<EvalGrid>,
) -> Result<NativeTree> {
    let output_field = data_free_value_field(stmt);
    let compiled = compile_selectors_with_grid_and_value_field(
        stmt,
        &|_| {
            Err(not_implemented(&format!(
                "a selector in `{}`, which is compiled as a statement that reads no data",
                stmt.expr
            )))
        },
        SeriesOrdering::Contiguous,
        output_field,
        Some(grid),
        None,
    )?;
    Ok(compiled.tree)
}

/// Compiles `stmt` into a tree and returns a binding record for each selector occurrence.
///
/// `bindings` is called once per selector, in traversal order, and returns its input schema.
pub fn compile_selectors(
    stmt: &EvalStmt,
    bindings: &dyn Fn(&SelectorRequirement) -> Result<Arc<SeriesSchema>>,
    ordering: SeriesOrdering,
    source_kind: Option<FieldKind>,
) -> Result<CompiledTree> {
    compile_selectors_with_value_field(
        stmt,
        bindings,
        ordering,
        Arc::from(VECTOR_FIELD),
        source_kind,
    )
}

/// Compiles `stmt` with an explicit evaluation grid. See [`compile_statement_with_grid`].
pub(crate) fn compile_selectors_with_grid(
    stmt: &EvalStmt,
    bindings: &dyn Fn(&SelectorRequirement) -> Result<Arc<SeriesSchema>>,
    ordering: SeriesOrdering,
    grid: Arc<EvalGrid>,
    source_kind: Option<FieldKind>,
) -> Result<CompiledTree> {
    compile_selectors_with_grid_and_value_field(
        stmt,
        bindings,
        ordering,
        Arc::from(VECTOR_FIELD),
        Some(grid),
        source_kind,
    )
}

fn compile_selectors_with_value_field(
    stmt: &EvalStmt,
    bindings: &dyn Fn(&SelectorRequirement) -> Result<Arc<SeriesSchema>>,
    ordering: SeriesOrdering,
    value_field: Arc<str>,
    source_kind: Option<FieldKind>,
) -> Result<CompiledTree> {
    compile_selectors_with_grid_and_value_field(
        stmt,
        bindings,
        ordering,
        value_field,
        None,
        source_kind,
    )
}

fn compile_selectors_with_grid_and_value_field(
    stmt: &EvalStmt,
    bindings: &dyn Fn(&SelectorRequirement) -> Result<Arc<SeriesSchema>>,
    ordering: SeriesOrdering,
    value_field: Arc<str>,
    grid: Option<Arc<EvalGrid>>,
    source_kind: Option<FieldKind>,
) -> Result<CompiledTree> {
    if ordering == SeriesOrdering::Interleaved {
        return Err(not_implemented(
            "raw samples whose series interleave, which this compiler cannot consume: a native \
             evaluation reads one series to its last chunk before the next, so chunks of different \
             series may not interleave",
        ));
    }
    if !matches!(
        stmt.expr.value_type(),
        ValueType::Vector | ValueType::Scalar
    ) {
        return Err(not_implemented(&format!(
            "the expression `{}`, which is a {} where a tree reports evaluated samples or a \
             scalar",
            stmt.expr,
            stmt.expr.value_type()
        )));
    }
    let inputs = RefCell::new(Vec::new());
    let context = Context {
        stmt,
        bindings,
        ordering,
        inputs: &inputs,
        grid: match grid {
            Some(grid) => grid,
            None => statement_grid(stmt)?,
        },
        value_field,
        source_kind,
    };
    let tree = NativeTree::try_new(compile_expr(&context, &stmt.expr)?)?;
    let inputs = inputs.into_inner();
    validate_inputs(tree.root(), &inputs)?;
    Ok(CompiledTree {
        tree,
        inputs: inputs.into(),
    })
}

fn validate_inputs(root: &NativeNode, inputs: &[CompiledInput]) -> Result<()> {
    let bound = root.inputs();
    if bound.len() == inputs.len()
        && bound
            .iter()
            .zip(inputs)
            .enumerate()
            .all(|(index, (bound, input))| {
                input.source_index == index
                    && bound.source_index == input.source_index
                    && bound.ordering == input.ordering
                    && bound
                        .output
                        .samples()
                        .is_some_and(|schema| Arc::ptr_eq(schema, &input.schema))
            })
    {
        return Ok(());
    }
    Err(DataFusionError::Internal(
        "a compiled tree's recorded inputs do not match its input nodes".into(),
    ))
}

/// Shared statement, per-selector binding callback, occurrence records, ordering and current grid.
///
/// A subquery substitutes its own grid while reusing the statement-level bindings and input list.
struct Context<'a> {
    stmt: &'a EvalStmt,
    bindings: &'a dyn Fn(&SelectorRequirement) -> Result<Arc<SeriesSchema>>,
    ordering: SeriesOrdering,
    inputs: &'a RefCell<Vec<CompiledInput>>,
    grid: Arc<EvalGrid>,
    value_field: Arc<str>,
    /// The field kind of the statement's sources; `None` for a statement without sources.
    source_kind: Option<FieldKind>,
}

fn compile_expr(context: &Context, expr: &PromExpr) -> Result<NativeNode> {
    // Constants fold first, so `x * (1 + 2)` binds `3` and no shape below handles a constant
    // operand.
    if let Some(value) = constant_value(expr) {
        return Ok(scalar_node(context, value));
    }
    match expr {
        PromExpr::Paren(paren) => compile_expr(context, &paren.expr),
        PromExpr::Call(call) => compile_call(context, call),
        PromExpr::VectorSelector(selector) => instant_selection(context, selector),
        PromExpr::Unary(unary) => negate(context, &unary.expr),
        PromExpr::Binary(binary) => binary_operation(context, binary),
        PromExpr::Aggregate(aggregate) => aggregate_expr(context, aggregate),
        found => Err(not_implemented(&format!("the expression `{found}`"))),
    }
}

/// Binds `value` as a scalar on the context's grid: the statement's, or the enclosing subquery's.
fn scalar_node(context: &Context, value: f64) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(Arc::clone(&context.grid)),
    }
}

/// Compiles `-x` as `-1 * x`.
fn negate(context: &Context, operand: &PromExpr) -> Result<NativeNode> {
    Ok(broadcast(
        scalar_node(context, -1.0),
        compile_expr(context, operand)?,
        BinaryOp::Mul,
        false,
    ))
}

/// Compiles a binary operation, preserving written operand order and vector matching modifiers.
fn binary_operation(context: &Context, binary: &BinaryExpr) -> Result<NativeNode> {
    if let Some(op) = set_op(&binary.op) {
        return set_operation(context, binary, op);
    }
    let Some(op) = binary_op(&binary.op) else {
        return Err(not_implemented(&format!(
            "the binary operator `{}` in `{binary}`",
            binary.op
        )));
    };
    if binary.lhs.value_type() == ValueType::Vector && binary.rhs.value_type() == ValueType::Vector
    {
        let left = compile_expr(context, &binary.lhs)?;
        let right = compile_expr(context, &binary.rhs)?;
        return bind_vector_binary(binary, left, right, op);
    }
    // One side is a scalar. Prometheus drops an empty `on()`/`ignoring()` here and rejects one with
    // labels or a fill at parse time; this refuses all of them.
    if refuses_modifier(binary.modifier.as_ref()) {
        return Err(not_implemented(&format!(
            "the vector matching in `{binary}`, which a constant operand has nothing to match: \
             `on`, `ignoring`, `group_left` and `group_right` name labels of a vector, and a fill \
             value fills in a match group that could miss"
        )));
    }
    // Prometheus refuses to parse a scalar–scalar comparison without `bool`. Two constants never
    // reach here: they fold.
    if binary.lhs.value_type() == ValueType::Scalar
        && binary.rhs.value_type() == ValueType::Scalar
        && matches!(op.kind(), BinaryKind::Comparison(_))
        && !binary.return_bool()
    {
        return Err(not_implemented(&format!(
            "the binary operation `{binary}` between two scalars, which reports no value: a \
             comparison of two scalars is written with `bool` to say whether it holds"
        )));
    }
    Ok(broadcast(
        compile_expr(context, &binary.lhs)?,
        compile_expr(context, &binary.rhs)?,
        op,
        binary.return_bool(),
    ))
}

/// Binds the same vector operation over compiled or already evaluated operands.
fn bind_vector_binary(
    binary: &BinaryExpr,
    left: NativeNode,
    right: NativeNode,
    op: BinaryOp,
) -> Result<NativeNode> {
    let (Some(lhs), Some(rhs)) = (left.output.samples(), right.output.samples()) else {
        return Err(DataFusionError::Internal(format!(
            "the binary operation `{binary}` compiled a vector operand to something other than samples"
        )));
    };
    let output = NativeType::Samples(binary_output_schema(lhs, rhs)?);
    Ok(NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: vector_matching(binary, op)?,
        },
        output,
    })
}

/// Uses the ordinary binary binder for two evaluated frame inputs.
pub(crate) fn bind_evaluated_binary(
    binary: &BinaryExpr,
    left: NativeNode,
    right: NativeNode,
) -> Result<NativeTree> {
    let op = binary_op(&binary.op).ok_or_else(|| {
        not_implemented(&format!(
            "the binary operator `{}` in `{binary}`",
            binary.op
        ))
    })?;
    NativeTree::try_new(bind_vector_binary(binary, left, right, op)?)
}

/// Compiles `and`, `or` or `unless` between two vectors, matched by the labels the query wrote.
///
/// The parser already refuses a scalar operand, `bool` and `group_left` / `group_right` on a set
/// operator. It does not refuse a fill value, which Prometheus does ("filling in missing series not
/// allowed for set operators"), so that is refused here along with any cardinality other than the
/// many-to-many the parser assigns.
fn set_operation(context: &Context, binary: &BinaryExpr, op: SetOp) -> Result<NativeNode> {
    let modifier = binary.modifier.as_ref();
    if modifier.is_some_and(|modifier| {
        modifier.card != VectorMatchCardinality::ManyToMany
            || modifier.fill_values != VectorMatchFillValues::default()
    }) {
        return Err(not_implemented(&format!(
            "the modifiers in `{binary}`: a set operator matches many-to-many and takes no fill \
             value"
        )));
    }
    let labels = matching_labels(modifier);
    let left = compile_expr(context, &binary.lhs)?;
    let right = compile_expr(context, &binary.rhs)?;
    let (Some(lhs), Some(rhs)) = (left.output.samples(), right.output.samples()) else {
        return Err(DataFusionError::Internal(format!(
            "the set operation `{binary}` compiled an operand to something other than samples: the \
             parser only accepts a set operator between two vectors"
        )));
    };
    // Prometheus has one value per sample, and the existing planner refuses a set operator over
    // several fields, so per-field presence has no reference semantics to follow.
    if lhs.fields.len() != 1 || rhs.fields.len() != 1 {
        return Err(not_implemented(&format!(
            "the set operation `{binary}` between vectors of {} and {} fields: a set operator is \
             compiled over one field on each side",
            lhs.fields.len(),
            rhs.fields.len()
        )));
    }
    // Kept samples pass through unchanged, so the output is the left side's layout.
    let output = left.output.clone();
    Ok(NativeNode {
        expr: NativeExpr::Set {
            left: Box::new(left),
            right: Box::new(right),
            op: BoundSetOp { op, labels },
        },
        output,
    })
}

/// Binds `op` between two operands, at least one of them a scalar, in written order.
///
/// The output is the vector side's layout, or the left scalar's grid when both are scalars (e.g.
/// `time() - 60`).
fn broadcast(left: NativeNode, right: NativeNode, op: BinaryOp, return_bool: bool) -> NativeNode {
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

/// The matching of an operation with a scalar side.
///
/// A scalar has no labels, so `labels` and `cardinality` are unused and hold the vector–vector
/// defaults. `return_bool` is the only modifier that applies.
fn scalar_matching(op: BinaryOp, return_bool: bool) -> BinaryMatching {
    BinaryMatching {
        op,
        return_bool,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        // A scalar exists at every grid point, so there is no missing side to fill.
        fill: FillValues::default(),
    }
}

/// The matching a vector–vector operation records.
///
/// Without a modifier this is PromQL's default: `ignoring()` and one-to-one. Fill values keep the
/// sides as written; the kernel swaps them with the operands under `group_right`. `ManyToMany`
/// belongs to set operators, which [`binary_operation`] dispatches first, so it is refused here.
fn vector_matching(binary: &BinaryExpr, op: BinaryOp) -> Result<BinaryMatching> {
    let modifier = binary.modifier.as_ref();
    let labels = matching_labels(modifier);
    let cardinality = match modifier.map(|modifier| &modifier.card) {
        None | Some(VectorMatchCardinality::OneToOne) => Cardinality::OneToOne,
        Some(VectorMatchCardinality::ManyToOne(labels)) => {
            Cardinality::ManyToOne(label_names(&labels.labels))
        }
        Some(VectorMatchCardinality::OneToMany(labels)) => {
            Cardinality::OneToMany(label_names(&labels.labels))
        }
        Some(VectorMatchCardinality::ManyToMany) => {
            return Err(not_implemented(&format!(
                "the many-to-many matching in `{binary}`, which is the cardinality of a set \
                 operator: it combines two vectors' series, and this node is an arithmetic or \
                 comparison operation over values"
            )));
        }
    };
    let fill = modifier.map_or_else(FillValues::default, |modifier| FillValues {
        lhs: modifier.fill_values.lhs,
        rhs: modifier.fill_values.rhs,
    });
    Ok(BinaryMatching {
        op,
        return_bool: binary.return_bool(),
        labels,
        cardinality,
        fill,
    })
}

/// The set operator a PromQL token denotes, or `None` when it is not one.
fn set_op(op: &TokenType) -> Option<SetOp> {
    match op.id() {
        token::T_LAND => Some(SetOp::And),
        token::T_LOR => Some(SetOp::Or),
        token::T_LUNLESS => Some(SetOp::Unless),
        _ => None,
    }
}

/// The labels `on(...)` or `ignoring(...)` names; with neither, every label but `__name__`.
fn matching_labels(modifier: Option<&BinModifier>) -> MatchingLabels {
    match modifier.and_then(|modifier| modifier.matching.as_ref()) {
        None => MatchingLabels::Ignoring(Vec::new()),
        Some(LabelModifier::Include(labels)) => MatchingLabels::On(label_names(&labels.labels)),
        Some(LabelModifier::Exclude(labels)) => {
            MatchingLabels::Ignoring(label_names(&labels.labels))
        }
    }
}

fn label_names(labels: &[String]) -> Vec<Arc<str>> {
    labels.iter().map(|name| Arc::from(name.as_str())).collect()
}

/// The binary operator a PromQL token denotes, or `None` when there is no node for it.
///
/// `x > 0` and `x > bool 0` map to the same operator; `return_bool` is carried separately.
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

/// Compiles an aggregation as a [`AggregateStage::Complete`] aggregate node.
///
/// `topk`, `bottomk`, `limitk`, `limit_ratio` and `count_values` do not fold a group into one value
/// and bind as a vector operation instead ([`vector_aggregate`]). A `quantile` whose φ is a
/// non-constant scalar binds as [`NativeExpr::ParameterizedAggregate`] and reads φ at every step,
/// as Prometheus does.
fn aggregate_expr(context: &Context, aggregate: &AggregateExpr) -> Result<NativeNode> {
    let AggregateExpr {
        op,
        expr,
        param,
        modifier,
    } = aggregate;
    // A `quantile` φ read per step, e.g. `quantile(scalar(foo), v)`.
    let mut per_point = None;
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
            let param = param.as_ref().ok_or_else(|| {
                not_implemented(&format!(
                    "the aggregate `{op}` in `{aggregate}`, which takes its quantile as its first \
                     argument"
                ))
            })?;
            // A constant φ folds below. A vector φ is left to `constant_argument` to refuse, as
            // Prometheus refuses `quantile(foo, v)` at type check.
            if constant_value(param).is_none() && param.value_type() == ValueType::Scalar {
                per_point = Some(param);
            }
            AggregateKind::Quantile {
                // Unused when φ is read per step.
                quantile: match per_point {
                    Some(_) => 0.0,
                    None => constant_argument(&op.to_string(), param, 0)?,
                },
            }
        }
        token::T_TOPK
        | token::T_BOTTOMK
        | token::T_LIMITK
        | token::T_LIMIT_RATIO
        | token::T_COUNT_VALUES => {
            return vector_aggregate(context, aggregate);
        }
        // Unreachable for the parser's `aggregate_op` rule, which lists exactly the names above.
        _ => {
            return Err(not_implemented(&format!(
                "the aggregate `{op}` in `{aggregate}`"
            )));
        }
    };
    let input = compile_expr(context, expr)?;
    // The state layout follows the direct input's kind, not the sources':
    // `sum(count_over_time(h[5m]))` folds floats built from histograms. An unnameable kind over
    // histogram sources is refused. Over float sources it is a float: a histogram sample can only
    // come from a histogram source.
    let field_kind = match input.output_field_kind(context.source_kind) {
        Some(kind) => kind,
        None if context.source_kind == Some(FieldKind::Histogram) => {
            return Err(not_implemented(&format!(
                "the aggregate `{op}` in `{aggregate}`, whose input's field kind this compile cannot \
                 name while its sources carry native histograms: the aggregate's state depends on \
                 whether the input carries floats or histograms"
            )));
        }
        None => FieldKind::Float,
    };
    let output = input.output.clone();
    let op = BoundAggregate {
        kind,
        group: grouping(modifier.as_ref()),
        stage: AggregateStage::Complete,
        field_kind,
    };
    // Input before parameter: selector occurrence numbers follow this order.
    let expr = match per_point {
        Some(parameter) => NativeExpr::ParameterizedAggregate {
            input: Box::new(input),
            op,
            parameter: Box::new(compile_expr(context, parameter)?),
        },
        None => NativeExpr::Aggregate {
            input: Box::new(input),
            op,
        },
    };
    Ok(NativeNode { expr, output })
}

/// Binds `topk`, `bottomk`, `limitk`, `limit_ratio` or `count_values` as a vector operation over
/// the expression it aggregates.
///
/// The parser gives each of them its parameter: a number for the first four, folded to the constant
/// it means like a `quantile`, and a string for `count_values`, the label it writes the value to.
fn vector_aggregate(context: &Context, aggregate: &AggregateExpr) -> Result<NativeNode> {
    let AggregateExpr {
        op,
        expr,
        param,
        modifier,
    } = aggregate;
    let name = op.to_string();
    let param = param.as_ref().ok_or_else(|| {
        not_implemented(&format!(
            "the aggregate `{op}` in `{aggregate}`, which takes a parameter as its first argument"
        ))
    })?;
    let group = grouping(modifier.as_ref());
    if matches!(op.id(), token::T_TOPK | token::T_BOTTOMK) && constant_value(param).is_none() {
        let input = compile_expr(context, expr)?;
        let output = input.output.clone();
        let parameter = compile_expr(context, param)?;
        return Ok(NativeNode {
            expr: NativeExpr::ParameterizedTopK {
                input: Box::new(input),
                op: BoundVectorOp::TopK {
                    k: 0.0,
                    top: op.id() == token::T_TOPK,
                    group,
                },
                parameter: Box::new(parameter),
            },
            output,
        });
    }
    let bound = match op.id() {
        token::T_TOPK | token::T_BOTTOMK => BoundVectorOp::TopK {
            k: constant_argument(&name, param, 0)?,
            top: op.id() == token::T_TOPK,
            group,
        },
        token::T_LIMITK => BoundVectorOp::LimitK {
            k: constant_argument(&name, param, 0)?,
            group,
        },
        // `limit_ratio` keeps whole series by their label hash, so a grouping changes nothing.
        token::T_LIMIT_RATIO => BoundVectorOp::LimitRatio {
            ratio: constant_argument(&name, param, 0)?,
        },
        _ => {
            let PromExpr::StringLiteral(StringLiteral { val }) = unparen(param) else {
                return Err(not_implemented(&format!(
                    "the aggregate `{op}` in `{aggregate}`, whose label `{param}` is not a string \
                     literal"
                )));
            };
            BoundVectorOp::CountValues {
                label: Arc::from(val.as_str()),
                group,
            }
        }
    };
    vector_call(context, &name, bound, expr)
}

/// How a `by` or `without` modifier identifies a group, in the tree's own terms.
///
/// Labels keep their written order.
fn grouping(modifier: Option<&LabelModifier>) -> Grouping {
    fn labels(labels: &[String]) -> Arc<[Arc<str>]> {
        labels
            .iter()
            .map(|label| Arc::<str>::from(label.as_str()))
            .collect()
    }
    match modifier {
        // No modifier means `by()`: one group.
        None => Grouping::By(Arc::new([])),
        Some(LabelModifier::Include(by)) => Grouping::By(labels(&by.labels)),
        Some(LabelModifier::Exclude(without)) => Grouping::Without(labels(&without.labels)),
    }
}

/// Compiles `timestamp`, projecting selected source times for a selector and evaluation times for
/// an already evaluated vector.
fn timestamp_call(context: &Context, args: &[Box<PromExpr>]) -> Result<NativeNode> {
    if args.len() != 1 {
        return Err(not_implemented("timestamp expects one argument"));
    }
    let argument = unparen(&args[0]);
    if argument.value_type() != ValueType::Vector {
        return Err(not_implemented("timestamp argument is not a vector"));
    }
    if let PromExpr::VectorSelector(selector) = argument {
        return instant_selection_of(context, selector, InstantValue::Timestamp);
    }
    let input = compile_expr(context, argument)?;
    let NativeType::Samples(schema) = &input.output else {
        return Err(not_implemented("timestamp argument is not a vector"));
    };
    if schema.fields.len() != 1 {
        return Err(not_implemented(
            "timestamp() over a multi-value input is not supported natively",
        ));
    }
    let output = input.output.clone();
    Ok(NativeNode {
        expr: NativeExpr::Timestamp {
            input: Box::new(input),
        },
        output,
    })
}

/// Compiles `info(v[, {selector}])` as its base `v` and the instant selection of the info series it
/// reads.
///
/// The info series are selected as Prometheus' `fetchInfoSeries` does: by the info metric's name
/// and the data matchers, at the reference time the selectors of `v` share ([`info_input`]), with
/// the statement's lookback. The kernel matches base series to them.
fn info_call(context: &Context, args: &[Box<PromExpr>]) -> Result<NativeNode> {
    let InfoInput { selector, kernel } = info_input(context.stmt, args)?;
    let base = compile_expr(context, &args[0])?;
    if base
        .output
        .samples()
        .is_none_or(|schema| schema.eval_grid.is_none())
    {
        return Err(DataFusionError::Internal(format!(
            "info's first argument `{}` compiled to something other than an evaluated vector: the \
             parser only accepts a vector there",
            args[0]
        )));
    }
    let info = instant_selection_of(context, &selector, InstantValue::InfoTimestamp)?;
    let fields = info
        .output
        .samples()
        .map_or(0, |schema| schema.fields.len());
    // Prometheus reads one sample per info series and point, and the existing planner has no
    // `info` to follow for several fields.
    if fields != 1 {
        return Err(not_implemented(&format!(
            "info over the info metric `{selector}` with {fields} fields: an info series is read as \
             one field"
        )));
    }
    let output = base.output.clone();
    Ok(NativeNode {
        expr: NativeExpr::Info {
            base: Box::new(base),
            info: Box::new(info),
            selector: kernel,
        },
        output,
    })
}

/// The info series `info(v[, {selector}])` reads, as a selector, and the selector its kernel
/// combines by.
pub(crate) struct InfoInput {
    /// The info metric with the data matchers, at the reference time the selectors of `v` share.
    pub selector: VectorSelector,
    pub kernel: InfoSelector,
}

/// Splits the arguments of `info` into the selector of its info series and the kernel's selector.
///
/// Shared by the compiler and the planner, which bind the info scan at the same position. The
/// metric must be `target_info` or one `__name__="..."` matcher, since one info metric is one
/// table. `__field__`, `__schema__` and `__database__` matchers are refused.
pub(crate) fn info_input(stmt: &EvalStmt, args: &[Box<PromExpr>]) -> Result<InfoInput> {
    let (base, labels) = match args {
        [base] => (base, None),
        [base, labels] => (base, Some(labels)),
        _ => {
            return Err(not_implemented(&format!(
                "a call to `info` with {} argument(s)",
                args.len()
            )));
        }
    };
    let matchers: &[Matcher] = match labels.map(|labels| labels.as_ref()) {
        None => &[],
        Some(PromExpr::VectorSelector(VectorSelector {
            name: None,
            matchers,
            offset: None,
            at: None,
        })) if matchers.or_matchers.is_empty() => &matchers.matchers,
        Some(other) => {
            return Err(not_implemented(&format!(
                "`info` whose second argument `{other}` is not one label selector without a name \
                 or modifiers"
            )));
        }
    };
    let (names, data): (Vec<&Matcher>, Vec<&Matcher>) = matchers
        .iter()
        .partition(|matcher| matcher.name == "__name__");
    let metric = match names.as_slice() {
        [] => "target_info".to_string(),
        [name] if name.op == ParserMatchOp::Equal => name.value.clone(),
        _ => {
            return Err(not_implemented(
                "`info` over the info metrics a regular expression, a negative matcher or several \
                 `__name__` matchers select, which can be several tables",
            ));
        }
    };
    if data.iter().any(|matcher| {
        matches!(
            matcher.name.as_str(),
            "__field__" | "__schema__" | "__database__"
        )
    }) {
        return Err(not_implemented(
            "`info` with a `__field__`, `__schema__` or `__database__` matcher, which selects \
             what is read rather than a data label",
        ));
    }
    let (at, offset_ms) = info_reference(stmt, base)?;
    let offset = match offset_ms {
        0 => None,
        ms if ms > 0 => Some(Offset::Pos(Duration::from_millis(ms.unsigned_abs()))),
        ms => Some(Offset::Neg(Duration::from_millis(ms.unsigned_abs()))),
    };
    let kernel = InfoSelector {
        name_matchers: names
            .iter()
            .map(|matcher| (match_op(&matcher.op), Arc::from(matcher.value.as_str())))
            .collect(),
        data_labels: data
            .iter()
            .map(|matcher| Arc::from(matcher.name.as_str()))
            .collect(),
        keep_unmatched: data.iter().all(|matcher| matcher.is_match("")),
    };
    Ok(InfoInput {
        selector: VectorSelector {
            name: Some(metric),
            matchers: Matchers::new(data.into_iter().cloned().collect()),
            offset,
            at,
        },
        kernel,
    })
}

/// The `@` anchor and offset the info series are selected at: those of the first selector of
/// `expr` when every vector-producing path of it reaches a selector with the same effective
/// reference time, and none otherwise, as Prometheus `infoSeriesSelectTimestampAndOffset` decides.
///
/// A selector inside subqueries adds their offsets until one of them anchors it with `@`. A path
/// that produces a vector without a selector, such as `vector(1)`, is evaluated at the step itself,
/// so it leaves no shared reference.
fn info_reference(stmt: &EvalStmt, expr: &PromExpr) -> Result<(Option<AtModifier>, i64)> {
    #[derive(Default)]
    struct Walk {
        /// The first selector's anchor, its resolved time and its offset.
        first: Option<(Option<AtModifier>, Option<i64>, i64)>,
        uniform: bool,
        reference_free: bool,
    }

    fn inspect<'a>(
        stmt: &EvalStmt,
        expr: &'a PromExpr,
        subqueries: &mut Vec<&'a SubqueryExpr>,
        walk: &mut Walk,
    ) -> Result<bool> {
        if !matches!(expr.value_type(), ValueType::Vector | ValueType::Matrix) {
            return Ok(false);
        }
        let selector = match expr {
            PromExpr::VectorSelector(selector) => Some(selector),
            PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => Some(vs),
            _ => None,
        };
        if let Some(selector) = selector {
            let mut at = selector.at.clone();
            let mut offset = selector_offset_ms(&selector.offset)?;
            for subquery in subqueries.iter().rev() {
                if at.is_some() {
                    break;
                }
                offset = offset
                    .checked_add(selector_offset_ms(&subquery.offset)?)
                    .ok_or_else(|| DataFusionError::Plan("selector offset overflows".into()))?;
                at = subquery.at.clone();
            }
            let at_ms = selector_anchor_ms(&at, stmt)?;
            match &walk.first {
                None => walk.first = Some((at, at_ms, offset)),
                Some((_, first_at, first_offset)) => {
                    let same = match (first_at, at_ms) {
                        (None, None) => *first_offset == offset,
                        (Some(first_at), Some(at)) => {
                            i128::from(*first_at) - i128::from(*first_offset)
                                == i128::from(at) - i128::from(offset)
                        }
                        _ => false,
                    };
                    walk.uniform &= same;
                }
            }
            return Ok(true);
        }
        let children: Vec<&PromExpr> = match expr {
            PromExpr::Paren(paren) => vec![&paren.expr],
            PromExpr::Unary(unary) => vec![&unary.expr],
            PromExpr::Binary(binary) => vec![&binary.lhs, &binary.rhs],
            PromExpr::Aggregate(aggregate) => aggregate
                .param
                .iter()
                .chain([&aggregate.expr])
                .map(|child| child.as_ref())
                .collect(),
            // The second argument of a nested `info` is selector syntax, not a vector it evaluates.
            PromExpr::Call(call) if call.func.name == "info" => call
                .args
                .args
                .iter()
                .take(1)
                .map(|arg| arg.as_ref())
                .collect(),
            PromExpr::Call(call) => call.args.args.iter().map(|arg| arg.as_ref()).collect(),
            PromExpr::Subquery(subquery) => {
                subqueries.push(subquery);
                let found = inspect(stmt, &subquery.expr, subqueries, walk);
                subqueries.pop();
                let found = found?;
                walk.reference_free |= !found;
                return Ok(found);
            }
            other => {
                return Err(not_implemented(&format!(
                    "`info` over `{other}`, whose reference time is not derived here"
                )));
            }
        };
        let mut found = false;
        for child in children {
            found |= inspect(stmt, child, subqueries, walk)?;
        }
        walk.reference_free |= !found;
        Ok(found)
    }

    let mut walk = Walk {
        uniform: true,
        ..Default::default()
    };
    inspect(stmt, expr, &mut Vec::new(), &mut walk)?;
    match walk.first {
        Some((at, _, offset)) if walk.uniform && !walk.reference_free => Ok((at, offset)),
        _ => Ok((None, 0)),
    }
}

fn match_op(op: &ParserMatchOp) -> MatchOp {
    match op {
        ParserMatchOp::Equal => MatchOp::Equal,
        ParserMatchOp::NotEqual => MatchOp::NotEqual,
        ParserMatchOp::Re(_) => MatchOp::Regex,
        ParserMatchOp::NotRe(_) => MatchOp::NotRegex,
    }
}

/// Compiles a function call by looking its name up in each function table; an unknown name is
/// refused.
fn compile_call(context: &Context, call: &Call) -> Result<NativeNode> {
    if call.func.name == "timestamp" {
        return timestamp_call(context, &call.args.args);
    }
    if call.func.name == "info" {
        return info_call(context, &call.args.args);
    }
    if call.func.name == ABSENT_OVER_TIME {
        return absent_over_time_call(context, &call.args.args);
    }
    let Call { func, args } = call;
    if let Some(node) = scalar_call(context, func.name, &args.args)? {
        return Ok(node);
    }
    if let Some(signature) = range_signature(func.name) {
        return window_call(context, func.name, signature, &args.args);
    }
    if let Some(node) = parameterized_histogram_call(context, func.name, &args.args)? {
        return Ok(node);
    }
    if let Some((op, vector)) = vector_op(func.name, &args.args)? {
        return vector_call(context, func.name, op, vector);
    }
    if let Some(node) = scalar_function_call(context, func.name, &args.args)? {
        return Ok(node);
    }
    if let Some(node) = parameterized_call(context, func.name, &args.args)? {
        return Ok(node);
    }
    if let Some((op, vector)) = math_op(func.name, &args.args)? {
        return map_call(context, func.name, MapOp::Math(op), vector);
    }
    if let Some(function) = histogram_function(func.name) {
        let [vector] = args.args.as_slice() else {
            return Err(not_implemented(&format!(
                "a call to `{}` with {} argument(s), where it takes its vector",
                func.name,
                args.args.len()
            )));
        };
        return map_call(context, func.name, MapOp::Histogram(function), vector);
    }
    if let Some((rewrite, vector)) = label_rewrite(func.name, &args.args)? {
        return map_call(context, func.name, MapOp::Label(Arc::new(rewrite)), vector);
    }
    Err(not_implemented(&format!(
        "a call to `{}`, which has no native kernel",
        func.name
    )))
}

/// Binds `time()`, `vector(s)` and a date function called without an argument, or
/// returns `None` when the call is none of them.
///
/// A date function without an argument is `f(vector(time()))` in Prometheus, but reads the
/// evaluation time truncated to whole seconds, so it binds as `EvalTime { date }` rather than over
/// `time()`.
fn scalar_call(
    context: &Context,
    name: &str,
    args: &[Box<PromExpr>],
) -> Result<Option<NativeNode>> {
    let grid = || Arc::clone(&context.grid);
    let eval_time = |date| NativeNode {
        expr: NativeExpr::EvalTime { date },
        output: NativeType::Scalar(grid()),
    };
    let node = match (name, args) {
        ("time", []) => eval_time(None),
        ("vector", [scalar]) => vector_of(
            compile_expr(context, scalar)?,
            &grid(),
            &context.value_field,
        ),
        ("scalar", [vector]) => {
            if vector.value_type() != ValueType::Vector {
                return Err(not_implemented("scalar() requires a vector argument"));
            }
            let input = compile_expr(context, vector)?;
            let NativeType::Samples(schema) = &input.output else {
                return Err(not_implemented("scalar() requires an evaluated vector"));
            };
            let Some(input_grid) = schema.eval_grid.clone() else {
                return Err(not_implemented("scalar() requires an evaluated vector"));
            };
            NativeNode {
                expr: NativeExpr::ScalarOf {
                    input: Box::new(input),
                },
                output: NativeType::Scalar(input_grid),
            }
        }
        (name, []) => match math_signature(name) {
            Some(MathSignature::Unary(MathOp::Date(date))) => {
                vector_of(eval_time(Some(date)), &grid(), &context.value_field)
            }
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    Ok(Some(node))
}

/// Binds `max_of(a, b)` or `min_of(a, b)`, whose arguments are both scalars, or returns `None` for
/// any other call.
///
/// Prometheus types both as `(scalar, scalar) -> scalar`. The first argument is the map's input and
/// the second its parameter; a vector argument is refused.
fn scalar_function_call(
    context: &Context,
    name: &str,
    args: &[Box<PromExpr>],
) -> Result<Option<NativeNode>> {
    let op = match name {
        "max_of" => ParameterizedOp::MaxOf,
        "min_of" => ParameterizedOp::MinOf,
        _ => return Ok(None),
    };
    let [left, right] = args else {
        return Err(not_implemented(&format!(
            "a call to `{name}` with {} argument(s), where it takes two scalars",
            args.len()
        )));
    };
    for argument in [left, right] {
        if argument.value_type() != ValueType::Scalar {
            return Err(not_implemented(&format!(
                "a call to `{name}` whose argument `{argument}` is a {}, where the function takes \
                 scalars",
                argument.value_type()
            )));
        }
    }
    let input = compile_expr(context, left)?;
    let output_grid = match &input.output {
        NativeType::Scalar(grid) => Arc::clone(grid),
        _ => {
            return Err(not_implemented(&format!(
                "a call to `{name}` whose argument `{left}` does not evaluate to a scalar"
            )));
        }
    };
    let parameter = compile_expr(context, right)?;
    match &parameter.output {
        NativeType::Scalar(grid) if grid.spec() == output_grid.spec() => {}
        NativeType::Scalar(_) => {
            return Err(not_implemented(&format!(
                "a call to `{name}` whose arguments are evaluated on grids that differ"
            )));
        }
        _ => {
            return Err(not_implemented(&format!(
                "a call to `{name}` whose argument `{right}` does not evaluate to a scalar"
            )));
        }
    }
    Ok(Some(NativeNode {
        expr: NativeExpr::ParameterizedMap {
            input: Box::new(input),
            op,
            parameters: vec![parameter],
        },
        output: NativeType::Scalar(output_grid),
    }))
}

/// Binds `round`, `clamp`, `clamp_min` or `clamp_max` whose parameters are not all constants, or
/// returns `None` for any other call. Prometheus reads each parameter at every step, so each one is
/// bound as the scalar node it compiles to; with constant parameters the call is a [`MathOp`].
fn parameterized_call(
    context: &Context,
    name: &str,
    args: &[Box<PromExpr>],
) -> Result<Option<NativeNode>> {
    let op = match name {
        "round" => ParameterizedOp::Round,
        "clamp" => ParameterizedOp::Clamp,
        "clamp_min" => ParameterizedOp::ClampMin,
        "clamp_max" => ParameterizedOp::ClampMax,
        _ => return Ok(None),
    };
    let Some((vector, parameters)) = args.split_first() else {
        return Ok(None);
    };
    if parameters.len() != op.arity()
        || parameters
            .iter()
            .all(|parameter| constant_value(parameter).is_some())
    {
        return Ok(None);
    }
    if vector.value_type() != ValueType::Vector {
        return Err(not_implemented(&format!(
            "a call to `{name}` whose first argument is `{vector}`, which is a {}, where the \
             function takes a vector",
            vector.value_type()
        )));
    }
    let input = compile_expr(context, vector)?;
    let parameters = parameters
        .iter()
        .map(|parameter| compile_expr(context, parameter))
        .collect::<Result<Vec<_>>>()?;
    let output = input.output.clone();
    Ok(Some(NativeNode {
        expr: NativeExpr::ParameterizedMap {
            input: Box::new(input),
            op,
            parameters,
        },
        output,
    }))
}

/// `vector(s)`: the scalar `input` as one series without labels, under one value field.
fn vector_of(input: NativeNode, grid: &Arc<EvalGrid>, value_field: &str) -> NativeNode {
    NativeNode {
        expr: NativeExpr::VectorOf {
            input: Box::new(input),
        },
        output: NativeType::Samples(Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec {
                name: Arc::from(value_field),
            }]),
            eval_grid: Some(Arc::clone(grid)),
        })),
    }
}

/// The field `vector(s)` reports its value under: the planner's default value column.
const VECTOR_FIELD: &str = "value";

/// Binds a range function over a range selector or a subquery.
///
/// The matrix is the argument at [`RangeSignature::selector_index`]; every other argument is a
/// constant.
fn window_call(
    context: &Context,
    name: &str,
    signature: RangeSignature,
    args: &[Box<PromExpr>],
) -> Result<NativeNode> {
    let expected = signature.arity();
    if args.len() != expected {
        return Err(not_implemented(&format!(
            "a call to `{name}` with {} argument(s), where it takes {expected}: {}",
            args.len(),
            signature.arguments()
        )));
    }
    let selector_index = signature.selector_index();
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

/// Wraps the pointwise `op` (math, native histogram or label function) over `vector`; the output
/// layout is the input's.
fn map_call(context: &Context, name: &str, op: MapOp, vector: &PromExpr) -> Result<NativeNode> {
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

/// Wraps the vector operation `op` over `vector`; the output layout is the input's.
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

/// Compiles a classic histogram call with at least one varying scalar argument.
fn parameterized_histogram_call(
    context: &Context,
    name: &str,
    arguments: &[Box<PromExpr>],
) -> Result<Option<NativeNode>> {
    if !matches!(name, "histogram_quantile" | "histogram_fraction") {
        return Ok(None);
    }
    let Some((vector, args)) = arguments.split_last() else {
        return Ok(None);
    };
    let expected = if name == "histogram_quantile" { 1 } else { 2 };
    if args.len() != expected || args.iter().all(|arg| constant_value(arg).is_some()) {
        return Ok(None);
    }
    if vector.value_type() != ValueType::Vector {
        return Err(not_implemented(&format!(
            "a call to `{name}` whose vector argument is `{vector}` is not a vector"
        )));
    }
    let function = if name == "histogram_quantile" {
        HistogramFunction::Quantile(
            args.first()
                .and_then(|arg| constant_value(arg))
                .unwrap_or(0.0),
        )
    } else {
        HistogramFunction::Fraction {
            lower: args
                .first()
                .and_then(|arg| constant_value(arg))
                .unwrap_or(0.0),
            upper: args
                .get(1)
                .and_then(|arg| constant_value(arg))
                .unwrap_or(0.0),
        }
    };
    let input = compile_expr(context, vector)?;
    let output = input.output.clone();
    let parameters = args
        .iter()
        .map(|arg| compile_expr(context, arg))
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(NativeNode {
        expr: NativeExpr::ParameterizedHistogram {
            input: Box::new(input),
            op: BoundVectorOp::Histogram(function),
            parameters,
        },
        output,
    }))
}

/// Not a range function here: it binds `absent`'s vector operation over a window.
const ABSENT_OVER_TIME: &str = "absent_over_time";

/// The range function `absent_over_time` uses to test whether a window holds a sample.
///
/// `last_over_time` rather than `present_over_time` because it keeps the metric name: a nameless
/// selector reads one source per metric, and two metrics with equal labels would collide in the
/// window once the name is dropped. Only presence is read, not the value.
const PRESENCE_FOLD: RangeFunction = RangeFunction::LastOverTime;

/// Binds `absent_over_time(m[d])` as `absent` over a [`PRESENCE_FOLD`] window of `m[d]`.
fn absent_over_time_call(context: &Context, arguments: &[Box<PromExpr>]) -> Result<NativeNode> {
    let [matrix] = arguments else {
        return Err(not_implemented(&format!(
            "a call to `{ABSENT_OVER_TIME}` with {} argument(s), where it takes one matrix",
            arguments.len()
        )));
    };
    let labels = absent_labels_of(matrix)?;
    let input = match unparen(matrix) {
        PromExpr::MatrixSelector(MatrixSelector { vs, range }) => {
            window_selection(context, PRESENCE_FOLD, vs, range)?
        }
        PromExpr::Subquery(subquery) => {
            subquery_window(context, ABSENT_OVER_TIME, PRESENCE_FOLD, subquery)?
        }
        other => {
            return Err(not_implemented(&format!(
                "a call to `{ABSENT_OVER_TIME}` whose matrix is `{other}`, where it has to be a \
                 range selector like `x[5m]` or a subquery like `x[5m:1m]`"
            )));
        }
    };
    let output = input.output.clone();
    Ok(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(input),
            op: BoundVectorOp::Absent { labels },
        },
        output,
    })
}

/// The vector operation a sort, `absent` or a classic histogram function binds, and the vector it
/// reads, or `None` when `name` is none of them.
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

/// The labels `absent(expr)` and `absent_over_time(m[d])` answer with, as Prometheus
/// `createLabelsForAbsentFunction` derives them: from a vector or range selector's matchers, and
/// none for any other expression. `__field__`, `__schema__` and `__database__` are not labels.
fn absent_labels_of(expr: &PromExpr) -> Result<SeriesLabels> {
    let (selector, alternatives) = match unparen(expr) {
        PromExpr::VectorSelector(selector) => (&selector.matchers, &selector.matchers.or_matchers),
        PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => {
            (&vs.matchers, &vs.matchers.or_matchers)
        }
        _ => return absent_labels(&[]),
    };
    if !alternatives.is_empty() {
        return Err(not_implemented(&format!(
            "`absent` or `absent_over_time` over `{expr}`, whose matchers are alternatives"
        )));
    }
    let matchers = selector
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
            op: match_op(&matcher.op),
            value: Arc::from(matcher.value.as_str()),
            matches_empty: matcher.is_match(""),
        })
        .collect::<Vec<_>>();
    absent_labels(&matchers)
}

/// Binds `function` over the range selector `vs[range]` into a window over the raw input.
fn window_selection(
    context: &Context,
    function: RangeFunction,
    vs: &VectorSelector,
    range: &Duration,
) -> Result<NativeNode> {
    let range_ms = range_millis(range.as_millis())?;
    let offset_ms = selector_offset_ms(&vs.offset)?;
    let anchor_ms = selector_anchor_ms(&vs.at, context.stmt)?;
    let output_grid = Arc::clone(&context.grid);

    let (grid_start_ms, grid_end_ms) = grid_endpoints(&output_grid)?;
    let (source_index, schema) = bind_selector(
        context,
        SelectorRequirement {
            metric: vs.name.clone(),
            matchers: vs.matchers.clone(),
            offset_ms,
            at_ms: anchor_ms,
            grid_start_ms,
            grid_end_ms,
            timing: SelectorTiming::Window { range_ms },
        },
    )?;
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::clone(&schema.fields),
        eval_grid: Some(Arc::clone(&output_grid)),
    }));
    let raw = NativeType::Samples(Arc::clone(&schema));

    Ok(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: raw.clone(),
                    source_index,
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
                    modifier: None,
                },
                input_grid: None,
                output_grid,
            },
        },
        output,
    })
}

/// The step a subquery names when it names none, as in `x[5m:]`.
///
/// Prometheus uses the global evaluation interval, one minute by default, not the statement's step.
/// The existing planner keeps a private copy of the same constant.
const DEFAULT_SUBQUERY_STEP_MS: i64 = 60_000;

/// The grid a subquery's inner expression is evaluated on, and the window that reads it.
///
/// The compiler and the planner both derive it here so they cannot disagree on the grid.
pub(crate) struct SubqueryGrid {
    /// The interval the range function folds, and how far back from the anchor it reads.
    pub(crate) window: RangeWindow,
    /// The grid `subquery.expr` is evaluated on.
    pub(crate) grid: Arc<EvalGrid>,
}

/// Derives the [`SubqueryGrid`] of `subquery`, whose outer statement is evaluated on `outer`.
///
/// The child grid has one point per `step` over the interval the subquery's window reads. `None`
/// when that interval contains no step boundary. Errors on a zero step or an out-of-range duration.
pub(crate) fn subquery_child_grid(
    stmt: &EvalStmt,
    outer: &EvalGrid,
    subquery: &SubqueryExpr,
) -> Result<Option<SubqueryGrid>> {
    let anchor_ms = selector_anchor_ms(&subquery.at, stmt)?;
    let range_ms = range_millis(subquery.range.as_millis())?;
    let step_ms = match subquery.step {
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
    let offset_ms = selector_offset_ms(&subquery.offset)?;
    let window = RangeWindow {
        range_ms,
        offset_ms,
        anchor_ms,
        modifier: None,
    };
    let Some(spec) = subquery_grid(outer.spec(), window, step_ms)? else {
        return Ok(None);
    };
    // Keep subquery grids deferred: selector binding needs only their spec endpoints, not a
    // materialized timestamp vector proportional to the range.
    let grid = Arc::new(EvalGrid::try_deferred(spec).map_err(grid_error)?);
    Ok(Some(SubqueryGrid { window, grid }))
}

/// Binds `function` over `subquery`, whose inner expression is compiled on the subquery's child
/// grid. The window's `input_grid` is that child grid; its output grid is the enclosing one.
fn subquery_window(
    context: &Context,
    name: &str,
    function: RangeFunction,
    subquery: &SubqueryExpr,
) -> Result<NativeNode> {
    let SubqueryExpr { expr, .. } = subquery;
    if expr.value_type() != ValueType::Vector {
        return Err(not_implemented(&format!(
            "a call to `{name}` over the subquery `{subquery}`, whose expression is a {}, where a \
             subquery has to evaluate a vector",
            expr.value_type()
        )));
    }
    let Some(SubqueryGrid {
        window,
        grid: child_grid,
    }) = subquery_child_grid(context.stmt, &context.grid, subquery)?
    else {
        return Err(not_implemented(&format!(
            "the subquery `{subquery}`, which names no evaluation point in its anchored interval or \
             the statement's range"
        )));
    };
    let inner = compile_expr(
        &Context {
            stmt: context.stmt,
            bindings: context.bindings,
            inputs: context.inputs,
            ordering: context.ordering,
            grid: Arc::clone(&child_grid),
            value_field: Arc::clone(&context.value_field),
            source_kind: context.source_kind,
        },
        expr,
    )?;

    let inner_schema = inner
        .output
        .samples()
        .ok_or_else(|| DataFusionError::Internal("subquery body did not produce samples".into()))?;
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::clone(&inner_schema.fields),
        eval_grid: Some(Arc::clone(&context.grid)),
    }));

    Ok(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function,
                window,
                input_grid: Some(child_grid),
                output_grid: Arc::clone(&context.grid),
            },
        },
        output,
    })
}

/// Compiles a bare selector into an instant selection: the last sample within the lookback at each
/// grid point.
fn instant_selection(context: &Context, selector: &VectorSelector) -> Result<NativeNode> {
    instant_selection_of(context, selector, InstantValue::Sample)
}

fn instant_selection_of(
    context: &Context,
    selector: &VectorSelector,
    value: InstantValue,
) -> Result<NativeNode> {
    let lookback_ms = lookback_millis(context.stmt.lookback_delta.as_millis())?;
    let offset_ms = selector_offset_ms(&selector.offset)?;
    let at_ms = selector_anchor_ms(&selector.at, context.stmt)?;
    let output_grid = Arc::clone(&context.grid);
    let (grid_start_ms, grid_end_ms) = grid_endpoints(&output_grid)?;
    let (source_index, schema) = bind_selector(
        context,
        SelectorRequirement {
            metric: selector.name.clone(),
            matchers: selector.matchers.clone(),
            offset_ms,
            at_ms,
            grid_start_ms,
            grid_end_ms,
            timing: SelectorTiming::Instant { lookback_ms },
        },
    )?;
    if value == InstantValue::Timestamp && schema.fields.len() != 1 {
        return Err(not_implemented(
            "timestamp() over a multi-value input is not supported natively",
        ));
    }
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::clone(&schema.fields),
        eval_grid: Some(Arc::clone(&output_grid)),
    }));
    let raw = NativeType::Samples(Arc::clone(&schema));
    Ok(NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: raw.clone(),
                    source_index,
                    ordering: context.ordering,
                }),
                output: raw,
            }),
            op: BoundInstant {
                lookback_ms,
                offset_ms,
                at_ms,
                value,
                output_grid,
            },
        },
        output,
    })
}

fn bind_selector(
    context: &Context,
    requirement: SelectorRequirement,
) -> Result<(usize, Arc<SeriesSchema>)> {
    let schema = (context.bindings)(&requirement)?;
    let source_index = context.inputs.borrow().len();
    context.inputs.borrow_mut().push(CompiledInput {
        source_index,
        requirement,
        schema: Arc::clone(&schema),
        ordering: context.ordering,
    });
    Ok((source_index, schema))
}

fn grid_endpoints(grid: &EvalGrid) -> Result<(i64, i64)> {
    let spec = grid.spec();
    let end = i128::from(spec.start_ms) + i128::from(spec.len - 1) * i128::from(spec.step_ms);
    let end = i64::try_from(end)
        .map_err(|_| DataFusionError::Internal("evaluation grid endpoint overflows".into()))?;
    Ok((spec.start_ms, end))
}

/// The grid the statement is evaluated on: `(end - start) / step + 1` points, the last of which
/// does not pass `end`.
///
/// The planner reads it here so the tree and its binding use the same grid.
pub(crate) fn statement_grid(stmt: &EvalStmt) -> Result<Arc<EvalGrid>> {
    let start_ms = signed_millis_since_epoch(stmt.start)?;
    let end_ms = signed_millis_since_epoch(stmt.end)?;
    let step_ms = millis(stmt.interval.as_millis()).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "the statement's evaluation step of {}ms is out of range",
            stmt.interval.as_millis()
        ))
    })?;
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
/// `start()` and `end()` resolve against `stmt.start` and `stmt.end`. The offset is not folded in.
/// An anchored window can lie outside the statement's range, so a scan must be bounded by
/// [`SelectorRequirement::raw_scan_bounds`], not by the statement.
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

/// Where a range function's matrix sits among its arguments and how its constants bind.
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

    fn arity(self) -> usize {
        self.constants() + 1
    }

    /// The call's arguments, for the arity rejection.
    fn arguments(self) -> &'static str {
        match self {
            Self::Selector(_) => "its range selector",
            Self::ConstantBeforeSelector(_) => "a number and its range selector",
            Self::ConstantAfterSelector(_) => "its range selector and a number",
            Self::TwoConstantsAfterSelector(_) => "its range selector and two numbers",
        }
    }

    /// Binds the function from arguments that already passed the arity check.
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

/// The signature of the range function `name`, or `None` when there is no range kernel for it.
/// Functions with constants are listed here; the rest come from [`range_function`].
fn range_signature(name: &str) -> Option<RangeSignature> {
    match name {
        "quantile_over_time" => Some(RangeSignature::ConstantBeforeSelector(
            RangeFunction::QuantileOverTime,
        )),
        "predict_linear" => Some(RangeSignature::ConstantAfterSelector(
            RangeFunction::PredictLinear,
        )),
        "double_exponential_smoothing" => Some(RangeSignature::TwoConstantsAfterSelector(
            |smoothing, trend| RangeFunction::DoubleExponentialSmoothing { smoothing, trend },
        )),
        _ => range_function(name).map(RangeSignature::Selector),
    }
}

/// The constants a math function reads after its vector, and how they bind.
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

    /// The call's arguments, for the arity rejection.
    fn arguments(self) -> &'static str {
        match self {
            Self::Unary(_) => "its vector",
            Self::OneConstant(_) => "its vector and one constant",
            Self::DefaultedConstant { .. } => "its vector and one optional constant",
            Self::TwoConstants(_) => "its vector and two constants",
        }
    }
}

/// The signature of the math function `name`, or `None` when there is no math kernel for it.
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
        // Called without an argument, a date function reads the evaluation time and is bound by
        // `scalar_call` before it reaches `math_op`.
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
        // `round(x)` is `round(x, 1)`.
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

/// The math kernel `name` denotes and the vector it maps, or `None` when `name` is not in
/// [`math_signature`]. `arguments` is the whole argument list, vector first; the constants after
/// it must fold.
fn math_op<'a>(
    name: &str,
    arguments: &'a [Box<PromExpr>],
) -> Result<Option<(MathOp, &'a PromExpr)>> {
    let Some(signature) = math_signature(name) else {
        return Ok(None);
    };
    let Some((vector, parameters)) = arguments.split_first() else {
        return Err(arity_error(name, signature, 0));
    };
    let given = parameters.len() + 1;
    if given < signature.required() + 1 || given > signature.parameters() + 1 {
        return Err(arity_error(name, signature, given));
    }
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

/// The native histogram function `name` denotes, or `None` when it is not one.
///
/// These read only native histogram samples and drop the float ones, as Prometheus'
/// `simpleHistogramFunc` does; `histogram_quantile` and `histogram_fraction` also read classic
/// bucket series and are not listed.
fn histogram_function(name: &str) -> Option<NativeHistogramFunction> {
    match name {
        "histogram_count" => Some(NativeHistogramFunction::Count),
        "histogram_sum" => Some(NativeHistogramFunction::Sum),
        "histogram_avg" => Some(NativeHistogramFunction::Avg),
        "histogram_stddev" => Some(NativeHistogramFunction::StdDev),
        "histogram_stdvar" => Some(NativeHistogramFunction::StdVar),
        _ => None,
    }
}

/// The label change `label_replace` or `label_join` makes, and the vector it is made to, or `None`
/// when `name` is neither.
///
/// [`LabelRewrite`] validates the arguments as Prometheus does at evaluation, so an invalid regex
/// or label name fails the statement here.
fn label_rewrite<'a>(
    name: &str,
    arguments: &'a [Box<PromExpr>],
) -> Result<Option<(LabelRewrite, &'a PromExpr)>> {
    if !matches!(name, "label_replace" | "label_join") {
        return Ok(None);
    }
    let Some((vector, strings)) = arguments.split_first() else {
        return Err(not_implemented(&format!(
            "a call to `{name}` with no argument"
        )));
    };
    let strings = strings
        .iter()
        .map(|argument| match unparen(argument) {
            PromExpr::StringLiteral(StringLiteral { val }) => Ok(val.as_str()),
            other => Err(not_implemented(&format!(
                "a call to `{name}` whose argument `{other}` is not a string literal"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    let rewrite = match (name, strings.as_slice()) {
        ("label_replace", [dst, replacement, src, regex]) => {
            LabelRewrite::replace(dst, replacement, src, regex)?
        }
        ("label_join", [dst, separator, src @ ..]) => LabelRewrite::join(dst, separator, src)?,
        _ => {
            return Err(not_implemented(&format!(
                "a call to `{name}` with {} argument(s)",
                arguments.len()
            )));
        }
    };
    Ok(Some((rewrite, vector)))
}

/// The rejection of a math call with `given` arguments, the vector counted in.
fn arity_error(name: &str, signature: MathSignature, given: usize) -> DataFusionError {
    not_implemented(&format!(
        "a call to `{name}` with {given} argument(s), where it takes {}",
        signature.arguments()
    ))
}

/// The constant `argument` folds to, or a rejection naming `name` (a function or aggregator) and
/// the argument position.
///
/// A quantile outside `[0, 1]` is not refused: Prometheus answers it with `-Inf` or `+Inf`.
fn constant_argument(name: &str, argument: &PromExpr, position: usize) -> Result<f64> {
    constant_value(argument).ok_or_else(|| {
        not_implemented(&format!(
            "`{name}`, whose {} is `{argument}`, where it has to be a constant expression this \
             compiler folds to a number literal",
            argument_position(position)
        ))
    })
}

/// Callers pass a literal position below three.
fn argument_position(position: usize) -> &'static str {
    const POSITIONS: [&str; 3] = ["first argument", "second argument", "third argument"];
    POSITIONS[position]
}

/// Whether a binary operation carries a modifier this compiler refuses to bind.
///
/// Matching, a non-default cardinality or a fill value has no meaning with a scalar operand. Shared
/// by [`constant_value`] and [`binary_operation`] so the fold never drops a modifier the dispatch
/// refuses.
fn refuses_modifier(modifier: Option<&BinModifier>) -> bool {
    modifier.is_some_and(|modifier| {
        modifier.matching.is_some()
            || modifier.card != VectorMatchCardinality::OneToOne
            || modifier.fill_values != VectorMatchFillValues::default()
    })
}

/// The number `expr` folds to, or `None` when it depends on data or evaluation time.
///
/// Folds number literals, parentheses, negation, `pi()`, and arithmetic or `bool` comparisons of
/// two folded operands, using the kernel's [`BinaryOp::kind`] so the fold and the kernel agree.
pub(crate) fn constant_value(expr: &PromExpr) -> Option<f64> {
    match expr {
        PromExpr::NumberLiteral(NumberLiteral { val }) => Some(*val),
        PromExpr::Paren(paren) => constant_value(&paren.expr),
        PromExpr::Unary(unary) => constant_value(&unary.expr).map(|value| -value),
        PromExpr::Binary(binary) => {
            // Left unfolded so `binary_operation` refuses the modifier instead of the fold
            // dropping it, e.g. the fill in `x * (1 + fill(0) 2)`.
            if refuses_modifier(binary.modifier.as_ref()) {
                return None;
            }
            let left = constant_value(&binary.lhs)?;
            let right = constant_value(&binary.rhs)?;
            match binary_op(&binary.op)?.kind() {
                BinaryKind::Arithmetic(op) => Some(op.calculate(left, right)),
                // Without `bool`, `1 > 2` is a parse error in Prometheus; left unfolded, the
                // dispatch refuses it.
                BinaryKind::Comparison(op) => binary
                    .return_bool()
                    .then(|| f64::from(op.apply(left, right))),
            }
        }
        PromExpr::Call(call) if call.func.name == "pi" && call.args.args.is_empty() => {
            Some(std::f64::consts::PI)
        }
        _ => None,
    }
}

/// The range function a PromQL name denotes, or `None` when this compiler has no kernel for it.
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
        "first_over_time" => Some(RangeFunction::FirstOverTime),
        "mad_over_time" => Some(RangeFunction::MadOverTime),
        "ts_of_first_over_time" => Some(RangeFunction::TsOfFirstOverTime),
        "ts_of_last_over_time" => Some(RangeFunction::TsOfLastOverTime),
        "ts_of_min_over_time" => Some(RangeFunction::TsOfMinOverTime),
        "ts_of_max_over_time" => Some(RangeFunction::TsOfMaxOverTime),
        _ => None,
    }
}

/// The expression inside any number of parentheses.
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
