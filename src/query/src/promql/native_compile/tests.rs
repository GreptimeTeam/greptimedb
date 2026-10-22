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
use std::time::{Duration, UNIX_EPOCH};

use datafusion::error::DataFusionError;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use promql::map::MathOp;
use promql::native::plan::{
    BoundInstant, BoundMap, BoundWindow, NativeExpr, NativeNode, NativeTree, NativeType,
};
use promql::range_eval::RangeFunction;
use promql::vector_match::{BinaryMatching, BinaryOp};
use promql_parser::parser::{
    BinModifier, EvalStmt, Expr as PromExpr, NumberLiteral, VectorMatchFillValues,
};

use super::compile;

/// The raw series schema a caller binds from the table: one field and no evaluation grid.
fn raw_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::new([FieldSpec {
            name: "value".into(),
        }]),
        eval_grid: None,
    })
}

/// A statement over 100 seconds in 5-second steps, which is 21 grid points.
fn eval_stmt(query: &str) -> EvalStmt {
    eval_stmt_over(query, Duration::from_secs(100))
}

/// The statement `query` over `UNIX_EPOCH..=end`, on a 5s grid.
///
/// The end is a parameter because a grid stops before an end it does not land on, which is the only
/// way `end()` and the last grid point can be told apart.
fn eval_stmt_over(query: &str, end: Duration) -> EvalStmt {
    EvalStmt {
        expr: promql_parser::parser::parse(query).unwrap(),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH + end,
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(5),
    }
}

/// `rate(x[<range>])` with the selector's range set directly: the parser refuses a zero-length
/// range, so a zero-length window can only be handed to the compiler here.
fn with_selector_range(range: Duration) -> EvalStmt {
    let mut stmt = eval_stmt("rate(x[5m])");
    let PromExpr::Call(call) = &mut stmt.expr else {
        unreachable!("`rate(x[5m])` parses to a call");
    };
    let PromExpr::MatrixSelector(selector) = &mut *call.args.args[0] else {
        unreachable!("`rate(x[5m])`'s argument parses to a range selector");
    };
    selector.range = range;
    stmt
}

/// `query`'s statement with its binary operation's modifier edited by `edit`.
///
/// The parser rewrites and rejects some modifiers, so a modifier the compiler has to refuse as
/// written can only reach it on a tree the parser accepted: this is the way in.
fn with_binary_modifier(query: &str, edit: impl FnOnce(&mut BinModifier)) -> EvalStmt {
    let mut stmt = eval_stmt(query);
    let PromExpr::Binary(binary) = &mut stmt.expr else {
        unreachable!("`{query}` parses to a binary operation");
    };
    edit(binary.modifier.get_or_insert_with(BinModifier::default));
    stmt
}

/// `query`'s statement with its call's arguments edited by `edit`.
///
/// The parser checks arity, so a call with the wrong number of arguments cannot be parsed: the only
/// way to hand one to the compiler is to edit a tree the parser accepted.
fn with_call_arguments(query: &str, edit: impl FnOnce(&mut Vec<Box<PromExpr>>)) -> EvalStmt {
    let mut stmt = eval_stmt(query);
    let PromExpr::Call(call) = &mut stmt.expr else {
        unreachable!("`{query}` parses to a call");
    };
    edit(&mut call.args.args);
    stmt
}

/// The range selector out of `rate(x[5m])`.
///
/// The parser type-checks function arguments, so a range selector can only be handed to a function
/// that does not take one by editing a tree the parser accepted.
fn a_matrix_selector() -> Box<PromExpr> {
    let mut stmt = eval_stmt("rate(x[5m])");
    let PromExpr::Call(call) = &mut stmt.expr else {
        unreachable!("`rate(x[5m])` parses to a call");
    };
    call.args.args.remove(0)
}

/// The window at the root of `tree`, for the range-function shape.
fn root_window(tree: &NativeTree) -> (&NativeNode, &BoundWindow) {
    let NativeExpr::Window { input, op } = &tree.root().expr else {
        panic!("the root is not a window: {:?}", tree.root().expr);
    };
    (input, op)
}

/// The instant selection at the root of `tree`, for the bare-selector shape.
fn root_instant(tree: &NativeTree) -> (&NativeNode, &BoundInstant) {
    let NativeExpr::Instant { input, op } = &tree.root().expr else {
        panic!(
            "the root is not an instant selection: {:?}",
            tree.root().expr
        );
    };
    (input, op)
}

/// The input and the operation of a `Map` node.
fn map_parts(node: &NativeNode) -> (&NativeNode, &BoundMap) {
    let NativeExpr::Map { input, op } = &node.expr else {
        panic!("the node is not a map: {:?}", node.expr);
    };
    (input, op)
}

/// The one operation a `Map` node applies.
fn map_op(node: &NativeNode) -> MathOp {
    let (_, op) = map_parts(node);
    let [only] = &op.ops[..] else {
        panic!("the map holds {} operation(s)", op.ops.len());
    };
    *only
}

/// The schema a node's evaluated output carries.
fn evaluated(node: &NativeNode) -> &Arc<SeriesSchema> {
    let Some(schema) = node.output.samples() else {
        panic!("the node does not produce samples: {:?}", node.output);
    };
    assert!(schema.eval_grid.is_some(), "the node is not evaluated");
    schema
}

/// The two operands and the matching of a `Binary` node.
fn binary_parts(node: &NativeNode) -> (&NativeNode, &NativeNode, &BinaryMatching) {
    let NativeExpr::Binary {
        left,
        right,
        matching,
    } = &node.expr
    else {
        panic!("the node is not a binary operation: {:?}", node.expr);
    };
    (left, right, matching)
}

/// The constant a `Scalar` node binds, if it is one.
fn constant(node: &NativeNode) -> Option<f64> {
    match &node.expr {
        NativeExpr::Scalar(scalar) => Some(scalar.value),
        _ => None,
    }
}

/// The grid spec a `Scalar` node reports, as `(start_ms, step_ms, len)`.
fn scalar_grid(node: &NativeNode) -> (i64, i64, u64) {
    let NativeType::Scalar(grid) = &node.output else {
        panic!("the node is not a scalar: {:?}", node.output);
    };
    let spec = grid.spec();
    (spec.start_ms, spec.step_ms, spec.len)
}

/// The grid spec a node's evaluated output carries, as `(start_ms, step_ms, len)`.
fn grid_of(node: &NativeNode) -> (i64, i64, u64) {
    let spec = evaluated(node).eval_grid.as_ref().unwrap().spec();
    (spec.start_ms, spec.step_ms, spec.len)
}

/// `rate(x[5m])` becomes a window over the raw input, on the statement's grid.
#[test]
fn a_rate_over_a_selector_becomes_a_window_over_raw_input() {
    // `compile` returns `NativeTree::try_new`'s result, so unwrapping is the validation.
    let tree = compile(
        &eval_stmt("rate(x[5m])"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();

    let NativeExpr::Window { input, op } = &tree.root().expr else {
        panic!("the root is not a window: {:?}", tree.root().expr);
    };
    assert_eq!(op.function, RangeFunction::Rate);
    assert_eq!(op.window.range_ms, 300_000);
    assert_eq!(op.window.offset_ms, 0);
    assert_eq!(op.window.anchor_ms, None);
    assert!(op.input_grid.is_none(), "raw input has no grid to declare");

    // The statement's grid: 0, 5s, ..., 100s.
    let spec = op.output_grid.spec();
    assert_eq!(
        (spec.start_ms, spec.step_ms, spec.len),
        (0, 5_000, 21),
        "the window does not land on the statement's grid"
    );

    let NativeExpr::Input(bound) = &input.expr else {
        panic!("the window's input is not an input: {:?}", input.expr);
    };
    assert!(
        matches!(&bound.output, NativeType::Samples(schema) if schema.eval_grid.is_none()),
        "the input is not raw samples: {:?}",
        bound.output
    );
    assert_eq!(bound.ordering, SeriesOrdering::KeyThenTime);
}

/// Every mapped name produces its own kernel; a name that fell through to the same kernel as
/// another would be caught here.
#[test]
fn the_supported_range_functions_map_to_their_kernels() {
    let cases = [
        ("rate", RangeFunction::Rate),
        ("increase", RangeFunction::Increase),
        ("delta", RangeFunction::Delta),
        ("irate", RangeFunction::Irate),
        ("idelta", RangeFunction::Idelta),
        ("avg_over_time", RangeFunction::AvgOverTime),
        ("min_over_time", RangeFunction::MinOverTime),
        ("max_over_time", RangeFunction::MaxOverTime),
        ("sum_over_time", RangeFunction::SumOverTime),
        ("count_over_time", RangeFunction::CountOverTime),
        ("last_over_time", RangeFunction::LastOverTime),
        ("present_over_time", RangeFunction::PresentOverTime),
        ("stddev_over_time", RangeFunction::StddevOverTime),
        ("stdvar_over_time", RangeFunction::StdvarOverTime),
        ("resets", RangeFunction::Resets),
        ("changes", RangeFunction::Changes),
        ("deriv", RangeFunction::Deriv),
    ];

    for (name, kernel) in cases {
        let tree = compile(
            &eval_stmt(&format!("{name}(x[1m])")),
            raw_schema(),
            SeriesOrdering::KeyThenTime,
        )
        .unwrap();
        let NativeExpr::Window { op, .. } = &tree.root().expr else {
            panic!(
                "`{name}` did not compile to a window: {:?}",
                tree.root().expr
            );
        };
        assert_eq!(op.function, kernel, "`{name}` produced the wrong kernel");
        assert_eq!(op.window.range_ms, 60_000, "`{name}` lost its window");
    }
}

/// `quantile_over_time(0.9, x[5m])` binds the quantile the query wrote, over the same window a
/// function without parameters would fold: a parameter decides what folds the window, not which
/// window is folded.
#[test]
fn a_quantile_over_time_binds_its_quantile_and_its_window() {
    let tree = compile(
        &eval_stmt("quantile_over_time(0.9, x[5m])"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();

    let (_, op) = root_window(&tree);
    assert_eq!(op.function, RangeFunction::QuantileOverTime(0.9));
    assert_eq!(op.window.range_ms, 300_000);
    assert_eq!(op.window.offset_ms, 0);
    assert_eq!(op.window.anchor_ms, None);
    assert!(op.input_grid.is_none(), "raw input has no grid to declare");

    // The statement's grid: 0, 5s, ..., 100s.
    let spec = op.output_grid.spec();
    assert_eq!(
        (spec.start_ms, spec.step_ms, spec.len),
        (0, 5_000, 21),
        "the window does not land on the statement's grid"
    );
}

/// A quantile outside `[0, 1]` compiles, because it is not an error in Prometheus: the kernel
/// returns `-Infinity` below zero, `+Infinity` above one, and a real quantile at either bound. A
/// compiler that refused it would invent a rule the query engine does not have, so the number is
/// bound exactly as written.
#[test]
fn a_quantile_outside_the_unit_interval_is_bound_as_given() {
    let cases = [("-1.0", -1.0), ("2.0", 2.0), ("0.0", 0.0), ("1.0", 1.0)];

    for (written, quantile) in cases {
        let query = format!("quantile_over_time({written}, x[5m])");
        let tree = compile(
            &eval_stmt(&query),
            raw_schema(),
            SeriesOrdering::KeyThenTime,
        )
        .unwrap_or_else(|error| panic!("`{query}` did not compile: {error}"));
        let (_, op) = root_window(&tree);
        assert_eq!(
            op.function,
            RangeFunction::QuantileOverTime(quantile),
            "`{query}` did not bind the quantile it wrote"
        );
        assert_eq!(op.window.range_ms, 300_000, "`{query}` lost its window");
    }
}

/// A parameter in parentheses binds the same quantile, because it is the same query: the planner
/// already looks through the parentheses when it reads a literal, so the native compiler does too.
#[test]
fn a_parenthesised_quantile_binds_as_written() {
    let tree = compile(
        &eval_stmt("quantile_over_time((0.9), x[5m])"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect("a parenthesised literal is the same quantile");

    let (_, op) = root_window(&tree);
    assert_eq!(op.function, RangeFunction::QuantileOverTime(0.9));
}

/// The selector of a parameterised function is looked through its parentheses too, so the range
/// function and the parameter are both read from a statement that was merely typed with grouping.
#[test]
fn a_parenthesised_selector_binds_as_written() {
    let bare = compile(
        &eval_stmt("quantile_over_time(0.9, x[5m])"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect("the bare selector binds");

    for query in [
        "quantile_over_time(0.9, (x[5m]))",
        "quantile_over_time((0.9), (x[5m]))",
        "(quantile_over_time(0.9, (x[5m])))",
    ] {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        assert_eq!(
            tree.to_string(),
            bare.to_string(),
            "`{query}` did not compile to the tree `quantile_over_time(0.9, x[5m])` does"
        );
    }
}

/// A parameter written as an expression binds the number it folds to, because the kernel's constant
/// is a field of the tree: `1 - 0.1` is the quantile `0.9` written two ways, and carrying the
/// expression would bind nothing at all. The value is asserted exactly, so a fold that produced a
/// neighbouring number rather than the arithmetic's own result would fail here.
#[test]
fn a_parameter_folded_from_an_expression_binds_its_value() {
    // `1 - 0.1` is `0.9` bit for bit in IEEE754, and `-(0.9)` is the negative of the same number:
    // negation is exact, and the fold does not decide whether a quantile is inside `[0, 1]`.
    let cases = [
        ("quantile_over_time(1 - 0.1, x[5m])", 0.9),
        ("quantile_over_time(-(0.9), x[5m])", -0.9),
    ];

    for (query, quantile) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (_, op) = root_window(&tree);
        assert_eq!(
            op.function,
            RangeFunction::QuantileOverTime(quantile),
            "`{query}` did not bind the quantile it folds to"
        );
        assert_eq!(op.window.range_ms, 300_000, "`{query}` lost its window");
    }
}

/// A parameter computed at query time is refused, and the error names it: the tree binds a kernel's
/// constant, so a quantile that would only exist at run time has no field to go in.
#[test]
fn a_computed_quantile_is_refused_and_names_what_it_found() {
    // `time()` is a scalar, so the parser accepts it as the quantile; the compiler binds literals.
    let query = "quantile_over_time(time(), x[5m])";
    let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
        .expect_err(&format!("`{query}` compiled, and it should not have"));
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "`{query}` did not report NotImplemented: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("whose first argument is `time()`"),
        "`{query}` did not name the argument it found: {message}"
    );
    assert!(
        message.contains("number literal"),
        "`{query}` did not say what the argument has to be: {message}"
    );
}

/// A parameterised window binds its anchor and its offset exactly as an unparameterised one does.
/// The two are asserted apart because the runtime folds `anchor - offset`, so a binding that
/// dropped either number would still compile a window, and only these assertions would notice.
#[test]
fn a_quantile_over_time_binds_an_anchor_and_an_offset_apart() {
    let tree = compile(
        &eval_stmt("quantile_over_time(0.9, x[5m] @ 1234 offset 1m)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();

    let (_, op) = root_window(&tree);
    assert_eq!(op.function, RangeFunction::QuantileOverTime(0.9));
    assert_eq!(op.window.range_ms, 300_000, "the window was not bound");
    assert_eq!(
        op.window.anchor_ms,
        Some(1_234_000),
        "the anchor was not bound"
    );
    assert_eq!(op.window.offset_ms, 60_000, "the offset was not bound");
}

/// A call with the wrong number of arguments names the count its function takes, rather than
/// reporting whatever the compiler does with one argument. The parser checks arity too, so a
/// mismatched call can only reach the compiler as an edited tree.
#[test]
fn a_call_with_the_wrong_number_of_arguments_names_the_count_it_takes() {
    let cases = [
        // `quantile_over_time` takes its quantile and its range selector: two arguments.
        (
            with_call_arguments("quantile_over_time(0.9, x[5m])", |args| {
                args.remove(1);
            }),
            "quantile_over_time",
            "where it takes 2",
        ),
        // `rate` takes one argument, so nothing can be passed in front of its range selector.
        (
            with_call_arguments("rate(x[5m])", |args| {
                args.push(Box::new(PromExpr::NumberLiteral(NumberLiteral::new(1.0))));
            }),
            "rate",
            "where it takes 1",
        ),
        // A math function's constants are counted the same way: `clamp` reads two of them after its
        // vector, so one is one too few.
        (
            with_call_arguments("clamp(x, 0, 1)", |args| {
                args.pop();
            }),
            "clamp",
            "where it takes its vector and two constants",
        ),
        // A listed math name called with no argument at all: the vector is counted as the first
        // argument, so a call that wrote none is one argument short of `abs` rather than a shape
        // the compiler has no words for.
        (
            with_call_arguments("abs(x)", |args| args.clear()),
            "abs",
            "with 0 argument(s), where it takes its vector",
        ),
    ];

    for (stmt, name, expected) in cases {
        let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{name}` with the wrong argument count compiled"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{name}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains(&format!("a call to `{name}`")),
            "`{name}` did not name itself: {message}"
        );
        assert!(
            message.contains(expected),
            "`{name}` did not name the count it takes: {message}"
        );
    }
}

/// `x` becomes an instant selection over the raw input, on the statement's grid, with the
/// statement's lookback and the input's fields.
#[test]
fn a_bare_selector_becomes_an_instant_over_raw_input() {
    let input = raw_schema();
    // A lookback the statement chose, so the binding cannot be a default the compiler invented.
    let stmt = EvalStmt {
        lookback_delta: Duration::from_secs(120),
        ..eval_stmt("x")
    };
    let tree = compile(&stmt, Arc::clone(&input), SeriesOrdering::KeyThenTime).unwrap();

    let (input_node, op) = root_instant(&tree);
    assert_eq!(op.lookback_ms, 120_000);
    assert_eq!(op.offset_ms, 0);

    // The statement's grid: 0, 5s, ..., 100s.
    let spec = op.output_grid.spec();
    assert_eq!(
        (spec.start_ms, spec.step_ms, spec.len),
        (0, 5_000, 21),
        "the instant selection does not land on the statement's grid"
    );

    // The output is the input's fields, evaluated on the statement's grid.
    let output = tree
        .root()
        .output
        .samples()
        .expect("an instant selection produces samples");
    assert!(
        Arc::ptr_eq(&output.fields, &input.fields),
        "the instant selection did not keep the input's fields"
    );
    let output_grid = output
        .eval_grid
        .as_ref()
        .expect("an instant selection produces evaluated values");
    assert!(
        Arc::ptr_eq(output_grid, &op.output_grid),
        "the output grid is not the bound grid"
    );

    let NativeExpr::Input(bound) = &input_node.expr else {
        panic!(
            "the instant selection's input is not an input: {:?}",
            input_node.expr
        );
    };
    assert!(
        matches!(&bound.output, NativeType::Samples(schema) if schema.eval_grid.is_none()),
        "the input is not raw samples: {:?}",
        bound.output
    );
    assert_eq!(bound.ordering, SeriesOrdering::KeyThenTime);
}

/// A bare selector's offset reaches the instant binding with its sign.
#[test]
fn an_offset_on_a_bare_selector_keeps_its_sign() {
    let cases = [("x offset 1m", 60_000), ("x offset -1m", -60_000)];

    for (query, offset_ms) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime).unwrap();
        let (_, op) = root_instant(&tree);
        assert_eq!(op.offset_ms, offset_ms, "`{query}` lost the offset's sign");
        assert_eq!(op.lookback_ms, 5_000, "`{query}` lost the lookback");
    }
}

/// An `@`-anchored bare selector is refused because the instant node has no anchor field, not
/// because anchoring is unsupported: the window shape binds an anchor.
#[test]
fn an_anchored_instant_selector_is_refused_for_the_missing_anchor_field() {
    let error = compile(
        &eval_stmt("x @ 100"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect_err("`x @ 100` compiled, and it should not have");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "`x @ 100` did not report NotImplemented: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("whose anchor the instant selection node has no field for"),
        "`x @ 100` did not say why its anchor cannot be bound: {message}"
    );
}

/// A window's `@` anchor is bound as an absolute millisecond timestamp: `start()` and `end()` are
/// the statement's own bounds, and a number is the unix second it names.
#[test]
fn an_anchor_on_a_window_resolves_to_absolute_milliseconds() {
    let cases = [
        // `@ <seconds>`: 1234 is unix second 1234, which is 1_234_000 ms.
        ("rate(x[5m] @ 1234)", 1_234_000),
        // A sub-second anchor resolves to the millisecond it names.
        ("rate(x[5m] @ 1.234)", 1_234),
        // Prometheus allows a pre-epoch anchor: `signed_millis_since_epoch` takes `Err(earlier)`,
        // where a `duration_since(UNIX_EPOCH).unwrap_or_default()` helper would silently give 0.
        ("rate(x[5m] @ -1.5)", -1_500),
        // The statement here is UNIX_EPOCH..100s, which is what `start()` and `end()` mean.
        ("rate(x[5m] @ start())", 0),
        ("rate(x[5m] @ end())", 100_000),
    ];

    for (query, anchor_ms) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime).unwrap();
        let (_, op) = root_window(&tree);
        assert_eq!(
            op.window.anchor_ms,
            Some(anchor_ms),
            "`{query}` bound the wrong anchor"
        );
        assert_eq!(op.window.offset_ms, 0, "`{query}` invented an offset");
    }
}

/// `end()` is the statement's end, not the last grid point.
///
/// The grid stops before an end it does not land on, so an end of 103s over a 5s step has its last
/// point at 100s. Binding `end()` to that point instead of to the statement's own end would pass a
/// test whose end is a multiple of the step; here the two numbers differ.
#[test]
fn end_resolves_to_the_statement_end_not_the_last_grid_point() {
    let stmt = eval_stmt_over("rate(x[5m] @ end())", Duration::from_secs(103));
    let tree = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime).unwrap();
    let (_, op) = root_window(&tree);
    assert_eq!(
        op.window.anchor_ms,
        Some(103_000),
        "`@ end()` bound something other than the statement's end"
    );

    // The grid the same statement compiles onto ends at 100s, so the two are not the same number.
    let spec = op.output_grid.spec();
    let last_point = spec.start_ms + (spec.len as i64 - 1) * spec.step_ms;
    assert_eq!(
        last_point, 100_000,
        "the test's own grid does not stop before the statement end"
    );
}

/// An anchor and an offset are bound apart: the window ends at `anchor - offset`, which the
/// runtime folds, so both numbers have to survive compilation.
#[test]
fn an_anchor_and_an_offset_are_bound_apart() {
    let cases = [
        ("rate(x[5m] @ 1.234 offset 1m)", 1_234, 60_000),
        ("rate(x[5m] @ 1234 offset 1m)", 1_234_000, 60_000),
        ("rate(x[5m] @ 1.234 offset -1m)", 1_234, -60_000),
    ];

    for (query, anchor_ms, offset_ms) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime).unwrap();
        let (_, op) = root_window(&tree);
        assert_eq!(
            op.window.anchor_ms,
            Some(anchor_ms),
            "`{query}` lost the anchor"
        );
        assert_eq!(op.window.offset_ms, offset_ms, "`{query}` lost the offset");
    }
}

/// The numbers a tree is made of, at the edges: a span the step does not divide, offsets in either
/// direction, ranges that cannot be a window, and an ordering the scan did not default to.
#[test]
fn the_numeric_edges_of_a_statement_are_bound_as_given() {
    /// What a row asserts about the tree `compile` returns.
    enum Expected {
        /// The statement's grid, by its start, step and length.
        Grid { step_ms: i64, len: u64 },
        /// The window's offset, in milliseconds.
        Offset(i64),
        /// The input's ordering.
        Ordering(SeriesOrdering),
        /// The query is refused.
        Rejected(Rejection),
    }

    /// How a refused query is reported.
    enum Rejection {
        Plan,
        NotImplemented,
    }

    let cases = [
        // 100s at 7s per step is 14 whole steps and a remainder: the grid keeps the last point
        // that does not pass the end, so the remainder is not a point.
        (
            "a span the step does not divide",
            EvalStmt {
                end: UNIX_EPOCH + Duration::from_secs(100),
                interval: Duration::from_secs(7),
                ..eval_stmt("rate(x[5m])")
            },
            SeriesOrdering::KeyThenTime,
            Expected::Grid {
                step_ms: 7_000,
                len: 15,
            },
        ),
        // The offset's sign is what decides whether the window reads earlier or later samples.
        (
            "a positive offset",
            eval_stmt("rate(x[5m] offset 1m)"),
            SeriesOrdering::KeyThenTime,
            Expected::Offset(60_000),
        ),
        (
            "a negative offset",
            eval_stmt("rate(x[5m] offset -1m)"),
            SeriesOrdering::KeyThenTime,
            Expected::Offset(-60_000),
        ),
        // A selector that selects nothing, and one whose length no millisecond count can hold.
        (
            "a zero-length range",
            with_selector_range(Duration::ZERO),
            SeriesOrdering::KeyThenTime,
            Expected::Rejected(Rejection::Plan),
        ),
        (
            "a range wider than i64 milliseconds",
            eval_stmt("rate(x[4294967295y])"),
            SeriesOrdering::KeyThenTime,
            Expected::Rejected(Rejection::Plan),
        ),
        // An instant selection's lookback is narrowed like every other duration.
        (
            "a lookback wider than i64 milliseconds",
            EvalStmt {
                lookback_delta: Duration::from_millis(u64::MAX),
                ..eval_stmt("x")
            },
            SeriesOrdering::KeyThenTime,
            Expected::Rejected(Rejection::Plan),
        ),
        // Every shape reads one series to its last chunk before the next, so a scan whose chunks
        // of different series interleave is refused instead of recorded onto an input that shape
        // would fail on.
        (
            "an interleaved ordering, which no shape can consume to its last chunk",
            eval_stmt("rate(x[5m])"),
            SeriesOrdering::Interleaved,
            Expected::Rejected(Rejection::NotImplemented),
        ),
        // A supported ordering is recorded on the input rather than dropped.
        (
            "a non-default ordering a window can consume",
            eval_stmt("rate(x[5m])"),
            SeriesOrdering::Contiguous,
            Expected::Ordering(SeriesOrdering::Contiguous),
        ),
        // A shape outside the subset is refused for its shape, whatever its timing: the statement
        // range of an unsupported shape is not read, so it cannot turn a refusal into a plan
        // error. A subquery is the reachable case of a shape with no node at all.
        (
            "an unsupported shape with an inverted range",
            EvalStmt {
                end: UNIX_EPOCH,
                start: UNIX_EPOCH + Duration::from_secs(100),
                ..eval_stmt("x[5m:]")
            },
            SeriesOrdering::KeyThenTime,
            Expected::Rejected(Rejection::NotImplemented),
        ),
    ];

    for (case, stmt, ordering, expected) in cases {
        match (expected, &compile(&stmt, raw_schema(), ordering)) {
            (Expected::Rejected(rejection), Err(error)) => {
                let as_expected = match rejection {
                    Rejection::Plan => matches!(error, DataFusionError::Plan(_)),
                    Rejection::NotImplemented => {
                        matches!(error, DataFusionError::NotImplemented(_))
                    }
                };
                assert!(
                    as_expected,
                    "`{case}` was rejected with the wrong kind of error: {error}"
                );
            }
            (Expected::Rejected(_), Ok(_)) => panic!("`{case}` compiled, and it should not have"),
            (_, Err(error)) => panic!("`{case}` did not compile: {error}"),
            (Expected::Grid { step_ms, len }, Ok(tree)) => {
                let (_, op) = root_window(tree);
                let spec = op.output_grid.spec();
                assert_eq!(
                    (spec.start_ms, spec.step_ms, spec.len),
                    (0, step_ms, len),
                    "`{case}` did not land on the statement's grid"
                );
            }
            (Expected::Offset(offset_ms), Ok(tree)) => {
                let (_, op) = root_window(tree);
                assert_eq!(
                    op.window.offset_ms, offset_ms,
                    "`{case}` lost the offset's sign"
                );
            }
            (Expected::Ordering(want), Ok(tree)) => {
                let (input, _) = root_window(tree);
                let NativeExpr::Input(bound) = &input.expr else {
                    panic!("`{case}`'s window has no input: {:?}", input.expr);
                };
                assert_eq!(
                    bound.ordering, want,
                    "`{case}` dropped the caller's ordering"
                );
            }
        }
    }
}

/// An interleaved scan is refused whatever shape the statement has. The refusal sits at the entry
/// of the compiler rather than in one shape's branch, because a bare selector returns before any
/// window exists and would otherwise be accepted with an ordering it cannot be evaluated on.
#[test]
fn an_interleaved_scan_is_refused_whatever_the_shape() {
    // The plain range function is the ordering table's case, which compiles `rate(x[5m])` under
    // `Interleaved` and expects the same refusal; these cover the shapes that table cannot: the
    // bare selector, a math function over one, and a parameterised range function.
    for query in ["x", "abs(x)", "quantile_over_time(0.9, x[5m])"] {
        let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::Interleaved)
            .expect_err(&format!("`{query}` compiled over an interleaved scan"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("reads one series to its last chunk before the next"),
            "`{query}` did not say why the ordering is refused: {message}"
        );
    }
    // The same scan compiles a bare selector when its chunks are contiguous, and the ordering it
    // was compiled under is recorded on the tree's input rather than dropped: that is what makes
    // the refusals above about the ordering rather than about the shape.
    let tree = compile(&eval_stmt("x"), raw_schema(), SeriesOrdering::Contiguous)
        .expect("a contiguous scan compiles a bare selector");
    let (input, _) = root_instant(&tree);
    let NativeExpr::Input(bound) = &input.expr else {
        panic!(
            "an instant selection's input is not an input: {:?}",
            input.expr
        );
    };
    assert_eq!(
        bound.ordering,
        SeriesOrdering::Contiguous,
        "the instant selection dropped the scan's ordering"
    );
}

/// A shape outside the subset is refused, and the error says what was found so the query can be
/// rewritten rather than guessed at.
#[test]
fn an_unsupported_shape_is_rejected_and_names_what_it_found() {
    let cases = [
        ("x[5m:]", "the expression `x[5m:]`"),
        // Nested shapes are refused by the argument check, not skipped over. A subquery is the
        // reachable case of an argument that is not a range selector: the parser type-checks
        // function arguments, so `rate(x)` never gets past parsing.
        (
            "rate(x[5m:])",
            "a call to `rate` whose argument is `x[5m:]`",
        ),
        // A function with no kernel, whether or not it takes a constant argument.
        (
            "predict_linear(x[5m], 300)",
            "a call to `predict_linear`, which has no native kernel",
        ),
        (
            "absent(x)",
            "a call to `absent`, which has no native kernel",
        ),
    ];

    for (query, named) in cases {
        let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains(named),
            "`{query}` did not name `{named}`: {message}"
        );
        assert!(
            message.contains("rate(x[5m])"),
            "`{query}` did not quote the supported shape: {message}"
        );
    }
}

/// The display of a compiled tree, checked as a whole so the compiler and the display cannot drift
/// apart: a wrong grid, kernel or nesting shows up on a line someone would read.
#[test]
fn the_display_of_a_compiled_tree_is_the_tree() {
    let tree = compile(
        &eval_stmt("rate(x[5m])"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();

    assert_eq!(
        tree.to_string(),
        [
            "NativeTree",
            "  root Window rate [300000ms] offset 0 anchor - on grid 0/5000x21",
            "    input Input raw samples",
        ]
        .join("\n")
    );
}

/// The display of a tree the batch added nodes to, checked as a whole: the shapes are read by
/// someone reading an `EXPLAIN`, so a wrong nesting, operator, constant or grouping has to show up on
/// a line rather than only in an assertion over the tree.
#[test]
fn the_display_of_a_tree_the_batch_added_to_is_the_query() {
    let cases: [(&str, &[&str]); 2] = [
        (
            "-x",
            &[
                "NativeTree",
                "  root Binary * ignoring [] one-to-one",
                "    left Scalar -1",
                "    right Instant lookback 5000ms offset 0 on grid 0/5000x21",
                "      input Input raw samples",
            ],
        ),
        (
            "0 < x",
            &[
                "NativeTree",
                "  root Binary < ignoring [] one-to-one",
                "    left Scalar 0",
                "    right Instant lookback 5000ms offset 0 on grid 0/5000x21",
                "      input Input raw samples",
            ],
        ),
    ];

    for (query, expected) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        assert_eq!(
            tree.to_string(),
            expected.join("\n"),
            "`{query}` is displayed as a different query"
        );
    }
}

/// Parentheses group; they do not change what a statement asks for. A compiler that refused them
/// would reject a query on how it was typed rather than on what it means, and a statement written
/// with them is otherwise exactly the shape the compiler accepts.
#[test]
fn parentheses_around_the_accepted_shape_are_transparent() {
    let bare = compile(
        &eval_stmt("rate(x[5m])"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    for query in [
        "(rate(x[5m]))",
        "rate((x[5m]))",
        "rate(((x[5m])))",
        "((rate((x[5m]))))",
    ] {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        assert_eq!(
            tree.to_string(),
            bare.to_string(),
            "`{query}` did not compile to the tree `rate(x[5m])` does"
        );
    }
}
/// A math function compiles by compiling the expression it is applied to, so its node sits over the
/// node that expression produces: `abs(rate(x[5m]))` is a map over the window `rate(x[5m])` is, not
/// a fourth shape the compiler knows by name. The same rule reads `abs(x)` over an instant
/// selection, which is why the two are asserted together: nothing below the map is specific to it.
#[test]
fn a_math_function_maps_over_the_node_its_argument_compiles_to() {
    let tree = compile(
        &eval_stmt("abs(rate(x[5m]))"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`abs(rate(x[5m]))` was refused: {error}"));

    assert!(
        matches!(map_op(tree.root()), MathOp::Abs),
        "the root is not an abs: {:?}",
        map_parts(tree.root()).1.ops
    );
    let (window, _) = map_parts(tree.root());
    let NativeExpr::Window { input, op } = &window.expr else {
        panic!("the map's input is not a window: {:?}", window.expr);
    };
    assert_eq!(op.function, RangeFunction::Rate);
    assert_eq!(op.window.range_ms, 300_000);
    let NativeExpr::Input(bound) = &input.expr else {
        panic!("the window's input is not an input: {:?}", input.expr);
    };
    assert!(
        matches!(&bound.output, NativeType::Samples(schema) if schema.eval_grid.is_none()),
        "the window's input is not raw samples: {:?}",
        bound.output
    );
    assert_eq!(bound.ordering, SeriesOrdering::KeyThenTime);

    // The same rule over the other selection shape: a map over an instant selection.
    let tree = compile(
        &eval_stmt("abs(x)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`abs(x)` was refused: {error}"));
    assert!(matches!(map_op(tree.root()), MathOp::Abs));
    let (instant, _) = map_parts(tree.root());
    assert!(
        matches!(instant.expr, NativeExpr::Instant { .. }),
        "`abs(x)`'s map is not over an instant selection: {:?}",
        instant.expr
    );
}

/// Nested math functions are two maps over one window, and every map reports that window's layout.
///
/// The maps are separate nodes because the arguments nest, not because the compiler knows
/// `clamp_min(abs(...))`: a compiler that bound the pair to one node would fail the tree assertions
/// here. The grids are compared by their spec, which is what [`NativeTree::validate`] compares: a
/// map that allocated a second, equal grid would still be the window's layout, while one that
/// reported a different grid or different fields would not.
#[test]
fn nested_math_functions_are_two_maps_over_one_window() {
    let tree = compile(
        &eval_stmt("clamp_min(abs(rate(x[5m])), 0)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`clamp_min(abs(rate(x[5m])), 0)` was refused: {error}"));

    let MathOp::ClampMin { min } = map_op(tree.root()) else {
        panic!(
            "the root is not a clamp_min: {:?}",
            map_parts(tree.root()).1.ops
        );
    };
    assert_eq!(
        min, 0.0,
        "the clamp bound is not the number the query wrote"
    );

    let (inner, _) = map_parts(tree.root());
    assert!(
        matches!(map_op(inner), MathOp::Abs),
        "the inner node is not an abs: {:?}",
        map_parts(inner).1.ops
    );
    let (window, op) = map_parts(inner);
    let NativeExpr::Window { op: bound, .. } = &window.expr else {
        panic!("the inner map's input is not a window: {:?}", window.expr);
    };
    assert_eq!(bound.function, RangeFunction::Rate);
    assert_eq!(op.ops.len(), 1, "a map applies one operation");

    let window_schema = evaluated(window);
    let window_grid = window_schema
        .eval_grid
        .as_ref()
        .expect("the window is evaluated");
    for (what, node) in [("the clamp_min", tree.root()), ("the abs", inner)] {
        let schema = evaluated(node);
        assert_eq!(
            schema
                .eval_grid
                .as_ref()
                .expect("the map is evaluated")
                .spec(),
            window_grid.spec(),
            "`{what}` reports a grid the window does not"
        );
        assert!(
            schema
                .fields
                .iter()
                .map(|field| &field.name)
                .eq(window_schema.fields.iter().map(|field| &field.name)),
            "`{what}` reports fields the window does not"
        );
    }
}

/// Every listed math name binds its own kernel, with the constants the call wrote. The display names
/// the operation and its constants, so a name that fell through to a neighbour's variant — or a
/// parameter that was bound from the wrong argument — shows up in a line someone would read.
#[test]
fn the_supported_math_functions_map_to_their_kernels() {
    let cases = [
        ("abs(x)", "abs"),
        ("ceil(x)", "ceil"),
        ("floor(x)", "floor"),
        ("sqrt(x)", "sqrt"),
        ("exp(x)", "exp"),
        ("ln(x)", "ln"),
        ("log2(x)", "log2"),
        ("log10(x)", "log10"),
        ("sin(x)", "sin"),
        ("cos(x)", "cos"),
        ("tan(x)", "tan"),
        ("asin(x)", "asin"),
        ("acos(x)", "acos"),
        ("atan(x)", "atan"),
        ("sinh(x)", "sinh"),
        ("cosh(x)", "cosh"),
        ("tanh(x)", "tanh"),
        ("asinh(x)", "asinh"),
        ("acosh(x)", "acosh"),
        ("atanh(x)", "atanh"),
        ("deg(x)", "deg"),
        ("rad(x)", "rad"),
        ("sgn(x)", "sgn"),
        // The three shapes that carry constants, including the default `round` binds one for.
        ("round(x)", "round 1"),
        ("round(x, 0.5)", "round 0.5"),
        ("clamp(x, 0, 1)", "clamp 0 1"),
        ("clamp_min(x, 0)", "clamp_min 0"),
        ("clamp_max(x, 1)", "clamp_max 1"),
    ];

    for (query, op) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let display = tree.to_string();
        let root = display.lines().nth(1).expect("a compiled tree has a root");
        assert_eq!(
            root,
            format!("  root Map {op}"),
            "`{query}` bound the wrong kernel"
        );
    }
}

/// The constants a function folds are folded by the kernel's own arithmetic, not by a second copy of
/// it: `%` keeps the sign of its left operand and `^` is `go_math`'s power. Both numbers below are
/// ones a hand-written copy would get wrong: a Euclidean modulo folds `-7 % 3` to 2, `*` folds the
/// power to about 9.8e8, and Rust's own `f64::powf` folds it to 3.623449445683154e291 where the
/// kernel gives 3.6234491766672406e291, because `go_math::pow` accumulates error by repeated
/// squaring as Go's `math.Pow` does. That is the point: the compiler must not disagree with the
/// kernel about what an operator means.
#[test]
fn the_constant_fold_is_the_kernels_arithmetic() {
    // Go's `math.Mod` gives -1 for `-7 % 3`; a Euclidean modulo would give 2.
    let tree = compile(
        &eval_stmt("clamp_min(x, (-7) % 3)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`clamp_min(x, (-7) % 3)` was refused: {error}"));
    let MathOp::ClampMin { min } = map_op(tree.root()) else {
        panic!(
            "the root is not a clamp_min: {:?}",
            map_parts(tree.root()).1.ops
        );
    };
    assert_eq!(min, -1.0, "`-7 % 3` did not fold with `math.Mod`'s sign");

    // `1.0000006850274261 ^ 9.80019037e8` is a power over a base barely above one: as multiplication
    // it would be about 9.8e8, and it is neither a comparison nor a set operator, which fold nothing
    // at all. It is also the case `go_math` keeps its own `pow` for: Go's repeated squaring
    // accumulates error the standard library's `pow` does not, so the two disagree well above the
    // last bit and a fold that hand-wrote `powf` binds a different number here.
    let tree = compile(
        &eval_stmt("round(x, 1.0000006850274261 ^ 9.80019037e8)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| {
        panic!("`round(x, 1.0000006850274261 ^ 9.80019037e8)` was refused: {error}")
    });
    let MathOp::Round { nearest } = map_op(tree.root()) else {
        panic!(
            "the root is not a round: {:?}",
            map_parts(tree.root()).1.ops
        );
    };
    assert_eq!(
        nearest, 3.6234491766672406e291,
        "`1.0000006850274261 ^ 9.80019037e8` did not fold with `go_math`'s power"
    );
}

/// A math function is pointwise over a vector, so an argument that is not one is refused and the
/// rejection names the argument rather than the function alone. The parser refuses `abs(1)` too, so
/// the AST is edited the way the arity test edits one: a shape the parser would have caught is still
/// a shape the compiler must not compile into something else.
#[test]
fn a_math_function_over_a_non_vector_argument_is_refused_and_names_it() {
    let cases = [
        // A scalar, which a pointwise function has no positions to be pointwise over.
        (
            with_call_arguments("abs(x)", |args| {
                *args[0] = PromExpr::NumberLiteral(NumberLiteral::new(1.0));
            }),
            "a call to `abs` whose first argument is `1`, which is a scalar",
        ),
        // A range selector, which belongs in a window rather than under a map.
        (
            with_call_arguments("abs(x)", |args| {
                args[0] = a_matrix_selector();
            }),
            "a call to `abs` whose first argument is `x[5m]`, which is a matrix",
        ),
    ];

    for (stmt, named) in cases {
        let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err("a math function over a non-vector compiled");
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "the refusal is not NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains(named),
            "the refusal did not name the argument it found: {message}"
        );
        assert!(
            message.contains("where the function takes a vector"),
            "the refusal did not say what the argument has to be: {message}"
        );
    }
}

/// A constant on the right of a vector binds as the number it is, next to the vector the rest of the
/// query compiles to: `x * 100` is `x`'s instant selection multiplied by `100`.
#[test]
fn a_constant_binds_as_a_scalar_next_to_the_vector() {
    let input = raw_schema();
    let tree = compile(
        &eval_stmt("x * 100"),
        Arc::clone(&input),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();

    let (left, right, matching) = binary_parts(tree.root());
    assert_eq!(matching.op, BinaryOp::Mul);
    assert!(
        !matching.return_bool,
        "`x * 100` is not a comparison and has no bool"
    );

    let NativeExpr::Instant { .. } = &left.expr else {
        panic!(
            "the left operand is not the selector's instant: {:?}",
            left.expr
        );
    };
    assert_eq!(
        constant(right),
        Some(100.0),
        "the right operand is not the constant the query wrote"
    );
    assert_eq!(
        scalar_grid(right),
        (0, 5_000, 21),
        "the constant is not on the statement's grid"
    );

    // A scalar carries no fields, so the operation reports the vector side's layout and grid.
    let output = evaluated(tree.root());
    assert!(
        Arc::ptr_eq(&output.fields, &input.fields),
        "the broadcast did not keep the vector's fields"
    );
    assert_eq!(
        grid_of(tree.root()),
        (0, 5_000, 21),
        "the operation does not land on the statement's grid"
    );
}

/// The operands keep the order they were written in, because most operators are not commutative:
/// `1 / x` is not `x / 1`. The constant side is what tells them apart, so a compiler that sorted the
/// operands would compile both queries to the same, wrong tree.
#[test]
fn a_non_commutative_operation_keeps_the_operands_where_they_were_written() {
    let cases = [
        // (query, operator, the constant, whether the constant is on the left)
        ("1 / x", BinaryOp::Div, 1.0, true),
        ("x / 1", BinaryOp::Div, 1.0, false),
        ("2 - x", BinaryOp::Sub, 2.0, true),
        ("x - 2", BinaryOp::Sub, 2.0, false),
        ("x % 2", BinaryOp::Mod, 2.0, false),
        ("2 ^ x", BinaryOp::Pow, 2.0, true),
    ];

    for (query, op, value, constant_on_the_left) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (left, right, matching) = binary_parts(tree.root());
        assert_eq!(matching.op, op, "`{query}` bound the wrong operator");
        let (scalar_side, vector_side) = if constant_on_the_left {
            (left, right)
        } else {
            (right, left)
        };
        assert_eq!(
            constant(scalar_side),
            Some(value),
            "`{query}` put the constant on the wrong side"
        );
        assert!(
            !matches!(vector_side.expr, NativeExpr::Scalar(_)),
            "`{query}` compiled the vector operand to a constant: {:?}",
            vector_side.expr
        );
    }
}

/// A binary operation compiles by compiling both operands, so it nests as deeply as the query was
/// written: `2 / (x + 1)` is a division whose right operand is the addition, not a shape the
/// compiler knows by name.
#[test]
fn a_binary_operation_nests_over_the_operands_it_was_written_with() {
    let tree = compile(
        &eval_stmt("2 / (x + 1)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`2 / (x + 1)` was refused: {error}"));

    let (left, right, matching) = binary_parts(tree.root());
    assert_eq!(matching.op, BinaryOp::Div);
    assert_eq!(constant(left), Some(2.0), "the divisor was not bound");

    let (inner_left, inner_right, inner_matching) = binary_parts(right);
    assert_eq!(inner_matching.op, BinaryOp::Add);
    let NativeExpr::Instant { .. } = &inner_left.expr else {
        panic!(
            "the sum's left operand is not the selector: {:?}",
            inner_left.expr
        );
    };
    assert_eq!(
        constant(inner_right),
        Some(1.0),
        "the summand was not bound as a constant"
    );

    // Every node is on the statement's grid, which is what lets the operation align them.
    assert_eq!(grid_of(tree.root()), (0, 5_000, 21));
    assert_eq!(grid_of(right), (0, 5_000, 21));
}

/// `return_bool` is a field of the operation rather than a different tree: `x > 0` filters and
/// `x > bool 0` reports zero or one, and the two differ in nothing else. Both directions of a
/// comparison are asserted, because which side the constant is on decides what the filter keeps and
/// a compiler that always put the constant on one side would compile `0 < x` as `x > 0`.
#[test]
fn a_comparison_carries_whether_it_filters_or_reports_bool() {
    let cases = [
        ("x > 0", BinaryOp::Gt, false),
        ("x > bool 0", BinaryOp::Gt, true),
        ("0 < x", BinaryOp::Lt, false),
        ("0 < bool x", BinaryOp::Lt, true),
        ("x == 0", BinaryOp::Eq, false),
        ("x != 0", BinaryOp::Ne, false),
        ("x >= 0", BinaryOp::Ge, false),
        ("x <= 0", BinaryOp::Le, false),
    ];

    for (query, op, return_bool) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (_, _, matching) = binary_parts(tree.root());
        assert_eq!(matching.op, op, "`{query}` bound the wrong operator");
        assert_eq!(
            matching.return_bool, return_bool,
            "`{query}` bound the wrong comparison mode"
        );
    }

    // The constant keeps the side it was written on. `0 < x` and `x > 0` keep the same values but
    // are not the same tree: a compiler that normalised the comparison would answer the other
    // query's filter, which is why the side is asserted rather than only the operator.
    let left_constant = compile(
        &eval_stmt("0 < x"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    let (left, right, _) = binary_parts(left_constant.root());
    assert_eq!(
        constant(left),
        Some(0.0),
        "`0 < x` did not keep its constant on the left"
    );
    assert!(
        constant(right).is_none(),
        "`0 < x` put the constant on the right, which is `x > 0`"
    );

    // The two modes are one tree otherwise: the query text is what differs, and a tree that recorded
    // the mode somewhere else would fail the assertions above rather than this one.
    let filters = compile(
        &eval_stmt("x > 0"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    let reports = compile(
        &eval_stmt("x > bool 0"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    assert_eq!(grid_of(filters.root()), grid_of(reports.root()));
}

/// A negation binds as the multiplication it is: `-x` is `-1 * x`. The tree has no negate node, and
/// the multiplication kernel computes exactly the value the query asks for.
#[test]
fn a_negation_binds_as_a_multiplication_by_minus_one() {
    for query in ["-x", "-(x)"] {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (left, right, matching) = binary_parts(tree.root());
        assert_eq!(
            matching.op,
            BinaryOp::Mul,
            "`{query}` is not bound as a multiplication"
        );
        assert_eq!(
            constant(left),
            Some(-1.0),
            "`{query}` did not bind `-1` on the left"
        );
        let NativeExpr::Instant { .. } = &right.expr else {
            panic!("`{query}`'s operand is not the selector: {:?}", right.expr);
        };
    }
}

/// A doubled negation is a negation of a negation, not a compile error or a dropped sign.
///
/// `--x` binds as `-1 * (-1 * x)`: the outer multiplication is the one the query's first minus asks
/// for, and the inner one is what the second minus lowered to. Nothing here cancels the two into
/// `x`, because the tree has no rule for that and the kernel computes the product anyway.
#[test]
fn a_doubled_negation_nests_two_multiplications() {
    let tree = compile(&eval_stmt("--x"), raw_schema(), SeriesOrdering::KeyThenTime)
        .unwrap_or_else(|error| panic!("`--x` was refused: {error}"));
    let (left, right, matching) = binary_parts(tree.root());
    assert_eq!(matching.op, BinaryOp::Mul, "`--x` is not a multiplication");
    assert_eq!(
        constant(left),
        Some(-1.0),
        "`--x` did not bind its outer `-1` on the left"
    );
    let (inner_left, _, inner_matching) = binary_parts(right);
    assert_eq!(
        inner_matching.op,
        BinaryOp::Mul,
        "the operand of `--x` is not the inner multiplication"
    );
    assert_eq!(
        constant(inner_left),
        Some(-1.0),
        "the inner negation did not bind its own `-1`"
    );
}

/// The negation applies to whatever its operand compiles to, so a window can be negated: `-rate(x[5m])`
/// is a multiplication over the window rather than a new shape.
#[test]
fn a_negation_applies_to_the_node_its_operand_compiles_to() {
    let tree = compile(
        &eval_stmt("-rate(x[5m])"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`-rate(x[5m])` was refused: {error}"));

    let (left, right, matching) = binary_parts(tree.root());
    assert_eq!(matching.op, BinaryOp::Mul);
    assert_eq!(constant(left), Some(-1.0));
    let NativeExpr::Window { op, .. } = &right.expr else {
        panic!("the negation's operand is not a window: {:?}", right.expr);
    };
    assert_eq!(op.function, RangeFunction::Rate);
    assert_eq!(op.window.range_ms, 300_000);
}

/// A constant written as an expression binds the number it folds to, by the fold a call's parameter
/// already uses. The value is asserted exactly, so a fold that produced a neighbouring number would
/// fail here.
#[test]
fn a_constant_operand_binds_the_value_it_folds_to() {
    let cases = [
        ("x * (1 + 2)", 3.0),
        ("x + 2 ^ 10", 1024.0),
        // `^` binds tighter than `*`, so this is `x * 8` rather than `(x * 2) ^ 3`.
        ("x * 2 ^ 3", 8.0),
        ("x * -0.5", -0.5),
        ("x * (1 - 0.1)", 0.9),
    ];

    for (query, value) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (left, right, _) = binary_parts(tree.root());
        let scalar_side = if constant(left).is_some() {
            left
        } else {
            right
        };
        assert_eq!(
            constant(scalar_side),
            Some(value),
            "`{query}` did not bind the number it folds to"
        );
    }
}

/// An operation between two vectors is refused because the tree cannot say which selector each
/// `Input` reads: `a + b` and `a + a` would compile to the same tree. The refusal names the missing
/// identity rather than the query it found, so whoever adds it knows what to add.
#[test]
fn an_operation_between_two_vectors_is_refused_for_the_missing_selector_identity() {
    for query in [
        "a + b",
        "a > b",
        "rate(a[5m]) * rate(b[5m])",
        "abs(a) + abs(b)",
    ] {
        let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("between two vectors"),
            "`{query}` did not say what it found: {message}"
        );
        assert!(
            message.contains("selector"),
            "`{query}` did not name what an input is missing: {message}"
        );
    }
}

/// A set operator has no node at all, so it is refused as the set operator it is rather than as an
/// arithmetic operation the compiler could not bind: `and`, `or` and `unless` combine two vectors'
/// series, which is not what a binary value node does.
#[test]
fn a_set_operator_is_refused_as_a_set_operator() {
    for (query, name) in [
        ("a and b", "and"),
        ("a or b", "or"),
        ("a unless b", "unless"),
    ] {
        let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains(&format!("set operator `{name}`")),
            "`{query}` did not name the operator it found: {message}"
        );
    }
}

/// A statement with no vector has nothing to read, so it is refused: a tree reads a scan and reports
/// evaluated samples, and a constant is bound next to a vector rather than on its own. The refusal
/// names the expression and its type, because the type is why it cannot be bound.
#[test]
fn a_statement_with_no_vector_is_refused() {
    // `pi()` and `time()` are the two functions PromQL calls with no arguments. Both are scalars, so
    // they reach this refusal before any function-name lookup: what a tree cannot do with them is
    // report them, not fold them. The rejection still names the expression it found, so a reader
    // learns which function it was.
    for (query, found) in [
        ("1 + 2", "which is a scalar"),
        ("1", "which is a scalar"),
        ("-3", "which is a scalar"),
        ("pi()", "which is a scalar"),
        ("time()", "which is a scalar"),
    ] {
        let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains(&format!("`{query}`")),
            "`{query}` did not name the expression it found: {message}"
        );
        assert!(
            message.contains(found),
            "`{query}` did not name the type it found: {message}"
        );
    }
}

/// A vector-matching modifier has nothing to match when one operand is a constant, and the tree's
/// matching has no field for a fill value. Both are refused rather than dropped: dropping one would
/// compile a query into an operation other than the one it wrote. `bool` is not a matching modifier
/// and is bound, which is why it is not in this list.
#[test]
fn a_vector_matching_modifier_over_a_constant_is_refused() {
    let cases = [
        (eval_stmt("x + ignoring() 1"), "x + ignoring() 1"),
        (eval_stmt("x + on() group_left 1"), "x + on() group_left 1"),
        (
            eval_stmt("x + on() group_right 1"),
            "x + on() group_right 1",
        ),
        // A fill value is a matching extension the tree has no field for, so a fill is refused
        // rather than bound without it. The parser accepts a fill only between two vectors, so the
        // value is set on a tree the parser accepted.
        (
            with_binary_modifier("x + 1", |modifier| {
                modifier.fill_values = VectorMatchFillValues::new(0.0, 0.0);
            }),
            "x + 1 with a fill of 0 on both sides",
        ),
    ];

    for (stmt, query) in cases {
        let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("vector matching"),
            "`{query}` did not say what it found: {message}"
        );
        assert!(
            message.contains("constant operand"),
            "`{query}` blamed the wrong operand: {message}"
        );
    }
}

/// A modifier inside a constant argument is refused for every call that folds one, not just for an
/// arithmetic operand.
///
/// A call's constant is folded by the same [`constant_value`], so a fill written inside one has to
/// reach the same refusal: `quantile(0.5 + fill(0) 0.1, x)` used to bind `Quantile{0.6}`, and
/// `predict_linear(x[5m], 1 + fill(0) 2)` used to bind its second argument as `3`. Both dropped the
/// fill the query wrote.
#[test]
fn a_modifier_inside_a_folded_call_argument_is_refused() {
    for (stmt, query, argument) in [
        (
            eval_stmt("quantile(0.5 + fill(0) 0.1, x)"),
            "quantile(0.5 + fill(0) 0.1, x)",
            "first argument",
        ),
        (
            eval_stmt("predict_linear(x[5m], 1 + fill(0) 2)"),
            "predict_linear(x[5m], 1 + fill(0) 2)",
            "second argument",
        ),
    ] {
        let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        // The two calls refuse it on different paths -- `quantile` reads its parameter as a constant
        // expression, a range function's constant is bound by the signature -- so what is asserted is
        // what both have to say: which argument was folded, and that a fill is what it found.
        let message = error.to_string();
        assert!(
            message.contains(argument),
            "`{query}` blamed the wrong argument: {message}"
        );
        assert!(
            message.contains("fill"),
            "`{query}` did not name the fill it found: {message}"
        );
    }
}

/// A `bool` comparison of two constants folds to the number it reports, so the query runs.
///
/// `x * (1 < bool 2)` is `x * 1`: the inner comparison is a value, not an operation over a vector.
/// Leaving it unfolded produces a `Binary` of two constants, which `validate` accepts and the
/// executor refuses only when it builds the state — a legal query that fails at run time instead of
/// answering. The fold keeps it a number, and the number is the kernel's own answer.
#[test]
fn a_bool_comparison_of_two_constants_folds_to_its_number() {
    for (query, folded) in [
        ("x * (1 < bool 2)", 1.0),
        ("x * (1 > bool 2)", 0.0),
        ("x * (1 == bool 1)", 1.0),
        ("x * (1 != bool 1)", 0.0),
        // NaN satisfies only `!=`, which the fold inherits from the kernel rather than restating.
        ("x * (0 / 0 == bool 0)", 0.0),
        ("x * (0 / 0 != bool 0)", 1.0),
    ] {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (left, right, _) = binary_parts(tree.root());
        let scalar_side = if constant(left).is_some() {
            left
        } else {
            right
        };
        assert_eq!(
            constant(scalar_side),
            Some(folded),
            "`{query}` did not fold its comparison to {folded}"
        );
    }
}

/// A comparison of two constants without `bool` has no value to report, and is refused as such.
///
/// The parser refuses this shape outright — "comparisons between scalars must use BOOL modifier" is
/// its own message for `1 > 2` — so the only way to hand the compiler one is to clear the flag on a
/// tree it accepted. The compiler has to refuse it all the same: a caller that builds a statement
/// without going through the parser would otherwise get a node that only fails when it runs.
#[test]
fn a_comparison_of_two_constants_without_bool_is_refused() {
    let mut stmt = eval_stmt("x * (1 > bool 2)");
    let PromExpr::Binary(outer) = &mut stmt.expr else {
        unreachable!("`x * (1 > bool 2)` parses to a binary operation");
    };
    // The comparison is written in parentheses, which the parser keeps as a node of their own.
    let PromExpr::Paren(grouped) = &mut *outer.rhs else {
        unreachable!("the right operand is a parenthesised expression");
    };
    let PromExpr::Binary(inner) = &mut *grouped.expr else {
        unreachable!("the parenthesised operand is the comparison");
    };
    inner
        .modifier
        .get_or_insert_with(BinModifier::default)
        .return_bool = false;

    let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime).expect_err(
        "a comparison of two constants without `bool` compiled, and it should not have",
    );
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "the comparison did not report NotImplemented: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("between two constants"),
        "the comparison did not say what it found: {message}"
    );
}

/// An aggregate is a shape this batch does not bind, and it is refused as the expression it is.
///
/// The later batch that binds `sum` and the rest replaces the shape this test names, which is why
/// the refusal is asserted here rather than left to whichever expression the match happens to reach.
#[test]
fn an_aggregate_is_refused_as_an_expression() {
    for query in ["sum(x)", "avg by(job)(rate(x[5m]))"] {
        let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("the expression `"),
            "`{query}` did not name the expression it found: {message}"
        );
    }
}

/// A modifier inside a constant subexpression is refused rather than folded away.
///
/// `x * (1 + fill(0) 2)` asks for a fill, which the tree has no field for. The inner addition is a
/// constant, so the fold runs before the binary operation is dispatched: a fold that computes the
/// number without reading the modifier answers `x * 3` and the fill disappears from a query that
/// wrote one.
#[test]
fn a_modifier_inside_a_folded_constant_is_refused() {
    let error = compile(
        &eval_stmt("x * (1 + fill(0) 2)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect_err("`x * (1 + fill(0) 2)` compiled, and it should not have");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "`x * (1 + fill(0) 2)` did not report NotImplemented: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("vector matching"),
        "`x * (1 + fill(0) 2)` did not say what it found: {message}"
    );

    // An unmodified constant subexpression still folds: the refusal is about the modifier, not
    // about the fold.
    compile(
        &eval_stmt("x * (1 + 2)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect("`x * (1 + 2)` has no modifier to refuse");
}

/// The shared acceptance queries of this batch, compiled whole: each one combines the shapes the
/// batch added, so a compiler that bound any of them wrongly would fail here rather than in a
/// narrower test.
#[test]
fn the_batch_acceptance_queries_compile() {
    let cases = [
        // Recursion plus a constant on the left.
        "2 / (x + 1)",
        // Filter against bool comparison, in both directions.
        "x > 0",
        "0 < x",
        "x > bool 0",
        // A negation.
        "-x",
    ];

    for query in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        // Every tree the compiler accepts reports a grid, whether it ends in a comparison or a
        // scalar broadcast.
        assert_eq!(
            grid_of(tree.root()),
            (0, 5_000, 21),
            "`{query}` does not land on the statement's grid"
        );
    }
}
