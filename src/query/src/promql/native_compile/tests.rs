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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datatypes::arrow::array::Float64Array;
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, SampleRef, Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, SeriesLabels};
use promql::evaluated::EvaluatedChunk;
use promql::extension_plan::FieldKind;
use promql::map::MathOp;
use promql::map_time::DateFunction;
use promql::native::execute::{InputId, NativeExecutionState, NativeExecutor};
use promql::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundSetOp,
    BoundWindow, Grouping, InstantValue, MapOp, NativeExpr, NativeNode, NativeTree, NativeType,
    ParameterizedOp,
};
use promql::range_eval::RangeFunction;
use promql::select::MatchOp;
use promql::vector_match::{BinaryMatching, BinaryOp, Cardinality, MatchingLabels, SetOp};
use promql_parser::parser::{
    AtModifier, BinModifier, EvalStmt, Expr as PromExpr, NumberLiteral, Offset,
    VectorMatchFillValues,
};

use super::{
    CompiledInput, SelectorRequirement, SelectorTiming, compile, compile_selectors,
    compile_statement, validate_inputs,
};

/// Whether a matching names its labels with `on` (`true`) or `ignoring` (`false`), and which.
fn matching_labels(labels: &MatchingLabels) -> (bool, Vec<&str>) {
    match labels {
        MatchingLabels::On(names) => (true, names.iter().map(|name| &**name).collect()),
        MatchingLabels::Ignoring(names) => (false, names.iter().map(|name| &**name).collect()),
    }
}

/// Which cardinality a matching declares, and the labels it copies from the one side.
fn matching_cardinality(cardinality: &Cardinality) -> (&'static str, Vec<&str>) {
    match cardinality {
        Cardinality::OneToOne => ("one-to-one", Vec::new()),
        Cardinality::ManyToOne(names) => {
            ("many-to-one", names.iter().map(|name| &**name).collect())
        }
        Cardinality::OneToMany(names) => {
            ("one-to-many", names.iter().map(|name| &**name).collect())
        }
    }
}

/// The raw series schema a caller binds from the table: one field and no evaluation grid.
fn raw_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::new([FieldSpec {
            name: "value".into(),
        }]),
        eval_grid: None,
    })
}

/// The same layout with `count` fields, for the shapes whose answer depends on how many a scan
/// holds.
fn raw_schema_with_fields(count: usize) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: (0..count)
            .map(|index| FieldSpec {
                name: format!("value_{index}").into(),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    })
}

/// A statement over 100 seconds in 5-second steps, which is 21 grid points.
fn eval_stmt(query: &str) -> EvalStmt {
    eval_stmt_over(query, Duration::from_secs(100))
}

/// The statement `query` over `UNIX_EPOCH..=end`, on a 5s grid.
fn eval_stmt_over(query: &str, end: Duration) -> EvalStmt {
    eval_stmt_between(query, Duration::ZERO, end)
}

/// The statement `query` over `UNIX_EPOCH + start..=UNIX_EPOCH + end`, on a 5s grid.
fn eval_stmt_between(query: &str, start: Duration, end: Duration) -> EvalStmt {
    EvalStmt {
        expr: promql_parser::parser::parse(query).unwrap(),
        start: UNIX_EPOCH + start,
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

/// `query`'s statement with its binary operation's modifier edited by `edit`, for modifiers the
/// parser rewrites or rejects.
fn with_binary_modifier(query: &str, edit: impl FnOnce(&mut BinModifier)) -> EvalStmt {
    let mut stmt = eval_stmt(query);
    let PromExpr::Binary(binary) = &mut stmt.expr else {
        unreachable!("`{query}` parses to a binary operation");
    };
    edit(binary.modifier.get_or_insert_with(BinModifier::default));
    stmt
}

/// `query`'s statement with its call's arguments edited by `edit`, for arities the parser rejects.
fn with_call_arguments(query: &str, edit: impl FnOnce(&mut Vec<Box<PromExpr>>)) -> EvalStmt {
    let mut stmt = eval_stmt(query);
    let PromExpr::Call(call) = &mut stmt.expr else {
        unreachable!("`{query}` parses to a call");
    };
    edit(&mut call.args.args);
    stmt
}

/// The bare selector `x`, for editing into argument positions the parser would type-check.
fn a_bare_selector() -> PromExpr {
    let stmt = eval_stmt("x");
    stmt.expr
}

/// The range selector out of `rate(x[5m])`, for editing into argument positions the parser would
/// type-check.
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

/// The one math operation a `Map` node applies.
fn map_op(node: &NativeNode) -> MathOp {
    let (_, op) = map_parts(node);
    let [MapOp::Math(only)] = &op.ops[..] else {
        panic!("the map holds {:?}, not one math operation", op.ops);
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

/// The two operands and the binding of a `Set` node.
fn set_parts(node: &NativeNode) -> (&NativeNode, &NativeNode, &BoundSetOp) {
    let NativeExpr::Set { left, right, op } = &node.expr else {
        panic!("the node is not a set operation: {:?}", node.expr);
    };
    (left, right, op)
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

/// The input and the operation of an `Aggregate` node.
fn aggregate_parts(node: &NativeNode) -> (&NativeNode, &BoundAggregate) {
    let NativeExpr::Aggregate { input, op } = &node.expr else {
        panic!("the node is not an aggregate: {:?}", node.expr);
    };
    (input, op)
}

/// The grid spec a node's evaluated output carries, as `(start_ms, step_ms, len)`.
fn grid_of(node: &NativeNode) -> (i64, i64, u64) {
    let spec = evaluated(node).eval_grid.as_ref().unwrap().spec();
    (spec.start_ms, spec.step_ms, spec.len)
}

#[test]
fn a_long_statement_grid_is_deferred() {
    let stmt = EvalStmt {
        expr: promql_parser::parser::parse("x").unwrap(),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH + Duration::from_secs(86_400),
        interval: Duration::from_millis(1),
        lookback_delta: Duration::from_secs(5),
    };
    let tree = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime).unwrap();

    assert_eq!(grid_of(tree.root()), (0, 1, 86_400_001));
}

#[test]
fn literal_topk_keeps_the_constant_vector_plan() {
    let tree = compile(
        &eval_stmt("topk(3, x)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    assert!(matches!(
        tree.root().expr,
        NativeExpr::Vector {
            op: promql::native::plan::BoundVectorOp::TopK { k: 3.0, .. },
            ..
        }
    ));
}

#[test]
fn a_rate_over_a_selector_becomes_a_window_over_raw_input() {
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
        ("first_over_time", RangeFunction::FirstOverTime),
        ("mad_over_time", RangeFunction::MadOverTime),
        ("ts_of_first_over_time", RangeFunction::TsOfFirstOverTime),
        ("ts_of_last_over_time", RangeFunction::TsOfLastOverTime),
        ("ts_of_min_over_time", RangeFunction::TsOfMinOverTime),
        ("ts_of_max_over_time", RangeFunction::TsOfMaxOverTime),
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

/// Prometheus answers a quantile outside `[0, 1]` with `-Inf` or `+Inf` rather than an error.
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
        (
            with_call_arguments("clamp(x, 0, 1)", |args| {
                args.pop();
            }),
            "clamp",
            "where it takes its vector and two constants",
        ),
        // The vector counts as the first argument.
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

#[test]
fn a_bare_selector_becomes_an_instant_over_raw_input() {
    let input = raw_schema();
    // Not the default lookback, so a hard-coded default would fail.
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

#[test]
fn an_anchored_instant_selector_binds_its_anchor() {
    let tree = compile(
        &eval_stmt("x @ 100"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    let (_, op) = root_instant(&tree);
    assert_eq!(op.at_ms, Some(100_000));
    assert_eq!(op.offset_ms, 0);
}

#[test]
fn anchored_instant_selector_binds_scan_bounds() {
    for (query, at_ms, offset_ms, bounds) in [
        (
            "x @ 1234 offset 1m",
            Some(1_234_000),
            60_000,
            (1_169_000, 1_174_000, false),
        ),
        ("x @ start()", Some(0), 0, (-5_000, 0, false)),
        ("x @ end()", Some(100_000), 0, (95_000, 100_000, false)),
    ] {
        let compiled = compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap();
        let (_, op) = root_instant(&compiled.tree);
        assert_eq!(op.at_ms, at_ms, "{query}");
        assert_eq!(op.offset_ms, offset_ms, "{query}");
        assert_eq!(
            compiled.inputs[0].requirement.raw_scan_bounds().unwrap(),
            bounds,
            "{query}"
        );
    }
}

#[test]
fn an_anchor_on_a_window_resolves_to_absolute_milliseconds() {
    let cases = [
        ("rate(x[5m] @ 1234)", 1_234_000),
        ("rate(x[5m] @ 1.234)", 1_234),
        // Prometheus allows a pre-epoch anchor.
        ("rate(x[5m] @ -1.5)", -1_500),
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

/// An end of 103s on a 5s grid, so the statement's end and the last grid point (100s) differ.
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

    let spec = op.output_grid.spec();
    let last_point = spec.start_ms + (spec.len as i64 - 1) * spec.step_ms;
    assert_eq!(
        last_point, 100_000,
        "the test's own grid does not stop before the statement end"
    );
}

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

#[test]
fn the_numeric_edges_of_a_statement_are_bound_as_given() {
    enum Expected {
        Grid { step_ms: i64, len: u64 },
        Offset(i64),
        Ordering(SeriesOrdering),
        Rejected(Rejection),
    }

    enum Rejection {
        Plan,
        NotImplemented,
    }

    let cases = [
        // 100s at 7s per step: the last point is 98s.
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
        (
            "a lookback wider than i64 milliseconds",
            EvalStmt {
                lookback_delta: Duration::from_millis(u64::MAX),
                ..eval_stmt("x")
            },
            SeriesOrdering::KeyThenTime,
            Expected::Rejected(Rejection::Plan),
        ),
        (
            "an interleaved ordering, which no shape can consume to its last chunk",
            eval_stmt("rate(x[5m])"),
            SeriesOrdering::Interleaved,
            Expected::Rejected(Rejection::NotImplemented),
        ),
        (
            "a non-default ordering a window can consume",
            eval_stmt("rate(x[5m])"),
            SeriesOrdering::Contiguous,
            Expected::Ordering(SeriesOrdering::Contiguous),
        ),
        // An unsupported shape is refused as `NotImplemented` even with an invalid range.
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

#[test]
fn an_interleaved_scan_is_refused_whatever_the_shape() {
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

#[test]
fn an_unsupported_shape_is_rejected_and_names_what_it_found() {
    let cases = [
        (eval_stmt("x[5m:]"), "x[5m:]", "the expression `x[5m:]`"),
        // A vector selector where a matrix belongs.
        (
            with_call_arguments("rate(x[5m])", |args| {
                *args[0] = a_bare_selector();
            }),
            "rate(x)",
            "whose matrix is `x`",
        ),
    ];

    for (stmt, query, named) in cases {
        let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
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
            "Window: rate [300000ms] offset 0 anchor - on grid 0/5000x21",
            "  Input: raw samples source=0",
        ]
        .join("\n")
    );
}

#[test]
fn the_display_of_a_tree_the_batch_added_to_is_the_query() {
    let cases: [(&str, &[&str]); 3] = [
        (
            "sum by(job)(rate(x[5m]) * 100)",
            &[
                "Aggregate: sum by [job] complete",
                "  Binary: * ignoring [] one-to-one",
                "    Window: rate [300000ms] offset 0 anchor - on grid 0/5000x21",
                "      Input: raw samples source=0",
                "    Scalar: 100",
            ],
        ),
        (
            "-x",
            &[
                "Binary: * ignoring [] one-to-one",
                "  Scalar: -1",
                "  Instant: lookback 5000ms offset 0 on grid 0/5000x21",
                "    Input: raw samples source=0",
            ],
        ),
        (
            "0 < x",
            &[
                "Binary: < ignoring [] one-to-one",
                "  Scalar: 0",
                "  Instant: lookback 5000ms offset 0 on grid 0/5000x21",
                "    Input: raw samples source=0",
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
        ("round(x)", "round 1"),
        ("round(x, 0.5)", "round 0.5"),
        ("clamp(x, 0, 1)", "clamp 0 1"),
        ("clamp_min(x, 0)", "clamp_min 0"),
        ("clamp_max(x, 1)", "clamp_max 1"),
        // Date functions over a vector read its values as Unix seconds.
        ("day_of_month(x)", "day_of_month"),
        ("day_of_week(x)", "day_of_week"),
        ("day_of_year(x)", "day_of_year"),
        ("days_in_month(x)", "days_in_month"),
        ("hour(x)", "hour"),
        ("minute(x)", "minute"),
        ("month(x)", "month"),
        ("year(x)", "year"),
    ];

    for (query, op) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let display = tree.to_string();
        let root = display.lines().next().expect("a compiled tree has a root");
        assert_eq!(
            root,
            format!("Map: {op}"),
            "`{query}` bound the wrong kernel"
        );
    }
}

/// `pi()` folds wherever a literal does: as a statement, as a broadcast operand, and as a kernel
/// constant.
#[test]
fn pi_folds_to_the_number_it_means() {
    let tree = compile_statement(&eval_stmt("pi()"))
        .unwrap_or_else(|error| panic!("`pi()` was refused: {error}"));
    assert_eq!(Some(std::f64::consts::PI), constant(tree.root()));
    assert_eq!((0, 5_000, 21), scalar_grid(tree.root()));

    let tree = compile(
        &eval_stmt("pi() * x"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`pi() * x` was refused: {error}"));
    let (left, right, matching) = binary_parts(tree.root());
    assert_eq!(Some(std::f64::consts::PI), constant(left));
    assert!(matches!(right.expr, NativeExpr::Instant { .. }));
    assert_eq!(BinaryOp::Mul, matching.op);

    // A constant parameter binds a `MathOp`, not a parameterized map.
    let tree = compile(
        &eval_stmt("clamp_max(x, pi())"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`clamp_max(x, pi())` was refused: {error}"));
    let MathOp::ClampMax { max } = map_op(tree.root()) else {
        panic!(
            "the root is not a clamp_max: {:?}",
            map_parts(tree.root()).1.ops
        );
    };
    assert_eq!(std::f64::consts::PI, max);
    let (input, _) = map_parts(tree.root());
    assert!(matches!(input.expr, NativeExpr::Instant { .. }));
}

/// Constant folding uses the kernel's arithmetic; both inputs below are ones a hand-written copy
/// would get wrong.
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

    // As multiplication instead of `^` this would be about 9.8e8.
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
        nearest,
        promql::map::ArithmeticOp::Power.calculate(1.0000006850274261, 9.80019037e8),
        "`1.0000006850274261 ^ 9.80019037e8` did not fold as a power"
    );
}

#[test]
fn a_math_function_over_a_non_vector_argument_is_refused_and_names_it() {
    let cases = [
        (
            with_call_arguments("abs(x)", |args| {
                *args[0] = PromExpr::NumberLiteral(NumberLiteral::new(1.0));
            }),
            "a call to `abs` whose first argument is `1`, which is a scalar",
        ),
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
}

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

/// Two vectors bind independently, while the compatibility entry refuses to hide the two inputs.
#[test]
fn a_two_vector_operation_binds_one_input_per_selector() {
    let compiled = compile_selectors(
        &eval_stmt("a + b"),
        &|_| Ok(raw_schema()),
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .unwrap_or_else(|error| panic!("`a + b` was refused: {error}"));
    assert_eq!(compiled.inputs.len(), 2);
    assert_eq!(compiled.inputs[0].requirement.metric.as_deref(), Some("a"));
    assert_eq!(compiled.inputs[1].requirement.metric.as_deref(), Some("b"));
    assert!(matches!(
        compiled.tree.root().expr,
        NativeExpr::Binary { .. }
    ));

    let error = compile(
        &eval_stmt("a + b"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect_err("`a + b` requires separately bound selector inputs");
    assert!(matches!(error, DataFusionError::NotImplemented(_)));
    assert!(
        error.to_string().contains("use `compile_selectors`"),
        "{error}"
    );
}

#[test]
fn vector_binary_matching_binds_labels_cardinality_fills_and_bool() {
    for (query, on, labels, card, copies, lhs, rhs, bool_result) in [
        (
            "a + on(job) b",
            true,
            vec!["job"],
            "one-to-one",
            vec![],
            None,
            None,
            false,
        ),
        (
            "a / ignoring(zone) b",
            false,
            vec!["zone"],
            "one-to-one",
            vec![],
            None,
            None,
            false,
        ),
        (
            "a + on(job) group_left(zone) b",
            true,
            vec!["job"],
            "many-to-one",
            vec!["zone"],
            None,
            None,
            false,
        ),
        (
            "a - on(job) group_right(zone) b",
            true,
            vec!["job"],
            "one-to-many",
            vec!["zone"],
            None,
            None,
            false,
        ),
        (
            "a + fill_left(0) b",
            false,
            vec![],
            "one-to-one",
            vec![],
            Some(0.0),
            None,
            false,
        ),
        (
            "a + fill_right(1.5) b",
            false,
            vec![],
            "one-to-one",
            vec![],
            None,
            Some(1.5),
            false,
        ),
        (
            "a + fill(2) b",
            false,
            vec![],
            "one-to-one",
            vec![],
            Some(2.0),
            Some(2.0),
            false,
        ),
        (
            "a > bool ignoring(zone) b",
            false,
            vec!["zone"],
            "one-to-one",
            vec![],
            None,
            None,
            true,
        ),
    ] {
        let compiled = compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (_, _, matching) = binary_parts(compiled.tree.root());
        assert_eq!(matching_labels(&matching.labels), (on, labels), "{query}");
        assert_eq!(
            matching_cardinality(&matching.cardinality),
            (card, copies),
            "{query}"
        );
        assert_eq!(
            (matching.fill.lhs, matching.fill.rhs),
            (lhs, rhs),
            "{query}"
        );
        assert_eq!(matching.return_bool, bool_result, "{query}");
    }
}

#[test]
fn vector_binary_matching_keeps_ignoring_and_group_left_separate() {
    let compiled = compile_selectors(
        &eval_stmt("a / ignoring(instance) group_left(host) b"),
        &|_| Ok(raw_schema()),
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .unwrap();
    let (_, _, matching) = binary_parts(compiled.tree.root());
    assert_eq!(matching_labels(&matching.labels), (false, vec!["instance"]));
    assert_eq!(
        matching_cardinality(&matching.cardinality),
        ("many-to-one", vec!["host"])
    );
}

#[test]
fn set_operators_bind_their_operator_and_labels() {
    for (query, op, on, labels) in [
        ("a and b", SetOp::And, false, vec![]),
        ("a or on(job) b", SetOp::Or, true, vec!["job"]),
        (
            "a unless ignoring(pod) b",
            SetOp::Unless,
            false,
            vec!["pod"],
        ),
    ] {
        let compiled = compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        assert_eq!(compiled.inputs.len(), 2, "{query}");
        let (left, right, bound) = set_parts(compiled.tree.root());
        assert_eq!(bound.op, op, "{query}");
        assert_eq!(matching_labels(&bound.labels), (on, labels), "{query}");
        assert!(matches!(left.expr, NativeExpr::Instant { .. }), "{query}");
        assert!(matches!(right.expr, NativeExpr::Instant { .. }), "{query}");
    }

    let compiled = compile_selectors(
        &eval_stmt("abs(rate(x[5m]) and on(job) sum by (job) (up)) * 2"),
        &|_| Ok(raw_schema()),
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .unwrap();
    let (vector, _, _) = binary_parts(compiled.tree.root());
    let NativeExpr::Map { input, .. } = &vector.expr else {
        panic!(
            "the multiplication does not read the `abs`: {:?}",
            vector.expr
        );
    };
    let (left, right, _) = set_parts(input);
    assert!(
        matches!(left.expr, NativeExpr::Window { .. }),
        "{:?}",
        left.expr
    );
    assert!(
        matches!(right.expr, NativeExpr::Aggregate { .. }),
        "{:?}",
        right.expr
    );
}

/// Prometheus refuses a fill value on a set operator, which this parser accepts.
#[test]
fn a_set_operation_refuses_a_fill_value_and_several_fields() {
    let error = compile_selectors(
        &with_binary_modifier("a and b", |modifier| {
            modifier.fill_values = VectorMatchFillValues::new(0.0, 0.0);
        }),
        &|_| Ok(raw_schema()),
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .expect_err("a fill value on `and` compiled");
    assert!(
        matches!(&error, DataFusionError::NotImplemented(message) if message.contains("no fill")),
        "{error}"
    );

    for (query, fields) in [("a or b", (2, 1)), ("a and b", (2, 2))] {
        let error = compile_selectors(
            &eval_stmt(query),
            &|requirement| {
                Ok(match requirement.metric.as_deref() {
                    Some("a") => raw_schema_with_fields(fields.0),
                    _ => raw_schema_with_fields(fields.1),
                })
            },
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .expect_err(&format!("`{query}` over {fields:?} fields compiled"));
        let expected = format!("{} and {} fields", fields.0, fields.1);
        assert!(
            matches!(&error, DataFusionError::NotImplemented(message) if message.contains(&expected)),
            "{query}: {error}"
        );
    }
}

#[test]
fn info_binds_its_info_metric_after_the_base() {
    for (query, metric, matchers, names, keep_unmatched) in [
        ("info(rate(x[5m]))", "target_info", vec![], vec![], true),
        (
            "info(rate(x[5m]), {__name__=\"build_info\", version=~\"v.+\", zone=~\".*\"})",
            "build_info",
            vec!["version", "zone"],
            vec!["build_info"],
            false,
        ),
    ] {
        let compiled = compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let [base_input, info_input] = &compiled.inputs[..] else {
            panic!("{query}: {} inputs", compiled.inputs.len());
        };
        assert_eq!(
            base_input.requirement.metric.as_deref(),
            Some("x"),
            "{query}"
        );
        let requirement = &info_input.requirement;
        assert_eq!(requirement.metric.as_deref(), Some(metric), "{query}");
        assert_eq!(
            requirement
                .matchers
                .matchers
                .iter()
                .map(|matcher| matcher.name.as_str())
                .collect::<Vec<_>>(),
            matchers,
            "{query}"
        );
        assert_eq!(
            requirement.timing,
            SelectorTiming::Instant { lookback_ms: 5_000 },
            "{query}"
        );
        let NativeExpr::Info {
            base,
            info,
            selector,
        } = &compiled.tree.root().expr
        else {
            panic!(
                "{query}: the root is not info: {:?}",
                compiled.tree.root().expr
            );
        };
        assert!(matches!(base.expr, NativeExpr::Window { .. }), "{query}");
        let NativeExpr::Instant { input, op } = &info.expr else {
            panic!("{query}: the info side is not a selection: {:?}", info.expr);
        };
        assert_eq!(op.value, InstantValue::InfoTimestamp, "{query}");
        assert!(
            matches!(&input.expr, NativeExpr::Input(bound) if bound.source_index == 1),
            "{query}"
        );
        assert_eq!(
            selector
                .name_matchers
                .iter()
                .map(|(op, value)| (*op, &**value))
                .collect::<Vec<_>>(),
            names
                .into_iter()
                .map(|name| (MatchOp::Equal, name))
                .collect::<Vec<_>>(),
            "{query}"
        );
        assert_eq!(
            selector
                .data_labels
                .iter()
                .map(|label| &**label)
                .collect::<Vec<_>>(),
            matchers,
            "{query}"
        );
        assert_eq!(selector.keep_unmatched, keep_unmatched, "{query}");
    }
}

/// Follows Prometheus `infoSeriesSelectTimestampAndOffset`.
#[test]
fn info_selects_its_info_series_at_the_shared_reference_time() {
    for (query, offset_ms, at_ms) in [
        ("info(x offset 1m)", 60_000, None),
        ("info(rate(x[1m] @ 100 offset 5s))", 5_000, Some(100_000)),
        (
            "info(x @ 50 offset 10s + on(job) y @ 60 offset 20s)",
            10_000,
            Some(50_000),
        ),
        ("info(x offset 1m + on(job) y)", 0, None),
        ("info(x @ 50 + on(job) y)", 0, None),
        ("info(rate(x[1m:10s] offset 1m))", 60_000, None),
        (
            "info(rate(x[1m:10s] @ 100 offset 1m))",
            60_000,
            Some(100_000),
        ),
        (
            "info(max_over_time(rate(x[1m] @ 30 offset 2s)[1m:10s] offset 1m))",
            2_000,
            Some(30_000),
        ),
    ] {
        let compiled = compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let info = &compiled.inputs.last().unwrap().requirement;
        assert_eq!(info.metric.as_deref(), Some("target_info"), "{query}");
        assert_eq!((info.offset_ms, info.at_ms), (offset_ms, at_ms), "{query}");
    }
}

#[test]
fn info_refuses_info_metrics_it_cannot_read_as_one_table() {
    for (query, fields, expected) in [
        ("info(x, {__name__=~\".+_info\"})", 1, "several tables"),
        (
            "info(x, {__name__!=\"build_info\", version=\"v1\"})",
            1,
            "several tables",
        ),
        ("info(x, {__field__=\"v\"})", 1, "`__field__`"),
        ("info(x)", 2, "with 2 fields"),
    ] {
        let error = compile_selectors(
            &eval_stmt(query),
            &|requirement| {
                Ok(match requirement.metric.as_deref() {
                    Some("x") => raw_schema(),
                    _ => raw_schema_with_fields(fields),
                })
            },
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .expect_err(&format!("`{query}` compiled"));
        assert!(
            matches!(&error, DataFusionError::NotImplemented(message) if message.contains(expected)),
            "{query}: {error}"
        );
    }
}

#[test]
fn a_statement_without_values_on_a_grid_is_refused() {
    for (query, found) in [
        ("x[5m]", "which is a matrix"),
        ("\"a\"", "which is a string"),
    ] {
        let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains(found),
            "`{query}` did not name the type it found: {message}"
        );
    }
}

#[test]
fn a_constant_statement_is_a_scalar_on_the_statement_grid_without_input() {
    for (query, value) in [("1 + 2", 3.0), ("-1", -1.0)] {
        let stmt = eval_stmt(query);
        let tree = compile_statement(&stmt)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        assert_eq!(Some(value), constant(tree.root()), "query: {query}");
        assert_eq!((0, 5_000, 21), scalar_grid(tree.root()), "query: {query}");
    }
}

#[test]
fn a_statement_that_reads_no_data_compiles_without_input() {
    let tree = compile_statement(&eval_stmt("time()")).unwrap();
    assert!(matches!(
        tree.root().expr,
        NativeExpr::EvalTime { date: None }
    ));
    assert_eq!((0, 5_000, 21), scalar_grid(tree.root()));

    let tree = compile_statement(&eval_stmt("time() - 60")).unwrap();
    let (left, right, matching) = binary_parts(tree.root());
    assert!(matches!(left.expr, NativeExpr::EvalTime { date: None }));
    assert_eq!(constant(right), Some(60.0));
    assert_eq!(matching.op, BinaryOp::Sub);
    assert!(matches!(tree.root().output, NativeType::Scalar(_)));

    let tree = compile_statement(&eval_stmt("vector(1)")).unwrap();
    let NativeExpr::VectorOf { input } = &tree.root().expr else {
        panic!("`vector(1)` is a vector over its scalar: {tree}");
    };
    assert_eq!(constant(input), Some(1.0));
    assert_eq!(evaluated(tree.root()).fields.len(), 1);

    let tree = compile_statement(&eval_stmt("hour()")).unwrap();
    let NativeExpr::VectorOf { input } = &tree.root().expr else {
        panic!("`hour()` is a vector over the evaluation time: {tree}");
    };
    assert!(matches!(
        input.expr,
        NativeExpr::EvalTime {
            date: Some(DateFunction::Hour)
        }
    ));

    for (query, found) in [("x", "a selector"), ("step()", "a call to `step`")] {
        let error = compile_statement(&eval_stmt(query))
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        assert!(error.to_string().contains(found), "`{query}`: {error}");
    }
}

#[test]
fn a_varying_scalar_is_broadcast_where_it_was_written() {
    let compile_tree = |query: &str| {
        compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"))
        .tree
    };
    let tree = compile_tree("x > bool time()");
    let (left, right, matching) = binary_parts(tree.root());
    assert!(matches!(left.expr, NativeExpr::Instant { .. }));
    assert!(matches!(right.expr, NativeExpr::EvalTime { date: None }));
    assert!(matching.return_bool);
    assert!(matches!(tree.root().output, NativeType::Samples(_)));

    let tree = compile_tree("time() - timestamp(x)");
    let (left, right, matching) = binary_parts(tree.root());
    assert!(matches!(left.expr, NativeExpr::EvalTime { date: None }));
    assert!(matches!(right.expr, NativeExpr::Instant { .. }));
    assert_eq!(matching.op, BinaryOp::Sub);
}

#[test]
fn a_math_function_with_a_varying_parameter_binds_its_parameters_as_scalars() {
    let compiled = |query: &str| {
        compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"))
    };
    let parts = |tree: &NativeTree| match &tree.root().expr {
        NativeExpr::ParameterizedMap {
            input,
            op,
            parameters,
        } => (input.as_ref().clone(), *op, parameters.clone()),
        other => panic!("expected a parameterized map, got {other:?}"),
    };

    let tree = compiled("round(x, time() / 10)").tree;
    let (input, op, parameters) = parts(&tree);
    assert_eq!(op, ParameterizedOp::Round);
    assert!(matches!(input.expr, NativeExpr::Instant { .. }));
    assert!(matches!(
        parameters.as_slice(),
        [NativeNode {
            expr: NativeExpr::Binary { .. },
            ..
        }]
    ));

    let tree = compiled("clamp(x, 0, time())").tree;
    let (_, op, parameters) = parts(&tree);
    assert_eq!(op, ParameterizedOp::Clamp);
    assert_eq!(constant(&parameters[0]), Some(0.0));
    assert!(matches!(
        parameters[1].expr,
        NativeExpr::EvalTime { date: None }
    ));

    let scalar_tree =
        compile_statement(&eval_stmt("topk(scalar(vector(time())), vector(time()))")).unwrap();
    let NativeExpr::ParameterizedTopK { parameter, .. } = &scalar_tree.root().expr else {
        panic!("expected parameterized topk")
    };
    assert!(matches!(parameter.expr, NativeExpr::ScalarOf { .. }));

    let tree = compiled("round(x, 2 * 5)").tree;
    assert!(matches!(
        map_op(tree.root()),
        MathOp::Round { nearest } if nearest == 10.0
    ));
}

/// Prometheus reads a `quantile` φ at every step.
#[test]
fn a_quantile_reads_its_argument_per_point() {
    let compiled = |query: &str| {
        compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
    };
    let per_point = |query: &str| match &compiled(query)
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"))
        .tree
        .root()
        .expr
    {
        NativeExpr::ParameterizedAggregate { .. } => {}
        other => panic!("`{query}`: expected a parameterized aggregate, got {other:?}"),
    };
    let tree = compiled("quantile(scalar(x), y)").unwrap().tree;
    let NativeExpr::ParameterizedAggregate {
        input,
        op,
        parameter,
    } = &tree.root().expr
    else {
        panic!(
            "expected a parameterized aggregate, got {:?}",
            tree.root().expr
        );
    };
    assert!(
        matches!(op.kind, AggregateKind::Quantile { .. }),
        "{:?}",
        op.kind
    );
    assert_eq!(op.stage, AggregateStage::Complete);
    // Input before parameter, the order the planner's selector walk prepares scans in.
    assert!(matches!(input.expr, NativeExpr::Instant { .. }));
    assert!(matches!(parameter.expr, NativeExpr::ScalarOf { .. }));
    assert!(matches!(tree.root().output, NativeType::Samples(_)));
    assert_eq!(tree.root().inputs().len(), 2);
    per_point("quantile(time() / 150 + 0.5, y)");
    per_point("quantile(scalar(x), y) by (tag_0)");

    let quantile = 0.25_f64 + 0.1 + 0.15;
    let tree = compiled("quantile(0.25 + 0.1 + 0.15, y)").unwrap().tree;
    let NativeExpr::Aggregate { op, .. } = &tree.root().expr else {
        panic!("the constant argument left the constant path");
    };
    assert!(
        matches!(op.kind, AggregateKind::Quantile { quantile: value } if value.to_bits() == quantile.to_bits()),
        "{:?}",
        op.kind
    );

    assert!(
        promql_parser::parser::parse("quantile(x, y)").is_err(),
        "a vector argument parsed"
    );
}

#[test]
fn a_call_of_two_scalars_binds_them_as_a_scalar_map() {
    let compiled = |query: &str| {
        compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            None,
        )
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"))
    };
    let parts = |tree: &NativeTree| match &tree.root().expr {
        NativeExpr::ParameterizedMap {
            input,
            op,
            parameters,
        } => (input.as_ref().clone(), *op, parameters.clone()),
        other => panic!("expected a parameterized map, got {other:?}"),
    };

    let tree = compiled("max_of(time(), 2)").tree;
    assert!(
        matches!(tree.root().output, NativeType::Scalar(_)),
        "the answer is a scalar, not a vector"
    );
    let (input, op, parameters) = parts(&tree);
    assert_eq!(op, ParameterizedOp::MaxOf);
    assert!(matches!(input.expr, NativeExpr::EvalTime { date: None }));
    assert_eq!(constant(&parameters[0]), Some(2.0));

    let tree = compiled("min_of(2, scalar(x))").tree;
    let (input, op, parameters) = parts(&tree);
    assert_eq!(op, ParameterizedOp::MinOf);
    assert_eq!(constant(&input), Some(2.0));
    assert!(matches!(
        parameters.as_slice(),
        [NativeNode {
            expr: NativeExpr::ScalarOf { .. },
            ..
        }]
    ));

    // `max_of` is not constant-folded.
    let tree = compiled("max_of(1, 2)").tree;
    let (input, op, parameters) = parts(&tree);
    assert_eq!(op, ParameterizedOp::MaxOf);
    assert_eq!(constant(&input), Some(1.0));
    assert_eq!(constant(&parameters[0]), Some(2.0));
}

#[test]
fn a_scalar_function_over_a_vector_is_refused() {
    let stmt = with_call_arguments("max_of(1, 2)", |args| {
        *args[0] = a_bare_selector();
    });
    let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
        .expect_err("`max_of(x, 2)` compiled, and it should not have");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "the refusal did not report NotImplemented, so the statement would not fall back: {error}"
    );
    let message = error.to_string();
    assert!(message.contains("a call to `max_of`"), "{message}");
    assert!(
        message.contains("where the function takes scalars"),
        "the refusal did not say what the function takes: {message}"
    );
}

#[test]
fn a_scalar_function_with_one_argument_is_refused() {
    let stmt = with_call_arguments("max_of(1, 2)", |args| {
        args.pop();
    });
    let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
        .expect_err("`max_of(1)` compiled, and it should not have");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "the refusal did not report NotImplemented: {error}"
    );
    let message = error.to_string();
    assert!(message.contains("a call to `max_of`"), "{message}");
    assert!(
        message.contains("where it takes two scalars"),
        "the refusal did not name the arguments it takes: {message}"
    );
}

#[test]
fn a_vector_matching_modifier_over_a_constant_is_refused() {
    let cases = [
        (eval_stmt("x + ignoring() 1"), "x + ignoring() 1"),
        (eval_stmt("x + on() group_left 1"), "x + on() group_left 1"),
        (
            eval_stmt("x + on() group_right 1"),
            "x + on() group_right 1",
        ),
        // The parser accepts a fill only between two vectors.
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

#[test]
fn a_modifier_inside_a_folded_call_argument_is_refused() {
    let cases = [
        (
            "quantile_over_time(0.5 + fill(0) 0.1, x[5m])",
            "first argument",
        ),
        ("predict_linear(x[5m], 1 + fill(0) 2)", "second argument"),
        (
            "double_exponential_smoothing(x[5m], 0.5, 1 + fill(0) 2)",
            "third argument",
        ),
    ];
    for (query, position) in cases {
        let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains(position),
            "`{query}` blamed the wrong argument: {message}"
        );
        assert!(
            message.contains("fill"),
            "`{query}` did not name the fill it found: {message}"
        );
    }
}

/// Unfolded, a `Binary` of two constants passes tree validation and fails only at execution.
#[test]
fn a_bool_comparison_of_two_constants_folds_to_its_number() {
    for (query, folded) in [
        ("x * (1 < bool 2)", 1.0),
        ("x * (1 > bool 2)", 0.0),
        ("x * (1 == bool 1)", 1.0),
        ("x * (1 != bool 1)", 0.0),
        // NaN satisfies only `!=`.
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

/// The parser refuses `1 > 2`, so the `bool` flag is cleared on a parsed tree.
#[test]
fn a_comparison_of_two_constants_without_bool_is_refused() {
    let mut stmt = eval_stmt("x * (1 > bool 2)");
    let PromExpr::Binary(outer) = &mut stmt.expr else {
        unreachable!("`x * (1 > bool 2)` parses to a binary operation");
    };
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
        message.contains("between two scalars"),
        "the comparison did not say what it found: {message}"
    );
}

/// Folding `x * (1 + fill(0) 2)` to `x * 3` would silently drop the fill.
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

    compile(
        &eval_stmt("x * (1 + 2)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect("`x * (1 + 2)` has no modifier to refuse");
}

/// The labels of a `by` or `without` modifier, in the order they were written.
fn grouping_labels(op: &BoundAggregate) -> Vec<String> {
    let labels = match &op.group {
        Grouping::By(labels) | Grouping::Without(labels) => labels,
    };
    labels.iter().map(|label| label.to_string()).collect()
}

/// Asserts that `op`'s grouping names exactly `expected`, in the order it was written.
fn assert_grouping_labels(op: &BoundAggregate, expected: &[&str]) {
    let expected: Vec<String> = expected.iter().map(|label| label.to_string()).collect();
    assert_eq!(
        grouping_labels(op),
        expected,
        "the grouping names the wrong labels"
    );
}

#[test]
fn a_bare_sum_groups_by_no_label() {
    let tree = compile(
        &eval_stmt("sum(x)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`sum(x)` was refused: {error}"));

    let (input, op) = aggregate_parts(tree.root());
    assert_eq!(op.kind, AggregateKind::Sum);
    assert_eq!(op.stage, AggregateStage::Complete);
    let Grouping::By(labels) = &op.group else {
        panic!("a bare `sum` grouped without labels: {:?}", op.group);
    };
    assert!(labels.is_empty(), "a bare `sum` kept the labels {labels:?}");

    assert_eq!(evaluated(tree.root()).fields.len(), 1);
    assert_eq!(grid_of(tree.root()), (0, 5_000, 21));
    assert_eq!(
        grid_of(input),
        (0, 5_000, 21),
        "the aggregate's input is not on the statement's grid"
    );
}

#[test]
fn a_sum_by_keeps_the_labels_it_names() {
    let cases = [
        ("sum by() (x)", &[][..]),
        ("sum by(job)(x)", &["job"][..]),
        ("sum by(job, instance)(x)", &["job", "instance"][..]),
        ("sum(x) by (job)", &["job"][..]),
    ];

    for (query, expected) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (_, op) = aggregate_parts(tree.root());
        assert!(
            matches!(&op.group, Grouping::By(_)),
            "`{query}` did not group by labels: {:?}",
            op.group
        );
        assert_grouping_labels(op, expected);
    }
}

#[test]
fn a_sum_without_records_the_labels_to_drop() {
    let cases = [
        ("sum without(instance)(x)", &["instance"][..]),
        ("sum without(instance, job)(x)", &["instance", "job"][..]),
        ("sum(x) without (instance)", &["instance"][..]),
    ];

    for (query, expected) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (_, op) = aggregate_parts(tree.root());
        assert!(
            matches!(&op.group, Grouping::Without(_)),
            "`{query}` did not group without labels: {:?}",
            op.group
        );
        assert_grouping_labels(op, expected);
    }
}

#[test]
fn the_supported_aggregates_map_to_their_kinds() {
    let cases = [
        ("sum(x)", AggregateKind::Sum),
        ("avg(x)", AggregateKind::Avg),
        ("count(x)", AggregateKind::Count),
        ("min(x)", AggregateKind::Min),
        ("max(x)", AggregateKind::Max),
        ("group(x)", AggregateKind::Group),
        ("stddev(x)", AggregateKind::Stddev),
        ("stdvar(x)", AggregateKind::Stdvar),
        (
            "quantile(0.9, x)",
            AggregateKind::Quantile { quantile: 0.9 },
        ),
    ];

    for (query, kind) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (_, op) = aggregate_parts(tree.root());
        assert_eq!(op.kind, kind, "`{query}` produced the wrong kind");
        assert_eq!(
            op.stage,
            AggregateStage::Complete,
            "`{query}` is not a complete aggregate"
        );
    }
}

#[test]
fn a_quantile_aggregate_binds_the_quantile_it_was_written_with() {
    let cases = [
        ("quantile(0.9, x)", 0.9),
        ("quantile(1 - 0.1, x)", 0.9),
        ("quantile((0.9), x)", 0.9),
        ("quantile(0, x)", 0.0),
        ("quantile(1, x)", 1.0),
        ("quantile(-1.0, x)", -1.0),
        ("quantile(2.0, x)", 2.0),
    ];

    for (query, quantile) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (_, op) = aggregate_parts(tree.root());
        assert_eq!(
            op.kind,
            AggregateKind::Quantile { quantile },
            "`{query}` did not bind the quantile it wrote"
        );
        assert_eq!(
            op.stage,
            AggregateStage::Complete,
            "`{query}` is not a complete aggregate"
        );
    }
}

/// The parser refuses a vector φ; the compiler refuses one on an edited tree too.
#[test]
fn an_aggregate_quantile_over_a_vector_is_refused_and_names_the_argument() {
    let query = "quantile(x, y)";
    let error = promql_parser::parser::parse(query)
        .expect_err(&format!("`{query}` parsed, and it should not have"));
    assert!(
        error.contains("expected type scalar"),
        "`{query}` was refused by something other than the argument's type: {error}"
    );

    let tree = compile(
        &eval_stmt("quantile(time(), x)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect("a scalar argument is a per-point `φ`");
    assert!(matches!(
        tree.root().expr,
        NativeExpr::ParameterizedAggregate { .. }
    ));

    let PromExpr::VectorSelector(selector) = promql_parser::parser::parse("x").unwrap() else {
        unreachable!("`x` is a selector")
    };
    let PromExpr::Aggregate(mut aggregate) =
        promql_parser::parser::parse("quantile(0.5, x)").unwrap()
    else {
        unreachable!("`quantile(0.5, x)` is an aggregate")
    };
    aggregate.param = Some(Box::new(PromExpr::VectorSelector(selector)));
    let stmt = EvalStmt {
        expr: PromExpr::Aggregate(aggregate),
        ..eval_stmt("quantile(0.5, x)")
    };
    let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
        .expect_err("a vector argument compiled");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "a vector argument did not report NotImplemented: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("whose first argument is `x`"),
        "the refusal did not name the argument it found: {message}"
    );
    assert!(
        message.contains("number literal"),
        "the refusal did not say what the argument has to be: {message}"
    );
}

#[test]
fn a_fill_inside_an_aggregate_quantile_is_refused_where_the_fill_is() {
    let query = "quantile(0.5 + fill(0) 0.1, x)";
    let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
        .expect_err(&format!("`{query}` compiled, and it should not have"));
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "`{query}` did not report NotImplemented: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("0.5 + fill (0) 0.1"),
        "`{query}` did not name the argument it found: {message}"
    );
    assert!(
        message.contains("fill"),
        "`{query}` did not name the fill it found: {message}"
    );
}

#[test]
fn the_aggregates_that_are_not_a_fold_bind_a_vector_operation() {
    let cases = [
        ("topk(1 + 2, x)", "topk 3 by []"),
        ("bottomk by(job) (3, x)", "bottomk 3 by [job]"),
        (
            "limitk(2, rate(x[5m])) without(instance)",
            "limitk 2 without [instance]",
        ),
        ("limit_ratio by(job) (0.5, x)", "limit_ratio 0.5"),
        (
            r#"count_values without(job) ("v", x)"#,
            r#"count_values "v" without [job]"#,
        ),
    ];
    for (query, op) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let display = tree.to_string();
        let root = display.lines().next().expect("a compiled tree has a root");
        assert_eq!(root, format!("Vector: {op}"), "`{query}`");
    }

    let compiled = compile_selectors(
        &eval_stmt("topk(scalar(x), x)"),
        &|_| Ok(raw_schema()),
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .expect("scalar(selector) is now compiled as a dynamic k");
    let NativeExpr::ParameterizedTopK { parameter, .. } = &compiled.tree.root().expr else {
        panic!("expected parameterized topk")
    };
    assert!(matches!(parameter.expr, NativeExpr::ScalarOf { .. }));

    // Only `topk`/`bottomk` take a per-point `k`.
    let error = compile(
        &eval_stmt("limitk(scalar(x), x)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect_err("a run-time k compiled");
    let message = error.to_string();
    assert!(message.contains("first argument"), "{message}");
    // The rejection's quoted shapes must name every accepted aggregator.
    for aggregator in [
        "sum",
        "avg",
        "count",
        "min",
        "max",
        "group",
        "stddev",
        "stdvar",
        "quantile",
        "topk",
        "bottomk",
        "limitk",
        "limit_ratio",
        "count_values",
    ] {
        assert!(
            message.contains(&format!("`{aggregator}")),
            "the rejection does not quote `{aggregator}`: {message}"
        );
    }
}

/// Compound queries compile and land on the statement's grid; only the grid is asserted.
#[test]
fn the_batch_acceptance_queries_compile() {
    let cases = [
        // Recursion plus a constant on the left.
        "2 / (x + 1)",
        // Filter against bool comparison, in both directions.
        "x > 0",
        "0 < x",
        "x > bool 0",
        // Window, scalar arithmetic and aggregation in one tree.
        "sum by(job)(rate(x[5m]) * 100)",
        // Grouping over a pointwise function.
        "sum without(instance)(abs(x))",
        // A negation.
        "-x",
    ];

    for query in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        assert_eq!(
            grid_of(tree.root()),
            (0, 5_000, 21),
            "`{query}` does not land on the statement's grid"
        );
    }
}

/// One raw chunk of the metric `x`; a `None` value is a missing sample at that timestamp.
fn raw_x(timestamps: &[i64], values: Vec<Option<f64>>, is_last: bool) -> RawSeriesChunk {
    let metric = SeriesLabels::try_new(Some("x".into()), vec![]).expect("a metric name");
    RawSeriesChunk::try_new(
        &raw_schema(),
        Buffer::from_slice_ref(b"series-a"),
        Some(metric),
        RawTimestamps::try_new(ScalarBuffer::from(timestamps.to_vec())).expect("timestamps"),
        SeriesFields::one(Samples::from_float(Float64Array::from(values))),
        is_last,
    )
    .expect("a raw chunk")
}

/// `x`'s samples at every grid point of [`eval_stmt`]'s grid, holding `1..=21`.
fn samples_at_every_grid_point() -> (Vec<i64>, Vec<Option<f64>>) {
    let timestamps = (0..21).map(|index| index * 5_000).collect();
    let values = (1..=21).map(|value| Some(f64::from(value))).collect();
    (timestamps, values)
}

/// Compiles `query`, runs it over one raw series pushed as `chunks`, and returns every chunk the
/// root handed over.
fn execute_compiled(query: &str, chunks: Vec<RawSeriesChunk>) -> Vec<EvaluatedChunk> {
    let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let mut execution = NativeExecutor::new(
        NativeExecutionState::try_new(&tree, pool, None)
            .unwrap_or_else(|error| panic!("`{query}` was not executable: {error}")),
    );
    for chunk in chunks {
        execution
            .push_raw(InputId::new(0), chunk)
            .unwrap_or_else(|error| panic!("`{query}` refused its input: {error}"));
    }
    let mut out = Vec::new();
    while let Some(chunk) = execution
        .next_chunk()
        .unwrap_or_else(|error| panic!("`{query}` failed while running: {error}"))
    {
        out.push(chunk);
    }
    out.extend(
        execution
            .finish()
            .unwrap_or_else(|error| panic!("`{query}` failed while closing: {error}")),
    );
    out
}

/// The `(grid position, value)` rows `chunks` hold, in arrival order; empty positions are skipped.
fn rows(chunks: &[EvaluatedChunk]) -> Vec<(usize, f64)> {
    let mut rows = Vec::new();
    for chunk in chunks {
        let mut field = chunk.series.fields.as_slice()[0].iter();
        for (position, _) in chunk.series.time.positions() {
            match field.next().expect("one sample per row") {
                SampleRef::Float(value) => rows.push((position, value)),
                SampleRef::Missing => {}
                SampleRef::Histogram(_) => panic!("a scalar broadcast produced a histogram"),
            }
        }
    }
    rows
}

/// The label set the executed chunks carry, or `None` when they carry the empty one.
fn executed_name(chunks: &[EvaluatedChunk]) -> Option<String> {
    chunks
        .first()
        .and_then(|chunk| chunk.series.metric.name().map(|name| name.to_string()))
}

#[test]
fn a_compiled_constant_multiplies_the_vector_it_is_paired_with() {
    let (timestamps, values) = samples_at_every_grid_point();
    let out = execute_compiled("x * 100", vec![raw_x(&timestamps, values, true)]);

    let expected: Vec<(usize, f64)> = (0..21)
        .map(|index| (index, (index as f64 + 1.0) * 100.0))
        .collect();
    assert_eq!(
        rows(&out),
        expected,
        "`x * 100` did not multiply every value"
    );
    // Arithmetic drops the metric name, as Prometheus does.
    assert_eq!(executed_name(&out), None, "`x * 100` kept the metric name");
}

#[test]
fn a_compiled_constant_on_the_left_keeps_the_operand_order() {
    let (timestamps, values) = samples_at_every_grid_point();
    let divided = rows(&execute_compiled(
        "100 / x",
        vec![raw_x(&timestamps, values.clone(), true)],
    ));
    let expected: Vec<(usize, f64)> = (0..21)
        .map(|index| (index, 100.0 / (index as f64 + 1.0)))
        .collect();
    assert_eq!(divided, expected, "`100 / x` did not divide by the vector");

    let other = rows(&execute_compiled(
        "x / 100",
        vec![raw_x(&timestamps, values, true)],
    ));
    assert_ne!(divided, other, "`100 / x` and `x / 100` agree");
}

#[test]
fn a_compiled_bool_comparison_reports_one_and_zero() {
    let (timestamps, _) = samples_at_every_grid_point();
    // Half the values are negative, so the comparison decides both ways.
    let values = (1..=21)
        .map(|value| Some(f64::from(value) - 10.0))
        .collect();
    let out = execute_compiled("x > bool 0", vec![raw_x(&timestamps, values, true)]);

    let expected: Vec<(usize, f64)> = (0..21)
        .map(|index| {
            let value = index as f64 + 1.0 - 10.0;
            (index, f64::from(u8::from(value > 0.0)))
        })
        .collect();
    assert_eq!(rows(&out), expected, "`x > bool 0` did not report 0/1");
    assert_eq!(
        executed_name(&out),
        None,
        "`x > bool 0` kept the metric name"
    );
}

/// Constant comparisons fold before the executor runs the vector operation.
#[test]
fn a_compiled_bool_constant_comparison_runs_with_vector_arithmetic() {
    let (timestamps, _) = samples_at_every_grid_point();
    let values: Vec<Option<f64>> = [-2.0, 0.0, 2.0]
        .into_iter()
        .cycle()
        .take(timestamps.len())
        .map(Some)
        .collect();
    let cases = [
        ("x * (1 < bool 2)", [-2.0, 0.0, 2.0]),
        ("x * (1 > bool 2)", [0.0, 0.0, 0.0]),
        ("-(1 < bool 2) + x", [-3.0, -1.0, 1.0]),
        ("x > bool (1 == bool 1)", [0.0, 0.0, 1.0]),
    ];

    for (query, pattern) in cases {
        let out = execute_compiled(query, vec![raw_x(&timestamps, values.clone(), true)]);
        let expected: Vec<(usize, f64)> = pattern
            .into_iter()
            .cycle()
            .take(timestamps.len())
            .enumerate()
            .collect();
        assert_eq!(rows(&out), expected, "`{query}` returned wrong rows");
        assert_eq!(executed_name(&out), None, "`{query}` kept the metric name");
        assert_eq!(out.len(), 1, "`{query}` did not produce one output chunk");
        assert!(out[0].is_last, "`{query}`'s output did not close");
    }
}

#[test]
fn a_compiled_filtering_comparison_keeps_the_values_that_passed() {
    let (timestamps, _) = samples_at_every_grid_point();
    let values = (1..=21)
        .map(|value| Some(f64::from(value) - 10.0))
        .collect();
    let out = execute_compiled("x > 0", vec![raw_x(&timestamps, values, true)]);

    let expected: Vec<(usize, f64)> = (0..21)
        .filter_map(|index| {
            let value = index as f64 + 1.0 - 10.0;
            (value > 0.0).then_some((index, value))
        })
        .collect();
    assert_eq!(rows(&out), expected, "`x > 0` did not filter the vector");
    assert_eq!(
        executed_name(&out),
        Some("x".to_string()),
        "a filtering comparison keeps the labels it read"
    );
}

#[test]
fn a_compiled_negation_negates_every_value() {
    let (timestamps, values) = samples_at_every_grid_point();
    let out = execute_compiled("-x", vec![raw_x(&timestamps, values, true)]);
    let expected: Vec<(usize, f64)> = (0..21)
        .map(|index| (index, -(index as f64 + 1.0)))
        .collect();
    assert_eq!(rows(&out), expected, "`-x` did not negate every value");
}

#[test]
fn a_compiled_negation_of_a_window_negates_the_window_result() {
    let (timestamps, values) = samples_at_every_grid_point();
    let input = || vec![raw_x(&timestamps, values.clone(), true)];

    let baseline = rows(&execute_compiled("rate(x[5m])", input()));
    assert!(
        !baseline.is_empty(),
        "the window answered nothing, so this case would prove nothing"
    );

    let out_chunks = execute_compiled("-rate(x[5m])", input());
    let out = rows(&out_chunks);
    let expected: Vec<(usize, f64)> = baseline
        .iter()
        .map(|(position, value)| (*position, -*value))
        .collect();
    assert_eq!(
        out, expected,
        "`-rate(x[5m])` did not negate the window's answer"
    );
    // `rate(x[5m]) * 100` anchors the first result at 0.5, so the rate alone is 0.005 -- the same
    // window `(5 + 2.5 + 0) / 5 / 300s` -- and negation flips only its sign.
    assert_eq!(
        out.first(),
        Some(&(1, -0.005)),
        "the first negated rate was not the sign-flipped anchor"
    );
    assert_eq!(
        executed_name(&out_chunks),
        None,
        "arithmetic drops the metric name"
    );
}

#[test]
fn a_compiled_constant_on_the_left_of_a_comparison_keeps_the_operand_order() {
    let (timestamps, values) = samples_at_every_grid_point();
    let left = execute_compiled("10 < x", vec![raw_x(&timestamps, values.clone(), true)]);
    let left_rows = rows(&left);
    let expected: Vec<(usize, f64)> = (0..21)
        .filter_map(|index| {
            let value = index as f64 + 1.0;
            (value > 10.0).then_some((index, value))
        })
        .collect();
    assert_eq!(
        left_rows, expected,
        "`10 < x` did not keep values greater than 10"
    );
    assert_eq!(
        executed_name(&left),
        Some("x".to_string()),
        "a filtering comparison keeps the labels it read"
    );

    let right = rows(&execute_compiled(
        "x < 10",
        vec![raw_x(&timestamps, values, true)],
    ));
    let complementary: Vec<(usize, f64)> = (0..21)
        .filter_map(|index| {
            let value = index as f64 + 1.0;
            (value < 10.0).then_some((index, value))
        })
        .collect();
    assert_eq!(
        right, complementary,
        "`x < 10` did not keep values below 10"
    );
    assert_ne!(
        left_rows, right,
        "the two operand orders produced the same rows"
    );
    assert!(!left_rows.is_empty(), "`10 < x` produced no rows");
    assert!(!right.is_empty(), "`x < 10` produced no rows");
}

/// The baseline is the same executor's `rate(x[5m])` on the same input, not a re-derived rate.
#[test]
fn a_compiled_broadcast_over_a_window_multiplies_the_window_result() {
    let (timestamps, values) = samples_at_every_grid_point();
    let input = || vec![raw_x(&timestamps, values.clone(), true)];

    let baseline = rows(&execute_compiled("rate(x[5m])", input()));
    assert!(
        !baseline.is_empty(),
        "the window answered nothing, so this case would prove nothing"
    );

    let out = rows(&execute_compiled("rate(x[5m]) * 100", input()));
    let expected: Vec<(usize, f64)> = baseline
        .iter()
        .map(|(position, value)| (*position, value * 100.0))
        .collect();
    assert_eq!(
        out, expected,
        "`rate(x[5m]) * 100` is not the window's answer multiplied by 100"
    );

    // One absolute value, so a regressed baseline cannot pass. Window `(-295s, 5s]` holds two
    // samples; the start gap exceeds the 5.5s threshold and extrapolates by half the 5s interval:
    // `(5 + 2.5 + 0) / 5 / 300s = 0.005`, times 100 is 0.5.
    assert_eq!(
        out.first(),
        Some(&(1, 0.5)),
        "the first rate was not the extrapolated one the kernel computes"
    );
}

#[test]
fn a_compiled_broadcast_preserves_missing_values_and_skipped_positions() {
    // A missing sample at 10s and no sample at all at 20s.
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for index in 0..21 {
        if index == 4 {
            continue;
        }
        timestamps.push(index * 5_000);
        values.push((index != 2).then_some(index as f64 + 1.0));
    }

    let out = execute_compiled("x * 100", vec![raw_x(&timestamps, values, true)]);
    let expected: Vec<(usize, f64)> = (0..21)
        .filter(|index| *index != 2 && *index != 4)
        .map(|index| (index, (index as f64 + 1.0) * 100.0))
        .collect();
    assert_eq!(
        rows(&out),
        expected,
        "the broadcast changed which positions hold a value"
    );
    // Holes, not zeros: both neighbours of each are present.
    let positions: Vec<usize> = rows(&out)
        .into_iter()
        .map(|(position, _)| position)
        .collect();
    assert!(!positions.contains(&2), "a missing sample became a value");
    assert!(!positions.contains(&4), "a skipped position became a value");
}

/// Checks finality, not streaming: the `Instant` below the binary buffers the whole series.
#[test]
fn a_compiled_broadcast_answers_a_series_delivered_in_two_chunks() {
    let timestamps: Vec<i64> = (0..4).map(|index| index * 5_000).collect();
    let first = raw_x(&timestamps[..2], vec![Some(1.0), Some(2.0)], false);
    let last = raw_x(&timestamps[2..], vec![Some(3.0), Some(4.0)], true);

    let out = execute_compiled("x * 100", vec![first, last]);
    let expected: Vec<(usize, f64)> = (0..4)
        .map(|index| (index, (index as f64 + 1.0) * 100.0))
        .collect();
    assert_eq!(
        rows(&out),
        expected,
        "`x * 100` did not transform every chunk"
    );
    assert_eq!(
        out.len(),
        1,
        "the selection answers a whole series at once, so the broadcast saw one chunk"
    );
    assert_eq!(
        out.iter().filter(|chunk| chunk.is_last).count(),
        1,
        "the series closed more or less than once"
    );
    assert!(
        out.last().expect("an answer").is_last,
        "the closing chunk of the input did not close the output"
    );
}

#[test]
fn a_predict_linear_binds_its_horizon_and_its_window() {
    let tree = compile(
        &eval_stmt("predict_linear(x[5m], 300)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`predict_linear(x[5m], 300)` was refused: {error}"));

    let (_, op) = root_window(&tree);
    assert_eq!(op.function, RangeFunction::PredictLinear(300.0));
    assert_eq!(op.window.range_ms, 300_000, "the window was not bound");
    assert!(op.input_grid.is_none(), "raw input has no grid to declare");
}

#[test]
fn a_double_exponential_smoothing_binds_both_factors_in_order() {
    let cases = [
        ("double_exponential_smoothing(x[5m], 0.5, 0.1)", 0.5, 0.1),
        ("double_exponential_smoothing(x[5m], 0.1, 0.5)", 0.1, 0.5),
        (
            "double_exponential_smoothing(x[5m], 1 / 2, 0.1 + 0.2)",
            0.5,
            0.30000000000000004,
        ),
    ];

    for (query, smoothing, trend) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (_, op) = root_window(&tree);
        assert_eq!(
            op.function,
            RangeFunction::DoubleExponentialSmoothing { smoothing, trend },
            "`{query}` bound its factors wrongly"
        );
        assert_eq!(op.window.range_ms, 300_000, "`{query}` lost its window");
    }
}

#[test]
fn a_smoothing_with_one_factor_is_refused_and_names_what_it_reads() {
    let stmt = with_call_arguments("double_exponential_smoothing(x[5m], 0.5, 0.1)", |args| {
        args.pop();
    });
    let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
        .expect_err("a smoothing with one factor compiled, and it should not have");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "the refusal is not NotImplemented: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("with 2 argument(s), where it takes 3"),
        "the refusal did not name the count the function takes: {message}"
    );
    assert!(
        message.contains("its range selector and two numbers"),
        "the refusal did not say what the function reads: {message}"
    );
}

/// The reported position counts the range selector as an argument.
#[test]
fn a_computed_range_parameter_is_refused_and_names_its_position() {
    let cases = [
        ("quantile_over_time(time(), x[5m])", "first argument"),
        ("predict_linear(x[5m], time())", "second argument"),
        (
            "double_exponential_smoothing(x[5m], time(), 0.1)",
            "second argument",
        ),
        (
            "double_exponential_smoothing(x[5m], 0.5, time())",
            "third argument",
        ),
    ];

    for (query, position) in cases {
        let error = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .expect_err(&format!("`{query}` compiled, and it should not have"));
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "`{query}` did not report NotImplemented: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains(position),
            "`{query}` did not name which argument it found: {message}"
        );
        assert!(
            message.contains("constant expression"),
            "`{query}` did not say what the argument has to be: {message}"
        );
    }
}

/// The grid a subquery evaluates its inner expression on, as `(start_ms, step_ms, len)`.
fn child_grid(window: &BoundWindow) -> (i64, i64, u64) {
    let grid = window
        .input_grid
        .as_ref()
        .expect("a subquery's window declares the grid its input was evaluated on");
    let spec = grid.spec();
    (spec.start_ms, spec.step_ms, spec.len)
}

#[test]
fn a_subquery_feeds_a_window_evaluated_on_its_own_grid() {
    let tree = compile(
        &eval_stmt("rate(x[5m:1m])"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`rate(x[5m:1m])` was refused: {error}"));

    let (input, op) = root_window(&tree);
    assert_eq!(op.function, RangeFunction::Rate);
    assert_eq!(op.window.range_ms, 300_000, "the subquery range was lost");
    assert_eq!(op.window.offset_ms, 0);
    assert_eq!(op.window.anchor_ms, None);

    // The output lands on the statement's grid: 0, 5s, ..., 100s.
    let spec = op.output_grid.spec();
    assert_eq!((spec.start_ms, spec.step_ms, spec.len), (0, 5_000, 21));

    // The inner selector is evaluated at one-minute steps, and the earliest read window opens at
    // `0 - 300000`, so the first step boundary strictly after it is `-240000`.
    assert_eq!(
        child_grid(op),
        (-240_000, 60_000, 6),
        "the subquery's own grid is wrong"
    );
    let inner = evaluated(input);
    assert_eq!(
        inner.eval_grid.as_ref().unwrap().spec().start_ms,
        -240_000,
        "the inner expression was not evaluated on the subquery's grid"
    );

    let NativeExpr::Instant { input: scan, .. } = &input.expr else {
        panic!(
            "the subquery's input is not an instant selection: {:?}",
            input.expr
        );
    };
    let NativeExpr::Input(bound) = &scan.expr else {
        panic!("the innermost node is not an input: {:?}", scan.expr);
    };
    assert!(
        matches!(&bound.output, NativeType::Samples(schema) if schema.eval_grid.is_none()),
        "the subquery's innermost input is not raw samples: {:?}",
        bound.output
    );
}

/// The child grid is derived from the statement's grid, the range, the offset and the step, and
/// each of those changes it. The cases are asserted as whole specs, because a formula that dropped
/// one term would still produce a plausible grid.
#[test]
fn a_subquery_grid_follows_its_range_step_and_offset() {
    let cases = [
        // (query, start_ms, step_ms, len)
        // The plain case: one minute steps, five minute windows, no offset.
        ("rate(x[5m:1m])", -240_000, 60_000, 6),
        // The statement's own step is not the subquery's step.
        ("rate(x[5m:5s])", -295_000, 5_000, 80),
        // A step that does not divide the range, so the first boundary is not `-range`, and the
        // last point is the last multiple of the step rather than the statement's end.
        ("rate(x[5m:7s])", -294_000, 7_000, 57),
        // A positive offset reads earlier samples, so the whole child grid moves back.
        ("rate(x[5m:1m] offset 1m)", -300_000, 60_000, 6),
        // A negative offset reads later ones, so it moves forward.
        ("rate(x[5m:1m] offset -1m)", -180_000, 60_000, 6),
    ];

    for (query, start_ms, step_ms, len) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (_, op) = root_window(&tree);
        assert_eq!(
            child_grid(op),
            (start_ms, step_ms, len),
            "`{query}` bound the wrong child grid"
        );
        // Whatever the child grid is, the window still reports the statement's.
        let spec = op.output_grid.spec();
        assert_eq!(
            (spec.start_ms, spec.step_ms, spec.len),
            (0, 5_000, 21),
            "`{query}` moved the output off the statement's grid"
        );
    }
}

#[test]
fn a_subquery_with_an_empty_child_grid_is_refused() {
    let error = compile(
        &eval_stmt("rate(x[1ms:1m] @ 0.001)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect_err("an empty subquery child grid compiled");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "an empty child grid is an unsupported subquery shape: {error}"
    );
    assert!(
        error.to_string().contains("names no evaluation point"),
        "the refusal did not identify the empty child grid: {error}"
    );
}

#[test]
fn large_subquery_child_grids_compile_deferred() {
    const DAY_STEP_GRID: (i64, i64, u64) = (-86_399_999, 1, 86_400_000);
    let cases = [
        ("rate(x[1d:1ms] @ 0)", false),
        ("rate(rate(x[1d:1ms] @ 0)[1d:1ms] @ 0)", true),
    ];

    for (query, nested) in cases {
        let mut stmt = eval_stmt(query);
        stmt.end = UNIX_EPOCH;
        stmt.interval = Duration::from_millis(1);
        let tree = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (input, outer) = root_window(&tree);
        assert_eq!(child_grid(outer), DAY_STEP_GRID, "`{query}` outer grid");
        if nested {
            let NativeExpr::Window { op: inner, .. } = &input.expr else {
                panic!(
                    "`{query}` inner expression is not a window: {:?}",
                    input.expr
                );
            };
            assert_eq!(child_grid(inner), DAY_STEP_GRID, "`{query}` inner grid");
        }
    }

    // An eager timestamp vector for this range would exceed isize::MAX bytes.
    let range_ms = (isize::MAX / 8 + 1) as u64;
    let mut stmt = eval_stmt("rate(x[1ms:1ms] @ 0)");
    stmt.end = UNIX_EPOCH;
    stmt.interval = Duration::from_millis(1);
    let PromExpr::Call(call) = &mut stmt.expr else {
        unreachable!("query parses to a call");
    };
    let PromExpr::Subquery(subquery) = &mut *call.args.args[0] else {
        unreachable!("call argument parses to a subquery");
    };
    subquery.range = Duration::from_millis(range_ms);

    let tree = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
        .expect("a huge but valid deferred child grid should compile");
    let (_, window) = root_window(&tree);
    let grid = child_grid(window);
    assert_eq!(grid.0, -(range_ms as i64) + 1);
    assert_eq!(grid.1, 1);
    assert_eq!(grid.2, range_ms);
}

/// `i64::MAX` milliseconds as a `SystemTime`, or `None` where `SystemTime` cannot hold it (Windows
/// counts 100ns intervals from 1601). The tests below return early on `None` and check nothing.
fn extreme_millisecond_endpoint() -> Option<SystemTime> {
    UNIX_EPOCH.checked_add(Duration::from_millis(i64::MAX as u64))
}

/// A sampled child endpoint can fit even when the unrounded read-window endpoint does not.
#[test]
fn a_subquery_accepts_an_in_range_sampled_endpoint() {
    let Some(endpoint) = extreme_millisecond_endpoint() else {
        return;
    };
    let mut stmt = eval_stmt("rate(x[1s:1s] offset -1ms)");
    stmt.start = endpoint;
    stmt.end = endpoint;
    stmt.interval = Duration::from_millis(1);

    let tree = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
        .expect("the in-range sampled endpoint should compile");
    let (_, op) = root_window(&tree);
    let spec = op.input_grid.as_ref().unwrap().spec();
    assert_eq!(
        (spec.start_ms, spec.step_ms, spec.len),
        (9_223_372_036_854_775_000, 1_000, 1),
        "the child grid should contain the last in-range 1-second sample"
    );
}

#[test]
fn a_subquery_with_an_overflowing_fold_offset_is_refused() {
    let Some(endpoint) = extreme_millisecond_endpoint() else {
        return;
    };
    let mut stmt = eval_stmt("rate(x[5m:1m] @ 0)");
    let PromExpr::Call(call) = &mut stmt.expr else {
        unreachable!("query parses to a call");
    };
    let PromExpr::Subquery(subquery) = &mut *call.args.args[0] else {
        unreachable!("call argument parses to a subquery");
    };
    subquery.at = Some(AtModifier::At(endpoint));
    subquery.offset = Some(Offset::Neg(Duration::from_millis(1)));

    let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
        .expect_err("an overflowing subquery fold offset compiled");
    assert!(
        matches!(error, DataFusionError::Plan(_)),
        "an overflowing folded offset is a plan error: {error}"
    );
    assert!(
        error.to_string().contains("out of range"),
        "the rejection did not identify the offset overflow: {error}"
    );
}

#[test]
fn a_subquery_step_must_be_positive() {
    // The parser refuses `x[5m:0s]`, so the step is edited on a parsed tree.
    let mut stmt = eval_stmt("rate(x[5m:1m])");
    let PromExpr::Call(call) = &mut stmt.expr else {
        unreachable!("`rate(x[5m:1m])` parses to a call");
    };
    let PromExpr::Subquery(subquery) = &mut *call.args.args[0] else {
        unreachable!("`x[5m:1m]` parses to a subquery");
    };
    subquery.step = Some(Duration::ZERO);

    let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
        .expect_err("a zero subquery step compiled, and it should not have");
    assert!(
        matches!(error, DataFusionError::Plan(_)),
        "a zero step is a plan error, not a shape this compiler lacks: {error}"
    );
    assert!(
        error.to_string().contains("has to be longer than zero"),
        "the rejection did not say what a step has to be: {error}"
    );
}

/// Prometheus's default evaluation interval (1m), not the statement's 5s step.
#[test]
fn a_subquery_without_a_step_uses_the_default_interval() {
    let tree = compile(
        &eval_stmt("rate(x[5m:])"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`rate(x[5m:])` was refused: {error}"));

    let (_, op) = root_window(&tree);
    assert_eq!(op.window.range_ms, 300_000);
    assert_eq!(
        child_grid(op).1,
        60_000,
        "a subquery without a step did not use the default interval"
    );
}

/// The expected child grid of an anchored subquery: the multiples of `step` in
/// `(anchor - offset - range, anchor - offset]`. Found by enumeration rather than division so it
/// shares no rounding convention with the compiler's formula.
fn anchored_child_grid(
    anchor_ms: i64,
    range_ms: i64,
    step_ms: i64,
    offset_ms: i64,
) -> (i64, i64, u64) {
    let end = anchor_ms - offset_ms;
    let start = end - range_ms;
    let mut first = None;
    let mut len = 0_u64;
    for k in (start / step_ms - 2)..=(end / step_ms + 2) {
        let sample = k * step_ms;
        if sample <= start || sample > end {
            continue;
        }
        first.get_or_insert(sample);
        len += 1;
    }
    (
        first.expect("the read interval names no sample at all"),
        step_ms,
        len,
    )
}

/// The anchor is restated in the case table even for `start()` and `end()`, so the expected grid
/// does not come from the compiler.
#[test]
fn an_anchored_subquery_reads_one_fixed_interval() {
    // (query, anchor_ms, range_ms, step_ms, offset_ms, statement start_ms, statement end_ms)
    let cases = [
        (
            "rate(x[5m:1m] @ 1234)",
            1_234_000,
            300_000,
            60_000,
            0,
            0,
            100_000,
        ),
        // `start()` and `end()` are the statement's own bounds, not points of its grid.
        (
            "rate(x[5m:1m] @ start())",
            0,
            300_000,
            60_000,
            0,
            0,
            100_000,
        ),
        (
            "rate(x[5m:1m] @ end())",
            100_000,
            300_000,
            60_000,
            0,
            0,
            100_000,
        ),
        // A pre-epoch anchor.
        (
            "rate(x[5m:1m] @ -1.5)",
            -1_500,
            300_000,
            60_000,
            0,
            0,
            100_000,
        ),
        // `@` and `offset` in either order.
        (
            "rate(x[5m:1m] offset 1m @ 1234)",
            1_234_000,
            300_000,
            60_000,
            60_000,
            0,
            100_000,
        ),
        (
            "rate(x[5m:1m] @ 1234 offset 1m)",
            1_234_000,
            300_000,
            60_000,
            60_000,
            0,
            100_000,
        ),
        // A step that divides neither the range nor the anchor, so a grid rounded to either would
        // name different samples.
        (
            "rate(x[5m:7s] @ 1234)",
            1_234_000,
            300_000,
            7_000,
            0,
            0,
            100_000,
        ),
        // The end is not a grid point and the start is off the child step.
        (
            "rate(x[5m:1m] @ end())",
            103_000,
            300_000,
            60_000,
            0,
            3_000,
            103_000,
        ),
    ];

    for (query, anchor_ms, range_ms, step_ms, offset_ms, start_ms, end_ms) in cases {
        let stmt = eval_stmt_between(
            query,
            Duration::from_millis(start_ms as u64),
            Duration::from_millis(end_ms as u64),
        );
        let tree = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (input, op) = root_window(&tree);

        assert_eq!(
            op.window.anchor_ms,
            Some(anchor_ms),
            "`{query}` bound the wrong anchor"
        );
        assert_eq!(op.window.range_ms, range_ms, "`{query}`: wrong range");
        assert_eq!(op.window.offset_ms, offset_ms, "`{query}`: wrong offset");

        let expected = anchored_child_grid(anchor_ms, range_ms, step_ms, offset_ms);
        assert_eq!(
            child_grid(op),
            expected,
            "`{query}` bound the wrong child grid: the interval \
             ({}, {}] at {step_ms}ms per step holds {expected:?}",
            anchor_ms - offset_ms - range_ms,
            anchor_ms - offset_ms
        );
        let inner = evaluated(input);
        assert_eq!(
            inner.eval_grid.as_ref().unwrap().spec().start_ms,
            expected.0,
            "`{query}` evaluated its expression off the declared child grid"
        );
        let out = op.output_grid.spec();
        assert_eq!(
            (out.start_ms, out.step_ms),
            (start_ms, 5_000),
            "`{query}` moved the output off the statement's grid"
        );
        assert_eq!(
            out.len as i64,
            (end_ms - start_ms) / 5_000 + 1,
            "`{query}` changed the statement's grid length"
        );
    }
}

#[test]
fn an_anchored_subquery_grid_ignores_the_statement() {
    let short = eval_stmt_between(
        "rate(x[5m:1m] @ 1234)",
        Duration::ZERO,
        Duration::from_secs(100),
    );
    let long = eval_stmt_between(
        "rate(x[5m:1m] @ 1234)",
        Duration::from_secs(10),
        Duration::from_secs(600),
    );

    let mut grids = Vec::new();
    for stmt in [&short, &long] {
        let tree = compile(stmt, raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("an anchored subquery was refused: {error}"));
        let (_, op) = root_window(&tree);
        grids.push((child_grid(op), grid_of(tree.root())));
    }

    let expected_child = anchored_child_grid(1_234_000, 300_000, 60_000, 0);
    assert_eq!(
        grids[0].0, expected_child,
        "the short statement's child grid is wrong"
    );
    assert_eq!(
        grids[1].0, expected_child,
        "the child grid moved with the statement, so the anchor is not what it reads"
    );
    assert_ne!(
        grids[0].1, grids[1].1,
        "the two statements have the same output grid, so this case proves nothing"
    );
}

#[test]
fn a_subquery_over_a_scalar_is_refused() {
    let mut stmt = eval_stmt("rate(x[5m:1m])");
    let PromExpr::Call(call) = &mut stmt.expr else {
        unreachable!("`rate(x[5m:1m])` parses to a call");
    };
    let PromExpr::Subquery(subquery) = &mut *call.args.args[0] else {
        unreachable!("`x[5m:1m]` parses to a subquery");
    };
    *subquery.expr = PromExpr::NumberLiteral(NumberLiteral::new(1.0));

    let error = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
        .expect_err("a subquery over a scalar compiled, and it should not have");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "the refusal is not NotImplemented: {error}"
    );
    assert!(
        error.to_string().contains("has to evaluate a vector"),
        "the refusal did not say what a subquery evaluates: {error}"
    );
}

/// Each level's child grid is derived from its parent's grid, not the statement's.
#[test]
fn a_subquery_nests_and_each_level_has_its_own_grid() {
    let tree = compile(
        &eval_stmt("rate(rate(x[5m:1m])[10m:30s])"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_or_else(|error| panic!("`rate(rate(x[5m:1m])[10m:30s])` was refused: {error}"));

    let (outer_input, outer) = root_window(&tree);
    assert_eq!(outer.window.range_ms, 600_000);
    // The outer subquery reads ten minutes back at thirty-second steps, so its first boundary after
    // `-600000` is `-570000` and its last multiple of 30000 below the statement's end is `90000`.
    assert_eq!(
        child_grid(outer),
        (-570_000, 30_000, 23),
        "the outer subquery's grid is wrong"
    );

    let NativeExpr::Window {
        input: inner_input,
        op: inner,
    } = &outer_input.expr
    else {
        panic!(
            "the outer subquery's input is not a window: {:?}",
            outer_input.expr
        );
    };
    assert_eq!(inner.function, RangeFunction::Rate);
    assert_eq!(inner.window.range_ms, 300_000);
    // The inner subquery is evaluated on the outer one's grid, not on the statement's: its earliest
    // read window opens at `-570000 - 300000`, whose first one-minute boundary is `-840000`.
    assert_eq!(
        child_grid(inner),
        (-840_000, 60_000, 16),
        "the inner subquery's grid is wrong, so the outer grid was not passed down"
    );

    let NativeExpr::Instant { input: scan, .. } = &inner_input.expr else {
        panic!(
            "the inner window's input is not an instant selection: {:?}",
            inner_input.expr
        );
    };
    assert!(
        matches!(scan.expr, NativeExpr::Input(_)),
        "the innermost node is not the scan: {:?}",
        scan.expr
    );
}

/// The subquery grid holds every sample its windows read and none they cannot. The oracle uses only
/// the query's own numbers (restated in the table) and checks membership by modulo, since a grid
/// with a wrong step or phase can still span the right bounds.
#[test]
fn a_subquery_grid_holds_every_sample_its_windows_read() {
    let end = Duration::from_secs(100);

    // (query, statement start, range_ms, step_ms, offset_ms, some point reads no sample)
    let cases = [
        ("rate(x[5m:1m])", 0, 300_000, 60_000, 0, false),
        // A step that divides neither the range nor the statement's 5s step.
        ("rate(x[5m:7s])", 0, 300_000, 7_000, 0, false),
        ("rate(x[5m:5s])", 0, 300_000, 5_000, 0, false),
        // A step that divides neither the range nor the offset.
        (
            "rate(x[90s:40s] offset 30s)",
            0,
            90_000,
            40_000,
            30_000,
            false,
        ),
        (
            "rate(x[5m:1m] offset 1m)",
            0,
            300_000,
            60_000,
            60_000,
            false,
        ),
        (
            "rate(x[5m:1m] offset -1m)",
            0,
            300_000,
            60_000,
            -60_000,
            false,
        ),
        // A window narrower than the child step, so some points read no sample.
        ("rate(x[30s:1m])", 0, 30_000, 60_000, 0, true),
        // A start 3s off the child step catches a grid stepped from the statement's start.
        ("rate(x[5m:1m])", 3_000, 300_000, 60_000, 0, false),
        ("rate(x[30s:1m])", 3_000, 30_000, 60_000, 0, true),
    ];

    for (query, start, range_ms, step_ms, offset_ms, some_points_read_nothing) in cases {
        let stmt = eval_stmt_between(query, Duration::from_millis(start as u64), end);
        let tree = compile(&stmt, raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let (_, op) = root_window(&tree);

        let (child_start, child_step, child_len) = child_grid(op);
        let child_last = child_start + (child_len as i64 - 1) * child_step;
        let out = op.output_grid.spec();
        let out_last = out.start_ms + (out.len as i64 - 1) * out.step_ms;

        // The grid must stay within the union of the read windows.
        let earliest = out.start_ms - offset_ms - range_ms;
        let latest = out_last - offset_ms;
        for index in 0..child_len as i64 {
            let point = child_start + index * child_step;
            assert!(
                point > earliest && point <= latest,
                "`{query}`: the child grid point {point}ms lies outside the read windows \
                 {earliest}ms..={latest}ms, where no window can read it"
            );
        }

        let mut every_point_reads = true;
        let mut samples_checked = 0_usize;
        for index in 0..out.len as i64 {
            let point = out.start_ms + index * out.step_ms;
            let read_start = point - offset_ms - range_ms;
            let read_end = point - offset_ms;
            // Enumerated two steps wider on each side to avoid assuming a rounding convention.
            let mut read_anything = false;
            for k in (read_start / step_ms - 2)..=(read_end / step_ms + 2) {
                let sample = k * step_ms;
                if sample <= read_start || sample > read_end {
                    continue;
                }
                read_anything = true;
                samples_checked += 1;
                assert!(
                    sample >= child_start
                        && sample <= child_last
                        && (sample - child_start) % child_step == 0,
                    "`{query}`: the window at {point}ms reads {sample}ms, which is not a point of \
                     the declared child grid {child_start}ms..={child_last}ms at {child_step}ms per \
                     step"
                );
            }
            every_point_reads &= read_anything;
        }

        assert!(
            samples_checked > 0,
            "`{query}`: no point of the statement's grid reads a sample, so this case checks nothing"
        );
        assert_eq!(
            every_point_reads, !some_points_read_nothing,
            "`{query}`: the case table says some point reads no sample, so every point reading one \
             (or vice versa) means the window is not the one the query wrote"
        );
    }
}

/// `absent` labels follow Prometheus: the first equality matcher on a name sets the label, any
/// other matcher on it removes it, and matchers accepting a missing label are applied last.
#[test]
fn whole_vector_functions_bind_a_vector_operation() {
    let cases = [
        ("sort(x)", "sort"),
        ("sort_desc(rate(x[5m]))", "sort_desc"),
        (
            r#"sort_by_label(x, "job", "le")"#,
            "sort_by_label [job, le]",
        ),
        (
            r#"sort_by_label_desc(x, "job")"#,
            "sort_by_label_desc [job]",
        ),
        (
            r#"absent(x{job="a", env=~"p.*", instance!="", __field__="v"})"#,
            r#"absent {job="a"}"#,
        ),
        // `job!="b"` accepts a missing `job`, so it reads after `job="a"` and removes it.
        (r#"absent(x{job!="b", job="a"})"#, "absent {}"),
        (r#"absent({__name__="x", job="a"})"#, r#"absent {job="a"}"#),
        (
            "histogram_quantile(1 - 0.1, rate(x[5m]))",
            "histogram_quantile 0.9",
        ),
        ("histogram_quantile(0.9, x)", "histogram_quantile 0.9"),
        ("histogram_fraction(0, 0.5, x)", "histogram_fraction 0 0.5"),
        ("absent(rate(x[5m]))", "absent {}"),
    ];
    for (query, op) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let display = tree.to_string();
        let root = display.lines().next().expect("a compiled tree has a root");
        assert_eq!(root, format!("Vector: {op}"), "`{query}`");
    }
}

#[test]
fn absent_over_time_binds_absent_over_a_presence_read() {
    let cases = [
        // A nameless selector; the existing plan's UDF needs a metric name.
        (
            r#"absent_over_time({no_such_label="x", job="api"}[5m])"#,
            r#"absent {job="api", no_such_label="x"}"#,
        ),
        (
            r#"absent_over_time(x{job="a", env=~"p.*", instance!="", __field__="v"}[5m])"#,
            r#"absent {job="a"}"#,
        ),
        // `job!="b"` accepts a missing `job`, so it reads after `job="a"` and removes it.
        (r#"absent_over_time(x{job!="b", job="a"}[5m])"#, "absent {}"),
        // A subquery is a matrix too, and it is not a selector, so it fixes no label.
        (r#"absent_over_time(rate(x[1m])[5m:1m])"#, "absent {}"),
    ];
    for (query, op) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let display = tree.to_string();
        let root = display.lines().next().expect("a compiled tree has a root");
        assert_eq!(root, format!("Vector: {op}"), "`{query}`");
        // `present_over_time` would drop the metric name.
        assert!(
            display.contains("  Window: last_over_time [300000ms]"),
            "`{query}` does not read its window through the presence fold: {display}"
        );
    }
}

/// `absent_over_time(x)` with `argument` in place of its matrix, which the parser would reject.
fn with_absent_over_time_argument(argument: &str) -> EvalStmt {
    let mut stmt = eval_stmt("absent_over_time(x[5m])");
    let PromExpr::Call(call) = &mut stmt.expr else {
        unreachable!("`absent_over_time(x[5m])` parses to a call");
    };
    *call.args.args[0] = promql_parser::parser::parse(argument).unwrap();
    stmt
}

#[test]
fn absent_over_time_refuses_what_is_not_a_matrix() {
    for query in ["absent_over_time(x)", "absent_over_time(x[5m], 1)"] {
        assert!(
            promql_parser::parser::parse(query).is_err(),
            "`{query}` is a call this compiler would have had to refuse"
        );
    }
    let error = compile(
        &with_absent_over_time_argument("x"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect_err("a call over a vector was accepted");
    assert!(
        error
            .to_string()
            .contains("whose matrix is `x`, where it has to be a range selector"),
        "{error}"
    );
    let error = compile(
        &eval_stmt(r#"absent_over_time(x{a="b" or c="d"}[5m])"#),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .expect_err("alternative matchers were accepted");
    assert!(
        error
            .to_string()
            .contains("whose matchers are alternatives"),
        "{error}"
    );
}

/// `value` at each of [`eval_stmt`]'s 21 grid points.
fn grid_values(value: f64) -> Vec<(usize, Option<f64>)> {
    (0..21).map(|position| (position, Some(value))).collect()
}

/// Expected answers match Prometheus v0.315 on the same statements.
#[test]
fn absent_over_time_answers_only_the_points_no_series_has_a_sample_at() {
    let schema = raw_schema();
    let timestamps = agg_timestamps_0_to_100();
    let values: Vec<f64> = (1..=21).map(f64::from).collect();
    let with_samples = || {
        vec![agg_raw_chunk(
            &schema,
            b"a",
            agg_metric("a", &[("job", "api")]),
            &timestamps,
            &values,
        )]
    };

    assert!(
        aggregate_results(&execute_compiled("absent_over_time(a[5m])", with_samples())).is_empty(),
        "a series holding a sample in every window is not absent"
    );

    // `__name__` is never an `absent` label.
    assert_eq!(
        aggregate_results(&execute_compiled("absent_over_time(a[5m])", Vec::new())),
        vec![(None, Vec::new(), grid_values(1.0))],
        "a metric with no sample in any window answers the empty label set"
    );

    assert_eq!(
        aggregate_results(&execute_compiled(
            r#"absent_over_time({no_such_label="x", job="api"}[5m])"#,
            Vec::new()
        )),
        vec![(
            None,
            vec![
                ("job".to_string(), "api".to_string()),
                ("no_such_label".to_string(), "x".to_string()),
            ],
            grid_values(1.0),
        )]
    );

    assert_eq!(
        aggregate_results(&execute_compiled(
            r#"absent_over_time(a{job="api"}[5m])"#,
            Vec::new()
        )),
        vec![(
            None,
            vec![("job".to_string(), "api".to_string())],
            grid_values(1.0)
        )]
    );
}

#[test]
fn absent_over_time_reads_the_window_and_not_the_series() {
    let schema = raw_schema();
    let chunk_at = |timestamps: &[i64]| {
        vec![agg_raw_chunk(
            &schema,
            b"a",
            agg_metric("a", &[("job", "api")]),
            timestamps,
            &[1.0; 2],
        )]
    };
    let labels = vec![("job".to_string(), "api".to_string())];

    // 90s before the start: inside every 5m window.
    assert!(
        aggregate_results(&execute_compiled(
            "absent_over_time(a[5m])",
            chunk_at(&[-90_000, -60_000])
        ))
        .is_empty(),
        "a window reaching back past the samples holds them"
    );

    // 4m before the start: outside every window.
    assert_eq!(
        aggregate_results(&execute_compiled(
            r#"absent_over_time(a{job="api"}[5m])"#,
            chunk_at(&[-400_000, -350_000])
        )),
        vec![(None, labels.clone(), grid_values(1.0))]
    );

    let instant = eval_stmt_between(
        r#"absent_over_time(a{job="api"}[5m])"#,
        Duration::from_secs(100),
        Duration::from_secs(100),
    );
    let tree = compile(&instant, raw_schema(), SeriesOrdering::KeyThenTime).unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let mut execution =
        NativeExecutor::new(NativeExecutionState::try_new(&tree, pool, None).unwrap());
    for chunk in chunk_at(&[-400_000, -350_000]) {
        execution.push_raw(InputId::new(0), chunk).unwrap();
    }
    let output = execution.finish().unwrap();
    assert_eq!(
        aggregate_results(&output),
        vec![(None, labels, vec![(0, Some(1.0))])],
        "an instant statement answers at its one grid point"
    );
}

/// Two discovered metrics with equal ordinary labels stay two series because the presence fold
/// keeps `__name__`.
#[test]
fn absent_over_time_keeps_two_discovered_metrics_apart() {
    let schema = raw_schema();
    let timestamps = agg_timestamps_0_to_100();
    let values: Vec<f64> = (1..=21).map(f64::from).collect();
    let chunks = vec![
        agg_raw_chunk(
            &schema,
            b"a",
            agg_metric("a", &[("job", "api")]),
            &timestamps,
            &values,
        ),
        agg_raw_chunk(
            &schema,
            b"b",
            agg_metric("b", &[("job", "api")]),
            &timestamps,
            &values,
        ),
    ];
    assert!(
        aggregate_results(&execute_compiled(
            r#"absent_over_time({job="api"}[5m])"#,
            chunks
        ))
        .is_empty(),
        "two metrics holding samples are present, whatever labels they share"
    );
}

/// One raw chunk of `metric`, keyed by `key`, closing the series.
fn agg_raw_chunk(
    schema: &SeriesSchema,
    key: &[u8],
    metric: SeriesLabels,
    timestamps: &[i64],
    values: &[f64],
) -> RawSeriesChunk {
    agg_raw_chunk_with_last(schema, key, metric, timestamps, values, true)
}

/// One raw chunk of `metric`, keyed by `key`, with an explicit series-ending flag.
fn agg_raw_chunk_with_last(
    schema: &SeriesSchema,
    key: &[u8],
    metric: SeriesLabels,
    timestamps: &[i64],
    values: &[f64],
    is_last: bool,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        schema,
        Buffer::from_slice_ref(key),
        Some(metric),
        RawTimestamps::try_new(ScalarBuffer::from(timestamps.to_vec())).unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(values.to_vec()))),
        is_last,
    )
    .unwrap()
}

/// `name` with the labels in `tags`, in the order given.
fn agg_metric(name: &str, tags: &[(&str, &str)]) -> SeriesLabels {
    SeriesLabels::try_new(
        Some(name.into()),
        tags.iter()
            .map(|(name, value)| Label {
                name: (*name).into(),
                value: (*value).into(),
            })
            .collect(),
    )
    .unwrap()
}

/// What one result series of a native execution reports.
type AggSeries = (
    Option<String>,
    Vec<(String, String)>,
    Vec<(usize, Option<f64>)>,
);

/// Reads the outputs a native execution produced.
fn aggregate_results(output: &[EvaluatedChunk]) -> Vec<AggSeries> {
    let mut results: Vec<AggSeries> = output
        .iter()
        .map(|chunk| {
            let mut tags: Vec<(String, String)> = chunk
                .series
                .metric
                .tags()
                .iter()
                .map(|tag| (tag.name.to_string(), tag.value.to_string()))
                .collect();
            tags.sort();
            let points = chunk
                .series
                .time
                .positions()
                .map(|(position, row)| {
                    let sample = chunk.series.fields.as_slice()[0]
                        .iter()
                        .nth(row)
                        .expect("the axis and the field are aligned");
                    (
                        position,
                        match sample {
                            SampleRef::Float(value) => Some(value),
                            _ => None,
                        },
                    )
                })
                .collect();
            (chunk.series.metric.name().map(str::to_string), tags, points)
        })
        .collect();
    results.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    results
}

/// Pushes `chunks` into the tree's only input, ends it, and collects what the root reports.
fn run_aggregate_tree(tree: &NativeTree, chunks: Vec<RawSeriesChunk>) -> Vec<AggSeries> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 26));
    let state = NativeExecutionState::try_new(tree, pool, None).unwrap();
    let mut execution = NativeExecutor::new(state);
    for chunk in chunks {
        execution
            .push_raw(InputId::new(0), chunk)
            .unwrap_or_else(|error| panic!("a source chunk was refused: {error}"));
    }
    let output = execution
        .finish()
        .unwrap_or_else(|error| panic!("the tree did not answer: {error}"));
    aggregate_results(&output)
}

/// The 21 grid points of a statement over 100 seconds in 5-second steps.
fn agg_points_0_to_100() -> Vec<usize> {
    (0..21).collect()
}

/// The timestamps of the same 21 points, in milliseconds.
fn agg_timestamps_0_to_100() -> Vec<i64> {
    (0..21).map(|index| index * 5_000).collect()
}

#[test]
fn a_compiled_sum_drops_every_label_and_adds_its_group() {
    let tree = compile(
        &eval_stmt("sum(x)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    let schema = raw_schema();
    let timestamps = agg_timestamps_0_to_100();
    let chunks = vec![
        agg_raw_chunk(
            &schema,
            b"a",
            agg_metric("x", &[("job", "api"), ("instance", "a")]),
            &timestamps,
            &(1..=21).map(f64::from).collect::<Vec<_>>(),
        ),
        agg_raw_chunk(
            &schema,
            b"b",
            agg_metric("x", &[("job", "db"), ("instance", "b")]),
            &timestamps,
            &(1..=21).map(|value| (2 * value) as f64).collect::<Vec<_>>(),
        ),
    ];

    let output = run_aggregate_tree(&tree, chunks);
    // `3` at the first point, three more at each of the twenty after it: the two members summed.
    let values: Vec<(usize, Option<f64>)> = agg_points_0_to_100()
        .into_iter()
        .map(|position| (position, Some(3.0 * (position as f64 + 1.0))))
        .collect();
    assert_eq!(output, vec![(None, Vec::new(), values)]);
}

#[test]
fn a_compiled_sum_aggregates_series_chunks_after_input_ends() {
    let schema = raw_schema();
    let timestamps = agg_timestamps_0_to_100();
    let values: Vec<f64> = (1..=21).map(f64::from).collect();
    let cases = [
        ("sum(x)", None, Vec::new()),
        (
            "sum by(job)(x)",
            None,
            vec![("job".to_string(), "api".to_string())],
        ),
        (
            "sum without(instance)(x)",
            None,
            vec![("job".to_string(), "api".to_string())],
        ),
    ];

    for (query, name, tags) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime).unwrap();
        let metric = agg_metric("x", &[("job", "api"), ("instance", "a")]);
        let chunks = vec![
            agg_raw_chunk_with_last(
                &schema,
                b"a",
                metric.clone(),
                &timestamps[..10],
                &values[..10],
                false,
            ),
            agg_raw_chunk_with_last(
                &schema,
                b"a",
                metric,
                &timestamps[10..],
                &values[10..],
                true,
            ),
        ];
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 26));
        let state = NativeExecutionState::try_new(&tree, pool, None).unwrap();
        let mut execution = NativeExecutor::new(state);
        for chunk in chunks {
            execution.push_raw(InputId::new(0), chunk).unwrap();
            assert!(
                execution.next_chunk().unwrap().is_none(),
                "`{query}` produced output before the whole input ended"
            );
        }
        let output = execution.finish().unwrap();
        let results = aggregate_results(&output);
        let expected_points = values
            .iter()
            .enumerate()
            .map(|(position, value)| (position, Some(*value)))
            .collect();
        assert_eq!(
            results,
            vec![(name.map(str::to_string), tags, expected_points)],
            "`{query}`"
        );
    }
}

#[test]
fn a_compiled_aggregate_over_empty_input_produces_no_groups() {
    for query in ["sum(x)", "sum by(job)(x)", "sum without(instance)(x)"] {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime).unwrap();
        assert_eq!(
            run_aggregate_tree(&tree, Vec::new()),
            Vec::new(),
            "`{query}` invented a group for empty input"
        );
    }
}

/// Members cover different points, so each group sums a subset that changes per point. No sample
/// sits exactly one lookback behind a grid point, so the boundary convention does not matter.
#[test]
fn a_compiled_sum_by_groups_its_members_and_sums_each_group() {
    let tree = compile(
        &eval_stmt("sum by(job)(x)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    let schema = raw_schema();
    let all = agg_timestamps_0_to_100();
    let chunks = vec![
        // The whole grid, holding one.
        agg_raw_chunk(
            &schema,
            b"a",
            agg_metric("x", &[("job", "api"), ("instance", "a")]),
            &all,
            &[1.0; 21],
        ),
        // One sample halfway between grid points 0 and 1, so only grid point 1 selects it.
        agg_raw_chunk(
            &schema,
            b"b",
            agg_metric("x", &[("job", "api"), ("instance", "b")]),
            &[2_500],
            &[2.0],
        ),
        // Four samples between grid points, covering grid points 1 to 4.
        agg_raw_chunk(
            &schema,
            b"e",
            agg_metric("x", &[("job", "api"), ("instance", "e")]),
            &[2_400, 7_400, 12_400, 17_400],
            &[5.0; 4],
        ),
        // The whole grid, holding ten.
        agg_raw_chunk(
            &schema,
            b"c",
            agg_metric("x", &[("job", "db"), ("instance", "c")]),
            &all,
            &[10.0; 21],
        ),
        // One sample halfway between grid points 0 and 1, holding twenty.
        agg_raw_chunk(
            &schema,
            b"d",
            agg_metric("x", &[("job", "db"), ("instance", "d")]),
            &[2_500],
            &[20.0],
        ),
    ];

    let output = run_aggregate_tree(&tree, chunks);
    // `api` is `a` alone at points 0 and 5 to 20, and every member at the points they cover:
    // point 1 is `1 + 2 + 5`, points 2 to 4 are `1 + 5`.
    let api: Vec<(usize, Option<f64>)> = agg_points_0_to_100()
        .into_iter()
        .map(|position| {
            let value = match position {
                1 => 8.0,
                2..=4 => 6.0,
                _ => 1.0,
            };
            (position, Some(value))
        })
        .collect();
    // `db` is the same shape over its own members: point 1 is `10 + 20`, the rest is `c` alone.
    let db: Vec<(usize, Option<f64>)> = agg_points_0_to_100()
        .into_iter()
        .map(|position| (position, Some(if position == 1 { 30.0 } else { 10.0 })))
        .collect();
    assert_eq!(
        output,
        vec![
            (None, vec![("job".to_string(), "api".to_string())], api),
            (None, vec![("job".to_string(), "db".to_string())], db),
        ]
    );
}

#[test]
fn a_compiled_sum_without_drops_the_named_label_and_the_name() {
    let tree = compile(
        &eval_stmt("sum without(instance)(x)"),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    let schema = raw_schema();
    let timestamps = agg_timestamps_0_to_100();
    let chunks = vec![
        agg_raw_chunk(
            &schema,
            b"a",
            agg_metric("x", &[("job", "api"), ("instance", "a"), ("zone", "west")]),
            &timestamps,
            &[1.0; 21],
        ),
        agg_raw_chunk(
            &schema,
            b"b",
            agg_metric("x", &[("job", "api"), ("instance", "b"), ("zone", "east")]),
            &timestamps,
            &[2.0; 21],
        ),
    ];

    let output = run_aggregate_tree(&tree, chunks);
    let values_of = |value: f64| -> Vec<(usize, Option<f64>)> {
        agg_points_0_to_100()
            .into_iter()
            .map(|position| (position, Some(value)))
            .collect()
    };
    assert_eq!(
        output,
        vec![
            (
                None,
                vec![
                    ("job".to_string(), "api".to_string()),
                    ("zone".to_string(), "east".to_string()),
                ],
                values_of(2.0),
            ),
            (
                None,
                vec![
                    ("job".to_string(), "api".to_string()),
                    ("zone".to_string(), "west".to_string()),
                ],
                values_of(1.0),
            ),
        ]
    );
}

/// Counters rising by one per 5s give the same extrapolated rate at every point: `59 / 295 = 0.2`.
#[test]
fn a_compiled_sum_over_a_rate_groups_the_window_it_reads() {
    let statement = EvalStmt {
        expr: promql_parser::parser::parse("sum by(job)(rate(x[5m]))").unwrap(),
        start: UNIX_EPOCH + Duration::from_secs(300),
        end: UNIX_EPOCH + Duration::from_secs(400),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(5),
    };
    let tree = compile(&statement, raw_schema(), SeriesOrdering::KeyThenTime).unwrap();
    let schema = raw_schema();
    let timestamps: Vec<i64> = (0..=80).map(|index| index * 5_000).collect();
    let values: Vec<f64> = (0..=80).map(|index| f64::from(index) + 1.0).collect();
    let chunks = vec![
        agg_raw_chunk(
            &schema,
            b"a",
            agg_metric("x", &[("job", "api"), ("instance", "a")]),
            &timestamps,
            &values,
        ),
        agg_raw_chunk(
            &schema,
            b"b",
            agg_metric("x", &[("job", "api"), ("instance", "b")]),
            &timestamps,
            &values,
        ),
        agg_raw_chunk(
            &schema,
            b"c",
            agg_metric("x", &[("job", "db"), ("instance", "c")]),
            &timestamps,
            &values,
        ),
    ];

    let output = run_aggregate_tree(&tree, chunks);
    let at_every_point = |value: f64| -> Vec<(usize, Option<f64>)> {
        (0..21).map(|position| (position, Some(value))).collect()
    };
    // One `rate` is `0.19999999999999998`, and the pair of them add to `0.39999999999999997`.
    assert_eq!(
        output,
        vec![
            (
                None,
                vec![("job".to_string(), "api".to_string())],
                at_every_point(0.39999999999999997),
            ),
            (
                None,
                vec![("job".to_string(), "db".to_string())],
                at_every_point(0.19999999999999998),
            ),
        ]
    );
}

#[test]
fn a_compiled_sum_over_a_rate_aggregates_series_chunks_after_input_ends() {
    let statement = EvalStmt {
        expr: promql_parser::parser::parse("sum by(job)(rate(x[5m]))").unwrap(),
        start: UNIX_EPOCH + Duration::from_secs(300),
        end: UNIX_EPOCH + Duration::from_secs(400),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(5),
    };
    let tree = compile(&statement, raw_schema(), SeriesOrdering::KeyThenTime).unwrap();
    let schema = raw_schema();
    let timestamps: Vec<i64> = (0..=80).map(|index| index * 5_000).collect();
    let values: Vec<f64> = (0..=80).map(|index| f64::from(index) + 1.0).collect();
    let metric = agg_metric("x", &[("job", "api"), ("instance", "a")]);
    let chunks = [
        agg_raw_chunk_with_last(
            &schema,
            b"a",
            metric.clone(),
            &timestamps[..41],
            &values[..41],
            false,
        ),
        agg_raw_chunk_with_last(
            &schema,
            b"a",
            metric,
            &timestamps[41..],
            &values[41..],
            true,
        ),
    ];
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 26));
    let state = NativeExecutionState::try_new(&tree, pool, None).unwrap();
    let mut execution = NativeExecutor::new(state);
    for chunk in chunks {
        execution.push_raw(InputId::new(0), chunk).unwrap();
        assert!(
            execution.next_chunk().unwrap().is_none(),
            "the rate aggregate produced output before the whole input ended"
        );
    }
    let output = execution.finish().unwrap();
    assert_eq!(
        aggregate_results(&output),
        vec![(
            None,
            vec![("job".to_string(), "api".to_string())],
            (0..21)
                .map(|position| (position, Some(0.19999999999999998)))
                .collect(),
        )]
    );
}

#[test]
fn a_compiled_sum_over_a_rate_with_empty_input_produces_no_groups() {
    let statement = EvalStmt {
        expr: promql_parser::parser::parse("sum by(job)(rate(x[5m]))").unwrap(),
        start: UNIX_EPOCH + Duration::from_secs(300),
        end: UNIX_EPOCH + Duration::from_secs(400),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(5),
    };
    let tree = compile(&statement, raw_schema(), SeriesOrdering::KeyThenTime).unwrap();
    assert_eq!(run_aggregate_tree(&tree, Vec::new()), Vec::new());
}

#[test]
fn label_functions_bind_their_change_into_a_map() {
    let cases = [
        (
            r#"label_replace(x, "dst", "$1", "src", "(.*)")"#,
            r#"label_replace "dst" "$1" "src" "(.*)""#,
        ),
        (
            r#"label_join(x, "dst", "-", "a", "b")"#,
            r#"label_join "dst" "-" ["a", "b"]"#,
        ),
    ];
    for (query, op) in cases {
        let tree = compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let display = tree.to_string();
        let root = display.lines().next().expect("a compiled tree has a root");
        assert_eq!(root, format!("Map: {op}"), "`{query}`");
    }
}

#[test]
fn histogram_functions_bind_their_kernel_into_a_map() {
    for name in [
        "histogram_count",
        "histogram_sum",
        "histogram_avg",
        "histogram_stddev",
        "histogram_stdvar",
    ] {
        let query = format!("{name}(x)");
        let tree = compile(
            &eval_stmt(&query),
            raw_schema(),
            SeriesOrdering::KeyThenTime,
        )
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"));
        let display = tree.to_string();
        let root = display.lines().next().expect("a compiled tree has a root");
        assert_eq!(root, format!("Map: {name}"), "`{query}`");
    }
}

#[test]
fn an_invalid_label_replace_expression_fails_the_statement() {
    let error = compile(
        &eval_stmt(r#"label_replace(x, "dst", "$1", "src", "(")"#),
        raw_schema(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap_err();
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
}

#[test]
fn timestamp_multi_field_input_is_not_supported_natively() {
    for query in ["timestamp(x)", "timestamp(abs(x))"] {
        let error = compile(
            &eval_stmt(query),
            raw_schema_with_fields(2),
            SeriesOrdering::KeyThenTime,
        )
        .unwrap_err();
        assert!(
            matches!(&error, DataFusionError::NotImplemented(_)),
            "`{query}` is a shape this compiler declines: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("timestamp() over a multi-value input"),
            "`{query}` did not name the field count as the reason: {error}"
        );
    }
    // One field is the shape the node does answer, so the refusal is about the field count.
    for query in ["timestamp(x)", "timestamp(abs(x))"] {
        compile(&eval_stmt(query), raw_schema(), SeriesOrdering::KeyThenTime)
            .unwrap_or_else(|error| panic!("`{query}` over one field was refused: {error}"));
    }
}

#[test]
fn timestamp_anchored_selector_retains_offset_and_ordinary_selection_keeps_it() {
    for (query, expected_offset, expected_anchor, expected_bounds) in [
        (
            "timestamp(x @ 50 offset 60s)",
            60_000,
            Some(50_000),
            (-15_000, -10_000),
        ),
        ("timestamp(x offset 2s)", 2_000, None, (-7_000, 98_000)),
        (
            "x @ 50 offset 60s",
            60_000,
            Some(50_000),
            (-15_000, -10_000),
        ),
    ] {
        let compiled = compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap_or_else(|error| panic!("{query}: {error}"));
        let requirement = &compiled.inputs[0].requirement;
        assert_eq!(requirement.offset_ms, expected_offset, "{query}");
        assert_eq!(requirement.at_ms, expected_anchor, "{query}");
        let (start, end, _) = requirement.raw_scan_bounds().unwrap();
        assert_eq!((start, end), expected_bounds, "{query}");
        if query.starts_with("timestamp(") && expected_anchor.is_some() {
            let NativeExpr::Instant { op, .. } = &compiled.tree.root().expr else {
                panic!("{query} did not compile as instant timestamp");
            };
            assert_eq!(op.offset_ms, expected_offset, "{query}");
            assert_eq!(op.at_ms, expected_anchor, "{query}");
        }
    }
}

#[test]
fn selector_requirements_capture_bounds_and_source_occurrence() {
    let compiled = compile_selectors(
        &eval_stmt("rate(x[10s] offset 2s @ 50)"),
        &|requirement| {
            assert_eq!(requirement.metric.as_deref(), Some("x"));
            assert_eq!(requirement.raw_scan_bounds()?, (38_000, 48_000, false));
            Ok(raw_schema())
        },
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .unwrap();
    assert_eq!(compiled.inputs.len(), 1);
    assert_eq!(compiled.inputs[0].source_index, 0);
    assert_eq!(
        compiled.inputs[0].requirement.timing,
        SelectorTiming::Window { range_ms: 10_000 }
    );
    let NativeExpr::Window { input, .. } = &compiled.tree.root().expr else {
        panic!("not window")
    };
    let NativeExpr::Input(bound) = &input.expr else {
        panic!("not input")
    };
    assert_eq!(bound.source_index, 0);
}

fn selector_requirement(
    grid_start_ms: i64,
    grid_end_ms: i64,
    offset_ms: i64,
    at_ms: Option<i64>,
    timing: SelectorTiming,
) -> SelectorRequirement {
    SelectorRequirement {
        metric: None,
        matchers: promql_parser::label::Matchers::empty(),
        offset_ms,
        at_ms,
        grid_start_ms,
        grid_end_ms,
        timing,
    }
}

#[test]
fn selector_scan_bounds_cover_instant_ranges_anchors_and_overflow() {
    let cases = [
        ("x", (-5_000, 95_000, false)), // 5s lookback, end before non-aligned statement end
        ("rate(x[10s])", (-10_000, 95_000, false)),
        ("rate(x[10s] offset 2s)", (-12_000, 93_000, false)),
        ("rate(x[10s] offset -2s)", (-8_000, 97_000, false)),
        ("rate(x[10s] @ start())", (-10_000, 0, false)),
        ("rate(x[10s] @ end())", (87_000, 97_000, false)),
        ("rate(x[10s] @ end() offset 2s)", (85_000, 95_000, false)),
    ];
    for (query, expected) in cases {
        let mut stmt = eval_stmt_over(query, Duration::from_secs(97));
        if query == "x" {
            stmt.lookback_delta = Duration::from_secs(5);
        }
        let tree = compile_selectors(
            &stmt,
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap_or_else(|e| panic!("{query}: {e}"));
        assert_eq!(
            tree.inputs[0].requirement.raw_scan_bounds().unwrap(),
            expected,
            "{query}"
        );
    }

    let mut zero = eval_stmt("x");
    zero.lookback_delta = Duration::ZERO;
    let tree = compile_selectors(
        &zero,
        &|_| Ok(raw_schema()),
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .unwrap();
    assert_eq!(
        tree.inputs[0].requirement.raw_scan_bounds().unwrap(),
        (0, 100_000, true)
    );

    for (req, edge) in [
        (
            selector_requirement(
                0,
                0,
                -1,
                Some(i64::MAX),
                SelectorTiming::Instant { lookback_ms: 0 },
            ),
            "end",
        ),
        (
            selector_requirement(
                i64::MIN,
                0,
                1,
                None,
                SelectorTiming::Instant { lookback_ms: 0 },
            ),
            "start",
        ),
        (
            selector_requirement(i64::MIN, 0, 0, None, SelectorTiming::Window { range_ms: 1 }),
            "duration",
        ),
    ] {
        assert!(
            matches!(req.raw_scan_bounds(), Err(DataFusionError::Plan(_))),
            "{edge}"
        );
    }
}

#[test]
fn subquery_selector_bounds_use_inner_grid_and_selector_offset() {
    for (query, expected_grid, expected_scan) in [
        (
            "max_over_time(rate(x[1m] offset 10s)[5m:30s])",
            (-270_000, 90_000),
            (-340_000, 80_000, false),
        ),
        (
            "max_over_time(rate(x[1m])[5m:30s] @ 1234 offset 1m)",
            (900_000, 1_170_000),
            (840_000, 1_170_000, false),
        ),
    ] {
        let compiled = compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap_or_else(|e| panic!("{query}: {e}"));
        let req = &compiled.inputs[0].requirement;
        assert_eq!(
            (req.grid_start_ms, req.grid_end_ms),
            expected_grid,
            "{query}"
        );
        assert_eq!(req.raw_scan_bounds().unwrap(), expected_scan, "{query}");
    }
}

#[test]
fn selector_binding_callback_error_is_returned_once() {
    let calls = std::cell::Cell::new(0);
    let error = compile_selectors(
        &eval_stmt("rate(x[1m])"),
        &|_| {
            calls.set(calls.get() + 1);
            Err(DataFusionError::Plan("bind failed".into()))
        },
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .unwrap_err();
    assert_eq!(calls.get(), 1);
    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("bind failed"));
}

#[test]
fn validate_inputs_rejects_misbound_occurrences_and_accepts_scalar_wrappers() {
    for query in ["x * 2", "2 * x"] {
        let compiled = compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .unwrap();
        validate_inputs(compiled.tree.root(), &compiled.inputs).unwrap();
    }
    let compiled = compile_selectors(
        &eval_stmt("x * 2"),
        &|_| Ok(raw_schema()),
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .unwrap();
    let original = compiled.inputs[0].clone();

    for records in [Vec::new(), vec![original.clone(), original.clone()]] {
        assert!(matches!(
            validate_inputs(compiled.tree.root(), &records),
            Err(DataFusionError::Internal(_))
        ));
    }
    let mut wrong_source_record = original.clone();
    wrong_source_record.source_index = 1;
    assert!(validate_inputs(compiled.tree.root(), &[wrong_source_record]).is_err());

    let scalar_output = compiled.tree.root().output.clone();
    fn set_input_source(node: &mut NativeNode, source: usize) {
        match &mut node.expr {
            NativeExpr::Input(bound) => bound.source_index = source,
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Vector { input, .. }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::VectorOf { input }
            | NativeExpr::ScalarOf { input } => set_input_source(input, source),
            NativeExpr::Binary { left, right, .. }
            | NativeExpr::Set { left, right, .. }
            | NativeExpr::Info {
                base: left,
                info: right,
                ..
            } => {
                set_input_source(left, source);
                set_input_source(right, source);
            }
            NativeExpr::ParameterizedTopK {
                input, parameter, ..
            }
            | NativeExpr::ParameterizedAggregate {
                input, parameter, ..
            } => {
                set_input_source(input, source);
                set_input_source(parameter, source);
            }
            NativeExpr::ParameterizedHistogram {
                input, parameters, ..
            } => {
                set_input_source(input, source);
                for parameter in parameters {
                    set_input_source(parameter, source);
                }
            }
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            } => {
                set_input_source(input, source);
                for parameter in parameters {
                    set_input_source(parameter, source);
                }
            }
            NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {}
        }
    }
    let mut wrong_bound_source = compiled.tree.root().clone();
    set_input_source(&mut wrong_bound_source, 1);
    assert!(validate_inputs(&wrong_bound_source, std::slice::from_ref(&original)).is_err());

    let wrong_schema = Arc::new(SeriesSchema {
        fields: Arc::clone(&original.schema.fields),
        eval_grid: None,
    });
    let mut wrong_record = original.clone();
    wrong_record.schema = wrong_schema;
    assert!(validate_inputs(compiled.tree.root(), &[wrong_record]).is_err());
    let mut wrong_ordering = original;
    wrong_ordering.ordering = SeriesOrdering::Contiguous;
    assert!(validate_inputs(compiled.tree.root(), &[wrong_ordering]).is_err());

    // A scalar is not an input leaf and must not consume an input record.
    let scalar = NativeNode {
        expr: NativeExpr::Scalar(promql::native::plan::BoundScalar { value: 2.0 }),
        output: scalar_output,
    };
    assert!(validate_inputs(&scalar, &[]).is_ok());
}

#[test]
fn validate_inputs_accepts_distinct_occurrences_sharing_a_schema() {
    let schema = raw_schema();
    let input = |source_index| NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index,
            output: NativeType::Samples(Arc::clone(&schema)),
            ordering: SeriesOrdering::KeyThenTime,
            time_unit: Default::default(),
        }),
        output: NativeType::Samples(Arc::clone(&schema)),
    };
    let compiled = compile_selectors(
        &eval_stmt("x"),
        &|_| Ok(Arc::clone(&schema)),
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .unwrap();
    let scalar_tree = compile_selectors(
        &eval_stmt("x + 1"),
        &|_| Ok(Arc::clone(&schema)),
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .unwrap();
    let NativeExpr::Binary { matching, .. } = &scalar_tree.tree.root().expr else {
        panic!("not binary")
    };
    let tree = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(input(0)),
            right: Box::new(input(1)),
            matching: matching.clone(),
        },
        output: NativeType::Samples(Arc::clone(&schema)),
    };
    let requirements = [0, 1].map(|source_index| CompiledInput {
        source_index,
        requirement: compiled.inputs[0].requirement.clone(),
        schema: Arc::clone(&schema),
        ordering: SeriesOrdering::KeyThenTime,
    });
    validate_inputs(&tree, &requirements).unwrap();
    let mut wrong = requirements;
    wrong[1].source_index = 0;
    assert!(validate_inputs(&tree, &wrong).is_err());
}

#[test]
fn selector_binding_callback_receives_matchers_once() {
    let calls = std::cell::Cell::new(0);
    let compiled = compile_selectors(
        &eval_stmt("rate(x{job=\"api\"}[1m])"),
        &|requirement| {
            calls.set(calls.get() + 1);
            assert_eq!(requirement.metric.as_deref(), Some("x"));
            assert_eq!(requirement.matchers.matchers.len(), 1);
            assert_eq!(requirement.matchers.matchers[0].name, "job");
            assert_eq!(requirement.matchers.matchers[0].value, "api");
            Ok(raw_schema())
        },
        SeriesOrdering::KeyThenTime,
        Some(FieldKind::Float),
    )
    .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(compiled.inputs.len(), 1);
}

#[test]
fn an_aggregate_binds_the_kind_its_direct_input_answers_with() {
    let bound = |query: &str, source: Option<FieldKind>| {
        compile_selectors(
            &eval_stmt(query),
            &|_| Ok(raw_schema()),
            SeriesOrdering::KeyThenTime,
            source,
        )
        .map(|compiled| {
            let NativeExpr::Aggregate { op, .. } = &compiled.tree.root().expr else {
                panic!("`{query}` is not an aggregate");
            };
            op.field_kind
        })
    };
    for query in ["sum(h)", "avg(h)", "sum(rate(h[5m]))"] {
        assert_eq!(
            bound(query, Some(FieldKind::Histogram)).unwrap(),
            FieldKind::Histogram,
            "{query}"
        );
        assert_eq!(
            bound(query, Some(FieldKind::Float)).unwrap(),
            FieldKind::Float,
            "{query}"
        );
    }
    // Both answer floats built from histograms.
    for query in ["sum(count_over_time(h[5m]))", "sum(histogram_count(h))"] {
        assert_eq!(
            bound(query, Some(FieldKind::Histogram)).unwrap(),
            FieldKind::Float,
            "{query}"
        );
    }
    // An unnameable kind over float sources is a float.
    assert_eq!(
        bound("sum(clamp(h, 1, 2))", Some(FieldKind::Float)).unwrap(),
        FieldKind::Float
    );
    assert_eq!(bound("sum(vector(1))", None).unwrap(), FieldKind::Float);
    // Over histogram sources it is refused.
    let error = bound("sum(clamp(h, 1, 2))", Some(FieldKind::Histogram))
        .expect_err("an unnameable input over a histogram source must not bind");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{error:?}"
    );
}
