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
use promql::native::plan::{
    BoundInstant, BoundWindow, NativeExpr, NativeNode, NativeTree, NativeType,
};
use promql::range_eval::RangeFunction;
use promql_parser::parser::{EvalStmt, Expr as PromExpr, NumberLiteral};

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
        // A range window reads one series to its last chunk before the next, so a scan whose
        // chunks of different series interleave is refused instead of recorded onto an input the
        // window would fail on.
        (
            "an interleaved ordering, which a window cannot consume to its last chunk",
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
        // range of an unsupported query is not read, so it cannot turn a refusal into a plan error.
        (
            "an unsupported shape with an inverted range",
            EvalStmt {
                end: UNIX_EPOCH,
                start: UNIX_EPOCH + Duration::from_secs(100),
                ..eval_stmt("x + 1")
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

/// A shape outside the subset is refused, and the error says what was found so the query can be
/// rewritten rather than guessed at.
#[test]
fn an_unsupported_shape_is_rejected_and_names_what_it_found() {
    let cases = [
        ("x + 1", "the expression `x + 1`"),
        ("sum(x)", "the expression `sum(x)`"),
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
            "a call to `predict_linear`, which has no native range kernel",
        ),
        (
            "absent(x)",
            "a call to `absent`, which has no native range kernel",
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
