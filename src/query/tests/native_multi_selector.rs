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

//! A two-selector query compiled by the real compiler and answered by the real executor.
//!
//! This is an external integration test: the tree comes from
//! [`compile_selectors`], never from a hand-built node, and it is run through
//! [`promql::native::execute`] the way a consumer above these crates would run it. What it checks
//! is what per-selector binding exists for: `a + b` reads two inputs, each selector's samples are
//! fed at the position the compiler says that selector's input sits at, and the operation matches
//! the two vectors by the labels the query wrote.
//!
//! Every case asserts the series count, the label set of each output series and the value at every
//! grid point, because a case that only asserted "something came out" would pass under a wrong label
//! mapping. The grid is five points one second apart starting at the Unix epoch, and the statement's
//! lookback is five seconds, so an instant selection carries the last sample of the previous points
//! forward.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datatypes::arrow::array::Float64Array;
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, SampleRef, Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, SeriesLabels, SeriesOrdering};
use promql::evaluated::EvaluatedChunk;
use promql::native::execute::{NativeExecutionState, NativeExecutor};
use promql::native::plan::NativeExpr;
use query::promql::native_compile::SelectorRequirement;
use query::promql::native_compile::{CompiledTree, compile_selectors};

/// The layout every selector of these queries is fed in: one float field of raw samples.
fn raw_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::new([FieldSpec {
            name: "value".into(),
        }]),
        eval_grid: None,
    })
}

/// `query` over `0..=4000ms` on a `1000ms` grid with a five-second lookback, which is five points.
fn eval_stmt(query: &str) -> promql_parser::parser::EvalStmt {
    promql_parser::parser::EvalStmt {
        expr: promql_parser::parser::parse(query).unwrap(),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH + Duration::from_millis(4_000),
        interval: Duration::from_millis(1_000),
        lookback_delta: Duration::from_millis(5_000),
    }
}

/// `query` compiled by the real compiler, with its tree and the inputs it reads.
fn compile(query: &str) -> CompiledTree {
    let schema = raw_schema();
    let bindings = |requirement: &SelectorRequirement| {
        let field = requirement
            .matchers
            .matchers
            .iter()
            .find(|matcher| matcher.name == "__field__")
            .map_or("value", |matcher| matcher.value.as_str());
        if field == "value" {
            Ok(Arc::clone(&schema))
        } else {
            Ok(Arc::new(SeriesSchema {
                fields: Arc::new([FieldSpec { name: field.into() }]),
                eval_grid: None,
            }))
        }
    };
    compile_selectors(&eval_stmt(query), &bindings, SeriesOrdering::KeyThenTime)
        .unwrap_or_else(|error| panic!("`{query}` was refused: {error}"))
}

/// The metric `name` with `labels`, as a source series of the samples a scan would read.
fn metric(name: &str, labels: &[(&str, &str)]) -> SeriesLabels {
    let tags = labels
        .iter()
        .map(|(name, value)| Label {
            name: (*name).into(),
            value: (*value).into(),
        })
        .collect();
    SeriesLabels::try_new(Some(name.into()), tags).expect("distinct label names")
}

/// One raw chunk of `series`, holding `rows` at their source timestamps, closing the series.
///
/// The chunk is built against `schema`, the layout the compiler bound that selector to, so a chunk
/// the input would refuse cannot be pushed here by accident.
fn raw_chunk(
    schema: &SeriesSchema,
    key: &str,
    series: SeriesLabels,
    rows: &[(i64, f64)],
) -> RawSeriesChunk {
    let timestamps: Vec<i64> = rows.iter().map(|&(time, _)| time).collect();
    let values: Vec<f64> = rows.iter().map(|&(_, value)| value).collect();
    let fields = schema
        .fields
        .iter()
        .map(|_| Samples::from_float(Float64Array::from(values.clone())))
        .collect();
    RawSeriesChunk::try_new(
        schema,
        Buffer::from_slice_ref(key.as_bytes()),
        Some(series),
        RawTimestamps::try_new(ScalarBuffer::from(timestamps)).expect("increasing timestamps"),
        SeriesFields::try_from_vec(fields).unwrap(),
        true,
    )
    .expect("the chunk is bound to the schema it is built against")
}

/// Builds only source rows in the requirement's metric/matcher/time/field selection.
fn scan_rows(
    requirement: &SelectorRequirement,
    metric_name: &str,
    labels: &[(&str, &str)],
    rows: &[(i64, f64)],
) -> Vec<(i64, f64)> {
    if requirement
        .metric
        .as_deref()
        .is_some_and(|name| name != metric_name)
    {
        return Vec::new();
    }
    if !requirement.matchers.matchers.iter().all(|matcher| {
        let value = if matcher.name == "__name__" {
            metric_name
        } else {
            labels
                .iter()
                .find(|(name, _)| *name == matcher.name)
                .map_or("", |(_, value)| *value)
        };
        matcher.is_match(value)
    }) {
        return Vec::new();
    }
    let (start, end, inclusive_start) = requirement.raw_scan_bounds().unwrap();
    rows.iter()
        .copied()
        .filter(|(time, _)| (*time > start || (inclusive_start && *time == start)) && *time <= end)
        .collect()
}

/// Scans the small fixture source independently for each compiled input and pushes only selected rows.
fn scan_push(
    compiled: &CompiledTree,
    input: usize,
    key: &str,
    metric_name: &str,
    labels: &[(&str, &str)],
    rows: &[(i64, f64)],
) -> Option<(usize, RawSeriesChunk)> {
    let input_spec = &compiled.inputs[input];
    let rows = scan_rows(&input_spec.requirement, metric_name, labels, rows);
    (!rows.is_empty()).then(|| {
        (
            input,
            raw_chunk(
                input_spec.schema.as_ref(),
                key,
                metric(
                    metric_name,
                    &labels
                        .iter()
                        .copied()
                        .filter(|(name, _)| *name != "__field__")
                        .collect::<Vec<_>>(),
                ),
                &rows,
            ),
        )
    })
}

/// Runs `compiled` over `pushes`, each naming the input its chunk belongs to by that input's index.
///
/// A binary node retains both operands and decides nothing until its input ends, so every pull
/// before [`NativeExecutor::finish`] has to answer with nothing; a pull that answered earlier would
/// mean a node computed over half a vector.
fn run(compiled: &CompiledTree, pushes: Vec<(usize, RawSeriesChunk)>) -> Vec<EvaluatedChunk> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let state = NativeExecutionState::try_new(&compiled.tree, Arc::clone(&pool))
        .expect("the runtime executes this tree");
    let mut input_ids = Vec::with_capacity(compiled.inputs.len());
    for input in compiled.inputs.iter() {
        let ids: Vec<_> = state.input_ids(input.source_index).collect();
        assert_eq!(
            ids.len(),
            1,
            "each compiled source has exactly one runtime input"
        );
        input_ids.push(ids[0]);
    }
    let mut executor = NativeExecutor::new(state);
    for (index, chunk) in pushes {
        executor
            .push_raw(input_ids[index], chunk)
            .expect("the chunk is pushed to its compiled input");
        let pulled = executor.next_chunk().expect("the pull succeeds");
        assert!(
            pulled.is_none(),
            "binary output arrived before both inputs ended: {pulled:?}"
        );
    }
    let chunks = executor.finish().expect("every series closed");
    assert_eq!(executor.state().reserved_bytes(), 0, "nothing held");
    assert_eq!(pool.reserved(), 0, "a finished execution reserves nothing");
    chunks
}

/// The label set a chunk's series is filed under, rendered with the metric name first.
fn label_set(chunk: &EvaluatedChunk) -> String {
    let metric = &chunk.series.metric;
    let mut parts = Vec::new();
    if let Some(name) = metric.name() {
        parts.push(format!("__name__=\"{name}\""));
    }
    for label in metric.tags() {
        parts.push(format!("{}=\"{}\"", label.name, label.value));
    }
    format!("{{{}}}", parts.join(", "))
}

/// What every output series holds, as its rendered label set and its value at each grid position.
///
/// A series may arrive as one chunk or as several: a chunk says which series it carries and closes
/// it, and how a node batches what it produces is not this test's to pin. Rows are therefore
/// appended to the entry for their label set, and a series split over two chunks reads as one.
fn results(chunks: &[EvaluatedChunk]) -> BTreeMap<String, Vec<(usize, f64)>> {
    let mut output: BTreeMap<String, Vec<(usize, f64)>> = BTreeMap::new();
    for chunk in chunks {
        let field = chunk
            .series
            .fields
            .as_slice()
            .first()
            .expect("the result carries the one field it was bound to");
        let entry = output.entry(label_set(chunk)).or_default();
        for (position, row) in chunk.series.time.positions() {
            match field.sample(row) {
                SampleRef::Float(value) => entry.push((position, value)),
                _ => panic!("the row at {row} holds no float"),
            }
        }
    }
    output
}

/// The value at every one of the five grid points.
fn every_point(value: f64) -> Vec<(usize, f64)> {
    (0..5).map(|position| (position, value)).collect()
}

/// `a + b` over two genuinely different inputs: different metric names, different matcher sets and
/// different samples. Each input is fed at the position its own selector was bound to, and the sum
/// is asserted point by point.
#[test]
fn two_selectors_are_summed_from_two_inputs() {
    let compiled = compile("a{job=\"x\"} + b{job=\"x\", instance=\"i1\"}");
    assert_eq!(
        compiled.inputs.len(),
        2,
        "`a + b` did not bind one input per selector"
    );
    assert_eq!(
        compiled.inputs[0].requirement.metric.as_deref(),
        Some("a"),
        "the first input is not the left selector's"
    );
    assert_eq!(
        compiled.inputs[1].requirement.metric.as_deref(),
        Some("b"),
        "the second input is not the right selector's"
    );
    // The two selectors asked for different series, so each input records its own matcher set.
    assert_eq!(
        compiled.inputs[0].requirement.matchers.matchers.len(),
        1,
        "the left input did not record the one matcher its selector wrote"
    );
    assert_eq!(
        compiled.inputs[1].requirement.matchers.matchers.len(),
        2,
        "the right input did not record the two matchers its selector wrote"
    );
    assert_ne!(
        compiled.inputs[0].source_index, compiled.inputs[1].source_index,
        "two selectors were bound to one position"
    );

    // `a` holds 1 at t=0 and 2 at t=2000, so an instant selection carries 1 forward to p0 and p1 and
    // 2 to p2..p4. `b` holds 10 at t=0 and 40 at t=4000: `[10, 10, 10, 10, 40]`.
    let chunks = run(
        &compiled,
        vec![
            (
                0,
                raw_chunk(
                    compiled.inputs[0].schema.as_ref(),
                    "a-job=x-instance=i1",
                    metric("a", &[("instance", "i1"), ("job", "x")]),
                    &[(0, 1.0), (2_000, 2.0)],
                ),
            ),
            (
                1,
                raw_chunk(
                    compiled.inputs[1].schema.as_ref(),
                    "b-job=x-instance=i1",
                    metric("b", &[("instance", "i1"), ("job", "x")]),
                    &[(0, 10.0), (4_000, 40.0)],
                ),
            ),
        ],
    );
    let output = results(&chunks);
    assert_eq!(output.len(), 1, "the sum is not one series: {output:?}");
    // Arithmetic drops the metric name and keeps the labels the match was made on.
    assert_eq!(
        output.get("{instance=\"i1\", job=\"x\"}"),
        Some(&vec![(0, 11.0), (1, 11.0), (2, 12.0), (3, 12.0), (4, 42.0)]),
        "the sum is not the values each side selected: {output:?}"
    );
}

/// `a / on(job) b` matches on `job` alone, so two series that differ in `instance` pair up. The same
/// inputs through `a / b` — the default matching, which matches on every label but `__name__` — pair
/// nothing, and that difference is what proves the `on` list reached the kernel.

#[test]
fn noncommutative_vector_operations_keep_left_right_values() {
    for (query, expected) in [("a / on() b", 0.5), ("a - on() b", -1.0)] {
        let compiled = compile(query);
        let chunks = run(
            &compiled,
            vec![
                (
                    0,
                    raw_chunk(
                        compiled.inputs[0].schema.as_ref(),
                        "left",
                        metric("a", &[]),
                        &[(0, 2.0)],
                    ),
                ),
                (
                    1,
                    raw_chunk(
                        compiled.inputs[1].schema.as_ref(),
                        "right",
                        metric("b", &[]),
                        &[(0, 4.0)],
                    ),
                ),
            ],
        );
        let output = results(&chunks);
        assert_eq!(output.len(), 1, "{query}: {output:?}");
        assert_eq!(
            output.values().next().unwrap(),
            &every_point(expected),
            "{query}"
        );
    }
}

#[test]
fn an_on_matching_pairs_series_the_default_matching_would_not() {
    // Both inputs are the same in both runs: only the compiled matching differs.
    let pushes = |compiled: &CompiledTree| {
        vec![
            (
                0,
                raw_chunk(
                    compiled.inputs[0].schema.as_ref(),
                    "a-job=x-instance=i1",
                    metric("a", &[("instance", "i1"), ("job", "x")]),
                    &[(0, 1.0), (2_000, 2.0)],
                ),
            ),
            (
                1,
                raw_chunk(
                    compiled.inputs[1].schema.as_ref(),
                    "b-job=x-instance=i2",
                    metric("b", &[("instance", "i2"), ("job", "x")]),
                    &[(0, 10.0), (4_000, 40.0)],
                ),
            ),
        ]
    };

    let matched = compile("a{job=\"x\"} / on(job) b{job=\"x\"}");
    let output = results(&run(&matched, pushes(&matched)));
    assert_eq!(output.len(), 1, "`on(job)` did not match: {output:?}");
    // `on` keeps only the labels it matched on, so `instance` is gone.
    assert_eq!(
        output.get("{job=\"x\"}"),
        Some(&vec![
            (0, 0.1),
            (1, 0.1),
            (2, 0.2),
            (3, 0.2),
            (4, 2.0 / 40.0),
        ]),
        "`on(job)` did not divide each side's value: {output:?}"
    );

    // The default matching matches on `instance` too, and the two sides disagree on it.
    let unmatched = compile("a{job=\"x\"} / b{job=\"x\"}");
    let output = results(&run(&unmatched, pushes(&unmatched)));
    assert!(
        output.is_empty(),
        "the default matching paired series that differ in `instance`: {output:?}"
    );
}

/// `group_left(env)` reuses one right series for two left series and copies `env` from the right
/// side into each result, which is the cardinality a one-to-one matching cannot express.
#[test]
fn a_group_left_reuses_the_one_side_and_copies_its_label() {
    let compiled = compile("a{job=\"x\"} / on(job) group_left(env) b{job=\"x\"}");
    let chunks = run(
        &compiled,
        vec![
            (
                0,
                raw_chunk(
                    compiled.inputs[0].schema.as_ref(),
                    "a-job=x-instance=i1",
                    metric("a", &[("instance", "i1"), ("job", "x")]),
                    &[(0, 1.0)],
                ),
            ),
            (
                0,
                raw_chunk(
                    compiled.inputs[0].schema.as_ref(),
                    "a-job=x-instance=i3",
                    metric("a", &[("instance", "i3"), ("job", "x")]),
                    &[(0, 2.0)],
                ),
            ),
            (
                1,
                raw_chunk(
                    compiled.inputs[1].schema.as_ref(),
                    "b-env=prod-job=x",
                    metric("b", &[("env", "prod"), ("job", "x")]),
                    &[(0, 10.0)],
                ),
            ),
        ],
    );
    let output = results(&chunks);
    // One right series matched both left series, so both are answered, each keeping its own
    // `instance` and gaining the copied `env`.
    assert_eq!(
        output.len(),
        2,
        "the left series was not reused: {output:?}"
    );
    assert_eq!(
        output.get("{env=\"prod\", instance=\"i1\", job=\"x\"}"),
        Some(&every_point(0.1)),
        "the first left series did not keep its own labels beside the copied one: {output:?}"
    );
    assert_eq!(
        output.get("{env=\"prod\", instance=\"i3\", job=\"x\"}"),
        Some(&every_point(0.2)),
        "the second left series did not keep its own labels beside the copied one: {output:?}"
    );
}

/// `fill_right(100)` stands in for a right sample a left series found no match for: `a * on(job) b`
/// drops the left series `job="y"`, and the filled operation answers it from the fill value.
#[test]
fn a_fill_answers_a_left_series_that_found_no_match() {
    let pushes = |compiled: &CompiledTree| {
        vec![
            (
                0,
                raw_chunk(
                    compiled.inputs[0].schema.as_ref(),
                    "a-job=x",
                    metric("a", &[("job", "x")]),
                    &[(0, 2.0)],
                ),
            ),
            (
                0,
                raw_chunk(
                    compiled.inputs[0].schema.as_ref(),
                    "a-job=y",
                    metric("a", &[("job", "y")]),
                    &[(0, 3.0)],
                ),
            ),
            (
                1,
                raw_chunk(
                    compiled.inputs[1].schema.as_ref(),
                    "b-job=x",
                    metric("b", &[("job", "x")]),
                    &[(0, 10.0)],
                ),
            ),
        ]
    };

    let unfilled = compile("a * on(job) b");
    let output = results(&run(&unfilled, pushes(&unfilled)));
    assert_eq!(
        output.len(),
        1,
        "a series with no match was answered without a fill: {output:?}"
    );
    assert_eq!(
        output.get("{job=\"x\"}"),
        Some(&every_point(20.0)),
        "the matched series is not the product of the two sides: {output:?}"
    );

    let filled = compile("a * on(job) fill_right(100) b");
    let output = results(&run(&filled, pushes(&filled)));
    assert_eq!(
        output.len(),
        2,
        "`fill_right(100)` did not answer the unmatched series: {output:?}"
    );
    assert_eq!(
        output.get("{job=\"x\"}"),
        Some(&every_point(20.0)),
        "the fill changed the series that did match: {output:?}"
    );
    assert_eq!(
        output.get("{job=\"y\"}"),
        Some(&every_point(300.0)),
        "the filled series is not `3 * 100`: {output:?}"
    );
}

/// `group_right(env)` makes the right operand the many side and copies `env` from the left one, so
/// one left series answers two right series.
#[test]
fn group_right_answers_each_right_series_and_copies_its_label() {
    let pushes = |compiled: &CompiledTree| {
        vec![
            (
                0,
                raw_chunk(
                    compiled.inputs[0].schema.as_ref(),
                    "a-env=prod-job=x",
                    metric("a", &[("env", "prod"), ("job", "x")]),
                    &[(0, 2.0)],
                ),
            ),
            (
                1,
                raw_chunk(
                    compiled.inputs[1].schema.as_ref(),
                    "b-job=x-instance=i1",
                    metric("b", &[("instance", "i1"), ("job", "x")]),
                    &[(0, 10.0)],
                ),
            ),
            (
                1,
                raw_chunk(
                    compiled.inputs[1].schema.as_ref(),
                    "b-job=x-instance=i2",
                    metric("b", &[("instance", "i2"), ("job", "x")]),
                    &[(0, 20.0)],
                ),
            ),
        ]
    };

    let compiled = compile("a{job=\"x\"} / on(job) group_right(env) b");
    let output = results(&run(&compiled, pushes(&compiled)));
    assert_eq!(
        output.len(),
        2,
        "the many side was not answered once per right series: {output:?}"
    );
    assert_eq!(
        output.get("{env=\"prod\", instance=\"i1\", job=\"x\"}"),
        Some(&every_point(0.2)),
        "the first right series is not `2 / 10`: {output:?}"
    );
    assert_eq!(
        output.get("{env=\"prod\", instance=\"i2\", job=\"x\"}"),
        Some(&every_point(0.1)),
        "the second right series is not `2 / 20`: {output:?}"
    );
}

/// `fill_left` and `fill_right` fill the side written in the query, also under `group_right`, where
/// the many side is the right one: prometheus/prometheus#19939. The two values differ so a swap is
/// visible. `fill_left(5)` answers a right series with no left counterpart as `5 - 4`, and
/// `fill_right(7)` answers a left series with no right counterpart as `2 - 7`; the pre-fix kernel
/// had the two trade places, answering `7 - 4` and `2 - 5`.
#[test]
fn group_right_fills_the_side_the_query_wrote() {
    let compiled = compile("a{job=\"x\"} - on(job) group_right() fill_left(5) fill_right(7) b");
    let pushes = vec![
        (
            0,
            raw_chunk(
                compiled.inputs[0].schema.as_ref(),
                "a-job=x",
                metric("a", &[("job", "x")]),
                &[(0, 2.0)],
            ),
        ),
        // This left series has no right counterpart at all, so `fill_right` answers every point.
        (
            0,
            raw_chunk(
                compiled.inputs[0].schema.as_ref(),
                "a-job=z",
                metric("a", &[("job", "z")]),
                &[(0, 2.0)],
            ),
        ),
        (
            1,
            raw_chunk(
                compiled.inputs[1].schema.as_ref(),
                "b-job=x-instance=i1",
                metric("b", &[("instance", "i1"), ("job", "x")]),
                &[(0, 10.0)],
            ),
        ),
        // This right series has no left counterpart, so `fill_left` answers every point.
        (
            1,
            raw_chunk(
                compiled.inputs[1].schema.as_ref(),
                "b-job=y-instance=i2",
                metric("b", &[("instance", "i2"), ("job", "y")]),
                &[(0, 4.0)],
            ),
        ),
    ];
    let output = results(&run(&compiled, pushes));
    assert_eq!(output.len(), 3, "every series is answered: {output:?}");
    assert_eq!(
        output.get("{instance=\"i1\", job=\"x\"}"),
        Some(&every_point(-8.0)),
        "the matched series is not `2 - 10`: {output:?}"
    );
    assert_eq!(
        output.get("{job=\"z\"}"),
        Some(&every_point(-5.0)),
        "`fill_right(7)` did not fill the right side the query wrote: {output:?}"
    );
    assert_eq!(
        output.get("{instance=\"i2\", job=\"y\"}"),
        Some(&every_point(1.0)),
        "`fill_left(5)` did not fill the left side the query wrote: {output:?}"
    );
}

#[test]
fn requirements_drive_scans_for_repeated_metric_nested_windows_and_fields() {
    let stmt =
        eval_stmt("abs(rate(a{__field__=\"value\"}[2s])) + on() rate(a{__field__=\"other\"}[3s])");
    let compiled = compile_selectors(
        &stmt,
        &|requirement| {
            let field = requirement
                .matchers
                .matchers
                .iter()
                .find(|matcher| matcher.name == "__field__")
                .map_or("value", |matcher| matcher.value.as_str());
            Ok(Arc::new(SeriesSchema {
                fields: Arc::new([FieldSpec { name: field.into() }]),
                eval_grid: None,
            }))
        },
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    assert_eq!(compiled.inputs.len(), 2);
    assert_eq!(
        compiled.inputs[0].requirement.metric,
        compiled.inputs[1].requirement.metric
    );
    assert_ne!(
        compiled.inputs[0].requirement.raw_scan_bounds().unwrap(),
        compiled.inputs[1].requirement.raw_scan_bounds().unwrap()
    );
    // Each selector binds only its requested field before its selected scan rows reach the executor.
    // The two fields rise at different rates, `2` and `10` per second, so a sample that reached the
    // other selector's window would not read as the same answer.
    let mut pushes = Vec::new();
    for (field, rows) in [
        (
            "value",
            &[(0, 1.0), (1_000, 3.0), (2_000, 5.0), (3_000, 7.0)][..],
        ),
        (
            "other",
            &[(0, 10.0), (1_000, 20.0), (2_000, 30.0), (3_000, 40.0)][..],
        ),
    ] {
        for index in 0..compiled.inputs.len() {
            if let Some(push) = scan_push(&compiled, index, "a", "a", &[("__field__", field)], rows)
            {
                pushes.push(push);
            }
        }
    }
    assert_eq!(
        pushes.len(),
        2,
        "each field requirement selects only its own source field"
    );
    assert_eq!(pushes[0].0, 0);
    assert_eq!(pushes[1].0, 1);
    assert!(pushes.iter().all(|(_, chunk)| chunk.fields.len() == 1));
    let output_chunks = run(&compiled, pushes);
    let output = results(&output_chunks);
    assert_eq!(
        output.len(),
        1,
        "the same-metric windows did not match: {output:?}"
    );
    // The scan behind the two-second window sits under `abs`, which maps over it, and the one
    // behind the three-second window under the window itself: each is read at its own path.
    assert_eq!(compiled.inputs[0].source_index, 0);
    assert_eq!(compiled.inputs[1].source_index, 1);
    assert_eq!(compiled.inputs[0].schema.fields[0].name.as_ref(), "value");
    assert_eq!(compiled.inputs[1].schema.fields[0].name.as_ref(), "other");
    assert_eq!(output_chunks[0].series.fields.len(), 1);
    let root_output = compiled
        .tree
        .root()
        .output
        .samples()
        .expect("the addition reports evaluated samples");
    // The operation pairs its operands' fields by position and names the output after the left
    // side, which is the field the two-second window read.
    assert_eq!(root_output.fields[0].name.as_ref(), "value");
    // Each window answers its own source rows: the two-second window over a `2`-per-second field
    // answers `1.5`, `2`, `2` and the three-second window over a `10`-per-second field answers `5`,
    // `10`, `10`, so their sum is `6.5`, `12`, `12`.
    assert_eq!(
        output.values().next().unwrap(),
        &vec![(1, 6.5), (2, 12.0), (3, 12.0)],
        "the two separately scanned rates were not added: {output:?}"
    );

    let nested = compile("(a + b) / c");
    let rows_a = [
        (0, 2.0),
        (1_000, 2.0),
        (2_000, 2.0),
        (3_000, 2.0),
        (4_000, 2.0),
    ];
    let rows_b = [
        (0, 6.0),
        (1_000, 6.0),
        (2_000, 6.0),
        (3_000, 6.0),
        (4_000, 6.0),
    ];
    let rows_c = [
        (0, 4.0),
        (1_000, 4.0),
        (2_000, 4.0),
        (3_000, 4.0),
        (4_000, 4.0),
    ];
    let pushes = [
        scan_push(&nested, 0, "a", "a", &[], &rows_a).unwrap(),
        scan_push(&nested, 1, "b", "b", &[], &rows_b).unwrap(),
        scan_push(&nested, 2, "c", "c", &[], &rows_c).unwrap(),
    ];
    let output = results(&run(&nested, pushes.into()));
    assert_eq!(
        output.len(),
        1,
        "nested/repeated selectors did not match: {output:?}"
    );
    assert_eq!(output.values().next().unwrap(), &every_point(2.0));
}

#[test]
fn compiled_comparisons_preserve_filter_and_bool_semantics() {
    for (query, expected_bool) in [("a > b", false), ("a > bool b", true)] {
        let compiled = compile(query);
        let NativeExpr::Binary { matching, .. } = &compiled.tree.root().expr else {
            panic!("`{query}` did not compile to a binary node");
        };
        assert_eq!(
            matching.return_bool, expected_bool,
            "comparison mode for `{query}`"
        );
        let requirement = &compiled.inputs[0].requirement;
        assert_eq!(requirement.metric.as_deref(), Some("a"));
        let (start, end, inclusive) = requirement.raw_scan_bounds().unwrap();
        assert!(start <= 0 && end >= 4_000);
        assert!(!inclusive);
        let rows_a = [
            (0, 1.0),
            (1_000, 2.0),
            (2_000, 3.0),
            (3_000, 4.0),
            (4_000, 5.0),
        ];
        let rows_b = [
            (0, 0.0),
            (1_000, 1.0),
            (2_000, 4.0),
            (3_000, 2.0),
            (4_000, 6.0),
        ];
        let left = scan_push(&compiled, 0, "a", "a", &[], &rows_a).unwrap();
        let right = scan_push(&compiled, 1, "b", "b", &[], &rows_b).unwrap();
        let output = results(&run(&compiled, vec![left, right]));
        let expected = if expected_bool {
            BTreeMap::from([(
                "{}".to_string(),
                vec![(0, 1.0), (1, 1.0), (2, 0.0), (3, 1.0), (4, 0.0)],
            )])
        } else {
            BTreeMap::from([(
                "{__name__=\"a\"}".to_string(),
                vec![(0, 1.0), (1, 2.0), (3, 4.0)],
            )])
        };
        assert_eq!(
            output, expected,
            "wrong vector comparison result for `{query}`"
        );
    }
}

#[test]
fn ignoring_matching_uses_compiled_selectors_and_scanned_inputs() {
    let compiled = compile("a{job=\"x\"} / ignoring(instance) b{job=\"x\"}");
    let NativeExpr::Binary { matching, .. } = &compiled.tree.root().expr else {
        panic!("the expression did not compile to a binary node");
    };
    assert_eq!(
        matching.labels,
        promql::vector_match::MatchingLabels::Ignoring(vec![Arc::from("instance")])
    );

    let rows = [(0, 1.0), (2_000, 2.0), (4_000, 9.0), (5_000, 12.0)];
    let left = scan_push(
        &compiled,
        0,
        "left",
        "a",
        &[("job", "x"), ("instance", "i1")],
        &rows,
    )
    .unwrap();
    let right = scan_push(
        &compiled,
        1,
        "right",
        "b",
        &[("job", "x"), ("instance", "i2")],
        &rows,
    )
    .unwrap();
    assert_eq!(left.0, 0);
    assert_eq!(right.0, 1);
    assert_eq!(left.1.time.len(), 3);
    assert_eq!(right.1.time.len(), 3);
    let output = results(&run(&compiled, vec![left, right]));
    assert_eq!(
        output.len(),
        1,
        "ignoring matching did not pair differing instance labels: {output:?}"
    );
    assert_eq!(
        output.values().next().unwrap(),
        &vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)],
        "ignoring match output did not divide the vectors: {output:?}"
    );
    assert!(scan_push(&compiled, 0, "wrong-metric", "b", &[("job", "x")], &rows).is_none());
    assert!(scan_push(&compiled, 0, "wrong-matcher", "a", &[("job", "y")], &rows).is_none());
}

#[test]
fn historical_range_sources_are_not_clipped_to_the_statement_start() {
    let stmt = promql_parser::parser::EvalStmt {
        expr: promql_parser::parser::parse("sum_over_time(a[2s]) + sum_over_time(a[3s])").unwrap(),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH,
        interval: Duration::from_secs(1),
        lookback_delta: Duration::from_secs(5),
    };
    let compiled =
        compile_selectors(&stmt, &|_| Ok(raw_schema()), SeriesOrdering::KeyThenTime).unwrap();
    assert_eq!(compiled.inputs.len(), 2);
    let rows = [(-2_500, 10.0), (-1_500, 20.0), (-500, 30.0)];
    let short = scan_push(&compiled, 0, "a-short", "a", &[], &rows).unwrap();
    let long = scan_push(&compiled, 1, "a-long", "a", &[], &rows).unwrap();
    assert_eq!(short.1.time.len(), 2);
    assert_eq!(long.1.time.len(), 3);
    let output = results(&run(&compiled, vec![short, long]));
    assert_eq!(
        output,
        BTreeMap::from([("{}".to_string(), vec![(0, 110.0)])]),
        "historical range samples were clipped or a selector reused the other window"
    );
}
