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

//! A nested binary and map tree answered through the public native API.
//!
//! This is an external integration test: it reaches the evaluator the way a consumer above this
//! crate does, through [`promql::native::execute`] and [`promql::native::plan`], and it builds the
//! evaluated chunks it feeds in itself instead of reading them from a scan. What it checks is that
//! `abs(A - (B / C))` is answered at the absolute grid positions both sides fill, under one unnamed
//! label set, however the three inputs are split up and interleaved.

use std::sync::Arc;

use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::Float64Array;
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use promql::evaluated::EvaluatedChunk;
use promql::map::MathOp;
use promql::native::execute::{NativeExecutionState, NativeExecutor};
use promql::native::metrics::NodePath;
use promql::native::plan::{BoundInput, BoundMap, NativeExpr, NativeNode, NativeTree, NativeType};
use promql::vector_match::{BinaryMatching, BinaryOp, Cardinality, MatchingLabels};

/// The statement's grid: five points one second apart, starting at zero.
fn grid() -> Arc<EvalGrid> {
    let spec = GridSpec::try_new(0, 1_000, 5).expect("a five-point grid");
    Arc::new(EvalGrid::try_new(spec).expect("the grid materialises its timestamps"))
}

/// The layout every node of the tree binds: one float field evaluated on `grid`.
fn evaluated_schema(grid: &Arc<EvalGrid>) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec { name: "v0".into() }]),
        eval_grid: Some(Arc::clone(grid)),
    })
}

/// `name{host="h1",job="x"}`, the label set all three inputs of the tree share.
fn metric(name: &str) -> MetricName {
    MetricName::try_new(
        Some(name.into()),
        vec![
            Label {
                name: "host".into(),
                value: "h1".into(),
            },
            Label {
                name: "job".into(),
                value: "x".into(),
            },
        ],
    )
    .expect("two distinct labels")
}

/// The runs of consecutive positions in `rows`, as the spans a chunk is addressed by.
///
/// A chunk carries only the positions it holds a value at, so an input with a hole in the middle
/// costs the runs it has and not a row per empty position.
fn spans(rows: &[(usize, f64)]) -> Vec<GridSpan> {
    let mut runs: Vec<(usize, usize)> = Vec::new();
    for &(position, _) in rows {
        match runs.last_mut() {
            Some((start, len)) if *start + *len == position => *len += 1,
            _ => runs.push((position, 1)),
        }
    }
    runs.into_iter()
        .map(|(start, len)| GridSpan::new(start, len))
        .collect()
}

/// One evaluated chunk of the series `name`, holding `rows` at their absolute grid positions.
///
/// The chunk closes its series when `is_last`, which is what tells a consumer no row follows.
fn chunk(
    schema: &SeriesSchema,
    name: &str,
    rows: &[(usize, f64)],
    is_last: bool,
) -> EvaluatedChunk {
    let grid = schema.eval_grid.clone().expect("the schema has no grid");
    let time = TimeAxis::from_grid_spans(grid, spans(rows)).expect("ordered runs on the grid");
    let values: Vec<f64> = rows.iter().map(|&(_, value)| value).collect();
    let samples = Samples::from_float(Float64Array::from(values));
    let series = Series::try_new(schema, metric(name), time, SeriesFields::one(samples))
        .expect("one field aligned with the axis");
    EvaluatedChunk::try_new(series, is_last).expect("the axis is addressed on the grid")
}

/// An evaluated source bound to `schema`'s grid and field layout.
fn source(schema: &Arc<SeriesSchema>) -> NativeNode {
    let output = NativeType::Samples(Arc::clone(schema));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// `abs(A - (B / C))`, with every node bound to `schema`'s grid and field layout.
///
/// The operands are matched on every label but `__name__` (`ignoring()` with no labels), one
/// series to one, so a point is decided exactly where both sides of an operator hold a value.
fn bound_tree(schema: &Arc<SeriesSchema>) -> NativeTree {
    let binary = |left: NativeNode, right: NativeNode, op: BinaryOp| NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: BinaryMatching {
                op,
                return_bool: false,
                labels: MatchingLabels::Ignoring(Vec::new()),
                cardinality: Cardinality::OneToOne,
            },
        },
        output: NativeType::Samples(Arc::clone(schema)),
    };
    let divide = binary(source(schema), source(schema), BinaryOp::Div);
    let subtract = binary(source(schema), divide, BinaryOp::Sub);
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(subtract),
            op: BoundMap {
                ops: vec![MathOp::Abs].into(),
            },
        },
        output: NativeType::Samples(Arc::clone(schema)),
    })
    .expect("the contract of every node in this tree holds")
}

/// Runs `tree` over `pushes`, in the order given, and returns what the root produced.
///
/// Nothing is handed out while the inputs are still arriving: a binary node needs both operands
/// whole, so the chunks it is given are retained and the root answers on [`NativeExecutor::finish`].
fn run(tree: &NativeTree, pushes: Vec<(&NodePath, EvaluatedChunk)>) -> Vec<EvaluatedChunk> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let metrics = ExecutionPlanMetricsSet::new();
    let state = NativeExecutionState::try_new(tree, Arc::clone(&pool), &metrics, 0)
        .expect("the runtime executes this tree");
    let mut executor = NativeExecutor::new(state);
    for (path, chunk) in pushes {
        executor
            .push_evaluated_at(path, chunk)
            .expect("the chunk is bound to the input it is pushed at");
        let pulled = executor.next_chunk().expect("the pull succeeds");
        assert!(
            pulled.is_none(),
            "answered at {path} before its input ended"
        );
    }
    let chunks = executor.finish().expect("every series closed");
    assert_eq!(executor.state().reserved_bytes(), 0, "nothing held");
    assert_eq!(pool.reserved(), 0, "a finished execution reserves nothing");
    chunks
}

/// Checks the one chunk the tree must answer with, position by position.
fn assert_addressed_result(chunks: &[EvaluatedChunk], grid: &Arc<EvalGrid>) {
    assert_eq!(chunks.len(), 1, "one series, closed by one chunk");
    let chunk = &chunks[0];
    assert!(chunk.is_last, "the chunk that answers closes the series");
    // Arithmetic and `abs` drop the metric name and keep the labels the match was made on.
    assert_eq!(chunk.series.metric.name(), None, "the result is unnamed");
    let mut tags = Vec::new();
    for label in chunk.series.metric.tags() {
        tags.push((label.name.as_ref(), label.value.as_ref()));
    }
    assert_eq!(tags, vec![("host", "h1"), ("job", "x")]);
    // The chunk is addressed on the statement's grid and holds a row only where a value was
    // decided: p1 has a value on the left side alone, and p3 on the right side alone.
    let axis = chunk.series.time.grid().expect("addressed on a grid");
    assert_eq!(axis.spec(), grid.spec());
    assert_eq!(chunk.series.fields.len(), 1, "one field");
    let field = &chunk.series.fields.as_slice()[0];
    let mut values = Vec::new();
    for (position, row) in chunk.series.time.positions() {
        match field.sample(row) {
            SampleRef::Float(value) => values.push((position, value)),
            _ => panic!("the row at {row} holds no float"),
        }
    }
    assert_eq!(values, vec![(0, 6.0), (2, 8.0), (4, 8.0)]);
}

/// `abs(A - (B / C))` through the public API, at the positions both operands fill.
///
/// The same result is asserted literally for each arrival order, and the two runs are never
/// compared with each other, so each one has to hold on its own.
#[test]
fn public_nested_binary_map_is_addressed() {
    let grid = grid();
    let schema = evaluated_schema(&grid);
    let tree = bound_tree(&schema);
    // The root is the map; its input is the subtraction, whose right input is the division.
    let minuend = NodePath::root().input();
    let a = minuend.left();
    let b = minuend.right().left();
    let c = minuend.right().right();

    // What each input holds: A has no value at p3, B none at p1 and C none at p1 or p3.
    let a_rows: &[(usize, f64)] = &[(0, -2.0), (1, 100.0), (2, 10.0), (4, -3.0)];
    let b_rows: &[(usize, f64)] = &[(0, 8.0), (2, 6.0), (3, 99.0), (4, 20.0)];
    let c_rows: &[(usize, f64)] = &[(0, 2.0), (2, 3.0), (4, 4.0)];

    // Each input arrives whole, as one closing chunk, in the order B, C, A.
    let whole = run(
        &tree,
        vec![
            (&b, chunk(&schema, "b", b_rows, true)),
            (&c, chunk(&schema, "c", c_rows, true)),
            (&a, chunk(&schema, "a", a_rows, true)),
        ],
    );
    assert_addressed_result(&whole, &grid);

    // The same values split up and interleaved: A only up to p1 first, B and C only at p0 first,
    // then the tails in the reverse order.
    let split = run(
        &tree,
        vec![
            (&b, chunk(&schema, "b", &b_rows[..1], false)),
            (&c, chunk(&schema, "c", &c_rows[..1], false)),
            (&a, chunk(&schema, "a", &a_rows[..2], false)),
            (&c, chunk(&schema, "c", &c_rows[1..], true)),
            (&a, chunk(&schema, "a", &a_rows[2..], true)),
            (&b, chunk(&schema, "b", &b_rows[1..], true)),
        ],
    );
    assert_addressed_result(&split, &grid);
}
