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

//! What the runtime has to get right: each layer's grid, per-node accounting, rejection and
//! release.

use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue};
use datatypes::arrow::array::Float64Array;
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpec, TimeAxis};
use datatypes::series::{MetricName, SeriesOrdering};

use super::{NativeExecutionState, NativeExecutor, map_chunk};
use crate::evaluated::EvaluatedChunk;
use crate::map::MathOp;
use crate::native::metrics::{NodeMetric, NodePath};
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundWindow, NativeExpr, NativeNode, NativeTree, NativeType,
};
use crate::range_eval::{RangeEvaluator, RangeFunction, RangeWindow};
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, MatchingLabels};

fn grid(start: i64, step: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start, step, len).unwrap()).unwrap())
}

fn fields() -> Arc<[FieldSpec]> {
    Arc::from([FieldSpec { name: "v0".into() }])
}

fn raw_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: fields(),
        eval_grid: None,
    })
}

fn evaluated_schema(grid: Arc<EvalGrid>) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: fields(),
        eval_grid: Some(grid),
    })
}

fn evaluated(grid: Arc<EvalGrid>) -> NativeType {
    NativeType::Samples(evaluated_schema(grid))
}

fn metric() -> MetricName {
    MetricName::try_new(Some("requests".into()), Vec::new()).unwrap()
}

/// The raw source every supported tree starts from.
fn input() -> NativeNode {
    let output = NativeType::Samples(raw_schema());
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

fn window(inner: NativeNode, output_grid: Arc<EvalGrid>) -> NativeNode {
    window_with(
        inner,
        RangeFunction::LastOverTime,
        300_000,
        None,
        output_grid,
    )
}

fn window_with(
    inner: NativeNode,
    function: RangeFunction,
    range_ms: i64,
    input_grid: Option<Arc<EvalGrid>>,
    output_grid: Arc<EvalGrid>,
) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function,
                window: RangeWindow {
                    range_ms,
                    offset_ms: 0,
                    anchor_ms: None,
                },
                input_grid,
                output_grid: Arc::clone(&output_grid),
            },
        },
        output: evaluated(output_grid),
    }
}

fn map(inner: NativeNode, ops: Vec<MathOp>) -> NativeNode {
    let output = inner.output.grid().expect("a map needs a grid").clone();
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap { ops: ops.into() },
        },
        output: evaluated(output),
    }
}

fn scalar(value: f64, grid: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(grid),
    }
}

fn raw_chunk(
    schema: &SeriesSchema,
    timestamps: &[i64],
    values: &[f64],
    is_last: bool,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        schema,
        Buffer::from_slice_ref(b"series-a"),
        Some(metric()),
        RawTimestamps::try_new(ScalarBuffer::from(timestamps.to_vec())).unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(values.to_vec()))),
        is_last,
    )
    .unwrap()
}

fn pool() -> Arc<dyn MemoryPool> {
    Arc::new(GreedyMemoryPool::new(1 << 20))
}

fn executor(
    tree: &NativeTree,
    pool: Arc<dyn MemoryPool>,
    set: &ExecutionPlanMetricsSet,
) -> NativeExecutor {
    NativeExecutor::new(NativeExecutionState::try_new(tree, pool, set, 0).unwrap())
}

/// Pulls until the tree has nothing settled, then closes it.
fn run(executor: &mut NativeExecutor) -> Vec<EvaluatedChunk> {
    let mut out = Vec::new();
    while let Some(chunk) = executor.next_chunk().unwrap() {
        out.push(chunk);
    }
    out.extend(executor.finish().unwrap());
    out
}

fn sample_values(series: &Series) -> Vec<Option<f64>> {
    let field = series.fields.get(0).expect("these series have one field");
    field
        .iter()
        .map(|sample| match sample {
            SampleRef::Float(value) => Some(value),
            _ => None,
        })
        .collect()
}

fn series_start(series: &Series) -> usize {
    match &series.time {
        TimeAxis::Grid(range) => range.start(),
        _ => panic!("an evaluated series is addressed on a grid"),
    }
}

/// The absolute start and the values of every chunk, which is what addressing means here.
fn addressed(chunks: &[EvaluatedChunk]) -> Vec<(usize, Vec<Option<f64>>)> {
    chunks
        .iter()
        .map(|chunk| (chunk.range().start(), sample_values(&chunk.series)))
        .collect()
}

fn count(set: &ExecutionPlanMetricsSet, path: &NodePath, metric: &str) -> usize {
    let name = path.metric_name(metric);
    match set.clone_inner().sum_by_name(&name) {
        Some(MetricValue::Count { count, .. }) => count.value(),
        other => panic!("no count named {name}: {other:?}"),
    }
}

fn gauge(set: &ExecutionPlanMetricsSet, name: &str) -> usize {
    match set.clone_inner().sum_by_name(name) {
        Some(MetricValue::Gauge { gauge, .. }) => gauge.value(),
        other => panic!("no gauge named {name}: {other:?}"),
    }
}

/// A window over raw input evaluates onto its own output grid, and its values are the evaluator's.
///
/// The grid is compared by allocation identity, so a window that evaluated on a copy of its
/// output grid — the same timestamps, a different grid — fails here even though every value and
/// every timestamp would still look right.
#[test]
fn a_window_over_raw_input_evaluates_onto_its_own_grid() {
    let out_grid = grid(0, 60_000, 4);
    let tree = NativeTree::try_new(window(input(), Arc::clone(&out_grid))).unwrap();
    let schema = raw_schema();
    let chunk = raw_chunk(
        &schema,
        &[0, 60_000, 120_000, 180_000, 240_000],
        &[1.0, 2.0, 3.0, 4.0, 5.0],
        true,
    );

    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution.push_raw(chunk.clone()).unwrap();
    let out = run(&mut execution);

    // The same samples, evaluated by the evaluator this runtime wraps.
    let mut evaluator = RangeEvaluator::try_new(
        &schema,
        Arc::clone(&out_grid),
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::LastOverTime,
    )
    .unwrap();
    let expected: Vec<(usize, Vec<Option<f64>>)> = evaluator
        .push(&chunk)
        .unwrap()
        .iter()
        .map(|series| (series_start(series), sample_values(series)))
        .collect();
    evaluator.finish().unwrap();

    assert_eq!(out.len(), 1, "one closed series, one chunk");
    for chunk in &out {
        assert!(
            Arc::ptr_eq(chunk.range().grid(), &out_grid),
            "the chunk is addressed on a grid that is not the window's output grid"
        );
        assert_eq!(chunk.range().grid().spec(), out_grid.spec());
        assert!(chunk.is_last);
    }
    assert_eq!(
        addressed(&out),
        vec![(0, vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)])]
    );
    assert_eq!(addressed(&out), expected);
}

/// `Window -> Map -> Window` keeps the inner grid inside and the outer grid outside.
///
/// The inner window runs on a 60 s grid and the outer one on a 90 s grid offset by 30 s, so a
/// layer that resampled onto its consumer's grid would change the addressing of every chunk and
/// the values of the last one.
#[test]
fn a_window_map_window_chain_keeps_each_layer_on_its_own_grid() {
    let inner_grid = grid(0, 60_000, 5);
    let outer_grid = grid(30_000, 90_000, 3);
    let inner = || {
        window_with(
            input(),
            RangeFunction::LastOverTime,
            120_000,
            None,
            Arc::clone(&inner_grid),
        )
    };
    let mapped = || map(inner(), vec![MathOp::Abs]);
    let chain = || {
        window_with(
            map(inner(), vec![MathOp::Abs]),
            RangeFunction::LastOverTime,
            240_000,
            Some(Arc::clone(&inner_grid)),
            Arc::clone(&outer_grid),
        )
    };
    let schema = raw_schema();
    let chunk = raw_chunk(
        &schema,
        &[0, 60_000, 120_000, 180_000, 240_000],
        &[-1.0, -2.0, -3.0, -4.0, -5.0],
        true,
    );

    // Down to the map, on its own: the map's chunks stay addressed on the inner window's grid.
    let map_tree = NativeTree::try_new(mapped()).unwrap();
    let mut map_execution = executor(&map_tree, pool(), &ExecutionPlanMetricsSet::new());
    map_execution.push_raw(chunk.clone()).unwrap();
    let mapped_chunks = run(&mut map_execution);
    assert_eq!(mapped_chunks.len(), 1);
    for chunk in &mapped_chunks {
        assert!(
            Arc::ptr_eq(chunk.range().grid(), &inner_grid),
            "the map handed over a chunk that is not addressed on the inner grid"
        );
    }
    assert_eq!(mapped_chunks[0].range().len(), 5);
    assert_eq!(
        addressed(&mapped_chunks),
        vec![(
            0,
            vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
        )]
    );

    // The whole chain: only the outer window's grid leaves the tree.
    let chain_tree = NativeTree::try_new(chain()).unwrap();
    let mut chain_execution = executor(&chain_tree, pool(), &ExecutionPlanMetricsSet::new());
    chain_execution.push_raw(chunk.clone()).unwrap();
    let out = run(&mut chain_execution);
    assert_eq!(out.len(), 1);
    for chunk in &out {
        assert!(
            Arc::ptr_eq(chunk.range().grid(), &outer_grid),
            "the chain handed over a chunk that is not addressed on the outer grid"
        );
    }
    assert_eq!(out[0].range().len(), 3);
    assert_eq!(
        addressed(&out),
        vec![(0, vec![Some(1.0), Some(3.0), Some(4.0)])]
    );

    // The same chain driven by hand, one kernel at a time.
    let mut inner_evaluator = RangeEvaluator::try_new(
        &schema,
        Arc::clone(&inner_grid),
        RangeWindow {
            range_ms: 120_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::LastOverTime,
    )
    .unwrap();
    let inner_series = inner_evaluator.push(&chunk).unwrap().unwrap();
    inner_evaluator.finish().unwrap();
    let map_schema = evaluated_schema(Arc::clone(&inner_grid));
    let mapped_chunk = map_chunk(
        &map_schema,
        &EvaluatedChunk::try_new(inner_series, true).unwrap(),
        &[MathOp::Abs],
    )
    .unwrap();
    assert!(
        Arc::ptr_eq(mapped_chunk.range().grid(), &inner_grid),
        "the map kernel moved the inner grid"
    );
    let mut outer_evaluator = RangeEvaluator::try_new(
        &map_schema,
        Arc::clone(&outer_grid),
        RangeWindow {
            range_ms: 240_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::LastOverTime,
    )
    .unwrap();
    let outer_series = outer_evaluator
        .push_series(&mapped_chunk.series)
        .unwrap()
        .unwrap();
    outer_evaluator.finish().unwrap();
    assert_eq!(
        addressed(&out),
        vec![(series_start(&outer_series), sample_values(&outer_series))]
    );
}

/// Every node kind the runtime does not execute is rejected, naming the kind and its position.
#[test]
fn an_unimplemented_node_is_reported_not_panicked() {
    // One grid shared by the nodes that validate by allocation identity.
    let statement = || grid(0, 120_000, 2);

    let scalar_tree = scalar(1.0, statement());
    let instant_tree = {
        let out = statement();
        map(
            NativeNode {
                expr: NativeExpr::Instant {
                    input: Box::new(input()),
                    op: BoundInstant {
                        lookback_ms: 300_000,
                        offset_ms: 0,
                        output_grid: Arc::clone(&out),
                    },
                },
                output: NativeType::Samples(evaluated_schema(out)),
            },
            vec![MathOp::Abs],
        )
    };
    let aggregate_tree = {
        let out = statement();
        NativeNode {
            expr: NativeExpr::Aggregate {
                input: Box::new(window(input(), Arc::clone(&out))),
                op: BoundAggregate {
                    kind: AggregateKind::Sum,
                    group: Arc::new(["job".into()]),
                    stage: AggregateStage::Complete,
                },
            },
            output: NativeType::Samples(evaluated_schema(out)),
        }
    };
    let binary_tree = {
        let out = statement();
        NativeNode {
            expr: NativeExpr::Binary {
                left: Box::new(window(input(), Arc::clone(&out))),
                right: Box::new(scalar(1.0, Arc::clone(&out))),
                matching: BinaryMatching {
                    op: BinaryOp::Add,
                    return_bool: false,
                    labels: MatchingLabels::Ignoring(Vec::new()),
                    cardinality: Cardinality::OneToOne,
                },
            },
            output: NativeType::Samples(evaluated_schema(out)),
        }
    };

    let cases = [
        ("Scalar", "root", scalar_tree),
        ("Instant", "root.input", instant_tree),
        ("Aggregate", "root", aggregate_tree),
        ("Binary", "root", binary_tree),
    ];
    for (kind, path, root) in cases {
        // Legality stays `validate`'s: the plan accepts these trees, the runtime does not run them.
        let tree = NativeTree::try_new(root).expect("the bound tree is legal");
        let error = match NativeExecutionState::try_new(
            &tree,
            pool(),
            &ExecutionPlanMetricsSet::new(),
            0,
        ) {
            Ok(_) => panic!("the runtime accepted a {kind} node"),
            Err(error) => error,
        };
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{kind}: {error}"
        );
        let message = error.to_string();
        assert!(message.contains(kind), "{kind}: {message}");
        assert!(message.contains(&format!("at {path}")), "{message}");
    }
}

/// Each node reports its own chunks, grid points and closed series, at its own position.
///
/// The two windows see different traffic — the inner one three source chunks, the outer one the
/// single series the inner one closed — so a single total for the tree cannot produce both
/// numbers.
#[test]
fn each_node_records_its_own_metrics() {
    let inner_grid = grid(0, 60_000, 4);
    let outer_grid = grid(0, 120_000, 2);
    let root = window_with(
        window_with(
            input(),
            RangeFunction::LastOverTime,
            300_000,
            None,
            Arc::clone(&inner_grid),
        ),
        RangeFunction::LastOverTime,
        300_000,
        Some(inner_grid),
        outer_grid,
    );
    let tree = NativeTree::try_new(root).unwrap();
    let set = ExecutionPlanMetricsSet::new();
    let mut execution = executor(&tree, pool(), &set);
    let schema = raw_schema();
    execution
        .push_raw(raw_chunk(&schema, &[0, 60_000], &[1.0, 2.0], false))
        .unwrap();
    execution
        .push_raw(raw_chunk(&schema, &[120_000], &[3.0], false))
        .unwrap();
    execution
        .push_raw(raw_chunk(&schema, &[180_000], &[4.0], true))
        .unwrap();
    let out = run(&mut execution);
    assert_eq!(addressed(&out), vec![(0, vec![Some(1.0), Some(3.0)])]);

    let outer = NodePath::root();
    let inner = outer.input();
    let source = inner.input();

    // Three source chunks reach the inner window; one assembled series reaches the outer one.
    assert_eq!(count(&set, &source, NodeMetric::INPUT_BATCHES), 3);
    assert_eq!(count(&set, &source, NodeMetric::OUTPUT_BATCHES), 3);
    assert_eq!(count(&set, &inner, NodeMetric::INPUT_BATCHES), 3);
    assert_eq!(count(&set, &outer, NodeMetric::INPUT_BATCHES), 1);
    assert_eq!(count(&set, &inner, NodeMetric::OUTPUT_BATCHES), 1);
    assert_eq!(count(&set, &outer, NodeMetric::OUTPUT_BATCHES), 1);
    // The grid points each window decided, which is its own output range.
    assert_eq!(count(&set, &inner, NodeMetric::EVALUATED_GRID_POINTS), 4);
    assert_eq!(count(&set, &outer, NodeMetric::EVALUATED_GRID_POINTS), 2);
    // The one series closes once, on the chunk carrying `is_last`.
    assert_eq!(count(&set, &source, NodeMetric::COMPLETED_SERIES), 1);
    assert_eq!(count(&set, &inner, NodeMetric::COMPLETED_SERIES), 1);
    assert_eq!(count(&set, &outer, NodeMetric::COMPLETED_SERIES), 1);
    // Per node, not one total for the tree.
    assert_ne!(
        count(&set, &inner, NodeMetric::INPUT_BATCHES),
        count(&set, &outer, NodeMetric::INPUT_BATCHES)
    );
}

/// An error returns the reservation while the execution is still alive, and a drop returns it too.
#[test]
fn an_error_releases_the_reservation() {
    let tree = NativeTree::try_new(window(input(), grid(0, 60_000, 4))).unwrap();
    let pool = pool();
    let set = ExecutionPlanMetricsSet::new();
    let schema = raw_schema();

    let mut execution = executor(&tree, Arc::clone(&pool), &set);
    assert_eq!(pool.reserved(), 0);
    execution
        .push_raw(raw_chunk(&schema, &[0, 60_000], &[1.0, 2.0], false))
        .unwrap();
    assert!(
        pool.reserved() > 0,
        "the queued chunk has to be reserved for"
    );
    assert!(execution.next_chunk().unwrap().is_none());
    let held = pool.reserved();
    assert!(held > 0, "the open series has to be reserved for");

    // A second chunk of the same series whose timestamps do not increase: the evaluator rejects it
    // after the node already held input for it.
    execution
        .push_raw(raw_chunk(&schema, &[0, 60_000], &[3.0, 4.0], true))
        .unwrap();
    let error = execution.next_chunk().unwrap_err();
    assert!(error.to_string().contains("not increasing"), "{error}");
    // Everything went back to the pool while the execution is alive: waiting for the drop would
    // keep the query's bytes in use for as long as the caller holds the executor.
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
    let current = NodePath::root().metric_name(NodeMetric::CURRENT_RESERVED_BYTES);
    assert_eq!(gauge(&set, &current), 0);
    let peak = NodePath::root().partitioned_metric_name(NodeMetric::PEAK_RESERVED_BYTES, 0);
    assert!(gauge(&set, &peak) >= held);
    // A stopped execution is not resumed.
    let again = execution.next_chunk().unwrap_err();
    assert!(
        again.to_string().contains("stopped after an earlier error"),
        "{again}"
    );

    // Dropping an execution that never finished returns its bytes too.
    let mut unfinished = executor(&tree, Arc::clone(&pool), &set);
    unfinished
        .push_raw(raw_chunk(&schema, &[0], &[1.0], false))
        .unwrap();
    assert!(unfinished.next_chunk().unwrap().is_none());
    assert!(
        pool.reserved() > 0,
        "the open series has to be reserved for"
    );
    drop(unfinished);
    assert_eq!(pool.reserved(), 0);
}
