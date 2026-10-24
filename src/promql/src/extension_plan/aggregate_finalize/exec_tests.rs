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

//! What the merge has to get right once it runs: the state of several partitions folded into one
//! series, the tree above the cut run over the merged values, and an answer only after every
//! producer closed.
//!
//! The trees are built node by node with the bindings the PromQL compiler produces, because the
//! compiler lives in the `query` crate, which depends on this one. The input batches are made by the
//! producer's own fold ([`PartialFold`]), so a test never writes a state row the exporter could not.
//!
//! The state is chosen so that a wrong merge is visible in the answer: one group holds the samples
//! `2`, `6` and `10` at one point, one of them folded by the first producer and two by the second, so
//! the merge has to answer `sum = 18`, `count = 3` and `avg = 6` - a mean of the producers' means
//! would answer `5`.

use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use datafusion::arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::EmptyRelation;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::{RecordBatchStream, collect};
use datafusion::prelude::SessionContext;
use datatypes::series::samples::{Samples, Series, SeriesFields};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName};
use futures::Stream;

use super::*;
use crate::extension_plan::aggregate_exchange::PartialFold;
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};

/// The query the trees here are compiled from.
const PROMQL: &str = "avg(rate(x[5m]))";
/// The output column holding the grid timestamps.
const TIME: &str = "ts";
/// The output column holding the merged values.
const VALUE: &str = "avg(rate(x[5m]))";
/// The one tag every group here carries.
const K0: &str = "k0";
/// The value of that tag.
const LABEL: &str = "a";
/// The field of the cut aggregate's schema.
const FIELD: &str = "v0";
/// The metric name the input series are bound to.
const METRIC: &str = "m";
/// The statement's grid: twelve points, a minute apart.
const GRID_STEP: i64 = 60_000;
const GRID_LEN: u64 = 12;
/// The one grid point every sample of these tests falls on.
const POINT: usize = 5;
/// The timestamp of [`POINT`].
const POINT_TS: i64 = 300_000;

/// The statement's grid, which every evaluated chunk is addressed on.
fn grid() -> Arc<EvalGrid> {
    grid_of(GRID_LEN)
}

/// A grid of `len` points on this statement's step, for a test that needs an answer of its own width.
fn grid_of(len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(0, GRID_STEP, len).unwrap()).unwrap())
}

/// The raw fields the tree's input binds, which a scan below it has to be able to produce.
fn raw_fields() -> Arc<[FieldSpec]> {
    Arc::from([FieldSpec { name: FIELD.into() }])
}

/// The raw samples a scan hands over: one field, no grid.
fn raw() -> NativeNode {
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: raw_fields(),
        eval_grid: None,
    }));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: output.clone(),
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// The evaluated layout a window reports, which is also the layout of the cut aggregate's output.
fn evaluated(grid: &Arc<EvalGrid>) -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec { name: FIELD.into() }]),
        eval_grid: Some(Arc::clone(grid)),
    }))
}

/// `rate(v0[5m])`, the per-series prefix of every tree here.
fn window() -> NativeNode {
    window_on(&grid())
}

/// `rate(v0[5m])` evaluated on `grid`.
fn window_on(grid: &Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(raw()),
            op: crate::native::plan::BoundWindow {
                function: crate::range_eval::RangeFunction::Rate,
                window: crate::range_eval::RangeWindow {
                    range_ms: 300_000,
                    offset_ms: 0,
                    anchor_ms: None,
                },
                input_grid: None,
                output_grid: Arc::clone(grid),
            },
        },
        output: evaluated(grid),
    }
}

fn map_over(inner: NativeNode, ops: Vec<MathOp>) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: crate::native::plan::BoundMap {
                ops: ops.into_iter().map(MapOp::Math).collect::<Vec<_>>().into(),
            },
        },
        output,
    }
}

fn aggregate_over(
    inner: NativeNode,
    kind: AggregateKind,
    group: Grouping,
    stage: AggregateStage,
) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate { kind, group, stage },
        },
        output,
    }
}

fn complete(kind: AggregateKind, group: Grouping) -> impl Fn(NativeNode) -> NativeNode {
    move |inner| aggregate_over(inner, kind, group.clone(), AggregateStage::Complete)
}

fn by(labels: &[&str]) -> Grouping {
    Grouping::By(
        labels
            .iter()
            .map(|label| Arc::<str>::from(*label))
            .collect(),
    )
}

/// The tree every test below cuts: `avg(rate(v0[5m]))`, with a point-wise map as the prefix's outer
/// node so the prefix is the shape the split admits.
fn avg_tree(kind: AggregateKind) -> NativeNode {
    complete(kind, by(&[K0]))(map_over(window(), vec![MathOp::Ceil]))
}

/// `left <op> right` with a constant on one side, bound the way the compiler binds a broadcast: the
/// output is the vector side's samples.
fn broadcast(left: NativeNode, right: NativeNode, op: BinaryOp) -> NativeNode {
    let output = if matches!(left.output, NativeType::Scalar(_)) {
        right.output.clone()
    } else {
        left.output.clone()
    };
    NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: BinaryMatching {
                op,
                return_bool: false,
                labels: MatchingLabels::Ignoring(Vec::new()),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output,
    }
}

/// The constant `value` on the statement's grid.
fn constant(value: f64) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(crate::native::plan::BoundScalar { value }),
        output: NativeType::Scalar(grid()),
    }
}

/// The producer's aggregate, bound the way the compiler binds it.
fn cut_op(kind: AggregateKind) -> BoundAggregate {
    cut_op_by(kind, &[K0])
}

/// The producer's aggregate of a cut grouped `labels`.
fn cut_op_by(kind: AggregateKind, labels: &[&str]) -> BoundAggregate {
    BoundAggregate {
        kind,
        group: by(labels),
        stage: AggregateStage::Complete,
    }
}

/// The field layout the cut aggregate folds: one field on the statement's grid.
fn cut_schema() -> Arc<SeriesSchema> {
    cut_schema_of(GRID_LEN)
}

/// The field layout the cut aggregate folds on a grid of `len` points.
fn cut_schema_of(len: u64) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec { name: FIELD.into() }]),
        eval_grid: Some(grid_of(len)),
    })
}

fn binding() -> FusedSeriesBinding {
    binding_of(&[K0], GRID_LEN)
}

/// What the tree's input was compiled against, with `tags` naming the label columns the rows carry.
fn binding_of(tags: &[&str], len: u64) -> FusedSeriesBinding {
    FusedSeriesBinding {
        grid: grid_of(len),
        metric_name: Some(METRIC.into()),
        time_index: TIME.to_string(),
        tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
    }
}

/// The plan below the producer: a scan producing the raw fields the tree's input binds.
///
/// The producer checks that its child can produce every field its tree input declares, so the scan
/// names them as nullable float columns; the merge itself reads none of them.
fn scan() -> LogicalPlan {
    let columns = raw_fields()
        .iter()
        .map(|field| {
            (
                None,
                Arc::new(Field::new(field.name.as_ref(), DataType::Float64, true)),
            )
        })
        .collect::<Vec<_>>();
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::new_with_metadata(columns, Default::default()).unwrap()),
    })
}

/// The rows a finalized aggregate reports: the time index, one value column and the grouped labels.
fn output_schema(value: &str, tags: &[&str]) -> DFSchemaRef {
    let mut columns = vec![(
        None,
        Arc::new(Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )),
    )];
    columns.push((None, Arc::new(Field::new(value, DataType::Float64, true))));
    for tag in tags {
        columns.push((None, Arc::new(Field::new(*tag, DataType::Utf8, true))));
    }
    Arc::new(DFSchema::new_with_metadata(columns, Default::default()).unwrap())
}

fn finalize(root: NativeNode, output: DFSchemaRef) -> AggregateFinalize {
    finalize_with(binding(), root, output)
}

/// The node of a tree cut on `binding`, reporting `output`.
fn finalize_with(
    binding: FusedSeriesBinding,
    root: NativeNode,
    output: DFSchemaRef,
) -> AggregateFinalize {
    AggregateFinalize::try_new(
        Arc::from(PROMQL),
        NativeTree::try_new(root).unwrap(),
        binding,
        scan(),
        output,
    )
    .unwrap()
}

/// The metric of the one group these tests fold.
fn labels() -> MetricName {
    MetricName::try_new(
        Some(METRIC.into()),
        vec![Label {
            name: K0.into(),
            value: LABEL.into(),
        }],
    )
    .unwrap()
}

/// One whole evaluated series of the input: one sample at [`POINT`].
fn series_at(schema: &Arc<SeriesSchema>, value: f64) -> Series {
    series_span(schema, &[value])
}

/// One whole evaluated series of the input holding `values` from [`POINT`] on, on the grid `schema`
/// declares.
fn series_span(schema: &Arc<SeriesSchema>, values: &[f64]) -> Series {
    let grid = Arc::clone(schema.eval_grid.as_ref().expect("the cut is on a grid"));
    Series::try_new(
        schema,
        labels(),
        TimeAxis::Grid(GridRange::try_new(grid, POINT, values.len()).unwrap()),
        SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(
            values.to_vec(),
        ))])
        .unwrap(),
    )
    .unwrap()
}

/// One producer's state, folded by the producer's own fold: `values` are the samples that producer's
/// scan returned for the group, in the order it read them.
fn state_of(op: BoundAggregate, values: &[f64]) -> Vec<RecordBatch> {
    let schema = cut_schema();
    let chunks = values
        .iter()
        .map(|value| series_at(&schema, *value))
        .collect();
    state_of_series(&schema, op, &[K0], chunks)
}

/// One producer's state from the whole series it read, folded by the producer's own fold. `tags` names
/// the label columns the state batch carries, which is what the binding of that producer declares.
fn state_of_series(
    schema: &Arc<SeriesSchema>,
    op: BoundAggregate,
    tags: &[&str],
    chunks: Vec<Series>,
) -> Vec<RecordBatch> {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let tags: Arc<[String]> = tags.iter().map(|tag| (*tag).to_string()).collect();
    let mut fold = PartialFold::try_new(
        op,
        Arc::clone(schema),
        tags,
        Arc::new(Mutex::new(None)),
        &pool,
    )
    .unwrap();
    for series in chunks {
        fold.update(EvaluatedChunk {
            series,
            is_last: true,
        })
        .unwrap();
    }
    fold.finish().unwrap()
}

/// A physical input producing each of `partitions`' batches in a partition of its own, on the state
/// layout.
fn state_input(partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    state_input_with(&[K0], partitions)
}

/// A physical input of state batches labelled by `tags`, each of `partitions`' lists in a partition of
/// its own.
fn state_input_with(tags: &[&str], partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    let tags: Vec<String> = tags.iter().map(|tag| (*tag).to_string()).collect();
    Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&partitions, state_schema(&tags), None)
            .expect("every partition holds state batches"),
    )))
}

/// The state of the group these tests merge: one producer folded `2`, another folded `6` and `10`.
fn two_producers(kind: AggregateKind) -> Vec<Vec<RecordBatch>> {
    vec![
        state_of(cut_op(kind), &[2.0]),
        state_of(cut_op(kind), &[6.0, 10.0]),
    ]
}

/// The rows of `batches`, as (timestamp, value, label).
fn rows(batches: &[RecordBatch]) -> Vec<(i64, Option<f64>, Option<String>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let time = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("the time column");
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("the value column");
        let label = if batch.num_columns() > 2 {
            Some(
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("the label column"),
            )
        } else {
            None
        };
        for row in 0..batch.num_rows() {
            rows.push((
                time.value(row),
                value.is_valid(row).then(|| value.value(row)),
                label
                    .and_then(|labels| labels.is_valid(row).then(|| labels.value(row).to_string())),
            ));
        }
    }
    rows
}

/// A task context whose memory pool is `pool`, so what the node charges can be read afterwards.
fn task_context(pool: &Arc<GreedyMemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool) as Arc<dyn MemoryPool>)
        .build()
        .expect("the runtime environment builds");
    Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
}

/// Two producers fold the same group, and the merge answers their sum rather than their parts.
#[tokio::test]
async fn the_state_of_two_producers_merges_into_one_series() {
    let kind = AggregateKind::Avg;
    let node = finalize(avg_tree(kind), output_schema(VALUE, &[K0]));
    let plan = node
        .to_execution_plan(state_input(two_producers(kind)))
        .expect("the merge runs on the state producer's output");
    // The producers are gathered, and the merge answers the one partition it declares.
    assert_eq!(plan.children()[0].name(), "CoalescePartitionsExec");
    assert_eq!(plan.properties().partitioning.partition_count(), 1);
    assert_eq!(
        plan.schema().as_ref(),
        node.schema().inner().as_ref(),
        "the rows have the node's schema"
    );
    let batches = collect(plan, SessionContext::default().task_ctx())
        .await
        .expect("the merge runs");
    // `(2 + 6 + 10) / 3`; averaging the producers' means would answer `(2 + 8) / 2 = 5`.
    assert_eq!(
        rows(&batches),
        [(POINT_TS, Some(6.0), Some(LABEL.to_string()))]
    );
}

/// Every fixed-width accumulator merges the producers' state, whichever fields it keeps.
#[tokio::test]
async fn every_fixed_width_kind_merges_across_producers() {
    let expected: [(AggregateKind, f64); 8] = [
        (AggregateKind::Sum, 18.0),
        (AggregateKind::Count, 3.0),
        (AggregateKind::Avg, 6.0),
        (AggregateKind::Min, 2.0),
        (AggregateKind::Max, 10.0),
        (AggregateKind::Group, 1.0),
        (AggregateKind::Stddev, (32.0f64 / 3.0).sqrt()),
        (AggregateKind::Stdvar, 32.0 / 3.0),
    ];
    for (kind, expected) in expected {
        let plan = finalize(avg_tree(kind), output_schema(VALUE, &[K0]))
            .to_execution_plan(state_input(two_producers(kind)))
            .unwrap_or_else(|error| panic!("{kind:?}: {error}"));
        let batches = collect(plan, SessionContext::default().task_ctx())
            .await
            .unwrap_or_else(|error| panic!("{kind:?}: {error}"));
        let rows = rows(&batches);
        assert_eq!(rows.len(), 1, "{kind:?}: {rows:?}");
        let (time, value, label) = rows[0].clone();
        let value = value.unwrap_or_else(|| panic!("{kind:?} answered no value"));
        assert_eq!(time, POINT_TS, "{kind:?}");
        assert!(
            (value - expected).abs() < 1e-12,
            "{kind:?}: {value} != {expected}"
        );
        assert_eq!(label.as_deref(), Some(LABEL), "{kind:?}");
    }
}

/// The tree above the cut runs over the merged values: `sum(abs(avg(...)))` answers one series with
/// no labels, built by a point-wise map and a second aggregate.
#[tokio::test]
async fn the_tree_above_the_cut_runs_over_the_merged_series() {
    let kind = AggregateKind::Avg;
    // `abs` is what turns the merged `-6` into the `6` the outer `sum` answers.
    let state = vec![
        state_of(cut_op(kind), &[-2.0]),
        state_of(cut_op(kind), &[-6.0, -10.0]),
    ];
    let root = complete(AggregateKind::Sum, by(&[]))(map_over(avg_tree(kind), vec![MathOp::Abs]));
    let plan = finalize(root, output_schema("sum(abs(avg(v0)))", &[]))
        .to_execution_plan(state_input(state))
        .expect("the suffix runs over merged values");
    let batches = collect(plan, SessionContext::default().task_ctx())
        .await
        .expect("the suffix runs");
    assert_eq!(rows(&batches), [(POINT_TS, Some(6.0), None)]);
}

/// A constant operand of the suffix is applied to the merged values, whichever side it is written on
/// and whichever operator it carries, and the answer's own direction follows the operand order.
///
/// Every case merges the same three samples: `2`, `6` and `10` of one group, folded `2` by one
/// producer and `6` and `10` by the other. The partitions are therefore unequal, and the merged
/// `avg` is `6` - the average of all three samples, where a mean of the two producers' means would
/// answer `5` - while the merged `sum` is `18`. The two `Sub` cases answer `94` and `-94`, and the
/// `Div` case `100 / 18`, so a swapped pair answers a different number; where the constant commutes
/// with the fold - `* 2`, and the `-1` of the negation - the prefix would answer the same number,
/// so those cases cover the operator rather than the placement.
#[tokio::test]
async fn a_constant_operand_of_the_suffix_computes_on_the_merged_values() {
    use AggregateKind::{Avg, Sum};
    let cases = [
        (
            Avg,
            broadcast(constant(100.0), avg_tree(Avg), BinaryOp::Sub),
            "100 - avg(rate(x[5m]))",
            100.0 - 6.0,
        ),
        (
            Avg,
            broadcast(avg_tree(Avg), constant(100.0), BinaryOp::Sub),
            "avg(rate(x[5m])) - 100",
            6.0 - 100.0,
        ),
        (
            Sum,
            broadcast(avg_tree(Sum), constant(2.0), BinaryOp::Mul),
            "sum(rate(x[5m])) * 2",
            2.0 * 18.0,
        ),
        (
            Sum,
            broadcast(constant(100.0), avg_tree(Sum), BinaryOp::Div),
            "100 / sum(rate(x[5m]))",
            100.0 / 18.0,
        ),
        (
            Sum,
            broadcast(constant(-1.0), avg_tree(Sum), BinaryOp::Mul),
            "-sum(rate(x[5m]))",
            -18.0,
        ),
    ];
    for (kind, root, column, expected) in cases {
        let plan = finalize(root, output_schema(column, &[K0]))
            .to_execution_plan(state_input(two_producers(kind)))
            .expect("the operation runs over the merged values");
        let batches = collect(plan, SessionContext::default().task_ctx())
            .await
            .expect("the suffix runs");
        let rows = rows(&batches);
        assert_eq!(rows.len(), 1, "{column}: {rows:?}");
        let (time, value, label) = rows[0].clone();
        let value = value.unwrap_or_else(|| panic!("{column} answered no value"));
        assert_eq!(time, POINT_TS, "{column}");
        assert!(
            (value - expected).abs() < 1e-12,
            "{column}: {value} != {expected}"
        );
        assert_eq!(label.as_deref(), Some(LABEL), "{column}");
    }
}

/// Every producer is read at the same time. Each stream of the input below holds its rows back until
/// all of them have been polled, so a merge that drained one partition before reading the next would
/// deadlock here instead of answering.
#[tokio::test]
async fn every_producer_is_read_at_once() {
    let kind = AggregateKind::Avg;
    let states = two_producers(kind);
    let barrier = Arc::new(Barrier::new(states.len()));
    let input = Arc::new(ScriptedPlan::new(
        states
            .into_iter()
            .map(|state| state.into_iter().map(Script::Batch).collect())
            .collect(),
        Some(Arc::clone(&barrier)),
        None,
    ));
    let plan = finalize(avg_tree(kind), output_schema(VALUE, &[K0]))
        .to_execution_plan(input)
        .expect("the merge runs on the scripted input");
    let batches = tokio::time::timeout(
        Duration::from_secs(2),
        collect(plan, SessionContext::default().task_ctx()),
    )
    .await
    .expect("every producer is polled, so the barrier opens")
    .expect("the merge runs");
    assert_eq!(
        rows(&batches),
        [(POINT_TS, Some(6.0), Some(LABEL.to_string()))]
    );
    assert_eq!(2, barrier.polled(), "every producer was read");
}

/// A producer that fails after a good batch is reported before any row: the merge has no final answer
/// until the input ended.
#[tokio::test]
async fn an_input_error_after_a_valid_batch_is_the_first_item() {
    let kind = AggregateKind::Avg;
    let state = state_of(cut_op(kind), &[2.0]);
    let input = Arc::new(ScriptedPlan::new(
        vec![
            vec![Script::Batch(state[0].clone())],
            vec![
                Script::Batch(state[0].clone()),
                Script::Error("the producer failed"),
            ],
        ],
        None,
        None,
    ));
    let pool = Arc::new(GreedyMemoryPool::new(4 << 20));
    let plan = finalize(avg_tree(kind), output_schema(VALUE, &[K0]))
        .to_execution_plan(input)
        .expect("the merge runs on the scripted input");
    let mut stream = plan
        .execute(0, task_context(&pool))
        .expect("the merge executes");
    let error = stream
        .next()
        .await
        .expect("the failure is reported")
        .expect_err("the producer failed");
    assert!(error.to_string().contains("the producer failed"), "{error}");
    assert!(stream.next().await.is_none(), "a row followed the error");
    // The plan outlives the failed stream, and keeps none of what that stream charged.
    drop(stream);
    assert_eq!(0, pool.reserved(), "the plan kept a failed answer's charge");
}

/// The answer's charge belongs to the stream that hands it over, not to the plan.
#[tokio::test]
async fn the_answer_reservation_is_released_with_the_stream() {
    let pool = Arc::new(GreedyMemoryPool::new(4 << 20));
    let kind = AggregateKind::Avg;
    let plan = finalize(avg_tree(kind), output_schema(VALUE, &[K0]))
        .to_execution_plan(state_input(two_producers(kind)))
        .expect("the merge runs on the state producer's output");
    let mut stream = plan
        .execute(0, task_context(&pool))
        .expect("the merge executes");
    let batch = stream
        .next()
        .await
        .expect("the answer is handed over")
        .expect("the merge answers");
    let rows = rows(std::slice::from_ref(&batch));
    assert_eq!(
        rows,
        [(POINT_TS, Some(6.0), Some(LABEL.to_string()))],
        "the answer is the merged group"
    );
    assert!(
        pool.reserved() > 0,
        "the answer is charged while the stream holds it"
    );
    // The plan is alive and the row is still in hand, so what was given back is the plan's own share of
    // the answer rather than the row's bytes.
    drop(stream);
    assert_eq!(0, pool.reserved(), "the plan kept the answer's charge");
    drop(batch);
    drop(plan);
}

/// A wide answer of one unlabelled group needs no kilobyte a row: the rows are a value and a grid
/// point each, so an ordinary pool holds all of them.
#[tokio::test]
async fn a_wide_answer_of_one_group_fits_an_ordinary_pool() {
    // Enough points that a kilobyte a row would not fit the pool below.
    const POINTS: usize = 1_500;
    const LEN: u64 = POINTS as u64 + POINT as u64;
    let kind = AggregateKind::Sum;
    let grid = grid_of(LEN);
    let schema = cut_schema_of(LEN);
    // One group, no labels: the rows are the time index and one value.
    let values = vec![3.0; POINTS];
    let state = state_of_series(
        &schema,
        cut_op_by(kind, &[]),
        &[],
        vec![series_span(&schema, &values)],
    );
    // A megabyte is far more than these rows need and far less than a kilobyte a row.
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let root = complete(kind, by(&[]))(map_over(window_on(&grid), vec![MathOp::Ceil]));
    let plan = finalize_with(binding_of(&[], LEN), root, output_schema(VALUE, &[]))
        .to_execution_plan(state_input_with(&[], vec![state]))
        .expect("the merge runs on a wide state");
    let batches = collect(plan, task_context(&pool))
        .await
        .expect("a megabyte holds these rows");
    let rows = rows(&batches);
    assert_eq!(POINTS, rows.len(), "every grid point is answered");
    assert!(
        rows.iter()
            .all(|(_, value, label)| *value == Some(3.0) && label.is_none()),
        "each row holds the group's value and no label"
    );
}

/// No contribution is no answer, not an empty series.
#[tokio::test]
async fn an_empty_state_answers_no_rows() {
    let kind = AggregateKind::Avg;
    let empty = RecordBatch::new_empty(state_schema(&[K0.to_string()]));
    let plan = finalize(avg_tree(kind), output_schema(VALUE, &[K0]))
        .to_execution_plan(state_input(vec![vec![empty.clone()], vec![empty]]))
        .expect("the merge runs on an empty state");
    let batches = collect(plan, SessionContext::default().task_ctx())
        .await
        .expect("an empty state answers");
    assert!(batches.is_empty(), "{:?}", rows(&batches));
}

/// The node merges state and answers one partition, so anything else is refused when it is built or
/// before it allocates.
#[tokio::test]
async fn the_input_has_to_be_state_and_the_node_answers_one_partition() {
    let node = finalize(avg_tree(AggregateKind::Avg), output_schema(VALUE, &[K0]));
    // A producer whose batches are rows rather than state.
    let wrong_schema = Arc::new(Schema::new(vec![Field::new(TIME, DataType::Utf8, true)]));
    let wrong = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(
            &[vec![RecordBatch::new_empty(Arc::clone(&wrong_schema))]],
            wrong_schema,
            None,
        )
        .unwrap(),
    )));
    let error = node
        .to_execution_plan(wrong)
        .expect_err("the merge reads state, not rows");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");

    let plan = node
        .to_execution_plan(state_input(two_producers(AggregateKind::Avg)))
        .expect("the merge runs on the state producer's output");
    let error = match plan.execute(1, SessionContext::default().task_ctx()) {
        Ok(_) => panic!("the merge answers one partition"),
        Err(error) => error,
    };
    assert!(matches!(error, DataFusionError::Execution(_)), "{error}");
}

/// What the merge and its answer do not fit is refused by the pool, and the refusal frees everything
/// the plan charged.
#[tokio::test]
async fn a_tight_budget_fails_and_frees_what_it_charged() {
    let pool = Arc::new(GreedyMemoryPool::new(64));
    let context = task_context(&pool);
    let kind = AggregateKind::Avg;
    let plan = finalize(avg_tree(kind), output_schema(VALUE, &[K0]))
        .to_execution_plan(state_input(two_producers(kind)))
        .expect("the merge runs on the state producer's output");
    // Either the execution does not start or its stream refuses, and both name the pool.
    let error = match plan.execute(0, Arc::clone(&context)) {
        Ok(stream) => collect_stream(stream)
            .await
            .expect_err("the answer does not fit 64 bytes"),
        Err(error) => error,
    };
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    drop(plan);
    assert_eq!(0, pool.reserved(), "the plan kept its charge");
}

/// Drains `stream`, which is what a consumer of this node does.
async fn collect_stream(mut stream: SendableRecordBatchStream) -> DfResult<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        batches.push(batch);
    }
    Ok(batches)
}

/// A stream that is dropped instead of drained drops the producer it owns.
#[tokio::test]
async fn dropping_the_stream_drops_the_input() {
    let dropped = Arc::new(AtomicBool::new(false));
    let kind = AggregateKind::Avg;
    let input = Arc::new(ScriptedPlan::new(
        vec![
            state_of(cut_op(kind), &[2.0])
                .into_iter()
                .map(Script::Batch)
                .collect(),
        ],
        None,
        Some(Arc::clone(&dropped)),
    ));
    let plan = finalize(avg_tree(kind), output_schema(VALUE, &[K0]))
        .to_execution_plan(input)
        .expect("the merge runs on the scripted input");
    let stream = plan
        .execute(0, SessionContext::default().task_ctx())
        .expect("the merge executes");
    drop(stream);
    assert!(
        dropped.load(Ordering::SeqCst),
        "the terminated stream still holds its input"
    );
}

/// One item a scripted partition hands over.
enum Script {
    Batch(RecordBatch),
    Error(&'static str),
}

/// A physical input whose partitions hand over scripted items, and which can hold every partition
/// back until all of them have been polled.
struct ScriptedPlan {
    properties: Arc<PlanProperties>,
    /// One item list per partition.
    script: Arc<Vec<Vec<Script>>>,
    barrier: Option<Arc<Barrier>>,
    /// Set when the input's streams are dropped, which a dropped consumer has to do.
    dropped: Option<Arc<AtomicBool>>,
}

impl ScriptedPlan {
    fn new(
        script: Vec<Vec<Script>>,
        barrier: Option<Arc<Barrier>>,
        dropped: Option<Arc<AtomicBool>>,
    ) -> Self {
        let partitions = script.len();
        Self {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(state_schema(&[K0.to_string()])),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            script: Arc::new(script),
            barrier,
            dropped,
        }
    }
}

impl fmt::Debug for ScriptedPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScriptedPlan").finish()
    }
}

impl DisplayAs for ScriptedPlan {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for ScriptedPlan {
    fn name(&self) -> &str {
        "ScriptedPlan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        Ok(Box::pin(ScriptedStream {
            schema: state_schema(&[K0.to_string()]),
            script: Arc::clone(&self.script),
            partition,
            cursor: 0,
            barrier: self.barrier.clone(),
            registered: false,
            dropped: self.dropped.clone(),
        }))
    }
}

/// One scripted partition's stream.
struct ScriptedStream {
    schema: SchemaRef,
    script: Arc<Vec<Vec<Script>>>,
    partition: usize,
    cursor: usize,
    barrier: Option<Arc<Barrier>>,
    /// Whether this stream has already told the barrier it was polled.
    registered: bool,
    dropped: Option<Arc<AtomicBool>>,
}

impl Stream for ScriptedStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(barrier) = &this.barrier {
            if !this.registered {
                this.registered = true;
                barrier.arrive(cx.waker());
            }
            if !barrier.opened() {
                return Poll::Pending;
            }
        }
        let Some(item) = this.script[this.partition].get(this.cursor) else {
            return Poll::Ready(None);
        };
        this.cursor += 1;
        Poll::Ready(Some(match item {
            Script::Batch(batch) => Ok(batch.clone()),
            Script::Error(message) => Err(DataFusionError::Execution((*message).to_string())),
        }))
    }
}

impl RecordBatchStream for ScriptedStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for ScriptedStream {
    fn drop(&mut self) {
        if let Some(dropped) = &self.dropped {
            dropped.store(true, Ordering::SeqCst);
        }
    }
}

/// Holds every partition's stream until all of them have been polled at least once.
///
/// A driver that read one partition to its end before reading the next would never open it, which is
/// what makes a merge that drained its producers one at a time visible as a deadlock rather than as a
/// wrong answer.
struct Barrier {
    total: usize,
    state: Mutex<BarrierState>,
    /// How many partitions were polled, which is what proves all of them were read.
    polled: AtomicUsize,
}

#[derive(Default)]
struct BarrierState {
    wakers: Vec<Waker>,
    opened: bool,
}

impl Barrier {
    fn new(total: usize) -> Self {
        Self {
            total,
            state: Mutex::new(BarrierState::default()),
            polled: AtomicUsize::new(0),
        }
    }

    /// Records one stream's first poll, opening the barrier once every partition has arrived.
    fn arrive(&self, waker: &Waker) {
        self.polled.fetch_add(1, Ordering::SeqCst);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.wakers.len() == self.total {
            return;
        }
        state.wakers.push(waker.clone());
        if state.wakers.len() < self.total {
            return;
        }
        state.opened = true;
        let wakers = std::mem::take(&mut state.wakers);
        drop(state);
        for waker in wakers {
            waker.wake();
        }
    }

    fn opened(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).opened
    }

    fn polled(&self) -> usize {
        self.polled.load(Ordering::SeqCst)
    }
}
