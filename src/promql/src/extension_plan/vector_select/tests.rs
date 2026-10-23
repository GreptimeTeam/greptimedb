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

//! What this node has to get right: the one shape it accepts, the global order it restores, the
//! answers it gives once that order is in place, and what it does when the input or the pool fails.
//!
//! The trees are built node by node with the same bindings the PromQL compiler produces for
//! `topk(2, x)`: the compiler lives in the `query` crate, which depends on this one, so a test here
//! cannot call it. The frames are packed by the encoder the fused producer uses
//! ([`evaluated_frame::pack`]), so a test states each series' whole label set, its grid positions and
//! its finality instead of relying on where a batch boundary happens to fall.
//!
//! Kernel numerics are not the subject: every series holds a distinct constant, so the two series a
//! `topk(2, ...)` keeps are known in advance and any ordering, finality or framing mistake shows up
//! as a wrong, missing or extra row.

use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use datafusion::arrow::array::{
    Array, Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ToDFSchema;
use datafusion::common::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{LexRequirement, OrderingRequirements, PhysicalSortRequirement};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_plan::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{Distribution, ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName, SeriesOrdering};

use super::{VectorSelect, VectorSelectExec, frame_ordering, unsupported};
use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::evaluated_frame;
use crate::extension_plan::fused_series::{FusedSeries, FusedSeriesBinding};
use crate::extension_plan::test_util::{StagedStream, staged, task_context};
use crate::native::plan::{
    BoundInput, BoundInstant, BoundVectorOp, Grouping, NativeExpr, NativeNode, NativeTree,
    NativeType,
};
use crate::select::SortOrder;

/// The query every tree here is compiled from.
const PROMQL: &str = "topk(2, x)";
/// How many series the selection keeps.
const K: usize = 2;
/// How far back the child's instant selection reads, which these chunks' positions satisfy.
const LOOKBACK_MS: i64 = 300_000;
const GRID_START: i64 = 300_000;
const GRID_STEP: i64 = 60_000;
/// The number of grid points, and so the rows one series filling the grid contributes.
const GRID_LEN: usize = 8;

const TIME: &str = "time";
const VALUE: &str = "value";
const HOST: &str = "host";
const NAME: &str = "__name__";
const METRIC: &str = "requests";

fn grid() -> Arc<EvalGrid> {
    Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN as u64).unwrap())
            .unwrap(),
    )
}

/// A grid one point longer than [`grid`], for the mismatch case.
fn another_grid() -> Arc<EvalGrid> {
    Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN as u64 + 1).unwrap())
            .unwrap(),
    )
}

fn one_field() -> Arc<[FieldSpec]> {
    Arc::from([FieldSpec { name: VALUE.into() }])
}

/// The raw schema the child tree's input is bound to.
fn raw_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: one_field(),
        eval_grid: None,
    })
}

/// The evaluated schema the child tree produces, which is what a frame is packed against.
fn evaluated_schema() -> Arc<SeriesSchema> {
    schema_on(grid())
}

fn schema_on(grid: Arc<EvalGrid>) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: one_field(),
        eval_grid: Some(grid),
    })
}

/// The child tree: one instant selection of a raw scan onto the statement's grid.
fn selection_child(schema: Arc<SeriesSchema>, grid: Arc<EvalGrid>) -> NativeNode {
    let raw = NativeType::Samples(schema);
    let evaluated = NativeType::Samples(Arc::new(SeriesSchema {
        fields: one_field(),
        eval_grid: Some(Arc::clone(&grid)),
    }));
    NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: raw.clone(),
                    ordering: SeriesOrdering::KeyThenTime,
                }),
                output: raw,
            }),
            op: BoundInstant {
                lookback_ms: LOOKBACK_MS,
                offset_ms: 0,
                output_grid: grid,
            },
        },
        output: evaluated,
    }
}

/// A `topk` over `child`, with the output the compiler would declare for it.
fn topk_over(child: NativeNode) -> NativeTree {
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(child.clone()),
            op: BoundVectorOp::TopK {
                k: K as f64,
                top: true,
                group: Grouping::By(Arc::from([])),
            },
        },
        output: child.output.clone(),
    })
    .expect("the bound tree is legal")
}

/// A `sort` over `child`, which is a vector operation this node does not answer.
fn sort_over(child: NativeNode) -> NativeTree {
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(child.clone()),
            op: BoundVectorOp::Sort(SortOrder::Asc),
        },
        output: child.output.clone(),
    })
    .expect("the bound tree is legal")
}

fn selection_tree() -> NativeTree {
    topk_over(selection_child(raw_schema(), grid()))
}

/// What the tree's input was compiled against, for the tags these tests report.
fn binding(tree: &NativeTree) -> FusedSeriesBinding {
    FusedSeriesBinding {
        series_schema: raw_schema(),
        grid: tree
            .root()
            .output
            .grid()
            .expect("the tree's output is evaluated")
            .clone(),
        metric_name: Some(Arc::from(METRIC)),
        time_index: TIME.to_string(),
        tags: vec![HOST.to_string(), NAME.to_string()],
    }
}

/// The columns this node produces: the field, the two tags and the grid timestamps.
fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(NAME, DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

/// A logical input no test here reads: the frames come from a memory source instead.
fn input_plan() -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: evaluated_frame::schema().to_dfschema_ref().unwrap(),
    })
}

fn node_of(tree: NativeTree) -> VectorSelect {
    let binding = binding(&tree);
    VectorSelect::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("a topk over one evaluated field is this node's shape")
}

fn node() -> VectorSelect {
    node_of(selection_tree())
}

/// One series' labels: the metric name and a `host` tag.
fn metric(host: &str) -> MetricName {
    MetricName::try_new(
        Some(Arc::from(METRIC)),
        vec![Label {
            name: Arc::from(HOST),
            value: Arc::from(host),
        }],
    )
    .expect("the labels of these tests are legal")
}

/// One evaluated chunk of `host` over `runs`, each `(start position, one value per row)`.
fn chunk(host: &str, runs: &[(usize, Vec<Option<f64>>)], is_last: bool) -> EvaluatedChunk {
    let schema = evaluated_schema();
    let mut spans = Vec::new();
    let mut values = Vec::new();
    for (start, run) in runs {
        if run.is_empty() {
            continue;
        }
        spans.push(GridSpan::new(*start, run.len()));
        values.extend(run.iter().copied());
    }
    let axis =
        TimeAxis::from_grid_spans(grid(), spans).expect("the runs are ascending and disjoint");
    let series = Series::try_new(
        &schema,
        metric(host),
        axis,
        SeriesFields::one(Samples::from_float(Float64Array::from(values))),
    )
    .expect("the runs and the values agree");
    EvaluatedChunk::try_new(series, is_last).expect("the chunk is addressed on the grid")
}

/// A chunk of `host` holding `value` at every grid point.
fn full_chunk(host: &str, value: f64, is_last: bool) -> EvaluatedChunk {
    chunk(host, &[(0, vec![Some(value); GRID_LEN])], is_last)
}

/// One frame carrying `chunk`, produced by `producer` as its frame `ordinal`.
fn frame(chunk: &EvaluatedChunk, producer: u64, ordinal: u64) -> RecordBatch {
    evaluated_frame::pack(chunk, &evaluated_schema(), producer, ordinal)
        .expect("the chunk is one field on the statement's grid")
}

/// The four producers' closing frames, in producer order.
///
/// Every `web-<i>` holds `i` at every grid point, so `topk(2, ...)` keeps `web-3` and `web-2`
/// whatever order the frames arrive in.
fn closing_frames() -> Vec<RecordBatch> {
    (0..4u64)
        .map(|producer| {
            let host = format!("web-{producer}");
            frame(&full_chunk(&host, producer as f64, true), producer, 0)
        })
        .collect()
}

/// A physical input producing `partitions` in as many partitions, on the frame layout.
fn memory_exec(partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&partitions, evaluated_frame::schema(), None)
            .expect("every partition holds frames of the frame layout"),
    )))
}

fn plan_of(
    node: &VectorSelect,
    input: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    node.to_execution_plan(input)
}

/// The rows of `batches`: host, metric name, timestamp and value.
type Row = (String, Option<String>, i64, Option<f64>);

fn rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let value = batch
            .column_by_name(VALUE)
            .expect("every batch has the field column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("the field column is Float64");
        let host = batch
            .column_by_name(HOST)
            .expect("every batch has the host column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("the host column is Utf8");
        let name = batch
            .column_by_name(NAME)
            .expect("every batch has the metric name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("the metric name column is Utf8");
        let time = batch
            .column_by_name(TIME)
            .expect("every batch has the time column")
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("the time column is a millisecond timestamp");
        for row in 0..batch.num_rows() {
            rows.push((
                host.value(row).to_string(),
                name.is_valid(row).then(|| name.value(row).to_string()),
                time.value(row),
                value.is_valid(row).then(|| value.value(row)),
            ));
        }
    }
    rows
}

/// The timestamps of every grid point, which a series filling the grid reports.
fn grid_timestamps() -> Vec<i64> {
    (0..GRID_LEN)
        .map(|index| GRID_START + index as i64 * GRID_STEP)
        .collect()
}

fn count(metrics: &MetricsSet, name: &str) -> usize {
    match metrics.sum_by_name(name) {
        Some(MetricValue::Count { count, .. }) => count.value(),
        other => panic!("no count named {name}: {other:?}"),
    }
}

/// The hosts of `rows`, deduplicated.
fn hosts_of(rows: &[Row]) -> HashSet<&str> {
    rows.iter().map(|row| row.0.as_str()).collect()
}

/// The node is an extension over the fused frame node, and it explains the query it came from.
#[test]
fn the_node_runs_the_selection_over_the_fused_frame_node() {
    let node = node();
    assert_eq!("VectorSelect: topk(2, x)", format!("{}", Explain(&node)));
    let [input] = node.inputs().try_into().expect("exactly one input");
    let LogicalPlan::Extension(extension) = input else {
        panic!("the frames come from an extension node: {input:?}");
    };
    let prefix = extension
        .node
        .as_any()
        .downcast_ref::<FusedSeries>()
        .expect("the prefix is the fused node");
    assert!(
        format!("{}", Prefix(prefix)).contains("evaluated frames"),
        "the prefix hands over frames, not a row table"
    );
    // The suffix is the root the compiler bound, over one input rather than over the child tree.
    let NativeExpr::Vector { op, input } = &node.suffix.root().expr else {
        panic!("the suffix root is the vector operation");
    };
    assert!(matches!(op, BoundVectorOp::TopK { k, top: true, .. } if *k == K as f64));
    assert!(
        matches!(
            &input.expr,
            NativeExpr::Input(BoundInput {
                ordering: SeriesOrdering::Contiguous,
                ..
            })
        ),
        "the frames arrive contiguous, so the suffix may read one series to its close: {:?}",
        input.expr
    );
    assert_eq!(
        node.schema().inner().as_ref(),
        output_schema().as_ref(),
        "the node reports the row table it converts to"
    );
}

/// Prints a logical node the way `EXPLAIN` prints it.
struct Explain<'a>(&'a VectorSelect);

impl Display for Explain<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.0.fmt_for_explain(f)
    }
}

/// Prints the fused node the way `EXPLAIN` prints it.
struct Prefix<'a>(&'a FusedSeries);

impl Display for Prefix<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.0.fmt_for_explain(f)
    }
}

/// Every shape other than a `topk` over one evaluated field is declined, not planned wrong.
///
/// The refusal is [`DataFusionError::NotImplemented`] and it happens before this node exists, which
/// is what lets a statement fall back to the existing PromQL planner. Each case below is one
/// condition the construction states.
#[test]
fn another_shape_is_declined_rather_than_planned_wrong() {
    let output = output_schema().to_dfschema_ref().unwrap();
    let expect_declined = |tree: NativeTree, binding: FusedSeriesBinding, why: &str| {
        let error = VectorSelect::try_new(
            Arc::from(PROMQL),
            tree,
            binding,
            input_plan(),
            output.clone(),
        )
        .expect_err(why);
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{error}"
        );
    };

    // A root that is not a vector operation.
    expect_declined(
        NativeTree::try_new(selection_child(raw_schema(), grid()))
            .expect("the child tree is legal"),
        binding(&selection_tree()),
        "a tree without a selection at its root is not this node's shape",
    );
    // A vector operation that is not a topk.
    expect_declined(
        sort_over(selection_child(raw_schema(), grid())),
        binding(&selection_tree()),
        "only a topk or bottomk root is answered here",
    );
    // A child that reads across series cannot be framed at all.
    expect_declined(
        topk_over(
            sort_over(selection_child(raw_schema(), grid()))
                .root()
                .clone(),
        ),
        binding(&selection_tree()),
        "a frame carries one series, not a vector",
    );
    // A child evaluated on another grid than the binding names.
    expect_declined(
        selection_tree(),
        FusedSeriesBinding {
            grid: another_grid(),
            ..binding(&selection_tree())
        },
        "the frames' positions are on the statement's grid",
    );

    // A vector of more than one field does not fit one frame's value column.
    let two_fields = Arc::new(SeriesSchema {
        fields: Arc::from([
            FieldSpec { name: VALUE.into() },
            FieldSpec {
                name: "other".into(),
            },
        ]),
        eval_grid: None,
    });
    let wide = topk_over(NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: NativeType::Samples(Arc::clone(&two_fields)),
                    ordering: SeriesOrdering::KeyThenTime,
                }),
                output: NativeType::Samples(Arc::clone(&two_fields)),
            }),
            op: BoundInstant {
                lookback_ms: LOOKBACK_MS,
                offset_ms: 0,
                output_grid: grid(),
            },
        },
        output: NativeType::Samples(Arc::new(SeriesSchema {
            fields: two_fields.fields.clone(),
            eval_grid: Some(grid()),
        })),
    });
    expect_declined(
        wide,
        FusedSeriesBinding {
            series_schema: Arc::clone(&two_fields),
            ..binding(&selection_tree())
        },
        "a frame carries one field",
    );

    // The refusal says what it refused, so a reader of a fallback knows why.
    assert!(
        unsupported("a tree")
            .to_string()
            .contains("VectorSelect does not handle")
    );
}

/// The sort below the node is the only ordering, and the optimizer keeps it.
///
/// The node states the exact requirement its sort satisfies and asks for one partition. A hash
/// requirement would make a hash exchange necessary as soon as the child has more than one
/// partition, and that exchange reorders the frames the sort just put in order.
#[test]
fn the_optimizer_keeps_the_single_ordered_input_it_was_built_with() {
    let node = node();
    let exec = plan_of(
        &node,
        memory_exec(
            closing_frames()
                .into_iter()
                .map(|frame| vec![frame])
                .collect(),
        ),
    )
    .expect("the runtime executes a topk");
    assert_eq!(1, exec.properties().output_partitioning().partition_count());

    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 8;
    let optimized = EnsureRequirements::new()
        .optimize(Arc::clone(&exec), &config)
        .expect("the rule accepts the plan");
    assert_eq!(
        1,
        optimized
            .properties()
            .output_partitioning()
            .partition_count(),
        "a global selection answers from one partition"
    );
    // `EnsureRequirements` may turn the coalesce-and-sort pair into per-partition sorts merged by
    // a `SortPreservingMergeExec`; either way the frames reach this node as one stream ordered by
    // producer and ordinal, and no sort cuts the stream with a fetch.
    let child = optimized.children()[0].clone();
    assert_eq!(
        1,
        child.properties().output_partitioning().partition_count(),
        "the frames reach the selection as one global stream"
    );
    let ordering = child
        .properties()
        .output_ordering()
        .unwrap_or_else(|| panic!("the child hands over ordered frames: {child:?}"));
    let keys = ordering
        .iter()
        .map(|sort| sort.expr.to_string())
        .collect::<Vec<_>>();
    assert_eq!(2, keys.len(), "{keys:?}");
    assert!(keys[0].contains("producer"), "{keys:?}");
    assert!(keys[1].contains("frame_ordinal"), "{keys:?}");
    assert!(ordering.iter().all(|sort| sort.options == super::ASCENDING));
    let mut stack = vec![child];
    let mut sorts = 0;
    while let Some(plan) = stack.pop() {
        if let Some(sort) = plan.downcast_ref::<SortExec>() {
            sorts += 1;
            assert!(
                sort.fetch().is_none(),
                "a fetch would cut the frame stream before every producer closed"
            );
        }
        stack.extend(plan.children().into_iter().cloned());
    }
    assert!(sorts > 0, "the frames are ordered by a sort in the plan");
    // The only fan-in is the frames themselves: the four frame partitions are merged, and the
    // source that produced them is untouched.
    let mut sources = 0;
    stack = vec![Arc::clone(&optimized)];
    while let Some(plan) = stack.pop() {
        if plan.name() == "DataSourceExec" {
            sources += 1;
            assert_eq!(4, plan.properties().output_partitioning().partition_count());
        }
        stack.extend(plan.children().into_iter().cloned());
    }
    assert_eq!(1, sources, "one frame source feeds the selection");
    let distributions = optimized.input_distribution_requirements().into_per_child();
    assert!(
        matches!(distributions.as_slice(), [Distribution::SinglePartition]),
        "the selection reads one vector: {distributions:?}"
    );
    assert_eq!(vec![false], optimized.benefits_from_input_partitioning());
    // The requirement names the sort's own keys, so `EnforceSorting` can neither drop the sort nor
    // re-derive an ordering this node does not read.
    let expected = LexRequirement::from([
        PhysicalSortRequirement::new(Arc::new(Column::new("producer", 0)), Some(super::ASCENDING)),
        PhysicalSortRequirement::new(
            Arc::new(Column::new("frame_ordinal", 1)),
            Some(super::ASCENDING),
        ),
    ]);
    assert_eq!(
        vec![Some(OrderingRequirements::new(expected))],
        optimized.required_input_ordering()
    );
}

/// Frames that arrive interleaved and rebatched are ordered before the selection reads them.
///
/// A producer's frames stay contiguous and in ordinal order once the sort has run, which is what
/// lets the frame reader check the sequence and the suffix close each series exactly once. The
/// frames here are rebatched into one batch in the wrong producer order, and they cover the shapes
/// the order has to keep: one producer that closes two series in a row - a close ends a series, not
/// the producer - one series that does not close until an empty closing frame, one sparse series,
/// and two producers that close on their first frame.
#[tokio::test]
async fn interleaved_and_rebatched_frames_are_ordered_before_the_selection() {
    let node = node();
    let frames = [
        (3, 0, full_chunk("web-3", 3.0, true)),
        (
            0,
            0,
            chunk(
                "web-0",
                &[(0, vec![Some(0.0); 3]), (5, vec![Some(0.0); 3])],
                true,
            ),
        ),
        (0, 1, full_chunk("alt-0", 0.5, true)),
        (2, 0, full_chunk("web-2", 2.0, true)),
        (1, 0, chunk("web-1", &[(0, vec![Some(1.0); 6])], false)),
        (1, 1, chunk("web-1", &[], true)),
    ];
    let shuffled = frames
        .iter()
        .map(|(producer, ordinal, chunk)| frame(chunk, *producer, *ordinal))
        .collect::<Vec<_>>();
    let batch =
        concat_batches(&evaluated_frame::schema(), &shuffled).expect("one batch of six frames");
    assert_eq!(frames.len(), batch.num_rows());

    let exec = plan_of(&node, memory_exec(vec![vec![batch]])).expect("the runtime executes a topk");
    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("one producer order once the sort ran");
    let rows = rows(&batches);
    assert_eq!(K * GRID_LEN, rows.len(), "{rows:?}");
    assert_eq!(
        HashSet::from(["web-2", "web-3"]),
        hosts_of(&rows),
        "topk(2) keeps the two best of every producer"
    );
    for (host, name, time, value) in &rows {
        let expected = if host == "web-3" { 3.0 } else { 2.0 };
        assert_eq!(Some(expected), *value, "{host} at {time}");
        assert_eq!(Some(METRIC), name.as_deref());
        assert!(*time >= GRID_START, "{time} is not a grid point");
    }
    // The kept series is answered at every grid point, and at each of them once.
    let mut times = rows
        .iter()
        .filter(|row| row.0 == "web-3")
        .map(|row| row.2)
        .collect::<Vec<_>>();
    times.sort_unstable();
    assert_eq!(grid_timestamps(), times);
}

/// The four producers' frames in one partition each, so the coalesce is exercised.
#[tokio::test]
async fn every_producer_contributes_to_the_global_selection() {
    let node = node();
    let exec = plan_of(
        &node,
        memory_exec(
            closing_frames()
                .into_iter()
                .map(|frame| vec![frame])
                .collect(),
        ),
    )
    .expect("the runtime executes a topk");
    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("four partitions become one ordered stream");
    let rows = rows(&batches);
    assert_eq!(K * GRID_LEN, rows.len(), "{rows:?}");
    assert_eq!(
        HashSet::from(["web-2", "web-3"]),
        hosts_of(&rows),
        "a per-partition selection would keep web-1's best instead"
    );
    assert_eq!(1, exec.properties().output_partitioning().partition_count());
}

/// Frames whose producer order is broken are refused, and nothing is answered from them.
#[tokio::test]
async fn a_broken_producer_sequence_fails_without_answers() {
    let node = node();
    // Producer 0 opens and does not close, and producer 1 starts anyway. The sort orders the frames;
    // it cannot close a producer that was cut off.
    let frames = vec![
        frame(&chunk("web-0", &[(0, vec![Some(1.0)])], false), 0, 0),
        frame(&full_chunk("web-1", 2.0, true), 1, 0),
    ];
    let exec = plan_of(&node, memory_exec(vec![frames])).expect("the runtime executes a topk");
    let mut stream = exec
        .execute(0, SessionContext::default().task_ctx())
        .expect("the stream starts");
    let mut batches = 0;
    let error = loop {
        match std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            Some(Ok(_)) => batches += 1,
            Some(Err(error)) => break error,
            None => panic!("the sequence violation was not reported"),
        }
    };
    assert_eq!(
        0, batches,
        "no answer may be handed out from a broken stream"
    );
    assert!(
        error.to_string().contains("producer"),
        "the failure names the sequence: {error}"
    );
    // The stream ended: the error is reported once, not on every poll.
    assert!(
        std::future::poll_fn(|cx| stream.as_mut().poll_next(cx))
            .await
            .is_none()
    );
}

/// An input that fails before it ended yields no selection at all.
///
/// The series the suffix holds were ranked from part of the vector, so a `topk` computed from them
/// is not the query's answer. The error is reported once, and the terminated stream releases the
/// execution and every frame it held.
#[tokio::test]
async fn an_input_error_before_the_end_yields_no_selection() {
    let node = node();
    let dropped = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let frames = closing_frames();
    let source = staged(evaluated_frame::schema(), {
        let dropped = Arc::clone(&dropped);
        let failed = Arc::clone(&failed);
        move || {
            Box::pin(
                StagedStream::new(evaluated_frame::schema(), frames.clone())
                    .error("the scan failed")
                    .dropped(&dropped)
                    .failed(&failed),
            )
        }
    });
    let exec = plan_of(&node, source).expect("the runtime executes a topk");
    let mut stream = exec
        .execute(0, SessionContext::default().task_ctx())
        .expect("the stream starts");
    let mut batches = 0;
    let reported = loop {
        match std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            Some(Ok(_)) => batches += 1,
            Some(Err(error)) => break error,
            None => panic!("the input error was not reported"),
        }
    };
    assert_eq!(0, batches, "a partial vector has no global topk");
    assert!(
        reported.to_string().contains("the scan failed"),
        "{reported}"
    );
    assert!(
        failed.load(AtomicOrdering::SeqCst),
        "the input was read to its failure"
    );
    assert!(
        dropped.load(AtomicOrdering::SeqCst),
        "the terminated stream released its input"
    );
}

/// A pool too small for a frame fails the stream and releases everything it held.
///
/// The node is built directly over the frames, without the sort below it, so the only charge on the
/// pool is this node's own: the frame batch it holds. What the plan owns - the label-set ledger -
/// has a life of its own, so this checks the pool after the failed stream is gone.
#[tokio::test]
async fn a_pool_too_small_for_a_frame_releases_the_stream_it_failed() {
    let node = node();
    let exec = VectorSelectExec::try_new(
        Arc::from(PROMQL),
        memory_exec(vec![closing_frames()]),
        Arc::clone(&node.suffix),
        evaluated_schema(),
        output_schema(),
        TIME.to_string(),
        vec![HOST.to_string(), NAME.to_string()],
    )
    .expect("the runtime executes a topk");
    let pool = Arc::new(GreedyMemoryPool::new(1));
    let context = task_context(Arc::clone(&pool) as Arc<dyn MemoryPool>);
    let mut stream = exec.execute(0, context).expect("the stream starts");
    let error = match std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        Some(Ok(batch)) => panic!("a one-byte pool admitted {} rows", batch.num_rows()),
        Some(Err(error)) => error,
        None => panic!("the pool refused the frame without reporting it"),
    };
    assert!(
        error.to_string().to_lowercase().contains("memory"),
        "the failure is the pool's: {error}"
    );
    assert!(
        std::future::poll_fn(|cx| stream.as_mut().poll_next(cx))
            .await
            .is_none(),
        "the failed stream reports its error once"
    );
    drop(stream);
    assert_eq!(
        0,
        pool.reserved(),
        "the failed stream released the runtime and the frame it held"
    );
    drop(exec);
    assert_eq!(
        0,
        pool.reserved(),
        "the plan's ledger held nothing to release"
    );
}

/// Two series that ended on one label set are legal while their points are disjoint.
///
/// Prometheus merges such fragments into one series, and this node emits them separately, so no
/// sample is lost. [`a_selection_that_lands_two_values_on_one_point_fails`] covers the case that is
/// refused.
#[tokio::test]
async fn one_label_set_over_disjoint_points_is_emitted() {
    let node = node();
    let half = GRID_LEN / 2;
    // One host, two occurrences: the first holds the early points and the second the late ones.
    let early = chunk("same", &[(0, vec![Some(10.0); half])], true);
    let late = chunk("same", &[(half, vec![Some(1.0); half])], true);
    let exec = plan_of(
        &node,
        memory_exec(vec![vec![frame(&early, 0, 0), frame(&late, 1, 0)]]),
    )
    .expect("the runtime executes a topk");
    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("disjoint points of one label set are legal");
    let rows = rows(&batches);
    assert_eq!(GRID_LEN, rows.len(), "{rows:?}");
    assert!(rows.iter().all(|row| row.0 == "same"));
}

/// Two kept series that hold a value at the same point of the same field are refused.
#[tokio::test]
async fn a_selection_that_lands_two_values_on_one_point_fails() {
    let node = node();
    let exec = plan_of(
        &node,
        memory_exec(vec![vec![
            frame(&full_chunk("same", 2.0, true), 0, 0),
            frame(&full_chunk("same", 1.0, true), 1, 0),
        ]]),
    )
    .expect("the runtime executes a topk");
    let error = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect_err("one point cannot hold two values of one field");
    assert!(
        error.to_string().contains("same labelset"),
        "the failure names the label set: {error}"
    );
}

/// The plan reports the runtime's own metrics, and only from executions it actually ran.
#[tokio::test]
async fn the_plan_reports_the_runtime_root_and_its_input() {
    let node = node();
    let exec =
        plan_of(&node, memory_exec(vec![closing_frames()])).expect("the runtime executes a topk");
    assert_eq!(
        0,
        exec.metrics()
            .expect("a plan that has not run reports an empty set")
            .iter()
            .count()
    );
    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("the four series are ranked");
    assert_eq!(K * GRID_LEN, rows(&batches).len());

    // The stream is gone and the plan still reports what its execution did: the root is the
    // selection's runtime, and the input below it closed four series.
    let metrics = exec
        .metrics()
        .expect("the node reports its runtime's metrics");
    assert_eq!(2, count(&metrics, "native.root.completed_series"));
    assert_eq!(4, count(&metrics, "native.root.input.completed_series"));

    // A rebuilt node keeps the same tree and reports nothing until it runs, as the fused node does.
    let rebuilt = Arc::clone(&exec)
        .replace_children(
            vec![memory_exec(vec![closing_frames()])],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .expect("the node rebuilds over another input");
    assert_eq!(
        0,
        rebuilt
            .metrics()
            .expect("a rebuilt node reports an empty set")
            .iter()
            .count()
    );
    assert_eq!(
        2,
        count(&exec.metrics().unwrap(), "native.root.completed_series")
    );
}

/// An ended stream gives the series schema back, even when a consumer keeps it.
///
/// The schema is what names the statement's grid, and it is the only thing the stream could still
/// pin after its last frame: the plan holds its own copy and keeps it, but the stream does not.
#[tokio::test]
async fn an_ended_stream_gives_up_the_schema_it_read() {
    let node = node();
    let schema = evaluated_schema();
    let exec = VectorSelectExec::try_new(
        Arc::from(PROMQL),
        memory_exec(vec![closing_frames()]),
        Arc::clone(&node.suffix),
        Arc::clone(&schema),
        output_schema(),
        TIME.to_string(),
        vec![HOST.to_string(), NAME.to_string()],
    )
    .expect("the runtime executes a topk");
    let weak = Arc::downgrade(&schema);
    let mut stream = exec
        .execute(0, SessionContext::default().task_ctx())
        .expect("the stream starts");
    while let Some(batch) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        assert!(batch.expect("the frames rank").num_rows() > 0);
    }
    // The plan keeps its own copy for its metrics; the ended stream gives its copy back, so
    // dropping the plan leaves this test's handle as the only one while the stream is still alive.
    drop(exec);
    assert_eq!(
        1,
        Arc::strong_count(&schema),
        "the ended stream still pins the grid it read"
    );
    drop(stream);
    assert!(weak.upgrade().is_some(), "this test still holds the schema");
    drop(schema);
    assert!(weak.upgrade().is_none());
}

/// An output column the conversion cannot fill is refused when the node is built.
#[test]
fn an_output_column_of_no_known_origin_is_refused() {
    let node = VectorSelect::try_new(
        Arc::from(PROMQL),
        selection_tree(),
        binding(&selection_tree()),
        input_plan(),
        Arc::new(Schema::new(vec![
            Field::new(VALUE, DataType::Float64, true),
            Field::new("unknown", DataType::Utf8, true),
            Field::new(
                TIME,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]))
        .to_dfschema_ref()
        .unwrap(),
    )
    .expect("the logical node only records the schema");
    // The schema is only read where the conversion is chosen, so the refusal is the physical build.
    let error = plan_of(&node, memory_exec(vec![closing_frames()]))
        .expect_err("the conversion has no column to put there");
    assert!(error.to_string().contains("unknown column"), "{error}");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    // A frame with no second column has no frame ordinal to order by.
    assert!(frame_ordering(&Schema::empty()).is_err());
}

/// An input that is not a frame layout, and one that is not one global input, are refused.
#[test]
fn an_input_that_is_not_one_global_frame_stream_is_refused() {
    let node = node();
    // Two columns of another type are not the frame layout, though they are enough to order by.
    let wrong = Arc::new(Schema::new(vec![
        Field::new("producer", DataType::Int64, false),
        Field::new("frame_ordinal", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&wrong),
        vec![
            Arc::new(Int64Array::from(vec![0i64])) as _,
            Arc::new(Int64Array::from(vec![0i64])) as _,
        ],
    )
    .unwrap();
    let source = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[vec![batch]], wrong, None).unwrap(),
    ))) as Arc<dyn ExecutionPlan>;
    let error = plan_of(&node, source).expect_err("the frames are a fixed layout");
    assert!(error.to_string().contains("not that layout"), "{error}");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");

    // A two-partition input is refused as well: this node reads one global input, and the plan it
    // builds is what brings the partitions together.
    let suffix = Arc::clone(&node.suffix);
    let frames = closing_frames();
    let error = VectorSelectExec::try_new(
        Arc::from(PROMQL),
        memory_exec(vec![frames.clone(), frames]),
        suffix,
        evaluated_schema(),
        output_schema(),
        TIME.to_string(),
        vec![HOST.to_string(), NAME.to_string()],
    )
    .expect_err("one global input cannot be read from two partitions");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
}
