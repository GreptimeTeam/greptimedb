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

//! What this node has to get right: the two `EXPLAIN` lines it shows, the batches it runs end to
//! end, and what it refuses to run.
//!
//! The trees here are built node by node with the same bindings the PromQL compiler produces for
//! `rate(x[5m])`: the compiler itself lives in the `query` crate, which depends on this one, so a
//! test in this crate cannot call it.
//!
//! The input is what a scan hands over: native raw series frames, one batch per chunk here, packed
//! by the same code the scan uses ([`pack_raw_series_chunk`]). A frame carries the series' encoded
//! key, the chunk's finality, its timestamps and its fields, plus the complete labels, so a test
//! says which series a chunk belongs to and whether it closes it instead of relying on where a row
//! boundary happens to fall.
//!
//! Kernel numerics are not the subject: `rate` is checked against a counter that advances one
//! second per second, so the value it has to produce is exactly 1 (or 2) at every grid point and
//! any position or finalization mistake shows up as a wrong, missing or duplicated row.

use std::collections::VecDeque;
use std::fmt::{self, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{SendableEvaluatedSeriesStream, pack_evaluated_series};
use common_query::series_frame::{series_frame_ordering, unpack_evaluated_series};
use datafusion::arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ToDFSchema;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    Partitioning, PlanProperties, RecordBatchStream, ReplaceChildrenOptions,
    SendableRecordBatchStream, collect, displayable,
};
use datafusion::prelude::SessionContext;
use datatypes::prelude::ConcreteDataType;
use datatypes::series::frame::{
    FrameContent, FrameLabelMode, LABEL_DECODE_VERSION, LabelDecodeColumn, LabelDecodeEncoding,
    LabelDecodeSpec, LabelDecodeTag, SeriesFrameBuilder, SeriesFrameDeclaration,
    pack_raw_series_chunk, raw_series_frame_schema, series_frame_schema,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, SeriesLabels, SeriesOrdering};
use futures::{Stream, StreamExt, TryStreamExt};
use store_api::region_engine::RawLabelSource;

use super::{
    FusedOutputFormat, FusedSeries, FusedSeriesBinding, FusedSeriesExec, LabelSets, OutputColumn,
    POLL_BUDGET, evaluated_output, label_set, occupied_runs, series_to_record_batch,
};
use crate::evaluated::EvaluatedChunk;
use crate::map::MathOp;
use crate::map_label::LabelRewrite;
use crate::native::execute::{NativeExecutionState, NativeExecutor};
use crate::native::plan::{
    AggregateKind, AggregateStage, AggregateStateSchema, BoundAggregate, BoundInput, BoundInstant,
    BoundMap, BoundScalar, BoundWindow, Grouping, MapOp, NativeExpr, NativeNode, NativeTree,
    NativeType,
};
use crate::native::runs::absorb_rebuilds;
use crate::range_eval::{RangeFunction, RangeWindow};

/// The query every tree here is compiled from.
const PROMQL: &str = "rate(x[5m])";
/// The range the statement's window selects, which is the `5m` of [`PROMQL`].
const RANGE_MS: i64 = 300_000;
/// The statement's grid: `rate(x[5m])` evaluated every minute from five minutes in.
const GRID_START: i64 = 300_000;
const GRID_STEP: i64 = 60_000;
const GRID_LEN: u64 = 5;
/// One raw sample per minute, from 0 to 9 minutes: enough to fill every window on the grid.
const SAMPLE_STEP: i64 = 60_000;
const SAMPLES: i64 = 10;
/// How many one-sample chunks the quiet series below arrives in.
///
/// Two bounds hold at once: the series reaches the end of the last grid point's window (540s, so
/// indices `0..=540`), which is what lets it rate at 1 over the whole grid, and it arrives in more
/// batches than one poll's budget, which is what makes a poll that ignored the budget visible.
const QUIET_SAMPLES: i64 = 541;
/// One second between those samples, so the series rates at 1 once it is read to its end.
///
/// The step is a second and not a minute on purpose: at a minute the series would arrive in ten
/// batches, fewer than [`POLL_BUDGET`], and the budget the test above exists to check would never
/// be reached.
const QUIET_STEP: i64 = 1_000;

const TIME: &str = "time";
const VALUE: &str = "value";
const OTHER: &str = "other";
const HOST: &str = "host";
const ZONE: &str = "zone";
const NAME: &str = "__name__";
/// A metadata label, which the window drops along with the metric name.
const TYPE: &str = "__type__";
const METRIC: &str = "http_requests_total";

fn grid() -> Arc<EvalGrid> {
    Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN).unwrap()).unwrap(),
    )
}

/// The raw schema the tree's input is bound to: one float field called [`VALUE`].
fn raw_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec { name: VALUE.into() }]),
        eval_grid: None,
    })
}

/// A raw schema of two float fields, so one series can fill one of them and another the other.
fn two_field_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([
            FieldSpec { name: VALUE.into() },
            FieldSpec { name: OTHER.into() },
        ]),
        eval_grid: None,
    })
}

/// The tree `rate(x[5m])` compiles to over `schema`: raw samples folded by one window onto the grid.
fn rate_tree_of(schema: Arc<SeriesSchema>) -> NativeTree {
    let grid = grid();
    // One schema shared by the window's binding and its output, which is what `NativeTree`'s
    // validation compares by identity.
    let evaluated = NativeType::Samples(Arc::new(SeriesSchema {
        fields: schema.fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    }));
    let raw = NativeType::Samples(schema);
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: raw.clone(),
                    ordering: SeriesOrdering::KeyThenTime,
                }),
                output: raw,
            }),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: RANGE_MS,
                    offset_ms: 0,
                    anchor_ms: None,
                },
                // The input is raw samples, so it has no grid of its own.
                input_grid: None,
                output_grid: grid,
            },
        },
        output: evaluated,
    })
    .unwrap()
}

/// The tree `rate(x[5m])` compiles to over one field.
fn rate_tree() -> NativeTree {
    rate_tree_of(raw_schema())
}

/// An instant selection feeding a range window.
fn instant_window_tree() -> NativeTree {
    let instant = instant_tree().root().clone();
    let grid = instant
        .output
        .samples()
        .expect("an instant selection produces samples")
        .eval_grid
        .clone()
        .expect("an instant selection's output is evaluated on a grid");
    let evaluated = instant.output.clone();
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(instant),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: RANGE_MS,
                    offset_ms: 0,
                    anchor_ms: None,
                },
                input_grid: Some(Arc::clone(&grid)),
                output_grid: grid,
            },
        },
        output: evaluated,
    })
    .expect("the bound tree is legal")
}

/// Two windows stacked, `rate(last_over_time(x[1s])[2s])`, which no compiler produces.
///
/// A range function has to apply directly to a selector, and a selector is raw samples, so a tree
/// with a window above a window is only reachable by writing it down by hand - which is a tree
/// [`FusedSeriesExec`](super::FusedSeriesExec) accepts, and therefore one it has to answer correctly.
///
/// The inner window is the interesting one: `last_over_time` keeps `__name__`, so the pair it can
/// split is not two series whose names it dropped but `{host="h", zone=""}` and `{host="h"}`, one
/// canonical label set and two raw keys. Its fragments are not merged unless the node above it
/// merges them, and that merge happens once per execution: with one execution per partition each
/// partition sees one fragment, so the outer window reads one half of the pair.
fn nested_window_tree() -> NativeTree {
    let grid = grid();
    // The inner window's output, which is also the outer window's output: a window reports one value
    // per input field on its own grid.
    let evaluated = NativeType::Samples(Arc::new(SeriesSchema {
        fields: raw_schema().fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    }));
    let raw = NativeType::Samples(raw_schema());
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(NativeNode {
                expr: NativeExpr::Window {
                    input: Box::new(NativeNode {
                        expr: NativeExpr::Input(BoundInput {
                            output: raw.clone(),
                            ordering: SeriesOrdering::KeyThenTime,
                        }),
                        output: raw,
                    }),
                    op: BoundWindow {
                        function: RangeFunction::LastOverTime,
                        window: RangeWindow {
                            range_ms: 1_000,
                            offset_ms: 0,
                            anchor_ms: None,
                        },
                        // The inner window's input is raw samples, so it has no grid of its own.
                        input_grid: None,
                        output_grid: Arc::clone(&grid),
                    },
                },
                output: evaluated.clone(),
            }),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: 2_000,
                    offset_ms: 0,
                    anchor_ms: None,
                },
                // The outer window reads the inner one's evaluated output, on that output's grid,
                // and reports one value per field there like the inner one does.
                input_grid: Some(Arc::clone(&grid)),
                output_grid: Arc::clone(&grid),
            },
        },
        output: evaluated,
    })
    .expect("the bound tree is legal")
}

/// The same tree asking for `quantile`, whose state the runtime does not keep yet.
fn quantile_aggregate_tree() -> NativeTree {
    let complete = aggregate_tree();
    let root = complete.root();
    let NativeExpr::Aggregate { input, op } = &root.expr else {
        unreachable!("the fixture is an aggregate over a window");
    };
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Aggregate {
            input: input.clone(),
            op: BoundAggregate {
                kind: AggregateKind::Quantile { quantile: 0.5 },
                group: op.group.clone(),
                stage: AggregateStage::Complete,
            },
        },
        output: root.output.clone(),
    })
    .expect("the bound tree is legal")
}

/// The `rate(x[5m])` tree with a `sum by (job)` over it.
///
/// A `Complete` aggregate: every contribution to each group lies inside one execution.
fn aggregate_tree() -> NativeTree {
    let rate = rate_tree();
    let window = rate.root().clone();
    let grid = window
        .output
        .samples()
        .expect("a window produces samples")
        .eval_grid
        .clone()
        .expect("a window's output is evaluated on a grid");
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: NativeType::Samples(Arc::new(SeriesSchema {
            fields: raw_schema().fields.clone(),
            eval_grid: Some(grid),
        })),
    })
    .expect("the bound tree is legal")
}

/// The tree an instant selection compiles to: the last raw sample at or before each grid point.
///
/// Unlike a window or a map, a selection rewrites no label: the series it answers carries the labels
/// its input did.
fn instant_tree() -> NativeTree {
    let grid = grid();
    let evaluated = NativeType::Samples(Arc::new(SeriesSchema {
        fields: raw_schema().fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    }));
    let raw = NativeType::Samples(raw_schema());
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: raw.clone(),
                    ordering: SeriesOrdering::KeyThenTime,
                }),
                output: raw,
            }),
            op: BoundInstant {
                lookback_ms: RANGE_MS,
                offset_ms: 0,
                output_grid: grid,
            },
        },
        output: evaluated,
    })
    .expect("the bound tree is legal")
}

/// What the tree's input was compiled against, for the input schema and output tags given.
fn binding_of(
    tree: &NativeTree,
    series_schema: Arc<SeriesSchema>,
    tags: Vec<String>,
) -> FusedSeriesBinding {
    FusedSeriesBinding {
        series_schema,
        grid: tree
            .root()
            .output
            .samples()
            .expect("this tree produces samples")
            .eval_grid
            .clone()
            .expect("this tree's output is evaluated on a grid"),
        metric_name: Some(Arc::from(METRIC)),
        time_index: TIME.to_string(),
        tags,
    }
}

/// What the tree's input was compiled against, read off the tree itself.
fn binding(tree: &NativeTree) -> FusedSeriesBinding {
    binding_of(tree, raw_schema(), vec![HOST.to_string(), NAME.to_string()])
}

/// The schema of a frame carrying series of `schema` with their labels explicit.
fn frame_schema_of(schema: &SeriesSchema) -> SchemaRef {
    raw_series_frame_schema(schema, FrameLabelMode::Explicit).expect("explicit labels are a frame")
}

/// The schema of a frame carrying these series, which is what the node declares as its input.
fn frame_schema() -> SchemaRef {
    frame_schema_of(&raw_schema())
}

/// The description a frame with its labels encoded in a dense [`HOST`] key declares.
fn host_label_decode() -> LabelDecodeSpec {
    LabelDecodeSpec {
        version: LABEL_DECODE_VERSION,
        metric_name: METRIC.to_string(),
        encoding: LabelDecodeEncoding::Dense,
        primary_key: vec![LabelDecodeColumn {
            column_id: 1,
            name: HOST.to_string(),
            physical_type: ConcreteDataType::string_datatype(),
        }],
        logical_tags: vec![LabelDecodeTag {
            column_id: 1,
            name: HOST.to_string(),
        }],
    }
}

/// The schema of a frame carrying series of `schema` whose labels are encoded in their key.
fn keyed_frame_schema_of(schema: &SeriesSchema) -> SchemaRef {
    series_frame_schema(schema, FrameLabelMode::Key, Some(&host_label_decode()))
        .expect("key labels are a frame")
}

/// The columns this node produces: the field, the tags and the grid timestamps.
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

fn input_plan(schema: SchemaRef) -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: schema.to_dfschema_ref().unwrap(),
    })
}

/// A physical input producing `batches` in one partition, with `schema` as its schema.
fn memory_exec_with(schema: SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[batches], schema, None).unwrap(),
    )))
}

/// A physical input producing `batches` in one partition, on the frame schema of these tests.
fn memory_exec(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    memory_exec_with(frame_schema(), batches)
}

/// A physical input producing each of `partitions`' batches in a partition of its own, with
/// `schema` as its schema.
fn memory_exec_partitioned_with(
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&partitions, schema, None)
            .expect("every partition holds batches of the given frame schema"),
    )))
}

/// A physical input producing each of `partitions`' batches in a partition of its own.
fn memory_exec_partitioned(partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    memory_exec_partitioned_with(frame_schema(), partitions)
}

/// A task context whose memory pool is `pool`, so what the node charges can be read afterwards.
fn task_context(pool: &Arc<GreedyMemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool) as Arc<dyn MemoryPool>)
        .build()
        .expect("the runtime environment builds");
    Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
}

/// A physical input whose one partition's stream `make` builds.
fn staged(
    schema: SchemaRef,
    make: impl Fn() -> SendableRecordBatchStream + Send + Sync + 'static,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(StagedPlan {
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
        make: Box::new(make),
    })
}

/// A physical input, so a test can stage what a memory source cannot: a pending poll, an error
/// after some rows, and a stream whose drop it can see.
struct StagedPlan {
    make: Box<dyn Fn() -> SendableRecordBatchStream + Send + Sync>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for StagedPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StagedPlan").finish()
    }
}

impl DisplayAs for StagedPlan {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for StagedPlan {
    fn name(&self) -> &str {
        "StagedPlan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // A scan answers the partitions it declares and no others, which is what makes an index
        // past this plan's count an error the node above has to pass on.
        if partition >= self.properties.partitioning.partition_count() {
            return Err(DataFusionError::Execution(format!(
                "StagedPlan has no partition {partition}"
            )));
        }
        Ok((self.make)())
    }
}

/// The stream a [`StagedPlan`] hands over: `batches`, then an optional error, with a pending poll
/// and a drop a test can observe.
struct StagedStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    handed: usize,
    /// Hand this many batches, then answer [`Poll::Pending`] once.
    pending_after: Option<usize>,
    /// The error to report once the batches are exhausted.
    error: Option<&'static str>,
    /// Set when this stream is dropped, so a test can see the input being released.
    dropped: Option<Arc<AtomicBool>>,
    /// Counts the batches this stream has handed over, so a test can see how far one poll read.
    handed_out: Option<Arc<AtomicUsize>>,
}

impl StagedStream {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into(),
            handed: 0,
            pending_after: None,
            error: None,
            dropped: None,
            handed_out: None,
        }
    }

    fn pending_after(mut self, batches: usize) -> Self {
        self.pending_after = Some(batches);
        self
    }

    fn error(mut self, message: &'static str) -> Self {
        self.error = Some(message);
        self
    }

    fn dropped(mut self, flag: &Arc<AtomicBool>) -> Self {
        self.dropped = Some(Arc::clone(flag));
        self
    }

    fn counted(mut self, counter: &Arc<AtomicUsize>) -> Self {
        self.handed_out = Some(Arc::clone(counter));
        self
    }
}

impl Stream for StagedStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.pending_after == Some(this.handed) {
            this.pending_after = None;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        match this.batches.pop_front() {
            Some(batch) => {
                this.handed += 1;
                if let Some(counter) = &this.handed_out {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                Poll::Ready(Some(Ok(batch)))
            }
            None => match this.error.take() {
                Some(message) => {
                    Poll::Ready(Some(Err(DataFusionError::Execution(message.to_string()))))
                }
                None => Poll::Ready(None),
            },
        }
    }
}

impl RecordBatchStream for StagedStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for StagedStream {
    fn drop(&mut self) {
        if let Some(dropped) = &self.dropped {
            dropped.store(true, Ordering::SeqCst);
        }
    }
}

fn fused(tree: NativeTree) -> FusedSeries {
    let binding = binding(&tree);
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(frame_schema()),
        output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::PlainRows,
    )
    .expect("the binding matches the tree")
}

/// The physical node of `node` over `input`, reading the complete labels every frame carries.
fn plan_of(
    node: FusedSeries,
    input: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    node.to_execution_plan(input, RawLabelSource::Explicit, SeriesOrdering::Contiguous)
}

/// The physical node of `tree` over `input`.
fn plan(
    tree: NativeTree,
    input: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    plan_of(fused(tree), input)
}

/// The timestamps of `rows`.
fn times_of(rows: &[(i64, f64)]) -> Vec<i64> {
    rows.iter().map(|(time, _)| *time).collect()
}

/// The values of `rows`, one sample per timestamp.
fn values_of(rows: &[(i64, f64)]) -> Vec<Option<f64>> {
    rows.iter().map(|(_, value)| Some(*value)).collect()
}

/// One missing sample per timestamp of `rows`, which is a field the series holds no value in.
fn no_values(rows: &[(i64, f64)]) -> Vec<Option<f64>> {
    rows.iter().map(|_| None).collect()
}

/// One chunk of `metric`, keyed `key`, holding `times` and one sample per field of `schema`.
fn chunk_of(
    schema: &SeriesSchema,
    key: &str,
    metric: SeriesLabels,
    times: &[i64],
    values: Vec<Vec<Option<f64>>>,
    last: bool,
) -> RawSeriesChunk {
    assert_eq!(
        schema.fields.len(),
        values.len(),
        "one value list per field of the schema"
    );
    assert!(
        values.iter().all(|values| values.len() == times.len()),
        "one sample per timestamp per field"
    );
    let fields = values
        .into_iter()
        .map(|values| Samples::from_float(Float64Array::from(values)))
        .collect::<Vec<_>>();
    RawSeriesChunk::try_new(
        schema,
        Buffer::from_slice_ref(key.as_bytes()),
        Some(metric),
        RawTimestamps::try_new(ScalarBuffer::from(times.to_vec())).expect("ascending timestamps"),
        SeriesFields::try_from_vec(fields).expect("one sample per timestamp per field"),
        last,
    )
    .expect("one series' samples are a legal raw chunk")
}

/// One chunk of the series `{__name__=METRIC, host=<host>}`: `rows` as its samples, and the chunk
/// that closes the series when `last`. The host names the series, so it doubles as the key.
fn series_chunk(host: &str, rows: &[(i64, f64)], last: bool) -> RawSeriesChunk {
    chunk_of(
        &raw_schema(),
        host,
        metric(&[(HOST, host)], Some(METRIC)),
        &times_of(rows),
        vec![values_of(rows)],
        last,
    )
}

/// The metric these tests read, with `labels` on it and the name given.
fn metric(labels: &[(&str, &str)], name: Option<&str>) -> SeriesLabels {
    SeriesLabels::try_new(
        name.map(Arc::from),
        labels
            .iter()
            .map(|(name, value)| Label {
                name: Arc::from(*name),
                value: Arc::from(*value),
            })
            .collect(),
    )
    .expect("the labels of these tests are legal")
}

/// One frame batch carrying `chunk` of `schema`, packed by the scan's own encoder.
fn frame_of(schema: &SeriesSchema, chunk: &RawSeriesChunk) -> RecordBatch {
    let mut batches = pack_raw_series_chunk(schema, FrameLabelMode::Explicit, None, chunk)
        .expect("a chunk of this schema packs into a frame");
    assert_eq!(1, batches.len(), "one small chunk is one frame batch");
    batches.remove(0)
}

/// One frame batch carrying `chunk` of the one-field schema of these tests.
fn frame(chunk: &RawSeriesChunk) -> RecordBatch {
    frame_of(&raw_schema(), chunk)
}

/// The frames carrying `chunks` of `schema`, one batch each, so a test can say how much input one
/// poll reads.
fn frames_of(schema: &SeriesSchema, chunks: &[RawSeriesChunk]) -> Vec<RecordBatch> {
    chunks.iter().map(|chunk| frame_of(schema, chunk)).collect()
}

/// The frames carrying `chunks` of the one-field schema of these tests, one batch each.
fn frames(chunks: &[RawSeriesChunk]) -> Vec<RecordBatch> {
    frames_of(&raw_schema(), chunks)
}

/// One series' samples: one per minute from 0, `scale` seconds per second.
fn series_samples(scale: f64) -> Vec<(i64, f64)> {
    (0..SAMPLES)
        .map(|index| {
            let time = index * SAMPLE_STEP;
            (time, scale * time as f64 / 1000.0)
        })
        .collect()
}

/// Two series, each split over two chunks. A series' chunks stay adjacent, and the second of each
/// pair closes it, which is the input contract: a series' chunks arrive together.
fn two_series_frames() -> Vec<RecordBatch> {
    let first = series_samples(1.0);
    let second = series_samples(2.0);
    frames(&[
        series_chunk("web-1", &first[..3], false),
        series_chunk("web-1", &first[3..], true),
        series_chunk("web-2", &second[..3], false),
        series_chunk("web-2", &second[3..], true),
    ])
}

/// One series split over three chunks, so its chunks have to be stitched.
fn one_series_frames() -> Vec<RecordBatch> {
    let rows = series_samples(1.0);
    frames(&[
        series_chunk("web-1", &rows[..4], false),
        series_chunk("web-1", &rows[4..7], false),
        series_chunk("web-1", &rows[7..], true),
    ])
}

/// The rows of `batches`: timestamp, value, host and metric name.
type Row = (i64, Option<f64>, Option<String>, Option<String>);

fn rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let time = batch
            .column_by_name(TIME)
            .expect("every batch has a time column")
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("the time column is a millisecond timestamp");
        let value = batch
            .column_by_name(VALUE)
            .expect("every batch has the field column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("the field column is Float64");
        let host = batch
            .column_by_name(HOST)
            .expect("every batch has the tag column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("the tag column is Utf8");
        let name = batch
            .column_by_name(NAME)
            .expect("every batch has the metric name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("the metric name column is Utf8");
        for row in 0..batch.num_rows() {
            rows.push((
                time.value(row),
                value.is_valid(row).then(|| value.value(row)),
                host.is_valid(row).then(|| host.value(row).to_string()),
                name.is_valid(row).then(|| name.value(row).to_string()),
            ));
        }
    }
    rows
}

/// The output of [`zone_node`]: the field, `host`, `zone`, the dropped name and the grid times.
fn zone_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(ZONE, DataType::Utf8, true),
        Field::new(NAME, DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

/// `rate(x[5m])` over frames that carry `zone` as well as `host`, with both in the output.
fn zone_node() -> FusedSeries {
    let tree = rate_tree();
    let binding = binding_of(
        &tree,
        raw_schema(),
        vec![HOST.to_string(), ZONE.to_string(), NAME.to_string()],
    );
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(frame_schema()),
        zone_output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::PlainRows,
    )
    .expect("the binding matches the tree")
}

/// The instant-selection node over frames that carry `zone` as well as `host`.
///
/// Its output labels are the input's, so an empty-valued label reaches this node's own state as the
/// frame wrote it.
fn instant_node() -> FusedSeries {
    let tree = instant_tree();
    let binding = binding_of(
        &tree,
        raw_schema(),
        vec![HOST.to_string(), ZONE.to_string(), NAME.to_string()],
    );
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(frame_schema()),
        zone_output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::PlainRows,
    )
    .expect("the binding matches the tree")
}

/// The two chunks of the collision: one series once with an empty `zone` and once with no `zone`.
///
/// They are one identity to Prometheus — an empty label value is no label — but two different raw
/// series: the scan encodes the empty value and the absent one differently, so they reach the
/// runtime as two series that close on the same label set.
fn zone_chunks() -> Vec<RawSeriesChunk> {
    let rows = series_samples(1.0);
    vec![
        chunk_of(
            &raw_schema(),
            "zone-empty",
            metric(&[(HOST, "web-1"), (ZONE, "")], Some(METRIC)),
            &times_of(&rows),
            vec![values_of(&rows)],
            true,
        ),
        chunk_of(
            &raw_schema(),
            "zone-absent",
            metric(&[(HOST, "web-1")], Some(METRIC)),
            &times_of(&rows),
            vec![values_of(&rows)],
            true,
        ),
    ]
}

/// The canonical label set these tests collide on, as the runtime renders it in its refusal.
const DUPLICATE_LABEL_SET: &str =
    "the node at root produces two series under the label set [(\"host\", \"web-1\")]";

/// Two raw series that are already one Prometheus identity are refused by the runtime that would
/// have to answer both.
///
/// `{host="web-1", zone=""}` and `{host="web-1"}` are one identity to Prometheus: an empty label
/// value is no label, so the two chunks are the same series and `rate` creates no collision here.
/// What the two *raw* keys differ in is the empty `zone` against the absent one, which the scan
/// encodes differently and the decoder therefore opens two input series for. `rate` drops the
/// metric name and the empty value, so the second raw series arrives on the canonical label set the
/// first one already occupies; the refusal names that label set rather than answering it, because
/// answering would report one point twice.
#[tokio::test]
async fn differing_raw_keys_that_close_on_one_label_set_are_refused() {
    let exec = plan_of(zone_node(), memory_exec(frames(&zone_chunks())))
        .expect("the runtime executes a rate tree");
    let error = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect_err("the second raw key cannot be answered under the first one's label set");
    assert!(
        error.to_string().contains(DUPLICATE_LABEL_SET),
        "refused for another reason: {error}"
    );
}

/// A runtime error after buffered rows is reported, and the terminated stream releases its input.
///
/// The decoder holds the batch it is reading, the chunks it decoded from it and its own source, so a
/// stream that kept its input after the error would hold all three until the consumer drops it. The
/// collision above is the error: it happens on the second series, after the first one's rows have
/// been handed out.
#[tokio::test]
async fn a_runtime_error_after_rows_releases_the_input() {
    let dropped = Arc::new(AtomicBool::new(false));
    let schema = frame_schema();
    let batches = frames(&zone_chunks());
    let source = staged(schema.clone(), {
        let dropped = Arc::clone(&dropped);
        move || Box::pin(StagedStream::new(schema.clone(), batches.clone()).dropped(&dropped))
    });
    let mut stream = plan_of(zone_node(), source)
        .expect("the runtime executes a rate tree")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    let mut answered = Vec::new();
    let error = loop {
        match stream.next().await {
            Some(Ok(batch)) => answered.push(batch),
            Some(Err(error)) => break error,
            None => panic!(
                "the collision was answered instead of refused: {:?}",
                rows(&answered)
            ),
        }
    };
    assert!(
        error.to_string().contains(DUPLICATE_LABEL_SET),
        "refused for another reason: {error}"
    );
    // The first series' rows were handed out before the error, and the input is gone although the
    // terminated stream is still alive here.
    assert!(
        !answered.is_empty(),
        "the first series is answered before the error"
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "the terminated stream still holds its input"
    );
}

/// Two series with *different* names collapse once `rate` drops the name.
///
/// This is the collision the canonical label set exists for, at the layer that sees it:
/// `http_requests_total{host="web-1"}` and `http_errors_total{host="web-1"}` reach the window as two
/// identities and leave it as `{host="web-1"}`. [`FusedSeries`] reads one metric per scan, so this
/// collision cannot be staged through it; the runtime is driven directly, with the names on the raw
/// chunks.
#[test]
fn two_differently_named_series_collide_once_rate_drops_the_name() {
    let tree = rate_tree();
    let mut execution = NativeExecutor::new(
        NativeExecutionState::try_new(
            &tree,
            Arc::new(UnboundedMemoryPool::default()),
            &ExecutionPlanMetricsSet::new(),
            0,
        )
        .expect("the runtime executes a rate tree"),
    );
    let samples = series_samples(1.0);
    execution
        .push_raw(chunk_of(
            &raw_schema(),
            METRIC,
            metric(&[(HOST, "web-1")], Some(METRIC)),
            &times_of(&samples),
            vec![values_of(&samples)],
            true,
        ))
        .expect("the first series is a legal raw chunk");
    assert!(
        execution
            .next_chunk()
            .expect("the first series runs")
            .is_some(),
        "the first series is answered"
    );
    execution
        .push_raw(chunk_of(
            &raw_schema(),
            "http_errors_total",
            metric(&[(HOST, "web-1")], Some("http_errors_total")),
            &times_of(&samples),
            vec![values_of(&samples)],
            true,
        ))
        .expect("the second series is a legal raw chunk");
    let error = execution
        .next_chunk()
        .expect_err("the second series puts a value where the first one already holds one");
    assert!(
        error.to_string().contains(DUPLICATE_LABEL_SET),
        "refused for another reason: {error}"
    );
}

/// The grid timestamps of the statement, in order.
fn grid_timestamps() -> Vec<i64> {
    (0..GRID_LEN)
        .map(|index| GRID_START + index as i64 * GRID_STEP)
        .collect()
}

/// A waker a test can ask whether it was woken.
///
/// A poll that stops on its own budget has to wake itself, and the default waker a `poll!` uses
/// swallows that, so the tests that check it poll with this one.
#[derive(Default)]
struct CountingWake(AtomicBool);

impl futures::task::ArcWake for CountingWake {
    fn wake_by_ref(woken: &Arc<Self>) {
        woken.0.store(true, Ordering::SeqCst);
    }
}

impl CountingWake {
    fn woken(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// One series as many chunks that settle nothing: one sample per chunk, all before the grid starts.
///
/// The counter advances one second per second, so `rate` answers 1 at every grid point once the
/// series is read to its end; until a sample reaches the grid's first point, a push decides
/// nothing and settles no chunk.
fn quiet_series_frames() -> Vec<RecordBatch> {
    frames(
        &(0..QUIET_SAMPLES)
            .map(|index| {
                series_chunk(
                    "web-1",
                    &[(index * QUIET_STEP, index as f64)],
                    index == QUIET_SAMPLES - 1,
                )
            })
            .collect::<Vec<_>>(),
    )
}

/// Asserts that `rows` are `rates`' series, each over the whole grid in order.
fn assert_series_rates(rows: &[Row], rates: &[(f64, &str)]) {
    let expected = rates
        .iter()
        .flat_map(|(rate, host)| {
            grid_timestamps()
                .into_iter()
                .map(move |time| (time, *rate, *host))
        })
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), rows.len(), "{rows:?}");
    for (row, (expected_time, expected_rate, expected_host)) in rows.iter().zip(expected) {
        let (time, value, host, _) = row;
        assert_eq!(&expected_time, time);
        assert_eq!(Some(expected_host), host.as_deref());
        let value = value.expect("rate has a value at every grid point");
        assert!(
            (expected_rate - value).abs() < 1e-9,
            "{expected_host} at {expected_time}: {value} is not {expected_rate}"
        );
    }
}

fn count(metrics: &MetricsSet, name: &str) -> usize {
    match metrics.sum_by_name(name) {
        Some(MetricValue::Count { count, .. }) => count.value(),
        other => panic!("no count named {name}: {other:?}"),
    }
}

/// Prints `node` the way the logical `EXPLAIN` prints it.
struct Explain<'a>(&'a FusedSeries);

impl Display for Explain<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.0.fmt_for_explain(f)
    }
}

/// The logical node prints the query a reader wrote, not the tree it compiled to.
#[test]
fn fmt_for_explain_prints_the_promql() {
    assert_eq!(
        "FusedSeries: rate(x[5m])",
        Explain(&fused(rate_tree())).to_string()
    );
}

/// The physical node prints the tree it was built from, rendered by the tree's own display.
#[test]
fn fmt_as_prints_the_native_tree() {
    let exec =
        plan(rate_tree(), memory_exec(Vec::new())).expect("the runtime executes a rate tree");
    let text = displayable(exec.as_ref()).indent(false).to_string();

    assert!(
        text.starts_with("FusedSeriesExec: rate(x[5m])"),
        "the node line names the query first: {text}"
    );
    // A distinctive line of the compiled tree: the window's bound range, offset, anchor and grid.
    assert!(
        text.contains("root Window rate [300000ms] offset 0 anchor - on grid 300000/60000x5"),
        "the node line carries the tree it runs: {text}"
    );
    assert!(
        text.contains("input Input raw samples"),
        "the node line carries the tree's source: {text}"
    );
}

/// `rate(x[5m])` runs end to end over two raw series: values, tags and the dropped metric name.
#[tokio::test]
async fn runs_rate_over_two_series() {
    let exec = plan(rate_tree(), memory_exec(two_series_frames()))
        .expect("the runtime executes a rate tree");
    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("the node runs");
    let rows = rows(&batches);

    // A counter advancing one second per second rates at 1, and at 2 for the series that advances
    // two: the value at each grid point says the right window was read for the right series.
    let expected = [(1.0, "web-1"), (2.0, "web-2")]
        .into_iter()
        .flat_map(|(rate, host)| {
            grid_timestamps()
                .into_iter()
                .map(move |time| (time, rate, host))
        })
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), rows.len(), "{rows:?}");
    for (row, (expected_time, expected_rate, expected_host)) in rows.iter().zip(expected) {
        let (time, value, host, _) = row;
        assert_eq!(&expected_time, time);
        assert_eq!(Some(expected_host), host.as_deref());
        let value = value.expect("rate has a value at every grid point");
        assert!(
            (expected_rate - value).abs() < 1e-9,
            "{expected_host} at {expected_time}: {value} is not {expected_rate}"
        );
    }
    // `rate` drops `__name__`, so the tag column that reports it is empty for every row.
    assert!(
        rows.iter().all(|(_, _, _, name)| name.is_none()),
        "rate drops the metric name: {rows:?}"
    );
}

/// A series that spans several input chunks keeps its absolute grid positions and closes once.
#[tokio::test]
async fn a_series_spanning_chunks_keeps_absolute_positions_and_closes_once() {
    let exec = plan(rate_tree(), memory_exec(one_series_frames()))
        .expect("the runtime executes a rate tree");
    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("the node runs");
    let rows = rows(&batches);

    // Every grid position exactly once, in order: a chunk's range is never read as a row number.
    let times = rows.iter().map(|row| row.0).collect::<Vec<_>>();
    assert_eq!(grid_timestamps(), times);
    assert!(
        rows.iter().all(|(_, value, ..)| value.is_some()),
        "every grid point of this series has a value: {rows:?}"
    );

    // The source closed the series on exactly one chunk, and the window saw exactly that one
    // closing: one `is_last`, not one per input batch.
    let metrics = exec
        .metrics()
        .expect("the node reports its runtime's metrics");
    assert_eq!(1, count(&metrics, "native.root.input.completed_series"));
    assert_eq!(1, count(&metrics, "native.root.completed_series"));
}

/// A tree the runtime does not execute is refused when the execution plan is built.
///
/// The runtime folds the supported `Complete` kinds now, so the tree below asks for one it still
/// refuses on purpose: `quantile` needs the parameter and the member buffer that the other kinds
/// do not. What the test pins is the refusal happening while the plan is built, not the particular
/// kind of node that provokes it.
#[tokio::test]
async fn a_tree_the_runtime_cannot_run_is_rejected_when_the_exec_is_built() {
    let node = fused(quantile_aggregate_tree());
    let error = plan_of(node, memory_exec(Vec::new()))
        .expect_err("the runtime does not execute a quantile aggregate");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "the runtime refuses the tree it cannot run: {error}"
    );
    assert!(error.to_string().contains("Quantile"), "{error}");
}

/// A tree evaluated on another grid than the node declares is refused before it runs.
#[test]
fn a_tree_bound_to_another_grid_is_rejected() {
    let tree = rate_tree();
    let other_grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(0, GRID_STEP, GRID_LEN).unwrap()).unwrap());
    let binding = FusedSeriesBinding {
        grid: other_grid,
        ..binding(&tree)
    };
    let error = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(frame_schema()),
        output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::PlainRows,
    )
    .expect_err("the tree is not evaluated on the declared grid");
    assert!(error.to_string().contains("grid"), "{error}");
}

/// A grid allocated separately from the tree's, with the same spec, is the tree's grid.
///
/// The comparison is by spec, which is the rule [`NativeTree`]'s own validation and the kernels
/// apply, and a chunk's timestamps come from its absolute grid positions rather than from the
/// allocation, so nothing downstream needs the `Arc` to be shared. This pairs with
/// [`a_tree_bound_to_another_grid_is_rejected`]: a different spec is still refused.
#[test]
fn a_separately_allocated_grid_with_an_equal_spec_is_accepted() {
    let tree = rate_tree();
    let equal = Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN).unwrap()).unwrap(),
    );
    assert!(
        !Arc::ptr_eq(&equal, &binding(&tree).grid),
        "the two grids are separate allocations"
    );
    let binding = FusedSeriesBinding {
        grid: equal,
        ..binding(&tree)
    };
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(frame_schema()),
        output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::PlainRows,
    )
    .expect("an equal grid spec is the same grid");
}

/// An input that ends without a series still runs the tree, and yields no rows.
#[tokio::test]
async fn an_empty_input_runs_the_tree_and_yields_no_rows() {
    let exec =
        plan(rate_tree(), memory_exec(Vec::new())).expect("the runtime executes a rate tree");
    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("a tree with no series in it still closes");
    assert!(rows(&batches).is_empty(), "{:?}", rows(&batches));
    // The run closed on no series, so nothing was completed and nothing was decided.
    let metrics = exec
        .metrics()
        .expect("the node reports its runtime's metrics");
    assert_eq!(0, count(&metrics, "native.root.completed_series"));
    assert_eq!(0, count(&metrics, "native.root.input.completed_series"));
}

/// An upstream error after some rows is reported, and the terminated stream releases that input.
#[tokio::test]
async fn an_upstream_error_after_rows_is_reported() {
    let dropped = Arc::new(AtomicBool::new(false));
    let schema = frame_schema();
    let batches = one_series_frames();
    let source = staged(schema.clone(), {
        let dropped = Arc::clone(&dropped);
        move || {
            Box::pin(
                StagedStream::new(schema.clone(), batches.clone())
                    .error("the source failed")
                    .dropped(&dropped),
            )
        }
    });
    let mut stream = plan(rate_tree(), source)
        .expect("the runtime executes a rate tree")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    // The error is reported rather than swallowed by the rows the input already handed over.
    let mut answered = Vec::new();
    let error = loop {
        match stream.next().await {
            Some(Ok(batch)) => answered.push(batch),
            Some(Err(error)) => break error,
            None => panic!("the source failure was swallowed: {:?}", rows(&answered)),
        }
    };
    assert!(
        error.to_string().contains("the source failed"),
        "reported another error: {error}"
    );
    // Still alive here: the release is the stream's, not the drop of a temporary.
    assert!(
        dropped.load(Ordering::SeqCst),
        "the terminated stream still holds its input"
    );
}

/// A pending poll loses nothing when the next-item future is dropped and the stream polled again.
#[tokio::test]
async fn a_pending_boundary_loses_no_rows() {
    let expected = rows(
        &collect(
            plan(rate_tree(), memory_exec(one_series_frames()))
                .expect("the runtime executes a rate tree"),
            SessionContext::default().task_ctx(),
        )
        .await
        .expect("the node runs"),
    );
    assert!(!expected.is_empty(), "the fixture decides some grid points");

    let schema = frame_schema();
    let batches = one_series_frames();
    let source = staged(schema.clone(), move || {
        Box::pin(
            StagedStream::new(schema.clone(), batches.clone())
                // One batch is in the decoder's hands when the stream reports pending.
                .pending_after(1),
        )
    });
    let mut stream = plan(rate_tree(), source)
        .expect("the runtime executes a rate tree")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    // The future is dropped at the end of this block, as a consumer that gives up waiting does.
    {
        let next = stream.next();
        futures::pin_mut!(next);
        assert!(
            futures::poll!(next.as_mut()).is_pending(),
            "the input's pending poll reaches the consumer"
        );
    }
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .expect("the stream resumes where the pending poll left it");
    assert_eq!(expected, rows(&batches));
}

/// A frame whose labels are encoded in its key is refused when the plan is built.
///
/// The tags this node declares are read off each chunk's labels, and a key-mode frame carries none:
/// decoding it with a binding that promises complete labels would produce chunks that name no
/// series. The frame's own declared layout says which of the two it is, so the mismatch is caught
/// before the first batch, not when the missing labels reach the output.
#[test]
fn a_frame_whose_labels_are_encoded_is_rejected_when_the_plan_is_built() {
    let keyed = keyed_frame_schema_of(&raw_schema());
    let error = plan(rate_tree(), memory_exec_with(keyed, Vec::new()))
        .expect_err("the frames carry no labels to read tags from");
    assert!(
        error.to_string().contains("raw frame"),
        "refused for another reason: {error}"
    );
}

/// A frame that carries an evaluation grid is refused when the plan is built.
///
/// A frame holds raw series: its rows are samples, not grid positions, and the grid every output
/// row is addressed on is the tree's own. A frame that declares a grid is one whose rows this node
/// would read as positions on it, so the shared decoder refuses it rather than reading samples out
/// of it. The source below is one partition, which is the shape the node accepts, so what refuses
/// the plan here is the frame's own declaration and not the node's partition count: the refusal has
/// to name the frame.
#[test]
fn a_frame_with_an_evaluation_grid_is_rejected_when_the_plan_is_built() {
    let gridded_schema = Arc::new(SeriesSchema {
        fields: raw_schema().fields.clone(),
        eval_grid: Some(grid()),
    });
    let gridded = series_frame_schema(&gridded_schema, FrameLabelMode::Explicit, None)
        .expect("a grid is a frame");
    // One empty partition: every check but the frame's passes.
    let source = memory_exec_with(gridded, Vec::new());
    assert_eq!(
        1,
        source.properties().output_partitioning().partition_count(),
        "the node only looks at the frame when its input has a partition"
    );
    let error = plan(rate_tree(), source).expect_err("the frame declares a grid");
    let message = error_chain(&error);
    assert!(
        message.contains("expected a raw frame, found an evaluated one"),
        "refused for another reason: {message}"
    );
    assert!(
        !message.contains("no partitions"),
        "the partition count is not what refuses this plan: {message}"
    );
}

/// A frame of another series schema is refused when the plan is built.
///
/// The tree's input node was compiled against one schema, and a frame for a different one names
/// fields the tree does not read and omits ones it does: decoding it would hand the tree a column
/// it never bound. Nothing about the two schemas says which one was meant, so the frame is refused
/// instead of half-read.
#[test]
fn a_frame_of_another_series_schema_is_rejected_when_the_plan_is_built() {
    let other = frame_schema_of(&two_field_schema());
    let error = plan(rate_tree(), memory_exec_with(other, Vec::new()))
        .expect_err("the tree's input reads one field");
    assert!(
        error
            .to_string()
            .contains("frame fields do not match the expected series schema"),
        "refused for another reason: {error}"
    );
}

/// An input with no partitions is refused when the plan is built, and no other count is.
///
/// The node runs one evaluator per input partition and adds none of its own, so an input with no
/// partition is one that could answer nothing: the plan is refused rather than built as a node that
/// would report an empty result for data it never read. Any non-zero count is the input's to
/// choose: the node takes it and answers each partition's own series.
#[test]
fn an_input_with_no_partitions_is_rejected_when_the_plan_is_built() {
    let schema = frame_schema();
    let empty = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[], Arc::clone(&schema), None)
            .expect("no partitions is a legal source"),
    )));
    let error = plan(rate_tree(), empty).expect_err("there is no partition to run");
    assert!(
        error.to_string().contains("input has no partitions"),
        "refused for another reason: {error}"
    );
    // The only count refused is zero: two partitions build and answer, which is what the
    // multi-partition tests below run.
    let two = plan(rate_tree(), memory_exec_partitioned(vec![vec![], vec![]]))
        .expect("a two-partition input is not refused");
    assert_eq!(
        2,
        two.properties().output_partitioning().partition_count(),
        "the node takes the input's count"
    );
}

/// Two windows stacked are refused over more than one partition, because the inner one's fragments
/// need not have been merged.
///
/// The counterexample is legal PromQL once nesting is expressible: the inner `last_over_time` runs
/// over two raw series that are one canonical label set, `{host="h", zone=""}` holding `(1s, 1)` and
/// `{host="h"}` holding `(2s, 2)`, and the outer `rate` needs both in one window. In a single
/// partition the outer window merges the two fragments and answers `(2s, 1)`; in two partitions each
/// executor is handed one fragment and has one sample to rate, so the outer window answers nothing.
/// That is an empty result rather than an error, which is why no check further down can see it.
///
/// `last_over_time` keeps `__name__`, so a check that looked only for a dropped metric name accepts
/// this tree. What makes it unsafe is that a window's output can hold two series under one label set
/// whenever it did not merge its input, which a bare tree cannot say.
///
/// One partition stays accepted: there is nothing to split there, which is what makes the refusal
/// about the split rather than about the shape of the tree.
#[test]
fn a_nested_window_over_two_partitions_is_refused() {
    let error = plan(
        nested_window_tree(),
        memory_exec_partitioned(vec![vec![], vec![]]),
    )
    .expect_err("each of two partitions would merge only the fragment it was given");
    let message = error.to_string();
    assert!(
        message.contains("`root.input`"),
        "the refusal must name the inner window: {message}"
    );
    assert!(
        message.contains("`root`"),
        "the refusal must name the window above it: {message}"
    );
    assert!(
        message.contains("2 partitions"),
        "the refusal must name the partition count: {message}"
    );

    // The same tree over one partition is accepted, because one execution sees every fragment and
    // merges them into one series before the window above it evaluates.
    plan(nested_window_tree(), memory_exec(Vec::new()))
        .expect("a single partition cannot split a logical series");
}

/// An instant selection can leave distinct raw labels on one canonical label set for a window.
#[test]
fn an_instant_then_window_over_two_partitions_is_refused() {
    let error = plan(
        instant_window_tree(),
        memory_exec_partitioned(vec![vec![], vec![]]),
    )
    .expect_err("each partition could hand the window only one fragment");
    let message = error.to_string();
    assert!(
        message.contains("`root.input`"),
        "the refusal must name the instant selection: {message}"
    );
    assert!(
        message.contains("`root`"),
        "the refusal must name the consuming window: {message}"
    );
    assert!(
        message.contains("2 partitions"),
        "the refusal must name the partition count: {message}"
    );
}

/// A single partition cannot split a logical series, even when an instant feeds a window.
#[test]
fn an_instant_then_window_over_one_partition_is_accepted() {
    plan(instant_window_tree(), memory_exec(Vec::new())).expect("one partition sees all fragments");
}

/// A window directly over raw input remains valid across partitions without a preceding rewriter.
#[test]
fn a_window_over_raw_input_on_two_partitions_is_accepted() {
    plan(rate_tree(), memory_exec_partitioned(vec![vec![], vec![]]))
        .expect("ordinary input-to-window trees do not split a logical series");
}

/// One poll reads at most [`POLL_BUDGET`] chunks of input, and stopping there loses nothing.
///
/// `rate(x[5m])` decides nothing until a sample reaches the grid's first point, so a series can
/// arrive as many chunks that settle no batch at all: a poll that kept reading them would hold the
/// task's thread until the series ended, which is what stops other tasks on that thread and delays
/// the cancellation a consumer asked for. The first series here is [`QUIET_SAMPLES`] such chunks;
/// the second one covers the grid, so a poll that resumes properly still answers both.
#[tokio::test]
async fn one_poll_reads_at_most_the_budget_and_loses_nothing() {
    let mut batches = quiet_series_frames();
    batches.push(frame(&series_chunk("web-2", &series_samples(2.0), true)));
    let total = batches.len();
    assert!(
        total > POLL_BUDGET,
        "the fixture has to outlast one poll: {total} batches"
    );

    let pulled = Arc::new(AtomicUsize::new(0));
    let woken = Arc::new(CountingWake::default());
    let schema = frame_schema();
    let source = staged(schema.clone(), {
        let pulled = Arc::clone(&pulled);
        move || Box::pin(StagedStream::new(schema.clone(), batches.clone()).counted(&pulled))
    });
    let mut stream = plan(rate_tree(), source)
        .expect("the runtime executes a rate tree")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    // The first poll: the quiet series settles nothing, so the budget is what ends this poll.
    let waker = futures::task::waker(Arc::clone(&woken));
    let mut context = Context::from_waker(&waker);
    assert!(
        stream.as_mut().poll_next(&mut context).is_pending(),
        "chunks that settle nothing cannot answer a poll"
    );
    let read = pulled.load(Ordering::SeqCst);
    assert!(read > 0, "the poll did read its input");
    // One step polls the decoder once, and the decoder reads at most one frame per poll: every
    // frame here carries one chunk. The bound is therefore one frame per step, and the budget is
    // what it is measured in.
    assert!(
        read <= POLL_BUDGET + 1,
        "one poll read {read} of {total} batches, past its budget of {POLL_BUDGET} steps"
    );
    assert!(
        read < total,
        "one poll read the whole input ({read} batches): the budget did not stop it"
    );
    assert!(
        woken.woken(),
        "a poll that stopped on its budget has to wake itself"
    );

    // Nothing the budget stopped is lost: the stream resumes where the poll left it.
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .expect("the stream resumes where the bounded poll left it");
    assert_series_rates(&rows(&batches), &[(1.0, "web-1"), (2.0, "web-2")]);
}

/// A batch that is ready is handed over before any batch behind it is read.
///
/// Each series below is one chunk, and a chunk that carries `is_last` closes its series, so the
/// first answer needs only the first chunk. A poll that read on would answer the same batch only
/// after the whole input, which is exactly what the budget above exists to prevent.
#[tokio::test]
async fn a_ready_batch_is_returned_before_later_input_is_read() {
    let batches = (0..20)
        .map(|index| {
            frame(&series_chunk(
                &format!("web-{index}"),
                &series_samples(1.0),
                true,
            ))
        })
        .collect::<Vec<_>>();
    let total = batches.len();
    let pulled = Arc::new(AtomicUsize::new(0));
    let schema = frame_schema();
    let source = staged(schema.clone(), {
        let pulled = Arc::clone(&pulled);
        move || Box::pin(StagedStream::new(schema.clone(), batches.clone()).counted(&pulled))
    });
    let mut stream = plan(rate_tree(), source)
        .expect("the runtime executes a rate tree")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    let first = stream
        .next()
        .await
        .expect("the first series is answered")
        .expect("the first series is answered without an error");
    let answered = rows(&[first]);
    assert_series_rates(&answered, &[(1.0, "web-0")]);
    // One batch: the series' own chunk closes it, so nothing behind it has to be read.
    assert_eq!(
        1,
        pulled.load(Ordering::SeqCst),
        "the answer waited for input behind the series it reports"
    );
    assert!(total > 1, "the fixture has to outlast the first answer");
}

/// A chunk whose rows form several runs is converted row by row from the rows it carries.
///
/// A sparse result is one chunk holding two runs with a hole between them
/// ([`EvaluatedChunk::range`] answers `None` for it). Reading the hole as rows would invent grid
/// points this series holds no value at, and reading positions as compact rows would shift every
/// row after the hole onto the wrong timestamp: the batch has to hold exactly the carried rows, at
/// their absolute grid times, with nulls nowhere else.
#[test]
fn a_chunk_of_several_runs_converts_its_own_rows_only() {
    let grid = grid();
    let axis = TimeAxis::from_grid_spans(
        Arc::clone(&grid),
        // Grid positions 0..2 and 4..5: position 2 and 3 hold no row at all.
        vec![GridSpan::new(0, 2), GridSpan::new(4, 1)],
    )
    .expect("the spans are increasing and inside the grid");
    let chunk = EvaluatedChunk::try_new(
        Series {
            metric: SeriesLabels::try_new(Some(Arc::from(METRIC)), Vec::new())
                .expect("one metric name is a legal identity"),
            time: axis,
            fields: SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(
                // One value per carried row, so the second run's value sits at grid position 4.
                vec![7.0, 8.0, 11.0],
            ))])
            .expect("one field is a legal field list"),
        },
        true,
    )
    .expect("a chunk on a grid");

    // The columns of `output_schema`, in its order: the field, the two tags, then the time.
    let batch = series_to_record_batch(
        &[
            OutputColumn::Field(0),
            OutputColumn::Tag(0),
            OutputColumn::Tag(1),
            OutputColumn::Time,
        ],
        &[HOST.to_string(), NAME.to_string()],
        &output_schema(),
        &chunk,
    )
    .expect("the chunk converts");

    let stamps = batch
        .column_by_name(TIME)
        .expect("the batch has its time column")
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("the time column is a millisecond timestamp");
    let values = batch
        .column_by_name(VALUE)
        .expect("the batch has its field column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("the field column is Float64");
    assert_eq!(3, batch.num_rows(), "only the carried rows are rows");
    let grid_stamps = grid.timestamps().expect("the grid timestamps build");
    assert_eq!(
        vec![grid_stamps[0], grid_stamps[1], grid_stamps[4]],
        stamps.values()[..3].to_vec(),
        "the carried rows keep their absolute grid positions"
    );
    assert_eq!(vec![7.0, 8.0, 11.0], values.values()[..3].to_vec());
}

/// Every message in `error`'s chain, so an error a layer above wrapped is still read.
fn error_chain(error: &DataFusionError) -> String {
    let mut message = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

/// Two partitions, one series each, and both series leave the window on the same label set.
///
/// `rate` drops `__name__`, so `http_requests_total{host="web-1"}` reads as `{host="web-1"}` in
/// the output. The key column keeps each of the two whole inside its own partition, so a check that
/// lived inside one execution would see one series each and report nothing: only state shared by
/// the partitions sees that the output vector holds the label set twice.
#[tokio::test]
async fn a_duplicate_label_set_across_partitions_is_refused() {
    let series = frame(&series_chunk("web-1", &series_samples(1.0), true));
    let exec = plan(
        rate_tree(),
        memory_exec_partitioned(vec![vec![series.clone()], vec![series]]),
    )
    .expect("the runtime executes a rate tree");
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "this test needs one series in each of two partitions"
    );

    let error = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect_err("the second partition lands on the label set the first one claimed");
    let message = error_chain(&error);
    assert!(
        message.contains("vector cannot contain metrics with the same labelset"),
        "refused for another reason: {message}"
    );
    assert!(
        message.contains("host=\"web-1\""),
        "the label set must be readable: {message}"
    );
}

/// The same label set on points that do not overlap is legal, and every point survives.
///
/// A rate needs two samples in its window, so the first series only has a value at the first grid
/// point and the second only at the last: the two share a final label set but no evaluation
/// timestamp. Prometheus merges such series into one; this node emits both batches and leaves the
/// merge to matrix assembly, so the check has to allow what it would otherwise refuse.
#[tokio::test]
async fn a_duplicate_label_set_on_disjoint_grid_points_is_emitted() {
    let first = vec![frame(&series_chunk(
        "web-1",
        &[(60_000, 60.0), (GRID_START, GRID_START as f64 / 1_000.0)],
        true,
    ))];
    let last = GRID_START + (GRID_LEN as i64 - 1) * GRID_STEP;
    let second = vec![frame(&series_chunk(
        "web-1",
        &[
            (GRID_START, GRID_START as f64 / 1_000.0),
            (last, last as f64 / 1_000.0),
        ],
        true,
    ))];
    let exec = plan(rate_tree(), memory_exec_partitioned(vec![first, second]))
        .expect("the runtime executes a rate tree");

    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("disjoint grid points are legal");
    assert_eq!(2, batches.len(), "each partition emits its own series");
    let mut rows = rows(&batches);
    rows.sort_by_key(|row| row.0);
    assert_eq!(
        vec![GRID_START, last],
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        "both series must survive with the one point each holds"
    );
    for ((_, value, host, _), rate) in rows.iter().zip([1.0, 1.0]) {
        assert_eq!(Some("web-1"), host.as_deref());
        let value = value.expect("the point each series holds has a value");
        assert!((rate - value).abs() < 1e-9, "{value} is not {rate}");
    }
}

/// What a label set holds stays charged after the stream that first claimed it ends, and what a
/// later partition adds to it is charged as well.
///
/// The map belongs to the plan: it outlives every partition and is released with the plan. Charging
/// each stream for what it inserted returned the bytes of a label set when *that* stream ended,
/// while the entry the next partition is checked against stayed in the map, so everything the map
/// held after the first partition was invisible to the pool. What is charged here is therefore read
/// between the partitions: it has to survive the first stream, and the runs the second partition
/// merges into the entry have to cost what they hold.
#[tokio::test]
async fn a_label_set_stays_charged_after_its_first_stream_ends() {
    // The first series holds a value only at the first grid point and the second only at the last,
    // so the two share a final label set without sharing an evaluation timestamp: the second
    // partition is legal and has to be answered, and it has to find the first one's entry there.
    let first = vec![frame(&series_chunk(
        "web-0",
        &[(60_000, 60.0), (GRID_START, GRID_START as f64 / 1_000.0)],
        true,
    ))];
    let last = GRID_START + (GRID_LEN as i64 - 1) * GRID_STEP;
    let second = vec![frame(&series_chunk(
        "web-0",
        &[
            (GRID_START, GRID_START as f64 / 1_000.0),
            (last, last as f64 / 1_000.0),
        ],
        true,
    ))];
    let exec = plan(rate_tree(), memory_exec_partitioned(vec![first, second]))
        .expect("the runtime executes a rate tree");
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "this test needs one series in each of two partitions"
    );
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);

    // The first partition claims the label set, answers it and ends.
    let mut stream = exec
        .execute(0, Arc::clone(&context))
        .expect("the node executes");
    let mut answered = Vec::new();
    while let Some(batch) = stream.next().await {
        answered.push(batch.expect("the first partition runs"));
    }
    assert_eq!(1, answered.len(), "one series over one grid point");
    drop(stream);
    let charged = pool.reserved();
    assert!(
        charged > 0,
        "the first partition's label set is still in the plan's map, but nothing is charged for it"
    );

    // The second partition lands on that same label set on points the first one does not hold: it
    // is answered, and the run it merges into the entry is what its charge grows by.
    let mut stream = exec
        .execute(1, Arc::clone(&context))
        .expect("the node executes");
    let mut answered = Vec::new();
    while let Some(batch) = stream.next().await {
        answered.push(batch.expect("the second partition runs"));
    }
    assert_eq!(1, answered.len(), "disjoint points are legal");
    drop(stream);
    assert!(
        pool.reserved() > charged,
        "the second partition's run grew the entry, but nothing was charged for it: {} is not \
         above {charged}",
        pool.reserved()
    );

    // The entries live as long as the plan, so dropping it is what gives their bytes back.
    drop(exec);
    assert_eq!(
        0,
        pool.reserved(),
        "the plan's drop releases what its label sets held"
    );
}

/// Each partition runs its own series and answers it, with no label set claimed twice.
#[tokio::test]
async fn every_partition_evaluates_its_own_series() {
    let partitions = [1.0, 2.0, 3.0]
        .into_iter()
        .enumerate()
        .map(|(index, scale)| {
            vec![frame(&series_chunk(
                &format!("web-{index}"),
                &series_samples(scale),
                true,
            ))]
        })
        .collect::<Vec<_>>();
    let exec = plan(rate_tree(), memory_exec_partitioned(partitions))
        .expect("the runtime executes a rate tree");
    assert_eq!(
        3,
        exec.properties().output_partitioning().partition_count(),
        "the node's partition count is its input's"
    );

    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("each partition runs its own tree");
    assert_eq!(3, batches.len(), "one batch per partition");
    let rows = rows(&batches);
    assert_eq!(
        3 * GRID_LEN as usize,
        rows.len(),
        "every partition reports its own series over the whole grid"
    );
    // Batches arrive in whatever order the partitions settle, so the series are read apart.
    for (index, scale) in [1.0, 2.0, 3.0].into_iter().enumerate() {
        let host = format!("web-{index}");
        let mut series = rows
            .iter()
            .filter(|row| row.2.as_deref() == Some(host.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        series.sort_by_key(|row| row.0);
        assert_eq!(GRID_LEN as usize, series.len(), "{host}: {series:?}");
        assert_series_rates(&series, &[(scale, host.as_str())]);
    }
}

/// The optimizer inserts no exchange between a two-partition scan and this node.
///
/// This is the claim the whole arrangement rests on, so it is checked against the real rule rather
/// than against the flags that are supposed to satisfy it: a plan that reports the right
/// requirements can still be repartitioned, and that is exactly how the previous `KeyPartitioned`
/// requirement failed - a hash requirement is classed as necessary for any child with more than one
/// partition, so a hash exchange was inserted even though the child already satisfied it.
///
/// The input here is a two-partition source, and `target_partitions` is raised above it because
/// that is when an optimizer is most willing to repartition. Both are the conditions under which the
/// old requirement broke, so a plan that survives them is one where the guarantee the scan makes
/// arrives at this node intact.
#[test]
fn the_optimizer_inserts_no_exchange_between_the_input_and_this_node() {
    let exec = plan(
        rate_tree(),
        memory_exec_partitioned(vec![
            vec![frame(&series_chunk("web-0", &series_samples(1.0), true))],
            vec![frame(&series_chunk("web-1", &series_samples(2.0), true))],
        ]),
    )
    .expect("the runtime executes a rate tree");
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "this test needs a child with more than one partition"
    );

    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 8;
    let optimized = EnsureRequirements::new()
        .optimize(Arc::clone(&exec), &config)
        .expect("the rule accepts the plan");

    let exchanges = exchanges_between(&optimized, &exec);
    assert!(
        exchanges.is_empty(),
        "the optimizer inserted {exchanges:?} between the input and the node, and an exchange \
         reorders a series' chunks: the node would close a series it has not finished"
    );
    // No exchange is not the same as no change: a rule that raised the count some other way would
    // still split a series. The count is the input's, which is what the node reports and what it
    // runs one evaluator per.
    assert_eq!(
        2,
        optimized
            .properties()
            .output_partitioning()
            .partition_count(),
        "the optimizer kept no exchange but changed how many partitions the node runs"
    );
    assert_eq!(
        2,
        optimized.children()[0]
            .properties()
            .output_partitioning()
            .partition_count(),
        "the input's own partition count changed under the node"
    );
}

/// The `RepartitionExec`s in `plan` that sit above `stop` and below no other `RepartitionExec`.
///
/// Named by the kind the rule chose, so a failure says whether it was a hash or a round-robin
/// exchange: the two are inserted for different reasons and only the hash one is the trap.
fn exchanges_between(plan: &Arc<dyn ExecutionPlan>, stop: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if Arc::ptr_eq(&node, stop) {
            continue;
        }
        if let Some(exchange) =
            node.downcast_ref::<datafusion::physical_plan::repartition::RepartitionExec>()
        {
            found.push(format!(
                "{}({:?})",
                exchange.name(),
                exchange.partitioning()
            ));
        }
        stack.extend(node.children().into_iter().cloned());
    }
    found
}

/// The node asks the optimizer for nothing, so no exchange can be inserted under it.
///
/// A window holds the state of the series it is reading and closes it on the chunk carrying
/// `is_last`, so it can answer a series only if the whole series arrives in the partition it reads.
/// What makes that true is the input: a raw series frame comes from `SeriesScan`, whose contract is
/// that the same series (primary key) always reaches the same partition
/// (`src/mito2/src/read/series_scan.rs`), and the frame decoder keeps each series' chunks contiguous
/// within that partition. The node therefore needs the input's own layout kept, not a particular
/// layout of its own.
///
/// The trap is the requirement writing that as `KeyPartitioned`, which reads like the honest
/// description of the guarantee. A `Hash|KeyPartitioned` requirement is classed as a hash
/// requirement, and a hash requirement forces a hash exchange whenever the child has more than one
/// partition - even where the child already satisfies it, and even when the partition counts match.
/// A hash exchange does not preserve order, so it would interleave one series' chunks across
/// partitions and the decoder would close a series it has not finished. Asking for
/// [`Distribution::UnspecifiedDistribution`] is what leaves the child alone, and declining extra
/// partitioning is the second half of the same thing: no round-robin is added to raise parallelism
/// either. The node is not pinned to one partition — it runs one tree per input partition and keeps
/// the input's count.
#[test]
fn the_input_is_left_alone_so_no_exchange_can_be_inserted() {
    let exec = plan(rate_tree(), memory_exec_partitioned(vec![vec![], vec![]]))
        .expect("the runtime executes a rate tree");
    let distributions = exec.input_distribution_requirements().into_per_child();
    // `Distribution` has no equality of its own, so the exact requirement is matched structurally:
    // one child, and what it asks for is that the child's layout is left alone.
    assert!(
        matches!(
            distributions.as_slice(),
            [Distribution::UnspecifiedDistribution]
        ),
        "anything but leaving the child alone makes a hash exchange necessary: {distributions:?}"
    );
    assert_eq!(
        vec![false],
        exec.benefits_from_input_partitioning(),
        "raising the partition count would add a round-robin exchange"
    );
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "the node's own count is its input's"
    );
}

/// A partition index the input does not have is refused rather than answered.
///
/// The node runs one evaluator per input partition and adds none of its own, so it forwards the
/// index to its input: the count it declares is the input's, and an index past that is the input's
/// refusal, not a series read from another partition.
#[tokio::test]
async fn a_partition_past_the_inputs_count_is_refused() {
    let schema = frame_schema();
    let source = staged(schema.clone(), move || {
        Box::pin(StagedStream::new(schema.clone(), Vec::new()))
    });
    let exec = plan(rate_tree(), source).expect("the runtime executes a rate tree");
    assert_eq!(
        1,
        exec.properties().output_partitioning().partition_count(),
        "the node's partition count is its input's"
    );
    exec.execute(0, SessionContext::default().task_ctx())
        .expect("the one partition the input has");
    let error = match exec.execute(1, SessionContext::default().task_ctx()) {
        Ok(_) => panic!("a partition the input does not have is answered"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("partition"),
        "refused for another reason: {error}"
    );
}

/// The `rate(x[5m])` tree over two fields, with both fields and the tags in the output.
fn two_field_node() -> FusedSeries {
    let tree = rate_tree_of(two_field_schema());
    let binding = binding_of(
        &tree,
        two_field_schema(),
        vec![HOST.to_string(), NAME.to_string()],
    );
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(frame_schema_of(&two_field_schema())),
        two_field_output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::PlainRows,
    )
    .expect("the binding matches the tree")
}

/// The output of [`two_field_node`]: both fields, the tags and the grid times.
fn two_field_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new(OTHER, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(NAME, DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

/// One chunk of `{__name__=METRIC, host=<host>, __type__=<kind>}` whose `field` holds `rows` and
/// whose other field is missing everywhere.
fn one_field_chunk(field: usize, host: &str, kind: &str, last: bool) -> RawSeriesChunk {
    let rows = series_samples(1.0);
    let (value, other) = if field == 0 {
        (values_of(&rows), no_values(&rows))
    } else {
        (no_values(&rows), values_of(&rows))
    };
    chunk_of(
        &two_field_schema(),
        &format!("{host}-{kind}"),
        metric(&[(HOST, host), (TYPE, kind)], Some(METRIC)),
        &times_of(&rows),
        vec![value, other],
        last,
    )
}

/// Two series that close on one label set may still fill it, one field each.
///
/// `rate` drops `__name__` and `__type__`, so the two series below leave the window as
/// `{host="web-1"}` and read as one label set. They hold values in *different* fields, which is the
/// pair [`crate::grid_series::merge_same_labelset`] merges: one value of one field at each position,
/// never two. Occupancy is therefore per field, and refusing this pair would refuse a series
/// Prometheus answers.
#[tokio::test]
async fn a_duplicate_label_set_of_complementary_fields_is_emitted() {
    let exec = plan_of(
        two_field_node(),
        memory_exec_partitioned_with(
            frame_schema_of(&two_field_schema()),
            vec![
                frames_of(
                    &two_field_schema(),
                    &[one_field_chunk(0, "web-1", "counter", true)],
                ),
                frames_of(
                    &two_field_schema(),
                    &[one_field_chunk(1, "web-1", "gauge", true)],
                ),
            ],
        ),
    )
    .expect("the runtime executes a rate tree");
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "this test needs one series in each of two partitions"
    );

    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("complementary fields are legal");
    assert_eq!(2, batches.len(), "each partition emits its own series");
    // Each batch answers one field of the shared label set and leaves the other missing: the two
    // together are the merged series, with no value of one field holding a position twice.
    let answered = batches
        .iter()
        .map(|batch| {
            let host = batch
                .column_by_name(HOST)
                .and_then(|host| host.as_any().downcast_ref::<StringArray>())
                .expect("every batch has its tag column");
            assert!(
                (0..host.len()).all(|row| host.value(row) == "web-1"),
                "both batches answer the one label set"
            );
            let present = [VALUE, OTHER]
                .into_iter()
                .filter(|field| {
                    let column = batch
                        .column_by_name(field)
                        .unwrap_or_else(|| panic!("every batch has a {field} column"))
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("the field column is Float64");
                    column.null_count() < column.len()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                1,
                present.len(),
                "each series fills exactly one field: {present:?}"
            );
            present[0]
        })
        .collect::<Vec<_>>();
    let mut answered = answered;
    answered.sort_unstable();
    assert_eq!(vec![OTHER, VALUE], answered, "the two fields are filled");
}

/// Two series that close on one label set may not hold the same field at the same position.
///
/// This is the pair no merge resolves: at one grid position one field of the label set would carry
/// two values. It is refused even though the two series name different `__type__` labels, because
/// the window drops that label and what reaches the output is one label set twice.
#[tokio::test]
async fn a_duplicate_label_set_of_one_field_is_refused() {
    let exec = plan_of(
        two_field_node(),
        memory_exec_partitioned_with(
            frame_schema_of(&two_field_schema()),
            vec![
                frames_of(
                    &two_field_schema(),
                    &[one_field_chunk(0, "web-1", "counter", true)],
                ),
                frames_of(
                    &two_field_schema(),
                    &[one_field_chunk(0, "web-1", "gauge", true)],
                ),
            ],
        ),
    )
    .expect("the runtime executes a rate tree");

    let error = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect_err("both series hold a value of one field at one position");
    let message = error_chain(&error);
    assert!(
        message.contains("vector cannot contain metrics with the same labelset"),
        "refused for another reason: {message}"
    );
    assert!(
        message.contains("host=\"web-1\""),
        "the label set must be readable: {message}"
    );
}

/// Two partitions whose label sets differ only in an empty value hold one label set.
///
/// `{host="web-1", zone=""}` and `{host="web-1"}` are one series to Prometheus: an empty label value
/// is no label. An instant selection rewrites nothing, so both label sets reach this node's own
/// state as their frames wrote them, and a state that compared the tags as they arrived would file
/// them apart. Neither partition sees the other's series, so nothing else in the plan can refuse
/// the pair: this node's state is what compares them, and what it has to compare is the label set
/// Prometheus compares.
#[tokio::test]
async fn partitions_whose_label_sets_differ_only_in_an_empty_value_are_refused() {
    let [empty, absent] = <[_; 2]>::try_from(zone_chunks()).expect("the fixture has two chunks");
    let exec = plan_of(
        instant_node(),
        memory_exec_partitioned(vec![frames(&[empty]), frames(&[absent])]),
    )
    .expect("the runtime executes an instant selection");
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "this test needs one series in each of two partitions"
    );

    let error = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect_err("both partitions hold one value of one field at the same position");
    let message = error_chain(&error);
    assert!(
        message.contains("vector cannot contain metrics with the same labelset"),
        "refused for another reason: {message}"
    );
    assert!(
        message.contains("host=\"web-1\""),
        "the label set must be readable: {message}"
    );
}

/// A chunk holding no value records nothing, so a query that answers nothing holds nothing.
///
/// A selection over a series that staleness or the range left empty still closes that series, and
/// hands over a closing chunk with no rows - [`crate::native::execute::no_row_chunk`] is where it is
/// built. Every chunk reaches [`LabelSets::claim`], so an implementation that wrote an entry per
/// chunk would record one per empty series and charge the plan's reservation for it, for a query
/// whose answer is empty: enough of them would exhaust a finite pool without a single row produced.
///
/// The entries are checked rather than the charge, because a charge of zero is what a mistake here
/// would still show if the label bytes happened to be free: what must not grow is the table.
#[test]
fn a_chunk_holding_no_value_is_claimed_without_recording_it() {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut labels = LabelSets::default();
    labels.attach(&pool);
    let axis = TimeAxis::from_grid_spans(Arc::clone(&grid()), vec![GridSpan::new(0, 0)])
        .expect("an empty run over the grid");
    let empty_series = |index: usize| Series {
        metric: metric(&[(HOST, &format!("web-{index}"))], Some(METRIC)),
        time: axis.clone(),
        // Five positions were never filled, so no field occupies one.
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![f64::NAN; 0]))),
    };

    for index in 0..64 {
        labels
            .claim(&empty_series(index))
            .expect("an empty chunk cannot collide with anything");
    }

    assert_eq!(
        0,
        labels.seen.len(),
        "64 empty series were recorded, and the table outlives the query: {:?}",
        labels.seen.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        0,
        pool.reserved(),
        "nothing is held, so nothing may be charged"
    );
}

/// An empty chunk does not stop a later chunk of the same label set from being recorded.
///
/// Skipping the empty chunk is only safe while it also records nothing a later one needs: a series
/// that answers nothing holds no position, so a second series under one label set has to be free to
/// claim the positions it really holds.
#[test]
fn a_later_chunk_still_claims_the_label_set_an_empty_one_skipped() {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut labels = LabelSets::default();
    labels.attach(&pool);
    let empty_axis = TimeAxis::from_grid_spans(Arc::clone(&grid()), vec![GridSpan::new(0, 0)])
        .expect("an empty run over the grid");
    let full_axis = TimeAxis::from_grid_spans(Arc::clone(&grid()), vec![GridSpan::new(0, 5)])
        .expect("one run over the whole grid");

    labels
        .claim(&Series {
            metric: metric(&[(HOST, "h")], Some(METRIC)),
            time: empty_axis,
            fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![f64::NAN; 0]))),
        })
        .expect("an empty chunk cannot collide with anything");
    labels
        .claim(&Series {
            metric: metric(&[(HOST, "h")], Some(METRIC)),
            time: full_axis,
            fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0; 5]))),
        })
        .expect("the first chunk to hold a value claims the positions it holds");

    assert_eq!(1, labels.seen.len(), "the positions are recorded once");
    assert!(pool.reserved() > 0, "the recorded positions are charged");
}

/// The claim compares the label set Prometheus compares, so an empty value is no label.
///
/// Driven directly at the layer that keeps the state: a window and a map both drop an empty-valued
/// label before a chunk reaches this node, so no tree this crate can build produces the pair below.
/// The state has to key on the same normalization they apply all the same, or the equality it
/// enforces would be one Prometheus does not have.
#[test]
fn a_claim_compares_label_sets_without_empty_values() {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut labels = LabelSets::default();
    labels.attach(&pool);
    let axis = TimeAxis::from_grid_spans(Arc::clone(&grid()), vec![GridSpan::new(0, 5)])
        .expect("one run over the whole grid");
    let series_of = |metric: SeriesLabels| Series {
        metric,
        time: axis.clone(),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0; 5]))),
    };

    labels
        .claim(&series_of(metric(&[(HOST, "h"), (ZONE, "")], Some(METRIC))))
        .expect("the first label set is claimed");
    let error = labels
        .claim(&series_of(metric(&[(HOST, "h")], Some(METRIC))))
        .expect_err("an empty label value is no label");
    assert!(
        error
            .to_string()
            .contains("vector cannot contain metrics with the same labelset"),
        "refused for another reason: {error}"
    );
    assert_eq!(
        vec![(Arc::from(HOST), Arc::from("h"))],
        label_set(&metric(&[(HOST, "h"), (ZONE, "")], None)),
        "the empty value is not part of the label set"
    );
}

/// A series whose chunks move forward, a hole apart, appends to its stored runs rather than
/// re-merging them.
///
/// One grid point per chunk with a hole after it (`0, 2, 4, ...`) is a legal stream and the shape a
/// short `count_over_time` over sub-sampled, chunked input produces: the chunks of one label set
/// hold their values at disjoint positions, so they merge into one series and no position ever
/// carries two values. What it used to cost was quadratic - `claim` walked the whole stored history
/// to look for a collision and then rebuilt all of it with a merge, for each chunk - and all of it
/// ran under the label-set lock every partition shares, so a sparse series of `K` chunks made the
/// other partitions wait for `K(K+1)/2` run moves.
///
/// The counter is the runtime's own, so this test measures the append path the native table takes
/// as well: one fix, not two that can drift apart again.
#[test]
fn a_forward_chunk_does_not_rebuild_the_stored_runs() {
    // How many one-position chunks the series arrives in: enough that the quadratic cost it used to
    // pay would be unmistakable, since that cost was the square of this figure.
    const CHUNKS: usize = 64;

    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut labels = LabelSets::default();
    labels.attach(&pool);
    // Two positions per chunk, so a hole follows every point the series holds.
    let grid = Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, 2 * CHUNKS as u64).unwrap())
            .unwrap(),
    );
    let chunk = |position: usize| Series {
        metric: metric(&[(HOST, "h")], Some(METRIC)),
        time: TimeAxis::from_grid_spans(Arc::clone(&grid), vec![GridSpan::new(position, 1)])
            .expect("one position of the grid"),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0]))),
    };

    let before = absorb_rebuilds();
    for index in 0..CHUNKS {
        labels
            .claim(&chunk(2 * index))
            .expect("the chunks of one label set hold values at disjoint positions");
    }

    assert_eq!(
        0,
        absorb_rebuilds() - before,
        "the {CHUNKS} forward chunks rebuilt the stored runs instead of appending to them"
    );
    assert_eq!(
        1,
        labels.seen.len(),
        "every chunk belongs to one label set, so the history is one entry"
    );
}

/// A backward disjoint claim after a spare-capacity run merge updates the exact memory charge.
#[test]
fn a_backward_disjoint_claim_after_spare_capacity_is_charged_correctly() {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut labels = LabelSets::default();
    labels.attach(&pool);
    let grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, 16).unwrap()).unwrap());
    let chunk = |start, len| Series {
        metric: metric(&[(HOST, "h")], Some(METRIC)),
        time: TimeAxis::from_grid_spans(Arc::clone(&grid), vec![GridSpan::new(start, len)])
            .unwrap(),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0; len]))),
    };
    labels.claim(&chunk(10, 2)).unwrap();
    let runs = labels
        .seen
        .values_mut()
        .next()
        .unwrap()
        .first_mut()
        .unwrap();
    let old_capacity = runs.capacity();
    runs.reserve(16);
    let new_capacity = runs.capacity();
    labels
        .reservation
        .as_mut()
        .unwrap()
        .try_grow((new_capacity - old_capacity) * std::mem::size_of::<crate::native::runs::Run>())
        .unwrap();
    let before = pool.reserved();

    labels
        .claim(&chunk(0, 1))
        .expect("the position is disjoint and earlier than stored history");
    let runs = labels.seen.values().next().unwrap().first().unwrap();
    assert_eq!(
        vec![(0, 1), (10, 12)],
        runs.iter()
            .map(|run| (run.start, run.end))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        before - (new_capacity - runs.capacity()) * std::mem::size_of::<crate::native::runs::Run>(),
        pool.reserved(),
        "the accounting follows the replacement vector's actual capacity"
    );
}

/// A backward claim that joins two spare-capacity runs charges a replacement vector's shrinkage.
#[test]
fn a_backward_claim_merging_spare_capacity_runs_accounts_for_shrinkage() {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut labels = LabelSets::default();
    labels.attach(&pool);
    let grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, 3).unwrap()).unwrap());
    let chunk = |values: Vec<Option<f64>>| Series {
        metric: metric(&[(HOST, "shrink")], Some(METRIC)),
        time: TimeAxis::from_grid_spans(Arc::clone(&grid), vec![GridSpan::new(0, 3)])
            .expect("the chunk covers the small grid"),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(values))),
    };

    labels
        .claim(&chunk(vec![Some(1.0), None, Some(1.0)]))
        .expect("the first claim stores runs at positions 0 and 2");
    let runs = labels.seen.values().next().unwrap().first().unwrap();
    assert_eq!(
        vec![(0, 1), (2, 3)],
        runs.iter()
            .map(|run| (run.start, run.end))
            .collect::<Vec<_>>()
    );
    let before_capacity = runs.capacity();
    let before_reserved = pool.reserved();

    labels
        .claim(&chunk(vec![None, Some(1.0), None]))
        .expect("the disjoint middle position joins both existing runs");

    let runs = labels.seen.values().next().unwrap().first().unwrap();
    let after_capacity = runs.capacity();
    assert_eq!(
        vec![(0, 3)],
        runs.iter()
            .map(|run| (run.start, run.end))
            .collect::<Vec<_>>()
    );
    assert!(
        before_capacity > after_capacity,
        "the regression requires replacement-vector shrinkage (before={before_capacity}, after={after_capacity})"
    );
    let run_size = std::mem::size_of::<crate::native::runs::Run>();
    assert_eq!(
        before_reserved - (before_capacity - after_capacity) * run_size,
        pool.reserved(),
        "the reservation tracks the replacement vector's actual capacity decrease"
    );
}

/// A newly retained field is charged for the cloned run vector's capacity, not incoming slack.
#[test]
fn a_new_field_claim_is_charged_for_its_retained_capacity() {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut labels = LabelSets::default();
    labels.attach(&pool);
    let grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, 4).unwrap()).unwrap());
    let first = Series {
        metric: metric(&[(HOST, "new-field")], Some(METRIC)),
        time: TimeAxis::from_grid_spans(Arc::clone(&grid), vec![GridSpan::new(0, 1)]).unwrap(),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0]))),
    };
    labels.claim(&first).unwrap();

    let incoming = Series {
        metric: metric(&[(HOST, "new-field")], Some(METRIC)),
        time: TimeAxis::from_grid_spans(Arc::clone(&grid), vec![GridSpan::new(0, 4)]).unwrap(),
        fields: SeriesFields::try_from_vec(vec![
            Samples::from_float(Float64Array::from(vec![None, None, None, None])),
            Samples::from_float(Float64Array::from(vec![Some(1.0), None, Some(2.0), None])),
        ])
        .unwrap(),
    };
    let incoming_runs = occupied_runs(&incoming).unwrap();
    assert_eq!(2, incoming_runs[1].len());
    assert!(
        incoming_runs[1].capacity() > incoming_runs[1].len(),
        "the incoming run vector has spare capacity"
    );
    let before = pool.reserved();
    labels.claim(&incoming).unwrap();

    let retained = &labels.seen.values().next().unwrap()[1];
    assert_eq!(2, retained.len());
    assert_eq!(
        retained.capacity() * std::mem::size_of::<crate::native::runs::Run>(),
        pool.reserved() - before,
        "the reservation charges the retained cloned vector capacity"
    );
    assert!(
        retained.capacity() < incoming_runs[1].capacity(),
        "the incoming spare capacity was not retained or charged"
    );
}

/// Chunked fields are traversed sequentially while occupancy is converted into grid runs.
#[test]
fn chunked_samples_occupancy_preserves_sparse_rows() {
    let values = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::from(vec![Some(1.0), None])),
        Samples::from_float(Float64Array::from(vec![None, Some(2.0)])),
    ])
    .unwrap();
    let series = Series {
        metric: metric(&[(HOST, "chunked")], Some(METRIC)),
        time: TimeAxis::from_grid_spans(Arc::clone(&grid()), vec![GridSpan::new(0, 4)]).unwrap(),
        fields: SeriesFields::one(values),
    };
    let runs = occupied_runs(&series).expect("chunked occupancy is read through a forward cursor");
    assert_eq!(
        vec![(0, 1), (3, 4)],
        runs[0]
            .iter()
            .map(|run| (run.start, run.end))
            .collect::<Vec<_>>()
    );
}

/// Appending a run that touches stored history reuses its last slot instead of rebuilding it.
#[test]
fn a_touching_append_with_full_capacity_does_not_rebuild_history() {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut labels = LabelSets::default();
    labels.attach(&pool);
    let grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, 16).unwrap()).unwrap());
    let chunk = |start| Series {
        metric: metric(&[(HOST, "touch")], Some(METRIC)),
        time: TimeAxis::from_grid_spans(Arc::clone(&grid), vec![GridSpan::new(start, 1)]).unwrap(),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0]))),
    };
    labels.claim(&chunk(0)).unwrap();
    let stored = labels
        .seen
        .values_mut()
        .next()
        .unwrap()
        .first_mut()
        .unwrap();
    stored.shrink_to_fit();
    let cap = stored.capacity();
    let allocation = stored.as_ptr();
    assert_eq!(
        stored.len(),
        cap,
        "the test requires a full-capacity run list"
    );
    let before = absorb_rebuilds();
    labels
        .claim(&chunk(1))
        .expect("the touching position extends the same run");
    let stored = labels.seen.values().next().unwrap().first().unwrap();
    assert_eq!(
        vec![(0, 2)],
        stored
            .iter()
            .map(|run| (run.start, run.end))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        cap,
        stored.capacity(),
        "extending a run did not allocate or copy its history"
    );
    assert_eq!(
        allocation,
        stored.as_ptr(),
        "extending a run retained the stored vector allocation"
    );
    assert_eq!(
        before,
        absorb_rebuilds(),
        "touching append uses the append path"
    );
}

/// A failed claim does not mutate the shared entry, while its reservation owner remains alive.
#[test]
fn a_refused_growth_keeps_shared_label_entry_unchanged() {
    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
    let limit = 4096;
    let pool = Arc::new(GreedyMemoryPool::new(limit));
    let erased: Arc<dyn MemoryPool> = pool.clone();
    let mut labels = LabelSets::default();
    labels.attach(&erased);
    let grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, 16).unwrap()).unwrap());
    let chunk = |host: &str, start| Series {
        metric: metric(&[(HOST, host)], Some(METRIC)),
        time: TimeAxis::from_grid_spans(Arc::clone(&grid), vec![GridSpan::new(start, 1)]).unwrap(),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0]))),
    };
    labels
        .claim(&chunk("shared", 0))
        .expect("initial label fits");
    let key = label_set(&chunk("shared", 0).metric);
    let before_runs = labels.seen[&key][0]
        .iter()
        .map(|run| (run.start, run.end))
        .collect::<Vec<_>>();
    let before_reserved = pool.reserved();
    let blocker: MemoryReservation = MemoryConsumer::new("test blocker").register(&erased);
    blocker
        .try_grow(limit - before_reserved)
        .expect("the blocker occupies remaining capacity");
    let error = labels
        .claim(&chunk("shared", 2))
        .expect_err("growth exceeds the pool");
    assert!(!error.to_string().is_empty(), "pool refusal is reported");
    assert_eq!(
        before_runs,
        labels.seen[&key][0]
            .iter()
            .map(|run| (run.start, run.end))
            .collect::<Vec<_>>(),
        "the persistent entry is unchanged"
    );
    assert_eq!(limit, pool.reserved(), "both reservations remain live");
    drop(blocker);
    assert_eq!(
        before_reserved,
        pool.reserved(),
        "the plan-owned entry remains charged"
    );
}

/// Binding ordering includes field names, while schema ordering refuses unequal DF schemas.
#[test]
fn unequal_binding_and_output_schema_are_not_ordered_equal() {
    let tree = rate_tree();
    let left = binding(&tree);
    let other_schema = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: "different".into(),
        }]),
        eval_grid: None,
    });
    let right = binding_of(&tree, other_schema, left.tags.clone());
    assert_ne!(left, right);
    assert_ne!(Some(std::cmp::Ordering::Equal), left.partial_cmp(&right));

    let left_node = fused(tree.clone());
    let nullability_changed = Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, false),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(NAME, DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let right_node = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding(&rate_tree()),
        input_plan(frame_schema()),
        nullability_changed.to_dfschema_ref().unwrap(),
        FusedOutputFormat::PlainRows,
    )
    .unwrap();
    assert_ne!(left_node, right_node);
    assert_ne!(
        Some(std::cmp::Ordering::Equal),
        left_node.partial_cmp(&right_node)
    );
}

/// A chunk reaching back before the last stored run still merges into the history.
///
/// The forward path only takes a chunk that starts at or after the end of the last stored run; a
/// chunk that starts before it can interleave with what is stored, so it has to go through the full
/// merge, and the result still has to be sorted, disjoint and with the runs that touch joined -
/// which is what the look-up in [`LabelSets::claim`] relies on.
///
/// A chunk reaching back over a stored run is the other case, and it stays refused: `[0, 11)` shares
/// position 10 with the stored `[10, 12)`, so it is a collision and the stored history is left
/// as it was.
#[test]
fn a_chunk_reaching_back_still_merges() {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut labels = LabelSets::default();
    labels.attach(&pool);
    let grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, 16).unwrap()).unwrap());
    // One chunk holding `len` positions from `start`, every one of them with a value.
    let chunk = |start: usize, len: usize| Series {
        metric: metric(&[(HOST, "h")], Some(METRIC)),
        time: TimeAxis::from_grid_spans(Arc::clone(&grid), vec![GridSpan::new(start, len)])
            .expect("one run over the grid"),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0; len]))),
    };
    // The runs stored under the one label set, which one field of the output holds.
    let stored = |labels: &LabelSets| -> Vec<(usize, usize)> {
        labels
            .seen
            .values()
            .next()
            .expect("the chunk was recorded under one label set")
            .first()
            .expect("the series has one field")
            .iter()
            .map(|run| (run.start, run.end))
            .collect()
    };

    labels
        .claim(&chunk(10, 2))
        .expect("the first chunk claims the positions it holds");
    assert_eq!(
        vec![(10, 12)],
        stored(&labels),
        "the run `[10, 12)` is stored"
    );

    let error = labels
        .claim(&chunk(0, 11))
        .expect_err("`[0, 11)` holds a value at position 10, where `[10, 12)` holds one");
    assert!(
        error
            .to_string()
            .contains("vector cannot contain metrics with the same labelset"),
        "refused for another reason: {error}"
    );
    assert_eq!(
        vec![(10, 12)],
        stored(&labels),
        "a refused claim left the stored history as it was"
    );

    let before = absorb_rebuilds();
    labels
        .claim(&chunk(0, 10))
        .expect("`[0, 10)` reaches back before the last stored run without sharing a position");
    assert_eq!(
        vec![(0, 12)],
        stored(&labels),
        "the run reaching back did not merge into the stored one and join it"
    );
    assert_eq!(
        1,
        absorb_rebuilds() - before,
        "a chunk starting before the last stored run was appended rather than merged"
    );
}

// ---------------------------------------------------------------------------
// The two output formats
//
// The same plan answers in one of two shapes. `PlainRows` is the row table a query returns: one row
// per carried grid position, one column per field and one per declared tag. `SeriesFrame` is the
// unified evaluated frame: one row per run of positions, carrying the series' complete labels, so
// the *same* chunks are handed over without being flattened. Everything below compares the two
// shapes against each other rather than against a second expectation: what has to hold is that the
// format is a transport choice and not a second result.
// ---------------------------------------------------------------------------

/// One output row: the grid timestamp, one value per field, and one tag value per declared tag.
type OutputRow = (i64, Vec<Option<f64>>, Vec<Option<String>>);

/// The same row with every value as its bits, so `NaN` and a missing sample are not the same row.
///
/// Comparing `Option<f64>` directly would call two `NaN`s equal and, worse, cannot tell a value
/// that was dropped from one that was never there; comparing the bit patterns tells all of it
/// apart, and `None` (a null) stays distinct from `Some(bits)`.
type BitRow = (i64, Vec<Option<u64>>, Vec<Option<String>>);

/// The bits of `values`, which is what comparing samples without `NaN != NaN` needs.
fn bit_values(values: &[Option<f64>]) -> Vec<Option<u64>> {
    values.iter().map(|value| value.map(f64::to_bits)).collect()
}

/// `rows` with every value turned into its bits.
fn bit_rows(rows: &[OutputRow]) -> Vec<BitRow> {
    rows.iter()
        .map(|(time, values, tags)| (*time, bit_values(values), tags.clone()))
        .collect()
}

/// The rows in one canonical order, so two formats that answer the same rows compare equal.
///
/// The two paths do not order their series the same way: the row table yields one batch per chunk in
/// the order the partitions settle, the frame keeps its chains in input order. The order of the
/// *rows within* a series is not free though - a series' positions ascend, and the merge the frame
/// promises is per series - so sorting by timestamp, then tags, then the values themselves is what
/// makes the comparison about content only.
fn sorted_rows(mut rows: Vec<OutputRow>) -> Vec<OutputRow> {
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| bit_values(&left.1).cmp(&bit_values(&right.1)))
    });
    rows
}

/// The rows of a row-table output: `fields` per field column and `tags` per declared tag column.
fn row_table(batches: &[RecordBatch], fields: &[&str], tags: &[&str]) -> Vec<OutputRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let time = batch
            .column_by_name(TIME)
            .expect("every batch has a time column")
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("the time column is a millisecond timestamp");
        let values = fields
            .iter()
            .map(|field| {
                batch
                    .column_by_name(field)
                    .unwrap_or_else(|| panic!("every batch has a {field} column"))
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("a field column is Float64")
            })
            .collect::<Vec<_>>();
        let labels = tags
            .iter()
            .map(|tag| {
                batch
                    .column_by_name(tag)
                    .unwrap_or_else(|| panic!("every batch has a {tag} column"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("a tag column is Utf8")
            })
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            rows.push((
                time.value(row),
                values
                    .iter()
                    .map(|column| column.is_valid(row).then(|| column.value(row)))
                    .collect(),
                labels
                    .iter()
                    .map(|column| column.is_valid(row).then(|| column.value(row).to_string()))
                    .collect(),
            ));
        }
    }
    rows
}

/// The values `series` holds, one list per field, one entry per carried position.
fn series_values(series: &Series) -> Vec<Vec<Option<f64>>> {
    series
        .fields
        .iter()
        .map(|field| {
            field
                .iter()
                .map(|sample| match sample {
                    SampleRef::Missing => None,
                    SampleRef::Float(value) => Some(value),
                    SampleRef::Histogram(_) => {
                        panic!("these tests carry float samples only")
                    }
                })
                .collect()
        })
        .collect()
}

/// The rows a frame output carries: every decoded chunk's positions with its labels.
///
/// A chunk's axis names exactly the positions it holds - the holes between its runs are simply not
/// rows - so nothing is filled in here: a grid position absent from every chunk stays absent.
fn frame_table(decoded: &[(Series, bool)], tags: &[&str]) -> Vec<OutputRow> {
    let mut rows = Vec::new();
    for (series, _) in decoded {
        let timestamps = series
            .time
            .timestamps()
            .expect("a decoded chunk names its positions");
        let values = series_values(series);
        let labels = tags
            .iter()
            .map(|tag| series.metric.get(tag).map(str::to_string))
            .collect::<Vec<_>>();
        for (index, time) in timestamps.iter().enumerate() {
            rows.push((
                *time,
                values.iter().map(|field| field[index]).collect(),
                labels.clone(),
            ));
        }
    }
    rows
}

/// The batches a frame plan yields, each checked against the schema the plan declares.
///
/// The frame is only decodable if every batch agrees with the declaration the plan made, metadata
/// included: the encoder sets the plan's own schema on each batch it hands out, and a batch that
/// slipped through with the builder's schema would decode under a different declaration.
async fn collect_frames(exec: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    let schema = exec.schema();
    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("the node runs a frame plan");
    for batch in &batches {
        assert_eq!(
            schema,
            batch.schema(),
            "a frame batch has to declare the schema the plan does"
        );
    }
    batches
}

/// The chunks `batches` decode to, through the shared evaluated-frame decoder.
async fn decoded_frames(batches: Vec<RecordBatch>) -> Vec<(Series, bool)> {
    let schema = batches
        .first()
        .expect("a frame plan answers at least one batch")
        .schema();
    let source = Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)),
    ));
    let mut decoded = unpack_evaluated_series(source, None)
        .expect("the schema a frame plan declares is an evaluated frame");
    let mut chunks = Vec::new();
    while let Some(chunk) = decoded.next().await {
        chunks.push(chunk.expect("the frame decodes"));
    }
    chunks
}

/// The rows `node` answers over `input` in the row table, read off `fields` and `tags`.
async fn plain_rows_of(
    node: &FusedSeries,
    input: Arc<dyn ExecutionPlan>,
    fields: &[&str],
    tags: &[&str],
) -> Vec<OutputRow> {
    let exec = node
        .to_execution_plan(input, RawLabelSource::Explicit, SeriesOrdering::Contiguous)
        .expect("the runtime executes this tree in the row format");
    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("the node runs in the row format");
    row_table(&batches, fields, tags)
}

/// The rows `node` answers over `input` in the frame format, decoded back into rows.
async fn frame_rows_of(
    node: &FusedSeries,
    input: Arc<dyn ExecutionPlan>,
    tags: &[&str],
) -> Vec<OutputRow> {
    let node = in_format(node, FusedOutputFormat::SeriesFrame);
    let exec = node
        .to_execution_plan(input, RawLabelSource::Explicit, SeriesOrdering::Contiguous)
        .expect("the runtime executes this tree in the frame format");
    let batches = collect_frames(exec).await;
    let decoded = decoded_frames(batches).await;
    frame_table(&decoded, tags)
}

/// The same node, answering in `format`.
///
/// Everything but the format is the node's own: the tree, the binding it was bound against, its
/// input and the row table it could answer in. The row-table schema is handed over unchanged even
/// for a frame, because a frame derives its own declaration from the tree's evaluated output.
fn in_format(node: &FusedSeries, format: FusedOutputFormat) -> FusedSeries {
    let input = node
        .inputs()
        .into_iter()
        .next()
        .expect("the node runs one input")
        .clone();
    FusedSeries::try_new(
        Arc::from(node.promql()),
        node.tree().clone(),
        node.binding().clone(),
        input,
        Arc::clone(node.schema()),
        format,
    )
    .expect("the same node in another format")
}

/// `abs(rate(x[5m]))`: the window with a map over it, which is a second node above the evaluated
/// output the frame is encoded from.
fn abs_rate_tree() -> NativeTree {
    let rate = rate_tree();
    let output = rate.root().output.clone();
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(rate.root().clone()),
            op: BoundMap {
                ops: Arc::from([MapOp::Math(MathOp::Abs)]),
            },
        },
        output,
    })
    .expect("a map over a window is a legal tree")
}

/// `label_replace(rate(x[5m]), "zone", "west", "host", "web-1")`.
///
/// The labels a frame carries are the ones the rewrite *wrote*, not the ones the input arrived with:
/// `web-1` gains `zone="west"` and `web-2` does not, so a frame that reported the input's labels
/// would be caught rather than passing for the result.
fn label_rewritten_tree() -> NativeTree {
    let rate = rate_tree();
    let output = rate.root().output.clone();
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(rate.root().clone()),
            op: BoundMap {
                ops: Arc::from([MapOp::Label(Arc::new(
                    LabelRewrite::replace(ZONE, "west", HOST, "web-1")
                        .expect("the rewrite is a legal label_replace"),
                ))]),
            },
        },
        output,
    })
    .expect("a label rewrite over a window is a legal tree")
}

/// A scalar: one value per grid point and no series to name.
fn scalar_tree() -> NativeTree {
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value: 1.0 }),
        output: NativeType::Scalar(grid()),
    })
    .expect("a scalar is a legal tree")
}

/// `sum by (job)`'s `Partial` stage: the same aggregate, output as state for a later merge.
fn partial_aggregate_tree() -> NativeTree {
    let complete = aggregate_tree();
    let root = complete.root();
    let NativeExpr::Aggregate { input, op } = &root.expr else {
        unreachable!("the fixture is an aggregate over a window");
    };
    let grid = input
        .output
        .samples()
        .expect("the aggregate reads an evaluated vector")
        .eval_grid
        .clone()
        .expect("the aggregate's input is evaluated on a grid");
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Aggregate {
            input: input.clone(),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: op.group.clone(),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(AggregateStateSchema {
            group: op.group.clone(),
            accumulators: Arc::from([FieldSpec { name: VALUE.into() }]),
            grid,
        })),
    })
    .expect("a partial aggregate is a legal tree")
}

/// The binding a tree over the raw samples of [`raw_schema`] declares: the tags and the grid
/// [`rate_tree`] is evaluated on, which every tree in these tests shares.
///
/// It is read from [`rate_tree`] rather than from the tree under test because the trees that need
/// it are the ones whose own output is not samples at all - a scalar, an aggregate's partial state -
/// and a binding has to name the fields and the grid the *node* answers on.
fn raw_binding() -> FusedSeriesBinding {
    binding(&rate_tree())
}

/// `rate(x[5m])` over frames that carry `zone` as well as `host`, with both in the output.
fn zone_node_of(tree: NativeTree) -> FusedSeries {
    let binding = binding_of(
        &tree,
        raw_schema(),
        vec![HOST.to_string(), ZONE.to_string(), NAME.to_string()],
    );
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(frame_schema()),
        zone_output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::PlainRows,
    )
    .expect("the binding matches the tree")
}

/// The physical node `tree` would run over `input`, built without the logical node in between.
///
/// [`FusedSeriesExec::try_new`] is where the format is turned into a declaration, so a tree or a
/// schema it refuses is refused here as well - and with the same error the logical node reports,
/// because both read the same two facts.
fn framed_exec(
    tree: NativeTree,
    format: FusedOutputFormat,
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
) -> DataFusionResult<Arc<FusedSeriesExec>> {
    FusedSeriesExec::try_new(
        Arc::from(PROMQL),
        input,
        RawLabelSource::Explicit,
        SeriesOrdering::Contiguous,
        Arc::new(tree),
        output_schema,
        TIME.to_string(),
        vec![HOST.to_string(), NAME.to_string()],
        format,
    )
    .map(Arc::new)
}

/// The row format and the frame format answer the same `rate(x[5m])`.
///
/// The rates are the two counters' own - 1 and 2 - at every grid point of both series, and they are
/// compared bit for bit: a value that reached the frame as a null, or a position one path dropped,
/// is a difference and not a rounding.
#[tokio::test]
async fn both_formats_carry_the_same_rate_values() {
    let input = memory_exec(two_series_frames());
    let node = fused(rate_tree());
    let rows = plain_rows_of(&node, Arc::clone(&input), &[VALUE], &[HOST, NAME]).await;
    let frames = frame_rows_of(&node, input, &[HOST, NAME]).await;

    assert_eq!(
        2 * GRID_LEN as usize,
        rows.len(),
        "two series cover every grid point"
    );
    assert_eq!(
        bit_rows(&sorted_rows(rows.clone())),
        bit_rows(&sorted_rows(frames)),
        "the frame carries different rows than the row table"
    );
    // Both paths also carry the values the counters' rates are, so the comparison above is not two
    // mistakes agreeing.
    for expected_host in ["web-1", "web-2"] {
        let rate = if expected_host == "web-1" { 1.0 } else { 2.0 };
        let mut series = rows
            .iter()
            .filter(|(_, _, tags)| tags[0].as_deref() == Some(expected_host))
            .cloned()
            .collect::<Vec<_>>();
        series.sort_by_key(|row| row.0);
        assert_eq!(
            grid_timestamps(),
            series.iter().map(|row| row.0).collect::<Vec<_>>(),
            "{expected_host} covers the whole grid"
        );
        for (time, values, _) in &series {
            let value = values[0].expect("rate answers a value at every grid point");
            assert!(
                (rate - value).abs() < 1e-9,
                "{expected_host} at {time}: {value} is not {rate}"
            );
        }
    }
}

/// The two formats answer the same mapped vector: `abs(rate(x[5m]))`.
///
/// A map above the window is the shape a real query has most of the time, and it is a second node
/// between the evaluated output and the frame: what the frame encodes is the map's output, not the
/// window's. The values, the positions and the labels still have to match the row table's.
#[tokio::test]
async fn both_formats_carry_the_same_mapped_values() {
    let input = memory_exec(two_series_frames());
    let node = fused(abs_rate_tree());
    let rows = plain_rows_of(&node, Arc::clone(&input), &[VALUE], &[HOST, NAME]).await;
    let frames = frame_rows_of(&node, input, &[HOST, NAME]).await;

    assert_eq!(
        2 * GRID_LEN as usize,
        rows.len(),
        "two series cover every grid point"
    );
    assert_eq!(
        bit_rows(&sorted_rows(rows.clone())),
        bit_rows(&sorted_rows(frames)),
        "the frame carries different rows than the row table"
    );
    assert!(
        rows.iter()
            .all(|(_, values, _)| values[0].is_some_and(|value| value >= 0.0)),
        "abs answers a non-negative value at every grid point: {rows:?}"
    );
}

/// The two formats answer the same instant selection, labels included.
///
/// An instant selection rewrites nothing, so what it answers is the input's own labels at the
/// selected points: a frame that kept them (rather than dropping the name the way a window does)
/// is what the row table has to agree with.
#[tokio::test]
async fn both_formats_carry_the_same_instant_selection() {
    let input = memory_exec(two_series_frames());
    let node = fused(instant_tree());
    let rows = plain_rows_of(&node, Arc::clone(&input), &[VALUE], &[HOST, NAME]).await;
    let frames = frame_rows_of(&node, input, &[HOST, NAME]).await;

    assert_eq!(
        2 * GRID_LEN as usize,
        rows.len(),
        "two series hold a sample for every grid point"
    );
    assert!(
        rows.iter()
            .all(|(_, _, tags)| tags[1].as_deref() == Some(METRIC)),
        "an instant selection keeps the metric name: {rows:?}"
    );
    assert_eq!(
        bit_rows(&sorted_rows(rows)),
        bit_rows(&sorted_rows(frames)),
        "the frame carries different rows than the row table"
    );
}

/// The two formats answer the same rewritten labels.
///
/// The labels compared are the ones the *rewrite* wrote: `web-1` gains `zone="west"` and `web-2`,
/// which the expression does not match, gains nothing. A frame that carried the input's labels would
/// therefore differ from the row table here, and so would one that dropped the label the rewrite
/// added.
#[tokio::test]
async fn both_formats_carry_the_same_rewritten_labels() {
    let input = memory_exec(two_series_frames());
    let node = zone_node_of(label_rewritten_tree());
    let rows = plain_rows_of(&node, Arc::clone(&input), &[VALUE], &[HOST, ZONE]).await;
    let frames = frame_rows_of(&node, input, &[HOST, ZONE]).await;

    assert_eq!(
        2 * GRID_LEN as usize,
        rows.len(),
        "two series cover every grid point"
    );
    // The rewrite is visible in both shapes: this is the label the result names, not the one the
    // input carried.
    for (name, expected) in [("web-1", Some("west")), ("web-2", None)] {
        let series = rows
            .iter()
            .filter(|(_, _, tags)| tags[0].as_deref() == Some(name))
            .collect::<Vec<_>>();
        assert_eq!(GRID_LEN as usize, series.len(), "{name} covers the grid");
        assert!(
            series
                .iter()
                .all(|(_, _, tags)| tags[1].as_deref() == expected),
            "{name} answers the rewritten zone: {series:?}"
        );
    }
    assert_eq!(
        bit_rows(&sorted_rows(rows)),
        bit_rows(&sorted_rows(frames)),
        "the frame carries different rows than the row table"
    );
}

/// A frame keeps the holes a sparse chunk leaves: nothing is filled in.
///
/// The counter here stops two minutes in, so only the first grid point's window holds two samples
/// and every later point holds one: `rate` answers one position and leaves four holes. A frame that
/// densified its axis - writing a null at every grid point - would report five positions, and the
/// row table, which walks the carried rows, reports one.
#[tokio::test]
async fn a_frame_keeps_a_sparse_chunks_holes() {
    let input = memory_exec(frames(&[series_chunk(
        "web-1",
        &[(0, 0.0), (60_000, 1.0), (120_000, 2.0)],
        true,
    )]));
    let node = fused(rate_tree());
    let rows = plain_rows_of(&node, Arc::clone(&input), &[VALUE], &[HOST, NAME]).await;
    let frames = frame_rows_of(&node, input, &[HOST, NAME]).await;

    assert_eq!(
        vec![GRID_START],
        sorted_rows(rows.clone())
            .iter()
            .map(|row| row.0)
            .collect::<Vec<_>>(),
        "only the first window holds two samples"
    );
    assert!(
        (rows.len() as u64) < GRID_LEN,
        "this fixture has to leave holes: {rows:?}"
    );
    assert_eq!(
        bit_rows(&sorted_rows(rows)),
        bit_rows(&sorted_rows(frames)),
        "the frame filled in positions the row table left empty"
    );
}

/// The two formats answer the same two fields, and a missing sample stays missing.
///
/// The two chains fill *different* fields of one label set, which is the pair Prometheus merges: the
/// row table yields one null per row in one of its two field columns, and the frame has to yield
/// the same null rather than a zero or a position it does not hold.
#[tokio::test]
async fn both_formats_carry_the_same_two_field_values() {
    let input = memory_exec_partitioned_with(
        frame_schema_of(&two_field_schema()),
        vec![
            frames_of(
                &two_field_schema(),
                &[one_field_chunk(0, "web-1", "counter", true)],
            ),
            frames_of(
                &two_field_schema(),
                &[one_field_chunk(1, "web-1", "gauge", true)],
            ),
        ],
    );
    let node = two_field_node();
    let rows = plain_rows_of(&node, Arc::clone(&input), &[VALUE, OTHER], &[HOST, NAME]).await;
    let frames = frame_rows_of(&node, input, &[HOST, NAME]).await;

    assert_eq!(
        2 * GRID_LEN as usize,
        rows.len(),
        "each chain answers the whole grid"
    );
    for (field, (time, values, _)) in rows.iter().enumerate() {
        let filled = values.iter().filter(|value| value.is_some()).count();
        assert_eq!(
            1, filled,
            "each of the two chains fills exactly one field, row {field} at {time}: {values:?}"
        );
    }
    assert_eq!(
        bit_rows(&sorted_rows(rows)),
        bit_rows(&sorted_rows(frames)),
        "the frame carries different rows than the row table"
    );
}

/// A closing chunk that holds no position is a row of a frame and nothing in a row table.
///
/// `rate` closes a series on an empty chunk when the last grid point was already decided by an
/// earlier delivery: the counter's last sample lies past the grid, so the window has answered every
/// point it ever will and the chunk that closes the series carries no position at all. Such a chunk
/// still says something - the series ended here - and the frame is the only one of the two shapes
/// that can carry it: it becomes one row with no position and `is_last`, while the row table's
/// batch for it has no row to put it in.
#[tokio::test]
async fn a_frame_carries_the_closing_chunk_a_row_table_drops() {
    let samples = (0..=9)
        .map(|index| (index * SAMPLE_STEP, index as f64))
        .collect::<Vec<_>>();
    let input = memory_exec(frames(&[
        series_chunk("web-1", &samples[..3], false),
        series_chunk("web-1", &samples[3..], false),
        // Past the last grid point, so the chunk that closes the series decides nothing.
        series_chunk("web-1", &[(samples[9].0 + SAMPLE_STEP, 10.0)], true),
    ]));
    let rows_node = fused(rate_tree());
    let node = in_format(&rows_node, FusedOutputFormat::SeriesFrame);

    let rows = plain_rows_of(&rows_node, Arc::clone(&input), &[VALUE], &[HOST, NAME]).await;
    assert_eq!(
        GRID_LEN as usize,
        rows.len(),
        "the row table answers the grid and leaves the closing chunk out: {rows:?}"
    );

    let exec = node
        .to_execution_plan(input, RawLabelSource::Explicit, SeriesOrdering::Contiguous)
        .expect("the runtime executes this tree in the frame format");
    let batches = collect_frames(exec).await;
    let frame_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(
        2, frame_rows,
        "one row carries the grid and one carries the close"
    );

    let decoded = decoded_frames(batches).await;
    assert_eq!(
        2,
        decoded.len(),
        "the frame decodes to the values and to the close: {:?}",
        decoded
            .iter()
            .map(|(series, last)| (series.time.len(), *last))
            .collect::<Vec<_>>()
    );
    assert!(!decoded[0].1, "the values do not close the series");
    assert!(decoded[1].1, "the empty chunk is what closes it");
    assert!(
        decoded[1].0.time.is_empty(),
        "the closing chunk carries no position"
    );
    assert_eq!(
        grid_timestamps(),
        decoded[0]
            .0
            .time
            .timestamps()
            .expect("the axis names its positions")
            .to_vec(),
        "the values are the whole grid"
    );
}

/// Four input partitions stay four partitions in the frame format, and all four run.
///
/// The node answers one stream per input partition and adds none of its own, which is what keeps its
/// parallelism the scan's. The `is_last` of every chain is the proof that four streams ran rather
/// than one stream that happened to see four series: only a chain's own closure carries it.
#[tokio::test]
async fn four_partitions_stay_four_in_the_frame_format() {
    let rates = [1.0, 2.0, 3.0, 4.0];
    let partitions = rates
        .iter()
        .enumerate()
        .map(|(index, scale)| {
            vec![frame(&series_chunk(
                &format!("web-{index}"),
                &series_samples(*scale),
                true,
            ))]
        })
        .collect::<Vec<_>>();
    let node = in_format(&fused(rate_tree()), FusedOutputFormat::SeriesFrame);
    let exec = node
        .to_execution_plan(
            memory_exec_partitioned(partitions),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )
        .expect("the runtime executes this tree in the frame format");
    assert_eq!(
        4,
        exec.properties().output_partitioning().partition_count(),
        "the node's partition count is its input's"
    );

    let batches = collect_frames(exec).await;
    let decoded = decoded_frames(batches).await;
    assert_eq!(
        4,
        decoded.iter().filter(|(_, last)| *last).count(),
        "one chain per partition closed, so four streams ran"
    );

    let rows = frame_table(&decoded, &[HOST]);
    assert_eq!(
        4 * GRID_LEN as usize,
        rows.len(),
        "every partition answers its series over the whole grid"
    );
    for (host, rate) in rates
        .iter()
        .enumerate()
        .map(|(index, scale)| (format!("web-{index}"), *scale))
    {
        let mut series = rows
            .iter()
            .filter(|(_, _, tags)| tags[0].as_deref() == Some(host.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        series.sort_by_key(|row| row.0);
        assert_eq!(
            grid_timestamps(),
            series.iter().map(|row| row.0).collect::<Vec<_>>(),
            "{host} covers the whole grid"
        );
        for (time, values, _) in &series {
            let value = values[0].expect("rate answers a value at every grid point");
            assert!(
                (rate - value).abs() < 1e-9,
                "{host} at {time}: {value} is not {rate}"
            );
        }
    }
}

/// One label set from two chains is refused in rows and emitted in a frame.
///
/// The row table is the answer of one query: one label set may not hold a value twice, and this node
/// enforces that when it claims the label set for its own state, across partitions, because the
/// table is the plan's. A frame is not that answer - it is one chunk of one series handed over - so
/// the same pair is legal there: the two chains are two chunks of one label set, each closed by its
/// own `is_last`, and the merge that would refuse two values of one field happens after it.
#[tokio::test]
async fn the_same_label_set_from_two_chains_is_refused_in_rows_and_emitted_in_a_frame() {
    let partitions = vec![
        frames(&[series_chunk("web-1", &series_samples(1.0), true)]),
        frames(&[series_chunk("web-1", &series_samples(2.0), true)]),
    ];
    let input = memory_exec_partitioned(partitions);

    let exec =
        plan_of(fused(rate_tree()), Arc::clone(&input)).expect("the runtime executes a rate tree");
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "this test needs one chain in each of two partitions"
    );
    let error = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect_err("the second chain lands on the label set the first one claimed");
    let message = error_chain(&error);
    assert!(
        message.contains("vector cannot contain metrics with the same labelset"),
        "refused for another reason: {message}"
    );

    let node = in_format(&fused(rate_tree()), FusedOutputFormat::SeriesFrame);
    let exec = node
        .to_execution_plan(input, RawLabelSource::Explicit, SeriesOrdering::Contiguous)
        .expect("the runtime executes this tree in the frame format");
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "the frame keeps the input's partition count"
    );
    let batches = collect_frames(exec).await;
    let decoded = decoded_frames(batches).await;
    assert_eq!(
        2,
        decoded.iter().filter(|(_, last)| *last).count(),
        "each chain closes once, in its own partition"
    );
    assert_eq!(
        decoded[0].0.metric, decoded[1].0.metric,
        "the two chains carry the one label set"
    );
    let rows = frame_table(&decoded, &[HOST]);
    assert_eq!(
        2 * GRID_LEN as usize,
        rows.len(),
        "both chains' values survive: {rows:?}"
    );
}

/// `EXPLAIN` names the format of a frame and says nothing extra about the row table.
///
/// The line is the only place a reader can see that the same query answers something other than
/// rows, so it is checked byte for byte - both the logical line and the physical one's first line.
#[test]
fn the_explain_text_names_the_output_format() {
    let rows = fused(rate_tree());
    assert_eq!(
        "FusedSeries: rate(x[5m])",
        Explain(&rows).to_string(),
        "a row table says nothing extra"
    );
    let exec = plan_of(rows, memory_exec(Vec::new())).expect("the runtime executes a rate tree");
    let text = displayable(exec.as_ref()).indent(false).to_string();
    assert!(
        text.starts_with("FusedSeriesExec: rate(x[5m])\n"),
        "the physical line is unchanged for a row table: {text}"
    );

    let frames = in_format(&fused(rate_tree()), FusedOutputFormat::SeriesFrame);
    assert_eq!(
        "FusedSeries: rate(x[5m]) (output: series_frame)",
        Explain(&frames).to_string(),
        "the logical line names the frame"
    );
    let exec = frames
        .to_execution_plan(
            memory_exec(Vec::new()),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )
        .expect("the runtime executes a rate tree in the frame format");
    let text = displayable(exec.as_ref()).indent(false).to_string();
    assert!(
        text.starts_with("FusedSeriesExec: rate(x[5m]) (output: series_frame)\n"),
        "the physical line names the frame before the tree: {text}"
    );
}

/// The logical schema follows the format: the row table, or the frame's own declaration.
///
/// A frame's schema is not derived here: it is the codec's declaration of an evaluated frame, read
/// back with the codec's own parser. That is what makes the two agree - the plan cannot declare a
/// layout the decoder would not read - and it is why the fields, the grid and the ordering are
/// checked through [`SeriesFrameDeclaration`] rather than by name here.
#[test]
fn the_logical_schema_follows_the_output_format() {
    let tree = rate_tree();
    let rows = fused(tree.clone());
    assert_eq!(
        &output_schema(),
        rows.schema().inner(),
        "a row table declares the columns it answers with"
    );

    let frames = in_format(&rows, FusedOutputFormat::SeriesFrame);
    let declaration =
        SeriesFrameDeclaration::parse(frames.schema().inner()).expect("the frame declares itself");
    assert_eq!(
        FrameContent::Evaluated,
        declaration.content(),
        "the output is evaluated samples, not raw ones"
    );
    assert_eq!(
        FrameLabelMode::Explicit,
        declaration.labels(),
        "an evaluated frame names its series by their complete labels"
    );
    assert_eq!(
        SeriesOrdering::Contiguous,
        series_frame_ordering(frames.schema().inner()).expect("the ordering is a known one"),
        "the frame promises each series' rows contiguous"
    );
    let declared = declaration.series();
    assert_eq!(
        1,
        declared.fields.len(),
        "the frame carries the one field the tree's output has: {:?}",
        declared.fields
    );
    let grid = declared
        .eval_grid
        .as_ref()
        .expect("an evaluated frame declares its grid");
    let evaluated = evaluated_output(&tree).expect("a rate is evaluated on a grid");
    assert_eq!(
        evaluated
            .eval_grid
            .as_ref()
            .expect("the tree's output is on a grid")
            .spec(),
        grid.spec(),
        "the frame is addressed on the tree's own grid"
    );
    assert_eq!(GRID_START, grid.spec().start_ms);
}

/// Rebuilding the node keeps its format, both as a logical node and as a physical one.
///
/// The format is part of the plan's identity, so it has to survive every path that rebuilds a node
/// out of its parts: the logical `with_exprs_and_inputs`, and the physical `replace_children` the
/// optimizer walks the plan with. What the rebuilt plan must not do is fall back to rows - a plan
/// rebuilt into the other format would answer a schema its children were not built for.
#[test]
fn rebuilding_a_node_keeps_its_output_format() {
    let node = in_format(&fused(rate_tree()), FusedOutputFormat::SeriesFrame);
    let rebuilt = node
        .with_exprs_and_inputs(Vec::new(), vec![input_plan(frame_schema())])
        .expect("the node is rebuilt over its own input");
    assert_eq!(
        FusedOutputFormat::SeriesFrame,
        rebuilt.format(),
        "the logical rebuild kept the format"
    );
    assert_eq!(node, rebuilt, "the rebuild is the same plan");
    assert_eq!(
        "FusedSeries: rate(x[5m]) (output: series_frame)",
        Explain(&rebuilt).to_string()
    );

    let exec = node
        .to_execution_plan(
            memory_exec(one_series_frames()),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )
        .expect("the runtime executes a rate tree in the frame format");
    let text = displayable(exec.as_ref()).indent(false).to_string();
    assert!(
        text.contains(" (output: series_frame)"),
        "the frame is named in the plan that is rebuilt: {text}"
    );
    let replaced = Arc::clone(&exec)
        .replace_children(
            vec![memory_exec(one_series_frames())],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .expect("the optimizer's own rebuild accepts the node");
    assert_eq!(
        exec.schema(),
        replaced.schema(),
        "a rebuilt frame plan declares the same schema"
    );
    assert_eq!(
        text,
        displayable(replaced.as_ref()).indent(false).to_string(),
        "a rebuilt frame plan prints the same node line"
    );
}

/// A scalar root cannot be framed, and the physical plan says so when it is built.
///
/// A scalar has no series to name: there are no labels and no positions in a vector, so writing it
/// as evaluated float samples would invent a series. The refusal happens while the plan is built, so
/// a plan that is built is a plan that runs.
#[test]
fn a_series_frame_refuses_a_scalar_root_when_the_plan_is_built() {
    let error = framed_exec(
        scalar_tree(),
        FusedOutputFormat::SeriesFrame,
        memory_exec(Vec::new()),
        output_schema(),
    )
    .expect_err("a scalar has no series to frame");
    assert!(
        matches!(error, DataFusionError::Plan(_)),
        "the refusal is a plan error: {error}"
    );
    let message = error_chain(&error);
    assert!(
        message.contains("scalar"),
        "the refusal has to name what it refused: {message}"
    );
}

/// Aggregate state cannot be framed either, in either the logical or the physical build.
///
/// Partial state is a contribution to a later merge rather than a value: writing it as evaluated
/// float samples would hand a consumer a series it would read as an answer. The two builders read
/// the same fact - the tree's root type - so both refuse it.
#[test]
fn a_series_frame_refuses_an_aggregate_state_root_when_the_plan_is_built() {
    let error = framed_exec(
        partial_aggregate_tree(),
        FusedOutputFormat::SeriesFrame,
        memory_exec(Vec::new()),
        output_schema(),
    )
    .expect_err("state is a contribution, not a value");
    assert!(
        matches!(error, DataFusionError::Plan(_)),
        "the refusal is a plan error: {error}"
    );
    let message = error_chain(&error);
    assert!(
        message.contains("aggregate state"),
        "the physical refusal has to name what it refused: {message}"
    );

    let error = FusedSeries::try_new(
        Arc::from(PROMQL),
        partial_aggregate_tree(),
        raw_binding(),
        input_plan(frame_schema()),
        output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::SeriesFrame,
    )
    .expect_err("state is a contribution, not a value");
    assert!(
        matches!(error, DataFusionError::Plan(_)),
        "the refusal is a plan error: {error}"
    );
    let message = error_chain(&error);
    assert!(
        message.contains("aggregate state"),
        "the logical refusal has to name what it refused: {message}"
    );
}

/// A frame declared with the row table's layout is refused when the plan is built.
///
/// The schema a frame answers in is the codec's declaration for the tree's evaluated output, not the
/// columns a caller would like. A plan handed the row table's schema as its frame output would hand
/// out batches no decoder could read, so the mismatch is refused here rather than at the first
/// batch.
#[test]
fn a_series_frame_declared_with_another_layout_is_refused() {
    let error = framed_exec(
        rate_tree(),
        FusedOutputFormat::SeriesFrame,
        memory_exec(Vec::new()),
        output_schema(),
    )
    .expect_err("the row table is not a series frame");
    assert!(
        matches!(error, DataFusionError::Plan(_)),
        "the refusal is a plan error: {error}"
    );
    let message = error_chain(&error);
    assert!(
        message.contains("series frame output"),
        "the refusal has to name what it refused: {message}"
    );
}

/// A frame hands its buffered rows over when its input settles nothing.
///
/// A frame batches: the rows a chunk produces are buffered until the builder's bounds are reached,
/// which is what makes one frame batch out of many chunks. That buffering must not turn into a
/// stall - a consumer waiting for the position that closes a series would be waiting for input that
/// may never arrive - so the rows a frame holds are handed over at the pending boundary and polled
/// again afterwards.
///
/// The staged input goes pending after the second batch on purpose: the first batch's samples lie
/// before the first grid point and decide nothing, the second one decides the first two points, and
/// it is those two buffered positions that the pending poll has to hand over in the same poll.
#[tokio::test]
async fn a_pending_input_hands_over_the_rows_a_frame_holds() {
    let schema = frame_schema();
    let batches = one_series_frames();
    let source = staged(schema.clone(), move || {
        Box::pin(StagedStream::new(schema.clone(), batches.clone()).pending_after(2))
    });
    let node = in_format(&fused(rate_tree()), FusedOutputFormat::SeriesFrame);
    let exec = node
        .to_execution_plan(source, RawLabelSource::Explicit, SeriesOrdering::Contiguous)
        .expect("the runtime executes a rate tree in the frame format");
    let declared = exec.schema();
    let mut stream = exec
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    // The first poll answers a batch: the rows the frame held when the input went pending, handed
    // over instead of waiting for input behind it.
    let first = {
        let next = stream.next();
        futures::pin_mut!(next);
        match futures::poll!(next.as_mut()) {
            Poll::Ready(Some(Ok(batch))) => batch,
            Poll::Ready(Some(Err(error))) => panic!("the first poll failed: {error}"),
            Poll::Ready(None) => panic!("the first poll ended the stream"),
            Poll::Pending => panic!("the frame held its rows while its input settled nothing"),
        }
    };
    assert_eq!(
        declared,
        first.schema(),
        "the batch handed over declares the plan's schema"
    );
    assert!(
        first.num_rows() > 0,
        "a batch handed over at the pending boundary carries rows"
    );
    // It is a stretch of the answer rather than the whole of it: the series' chain is still open, so
    // a frame that had buffered everything would hand nothing over before its input ended.
    assert!(
        first.num_rows() < GRID_LEN as usize,
        "the batch handed over is the rows the frame held, not the finished series: {} rows",
        first.num_rows()
    );

    let mut answered = vec![first];
    answered.extend(
        stream
            .try_collect::<Vec<_>>()
            .await
            .expect("the stream resumes"),
    );
    let decoded = decoded_frames(answered).await;
    assert_eq!(
        1,
        decoded.iter().filter(|(_, last)| *last).count(),
        "the series closes exactly once"
    );
    let rows = frame_table(&decoded, &[HOST]);
    assert_eq!(
        grid_timestamps(),
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        "the pending boundary lost or repeated no position"
    );
}

/// A frame drops what it buffered when its input fails, and the failure is reported.
///
/// The rows a builder holds belong to the run that produced them: a run that stopped cannot complete
/// them, so they are dropped rather than handed over as if the series had ended. The stream is done
/// after the error as well - a failed execution answers nothing more.
#[tokio::test]
async fn a_frame_drops_its_buffered_rows_when_its_input_fails() {
    let dropped = Arc::new(AtomicBool::new(false));
    let schema = frame_schema();
    let batches = one_series_frames();
    let source = staged(schema.clone(), {
        let dropped = Arc::clone(&dropped);
        move || {
            Box::pin(
                StagedStream::new(schema.clone(), batches.clone())
                    .error("the source failed")
                    .dropped(&dropped),
            )
        }
    });
    let node = in_format(&fused(rate_tree()), FusedOutputFormat::SeriesFrame);
    let mut stream = node
        .to_execution_plan(source, RawLabelSource::Explicit, SeriesOrdering::Contiguous)
        .expect("the runtime executes a rate tree in the frame format")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    let mut answered = Vec::new();
    let error = loop {
        match stream.next().await {
            Some(Ok(batch)) => answered.push(batch),
            Some(Err(error)) => break error,
            None => panic!("the failing input was answered instead of reported"),
        }
    };
    assert!(
        error.to_string().contains("the source failed"),
        "reported for another reason: {error}"
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "the failed stream still holds its input"
    );
    // Whatever came out before the error is not the answer, and nothing is handed out after it.
    assert!(
        answered.iter().map(RecordBatch::num_rows).sum::<usize>() < GRID_LEN as usize,
        "the half-answered series must not look complete: {:?}",
        frame_table(&decoded_frames(answered).await, &[HOST])
    );
    assert!(
        stream.next().await.is_none(),
        "a failed stream answers nothing more"
    );
}

/// Dropping a frame stream releases its input without a poll.
///
/// The decoder behind the stream holds the batch it is reading and its own source, so a consumer that
/// gives up waiting would hold all of it until the stream is dropped. It is dropped here before
/// anything is polled on purpose: a poll of a frame stream that goes pending wakes itself and drives
/// the stream to its end, which releases the input on its own and would hide whether the drop does.
#[tokio::test]
async fn dropping_a_frame_stream_releases_its_input() {
    let dropped = Arc::new(AtomicBool::new(false));
    let schema = frame_schema();
    let batches = one_series_frames();
    let source = staged(schema.clone(), {
        let dropped = Arc::clone(&dropped);
        move || {
            Box::pin(
                StagedStream::new(schema.clone(), batches.clone())
                    .pending_after(1)
                    .dropped(&dropped),
            )
        }
    });
    let node = in_format(&fused(rate_tree()), FusedOutputFormat::SeriesFrame);
    let stream = node
        .to_execution_plan(source, RawLabelSource::Explicit, SeriesOrdering::Contiguous)
        .expect("the runtime executes a rate tree in the frame format")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");
    assert!(
        !dropped.load(Ordering::SeqCst),
        "the input is open before anything is polled"
    );
    drop(stream);
    assert!(
        dropped.load(Ordering::SeqCst),
        "dropping the stream has to release its input"
    );
}

/// A chain longer than the builder's bounds is split without losing a row.
///
/// The builder's `DEFAULT_MAX_ROWS` bounds how many *rows* one frame batch carries, and a row is one
/// intersection of a grid run with a sample segment - so a dense chunk is one row however many
/// positions it holds, and a chain is split only when it arrives as more row-carrying chunks than
/// that. The chain here is one position per chunk, three bounds long: the batches it encodes to are
/// several, and decoding them back has to give the row table's own answer - every position once, in
/// order, with its own value - and one single `is_last`.
#[tokio::test]
async fn a_frame_splits_a_chain_at_the_builder_bounds_without_losing_rows() {
    let positions = SeriesFrameBuilder::DEFAULT_MAX_ROWS * 2 + 7;
    let grid = Arc::new(
        EvalGrid::try_new(GridSpec::try_new(1_000, 10_000, positions as u64).unwrap())
            .expect("a grid that long is a legal one"),
    );
    let schema = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec { name: VALUE.into() }]),
        eval_grid: Some(Arc::clone(&grid)),
    });
    // One chunk per position, each closing nothing but the last: a chain long enough to cross the
    // builder's row bound twice.
    let chunks = (0..positions)
        .map(|index| {
            let series = Series::try_new(
                &schema,
                metric(&[(HOST, "web-1")], None),
                TimeAxis::from_grid_spans(Arc::clone(&grid), vec![GridSpan::new(index, 1)])
                    .expect("one position of the grid"),
                SeriesFields::one(Samples::from_float(Float64Array::from(vec![index as f64]))),
            )
            .expect("one position and one value are a legal series");
            (series, index + 1 == positions)
        })
        .collect::<Vec<_>>();
    let source: SendableEvaluatedSeriesStream = Box::pin(futures::stream::iter(
        chunks.into_iter().map(Ok::<_, BoxedError>),
    ));
    let batches = pack_evaluated_series(source, Arc::clone(&schema))
        .expect("the chain is a legal evaluated stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("the chain packs");

    assert!(
        batches.len() > 1,
        "a chain past the row bound has to be packed as several batches, not {}",
        batches.len()
    );
    let decoded = decoded_frames(batches).await;
    assert_eq!(
        1,
        decoded.iter().filter(|(_, last)| *last).count(),
        "the chain closes exactly once, in its last batch"
    );
    let rows = frame_table(&decoded, &[HOST]);
    assert_eq!(
        (0..positions as i64)
            .map(|index| 1_000 + index * 10_000)
            .collect::<Vec<_>>(),
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        "every position survives the split, in order and once"
    );
    assert_eq!(
        (0..positions)
            .map(|index| Some(index as f64))
            .collect::<Vec<_>>(),
        rows.iter().map(|row| row.1[0]).collect::<Vec<_>>(),
        "every value stays with its own position"
    );
}

/// A conflict between two chains of one label set is still reported in the frame format.
///
/// Skipping the plan-level claim is safe exactly here, and this is the evidence: two chunks that hold
/// a value of the *same* field at the same position are refused by the runtime itself (`Emitted
/// ::occupy` and the merge it feeds), which is the authority the claim was standing in for. The two
/// chunks are two batches of one partition on purpose - the merge is what sees them - and the
/// refusal names the label set both of them collapsed to.
///
/// `rate` drops `__name__`, `__type__` and every empty-valued label, so the two series below - which
/// differ only in `__type__` - reach the output as one label set `{host="web-1"}`. That collapse is
/// checked against the frames themselves before the refusal is read: the same two chunks in the frame
/// format in *two* partitions are emitted, and the two chains there carry the very same label set.
#[tokio::test]
async fn a_runtime_conflict_is_still_reported_in_the_frame_format() {
    let schema = frame_schema_of(&two_field_schema());
    let chunks = || {
        vec![
            frames_of(
                &two_field_schema(),
                &[one_field_chunk(0, "web-1", "counter", true)],
            ),
            frames_of(
                &two_field_schema(),
                &[one_field_chunk(0, "web-1", "gauge", true)],
            ),
        ]
    };
    let node = in_format(&two_field_node(), FusedOutputFormat::SeriesFrame);

    // What the two chains' label sets are, read off the frames their own partitions answer: `rate`
    // dropped `__type__`, so both chains name one label set with one entry.
    let split = node
        .to_execution_plan(
            memory_exec_partitioned_with(schema.clone(), chunks()),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )
        .expect("the runtime executes a rate tree in the frame format");
    let decoded = decoded_frames(collect_frames(split).await).await;
    assert_eq!(2, decoded.len(), "one chain per partition");
    let label_sets = decoded
        .iter()
        .map(|(series, _)| label_set(&series.metric))
        .collect::<Vec<_>>();
    assert_eq!(
        label_sets[0], label_sets[1],
        "the two chains must end on one label set, or this test proves nothing: {label_sets:?}"
    );
    assert_eq!(
        vec![(Arc::from(HOST), Arc::from("web-1"))],
        label_sets[0],
        "the label set is exactly the one `rate` leaves: {label_sets:?}"
    );

    // Both chunks in one partition, as two batches: one chunk per batch, because two chunks of
    // different series in one batch would be refused by the frame's own sequence validation before
    // the runtime ever saw them.
    let exec = node
        .to_execution_plan(
            memory_exec_partitioned_with(schema, vec![chunks().concat()]),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )
        .expect("the runtime executes a rate tree in the frame format");
    assert_eq!(
        1,
        exec.properties().output_partitioning().partition_count(),
        "the two chunks have to meet inside one execution"
    );
    let error = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect_err("both chains hold a value of one field at one position");
    // The refusal is the runtime's own `Emitted::occupy`: with the plan-level claim gone, what
    // reports the pair is the node that hands the two chunks out.
    let message = error_chain(&error);
    assert!(
        message.contains("two series under the label set"),
        "refused for another reason: {message}"
    );
    assert!(
        message.contains("(\"host\", \"web-1\")"),
        "the label set must be readable: {message}"
    );
    assert!(
        message.contains("field 0 at position 0"),
        "the refusal has to name the field and the position that collide: {message}"
    );
    assert!(
        !message.contains(TYPE) && !message.contains(NAME),
        "the refusal names the label set the window left, not the labels the input arrived with: \
         {message}"
    );
}
