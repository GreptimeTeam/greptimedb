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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common_query::series_frame::unpack_evaluated_series;
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
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_plan::execution_plan::{
    Boundedness, ChildrenPropertiesMode, EmissionType, ReplaceChildrenOptions,
};
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, collect, displayable,
};
use datafusion::prelude::SessionContext;
use datatypes::prelude::ConcreteDataType;
use datatypes::series::frame::{
    FrameContent, FrameLabelMode, LABEL_DECODE_VERSION, LabelDecodeColumn, LabelDecodeEncoding,
    LabelDecodeSpec, LabelDecodeTag, SeriesFrameDeclaration, pack_raw_series_chunk,
    raw_series_frame_schema, series_frame_schema,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, SeriesLabels, SeriesOrdering};
use futures::{Stream, StreamExt, TryStreamExt};
use store_api::region_engine::RawLabelSource;

mod frame_output;

use super::{
    FusedOutputFormat, FusedSeries, FusedSeriesBinding, OutputColumn, POLL_BUDGET,
    series_to_record_batch,
};
use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::aggregate_exchange::state_schema;
use crate::map_label::LabelRewrite;
use crate::native::execute::{InputId, NativeExecutionState, NativeExecutor};
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundVectorOp, BoundWindow, Grouping, InstantValue, MapOp, NativeExpr, NativeNode, NativeTree,
    NativeType,
};
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};
use common_query::series_frame::series_frame_ordering;

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
/// A second label, so two partitions can hold distinct prefix series that fold into one group.
const JOB: &str = "job";
const ZONE: &str = "zone";
const NAME: &str = "__name__";
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

/// A raw schema of two float fields, used to verify a frame with different fields is rejected.
fn two_field_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([
            FieldSpec { name: VALUE.into() },
            FieldSpec { name: OTHER.into() },
        ]),
        eval_grid: None,
    })
}

/// A one-field raw schema with a distinct name for independent-source decoding tests.
fn other_field_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec { name: OTHER.into() }]),
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
                    source_index: 0,
                    ordering: SeriesOrdering::Contiguous,
                }),
                output: raw,
            }),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: RANGE_MS,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
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

/// `left + right` on one grid, both operands reading the one actual source.
fn binary_tree(left_schema: Arc<SeriesSchema>, right_schema: Arc<SeriesSchema>) -> NativeTree {
    binary_tree_over_sources(BinaryOp::Add, left_schema, right_schema, 0, 0)
}

/// A binary tree over `op` whose operands are bound to the given source indexes.
///
/// `left_schema` is the operand at `root.left` and `right_schema` the one at `root.right`, so a
/// subtracting tree answers `left - right`. Equal indexes model two leaves sharing one source;
/// distinct dense indexes model independent physical sources. The renderer names each input index,
/// so trees with otherwise identical operands remain distinct computations.
fn binary_tree_over_sources(
    op: BinaryOp,
    left_schema: Arc<SeriesSchema>,
    right_schema: Arc<SeriesSchema>,
    left_source: usize,
    right_source: usize,
) -> NativeTree {
    let grid = grid();
    let left_eval = Arc::new(SeriesSchema {
        fields: left_schema.fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    });
    let right_eval = Arc::new(SeriesSchema {
        fields: right_schema.fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    });
    let output = crate::vector_match::binary_output_schema(&left_eval, &right_eval).unwrap();
    let raw_left = Arc::clone(&left_schema);
    let raw_right = Arc::clone(&right_schema);
    let left = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index: left_source,
                    output: NativeType::Samples(Arc::clone(&raw_left)),
                    ordering: SeriesOrdering::Contiguous,
                }),
                output: NativeType::Samples(raw_left),
            }),
            op: BoundInstant {
                lookback_ms: 0,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(&grid),
            },
        },
        output: NativeType::Samples(left_eval),
    };
    let right = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index: right_source,
                    output: NativeType::Samples(Arc::clone(&raw_right)),
                    ordering: SeriesOrdering::Contiguous,
                }),
                output: NativeType::Samples(raw_right),
            }),
            op: BoundInstant {
                lookback_ms: 0,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(&grid),
            },
        },
        output: NativeType::Samples(right_eval),
    };
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: BinaryMatching {
                op,
                return_bool: false,
                labels: MatchingLabels::On(vec![Arc::from(HOST)]),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output: NativeType::Samples(output),
    })
    .unwrap()
}

/// The constant scalar `value`, evaluated on `grid`.
///
/// A scalar is the one thing a tree can produce while reading no source, so this is the tree the
/// zero-child case is about.
fn scalar_tree(grid: Arc<EvalGrid>, value: f64) -> NativeTree {
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(grid),
    })
    .expect("a constant scalar is a legal tree")
}

/// An instant selection over the tree input bound to `source_index`.
fn instant_of(source_index: usize, grid: &Arc<EvalGrid>) -> NativeNode {
    let raw = NativeType::Samples(raw_schema());
    NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index,
                    output: raw.clone(),
                    ordering: SeriesOrdering::Contiguous,
                }),
                output: raw,
            }),
            op: BoundInstant {
                lookback_ms: 0,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(grid),
            },
        },
        output: evaluated_of(&raw_schema(), grid),
    }
}

/// `raw_schema`'s fields on `grid`: one value per grid point.
fn evaluated_of(schema: &SeriesSchema, grid: &Arc<EvalGrid>) -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: schema.fields.clone(),
        eval_grid: Some(Arc::clone(grid)),
    }))
}

/// `tree` with the orderings its input nodes declare replaced, in the order the nodes are reached.
///
/// The fixtures here declare `Contiguous`, which is what the plans below them present, so a test
/// that needs another declaration rewrites it on a clone rather than keeping a second tree builder
/// beside the first. The rewrite is revalidated by [`NativeTree::try_new`], so one that broke a
/// contract of the tree is refused here instead of at the node under test.
fn declaring_orderings(tree: &NativeTree, orderings: &[SeriesOrdering]) -> NativeTree {
    fn rewrite(node: &NativeNode, orderings: &[SeriesOrdering], seen: &mut usize) -> NativeNode {
        let mut expr = node.expr.clone();
        match &mut expr {
            NativeExpr::Input(bound) => {
                bound.ordering = orderings[*seen];
                *seen += 1;
            }
            // A constant reads no source, so it declares no ordering.
            NativeExpr::Scalar(_) => {}
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. } => {
                **input = rewrite(input, orderings, seen);
            }
            NativeExpr::Binary { left, right, .. }
            | NativeExpr::Set { left, right, .. }
            | NativeExpr::Info {
                base: left,
                info: right,
                ..
            } => {
                **left = rewrite(left, orderings, seen);
                **right = rewrite(right, orderings, seen);
            }
        }
        NativeNode {
            expr,
            output: node.output.clone(),
        }
    }
    let mut seen = 0;
    let tree = NativeTree::try_new(rewrite(tree.root(), orderings, &mut seen))
        .expect("another declared ordering keeps the tree legal");
    assert_eq!(orderings.len(), seen, "one ordering per input of the tree");
    tree
}

/// `x - 2 * x`: one source read at both operands, with a scalar multiplier on the right.
///
/// The subtraction and the multiplier together make the answer depend on which operand is which:
/// this tree answers `-x` at every grid point, and the same operands paired the other way round
/// answer `+x`. `x - x` would answer zero either way and could not tell the two apart.
fn subtracted_scaled_tree() -> NativeTree {
    let grid = grid();
    let output = evaluated_of(&raw_schema(), &grid);
    let doubled = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(instant_of(0, &grid)),
            right: Box::new(NativeNode {
                expr: NativeExpr::Scalar(BoundScalar { value: 2.0 }),
                output: NativeType::Scalar(Arc::clone(&grid)),
            }),
            matching: BinaryMatching {
                op: BinaryOp::Mul,
                return_bool: false,
                // What a scalar broadcast binds: the modifiers of a vector-vector match have no
                // meaning here, and the runtime refuses a constant carrying them.
                labels: MatchingLabels::Ignoring(Vec::new()),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output: output.clone(),
    };
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(instant_of(0, &grid)),
            right: Box::new(doubled),
            matching: BinaryMatching {
                op: BinaryOp::Sub,
                return_bool: false,
                labels: MatchingLabels::On(vec![Arc::from(HOST)]),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output,
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

/// The `rate(x[5m])` tree with a `topk(k, ...)` over it.
///
/// A vector operation over the whole vector: `k` ranks a group's series against each other, so the
/// operator over part of the metric keeps each part's `k` instead of the `k` of the whole.
fn topk_tree(k: f64) -> NativeTree {
    let window = rate_tree().root().clone();
    let output = window.output.clone();
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(window),
            op: BoundVectorOp::TopK {
                k,
                top: true,
                // No grouping, so every series is one group and the kept series are the largest of
                // the whole vector.
                group: Grouping::By(Arc::new([])),
            },
        },
        output,
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

/// What the tree's input was compiled against, for the input schema and output tags given.
fn binding_of(
    tree: &NativeTree,
    _series_schema: Arc<SeriesSchema>,
    tags: Vec<String>,
) -> FusedSeriesBinding {
    FusedSeriesBinding {
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

fn canonical_frame_schema(tree: &NativeTree) -> datafusion::common::DFSchemaRef {
    let evaluated = super::evaluated_output(tree).unwrap();
    let schema = super::series_frame_output_schema(evaluated.as_ref()).unwrap();
    Arc::new(datafusion::common::DFSchema::try_from(schema).unwrap())
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

/// A one-to-one `on(host)` binary output schema.
fn binary_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
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
        vec![input_plan(frame_schema())],
        zone_output_schema().to_dfschema_ref().unwrap(),
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

fn input_plan(schema: SchemaRef) -> LogicalPlan {
    input_plan_with_rows(schema, false)
}

fn input_plan_with_rows(schema: SchemaRef, produce_one_row: bool) -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row,
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

/// A task context whose memory pool is `pool`, so the execution's reservations can be observed.
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

/// Counts calls per partition while preserving an existing physical input unchanged.
struct CountedExec {
    input: Arc<dyn ExecutionPlan>,
    calls: Arc<Vec<AtomicUsize>>,
}

impl std::fmt::Debug for CountedExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountedExec").finish()
    }
}

impl DisplayAs for CountedExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        self.input.fmt_as(t, f)
    }
}

impl ExecutionPlan for CountedExec {
    fn name(&self) -> &str {
        "CountedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        self.input.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "CountedExec requires one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children.remove(0),
            calls: Arc::clone(&self.calls),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.calls[partition].fetch_add(1, Ordering::SeqCst);
        self.input.execute(partition, context)
    }
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
    let output_schema = if matches!(&tree.root().expr, NativeExpr::Binary { .. }) {
        binary_output_schema()
    } else {
        output_schema()
    };
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        vec![input_plan(frame_schema())],
        output_schema.to_dfschema_ref().unwrap(),
    )
    .expect("the binding matches the tree")
}

/// The physical node of `node` over `input`, reading the complete labels every frame carries.
fn plan_of(
    node: FusedSeries,
    input: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    node.to_execution_plan(vec![(
        input,
        RawLabelSource::Explicit,
        SeriesOrdering::Contiguous,
    )])
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

/// One frame with a different raw field layout, used to verify source-local decoding.
fn other_field_chunk(host: &str, value: f64) -> RecordBatch {
    let schema = other_field_schema();
    let chunk = chunk_of(
        &schema,
        host,
        metric(&[(HOST, host)], Some(METRIC)),
        &[GRID_START],
        vec![vec![Some(value)]],
        true,
    );
    frame_of(&schema, &chunk)
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

fn binary_values(batches: &[RecordBatch]) -> Vec<f64> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(VALUE)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            (0..values.len())
                .filter(|&row| values.is_valid(row))
                .map(|row| values.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
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

/// The canonical label set these tests collide on, as the runtime renders it in its refusal.
const DUPLICATE_LABEL_SET: &str =
    "the node produces two series under the label set [(\"host\", \"web-1\")]";

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

/// A collision produced inside one native execution is still refused by its local runtime state.
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
        NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default()))
            .expect("the runtime executes a rate tree"),
    );
    let samples = series_samples(1.0);
    execution
        .push_raw(
            InputId::new(0),
            chunk_of(
                &raw_schema(),
                METRIC,
                metric(&[(HOST, "web-1")], Some(METRIC)),
                &times_of(&samples),
                vec![values_of(&samples)],
                true,
            ),
        )
        .expect("the first series is a legal raw chunk");
    assert!(
        execution
            .next_chunk()
            .expect("the first series runs")
            .is_some(),
        "the first series is answered"
    );
    execution
        .push_raw(
            InputId::new(0),
            chunk_of(
                &raw_schema(),
                "http_errors_total",
                metric(&[(HOST, "web-1")], Some("http_errors_total")),
                &times_of(&samples),
                vec![values_of(&samples)],
                true,
            ),
        )
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
        vec![input_plan(frame_schema())],
        output_schema().to_dfschema_ref().unwrap(),
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
        vec![input_plan(frame_schema())],
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("an equal grid spec is the same grid");
}

/// A scalar tree is a legal logical node with no input plans, while a mismatched child is not.
#[test]
fn scalar_logical_node_has_no_children_and_refuses_a_child() {
    let grid = grid();
    let tree = scalar_tree(Arc::clone(&grid), 7.0);
    let binding = FusedSeriesBinding {
        grid,
        metric_name: None,
        time_index: TIME.to_string(),
        tags: vec![HOST.to_string()],
    };
    let node = FusedSeries::try_new(
        Arc::from("vector(7)"),
        tree.clone(),
        binding.clone(),
        Vec::new(),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("a scalar tree reads no children");
    assert!(node.children().is_empty());
    assert!(node.inputs().is_empty());
    assert!(
        FusedSeries::try_new(
            Arc::from("vector(7)"),
            tree,
            binding,
            vec![input_plan(frame_schema())],
            output_schema().to_dfschema_ref().unwrap(),
        )
        .is_err(),
        "a scalar tree cannot be bound to a child"
    );
}

/// Logical children stay in source-index order through optimizer replacement.
#[test]
fn logical_children_and_inputs_roundtrip_in_source_order() {
    let tree = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let left = input_plan_with_rows(frame_schema(), false);
    let right = input_plan_with_rows(frame_schema(), true);
    let node = FusedSeries::try_new(
        Arc::from("left + right"),
        tree,
        binding(&rate_tree()),
        vec![left.clone(), right.clone()],
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("two dense source indexes bind to two plans");
    assert_eq!(2, node.children().len());
    assert_eq!(2, node.inputs().len());
    assert!(matches!(
        node.children()[0],
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            ..
        })
    ));
    assert!(matches!(
        node.children()[1],
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            ..
        })
    ));
    let rebuilt = node
        .with_exprs_and_inputs(Vec::<Expr>::new(), vec![right.clone(), left.clone()])
        .expect("optimizer replacement preserves the replacement source order");
    assert!(matches!(
        rebuilt.children()[0],
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            ..
        })
    ));
    assert!(matches!(
        rebuilt.children()[1],
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            ..
        })
    ));
    assert!(matches!(
        rebuilt.inputs()[0],
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            ..
        })
    ));
}

/// Invalid logical source layouts fail during construction, including a huge sparse index.
#[test]
fn logical_source_indexes_must_match_dense_children() {
    let child = input_plan(frame_schema());
    for (name, tree, children) in [
        ("wrong child count", rate_tree(), Vec::new()),
        (
            "gap",
            binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 2),
            vec![child.clone(), child.clone(), child.clone()],
        ),
        (
            "huge source index",
            binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, usize::MAX),
            vec![child.clone(); 2],
        ),
    ] {
        assert!(
            FusedSeries::try_new(
                Arc::from(name),
                tree.clone(),
                binding(&tree),
                children,
                output_schema().to_dfschema_ref().unwrap(),
            )
            .is_err(),
            "{name} must be refused without allocating by source index"
        );
    }
}

/// Repeated bindings to one source must agree on fields, and on producer ordering.
#[test]
fn repeated_source_bindings_validate_fields_and_second_ordering() {
    let fields_mismatch =
        binary_tree_over_sources(BinaryOp::Add, raw_schema(), two_field_schema(), 0, 0);
    assert!(
        FusedSeries::try_new(
            Arc::from("x + x"),
            fields_mismatch.clone(),
            binding(&fields_mismatch),
            vec![input_plan(frame_schema())],
            output_schema().to_dfschema_ref().unwrap(),
        )
        .is_err(),
        "one source cannot declare incompatible field lists"
    );

    let tree = binary_tree(raw_schema(), raw_schema());
    let second_ordering = declaring_orderings(
        &tree,
        &[SeriesOrdering::Contiguous, SeriesOrdering::KeyThenTime],
    );
    let node = fused(second_ordering);
    let error = node
        .to_execution_plan(vec![(
            memory_exec(Vec::new()),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .expect_err("the second repeated leaf's incompatible ordering is rejected");
    assert!(error.to_string().contains("ordering"), "{error}");
}

/// Logical trees with distinct dense sources are accepted by the physical adapter.
#[tokio::test]
async fn logical_two_source_tree_is_accepted_by_physical_adapter() {
    let tree = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let node = FusedSeries::try_new(
        Arc::from("left + right"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the logical node accepts two dense sources");
    let source = || {
        Arc::new(CountedExec {
            input: memory_exec(Vec::new()),
            calls: Arc::new((0..1).map(|_| AtomicUsize::new(0)).collect()),
        }) as Arc<dyn ExecutionPlan>
    };
    let plan = node
        .to_execution_plan(vec![
            (
                source(),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
            (
                source(),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
        ])
        .expect("the physical boundary accepts independent source bindings");
    assert_eq!(2, plan.children().len());
    let results = collect(Arc::clone(&plan), SessionContext::default().task_ctx())
        .await
        .unwrap();
    assert!(rows(&results).is_empty());
    for child in plan.children() {
        let counted = child
            .downcast_ref::<CountedExec>()
            .expect("child preserves its counted execution plan");
        assert_eq!(1, counted.calls[0].load(Ordering::SeqCst));
    }
}

/// Independent sources gather all child partitions, without zipping.
#[tokio::test]
async fn independent_sources_execute_all_partitions() {
    let tree = binary_tree_over_sources(BinaryOp::Sub, raw_schema(), other_field_schema(), 0, 1);
    let left_calls = Arc::new((0..2).map(|_| AtomicUsize::new(0)).collect());
    let right_calls = Arc::new((0..3).map(|_| AtomicUsize::new(0)).collect());
    let left = Arc::new(CountedExec {
        input: memory_exec_partitioned(vec![
            vec![frame(&series_chunk("web-2", &[(GRID_START, 8.0)], true))],
            vec![frame(&series_chunk("web-1", &[(GRID_START, 9.0)], true))],
        ]),
        calls: Arc::clone(&left_calls),
    });
    let right = Arc::new(CountedExec {
        input: memory_exec_partitioned_with(
            frame_schema_of(&other_field_schema()),
            vec![
                vec![other_field_chunk("web-2", 3.0)],
                vec![other_field_chunk("web-1", 2.0)],
                vec![other_field_chunk("web-3", 4.0)],
            ],
        ),
        calls: Arc::clone(&right_calls),
    });
    let node = FusedSeries::try_new(
        Arc::from("left - right"),
        tree.clone(),
        binding(&tree),
        vec![
            input_plan(frame_schema()),
            input_plan(frame_schema_of(&other_field_schema())),
        ],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let exec = node
        .to_execution_plan(vec![
            (left, RawLabelSource::Explicit, SeriesOrdering::Contiguous),
            (right, RawLabelSource::Explicit, SeriesOrdering::Contiguous),
        ])
        .unwrap();
    assert_eq!(1, exec.properties().output_partitioning().partition_count());
    let results = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .unwrap();
    let mut actual = binary_values(&results);
    actual.sort_by(f64::total_cmp);
    assert_eq!(vec![5.0, 7.0], actual);
    assert_eq!(
        vec![1, 1],
        left_calls
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        vec![1, 1, 1],
        right_calls
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .collect::<Vec<_>>()
    );
}

/// One producer feeding both inputs, as a region's two scans for one query are fed, stalls a node
/// that drains one input before it polls the other: the producer waits on the unread input.
#[tokio::test]
async fn both_inputs_are_read_concurrently() {
    const SERIES: usize = 64;
    let tree = binary_tree_over_sources(BinaryOp::Sub, raw_schema(), raw_schema(), 0, 1);
    let (left_tx, left_rx) = tokio::sync::mpsc::channel(1);
    let (right_tx, right_rx) = tokio::sync::mpsc::channel(1);
    let fed = |receiver: tokio::sync::mpsc::Receiver<RecordBatch>| {
        let receiver = Mutex::new(Some(receiver));
        staged(frame_schema(), move || {
            let receiver = receiver.lock().unwrap().take().expect("executed once");
            Box::pin(RecordBatchStreamAdapter::new(
                frame_schema(),
                futures::stream::unfold(receiver, |mut receiver| async move {
                    receiver.recv().await.map(|batch| (Ok(batch), receiver))
                }),
            ))
        })
    };
    let producer = tokio::spawn(async move {
        for series in 0..SERIES {
            let host = format!("web-{series}");
            left_tx
                .send(frame(&series_chunk(&host, &[(GRID_START, 10.0)], true)))
                .await
                .unwrap();
            right_tx
                .send(frame(&series_chunk(&host, &[(GRID_START, 3.0)], true)))
                .await
                .unwrap();
        }
    });
    let node = FusedSeries::try_new(
        Arc::from("left - right"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let exec = node
        .to_execution_plan(vec![
            (
                fed(left_rx),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
            (
                fed(right_rx),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
        ])
        .unwrap();
    let results = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        collect(exec, SessionContext::default().task_ctx()),
    )
    .await
    .expect("the node stalled on an input it did not poll")
    .unwrap();
    producer.await.unwrap();
    assert_eq!(vec![7.0; SERIES], binary_values(&results));
}

/// On-label matching projects binary output tags while retaining all raw tags for frame decoding.
#[tokio::test]
async fn binary_output_tags_are_projected_but_raw_binding_is_full() {
    let tree = binary_tree_over_sources(BinaryOp::Sub, raw_schema(), raw_schema(), 0, 1);
    let node = FusedSeries::try_new(
        Arc::from("left - right"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let left = memory_exec(vec![frame(&chunk_of(
        &raw_schema(),
        "web-1",
        metric(&[(HOST, "web-1"), (ZONE, "west")], Some(METRIC)),
        &[GRID_START],
        vec![vec![Some(9.0)]],
        true,
    ))]);
    let right = memory_exec(vec![frame(&chunk_of(
        &raw_schema(),
        "web-1",
        metric(&[(HOST, "web-1"), (ZONE, "east")], Some(METRIC)),
        &[GRID_START],
        vec![vec![Some(2.0)]],
        true,
    ))]);
    let exec = node
        .to_execution_plan(vec![
            (left, RawLabelSource::Explicit, SeriesOrdering::Contiguous),
            (right, RawLabelSource::Explicit, SeriesOrdering::Contiguous),
        ])
        .unwrap();
    let results = collect(exec, SessionContext::default().task_ctx())
        .await
        .unwrap();
    assert_eq!(vec![7.0], binary_values(&results));
    assert!(
        results
            .iter()
            .all(|batch| batch.column_by_name(ZONE).is_none())
    );
}

/// Empty earlier sources do not stop later sources, and one-to-one matching can yield no rows.
#[tokio::test]
async fn empty_first_source_does_not_end_gather_early() {
    let tree = binary_tree_over_sources(BinaryOp::Sub, raw_schema(), raw_schema(), 0, 1);
    let node = FusedSeries::try_new(
        Arc::from("left - right"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let second = memory_exec(vec![frame(&series_chunk(
        "web-1",
        &[(GRID_START, 2.0)],
        true,
    ))]);
    let exec = node
        .to_execution_plan(vec![
            (
                memory_exec(Vec::new()),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
            (second, RawLabelSource::Explicit, SeriesOrdering::Contiguous),
        ])
        .unwrap();
    let results = collect(exec, SessionContext::default().task_ctx())
        .await
        .unwrap();
    assert!(rows(&results).is_empty());
}

/// A multiparty gather requires Contiguous input, not global KeyThenTime ordering.
#[test]
fn multiparty_gather_rejects_key_then_time_leaf_requirement() {
    let tree = declaring_orderings(
        &binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1),
        &[SeriesOrdering::KeyThenTime, SeriesOrdering::Contiguous],
    );
    let node = FusedSeries::try_new(
        Arc::from("left + right"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let error = node
        .to_execution_plan(vec![
            (
                memory_exec_partitioned(vec![vec![], vec![]]),
                RawLabelSource::Explicit,
                SeriesOrdering::KeyThenTime,
            ),
            (
                memory_exec_partitioned(vec![vec![], vec![]]),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
        ])
        .expect_err("concatenated partitions are not globally KeyThenTime");
    assert!(error.to_string().contains("ordering"), "{error}");
}

/// A later source failure drops streams already opened for earlier sources and releases state.
#[tokio::test]
async fn later_source_error_drops_all_gathered_inputs() {
    let tree = binary_tree_over_sources(BinaryOp::Sub, raw_schema(), raw_schema(), 0, 1);
    let node = FusedSeries::try_new(
        Arc::from("left - right"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let dropped = [
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    ];
    let left_schema = frame_schema();
    let left_drop = Arc::clone(&dropped[0]);
    let left = staged(left_schema.clone(), move || {
        Box::pin(
            StagedStream::new(
                left_schema.clone(),
                vec![frame(&series_chunk("web-1", &[(GRID_START, 9.0)], true))],
            )
            .dropped(&left_drop),
        )
    });
    let right_schema = frame_schema();
    let right_drop = Arc::clone(&dropped[1]);
    let right = staged(right_schema.clone(), move || {
        Box::pin(
            StagedStream::new(right_schema.clone(), Vec::new())
                .error("later source failed")
                .dropped(&right_drop),
        )
    });
    let exec = node
        .to_execution_plan(vec![
            (left, RawLabelSource::Explicit, SeriesOrdering::Contiguous),
            (right, RawLabelSource::Explicit, SeriesOrdering::Contiguous),
        ])
        .unwrap();
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let mut stream = exec.execute(0, task_context(&pool)).unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(error.to_string().contains("later source failed"));
    // Each partition is read on a spawned task that the failure only aborts, so its stream and the
    // chunk it holds are dropped once its runtime cancels it, after the error is out.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !dropped.iter().all(|flag| flag.load(Ordering::SeqCst)) || pool.reserved() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the failure left an input stream or its charge alive");
}

/// A hand-bound merge canonicalizes the observable output values from distinct raw keys that map
/// to one rewritten label set.
#[tokio::test]
async fn hand_bound_fused_merge_canonicalizes_shared_labels() {
    let grid = grid();
    let schema = Arc::new(SeriesSchema {
        fields: raw_schema().fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    });
    let evaluated = NativeType::Samples(Arc::clone(&schema));
    let raw = NativeType::Samples(raw_schema());
    let input = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: raw.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Contiguous,
        }),
        output: raw,
    };
    let window = NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(input),
            op: BoundWindow {
                function: RangeFunction::LastOverTime,
                window: RangeWindow {
                    range_ms: 1,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: None,
                output_grid: Arc::clone(&grid),
            },
        },
        output: evaluated.clone(),
    };
    let map = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(window),
            op: BoundMap {
                ops: Arc::from([MapOp::Label(Arc::new(
                    LabelRewrite::replace("host", "web-1", "host", "web-.*").unwrap(),
                ))]),
            },
        },
        output: evaluated.clone(),
    };
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(map),
        },
        output: evaluated,
    })
    .unwrap();
    let points = grid_timestamps();
    let exec = plan(
        tree,
        memory_exec(frames(&[
            series_chunk("web-1", &[(points[0], 1.0), (points[2], 3.0)], true),
            series_chunk("web-2", &[(points[1], 2.0), (points[3], 4.0)], true),
        ])),
    )
    .unwrap();
    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .unwrap();
    assert_eq!(
        vec![
            (
                points[0],
                Some(1.0),
                Some("web-1".to_string()),
                Some(METRIC.to_string())
            ),
            (
                points[1],
                Some(2.0),
                Some("web-1".to_string()),
                Some(METRIC.to_string())
            ),
            (
                points[2],
                Some(3.0),
                Some("web-1".to_string()),
                Some(METRIC.to_string())
            ),
            (
                points[3],
                Some(4.0),
                Some("web-1".to_string()),
                Some(METRIC.to_string())
            ),
        ],
        rows(&batches),
        "Merge must combine complementary mapped window outputs under one canonical label set"
    );
}

/// A scalar root needs no source child and can be evaluated directly on its bound grid.
#[tokio::test]
async fn physical_plan_accepts_scalar_root_without_sources() {
    let statement_grid = grid();
    let tree = scalar_tree(Arc::clone(&statement_grid), 4.0);
    let binding = FusedSeriesBinding {
        grid: statement_grid,
        metric_name: None,
        time_index: TIME.to_string(),
        tags: vec![],
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let node = FusedSeries::try_new(
        Arc::from("4"),
        tree,
        binding.clone(),
        Vec::new(),
        schema.clone().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let exec = node.to_execution_plan(Vec::new()).unwrap();
    assert_eq!(1, exec.properties().output_partitioning().partition_count());
    let rebuilt = exec
        .clone()
        .replace_children(
            Vec::new(),
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .unwrap();
    assert!(matches!(
        exec.properties().boundedness,
        Boundedness::Bounded
    ));
    assert!(exec.children().is_empty());
    assert!(rebuilt.children().is_empty());
    let zero_pool = Arc::new(GreedyMemoryPool::new(0));
    let mut rejected = exec.execute(0, task_context(&zero_pool)).unwrap();
    assert!(rejected.next().await.unwrap().is_err());
    drop(rejected);
    assert_eq!(0, zero_pool.reserved());
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let mut stream = exec.execute(0, task_context(&pool)).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
    drop(stream);
    let batches = [first];
    assert_eq!(1, batches.len());
    assert_eq!(schema.as_ref(), batches[0].schema().as_ref());
    assert_eq!(GRID_LEN as usize, batches[0].num_rows());
    let timestamps = batches[0]
        .column_by_name(TIME)
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(grid_timestamps(), timestamps.values().as_ref());
    let values = batches[0]
        .column_by_name(VALUE)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(values.iter().all(|value| value == Some(4.0)));
    assert_eq!(0, pool.reserved());
    let rebuilt_pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let rebuilt_batches = rebuilt
        .execute(0, task_context(&rebuilt_pool))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(1, rebuilt_batches.len());
    assert_eq!(batches[0].schema(), rebuilt_batches[0].schema());
    assert_eq!(batches[0].columns(), rebuilt_batches[0].columns());
    assert_eq!(0, rebuilt_pool.reserved());
    let drop_pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let unpolled = rebuilt.execute(0, task_context(&drop_pool)).unwrap();
    drop(unpolled);
    assert_eq!(0, drop_pool.reserved());
    let mut early = rebuilt.execute(0, task_context(&drop_pool)).unwrap();
    let early_batch = early.next().await.unwrap().unwrap();
    drop(early);
    assert_eq!(0, drop_pool.reserved());
    drop(early_batch);
    assert_eq!(0, drop_pool.reserved());
    assert!(exec.execute(1, task_context(&pool)).is_err());
    let metrics = exec.metrics().unwrap();
    assert_eq!(1, count(&metrics, "native.root.output_batches"));
    assert_eq!(
        GRID_LEN as usize,
        count(&metrics, "native.root.output_values")
    );
    assert_eq!(
        GRID_LEN as usize,
        count(&metrics, "native.root.evaluated_grid_points")
    );
    assert_eq!(1, count(&metrics, "native.root.completed_series"));
    assert_eq!(0, count(&metrics, "native.root.input_batches"));
    assert_eq!(0, count(&metrics, "native.root.input_samples"));
    let partial_error = FusedSeries::try_new_partial(
        Arc::from("4"),
        scalar_tree(grid(), 4.0),
        binding,
        vec![],
        partial_op(AggregateKind::Sum, &[]),
    )
    .expect_err("scalar roots cannot be partial folds");
    assert!(partial_error.to_string().contains("one raw child"));
}

#[tokio::test]
async fn physical_scalar_rejects_malformed_output_schema() {
    let tree = scalar_tree(grid(), 4.0);
    let binding = FusedSeriesBinding {
        grid: grid(),
        metric_name: None,
        time_index: TIME.to_string(),
        tags: vec![],
    };
    let schemas = [
        Schema::new(vec![Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]),
        Schema::new(vec![Field::new(VALUE, DataType::Float64, true)]),
        Schema::new(vec![
            Field::new(VALUE, DataType::Float64, true),
            Field::new(OTHER, DataType::Float64, true),
            Field::new(
                TIME,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]),
        Schema::new(vec![
            Field::new(VALUE, DataType::Utf8, true),
            Field::new(
                TIME,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]),
        Schema::new(vec![
            Field::new(VALUE, DataType::Float64, true),
            Field::new(TIME, DataType::Float64, false),
        ]),
    ];
    for schema in schemas {
        let node = FusedSeries::try_new(
            Arc::from("4"),
            tree.clone(),
            binding.clone(),
            Vec::new(),
            Arc::new(schema).to_dfschema_ref().unwrap(),
        )
        .unwrap();
        assert!(node.to_execution_plan(Vec::new()).is_err());
    }
    let tagged = FusedSeries::try_new(
        Arc::from("4"),
        tree,
        FusedSeriesBinding {
            tags: vec![HOST.to_string()],
            ..binding
        },
        Vec::new(),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    assert!(tagged.to_execution_plan(Vec::new()).is_err());
}

#[tokio::test]
async fn physical_scalar_conversion_preserves_special_float_bits() {
    for value in [f64::NAN, -0.0, f64::INFINITY, f64::NEG_INFINITY] {
        let statement_grid = grid();
        let tree = scalar_tree(Arc::clone(&statement_grid), value);
        let binding = FusedSeriesBinding {
            grid: statement_grid,
            metric_name: None,
            time_index: TIME.to_string(),
            tags: vec![],
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE, DataType::Float64, true),
            Field::new(
                TIME,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let node = FusedSeries::try_new(
            Arc::from("4"),
            tree,
            binding,
            Vec::new(),
            schema.to_dfschema_ref().unwrap(),
        )
        .unwrap();
        let exec = node.to_execution_plan(Vec::new()).unwrap();
        let batches = exec
            .execute(0, SessionContext::default().task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(1, batches.len());
        assert_eq!(GRID_LEN as usize, batches[0].num_rows());
        let actual = batches[0]
            .column_by_name(VALUE)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            actual
                .iter()
                .all(|actual| actual.unwrap().to_bits() == value.to_bits())
        );
    }
}

#[test]
fn physical_plan_requires_matching_source_count() {
    let node = fused(rate_tree());
    let zero = node
        .to_execution_plan(Vec::new())
        .expect_err("a raw-series tree cannot run with zero sources");
    assert!(zero.to_string().contains("count") || zero.to_string().contains("source"));

    let source = memory_exec(Vec::new());
    let tuple = || {
        (
            Arc::clone(&source),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )
    };
    let many = node
        .to_execution_plan(vec![tuple(), tuple()])
        .expect_err("physical input count differs from the tree's source count");
    assert!(many.to_string().contains("source") || many.to_string().contains("input"));
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
        error.to_string().contains("input 0 has no partitions"),
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

/// Each partition runs its own series and answers it independently.
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

/// The rows of the series `host` rates `rate` at, in time order.
fn one_series(rows: &[Row], rate: f64, host: &str) -> Vec<Row> {
    let mut series = rows
        .iter()
        .filter(|row| row.2.as_deref() == Some(host))
        .filter(|row| row.1.is_some_and(|value| (value - rate).abs() < 1e-9))
        .cloned()
        .collect::<Vec<_>>();
    series.sort_by_key(|row| row.0);
    series
}

/// A tree that reads the whole vector runs once over every scan partition, and that one execution
/// sees the series of both.
///
/// `topk(k, rate(x[5m]))` ranks series against each other, which is a decision over the whole
/// vector: run per partition it keeps each partition's `k`, so `topk(1, ...)` would answer `web-0`
/// as well. The two series live in different partitions and are told apart by their rate, so
/// `topk(1, ...)` names the series only partition 1 holds, and `topk(2, ...)` names both.
#[tokio::test]
async fn a_whole_vector_tree_runs_once_over_every_partition() {
    let partitions = [1.0, 5.0]
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

    // The same split, read by a per-series tree, keeps one execution per partition.
    let per_series = plan(rate_tree(), memory_exec_partitioned(partitions.clone()))
        .expect("the runtime executes a rate tree");
    assert_eq!(
        2,
        per_series
            .properties()
            .output_partitioning()
            .partition_count(),
        "a per-series tree answers each partition's series on its own"
    );

    // `topk(2, ...)` keeps both series, which are each in a partition of their own: the one
    // execution the node reports is the only thing that could have gathered them together, since a
    // per-partition run would answer one series per partition in two partitions.
    let both = plan(topk_tree(2.0), memory_exec_partitioned(partitions.clone()))
        .expect("the runtime executes a topk tree");
    assert_eq!(
        1,
        both.properties().output_partitioning().partition_count(),
        "topk reads every series, so one execution answers it"
    );
    let batches = collect(both, SessionContext::default().task_ctx())
        .await
        .expect("the single execution runs");
    let gathered = rows(&batches);
    // The rates are distinct, so the two series are told apart by value and not only by name.
    assert_series_rates(&one_series(&gathered, 1.0, "web-0"), &[(1.0, "web-0")]);
    assert_series_rates(&one_series(&gathered, 5.0, "web-1"), &[(5.0, "web-1")]);

    // `topk(1, ...)` keeps the larger of the two, which lives in partition 1: answering `web-1`
    // means the comparison saw partition 0's `web-0` and rejected it.
    let top = plan(topk_tree(1.0), memory_exec_partitioned(partitions))
        .expect("the runtime executes a topk tree");
    let batches = collect(top, SessionContext::default().task_ctx())
        .await
        .expect("the single execution runs");
    let rows = rows(&batches);
    assert_eq!(
        GRID_LEN as usize,
        rows.len(),
        "one kept series over the whole grid: {rows:?}"
    );
    assert_series_rates(&rows, &[(5.0, "web-1")]);
}
/// One repeated source, read once inside one execution, still applies its operands in order.
///
/// Both leaves of the tree bind the same source and the outer node is an `on(host)` one-to-one
/// match, so the node is placed in a single execution that reads all four scan partitions (the
/// reason is pinned by `a_one_to_one_match_over_one_repeated_source_runs_in_one_execution` below).
/// The source is nevertheless read once and not once per recipient: the runtime decodes each child
/// partition's frames once and hands them to both operands, so every child partition is executed
/// exactly once, by the one execution that reads them all.
///
/// The arithmetic is what proves the operands kept their order: this tree answers `x - 2 * x`, which
/// is `-x` and not `+x`. The fixture gives each series distinct values, so no row of another series
/// can pass for one of these.
#[tokio::test]
async fn a_repeated_source_runs_once_and_keeps_its_operand_order() {
    let partitions = (0..4)
        .map(|partition| {
            let samples = (0..GRID_LEN as i64)
                .map(|index| {
                    let time = GRID_START + index * GRID_STEP;
                    (time, partition as f64 * 10.0 + index as f64 + 1.0)
                })
                .collect::<Vec<_>>();
            vec![frame(&series_chunk(
                &format!("web-{partition}"),
                &samples,
                true,
            ))]
        })
        .collect::<Vec<_>>();
    let input = memory_exec_partitioned(partitions);
    let calls_by_partition = Arc::new((0..4).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let counted = Arc::new(CountedExec {
        input,
        calls: Arc::clone(&calls_by_partition),
    });
    let exec = plan(subtracted_scaled_tree(), counted)
        .expect("the repeated source tree runs over one physical child");
    assert_eq!(
        1,
        exec.properties().output_partitioning().partition_count(),
        "the match over the repeated source reads every series in one execution"
    );
    assert_eq!(1, exec.children().len());

    // The one execution, which has to read all four child partitions.
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let mut stream = exec
        .execute(0, context)
        .expect("the single execution exists");
    let batches = stream
        .as_mut()
        .try_collect::<Vec<_>>()
        .await
        .expect("the execution completes without errors");
    // Each row's timestamp, value and host, in the order the execution emitted them.
    let actual = batches
        .iter()
        .flat_map(|batch| {
            let time = batch
                .column_by_name(TIME)
                .expect("binary output has time")
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("time output is millisecond timestamp");
            let value = batch
                .column_by_name(VALUE)
                .expect("binary output has value")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("value output is Float64");
            let host = batch
                .column_by_name(HOST)
                .expect("binary output retains host")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("host output is Utf8");
            (0..batch.num_rows())
                .map(|row| {
                    (
                        time.value(row),
                        value.is_valid(row).then(|| value.value(row)),
                        host.is_valid(row).then(|| host.value(row).to_string()),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        4 * GRID_LEN as usize,
        actual.len(),
        "every partition's series comes back over the whole grid: {actual:?}"
    );

    // The four series keep their own hosts: a collapse that dropped or duplicated a partition's
    // series would not answer all four.
    let mut hosts = actual
        .iter()
        .filter_map(|row| row.2.clone())
        .collect::<Vec<_>>();
    hosts.sort();
    hosts.dedup();
    assert_eq!(
        vec!["web-0", "web-1", "web-2", "web-3"],
        hosts,
        "the four series keep their own hosts"
    );

    let expected_times = grid_timestamps();
    for partition in 0..4usize {
        let host = format!("web-{partition}");
        // This series' rows in emission order: one series' rows come out in grid order.
        let series = actual
            .iter()
            .filter(|row| row.2.as_deref() == Some(host.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(GRID_LEN as usize, series.len(), "{host}: {series:?}");
        assert_eq!(
            expected_times,
            series.iter().map(|row| row.0).collect::<Vec<_>>(),
            "{host} timestamps preserve grid order"
        );
        // `x - 2 * x` is `-x`. Read the other way round the same operands answer `+x`, so a swapped
        // operand shows up here as the wrong sign and nowhere else.
        let expected_values = (0..GRID_LEN as usize)
            .map(|index| Some(-((partition * 10 + index + 1) as f64)))
            .collect::<Vec<_>>();
        assert_eq!(
            expected_values,
            series.iter().map(|row| row.1).collect::<Vec<_>>(),
            "{host} answers x - 2 * x"
        );
    }

    // The repeated source is read once, not once per recipient: every child partition was executed
    // exactly once, and the single execution above is what executed them.
    assert_eq!(
        vec![1, 1, 1, 1],
        calls_by_partition
            .iter()
            .map(|call| call.load(Ordering::SeqCst))
            .collect::<Vec<_>>(),
        "the repeated source is read once per child partition"
    );
    assert_eq!(
        0,
        pool.reserved(),
        "the execution released reservations at EOF"
    );
}

/// A one-to-one match over one repeated source is placed in one execution, and this is why.
///
/// Both leaves of [`subtracted_scaled_tree`] bind the same source and the outer node is an
/// `on(host)` one-to-one match. Canonical-key co-location - a whole partition, or one owner per
/// canonical label set - proves a *merge*'s input whole, and proves nothing about a match's label
/// set: `on(host)` can group two series that differ in every other label and so sit under two
/// canonical keys in two partitions. Each partition would then pair only its own series against its
/// own and never report the duplicate label set a whole one-to-one match owes.
///
/// So the node reports one partition, which is what makes its single execution see every series the
/// match could pair. The four-partition child here is the shape that would expose a per-partition
/// placement, so a reader who "fixes" this count back to four reopens exactly that hole.
#[test]
fn a_one_to_one_match_over_one_repeated_source_runs_in_one_execution() {
    let partitions = (0..4)
        .map(|partition| {
            vec![frame(&series_chunk(
                &format!("web-{partition}"),
                &series_samples(1.0),
                true,
            ))]
        })
        .collect::<Vec<_>>();
    let exec = plan(
        subtracted_scaled_tree(),
        memory_exec_partitioned(partitions),
    )
    .expect("the repeated source tree runs over one physical child");
    assert_eq!(
        1,
        exec.properties().output_partitioning().partition_count(),
        "a per-partition placement would pair each partition's series only against its own and \
         never report the duplicate label set a whole one-to-one match owes"
    );
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
    // The count is the input's, which is what the node reports and what it
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
    assert_eq!(left, right);
    assert_eq!(Some(std::cmp::Ordering::Equal), left.partial_cmp(&right));

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
        vec![input_plan(frame_schema())],
        nullability_changed.to_dfschema_ref().unwrap(),
    )
    .unwrap();
    assert_ne!(left_node, right_node);
    assert_ne!(
        Some(std::cmp::Ordering::Equal),
        left_node.partial_cmp(&right_node)
    );
}

/// A later repeated-source enqueue failure is surfaced before any output and releases the input.
#[tokio::test]
async fn a_later_repeated_source_enqueue_failure_releases_the_input() {
    use common_query::series_frame::unpack_raw_series;

    let schema = raw_schema();
    let chunk = series_chunk("web-1", &series_samples(1.0)[..4], false);
    let one_frame = frame_of(&schema, &chunk);
    let decoded = unpack_raw_series(
        Box::pin(RecordBatchStreamAdapter::new(
            frame_schema(),
            futures::stream::iter(vec![Ok(one_frame.clone())]),
        )),
        Some(Arc::clone(&schema)),
    )
    .expect("the frame decodes using the source decoder");
    let mut decoded = crate::source::materialize_raw_labels(decoded)
        .expect("the labels materialize as the source does");
    let decoded_chunk = decoded
        .next()
        .await
        .expect("the decoder returns its frame")
        .expect("the frame decodes successfully");

    let probe_pool = Arc::new(GreedyMemoryPool::new(1 << 30));
    let probe_state = NativeExecutionState::try_new(
        &subtracted_scaled_tree(),
        Arc::clone(&probe_pool) as Arc<dyn MemoryPool>,
    )
    .expect("the repeated-source runtime builds");
    let ids = probe_state.input_ids(0).collect::<Vec<_>>();
    let mut probe = NativeExecutor::new(probe_state);
    assert_eq!(2, ids.len(), "the tree has two runtime recipients");
    probe
        .push_raw(ids[0], decoded_chunk.clone())
        .expect("the first recipient accepts the decoded chunk");
    let charge = probe_pool.reserved();
    assert!(charge > 0, "the decoded chunk reserves memory");
    drop(probe);

    let bounded_pool = Arc::new(GreedyMemoryPool::new(charge));
    let bounded_state = NativeExecutionState::try_new(
        &subtracted_scaled_tree(),
        Arc::clone(&bounded_pool) as Arc<dyn MemoryPool>,
    )
    .expect("the repeated-source runtime builds in the bounded pool");
    let bounded_ids = bounded_state.input_ids(0).collect::<Vec<_>>();
    let mut bounded = NativeExecutor::new(bounded_state);
    bounded
        .push_raw(bounded_ids[0], decoded_chunk.clone())
        .expect("the bounded pool admits the first recipient");
    assert!(
        bounded.push_raw(bounded_ids[1], decoded_chunk).is_err(),
        "the bounded pool refuses the second recipient"
    );
    assert_eq!(
        0,
        bounded_pool.reserved(),
        "failure releases both recipients"
    );
    drop(bounded);
    assert_eq!(0, bounded_pool.reserved(), "the bounded probe releases it");
    assert_eq!(0, probe_pool.reserved(), "dropping the probe releases it");

    let dropped = Arc::new(AtomicBool::new(false));
    let pulled = Arc::new(AtomicUsize::new(0));
    let later_frame = frame(&series_chunk("web-2", &series_samples(2.0)[..4], false));
    let source_schema = frame_schema();
    let source = staged(source_schema.clone(), {
        let dropped = Arc::clone(&dropped);
        let pulled = Arc::clone(&pulled);
        move || {
            Box::pin(
                StagedStream::new(
                    source_schema.clone(),
                    vec![one_frame.clone(), later_frame.clone()],
                )
                .dropped(&dropped)
                .counted(&pulled),
            )
        }
    });
    let pool = Arc::new(GreedyMemoryPool::new(charge));
    let mut stream = plan(subtracted_scaled_tree(), source)
        .expect("the repeated-source plan builds")
        .execute(0, task_context(&pool))
        .expect("the node executes");
    let error = stream
        .next()
        .await
        .expect("the later enqueue fails")
        .expect_err("no output is produced before feed-all completes");
    assert!(
        error_chain(&error).to_lowercase().contains("memory")
            || error_chain(&error).to_lowercase().contains("reservation")
            || error_chain(&error).to_lowercase().contains("resource"),
        "the failure is the exhausted memory pool: {error}"
    );
    assert_eq!(
        0,
        pool.reserved(),
        "failure immediately releases reservations"
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "failure releases the upstream stream"
    );
    let batches_at_failure = pulled.load(Ordering::SeqCst);
    assert!(
        batches_at_failure >= 1,
        "the first frame reached the decoder"
    );
    assert!(
        stream.next().await.is_none(),
        "the terminated stream stays ended"
    );
    assert_eq!(batches_at_failure, pulled.load(Ordering::SeqCst));
}

/// Logical equality distinguishes source field layouts, even when the PromQL and output match.
#[test]
fn logical_identity_includes_native_source_field_order() {
    let ordered_schema = two_field_schema();
    let mut reversed_schema = (*ordered_schema).clone();
    reversed_schema.fields = Arc::from([
        FieldSpec { name: OTHER.into() },
        FieldSpec { name: VALUE.into() },
    ]);
    let make_node = |schema: Arc<SeriesSchema>| {
        let tree = rate_tree_of(schema);
        FusedSeries::try_new(
            Arc::from(PROMQL),
            tree.clone(),
            binding(&tree),
            vec![input_plan(frame_schema())],
            output_schema().to_dfschema_ref().unwrap(),
        )
        .expect("the source schema is legal")
    };
    let left = make_node(Arc::clone(&ordered_schema));
    let right = make_node(Arc::new(reversed_schema));
    assert_eq!(left.promql(), right.promql());
    assert_eq!(left.binding(), right.binding());
    assert_ne!(left, right);
    assert_ne!(Some(std::cmp::Ordering::Equal), left.partial_cmp(&right));

    let same_order = make_node(Arc::clone(&ordered_schema));
    assert_eq!(left, same_order);
    assert_eq!(
        Some(std::cmp::Ordering::Equal),
        left.partial_cmp(&same_order)
    );
}

/// Dropping a pending repeated-source execution releases its reservations and physical input.
#[tokio::test]
async fn dropping_a_pending_repeated_source_releases_input_and_reservations() {
    let dropped = Arc::new(AtomicBool::new(false));
    let schema = frame_schema();
    let partial = frame(&series_chunk("web-1", &series_samples(1.0)[..4], false));
    let source = staged(schema.clone(), {
        let dropped = Arc::clone(&dropped);
        move || {
            Box::pin(
                StagedStream::new(schema.clone(), vec![partial.clone()])
                    .pending_after(1)
                    .dropped(&dropped),
            )
        }
    });
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let mut stream = plan(subtracted_scaled_tree(), source)
        .expect("the repeated-source tree runs")
        .execute(0, task_context(&pool))
        .expect("the node executes");

    assert!(futures::poll!(stream.next()).is_pending());
    assert!(
        pool.reserved() > 0,
        "the pending execution holds input data"
    );
    assert!(
        !dropped.load(Ordering::SeqCst),
        "the source is still executing"
    );
    drop(stream);
    assert_eq!(0, pool.reserved(), "dropping releases every reservation");
    assert!(
        dropped.load(Ordering::SeqCst),
        "dropping releases the source"
    );
}

/// Rebuilding keeps execution metrics independent from those of the original plan.
#[tokio::test]
async fn a_rebuilt_node_reports_only_its_own_execution() {
    let exec = plan(rate_tree(), memory_exec(one_series_frames()))
        .expect("the runtime executes a rate tree");
    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("the node runs");
    let first_rows = rows(&batches);
    assert_eq!(
        grid_timestamps(),
        first_rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        "the series is answered at every grid position"
    );
    let original = exec
        .metrics()
        .expect("the node reports its runtime's metrics");
    assert_eq!(1, count(&original, "native.root.completed_series"));

    // Rebuilt over a fresh input of the same frames, the way an optimizer replaces a child.
    let rebuilt = Arc::clone(&exec)
        .replace_children(
            vec![memory_exec(one_series_frames())],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .expect("the node rebuilds over another input");
    // No execution of the rebuilt node has run, so it registered no handles to report.
    let before = rebuilt
        .metrics()
        .expect("a plan that has not run reports an empty set rather than none");
    assert_eq!(
        0,
        before.iter().count(),
        "the rebuilt node inherited an execution's metrics"
    );
    // Rebuilding does not reset the node it was built from.
    assert_eq!(1, count(&original, "native.root.completed_series"));

    let rebuilt_batches = collect(Arc::clone(&rebuilt), SessionContext::default().task_ctx())
        .await
        .expect("the rebuilt node runs");
    assert_eq!(first_rows, rows(&rebuilt_batches));

    let after = rebuilt
        .metrics()
        .expect("the rebuilt node reports the execution it ran");
    assert_eq!(1, count(&after, "native.root.completed_series"));
    assert_eq!(1, count(&after, "native.root.input.completed_series"));
    // One execution's set, not two: a copy of the original's roots would answer 2 here.
    assert_eq!(original.iter().count(), after.iter().count());
    let retained = exec
        .metrics()
        .expect("the original keeps the handles its execution registered");
    assert_eq!(original.iter().count(), retained.iter().count());
    assert_eq!(1, count(&retained, "native.root.completed_series"));
    assert_eq!(1, count(&retained, "native.root.input.completed_series"));
}

/// Reading metrics retains handles, not runtime ownership or duplicate registrations.
#[tokio::test]
async fn repeated_metrics_reads_register_nothing_and_outlive_the_execution() {
    let schema = frame_schema();
    let partial = frame(&series_chunk("web-1", &series_samples(1.0)[..4], false));
    let exec = plan(
        rate_tree(),
        staged(schema.clone(), move || {
            Box::pin(StagedStream::new(schema.clone(), vec![partial.clone()]).pending_after(1))
        }),
    )
    .expect("the runtime executes a rate tree");
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let mut stream = exec
        .execute(0, task_context(&pool))
        .expect("the node executes");
    // The source hands one unclosed chunk over and parks, so the window holds it while the plan's
    // metrics are read: the reads below happen while an execution is alive.
    assert!(futures::poll!(stream.next()).is_pending());

    let first = exec
        .metrics()
        .expect("the metrics are registered before the run");
    let names = |metrics: &MetricsSet| -> Vec<String> {
        metrics
            .iter()
            .map(|metric| metric.value().name().to_string())
            .collect()
    };
    let second = exec.metrics().expect("metrics can be read repeatedly");
    assert_eq!(
        names(&first),
        names(&second),
        "reading the metrics again registered another copy of them"
    );
    assert_eq!(
        first.iter().count(),
        second.iter().count(),
        "the second read changed how many metrics the plan reports"
    );
    // A read hands back the very entries the execution records into, not a fresh registration.
    for (early, late) in first.iter().zip(second.iter()) {
        assert!(
            Arc::ptr_eq(early, late),
            "the second read registered {} again",
            late.value().name()
        );
    }

    drop(stream);
    // What the execution recorded is still readable once it is gone, and nothing it held is charged
    // any more.
    let after = exec
        .metrics()
        .expect("the handles outlive the execution they were registered for");
    assert_eq!(
        names(&first),
        names(&after),
        "dropping the execution changed the metrics the plan reports"
    );
    let current = after
        .sum_by_name("native.root.current_reserved_bytes")
        .expect("the current-bytes gauge is registered");
    let peak = after
        .sum_by_name("native.p0.root.peak_reserved_bytes")
        .expect("the peak-bytes gauge is registered");
    assert!(
        matches!(&current, MetricValue::Gauge { gauge, .. } if gauge.value() == 0),
        "the dropped execution holds nothing: {current:?}"
    );
    assert!(
        matches!(&peak, MetricValue::Gauge { gauge, .. } if gauge.value() > 0),
        "the peak is what the execution did, not what it holds: {peak:?}"
    );
    assert_eq!(
        1,
        count(&after, "native.root.input.input_batches"),
        "the counter of the batches the leaf read did not survive the execution"
    );
    assert_eq!(0, pool.reserved(), "the dropped execution holds no bytes");

    // A second execution of the same plan registers its own tree's handles. It is dropped without
    // being polled, so what it adds is its own counters at zero -- the first execution's figures are
    // untouched -- and no part of the tree it built outlives it.
    let registered = after.iter().count();
    let second_stream = exec
        .execute(0, task_context(&pool))
        .expect("the plan runs a second time");
    drop(second_stream);
    let twice = exec
        .metrics()
        .expect("the metrics of both executions are read");
    assert_eq!(
        twice.iter().count(),
        registered * 2,
        "the second execution did not register its own node handles"
    );
    assert_eq!(
        count(&twice, "native.root.input.input_batches"),
        1,
        "the second execution's leaf counted the batches of the first"
    );
    let current = twice
        .sum_by_name("native.root.current_reserved_bytes")
        .expect("the current-bytes gauge is registered");
    assert!(
        matches!(&current, MetricValue::Gauge { gauge, .. } if gauge.value() == 0),
        "an execution that did nothing kept bytes: {current:?}"
    );
    assert_eq!(0, pool.reserved(), "the dropped executions hold no bytes");

    // Reading both executions' metrics is still a read: the same entries come back.
    let again = exec.metrics().expect("metrics can be read repeatedly");
    assert_eq!(twice.iter().count(), again.iter().count());
    for (early, late) in twice.iter().zip(again.iter()) {
        assert!(
            Arc::ptr_eq(early, late),
            "the read registered {} again",
            late.value().name()
        );
    }
}

/// One chunk of the series keyed `key`, carrying `labels` and closing when `last`.
///
/// The key and labels are separate so a test can hold two raw series under one label set; different
/// keys make such a pair two series.
fn labelled_chunk(
    key: &str,
    labels: &[(&str, &str)],
    rows: &[(i64, f64)],
    last: bool,
) -> RawSeriesChunk {
    chunk_of(
        &raw_schema(),
        key,
        metric(labels, Some(METRIC)),
        &times_of(rows),
        vec![values_of(rows)],
        last,
    )
}

/// The `by (grouped_by)` aggregate of `kind` that the scope side of a split aggregate exports.
fn partial_op(kind: AggregateKind, grouped_by: &[&str]) -> BoundAggregate {
    let labels: Vec<Arc<str>> = grouped_by.iter().map(|label| Arc::from(*label)).collect();
    BoundAggregate {
        kind,
        group: Grouping::By(labels.into()),
        stage: AggregateStage::Complete,
    }
}

/// The binding of the prefix tree, declaring `tags` as the node's labels.
fn partial_binding_of(tree: &NativeTree, tags: &[&str]) -> FusedSeriesBinding {
    binding_of(
        tree,
        raw_schema(),
        tags.iter().map(|tag| (*tag).to_string()).collect(),
    )
}

/// The logical node that runs the prefix and exports `kind`'s state, grouped by `host`.
///
/// The prefix's one raw input is bound to source `0`, which is the only child the node carries: a
/// state export is per partition of that one scan, not a second source.
fn partial_node(kind: AggregateKind) -> FusedSeries {
    let tree = rate_tree();
    let binding = partial_binding_of(&tree, &[HOST]);
    FusedSeries::try_new_partial(
        Arc::from(PROMQL),
        tree,
        binding,
        vec![input_plan(frame_schema())],
        partial_op(kind, &[HOST]),
    )
    .expect("the binding matches the prefix tree")
}

/// The physical node that runs `tree` and exports `op`'s per-partition state over `input`.
///
/// `tags` are the labels the node declares, which the exported state layout is built from.
fn partial_plan(
    tree: NativeTree,
    input: Arc<dyn ExecutionPlan>,
    op: BoundAggregate,
    tags: &[&str],
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let binding = partial_binding_of(&tree, tags);
    FusedSeries::try_new_partial(
        Arc::from(PROMQL),
        tree,
        binding,
        vec![input_plan(frame_schema())],
        op,
    )?
    .to_execution_plan(vec![(
        input,
        RawLabelSource::Explicit,
        SeriesOrdering::Contiguous,
    )])
}

/// The one state batch a partition exports, with the schema `state`.
async fn state_of(
    exec: &Arc<dyn ExecutionPlan>,
    partition: usize,
    state: &SchemaRef,
) -> RecordBatch {
    let context = SessionContext::default().task_ctx();
    let batches = exec
        .execute(partition, Arc::clone(&context))
        .expect("the partition exists")
        .try_collect::<Vec<_>>()
        .await
        .expect("the partition's fold ends");
    assert_eq!(
        1,
        batches.len(),
        "one partition exports one state batch: {:?}",
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>()
    );
    assert_eq!(state.as_ref(), batches[0].schema().as_ref());
    batches.into_iter().next().unwrap()
}

/// The `tag_0` values of one state batch, which are the group's first declared label.
fn group_label(state: &SchemaRef, batch: &RecordBatch) -> Vec<Option<String>> {
    batch
        .column_by_name(state.field(1).name())
        .expect("the state carries the group label")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a group label is text")
        .iter()
        .map(|value| value.map(ToOwned::to_owned))
        .collect()
}

/// A partial fold exports one state batch per partition, and two partitions may fold one group.
///
/// The two partitions hold series whose *prefix* labels differ (`host="web-0"` and `host="web-1"`)
/// while the aggregate groups by `job` alone, so both fold into the one group `job="shared"`. That
/// group repeats in the exported state, which is legal because the merge above the exchange collects
/// both partitions' states. The first partition's series also arrives in two chunks, so the fold has
/// to keep what it folded between them.
#[tokio::test]
async fn a_partial_fold_exports_state_from_every_partition() {
    let rows = series_samples(1.0);
    let first = [(HOST, "web-0"), (JOB, "shared")];
    let second = [(HOST, "web-1"), (JOB, "shared")];
    let partitions = vec![
        frames(&[
            labelled_chunk("web-0", &first, &rows[..4], false),
            labelled_chunk("web-0", &first, &rows[4..], true),
        ]),
        vec![frame(&labelled_chunk("web-1", &second, &rows, true))],
    ];
    let exec = partial_plan(
        rate_tree(),
        memory_exec_partitioned(partitions),
        partial_op(AggregateKind::Count, &[JOB]),
        &[JOB],
    )
    .expect("a rate tree with a partial count is executable");

    assert_eq!(
        datafusion::physical_plan::execution_plan::EmissionType::Final,
        exec.properties().emission_type,
        "the partial state is emitted only after the input reaches EOF"
    );
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "the node's partition count is its input's"
    );
    let state = state_schema(&[JOB.to_string()]);
    assert_eq!(state, exec.schema(), "the node declares the state layout");

    for partition in 0..2 {
        let batch = state_of(&exec, partition, &state).await;
        assert_eq!(
            GRID_LEN as usize,
            batch.num_rows(),
            "the group has one row per grid point, whichever chunks its series arrived in"
        );
        assert_eq!(
            vec![Some("shared".to_string()); GRID_LEN as usize],
            group_label(&state, &batch),
            "the exported rows name the group they were folded into"
        );
    }
}

/// Prefix series of one label set on disjoint grid points are legal in state mode.
///
/// The exported states can be merged above this partition-local fold.
///
/// A window is left-open, `(T - 5m, T]`, so a fragment answers at a grid point only when two of its
/// own samples fall inside that window. The two fragments are spaced for exactly that: the early one
/// answers at 300s alone, the late one at 540s alone, and neither answers where the other does.
#[tokio::test]
async fn disjoint_prefix_points_across_partitions_export_state() {
    let labels = [(HOST, "web")];
    // Sample `0` would fall outside the left-open window of grid point 300s, so both of these do.
    let early = [(SAMPLE_STEP, 60.0), (2 * SAMPLE_STEP, 120.0)];
    let late = [(470_000, 470.0), (540_000, 540.0)];
    let partitions = vec![
        vec![frame(&labelled_chunk("web-0", &labels, &early, true))],
        vec![frame(&labelled_chunk("web-1", &labels, &late, true))],
    ];
    let exec = partial_plan(
        rate_tree(),
        memory_exec_partitioned(partitions),
        partial_op(AggregateKind::Sum, &[HOST]),
        &[HOST],
    )
    .expect("a rate tree with a partial sum is executable");

    let state = state_schema(&[HOST.to_string()]);
    for partition in 0..2 {
        let batch = state_of(&exec, partition, &state).await;
        assert!(
            group_label(&state, &batch)
                .iter()
                .flatten()
                .all(|value| value == "web"),
            "each partition's state names the group it folded"
        );
    }
}

/// Each stream owns its exported-state reservation until that stream ends, even while the plan
/// stays alive and another execution of the same plan still holds its own state.
#[tokio::test]
async fn an_exported_state_batch_stays_charged_until_its_stream_ends() {
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let exec = partial_plan(
        rate_tree(),
        memory_exec(vec![frame(&series_chunk(
            "web-0",
            &series_samples(1.0),
            true,
        ))]),
        partial_op(AggregateKind::Sum, &[HOST]),
        &[HOST],
    )
    .expect("a rate tree with a partial sum is executable");
    let mut first = exec
        .execute(0, Arc::clone(&context))
        .expect("the first execution's partition exists");
    let mut second = exec
        .execute(0, Arc::clone(&context))
        .expect("the second execution's partition exists");
    let first_batches = (&mut first)
        .try_collect::<Vec<_>>()
        .await
        .expect("the first partition's fold ends");
    let second_batches = (&mut second)
        .try_collect::<Vec<_>>()
        .await
        .expect("the second partition's fold ends");
    assert_eq!(1, first_batches.len(), "one state batch per execution");
    assert_eq!(1, second_batches.len(), "one state batch per execution");
    assert!(pool.reserved() > 0, "both exported states are charged");

    drop(first);
    assert!(
        pool.reserved() > 0,
        "dropping one stream preserves the other stream's reservation while the plan lives"
    );
    drop(second);
    assert_eq!(
        0,
        pool.reserved(),
        "dropping both streams releases their reservations even though the plan remains alive"
    );
    assert_eq!(
        1,
        first_batches.len(),
        "the first batches are still the test's"
    );
    assert_eq!(
        1,
        second_batches.len(),
        "the second batches are still the test's"
    );
}

/// A partial node keeps its mode when an optimizer rebuilds it, and another aggregate is another
/// node.
#[test]
fn a_partial_mode_survives_rebuilding_and_is_compared() {
    let node = partial_node(AggregateKind::Sum);
    assert!(node.partial_fold().is_some(), "the fixture exports state");
    let rebuilt = node
        .with_exprs_and_inputs(Vec::new(), vec![input_plan(frame_schema())])
        .expect("the rebuilt node is legal");
    assert!(
        matches!(rebuilt.partial_fold(), Some(op) if op.kind == AggregateKind::Sum),
        "rebuilding the node dropped the aggregate it exports"
    );
    assert_eq!(node, rebuilt);

    let counted = partial_node(AggregateKind::Count);
    assert_ne!(node, counted, "another aggregate is another computation");
    assert_ne!(Some(std::cmp::Ordering::Equal), node.partial_cmp(&counted));
}

/// A prefix tree that itself reads across series is refused: every partition would fold a part.
#[test]
fn a_prefix_that_reads_across_series_is_refused() {
    let tree = aggregate_tree();
    let binding = binding(&tree);
    let error = FusedSeries::try_new_partial(
        Arc::from(PROMQL),
        tree,
        binding,
        vec![input_plan(frame_schema())],
        partial_op(AggregateKind::Sum, &[HOST]),
    )
    .expect_err("a prefix that folds a group per partition is not a per-series prefix");
    assert!(error.to_string().contains("reads across"), "{error}");
}

/// `constant <op> vector`, bound the way the compiler binds a scalar broadcast: the constant on the
/// left, and the vector side's samples as the operation's output.
fn broadcast_left(constant: f64, vector: NativeNode, op: BinaryOp) -> NativeNode {
    let grid = vector
        .output
        .grid()
        .expect("a broadcast reads one evaluated vector")
        .clone();
    let output = vector.output.clone();
    NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(NativeNode {
                expr: NativeExpr::Scalar(BoundScalar { value: constant }),
                output: NativeType::Scalar(grid),
            }),
            right: Box::new(vector),
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

/// A constant broadcast is per-series work, so a partition folds it like any other prefix.
///
/// `max(10 - rate(x[5m]))` over the rates `1` and `2` of the two series below folds `9` and `8` of one
/// group and exports `9`. Reading the constant on the other side exports `-8`, and folding the rate
/// and subtracting the constant above the merge exports `10 - 2 = 8`, so the answer says where the
/// operation ran and which operand its constant was.
#[tokio::test]
async fn a_constant_broadcast_prefix_exports_the_values_the_constant_computes() {
    let tree = NativeTree::try_new(broadcast_left(
        10.0,
        rate_tree().root().clone(),
        BinaryOp::Sub,
    ))
    .expect("a broadcast over the rate tree is legal");
    let exec = partial_plan(
        tree,
        memory_exec(two_series_frames()),
        partial_op(AggregateKind::Max, &[]),
        &[],
    )
    .expect("a per-series broadcast is a prefix one partition evaluates on its own");

    let state = state_schema(&[]);
    let batch = state_of(&exec, 0, &state).await;
    let values = batch
        .column_by_name("value")
        .expect("the state carries the accumulator")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("an accumulator is Float64");
    let values: Vec<Option<f64>> = values.iter().collect();
    assert_eq!(
        vec![Some(9.0); GRID_LEN as usize],
        values,
        "every grid point holds `10 - 1`, the larger of the two series' values"
    );
}

/// A partial fold that fails after valid input exports no state and releases its reservation.
#[tokio::test]
async fn a_failed_input_exports_no_partial_state() {
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
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
    let exec = partial_plan(
        rate_tree(),
        source,
        partial_op(AggregateKind::Sum, &[HOST]),
        &[HOST],
    )
    .expect("a rate tree with a partial sum is executable");
    assert_eq!(
        1,
        exec.properties().output_partitioning().partition_count(),
        "the node's partition count is its input's"
    );
    let mut stream = exec
        .execute(0, Arc::clone(&context))
        .expect("the node executes");
    match stream.next().await {
        Some(Err(error)) => assert!(
            error.to_string().contains("the source failed"),
            "reported another error: {error}"
        ),
        other => panic!("a failed input exported state: {other:?}"),
    }
    assert!(
        dropped.load(Ordering::SeqCst),
        "the failed stream retains its upstream input"
    );
    assert_eq!(
        0,
        pool.reserved(),
        "failure releases the prefix fold's reservation while the error stream remains held"
    );
    drop(stream);

    let error = match exec.execute(1, Arc::clone(&context)) {
        Ok(_) => panic!("a partition the input does not have is answered"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("partition"),
        "refused for another reason: {error}"
    );
}

/// Frame output is planned from the evaluated root and is emitted without flattening to rows.
#[tokio::test]
async fn series_frame_output_uses_evaluated_codec_schema() {
    let tree = rate_tree();
    let frame_node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .expect("evaluated samples have a frame schema");
    assert!(
        frame_node
            .schema()
            .fields()
            .iter()
            .all(|field| field.name() != "ts"),
        "frame schema comes from the codec rather than the row output"
    );
    let wrong_schema = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::SeriesFrame,
    )
    .expect_err("the caller's incorrect output schema is rejected");
    assert!(wrong_schema.to_string().contains("schema does not match"));
    assert!(format!("{frame_node:?}").contains("FusedSeries"));
    let exec = plan_of(frame_node, memory_exec(one_series_frames())).unwrap();
    assert_eq!(1, exec.properties().partitioning.partition_count());
    let batches = collect(exec.clone(), SessionContext::default().task_ctx())
        .await
        .unwrap();
    assert!(!batches.is_empty());
    assert!(batches.iter().all(|batch| batch.schema() == exec.schema()));
    assert!(batches.iter().any(|batch| batch.num_rows() > 0));
}

/// Frame output refuses the currently unsupported multi-source physical shape at planning time.
#[test]
fn series_frame_rejects_multiple_sources_during_planning() {
    let tree = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let err = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap_err();
    assert!(err.to_string().contains("multiple sources"));
}

/// A real four-partition input remains four independent frame-output executions.
#[tokio::test]
async fn series_frame_preserves_four_input_partitions() {
    let partitions = (0..4)
        .map(|index| {
            frames(&[series_chunk(
                &format!("part-{index}"),
                &series_samples((index + 1) as f64),
                true,
            )])
        })
        .collect::<Vec<_>>();
    let tree = rate_tree();
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let exec = plan_of(node, memory_exec_partitioned(partitions)).unwrap();
    assert_eq!(4, exec.properties().partitioning.partition_count());
    let schema = exec.schema();
    let mut closes = 0;
    for partition in 0..4 {
        let stream = exec
            .execute(partition, SessionContext::default().task_ctx())
            .unwrap();
        let decoded = unpack_evaluated_series(stream, None)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            1,
            decoded.len(),
            "one chain closes in partition {partition}"
        );
        assert!(decoded[0].1);
        closes += decoded.iter().filter(|(_, last)| *last).count();
        let values = decoded[0].0.fields.get(0).unwrap();
        let value = values.iter().next().unwrap();
        let datatypes::series::samples::SampleRef::Float(value) = value else {
            panic!("float sample expected")
        };
        assert!((value - (partition + 1) as f64).abs() < 1e-9);
        assert_eq!(schema, exec.schema());
    }
    assert_eq!(4, closes);
}

/// PlainRows and SeriesFrame encode precisely the same rate values, timestamps and labels.
#[tokio::test]
async fn series_frame_rate_matches_plain_rows_bitwise() {
    let input = memory_exec(two_series_frames());
    let plain = plan(rate_tree(), Arc::clone(&input)).unwrap();
    let plain_rows = rows(
        &collect(plain, SessionContext::default().task_ctx())
            .await
            .unwrap(),
    );
    let tree = rate_tree();
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let exec = plan_of(node, input).unwrap();
    let batches = collect(exec.clone(), SessionContext::default().task_ctx())
        .await
        .unwrap();
    assert!(batches.iter().all(|batch| batch.schema() == exec.schema()));
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        exec.schema(),
        futures::stream::iter(batches.into_iter().map(Ok)),
    ));
    let decoded = unpack_evaluated_series(stream, None)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut frame_rows = Vec::new();
    for (series, _) in decoded {
        let timestamps = series.time.timestamps().unwrap();
        let samples = series.fields.get(0).unwrap();
        for (row, timestamp) in timestamps.iter().enumerate() {
            let value = samples.iter().nth(row).unwrap();
            let datatypes::series::samples::SampleRef::Float(value) = value else {
                panic!("float sample expected")
            };
            frame_rows.push((
                *timestamp,
                value.to_bits(),
                series.metric.get(HOST).map(str::to_owned),
                series.metric.name().map(str::to_owned),
            ));
        }
    }
    let mut expected = plain_rows
        .into_iter()
        .map(|(time, value, host, name)| (time, value.map(f64::to_bits), host, name))
        .collect::<Vec<_>>();
    frame_rows.sort();
    expected.sort();
    assert_eq!(
        expected,
        frame_rows
            .into_iter()
            .map(|(time, bits, host, name)| (time, Some(bits), host, name))
            .collect::<Vec<_>>()
    );
}

/// The partial-state node keeps its state schema and explain annotation, independently of output format.
#[tokio::test]
async fn partial_fold_keeps_plain_rows_state_export() {
    let node = partial_node(AggregateKind::Sum);
    assert_eq!(FusedOutputFormat::PlainRows, node.format);
    let exec = plan_of(node, memory_exec(one_series_frames())).unwrap();
    let state = state_schema(&[HOST.to_string()]);
    assert_eq!(state.as_ref(), exec.schema().as_ref());
    assert!(
        displayable(exec.as_ref())
            .indent(false)
            .to_string()
            .contains("partial state:")
    );
    let batch = state_of(&exec, 0, &state).await;
    assert_eq!(state.as_ref(), batch.schema().as_ref());
}

/// `EXPLAIN` says nothing extra about the row table and names the frame output.
///
/// The row table is what a query returns, so its node line has to stay exactly what it was; a frame
/// output answers something else, and the line is the only place a reader can see that.
#[test]
fn the_explain_text_names_the_frame_format_only() {
    let rows = fused(rate_tree());
    assert_eq!(
        "FusedSeries: rate(x[5m])",
        Explain(&rows).to_string(),
        "the row-table line is what it always was"
    );
    let framed = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        rows.tree().clone(),
        rows.binding().clone(),
        rows.inputs().into_iter().cloned().collect(),
        canonical_frame_schema(rows.tree()),
        FusedOutputFormat::SeriesFrame,
    )
    .expect("a rate tree is evaluated on a grid");
    assert!(
        Explain(&framed)
            .to_string()
            .contains("(output: series_frame)"),
        "a frame output names its format: {}",
        Explain(&framed)
    );

    let plain = plan(rate_tree(), memory_exec(Vec::new())).expect("the runtime executes the tree");
    let text = displayable(plain.as_ref()).indent(false).to_string();
    assert!(
        text.starts_with("FusedSeriesExec: rate(x[5m])\n"),
        "the row-table node line is unchanged: {text}"
    );
}

/// The logical schema follows the format, and a frame schema is the codec's own declaration.
///
/// The frame layout is not assembled here: it is read back through the declaration parser, so what
/// the plan claims is what a consumer of the frame will decode.
#[test]
fn the_logical_schema_follows_the_output_format() {
    let rows = fused(rate_tree());
    assert_eq!(FusedOutputFormat::PlainRows, rows.format);
    assert_eq!(&output_schema(), rows.schema().inner());

    let framed = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        rows.tree().clone(),
        rows.binding().clone(),
        rows.inputs().into_iter().cloned().collect(),
        canonical_frame_schema(rows.tree()),
        FusedOutputFormat::SeriesFrame,
    )
    .expect("a rate tree is evaluated on a grid");
    assert_eq!(FusedOutputFormat::SeriesFrame, framed.format);
    let declaration =
        SeriesFrameDeclaration::parse(framed.schema().inner().as_ref()).expect("a frame schema");
    assert_eq!(FrameContent::Evaluated, declaration.content());
    assert_eq!(FrameLabelMode::Explicit, declaration.labels());
    assert_eq!(
        SeriesOrdering::Contiguous,
        series_frame_ordering(framed.schema().inner().as_ref())
            .expect("the frame declares its ordering"),
        "computed values promise per-chain contiguity and nothing stronger"
    );
}

/// Rebuilding a frame node keeps the format it was built with.
///
/// The format decides the node's schema, so a rebuild that dropped it would answer rows where the
/// plan said frame. `with_exprs_and_inputs` is what the optimizer uses.
#[test]
fn rebuilding_a_frame_node_keeps_its_output_format() {
    let rows = fused(rate_tree());
    let framed = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        rows.tree().clone(),
        rows.binding().clone(),
        rows.inputs().into_iter().cloned().collect(),
        canonical_frame_schema(rows.tree()),
        FusedOutputFormat::SeriesFrame,
    )
    .expect("a rate tree is evaluated on a grid");
    let rebuilt = framed
        .with_exprs_and_inputs(
            Vec::new(),
            framed.inputs().into_iter().cloned().collect::<Vec<_>>(),
        )
        .expect("the node rebuilds over its own input");
    assert_eq!(FusedOutputFormat::SeriesFrame, rebuilt.format);
    assert_eq!(framed.schema(), rebuilt.schema());
    assert_eq!(Explain(&framed).to_string(), Explain(&rebuilt).to_string());
}

/// A tree whose root is not evaluated samples cannot be framed, and says so when the plan is built.
///
/// A scalar has no series to name, so it is not a series of float samples; an aggregate-state root is
/// refused by the same check for the same reason, and `partial_node` cannot even be built with a
/// frame format. The refusal is a planning error, so a caller learns about it before the query runs
/// rather than from a partition that fails later.
#[test]
fn a_series_frame_refuses_a_root_it_cannot_carry() {
    let scalar = scalar_tree(grid(), 1.0);
    // A scalar tree binds no raw source, so it is built over no child at all: the refusal below is
    // then about the root, not about a source count that was wrong first.
    let binding = FusedSeriesBinding {
        grid: grid(),
        metric_name: Some(Arc::from(METRIC)),
        time_index: TIME.to_string(),
        tags: vec![HOST.to_string(), NAME.to_string()],
    };
    let error = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        scalar,
        binding,
        Vec::new(),
        output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::SeriesFrame,
    )
    .expect_err("a scalar is not a series to frame");
    assert!(
        matches!(error, DataFusionError::Plan(_)),
        "refused with {error}"
    );
    assert!(
        error.to_string().contains("scalar"),
        "the refusal says what the root is: {error}"
    );

    // The partial mode is a different output entirely, so a frame format cannot even be asked for.
    assert_eq!(
        FusedOutputFormat::PlainRows,
        partial_node(AggregateKind::Sum).format
    );
}

/// A pending input hands the rows a frame already encoded over before it reports pending.
///
/// The builder buffers up to a batch, and a closing row only exists once its chunk is encoded. If a
/// pending input could hold those rows, a consumer waiting for a series to close would wait for input
/// that may never arrive.
///
/// This polls by hand rather than awaiting the stream: `StagedStream` reports pending once and then
/// wakes itself, so an `await` would run straight through the boundary and satisfy every later
/// assertion whether or not the rows were handed over at the right moment. What is checked instead is
/// the poll that *meets* the pending input - it has to return rows, and the input has to have been
/// read only once by then.
#[tokio::test]
async fn a_pending_input_does_not_hold_the_rows_a_frame_buffered() {
    let schema = frame_schema();
    let batches = one_series_frames();
    let handed = Arc::new(AtomicUsize::new(0));
    let staged_schema = Arc::clone(&schema);
    let staged_handed = Arc::clone(&handed);
    let source = staged(Arc::clone(&schema), move || {
        Box::pin(
            StagedStream::new(Arc::clone(&staged_schema), batches.clone())
                // The third read is the one that reports pending. By then the first two chunks have
                // filled an evaluation window, so the encoder is holding rows: the boundary is met
                // with something buffered, which is the case worth checking.
                .pending_after(2)
                .counted(&staged_handed),
        )
    });
    let tree = rate_tree();
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(schema)],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .expect("a rate tree is evaluated on a grid");
    let exec = plan_of(node, source).expect("the runtime executes the tree");
    let declared = exec.schema();
    let mut stream = exec
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    // Poll by hand so the boundary is observable: `await` runs through the pending poll, and every
    // later assertion would then hold whether or not the rows came out at the right moment.
    //
    // The pending poll is the one under test. `StagedStream` wakes itself straight after reporting
    // pending, so a stream that answered nothing there would still be collected correctly; the only
    // way to see the difference is to look at what that single poll returned. Two chunks have been
    // read by then and an evaluation window is full, so the encoder is holding rows: it must answer
    // them, not pending.
    let next = stream.next();
    futures::pin_mut!(next);
    let mut answered = match futures::poll!(next) {
        std::task::Poll::Ready(Some(Ok(batch))) => vec![batch],
        std::task::Poll::Ready(Some(Err(error))) => panic!("the frame answered {error}"),
        std::task::Poll::Ready(None) => panic!("the frame ended before answering a row"),
        std::task::Poll::Pending => panic!(
            "the pending input held the rows the frame already encoded: the input handed over {} \
             batches",
            handed.load(Ordering::SeqCst)
        ),
    };
    assert_eq!(
        declared.as_ref(),
        answered[0].schema().as_ref(),
        "a batch is declared as the plan declared it"
    );
    answered.extend(
        stream
            .try_collect::<Vec<_>>()
            .await
            .expect("the stream resumes where the pending poll left it"),
    );
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        declared,
        futures::stream::iter(answered.into_iter().map(Ok)),
    ));
    let decoded = unpack_evaluated_series(stream, None)
        .expect("the node declares the frame it yields")
        .try_collect::<Vec<_>>()
        .await
        .expect("every batch of the frame decodes");
    assert_eq!(
        1,
        decoded.iter().filter(|(_, last)| *last).count(),
        "one series closes exactly once, across the pending boundary"
    );
    // What the row table answers for the same input is the reference: `rate` decides which grid
    // points carry a value, so computing the expected timestamps here would guess at it.
    let plain =
        plan(rate_tree(), memory_exec(one_series_frames())).expect("the runtime executes a tree");
    let mut expected = rows(
        &collect(plain, SessionContext::default().task_ctx())
            .await
            .expect("the row table runs"),
    )
    .into_iter()
    .map(|(time, _, _, _)| time)
    .collect::<Vec<_>>();
    expected.sort_unstable();
    let mut times = Vec::new();
    for (series, _) in &decoded {
        for timestamp in series.time.timestamps().expect("a grid axis").iter() {
            times.push(*timestamp);
        }
    }
    times.sort_unstable();
    assert_eq!(
        expected, times,
        "every row the row table answers is in the frame, none twice"
    );
}

/// Errors from a fused frame input terminate once and release its upstream immediately.
#[tokio::test]
async fn fused_frame_input_error_terminates_and_releases_input() {
    let schema = frame_schema();
    let partial = frame(&series_chunk(
        "web-1",
        &[(GRID_START + RANGE_MS + SAMPLE_STEP, 1.0)],
        false,
    ));
    let dropped = Arc::new(AtomicBool::new(false));
    let staged_schema = Arc::clone(&schema);
    let staged_dropped = Arc::clone(&dropped);
    let source = staged(Arc::clone(&schema), move || {
        Box::pin(
            StagedStream::new(Arc::clone(&staged_schema), vec![partial.clone()])
                .error("fused frame input failed")
                .dropped(&staged_dropped),
        )
    });
    let tree = rate_tree();
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(schema)],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let exec = plan_of(node, source).unwrap();
    let mut output = exec
        .execute(0, SessionContext::default().task_ctx())
        .unwrap();
    let error = match output.next().await {
        Some(Err(error)) => error,
        Some(Ok(_)) => panic!("unexpected output before the upstream failure"),
        None => panic!("the upstream failure was swallowed"),
    };
    assert!(error.to_string().contains("fused frame input failed"));
    assert!(
        dropped.load(Ordering::SeqCst),
        "the failed adapter releases input"
    );
    assert!(output.next().await.is_none(), "failure is yielded once");
    assert!(
        output.next().await.is_none(),
        "EOF remains stable after failure"
    );
}

/// A closed chain that holds no value still reaches the consumer of a frame.
///
/// The last sample sits past the grid, so no evaluation point has a sample in its window and the
/// window closes the series with a chunk that carries no position. Dropping it would lose the only
/// signal that says this series is done - the row table has no row for it at all, which is exactly
/// why the frame has to keep it.
#[tokio::test]
async fn a_frame_carries_the_closing_chunk_a_row_table_drops() {
    let schema = raw_schema();
    // Every sample is newer than the last grid point, so `rate` decides nothing at any of them.
    let late = [(GRID_START + RANGE_MS + SAMPLE_STEP, 1.0)];
    let chunk = chunk_of(
        &schema,
        "late",
        metric(&[(HOST, "late")], Some(METRIC)),
        &late.iter().map(|(time, _)| *time).collect::<Vec<_>>(),
        vec![late.iter().map(|(_, value)| Some(*value)).collect()],
        true,
    );
    let tree = rate_tree();
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .expect("a rate tree is evaluated on a grid");
    let exec = plan_of(node, memory_exec(vec![frame_of(&schema, &chunk)]))
        .expect("the runtime executes the tree");
    let declared = exec.schema();
    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("the frame runs");
    assert!(
        batches
            .iter()
            .all(|batch| batch.schema().as_ref() == declared.as_ref()),
        "every batch carries the declared schema"
    );
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        declared,
        futures::stream::iter(batches.into_iter().map(Ok)),
    ));
    let decoded = unpack_evaluated_series(stream, None)
        .expect("the node declares the frame it yields")
        .try_collect::<Vec<_>>()
        .await
        .expect("the frame decodes");

    assert_eq!(
        1,
        decoded.len(),
        "the closing chunk is one frame row, not a dropped chunk: {decoded:?}"
    );
    let (series, is_last) = &decoded[0];
    assert!(is_last, "that row is the one that closes the chain");
    assert!(
        series.time.is_empty(),
        "it carries no grid position: {:?}",
        series.time.positions().collect::<Vec<_>>()
    );

    // The row table answers nothing for the same input, which is what makes this chunk worth keeping.
    let plain = plan(rate_tree(), memory_exec(vec![frame_of(&schema, &chunk)]))
        .expect("the runtime executes a tree");
    let rows = rows(
        &collect(plain, SessionContext::default().task_ctx())
            .await
            .expect("the row table runs"),
    );
    assert!(
        rows.is_empty(),
        "the row table drops the closing chunk entirely: {rows:?}"
    );
}

/// Two chains that close on one label set are both emitted, each closing itself.
///
/// A range function drops the metric name, so two different raw series can reach the output under the
/// same complete labels. A frame carries one chain per chunk and the later one is a further
/// contribution, not a repeat of the first: refusing it as a duplicate identity would drop values the
/// query answers with.
#[tokio::test]
async fn two_chains_that_close_on_one_label_set_each_keep_their_frame() {
    // `rate` drops `__name__`, and neither series declares any other tag, so both collapse to the
    // same empty label set.
    let schema = raw_schema();
    let chunk_of_host = |host: &str, scale: f64| {
        let samples = series_samples(scale);
        chunk_of(
            &schema,
            host,
            metric(&[], None),
            &samples.iter().map(|(time, _)| *time).collect::<Vec<_>>(),
            vec![samples.iter().map(|(_, value)| Some(*value)).collect()],
            true,
        )
    };
    let partitions = vec![
        vec![frame_of(&schema, &chunk_of_host("web-1", 1.0))],
        vec![frame_of(&schema, &chunk_of_host("web-2", 2.0))],
    ];
    let tree = rate_tree();
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .expect("a rate tree is evaluated on a grid");
    let exec =
        plan_of(node, memory_exec_partitioned(partitions)).expect("the runtime executes the tree");
    assert_eq!(
        2,
        exec.properties().partitioning.partition_count(),
        "the frame keeps the input's partition count"
    );
    let declared = exec.schema();
    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("both chains are legal");
    let decoded = unpack_evaluated_series(
        Box::pin(RecordBatchStreamAdapter::new(
            declared,
            futures::stream::iter(batches.into_iter().map(Ok)),
        )),
        None,
    )
    .expect("the node declares the frame it yields")
    .try_collect::<Vec<_>>()
    .await
    .expect("the frame decodes");

    assert_eq!(
        2,
        decoded.iter().filter(|(_, last)| *last).count(),
        "each chain closes once, in its own partition"
    );
    assert_eq!(
        decoded[0].0.metric, decoded[1].0.metric,
        "the two chains carry the one label set"
    );
    let mut per_chain = Vec::new();
    for (series, last) in &decoded {
        assert!(*last, "each partition's single chain closes exactly once");
        assert_eq!(None, series.metric.name(), "rate drops the metric name");
        assert!(
            series.metric.tags().is_empty(),
            "both chains have empty labels"
        );
        let mut rows = Vec::new();
        for (position, row) in series.time.positions() {
            let value = match series.fields.as_slice()[0].sample(row) {
                datatypes::series::samples::SampleRef::Float(value) => value,
                _ => panic!("each rate grid position has a present value"),
            };
            rows.push((position, value.to_bits()));
        }
        rows.sort_unstable_by_key(|(position, _)| *position);
        per_chain.push(rows);
    }
    assert_eq!(2, per_chain.len(), "one closing chain from each partition");
    per_chain.sort();
    for (rows, value) in per_chain.iter().zip([1.0f64, 2.0]) {
        assert_eq!(
            (0..GRID_LEN as usize)
                .map(|i| (i, value.to_bits()))
                .collect::<Vec<_>>(),
            *rows,
            "each chain preserves every exact grid position and value",
        );
    }
}
