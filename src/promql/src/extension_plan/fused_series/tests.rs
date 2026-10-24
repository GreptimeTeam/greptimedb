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
//! One actual source is what the runtime runs: every tree input of the admitted trees is bound to
//! source `0`, and the one child's chunks are handed to each of them. A tree that names a source
//! index no child covers, or a plan given more than one child, is refused where the plan is built
//! rather than run against half of its input.
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
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, collect, displayable,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner};
use datafusion::prelude::SessionContext;
use datatypes::series::frame::{RawLabelMode, pack_raw_series_chunk, raw_series_frame_schema};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use futures::{Stream, StreamExt, TryStreamExt};
use store_api::region_engine::RawLabelSource;

use super::{
    FusedSeries, FusedSeriesBinding, FusedSeriesExec, LabelSets, OutputColumn, POLL_BUDGET,
    label_set, occupied_runs, series_to_record_batch,
};
use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::PromExtensionPlanner;
use crate::extension_plan::aggregate_exchange::state_schema;
use crate::native::execute::{NativeExecutionState, NativeExecutor};
use crate::native::metrics::{NodeMetric, NodePath};
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundScalar,
    BoundWindow, Grouping, NativeExpr, NativeNode, NativeTree, NativeType,
};
use crate::native::runs::absorb_rebuilds;
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};

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
    rate_tree_at(schema, 0)
}

/// [`rate_tree_of`] with its input bound to the child at `source_index`.
///
/// The input declares `Contiguous`, which is what every plan these fixtures build presents
/// ([`plan_of`]): a tree is legal only against frames that satisfy the ordering each of its inputs
/// declares, so a fixture declares the ordering its own plans really hand over. A test that needs
/// another declaration rewrites it on a clone with [`declaring_orderings`].
fn rate_tree_at(schema: Arc<SeriesSchema>, source_index: usize) -> NativeTree {
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
                    source_index,
                    output: raw.clone(),
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
/// subtracting tree answers `left - right`. One source read at both operands is what a statement
/// that names one selector twice plans to; two different indexes are what a statement whose
/// operands are two scans plans to, which the runtime refuses for now. The tree is the same shape
/// either way, and the renderer names the index each input reads, so the trees are distinct
/// computations.
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
            | NativeExpr::Map { input, .. }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. } => {
                **input = rewrite(input, orderings, seen);
            }
            NativeExpr::Binary { left, right, .. } => {
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
                            source_index: 0,
                            output: raw.clone(),
                            ordering: SeriesOrdering::Contiguous,
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
                    source_index: 0,
                    output: raw.clone(),
                    ordering: SeriesOrdering::Contiguous,
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

/// The grid `tree` is evaluated on, which is the grid its binding declares.
fn tree_grid(tree: &NativeTree) -> Arc<EvalGrid> {
    Arc::clone(
        tree.root()
            .output
            .grid()
            .expect("these trees produce an evaluated value"),
    )
}

/// The tags the output reports: the series' host and the metric name.
fn default_tags() -> Vec<String> {
    vec![HOST.to_string(), NAME.to_string()]
}

/// The binding of these tests' scans over `tree`, reporting `tags` as the output's tag columns.
fn binding_of(tree: &NativeTree, tags: Vec<String>) -> FusedSeriesBinding {
    FusedSeriesBinding {
        grid: tree_grid(tree),
        time_index: TIME.to_string(),
        tags,
        metric_name: Some(Arc::from(METRIC)),
    }
}

/// The binding of the one-field schema of these tests, read off the tree itself.
fn binding(tree: &NativeTree) -> FusedSeriesBinding {
    binding_of(tree, default_tags())
}

/// One logical child: the plan a scan stands in for when the caller is not testing the plans below.
fn child() -> LogicalPlan {
    input_plan(frame_schema())
}

/// `count` logical children, each on the frame schema of these tests.
fn children(count: usize) -> Vec<LogicalPlan> {
    vec![child(); count]
}

/// The logical node of `tree` over `children`, with the grid the tree was evaluated on.
fn logical(tree: &NativeTree, children: Vec<LogicalPlan>) -> DataFusionResult<FusedSeries> {
    FusedSeries::try_new(
        Arc::from("a + on(host) a"),
        tree.clone(),
        binding(tree),
        children,
        output_schema().to_dfschema_ref().unwrap(),
    )
}

/// The schema of a frame carrying series of `schema` with their labels explicit.
fn frame_schema_of(schema: &SeriesSchema) -> SchemaRef {
    raw_series_frame_schema(schema, RawLabelMode::Explicit).expect("explicit labels are a frame")
}

/// The schema of a frame carrying these series, which is what the node declares as its input.
fn frame_schema() -> SchemaRef {
    frame_schema_of(&raw_schema())
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

/// A plan that records the partition of every `execute` call it is asked for.
///
/// One actual source is read once per partition, so what the node asked its child for is the
/// evidence that it neither read a partition twice nor answered a series from one execution in the
/// name of another.
struct ExecuteRecorder {
    input: Arc<dyn ExecutionPlan>,
    calls: Arc<Mutex<Vec<usize>>>,
}

impl ExecuteRecorder {
    fn counting(
        input: Arc<dyn ExecutionPlan>,
        calls: &Arc<Mutex<Vec<usize>>>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            input,
            calls: Arc::clone(calls),
        })
    }
}

impl std::fmt::Debug for ExecuteRecorder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecuteRecorder").finish()
    }
}

impl DisplayAs for ExecuteRecorder {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for ExecuteRecorder {
    fn name(&self) -> &str {
        "ExecuteRecorder"
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(partition);
        self.input.execute(partition, context)
    }
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
        tree.clone(),
        binding,
        vec![child()],
        output_schema().to_dfschema_ref().unwrap(),
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

/// One missing sample per timestamp of `rows`, which is a field the series holds no value in.
fn no_values(rows: &[(i64, f64)]) -> Vec<Option<f64>> {
    rows.iter().map(|_| None).collect()
}

/// One chunk of `metric`, keyed `key`, holding `times` and one sample per field of `schema`.
fn chunk_of(
    schema: &SeriesSchema,
    key: &str,
    metric: MetricName,
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
fn metric(labels: &[(&str, &str)], name: Option<&str>) -> MetricName {
    MetricName::try_new(
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
    let mut batches = pack_raw_series_chunk(schema, RawLabelMode::Explicit, chunk)
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
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        binding_of(
            &tree,
            vec![HOST.to_string(), ZONE.to_string(), NAME.to_string()],
        ),
        vec![child()],
        zone_output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the binding matches the tree")
}

/// The instant-selection node over frames that carry `zone` as well as `host`.
///
/// Its output labels are the input's, so an empty-valued label reaches this node's own state as the
/// frame wrote it.
fn instant_node() -> FusedSeries {
    let tree = instant_tree();
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        binding_of(
            &tree,
            vec![HOST.to_string(), ZONE.to_string(), NAME.to_string()],
        ),
        vec![child()],
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

/// A waker a test can ask whether it was notified.
///
/// The remaining poll test uses it to see that a poll which stopped on its budget wakes the
/// consumer, which is what makes a stopped poll resumable rather than a lost one.
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

/// The gauge `name` reports, which must be one the node records.
fn gauge(metrics: &MetricsSet, name: &str) -> usize {
    match metrics.sum_by_name(name) {
        Some(MetricValue::Gauge { gauge, .. }) => gauge.value(),
        other => panic!("no gauge named {name}: {other:?}"),
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
        text.contains("input Input source=0 raw samples"),
        "the node line carries the tree's source and the child it reads: {text}"
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

/// One child read at both operands of `x - 2 * x` answers `-x`, so the operands cannot be swapped.
///
/// A statement that names one selector twice plans to one scan and two tree inputs, and the tree
/// hands every chunk of that scan to both of them. What each input reads is its own
/// `source_index`, so both read the one child and no per-child list of destinations exists to
/// disagree with the tree. The subtraction and the scalar multiplier together make the answer
/// order-sensitive: `x - 2 * x` is `-x` at every grid point, while the same samples paired the other
/// way round answer `+x`. `x - x` would answer zero whichever way it was paired and could not tell.
#[tokio::test]
async fn one_child_read_at_both_operands_answers_a_scalar_scaled_subtraction() {
    let tree = subtracted_scaled_tree();
    let node = logical(&tree, vec![child()]).expect("one child covers both of the tree's inputs");
    assert_eq!(1, node.children().len(), "one actual source");

    let samples = series_samples(1.0);
    let exec = plan_of(
        node,
        memory_exec(frames(&[series_chunk("h1", &samples, true)])),
    )
    .expect("the physical node builds");
    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("the one partition answers");
    let rows = rows(&batches);
    let expected = grid_timestamps()
        .into_iter()
        .map(|stamp| {
            let sample = samples
                .iter()
                .find_map(|(time, value)| (*time == stamp).then_some(*value))
                .expect("the samples cover every grid point");
            (stamp, Some(-sample), Some("h1".to_string()))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        expected,
        rows.iter()
            .map(|row| (row.0, row.1, row.2.clone()))
            .collect::<Vec<_>>(),
        "the left operand minus the doubled right one, not the other way round"
    );
}

/// A second child is refused, even when each child has one partition.
///
/// Pairing two children's series by label needs an exchange that merges them, which this node does
/// not have: one execution would read both children's partitions out of order behind the tree's
/// back. The refusal is on the count and not on the partitioning, so two children of one partition
/// each are refused exactly like two children of several. The tree below is the two-source shape
/// such a query plans to, and it is refused where the execution plan is built.
#[test]
fn a_second_child_is_refused() {
    let tree = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let node = logical(&tree, children(2)).expect("the tree names both of its children");
    let rows = frames(&[series_chunk("h1", &series_samples(1.0), true)]);
    let error = node
        .to_execution_plan(vec![
            (
                memory_exec(rows.clone()),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
            (
                memory_exec(rows),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
        ])
        .expect_err("two children would have to be paired by label");
    let message = error.to_string();
    assert!(
        message.contains("2 sources"),
        "the refusal must name how many sources it would run: {message}"
    );
    assert!(
        message.contains("exchange"),
        "the refusal must name what is missing: {message}"
    );
}

/// One source feeding both operands is read once and answered as `x + x`, not as `x`.
///
/// This is the shape a statement that names one selector twice plans to: one scan and two tree
/// inputs. The tree sees the same samples at both operands, so every grid point holds the sample
/// summed with itself. Answering `x` is what a node that queued the chunk at only one of the two
/// inputs would produce, and that is what this test separates from the sum.
#[tokio::test]
async fn one_source_read_at_both_operands_answers_x_plus_x() {
    let tree = binary_tree(raw_schema(), raw_schema());
    let node = logical(&tree, vec![child()]).expect("one source covers both of the tree's inputs");
    assert_eq!(1, node.children().len(), "one actual source");

    let samples = series_samples(1.0);
    let exec = plan_of(
        node,
        memory_exec(frames(&[series_chunk("h1", &samples, true)])),
    )
    .expect("the physical node builds");
    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("the one partition answers");
    let rows = rows(&batches);
    let expected = grid_timestamps()
        .into_iter()
        .map(|stamp| {
            let sample = samples
                .iter()
                .find_map(|(time, value)| (*time == stamp).then_some(*value))
                .expect("the samples cover every grid point");
            (stamp, Some(2.0 * sample), Some("h1".to_string()))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        expected,
        rows.iter()
            .map(|row| (row.0, row.1, row.2.clone()))
            .collect::<Vec<_>>(),
        "every grid point must hold the sample of the series plus itself, on the label it carries"
    );
}

/// A source read at both inputs is executed once per partition, and each partition is its own.
///
/// Each of the `partitions` series sits in a partition of its own in one scan, so each execution of
/// the tree owns one series and reads it at both operands. Three things are checked at once: the
/// source is asked for each partition exactly once (a second execution in one partition would read
/// the series twice), the count the node declares is the count it is given rather than a fixed one,
/// and each partition's peak is reported under its own name, which is what a figure that must not be
/// summed across partitions looks like.
///
/// The count is checked at two values on purpose: a node that ran only the first two partitions, or
/// that answered a partition from a hardcoded executors-per-source figure, reads as correct at one.
#[tokio::test]
async fn one_source_read_at_both_operands_is_executed_once_per_partition() {
    for partitions in [2_usize, 4] {
        let tree = binary_tree(raw_schema(), raw_schema());
        let node =
            logical(&tree, vec![child()]).expect("one source covers both of the tree's inputs");
        let scales = (1..=partitions).map(|n| n as f64).collect::<Vec<_>>();
        let source_partitions = scales
            .iter()
            .enumerate()
            .map(|(index, scale)| {
                frames(&[series_chunk(
                    &format!("web-{index}"),
                    &series_samples(*scale),
                    true,
                )])
            })
            .collect::<Vec<_>>();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let source = ExecuteRecorder::counting(memory_exec_partitioned(source_partitions), &calls);
        let exec = plan_of(node, source).expect("the physical node builds");
        assert_eq!(
            partitions,
            exec.properties().output_partitioning().partition_count(),
            "the node's count is its one source's"
        );

        // The optimizer leaves the child alone at this count as well, so the series each execution
        // owns still arrives whole: an exchange under the node would move half of one to another.
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = partitions;
        let optimized = EnsureRequirements::new()
            .optimize(Arc::clone(&exec), &config)
            .expect("the rule accepts the plan");
        let exchanges = exchanges_between(&optimized, &exec);
        assert!(
            exchanges.is_empty(),
            "the optimizer inserted {exchanges:?} under a {partitions}-partition source"
        );

        let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
            .await
            .expect("each partition runs its own tree");
        let actual = rows(&batches);
        assert_eq!(
            partitions * GRID_LEN as usize,
            actual.len(),
            "{partitions} partitions: {actual:?}"
        );
        for (index, scale) in scales.iter().enumerate() {
            let host = format!("web-{index}");
            let series = actual
                .iter()
                .filter(|row| row.2.as_deref() == Some(host.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(GRID_LEN as usize, series.len(), "{host}: {series:?}");
            for (row, stamp) in series.iter().zip(grid_timestamps()) {
                assert_eq!(stamp, row.0, "{host}");
                let value = row
                    .1
                    .unwrap_or_else(|| panic!("{host} at {stamp} holds no value"));
                let expected = 2.0 * scale * stamp as f64 / 1_000.0;
                assert!(
                    (expected - value).abs() < 1e-9,
                    "{host} at {stamp}: {value} is not {expected}"
                );
            }
        }
        // One execution per partition, and no partition executed twice.
        let mut called = calls.lock().unwrap_or_else(|e| e.into_inner()).clone();
        called.sort_unstable();
        assert_eq!(
            (0..partitions).collect::<Vec<_>>(),
            called,
            "{partitions} partitions were not executed once each"
        );
        // The peak of each partition is named after it, so the collector cannot sum one partition's
        // peak into another's and report a figure that is neither.
        let metrics = exec
            .metrics()
            .expect("the node reports its runtime's metrics");
        for partition in 0..partitions {
            let peak = gauge(
                &metrics,
                &NodePath::root()
                    .partitioned_metric_name(NodeMetric::PEAK_RESERVED_BYTES, partition),
            );
            assert!(
                peak > 0,
                "partition {partition} of {partitions} ran its tree but reported no peak"
            );
        }
    }
}

/// Two stateful windows over one child each receive every chunk of the series.
///
/// `rate(x[5m]) - rate(x[5m])` names one selector twice, so both windows are bound to source `0`
/// and one child's chunks are queued at both of them. A window never hands a chunk on, but it does
/// not hold a series either: it keeps only the samples its remaining windows still read and hands
/// out each grid point as the pushed samples complete its window. So the subtraction is not the
/// delivery proof: `rate - rate` is zero wherever both operands decided a point, and the counter
/// here advances one second per second, so a sample both windows missed costs neither one its
/// rate - identical omissions are invisible to a comparison of the two sides or to the values
/// themselves.
///
/// What rules omissions out is what the runtime reports per input: each one counts one
/// [`NodeMetric::INPUT_BATCHES`] per batch of the child ([`one_series_frames`] sends three) and one
/// completed series, so a window handed less of the stream than the other is visible even at the
/// points where its answer would have been right.
#[tokio::test]
async fn two_windows_over_one_child_each_read_the_whole_series() {
    // `rate(x[5m]) - rate(x[5m])` over one source: the two operands are the window's own root,
    // cloned so both halves share its bindings and its grid by allocation.
    let rate = rate_tree();
    let window = rate.root().clone();
    let output = crate::vector_match::binary_output_schema(
        window
            .output
            .samples()
            .expect("a window produces evaluated samples"),
        window
            .output
            .samples()
            .expect("a window produces evaluated samples"),
    )
    .expect("both operands are evaluated on the one grid");
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window.clone()),
            right: Box::new(window),
            matching: BinaryMatching {
                op: BinaryOp::Sub,
                return_bool: false,
                labels: MatchingLabels::On(vec![Arc::from(HOST)]),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output: NativeType::Samples(output),
    })
    .expect("two windows over one source are a legal tree");
    let node = logical(&tree, vec![child()]).expect("one child feeds both windows");
    assert_eq!(1, node.children().len(), "one actual source");

    let batches = one_series_frames();
    let chunks = batches.len();
    assert!(
        chunks > 1,
        "the fixture has to arrive in several chunks: {chunks}"
    );
    let calls = Arc::new(Mutex::new(Vec::new()));
    let source = ExecuteRecorder::counting(memory_exec(batches), &calls);
    let exec = plan_of(node, source).expect("the physical node builds");

    let answered = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("the one partition answers");
    let rows = rows(&answered);
    // The whole grid, in order, with the host `rate` kept and the name it dropped: `rate - rate`
    // is zero at every point both windows decided, which is the shape of the answer.
    let expected = grid_timestamps()
        .into_iter()
        .map(|stamp| (stamp, Some(0.0), Some("web-1".to_string()), None))
        .collect::<Vec<_>>();
    assert_eq!(expected, rows, "every grid point holds rate minus rate");

    // The one child was executed once, for the partition this node answers.
    let called = calls.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert_eq!(
        vec![0],
        called,
        "the child is executed once, at partition 0"
    );

    // Each window completed the one series it read, and the subtraction completed its one result.
    // A window that never received the closing chunk reports no completed series, so this is the
    // stateful half of the claim the rows above make.
    let metrics = exec
        .metrics()
        .expect("the node reports its runtime's metrics");
    for node in [
        NodePath::root(),
        NodePath::root().left(),
        NodePath::root().right(),
    ] {
        assert_eq!(
            1,
            count(&metrics, &node.metric_name(NodeMetric::COMPLETED_SERIES)),
            "{node} did not complete the one series it read"
        );
    }

    // Both inputs were fed the whole stream: one batch per batch of the child, which is what the
    // two windows' answers cannot say on their own.
    let left = NodePath::root().left().input();
    let right = NodePath::root().right().input();
    for input in [&left, &right] {
        assert_eq!(
            chunks,
            count(&metrics, &input.metric_name(NodeMetric::INPUT_BATCHES)),
            "{input} did not receive every chunk of the series"
        );
        assert_eq!(
            1,
            count(&metrics, &input.metric_name(NodeMetric::COMPLETED_SERIES)),
            "{input} did not close the one series it read"
        );
    }
}

/// One source read at two inputs must be read through one raw series schema.
///
/// Both inputs of this tree name source `0`, so one child's frames are decoded for both of them.
/// The two operands were compiled against different field lists: decoding the one frame either way
/// would hand the other operand a column it never bound, so the job is refused while it is built.
/// There is no per-input copy of the schema to mutate here - the schema each input reads is the one
/// its own node carries - so what is compared is the tree against itself.
#[test]
fn a_source_shared_by_both_inputs_must_read_one_schema() {
    let tree = binary_tree(raw_schema(), two_field_schema());
    let error =
        logical(&tree, vec![child()]).expect_err("one child cannot be decoded through two layouts");
    let message = error.to_string();
    assert!(
        message.contains("source 0"),
        "the refusal must name the source both inputs read: {message}"
    );
    assert!(
        message.contains("layout"),
        "the refusal must say why one child cannot serve both: {message}"
    );
}

/// A tree input that is not raw samples is refused by the logical node.
///
/// The right operand below reads evaluated samples at source `0` - the shape an exchange boundary
/// hands over - while the left one reads raw samples from the same child, and the tree is otherwise
/// valid: both sides are on one grid and the binary operation is legal. This node decodes raw series
/// frames, so every input it runs has to be raw; the tree is refused where the node is created,
/// before any execution plan exists to run.
#[test]
fn a_tree_input_that_is_not_raw_series_is_refused() {
    let grid = grid();
    let evaluated = evaluated_of(&raw_schema(), &grid);
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(instant_of(0, &grid)),
            right: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index: 0,
                    output: evaluated.clone(),
                    ordering: SeriesOrdering::Contiguous,
                }),
                output: evaluated.clone(),
            }),
            matching: BinaryMatching {
                op: BinaryOp::Add,
                return_bool: false,
                labels: MatchingLabels::On(vec![Arc::from(HOST)]),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output: evaluated,
    })
    .expect("an evaluated input is a legal tree: it is what an exchange boundary hands over");
    let error = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![child()],
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect_err("the node decodes raw frames, so an evaluated input cannot be run");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    let message = error.to_string();
    // The operand *is* the input node - nothing sits below it - so the walk reaches it at
    // `root.right`, not at a `root.right.input` that does not exist in this tree.
    assert!(
        message.contains("`root.right`"),
        "the refusal must name the input node that is not raw: {message}"
    );
    assert!(
        !message.contains("`root.right.input`"),
        "an input node is its own position, so the refusal cannot name a node below it: {message}"
    );
    assert!(
        message.contains("not raw series"),
        "the refusal must say what the input has to be: {message}"
    );
}

/// The ordering a tree input declares has to be met by the frames its child produces.
///
/// This is the whole matrix of the two facts: what an input declares, and what the producer
/// promises. `Contiguous` frames satisfy an input bound to `Contiguous` and one bound to the
/// stronger `KeyThenTime` does not - contiguous says only that a series' chunks are adjacent, not
/// that the series arrive in identity order. `KeyThenTime` frames satisfy both, because they are
/// the stronger promise. `Interleaved` frames satisfy neither: they may split one series' chunks
/// with another's, and both admitted inputs are read one series to its last chunk. The refusal
/// names the source, the ordering the producer promises, the one the input asked for, and the input
/// itself.
#[test]
fn the_declared_ordering_must_be_met_by_the_producer() {
    let contiguous = SeriesOrdering::Contiguous;
    let key_then_time = SeriesOrdering::KeyThenTime;
    let interleaved = SeriesOrdering::Interleaved;
    // (declared by the tree input, promised by the producer, admitted)
    let cases = [
        (contiguous, contiguous, true),
        (contiguous, key_then_time, true),
        (key_then_time, key_then_time, true),
        (key_then_time, contiguous, false),
        (contiguous, interleaved, false),
        (key_then_time, interleaved, false),
    ];
    for (declared, actual, accepted) in cases {
        let tree = declaring_orderings(&rate_tree(), &[declared]);
        let node = logical(&tree, vec![child()]).expect("the binding matches the tree");
        let built = node.to_execution_plan(vec![(
            memory_exec(Vec::new()),
            RawLabelSource::Explicit,
            actual,
        )]);
        match (accepted, built) {
            (true, Ok(exec)) => assert_eq!(
                1,
                exec.properties().output_partitioning().partition_count(),
                "{actual:?} frames satisfy an input bound to {declared:?}"
            ),
            (true, Err(error)) => {
                panic!("{actual:?} frames were refused for an input bound to {declared:?}: {error}")
            }
            (false, Ok(_)) => panic!(
                "{actual:?} frames were accepted for an input bound to {declared:?}, which asks \
                 for more than they promise"
            ),
            (false, Err(error)) => {
                assert!(
                    matches!(error, DataFusionError::Plan(_)),
                    "{declared:?} against {actual:?}: {error}"
                );
                let message = error.to_string();
                assert!(
                    message.contains("ordering"),
                    "the refusal must say what it is about: {message}"
                );
                assert!(
                    message.contains("source 0"),
                    "the refusal must name the source: {message}"
                );
                assert!(
                    message.contains(&format!("{actual:?}")),
                    "the refusal must name the ordering the producer promises: {message}"
                );
                assert!(
                    message.contains(&format!("{declared:?}")),
                    "the refusal must name the ordering the input asked for: {message}"
                );
                assert!(
                    message.contains("`root.input`"),
                    "the refusal must name the input that asked for it: {message}"
                );
            }
        }
    }
}

/// Every input's ordering is checked, not only the first one's.
///
/// Both operands of this tree are fed by the one child - `x + x` over one scan - but the left one
/// was compiled against contiguous frames and the right one against identity-then-time frames. A
/// check that read only the first input would admit this plan and then run the right operand over
/// frames it never bound. The refusal names the right input, and the same tree builds over frames
/// that really are identity-then-time, which is what makes it about the promise and not the shape.
#[test]
fn every_inputs_ordering_is_checked_not_only_the_first() {
    let tree = declaring_orderings(
        &binary_tree(raw_schema(), raw_schema()),
        &[SeriesOrdering::Contiguous, SeriesOrdering::KeyThenTime],
    );
    let node = logical(&tree, vec![child()]).expect("the binding matches the tree");
    let error = node
        .to_execution_plan(vec![(
            memory_exec(Vec::new()),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .expect_err("the right operand asked for more than the child promises");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    let message = error.to_string();
    assert!(
        message.contains("source 0"),
        "the refusal must name the source both inputs read: {message}"
    );
    assert!(
        message.contains("`root.right.input`"),
        "the refusal must name the input that asked for the stronger ordering, not the first \
         one: {message}"
    );
    assert!(
        message.contains("Contiguous") && message.contains("KeyThenTime"),
        "the refusal must name both orderings: {message}"
    );
    node.to_execution_plan(vec![(
        memory_exec(Vec::new()),
        RawLabelSource::Explicit,
        SeriesOrdering::KeyThenTime,
    )])
    .expect("identity-then-time frames satisfy both inputs");
}

/// A tree whose inputs name a source index past the sources it reads is refused.
///
/// The index is the input's own fact, so a tree that skips an index has no child to read it from:
/// source `1` with no source `0` would silently read the wrong child, and a source past the list
/// would read nothing at all. The refusal names the index.
///
/// Every index is bounded by the number of input occurrences before any per-index state is sized,
/// so the four indexes below - a skip of one, a sparse one, and the two extremes - are refused
/// rather than reserving slots no input can ever fill: were the bound checked after the sizing,
/// these cases would abort on the allocation instead of answering an error. The sparse index is a
/// shift of `usize::BITS` rather than a fixed literal, so it is out of any real source list's range
/// on a 32-bit target as well as a 64-bit one.
#[test]
fn a_source_index_without_the_sources_below_it_is_refused() {
    for index in [1, 1usize << (usize::BITS / 2), usize::MAX / 2, usize::MAX] {
        let tree = rate_tree_at(raw_schema(), index);
        let error = match logical(&tree, vec![child()]) {
            Ok(_) => panic!("source {index} has no source 0 below it, yet the tree was admitted"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains(&format!("source index {index}")),
            "the refusal must name the index the tree binds: {message}"
        );
    }
    // The same tree with its input bound to the only child it has is legal, which is what makes the
    // refusal about the index and not about the number of children.
    logical(&rate_tree_at(raw_schema(), 0), vec![child()]).expect("source 0 is the first child");
}

/// A child list that is not one child per source is refused, in both directions.
///
/// The children are the tree's sources, in source order, so a list that is too short leaves a
/// source unread and a list that is too long carries a child no input names. Both are refused
/// rather than zipped, which would drop whichever side is longer.
#[test]
fn a_tree_wants_one_child_per_source() {
    let two_sources = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let error = logical(&two_sources, vec![child()]).expect_err("source 1 has no child");
    let message = error.to_string();
    assert!(
        message.contains("2 sources") && message.contains("1 child"),
        "the refusal must name both counts: {message}"
    );
    let one_source = rate_tree();
    let error = logical(&one_source, children(2)).expect_err("the second child is unreferenced");
    let message = error.to_string();
    assert!(
        message.contains("1 source") && message.contains("2 child"),
        "the refusal must name both counts: {message}"
    );
}

/// A tree that reads no source is legal only as the statement's constant scalar.
///
/// A scalar is declared by the type that produces it, so the one zero-input tree this node accepts
/// is a bound [`NativeExpr::Scalar`] on the statement's grid: it reads nothing and its rows are the
/// same value at every grid point. A scalar on another grid is a different value at every grid
/// point than the one the binding names, so it is refused; a tree that produces samples with no
/// input at all would be an empty vector reported as an answer, which is not a shape this node
/// invents. Nothing is read for the scalar yet, so asking for its physical node is refused rather
/// than answered with an empty result.
#[test]
fn a_zero_child_tree_must_be_a_constant_scalar_on_the_statement_grid() {
    let scalar = scalar_tree(grid(), 7.0);
    let node = FusedSeries::try_new(
        Arc::from("7"),
        scalar.clone(),
        binding(&scalar),
        Vec::new(),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("a constant scalar on the statement's grid reads no source");
    assert!(node.children().is_empty(), "the scalar binds no child");
    let error = node
        .to_execution_plan(Vec::new())
        .expect_err("a constant scalar's rows are not produced yet");
    assert!(
        matches!(
            error,
            DataFusionError::Plan(_) | DataFusionError::NotImplemented(_)
        ),
        "the refusal is explicit, not an empty answer: {error}"
    );

    // The same tree evaluated on another grid is not the statement's scalar.
    let elsewhere = scalar_tree(
        Arc::new(EvalGrid::try_new(GridSpec::try_new(0, GRID_STEP, GRID_LEN).unwrap()).unwrap()),
        7.0,
    );
    let error = FusedSeries::try_new(
        Arc::from("7"),
        elsewhere.clone(),
        FusedSeriesBinding {
            grid: binding(&scalar).grid,
            ..binding(&elsewhere)
        },
        Vec::new(),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect_err("the scalar is not evaluated on the grid the binding names");
    assert!(
        error.to_string().contains("constant scalar"),
        "the refusal must say the zero-child tree has to be a constant scalar: {error}"
    );
}

/// The child a tree input reads is part of what the tree is.
///
/// `a - b` over two scans and the same subtraction with the scans bound the other way round are the
/// same shape while still reading different children at each operand: what tells them apart is the
/// index that makes it part of the tree's own text, and that text is what the logical node compares
/// since the tree carries no structural equality of its own.
#[test]
fn a_source_index_is_part_of_the_tree() {
    let left_first = binary_tree_over_sources(BinaryOp::Sub, raw_schema(), raw_schema(), 0, 1);
    let right_first = binary_tree_over_sources(BinaryOp::Sub, raw_schema(), raw_schema(), 1, 0);
    assert!(
        left_first.to_string().contains("source=0") && left_first.to_string().contains("source=1"),
        "the tree's text names the source each input reads: {left_first}"
    );
    assert_ne!(
        left_first.to_string(),
        right_first.to_string(),
        "the two bindings of the same subtraction are different trees"
    );

    let mine = logical(&left_first, children(2)).expect("both sources have a child");
    let theirs = logical(&right_first, children(2)).expect("the same sources, bound the other way");
    assert_ne!(mine, theirs, "the swapped tree is another computation");
    assert_ne!(
        Some(std::cmp::Ordering::Equal),
        mine.partial_cmp(&theirs),
        "a different tree is not ordered equal to this one"
    );
}

/// The execution children are one per child, in both directions.
///
/// A zip would drop the longer side, so a caller that passes one child too many would have it
/// silently ignored and a caller that passes one too few would run a tree over half its sources.
#[test]
fn the_execution_children_must_be_one_per_child() {
    let tree = rate_tree();
    let node = fused(tree);
    let bare = || memory_exec(Vec::new());
    let error = node
        .to_execution_plan(vec![
            (bare(), RawLabelSource::Explicit, SeriesOrdering::Contiguous),
            (bare(), RawLabelSource::Explicit, SeriesOrdering::Contiguous),
        ])
        .expect_err("two execution children for one child");
    assert!(
        error.to_string().contains("2 execution inputs were given"),
        "{error}"
    );
    let error = node
        .to_execution_plan(Vec::new())
        .expect_err("no execution child for one child");
    assert!(
        error.to_string().contains("0 execution inputs were given"),
        "{error}"
    );
}

/// The physical inputs of the node must number its children, whoever plans it.
///
/// DataFusion plans every physical child before it asks for the node, so its own route always
/// matches the count. A direct call of `plan_extension` need not, and the children are paired with
/// the inputs by position, so a list of another length is a plan error naming both counts instead
/// of a child read past its end or an input silently ignored. The inputs here are memory sources,
/// which this node cannot read as raw series: that they are refused for their count and not for
/// their type is what shows the count is asked before a child is looked at, since the scan check
/// would have answered `Ok(None)` for both counts instead.
#[tokio::test]
async fn the_physical_inputs_must_number_the_children() {
    let node = fused(rate_tree());
    let planner = DefaultPhysicalPlanner::default();
    let session = SessionContext::default();
    let state = session.state();
    let planning_ctx = PhysicalPlanningContext::default();
    for count in [0, 2] {
        let inputs = (0..count)
            .map(|_| memory_exec(Vec::new()))
            .collect::<Vec<_>>();
        let error = PromExtensionPlanner
            .plan_extension(&planner, &node, &[], &inputs, &state, &planning_ctx)
            .await
            .expect_err("the node binds one child, whatever it was given");
        assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    }
    let planned = PromExtensionPlanner
        .plan_extension(
            &planner,
            &node,
            &[],
            &[memory_exec(Vec::new())],
            &state,
            &planning_ctx,
        )
        .await
        .expect("one input is the node's own arity");
    assert!(
        planned.is_none(),
        "a memory source is not a region scan: {planned:?}"
    );
}

/// The physical children must number the tree's sources.
///
/// The tree's own input nodes say how many children it reads, so a child list of another length is
/// refused where the physical node is built: a shorter one leaves a source unread, and the tree
/// would run over part of its input.
#[test]
fn the_physical_children_must_number_the_trees_sources() {
    let tree = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let error = FusedSeriesExec::try_new(
        Arc::from("a + on(host) b"),
        vec![(
            memory_exec(Vec::new()),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )],
        Arc::new(tree),
        output_schema(),
        TIME.into(),
        default_tags(),
    )
    .expect_err("the tree reads two sources and was given one child");
    let message = error.to_string();
    assert!(
        message.contains("2 sources") && message.contains("1 child"),
        "the refusal must name both counts: {message}"
    );
}

/// Rebuilding a node over new children keeps the arity and the fields they must provide.
///
/// A rewrite above this node replaces the plans below it and nothing else: the binding is the
/// node's own fact, so it travels unchanged, while the number of children and the fields each of
/// them must carry are asked again. A replacement child missing a field the tree reads is refused
/// rather than decoded with a stale layout.
#[test]
fn with_exprs_and_inputs_keeps_the_arity_and_the_fields() {
    let tree = binary_tree(raw_schema(), raw_schema());
    let node = logical(&tree, vec![child()]).expect("one child covers both of the tree's inputs");
    let rebuilt = node
        .with_exprs_and_inputs(vec![], vec![child()])
        .expect("one child still covers both of the tree's inputs");
    assert_eq!(
        node.binding(),
        rebuilt.binding(),
        "the binding must survive a rewrite above the node"
    );
    assert_eq!(
        node.schema(),
        rebuilt.schema(),
        "the columns the node reports, and their metadata, are the node's own"
    );
    let error = node
        .with_exprs_and_inputs(vec![], Vec::new())
        .expect_err("the one child cannot be dropped");
    assert!(error.to_string().contains("expected 1 inputs"), "{error}");
    // The replacement has to carry the field the tree's input reads.
    let other = input_plan(Arc::new(Schema::new(vec![Field::new(
        OTHER,
        DataType::Float64,
        true,
    )])));
    let error = node
        .with_exprs_and_inputs(vec![], vec![other])
        .expect_err("the replacement child has no `value` column");
    assert!(
        error.to_string().contains("no column"),
        "the refusal must name the missing column: {error}"
    );
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
    let error = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        FusedSeriesBinding {
            grid: other_grid,
            ..binding(&tree)
        },
        vec![child()],
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
    let _node = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        FusedSeriesBinding {
            grid: equal,
            ..binding(&tree)
        },
        vec![child()],
        output_schema().to_dfschema_ref().unwrap(),
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
    let keyed =
        raw_series_frame_schema(&raw_schema(), RawLabelMode::Key).expect("key labels are a frame");
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
/// row is addressed on is the tree's own. A schema that declares a grid is one this node would
/// read positions out of, so it is refused rather than decoded. The source below is one partition,
/// which is the shape the node accepts, so what refuses the plan here is the frame's own
/// declaration and not the node's partition count: the refusal has to name the grid.
#[test]
fn a_frame_with_an_evaluation_grid_is_rejected_when_the_plan_is_built() {
    let mut metadata = frame_schema().metadata().clone();
    metadata.insert("greptime.series.grid".to_string(), "v1".to_string());
    let gridded = Arc::new(Schema::new_with_metadata(
        frame_schema().fields().clone(),
        metadata,
    ));
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
        message.contains("raw series schema cannot have an evaluation grid"),
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
        error.to_string().contains("raw frame"),
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
        error.to_string().contains("has no partitions"),
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
            metric: MetricName::try_new(Some(Arc::from(METRIC)), Vec::new())
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
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema_of(&two_field_schema()))],
        two_field_output_schema().to_dfschema_ref().unwrap(),
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
    let series_of = |metric: MetricName| Series {
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

/// A binding is ordered by its grid, its time column, its tags and its metric, while a node is
/// ordered last by its output schema and refuses unequal DF schemas.
#[test]
fn unequal_binding_and_output_schema_are_not_ordered_equal() {
    let tree = rate_tree();
    let left = binding(&tree);
    let right = binding_of(&tree, default_tags());
    assert_eq!(left, right);
    assert_eq!(Some(std::cmp::Ordering::Equal), left.partial_cmp(&right));
    let named = FusedSeriesBinding {
        metric_name: Some(Arc::from("another_metric")),
        ..binding(&tree)
    };
    assert_ne!(left, named);
    assert_ne!(Some(std::cmp::Ordering::Equal), left.partial_cmp(&named));

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
        tree.clone(),
        binding(&rate_tree()),
        vec![child()],
        nullability_changed.to_dfschema_ref().unwrap(),
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

/// One chunk of the series keyed `key`, carrying `labels` and closing when `last`.
///
/// The key and the labels are separate so a test can hold two raw series under one label set: the
/// collision check reads the labels, and two different keys are what makes such a pair two series.
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
    binding_of(tree, tags.iter().map(|tag| (*tag).to_string()).collect())
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
/// both partitions' states; a ledger over the exported rows would refuse the second one. The prefix
/// labels are distinct on purpose: two partitions holding the same prefix labels would be a
/// collision, which the test below covers. The first partition's series also arrives in two chunks,
/// so the fold has to keep what it folded between them.
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

/// Two partitions whose prefix series share one label set and grid points are refused.
///
/// The aggregate would fold both into its group, and the merge above would take the two sets of
/// contributions for the whole group: a series that reached two partitions was never one series.
/// The two chunks are two raw series - distinct keys - that carry the *same* complete labels, which
/// is what a split series looks like once the scan has lost track of its identity.
#[tokio::test]
async fn a_repeated_prefix_label_set_across_partitions_is_refused_in_state_mode() {
    let rows = series_samples(1.0);
    let labels = [(HOST, "web")];
    let left = labelled_chunk("web-0", &labels, &rows, true);
    let right = labelled_chunk("web-1", &labels, &rows, true);
    assert_eq!(
        left.metric, right.metric,
        "the two chunks carry one label set"
    );
    assert_ne!(
        left.primary_key, right.primary_key,
        "the two chunks are two raw series"
    );
    let partitions = vec![vec![frame(&left)], vec![frame(&right)]];
    let exec = partial_plan(
        rate_tree(),
        memory_exec_partitioned(partitions),
        partial_op(AggregateKind::Sum, &[HOST]),
        &[HOST],
    )
    .expect("a rate tree with a partial sum is executable");

    // The ledger is the plan's, so the first partition has to finish claiming before the second
    // reaches the same label set.
    state_of(&exec, 0, &state_schema(&[HOST.to_string()])).await;
    let context = SessionContext::default().task_ctx();
    let error = exec
        .execute(1, Arc::clone(&context))
        .expect("the partition exists")
        .try_collect::<Vec<_>>()
        .await
        .expect_err("both partitions claim the same label set on the same points");
    assert!(
        error
            .to_string()
            .contains("vector cannot contain metrics with the same labelset"),
        "refused for another reason: {error}"
    );
}

/// An empty tag value and an absent tag are one label set, in state mode too.
#[tokio::test]
async fn an_empty_tag_and_an_absent_one_collide_across_partitions_in_state_mode() {
    let rows = series_samples(1.0);
    let empty = [(HOST, "web"), (JOB, "")];
    let absent = [(HOST, "web")];
    let partitions = vec![
        vec![frame(&labelled_chunk("web-0", &empty, &rows, true))],
        vec![frame(&labelled_chunk("web-1", &absent, &rows, true))],
    ];
    let exec = partial_plan(
        rate_tree(),
        memory_exec_partitioned(partitions),
        partial_op(AggregateKind::Sum, &[HOST]),
        &[HOST],
    )
    .expect("a rate tree with a partial sum is executable");

    state_of(&exec, 0, &state_schema(&[HOST.to_string()])).await;
    let context = SessionContext::default().task_ctx();
    let error = exec
        .execute(1, Arc::clone(&context))
        .expect("the partition exists")
        .try_collect::<Vec<_>>()
        .await
        .expect_err("an empty value is no label, so both partitions claim one label set");
    assert!(
        error
            .to_string()
            .contains("vector cannot contain metrics with the same labelset"),
        "refused for another reason: {error}"
    );
}

/// Prefix series of one label set on disjoint grid points are legal in state mode.
///
/// Only a shared point is a collision: two fragments that fill different points of one label set are
/// both kept in the ordinary mode, and the exported state is where they legitimately meet.
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

/// Exported state stays charged until the stream that queued it ends, not until the plan does.
///
/// The fold is done by the time the state is queued, so the bytes it exported outlive it: the plan
/// that owns the reservation may already be dropped while the stream still holds batches a consumer
/// has not read. What keeps them paid for is the stream's own share of the plan's reservation, so the
/// pool only returns to zero when the stream itself is dropped - and a batch that has left the stream
/// is no longer charged, because a record batch cannot be charged after the plan and stream that
/// declared it are gone.
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
    let mut stream = exec
        .execute(0, Arc::clone(&context))
        .expect("the partition exists");
    // The plan is not needed to read what it already queued.
    drop(exec);

    let batches = (&mut stream)
        .try_collect::<Vec<_>>()
        .await
        .expect("the partition's fold ends");
    assert_eq!(1, batches.len(), "the state of one partition is one batch");
    assert!(
        pool.reserved() > 0,
        "the exported batch is charged while its stream lives"
    );

    drop(stream);
    assert_eq!(
        0,
        pool.reserved(),
        "the stream's end returns everything the plan still held"
    );
    assert_eq!(1, batches.len(), "the batches read are still the test's");
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

/// An input that fails after rows exports no state, and a partition the input lacks is refused.
#[tokio::test]
async fn a_failed_input_exports_no_partial_state() {
    let schema = frame_schema();
    let batches = one_series_frames();
    let source = staged(schema.clone(), move || {
        Box::pin(StagedStream::new(schema.clone(), batches.clone()).error("the source failed"))
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
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");
    match stream.next().await {
        Some(Err(error)) => assert!(
            error.to_string().contains("the source failed"),
            "reported another error: {error}"
        ),
        other => panic!("a failed input exported state: {other:?}"),
    }

    let error = match exec.execute(1, SessionContext::default().task_ctx()) {
        Ok(_) => panic!("a partition the input does not have is answered"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("partition"),
        "refused for another reason: {error}"
    );
}
