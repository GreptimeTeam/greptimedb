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

use std::collections::{BTreeSet, VecDeque};
use std::fmt::{self, Display, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_query::native_histogram::NativeHistogram;
use common_query::series_frame::{
    frame_schema_with, pack_evaluated_series, unpack_evaluated_series, unpack_raw_series,
};
use common_recordbatch::adapter::{
    NATIVE_TREE_METRIC, NativeTreeMetric, NativeTreeMetrics, format_metric,
};
use common_recordbatch::{
    EmptyRecordBatchStream, SendableRecordBatchStream as CommonSendableRecordBatchStream,
};
use datafusion::arrow::array::{
    Array, Float64Array, StringArray, TimestampMillisecondArray, UInt32Array,
};
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
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, collect, displayable,
};
use datafusion::prelude::SessionContext;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaRef as DatatypesSchemaRef};
use datatypes::series::aggregate_state::{AggregateStateKind, StateComponent};
use datatypes::series::frame::{
    FrameContent, FrameLabelMode, LABEL_DECODE_VERSION, LabelDecodeColumn, LabelDecodeEncoding,
    LabelDecodeSpec, LabelDecodeTag, SeriesFrameDeclaration, StateFrameRow, pack_raw_series_chunk,
    raw_series_frame_schema, series_frame_schema,
};
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, SeriesLabels, SeriesOrdering};
use futures::{Stream, StreamExt, TryStreamExt};
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::region_engine::{
    BoundTag, PrepareRequest, QueryScanContext, RawLabelSource, RegionScanner, ScannerProperties,
};
use store_api::storage::{RegionId, ScanRequest};
use table::table::scan::RegionScanExec;

mod frame_output;

use super::{
    FieldKind, FusedOutputFormat, FusedSeries, FusedSeriesBinding, FusedSeriesExec, OutputColumn,
    POLL_BUDGET, canonical_histogram, series_to_record_batch,
};
use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::raw_series_source::tests::{per_series_scan, raw_source};
use crate::extension_plan::series_execution::{SeriesChunk, series_execution_plan};
use crate::extension_plan::{RawSeriesSourceExec, SeriesRepartitionExec, SeriesRouting};
use crate::histogram_eval::HistogramFunction;
use crate::info::InfoSelector;
use crate::map_label::LabelRewrite;
use crate::native::execute::{InputId, NativeExecutionState, NativeExecutor};
use crate::native::metrics::{NodeMetric, NodeMetricsSnapshot, is_node_metric};
use crate::native::plan::{
    AggregateKind, AggregateStage, AggregateStateSchema, BoundAggregate, BoundInput, BoundInstant,
    BoundMap, BoundScalar, BoundVectorOp, BoundWindow, Grouping, InstantValue, MapOp, NativeExpr,
    NativeNode, NativeTree, NativeType, ParameterizedOp,
};
use crate::native::state::{StateChunk, state_bytes};
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};
use common_query::series_frame::series_frame_ordering;
use common_query::series_frame::unpack_aggregate_state;

/// The query every tree here is compiled from, which names the value columns it publishes.
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

/// [`rate_tree`] with its window function replaced by `function`.
///
/// Only the kernel changes: the input stays raw samples and the output stays the same evaluated
/// schema, so a tree built here differs from [`rate_tree`] in the one thing a function decides -
/// whether the metric name survives its window.
fn window_tree_of(function: RangeFunction) -> NativeTree {
    let mut root = rate_tree().root().clone();
    let NativeExpr::Window { op, .. } = &mut root.expr else {
        panic!("the rate fixture is a window root");
    };
    op.function = function;
    NativeTree::try_new(root).expect("a window function keeps the raw input and its grid")
}

/// `rate(x[5m])` as the canonical pass hands it over: one merge over the window's output.
///
/// A range function drops the metric name, so two series that agreed on every other label leave the
/// window under one label set. Collecting them is what refuses a second value at one position: the
/// merge groups by the label set the window handed out and reports the collision, as Prometheus
/// refuses two series of one label set.
fn canonical_rate_tree_of(schema: Arc<SeriesSchema>) -> NativeTree {
    let window = rate_tree_of(schema);
    let output = window.root().output.clone();
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(window.root().clone()),
        },
        output,
    })
    .expect("a merge over a window on a grid is a legal tree")
}

/// A canonical merge on the aggregate's input side refuses the split.
///
/// The merge answers one series per canonical label set from every piece of that set, and the pieces
/// of one set are not in one partition: the rewrite that created them ran after the scan split the
/// rows. A split keeps the merge in its per-partition prefix, where it sees only the pieces its own
/// partition holds, so two colliding series that landed in different partitions are each collected
/// apart and the final half adds their states - a value where Prometheus reports
/// `vector cannot contain metrics with the same labelset`.
///
/// So the split refuses and the statement keeps the plan a collected subtree gets: the merge is the
/// boundary, its producer collects each partition's pieces, and the canonical router collects one
/// label set from every partition before anything folds it. That is the shape an overlap is reported
/// in, whether the aggregate is the statement root or sits under the merge.
#[test]
fn a_canonical_merge_below_the_aggregate_refuses_the_split() {
    let merged = canonical_rate_tree_of(raw_schema());
    let output = merged.root().output.clone();
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(merged.root().clone()),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::Without(Arc::from([])),
                stage: AggregateStage::Complete,
                field_kind: FieldKind::Float,
            },
        },
        output: output.clone(),
    })
    .expect("an aggregate over a collected window on a grid is a legal tree");
    // `split_aggregate` reads a refusal as "no split here", which is what leaves the merge above the
    // partitions instead of inside them.
    assert!(
        FusedSeries::split_aggregate(&tree, TIME)
            .unwrap_or_else(|error| panic!("a refused split is not a failure: {error}"))
            .is_none(),
        "a merge below the aggregate keeps the collected plan: {tree}"
    );
    let fields = crate::native::split::prefix_fields(&tree).expect("the prefix fields are bound");
    let error = match crate::native::split::split(tree.root(), &fields, TIME) {
        Ok(_) => panic!("a merge below the aggregate is not a prefix the split carries: {tree}"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("an aggregate split across a canonical merge"),
        "{error}"
    );

    // A merge *above* the aggregate is refused for the same reason, and by the same arm: applying it
    // to the merged state is what no arm of the split carries, and collecting the state it folds is
    // what the merge cannot do.
    let above = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(tree.root().clone()),
        },
        output,
    })
    .expect("a merge over an aggregate on a grid is a legal tree");
    let fields = crate::native::split::prefix_fields(&above).expect("the prefix fields are bound");
    let error = match crate::native::split::split(above.root(), &fields, TIME) {
        Ok(_) => panic!("a merge above the aggregate has no arm that carries it: {above}"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("an aggregate split across a canonical merge"),
        "{error}"
    );
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

/// A mixed-input binary: raw source 0 is rate-evaluated, while evaluated source 1 is already on `grid`.
fn mixed_raw_evaluated_tree() -> NativeTree {
    let grid = grid();
    let raw = raw_schema();
    let evaluated_schema = Arc::new(SeriesSchema {
        fields: raw.fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    });
    let evaluated = NativeType::Samples(Arc::clone(&evaluated_schema));
    let raw_type = NativeType::Samples(Arc::clone(&raw));
    let left_input = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: raw_type.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Contiguous,
        }),
        output: raw_type,
    };
    let left = NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(left_input),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: RANGE_MS,
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
    let right = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: evaluated.clone(),
            source_index: 1,
            ordering: SeriesOrdering::Contiguous,
        }),
        output: evaluated.clone(),
    };
    let output = crate::vector_match::binary_output_schema(&evaluated_schema, &evaluated_schema)
        .expect("same-grid samples can be matched");
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: BinaryMatching {
                op: BinaryOp::Add,
                return_bool: false,
                labels: MatchingLabels::On(vec![Arc::from(HOST)]),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output: NativeType::Samples(output),
    })
    .expect("raw-rate and evaluated samples share the output grid")
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

#[test]
fn duplicate_evidence_detects_binaries_in_dynamic_nodes() {
    use crate::histogram_eval::HistogramFunction;
    use crate::native::plan::BoundVectorOp;

    let grid = grid();
    let binary = binary_tree(raw_schema(), raw_schema()).root().clone();
    let scalar_of = |input| NativeNode {
        expr: NativeExpr::ScalarOf {
            input: Box::new(input),
        },
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let no_binary = instant_of(0, &grid);
    let literal = scalar_tree(Arc::clone(&grid), 1.0).root().clone();
    let topk = |input: NativeNode, parameter| NativeNode {
        output: input.output.clone(),
        expr: NativeExpr::ParameterizedTopK {
            input: Box::new(input),
            op: BoundVectorOp::TopK {
                k: 0.0,
                top: true,
                group: Grouping::By(Arc::from([])),
            },
            parameter: Box::new(parameter),
        },
    };
    let histogram = |input: NativeNode, parameters| NativeNode {
        output: input.output.clone(),
        expr: NativeExpr::ParameterizedHistogram {
            input: Box::new(input),
            op: BoundVectorOp::Histogram(HistogramFunction::Fraction {
                lower: 0.0,
                upper: 0.0,
            }),
            parameters,
        },
    };
    let has_binary = super::native_tree_has_vector_binary;
    assert!(has_binary(&scalar_of(binary.clone())));
    assert!(!has_binary(&scalar_of(no_binary.clone())));
    assert!(has_binary(&topk(binary.clone(), literal.clone())));
    assert!(has_binary(&topk(
        no_binary.clone(),
        scalar_of(binary.clone())
    )));
    assert!(!has_binary(&topk(no_binary.clone(), literal.clone())));
    assert!(has_binary(&histogram(
        binary.clone(),
        vec![literal.clone(), literal.clone()]
    )));
    assert!(has_binary(&histogram(
        no_binary.clone(),
        vec![literal.clone(), scalar_of(binary)]
    )));
    assert!(!has_binary(&histogram(
        no_binary,
        vec![literal.clone(), literal]
    )));
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
            NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {}
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. }
            | NativeExpr::VectorOf { input }
            | NativeExpr::ScalarOf { input } => {
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
            NativeExpr::ParameterizedHistogram {
                input, parameters, ..
            } => {
                **input = rewrite(input, orderings, seen);
                for parameter in parameters {
                    *parameter = rewrite(parameter, orderings, seen);
                }
            }
            NativeExpr::ParameterizedTopK {
                input, parameter, ..
            }
            | NativeExpr::ParameterizedAggregate {
                input, parameter, ..
            } => {
                **input = rewrite(input, orderings, seen);
                **parameter = rewrite(parameter, orderings, seen);
            }
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            } => {
                **input = rewrite(input, orderings, seen);
                for parameter in parameters {
                    *parameter = rewrite(parameter, orderings, seen);
                }
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

fn binary_with_abs_and_matching_key(key: &str) -> NativeTree {
    let tree = binary_tree(raw_schema(), raw_schema());
    let mut root = tree.root().clone();
    let NativeExpr::Binary { left, right, .. } = &mut root.expr else {
        unreachable!()
    };
    for side in [left, right] {
        let input = side.clone();
        let output = side.output.clone();
        **side = NativeNode {
            expr: NativeExpr::Map {
                input,
                op: BoundMap {
                    ops: Arc::from([MapOp::Math(crate::map::MathOp::Abs)]),
                },
            },
            output,
        };
    }
    let NativeExpr::Binary { matching, .. } = &mut root.expr else {
        unreachable!()
    };
    matching.labels = MatchingLabels::On(vec![Arc::from(key)]);
    NativeTree::try_new(root).unwrap()
}

fn scalar_operation_tree(key: &str, scalar_left: bool, op: BinaryOp) -> NativeTree {
    scalar_operation_tree_over_sources(key, scalar_left, op, 0, 0)
}

fn scalar_operation_tree_over_sources(
    key: &str,
    scalar_left: bool,
    op: BinaryOp,
    left_source: usize,
    right_source: usize,
) -> NativeTree {
    let tree = binary_tree_over_sources(
        BinaryOp::Add,
        raw_schema(),
        raw_schema(),
        left_source,
        right_source,
    );
    let mut root = tree.root().clone();
    let NativeExpr::Binary {
        left,
        right,
        matching,
    } = &mut root.expr
    else {
        unreachable!()
    };
    for side in [left, right] {
        let vector = (**side).clone();
        let scalar = NativeNode {
            expr: NativeExpr::Scalar(BoundScalar { value: 2.0 }),
            output: NativeType::Scalar(grid()),
        };
        **side = NativeNode {
            expr: NativeExpr::Binary {
                left: Box::new(if scalar_left {
                    scalar.clone()
                } else {
                    vector.clone()
                }),
                right: Box::new(if scalar_left { vector } else { scalar }),
                matching: BinaryMatching {
                    op,
                    return_bool: false,
                    labels: MatchingLabels::Ignoring(Vec::new()),
                    cardinality: Cardinality::OneToOne,
                    fill: FillValues::default(),
                },
            },
            output: side.output.clone(),
        };
    }
    matching.labels = MatchingLabels::On(vec![Arc::from(key)]);
    NativeTree::try_new(root).unwrap()
}

fn timestamp_match_tree() -> NativeTree {
    let tree = binary_tree(raw_schema(), raw_schema());
    let mut root = tree.root().clone();
    let NativeExpr::Binary {
        left,
        right,
        matching,
    } = &mut root.expr
    else {
        unreachable!()
    };
    for side in [left, right] {
        let NativeExpr::Instant { op, .. } = &mut side.expr else {
            unreachable!()
        };
        op.value = InstantValue::Timestamp;
    }
    matching.labels = MatchingLabels::On(vec![Arc::from("__name__")]);
    NativeTree::try_new(root).unwrap()
}

/// `instant_of(0, grid)` broadcast against the constant 2 with `op`.
///
/// A scalar has no labels, so the operation reaches each series of the vector side as it is; which
/// side the scalar is bound to changes nothing about the identity the node answers.
fn scalar_broadcast_tree(op: BinaryOp, scalar_left: bool) -> NativeTree {
    let grid = grid();
    let vector = instant_of(0, &grid);
    let output = vector.output.clone();
    let scalar = NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value: 2.0 }),
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let (left, right) = if scalar_left {
        (scalar, vector)
    } else {
        (vector, scalar)
    };
    NativeTree::try_new(NativeNode {
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
    })
    .expect("a scalar broadcast over one source is a legal tree")
}

/// `instant_of(0, grid)` with one `label_replace` writing `destination` into it.
fn label_rewrite_tree(destination: &str) -> NativeTree {
    let input = instant_of(0, &grid());
    let output = input.output.clone();
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(input),
            op: BoundMap {
                ops: Arc::from([MapOp::Label(Arc::new(
                    LabelRewrite::replace(destination, "fresh", HOST, ".*")
                        .expect("the rewrite is a legal label_replace"),
                ))]),
            },
        },
        output,
    })
    .expect("a label rewrite over one source is a legal tree")
}

/// The labels the per-series fixture scan binds: one metric named `cpu` keyed by `host`.
fn fixture_labels() -> super::StoredSeriesLabels {
    super::StoredSeriesLabels::of(&fixture_binding(HOST))
        .expect("the fixture binding states labels")
}

/// A per-series binding of one metric whose single tag - and so its primary key - is `name`.
fn fixture_binding(name: &str) -> RawLabelSource {
    fixture_binding_of(name, false)
}

/// The same binding over a region that states its tag nullable, as a metric data region does.
///
/// A nullable tag distinguishes a `NULL` value from an empty string in storage: they are different
/// keys, and they decode to an absent label and to an empty one.
fn fixture_binding_with_nullable_tag(name: &str) -> RawLabelSource {
    fixture_binding_of(name, true)
}

/// A per-series binding of one metric whose single tag - and so its primary key - is `name`, in a
/// region that states the tag `nullable`.
fn fixture_binding_of(name: &str, nullable: bool) -> RawLabelSource {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1024, 1));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(name, ConcreteDataType::string_datatype(), nullable),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        })
        .primary_key(vec![1]);
    RawLabelSource::PrimaryKey {
        metric_name: Arc::from(METRIC),
        physical_metadata: Arc::new(builder.build().expect("the fixture metadata is valid")),
        logical_tags: Arc::from([BoundTag {
            column_id: 1,
            name: Arc::from(name),
        }]),
    }
}

/// A per-series scan of `partitions` scan partitions over a region whose only tag is nullable.
///
/// A metric data region states every user tag nullable, because the logical metrics it holds share
/// them, so this is the region a `rate` over a metric table reads. The scan never runs here: what a
/// disjointness claim says is decided from the plan alone.
fn nullable_tag_scan(partitions: usize) -> Arc<dyn ExecutionPlan> {
    let binding = fixture_binding_with_nullable_tag(HOST);
    let RawLabelSource::PrimaryKey {
        physical_metadata, ..
    } = &binding
    else {
        unreachable!("the fixture binding binds a primary key")
    };
    let scanner = NullableTagScanner {
        metadata: Arc::clone(physical_metadata),
        properties: ScannerProperties::new(vec![vec![]; partitions], false, 0),
    };
    let scan = Arc::new(
        RegionScanExec::new(Box::new(scanner), ScanRequest::default(), None)
            .expect("the fixture scanner scans"),
    );
    scan.try_to_series_scan(binding)
        .expect("the fixture binding is legal")
        .expect("the fixture scanner answers raw series")
}

/// The region scan [`nullable_tag_scan`] builds: it publishes no chunks, only the metadata a
/// disjointness claim is read from.
#[derive(Debug)]
struct NullableTagScanner {
    metadata: RegionMetadataRef,
    properties: ScannerProperties,
}

impl DisplayAs for NullableTagScanner {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "NullableTagScanner")
    }
}

impl RegionScanner for NullableTagScanner {
    fn name(&self) -> &str {
        "NullableTagScanner"
    }

    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> DatatypesSchemaRef {
        Arc::clone(&self.metadata.schema)
    }

    fn metadata(&self) -> RegionMetadataRef {
        Arc::clone(&self.metadata)
    }

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.properties.prepare(request);
        Ok(())
    }

    fn scan_partition(
        &self,
        _ctx: &QueryScanContext,
        _metrics: &ExecutionPlanMetricsSet,
        _partition: usize,
    ) -> Result<CommonSendableRecordBatchStream, BoxedError> {
        Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
            &self.metadata.schema,
        ))))
    }

    fn has_predicate_without_region(&self) -> bool {
        true
    }

    fn add_dyn_filter_to_predicate(
        &mut self,
        filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Vec<bool> {
        vec![false; filter_exprs.len()]
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }

    fn set_query_load_region_id(&mut self, region_id: RegionId) {
        self.properties.set_query_load_region_id(region_id);
    }

    fn raw_series_schema(
        &self,
        _label_source: &RawLabelSource,
    ) -> Result<Option<Arc<SeriesSchema>>, BoxedError> {
        Ok(Some(raw_schema()))
    }
}

fn partition_disjoint(tree: NativeTree, input: Arc<dyn ExecutionPlan>) -> bool {
    partition_disjoint_over(tree, input, RawLabelSource::Explicit)
}

/// Whether the fused node over `input`, whose frames `label_source` binds, claims disjoint
/// identities.
fn partition_disjoint_over(
    tree: NativeTree,
    input: Arc<dyn ExecutionPlan>,
    label_source: RawLabelSource,
) -> bool {
    let node = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        if matches!(tree.root().expr, NativeExpr::Binary { .. }) {
            binary_output_schema().to_dfschema_ref().unwrap()
        } else {
            output_schema().to_dfschema_ref().unwrap()
        },
    )
    .unwrap();
    let exec = node
        .to_execution_plan(vec![(input, label_source, SeriesOrdering::Contiguous)])
        .unwrap();
    exec.downcast_ref::<super::FusedSeriesExec>()
        .unwrap()
        .has_partition_disjoint_identities()
}

#[test]
fn partition_disjoint_rejects_non_identity_trees() {
    let grid = grid();
    let sample = NativeTree::try_new(instant_of(0, &grid)).unwrap();
    let mut timestamp = sample.root().clone();
    let NativeExpr::Instant { op, .. } = &mut timestamp.expr else {
        unreachable!();
    };
    op.value = InstantValue::Timestamp;
    let timestamp = NativeTree::try_new(timestamp).unwrap();
    let mut mapped = sample.root().clone();
    let output = mapped.output.clone();
    mapped = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(mapped),
            op: BoundMap {
                ops: Arc::from([MapOp::Math(crate::map::MathOp::Abs)]),
            },
        },
        output,
    };
    let mapped = NativeTree::try_new(mapped).unwrap();
    let trees = [
        timestamp,
        rate_tree(),
        mapped,
        aggregate_tree(),
        binary_tree(raw_schema(), raw_schema()),
    ];
    for tree in trees {
        assert!(
            !partition_disjoint(tree, memory_exec(vec![])),
            "non-identity tree must not prove disjoint identities"
        );
    }
    assert!(
        !super::preserves_partition_identity(
            binary_tree(raw_schema(), raw_schema()).root(),
            Some(&fixture_labels())
        ),
        "two Input leaves sharing source 0 are still a Binary tree"
    );
}

#[test]
fn partition_disjoint_requires_proven_input() {
    let sample = NativeTree::try_new(instant_of(0, &grid())).unwrap();
    assert!(!partition_disjoint(sample.clone(), memory_exec(vec![])));
    assert!(super::preserves_partition_identity(sample.root(), None));

    let scalar = scalar_tree(grid(), 1.0);
    assert!(!super::preserves_partition_identity(scalar.root(), None));
}

#[test]
fn partition_disjoint_accepts_a_raw_source_that_kept_its_per_series_scan() {
    let tree = NativeTree::try_new(instant_of(0, &grid())).unwrap();
    let source = raw_source(per_series_scan(2));
    assert!(
        source
            .downcast_ref::<RawSeriesSourceExec>()
            .expect("the fixture input is the region's raw source")
            .is_per_series_scan(),
        "the fixture input keeps the per-series provenance the region planner declared"
    );

    assert!(
        partition_disjoint(tree, source),
        "a raw source over a per-series scan confines each identity to one scan partition, so the \
         fused node above it keeps that placement"
    );
}

#[test]
fn partition_disjoint_accepts_a_window_that_keeps_the_metric_name() {
    let source = raw_source(per_series_scan(2));
    for function in [RangeFunction::LastOverTime, RangeFunction::FirstOverTime] {
        let tree = window_tree_of(function);
        assert!(
            super::preserves_partition_identity(tree.root(), None),
            "{function:?} answers one series per input series under the labels it read"
        );
        assert!(
            partition_disjoint(tree, Arc::clone(&source)),
            "a window root that keeps the metric name is one output identity per input series, so \
             the per-series scan below it still places each identity on one partition"
        );
    }
}

#[test]
fn partition_disjoint_accepts_a_window_that_drops_the_metric_name_over_one_bound_metric() {
    let source = raw_source(per_series_scan(2));
    for function in [
        RangeFunction::Rate,
        RangeFunction::Increase,
        RangeFunction::SumOverTime,
    ] {
        let tree = window_tree_of(function);
        assert!(
            partition_disjoint(tree, Arc::clone(&source)),
            "{function:?} drops the metric name, and the binding below it names one metric and \
             binds its primary key: the dropped name told no two stored series apart, and every tag \
             that does tell them apart survives the drop, so each output identity is still one \
             stored series' own"
        );
    }
}

#[test]
fn partition_disjoint_refuses_a_window_that_drops_the_metric_name_without_a_bound_key() {
    let tree = window_tree_of(RangeFunction::Rate);
    assert!(
        !super::preserves_partition_identity(tree.root(), None),
        "rate drops the metric name, and without a binding that proves the name was shared the \
         name is part of what tells two series apart"
    );
    assert!(
        !partition_disjoint(tree, memory_exec(vec![])),
        "a source that states no bound labels proves neither that the dropped name was shared nor \
         that the remaining tags distinguish the series it reads"
    );
}

#[test]
fn partition_disjoint_refuses_a_key_bound_by_a_metadata_label() {
    let tree = window_tree_of(RangeFunction::Rate);
    let keeping_name = window_tree_of(RangeFunction::LastOverTime);
    for name in ["__type__", "__unit__"] {
        let binding = fixture_binding(name);
        let labels = super::StoredSeriesLabels::of(&binding)
            .expect("a per-series binding states its labels");
        assert!(
            !labels.keys_survive_metadata_drop,
            "a region that keys a series by {name} is keyed by a label a name-dropping kernel \
             removes"
        );
        assert!(
            !super::preserves_partition_identity(tree.root(), Some(&labels)),
            "rate drops {name} with the name, so two stored series that differ only there meet \
             under one output label set"
        );
        assert!(
            super::preserves_partition_identity(keeping_name.root(), Some(&labels)),
            "a window that keeps the name keeps {name} too, so the binding's key survives it"
        );
    }
}

#[test]
fn partition_disjoint_refuses_a_nullable_key_a_name_drop_normalizes() {
    // One stored series carries `host = NULL` and another `host = ''`: two different storage keys,
    // which decode to an absent label and to an empty one. A kernel that drops the metric name drops
    // the empty value with it, so both series leave it under one label set - and a node that claims
    // disjoint identities never merges that label set again, so the two need not even meet: one
    // partition closing each is enough for both to survive.
    let binding = fixture_binding_with_nullable_tag(HOST);
    let labels =
        super::StoredSeriesLabels::of(&binding).expect("a per-series binding states its labels");
    assert!(
        !labels.keys_survive_metadata_drop,
        "a nullable key column can hold `NULL` in one series and `''` in another, which a \
         name-dropping kernel hands over as no label at all in either case"
    );

    let source = nullable_tag_scan(2);
    assert_eq!(
        source.properties().output_partitioning().partition_count(),
        2,
        "the regression is about two partitions, each holding one of the two series"
    );
    assert!(
        !super::preserves_partition_identity(
            window_tree_of(RangeFunction::Rate).root(),
            Some(&labels)
        ),
        "rate drops the empty value with the name, so the two stored series meet under one label set"
    );
    for tree in [
        window_tree_of(RangeFunction::Rate),
        window_tree_of(RangeFunction::Increase),
        scalar_broadcast_tree(BinaryOp::Mul, false),
    ] {
        assert!(
            !partition_disjoint_over(tree, Arc::clone(&source), binding.clone()),
            "a nullable key proves nothing a name-dropping kernel could rely on, so the node keeps \
             the merging path instead of claiming disjoint identities"
        );
    }

    // What the normalization does not touch still proves: a window that keeps the name and an
    // instant selection keep the labels they read, and with them the absent-versus-empty
    // distinction the nullable key has in storage.
    assert!(
        partition_disjoint_over(
            window_tree_of(RangeFunction::LastOverTime),
            Arc::clone(&source),
            binding.clone()
        ),
        "a window that keeps the name keeps every label it read, so the nullable key still tells \
         two series apart"
    );
    assert!(
        partition_disjoint_over(
            NativeTree::try_new(instant_of(0, &grid())).unwrap(),
            source,
            binding
        ),
        "an instant selection of the sample keeps the labels it read, `NULL` and `''` included"
    );
}

#[test]
fn partition_disjoint_refuses_a_binding_whose_tags_the_rows_do_not_carry() {
    let tree = window_tree_of(RangeFunction::Rate);
    let binding = fixture_binding("zone");
    let labels =
        super::StoredSeriesLabels::of(&binding).expect("a per-series binding states its labels");
    assert!(
        !labels.answers_every_tag(&[HOST.to_string(), NAME.to_string()]),
        "the rows carry `host` and `__name__`, so the `zone` tag that tells stored series apart \
         would be dropped and two of them would leave as one identity"
    );
    assert!(
        labels.answers_every_tag(&["zone".to_string(), NAME.to_string()]),
        "the rows that carry the tag keep the distinction"
    );
    assert!(
        partition_disjoint(tree, raw_source(per_series_scan(2))),
        "the fixture scan binds `host`, which the fixture output rows carry"
    );
}

#[test]
fn partition_disjoint_accepts_an_info_sample_selection() {
    let mut root = NativeTree::try_new(instant_of(0, &grid()))
        .unwrap()
        .root()
        .clone();
    let NativeExpr::Instant { op, .. } = &mut root.expr else {
        panic!("the instant fixture is an instant selection");
    };
    op.value = InstantValue::InfoTimestamp;
    let tree = NativeTree::try_new(root).expect("an info sample selection is a legal tree");
    assert!(
        partition_disjoint(tree, raw_source(per_series_scan(2))),
        "an info sample selection hands each series over under the labels it read, so it keeps \
         every identity its input had"
    );
}

#[test]
fn partition_disjoint_accepts_a_filtering_scalar_comparison() {
    let source = raw_source(per_series_scan(2));
    for scalar_left in [false, true] {
        assert!(
            partition_disjoint(
                scalar_broadcast_tree(BinaryOp::Gt, scalar_left),
                Arc::clone(&source)
            ),
            "a filtering comparison keeps the label set it read, so it keeps every identity"
        );
        assert!(
            partition_disjoint(
                scalar_broadcast_tree(BinaryOp::Mul, scalar_left),
                Arc::clone(&source)
            ),
            "an arithmetic broadcast drops the name of a scan that names one metric and binds its \
             key, which merges no two stored series"
        );
        assert!(
            !super::preserves_partition_identity(
                scalar_broadcast_tree(BinaryOp::Mul, scalar_left).root(),
                None
            ),
            "without the binding, the name arithmetic drops is part of what tells series apart"
        );
    }
}

#[test]
fn partition_disjoint_admits_a_label_rewrite_only_when_it_writes_a_fresh_label() {
    let source = raw_source(per_series_scan(2));
    assert!(
        partition_disjoint(label_rewrite_tree("zone"), Arc::clone(&source)),
        "writing a label no series carries cannot make two of them equal"
    );
    for destination in [HOST, NAME] {
        assert!(
            !partition_disjoint(label_rewrite_tree(destination), Arc::clone(&source)),
            "{destination} is a label a stored series carries, so rewriting it can fold two \
             series into one label set"
        );
    }
}

#[test]
fn partition_disjoint_rejects_a_multi_input_tree() {
    let tree = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let node = FusedSeries::try_new(
        Arc::from("left + right"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the logical node accepts two dense sources");
    let exec = node
        .to_execution_plan(vec![
            (
                raw_source(per_series_scan(2)),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
            (
                raw_source(per_series_scan(2)),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
        ])
        .expect("both sources are per-series scans");
    let fused = exec
        .downcast_ref::<super::FusedSeriesExec>()
        .expect("the fused node is the root here");
    assert_eq!(
        2,
        fused.inputs.len(),
        "the fixture really has more than one input"
    );
    assert!(
        !fused.has_partition_disjoint_identities(),
        "a tree reading several sources matches their series against each other, so no output \
         identity is one input's"
    );
}

#[test]
fn partition_disjoint_fails_closed_across_a_repartition() {
    let tree = window_tree_of(RangeFunction::LastOverTime);
    let repartitioned: Arc<dyn ExecutionPlan> = Arc::new(
        SeriesRepartitionExec::try_new(raw_source(per_series_scan(2)), 2)
            .expect("a raw frame routes by its primary key"),
    );
    assert!(
        !partition_disjoint(tree, repartitioned),
        "a redistribution below the producer breaks the one-identity-one-partition contract, so the \
         per-series provenance below it cannot be inherited through it"
    );
}

#[test]
fn scalar_binaries_only_preserve_routable_keys_when_they_keep_them() {
    for scalar_left in [false, true] {
        let name_tree = scalar_operation_tree("__name__", scalar_left, BinaryOp::Mul);
        assert!(matches!(
            super::input_locality(name_tree.root()),
            super::InputLocality::SinglePartition
        ));
        let host_tree = scalar_operation_tree(HOST, scalar_left, BinaryOp::Mul);
        assert!(matches!(
            super::input_locality(host_tree.root()),
            super::InputLocality::MatchingLocal(_)
        ));
        let filter_tree = scalar_operation_tree("__name__", scalar_left, BinaryOp::Gt);
        // Filtering comparisons retain labels; the output can still be routed by the name.
        assert!(matches!(
            super::input_locality(filter_tree.root()),
            super::InputLocality::MatchingLocal(_)
        ));
    }
}

fn matching_name_partition(name: &str, partition_count: usize) -> usize {
    use std::hash::{Hash, Hasher};
    use twox_hash::XxHash64;

    let mut hasher = XxHash64::with_seed(0x517c_c1b7_2722_0a95);
    crate::vector_match::group_key(
        &metric(&[], Some(name)),
        &MatchingLabels::On(vec![Arc::from("__name__")]),
    )
    .hash(&mut hasher);
    (hasher.finish() % partition_count as u64) as usize
}

#[tokio::test]
async fn scalar_binary_matching_key_is_not_routed_by_dropped_names() {
    let tree = scalar_operation_tree_over_sources("__name__", false, BinaryOp::Mul, 0, 1);
    // The routing uses xxHash64 with ROUTING_SEED and hashes vector_match::group_key.
    // Find two metric names whose `on(__name__)` group keys separate into two outputs.
    let partition_count = 2;
    // Select names deterministically and verify their destinations below rather than assuming
    // the seeded hash separates them.
    let (left_name, right_name) = (0..100)
        .flat_map(|left| (0..100).map(move |right| (format!("x{left}"), format!("y{right}"))))
        .find(|(left, right)| {
            let left_partition = matching_name_partition(left, partition_count);
            let right_partition = matching_name_partition(right, partition_count);
            left_partition != right_partition
        })
        .expect("some name pair separates with two output partitions");
    let left_partition = matching_name_partition(&left_name, partition_count);
    let right_partition = matching_name_partition(&right_name, partition_count);
    assert_ne!(left_partition, right_partition);
    assert_eq!(("x0", "y0"), (left_name.as_str(), right_name.as_str()));
    assert_eq!((0, 1), (left_partition, right_partition));

    let mut left = series_chunk("left", &[(GRID_START, 2.0)], true);
    left.metric = Some(metric(&[], Some(&left_name)));
    let mut right = series_chunk("right", &[(GRID_START, 3.0)], true);
    right.metric = Some(metric(&[], Some(&right_name)));
    let left_input = memory_exec_partitioned(vec![vec![frame(&left)], vec![]]);
    let right_input = memory_exec_partitioned(vec![vec![], vec![frame(&right)]]);
    let node = FusedSeries::try_new(
        Arc::from("(x * 2) + on(__name__) (y * 2)"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    assert!(matches!(
        super::input_locality(tree.root()),
        super::InputLocality::SinglePartition
    ));
    let exec = node
        .to_execution_plan(vec![
            (
                left_input,
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
            (
                right_input,
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
        ])
        .expect("the scalar binary executes");
    // A bare tree is conservatively classified as SinglePartition because its computed operands
    // dropped the requested key. Planner segmentation cuts those per-series prefixes and routes
    // their evaluated outputs by the actual labels; see the planner and integration regressions.
    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("the scalar binary completes");
    assert_eq!(vec![10.0], binary_values(&batches));
}

#[test]
fn timestamp_selection_does_not_preserve_computed_matching_labels() {
    let tree = timestamp_match_tree();
    assert!(matches!(
        super::input_locality(tree.root()),
        super::InputLocality::SinglePartition
    ));
}

#[test]
fn math_map_matching_key_preservation_controls_locality() {
    let name_tree = binary_with_abs_and_matching_key("__name__");
    assert!(matches!(
        super::input_locality(name_tree.root()),
        super::InputLocality::SinglePartition
    ));

    let host_tree = binary_with_abs_and_matching_key(HOST);
    assert!(matches!(
        super::input_locality(host_tree.root()),
        super::InputLocality::MatchingLocal(_)
    ));
}

/// The inner ranking's per-point parameter children finish before the outer ranking settles.
/// Dropping those children must not prevent the outer `topk` from returning its suffix rows.
#[tokio::test]
async fn constant_topk_over_parameterized_topk_keeps_suffix_rows() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 60_000, 3).unwrap()).unwrap());
    let raw = NativeType::Samples(raw_schema());
    let output = evaluated_of(&raw_schema(), &grid);
    let window = NativeNode {
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
                    range_ms: 60_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: None,
                output_grid: Arc::clone(&grid),
            },
        },
        output: output.clone(),
    };
    let ceil = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(window),
            op: BoundMap {
                ops: Arc::from([MapOp::Math(crate::map::MathOp::Ceil)]),
            },
        },
        output: output.clone(),
    };
    let sum = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(ceil),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::from([Arc::from(HOST)])),
                stage: AggregateStage::Complete,
                field_kind: FieldKind::Float,
            },
        },
        output: output.clone(),
    };
    let scalar = |value| NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let binary = |left, right, op| NativeNode {
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
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let time = NativeNode {
        expr: NativeExpr::EvalTime { date: None },
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let parameter = binary(
        binary(time, scalar(1.0 / 60.0), BinaryOp::Mul),
        scalar(1.0),
        BinaryOp::Add,
    );
    let rank = |k| BoundVectorOp::TopK {
        k,
        top: true,
        group: Grouping::By(Arc::from([])),
    };
    let inner = NativeNode {
        expr: NativeExpr::ParameterizedTopK {
            input: Box::new(sum),
            op: rank(0.0),
            parameter: Box::new(parameter),
        },
        output: output.clone(),
    };
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(inner),
            op: rank(1.0),
        },
        output,
    })
    .unwrap();

    // `h0` through `h7` sum to 0 through 7. Splitting `h7` across the two physical
    // partitions also proves that the gathered execution reads both contributions: partition 0
    // alone would rank `h6=6` over its `h7=3` fragment.
    let mut partitions = vec![Vec::new(), Vec::new()];
    for host in 0..8 {
        let host_name = format!("h{host}");
        let parts: Vec<(usize, &str, f64)> = if host == 7 {
            vec![(0, "a", 3.0), (1, "b", 4.0)]
        } else {
            vec![(host % 2, "a", host as f64)]
        };
        for (partition, job, value) in parts {
            let key = format!("{host_name}-{job}");
            let samples = [
                (-60_000, value),
                (0, value),
                (60_000, value),
                (120_000, value),
            ];
            partitions[partition].push(frame(&labelled_chunk(
                &key,
                &[(HOST, &host_name), (JOB, job)],
                &samples,
                true,
            )));
        }
    }
    assert!(matches!(
        super::input_locality(tree.root()),
        super::InputLocality::SinglePartition
    ));
    let exec = plan(tree, memory_exec_partitioned(partitions)).unwrap();
    let mut actual = rows(
        &collect(exec, SessionContext::default().task_ctx())
            .await
            .unwrap(),
    );
    actual.sort_by_key(|row| row.0);
    assert_eq!(
        actual,
        vec![
            (0, Some(7.0), Some("h7".to_string()), None),
            (60_000, Some(7.0), Some("h7".to_string()), None),
            (120_000, Some(7.0), Some("h7".to_string()), None),
        ]
    );
}

/// A node that reads the whole vector at a point answers one value (or a whole ranking) rather than
/// one output series per input series, so it cannot keep a source's matching key: it needs every
/// series of that source in one place.
///
/// Each of these shapes takes a per-point `time()` or a literal parameter, which is exactly the case
/// a parameter-shaped `same_source` arm was tempted to pass through. Answering `PartitionLocal` here
/// does not only cost parallelism: a `topk` or a histogram function run once per partition answers
/// the union of the per-partition rankings.
#[test]
fn whole_vector_parameter_nodes_need_every_series_in_one_partition() {
    let grid = grid();
    let eval_time = || NativeNode {
        expr: NativeExpr::EvalTime { date: None },
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let single_partition = |tree: NativeTree| {
        assert!(
            matches!(
                super::input_locality(tree.root()),
                super::InputLocality::SinglePartition
            ),
            "a whole-vector node must read its source in one partition: {tree}"
        );
    };

    // `scalar(m)`: which series carries the one value at a point decides the answer.
    let instant = instant_of(0, &grid);
    single_partition(
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::ScalarOf {
                input: Box::new(instant),
            },
            output: NativeType::Scalar(Arc::clone(&grid)),
        })
        .expect("scalar() over a raw selection is a legal tree"),
    );

    // A per-point `k` decides which of the whole vector's series survive.
    let instant = instant_of(0, &grid);
    let output = instant.output.clone();
    single_partition(
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::ParameterizedTopK {
                input: Box::new(instant),
                op: BoundVectorOp::TopK {
                    k: 0.0,
                    top: true,
                    group: Grouping::By(Arc::from([])),
                },
                parameter: Box::new(eval_time()),
            },
            output,
        })
        .expect("topk with a per-point k over a raw selection is a legal tree"),
    );

    // The histogram function folds the bucket series of the whole vector into one.
    let rate = rate_tree();
    let output = rate.root().output.clone();
    single_partition(
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::ParameterizedHistogram {
                input: Box::new(rate.root().clone()),
                op: BoundVectorOp::Histogram(HistogramFunction::Quantile(0.5)),
                parameters: vec![eval_time()],
            },
            output,
        })
        .expect("a histogram function over a raw rate window is a legal tree"),
    );

    // `b * scalar(a)`: the scalar side is not source-free, so its broadcast has to wait until the
    // whole of its own vector is gathered.
    let vector_side = instant_of(0, &grid);
    let output = vector_side.output.clone();
    let scalar_side = NativeNode {
        expr: NativeExpr::ScalarOf {
            input: Box::new(instant_of(1, &grid)),
        },
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    single_partition(
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::Binary {
                left: Box::new(vector_side),
                right: Box::new(scalar_side),
                matching: BinaryMatching {
                    op: BinaryOp::Mul,
                    return_bool: false,
                    labels: MatchingLabels::Ignoring(Vec::new()),
                    cardinality: Cardinality::OneToOne,
                    fill: FillValues::default(),
                },
            },
            output,
        })
        .expect("a scalar broadcast over a raw selection is a legal tree"),
    );

    // `clamp_min(b, scalar(a))`: a parameterized map is only as local as its parameter.
    let instant = instant_of(0, &grid);
    let output = instant.output.clone();
    single_partition(
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::ParameterizedMap {
                input: Box::new(instant),
                op: ParameterizedOp::ClampMin,
                parameters: vec![NativeNode {
                    expr: NativeExpr::ScalarOf {
                        input: Box::new(instant_of(1, &grid)),
                    },
                    output: NativeType::Scalar(Arc::clone(&grid)),
                }],
            },
            output,
        })
        .expect("a parameterized map over a scalar() parameter is a legal tree"),
    );

    // `(a + on(job) b) * scalar(c)`: the match sits under a broadcast whose scalar side reads a
    // third source, so the match's key is not the unit of locality. Routing by it would answer
    // `scalar(c)` once per key class, and a statement whose global answer is NaN — two `c` series
    // at one point — would answer one finite threshold per class instead.
    let matched = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let output = matched.root().output.clone();
    single_partition(
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::Binary {
                left: Box::new(matched.root().clone()),
                right: Box::new(NativeNode {
                    expr: NativeExpr::ScalarOf {
                        input: Box::new(instant_of(2, &grid)),
                    },
                    output: NativeType::Scalar(Arc::clone(&grid)),
                }),
                matching: BinaryMatching {
                    op: BinaryOp::Mul,
                    return_bool: false,
                    labels: MatchingLabels::Ignoring(Vec::new()),
                    cardinality: Cardinality::OneToOne,
                    fill: FillValues::default(),
                },
            },
            output,
        })
        .expect("a match under a scalar() broadcast is a legal tree"),
    );
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

/// The same tree grouped by a value field, which no kernel can address.
///
/// A group is identified by labels, and a value field is not one: its value varies between the
/// samples of one series, so a group keyed by it would be a group per point. The runtime refuses the
/// grouping rather than folding series into groups the previous route keeps apart.
fn ungroupable_aggregate_tree() -> NativeTree {
    let complete = aggregate_tree();
    let root = complete.root();
    let NativeExpr::Aggregate { input, .. } = &root.expr else {
        unreachable!("the fixture is an aggregate over a window");
    };
    let field = root
        .output
        .samples()
        .expect("the aggregate produces samples")
        .fields
        .first()
        .expect("the aggregate has a value field")
        .name
        .clone();
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Aggregate {
            input: input.clone(),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new([field])),
                stage: AggregateStage::Complete,
                field_kind: FieldKind::Float,
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
                field_kind: FieldKind::Float,
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
        grid: match &tree.root().output {
            NativeType::Samples(schema) => schema
                .eval_grid
                .clone()
                .expect("this tree's output is evaluated on a grid"),
            NativeType::AggregateState(state) => Arc::clone(&state.grid),
            NativeType::Scalar(grid) => Arc::clone(grid),
        },
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

fn state_tree(stage: AggregateStage, input: NativeType) -> NativeTree {
    let state = Arc::new(AggregateStateSchema {
        group: Grouping::By(Arc::new([Arc::from(JOB)])),
        kind: AggregateStateKind::Sum,
        components: Arc::from([StateComponent::Value, StateComponent::Compensation]),
        accumulators: raw_schema().fields.clone(),
        grid: grid(),
    });
    let child_type = if stage == AggregateStage::Partial {
        NativeType::Samples(Arc::new(SeriesSchema {
            fields: raw_schema().fields.clone(),
            eval_grid: Some(grid()),
        }))
    } else {
        input
    };
    let state_input = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: child_type.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Contiguous,
        }),
        output: child_type,
    };
    let output = match stage {
        AggregateStage::Partial => NativeType::AggregateState(state),
        AggregateStage::Final | AggregateStage::Complete => {
            NativeType::Samples(Arc::new(SeriesSchema {
                fields: raw_schema().fields.clone(),
                eval_grid: Some(grid()),
            }))
        }
    };
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(state_input),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new([Arc::from(JOB)])),
                stage,
                field_kind: FieldKind::Float,
            },
        },
        output,
    })
    .expect("aggregate stage binding is valid")
}

fn canonical_state_frame_schema(tree: &NativeTree) -> datafusion::common::DFSchemaRef {
    let schema = super::state_frame_output_schema(tree).unwrap();
    Arc::new(datafusion::common::DFSchema::try_from(schema).unwrap())
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
    zone_node_with(rate_tree())
}

/// The same node over `tree`, so a test can stage the tree the canonical pass produces.
fn zone_node_with(tree: NativeTree) -> FusedSeries {
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

fn canonical_producer_node() -> FusedSeries {
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
    FusedSeries::try_new_canonical_producer(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
    )
    .unwrap()
}

#[test]
fn fused_series_canonical_producer_exec_tracks_physical_routing() {
    let canonical = canonical_producer_node();
    let ordinary = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        canonical.tree().clone(),
        canonical.binding().clone(),
        vec![input_plan(frame_schema())],
        canonical.schema().clone(),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let input = || {
        (
            memory_exec_with(frame_schema(), vec![]),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )
    };
    let router = canonical.to_execution_plan(vec![input()]).unwrap();
    let producer = router.children()[0];
    let ordinary_exec = ordinary.to_execution_plan(vec![input()]).unwrap();
    let rebuilt = Arc::clone(producer)
        .replace_children(
            vec![Arc::clone(producer.children()[0])],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .unwrap();

    assert!(router.downcast_ref::<SeriesRepartitionExec>().is_some());
    assert!(
        producer
            .downcast_ref::<super::FusedSeriesExec>()
            .unwrap()
            .canonical_producer
    );
    assert!(
        !ordinary_exec
            .downcast_ref::<super::FusedSeriesExec>()
            .unwrap()
            .canonical_producer
    );
    assert!(
        rebuilt
            .downcast_ref::<super::FusedSeriesExec>()
            .unwrap()
            .canonical_producer
    );

    #[cfg(feature = "test-utils")]
    {
        assert!(!super::is_canonical_producer_exec(router.as_ref()));
        assert!(super::is_canonical_producer_exec(producer.as_ref()));
        assert!(!super::is_canonical_producer_exec(ordinary_exec.as_ref()));
        assert!(super::is_canonical_producer_exec(rebuilt.as_ref()));
    }
}

#[test]
fn fused_series_encoding_preserves_state_and_canonical_producer() {
    // A state-exporting producer encodes like any other node: serialization is the node's own
    // capability, not a decision about where it may run.
    let tree = partial_tree(rate_tree(), AggregateKind::Sum, &[HOST, JOB]);
    let producer = FusedSeries::try_new_state_export(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
    )
    .unwrap();
    let decoded = FusedSeries::deserialize(&producer.serialize().unwrap()).unwrap();
    assert_eq!(decoded.tree(), producer.tree());
    assert_eq!(decoded.binding(), producer.binding());
    assert_eq!(decoded.schema(), producer.schema());
    assert!(decoded.reads_aggregate_state() == producer.reads_aggregate_state());

    // A canonical producer's flag is part of the node, so it survives the round trip; the
    // decoder does not get to decide whether its prefix is partition-local.
    let canonical = canonical_producer_node();
    let decoded = FusedSeries::deserialize(&canonical.serialize().unwrap()).unwrap();
    assert!(decoded.is_canonical_producer());
    assert_eq!(decoded.tree(), canonical.tree());
    assert_eq!(decoded.schema(), canonical.schema());
}

#[test]
fn fused_series_ordinary_encoding_round_trip_and_rejects_unsupported_payloads() {
    let tree = rate_tree();
    let original_binding = binding(&tree);
    // A frame-output node is checked against the codec's own frame layout, so it is built with
    // `canonical_frame_schema` rather than with the plain row `output_schema`.
    let declared = canonical_frame_schema(&tree);
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree,
        original_binding.clone(),
        vec![input_plan(frame_schema())],
        declared,
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let bytes = node.serialize().unwrap();
    let decoded = FusedSeries::deserialize(&bytes).unwrap();
    assert_eq!(decoded.tree(), node.tree());
    assert_eq!(decoded.binding(), node.binding());
    assert_eq!(decoded.schema(), node.schema());
    let rows = fused(rate_tree());
    let row_decoded = FusedSeries::deserialize(&rows.serialize().unwrap()).unwrap();
    assert_eq!(row_decoded.schema(), rows.schema());
    assert_eq!(row_decoded.tree(), rows.tree());

    // A legal noncanonical order must survive the wire unchanged.
    let canonical = rows.schema().as_arrow();
    let reordered_schema = Arc::new(Schema::new(vec![
        canonical.field_with_name(VALUE).unwrap().clone(),
        canonical.field_with_name(HOST).unwrap().clone(),
        canonical.field_with_name(NAME).unwrap().clone(),
        canonical.field_with_name(TIME).unwrap().clone(),
    ]));
    let reordered = FusedSeries::try_new(
        Arc::from(PROMQL),
        rows.tree().clone(),
        rows.binding().clone(),
        rows.children().to_vec(),
        Arc::new(
            datafusion::common::DFSchema::try_from(reordered_schema.as_ref().clone()).unwrap(),
        ),
    )
    .unwrap();
    let reordered_decoded = FusedSeries::deserialize(&reordered.serialize().unwrap()).unwrap();
    assert_eq!(reordered_decoded.schema(), reordered.schema());

    // The output schema owns the label layout; the source binding may have no tags or different
    // labels after a tree operation such as label rewriting.
    let empty_binding = binding_of(rows.tree(), raw_schema(), Vec::new());
    let empty_tags = FusedSeries::try_new(
        Arc::from(PROMQL),
        rate_tree(),
        empty_binding,
        vec![input_plan(frame_schema())],
        output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let decoded = FusedSeries::deserialize(&empty_tags.serialize().unwrap()).unwrap();
    assert_eq!(decoded.binding().tags, empty_tags.binding().tags);
    assert_eq!(decoded.schema(), empty_tags.schema());

    let projected = fused(binary_tree(raw_schema(), raw_schema()));
    let projected_bytes = projected.serialize().unwrap();
    let projected_decoded = FusedSeries::deserialize(&projected_bytes).unwrap();
    assert_eq!(projected_decoded.schema(), projected.schema());

    // A state-exporting producer encodes like any other node; where it may run is decided by the
    // planner, not by the codec.
    let partial = partial_node(AggregateKind::Sum);
    let partial_decoded = FusedSeries::deserialize(&partial.serialize().unwrap()).unwrap();
    assert_eq!(partial_decoded.tree(), partial.tree());
    assert_eq!(partial_decoded.schema(), partial.schema());
    let mut bad_version = bytes.clone();
    bad_version[..4].copy_from_slice(&99u32.to_le_bytes());
    assert!(
        FusedSeries::deserialize(&bad_version)
            .unwrap_err()
            .to_string()
            .contains("version")
    );
    assert!(FusedSeries::deserialize(&bytes[..5]).is_err());
    let mut trailing_payload = bytes.clone();
    let payload_len = u32::from_le_bytes(trailing_payload[4..8].try_into().unwrap()) as usize;
    trailing_payload.insert(8 + payload_len, 0);
    trailing_payload[4..8].copy_from_slice(&((payload_len + 1) as u32).to_le_bytes());
    assert!(FusedSeries::deserialize(&trailing_payload).is_err());

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(FusedSeries::deserialize(&trailing).is_err());
}

/// A payload marked with an unsupported version is refused before its body is read.
///
/// The body here is the current payload's body with the version field set to a future number this
/// reader does not accept: it decodes cleanly under the current reader, which makes it a control.
/// A reader that checked the version only after decoding would accept it and hand back a node whose
/// fields were read at the wrong offsets; this asserts the version is what answers, by name.
#[test]
fn fused_series_envelope_refuses_an_unsupported_payload_version() {
    let tree = rate_tree();
    let declared = canonical_frame_schema(&tree);
    let bound = binding(&tree);
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree,
        bound,
        vec![input_plan(frame_schema())],
        declared,
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let bytes = node.serialize().unwrap();
    assert_eq!(
        super::FUSED_SERIES_ENVELOPE_VERSION.to_le_bytes(),
        bytes[..4],
        "the version is the payload's first field"
    );

    // The control: the same body under the accepted version decodes.
    assert!(FusedSeries::deserialize(&bytes).is_ok());

    let mut unsupported = bytes.clone();
    unsupported[..4].copy_from_slice(&2u32.to_le_bytes());
    let message = FusedSeries::deserialize(&unsupported)
        .unwrap_err()
        .to_string();
    assert!(
        message.contains("version") && message.contains('2'),
        "the version must be what is reported, not a field read at the wrong offset: {message}"
    );
}

/// The `SeriesFrame` node of [`rate_tree`] declaring `tags` as its labels.
///
/// A `SeriesFrame` node is checked against the codec's own frame layout, so it is built with
/// [`canonical_frame_schema`] rather than with the plain row [`output_schema`].
fn envelope_node_with_tags(tags: Vec<String>) -> FusedSeries {
    let tree = rate_tree();
    let binding = binding_of(&tree, raw_schema(), tags);
    let output_schema = canonical_frame_schema(&tree);
    FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree,
        binding,
        vec![input_plan(frame_schema())],
        output_schema,
        FusedOutputFormat::SeriesFrame,
    )
    .expect("the frame layout matches the evaluated root")
}

fn mutate_envelope_payload(
    bytes: &[u8],
    mutate: impl FnOnce(&mut super::FusedSeriesPayload),
) -> Vec<u8> {
    let (mut payload, remainder) =
        postcard::take_from_bytes::<super::FusedSeriesPayload>(&bytes[8..]).unwrap();
    assert!(remainder.is_empty());
    mutate(&mut payload);
    let changed = postcard::to_stdvec(&payload).unwrap();
    let mut result = Vec::with_capacity(8 + changed.len());
    result.extend(super::FUSED_SERIES_ENVELOPE_VERSION.to_le_bytes());
    result.extend((changed.len() as u32).to_le_bytes());
    result.extend(changed);
    result
}

fn mutate_schema(bytes: &[u8], schema: Schema) -> Vec<u8> {
    mutate_envelope_payload(bytes, |payload| payload.output_schema = schema)
}

fn scalar_node(tree: NativeTree, value_name: &str) -> FusedSeries {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(value_name, DataType::Float64, true),
    ]));
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        FusedSeriesBinding {
            grid: grid(),
            time_index: TIME.into(),
            tags: Vec::new(),
            metric_name: None,
        },
        Vec::new(),
        Arc::new(datafusion::common::DFSchema::try_from(schema.as_ref().clone()).unwrap()),
    )
    .unwrap()
}

fn assert_decode_error(bytes: &[u8], expected: &str) {
    let error = FusedSeries::deserialize(bytes).unwrap_err().to_string();
    assert!(
        error.contains(expected),
        "expected {expected:?}, got {error:?}"
    );
}

#[test]
fn renamed_reordered_value_columns_keep_field_identity_after_decode() {
    let tree = rate_tree_of(two_field_schema());
    let schema = Arc::new(Schema::new(vec![
        Field::new(format!("{PROMQL}.{OTHER}"), DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(format!("{PROMQL}.{VALUE}"), DataType::Float64, true),
    ]));
    let node = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        binding_of(&tree, two_field_schema(), vec![HOST.to_string()]),
        vec![input_plan(frame_schema_of(&two_field_schema()))],
        Arc::new(datafusion::common::DFSchema::try_from(schema.as_ref().clone()).unwrap()),
    )
    .unwrap();
    let decoded = FusedSeries::deserialize(&node.serialize().unwrap()).unwrap();
    assert_eq!(node.schema(), decoded.schema());
    let tags = decoded.output_tags().unwrap();
    assert_eq!(vec![HOST], tags);
    let classification =
        super::classify_output(&schema, &two_field_schema().fields, &tags, TIME, PROMQL).unwrap();
    let chunk = EvaluatedChunk::try_new(
        Series {
            metric: SeriesLabels::try_new(None, Vec::new()).unwrap(),
            time: TimeAxis::from_grid_spans(grid(), vec![GridSpan::new(0, 1)]).unwrap(),
            fields: SeriesFields::try_from_vec(vec![
                Samples::from_float(Float64Array::from(vec![10.0])),
                Samples::from_float(Float64Array::from(vec![20.0])),
            ])
            .unwrap(),
        },
        true,
    )
    .unwrap();
    let batch = series_to_record_batch(&classification, &tags, &schema, &chunk).unwrap();
    for (name, expected) in [(OTHER, 20.0), (VALUE, 10.0)] {
        let values = batch
            .column_by_name(&format!("{PROMQL}.{name}"))
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(expected, values.value(0));
    }
}

/// A tag that carries the expression's name is a tag, not the statement's value.
///
/// This is an old-style row schema: the statement is `host`, the schema names its one value
/// `field_0` and also carries the `host` tag. Deciding the value by whether a name occurs
/// anywhere in the schema would pick the tag as the value; the typed columns put the value at
/// `field_0` and the tag at `host`.
#[test]
fn a_tag_named_after_the_expression_is_not_the_value() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("field_0", DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
    ]));
    let fields = [FieldSpec {
        name: "field_0".into(),
    }];
    let classification =
        super::classify_output(&schema, &fields, &[HOST.to_string()], TIME, HOST).unwrap();
    assert!(
        matches!(
            classification[1],
            OutputColumn::Field {
                index: 0,
                kind: FieldKind::Float
            }
        ),
        "{classification:?}"
    );
    assert!(
        matches!(classification[2], OutputColumn::Tag(0)),
        "{classification:?}"
    );
}

/// A reordered output still binds every value column to its own field.
///
/// The expression names are matched as a set against the typed value columns, and the result is
/// kept in `series_fields` order - here `other` before `value` - because that order is what the
/// classification indexes the chunk's fields by. Names taken in output order would bind the first
/// column to `value`.
#[test]
fn reordered_expression_names_bind_fields_in_series_order() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(format!("{PROMQL}.{OTHER}"), DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(format!("{PROMQL}.{VALUE}"), DataType::Float64, true),
    ]));
    let series_schema = two_field_schema();
    let classification = super::classify_output(
        &schema,
        &series_schema.fields,
        &[HOST.to_string()],
        TIME,
        PROMQL,
    )
    .unwrap();
    assert!(
        matches!(
            classification[1],
            OutputColumn::Field {
                index: 1,
                kind: FieldKind::Float
            }
        ),
        "{classification:?}"
    );
    assert!(
        matches!(classification[2], OutputColumn::Tag(0)),
        "{classification:?}"
    );
    assert!(
        matches!(
            classification[3],
            OutputColumn::Field {
                index: 0,
                kind: FieldKind::Float
            }
        ),
        "{classification:?}"
    );
}

#[test]
fn plain_rows_decode_rejects_invalid_schemas() {
    let node = fused(rate_tree());
    let bytes = node.serialize().unwrap();
    let scalar = scalar_node(scalar_tree(grid(), 1.0), "clock_value");
    let scalar_bytes = scalar.serialize().unwrap();
    let scalar_with_tag = mutate_envelope_payload(&scalar_bytes, |payload| {
        let mut fields = payload.output_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(HOST, DataType::Utf8, true)));
        payload.output_schema = Schema::new(fields);
    });
    assert_decode_error(&scalar_with_tag, "cannot produce 1 tag columns");

    let malformed_schemas = [
        (
            Schema::new(vec![Field::new(TIME, DataType::Utf8, false)]),
            "must be Timestamp",
        ),
        (
            Schema::new(vec![
                Field::new(
                    TIME,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new(VALUE, DataType::Float64, true),
                Field::new(VALUE, DataType::Float64, true),
            ]),
            "duplicate",
        ),
        (
            Schema::new(vec![
                Field::new(
                    TIME,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new(HOST, DataType::Utf8, true),
                Field::new(NAME, DataType::Utf8, true),
            ]),
            "missing field column",
        ),
        (
            Schema::new(vec![
                Field::new(
                    TIME,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new(VALUE, DataType::Float64, true),
                // A column's role is its type: a float here is not a tag, so this payload names a
                // column the conversion cannot produce.
                Field::new(HOST, DataType::Float64, true),
                Field::new(NAME, DataType::Utf8, true),
            ]),
            "unknown column",
        ),
    ];
    for (schema, expected) in malformed_schemas {
        assert_decode_error(&mutate_schema(&bytes, schema), expected);
    }

    let renamed = mutate_envelope_payload(&bytes, |payload| {
        let mut fields = payload.output_schema.fields().to_vec();
        let index = fields
            .iter()
            .position(|field| field.name() == VALUE)
            .unwrap();
        fields[index] = Arc::new(Field::new(PROMQL, DataType::Float64, true));
        payload.output_schema = Schema::new(fields);
    });
    assert!(FusedSeries::deserialize(&renamed).is_ok());
    // A value column is identified by type, so this payload types its one float column as neither
    // a float nor a native histogram and names no value column the tree can read; a float value
    // column that cannot be null is refused for its nullability, because a grid point may be
    // missing.
    for (data_type, nullable, expected) in [
        (DataType::Int64, true, "unknown column"),
        (DataType::Float64, false, "must be nullable"),
    ] {
        let malformed = mutate_envelope_payload(&renamed, |payload| {
            let mut fields = payload.output_schema.fields().to_vec();
            let index = fields
                .iter()
                .position(|field| field.name() == PROMQL)
                .unwrap();
            fields[index] = Arc::new(Field::new(PROMQL, data_type, nullable));
            payload.output_schema = Schema::new(fields);
        });
        assert_decode_error(&malformed, expected);
    }

    // A scalar's value column is its one non-time column, so the wrong type is reported as a kind
    // the conversion cannot produce.
    let scalar_wrong_kind = mutate_envelope_payload(&scalar_bytes, |payload| {
        let mut fields = payload.output_schema.fields().to_vec();
        let index = fields
            .iter()
            .position(|field| field.name() != TIME)
            .expect("a scalar carries one value column");
        let name = fields[index].name().clone();
        fields[index] = Arc::new(Field::new(name, DataType::Int64, true));
        payload.output_schema = Schema::new(fields);
    });
    assert_decode_error(&scalar_wrong_kind, "must be Float64");

    let scalar_decoded = FusedSeries::deserialize(&scalar_bytes).unwrap();
    assert_eq!(scalar_decoded.schema(), scalar.schema());

    // Binding tags describe the source; the carried schema independently describes outputs.
    let empty_binding = binding_of(node.tree(), raw_schema(), Vec::new());
    let cross_table = FusedSeries::try_new(
        Arc::from(PROMQL),
        node.tree().clone(),
        empty_binding,
        node.children().to_vec(),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let decoded = FusedSeries::deserialize(&cross_table.serialize().unwrap()).unwrap();
    assert_eq!(decoded.schema(), cross_table.schema());
    assert!(decoded.binding().tags.is_empty());
}

#[test]
fn a_non_default_column_order_survives_the_round_trip() {
    let tree = rate_tree();
    let binding = binding(&tree);
    let reordered = Schema::new_with_metadata(
        vec![
            Field::new(VALUE, DataType::Float64, true),
            Field::new(
                TIME,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(HOST, DataType::Utf8, true)
                .with_metadata([("origin".to_string(), "carried".to_string())].into()),
            Field::new(NAME, DataType::Utf8, true),
        ],
        [("schema-origin".to_string(), "carried".to_string())].into(),
    );
    let node = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        vec![input_plan(frame_schema())],
        Arc::new(datafusion::common::DFSchema::try_from(reordered.clone()).unwrap()),
    )
    .unwrap();
    let decoded = FusedSeries::deserialize(&node.serialize().unwrap()).unwrap();
    assert_eq!(decoded.schema(), node.schema());
    assert_eq!(decoded.schema().as_arrow().metadata(), reordered.metadata());
    assert_eq!(
        decoded
            .schema()
            .field_with_name(None, HOST)
            .unwrap()
            .metadata(),
        reordered.field_with_name(HOST).unwrap().metadata()
    );
}

#[test]
fn series_frame_decode_rejects_schema_mismatch() {
    let node = envelope_node_with_tags(vec![HOST.into(), NAME.into()]);
    let bytes = node.serialize().unwrap();
    let wrong = mutate_envelope_payload(&bytes, |payload| {
        let mut fields = payload.output_schema.fields().to_vec();
        fields.push(Arc::new(Field::new("extra", DataType::Utf8, true)));
        payload.output_schema = Schema::new(fields);
    });
    assert_decode_error(&wrong, "SeriesFrame schema does not match");
}

#[test]
fn state_export_decode_rejects_schema_and_format_mismatch() {
    let tree = partial_tree(rate_tree(), AggregateKind::Sum, &[HOST, JOB]);
    let node = FusedSeries::try_new_state_export(
        Arc::from(PROMQL),
        tree.clone(),
        binding_of(&tree, raw_schema(), vec![HOST.to_string(), JOB.to_string()]),
        vec![input_plan(frame_schema())],
    )
    .unwrap();
    // A state exporter is refused by the serializer, so its decode-time contracts are exercised
    // through a payload that says what a state exporter's payload would say.
    let bytes = mutate_envelope_payload(
        &envelope_node_with_tags(vec![HOST.into(), NAME.into()])
            .serialize()
            .unwrap(),
        |payload| {
            payload.tree = node.tree().clone();
            payload.output_schema = node.schema().as_arrow().clone();
            payload.format = FusedOutputFormat::SeriesFrame;
        },
    );
    assert_eq!(
        FusedSeries::deserialize(&bytes).unwrap().tree(),
        node.tree()
    );

    let wrong_schema = mutate_envelope_payload(&bytes, |payload| {
        let mut fields = payload.output_schema.fields().to_vec();
        fields.swap(0, 1);
        payload.output_schema = Schema::new(fields);
    });
    assert_decode_error(&wrong_schema, "SeriesFrame schema does not match");

    // Exported state has no row layout, so a payload that claims rows is refused.
    let wrong_format = mutate_envelope_payload(&bytes, |payload| {
        payload.format = FusedOutputFormat::PlainRows;
    });
    assert_decode_error(&wrong_format, "Partial stage requires SeriesFrame output");
}

#[test]
fn fused_series_encoding_round_trips_large_tag_lists() {
    let tags: Vec<String> = (0..1025).map(|i| format!("t{i}")).collect();
    let node = envelope_node_with_tags(tags);
    let decoded = FusedSeries::deserialize(&node.serialize().unwrap()).unwrap();
    assert_eq!(decoded.binding().tags, node.binding().tags);
}

#[test]
fn bound_tags_cover_primary_key_checks_all_primary_key_columns() {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(9, 9));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "host",
                ConcreteDataType::string_datatype(),
                false,
            ),
            semantic_type: api::v1::SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "zone",
                ConcreteDataType::string_datatype(),
                false,
            ),
            semantic_type: api::v1::SemanticType::Tag,
            column_id: 2,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: api::v1::SemanticType::Timestamp,
            column_id: 3,
        });
    builder.primary_key(vec![1, 2]);
    let metadata = Arc::new(builder.build().unwrap());
    let source = |ids: &[u32]| RawLabelSource::PrimaryKey {
        metric_name: Arc::from("m"),
        physical_metadata: Arc::clone(&metadata),
        logical_tags: ids
            .iter()
            .map(|id| BoundTag {
                column_id: *id,
                name: Arc::from(if *id == 1 { "host" } else { "zone" }),
            })
            .collect::<Vec<_>>()
            .into(),
    };
    assert!(!super::bound_tags_cover_primary_key(&source(&[1])));
    assert!(super::bound_tags_cover_primary_key(&source(&[1, 2])));
    assert!(!super::bound_tags_cover_primary_key(
        &RawLabelSource::Explicit
    ));

    let mut metric_builder = RegionMetadataBuilder::new(RegionId::new(9, 10));
    metric_builder
        .push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            semantic_type: api::v1::SemanticType::Tag,
            column_id: store_api::storage::consts::ReservedColumnId::table_id(),
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                false,
            ),
            semantic_type: api::v1::SemanticType::Tag,
            column_id: store_api::storage::consts::ReservedColumnId::tsid(),
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "host",
                ConcreteDataType::string_datatype(),
                false,
            ),
            semantic_type: api::v1::SemanticType::Tag,
            column_id: 12,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: api::v1::SemanticType::Timestamp,
            column_id: 14,
        });
    metric_builder.primary_key(vec![
        store_api::storage::consts::ReservedColumnId::table_id(),
        store_api::storage::consts::ReservedColumnId::tsid(),
        12,
    ]);
    let metric_metadata = Arc::new(metric_builder.build().unwrap());
    let metric_source = |ids: &[u32]| RawLabelSource::PrimaryKey {
        metric_name: Arc::from("m"),
        physical_metadata: Arc::clone(&metric_metadata),
        logical_tags: ids
            .iter()
            .map(|id| BoundTag {
                column_id: *id,
                name: Arc::from(if *id == 12 { "host" } else { "internal" }),
            })
            .collect::<Vec<_>>()
            .into(),
    };
    assert!(super::bound_tags_cover_primary_key(&metric_source(&[12])));
    assert!(!super::bound_tags_cover_primary_key(&metric_source(&[])));

    let mut user_named_builder = RegionMetadataBuilder::new(RegionId::new(9, 11));
    user_named_builder
        .push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "__tsid",
                ConcreteDataType::string_datatype(),
                false,
            ),
            semantic_type: api::v1::SemanticType::Tag,
            column_id: 20,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "__table_id",
                ConcreteDataType::string_datatype(),
                false,
            ),
            semantic_type: api::v1::SemanticType::Tag,
            column_id: 21,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: api::v1::SemanticType::Timestamp,
            column_id: 22,
        });
    user_named_builder.primary_key(vec![20, 21]);
    let user_named_metadata = Arc::new(user_named_builder.build().unwrap());
    let user_named_source = |ids: &[u32]| RawLabelSource::PrimaryKey {
        metric_name: Arc::from("ordinary"),
        physical_metadata: Arc::clone(&user_named_metadata),
        logical_tags: ids
            .iter()
            .map(|id| BoundTag {
                column_id: *id,
                name: Arc::from(if *id == 20 { "__tsid" } else { "__table_id" }),
            })
            .collect::<Vec<_>>()
            .into(),
    };
    assert!(!super::bound_tags_cover_primary_key(
        &user_named_source(&[])
    ));
    assert!(!super::bound_tags_cover_primary_key(&user_named_source(&[
        20
    ])));
    assert!(super::bound_tags_cover_primary_key(&user_named_source(&[
        20, 21
    ])));
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
///
/// The wording is Prometheus' own for this defect - a vector holding two series under one label set -
/// and the label set is rendered the way the reader writes it, without the empty value.
const DUPLICATE_LABEL_SET: &str =
    "vector cannot contain metrics with the same labelset {host=\"web-1\"}";

/// Two raw series that are already one Prometheus identity are refused by the merge that would
/// have to answer both.
///
/// `{host="web-1", zone=""}` and `{host="web-1"}` are one identity to Prometheus: an empty label
/// value is no label, so the two chunks are the same series and `rate` creates no collision here.
/// What the two *raw* keys differ in is the empty `zone` against the absent one, which the scan
/// encodes differently and the decoder therefore opens two input series for. `rate` drops the
/// metric name and the empty value, so the second raw series arrives on the canonical label set the
/// first one already occupies, and the merge collecting the window's output refuses it: the
/// refusal names that label set rather than answering it, because answering would report one point
/// twice.
#[tokio::test]
async fn differing_raw_keys_that_close_on_one_label_set_are_refused() {
    let exec = plan_of(
        zone_node_with(canonical_rate_tree_of(raw_schema())),
        memory_exec(frames(&zone_chunks())),
    )
    .expect("the runtime executes a canonical rate tree");
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
/// error staged here is the truncated second series: the first one's rows are handed out, and the
/// second one ends the input without saying it was its last chunk, which the frame decoder refuses.
#[tokio::test]
async fn a_runtime_error_after_rows_releases_the_input() {
    let dropped = Arc::new(AtomicBool::new(false));
    let schema = frame_schema();
    // The second series is cut off: its chunk does not close it.
    let mut chunks = zone_chunks();
    chunks.pop().expect("the collision stages two series");
    let samples = series_samples(1.0);
    chunks.push(chunk_of(
        &raw_schema(),
        "zone-absent",
        metric(&[(HOST, "web-1")], Some(METRIC)),
        &times_of(&samples),
        vec![values_of(&samples)],
        false,
    ));
    let batches = frames(&chunks);
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
        error.to_string().contains("ended before key was final"),
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

/// A collision produced inside one native execution is refused by the merge that collects it.
///
/// This is the collision the canonical label set exists for, at the layer that sees it:
/// `http_requests_total{host="web-1"}` and `http_errors_total{host="web-1"}` reach the window as two
/// identities and leave it as `{host="web-1"}`. [`FusedSeries`] reads one metric per scan, so this
/// collision cannot be staged through it; the runtime is driven directly, with the names on the raw
/// chunks. The window is handed over collected, as the canonical pass places it: nothing is answered
/// until its input ends, and the merge then refuses the second value of the one label set.
#[test]
fn two_differently_named_series_collide_once_rate_drops_the_name() {
    let tree = canonical_rate_tree_of(raw_schema());
    let mut execution = NativeExecutor::new(
        NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default()), None)
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
    // Both series sit in the merge's collection: the second one puts a value where the first one
    // already holds one, and the merge refuses the pair as it collects them.
    let error = execution
        .end_of_input()
        .and_then(|()| execution.next_chunk().map(|_| ()))
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

/// The figures the fused node at the top of `plan` registered, exactly as the tree registered them.
fn node_metric_set(plan: &Arc<dyn ExecutionPlan>) -> MetricsSet {
    plan.downcast_ref::<FusedSeriesExec>()
        .expect("the plan under test is one fused node")
        .node_metric_set()
}

/// How many figures - as opposed to structure, such as the tree a node runs - `metrics` holds.
fn node_figures(metrics: &MetricsSet) -> usize {
    metrics
        .iter()
        .filter(|metric| !matches!(metric.value(), MetricValue::Custom { .. }))
        .count()
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

/// The live tree registered in a physical operator's own metrics.
fn native_tree(metrics: &MetricsSet) -> &NativeTreeMetric {
    metrics
        .iter()
        .find_map(|metric| match metric.value() {
            MetricValue::Custom { name, value } if name.as_ref() == NATIVE_TREE_METRIC => {
                value.as_any().downcast_ref::<NativeTreeMetric>()
            }
            _ => None,
        })
        .expect("the operator reports its native tree")
}

fn tree_output_values(tree: &NativeTreeMetrics) -> usize {
    tree.metrics
        .iter()
        .find_map(|(name, value)| (name == "output_values").then_some(*value))
        .unwrap_or(0)
}

/// The logical node prints the compiled tree without metrics.
#[test]
fn fmt_for_explain_prints_the_native_tree() {
    let text = Explain(&fused(rate_tree())).to_string();
    assert!(
        text.starts_with("FusedSeries\nnative_tree=[\nWindow: rate [300000ms]"),
        "{text}"
    );
    assert!(text.contains("\n  Input: raw samples source=0"), "{text}");
    assert!(!text.contains("metrics="), "{text}");
}

/// The physical node prints the tree it was built from, rendered by the tree's own display.
#[test]
fn fmt_as_prints_the_native_tree() {
    let exec =
        plan(rate_tree(), memory_exec(Vec::new())).expect("the runtime executes a rate tree");
    let text = displayable(exec.as_ref()).indent(false).to_string();
    assert!(!text.contains("metrics="), "{text}");

    assert!(
        text.starts_with("FusedSeriesExec\nnative_tree=[\nWindow: rate"),
        "the node line precedes its tree: {text}"
    );
    // A distinctive line of the compiled tree: the window's bound range, offset, anchor and grid.
    assert!(
        text.contains("\nWindow: rate [300000ms] offset 0 anchor - on grid 300000/60000x5"),
        "the node line carries the tree it runs: {text}"
    );
    assert!(
        text.contains("\n  Input: raw samples"),
        "the node line carries the tree's source: {text}"
    );
}

/// Physical ANALYZE renders the figures registered on each tree node on that node's own line.
#[tokio::test]
async fn fmt_as_reports_node_metrics_after_execution() {
    let exec = plan(rate_tree(), memory_exec(two_series_frames())).unwrap();
    collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .unwrap();
    let text = displayable(exec.as_ref()).indent(false).to_string();
    assert!(
        text.starts_with("FusedSeriesExec\nnative_tree=[\nWindow: rate"),
        "{text}"
    );
    // Each node carries its own figures. A figure anywhere in the text says nothing about which
    // node recorded it: the window's and the source's are found on their own lines.
    let window = tree_line(&text, "Window: rate");
    assert!(
        window.contains(", metrics=["),
        "the window's line carries no figures of its own: {window}\n{text}"
    );
    assert!(window.contains("output_values:"), "{window}\n{text}");
    let input = tree_line(&text, "Input: raw samples source=0");
    assert!(
        input.contains(", metrics=["),
        "the source's line carries no figures of its own: {input}\n{text}"
    );
    assert!(input.contains("input_batches:"), "{input}\n{text}");
}

/// The one line of the tree block that starts with `prefix`, without its indentation.
fn tree_line<'a>(text: &'a str, prefix: &str) -> &'a str {
    text.lines()
        .map(str::trim_start)
        .find(|line| line.starts_with(prefix))
        .unwrap_or_else(|| panic!("no line starts with {prefix:?}: {text}"))
}

/// The figures of the tree a node runs are the tree's, not the node's own.
///
/// [`ExecutionPlan::metrics`] is a flat, name-keyed bag, so reporting the tree's figures through it
/// would attribute every node's figures to this one node. What this node reports is therefore only
/// what an operator of its own adds, and the tree - which reads as structure, not as a figure - is
/// still there to be read.
#[tokio::test]
async fn metrics_reports_no_figure_of_the_tree_it_runs() {
    let exec = plan(rate_tree(), memory_exec(two_series_frames()))
        .expect("the runtime executes a rate tree");
    collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("the node runs");

    let reported = exec
        .metrics()
        .expect("the node reports what it records itself");
    let names = reported
        .iter()
        .map(|metric| metric.value().name().to_string())
        .collect::<Vec<_>>();
    // The tree is reported as itself rather than flattened into a number: it is the one entry a
    // node keeps of the tree it runs, and the assertion below is about the figures it leaves out.
    assert!(
        names.iter().any(|name| name == NATIVE_TREE_METRIC),
        "the node reports no tree at all: {names:?}"
    );
    let own = names
        .iter()
        .filter(|name| is_node_figure(name))
        .collect::<Vec<_>>();
    assert!(
        own.is_empty(),
        "the node reports the figures of the tree it runs as its own: {own:?}"
    );

    // The same run's figures are registered under the path of the node that recorded each one, so
    // what `metrics()` leaves out has not been dropped: it has moved to the set that keys by path.
    let registered = node_metric_set(&exec);
    let registered_names = registered
        .iter()
        .map(|metric| metric.value().name().to_string())
        .collect::<Vec<_>>();
    let tree_figures = registered_names
        .iter()
        .filter(|name| is_node_figure(name))
        .count();
    assert!(
        tree_figures > 0,
        "the run registered no figure under a node's path: {registered_names:?}"
    );
    assert!(
        registered_names
            .iter()
            .any(|name| name == "native.root.output_values"),
        "{registered_names:?}"
    );
    assert!(
        registered_names
            .iter()
            .any(|name| name == "native.root.input.output_values"),
        "{registered_names:?}"
    );
    assert!(
        count(&registered, "native.root.output_values") > 0,
        "the run recorded no output for the window"
    );
}

/// Caching membership preserves the old filtering rule and the live tree's changing values.
#[tokio::test]
async fn metrics_membership_is_fixed_while_native_tree_values_change() {
    let exec = plan(rate_tree(), memory_exec(two_series_frames())).unwrap();
    let before = exec.metrics().unwrap();
    let names = |metrics: &MetricsSet| {
        metrics
            .iter()
            .map(|metric| metric.value().name().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(vec![NATIVE_TREE_METRIC.to_string()], names(&before));
    assert_eq!(0, tree_output_values(&native_tree(&before).report()));

    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .unwrap();
    let output_values = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert!(output_values > 0);

    let after = exec.metrics().unwrap();
    assert_eq!(before.iter().count(), after.iter().count());
    assert_eq!(names(&before), names(&after));
    for (initial, current) in before.iter().zip(after.iter()) {
        assert!(Arc::ptr_eq(initial, current));
    }
    // The handle captured before execution must observe values registered afterwards, too.
    assert_eq!(
        output_values,
        tree_output_values(&native_tree(&before).report())
    );
    assert_eq!(
        output_values,
        tree_output_values(&native_tree(&after).report())
    );

    // Compare the cached membership against the complete set using the pre-cache filtering rule.
    // This also catches a future operator metric registered during execute but omitted by the cache.
    let registered = node_metric_set(&exec);
    let formerly_reported = registered
        .iter()
        .filter(|metric| {
            matches!(metric.value(), MetricValue::Custom { .. })
                || !is_node_metric(metric.value().name())
        })
        .map(|metric| metric.value().name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(formerly_reported, names(&after));
}

/// The complete registration and the ANALYZE tree retain figures from every execution partition.
#[tokio::test]
async fn native_tree_metrics_preserve_all_partitions() {
    let partitions = [1.0, 2.0, 3.0]
        .into_iter()
        .enumerate()
        .map(|(partition, scale)| {
            vec![frame(&series_chunk(
                &format!("web-{partition}"),
                &series_samples(scale),
                true,
            ))]
        })
        .collect::<Vec<_>>();
    let exec = plan(rate_tree(), memory_exec_partitioned(partitions)).unwrap();
    let own = exec.metrics().unwrap();
    assert_eq!(0, tree_output_values(&native_tree(&own).report()));
    let context = SessionContext::default().task_ctx();

    // Read the same cached handle after each partition; it must see later registrations as well.
    for partition in 0..3 {
        let batches = exec
            .execute(partition, Arc::clone(&context))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            GRID_LEN as usize,
            batches.iter().map(RecordBatch::num_rows).sum::<usize>()
        );

        let registered = node_metric_set(&exec);
        for completed in 0..=partition {
            let metric = registered
                .iter()
                .find(|metric| {
                    metric.value().name() == "native.root.output_values"
                        && metric.partition() == Some(completed)
                })
                .expect("the complete set retains each partition's output counter");
            assert_eq!(GRID_LEN as usize, metric.value().as_usize());
            assert!(registered.iter().any(|metric| {
                metric.value().name() == "native.root.input.input_batches"
                    && metric.partition() == Some(completed)
                    && metric.value().as_usize() > 0
            }));
            assert!(registered.iter().any(|metric| {
                metric.value().name() == format!("native.p{completed}.root.peak_reserved_bytes")
                    && metric.partition() == Some(completed)
            }));
        }
        let output_values = (partition + 1) * GRID_LEN as usize;
        assert_eq!(
            output_values,
            count(&registered, "native.root.output_values")
        );
        let report = native_tree(&own).report();
        assert_eq!(output_values, tree_output_values(&report));
        // ANALYZE first aggregates operator metrics by name before reading the Custom tree.
        let analyzed = native_tree(&own.aggregate_by_name()).report();
        assert_eq!(output_values, tree_output_values(&analyzed));
        assert!(
            report.children[0]
                .metrics
                .iter()
                .any(|(name, value)| { name == "input_batches" && *value == partition + 1 })
        );

        let text = displayable(exec.as_ref()).indent(false).to_string();
        assert!(
            tree_line(&text, "Window: rate").contains(&format!("output_values: {output_values}")),
            "{text}"
        );
    }
}

/// Whether `name` is one of the figures a node of a tree registered, `native.<path>.<metric>`.
///
/// The tree an operator runs is registered under [`NATIVE_TREE_METRIC`], which shares the prefix
/// but names no node and reports no figure.
fn is_node_figure(name: &str) -> bool {
    NodeMetric::ALL.iter().any(|metric| {
        name.strip_suffix(metric)
            .and_then(|head| head.strip_suffix('.'))
            .is_some_and(|path| {
                path.strip_prefix("native.")
                    .is_some_and(|path| !path.is_empty())
            })
    })
}

/// An instant selection over `source_index` that reads `lookback_ms` back and reports `value`.
fn instant_selecting(
    source_index: usize,
    grid: &Arc<EvalGrid>,
    lookback_ms: i64,
    value: InstantValue,
) -> NativeNode {
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
                lookback_ms,
                offset_ms: 0,
                at_ms: None,
                value,
                output_grid: Arc::clone(grid),
            },
        },
        output: evaluated_of(&raw_schema(), grid),
    }
}

/// `info(up, {version}) + clamp_min(up, scalar(threshold))`.
///
/// The sum's two sides are an info join and a parameterized map, so the tree holds every segment
/// the display and the runtime name by hand: `left` and `right` twice, and `parameter0` once. Its
/// four sources are `up` for the join, the info series that join reads, `up` again for the map, and
/// the `threshold` the map is parameterized by.
fn info_and_parameterized_tree() -> NativeTree {
    let grid = grid();
    let evaluated = evaluated_of(&raw_schema(), &grid);
    let enriched = NativeNode {
        expr: NativeExpr::Info {
            base: Box::new(instant_selecting(0, &grid, 0, InstantValue::Sample)),
            // The kernel reads the info side's values as sample times, so the selection reports
            // those times rather than the values it selected.
            info: Box::new(instant_selecting(
                1,
                &grid,
                RANGE_MS,
                InstantValue::InfoTimestamp,
            )),
            selector: InfoSelector {
                name_matchers: Vec::new(),
                data_labels: vec![Arc::from("version")],
                keep_unmatched: false,
            },
        },
        output: evaluated.clone(),
    };
    let clamped = NativeNode {
        expr: NativeExpr::ParameterizedMap {
            input: Box::new(instant_selecting(2, &grid, 0, InstantValue::Sample)),
            op: ParameterizedOp::ClampMin,
            parameters: vec![NativeNode {
                expr: NativeExpr::ScalarOf {
                    input: Box::new(instant_selecting(3, &grid, 0, InstantValue::Sample)),
                },
                output: NativeType::Scalar(Arc::clone(&grid)),
            }],
        },
        output: evaluated.clone(),
    };
    let NativeType::Samples(schema) = &evaluated else {
        panic!("an evaluated vector is samples");
    };
    let output = crate::vector_match::binary_output_schema(schema, schema)
        .expect("two operands on one grid are matched");
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(enriched),
            right: Box::new(clamped),
            matching: BinaryMatching {
                op: BinaryOp::Add,
                return_bool: false,
                labels: MatchingLabels::On(vec![Arc::from(HOST)]),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output: NativeType::Samples(output),
    })
    .expect("an info join and a parameterized map are legal operands of a sum")
}

/// One chunk of the series `name` under `labels`, keyed `key`: `rows` as its samples, and the chunk
/// that closes the series when `last`.
fn named_chunk(
    name: &str,
    key: &str,
    labels: &[(&str, &str)],
    rows: &[(i64, f64)],
    last: bool,
) -> RawSeriesChunk {
    chunk_of(
        &raw_schema(),
        key,
        metric(labels, Some(name)),
        &times_of(rows),
        vec![values_of(rows)],
        last,
    )
}

/// The node paths `figures` was registered under, read off the names `native.<path>.<metric>`.
///
/// A figure that must not be merged across partitions carries its partition in its name, and a
/// partition belongs to the registration rather than to the node, so it is dropped here. The
/// tree's own record is not a figure of any node, so it names no path.
fn registered_paths(figures: &MetricsSet) -> BTreeSet<String> {
    figures
        .iter()
        .filter_map(|metric| {
            let name = metric.value().name();
            let rest = name.strip_prefix("native.")?;
            let rest = match rest.split_once('.') {
                Some((head, tail))
                    if head.len() > 1
                        && head.starts_with('p')
                        && head[1..].bytes().all(|byte| byte.is_ascii_digit()) =>
                {
                    tail
                }
                _ => rest,
            };
            let (path, _) = rest.rsplit_once('.')?;
            Some(path.to_string())
        })
        .collect()
}

/// Every node of an executed tree shows its own figures, whichever side of a pair or parameter it
/// sits on.
///
/// The display names a node's child by the segment the runtime registered it under, and the two
/// lists are kept by hand: `left` and `right` for the sides of a pair, `parameter` for the `k` of
/// a `topk` and `parameter<index>` for the arguments of a histogram function or a map. A display
/// that names a child where nothing was registered loses that node's figures without failing
/// anything else - what it prints is still a tree, and the run still records what it recorded - so
/// what this test pins is that no path the run filed figures under goes unshown.
#[tokio::test]
async fn every_node_of_an_executed_tree_reaches_its_own_figures() {
    let tree = info_and_parameterized_tree();
    let up = named_chunk(
        METRIC,
        "web-1",
        &[(HOST, "web-1"), ("instance", "a")],
        &series_samples(1.0),
        true,
    );
    // The info series the join takes `version` from, sampled where the grid reads it.
    let info = named_chunk(
        "target_info",
        "info",
        &[(HOST, "web-1"), ("instance", "a"), ("version", "v1")],
        &[(GRID_START, 1.0)],
        true,
    );
    let threshold = named_chunk(
        METRIC,
        "web-1",
        &[(HOST, "web-1"), ("instance", "a")],
        &grid_timestamps()
            .into_iter()
            .map(|time| (time, 2.0))
            .collect::<Vec<_>>(),
        true,
    );
    let exec = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        (0..4).map(|_| input_plan(frame_schema())).collect(),
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the binding matches the tree")
    .to_execution_plan(
        [frame(&up), frame(&info), frame(&up), frame(&threshold)]
            .into_iter()
            .map(|batch| {
                (
                    memory_exec(vec![batch]),
                    RawLabelSource::Explicit,
                    SeriesOrdering::Contiguous,
                )
            })
            .collect(),
    )
    .expect("the tree runs");
    collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("the node runs");

    // What the run filed, keyed by the path of the node that recorded it. Both sides of the sum
    // and the parameter of the map are among the paths, so the tree below is the one described.
    let figures = node_metric_set(&exec);
    let paths = registered_paths(&figures);
    for path in [
        "root.left",
        "root.left.left",
        "root.left.right",
        "root.right",
        "root.right.input",
        "root.right.parameter0",
    ] {
        assert!(
            paths.contains(path),
            "the run filed nothing under {path}: {paths:?}"
        );
    }

    // The displayed tree, one line per node.
    let text = displayable(exec.as_ref()).indent(false).to_string();
    let lines = text
        .lines()
        .skip_while(|line| *line != "native_tree=[")
        .skip(1)
        .take_while(|line| *line != "]")
        .collect::<Vec<_>>();
    assert_eq!(
        paths.len(),
        lines.len(),
        "the display prints {} nodes where {} recorded figures:\n{text}",
        lines.len(),
        paths.len()
    );
    // Each line shows the figures of the path that node was registered under - its own, and only
    // its own - so a node whose figures the display never reached has no line that shows them.
    let registered: ExecutionPlanMetricsSet = figures.into();
    let snapshot = NodeMetricsSnapshot::read(&registered);
    for path in &paths {
        let described = snapshot.described(path);
        assert!(!described.is_empty(), "{path} recorded no figure at all");
        let shown = described
            .iter()
            .map(|(name, value)| format_metric(name, *value))
            .collect::<Vec<_>>()
            .join(", ");
        assert!(
            lines
                .iter()
                .any(|line| line.ends_with(&format!("metrics=[{shown}]"))),
            "no node line shows what {path} recorded ({shown}):\n{text}"
        );
    }

    // `topk(scalar(up), up)`: a `k` that changes per point is the topk's `parameter`, so the
    // segment the runtime files it under is `parameter` and not `left`, `input` or a bare list.
    let grid = grid();
    let vector = instant_of(0, &grid);
    let output = vector.output.clone();
    let per_point_k = NativeTree::try_new(NativeNode {
        expr: NativeExpr::ParameterizedTopK {
            input: Box::new(vector),
            op: BoundVectorOp::TopK {
                k: 0.0,
                top: true,
                group: Grouping::By(Arc::from([])),
            },
            parameter: Box::new(NativeNode {
                expr: NativeExpr::ScalarOf {
                    input: Box::new(instant_of(1, &grid)),
                },
                output: NativeType::Scalar(Arc::clone(&grid)),
            }),
        },
        output,
    })
    .expect("a topk over a per-point k is a legal tree");
    let (lines, snapshot) = displayed_tree_of(
        &per_point_k,
        vec![
            frames(&[series_chunk("web-1", &series_samples(1.0), true)]),
            frames(&[series_chunk("web-2", &series_samples(1.0), true)]),
        ],
    )
    .await;
    assert_eq!(
        per_point_k.to_string().lines().count(),
        lines.len(),
        "the display prints {} lines for a tree of {} nodes:\n{}",
        lines.len(),
        per_point_k.to_string().lines().count(),
        lines.join("\n")
    );
    assert_lines_show_their_own_figures(
        &per_point_k,
        &lines,
        &snapshot,
        &[
            (0, "root"),
            (1, "root.input"),
            (2, "root.input.input"),
            (3, "root.parameter"),
            (4, "root.parameter.input"),
            (5, "root.parameter.input.input"),
        ],
    );

    // `histogram_fraction(<lower>, <upper>, rate(buckets[5m]))`: both arguments are read at every
    // grid point, so each is one of the histogram's parameters, filed under `parameter<index>` in
    // the order the statement wrote them. A histogram function is bound over its bucket vector
    // whether or not a fold sits in between, so the window is the whole input here.
    let rate = rate_tree();
    let histogram_output = rate.root().output.clone();
    let eval_time = || NativeNode {
        expr: NativeExpr::EvalTime { date: None },
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let fraction = NativeTree::try_new(NativeNode {
        expr: NativeExpr::ParameterizedHistogram {
            input: Box::new(rate.root().clone()),
            op: BoundVectorOp::Histogram(HistogramFunction::Fraction {
                lower: 0.0,
                upper: 0.0,
            }),
            parameters: vec![
                eval_time(),
                NativeNode {
                    expr: NativeExpr::Binary {
                        left: Box::new(eval_time()),
                        right: Box::new(NativeNode {
                            expr: NativeExpr::Scalar(BoundScalar { value: 0.5 }),
                            output: NativeType::Scalar(Arc::clone(&grid)),
                        }),
                        matching: BinaryMatching {
                            op: BinaryOp::Add,
                            return_bool: false,
                            labels: MatchingLabels::Ignoring(Vec::new()),
                            cardinality: Cardinality::OneToOne,
                            fill: FillValues::default(),
                        },
                    },
                    output: NativeType::Scalar(Arc::clone(&grid)),
                },
            ],
        },
        output: histogram_output,
    })
    .expect("a histogram over two per-point parameters is a legal tree");
    let (lines, snapshot) = displayed_tree_of(
        &fraction,
        vec![frames(&[
            labelled_chunk("a-1", &[("le", "1")], &series_samples(1.0), true),
            labelled_chunk("a-inf", &[("le", "+Inf")], &series_samples(4.0), true),
        ])],
    )
    .await;
    assert_eq!(
        fraction.to_string().lines().count(),
        lines.len(),
        "the display prints {} lines for a tree of {} nodes:\n{}",
        lines.len(),
        fraction.to_string().lines().count(),
        lines.join("\n")
    );
    assert_lines_show_their_own_figures(
        &fraction,
        &lines,
        &snapshot,
        &[
            (0, "root"),
            (1, "root.input"),
            (2, "root.input.input"),
            (3, "root.parameter0"),
            (4, "root.parameter1"),
            (5, "root.parameter1.left"),
        ],
    );
}

/// `tree` executed over `sources` - one source's frames per element - and the tree block the
/// executed node displays, one line per node, with the figures the run filed.
async fn displayed_tree_of(
    tree: &NativeTree,
    sources: Vec<Vec<RecordBatch>>,
) -> (Vec<String>, NodeMetricsSnapshot) {
    let exec = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        binding(tree),
        (0..sources.len())
            .map(|_| input_plan(frame_schema()))
            .collect(),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the binding matches the tree")
    .to_execution_plan(
        sources
            .into_iter()
            .map(|batches| {
                (
                    memory_exec(batches),
                    RawLabelSource::Explicit,
                    SeriesOrdering::Contiguous,
                )
            })
            .collect(),
    )
    .expect("the tree runs");
    collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("the node runs");
    let text = displayable(exec.as_ref()).indent(false).to_string();
    let lines = text
        .lines()
        .skip_while(|line| *line != "native_tree=[")
        .skip(1)
        .take_while(|line| *line != "]")
        .map(str::to_string)
        .collect::<Vec<_>>();
    let registered: ExecutionPlanMetricsSet = node_metric_set(&exec).into();
    (lines, NodeMetricsSnapshot::read(&registered))
}

/// The line at `index` of the display of `tree` is the tree's own line for the node at `path`,
/// with exactly that node's figures appended.
///
/// The line is compared whole - its indentation, its words and its figures - so a display that
/// reads a child's figures from a segment the runtime did not file them under prints a line that
/// has no figures at all, and one that shows another node's figures prints figures that differ.
fn assert_lines_show_their_own_figures(
    tree: &NativeTree,
    lines: &[String],
    snapshot: &NodeMetricsSnapshot,
    lines_of_paths: &[(usize, &str)],
) {
    let shape = tree.to_string();
    let shape = shape.lines().collect::<Vec<_>>();
    for (index, path) in lines_of_paths {
        let described = snapshot.described(path);
        assert!(
            !described.is_empty(),
            "{path} recorded no figure at all: {snapshot:?}"
        );
        let shown = described
            .iter()
            .map(|(name, value)| format_metric(name, *value))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(
            lines[*index],
            format!("{}, metrics=[{shown}]", shape[*index]),
            "the line at {index} is not {path} with its own figures:\n{}",
            lines.join("\n")
        );
    }
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
    let metrics = node_metric_set(&exec);
    assert_eq!(1, count(&metrics, "native.root.input.completed_series"));
    assert_eq!(1, count(&metrics, "native.root.completed_series"));
}

/// A tree the runtime does not execute is refused when the execution plan is built.
///
/// The runtime folds every supported `Complete` kind now, `quantile` among them, so the tree below
/// asks for a grouping no kernel can address - a group by a value field. What the test pins is the
/// refusal happening while the plan is built, not the particular kind of node that provokes it.
#[tokio::test]
async fn a_tree_the_runtime_cannot_run_is_rejected_when_the_exec_is_built() {
    let node = fused(ungroupable_aggregate_tree());
    let error = plan_of(node, memory_exec(Vec::new()))
        .expect_err("the runtime does not fold a group by a value field");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "the runtime refuses the tree it cannot run: {error}"
    );
    assert!(error.to_string().contains("per-point"), "{error}");
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

#[test]
fn fragmented_raw_input_is_reconciled_even_at_width_one() {
    use crate::extension_plan::{IdentityCompletion, SeriesRepartitionExec, SeriesRouting};
    use common_query::series_frame::frame_schema_with;

    let make_input = |ordering| {
        let declaration = SeriesFrameDeclaration::parse(frame_schema().as_ref()).unwrap();
        let schema = frame_schema_with(
            declaration.series(),
            declaration.labels(),
            declaration.label_decode(),
            ordering,
        )
        .unwrap();
        memory_exec_with(schema, Vec::new())
    };
    for ordering in [SeriesOrdering::Contiguous, SeriesOrdering::Interleaved] {
        let plan = fused(rate_tree())
            .to_execution_plan(vec![(
                make_input(ordering),
                RawLabelSource::Explicit,
                ordering,
            )])
            .unwrap();
        let child = plan.children()[0];
        if ordering == SeriesOrdering::Interleaved {
            let router = child
                .downcast_ref::<SeriesRepartitionExec>()
                .expect("fragmented input needs an owner even for one partition");
            assert_eq!(router.routing_policy(), &SeriesRouting::RawPrimaryKey);
            assert_eq!(
                router.identity_completion(),
                IdentityCompletion::MergeInputs
            );
            assert_eq!(router.outputs(), 1);
            assert_eq!(
                series_frame_ordering(child.schema().as_ref()).unwrap(),
                SeriesOrdering::Contiguous
            );
        } else {
            assert!(
                child.downcast_ref::<SeriesRepartitionExec>().is_none(),
                "disjoint input needs no router"
            );
        }
    }
}

/// Disjoint regional streams keep their contiguous guarantee and need no raw owner.
#[tokio::test]
async fn contiguous_raw_input_runs_rate_directly_at_any_width() {
    use crate::extension_plan::SeriesRepartitionExec;

    let samples = series_samples(1.0);
    let region_batch = |key, samples: &[(i64, f64)]| {
        let batch = frame(&series_chunk(key, samples, true));
        RecordBatch::try_new(frame_schema(), batch.columns().to_vec()).unwrap()
    };
    let first = region_batch("web-1", &samples);
    let second = region_batch("web-2", &samples);
    for partitions in [
        vec![vec![first.clone(), second.clone()]],
        vec![vec![first.clone()], vec![second.clone()]],
    ] {
        let input = memory_exec_partitioned_with(frame_schema(), partitions);
        let exec = fused(rate_tree())
            .to_execution_plan(vec![(
                input,
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            )])
            .unwrap();
        assert!(
            exec.children()[0]
                .downcast_ref::<SeriesRepartitionExec>()
                .is_none(),
            "disjoint contiguous input needs no reconciliation"
        );
        let batches = collect(exec, SessionContext::default().task_ctx())
            .await
            .unwrap();
        assert_series_rates(&rows(&batches), &[(1.0, "web-1"), (1.0, "web-2")]);
    }
}

/// Two region-local chains of one key produce one rate series at either transport width.
#[tokio::test]
async fn fragmented_raw_input_merges_region_chains_before_rate() {
    use common_query::series_frame::frame_schema_with;

    let declaration = SeriesFrameDeclaration::parse(frame_schema().as_ref()).unwrap();
    let schema = frame_schema_with(
        declaration.series(),
        declaration.labels(),
        declaration.label_decode(),
        SeriesOrdering::Interleaved,
    )
    .unwrap();
    let samples = series_samples(1.0);
    let region_batch = |samples: &[(i64, f64)]| {
        let batch = frame(&series_chunk("web-1", samples, true));
        RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).unwrap()
    };
    let early = region_batch(&samples[..5]);
    let late = region_batch(&samples[5..]);
    for partitions in [
        vec![vec![early.clone(), late.clone()]],
        vec![vec![early.clone()], vec![late.clone()]],
    ] {
        let input = memory_exec_partitioned_with(schema.clone(), partitions);
        let exec = fused(rate_tree())
            .to_execution_plan(vec![(
                input,
                RawLabelSource::Explicit,
                SeriesOrdering::Interleaved,
            )])
            .unwrap();
        let batches = collect(exec, SessionContext::default().task_ctx())
            .await
            .unwrap();
        assert_series_rates(&rows(&batches), &[(1.0, "web-1")]);
    }
}

#[test]
fn fragmented_raw_multi_input_is_reconciled_before_matching_router() {
    use crate::extension_plan::{SeriesRepartitionExec, SeriesRouting};
    use common_query::series_frame::frame_schema_with;
    let tree = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let node = FusedSeries::try_new(
        Arc::from("left + right"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let declaration = SeriesFrameDeclaration::parse(frame_schema().as_ref()).unwrap();
    let schema = frame_schema_with(
        declaration.series(),
        declaration.labels(),
        declaration.label_decode(),
        SeriesOrdering::Interleaved,
    )
    .unwrap();
    let input = || memory_exec_partitioned_with(schema.clone(), vec![vec![], vec![]]);
    let exec = node
        .to_execution_plan_with_parallelism(
            vec![
                (
                    input(),
                    RawLabelSource::Explicit,
                    SeriesOrdering::Interleaved,
                ),
                (
                    input(),
                    RawLabelSource::Explicit,
                    SeriesOrdering::Interleaved,
                ),
            ],
            2,
        )
        .unwrap();
    for child in exec.children() {
        let matching = child
            .downcast_ref::<SeriesRepartitionExec>()
            .expect("matching router");
        assert!(matches!(
            matching.routing_policy(),
            SeriesRouting::Matching(_)
        ));
        let matching_children = child.children();
        let owner = matching_children[0]
            .downcast_ref::<SeriesRepartitionExec>()
            .expect("raw owner below matching router");
        assert_eq!(owner.routing_policy(), &SeriesRouting::RawPrimaryKey);
        assert_eq!(
            series_frame_ordering(child.schema().as_ref()).unwrap(),
            SeriesOrdering::Contiguous
        );
    }
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

fn binary_identity_rows(batches: &[RecordBatch]) -> Vec<(Option<String>, i64, Option<f64>)> {
    batches
        .iter()
        .flat_map(|batch| {
            let time = batch
                .column_by_name(TIME)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let value = batch
                .column_by_name(VALUE)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let host = batch
                .column_by_name(HOST)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| {
                    (
                        host.is_valid(row).then(|| host.value(row).to_string()),
                        time.value(row),
                        value.is_valid(row).then(|| value.value(row)),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Configured matching parallelism overrides both one-partition and uneven source widths.
#[tokio::test]
async fn matching_parallelism_uses_configured_width_without_changing_rows() {
    let tree = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let node = FusedSeries::try_new(
        Arc::from("left + right"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let mk_inputs = |left_parts: Vec<Vec<RecordBatch>>, right_parts: Vec<Vec<RecordBatch>>| {
        vec![
            (
                memory_exec_partitioned(left_parts),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
            (
                memory_exec_partitioned(right_parts),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
        ]
    };
    let left = frame(&series_chunk("web-1", &[(GRID_START, 3.0)], true));
    let right = frame(&series_chunk("web-1", &[(GRID_START, 4.0)], true));
    for (parts, configured_width) in [(1, 4), (3, 4)] {
        let left_parts = (0..parts)
            .map(|i| if i == 0 { vec![left.clone()] } else { vec![] })
            .collect();
        let right_parts = (0..if parts == 3 { 2 } else { 1 })
            .map(|i| if i == 0 { vec![right.clone()] } else { vec![] })
            .collect();
        let inputs = mk_inputs(left_parts, right_parts);
        let parallel = node
            .to_execution_plan_with_parallelism(inputs, configured_width)
            .unwrap();
        assert_eq!(
            configured_width,
            parallel
                .properties()
                .output_partitioning()
                .partition_count()
        );
        let actual = binary_identity_rows(
            &collect(parallel, SessionContext::default().task_ctx())
                .await
                .unwrap(),
        );
        let baseline_inputs = mk_inputs(
            (0..parts)
                .map(|i| if i == 0 { vec![left.clone()] } else { vec![] })
                .collect(),
            (0..if parts == 3 { 2 } else { 1 })
                .map(|i| if i == 0 { vec![right.clone()] } else { vec![] })
                .collect(),
        );
        let baseline = node
            .to_execution_plan_with_parallelism(baseline_inputs, 1)
            .unwrap();
        assert_eq!(
            1,
            baseline
                .properties()
                .output_partitioning()
                .partition_count()
        );
        let mut expected = binary_identity_rows(
            &collect(baseline, SessionContext::default().task_ctx())
                .await
                .unwrap(),
        );
        let mut actual = actual;
        let sort_rows = |rows: &mut Vec<(Option<String>, i64, Option<f64>)>| {
            rows.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then(a.1.cmp(&b.1))
                    .then_with(|| a.2.unwrap_or_default().total_cmp(&b.2.unwrap_or_default()))
            })
        };
        sort_rows(&mut expected);
        sort_rows(&mut actual);
        assert_eq!(
            expected, actual,
            "all labels/timestamps/values match width one without duplicates or omissions"
        );
        assert_eq!(1, actual.len());
        assert_eq!(Some(7.0), actual[0].2);
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
    assert_eq!(3, exec.properties().output_partitioning().partition_count());
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
    let metrics = node_metric_set(&exec);
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
    let metrics = node_metric_set(&exec);
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
        message.contains("frame content does not match its compiled input"),
        "refused for another reason: {message}"
    );
    assert!(
        !message.contains("no partitions"),
        "the partition count is not what refuses this plan: {message}"
    );
}

/// The other content direction: an input compiled to read evaluated values refuses a raw frame.
///
/// A frame says what its rows are and the tree's input node says what it was compiled to read, and
/// the two decoders refuse a frame that declares the other content. Reading raw samples as grid
/// positions would address values that are not there, so the plan is refused when it is built
/// rather than run with a child decoded as something it is not. The frame here declares raw
/// samples and the tree's input is bound to the grid, which is the mirror of
/// [`a_frame_with_an_evaluation_grid_is_rejected_when_the_plan_is_built`].
#[test]
fn a_raw_frame_is_rejected_by_an_input_compiled_to_read_evaluated_values() {
    let error = plan(
        summed_window_tree(),
        memory_exec_with(frame_schema(), Vec::new()),
    )
    .expect_err("a grid-bound input cannot read raw samples");
    let message = error_chain(&error);
    assert!(
        message.contains("frame content does not match its compiled input"),
        "refused for another reason: {message}"
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
            OutputColumn::Field {
                index: 0,
                kind: FieldKind::Float,
            },
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

/// A single repeated source feeds both operands once in each of four independent partitions.
#[tokio::test]
async fn repeated_source_runs_once_per_partition_with_ordered_operands() {
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
    assert_eq!(4, exec.properties().output_partitioning().partition_count());
    assert_eq!(1, exec.children().len());

    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let expected_times = grid_timestamps();
    let mut actual_rows = Vec::new();
    for partition in 0..4 {
        let mut stream = exec
            .execute(partition, Arc::clone(&context))
            .expect("each requested partition exists");
        let batches = stream
            .as_mut()
            .try_collect::<Vec<_>>()
            .await
            .expect("the partition completes without errors");
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
        actual_rows.extend(actual);
        assert_eq!(1, calls_by_partition[partition].load(Ordering::SeqCst));
        // Router and matching buffers may remain charged between output partitions. The final
        // zero below checks that execution-scoped evidence and those buffers are all released.
    }
    assert_eq!(
        0,
        pool.reserved(),
        "execution-scoped evidence is released after all streams"
    );
    actual_rows.sort_by_key(|row| (row.0, row.2.clone()));
    let mut expected_rows = (0..4)
        .flat_map(|partition| {
            expected_times.iter().map(move |&time| {
                let index = ((time - GRID_START) / GRID_STEP) as usize;
                (
                    time,
                    Some(-((partition * 10 + index + 1) as f64)),
                    Some(format!("web-{partition}")),
                )
            })
        })
        .collect::<Vec<_>>();
    expected_rows.sort_by_key(|row| (row.0, row.2.clone()));
    assert_eq!(expected_rows, actual_rows);
    assert_eq!(
        vec![1, 1, 1, 1],
        calls_by_partition
            .iter()
            .map(|call| call.load(Ordering::SeqCst))
            .collect::<Vec<_>>()
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
/// [`Distribution::UnspecifiedDistribution`] is what leaves the child alone, and refusing extra
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
        None,
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
        None,
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

/// Logical equality distinguishes source field layouts even when the binding and output match.
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
    let original = node_metric_set(&exec);
    assert_eq!(1, count(&original, "native.root.completed_series"));

    // Rebuilt over a fresh input of the same frames, the way an optimizer replaces a child.
    let rebuilt = Arc::clone(&exec)
        .replace_children(
            vec![memory_exec(one_series_frames())],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .expect("the node rebuilds over another input");
    // No execution of the rebuilt node has run, so it registered no figures to report: the only
    // entry is the tree it runs, which is structure rather than a figure.
    let before = node_metric_set(&rebuilt);
    assert_eq!(
        0,
        node_figures(&before),
        "the rebuilt node inherited an execution's metrics"
    );
    // Rebuilding does not reset the node it was built from.
    assert_eq!(1, count(&original, "native.root.completed_series"));

    let rebuilt_batches = collect(Arc::clone(&rebuilt), SessionContext::default().task_ctx())
        .await
        .expect("the rebuilt node runs");
    assert_eq!(first_rows, rows(&rebuilt_batches));

    let after = node_metric_set(&rebuilt);
    assert_eq!(1, count(&after, "native.root.completed_series"));
    assert_eq!(1, count(&after, "native.root.input.completed_series"));
    // One execution's set, not two: a copy of the original's roots would answer 2 here.
    assert_eq!(original.iter().count(), after.iter().count());
    let retained = node_metric_set(&exec);
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

    let first = node_metric_set(&exec);
    let names = |metrics: &MetricsSet| -> Vec<String> {
        metrics
            .iter()
            .map(|metric| metric.value().name().to_string())
            .collect()
    };
    let second = node_metric_set(&exec);
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
    let after = node_metric_set(&exec);
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
    let registered = node_figures(&after);
    let second_stream = exec
        .execute(0, task_context(&pool))
        .expect("the plan runs a second time");
    drop(second_stream);
    let twice = node_metric_set(&exec);
    assert_eq!(
        node_figures(&twice),
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
    let again = node_metric_set(&exec);
    assert_eq!(node_figures(&twice), node_figures(&again));
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
fn partial_tree(prefix: NativeTree, kind: AggregateKind, grouped_by: &[&str]) -> NativeTree {
    let input = prefix.root().clone();
    let schema = match &input.output {
        NativeType::Samples(schema) => Arc::clone(schema),
        _ => panic!("partial needs samples"),
    };
    let fields = schema.fields.clone();
    let grid = schema
        .eval_grid
        .clone()
        .expect("partial input is evaluated");
    let (state_kind, components) =
        crate::native::plan::aggregate_state_binding(kind, FieldKind::Float)
            .expect("mergeable aggregate");
    let state = Arc::new(AggregateStateSchema {
        group: Grouping::By(
            grouped_by
                .iter()
                .map(|label| Arc::from(*label))
                .collect::<Vec<_>>()
                .into(),
        ),
        kind: state_kind,
        components: components.into(),
        accumulators: fields,
        grid,
    });
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(input),
            op: BoundAggregate {
                kind,
                group: Grouping::By(
                    grouped_by
                        .iter()
                        .map(|label| Arc::from(*label))
                        .collect::<Vec<_>>()
                        .into(),
                ),
                stage: AggregateStage::Partial,
                field_kind: FieldKind::Float,
            },
        },
        output: NativeType::AggregateState(state),
    })
    .unwrap()
}

fn partial_op(kind: AggregateKind, grouped_by: &[&str]) -> BoundAggregate {
    let labels: Vec<Arc<str>> = grouped_by.iter().map(|label| Arc::from(*label)).collect();
    BoundAggregate {
        kind,
        group: Grouping::By(labels.into()),
        stage: AggregateStage::Complete,
        field_kind: FieldKind::Float,
    }
}

/// The logical node that runs the prefix and exports `kind`'s state, grouped by `host`.
///
/// The prefix's one raw input is bound to source `0`, which is the only child the node carries: a
/// state export is per partition of that one scan, not a second source.
fn partial_node(kind: AggregateKind) -> FusedSeries {
    let tree = rate_tree();
    let tree = partial_tree(tree, kind, &[HOST]);
    let schema = canonical_state_frame_schema(&tree);
    FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding_of(&tree, raw_schema(), vec![HOST.to_string()]),
        vec![input_plan(frame_schema())],
        schema,
        FusedOutputFormat::SeriesFrame,
    )
    .expect("the binding matches the partial tree")
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
    let tree = partial_tree(tree, op.kind, tags);
    FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding_of(
            &tree,
            raw_schema(),
            tags.iter().map(|s| s.to_string()).collect(),
        ),
        vec![input_plan(frame_schema())],
        canonical_state_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )?
    .to_execution_plan(vec![(
        input,
        RawLabelSource::Explicit,
        SeriesOrdering::Contiguous,
    )])
}

/// Reads the output state frames for one partition and validates their declared binding.
async fn state_batches(exec: &Arc<dyn ExecutionPlan>, partition: usize) -> Vec<RecordBatch> {
    exec.execute(partition, SessionContext::default().task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
}

/// Decodes state-frame batches into per-series states, checking their declaration on the way.
///
/// A state frame has no flat accumulator column - each aggregate field is one
/// `List<Struct<component...>>` - so a reader of its values goes through the decoder, which is the
/// same path the `Final` half takes.
async fn decode_state_batches(batches: Vec<RecordBatch>) -> Vec<StateFrameRow> {
    let schema = batches
        .first()
        .expect("a state-exporting node answers at least one batch")
        .schema();
    let declaration = SeriesFrameDeclaration::parse(schema.as_ref()).unwrap();
    assert_eq!(FrameContent::AggregateState, declaration.content());
    let series = Arc::new(declaration.series().clone());
    let state = Arc::new(declaration.aggregate_state().unwrap().to_owned());
    unpack_aggregate_state(
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        )),
        Some(state),
        Some(series),
    )
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap()
}

/// State is keyed by the cut aggregate's grouping, not by the tags its scan was bound with.
///
/// The scan below the fold declares `host` while the aggregate groups by `job` alone, so the exported
/// series carry the group's `job` and not its input's `host`: the key a merge folds on is the
/// aggregate's own output labels, not the columns its input happened to be bound with. Exporting
/// state under the scan's tags would instead split groups the aggregate keeps apart.
#[tokio::test]
async fn a_state_export_keys_its_labels_by_the_cut_aggregate_grouping() {
    let tree = partial_tree(rate_tree(), AggregateKind::Sum, &[JOB]);
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        // The scan is bound with `host`; the aggregate groups by `job`.
        binding_of(&tree, raw_schema(), vec![HOST.to_string()]),
        vec![input_plan(frame_schema())],
        canonical_state_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .expect("the scan's tags need not equal the cut's grouping labels");
    let labels = [(HOST, "web-0"), (JOB, "shared")];
    let rows = series_samples(1.0);
    let exec = node
        .to_execution_plan(vec![(
            memory_exec(vec![frame(&labelled_chunk("web-0", &labels, &rows, true))]),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .expect("the state export is executable");
    let states = decode_state_batches(state_batches(&exec, 0).await).await;
    let tags = states
        .first()
        .expect("the state export answers at least one series")
        .metric()
        .tags();
    assert_eq!(
        Some("shared"),
        tags.iter()
            .find(|tag| tag.name.as_ref() == JOB)
            .map(|tag| tag.value.as_ref()),
        "the exported state carries the grouping label the cut folds on"
    );
    assert!(
        tags.iter().all(|tag| tag.name.as_ref() != HOST),
        "the scan's tag is not the state's key: {tags:?}"
    );
}

/// A Partial root exports state-frame batches per partition, and two partitions may fold one group.
///
/// The two partitions hold series whose *prefix* labels differ (`host="web-0"` and `host="web-1"`)
/// while the aggregate groups by `job` alone, so both fold into the one group `job="shared"`. That
/// group repeats in the exported state, which is legal because the merge above the exchange collects
/// both partitions' states. The first partition's series also arrives in two chunks, so the fold has
/// to keep what it folded between them.
#[tokio::test]
async fn a_partial_root_exports_state_frames_from_every_partition() {
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
    assert_eq!(
        canonical_state_frame_schema(&partial_tree(rate_tree(), AggregateKind::Count, &[JOB]))
            .as_arrow(),
        exec.schema().as_ref()
    );

    for partition in 0..2 {
        let batches = state_batches(&exec, partition).await;
        let declaration = SeriesFrameDeclaration::parse(batches[0].schema().as_ref()).unwrap();
        assert_eq!(FrameContent::AggregateState, declaration.content());
        let schema = Arc::new(SeriesSchema {
            fields: raw_schema().fields.clone(),
            eval_grid: Some(grid()),
        });
        let binding = Arc::new(
            datatypes::series::aggregate_state::AggregateStateSchema::try_new(
                AggregateStateKind::Count,
                vec![StateComponent::Count],
            )
            .unwrap(),
        );
        let decoded = unpack_aggregate_state(
            Box::pin(RecordBatchStreamAdapter::new(
                batches[0].schema(),
                futures::stream::iter(batches.into_iter().map(Ok)),
            )),
            Some(binding.clone()),
            Some(schema),
        )
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert!(!decoded.is_empty());
        let batch = &decoded[0];
        assert_eq!(
            Some("shared"),
            batch
                .metric()
                .tags()
                .iter()
                .find(|tag| tag.name.as_ref() == JOB)
                .map(|tag| tag.value.as_ref())
        );
        assert_eq!(GRID_LEN as usize, batch.time().len());
        let fields = batch.fields().unwrap();
        assert_eq!(1, fields.len());
        assert!((0..GRID_LEN as usize).all(|point| fields.is_present(0, point).unwrap()));
        assert_eq!(AggregateStateKind::Count, binding.kind);
        assert_eq!(vec![StateComponent::Count], binding.components.as_ref());
    }
}

/// A state export releases everything it held: when drained, and when the consumer drops it early.
///
/// This is lifecycle coverage, not the regression for the producer hand-off: other reservations are
/// alive at the same moment, so a positive figure here would not by itself prove the batch is the
/// charged one. `a_handed_state_chunk_holds_its_charge_until_it_is_dropped` owns that claim.
#[tokio::test]
async fn a_state_export_releases_its_charge_on_drain_and_on_early_drop() {
    const GROUPS: usize = 600;
    let rows = series_samples(1.0);
    let chunks = (0..GROUPS)
        .map(|index| {
            let host = format!("h{index}");
            let labels = [(HOST, host.as_str())];
            labelled_chunk(&host, &labels, &rows, true)
        })
        .collect::<Vec<_>>();
    let exec = partial_plan(
        rate_tree(),
        memory_exec(frames(&chunks)),
        partial_op(AggregateKind::Count, &[HOST]),
        &[HOST],
    )
    .expect("a rate tree with a partial count is executable");

    // Draining: the last batch and the execution behind it release everything.
    let pool = Arc::new(GreedyMemoryPool::new(64 << 20));
    let mut stream = exec.execute(0, task_context(&pool)).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert!(
        first.num_rows() > 0,
        "the exporter answers a batch once the builder reaches its row limit"
    );
    stream.try_for_each(|_| async { Ok(()) }).await.unwrap();
    assert_eq!(0, pool.reserved(), "a drained export holds nothing");

    // Dropping mid-export: the batches the packer never read go with the stream.
    let pool = Arc::new(GreedyMemoryPool::new(64 << 20));
    let mut stream = exec.execute(0, task_context(&pool)).unwrap();
    stream.next().await.unwrap().unwrap();
    drop(stream);
    assert_eq!(0, pool.reserved(), "a dropped export holds nothing");
}

/// Prefix series of one label set on disjoint grid points are legal in state mode.
///
/// The exported states can be merged above this partition-local fold.
///
/// A window is left-open, `(T - 5m, T]`, so a fragment answers at a grid point only when two of its
/// own samples fall inside that window. The two fragments are spaced for exactly that: the early one
/// answers at 300s alone, the late one at 540s alone, and neither answers where the other does.
#[tokio::test]
async fn disjoint_partial_state_points_across_partitions() {
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

    for partition in 0..2 {
        let batches = state_batches(&exec, partition).await;
        assert!(!batches.is_empty());
        let batch = &batches[0];
        assert!(batch.num_rows() > 0);
    }
}

/// A partial node keeps its mode when an optimizer rebuilds it, and another aggregate is another
/// node.
#[test]
fn a_partial_tree_survives_rebuilding_and_compares_by_tree() {
    let node = partial_node(AggregateKind::Sum);
    assert!(
        matches!(node.tree().root().output, NativeType::AggregateState(_)),
        "tree exports state"
    );
    let rebuilt = node
        .with_exprs_and_inputs(Vec::new(), vec![input_plan(frame_schema())])
        .expect("the rebuilt node is legal");
    assert!(
        matches!(rebuilt.tree().root().output, NativeType::AggregateState(_)),
        "rebuilding the node dropped the aggregate it exports"
    );
    assert_eq!(node, rebuilt);

    let counted = partial_node(AggregateKind::Count);
    assert_ne!(node, counted, "another aggregate is another computation");
    assert_ne!(Some(std::cmp::Ordering::Equal), node.partial_cmp(&counted));
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

    // The state is a frame whose fields are `List<Struct<component...>>`, so its values are read
    // through the decoder rather than off a flat column.
    let batches = state_batches(&exec, 0).await;
    let decoded = decode_state_batches(batches).await;
    let values: Vec<Option<f64>> = decoded
        .iter()
        .flat_map(|batch| {
            let fields = batch.fields().unwrap();
            (0..batch.time().len())
                .map(move |position| {
                    fields.is_present(0, position).unwrap().then(|| {
                        fields
                            .field_values(0, StateComponent::Value)
                            .unwrap()
                            .expect("the count state carries a value component")
                            .value(position)
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(GRID_LEN as usize, values.len());
    assert_eq!(
        vec![Some(9.0); GRID_LEN as usize],
        values,
        "every grid point holds `10 - 1`, the larger of the two series' values"
    );
}

/// A fused tree can decode its raw source and already-evaluated source independently.
#[tokio::test]
async fn mixed_raw_and_evaluated_inputs_execute_with_source_local_decoders() {
    let tree = mixed_raw_evaluated_tree();
    let mut evaluated_batches = Vec::new();
    for (host, values) in [
        ("web-1", vec![10.0, 20.0, 30.0, 40.0, 50.0]),
        ("web-2", vec![60.0, 70.0, 80.0, 90.0, 100.0]),
    ] {
        let schema = Arc::new(SeriesSchema {
            fields: raw_schema().fields.clone(),
            eval_grid: Some(grid()),
        });
        let chunk = EvaluatedChunk::try_new(
            Series {
                metric: metric(&[(HOST, host)], Some(METRIC)),
                time: TimeAxis::from_grid_spans(
                    Arc::clone(schema.eval_grid.as_ref().unwrap()),
                    vec![GridSpan::new(0, GRID_LEN as usize)],
                )
                .unwrap(),
                fields: SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(
                    values,
                ))])
                .unwrap(),
            },
            true,
        )
        .unwrap();
        evaluated_batches.extend(
            pack_evaluated_series(
                Box::pin(futures::stream::iter(vec![Ok((
                    chunk.series,
                    chunk.is_last,
                ))])),
                schema,
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
        );
    }
    let raw_batches = frames(&[
        series_chunk("web-1", &series_samples(1.0), true),
        series_chunk("web-2", &series_samples(1.0), true),
    ]);
    let evaluated_frame_schema = series_frame_schema(
        &SeriesSchema {
            fields: raw_schema().fields.clone(),
            eval_grid: Some(grid()),
        },
        FrameLabelMode::Explicit,
        None,
    )
    .unwrap();
    let exec = FusedSeries::try_new(
        Arc::from("mixed raw/evaluated"),
        tree.clone(),
        binding_of(
            &tree,
            raw_schema(),
            vec![HOST.to_string(), NAME.to_string()],
        ),
        vec![
            input_plan(frame_schema()),
            input_plan(evaluated_frame_schema.clone()),
        ],
        output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap()
    .to_execution_plan(vec![
        (
            memory_exec(raw_batches),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        ),
        (
            memory_exec_with(evaluated_frame_schema, evaluated_batches),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        ),
    ])
    .unwrap();
    let mut actual = rows(
        &collect(exec, SessionContext::default().task_ctx())
            .await
            .unwrap(),
    );
    let mut expected = vec![
        (GRID_START, Some(11.0), Some("web-1".into()), None),
        (
            GRID_START + GRID_STEP,
            Some(21.0),
            Some("web-1".into()),
            None,
        ),
        (
            GRID_START + 2 * GRID_STEP,
            Some(31.0),
            Some("web-1".into()),
            None,
        ),
        (
            GRID_START + 3 * GRID_STEP,
            Some(41.0),
            Some("web-1".into()),
            None,
        ),
        (
            GRID_START + 4 * GRID_STEP,
            Some(51.0),
            Some("web-1".into()),
            None,
        ),
        (GRID_START, Some(61.0), Some("web-2".into()), None),
        (
            GRID_START + GRID_STEP,
            Some(71.0),
            Some("web-2".into()),
            None,
        ),
        (
            GRID_START + 2 * GRID_STEP,
            Some(81.0),
            Some("web-2".into()),
            None,
        ),
        (
            GRID_START + 3 * GRID_STEP,
            Some(91.0),
            Some("web-2".into()),
            None,
        ),
        (
            GRID_START + 4 * GRID_STEP,
            Some(101.0),
            Some("web-2".into()),
            None,
        ),
    ];
    actual.sort_by(|left, right| {
        (left.0, left.2.as_deref(), left.3.as_deref()).cmp(&(
            right.0,
            right.2.as_deref(),
            right.3.as_deref(),
        ))
    });
    expected.sort_by(|left, right| {
        (left.0, left.2.as_deref(), left.3.as_deref()).cmp(&(
            right.0,
            right.2.as_deref(),
            right.3.as_deref(),
        ))
    });
    assert_eq!(10, actual.len());
    assert_eq!(expected, actual);
}

#[test]
fn final_aggregate_state_input_constructs_with_evaluated_schema() {
    let state = Arc::new(AggregateStateSchema {
        group: Grouping::By(Arc::new([Arc::from(JOB)])),
        kind: AggregateStateKind::Sum,
        components: Arc::from([StateComponent::Value, StateComponent::Compensation]),
        accumulators: raw_schema().fields.clone(),
        grid: grid(),
    });
    let tree = state_tree(AggregateStage::Final, NativeType::AggregateState(state));
    let schema = canonical_frame_schema(&tree);
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        schema.clone(),
        FusedOutputFormat::SeriesFrame,
    )
    .expect("Final accepts an aggregate-state input binding");
    assert_eq!(schema.as_arrow(), node.schema().as_arrow());
}

#[test]
fn partial_aggregate_declares_state_frame_schema() {
    let tree = state_tree(
        AggregateStage::Partial,
        NativeType::Samples(Arc::new(SeriesSchema {
            fields: raw_schema().fields.clone(),
            eval_grid: Some(grid()),
        })),
    );
    let state_schema = canonical_state_frame_schema(&tree);
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        state_schema,
        FusedOutputFormat::SeriesFrame,
    )
    .expect("Partial declares its aggregate-state frame layout");
    assert_eq!(
        canonical_state_frame_schema(&tree).as_arrow(),
        node.schema().as_arrow()
    );

    let error = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        output_schema().to_dfschema_ref().unwrap(),
        FusedOutputFormat::SeriesFrame,
    )
    .expect_err("a wrong declaration is refused");
    let message = error.to_string();
    assert!(message.contains("declared"), "{message}");
    assert!(message.contains("expected"), "{message}");
}

#[test]
fn final_root_refuses_samples_binding() {
    let result = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: NativeType::Samples(Arc::new(SeriesSchema {
                        fields: raw_schema().fields.clone(),
                        eval_grid: Some(grid()),
                    })),
                    source_index: 0,
                    ordering: SeriesOrdering::Contiguous,
                }),
                output: NativeType::Samples(Arc::new(SeriesSchema {
                    fields: raw_schema().fields.clone(),
                    eval_grid: Some(grid()),
                })),
            }),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new([Arc::from(JOB)])),
                stage: AggregateStage::Final,
                field_kind: FieldKind::Float,
            },
        },
        output: NativeType::Samples(Arc::new(SeriesSchema {
            fields: raw_schema().fields.clone(),
            eval_grid: Some(grid()),
        })),
    });
    assert!(
        result.is_err(),
        "Final requires state, not evaluated samples"
    );
}

#[test]
fn repeated_source_with_state_and_samples_is_refused() {
    let state = Arc::new(AggregateStateSchema {
        group: Grouping::By(Arc::new([Arc::from(JOB)])),
        kind: AggregateStateKind::Sum,
        components: Arc::from([StateComponent::Value, StateComponent::Compensation]),
        accumulators: raw_schema().fields.clone(),
        grid: grid(),
    });
    let state_input = BoundInput {
        output: NativeType::AggregateState(state),
        source_index: 0,
        ordering: SeriesOrdering::Contiguous,
    };
    let samples_input = BoundInput {
        output: NativeType::Samples(Arc::new(SeriesSchema {
            fields: raw_schema().fields.clone(),
            eval_grid: Some(grid()),
        })),
        source_index: 0,
        ordering: SeriesOrdering::Contiguous,
    };
    let error =
        super::collect_source_schemas_from_inputs(vec![&state_input, &samples_input]).unwrap_err();
    assert!(error.to_string().contains("disagrees on input kind"));
}

/// The declared schema and its error mapping match the evaluated stream packer exactly.
#[test]
fn series_frame_output_schema_matches_evaluated_packer() {
    for fields in [
        vec![FieldSpec { name: VALUE.into() }],
        vec![
            FieldSpec { name: VALUE.into() },
            FieldSpec { name: OTHER.into() },
        ],
    ] {
        let schema = Arc::new(SeriesSchema {
            fields: fields.into(),
            eval_grid: Some(grid()),
        });
        let declared = super::series_frame_output_schema(&schema).unwrap();
        let packed = pack_evaluated_series(Box::pin(futures::stream::empty()), schema).unwrap();
        assert_eq!(declared, packed.schema());
        let declaration = SeriesFrameDeclaration::parse(declared.as_ref()).unwrap();
        assert_eq!(FrameContent::Evaluated, declaration.content());
        assert_eq!(FrameLabelMode::Explicit, declaration.labels());
        assert_eq!(
            SeriesOrdering::Contiguous,
            series_frame_ordering(declared.as_ref()).unwrap()
        );
    }

    let invalid = Arc::new(SeriesSchema {
        fields: Arc::from([]),
        eval_grid: Some(grid()),
    });
    let packed_error =
        pack_evaluated_series(Box::pin(futures::stream::empty()), Arc::clone(&invalid))
            .err()
            .expect("the packer rejects a frame without fields");
    let error = super::series_frame_output_schema(&invalid)
        .expect_err("the declaration rejects a frame without fields");
    assert!(matches!(
        error,
        DataFusionError::Plan(ref message)
            if *message == format!("FusedSeries cannot declare frame schema: {packed_error}")
    ));

    let output = NativeType::Samples(raw_schema());
    let raw_tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Contiguous,
        }),
        output,
    })
    .unwrap();
    let error = FusedSeries::series_frame_schema(&raw_tree).unwrap_err();
    assert!(matches!(
        error,
        DataFusionError::Plan(ref message)
            if message == "FusedSeries frame output requires evaluated samples with a grid"
    ));
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

/// Frame output accepts multiple sources when their partition counts agree.
#[test]
fn series_frame_accepts_multiple_sources_with_equal_partition_counts() {
    let tree = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1);
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let plan = node
        .to_execution_plan(vec![
            (
                memory_exec_partitioned(vec![vec![], vec![]]),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
            (
                memory_exec_partitioned(vec![vec![], vec![]]),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
        ])
        .expect("equal source partition counts are accepted");
    assert_eq!(2, plan.properties().partitioning.partition_count());
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

/// An empty Partial input ends cleanly while retaining the declared state-frame schema.
#[tokio::test]
async fn partial_empty_input_yields_valid_empty_state_frame_stream() {
    let tree = partial_tree(rate_tree(), AggregateKind::Sum, &[HOST]);
    let node = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        canonical_state_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let exec = plan_of(node, memory_exec(Vec::new())).unwrap();
    let batches = state_batches(&exec, 0).await;
    assert!(batches.is_empty());
    assert_eq!(
        canonical_state_frame_schema(&partial_tree(rate_tree(), AggregateKind::Sum, &[HOST]))
            .as_arrow(),
        exec.schema().as_ref()
    );
}

#[tokio::test]
async fn partial_root_requires_frame_output() {
    let prefix = rate_tree();
    let tree = partial_tree(prefix, AggregateKind::Sum, &[HOST]);
    let error = FusedSeries::try_new_with_format(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        canonical_state_frame_schema(&tree),
        FusedOutputFormat::PlainRows,
    )
    .expect_err("Partial rows output is refused");
    assert!(error.to_string().contains("Partial stage"));
}

/// `EXPLAIN` says nothing extra about the row table and names the frame output.
///
/// The row-table header has no parameters; a frame output names its format in the header.
#[test]
fn the_explain_text_names_the_frame_format_only() {
    let rows = fused(rate_tree());
    assert!(
        Explain(&rows)
            .to_string()
            .starts_with("FusedSeries\nnative_tree=[\nWindow:")
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
            .starts_with("FusedSeries: (output: series_frame)\nnative_tree=[\nWindow:"),
        "a frame output names its format: {}",
        Explain(&framed)
    );
    let framed_exec = plan_of(framed, memory_exec(Vec::new())).unwrap();
    let text = displayable(framed_exec.as_ref()).indent(false).to_string();
    assert!(
        text.starts_with("FusedSeriesExec: (output: series_frame)\nnative_tree=[\nWindow:"),
        "{text}"
    );
    let canonical = canonical_producer_node();
    let text = Explain(&canonical).to_string();
    assert!(
        text.starts_with(
            "FusedSeries: (output: series_frame, per-partition canonical merge)\nnative_tree=[\nMerge:"
        ),
        "{text}"
    );

    let plain = plan(rate_tree(), memory_exec(Vec::new())).expect("the runtime executes the tree");
    let text = displayable(plain.as_ref()).indent(false).to_string();
    assert!(
        text.starts_with("FusedSeriesExec\nnative_tree=[\nWindow:"),
        "the row-table node line has no parameters: {text}"
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

    // The partial half of a split is a frame, not a row table: the state it exports has no row
    // layout, so the node that runs it is built with the frame format.
    assert_eq!(
        FusedOutputFormat::SeriesFrame,
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

/// The one runtime input takes the empty closing chunk by value and its frame keeps it.
///
/// A single-consumer push moves the chunk into the executor rather than cloning it, so this is the
/// chunk a move could lose: it carries no grid position at all, and the tree binds exactly one
/// runtime input to take it.
#[tokio::test]
async fn a_single_consumer_keeps_the_empty_closing_chunk_in_its_frame() {
    let schema = raw_schema();
    // Every sample is newer than the last grid point, so `rate` decides nothing at any of them and
    // the series closes with a chunk that carries no position.
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
    // The shape the push moves for: one source bound to one runtime input.
    let execution =
        NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default()), None)
            .expect("a rate tree has a runtime");
    assert_eq!(
        1,
        execution.input_ids(0).count(),
        "one runtime input takes the closing chunk"
    );
    drop(execution);

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

#[test]
fn duplicate_evidence_claims_share_evidence_and_never_reissue_partitions() {
    use std::sync::{Arc, Mutex};

    use super::{EvidenceClaims, claim_evidence};
    use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let claims = Mutex::new(EvidenceClaims {
        evidence: None,
        claimed: Vec::new(),
    });
    let first = claim_evidence(&claims, 0, 2, &pool).unwrap();
    let second = claim_evidence(&claims, 1, 2, &pool).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    first.complete();
    second.complete();

    for partition in [0, 1] {
        let error = match claim_evidence(&claims, partition, 2, &pool) {
            Ok(_) => panic!("reissued partition was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("executed more than once"));
    }

    first.abort();
    let error = match claim_evidence(&claims, 0, 2, &pool) {
        Ok(_) => panic!("aborted execution partition was reclaimed"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("executed more than once"));
}

#[tokio::test]
async fn duplicate_evidence_rejects_concurrent_reissue_of_one_partition() {
    let exec =
        duplicate_evidence_partitioned_exec(vec![vec![], vec![]], vec![vec![], vec![]], 0, 0);
    let context = SessionContext::default().task_ctx();
    let stream = exec.execute(0, Arc::clone(&context)).unwrap();
    let error = exec
        .execute(0, context)
        .err()
        .expect("duplicate claim is rejected");
    assert!(
        error.to_string().contains("executed more than once"),
        "{error}"
    );
    drop(stream);
}

fn duplicate_evidence_partitioned_exec(
    left_partitions: Vec<Vec<RecordBatch>>,
    right_partitions: Vec<Vec<RecordBatch>>,
    left_lookback_ms: i64,
    right_lookback_ms: i64,
) -> Arc<dyn ExecutionPlan> {
    let mut root = binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1)
        .root()
        .clone();
    if let NativeExpr::Binary {
        left,
        right,
        matching,
    } = &mut root.expr
    {
        matching.labels = MatchingLabels::On(vec![Arc::from(HOST)]);
        matching.cardinality = Cardinality::OneToOne;
        if let NativeExpr::Instant { op, .. } = &mut left.expr {
            op.lookback_ms = left_lookback_ms;
        }
        if let NativeExpr::Instant { op, .. } = &mut right.expr {
            op.lookback_ms = right_lookback_ms;
        }
    }
    let tree =
        NativeTree::try_new(root).expect("differently bound instant selectors form a valid tree");
    let node = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema()), input_plan(frame_schema())],
        binary_output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the tree binds to its two physical sources");
    let exec = node
        .to_execution_plan(vec![
            (
                memory_exec_partitioned(left_partitions),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
            (
                memory_exec_partitioned(right_partitions),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            ),
        ])
        .expect("two-source binary runs over partitioned inputs");
    assert_eq!(2, exec.properties().partitioning.partition_count());
    exec
}

fn match_series(key: &str, job: &str, time: i64, value: f64) -> RecordBatch {
    frame(&chunk_of(
        &raw_schema(),
        &format!("{key}-{job}"),
        metric(&[(HOST, key), (JOB, job)], Some(METRIC)),
        &[time],
        vec![vec![Some(value)]],
        true,
    ))
}

async fn partitioned_duplicate_verdict(exec: Arc<dyn ExecutionPlan>) -> Vec<String> {
    let context = SessionContext::default().task_ctx();
    let streams = (0..exec.properties().partitioning.partition_count())
        .map(|partition| {
            exec.execute(partition, Arc::clone(&context))
                .expect("each fused output partition executes")
        })
        .collect::<Vec<_>>();
    let mut errors = Vec::new();
    for stream in streams {
        if let Err(error) = stream.try_collect::<Vec<_>>().await {
            errors.push(error.to_string());
        }
    }
    errors
}

#[tokio::test]
async fn duplicate_evidence_merges_presence_across_partitions() {
    let exec = duplicate_evidence_partitioned_exec(
        vec![vec![], vec![match_series("k", "left", GRID_START, 3.0)]],
        vec![
            vec![
                match_series("k", "right-a", GRID_START - 1, 1.0),
                match_series("k", "right-b", GRID_START - 1, 2.0),
            ],
            vec![],
        ],
        0,
        GRID_STEP,
    );
    let errors = partitioned_duplicate_verdict(exec).await;
    assert_eq!(
        1,
        errors.len(),
        "the merged verdict is emitted once: {errors:?}"
    );
    assert!(
        errors[0].contains("found duplicate series for the match group"),
        "{}",
        errors[0]
    );
    assert!(errors[0].contains("{host=\"k\"}"), "{}", errors[0]);

    let only_duplicate_partition = duplicate_evidence_partitioned_exec(
        vec![vec![], vec![]],
        vec![
            vec![
                match_series("k", "right-a", GRID_START - 1, 1.0),
                match_series("k", "right-b", GRID_START - 1, 2.0),
            ],
            vec![],
        ],
        0,
        GRID_STEP,
    );
    assert!(
        partitioned_duplicate_verdict(only_duplicate_partition)
            .await
            .is_empty(),
        "the duplicate partition has no left presence, so it cannot self-report"
    );
}

#[tokio::test]
async fn duplicate_evidence_allows_presence_at_disjoint_steps_across_partitions() {
    let left = vec![
        vec![],
        vec![
            match_series("k", "left-a", GRID_START, 3.0),
            match_series("k", "left-b", GRID_START + GRID_STEP, 4.0),
        ],
    ];
    let right = vec![
        vec![
            match_series("k", "right-a", GRID_START - 1, 1.0),
            match_series("k", "right-b", GRID_START + GRID_STEP - 1, 2.0),
        ],
        vec![],
    ];
    let exec = duplicate_evidence_partitioned_exec(left.clone(), right.clone(), 0, GRID_STEP);
    let errors = partitioned_duplicate_verdict(Arc::clone(&exec)).await;
    assert!(errors.is_empty(), "{errors:?}");
    let rows_exec = duplicate_evidence_partitioned_exec(left, right, 0, GRID_STEP);
    let rows = collect(rows_exec, SessionContext::default().task_ctx())
        .await
        .unwrap();
    assert_eq!(
        2,
        binary_values(&rows).len(),
        "both disjoint steps produce rows"
    );
}

#[tokio::test]
async fn duplicate_evidence_allows_same_key_at_disjoint_steps_across_partitions() {
    let exec = duplicate_evidence_partitioned_exec(
        vec![
            vec![match_series("k", "left", GRID_START + GRID_STEP, 3.0)],
            vec![],
        ],
        vec![vec![], vec![match_series("k", "right", GRID_START, 1.0)]],
        0,
        0,
    );
    let errors = partitioned_duplicate_verdict(exec).await;
    assert!(errors.is_empty(), "{errors:?}");
}

/// The bound shape of `histogram_quantile(0.99, sum by (...) (rate(x[5m])))`.
fn histogram_over_sum_tree(group: Grouping, function: HistogramFunction) -> NativeTree {
    let rate = rate_tree();
    let output = rate.root().output.clone();
    let aggregate = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(rate.root().clone()),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group,
                stage: AggregateStage::Complete,
                field_kind: FieldKind::Float,
            },
        },
        output: output.clone(),
    };
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(aggregate),
            op: BoundVectorOp::Histogram(function),
        },
        output,
    })
    .unwrap()
}

/// The same histogram over the same fold, with its parameters evaluated at every grid point instead
/// of bound as constants: `histogram_quantile(<parameter>, sum by (...) (rate(x[5m])))`.
fn histogram_parameterized_over_sum(
    histogram: NativeTree,
    parameters: Vec<NativeNode>,
) -> NativeTree {
    let NativeExpr::Vector { input, op } = &histogram.root().expr else {
        unreachable!("`histogram_over_sum_tree` builds a literal histogram")
    };
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::ParameterizedHistogram {
            input: input.clone(),
            op: op.clone(),
            parameters,
        },
        output: histogram.root().output.clone(),
    })
    .expect("a parameterized histogram over the same fold is a legal tree")
}

fn histogram_routing() -> SeriesRouting {
    SeriesRouting::CanonicalLabelsExcept(
        ["le", "__name__", "__type__", "__unit__"]
            .into_iter()
            .map(Arc::from)
            .collect::<Vec<_>>()
            .into(),
    )
}

#[test]
fn histogram_over_sum_splits_and_declares_complete_bucket_routing() {
    for function in [
        HistogramFunction::Quantile(0.99),
        HistogramFunction::Fraction {
            lower: 0.0,
            upper: 1.0,
        },
    ] {
        for group in [
            Grouping::By(Arc::from([Arc::from("le")])),
            Grouping::By(Arc::from([Arc::from("le"), Arc::from(JOB)])),
            Grouping::Without(Arc::from([Arc::from(HOST)])),
        ] {
            let tree = histogram_over_sum_tree(group.clone(), function);
            let (partial, final_tree) = FusedSeries::split_aggregate(&tree, TIME)
                .unwrap()
                .expect("histogram over sum must split");
            assert!(matches!(
                &partial.root().expr,
                NativeExpr::Aggregate { op, .. }
                    if op.stage == AggregateStage::Partial && op.group == group
            ));
            assert!(matches!(
                partial.root().output,
                NativeType::AggregateState(_)
            ));
            let NativeExpr::Vector {
                input,
                op: BoundVectorOp::Histogram(actual),
            } = &final_tree.root().expr
            else {
                panic!("the histogram must be above the Final aggregate");
            };
            assert_eq!(*actual, function);
            assert!(matches!(
                &input.expr,
                NativeExpr::Aggregate { op, .. }
                    if op.stage == AggregateStage::Final && op.group == group
            ));
            let node = FusedSeries::try_new_with_format(
                Arc::from("histogram over sum"),
                final_tree.clone(),
                binding(&final_tree),
                vec![input_plan(
                    canonical_state_frame_schema(&partial).inner().clone(),
                )],
                canonical_frame_schema(&final_tree),
                FusedOutputFormat::SeriesFrame,
            )
            .unwrap();
            let logical = LogicalPlan::Extension(datafusion::logical_expr::Extension {
                node: Arc::new(node),
            });
            let display = logical.display_indent().to_string();
            assert!(
                display.lines().next().unwrap().starts_with(
                    "FusedSeries: (output: series_frame) (state routing: canonical labels except [\"le\", \"__name__\", \"__type__\", \"__unit__\"])"
                ),
                "{display}"
            );
        }
    }
}

#[test]
fn histogram_split_refuses_only_rewrites_that_change_the_routing_key() {
    let group = Grouping::By(Arc::from([Arc::from("le"), Arc::from(JOB)]));
    let base = histogram_over_sum_tree(group, HistogramFunction::Quantile(0.99));
    for (dst, accepted) in [
        ("le", true),
        ("__type__", true),
        ("zone", true),
        (JOB, false),
    ] {
        let NativeExpr::Vector { input, op } = &base.root().expr else {
            unreachable!()
        };
        let mapped = NativeNode {
            expr: NativeExpr::Map {
                input: input.clone(),
                op: BoundMap {
                    ops: vec![MapOp::Label(Arc::new(
                        LabelRewrite::join(dst, "", &[HOST]).unwrap(),
                    ))]
                    .into(),
                },
            },
            output: input.output.clone(),
        };
        let tree = NativeTree::try_new(NativeNode {
            expr: NativeExpr::Vector {
                input: Box::new(mapped),
                op: op.clone(),
            },
            output: base.root().output.clone(),
        })
        .unwrap();
        assert_eq!(
            FusedSeries::split_aggregate(&tree, TIME).unwrap().is_some(),
            accepted,
            "{dst}"
        );
    }
}

#[test]
fn histogram_split_allows_single_owner_routing_label_rewrites() {
    for (group, accepted) in [
        (Grouping::By(Arc::from([Arc::from("le")])), true),
        (
            Grouping::By(Arc::from([Arc::from("le"), Arc::from("__type__")])),
            true,
        ),
        (
            Grouping::By(Arc::from([Arc::from("le"), Arc::from(JOB)])),
            false,
        ),
        (Grouping::Without(Arc::from([Arc::from(HOST)])), false),
    ] {
        let base = histogram_over_sum_tree(group.clone(), HistogramFunction::Quantile(0.99));
        let NativeExpr::Vector { input, op } = &base.root().expr else {
            unreachable!()
        };
        for before_histogram in [true, false] {
            // Rewriting the routing label from `le` can assign different keys to sibling buckets.
            let map = |input: NativeNode| NativeNode {
                output: input.output.clone(),
                expr: NativeExpr::Map {
                    input: Box::new(input),
                    op: BoundMap {
                        ops: vec![MapOp::Label(Arc::new(
                            LabelRewrite::join(JOB, "", &["le"]).unwrap(),
                        ))]
                        .into(),
                    },
                },
            };
            let root = if before_histogram {
                NativeNode {
                    expr: NativeExpr::Vector {
                        input: Box::new(map((**input).clone())),
                        op: op.clone(),
                    },
                    output: base.root().output.clone(),
                }
            } else {
                map(base.root().clone())
            };
            let tree = NativeTree::try_new(root).unwrap();
            assert_eq!(
                FusedSeries::split_aggregate(&tree, TIME).unwrap().is_some(),
                accepted,
                "group {group:?}, before histogram: {before_histogram}"
            );
        }
    }
}

#[test]
fn histogram_split_carries_a_per_point_parameter() {
    let grid = grid();
    let base = histogram_over_sum_tree(
        Grouping::By(Arc::from([Arc::from("le")])),
        HistogramFunction::Quantile(0.99),
    );
    // `histogram_quantile(time() / 150, sum by (le) (rate(x[5m])))`, with the parameter bound the
    // way the compiler binds a per-point argument: the evaluation time broadcast over a constant.
    let eval_time = NativeNode {
        expr: NativeExpr::EvalTime { date: None },
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let divisor = NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value: 150.0 }),
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let parameter = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(eval_time),
            right: Box::new(divisor),
            matching: BinaryMatching {
                op: BinaryOp::Div,
                return_bool: false,
                labels: MatchingLabels::Ignoring(Vec::new()),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let tree = histogram_parameterized_over_sum(base.clone(), vec![parameter.clone()]);
    let (partial, final_tree) = FusedSeries::split_aggregate(&tree, TIME)
        .unwrap_or_else(|error| panic!("a per-point parameter must split, not fail: {error}"))
        .unwrap_or_else(|| panic!("a per-point constant parameter must split: {tree}"));
    assert!(matches!(
        &partial.root().expr,
        NativeExpr::Aggregate { op, .. } if op.stage == AggregateStage::Partial
    ));
    let NativeExpr::ParameterizedHistogram {
        input,
        op,
        parameters,
    } = &final_tree.root().expr
    else {
        panic!("the parameterized histogram must run above the Final aggregate: {final_tree}");
    };
    assert_eq!(
        &BoundVectorOp::Histogram(HistogramFunction::Quantile(0.99)),
        op
    );
    assert!(matches!(
        &input.expr,
        NativeExpr::Aggregate { op, .. } if op.stage == AggregateStage::Final
    ));
    // The parameter is carried untouched: it reads no series, so it means the same above the cut.
    assert_eq!(vec![parameter], *parameters);

    // A parameter that reads the series the split reads has no source above the cut, so it must
    // refuse rather than be answered from the merged values: `histogram_quantile(scalar(sum(
    // rate(x[5m]))), sum by (le) (rate(x[5m])))`, whose parameter holds the window the prefix reads.
    let NativeExpr::Vector {
        input: aggregate, ..
    } = &base.root().expr
    else {
        unreachable!("`histogram_over_sum_tree` builds a literal histogram")
    };
    let NativeExpr::Aggregate { input: window, .. } = &aggregate.expr else {
        unreachable!("`histogram_over_sum_tree` folds the rate window")
    };
    let scalar_of_window = NativeNode {
        expr: NativeExpr::ScalarOf {
            input: window.clone(),
        },
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let reading = histogram_parameterized_over_sum(base, vec![scalar_of_window]);
    assert!(
        FusedSeries::split_aggregate(&reading, TIME)
            .unwrap()
            .is_none(),
        "`histogram_quantile(scalar(sum(rate(x[5m]))), sum by (le) (rate(x[5m])))` must refuse: {reading}"
    );
}

/// A `quantile` exports no state, with a per-point `φ` or without: the split refuses both.
///
/// Its answer is the quantile of a group's whole sample set, and the quantile of two partitions'
/// quantiles is not the quantile of their union. Reading the `φ` per point changes where the number
/// comes from and nothing about that, so the fold that binds a number and the one that reads the
/// statement's are refused by the same rule.
#[test]
fn a_per_point_quantile_is_not_split() {
    let window = rate_tree().root().clone();
    let grid = window
        .output
        .samples()
        .expect("a window produces samples")
        .eval_grid
        .clone()
        .expect("a window's output is evaluated on a grid");
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: raw_schema().fields.clone(),
        eval_grid: Some(grid.clone()),
    }));
    let quantile = |parameter: NativeNode| NativeNode {
        expr: NativeExpr::ParameterizedAggregate {
            input: Box::new(window.clone()),
            // Unused: the `φ` this fold reads is its parameter's.
            op: BoundAggregate {
                kind: AggregateKind::Quantile { quantile: 0.0 },
                group: Grouping::By(Arc::from([Arc::from(JOB)])),
                stage: AggregateStage::Complete,
                field_kind: FieldKind::Float,
            },
            parameter: Box::new(parameter),
        },
        output: output.clone(),
    };
    // `time()`, and a scalar the statement reads from the series the split would read.
    let eval_time = NativeNode {
        expr: NativeExpr::EvalTime { date: None },
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    let scalar_of_window = NativeNode {
        expr: NativeExpr::ScalarOf {
            input: Box::new(window.clone()),
        },
        output: NativeType::Scalar(Arc::clone(&grid)),
    };
    for parameter in [eval_time, scalar_of_window] {
        let tree = NativeTree::try_new(quantile(parameter)).expect("the bound tree is legal");
        assert!(
            FusedSeries::split_aggregate(&tree, TIME).unwrap().is_none(),
            "a per-point quantile must keep the plan it had: {tree}"
        );
    }
}

/// A fold above a state input reads every owner of the `Final` it folds.
///
/// The rows of a grouped `Final` state are split by their group's labels, which is how a split
/// aggregate folds in parallel: each owner folds its own groups. A fold above that `Final` reads
/// whole vectors, though, so the same split answers something else there: an ungrouped `quantile`
/// is the quantile of every group's samples at once, and one fold per owner is a set of per-group
/// quantiles instead. The state is therefore gathered when the fold above it reads across groups,
/// wherever that fold sits: `quantile(time(), v)` reads the state as its input, and
/// `quantile(scalar(v), v)` reads it through the parameter, where the routing walk finds it too.
#[test]
fn a_fold_across_groups_gathers_the_state_it_reads() {
    let out = grid();
    let samples = NativeType::Samples(Arc::new(SeriesSchema {
        fields: raw_schema().fields.clone(),
        eval_grid: Some(Arc::clone(&out)),
    }));
    let state = Arc::new(AggregateStateSchema {
        group: Grouping::By(Arc::new([Arc::from(JOB)])),
        kind: AggregateStateKind::Sum,
        components: Arc::from([StateComponent::Value, StateComponent::Compensation]),
        accumulators: raw_schema().fields.clone(),
        grid: Arc::clone(&out),
    });
    let bound_input = |output: NativeType| NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Contiguous,
        }),
        output,
    };
    // `sum by (job) (…)` at the end of its fold: one row per group and point.
    let final_over_state = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(bound_input(NativeType::AggregateState(Arc::clone(&state)))),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new([Arc::from(JOB)])),
                stage: AggregateStage::Final,
                field_kind: FieldKind::Float,
            },
        },
        output: samples.clone(),
    };
    let eval_time = NativeNode {
        expr: NativeExpr::EvalTime { date: None },
        output: NativeType::Scalar(Arc::clone(&out)),
    };
    let quantile = |input: NativeNode, parameter: NativeNode| NativeNode {
        expr: NativeExpr::ParameterizedAggregate {
            input: Box::new(input),
            op: BoundAggregate {
                kind: AggregateKind::Quantile { quantile: 0.5 },
                group: Grouping::By(Arc::from([])),
                stage: AggregateStage::Complete,
                field_kind: FieldKind::Float,
            },
            parameter: Box::new(parameter),
        },
        output: samples.clone(),
    };
    // `quantile(time(), sum by (job) (…))`: ungrouped, so the fold folds every group together.
    let across = quantile(final_over_state.clone(), eval_time);
    assert_eq!(
        Some((super::StateGroup::Single, SeriesRouting::CanonicalLabels)),
        super::state_routing(&across),
        "a fold across the Final's groups must read them on one owner"
    );
    // `quantile(scalar(sum by (job) (…)), v)`: the state is read by the parameter, which is one
    // value of the whole vector, so it is as much a fold across groups as the one above.
    let through_parameter = quantile(
        bound_input(samples.clone()),
        NativeNode {
            expr: NativeExpr::ScalarOf {
                input: Box::new(final_over_state),
            },
            output: NativeType::Scalar(Arc::clone(&out)),
        },
    );
    assert_eq!(
        Some((super::StateGroup::Single, SeriesRouting::CanonicalLabels)),
        super::state_routing(&through_parameter),
        "the Final under the fold's parameter is the state this tree reads"
    );
}

#[test]
fn histogram_final_replaces_incompatible_state_router() {
    for (group, old_width, target_width, required_width, old_policy, reuse) in [
        (
            Grouping::By(Arc::from([Arc::from("le"), Arc::from(JOB)])),
            4,
            4,
            4,
            SeriesRouting::CanonicalLabels,
            false,
        ),
        (
            Grouping::By(Arc::from([Arc::from("le"), Arc::from(JOB)])),
            4,
            8,
            8,
            SeriesRouting::CanonicalLabels,
            false,
        ),
        (
            Grouping::By(Arc::from([Arc::from("le"), Arc::from(JOB)])),
            4,
            8,
            8,
            histogram_routing(),
            false,
        ),
        (
            Grouping::By(Arc::from([Arc::from("le")])),
            1,
            8,
            1,
            SeriesRouting::CanonicalLabels,
            true,
        ),
        (
            Grouping::By(Arc::from([Arc::from("le")])),
            4,
            8,
            1,
            SeriesRouting::CanonicalLabels,
            false,
        ),
    ] {
        let tree = histogram_over_sum_tree(group, HistogramFunction::Quantile(0.5));
        let (partial, final_tree) = FusedSeries::split_aggregate(&tree, TIME).unwrap().unwrap();
        let partial_node = FusedSeries::try_new_state_export(
            Arc::from("partial"),
            partial.clone(),
            binding(&partial),
            vec![input_plan(frame_schema())],
        )
        .unwrap();
        let producer = partial_node
            .to_execution_plan(vec![(
                memory_exec_partitioned(vec![vec![], vec![]]),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            )])
            .unwrap();
        let old_router = Arc::new(
            SeriesRepartitionExec::try_new_with_routing(
                Arc::clone(&producer),
                old_width,
                old_policy.clone(),
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let final_node = FusedSeries::try_new_with_format(
            Arc::from("final"),
            final_tree.clone(),
            binding(&final_tree),
            vec![input_plan(
                canonical_state_frame_schema(&partial).inner().clone(),
            )],
            canonical_frame_schema(&final_tree),
            FusedOutputFormat::SeriesFrame,
        )
        .unwrap();
        let build = |input| {
            final_node
                .to_execution_plan_with_parallelism(
                    vec![(input, RawLabelSource::Explicit, SeriesOrdering::Contiguous)],
                    target_width,
                )
                .unwrap()
        };
        let plan = build(Arc::clone(&old_router));
        let child = plan.children()[0];
        let router = child.downcast_ref::<SeriesRepartitionExec>().unwrap();
        assert_eq!(router.outputs(), required_width);
        if reuse {
            assert_eq!(router.routing_policy(), &old_policy);
            assert!(
                Arc::ptr_eq(child, &old_router),
                "a single-owner router is reused regardless of policy"
            );
        } else {
            assert_eq!(router.routing_policy(), &histogram_routing());
            assert!(!Arc::ptr_eq(child, &old_router), "old router is replaced");
        }
        assert!(
            Arc::ptr_eq(router.children()[0], &producer),
            "the only router reads the original state producer directly"
        );
        let correct_router = Arc::clone(child);
        let reused = build(Arc::clone(&correct_router));
        assert!(
            Arc::ptr_eq(reused.children()[0], &correct_router),
            "a router with the required width and policy is reused"
        );
    }
}

#[tokio::test]
async fn histogram_split_routes_whole_state_batches_and_preserves_each_bound() {
    for with_job in [false, true] {
        let group: Arc<[Arc<str>]> = if with_job {
            Arc::from([Arc::from("le"), Arc::from(JOB)])
        } else {
            Arc::from([Arc::from("le")])
        };
        let tree = histogram_over_sum_tree(Grouping::By(group), HistogramFunction::Quantile(0.5));
        let (partial, final_tree) = FusedSeries::split_aggregate(&tree, TIME).unwrap().unwrap();
        let partitions = vec![
            frames(&[
                labelled_chunk(
                    "a-1",
                    &[("le", "1"), (JOB, "api")],
                    &series_samples(1.0),
                    true,
                ),
                labelled_chunk(
                    "a-inf",
                    &[("le", "+Inf"), (JOB, "api")],
                    &series_samples(4.0),
                    true,
                ),
            ]),
            frames(&[
                labelled_chunk(
                    "b-1",
                    &[("le", "1"), (JOB, "api")],
                    &series_samples(2.0),
                    true,
                ),
                labelled_chunk(
                    "b-inf",
                    &[("le", "+Inf"), (JOB, "api")],
                    &series_samples(4.0),
                    true,
                ),
            ]),
        ];
        let partial_node = FusedSeries::try_new_state_export(
            Arc::from("partial"),
            partial.clone(),
            binding(&partial),
            vec![input_plan(frame_schema())],
        )
        .unwrap();
        let make_partial = || {
            partial_node
                .to_execution_plan(vec![(
                    memory_exec_partitioned(partitions.clone()),
                    RawLabelSource::Explicit,
                    SeriesOrdering::Contiguous,
                )])
                .unwrap()
        };
        let partial_exec = make_partial();
        for partition in 0..2 {
            let states = decode_state_batches(state_batches(&partial_exec, partition).await).await;
            let bounds: std::collections::BTreeMap<_, _> = states
                .iter()
                .map(|state| {
                    let fields = state.fields().unwrap();
                    assert!(fields.is_present(0, 0).unwrap());
                    (
                        state.metric().get("le").unwrap().to_string(),
                        fields
                            .field_values(0, StateComponent::Value)
                            .unwrap()
                            .expect("the histogram state carries a value component")
                            .value(0),
                    )
                })
                .collect();
            let expected = if partition == 0 {
                (1.0, 4.0)
            } else {
                (2.0, 4.0)
            };
            assert_eq!(
                bounds,
                std::collections::BTreeMap::from([
                    ("1".to_string(), expected.0),
                    ("+Inf".to_string(), expected.1),
                ])
            );
        }
        let final_node = FusedSeries::try_new_with_format(
            Arc::from("final"),
            final_tree.clone(),
            binding(&final_tree),
            vec![input_plan(
                canonical_state_frame_schema(&partial).inner().clone(),
            )],
            canonical_frame_schema(&final_tree),
            FusedOutputFormat::SeriesFrame,
        )
        .unwrap();
        let execute = |width| {
            // Region exit is a one-output state router, which forwards whole packed batches.
            let region = Arc::new(SeriesRepartitionExec::try_new(make_partial(), 1).unwrap());
            final_node
                .to_execution_plan_with_parallelism(
                    vec![(region, RawLabelSource::Explicit, SeriesOrdering::Contiguous)],
                    width,
                )
                .unwrap()
        };
        let parallel = execute(8);
        let child = parallel.children()[0];
        if with_job {
            let router = child.downcast_ref::<SeriesRepartitionExec>().unwrap();
            assert_eq!(router.outputs(), 8);
            assert_eq!(router.routing_policy(), &histogram_routing());
        } else {
            assert_eq!(
                parallel
                    .properties()
                    .output_partitioning()
                    .partition_count(),
                1
            );
        }
        let decode = |batches: Vec<RecordBatch>| async move {
            let schema = batches[0].schema();
            unpack_evaluated_series(
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::iter(batches.into_iter().map(Ok)),
                )),
                None,
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
        };
        let got = decode(
            collect(parallel, SessionContext::default().task_ctx())
                .await
                .unwrap(),
        )
        .await;
        let baseline = decode(
            collect(execute(1), SessionContext::default().task_ctx())
                .await
                .unwrap(),
        )
        .await;
        let values = |series: &[(Series, bool)]| {
            series
                .iter()
                .flat_map(|(series, _)| {
                    series
                        .fields
                        .get(0)
                        .unwrap()
                        .iter()
                        .filter_map(|sample| match sample {
                            datatypes::series::samples::SampleRef::Float(v) => Some(v),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(values(&got), values(&baseline));
        // Sum per bound is 3 and 8; 0.5 * 8 lies in the infinite bucket and yields the
        // highest finite bound, 1. Pooling the bounds into one sum would have no histogram.
        assert_eq!(values(&got), vec![1.0; GRID_LEN as usize]);
    }
}

#[tokio::test]
async fn histogram_split_colocates_metadata_distinct_results_before_their_merge() {
    let grouping = Grouping::By(
        ["le", JOB, "__type__", "__unit__"]
            .into_iter()
            .map(Arc::from)
            .collect::<Vec<_>>()
            .into(),
    );
    let instant = instant_of(0, &grid());
    let output = instant.output.clone();
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(NativeNode {
                expr: NativeExpr::Aggregate {
                    input: Box::new(instant),
                    op: BoundAggregate {
                        kind: AggregateKind::Sum,
                        group: grouping,
                        stage: AggregateStage::Complete,
                        field_kind: FieldKind::Float,
                    },
                },
                output: output.clone(),
            }),
            op: BoundVectorOp::Histogram(HistogramFunction::Quantile(0.5)),
        },
        output,
    })
    .unwrap();
    let (partial, final_tree) = FusedSeries::split_aggregate(&tree, TIME).unwrap().unwrap();
    let build = |overlap: bool| {
        let mut partitions = vec![Vec::new(), Vec::new()];
        for (partition, kind) in ["a", "b"].into_iter().enumerate() {
            let timestamps = if overlap || partition == 0 {
                vec![GRID_START]
            } else {
                vec![GRID_START + GRID_STEP]
            };
            for (le, value) in [("1", if partition == 0 { 1.0 } else { 3.0 }), ("+Inf", 4.0)] {
                let key = format!("{kind}-{le}");
                let item = chunk_of(
                    &raw_schema(),
                    &key,
                    metric(
                        &[
                            ("le", le),
                            (JOB, "api"),
                            ("__type__", kind),
                            ("__unit__", kind),
                        ],
                        Some(METRIC),
                    ),
                    &timestamps,
                    vec![vec![Some(value)]],
                    true,
                );
                partitions[partition].push(frame(&item));
            }
        }
        let partial_node = FusedSeries::try_new_state_export(
            Arc::from("partial"),
            partial.clone(),
            binding(&partial),
            vec![input_plan(frame_schema())],
        )
        .unwrap();
        let partial_exec = partial_node
            .to_execution_plan(vec![(
                memory_exec_partitioned(partitions),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            )])
            .unwrap();
        let final_node = FusedSeries::try_new_with_format(
            Arc::from("final"),
            final_tree.clone(),
            binding(&final_tree),
            vec![input_plan(
                canonical_state_frame_schema(&partial).inner().clone(),
            )],
            canonical_frame_schema(&final_tree),
            FusedOutputFormat::SeriesFrame,
        )
        .unwrap();
        final_node
            .to_execution_plan_with_parallelism(
                vec![(
                    partial_exec,
                    RawLabelSource::Explicit,
                    SeriesOrdering::Contiguous,
                )],
                8,
            )
            .unwrap()
    };
    let output = collect(build(false), SessionContext::default().task_ctx())
        .await
        .unwrap();
    let schema = output[0].schema();
    let series = unpack_evaluated_series(
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(output.into_iter().map(Ok)),
        )),
        None,
    )
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let mut values = series
        .iter()
        .flat_map(|(series, _)| {
            assert_eq!(series.metric.get(JOB), Some("api"));
            assert_eq!(series.metric.get("__type__"), None);
            assert_eq!(series.metric.get("__unit__"), None);
            series
                .time
                .positions()
                .zip(series.fields.get(0).unwrap().iter())
                .map(|((point, _), sample)| {
                    let datatypes::series::samples::SampleRef::Float(value) = sample else {
                        panic!("float expected")
                    };
                    (point, value)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values.sort_by_key(|(point, _)| *point);
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], (0, 1.0));
    assert_eq!(values[1].0, 1);
    assert!((values[1].1 - 2.0 / 3.0).abs() < 1e-12);
    assert!(
        collect(build(true), SessionContext::default().task_ctx())
            .await
            .is_err(),
        "overlapping metadata-distinct histograms must collide, not mix buckets"
    );
}

/// The sample time of the piece that fills the start of the statement's grid.
///
/// `count_over_time(x[5m])` reads the window `(g - 5m, g]` at grid point `g`, so one sample at
/// 180s is inside the windows ending at 300s, 360s and 420s and outside the two whose start is at
/// or after it.
const EARLY_PIECE_MS: [i64; 1] = [180_000];

/// The sample times of the piece that fills the end of the grid: both are inside the windows
/// ending at 480s and 540s and outside the one ending at 420s, which starts at 120s.
const LATE_PIECE_MS: [i64; 2] = [450_000, 480_000];

/// The host `label_replace` rewrites the aliased pieces onto, so two raw identities leave the
/// window as one canonical label set.
const ALIASED: &str = "same";

/// A host the rewrite does not match, so the answer holds a second canonical label set and one of
/// the three owners is left with nothing.
const PLAIN: &str = "plain";

/// The tree `label_replace(count_over_time(x[5m]), "host", "same", "host", "web-.*")` compiles to:
/// a window that drops the metric name, then a rewrite that folds the matched hosts onto one value.
///
/// This is the alias the planner produces for that query - `label_replace` writing a label its
/// input already carries is what makes two distinct raw series one canonical label set - narrowed
/// to the hosts this test aliases so that [`PLAIN`] stays a label set of its own. The two pieces
/// sit on disjoint grid positions, so they only form one series once they reach one owner.
fn aliased_count_tree() -> NativeTree {
    let grid = grid();
    let raw = NativeType::Samples(raw_schema());
    let evaluated = evaluated_of(&raw_schema(), &grid);
    let window = NativeNode {
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
                function: RangeFunction::CountOverTime,
                window: RangeWindow {
                    range_ms: RANGE_MS,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                // The input is raw samples, so it has no grid of its own.
                input_grid: None,
                output_grid: Arc::clone(&grid),
            },
        },
        output: evaluated.clone(),
    };
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(window),
            op: BoundMap {
                ops: Arc::from([MapOp::Label(Arc::new(
                    LabelRewrite::replace(HOST, ALIASED, HOST, "web-.*")
                        .expect("the rewrite compiles"),
                ))]),
            },
        },
        output: evaluated,
    })
    .expect("a rewrite over a window is a legal tree")
}

/// The tree `sum_over_time(x[5m])` compiles to over an evaluated child: a window over the values the
/// frame already carries.
///
/// There is no canonical merge in it on purpose. A merge reads across series, so this node would
/// gather the owners back into one execution; the router below has already merged each label set
/// into one chain, which is exactly what leaves the window one series per owner to fold.
fn summed_window_tree() -> NativeTree {
    let grid = grid();
    let evaluated = evaluated_of(&raw_schema(), &grid);
    let input = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: evaluated.clone(),
            ordering: SeriesOrdering::Contiguous,
        }),
        output: evaluated.clone(),
    };
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(input),
            op: BoundWindow {
                function: RangeFunction::SumOverTime,
                window: RangeWindow {
                    range_ms: RANGE_MS,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                // The window reads the series its child declares, on the grid it declares.
                input_grid: Some(Arc::clone(&grid)),
                output_grid: grid,
            },
        },
        output: evaluated,
    })
    .expect("a window over an evaluated input is a legal tree")
}

/// One raw chunk of `{__name__ = METRIC, host}`, holding one sample of 1.0 at each of `times` and
/// closing it: `count_over_time` counts the samples, so what they hold never reaches the answer.
fn piece_chunk(host: &str, times: &[i64]) -> RawSeriesChunk {
    chunk_of(
        &raw_schema(),
        host,
        metric(&[(HOST, host)], Some(METRIC)),
        times,
        vec![times.iter().map(|_| Some(1.0)).collect()],
        true,
    )
}

/// A real `FusedSeriesExec` running [`aliased_count_tree`] over `input`, exporting one evaluated
/// frame per partition: `A` of the composition below.
fn aliasing_producer_over(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let tree = aliased_count_tree();
    let node = FusedSeries::try_new_with_format(
        Arc::from(
            "label_replace(count_over_time(x[5m]), \"host\", \"same\", \"host\", \"web-.*\")",
        ),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .expect("the binding matches the tree");
    plan_of(node, input).expect("the runtime executes the counting tree")
}

/// The same over one memory source partition per entry of `partitions`.
fn aliasing_producer(partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    aliasing_producer_over(memory_exec_partitioned(partitions))
}

/// A real `FusedSeriesExec` running [`summed_window_tree`] over `input`: `B`.
fn summed_window_over(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    plan_of(fused(summed_window_tree()), input).expect("the runtime executes the folding tree")
}

/// Every owner `plan` publishes, read at once, with the chunks each of them handed over.
///
/// Reading them together states the partitioned shape this node is part of: the routing decides
/// which owner an identity belongs to, so a caller claims every owner's stream and reads them as
/// one composition. The owners' queues are unbounded, so this is the shape being tested rather than
/// a requirement the read has to meet - draining one owner to its end before starting the next
/// would still see the same chunks.
async fn owner_chunks(plan: &Arc<dyn ExecutionPlan>) -> Vec<Vec<(Series, bool)>> {
    let context = SessionContext::default().task_ctx();
    let reads = (0..plan.properties().partitioning.partition_count())
        .map(|owner| {
            let plan = Arc::clone(plan);
            let context = Arc::clone(&context);
            async move {
                let frames = plan.execute(owner, context).expect("the owner executes");
                unpack_evaluated_series(frames, None)
                    .expect("an owner declares an evaluated frame")
                    .try_collect::<Vec<_>>()
                    .await
            }
        })
        .collect::<Vec<_>>();
    futures::future::try_join_all(reads)
        .await
        .expect("every owner is read")
}

/// The `(grid position, value)` pairs `series` carries, in position order.
fn carried_positions(series: &Series) -> Vec<(usize, f64)> {
    let mut carried = series
        .time
        .positions()
        .map(|(position, row)| {
            let value = match series.fields.as_slice()[0].sample(row) {
                datatypes::series::samples::SampleRef::Float(value) => value,
                _ => panic!("every fixture here holds float samples"),
            };
            (position, value)
        })
        .collect::<Vec<_>>();
    carried.sort_unstable_by_key(|(position, _)| *position);
    carried
}

/// One canonical chain: the label set it belongs to, whether it closed, and the `(grid position,
/// value)` pairs it carries in position order.
type Chain = (String, bool, Vec<(usize, f64)>);

/// The chains `chunks` hands over.
///
/// A chain is a run of rows of one label set closed by its last row: a frame encodes a merged series
/// as one row per dense sample segment, so one chain can be several rows, and two rows of one label
/// set that are not adjacent in the stream are two chains rather than one.
fn chains_of(chunks: &[(Series, bool)]) -> Vec<Chain> {
    let mut chains: Vec<Chain> = Vec::new();
    for (series, is_last) in chunks {
        let host = series
            .metric
            .get(HOST)
            .expect("`A`'s window keeps the host label")
            .to_string();
        match chains.last_mut() {
            Some((last, closed, carried)) if *last == host && !*closed => {
                carried.extend(carried_positions(series));
                *closed = *is_last;
            }
            _ => chains.push((host, *is_last, carried_positions(series))),
        }
    }
    chains
}

/// The rows each partition of `plan` answers, all of them read at once, with whether each partition
/// reported its end and reported it again on the poll after.
async fn polled_partition_rows(plan: &Arc<dyn ExecutionPlan>) -> Vec<(Vec<Row>, bool)> {
    let context = SessionContext::default().task_ctx();
    let reads = (0..plan.properties().partitioning.partition_count())
        .map(|partition| {
            let plan = Arc::clone(plan);
            let context = Arc::clone(&context);
            async move {
                let mut stream = plan
                    .execute(partition, context)
                    .expect("the partition executes");
                let mut batches = Vec::new();
                while let Some(batch) = stream.next().await {
                    batches.push(batch.expect("the partition answers instead of failing"));
                }
                // The end above is the input's own: a chain the producer abandoned arrives as an
                // error rather than as an end, so what is left to check is that the end is final.
                (rows(&batches), stream.next().await.is_none())
            }
        })
        .collect::<Vec<_>>();
    futures::future::join_all(reads).await
}

/// A real `Fused A` with two partitions, a three-owner `SeriesRepartitionExec` and a real `Fused B`
/// folding what each owner was handed: the composition this router's merge is for, with N != M.
///
/// The stages are stated separately (2, 3, 3) so no count stands for another: two owners over two
/// upstream partitions would hide an implementation that assumed owner `i` were `A`'s partition `i`,
/// and three over two has no such index to carry. The nodes are wired by hand here, which makes the
/// width of three this test's choice rather than something the planner produced.
///
/// The input is the aliasing this composition exists for: `http_requests_total{host="web-1"}` and
/// `http_requests_total{host="web-2"}` are two raw identities that both leave `count_over_time`'s
/// window as the one canonical label set `{host="same"}`. They are placed on *different* `A`
/// partitions - which is what keeps them from being refused as two raw keys closing on one label
/// set inside a single execution - so the same label set reaches the router twice, from two
/// partitions. The pieces hold disjoint grid positions, so they only form one series (`1, 1, 1, 2,
/// 2`) once they reach one owner, and `sum_over_time` folds that series to `1, 2, 3, 5, 7`; a merge
/// that skipped a piece, or put it on another owner, could answer neither.
///
/// The router is read on its own first, so that what it hands over is checked where it decides it:
/// the new contract is that an identity's pieces are merged *into one canonical chain* before they
/// are published, so each label set leaves as one run of rows closed by its last row - a frame
/// encodes a merged series as one row per dense sample segment, so that run can be several rows -
/// and the Fused above has nothing left to collect. Then the whole composition runs on the same
/// fixtures with all three owners read at once, which is the partitioned shape it exists for, and
/// with the reads of `A` counted: three owners over two upstream partitions have to be one read of
/// each partition, fanned out, rather than a re-read per owner or a gather.
#[tokio::test]
async fn a_real_three_owner_repartition_between_two_real_fuseds_merges_every_piece() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        // Two partitions: the early piece of the aliased label set on one, its late piece beside
        // the plain series on the other, so two canonical label sets go in over two partitions and
        // one of them arrives as two pieces.
        let partitions = vec![
            vec![frame(&piece_chunk("web-1", &EARLY_PIECE_MS))],
            vec![
                frame(&piece_chunk("web-2", &LATE_PIECE_MS)),
                frame(&piece_chunk(PLAIN, &EARLY_PIECE_MS)),
            ],
        ];

        // First the router's own stage, and what it hands over.
        let a = aliasing_producer(partitions.clone());
        assert_eq!(
            2,
            a.properties().partitioning.partition_count(),
            "A runs its window once per scan partition, so two partitions go in"
        );
        let probe: Arc<dyn ExecutionPlan> = Arc::new(
            SeriesRepartitionExec::try_new(a, 3)
                .expect("the router is built over A's evaluated frames"),
        );
        assert_eq!(
            3,
            probe.properties().partitioning.partition_count(),
            "the router publishes the three owners it was asked for, and not A's two"
        );

        let routed = owner_chunks(&probe).await;
        assert_eq!(3, routed.len(), "one read per owner, all of them at once");
        let handed = routed
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<(Series, bool)>>();
        for (series, _) in &handed {
            assert_eq!(
                None,
                series.metric.name(),
                "`count_over_time` drops the metric name"
            );
        }
        // The contract this test is about: the pieces of one identity are merged by the owner that
        // receives them, so each canonical label set leaves as ONE chain - a run of rows that only
        // its last row closes - and not as one chain per input partition for the Fused above to
        // collect.
        let chains = chains_of(&handed);
        assert_eq!(
            2,
            chains.len(),
            "one canonical chain per label set, and not one per input partition: {chains:?}"
        );
        assert_eq!(
            2,
            handed.iter().filter(|(_, is_last)| *is_last).count(),
            "each canonical chain closes exactly once: {handed:?}"
        );
        for (host, closed, _) in &chains {
            assert!(*closed, "the owner closes the chain it merged: `{host}`");
        }
        let mut hosts = chains.iter().map(|(host, ..)| host.clone()).collect::<Vec<_>>();
        hosts.sort();
        let mut went_in = vec![ALIASED.to_string(), PLAIN.to_string()];
        went_in.sort();
        assert_eq!(
            went_in, hosts,
            "the two canonical label sets that went in are the two that came out"
        );
        let carried_by = |host: &str| {
            chains
                .iter()
                .find(|(found, ..)| found == host)
                .map(|(_, _, carried)| carried.clone())
                .unwrap_or_else(|| panic!("`{host}` is handed over: {chains:?}"))
        };
        // Both pieces are in the one chain: the early one counts 1 at the first three grid points
        // and the late one 2 at the last two, and only a merge of the two carries both.
        assert_eq!(
            vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 2.0), (4, 2.0)],
            carried_by(ALIASED),
            "`{{host=\"{ALIASED}\"}}` is one chain over the whole grid, carrying both A partitions' \
             pieces"
        );
        assert_eq!(
            vec![(0, 1.0), (1, 1.0), (2, 1.0)],
            carried_by(PLAIN),
            "`{{host=\"{PLAIN}\"}}` is its own chain, and its piece covers only the start"
        );

        // Then the whole composition, on the same fixtures: three owners over one read of A.
        let reads = Arc::new(
            (0..partitions.len())
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>(),
        );
        let router: Arc<dyn ExecutionPlan> = Arc::new(
            SeriesRepartitionExec::try_new(
                aliasing_producer_over(Arc::new(CountedExec {
                    input: memory_exec_partitioned(partitions),
                    calls: Arc::clone(&reads),
                })),
                3,
            )
            .expect("the router is built over A's evaluated frames"),
        );
        let b = summed_window_over(Arc::clone(&router));
        assert_eq!(
            3,
            b.properties().partitioning.partition_count(),
            "the router's merge leaves each canonical label set whole in the owner that holds it, so \
             B runs one execution per owner instead of gathering them into one or following A's two"
        );

        // Every owner is read at once, which is the partitioned composition this test is about: one
        // stream per owner, all of them claimed together, rather than one execution answering for
        // all of them.
        let answered = polled_partition_rows(&b).await;
        assert_eq!(3, answered.len(), "B runs once per owner of the router");
        for (owner, (_, ended)) in answered.iter().enumerate() {
            assert!(
                *ended,
                "owner {owner} of B reports its end, and the poll after it reported `None` again"
            );
        }
        // Draining the three owners together is one read of A's two partitions, not one read per
        // owner and not one owner gathering what the others were routed: the router fans one read
        // out to its owners.
        assert_eq!(
            vec![1, 1],
            reads
                .iter()
                .map(|calls| calls.load(Ordering::SeqCst))
                .collect::<Vec<_>>(),
            "the three owners read each of A's partitions once"
        );

        // Each owner answers the label set the router put on it, and an owner the router left with
        // nothing answers nothing: the fold above keeps the placement instead of re-deciding it.
        let expected = |host: &str, sums: [f64; GRID_LEN as usize]| -> Vec<Row> {
            grid_timestamps()
                .into_iter()
                .zip(sums)
                .map(|(time, value)| (time, Some(value), Some(host.to_string()), None))
                .collect()
        };
        let aliased_rows = expected(ALIASED, [1.0, 2.0, 3.0, 5.0, 7.0]);
        let plain_rows = expected(PLAIN, [1.0, 2.0, 3.0, 3.0, 3.0]);
        for (owner, (rows, _)) in answered.iter().enumerate() {
            let mut routed_here = routed[owner]
                .iter()
                .map(|(series, _)| {
                    series
                        .metric
                        .get(HOST)
                        .expect("`A`'s window keeps the host label")
                })
                .collect::<Vec<_>>();
            // One chain can be several rows, so what an owner carried is the set of label sets it
            // was routed, not the rows they arrived in.
            routed_here.sort_unstable();
            routed_here.dedup();
            let mut answered_here = rows
                .iter()
                .map(|(_, _, host, _)| {
                    host.as_deref()
                        .expect("`B`'s window keeps the host label")
                })
                .collect::<Vec<_>>();
            answered_here.sort_unstable();
            answered_here.dedup();
            assert_eq!(
                routed_here, answered_here,
                "owner {owner} answers the label sets the router put on it and no other: {rows:?}"
            );
            // Each of those label sets is answered as one series over the whole grid: the pieces
            // of the aliased one were merged below, and the fold above reads the whole of each.
            let mut wanted = answered_here
                .iter()
                .flat_map(|host| match *host {
                    ALIASED => aliased_rows.clone(),
                    PLAIN => plain_rows.clone(),
                    other => panic!("owner {owner} answered a label set that went in: {other}"),
                })
                .collect::<Vec<Row>>();
            wanted.sort_by(|left, right| left.partial_cmp(right).expect("rows of a grid compare"));
            let mut got = rows.clone();
            got.sort_by(|left, right| left.partial_cmp(right).expect("rows of a grid compare"));
            assert_eq!(
                wanted, got,
                "owner {owner} answers every label set it was routed, one series over the whole \
                 grid each"
            );
        }
        // The router routes by a hash of the label set, so two identities can land on one owner:
        // how many owners answer is not fixed. Two label sets cannot fill three owners, though, so
        // at least one is empty - and an empty owner answers nothing rather than inventing a series
        // for it.
        assert!(
            answered.iter().any(|(rows, _)| rows.is_empty()),
            "an owner that was routed nothing answers nothing: {answered:?}"
        );
        assert_eq!(
            2 * GRID_LEN as usize,
            answered.iter().map(|(rows, _)| rows.len()).sum::<usize>(),
            "two canonical label sets in, two series out, one row per grid point"
        );
    })
    .await
    .expect("the composition finishes instead of waiting for an owner nobody reads");
}

// ---------------------------------------------------------------------------
// The in-process handover
// ---------------------------------------------------------------------------

// The two ends of the handover, each checked against the frame path it replaces.
//
// A raw source decodes a region's frames into chunks, and a fused node decodes frames into the same
// chunks; between two of our own operators neither has to. The fixtures below are the same for both
// ends, so the tests live together: what has to hold is that skipping the encode changes nothing a
// consumer can see - the raw source's frames and its chunks carry the same values, and a tree run
// over the chunks answers the same batches as one run over the frames.
//
// The fixtures carry what a raw frame can carry and a naive comparison would miss: several series,
// a null sample in each, one series whose field holds native histograms rather than floats, and a
// series split over two chunks.

/// One sample as these tests compare it.
#[derive(Clone, Debug, PartialEq)]
enum DecodedSample {
    /// No sample at this position.
    Missing,
    /// The bits of the value, so a signed zero or a NaN payload is compared rather than coerced.
    Float(u64),
    /// A native histogram, in the canonical form both paths encode it as.
    Histogram(NativeHistogram),
}

/// One chunk as these tests compare it: the decoded values, in the order they arrived.
///
/// The buffers a chunk happens to share are left out on purpose. The two paths may hand over
/// different buffers, and what has to be identical is what a consumer reads.
#[derive(Clone, Debug, PartialEq)]
struct DecodedChunk {
    /// The key the series was read under, byte for byte.
    key: Vec<u8>,
    /// Its complete labels, when the frame named it by them.
    metric: Option<SeriesLabels>,
    /// Its timestamps, in the order the chunk carries them.
    time: Vec<i64>,
    /// One sample per timestamp, one list per field.
    fields: Vec<Vec<DecodedSample>>,
    /// Whether this chunk closes its series.
    is_last: bool,
}

/// `chunk` as these tests compare it.
fn decoded_chunk(chunk: &RawSeriesChunk) -> DecodedChunk {
    DecodedChunk {
        key: chunk.primary_key.as_slice().to_vec(),
        metric: chunk.metric.clone(),
        time: (0..chunk.time.len())
            .map(|index| chunk.time.timestamp(index).expect("the index is in range"))
            .collect(),
        fields: chunk.fields.iter().map(decoded_field).collect(),
        is_last: chunk.is_last,
    }
}

/// One field of a chunk as these tests compare it.
fn decoded_field(field: &Samples) -> Vec<DecodedSample> {
    field.iter().map(decoded_sample).collect()
}

/// One sample as these tests compare it, whichever way a chunk carries it.
fn decoded_sample(sample: SampleRef) -> DecodedSample {
    match sample {
        SampleRef::Missing => DecodedSample::Missing,
        SampleRef::Float(value) => DecodedSample::Float(value.to_bits()),
        SampleRef::Histogram(view) => DecodedSample::Histogram(canonical_histogram(&view)),
    }
}

/// The histogram batch `sums` describes, one histogram per entry.
fn histogram_batch(sums: &[f64]) -> Arc<HistogramBatch> {
    let mut builder = HistogramBatchBuilder::new();
    let positive_offsets = [1];
    let positive_lengths = [2];
    let positive_buckets = [1.5, 2.5];
    for (index, sum) in sums.iter().enumerate() {
        builder
            .push(HistogramView {
                schema: 1,
                zero_threshold: 0.5,
                sum: *sum,
                count: 8.0,
                zero_count: 1.0,
                reset_hint: CounterResetHint::Unknown,
                start_timestamp: Some(index as i64 * 1_000),
                custom_values: &[],
                positive: BucketSideView {
                    span_offsets: &positive_offsets,
                    span_lengths: &positive_lengths,
                    buckets: &positive_buckets,
                },
                negative: BucketSideView {
                    span_offsets: &[],
                    span_lengths: &[],
                    buckets: &[],
                },
            })
            .expect("the fixture histogram is legal");
    }
    builder.finish().expect("the fixture batch finishes")
}

/// The chunks these tests hand over: four timestamps in all, spread over several series, with a
/// null sample, a series in two chunks, and one series whose field holds native histograms.
fn handover_chunks() -> Vec<RawSeriesChunk> {
    let times = [60_000_i64, 120_000, 180_000, 240_000];
    // A float series with a hole in it: the field carries one null, which both paths have to keep
    // as a missing sample rather than as a zero.
    let floats = |values: Vec<Option<f64>>| vec![values];
    let mut chunks = vec![
        chunk_of(
            &raw_schema(),
            "web-1",
            metric(&[(HOST, "web-1")], Some(METRIC)),
            &times,
            floats(vec![Some(1.0), None, Some(3.0), Some(4.0)]),
            false,
        ),
        // The same series, closed by a second chunk, so a chain has to be read in order.
        chunk_of(
            &raw_schema(),
            "web-1",
            metric(&[(HOST, "web-1")], Some(METRIC)),
            &[300_000, 360_000],
            floats(vec![Some(5.0), Some(6.0)]),
            true,
        ),
        chunk_of(
            &raw_schema(),
            "web-2",
            metric(&[(HOST, "web-2")], Some(METRIC)),
            &times,
            floats(vec![None, Some(2.5), Some(2.5), None]),
            true,
        ),
    ];
    // The histogram series: two histograms and one position with no sample, in the same field name
    // the float series use, because a raw frame's field is a payload that can hold either.
    let histograms = Samples::try_from_histogram(
        UInt32Array::from(vec![Some(0), None, Some(1), None]),
        histogram_batch(&[10.5, 20.5]),
    )
    .expect("the fixture histograms are aligned with the timestamps");
    chunks.push(
        RawSeriesChunk::try_new(
            &raw_schema(),
            Buffer::from_slice_ref(b"web-3".as_slice()),
            Some(metric(&[(HOST, "web-3")], Some(METRIC))),
            RawTimestamps::try_new(ScalarBuffer::from(times.to_vec())).expect("ascending times"),
            SeriesFields::try_from_vec(vec![histograms]).expect("one field over four positions"),
            true,
        )
        .expect("the fixture histogram series is a legal chunk"),
    );
    chunks
}

/// `chunks` as one frame batch each, declared under `schema`.
///
/// The packer writes the schema it declares, so a caller that wants another one - an ordering this
/// node does not publish, say - re-declares the packed columns under it.
fn frames_under(schema: &SchemaRef, chunks: &[RawSeriesChunk]) -> Vec<RecordBatch> {
    chunks
        .iter()
        .map(|chunk| {
            let packed =
                pack_raw_series_chunk(&raw_schema(), FrameLabelMode::Explicit, None, chunk)
                    .expect("a chunk of this schema packs");
            assert_eq!(1, packed.len(), "one small chunk is one frame batch");
            RecordBatch::try_new(Arc::clone(schema), packed[0].columns().to_vec())
                .expect("the packed columns match the declared frame")
        })
        .collect()
}

/// The frame schema of these series under `ordering`.
fn frame_schema_with_ordering(ordering: SeriesOrdering) -> SchemaRef {
    frame_schema_with(&raw_schema(), FrameLabelMode::Explicit, None, ordering)
        .expect("explicit labels are a frame under any ordering")
}

/// The producer's two entry points decode the same chunks.
///
/// The frame path here is the published frame decoded back, which is exactly what a consumer on the
/// other side of a boundary does with it; the in-process path is what a consumer that is native
/// like this one receives instead. They have to agree chunk for chunk, including the labels, the
/// nulls, the finality and the native histograms - otherwise the shortcut would be a second decoder
/// rather than the same one without the encode.
#[tokio::test]
async fn the_raw_source_hands_over_the_chunks_its_frames_decode_to() {
    // The child's frames arrive two ways. Under `Contiguous` they are already this node's own
    // frame, so the frame path re-declares them without decoding at all. Under `Interleaved` they
    // are in an order this node does not publish, so the frame path decodes them and packs them
    // again - which is where the in-process path and the frame path share their decode.
    for pass_through in [true, false] {
        let chunks = handover_chunks();
        let child = if pass_through {
            memory_exec(frames_under(&frame_schema(), &chunks))
        } else {
            let schema = frame_schema_with_ordering(SeriesOrdering::Interleaved);
            memory_exec_partitioned_with(Arc::clone(&schema), vec![frames_under(&schema, &chunks)])
        };
        let source = raw_source(child);
        let exec = source
            .downcast_ref::<RawSeriesSourceExec>()
            .expect("the fixture root is the region's raw source");
        let context = SessionContext::default().task_ctx();

        // What a consumer behind a boundary reads: the published frame, decoded.
        let published = exec
            .execute(0, Arc::clone(&context))
            .expect("the node has one partition");
        let from_frames = unpack_raw_series(published, Some(raw_schema()))
            .expect("the node publishes a raw frame of these fields")
            .try_collect::<Vec<_>>()
            .await
            .expect("the published frames decode");

        // What a native consumer reads: the chunks themselves.
        let handed_over = series_execution_plan(source.as_ref())
            .expect("the raw source hands its chunks over")
            .execute_series(0, context)
            .expect("the node has one partition")
            .try_collect::<Vec<_>>()
            .await
            .expect("the chunks arrive");
        let in_process = handed_over
            .into_iter()
            .map(|handed| match handed.into_parts().0 {
                SeriesChunk::Raw(chunk) => decoded_chunk(&chunk),
                other => panic!("a raw source handed over a chunk it cannot produce: {other:?}"),
            })
            .collect::<Vec<_>>();

        assert_eq!(
            handover_chunks().len(),
            in_process.len(),
            "every fixture chunk is handed over, pass-through: {pass_through}"
        );
        assert_eq!(
            from_frames.iter().map(decoded_chunk).collect::<Vec<_>>(),
            in_process,
            "the in-process chunks are the published frame decoded, pass-through: {pass_through}"
        );
    }
}

/// A tree run over in-process chunks answers the same batches as one run over their frames.
///
/// This is the consumer's half of the handover: the node reads chunks instead of decoding frames,
/// and everything above it - the tree, its kernels, the row conversion - sees the same input, so
/// the answer has to be identical batch for batch, including where each batch ends.
#[tokio::test]
async fn the_in_process_handover_answers_the_batches_the_frames_answer() {
    let chunks = handover_chunks();
    // Only the float series: a tree binds one value kind for its whole output, so a fixture that
    // holds histograms in some series would have to be read by a histogram tree.
    let chunks = chunks
        .into_iter()
        .filter(|chunk| {
            chunk
                .metric
                .as_ref()
                .and_then(|metric| metric.get(HOST))
                .is_some_and(|host| host != "web-3")
        })
        .collect::<Vec<_>>();
    let source = raw_source(memory_exec(frames_under(&frame_schema(), &chunks)));
    let context = SessionContext::default().task_ctx();

    // The same frames the second plan reads, taken from the node that publishes them, so both plans
    // read one input rather than two that are assumed to be alike.
    let published = collect(Arc::clone(&source), Arc::clone(&context))
        .await
        .expect("the raw source publishes its frames");
    let framed = memory_exec_partitioned_with(source.schema(), vec![published]);

    let in_process =
        plan(rate_tree(), Arc::clone(&source)).expect("the runtime executes a rate tree");
    let over_frames = plan(rate_tree(), framed).expect("the runtime executes a rate tree");
    assert!(
        uses_in_process_series(&in_process),
        "every input of this plan is a native operator, so the node reads it in process"
    );
    assert!(
        !uses_in_process_series(&over_frames),
        "a memory leaf publishes frames, so the node reads them"
    );

    let in_process_batches = collect(Arc::clone(&in_process), context.clone())
        .await
        .expect("the node runs over the chunks");
    let frame_batches = collect(Arc::clone(&over_frames), context)
        .await
        .expect("the node runs over the frames");
    assert!(
        !in_process_batches.is_empty(),
        "the fixture is a grid the tree answers on"
    );
    assert_eq!(
        frame_batches, in_process_batches,
        "the two paths answer the same batches, in the same order"
    );
    // Nothing a reader observes may change with the representation. The figures the tree records
    // are recorded per chunk it was fed, and the fast path feeds the producer's own chunks, which
    // are the ones the frame path decodes back - every fixture chunk here is one dense field
    // segment, so a frame row is a chunk and the counts agree exactly, not just the rows.
    assert_eq!(
        node_counters(&over_frames),
        node_counters(&in_process),
        "the tree records the same figures either way"
    );
}

/// The counters the fused node of `plan` registered, by name, so two executions can be compared.
fn node_counters(plan: &Arc<dyn ExecutionPlan>) -> Vec<(String, usize)> {
    let mut counters = node_metric_set(plan)
        .iter()
        .filter_map(|metric| match metric.value() {
            MetricValue::Count { count, .. } => {
                Some((metric.value().name().to_string(), count.value()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    counters.sort();
    counters
}

// ---------------------------------------------------------------------------
// The routing in front of a native consumer
// ---------------------------------------------------------------------------

/// Whether the fused node at the top of `plan` reads any of its inputs in process.
///
/// This is the question the node asks itself, one input at a time, before it decides how to read that
/// input - so a test that says "this plan goes through the handover" states what the node does,
/// rather than what some build-time flag was set to. There is no content the node reads another way:
/// every input whose operator answers is read in process, aggregate state included.
fn uses_in_process_series(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let fused = plan
        .downcast_ref::<FusedSeriesExec>()
        .expect("the plan under test is one fused node");
    (0..fused.inputs.len())
        .any(|source| series_execution_plan(fused.inputs[source].0.as_ref()).is_some())
}

/// The frames each partition of `plan` publishes, in order, read once.
///
/// This is what a consumer behind a boundary receives from each owner: the batches exactly as the
/// producer wrote them, which is the fixture the frame path of these tests is then given.
async fn published_frames(plan: &Arc<dyn ExecutionPlan>) -> Vec<Vec<RecordBatch>> {
    let context = SessionContext::default().task_ctx();
    let mut partitions = Vec::new();
    for partition in 0..plan.properties().partitioning.partition_count() {
        let mut stream = plan
            .execute(partition, Arc::clone(&context))
            .expect("every partition of the fixture executes");
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.expect("the fixture publishes frames instead of failing"));
        }
        partitions.push(batches);
    }
    partitions
}

/// The raw chunks `batches` decode to, as a consumer behind the boundary recovers them.
async fn raw_frames_decoded(schema: SchemaRef, batches: Vec<RecordBatch>) -> Vec<DecodedChunk> {
    let leaf = memory_exec_partitioned_with(schema, vec![batches]);
    let frames = leaf
        .execute(0, SessionContext::default().task_ctx())
        .expect("the fixture's frames are re-read");
    unpack_raw_series(frames, Some(raw_schema()))
        .expect("the owner publishes a raw frame of these fields")
        .try_collect::<Vec<_>>()
        .await
        .expect("the frames decode")
        .iter()
        .map(decoded_chunk)
        .collect()
}

/// One evaluated chunk as these tests compare it.
///
/// The samples, the labels and the finality are what a consumer reads; the buffers they sit in are
/// not, and the two paths are allowed to aim at different ones.
#[derive(Clone, Debug, PartialEq)]
struct DecodedSeries {
    /// The complete labels the series belongs to.
    metric: SeriesLabels,
    /// The grid positions it carries, in order.
    positions: Vec<i64>,
    /// One list of samples per field.
    fields: Vec<Vec<DecodedSample>>,
    /// Whether this chunk closes its series.
    is_last: bool,
}

/// `series` as these tests compare it.
fn decoded_evaluated(series: &Series, is_last: bool) -> DecodedSeries {
    DecodedSeries {
        metric: series.metric.clone(),
        positions: series
            .time
            .positions()
            .map(|(position, _)| position as i64)
            .collect(),
        fields: series.fields.as_slice().iter().map(decoded_field).collect(),
        is_last,
    }
}

/// `chunks` joined into whole series, in a fixed order, so two lists of them can be compared.
///
/// A frame row never spans two segments of one field, so a series whose values sit in two dense
/// segments is one chunk in process and two rows in a frame - the split-segment difference the
/// handover documents. What both paths have to agree on is the series, not how many rows it was
/// encoded as, so adjacent pieces of one series are joined before they are compared.
fn join_raw(mut chunks: Vec<DecodedChunk>) -> Vec<DecodedChunk> {
    chunks.sort_by(|left, right| {
        (left.key.clone(), left.time.first()).cmp(&(right.key.clone(), right.time.first()))
    });
    let mut joined: Vec<DecodedChunk> = Vec::new();
    for chunk in chunks {
        match joined.last_mut() {
            Some(open) if open.key == chunk.key && !open.is_last => {
                open.time.extend(chunk.time);
                for (into, from) in open.fields.iter_mut().zip(chunk.fields) {
                    into.extend(from);
                }
                open.is_last = chunk.is_last;
            }
            _ => joined.push(chunk),
        }
    }
    joined
}

/// [`join_raw`] for evaluated pieces.
fn join_evaluated(mut series: Vec<DecodedSeries>) -> Vec<DecodedSeries> {
    series.sort_by(|left, right| {
        (format!("{:?}", left.metric), &left.positions)
            .cmp(&(format!("{:?}", right.metric), &right.positions))
    });
    let mut joined: Vec<DecodedSeries> = Vec::new();
    for chunk in series {
        match joined.last_mut() {
            Some(open) if open.metric == chunk.metric && !open.is_last => {
                open.positions.extend(chunk.positions);
                for (into, from) in open.fields.iter_mut().zip(chunk.fields) {
                    into.extend(from);
                }
                open.is_last = chunk.is_last;
            }
            _ => joined.push(chunk),
        }
    }
    joined
}

/// The evaluated series `batches` decode to, as a consumer behind the boundary recovers them.
async fn evaluated_frames_decoded(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Vec<DecodedSeries> {
    let leaf = memory_exec_partitioned_with(schema, vec![batches]);
    let frames = leaf
        .execute(0, SessionContext::default().task_ctx())
        .expect("the fixture's frames are re-read");
    unpack_evaluated_series(frames, None)
        .expect("the owner publishes an evaluated frame")
        .try_collect::<Vec<(Series, bool)>>()
        .await
        .expect("the frames decode")
        .iter()
        .map(|(series, is_last)| decoded_evaluated(series, *is_last))
        .collect()
}

/// The raw chunks `owner` of `plan` hands over, decoded, as a native consumer receives them.
async fn handed_raw_chunks(plan: &Arc<dyn ExecutionPlan>, owner: usize) -> Vec<DecodedChunk> {
    let handed = series_execution_plan(plan.as_ref())
        .expect("the router hands its chunks over")
        .execute_series(owner, SessionContext::default().task_ctx())
        .expect("the owner hands its chunks over")
        .try_collect::<Vec<_>>()
        .await
        .expect("the chunks arrive");
    handed
        .into_iter()
        .map(|handed| match handed.into_parts().0 {
            SeriesChunk::Raw(chunk) => decoded_chunk(&chunk),
            other => panic!("a raw owner handed over {other:?}"),
        })
        .collect()
}

/// The evaluated chunks `owner` of `plan` hands over, decoded, as a native consumer receives them.
async fn handed_evaluated_chunks(
    plan: &Arc<dyn ExecutionPlan>,
    owner: usize,
) -> Vec<DecodedSeries> {
    let handed = series_execution_plan(plan.as_ref())
        .expect("the router hands its chunks over")
        .execute_series(owner, SessionContext::default().task_ctx())
        .expect("the owner hands its chunks over")
        .try_collect::<Vec<_>>()
        .await
        .expect("the chunks arrive");
    handed
        .into_iter()
        .map(|handed| match handed.into_parts().0 {
            SeriesChunk::Evaluated(chunk) => decoded_evaluated(&chunk.series, chunk.is_last),
            other => panic!("an evaluated owner handed over {other:?}"),
        })
        .collect()
}

/// A router over `partitions`' raw frames, publishing `owners` outputs.
fn raw_router(partitions: Vec<Vec<RecordBatch>>, owners: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        SeriesRepartitionExec::try_new(
            memory_exec_partitioned_with(frame_schema(), partitions),
            owners,
        )
        .expect("raw frames route by the stored primary key"),
    )
}

/// The raw series the routing tests route: thirteen identities over two input partitions.
///
/// Twelve of them are closed chains that arrive whole from one partition each, so what the routing
/// has to do is send each to its owner and leave it alone. The thirteenth, `web-split`, arrives as
/// two closed chains of the same stored key, one from each partition - a frame input has to end
/// between chains, so a chain cannot be left open at the end of a partition - and the owner it is
/// routed to has to merge them into one identity: the raw form of the merge a router exists for.
fn routed_raw_partitions() -> Vec<Vec<RecordBatch>> {
    let times = [60_000_i64, 120_000];
    let closed = |host: &str| {
        frame(&chunk_of(
            &raw_schema(),
            host,
            metric(&[(HOST, host)], Some(METRIC)),
            &times,
            vec![vec![Some(1.0), Some(2.0)]],
            true,
        ))
    };
    let piece = |host: &str, time: i64, value: f64, last: bool| {
        frame(&chunk_of(
            &raw_schema(),
            host,
            metric(&[(HOST, host)], Some(METRIC)),
            &[time],
            vec![vec![Some(value)]],
            last,
        ))
    };
    let mut early = (0..6)
        .map(|index| closed(&format!("web-{index}")))
        .collect::<Vec<_>>();
    let mut late = (6..12)
        .map(|index| closed(&format!("web-{index}")))
        .collect::<Vec<_>>();
    early.push(piece("web-split", 60_000, 1.0, true));
    late.push(piece("web-split", 120_000, 2.0, true));
    vec![early, late]
}

/// The evaluated series the routing tests route, through the window-and-rewrite producer.
///
/// Four canonical label sets over two input partitions and three owners: `web-1` and `web-2` are two
/// raw identities the rewrite folds onto `{host="same"}`, and their pieces sit on disjoint grid
/// positions, so that one label set only becomes one series once both halves reach one owner.
fn routed_evaluated_partitions() -> Vec<Vec<RecordBatch>> {
    let piece = |host: &str, times: &[i64]| frame(&piece_chunk(host, times));
    vec![
        vec![
            piece("web-1", &EARLY_PIECE_MS),
            piece(PLAIN, &EARLY_PIECE_MS),
            piece("third", &EARLY_PIECE_MS),
        ],
        vec![
            piece("web-2", &LATE_PIECE_MS),
            piece("fourth", &EARLY_PIECE_MS),
        ],
    ]
}

/// A three-owner raw router hands over the chunks its frames decode to, owner by owner.
///
/// The producer's half of the handover for a router: what it publishes as frames and what it hands
/// over in process have to be the same series - the same key, labels, samples and finality - and
/// the merge it performs on an identity that arrived from two partitions has to have happened in
/// both, not in one. Owner by owner, because which owner an identity lands on is the routing
/// decision itself, and per owner as whole series, because how many rows one series is encoded as
/// is the encoder's business.
#[tokio::test]
async fn a_three_owner_raw_router_hands_over_what_its_frames_decode_to() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        // The frame side: one router's publication, read owner by owner, which is both what a
        // boundary consumer reads and the fixture the frame path below is given.
        let publishing = raw_router(routed_raw_partitions(), 3);
        let published = published_frames(&publishing).await;
        assert_eq!(3, published.len(), "one publication per owner");
        assert_eq!(
            3,
            published
                .iter()
                .filter(|batches| !batches.is_empty())
                .count(),
            "thirteen identities over three owners reach all three, which is the re-routing this \
             test needs: {published:?}"
        );

        // The chunk side reads the same fixture through a router of its own.
        let handing = raw_router(routed_raw_partitions(), 3);
        let mut in_process = Vec::new();
        for owner in 0..3 {
            in_process.push(handed_raw_chunks(&handing, owner).await);
        }
        assert_eq!(
            13,
            in_process.iter().flatten().count(),
            "every identity is published once, and the identity that arrived in two halves once"
        );
        for (owner, handed) in in_process.iter().enumerate() {
            // An owner publishes the identities that became ready together in the order they
            // completed, and neither path promises that order - the frame a consumer decodes is
            // read for its chains, not for a sequence of keys. So the two are compared as the same
            // series, sorted by the key that names each one.
            let from_frames = raw_frames_decoded(frame_schema(), published[owner].clone()).await;
            assert_eq!(
                join_raw(from_frames),
                join_raw(handed.clone()),
                "owner {owner} hands over the series its frames decode to"
            );
        }
        // The identity that arrived from two partitions is the merge: two halves in, one closed
        // chunk out, on whichever owner the routing put it on.
        let split = in_process
            .iter()
            .flatten()
            .filter(|chunk| chunk.key == b"web-split")
            .collect::<Vec<_>>();
        assert_eq!(
            1,
            split.len(),
            "the split identity is published once: {in_process:?}"
        );
        let mut merged_times = split[0].time.clone();
        merged_times.sort_unstable();
        assert_eq!(
            vec![60_000, 120_000],
            merged_times,
            "the two halves are one chunk: {split:?}"
        );
        assert!(split[0].is_last, "the identity the owner merged is closed");

        // And the whole composition above it: a real tree over the chunks and the same tree over
        // the frames answer the same batches.
        let over_frames = plan(
            rate_tree(),
            memory_exec_partitioned_with(frame_schema(), published),
        )
        .expect("the runtime executes a rate tree");
        let in_process =
            plan(rate_tree(), raw_router(routed_raw_partitions(), 3)).expect("the same here");
        assert!(
            uses_in_process_series(&in_process),
            "the router hands its chunks over, so the node reads them"
        );
        assert!(
            !uses_in_process_series(&over_frames),
            "a memory leaf publishes frames, so the node reads them"
        );
        // Owner by owner, because a row belongs to the owner the routing put its series on, and
        // per owner the rows are compared as the same rows: an owner answers the identities it was
        // routed in the order they completed, which is not a promise either path makes.
        let frame_rows = polled_partition_rows(&over_frames).await;
        let chunk_rows = polled_partition_rows(&in_process).await;
        assert_eq!(3, chunk_rows.len(), "the tree runs once per owner");
        for owner in 0..chunk_rows.len() {
            assert!(chunk_rows[owner].1, "owner {owner} reports its end");
            let mut answered_over_frames = frame_rows[owner].0.clone();
            answered_over_frames
                .sort_by(|left, right| left.partial_cmp(right).expect("rows compare"));
            let mut answered_over_chunks = chunk_rows[owner].0.clone();
            answered_over_chunks
                .sort_by(|left, right| left.partial_cmp(right).expect("rows compare"));
            assert_eq!(
                answered_over_frames, answered_over_chunks,
                "owner {owner} answers the same rows over the chunks and over the frames"
            );
        }
        assert!(
            chunk_rows.iter().any(|(rows, _)| !rows.is_empty()),
            "the fixture answers on the grid"
        );
    })
    .await
    .expect("the composition finishes");
}

/// A three-owner evaluated router hands over the chunks its frames decode to, owner by owner.
///
/// The same contract as the raw half, on the content a router publishes after an owner merged it: the
/// owner's merged series is what is handed over, already closed, so the `is_last` a frame carries is
/// the one the chunk carries. The identity the rewrite folded out of two partitions is one series in
/// both, and the tree above folds it to the same batches.
#[tokio::test]
async fn a_three_owner_evaluated_router_hands_over_what_its_frames_decode_to() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let publishing: Arc<dyn ExecutionPlan> = Arc::new(
            SeriesRepartitionExec::try_new(aliasing_producer(routed_evaluated_partitions()), 3)
                .expect("the router is built over the producer's evaluated frames"),
        );
        let published = published_frames(&publishing).await;
        assert_eq!(3, published.len(), "one publication per owner");
        // The router routes by a hash of the label set, so which owners carry a series is not this
        // test's to fix - only that every label set is published, on whichever owner owns it, and
        // that the chunks match the frames there. A merged series whose pieces sit on disjoint grid
        // positions is encoded as one row per dense segment, so the rows of an owner are not the
        // count of its series and are not compared against it.

        let handing: Arc<dyn ExecutionPlan> = Arc::new(
            SeriesRepartitionExec::try_new(aliasing_producer(routed_evaluated_partitions()), 3)
                .expect("the router is built over the producer's evaluated frames"),
        );
        let mut in_process = Vec::new();
        for owner in 0..3 {
            in_process.push(handed_evaluated_chunks(&handing, owner).await);
        }
        assert_eq!(
            4,
            in_process.iter().flatten().count(),
            "four canonical label sets, each published once: {in_process:?}"
        );
        for (owner, handed) in in_process.iter().enumerate() {
            // As in the raw half: the order an owner publishes in is the order its series
            // completed, so the two paths are compared as the same series rather than as one
            // sequence.
            let from_frames =
                evaluated_frames_decoded(publishing.schema(), published[owner].clone()).await;
            assert_eq!(
                join_evaluated(from_frames),
                join_evaluated(handed.clone()),
                "owner {owner} hands over the series its frames decode to"
            );
        }
        // The merged label set is one closed series carrying both pieces, which is the `is_last` the
        // frame declares for it as well.
        let merged = in_process
            .iter()
            .flatten()
            .filter(|series| series.metric.get(HOST).is_some_and(|host| host == ALIASED))
            .collect::<Vec<_>>();
        assert_eq!(1, merged.len(), "the aliased label set is published once");
        assert!(
            merged[0].is_last,
            "an owner publishes a merged identity closed"
        );
        assert!(
            merged[0].positions.len() > 1,
            "both pieces are in the one series: {:?}",
            merged[0].positions
        );

        let over_frames =
            summed_window_over(memory_exec_partitioned_with(publishing.schema(), published));
        let in_process = summed_window_over(Arc::new(
            SeriesRepartitionExec::try_new(aliasing_producer(routed_evaluated_partitions()), 3)
                .expect("the router is built over the producer's evaluated frames"),
        ) as Arc<dyn ExecutionPlan>);
        assert!(
            uses_in_process_series(&in_process),
            "the router hands its chunks over, so the node reads them"
        );
        assert!(
            !uses_in_process_series(&over_frames),
            "a memory leaf publishes frames, so the node reads them"
        );
        let frame_rows = polled_partition_rows(&over_frames).await;
        let chunk_rows = polled_partition_rows(&in_process).await;
        assert_eq!(3, chunk_rows.len(), "the tree runs once per owner");
        for owner in 0..chunk_rows.len() {
            assert!(chunk_rows[owner].1, "owner {owner} reports its end");
            let mut answered_over_frames = frame_rows[owner].0.clone();
            answered_over_frames
                .sort_by(|left, right| left.partial_cmp(right).expect("rows compare"));
            let mut answered_over_chunks = chunk_rows[owner].0.clone();
            answered_over_chunks
                .sort_by(|left, right| left.partial_cmp(right).expect("rows compare"));
            assert_eq!(
                answered_over_frames, answered_over_chunks,
                "owner {owner} answers the same rows over the chunks and over the frames"
            );
        }
        assert!(
            chunk_rows.iter().any(|(rows, _)| !rows.is_empty()),
            "the fixture answers on the grid"
        );
    })
    .await
    .expect("the composition finishes");
}

/// A state input is read in process, like every other content a native operator publishes.
///
/// `SeriesChunk::State` is the exported row itself, not a second encoding of it, so the node folds
/// the row its operator hands over instead of decoding a frame on a state path of its own.
#[test]
fn a_state_input_is_read_in_process() {
    let router: Arc<dyn ExecutionPlan> = Arc::new(
        SeriesRepartitionExec::try_new(state_producer(), 2)
            .expect("the producer declares aggregate-state frames"),
    );
    assert!(
        series_execution_plan(router.as_ref()).is_some(),
        "a state publication is a row this crate holds, so it is handed over"
    );
    let fused = final_state_over(router);
    assert!(
        uses_in_process_series(&fused),
        "the state input is handed over, not decoded from a frame"
    );
}

// ---------------------------------------------------------------------------
// Aggregate state over the same handover
// ---------------------------------------------------------------------------

/// One exported state row as these tests compare it.
///
/// Which group it belongs to, where it has contributions, and whether it closes its chain: the
/// payload is compared where it is read, by the answer the two paths fold.
#[derive(Clone, Debug, PartialEq)]
struct DecodedState {
    metric: SeriesLabels,
    runs: Vec<(usize, usize)>,
    is_last: bool,
}

/// `row` as these tests compare it.
fn decoded_state(row: &StateFrameRow) -> DecodedState {
    DecodedState {
        metric: row.metric().clone(),
        runs: row
            .time()
            .runs()
            .map(|run| (run.position, run.len))
            .collect(),
        is_last: row.is_last(),
    }
}

/// `rows` in a fixed order, so two lists of them can be compared as sets.
fn state_rows_sorted(mut rows: Vec<DecodedState>) -> Vec<DecodedState> {
    rows.sort_by_key(|row| format!("{:?}|{:?}", row.metric, row.runs));
    rows
}

/// A real `Partial` producer over two partitions, both carrying the same group.
///
/// Grouping by `job` puts both partitions' state in one group, so the router routes both rows to the
/// owner that folds them and the `Final` above has to merge what it is given: that merge is what
/// this differential is about.
fn state_producer() -> Arc<dyn ExecutionPlan> {
    let rows = series_samples(1.0);
    let first = [(HOST, "web-0"), (JOB, "shared")];
    let second = [(HOST, "web-1"), (JOB, "shared")];
    let partitions = vec![
        frames(&[labelled_chunk("web-0", &first, &rows[..4], true)]),
        vec![frame(&labelled_chunk("web-1", &second, &rows, true))],
    ];
    partial_plan(
        rate_tree(),
        memory_exec_partitioned(partitions),
        partial_op(AggregateKind::Sum, &[JOB]),
        &[JOB],
    )
    .expect("a rate tree with a partial sum is executable")
}

/// A real `Final` fold over `input`, answering rows: the aggregate that reads exported state.
fn final_state_over(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let state = Arc::new(AggregateStateSchema {
        group: Grouping::By(Arc::new([Arc::from(JOB)])),
        kind: AggregateStateKind::Sum,
        components: Arc::from([StateComponent::Value, StateComponent::Compensation]),
        accumulators: raw_schema().fields.clone(),
        grid: grid(),
    });
    let tree = state_tree(AggregateStage::Final, NativeType::AggregateState(state));
    plan_of(fused(tree), input).expect("the runtime executes a final fold")
}

/// The state rows `owner` of `plan` hands over, as these tests compare them.
async fn handed_state_rows(plan: &Arc<dyn ExecutionPlan>, owner: usize) -> Vec<DecodedState> {
    let handed = series_execution_plan(plan.as_ref())
        .expect("the router hands its rows over")
        .execute_series(owner, SessionContext::default().task_ctx())
        .expect("the owner hands its rows over")
        .try_collect::<Vec<_>>()
        .await
        .expect("the rows arrive");
    handed
        .into_iter()
        .map(|handed| match handed.into_parts().0 {
            SeriesChunk::State(chunk) => match chunk {
                StateChunk::Series { series, .. } => {
                    let series = series.first().expect("one group per handed chunk");
                    DecodedState {
                        metric: series.metric.clone(),
                        runs: series
                            .time
                            .runs()
                            .map(|run| (run.position, run.len))
                            .collect(),
                        // A handed-over group is a whole publication: the producer folded it
                        // completely, which is what the frame row it would have been says too.
                        is_last: true,
                    }
                }
                StateChunk::Row(row) => DecodedState {
                    metric: row.metric.clone(),
                    runs: row.time.runs().map(|run| (run.position, run.len)).collect(),
                    is_last: true,
                },
            },
            other => panic!("a state owner handed over {other:?}"),
        })
        .collect()
}

/// The state rows `batches` decode to, as a consumer behind the boundary recovers them.
async fn state_rows_decoded(schema: SchemaRef, batches: Vec<RecordBatch>) -> Vec<DecodedState> {
    let declaration =
        SeriesFrameDeclaration::parse(schema.as_ref()).expect("the export declares a series frame");
    let state = declaration
        .aggregate_state()
        .expect("the export declares aggregate state");
    let binding = Arc::new(
        datatypes::series::aggregate_state::AggregateStateSchema::try_new(
            state.kind,
            state.components.to_vec(),
        )
        .expect("the declared layout is legal"),
    );
    let series = Arc::new(declaration.series().clone());
    let leaf = memory_exec_partitioned_with(schema, vec![batches]);
    let frames = leaf
        .execute(0, SessionContext::default().task_ctx())
        .expect("the export's frames are re-read");
    unpack_aggregate_state(frames, Some(binding), Some(series))
        .expect("the export decodes")
        .try_collect::<Vec<_>>()
        .await
        .expect("the rows decode")
        .iter()
        .map(decoded_state)
        .collect()
}

/// A state publication is one publication read two ways: the rows have to be the same, and so does
/// the fold above them.
///
/// The rows are compared per owner - the group they name, the grid positions they carry and their
/// finality, labels included - and the fold is compared per owner as the rows it answers, because
/// that is where the payload is read.
#[tokio::test]
async fn a_state_router_hands_over_the_rows_its_frames_decode_to() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        // The frame side: one router's export, read owner by owner, which is both what a boundary
        // consumer decodes and the fixture the frame path below is given.
        let publishing: Arc<dyn ExecutionPlan> = Arc::new(
            SeriesRepartitionExec::try_new(state_producer(), 2)
                .expect("the producer declares aggregate-state frames"),
        );
        let published = published_frames(&publishing).await;
        assert_eq!(2, published.len(), "one publication per owner");

        // The chunk side reads the same fixture through a router of its own.
        let handing: Arc<dyn ExecutionPlan> = Arc::new(
            SeriesRepartitionExec::try_new(state_producer(), 2)
                .expect("the producer declares aggregate-state frames"),
        );
        let mut in_process = Vec::new();
        for owner in 0..2 {
            in_process.push(handed_state_rows(&handing, owner).await);
        }
        let handed = in_process.iter().flatten().count();
        assert!(handed > 0, "the producer exports at least one group");
        for (owner, handed) in in_process.iter().enumerate() {
            let from_frames =
                state_rows_decoded(publishing.schema(), published[owner].clone()).await;
            assert_eq!(
                state_rows_sorted(from_frames),
                state_rows_sorted(handed.clone()),
                "owner {owner} hands over the rows its frames decode to"
            );
        }
        // Every exported row closes its chain: a partial producer exports a group once.
        assert!(
            in_process.iter().flatten().all(|row| row.is_last),
            "the export closes what it publishes: {in_process:?}"
        );

        // And the fold above: the same tree over the rows and over the frames answers the same rows
        // per owner, which is where the payload itself is read.
        let over_frames =
            final_state_over(memory_exec_partitioned_with(publishing.schema(), published));
        let in_process = final_state_over(Arc::new(
            SeriesRepartitionExec::try_new(state_producer(), 2)
                .expect("the producer declares aggregate-state frames"),
        ) as Arc<dyn ExecutionPlan>);
        assert!(
            uses_in_process_series(&in_process),
            "a state input is handed over like any other content"
        );
        // And so is the framed fixture: a `Final` normalizes every state input onto a router, and a
        // router now hands its rows over whatever its content, so there is no state consumer left
        // that decodes a frame here. The frame branch of `execute` stays for the one state producer
        // this crate does not own - a merge scan reading another process's frames - which a
        // single-process fixture cannot build, so this comparison runs through the frames the
        // producer published and the rows they decode back to.
        assert!(
            uses_in_process_series(&over_frames),
            "a state input is normalized onto a router, which hands its rows over"
        );
        let frame_rows = polled_partition_rows(&over_frames).await;
        let chunk_rows = polled_partition_rows(&in_process).await;
        assert_eq!(2, chunk_rows.len(), "the fold runs once per owner");
        for owner in 0..chunk_rows.len() {
            assert!(chunk_rows[owner].1, "owner {owner} reports its end");
            let mut answered_over_frames = frame_rows[owner].0.clone();
            answered_over_frames
                .sort_by(|left, right| left.partial_cmp(right).expect("rows compare"));
            let mut answered_over_chunks = chunk_rows[owner].0.clone();
            answered_over_chunks
                .sort_by(|left, right| left.partial_cmp(right).expect("rows compare"));
            assert_eq!(
                answered_over_frames, answered_over_chunks,
                "owner {owner} folds the same answer over the rows and over the frames"
            );
        }
        assert!(
            chunk_rows.iter().any(|(rows, _)| !rows.is_empty()),
            "the fixture folds something"
        );
    })
    .await
    .expect("the composition finishes");
}

/// A state chunk's charge is released with the chunk, exactly as an evaluated one's is.
///
/// The state frame path keeps the router's reservation until the poll after the batch it backed, so
/// `Final` can admit what it kept of it. A handed-over row has no batch to flush and no next poll to
/// wait for: the reservation travels with the row, which is the same moment - the row is admitted by
/// the push that reads it, and the charge goes when the value it accounts for goes.
#[tokio::test]
async fn a_handed_state_chunk_holds_its_charge_until_it_is_dropped() {
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let context = task_context(&pool);
    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        SeriesRepartitionExec::try_new(state_producer(), 2)
            .expect("the producer declares aggregate-state frames"),
    );
    let mut chunks = series_execution_plan(plan.as_ref())
        .expect("the router hands its rows over")
        .execute_series(0, Arc::clone(&context))
        .expect("the output is claimed once");

    let first = futures::future::poll_fn(|cx| chunks.as_mut().poll_next(cx))
        .await
        .expect("the producer exports a row")
        .expect("the row arrives");
    assert!(
        matches!(first.chunk(), SeriesChunk::State(_)),
        "a state owner hands over state rows"
    );
    let held = pool.reserved();
    assert!(
        held > 0,
        "a held state row keeps the export it points into charged"
    );

    drop(first);
    let after = pool.reserved();
    assert!(
        after < held,
        "dropping the state row releases what it was charged: {held} held, {after} still reserved"
    );

    // The rest of the export, then the rows themselves: nothing is left charged once both are gone.
    let mut rest = chunks
        .by_ref()
        .try_collect::<Vec<_>>()
        .await
        .expect("the export ends instead of failing");
    rest.clear();
    drop(chunks);
    // The other owner of this router is never claimed, so the read it belongs to ends with the plan.
    drop(plan);
    assert_eq!(0, pool.reserved(), "the finished export holds nothing");
}

/// Whether `plan` is answered by the guard.
fn guard_answers(plan: &Arc<dyn ExecutionPlan>) -> bool {
    series_execution_plan(plan.as_ref()).is_some()
}

/// A frame the guard answers for, and the row table it does not.
///
/// A `SeriesFrame` - of values, or of the state a `Partial` folded - is a publication another native
/// operator reads in process. The row table a statement is answered with is the query's own answer,
/// and only `execute` hands that out, so the guard says no and no consumer above reads a chunk.
#[test]
fn the_guard_answers_for_frames_and_not_for_rows() {
    let values = aliasing_producer(routed_evaluated_partitions());
    assert!(
        guard_answers(&values),
        "a node publishing a frame of values hands its chunks over"
    );

    let state = state_producer();
    assert!(
        guard_answers(&state),
        "the state a Partial folds is handed over as the chunk it is"
    );

    let rows = plan_of(
        fused(rate_tree()),
        memory_exec(frames(&[series_chunk("web-0", &series_samples(1.0), true)])),
    )
    .expect("the runtime executes a rate tree");
    assert!(
        !guard_answers(&rows),
        "a node answering rows is the query's answer, not a chunk"
    );
}

/// A fused node above another reads its chunks, and answers what the frames would have.
///
/// The same tree over the same publication: `over_chunks` reads the node below in process, and
/// `over_frames` decodes what that node publishes for a boundary consumer. The answer has to be the
/// same batch for batch, which is what makes the handover a saving rather than a second semantics.
#[tokio::test]
async fn a_fused_node_hands_over_what_the_frames_above_it_would_decode() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let context = SessionContext::default().task_ctx();
        // What the node below publishes, taken from the node itself so both plans read one input.
        let lower = aliasing_producer(routed_evaluated_partitions());
        let published = published_frames(&lower).await;
        let lower_frames = memory_exec_partitioned_with(lower.schema(), published);

        let over_chunks = summed_window_over(aliasing_producer(routed_evaluated_partitions()));
        let over_frames = summed_window_over(lower_frames);
        assert!(
            uses_in_process_series(&over_chunks),
            "the node below is one of ours, so its chunks are read"
        );
        assert!(
            !uses_in_process_series(&over_frames),
            "a memory leaf publishes frames, so the node above decodes them"
        );

        let chunk_batches = collect(Arc::clone(&over_chunks), Arc::clone(&context))
            .await
            .expect("the chunk path completes");
        let frame_batches = collect(Arc::clone(&over_frames), context)
            .await
            .expect("the frame path completes");
        assert!(!chunk_batches.is_empty(), "the fixture answers on the grid");
        assert_eq!(
            frame_batches, chunk_batches,
            "the two paths answer the same batches"
        );
    })
    .await
    .expect("the composition finishes");
}

// ---------------------------------------------------------------------------
// The handover's contract, held to the same standard as a frame
// ---------------------------------------------------------------------------

/// A child's chunks are checked at the order its own plan declares, exactly as its frames are.
///
/// The producer here names its keys by an `Interleaved` promise and then closes the same key twice.
/// Its frames cannot carry that - it publishes them under the contiguous order it declares, and the
/// packer refuses the recurrence - so a consumer reading its chunks has to refuse it too: the
/// handover is a way to read a producer, not a way around what it promised.
#[tokio::test]
async fn a_router_refuses_the_chunk_sequence_the_frames_refuse() {
    let closed = |time: i64, last: bool| {
        chunk_of(
            &raw_schema(),
            "web-1",
            metric(&[(HOST, "web-1")], Some(METRIC)),
            &[time],
            vec![vec![Some(1.0)]],
            last,
        )
    };
    // Two closed chunks of one key, which the contiguous order of the node above forbids.
    let batches = || {
        vec![frames_under(
            &frame_schema_with_ordering(SeriesOrdering::Interleaved),
            &[closed(60_000, true), closed(120_000, true)],
        )]
    };
    let context = SessionContext::default().task_ctx();
    let refused = |error: BoxedError| {
        error
            .to_string()
            .contains("raw series key recurred after final chunk")
    };

    // The frame path: the producer cannot pack this under the order it publishes.
    let source = raw_source(memory_exec_partitioned_with(
        frame_schema_with_ordering(SeriesOrdering::Interleaved),
        batches(),
    ));
    let mut frames = source
        .execute(0, Arc::clone(&context))
        .expect("the node executes");
    let error = frames
        .next()
        .await
        .expect("the producer answers instead of ending")
        .expect_err("the frame path refuses a key that recurs after it closed");
    assert!(
        refused(BoxedError::new(common_query::error::Error::from(error))),
        "the frame path refuses a key that recurs after it closed"
    );

    // The chunk path: read through a router, which asks the child for its chunks.
    let router: Arc<dyn ExecutionPlan> = Arc::new(
        SeriesRepartitionExec::try_new(
            raw_source(memory_exec_partitioned_with(
                frame_schema_with_ordering(SeriesOrdering::Interleaved),
                batches(),
            )),
            1,
        )
        .expect("the child declares a raw frame"),
    );
    let chunks = series_execution_plan(router.as_ref())
        .expect("the router hands its publication over")
        .execute_series(0, context)
        .expect("the output is claimed");
    let error = chunks
        .try_collect::<Vec<_>>()
        .await
        .expect_err("the chunk path refuses what the frame path refuses");
    let message = error.to_string();
    assert!(refused(error), "{message}");
}

/// A state group is charged for as long as the reader holds it, with no router in between.
///
/// The tree's own export carries the fold's reservation; handing the groups of that export over has
/// to carry it too, or a reader of a `Partial` holds uncharged bytes between the fold and its own
/// accounting.
#[tokio::test]
async fn a_handed_state_group_keeps_the_export_charged() {
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let context = task_context(&pool);
    let producer = state_producer();
    let mut chunks = series_execution_plan(producer.as_ref())
        .expect("a Partial hands its state over")
        .execute_series(0, Arc::clone(&context))
        .expect("the output is claimed");

    let group = futures::future::poll_fn(|cx| chunks.as_mut().poll_next(cx))
        .await
        .expect("the partial exports a group")
        .expect("the group arrives");
    assert!(
        matches!(group.chunk(), SeriesChunk::State(StateChunk::Series { .. })),
        "a partial hands its folded groups over"
    );
    let held = pool.reserved();
    assert!(
        held > 0,
        "a held state group keeps the export it was folded in charged"
    );

    drop(group);
    let after = pool.reserved();
    assert!(
        after < held,
        "dropping the group gives its charge back: {held} held, {after} still reserved"
    );

    drop(futures::future::poll_fn(|cx| chunks.as_mut().poll_next(cx)).await);
    drop(chunks);
    drop(producer);
    assert_eq!(0, pool.reserved(), "the finished export holds nothing");
}

/// A state export whose first partition folds two groups, so a reader can keep one and read on.
fn two_group_state_producer() -> Arc<dyn ExecutionPlan> {
    let rows = series_samples(1.0);
    let first = [(HOST, "web-0"), (JOB, "first")];
    let second = [(HOST, "web-1"), (JOB, "second")];
    let partitions = vec![frames(&[
        labelled_chunk("web-0", &first, &rows[..4], true),
        labelled_chunk("web-1", &second, &rows[..4], true),
    ])];
    partial_plan(
        rate_tree(),
        memory_exec_partitioned(partitions),
        partial_op(AggregateKind::Sum, &[JOB]),
        &[JOB],
    )
    .expect("a rate tree with a partial sum is executable")
}

/// A group a reader keeps stays charged after the export that produced it is gone.
///
/// The reviewer's case, and the one the sibling above does not cover: a consumer polls a group, keeps
/// it, and then exhausts or drops the export. The reservation the export carried has to travel with
/// the group rather than stay in the stream, or reading to the end - or letting the stream go - would
/// release bytes the reader is still holding, and the pool would hand them to somebody else.
///
/// The figures are read once the export and its producer are gone, so what the pool counts is the
/// group's alone; the second figure is what that group holds by itself, so a reservation released
/// with the stream cannot pass on the strength of the group's own bytes.
#[tokio::test]
async fn a_held_state_group_outlives_the_export_that_produced_it() {
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let context = task_context(&pool);
    let producer = two_group_state_producer();
    let mut chunks = series_execution_plan(producer.as_ref())
        .expect("a Partial hands its state over")
        .execute_series(0, Arc::clone(&context))
        .expect("the output is claimed");

    let kept = futures::future::poll_fn(|cx| chunks.as_mut().poll_next(cx))
        .await
        .expect("the partial exports a group")
        .expect("the group arrives");
    // The export's other group, read and dropped: the reservation the two share is the kept group's
    // to hold, so letting the sibling go must not release it.
    let sibling = futures::future::poll_fn(|cx| chunks.as_mut().poll_next(cx))
        .await
        .expect("the partial exports its second group")
        .expect("the group arrives");
    let held = pool.reserved();
    drop(sibling);
    assert!(held > 0, "a held state group keeps its export charged");

    // Drain the rest of the export and drop everything but the group: the stream, the rows it
    // handed over and the plan that produced them.
    let rest = chunks
        .by_ref()
        .try_collect::<Vec<_>>()
        .await
        .expect("the export ends instead of failing");
    drop(rest);
    drop(chunks);
    drop(producer);
    let after = pool.reserved();
    let group_bytes = match kept.chunk() {
        SeriesChunk::State(chunk) => state_bytes(chunk),
        other => panic!("a partial hands its groups over, not {other:?}"),
    };
    assert!(
        after >= group_bytes,
        "the group alone keeps its export charged: {held} held while it was read, {after} still \
         reserved once the export and its producer are gone, and the group alone holds {group_bytes}"
    );
    assert!(
        after <= held,
        "nothing the export held is left besides the reservation the group keeps: {after} reserved, \
         {held} while the export was being read"
    );

    drop(kept);
    assert_eq!(
        0,
        pool.reserved(),
        "dropping the last group of a finished export releases everything"
    );
}

/// The two entry points cannot be mixed on one execution, and the refusal names both.
///
/// The child is read once for every output of one execution, so the way it is read is fixed by the
/// first output that claims it. A later output asking for the other form would be handed a stream it
/// did not ask for; it is refused instead, naming what the read already is and what it was asked to
/// become.
#[tokio::test]
async fn the_two_entry_points_cannot_be_mixed_on_one_execution() {
    let context = SessionContext::default().task_ctx();
    let router: Arc<dyn ExecutionPlan> = Arc::new(
        SeriesRepartitionExec::try_new(
            raw_source(memory_exec_partitioned(vec![frames(&[series_chunk(
                "web-0",
                &series_samples(1.0),
                true,
            )])])),
            2,
        )
        .expect("the child declares a raw frame"),
    );
    router
        .execute(0, Arc::clone(&context))
        .expect("the first output reads frames");
    let claimed = series_execution_plan(router.as_ref())
        .expect("the router hands its publication over")
        .execute_series(1, context);
    // The refusal is an error, and naming it is what the two assertions below are about.
    let Err(error) = claimed else {
        panic!("a second output cannot read the same execution as chunks");
    };
    let message = error.to_string();
    assert!(message.contains("already read as frames"), "{message}");
    assert!(message.contains("cannot read it as chunks"), "{message}");
}
