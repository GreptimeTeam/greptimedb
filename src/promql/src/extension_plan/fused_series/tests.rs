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

use common_query::series_frame::{pack_evaluated_series, unpack_evaluated_series};
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
use datatypes::series::aggregate_state::{
    AggregateStateKind, StateComponent, StatePayload, StateSeries,
};
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
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
use store_api::region_engine::{BoundTag, RawLabelSource};
use store_api::storage::RegionId;

mod frame_output;

use super::{
    FieldKind, FusedOutputFormat, FusedSeries, FusedSeriesBinding, OutputColumn, POLL_BUDGET,
    series_to_record_batch,
};
use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::{SeriesRepartitionExec, SeriesRouting};
use crate::histogram_eval::HistogramFunction;
use crate::map_label::LabelRewrite;
use crate::native::execute::{InputId, NativeExecutionState, NativeExecutor};
use crate::native::plan::{
    AggregateKind, AggregateStage, AggregateStateSchema, BoundAggregate, BoundInput, BoundInstant,
    BoundMap, BoundScalar, BoundVectorOp, BoundWindow, Grouping, InstantValue, MapOp, NativeExpr,
    NativeNode, NativeTree, NativeType,
};
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};
use common_query::series_frame::series_frame_ordering;
use common_query::series_frame::unpack_aggregate_state;

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
            | NativeExpr::VectorOf { input } => {
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

fn partition_disjoint(tree: NativeTree, input: Arc<dyn ExecutionPlan>) -> bool {
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
        .to_execution_plan(vec![(
            input,
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
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
        !super::preserves_partition_identity(binary_tree(raw_schema(), raw_schema()).root()),
        "two Input leaves sharing source 0 are still a Binary tree"
    );
}

#[test]
fn partition_disjoint_requires_proven_input() {
    let sample = NativeTree::try_new(instant_of(0, &grid())).unwrap();
    assert!(!partition_disjoint(sample.clone(), memory_exec(vec![])));
    assert!(super::preserves_partition_identity(sample.root()));

    let scalar = scalar_tree(grid(), 1.0);
    assert!(!super::preserves_partition_identity(scalar.root()));
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

    // A state-exporting producer never crosses the wire: the frame's state content has no wire
    // form, so the producer stays on the machine that reads the samples.
    let partial = partial_node(AggregateKind::Sum);
    assert!(
        partial
            .serialize()
            .unwrap_err()
            .to_string()
            .contains("exporting aggregate state"),
        "a state-exporting producer must be refused by the serializer"
    );
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

/// A payload written under another version is refused by its version, before its body is read.
///
/// The body here is the current payload's body with the version field set to a number this reader
/// does not accept: it decodes cleanly under the current reader, which is exactly what makes it a
/// control. A reader that checked the version only after decoding would accept it and hand back a
/// node whose fields were read at the wrong offsets; this asserts the version is what answers, by
/// name.
#[test]
fn fused_series_envelope_refuses_the_older_payload_version_before_reading_it() {
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

    let mut old = bytes.clone();
    old[..4].copy_from_slice(&2u32.to_le_bytes());
    let message = FusedSeries::deserialize(&old).unwrap_err().to_string();
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
                Field::new(HOST, DataType::Float64, true),
                Field::new(NAME, DataType::Utf8, true),
            ]),
            "must be Utf8",
        ),
    ];
    for (schema, expected) in malformed_schemas {
        assert_decode_error(&mutate_schema(&bytes, schema), expected);
    }

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
        message.contains("frame content does not match its compiled input"),
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
        crate::native::plan::aggregate_state_binding(kind).expect("mergeable aggregate");
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
async fn decode_state_batches(batches: Vec<RecordBatch>) -> Vec<StateSeries> {
    let schema = batches
        .first()
        .expect("a state-exporting node answers at least one batch")
        .schema();
    let declaration = SeriesFrameDeclaration::parse(schema.as_ref()).unwrap();
    assert_eq!(FrameContent::AggregateState, declaration.content());
    let series = Arc::new(declaration.series().clone());
    let state = Arc::new(declaration.aggregate_state().unwrap().to_owned());
    let decoded = unpack_aggregate_state(
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
    .unwrap();
    decoded
        .iter()
        .map(|batch| batch.to_state_series().unwrap())
        .collect()
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
        .metric
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
        let batch = decoded[0].to_state_series().unwrap();
        assert_eq!(
            Some("shared"),
            batch
                .metric
                .tags()
                .iter()
                .find(|tag| tag.name.as_ref() == JOB)
                .map(|tag| tag.value.as_ref())
        );
        assert_eq!(GRID_LEN as usize, batch.time.len());
        assert_eq!(1, batch.states.len());
        assert!(batch.states[0].iter().all(Option::is_some));
        assert_eq!(AggregateStateKind::Count, binding.kind);
        assert_eq!(vec![StateComponent::Count], binding.components.as_ref());
    }
}

/// A state export releases everything it held: when drained, and when the consumer drops it early.
///
/// This is lifecycle coverage, not the regression for the producer hand-off: other reservations are
/// alive at the same moment, so a positive figure here would not by itself prove the batch is the
/// charged one. `a_handed_over_state_chunk_keeps_its_charge_until_it_is_dropped` owns that claim.
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
            (0..batch.time.len())
                .map(|position| {
                    batch
                        .states
                        .first()
                        .and_then(|field| field.get(position))
                        .and_then(|state| state.as_ref())
                        .and_then(StatePayload::float)
                        .map(|state| state.value)
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
                display.lines().next().unwrap().ends_with(
                    " (state routing: canonical labels except [\"le\", \"__name__\", \"__type__\", \"__unit__\"])"
                ),
                "{display}"
            );
        }
    }
}

#[test]
fn histogram_split_declines_only_rewrites_that_change_the_routing_key() {
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
                    (
                        state.metric.get("le").unwrap().to_string(),
                        state.states[0][0]
                            .as_ref()
                            .and_then(StatePayload::float)
                            .unwrap()
                            .value,
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
