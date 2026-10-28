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

//! What the insertion pass has to get right: it inserts a merge only where a consumer needs one,
//! the tree it produces is one its own validator accepts, and the facts it derives are the ones the
//! nodes below it actually preserve.
//!
//! The trees here are built node by node, over the one-field schemas of the `rate(x[5m])` fixture:
//! a raw scan, the window that folds it onto a grid, and - for `info` - the instant selection that
//! reports each info sample's own time. The compiler lives in the `query` crate, which depends on
//! this one, so a test in this crate cannot call it.
//!
//! Each test is named for the consumer it pins, because the whole point of the pass is that the
//! consumers are decided one at a time: a future change that re-generalizes them has to change a
//! test that says which kernel behaviour it would be overriding.

use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datatypes::arrow::array::Float64Array;
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{SeriesLabels, SeriesOrdering};

use super::insert_native_merge_inside_current_fused;
use crate::evaluated::EvaluatedChunk;
use crate::histogram_eval::HistogramFunction;
use crate::info::InfoSelector;
use crate::map::MathOp;
use crate::native::execute::{NativeExecutionState, NativeExecutor};
use crate::native::plan::{
    AggregateKind, AggregateStage, AggregateStateSchema, BoundAggregate, BoundInput, BoundInstant,
    BoundMap, BoundScalar, BoundVectorOp, BoundWindow, CanonicalGrouping, CanonicalOwner, Grouping,
    InstantValue, MapOp, NativeExpr, NativeNode, NativeProperties, NativeTree, NativeType,
};
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::select::SortOrder;
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};

fn grid(start: i64, step: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start, step, len).unwrap()).unwrap())
}

fn fields(count: usize) -> Arc<[FieldSpec]> {
    (0..count)
        .map(|index| FieldSpec {
            name: format!("v{index}").into(),
        })
        .collect()
}

fn evaluated(schema_fields: Arc<[FieldSpec]>, grid: Arc<EvalGrid>) -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: schema_fields,
        eval_grid: Some(grid),
    }))
}

/// A scan of raw samples, the leaf every tree here is built over.
fn raw_input(source_index: usize) -> NativeNode {
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: fields(1),
        eval_grid: None,
    }));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// A leaf that hands over samples already evaluated on `out`.
///
/// This is the boundary a tree split at a repartition starts from: Fused B's `Input` in
/// `A -> Repartition -> B`, whose fragments are the pieces of one canonical label set that the
/// repartition fanned out. It is also the one leaf the runtime does *not* collect for a window
/// above it, which is what the tests built over it are about.
fn evaluated_input(source_index: usize, out: &Arc<EvalGrid>) -> NativeNode {
    let output = evaluated(fields(1), Arc::clone(out));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// A constant, which a binary operation broadcasts.
fn scalar(value: f64, out: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(out),
    }
}

/// `rate(x[5m])` over `inner`, a window that folds it onto `out` and drops the metric name.
///
/// `input_grid` is the grid `inner` is evaluated on, which is `None` for raw samples: the validator
/// compares it against what the input actually produces.
fn window_on(
    inner: NativeNode,
    input_grid: Option<Arc<EvalGrid>>,
    out: Arc<EvalGrid>,
) -> NativeNode {
    window_function_on(RangeFunction::Rate, inner, input_grid, out)
}

/// `function(x[5m])` over `inner`, a window on `out` whose input is evaluated on `input_grid`.
fn window_function_on(
    function: RangeFunction,
    inner: NativeNode,
    input_grid: Option<Arc<EvalGrid>>,
    out: Arc<EvalGrid>,
) -> NativeNode {
    window_sized_on(function, inner, input_grid, out, 300_000)
}

/// [`window_function_on`] over a window of `range_ms`, which is what decides how many output points
/// one contribution decides.
///
/// A range that spans several input steps makes every contribution decide every output point it
/// reaches, so two contributions of one label set collide at each of them and the window refuses the
/// second. A range that holds one input step makes each contribution decide its own output point, so
/// the same two contributions are two *series* instead - which is the gap these tests measure.
fn window_sized_on(
    function: RangeFunction,
    inner: NativeNode,
    input_grid: Option<Arc<EvalGrid>>,
    out: Arc<EvalGrid>,
    range_ms: i64,
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
                    modifier: None,
                },
                input_grid,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(1), out),
    }
}

/// [`window_on`] over raw samples, which is `rate(x[5m])`.
fn window_over(inner: NativeNode, out: Arc<EvalGrid>) -> NativeNode {
    window_on(inner, None, out)
}

/// `abs(inner)`: a point-wise map, which also drops the metric name.
fn map_over(inner: NativeNode) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap {
                ops: Arc::from([MapOp::Math(MathOp::Abs)]),
            },
        },
        output,
    }
}

/// `sum without() (inner)` at `stage`, which folds every series it is handed into one group.
fn aggregate_over(inner: NativeNode, out: Arc<EvalGrid>, stage: AggregateStage) -> NativeNode {
    let output = match stage {
        AggregateStage::Partial => NativeType::AggregateState(Arc::new(AggregateStateSchema {
            group: Grouping::By(Arc::from([])),
            accumulators: fields(1),
            grid: Arc::clone(&out),
        })),
        AggregateStage::Complete | AggregateStage::Final => evaluated(fields(1), out),
    };
    NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::from([])),
                stage,
            },
        },
        output,
    }
}

/// A vector operation over `inner`, laid out as `inner` is.
fn vector_over(inner: NativeNode, op: BoundVectorOp) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(inner),
            op,
        },
        output,
    }
}

/// An `adding` match with no `on`/`ignoring`, which is `x + y`.
fn add() -> BinaryMatching {
    BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    }
}

/// A binary operation over `left` and `right` on `out`, which is one value per input field.
fn binary_over(left: NativeNode, right: NativeNode, out: Arc<EvalGrid>) -> NativeNode {
    binary_matched(left, right, out, add())
}

/// A binary operation over `left` and `right` on `out` with `matching`, one value per input field.
fn binary_matched(
    left: NativeNode,
    right: NativeNode,
    out: Arc<EvalGrid>,
    matching: BinaryMatching,
) -> NativeNode {
    let output = evaluated(fields(1), out);
    NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching,
        },
        output,
    }
}

/// A comparison with no `bool`, which is a **filter**: `x > y` keeps the label set it read.
///
/// This is the one binary modifier that does not drop the metric name (`broadcast_drops_labels`),
/// so a broadcast bound this way reaches the vector side's series without rewriting their labels.
fn filter() -> BinaryMatching {
    BinaryMatching {
        op: BinaryOp::Gt,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    }
}

/// `timestamp(inner)`, which reports a position under the metric name dropped.
fn timestamp_over(inner: NativeNode) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Timestamp {
            input: Box::new(inner),
        },
        output,
    }
}

/// `Merge(inner)`: the canonical collection, placed by hand as the pass would place it.
fn merge_over(inner: NativeNode) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(inner),
        },
        output,
    }
}

/// An instant selection over `inner` that reports what `value` says it reports, on `out`.
fn selection(inner: NativeNode, out: Arc<EvalGrid>, value: InstantValue) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(inner),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(1), out),
    }
}

/// `info(base, {})`: the base series with the data labels of the info series that share theirs.
fn info_over(base: NativeNode, info: NativeNode) -> NativeNode {
    let output = base.output.clone();
    NativeNode {
        expr: NativeExpr::Info {
            base: Box::new(base),
            info: Box::new(info),
            selector: InfoSelector {
                name_matchers: Vec::new(),
                data_labels: Vec::new(),
                keep_unmatched: true,
            },
        },
        output,
    }
}

/// The node for the info side of [`info_over`]: the instant selection that reports sample times.
fn info_selection(out: &Arc<EvalGrid>) -> NativeNode {
    selection(raw_input(1), Arc::clone(out), InstantValue::InfoTimestamp)
}

/// `window_fn(x[5m])` over the scan, a window that folds raw samples onto `out`.
fn window_of(function: RangeFunction, out: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(raw_input(0)),
            op: BoundWindow {
                function,
                window: RangeWindow {
                    range_ms: 300_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: None,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(1), out),
    }
}

/// Runs `root` over one scan and reads its answer as the rows of its one field, or the error the
/// execution reported.
///
/// `fragments` are `(timestamp, value, is_last)` and are all pushed to the tree's one input under
/// one storage key, which is how one series arriving as several chunks looks: a chunk that does not
/// close the series leaves it open for the next one.
fn runtime_rows(
    root: NativeNode,
    fragments: &[(i64, f64, bool)],
) -> Result<Vec<(usize, f64)>, String> {
    let schema = SeriesSchema {
        fields: fields(1),
        eval_grid: None,
    };
    let metric = SeriesLabels::try_new(Some("x".into()), Vec::new()).expect("a metric name");
    let tree = NativeTree::try_new(root).expect("the fixture is a legal tree");
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let state =
        NativeExecutionState::try_new(&tree, Arc::clone(&pool)).expect("the runtime runs the tree");
    let ids = state.input_ids(0).collect::<Vec<_>>();
    let mut executor = NativeExecutor::new(state);
    for &(timestamp, value, is_last) in fragments {
        let chunk = RawSeriesChunk::try_new(
            &schema,
            Buffer::from_slice_ref(b"series-a"),
            Some(metric.clone()),
            RawTimestamps::try_new(ScalarBuffer::from(vec![timestamp])).expect("one timestamp"),
            SeriesFields::one(Samples::from_float(Float64Array::from(vec![value]))),
            is_last,
        )
        .expect("the chunk is bound to the schema it is built against");
        executor
            .push_raw(ids[0], chunk)
            .expect("the chunk is pushed to the tree's input");
    }
    let chunks = executor.finish().map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    for chunk in &chunks {
        let field = chunk
            .series
            .fields
            .as_slice()
            .first()
            .expect("the result carries the one field it was bound to");
        for (position, row) in chunk.series.time.positions() {
            match field.sample(row) {
                SampleRef::Float(value) => rows.push((position, value)),
                _ => panic!("the row at {row} holds no float"),
            }
        }
    }
    Ok(rows)
}

/// Runs `tree` over closing evaluated fragments of one canonical label set and reads its answer as
/// one entry per series the root handed over, holding that series' `(position, value)` rows.
///
/// `fragments` are `(position on the input grid, value)`, and each is a chunk that closes the series
/// it carries: this is the shape a repartition's pieces of one label set take, several chunks with
/// one point each, each complete in itself. One entry back is one series, which is what makes the
/// count of them the answer to "did the window answer one series per label set, or one per piece".
fn evaluated_series_rows(
    tree: &NativeTree,
    input_grid: &Arc<EvalGrid>,
    fragments: &[(usize, f64)],
) -> Result<Vec<Vec<(usize, f64)>>, String> {
    let schema = Arc::new(SeriesSchema {
        fields: fields(1),
        eval_grid: Some(Arc::clone(input_grid)),
    });
    let metric = SeriesLabels::try_new(Some("x".into()), Vec::new()).expect("a metric name");
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let state = NativeExecutionState::try_new(tree, Arc::clone(&pool))
        .map_err(|error| error.to_string())?;
    let ids = state.input_ids(0).collect::<Vec<_>>();
    let mut executor = NativeExecutor::new(state);
    for &(position, value) in fragments {
        let chunk = EvaluatedChunk::try_new(
            Series::try_new(
                &schema,
                metric.clone(),
                TimeAxis::from_grid_spans(Arc::clone(input_grid), vec![GridSpan::new(position, 1)])
                    .expect("a span on the grid"),
                SeriesFields::one(Samples::from_float(Float64Array::from(vec![value]))),
            )
            .expect("a series on the schema"),
            true,
        )
        .expect("a closing chunk");
        executor
            .push_evaluated(ids[0], chunk)
            .map_err(|error| error.to_string())?;
    }
    let chunks = executor.finish().map_err(|error| error.to_string())?;
    let mut series = Vec::new();
    for chunk in &chunks {
        let field = chunk
            .series
            .fields
            .as_slice()
            .first()
            .expect("the result carries the one field it was bound to");
        let mut rows = Vec::new();
        for (position, row) in chunk.series.time.positions() {
            match field.sample(row) {
                SampleRef::Float(value) => rows.push((position, value)),
                _ => panic!("the row at {row} holds no float"),
            }
        }
        series.push(rows);
    }
    Ok(series)
}

/// What a caller that placed the tree in one execution knows about its scans: this execution holds
/// one scan partition's pieces of the metric, and nothing says which series share a label set.
fn source_pieces() -> NativeProperties {
    NativeProperties::source_pieces()
}

/// The count of merges the pass put into `tree`, which is the number this change is about.
fn merges(tree: &NativeTree) -> usize {
    tree.to_string().matches("Merge").count()
}

/// Passes `root` with one boundary entry per source and returns the rewritten tree.
fn passed(root: NativeNode, sources: usize) -> NativeTree {
    let tree = NativeTree::try_new(root).expect("the fixture is a legal tree");
    insert_native_merge_inside_current_fused(&tree, &vec![source_pieces(); sources])
        .expect("the pass keeps the tree legal")
}

/// A point-wise map reads one series at a time, so it collects nothing and needs no merge.
///
/// The old pass wrapped every map's input, because a scan's facts are
/// [`CanonicalGrouping::SourcePiecesOnly`] and that was reason enough. What that assertion encoded
/// is exactly the class rule this change removes: "the input is source pieces, so collect it",
/// asked without ever asking whether *this* consumer needed one series. A map does not: it applies
/// its operations to each chunk as it arrives (`map_chunk`), and a label set it makes two series
/// share is a fact about its output, answered at the consumer of that output.
#[test]
fn a_pointwise_map_does_not_collect_its_input() {
    let out = grid(0, 60_000, 4);
    let tree = passed(map_over(window_over(raw_input(0), Arc::clone(&out))), 1);
    assert_eq!(0, merges(&tree), "a map collected its input:\n{tree}");
    let NativeExpr::Map { input, .. } = &tree.root().expr else {
        panic!("the map is not the root:\n{tree}");
    };
    assert!(
        matches!(input.expr, NativeExpr::Window { .. }),
        "the map's input is not the window it was given:\n{tree}"
    );
}

/// A broadcast reaches each of the vector side's series as it is, so neither side is collected.
#[test]
fn a_scalar_broadcast_does_not_collect_its_vector_side() {
    let out = grid(0, 60_000, 4);
    let tree = passed(
        binary_over(
            window_over(raw_input(0), Arc::clone(&out)),
            scalar(2.0, Arc::clone(&out)),
            Arc::clone(&out),
        ),
        1,
    );
    assert_eq!(0, merges(&tree), "`x * 2` collected a side:\n{tree}");
    let NativeExpr::Binary { left, right, .. } = &tree.root().expr else {
        panic!("the binary operation is not the root:\n{tree}");
    };
    assert!(
        matches!(right.expr, NativeExpr::Scalar(_)),
        "the constant side changed:\n{tree}"
    );
    assert!(
        matches!(left.expr, NativeExpr::Window { .. }),
        "the vector side was collected anyway:\n{tree}"
    );
}

/// A `Complete` aggregate folds a group one point at a time, so it does not need its input
/// collected into one series per label set.
///
/// The kernel, read in `execute.rs` and `aggregate.rs`: `AggregateRuntime::accumulate` keeps a
/// `FloatAggregator` whose states are indexed by `(group, absolute grid position)` and folds every
/// chunk it is handed into those states before dropping the chunk, and `FloatAggregator::finish`
/// then builds one series per group. Several contributions of one group at one position are
/// therefore the same accumulation as one contiguous series would give. What a `Complete` aggregate
/// does need is that every contribution to a group lies in this execution, which
/// `needs_single_execution_in` answers (`AggregateStage::Complete => true`) and the placement
/// enforces - a placement condition, not a merge.
///
/// The merge the old pass put here was not re-assembling a series for the fold; it was refusing a
/// vector holding two values of one field at one position under one label set
/// (`merge_same_labelset`'s "vector cannot contain metrics with the same labelset"). That check is
/// gone from this path with the merge; the module documentation's duplicate-check section says which
/// paths keep it and which are Part C's to restore.
#[test]
fn a_complete_aggregate_does_not_collect_its_input() {
    let out = grid(0, 60_000, 4);
    let tree = passed(
        aggregate_over(
            window_over(raw_input(0), Arc::clone(&out)),
            Arc::clone(&out),
            AggregateStage::Complete,
        ),
        1,
    );
    assert_eq!(
        0,
        merges(&tree),
        "a complete aggregate collected its input:\n{tree}"
    );
    let NativeExpr::Aggregate { input, .. } = &tree.root().expr else {
        panic!("the aggregate is not the root:\n{tree}");
    };
    assert!(
        matches!(input.expr, NativeExpr::Window { .. }),
        "the aggregate's input was collected anyway:\n{tree}"
    );
}

/// A window over fragments is served by the runtime, not by a merge in the tree - for the children
/// whose fragments the runtime is already the collection for.
///
/// `RuntimeNode::build`'s `Window` arm asks
/// [`unifies_its_own_contributions`](crate::native::plan::unifies_its_own_contributions) about its
/// child and gives the window a `MergeInput` when the answer is `Some(false)` - the child reports
/// its own fragments: they are collected, checked against the stream contract (`MergeInput::check`)
/// and merged by `merge_same_labelset` before the evaluator sees them. A merge node in the tree
/// would turn that collection *off* (a merge answers `Some(true)`), so the window would stream an
/// already-merged series while the check moved to a node without the evaluator's context. The two
/// children below are exactly those: a `Map`, which always reports fragments, and a scan leaf, which
/// hands over one whole storage series at a time and which no merge could collect in any case (a
/// merge binds samples evaluated on a grid, and a scan's are raw).
///
/// The third child, an evaluated `Input`, is the one this pairing does not cover: `Input` reports
/// nothing (`None`) and nothing below it unified anything, so the pass inserts the merge there and
/// [`a_window_over_a_plain_evaluated_input_collects_aliasing_fragments`] is that test.
#[test]
fn a_window_over_fragments_is_served_by_the_runtime_not_a_merge() {
    let out = grid(0, 60_000, 4);
    // A window over raw samples: the leaf hands over one whole series at a time, so it streams.
    let over_scan = passed(window_over(raw_input(0), Arc::clone(&out)), 1);
    assert_eq!(
        0,
        merges(&over_scan),
        "a window over a scan gained a merge:\n{over_scan}"
    );
    // A window over an evaluated child: this is the shape whose runtime arm builds a `MergeInput`
    // (`child_shared`), so the collection is the runtime's and not the tree's.
    let over_evaluated = passed(
        window_on(
            map_over(window_over(raw_input(0), Arc::clone(&out))),
            Some(Arc::clone(&out)),
            Arc::clone(&out),
        ),
        1,
    );
    assert_eq!(
        0,
        merges(&over_evaluated),
        "a window over an evaluated child gained a merge as well as the runtime's own:\n{over_evaluated}"
    );
}

/// A vector/vector match reads both sides whole and files its rows by result label set, so neither
/// side is collected.
///
/// The kernel, read in `vector_match.rs`: `binary_vectors` groups each operand by the match's labels
/// (`group`), pairs them point by point, and files every pairing's sample in `Results` under the
/// label set the sample ends under; `merge_fields` then hands over one series per set, and
/// `Results::sort_rows` refuses two values of a field at one point under one set. Which series may
/// pair is the placement's question (`matching_co_located`, via `needs_single_execution_in`), not a
/// merge's.
///
/// The old pass collected both sides of every match, because both sides were "source pieces". What
/// that encoded is that a match cannot match pieces; it can - and the same-label-set refusal a merge
/// would have produced is the kernel's own.
#[test]
fn a_vector_match_does_not_collect_either_side() {
    let out = grid(0, 60_000, 4);
    let tree = passed(
        binary_over(
            window_over(raw_input(0), Arc::clone(&out)),
            window_over(raw_input(1), Arc::clone(&out)),
            Arc::clone(&out),
        ),
        2,
    );
    assert_eq!(0, merges(&tree), "`x + y` collected a side:\n{tree}");
    let NativeExpr::Binary { left, right, .. } = &tree.root().expr else {
        panic!("the match is not the root:\n{tree}");
    };
    for side in [left, right] {
        assert!(
            matches!(side.expr, NativeExpr::Window { .. }),
            "a side of the match was collected:\n{tree}"
        );
    }
}

/// The root is not canonicalized by default: its caller reads chunks, and a chunk stream holding
/// the pieces of a label set is what it asks for.
///
/// `next_chunk` states the contract: a chunk's `is_last` closes the series it carries and not its
/// label set, a map at the root hands over pieces on purpose, and a caller that needs one series per
/// label set merges them itself. Canonicalizing the root would hold the whole answer back for a
/// consumer that never asked, which is why the old pass's unconditional call at the root is gone.
#[test]
fn the_root_is_not_canonicalized_by_default() {
    let out = grid(0, 60_000, 4);
    // A map at the root is the shape `next_chunk` names: it hands over pieces.
    let mapped = passed(map_over(window_over(raw_input(0), Arc::clone(&out))), 1);
    assert!(
        !matches!(mapped.root().expr, NativeExpr::Merge { .. }),
        "the root was canonicalized by default:\n{mapped}"
    );
    // A bare scan is the other: one whole series at a time, never a collected one.
    let scanned = passed(raw_input(0), 1);
    assert!(
        !matches!(scanned.root().expr, NativeExpr::Merge { .. }),
        "a scan root was canonicalized:\n{scanned}"
    );
}

/// The seven vector operations are decided one at a time, because only one of them needs its input
/// collected into one series per canonical label set.
///
/// The table below is the finding, one line per kernel:
///
/// - `topk`: ranks the series holding a sample at each point and keeps the selected points of the
///   series it picked. Pieces of one label set hold their samples at different positions, so at any
///   point at most one of them is a candidate, and `k` counts series.
/// - `limitk`: **the one that needs a merge.** `k` counts *series* in input order (`select::limitk`
///   walks a group's members in that order and stops after `k`), so pieces of one label set would
///   occupy several slots and be counted against `k`. The runtime merges its input only for a child
///   that shares runs (`vector.merge_input = limitk && child_shared`), and a scan leaf reports
///   `shared = false`, so the tree has to carry the merge.
/// - `limit_ratio`: keeps whole series by the hash of their labels, so all pieces of one label set
///   are kept or dropped together and each is rebuilt from the positions it holds.
/// - `sort`: reorders the series it is given; the series keep their labels and their points.
/// - `histogram_quantile`/`histogram_fraction`: the kernel merges label-set pieces itself
///   (`vector_series` calls `merge_same_labelset` before `evaluate_histograms`).
/// - `count_values`: counts samples per group and value at each point, which folds contributions the
///   way an aggregate folds them.
/// - `absent`: reads presence per point over the whole vector, so the union of the contributions is
///   the presence it needs.
#[test]
fn each_vector_operator_is_decided_on_its_own() {
    let out = grid(0, 60_000, 4);
    let by_job = || Grouping::By(Arc::from([Arc::from("job")]));
    for (op, expected) in [
        (
            BoundVectorOp::TopK {
                k: 1.0,
                top: true,
                group: by_job(),
            },
            0,
        ),
        (
            BoundVectorOp::LimitK {
                k: 1.0,
                group: by_job(),
            },
            1,
        ),
        (BoundVectorOp::LimitRatio { ratio: 0.5 }, 0),
        (BoundVectorOp::Sort(SortOrder::Asc), 0),
        (
            BoundVectorOp::Absent {
                labels: SeriesLabels::try_new(None, Vec::new()).expect("no labels"),
            },
            0,
        ),
        (
            BoundVectorOp::Histogram(HistogramFunction::Quantile(0.9)),
            0,
        ),
        (
            BoundVectorOp::CountValues {
                label: Arc::from("value"),
                group: by_job(),
            },
            0,
        ),
    ] {
        let tree = passed(
            vector_over(window_over(raw_input(0), Arc::clone(&out)), op.clone()),
            1,
        );
        assert_eq!(
            expected,
            merges(&tree),
            "`{op:?}` should have gained {expected} merge(s):\n{tree}"
        );
    }
}

/// The three-condition rule, positively: a consumer that needs one unified series, over an input that
/// does not already provide one, still gets a merge.
///
/// `limitk` is that consumer (see [`each_vector_operator_is_decided_on_its_own`]); the input is a
/// window over a scan, which proves no grouping. Co-location - condition 3 - is not this pass's to
/// answer: the route only runs it for a placement that reads every input partition, and
/// `merges_are_placeable_in` is where a tree carrying a merge is checked against that execution.
///
/// The second half is the other side of condition 2: an input that already answers one series per
/// label set - here the merge the first half inserted - gains nothing.
#[test]
fn the_only_consumer_that_needs_a_merge_still_gets_one() {
    let out = grid(0, 60_000, 4);
    let limitk = || BoundVectorOp::LimitK {
        k: 1.0,
        group: Grouping::By(Arc::from([])),
    };
    let tree = passed(
        vector_over(window_over(raw_input(0), Arc::clone(&out)), limitk()),
        1,
    );
    assert_eq!(1, merges(&tree), "`limitk` gained no merge:\n{tree}");
    let NativeExpr::Vector { input, .. } = &tree.root().expr else {
        panic!("the vector operation is not the root:\n{tree}");
    };
    let NativeExpr::Merge { input: collected } = &input.expr else {
        panic!("the merge does not sit over the `limitk` input:\n{tree}");
    };
    assert!(
        matches!(collected.expr, NativeExpr::Window { .. }),
        "the merge does not sit directly over the window:\n{tree}"
    );

    // Condition 2 again, one level down: a `limitk` over a `limitk` whose input was already
    // collected gains nothing, because the inner one reports `CanonicalComplete`.
    let nested = passed(
        vector_over(
            vector_over(window_over(raw_input(0), Arc::clone(&out)), limitk()),
            limitk(),
        ),
        1,
    );
    assert_eq!(
        1,
        merges(&nested),
        "a second `limitk` collected an input that was already complete:\n{nested}"
    );
}

/// A legal `info` tree survives the pass, and the tree the pass produces is the one its validator
/// accepts.
///
/// This is the contract the pass once broke: it wrapped every whole-series consumer in a merge,
/// `info`'s info side included, and the validator used to require that side be *directly* an instant
/// selection reporting sample times - so the pass built a tree `NativeTree::try_new` then rejected.
/// `info` needs no merge on either side (the kernel indexes the info side by `(name, identifying
/// values)` and merges the results that share a label set), and the validator still looks through a
/// merge for a tree that carries one.
#[test]
fn a_legal_info_tree_survives_canonicalize_and_validation() {
    let out = grid(0, 60_000, 4);
    let tree = NativeTree::try_new(info_over(
        window_over(raw_input(0), Arc::clone(&out)),
        info_selection(&out),
    ))
    .expect("the compiled tree is legal");
    let boundary = [NativeProperties::source_pieces()];
    let passed = insert_native_merge_inside_current_fused(&tree, &boundary)
        .expect("the pass keeps the tree legal");

    assert_eq!(0, merges(&passed), "`info` collected a side:\n{passed}");

    // The pass returns a tree it validated, and the same tree validates again from scratch.
    NativeTree::try_new(passed.root().clone()).expect("the passed tree is legal on its own");

    // The pass is idempotent on its own output: the one consumer it collects already carries a
    // merge, so a second run adds nothing.
    let again = insert_native_merge_inside_current_fused(&passed, &boundary)
        .expect("the pass accepts its own output");
    assert_eq!(
        passed.to_string(),
        again.to_string(),
        "the pass changed its own output"
    );
}

/// The pass is idempotent over the output it actually produces, which is the tree with a merge in
/// it: a second run must not collect an input a merge already made complete.
#[test]
fn the_pass_is_idempotent() {
    let out = grid(0, 60_000, 4);
    let boundary = [NativeProperties::source_pieces()];
    let limitk = || BoundVectorOp::LimitK {
        k: 1.0,
        group: Grouping::By(Arc::from([])),
    };
    for root in [
        // The consumers that gain a merge: `limitk`, and a window sitting on a fused input.
        vector_over(window_over(raw_input(0), Arc::clone(&out)), limitk()),
        window_on(
            evaluated_input(0, &out),
            Some(Arc::clone(&out)),
            Arc::clone(&out),
        ),
        // And shapes that gain none, so "changed nothing" is asserted on both answers.
        map_over(window_over(raw_input(0), Arc::clone(&out))),
        window_over(raw_input(0), Arc::clone(&out)),
    ] {
        let once = NativeTree::try_new(root).expect("the fixture is a legal tree");
        let once = insert_native_merge_inside_current_fused(&once, &boundary)
            .expect("the pass keeps the tree legal");
        let twice = insert_native_merge_inside_current_fused(&once, &boundary)
            .expect("the pass accepts its own output");
        assert_eq!(
            once.to_string(),
            twice.to_string(),
            "the pass changed its own output:\n{once}"
        );
    }
}

/// An instant selection that reports an info series' own time keeps every fact its input had: the
/// runtime files that series under all of its labels, so which series share a label set is
/// unchanged. `timestamp(x)` is the one that does not - it drops the metric name on the way - and
/// the two are asserted together so the distinction cannot be lost.
#[test]
fn info_timestamp_keeps_its_input_facts() {
    let out = grid(0, 60_000, 4);
    // A boundary that proves something, so a downgrade is visible rather than a coincidence of two
    // `Unknown`s.
    let boundary = [NativeProperties {
        owner: CanonicalOwner::ByCanonicalKey,
        grouping: CanonicalGrouping::SourcePiecesOnly,
    }];
    let facts = |value| selection(raw_input(0), Arc::clone(&out), value).properties(&boundary);

    assert_eq!(
        boundary[0],
        facts(InstantValue::InfoTimestamp),
        "an info timestamp rewrote the label sets it read"
    );
    // The sample itself is answered under the label set it was read from, so it is the same answer.
    assert_eq!(boundary[0], facts(InstantValue::Sample));
    assert_eq!(
        NativeProperties {
            owner: CanonicalOwner::Unknown,
            grouping: CanonicalGrouping::SourcePiecesOnly,
        },
        facts(InstantValue::Timestamp),
        "a timestamp kept facts it rewrote"
    );
}

/// A window folds the contributions of one canonical label set as **one** logical series, and the
/// `avg_over_time` / `sum_of_avg` pair below is what makes that visible: `sum_over_time` cannot tell
/// the two behaviours apart (`2 + 4 = 6` either way), while an average of the union (`(2 + 4) / 2 =
/// 3`) differs from a sum of separate averages (`2 + 4 = 6`).
///
/// What performs the joint processing, and therefore why the pass inserts no merge here: the window's
/// own evaluator carries one series' state across its chunks and closes it on the chunk that says
/// `is_last` (`NativeChunk::Raw` -> `RangeEvaluator::push`, whose `SeriesState` keeps the window's
/// samples and its last timestamp for the next chunk). The chunks below are one *storage* series
/// handed over in two pieces - the shape a scan partition's pieces take - and one storage series is
/// one canonical label set, so nothing can alias at the leaf and the evaluator is the unification. A
/// merge could not stand in for it either: one binds samples evaluated on a grid, and this leaf's
/// samples are raw, which is why the pass never collects a scan even though it is an `Input`.
///
/// The evaluated-input side is the one that splits in two, and both halves are pinned. Where the
/// contributions come from a node that reports its own fragments, the window gets its `MergeInput`
/// from the runtime: [`a_window_over_aliasing_child_collects_its_fragments_at_runtime`]. Where they
/// arrive at a fused input - the split shape `A -> Repartition -> B` - the leaf reports nothing, so
/// the runtime collects nothing either and the tree has to carry the merge:
/// [`a_window_over_a_plain_evaluated_input_collects_aliasing_fragments`] is the tree it carries it
/// in, and [`the_merge_in_front_of_a_window_is_what_collects_the_fragments`] is the `avg_over_time`
/// measurement of the pair above taken on that collected shape, plus the same fixture run without
/// the merge, where it does not answer a wrong number but is refused.
#[test]
fn a_window_folds_the_contributions_of_one_label_set_as_one_series() {
    let rows = |function| {
        // `sum(avg_over_time(x[5m]))`, so the answer is one number.
        runtime_rows(
            aggregate_over(
                window_of(function, grid(60_000, 60_000, 1)),
                grid(60_000, 60_000, 1),
                AggregateStage::Complete,
            ),
            // One series at 0s holding 2, continuing at 20s holding 4; the 300s window over the one
            // grid point at 60s covers both.
            &[(0, 2.0, false), (20_000, 4.0, true)],
        )
    };
    assert_eq!(
        Ok(vec![(0, 3.0)]),
        rows(RangeFunction::AvgOverTime),
        "the window did not average the union of the two contributions: it folded them apart"
    );
    // The control: `sum_over_time` answers 6 either way, so it proves nothing on its own - it is
    // here to show that the assertion above is the discriminating one.
    assert_eq!(
        Ok(vec![(0, 6.0)]),
        rows(RangeFunction::SumOverTime),
        "`sum_over_time` no longer answers the sum of the contributions"
    );
}

/// The same shape with two contributions holding a value at the **same** position is an error, not a
/// sum: `sum` would hide the conflict by adding the two values.
///
/// On a streaming window the check is the evaluator's: `RangeEvaluator::push` refuses timestamps that
/// do not increase across the chunks of one series ("raw series timestamps are not increasing across
/// chunks"). It is therefore still made, by the node that folds the contributions.
#[test]
fn a_window_refuses_two_contributions_holding_one_position() {
    let error = runtime_rows(
        aggregate_over(
            window_of(RangeFunction::SumOverTime, grid(60_000, 60_000, 1)),
            grid(60_000, 60_000, 1),
            AggregateStage::Complete,
        ),
        &[(0, 2.0, false), (0, 4.0, true)],
    )
    .expect_err("two values of one position under one label set were summed");
    assert!(
        error.contains("not increasing"),
        "the refusal does not name the stream contract it broke: {error}"
    );
}

/// A window over a node that *can* alias gets its collection from the runtime, and that collection is
/// a `MergeInput`: `RuntimeNode::build`'s `Window` arm asks its child for `shared`, and the nodes that
/// rewrite labels report `true` - a `Map` always, a `Timestamp` always, a `Window` always, a
/// label-dropping broadcast always. The window then collects the fragments, checks the stream contract
/// (`MergeInput::check`) and merges them with `merge_same_labelset` before the evaluator sees them.
///
/// That merge *is* the joint processing: cross-contribution ordering (it sorts by position), one state
/// per label set (each merged set is handed to the evaluator as one closing series) and the
/// end-of-series decision (read from the `is_last` of the collected fragments, not of each piece).
///
/// The fixture is two fragments under one label set holding values at *different* positions, which is
/// what a partition's pieces of a series look like. A window that opened one state per fragment would
/// report two series under one label set; one `MergeInput` reports one. The pass' own output is
/// asserted first, because the merge that does it is the runtime's.
///
/// That is what makes this the pair of
/// [`a_window_over_a_plain_evaluated_input_collects_aliasing_fragments`], where the same fragments
/// arrive at a fused input instead: the pass leaves this tree alone because its window's child is a
/// `Map` - a node whose fragments the runtime reports and collects, and whose collection a
/// tree-level merge would turn off - and it collects that one because its window's child is the
/// boundary itself, which proves nothing at all. The pass decides between the two with
/// [`unifies_its_own_contributions`](crate::native::plan::unifies_its_own_contributions), the same
/// function `RuntimeNode::build` computes its `shared` flag from; the module documentation of the
/// window arm is the full statement.
#[test]
fn a_window_over_aliasing_child_collects_its_fragments_at_runtime() {
    // Two grid points, so the two fragments have a position each.
    let out = grid(60_000, 60_000, 2);
    // The pass leaves the tree alone: the collection below is the runtime's.
    let passed = passed(
        window_on(
            map_over(window_over(raw_input(0), Arc::clone(&out))),
            Some(Arc::clone(&out)),
            Arc::clone(&out),
        ),
        1,
    );
    assert_eq!(
        0,
        merges(&passed),
        "the pass inserted a merge the runtime already performs:\n{passed}"
    );

    // The same shape executed over two complementary fragments of one label set.
    let schema = Arc::new(SeriesSchema {
        fields: fields(1),
        eval_grid: Some(Arc::clone(&out)),
    });
    let metric = SeriesLabels::try_new(Some("x".into()), Vec::new()).expect("a metric name");
    let fragment = |position: usize, value: f64| {
        EvaluatedChunk::try_new(
            Series::try_new(
                &schema,
                metric.clone(),
                TimeAxis::from_grid_spans(Arc::clone(&out), vec![GridSpan::new(position, 1)])
                    .expect("a span on the grid"),
                SeriesFields::one(Samples::from_float(Float64Array::from(vec![value]))),
            )
            .expect("a series on the schema"),
            true,
        )
        .expect("a closing chunk")
    };
    // The input's node output and its binding are one schema, which is what `validate` compares.
    let input = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: NativeType::Samples(Arc::clone(&schema)),
            source_index: 0,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output: NativeType::Samples(Arc::clone(&schema)),
    };
    let tree = NativeTree::try_new(window_on(
        map_over(input),
        Some(Arc::clone(&out)),
        Arc::clone(&out),
    ))
    .expect("a legal tree");
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let state =
        NativeExecutionState::try_new(&tree, Arc::clone(&pool)).expect("the runtime runs the tree");
    let ids = state.input_ids(0).collect::<Vec<_>>();
    let mut executor = NativeExecutor::new(state);
    for (position, value) in [(0, 1.0), (1, 2.0)] {
        executor
            .push_evaluated(ids[0], fragment(position, value))
            .expect("the fragment is pushed");
    }
    let chunks = executor.finish().expect("the window folds both fragments");
    assert_eq!(
        1,
        chunks.len(),
        "the window opened one state per fragment instead of collecting them:\n{tree}"
    );
    // One series, holding the positions the window decided for the unified series. Its values are
    // the function's (`rate` here), so the positions are what this asserts: a window that opened one
    // state per fragment answers one series per fragment, not one series with both fragments' rows.
    let positions: Vec<usize> = chunks[0]
        .series
        .time
        .positions()
        .map(|(position, _)| position)
        .collect();
    assert_eq!(
        vec![1],
        positions,
        "the collected series does not hold the position the two fragments decide together"
    );
}

/// The duplicate check an aliasing node owes is its own, and it fires before a window above it
/// collects anything: `Map` keeps an `Emitted` occupancy table per output label set and refuses a
/// second series writing a position a first one wrote ("no merge resolves two values of one field at
/// one position").
///
/// This is the check a merge in the tree would otherwise have made, and on this path it was never the
/// merge's: the node that makes two series one label set is the node that refuses the conflict, which
/// is why removing the merge above it takes no check with it.
#[test]
fn an_aliasing_node_refuses_two_contributions_holding_one_position() {
    let out = grid(60_000, 60_000, 1);
    let schema = Arc::new(SeriesSchema {
        fields: fields(1),
        eval_grid: Some(Arc::clone(&out)),
    });
    let metric = SeriesLabels::try_new(Some("x".into()), Vec::new()).expect("a metric name");
    let fragment = |value: f64| {
        EvaluatedChunk::try_new(
            Series::try_new(
                &schema,
                metric.clone(),
                TimeAxis::from_grid_spans(Arc::clone(&out), vec![GridSpan::new(0, 1)])
                    .expect("a span on the grid"),
                SeriesFields::one(Samples::from_float(Float64Array::from(vec![value]))),
            )
            .expect("a series on the schema"),
            true,
        )
        .expect("a closing chunk")
    };
    let input = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: NativeType::Samples(Arc::clone(&schema)),
            source_index: 0,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output: NativeType::Samples(Arc::clone(&schema)),
    };
    // A `Map` is the label-rewriting node, and it keeps the table whether or not it rewrites here.
    let tree = NativeTree::try_new(map_over(input)).expect("a legal tree");
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let state =
        NativeExecutionState::try_new(&tree, Arc::clone(&pool)).expect("the runtime runs the tree");
    let ids = state.input_ids(0).collect::<Vec<_>>();
    let mut executor = NativeExecutor::new(state);
    executor
        .push_evaluated(ids[0], fragment(2.0))
        .expect("the first fragment is pushed");
    executor
        .push_evaluated(ids[0], fragment(4.0))
        .expect("the second fragment is pushed");
    let error = executor
        .finish()
        .expect_err("two values of one position under one label set were folded");
    assert!(
        error.to_string().contains("no merge resolves"),
        "the aliasing node did not refuse the two values: {error}"
    );
}

/// A window over a **plain** evaluated input collects the aliasing fragments of one label set into
/// one series. This is the split shape: `A -> Repartition -> B` with `B`'s tree `Window(Input)`.
///
/// The two fragments below are the pieces of one canonical label set that crossed the repartition -
/// one closing chunk each, holding one point each, at different positions. The pass hands the tree
/// over with a merge between the window and the boundary, because the runtime's own collection is
/// not reachable here: `RuntimeNode::build`'s `Window` arm builds its `MergeInput` only when the
/// child reports its fragments, and `NativeExpr::Input` reports nothing at all - a leaf hands over
/// what its boundary bound it to and claims nothing about the pieces being one series, so
/// [`unifies_its_own_contributions`](crate::native::plan::unifies_its_own_contributions) answers
/// `None` for it and the runtime's flag follows with `false`. Nothing below the window in a split
/// tree unifies them either, so without the merge the window opens one state per fragment and
/// answers two series where one is owed. That is what this test measures, and both halves are
/// asserted: the merge is in the tree, and the answer is one series.
///
/// [`a_window_over_aliasing_child_collects_its_fragments_at_runtime`] is the other side of the same
/// measurement: a child that *does* report `shared` - a `Map` - gets its `MergeInput` from the
/// runtime, and the pass inserts nothing there.
#[test]
fn a_window_over_a_plain_evaluated_input_collects_aliasing_fragments() {
    let out = grid(60_000, 60_000, 2);
    let tree = passed(
        window_on(
            evaluated_input(0, &out),
            Some(Arc::clone(&out)),
            Arc::clone(&out),
        ),
        1,
    );
    // The merge is where the collection has to be, since the runtime does not perform it: between
    // the window and the input whose fragments are the pieces of one label set.
    assert_eq!(
        1,
        merges(&tree),
        "the pass left the window's fused input uncollected:\n{tree}"
    );
    let NativeExpr::Window { input, .. } = &tree.root().expr else {
        panic!("the window is not the root:\n{tree}");
    };
    let NativeExpr::Merge { input: collected } = &input.expr else {
        panic!("the merge does not sit directly over the window's input:\n{tree}");
    };
    assert!(
        matches!(&collected.expr, NativeExpr::Input(_)),
        "the merge does not sit over the boundary:\n{tree}"
    );

    // Two fragments of one label set, at different positions, each closing the series it carries.
    let series = evaluated_series_rows(&tree, &out, &[(0, 1.0), (1, 2.0)])
        .expect("the runtime runs the tree");
    assert_eq!(
        1,
        series.len(),
        "the window opened one state per fragment instead of collecting them: {series:?}\n{tree}"
    );
}

/// The merge the pass inserts is the joint processing, and this fixture is what measures it:
/// `avg_over_time` over two complementary contributions of one label set is their average over the
/// union (`(2 + 4) / 2 = 3`), and a `sum` around it keeps that 3. A window that opened one state per
/// contribution and averaged each on its own would not answer 3 from these two values, so the
/// assertion on the collected shape is one only a collected shape satisfies.
///
/// `sum_over_time` is kept as the control and is labelled one because it does *not* discriminate: it
/// answers `2 + 4 = 6` whether the contributions arrived collected or apart, so it shows that the
/// average above is the assertion that separates the two behaviours, and proves nothing by itself.
///
/// The un-collected shape is pinned as well, and it is not merely wrong - it is *refused*. Both
/// contributions decide the same output position, and the window's `Emitted` occupancy table reports
/// two series holding a value of one field at one position under one label set: "no merge resolves
/// two values of one field at one position". That is the check the module documentation's
/// duplicate-check section says a label-rewriting node keeps, and it is why this fixture separates
/// the two shapes by an error as well as by an answer: the merge is what turns two contributions
/// into the one series whose single pass is what the query owes.
#[test]
fn the_merge_in_front_of_a_window_is_what_collects_the_fragments() {
    // The window reads a 300s range over an input on a 20s grid, at the one point of a 60s grid, so
    // its range covers both contributions.
    let input_grid = grid(0, 20_000, 2);
    let out = grid(60_000, 60_000, 1);
    // The two contributions, as `(position on the input grid, value)`: 2 at 0s and 4 at 20s.
    let fragments = [(0, 2.0), (1, 4.0)];
    let window = |function| {
        window_function_on(
            function,
            evaluated_input(0, &input_grid),
            Some(Arc::clone(&input_grid)),
            Arc::clone(&out),
        )
    };
    // `sum(window_fn(x[5m]))`: `sum` folds whatever the window answered, so the answer is one number
    // whether the window collected its contributions or not.
    let summed = |function| -> Result<f64, String> {
        let root = aggregate_over(window(function), Arc::clone(&out), AggregateStage::Complete);
        let tree = passed(root, 1);
        assert_eq!(
            1,
            merges(&tree),
            "the pass did not collect the window's fused input:\n{tree}"
        );
        let series = evaluated_series_rows(&tree, &input_grid, &fragments)?;
        Ok(series.iter().flatten().map(|&(_, value)| value).sum())
    };

    // The bare window: `avg_over_time` over the two contributions as one series is their average, and
    // the collected series holds the one output position with that value.
    let collected = passed(window(RangeFunction::AvgOverTime), 1);
    assert_eq!(
        1,
        merges(&collected),
        "the pass did not collect the window's fused input:\n{collected}"
    );
    assert_eq!(
        Ok(vec![vec![(0, 3.0)]]),
        evaluated_series_rows(&collected, &input_grid, &fragments),
        "`avg_over_time` did not average the collected contributions into one series"
    );
    // An aggregate above it keeps that 3: one series in, one value out.
    assert_eq!(
        Ok(3.0),
        summed(RangeFunction::AvgOverTime),
        "`sum(avg_over_time(x[5m]))` did not keep the collected average"
    );
    // The control, which answers 6 either way and therefore discriminates nothing: `sum_over_time`
    // folds the two contributions into their sum whether they arrived collected or apart.
    assert_eq!(
        Ok(6.0),
        summed(RangeFunction::SumOverTime),
        "`sum_over_time` no longer answers the sum of the contributions"
    );

    // Without the pass the same fixture is not answered at all: two states decide the one output
    // position, and the window's occupancy table refuses the second. This is the shape the merge is
    // there to prevent, so it is asserted rather than described.
    let uncollected = NativeTree::try_new(window(RangeFunction::AvgOverTime))
        .expect("the fixture is a legal tree");
    assert_eq!(
        0,
        merges(&uncollected),
        "the fixture carries a merge of its own:\n{uncollected}"
    );
    let error = evaluated_series_rows(&uncollected, &input_grid, &fragments)
        .expect_err("two states decided one output position under one label set");
    assert!(
        error.contains("no merge resolves"),
        "the uncollected window did not report the duplicate it decided: {error}"
    );
}

/// A `Vector` that hands the series it ranked back unchanged is collected **above** it, because the
/// window over it reads the fragments of one label set the boundary handed over.
///
/// `topk`, `limit_ratio` and `sort` share one arm of both readers - `unifies_its_own_contributions`
/// answers for all three by passing its input's answer on, and `RuntimeNode::build`'s report follows
/// that answer - so they are asserted together. Each hands back the series it read with their
/// labels, so several contributions of one label set stay several series. Over a boundary all three
/// answer "cannot say", which is what makes [`rewrite`]'s `Window` arm insert the merge.
///
/// The measurement is the pair the gap is stated as. The window below is one input step wide and the
/// input grid and the output grid are the same two points, so each fragment decides exactly one
/// output position:
///
/// - **without** the pass: `merges = 0` and the window opens one state per fragment, so it answers
///   **two** series where one is owed;
/// - **with** the pass: `merges = 1` and the fragments are collected first, so the window answers
///   **one** series holding both positions.
///
/// The merge goes above the operation and not below it: `topk` ranks the series it is handed, so
/// collecting below it would change which series it keeps. What this test pins is the window's
/// series identity; a collection *below* a `topk` is a different question and is not what this pass
/// does.
#[test]
fn a_vector_that_hands_its_series_back_is_collected_above() {
    // Two points 20s apart, and a window one step wide, so a fragment at input position `i` decides
    // output position `i` and nothing else.
    let grid_two = grid(0, 20_000, 2);
    let range_ms = 20_000;
    let by_job = || Grouping::By(Arc::from([Arc::from("job")]));
    for op in [
        BoundVectorOp::TopK {
            k: 1.0,
            top: true,
            group: by_job(),
        },
        BoundVectorOp::LimitRatio { ratio: 1.0 },
        BoundVectorOp::Sort(SortOrder::Asc),
    ] {
        let shape = || {
            window_sized_on(
                RangeFunction::AvgOverTime,
                vector_over(evaluated_input(0, &grid_two), op.clone()),
                Some(Arc::clone(&grid_two)),
                Arc::clone(&grid_two),
                range_ms,
            )
        };
        // Without the pass: two fragments, two states, two series under one label set.
        let uncollected = NativeTree::try_new(shape()).expect("the fixture is a legal tree");
        assert_eq!(
            0,
            merges(&uncollected),
            "the fixture carries a merge of its own:\n{uncollected}"
        );
        let apart = evaluated_series_rows(&uncollected, &grid_two, &[(0, 1.0), (1, 2.0)])
            .unwrap_or_else(|error| panic!("the runtime runs `{op:?}`: {error}\n{uncollected}"));
        assert_eq!(
            2,
            apart.len(),
            "`{op:?}` answered {} series where the un-collected window owes one per fragment: \
             {apart:?}\n{uncollected}",
            apart.len()
        );

        // With the pass: one merge above the operation, and one series holding both positions.
        let tree = passed(shape(), 1);
        assert_eq!(
            1,
            merges(&tree),
            "`{op:?}` over a boundary was not collected:\n{tree}"
        );
        let NativeExpr::Window { input, .. } = &tree.root().expr else {
            panic!("the window is not the root:\n{tree}");
        };
        let NativeExpr::Merge { input: collected } = &input.expr else {
            panic!("the merge does not sit over the window's input:\n{tree}");
        };
        assert!(
            matches!(&collected.expr, NativeExpr::Vector { .. }),
            "the merge does not sit over the vector operation:\n{tree}"
        );
        let series = evaluated_series_rows(&tree, &grid_two, &[(0, 1.0), (1, 2.0)])
            .unwrap_or_else(|error| panic!("the runtime runs `{op:?}`: {error}\n{tree}"));
        assert_eq!(
            vec![vec![(0, 1.0), (1, 2.0)]],
            series,
            "`{op:?}` did not hand the window one folded series: {series:?}\n{tree}"
        );
    }
}

/// A scalar `Broadcast` whose comparison keeps the label set it read is collected **above** it, for
/// the same reason a `Vector` that keeps its series is.
///
/// `x > 0` is bound as a filter, so `BroadcastRuntime::drops_labels` is `false` and the broadcast
/// reaches each series of its vector side as it is, keeping its labels: what arrives at the window is
/// the boundary's fragments, still several series of one label set.
/// `unifies_its_own_contributions` passes the vector side's "cannot say" on for exactly the
/// operations that keep the labels, which is what makes the window collect them.
///
/// The measurement is the same pair as [`a_vector_that_hands_its_series_back_is_collected_above`]'s,
/// on the same one-step window: **without** the pass `merges = 0` and the window answers **two**
/// series where one is owed, and **with** it `merges = 1` and the answer is **one** series holding
/// both positions.
///
/// The control is the other label policy on the same shape: `x + 0` drops the metric name, the
/// runtime reports its fragments itself, and the tree gains no merge.
#[test]
fn a_filtering_comparison_that_keeps_its_labels_is_collected_above() {
    let grid_two = grid(0, 20_000, 2);
    let range_ms = 20_000;
    let shape = |matching: BinaryMatching| {
        window_sized_on(
            RangeFunction::AvgOverTime,
            binary_matched(
                evaluated_input(0, &grid_two),
                scalar(0.0, Arc::clone(&grid_two)),
                Arc::clone(&grid_two),
                matching,
            ),
            Some(Arc::clone(&grid_two)),
            Arc::clone(&grid_two),
            range_ms,
        )
    };

    // Without the pass: `avg_over_time((x > 0)[5m:1m])` answers one series per fragment.
    let uncollected = NativeTree::try_new(shape(filter())).expect("the fixture is a legal tree");
    assert_eq!(
        0,
        merges(&uncollected),
        "the fixture carries a merge of its own:\n{uncollected}"
    );
    let apart = evaluated_series_rows(&uncollected, &grid_two, &[(0, 1.0), (1, 2.0)])
        .expect("the runtime runs the filtering comparison");
    assert_eq!(
        2,
        apart.len(),
        "`x > 0` answered {} series where the un-collected window owes one per fragment: \
         {apart:?}\n{uncollected}",
        apart.len()
    );

    // With the pass: the merge sits over the comparison and the window answers one series.
    let tree = passed(shape(filter()), 1);
    assert_eq!(
        1,
        merges(&tree),
        "the filtering comparison was not collected:\n{tree}"
    );
    let NativeExpr::Window { input, .. } = &tree.root().expr else {
        panic!("the window is not the root:\n{tree}");
    };
    let NativeExpr::Merge { input: collected } = &input.expr else {
        panic!("the merge does not sit over the window's input:\n{tree}");
    };
    assert!(
        matches!(&collected.expr, NativeExpr::Binary { .. }),
        "the merge does not sit over the comparison:\n{tree}"
    );
    // Both fragments pass the filter (1 > 0 and 2 > 0), so both positions survive the comparison;
    // the merge makes them one series and the window answers one series covering both.
    let series = evaluated_series_rows(&tree, &grid_two, &[(0, 1.0), (1, 2.0)])
        .expect("the runtime runs the filtering comparison");
    assert_eq!(
        vec![vec![(0, 1.0), (1, 2.0)]],
        series,
        "the filtering comparison did not hand the window one folded series: {series:?}\n{tree}"
    );

    // The control: the same shape under an operation that drops the metric name reports its own
    // fragments, so the runtime collects them and the tree gains nothing.
    let dropping = passed(shape(add()), 1);
    assert_eq!(
        0,
        merges(&dropping),
        "a label-dropping broadcast was collected as well as announced:\n{dropping}"
    );
}

/// The merge-count table: one row per consumer, and the number of merges the pass leaves for it.
///
/// The counts are what `unifies_its_own_contributions` decides, so this table is the whole answer in
/// one place. A child that reports its fragments itself (`Map`, `Timestamp`, a label-dropping
/// broadcast, a match of two vectors, a `Window` over one of those) is collected by the runtime's
/// `MergeInput`, and a child that has already unified (`Merge`, an `Aggregate`, two vectors matched
/// by the kernel) needs no collection at all: both are `0`. The children that answer "cannot say"
/// are the boundary itself and the nodes that hand its series back unchanged - `topk`/`limit_ratio`/
/// `sort`, a filtering comparison - and those are the `1`s.
///
/// `limitk` is the one consumer whose merge is *below* the operation rather than above it, because
/// `k` counts series: its row is a `1` that the `Vector` arm of [`rewrite`] inserts, not this
/// table's window. It is also the one row that gained no change from this work - the `Vector` arm
/// already collected it - which is why it is in the table: the two rows this change added are the
/// `topk`/`limit_ratio`/`sort` ones and `x > 0`, and the `0` rows above them are the ones that must
/// stay `0`.
#[test]
fn the_merge_count_table() {
    let out = grid(0, 60_000, 4);
    let grid_of_input = || Arc::clone(&out);
    let by_job = || Grouping::By(Arc::from([Arc::from("job")]));
    let window_on_input = |inner: NativeNode| {
        window_function_on(
            RangeFunction::AvgOverTime,
            inner,
            Some(grid_of_input()),
            Arc::clone(&out),
        )
    };
    let rows: Vec<(&str, NativeNode, usize, usize)> = vec![
        (
            "`rate(x[5m])`: a window over a raw scan",
            window_over(raw_input(0), Arc::clone(&out)),
            1,
            0,
        ),
        (
            "`avg_over_time(abs(rate(x[5m]))[5m:1m])`: a map reports its fragments",
            window_on_input(map_over(window_over(raw_input(0), Arc::clone(&out)))),
            1,
            0,
        ),
        (
            "`avg_over_time(timestamp(rate(x[5m]))[5m:1m])`: a timestamp reports its fragments",
            window_on_input(timestamp_over(window_over(raw_input(0), Arc::clone(&out)))),
            1,
            0,
        ),
        (
            "`avg_over_time(rate(x[5m])[5m:1m])`: the inner window reports the fragments it collects",
            window_on_input(window_over(raw_input(0), Arc::clone(&out))),
            1,
            0,
        ),
        (
            "a window over a merge: it has already unified",
            window_on_input(merge_over(evaluated_input(0, &out))),
            1,
            0,
        ),
        (
            "`sum(sum_over_time(x[5m]))`: a finished aggregate has",
            window_on_input(aggregate_over(
                window_over(raw_input(0), Arc::clone(&out)),
                Arc::clone(&out),
                AggregateStage::Complete,
            )),
            1,
            0,
        ),
        (
            "a window over `x + y`: the kernel files rows by result label set",
            window_on_input(binary_over(
                window_over(raw_input(0), Arc::clone(&out)),
                window_over(raw_input(1), Arc::clone(&out)),
                Arc::clone(&out),
            )),
            2,
            0,
        ),
        (
            "a window over `x * 2`: a label-dropping broadcast reports its fragments",
            window_on_input(binary_over(
                window_over(raw_input(0), Arc::clone(&out)),
                scalar(2.0, Arc::clone(&out)),
                Arc::clone(&out),
            )),
            1,
            0,
        ),
        (
            "`avg_over_time(x[5m:1m])`: a window over a fused input",
            window_on_input(evaluated_input(0, &out)),
            1,
            1,
        ),
        (
            "`avg_over_time(topk(1, x)[5m:1m])`: a `Vector` hands its series back",
            window_on_input(vector_over(
                evaluated_input(0, &out),
                BoundVectorOp::TopK {
                    k: 1.0,
                    top: true,
                    group: by_job(),
                },
            )),
            1,
            1,
        ),
        (
            "`avg_over_time(limit_ratio(0.5, x)[5m:1m])`",
            window_on_input(vector_over(
                evaluated_input(0, &out),
                BoundVectorOp::LimitRatio { ratio: 0.5 },
            )),
            1,
            1,
        ),
        (
            "`avg_over_time(sort(x)[5m:1m])`",
            window_on_input(vector_over(
                evaluated_input(0, &out),
                BoundVectorOp::Sort(SortOrder::Asc),
            )),
            1,
            1,
        ),
        (
            "`avg_over_time(limitk(1, x)[5m:1m])`: the merge is below the operation",
            window_on_input(vector_over(
                evaluated_input(0, &out),
                BoundVectorOp::LimitK {
                    k: 1.0,
                    group: by_job(),
                },
            )),
            1,
            1,
        ),
        (
            "`avg_over_time((x > 0)[5m:1m])`: a filtering comparison keeps its labels",
            window_on_input(binary_matched(
                evaluated_input(0, &out),
                scalar(0.0, Arc::clone(&out)),
                Arc::clone(&out),
                filter(),
            )),
            1,
            1,
        ),
    ];
    for (shape, root, sources, expected) in rows {
        // The count is what the *pass* inserted, so a fixture that carries a merge of its own (the
        // "already unified" row, whose merge is placed by hand to be the input) does not have that
        // merge counted against it.
        let compiled = NativeTree::try_new(root).expect("the fixture is a legal tree");
        let before = merges(&compiled);
        let tree = passed(compiled.root().clone(), sources);
        let inserted = merges(&tree) - before;
        assert_eq!(
            expected, inserted,
            "{shape}: expected {expected} inserted merge(s) in\n{tree}"
        );
    }
}
