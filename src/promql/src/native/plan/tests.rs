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

use std::sync::Arc;

use super::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundSetOp, BoundVectorOp, BoundWindow, CanonicalGrouping, CanonicalOwner, CoPartitionedPair,
    Grouping, InstantValue, MapOp, NativeExpr, NativeNode, NativeProperties, NativeTree,
    NativeType, match_is_co_partitioned, merges_are_placeable_in, needs_single_execution,
    needs_single_execution_in, needs_single_execution_in_co, no_label_sets, output_has_metric_name,
    preserves_canonical_identity, reads_across_series,
};
use crate::extension_plan::{FusedSeries, FusedSeriesBinding};
use crate::histogram_eval::NativeHistogramFunction;
use crate::info::InfoSelector;
use crate::map::MathOp;
use crate::map_label::LabelRewrite;
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::select::SortOrder;
use crate::vector_match::{
    BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels, SetOp,
};
use datafusion::common::DFSchema;
use datatypes::series::SeriesLabels;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};

// The tree builders below are `pub(crate)` because the rewrite in [`canonicalize`] is proved with
// the trees they build, rather than with a second set of fixtures that could drift from these.
//
// [`canonicalize`]: crate::native::canonicalize

pub(crate) fn grid(start: i64, step: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start, step, len).unwrap()).unwrap())
}

pub(crate) fn evaluated(
    schema_fields: Arc<[datatypes::series::samples::FieldSpec]>,
    grid: Arc<EvalGrid>,
) -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: schema_fields,
        eval_grid: Some(grid),
    }))
}

/// `count` fields named `f0`, `f1`, ..., which is the layout a multi-field source has.
fn fields(count: usize) -> Arc<[FieldSpec]> {
    (0..count)
        .map(|index| FieldSpec {
            name: Arc::from(format!("f{index}")),
        })
        .collect::<Vec<_>>()
        .into()
}

fn raw() -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::new([]),
        eval_grid: None,
    }))
}

fn input() -> NativeNode {
    raw_input_at(0)
}

/// A scan of raw samples bound to source `source_index`.
pub(crate) fn raw_input_at(source_index: usize) -> NativeNode {
    let output = raw();
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

pub(crate) fn input_on(grid: Arc<EvalGrid>) -> NativeNode {
    input_on_at(0, grid)
}

/// A scan bound to source `source_index`, evaluated on `grid`.
///
/// Two of these are two sources, and the boundary holds one fact for each of them: what tells them
/// apart is the index on the binding, not the order the tree is walked in.
pub(crate) fn input_on_at(source_index: usize, grid: Arc<EvalGrid>) -> NativeNode {
    // One schema allocation shared by the binding and the output: `validate` checks that the output
    // is the binding it was bound to, which is about the binding itself, not the grid's spec.
    let output = evaluated(Arc::new([]), grid);
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index,
            ordering: SeriesOrdering::Contiguous,
        }),
        output,
    }
}

/// A scan of `count` fields, so a consumer's field check has something to disagree with.
fn input_with_fields(count: usize) -> NativeNode {
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: fields(count),
        eval_grid: None,
    }));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// A scan of raw samples whose chunks of different series may interleave.
fn interleaved_input() -> NativeNode {
    let output = raw();
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Interleaved,
        }),
        output,
    }
}

/// A constant scalar, which is a scalar type evaluated on the statement's grid.
fn scalar_on(value: f64, grid: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(grid),
    }
}

/// The grid a state node is indexed on.
fn state_grid(node: &NativeNode) -> Arc<EvalGrid> {
    let NativeType::AggregateState(schema) = &node.output else {
        unreachable!("expected aggregate state")
    };
    Arc::clone(&schema.grid)
}

fn partial_aggregate(inner: NativeNode) -> NativeNode {
    let grid = inner
        .output
        .grid()
        .expect("a partial aggregate consumes an evaluated vector")
        .clone();
    NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Grouping::By(Arc::new(["job".into()])),
            accumulators: Arc::new([]),
            grid,
        })),
    }
}

fn final_aggregate(state: NativeNode) -> NativeNode {
    let grid = state_grid(&state);
    NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(state),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Final,
            },
        },
        output: evaluated(Arc::new([]), grid),
    }
}

fn window_over(inner: NativeNode, input_grid: Option<Arc<EvalGrid>>) -> NativeNode {
    window_on(inner, input_grid, grid(0, 60_000, 4))
}

/// A window over `inner` that produces `count` fields on `out`, so a consumer of it can be given a
/// layout the window itself holds legally.
fn window_fields(inner: NativeNode, out: Arc<EvalGrid>, count: usize) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function: RangeFunction::Rate,
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
        output: evaluated(fields(count), out),
    }
}

/// A window over `inner` that produces `out`, so two windows can be given the same grid.
pub(crate) fn window_on(
    inner: NativeNode,
    input_grid: Option<Arc<EvalGrid>>,
    out: Arc<EvalGrid>,
) -> NativeNode {
    window_with(inner, RangeFunction::Rate, input_grid, out)
}

/// A window of `range_ms` with no offset, anchor or modifier: the shape a selector's range takes.
pub(crate) fn range(range_ms: i64) -> RangeWindow {
    RangeWindow {
        range_ms,
        offset_ms: 0,
        anchor_ms: None,
        modifier: None,
    }
}

/// A window running `function` over `inner`, evaluated on `out`.
///
/// Which function it runs is what decides whether the output keeps the label set it read, so a test
/// about the facts a window hands on picks the function it means.
pub(crate) fn window_with(
    inner: NativeNode,
    function: RangeFunction,
    input_grid: Option<Arc<EvalGrid>>,
    out: Arc<EvalGrid>,
) -> NativeNode {
    window_of_range(inner, function, 300_000, input_grid, out)
}

/// The same, over a window `range_ms` wide.
pub(crate) fn window_of_range(
    inner: NativeNode,
    function: RangeFunction,
    range_ms: i64,
    input_grid: Option<Arc<EvalGrid>>,
    out: Arc<EvalGrid>,
) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function,
                window: range(range_ms),
                input_grid,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(Arc::new([]), out),
    }
}

/// An instant selection over raw samples. With `value` other than [`InstantValue::Sample`] it
/// reports a time instead of the sample's value, which is the selection that rewrites the labels.
pub(crate) fn instant_over(
    inner: NativeNode,
    value: InstantValue,
    out: Arc<EvalGrid>,
) -> NativeNode {
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
        output: evaluated(Arc::new([]), out),
    }
}

/// A timestamp over an evaluated vector: each value replaced by its sample's time.
pub(crate) fn timestamp_of(inner: NativeNode) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Timestamp {
            input: Box::new(inner),
        },
        output,
    }
}

pub(crate) fn map_over(inner: NativeNode) -> NativeNode {
    let out = inner.output.grid().expect("a map needs a grid").clone();
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap {
                ops: Arc::new([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: evaluated(Arc::new([]), out),
    }
}

/// A canonical merge over `inner`, laid out as `inner` is: a merge writes no field and no grid of its
/// own, so it reports its input's layout.
pub(crate) fn merge_over(inner: NativeNode) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(inner),
        },
        output,
    }
}

pub(crate) fn add() -> BinaryMatching {
    BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    }
}

/// The same modifiers with another operator, which is what decides whether a scalar broadcast drops
/// the label set it read: `broadcast_drops_labels` reads the operator and whether it returns a bool.
fn matching(op: BinaryOp, return_bool: bool) -> BinaryMatching {
    BinaryMatching {
        op,
        return_bool,
        ..add()
    }
}

/// `on(host)` one-to-one, the match a caller writes when it means to pair series by a label set of
/// its own rather than by the canonical keys a placement groups by.
fn on_host() -> BinaryMatching {
    BinaryMatching {
        labels: MatchingLabels::On(vec!["host".into()]),
        ..add()
    }
}

/// `by(names)`: the grouping `sum by(names)` folds with, which keeps only the labels it lists.
fn by(names: &[&str]) -> Grouping {
    Grouping::By(
        names
            .iter()
            .map(|name| Arc::from(*name))
            .collect::<Vec<_>>()
            .into(),
    )
}

/// `without(names)`: the grouping `sum without(names)` folds with, which drops the labels it lists and
/// `__name__` with them.
fn without(names: &[&str]) -> Grouping {
    Grouping::Without(
        names
            .iter()
            .map(|name| Arc::from(*name))
            .collect::<Vec<_>>()
            .into(),
    )
}

/// `sum` over `inner` folded into a query value, grouped as `group` says.
///
/// A `Complete` fold answers a group key of its own: `by(ns)` drops the metric name, `by("__name__")`
/// keeps whatever the input had, and the same group is what a split plan's `Final` stage answers.
fn sum_by(inner: NativeNode, group: Grouping) -> NativeNode {
    let out = inner
        .output
        .grid()
        .expect("an aggregate needs a grid")
        .clone();
    NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group,
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), out),
    }
}

/// A `sum by (job)` folded into `inner` within this execution, which is an aggregate whose output is
/// a query value rather than state.
pub(crate) fn complete_aggregate(inner: NativeNode) -> NativeNode {
    let out = inner
        .output
        .grid()
        .expect("an aggregate needs a grid")
        .clone();
    NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), out),
    }
}

/// A `sort` over `inner`, which is a vector operation: it decides the whole vector at a point.
pub(crate) fn sorted(inner: NativeNode) -> NativeNode {
    let out = inner
        .output
        .grid()
        .expect("a vector operation needs a grid")
        .clone();
    NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(inner),
            op: BoundVectorOp::Sort(SortOrder::Asc),
        },
        output: evaluated(Arc::new([]), out),
    }
}

/// `left + right`, which matches two vectors by label.
pub(crate) fn sum_of(left: NativeNode, right: NativeNode) -> NativeNode {
    let out = left
        .output
        .grid()
        .expect("a binary operation needs a grid")
        .clone();
    NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: add(),
        },
        output: evaluated(Arc::new([]), out),
    }
}

/// `left <op> right` with the modifiers `matching` states, on the grid both sides are evaluated on.
///
/// The output is what the kinds of the two sides make it: two constants make a scalar and anything
/// else an evaluated vector, which is what [`NativeNode::validate`] asks of a binary operation.
pub(crate) fn binary_with(
    left: NativeNode,
    right: NativeNode,
    matching: BinaryMatching,
) -> NativeNode {
    let output = match (&left.output, &right.output) {
        (NativeType::Scalar(grid), NativeType::Scalar(_)) => NativeType::Scalar(Arc::clone(grid)),
        _ => evaluated(
            Arc::new([]),
            left.output
                .grid()
                .expect("a binary operation needs a grid")
                .clone(),
        ),
    };
    NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching,
        },
        output,
    }
}

/// A `label_replace` over `inner`, which writes a label of its own choosing: the one map operation
/// that keeps `__name__` and still changes which series share a label set.
pub(crate) fn map_rewriting_labels(inner: NativeNode) -> NativeNode {
    let out = inner.output.grid().expect("a map needs a grid").clone();
    let rewrite = LabelRewrite::replace("dst", "$1", "src", "(.*)")
        .expect("a label_replace with a valid expression");
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap {
                ops: Arc::new([MapOp::Label(Arc::new(rewrite))]),
            },
        },
        output: evaluated(Arc::new([]), out),
    }
}

/// A compiled tree is accepted when every node's contract holds.
#[test]
fn a_window_over_raw_samples_is_valid() {
    let root = window_over(input(), None);
    assert!(NativeTree::try_new(root).is_ok());
}

/// Adding a map to a window changes the tree and nothing else: no new shape is recognised.
#[test]
fn adding_a_map_only_changes_the_tree() {
    let bare = window_over(input(), None);
    let mapped = map_over(window_over(input(), None));
    // Both trees are legal, so the comparison below is about the shape and not about both failing.
    assert!(bare.validate().is_ok(), "{}", bare.validate().unwrap_err());
    assert!(
        mapped.validate().is_ok(),
        "{}",
        mapped.validate().unwrap_err()
    );
    // The mapped tree is that map over that window over that input, and nothing else: the map on
    // top is the one node the bare tree gained, and what is below it is the tree it was built from.
    let NativeExpr::Map { input, .. } = &mapped.expr else {
        unreachable!("the mapped tree's root is a map")
    };
    let NativeExpr::Window { input: leaf, .. } = &input.expr else {
        unreachable!("the map is not over the window the bare tree is")
    };
    assert!(
        matches!(leaf.expr, NativeExpr::Input(_)),
        "the window is not over the input the bare tree reads"
    );
}

#[test]
fn merge_rejects_raw_and_mismatched_input_shapes() {
    let raw_merge = NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(input()),
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    assert!(raw_merge.validate().is_err());

    let output_grid = grid(0, 60_000, 4);
    let supported = [
        input_on(Arc::clone(&output_grid)),
        window_over(
            input_on(Arc::clone(&output_grid)),
            Some(Arc::clone(&output_grid)),
        ),
        map_over(input_on(Arc::clone(&output_grid))),
    ];
    for inner in supported {
        let merge = NativeNode {
            expr: NativeExpr::Merge {
                input: Box::new(inner),
            },
            output: evaluated(Arc::new([]), Arc::clone(&output_grid)),
        };
        assert!(
            merge.validate().is_ok(),
            "{}",
            merge.validate().unwrap_err()
        );
        assert!(reads_across_series(&merge));
    }

    let inner = window_over(
        input_on(Arc::clone(&output_grid)),
        Some(Arc::clone(&output_grid)),
    );
    assert!(
        inner.validate().is_ok(),
        "{}",
        inner.validate().unwrap_err()
    );
    let wrong_grid = NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(inner.clone()),
        },
        output: evaluated(Arc::new([]), grid(1, 60_000, 4)),
    };
    let grid_error = wrong_grid.validate().unwrap_err();
    assert!(grid_error.to_string().contains("grid"), "{grid_error}");

    let wrong_fields = NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(inner),
        },
        output: evaluated(fields(1), output_grid),
    };
    let fields_error = wrong_fields.validate().unwrap_err();
    assert!(fields_error.to_string().contains("field"), "{fields_error}");
}

/// A merge canonicalizes evaluated fragments and names no operator below it, so any tree whose output
/// is an evaluated vector is one it may sit over: a scan's samples, a window's rate, an aggregate's
/// fold, a vector operation's choice and a binary operation's match all hand over the same thing.
/// An input that is not an evaluated vector at all is still refused.
#[test]
fn a_merge_sits_over_any_evaluated_vector() {
    let shared = grid(0, 60_000, 4);
    let rate = || window_on(input(), None, Arc::clone(&shared));

    // The prefix the merge was first written for, then the shapes it now also sits over: each of them
    // answers evaluated values, which is what the collection reads.
    for node in [
        merge_over(input_on(Arc::clone(&shared))),
        merge_over(rate()),
        merge_over(map_over(rate())),
        merge_over(complete_aggregate(rate())),
        merge_over(sorted(rate())),
        merge_over(sum_of(rate(), rate())),
    ] {
        assert!(node.validate().is_ok(), "{}", node.validate().unwrap_err());
        assert!(reads_across_series(&node));
    }

    // Raw samples carry their own timestamps instead of positions on a grid, so there is nothing to
    // merge by: a scan is the one input a merge refuses however many fields it carries.
    for inner in [input(), input_with_fields(1)] {
        let error = merge_over(inner).validate().unwrap_err();
        assert!(error.to_string().contains("evaluated vector"), "{error}");
    }
    // A scalar is not a vector at all, and a merge reports samples.
    let scalar_input = NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(scalar_on(1.0, Arc::clone(&shared))),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let error = scalar_input.validate().unwrap_err();
    assert!(error.to_string().contains("evaluated vector"), "{error}");
}

/// What a caller knows about a source it has collected into one series per canonical label set and
/// placed by canonical key: every label set it names is whole in one execution.
fn proven_by_key() -> NativeProperties {
    NativeProperties {
        owner: CanonicalOwner::ByCanonicalKey,
        grouping: CanonicalGrouping::CanonicalComplete,
    }
}

/// The same, in an execution that holds every series there is.
fn proven_in_one_execution() -> NativeProperties {
    NativeProperties {
        owner: CanonicalOwner::SinglePartition,
        ..proven_by_key()
    }
}

/// What a plain raw scan proves about the pieces of a label set: nothing.
///
/// A scan partitions by storage `read_key` and not by canonical key, so it holds pieces of a metric
/// with nothing grouped - which is the facts the node itself derives for an input.
pub(crate) fn source_pieces() -> NativeProperties {
    NativeProperties::source_pieces()
}

/// The facts a node reports follow what the node does to a label set.
///
/// This is where the RFC's simplified rule ("a pure numeric map inherits grouping") had to be
/// corrected: none of the three map operations leaves a label set as it is, so a map is one of the
/// nodes that lose grouping rather than pass it on.
///
/// The facts are two questions and a node may answer them apart, which is why this test asks both of
/// every shape below: *grouping* is whether the output holds at most one series per canonical label
/// set, and *ownership* is how much of each of those label sets this execution holds. A finished
/// fold and a two-vector match derive their own grouping and neither side's ownership, so an
/// expectation that reads one answer as the other is the mistake this test exists to catch.
#[test]
fn properties_follow_what_each_node_does_to_a_label_set() {
    let shared = grid(0, 60_000, 4);

    // What a caller knows about a scan: this execution holds pieces of a metric and nothing has
    // grouped those pieces by label set.
    let boundary = NativeProperties::source_pieces();

    // A bare input is the boundary it was bound to, so it proves exactly the entry supplied beside
    // it and nothing else.
    let bare = input_on(Arc::clone(&shared));
    let scanned = bare.properties(&[boundary]);
    assert_eq!(
        scanned,
        NativeProperties {
            owner: CanonicalOwner::Unknown,
            grouping: CanonicalGrouping::SourcePiecesOnly,
        }
    );

    // A window folds a range of one series' samples, so it hands over the label set it read and each
    // series stays one stream: what it proves is what its input proved.
    //
    // That holds only for the functions that keep the metric name, which is `last_over_time` and
    // `first_over_time`; every other one rewrites the labels, and the test below this one is what
    // pins that.
    let window = |function: RangeFunction| {
        window_with(
            input_on(Arc::clone(&shared)),
            function,
            Some(Arc::clone(&shared)),
            Arc::clone(&shared),
        )
    };
    assert_eq!(
        window(RangeFunction::LastOverTime).properties(&[scanned]),
        scanned
    );
    assert_eq!(
        window(RangeFunction::Rate).properties(&[scanned]),
        scanned.label_sets_of_its_own()
    );

    // The grouping proof a map could inherit from a complete child is exactly what it loses: a
    // series ends under a label set the map built, so no grouping proof survives.
    let complete = NativeProperties {
        owner: CanonicalOwner::ByCanonicalKey,
        grouping: CanonicalGrouping::CanonicalComplete,
    };
    // `map_math` ends with `drop_metric_name`, so `abs(x)` and `x` no longer hold one label set:
    // this is the case the RFC read as inheriting grouping.
    let absolute = map_over(input_on(Arc::clone(&shared)));
    let rewritten = absolute.properties(&[complete]);
    assert_eq!(
        rewritten,
        NativeProperties {
            owner: CanonicalOwner::Unknown,
            grouping: CanonicalGrouping::SourcePiecesOnly,
        }
    );

    // A single execution still holds every series it produces, so that ownership is the one a rewrite
    // does not invalidate.
    let single = NativeProperties {
        owner: CanonicalOwner::SinglePartition,
        ..complete
    };
    assert_eq!(
        absolute.properties(&[single]).owner,
        CanonicalOwner::SinglePartition
    );

    // The other two variants are no purer: `label_replace` writes a label of its own choosing
    // (`__name__` included) and `histogram_*` removes `__name__`, so no map operation preserves a
    // label set and none of them can inherit a grouping proof.
    assert!(!preserves_canonical_identity(&MapOp::Math(MathOp::Abs)));
    assert!(!preserves_canonical_identity(&MapOp::Histogram(
        NativeHistogramFunction::Count
    )));
    assert!(!preserves_canonical_identity(&MapOp::Label(Arc::new(
        LabelRewrite::replace("dst", "$1", "src", "(.*)").expect("a valid expression")
    ))));
    assert_eq!(
        map_rewriting_labels(input_on(Arc::clone(&shared))).properties(&[complete]),
        rewritten
    );

    // An aggregate answers the two questions apart, and a finished fold answers the grouping one
    // with a proof of its own: `FloatAggregator::finish` builds one series per group, so the output
    // already holds at most one series per canonical label set however the input's pieces arrived,
    // and nothing above it has an alias left to collect.
    let folded = complete_aggregate(input_on(Arc::clone(&shared)));
    assert_eq!(
        folded.properties(&[complete]).grouping,
        CanonicalGrouping::CanonicalComplete
    );
    // A group's identity is the group key, so an ownership per input key says nothing about it: two
    // input label sets placed in different executions can fold into one group, which is why the
    // ownership is re-derived while the grouping above is the fold's own.
    assert_eq!(
        folded.properties(&[complete]).owner,
        CanonicalOwner::Unknown
    );
    // A `sort` hands back the series it was given with their labels unchanged, so it inherits both
    // of its input's answers: what it reorders it answers under the same label sets, and a grouping
    // a complete child proved still holds for the order it wrote.
    let ordered = sorted(input_on(Arc::clone(&shared)));
    assert_eq!(
        ordered.properties(&[complete]).grouping,
        CanonicalGrouping::CanonicalComplete
    );

    // Two vectors are matched by label, and the two questions are answered apart here too. The
    // kernel files its rows by the *result* label set (`Results` is keyed by the label set a
    // pairing's sample ends under) and refuses two values of a field at one point, so it hands over
    // one series per canonical label set however the operands arrived: the grouping is the match's
    // own proof, not either side's. The ownership is the other question, and it is not. The two
    // sides are two sources, so each reads its own entry of the boundary.
    let sum = sum_of(
        input_on_at(0, Arc::clone(&shared)),
        input_on_at(1, Arc::clone(&shared)),
    );
    assert_eq!(
        sum.properties(&[complete, complete]).grouping,
        CanonicalGrouping::CanonicalComplete
    );
    // Its output holds series from both sides, so its ownership is only what both agree on - and
    // then only if it survives the match, whose output identity is its modifiers and not the keys it
    // read.
    assert_eq!(
        sum.properties(&[complete, complete]).owner,
        CanonicalOwner::Unknown
    );
    assert_eq!(
        sum.properties(&[complete, scanned]).owner,
        CanonicalOwner::Unknown
    );
    // The per-source indexing is visible here: a boundary that states facts for source 0 only leaves
    // source 1 unproved, which costs the ownership - the two sides then agree on nothing they were
    // asked - and not the grouping, which the match establishes for its own output whatever its
    // sides proved.
    assert_eq!(sum.properties(&[complete]).owner, CanonicalOwner::Unknown);
    assert_eq!(
        sum.properties(&[complete]).grouping,
        CanonicalGrouping::CanonicalComplete
    );

    // A merge is the node that establishes that fact, and the placement of what it read is the
    // placement of what it wrote.
    let merged = merge_over(input_on(Arc::clone(&shared)));
    assert_eq!(
        merged.properties(&[complete]),
        NativeProperties {
            owner: CanonicalOwner::ByCanonicalKey,
            grouping: CanonicalGrouping::CanonicalComplete,
        }
    );
}

/// A scalar broadcast reaches each series of its vector side as it is, so the facts it derives are
/// the vector side's and nothing else: a constant holds no label set, and which side it is written on
/// cannot change the answer.
///
/// The old derivation united the two sides before looking at them, which answered wrong in both
/// directions: with the vector on the left it took the ownership only the two sides agree on, and a
/// vector side placed by canonical key came out `Unknown` (the constant answers `SinglePartition`);
/// with the vector on the right it kept the left side's grouping, so the constant's
/// `CanonicalComplete` was handed to a vector side that is still only source pieces.
/// The two orders are one query, `x > 0` and `0 < x`, so they are asserted against the same answer.
#[test]
fn a_broadcast_derives_its_vector_sides_properties_whichever_side_the_vector_is_on() {
    let shared = grid(0, 60_000, 4);
    // `0` broadcast over the vector, which is a filtering comparison and keeps the label set it read.
    let constant = || scalar_on(0.0, Arc::clone(&shared));
    let vector = || input_on(Arc::clone(&shared));

    for boundary in [proven_by_key(), source_pieces()] {
        // What the vector side proves on its own, which is what the broadcast has to prove: nothing
        // about a label set is added, dropped or rewritten.
        let expected = vector().properties(&[boundary]);
        // `x > 0` and `0 < x`.
        let vector_left = binary_with(vector(), constant(), matching(BinaryOp::Gt, false));
        let vector_right = binary_with(constant(), vector(), matching(BinaryOp::Lt, false));
        assert_eq!(vector_left.properties(&[boundary]), expected);
        assert_eq!(vector_right.properties(&[boundary]), expected);
        assert_eq!(
            vector_left.properties(&[boundary]),
            vector_right.properties(&[boundary])
        );
    }

    // The facts the old formula lost, spelled out: a vector side placed by canonical key keeps that
    // ownership (which is what a merge above the broadcast needs), and a vector side of source pieces
    // stays ungrouped (which is what a merge above it has to collect).
    let by_key = binary_with(vector(), constant(), matching(BinaryOp::Gt, false));
    assert_eq!(
        by_key.properties(&[proven_by_key()]).owner,
        CanonicalOwner::ByCanonicalKey
    );
    assert_eq!(by_key.properties(&[proven_by_key()]), proven_by_key());
    let pieces = binary_with(constant(), vector(), matching(BinaryOp::Lt, false));
    assert_eq!(
        pieces.properties(&[source_pieces()]).grouping,
        CanonicalGrouping::SourcePiecesOnly
    );
}

/// Arithmetic and a `bool` comparison answer a series under the metric name dropped, so the broadcast
/// derives its vector side's facts rewritten — and, as for a filtering comparison, only its vector
/// side's: the constant's `no_label_sets()` facts never reach the output.
#[test]
fn arithmetic_and_a_bool_comparison_rewrite_the_vector_sides_labels_only() {
    let shared = grid(0, 60_000, 4);
    let constant = || scalar_on(2.0, Arc::clone(&shared));
    let vector = || input_on(Arc::clone(&shared));

    for boundary in [proven_by_key(), source_pieces()] {
        let expected = vector().properties(&[boundary]).label_sets_of_its_own();
        // `x * 2` and `2 * x`, which drop `__name__` in `BroadcastRuntime`; `x > bool 0` and
        // `0 < bool x`, the comparison that keeps a value instead of filtering.
        for (vector_left_op, constant_left_op) in [
            (
                matching(BinaryOp::Mul, false),
                matching(BinaryOp::Mul, false),
            ),
            (matching(BinaryOp::Gt, true), matching(BinaryOp::Lt, true)),
        ] {
            let vector_left = binary_with(vector(), constant(), vector_left_op);
            let constant_left = binary_with(constant(), vector(), constant_left_op);
            let derived = vector_left.properties(&[boundary]);
            assert_eq!(derived, expected);
            assert_eq!(constant_left.properties(&[boundary]), expected);
            // The rewrite is a real downgrade and not a carried-over proof.
            assert_eq!(derived.grouping, CanonicalGrouping::SourcePiecesOnly);
            assert_eq!(derived.owner, CanonicalOwner::Unknown);
        }
    }
}

/// Two constants answer one value for every series there is, so neither holds a label set and the
/// operation cannot claim one: what it derives is `no_label_sets()`, whatever the boundary says.
#[test]
fn two_constants_hold_no_label_set() {
    let shared = grid(0, 60_000, 4);
    let node = binary_with(
        scalar_on(1.0, Arc::clone(&shared)),
        scalar_on(2.0, Arc::clone(&shared)),
        add(),
    );
    assert!(node.validate().is_ok(), "{}", node.validate().unwrap_err());
    assert_eq!(node.properties(&[]), no_label_sets());
    assert_eq!(node.properties(&[proven_by_key()]), no_label_sets());
}

/// A vector/vector match answers series of both sides, so its ownership is only what the two sides
/// agree on, and the match re-derives it for the label sets its modifiers name: `label_sets_of_its_own` keeps
/// a one-execution ownership and downgrades one per canonical key, because the keys it read are not
/// the label sets it writes.
///
/// The grouping is the match's own and is complete however its sides arrived: the kernel files rows
/// by *result* label set. What the two sides' proofs can change is therefore the ownership alone,
/// which is the only answer that differs between the three cases below.
#[test]
fn a_two_vector_match_still_rederives_its_ownership() {
    let shared = grid(0, 60_000, 4);
    // `rate(a) + rate(b)`, whose two sides are two sources with an entry of their own.
    let matched = || {
        sum_of(
            input_on_at(0, Arc::clone(&shared)),
            input_on_at(1, Arc::clone(&shared)),
        )
    };
    // What a match proves whatever it read: a match is complete in grouping - `Results` files the
    // paired samples by the label set each one ends under and refuses two values of a field at one
    // point, so what it hands over is one series per canonical label set - while its ownership is
    // its modifiers' decision and the two sides' proofs only bound it.
    let whole_vector = |owner: CanonicalOwner| NativeProperties {
        owner,
        grouping: CanonicalGrouping::CanonicalComplete,
    };

    // Both sides placed by canonical key agree on that ownership, and the match still downgrades it.
    assert_eq!(
        matched().properties(&[proven_by_key(), proven_by_key()]),
        whole_vector(CanonicalOwner::Unknown)
    );
    // Sides that disagree agree on nothing.
    assert_eq!(
        matched().properties(&[proven_by_key(), source_pieces()]),
        whole_vector(CanonicalOwner::Unknown)
    );
    // One execution holds every series there is, so that is the one ownership the match keeps.
    assert_eq!(
        matched().properties(&[proven_in_one_execution(), proven_in_one_execution()]),
        whole_vector(CanonicalOwner::SinglePartition)
    );
}

/// A timestamp and a timestamp-mode instant replace the identity with the sample's time, so neither
/// can inherit the proof a value selection may.
///
/// `timestamp(x)` rebuilds the labels through `map::drop_metric_name`, which removes `__name__`
/// together with the type and unit labels, and the selection that reports a time instead of a value
/// does the same in `instant_eval::timestamp_of_selection`. Two series that differed only by metric
/// name therefore answer under one label set, and nothing has assembled them.
#[test]
fn the_two_timestamp_shapes_downgrade_and_a_value_selection_inherits() {
    let shared = grid(0, 60_000, 4);
    let boundary = [proven_in_one_execution()];

    // `timestamp(rate(x[5m]))` and `timestamp(x)` both rewrite the labels.
    let stamped = timestamp_of(input_on(Arc::clone(&shared)));
    assert_eq!(
        stamped.properties(&boundary),
        boundary[0].label_sets_of_its_own()
    );
    assert_eq!(
        stamped.properties(&boundary).grouping,
        CanonicalGrouping::SourcePiecesOnly
    );

    // A one-execution ownership is not invalidated by a rewrite, a per-key one is.
    assert_eq!(
        stamped.properties(&boundary).owner,
        CanonicalOwner::SinglePartition
    );
    assert_eq!(
        stamped.properties(&[proven_by_key()]).owner,
        CanonicalOwner::Unknown
    );

    // A selection that reports the sample's time is the same rewrite, applied to a selected sample.
    let selected = instant_over(
        raw_input_at(0),
        InstantValue::Timestamp,
        Arc::clone(&shared),
    );
    assert_eq!(
        selected.properties(&boundary),
        boundary[0].label_sets_of_its_own()
    );

    // A selection that reports the sample's value keeps the label set it read, so it inherits.
    let values = instant_over(raw_input_at(0), InstantValue::Sample, Arc::clone(&shared));
    assert_eq!(values.properties(&boundary), boundary[0]);
}

/// A map is point-wise over values, so raw samples are not something it can consume.
#[test]
fn a_map_cannot_consume_raw_samples() {
    let node = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(input()),
            op: BoundMap {
                ops: Arc::new([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("an evaluated vector"), "{error}");
}

#[test]
fn timestamp_requires_evaluated_vector_and_preserves_its_layout() {
    let evaluated_input = window_over(input(), None);
    let node = NativeNode {
        expr: NativeExpr::Timestamp {
            input: Box::new(evaluated_input.clone()),
        },
        output: evaluated_input.output.clone(),
    };
    assert!(node.validate().is_ok());
    let raw = NativeNode {
        expr: NativeExpr::Timestamp {
            input: Box::new(input()),
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    assert!(
        raw.validate()
            .unwrap_err()
            .to_string()
            .contains("evaluated vector")
    );
}

/// A scalar is one value per grid point, not a vector, so there are no fields to map.
#[test]
fn a_map_cannot_consume_a_scalar() {
    let out = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(scalar_on(1.0, Arc::clone(&out))),
            op: BoundMap {
                ops: Arc::new([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: evaluated(Arc::new([]), out),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("not a scalar"), "{error}");
}

/// A map with no operation would be a node that does nothing, which is a compiler mistake.
#[test]
fn a_map_needs_an_operation() {
    let inner = window_over(input(), None);
    let out = inner.output.grid().unwrap().clone();
    let node = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap { ops: Arc::new([]) },
        },
        output: evaluated(Arc::new([]), out),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("no operation"), "{error}");
}

/// A map reports one value per input field, so it cannot declare a layout its input does not
/// have. Without this check the tree would validate and only fail when the runtime compared the
/// node's schema against the one its input actually produces.
#[test]
fn a_map_cannot_change_the_field_count() {
    let inner = window_over(input(), None);
    let out = inner.output.grid().unwrap().clone();
    let node = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap {
                ops: Arc::new([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: evaluated(
            Arc::new([datatypes::series::samples::FieldSpec {
                name: Arc::from("value"),
            }]),
            out,
        ),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a map declares 1 field(s) but its input provides 0"),
        "{error}"
    );
}

/// A window reports one value per input field, so it carries the same rule as a map.
#[test]
fn a_window_cannot_change_the_field_count() {
    let inner = window_over(input(), None);
    // The output grid is the input's, so the grid check passes and the field check is what the
    // test actually exercises.
    let out = inner.output.grid().unwrap().clone();
    let node = NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: 30_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: Some(Arc::clone(&out)),
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(
            Arc::new([datatypes::series::samples::FieldSpec {
                name: Arc::from("value"),
            }]),
            out,
        ),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a window declares 1 field(s) but its input provides 0"),
        "{error}"
    );
}

/// An instant selection picks one row of the source per grid point, so it carries one value per
/// input field. A tree that declares anything else would only fail at run time, against a schema
/// the tree was supposed to have validated.
#[test]
fn an_instant_selection_cannot_change_the_field_count() {
    let out = grid(0, 60_000, 4);
    // The source has two fields; the selection declares one value.
    let node = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(input_with_fields(2)),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(1), out),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("an instant selection declares 1 field(s) but its input provides 2"),
        "{error}"
    );
}

/// An instant selection reads raw samples; feeding it evaluated values would re-anchor a grid
/// that is already fixed.
#[test]
fn an_instant_selection_reads_raw_samples() {
    let out = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(window_over(input(), None)),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(Arc::new([]), out),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("reads evaluated"), "{error}");
}

/// Aggregate state is not a sample stream, so an instant selection cannot read it.
#[test]
fn an_instant_selection_cannot_consume_state() {
    let out = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(partial_aggregate(window_on(
                input(),
                None,
                Arc::clone(&out),
            ))),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(Arc::new([]), out),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("aggregate state"), "{error}");
}

/// A window's declared input grid has to be the grid its input actually produces.
#[test]
fn a_window_checks_its_declared_input_grid() {
    let inner_grid = grid(0, 60_000, 4);
    let other_grid = grid(0, 30_000, 8);
    // The input produces `inner_grid`, but the window claims a different one.
    let node = window_over(input_on(inner_grid), Some(other_grid));
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("input grid is not the input's grid"),
        "{error}"
    );
}

/// A range evaluator reads one raw series to its last chunk before it starts the next, so raw
/// chunks of different series may not interleave.
#[test]
fn a_window_cannot_read_interleaved_input() {
    let error = window_over(interleaved_input(), None)
        .validate()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("interleaved chunks cannot be fed to it"),
        "{error}"
    );
}

/// An instant selection's consumer collects one series' chunks into one buffer, so it reads a
/// series to its last chunk before the next: it refuses an interleaved input for the reason a
/// window does.
#[test]
fn an_instant_selection_cannot_read_interleaved_input() {
    let out = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(interleaved_input()),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(Arc::new([]), out),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("interleaved chunks cannot be fed to it"),
        "{error}"
    );
}

/// A window whose range is empty or negative reads nothing at every grid point.
#[test]
fn a_window_reads_a_non_empty_range() {
    let mut node = window_over(input(), None);
    let NativeExpr::Window { op, .. } = &mut node.expr else {
        unreachable!("a window tree has a window root")
    };
    op.window.range_ms = 0;
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("empty range"), "{error}");
}

/// A node that declares an output grid its output does not carry would be addressed on one grid
/// and hand out positions on another.
#[test]
fn a_window_checks_its_output_grid() {
    let mut node = window_over(input(), None);
    let NativeExpr::Window { op, .. } = &mut node.expr else {
        unreachable!("a window tree has a window root")
    };
    op.output_grid = grid(0, 30_000, 8);
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("declares a grid its output does not carry"),
        "{error}"
    );
}

/// A window consumes samples: state holds accumulators, not a sample stream to range over.
#[test]
fn a_window_cannot_consume_state() {
    let out = grid(0, 60_000, 4);
    let node = window_on(
        partial_aggregate(window_on(input(), None, Arc::clone(&out))),
        None,
        grid(0, 60_000, 4),
    );
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("aggregate state"), "{error}");
}

/// A window ranges over timestamps, and a scalar has none of its own.
#[test]
fn a_window_cannot_consume_a_scalar() {
    let out = grid(0, 60_000, 4);
    let node = window_on(
        scalar_on(1.0, Arc::clone(&out)),
        Some(out),
        grid(0, 60_000, 4),
    );
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("not a scalar"), "{error}");
}

/// A binary node aligns two inputs, so two different grids cannot be aligned implicitly.
#[test]
fn a_binary_operation_rejects_two_grids() {
    let left = window_over(input(), None);
    let left_grid = left.output.grid().unwrap().clone();
    let right = window_on(input(), None, grid(0, 30_000, 8));
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: add(),
        },
        output: evaluated(Arc::new([]), left_grid),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("different grids"), "{error}");
}

/// Two inputs on the same grid are a legal binary operation, which is how
/// `rate(a[5m]) + rate(b[5m])` becomes an ordinary tree.
#[test]
fn a_binary_operation_over_one_grid_is_valid() {
    // Both sides evaluate on the statement's grid, so the compiler binds one grid to both.
    let shared = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_on(input(), None, Arc::clone(&shared))),
            right: Box::new(window_on(input(), None, Arc::clone(&shared))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), shared),
    };
    assert!(node.validate().is_ok());
}

/// A compiler that allocates one grid per window still produces one grid: the kernel aligns
/// operands by `EvalGrid::spec` (`vector_match::same_grid`), so `validate` compares specs too and
/// does not reject a tree the runtime would run correctly.
#[test]
fn a_binary_operation_aligns_separately_allocated_equal_specs() {
    // Two windows, one fresh `EvalGrid` each, and the statement's grid a third: equal specs, no
    // shared allocation anywhere in the tree.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_on(input(), None, grid(0, 60_000, 4))),
            right: Box::new(window_on(input(), None, grid(0, 60_000, 4))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    assert!(node.validate().is_ok(), "{}", node.validate().unwrap_err());

    // The same tree with a different spec is still rejected: the kernel rejects that pair too.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_on(input(), None, grid(0, 60_000, 4))),
            right: Box::new(window_on(input(), None, grid(30_000, 60_000, 4))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("different grids"), "{error}");
}

/// A scalar side is broadcast, not matched: `sum by(job)(rate(x[5m])) + time()` has no labels to
/// line up, so `matching` is not consulted and the output keeps the vector side's fields.
#[test]
fn a_binary_operation_broadcasts_a_scalar() {
    let shared = grid(0, 60_000, 4);
    let vector = window_on(input(), None, Arc::clone(&shared));
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(vector),
            right: Box::new(scalar_on(1.0, Arc::clone(&shared))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    assert!(node.validate().is_ok());

    // The mirror case: the scalar on the left keeps the right side's fields.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(scalar_on(1.0, Arc::clone(&shared))),
            right: Box::new(window_on(input(), None, Arc::clone(&shared))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    assert!(node.validate().is_ok());
}

/// A scalar has no fields of its own, so a broadcast that declares a different count than the
/// vector side would report a layout the kernel does not produce.
#[test]
fn a_scalar_broadcast_cannot_change_the_field_count() {
    let shared = grid(0, 60_000, 4);
    // The vector side has two fields; the broadcast declares one.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_fields(input_with_fields(2), Arc::clone(&shared), 2)),
            right: Box::new(scalar_on(1.0, Arc::clone(&shared))),
            matching: add(),
        },
        output: evaluated(fields(1), shared),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not produce the vector side's fields"),
        "{error}"
    );
}

/// Two vectors pair their fields by position, so the match reports the shorter side's count
/// (`vector_match::binary_output_schema` takes `min(lhs, rhs)`); a tree declaring anything else is
/// rejected instead of failing at run time.
#[test]
fn a_binary_operation_over_two_vectors_matches_the_shorter_side() {
    let shared = grid(0, 60_000, 4);
    // Two fields matched against one: the match produces one field, not two.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_fields(input_with_fields(2), Arc::clone(&shared), 2)),
            right: Box::new(window_fields(input_with_fields(1), Arc::clone(&shared), 1)),
            matching: add(),
        },
        output: evaluated(fields(2), Arc::clone(&shared)),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains(
            "a binary operation over two vectors declares 2 field(s) but its input provides 1"
        ),
        "{error}"
    );

    // Declaring the shorter side's count is the legal tree, so the check is not vacuous.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_fields(input_with_fields(2), Arc::clone(&shared), 2)),
            right: Box::new(window_fields(input_with_fields(1), Arc::clone(&shared), 1)),
            matching: add(),
        },
        output: evaluated(fields(1), shared),
    };
    assert!(node.validate().is_ok(), "{}", node.validate().unwrap_err());
}

/// Two scalars make a scalar, because there is nothing to attach labels or fields to.
#[test]
fn two_scalars_make_a_scalar() {
    let shared = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(scalar_on(1.0, Arc::clone(&shared))),
            right: Box::new(scalar_on(2.0, Arc::clone(&shared))),
            matching: add(),
        },
        output: NativeType::Scalar(Arc::clone(&shared)),
    };
    assert!(node.validate().is_ok());
}

/// Two scalars have nothing to attach labels or fields to, so they cannot produce a vector.
#[test]
fn two_scalars_cannot_produce_a_vector() {
    let shared = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(scalar_on(1.0, Arc::clone(&shared))),
            right: Box::new(scalar_on(2.0, Arc::clone(&shared))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), shared),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("two scalars do not produce a scalar"),
        "{error}"
    );
}

/// A constant operand is broadcast to each series the other side answers, so such an operation reads
/// one series at a time exactly when that other side does.
///
/// Two vectors are a match over the whole vector, and an aggregate folds a group, so either of them
/// under the constant still reads across series. Two constants answer the same value for every series
/// there is, so they read none.
#[test]
fn a_constant_operand_reads_one_series_at_a_time() {
    let shared = grid(0, 60_000, 4);
    let vector = || window_on(input(), None, Arc::clone(&shared));
    let binary = |left: NativeNode, right: NativeNode| NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: add(),
        },
        output: vector().output,
    };
    let aggregate = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(vector()),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };

    // `100 - rate(v0[5m])`, and the same constant written on the other side.
    for node in [
        binary(scalar_on(100.0, Arc::clone(&shared)), vector()),
        binary(vector(), scalar_on(100.0, Arc::clone(&shared))),
    ] {
        assert!(
            !reads_across_series(&node),
            "a broadcast reaches each series the vector side answers: {node:?}"
        );
    }
    // `sum(rate(v0[5m])) + 100` folds a group first, so the constant is broadcast to each group.
    assert!(reads_across_series(&binary(
        aggregate,
        scalar_on(100.0, Arc::clone(&shared))
    )));
    // `rate(v0[5m]) + rate(v0[5m])` matches two vectors by label.
    assert!(reads_across_series(&binary(vector(), vector())));
    // `1 + 2` answers one value for every series there is.
    assert!(!reads_across_series(&binary(
        scalar_on(1.0, Arc::clone(&shared)),
        scalar_on(2.0, Arc::clone(&shared))
    )));
}

/// A tree needs one execution over every scan partition exactly when one of its operators answers a
/// value for the whole vector; every other tree answers each series on its own and keeps running one
/// execution per partition.
#[test]
fn only_a_whole_vector_tree_needs_one_execution() {
    let shared = grid(0, 60_000, 4);
    let rate = || window_on(input(), None, Arc::clone(&shared));
    let vector = |inner: NativeNode, op: BoundVectorOp| NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(inner),
            op,
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let binary = |left: NativeNode, right: NativeNode| NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let aggregate = |inner: NativeNode, stage: AggregateStage| NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage,
            },
        },
        output: if stage == AggregateStage::Partial {
            partial_aggregate(rate()).output
        } else {
            evaluated(Arc::new([]), Arc::clone(&shared))
        },
    };

    // One series' rows answer one series: the input, `rate(x[5m])` and `abs(rate(x[5m]))`.
    for node in [input(), rate(), map_over(rate())] {
        assert!(
            !needs_single_execution(&node),
            "a per-series tree keeps one execution per partition: {node:?}"
        );
    }
    // A broadcast reaches each series of the vector side, and a state aggregate is contractually
    // one execution's contribution.
    assert!(!needs_single_execution(&binary(
        scalar_on(1.0, Arc::clone(&shared)),
        rate()
    )));
    assert!(!needs_single_execution(&aggregate(
        rate(),
        AggregateStage::Partial
    )));
    // `rate(a) + rate(b)` is a match, not a broadcast, so it is *not* per-series: it pairs the two
    // sides' series by label, and with no boundary facts there is no proof that one execution holds
    // both. This answered `false` while a match was decided by its sides alone, which is the gap the
    // match's own co-location proof closes.
    assert!(needs_single_execution(&binary(rate(), rate())));

    // `topk(1, rate(x[5m]))`, `sort(rate(x[5m]))` and `limitk(1, rate(x[5m]))` read every series
    // against the others, as does a complete aggregate.
    let group = Grouping::By(Arc::new(["job".into()]));
    for op in [
        BoundVectorOp::TopK {
            k: 1.0,
            top: true,
            group: group.clone(),
        },
        BoundVectorOp::LimitK {
            k: 1.0,
            group: group.clone(),
        },
        BoundVectorOp::Sort(SortOrder::Asc),
    ] {
        assert!(
            needs_single_execution(&vector(rate(), op)),
            "a global vector operation needs every series at once"
        );
    }
    assert!(needs_single_execution(&aggregate(
        rate(),
        AggregateStage::Complete
    )));
    // Depth does not hide it: `sum(rate(x[5m])) + 100` still folds a group, and so does a per-series
    // left side over a whole-vector right side.
    assert!(needs_single_execution(&binary(
        aggregate(rate(), AggregateStage::Complete),
        scalar_on(100.0, Arc::clone(&shared))
    )));
    assert!(needs_single_execution(&binary(
        rate(),
        vector(rate(), BoundVectorOp::Sort(SortOrder::Asc))
    )));
}

/// A whole-vector operator reads the input vector itself, so what the boundary says about where the
/// sources sit cannot make one execution unnecessary.
#[test]
fn a_whole_vector_operator_needs_one_execution_whatever_the_boundary() {
    let shared = grid(0, 60_000, 4);
    // `sort(rate(x[5m]))` orders every series against the others, and `sum(rate(x[5m]))` folds every
    // contribution to a group.
    let vector = sorted(input_on(Arc::clone(&shared)));
    let aggregate = complete_aggregate(input_on(Arc::clone(&shared)));

    for boundary in [
        &[][..],
        &[source_pieces()][..],
        &[proven_by_key()][..],
        &[proven_in_one_execution()][..],
    ] {
        assert!(
            needs_single_execution_in(&vector, boundary),
            "a vector operation reads the whole vector: {boundary:?}"
        );
        assert!(
            needs_single_execution_in(&aggregate, boundary),
            "a complete aggregate folds every contribution to its group: {boundary:?}"
        );
        // The wrapper that states no facts agrees, because it proves nothing.
        assert!(needs_single_execution(&vector));
        assert!(needs_single_execution(&aggregate));
    }
}

/// A merge needs every piece of one canonical label set and not every label set in one execution, so
/// the boundary's ownership decides where it is placed - and never how many executions the tree
/// takes, which is its input's answer.
///
/// The fix that split the two questions is what this test now asserts together: a merge over a
/// boundary that proves nothing is not placeable in one partition, and it still does not demand a
/// single execution to make itself placeable. It used to answer the second question with the first,
/// which is what collapsed the caller's partitions.
#[test]
fn a_merge_is_placed_by_the_boundary_and_needs_what_its_input_needs() {
    let shared = grid(0, 60_000, 4);
    let merge = merge_over(input_on(Arc::clone(&shared)));

    // A raw scan partitions by storage `read_key`, which proves nothing about canonical keys: the
    // pieces of one label set can sit in different partitions, and a merge in each of them would
    // hand what reads it a partial series as a canonical one. So it is not placeable there - and it
    // does not answer that by asking for one execution either.
    assert!(!merges_are_placeable_in(&merge, &[source_pieces()], false));
    assert!(!needs_single_execution_in(&merge, &[source_pieces()]));
    // No facts at all prove even less: the same answer, and again not one that costs an execution.
    assert!(!merges_are_placeable_in(&merge, &[], false));
    assert!(!needs_single_execution_in(&merge, &[]));
    // Placed by canonical key, every piece of a label set is in the execution that owns it, so a
    // merge per owner partition is the whole answer.
    assert!(merges_are_placeable_in(&merge, &[proven_by_key()], false));
    // One execution holds every series there is.
    assert!(merges_are_placeable_in(
        &merge,
        &[proven_in_one_execution()],
        false
    ));
    // Whatever the boundary proves, the merge's own need is its input's, which a scan does not
    // raise: one execution is never what a merge asks for.
    for boundary in [
        &[][..],
        &[source_pieces()][..],
        &[proven_by_key()][..],
        &[proven_in_one_execution()][..],
    ] {
        assert!(
            !needs_single_execution_in(&merge, boundary),
            "a merge demanded a single execution: {boundary:?}"
        );
    }

    // What sits above a merge is not proved co-located by what the merge reads: `rate` drops the
    // metric name and can put two sources' label sets in one, so the merge over it cannot read each
    // label set whole in an owner partition - and still does not demand one execution.
    let windowed = merge_over(window_with(
        input_on(Arc::clone(&shared)),
        RangeFunction::Rate,
        Some(Arc::clone(&shared)),
        Arc::clone(&shared),
    ));
    assert!(!merges_are_placeable_in(
        &windowed,
        &[proven_by_key()],
        false
    ));
    assert!(!needs_single_execution_in(&windowed, &[proven_by_key()]));
    // In an execution that reads every input partition, every piece is present and the same merge is
    // placeable: the label sets `rate` invented are still whole there.
    assert!(merges_are_placeable_in(&windowed, &[proven_by_key()], true));

    // A window that keeps the metric name hands the same label sets on, so the proof survives it and
    // the merge stays within the owner partitions.
    let keeping = merge_over(window_with(
        input_on(Arc::clone(&shared)),
        RangeFunction::LastOverTime,
        Some(Arc::clone(&shared)),
        Arc::clone(&shared),
    ));
    assert!(merges_are_placeable_in(&keeping, &[proven_by_key()], false));
    assert!(!needs_single_execution_in(&keeping, &[proven_by_key()]));

    // A complete aggregate below it needs one execution of its own, whatever the merge needs - which
    // is the one case where a tree holding a merge does need one, and it is the aggregate's.
    let folded = merge_over(complete_aggregate(input_on(Arc::clone(&shared))));
    assert!(needs_single_execution_in(
        &folded,
        &[proven_in_one_execution()]
    ));
}

/// The reported shape: one source read twice, a canonical merge over each occurrence, and a match
/// over the two merges.
///
/// `Merge(Input(0))` on both sides is a tree the native API accepts - `collect_source_schemas` takes
/// the same `source_index` in several leaves - and a per-canonical-key placement proves each
/// *canonical* label set whole, which is what each merge needs and is not what the match needs. With
/// `{host="h", zone="a"}` and `{host="h", zone="b"}` under two canonical owners, each owner holds one
/// series per side, matches them locally, and the whole-vector `on(host)` duplicate-series check
/// (`vector_match`) never runs over the real group: a silently wrong answer where an error is owed.
///
/// Before the fix this answered `false`: both sides were `ByCanonicalKey`, which the match arm took
/// as a proof of its own co-location because the arm asked only what each side needs on its own.
#[test]
fn a_match_over_two_canonical_key_merges_needs_one_execution() {
    let shared = grid(0, 60_000, 4);
    let matched = || {
        binary_with(
            merge_over(input_on(Arc::clone(&shared))),
            merge_over(input_on(Arc::clone(&shared))),
            on_host(),
        )
    };

    assert!(
        needs_single_execution_in(&matched(), &[proven_by_key()]),
        "an `on(host)` match pairs by `host`, and a canonical key is not that key"
    );

    // Each side alone is the case the merge already settles: one merge over a per-key placement stays
    // in its owner partition, so the `true` above is the match's requirement and not the merges'.
    let side = || merge_over(input_on(Arc::clone(&shared)));
    assert!(!needs_single_execution_in(&side(), &[proven_by_key()]));
}

/// The same match in an execution that holds every series there is: both sides are already here, so
/// the match needs nothing beyond it and the tree keeps running as the one execution it is.
#[test]
fn a_match_over_two_merges_in_one_execution_stays_parallel() {
    let shared = grid(0, 60_000, 4);
    let matched = || {
        binary_with(
            merge_over(input_on(Arc::clone(&shared))),
            merge_over(input_on(Arc::clone(&shared))),
            on_host(),
        )
    };

    assert!(
        !needs_single_execution_in(&matched(), &[proven_in_one_execution()]),
        "one execution already pairs every series of the `on(host)` group"
    );
}

/// A set operation matches two vectors the same way a binary one does, so the boundary answers the
/// same way for it: `ByCanonicalKey` is not a matching-key proof, and one execution needs no proof.
#[test]
fn a_set_over_two_canonical_key_merges_needs_one_execution() {
    let shared = grid(0, 60_000, 4);
    let set = || NativeNode {
        expr: NativeExpr::Set {
            left: Box::new(merge_over(input_on(Arc::clone(&shared)))),
            right: Box::new(merge_over(input_on(Arc::clone(&shared)))),
            op: BoundSetOp {
                op: SetOp::Or,
                labels: MatchingLabels::On(vec!["host".into()]),
            },
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };

    assert!(
        needs_single_execution_in(&set(), &[proven_by_key()]),
        "`or on(host)` pairs by `host`, and a canonical key is not that key"
    );
    assert!(
        !needs_single_execution_in(&set(), &[proven_in_one_execution()]),
        "one execution already pairs every series of the `on(host)` group"
    );
}

/// A broadcast is not a match: it pairs a vector with one value, so no label set is paired on the
/// constant's side and nothing about matching keys has to be proved for it. The fix for the match
/// must therefore not collapse `x > 0` or `x * 2` into one execution.
#[test]
fn a_scalar_broadcast_over_a_canonical_key_vector_stays_parallel() {
    let shared = grid(0, 60_000, 4);
    let by_key = [proven_by_key()];
    let vector = || input_on(Arc::clone(&shared));
    let constant = || scalar_on(0.0, Arc::clone(&shared));

    // `x > 0`, `x * 2` and the same two written with the constant on the left, all over a vector side
    // placed by canonical key: each is one value against each series, not a pair of series.
    for node in [
        binary_with(vector(), constant(), matching(BinaryOp::Gt, false)),
        binary_with(vector(), constant(), matching(BinaryOp::Mul, false)),
        binary_with(constant(), vector(), matching(BinaryOp::Lt, false)),
        binary_with(constant(), vector(), matching(BinaryOp::Mul, false)),
    ] {
        assert!(
            !needs_single_execution_in(&node, &by_key),
            "a broadcast reaches each series of its vector side on its own:\n{node:?}"
        );
    }
}

/// A merge is grouped by the canonical label set, so a per-key boundary is exactly the proof it needs
/// and it must keep staying within the owner partitions the fix above does not touch.
#[test]
fn a_merge_over_canonical_keys_still_keeps_the_owner_partitions() {
    let shared = grid(0, 60_000, 4);
    let merge = merge_over(input_on(Arc::clone(&shared)));

    assert!(
        !needs_single_execution_in(&merge, &[proven_by_key()]),
        "a canonical key is what a merge groups by, so it is a proof of a merge's input"
    );
    assert!(
        merges_are_placeable_in(&merge, &[proven_by_key()], false),
        "the proof a canonical key gives is the one a merge is placed by"
    );
    // The contrast that keeps the assertion above from being vacuous: the same merge over a scan that
    // proves nothing about canonical keys is not placeable in one partition. It does not answer that
    // by needing a single execution - a merge's own need is its input's, which a scan does not raise -
    // so the unproved shape is declined rather than collapsed.
    assert!(!needs_single_execution_in(&merge, &[source_pieces()]));
    assert!(!merges_are_placeable_in(&merge, &[source_pieces()], false));
}

/// A per-series wrapper composes with the match's requirement by recursing into it: `rate` over the
/// match needs what the match needs and adds no requirement of its own, so the answer is the match's.
#[test]
fn a_window_over_a_match_hands_the_matches_requirement_up_unchanged() {
    let shared = grid(0, 60_000, 4);
    let matched = || {
        binary_with(
            merge_over(input_on(Arc::clone(&shared))),
            merge_over(input_on(Arc::clone(&shared))),
            on_host(),
        )
    };
    // `rate(<match>)`, with a name-dropping function, so the window itself changes the label sets the
    // match pairs and cannot add a co-location proof of its own either way.
    let windowed = || {
        window_with(
            matched(),
            RangeFunction::Rate,
            Some(Arc::clone(&shared)),
            Arc::clone(&shared),
        )
    };

    // One execution holds both sides, so the match is whole there and the window over it needs
    // nothing more.
    assert!(!needs_single_execution_in(
        &windowed(),
        &[proven_in_one_execution()]
    ));
    // Placed by canonical key, the window cannot make the match whole: what it hands on is the
    // match's requirement, one execution.
    assert!(needs_single_execution_in(&windowed(), &[proven_by_key()]));
}

/// A tree whose only whole-vector-looking node is a merge is placed per partition once the boundary
/// proves the pieces of a label set arrive together, which is the case that must stop collapsing the
/// owner partitions a canonical repartition just created.
///
/// The answers below are the split the fix made: the tree never needs one execution, because neither
/// the window nor the merge raises that requirement, and whether it can run per partition is what the
/// boundary's facts decide. The last case is the shape that used to be collapsed - unproved pieces
/// under a merge - and it is now declined instead: `merges_are_placeable_in` says the one partition
/// the node would run in cannot answer it.
#[test]
fn a_merge_alone_does_not_collapse_the_owner_partitions() {
    let shared = grid(0, 60_000, 4);
    // The shape a repartition hands on: a canonical merge under a per-series window, and no
    // whole-vector operator anywhere.
    let tree = window_with(
        merge_over(input_on_at(1, Arc::clone(&shared))),
        RangeFunction::LastOverTime,
        Some(Arc::clone(&shared)),
        Arc::clone(&shared),
    );

    // Placed by canonical key, the pieces of the label set source 1 reads are already together in the
    // partition that owns them.
    for boundary in [
        [proven_by_key(), proven_by_key()],
        [source_pieces(), proven_by_key()],
        [proven_in_one_execution(), source_pieces()],
    ] {
        assert!(
            !needs_single_execution_in(&tree, &boundary),
            "a merge under a per-series window demanded one execution: {boundary:?}"
        );
    }
    assert!(
        merges_are_placeable_in(&tree, &[proven_by_key(), proven_by_key()], false),
        "the merge reads source 1, whose pieces the boundary proves co-located"
    );
    assert!(merges_are_placeable_in(
        &tree,
        &[source_pieces(), proven_by_key()],
        false
    ));
    // The entries are read per source: source 1 is the one the merge reads, so leaving it unproved
    // is what makes the tree unplaceable per partition, however canonical source 0 is.
    assert!(!merges_are_placeable_in(
        &tree,
        &[proven_in_one_execution(), source_pieces()],
        false
    ));
    // The wrapper that states no facts proves nothing about source 1 either, so the answer is the
    // same - and it is still not one execution.
    assert!(!merges_are_placeable_in(&tree, &[], false));
    assert!(!needs_single_execution(&tree));
}

/// The ownership a broadcast derives is what decides where a merge above it is placed, so deriving
/// an arithmetic constant's facts into the vector's had a placement consequence and not only a fact
/// wrong on paper: `x > 0` keeps the label set it read, so a vector side placed by canonical key
/// still proves that the pieces of a label set arrive together and a merge over the comparison stays
/// within the owner partitions a canonical repartition just created.
///
/// The other comparisons below are what keep the assertions from being vacuous: arithmetic and a
/// `bool` comparison drop the metric name, so a merge over the same vector side cannot read each
/// label set whole there, and neither can a merge over a tree whose facts are the ones the old
/// formula derived - the vector side united with the constant's `no_label_sets()`, whose ownership is
/// `Unknown`. All of them are placement answers: the merge's own need is its input's throughout, so
/// none of these shapes is turned into a single execution by the merge.
#[test]
fn a_merge_over_a_filtering_comparison_keeps_the_owner_partitions() {
    let shared = grid(0, 60_000, 4);
    let by_key = [proven_by_key()];
    let vector = || input_on(Arc::clone(&shared));
    let constant = || scalar_on(0.0, Arc::clone(&shared));
    // `x > 0`, which keeps the label set it read, and `0 < x`, the same query written the other way.
    let filtering = |vector_left: bool| {
        if vector_left {
            binary_with(vector(), constant(), matching(BinaryOp::Gt, false))
        } else {
            binary_with(constant(), vector(), matching(BinaryOp::Lt, false))
        }
    };

    for node in [filtering(true), filtering(false)] {
        let merged = merge_over(node);
        assert!(
            merges_are_placeable_in(&merged, &by_key, false),
            "a broadcast that keeps the label set leaves the merge's pieces co-located"
        );
        assert!(
            !needs_single_execution_in(&merged, &by_key),
            "a merge over a broadcast demanded one execution of its own"
        );
    }

    // The contrast: a label set the broadcast rewrote is not the one the boundary's keys name, so
    // the merge above it cannot read each label set whole - and it still does not ask for one
    // execution to get them.
    for node in [
        binary_with(vector(), constant(), matching(BinaryOp::Mul, false)),
        binary_with(constant(), vector(), matching(BinaryOp::Mul, false)),
        binary_with(vector(), constant(), matching(BinaryOp::Gt, true)),
        binary_with(constant(), vector(), matching(BinaryOp::Lt, true)),
    ] {
        let merged = merge_over(node);
        assert!(
            !merges_are_placeable_in(&merged, &by_key, false),
            "a broadcast that drops the metric name leaves the merge's pieces unproved"
        );
        assert!(!needs_single_execution_in(&merged, &by_key));
    }

    // The old answer, reproduced through the facts it derived: a vector side placed by canonical key
    // united with the constant's facts owns nothing it can name, so a merge over a tree whose vector
    // side proves that is not placeable in one owner partition. That is the answer the fix removes
    // from the co-located case above.
    assert_eq!(
        proven_by_key().united(no_label_sets()).owner,
        CanonicalOwner::Unknown
    );
    assert!(!merges_are_placeable_in(
        &merge_over(filtering(true)),
        &[proven_by_key().united(no_label_sets())],
        false
    ));
    assert!(!needs_single_execution_in(
        &merge_over(filtering(true)),
        &[proven_by_key().united(no_label_sets())]
    ));
}

/// A binary operation is point-wise on a grid, so a side of raw samples has no positions to line
/// up with the other side.
#[test]
fn a_binary_operation_cannot_consume_raw_samples() {
    let shared = grid(0, 60_000, 4);
    let vector = || window_on(input(), None, Arc::clone(&shared));

    let left_raw = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(input()),
            right: Box::new(vector()),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let error = left_raw.validate().unwrap_err();
    assert!(
        error.to_string().contains("not raw samples on its left"),
        "{error}"
    );

    let right_raw = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(vector()),
            right: Box::new(input()),
            matching: add(),
        },
        output: evaluated(Arc::new([]), shared),
    };
    let error = right_raw.validate().unwrap_err();
    assert!(
        error.to_string().contains("not raw samples on its right"),
        "{error}"
    );
}

/// State is not a value, so neither side of a binary operation may be state.
#[test]
fn a_binary_operation_cannot_consume_state() {
    let shared = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_on(input(), None, Arc::clone(&shared))),
            right: Box::new(partial_aggregate(window_on(
                input(),
                None,
                Arc::clone(&shared),
            ))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("aggregate state"), "{error}");
}

/// A partial aggregate produces state, not a value; a complete one produces a value on the same
/// grid as its input.
#[test]
fn an_aggregate_output_matches_its_stage() {
    let inner = window_over(input(), None);
    let value_grid = inner.output.grid().unwrap().clone();

    let complete = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner.clone()),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), Arc::clone(&value_grid)),
    };
    assert!(complete.validate().is_ok());

    let partial = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Grouping::By(Arc::new(["job".into()])),
            accumulators: Arc::new([]),
            grid: value_grid,
        })),
    };
    assert!(partial.validate().is_ok());
}

/// A complete aggregate folds each field on its own, so it reports one value per input field
/// (`aggregate::aggregate_float` builds its rows from the input schema's fields).
#[test]
fn a_complete_aggregate_cannot_change_the_field_count() {
    let out = grid(0, 60_000, 4);
    // The input is a legal two-field window; the aggregate declares one value.
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_fields(input_with_fields(2), Arc::clone(&out), 2)),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(fields(1), out),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a complete aggregate declares 1 field(s) but its input provides 2"),
        "{error}"
    );
}

/// Declaring a value where the stage produces state is a compiler mistake, and is rejected.
#[test]
fn an_aggregate_stage_must_agree_with_its_type() {
    let inner = window_over(input(), None);
    let value_grid = inner.output.grid().unwrap().clone();
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: evaluated(Arc::new([]), value_grid),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("produces aggregate state"),
        "{error}"
    );
}

/// A `Partial` aggregate keeps one accumulator per input field (`aggregate_float` lays its state
/// out in `schema.fields.len()`-sized points), so state declaring another count is rejected.
#[test]
fn a_partial_aggregate_cannot_change_the_accumulator_count() {
    let out = grid(0, 60_000, 4);
    // The input is a legal two-field window; the state declares one accumulator.
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_fields(input_with_fields(2), Arc::clone(&out), 2)),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Grouping::By(Arc::new(["job".into()])),
            accumulators: fields(1),
            grid: out,
        })),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a partial aggregate declares 1 field(s) but its input provides 2"),
        "{error}"
    );
}

/// A `Partial` aggregate's state is grouped by the binding's group labels, so state declaring
/// other labels is not the state this aggregate produces.
#[test]
fn a_partial_aggregate_groups_its_state_like_its_binding() {
    let mut node = partial_aggregate(window_over(input(), None));
    let NativeExpr::Aggregate { op, .. } = &mut node.expr else {
        unreachable!("a partial aggregate tree has an aggregate root")
    };
    op.group = Grouping::By(Arc::new(["instance".into()]));
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("groups differ from the binding"),
        "{error}"
    );
}

/// A `Partial` aggregate indexes its state on the grid its input produces, so state on another
/// grid is addressed with positions the input never wrote.
#[test]
fn a_partial_aggregate_indexes_its_state_on_the_inputs_grid() {
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_over(input(), None)),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Grouping::By(Arc::new(["job".into()])),
            accumulators: Arc::new([]),
            grid: grid(0, 30_000, 8),
        })),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("indexed on a grid the input does not produce"),
        "{error}"
    );
}

/// A `Final` aggregate merges into the binding's group labels, so the state it reads has to be
/// grouped the same way.
#[test]
fn a_final_aggregate_groups_its_state_like_its_binding() {
    let mut node = final_aggregate(partial_aggregate(window_over(input(), None)));
    let NativeExpr::Aggregate { op, .. } = &mut node.expr else {
        unreachable!("a final aggregate tree has an aggregate root")
    };
    op.group = Grouping::By(Arc::new(["instance".into()]));
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("groups differ from the binding"),
        "{error}"
    );
}

/// `by` and `without` are not interchangeable, even over the same label.
///
/// `by(job)` keeps `job` and drops every other label; `without(job)` drops `job` and keeps the
/// rest. A binding that spelled `without` as a label list could only produce the first, so state
/// built for one has to be refused for the other rather than merged under the wrong labels.
#[test]
fn a_grouping_cannot_swap_by_for_without() {
    let mut node = final_aggregate(partial_aggregate(window_over(input(), None)));
    let NativeExpr::Aggregate { op, .. } = &mut node.expr else {
        unreachable!("a final aggregate tree has an aggregate root")
    };
    op.group = Grouping::Without(Arc::new(["job".into()]));
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("groups differ from the binding"),
        "{error}"
    );

    // The same labels under the same shape validate, so the refusal is about the shape.
    let mut node = final_aggregate(partial_aggregate(window_over(input(), None)));
    let NativeExpr::Aggregate { op, .. } = &mut node.expr else {
        unreachable!("a final aggregate tree has an aggregate root")
    };
    op.group = Grouping::By(Arc::new(["job".into()]));
    assert!(node.validate().is_ok());
}

/// A `Final` aggregate consumes the state a `Partial` produced, which is what an exchange
/// boundary hands over between the two stages.
#[test]
fn a_final_aggregate_merges_partial_state() {
    let shared = grid(0, 60_000, 4);
    let state = partial_aggregate(window_on(input(), None, Arc::clone(&shared)));
    let merged = final_aggregate(state);
    assert!(merged.validate().is_ok());
    assert!(
        matches!(&merged.output, NativeType::Samples(schema) if schema.eval_grid.as_ref().is_some_and(|grid| Arc::ptr_eq(grid, &shared)))
    );
}

/// A `Final` aggregate merges the state into one value per accumulator, so it reports the state's
/// count and nothing else.
#[test]
fn a_final_aggregate_cannot_change_the_field_count() {
    let out = grid(0, 60_000, 4);
    // A legal partial aggregate: two input fields, two accumulators.
    let state = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_fields(input_with_fields(2), Arc::clone(&out), 2)),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Grouping::By(Arc::new(["job".into()])),
            accumulators: fields(2),
            grid: Arc::clone(&out),
        })),
    };
    assert!(
        state.validate().is_ok(),
        "{}",
        state.validate().unwrap_err()
    );

    // The merge declares one value for the state's two accumulators.
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(state),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Final,
            },
        },
        output: evaluated(fields(1), out),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a final aggregate declares 1 field(s) but its input provides 2"),
        "{error}"
    );
}

/// A `Final` aggregate consumes state, so a vector is not something it can merge.
#[test]
fn a_final_aggregate_cannot_consume_a_window() {
    let inner = window_over(input(), None);
    let value_grid = inner.output.grid().unwrap().clone();
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Final,
            },
        },
        output: evaluated(Arc::new([]), value_grid),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("consumes aggregate state"),
        "{error}"
    );
}

/// A `Complete` aggregate aggregates vectors, so state is not something it can consume.
#[test]
fn a_complete_aggregate_cannot_consume_state() {
    let state = partial_aggregate(window_over(input(), None));
    let state_grid = state_grid(&state);
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(state),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), state_grid),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("consumes an evaluated vector"),
        "{error}"
    );
}

/// The first-phase walkthrough shapes compile to trees, which is the acceptance the RFC states
/// for composition.
#[test]
fn the_walkthrough_shapes_are_trees() {
    // Every node in one query evaluates on the statement's grid; `validate` compares the specs of
    // the grids, not their allocations, which is the rule the kernels apply.
    let statement_grid = grid(0, 60_000, 4);

    // `abs(rate(x[5m]))`
    let abs_rate = map_over(window_over(input(), None));
    assert!(abs_rate.validate().is_ok());

    // `sum by(job)(rate(x[5m]))`, complete because one execution sees every contribution.
    let sum = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_on(input(), None, Arc::clone(&statement_grid))),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), Arc::clone(&statement_grid)),
    };
    assert!(sum.validate().is_ok());

    // `sum by(job)(rate(x[5m])) + 1`: the aggregate is complete, and the scalar side is
    // broadcast, so `matching` is not consulted for it.
    let plus_one = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(sum.clone()),
            right: Box::new(scalar_on(1.0, Arc::clone(&statement_grid))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), statement_grid),
    };
    assert!(plus_one.validate().is_ok());
}

/// A scalar node's output is the scalar type, which is what keeps a broadcast from turning into
/// label matching.
#[test]
fn a_scalar_node_is_valid() {
    let node = scalar_on(1.0, grid(0, 60_000, 4));
    assert!(node.validate().is_ok());
}

/// A scalar node declared as a vector would let a broadcast be planned as a match.
#[test]
fn a_scalar_node_must_produce_a_scalar() {
    let node = NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value: 1.0 }),
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("does not produce a scalar"),
        "{error}"
    );
}

/// An input whose declared output is not the binding it was bound to cannot be trusted.
#[test]
fn an_input_must_report_the_binding_it_was_bound_to() {
    let node = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: raw(),
            source_index: 0,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output: raw(),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("not the binding"), "{error}");
}

/// An exchange boundary binds state, and the node has to report that same state.
#[test]
fn an_input_can_bind_aggregate_state() {
    let state = partial_aggregate(window_over(input(), None));
    let bound = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: state.output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Contiguous,
        }),
        output: state.output.clone(),
    };
    assert!(bound.validate().is_ok());

    // A `Final` aggregate can then consume it, which is the shape of a distributed aggregate.
    assert!(final_aggregate(bound).validate().is_ok());

    // A fresh state allocation is a different binding, so it cannot be reported as the bound one.
    let other = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: state.output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Contiguous,
        }),
        output: partial_aggregate(window_over(input(), None)).output,
    };
    let error = other.validate().unwrap_err();
    assert!(error.to_string().contains("not the binding"), "{error}");
}

/// A vector operation reads the values at each grid point, so raw samples, which have no shared points,
/// are not something it can consume.
#[test]
fn a_vector_operation_cannot_consume_raw_samples() {
    let node = NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(input()),
            op: BoundVectorOp::LimitRatio { ratio: 0.5 },
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("an evaluated vector"), "{error}");
}

/// A node that reports samples while holding no input node is refused where the node is built.
///
/// The tree is constructed directly here — a child module of `native::plan` may — because the node
/// is one the public constructors decline: a scalar root is the only zero-input shape a fused node
/// accepts, and samples without a source would be an empty vector reported as an answer. Every
/// public constructor therefore stays a checked one.
#[test]
fn a_samples_root_without_an_input_is_refused() {
    let statement_grid = grid(0, 60_000, 4);
    let tree = NativeTree {
        root: NativeNode {
            expr: NativeExpr::Scalar(BoundScalar { value: 1.0 }),
            output: evaluated(Arc::new([]), Arc::clone(&statement_grid)),
        },
    };
    let binding = FusedSeriesBinding {
        grid: statement_grid,
        time_index: "time".to_string(),
        tags: vec![],
        metric_name: None,
    };

    let error = FusedSeries::try_new(
        Arc::from("1"),
        tree,
        binding,
        Vec::new(),
        Arc::new(DFSchema::empty()),
    )
    .expect_err("a samples root reads no source while reporting samples");

    assert!(
        error.to_string().contains("constant scalar"),
        "the refusal must name the shape a zero-child tree has to be: {error}"
    );
}

/// `info` reads its info side's values as sample times, so it accepts only the instant selection
/// that reports them: one that reports the samples themselves would compare every info series as
/// equally new.
#[test]
fn info_reads_the_sample_times_of_one_field() {
    let out = grid(0, 60_000, 4);
    let info_of = |value: InstantValue, fields: usize| NativeNode {
        expr: NativeExpr::Info {
            base: Box::new(window_fields(input_with_fields(1), Arc::clone(&out), 1)),
            info: Box::new(NativeNode {
                expr: NativeExpr::Instant {
                    input: Box::new(input_with_fields(fields)),
                    op: BoundInstant {
                        lookback_ms: 300_000,
                        offset_ms: 0,
                        at_ms: None,
                        value,
                        output_grid: Arc::clone(&out),
                    },
                },
                output: evaluated(self::fields(fields), Arc::clone(&out)),
            }),
            selector: InfoSelector {
                name_matchers: Vec::new(),
                data_labels: Vec::new(),
                keep_unmatched: true,
            },
        },
        output: evaluated(self::fields(1), Arc::clone(&out)),
    };
    info_of(InstantValue::InfoTimestamp, 1).validate().unwrap();
    for (value, fields, expected) in [
        (InstantValue::Sample, 1, "not their values"),
        (InstantValue::InfoTimestamp, 2, "carry 2 fields"),
    ] {
        let error = info_of(value, fields).validate().unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

/// What a child that routes its chunks by canonical label set proves: this execution holds every
/// piece of the label sets it names, and no other output holds a fragment of one of them.
fn routed_by_canonical_key() -> NativeProperties {
    NativeProperties {
        owner: CanonicalOwner::ByCanonicalKey,
        grouping: CanonicalGrouping::SourcePiecesOnly,
    }
}

/// A `topk` over `inner`, which reads the whole vector at each grid point.
fn topk_over(inner: NativeNode) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(inner),
            op: BoundVectorOp::TopK {
                k: 1.0,
                top: true,
                group: Grouping::By(Arc::new([])),
            },
        },
        output,
    }
}

/// A merge inherits what its input needs from an execution and demands nothing of its own.
///
/// This is what the old answer got wrong: a merge whose pieces no boundary proves co-located used
/// to answer `true`, which the fused node turned into one execution over every input partition. The
/// merge does not need that - given the pieces it answers one series per label set - and what it
/// cannot do in a per-partition execution is a fact about the placement, not a reason to take the
/// caller's parallelism away. The two boundaries below are the two ways a boundary proves nothing:
/// an empty one, where the caller said nothing, and [`source_pieces`], which is a scan's own facts.
#[test]
fn a_merge_inherits_and_never_demands_a_single_execution() {
    let merge = merge_over(input_on(grid(0, 60_000, 4)));
    for boundary in [Vec::new(), vec![source_pieces()]] {
        assert!(
            !needs_single_execution_in(&merge, &boundary),
            "a merge demanded a single execution of a boundary that proves nothing"
        );
        // ... and it is nevertheless not placeable per partition: that is the other answer, and it
        // is the one the scan's boundary gives.
        assert!(
            !merges_are_placeable_in(&merge, &boundary, false),
            "a merge over an unproven boundary was placed in a single partition"
        );
    }
    // One execution reads every input partition, so every piece is present and the merge is
    // placeable there whatever the boundary proves.
    assert!(merges_are_placeable_in(&merge, &[], true));

    // What the merge does inherit is its input's need: a `topk` below it still reads the whole
    // vector, so the tree needs one execution for the `topk` and not for the merge.
    let ranked = merge_over(topk_over(input_on(grid(0, 60_000, 4))));
    assert!(
        needs_single_execution_in(&ranked, &[]),
        "the `topk` below the merge no longer needs one execution"
    );
    assert!(merges_are_placeable_in(&ranked, &[], true));
}

/// A merge over a child that routes pieces by canonical key keeps the partitions it was given.
///
/// The proof arrives in the boundary and not in a single execution: such a child hands every piece
/// of each label set it names to one partition and no other partition a fragment of them, so the
/// merge reads each label set whole inside the partition it runs in. This is the legal parallel path,
/// where the node is neither collapsed to one execution nor refused, and it is why a merge no longer
/// answers the "needs one execution" question with its own placement.
#[test]
fn a_merge_over_a_repartitioning_child_keeps_its_partitions() {
    let merge = merge_over(input_on(grid(0, 60_000, 4)));
    let boundary = [routed_by_canonical_key()];
    assert!(
        !needs_single_execution_in(&merge, &boundary),
        "the merge demanded a single execution of a boundary that already proves co-location"
    );
    assert!(
        merges_are_placeable_in(&merge, &boundary, false),
        "the merge was not placeable where the pieces are co-located"
    );
    // The proof is about the label sets the child names, so it survives an operator that leaves the
    // identity alone and stops at one that does not (the next test).
    assert!(merges_are_placeable_in(&merge, &boundary, false));
}

/// A proof about the label sets a child names does not survive a rewrite of those label sets.
///
/// `rate` drops the metric name, so `rate(a)` and `rate(b)` coincide on the labels they have left
/// where `a` and `b` did not: the window invents label sets the boundary's keys do not name, and a
/// merge over what it answers reads the pieces of a label set the proof never covered. The merge is
/// then not placeable per partition - and, again, it is not a reason to run the tree in one
/// execution either: the shape is declined instead.
#[test]
fn a_label_rewrite_under_a_repartition_invalidates_the_merge_proof() {
    let boundary = [routed_by_canonical_key()];
    let rewritten = merge_over(window_over(input(), None));
    assert!(
        !needs_single_execution_in(&rewritten, &boundary),
        "a merge demanded a single execution because its proof was lost"
    );
    assert!(
        !merges_are_placeable_in(&rewritten, &boundary, false),
        "the proof survived a window that drops the metric name"
    );
    // A map that drops the name loses it the same way, so losing the proof is not about windows.
    let mapped = merge_over(map_over(input_on(grid(0, 60_000, 4))));
    assert!(!merges_are_placeable_in(&mapped, &boundary, false));
    // A merge of a merge is checked at every level, so the recursion does not stop at the outer
    // merge: the rewrite takes the proof away and neither level has it.
    let nested = merge_over(merge_over(window_over(input(), None)));
    assert!(!merges_are_placeable_in(&nested, &boundary, false));
}

/// A canonical merge above the instant selection preserves the sample times it was handed, so the
/// check looks through it; an operation that computes values is not looked through, because a
/// sample's value is not a sample's time and reading one as the other would compare values.
///
/// The merge is what the insertion pass puts over the info side of a legal `info` tree, so a check
/// that did not look through it rejected the tree the pass had just built.
#[test]
fn info_reads_the_sample_times_of_a_merged_selection_and_nothing_else() {
    let out = grid(0, 60_000, 4);
    let selection = |value: InstantValue| NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(input_with_fields(1)),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(1), Arc::clone(&out)),
    };
    let info_of = |info: NativeNode| NativeNode {
        expr: NativeExpr::Info {
            base: Box::new(window_fields(input_with_fields(1), Arc::clone(&out), 1)),
            info: Box::new(info),
            selector: InfoSelector {
                name_matchers: Vec::new(),
                data_labels: Vec::new(),
                keep_unmatched: true,
            },
        },
        output: evaluated(fields(1), Arc::clone(&out)),
    };

    // The merged selection reports sample times, and the merge hands them on unchanged.
    let merged = merge_over(selection(InstantValue::InfoTimestamp));
    assert!(
        info_of(merged.clone()).validate().is_ok(),
        "a canonical merge over the info side rejected the tree the pass builds: {}",
        info_of(merged).validate().unwrap_err()
    );
    // Merges nest, and the check looks through every one of them.
    assert!(
        info_of(merge_over(merged.clone())).validate().is_ok(),
        "a merge of a merge over the info side was not looked through"
    );

    // A merge over values is not a set of sample times, so what the merge wraps is what decides:
    // the selection is still read there, but a node that computes values is not an instant
    // selection and is refused.
    let selected = selection(InstantValue::InfoTimestamp);
    let value_source = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(selected.clone()),
            op: BoundMap {
                ops: Arc::new([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: selected.output.clone(),
    };
    let error = info_of(merge_over(value_source)).validate().unwrap_err();
    assert!(
        error.to_string().contains("instant selection"),
        "values were read as sample times: {error}"
    );

    // A merge over the samples themselves is refused for the same reason, whether or not it is
    // merged: the check unwraps and then requires the sample times.
    let error = info_of(merge_over(selection(InstantValue::Sample)))
        .validate()
        .unwrap_err();
    assert!(
        error.to_string().contains("not their values"),
        "a selection of samples was read as sample times: {error}"
    );
}

/// The metric-name fact each shape answers, and the kernel each answer rests on.
///
/// The shapes whose output carries no name are the ones that write it away whatever they read, the
/// shapes that only carry a label set through follow their input, and the shapes whose output labels
/// are decided elsewhere answer `None`, which is not an answer in either direction.
#[test]
fn the_metric_name_fact_follows_what_each_shape_does_to_a_label_set() {
    let shared = grid(0, 60_000, 4);
    let selected = || instant_over(raw_input_at(0), InstantValue::Sample, Arc::clone(&shared));

    // A scan of one metric hands its series over under that metric's name. A value another execution
    // produced carries whatever that execution left on it, and this tree does not hold that
    // execution, so the answer is `None` rather than a guess in either direction: reading it as "no
    // name" is what would make a named match look name-free and run it per partition. That is why the
    // match proof takes the fact on [`CoPartitionedPair::name_free`], from the layer that built the
    // producers, instead of deriving it here.
    assert_eq!(output_has_metric_name(&raw_input_at(0)), Some(true));
    assert_eq!(
        output_has_metric_name(&input_on_at(0, Arc::clone(&shared))),
        None
    );

    // A selection answers the label set it read, except `timestamp(x)`, which rebuilds the labels
    // through `drop_metric_name`. Over an evaluated input the same selection follows it into `None`.
    assert_eq!(output_has_metric_name(&selected()), Some(true));
    assert_eq!(
        output_has_metric_name(&instant_over(
            raw_input_at(0),
            InstantValue::InfoTimestamp,
            Arc::clone(&shared)
        )),
        Some(true)
    );
    assert_eq!(
        output_has_metric_name(&instant_over(
            raw_input_at(0),
            InstantValue::Timestamp,
            Arc::clone(&shared)
        )),
        Some(false)
    );
    assert_eq!(
        output_has_metric_name(&instant_over(
            input_on_at(0, Arc::clone(&shared)),
            InstantValue::Sample,
            Arc::clone(&shared)
        )),
        None
    );

    // A window drops the name with every function but the two that hand the sample they read on.
    assert_eq!(
        output_has_metric_name(&window_with(
            raw_input_at(0),
            RangeFunction::Rate,
            None,
            Arc::clone(&shared)
        )),
        Some(false)
    );
    assert_eq!(
        output_has_metric_name(&window_with(
            raw_input_at(0),
            RangeFunction::LastOverTime,
            None,
            Arc::clone(&shared)
        )),
        Some(true)
    );

    // An aggregate answers its group key: `by(ns)` and `without(...)` drop `__name__`, and
    // `by("__name__")` keeps the name the input had.
    assert_eq!(
        output_has_metric_name(&sum_by(selected(), by(&["ns"]))),
        Some(false)
    );
    assert_eq!(
        output_has_metric_name(&sum_by(selected(), by(&["__name__"]))),
        Some(true)
    );
    assert_eq!(
        output_has_metric_name(&sum_by(selected(), without(&["ns"]))),
        Some(false)
    );

    // A timestamp reports a position under the name dropped, and a merge writes no label of its own.
    assert_eq!(
        output_has_metric_name(&timestamp_of(selected())),
        Some(false)
    );
    assert_eq!(output_has_metric_name(&merge_over(selected())), Some(true));

    // A map of `map_math`/`map_native_histogram` drops the name, while a label rewrite writes a label
    // of its own choosing - `__name__` included - and can clear it.
    let mapped = map_over(selected());
    assert_eq!(output_has_metric_name(&mapped), Some(false));
    assert_eq!(
        output_has_metric_name(&map_rewriting_labels(selected())),
        None
    );

    // `absent` builds its labels from the argument's matchers and binds no name; `sort` hands back the
    // series it read.
    let absent = NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(selected()),
            op: BoundVectorOp::Absent {
                labels: SeriesLabels::try_new(None, Vec::new()).expect("an empty label set"),
            },
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    assert_eq!(output_has_metric_name(&absent), Some(false));
    assert_eq!(output_has_metric_name(&sorted(selected())), Some(true));

    // A two-vector match drops the name for arithmetic and for a `bool` comparison, keeps the many
    // side's labels for a filtering comparison under the default modifiers, and drops them under
    // `on(host)`, which keeps only the labels it names.
    let two_vectors = |modifiers: BinaryMatching| binary_with(selected(), selected(), modifiers);
    assert_eq!(output_has_metric_name(&two_vectors(add())), Some(false));
    assert_eq!(
        output_has_metric_name(&two_vectors(matching(BinaryOp::Gt, false))),
        Some(true)
    );
    assert_eq!(
        output_has_metric_name(&two_vectors(matching(BinaryOp::Gt, true))),
        Some(false)
    );
    assert_eq!(output_has_metric_name(&two_vectors(on_host())), Some(false));

    // A broadcast answers the vector side's series, and the operation decides whether the labels
    // survive it.
    let constant = || scalar_on(2.0, Arc::clone(&shared));
    assert_eq!(
        output_has_metric_name(&binary_with(
            selected(),
            constant(),
            matching(BinaryOp::Mul, false)
        )),
        Some(false)
    );
    assert_eq!(
        output_has_metric_name(&binary_with(
            selected(),
            constant(),
            matching(BinaryOp::Gt, false)
        )),
        Some(true)
    );

    // A set operation keeps each side's samples, so it answers only when both sides agree.
    let set = |left: NativeNode, right: NativeNode| NativeNode {
        expr: NativeExpr::Set {
            left: Box::new(left),
            right: Box::new(right),
            op: BoundSetOp {
                op: SetOp::Or,
                labels: MatchingLabels::Ignoring(Vec::new()),
            },
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    assert_eq!(
        output_has_metric_name(&set(selected(), selected())),
        Some(true)
    );
    assert_eq!(
        output_has_metric_name(&set(selected(), sum_by(selected(), by(&["ns"])))),
        None
    );
}

/// The `m1{ns="a"} / m2{ns="a"}` match: two canonical label sets and one match group.
///
/// Both sides are selections over raw scans, so every series carries its metric's name, and the
/// canonical key a placement routes by holds `__name__` while the default match key does not. A
/// co-partitioned pair of sources therefore proves nothing about this match: routing by canonical key
/// sends `m1{ns="a"}` and `m2{ns="a"}` to different partitions, each partition finds no partner, and
/// the match answers nothing where the whole match answers a value. That is the silent-empty-answer
/// case, and the two-sided proof must not touch it.
///
/// The pair below states the producer half as `false` because both scans hand over named series, which
/// is what a caller that could observe the producers would state. The pairing itself is real, so what
/// refuses the shape is the naming fact and nothing else.
#[test]
fn a_match_of_two_named_sides_needs_one_execution_whatever_the_routing_proved() {
    let shared = grid(0, 60_000, 4);
    let matched = || {
        binary_with(
            instant_over(raw_input_at(0), InstantValue::Sample, Arc::clone(&shared)),
            instant_over(raw_input_at(1), InstantValue::Sample, Arc::clone(&shared)),
            add(),
        )
    };
    let named_pair = CoPartitionedPair {
        left: 0,
        right: 1,
        name_free: false,
    };

    assert_eq!(
        output_has_metric_name(&instant_over(
            raw_input_at(0),
            InstantValue::Sample,
            Arc::clone(&shared)
        )),
        Some(true),
        "a scan of one metric hands its series over under that metric's name"
    );
    assert_eq!(
        output_has_metric_name(&instant_over(
            raw_input_at(1),
            InstantValue::Sample,
            Arc::clone(&shared)
        )),
        Some(true)
    );
    assert!(
        needs_single_execution_in_co(
            &matched(),
            &[proven_by_key(), proven_by_key()],
            &[named_pair]
        ),
        "the canonical key of `m1{{ns=\"a\"}}` is not the key `m1{{ns=\"a\"}} / m2{{ns=\"a\"}}` pairs by"
    );
    // The same tree in one execution is both sides here at once, which is the per-side proof and stays
    // the answer it was.
    assert!(!needs_single_execution_in_co(
        &matched(),
        &[proven_in_one_execution(), proven_in_one_execution()],
        &[named_pair]
    ));
    // The naming conjunct on its own, asked of the operands the shape actually has: two routed inputs
    // over named series, a pair that says so, and the answer is still no.
    assert!(
        !match_is_co_partitioned(
            &instant_over(raw_input_at(0), InstantValue::Sample, Arc::clone(&shared)),
            &instant_over(raw_input_at(1), InstantValue::Sample, Arc::clone(&shared)),
            &add(),
            &[named_pair]
        ),
        "both sides hand over named series, so the match key is not the canonical key"
    );
}

/// The shape the feature exists to serve: `B = FusedSeries { tree: Binary { left: Input(0), right:
/// Input(1) } }` over two routed producers, each of which answers a name-free vector.
///
/// Both operands are an [`NativeExpr::Input`] over *evaluated* samples, so [`output_has_metric_name`]
/// answers `None` for them: the node that built those label sets sits in the producers, not in this
/// tree, and guessing `Some(false)` there would be exactly the silent-empty-answer bug. The producer
/// half therefore travels on the pair ([`CoPartitionedPair::name_free`]), stated by the layer that
/// built the producers, and this is the case that must engage: with the joint fact stated, the match
/// runs once per partition.
#[test]
fn a_co_partitioned_name_free_pair_lets_the_match_run_per_partition() {
    let shared = grid(0, 60_000, 4);
    let matched = || {
        binary_with(
            input_on_at(0, Arc::clone(&shared)),
            input_on_at(1, Arc::clone(&shared)),
            add(),
        )
    };
    let by_key = [proven_by_key(), proven_by_key()];
    let name_free = CoPartitionedPair {
        left: 0,
        right: 1,
        name_free: true,
    };

    // What the tree cannot see, and the reason the fact has to travel beside it.
    assert_eq!(
        output_has_metric_name(&input_on_at(0, Arc::clone(&shared))),
        None,
        "an input over evaluated samples is another execution's answer"
    );
    // Conjuncts 1-5 all hold, so the match needs no execution of its own: `sum by(ns)(m1)` and
    // `sum by(ns)(m2)` pair by the very key the routing used.
    assert!(
        !needs_single_execution_in_co(&matched(), &by_key, &[name_free]),
        "a name-free co-partitioned pair is the case the two-sided proof exists for"
    );
    assert!(match_is_co_partitioned(
        &input_on_at(0, Arc::clone(&shared)),
        &input_on_at(1, Arc::clone(&shared)),
        &add(),
        &[name_free]
    ));

    // The producer half is required: the same pair stated as naming its series refuses the proof, and
    // the match needs one execution again.
    let named = CoPartitionedPair {
        name_free: false,
        ..name_free
    };
    assert!(!match_is_co_partitioned(
        &input_on_at(0, Arc::clone(&shared)),
        &input_on_at(1, Arc::clone(&shared)),
        &add(),
        &[named]
    ));
    assert!(
        needs_single_execution_in_co(&matched(), &by_key, &[named]),
        "a pair that names its series proves nothing about the match key"
    );

    // No pair at all proves nothing either: the routing fact is also required.
    assert!(needs_single_execution_in_co(&matched(), &by_key, &[]));

    // The two orientations are one pair. A match writes one source on its left and the other on its
    // right because of how the query was written, not because of how the routing placed them, so a
    // pair listed the other way round proves the same thing.
    let reversed = CoPartitionedPair {
        left: 1,
        right: 0,
        name_free: true,
    };
    assert!(match_is_co_partitioned(
        &input_on_at(0, Arc::clone(&shared)),
        &input_on_at(1, Arc::clone(&shared)),
        &add(),
        &[reversed]
    ));
    assert!(
        !needs_single_execution_in_co(&matched(), &by_key, &[reversed]),
        "the reversed orientation is the same pair"
    );
}

/// A match with modifiers pairs by a key of the caller's own choosing, so co-partitioned sources say
/// nothing about it: `on(host)` pairs two series whose canonical keys differ, and `group_left` answers
/// a label set copied out of its one side.
///
/// The operands are the routed inputs over evaluated samples, and the pair states a name-free
/// production, so conjuncts 1, 2 and 5 all hold: what refuses the shape is the modifier alone.
#[test]
fn a_match_with_modifiers_needs_one_execution_whatever_the_routing_proved() {
    let shared = grid(0, 60_000, 4);
    let matched = |modifiers: BinaryMatching| {
        binary_with(
            input_on_at(0, Arc::clone(&shared)),
            input_on_at(1, Arc::clone(&shared)),
            modifiers,
        )
    };
    let by_key = [proven_by_key(), proven_by_key()];
    let name_free = CoPartitionedPair {
        left: 0,
        right: 1,
        name_free: true,
    };
    let group_left = BinaryMatching {
        cardinality: Cardinality::ManyToOne(Vec::new()),
        ..add()
    };
    // Every match modifier a caller could write that is not the default `Ignoring([])` one: `on(host)`
    // names its own key, and `ignoring(host)` still names a key of its own.
    let ignoring_host = BinaryMatching {
        labels: MatchingLabels::Ignoring(vec!["host".into()]),
        ..add()
    };

    for modifiers in [on_host(), ignoring_host, group_left] {
        let tree = matched(modifiers);
        assert!(
            needs_single_execution_in_co(&tree, &by_key, &[name_free]),
            "a modified match pairs by its own key, not by the one the routing used: {tree:?}"
        );
    }
}

/// The naming fact is joint, and it is the half the tree cannot see: with conjuncts 1-4 all satisfied
/// and the operands being the routed inputs themselves, the pair's [`CoPartitionedPair::name_free`]
/// alone decides.
///
/// The shape is `sum by(ns)(m1) / m2`, whose two producers disagree: one hands over a folded, name-free
/// vector and the other a named series. One named side is enough, so the pair that knows both
/// producers states `name_free: false`. Reading that bit as `true` is what would run the match per
/// partition, pair the two sides under a key one of them does not answer under, and emit nothing.
#[test]
fn one_named_side_is_enough_to_refuse_the_name_free_proof() {
    let shared = grid(0, 60_000, 4);
    // The two operands are both routed inputs, so this pins the naming bit and nothing else: the tree
    // half of the proof is satisfied either way.
    let left = || input_on_at(0, Arc::clone(&shared));
    let right = || input_on_at(1, Arc::clone(&shared));
    let one_named = CoPartitionedPair {
        left: 0,
        right: 1,
        name_free: false,
    };

    assert!(!match_is_co_partitioned(
        &left(),
        &right(),
        &add(),
        &[one_named]
    ));
    assert!(needs_single_execution_in_co(
        &binary_with(left(), right(), add()),
        &[proven_by_key(), proven_by_key()],
        &[one_named]
    ));
    // The very same operands and pair with the joint bit flipped are parallel, so the bit is the whole
    // of the producer half here rather than decoration.
    let both_name_free = CoPartitionedPair {
        name_free: true,
        ..one_named
    };
    assert!(match_is_co_partitioned(
        &left(),
        &right(),
        &add(),
        &[both_name_free]
    ));
}

/// A pair the caller did not prove co-partitioned proves nothing: source 0 and source 2 route as the
/// caller said, and the caller said nothing about 0 and 2. The pair is read by the sources it names,
/// never by the position the two sides happen to sit in.
#[test]
fn a_pair_missing_from_the_co_partitioned_list_needs_one_execution() {
    let shared = grid(0, 60_000, 4);
    let matched = || {
        binary_with(
            input_on_at(0, Arc::clone(&shared)),
            input_on_at(2, Arc::clone(&shared)),
            add(),
        )
    };
    let three_sources = [proven_by_key(), proven_by_key(), proven_by_key()];
    let names_free_but_other_pair = CoPartitionedPair {
        left: 0,
        right: 1,
        name_free: true,
    };

    assert!(
        needs_single_execution_in_co(&matched(), &three_sources, &[names_free_but_other_pair]),
        "sources 0 and 2 were never proved to route together"
    );
    // A pair that does name these two sources engages, which is what makes the assertion above about
    // the pair list and not about the operands: the shape is otherwise the one that runs per partition,
    // and the orientation is the reverse of the one the match writes.
    assert!(!needs_single_execution_in_co(
        &matched(),
        &three_sources,
        &[CoPartitionedPair {
            left: 2,
            right: 0,
            name_free: true,
        }]
    ));
}
