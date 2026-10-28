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

//! What the insertion pass has to get right: a merge exactly where a consumer needs one whole
//! canonical series and none where it already has one, a tree its own validator accepts, the facts
//! the nodes below it actually preserve, and the same tree when the pass runs twice.
//!
//! The trees here are built node by node over the builders of the `rate(x[5m])` fixture in
//! [`crate::native::plan::tests`], and - for `info` - over this module's own one-field fixtures: a
//! raw scan, the window that folds it onto a grid, and the instant selection that reports each info
//! sample's own time. The compiler lives in the `query` crate, which depends on this one, so a test
//! in this crate cannot call it.

use std::sync::Arc;

use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::EvalGrid;

use super::insert_native_merge_inside_current_fused;
use crate::info::InfoSelector;
use crate::native::plan::tests::{
    complete_aggregate, evaluated, grid, input_on, input_on_at, instant_over, map_over,
    map_rewriting_labels, merge_over, raw_input_at, sorted, source_pieces, sum_of, timestamp_of,
    window_of_range, window_on, window_with,
};
use crate::native::plan::{
    BoundInput, BoundInstant, BoundSetOp, BoundWindow, CanonicalGrouping, CanonicalOwner,
    InstantValue, NativeExpr, NativeNode, NativeProperties, NativeTree, NativeType,
};
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::vector_match::{MatchingLabels, SetOp};

/// What a caller knows about a scan it has already collected into one series per label set.
fn canonical_source() -> NativeProperties {
    NativeProperties {
        owner: CanonicalOwner::SinglePartition,
        grouping: CanonicalGrouping::CanonicalComplete,
    }
}

/// `rate` over a scan on `shared`, which is the shape a window over a fused input has.
fn rate(shared: &Arc<EvalGrid>) -> NativeNode {
    window_on(
        input_on(Arc::clone(shared)),
        Some(Arc::clone(shared)),
        Arc::clone(shared),
    )
}

/// The same shape running a function that keeps the metric name, so a rewrite adds only the merge the
/// window's input needs and none above the window.
fn last_over_time(shared: &Arc<EvalGrid>) -> NativeNode {
    window_with(
        input_on(Arc::clone(shared)),
        RangeFunction::LastOverTime,
        Some(Arc::clone(shared)),
        Arc::clone(shared),
    )
}

/// The rewrite as a test reads it: legal trees in, a legal tree out, where a broken rewrite panics.
fn rewrite(root: NativeNode, facts: &[NativeProperties]) -> NativeTree {
    let tree = NativeTree::try_new(root).expect("the fixture is a legal tree");
    insert_native_merge_inside_current_fused(&tree, facts).expect("the rewrite is legal")
}

/// The node at the top of the tree, below the merge the root itself needed.
///
/// The root is a consumer too, so a tree whose root loses grouping gains a merge above itself. What
/// these tests assert about is the merge a consumer inside the tree gains.
fn below_root(node: &NativeNode) -> &NativeNode {
    match &node.expr {
        NativeExpr::Merge { input } => input,
        _ => node,
    }
}

/// The number of merges a tree holds, which is what says a rewrite put one there and not three.
fn merges(tree: &NativeTree) -> usize {
    tree.to_string().matches("Merge").count()
}

// ---------------------------------------------------------------------------
// The one-field fixtures the `info` trees are built over.
//
// `info` pairs a base series with the info series that share its identifying labels and hands the
// base's layout on, so the base and the info side have to agree on it: these trees carry one field,
// which is why they are not built with the imported zero-field builders. The scan, the window and
// the selection below are the same shapes those builders make, over that one field.
// ---------------------------------------------------------------------------

/// `count` fields named `v0..`, the layout the `info` fixtures read.
fn fields(count: usize) -> Arc<[FieldSpec]> {
    (0..count)
        .map(|index| FieldSpec {
            name: format!("v{index}").into(),
        })
        .collect()
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

/// `rate(x[5m])` over `inner`: a window that folds raw samples onto `out` and drops the metric name.
fn window_over(inner: NativeNode, out: Arc<EvalGrid>) -> NativeNode {
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
        output: evaluated(fields(1), out),
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

/// A window reads a range of one series' samples, so a scan's pieces of that series have to be one
/// series before the range is folded: the window gains one merge directly below it.
///
/// A child that already answers one series per label set gains none, whether it is a merge the tree
/// already holds or a boundary the caller proved canonical. The window here keeps the metric name, so
/// the merge under it is the only one the rewrite adds; a function that drops the name also gains one
/// above the window, which `a_name_dropping_window_gains_a_merge_above_it` pins.
#[test]
fn a_window_gains_one_merge_below_it() {
    let shared = grid(0, 60_000, 4);
    let tree = rewrite(last_over_time(&shared), &[source_pieces()]);

    let NativeExpr::Window { input, .. } = &tree.root().expr else {
        unreachable!("the root is the window the fixture was built from")
    };
    assert!(
        matches!(input.expr, NativeExpr::Merge { .. }),
        "the window's input is not a merge:\n{tree}"
    );
    // Exactly one, and not one at the root: a window hands over the label set it read, so what its
    // merge proved is what the tree proves.
    assert_eq!(merges(&tree), 1, "{tree}");
    // The rewrite adds a node and binds nothing: the scan is still the source it was compiled as.
    let NativeExpr::Merge { input: merged } = &input.expr else {
        unreachable!("just asserted")
    };
    let NativeExpr::Input(binding) = &merged.expr else {
        unreachable!("the merge sits over the scan")
    };
    assert_eq!(binding.source_index, 0);

    // A merge above the scan is the rewrite's own output, so a window over one has the whole series
    // already and the second run adds nothing.
    let merged_tree = rewrite(
        window_with(
            merge_over(input_on(Arc::clone(&shared))),
            RangeFunction::LastOverTime,
            Some(Arc::clone(&shared)),
            Arc::clone(&shared),
        ),
        &[source_pieces()],
    );
    assert_eq!(merges(&merged_tree), 1, "{merged_tree}");

    // A boundary the caller proves canonical needs no merge either: the facts travel beside the
    // tree, so the window inherits them without a node being added.
    let proven = rewrite(last_over_time(&shared), &[canonical_source()]);
    assert_eq!(merges(&proven), 0, "{proven}");
}

/// Running the pass over its own output changes nothing.
#[test]
fn the_rewrite_is_idempotent() {
    let shared = grid(0, 60_000, 4);
    // A tree that gains a merge in more than one place: the window needs one above the scan, and the
    // label rewrite at the root loses grouping, so the root needs one too.
    let tree = rewrite(map_rewriting_labels(rate(&shared)), &[source_pieces()]);
    let again = rewrite(tree.root().clone(), &[source_pieces()]);

    assert_eq!(tree.to_string(), again.to_string());
    assert_eq!(merges(&tree), 2, "{tree}");
}

/// The root is a consumer too: what reads this tree reads one series per label set.
#[test]
fn the_root_gains_a_merge_when_it_cannot_answer_complete_series() {
    let shared = grid(0, 60_000, 4);
    // A label rewrite changes which series share a label set, so the tree cannot prove its root holds
    // one series per label set.
    let tree = rewrite(
        map_rewriting_labels(input_on(Arc::clone(&shared))),
        &[source_pieces()],
    );

    let NativeExpr::Merge { input } = &tree.root().expr else {
        unreachable!("the root is not a merge:\n{tree}")
    };
    assert!(matches!(input.expr, NativeExpr::Map { .. }), "{tree}");
    // A map is point-wise, so it needs no merge of its own: the one at the root is the only one.
    assert_eq!(merges(&tree), 1, "{tree}");
    let NativeExpr::Map { input: scanned, .. } = &input.expr else {
        unreachable!("just asserted")
    };
    assert!(matches!(scanned.expr, NativeExpr::Input(_)), "{tree}");
}

/// Every consumer that folds or matches whole series gains a merge on each side it reads.
#[test]
fn every_stateful_consumer_gains_a_merge() {
    let shared = grid(0, 60_000, 4);

    // A window folds a range of one series, so the merge is directly below it; this window keeps the
    // metric name, so nothing is added above it either.
    let window = rewrite(last_over_time(&shared), &[source_pieces()]);
    let NativeExpr::Window { input, .. } = &window.root().expr else {
        unreachable!("the root is the window the fixture was built from")
    };
    assert!(matches!(input.expr, NativeExpr::Merge { .. }), "{window}");

    // An aggregate folds a group, which is one label set's series, so pieces of one have to be one
    // series before the fold. A map is point-wise, so it is a child the aggregate has to canonicalize
    // itself: a window would already have gained a merge of its own, and what it hands on would then
    // prove the grouping the aggregate needs.
    let aggregate = rewrite(
        complete_aggregate(map_over(input_on(Arc::clone(&shared)))),
        &[source_pieces()],
    );
    let NativeExpr::Aggregate { input, .. } = &below_root(aggregate.root()).expr else {
        unreachable!("the aggregate is under the merge the root needed:\n{aggregate}")
    };
    assert!(
        matches!(input.expr, NativeExpr::Merge { .. }),
        "{aggregate}"
    );

    // A vector operation decides whole series against each other.
    let vector = rewrite(
        sorted(map_over(input_on(Arc::clone(&shared)))),
        &[source_pieces()],
    );
    let NativeExpr::Vector { input, .. } = &below_root(vector.root()).expr else {
        unreachable!("the vector operation is under the merge the root needed:\n{vector}")
    };
    assert!(matches!(input.expr, NativeExpr::Merge { .. }), "{vector}");

    // A binary operation matches two vectors by label, so each vector operand is canonicalized on its
    // own.
    let binary = rewrite(
        sum_of(
            map_over(input_on(Arc::clone(&shared))),
            map_over(input_on(Arc::clone(&shared))),
        ),
        &[source_pieces()],
    );
    let NativeExpr::Binary { left, right, .. } = &below_root(binary.root()).expr else {
        unreachable!("the binary operation is under the merge the root needed:\n{binary}")
    };
    assert!(matches!(left.expr, NativeExpr::Merge { .. }), "{binary}");
    assert!(matches!(right.expr, NativeExpr::Merge { .. }), "{binary}");

    // A set operation keeps samples of either side as they are, but it still reads both sides whole.
    let set = rewrite(
        NativeNode {
            expr: NativeExpr::Set {
                left: Box::new(map_over(input_on(Arc::clone(&shared)))),
                right: Box::new(map_over(input_on(Arc::clone(&shared)))),
                op: BoundSetOp {
                    op: SetOp::Or,
                    labels: MatchingLabels::Ignoring(Vec::new()),
                },
            },
            output: evaluated(Arc::new([]), Arc::clone(&shared)),
        },
        &[source_pieces()],
    );
    let NativeExpr::Set { left, right, .. } = &below_root(set.root()).expr else {
        unreachable!("the set operation is under the merge the root needed:\n{set}")
    };
    assert!(matches!(left.expr, NativeExpr::Merge { .. }), "{set}");
    assert!(matches!(right.expr, NativeExpr::Merge { .. }), "{set}");
}

/// An input's facts are its own: a tree with two sources is judged source by source, and one
/// source's canonical proof never stands in for another's.
///
/// Source 0 is already one series per label set and source 1 hands over pieces of a metric, so a
/// window that reads source 1 cannot answer a whole series yet and has to collect them. Reading the
/// first entry of the boundary for every leaf — which is what taking `boundary.first()` did — found
/// source 0's proof and added nothing at all.
#[test]
fn each_input_reads_its_own_boundary_facts() {
    let shared = grid(0, 60_000, 4);
    let boundary = [canonical_source(), source_pieces()];
    let tree = rewrite(
        window_with(
            input_on_at(1, Arc::clone(&shared)),
            RangeFunction::Rate,
            Some(Arc::clone(&shared)),
            Arc::clone(&shared),
        ),
        &boundary,
    );

    // `rate` folds a range of one series' samples, so the pieces of source 1 have to be one series
    // before the fold: that merge sits directly below the window.
    let root = below_root(tree.root());
    let NativeExpr::Window { input, .. } = &root.expr else {
        unreachable!("the root is the window the fixture was built from:\n{tree}")
    };
    let NativeExpr::Merge { input: leaf } = &input.expr else {
        unreachable!("the window's input is not a merge:\n{tree}")
    };
    let NativeExpr::Input(binding) = &leaf.expr else {
        unreachable!("the merge does not sit over the scan:\n{tree}")
    };
    // The scan the merge collects is source 1's, which is the source the window reads.
    assert_eq!(binding.source_index, 1);
    // `rate` also drops the metric name, so the rewrite's aliases need a merge above the window: one
    // merge for the pieces it reads, one for the label sets it invents.
    assert!(
        matches!(tree.root().expr, NativeExpr::Merge { .. }),
        "{tree}"
    );
    assert_eq!(merges(&tree), 2, "{tree}");
}

/// The counterexample from review: one canonical label set that source 1 hands over as two separately
/// closed fragments, folded by `sum_over_time` over a range that spans the gap between them.
///
/// The grid is `[0ms, 10ms]` and the window is 11ms wide. Fragment A carries a sample of `1` at `0ms`
/// and fragment B a sample of `2` at `10ms`; both are fragments of `{job="x"}`, closed separately
/// because a scan partition boundary fell between them. Read as one series the second grid point's
/// window holds both samples and the answer is `[1, 3]`. Read as two fragments the window folds each
/// one alone: A answers `1` at the first point and B answers `2` at the second, and the two series
/// alias on `{job="x"}` with neither holding the sum, which nothing above can repair.
///
/// What this asserts is the rewrite: without the facts of source 1 the pass borrows source 0's proof,
/// leaves the fragments apart and the window folds pieces. The runtime half is pinned by the
/// fused-series executor's own `two_partitions_pieces_of_one_label_set_arrive_as_two_chains`, which
/// drives two real fragments of one label set through a merged tree; what this test pins is the merge
/// the pass has to insert for that to be possible.
#[test]
fn a_fragment_of_one_label_set_is_collected_before_the_window_folds_it() {
    let shared = grid(0, 10, 2);
    let boundary = [canonical_source(), source_pieces()];
    // A fused input hands over evaluated fragments, which is the shape the pass inserts into: a raw
    // scan has no grid for a merge to bind.
    let tree = rewrite(
        window_of_range(
            input_on_at(1, Arc::clone(&shared)),
            RangeFunction::SumOverTime,
            11,
            Some(Arc::clone(&shared)),
            Arc::clone(&shared),
        ),
        &boundary,
    );

    // `sum_over_time` drops the metric name, so the root gains a merge of its own; the merge this
    // test is about is the one under the window.
    let root = below_root(tree.root());
    let NativeExpr::Window { input, op } = &root.expr else {
        unreachable!("the root is the window the fixture was built from:\n{tree}")
    };
    assert_eq!(op.window.range_ms, 11);
    let NativeExpr::Merge { input: leaf } = &input.expr else {
        unreachable!("the window's input is not a merge:\n{tree}")
    };
    let NativeExpr::Input(binding) = &leaf.expr else {
        unreachable!("the merge does not sit over the scan:\n{tree}")
    };
    assert_eq!(binding.source_index, 1);
}

/// A window that drops the metric name makes label sets that were distinct coincide, so the merge its
/// aliases need sits above the window.
///
/// Both inputs are valid on their own: `a{job="x"}` holds a value only at position 0 and `b{job="x"}`
/// only at position 1, so no two fragments of the input answer the same position under one label set
/// and an overlap check over the input is satisfied. The metric name is what tells them apart, and
/// `rate` drops it in `RangeEvaluator::emit`; `{job="x"}` then holds two fragments at every position,
/// one from each input series, and only a merge can assemble them. The input's grouping is therefore
/// not the output's, which is why the pass has to work from the facts of what the window does.
#[test]
fn a_name_dropping_window_gains_a_merge_above_it() {
    let shared = grid(0, 60_000, 4);

    // The scan is already canonical, so nothing is collected below the window: the one merge is the
    // window's own doing.
    let tree = rewrite(
        window_with(
            input_on(Arc::clone(&shared)),
            RangeFunction::Rate,
            Some(Arc::clone(&shared)),
            Arc::clone(&shared),
        ),
        &[canonical_source()],
    );
    let NativeExpr::Merge { input } = &tree.root().expr else {
        unreachable!("the root is not a merge:\n{tree}")
    };
    let NativeExpr::Window { input: scanned, .. } = &input.expr else {
        unreachable!("the merge is not over the window:\n{tree}")
    };
    assert!(matches!(scanned.expr, NativeExpr::Input(_)), "{tree}");
    assert_eq!(merges(&tree), 1, "{tree}");
}

/// The fix is not "every window loses its proof": `last_over_time` and `first_over_time` keep the
/// metric name, so a series stays under the label set it read and nothing above the window needs a
/// merge.
#[test]
fn a_window_that_keeps_the_metric_name_inherits() {
    let shared = grid(0, 60_000, 4);
    let tree = rewrite(
        window_with(
            input_on(Arc::clone(&shared)),
            RangeFunction::LastOverTime,
            Some(Arc::clone(&shared)),
            Arc::clone(&shared),
        ),
        &[canonical_source()],
    );

    assert!(
        matches!(tree.root().expr, NativeExpr::Window { .. }),
        "a window that keeps the label set it read needs no merge above it:\n{tree}"
    );
    assert_eq!(merges(&tree), 0, "{tree}");
}

/// A timestamp and a timestamp-mode selection replace the identity with the sample's time, so both
/// need the merge a value selection does not.
#[test]
fn the_timestamp_shapes_gain_a_merge_and_a_value_selection_does_not() {
    let shared = grid(0, 60_000, 4);

    // `timestamp(x)` rebuilds the labels through `drop_metric_name`.
    let stamped = rewrite(
        timestamp_of(input_on(Arc::clone(&shared))),
        &[canonical_source()],
    );
    assert!(
        matches!(stamped.root().expr, NativeExpr::Merge { .. }),
        "the timestamp's aliases need a merge:\n{stamped}"
    );

    // `timestamp(x)` over a selection is the same rewrite on the selected sample.
    let selected = rewrite(
        instant_over(
            raw_input_at(0),
            InstantValue::Timestamp,
            Arc::clone(&shared),
        ),
        &[canonical_source()],
    );
    assert!(
        matches!(selected.root().expr, NativeExpr::Merge { .. }),
        "a timestamp-mode selection's aliases need a merge:\n{selected}"
    );

    // A selection that reports the value keeps the label set it read.
    let values = rewrite(
        instant_over(raw_input_at(0), InstantValue::Sample, Arc::clone(&shared)),
        &[canonical_source()],
    );
    assert_eq!(merges(&values), 0, "{values}");
    assert!(
        matches!(values.root().expr, NativeExpr::Instant { .. }),
        "{values}"
    );
}

// ---------------------------------------------------------------------------
// The `info` side: what the merges the pass inserts must not change.
// ---------------------------------------------------------------------------

/// A legal `info` tree survives the pass, and the tree the pass produces is the one its validator
/// accepts.
///
/// This is the contract the pass broke: it wraps every whole-series consumer in a merge, `info`'s
/// info side included, and the validator used to require that side be *directly* an instant
/// selection reporting sample times - so the pass built a tree `NativeTree::try_new` then rejected.
/// The merge only collects the pieces of one label set into one series, so the times it hands on are
/// the times it read, and the check looks through it.
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

    // The pass wrapped what it had to; what matters is that the info side still *proves* an instant
    // selection reporting sample times, however many merges sit above it.
    fn through_merges(node: &NativeNode) -> &NativeNode {
        let mut node = node;
        while let NativeExpr::Merge { input } = &node.expr {
            node = input;
        }
        node
    }
    let NativeExpr::Info { info, .. } = &through_merges(passed.root()).expr else {
        panic!("the passed tree is not an `info` node below its merges:\n{passed}");
    };
    let NativeExpr::Instant { op, .. } = &through_merges(info).expr else {
        panic!("the info side is not an instant selection below its merges:\n{passed}");
    };
    assert_eq!(
        InstantValue::InfoTimestamp,
        op.value,
        "the info side no longer proves the sample times `info` reads:\n{passed}"
    );

    // The pass returns a tree it validated, and the same tree validates again from scratch.
    NativeTree::try_new(passed.root().clone()).expect("the passed tree is legal on its own");

    // The pass is idempotent on its own output: every consumer it wrapped is handed a merge already,
    // so a second run finds nothing to add.
    let again = insert_native_merge_inside_current_fused(&passed, &boundary)
        .expect("the pass accepts its own output");
    assert_eq!(
        passed.to_string(),
        again.to_string(),
        "the pass changed its own output"
    );
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
