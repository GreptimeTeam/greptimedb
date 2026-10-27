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

//! What the rewrite has to get right: a merge exactly where a consumer needs one whole canonical
//! series, none where it already has one, and the same tree when the pass runs twice.

use std::sync::Arc;

use datatypes::series::SeriesOrdering;
use datatypes::series::time::EvalGrid;

use super::insert_native_merge_inside_current_fused;
use crate::native::plan::tests::{
    complete_aggregate, evaluated, grid, input_on, map_over, map_rewriting_labels, merge_over,
    sorted, sum_of, window_on,
};
use crate::native::plan::{
    BoundSetOp, CanonicalGrouping, CanonicalOwner, NativeExpr, NativeNode, NativeProperties,
    NativeTree, OverlapStatus,
};
use crate::vector_match::{MatchingLabels, SetOp};

/// What a caller knows about a scan: this execution holds one partition's pieces of a metric, in
/// scans that hand each series over whole.
fn source_pieces() -> NativeProperties {
    NativeProperties::source_pieces(SeriesOrdering::Contiguous)
}

/// What a caller knows about a scan it has already collected into one series per label set.
fn canonical_source() -> NativeProperties {
    NativeProperties {
        owner: CanonicalOwner::SinglePartition,
        grouping: CanonicalGrouping::CanonicalComplete,
        overlaps: OverlapStatus::Checked,
        ordering: SeriesOrdering::Contiguous,
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

/// A window reads a range of one series' samples, so a scan's pieces of that series have to be one
/// series before the range is folded: the window gains one merge directly below it.
///
/// A child that already answers one series per label set gains none, whether it is a merge the tree
/// already holds or a boundary the caller proved canonical.
#[test]
fn a_window_gains_one_merge_below_it() {
    let shared = grid(0, 60_000, 4);
    let tree = rewrite(rate(&shared), &[source_pieces()]);

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
        window_on(
            merge_over(input_on(Arc::clone(&shared))),
            Some(Arc::clone(&shared)),
            Arc::clone(&shared),
        ),
        &[source_pieces()],
    );
    assert_eq!(merges(&merged_tree), 1, "{merged_tree}");

    // A boundary the caller proves canonical needs no merge either: the facts travel beside the
    // tree, so the window inherits them without a node being added.
    let proven = rewrite(rate(&shared), &[canonical_source()]);
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

    // A window folds a range of one series, so the merge is directly below it and nothing is added
    // above it.
    let window = rewrite(rate(&shared), &[source_pieces()]);
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
