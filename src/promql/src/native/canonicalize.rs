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

//! Inserts the canonical merge a consumer of whole series needs.
//!
//! A fused input hands over the pieces of a metric one scan partition holds, so between those
//! pieces and a consumer that reads a series whole there has to be a node that collects the pieces
//! of one canonical label set into one series. [`NativeExpr::Merge`] is that node, and this module
//! is where a tree gains one: [`insert_native_merge_inside_current_fused`] walks the tree and puts
//! a merge below every consumer that cannot answer a whole series from pieces.
//!
//! The pass reads [`NativeProperties`] from the tree rather than storing them on it, so a rewritten
//! tree keeps no fact that the rewrite itself invalidated: each node's facts are derived again from
//! its rewritten children.

use datafusion::common::Result as DfResult;

use crate::native::plan::{
    CanonicalGrouping, NativeExpr, NativeNode, NativeProperties, NativeTree, NativeType,
};

/// Rewrites `tree` so that every consumer of a whole canonical series is handed one.
///
/// `inputs` holds what the caller knows about the fused inputs the tree was compiled over, indexed
/// by the `source_index` of the [`BoundInput`](crate::native::plan::BoundInput) each leaf was bound
/// to; a boundary the caller says nothing about proves nothing. The index is what a leaf reads, so a
/// tree with several sources is judged source by source: one scan's proof is never another's. What
/// this adds is only a [`NativeExpr::Merge`] over a node whose fragments may hold one label set:
/// every operation binding and every `source_index` stays where it was, so the rewrite changes which
/// series a consumer reads and not what the query computes.
///
/// Where the merge goes is decided by what each node does to a label set, not by what its input
/// proved: a window that drops the metric name, a timestamp and a label-rewriting map invent label
/// sets of their own, so the merge their aliases need sits above them and not below.
///
/// Running the pass over its own output changes nothing: a merge proves one series per label set, so
/// the consumer that gained one is not handed a mergeable child the second time.
///
/// The result is checked by [`NativeTree::try_new`], because a merge has to sit over an evaluated
/// vector and the pass may only put it there.
pub fn insert_native_merge_inside_current_fused(
    tree: &NativeTree,
    inputs: &[NativeProperties],
) -> DfResult<NativeTree> {
    // The root is a consumer too: whatever reads this tree reads one series per label set.
    let root = canonical(rewrite(tree.root(), inputs)?, inputs);
    NativeTree::try_new(root)
}

/// Wraps `node` in a [`NativeExpr::Merge`] unless it already answers one series per canonical label
/// set.
///
/// A consumer that folds a label set's series into one answer needs the whole series and not the
/// pieces one partition holds: given pieces it answers pieces. [`CanonicalGrouping`] is the fact
/// that says whether it has the whole one, so a node that cannot prove it is wrapped.
///
/// A node whose output is not an evaluated vector keeps its place: a merge binds samples evaluated on
/// a grid, so a scalar, raw samples and aggregate state have nothing a merge could take, and no
/// consumer asks for one over those.
///
/// The facts are read through [`NativeNode::properties`], which derives them from the rewritten node
/// and the boundary every time. A merge proves one series per label set from the pieces it was
/// handed, and it cannot claim that a fragment which never arrived was collected: that is why the
/// ownership it reports stays its input's, unchanged.
fn canonical(node: NativeNode, boundary: &[NativeProperties]) -> NativeNode {
    let properties = node.properties(boundary);
    if properties.grouping == CanonicalGrouping::CanonicalComplete {
        return node;
    }
    if !matches!(&node.output, NativeType::Samples(schema) if schema.eval_grid.is_some()) {
        return node;
    }
    // A merge reports its input's layout: the same fields on the same grid.
    let output = node.output.clone();
    NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(node),
        },
        output,
    }
}

/// Rewrites `node` bottom-up: its children first, then the merge a consumer of them needs.
///
/// `boundary` is what the caller knows about the fused inputs this tree was compiled over, indexed by
/// `source_index`. It is the one thing every node's facts are derived from, leaves included, so the
/// rewrite never has to say which of its own lists a node should read: [`NativeNode::properties`]
/// asks a leaf for its own entry and every other node for its children's.
fn rewrite(node: &NativeNode, boundary: &[NativeProperties]) -> DfResult<NativeNode> {
    let expr = match &node.expr {
        // A source is the boundary it was bound to and there is nothing below it to rewrite. A
        // constant reads nothing.
        NativeExpr::Input(_) | NativeExpr::Scalar(_) => node.expr.clone(),
        NativeExpr::Instant { input, op } => NativeExpr::Instant {
            input: Box::new(rewrite(input, boundary)?),
            op: op.clone(),
        },
        // A merge is the canonicalizer itself, so it asks nothing of its input: fragments are what it
        // was written to receive.
        NativeExpr::Merge { input } => NativeExpr::Merge {
            input: Box::new(rewrite(input, boundary)?),
        },
        // A window folds a range of one series' samples, so it needs that series whole.
        NativeExpr::Window { input, op } => NativeExpr::Window {
            input: Box::new(canonical(rewrite(input, boundary)?, boundary)),
            op: op.clone(),
        },
        // A map is point-wise, so it reads each piece as it is; what it does to a label set is a fact
        // about its output, not a reason to collect its input.
        NativeExpr::Map { input, op } => NativeExpr::Map {
            input: Box::new(rewrite(input, boundary)?),
            op: op.clone(),
        },
        // A timestamp reads one series' positions, so it reads each piece as it is.
        NativeExpr::Timestamp { input } => NativeExpr::Timestamp {
            input: Box::new(rewrite(input, boundary)?),
        },
        // An aggregate folds a group, which is one label set's series summed, counted or compared:
        // the pieces of that series have to be one series before the fold. A final aggregate reads
        // state instead, which `canonical` leaves alone because a merge binds samples.
        NativeExpr::Aggregate { input, op } => NativeExpr::Aggregate {
            input: Box::new(canonical(rewrite(input, boundary)?, boundary)),
            op: op.clone(),
        },
        // A vector operation picks or orders series against each other, so it decides whole series.
        NativeExpr::Vector { input, op } => NativeExpr::Vector {
            input: Box::new(canonical(rewrite(input, boundary)?, boundary)),
            op: op.clone(),
        },
        // Each side of a binary or set operation is matched by label on its own, so each is
        // canonicalized on its own. A constant side has nothing to canonicalize and a merge could not
        // sit over one, which is what `canonical` leaves it as.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => NativeExpr::Binary {
            left: Box::new(canonical(rewrite(left, boundary)?, boundary)),
            right: Box::new(canonical(rewrite(right, boundary)?, boundary)),
            matching: matching.clone(),
        },
        NativeExpr::Set { left, right, op } => NativeExpr::Set {
            left: Box::new(canonical(rewrite(left, boundary)?, boundary)),
            right: Box::new(canonical(rewrite(right, boundary)?, boundary)),
            op: op.clone(),
        },
    };
    Ok(NativeNode {
        expr,
        output: node.output.clone(),
    })
}

#[cfg(test)]
mod tests;
