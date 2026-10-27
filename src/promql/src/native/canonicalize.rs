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
    OverlapStatus,
};

/// Rewrites `tree` so that every consumer of a whole canonical series is handed one.
///
/// `inputs` holds what the caller knows about the fused inputs the tree was compiled over, indexed
/// by the `source_index` of the [`BoundInput`](crate::native::plan::BoundInput) each leaf was bound
/// to; a boundary the caller says nothing about proves nothing. What this adds is only a
/// [`NativeExpr::Merge`] over a node whose fragments may hold one label set: every operation binding
/// and every `source_index` stays where it was, so the rewrite changes which series a consumer reads
/// and not what the query computes.
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
    let root = rewrite(tree.root(), inputs)?.canonical();
    NativeTree::try_new(root.node)
}

/// A rewritten node and the facts it reports to whatever reads it.
struct Rewritten {
    /// The rewritten node.
    node: NativeNode,
    /// What it proves to its consumer.
    properties: NativeProperties,
}

impl Rewritten {
    /// Splits this into the node and its facts.
    fn into_parts(self) -> (NativeNode, NativeProperties) {
        (self.node, self.properties)
    }

    /// Wraps this node in a [`NativeExpr::Merge`] unless it already answers one series per canonical
    /// label set.
    ///
    /// A consumer that folds a label set's series into one answer needs the whole series and not the
    /// pieces one partition holds: given pieces it answers pieces. [`CanonicalGrouping`] and
    /// [`OverlapStatus`] are the two facts that say whether it has the whole one, so a node that
    /// cannot prove both is wrapped.
    ///
    /// A node whose output is not an evaluated vector keeps its place: a merge binds samples
    /// evaluated on a grid, so a scalar, raw samples and aggregate state have nothing a merge could
    /// take, and no consumer asks for one over those.
    fn canonical(self) -> Self {
        if self.properties.grouping == CanonicalGrouping::CanonicalComplete
            && self.properties.overlaps == OverlapStatus::Checked
        {
            return self;
        }
        if !matches!(&self.node.output, NativeType::Samples(schema) if schema.eval_grid.is_some()) {
            return self;
        }
        // A merge reports its input's layout: the same fields on the same grid.
        let output = self.node.output.clone();
        let input = self.properties;
        let node = NativeNode {
            expr: NativeExpr::Merge {
                input: Box::new(self.node),
            },
            output,
        };
        let properties = node.properties(&[input]);
        Self { node, properties }
    }
}

/// Rewrites `node` bottom-up: its children first, then the merge a consumer of them needs, then the
/// node's own facts.
///
/// `boundary` is what the caller knows about the fused inputs this tree was compiled over. It is
/// read at a leaf only, because that is the one node whose facts are not its children's.
fn rewrite(node: &NativeNode, boundary: &[NativeProperties]) -> DfResult<Rewritten> {
    let (expr, children) = match &node.expr {
        // A source is the boundary it was bound to, so its facts are the boundary's and there is
        // nothing below it to rewrite. A constant reads nothing.
        NativeExpr::Input(_) | NativeExpr::Scalar(_) => (node.expr.clone(), Vec::new()),
        NativeExpr::Instant { input, op } => {
            let (input, properties) = rewrite(input, boundary)?.into_parts();
            (
                NativeExpr::Instant {
                    input: Box::new(input),
                    op: op.clone(),
                },
                vec![properties],
            )
        }
        // A merge is the canonicalizer itself, so it asks nothing of its input: fragments are what it
        // was written to receive.
        NativeExpr::Merge { input } => {
            let (input, properties) = rewrite(input, boundary)?.into_parts();
            (
                NativeExpr::Merge {
                    input: Box::new(input),
                },
                vec![properties],
            )
        }
        // A window folds a range of one series' samples, so it needs that series whole.
        NativeExpr::Window { input, op } => {
            let (input, properties) = rewrite(input, boundary)?.canonical().into_parts();
            (
                NativeExpr::Window {
                    input: Box::new(input),
                    op: op.clone(),
                },
                vec![properties],
            )
        }
        // A map is point-wise, so it reads each piece as it is; what it does to a label set is a fact
        // about its output, not a reason to collect its input.
        NativeExpr::Map { input, op } => {
            let (input, properties) = rewrite(input, boundary)?.into_parts();
            (
                NativeExpr::Map {
                    input: Box::new(input),
                    op: op.clone(),
                },
                vec![properties],
            )
        }
        // A timestamp reads one series' positions, so it reads each piece as it is.
        NativeExpr::Timestamp { input } => {
            let (input, properties) = rewrite(input, boundary)?.into_parts();
            (
                NativeExpr::Timestamp {
                    input: Box::new(input),
                },
                vec![properties],
            )
        }
        // An aggregate folds a group, which is one label set's series summed, counted or compared:
        // the pieces of that series have to be one series before the fold. A final aggregate reads
        // state instead, which `canonical` leaves alone because a merge binds samples.
        NativeExpr::Aggregate { input, op } => {
            let (input, properties) = rewrite(input, boundary)?.canonical().into_parts();
            (
                NativeExpr::Aggregate {
                    input: Box::new(input),
                    op: op.clone(),
                },
                vec![properties],
            )
        }
        // A vector operation picks or orders series against each other, so it decides whole series.
        NativeExpr::Vector { input, op } => {
            let (input, properties) = rewrite(input, boundary)?.canonical().into_parts();
            (
                NativeExpr::Vector {
                    input: Box::new(input),
                    op: op.clone(),
                },
                vec![properties],
            )
        }
        // Each side of a binary or set operation is matched by label on its own, so each is
        // canonicalized on its own. A constant side has nothing to canonicalize and a merge could not
        // sit over one, which is what `canonical` leaves it as.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            let (left, left_properties) = rewrite(left, boundary)?.canonical().into_parts();
            let (right, right_properties) = rewrite(right, boundary)?.canonical().into_parts();
            (
                NativeExpr::Binary {
                    left: Box::new(left),
                    right: Box::new(right),
                    matching: matching.clone(),
                },
                vec![left_properties, right_properties],
            )
        }
        NativeExpr::Set { left, right, op } => {
            let (left, left_properties) = rewrite(left, boundary)?.canonical().into_parts();
            let (right, right_properties) = rewrite(right, boundary)?.canonical().into_parts();
            (
                NativeExpr::Set {
                    left: Box::new(left),
                    right: Box::new(right),
                    op: op.clone(),
                },
                vec![left_properties, right_properties],
            )
        }
    };
    let rebuilt = NativeNode {
        expr,
        output: node.output.clone(),
    };
    let properties = match &rebuilt.expr {
        // A source's facts are the boundary's: the boundary is what it reads.
        NativeExpr::Input(_) => rebuilt.properties(boundary),
        _ => rebuilt.properties(&children),
    };
    Ok(Rewritten {
        node: rebuilt,
        properties,
    })
}

#[cfg(test)]
mod tests;
