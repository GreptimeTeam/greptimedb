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

//! Puts a canonical merge where a label rewrite made one necessary.
//!
//! A fused input hands over the pieces of a metric one scan partition holds, and a label rewrite in
//! the tree can give two of those pieces one label set. Every consumer that answers a whole label
//! set - a fold, a ranking, a vector match, the statement's own output - then reads two series where
//! Prometheus reads one, or answers the sum of a group twice. [`NativeExpr::Merge`] is the node that
//! collects the pieces of one label set into one series, and this module decides where one is
//! needed.
//!
//! # What decides it
//!
//! Only a rewrite can alias, and only one that writes a label its input already carries:
//! [`duplicates_canonical_labels`] is that fact, derived from the tree and the tags each source
//! reports. A tree with no such rewrite gains no merge at all, so a per-series query keeps every
//! scan partition and pays nothing for this pass.
//!
//! Where a merge is added is a property of the consumer, not of the rewrite:
//!
//! | consumer | merge below it? | why |
//! | --- | --- | --- |
//! | the root | yes | the answer names one series per label set, so two rows sharing one set are the error Prometheus reports |
//! | `Aggregate` | yes | a fold sums the values it is handed at a point, so two pieces of one series are summed as if the query said so twice |
//! | `Vector` | yes | ranking, presence checks and folds must read canonical series rather than duplicate fragments |
//! | `Binary`/`Set` over two vectors | yes, on both sides | each side is matched by label, and the kernel refuses two values of one field at a point only after the pair was matched |
//! | `Info` | yes, on both sides | it looks the info series up by identifying labels, so the pieces of one label set have to be one series first |
//! | `Window` over an evaluated vector | yes | a window folds a range of one series, so it reads the series whole |
//! | `Map`, `Timestamp`, `Instant`, a scalar broadcast | no | point-wise per piece; a merge would only move the error the `Emitted` check already reports |
//!
//! A merge is never added below a raw scan, a scalar or aggregate state: it binds samples evaluated
//! on a grid, and no consumer asks for one over those.
//!
//! Running the pass over its own output changes nothing: the node that gained a merge then answers
//! one series per label set, so [`duplicates_canonical_labels`] is `false` for it and a second run
//! finds nothing to add. That also makes the pass safe to apply at every segment boundary rather
//! than only at the statement root.
//!
//! What this pass does *not* decide is placement: whether the merge can be answered inside one
//! execution is the lowering's question. A merge that spans partitions uses a producer and the
//! existing canonical router; downstream consumers read ordinary evaluated frames.

use datafusion::error::Result as DfResult;

use crate::native::plan::{
    NativeExpr, NativeNode, NativeTree, NativeType, duplicates_canonical_labels,
};

#[cfg(test)]
mod tests;

/// Rewrites `tree` so that every consumer of a whole canonical series is handed one.
///
/// `source_tags` is what the caller knows about each fused input's tags, indexed by the
/// `source_index` of the [`BoundInput`](crate::native::plan::BoundInput) each leaf was bound to; a
/// source the caller says nothing about proves nothing, and a rewrite over it is collected. The
/// index is what a leaf reads, so a tree with several sources is judged source by source.
///
/// Every operation binding and every `source_index` stays where it was, so the rewrite changes which
/// series a consumer reads and not what the query computes. The result is checked by
/// [`NativeTree::try_new`], because a merge has to sit over an evaluated vector and the pass may
/// only put it there.
pub fn merge_label_rewrites(
    tree: &NativeTree,
    source_tags: &[Vec<String>],
) -> DfResult<NativeTree> {
    let root = rewrite(tree.root(), source_tags)?;
    // The root is a consumer too: whatever reads this tree reads one series per label set.
    NativeTree::try_new(canonical(root, source_tags))
}

/// Wraps `node` in a [`NativeExpr::Merge`] when its pieces can share a label set.
///
/// Three things have to hold, and each is asked rather than assumed:
///
/// - the node's output is an evaluated vector, because a merge binds samples on a grid and a raw
///   scan, a scalar and aggregate state have nothing it could take;
/// - the node can hand over two series of one label set
///   ([`duplicates_canonical_labels`]), because collecting pieces that are already distinct is a
///   barrier that buys nothing;
/// - it is not already a merge, which would collect what is already collected.
fn canonical(node: NativeNode, source_tags: &[Vec<String>]) -> NativeNode {
    if !matches!(&node.output, NativeType::Samples(schema) if schema.eval_grid.is_some()) {
        return node;
    }
    if matches!(node.expr, NativeExpr::Merge { .. }) {
        return node;
    }
    if !duplicates_canonical_labels(&node, source_tags) {
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

/// Rewrites `node` bottom-up: its children first, then the merge its own consumer contract needs of
/// them.
fn rewrite(node: &NativeNode, source_tags: &[Vec<String>]) -> DfResult<NativeNode> {
    let expr = match &node.expr {
        // A source is the boundary it was bound to and there is nothing below it to rewrite. A
        // constant reads nothing.
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {
            node.expr.clone()
        }
        // A selection reads raw samples, one whole storage series at a time, and stamps them onto a
        // grid; a timestamp replaces each value with its sample's time. Both read each piece as it
        // is, so neither collects what it reads.
        NativeExpr::Instant { input, op } => NativeExpr::Instant {
            input: Box::new(rewrite(input, source_tags)?),
            op: op.clone(),
        },
        NativeExpr::Timestamp { input } => NativeExpr::Timestamp {
            input: Box::new(rewrite(input, source_tags)?),
        },
        // A merge is the canonicalizer itself, so it asks nothing of its input: fragments are what it
        // was written to receive.
        NativeExpr::Merge { input } => NativeExpr::Merge {
            input: Box::new(rewrite(input, source_tags)?),
        },
        // A window folds a range of one series' samples and hands the answer over as that series, so
        // it reads the series whole. Over a raw scan the pieces are whole storage series already and
        // the merge above is a no-op; over an evaluated vector a rewrite can have aliased them.
        NativeExpr::Window { input, op } => NativeExpr::Window {
            input: Box::new(canonical(rewrite(input, source_tags)?, source_tags)),
            op: op.clone(),
        },
        // A map is point-wise, so it reads each piece as it is; what it does to a label set is a fact
        // about its output, asked at the consumer of that output and not here.
        NativeExpr::Map { input, op } => NativeExpr::Map {
            input: Box::new(rewrite(input, source_tags)?),
            op: op.clone(),
        },
        // A parameterized map's parameters are scalars on the input's grid; the input itself is read
        // point-wise like a map's.
        NativeExpr::ParameterizedMap {
            input,
            op,
            parameters,
        } => NativeExpr::ParameterizedMap {
            input: Box::new(rewrite(input, source_tags)?),
            op: *op,
            parameters: parameters
                .iter()
                .map(|parameter| rewrite(parameter, source_tags))
                .collect::<DfResult<Vec<_>>>()?,
        },
        // A fold answers its groups, so two pieces of one series in a group are summed as if the
        // query had written them twice: the merge is what makes one series arrive as one.
        NativeExpr::Aggregate { input, op } => NativeExpr::Aggregate {
            input: Box::new(canonical(rewrite(input, source_tags)?, source_tags)),
            op: op.clone(),
        },
        // Vector operations consume canonical series: ranking counts them, and presence checks and
        // folds must not silently consume overlapping fragments that a merge would reject.
        NativeExpr::Vector { input, op } => NativeExpr::Vector {
            input: Box::new(canonical(rewrite(input, source_tags)?, source_tags)),
            op: op.clone(),
        },
        // Each side of a binary or set operation is matched by label on its own, so each is
        // canonicalized on its own. A constant side has nothing to canonicalize and a merge could not
        // sit over one, which `canonical` leaves it as.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => NativeExpr::Binary {
            left: Box::new(canonical(rewrite(left, source_tags)?, source_tags)),
            right: Box::new(canonical(rewrite(right, source_tags)?, source_tags)),
            matching: matching.clone(),
        },
        NativeExpr::Set { left, right, op } => NativeExpr::Set {
            left: Box::new(canonical(rewrite(left, source_tags)?, source_tags)),
            right: Box::new(canonical(rewrite(right, source_tags)?, source_tags)),
            op: op.clone(),
        },
        // `info` pairs each base series with the info series that share its identifying labels, so it
        // reads both sides whole: the pieces of one label set have to be one series before they are
        // matched.
        NativeExpr::Info {
            base,
            info,
            selector,
        } => NativeExpr::Info {
            base: Box::new(canonical(rewrite(base, source_tags)?, source_tags)),
            info: Box::new(canonical(rewrite(info, source_tags)?, source_tags)),
            selector: selector.clone(),
        },
        // `vector(s)` builds its series from a scalar, which carries no label set to collect.
        NativeExpr::VectorOf { input } => NativeExpr::VectorOf {
            input: Box::new(rewrite(input, source_tags)?),
        },
    };
    Ok(NativeNode {
        expr,
        output: node.output.clone(),
    })
}
