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

//! Puts a canonical [`NativeExpr::Merge`] where a label rewrite or a metadata-label drop can give
//! two series one label set.
//!
//! A fused input hands over the series one scan partition holds. A node that writes an existing
//! label or drops `__name__`/`__type__`/`__unit__` can make two of them share a canonical label set
//! ([`duplicates_canonical_labels`]); a consumer that reads whole label sets then needs a merge
//! below it:
//!
//! | consumer | merge below it? |
//! | --- | --- |
//! | the root | yes |
//! | `Aggregate`, `ParameterizedAggregate`, `Vector`, `ParameterizedTopK`, `ParameterizedHistogram`, `ScalarOf` | yes |
//! | `Binary`, `Set`, `Info` | yes, on each evaluated-vector side (a scalar broadcast's vector side included) |
//! | `Window` | yes |
//! | `Map`, `ParameterizedMap` | yes, below it: a map can empty a point (histogram functions drop floats, math skips histograms), so a merge above it would judge occupancy the map already erased |
//! | `Timestamp`, `Instant`, `VectorOf` | no |
//!
//! The `NULL`/`''` key-column pair is not collected; see
//! [`leaves_a_drop_to_the_storage_keys`](crate::native::plan::leaves_a_drop_to_the_storage_keys).
//!
//! The pass is idempotent: a merge answers `false` to [`duplicates_canonical_labels`], so it is
//! safe to run at every segment boundary. Placement of the merge is the lowering's decision.

use std::sync::Arc;

use datafusion::error::Result as DfResult;

use crate::native::plan::{
    NativeExpr, NativeNode, NativeTree, NativeType, duplicates_canonical_labels,
};

#[cfg(test)]
mod tests;

/// Rewrites `tree` so that every consumer of a whole canonical series is handed one.
///
/// `source_tags` and `source_metrics` are indexed by each leaf's `source_index`: the label names a
/// source reports, and the one metric it is bound to (`None` when it may hand over several). A
/// source without facts proves nothing, so a rewrite over it is collected. Bindings and
/// `source_index`es are unchanged.
pub fn merge_label_rewrites(
    tree: &NativeTree,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
) -> DfResult<NativeTree> {
    let root = rewrite(tree.root(), source_tags, source_metrics)?;
    NativeTree::try_new(canonical(root, source_tags, source_metrics))
}

/// Wraps `node` in a [`NativeExpr::Merge`] when it is an evaluated vector, not already a merge, and
/// may hand over two series of one label set.
fn canonical(
    node: NativeNode,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
) -> NativeNode {
    if !matches!(&node.output, NativeType::Samples(schema) if schema.eval_grid.is_some()) {
        return node;
    }
    if matches!(node.expr, NativeExpr::Merge { .. }) {
        return node;
    }
    if !duplicates_canonical_labels(&node, source_tags, source_metrics) {
        return node;
    }
    let output = node.output.clone();
    NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(node),
        },
        output,
    }
}

fn rewrite(
    node: &NativeNode,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
) -> DfResult<NativeNode> {
    let expr = match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {
            node.expr.clone()
        }
        // Both keep each sample's position, so no occupancy is lost by not collecting here.
        NativeExpr::Instant { input, op } => NativeExpr::Instant {
            input: Box::new(rewrite(input, source_tags, source_metrics)?),
            op: op.clone(),
        },
        NativeExpr::Timestamp { input } => NativeExpr::Timestamp {
            input: Box::new(rewrite(input, source_tags, source_metrics)?),
        },
        NativeExpr::Merge { input } => NativeExpr::Merge {
            input: Box::new(rewrite(input, source_tags, source_metrics)?),
        },
        NativeExpr::Window { input, op } => NativeExpr::Window {
            input: Box::new(canonical(
                rewrite(input, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            op: op.clone(),
        },
        NativeExpr::Map { input, op } => NativeExpr::Map {
            input: Box::new(canonical(
                rewrite(input, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            op: op.clone(),
        },
        // Scalar parameters carry no label set, so only the input is collected.
        NativeExpr::ParameterizedMap {
            input,
            op,
            parameters,
        } => NativeExpr::ParameterizedMap {
            input: Box::new(canonical(
                rewrite(input, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            op: *op,
            parameters: parameters
                .iter()
                .map(|parameter| rewrite(parameter, source_tags, source_metrics))
                .collect::<DfResult<Vec<_>>>()?,
        },
        NativeExpr::Aggregate { input, op } => NativeExpr::Aggregate {
            input: Box::new(canonical(
                rewrite(input, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            op: op.clone(),
        },
        NativeExpr::ParameterizedAggregate {
            input,
            op,
            parameter,
        } => NativeExpr::ParameterizedAggregate {
            input: Box::new(canonical(
                rewrite(input, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            op: op.clone(),
            parameter: Box::new(rewrite(parameter, source_tags, source_metrics)?),
        },
        NativeExpr::Vector { input, op } => NativeExpr::Vector {
            input: Box::new(canonical(
                rewrite(input, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            op: op.clone(),
        },
        // `canonical` leaves a scalar side alone.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => NativeExpr::Binary {
            left: Box::new(canonical(
                rewrite(left, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            right: Box::new(canonical(
                rewrite(right, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            matching: matching.clone(),
        },
        NativeExpr::Set { left, right, op } => NativeExpr::Set {
            left: Box::new(canonical(
                rewrite(left, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            right: Box::new(canonical(
                rewrite(right, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            op: op.clone(),
        },
        NativeExpr::Info {
            base,
            info,
            selector,
        } => NativeExpr::Info {
            base: Box::new(canonical(
                rewrite(base, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            info: Box::new(canonical(
                rewrite(info, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            selector: selector.clone(),
        },
        NativeExpr::VectorOf { input } => NativeExpr::VectorOf {
            input: Box::new(rewrite(input, source_tags, source_metrics)?),
        },
        NativeExpr::ScalarOf { input } => NativeExpr::ScalarOf {
            input: Box::new(canonical(
                rewrite(input, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
        },
        NativeExpr::ParameterizedTopK {
            input,
            op,
            parameter,
        } => NativeExpr::ParameterizedTopK {
            input: Box::new(canonical(
                rewrite(input, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            op: op.clone(),
            parameter: Box::new(rewrite(parameter, source_tags, source_metrics)?),
        },
        NativeExpr::ParameterizedHistogram {
            input,
            op,
            parameters,
        } => NativeExpr::ParameterizedHistogram {
            input: Box::new(canonical(
                rewrite(input, source_tags, source_metrics)?,
                source_tags,
                source_metrics,
            )),
            op: op.clone(),
            parameters: parameters
                .iter()
                .map(|parameter| rewrite(parameter, source_tags, source_metrics))
                .collect::<DfResult<Vec<_>>>()?,
        },
    };
    Ok(NativeNode {
        expr,
        output: node.output.clone(),
    })
}
