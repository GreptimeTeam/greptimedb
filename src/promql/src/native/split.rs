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

//! Where a native aggregate is split so two planes can run it.
//!
//! A `Complete` aggregate folds every contribution to a group, which is only correct when one
//! execution reads all of them. A raw-series scan hands its partitions to separate executions, so the
//! aggregate is split instead: the per-series prefix of the tree runs below the aggregate in the
//! partitions that read series, that aggregate becomes a `Partial` one exporting the state each
//! partition folded, and everything above it runs over the merged series.
//!
//! The split is structural. The innermost aggregate of the tree is replaced by an input node at its
//! own output - that is where state enters - and the nodes above it become the suffix. What cannot be
//! split correctly is refused with [`DataFusionError::NotImplemented`], which the planner reads as
//! "plan this query the existing way".

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DfResult};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};

use crate::map::MathOp;
use crate::native::plan::{
    AggregateKind, AggregateStage, AggregateStateSchema, BoundAggregate, BoundInput, BoundVectorOp,
    Grouping, MapOp, NativeExpr, NativeNode, NativeTree, NativeType, aggregate_state_binding,
};

pub(crate) struct Split {
    /// The per-series tree below the cut aggregate, which the producer runs.
    prefix: NativeNode,
    /// The tree above it, starting at the input the merged values enter at.
    suffix: NativeNode,
    /// The aggregate that was cut out.
    op: BoundAggregate,
    /// What that aggregate produces: the layout the exported state is indexed by.
    schema: Arc<SeriesSchema>,
}

/// Splits the innermost aggregate out of `node`.
///
/// `Ok(None)` is a subtree holding no aggregate. What may sit above the cut is only what answers
/// each group on its own: a map and a binary operation with a constant on one side are point-wise,
/// so a partition that owns a group computes that group's answer from its own rows. Every other
/// operation is refused, because the groups a `Final` aggregate owns are split across partitions
/// and such an operation would have to see all of them: `topk` ranks a whole vector, a second
/// aggregate folds one, a vector-vector match pairs two, and a classic histogram function reads
/// the bucket series that share a label set. Those shapes keep the plan the existing route builds
/// until the boundary each of them needs is planned.
///
/// An instant selection or window above the cut cannot be applied to the merged values under the
/// tree's current binding: the cut output is evaluated, whereas these nodes consume raw samples.
pub(crate) fn split(
    node: &NativeNode,
    fields: &[FieldSpec],
    time_index: &str,
) -> DfResult<Option<Split>> {
    match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => Ok(None),
        NativeExpr::Map { input, op } => {
            let Some(mut inner) = split(input, fields, time_index)? else {
                return Ok(None);
            };
            inner.suffix = NativeNode {
                expr: NativeExpr::Map {
                    input: Box::new(inner.suffix),
                    op: op.clone(),
                },
                output: node.output.clone(),
            };
            Ok(Some(inner))
        }
        NativeExpr::Aggregate { input, op } => {
            check_cut(op, fields, time_index)?;
            match split(input, fields, time_index)? {
                // A second aggregate above the cut folds the merged values of every group, which
                // no partition owns on its own: it would have to be replanned as its own
                // `Partial`/`Final` pair above the first one's result.
                Some(_) => Err(declined("an aggregate above a split aggregate")),
                None => {
                    admit_prefix(input)?;
                    let NativeType::Samples(schema) = &node.output else {
                        return Err(declined("an aggregate that produces no values"));
                    };
                    if schema.eval_grid.is_none() {
                        return Err(declined("an aggregate that produces no values"));
                    }
                    Ok(Some(Split {
                        prefix: (**input).clone(),
                        // The merged values enter the suffix as the aggregate's own output: the same
                        // binding and the same value, which is what the tree validates, at source
                        // `0` of the suffix's own execution - a source index is local to one
                        // execution, so the suffix names its one input rather than inheriting the
                        // prefix's.
                        suffix: NativeNode {
                            expr: NativeExpr::Input(BoundInput {
                                source_index: 0,
                                output: node.output.clone(),
                                ordering: SeriesOrdering::Contiguous,
                            }),
                            output: node.output.clone(),
                        },
                        op: op.clone(),
                        schema: Arc::clone(schema),
                    }))
                }
            }
        }
        // A vector operation reads the whole vector at a grid point, and the groups a `Final`
        // aggregate owns are only part of it.
        NativeExpr::Vector {
            op: BoundVectorOp::TopK { .. } | BoundVectorOp::Histogram(_),
            ..
        } => Err(declined("a whole-vector operation above a split aggregate")),
        NativeExpr::Merge { .. } => Err(declined("an aggregate split across a canonical merge")),
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::Vector { input, .. }
        | NativeExpr::VectorOf { input }
        | NativeExpr::ParameterizedMap { input, .. } => match split(input, fields, time_index)? {
            Some(_) => Err(declined(
                "an aggregate under an operation that reads evaluated values",
            )),
            None => Ok(None),
        },
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            // Each side is split once, and at most one of them may hold the innermost aggregate: two
            // would be two cuts, which this node cannot merge.
            let (left_split, right_split) = (
                split(left, fields, time_index)?,
                split(right, fields, time_index)?,
            );
            // A constant operand is broadcast over whatever the other side answers, so applying the
            // same operation to the merged values is the operation the query wrote. Any other sibling
            // would have to be matched with those values by label, which nothing below this node
            // gathers.
            let (mut inner, constant_on_the_left) = match (left_split, right_split) {
                (None, None) => return Ok(None),
                (Some(inner), None) if matches!(right.expr, NativeExpr::Scalar(_)) => {
                    (inner, false)
                }
                (None, Some(inner)) if matches!(left.expr, NativeExpr::Scalar(_)) => (inner, true),
                _ => {
                    return Err(declined(
                        "an aggregate under a binary operation without a constant on the other side",
                    ));
                }
            };
            // The cut side's own suffix takes the place of the aggregate, on the side the query wrote
            // it: a non-commutative operator means what that order says, and the matching and the
            // output are the ones the compiler bound.
            let (left, right) = if constant_on_the_left {
                ((**left).clone(), inner.suffix)
            } else {
                (inner.suffix, (**right).clone())
            };
            inner.suffix = NativeNode {
                expr: NativeExpr::Binary {
                    left: Box::new(left),
                    right: Box::new(right),
                    matching: matching.clone(),
                },
                output: node.output.clone(),
            };
            Ok(Some(inner))
        }
        // A set operation keeps samples by whether their match group is present on the other side,
        // which the merged values of one side cannot answer.
        NativeExpr::Set { left, right, .. } => {
            match (
                split(left, fields, time_index)?,
                split(right, fields, time_index)?,
            ) {
                (None, None) => Ok(None),
                _ => Err(declined("an aggregate under a set operation")),
            }
        }
        // The suffix above a split aggregate reads the merged state alone, so it has no source for
        // the info series.
        NativeExpr::Info { base, info, .. } => {
            match (
                split(base, fields, time_index)?,
                split(info, fields, time_index)?,
            ) {
                (None, None) => Ok(None),
                _ => Err(declined("an aggregate under info")),
            }
        }
    }
}

/// The field names of the raw series the prefix reads, in source order.
///
/// Read off the tree's own raw input node, which is the one authority on what the prefix is
/// compiled against: the root is already evaluated on the statement's grid, so its fields are the
/// statement's value columns rather than the raw ones a `by(...)` clause would name. The prefix is
/// one series at a time and the tree reads exactly one source, so the first raw input answers for
/// all of them.
pub(crate) fn prefix_fields(tree: &NativeTree) -> DfResult<Vec<FieldSpec>> {
    fn raw_fields(node: &NativeNode) -> Option<Vec<FieldSpec>> {
        match &node.expr {
            NativeExpr::Input(input) => input
                .output
                .samples()
                .filter(|schema| schema.eval_grid.is_none())
                .map(|schema| schema.fields.to_vec()),
            NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => None,
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. }
            | NativeExpr::VectorOf { input } => raw_fields(input),
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            } => raw_fields(input).or_else(|| parameters.iter().find_map(raw_fields)),
            NativeExpr::Binary { left, right, .. }
            | NativeExpr::Set { left, right, .. }
            | NativeExpr::Info {
                base: left,
                info: right,
                ..
            } => raw_fields(left).or_else(|| raw_fields(right)),
        }
    }

    raw_fields(tree.root()).ok_or_else(|| declined("a tree that reads no raw series"))
}

/// Refuses an aggregate whose state the split cannot carry, and one grouped by a value field.
///
/// Only the eight fixed-width accumulators are carried: a quantile's state is the group's samples, and
/// a non-`Complete` stage is a state or a merge rather than an answer. A `by(...)` naming a value field
/// or the time index cannot be kept either - the state rows are indexed by field and by grid point,
/// so a group that varies with either is not a group - while a label the input does not have is
/// simply absent, exactly as it is in the existing planner.
pub(crate) fn check_cut(
    op: &BoundAggregate,
    fields: &[FieldSpec],
    time_index: &str,
) -> DfResult<()> {
    if op.stage != AggregateStage::Complete {
        return Err(declined("an aggregate that is not `Complete`"));
    }
    if matches!(op.kind, AggregateKind::Quantile { .. }) {
        return Err(declined("a `quantile` aggregate"));
    }
    if let Grouping::By(labels) = &op.group
        && labels.iter().any(|label| {
            label.as_ref() == time_index
                || fields
                    .iter()
                    .any(|field| field.name.as_ref() == label.as_ref())
        })
    {
        return Err(declined("an aggregate grouped by a value field"));
    }
    Ok(())
}

/// Checks that the per-series prefix is a tree one partition can evaluate on its own.
///
/// The producer runs one execution per partition and folds exactly the series that partition's scan
/// returned, so the prefix has to answer one series from one series' samples: maps are point-wise, a
/// binary operation with a constant side applies that constant to each series its other side answers,
/// and a window or an instant selection reads the raw input itself. A raw input means no evaluation
/// grid - a tree whose input is already evaluated is a subquery's, which one partition cannot evaluate.
///
/// A `clamp` whose maximum is below its minimum answers no value at all, so the fold would see a group
/// with no sample of that field rather than the query the reader wrote.
pub(crate) fn admit_prefix(root: &NativeNode) -> DfResult<()> {
    let mut node = root;
    loop {
        match &node.expr {
            NativeExpr::Map { input, op } => {
                for map_op in op.ops.iter() {
                    if let MapOp::Math(MathOp::Clamp { min, max }) = map_op
                        && max < min
                    {
                        return Err(declined("a `clamp` whose maximum is below its minimum"));
                    }
                }
                node = input;
            }
            // Only a constant side is broadcast: it reaches each series the other side answers, so
            // that side is the one the fold runs. Two constants answer one value per grid point and
            // read no series, and two vectors are a label match over the whole vector, which is not
            // what either half of the split produces.
            NativeExpr::Binary { left, right, .. } => {
                node = match (&left.expr, &right.expr) {
                    (NativeExpr::Scalar(_), NativeExpr::Scalar(_)) => {
                        return Err(declined("a per-series prefix of two constants"));
                    }
                    (NativeExpr::Scalar(_), _) => &**right,
                    (_, NativeExpr::Scalar(_)) => &**left,
                    (_, _) => {
                        return Err(declined(
                            "a per-series prefix that matches two vectors by label",
                        ));
                    }
                };
            }
            _ => break,
        }
    }
    let raw = |node: &NativeNode| match &node.expr {
        NativeExpr::Input(input) => input
            .output
            .samples()
            .is_some_and(|schema| schema.eval_grid.is_none()),
        _ => false,
    };
    match &node.expr {
        NativeExpr::Instant { input, .. } | NativeExpr::Window { input, .. } if raw(input) => {
            Ok(())
        }
        _ => Err(declined(
            "a per-series prefix that is not one series at a time",
        )),
    }
}

pub(crate) fn declined(reason: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native aggregate split does not carry {reason}"
    ))
}

/// A split aggregate as the two trees that run it, and the state they exchange between them.
///
/// The prefix runs where the series are read and ends at a `Partial` aggregate exporting the state
/// each partition folded. The suffix runs where every partition's state has arrived and starts at a
/// `Final` aggregate consuming it, so nothing above the cut sees a partial contribution.
pub struct SplitTrees {
    /// The prefix, with the cut aggregate bound over it as `Partial`; it answers aggregate state.
    pub partial: NativeTree,
    /// The `Final` aggregate over that state, and everything the split moved above the cut.
    pub final_tree: NativeTree,
}

impl Split {
    /// Binds this split as the two trees that run it.
    ///
    /// `Ok(None)` is an aggregate whose kernel keeps no mergeable state, which cannot cross a
    /// partition boundary at all.
    pub fn into_trees(self) -> DfResult<Option<SplitTrees>> {
        let Self {
            prefix,
            suffix,
            op,
            schema,
        } = self;
        let Some((kind, components)) = aggregate_state_binding(op.kind) else {
            return Ok(None);
        };
        let Some(grid) = schema.eval_grid.clone() else {
            return Err(DataFusionError::Internal(
                "a split aggregate has no evaluation grid".into(),
            ));
        };
        let state = Arc::new(AggregateStateSchema {
            group: op.group.clone(),
            kind,
            components: components.into(),
            accumulators: schema.fields.clone(),
            grid,
        });
        let partial = NativeTree::try_new(NativeNode {
            expr: NativeExpr::Aggregate {
                input: Box::new(prefix),
                op: BoundAggregate {
                    stage: AggregateStage::Partial,
                    ..op.clone()
                },
            },
            output: NativeType::AggregateState(Arc::clone(&state)),
        })?;
        let merged = NativeNode {
            expr: NativeExpr::Aggregate {
                input: Box::new(NativeNode {
                    expr: NativeExpr::Input(BoundInput {
                        source_index: 0,
                        output: NativeType::AggregateState(Arc::clone(&state)),
                        ordering: SeriesOrdering::Contiguous,
                    }),
                    output: NativeType::AggregateState(Arc::clone(&state)),
                }),
                op: BoundAggregate {
                    stage: AggregateStage::Final,
                    ..op
                },
            },
            output: NativeType::Samples(Arc::clone(&schema)),
        };
        let root = enter_suffix(suffix, &merged)?;
        Ok(Some(SplitTrees {
            partial,
            final_tree: NativeTree::try_new(root)?,
        }))
    }
}

/// Puts `merged` where the suffix reads its values, returning the suffix above it.
///
/// The suffix is the spine the split built while unwinding, so its leaf is the one input node and
/// every node between that leaf and the root is one of the operations the walk admits. A node of
/// another shape would be a split this code did not build.
fn enter_suffix(node: NativeNode, merged: &NativeNode) -> DfResult<NativeNode> {
    let NativeNode { expr, output } = node;
    let rejected = || {
        DataFusionError::Internal("the split suffix holds an operation it does not build".into())
    };
    let expr = match expr {
        NativeExpr::Input(_) => return Ok(merged.clone()),
        // A constant carries no state: the suffix keeps it as it is, beside the merged values.
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {
            return Ok(NativeNode { expr, output });
        }
        NativeExpr::Map { input, op } => NativeExpr::Map {
            input: Box::new(enter_suffix(*input, merged)?),
            op,
        },
        NativeExpr::Aggregate { input, op } => NativeExpr::Aggregate {
            input: Box::new(enter_suffix(*input, merged)?),
            op,
        },
        NativeExpr::Vector { input, op } => NativeExpr::Vector {
            input: Box::new(enter_suffix(*input, merged)?),
            op,
        },
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => NativeExpr::Binary {
            left: Box::new(enter_suffix(*left, merged)?),
            right: Box::new(enter_suffix(*right, merged)?),
            matching,
        },
        _ => return Err(rejected()),
    };
    Ok(NativeNode { expr, output })
}
