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
//! A `Complete` aggregate is only correct when one execution reads every contribution. Over a
//! partitioned raw-series scan, the innermost aggregate is split: the per-series prefix runs in each
//! partition under a `Partial` aggregate, and the suffix above the cut runs over a `Final` aggregate
//! of the merged state.
//!
//! What cannot be split correctly is refused with [`DataFusionError::NotImplemented`], which the
//! planner reads as "plan this query the existing way".

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
    /// The per-series tree below the cut aggregate.
    prefix: NativeNode,
    /// The tree above the cut, with an input node where the merged values enter.
    suffix: NativeNode,
    op: BoundAggregate,
    /// The cut aggregate's output, which the exported state is indexed by.
    schema: Arc<SeriesSchema>,
    /// The suffix reads whole histograms rather than individual groups.
    histogram: bool,
    /// The suffix rewrites at least one label of the histogram routing key.
    label_rewrite: bool,
}

/// Whether every histogram-routing key of this aggregate is necessarily empty.
pub(crate) fn histogram_group_is_single(group: &Grouping) -> bool {
    matches!(group, Grouping::By(labels) if labels.iter().all(|label| {
        crate::histogram_eval::HISTOGRAM_ROUTING_EXCLUDED_LABELS.contains(&label.as_ref())
    }))
}

/// Splits the innermost aggregate out of `node`; `Ok(None)` when `node` holds no aggregate.
///
/// The suffix may hold maps, constant-sided binaries, subquery windows and one histogram function.
/// A label rewrite that changes a label of the cut's routing key is refused alongside a partitioned
/// histogram, because the state router co-locates buckets by that key. A histogram parameter must
/// read no series (see [`suffix_carries`]).
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
            let changes_key = op.ops.iter().any(|op| {
                let MapOp::Label(rewrite) = op else {
                    return false;
                };
                let destination = rewrite.destination();
                !crate::histogram_eval::HISTOGRAM_ROUTING_EXCLUDED_LABELS.contains(&destination)
                    && match &inner.op.group {
                        Grouping::By(labels) => {
                            labels.iter().any(|label| label.as_ref() == destination)
                        }
                        Grouping::Without(labels) => {
                            !labels.iter().any(|label| label.as_ref() == destination)
                        }
                    }
            });
            if changes_key && inner.histogram && !histogram_group_is_single(&inner.op.group) {
                return Err(declined(
                    "a routing-label rewrite around a partitioned histogram",
                ));
            }
            inner.label_rewrite |= changes_key;
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
                        // Source indices are local to one execution; the suffix has one input.
                        suffix: NativeNode {
                            expr: NativeExpr::Input(BoundInput {
                                source_index: 0,
                                output: node.output.clone(),
                                ordering: SeriesOrdering::Contiguous,
                                time_unit: Default::default(),
                            }),
                            output: node.output.clone(),
                        },
                        op: op.clone(),
                        schema: Arc::clone(schema),
                        histogram: false,
                        label_rewrite: false,
                    }))
                }
            }
        }
        NativeExpr::Vector {
            input,
            op: BoundVectorOp::Histogram(function),
        } => {
            let Some(mut inner) = split(input, fields, time_index)? else {
                return Ok(None);
            };
            // Two histogram calls may drop different labels at successive levels (in particular,
            // a native histogram retains `le`), so the first call's partition key is not enough.
            if inner.histogram {
                return Err(declined("nested histograms above a split aggregate"));
            }
            if inner.label_rewrite && !histogram_group_is_single(&inner.op.group) {
                return Err(declined(
                    "a routing-label rewrite around a partitioned histogram",
                ));
            }
            inner.histogram = true;
            inner.suffix = NativeNode {
                expr: NativeExpr::Vector {
                    input: Box::new(inner.suffix),
                    op: BoundVectorOp::Histogram(*function),
                },
                output: node.output.clone(),
            };
            Ok(Some(inner))
        }
        NativeExpr::ParameterizedHistogram {
            input,
            op,
            parameters,
        } => {
            let Some(mut inner) = split(input, fields, time_index)? else {
                return Ok(None);
            };
            if inner.histogram {
                return Err(declined("nested histograms above a split aggregate"));
            }
            if inner.label_rewrite && !histogram_group_is_single(&inner.op.group) {
                return Err(declined(
                    "a routing-label rewrite around a partitioned histogram",
                ));
            }
            if parameters
                .iter()
                .any(|parameter| !suffix_carries(parameter))
            {
                return Err(declined(
                    "a histogram parameter that reads the series the split reads",
                ));
            }
            inner.histogram = true;
            inner.suffix = NativeNode {
                expr: NativeExpr::ParameterizedHistogram {
                    input: Box::new(inner.suffix),
                    op: op.clone(),
                    parameters: parameters.clone(),
                },
                output: node.output.clone(),
            };
            Ok(Some(inner))
        }
        NativeExpr::Vector {
            op: BoundVectorOp::TopK { .. },
            ..
        } => Err(declined("a whole-vector operation above a split aggregate")),
        // Always a `quantile`, which `check_cut` declines.
        NativeExpr::ParameterizedAggregate { .. } => Err(declined("a `quantile` aggregate")),
        // A merge in the prefix would see one partition's pieces only, so colliding series in
        // different partitions would be folded instead of reported as
        // `vector cannot contain metrics with the same labelset`. The merge must stay a boundary
        // above every partition (`canonical_boundary_stages`).
        NativeExpr::Merge { .. } => Err(declined("an aggregate split across a canonical merge")),
        // Only a subquery window, whose input is evaluated, can sit above the cut.
        NativeExpr::Window { input, op } => {
            let Some(mut inner) = split(input, fields, time_index)? else {
                return Ok(None);
            };
            let input_grid = input
                .output
                .samples()
                .and_then(|schema| schema.eval_grid.as_ref());
            if input_grid.is_none() {
                return Err(declined("a window over raw samples above an aggregate"));
            }
            inner.suffix = NativeNode {
                expr: NativeExpr::Window {
                    input: Box::new(inner.suffix),
                    op: op.clone(),
                },
                output: node.output.clone(),
            };
            Ok(Some(inner))
        }
        // None of these is rebuilt above the cut by `enter_suffix`: an instant selection reads raw
        // samples, `scalar()` and a ranking read the whole vector, and the rest are not supported
        // there.
        NativeExpr::Instant { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::Vector { input, .. }
        | NativeExpr::VectorOf { input }
        | NativeExpr::ScalarOf { input }
        | NativeExpr::ParameterizedMap { input, .. }
        | NativeExpr::ParameterizedTopK { input, .. } => match split(input, fields, time_index)? {
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
            let (left_split, right_split) = (
                split(left, fields, time_index)?,
                split(right, fields, time_index)?,
            );
            // Only a broadcast against a literal applies unchanged to the merged values.
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
        NativeExpr::Set { left, right, .. } => {
            match (
                split(left, fields, time_index)?,
                split(right, fields, time_index)?,
            ) {
                (None, None) => Ok(None),
                _ => Err(declined("an aggregate under a set operation")),
            }
        }
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

/// The fields of the first raw input in `tree`, which a `by(...)` naming a value field would name.
/// The split tree reads exactly one source.
pub(crate) fn prefix_fields(tree: &NativeTree) -> DfResult<Vec<FieldSpec>> {
    tree.root()
        .inputs()
        .into_iter()
        .find_map(|input| {
            input
                .output
                .samples()
                .filter(|schema| schema.eval_grid.is_none())
                .map(|schema| schema.fields.to_vec())
        })
        .ok_or_else(|| declined("a tree that reads no raw series"))
}

/// Refuses a non-`Complete` aggregate, a `quantile` (no fixed-width state), and a `by(...)` naming
/// a value field or the time index.
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

/// Checks that the prefix answers one series from one series' samples: maps and constant-sided
/// binaries over a window or instant selection of a raw input.
///
/// A `clamp` whose maximum is below its minimum answers no value, so it is refused too.
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
            NativeExpr::Merge { .. } => {
                return Err(declined("a per-series prefix that collects a label set"));
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

/// A split aggregate as the two trees that run it.
pub struct SplitTrees {
    /// The prefix under a `Partial` aggregate, answering aggregate state per partition.
    pub partial: NativeTree,
    /// The suffix over a `Final` aggregate of every partition's state.
    pub final_tree: NativeTree,
}

impl Split {
    /// `Ok(None)` when the aggregate has no mergeable state.
    pub fn into_trees(self) -> DfResult<Option<SplitTrees>> {
        let Self {
            prefix,
            suffix,
            op,
            schema,
            histogram: _,
            label_rewrite: _,
        } = self;
        let Some((kind, components)) = aggregate_state_binding(op.kind, op.field_kind) else {
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
                        time_unit: Default::default(),
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

/// Replaces the suffix's input node with `merged`.
fn enter_suffix(node: NativeNode, merged: &NativeNode) -> DfResult<NativeNode> {
    let NativeNode { expr, output } = node;
    let rejected = || {
        DataFusionError::Internal("the split suffix holds an operation it does not build".into())
    };
    let expr = match expr {
        NativeExpr::Input(_) => return Ok(merged.clone()),
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {
            return Ok(NativeNode { expr, output });
        }
        NativeExpr::Window { input, op } => NativeExpr::Window {
            input: Box::new(enter_suffix(*input, merged)?),
            op,
        },
        NativeExpr::Map { input, op } => NativeExpr::Map {
            input: Box::new(enter_suffix(*input, merged)?),
            op,
        },
        NativeExpr::Vector {
            input,
            op: op @ BoundVectorOp::Histogram(_),
        } => NativeExpr::Vector {
            input: Box::new(enter_suffix(*input, merged)?),
            op,
        },
        // `split` checked the parameters read no series, so they are kept as they are.
        NativeExpr::ParameterizedHistogram {
            input,
            op,
            parameters,
        } => NativeExpr::ParameterizedHistogram {
            input: Box::new(enter_suffix(*input, merged)?),
            op,
            parameters,
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

/// Whether [`enter_suffix`] rebuilds this subtree from constants alone. It accepts a subset of the
/// shapes `enter_suffix` builds; a shape outside them would become an `Internal` error there.
fn suffix_carries(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => true,
        NativeExpr::Map { input, .. }
        | NativeExpr::Vector {
            input,
            op: BoundVectorOp::Histogram(_),
        } => suffix_carries(input),
        NativeExpr::Binary { left, right, .. } => suffix_carries(left) && suffix_carries(right),
        _ => false,
    }
}
