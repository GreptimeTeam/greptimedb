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

//! The bound tree a native PromQL evaluation compiles to.
//!
//! A query is compiled into [`NativeTree`]: [`NativeNode`]s whose operations are already bound to
//! kernels, fields and grids. Nothing in this module interprets a function name, resolves a label
//! or picks a column at run time; those decisions are made before the tree exists, so the hot
//! path only feeds batches to kernels it was handed.
//!
//! The tree is also the unit of legality. [`NativeNode::validate`] checks the contract every
//! operation states about its input, which is what lets a new combination of supported operations
//! be an ordinary tree instead of another hand-written shape.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::EvalGrid;

use crate::map::MathOp;
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::vector_match::BinaryMatching;

/// What one node produces.
///
/// A sample type with an evaluation grid is an evaluated series: one value per grid point. Without
/// a grid it is raw samples carrying their own timestamps. A scalar is one value per grid point
/// and no labels. Intermediate aggregate state is a different type altogether, so an operation
/// that cannot consume it is rejected by type rather than by convention.
#[derive(Clone, Debug)]
pub enum NativeType {
    /// Samples, evaluated when the schema carries a grid and raw otherwise.
    Samples(Arc<SeriesSchema>),
    /// One value per grid point and no labels, which binary operations broadcast.
    ///
    /// PromQL scalars vary with the evaluation time (`time()`, `scalar(v)`), so this carries the
    /// grid it is evaluated on. Its value carrier is a series without labels, but its type is
    /// what keeps a broadcast from becoming label matching.
    Scalar(Arc<EvalGrid>),
    /// Intermediate aggregate state, not a query value.
    AggregateState(Arc<AggregateStateSchema>),
}

impl NativeType {
    /// The schema of a sample type, or `None` for a scalar or aggregate state.
    pub fn samples(&self) -> Option<&Arc<SeriesSchema>> {
        match self {
            Self::Samples(schema) => Some(schema),
            Self::Scalar(_) | Self::AggregateState(_) => None,
        }
    }

    /// The grid this type is evaluated on, or `None` when it holds raw samples or state.
    ///
    /// A scalar is evaluated on its grid like a vector, which is what lets a binary operation
    /// line it up with a vector point by point; only raw samples and state have no positions.
    pub fn grid(&self) -> Option<&Arc<EvalGrid>> {
        match self {
            Self::Samples(schema) => schema.eval_grid.as_ref(),
            Self::Scalar(grid) => Some(grid),
            Self::AggregateState(_) => None,
        }
    }

    /// What this type is, for a rejection that has to say what a consumer was handed.
    fn describe(&self) -> &'static str {
        match self {
            Self::Samples(schema) if schema.eval_grid.is_some() => "an evaluated vector",
            Self::Samples(_) => "raw samples",
            Self::Scalar(_) => "a scalar",
            Self::AggregateState(_) => "aggregate state",
        }
    }

    /// Whether `self` binds the same thing as `other`: the same samples or the same state.
    ///
    /// Bindings are compared by allocation identity, because a compiler that meant one schema or
    /// one state has no reason to allocate it twice. A scalar is produced by a node and never
    /// handed to one by a source, so it never matches.
    fn same_binding(&self, other: &NativeType) -> bool {
        match (self, other) {
            (Self::Samples(mine), Self::Samples(theirs)) => Arc::ptr_eq(mine, theirs),
            (Self::AggregateState(mine), Self::AggregateState(theirs)) => Arc::ptr_eq(mine, theirs),
            _ => false,
        }
    }
}

/// The layout of intermediate aggregate state.
///
/// State is indexed by `(group labels, absolute grid index)`, so it carries the grid it was
/// accumulated on. It is not a query value and does not pretend to be one.
#[derive(Clone, Debug)]
pub struct AggregateStateSchema {
    /// The labels a group is identified by, in output order.
    pub group: Arc<[Arc<str>]>,
    /// The accumulator columns, one per aggregated field.
    pub accumulators: Arc<[FieldSpec]>,
    /// The grid the state is indexed on.
    pub grid: Arc<EvalGrid>,
}

/// A scan or exchange boundary bound as a node input.
///
/// The binding records what the input already guarantees, so the compiler can check a consumer's
/// requirement instead of assuming it.
#[derive(Clone, Debug)]
pub struct BoundInput {
    /// What the input produces: raw samples from a scan, or state from an exchange boundary.
    pub output: NativeType,
    /// The ordering the input guarantees.
    pub ordering: SeriesOrdering,
}

/// A constant scalar bound for broadcast.
///
/// A scalar that varies per grid point is the same [`NativeType::Scalar`] as this constant: what
/// keeps a binary operation from label-matching it is its type, not its constness. This binding
/// only holds the constant case.
#[derive(Clone, Debug)]
pub struct BoundScalar {
    /// The constant value.
    pub value: f64,
}

/// An instant selection: the last sample at or before each grid point, within a lookback.
#[derive(Clone, Debug)]
pub struct BoundInstant {
    /// How far back a sample may lie before it counts as stale.
    pub lookback_ms: i64,
    /// Selector offset; positive reads earlier samples.
    pub offset_ms: i64,
    /// The grid the selection produces values on.
    pub output_grid: Arc<EvalGrid>,
}

/// A window function over raw samples or over an evaluated input.
///
/// The input and output grids are bound separately: a subquery evaluates its inner expression on
/// its own grid and consumes the result directly, without resampling it onto the outer grid.
#[derive(Clone, Debug)]
pub struct BoundWindow {
    /// The function this node runs.
    pub function: RangeFunction,
    /// The window length, offset and anchor.
    pub window: RangeWindow,
    /// The grid the input is evaluated on, or `None` when the input is raw samples.
    pub input_grid: Option<Arc<EvalGrid>>,
    /// The grid this node produces values on.
    pub output_grid: Arc<EvalGrid>,
}

/// A chain of point-wise operations applied to a whole evaluated value.
#[derive(Clone, Debug)]
pub struct BoundMap {
    /// The operations, applied in order.
    pub ops: Arc<[MathOp]>,
}

/// Which aggregate a node computes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateKind {
    /// `sum` over the values of each group at each grid point.
    Sum,
}

/// Where an aggregate sits in the execution, which decides whether its output is a query value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateStage {
    /// Every contribution to each group lies within this execution, so the output is complete.
    Complete,
    /// Only this execution's contributions were seen, so the output is state for a later merge.
    Partial,
    /// The state was collected from every contributing execution, so the output is complete.
    Final,
}

/// An aggregate bound to its kind, grouping and stage.
#[derive(Clone, Debug)]
pub struct BoundAggregate {
    /// Which aggregate to compute.
    pub kind: AggregateKind,
    /// The labels a group is identified by.
    pub group: Arc<[Arc<str>]>,
    /// Where this aggregate sits in the execution.
    pub stage: AggregateStage,
}

/// What a node computes. Every variant holds a bound operation, never a name to resolve.
#[derive(Clone, Debug)]
pub enum NativeExpr {
    /// A scan or an exchange boundary.
    Input(BoundInput),
    /// A constant scalar, evaluated on the statement's grid.
    Scalar(BoundScalar),
    /// An instant selection over its input.
    Instant {
        input: Box<NativeNode>,
        op: BoundInstant,
    },
    /// A window function over its input.
    Window {
        input: Box<NativeNode>,
        op: BoundWindow,
    },
    /// Point-wise operations over its input.
    Map {
        input: Box<NativeNode>,
        op: BoundMap,
    },
    /// An aggregate over its input.
    Aggregate {
        input: Box<NativeNode>,
        op: BoundAggregate,
    },
    /// A binary operation between two inputs, with its matching rules.
    ///
    /// Scalar operands are broadcast, so `matching` only applies to vector–vector operations.
    Binary {
        left: Box<NativeNode>,
        right: Box<NativeNode>,
        matching: BinaryMatching,
    },
}

/// One node of a bound tree.
#[derive(Clone, Debug)]
pub struct NativeNode {
    /// What this node computes, including its bound operands.
    pub expr: NativeExpr,
    /// What this node produces.
    pub output: NativeType,
}

/// A bound evaluation tree.
///
/// The tree itself carries no run-time state: every execution builds its own, so one compiled plan
/// can serve every partition.
#[derive(Clone, Debug)]
pub struct NativeTree {
    root: NativeNode,
}

impl NativeTree {
    /// Creates a tree over `root`, checking that every node's contract holds.
    ///
    /// A rejected tree is a compiler defect rather than a query error: the compiler only builds a
    /// node after deciding that this combination is supported.
    pub fn try_new(root: NativeNode) -> Result<Self> {
        root.validate()?;
        Ok(Self { root })
    }

    /// The root node.
    pub fn root(&self) -> &NativeNode {
        &self.root
    }
}

impl NativeNode {
    /// Checks this node and its inputs against the contract of its operation.
    pub fn validate(&self) -> Result<()> {
        match &self.expr {
            NativeExpr::Input(input) => {
                // An input node's output is exactly the binding it was bound to: a scan hands over
                // the samples it was created for, and an exchange boundary the state.
                if !self.output.same_binding(&input.output) {
                    return Err(invalid(
                        "an input node's output is not the binding it was bound to",
                    ));
                }
            }
            NativeExpr::Scalar(_) => {
                // A scalar's output is the scalar type: one value per grid point and no labels.
                if !matches!(self.output, NativeType::Scalar(_)) {
                    return Err(invalid("a scalar node does not produce a scalar"));
                }
            }
            NativeExpr::Instant { input, op } => {
                input.validate()?;
                // An instant selection reads raw samples and stamps them onto its own grid: an
                // evaluated vector is already stamped, and a scalar has no timestamps at all.
                match &input.output {
                    NativeType::Samples(schema) if schema.eval_grid.is_none() => {}
                    NativeType::Samples(_) => {
                        return Err(invalid("an instant selection reads evaluated values"));
                    }
                    other => {
                        return Err(invalid(&format!(
                            "an instant selection consumes raw samples, not {}",
                            other.describe()
                        )));
                    }
                }
                ensure_samples(&self.output, "an instant selection")?;
                ensure_grid(&self.output, &op.output_grid, "an instant selection")?;
            }
            NativeExpr::Window { input, op } => {
                input.validate()?;
                // A window reads a range of samples per grid point, so it consumes samples: state
                // is not a sample stream and a scalar has no timestamps.
                let NativeType::Samples(schema) = &input.output else {
                    return Err(invalid(&format!(
                        "a window consumes samples, not {}",
                        input.output.describe()
                    )));
                };
                // The declared input grid has to be the grid the input actually produces, or the
                // window would read positions the input never wrote.
                match (op.input_grid.as_ref(), schema.eval_grid.as_ref()) {
                    (None, None) => {}
                    (Some(bound), Some(actual)) if Arc::ptr_eq(bound, actual) => {}
                    _ => {
                        return Err(invalid(
                            "a window's declared input grid is not the input's grid",
                        ));
                    }
                }
                ensure_samples(&self.output, "a window")?;
                ensure_grid(&self.output, &op.output_grid, "a window")?;
            }
            NativeExpr::Map { input, op } => {
                input.validate()?;
                // A map is point-wise over an evaluated vector: raw samples have no shared
                // positions to be point-wise over, and a scalar is not a vector.
                let Some(input_grid) = evaluated_grid(&input.output) else {
                    return Err(invalid(&format!(
                        "a map consumes an evaluated vector, not {}",
                        input.output.describe()
                    )));
                };
                if op.ops.is_empty() {
                    return Err(invalid("a map holds no operation"));
                }
                ensure_samples(&self.output, "a map")?;
                ensure_grid(&self.output, input_grid, "a map")?;
            }
            NativeExpr::Aggregate { input, op } => {
                input.validate()?;
                // A stage's contract is a pair of types, not only what it produces: `Partial`
                // turns a vector into state and `Final` turns state back into a vector, so state
                // never sits where a value is expected and a value never sits where state is.
                match op.stage {
                    AggregateStage::Complete => {
                        // Every contribution lies within this execution: vector in, vector out,
                        // both on the input's grid.
                        let Some(input_grid) = evaluated_grid(&input.output) else {
                            return Err(invalid(&format!(
                                "a complete aggregate consumes an evaluated vector, not {}",
                                input.output.describe()
                            )));
                        };
                        ensure_samples(&self.output, "a complete aggregate")?;
                        ensure_grid(&self.output, input_grid, "a complete aggregate")?;
                    }
                    AggregateStage::Partial => {
                        // Only this execution's contributions were seen, so the output is state
                        // for a later merge: on the input's grid, grouped the same way.
                        let Some(input_grid) = evaluated_grid(&input.output) else {
                            return Err(invalid(&format!(
                                "a partial aggregate consumes an evaluated vector, not {}",
                                input.output.describe()
                            )));
                        };
                        let NativeType::AggregateState(state) = &self.output else {
                            return Err(invalid(&format!(
                                "a partial aggregate produces aggregate state, not {}",
                                self.output.describe()
                            )));
                        };
                        if !Arc::ptr_eq(&state.grid, input_grid) {
                            return Err(invalid(
                                "aggregate state is indexed on a grid the input does not produce",
                            ));
                        }
                        if state.group != op.group {
                            return Err(invalid("aggregate state groups differ from the binding"));
                        }
                    }
                    AggregateStage::Final => {
                        // The state was collected from every contributing execution, so the
                        // output is the complete value on the grid that state is indexed on.
                        let NativeType::AggregateState(state) = &input.output else {
                            return Err(invalid(&format!(
                                "a final aggregate consumes aggregate state, not {}",
                                input.output.describe()
                            )));
                        };
                        if state.group != op.group {
                            return Err(invalid("aggregate state groups differ from the binding"));
                        }
                        ensure_samples(&self.output, "a final aggregate")?;
                        ensure_grid(&self.output, &state.grid, "a final aggregate")?;
                    }
                }
            }
            NativeExpr::Binary { left, right, .. } => {
                left.validate()?;
                right.validate()?;
                // A binary operation is point-wise, so both sides are evaluated on the statement's
                // grid: state has no positions at all, and raw samples carry their own timestamps
                // instead of positions on a shared grid.
                let Some(left_grid) = left.output.grid() else {
                    return Err(invalid(&format!(
                        "a binary operation consumes evaluated values, not {} on its left",
                        left.output.describe()
                    )));
                };
                let Some(right_grid) = right.output.grid() else {
                    return Err(invalid(&format!(
                        "a binary operation consumes evaluated values, not {} on its right",
                        right.output.describe()
                    )));
                };
                if !Arc::ptr_eq(left_grid, right_grid) {
                    return Err(invalid("a binary operation aligns two different grids"));
                }
                match (&left.output, &right.output) {
                    // Two scalars make a scalar: there is nothing to match and nothing to keep.
                    (NativeType::Scalar(_), NativeType::Scalar(_)) => {
                        if !matches!(&self.output, NativeType::Scalar(grid) if Arc::ptr_eq(grid, left_grid))
                        {
                            return Err(invalid(
                                "two scalars do not produce a scalar on the statement's grid",
                            ));
                        }
                    }
                    // A scalar side carries no labels and no fields, so the operation is a
                    // broadcast on the statement's grid and `matching` is not consulted. A scalar
                    // has no fields of its own, so the broadcast keeps the vector side's fields.
                    (NativeType::Scalar(_), NativeType::Samples(vector))
                    | (NativeType::Samples(vector), NativeType::Scalar(_)) => {
                        let Some(output) = self.output.samples() else {
                            return Err(invalid(&format!(
                                "a scalar broadcast produces the vector side's samples, not {}",
                                self.output.describe()
                            )));
                        };
                        ensure_grid(&self.output, left_grid, "a scalar broadcast")?;
                        if output.fields.len() != vector.fields.len() {
                            return Err(invalid(
                                "a scalar broadcast does not produce the vector side's fields",
                            ));
                        }
                    }
                    // Two vectors are matched by `matching` on the grid they share.
                    (NativeType::Samples(_), NativeType::Samples(_)) => {
                        if self.output.samples().is_none() {
                            return Err(invalid(&format!(
                                "a binary operation over two vectors produces samples, not {}",
                                self.output.describe()
                            )));
                        }
                        ensure_grid(&self.output, left_grid, "a binary operation")?;
                    }
                    // Both sides carry a grid, so this is state, which the checks above reject.
                    // Kept as an error rather than a panic so a future type cannot slip through.
                    _ => {
                        return Err(invalid(
                            "a binary operation consumes aggregate state, not a value",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// The grid of an evaluated vector, or `None` when `ty` is raw samples, a scalar or state.
///
/// An operation over values needs both halves: that its input is samples at all, which rules out
/// a scalar, and that those samples are stamped onto a grid, which rules out raw samples.
fn evaluated_grid(ty: &NativeType) -> Option<&Arc<EvalGrid>> {
    ty.samples().and_then(|schema| schema.eval_grid.as_ref())
}

fn ensure_samples(ty: &NativeType, what: &str) -> Result<()> {
    if matches!(ty, NativeType::Samples(_)) {
        Ok(())
    } else {
        Err(invalid(&format!("{what} does not produce samples")))
    }
}

fn ensure_grid(ty: &NativeType, expected: &Arc<EvalGrid>, what: &str) -> Result<()> {
    match ty.grid() {
        Some(actual) if Arc::ptr_eq(actual, expected) => Ok(()),
        Some(_) => Err(invalid(&format!(
            "{what} declares a grid its output does not carry"
        ))),
        None => Err(invalid(&format!("{what} produces values without a grid"))),
    }
}

fn invalid(reason: &str) -> DataFusionError {
    // A rejected tree is a compiler defect, not a query error: the compiler only builds a node
    // after deciding that this combination is supported.
    DataFusionError::Plan(reason.to_string())
}

#[cfg(test)]
mod tests;
