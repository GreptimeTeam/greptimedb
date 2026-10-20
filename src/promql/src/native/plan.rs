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
//!
//! What the tree cannot express yet is deliberate for this stage: [`NativeExpr::Binary`] holds an
//! arithmetic or comparison operation, so `and`, `or` and `unless` have no node, and
//! [`NativeExpr::Scalar`] holds a constant, so `time()` and `scalar(v)` have no node either. The
//! legality below is therefore about the operations that exist, not about PromQL's whole surface.

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
    /// handed to one by a source, so it never matches. This is about the binding itself: grids are
    /// aligned by [`EvalGrid::spec`] everywhere else, here and in the kernels.
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
/// The binding records what the input was created for: the output a consumer type-checks against,
/// and the ordering it guarantees. A window reads the ordering, rejecting an interleaved input; no
/// other consumer reads it yet.
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
    /// node after deciding that this combination is supported, so the error is
    /// [`DataFusionError::Internal`] and not a statement the client wrote wrong.
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
                // A selection is one source row per grid point: `select_instants` picks row
                // indices and every field is taken at them, so it carries one value per input
                // field.
                ensure_same_fields(&input.output, &self.output, "an instant selection")?;
                ensure_grid(&self.output, &op.output_grid, "an instant selection")?;
                // `select_instants` reads one series at a time and holds no cross-series state, so
                // an interleaved input is enough for it: no ordering check here.
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
                // A range that is not positive reads nothing at every grid point.
                if op.window.range_ms <= 0 {
                    return Err(invalid(
                        "a window reads an empty range: its range must be positive",
                    ));
                }
                // A range evaluator consumes one series to its last chunk before it starts the
                // next, so interleaved chunks of different series cannot be fed to it. Only an
                // input node carries an ordering, so only there is there one to check.
                if let NativeExpr::Input(binding) = &input.expr
                    && binding.ordering == SeriesOrdering::Interleaved
                {
                    return Err(invalid(
                        "a window reads one series to its last chunk before the next, so \
                         interleaved chunks cannot be fed to it",
                    ));
                }
                // What is checked here and just below is that the declared input grid equals the
                // input's grid, and that the output grid matches the declared one. The grid is
                // not checked to cover the window's read range
                // `(out.start - offset - range, out.end - offset]`: only a subquery feeds an
                // evaluated input to a window, and this compiler does not accept subqueries yet,
                // so that coverage check waits for it.
                match (op.input_grid.as_ref(), schema.eval_grid.as_ref()) {
                    (None, None) => {}
                    (Some(bound), Some(actual)) if bound.spec() == actual.spec() => {}
                    _ => {
                        return Err(invalid(
                            "a window's declared input grid is not the input's grid",
                        ));
                    }
                }
                ensure_samples(&self.output, "a window")?;
                ensure_same_fields(&input.output, &self.output, "a window")?;
                ensure_grid(&self.output, &op.output_grid, "a window")?;
            }
            NativeExpr::Map { input, op } => {
                input.validate()?;
                // A map is point-wise over an evaluated vector: raw samples have no shared
                // positions to be point-wise over, and a scalar is not a vector.
                let Some((_, input_grid)) = evaluated_vector(&input.output) else {
                    return Err(invalid(&format!(
                        "a map consumes an evaluated vector, not {}",
                        input.output.describe()
                    )));
                };
                if op.ops.is_empty() {
                    return Err(invalid("a map holds no operation"));
                }
                ensure_samples(&self.output, "a map")?;
                ensure_same_fields(&input.output, &self.output, "a map")?;
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
                        // both on the input's grid, one value per input field.
                        let Some((_, input_grid)) = evaluated_vector(&input.output) else {
                            return Err(invalid(&format!(
                                "a complete aggregate consumes an evaluated vector, not {}",
                                input.output.describe()
                            )));
                        };
                        ensure_samples(&self.output, "a complete aggregate")?;
                        ensure_same_fields(&input.output, &self.output, "a complete aggregate")?;
                        ensure_grid(&self.output, input_grid, "a complete aggregate")?;
                    }
                    AggregateStage::Partial => {
                        // Only this execution's contributions were seen, so the output is state
                        // for a later merge: on the input's grid, grouped the same way, one
                        // accumulator per input field.
                        let Some((input_schema, input_grid)) = evaluated_vector(&input.output)
                        else {
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
                        ensure_field_count(
                            state.accumulators.len(),
                            input_schema.fields.len(),
                            "a partial aggregate",
                        )?;
                        if state.grid.spec() != input_grid.spec() {
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
                        let output = ensure_samples(&self.output, "a final aggregate")?;
                        // The merged value carries one value per accumulator the state holds.
                        ensure_field_count(
                            output.fields.len(),
                            state.accumulators.len(),
                            "a final aggregate",
                        )?;
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
                if left_grid.spec() != right_grid.spec() {
                    return Err(invalid("a binary operation aligns two different grids"));
                }
                match (&left.output, &right.output) {
                    // Two scalars make a scalar: there is nothing to match and nothing to keep.
                    (NativeType::Scalar(_), NativeType::Scalar(_)) => {
                        if !matches!(&self.output, NativeType::Scalar(grid) if grid.spec() == left_grid.spec())
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
                    // Two vectors are matched by `matching` on the grid they share, and the match
                    // pairs fields by position, so it keeps the shorter side's fields.
                    (NativeType::Samples(left_vector), NativeType::Samples(right_vector)) => {
                        let Some(output) = self.output.samples() else {
                            return Err(invalid(&format!(
                                "a binary operation over two vectors produces samples, not {}",
                                self.output.describe()
                            )));
                        };
                        ensure_field_count(
                            output.fields.len(),
                            left_vector.fields.len().min(right_vector.fields.len()),
                            "a binary operation over two vectors",
                        )?;
                        ensure_grid(&self.output, left_grid, "a binary operation")?;
                    }
                    // State has no grid, so the grid checks above return for either side before
                    // this match and these two arms cannot be reached at run time. They are
                    // spelled out because `NativeType` names `AggregateState` and the match has to
                    // cover the pair; the message says so rather than pretending a value arrived.
                    (NativeType::AggregateState(_), _) | (_, NativeType::AggregateState(_)) => {
                        return Err(invalid(
                            "a binary operation reached aggregate state, which has no grid: the \
                             grid check above should have rejected it",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// The schema and grid of an evaluated vector, or `None` when `ty` is raw samples, a scalar or
/// state.
///
/// An operation over values needs both halves: that its input is samples at all, which rules out
/// a scalar, and that those samples are stamped onto a grid, which rules out raw samples. The
/// schema comes back with the grid because a consumer that needs the grid also declares a field
/// layout for the fields the schema carries.
fn evaluated_vector(ty: &NativeType) -> Option<(&Arc<SeriesSchema>, &Arc<EvalGrid>)> {
    let schema = ty.samples().filter(|schema| schema.eval_grid.is_some())?;
    Some((schema, schema.eval_grid.as_ref()?))
}

/// Checks that a node declares the field layout its input produces.
///
/// A window, a map, an instant selection and a complete aggregate all report one value per input
/// field, so they keep the layout of their input. A node that declares a different number of
/// fields than it consumes describes a computation this model has no meaning for, and letting it
/// through would only surface at run time as a field count mismatch against a schema the tree was
/// supposed to have validated.
fn ensure_same_fields(input: &NativeType, output: &NativeType, what: &str) -> Result<()> {
    let (NativeType::Samples(input), NativeType::Samples(output)) = (input, output) else {
        return Ok(());
    };
    ensure_field_count(output.fields.len(), input.fields.len(), what)
}

/// Checks that a node declares `declared` fields where its input provides `expected`.
///
/// Some nodes compute their layout instead of keeping their input's: a final aggregate produces one
/// value per accumulator the state holds, and a vector-vector binary operation produces one field
/// per matched pair, which is the shorter side's count. Both are checked against the count the
/// kernel produces, so the message names the node and both numbers.
fn ensure_field_count(declared: usize, expected: usize, what: &str) -> Result<()> {
    if declared != expected {
        return Err(invalid(&format!(
            "{what} declares {declared} field(s) but its input provides {expected}"
        )));
    }
    Ok(())
}

/// Checks that `ty` produces samples and returns them.
fn ensure_samples<'a>(ty: &'a NativeType, what: &str) -> Result<&'a Arc<SeriesSchema>> {
    ty.samples()
        .ok_or_else(|| invalid(&format!("{what} does not produce samples")))
}

/// Checks that `ty` is evaluated on the grid `expected` describes.
///
/// The comparison is by [`EvalGrid::spec`], not by allocation: that is the rule the kernels apply
/// (`vector_match::same_grid`), so a compiler that allocates one grid per window but binds equal
/// specs produces a tree the kernel runs correctly.
fn ensure_grid(ty: &NativeType, expected: &Arc<EvalGrid>, what: &str) -> Result<()> {
    match ty.grid() {
        Some(actual) if actual.spec() == expected.spec() => Ok(()),
        Some(_) => Err(invalid(&format!(
            "{what} declares a grid its output does not carry"
        ))),
        None => Err(invalid(&format!("{what} produces values without a grid"))),
    }
}

fn invalid(reason: &str) -> DataFusionError {
    // A rejected tree is a compiler defect, not a query error: the compiler only builds a node
    // after deciding that this combination is supported. `Internal` keeps that meaning on the wire
    // instead of reporting a mistake in the client's statement, which `Plan` would.
    DataFusionError::Internal(reason.to_string())
}

#[cfg(test)]
mod tests;
