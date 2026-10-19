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
/// a grid it is raw samples carrying their own timestamps. Intermediate aggregate state is a
/// different type altogether, so an operation that cannot consume it is rejected by type rather
/// than by convention.
#[derive(Clone, Debug)]
pub enum NativeType {
    /// Samples, evaluated when the schema carries a grid and raw otherwise.
    Samples(Arc<SeriesSchema>),
    /// Intermediate aggregate state, not a query value.
    AggregateState(Arc<AggregateStateSchema>),
}

impl NativeType {
    /// The schema of a sample type, or `None` for aggregate state.
    pub fn samples(&self) -> Option<&Arc<SeriesSchema>> {
        match self {
            Self::Samples(schema) => Some(schema),
            Self::AggregateState(_) => None,
        }
    }

    /// The grid this type is evaluated on, or `None` when it holds raw samples or state.
    pub fn grid(&self) -> Option<&Arc<EvalGrid>> {
        self.samples().and_then(|schema| schema.eval_grid.as_ref())
    }

    /// Whether this type is an evaluated series, which is what operations over values consume.
    pub fn is_evaluated(&self) -> bool {
        self.grid().is_some()
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

/// A scan bound as a node input.
///
/// The binding records what the input already guarantees, so the compiler can check a consumer's
/// requirement instead of assuming it.
#[derive(Clone, Debug)]
pub struct BoundInput {
    /// The samples the input produces.
    pub schema: Arc<SeriesSchema>,
    /// The ordering the input guarantees.
    pub ordering: SeriesOrdering,
}

/// A scalar bound for broadcast.
///
/// A value that varies per grid point is not a scalar: it is an evaluated input, and it travels
/// as one. Only a constant is broadcast, which is why this carries no grid.
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

impl AggregateStage {
    /// Whether this stage produces a query value rather than intermediate state.
    pub fn produces_value(self) -> bool {
        matches!(self, Self::Complete | Self::Final)
    }
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
    /// A scan.
    Input(BoundInput),
    /// A scalar.
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
                ensure_samples(&self.output, "an input")?;
                let schema = self.output.samples().expect("checked above");
                if !Arc::ptr_eq(schema, &input.schema) {
                    return Err(invalid(
                        "an input node's output schema is not the schema it was bound to",
                    ));
                }
            }
            NativeExpr::Scalar(_) => {
                ensure_samples(&self.output, "a scalar")?;
            }
            NativeExpr::Instant { input, op } => {
                input.validate()?;
                // An instant selection reads raw samples and stamps them onto its own grid.
                if input.output.is_evaluated() {
                    return Err(invalid("an instant selection reads evaluated values"));
                }
                ensure_samples(&self.output, "an instant selection")?;
                ensure_grid(&self.output, &op.output_grid, "an instant selection")?;
            }
            NativeExpr::Window { input, op } => {
                input.validate()?;
                // The declared input grid has to be the grid the input actually produces, or the
                // window would read positions the input never wrote.
                match (op.input_grid.as_ref(), input.output.grid()) {
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
                // A map is point-wise over values, so it needs a grid to be point-wise over.
                if !input.output.is_evaluated() {
                    return Err(invalid("a map consumes values that are not evaluated"));
                }
                if op.ops.is_empty() {
                    return Err(invalid("a map holds no operation"));
                }
                ensure_samples(&self.output, "a map")?;
                let input_grid = input.output.grid().expect("checked above");
                ensure_grid(&self.output, input_grid, "a map")?;
            }
            NativeExpr::Aggregate { input, op } => {
                input.validate()?;
                if !input.output.is_evaluated() {
                    return Err(invalid(
                        "an aggregate consumes values that are not evaluated",
                    ));
                }
                match (&self.output, op.stage.produces_value()) {
                    (NativeType::Samples(schema), true) => {
                        ensure_grid(
                            &self.output,
                            input.output.grid().expect("checked above"),
                            "an aggregate",
                        )?;
                        debug_assert!(schema.eval_grid.is_some());
                    }
                    (NativeType::AggregateState(state), false) => {
                        let input_grid = input.output.grid().expect("checked above");
                        if !Arc::ptr_eq(&state.grid, input_grid) {
                            return Err(invalid(
                                "aggregate state is indexed on a grid the input does not produce",
                            ));
                        }
                        if state.group != op.group {
                            return Err(invalid("aggregate state groups differ from the binding"));
                        }
                    }
                    _ => {
                        return Err(invalid(
                            "an aggregate's output type does not match its stage",
                        ));
                    }
                }
            }
            NativeExpr::Binary { left, right, .. } => {
                left.validate()?;
                right.validate()?;
                if !left.output.is_evaluated() || !right.output.is_evaluated() {
                    return Err(invalid(
                        "a binary operation consumes values that are not evaluated",
                    ));
                }
                let left_grid = left.output.grid().expect("checked above");
                let right_grid = right.output.grid().expect("checked above");
                if !Arc::ptr_eq(left_grid, right_grid) {
                    return Err(invalid("a binary operation aligns two different grids"));
                }
                ensure_samples(&self.output, "a binary operation")?;
                ensure_grid(&self.output, left_grid, "a binary operation")?;
            }
        }
        Ok(())
    }
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
