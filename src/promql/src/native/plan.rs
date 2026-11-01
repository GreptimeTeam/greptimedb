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
//! What the tree cannot express yet is deliberate for this stage: `scalar(v)` has no node. The
//! legality below is therefore about the operations that exist, not about PromQL's whole surface.

use std::cell::Cell;
use std::hash::Hash;
use std::sync::Arc;

const MAX_NATIVE_NODE_DEPTH: usize = 64;
thread_local! {
    static NATIVE_NODE_DEPTH: Cell<usize> = const { Cell::new(0) };
}

struct NativeNodeDepthGuard;
impl NativeNodeDepthGuard {
    fn enter<E: serde::de::Error>() -> std::result::Result<Self, E> {
        NATIVE_NODE_DEPTH.with(|depth| {
            let current = depth.get();
            if current >= MAX_NATIVE_NODE_DEPTH {
                return Err(E::custom(format!(
                    "native tree nesting exceeds {MAX_NATIVE_NODE_DEPTH}"
                )));
            }
            depth.set(current + 1);
            Ok(Self)
        })
    }
}
impl Drop for NativeNodeDepthGuard {
    fn drop(&mut self) {
        NATIVE_NODE_DEPTH.with(|depth| depth.set(depth.get() - 1));
    }
}

use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::EvalGrid;
use datatypes::series::{SeriesLabels, SeriesOrdering};

use crate::histogram_eval::{HistogramFunction, NativeHistogramFunction};
use crate::info::InfoSelector;
use crate::map::MathOp;
use crate::map_label::LabelRewrite;
use crate::map_time::DateFunction;
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::select::SortOrder;
use crate::vector_match::{BinaryMatching, MatchingLabels, SetOp};

/// What one node produces.
///
/// A sample type with an evaluation grid is an evaluated series: one value per grid point. Without
/// a grid it is raw samples carrying their own timestamps. A scalar is one value per grid point
/// and no labels. Intermediate aggregate state is a different type altogether, so an operation
/// that cannot consume it is rejected by type rather than by convention.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
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
    /// Bindings compare their semantic content, not process-local allocation identity. Scalars are
    /// produced by a node and never handed to one by a source, so they never match.
    fn same_binding(&self, other: &NativeType) -> bool {
        match (self, other) {
            (Self::Samples(mine), Self::Samples(theirs)) => **mine == **theirs,
            (Self::AggregateState(mine), Self::AggregateState(theirs)) => **mine == **theirs,
            _ => false,
        }
    }
}

/// The layout of intermediate aggregate state.
///
/// State is indexed by `(group labels, absolute grid index)`, so it carries the grid it was
/// accumulated on. It is not a query value and does not pretend to be one.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct AggregateStateSchema {
    /// How the groups its rows are indexed by are identified.
    pub group: Grouping,
    /// The accumulator columns, one per aggregated field.
    pub accumulators: Arc<[FieldSpec]>,
    /// The grid the state is indexed on.
    pub grid: Arc<EvalGrid>,
}

/// A scan or exchange boundary bound as a node input.
///
/// The binding records what the input was created for: the output a consumer type-checks against,
/// and the ordering it guarantees. An instant selection and a window both read the ordering,
/// rejecting an interleaved input; no other consumer reads it yet.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct BoundInput {
    /// Child index in the DataFusion plan. The compiler initially assigns selector-occurrence
    /// indices; the caller maps them to child indices when constructing the plan.
    ///
    /// Multiple logical inputs may reference the same child after plan collapse. This is not a
    /// runtime `InputId` or an identity for an input leaf.
    pub source_index: usize,
    /// What the input produces: raw samples from a scan, or state from an exchange boundary.
    pub output: NativeType,
    /// The ordering the input guarantees.
    pub ordering: SeriesOrdering,
}

/// A constant scalar bound for broadcast or a statement root.
///
/// A scalar that varies per grid point is the same [`NativeType::Scalar`] as this constant: what
/// keeps a binary operation from label-matching it is its type, not its constness. The varying
/// ones are [`NativeExpr::EvalTime`] and a binary operation between scalars.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BoundScalar {
    /// The constant value.
    pub value: f64,
}

/// An instant selection: the last sample at or before each grid point, within a lookback.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct BoundInstant {
    /// How far back a sample may lie before it counts as stale.
    pub lookback_ms: i64,
    /// Selector offset; positive reads earlier samples.
    pub offset_ms: i64,
    /// Anchored selection instant; samples are selected at this time for every grid point.
    pub at_ms: Option<i64>,
    /// What the selection reports for the sample it selects.
    pub value: InstantValue,
    /// The grid the selection produces values on.
    pub output_grid: Arc<EvalGrid>,
}

/// What an instant selection reports at a grid point for the sample it selected there.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
pub enum InstantValue {
    /// The sample itself.
    Sample,
    /// `timestamp(x)`: the sample's own time in seconds, without the metric name.
    Timestamp,
    /// The info series `info()` reads: the sample's own time in milliseconds, under all of its
    /// labels, as Prometheus' `evalSeries` records it for `info`.
    InfoTimestamp,
}

/// A window function over raw samples or over an evaluated input.
///
/// The input and output grids are bound separately: a subquery evaluates its inner expression on
/// its own grid and consumes the result directly, without resampling it onto the outer grid.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct BoundMap {
    /// The operations, applied in order.
    pub ops: Arc<[MapOp]>,
}

/// A math function whose scalar parameters are bound as nodes, because at least one of them varies
/// per grid point; Prometheus reads each parameter at every step. With constant parameters the same
/// function is a [`MathOp`] in a [`BoundMap`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
pub enum ParameterizedOp {
    /// `round(v, to_nearest)`.
    Round,
    /// `clamp(v, min, max)`.
    Clamp,
    /// `clamp_min(v, min)`.
    ClampMin,
    /// `clamp_max(v, max)`.
    ClampMax,
}

impl ParameterizedOp {
    /// How many parameters the function reads after its vector.
    pub fn arity(self) -> usize {
        match self {
            Self::Clamp => 2,
            Self::Round | Self::ClampMin | Self::ClampMax => 1,
        }
    }
}

/// One operation of a map, applied to each series on its own: math over its float values, a
/// function of its native histograms, or a change of its labels. Each can give two series one label
/// set, which is why all live in a map.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub enum MapOp {
    Math(MathOp),
    Histogram(NativeHistogramFunction),
    Label(Arc<LabelRewrite>),
}

/// Which aggregate a node computes.
///
/// Every kind here folds a group's values into one value per grid point. The aggregates that
/// instead select some of the input's series (`topk`, `bottomk`, `limitk`, `limit_ratio`) or write
/// one output label per distinct value (`count_values`) are a different shape and have no variant,
/// so a node cannot claim to compute one of them.
///
/// A kind may be bound before its kernel exists: whether the tree can run is
/// [`NativeTree::try_new`]'s and the runtime's question, not this enum's.
///
/// `Eq` is missing because [`AggregateKind::Quantile`] carries an `f64`, as
/// [`RangeFunction::QuantileOverTime`] does.
///
/// [`RangeFunction::QuantileOverTime`]: crate::range_eval::RangeFunction::QuantileOverTime
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum AggregateKind {
    /// `sum`: the total of each group's values at each grid point.
    Sum,
    /// `avg`: the mean of each group's values at each grid point.
    Avg,
    /// `count`: how many values each group has at each grid point.
    Count,
    /// `min`: the smallest of each group's values at each grid point.
    Min,
    /// `max`: the largest of each group's values at each grid point.
    Max,
    /// `group`: `1` where a group has any value at a grid point.
    Group,
    /// `stddev`: the standard deviation of each group's values at each grid point.
    Stddev,
    /// `stdvar`: the variance of each group's values at each grid point.
    Stdvar,
    /// `quantile`: the φ-quantile of each group's values at each grid point, as written.
    Quantile {
        /// The φ the query wrote, which `0` to `1` does not bound: Prometheus reads a negative φ as
        /// `-Infinity` and one above `1` as `+Infinity`.
        quantile: f64,
    },
}

/// Where an aggregate sits in the execution, which decides whether its output is a query value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
pub enum AggregateStage {
    /// Every contribution to each group lies within this execution, so the output is complete.
    Complete,
    /// Only this execution's contributions were seen, so the output is state for a later merge.
    Partial,
    /// The state was collected from every contributing execution, so the output is complete.
    Final,
}

/// How an aggregate's groups are identified, in the binding's own terms.
///
/// The kernel already takes both shapes ([`aggregate::Grouping`]), and the two are not
/// interchangeable: `by(job)` keeps `job` and drops everything else, while `without(job)` keeps
/// every other label and drops `job` too. A list of group labels can only spell the first, so a
/// `without` written that way would group a query by labels it meant to remove.
///
/// [`aggregate::Grouping`]: crate::aggregate::Grouping
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
pub enum Grouping {
    /// The labels a group is identified by; every other label is dropped.
    By(Arc<[Arc<str>]>),
    /// The labels a group is not identified by; these are dropped, and so is `__name__`.
    Without(Arc<[Arc<str>]>),
}

/// An aggregate bound to its kind, grouping and stage.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct BoundAggregate {
    /// Which aggregate to compute.
    pub kind: AggregateKind,
    /// How its groups are identified.
    pub group: Grouping,
    /// Where this aggregate sits in the execution.
    pub stage: AggregateStage,
}

/// An operator over the whole input vector at each grid point: one that keeps some of the input's
/// samples with their series' labels and values, orders the series, reports where none is present,
/// reads a classic histogram from its bucket series, or counts the samples sharing a value.
///
/// Its constants are bound here, as a window's are: a `k` or a ratio only known at run time has no
/// field.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum BoundVectorOp {
    /// `topk` (`top`) or `bottomk`: the `k` largest or smallest values of each group.
    TopK { k: f64, top: bool, group: Grouping },
    /// `limitk`: the first `k` series of each group that hold a sample, in input order.
    LimitK { k: f64, group: Grouping },
    /// `limit_ratio`: the series whose label hash falls in the ratio; it keeps whole series, so it
    /// has no grouping.
    LimitRatio { ratio: f64 },
    /// `sort`, `sort_desc`, `sort_by_label` and `sort_by_label_desc`: the input's series in another
    /// order, which only an instant result shows. `sort` and `sort_desc` also drop histogram
    /// samples, in a range result too.
    Sort(SortOrder),
    /// `absent`: `1` under `labels` at every point where the input holds no sample. The labels are
    /// what the compiler derives from the argument's matchers.
    Absent { labels: SeriesLabels },
    /// `histogram_quantile` or `histogram_fraction`: each classic histogram is read from the bucket
    /// series that share its labels, so this too reads the whole vector at a point.
    Histogram(HistogramFunction),
    /// `count_values(label, v)`: how many samples of each group share a value, under a label set
    /// per value. It writes a label per distinct value, which an aggregate's fixed groups cannot.
    CountValues { label: Arc<str>, group: Grouping },
}

/// A set operator and the labels its two vectors are matched by.
///
/// It has no cardinality, `bool` or fill: PromQL matches a set operator many-to-many and refuses
/// the other modifiers on it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
pub struct BoundSetOp {
    /// `and`, `or` or `unless`.
    pub op: SetOp,
    /// The labels a sample's match group is read from on either side.
    pub labels: MatchingLabels,
}

/// What a node computes. Every variant holds a bound operation, never a name to resolve.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum NativeExpr {
    /// A scan or an exchange boundary.
    Input(BoundInput),
    /// A constant scalar, evaluated on the statement's grid.
    Scalar(BoundScalar),
    /// The evaluation time of each grid point, as `time()` reads it in seconds, or a date function
    /// of it, as a date function called without an argument reads it.
    ///
    /// A date function is bound here rather than over `time()`: Prometheus reads the calendar from
    /// the evaluation timestamp truncated to whole seconds, which the float `time()` does not carry
    /// exactly for every timestamp.
    EvalTime { date: Option<DateFunction> },
    /// `vector(s)`: the scalar as one series without labels.
    VectorOf { input: Box<NativeNode> },
    /// An instant selection over its input.
    Instant {
        input: Box<NativeNode>,
        op: BoundInstant,
    },
    /// Canonicalizes evaluated fragments that share a label set.
    Merge { input: Box<NativeNode> },
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
    /// A point-wise math function over its input whose parameters are scalar nodes, in argument
    /// order.
    ParameterizedMap {
        input: Box<NativeNode>,
        op: ParameterizedOp,
        parameters: Vec<NativeNode>,
    },
    /// The timestamp of an evaluated input, in seconds.
    Timestamp { input: Box<NativeNode> },
    /// An aggregate over its input.
    Aggregate {
        input: Box<NativeNode>,
        op: BoundAggregate,
    },
    /// An operation over the whole input vector at each grid point.
    Vector {
        input: Box<NativeNode>,
        op: BoundVectorOp,
    },
    /// A binary operation between two inputs, with its matching rules.
    ///
    /// Scalar operands are broadcast, so `matching` only applies to vector–vector operations.
    Binary {
        left: Box<NativeNode>,
        right: Box<NativeNode>,
        matching: BinaryMatching,
    },
    /// A set operation between two vectors, which keeps samples of either side as they are.
    Set {
        left: Box<NativeNode>,
        right: Box<NativeNode>,
        op: BoundSetOp,
    },
    /// `info(base, {selector})`: the series of `base` with the data labels of the info series that
    /// share their identifying labels. `info` is the instant selection of those info series, whose
    /// values are the times of the samples it selected ([`InstantValue::InfoTimestamp`]).
    Info {
        base: Box<NativeNode>,
        info: Box<NativeNode>,
        selector: InfoSelector,
    },
}

/// One node of a bound tree.
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq, Hash)]
pub struct NativeNode {
    /// What this node computes, including its bound operands.
    pub expr: NativeExpr,
    /// What this node produces.
    pub output: NativeType,
}

/// Whether `node` reads the whole input vector at a point rather than one series at a time.
///
/// An `Aggregate` folds every series of its group, a `Binary` and a `Set` pair two vectors' series
/// by label, an `Info` looks the info series of each series up by its identifying labels, and a
/// `Vector` operation (`sort`, `absent`, `topk`, `limitk`, `histogram_quantile`, ...) reorders or
/// reads the whole vector at a grid point.
///
/// `Merge` answers one series per label set, and the label sets it answers span every input series:
/// two of them can collapse into the one it emits. It is therefore whole-vector too, which is why it
/// also cannot be moved below a boundary that holds only part of a metric. Everything else - an
/// input, a constant or `time()`, a selection, a window, a map, `vector(s)`, and a binary operation
/// with a scalar on one side, whose broadcast reaches each series the other side answers - answers
/// one series from one series' rows, as long as its own operands do.
///
/// Both a region-side placement and the native route's admission ask this, and they must agree: a
/// tree admitted here but refused there becomes a node the physical layer cannot build, and one
/// refused here but admitted there would be pushed to a region that holds part of the metric.
pub fn reads_across_series(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => false,
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::VectorOf { input } => reads_across_series(input),
        NativeExpr::Binary { left, right, .. } => {
            // A scalar side has no labels and is broadcast over the other side's series unchanged,
            // so the operation reads what its two sides read. Two vectors are matched by label,
            // which pairs series of the whole vector.
            let scalar = |side: &NativeNode| matches!(side.output, NativeType::Scalar(_));
            if scalar(left) || scalar(right) {
                reads_across_series(left) || reads_across_series(right)
            } else {
                true
            }
        }
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => reads_across_series(input) || parameters.iter().any(reads_across_series),
        NativeExpr::Aggregate { .. }
        | NativeExpr::Vector { .. }
        | NativeExpr::Set { .. }
        | NativeExpr::Info { .. }
        | NativeExpr::Merge { .. } => true,
    }
}

/// Whether `node` is a supported child of Merge: only an input with optional windows and maps.
fn merge_shape_supported(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(_) => true,
        NativeExpr::Window { input, .. } | NativeExpr::Map { input, .. } => {
            merge_shape_supported(input)
        }
        _ => false,
    }
}

/// A bound evaluation tree.
///
/// The tree itself carries no run-time state: every execution builds its own, so one compiled plan
/// can serve every partition.
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq, Hash)]
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
        ensure_native_node_depth(&root)?;
        root.validate()?;
        Ok(Self { root })
    }

    /// The root node.
    pub fn root(&self) -> &NativeNode {
        &self.root
    }

    pub(crate) fn validate_depth(&self) -> Result<()> {
        ensure_native_node_depth(&self.root)
    }
}

impl<'de> serde::Deserialize<'de> for NativeNode {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let _guard = NativeNodeDepthGuard::enter::<D::Error>()?;
        #[derive(serde::Deserialize)]
        struct Wire {
            expr: NativeExpr,
            output: NativeType,
        }
        let wire = Wire::deserialize(deserializer)?;
        Ok(Self {
            expr: wire.expr,
            output: wire.output,
        })
    }
}

impl<'de> serde::Deserialize<'de> for NativeTree {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Wire {
            root: NativeNode,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::try_new(wire.root).map_err(serde::de::Error::custom)
    }
}

fn ensure_native_node_depth(root: &NativeNode) -> Result<()> {
    fn visit(node: &NativeNode, depth: usize) -> Result<()> {
        if depth > MAX_NATIVE_NODE_DEPTH {
            return Err(invalid(&format!(
                "native tree nesting exceeds {MAX_NATIVE_NODE_DEPTH}"
            )));
        }
        match &node.expr {
            NativeExpr::VectorOf { input }
            | NativeExpr::Instant { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. } => visit(input, depth + 1),
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            } => {
                visit(input, depth + 1)?;
                for parameter in parameters {
                    visit(parameter, depth + 1)?;
                }
                Ok(())
            }
            NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
                visit(left, depth + 1)?;
                visit(right, depth + 1)
            }
            NativeExpr::Info { base, info, .. } => {
                visit(base, depth + 1)?;
                visit(info, depth + 1)
            }
            NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => Ok(()),
        }
    }
    visit(root, 1)
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
            NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {
                // A scalar's output is the scalar type: one value per grid point and no labels.
                if !matches!(self.output, NativeType::Scalar(_)) {
                    return Err(invalid("a scalar node does not produce a scalar"));
                }
            }
            NativeExpr::VectorOf { input } => {
                input.validate()?;
                let NativeType::Scalar(input_grid) = &input.output else {
                    return Err(invalid(&format!(
                        "vector() consumes a scalar, not {}",
                        input.output.describe()
                    )));
                };
                let output = ensure_samples(&self.output, "vector()")?;
                ensure_field_count(output.fields.len(), 1, "vector()")?;
                ensure_grid(&self.output, input_grid, "vector()")?;
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
                ensure_not_interleaved(input)?;
                ensure_samples(&self.output, "an instant selection")?;
                // A selection is one source row per grid point: `select_instants` picks row
                // indices and every field is taken at them, so it carries one value per input
                // field.
                ensure_same_fields(&input.output, &self.output, "an instant selection")?;
                ensure_grid(&self.output, &op.output_grid, "an instant selection")?;
            }
            NativeExpr::Merge { input } => {
                input.validate()?;
                if !merge_shape_supported(input) {
                    return Err(invalid(
                        "a merge is only supported over input/window/map trees",
                    ));
                }
                let Some((_input_schema, input_grid)) = evaluated_vector(&input.output) else {
                    return Err(invalid("a merge consumes an evaluated vector"));
                };
                ensure_samples(&self.output, "a merge")?;
                ensure_same_fields(&input.output, &self.output, "a merge")?;
                ensure_grid(&self.output, input_grid, "a merge")?;
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
                ensure_not_interleaved(input)?;
                // What is checked here and just below is that the declared input grid equals the
                // input's grid, and that the output grid matches the declared one. That the grid
                // covers the window's read range
                // `(out.start - offset - range, out.end - offset]` is not checked here: only a
                // subquery feeds an evaluated input to a window, and deriving that coverage is the
                // compiler's, which builds the subquery's child grid to span it.
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
            NativeExpr::ParameterizedMap {
                input,
                op,
                parameters,
            } => {
                input.validate()?;
                let Some((_, input_grid)) = evaluated_vector(&input.output) else {
                    return Err(invalid(&format!(
                        "a map consumes an evaluated vector, not {}",
                        input.output.describe()
                    )));
                };
                if parameters.len() != op.arity() {
                    return Err(invalid(&format!(
                        "{op:?} reads {} parameter(s), but {} are bound",
                        op.arity(),
                        parameters.len()
                    )));
                }
                for parameter in parameters {
                    parameter.validate()?;
                    if !matches!(&parameter.output, NativeType::Scalar(grid) if grid.spec() == input_grid.spec())
                    {
                        return Err(invalid(
                            "a map parameter is not a scalar on its input's grid",
                        ));
                    }
                }
                ensure_samples(&self.output, "a map")?;
                ensure_same_fields(&input.output, &self.output, "a map")?;
                ensure_grid(&self.output, input_grid, "a map")?;
            }
            NativeExpr::Vector { input, .. } => {
                input.validate()?;
                // A vector operation keeps the input's fields and grid, so it consumes and produces
                // the same evaluated vector.
                let Some((_, input_grid)) = evaluated_vector(&input.output) else {
                    return Err(invalid(&format!(
                        "a vector operation consumes an evaluated vector, not {}",
                        input.output.describe()
                    )));
                };
                ensure_samples(&self.output, "a vector operation")?;
                ensure_same_fields(&input.output, &self.output, "a vector operation")?;
                ensure_grid(&self.output, input_grid, "a vector operation")?;
            }
            NativeExpr::Timestamp { input } => {
                input.validate()?;
                let Some((_, input_grid)) = evaluated_vector(&input.output) else {
                    return Err(invalid("timestamp consumes an evaluated vector"));
                };
                ensure_samples(&self.output, "timestamp")?;
                ensure_same_fields(&input.output, &self.output, "timestamp")?;
                ensure_grid(&self.output, input_grid, "timestamp")?;
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
                            return Err(invalid(
                                "aggregate state groups differ from the binding: state built for \
                                 one `by` or `without` cannot be merged into another",
                            ));
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
                            return Err(invalid(
                                "aggregate state groups differ from the binding: state built for \
                                 one `by` or `without` cannot be merged into another",
                            ));
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
            NativeExpr::Set { left, right, .. } => {
                left.validate()?;
                right.validate()?;
                // A set operation keeps samples of whole vectors on the grid both share: a scalar
                // has no labels to match, and PromQL refuses it on either side.
                let Some((left_schema, left_grid)) = evaluated_vector(&left.output) else {
                    return Err(invalid(&format!(
                        "a set operation consumes evaluated vectors, not {} on its left",
                        left.output.describe()
                    )));
                };
                let Some((right_schema, right_grid)) = evaluated_vector(&right.output) else {
                    return Err(invalid(&format!(
                        "a set operation consumes evaluated vectors, not {} on its right",
                        right.output.describe()
                    )));
                };
                if left_grid.spec() != right_grid.spec() {
                    return Err(invalid("a set operation aligns two different grids"));
                }
                // `or` hands the right side's samples on under the left side's fields, so the
                // kernel pairs every field by position and refuses sides of different counts.
                if left_schema.fields.len() != right_schema.fields.len() {
                    return Err(invalid(&format!(
                        "a set operation pairs fields by position, and its sides carry {} and {}",
                        left_schema.fields.len(),
                        right_schema.fields.len()
                    )));
                }
                ensure_samples(&self.output, "a set operation")?;
                ensure_same_fields(&left.output, &self.output, "a set operation")?;
                ensure_grid(&self.output, left_grid, "a set operation")?;
            }
            NativeExpr::Info { base, info, .. } => {
                base.validate()?;
                info.validate()?;
                let Some((_, base_grid)) = evaluated_vector(&base.output) else {
                    return Err(invalid(&format!(
                        "info consumes an evaluated vector, not {}",
                        base.output.describe()
                    )));
                };
                // The kernel reads the info side's values as sample times, which only an instant
                // selection that reports them produces.
                let NativeExpr::Instant { op, .. } = &info.expr else {
                    return Err(invalid(
                        "info reads its info series from an instant selection",
                    ));
                };
                if op.value != InstantValue::InfoTimestamp {
                    return Err(invalid(
                        "info reads the sample times of its info series, not their values",
                    ));
                }
                let Some((info_schema, info_grid)) = evaluated_vector(&info.output) else {
                    return Err(invalid("info's info series are not an evaluated vector"));
                };
                if info_schema.fields.len() != 1 {
                    return Err(invalid(&format!(
                        "info reads one sample time per info series, and its info series carry {} \
                         fields",
                        info_schema.fields.len()
                    )));
                }
                if base_grid.spec() != info_grid.spec() {
                    return Err(invalid("info aligns two different grids"));
                }
                ensure_samples(&self.output, "info")?;
                ensure_same_fields(&base.output, &self.output, "info")?;
                ensure_grid(&self.output, base_grid, "info")?;
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

/// Checks that `input`, when it is a [`NativeExpr::Input`], does not declare interleaved series.
///
/// A native evaluation reads one series to its last chunk before the next: a range evaluator folds
/// a series completely before it starts another, and an instant selection's consumer collects one
/// series' chunks into one buffer. Neither can be handed chunks of different series that
/// interleave, which is the one rule both consumers state. Only an input node carries an ordering,
/// so only there is there one to check.
fn ensure_not_interleaved(input: &NativeNode) -> Result<()> {
    if let NativeExpr::Input(binding) = &input.expr
        && binding.ordering == SeriesOrdering::Interleaved
    {
        return Err(invalid(
            "a native evaluation reads one series to its last chunk before the next, so \
             interleaved chunks cannot be fed to it",
        ));
    }
    Ok(())
}

fn invalid(reason: &str) -> DataFusionError {
    // A rejected tree is a compiler defect, not a query error: the compiler only builds a node
    // after deciding that this combination is supported. `Internal` keeps that meaning on the wire
    // instead of reporting a mistake in the client's statement, which `Plan` would.
    DataFusionError::Internal(reason.to_string())
}

#[cfg(test)]
mod tests;

impl PartialEq for BoundScalar {
    fn eq(&self, o: &Self) -> bool {
        self.value.to_bits() == o.value.to_bits()
    }
}
impl Eq for BoundScalar {}
impl std::hash::Hash for BoundScalar {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        self.value.to_bits().hash(h);
    }
}
impl PartialEq for AggregateKind {
    fn eq(&self, o: &Self) -> bool {
        match (self, o) {
            (Self::Quantile { quantile: a }, Self::Quantile { quantile: b }) => {
                a.to_bits() == b.to_bits()
            }
            (Self::Sum, Self::Sum)
            | (Self::Avg, Self::Avg)
            | (Self::Count, Self::Count)
            | (Self::Min, Self::Min)
            | (Self::Max, Self::Max)
            | (Self::Group, Self::Group)
            | (Self::Stddev, Self::Stddev)
            | (Self::Stdvar, Self::Stdvar) => true,
            _ => false,
        }
    }
}
impl Eq for AggregateKind {}
impl std::hash::Hash for AggregateKind {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        std::mem::discriminant(self).hash(h);
        if let Self::Quantile { quantile } = self {
            quantile.to_bits().hash(h);
        }
    }
}

impl PartialEq for BoundVectorOp {
    fn eq(&self, o: &Self) -> bool {
        match (self, o) {
            (
                Self::TopK {
                    k: a,
                    top: b,
                    group: c,
                },
                Self::TopK {
                    k: x,
                    top: y,
                    group: z,
                },
            ) => a.to_bits() == x.to_bits() && b == y && c == z,
            (Self::LimitK { k: a, group: b }, Self::LimitK { k: x, group: y }) => {
                a.to_bits() == x.to_bits() && b == y
            }
            (Self::LimitRatio { ratio: a }, Self::LimitRatio { ratio: b }) => {
                a.to_bits() == b.to_bits()
            }
            (Self::Sort(a), Self::Sort(b)) => a == b,
            (Self::Absent { labels: a }, Self::Absent { labels: b }) => a == b,
            (Self::Histogram(a), Self::Histogram(b)) => a == b,
            (
                Self::CountValues { label: a, group: b },
                Self::CountValues { label: x, group: y },
            ) => a == x && b == y,
            _ => false,
        }
    }
}
impl std::hash::Hash for BoundVectorOp {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        match self {
            Self::TopK { k, top, group } => {
                0u8.hash(h);
                k.to_bits().hash(h);
                top.hash(h);
                group.hash(h)
            }
            Self::LimitK { k, group } => {
                1u8.hash(h);
                k.to_bits().hash(h);
                group.hash(h)
            }
            Self::LimitRatio { ratio } => {
                2u8.hash(h);
                ratio.to_bits().hash(h)
            }
            Self::Sort(x) => {
                3u8.hash(h);
                x.hash(h)
            }
            Self::Absent { labels } => {
                4u8.hash(h);
                labels.hash(h)
            }
            Self::Histogram(x) => {
                5u8.hash(h);
                x.hash(h)
            }
            Self::CountValues { label, group } => {
                6u8.hash(h);
                label.hash(h);
                group.hash(h)
            }
        }
    }
}
impl Eq for BoundVectorOp {}
