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
//! What the tree cannot express yet is deliberate for this stage: [`NativeExpr::Scalar`] holds a
//! constant, so `time()` and `scalar(v)` have no node. The legality below is therefore about the
//! operations that exist, not about PromQL's whole surface.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::EvalGrid;
use datatypes::series::{SeriesLabels, SeriesOrdering};

use crate::histogram_eval::{HistogramFunction, NativeHistogramFunction};
use crate::info::InfoSelector;
use crate::map::MathOp;
use crate::map_label::LabelRewrite;
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::select::SortOrder;
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, MatchingLabels, SetOp};

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
#[derive(Clone, Debug)]
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

impl BoundInput {
    /// What the caller stated about the boundary this leaf was bound to.
    ///
    /// The facts travel beside the tree, one entry per source, and `source_index` is what names the
    /// entry: two scans are two sources with their own placement, so reading the facts by position
    /// in anything but the boundary would judge one source by another's proof. An entry the caller
    /// did not supply proves nothing ([`NativeProperties::not_known`]) rather than falling back to a
    /// neighbour's.
    fn facts_in(&self, boundary: &[NativeProperties]) -> NativeProperties {
        boundary
            .get(self.source_index)
            .copied()
            .unwrap_or_else(NativeProperties::not_known)
    }
}

/// A constant scalar bound for broadcast or a statement root.
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
    /// Anchored selection instant; samples are selected at this time for every grid point.
    pub at_ms: Option<i64>,
    /// What the selection reports for the sample it selects.
    pub value: InstantValue,
    /// The grid the selection produces values on.
    pub output_grid: Arc<EvalGrid>,
}

/// What an instant selection reports at a grid point for the sample it selected there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    pub ops: Arc<[MapOp]>,
}

/// One operation of a map, applied to each series on its own: math over its float values, a
/// function of its native histograms, or a change of its labels. Each can give two series one label
/// set, which is why all live in a map.
#[derive(Clone, Debug)]
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
#[derive(Clone, Copy, Debug, PartialEq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Grouping {
    /// The labels a group is identified by; every other label is dropped.
    By(Arc<[Arc<str>]>),
    /// The labels a group is not identified by; these are dropped, and so is `__name__`.
    Without(Arc<[Arc<str>]>),
}

/// An aggregate bound to its kind, grouping and stage.
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug, PartialEq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSetOp {
    /// `and`, `or` or `unless`.
    pub op: SetOp,
    /// The labels a sample's match group is read from on either side.
    pub labels: MatchingLabels,
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
#[derive(Clone, Debug)]
pub struct NativeNode {
    /// What this node computes, including its bound operands.
    pub expr: NativeExpr,
    /// What this node produces.
    pub output: NativeType,
}

/// Where the fragments of one canonical label set live.
///
/// A canonical label set is the label set a [`NativeExpr::Merge`] groups fragments by: `__name__`
/// plus the ordinary labels, which is exactly what [`SeriesLabels`] holds. The owner says how much
/// of one label set a single execution sees, which is what decides whether an answer computed from
/// a node's output can be the whole answer.
///
/// No node carries this fact. It follows from how the query was placed, so it is derived by
/// [`NativeNode::properties`] from the facts a caller supplies for the tree's inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CanonicalOwner {
    /// Every series of every label set the node can produce lies in this execution, so each label
    /// set it reads is read whole.
    SinglePartition,
    /// The execution holds the label sets its own canonical key names, and no other execution holds
    /// a fragment of one of them.
    ByCanonicalKey,
    /// How the output was placed against the label sets it produces is not known.
    Unknown,
}

/// Whether a node answers at most one series per canonical label set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CanonicalGrouping {
    /// One series per canonical label set, so a consumer that needs the whole series already has
    /// it.
    CanonicalComplete,
    /// Pieces of label sets, which is what one scan partition holds of a metric: a consumer of
    /// these answers a piece of the result.
    SourcePiecesOnly,
}

/// The static facts a node's output answers about canonical series.
///
/// These are what a tree rewrite needs and a runtime does not: whether the output already holds one
/// series per canonical label set ([`CanonicalGrouping`]) and how much of each label set this
/// execution holds ([`CanonicalOwner`]).
///
/// They are derived, never stored on a node: [`NativeNode::properties`] recomputes them from the
/// tree, so a rewritten tree cannot keep a fact that no longer holds. What a compiler knows about a
/// boundary it built itself travels as the [`NativeProperties`] it hands
/// [`NativeNode::properties`] for an [`NativeExpr::Input`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeProperties {
    /// How much of each canonical label set this execution holds.
    pub owner: CanonicalOwner,
    /// Whether the output holds at most one series per canonical label set.
    pub grouping: CanonicalGrouping,
}

impl NativeProperties {
    /// What an input's own fragments prove: nothing about which series share a label set.
    ///
    /// A scan hands over the pieces of a metric that one partition holds. Nothing has grouped them
    /// by label set, so the fact is not claimed; a caller that knows better says so instead.
    pub fn source_pieces() -> Self {
        Self {
            owner: CanonicalOwner::Unknown,
            grouping: CanonicalGrouping::SourcePiecesOnly,
        }
    }

    /// What a node the caller said nothing about proves: the same as [`Self::source_pieces`],
    /// because a boundary the caller left unstated proves no more than a plain scan.
    pub fn not_known() -> Self {
        Self::source_pieces()
    }

    /// What a node whose output label sets are its own still proves about *ownership*.
    ///
    /// Ownership is the first question: does this execution hold every piece of the label sets the
    /// output names? An operator whose output identity is not the input's keys cannot carry the
    /// input's answer over. A rewrite can give two series one label set - `rate(x)` and `rate(y)`
    /// coincide on `{job="z"}` where `x` and `y` did not - and an aggregate's output identity is its
    /// group key while a match's is its modifiers, so a co-location proof about the *input* keys says
    /// nothing about the output keys: two input label sets this execution holds and one it does not
    /// can fold into one group here, and that group is then not whole.
    ///
    /// `SinglePartition` survives because it is not a claim about keys at all - it holds every series
    /// there is - and `ByCanonicalKey` becomes `Unknown`, because the keys it named are not the ones
    /// the output answers under.
    ///
    /// This answers ownership only. What a node's *output* is grouped like is a separate question,
    /// answered where that node is derived: an operator that has collapsed to one series per key
    /// says so itself and is not downgraded by this.
    fn owner_of_its_own_labels(self) -> CanonicalOwner {
        match self.owner {
            CanonicalOwner::ByCanonicalKey => CanonicalOwner::Unknown,
            other => other,
        }
    }

    /// What a node whose output label sets are its own proves: neither question carries over.
    ///
    /// The ownership is [`Self::owner_of_its_own_labels`] and the grouping falls back to
    /// [`CanonicalGrouping::SourcePiecesOnly`], which is the honest answer for a node that has not
    /// established that its output is one series per key. A node that *has* established it - a
    /// finished aggregate, a match, a set operation - derives its grouping itself instead of calling
    /// this.
    fn label_sets_of_its_own(self) -> Self {
        Self {
            owner: self.owner_of_its_own_labels(),
            grouping: CanonicalGrouping::SourcePiecesOnly,
        }
    }

    /// What a merge proves.
    ///
    /// A merge groups the fragments it received by canonical label set and hands over one series per
    /// label set, so it is the node that establishes that fact for what it read: what it emits is
    /// one series per label set among the fragments that arrived. That is what a consumer folding a
    /// series needs of them.
    ///
    /// What it read says nothing about what it emits past that, and it cannot collect a fragment that
    /// never arrived: the placement of what it read is still the placement of what it wrote, so the
    /// ownership is kept unchanged and never upgraded. A merge running inside a partition that holds
    /// pieces of several label sets therefore still leaves the ownership `Unknown`, even though it
    /// grouped every fragment it was given.
    fn merged(self) -> Self {
        Self {
            grouping: CanonicalGrouping::CanonicalComplete,
            ..self
        }
    }

    /// What two inputs' facts prove together.
    ///
    /// A binary operation over two vectors, and a set operation, answers series of both of its sides,
    /// so its output holds every label set either side produced: the ownership is only what both
    /// sides agree on. A side that is a constant is never united in, because it holds no label set:
    /// the vector side's facts are the broadcast's alone.
    fn united(self, other: Self) -> Self {
        Self {
            owner: if self.owner == other.owner {
                self.owner
            } else {
                CanonicalOwner::Unknown
            },
            ..self
        }
    }
}

impl NativeNode {
    /// The static properties of this node, given what the caller knows about what is below it.
    ///
    /// `boundary` is the facts of the fused inputs this tree was compiled over, indexed by the
    /// [`BoundInput::source_index`] each leaf was bound to: it is the *only* thing this derivation
    /// is handed. A leaf answers from its own entry and from nothing else, and every other node's
    /// facts are its children's, in child order, which the recursion below asks for one child at a
    /// time. There is hence no place a node could read one child's facts for another's — the mistake
    /// that made a tree with two sources judge every source by the first one's proof, and a scan's
    /// pieces canonical by a neighbour's.
    ///
    /// Nothing is stored: the facts are derived from the tree and the boundary every time, so a
    /// rewritten tree cannot keep a fact the rewrite invalidated. Asking for a node's facts walks the
    /// subtree below it, so facts for every node of a tree cost one walk per consumer; a tree is a
    /// few dozen nodes and this is compile-time work, which is worth a derivation with no second
    /// representation to keep in step.
    pub fn properties(&self, boundary: &[NativeProperties]) -> NativeProperties {
        match &self.expr {
            // A leaf *is* the boundary it was bound to, and the facts are its own entry's.
            NativeExpr::Input(binding) => binding.facts_in(boundary),
            // A constant is one value everywhere and holds no label set at all, so it is whole as it
            // stands: a merge has nothing to canonicalize.
            NativeExpr::Scalar(_) => no_label_sets(),
            // A selection answers one series from one series' rows and keeps the label set it read,
            // so the whole series stays one stream under that label set.
            //
            // The selection that reports the sample's *time* keeps the label set it read only when
            // the time it reports is the info series' own (`InfoTimestamp`, which `info` reads): the
            // runtime files that under every label of the series. `Timestamp` (`timestamp(x)`)
            // drops the metric name before it builds the series, so `a{job="x"}` and `b{job="x"}`
            // both arrive as `{job="x"}` and nothing the input proved about which series share a
            // label set survives that.
            NativeExpr::Instant { input, op } => {
                let input = input.properties(boundary);
                // `Sample` answers the label set the selection read, and so does `InfoTimestamp`.
                // `Timestamp` reports a time instead of the value and rewrites the labels on the
                // way, so its facts are a rewrite's.
                match op.value {
                    InstantValue::Sample | InstantValue::InfoTimestamp => input,
                    InstantValue::Timestamp => input.label_sets_of_its_own(),
                }
            }
            // A window folds a range of one series' samples and hands the answer over as that
            // series, so the input's grouping is the output's — unless the function drops the metric
            // name, which `RangeFunction::drops_metric_name` reports for every function but
            // `last_over_time` and `first_over_time`. A dropping function rewrites the labels in
            // `RangeEvaluator::emit`/`empty_chunk`, so `rate(a)` and `rate(b)` can coincide on
            // `{job="x"}` where `a` and `b` did not: a proof about the input's label sets says
            // nothing about the output's. The merge the window runtime can run itself
            // (`RuntimeOp::Window`'s `merge`) joins the chunks it *reads*, so it cannot collect the
            // aliases its own rewrite creates afterwards: those are a consumer's to merge, which is
            // a merge above the window and not one below it.
            NativeExpr::Window { input, op } => {
                let input = input.properties(boundary);
                if op.function.drops_metric_name() {
                    input.label_sets_of_its_own()
                } else {
                    input
                }
            }
            // A timestamp replaces each value with its sample's time, and the labels with
            // `drop_metric_name`'s (`NativeNode::timestamp` in
            // [`execute`](crate::native::execute)), so the identity it answers under is not the one
            // it read.
            NativeExpr::Timestamp { input } => input.properties(boundary).label_sets_of_its_own(),
            // A map is point-wise over one series at a time, so it reads its input as it is: what it
            // does to the label set is a fact about its output.
            NativeExpr::Map { input, op } => {
                let input = input.properties(boundary);
                if op.ops.iter().all(preserves_canonical_identity) {
                    input
                } else {
                    input.label_sets_of_its_own()
                }
            }
            // A merge is the canonicalizer: it answers one series per label set whatever pieces it
            // read.
            // `info` enriches each base series with the info series that share its identifying
            // labels, which is a read across both sides' whole vectors: what it answers under is
            // decided per series against the other side, so neither side's grouping is the output's.
            NativeExpr::Info { base, info, .. } => base
                .properties(boundary)
                .united(info.properties(boundary))
                .label_sets_of_its_own(),
            NativeExpr::Merge { input } => input.properties(boundary).merged(),
            // An aggregate answers two questions with two answers, and they are not the same one.
            //
            // *Grouping*: a finished fold is one series per group (`FloatAggregator::finish` builds
            // one `Series` per group, and `AggregateRuntime::compute` hands over exactly that), and
            // two groups are two label sets, so the output already holds at most one series per
            // canonical label set. Nothing above it has a series to collect, whatever the labels it
            // ended up with. `Partial` answers aggregate state instead, which is not an evaluated
            // series at all: it carries no series per label set, so it makes no completeness claim.
            //
            // *Ownership*: the output's identity is the group key, so a co-location proof about the
            // input's canonical keys does not name the groups this node invents - two input label
            // sets this execution holds and one it does not can fold into one group here - and the
            // owner is re-derived rather than carried over.
            NativeExpr::Aggregate { input, .. } => {
                let input = input.properties(boundary);
                NativeProperties {
                    owner: input.owner_of_its_own_labels(),
                    grouping: match &self.output {
                        NativeType::Samples(_) => CanonicalGrouping::CanonicalComplete,
                        NativeType::Scalar(_) | NativeType::AggregateState(_) => {
                            CanonicalGrouping::SourcePiecesOnly
                        }
                    },
                }
            }
            // A vector operation is decided one operator at a time, because the seven do not answer
            // the same shape.
            //
            // *Grouping*: `topk`, `limitk`, `limit_ratio` and `sort` hand back series of their input
            // with their labels unchanged - `topk` rebuilds the selected points of the series it
            // picked, `limitk` the same over its merged input, `limit_ratio` keeps whole series and
            // `sort` reorders them - so what the input held about label sets is what the output
            // holds. `limitk` reads one series per label set only because the insertion pass (or the
            // runtime, when the child shares runs) merges its input, which is why its grouping is
            // the input's and cannot be claimed on its own. `absent` answers one series of its own,
            // `histogram_quantile`/`histogram_fraction` merge label-set pieces before the kernel
            // (`vector_series` calls `merge_same_labelset`) and answer one series per set, and
            // `count_values` files one series per group and value: all three are complete.
            //
            // *Ownership*: the same four that keep their labels keep the ownership too, while the
            // three that invent a label set - `absent`'s matcher labels, a histogram's shared bucket
            // labels, `count_values`' value label - re-derive it from a proof about keys they no
            // longer name.
            NativeExpr::Vector { input, op } => {
                let input = input.properties(boundary);
                match op {
                    BoundVectorOp::TopK { .. }
                    | BoundVectorOp::LimitK { .. }
                    | BoundVectorOp::LimitRatio { .. }
                    | BoundVectorOp::Sort(_) => input,
                    BoundVectorOp::Absent { .. }
                    | BoundVectorOp::Histogram(_)
                    | BoundVectorOp::CountValues { .. } => NativeProperties {
                        owner: input.owner_of_its_own_labels(),
                        grouping: CanonicalGrouping::CanonicalComplete,
                    },
                }
            }
            NativeExpr::Binary {
                left,
                right,
                matching,
            } => match (&left.output, &right.output) {
                // Two constants make a constant: there is no label set to canonicalize.
                (NativeType::Scalar(_), NativeType::Scalar(_)) => no_label_sets(),
                // A scalar broadcast reaches each series of its vector side as it is, unless the
                // operation drops the metric name on the way: arithmetic and a `bool` comparison do,
                // a filtering comparison keeps the label set it read.
                (NativeType::Samples(_), NativeType::Scalar(_))
                | (NativeType::Scalar(_), NativeType::Samples(_)) => {
                    // Only the vector side's facts are the output's, because a constant contributes
                    // no label set at all: the broadcast answers the vector side's series under the
                    // label set it read, or under that label set rewritten.
                    //
                    // A `united` of the two sides cannot express that, and answers wrong in both
                    // directions. It keeps the left side's grouping, so with the constant on the
                    // left the output would claim the constant's `CanonicalComplete` for a vector
                    // side that is still only source pieces - the alias a merge above has to
                    // collect would then never be collected. And it keeps an ownership only the two
                    // sides agree on, so with the constant on the right a vector side placed by
                    // canonical key would be downgraded to `Unknown`, which collapses the owner
                    // partitions a merge above it could have stayed within.
                    //
                    // The side that holds the samples is therefore the base either way, so `x > 0`
                    // and `0 < x` derive the same facts.
                    let vector = if left.output.samples().is_some() {
                        left
                    } else {
                        right
                    };
                    let vector = vector.properties(boundary);
                    if broadcast_drops_labels(matching) {
                        vector.label_sets_of_its_own()
                    } else {
                        vector
                    }
                }
                // Two vectors are matched by label. The two questions are answered apart.
                //
                // *Grouping*: the kernel files its rows by *result label set* (`Results` is keyed by
                // the label set a pairing's sample ends under) and hands over one series per set,
                // refusing two values of a field at one point (`Results::sort_rows` -> "vector
                // cannot contain metrics with the same labelset"). What it answers is therefore one
                // series per canonical label set however the operands arrived.
                //
                // *Ownership*: the modifiers decide what label set a sample ends under, so the
                // match's keys are not the keys a co-location proof about either side names; a match
                // answers series of both sides, so its ownership is only what both agree on and
                // `label_sets_of_its_own` re-derives it for the output's keys.
                (_, _) => NativeProperties {
                    owner: left
                        .properties(boundary)
                        .united(right.properties(boundary))
                        .label_sets_of_its_own()
                        .owner,
                    grouping: CanonicalGrouping::CanonicalComplete,
                },
            },
            // A set operation keeps each side's samples as they are, and its kernel files them by
            // label set as a match does, refusing two values of a field at one point
            // (`set_vectors` -> `Results::sort_rows`). *Grouping*: one series per label set, so
            // nothing above it has a series to collect. *Ownership*: the samples keep the labels
            // they arrived with, so the label sets the output names are the ones the sides' keys
            // named and the two proofs unite without being re-derived.
            NativeExpr::Set { left, right, .. } => NativeProperties {
                owner: left
                    .properties(boundary)
                    .united(right.properties(boundary))
                    .owner,
                grouping: CanonicalGrouping::CanonicalComplete,
            },
        }
    }
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
/// input, a scalar, a selection, a window, a map, and a binary operation with a constant on one
/// side, whose broadcast reaches each series the other side answers - answers one series from one
/// series' rows.
///
/// Both a region-side placement and the native route's admission ask this, and they must agree: a
/// tree admitted here but refused there becomes a node the physical layer cannot build, and one
/// refused here but admitted there would be pushed to a region that holds part of the metric.
pub fn reads_across_series(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) => false,
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input } => reads_across_series(input),
        NativeExpr::Binary { left, right, .. } => match (&left.expr, &right.expr) {
            // Two constants answer the same value for every series there is, so the operation reads
            // none of them; one constant is broadcast over the other side's series unchanged, so what
            // the operation reads is what that side reads.
            (NativeExpr::Scalar(_), NativeExpr::Scalar(_)) => false,
            (NativeExpr::Scalar(_), _) => reads_across_series(right),
            (_, NativeExpr::Scalar(_)) => reads_across_series(left),
            // Two vectors are matched by label, which pairs series of the whole vector.
            (_, _) => true,
        },
        NativeExpr::Aggregate { .. }
        | NativeExpr::Vector { .. }
        | NativeExpr::Set { .. }
        | NativeExpr::Info { .. }
        | NativeExpr::Merge { .. } => true,
    }
}

/// Whether `node` answers a value for the whole vector, so a tree holding it has to see every series
/// in one execution rather than one partition's worth at a time.
///
/// A `Vector` operation (`topk`, `bottomk`, `limitk`, `limit_ratio`, `sort`, `absent`,
/// `histogram_quantile`, `histogram_fraction`, `count_values`) picks, orders or reads its input's
/// series against each other, and a `Complete` aggregate folds every contribution to a group: given
/// part of the metric, either one answers part of the result instead of the result. A `Partial` or
/// `Final` aggregate's own stage is not: its contract is exactly the part it was handed, so only what
/// sits below it is followed.
///
/// This form passes an empty boundary, so what it asks of a merge is what
/// [`needs_single_execution_in`] answers for one: the merge's own need, which is its input's. Whether
/// a merge can be placed at all is [`merges_are_placeable_in`]'s question, and it is not this one -
/// it is asked by a caller that knows which execution it is placing the tree in. The native route's
/// placement (`NativePlacement::of` in `promql::planner`) and the region-side admission ask this and
/// both have no boundary facts at hand; a caller that has them, as the canonical-merge rewrite does,
/// asks [`needs_single_execution_in`].
///
/// Everything else recurses into its children. A window (`rate`, `sum_over_time`, ...), a map, an
/// instant selection and a timestamp compute one series from one series' rows, and a binary
/// operation that broadcasts a constant is decided by its vector side alone, so a tree built from
/// those still answers each of its series on its own: splitting it across scan partitions splits the
/// answer rather than an operator's view.
///
/// A binary or set operation that *matches* two vectors is not decided by its sides alone, though.
/// It pairs the series of one side with the series of the other that shares the match's label set,
/// which is a read across the whole vector on both sides, and with no boundary facts at hand a match
/// has no proof that one execution holds both. So it needs one execution.
///
/// This is about placement, not admission: it says how many executions the tree needs, not whether
/// it may run over part of the metric ([`reads_across_series`] answers that).
pub fn needs_single_execution(node: &NativeNode) -> bool {
    needs_single_execution_in(node, &[])
}

/// Two fused sources whose producers this execution reads in step: the routing placed them
/// identically, and their outputs name no series.
///
/// This is a *joint* fact about a pair and cannot be derived inside the tree that consumes them.
/// An [`NativeExpr::Input`] over evaluated samples answers `None` to [`output_has_metric_name`],
/// because whether its label sets still carry a metric name was decided by the node that built
/// them, which is not in this tree. So the layer that builds those producers asks each producer's
/// own root and states the answer here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoPartitionedPair {
    /// The left fused source index.
    pub left: usize,
    /// The right fused source index.
    pub right: usize,
    /// Both producers' outputs carry no metric name, proved where those producers were built.
    pub name_free: bool,
}

impl CoPartitionedPair {
    /// Whether this pair names the two sources `left` and `right`, in either orientation.
    ///
    /// The two orientations are one pair: a routing that sends source `i`'s partners to source `j`'s
    /// execution sends `j`'s to `i`'s too, and which side of the match a source sits on is the
    /// query's choice rather than the routing's.
    fn joins(&self, left: usize, right: usize) -> bool {
        (self.left == left && self.right == right) || (self.left == right && self.right == left)
    }
}

/// Whether `node` answers a value for the whole vector, given what the caller knows about where the
/// sources below it sit and which of them route identically.
///
/// `boundary` is the same slice [`NativeNode::properties`] takes: the facts of the fused inputs the
/// tree was compiled over, indexed by `source_index`. `co_partitioned` is what the caller proved
/// about pairs of those sources ([`CoPartitionedPair`]): that the routing placed the two identically,
/// and that both producers' outputs name no series. It is the fact a match needs and a merge does not
/// ([`match_is_co_partitioned`]), and it is read by no other node.
///
/// A `Merge` needs every piece of ONE canonical label set, and one execution is one way of having
/// them - but which way applies is not this predicate's question. A merge's own need is therefore
/// its input's ([`NativeExpr::Merge`] below), and whether the execution this tree is placed in can
/// answer a merge at all is [`merges_are_placeable_in`], asked by the caller that knows the
/// placement: a canonical merge is right in a single execution, and in a per-partition one only
/// where the boundary proves the pieces already arrive together ([`pieces_co_located`]).
///
/// A *match* needs something else. A `Binary` over two vectors and a `Set` pair each series of one
/// side with the series of the other that shares the label set the modifiers name, and a whole match
/// owes an error when one side holds two series of that label set. Canonical-key co-location does
/// not prove that: the keys it proves whole are the canonical label sets, and the match's label set
/// is the caller's. Two series that agree on `on(host)` can differ in every other label and sit
/// under two canonical keys, so two owner partitions can each hold one series of that match group,
/// pair them locally, and never run the duplicate-series check over the real group: the error a
/// whole match reports is silently not reported. A match therefore needs one execution unless one
/// of two proofs says otherwise. A broadcast is not a match, so a constant's side is not asked for
/// either.
///
/// The first proof is the per-side one ([`matching_co_located`]): each side's series are all in this
/// execution, so every pair of them is here too. The second is two-sided
/// ([`match_is_co_partitioned`]): each side's series are *not* all here, but the sources the two
/// sides read route identically and the match pairs by exactly the key that routing used, so a pair
/// built from this partition is the pair the whole match would have built. The two answer different
/// questions - "is everything here?" and "is the partner of what is here also here?" - and neither
/// implies the other: a per-key placement proves each canonical label set whole without proving a
/// match group whole, and a co-partitioned pair proves a match group whole without proving either
/// side whole.
pub fn needs_single_execution_in_co(
    node: &NativeNode,
    boundary: &[NativeProperties],
    co_partitioned: &[CoPartitionedPair],
) -> bool {
    match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) => false,
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input } => {
            needs_single_execution_in_co(input, boundary, co_partitioned)
        }
        // `info` pairs each base series with the info series that share its identifying labels,
        // which is a match by a label set of the caller's choosing rather than by a canonical key -
        // the same reason a two-vector `Binary` needs one execution unless both sides are proven to
        // sit in it.
        NativeExpr::Info { base, info, .. } => {
            needs_single_execution_in_co(base, boundary, co_partitioned)
                || needs_single_execution_in_co(info, boundary, co_partitioned)
                || !(matching_co_located(base, boundary) && matching_co_located(info, boundary))
        }
        NativeExpr::Aggregate { input, op } => {
            op.stage == AggregateStage::Complete
                || needs_single_execution_in_co(input, boundary, co_partitioned)
        }
        // A match pairs the series of both sides by the label set its modifiers name, which is not
        // the canonical key a placement proves whole ([`matching_co_located`]), so it needs one
        // execution unless both sides are already in it - or unless the two-sided proof says the
        // match pairs by the key the sides' sources were routed by ([`match_is_co_partitioned`]).
        // A broadcast is not a match: it pairs a vector with one value, so the constant's side holds
        // no series to pair and proves nothing it was asked for - `x > 0` and `x * 2` stay as
        // parallel as the vector side.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            let both_vectors = matches!(
                (&left.output, &right.output),
                (NativeType::Samples(_), NativeType::Samples(_))
            );
            needs_single_execution_in_co(left, boundary, co_partitioned)
                || needs_single_execution_in_co(right, boundary, co_partitioned)
                || (both_vectors
                    && !(matching_co_located(left, boundary)
                        && matching_co_located(right, boundary))
                    && !match_is_co_partitioned(left, right, matching, co_partitioned))
        }
        // A set operation keeps samples of two evaluated vectors, so both sides are always matched:
        // `validate` refuses a scalar on either side and there is no broadcast form of it.
        //
        // It gains no two-sided proof this round. A set operation keeps each side's samples under the
        // labels they arrived with, so what it compares is the two sides' own identities, and a
        // per-partition answer would have to show that those identities - not the match's modifiers,
        // which a set operation does not have - are the ones the routing used.
        NativeExpr::Set { left, right, .. } => {
            needs_single_execution_in_co(left, boundary, co_partitioned)
                || needs_single_execution_in_co(right, boundary, co_partitioned)
                || !(matching_co_located(left, boundary) && matching_co_located(right, boundary))
        }
        NativeExpr::Vector { .. } => true,
        // A merge needs every piece of one canonical label set, and not every label set in one
        // execution: given the pieces it answers one series per label set. One execution provides
        // that for any input; a per-partition execution provides it only when the pieces are
        // co-located. Which of the two applies is not this predicate's question: it answers what the
        // tree needs from an execution, and a merge's own need is its input's. Whether the merge can
        // be *placed* here is `merges_are_placeable_in`, asked by the caller that knows the
        // placement.
        NativeExpr::Merge { input } => {
            needs_single_execution_in_co(input, boundary, co_partitioned)
        }
    }
}

/// Whether `node` answers a value for the whole vector, given what the caller knows about where the
/// sources below it sit but nothing about how they route.
///
/// This is [`needs_single_execution_in_co`] with no co-partitioned pair: a caller that has not proved
/// two sources route identically has not proved a match between them can run per partition, so the
/// two-sided proof is never given a pair to fire on and the answer is the per-side one it always was.
pub fn needs_single_execution_in(node: &NativeNode, boundary: &[NativeProperties]) -> bool {
    needs_single_execution_in_co(node, boundary, &[])
}

/// Whether a match between two routed inputs pairs its series by the key their sources were routed
/// by, so one execution per partition answers it.
///
/// This is a *two-sided* proof, and the question it asks is neither the per-side one
/// ([`matching_co_located`]: are both sides' series all in this execution?) nor a merge's
/// ([`pieces_co_located`]: are the pieces of every canonical label set here?). It asks: does the key
/// this match pairs by name the same groups the placement grouped the two sources by?
///
/// That question has two halves, and they are answered by two different parties.
///
/// The *tree* half is answered here. A placement by canonical key partitions by the canonical label
/// set, which is `__name__` plus the ordinary labels - the key a [`NativeExpr::Merge`] groups by -
/// while a match pairs by the label set its modifiers name: the default
/// [`MatchingLabels::Ignoring`] with no label is every label *except* `__name__`. So
/// `m1{ns="a"} / m2{ns="a"}` is two canonical label sets and one match group: routing by canonical
/// key sends the two partners to different partitions, each partition finds no partner, and the
/// match answers nothing where the whole match would answer a value. The two keys coincide exactly
/// when neither side's series carries a metric name, because then the canonical key *is* the set of
/// ordinary labels. Which operands and which modifiers make the keys coincide is the tree's question,
/// and it is the one this function settles.
///
/// The *producer* half is [`CoPartitionedPair::name_free`], and it is supplied by whoever could
/// observe it. It cannot be derived here: both operands of the shape this proof exists to serve are
/// an [`NativeExpr::Input`] over evaluated samples, whose label sets another execution built, so
/// [`output_has_metric_name`] answers `None` for them - it has no node to read the name off. The layer
/// that builds those producers asks each producer's own root, where the answer is a function of the
/// tree in front of it, and states it on the pair. Reading `None` as "no name" instead would be the
/// guess that makes a named match look name-free, which is exactly the silent-empty-answer bug: the
/// match would be run per partition, find no partner, and answer nothing.
///
/// `co_partitioned` is therefore what the caller proved about the pair, and both halves are required.
/// Co-partitioned sources whose series carry a name still pair by a key no partition holds whole, and
/// name-free sides whose sources are not co-partitioned can still have their partners in two
/// executions.
///
/// The operands are read strictly: this round proves nothing about a node between the routed input
/// and the match. A per-series node above the input hands the label set it read on unchanged, so the
/// identity the router used would still be the identity the match sees through it, but a fold, a
/// name-dropping window or a label rewrite does not: it invents an identity the routing never saw, and
/// a proof about the routed key would then be about a key the match no longer reads. Looking through
/// those nodes - only the ones that provably keep the identity the router used - is a later round's
/// work, and answering it by assuming they all keep it is the silent-empty-answer bug this proof
/// exists to prevent.
///
/// Every conjunct below is required:
///
/// 1. each operand is the routed input itself ([`NativeExpr::Input`]);
/// 2. the caller proved those two sources co-partitioned, either way round;
/// 3. the match is the default one - [`MatchingLabels::Ignoring`] with no label, whose key is every
///    label but `__name__` - so its key is the canonical key of a name-free series;
/// 4. it is one-to-one: `group_left`/`group_right` copy the one side's labels into each result, so a
///    result's identity is not the many side's and the routed key does not name it;
/// 5. that pair says both producers are name-free, which is what makes the canonical key and the
///    match key one key. It is per pair, and one named side is enough to break it - which is why the
///    pair states the joint fact rather than each side stating its own.
pub fn match_is_co_partitioned(
    left: &NativeNode,
    right: &NativeNode,
    matching: &BinaryMatching,
    co_partitioned: &[CoPartitionedPair],
) -> bool {
    let (NativeExpr::Input(left_input), NativeExpr::Input(right_input)) = (&left.expr, &right.expr)
    else {
        return false;
    };
    let (left_source, right_source) = (left_input.source_index, right_input.source_index);
    let Some(pair) = co_partitioned
        .iter()
        .find(|pair| pair.joins(left_source, right_source))
    else {
        return false;
    };
    if !matches!(&matching.labels, MatchingLabels::Ignoring(names) if names.is_empty()) {
        return false;
    }
    if matching.cardinality != Cardinality::OneToOne {
        return false;
    }
    pair.name_free
}

/// Whether every merge in `node` can be answered by an execution placed as `co_located` says.
///
/// `co_located` is what the caller already decided about the execution this tree will run in:
/// `true` when the execution reads every input partition, so every piece of every label set is
/// present and a merge is always placeable; `false` when it reads one partition, so a merge is
/// placeable only where [`pieces_co_located`] proves the pieces arrive together.
///
/// This is deliberately not part of [`needs_single_execution_in`]: a merge that cannot be placed
/// is a tree this route does not support here, not a tree that needs one execution. Collapsing the
/// partitions to make it work would take the caller's parallelism away to serve a shape nobody
/// proved was placeable.
pub fn merges_are_placeable_in(
    node: &NativeNode,
    boundary: &[NativeProperties],
    co_located: bool,
) -> bool {
    match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) => true,
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input } => merges_are_placeable_in(input, boundary, co_located),
        // A merge reads the pieces of one canonical label set and answers one series of it, so it is
        // placeable where the execution holds every piece. The recursion does not stop here: a merge
        // of a merge has to be placeable at every level, and a label-rewriting node between two of
        // them rewrites the identity the inner proof was about, so the inner merge can be the one
        // that has no proof left.
        NativeExpr::Merge { input } => {
            (co_located || pieces_co_located(input, boundary))
                && merges_are_placeable_in(input, boundary, co_located)
        }
        NativeExpr::Info { base, info, .. } => {
            merges_are_placeable_in(base, boundary, co_located)
                && merges_are_placeable_in(info, boundary, co_located)
        }
        NativeExpr::Aggregate { input, .. } | NativeExpr::Vector { input, .. } => {
            merges_are_placeable_in(input, boundary, co_located)
        }
        NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
            merges_are_placeable_in(left, boundary, co_located)
                && merges_are_placeable_in(right, boundary, co_located)
        }
    }
}

/// Whether the boundary proves that the pieces of every canonical label set below `node` arrive in
/// one execution, so a merge over them reads each label set whole.
///
/// The proof is ownership, and it is the ownership that survives to `node` that counts: an operator
/// that rewrites the identity - a name-dropping window, an aggregate, a map - downgrades a per-key
/// ownership to `Unknown`, because the label sets it invents are not the ones the boundary's
/// canonical keys name. So `SinglePartition` ("this execution holds every series there is") or
/// `ByCanonicalKey` ("it holds the label sets its own key names and no other execution holds a
/// fragment of one of them") is what a merge needs, and anything else - `Unknown` in particular,
/// which is all a raw scan proves, since it partitions by storage `read_key` and not by canonical
/// key - leaves the pieces spread over partitions.
///
/// This is a merge's proof and only a merge's, because a merge groups by exactly the key it names: a
/// canonical label set. It proves nothing about the label set a *match* pairs by
/// ([`matching_co_located`]), which is the modifiers' and not the canonical key's, so a match that
/// read this answer would take a per-key placement for a whole one.
fn pieces_co_located(node: &NativeNode, boundary: &[NativeProperties]) -> bool {
    matches!(
        node.properties(boundary).owner,
        CanonicalOwner::SinglePartition | CanonicalOwner::ByCanonicalKey
    )
}

/// Whether `node`'s series all sit in this one execution, so a label match over them pairs every
/// series it should.
///
/// A match pairs the series of two sides by the label set its own modifiers name (`on(host)`,
/// `ignoring(zone)`, ...), and a whole match owes an error when one side holds two series of that
/// label set. Only an execution holding every series it could pair can answer that, which is what
/// `SinglePartition` states and what nothing weaker states.
///
/// `ByCanonicalKey` in particular does not, and that is the whole point of this predicate being
/// separate from [`pieces_co_located`]: it proves the canonical label sets its keys name are whole,
/// and a match does not read those keys. `{host="h", zone="a"}` and `{host="h", zone="b"}` are two
/// canonical label sets owned by two partitions, they are one `on(host)` match group, and each owner
/// would pair its own series against its own and report no duplicate - the answer a whole match
/// refuses.
///
/// The facts are read off `node` rather than off `node`'s children because what has to be whole is
/// what the match pairs, which is what `node` answers: the same derivation every merge proof uses.
fn matching_co_located(node: &NativeNode, boundary: &[NativeProperties]) -> bool {
    node.properties(boundary).owner == CanonicalOwner::SinglePartition
}

/// Whether `node` is a supported child of a [`NativeExpr::Merge`], and the evaluated vector it hands
/// over.
///
/// A merge receives fragments and groups them by canonical label set, combining the ones a label set
/// shares by grid position; it names neither the tree nor the operator that produced a fragment. What
/// it reads is therefore the whole of its requirement: samples evaluated on one grid in one field
/// layout, which is an evaluated vector and nothing else. This is that type check and no more, so no
/// node kind is refused for what it computes: a scan's samples, a window's rate, an aggregate's fold
/// and a binary operation's match all hand over the same thing, and which one of them did says
/// nothing about whether the fragments merge.
///
/// The collection does rely on the shape of the stream - a series arrives whole, its chunks
/// contiguous and its last one marked as closing it - and every node that answers an evaluated vector
/// keeps it. A window, a map, an instant selection, a timestamp and a scalar broadcast hand their
/// input's closing on, and an aggregate, a vector operation, a binary or set operation and a merge
/// itself close each series they answer, because each decides a whole series at once. There is hence
/// no operator above a merge that could break the collection, and none has to be listed here; the
/// collector checks the shape chunk by chunk before it keeps a fragment, which is where a stream that
/// broke it is refused.
fn merge_shape_supported(node: &NativeNode) -> Option<(&Arc<SeriesSchema>, &Arc<EvalGrid>)> {
    evaluated_vector(&node.output)
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
                // A merge canonicalizes evaluated fragments, so an evaluated vector is the whole of
                // what it needs of its input: [`merge_shape_supported`] is that check, and its
                // documentation says why no shape of tree above those fragments matters.
                let Some((_input_schema, input_grid)) = merge_shape_supported(input) else {
                    return Err(invalid(&format!(
                        "a merge consumes an evaluated vector, not {}",
                        input.output.describe()
                    )));
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
                // selection that reports them produces. A canonical merge above that selection only
                // collects the pieces of one label set into one series, so it hands the times on
                // unchanged and the check looks through it. Nothing else is looked through: an
                // evaluated value vector is not a set of sample times, and reading one as times
                // would compare a sample's value against another's.
                let mut source = info.as_ref();
                while let NativeExpr::Merge { input } = &source.expr {
                    source = input;
                }
                let NativeExpr::Instant { op, .. } = &source.expr else {
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

/// The properties of a value that holds no label set: a constant, or a binary operation of two of
/// them.
///
/// There is nothing to canonicalize, and a constant is the same value in every execution.
fn no_label_sets() -> NativeProperties {
    NativeProperties {
        owner: CanonicalOwner::SinglePartition,
        grouping: CanonicalGrouping::CanonicalComplete,
    }
}

/// Whether a map operation leaves the label set it reads as it is.
///
/// This is the rule the RFC states as "a pure numeric map inherits grouping", answered by the
/// kernels instead: none of the three operations leaves a label set alone, so no map preserves
/// grouping.
///
/// The per-variant answer is the decision point: a new [`MapOp`] has to say here what it writes.
fn preserves_canonical_identity(op: &MapOp) -> bool {
    match op {
        // `map_math`, `map_negate` and `map_arithmetic` all end with `drop_metric_name`, which
        // removes `__name__` together with the type and unit labels: `abs(x)` and `x` no longer hold
        // one label set.
        MapOp::Math(_) => false,
        // `map_native_histogram` follows Prometheus' `simpleHistogramFunc`: it drops `__name__` too
        // (keeping the type and unit), so a histogram function's output is not its input's label
        // set.
        MapOp::Histogram(_) => false,
        // A rewrite writes a caller-chosen destination label, `__name__` included, and can empty it,
        // so a series ends under a label set the rewrite built.
        MapOp::Label(_) => false,
    }
}

/// Whether a binary operation drops the metric name when one side is a scalar broadcast.
///
/// `BroadcastRuntime::drops_labels` in [`execute`](crate::native::execute) is the rule the runtime
/// applies: arithmetic and a `bool` comparison answer a series under the metric name dropped, and a
/// filtering comparison keeps the label set it read.
fn broadcast_drops_labels(matching: &BinaryMatching) -> bool {
    !matches!(
        matching.op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Ge | BinaryOp::Le
    ) || matching.return_bool
}

/// Whether every series `node` hands over carries a metric name (`__name__`).
///
/// This is the fact the two-sided match proof asks of each side ([`match_is_co_partitioned`]): a
/// placement routes by the canonical label set, which holds `__name__` when a series has one, while a
/// match pairs by the label set its modifiers name. The two keys are the same key only on series
/// without a name, so one named side is already enough to make them differ.
///
/// - `Some(true)`: every series the output holds carries a name.
/// - `Some(false)`: no series the output holds carries a name.
/// - `None`: this walk cannot decide, which is not a claim in either direction: a consumer that
///   needs the fact must read it as "not proved here" rather than as an answer.
///
/// The answer is derived from the node and the kernels it is bound to, never from a boundary, so a
/// shape whose output labels are decided outside the tree answers `None`. The shapes decided in every
/// case are the ones whose kernel writes the name away for whatever it read: `timestamp`, a window
/// running any function but `last_over_time`/`first_over_time`, a map of the two dropping operations,
/// an aggregate or `count_values` whose grouping does not list `__name__`, `absent`, the histogram
/// functions, and a two-vector arithmetic or `bool` comparison. The shapes that follow their input
/// are the ones that only carry labels through. Four answers are deliberately not given: a
/// `label_replace`/`label_join` may write `__name__` or clear it, an input of evaluated samples hands
/// on a value another execution built, a one-side-name-free `or`/`and`/`unless` keeps both sides'
/// samples, and a `group_left`/`group_right` may copy a name from its one side. Each of those is a
/// later fact's question, not this walk's.
pub fn output_has_metric_name(node: &NativeNode) -> Option<bool> {
    match &node.expr {
        // A scan of raw samples reads one metric: the selector it was bound from names the metric, so
        // every series it hands over carries that name, and raw samples are the only thing a scan
        // hands over. The name itself is not in the tree - `BoundInput` holds the source index, the
        // output type and the ordering, and the compiler keeps the name beside them - so this is the
        // one input shape the tree can answer for: which type an input is decides whether anything
        // above it can name a series at all.
        //
        // Evaluated samples at an input are another execution's answer: whether its label sets still
        // carry a name was decided by the node that built them, which is not in this tree. Aggregate
        // state is not a series at all, and no consumer of this fact has an operand of that type.
        NativeExpr::Input(binding) => match binding.output.samples() {
            Some(schema) if schema.eval_grid.is_none() => Some(true),
            _ => None,
        },
        // A constant is one value per grid point and holds no label set, so it carries no name.
        NativeExpr::Scalar(_) => Some(false),
        // A selection answers the label set it read, except `timestamp(x)`: `output_metric` in
        // [`execute`](crate::native::execute) rewrites the labels through `map::drop_metric_name` for
        // `InstantValue::Timestamp` and clones them for `Sample` and `InfoTimestamp`, which is the
        // selection `info` reads.
        NativeExpr::Instant { input, op } => match op.value {
            InstantValue::Sample | InstantValue::InfoTimestamp => output_has_metric_name(input),
            InstantValue::Timestamp => Some(false),
        },
        // A merge groups the fragments it read by canonical label set and hands each set on as it
        // arrived, so it writes no label of its own and what it emits carries the name the fragments
        // carried.
        NativeExpr::Merge { input } => output_has_metric_name(input),
        // A window folds values and keeps the label set of the series it read, unless the function
        // drops the metric name: `RangeFunction::drops_metric_name` holds for every function but
        // `last_over_time` and `first_over_time`, and `RangeEvaluator::emit` applies it by removing
        // `METADATA_LABELS` from the label set it hands over.
        NativeExpr::Window { input, op } => {
            if op.function.drops_metric_name() {
                Some(false)
            } else {
                output_has_metric_name(input)
            }
        }
        // A map rewrites the label set of every series it is handed. `map_math` and
        // `map_native_histogram` both end with `drop_metric_name` (see
        // [`preserves_canonical_identity`]), so their output carries no name whatever it read, while
        // `label_replace`/`label_join` write a label of the caller's choosing - `__name__` included -
        // and can empty it, so what a series ends under is not this node's to answer.
        NativeExpr::Map { input, op } => {
            if op.ops.is_empty() {
                // A map with no operation rewrites nothing; `validate` refuses the shape, and
                // following the input keeps the answer exact rather than reporting a rewrite that is
                // not there.
                output_has_metric_name(input)
            } else if op
                .ops
                .iter()
                .all(|op| matches!(op, MapOp::Math(_) | MapOp::Histogram(_)))
            {
                Some(false)
            } else {
                None
            }
        }
        // A timestamp reports each sample's position, and `NativeNode::timestamp` rebuilds the labels
        // through `map::drop_metric_name` on the way: its output carries no name.
        NativeExpr::Timestamp { .. } => Some(false),
        // An aggregate answers its group key, which is `GroupingLabels::apply` in
        // [`aggregate`](crate::aggregate): `by(names)` keeps the name only when `names` lists it
        // (`SeriesLabels::keep_only` clears a name the list does not hold) and `without(_)` removes it
        // whatever `names` says. Partial state is not a series at all, so it carries none.
        NativeExpr::Aggregate { input, op } => match &node.output {
            NativeType::Samples(_) => {
                grouping_keeps_metric_name(&op.group, output_has_metric_name(input))
            }
            NativeType::Scalar(_) | NativeType::AggregateState(_) => Some(false),
        },
        // A vector operation is decided one operator at a time.
        //
        // `topk`, `bottomk`, `limitk`, `limit_ratio` and `sort` hand back series they read under the
        // labels those series arrived with - `select.rs` rewrites no label - so the input's answer is
        // theirs. `absent` builds its labels from the argument's matchers and skips `__name__`
        // (`absent_labels`), `histogram_quantile`/`histogram_fraction` remove `METADATA_LABELS` from
        // each group they read (`histogram_eval`), and `count_values` answers a grouping's label sets
        // as an aggregate does.
        NativeExpr::Vector { input, op } => match op {
            BoundVectorOp::TopK { .. }
            | BoundVectorOp::LimitK { .. }
            | BoundVectorOp::LimitRatio { .. }
            | BoundVectorOp::Sort(_) => output_has_metric_name(input),
            BoundVectorOp::Absent { .. } | BoundVectorOp::Histogram(_) => Some(false),
            BoundVectorOp::CountValues { group, .. } => {
                grouping_keeps_metric_name(group, output_has_metric_name(input))
            }
        },
        // A binary operation answers under the label set `result_metric` builds in
        // [`vector_match`](crate::vector_match), which is the many side's labels and the modifiers'
        // rewrite of them.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => match (&left.output, &right.output) {
            // Two constants make a constant, which holds no label set.
            (NativeType::Scalar(_), NativeType::Scalar(_)) => Some(false),
            // A broadcast reaches each series of its vector side as it is, unless the operation drops
            // the metric name on the way: arithmetic and a `bool` comparison do, a filtering
            // comparison keeps the label set it read (`BroadcastRuntime::drops_labels`, which
            // [`broadcast_drops_labels`] restates).
            (NativeType::Scalar(_), _) | (_, NativeType::Scalar(_)) => {
                let vector = if left.output.samples().is_some() {
                    left
                } else {
                    right
                };
                if broadcast_drops_labels(matching) {
                    Some(false)
                } else {
                    output_has_metric_name(vector)
                }
            }
            // Two vectors are matched by the kernel, which files each pairing under the many side's
            // labels with the name removed unless the operation is a filtering comparison: `drop_name`
            // in `vector_match` is `!op.is_comparison() || return_bool`, the same rule the broadcast
            // applies. One-to-one then rewrites that label set by the modifiers - `on(names)` keeps
            // only `names`, `ignoring(names)` drops them - and `group_left`/`group_right` keep it as
            // it is and copy the `include` labels of the one side instead.
            (_, _) => {
                if broadcast_drops_labels(matching) {
                    return Some(false);
                }
                match &matching.cardinality {
                    Cardinality::OneToOne => {
                        let keeps_name = match &matching.labels {
                            MatchingLabels::On(names) => {
                                names.iter().any(|name| &**name == "__name__")
                            }
                            MatchingLabels::Ignoring(names) => {
                                !names.iter().any(|name| &**name == "__name__")
                            }
                        };
                        if keeps_name {
                            output_has_metric_name(left)
                        } else {
                            Some(false)
                        }
                    }
                    Cardinality::ManyToOne(include) => {
                        if include.iter().any(|name| &**name == "__name__") {
                            // The included name of the one side replaces the many side's.
                            None
                        } else {
                            output_has_metric_name(left)
                        }
                    }
                    Cardinality::OneToMany(include) => {
                        if include.iter().any(|name| &**name == "__name__") {
                            None
                        } else {
                            output_has_metric_name(right)
                        }
                    }
                }
            }
        },
        // A set operation keeps each side's samples under the label set they arrived with
        // (`set_vectors` rewrites no label), so a name survives it only if both sides carry one and a
        // name is absent only if both sides lack it. Sides that disagree settle nothing: `or` keeps
        // both sides' samples and `and`/`unless` keep the left side's, so the answer would be a claim
        // about one operator rather than about the shape.
        NativeExpr::Set { left, right, .. } => {
            match (output_has_metric_name(left), output_has_metric_name(right)) {
                (Some(left), Some(right)) if left == right => Some(left),
                _ => None,
            }
        }
        // `info` enriches each base series with the data labels of the info series that share its
        // identifying labels: `add_data_labels` copies the info series' ordinary tags only, never the
        // name, so each result keeps the name its base series had.
        NativeExpr::Info { base, .. } => output_has_metric_name(base),
    }
}

/// What a grouping does to the metric name of the series it is handed.
///
/// `by(names)` keeps the name only when `names` lists it and `without(_)` removes it, which is
/// `GroupingLabels::apply` in [`aggregate`](crate::aggregate): `keep_only` clears a name the list does
/// not hold, and the `without` arm removes `__name__` after removing its own labels.
fn grouping_keeps_metric_name(group: &Grouping, input: Option<bool>) -> Option<bool> {
    match group {
        Grouping::By(names) if names.iter().any(|name| &**name == "__name__") => input,
        Grouping::By(_) | Grouping::Without(_) => Some(false),
    }
}

/// Whether `node` hands over one logical series per canonical label set, or several.
///
/// This is the rule the runtime applies when it decides whether a consumer above `node` has to
/// collect what `node` emits, asked of the plan node rather than of the runtime node. It lives here
/// so that `RuntimeNode::build` in [`execute`](crate::native::execute) and the insertion pass
/// ([`insert_native_merge_inside_current_fused`]) read one rule: a second copy of it would be a
/// rule that can silently disagree with this one about the same tree.
///
/// Three answers, because the flag the runtime computes carries two facts and a boundary answers
/// neither:
///
/// - `Some(true)`: the output is one series per canonical label set, established here. The runtime
///   marks such a node `shared = false` and nothing above it collects.
/// - `Some(false)`: the output may hold several series of one label set, and this node *says so*.
///   The runtime marks it `shared = true`, so a consumer that needs one unified series collects it
///   through `MergeInput`. A [`NativeExpr::Merge`] inserted above such a node would turn that
///   collection off - a merge answers `Some(true)`, so the consumer would stream an already-merged
///   series - and the pass inserts none there.
/// - `None`: the output may hold several, and this node says nothing: it has neither a proof to
///   offer nor a report to make. Only a boundary ([`NativeExpr::Input`]) answers this, and a node
///   that hands its input's series back unchanged passes the answer on. The runtime's flag is
///   `false` here too, so the flag alone cannot tell this case from a proof - and `None` is exactly
///   the case the pass exists for: the one state in which neither the runtime nor the tree collects
///   anything.
///
/// `matches!(answer, Some(false))` is the runtime's `shared` flag bit for bit, which is what makes
/// the two readers' agreement checkable: `the_predicate_and_the_runtime_agree` in
/// [`execute`](crate::native::execute)'s tests asserts it node for node on
/// `RuntimeNode::build`'s own return value.
///
/// [`insert_native_merge_inside_current_fused`]:
///     crate::native::canonicalize::insert_native_merge_inside_current_fused
#[allow(clippy::match_same_arms)] // Each arm is one line of `RuntimeNode::build`, and the reason each answers as it does is written there rather than merged into a bucket.
pub fn unifies_its_own_contributions(node: &NativeNode) -> Option<bool> {
    match &node.expr {
        // A boundary hands over the pieces some upstream execution produced: what it holds is one
        // storage series at a time for a scan and one partition's fragments of a label set for an
        // exchange, and nothing in the tree can tell which series those fragments belong to. The
        // runtime reports `shared = false` for it because a merge binds samples evaluated on a
        // grid, not because the pieces are one series per label set, so a proof is not this
        // function's to give; it answers `None`, and the flag follows with `false`.
        NativeExpr::Input(_) => None,
        // A constant holds no label set at all, so it has nothing that could arrive twice.
        NativeExpr::Scalar(_) => Some(true),
        // A selection reads raw samples - `validate` refuses an evaluated input - and raw samples
        // arrive one whole storage series at a time, so it answers one series per series it read,
        // under the label set it read. `timestamp(x)` is the exception: it reports the sample's own
        // time under the metric name dropped, which can give two series one label set.
        NativeExpr::Instant { op, .. } => Some(op.value != InstantValue::Timestamp),
        // The merge is the collection: whatever its input handed over, it groups the fragments by
        // canonical label set and emits one series per set, so what it emits shares none.
        NativeExpr::Merge { .. } => Some(true),
        // The runtime's rule for a window is `drops_metric_name() || !child_shared`, so a window is
        // *not* shared - and this arm answers `Some(true)` - exactly when both hold: the function
        // keeps the metric name, and the child reports its own fragments. The window then collects
        // them (`MergeInput`) and emits one series per label set.
        //
        // Every other window answers `Some(false)`, and for two different reasons that the runtime's
        // formula does not separate:
        //
        // - the function drops the metric name, so what it emits can coincide with another series'
        //   label set whatever its input held (this is a real report, and the case `Map`, `Timestamp`
        //   and a label-dropping broadcast are in);
        // - or the child does not report fragments at all - `None` for a boundary, or `Some(true)`
        //   for a child that has already unified. With a `None` child the window folds each
        //   contribution on its own, so what it emits does alias its input's fragments and answering
        //   `Some(false)` is exact. With an already-unified child there is only one series per label
        //   set to begin with, so `Some(false)` is conservative rather than exact - it makes a
        //   consumer above collect a stream that needs no collecting, which is what the runtime does
        //   too, and is the safe direction: nothing is left uncollected.
        NativeExpr::Window { input, op } => Some(
            !op.function.drops_metric_name()
                && matches!(unifies_its_own_contributions(input), Some(false)),
        ),
        // A map rewrites labels: `preserves_canonical_identity` is `false` for every [`MapOp`], so
        // two series can end under one label set and the runtime records what it wrote in an
        // `Emitted` table.
        NativeExpr::Map { .. } => Some(false),
        // A timestamp reports a position under the metric name dropped.
        NativeExpr::Timestamp { .. } => Some(false),
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            if evaluated_vector(&left.output).is_some() && evaluated_vector(&right.output).is_some()
            {
                // Two vectors are matched by the kernel, which files its rows by the label set a
                // sample ends under and emits one series per set.
                Some(true)
            } else if broadcast_drops_labels(matching) {
                // A broadcast whose operation drops the metric name can give two series of its
                // vector side one label set.
                Some(false)
            } else {
                // A filtering comparison keeps the label set it read, so the vector side's answer
                // is this node's.
                let vector = if left.output.samples().is_some() {
                    left
                } else {
                    right
                };
                unifies_its_own_contributions(vector)
            }
        }
        // A set operation keeps each side's samples and files them by label set, one series per
        // set.
        NativeExpr::Set { .. } => Some(true),
        // `info` indexes the info side by identifying labels and merges the results that share a
        // label set.
        NativeExpr::Info { .. } => Some(true),
        // A finished aggregate emits one series per group, and two groups are two label sets.
        // `Partial` state is not a series at all, which is not several either.
        NativeExpr::Aggregate { .. } => Some(true),
        // `topk`, `limit_ratio` and `sort` hand back the series they read with their labels, so the
        // input's answer is theirs. The four that read the whole vector - `limitk`, `absent`,
        // `histogram_*` and `count_values` - emit one series per label set: `limitk` counts series
        // in input order, so its input carries the merge the pass inserts below it (or the one the
        // runtime performs when that input reports fragments), and the other three group their rows
        // by label set themselves.
        NativeExpr::Vector { input, op } => {
            let whole = matches!(
                op,
                BoundVectorOp::LimitK { .. }
                    | BoundVectorOp::Absent { .. }
                    | BoundVectorOp::Histogram(_)
                    | BoundVectorOp::CountValues { .. }
            );
            if whole {
                Some(true)
            } else {
                unifies_its_own_contributions(input)
            }
        }
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

// Shared with the rewrite's tests in `canonicalize`, which prove a merge lands where these trees
// say it should.
#[cfg(test)]
pub(crate) mod tests;
