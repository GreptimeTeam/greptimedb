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
//! kernels, fields and grids, so nothing here interprets a function name, resolves a label or picks
//! a column at run time.
//!
//! The tree is also the unit of legality: [`NativeNode::validate`] checks the contract each
//! operation states about its input, so any combination of supported operations that passes it is
//! executable.

use std::hash::Hash;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::aggregate_state::{AggregateStateKind, StateComponent};
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::EvalGrid;
use datatypes::series::{SeriesLabels, SeriesOrdering};

use common_query::native_histogram::native_histogram_arrow_type;

use crate::extension_plan::FieldKind;
use crate::histogram_eval::{HistogramFunction, NativeHistogramFunction};
use crate::info::InfoSelector;
use crate::map::MathOp;
use crate::map_label::LabelRewrite;
use crate::map_time::DateFunction;
use crate::range_eval::{RangeFunction, RangeWindow, fold_offset, predict_linear_instants};
use crate::select::SortOrder;
use crate::vector_match::{
    BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels, SetOp,
};

/// The bucket-bound label a classic histogram function folds away.
const LE_LABEL: &str = "le";

/// The metric-name label. A scan binds it beside its tag columns, not among them.
pub(crate) const METRIC_NAME: &str = "__name__";

/// The labels `map::drop_metric_name` removes (`grid_series::METADATA_LABELS`).
const PROMQL_METADATA_LABELS: [&str; 3] = ["__name__", "__type__", "__unit__"];

/// A conservative set of label names `node` may report, following Prometheus' result labels.
///
/// `source_tags` is indexed by each leaf's `source_index`. The canonical pass supplies runtime
/// label names, including `__name__` for named scans; the planner supplies declared tag columns.
/// The result is a superset over all output series: `or` includes a nullable `__name__` even when
/// both operands drop their names. A node whose kernel drops the metric name drops it here too, so
/// a name rewrite below it cannot make the name reappear above it.
///
/// `None` when the labels are not derived (a `count_values` with an empty destination, or an input
/// without a `source_tags` entry). Callers must treat `None` as "may alias", not as "distinct".
pub fn fused_output_tags(node: &NativeNode, source_tags: &[Vec<String>]) -> Option<Vec<String>> {
    match &node.expr {
        NativeExpr::Input(input) => source_tags.get(input.source_index).cloned(),
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } | NativeExpr::VectorOf { .. } => {
            Some(Vec::new())
        }
        NativeExpr::Instant { input, op } => {
            let tags = fused_output_tags(input, source_tags)?;
            Some(match op.value {
                InstantValue::Timestamp => without_metadata_labels(tags),
                InstantValue::Sample | InstantValue::InfoTimestamp => tags,
            })
        }
        NativeExpr::Window { input, op } => {
            let tags = fused_output_tags(input, source_tags)?;
            Some(if op.function.drops_metric_name() {
                without_metadata_labels(tags)
            } else {
                tags
            })
        }
        NativeExpr::Timestamp { input } => Some(without_metadata_labels(fused_output_tags(
            input,
            source_tags,
        )?)),
        // The ops apply in written order, so a math op after a rewrite of `__name__` still drops it.
        NativeExpr::Map { input, op } => {
            let mut tags = fused_output_tags(input, source_tags)?;
            for operation in op.ops.iter() {
                match operation {
                    MapOp::Label(rewrite) => {
                        let destination = rewrite.destination();
                        if tags.iter().any(|tag| tag == destination) {
                            tags.retain(|tag| tag != destination);
                            tags.push(destination.to_string());
                        } else if rewrite.writes_value() {
                            tags.push(destination.to_string());
                        }
                    }
                    MapOp::Math(_) | MapOp::Histogram(_) => tags = without_metadata_labels(tags),
                }
            }
            Some(tags)
        }
        NativeExpr::ParameterizedMap { input, .. } => Some(without_metadata_labels(
            fused_output_tags(input, source_tags)?,
        )),
        NativeExpr::ScalarOf { .. } => Some(Vec::new()),
        NativeExpr::ParameterizedTopK { input, .. } => fused_output_tags(input, source_tags),
        NativeExpr::ParameterizedHistogram { input, .. } => {
            let mut tags = fused_output_tags(input, source_tags)?;
            tags.retain(|tag| tag != LE_LABEL && !PROMQL_METADATA_LABELS.contains(&tag.as_str()));
            Some(tags)
        }
        NativeExpr::Aggregate { input, op } => grouped_output_tags(input, op, source_tags),
        NativeExpr::ParameterizedAggregate { input, op, .. } => {
            grouped_output_tags(input, op, source_tags)
        }
        NativeExpr::Vector { input, op } => match op {
            BoundVectorOp::TopK { .. }
            | BoundVectorOp::LimitK { .. }
            | BoundVectorOp::LimitRatio { .. }
            | BoundVectorOp::Sort(_) => fused_output_tags(input, source_tags),
            // `absent` reports the equality matchers' labels, not the input's.
            BoundVectorOp::Absent { labels } => Some(
                labels
                    .tags()
                    .iter()
                    .map(|label| label.name.to_string())
                    .collect(),
            ),
            // `count_values` adds its destination label, then applies its grouping; `by` always
            // keeps the destination.
            BoundVectorOp::CountValues { label, group } if !label.is_empty() => {
                let mut tags = fused_output_tags(input, source_tags)?;
                if !tags.contains(&label.to_string()) {
                    tags.push(label.to_string());
                }
                Some(match group {
                    Grouping::By(names) => {
                        let mut output = tags
                            .into_iter()
                            .filter(|tag| names.iter().any(|name| name.as_ref() == tag))
                            .collect::<Vec<_>>();
                        if !output.contains(&label.to_string()) {
                            output.push(label.to_string());
                        }
                        output
                    }
                    Grouping::Without(names) => tags
                        .into_iter()
                        .filter(|tag| {
                            tag != METRIC_NAME && !names.iter().any(|name| name.as_ref() == tag)
                        })
                        .collect(),
                })
            }
            BoundVectorOp::CountValues { .. } => None,
            BoundVectorOp::Histogram(_) => {
                let mut tags = fused_output_tags(input, source_tags)?;
                tags.retain(|tag| {
                    tag != LE_LABEL && !PROMQL_METADATA_LABELS.contains(&tag.as_str())
                });
                Some(tags)
            }
        },
        NativeExpr::Merge { input } => fused_output_tags(input, source_tags),
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            let keeps_name = keeps_metric_name(matching);
            let scalar_side = matches!(&left.output, NativeType::Scalar(_))
                || matches!(&right.output, NativeType::Scalar(_));
            let tags = match (&left.output, &right.output) {
                (NativeType::Scalar(_), _) => fused_output_tags(right, source_tags)?,
                (_, NativeType::Scalar(_)) => fused_output_tags(left, source_tags)?,
                _ => {
                    let kept = |tags: Vec<String>| -> Vec<String> {
                        match &matching.labels {
                            MatchingLabels::On(names) => tags
                                .into_iter()
                                .filter(|tag| {
                                    names.iter().any(|name| name.as_ref() == tag.as_str())
                                })
                                .collect(),
                            MatchingLabels::Ignoring(names) => tags
                                .into_iter()
                                .filter(|tag| {
                                    !names.iter().any(|name| name.as_ref() == tag.as_str())
                                })
                                .collect(),
                        }
                    };
                    let left_tags = fused_output_tags(left, source_tags)?;
                    let right_tags = fused_output_tags(right, source_tags)?;
                    // One-to-one narrows the left side's labels by `on`/`ignoring`; a group keeps
                    // the many side's labels and copies its include labels from the one side.
                    let mut tags = match &matching.cardinality {
                        Cardinality::OneToOne => kept(left_tags.clone()),
                        Cardinality::ManyToOne(_) => left_tags.clone(),
                        Cardinality::OneToMany(_) => right_tags.clone(),
                    };
                    // `result_metric` drops the name before copying include labels, so an include can
                    // still put it back from the one side.
                    if !keeps_name {
                        tags.retain(|tag| tag != METRIC_NAME);
                    }
                    let (includes, include_tags): (&[Arc<str>], &Vec<String>) =
                        match &matching.cardinality {
                            Cardinality::OneToOne => (&[], &left_tags),
                            Cardinality::ManyToOne(labels) => (labels, &right_tags),
                            Cardinality::OneToMany(labels) => (labels, &left_tags),
                        };
                    for tag in include_tags {
                        if includes.iter().any(|label| label.as_ref() == tag.as_str())
                            && !tags.contains(tag)
                        {
                            tags.push(tag.clone());
                        }
                    }
                    // `result_metric` removes an include label from the many side unless the one side
                    // has it. A row filled from a many-side series takes it from its match key, so
                    // the label survives only if the one side carries it or a fill keys on it.
                    if !includes.is_empty() {
                        let many_rows_filled = match &matching.cardinality {
                            // `group_right` puts the one side on the left, so the fills swap.
                            Cardinality::OneToMany(_) => matching.fill.lhs,
                            Cardinality::ManyToOne(_) | Cardinality::OneToOne => matching.fill.rhs,
                        }
                        .is_some();
                        let in_match_key = |label: &str| match &matching.labels {
                            MatchingLabels::On(names) => {
                                names.iter().any(|name| name.as_ref() == label)
                            }
                            // A match key is built from a series' tags, which never hold the metric name.
                            MatchingLabels::Ignoring(names) => {
                                label != "__name__"
                                    && !names.iter().any(|name| name.as_ref() == label)
                            }
                        };
                        tags.retain(|tag| {
                            !includes.iter().any(|label| label.as_ref() == tag.as_str())
                                || include_tags.contains(tag)
                                || (many_rows_filled && in_match_key(tag))
                        });
                    }
                    // A row filled from a one-side series keeps only its matching labels plus the
                    // include labels (`vector_match::binary_field`), so only the fill for a missing
                    // many side adds labels here. `group_right` swaps the fills.
                    let (one_tags, fills_many_side) = match &matching.cardinality {
                        Cardinality::OneToMany(_) => (&left_tags, matching.fill.rhs),
                        Cardinality::ManyToOne(_) | Cardinality::OneToOne => {
                            (&right_tags, matching.fill.lhs)
                        }
                    };
                    // A filled one-side row carries the name only through an `on(__name__)` key.
                    let fill_carries_name = keeps_name
                        && matches!(&matching.labels, MatchingLabels::On(names)
                            if names.iter().any(|name| name.as_ref() == METRIC_NAME));
                    if fills_many_side.is_some() {
                        for tag in kept(one_tags.clone()) {
                            if tag == METRIC_NAME && !fill_carries_name {
                                continue;
                            }
                            if !tags.contains(&tag) {
                                tags.push(tag);
                            }
                        }
                    }
                    tags
                }
            };
            Some(if scalar_side && !keeps_name {
                without_metadata_labels(tags)
            } else {
                tags
            })
        }
        NativeExpr::Set { left, right, op } => {
            let mut tags = fused_output_tags(left, source_tags)?;
            if op.op == SetOp::Or {
                for tag in fused_output_tags(right, source_tags)? {
                    if !tags.contains(&tag) {
                        tags.push(tag);
                    }
                }
                // Each `or` row keeps its own side's name, so the result needs the column even
                // when neither source declares it.
                if !tags.iter().any(|tag| tag == METRIC_NAME) {
                    tags.push(METRIC_NAME.to_string());
                }
            }
            Some(tags)
        }
        // The selector's data labels, or all of the info series' labels when it names none.
        NativeExpr::Info {
            base,
            info,
            selector,
        } => {
            let mut tags = fused_output_tags(base, source_tags)?;
            for tag in fused_output_tags(info, source_tags)? {
                let copied = selector.data_labels.is_empty()
                    || selector
                        .data_labels
                        .iter()
                        .any(|label| label.as_ref() == tag.as_str());
                if copied && !tags.contains(&tag) {
                    tags.push(tag);
                }
            }
            Some(tags)
        }
    }
}

/// `without(...)` also drops the metric name, as Prometheus does. A named scan reports `__name__`
/// only among the runtime labels, so `by (__name__)` keeps it there and publishes no column for it.
fn grouped_output_tags(
    input: &NativeNode,
    op: &BoundAggregate,
    source_tags: &[Vec<String>],
) -> Option<Vec<String>> {
    let input_tags = fused_output_tags(input, source_tags)?;
    Some(match &op.group {
        Grouping::By(kept) => input_tags
            .into_iter()
            .filter(|tag| kept.iter().any(|name| name.as_ref() == tag.as_str()))
            .collect(),
        Grouping::Without(dropped) => input_tags
            .into_iter()
            .filter(|tag| {
                tag != METRIC_NAME && !dropped.iter().any(|name| name.as_ref() == tag.as_str())
            })
            .collect(),
    })
}

/// Whether a binary operation keeps the metric name: only a filtering comparison does.
fn keeps_metric_name(matching: &BinaryMatching) -> bool {
    matching.op.is_comparison() && !matching.return_bool
}

/// Mirrors `map::drop_metric_name`, which removes `__type__` and `__unit__` along with the name.
fn without_metadata_labels(tags: Vec<String>) -> Vec<String> {
    tags.into_iter()
        .filter(|tag| !PROMQL_METADATA_LABELS.contains(&tag.as_str()))
        .collect()
}

/// What one node produces.
///
/// Aggregate state is a type of its own, so an operation that cannot consume it is rejected by type.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub enum NativeType {
    /// Samples, evaluated when the schema carries a grid and raw otherwise.
    Samples(Arc<SeriesSchema>),
    /// One value per grid point and no labels, which binary operations broadcast rather than
    /// match.
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

    /// The grid this type is evaluated on, or `None` for raw samples and state.
    pub fn grid(&self) -> Option<&Arc<EvalGrid>> {
        match self {
            Self::Samples(schema) => schema.eval_grid.as_ref(),
            Self::Scalar(grid) => Some(grid),
            Self::AggregateState(_) => None,
        }
    }

    fn describe(&self) -> &'static str {
        match self {
            Self::Samples(schema) if schema.eval_grid.is_some() => "an evaluated vector",
            Self::Samples(_) => "raw samples",
            Self::Scalar(_) => "a scalar",
            Self::AggregateState(_) => "aggregate state",
        }
    }

    /// Whether `self` and `other` bind equal schemas. Scalars never come from a source, so they
    /// never match.
    fn same_binding(&self, other: &NativeType) -> bool {
        match (self, other) {
            (Self::Samples(mine), Self::Samples(theirs)) => **mine == **theirs,
            (Self::AggregateState(mine), Self::AggregateState(theirs)) => **mine == **theirs,
            _ => false,
        }
    }
}

/// The [`FieldKind`] every referenced field carries, or `None` when they disagree or one is neither
/// `Float64` nor the native histogram struct.
pub fn uniform_field_kind<'a>(
    kinds: impl IntoIterator<Item = Option<&'a DataType>>,
) -> Option<FieldKind> {
    let mut answer = None;
    for kind in kinds {
        let this = match kind? {
            DataType::Float64 => FieldKind::Float,
            data_type if data_type == &native_histogram_arrow_type() => FieldKind::Histogram,
            _ => return None,
        };
        match answer {
            None => answer = Some(this),
            Some(seen) if seen == this => {}
            Some(_) => return None,
        }
    }
    answer
}

/// The output kind of a binary operation over two evaluated vectors, including fill pairs.
pub fn binary_output_field_kind(
    left: FieldKind,
    right: FieldKind,
    op: BinaryOp,
    return_bool: bool,
    fill: FillValues,
) -> Option<FieldKind> {
    if return_bool {
        return Some(FieldKind::Float);
    }
    match op {
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Mod
        | BinaryOp::Pow
        | BinaryOp::Atan2 => {
            // An unsupported pair emits no sample. Fill values are floats, so the field must agree
            // across the real pair and each enabled fill pair.
            let payload_kind = |left, right| match (left, op, right) {
                (FieldKind::Float, _, FieldKind::Float) => Some(FieldKind::Float),
                (FieldKind::Histogram, BinaryOp::Add | BinaryOp::Sub, FieldKind::Histogram)
                | (FieldKind::Histogram, BinaryOp::Mul | BinaryOp::Div, FieldKind::Float)
                | (FieldKind::Float, BinaryOp::Mul, FieldKind::Histogram) => {
                    Some(FieldKind::Histogram)
                }
                _ => None,
            };
            let candidates = [
                payload_kind(left, right),
                fill.rhs.and_then(|_| payload_kind(left, FieldKind::Float)),
                fill.lhs.and_then(|_| payload_kind(FieldKind::Float, right)),
            ];
            let mut kind = None;
            for candidate in candidates.into_iter().flatten() {
                if kind.is_some_and(|existing| existing != candidate) {
                    return None;
                }
                kind = Some(candidate);
            }
            // With no possible samples, retain the legacy float layout for an empty result.
            Some(kind.unwrap_or(FieldKind::Float))
        }
        // A mixed-kind comparison emits no sample (`vector_match::elem_binop`), so it gets the
        // empty float layout.
        _ if left == right => Some(left),
        _ => Some(FieldKind::Float),
    }
}

impl NativeNode {
    /// The kind this node's output field carries, given the kind its sources carry.
    ///
    /// `None` when `input` is `None` and the tree reads a source, or when the kind is not derived,
    /// e.g. a math function over histograms, a set over two kinds, or a vector binary whose fill
    /// pairs disagree.
    pub fn output_field_kind(&self, input: Option<FieldKind>) -> Option<FieldKind> {
        let kind_of = |node: &NativeNode| node.output_field_kind(input);
        match &self.expr {
            NativeExpr::Input(_) => input,
            NativeExpr::Scalar(_)
            | NativeExpr::EvalTime { .. }
            | NativeExpr::VectorOf { .. }
            | NativeExpr::Timestamp { .. } => Some(FieldKind::Float),
            NativeExpr::Window { input, op } => {
                let inner = kind_of(input)?;
                (!op.function.yields_histograms())
                    .then_some(FieldKind::Float)
                    .or(Some(inner))
            }
            NativeExpr::Instant { input, op } => match op.value {
                InstantValue::Sample => kind_of(input),
                InstantValue::Timestamp | InstantValue::InfoTimestamp => Some(FieldKind::Float),
            },
            NativeExpr::Map { input, op } => {
                let mut kind = kind_of(input)?;
                for op in op.ops.iter() {
                    kind = match op {
                        MapOp::Histogram(_) => FieldKind::Float,
                        MapOp::Label(_) => kind,
                        MapOp::Math(_) if kind == FieldKind::Histogram => return None,
                        MapOp::Math(_) => FieldKind::Float,
                    };
                }
                Some(kind)
            }
            NativeExpr::Binary {
                left,
                right,
                matching,
            } => {
                if matching.return_bool {
                    return Some(FieldKind::Float);
                }
                match (&left.expr, &right.expr) {
                    (NativeExpr::Scalar(_), NativeExpr::Scalar(_)) => Some(FieldKind::Float),
                    (NativeExpr::Scalar(_), _) => kind_of(right),
                    (_, NativeExpr::Scalar(_)) => kind_of(left),
                    (_, _) => {
                        let left_kind = kind_of(left)?;
                        let right_kind = kind_of(right)?;
                        binary_output_field_kind(
                            left_kind,
                            right_kind,
                            matching.op,
                            false,
                            matching.fill,
                        )
                    }
                }
            }
            NativeExpr::Merge { input } => kind_of(input),
            // A set passes both sides' samples through, so it is typed only when the sides agree.
            NativeExpr::Set { left, right, .. } => {
                let left_kind = kind_of(left)?;
                (left_kind == kind_of(right)?).then_some(left_kind)
            }
            NativeExpr::ScalarOf { .. } => Some(FieldKind::Float),
            NativeExpr::ParameterizedTopK { input, .. } => kind_of(input),
            NativeExpr::ParameterizedHistogram { .. } => Some(FieldKind::Float),
            NativeExpr::Aggregate { op, .. } => Some(aggregate_output_kind(op)),
            NativeExpr::ParameterizedAggregate { op, .. } => Some(aggregate_output_kind(op)),
            NativeExpr::Vector { input, op } => Some(match op {
                BoundVectorOp::TopK { .. }
                | BoundVectorOp::LimitK { .. }
                | BoundVectorOp::LimitRatio { .. }
                | BoundVectorOp::Sort(_) => kind_of(input)?,
                BoundVectorOp::Absent { .. }
                | BoundVectorOp::Histogram(_)
                | BoundVectorOp::CountValues { .. } => FieldKind::Float,
            }),
            NativeExpr::Info { base, .. } => kind_of(base),
            // `round` and `clamp*` skip histogram samples rather than converting them
            // (`map::map_math`); `max_of`/`min_of` take a scalar input, which is float.
            NativeExpr::ParameterizedMap { input, .. } => kind_of(input),
        }
    }
}

/// The kind an aggregate's output column carries, which follows its state binding rather than its
/// input: `count` over histograms answers a float, `sum`/`avg` over histograms a histogram.
pub fn aggregate_output_kind(op: &BoundAggregate) -> FieldKind {
    match aggregate_state_binding(op.kind, op.field_kind) {
        Some((kind, _)) if kind.is_histogram() => FieldKind::Histogram,
        _ => FieldKind::Float,
    }
}

/// The layout of intermediate aggregate state, indexed by `(group labels, absolute grid index)`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct AggregateStateSchema {
    pub group: Grouping,
    /// Aggregate whose partial accumulator this state carries.
    pub kind: AggregateStateKind,
    /// Ordered accumulator components, shared with the kernel layout.
    pub components: Arc<[StateComponent]>,
    /// The accumulator columns, one per aggregated field.
    pub accumulators: Arc<[FieldSpec]>,
    /// The grid the state is indexed on.
    pub grid: Arc<EvalGrid>,
}

/// The state an aggregate over `field_kind` binds, or `None` when it has no mergeable partial state
/// (a quantile). Numeric components come from the accumulator kernel, so the two stay in lockstep.
pub fn aggregate_state_binding(
    kind: AggregateKind,
    field_kind: FieldKind,
) -> Option<(AggregateStateKind, Vec<StateComponent>)> {
    if field_kind == FieldKind::Histogram {
        let state_kind = match kind {
            AggregateKind::Sum => AggregateStateKind::HistogramSum,
            AggregateKind::Avg => AggregateStateKind::HistogramAvg,
            // The rest answer floats over histograms (count, or skipping histogram samples).
            _ => return float_state_binding(kind),
        };
        return Some((state_kind, state_kind.components().to_vec()));
    }
    float_state_binding(kind)
}

fn float_state_binding(kind: AggregateKind) -> Option<(AggregateStateKind, Vec<StateComponent>)> {
    use crate::aggregate::{AggregateOp, StateField};

    let (state_kind, op) = match kind {
        AggregateKind::Sum => (AggregateStateKind::Sum, AggregateOp::Sum),
        AggregateKind::Count => (AggregateStateKind::Count, AggregateOp::Count),
        AggregateKind::Avg => (AggregateStateKind::Avg, AggregateOp::Avg),
        AggregateKind::Min => (AggregateStateKind::Min, AggregateOp::Min),
        AggregateKind::Max => (AggregateStateKind::Max, AggregateOp::Max),
        AggregateKind::Group => (AggregateStateKind::Group, AggregateOp::Group),
        AggregateKind::Stddev => (AggregateStateKind::Stddev, AggregateOp::Stddev),
        AggregateKind::Stdvar => (AggregateStateKind::Stdvar, AggregateOp::Stdvar),
        AggregateKind::Quantile { .. } => return None,
    };
    let components = op
        .state_fields()
        .iter()
        .map(|field| match field {
            StateField::Value => StateComponent::Value,
            StateField::Mean => StateComponent::Mean,
            StateField::Compensation => StateComponent::Compensation,
            StateField::Count => StateComponent::Count,
            StateField::IncrementalMean => StateComponent::IncrementalMean,
        })
        .collect();
    Some((state_kind, components))
}

/// A scan or exchange boundary bound as a node input.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct BoundInput {
    /// Child index in the DataFusion plan. The compiler assigns selector-occurrence indices; the
    /// caller maps them to child indices. Several inputs may share one child, so this is not an
    /// identity for an input leaf.
    pub source_index: usize,
    /// Raw samples from a scan, or state from an exchange boundary.
    pub output: NativeType,
    /// The ordering the input guarantees; an instant selection and a window reject an interleaved
    /// one.
    pub ordering: SeriesOrdering,
}

/// A constant scalar. Varying scalars ([`NativeExpr::EvalTime`], [`NativeExpr::ScalarOf`], a
/// scalar-producing [`NativeExpr::ParameterizedMap`], a binary between scalars) share its
/// [`NativeType::Scalar`] output type.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BoundScalar {
    pub value: f64,
}

/// An instant selection: the last sample at or before each grid point, within a lookback.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct BoundInstant {
    /// How far back a sample may lie before it counts as stale.
    pub lookback_ms: i64,
    /// Selector offset; positive reads earlier samples.
    pub offset_ms: i64,
    /// The `@` instant; when set, every grid point selects at this time.
    pub at_ms: Option<i64>,
    pub value: InstantValue,
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

/// A window function over raw samples or over an evaluated input. A subquery's input grid differs
/// from its output grid; the input is consumed without resampling.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct BoundWindow {
    pub function: RangeFunction,
    pub window: RangeWindow,
    /// The grid the input is evaluated on, or `None` when the input is raw samples.
    pub input_grid: Option<Arc<EvalGrid>>,
    pub output_grid: Arc<EvalGrid>,
}

/// A chain of point-wise operations applied to a whole evaluated value.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct BoundMap {
    /// The operations, applied in order.
    pub ops: Arc<[MapOp]>,
}

/// A math function whose scalar parameters are bound as nodes because at least one varies per grid
/// point. With constant parameters the same function is a [`MathOp`] in a [`BoundMap`].
///
/// `max_of` and `min_of` are functions of two scalars: the input is the first argument and the one
/// parameter is the second.
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
    /// `max_of(a, b)`: the larger of two scalars.
    MaxOf,
    /// `min_of(a, b)`: the smaller of two scalars.
    MinOf,
}

impl ParameterizedOp {
    /// The number of parameters after the input.
    pub fn arity(self) -> usize {
        match self {
            Self::Clamp => 2,
            Self::Round | Self::ClampMin | Self::ClampMax | Self::MaxOf | Self::MinOf => 1,
        }
    }

    /// Whether the input and output are scalars rather than vectors.
    pub fn takes_a_scalar_input(self) -> bool {
        matches!(self, Self::MaxOf | Self::MinOf)
    }
}

/// One per-series operation of a map. Each can give two series one label set.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub enum MapOp {
    Math(MathOp),
    Histogram(NativeHistogramFunction),
    Label(Arc<LabelRewrite>),
}

/// An aggregate that folds each group's values into one value per grid point. Series-selecting
/// aggregates and `count_values` are [`BoundVectorOp`]s.
///
/// [`AggregateKind::Quantile`] carries an `f64`, so equality and hashing are implemented by hand
/// over its bits.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum AggregateKind {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    Group,
    Stddev,
    Stdvar,
    Quantile {
        /// φ as written, unbounded: Prometheus answers `-Inf` below `0` and `+Inf` above `1`.
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

/// How an aggregate's groups are identified; mirrors [`aggregate::Grouping`].
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
    pub kind: AggregateKind,
    pub group: Grouping,
    pub stage: AggregateStage,
    /// The direct input's field kind, which picks the state binding. It is not the source's kind:
    /// `sum(count_over_time(h[5m]))` aggregates floats built from histograms.
    pub field_kind: FieldKind,
}

/// An operator over the whole input vector at each grid point, with constant parameters. A per-point
/// `k` is [`NativeExpr::ParameterizedTopK`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum BoundVectorOp {
    /// `topk` (`top`) or `bottomk`: the `k` largest or smallest values of each group.
    TopK { k: f64, top: bool, group: Grouping },
    /// `limitk`: the first `k` series of each group that hold a sample, in input order.
    LimitK { k: f64, group: Grouping },
    /// `limit_ratio`: the series whose label hash falls in the ratio; it keeps whole series, so it
    /// has no grouping.
    LimitRatio { ratio: f64 },
    /// `sort`, `sort_desc`, `sort_by_label` and `sort_by_label_desc`. The order shows only in an
    /// instant result; `sort` and `sort_desc` also drop histogram samples, in a range result too.
    Sort(SortOrder),
    /// `absent`: `1` under `labels` (derived from the argument's matchers) at every point where the
    /// input holds no sample.
    Absent { labels: SeriesLabels },
    /// `histogram_quantile` or `histogram_fraction` over classic bucket series.
    Histogram(HistogramFunction),
    /// `count_values(label, v)`.
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
    pub labels: MatchingLabels,
}

/// What a node computes.
///
/// Postcard encodes a variant by its position and a [`crate::extension_plan::FusedSeries`] carries
/// the whole [`NativeTree`], so variant indices are a wire contract: append new variants at the end
/// (pinned by `native_expr_variant_indices_are_a_wire_contract`).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum NativeExpr {
    /// A scan or an exchange boundary.
    Input(BoundInput),
    /// A constant scalar.
    Scalar(BoundScalar),
    /// `time()` in seconds, or a date function called without an argument.
    ///
    /// The date function is bound here rather than over `time()` because Prometheus reads the
    /// calendar from the timestamp truncated to whole seconds, which the float `time()` does not
    /// carry exactly.
    EvalTime { date: Option<DateFunction> },
    /// `vector(s)`: the scalar as one series without labels.
    VectorOf { input: Box<NativeNode> },
    /// An instant selection over raw samples.
    Instant {
        input: Box<NativeNode>,
        op: BoundInstant,
    },
    /// Canonicalizes evaluated fragments that share a label set.
    Merge { input: Box<NativeNode> },
    /// A range function over raw samples or a subquery's evaluated input.
    Window {
        input: Box<NativeNode>,
        op: BoundWindow,
    },
    /// Point-wise operations, applied in order.
    Map {
        input: Box<NativeNode>,
        op: BoundMap,
    },
    /// A point-wise math function whose parameters are scalar nodes, in argument order. For
    /// `max_of`/`min_of` the input is the first scalar argument and the output is a scalar.
    ParameterizedMap {
        input: Box<NativeNode>,
        op: ParameterizedOp,
        parameters: Vec<NativeNode>,
    },
    /// The timestamp of an evaluated input, in seconds.
    Timestamp { input: Box<NativeNode> },
    /// An aggregate at the stage its binding names.
    Aggregate {
        input: Box<NativeNode>,
        op: BoundAggregate,
    },
    /// An operation over the whole input vector at each grid point.
    Vector {
        input: Box<NativeNode>,
        op: BoundVectorOp,
    },
    /// Scalar operands are broadcast, so `matching` only applies between two vectors.
    Binary {
        left: Box<NativeNode>,
        right: Box<NativeNode>,
        matching: BinaryMatching,
    },
    /// `and`, `or` or `unless` between two vectors.
    Set {
        left: Box<NativeNode>,
        right: Box<NativeNode>,
        op: BoundSetOp,
    },
    /// `info(base, {selector})`. `info` is an instant selection with
    /// [`InstantValue::InfoTimestamp`].
    Info {
        base: Box<NativeNode>,
        info: Box<NativeNode>,
        selector: InfoSelector,
    },
    /// `scalar(v)`: at each grid point, the sole sample value or NaN when the vector has zero or
    /// more than one sample.
    ScalarOf { input: Box<NativeNode> },
    /// `topk`/`bottomk` with a per-point `k`. `op` is a `TopK` whose own `k` is ignored.
    ParameterizedTopK {
        input: Box<NativeNode>,
        op: BoundVectorOp,
        parameter: Box<NativeNode>,
    },
    /// A classic histogram vector operation whose arguments vary on the output grid.
    ParameterizedHistogram {
        input: Box<NativeNode>,
        op: BoundVectorOp,
        parameters: Vec<NativeNode>,
    },
    /// `quantile` with a per-point φ. `op` is a `Quantile` whose own φ is ignored.
    ParameterizedAggregate {
        input: Box<NativeNode>,
        op: BoundAggregate,
        parameter: Box<NativeNode>,
    },
}

/// One node of a bound tree.
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq, Hash)]
pub struct NativeNode {
    pub expr: NativeExpr,
    pub output: NativeType,
}

impl NativeExpr {
    /// The subtrees this expression reads, in the order the node evaluates them.
    pub fn children(&self) -> Vec<&NativeNode> {
        match self {
            NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {
                Vec::new()
            }
            NativeExpr::VectorOf { input }
            | NativeExpr::Instant { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. }
            | NativeExpr::ScalarOf { input } => vec![input],
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            }
            | NativeExpr::ParameterizedHistogram {
                input, parameters, ..
            } => std::iter::once(input.as_ref())
                .chain(parameters.iter())
                .collect(),
            NativeExpr::ParameterizedTopK {
                input, parameter, ..
            }
            | NativeExpr::ParameterizedAggregate {
                input, parameter, ..
            } => vec![input, parameter],
            NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
                vec![left, right]
            }
            NativeExpr::Info { base, info, .. } => vec![base, info],
        }
    }

    /// This expression with its children replaced, in the order [`Self::children`] reports them.
    ///
    /// Panics when `children` holds fewer nodes than the expression reads.
    pub fn with_children(&self, children: Vec<NativeNode>) -> NativeExpr {
        let mut children = children.into_iter();
        let mut next = || children.next().expect("one replacement per child");
        match self {
            NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {
                self.clone()
            }
            NativeExpr::VectorOf { .. } => NativeExpr::VectorOf {
                input: Box::new(next()),
            },
            NativeExpr::Instant { op, .. } => NativeExpr::Instant {
                input: Box::new(next()),
                op: op.clone(),
            },
            NativeExpr::Merge { .. } => NativeExpr::Merge {
                input: Box::new(next()),
            },
            NativeExpr::Window { op, .. } => NativeExpr::Window {
                input: Box::new(next()),
                op: op.clone(),
            },
            NativeExpr::Map { op, .. } => NativeExpr::Map {
                input: Box::new(next()),
                op: op.clone(),
            },
            NativeExpr::ParameterizedMap { op, parameters, .. } => NativeExpr::ParameterizedMap {
                input: Box::new(next()),
                op: *op,
                parameters: (0..parameters.len()).map(|_| next()).collect(),
            },
            NativeExpr::ScalarOf { .. } => NativeExpr::ScalarOf {
                input: Box::new(next()),
            },
            NativeExpr::ParameterizedTopK { op, .. } => NativeExpr::ParameterizedTopK {
                input: Box::new(next()),
                op: op.clone(),
                parameter: Box::new(next()),
            },
            NativeExpr::ParameterizedAggregate { op, .. } => NativeExpr::ParameterizedAggregate {
                input: Box::new(next()),
                op: op.clone(),
                parameter: Box::new(next()),
            },
            NativeExpr::ParameterizedHistogram { op, parameters, .. } => {
                NativeExpr::ParameterizedHistogram {
                    input: Box::new(next()),
                    op: op.clone(),
                    parameters: (0..parameters.len()).map(|_| next()).collect(),
                }
            }
            NativeExpr::Timestamp { .. } => NativeExpr::Timestamp {
                input: Box::new(next()),
            },
            NativeExpr::Aggregate { op, .. } => NativeExpr::Aggregate {
                input: Box::new(next()),
                op: op.clone(),
            },
            NativeExpr::Vector { op, .. } => NativeExpr::Vector {
                input: Box::new(next()),
                op: op.clone(),
            },
            NativeExpr::Binary { matching, .. } => NativeExpr::Binary {
                left: Box::new(next()),
                right: Box::new(next()),
                matching: matching.clone(),
            },
            NativeExpr::Set { op, .. } => NativeExpr::Set {
                left: Box::new(next()),
                right: Box::new(next()),
                op: op.clone(),
            },
            NativeExpr::Info { selector, .. } => NativeExpr::Info {
                base: Box::new(next()),
                info: Box::new(next()),
                selector: selector.clone(),
            },
        }
    }
}

impl NativeNode {
    /// Whether `predicate` holds for this node or any node below it.
    pub fn any(&self, predicate: &impl Fn(&NativeNode) -> bool) -> bool {
        predicate(self)
            || self
                .expr
                .children()
                .into_iter()
                .any(|child| child.any(predicate))
    }

    /// The input leaves below this node in evaluation order, one entry per leaf.
    pub fn inputs(&self) -> Vec<&BoundInput> {
        fn collect<'a>(node: &'a NativeNode, inputs: &mut Vec<&'a BoundInput>) {
            if let NativeExpr::Input(input) = &node.expr {
                inputs.push(input);
            }
            for child in node.expr.children() {
                collect(child, inputs);
            }
        }
        let mut inputs = Vec::new();
        collect(self, &mut inputs);
        inputs
    }

    /// Whether this node reads no data, so every execution computes the same value for it.
    pub fn is_data_free(&self) -> bool {
        match &self.expr {
            NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => true,
            NativeExpr::Binary { left, right, .. } => left.is_data_free() && right.is_data_free(),
            NativeExpr::VectorOf { input } => input.is_data_free(),
            _ => false,
        }
    }
}

/// Whether this tree can hand over two series that share one canonical label set (the metric name
/// plus the ordinary labels, which is what [`NativeExpr::Merge`] groups by).
///
/// A scan partition holds one series per canonical label set, so only a node that writes an
/// existing label or drops one can create duplicates:
///
/// - A label rewrite whose destination the input already carries, judged against `source_tags`
///   (indexed by `source_index`). A `__name__` destination always counts, because the scan below
///   may hold several metrics whose ordinary labels are equal.
/// - A node that drops [`PROMQL_METADATA_LABELS`] (a name-dropping window, `timestamp`, a math or
///   histogram map, a parameterized map, an arithmetic or `bool` scalar broadcast), unless
///   [`metadata_labels_keep_series_apart`] proves the drop injective.
///
/// Nodes that file their rows by result label set (aggregates, vector-vector binaries, sets, `info`,
/// `absent`, `count_values`, histogram functions) answer `false`, and so does `Merge`, which is what
/// makes [`merge_label_rewrites`](crate::native::canonicalize::merge_label_rewrites) idempotent.
/// Nodes that pass their input's series through propagate the input's answer.
pub fn duplicates_canonical_labels(
    node: &NativeNode,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
) -> bool {
    match &node.expr {
        NativeExpr::Input(_)
        | NativeExpr::Scalar(_)
        | NativeExpr::EvalTime { .. }
        | NativeExpr::VectorOf { .. } => false,
        NativeExpr::Merge { .. } => false,
        NativeExpr::Instant { input, op } => {
            (op.value == InstantValue::Timestamp
                && !metadata_labels_keep_series_apart(input, source_tags, source_metrics))
                || duplicates_canonical_labels(input, source_tags, source_metrics)
        }
        NativeExpr::Window { input, op } => {
            (op.function.drops_metric_name()
                && !metadata_labels_keep_series_apart(input, source_tags, source_metrics))
                || duplicates_canonical_labels(input, source_tags, source_metrics)
        }
        NativeExpr::Timestamp { input } => {
            !metadata_labels_keep_series_apart(input, source_tags, source_metrics)
                || duplicates_canonical_labels(input, source_tags, source_metrics)
        }
        NativeExpr::ParameterizedMap { input, .. } => {
            !metadata_labels_keep_series_apart(input, source_tags, source_metrics)
                || duplicates_canonical_labels(input, source_tags, source_metrics)
        }
        NativeExpr::ParameterizedTopK { input, .. } => {
            duplicates_canonical_labels(input, source_tags, source_metrics)
        }
        NativeExpr::ScalarOf { .. } => false,
        NativeExpr::ParameterizedHistogram { .. } => false,
        NativeExpr::Map { input, op } => {
            (op.ops
                .iter()
                .any(|operation| matches!(operation, MapOp::Math(_) | MapOp::Histogram(_)))
                && !metadata_labels_keep_series_apart(input, source_tags, source_metrics))
                || map_writes_existing_label(input, op, source_tags).unwrap_or(true)
                || duplicates_canonical_labels(input, source_tags, source_metrics)
        }
        NativeExpr::Aggregate { .. } | NativeExpr::ParameterizedAggregate { .. } => false,
        NativeExpr::Vector { input, op } => match op {
            BoundVectorOp::TopK { .. }
            | BoundVectorOp::LimitK { .. }
            | BoundVectorOp::LimitRatio { .. }
            | BoundVectorOp::Sort(_) => {
                duplicates_canonical_labels(input, source_tags, source_metrics)
            }
            BoundVectorOp::Absent { .. }
            | BoundVectorOp::Histogram(_)
            | BoundVectorOp::CountValues { .. } => false,
        },
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            let vector = |side: &NativeNode| side.output.samples().is_some();
            match (vector(left), vector(right)) {
                (true, true) => false,
                // Arithmetic and `bool` broadcasts drop the metadata labels; a filtering comparison
                // keeps them.
                (true, false) | (false, true) => {
                    let carried = if vector(left) { left } else { right };
                    let drops_metadata = !matching.op.is_comparison() || matching.return_bool;
                    (drops_metadata
                        && !metadata_labels_keep_series_apart(carried, source_tags, source_metrics))
                        || duplicates_canonical_labels(carried, source_tags, source_metrics)
                }
                (false, false) => false,
            }
        }
        NativeExpr::Set { .. } => false,
        NativeExpr::Info { .. } => false,
    }
}

/// Whether `op`, applied in written order to `input`'s series, writes a label they already carry.
///
/// A `__name__` destination always counts as carried: even after a kernel dropped the name, the
/// scan below may hold several metrics whose ordinary labels are equal. `None` when the input's
/// labels are not derivable.
fn map_writes_existing_label(
    input: &NativeNode,
    op: &crate::native::plan::BoundMap,
    source_tags: &[Vec<String>],
) -> Option<bool> {
    let mut tags = fused_output_tags(input, source_tags)?;
    let mut existing = false;
    for operation in op.ops.iter() {
        if let MapOp::Label(rewrite) = operation {
            let destination = rewrite.destination();
            if destination == METRIC_NAME || tags.iter().any(|tag| tag == destination) {
                existing = true;
            }
            if rewrite.writes_value() && !tags.iter().any(|tag| tag == destination) {
                tags.push(destination.to_string());
            }
        }
    }
    Some(existing)
}

/// Whether dropping [`PROMQL_METADATA_LABELS`] from `node`'s series keeps them distinct.
///
/// - If `node` reports `__type__` or `__unit__`, nothing is proved.
/// - If `node` still reports `__name__`, it is proved when every series carries the same name
///   ([`single_metric_name`]).
/// - If the name is already gone, it is proved when `node` itself answers one series per label set
///   ([`answers_one_series_per_label_set`]), or when every source below reads the same one metric
///   ([`single_source_metric`]), since a drop on the way may have folded several metrics.
///
/// `false` when `node`'s labels are not derivable. [`duplicates_canonical_labels`] and
/// [`unifies_its_own_contributions`] both use this, so planning and runtime agree on a merge.
///
/// A key column holding `NULL` in one series and `''` in another is deliberately outside the proof:
/// every metric-engine tag column is nullable, so counting it would put a merge over every
/// name-dropping statement. [`leaves_a_drop_to_the_storage_keys`] reports those statements instead.
fn metadata_labels_keep_series_apart(
    node: &NativeNode,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
) -> bool {
    let Some(tags) = fused_output_tags(node, source_tags) else {
        return false;
    };
    if tags
        .iter()
        .any(|tag| tag == "__type__" || tag == "__unit__")
    {
        return false;
    }
    if !tags.iter().any(|tag| tag == METRIC_NAME) {
        // e.g. `1 - a / b`: the vector match already filed its rows by result label set.
        if answers_one_series_per_label_set(node) {
            return true;
        }
        return single_source_metric(node, source_metrics).is_some();
    }
    single_metric_name(node, source_metrics).is_some()
}

/// Whether `node`'s kernel files or folds its rows by result label set, so its series are distinct
/// whatever its inputs held.
///
/// [`NativeExpr::Merge`] is not one: its canonical label set includes the name, so a drop above it
/// can still fold `a{job="x"}` and `b{job="x"}`.
fn answers_one_series_per_label_set(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Aggregate { .. }
        | NativeExpr::Set { .. }
        | NativeExpr::Info { .. }
        | NativeExpr::ParameterizedHistogram { .. } => true,
        // Only vector-vector matching; a scalar broadcast passes series through.
        NativeExpr::Binary { left, right, .. } => {
            left.output.samples().is_some() && right.output.samples().is_some()
        }
        NativeExpr::Vector { op, .. } => matches!(
            op,
            BoundVectorOp::Absent { .. }
                | BoundVectorOp::Histogram(_)
                | BoundVectorOp::CountValues { .. }
        ),
        _ => false,
    }
}

/// Whether a [`NativeExpr::Merge`] sits at or below `node` on its sample-carrying path.
///
/// A merge's output is unique under Prometheus' `label_set` comparison, which treats an empty value
/// as absent, so the `NULL`/`''` key pair cannot collide above it. Parameter children are skipped:
/// their series are not the ones `node` hands over.
fn canonical_below(node: &NativeNode) -> bool {
    matches!(node.expr, NativeExpr::Merge { .. })
        || node
            .expr
            .children()
            .into_iter()
            .filter(|child| matches!(child.output, NativeType::Samples(_)))
            .any(canonical_below)
}

/// Whether `node` or a descendant drops [`PROMQL_METADATA_LABELS`] where the labels prove the drop
/// ([`metadata_labels_keep_series_apart`]) but the storage keys may not: `keys_survive` is `false`
/// and no merge sits below the drop ([`canonical_below`]).
fn drops_metadata_labels_over_one_metric(
    node: &NativeNode,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
    keys_survive: bool,
) -> bool {
    let decided_by_the_keys = |carried: &NativeNode| {
        !keys_survive
            && metadata_labels_keep_series_apart(carried, source_tags, source_metrics)
            && !canonical_below(carried)
    };
    let dropped = match &node.expr {
        NativeExpr::Instant { input, op } => {
            op.value == InstantValue::Timestamp && decided_by_the_keys(input)
        }
        NativeExpr::Timestamp { input } => decided_by_the_keys(input),
        NativeExpr::Window { input, op } => {
            op.function.drops_metric_name() && decided_by_the_keys(input)
        }
        NativeExpr::ParameterizedMap { input, .. } => decided_by_the_keys(input),
        NativeExpr::Map { input, op } => {
            op.ops
                .iter()
                .any(|operation| matches!(operation, MapOp::Math(_) | MapOp::Histogram(_)))
                && decided_by_the_keys(input)
        }
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            let carried = match (
                left.output.samples().is_some(),
                right.output.samples().is_some(),
            ) {
                (true, false) => Some(left.as_ref()),
                (false, true) => Some(right.as_ref()),
                _ => None,
            };
            carried.is_some_and(|carried| {
                (!matching.op.is_comparison() || matching.return_bool)
                    && decided_by_the_keys(carried)
            })
        }
        _ => false,
    };
    dropped
        || node.expr.children().into_iter().any(|child| {
            drops_metadata_labels_over_one_metric(child, source_tags, source_metrics, keys_survive)
        })
}

/// Whether `tree` drops the metadata labels where the labels prove the drop but a key column that
/// can hold both `NULL` and `''` may not (`keys_survive` is `false`). Nothing is merged for that
/// case; the caller reports it.
pub fn leaves_a_drop_to_the_storage_keys(
    tree: &NativeTree,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
    keys_survive: bool,
) -> bool {
    drops_metadata_labels_over_one_metric(tree.root(), source_tags, source_metrics, keys_survive)
}

/// The one metric every source below `node` is bound to, or `None` when they are not all one.
fn single_source_metric(
    node: &NativeNode,
    source_metrics: &[Option<Arc<str>>],
) -> Option<Arc<str>> {
    let mut pending = vec![node];
    let mut metric: Option<&Arc<str>> = None;
    while let Some(node) = pending.pop() {
        match &node.expr {
            NativeExpr::Input(input) => {
                let bound = source_metrics.get(input.source_index)?.as_ref()?;
                match metric {
                    None => metric = Some(bound),
                    Some(seen) if seen == bound => {}
                    Some(_) => return None,
                }
            }
            NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {}
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::VectorOf { input }
            | NativeExpr::Merge { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. }
            | NativeExpr::ScalarOf { input } => pending.push(input),
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            }
            | NativeExpr::ParameterizedHistogram {
                input, parameters, ..
            } => {
                pending.push(input);
                pending.extend(parameters.iter());
            }
            NativeExpr::ParameterizedTopK {
                input, parameter, ..
            } => {
                pending.push(input);
                pending.push(parameter);
            }
            NativeExpr::ParameterizedAggregate {
                input, parameter, ..
            } => {
                pending.push(input);
                pending.push(parameter);
            }
            NativeExpr::Binary { left, right, .. }
            | NativeExpr::Set { left, right, .. }
            | NativeExpr::Info {
                base: left,
                info: right,
                ..
            } => {
                pending.push(left);
                pending.push(right);
            }
        }
    }
    metric.cloned()
}

/// The one metric name every series `node` hands over carries, followed only through nodes that
/// pass their input's label set on unchanged. `None` when this walk cannot say.
fn single_metric_name(node: &NativeNode, source_metrics: &[Option<Arc<str>>]) -> Option<Arc<str>> {
    match &node.expr {
        NativeExpr::Input(input) => source_metrics.get(input.source_index).cloned().flatten(),
        NativeExpr::Merge { input }
        | NativeExpr::ParameterizedTopK { input, .. }
        | NativeExpr::Vector {
            input,
            op:
                BoundVectorOp::TopK { .. }
                | BoundVectorOp::LimitK { .. }
                | BoundVectorOp::LimitRatio { .. }
                | BoundVectorOp::Sort(_),
        } => single_metric_name(input, source_metrics),
        NativeExpr::Instant { input, op } if op.value != InstantValue::Timestamp => {
            single_metric_name(input, source_metrics)
        }
        NativeExpr::Window { input, op } if !op.function.drops_metric_name() => {
            single_metric_name(input, source_metrics)
        }
        NativeExpr::Map { input, op }
            if op.ops.iter().all(|operation| {
                matches!(
                    operation,
                    MapOp::Label(rewrite) if rewrite.destination() != METRIC_NAME
                )
            }) =>
        {
            single_metric_name(input, source_metrics)
        }
        _ => None,
    }
}

/// Whether `node` reads the whole input vector at a point rather than one series at a time.
///
/// Aggregates, vector-vector binaries, sets, `info`, vector operations, `scalar()`, per-point
/// `topk`/histogram functions and `Merge` do; so does a window over an input that may hand over
/// fragments of one label set. A region-side placement and the native route's admission both ask
/// this and must agree, so it passes no source facts to [`unifies_its_own_contributions`].
pub fn reads_across_series(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => false,
        NativeExpr::Window { input, .. } => {
            matches!(unifies_its_own_contributions(input, &[], &[]), Some(false))
                || reads_across_series(input)
        }
        NativeExpr::Instant { input, .. }
        | NativeExpr::VectorOf { input }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input } => reads_across_series(input),
        // e.g. `clamp_min(b, scalar(a))` reads across series through its parameter.
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => reads_across_series(input) || parameters.iter().any(reads_across_series),
        NativeExpr::ScalarOf { .. } => true,
        NativeExpr::Binary { left, right, .. } => {
            let scalar = |side: &NativeNode| matches!(side.output, NativeType::Scalar(_));
            if scalar(left) || scalar(right) {
                reads_across_series(left) || reads_across_series(right)
            } else {
                true
            }
        }
        NativeExpr::ParameterizedTopK { .. } | NativeExpr::ParameterizedHistogram { .. } => true,
        NativeExpr::Aggregate { .. }
        | NativeExpr::ParameterizedAggregate { .. }
        | NativeExpr::Vector { .. }
        | NativeExpr::Set { .. }
        | NativeExpr::Info { .. }
        | NativeExpr::Merge { .. } => true,
    }
}

/// The evaluated vector a [`NativeExpr::Merge`] child hands over, or `None` if it is not one.
fn merge_shape_supported(node: &NativeNode) -> Option<(&Arc<SeriesSchema>, &Arc<EvalGrid>)> {
    evaluated_vector(&node.output)
}

/// A validated bound evaluation tree. It carries no run-time state, so one tree serves every
/// partition.
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq, Hash)]
pub struct NativeTree {
    root: NativeNode,
}

impl NativeTree {
    /// Creates a tree over `root`. Errors when [`NativeNode::validate`] rejects it, which is a
    /// compiler defect.
    pub fn try_new(root: NativeNode) -> Result<Self> {
        root.validate()?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &NativeNode {
        &self.root
    }
}

impl<'de> serde::Deserialize<'de> for NativeNode {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
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
        wire.root.validate().map_err(serde::de::Error::custom)?;
        Ok(Self { root: wire.root })
    }
}

/// Whether the native runtime builds `tree`.
///
/// The native route asks this before committing a statement, because a refusal raised during
/// physical planning cannot fall back. It mirrors what `RuntimeNode::build` and
/// `NativeExecutionState::try_new` refuse beyond [`NativeNode::validate`]:
///
/// - a root of raw samples;
/// - an interleaved input leaf;
/// - `scalar()`, `topk`, `bottomk`, `limitk`, `sort` or `sort_desc` over more than one field;
/// - a binary of two literals, a scalar broadcast with vector-matching modifiers, or `bool` on
///   arithmetic;
/// - a `quantile` aggregate at any stage but `Complete`;
/// - an aggregate below `Final`, or a `topk`/`bottomk`/`limitk`/`count_values`, whose grouping names
///   a value field, and a `count_values` with an empty label;
/// - an `anchored` or `smoothed` window over evaluated input, over a function its kernel does not
///   correct, or with a negative lookback;
/// - a window whose `@` fold offset or `predict_linear` instants overflow `i64`.
///
/// A memory reservation refusal is left to the runtime.
pub fn native_tree_supported(tree: &NativeTree) -> bool {
    let root_supported = match &tree.root().output {
        NativeType::Scalar(_) | NativeType::AggregateState(_) => true,
        NativeType::Samples(schema) => schema.eval_grid.is_some(),
    };
    root_supported && native_node_supported(tree.root())
}

fn native_node_supported(node: &NativeNode) -> bool {
    stacker::maybe_grow(VALIDATE_STACK_RED_ZONE, VALIDATE_STACK_GROWTH, || {
        native_node_supported_within_stack(node)
    })
}

fn native_node_supported_within_stack(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(bound) => bound.ordering != SeriesOrdering::Interleaved,
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => true,
        NativeExpr::VectorOf { input } | NativeExpr::Instant { input, .. } => {
            native_node_supported(input)
        }
        NativeExpr::Merge { input } | NativeExpr::Map { input, .. } => native_node_supported(input),
        NativeExpr::Timestamp { input } => native_node_supported(input),
        NativeExpr::Window { input, op } => {
            native_node_supported(input)
                && window_modifier_supported(input, op)
                && window_instants_fit(op)
        }
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => native_node_supported(input) && parameters.iter().all(native_node_supported),
        NativeExpr::ParameterizedHistogram {
            input, parameters, ..
        } => native_node_supported(input) && parameters.iter().all(native_node_supported),
        NativeExpr::Aggregate { input, op } => {
            native_node_supported(input) && aggregate_supported(node, input, op)
        }
        NativeExpr::ParameterizedAggregate {
            input,
            op,
            parameter,
        } => {
            native_node_supported(input)
                && native_node_supported(parameter)
                // A literal hands over no chunk to read φ from; `validate` refuses it too.
                && !matches!(parameter.expr, NativeExpr::Scalar(_))
                && aggregate_supported(node, input, op)
        }
        NativeExpr::Vector { input, op } => {
            native_node_supported(input)
                && node
                    .output
                    .samples()
                    .is_some_and(|schema| vector_op_supported(schema, op))
        }
        NativeExpr::ParameterizedTopK {
            input,
            op,
            parameter,
        } => {
            native_node_supported(input)
                && native_node_supported(parameter)
                && node.output.samples().is_some_and(|schema| {
                    schema.fields.len() == 1 && vector_op_supported(schema, op)
                })
        }
        NativeExpr::ScalarOf { input } => {
            native_node_supported(input)
                && input
                    .output
                    .samples()
                    .is_some_and(|schema| schema.fields.len() == 1)
        }
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            native_node_supported(left)
                && native_node_supported(right)
                && binary_supported(left, right, matching)
        }
        NativeExpr::Set { left, right, .. } => {
            native_node_supported(left) && native_node_supported(right)
        }
        NativeExpr::Info { base, info, .. } => {
            native_node_supported(base) && native_node_supported(info)
        }
    }
}

/// Two evaluated vectors are always supported. A broadcast must have default matching and no `bool`
/// on arithmetic, and two scalars must not both be literals.
fn binary_supported(left: &NativeNode, right: &NativeNode, matching: &BinaryMatching) -> bool {
    let evaluated = |node: &NativeNode| {
        node.output
            .samples()
            .is_some_and(|schema| schema.eval_grid.is_some())
    };
    if evaluated(left) && evaluated(right) {
        return true;
    }
    let literal = |node: &NativeNode| matches!(&node.expr, NativeExpr::Scalar(_));
    let broadcastable = match (&left.output, &right.output) {
        (NativeType::Samples(vector), NativeType::Scalar(_))
        | (NativeType::Scalar(_), NativeType::Samples(vector)) => vector.eval_grid.is_some(),
        (NativeType::Scalar(_), NativeType::Scalar(_)) => !(literal(left) && literal(right)),
        _ => false,
    };
    broadcastable
        && matching.labels == MatchingLabels::Ignoring(Vec::new())
        && matching.cardinality == Cardinality::OneToOne
        && matching.fill == FillValues::default()
        && !(!matching.op.is_comparison() && matching.return_bool)
}

/// Only `Complete` and `Partial` are checked against the schema; `Final` merges state that was
/// already grouped.
fn aggregate_supported(node: &NativeNode, input: &NativeNode, op: &BoundAggregate) -> bool {
    match op.kind {
        AggregateKind::Sum
        | AggregateKind::Count
        | AggregateKind::Avg
        | AggregateKind::Min
        | AggregateKind::Max
        | AggregateKind::Group
        | AggregateKind::Stddev
        | AggregateKind::Stdvar => {}
        AggregateKind::Quantile { .. } => {
            // A quantile keeps no partial state.
            if op.stage != AggregateStage::Complete {
                return false;
            }
        }
    }
    if op.stage == AggregateStage::Final {
        return true;
    }
    node.output
        .samples()
        .or_else(|| input.output.samples())
        .is_some_and(|schema| grouping_supported(schema, &op.group))
}

fn vector_op_supported(schema: &SeriesSchema, op: &BoundVectorOp) -> bool {
    let one_field = matches!(
        op,
        BoundVectorOp::TopK { .. }
            | BoundVectorOp::LimitK { .. }
            | BoundVectorOp::Sort(SortOrder::Asc | SortOrder::Desc)
    );
    if one_field && schema.fields.len() != 1 {
        return false;
    }
    match op {
        BoundVectorOp::TopK { group, .. } | BoundVectorOp::LimitK { group, .. } => {
            grouping_supported(schema, group)
        }
        BoundVectorOp::CountValues { label, group } => {
            let names = group_names(group);
            crate::aggregate::validate_count_values(
                schema,
                label.as_ref(),
                kernel_grouping(group, &names),
            )
            .is_ok()
        }
        BoundVectorOp::LimitRatio { .. }
        | BoundVectorOp::Sort(_)
        | BoundVectorOp::Absent { .. }
        | BoundVectorOp::Histogram(_) => true,
    }
}

/// The grouping check `FloatAggregator::try_new_budgeted` and `VectorRuntime::try_new` run.
fn grouping_supported(schema: &SeriesSchema, group: &Grouping) -> bool {
    let names = group_names(group);
    crate::aggregate::validate_vector_grouping(schema, kernel_grouping(group, &names)).is_ok()
}

fn group_names(group: &Grouping) -> Vec<&str> {
    match group {
        Grouping::By(names) | Grouping::Without(names) => names.iter().map(AsRef::as_ref).collect(),
    }
}

fn kernel_grouping<'a>(group: &Grouping, names: &'a [&'a str]) -> crate::aggregate::Grouping<'a> {
    match group {
        Grouping::By(_) => crate::aggregate::Grouping::By(names),
        Grouping::Without(_) => crate::aggregate::Grouping::Without(names),
    }
}

/// The [`check_modifier`](crate::range_eval::check_modifier) rule
/// [`RangeEvaluator::try_new`](crate::range_eval::RangeEvaluator::try_new) applies.
fn window_modifier_supported(input: &NativeNode, op: &BoundWindow) -> bool {
    let Some(modifier) = op.window.modifier else {
        return true;
    };
    input.output.samples().is_some_and(|schema| {
        crate::range_eval::check_modifier(schema, modifier, op.function).is_ok()
    })
}

/// Whether the `@` fold offset and `predict_linear` instants fit in `i64`, asked through the same
/// functions the runtime calls.
fn window_instants_fit(op: &BoundWindow) -> bool {
    let grid = op.output_grid.spec();
    let Ok(fold_offset) = fold_offset(grid, op.window) else {
        return false;
    };
    match op.function {
        RangeFunction::PredictLinear(_) => predict_linear_instants(grid, fold_offset).is_ok(),
        _ => true,
    }
}

/// Stack reserve before a tree walk grows its stack. Trees have no depth limit, and in a debug build
/// an 80-term chain needed about 1.9 MiB, over a test thread's 2 MiB.
const VALIDATE_STACK_RED_ZONE: usize = 256 * 1024;

const VALIDATE_STACK_GROWTH: usize = 4 * 1024 * 1024;

impl NativeNode {
    /// Checks this node and its inputs against the contract of its operation.
    pub fn validate(&self) -> Result<()> {
        stacker::maybe_grow(VALIDATE_STACK_RED_ZONE, VALIDATE_STACK_GROWTH, || {
            self.validate_within_stack()
        })
    }

    fn validate_within_stack(&self) -> Result<()> {
        match &self.expr {
            NativeExpr::Input(input) => {
                if !self.output.same_binding(&input.output) {
                    return Err(invalid(
                        "an input node's output is not the binding it was bound to",
                    ));
                }
            }
            NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {
                if !matches!(self.output, NativeType::Scalar(_)) {
                    return Err(invalid("a scalar node does not produce a scalar"));
                }
            }
            NativeExpr::ScalarOf { input } => {
                input.validate()?;
                let Some((_, input_grid)) = evaluated_vector(&input.output) else {
                    return Err(invalid("scalar() consumes an evaluated vector"));
                };
                let NativeType::Scalar(output_grid) = &self.output else {
                    return Err(invalid("scalar() does not produce a scalar"));
                };
                if output_grid.spec() != input_grid.spec() {
                    return Err(invalid("scalar() output is not on its input's grid"));
                }
            }
            NativeExpr::VectorOf { input } => {
                input.validate()?;
                let NativeType::Scalar(input_grid) = &input.output else {
                    return Err(invalid("vector() consumes a scalar"));
                };
                ensure_samples(&self.output, "vector()")?;
                if !self.output.samples().is_some_and(|s| s.fields.len() == 1) {
                    return Err(invalid("vector() produces one field"));
                }
                ensure_grid(&self.output, input_grid, "vector()")?;
            }
            NativeExpr::Instant { input, op } => {
                input.validate()?;
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
                ensure_same_fields(&input.output, &self.output, "an instant selection")?;
                ensure_grid(&self.output, &op.output_grid, "an instant selection")?;
            }
            NativeExpr::Merge { input } => {
                input.validate()?;
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
                let NativeType::Samples(schema) = &input.output else {
                    return Err(invalid(&format!(
                        "a window consumes samples, not {}",
                        input.output.describe()
                    )));
                };
                if op.window.range_ms <= 0 {
                    return Err(invalid(
                        "a window reads an empty range: its range must be positive",
                    ));
                }
                ensure_not_interleaved(input)?;
                // That a subquery's input grid covers the read range is the compiler's to ensure.
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
                let input_grid = if op.takes_a_scalar_input() {
                    let NativeType::Scalar(input_grid) = &input.output else {
                        return Err(invalid(&format!(
                            "{op:?} consumes a scalar, not {}",
                            input.output.describe()
                        )));
                    };
                    Arc::clone(input_grid)
                } else {
                    let Some((_, input_grid)) = evaluated_vector(&input.output) else {
                        return Err(invalid(&format!(
                            "a map consumes an evaluated vector, not {}",
                            input.output.describe()
                        )));
                    };
                    Arc::clone(input_grid)
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
                if op.takes_a_scalar_input() {
                    if !matches!(&self.output, NativeType::Scalar(grid) if grid.spec() == input_grid.spec())
                    {
                        return Err(invalid(
                            "a map over a scalar does not answer a scalar on its input's grid",
                        ));
                    }
                } else {
                    ensure_samples(&self.output, "a parameterized map")?;
                    ensure_same_fields(&input.output, &self.output, "a parameterized map")?;
                    ensure_grid(&self.output, &input_grid, "a parameterized map")?;
                }
            }
            NativeExpr::ParameterizedTopK {
                input,
                op,
                parameter,
            } => {
                input.validate()?;
                parameter.validate()?;
                if !matches!(op, BoundVectorOp::TopK { .. }) {
                    return Err(invalid("a parameterized topk operation is not TopK"));
                }
                let Some((_, output_grid)) = evaluated_vector(&self.output) else {
                    return Err(invalid(
                        "a parameterized topk does not produce an evaluated vector",
                    ));
                };
                if !matches!(&parameter.output, NativeType::Scalar(grid) if grid.spec() == output_grid.spec())
                {
                    return Err(invalid(
                        "a topk parameter is not a scalar on its output grid",
                    ));
                }
                let Some((_, input_grid)) = evaluated_vector(&input.output) else {
                    return Err(invalid("a parameterized topk consumes a non-vector"));
                };
                ensure_same_fields(&input.output, &self.output, "a vector operation")?;
                ensure_grid(&input.output, input_grid, "a vector operation")?;
                ensure_grid(&self.output, input_grid, "a vector operation")?;
            }
            NativeExpr::ParameterizedHistogram {
                input,
                op,
                parameters,
            } => {
                input.validate()?;
                if !matches!(op, BoundVectorOp::Histogram(_)) {
                    return Err(invalid(
                        "a parameterized histogram operation is not Histogram",
                    ));
                }
                let expected = match op {
                    BoundVectorOp::Histogram(HistogramFunction::Quantile(_)) => 1,
                    BoundVectorOp::Histogram(HistogramFunction::Fraction { .. }) => 2,
                    _ => {
                        return Err(invalid(
                            "parameterized histogram operation is not Histogram",
                        ));
                    }
                };
                if parameters.len() != expected {
                    return Err(invalid("parameterized histogram has the wrong arity"));
                }
                let Some((_, input_grid)) = evaluated_vector(&input.output) else {
                    return Err(invalid("a parameterized histogram consumes a non-vector"));
                };
                for parameter in parameters {
                    parameter.validate()?;
                    if !matches!(&parameter.output, NativeType::Scalar(grid) if grid.spec() == input_grid.spec())
                    {
                        return Err(invalid(
                            "a histogram parameter is not a scalar on its input grid",
                        ));
                    }
                }
                ensure_same_fields(&input.output, &self.output, "a vector operation")?;
                ensure_grid(&self.output, input_grid, "a vector operation")?;
            }
            NativeExpr::Vector { input, .. } => {
                input.validate()?;
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
                // `Partial` turns a vector into state and `Final` turns state back into a vector.
                match op.stage {
                    AggregateStage::Complete => {
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
                        let Some((expected_kind, expected_components)) =
                            aggregate_state_binding(op.kind, op.field_kind)
                        else {
                            return Err(DataFusionError::NotImplemented(format!(
                                "aggregate state is not defined for {:?}",
                                op.kind
                            )));
                        };
                        if state.kind != expected_kind
                            || state.components.as_ref() != expected_components
                        {
                            return Err(DataFusionError::NotImplemented(format!(
                                "partial {:?} aggregate requires state kind {:?} with components {:?}, found {:?} with {:?}",
                                op.kind,
                                expected_kind,
                                expected_components,
                                state.kind,
                                state.components
                            )));
                        }
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
                        let NativeType::AggregateState(state) = &input.output else {
                            return Err(invalid(&format!(
                                "a final aggregate consumes aggregate state, not {}",
                                input.output.describe()
                            )));
                        };
                        let Some((expected_kind, expected_components)) =
                            aggregate_state_binding(op.kind, op.field_kind)
                        else {
                            return Err(DataFusionError::NotImplemented(format!(
                                "aggregate state is not defined for {:?}",
                                op.kind
                            )));
                        };
                        if state.kind != expected_kind
                            || state.components.as_ref() != expected_components
                        {
                            return Err(DataFusionError::NotImplemented(format!(
                                "final {:?} aggregate requires state kind {:?} with components {:?}, found {:?} with {:?}",
                                op.kind,
                                expected_kind,
                                expected_components,
                                state.kind,
                                state.components
                            )));
                        }
                        if state.group != op.group {
                            return Err(invalid(
                                "aggregate state groups differ from the binding: state built for \
                                 one `by` or `without` cannot be merged into another",
                            ));
                        }
                        let output = ensure_samples(&self.output, "a final aggregate")?;
                        ensure_field_count(
                            output.fields.len(),
                            state.accumulators.len(),
                            "a final aggregate",
                        )?;
                        ensure_grid(&self.output, &state.grid, "a final aggregate")?;
                    }
                }
            }
            NativeExpr::ParameterizedAggregate {
                input,
                op,
                parameter,
            } => {
                input.validate()?;
                parameter.validate()?;
                // φ is read off the parameter's chunks, and a literal hands over none; the compiler
                // binds a constant φ on the `Aggregate` instead.
                if matches!(parameter.expr, NativeExpr::Scalar(_)) {
                    return Err(invalid(
                        "a parameterized aggregate reads a parameter node that hands over a \
                         scalar series, not a literal",
                    ));
                }
                if !matches!(op.kind, AggregateKind::Quantile { .. }) {
                    return Err(invalid(
                        "a parameterized aggregate that is not a `quantile`",
                    ));
                }
                if op.stage != AggregateStage::Complete {
                    return Err(invalid(
                        "a parameterized aggregate at a stage other than `Complete`",
                    ));
                }
                let Some((_, output_grid)) = evaluated_vector(&self.output) else {
                    return Err(invalid(
                        "a parameterized aggregate does not produce an evaluated vector",
                    ));
                };
                if !matches!(&parameter.output, NativeType::Scalar(grid) if grid.spec() == output_grid.spec())
                {
                    return Err(invalid(
                        "an aggregate parameter is not a scalar on its output grid",
                    ));
                }
                let Some((_, input_grid)) = evaluated_vector(&input.output) else {
                    return Err(invalid(&format!(
                        "a complete aggregate consumes an evaluated vector, not {}",
                        input.output.describe()
                    )));
                };
                ensure_same_fields(
                    &input.output,
                    &self.output,
                    "a parameterized complete aggregate",
                )?;
                ensure_grid(
                    &self.output,
                    input_grid,
                    "a parameterized complete aggregate",
                )?;
            }
            NativeExpr::Binary { left, right, .. } => {
                left.validate()?;
                right.validate()?;
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
                    (NativeType::Scalar(_), NativeType::Scalar(_)) => {
                        if !matches!(&self.output, NativeType::Scalar(grid) if grid.spec() == left_grid.spec())
                        {
                            return Err(invalid(
                                "two scalars do not produce a scalar on the statement's grid",
                            ));
                        }
                    }
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
                    // The match pairs fields by position, so it keeps the shorter side's count.
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
                    // Unreachable: state has no grid, so the checks above already returned.
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
                // `or` emits the right side's samples under the left side's fields, by position.
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
                // The kernel reads the info side's values as sample times, so it must be an
                // `InfoTimestamp` selection; a merge above it passes the times through unchanged.
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

/// Whether a scalar broadcast drops the metric name; mirrors `BroadcastRuntime::drops_labels` in
/// [`execute`](crate::native::execute).
fn broadcast_drops_labels(matching: &BinaryMatching) -> bool {
    !matching.op.is_comparison() || matching.return_bool
}

/// Whether `node` hands over one series per canonical label set.
///
/// - `Some(true)`: one series per label set; a consumer collects nothing.
/// - `Some(false)`: several series may share a label set; the `Window` runtime collects them
///   through `MergeInput`, and `limitk` merges its retained input.
/// - `None`: unknown. Only an [`NativeExpr::Input`] answers this, and pass-through nodes propagate
///   it; the runtime collectors treat it like `Some(true)`.
///
/// A metadata-label drop is judged by [`metadata_labels_keep_series_apart`] with the same
/// `source_tags` and `source_metrics` as [`duplicates_canonical_labels`], so this and the canonical
/// pass agree about a drop. [`reads_across_series`] passes empty slices and gets the conservative
/// answer.
#[allow(clippy::match_same_arms)] // Keep each expression's contribution guarantee explicit.
pub fn unifies_its_own_contributions(
    node: &NativeNode,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
) -> Option<bool> {
    match &node.expr {
        NativeExpr::Input(_) => None,
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } | NativeExpr::VectorOf { .. } => {
            Some(true)
        }
        // Raw samples arrive one whole storage series at a time.
        NativeExpr::Instant { input, op } => {
            if op.value == InstantValue::Timestamp {
                Some(metadata_labels_keep_series_apart(
                    input,
                    source_tags,
                    source_metrics,
                ))
            } else {
                Some(true)
            }
        }
        NativeExpr::Merge { .. } => Some(true),
        // Over a boundary the window folds each fragment on its own, so its output may alias.
        // Otherwise its input is unified or collected through `MergeInput`, so only a
        // name-dropping function can make two series collide.
        NativeExpr::Window { input, op } => {
            let child = unifies_its_own_contributions(input, source_tags, source_metrics);
            if child.is_none() {
                Some(false)
            } else if op.function.drops_metric_name() {
                Some(metadata_labels_keep_series_apart(
                    input,
                    source_tags,
                    source_metrics,
                ))
            } else {
                Some(true)
            }
        }
        NativeExpr::Map { .. } | NativeExpr::ParameterizedMap { .. } => Some(false),
        NativeExpr::ParameterizedTopK { input, .. } => {
            unifies_its_own_contributions(input, source_tags, source_metrics)
        }
        NativeExpr::ParameterizedHistogram { .. } => Some(true),
        NativeExpr::ScalarOf { .. } => Some(true),
        NativeExpr::Timestamp { input } => Some(metadata_labels_keep_series_apart(
            input,
            source_tags,
            source_metrics,
        )),
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            if evaluated_vector(&left.output).is_some() && evaluated_vector(&right.output).is_some()
            {
                Some(true)
            } else if broadcast_drops_labels(matching) {
                Some(false)
            } else {
                // A filtering comparison answers for the side that carries series, picked as
                // `broadcast_runtime` picks it: the vector, or the non-literal of two scalars.
                let carries_series = |node: &NativeNode| {
                    if node.output.samples().is_some() {
                        return true;
                    }
                    !matches!(node.expr, NativeExpr::Scalar(BoundScalar { .. }))
                };
                let vector = if carries_series(left) { left } else { right };
                unifies_its_own_contributions(vector, source_tags, source_metrics)
            }
        }
        NativeExpr::Set { .. } => Some(true),
        NativeExpr::Info { .. } => Some(true),
        NativeExpr::Aggregate { .. } | NativeExpr::ParameterizedAggregate { .. } => Some(true),
        // `limitk` collects its input's fragments at run time; `absent`, `histogram_*` and
        // `count_values` group by label set themselves.
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
                unifies_its_own_contributions(input, source_tags, source_metrics)
            }
        }
    }
}

/// The schema and grid of an evaluated vector, or `None` for raw samples, a scalar or state.
fn evaluated_vector(ty: &NativeType) -> Option<(&Arc<SeriesSchema>, &Arc<EvalGrid>)> {
    let schema = ty.samples().filter(|schema| schema.eval_grid.is_some())?;
    Some((schema, schema.eval_grid.as_ref()?))
}

/// Checks that a node declares as many fields as its input produces.
fn ensure_same_fields(input: &NativeType, output: &NativeType, what: &str) -> Result<()> {
    let (NativeType::Samples(input), NativeType::Samples(output)) = (input, output) else {
        return Ok(());
    };
    ensure_field_count(output.fields.len(), input.fields.len(), what)
}

fn ensure_field_count(declared: usize, expected: usize, what: &str) -> Result<()> {
    if declared != expected {
        return Err(invalid(&format!(
            "{what} declares {declared} field(s) but its input provides {expected}"
        )));
    }
    Ok(())
}

fn ensure_samples<'a>(ty: &'a NativeType, what: &str) -> Result<&'a Arc<SeriesSchema>> {
    ty.samples()
        .ok_or_else(|| invalid(&format!("{what} does not produce samples")))
}

/// Grids compare by [`EvalGrid::spec`], as the kernels do (`vector_match::same_grid`).
fn ensure_grid(ty: &NativeType, expected: &Arc<EvalGrid>, what: &str) -> Result<()> {
    match ty.grid() {
        Some(actual) if actual.spec() == expected.spec() => Ok(()),
        Some(_) => Err(invalid(&format!(
            "{what} declares a grid its output does not carry"
        ))),
        None => Err(invalid(&format!("{what} produces values without a grid"))),
    }
}

/// Instant selections and windows read one series to its last chunk before the next, so an input
/// leaf under them must not interleave.
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
    // A rejected tree is a compiler defect, not a mistake in the client's statement.
    DataFusionError::Internal(reason.to_string())
}

#[cfg(test)]
pub(crate) mod tests;

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
