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
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::select::SortOrder;
use crate::vector_match::{
    BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels, SetOp,
};

/// The bucket-bound label a classic histogram's series carry, which the histogram function folds and
/// its result does not report.
const LE_LABEL: &str = "le";

/// The label a metric name is stored under, which a scan binds beside its tag columns rather than
/// among them.
const METRIC_NAME: &str = "__name__";

/// Metadata labels the runtime removes from histogram result label sets.
const PROMQL_METADATA_LABELS: [&str; 3] = ["__name__", "__type__", "__unit__"];

/// The complete label set `node` reports at run time, from the labels each input reports and the
/// ones the nodes above it write.
///
/// This is the identity a [`NativeExpr::Merge`] groups by and a vector match reads, so the metric
/// name is part of it whenever the series carries one. The caller's `source_tags` are the complete
/// label sets of the sources, indexed by the `source_index` of the leaf that reads each: a scan
/// bound to a single metric names every series it hands over even though the name is not one of its
/// tag columns, so the canonical pass includes `__name__` in that source's entry. The query planner
/// derives the *output columns* with its own walk, which reads the same algebra from the tag columns
/// each source declares; the two have to agree on where a kernel drops the name, which is why this
/// walk drops it exactly where the runtime does.
///
/// They follow Prometheus' result labels: a one-to-one match keeps the left side's labels, narrowed
/// by `on` or `ignoring`; `and` and `unless` keep the left side's series and `or` adds the right
/// side's; `info` adds the data labels of the info table; an operation with a scalar or a function
/// of each value keeps its operand's; a scalar and `vector(s)` have none; `count_values` keeps its
/// grouping and the label it writes; a fill that answers a missing many-side sample files the one
/// side's matching labels, so they join the set, while a grouped `include` label joins them only when
/// the one side carries it or the matching keeps it in the key a fill reads. A label rewrite writes
/// its destination, and a destination that names the metric name writes that name.
///
/// A node that reports a value of its own drops the name where its kernel does - `timestamp`, a
/// range function whose result is computed (`rate`, `increase`, ...), a math or histogram map,
/// `histogram_quantile`, every non-filtering binary - so a name rewrite below one of them cannot
/// make the name reappear above it.
///
/// `None` for a node whose labels this walk does not derive - a `count_values` whose destination is
/// empty. `None` proves nothing, so the caller treats it as "may alias" rather than as "distinct".
pub fn fused_output_tags(node: &NativeNode, source_tags: &[Vec<String>]) -> Option<Vec<String>> {
    match &node.expr {
        NativeExpr::Input(input) => source_tags.get(input.source_index).cloned(),
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } | NativeExpr::VectorOf { .. } => {
            Some(Vec::new())
        }
        // These report a value of their own rather than the sample they read, and the runtime drops
        // the name for each of them. `source_tags` is the authority on what an input carries, so a
        // discovery input arrives carrying `__name__` and these arms are what decides it does not
        // survive.
        NativeExpr::Instant { input, op } => {
            let tags = fused_output_tags(input, source_tags)?;
            Some(match op.value {
                InstantValue::Timestamp => without_metric_name(tags),
                InstantValue::Sample | InstantValue::InfoTimestamp => tags,
            })
        }
        NativeExpr::Window { input, op } => {
            let tags = fused_output_tags(input, source_tags)?;
            Some(if op.function.drops_metric_name() {
                without_metric_name(tags)
            } else {
                tags
            })
        }
        NativeExpr::Timestamp { input } => {
            Some(without_metric_name(fused_output_tags(input, source_tags)?))
        }
        // A label rewrite changes the label set itself: `label_replace` adds its destination label
        // and an empty replacement removes it, so the input's labels are not the output's. A math or
        // histogram kernel drops the metric name where it runs, and the ops apply in written order,
        // so what follows a rewrite sees the set the rewrite left.
        NativeExpr::Map { input, op } => {
            let mut tags = fused_output_tags(input, source_tags)?;
            for operation in op.ops.iter() {
                match operation {
                    MapOp::Label(rewrite) => {
                        let destination = rewrite.destination();
                        if tags.iter().any(|tag| tag == destination) {
                            // The input carries the label, so the rewrite changes its value - and an
                            // empty replacement may remove it, which the column still represents.
                            tags.retain(|tag| tag != destination);
                            tags.push(destination.to_string());
                        } else if rewrite.writes_value() {
                            // A fresh destination is a label the output only carries when the
                            // rewrite can write a value; a delete-only rewrite of a label no input
                            // carried publishes nothing.
                            tags.push(destination.to_string());
                        }
                    }
                    MapOp::Math(_) | MapOp::Histogram(_) => tags = without_metric_name(tags),
                }
            }
            Some(tags)
        }
        // Its parameters are scalars, which carry no labels; the map computes a value of its own
        // with the input, and its kernel drops the metric name.
        NativeExpr::ParameterizedMap { input, .. } => {
            Some(without_metric_name(fused_output_tags(input, source_tags)?))
        }
        // `scalar()` reports one value and no labels, like the scalar it produces.
        NativeExpr::ScalarOf { .. } => Some(Vec::new()),
        // Its parameter is a scalar, which carries no labels, and ranking keeps each selected
        // series' own labels.
        NativeExpr::ParameterizedTopK { input, .. } => fused_output_tags(input, source_tags),
        // A parameterized histogram function folds the bucket series that share the other labels
        // into one series and drops `le` along with the metadata labels, exactly as its bound
        // counterpart does. Its parameters are scalars and carry no labels.
        NativeExpr::ParameterizedHistogram { input, .. } => {
            let mut tags = fused_output_tags(input, source_tags)?;
            tags.retain(|tag| tag != LE_LABEL && !PROMQL_METADATA_LABELS.contains(&tag.as_str()));
            Some(tags)
        }
        // An aggregate reports the grouping labels its input carries: `by(...)` keeps exactly the
        // ones it names, and `without(...)` keeps the rest and drops the metric name, as Prometheus
        // does. A grouping by `__name__` keeps the name where the input reports one; a named scan
        // only reports it among the runtime labels, so the aggregate keeps it there and publishes no
        // column for it, which is what `sum by (__name__)` over one metric reports.
        NativeExpr::Aggregate { input, op } => {
            let input_tags = fused_output_tags(input, source_tags)?;
            Some(match &op.group {
                Grouping::By(kept) => input_tags
                    .into_iter()
                    .filter(|tag| kept.iter().any(|name| name.as_ref() == tag.as_str()))
                    .collect(),
                Grouping::Without(dropped) => input_tags
                    .into_iter()
                    .filter(|tag| {
                        tag != METRIC_NAME
                            && !dropped.iter().any(|name| name.as_ref() == tag.as_str())
                    })
                    .collect(),
            })
        }
        // Selecting some of the input's series keeps each one's own labels, and `count_values`
        // keeps its grouping's; `histogram` folds bucket series into one histogram, so it does not
        // report the input's set.
        NativeExpr::Vector { input, op } => match op {
            BoundVectorOp::TopK { .. }
            | BoundVectorOp::LimitK { .. }
            | BoundVectorOp::LimitRatio { .. }
            | BoundVectorOp::Sort(_) => fused_output_tags(input, source_tags),
            // `absent` reports the labels its own matchers name, not the input's set and not the
            // table's columns: it answers one series carrying exactly those, so a label the input
            // does not carry is still a label of the result.
            BoundVectorOp::Absent { labels } => Some(
                labels
                    .tags()
                    .iter()
                    .map(|label| label.name.to_string())
                    .collect(),
            ),
            // `count_values` writes the value it counts into its own label and then keeps its
            // grouping: `by(...)` keeps the labels named plus that destination, `without(...)`
            // keeps the rest minus the labels named and the metric name. What the node reports is
            // therefore the grouping applied to the labels its input carries, with the destination
            // among them - not the input's set, and not the scan's, since the grouping narrows both.
            //
            // The destination is a label the node creates, so a rewrite of it above this node is a
            // rewrite of an existing label. A destination naming the metric name writes the name
            // the node groups by, which is the one label its result carries. This walk derives the
            // labels; it does not decide which groupings the kernel accepts, and those refusals stay
            // where they are.
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
                        // `by` keeps the label the node writes whether the input carried it or not.
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
            // A classic histogram function folds the bucket series that share the other labels into
            // one series and drops `le`, along with the metadata labels the runtime does not put on
            // its result. What it keeps is everything else its input carried.
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
                    // One-to-one matching narrows the left side's labels with `on`/`ignoring`.
                    // A group retains the many side's labels and copies the named labels from the one
                    // side: `group_left(container)` keeps the left labels and adds `container` from
                    // the right.
                    let mut tags = match &matching.cardinality {
                        Cardinality::OneToOne => kept(left_tags.clone()),
                        Cardinality::ManyToOne(_) => left_tags.clone(),
                        Cardinality::OneToMany(_) => right_tags.clone(),
                    };
                    // `result_metric` drops the metadata labels before it matches or copies an
                    // included label, so an arithmetic result starts without the name. An include can
                    // still put it back from the one side, which is why the final rule below does not
                    // run on this branch.
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
                    // A grouped result carries an include label only when a row can give it one.
                    // `result_metric` removes the label from the many side's labels unless the one side
                    // has it, so a paired row and a row filed from a one-side series take it from the
                    // one series, which is what the loop above declares. A row filed from a many-side
                    // series takes it from that series' match key instead, so it carries the label only
                    // when the matching keeps the label in the key. Everything else the many side
                    // carries is the base above, so a label neither of those can provide has to leave
                    // the set: nothing would ever fill its column.
                    if !includes.is_empty() {
                        let many_rows_filled = match &matching.cardinality {
                            // `group_right` puts the one side on the left, so the fills trade places:
                            // `fill_left` is the one that answers a missing one-side sample there.
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
                    // A fill answers a missing sample from the series of the other side, and the kernel
                    // files that row at the label set the series it has gives it
                    // (`vector_match::binary_field`): a many-side series keeps all of its labels, which
                    // `tags` already holds, while a one-side series keeps only its matching labels plus
                    // the group labels the modifier names, which the loop above adds. So what a fill can
                    // add here is the one side's matching labels, and only a fill that can file a
                    // one-side row has any to add: one where the many side is the missing side. Which
                    // fill that is follows the kernel, which trades the two for `group_right`, whose one
                    // side is the left - there it is `fill_right` that leaves the many side missing.
                    let (one_tags, fills_many_side) = match &matching.cardinality {
                        Cardinality::OneToMany(_) => (&left_tags, matching.fill.rhs),
                        Cardinality::ManyToOne(_) | Cardinality::OneToOne => {
                            (&right_tags, matching.fill.lhs)
                        }
                    };
                    // A filled row keeps only the one side's matching labels, so the name can reach
                    // it only through an `on` key that names the metric name, and only while the
                    // operation reports its samples' names.
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
                without_metric_name(tags)
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
            }
            Some(tags)
        }
        // A base series keeps its labels and takes the data labels of its info series: the ones the
        // selector names, or all of them when it names none.
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
/// Whether a binary operation reports the samples of the side it keeps, name included.
///
/// A filtering comparison reports those samples, so the name survives it. Every other binary
/// operation computes new values, and `bool` reports the comparison's own outcome rather than
/// either operand's samples, so both drop the name.
fn keeps_metric_name(matching: &BinaryMatching) -> bool {
    matches!(
        matching.op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Ge | BinaryOp::Le
    ) && !matching.return_bool
}

/// The labels a node reports after the metric name is dropped, which is what a kernel that computes
/// a value of its own does to the samples it reports.
fn without_metric_name(tags: Vec<String>) -> Vec<String> {
    tags.into_iter().filter(|tag| tag != METRIC_NAME).collect()
}

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

/// The [`FieldKind`] every referenced field of a scan carries, or `None` when they disagree or one
/// is a type this route does not answer.
///
/// The route declares one output column per field, so a statement whose selectors carry different
/// kinds has no column to declare them in and keeps the existing plan. `Float64` and the canonical
/// native histogram struct are the two kinds this route answers.
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
            // An unsupported pair emits no sample, not a float. Fill values are floats, so one
            // field can be produced by the real pair and by either enabled synthetic pair.
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
        // Comparison and bool behavior is separate from arithmetic's empty-field layout.
        _ if left == right => Some(left),
        _ => None,
    }
}

impl NativeNode {
    /// The kind this node's output field carries, given the kind its sources carry.
    ///
    /// `None` means this walk cannot name it: either the caller has no kind for the statement's
    /// sources and this tree reads one, or the shape's samples are ones the model does not describe.
    /// Both keep the existing plan rather than declaring a column the first batch might not fill.
    pub fn output_field_kind(&self, input: Option<FieldKind>) -> Option<FieldKind> {
        let kind_of = |node: &NativeNode| node.output_field_kind(input);
        match &self.expr {
            NativeExpr::Input(_) => input,
            // A tree that reads no source answers floats: a constant, the evaluation time, `vector()`
            // over one of them, and a date function of the evaluation time. None of them carries a
            // sample kind of its own, so none of them needs the source kind either.
            NativeExpr::Scalar(_)
            | NativeExpr::EvalTime { .. }
            | NativeExpr::VectorOf { .. }
            | NativeExpr::Timestamp { .. } => Some(FieldKind::Float),
            // The window functions that answer with a histogram are the ones the range kernel says
            // yield one; every other range function reduces its window to a float.
            NativeExpr::Window { input, op } => {
                let inner = kind_of(input)?;
                (!op.function.yields_histograms())
                    .then_some(FieldKind::Float)
                    .or(Some(inner))
            }
            // An instant selection reads one sample of the field, so it keeps the kind; the two
            // timestamp forms replace the sample with a float of its own time.
            NativeExpr::Instant { input, op } => match op.value {
                InstantValue::Sample => kind_of(input),
                InstantValue::Timestamp | InstantValue::InfoTimestamp => Some(FieldKind::Float),
            },
            // `histogram_count` and the other native-histogram readers answer with one float per
            // sample; the map kernels that keep the samples keep their kind as well.
            NativeExpr::Map { input, op } => {
                let mut kind = kind_of(input)?;
                for op in op.ops.iter() {
                    kind = match op {
                        // The native-histogram readers answer one float per sample.
                        MapOp::Histogram(_) => FieldKind::Float,
                        // A label rewrite writes labels only, so the samples it passes on are the
                        // ones it was given, kind included.
                        MapOp::Label(_) => kind,
                        // No math function keeps a histogram, so a histogram reaching one is a
                        // shape this walk does not name rather than a silent float.
                        MapOp::Math(_) if kind == FieldKind::Histogram => return None,
                        MapOp::Math(_) => FieldKind::Float,
                    };
                }
                Some(kind)
            }
            // A constant beside a vector is broadcast over it, so the operation answers with the
            // vector's own samples: a histogram times a float is still a histogram. Two vectors
            // use the arithmetic result's kind, including any samples emitted by fill, and an
            // empty float field only when no possible pairing can emit a sample.
            NativeExpr::Binary {
                left,
                right,
                matching,
            } => {
                // `bool` makes a comparison answer 1 or 0 whatever its operands hold, which is
                // what the kernel does before it looks at the kinds.
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
            // A canonical merge collects pieces of one label set: it assembles the samples it was
            // given and never converts them, so the field keeps the kind it had.
            NativeExpr::Merge { input } => kind_of(input),
            // A set operation keeps the payloads it selects - `and` and `unless` keep the left
            // samples, `or` keeps the left's and the right's unmatched ones - so a pair of
            // different kinds would answer a field carrying both. This route declares one type per
            // field, so it names the answer only when both sides agree. That is this planner's
            // condition for a uniformly typed column, not a rule Prometheus states.
            NativeExpr::Set { left, right, .. } => {
                let left_kind = kind_of(left)?;
                (left_kind == kind_of(right)?).then_some(left_kind)
            }
            // `scalar()` answers one float per grid point, whatever its input holds.
            NativeExpr::ScalarOf { .. } => Some(FieldKind::Float),
            // A ranking hands on the series it selected, so the field keeps the kind it read; its
            // parameter is a scalar, which carries no samples of its own.
            NativeExpr::ParameterizedTopK { input, .. } => kind_of(input),
            // `histogram_quantile` and `histogram_fraction` reduce a histogram to one float per
            // sample whatever their arguments are, constant or per point.
            NativeExpr::ParameterizedHistogram { .. } => Some(FieldKind::Float),
            // Every aggregate answers floats: `count`/`group` answer a count or a `1` whatever the
            // samples they counted, `min`/`max`/`stddev`/`stdvar` fold the float samples and skip
            // the histogram ones, and `sum`/`avg` - the only two that would fold the payload - keep
            // a histogram out at admission. Naming it here is what lets `sum(count_over_time(h[5m]))`
            // - a float built from histograms - declare a float column instead of keeping the
            // existing plan.
            NativeExpr::Aggregate { .. } => Some(FieldKind::Float),
            _ => None,
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
    /// Aggregate whose partial accumulator this state carries.
    pub kind: AggregateStateKind,
    /// Ordered accumulator components, shared with the kernel layout.
    pub components: Arc<[StateComponent]>,
    /// The accumulator columns, one per aggregated field.
    pub accumulators: Arc<[FieldSpec]>,
    /// The grid the state is indexed on.
    pub grid: Arc<EvalGrid>,
}

/// Returns the state type implied by an aggregate over an input of `field_kind`, if that state can
/// be merged across a partition boundary.
///
/// Two families share this table. A numeric input keeps the components the accumulator kernel
/// declares, and the component list comes from that kernel rather than a second mapping, so changing
/// a kernel's state fields changes the native binding in lockstep. A native-histogram input binds
/// the histogram state of the same aggregate: it carries the payload and the compensation that
/// belongs to it, plus the evidence a merge of two partitions has to report. A quantile has no
/// partial kernel at all, whatever it reads.
pub(crate) fn aggregate_state_binding(
    kind: AggregateKind,
    field_kind: FieldKind,
) -> Option<(AggregateStateKind, Vec<StateComponent>)> {
    if field_kind == FieldKind::Histogram {
        let state_kind = match kind {
            AggregateKind::Sum => AggregateStateKind::HistogramSum,
            AggregateKind::Avg => AggregateStateKind::HistogramAvg,
            // Every other aggregate folds a float answer out of the samples it reads - a count is a
            // count whatever it counted, and the extreme and deviation kinds skip a histogram
            // sample - so a histogram input keeps their numeric state.
            _ => return float_state_binding(kind),
        };
        // The layout is the kind's own canonical list, which is the list its frame accepts.
        return Some((state_kind, state_kind.components().to_vec()));
    }
    float_state_binding(kind)
}

/// The numeric state one aggregate keeps, from the components its kernel declares.
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
    /// The kind the aggregate's direct input answers with, decided where this node was bound.
    ///
    /// It is what its state carries: a `sum` or `avg` over native histograms keeps histogram state,
    /// while the same aggregate over floats keeps numeric components. It is decided from the input's
    /// own output kind rather than from the source, because an operator between the two can change
    /// it -- `sum(count_over_time(h[5m]))` aggregates a float built from histograms.
    pub field_kind: FieldKind,
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
///
/// The variants are numbered by their position, and postcard writes that number, so these indices
/// are a wire contract: a version 3 [`crate::extension_plan::FusedSeries`] envelope carries the
/// whole [`NativeTree`] rather than a prefix of it, and a variant inserted anywhere but the end
/// renames every variant after it for a reader of an earlier build. New variants are therefore
/// appended after `Info`, and `native_expr_variant_indices_are_a_wire_contract` in the tests below
/// pins the numbers.
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
    /// `scalar(v)`: at each grid point, the sole sample value or NaN when the vector has zero or
    /// more than one sample.
    ScalarOf { input: Box<NativeNode> },
    /// `topk`/`bottomk` whose `k` is evaluated at every grid point instead of bound as a constant.
    ///
    /// `op` is still the `TopK` the constant path binds; its `k` is unused here, because the value
    /// this node selects with is the parameter's, read once per point.
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
}

/// One node of a bound tree.
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq, Hash)]
pub struct NativeNode {
    /// What this node computes, including its bound operands.
    pub expr: NativeExpr,
    /// What this node produces.
    pub output: NativeType,
}

/// Whether this tree can hand over two series that share one canonical label set.
///
/// A canonical label set is the identity a [`NativeExpr::Merge`] groups by: the metric name plus the
/// ordinary labels, which is exactly what [`SeriesLabels`] holds. One scan partition holds one
/// series per such set - a stored series is one identity, and the scan splits rows by storage key
/// rather than by label set - so only a computation that *writes* an existing label can turn two of
/// those identities into one. A `label_replace` or `label_join` does exactly that, and it may name
/// the very label that told its input's series apart.
///
/// The rewrite is judged by the labels it writes and the ones its input carries, which is why the
/// caller supplies what it knows about each source's tags, indexed by the `source_index` the leaves
/// were bound to: a destination that names no ordinary label of the input writes a fresh one, and a
/// fresh ordinary label cannot make two series equal. That is the difference between
/// `label_replace(x, "dst", "$1", "host", "(.*)")` - which no merge has to collect - and
/// `label_replace(x, "host", "same", "host", ".*")`, which folds every series of `x` into one label
/// set.
///
/// A source's entry is its complete runtime label set, so `__name__` is one of its labels whenever
/// its series carry a name - a scan bound to one metric names every series from its binding. That is
/// what makes `label_replace(x, "__name__", "same", "__name__", ".*")` a rewrite of an existing
/// label: two metrics whose ordinary labels are equal can be told apart by name alone, and rewriting
/// that name folds them into one identity. A name write is collected even when the derived set no
/// longer carries `__name__` - after `abs`, a `timestamp` or a range function dropped it - because
/// the scan below those kernels can still hold several metrics whose ordinary labels are equal, and
/// writing one name puts them under one identity; the walk has no proof that the input reads a
/// single metric, so the conservative answer is the one that collects.
///
/// Everything else either keeps the set it read or answers a set of its own:
///
/// - A map of math drops the metric name, and an arithmetic broadcast does too, but within one
///   metric the remaining tags are the primary key: two stored series of one metric that agree on
///   every other label are the same series, so dropping the name cannot make them one. Each of
///   those nodes hands its input's answer on rather than claiming one of its own.
/// - An `Aggregate` answers its groups, a `Vector` that folds series answers its result sets, and a
///   `Binary` that reads whole vectors files its rows by the label set a sample *ends* under and
///   refuses two values of a field at one point. A `Set` does the same, and `info` merges the
///   results that share a label set. Each answers one series per set it names.
/// - A node that hands its input's series over unchanged - a selection, a window, a timestamp, a
///   parameterized map, `topk` and its ranking siblings - passes the answer on: an alias below it is
///   still an alias above it.
///
/// This is the fact [`merge_label_rewrites`](crate::native::canonicalize::merge_label_rewrites)
/// inserts a `Merge` for: a consumer that reads a whole label set cannot answer from pieces, and this
/// says where pieces of one set can exist at all. A `Merge` is the collection itself, so it answers
/// `false` - which is what makes the pass idempotent.
pub fn duplicates_canonical_labels(node: &NativeNode, source_tags: &[Vec<String>]) -> bool {
    match &node.expr {
        NativeExpr::Input(_)
        | NativeExpr::Scalar(_)
        | NativeExpr::EvalTime { .. }
        | NativeExpr::VectorOf { .. } => false,
        // The collection itself: one series per label set comes out of it.
        NativeExpr::Merge { .. } => false,
        // These hand each series over as they read it, so what aliases below them still aliases.
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::ParameterizedMap { input, .. }
        | NativeExpr::ParameterizedTopK { input, .. } => {
            duplicates_canonical_labels(input, source_tags)
        }
        // `scalar()` answers one value, which names no label set at all.
        NativeExpr::ScalarOf { .. } => false,
        // The histogram functions fold the bucket series of one histogram into one series, so two
        // pieces of one label set cannot survive them.
        NativeExpr::ParameterizedHistogram { .. } => false,
        // A rewrite writes a caller-chosen destination label, so two series can end under one set -
        // but only if that label was one of the input's, because writing a label no series carries
        // leaves every series' identity distinct. `map_writes_existing_label` is that question, and
        // an input whose labels this walk cannot derive is collected rather than trusted. Even a
        // whole-vector consumer can emit distinct series whose labels a later rewrite makes equal.
        NativeExpr::Map { input, op } => {
            map_writes_existing_label(input, op, source_tags).unwrap_or(true)
                || duplicates_canonical_labels(input, source_tags)
        }
        // A fold answers one series per group, which is one label set per group.
        NativeExpr::Aggregate { .. } => false,
        NativeExpr::Vector { input, op } => match op {
            // These keep and order the series they read, so they alias exactly as their input does.
            BoundVectorOp::TopK { .. }
            | BoundVectorOp::LimitK { .. }
            | BoundVectorOp::LimitRatio { .. }
            | BoundVectorOp::Sort(_) => duplicates_canonical_labels(input, source_tags),
            // `absent` reports one series, `count_values` one per distinct value, and the histogram
            // functions fold the bucket series of one histogram into one series.
            BoundVectorOp::Absent { .. }
            | BoundVectorOp::Histogram(_)
            | BoundVectorOp::CountValues { .. } => false,
        },
        NativeExpr::Binary { left, right, .. } => {
            let vector = |side: &NativeNode| side.output.samples().is_some();
            if vector(left) && vector(right) {
                // Two vectors are matched by label, and the kernel files its rows by the label set a
                // sample ends under, refusing two values of one field at one point.
                false
            } else {
                // A broadcast reaches each series of its vector side as it is.
                let carried = if vector(left) { left } else { right };
                duplicates_canonical_labels(carried, source_tags)
            }
        }
        // A set operation keeps each side's samples and files them by result label set, refusing two
        // values of one field at one point.
        NativeExpr::Set { .. } => false,
        // `info` indexes the info side by identifying labels and merges the results that share a
        // label set.
        NativeExpr::Info { .. } => false,
    }
}

/// Whether `op`, applied to `input`'s series, writes a label those series already carry.
///
/// The walk is over the ops in written order, starting from the labels the input reports. A
/// destination the input already carries can fold two series into one; a fresh destination cannot.
/// A `__name__` destination is always treated as carried, even when the derived set has lost it: a
/// kernel such as `abs`, `timestamp` or a range function drops the name, but the scan below it can
/// still hold several metrics whose ordinary labels are equal, and writing one name puts those
/// identities under one label set. Nothing here proves the input reads a single metric, so the
/// conservative answer is the one that collects.
///
/// `None` when the input's label set is not derivable, which is treated as "it may carry the
/// destination" by the caller: an unknown set proves nothing, so the conservative answer is the one
/// that collects.
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
            // A label the input did not carry is one the rewrite publishes only when it can write a
            // value; a delete-only rewrite adds nothing a later op could read as carried.
            if rewrite.writes_value() && !tags.iter().any(|tag| tag == destination) {
                tags.push(destination.to_string());
            }
        }
    }
    Some(existing)
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
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => false,
        NativeExpr::Window { input, .. } => {
            matches!(unifies_its_own_contributions(input), Some(false))
                || reads_across_series(input)
        }
        NativeExpr::Instant { input, .. }
        | NativeExpr::VectorOf { input }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input } => reads_across_series(input),
        // A parameterized operation reads its parameters too, and one of them can read across series
        // as well: `clamp_min(b, scalar(a))` bounds each series of `b` by a value that only exists
        // once the whole of `a` is gathered, so the node reads across series wherever its parameter
        // does.
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => reads_across_series(input) || parameters.iter().any(reads_across_series),
        // `scalar()` needs the whole vector: which series carries the one value at a point decides
        // the answer, so the node reads across series.
        NativeExpr::ScalarOf { .. } => true,
        NativeExpr::Binary { left, right, .. } => {
            let scalar = |side: &NativeNode| matches!(side.output, NativeType::Scalar(_));
            if scalar(left) || scalar(right) {
                reads_across_series(left) || reads_across_series(right)
            } else {
                // Two vectors are matched by label, which pairs series of the whole vector.
                true
            }
        }
        // A parameterized operation reads its parameter too, and a `k` that changes per point
        // decides which series survive.
        NativeExpr::ParameterizedTopK { .. } | NativeExpr::ParameterizedHistogram { .. } => true,
        NativeExpr::Aggregate { .. }
        | NativeExpr::Vector { .. }
        | NativeExpr::Set { .. }
        | NativeExpr::Info { .. }
        | NativeExpr::Merge { .. } => true,
    }
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
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq, Hash)]
pub struct NativeTree {
    root: NativeNode,
}

impl NativeTree {
    /// Creates a tree over `root`, checking that every node's contract holds.
    ///
    /// A structurally inconsistent tree is a compiler defect.
    pub fn try_new(root: NativeNode) -> Result<Self> {
        root.validate()?;
        Ok(Self { root })
    }

    /// The root node.
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

/// Stack one [`NativeNode::validate`] level needs below it before the check grows its stack.
///
/// The check recurses once per tree level, and a debug build's frame for one level is large:
/// measured on an 80-term chain, the whole check needed about 1.9 MiB, which overflows the 2 MiB
/// stack a test thread has. There is no depth limit on the trees this route compiles, so the check
/// grows its stack on demand rather than relying on the caller's.
const VALIDATE_STACK_RED_ZONE: usize = 256 * 1024;

/// Stack a growth adds when the reserve above is spent. It holds several levels of the same check.
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
                // An input node's output is exactly the binding it was bound to: a scan hands over
                // the samples it was created for, and an exchange boundary the state.
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
                // The scalar is materialized on the grid it is evaluated on, so an output on another
                // grid would address positions the scalar does not have.
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
                ensure_samples(&self.output, "a parameterized map")?;
                ensure_same_fields(&input.output, &self.output, "a parameterized map")?;
                ensure_grid(&self.output, input_grid, "a parameterized map")?;
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
                        // The state was collected from every contributing execution, so the
                        // output is the complete value on the grid that state is indexed on.
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

/// Whether `node` hands over one logical series per canonical label set, or several.
///
/// This is the rule the runtime collectors use to decide whether a consumer above `node` has to
/// collect what `node` emits. `RuntimeNode::build` in [`execute`](crate::native::execute) and the
/// Window and `limitk` collectors share this predicate so their decisions cannot disagree.
///
/// Three answers, because what a consumer may do about a contribution carries two facts and a
/// boundary answers neither:
///
/// - `Some(true)`: the output is one series per canonical label set, established here, so a
///   consumer that needs one unified series already has it and collects nothing.
/// - `Some(false)`: the output may hold several series of one label set, and this node *says so*,
///   so a consumer that needs one unified series collects them through `MergeInput`. A
///   [`NativeExpr::Merge`] above such a node would make it stream an already-merged series and
///   turn that collection off.
/// - `None`: the output may hold several, and this node says nothing: it has neither a proof to
///   offer nor a report to make. Only a boundary ([`NativeExpr::Input`]) answers this, and a node
///   that hands its input's series back unchanged passes the answer on. Runtime collectors leave
///   this case alone because they collect only when the answer is `Some(false)`.
///
/// The collection condition is `matches!(answer, Some(false))`: the Window `MergeInput` collector
/// at runtime and the `limitk` collector both ask this predicate of their input.
#[allow(clippy::match_same_arms)] // Keep each expression's contribution guarantee explicit.
pub fn unifies_its_own_contributions(node: &NativeNode) -> Option<bool> {
    match &node.expr {
        // A boundary hands over the pieces some upstream execution produced: what it holds is one
        // storage series at a time for a scan and one partition's fragments of a label set for an
        // exchange, and nothing in the tree can tell which series those fragments belong to. A
        // merge binds samples evaluated on a grid rather than proving the pieces are one series
        // per label set, so a proof is not this function's to give: it answers `None`. Runtime
        // collectors gather only when this node reports fragments with `Some(false)`.
        NativeExpr::Input(_) => None,
        // A constant holds no label set at all, so it has nothing that could arrive twice.
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } | NativeExpr::VectorOf { .. } => {
            Some(true)
        }
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
        // A map can rewrite labels, so two series can end under one label set and the runtime
        // records what it wrote in an `Emitted` table.
        NativeExpr::Map { .. } | NativeExpr::ParameterizedMap { .. } => Some(false),
        // `topk` hands back the series it read with their labels, so the input's answer is its own -
        // exactly as for the bound operation.
        NativeExpr::ParameterizedTopK { input, .. } => unifies_its_own_contributions(input),
        // A histogram function groups its rows by label set itself, so it emits one series per set.
        NativeExpr::ParameterizedHistogram { .. } => Some(true),
        // `scalar()` answers one value per point, which is not several series.
        NativeExpr::ScalarOf { .. } => Some(true),
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
                // A filtering comparison keeps the label set it read, so the side that carries
                // series answers for this node. Which side that is follows the rule the runtime's
                // broadcast matches on: an evaluated vector carries them, and with two scalars the
                // carrier is the side that is not a bound constant (`broadcast_runtime`'s
                // `literal(left).is_none()`), which is what the second test below reads.
                //
                // The two selection rules agree wherever it is observable: every scalar node kind
                // answers `Some(true)` (`Scalar`, `EvalTime`, `VectorOf`), so a scalar-scalar pair
                // answers `Some(true)` whichever side is named and a consumer collects nothing
                // either way. The constant check is still what keeps the two readers describing
                // the same node rather than coinciding by accident.
                let carries_series = |node: &NativeNode| {
                    if node.output.samples().is_some() {
                        return true;
                    }
                    !matches!(node.expr, NativeExpr::Scalar(BoundScalar { .. }))
                };
                let vector = if carries_series(left) { left } else { right };
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
        // in input order, so its input carries the merge the runtime performs when that input reports fragments, and the other three group their rows
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

// Tests for native plan validation and execution contracts.
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
