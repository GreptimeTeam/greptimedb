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

//! One line per node of a [`NativeTree`].
//!
//! A node's recorded figures are looked up by the path the runtime registered it under and shown
//! at the end of its own line. A tree that never ran (`EXPLAIN`) prints structure only.

use std::borrow::Cow;
use std::fmt::{self, Write};
use std::sync::Arc;

use common_recordbatch::adapter::{NativeTreeMetrics, format_metric};
use datatypes::series::time::EvalGrid;
use datatypes::series::{SeriesLabels, SeriesOrdering};

use crate::histogram_eval::HistogramFunction;
use crate::map::MathOp;
use crate::native::metrics::NodeMetricsSnapshot;
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundVectorOp, BoundWindow, Grouping,
    InstantValue, MapOp, NativeExpr, NativeNode, NativeTree, NativeType, ParameterizedOp,
};
use crate::range_eval::RangeFunction;
use crate::select::{MatchOp, SortOrder};
use crate::vector_match::{BinaryOp, Cardinality, FillValues, MatchingLabels, SetOp};

/// The path the runtime registers a tree's root node under; descendants append the segments of
/// [`child_segments`].
pub const ROOT_PATH: &str = "root";

/// One displayed node. It feeds both the text display and the machine-readable `ANALYZE` output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeNodeReport {
    /// The node's line without figures, as `<kind>: <what it computes>`.
    pub line: String,
    /// The figures this node recorded, in [`crate::native::metrics::NodeMetric::ALL`] order.
    pub metrics: Vec<(String, usize)>,
    /// The nodes this one reads, in child order.
    pub children: Vec<NativeNodeReport>,
}

impl From<NativeNodeReport> for NativeTreeMetrics {
    fn from(report: NativeNodeReport) -> Self {
        Self {
            line: report.line,
            metrics: report.metrics,
            children: report.children.into_iter().map(Self::from).collect(),
        }
    }
}

/// A grid as `<start>/<step>x<len>`.
fn grid_name(grid: &EvalGrid) -> String {
    let spec = grid.spec();
    format!(
        "{}/{step}x{len}",
        spec.start_ms,
        step = spec.step_ms,
        len = spec.len
    )
}

fn label_list(labels: &[Arc<str>]) -> String {
    labels
        .iter()
        .map(|label| label.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The ordering an input guarantees, when it is weaker than the scan's default.
fn ordering(ordering: SeriesOrdering) -> &'static str {
    match ordering {
        SeriesOrdering::KeyThenTime => "",
        SeriesOrdering::Contiguous => ", contiguous",
        SeriesOrdering::Interleaved => ", interleaved",
    }
}

/// The grid a node's output is evaluated on, or `-` for one that has none.
fn grid_of(node: &NativeNode) -> String {
    node.output
        .grid()
        .map_or_else(|| "-".to_string(), |grid| grid_name(grid))
}

/// The operation a node computes, in the words of the query that produced it.
fn describe(node: &NativeNode) -> String {
    match &node.expr {
        NativeExpr::Input(input) => match &input.output {
            NativeType::Samples(schema) if schema.eval_grid.is_some() => {
                format!(
                    "evaluated samples source={}{}",
                    input.source_index,
                    ordering(input.ordering)
                )
            }
            NativeType::Samples(_) => format!(
                "raw samples source={}{}",
                input.source_index,
                ordering(input.ordering)
            ),
            NativeType::Scalar(grid) => format!("scalar on grid {}", grid_name(grid)),
            NativeType::AggregateState(_) => "aggregate state".to_string(),
        },
        NativeExpr::Scalar(scalar) => scalar.value.to_string(),
        NativeExpr::EvalTime { date: None } => format!("time() on grid {}", grid_of(node)),
        NativeExpr::EvalTime { date: Some(date) } => {
            format!("{}() on grid {}", date.name(), grid_of(node))
        }
        NativeExpr::VectorOf { .. } => "vector()".to_string(),
        NativeExpr::ScalarOf { .. } => "scalar()".to_string(),
        NativeExpr::ParameterizedTopK { op, .. } => match op {
            BoundVectorOp::TopK { top: true, .. } => "parameterized topk".to_string(),
            BoundVectorOp::TopK { top: false, .. } => "parameterized bottomk".to_string(),
            _ => "parameterized topk".to_string(),
        },
        NativeExpr::ParameterizedHistogram { op, parameters, .. } => {
            format!(
                "{} ({} parameters)",
                match op {
                    BoundVectorOp::Histogram(HistogramFunction::Quantile(_)) =>
                        "histogram_quantile",
                    BoundVectorOp::Histogram(HistogramFunction::Fraction { .. }) =>
                        "histogram_fraction",
                    _ => "histogram",
                },
                parameters.len()
            )
        }
        NativeExpr::ParameterizedMap { op, .. } => match op {
            ParameterizedOp::Round => "round".to_string(),
            ParameterizedOp::Clamp => "clamp".to_string(),
            ParameterizedOp::ClampMin => "clamp_min".to_string(),
            ParameterizedOp::ClampMax => "clamp_max".to_string(),
            ParameterizedOp::MaxOf => "max_of".to_string(),
            ParameterizedOp::MinOf => "min_of".to_string(),
        },
        NativeExpr::Instant { op, .. } => {
            let mut text = format!(
                "lookback {}ms offset {} on grid {}",
                op.lookback_ms,
                op.offset_ms,
                grid_name(&op.output_grid)
            );
            if let Some(at) = op.at_ms {
                text.push_str(&format!(" @ {at}ms"));
            }
            match op.value {
                InstantValue::Sample => {}
                InstantValue::Timestamp => text.push_str(" timestamp"),
                InstantValue::InfoTimestamp => text.push_str(" info timestamp"),
            }
            text
        }
        NativeExpr::Window { op, .. } => window(op),
        NativeExpr::Merge { .. } => "canonical label-set merge".to_string(),
        NativeExpr::Timestamp { .. } => "evaluation time".to_string(),
        NativeExpr::Map { op, .. } => {
            let ops: Vec<String> = op.ops.iter().map(map_op).collect();
            ops.join(", ")
        }
        NativeExpr::Vector { op, .. } => match op {
            BoundVectorOp::TopK { k, top, group } => format!(
                "{} {k} {}",
                if *top { "topk" } else { "bottomk" },
                grouping(group)
            ),
            BoundVectorOp::LimitK { k, group } => format!("limitk {k} {}", grouping(group)),
            BoundVectorOp::LimitRatio { ratio } => format!("limit_ratio {ratio}"),
            BoundVectorOp::Sort(order) => match order {
                SortOrder::Asc => "sort".to_string(),
                SortOrder::Desc => "sort_desc".to_string(),
                SortOrder::ByLabel(labels) => format!("sort_by_label [{}]", label_list(labels)),
                SortOrder::ByLabelDesc(labels) => {
                    format!("sort_by_label_desc [{}]", label_list(labels))
                }
            },
            BoundVectorOp::Absent { labels } => format!("absent {{{}}}", label_pairs(labels)),
            BoundVectorOp::Histogram(HistogramFunction::Quantile(quantile)) => {
                format!("histogram_quantile {quantile}")
            }
            BoundVectorOp::Histogram(HistogramFunction::Fraction { lower, upper }) => {
                format!("histogram_fraction {lower} {upper}")
            }
            BoundVectorOp::CountValues { label, group } => {
                format!("count_values {label:?} {}", grouping(group))
            }
        },
        NativeExpr::Aggregate { op, .. } => describe_aggregate(op, &aggregate_kind(op.kind)),
        // The bound `φ` is unused, so it is left out.
        NativeExpr::ParameterizedAggregate { op, .. } => format!(
            "{}, parameter per point",
            describe_aggregate(op, aggregate_name(op.kind))
        ),
        NativeExpr::Binary { matching, .. } => format!(
            "{}{} {} {}{}",
            binary_op(matching.op),
            if matching.return_bool { " bool" } else { "" },
            matching_labels(&matching.labels),
            match &matching.cardinality {
                Cardinality::OneToOne => "one-to-one".to_string(),
                Cardinality::ManyToOne(labels) => format!("many-to-one [{}]", label_list(labels)),
                Cardinality::OneToMany(labels) => format!("one-to-many [{}]", label_list(labels)),
            },
            fill(matching.fill)
        ),
        NativeExpr::Set { op, .. } => format!(
            "{} {}",
            match op.op {
                SetOp::And => "and",
                SetOp::Or => "or",
                SetOp::Unless => "unless",
            },
            matching_labels(&op.labels)
        ),
        NativeExpr::Info { selector, .. } => format!(
            "names [{}] data [{}]{}",
            selector
                .name_matchers
                .iter()
                .map(|(op, value)| format!(
                    "__name__{}\"{value}\"",
                    match op {
                        MatchOp::Equal => "=",
                        MatchOp::NotEqual => "!=",
                        MatchOp::Regex => "=~",
                        MatchOp::NotRegex => "!~",
                    }
                ))
                .collect::<Vec<_>>()
                .join(", "),
            label_list(&selector.data_labels),
            if selector.keep_unmatched {
                " keep unmatched"
            } else {
                ""
            }
        ),
    }
}

fn matching_labels(labels: &MatchingLabels) -> String {
    match labels {
        MatchingLabels::On(labels) => format!("on [{}]", label_list(labels)),
        MatchingLabels::Ignoring(labels) => format!("ignoring [{}]", label_list(labels)),
    }
}

fn fill(values: FillValues) -> String {
    let side = |name: &str, value: Option<f64>| {
        value
            .map(|value| format!(" {name} {value}"))
            .unwrap_or_default()
    };
    format!(
        "{}{}",
        side("fill_left", values.lhs),
        side("fill_right", values.rhs)
    )
}

/// `folded` names the aggregate, with or without its bound argument.
fn describe_aggregate(op: &BoundAggregate, folded: &str) -> String {
    format!(
        "{} {} [{}] {}",
        folded,
        match &op.group {
            Grouping::By(_) => "by",
            Grouping::Without(_) => "without",
        },
        label_list(match &op.group {
            Grouping::By(labels) | Grouping::Without(labels) => labels.as_ref(),
        }),
        match op.stage {
            AggregateStage::Complete => "complete",
            AggregateStage::Partial => "partial",
            AggregateStage::Final => "final",
        }
    )
}

/// The PromQL name of an aggregate, with the constant argument it was bound to.
fn aggregate_kind(kind: AggregateKind) -> String {
    match kind {
        AggregateKind::Quantile { quantile } => format!("quantile {quantile}"),
        other => aggregate_name(other).to_string(),
    }
}

/// The PromQL name of an aggregate, without the argument it may have bound.
fn aggregate_name(kind: AggregateKind) -> &'static str {
    match kind {
        AggregateKind::Sum => "sum",
        AggregateKind::Avg => "avg",
        AggregateKind::Count => "count",
        AggregateKind::Min => "min",
        AggregateKind::Max => "max",
        AggregateKind::Group => "group",
        AggregateKind::Stddev => "stddev",
        AggregateKind::Stdvar => "stdvar",
        AggregateKind::Quantile { .. } => "quantile",
    }
}

/// A window: what it runs, over which range, and where it lands.
fn window(op: &BoundWindow) -> String {
    format!(
        "{} [{}ms]{} offset {} anchor {} on grid {}",
        function(op.function),
        op.window.range_ms,
        match op.window.modifier {
            Some(modifier) => format!(" {} lookback {}ms", modifier.name(), modifier.before_ms()),
            None => String::new(),
        },
        op.window.offset_ms,
        match op.window.anchor_ms {
            Some(anchor_ms) => anchor_ms.to_string(),
            None => "-".to_string(),
        },
        grid_name(&op.output_grid)
    )
}

/// The PromQL name of a range function, with the constant argument it was bound to.
fn function(function: RangeFunction) -> String {
    match function {
        RangeFunction::Rate => "rate".to_string(),
        RangeFunction::Increase => "increase".to_string(),
        RangeFunction::Delta => "delta".to_string(),
        RangeFunction::Irate => "irate".to_string(),
        RangeFunction::Idelta => "idelta".to_string(),
        RangeFunction::AvgOverTime => "avg_over_time".to_string(),
        RangeFunction::MinOverTime => "min_over_time".to_string(),
        RangeFunction::MaxOverTime => "max_over_time".to_string(),
        RangeFunction::SumOverTime => "sum_over_time".to_string(),
        RangeFunction::CountOverTime => "count_over_time".to_string(),
        RangeFunction::LastOverTime => "last_over_time".to_string(),
        RangeFunction::PresentOverTime => "present_over_time".to_string(),
        RangeFunction::StddevOverTime => "stddev_over_time".to_string(),
        RangeFunction::StdvarOverTime => "stdvar_over_time".to_string(),
        RangeFunction::QuantileOverTime(quantile) => format!("quantile_over_time {quantile}"),
        RangeFunction::Resets => "resets".to_string(),
        RangeFunction::Changes => "changes".to_string(),
        RangeFunction::Deriv => "deriv".to_string(),
        RangeFunction::PredictLinear(seconds) => format!("predict_linear {seconds}"),
        RangeFunction::DoubleExponentialSmoothing { smoothing, trend } => {
            format!("double_exponential_smoothing {smoothing} {trend}")
        }
        RangeFunction::FirstOverTime => "first_over_time".to_string(),
        RangeFunction::MadOverTime => "mad_over_time".to_string(),
        RangeFunction::TsOfFirstOverTime => "ts_of_first_over_time".to_string(),
        RangeFunction::TsOfLastOverTime => "ts_of_last_over_time".to_string(),
        RangeFunction::TsOfMinOverTime => "ts_of_min_over_time".to_string(),
        RangeFunction::TsOfMaxOverTime => "ts_of_max_over_time".to_string(),
    }
}

/// The PromQL call a map operation makes, with the constant arguments it was bound to.
fn map_op(op: &MapOp) -> String {
    match op {
        MapOp::Math(op) => math_op(op),
        MapOp::Histogram(function) => function.name().to_string(),
        MapOp::Label(rewrite) => rewrite.to_string(),
    }
}

/// The PromQL name of a math function, with the constant arguments it was bound to.
fn math_op(op: &MathOp) -> String {
    match op {
        MathOp::Abs => "abs".to_string(),
        MathOp::Ceil => "ceil".to_string(),
        MathOp::Floor => "floor".to_string(),
        MathOp::Sqrt => "sqrt".to_string(),
        MathOp::Round { nearest } => format!("round {nearest}"),
        MathOp::Exp => "exp".to_string(),
        MathOp::Ln => "ln".to_string(),
        MathOp::Log2 => "log2".to_string(),
        MathOp::Log10 => "log10".to_string(),
        MathOp::Sin => "sin".to_string(),
        MathOp::Cos => "cos".to_string(),
        MathOp::Tan => "tan".to_string(),
        MathOp::Asin => "asin".to_string(),
        MathOp::Acos => "acos".to_string(),
        MathOp::Atan => "atan".to_string(),
        MathOp::Sinh => "sinh".to_string(),
        MathOp::Cosh => "cosh".to_string(),
        MathOp::Tanh => "tanh".to_string(),
        MathOp::Asinh => "asinh".to_string(),
        MathOp::Acosh => "acosh".to_string(),
        MathOp::Atanh => "atanh".to_string(),
        MathOp::Deg => "deg".to_string(),
        MathOp::Rad => "rad".to_string(),
        MathOp::Sgn => "sgn".to_string(),
        MathOp::Clamp { min, max } => format!("clamp {min} {max}"),
        MathOp::ClampMin { min } => format!("clamp_min {min}"),
        MathOp::ClampMax { max } => format!("clamp_max {max}"),
        MathOp::Date(function) => function.name().to_string(),
    }
}

/// The PromQL symbol of a binary operator.
fn binary_op(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Pow => "^",
        BinaryOp::Atan2 => "atan2",
        BinaryOp::Eq => "==",
        BinaryOp::Ne => "!=",
        BinaryOp::Gt => ">",
        BinaryOp::Lt => "<",
        BinaryOp::Ge => ">=",
        BinaryOp::Le => "<=",
    }
}

/// The labels of `metric` as `name="value"` pairs, the metric name first.
fn label_pairs(metric: &SeriesLabels) -> String {
    metric
        .name()
        .map(|name| format!("__name__={name:?}"))
        .into_iter()
        .chain(
            metric
                .tags()
                .iter()
                .map(|tag| format!("{}={:?}", tag.name, tag.value)),
        )
        .collect::<Vec<_>>()
        .join(", ")
}

/// A grouping as `by [labels]` or `without [labels]`.
fn grouping(group: &Grouping) -> String {
    match group {
        Grouping::By(labels) => format!("by [{}]", label_list(labels)),
        Grouping::Without(labels) => format!("without [{}]", label_list(labels)),
    }
}

/// What kind of node this is, as its line names it.
fn kind(expr: &NativeExpr) -> &'static str {
    match expr {
        NativeExpr::Input(_) => "Input",
        NativeExpr::Scalar(_) => "Scalar",
        NativeExpr::Instant { .. } => "Instant",
        NativeExpr::Window { .. } => "Window",
        NativeExpr::Merge { .. } => "Merge",
        NativeExpr::Map { .. } => "Map",
        NativeExpr::Timestamp { .. } => "Timestamp",
        NativeExpr::Aggregate { .. } => "Aggregate",
        NativeExpr::Vector { .. } => "Vector",
        NativeExpr::Binary { .. } => "Binary",
        NativeExpr::EvalTime { .. } => "EvalTime",
        NativeExpr::VectorOf { .. } => "VectorOf",
        NativeExpr::ScalarOf { .. } => "ScalarOf",
        NativeExpr::ParameterizedMap { .. } => "ParameterizedMap",
        NativeExpr::ParameterizedTopK { .. } => "ParameterizedTopK",
        NativeExpr::ParameterizedAggregate { .. } => "ParameterizedAggregate",
        NativeExpr::ParameterizedHistogram { .. } => "ParameterizedHistogram",
        NativeExpr::Set { .. } => "Set",
        NativeExpr::Info { .. } => "Info",
    }
}

/// The child nodes of `node` and the path segment the runtime registers each under. The runtime
/// registers `info`'s `base` and `info` as `left` and `right`.
fn child_segments(node: &NativeNode) -> Vec<(&NativeNode, Cow<'static, str>)> {
    let mut children = Vec::new();
    fn child<'a>(
        children: &mut Vec<(&'a NativeNode, Cow<'static, str>)>,
        segment: &'static str,
        node: &'a NativeNode,
    ) {
        children.push((node, Cow::Borrowed(segment)));
    }
    match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {}
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Merge { input }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::Aggregate { input, .. }
        | NativeExpr::Vector { input, .. }
        | NativeExpr::VectorOf { input }
        | NativeExpr::ScalarOf { input } => child(&mut children, "input", input),
        NativeExpr::ParameterizedTopK {
            input, parameter, ..
        } => {
            child(&mut children, "input", input);
            child(&mut children, "parameter", parameter);
        }
        NativeExpr::ParameterizedAggregate {
            input, parameter, ..
        } => {
            child(&mut children, "input", input);
            child(&mut children, "parameter", parameter);
        }
        NativeExpr::ParameterizedHistogram {
            input, parameters, ..
        } => {
            child(&mut children, "input", input);
            for (index, parameter) in parameters.iter().enumerate() {
                children.push((parameter, Cow::Owned(format!("parameter{index}"))));
            }
        }
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => {
            child(&mut children, "input", input);
            for (index, parameter) in parameters.iter().enumerate() {
                children.push((parameter, Cow::Owned(format!("parameter{index}"))));
            }
        }
        NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
            child(&mut children, "left", left);
            child(&mut children, "right", right);
        }
        NativeExpr::Info { base, info, .. } => {
            child(&mut children, "left", base);
            child(&mut children, "right", info);
        }
    }
    children
}

/// The node at `path` and everything below it, with the figures each node recorded.
fn report_node(node: &NativeNode, path: &str, metrics: &NodeMetricsSnapshot) -> NativeNodeReport {
    NativeNodeReport {
        line: format!("{}: {}", kind(&node.expr), describe(node)),
        metrics: metrics.described(path),
        children: child_segments(node)
            .into_iter()
            .map(|(child, segment)| report_node(child, &format!("{path}.{segment}"), metrics))
            .collect(),
    }
}

impl NativeTree {
    /// This tree, one report per node, with the figures `metrics` holds for each.
    pub fn report(&self, metrics: &NodeMetricsSnapshot) -> NativeNodeReport {
        report_node(self.root(), ROOT_PATH, metrics)
    }

    /// This tree rendered from column zero, with each node's own figures when `metrics` holds them.
    pub fn display_with<'a>(&'a self, metrics: &'a NodeMetricsSnapshot) -> NativeTreeDisplay<'a> {
        NativeTreeDisplay {
            tree: self,
            metrics,
        }
    }
}

/// A tree as it appears below the operator line: one node per line, two spaces per depth, each
/// node's figures at the end of its line.
pub struct NativeTreeDisplay<'a> {
    tree: &'a NativeTree,
    metrics: &'a NodeMetricsSnapshot,
}

impl fmt::Display for NativeTreeDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The caller's enclosing block carries the indentation.
        write_report(f, &self.tree.report(self.metrics), 0)
    }
}

fn write_report(
    f: &mut fmt::Formatter<'_>,
    report: &NativeNodeReport,
    depth: usize,
) -> fmt::Result {
    write!(f, "{:indent$}{}", "", report.line, indent = depth * 2)?;
    if !report.metrics.is_empty() {
        let figures = report
            .metrics
            .iter()
            .map(|(name, value)| format_metric(name, *value))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, ", metrics=[{figures}]")?;
    }
    for child in &report.children {
        f.write_char('\n')?;
        write_report(f, child, depth + 1)?;
    }
    Ok(())
}

impl fmt::Display for NativeTree {
    /// One line per node, two spaces per depth, without figures.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_report(f, &self.report(&NodeMetricsSnapshot::default()), 0)
    }
}

#[cfg(test)]
mod tests;
