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

//! Where a node sits in the final execution tree, and the metrics named after that position.
//!
//! A node is identified by its structural position — `root`, `.input`, `.left`, `.right` — rather
//! than by an id it carries. The position is not stored on the node: it is derived while walking
//! the tree. [`walk`] is the one traversal rule, and [`register_tree`] is its caller.
//!
//! The position stays in the metric name because the collector aggregates by name first: two
//! `rate` nodes sharing a bare name would be merged into one number. Positions are only an
//! observation detail of one plan — they are not stable across queries, plan rewrites or versions,
//! so nothing may depend on them beyond correlating a metric with a tree it was taken from.

use std::sync::Arc;

use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Time,
};

use crate::native::plan::{NativeExpr, NativeNode};

/// The reserved first segment of every native metric name.
const NATIVE_PREFIX: &str = "native";

/// A node's position in the final tree.
///
/// Rendering allocates a short string once, when the run-time state is created; the hot path holds
/// the metric handles instead of looking anything up by name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodePath(String);

impl NodePath {
    /// The position of the root node.
    pub fn root() -> Self {
        Self("root".to_string())
    }

    /// The position of a unary node's input.
    pub fn input(&self) -> Self {
        Self(format!("{}.input", self.0))
    }

    /// The position of a binary node's left input.
    pub fn left(&self) -> Self {
        Self(format!("{}.left", self.0))
    }

    /// The position of a binary node's right input.
    pub fn right(&self) -> Self {
        Self(format!("{}.right", self.0))
    }

    /// The path without the metric namespace.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The full metric name for `metric` at this position.
    pub fn metric_name(&self, metric: &str) -> String {
        format!("{NATIVE_PREFIX}.{}.{}", self.0, metric)
    }

    /// The full metric name for `metric` at this position in `partition`.
    ///
    /// A figure that must not be merged across partitions needs the partition in its name: the
    /// collector aggregates by name first, and a sum of peaks is not a peak.
    pub fn partitioned_metric_name(&self, metric: &str, partition: usize) -> String {
        format!("{NATIVE_PREFIX}.p{partition}.{}.{metric}", self.0)
    }
}

impl std::fmt::Display for NodePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Visits every node of `root` with its position, outermost first.
///
/// This is the one traversal rule: [`register_tree`] is its caller, so a node's metric is always
/// named after the position this walk gives it.
pub fn walk(root: &NativeNode, visit: &mut dyn FnMut(&NativeNode, &NodePath)) {
    fn go(node: &NativeNode, path: &NodePath, visit: &mut dyn FnMut(&NativeNode, &NodePath)) {
        visit(node, path);
        match &node.expr {
            NativeExpr::Input(_) | NativeExpr::Scalar(_) => {}
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. } => go(input, &path.input(), visit),
            NativeExpr::Binary { left, right, .. } => {
                go(left, &path.left(), visit);
                go(right, &path.right(), visit);
            }
        }
    }
    go(root, &NodePath::root(), visit);
}

/// The metric names one node reports.
///
/// [`Self::ALL`] is the list a reader should treat as complete: [`NodeMetrics::register`] names each
/// handle itself, and `registered_metrics_are_exactly_the_declared_set` fails when the two sides
/// drift apart.
pub struct NodeMetric;

impl NodeMetric {
    /// How many batches this node consumed.
    pub const INPUT_BATCHES: &'static str = "input_batches";
    /// How many batches this node produced.
    pub const OUTPUT_BATCHES: &'static str = "output_batches";
    /// How many sample positions this node ingested.
    pub const INPUT_SAMPLES: &'static str = "input_samples";
    /// How many grid positions this node evaluated, including those with no valid result.
    pub const EVALUATED_GRID_POINTS: &'static str = "evaluated_grid_points";
    /// How many valid values this node produced.
    pub const OUTPUT_VALUES: &'static str = "output_values";
    /// How many series this node closed.
    pub const COMPLETED_SERIES: &'static str = "completed_series";
    /// Time this node spent in its own work, excluding its inputs.
    pub const SELF_COMPUTE_TIME_NS: &'static str = "self_compute_time_ns";
    /// Bytes this node currently reserves.
    pub const CURRENT_RESERVED_BYTES: &'static str = "current_reserved_bytes";
    /// The highest byte count this node reserved during the execution.
    pub const PEAK_RESERVED_BYTES: &'static str = "peak_reserved_bytes";
    /// How many groups this node tracks, counted per partition: a group reached by several
    /// partitions is one entry in each.
    pub const GROUPS: &'static str = "groups";
    /// How many state entries this node tracks.
    pub const STATE_ENTRIES: &'static str = "state_entries";

    /// Every metric a node may report.
    pub const ALL: &'static [&'static str] = &[
        Self::INPUT_BATCHES,
        Self::OUTPUT_BATCHES,
        Self::INPUT_SAMPLES,
        Self::EVALUATED_GRID_POINTS,
        Self::OUTPUT_VALUES,
        Self::COMPLETED_SERIES,
        Self::SELF_COMPUTE_TIME_NS,
        Self::CURRENT_RESERVED_BYTES,
        Self::PEAK_RESERVED_BYTES,
        Self::GROUPS,
        Self::STATE_ENTRIES,
    ];
}

/// The metric handles one node updates as it runs.
///
/// The node writes these directly; it never looks a metric up by name, and it never formats a path
/// on the hot path.
#[derive(Clone, Debug)]
pub struct NodeMetrics {
    input_batches: Count,
    output_batches: Count,
    input_samples: Count,
    evaluated_grid_points: Count,
    output_values: Count,
    completed_series: Count,
    self_compute_time: Time,
    current_reserved_bytes: Gauge,
    peak_reserved_bytes: Gauge,
    groups: Gauge,
    state_entries: Gauge,
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self {
            input_batches: Count::new(),
            output_batches: Count::new(),
            input_samples: Count::new(),
            evaluated_grid_points: Count::new(),
            output_values: Count::new(),
            completed_series: Count::new(),
            self_compute_time: Time::new(),
            current_reserved_bytes: Gauge::new(),
            peak_reserved_bytes: Gauge::new(),
            groups: Gauge::new(),
            state_entries: Gauge::new(),
        }
    }
}

impl NodeMetrics {
    /// Registers this node's metrics under `path` in `set`.
    ///
    /// `partition` is part of the registration, so two partitions of one plan report separately.
    /// It is also part of the peak's name, because a peak is the one figure a collector must not
    /// add up across partitions.
    pub fn register(&self, path: &NodePath, set: &ExecutionPlanMetricsSet, partition: usize) {
        fn count(set: &ExecutionPlanMetricsSet, partition: usize, name: String, count: &Count) {
            MetricBuilder::new(set)
                .with_partition(partition)
                .build(MetricValue::Count {
                    name: name.into(),
                    count: count.clone(),
                });
        }
        fn gauge(set: &ExecutionPlanMetricsSet, partition: usize, name: String, gauge: &Gauge) {
            MetricBuilder::new(set)
                .with_partition(partition)
                .build(MetricValue::Gauge {
                    name: name.into(),
                    gauge: gauge.clone(),
                });
        }

        count(
            set,
            partition,
            path.metric_name(NodeMetric::INPUT_BATCHES),
            &self.input_batches,
        );
        count(
            set,
            partition,
            path.metric_name(NodeMetric::OUTPUT_BATCHES),
            &self.output_batches,
        );
        count(
            set,
            partition,
            path.metric_name(NodeMetric::INPUT_SAMPLES),
            &self.input_samples,
        );
        count(
            set,
            partition,
            path.metric_name(NodeMetric::EVALUATED_GRID_POINTS),
            &self.evaluated_grid_points,
        );
        count(
            set,
            partition,
            path.metric_name(NodeMetric::OUTPUT_VALUES),
            &self.output_values,
        );
        count(
            set,
            partition,
            path.metric_name(NodeMetric::COMPLETED_SERIES),
            &self.completed_series,
        );
        gauge(
            set,
            partition,
            path.metric_name(NodeMetric::CURRENT_RESERVED_BYTES),
            &self.current_reserved_bytes,
        );
        // The peak is the one figure named per partition. The collector groups by name and sums
        // gauges, and a sum of two peaks is neither peak: partitions peak at different moments.
        // Summing the other gauges and counts stays meaningful — the memory held now and the
        // samples seen add up across partitions. `groups` does not: a `Partial` aggregate sees the
        // same group in every partition it reaches, so the sum is per-partition entries, not
        // distinct groups.
        gauge(
            set,
            partition,
            path.partitioned_metric_name(NodeMetric::PEAK_RESERVED_BYTES, partition),
            &self.peak_reserved_bytes,
        );
        gauge(
            set,
            partition,
            path.metric_name(NodeMetric::GROUPS),
            &self.groups,
        );
        gauge(
            set,
            partition,
            path.metric_name(NodeMetric::STATE_ENTRIES),
            &self.state_entries,
        );
        MetricBuilder::new(set)
            .with_partition(partition)
            .build(MetricValue::Time {
                name: path.metric_name(NodeMetric::SELF_COMPUTE_TIME_NS).into(),
                time: self.self_compute_time.clone(),
            });
    }

    /// Records that this node ingested `batches` batches carrying `samples` sample positions.
    pub fn record_input(&self, batches: usize, samples: usize) {
        self.input_batches.add(batches);
        self.input_samples.add(samples);
    }

    /// Records that this node produced `batches` batches carrying `values` valid values.
    pub fn record_output(&self, batches: usize, values: usize) {
        self.output_batches.add(batches);
        self.output_values.add(values);
    }

    /// Records that this node evaluated `points` grid positions.
    pub fn record_grid_points(&self, points: usize) {
        self.evaluated_grid_points.add(points);
    }

    /// Records that this node closed `series` series.
    pub fn record_completed_series(&self, series: usize) {
        self.completed_series.add(series);
    }

    /// Records that this node now reserves `bytes`.
    ///
    /// The peak is a high-water mark of this node in this partition, never a sum of the peaks of
    /// different nodes: those peaks can happen at different times.
    pub fn record_reserved_bytes(&self, bytes: usize) {
        self.current_reserved_bytes.set(bytes);
        // A high-water mark, not a sum: two nodes peak at different moments.
        self.peak_reserved_bytes.set_max(bytes);
    }

    /// Records the size of the grouping and state this node holds.
    pub fn record_state(&self, groups: usize, entries: usize) {
        self.groups.set(groups);
        self.state_entries.set(entries);
    }

    /// Starts timing this node's own work.
    ///
    /// The returned timer must be dropped before the node polls its input: a node's self time
    /// excludes the work its inputs do.
    pub fn timer(&self) -> datafusion::physical_plan::metrics::ScopedTimerGuard<'_> {
        self.self_compute_time.timer()
    }
}

/// Registers every node of `root` in `set` and returns the handles in traversal order.
pub fn register_tree(
    root: &NativeNode,
    set: &ExecutionPlanMetricsSet,
    partition: usize,
) -> Vec<(NodePath, Arc<NodeMetrics>)> {
    let mut out = Vec::new();
    walk(root, &mut |_, path| {
        let metrics = Arc::new(NodeMetrics::default());
        metrics.register(path, set, partition);
        out.push((path.clone(), metrics));
    });
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue};
    use datatypes::series::samples::SeriesSchema;
    use datatypes::series::time::{EvalGrid, GridSpec};

    use super::{NodeMetric, NodePath, register_tree, walk};
    use crate::map::MathOp;
    use crate::native::plan::{
        BoundInput, BoundMap, BoundWindow, NativeExpr, NativeNode, NativeType,
    };
    use crate::range_eval::{RangeFunction, RangeWindow};

    fn grid() -> Arc<EvalGrid> {
        Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 60_000, 4).unwrap()).unwrap())
    }

    fn schema() -> Arc<SeriesSchema> {
        Arc::new(SeriesSchema {
            fields: Arc::new([]),
            eval_grid: None,
        })
    }

    fn input() -> NativeNode {
        let output = NativeType::Samples(schema());
        NativeNode {
            expr: NativeExpr::Input(BoundInput {
                output: output.clone(),
                ordering: datatypes::series::SeriesOrdering::KeyThenTime,
            }),
            output,
        }
    }

    fn window(inner: NativeNode) -> NativeNode {
        let grid = grid();
        NativeNode {
            expr: NativeExpr::Window {
                input: Box::new(inner),
                op: BoundWindow {
                    function: RangeFunction::Rate,
                    window: RangeWindow {
                        range_ms: 300_000,
                        offset_ms: 0,
                        anchor_ms: None,
                        modifier: None,
                    },
                    input_grid: None,
                    output_grid: Arc::clone(&grid),
                },
            },
            output: NativeType::Samples(Arc::new(SeriesSchema {
                fields: Arc::new([]),
                eval_grid: Some(grid),
            })),
        }
    }

    fn map(inner: NativeNode) -> NativeNode {
        let grid = inner.output.grid().expect("a map needs a grid").clone();
        NativeNode {
            expr: NativeExpr::Map {
                input: Box::new(inner),
                op: BoundMap {
                    ops: Arc::new([MathOp::Abs]),
                },
            },
            output: NativeType::Samples(Arc::new(SeriesSchema {
                fields: Arc::new([]),
                eval_grid: Some(grid),
            })),
        }
    }

    /// A unary chain is addressed by `.input`, so a metric can be attributed to one node.
    #[test]
    fn a_unary_chain_is_addressed_by_input_steps() {
        let root = map(window(input()));
        let mut paths = Vec::new();
        walk(&root, &mut |_, path| paths.push(path.to_string()));
        assert_eq!(paths, vec!["root", "root.input", "root.input.input"]);
    }

    /// A binary node addresses its two sides by name, so the two `rate` subtrees stay distinct.
    #[test]
    fn a_binary_node_addresses_both_sides() {
        let binary = crate::native::plan::NativeNode {
            expr: NativeExpr::Binary {
                left: Box::new(window(input())),
                right: Box::new(map(window(input()))),
                matching: crate::vector_match::BinaryMatching {
                    op: crate::vector_match::BinaryOp::Add,
                    return_bool: false,
                    labels: crate::vector_match::MatchingLabels::Ignoring(Vec::new()),
                    cardinality: crate::vector_match::Cardinality::OneToOne,
                    fill: crate::vector_match::FillValues::default(),
                },
            },
            output: {
                let grid = grid();
                NativeType::Samples(Arc::new(SeriesSchema {
                    fields: Arc::new([]),
                    eval_grid: Some(grid),
                }))
            },
        };
        let mut paths = Vec::new();
        walk(&binary, &mut |_, path| paths.push(path.to_string()));
        assert_eq!(
            paths,
            vec![
                "root",
                "root.left",
                "root.left.input",
                "root.right",
                "root.right.input",
                "root.right.input.input",
            ]
        );
    }

    /// The name carries the position because the collector aggregates by name; a bare name would
    /// merge two nodes of the same operation into one number.
    #[test]
    fn a_metric_name_carries_its_position() {
        let path = NodePath::root().left().input();
        assert_eq!(path.as_str(), "root.left.input");
        assert_eq!(
            path.metric_name(NodeMetric::SELF_COMPUTE_TIME_NS),
            "native.root.left.input.self_compute_time_ns"
        );
        assert_eq!(
            path.partitioned_metric_name(NodeMetric::PEAK_RESERVED_BYTES, 1),
            "native.p1.root.left.input.peak_reserved_bytes"
        );
    }

    /// The peak keeps the partition in its name, so the collector's aggregation by name cannot add
    /// two partitions' peaks: 1024 and 2048 peak at different moments, and 3072 is neither peak.
    #[test]
    fn a_peak_is_not_merged_across_partitions() {
        let root = map(window(input()));
        let set = ExecutionPlanMetricsSet::new();
        let peaks = [(0, 1024), (1, 2048)];
        for (partition, peak) in peaks {
            let registered = register_tree(&root, &set, partition);
            // The root comes first in traversal order.
            registered[0].1.record_reserved_bytes(peak);
        }

        // The real collector path: `aggregate_by_name` groups by metric name and sums the
        // values in each group.
        let aggregated = set.clone_inner().aggregate_by_name();
        let peak = |partition: usize| -> Option<usize> {
            let expected = NodePath::root()
                .partitioned_metric_name(NodeMetric::PEAK_RESERVED_BYTES, partition);
            aggregated.iter().find_map(|metric| match metric.value() {
                MetricValue::Gauge { name, gauge } if name.as_ref() == expected.as_str() => {
                    Some(gauge.value())
                }
                _ => None,
            })
        };
        // Two separate names, each holding its own partition's peak: had the name not carried the
        // partition, aggregation would have merged them into one gauge summing to 3072.
        assert_eq!(peak(0), Some(1024));
        assert_eq!(peak(1), Some(2048));
    }

    /// Registration covers every node, so no node is executed without a way to observe it.
    ///
    /// The set is read back rather than named again from the tree, so this checks what
    /// registration actually did.
    #[test]
    fn registration_covers_every_node() {
        let root = map(window(input()));
        let set = datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new();
        let registered = register_tree(&root, &set, 2);
        assert_eq!(registered.len(), 3);

        // Read the names back from the set registration wrote to, so the check is against what
        // registration did rather than against the names built a second time here.
        let set = set.clone_inner();
        let names: Vec<&str> = set.iter().map(|metric| metric.value().name()).collect();
        // One entry per node per declared metric, and every node's position among them: no node
        // is executed without a way to observe it.
        assert_eq!(names.len(), registered.len() * NodeMetric::ALL.len());
        for (path, _) in &registered {
            // The peak is named per partition, so a plain name stands for the node here.
            let name = path.metric_name(NodeMetric::OUTPUT_VALUES);
            assert!(
                names.contains(&name.as_str()),
                "{path} registered nothing under its own position"
            );
        }
    }

    /// Every metric a node reports is listed in one place, so registration cannot drift from the
    /// documented set.
    #[test]
    fn every_metric_is_declared() {
        assert_eq!(NodeMetric::ALL.len(), 11);
        assert!(NodeMetric::ALL.contains(&NodeMetric::SELF_COMPUTE_TIME_NS));
        assert!(NodeMetric::ALL.contains(&NodeMetric::PEAK_RESERVED_BYTES));
    }

    /// Registration reports exactly the metrics `ALL` declares, in both directions.
    ///
    /// The names are read back from the set a node registered in, so the check is against what
    /// registration actually did, not against `ALL` itself. Adding a metric to `ALL` without
    /// registering it in `register`, or registering a metric that `ALL` does not list, fails this
    /// test: it compares the two sides instead of trusting either.
    #[test]
    fn registered_metrics_are_exactly_the_declared_set() {
        let root = map(window(input()));
        let set = ExecutionPlanMetricsSet::new();
        register_tree(&root, &set, 0);

        // One node reports each metric once; the node paths and the partition prefix separate
        // nodes, so the bare name is the last dot-separated segment.
        let set = set.clone_inner();
        let mut registered: Vec<&str> = set
            .iter()
            .map(|metric| {
                metric
                    .value()
                    .name()
                    .rsplit('.')
                    .next()
                    .expect("a metric name has a last segment")
            })
            .collect();
        registered.sort_unstable();
        registered.dedup();

        let mut declared: Vec<&str> = NodeMetric::ALL.to_vec();
        declared.sort_unstable();

        assert_eq!(registered, declared);
    }

    /// A peak is a high-water mark of one node, so it survives a later drop in reservations.
    #[test]
    fn a_peak_is_a_high_water_mark() {
        let metrics = super::NodeMetrics::default();
        metrics.record_reserved_bytes(1024);
        metrics.record_reserved_bytes(64);
        assert_eq!(metrics.peak_reserved_bytes.value(), 1024);
        assert_eq!(metrics.current_reserved_bytes.value(), 64);
    }
}
