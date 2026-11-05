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

//! Per-node metric handles for native PromQL execution and their DataFusion registration.
//!
//! Node metrics retain no names or tree topology: [`NodeMetrics::register`] names the handles for
//! one registration, and the set holds them from there. What outlives an execution is therefore its
//! counters and gauges, not the nodes that recorded them.
//!
//! The name carries the node's position in the runtime tree: the whole native tree registers into
//! the one [`ExecutionPlanMetricsSet`] the enclosing physical operator (`FusedSeriesExec`) owns, and
//! the reader aggregates by name, so two nodes of a kind sharing
//! a name would be summed into a single figure. What a consumer can rely on is the grammar
//! `native.<path>.<metric>` with the suffixes of [`NodeMetric::ALL`]: splitting off the final
//! component leaves the metric, and the path before it keys the node within that operator. That key
//! is promised only inside the topology that produced it:
//!
//! - the path names a node of one runtime topology;
//! - queries, plan rewrites, and versions move it, so it is neither a persisted nor a public
//!   identifier;
//! - `peak_reserved_bytes` alone carries `p<partition>`, because the reader sums gauges by name and
//!   a sum of peaks is not a peak.

use std::collections::HashMap;

use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Time,
};

/// The reserved first segment of every native metric name.
const NATIVE_PREFIX: &str = "native";

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

/// The figures every node of one execution left, keyed by the path it was registered under.
///
/// A path is the one [`NodeMetrics::register`] was called with (`root`, `root.left`, ...), which is
/// exactly what the runtime builds while it compiles the tree: a reader that walks the *same* tree
/// reaches the same path for the same node, so a figure is found rather than guessed.
///
/// Partitions are combined here because a node is registered once per partition: counts and times
/// add up, and `peak_reserved_bytes` — the one figure whose name carries its partition, because a
/// sum of peaks is not a peak — is the highest of them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeMetricsSnapshot {
    nodes: HashMap<String, Vec<(String, usize)>>,
}

impl NodeMetricsSnapshot {
    /// Reads every native node figure in `set`.
    ///
    /// `set` may hold other metrics, and holds nothing at all before an execution: a snapshot of a
    /// plan that never ran is empty, which is how a plain `EXPLAIN` shows a tree without figures.
    ///
    /// The partitions of one node are combined here rather than by
    /// [`MetricsSet::aggregate_by_name`](datafusion::physical_plan::metrics::MetricsSet::aggregate_by_name),
    /// which adds at least one nanosecond to every time it touches: a node whose timer never ran
    /// would then report time it did not spend.
    pub fn read(set: &ExecutionPlanMetricsSet) -> Self {
        let mut nodes: HashMap<String, Vec<(String, usize)>> = HashMap::new();
        for metric in set.clone_inner().iter() {
            let Some((path, name)) = split_name(metric.value().name()) else {
                continue;
            };
            let value = metric.value().as_usize();
            let figures = nodes.entry(path.to_string()).or_default();
            match figures.iter_mut().find(|(figure, _)| figure == name) {
                // A peak is per partition because partitions peak at different moments: the node's
                // peak is the highest of theirs, never their sum.
                Some((_, accumulated)) if name == NodeMetric::PEAK_RESERVED_BYTES => {
                    *accumulated = (*accumulated).max(value)
                }
                Some((_, accumulated)) => *accumulated = accumulated.saturating_add(value),
                None => figures.push((name.to_string(), value)),
            }
        }
        for figures in nodes.values_mut() {
            figures.sort_by_key(|(name, _)| node_metric_order(name));
        }
        Self { nodes }
    }

    /// What this node recorded, as `name: value`, dropping the figures that are zero.
    ///
    /// A node registers every figure it may record, so a node that did one thing would otherwise
    /// report ten things it did not: the tree would stop reading as a tree. A node that recorded
    /// nothing therefore reports nothing, the same way one that never ran does.
    pub fn described(&self, path: &str) -> Vec<(String, usize)> {
        self.of(path)
            .iter()
            .filter(|(_, value)| *value != 0)
            .cloned()
            .collect()
    }

    /// What this node recorded, in [`NodeMetric::ALL`] order, or an empty list when it recorded
    /// nothing.
    pub fn of(&self, path: &str) -> &[(String, usize)] {
        self.nodes.get(path).map(Vec::as_slice).unwrap_or_default()
    }
}

/// Splits a registered metric name into the node path and the figure it reports.
///
/// A name is `native.<path>.<metric>`, and `native.p<partition>.<path>.<metric>` for a figure that
/// is registered per partition. The partition belongs to that registration rather than to the
/// node, so it is dropped here and its values are combined by [`NodeMetricsSnapshot::read`]. A name
/// that is not one of this module's is not a node figure, and is left to whoever else reads `set`.
fn split_name(name: &str) -> Option<(&str, &str)> {
    let rest = name.strip_prefix(NATIVE_PREFIX)?.strip_prefix('.')?;
    let rest = match rest.strip_prefix('p').and_then(|rest| rest.split_once('.')) {
        Some((partition, rest))
            if !partition.is_empty() && partition.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            rest
        }
        _ => rest,
    };
    let (path, metric) = rest.rsplit_once('.')?;
    NodeMetric::ALL.contains(&metric).then_some((path, metric))
}

/// Where `metric` sits in the declared order, so a node's figures read the same on every node and
/// on every run.
fn node_metric_order(metric: &str) -> usize {
    NodeMetric::ALL
        .iter()
        .position(|declared| *declared == metric)
        .unwrap_or(usize::MAX)
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

/// Whether `name` is a figure a node of a native tree reports, rather than one of an operator.
pub fn is_node_metric(name: &str) -> bool {
    name.strip_prefix(NATIVE_PREFIX)
        .is_some_and(|rest| rest.starts_with('.'))
}

/// The full metric name for `metric` reported by the node called `name`.
fn named(name: &str, metric: &str) -> String {
    format!("{NATIVE_PREFIX}.{name}.{metric}")
}

/// The full metric name for `metric` in `partition` of the node called `name`.
///
/// A figure that must not be merged across partitions needs the partition in its name: the
/// collector aggregates by name first, and a sum of peaks is not a peak.
fn partitioned(name: &str, metric: &str, partition: usize) -> String {
    format!("{NATIVE_PREFIX}.p{partition}.{name}.{metric}")
}

impl NodeMetrics {
    /// Registers this node's handles under `name` into `set`.
    ///
    /// A caller registers once per execution, before that execution starts, so what `set` holds
    /// afterwards follows what this node records as it runs. The name belongs to that registration
    /// and is not retained here. `partition` is part of the DataFusion registration, and the peak
    /// name includes it because peaks must not be summed across partitions.
    pub fn register(&self, name: &str, set: &ExecutionPlanMetricsSet, partition: usize) {
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
            named(name, NodeMetric::INPUT_BATCHES),
            &self.input_batches,
        );
        count(
            set,
            partition,
            named(name, NodeMetric::OUTPUT_BATCHES),
            &self.output_batches,
        );
        count(
            set,
            partition,
            named(name, NodeMetric::INPUT_SAMPLES),
            &self.input_samples,
        );
        count(
            set,
            partition,
            named(name, NodeMetric::EVALUATED_GRID_POINTS),
            &self.evaluated_grid_points,
        );
        count(
            set,
            partition,
            named(name, NodeMetric::OUTPUT_VALUES),
            &self.output_values,
        );
        count(
            set,
            partition,
            named(name, NodeMetric::COMPLETED_SERIES),
            &self.completed_series,
        );
        gauge(
            set,
            partition,
            named(name, NodeMetric::CURRENT_RESERVED_BYTES),
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
            partitioned(name, NodeMetric::PEAK_RESERVED_BYTES, partition),
            &self.peak_reserved_bytes,
        );
        gauge(
            set,
            partition,
            named(name, NodeMetric::GROUPS),
            &self.groups,
        );
        gauge(
            set,
            partition,
            named(name, NodeMetric::STATE_ENTRIES),
            &self.state_entries,
        );
        MetricBuilder::new(set)
            .with_partition(partition)
            .build(MetricValue::Time {
                name: named(name, NodeMetric::SELF_COMPUTE_TIME_NS).into(),
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

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::metrics::{
        Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue,
    };

    use super::{NodeMetric, NodeMetrics, NodeMetricsSnapshot};

    fn metrics_named(set: &ExecutionPlanMetricsSet) -> Vec<String> {
        set.clone_inner()
            .iter()
            .map(|metric| metric.value().name().to_string())
            .collect()
    }

    #[test]
    fn fresh_exports_register_exactly_the_declared_metrics_without_growth() {
        assert_eq!(NodeMetric::ALL.len(), 11);
        let metrics = NodeMetrics::default();
        for _ in 0..2 {
            let set = ExecutionPlanMetricsSet::new();
            metrics.register("root", &set, 0);
            let names = metrics_named(&set);
            assert_eq!(names.len(), NodeMetric::ALL.len());
            let mut names: Vec<_> = names
                .iter()
                .map(|name| name.rsplit('.').next().unwrap().to_string())
                .collect();
            names.sort();
            let mut declared: Vec<_> = NodeMetric::ALL
                .iter()
                .map(|name| name.to_string())
                .collect();
            declared.sort();
            assert_eq!(names, declared);
        }
    }

    #[test]
    fn exported_counters_have_recorded_values() {
        let metrics = NodeMetrics::default();
        metrics.record_input(3, 17);
        metrics.record_output(2, 11);
        let set = ExecutionPlanMetricsSet::new();
        metrics.register("root", &set, 0);
        let registered = set.clone_inner();
        let counter = |name: &str| {
            registered.iter().find_map(|metric| match metric.value() {
                MetricValue::Count {
                    name: metric_name,
                    count,
                } if metric_name.as_ref() == name => Some(count.value()),
                _ => None,
            })
        };
        assert_eq!(counter("native.root.input_batches"), Some(3));
        assert_eq!(counter("native.root.input_samples"), Some(17));
        assert_eq!(counter("native.root.output_batches"), Some(2));
        assert_eq!(counter("native.root.output_values"), Some(11));
    }

    /// Each partition's peak is exported under a name of its own, from handles of its own.
    ///
    /// A [`NodeMetrics`] belongs to one node in one partition, which is the shape one runtime tree
    /// per partition gives. Two partitions are therefore two sets of handles that grow
    /// independently: what the second one records never reaches the figure the first one exported,
    /// and the peak of each is reported under the partition it belongs to rather than summed under
    /// one shared name.
    #[test]
    fn peak_exports_are_distinct_per_partition() {
        let first_metrics = NodeMetrics::default();
        first_metrics.record_reserved_bytes(1024);
        let first = ExecutionPlanMetricsSet::new();
        first_metrics.register("root", &first, 0);
        // The second partition's node is another set of handles, recorded and exported later: the
        // first export above must not follow it.
        let second_metrics = NodeMetrics::default();
        second_metrics.record_reserved_bytes(2048);
        let second = ExecutionPlanMetricsSet::new();
        second_metrics.register("root", &second, 1);
        let peak = |set: &ExecutionPlanMetricsSet, expected: &str| {
            set.clone_inner()
                .iter()
                .find_map(|metric| match metric.value() {
                    MetricValue::Gauge { name, gauge } if name.as_ref() == expected => {
                        Some(gauge.value())
                    }
                    _ => None,
                })
        };
        assert_eq!(
            peak(&first, "native.p0.root.peak_reserved_bytes"),
            Some(1024)
        );
        assert_eq!(
            peak(&second, "native.p1.root.peak_reserved_bytes"),
            Some(2048)
        );
    }

    /// A node is registered once per partition, so one node's figures are the sum of its
    /// partitions' - except the peak, which is the highest one, because partitions peak at
    /// different moments.
    #[test]
    fn a_node_reports_one_set_of_figures_across_its_partitions() {
        let set = ExecutionPlanMetricsSet::new();
        for (partition, batches, samples, peak) in [(0, 2, 10, 1024), (1, 3, 20, 4096)] {
            let metrics = NodeMetrics::default();
            metrics.record_input(batches, samples);
            metrics.record_reserved_bytes(peak);
            metrics.register("root", &set, partition);
        }

        let snapshot = NodeMetricsSnapshot::read(&set);
        let figure = |name: &str| {
            snapshot
                .of("root")
                .iter()
                .find_map(|(figure, value)| (figure == name).then_some(*value))
                .unwrap_or_else(|| panic!("{name} is reported"))
        };
        assert_eq!(figure(NodeMetric::INPUT_BATCHES), 5);
        assert_eq!(figure(NodeMetric::INPUT_SAMPLES), 30);
        assert_eq!(figure(NodeMetric::PEAK_RESERVED_BYTES), 4096);
    }

    /// The path is what the registration named, and a partition segment is not part of it: the two
    /// partitions above file under one node rather than under `p0.root` and `p1.root`.
    #[test]
    fn a_partition_belongs_to_the_registration_and_not_to_the_path() {
        let set = ExecutionPlanMetricsSet::new();
        NodeMetrics::default().register("root.left", &set, 7);

        let snapshot = NodeMetricsSnapshot::read(&set);
        assert!(!snapshot.of("root.left").is_empty());
        assert!(snapshot.of("root").is_empty());
        assert!(snapshot.of("p7.root.left").is_empty());
    }

    /// A metric this module did not name is not a node figure, so it is not filed under a node.
    #[test]
    fn a_name_outside_the_grammar_is_not_a_node_figure() {
        let set = ExecutionPlanMetricsSet::new();
        let metrics = NodeMetrics::default();
        metrics.register("root", &set, 0);
        MetricBuilder::new(&set).build(MetricValue::Count {
            name: "output_rows".into(),
            count: Count::new(),
        });

        let snapshot = NodeMetricsSnapshot::read(&set);
        assert_eq!(snapshot.of("root").len(), NodeMetric::ALL.len());
        assert!(
            snapshot
                .of("root")
                .iter()
                .all(|(name, _)| NodeMetric::ALL.contains(&name.as_str()))
        );
    }

    #[test]
    fn a_peak_is_a_high_water_mark() {
        let metrics = NodeMetrics::default();
        metrics.record_reserved_bytes(1024);
        metrics.record_reserved_bytes(64);
        assert_eq!(metrics.peak_reserved_bytes.value(), 1024);
        assert_eq!(metrics.current_reserved_bytes.value(), 64);
    }
}
