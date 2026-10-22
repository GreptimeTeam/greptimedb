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

//! Local float aggregation over evaluated series, following Prometheus' `aggregation`. State
//! is keyed by absolute grid index, never by local row number; sparse state and trimmed output
//! do not expand the full grid.

mod state;
#[cfg(test)]
mod tests;

pub use state::{
    AccumulatorState, FloatMerger, PartialAggregate, PartialGroup, StateField, merge_count_values,
};

use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};

use crate::go_format::format_float_f;
use crate::grid_series::{GridSeriesBuilder, external};
use crate::histogram_eval::histogram_string;
use crate::map::{drop_empty_labels, validate_float_series, validate_grid_series, validate_scalar};
use crate::range_eval::quantile;

/// Float aggregations without a parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateOp {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Group,
    Stddev,
    Stdvar,
}

/// Grouping by series labels. No modifier is `By(&[])`, as in Prometheus.
#[derive(Clone, Copy, Debug)]
pub enum Grouping<'a> {
    By(&'a [&'a str]),
    Without(&'a [&'a str]),
}

/// Aggregates each field at each absolute evaluation instant.
///
/// Each field is aggregated independently; an output row exists where any field has a value,
/// and a field without input values at that row is null. Ordinary NaNs count as values.
///
/// Inputs are folded in slice order. Prometheus folds them in storage postings order, and the
/// compensated `sum`/`avg` depend on that order in the last bits, so callers must keep it.
/// Groups are returned in the order their first input appears, as in Prometheus: a nested
/// aggregation folds them in that order.
///
/// Only float evaluated inputs on the shared schema/grid are admitted. `by` on a value field
/// needs per-point labels and an output schema adapter, and is rejected. There is no fallback.
pub fn aggregate_float(
    schema: &Arc<SeriesSchema>,
    input: &[Series],
    op: AggregateOp,
    grouping: Grouping<'_>,
) -> Result<Vec<Series>> {
    let mut aggregator = FloatAggregator::try_new(schema, op, grouping)?;
    for series in input {
        aggregator.update(series)?;
    }
    aggregator.finish()
}

/// [`aggregate_float`] fed one series at a time, for a caller that does not hold all its input
/// at once. Series are folded in the order they arrive, with the same semantics.
///
/// The grouping labels are copied in, so the aggregator borrows nothing and can live in the
/// state of a stream.
pub struct FloatAggregator {
    schema: Arc<SeriesSchema>,
    op: AggregateOp,
    /// `by` (true) or `without`, and its labels.
    by: bool,
    labels: Vec<Arc<str>>,
    group_index: HashMap<LabelKey, usize>,
    groups: Vec<Group>,
}

impl FloatAggregator {
    pub fn try_new(
        schema: &Arc<SeriesSchema>,
        op: AggregateOp,
        grouping: Grouping<'_>,
    ) -> Result<Self> {
        validate_input(schema, &[], grouping)?;
        let (by, labels) = match grouping {
            Grouping::By(labels) => (true, labels),
            Grouping::Without(labels) => (false, labels),
        };
        Ok(Self {
            schema: schema.clone(),
            op,
            by,
            labels: labels.iter().map(|label| Arc::from(*label)).collect(),
            group_index: HashMap::new(),
            groups: Vec::new(),
        })
    }

    /// Folds `series` into its group. A series that fails validation leaves the state unchanged.
    pub fn update(&mut self, series: &Series) -> Result<()> {
        validate_float_series(&self.schema, series)?;
        let labels: Vec<&str> = self.labels.iter().map(|label| &**label).collect();
        let grouping = if self.by {
            Grouping::By(&labels)
        } else {
            Grouping::Without(&labels)
        };
        let metric = grouping.apply(&series.metric)?;
        let index = match self.group_index.entry(label_key(&metric)) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                self.groups.push(Group {
                    metric,
                    blocks: BTreeMap::new(),
                });
                *entry.insert(self.groups.len() - 1)
            }
        };
        self.groups[index].accumulate(series, self.op);
        Ok(())
    }

    /// The result of every group, in the order the groups first appeared.
    pub fn finish(self) -> Result<Vec<Series>> {
        finish_groups(self.schema, self.op, self.groups)
    }
}

/// Turns the state of `groups` into result series, in group order.
fn finish_groups(
    schema: Arc<SeriesSchema>,
    op: AggregateOp,
    groups: Vec<Group>,
) -> Result<Vec<Series>> {
    let width = schema.fields.len();
    let mut builder = GridSeriesBuilder::try_new(schema)?;
    let mut output = Vec::with_capacity(groups.len());
    let mut row = Vec::with_capacity(width);
    for group in groups {
        // One builder serves every group, so each result series starts explicitly.
        builder.begin_series();
        for (&block, states) in &group.blocks {
            // The last block can extend past the grid; only points with a sample are pushed.
            for (offset, point) in states.chunks_exact(width).enumerate() {
                if !point.iter().any(|state| state.seen) {
                    continue;
                }
                row.clear();
                row.extend(point.iter().map(|state| state.finish(op)));
                builder.push(block * BLOCK_POINTS + offset, &row)?;
            }
        }
        output.extend(builder.finish(group.metric)?);
    }
    Ok(output)
}

/// `quantile(q, v)`, with `q` read at each grid point from an unlabelled single-field scalar on
/// the same grid, as Prometheus evaluates the parameter at every step. A point where `q` has no
/// value has no result; `q` outside `[0, 1]` or NaN gives what Prometheus' `quantile` gives.
pub fn aggregate_quantile(
    schema: &Arc<SeriesSchema>,
    input: &[Series],
    q: &Series,
    grouping: Grouping<'_>,
) -> Result<Vec<Series>> {
    validate_input(schema, input, grouping)?;
    let q_axis = validate_scalar(schema, q)?;
    let q: Vec<(usize, f64)> = q_axis
        .positions()
        .zip(q.fields.as_slice()[0].iter())
        .filter_map(|((point, _), sample)| match sample {
            SampleRef::Float(value) => Some((point, value)),
            _ => None,
        })
        .collect();
    // Members keep input order, the order Prometheus appends to its per-step heap.
    let mut group_index = HashMap::new();
    let mut groups: Vec<(MetricName, Vec<&Series>)> = Vec::new();
    for series in input {
        let metric = grouping.apply(&series.metric)?;
        let index = *group_index.entry(label_key(&metric)).or_insert_with(|| {
            groups.push((metric, Vec::new()));
            groups.len() - 1
        });
        groups[index].1.push(series);
    }
    let width = schema.fields.len();
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut output = Vec::with_capacity(groups.len());
    let (mut values, mut sorted, mut row) = (Vec::new(), Vec::new(), Vec::with_capacity(width));
    for (metric, members) in groups {
        builder.begin_series();
        // Each member's next position waits in a min-heap, so a point costs the members present
        // there and not the whole group: a group of short-lived series stays linear in its
        // samples. Ties pop in member order, the input order Prometheus appends values in.
        let mut positions: Vec<_> = members
            .iter()
            .map(|series| series.time.positions())
            .collect();
        // Indexed by field, then member.
        let mut cursors: Vec<Vec<_>> = (0..width)
            .map(|field| {
                members
                    .iter()
                    .map(|series| series.fields.as_slice()[field].cursor())
                    .collect()
            })
            .collect();
        let mut next: BinaryHeap<Reverse<(usize, usize, usize)>> = positions
            .iter_mut()
            .enumerate()
            .filter_map(|(member, positions)| {
                positions
                    .next()
                    .map(|(point, row)| Reverse((point, member, row)))
            })
            .collect();
        let mut present = Vec::new();
        while let Some(&Reverse((point, _, _))) = next.peek() {
            present.clear();
            while let Some(&Reverse((at, member, row))) = next.peek()
                && at == point
            {
                next.pop();
                present.push((member, row));
                if let Some((point, row)) = positions[member].next() {
                    next.push(Reverse((point, member, row)));
                }
            }
            let q = q
                .binary_search_by_key(&point, |&(point, _)| point)
                .ok()
                .map(|index| q[index].1);
            row.clear();
            for field in &mut cursors {
                values.clear();
                for &(member, member_row) in &present {
                    if let Some(SampleRef::Float(value)) = field[member].at(member_row) {
                        values.push(value);
                    }
                }
                row.push(match q {
                    Some(q) if !values.is_empty() => Some(quantile(q, &values, &mut sorted)),
                    _ => None,
                });
            }
            builder.push(point, &row)?;
        }
        output.extend(builder.finish(metric)?);
    }
    Ok(output)
}

/// `count_values(label, v)`: at each grid point, the number of samples in each group of series
/// that share a value, with the value written to `label` in Prometheus' `FormatFloat(v, 'f', -1)`
/// form, or for a native histogram as its `FloatHistogram.String`. With `by`, `label` is part of
/// the grouping; with `without`, it is dropped if listed.
pub fn count_values(
    schema: &Arc<SeriesSchema>,
    input: &[Series],
    label: &str,
    grouping: Grouping<'_>,
) -> Result<Vec<Series>> {
    // `by` may list the value label, which this operator writes itself, even when an input field
    // has that name; grouping by any other field name stays unsupported.
    let other_labels: Vec<&str>;
    let checked = match grouping {
        Grouping::By(labels) => {
            other_labels = labels
                .iter()
                .copied()
                .filter(|name| *name != label)
                .collect();
            Grouping::By(&other_labels)
        }
        without => without,
    };
    // Unlike the other aggregations, this one reads histogram samples too.
    validate_grouping(schema, checked)?;
    for series in input {
        validate_grid_series(schema, series)?;
    }
    if label.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "invalid label name {label:?}"
        )));
    }
    let by_labels: Vec<&str>;
    let grouping = match grouping {
        Grouping::By(labels) => {
            by_labels = labels.iter().copied().chain([label]).collect();
            Grouping::By(&by_labels)
        }
        without => without,
    };
    let width = schema.fields.len();
    let mut group_index = HashMap::new();
    // Counts are kept per point that has one, not in blocks: a gauge with distinct values makes
    // about one group per sample.
    let mut groups: Counts = Vec::new();
    for series in input {
        // Equal bits format to the same label value, so each distinct value of a series
        // resolves its group once. A histogram is keyed by the text it is written as.
        let mut group_of_value: HashMap<Value, usize> = HashMap::new();
        for (field_index, field) in series.fields.iter().enumerate() {
            for ((point, _), sample) in series.time.positions().zip(field.iter()) {
                let key = match sample {
                    SampleRef::Missing => continue,
                    SampleRef::Float(value) => Value::Float(value.to_bits()),
                    SampleRef::Histogram(h) => Value::Histogram(histogram_string(&h)?),
                };
                let group = match group_of_value.entry(key) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => {
                        // Prometheus writes a float in Go's `'f'` form and a histogram as
                        // `FloatHistogram.String` does.
                        let text = match entry.key() {
                            Value::Float(bits) => format_float_f(f64::from_bits(*bits)),
                            Value::Histogram(text) => text.clone(),
                        };
                        let mut metric = series.metric.clone();
                        metric.set(label, Arc::from(text)).map_err(external)?;
                        let metric = grouping.apply(&metric)?;
                        let index = *group_index.entry(label_key(&metric)).or_insert_with(|| {
                            groups.push((metric, BTreeMap::new()));
                            groups.len() - 1
                        });
                        *entry.insert(index)
                    }
                };
                groups[group]
                    .1
                    .entry(point)
                    .or_insert_with(|| vec![0.0; width])[field_index] += 1.0;
            }
        }
    }
    count_series(schema, groups)
}

/// Counts per point and field: one series per label set, a count of 0 being no sample.
type Counts = Vec<(MetricName, BTreeMap<usize, Vec<f64>>)>;

fn count_series(schema: &Arc<SeriesSchema>, groups: Counts) -> Result<Vec<Series>> {
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut output = Vec::with_capacity(groups.len());
    let mut row = Vec::with_capacity(schema.fields.len());
    for (metric, counts) in groups {
        builder.begin_series();
        for (point, counts) in counts {
            row.clear();
            row.extend(counts.iter().map(|&count| (count > 0.0).then_some(count)));
            builder.push(point, &row)?;
        }
        output.extend(builder.finish(metric)?);
    }
    Ok(output)
}

/// The value of a `count_values` sample, as it is grouped.
#[derive(PartialEq, Eq, Hash)]
enum Value {
    Float(u64),
    Histogram(String),
}

/// Schema validation is repeated because Series exposes its fields publicly.
fn validate_input(schema: &SeriesSchema, input: &[Series], grouping: Grouping<'_>) -> Result<()> {
    validate_grouping(schema, grouping)?;
    for series in input {
        validate_float_series(schema, series)?;
    }
    Ok(())
}

fn validate_grouping(schema: &SeriesSchema, grouping: Grouping<'_>) -> Result<()> {
    if schema.eval_grid.is_none() {
        return Err(DataFusionError::Execution(
            "Aggregate requires an evaluated grid".into(),
        ));
    }
    grouping.validate(schema)
}

impl Grouping<'_> {
    fn validate(self, schema: &SeriesSchema) -> Result<()> {
        if let Self::By(labels) = self
            && schema
                .fields
                .iter()
                .any(|field| labels.contains(&field.name.as_ref()))
        {
            return Err(DataFusionError::NotImplemented(
                "aggregate by value field requires a per-point label/schema adapter".into(),
            ));
        }
        Ok(())
    }

    /// Prometheus `generateGroupingLabels`; empty values are dropped as absent labels.
    fn apply(self, input: &MetricName) -> Result<MetricName> {
        let mut metric = input.clone();
        match self {
            Self::By(labels) => metric.keep_only(labels),
            Self::Without(labels) => {
                metric.remove_all(labels);
                metric.remove("__name__");
            }
        }
        drop_empty_labels(&metric)
    }
}

type LabelKey = Vec<(Arc<str>, Arc<str>)>;

fn label_key(metric: &MetricName) -> LabelKey {
    let mut labels = Vec::with_capacity(metric.tags().len() + 1);
    if let Some(name) = metric.name() {
        labels.push((Arc::from("__name__"), Arc::from(name)));
    }
    labels.extend(
        metric
            .tags()
            .iter()
            .map(|label| (label.name.clone(), label.value.clone())),
    );
    labels
}

/// Grid points per state block. A block is allocated only once a sample falls in it, so sparse
/// input never expands to the full grid and dense input costs one allocation per block.
const BLOCK_POINTS: usize = 64;

struct Group {
    metric: MetricName,
    /// Keyed by absolute grid index / `BLOCK_POINTS`; each block holds one state per field for
    /// each of its points, point by point.
    blocks: BTreeMap<usize, Box<[FloatState]>>,
}

impl Group {
    fn accumulate(&mut self, series: &Series, op: AggregateOp) {
        let width = series.fields.len();
        let mut pending = Vec::with_capacity(BLOCK_POINTS);
        for (field_index, field) in series.fields.iter().enumerate() {
            // The axis says which absolute positions the field holds and the field iterator
            // supplies them in order, so a sparse axis never turns into its envelope here.
            // `usize::MAX` is the "no block yet" marker; flushing it is a no-op, since it only
            // touches a block once a sample was buffered.
            let mut block = usize::MAX;
            for ((index, _), sample) in series.time.positions().zip(field.iter()) {
                if index / BLOCK_POINTS != block {
                    self.update_block(block, width, field_index, op, &mut pending);
                    block = index / BLOCK_POINTS;
                }
                if let SampleRef::Float(sample) = sample {
                    pending.push((index % BLOCK_POINTS, sample));
                }
            }
            self.update_block(block, width, field_index, op, &mut pending);
        }
    }

    /// Applies one field's samples within a block, so a block costs one lookup per field run.
    fn update_block(
        &mut self,
        block: usize,
        width: usize,
        field_index: usize,
        op: AggregateOp,
        pending: &mut Vec<(usize, f64)>,
    ) {
        if pending.is_empty() {
            return;
        }
        let states = self.blocks.entry(block).or_insert_with(|| {
            vec![FloatState::default(); BLOCK_POINTS * width].into_boxed_slice()
        });
        for (offset, sample) in pending.drain(..) {
            states[offset * width + field_index].update(op, sample);
        }
    }
}

/// The float part of Prometheus' `groupedAggregation`.
#[derive(Clone, Copy, Default)]
struct FloatState {
    seen: bool,
    value: f64,
    mean: f64,
    compensation: f64,
    count: f64,
    incremental_mean: bool,
}

impl FloatState {
    fn update(&mut self, op: AggregateOp, sample: f64) {
        if !self.seen {
            *self = Self {
                seen: true,
                value: match op {
                    // The running sum of squared deviations; Prometheus starts it at NaN for a
                    // non-finite first sample.
                    AggregateOp::Stddev | AggregateOp::Stdvar if !sample.is_finite() => f64::NAN,
                    AggregateOp::Stddev | AggregateOp::Stdvar => 0.0,
                    _ => sample,
                },
                mean: sample,
                count: 1.0,
                ..Self::default()
            };
            return;
        }
        match op {
            AggregateOp::Sum => {
                (self.value, self.compensation) = kahan_inc(sample, self.value, self.compensation);
            }
            AggregateOp::Avg => self.update_avg(sample),
            AggregateOp::Count => self.count += 1.0,
            AggregateOp::Max => {
                if self.value < sample || self.value.is_nan() {
                    self.value = sample;
                }
            }
            AggregateOp::Min => {
                if self.value > sample || self.value.is_nan() {
                    self.value = sample;
                }
            }
            AggregateOp::Group => {}
            // Welford's update, uncompensated as in Prometheus.
            AggregateOp::Stddev | AggregateOp::Stdvar => {
                self.count += 1.0;
                let delta = sample - self.mean;
                self.mean += delta / self.count;
                self.value += delta * (sample - self.mean);
            }
        }
    }

    /// Sums directly until the running sum would overflow, then switches to an incremental mean.
    fn update_avg(&mut self, sample: f64) {
        self.count += 1.0;
        if !self.incremental_mean {
            let (value, compensation) = kahan_inc(sample, self.value, self.compensation);
            if !value.is_infinite() {
                (self.value, self.compensation) = (value, compensation);
                return;
            }
            self.incremental_mean = true;
            self.mean = self.value / (self.count - 1.0);
            self.compensation /= self.count - 1.0;
        }
        let q = (self.count - 1.0) / self.count;
        (self.mean, self.compensation) =
            kahan_inc(sample / self.count, q * self.mean, q * self.compensation);
    }

    fn finish(self, op: AggregateOp) -> Option<f64> {
        if !self.seen {
            return None;
        }
        Some(match op {
            AggregateOp::Sum => self.value + self.compensation,
            AggregateOp::Avg if self.incremental_mean => self.mean + self.compensation,
            AggregateOp::Avg => self.value / self.count + self.compensation / self.count,
            AggregateOp::Count => self.count,
            AggregateOp::Min | AggregateOp::Max => self.value,
            AggregateOp::Group => 1.0,
            AggregateOp::Stdvar => self.value / self.count,
            AggregateOp::Stddev => (self.value / self.count).sqrt(),
        })
    }
}

/// Prometheus `kahansum.Inc`, with the Neumaier swap.
fn kahan_inc(inc: f64, sum: f64, compensation: f64) -> (f64, f64) {
    let t = sum + inc;
    let compensation = if t.is_infinite() {
        0.0
    } else if sum.abs() >= inc.abs() {
        compensation + ((sum - t) + inc)
    } else {
        compensation + ((inc - t) + sum)
    };
    (t, compensation)
}
