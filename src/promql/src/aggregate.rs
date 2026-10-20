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

#[cfg(test)]
mod tests;

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};

use crate::grid_series::{GridSeriesBuilder, external};
use crate::map::{drop_empty_labels, validate_float_series, validate_scalar};
use crate::range_eval::quantile;

/// Float aggregations without a parameter.
#[derive(Clone, Copy, Debug)]
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
    validate_input(schema, input, grouping)?;
    let groups = group_series(input, grouping, |state: &mut FloatState, sample| {
        state.update(op, sample)
    })?;
    build_output(
        schema,
        groups,
        |state| state.seen,
        |state, _| state.finish(op),
    )
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
    let groups = group_series(input, grouping, |values: &mut Vec<f64>, sample| {
        values.push(sample)
    })?;
    let mut sorted = Vec::new();
    build_output(
        schema,
        groups,
        |values| !values.is_empty(),
        |values, point| {
            let index = q.binary_search_by_key(&point, |&(point, _)| point).ok()?;
            Some(quantile(q[index].1, values, &mut sorted))
        },
    )
}

/// `count_values(label, v)`: at each grid point, the number of samples in each group of series
/// that share a value, with the value written to `label` in Prometheus' `FormatFloat(v, 'f', -1)`
/// form. With `by`, `label` is part of the grouping; with `without`, it is dropped if listed.
pub fn count_values(
    schema: &Arc<SeriesSchema>,
    input: &[Series],
    label: &str,
    grouping: Grouping<'_>,
) -> Result<Vec<Series>> {
    validate_input(schema, input, grouping)?;
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
    let mut groups: Vec<Group<f64>> = Vec::new();
    for series in input {
        // Equal bits format to the same label value, so each distinct value of a series
        // resolves its group once.
        let mut group_of_value: HashMap<u64, usize> = HashMap::new();
        for (field_index, field) in series.fields.iter().enumerate() {
            for ((point, _), sample) in series.time.positions().zip(field.iter()) {
                let SampleRef::Float(value) = sample else {
                    continue;
                };
                let group = match group_of_value.entry(value.to_bits()) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => {
                        let mut metric = series.metric.clone();
                        metric
                            .set(label, Arc::from(format_value(value)))
                            .map_err(external)?;
                        let metric = grouping.apply(&metric)?;
                        let index = *group_index.entry(label_key(&metric)).or_insert_with(|| {
                            groups.push(Group::new(metric));
                            groups.len() - 1
                        });
                        *entry.insert(index)
                    }
                };
                groups[group].update_point(point, width, field_index, |count| *count += 1.0);
            }
        }
    }
    build_output(
        schema,
        groups,
        |count| *count > 0.0,
        |count, _| Some(*count),
    )
}

/// Go's `strconv.FormatFloat(value, 'f', -1, 64)`; Rust's shortest round-trip decimal matches it
/// for finite values.
fn format_value(value: f64) -> String {
    match value {
        f64::INFINITY => "+Inf".to_string(),
        f64::NEG_INFINITY => "-Inf".to_string(),
        _ => value.to_string(),
    }
}

/// Groups `input` by `grouping` in first-seen order and folds each series into its group.
fn group_series<S: Clone + Default>(
    input: &[Series],
    grouping: Grouping<'_>,
    mut update: impl FnMut(&mut S, f64),
) -> Result<Vec<Group<S>>> {
    let mut group_index = HashMap::new();
    let mut groups = Vec::new();
    for series in input {
        let metric = grouping.apply(&series.metric)?;
        let index = *group_index.entry(label_key(&metric)).or_insert_with(|| {
            groups.push(Group::new(metric));
            groups.len() - 1
        });
        groups[index].accumulate(series, &mut update);
    }
    Ok(groups)
}

/// Writes one series per group, from the points where any field's state has a sample. `finish`
/// receives the absolute grid index.
fn build_output<S>(
    schema: &Arc<SeriesSchema>,
    groups: Vec<Group<S>>,
    seen: impl Fn(&S) -> bool,
    mut finish: impl FnMut(&S, usize) -> Option<f64>,
) -> Result<Vec<Series>> {
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut output = Vec::with_capacity(groups.len());
    let mut row = Vec::with_capacity(schema.fields.len());
    for group in groups {
        // One builder serves every group, so each result series starts explicitly.
        builder.begin_series();
        for (&block, states) in &group.blocks {
            // The last block can extend past the grid; only points with a sample are pushed.
            for (offset, point) in states.chunks_exact(schema.fields.len()).enumerate() {
                if !point.iter().any(&seen) {
                    continue;
                }
                let index = block * BLOCK_POINTS + offset;
                row.clear();
                row.extend(point.iter().map(|state| {
                    if seen(state) {
                        finish(state, index)
                    } else {
                        None
                    }
                }));
                builder.push(index, &row)?;
            }
        }
        output.extend(builder.finish(group.metric)?);
    }
    Ok(output)
}

/// Schema validation is repeated because Series exposes its fields publicly.
fn validate_input(schema: &SeriesSchema, input: &[Series], grouping: Grouping<'_>) -> Result<()> {
    if schema.eval_grid.is_none() {
        return Err(DataFusionError::Execution(
            "Aggregate requires an evaluated grid".into(),
        ));
    }
    grouping.validate(schema)?;
    for series in input {
        validate_float_series(schema, series)?;
    }
    Ok(())
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

fn label_key(metric: &MetricName) -> Vec<(Arc<str>, Arc<str>)> {
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

struct Group<S> {
    metric: MetricName,
    /// Keyed by absolute grid index / `BLOCK_POINTS`; each block holds one state per field for
    /// each of its points, point by point.
    blocks: BTreeMap<usize, Box<[S]>>,
}

impl<S: Clone + Default> Group<S> {
    fn new(metric: MetricName) -> Self {
        Self {
            metric,
            blocks: BTreeMap::new(),
        }
    }

    fn accumulate(&mut self, series: &Series, update: &mut impl FnMut(&mut S, f64)) {
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
                    self.update_block(block, width, field_index, update, &mut pending);
                    block = index / BLOCK_POINTS;
                }
                if let SampleRef::Float(sample) = sample {
                    pending.push((index % BLOCK_POINTS, sample));
                }
            }
            self.update_block(block, width, field_index, update, &mut pending);
        }
    }

    /// Applies one field's samples within a block, so a block costs one lookup per field run.
    fn update_block(
        &mut self,
        block: usize,
        width: usize,
        field_index: usize,
        update: &mut impl FnMut(&mut S, f64),
        pending: &mut Vec<(usize, f64)>,
    ) {
        if pending.is_empty() {
            return;
        }
        let states = self.block(block, width);
        for (offset, sample) in pending.drain(..) {
            update(&mut states[offset * width + field_index], sample);
        }
    }

    /// Updates the state of one field at absolute grid `point`.
    fn update_point(
        &mut self,
        point: usize,
        width: usize,
        field_index: usize,
        update: impl FnOnce(&mut S),
    ) {
        let states = self.block(point / BLOCK_POINTS, width);
        update(&mut states[point % BLOCK_POINTS * width + field_index]);
    }

    fn block(&mut self, block: usize, width: usize) -> &mut [S] {
        self.blocks
            .entry(block)
            .or_insert_with(|| vec![S::default(); BLOCK_POINTS * width].into_boxed_slice())
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
                    AggregateOp::Group => 1.0,
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
            AggregateOp::Min | AggregateOp::Max | AggregateOp::Group => self.value,
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
