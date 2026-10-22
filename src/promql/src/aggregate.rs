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
use std::mem::size_of;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::arrow::array::Float64Array;
use datatypes::series::MetricName;
use datatypes::series::samples::{SampleRef, Samples, Series, SeriesSchema};
use datatypes::series::time::{GridSpan, TimeAxis};

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
    retained: usize,
}

impl FloatAggregator {
    pub fn try_new(
        schema: &Arc<SeriesSchema>,
        op: AggregateOp,
        grouping: Grouping<'_>,
    ) -> Result<Self> {
        Self::try_new_budgeted(schema, op, grouping, |_| Ok(()))
    }

    pub(crate) fn try_new_budgeted(
        schema: &Arc<SeriesSchema>,
        op: AggregateOp,
        grouping: Grouping<'_>,
        mut budget: impl FnMut(usize) -> Result<()>,
    ) -> Result<Self> {
        validate_input(schema, &[], grouping)?;
        let (by, labels) = match grouping {
            Grouping::By(labels) => (true, labels),
            Grouping::Without(labels) => (false, labels),
        };
        let label_storage =
            grouping_bytes(grouping).saturating_add(labels_capacity_bytes(grouping));
        budget(label_storage)?;
        let mut owned_labels: Vec<Arc<str>> = Vec::new();
        owned_labels
            .try_reserve_exact(labels.len())
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "cannot reserve aggregate grouping labels: {error}"
                ))
            })?;
        owned_labels.extend(labels.iter().map(|label| Arc::from(*label)));
        let retained = owned_labels
            .capacity()
            .saturating_mul(size_of::<Arc<str>>())
            .saturating_add(owned_labels.iter().fold(0usize, |bytes, label| {
                bytes.saturating_add(label.len() + 2 * size_of::<usize>())
            }));
        Ok(Self {
            schema: schema.clone(),
            op,
            by,
            labels: owned_labels,
            group_index: HashMap::new(),
            groups: Vec::new(),
            retained,
        })
    }

    /// Folds `series` into its group. A series that fails validation leaves the state unchanged.
    ///
    /// A budget refusal is not a validation failure: the fold may have opened a group or filled
    /// blocks before the allocation it could not admit, so a failed [`Self::update_budgeted`]
    /// leaves the state half folded and its retained figure stale. The runtime stops the execution
    /// and drops the aggregator on that error, which is why no rollback is kept; a caller holding
    /// one must drop it rather than fold into it again.
    pub fn update(&mut self, series: &Series) -> Result<()> {
        self.update_budgeted(series, |_| Ok(()))
    }

    pub(crate) fn update_budgeted(
        &mut self,
        series: &Series,
        mut budget: impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        validate_float_series(&self.schema, series)?;
        let existing_bytes = self.state_bytes();
        let grouping_workspace = grouping_workspace_bytes(&series.metric);
        let mut workspace_bytes =
            grouping_workspace.saturating_add(pending_bytes(self.schema.fields.len()));
        budget(existing_bytes.saturating_add(workspace_bytes))?;
        workspace_bytes = workspace_bytes.saturating_add(
            self.labels
                .len()
                .saturating_mul(std::mem::size_of::<&str>()),
        );
        budget(existing_bytes.saturating_add(workspace_bytes))?;
        let labels: Vec<&str> = self.labels.iter().map(|label| &**label).collect();
        let grouping = if self.by {
            Grouping::By(&labels)
        } else {
            Grouping::Without(&labels)
        };
        let metric = grouping.apply(&series.metric)?;
        let key_bytes = metric
            .tags()
            .len()
            .saturating_add(1)
            .saturating_mul(std::mem::size_of::<(Arc<str>, Arc<str>)>());
        workspace_bytes = workspace_bytes
            .saturating_add(grouping_workspace_bytes(&metric))
            .saturating_add(key_bytes);
        budget(existing_bytes.saturating_add(workspace_bytes))?;
        let key = label_key(&metric);
        let key_retained = key
            .capacity()
            .saturating_mul(size_of::<(Arc<str>, Arc<str>)>());
        let new_group = !self.group_index.contains_key(&key);
        let group_capacity = self.groups.capacity();
        let map_capacity = self.group_index.capacity();
        let old_map_backing = hash_map_backing_bytes(map_capacity);
        if new_group {
            let next_group_capacity = if self.groups.len() == group_capacity {
                group_capacity.saturating_mul(2).max(4)
            } else {
                group_capacity
            };
            let group_realloc_bytes = if next_group_capacity > group_capacity {
                next_group_capacity.saturating_mul(size_of::<Group>())
            } else {
                0
            };
            budget(
                existing_bytes
                    .saturating_add(metric_bytes(&metric))
                    .saturating_add(key_bytes)
                    // A realloc that cannot grow in place keeps both buffers alive while it
                    // copies, so a growing step admits the old buffer beside the new one --
                    // the same reason the map below admits both tables. The old buffer alone
                    // is already in `existing_bytes`; what is added here is the new one.
                    .saturating_add(group_realloc_bytes)
                    .saturating_add(if self.group_index.len() == map_capacity {
                        hash_map_backing_bytes(next_hash_capacity(map_capacity))
                    } else {
                        0
                    })
                    .saturating_add(workspace_bytes),
            )?;
        }
        let group_capacity_growth = if new_group && self.groups.len() == group_capacity {
            group_capacity.saturating_mul(2).max(4)
        } else {
            group_capacity
        };
        if new_group && group_capacity_growth > group_capacity {
            self.groups.reserve(1);
        }
        if new_group && self.group_index.len() == self.group_index.capacity() {
            self.group_index.reserve(1);
        }
        let index = match self.group_index.entry(key) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                self.groups.push(Group {
                    metric: metric.clone(),
                    blocks: BTreeMap::new(),
                });
                let index = self.groups.len() - 1;
                entry.insert(index);
                index
            }
        };
        if new_group {
            self.retained = self
                .retained
                .saturating_add(metric_bytes(&metric))
                .saturating_add(key_retained)
                .saturating_add(
                    self.groups
                        .capacity()
                        .saturating_sub(group_capacity)
                        .saturating_mul(size_of::<Group>()),
                )
                .saturating_add(
                    hash_map_backing_bytes(self.group_index.capacity())
                        .saturating_sub(old_map_backing),
                );
        }
        drop(labels);
        drop(metric);
        self.retained = self.groups[index].accumulate(
            series,
            self.op,
            &mut budget,
            self.retained,
            self.schema.fields.len(),
        )?;
        Ok(())
    }

    /// Number of groups currently retained by this fold.
    pub(crate) fn groups(&self) -> usize {
        self.groups.len()
    }

    /// Bytes reserved for this fold, using bounded allowances for its maps and group payloads.
    pub(crate) fn state_bytes(&self) -> usize {
        self.retained
    }

    #[cfg(test)]
    pub(crate) fn recounted_state_bytes(&self) -> usize {
        let groups = self.groups.iter().fold(0usize, |total, group| {
            total
                .saturating_add(metric_bytes(&group.metric))
                .saturating_add(
                    group
                        .blocks
                        .len()
                        .saturating_mul(BLOCK_POINTS)
                        .saturating_mul(self.schema.fields.len())
                        .saturating_mul(std::mem::size_of::<FloatState>()),
                )
                .saturating_add(btree_backing_bytes(group.blocks.len()))
        });
        groups
            .saturating_add(
                self.labels
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Arc<str>>()),
            )
            .saturating_add(hash_map_backing_bytes(self.group_index.capacity()))
            .saturating_add(self.group_index.keys().fold(0usize, |bytes, key| {
                bytes.saturating_add(
                    key.capacity()
                        .saturating_mul(size_of::<(Arc<str>, Arc<str>)>()),
                )
            }))
            .saturating_add(
                self.labels
                    .iter()
                    .map(|label| label.len() + 2 * std::mem::size_of::<usize>())
                    .sum::<usize>(),
            )
            .saturating_add(
                self.groups
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Group>()),
            )
    }

    /// The result of every group, in the order the groups first appeared.
    pub fn finish(self) -> Result<Vec<Series>> {
        self.finish_budgeted(|_| Ok(()))
    }

    pub(crate) fn finish_budgeted(
        self,
        budget: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<Series>> {
        let state = self.state_bytes();
        finish_groups_budgeted(self.schema, self.op, self.groups, state, budget)
    }
}

/// Turns the state of `groups` into result series, in group order, admitting as it builds.
fn finish_groups_budgeted(
    schema: Arc<SeriesSchema>,
    op: AggregateOp,
    groups: Vec<Group>,
    state: usize,
    mut budget: impl FnMut(usize) -> Result<()>,
) -> Result<Vec<Series>> {
    let width = schema.fields.len();
    let group_count = groups.len();
    if group_count == 0 {
        return Ok(Vec::new());
    }
    let groups = groups.into_iter();
    let mut builder = GridSeriesBuilder::try_new(schema)?;
    let output_capacity_bytes = group_count.saturating_mul(std::mem::size_of::<Series>());
    let row_capacity_bytes = width.saturating_mul(std::mem::size_of::<Option<f64>>());
    let base = state
        .saturating_add(output_capacity_bytes)
        .saturating_add(row_capacity_bytes);
    budget(base)?;
    let mut output = Vec::new();
    output.try_reserve_exact(group_count).map_err(|error| {
        DataFusionError::Execution(format!("cannot reserve aggregate outputs: {error}"))
    })?;
    let mut row = Vec::new();
    row.try_reserve_exact(width).map_err(|error| {
        DataFusionError::Execution(format!("cannot reserve aggregate row: {error}"))
    })?;
    let mut completed_output_bytes = 0usize;
    for group in groups {
        // One builder serves every group, so each result series starts explicitly.
        builder.begin_series();
        let (rows, runs) = group_rows_runs(&group, width);
        let finish_bytes = finish_working_bytes(rows, runs, width);
        budget(
            state
                .saturating_add(output_capacity_bytes)
                .saturating_add(row_capacity_bytes)
                .saturating_add(completed_output_bytes)
                .saturating_add(finish_bytes),
        )?;
        builder.reserve_rows(rows)?;
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
        let finished = builder.finish(group.metric)?;
        completed_output_bytes = completed_output_bytes.saturating_add(
            finished
                .iter()
                .map(crate::grid_series::series_bytes)
                .sum::<usize>(),
        );
        output.extend(finished);
        budget(
            state
                .saturating_add(output_capacity_bytes)
                .saturating_add(row_capacity_bytes)
                .saturating_add(completed_output_bytes),
        )?;
    }
    Ok(output)
}

/// The result of every group, without a budget: the entry the partial-state merger takes, which
/// does not hold a query pool to admit against.
fn finish_groups(
    schema: Arc<SeriesSchema>,
    op: AggregateOp,
    groups: Vec<Group>,
) -> Result<Vec<Series>> {
    finish_groups_budgeted(schema, op, groups, 0, |_| Ok(()))
}

fn group_rows_runs(group: &Group, width: usize) -> (usize, usize) {
    if width == 0 {
        return (0, 0);
    }
    let mut rows = 0usize;
    let mut runs = 0usize;
    let mut previous: Option<usize> = None;
    for (&block, states) in &group.blocks {
        for (offset, point) in states.chunks_exact(width).enumerate() {
            if point.iter().any(|state| state.seen) {
                let index = block.saturating_mul(BLOCK_POINTS).saturating_add(offset);
                rows = rows.saturating_add(1);
                if previous.is_none_or(|previous| previous.checked_add(1) != Some(index)) {
                    runs = runs.saturating_add(1);
                }
                previous = Some(index);
            }
        }
    }
    (rows, runs)
}

fn finish_working_bytes(rows: usize, runs: usize, width: usize) -> usize {
    let values = rows
        .saturating_mul(width)
        .saturating_mul(std::mem::size_of::<f64>());
    let validity_bytes = rows
        .saturating_add(7)
        .checked_div(8)
        .unwrap_or(0)
        .div_ceil(64)
        .saturating_mul(64);
    let fields = crate::grid_series::field_builder_bytes(width).saturating_add(
        width.saturating_mul(
            std::mem::size_of::<Float64Array>()
                .saturating_add(std::mem::size_of::<Samples>())
                .saturating_add(std::mem::size_of::<usize>()),
        ),
    );
    let indices = rows.saturating_mul(std::mem::size_of::<usize>());
    let spans = runs.saturating_mul(std::mem::size_of::<GridSpan>());
    let axis_conversion = runs
        .saturating_mul(
            2 * std::mem::size_of::<GridSpan>() + std::mem::size_of::<Arc<[GridSpan]>>(),
        )
        .saturating_add(rows.saturating_mul(std::mem::size_of::<usize>()));
    values
        .saturating_add(width.saturating_mul(validity_bytes))
        .saturating_add(fields)
        .saturating_add(indices)
        .saturating_add(spans)
        .saturating_add(axis_conversion)
        .saturating_add(std::mem::size_of::<Series>() + std::mem::size_of::<TimeAxis>())
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

fn metric_bytes(metric: &MetricName) -> usize {
    metric
        .name()
        .map_or(0, str::len)
        .saturating_add(
            metric
                .tags()
                .iter()
                .map(|tag| tag.name.len() + tag.value.len())
                .sum::<usize>(),
        )
        .saturating_add(
            metric
                .tags()
                .len()
                .saturating_mul(std::mem::size_of::<(Arc<str>, Arc<str>)>()),
        )
        .saturating_add(std::mem::size_of::<MetricName>())
}

fn grouping_workspace_bytes(metric: &MetricName) -> usize {
    metric_bytes(metric)
        .saturating_add(
            metric
                .tags()
                .len()
                .saturating_add(1)
                .saturating_mul(std::mem::size_of::<(Arc<str>, Arc<str>)>()),
        )
        .saturating_add(std::mem::size_of::<LabelKey>())
}

/// Conservative backing estimate for Rust 1.98.1's insert-only `HashMap` (hashbrown 0.17.1).
/// Bucket count follows its 7/8 load factor; entries, one control byte per bucket and the
/// 16-byte control-group tail are counted. This models requested table storage, not allocator slack.
/// An upper bound on the bytes the group index holds for `capacity` entries.
///
/// The table keeps its entries in a power-of-two number of buckets below `16/7` of the usable
/// capacity, with one control byte beside each bucket. Three buckets per entry therefore covers the
/// table whatever exact growth the map uses, which is what keeps this a bound rather than a model of
/// one implementation: a toolchain that changes how the map grows cannot make it under-count.
fn hash_map_backing_bytes(capacity: usize) -> usize {
    capacity
        .saturating_mul(3)
        .saturating_mul(size_of::<(LabelKey, usize)>() + 1)
}

/// An upper bound on the capacity a map holding `capacity` entries grows to.
fn next_hash_capacity(capacity: usize) -> usize {
    capacity.saturating_mul(2).max(7)
}

/// An upper bound on the bytes one B-tree node holds.
///
/// A node keeps at most eleven key/value pairs and twelve child pointers, plus a header of a few
/// words; the leaf form is smaller than that. The header allowance is generous on purpose, so the
/// bound does not depend on how a toolchain lays the node out.
fn btree_node_bytes() -> usize {
    11 * size_of::<(usize, Box<[FloatState]>)>() + 16 * size_of::<usize>()
}

/// An upper bound on the bytes a B-tree of `entries` holds.
///
/// Every node but the root keeps at least five entries, so `entries / 5` nodes is enough to hold
/// them; a tree of one entry is one node.
fn btree_backing_bytes(entries: usize) -> usize {
    if entries == 0 {
        return 0;
    }
    entries.div_ceil(5).saturating_mul(btree_node_bytes())
}

fn pending_bytes(fields: usize) -> usize {
    working_set_bytes(fields, BLOCK_POINTS)
}

fn working_set_bytes(fields: usize, pending_capacity: usize) -> usize {
    pending_capacity
        .saturating_mul(std::mem::size_of::<(usize, f64)>())
        .saturating_add(fields.saturating_mul(std::mem::size_of::<Option<f64>>() + 1))
}

fn labels_capacity_bytes(grouping: Grouping<'_>) -> usize {
    let labels = match grouping {
        Grouping::By(labels) | Grouping::Without(labels) => labels,
    };
    labels.len().saturating_mul(std::mem::size_of::<Arc<str>>())
}

fn grouping_bytes(grouping: Grouping<'_>) -> usize {
    let labels = match grouping {
        Grouping::By(labels) | Grouping::Without(labels) => labels,
    };
    labels
        .iter()
        .map(|label| label.len() + std::mem::size_of::<Arc<str>>())
        .sum::<usize>()
}

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

/// Which field of which layout a fold is writing, and how it folds it.
///
/// One value carried through the per-block work instead of three separate arguments: the three
/// never vary independently within one series, and the block loop is the only place that reads
/// them.
#[derive(Clone, Copy)]
struct FieldFold {
    width: usize,
    field_index: usize,
    op: AggregateOp,
}

impl Group {
    fn accumulate(
        &mut self,
        series: &Series,
        op: AggregateOp,
        budget: &mut impl FnMut(usize) -> Result<()>,
        mut retained: usize,
        width: usize,
    ) -> Result<usize> {
        let pending_capacity_bytes = BLOCK_POINTS.saturating_mul(size_of::<(usize, f64)>());
        budget(retained.saturating_add(pending_capacity_bytes))?;
        let mut pending = Vec::with_capacity(BLOCK_POINTS);
        for (field_index, field) in series.fields.iter().enumerate() {
            // The axis says which absolute positions the field holds and the field iterator
            // supplies them in order, so a sparse axis never turns into its envelope here.
            // `usize::MAX` is the "no block yet" marker; flushing it is a no-op, since it only
            // touches a block once a sample was buffered.
            let mut block = usize::MAX;
            for ((index, _), sample) in series.time.positions().zip(field.iter()) {
                if index / BLOCK_POINTS != block {
                    retained = self.update_block(
                        block,
                        FieldFold {
                            width,
                            field_index,
                            op,
                        },
                        &mut pending,
                        budget,
                        retained,
                    )?;
                    block = index / BLOCK_POINTS;
                }
                if let SampleRef::Float(sample) = sample {
                    pending.push((index % BLOCK_POINTS, sample));
                }
            }
            retained = self.update_block(
                block,
                FieldFold {
                    width,
                    field_index,
                    op,
                },
                &mut pending,
                budget,
                retained,
            )?;
        }
        Ok(retained)
    }

    /// Applies one field's samples within a block, so a block costs one lookup per field run.
    fn update_block(
        &mut self,
        block: usize,
        fold: FieldFold,
        pending: &mut Vec<(usize, f64)>,
        budget: &mut impl FnMut(usize) -> Result<()>,
        retained: usize,
    ) -> Result<usize> {
        if pending.is_empty() {
            return Ok(retained);
        }
        let new_block = !self.blocks.contains_key(&block);
        let block_bytes = if new_block {
            BLOCK_POINTS
                .saturating_mul(fold.width)
                .saturating_mul(std::mem::size_of::<FloatState>())
        } else {
            0
        };
        let backing_delta = if new_block {
            btree_backing_bytes(self.blocks.len() + 1)
                .saturating_sub(btree_backing_bytes(self.blocks.len()))
        } else {
            0
        };
        if new_block {
            budget(
                retained
                    .saturating_add(backing_delta)
                    .saturating_add(block_bytes)
                    .saturating_add(pending.capacity().saturating_mul(size_of::<(usize, f64)>())),
            )?;
        }
        let states = self.blocks.entry(block).or_insert_with(|| {
            vec![FloatState::default(); BLOCK_POINTS * fold.width].into_boxed_slice()
        });
        for (offset, sample) in pending.drain(..) {
            states[offset * fold.width + fold.field_index].update(fold.op, sample);
        }
        Ok(retained
            .saturating_add(backing_delta)
            .saturating_add(block_bytes))
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
