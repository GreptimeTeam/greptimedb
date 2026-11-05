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

pub(crate) mod state;
#[cfg(test)]
mod tests;

pub use state::{AccumulatorState, FloatMerger, PartialAggregate, StateField, merge_count_values};

use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::mem::size_of;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::arrow::array::{Float64Array, UInt32Array};
use datatypes::series::SeriesLabels;
use datatypes::series::aggregate_state::{HistogramState, StatePayload};
use datatypes::series::histogram::{
    CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::samples::{SampleRef, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{GridSpan, TimeAxis};

use crate::go_format::format_float_f;
use crate::grid_series::{GridSeriesBuilder, external};
use crate::histogram_eval::float_histogram::FloatHistogram;
use crate::histogram_eval::histogram_string;
use crate::map::{drop_empty_labels, validate_grid_series, validate_scalar};
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
/// Evaluated inputs on the shared schema/grid are admitted. An operation that reads a value takes
/// float samples only; `count` and `group` ask whether a sample is present, so they also accept a
/// histogram. `by` on a value field needs per-point labels and an output schema adapter, and is
/// rejected. There is no fallback.
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

/// What an aggregate does with a native histogram sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HistogramSamples {
    /// Counts it as one present sample: `count` counts every present sample, and `group` answers
    /// `1` for a non-empty group, whatever the sample's type is.
    Presence,
    /// Ignores it and reports that it did: `min`, `max`, `stddev`, `stdvar` and `quantile` read a
    /// float value, and Prometheus skips the histogram samples instead of refusing the statement.
    Ignored,
    /// Folds the payload and answers a histogram: `sum` adds native histograms bucket by bucket
    /// and `avg` adds them and divides, as Prometheus does, so one group's result is a native
    /// histogram.
    Folded,
}

/// What `op` does with a native histogram sample.
pub(crate) fn histogram_samples(op: AggregateOp) -> HistogramSamples {
    match op {
        AggregateOp::Count | AggregateOp::Group => HistogramSamples::Presence,
        AggregateOp::Min | AggregateOp::Max | AggregateOp::Stddev | AggregateOp::Stdvar => {
            HistogramSamples::Ignored
        }
        AggregateOp::Sum | AggregateOp::Avg => HistogramSamples::Folded,
    }
}

/// Checks a series for the aggregate `op` folds, and returns its evaluated axis.
///
/// Every operation this route folds takes a histogram series: it counts the sample, skips it, or
/// folds the payload. What a series must satisfy is therefore its axis, not its sample type -- the
/// fold itself is where a payload an operation cannot use is refused or dropped.
pub(crate) fn validate_aggregate_series<'a>(
    schema: &SeriesSchema,
    series: &'a Series,
    op: AggregateOp,
) -> Result<&'a TimeAxis> {
    match histogram_samples(op) {
        HistogramSamples::Presence | HistogramSamples::Ignored | HistogramSamples::Folded => {
            validate_grid_series(schema, series)
        }
    }
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
        validate_aggregate_series(&self.schema, series, self.op)?;
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
                    histograms: BTreeMap::new(),
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

    /// The evidence the fold recorded, for the node that finishes the answer to report.
    pub(crate) fn evidence_summary(&self) -> HistogramEvidence {
        evidence_summary(&self.groups)
    }

    /// Bytes reserved for this fold, using bounded allowances for its maps and group payloads.
    pub(crate) fn state_bytes(&self) -> usize {
        self.retained
    }

    /// The evidence the histogram fold recorded for one position, for a fold test.
    ///
    /// It is the state the fold keeps beside its payload, so a test reads the bits where the state
    /// export would read them rather than from a value the answer happens to imply.
    #[cfg(test)]
    pub(crate) fn histogram_evidence(
        &self,
        group: usize,
        position: usize,
        field: usize,
    ) -> Option<HistogramEvidence> {
        self.groups
            .get(group)
            .and_then(|group| group.histograms.get(&(position, field)))
            .map(|accumulator| accumulator.evidence)
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
                .saturating_add(histogram_backing_bytes(group.histograms.len()))
                .saturating_add(
                    group
                        .histograms
                        .values()
                        .fold(0usize, |bytes, accumulator| {
                            bytes.saturating_add(accumulator_bytes(accumulator))
                        }),
                )
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
    let schema_arc = Arc::clone(&schema);
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
        // A group that folded histogram samples answers histogram columns; one that folded floats
        // keeps the float path below, so a group without a single histogram payload is built
        // exactly as before.
        if !group.histograms.is_empty() {
            let finish_bytes = histogram_finish_bytes(&group, width);
            budget(
                state
                    .saturating_add(output_capacity_bytes)
                    .saturating_add(row_capacity_bytes)
                    .saturating_add(completed_output_bytes)
                    .saturating_add(finish_bytes),
            )?;
            if let Some(finished) = finish_histogram_group(op, &schema_arc, group, width)? {
                completed_output_bytes = completed_output_bytes
                    .saturating_add(crate::grid_series::series_bytes(&finished));
                output.push(finished);
            }
            budget(
                state
                    .saturating_add(output_capacity_bytes)
                    .saturating_add(row_capacity_bytes)
                    .saturating_add(completed_output_bytes),
            )?;
            continue;
        }
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

/// Builds one group's histogram result, or `None` when it has nothing left to answer.
///
/// A position's answer is its live sum plus the compensation paired with it -- what the kernel's own
/// `sum` does at the end of its fold -- with the start timestamp cleared and the empty buckets
/// dropped. A poisoned position was removed, so it contributes no value; a position that is
/// poisoned in one field and answered in another still carries the row.
fn finish_histogram_group(
    op: AggregateOp,
    schema: &Arc<SeriesSchema>,
    group: Group,
    width: usize,
) -> Result<Option<Series>> {
    // A field's column is one type. The fold already refuses a histogram where it had folded a
    // float at that position; a float folded after a histogram is caught here, where the column
    // type is chosen for the whole group.
    if group
        .blocks
        .values()
        .any(|states| states.iter().any(|state| state.seen))
    {
        return Err(DataFusionError::Execution(
            "a native aggregate cannot mix float and histogram samples in one field".into(),
        ));
    }
    let grid = schema.eval_grid.clone().ok_or_else(|| {
        DataFusionError::Execution("histogram aggregation requires an evaluation grid".into())
    })?;
    let mut positions: Vec<usize> = group
        .histograms
        .keys()
        .map(|&(position, _)| position)
        .collect();
    positions.dedup();
    let mut builders: Vec<HistogramBatchBuilder> =
        (0..width).map(|_| HistogramBatchBuilder::new()).collect();
    let mut indices: Vec<Vec<Option<u32>>> = vec![Vec::new(); width];
    let mut spans: Vec<GridSpan> = Vec::new();
    let mut row: Vec<Option<FloatHistogram>> = vec![None; width];
    for position in positions {
        // A poisoned position has no answer, so the row it would have made is dropped whole: every
        // column is written once per *answered* position, or the columns and the axis would drift.
        let mut answered = false;
        for (field, value) in row.iter_mut().enumerate() {
            *value = match group.histograms.get(&(position, field)) {
                Some(accumulator) => {
                    let value = finished_histogram(op, accumulator)?;
                    answered |= value.is_some();
                    value
                }
                None => None,
            };
        }
        if !answered {
            continue;
        }
        // A gap between two answered positions ends a run, and a row belongs to the run it opens.
        match spans.last_mut() {
            Some(span) if span.start() + span.len() == position => {
                *span = GridSpan::new(span.start(), span.len() + 1);
            }
            _ => spans.push(GridSpan::new(position, 1)),
        }
        for ((builder, indices), value) in builders.iter_mut().zip(&mut indices).zip(&row) {
            indices.push(match value {
                Some(value) => Some(value.push_to(builder)?),
                None => None,
            });
        }
    }
    if spans.is_empty() {
        return Ok(None);
    }
    let mut samples = Vec::with_capacity(width);
    for (builder, indices) in builders.into_iter().zip(indices) {
        let batch = builder.finish().map_err(external)?;
        samples.push(
            Samples::try_from_histogram(UInt32Array::from(indices), batch).map_err(external)?,
        );
    }
    // The result is addressed by the runs that carry a histogram, not by the span between the first
    // and the last one: the nulls padding them stay inside a run, never between two.
    let time = TimeAxis::from_grid_spans(grid, spans).map_err(external)?;
    Series::try_new(
        schema,
        group.metric,
        time,
        SeriesFields::try_from_vec(samples).map_err(external)?,
    )
    .map(Some)
    .map_err(external)
}

/// One position's answer, compacted, or `None` when the point was poisoned.
///
/// A sum answers its live value with the compensation folded in. An average in mean mode answers its
/// mean with the compensation folded in; one that never switched divides the sum and the compensation
/// by the sample count and then folds the compensation in, which is what `sum_or_avg` does at the end
/// of both paths.
fn finished_histogram(
    op: AggregateOp,
    accumulator: &HistogramAccumulator,
) -> Result<Option<FloatHistogram>> {
    if accumulator.evidence.poisoned {
        return Ok(None);
    }
    let mut value = match (op, accumulator.mean.as_ref()) {
        (AggregateOp::Avg, Some(mean)) => {
            let mut mean = mean.clone();
            if let Some(compensation) = accumulator.compensation.as_ref() {
                mean.add(compensation)?;
            }
            mean
        }
        (AggregateOp::Avg, None) => {
            let mut value = accumulator.value.clone();
            value.div(accumulator.count);
            if let Some(mut compensation) = accumulator.compensation.clone() {
                compensation.div(accumulator.count);
                value.add(&compensation)?;
            }
            value
        }
        _ => {
            let mut value = accumulator.value.clone();
            if let Some(compensation) = accumulator.compensation.as_ref() {
                value.add(compensation)?;
            }
            value
        }
    };
    // The answer is a result this fold derived, so it carries no start timestamp of its own, and its
    // empty buckets are dropped the way the kernel's own aggregate drops them.
    value.start_timestamp = None;
    value.compact();
    Ok(Some(value))
}

/// Bytes [`finish_histogram_group`] works in while it builds one group's result.
fn histogram_finish_bytes(group: &Group, width: usize) -> usize {
    let payload = group
        .histograms
        .values()
        .fold(0usize, |bytes, accumulator| {
            bytes.saturating_add(accumulator_bytes(accumulator))
        });
    let rows = group.histograms.len();
    let fields = width.saturating_mul(
        size_of::<HistogramBatchBuilder>()
            .saturating_add(size_of::<Vec<Option<u32>>>())
            .saturating_add(size_of::<Samples>()),
    );
    // Twice the fold's payload: the answer is one derived histogram per position, and an average
    // divides a copy of its live value before it adds the compensation in.
    payload
        .saturating_mul(2)
        .saturating_add(rows.saturating_mul(size_of::<Option<u32>>()))
        .saturating_add(rows.saturating_mul(size_of::<GridSpan>()))
        .saturating_add(fields)
        .saturating_add(size_of::<Series>().saturating_add(size_of::<TimeAxis>()))
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
    let mut groups: Vec<(SeriesLabels, Vec<&Series>)> = Vec::new();
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

/// Validates the schema-only requirements shared by the kernel and native runtime construction.
pub(crate) fn validate_count_values(
    schema: &SeriesSchema,
    label: &str,
    grouping: Grouping<'_>,
) -> Result<()> {
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
    validate_grouping(schema, checked)?;
    if label.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "invalid label name {label:?}"
        )));
    }
    Ok(())
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
    validate_count_values(schema, label, grouping)?;
    // Unlike the other aggregations, this one reads histogram samples too.
    for series in input {
        validate_grid_series(schema, series)?;
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
type Counts = Vec<(SeriesLabels, BTreeMap<usize, Vec<f64>>)>;

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
        // `quantile` reads float values and skips a histogram sample the way Prometheus does, so
        // the only thing it needs from a series is that it fits this schema and grid.
        validate_grid_series(schema, series)?;
    }
    Ok(())
}

/// Validates the grouping a whole-vector operation addresses its series by.
///
/// The native route keys `topk`, `bottomk` and `limitk` groups by series labels only, while the
/// existing planner also accepts a value field there, where it varies between samples. Refusing
/// the field here keeps such a statement on the existing plan before native execution is chosen.
pub(crate) fn validate_vector_grouping(
    schema: &SeriesSchema,
    grouping: Grouping<'_>,
) -> Result<()> {
    validate_grouping(schema, grouping)
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
    fn apply(self, input: &SeriesLabels) -> Result<SeriesLabels> {
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

fn metric_bytes(metric: &SeriesLabels) -> usize {
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
        .saturating_add(std::mem::size_of::<SeriesLabels>())
}

fn grouping_workspace_bytes(metric: &SeriesLabels) -> usize {
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

fn label_key(metric: &SeriesLabels) -> LabelKey {
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
    metric: SeriesLabels,
    /// Keyed by absolute grid index / `BLOCK_POINTS`; each block holds one state per field for
    /// each of its points, point by point.
    blocks: BTreeMap<usize, Box<[FloatState]>>,
    /// The histogram states of this group, keyed by absolute grid position and field.
    ///
    /// Only an aggregate that folds a payload allocates one, and only where it saw a histogram
    /// sample, so a float fold never touches this map and its state stays the blocks alone.
    histograms: BTreeMap<(usize, usize), HistogramAccumulator>,
}

/// One position's histogram fold: the live payload, the compensation paired with it, the running
/// mean an average switched to, and the evidence the fold recorded beside them.
///
/// The payload here is the kernel's own owned histogram rather than the state frame's row
/// representation: a fold adds bucket by bucket, and rebuilding a frame row per sample would
/// re-encode the layout it just added into. The state export builds that row from this.
///
/// The fields are the `HistogramAvg` state layout of the design: `value` is the state's
/// `histogram_value`, `mean` is its `incremental_mean` mode, `count` is its `count`, and
/// `compensation` is its `histogram_compensation`. A `sum` fold leaves `mean` at `None` and only
/// counts for the average's sake.
struct HistogramAccumulator {
    /// The live value: the running sum, or -- once an average switched -- the sum the switch started
    /// from, which the mean is derived from.
    value: FloatHistogram,
    /// The compensation paired with `value`, carrying `value`'s bucket layout.
    ///
    /// It is `None` only while a [`FloatHistogram::kahan_add`] call owns it, and stays `None` once
    /// the position is poisoned: a poisoned point folds nothing more, so there is nothing left to
    /// compensate.
    compensation: Option<FloatHistogram>,
    /// The running mean an average switched to once its sum overflowed; `None` while it still sums.
    mean: Option<FloatHistogram>,
    /// Samples folded into this position, which is the divisor an average's mean uses.
    count: f64,
    evidence: HistogramEvidence,
}

/// The evidence bits a histogram fold records beside its payload.
///
/// These are the Boolean components of the histogram state layout: the fold records them per point
/// so a merge of two partitions can report what the inputs disagreed about. Only `poisoned` steers
/// the fold's own output today -- a poisoned point is removed -- and the rest are carried for that
/// merge.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HistogramEvidence {
    /// The point cannot be answered because its samples disagreed on their bucket layout.
    pub(crate) poisoned: bool,
    /// The disagreement was exponential buckets meeting custom buckets.
    pub(crate) incompatible_layout: bool,
    /// Two custom-bucket samples were reconciled into the intersection of their bounds.
    pub(crate) custom_bounds_reconciled: bool,
    /// Some sample reported a counter reset.
    pub(crate) counter_reset_seen: bool,
    /// Some sample reported that no counter reset happened.
    pub(crate) not_counter_reset_seen: bool,
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
                match sample {
                    SampleRef::Missing => {}
                    SampleRef::Float(sample) => pending.push((index % BLOCK_POINTS, sample)),
                    SampleRef::Histogram(view) => match histogram_samples(op) {
                        // `count` and `group` ask whether a sample is present, not which payload
                        // it carries, so a histogram is one contribution. The value is a
                        // placeholder: `FloatState` counts the contributions for `count` and only
                        // records that it saw one for `group`.
                        HistogramSamples::Presence => pending.push((index % BLOCK_POINTS, 1.0)),
                        // Prometheus skips a histogram sample where the aggregate reads a float
                        // value; the runtime reports that it did.
                        HistogramSamples::Ignored => {}
                        // `sum` and `avg` fold the payload themselves, into the position's own
                        // histogram state. The sample is addressed by its absolute grid position, so
                        // it does not go through the block buffer the float samples share.
                        HistogramSamples::Folded => {
                            retained = self.fold_histogram(
                                FieldFold {
                                    width,
                                    field_index,
                                    op,
                                },
                                index,
                                &view,
                                budget,
                                retained,
                            )?;
                        }
                    },
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

impl Group {
    /// Folds one histogram sample into its position's state, admitting what the fold allocates.
    ///
    /// The first sample of a position establishes the state: it is the live value, and its
    /// compensation is the layout-carrying zero `new_compensation` builds -- never an `empty_like`
    /// histogram, which has no bucket indices for the compensated add to index. Every later sample
    /// is one [`FloatHistogram::kahan_add`] into the live value (an average's mean once it switched),
    /// carrying the compensation this position already holds.
    ///
    /// Two samples whose bucket layouts cannot be added (exponential meeting custom buckets) poison
    /// the position instead of failing the statement, and a poisoned position folds nothing more:
    /// Prometheus drops the point, so the evidence is what survives. Numeric overflow is not a reason
    /// to poison a point -- a saturated sum answers `Inf`, as the kernel's own sum does, and an
    /// average switches to its mean instead.
    fn fold_histogram(
        &mut self,
        fold: FieldFold,
        position: usize,
        view: &HistogramView<'_>,
        budget: &mut impl FnMut(usize) -> Result<()>,
        retained: usize,
    ) -> Result<usize> {
        // A field's column is either float or histogram, so a position that already folded a float
        // cannot also fold a histogram. The other order is caught when the fold is finished, which
        // is where the column type is chosen.
        if let Some(states) = self.blocks.get(&(position / BLOCK_POINTS))
            && states[(position % BLOCK_POINTS) * fold.width + fold.field_index].seen
        {
            return Err(DataFusionError::Execution(
                "a native aggregate cannot mix float and histogram samples in one field".into(),
            ));
        }
        let sample = FloatHistogram::try_from_view(view)?;
        let key = (position, fold.field_index);
        let vacant = !self.histograms.contains_key(&key);
        let backing = if vacant {
            histogram_backing_bytes(self.histograms.len() + 1)
                .saturating_sub(histogram_backing_bytes(self.histograms.len()))
        } else {
            0
        };
        if vacant {
            // Admit the new position before allocating it: a live value, the compensation that
            // starts beside it, and the map node holding both.
            budget(
                retained
                    .saturating_add(size_of::<HistogramAccumulator>())
                    .saturating_add(sample.heap_bytes().saturating_mul(2))
                    .saturating_add(backing),
            )?;
        }
        let accumulator = self.histograms.entry(key).or_insert_with(|| {
            let compensation = sample.new_compensation();
            HistogramAccumulator {
                value: sample.clone(),
                compensation: Some(compensation),
                mean: None,
                count: 1.0,
                evidence: HistogramEvidence::default(),
            }
        });
        // The evidence follows the sample's own reset hint, whether or not the point still folds.
        match sample.reset_hint {
            CounterResetHint::CounterReset => accumulator.evidence.counter_reset_seen = true,
            CounterResetHint::NotCounterReset => accumulator.evidence.not_counter_reset_seen = true,
            _ => {}
        }
        if accumulator.evidence.poisoned {
            return Ok(retained);
        }
        if vacant {
            // The admitted estimate is an upper bound; what the fold keeps is the real figure.
            return Ok(retained
                .saturating_add(accumulator_bytes(accumulator))
                .saturating_add(backing));
        }
        // An add can grow both bucket lists, so the incoming sample's payload on top of what the
        // position already holds is what is admitted; the figure kept afterwards is the real one.
        let before = accumulator_bytes(accumulator);
        budget(
            retained
                .saturating_add(before)
                .saturating_add(sample.heap_bytes()),
        )?;
        if accumulator.value.uses_custom_buckets() != sample.uses_custom_buckets() {
            // Exponential buckets cannot be added to custom ones: the point is removed rather than
            // answered, and the evidence says why.
            accumulator.evidence.poisoned = true;
            accumulator.evidence.incompatible_layout = true;
            accumulator.compensation = None;
            return Ok(retained);
        }
        match fold.op {
            AggregateOp::Sum => accumulator.fold_sum(&sample)?,
            AggregateOp::Avg => accumulator.fold_avg(&sample)?,
            op => unreachable!("{op:?} does not fold a histogram payload"),
        }
        Ok(retained
            .saturating_sub(before)
            .saturating_add(accumulator_bytes(accumulator)))
    }
}

impl HistogramAccumulator {
    /// Adds one sample into the running sum, one compensated add into the live value.
    fn fold_sum(&mut self, sample: &FloatHistogram) -> Result<()> {
        let (compensation, outcome) = self.value.kahan_add(sample, self.compensation.take())?;
        self.compensation = Some(compensation);
        self.evidence.custom_bounds_reconciled |= outcome.custom_bounds_reconciled;
        Ok(())
    }

    /// Averages one sample in, switching from the running sum to a running mean when the sum would
    /// overflow.
    ///
    /// Each step is Prometheus v3.15's `aggregation`/`sum_or_avg` average in the same order: the
    /// trial add is what decides the mode, and once it overflows the trial is discarded, the sum so
    /// far becomes the mean of the samples before this one, and every later sample enters as
    /// `sample / count` weighted by `(count - 1) / count`. Dividing before adding is what keeps a
    /// mean of finite inputs finite: two histograms of `1e308` at one observation each average to
    /// `1e308`, while `(1e308 + 1e308) / 2` would be `Inf`.
    fn fold_avg(&mut self, sample: &FloatHistogram) -> Result<()> {
        self.count += 1.0;
        if self.mean.is_none() {
            // The trial runs on a copy, so an overflowing add leaves the sum it started from intact
            // -- that sum is what the mean begins at.
            let mut trial = self.value.clone();
            let (compensation, outcome) = trial.kahan_add(sample, self.compensation.clone())?;
            // A reconciliation counts whether or not the trial is kept: the two sides' bounds meet
            // in it either way.
            self.evidence.custom_bounds_reconciled |= outcome.custom_bounds_reconciled;
            if !trial.has_overflow() {
                self.value = trial;
                self.compensation = Some(compensation);
                return Ok(());
            }
            let mut started = self.value.clone();
            started.div(self.count - 1.0);
            if let Some(compensation) = self.compensation.as_mut() {
                compensation.div(self.count - 1.0);
            }
            self.mean = Some(started);
        }
        // The mean's unit follows the value's: the compensation is scaled by the same weight the
        // value is, and the incoming sample is divided before it is added.
        let q = (self.count - 1.0) / self.count;
        if let Some(compensation) = self.compensation.as_mut() {
            compensation.mul(q);
        }
        let mut sample = sample.clone();
        sample.div(self.count);
        let mean = self.mean.as_mut().expect("set above");
        mean.mul(q);
        let (compensation, outcome) = mean.kahan_add(&sample, self.compensation.take())?;
        self.compensation = Some(compensation);
        self.evidence.custom_bounds_reconciled |= outcome.custom_bounds_reconciled;
        Ok(())
    }
}

impl HistogramAccumulator {
    /// This position's state as the frame payload a `Partial` hands over.
    ///
    /// The payload is the fold's own state, not its answer: a sum exports the running sum and the
    /// compensation beside it, and folding the compensation in belongs to the merge that finishes the
    /// answer. An average exports whichever value it is keeping -- the sum it still adds to, or the
    /// mean it switched to -- with the count and the mode flag that say how to read them.
    ///
    /// A poisoned position exports the empty placeholders the state layout reserves for it: it
    /// answers nothing, and the merge reads its evidence, not its buckets.
    fn payload(&self, op: AggregateOp) -> Result<StatePayload> {
        if self.evidence.poisoned {
            let empty = self.value.empty_like();
            return Ok(StatePayload::Histogram(Box::new(HistogramState {
                value: histogram_batch(&empty)?,
                compensation: histogram_batch(&empty)?,
                count: 0.0,
                incremental_mean: false,
                poisoned: true,
                incompatible_layout: self.evidence.incompatible_layout,
                custom_bounds_reconciled: self.evidence.custom_bounds_reconciled,
                counter_reset_seen: self.evidence.counter_reset_seen,
                not_counter_reset_seen: self.evidence.not_counter_reset_seen,
            })));
        }
        let switched = self.mean.is_some();
        let value = match op {
            // A sum's state is its running sum: adding the compensation in is the merge's job.
            AggregateOp::Sum => &self.value,
            AggregateOp::Avg if switched => self.mean.as_ref().expect("checked above"),
            AggregateOp::Avg => &self.value,
            op => unreachable!("{op:?} does not fold a histogram payload"),
        };
        let compensation = self
            .compensation
            .clone()
            .unwrap_or_else(|| value.new_compensation());
        Ok(StatePayload::Histogram(Box::new(HistogramState {
            value: histogram_batch(value)?,
            compensation: histogram_batch(&compensation)?,
            // A sum's state declares no count and no mean mode, so both stay at their defaults: the
            // payload cannot claim a component its kind does not carry.
            count: if op == AggregateOp::Avg {
                self.count
            } else {
                0.0
            },
            incremental_mean: switched && op == AggregateOp::Avg,
            poisoned: false,
            incompatible_layout: self.evidence.incompatible_layout,
            custom_bounds_reconciled: self.evidence.custom_bounds_reconciled,
            counter_reset_seen: self.evidence.counter_reset_seen,
            not_counter_reset_seen: self.evidence.not_counter_reset_seen,
        })))
    }

    /// The state a decoded payload carries, as the merge keeps it.
    fn from_payload(op: AggregateOp, payload: &HistogramState) -> Result<Self> {
        let value = decoded_histogram(&payload.value)?;
        let compensation = decoded_histogram(&payload.compensation)?;
        let switched = op == AggregateOp::Avg && payload.incremental_mean;
        Ok(Self {
            // An average that switched keeps its mean in the mean slot; the sum it switched from is
            // not part of the state it exported, so the value slot holds the mean too.
            value: value.clone(),
            compensation: Some(compensation),
            mean: switched.then_some(value),
            count: payload.count,
            evidence: HistogramEvidence {
                poisoned: payload.poisoned,
                incompatible_layout: payload.incompatible_layout,
                custom_bounds_reconciled: payload.custom_bounds_reconciled,
                counter_reset_seen: payload.counter_reset_seen,
                not_counter_reset_seen: payload.not_counter_reset_seen,
            },
        })
    }

    /// Merges another partition's state into this position's.
    ///
    /// The rules are the design's: a sum adds both sides' values and then both sides' compensations
    /// into the live value, and an average either adds its sums the same way -- when that stays
    /// finite -- or continues in mean mode from the means and the counts, dividing before it adds.
    /// Poison is sticky, and both sides' evidence is kept whatever the outcome of the arithmetic.
    fn merge_state(&mut self, op: AggregateOp, other: &HistogramState) -> Result<()> {
        self.evidence.poisoned |= other.poisoned;
        self.evidence.incompatible_layout |= other.incompatible_layout;
        self.evidence.custom_bounds_reconciled |= other.custom_bounds_reconciled;
        self.evidence.counter_reset_seen |= other.counter_reset_seen;
        self.evidence.not_counter_reset_seen |= other.not_counter_reset_seen;
        if self.evidence.poisoned {
            // A poisoned point answers nothing, so there is no value to merge into it.
            return Ok(());
        }
        let right = decoded_histogram(&other.value)?;
        let right_compensation = decoded_histogram(&other.compensation)?;
        match op {
            AggregateOp::Sum => self.merge_sum(right, right_compensation),
            AggregateOp::Avg => self.merge_avg(
                right,
                right_compensation,
                other.count,
                other.incremental_mean,
            ),
            op => unreachable!("{op:?} does not merge a histogram payload"),
        }
    }

    /// Merges two sums: both values and then both compensations go into the live value.
    ///
    /// The receiver stays the live value for both adds -- the compensation's unit follows the value's
    /// -- so the second add folds the other side's compensation into the sum this state carries, not
    /// into the compensation the first add returned.
    fn merge_sum(
        &mut self,
        right: FloatHistogram,
        right_compensation: FloatHistogram,
    ) -> Result<()> {
        let left_compensation = self
            .compensation
            .take()
            .expect("a folding position keeps its compensation");
        let (compensation, first) = self.value.kahan_add(&right, Some(left_compensation))?;
        let (compensation, second) = self
            .value
            .kahan_add(&right_compensation, Some(compensation))?;
        self.compensation = Some(compensation);
        self.evidence.custom_bounds_reconciled |=
            first.custom_bounds_reconciled || second.custom_bounds_reconciled;
        Ok(())
    }

    /// Merges an average: the sums add while they stay finite, and otherwise the means and their
    /// counts do.
    fn merge_avg(
        &mut self,
        right: FloatHistogram,
        right_compensation: FloatHistogram,
        right_count: f64,
        right_is_mean: bool,
    ) -> Result<()> {
        let left_is_mean = self.mean.is_some();
        let left_count = self.count;
        let total = left_count + right_count;
        let left_value = if left_is_mean {
            self.mean.clone().expect("checked above")
        } else {
            self.value.clone()
        };
        let left_compensation = self
            .compensation
            .clone()
            .unwrap_or_else(|| left_value.new_compensation());
        if !left_is_mean && !right_is_mean {
            // Sum and sum: steps 1-2 on a copy of the left sum. The trial is what decides the mode,
            // and a reconciliation it reports counts whether or not it is kept.
            let mut trial = left_value.clone();
            let (compensation, first) = trial.kahan_add(&right, Some(left_compensation.clone()))?;
            let (compensation, second) =
                trial.kahan_add(&right_compensation, Some(compensation))?;
            self.evidence.custom_bounds_reconciled |=
                first.custom_bounds_reconciled || second.custom_bounds_reconciled;
            if !trial.has_overflow() {
                self.value = trial;
                self.compensation = Some(compensation);
                self.count = total;
                return Ok(());
            }
        }
        // The mean path, written so that no intermediate product or sum can overflow: each side
        // becomes a mean, its count becomes a weight, and the weighted means are scaled before they
        // are added -- with their compensations scaled the same way, because the compensation's unit
        // is the value's.
        let weight_left = left_count / total;
        let weight_right = right_count / total;
        let mut left_mean = left_value;
        let mut left_mean_compensation = left_compensation;
        if !left_is_mean {
            left_mean.div(left_count);
            left_mean_compensation.div(left_count);
        }
        let mut right_mean = right;
        let mut right_mean_compensation = right_compensation;
        if !right_is_mean {
            right_mean.div(right_count);
            right_mean_compensation.div(right_count);
        }
        left_mean.mul(weight_left);
        left_mean_compensation.mul(weight_left);
        right_mean.mul(weight_right);
        right_mean_compensation.mul(weight_right);
        let (compensation, first) =
            left_mean.kahan_add(&right_mean, Some(left_mean_compensation))?;
        let (compensation, second) =
            left_mean.kahan_add(&right_mean_compensation, Some(compensation))?;
        self.value = left_mean.clone();
        self.mean = Some(left_mean);
        self.compensation = Some(compensation);
        self.count = total;
        self.evidence.custom_bounds_reconciled |=
            first.custom_bounds_reconciled || second.custom_bounds_reconciled;
        Ok(())
    }
}

/// One histogram as the frame payload's own one-row batch.
fn histogram_batch(histogram: &FloatHistogram) -> Result<Arc<HistogramBatch>> {
    let mut builder = HistogramBatchBuilder::new();
    histogram.push_to(&mut builder)?;
    builder.finish().map_err(external)
}

/// The histogram a payload component carries.
fn decoded_histogram(batch: &HistogramBatch) -> Result<FloatHistogram> {
    let view = batch.view(0).ok_or_else(|| {
        DataFusionError::Execution("a histogram state component carries no histogram".into())
    })?;
    FloatHistogram::try_from_view(&view)
}

/// The evidence a fold or a merge has to report, once, wherever it finishes.
///
/// A poisoned point suppresses its own reconciliation bit: the point answers nothing, so reporting
/// that its bounds were reconciled would report a reconciliation of an answer that is not there.
/// Everything else is reported if any point recorded it, which is what makes the message set the same
/// whichever order the partitions were merged in.
fn evidence_summary(groups: &[Group]) -> HistogramEvidence {
    let mut summary = HistogramEvidence::default();
    for accumulator in groups.iter().flat_map(|group| group.histograms.values()) {
        let evidence = accumulator.evidence;
        summary.poisoned |= evidence.poisoned;
        summary.incompatible_layout |= evidence.incompatible_layout;
        summary.counter_reset_seen |= evidence.counter_reset_seen;
        summary.not_counter_reset_seen |= evidence.not_counter_reset_seen;
        summary.custom_bounds_reconciled |= evidence.custom_bounds_reconciled && !evidence.poisoned;
    }
    summary
}

/// Bytes one histogram position holds: every histogram it keeps, with buckets, bounds and spans.
fn accumulator_bytes(accumulator: &HistogramAccumulator) -> usize {
    size_of::<HistogramAccumulator>()
        .saturating_add(accumulator.value.heap_bytes())
        .saturating_add(
            accumulator
                .mean
                .as_ref()
                .map_or(0, FloatHistogram::heap_bytes),
        )
        .saturating_add(
            accumulator
                .compensation
                .as_ref()
                .map_or(0, FloatHistogram::heap_bytes),
        )
}

/// An upper bound on the bytes one node of a group's histogram map holds.
fn histogram_node_bytes() -> usize {
    11 * size_of::<((usize, usize), HistogramAccumulator)>() + 16 * size_of::<usize>()
}

/// An upper bound on the bytes a histogram map of `entries` holds, like [`btree_backing_bytes`].
fn histogram_backing_bytes(entries: usize) -> usize {
    if entries == 0 {
        return 0;
    }
    entries.div_ceil(5).saturating_mul(histogram_node_bytes())
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
