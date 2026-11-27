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
/// Inputs must be evaluated series on the schema's grid. `sum` and `avg` fold histogram samples,
/// `count` and `group` count them, and the other operations skip them. `by` naming a value field is
/// rejected.
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

/// [`aggregate_float`] fed one series at a time, in arrival order. Owns its grouping labels, so it
/// can live in a stream's state.
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
    /// `count` and `group`: counts as one present sample.
    Presence,
    /// `min`, `max`, `stddev`, `stdvar`: skipped and reported, as Prometheus does.
    Ignored,
    /// `sum` and `avg`: folded bucket by bucket into a histogram result.
    Folded,
}

impl AggregateOp {
    /// The `f64` words one field's state keeps per grid point. The last word is the presence
    /// word: the count where the state has one, else a `0`/`1` flag. Layouts:
    ///
    /// - `sum`: value, compensation, presence
    /// - `avg`: value, compensation, mean, incremental-mean flag, count
    /// - `min`/`max`: value, presence
    /// - `stddev`/`stdvar`: sum of squared deviations, mean, count
    /// - `count`: count; `group`: presence
    pub(crate) const fn state_words(self) -> usize {
        match self {
            Self::Count | Self::Group => 1,
            Self::Min | Self::Max => 2,
            Self::Sum | Self::Stddev | Self::Stdvar => 3,
            Self::Avg => 5,
        }
    }
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

/// Checks a series against the schema and grid and returns its axis. Every op accepts histogram
/// samples here; the fold decides what to do with them.
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
    /// A budget refusal in [`Self::update_budgeted`] can leave the state half folded, with no
    /// rollback; the caller must drop the aggregator after one.
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
        let group_capacity = self.groups.capacity();
        let map_capacity = self.group_index.capacity();
        // One admission, before any allocation, for everything the fold may allocate before
        // writing samples, including new-group costs whether or not a group opens. The grouping
        // copy with its workspace, the group's labels, and its key are each bounded by one label
        // workspace, hence three.
        let label_workspace = grouping_workspace_bytes(&series.metric);
        budget(
            existing_bytes
                .saturating_add(label_workspace.saturating_mul(3))
                .saturating_add(pending_bytes(self.schema.fields.len()))
                .saturating_add(
                    self.labels
                        .len()
                        .saturating_mul(std::mem::size_of::<&str>()),
                )
                // A realloc keeps both buffers alive while it copies, so growth admits the new
                // buffer beside the old one already in `retained`. Same for the map.
                .saturating_add(if self.groups.len() == group_capacity {
                    group_capacity
                        .saturating_mul(2)
                        .max(4)
                        .saturating_mul(size_of::<Group>())
                } else {
                    0
                })
                .saturating_add(if self.group_index.len() == map_capacity {
                    hash_map_backing_bytes(next_hash_capacity(map_capacity))
                } else {
                    0
                }),
        )?;
        let labels: Vec<&str> = self.labels.iter().map(|label| &**label).collect();
        let grouping = if self.by {
            Grouping::By(&labels)
        } else {
            Grouping::Without(&labels)
        };
        let metric = grouping.apply(&series.metric)?;
        let key = label_key(&metric);
        let key_retained = key
            .capacity()
            .saturating_mul(size_of::<(Arc<str>, Arc<str>)>());
        let new_group = !self.group_index.contains_key(&key);
        let old_map_backing = hash_map_backing_bytes(map_capacity);
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

    pub(crate) fn groups(&self) -> usize {
        self.groups.len()
    }

    pub(crate) fn evidence_summary(&self) -> HistogramEvidence {
        evidence_summary(&self.groups)
    }

    /// Bytes admitted for this fold; an upper bound on what it holds.
    pub(crate) fn state_bytes(&self) -> usize {
        self.retained
    }

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
                        .saturating_mul(self.op.state_words())
                        .saturating_mul(std::mem::size_of::<f64>()),
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
    let words = op.state_words();
    let point_words = width.saturating_mul(words);
    for group in groups {
        // A group that folded histogram samples answers histogram columns.
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
        builder.begin_series();
        let (rows, runs) = group_rows_runs(&group, width, op);
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
            for (offset, point) in states.chunks_exact(point_words).enumerate() {
                if !point.chunks_exact(words).any(|state| state_seen(state, op)) {
                    continue;
                }
                row.clear();
                row.extend(
                    point
                        .chunks_exact(words)
                        .map(|state| state_finish(state, op)),
                );
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
/// A poisoned position yields no value in its field; the row survives if another field answers.
fn finish_histogram_group(
    op: AggregateOp,
    schema: &Arc<SeriesSchema>,
    group: Group,
    width: usize,
) -> Result<Option<Series>> {
    // The output column type is chosen per group: any float state in a group that also folded
    // histograms is refused, even when it is in another field or position. The fold itself only
    // refuses the two kinds meeting in one field.
    if group.blocks.values().any(|states| {
        states
            .chunks_exact(op.state_words())
            .any(|state| state_seen(state, op))
    }) {
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
        // Every column is written once per answered position, or the columns and the axis drift.
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

/// One position's answer with the compensation folded in and empty buckets dropped, or `None`
/// when the point was poisoned. An average that never switched to mean mode divides the sum and
/// the compensation by the count first, as Prometheus `sum_or_avg` does.
pub(crate) fn finished_histogram(
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
    value.compact();
    Ok(Some(value))
}

/// Bytes [`finish_histogram_group`] allocates for one group's result.
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
    // Twice the payload: one derived histogram per position, plus the copy an average divides.
    payload
        .saturating_mul(2)
        .saturating_add(rows.saturating_mul(size_of::<Option<u32>>()))
        .saturating_add(rows.saturating_mul(size_of::<GridSpan>()))
        .saturating_add(fields)
        .saturating_add(size_of::<Series>().saturating_add(size_of::<TimeAxis>()))
}

/// [`finish_groups_budgeted`] without a budget.
fn finish_groups(
    schema: Arc<SeriesSchema>,
    op: AggregateOp,
    groups: Vec<Group>,
) -> Result<Vec<Series>> {
    finish_groups_budgeted(schema, op, groups, 0, |_| Ok(()))
}

fn group_rows_runs(group: &Group, width: usize, op: AggregateOp) -> (usize, usize) {
    if width == 0 {
        return (0, 0);
    }
    let words = op.state_words();
    let point_words = width * words;
    let mut rows = 0usize;
    let mut runs = 0usize;
    let mut previous: Option<usize> = None;
    for (&block, states) in &group.blocks {
        for (offset, point) in states.chunks_exact(point_words).enumerate() {
            if point.chunks_exact(words).any(|state| state_seen(state, op)) {
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

/// `quantile(q, v)` with `q` a time-varying scalar on the same grid. A point where `q` has no value
/// has no result; `q` outside `[0, 1]` or NaN behaves as in Prometheus. Histogram samples are
/// skipped.
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
        // A min-heap of each member's next position keeps a point's cost proportional to the
        // members present there. Ties pop in member order, the order Prometheus appends values.
        let mut positions: Vec<_> = members
            .iter()
            .map(|series| series.time.positions())
            .collect();
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

/// The schema checks of [`count_values`], shared with native runtime construction.
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
        // Each distinct value of a series resolves its group once.
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

/// Counts per point and field for each label set; a count of 0 means no sample.
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

/// A `count_values` grouping key: float bits, or a histogram's string form.
#[derive(PartialEq, Eq, Hash)]
enum Value {
    Float(u64),
    Histogram(String),
}

fn validate_input(schema: &SeriesSchema, input: &[Series], grouping: Grouping<'_>) -> Result<()> {
    validate_grouping(schema, grouping)?;
    for series in input {
        validate_grid_series(schema, series)?;
    }
    Ok(())
}

/// Validates the grouping of `topk`, `bottomk` and `limitk`. Native groups by series labels only,
/// so `by` a value field is refused, which keeps such a statement on the existing plan.
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

/// An upper bound on the bytes the group index holds for `capacity` entries.
///
/// A hash table keeps a power-of-two bucket count below `16/7` of its usable capacity, plus one
/// control byte per bucket, so three buckets per entry bounds it whatever growth policy it uses.
fn hash_map_backing_bytes(capacity: usize) -> usize {
    capacity
        .saturating_mul(3)
        .saturating_mul(size_of::<(LabelKey, usize)>() + 1)
}

/// An upper bound on the capacity a map holding `capacity` entries grows to.
fn next_hash_capacity(capacity: usize) -> usize {
    capacity.saturating_mul(2).max(7)
}

/// An upper bound on the bytes one B-tree node holds: at most eleven pairs and twelve child
/// pointers, with a generous header allowance so the bound does not depend on node layout.
fn btree_node_bytes() -> usize {
    11 * size_of::<(usize, Box<[f64]>)>() + 16 * size_of::<usize>()
}

/// An upper bound on the bytes a B-tree of `entries` holds: every node but the root keeps at
/// least five entries.
fn btree_backing_bytes(entries: usize) -> usize {
    if entries == 0 {
        return 0;
    }
    entries.div_ceil(5).saturating_mul(btree_node_bytes())
}

/// Transient bytes the fold needs for one row while it folds a series.
fn pending_bytes(fields: usize) -> usize {
    fields.saturating_mul(std::mem::size_of::<Option<f64>>() + 1)
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

/// Grid points per state block. Blocks are allocated on first sample, so sparse input never
/// expands to the full grid.
const BLOCK_POINTS: usize = 64;

/// Bytes one block keeps. The fold, the merge and the recount all size blocks through this.
fn block_bytes(width: usize, op: AggregateOp) -> usize {
    BLOCK_POINTS
        .saturating_mul(width)
        .saturating_mul(op.state_words())
        .saturating_mul(size_of::<f64>())
}

struct Group {
    metric: SeriesLabels,
    /// Keyed by absolute grid index / `BLOCK_POINTS`; point-major, then field, then state words.
    blocks: BTreeMap<usize, Box<[f64]>>,
    /// Histogram states of `sum`/`avg`, keyed by (absolute grid position, field).
    histograms: BTreeMap<(usize, usize), HistogramAccumulator>,
}

/// One position's histogram `sum`/`avg` fold. Maps onto the `HistogramState` payload: `value` is
/// `histogram_value`, `compensation` is `histogram_compensation`, `mean` is set in
/// `incremental_mean` mode. A `sum` fold leaves `mean` at `None` and does not update `count`.
///
/// The legacy `prom_native_histogram_agg_sum`/`_avg` accumulators fold through this type too.
#[derive(Debug)]
pub(crate) struct HistogramAccumulator {
    /// The running sum; once an average switched to mean mode, the sum at the switch.
    value: FloatHistogram,
    /// The compensation of the live value, with its bucket layout. `None` only while
    /// [`FloatHistogram::kahan_add`] owns it, and once the position is poisoned.
    compensation: Option<FloatHistogram>,
    /// The running mean an average switched to once its sum overflowed; `None` while it still sums.
    mean: Option<FloatHistogram>,
    /// Samples folded in; maintained only by `avg`.
    count: f64,
    pub(crate) evidence: HistogramEvidence,
}

/// Per-point flags a histogram fold records, carried in the state so a merge can report them.
/// Only `poisoned` changes the answer: a poisoned point is removed.
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
    /// One unpoisoned point saw both hints. Set only by [`evidence_summary`]; never carried in
    /// the state.
    pub(crate) counter_reset_collision: bool,
}

/// Which field of which block layout a fold is writing.
#[derive(Clone, Copy)]
struct FieldFold {
    width: usize,
    field_index: usize,
    op: AggregateOp,
}

impl FieldFold {
    fn point_words(self) -> usize {
        self.width.saturating_mul(self.op.state_words())
    }

    /// Where this field's state for the point at `offset` starts inside a block.
    fn state_start(self, offset: usize) -> usize {
        offset
            .saturating_mul(self.point_words())
            .saturating_add(self.field_index.saturating_mul(self.op.state_words()))
    }
}

impl Group {
    /// Folds one series into this group, one field at a time. Samples arrive in position order,
    /// so a block is looked up (and admitted, if new) once per run of samples inside it.
    fn accumulate(
        &mut self,
        series: &Series,
        op: AggregateOp,
        budget: &mut impl FnMut(usize) -> Result<()>,
        mut retained: usize,
        width: usize,
    ) -> Result<usize> {
        // Re-admit without the grouping workspace, which is freed by now.
        budget(retained)?;
        for (field_index, field) in series.fields.iter().enumerate() {
            let fold = FieldFold {
                width,
                field_index,
                op,
            };
            // The branch is per field, but `finish_histogram_group` refuses a group holding both
            // float and histogram state in any field.
            let folds_histograms = histogram_samples(op) == HistogramSamples::Folded
                && field
                    .iter()
                    .any(|sample| matches!(sample, SampleRef::Histogram(_)));
            if folds_histograms {
                for ((index, _), sample) in series.time.positions().zip(field.iter()) {
                    match sample {
                        SampleRef::Missing => {}
                        SampleRef::Histogram(view) => {
                            retained = self.fold_histogram(fold, index, &view, budget, retained)?;
                        }
                        SampleRef::Float(_) => {
                            return Err(DataFusionError::Execution(
                                "a native aggregate cannot mix float and histogram samples in one field"
                                    .into(),
                            ));
                        }
                    }
                }
                continue;
            }
            let mut samples = series
                .time
                .positions()
                .zip(field.iter())
                .filter_map(|((index, _), sample)| match sample {
                    SampleRef::Float(sample) => Some((index, sample)),
                    SampleRef::Histogram(_) => match histogram_samples(op) {
                        // The value is a placeholder: `count` and `group` never read it.
                        HistogramSamples::Presence => Some((index, 1.0)),
                        HistogramSamples::Ignored => None,
                        HistogramSamples::Folded => {
                            unreachable!("a payload-folding aggregate folds histograms above")
                        }
                    },
                    _ => None,
                })
                .peekable();
            while let Some((index, sample)) = samples.next() {
                let block = index / BLOCK_POINTS;
                let (states, admitted) = self.block_states(block, fold, budget, retained)?;
                retained = admitted;
                let words = fold.op.state_words();
                let start = fold.state_start(index % BLOCK_POINTS);
                FloatState::new(&mut states[start..start + words], fold.op).update(sample);
                while let Some(&(at, sample)) = samples.peek()
                    && at / BLOCK_POINTS == block
                {
                    samples.next();
                    let start = fold.state_start(at % BLOCK_POINTS);
                    FloatState::new(&mut states[start..start + words], fold.op).update(sample);
                }
            }
        }
        Ok(retained)
    }

    /// The states of `block`, admitting and allocating it if new, and the retained bytes after.
    /// Admission precedes insertion, so a refusal leaves no block behind.
    fn block_states<'a>(
        &'a mut self,
        block: usize,
        fold: FieldFold,
        budget: &mut impl FnMut(usize) -> Result<()>,
        retained: usize,
    ) -> Result<(&'a mut [f64], usize)> {
        let new_block = !self.blocks.contains_key(&block);
        let (states_bytes, backing_delta) = if new_block {
            (
                block_bytes(fold.width, fold.op),
                btree_backing_bytes(self.blocks.len() + 1)
                    .saturating_sub(btree_backing_bytes(self.blocks.len())),
            )
        } else {
            (0, 0)
        };
        let retained = retained
            .saturating_add(backing_delta)
            .saturating_add(states_bytes);
        if new_block {
            budget(retained)?;
        }
        let states = self
            .blocks
            .entry(block)
            .or_insert_with(|| vec![0.0; BLOCK_POINTS * fold.point_words()].into_boxed_slice());
        Ok((&mut states[..], retained))
    }
}

impl Group {
    /// Folds one histogram sample into its position's state, admitting what the fold allocates.
    ///
    /// Exponential buckets meeting custom buckets poison the position instead of failing the
    /// statement, as Prometheus drops the point; a poisoned position folds nothing more. Overflow
    /// does not poison: a sum answers `Inf` and an average switches to mean mode.
    fn fold_histogram(
        &mut self,
        fold: FieldFold,
        position: usize,
        view: &HistogramView<'_>,
        budget: &mut impl FnMut(usize) -> Result<()>,
        retained: usize,
    ) -> Result<usize> {
        // The reverse order, a float after a histogram, is caught in `finish_histogram_group`.
        if let Some(states) = self.blocks.get(&(position / BLOCK_POINTS)) {
            let words = fold.op.state_words();
            let start = fold.state_start(position % BLOCK_POINTS);
            if state_seen(&states[start..start + words], fold.op) {
                return Err(DataFusionError::Execution(
                    "a native aggregate cannot mix float and histogram samples in one field".into(),
                ));
            }
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
            budget(
                retained
                    .saturating_add(size_of::<HistogramAccumulator>())
                    .saturating_add(sample.heap_bytes().saturating_mul(2))
                    .saturating_add(backing),
            )?;
        }
        let accumulator = self
            .histograms
            .entry(key)
            .or_insert_with(|| HistogramAccumulator::from_sample(&sample));
        // Hints are recorded even for a poisoned point.
        match sample.reset_hint {
            CounterResetHint::CounterReset => accumulator.evidence.counter_reset_seen = true,
            CounterResetHint::NotCounterReset => accumulator.evidence.not_counter_reset_seen = true,
            _ => {}
        }
        if accumulator.evidence.poisoned {
            return Ok(retained);
        }
        if vacant {
            return Ok(retained
                .saturating_add(accumulator_bytes(accumulator))
                .saturating_add(backing));
        }
        // An add can grow the buckets by at most the sample's payload.
        let before = accumulator_bytes(accumulator);
        budget(
            retained
                .saturating_add(before)
                .saturating_add(sample.heap_bytes()),
        )?;
        if accumulator.uses_custom_buckets() != sample.uses_custom_buckets() {
            accumulator.poison();
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
    /// A fold of `sample` alone. The compensation must come from `new_compensation`: an
    /// `empty_like` histogram has no bucket indices for the compensated add.
    pub(crate) fn from_sample(sample: &FloatHistogram) -> Self {
        Self {
            value: sample.clone(),
            compensation: Some(sample.new_compensation()),
            mean: None,
            count: 1.0,
            evidence: HistogramEvidence::default(),
        }
    }

    /// Whether the live value (the mean once an average switched) uses custom buckets.
    pub(crate) fn uses_custom_buckets(&self) -> bool {
        match &self.mean {
            Some(mean) => mean.uses_custom_buckets(),
            None => self.value.uses_custom_buckets(),
        }
    }

    /// Marks this position as answering nothing because its bucket layouts are incompatible.
    pub(crate) fn poison(&mut self) {
        self.evidence.poisoned = true;
        self.evidence.incompatible_layout = true;
        self.compensation = None;
    }

    /// Adds one sample into the running sum. Does not update `count`.
    pub(crate) fn fold_sum(&mut self, sample: &FloatHistogram) -> Result<()> {
        let (compensation, outcome) = self.value.kahan_add(sample, self.compensation.take())?;
        self.compensation = Some(compensation);
        self.evidence.custom_bounds_reconciled |= outcome.custom_bounds_reconciled;
        Ok(())
    }

    /// Averages one sample in, following Prometheus `aggregation` for `avg`: sum until a trial
    /// add overflows, then switch to an incremental mean where each sample enters as
    /// `sample / count` and the mean is weighted by `(count - 1) / count`, so finite inputs keep a
    /// finite mean.
    pub(crate) fn fold_avg(&mut self, sample: &FloatHistogram) -> Result<()> {
        self.count += 1.0;
        if self.mean.is_none() {
            let mut trial = self.value.clone();
            let (compensation, outcome) = trial.kahan_add(sample, self.compensation.clone())?;
            // Counts whether or not the trial is kept.
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
        // The compensation is scaled with the mean.
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
    /// This position's state as a `Partial` payload: the value (sum, or mean once switched) and
    /// its compensation unfolded. A poisoned position exports empty histograms and its evidence.
    pub(crate) fn payload(&self, op: AggregateOp) -> Result<StatePayload> {
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
            // A sum's state kind carries no count and no mean mode.
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

    pub(crate) fn from_payload(op: AggregateOp, payload: &HistogramState) -> Result<Self> {
        let value = decoded_histogram(&payload.value)?;
        let compensation = decoded_histogram(&payload.compensation)?;
        let switched = op == AggregateOp::Avg && payload.incremental_mean;
        Ok(Self {
            // The pre-switch sum is not exported, so a switched average holds the mean in both.
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
                counter_reset_collision: false,
            },
        })
    }

    /// Merges another partition's state into this position's. Poison is sticky and both sides'
    /// evidence is kept.
    pub(crate) fn merge_state(&mut self, op: AggregateOp, other: &HistogramState) -> Result<()> {
        self.evidence.poisoned |= other.poisoned;
        self.evidence.incompatible_layout |= other.incompatible_layout;
        self.evidence.custom_bounds_reconciled |= other.custom_bounds_reconciled;
        self.evidence.counter_reset_seen |= other.counter_reset_seen;
        self.evidence.not_counter_reset_seen |= other.not_counter_reset_seen;
        if self.evidence.poisoned {
            return Ok(());
        }
        let right = decoded_histogram(&other.value)?;
        let right_compensation = decoded_histogram(&other.compensation)?;
        // Poison, as the fold does, rather than let the add fail the statement.
        let left_uses_custom = self.uses_custom_buckets();
        if left_uses_custom != right.uses_custom_buckets() {
            self.evidence.poisoned = true;
            self.evidence.incompatible_layout = true;
            return Ok(());
        }
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

    /// Merges two sums: the other value, then the other compensation, are each Kahan-added into
    /// the live value.
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

    /// Merges an average: sums add while they stay finite, otherwise count-weighted means do.
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
            // A reconciliation counts whether or not the trial is kept.
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
        // Scale each side's mean (and compensation) by its weight before adding, so no
        // intermediate overflows.
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

/// One histogram as a one-row batch.
pub(crate) fn histogram_batch(histogram: &FloatHistogram) -> Result<Arc<HistogramBatch>> {
    let mut builder = HistogramBatchBuilder::new();
    histogram.push_to(&mut builder)?;
    builder.finish().map_err(external)
}

fn decoded_histogram(batch: &HistogramBatch) -> Result<FloatHistogram> {
    let view = batch.view(0).ok_or_else(|| {
        DataFusionError::Execution("a histogram state component carries no histogram".into())
    })?;
    FloatHistogram::try_from_view(&view)
}

/// The evidence to report for all groups, independent of merge order. A poisoned point reports
/// neither a reconciliation nor a reset collision. A collision is per point: two points that each
/// saw one hint do not collide.
fn evidence_summary(groups: &[Group]) -> HistogramEvidence {
    let mut summary = HistogramEvidence::default();
    for accumulator in groups.iter().flat_map(|group| group.histograms.values()) {
        let evidence = accumulator.evidence;
        summary.poisoned |= evidence.poisoned;
        summary.incompatible_layout |= evidence.incompatible_layout;
        summary.counter_reset_seen |= evidence.counter_reset_seen;
        summary.not_counter_reset_seen |= evidence.not_counter_reset_seen;
        summary.counter_reset_collision |=
            evidence.counter_reset_seen && evidence.not_counter_reset_seen && !evidence.poisoned;
        summary.custom_bounds_reconciled |= evidence.custom_bounds_reconciled && !evidence.poisoned;
    }
    summary
}

/// Bytes one histogram position holds.
pub(crate) fn accumulator_bytes(accumulator: &HistogramAccumulator) -> usize {
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

/// The float part of Prometheus' `groupedAggregation`, over one state's words in place; the layout
/// is documented on [`AggregateOp::state_words`].
struct FloatState<'a> {
    words: &'a mut [f64],
    op: AggregateOp,
}

/// Whether the state in `words` has seen a sample.
fn state_seen(words: &[f64], op: AggregateOp) -> bool {
    words[op.state_words() - 1] != 0.0
}

/// The value the state in `words` finishes to, or `None` if it has seen no sample.
fn state_finish(words: &[f64], op: AggregateOp) -> Option<f64> {
    if !state_seen(words, op) {
        return None;
    }
    Some(match op {
        AggregateOp::Sum => words[0] + words[1],
        AggregateOp::Avg if words[3] != 0.0 => words[2] + words[1],
        AggregateOp::Avg => words[0] / words[4] + words[1] / words[4],
        AggregateOp::Count => words[0],
        AggregateOp::Min | AggregateOp::Max => words[0],
        AggregateOp::Group => 1.0,
        AggregateOp::Stdvar => words[0] / words[2],
        AggregateOp::Stddev => (words[0] / words[2]).sqrt(),
    })
}

impl<'a> FloatState<'a> {
    fn new(words: &'a mut [f64], op: AggregateOp) -> Self {
        debug_assert_eq!(words.len(), op.state_words());
        Self { words, op }
    }

    fn update(self, sample: f64) {
        let (op, words) = (self.op, self.words);
        if !state_seen(words, op) {
            match op {
                AggregateOp::Count => words[0] = 1.0,
                AggregateOp::Group => words[0] = 1.0,
                AggregateOp::Min | AggregateOp::Max => {
                    words[0] = sample;
                    words[1] = 1.0;
                }
                AggregateOp::Sum => {
                    words[0] = sample;
                    words[1] = 0.0;
                    words[2] = 1.0;
                }
                // Prometheus starts the sum of squared deviations at NaN for a non-finite sample.
                AggregateOp::Stddev | AggregateOp::Stdvar => {
                    words[0] = if sample.is_finite() { 0.0 } else { f64::NAN };
                    words[1] = sample;
                    words[2] = 1.0;
                }
                AggregateOp::Avg => {
                    words[0] = sample;
                    words[1] = 0.0;
                    words[2] = sample;
                    words[3] = 0.0;
                    words[4] = 1.0;
                }
            }
            return;
        }
        match op {
            AggregateOp::Sum => {
                (words[0], words[1]) = kahan_inc(sample, words[0], words[1]);
            }
            AggregateOp::Avg => update_avg(words, sample),
            AggregateOp::Count => words[0] += 1.0,
            AggregateOp::Max => {
                if words[0] < sample || words[0].is_nan() {
                    words[0] = sample;
                }
            }
            AggregateOp::Min => {
                if words[0] > sample || words[0].is_nan() {
                    words[0] = sample;
                }
            }
            AggregateOp::Group => {}
            // Welford's update, uncompensated as in Prometheus.
            AggregateOp::Stddev | AggregateOp::Stdvar => {
                let count = words[2] + 1.0;
                let delta = sample - words[1];
                words[1] += delta / count;
                words[0] += delta * (sample - words[1]);
                words[2] = count;
            }
        }
    }
}

/// `avg`'s update: sums until the running sum would overflow, then switches to an incremental
/// mean.
fn update_avg(words: &mut [f64], sample: f64) {
    let count = words[4] + 1.0;
    words[4] = count;
    if words[3] == 0.0 {
        let (value, compensation) = kahan_inc(sample, words[0], words[1]);
        if !value.is_infinite() {
            words[0] = value;
            words[1] = compensation;
            return;
        }
        words[3] = 1.0;
        words[2] = words[0] / (count - 1.0);
        words[1] /= count - 1.0;
    }
    let q = (count - 1.0) / count;
    (words[2], words[1]) = kahan_inc(sample / count, q * words[2], q * words[1]);
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
