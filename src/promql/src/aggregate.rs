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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};

use crate::grid_series::GridSeriesBuilder;
use crate::map::{drop_empty_labels, validate_float_series};

/// Basic float aggregations.
#[derive(Clone, Copy, Debug)]
pub enum AggregateOp {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Group,
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
    let mut accumulator = FloatAccumulator::new(op);
    for series in input {
        accumulator.accumulate(series, grouping)?;
    }
    accumulator.finish(schema)
}

/// The same fold, grown one series at a time instead of taken as a batch.
///
/// [`aggregate_float`] is this type used in one go: the fold's state is the groups it has opened and
/// the blocks each of them filled, and neither depends on how many series arrived at once. A series
/// may therefore be folded in chunks, one chunk at a time, and the result is the result of folding
/// it whole: [`Group::accumulate`] indexes its states by absolute grid position
/// ([`Series::time`](datatypes::series::time::TimeAxis)), never by a local row number, so the
/// positions of a later chunk land in the blocks of an earlier one.
///
/// Nothing about the input is kept: a folded series leaves its values in the states and nothing else,
/// so a runtime that folds a stream holds only what the fold has accumulated.
pub(crate) struct FloatAccumulator {
    op: AggregateOp,
    /// The number of fields a folded series carries, which is how many states one grid point holds.
    /// Kept from the widest series folded, because a block allocates its states when it is opened.
    width: usize,
    group_index: HashMap<LabelKey, usize>,
    groups: Vec<Group>,
}

impl FloatAccumulator {
    /// Rejects a grouping this fold cannot key its states by.
    ///
    /// It is [`aggregate_float`]'s grouping check, reached by a caller that folds a stream rather
    /// than a slice: `by` on a value field needs per-point labels and an output schema adapter, which
    /// this module does not have.
    pub(crate) fn validate_grouping(schema: &SeriesSchema, grouping: Grouping<'_>) -> Result<()> {
        grouping.validate(schema)
    }

    pub(crate) fn new(op: AggregateOp) -> Self {
        Self {
            op,
            width: 0,
            group_index: HashMap::new(),
            groups: Vec::new(),
        }
    }

    /// Folds one series (a chunk's, or a whole one) into its group.
    pub(crate) fn accumulate(&mut self, series: &Series, grouping: Grouping<'_>) -> Result<()> {
        self.width = self.width.max(series.fields.len());
        let metric = grouping.apply(&series.metric)?;
        let index = *self
            .group_index
            .entry(label_key(&metric))
            .or_insert_with(|| {
                self.groups.push(Group {
                    metric,
                    blocks: BTreeMap::new(),
                });
                self.groups.len() - 1
            });
        self.groups[index].accumulate(series, self.op)
    }

    /// The groups this fold has opened, which is what finishing it produces, one series each.
    pub(crate) fn groups(&self) -> usize {
        self.groups.len()
    }

    /// An estimate of the bytes the states hold, which the node owning them reserves for.
    ///
    /// One state block is [`BLOCK_POINTS`] states per field, so the figure is every group's blocks
    /// times that block: `groups x blocks x BLOCK_POINTS x fields x size_of::<FloatState>()`. The
    /// map's buckets, the metric payloads of the groups and the slack of a block's box are left out,
    /// so it is a rough estimate in the sense the runtime's other figures are, not an allocation
    /// bound.
    pub(crate) fn state_bytes(&self) -> usize {
        let blocks = self
            .groups
            .iter()
            .map(|group| group.blocks.len())
            .fold(0usize, usize::saturating_add);
        blocks
            .saturating_mul(BLOCK_POINTS)
            .saturating_mul(self.width)
            .saturating_mul(std::mem::size_of::<FloatState>())
    }

    /// Every group's result, in the order its first input appeared.
    pub(crate) fn finish(self, schema: &Arc<SeriesSchema>) -> Result<Vec<Series>> {
        let op = self.op;
        let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
        let mut output = Vec::with_capacity(self.groups.len());
        let mut row = Vec::with_capacity(schema.fields.len());
        for group in self.groups {
            // One builder serves every group, so each result series starts explicitly.
            builder.begin_series();
            for (&block, states) in &group.blocks {
                // The last block can extend past the grid; only points with a sample are pushed.
                for (offset, point) in states.chunks_exact(schema.fields.len()).enumerate() {
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

/// A group's identity: its name under `__name__` and then its tags, in the order they are held.
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
    fn accumulate(&mut self, series: &Series, op: AggregateOp) -> Result<()> {
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
        Ok(())
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
                value: if matches!(op, AggregateOp::Group) {
                    1.0
                } else {
                    sample
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
