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

//! Partial aggregation: the state one execution partition folds its series into, and the merge
//! of such states into the final result.
//!
//! A merge regroups the operations of a serial fold, so floating-point results differ from
//! [`aggregate_float`](crate::aggregate::aggregate_float) by rounding. The same reassociation can
//! also move an overflow of finite values: a serial `sum` of `[1e308, 1e308, -1e308]` is `+Inf`,
//! while the merge of `[1e308]` and `[1e308, -1e308]` is finite. `stddev` and `stdvar` differ
//! more where the samples differ by little more than the rounding of their mean: Prometheus'
//! uncompensated Welford update rounds the mean on every sample, and no pairwise merge reproduces
//! those roundings. For `[1e16]` and `[1e16 + 2]` Prometheus gives a variance of 2 and the merge
//! gives the exact 1. Explicit NaN and infinite inputs follow the same rules as the serial fold. Where the serial fold keeps the first of two equal
//! values, as `min` and `max` do for `0` and `-0`, a merge keeps the one merged first.

use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesLabels;
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};

use crate::aggregate::{
    AggregateOp, BLOCK_POINTS, Counts, FloatAggregator, FloatState, Group, LabelKey,
    btree_backing_bytes, count_series, finish_groups, finish_groups_budgeted,
    hash_map_backing_bytes, kahan_inc, label_key, metric_bytes, next_hash_capacity,
};
use crate::map::validate_float_series;

/// A component of [`AccumulatorState`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateField {
    /// The running sum (`sum`, and `avg` until it switches to a mean), the extreme (`min`,
    /// `max`), or the sum of squared deviations (`stddev`, `stdvar`).
    Value,
    /// The running mean (`avg` once switched, `stddev`, `stdvar`).
    Mean,
    /// The Kahan compensation of `Value`, or of `Mean` once `avg` has switched.
    Compensation,
    /// The number of samples folded in.
    Count,
    /// Whether `avg` has switched from a sum to a mean, which it does when the sum overflows.
    IncrementalMean,
}

impl AggregateOp {
    /// The components the state of this aggregation carries; the others are unused and need not
    /// be stored. `group` has none: a state that exists has a value of 1.
    pub fn state_fields(self) -> &'static [StateField] {
        use StateField::*;
        match self {
            Self::Sum => &[Value, Compensation],
            Self::Count => &[Count],
            Self::Avg => &[Value, Mean, Compensation, Count, IncrementalMean],
            Self::Min | Self::Max => &[Value],
            Self::Group => &[],
            Self::Stddev | Self::Stdvar => &[Value, Mean, Count],
        }
    }
}

/// The state of one field of one group at one grid point.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AccumulatorState {
    pub value: f64,
    pub mean: f64,
    pub compensation: f64,
    pub count: f64,
    pub incremental_mean: bool,
}

/// The state of one group: its points in ascending order, each with one state per field, `None`
/// where no sample of that field was folded in.
#[derive(Clone, Debug)]
pub struct PartialGroup {
    pub metric: SeriesLabels,
    pub points: Vec<(usize, Vec<Option<AccumulatorState>>)>,
}

/// The state of one execution partition, with its groups in the order they first appeared.
#[derive(Clone, Debug)]
pub struct PartialAggregate {
    pub op: AggregateOp,
    pub groups: Vec<PartialGroup>,
}

impl FloatAggregator {
    /// The state folded so far, for a merge with the state of other partitions.
    pub fn finish_partial(self) -> PartialAggregate {
        self.finish_partial_inner()
    }

    /// [`Self::finish_partial`] that admits what it builds first: the fold state and the group,
    /// point and state vectors, at the exact capacity [`Self::finish_partial_inner`] gives them. A
    /// refusal consumes the aggregator, so the caller drops the execution.
    pub(crate) fn finish_partial_budgeted(
        self,
        mut budget: impl FnMut(usize) -> Result<()>,
    ) -> Result<PartialAggregate> {
        let width = self.schema.fields.len();
        let mut partial_bytes = self.groups.len().saturating_mul(size_of::<PartialGroup>());
        for group in &self.groups {
            let points = seen_points(group, width);
            partial_bytes = partial_bytes
                .saturating_add(points.saturating_mul(size_of::<Point>()))
                .saturating_add(
                    points
                        .saturating_mul(width)
                        .saturating_mul(size_of::<Option<AccumulatorState>>()),
                );
        }
        budget(self.state_bytes().saturating_add(partial_bytes))?;
        Ok(self.finish_partial_inner())
    }

    fn finish_partial_inner(self) -> PartialAggregate {
        let width = self.schema.fields.len();
        let mut groups: Vec<PartialGroup> = Vec::with_capacity(self.groups.len());
        for group in self.groups {
            let mut points: Vec<Point> = Vec::with_capacity(seen_points(&group, width));
            for (&block, states) in &group.blocks {
                for (offset, point) in states.chunks_exact(width).enumerate() {
                    if point.iter().any(|state| state.seen) {
                        points.push((
                            block * BLOCK_POINTS + offset,
                            point.iter().map(|state| state.partial()).collect(),
                        ));
                    }
                }
            }
            groups.push(PartialGroup {
                metric: group.metric,
                points,
            });
        }
        PartialAggregate {
            op: self.op,
            groups,
        }
    }
}

/// One point of a partial group: its grid position and one state per field.
type Point = (usize, Vec<Option<AccumulatorState>>);

/// The points of `group` that carry a state: the ones a partial of it holds.
fn seen_points(group: &Group, width: usize) -> usize {
    group.blocks.values().fold(0usize, |count, states| {
        count.saturating_add(
            states
                .chunks_exact(width)
                .filter(|point| point.iter().any(|state| state.seen))
                .count(),
        )
    })
}

/// Bytes one partial group keeps: its labels and its point and state vectors at the capacity they
/// were built with, so spare capacity is paid for rather than skipped.
fn group_bytes(group: &PartialGroup) -> usize {
    metric_bytes(&group.metric)
        .saturating_add(group.points.capacity().saturating_mul(size_of::<Point>()))
        .saturating_add(group.points.iter().fold(0usize, |bytes, (_, states)| {
            bytes.saturating_add(
                states
                    .capacity()
                    .saturating_mul(size_of::<Option<AccumulatorState>>()),
            )
        }))
}

/// Bytes a group's label key holds: a pair per capacity slot, and the `__name__` entry, whose
/// label and value `label_key` copies, each with its own `Arc` header. The other label strings are
/// the metric's, already counted with it.
fn key_bytes(key: &LabelKey) -> usize {
    let name = key
        .iter()
        .find(|(label, _)| &**label == "__name__")
        .map_or(0, |(label, value)| {
            label
                .len()
                .saturating_add(value.len())
                .saturating_add(4 * size_of::<usize>())
        });
    key.capacity()
        .saturating_mul(size_of::<(Arc<str>, Arc<str>)>())
        .saturating_add(name)
}

/// Coarse bytes the key of `metric` holds while it is built: a pair per slot, the `__name__` label
/// and a copy of the metric name, each with its `Arc` header.
fn key_workspace_bytes(metric: &SeriesLabels) -> usize {
    metric
        .tags()
        .len()
        .saturating_add(1)
        .saturating_mul(size_of::<(Arc<str>, Arc<str>)>())
        .saturating_add("__name__".len())
        .saturating_add(metric.name().map_or(0, str::len))
        .saturating_add(4 * size_of::<usize>())
        .saturating_add(size_of::<LabelKey>())
}

/// Merges the partial states of one aggregation into its result. Groups are identified by their
/// full label set, already grouped by the partitions, and keep the order they first arrive in.
pub struct FloatMerger {
    schema: Arc<SeriesSchema>,
    op: AggregateOp,
    group_index: HashMap<LabelKey, usize>,
    groups: Vec<Group>,
    retained: usize,
}

impl FloatMerger {
    pub fn try_new(schema: &Arc<SeriesSchema>, op: AggregateOp) -> Result<Self> {
        if schema.eval_grid.is_none() {
            return Err(DataFusionError::Execution(
                "Aggregate requires an evaluated grid".into(),
            ));
        }
        Ok(Self {
            schema: schema.clone(),
            op,
            group_index: HashMap::new(),
            groups: Vec::new(),
            retained: 0,
        })
    }

    /// Merges the state of one partition. A partial that does not fit this merge, by its
    /// aggregation, field count or grid positions, fails and leaves the merge unchanged.
    pub fn merge(&mut self, partial: PartialAggregate) -> Result<()> {
        self.merge_budgeted(partial, |_| Ok(()))
    }

    /// Bytes this merge keeps: groups, labels, blocks, B-tree nodes, index table and its keys. Added
    /// to by the allocations that change it, never recounted.
    pub(crate) fn state_bytes(&self) -> usize {
        self.retained
    }

    /// [`Self::merge`] that admits each allocation before making it: the partial that stays live,
    /// the key, and the group, index and block backing about to be reserved. A refusal leaves the
    /// merge half applied, so a caller that budgets must drop it rather than merge into it again.
    pub(crate) fn merge_budgeted(
        &mut self,
        partial: PartialAggregate,
        mut budget: impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        let width = self.schema.fields.len();
        let points = self
            .schema
            .eval_grid
            .as_ref()
            .expect("checked at construction")
            .spec()
            .len as usize;
        if partial.op != self.op {
            return Err(DataFusionError::Execution(format!(
                "cannot merge {:?} state into {:?}",
                partial.op, self.op
            )));
        }
        for group in &partial.groups {
            for (point, states) in &group.points {
                if *point >= points || states.len() != width {
                    return Err(DataFusionError::Execution(
                        "aggregate state does not match the grid or fields of the merge".into(),
                    ));
                }
            }
        }
        let input_live = partial.groups.iter().fold(
            partial
                .groups
                .capacity()
                .saturating_mul(size_of::<PartialGroup>()),
            |bytes, group| bytes.saturating_add(group_bytes(group)),
        );
        for group in partial.groups {
            let metric = group.metric;
            // `label_key` copies the metric name into its own `Arc`, so the copy is admitted
            // before it is made. A group that is already merged needs a key too, and its
            // admission comes before anything else this call can allocate.
            budget(
                self.retained
                    .saturating_add(input_live)
                    .saturating_add(key_workspace_bytes(&metric)),
            )?;
            let key = label_key(&metric);
            let live = input_live.saturating_add(key_bytes(&key));
            let index = match self.group_index.get(&key).copied() {
                Some(index) => index,
                None => self.open_group(metric, key, live, &mut budget)?,
            };
            self.merge_points(index, group.points, live, &mut budget)?;
        }
        Ok(())
    }

    /// Adds a group and its key, admitting the group vector and index growth first.
    fn open_group(
        &mut self,
        metric: SeriesLabels,
        key: LabelKey,
        live: usize,
        budget: &mut impl FnMut(usize) -> Result<()>,
    ) -> Result<usize> {
        let capacity = self.groups.capacity();
        let next_capacity = if self.groups.len() == capacity {
            capacity.saturating_mul(2).max(4)
        } else {
            capacity
        };
        let map_capacity = self.group_index.capacity();
        let next_map_capacity = if self.group_index.len().saturating_add(1) > map_capacity {
            next_hash_capacity(map_capacity)
        } else {
            map_capacity
        };
        let key_live = key_bytes(&key);
        let metric_live = metric_bytes(&metric);
        // A realloc that cannot grow in place keeps both buffers alive while it copies, so a
        // growing step admits the new buffer beside the old one, which `retained` already holds.
        budget(
            self.retained
                .saturating_add(live)
                .saturating_add(key_live)
                .saturating_add(metric_live)
                .saturating_add(if next_capacity > capacity {
                    next_capacity.saturating_mul(size_of::<Group>())
                } else {
                    0
                })
                .saturating_add(if next_map_capacity > map_capacity {
                    hash_map_backing_bytes(next_map_capacity)
                } else {
                    0
                }),
        )?;
        self.groups.try_reserve(1).map_err(|error| {
            DataFusionError::Execution(format!("cannot reserve aggregate groups: {error}"))
        })?;
        self.groups.push(Group {
            metric,
            blocks: BTreeMap::new(),
        });
        let index = self.groups.len() - 1;
        self.group_index.insert(key, index);
        self.retained = self
            .retained
            .saturating_add(
                self.groups
                    .capacity()
                    .saturating_sub(capacity)
                    .saturating_mul(size_of::<Group>()),
            )
            .saturating_add(
                hash_map_backing_bytes(self.group_index.capacity())
                    .saturating_sub(hash_map_backing_bytes(map_capacity)),
            )
            .saturating_add(metric_live)
            .saturating_add(key_live);
        Ok(index)
    }

    /// Adds the points of one partial group to group `index`. A missing block is admitted with its
    /// states and B-tree node before either is allocated; an existing one only merges state.
    fn merge_points(
        &mut self,
        index: usize,
        points: Vec<Point>,
        live: usize,
        budget: &mut impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        let width = self.schema.fields.len();
        let block_bytes = BLOCK_POINTS
            .saturating_mul(width)
            .saturating_mul(size_of::<FloatState>());
        let blocks = &mut self.groups[index].blocks;
        for (point, states) in points {
            let block_index = point / BLOCK_POINTS;
            let entries = blocks.len();
            let block = match blocks.entry(block_index) {
                BTreeEntry::Occupied(entry) => entry.into_mut(),
                BTreeEntry::Vacant(entry) => {
                    budget(
                        self.retained
                            .saturating_add(live)
                            .saturating_add(block_bytes)
                            .saturating_add(btree_backing_bytes(entries.saturating_add(1))),
                    )?;
                    self.retained = self
                        .retained
                        .saturating_add(block_bytes)
                        .saturating_add(btree_backing_bytes(entries.saturating_add(1)))
                        .saturating_sub(btree_backing_bytes(entries));
                    entry.insert(
                        vec![FloatState::default(); BLOCK_POINTS * width].into_boxed_slice(),
                    )
                }
            };
            let offset = (point % BLOCK_POINTS) * width;
            for (slot, state) in block[offset..offset + width].iter_mut().zip(states) {
                if let Some(state) = state {
                    slot.merge(self.op, FloatState::from_partial(state));
                }
            }
        }
        Ok(())
    }

    /// The result of every group, in the order the groups first arrived.
    pub fn finish(self) -> Result<Vec<Series>> {
        finish_groups(self.schema, self.op, self.groups)
    }

    /// [`Self::finish`] that admits the result it builds, on top of what the merge still holds. The
    /// merge is consumed either way, so a caller that budgets drops it on a refusal.
    pub(crate) fn finish_budgeted(
        self,
        budget: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<Series>> {
        finish_groups_budgeted(self.schema, self.op, self.groups, self.retained, budget)
    }
}

impl FloatState {
    fn partial(&self) -> Option<AccumulatorState> {
        self.seen.then_some(AccumulatorState {
            value: self.value,
            mean: self.mean,
            compensation: self.compensation,
            count: self.count,
            incremental_mean: self.incremental_mean,
        })
    }

    fn from_partial(state: AccumulatorState) -> Self {
        Self {
            seen: true,
            value: state.value,
            mean: state.mean,
            compensation: state.compensation,
            count: state.count,
            incremental_mean: state.incremental_mean,
        }
    }

    /// Folds the state of another partition into this one.
    fn merge(&mut self, op: AggregateOp, other: Self) {
        if !self.seen {
            *self = other;
            return;
        }
        match op {
            AggregateOp::Sum => {
                (self.value, self.compensation) = add_compensated(*self, other);
            }
            AggregateOp::Count => self.count += other.count,
            AggregateOp::Group => {}
            AggregateOp::Max => {
                if self.value < other.value || self.value.is_nan() {
                    self.value = other.value;
                }
            }
            AggregateOp::Min => {
                if self.value > other.value || self.value.is_nan() {
                    self.value = other.value;
                }
            }
            AggregateOp::Avg => self.merge_avg(other),
            AggregateOp::Stddev | AggregateOp::Stdvar => {
                // Chan et al.'s pairwise update. `delta * (delta * weight)` keeps the product
                // finite where it is: `delta²` alone overflows for `[0]` and `[1.5e154]`, whose
                // merged sum of squares is finite.
                let count = self.count + other.count;
                let delta = other.mean - self.mean;
                let weight = self.count * other.count / count;
                self.mean += delta * (other.count / count);
                self.value = self.value + other.value + delta * (delta * weight);
                self.count = count;
            }
        }
    }

    /// Adds the sums while they stay finite, and otherwise continues with the count-weighted
    /// mean, as the serial update switches to an incremental mean once its sum overflows.
    fn merge_avg(&mut self, other: Self) {
        let count = self.count + other.count;
        if !self.incremental_mean && !other.incremental_mean {
            let (value, compensation) = add_compensated(*self, other);
            if !value.is_infinite() {
                self.value = value;
                self.compensation = compensation;
                self.count = count;
                return;
            }
        }
        let (mean, compensation) = self.mean_form();
        let (other_mean, other_compensation) = other.mean_form();
        let (weight, other_weight) = (self.count / count, other.count / count);
        (self.mean, self.compensation) = kahan_inc(
            other_mean * other_weight,
            mean * weight,
            compensation * weight + other_compensation * other_weight,
        );
        self.incremental_mean = true;
        self.count = count;
    }

    /// The mean and its compensation, converting a sum as the serial update does on switching.
    fn mean_form(&self) -> (f64, f64) {
        if self.incremental_mean {
            (self.mean, self.compensation)
        } else {
            (self.value / self.count, self.compensation / self.count)
        }
    }
}

/// The compensated sum of two partial sums. Both the other sum and its compensation are added as
/// Kahan addends: adding the compensation plainly would drop what the compensation already
/// holds once later partials cancel the sums, as merging `[1e100, 1]`, `[-1e100, 1e84]` and
/// `[-1e84]` would lose the `1`.
fn add_compensated(this: FloatState, other: FloatState) -> (f64, f64) {
    let (value, compensation) = kahan_inc(other.value, this.value, this.compensation);
    kahan_inc(other.compensation, value, compensation)
}

/// Merges `count_values` results of several partitions. Each partial already carries its full
/// output label set, value label included, so series with one label set add their counts point
/// by point and field by field.
pub fn merge_count_values(
    schema: &Arc<SeriesSchema>,
    partials: impl IntoIterator<Item = Series>,
) -> Result<Vec<Series>> {
    let width = schema.fields.len();
    let mut group_index: HashMap<LabelKey, usize> = HashMap::new();
    let mut groups: Counts = Vec::new();
    for series in partials {
        validate_float_series(schema, &series)?;
        let index = match group_index.entry(label_key(&series.metric)) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                groups.push((series.metric.clone(), BTreeMap::new()));
                *entry.insert(groups.len() - 1)
            }
        };
        for (field_index, field) in series.fields.iter().enumerate() {
            for ((point, _), sample) in series.time.positions().zip(field.iter()) {
                if let SampleRef::Float(count) = sample {
                    groups[index]
                        .1
                        .entry(point)
                        .or_insert_with(|| vec![0.0; width])[field_index] += count;
                }
            }
        }
    }
    count_series(schema, groups)
}
