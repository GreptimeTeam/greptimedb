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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};

use crate::aggregate::{
    AggregateOp, BLOCK_POINTS, Counts, FloatAggregator, FloatState, Group, LabelKey, count_series,
    finish_groups, kahan_inc, label_key,
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
    pub metric: MetricName,
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
        let width = self.schema.fields.len();
        let groups = self
            .groups
            .into_iter()
            .map(|group| {
                let mut points = Vec::new();
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
                PartialGroup {
                    metric: group.metric,
                    points,
                }
            })
            .collect();
        PartialAggregate {
            op: self.op,
            groups,
        }
    }
}

/// Merges the partial states of one aggregation into its result. Groups are identified by their
/// full label set, already grouped by the partitions, and keep the order they first arrive in.
pub struct FloatMerger {
    schema: Arc<SeriesSchema>,
    op: AggregateOp,
    group_index: HashMap<LabelKey, usize>,
    groups: Vec<Group>,
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
        })
    }

    /// Merges the state of one partition. A partial that does not fit this merge, by its
    /// aggregation, field count or grid positions, fails and leaves the merge unchanged.
    pub fn merge(&mut self, partial: PartialAggregate) -> Result<()> {
        let width = self.schema.fields.len();
        let points = self
            .schema
            .eval_grid
            .as_ref()
            .expect("checked at construction")
            .timestamps()
            .len();
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
        for group in partial.groups {
            let index = match self.group_index.entry(label_key(&group.metric)) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    self.groups.push(Group {
                        metric: group.metric,
                        blocks: BTreeMap::new(),
                    });
                    *entry.insert(self.groups.len() - 1)
                }
            };
            let blocks = &mut self.groups[index].blocks;
            for (point, states) in group.points {
                let block = blocks.entry(point / BLOCK_POINTS).or_insert_with(|| {
                    vec![FloatState::default(); BLOCK_POINTS * width].into_boxed_slice()
                });
                let offset = (point % BLOCK_POINTS) * width;
                for (slot, state) in block[offset..offset + width].iter_mut().zip(states) {
                    if let Some(state) = state {
                        slot.merge(self.op, FloatState::from_partial(state));
                    }
                }
            }
        }
        Ok(())
    }

    /// The result of every group, in the order the groups first arrived.
    pub fn finish(self) -> Result<Vec<Series>> {
        finish_groups(self.schema, self.op, self.groups)
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
