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

use datafusion::arrow::array::Array;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesLabels;
use datatypes::series::aggregate_state::{
    AggregateStateKind, AggregateStateValues, StateComponent, StateFieldsBatch, StateSeries,
};
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};
use datatypes::series::time::{GridSpan, TimeAxis};

use crate::aggregate::{
    AggregateOp, BLOCK_POINTS, Counts, FloatAggregator, FloatState, Group, LabelKey, block_bytes,
    btree_backing_bytes, count_series, finish_groups, finish_groups_budgeted,
    hash_map_backing_bytes, kahan_inc, label_key, metric_bytes, next_hash_capacity, state_seen,
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
///
/// The fold and the frame carry the same five components, so they share one type: the frame's own
/// [`AggregateStateValues`] is the definition, and this is the name the aggregate side reads it by.
pub use datatypes::series::aggregate_state::AggregateStateValues as AccumulatorState;

/// The state of one execution partition, with its groups in the order they first appeared.
///
/// A group is the frame's own [`StateSeries`]: field-major states addressed on that group's sparse
/// absolute grid positions. Exporting therefore hands the same representation over instead of
/// transposing a second one into it.
#[derive(Clone, Debug)]
pub struct PartialAggregate {
    pub op: AggregateOp,
    pub groups: Vec<StateSeries>,
}

/// One Arrow-backed aggregate state row, independent of executor plumbing.
#[derive(Clone, Debug)]
pub(crate) struct StateRow {
    pub metric: SeriesLabels,
    pub time: TimeAxis,
    pub fields: StateFieldsBatch,
    pub kind: AggregateStateKind,
}

impl FloatAggregator {
    /// The state folded so far, for a merge with the state of other partitions.
    pub fn finish_partial(self) -> Result<PartialAggregate> {
        self.finish_partial_inner()
    }

    /// [`Self::finish_partial`] that admits what it builds first: the fold state and the groups it
    /// is exported as, at the exact capacity [`Self::finish_partial_inner`] gives them. A refusal
    /// consumes the aggregator, so the caller drops the execution.
    pub(crate) fn finish_partial_budgeted(
        self,
        mut budget: impl FnMut(usize) -> Result<()>,
    ) -> Result<PartialAggregate> {
        let width = self.schema.fields.len();
        let mut partial_bytes = self.groups.len().saturating_mul(size_of::<StateSeries>());
        for group in &self.groups {
            let points = seen_points(group, width, self.op);
            partial_bytes = partial_bytes
                .saturating_add(size_of::<TimeAxis>())
                .saturating_add(
                    points.saturating_mul(size_of::<GridSpan>().saturating_add(size_of::<usize>())),
                )
                .saturating_add(
                    width.saturating_mul(size_of::<Vec<Option<AggregateStateValues>>>()),
                )
                .saturating_add(
                    points
                        .saturating_mul(width)
                        .saturating_mul(size_of::<Option<AggregateStateValues>>()),
                );
        }
        budget(self.state_bytes().saturating_add(partial_bytes))?;
        self.finish_partial_inner()
    }

    /// Turns the fold into the frame's own groups: field-major states on sparse absolute positions.
    ///
    /// The blocks are walked once, in ascending position order, so a position's states are appended
    /// to every field at the same time and adjacent positions coalesce into one span as they come.
    /// What comes out is the representation the frame writes; no per-point vector is built.
    fn finish_partial_inner(self) -> Result<PartialAggregate> {
        let width = self.schema.fields.len();
        let grid = self.schema.eval_grid.as_ref().ok_or_else(|| {
            DataFusionError::Execution("aggregate state is indexed on an evaluated grid".into())
        })?;
        let grid_len = grid.spec().len as usize;
        let op = self.op;
        let words = op.state_words();
        let point_words = width.saturating_mul(words);
        let mut groups: Vec<StateSeries> = Vec::with_capacity(self.groups.len());
        for group in self.groups {
            let points = seen_points(&group, width, op);
            let mut spans: Vec<GridSpan> = Vec::with_capacity(points.min(1));
            let mut states: Vec<Vec<Option<AggregateStateValues>>> =
                (0..width).map(|_| Vec::with_capacity(points)).collect();
            for (&block, block_states) in &group.blocks {
                for (offset, point) in block_states.chunks_exact(point_words).enumerate() {
                    if !point.chunks_exact(words).any(|state| state_seen(state, op)) {
                        continue;
                    }
                    let position = block * BLOCK_POINTS + offset;
                    if position >= grid_len {
                        return Err(DataFusionError::Execution(format!(
                            "aggregate state point {position} exceeds a grid of {grid_len}"
                        )));
                    }
                    match spans.last_mut() {
                        Some(span) if span.start() + span.len() == position => {
                            *span = GridSpan::new(span.start(), span.len() + 1);
                        }
                        _ => spans.push(GridSpan::new(position, 1)),
                    }
                    for (field, state) in point.chunks_exact(words).enumerate() {
                        states[field].push(state_partial(state, op));
                    }
                }
            }
            let time = TimeAxis::from_grid_spans(Arc::clone(grid), spans)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            groups.push(StateSeries {
                metric: group.metric,
                time,
                states,
            });
        }
        Ok(PartialAggregate {
            op: self.op,
            groups,
        })
    }
}

/// The points of `group` that carry a state: the ones a partial of it holds.
fn seen_points(group: &Group, width: usize, op: AggregateOp) -> usize {
    let words = op.state_words();
    let point_words = width.saturating_mul(words);
    group.blocks.values().fold(0usize, |count, states| {
        count.saturating_add(
            states
                .chunks_exact(point_words)
                .filter(|point| point.chunks_exact(words).any(|state| state_seen(state, op)))
                .count(),
        )
    })
}

/// Bytes one exported group keeps: its labels and the field vectors, at the capacity they were
/// built with, so spare capacity is paid for rather than skipped.
fn group_bytes(group: &StateSeries) -> usize {
    metric_bytes(&group.metric).saturating_add(group_field_bytes(group))
}

/// Bytes a group's axis and field vectors hold, without its labels.
pub(crate) fn group_field_bytes(group: &StateSeries) -> usize {
    size_of::<TimeAxis>()
        .saturating_add(
            group
                .time
                .runs()
                .count()
                .saturating_mul(size_of::<GridSpan>().saturating_add(size_of::<usize>())),
        )
        .saturating_add(
            group
                .states
                .capacity()
                .saturating_mul(size_of::<Vec<Option<AggregateStateValues>>>()),
        )
        .saturating_add(group.states.iter().fold(0usize, |bytes, field| {
            bytes.saturating_add(
                field
                    .capacity()
                    .saturating_mul(size_of::<Option<AggregateStateValues>>()),
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
            let carried = group.time.len();
            if group.states.len() != width
                || group.states.iter().any(|field| field.len() != carried)
            {
                return Err(DataFusionError::Execution(
                    "aggregate state does not match the grid or fields of the merge".into(),
                ));
            }
            if group.time.positions().any(|(point, _)| point >= points) {
                return Err(DataFusionError::Execution(
                    "aggregate state does not match the grid or fields of the merge".into(),
                ));
            }
        }
        let input_live = partial.groups.iter().fold(
            partial
                .groups
                .capacity()
                .saturating_mul(size_of::<StateSeries>()),
            |bytes, group| bytes.saturating_add(group_bytes(group)),
        );
        for group in partial.groups {
            let StateSeries {
                metric,
                time,
                states,
            } = group;
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
            self.merge_series(index, &time, &states, live, &mut budget)?;
        }
        Ok(())
    }

    /// Merges one Arrow-backed state row directly, without materializing a partial aggregate.
    pub(crate) fn merge_row(
        &mut self,
        row: &StateRow,
        mut budget: impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        let op = crate::native::state::op_of_state(row.kind);
        if op != self.op {
            return Err(DataFusionError::Execution(format!(
                "cannot merge {:?} state into {:?}",
                op, self.op
            )));
        }
        let width = self.schema.fields.len();
        if row.fields.len() != width {
            return Err(DataFusionError::Execution(
                "aggregate state does not match the grid or fields of the merge".into(),
            ));
        }
        let grid_len = self
            .schema
            .eval_grid
            .as_ref()
            .expect("checked at construction")
            .spec()
            .len as usize;
        let mut opened = None;
        let key_workspace = key_workspace_bytes(&row.metric);
        let mut live = None;
        for (point, position) in row.time.positions() {
            if point >= grid_len {
                return Err(DataFusionError::Execution(
                    "aggregate state does not match the grid or fields of the merge".into(),
                ));
            }
            let mut present = false;
            for field in 0..width {
                present |= row
                    .fields
                    .is_present(field, position)
                    .map_err(crate::grid_series::external)?;
            }
            if !present {
                return Err(DataFusionError::Execution(
                    "aggregate state names a position with no contribution".into(),
                ));
            }
            let index = match opened {
                Some(index) => index,
                None => {
                    budget(self.retained.saturating_add(key_workspace))?;
                    let key = label_key(&row.metric);
                    let live_bytes = key_bytes(&key);
                    let index = match self.group_index.get(&key).copied() {
                        Some(index) => index,
                        None => {
                            self.open_group(row.metric.clone(), key, live_bytes, &mut budget)?
                        }
                    };
                    live = Some(live_bytes);
                    opened = Some(index);
                    index
                }
            };
            let mut field = 0;
            let mut state_error = None;
            let states = std::iter::from_fn(|| {
                if field == width || state_error.is_some() {
                    return None;
                }
                let current = field;
                field += 1;
                match state_at(&row.fields, current, position) {
                    Ok(state) => Some(state),
                    Err(error) => {
                        state_error = Some(error);
                        None
                    }
                }
            });
            self.write_point(index, point, states, live.unwrap_or(0), &mut budget)?;
            if let Some(error) = state_error {
                return Err(error);
            }
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
    fn merge_series(
        &mut self,
        index: usize,
        time: &TimeAxis,
        states: &[Vec<Option<AggregateStateValues>>],
        live: usize,
        budget: &mut impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        // The axis carries the absolute positions and the row each one sits at, so a group's states
        // are read field by field at that row: this is the same order the point-major form had.
        for (point, row) in time.positions() {
            let point_states = states.iter().map(|field| field[row]);
            self.write_point(index, point, point_states, live, budget)?;
        }
        Ok(())
    }

    /// Writes one point's states, admitting a missing block before allocating it.
    fn write_point(
        &mut self,
        index: usize,
        point: usize,
        states: impl Iterator<Item = Option<AccumulatorState>>,
        live: usize,
        budget: &mut impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        let width = self.schema.fields.len();
        let op = self.op;
        let words = op.state_words();
        let point_words = width.saturating_mul(words);
        let states_bytes = block_bytes(width, op);
        let blocks = &mut self.groups[index].blocks;
        let block_index = point / BLOCK_POINTS;
        let entries = blocks.len();
        let block = match blocks.entry(block_index) {
            BTreeEntry::Occupied(entry) => entry.into_mut(),
            BTreeEntry::Vacant(entry) => {
                budget(
                    self.retained
                        .saturating_add(live)
                        .saturating_add(states_bytes)
                        .saturating_add(btree_backing_bytes(entries.saturating_add(1))),
                )?;
                self.retained = self
                    .retained
                    .saturating_add(states_bytes)
                    .saturating_add(btree_backing_bytes(entries.saturating_add(1)))
                    .saturating_sub(btree_backing_bytes(entries));
                entry.insert(vec![0.0; BLOCK_POINTS * point_words].into_boxed_slice())
            }
        };
        let offset = (point % BLOCK_POINTS) * point_words;
        for (slot, state) in block[offset..offset + point_words]
            .chunks_exact_mut(words)
            .zip(states)
        {
            if let Some(state) = state {
                FloatState::new(slot, op).merge_partial(&state);
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

/// One present field's state at one position, read from the row's component arrays.
///
/// The components come from the row's own declaration; the native aggregate caller checks
/// compatibility before this runs, so a component list that disagrees is caught before this runs.
fn state_at(
    fields: &StateFieldsBatch,
    field: usize,
    position: usize,
) -> Result<Option<AccumulatorState>> {
    if !fields
        .is_present(field, position)
        .map_err(crate::grid_series::external)?
    {
        return Ok(None);
    }
    let mut state = AccumulatorState::default();
    for &component in fields.components() {
        match component {
            StateComponent::IncrementalMean => {
                state.incremental_mean = fields
                    .field_bool_values(field, component)
                    .map_err(crate::grid_series::external)?
                    .and_then(|array| (!array.is_null(position)).then(|| array.value(position)))
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "present aggregate state has a missing component".into(),
                        )
                    })?;
            }
            _ => {
                let value = fields
                    .field_values(field, component)
                    .map_err(crate::grid_series::external)?
                    .and_then(|array| (!array.is_null(position)).then(|| array.value(position)))
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "present aggregate state has a missing component".into(),
                        )
                    })?;
                match component {
                    StateComponent::Value => state.value = value,
                    StateComponent::Mean => state.mean = value,
                    StateComponent::Compensation => state.compensation = value,
                    StateComponent::Count => state.count = value,
                    StateComponent::IncrementalMean => unreachable!(),
                }
            }
        }
    }
    Ok(Some(state))
}

/// The exported form of the state in `words`, or `None` if it has seen no sample.
///
/// Only the components the op keeps are filled in; the rest stay at their defaults, exactly as the
/// frame that carries them leaves them out.
fn state_partial(words: &[f64], op: AggregateOp) -> Option<AccumulatorState> {
    if !state_seen(words, op) {
        return None;
    }
    Some(match op {
        // `value`, its compensation, then the presence flag.
        AggregateOp::Sum => AccumulatorState {
            value: words[0],
            compensation: words[1],
            ..Default::default()
        },
        // `value`, `compensation`, `mean`, the incremental flag, then the count.
        AggregateOp::Avg => AccumulatorState {
            value: words[0],
            compensation: words[1],
            mean: words[2],
            incremental_mean: words[3] != 0.0,
            count: words[4],
        },
        AggregateOp::Count => AccumulatorState {
            count: words[0],
            ..Default::default()
        },
        AggregateOp::Min | AggregateOp::Max => AccumulatorState {
            value: words[0],
            ..Default::default()
        },
        AggregateOp::Group => AccumulatorState::default(),
        // `value`, `mean`, then the count.
        AggregateOp::Stddev | AggregateOp::Stdvar => AccumulatorState {
            value: words[0],
            mean: words[1],
            count: words[2],
            ..Default::default()
        },
    })
}

impl<'a> FloatState<'a> {
    /// Writes `other` into this state, for a state that has seen nothing yet.
    fn store_partial(self, other: &AccumulatorState) {
        let (op, words) = (self.op, self.words);
        match op {
            AggregateOp::Count => words[0] = other.count,
            // Presence alone, whatever the other side keeps: `group` answers 1 either way.
            AggregateOp::Group => words[0] = 1.0,
            AggregateOp::Min | AggregateOp::Max => {
                words[0] = other.value;
                words[1] = 1.0;
            }
            AggregateOp::Sum => {
                words[0] = other.value;
                words[1] = other.compensation;
                words[2] = 1.0;
            }
            AggregateOp::Stddev | AggregateOp::Stdvar => {
                words[0] = other.value;
                words[1] = other.mean;
                words[2] = other.count;
            }
            AggregateOp::Avg => {
                words[0] = other.value;
                words[1] = other.compensation;
                words[2] = other.mean;
                words[3] = if other.incremental_mean { 1.0 } else { 0.0 };
                words[4] = other.count;
            }
        }
    }

    /// Folds the state of another partition into this one.
    fn merge_partial(self, other: &AccumulatorState) {
        if !state_seen(self.words, self.op) {
            self.store_partial(other);
            return;
        }
        let (op, words) = (self.op, self.words);
        match op {
            AggregateOp::Sum => {
                (words[0], words[1]) =
                    add_compensated((words[0], words[1]), (other.value, other.compensation));
            }
            AggregateOp::Count => words[0] += other.count,
            AggregateOp::Group => {}
            AggregateOp::Max => {
                if words[0] < other.value || words[0].is_nan() {
                    words[0] = other.value;
                }
            }
            AggregateOp::Min => {
                if words[0] > other.value || words[0].is_nan() {
                    words[0] = other.value;
                }
            }
            AggregateOp::Avg => merge_avg(words, other),
            AggregateOp::Stddev | AggregateOp::Stdvar => {
                // Chan et al.'s pairwise update. `delta * (delta * weight)` keeps the product
                // finite where it is: `delta²` alone overflows for `[0]` and `[1.5e154]`, whose
                // merged sum of squares is finite.
                let count = words[2] + other.count;
                let delta = other.mean - words[1];
                let weight = words[2] * other.count / count;
                words[1] += delta * (other.count / count);
                words[0] = words[0] + other.value + delta * (delta * weight);
                words[2] = count;
            }
        }
    }
}

/// Adds the sums while they stay finite, and otherwise continues with the count-weighted mean, as
/// the serial update switches to an incremental mean once its sum overflows.
///
/// The state words are `value`, `compensation`, `mean`, the incremental flag and the count.
fn merge_avg(words: &mut [f64], other: &AccumulatorState) {
    let count = words[4] + other.count;
    if words[3] == 0.0 && !other.incremental_mean {
        let (value, compensation) =
            add_compensated((words[0], words[1]), (other.value, other.compensation));
        if !value.is_infinite() {
            words[0] = value;
            words[1] = compensation;
            words[4] = count;
            return;
        }
    }
    let (mean, compensation) = mean_form(words[0], words[1], words[2], words[3] != 0.0, words[4]);
    let (other_mean, other_compensation) = mean_form(
        other.value,
        other.compensation,
        other.mean,
        other.incremental_mean,
        other.count,
    );
    let (weight, other_weight) = (words[4] / count, other.count / count);
    (words[2], words[1]) = kahan_inc(
        other_mean * other_weight,
        mean * weight,
        compensation * weight + other_compensation * other_weight,
    );
    words[3] = 1.0;
    words[4] = count;
}

/// The mean and its compensation, converting a sum as the serial update does on switching.
fn mean_form(
    value: f64,
    compensation: f64,
    mean: f64,
    incremental: bool,
    count: f64,
) -> (f64, f64) {
    if incremental {
        (mean, compensation)
    } else {
        (value / count, compensation / count)
    }
}

/// The compensated sum of two partial sums. Both the other sum and its compensation are added as
/// Kahan addends: adding the compensation plainly would drop what the compensation already
/// holds once later partials cancel the sums, as merging `[1e100, 1]`, `[-1e100, 1e84]` and
/// `[-1e84]` would lose the `1`.
fn add_compensated(this: (f64, f64), other: (f64, f64)) -> (f64, f64) {
    let (value, compensation) = kahan_inc(other.0, this.0, this.1);
    kahan_inc(other.1, value, compensation)
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, ListArray, StructArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datatypes::series::aggregate_state::{AggregateStateSchema, StateComponent};
    use datatypes::series::samples::FieldSpec;
    use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};

    fn fields_batch() -> StateFieldsBatch {
        let item_fields: datafusion::arrow::datatypes::Fields = vec![
            Field::new("value", DataType::Float64, true),
            Field::new("compensation", DataType::Float64, true),
        ]
        .into();
        let make_field =
            |values: Vec<Option<f64>>, compensation: Vec<Option<f64>>, validity: Vec<bool>| {
                let item = StructArray::new(
                    item_fields.clone(),
                    vec![
                        Arc::new(Float64Array::from(values)),
                        Arc::new(Float64Array::from(compensation)),
                    ],
                    Some(NullBuffer::from(validity)),
                );
                Arc::new(ListArray::new(
                    Arc::new(Field::new(
                        "item",
                        DataType::Struct(item_fields.clone()),
                        true,
                    )),
                    OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 3])),
                    Arc::new(item),
                    None,
                ))
            };
        let field_a = make_field(
            vec![Some(2.), Some(3.), Some(4.)],
            vec![Some(0.), Some(0.), Some(0.)],
            vec![true, true, true],
        );
        let field_b = make_field(
            vec![Some(5.), None, Some(7.)],
            vec![Some(0.), None, Some(0.)],
            vec![true, false, true],
        );
        let outer_fields = vec![
            Field::new("a", field_a.data_type().clone(), false),
            Field::new("b", field_b.data_type().clone(), false),
        ]
        .into();
        let outer = StructArray::new(outer_fields, vec![field_a, field_b], None);
        let schema = AggregateStateSchema::try_new(
            AggregateStateKind::Sum,
            vec![StateComponent::Value, StateComponent::Compensation],
        )
        .unwrap();
        StateFieldsBatch::try_from_arrow(outer, &schema).unwrap()
    }

    fn schema() -> Arc<SeriesSchema> {
        Arc::new(SeriesSchema {
            fields: vec![
                FieldSpec { name: "a".into() },
                FieldSpec { name: "b".into() },
            ]
            .into(),
            eval_grid: Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(0, 1, 3).unwrap()).unwrap(),
            )),
        })
    }

    #[test]
    fn merge_row_matches_partial_merge_with_field_hole() {
        let schema = schema();
        let metric = SeriesLabels::try_new(Some("m".into()), Vec::new()).unwrap();
        let row = StateRow {
            metric: metric.clone(),
            time: TimeAxis::Grid(
                GridRange::try_new(schema.eval_grid.clone().unwrap(), 0, 3).unwrap(),
            ),
            fields: fields_batch(),
            kind: AggregateStateKind::Sum,
        };
        let mut from_row = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
        from_row.merge_row(&row, |_| Ok(())).unwrap();
        let mut from_partial = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
        from_partial
            .merge(PartialAggregate {
                op: AggregateOp::Sum,
                groups: vec![StateSeries {
                    metric,
                    time: TimeAxis::Grid(
                        GridRange::try_new(schema.eval_grid.clone().unwrap(), 0, 3).unwrap(),
                    ),
                    states: vec![
                        vec![
                            Some(AccumulatorState {
                                value: 2.0,
                                ..Default::default()
                            }),
                            Some(AccumulatorState {
                                value: 3.0,
                                ..Default::default()
                            }),
                            Some(AccumulatorState {
                                value: 4.0,
                                ..Default::default()
                            }),
                        ],
                        vec![
                            Some(AccumulatorState {
                                value: 5.0,
                                ..Default::default()
                            }),
                            None,
                            Some(AccumulatorState {
                                value: 7.0,
                                ..Default::default()
                            }),
                        ],
                    ],
                }],
            })
            .unwrap();
        assert_eq!(
            format!("{:?}", from_row.finish().unwrap()),
            format!("{:?}", from_partial.finish().unwrap()),
        );
    }
}
