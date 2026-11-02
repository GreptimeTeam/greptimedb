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

//! The aggregate-state chunk a native `Partial`/`Final` aggregate passes between executions.
//!
//! A state chunk is not a query value. It names the group a partition folded and the grid
//! positions that group has contributions at, and it carries the accumulator components its
//! aggregate declared -- nothing else. The conversions here are the only place kernel state
//! ([`PartialAggregate`]) and frame state ([`StateSeries`]) meet, so a hole in one is a hole in the
//! other: a field with no contribution at a carried position stays absent through both directions
//! rather than being filled in with a zero that a later merge would count.

use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::aggregate_state::{
    AggregateStateKind, AggregateStateValues, StateComponent, StateSeries,
};
use datatypes::series::frame::StateFrameRow;
use datatypes::series::samples::SeriesSchema;
use datatypes::series::time::{GridSpan, TimeAxis};

use crate::aggregate::{AggregateOp, PartialAggregate, PartialGroup};
use crate::native::plan::AggregateStateSchema;

/// One partition's aggregate state, either in producer series form or as a decoded consumer row.
#[derive(Clone, Debug)]
pub(crate) enum StateChunk {
    Series {
        series: Vec<StateSeries>,
        state_schema: Arc<AggregateStateSchema>,
    },
    Row(crate::aggregate::state::StateRow),
}

/// The state kind an aggregate's states are states of.
///
/// Every kind this returns is one the kernel keeps a mergeable partial for; `quantile` and the
/// other non-mergeable aggregates are rejected earlier, when the compiler binds the tree.
pub(crate) fn state_kind(op: AggregateOp) -> AggregateStateKind {
    match op {
        AggregateOp::Sum => AggregateStateKind::Sum,
        AggregateOp::Count => AggregateStateKind::Count,
        AggregateOp::Avg => AggregateStateKind::Avg,
        AggregateOp::Min => AggregateStateKind::Min,
        AggregateOp::Max => AggregateStateKind::Max,
        AggregateOp::Group => AggregateStateKind::Group,
        AggregateOp::Stddev => AggregateStateKind::Stddev,
        AggregateOp::Stdvar => AggregateStateKind::Stdvar,
    }
}

/// The accumulator components an aggregate's state carries, in kernel order.
///
/// Derived from [`AggregateOp::state_fields`](crate::aggregate::AggregateOp::state_fields) so the
/// frame layout and the kernel cannot drift apart: adding a component to a kernel adds a column to
/// its state frames in the same change.
fn state_components(op: AggregateOp) -> Vec<StateComponent> {
    op.state_fields()
        .iter()
        .map(|field| match field {
            crate::aggregate::StateField::Value => StateComponent::Value,
            crate::aggregate::StateField::Mean => StateComponent::Mean,
            crate::aggregate::StateField::Compensation => StateComponent::Compensation,
            crate::aggregate::StateField::Count => StateComponent::Count,
            crate::aggregate::StateField::IncrementalMean => StateComponent::IncrementalMean,
        })
        .collect()
}

/// Rejects a bound state whose kind or component layout is not what `op` produces.
///
/// Both halves matter. A frame carries its kind so a `sum` state is never merged as an `avg` one,
/// and it carries the component list so a merge reads exactly the columns the producer wrote. Two
/// states of one kind over the same field count and grid are still not interchangeable if their
/// components differ, which is why the comparison is over the list and not over its length.
fn ensure_bound_state(op: AggregateOp, state_schema: &AggregateStateSchema) -> Result<()> {
    if state_schema.kind != state_kind(op) {
        return Err(DataFusionError::Execution(format!(
            "aggregate state of kind {:?} is not {:?} state",
            state_schema.kind,
            state_kind(op)
        )));
    }
    let expected = state_components(op);
    if state_schema.components.as_ref() != expected.as_slice() {
        return Err(DataFusionError::Execution(format!(
            "aggregate state components {:?} are not what {:?} produces: {:?}",
            state_schema.components, op, expected
        )));
    }
    Ok(())
}

/// Converts one partition's kernel state into the frame state a consumer decodes.
///
/// Points arrive ascending and already sparse -- the kernel only records a point some field of
/// which was folded -- so the axis is built from their runs and every field keeps its own holes.
pub(crate) fn state_chunk_from_partial(
    partial: PartialAggregate,
    schema: &Arc<SeriesSchema>,
    state_schema: Arc<AggregateStateSchema>,
    mut budget: impl FnMut(usize) -> Result<()>,
) -> Result<StateChunk> {
    let grid = schema.eval_grid.as_ref().ok_or_else(|| {
        DataFusionError::Execution("aggregate state is indexed on an evaluated grid".into())
    })?;
    ensure_bound_state(partial.op, state_schema.as_ref())?;

    let width = schema.fields.len();
    let mut series = Vec::new();
    budget(
        partial
            .groups
            .len()
            .saturating_mul(size_of::<StateSeries>()),
    )?;
    series.reserve_exact(partial.groups.len());

    for group in partial.groups {
        let spans = state_spans(&group, grid.spec().len as usize, &mut budget)?;
        let time = TimeAxis::from_grid_spans(Arc::clone(grid), spans).map_err(|error| {
            DataFusionError::Execution(format!("invalid aggregate state grid spans: {error}"))
        })?;

        budget(
            width
                .saturating_mul(group.points.len())
                .saturating_mul(size_of::<Option<AggregateStateValues>>()),
        )?;
        let mut states = Vec::with_capacity(width);
        states.resize_with(width, || Vec::with_capacity(group.points.len()));
        for (_, values) in group.points {
            if values.len() != width {
                return Err(DataFusionError::Execution(format!(
                    "aggregate state has {} fields, the grid describes {width}",
                    values.len()
                )));
            }
            for (field, value) in values.into_iter().enumerate() {
                states[field].push(value.map(|state| AggregateStateValues {
                    value: state.value,
                    mean: state.mean,
                    compensation: state.compensation,
                    count: state.count,
                    incremental_mean: state.incremental_mean,
                }));
            }
        }
        series.push(StateSeries {
            metric: group.metric,
            time,
            states,
        });
    }

    let chunk = StateChunk::Series {
        series,
        state_schema,
    };
    budget(state_bytes(&chunk))?;
    Ok(chunk)
}

/// The grid runs a group's points occupy, coalescing adjacent positions into one span.
fn state_spans(
    group: &PartialGroup,
    points: usize,
    budget: &mut impl FnMut(usize) -> Result<()>,
) -> Result<Vec<GridSpan>> {
    let mut spans: Vec<GridSpan> = Vec::new();
    let mut previous: Option<usize> = None;
    for &(point, _) in &group.points {
        if previous.is_some_and(|last| point <= last) {
            return Err(DataFusionError::Execution(
                "aggregate state points are not ascending".into(),
            ));
        }
        previous = Some(point);
        if point >= points {
            return Err(DataFusionError::Execution(format!(
                "aggregate state point {point} exceeds a grid of {points}"
            )));
        }
        match spans.last_mut() {
            Some(span) if span.start() + span.len() == point => {
                *span = GridSpan::new(span.start(), span.len() + 1);
            }
            _ => {
                budget(size_of::<GridSpan>())?;
                spans.push(GridSpan::new(point, 1));
            }
        }
    }
    Ok(spans)
}

/// Builds the executor's Arrow-backed state row from one decoded frame row.
pub(crate) fn state_row_from_frame_row(
    row: StateFrameRow,
    binding: &AggregateStateSchema,
) -> Result<crate::aggregate::state::StateRow> {
    let fields = row
        .fields()
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    if fields.kind() != binding.kind {
        return Err(DataFusionError::Execution(format!(
            "aggregate state row kind {:?} does not match Final binding {:?}",
            fields.kind(),
            binding.kind
        )));
    }
    if fields.components() != binding.components.as_ref() {
        return Err(DataFusionError::Execution(format!(
            "aggregate state row components {:?} do not match Final binding {:?}",
            fields.components(),
            binding.components
        )));
    }
    Ok(crate::aggregate::state::StateRow {
        metric: row.metric().clone(),
        time: row.time().clone(),
        fields,
        kind: binding.kind,
    })
}

/// The kernel aggregate a state kind merges into.
pub(crate) fn op_of_state(kind: AggregateStateKind) -> AggregateOp {
    match kind {
        AggregateStateKind::Sum => AggregateOp::Sum,
        AggregateStateKind::Count => AggregateOp::Count,
        AggregateStateKind::Avg => AggregateOp::Avg,
        AggregateStateKind::Min => AggregateOp::Min,
        AggregateStateKind::Max => AggregateOp::Max,
        AggregateStateKind::Group => AggregateOp::Group,
        AggregateStateKind::Stddev => AggregateOp::Stddev,
        AggregateStateKind::Stdvar => AggregateOp::Stdvar,
    }
}

/// Bytes held by one decoded state row's owned wrappers.
pub(crate) fn state_row_bytes(row: &crate::aggregate::state::StateRow) -> usize {
    let fields = row.fields.len();
    // Child value buffers are shared with the frame batch; charging them here would double-count.
    size_of::<StateChunk>()
        .saturating_add(size_of::<crate::aggregate::state::StateRow>())
        .saturating_add(fields.saturating_mul(size_of::<datafusion::arrow::array::StructArray>()))
        .saturating_add(
            (0..fields)
                .filter_map(|index| row.fields.field(index))
                .map(|field| {
                    field
                        .num_columns()
                        .saturating_mul(size_of::<datafusion::arrow::array::ArrayRef>())
                })
                .fold(0usize, usize::saturating_add),
        )
}

/// Bytes a state chunk holds: labels, axis runs, and one sparse state vector per field.
///
/// Like [`crate::grid_series::series_bytes`] this is a work estimate rather than a heap
/// measurement -- it counts vector capacities, not allocator overhead -- which is what the
/// reservation it feeds needs: a figure that never under-counts what the executor holds.
pub(crate) fn state_bytes(chunk: &StateChunk) -> usize {
    let StateChunk::Series {
        series,
        state_schema,
    } = chunk
    else {
        let StateChunk::Row(row) = chunk else {
            unreachable!("a state chunk is either producer series or a decoded row")
        };
        return state_row_bytes(row);
    };
    let series_bytes = series.iter().fold(0usize, |total, series| {
        let runs = series.time.runs().count();
        let labels = series
            .metric
            .name()
            .map_or(0, str::len)
            .saturating_add(size_of_val(series.metric.tags()))
            .saturating_add(
                series
                    .metric
                    .tags()
                    .iter()
                    .map(|tag| tag.name.len().saturating_add(tag.value.len()))
                    .sum::<usize>(),
            );
        let fields = series
            .states
            .capacity()
            .saturating_mul(size_of::<Vec<Option<AggregateStateValues>>>());
        let states = series.states.iter().fold(fields, |bytes, field| {
            bytes.saturating_add(
                field
                    .capacity()
                    .saturating_mul(size_of::<Option<AggregateStateValues>>()),
            )
        });
        total
            .saturating_add(size_of::<StateSeries>())
            .saturating_add(runs.saturating_mul(size_of::<(usize, usize)>()))
            .saturating_add(labels)
            .saturating_add(states)
    });
    size_of::<StateChunk>()
        .saturating_add(series.capacity().saturating_mul(size_of::<StateSeries>()))
        .saturating_add(series_bytes)
        .saturating_add(
            state_schema
                .components
                .len()
                .saturating_mul(size_of::<StateComponent>()),
        )
}
