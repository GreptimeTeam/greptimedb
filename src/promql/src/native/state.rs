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
//! A state chunk carries a group's labels, the grid positions it has contributions at, and the
//! accumulator components its aggregate declares. Converting between kernel state
//! ([`PartialAggregate`]) and frame state ([`StateSeries`]) keeps a field with no contribution
//! absent rather than zero, so a later merge does not count it.

use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datatypes::series::aggregate_state::{
    AggregateStateKind, StateComponent, StatePayload, StateSeries,
};
use datatypes::series::frame::StateFrameRow;
use datatypes::series::time::{GridSpan, TimeAxis};

use crate::aggregate::{AggregateOp, PartialAggregate};
use crate::native::plan::{AggregateStateSchema, BoundAggregate, aggregate_state_binding};

/// One partition's aggregate state, either in producer series form or as a decoded consumer row.
#[derive(Debug)]
pub enum StateChunk {
    Series {
        series: Vec<StateSeries>,
        state_schema: Arc<AggregateStateSchema>,
        /// The producer's charge, moved here when the chunk leaves its node's queue: the consumer
        /// keeps the series across polls, so the charge travels with them.
        reservation: Option<MemoryReservation>,
    },
    Row(crate::aggregate::state::StateRow),
}

/// Rejects a state whose kind or component list differs from what `op`'s binding produces, so a
/// `sum` state is never merged as `avg` and a merge reads exactly the columns the producer wrote.
fn ensure_bound_state(op: &BoundAggregate, state_schema: &AggregateStateSchema) -> Result<()> {
    let Some((expected_kind, expected_components)) =
        aggregate_state_binding(op.kind, op.field_kind)
    else {
        return Err(DataFusionError::NotImplemented(format!(
            "aggregate state is not defined for {:?}",
            op.kind
        )));
    };
    if state_schema.kind != expected_kind {
        return Err(DataFusionError::Execution(format!(
            "aggregate state of kind {:?} is not {expected_kind:?} state",
            state_schema.kind
        )));
    }
    if state_schema.components.as_ref() != expected_components.as_slice() {
        return Err(DataFusionError::Execution(format!(
            "aggregate state components {:?} are not what {:?} produces: {:?}",
            state_schema.components, op.kind, expected_components
        )));
    }
    Ok(())
}

/// Hands one partition's kernel state over as frame state. The fold already builds the frame
/// representation, so this moves the groups without copying.
pub(crate) fn state_chunk_from_partial(
    partial: PartialAggregate,
    state_schema: Arc<AggregateStateSchema>,
    op: &BoundAggregate,
) -> Result<StateChunk> {
    ensure_bound_state(op, state_schema.as_ref())?;
    Ok(StateChunk::Series {
        series: partial.groups,
        state_schema,
        reservation: None,
    })
}

/// Builds a state row keeping the kind and components its producer declared; the consumer checks
/// them against its own binding.
pub(crate) fn state_row_of(row: StateFrameRow) -> Result<crate::aggregate::state::StateRow> {
    let fields = row
        .fields()
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let kind = fields.kind();
    Ok(crate::aggregate::state::StateRow {
        metric: row.metric().clone(),
        time: row.time().clone(),
        fields,
        kind,
    })
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

/// The aggregate a state kind is the state of. A histogram kind maps to the same aggregate; the
/// merge reads the payload type from the kind.
pub(crate) fn op_of_state(kind: AggregateStateKind) -> AggregateOp {
    match kind {
        AggregateStateKind::Sum | AggregateStateKind::HistogramSum => AggregateOp::Sum,
        AggregateStateKind::Count => AggregateOp::Count,
        AggregateStateKind::Avg | AggregateStateKind::HistogramAvg => AggregateOp::Avg,
        AggregateStateKind::Min => AggregateOp::Min,
        AggregateStateKind::Max => AggregateOp::Max,
        AggregateStateKind::Group => AggregateOp::Group,
        AggregateStateKind::Stddev => AggregateOp::Stddev,
        AggregateStateKind::Stdvar => AggregateOp::Stdvar,
    }
}

/// Transient bytes for the selected state array consumed by a decoded row.
///
/// This estimates the selected extent, not a reservation for the batch's shared parent buffers;
/// a router retains and charges those buffers separately while handing the batch downstream.
pub(crate) fn slice_bytes(array: &dyn Array) -> usize {
    array
        .to_data()
        .get_slice_memory_size()
        .unwrap_or_else(|_| array.get_array_memory_size())
}

/// Bytes held by one decoded state row's owned wrappers and retained payloads.
pub(crate) fn state_row_bytes(row: &crate::aggregate::state::StateRow) -> usize {
    let fields = row.fields.len();
    size_of::<StateChunk>()
        .saturating_add(size_of::<crate::aggregate::state::StateRow>())
        .saturating_add(fields.saturating_mul(size_of::<datafusion::arrow::array::StructArray>()))
        .saturating_add(
            (0..fields)
                .filter_map(|index| row.fields.field(index))
                .map(|field| slice_bytes(field))
                .fold(0usize, usize::saturating_add),
        )
        .saturating_add(
            row.metric
                .name()
                .map_or(0, str::len)
                .saturating_add(size_of_val(row.metric.tags()))
                .saturating_add(
                    row.metric
                        .tags()
                        .iter()
                        .map(|tag| tag.name.len().saturating_add(tag.value.len()))
                        .sum::<usize>(),
                ),
        )
        .saturating_add(
            row.time
                .runs()
                .count()
                .saturating_mul(size_of::<GridSpan>().saturating_add(size_of::<usize>()))
                .saturating_add(if row.time.is_empty() {
                    0
                } else {
                    size_of::<TimeAxis>()
                }),
        )
}

/// Bytes a state chunk holds: labels, axis runs, and one sparse state vector per field.
///
/// Like [`crate::grid_series::series_bytes`], an estimate from vector capacities that must not
/// under-count what the executor holds.
pub(crate) fn state_bytes(chunk: &StateChunk) -> usize {
    let StateChunk::Series {
        series,
        state_schema,
        ..
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
            .saturating_mul(size_of::<Vec<Option<StatePayload>>>());
        let states = series.states.iter().fold(fields, |bytes, field| {
            // A histogram payload owns boxed state and Arrow buffers, charged by bucket width.
            bytes
                .saturating_add(
                    field
                        .capacity()
                        .saturating_mul(size_of::<Option<StatePayload>>()),
                )
                .saturating_add(field.iter().flatten().fold(0usize, |heap, state| {
                    heap.saturating_add(state.heap_bytes())
                }))
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
