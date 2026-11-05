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

use datafusion::arrow::array::Array;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datatypes::series::aggregate_state::{
    AggregateStateKind, StateComponent, StatePayload, StateSeries,
};
use datatypes::series::frame::StateFrameRow;
use datatypes::series::time::{GridSpan, TimeAxis};

use crate::aggregate::{AggregateOp, PartialAggregate};
use crate::native::plan::AggregateStateSchema;

/// One partition's aggregate state, either in producer series form or as a decoded consumer row.
#[derive(Debug)]
pub(crate) enum StateChunk {
    Series {
        series: Vec<StateSeries>,
        state_schema: Arc<AggregateStateSchema>,
        /// The producer charge this chunk carries once it has left its node's queue.
        ///
        /// A state chunk is charged to the node that folded it until the chunk is handed over. The
        /// consumer keeps the batch across polls while it packs rows from it, so the charge travels
        /// with those series rather than being returned the moment the producer lets go.
        reservation: Option<MemoryReservation>,
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

/// Hands one partition's kernel state over as the frame state a consumer reads.
///
/// The fold already builds the frame's own representation - field-major states on sparse absolute
/// grid positions - so this moves the groups and checks the binding. It copies nothing: the charge
/// the fold reported for those groups is the charge the chunk keeps.
pub(crate) fn state_chunk_from_partial(
    partial: PartialAggregate,
    state_schema: Arc<AggregateStateSchema>,
) -> Result<StateChunk> {
    ensure_bound_state(partial.op, state_schema.as_ref())?;
    Ok(StateChunk::Series {
        series: partial.groups,
        state_schema,
        reservation: None,
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

/// The numeric kernel aggregate a state kind merges into.
///
/// A histogram state has no numeric accumulator, and no aggregate binds one through this route yet, so
/// the conversion is fallible instead of mapping a histogram kind onto its numeric namesake.
pub(crate) fn op_of_state(kind: AggregateStateKind) -> Result<AggregateOp> {
    Ok(match kind {
        AggregateStateKind::Sum => AggregateOp::Sum,
        AggregateStateKind::Count => AggregateOp::Count,
        AggregateStateKind::Avg => AggregateOp::Avg,
        AggregateStateKind::Min => AggregateOp::Min,
        AggregateStateKind::Max => AggregateOp::Max,
        AggregateStateKind::Group => AggregateOp::Group,
        AggregateStateKind::Stddev => AggregateOp::Stddev,
        AggregateStateKind::Stdvar => AggregateOp::Stdvar,
        AggregateStateKind::HistogramSum | AggregateStateKind::HistogramAvg => {
            return Err(DataFusionError::Execution(format!(
                "{kind:?} state has no numeric aggregate to merge into"
            )));
        }
    })
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
/// Like [`crate::grid_series::series_bytes`] this is a work estimate rather than a heap
/// measurement -- it counts vector capacities, not allocator overhead -- which is what the
/// reservation it feeds needs: a figure that never under-counts what the executor holds.
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
            // The slot is sized for the payload family it holds; a histogram payload also owns its
            // boxed state and both histograms' Arrow buffers, so its buckets are charged by width
            // rather than estimated at a fixed size.
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
