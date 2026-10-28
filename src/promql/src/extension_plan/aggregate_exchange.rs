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

//! The private Arrow layout of a partition's partial aggregate state, and the fold that writes it.
//! [`decode_state`] reads a batch back into the rows a `FloatMerger` merges. Only that producer and
//! that merge read [`state_schema`]: it is not a wire format, and has no serde or version.
//!
//! TODO(discord9): Replace this temporary state-row bridge with series frames emitted directly by
//! FusedSeries and consumed by native final aggregation, avoiding the extra state-row encoding,
//! decoding and reconstruction. This private layout is not the final intermediate representation.

#[cfg(test)]
mod tests;

use std::mem::size_of;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datatypes::series::samples::SeriesSchema;
use datatypes::series::{Label, SeriesLabels};

use crate::aggregate::{
    AccumulatorState, AggregateOp, FloatAggregator, Grouping, PartialAggregate, PartialGroup,
};
use crate::evaluated::EvaluatedChunk;
use crate::grid_series::series_bytes;
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, Grouping as BoundGrouping,
};

/// Rows per encoded batch; the exchange has no session context to read a batch size from.
const BATCH_ROWS: usize = 4096;
/// One column's `ArrayRef`, its `Field` and its buffers are all well under this.
const COLUMN_OVERHEAD: usize = 256;
/// The value capacity a string column starts with, nulls or not (Arrow's `StringArray::from`).
const STRING_CAPACITY: usize = 1024;

/// One state row: a group's labels, the point and field it is of, and its state. Label columns are
/// nullable, since absent and empty differ; `__name__` has the metric name's column.
pub(crate) fn state_schema(tags: &[String]) -> SchemaRef {
    let mut fields = vec![Field::new("metric_name", DataType::Utf8, true)];
    let labels = ordinary_tags(tags).count();
    fields.extend((0..labels).map(|at| Field::new(format!("tag_{at}"), DataType::Utf8, true)));
    fields.extend([
        Field::new("point", DataType::UInt64, false),
        Field::new("field", DataType::UInt32, false),
    ]);
    for name in ["value", "mean", "compensation", "count"] {
        fields.push(Field::new(name, DataType::Float64, false));
    }
    fields.push(Field::new("incremental_mean", DataType::Boolean, false));
    Arc::new(Schema::new(fields))
}

/// The kernel a `Complete` aggregate binds; `quantile` and the other stages are not a fold.
pub(crate) fn aggregate_op(op: &BoundAggregate) -> DfResult<AggregateOp> {
    if op.stage != AggregateStage::Complete {
        return Err(DataFusionError::NotImplemented(format!(
            "the aggregate state exchange folds only `Complete` aggregates, not `{:?}`",
            op.stage
        )));
    }
    match op.kind {
        AggregateKind::Sum => Ok(AggregateOp::Sum),
        AggregateKind::Count => Ok(AggregateOp::Count),
        AggregateKind::Avg => Ok(AggregateOp::Avg),
        AggregateKind::Min => Ok(AggregateOp::Min),
        AggregateKind::Max => Ok(AggregateOp::Max),
        AggregateKind::Group => Ok(AggregateOp::Group),
        AggregateKind::Stddev => Ok(AggregateOp::Stddev),
        AggregateKind::Stdvar => Ok(AggregateOp::Stdvar),
        AggregateKind::Quantile { quantile } => Err(DataFusionError::NotImplemented(format!(
            "the aggregate state exchange does not carry `quantile {quantile}`"
        ))),
    }
}

/// One partition's fold, and the state it encodes when its input ends.
pub(crate) struct PartialFold {
    tags: Arc<[String]>,
    pool: Arc<dyn MemoryPool>,
    /// What this fold holds beside the kernel's state, so each charge can be added to it.
    held: usize,
    reservation: MemoryReservation,
    /// The plan's charge for exported batches, which live until its queue drains.
    export_budget: Arc<Mutex<Option<MemoryReservation>>>,
    fold: Option<FloatAggregator>,
}

impl PartialFold {
    /// Starts one partition's fold, its `export_budget` registered against `pool` at first export.
    pub(crate) fn try_new(
        op: BoundAggregate,
        schema: Arc<SeriesSchema>,
        tags: Arc<[String]>,
        export_budget: Arc<Mutex<Option<MemoryReservation>>>,
        pool: &Arc<dyn MemoryPool>,
    ) -> DfResult<Self> {
        let kind = aggregate_op(&op)?;
        let per_tag = size_of::<String>() + size_of::<Field>() + size_of::<ArrayRef>();
        let held = tags.iter().map(|tag| tag.len() + per_tag).sum::<usize>() + 8 * per_tag;
        let reservation = MemoryConsumer::new("AggregateExchangeState").register(pool);
        reservation.try_resize(held)?;
        // `__name__` has the metric name's column; a list without it is reused, not copied.
        let tags: Arc<[String]> = if tags.iter().any(|tag| tag == "__name__") {
            ordinary_tags(&tags)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
                .into()
        } else {
            tags
        };
        // The grouping names are a scratch copy of the binding's list, which can be longer than the
        // tag list, so they are admitted while the kernel is built and given back after.
        let scratch = match &op.group {
            BoundGrouping::By(names) | BoundGrouping::Without(names) => {
                names.len() * size_of::<&str>()
            }
        };
        reservation.try_resize(held.saturating_add(scratch))?;
        let fold = {
            let names: Vec<&str> = match &op.group {
                BoundGrouping::By(names) | BoundGrouping::Without(names) => {
                    names.iter().map(AsRef::as_ref).collect()
                }
            };
            let grouping = match &op.group {
                BoundGrouping::By(_) => Grouping::By(&names),
                BoundGrouping::Without(_) => Grouping::Without(&names),
            };
            FloatAggregator::try_new_budgeted(&schema, kind, grouping, |total| {
                reservation.try_resize(held.saturating_add(scratch).saturating_add(total))
            })?
        };
        reservation.try_resize(held)?;
        Ok(Self {
            tags,
            pool: Arc::clone(pool),
            held,
            reservation,
            export_budget,
            fold: Some(fold),
        })
    }

    /// Folds one chunk, held while it is folded and dropped after. The kernel validates the chunk
    /// against the schema this fold was built with before it charges anything.
    pub(crate) fn update(&mut self, chunk: EvaluatedChunk) -> DfResult<()> {
        let fold = self.fold.as_mut().ok_or_else(finished)?;
        let pending = self.held + series_bytes(&chunk.series);
        fold.update_budgeted(&chunk.series, |fold_bytes| {
            self.reservation.try_resize(pending + fold_bytes)
        })?;
        drop(chunk);
        self.reservation.try_resize(self.held + fold.state_bytes())
    }

    /// Encodes the fold's state once the caller has read the input to its end. The batches stay
    /// charged to [`Self::export_budget`] after the states are given back, since they outlive them.
    pub(crate) fn finish(&mut self) -> DfResult<Vec<RecordBatch>> {
        let fold = self.fold.take().ok_or_else(finished)?;
        let partial =
            fold.finish_partial_budgeted(|total| self.reservation.try_resize(self.held + total))?;
        let batches = self.encode(&partial)?;
        drop(partial);
        self.reservation.try_resize(self.held)?;
        Ok(batches)
    }

    /// Encodes `partial` as one row buffer, admitted in full before it allocates, and hands it over
    /// as slices of at most [`BATCH_ROWS`] rows that share it.
    fn encode(&self, partial: &PartialAggregate) -> DfResult<Vec<RecordBatch>> {
        let mut rows = 0usize;
        let mut label_bytes = 0usize;
        for group in &partial.groups {
            let labels = group.metric.name().map_or(0, str::len).saturating_add(
                ordinary_tags(&self.tags)
                    .filter_map(|tag| group.metric.get(tag).map(|value| tag.len() + value.len()))
                    .sum::<usize>(),
            );
            let present: usize = group
                .points
                .iter()
                .map(|p| p.1.iter().flatten().count())
                .sum();
            rows += present;
            label_bytes += labels * present;
        }
        if rows > 0 {
            self.grow_export(export_bytes(rows, label_bytes, self.tags.len()))?;
        }
        let mut metric_name: Vec<Option<&str>> = Vec::with_capacity(rows);
        let mut tags: Vec<Vec<_>> = self.tags.iter().map(|_| Vec::with_capacity(rows)).collect();
        let (mut point, mut field) = (Vec::with_capacity(rows), Vec::with_capacity(rows));
        let mut components: [Vec<f64>; 4] = std::array::from_fn(|_| Vec::with_capacity(rows));
        let mut incremental_mean: Vec<bool> = Vec::with_capacity(rows);
        for group in &partial.groups {
            for (at, states) in &group.points {
                for (index, state) in states.iter().enumerate() {
                    let Some(state) = state else {
                        continue;
                    };
                    metric_name.push(group.metric.name());
                    for (column, tag) in tags.iter_mut().zip(self.tags.iter()) {
                        column.push(group.metric.get(tag));
                    }
                    point.push(*at as u64);
                    field.push(index as u32);
                    components[0].push(state.value);
                    components[1].push(state.mean);
                    components[2].push(state.compensation);
                    components[3].push(state.count);
                    incremental_mean.push(state.incremental_mean);
                }
            }
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(8 + tags.len());
        columns.push(Arc::new(StringArray::from(metric_name)));
        for values in tags {
            columns.push(Arc::new(StringArray::from(values)));
        }
        columns.push(Arc::new(UInt64Array::from(point)));
        columns.push(Arc::new(UInt32Array::from(field)));
        for values in components {
            columns.push(Arc::new(Float64Array::from(values)));
        }
        columns.push(Arc::new(BooleanArray::from(incremental_mean)));
        let batch = RecordBatch::try_new(state_schema(&self.tags), columns)?;
        Ok((0..rows)
            .step_by(BATCH_ROWS)
            .map(|offset| batch.slice(offset, BATCH_ROWS.min(rows - offset)))
            .collect())
    }

    /// Admits `bytes` to the plan's export reservation, registered against this pool at first use.
    fn grow_export(&self, bytes: usize) -> DfResult<()> {
        let mut budget = self.export_budget.lock().unwrap_or_else(|e| e.into_inner());
        let consumer = MemoryConsumer::new("AggregateExchangeExport");
        budget
            .get_or_insert_with(|| consumer.register(&self.pool))
            .try_grow(bytes)
    }
}

/// Reads one state batch back into the rows a `FloatMerger` merges.
///
/// The encoder writes a group's points in ascending order, one row per (point, field), so a label
/// change opens the next group and everything else extends the current one: consecutive rows of a
/// point fill that point's state vector, and a later point is appended to the same group. Rows of
/// one group split across batches, or concatenated back to back, start a further point entry
/// instead of failing; the merge addresses points by index, so both entries fold into one slot.
pub(crate) fn decode_state(
    batch: &RecordBatch,
    schema: &Arc<SeriesSchema>,
    op: AggregateOp,
    tags: &[String],
    mut budget: impl FnMut(usize) -> DfResult<()>,
) -> DfResult<PartialAggregate> {
    // The expected layout builds a name and a field per column, so it is admitted before it is
    // built, together with the column lookups below.
    let mut total = (tags.len() + 9)
        .saturating_mul(size_of::<&str>() + size_of::<String>() + size_of::<Field>());
    budget(total)?;
    let layout = state_schema(tags);
    let batch_schema = batch.schema_ref();
    if batch_schema.as_ref() != layout.as_ref() {
        return Err(DataFusionError::Execution(format!(
            "not the layout {layout:?}"
        )));
    }
    let grid = schema.eval_grid.as_ref().ok_or_else(|| {
        DataFusionError::Execution("aggregate state is indexed on an evaluated grid".into())
    })?;
    let width = schema.fields.len();
    // The name list and the label column lookups are workspace, sized exactly and charged before
    // either allocates.
    total = total.saturating_add(tags.len() * (size_of::<&str>() + size_of::<&StringArray>()));
    budget(total)?;
    let mut names: Vec<&str> = Vec::with_capacity(tags.len());
    names.extend(ordinary_tags(tags));
    let metric_name: &StringArray = column(batch, "metric_name")?;
    let mut tag_columns: Vec<&StringArray> = Vec::with_capacity(names.len());
    for at in 0..names.len() {
        tag_columns.push(column(batch, &format!("tag_{at}"))?);
    }
    let point: &UInt64Array = column(batch, "point")?;
    let field: &UInt32Array = column(batch, "field")?;
    let value: &Float64Array = column(batch, "value")?;
    let mean: &Float64Array = column(batch, "mean")?;
    let compensation: &Float64Array = column(batch, "compensation")?;
    let count: &Float64Array = column(batch, "count")?;
    let incremental_mean: &BooleanArray = column(batch, "incremental_mean")?;
    // A null in a state column is not a zero: that state was never folded. Only the label columns
    // ahead of these are nullable.
    for at in names.len() + 1..batch.num_columns() {
        let nulls = batch.column(at).null_count();
        let name = batch_schema.field(at).name();
        if nulls > 0 {
            return Err(DataFusionError::Execution(format!(
                "the aggregate state column `{name}` holds {nulls} nulls, a state being a value"
            )));
        }
    }
    let mut groups: Vec<PartialGroup> = Vec::new();
    for row in 0..batch.num_rows() {
        let at = usize::try_from(point.value(row)).unwrap_or(usize::MAX);
        let field = field.value(row) as usize;
        if at >= grid.spec().len as usize || field >= width {
            return Err(DataFusionError::Execution(format!(
                "aggregate state row {row} is of point {at} of the grid's {} or of field {field} \
                 of the aggregate's {width}",
                grid.spec().len
            )));
        }
        // The encoder writes a group's points in ascending order and its fields together, so only a
        // label change opens a group: a row repeating the labels of the row before it is the same
        // group, and is that point's next field or the group's next point. Comparing a label change
        // against a change of point would open one group per row, since consecutive points of one
        // group differ by definition.
        let differs = |column: &StringArray| {
            column.is_null(row - 1) != column.is_null(row)
                || (!column.is_null(row) && column.value(row - 1) != column.value(row))
        };
        let opens =
            row == 0 || differs(metric_name) || tag_columns.iter().any(|column| differs(column));
        if opens {
            // A push outgrowing the group vector doubles it, so the block it is about to allocate
            // is admitted here; the point's tuple and state vector and the label scratch follow
            // before either allocates, then what the labels carry once they are read.
            let growth = if groups.len() == groups.capacity() {
                groups.capacity().max(1) * 2 * size_of::<PartialGroup>()
            } else {
                0
            };
            total = total.saturating_add(
                growth
                    + size_of::<(usize, Vec<Option<AccumulatorState>>)>()
                    + width * size_of::<Option<AccumulatorState>>()
                    + (names.len() + 1) * size_of::<(&str, &str)>(),
            );
            budget(total)?;
            let name = (!metric_name.is_null(row)).then(|| metric_name.value(row));
            let mut carried: Vec<(&str, &str)> = Vec::with_capacity(names.len());
            for (column, label) in tag_columns.iter().zip(&names) {
                if !column.is_null(row) {
                    carried.push((*label, column.value(row)));
                }
            }
            // The label vector, the `Arc<str>` header each name and value points at, and the bytes
            // they hold. One extra pair is the metric name.
            total = total.saturating_add(
                (names.len() + 1) * (size_of::<Label>() + size_of::<usize>())
                    + name.map_or(0, str::len)
                    + carried
                        .iter()
                        .map(|(label, value)| label.len() + value.len())
                        .sum::<usize>(),
            );
            budget(total)?;
            let mut labels: Vec<Label> = Vec::with_capacity(carried.len());
            for (label, value) in carried {
                labels.push(Label {
                    name: Arc::from(label),
                    value: Arc::from(value),
                });
            }
            let metric = SeriesLabels::try_new(name.map(Arc::from), labels).map_err(|error| {
                DataFusionError::Execution(format!(
                    "aggregate state row {row} does not carry a label set: {error}"
                ))
            })?;
            groups.push(PartialGroup {
                metric,
                points: vec![(at, vec![None; width])],
            });
        } else {
            // The same group as the row before: this row's point is the one its state belongs to.
            // The encoder emits a group's points in ascending order, so a later point is appended
            // to the group rather than starting a new one.
            let group = groups.last_mut().ok_or_else(|| {
                DataFusionError::Execution(format!("aggregate state row {row} continued no group"))
            })?;
            // A batch that holds two encoders' output back to back restarts a group at an earlier
            // point, and repeats a (point, field) pair. Both are accepted: the merge addresses
            // points by index, so a second entry for the same point folds into the same slot,
            // exactly as merging the two batches separately would.
            let opens_point = match group.points.last() {
                Some((last, states)) => *last != at || states[field].is_some(),
                None => true,
            };
            if opens_point {
                // The point vector doubles when it outgrows itself, then the point's tuple and its
                // state vector follow, both admitted before either allocates.
                let growth = if group.points.len() == group.points.capacity() {
                    group.points.capacity().max(1)
                        * 2
                        * size_of::<(usize, Vec<Option<AccumulatorState>>)>()
                } else {
                    0
                };
                total = total.saturating_add(
                    growth
                        + size_of::<(usize, Vec<Option<AccumulatorState>>)>()
                        + width * size_of::<Option<AccumulatorState>>(),
                );
                budget(total)?;
                group.points.push((at, vec![None; width]));
            }
        }
        let group = groups.last_mut().ok_or_else(|| {
            DataFusionError::Execution(format!("aggregate state row {row} opened no group"))
        })?;
        let (_, states) = group.points.last_mut().ok_or_else(|| {
            DataFusionError::Execution(format!("aggregate state row {row} continued no point"))
        })?;
        states[field] = Some(AccumulatorState {
            value: value.value(row),
            mean: mean.value(row),
            compensation: compensation.value(row),
            count: count.value(row),
            incremental_mean: incremental_mean.value(row),
        });
    }
    Ok(PartialAggregate { op, groups })
}

/// The caller's tag names in column order, without the metric name, which has its own column.
fn ordinary_tags(tags: &[String]) -> impl Iterator<Item = &str> + '_ {
    tags.iter()
        .filter(|tag| tag.as_str() != "__name__")
        .map(String::as_str)
}

/// The error for a fold that has already been finished.
fn finished() -> DataFusionError {
    DataFusionError::Execution("the partial aggregate already finished its fold".into())
}

/// An upper bound on one export: the scratch row vectors, the Arrow arrays they are frozen into --
/// which live beside them while the strings are copied -- and the slices the output holds.
///
/// A string column starts at [`STRING_CAPACITY`] value bytes even when every row is null, and the
/// builder copies what it appended before handing its buffer over, so one minimum capacity is
/// admitted per column beside the label payload twice over. Coarse on purpose: an overestimate.
fn export_bytes(rows: usize, label_bytes: usize, tags: usize) -> usize {
    let strings = tags.saturating_add(1);
    let row = size_of::<u64>() + size_of::<u32>() + 4 * size_of::<f64>() + size_of::<bool>();
    rows * row
        + strings * rows * size_of::<Option<&str>>()
        + strings * (rows + 1) * size_of::<i32>()
        + strings * (rows / 8 + 1)
        + strings * STRING_CAPACITY
        + 2 * label_bytes
        + (strings + 8) * COLUMN_OVERHEAD
        + rows.div_ceil(BATCH_ROWS) * size_of::<RecordBatch>()
}

/// Reads the column `name` as `A`, which the layout check has already made certain.
fn column<'a, A: 'static>(batch: &'a RecordBatch, name: &str) -> DfResult<&'a A> {
    let found = batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<A>());
    found.ok_or_else(|| DataFusionError::Execution(format!("the column `{name}` has another type")))
}
