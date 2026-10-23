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

//! The private in-process frame between the parallel prefix and the global consumer.
//!
//! One [`EvaluatedChunk`] is one Arrow row of a fixed layout: the metric, its labels, the absolute
//! grid positions the chunk carries and their values. The frame names no grid, so the consumer
//! binds the [`SeriesSchema`] the chunks were evaluated against and the plan keeps that binding.
//! An empty row is a real row: it is how a series closes without holding a value. DataFusion may
//! rebatch, so [`read`] takes the row it wants and keeps nothing. [`Sequence`] checks the
//! producer/ordinal ordering a single row cannot state.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, ListArray, StringArray, UInt64Array,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::{
    DenseSamplesRef, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{GridSpan, TimeAxis};
use datatypes::series::{Label, MetricName};

use crate::evaluated::EvaluatedChunk;

/// The output stream the chunk belongs to.
const PRODUCER: &str = "producer";
/// The chunk's position in its producer's output.
const FRAME_ORDINAL: &str = "frame_ordinal";
/// The metric name; null when the series has none, empty when it is `""`.
const METRIC_NAME: &str = "metric_name";
/// The ordinary label names, parallel to [`LABEL_VALUES`].
const LABEL_NAMES: &str = "label_names";
/// The ordinary label values, parallel to [`LABEL_NAMES`].
const LABEL_VALUES: &str = "label_values";
/// The absolute grid positions of the chunk's rows.
const POSITIONS: &str = "positions";
/// The chunk's float values, parallel to [`POSITIONS`].
const VALUES: &str = "values";
/// Whether this chunk closes its series.
const IS_LAST: &str = "is_last";
/// Arrow's name for the child field of a list.
const LIST_ITEM: &str = "item";

/// The fixed frame layout: only the metric name and the values may be null, and a label is absent
/// by being absent from the parallel lists rather than by a null entry.
pub(crate) fn schema() -> SchemaRef {
    let list = |name: &str, item: DataType, nullable: bool| {
        Field::new(
            name,
            DataType::List(Arc::new(Field::new(LIST_ITEM, item, nullable))),
            false,
        )
    };
    Arc::new(Schema::new(vec![
        Field::new(PRODUCER, DataType::UInt64, false),
        Field::new(FRAME_ORDINAL, DataType::UInt64, false),
        Field::new(METRIC_NAME, DataType::Utf8, true),
        list(LABEL_NAMES, DataType::Utf8, false),
        list(LABEL_VALUES, DataType::Utf8, false),
        list(POSITIONS, DataType::UInt64, false),
        list(VALUES, DataType::Float64, true),
        Field::new(IS_LAST, DataType::Boolean, false),
    ]))
}

/// Packs one evaluated chunk into a one-row batch of [`schema`].
///
/// `series_schema` must name exactly one field and one grid, and the chunk is validated against it
/// through [`Series::try_new`] before the axis is reduced to positions. Samples must be float
/// segments, so a histogram or mixed segment is refused even when it holds no row; chunked float
/// segments are accepted. Nulls, NaN bits, sparse positions and an empty closing chunk are carried
/// through unchanged.
pub(crate) fn pack(
    chunk: &EvaluatedChunk,
    series_schema: &SeriesSchema,
    producer: u64,
    frame_ordinal: u64,
) -> Result<RecordBatch> {
    if series_schema.fields.len() != 1 || series_schema.eval_grid.is_none() {
        return Err(DataFusionError::Execution(
            "an evaluated frame requires one field and a grid".to_string(),
        ));
    }
    let series = Series::try_new(
        series_schema,
        chunk.series.metric.clone(),
        chunk.series.time.clone(),
        chunk.series.fields.clone(),
    )
    .map_err(series_error)?;
    let samples =
        series.fields.as_slice().first().ok_or_else(|| {
            DataFusionError::Execution("evaluated chunk holds no field".to_string())
        })?;
    // Every segment is checked, so an empty histogram segment is refused just like a filled one.
    for segment in samples.segments() {
        if !matches!(segment.dense(), Some(DenseSamplesRef::Float(_))) {
            return Err(DataFusionError::NotImplemented(
                "an evaluated frame carries float samples only".to_string(),
            ));
        }
    }

    // `positions` walks the carried rows, so the holes between runs never become rows.
    let mut positions: Vec<u64> = Vec::with_capacity(samples.len());
    let mut values: Vec<Option<f64>> = Vec::with_capacity(samples.len());
    let mut cursor = samples.cursor();
    for (position, row) in series.time.positions() {
        positions.push(u64::try_from(position).map_err(|_| {
            DataFusionError::Execution("evaluated frame position does not fit UInt64".to_string())
        })?);
        match cursor.at(row) {
            Some(SampleRef::Float(value)) => values.push(Some(value)),
            Some(SampleRef::Missing) => values.push(None),
            Some(SampleRef::Histogram(_)) => {
                return Err(DataFusionError::NotImplemented(
                    "an evaluated frame carries float samples only".to_string(),
                ));
            }
            None => {
                return Err(DataFusionError::Execution(format!(
                    "evaluated chunk row {row} is missing from its field"
                )));
            }
        }
    }

    let names: Vec<&str> = series
        .metric
        .tags()
        .iter()
        .map(|tag| tag.name.as_ref())
        .collect();
    let labels: Vec<&str> = series
        .metric
        .tags()
        .iter()
        .map(|tag| tag.value.as_ref())
        .collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![producer])),
        Arc::new(UInt64Array::from(vec![frame_ordinal])),
        Arc::new(StringArray::from(vec![series.metric.name()])),
        Arc::new(list(
            DataType::Utf8,
            false,
            Arc::new(StringArray::from(names)),
        )?),
        Arc::new(list(
            DataType::Utf8,
            false,
            Arc::new(StringArray::from(labels)),
        )?),
        Arc::new(list(
            DataType::UInt64,
            false,
            Arc::new(UInt64Array::from(positions)),
        )?),
        Arc::new(list(
            DataType::Float64,
            true,
            Arc::new(Float64Array::from(values)),
        )?),
        Arc::new(BooleanArray::from(vec![chunk.is_last])),
    ];
    Ok(RecordBatch::try_new(schema(), columns)?)
}

/// Reads the chunk held by `row` of `batch`.
///
/// `series_schema` is the binding that supplies the grid absolute positions refer to: it must name
/// exactly one field and one grid. The batch must carry exactly [`schema`], every required column
/// must be non-null at `row`, the label lists must be parallel and name one label set (`__name__`
/// stays the metric name), positions must be strictly increasing and inside the grid, and one value
/// must exist per position. The axis is rebuilt compactly, so no row is materialized for a hole.
pub(crate) fn read(
    batch: &RecordBatch,
    row: usize,
    series_schema: &Arc<SeriesSchema>,
) -> Result<(u64, u64, EvaluatedChunk)> {
    if batch.schema().as_ref() != schema().as_ref() {
        return Err(DataFusionError::Execution(
            "evaluated frame schema mismatch".to_string(),
        ));
    }
    if row >= batch.num_rows() {
        return Err(DataFusionError::Execution(format!(
            "evaluated frame row {row} is outside a batch of {} rows",
            batch.num_rows()
        )));
    }
    if series_schema.fields.len() != 1 {
        return Err(DataFusionError::Execution(
            "an evaluated frame requires one field".to_string(),
        ));
    }
    let Some(grid) = series_schema.eval_grid.clone() else {
        return Err(DataFusionError::Execution(
            "an evaluated frame requires a grid".to_string(),
        ));
    };
    let columns = batch.columns();
    for (column, index) in [
        (PRODUCER, 0),
        (FRAME_ORDINAL, 1),
        (LABEL_NAMES, 3),
        (LABEL_VALUES, 4),
        (POSITIONS, 5),
        (VALUES, 6),
        (IS_LAST, 7),
    ] {
        if columns[index].is_null(row) {
            return Err(DataFusionError::Execution(format!(
                "evaluated frame requires a non-null {column} at row {row}"
            )));
        }
    }
    let producers = down::<UInt64Array>(&columns[0], PRODUCER)?;
    let ordinals = down::<UInt64Array>(&columns[1], FRAME_ORDINAL)?;
    let names = down::<StringArray>(&columns[2], METRIC_NAME)?;
    let label_names = down::<ListArray>(&columns[3], LABEL_NAMES)?;
    let label_values = down::<ListArray>(&columns[4], LABEL_VALUES)?;
    let position_list = down::<ListArray>(&columns[5], POSITIONS)?;
    let value_list = down::<ListArray>(&columns[6], VALUES)?;
    let lasts = down::<BooleanArray>(&columns[7], IS_LAST)?;

    let name = (!names.is_null(row)).then(|| Arc::<str>::from(names.value(row)));
    let names = strings(label_names, row, LABEL_NAMES)?;
    let labels = strings(label_values, row, LABEL_VALUES)?;
    if names.len() != labels.len() {
        return Err(DataFusionError::Execution(format!(
            "evaluated frame has {} label names and {} label values",
            names.len(),
            labels.len()
        )));
    }
    let tags: Vec<Label> = names
        .into_iter()
        .zip(labels)
        .map(|(name, value)| Label { name, value })
        .collect();
    // The constructor sorts the labels and refuses duplicate names and a `__name__` tag.
    let metric = MetricName::try_new(name, tags).map_err(series_error)?;

    let positions = positions(position_list, row, grid.spec().len)?;
    let value_array = value_list.value(row);
    let values = down::<Float64Array>(&value_array, VALUES)?;
    if positions.len() != values.len() {
        return Err(DataFusionError::Execution(format!(
            "evaluated frame has {} positions and {} values",
            positions.len(),
            values.len()
        )));
    }
    let axis = TimeAxis::from_grid_spans(grid, run_spans(&positions)).map_err(series_error)?;
    let fields = SeriesFields::try_from_vec(vec![Samples::from_float(values.clone())])
        .map_err(series_error)?;
    let series = Series::try_new(series_schema, metric, axis, fields).map_err(series_error)?;
    let is_last = lasts.value(row);
    let chunk = EvaluatedChunk::try_new(series, is_last)?;
    Ok((producers.value(row), ordinals.value(row), chunk))
}

/// Builds a one-row list over `values`, refusing a row Arrow's `i32` offsets cannot address.
fn list(item: DataType, nullable: bool, values: ArrayRef) -> Result<ListArray> {
    let len = i32::try_from(values.len()).map_err(|_| {
        DataFusionError::Execution("evaluated frame row is longer than Arrow addresses".to_string())
    })?;
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, len]));
    let item = Arc::new(Field::new(LIST_ITEM, item, nullable));
    Ok(ListArray::try_new(item, offsets, values, None)?)
}

/// Reads the non-null UTF-8 entries of one list row.
fn strings(array: &ListArray, row: usize, column: &str) -> Result<Vec<Arc<str>>> {
    let row_values = array.value(row);
    let values = down::<StringArray>(&row_values, column)?;
    if values.null_count() != 0 {
        return Err(DataFusionError::Execution(format!(
            "evaluated frame requires non-null {column} entries at row {row}"
        )));
    }
    Ok((0..values.len())
        .map(|index| Arc::<str>::from(values.value(index)))
        .collect())
}

/// Reads one list row of positions, checking strict order and grid bounds.
fn positions(array: &ListArray, row: usize, grid_len: u64) -> Result<Vec<usize>> {
    let row_values = array.value(row);
    let values = down::<UInt64Array>(&row_values, POSITIONS)?;
    if values.null_count() != 0 {
        return Err(DataFusionError::Execution(format!(
            "evaluated frame requires non-null positions at row {row}"
        )));
    }
    let mut positions: Vec<usize> = Vec::with_capacity(values.len());
    for index in 0..values.len() {
        let position = values.value(index);
        if position >= grid_len {
            return Err(DataFusionError::Execution(format!(
                "evaluated frame position {position} is outside a grid of {grid_len} points"
            )));
        }
        let position = usize::try_from(position).map_err(|_| {
            DataFusionError::Execution("evaluated frame position does not fit usize".to_string())
        })?;
        if positions.last().is_some_and(|last| *last >= position) {
            return Err(DataFusionError::Execution(format!(
                "evaluated frame positions must be strictly increasing, {position} is not"
            )));
        }
        positions.push(position);
    }
    Ok(positions)
}

/// The maximal runs of consecutive absolute positions, in order.
fn run_spans(positions: &[usize]) -> Vec<GridSpan> {
    let mut spans: Vec<GridSpan> = Vec::new();
    for &position in positions {
        match spans.last_mut() {
            Some(span) if span.start() + span.len() == position => {
                *span = GridSpan::new(span.start(), span.len() + 1);
            }
            _ => spans.push(GridSpan::new(position, 1)),
        }
    }
    spans
}

/// Downcasts one column to its declared Arrow type.
fn down<'a, T: Array + 'static>(array: &'a ArrayRef, column: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "evaluated frame column {column} has an unexpected type"
        ))
    })
}

/// Reports a series-model rejection as the execution error the caller sees.
fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Orders the frames of one execution's output by producer and frame ordinal.
///
/// A producer's frames start at ordinal 0 and follow without gaps, whatever their finality, since
/// one producer hands over many series and each closes once on its own frame. Switching producer
/// requires the previous one to have closed, producers are numbered strictly upwards, and the
/// execution ends on a closing frame. The state is three scalars: labels, positions, occurrence
/// identity and per-producer history stay with the runtime that already owns them.
#[derive(Debug, Default)]
pub(crate) struct Sequence {
    previous_producer: Option<u64>,
    next_ordinal: u64,
    last_closed: bool,
}

impl Sequence {
    /// Records the next frame, rejecting an ordinal out of order, a producer boundary before the
    /// previous producer closed, and a producer that goes backwards.
    pub(crate) fn check(&mut self, producer: u64, ordinal: u64, is_last: bool) -> Result<()> {
        let expected = match self.previous_producer {
            None => 0,
            Some(previous) if producer == previous => self.next_ordinal,
            Some(previous) => {
                if producer < previous {
                    return Err(DataFusionError::Execution(format!(
                        "evaluated frame producer {producer} goes backwards from {previous}"
                    )));
                }
                if !self.last_closed {
                    return Err(DataFusionError::Execution(format!(
                        "evaluated frame of producer {producer} starts before producer {previous} \
                         closed"
                    )));
                }
                0
            }
        };
        if ordinal != expected {
            return Err(DataFusionError::Execution(format!(
                "evaluated frame of producer {producer} has ordinal {ordinal}, expected {expected}"
            )));
        }
        let next = ordinal.checked_add(1).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "evaluated frame ordinal {ordinal} of producer {producer} cannot be incremented"
            ))
        })?;
        self.previous_producer = Some(producer);
        self.next_ordinal = next;
        self.last_closed = is_last;
        Ok(())
    }

    /// Checks that the sequence ended on a closing frame, if it began at all.
    pub(crate) fn finish(&self) -> Result<()> {
        match self.previous_producer {
            Some(producer) if !self.last_closed => Err(DataFusionError::Execution(format!(
                "evaluated frame producer {producer} ended without a closing frame"
            ))),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;
