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

//! Finite Arrow batches for raw series chunks. Field names are ordinal (`f0`, `f1`, ...);
//! callers bind those positions to their logical names, which this frame cannot authenticate.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, StringArray,
    StructArray, UInt32Array,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::error::{InvalidSeriesSnafu, Result};
use crate::series::histogram::{HistogramBatch, fields as histogram_fields};
use crate::series::raw::{RawSeriesChunk, RawTimeAxis};
use crate::series::samples::{
    DenseSamplesRef, HistogramSamples, Samples, SeriesFields, SeriesSchema,
};
use crate::series::{Label, MetricName, invalid_series};

const LAYOUT: &str = "greptime.series.layout";
const CONTENT: &str = "greptime.series.content";
const LABELS: &str = "greptime.series.labels";
const TIME: &str = "greptime.series.time";

/// Whether a raw frame carries only the complete key or explicit metric labels as well.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawLabelMode {
    /// Labels remain encoded solely in `read_key`.
    Key,
    /// Complete metric name and ordinary labels are present in the frame.
    Explicit,
}

/// Builds the typed schema for a raw frame using ordinal domain fields.
pub fn raw_series_frame_schema(schema: &SeriesSchema, mode: RawLabelMode) -> Result<SchemaRef> {
    if schema.eval_grid.is_some() {
        return invalid_series("raw series schema cannot have an evaluation grid");
    }
    if schema.fields.is_empty() {
        return invalid_series("raw series schema must have fields");
    }
    let list = |ty, nullable| DataType::List(Arc::new(Field::new("item", ty, nullable)));
    let time = DataType::Struct(
        vec![Field::new(
            "timestamps",
            list(DataType::Int64, false),
            false,
        )]
        .into(),
    );
    let hist = DataType::Struct(
        vec![
            Field::new("indices", list(DataType::UInt32, true), false),
            Field::new(
                "table",
                list(DataType::Struct(histogram_fields()), false),
                false,
            ),
        ]
        .into(),
    );
    let payload = DataType::Struct(
        vec![
            Field::new("float_values", list(DataType::Float64, true), true),
            Field::new("histogram", hist, true),
        ]
        .into(),
    );
    let fields: Fields = schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, _)| Field::new(format!("f{i}"), payload.clone(), false))
        .collect();
    let mut columns = vec![
        Field::new("read_key", DataType::Binary, false),
        Field::new("is_last", DataType::Boolean, false),
        Field::new("time", time, false),
        Field::new("fields", DataType::Struct(fields), false),
    ];
    if mode == RawLabelMode::Explicit {
        columns.push(Field::new("name", DataType::Utf8, true));
        columns.push(Field::new(
            "labels",
            list(
                DataType::Struct(
                    vec![
                        Field::new("name", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            ),
            false,
        ));
    }
    let metadata = [
        (LAYOUT.to_string(), "v1".to_string()),
        (CONTENT.to_string(), "raw".to_string()),
        (
            LABELS.to_string(),
            match mode {
                RawLabelMode::Key => "key",
                RawLabelMode::Explicit => "explicit",
            }
            .to_string(),
        ),
        (TIME.to_string(), "timestamps".to_string()),
    ]
    .into_iter()
    .collect();
    Ok(Arc::new(Schema::new_with_metadata(columns, metadata)))
}

/// Packs one raw chunk into one-row batches at the union of field segment boundaries.
pub fn pack_raw_series_chunk(
    schema: &SeriesSchema,
    mode: RawLabelMode,
    chunk: &RawSeriesChunk,
) -> Result<Vec<RecordBatch>> {
    let checked = RawSeriesChunk::try_new(
        schema,
        chunk.read_key.clone(),
        chunk.metric.clone(),
        chunk.time.clone(),
        chunk.fields.clone(),
        chunk.is_last,
    )?;
    match (mode, &checked.metric) {
        (RawLabelMode::Key, None) | (RawLabelMode::Explicit, Some(_)) => {}
        _ => return invalid_series("raw chunk label mode does not match frame mode"),
    }
    let frame_schema = raw_series_frame_schema(schema, mode)?;
    let RawTimeAxis::Explicit(times) = &checked.time;
    let all: Vec<&[Samples]> = checked.fields.iter().map(Samples::segments).collect();
    let mut segment = vec![0usize; all.len()];
    let mut offset = vec![0usize; all.len()];
    let mut pos = 0usize;
    let mut batches = Vec::new();
    while pos < times.len() {
        let take = all
            .iter()
            .enumerate()
            .map(|(i, parts)| parts[segment[i]].len() - offset[i])
            .min()
            .unwrap_or(times.len())
            .min(times.len() - pos);
        if take == 0 {
            return invalid_series("empty sample segment");
        }
        let pieces = all
            .iter()
            .enumerate()
            .map(|(i, parts)| parts[segment[i]].slice(offset[i], take))
            .collect::<Result<Vec<_>>>()?;
        batches.push(encode_row(
            &frame_schema,
            mode,
            &checked.read_key,
            checked.metric.as_ref(),
            checked.is_last && pos + take == times.len(),
            times.slice(pos, take),
            &pieces,
        )?);
        pos += take;
        for (i, parts) in all.iter().enumerate() {
            offset[i] += take;
            if offset[i] == parts[segment[i]].len() {
                segment[i] += 1;
                offset[i] = 0;
            }
        }
    }
    Ok(batches)
}

/// Decodes all rows of a raw frame; any malformed row makes the entire call fail.
pub fn unpack_raw_series_frame(
    schema: &SeriesSchema,
    mode: RawLabelMode,
    batch: &RecordBatch,
) -> Result<Vec<RawSeriesChunk>> {
    let expected = raw_series_frame_schema(schema, mode)?;
    let actual = batch.schema();
    for key in [LAYOUT, CONTENT, LABELS, TIME] {
        if actual.metadata().get(key) != expected.metadata().get(key) {
            return invalid_series("raw frame metadata mismatch");
        }
    }
    if actual
        .metadata()
        .keys()
        .any(|key| key == "greptime.series.grid" || key.starts_with("greptime.series.grid."))
        || actual.fields() != expected.fields()
    {
        return invalid_series("raw frame schema mismatch");
    }
    let cols = batch.columns();
    let keys = down::<BinaryArray>(&cols[0], "read_key")?;
    let lasts = down::<BooleanArray>(&cols[1], "is_last")?;
    let time_struct = down::<StructArray>(&cols[2], "time")?;
    let times = down::<ListArray>(time_struct.column(0), "timestamps")?;
    let fs = down::<StructArray>(&cols[3], "fields")?;
    let (names, labels) = if mode == RawLabelMode::Explicit {
        (
            Some(down::<StringArray>(&cols[4], "name")?),
            Some(down::<ListArray>(&cols[5], "labels")?),
        )
    } else {
        (None, None)
    };
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if keys.is_null(row)
            || lasts.is_null(row)
            || time_struct.is_null(row)
            || times.is_null(row)
            || fs.is_null(row)
        {
            return invalid_series("null required raw frame parent");
        }
        let timestamp_values = times.value(row);
        let tv = down::<Int64Array>(&timestamp_values, "timestamp values")?;
        if tv.null_count() != 0 {
            return invalid_series("null timestamp");
        }
        let time = tv.values().clone();
        let metric = match (names, labels) {
            (None, None) => None,
            (Some(ns), Some(ls)) => {
                if ls.is_null(row) {
                    return invalid_series("null labels list");
                }
                let label_values = ls.value(row);
                let label_struct = down::<StructArray>(&label_values, "labels")?;
                if label_struct.null_count() != 0 {
                    return invalid_series("null label struct element");
                }
                let ln = down::<StringArray>(label_struct.column(0), "label names")?;
                let lv = down::<StringArray>(label_struct.column(1), "label values")?;
                if ln.null_count() != 0 || lv.null_count() != 0 {
                    return invalid_series("null label element");
                }
                let tags = (0..ln.len())
                    .map(|i| Label {
                        name: Arc::from(ln.value(i)),
                        value: Arc::from(lv.value(i)),
                    })
                    .collect();
                Some(MetricName::try_new(
                    (!ns.is_null(row)).then(|| Arc::from(ns.value(row))),
                    tags,
                )?)
            }
            _ => return invalid_series("incomplete explicit label columns"),
        };
        let mut decoded = Vec::with_capacity(fs.num_columns());
        for col in fs.columns() {
            let payload = down::<StructArray>(col, "field payload")?;
            if payload.is_null(row) {
                return invalid_series("null field payload");
            }
            let floats_list = down::<ListArray>(payload.column(0), "float values")?;
            let hist = down::<StructArray>(payload.column(1), "histogram payload")?;
            let has_float = !floats_list.is_null(row);
            let has_hist = !hist.is_null(row);
            if !has_float && !has_hist {
                return invalid_series("field has neither float nor histogram payload");
            }
            let floats = if has_float {
                down::<Float64Array>(&floats_list.value(row), "float values")?.clone()
            } else {
                Float64Array::new_null(0)
            };
            if has_float && floats.len() != time.len() {
                return invalid_series("float payload length mismatch");
            }
            let samples = if !has_hist {
                Samples::from_float(floats)
            } else {
                let inds = down::<ListArray>(hist.column(0), "histogram indices")?;
                let tables = down::<ListArray>(hist.column(1), "histogram table")?;
                if inds.is_null(row) || tables.is_null(row) {
                    return invalid_series("null histogram indices or table");
                }
                let indices = down::<UInt32Array>(&inds.value(row), "histogram indices")?.clone();
                if indices.len() != time.len() {
                    return invalid_series("histogram index length mismatch");
                }
                let table = down::<StructArray>(&tables.value(row), "histogram table")?.clone();
                let batch = Arc::new(HistogramBatch::try_from_arrow(table)?);
                let hs = HistogramSamples::try_new(indices, batch)?;
                if has_float {
                    Samples::try_from_mixed(floats, hs)?
                } else {
                    Samples::try_from_histogram(hs.indices().clone(), hs.batch().clone())?
                }
            };
            decoded.push(samples);
        }
        let fields = SeriesFields::try_from_vec(decoded)?;
        let key = binary_slice(keys, row)?;
        let raw_time = RawTimeAxis::try_new(time)?;
        out.push(RawSeriesChunk::try_new(
            schema,
            key,
            metric,
            raw_time,
            fields,
            lasts.value(row),
        )?);
    }
    Ok(out)
}

fn encode_row(
    schema: &SchemaRef,
    mode: RawLabelMode,
    key: &Buffer,
    metric: Option<&MetricName>,
    last: bool,
    times: ScalarBuffer<i64>,
    samples: &[Samples],
) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(binary_one(key.as_slice())?),
        Arc::new(BooleanArray::from(vec![last])),
    ];
    let ts = wrap_list(
        Arc::new(Int64Array::new(times, None)),
        DataType::Int64,
        false,
    )?;
    let DataType::Struct(time_fields) = schema.field(2).data_type() else {
        return invalid_series("time schema mismatch");
    };
    arrays.push(Arc::new(struct_array(
        time_fields.clone(),
        vec![Arc::new(ts)],
    )?));
    let DataType::Struct(field_schema) = schema.field(3).data_type() else {
        return invalid_series("fields schema mismatch");
    };
    let DataType::Struct(first_payload) = field_schema[0].data_type() else {
        return invalid_series("payload schema mismatch");
    };
    let DataType::Struct(histogram_fields) = first_payload[1].data_type() else {
        return invalid_series("histogram schema mismatch");
    };
    let mut field_arrays = Vec::new();
    for sample in samples {
        let DataType::Struct(dtype) = field_schema[field_arrays.len()].data_type() else {
            return invalid_series("payload schema mismatch");
        };
        let (floats, hist) = encode_payload(sample, histogram_fields)?;
        field_arrays.push(Arc::new(struct_array(dtype.clone(), vec![floats, hist])?) as ArrayRef);
    }
    arrays.push(Arc::new(struct_array(field_schema.clone(), field_arrays)?));
    if mode == RawLabelMode::Explicit {
        let metric = metric.ok_or_else(|| err("explicit labels missing"))?;
        checked_offset(metric.name().map_or(0, str::len))?;
        arrays.push(Arc::new(StringArray::from(vec![metric.name()])));
        checked_offset(metric.tags().len())?;
        checked_offset(
            metric
                .tags()
                .iter()
                .try_fold(0usize, |n, l| n.checked_add(l.name.len()))
                .ok_or_else(|| err("label name length overflow"))?,
        )?;
        checked_offset(
            metric
                .tags()
                .iter()
                .try_fold(0usize, |n, l| n.checked_add(l.value.len()))
                .ok_or_else(|| err("label value length overflow"))?,
        )?;
        let names: StringArray = metric
            .tags()
            .iter()
            .map(|l| Some(l.name.as_ref()))
            .collect();
        let values: StringArray = metric
            .tags()
            .iter()
            .map(|l| Some(l.value.as_ref()))
            .collect();
        let DataType::List(label_list) = schema.field(5).data_type() else {
            return invalid_series("labels schema mismatch");
        };
        let DataType::Struct(label_fields) = label_list.data_type() else {
            return invalid_series("label struct schema mismatch");
        };
        let labels = struct_array(
            label_fields.clone(),
            vec![Arc::new(names), Arc::new(values)],
        )?;
        arrays.push(Arc::new(wrap_list(
            Arc::new(labels),
            DataType::Struct(label_fields.clone()),
            false,
        )?));
    }
    arrow(RecordBatch::try_new_with_options(
        schema.clone(),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(1)),
    ))
}

fn encode_payload(samples: &Samples, histogram_fields: &Fields) -> Result<(ArrayRef, ArrayRef)> {
    let (floats, histograms) = match samples
        .dense()
        .ok_or_else(|| err("sample segment is not dense"))?
    {
        DenseSamplesRef::Float(values) => (Some(values), None),
        DenseSamplesRef::Histogram(values) => (None, Some(values)),
        DenseSamplesRef::Mixed(values) => (Some(values.floats()), Some(values.histograms())),
    };
    let float_array = match floats {
        Some(values) => Arc::new(wrap_list(
            Arc::new(values.clone()),
            DataType::Float64,
            true,
        )?) as ArrayRef,
        None => Arc::new(ListArray::new_null(
            Arc::new(Field::new("item", DataType::Float64, true)),
            1,
        )) as ArrayRef,
    };
    let hist_array = match histograms {
        Some(values) => {
            let DataType::List(indices_field) = histogram_fields[0].data_type() else {
                return invalid_series("histogram index schema mismatch");
            };
            let DataType::List(table_field) = histogram_fields[1].data_type() else {
                return invalid_series("histogram table schema mismatch");
            };
            let indices = wrap_list(
                Arc::new(values.indices().clone()),
                indices_field.data_type().clone(),
                true,
            )?;
            let table = wrap_list(
                Arc::new(values.batch().as_arrow().clone()),
                table_field.data_type().clone(),
                false,
            )?;
            Arc::new(struct_array(
                histogram_fields.clone(),
                vec![Arc::new(indices), Arc::new(table)],
            )?) as ArrayRef
        }
        None => null_histogram(histogram_fields)?,
    };
    Ok((float_array, hist_array))
}

fn binary_slice(array: &BinaryArray, row: usize) -> Result<Buffer> {
    let offsets = array.value_offsets();
    let start = usize::try_from(offsets[row]).map_err(|_| err("negative binary offset"))?;
    let end = usize::try_from(offsets[row + 1]).map_err(|_| err("negative binary offset"))?;
    let len = end
        .checked_sub(start)
        .ok_or_else(|| err("invalid binary offsets"))?;
    Ok(array.values().slice_with_length(start, len))
}
fn binary_one(value: &[u8]) -> Result<BinaryArray> {
    checked_offset(value.len())?;
    Ok(BinaryArray::from(vec![Some(value)]))
}
fn checked_offset(len: usize) -> Result<i32> {
    i32::try_from(len).map_err(|_| err("Arrow offset limit exceeded"))
}
fn offsets(len: usize) -> Result<OffsetBuffer<i32>> {
    let len = checked_offset(len)?;
    Ok(OffsetBuffer::new(ScalarBuffer::from(vec![0, len])))
}
fn wrap_list(values: ArrayRef, item: DataType, nullable: bool) -> Result<ListArray> {
    let len = values.len();
    arrow(ListArray::try_new(
        Arc::new(Field::new("item", item, nullable)),
        offsets(len)?,
        values,
        None,
    ))
}
fn null_histogram(fields: &Fields) -> Result<ArrayRef> {
    let DataType::List(indices_field) = fields[0].data_type() else {
        return invalid_series("histogram index schema mismatch");
    };
    let DataType::List(table_field) = fields[1].data_type() else {
        return invalid_series("histogram table schema mismatch");
    };
    let DataType::Struct(table_fields) = table_field.data_type() else {
        return invalid_series("histogram table schema mismatch");
    };
    let indices = wrap_list(
        Arc::new(UInt32Array::new_null(0)),
        indices_field.data_type().clone(),
        true,
    )?;
    let columns = table_fields
        .iter()
        .map(|f| arrow::array::new_null_array(f.data_type(), 0))
        .collect();
    let table = arrow(StructArray::try_new(table_fields.clone(), columns, None))?;
    let table = wrap_list(Arc::new(table), table_field.data_type().clone(), false)?;
    Ok(Arc::new(arrow(StructArray::try_new(
        fields.clone(),
        vec![Arc::new(indices), Arc::new(table)],
        Some(NullBuffer::new_null(1)),
    ))?))
}
fn struct_array(fields: Fields, arrays: Vec<ArrayRef>) -> Result<StructArray> {
    arrow(StructArray::try_new(fields, arrays, None))
}
fn down<'a, T: Array + 'static>(array: &'a ArrayRef, label: &str) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| err(format!("{label} type mismatch")))
}
fn err(reason: impl Into<String>) -> crate::error::Error {
    InvalidSeriesSnafu {
        reason: reason.into(),
    }
    .build()
}
fn arrow<T>(result: std::result::Result<T, arrow::error::ArrowError>) -> Result<T> {
    result.map_err(|e| err(e.to_string()))
}
