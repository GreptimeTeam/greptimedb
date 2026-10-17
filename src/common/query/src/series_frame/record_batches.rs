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

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, StringArray, StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::row::{RowConverter, Rows, SortField};
use datafusion_common::{DataFusionError, Result as DfResult};
use datatypes::arrow_array::string_array_value_at_index;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use futures_util::Stream;
use snafu::Location;
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

use crate::error::Error;
use crate::native_histogram::{histogram_samples_from_arrow, native_histogram_arrow_type};
use crate::series_frame::{codec_error, invalid};

/// Explicit mapping from selected record-batch columns to raw-series identity and payload.
///
/// `time_index` selects a timezone-naive millisecond timestamp column. `metric_name` is an
/// optional constant logical metric name, not a source column. `tags` binds all logical tag
/// names to their selected source columns; supported tag columns are Utf8, LargeUtf8, Utf8View,
/// or integer-key string dictionaries. `fields` binds selected source columns in output field
/// order; supported field columns are Float64 and the native-histogram struct type.
#[derive(Clone, Debug)]
pub struct InputMapping {
    /// Source column containing timezone-naive millisecond timestamps.
    pub time_index: usize,
    /// Optional constant metric name; `None` omits the metric name.
    pub metric_name: Option<Arc<str>>,
    /// Complete logical tag mapping, as `(tag_name, source_column_index)` pairs.
    pub tags: Vec<(Arc<str>, usize)>,
    /// Selected payload columns in output order, paired with their logical field specs.
    pub fields: Vec<(FieldSpec, usize)>,
}

/// Converts ordinary record batches to contiguous raw-series chunks.
///
/// The already-executed child must keep each complete identity contiguous and timestamps
/// ascending. The adapter checks adjacent runs only; it cannot prove global absence of A/B/A.
/// Encoded read keys are opaque identities comparable only within this converter stream; they
/// are not canonical cross-region or wire encodings. Float fields share their source buffers;
/// native-histogram struct fields use the existing histogram bridge.
pub fn record_batches_to_raw_series(
    source: DfSendableRecordBatchStream,
    mapping: InputMapping,
) -> DfResult<SendableRawSeriesStream> {
    let arrow_schema = source.schema();
    let (mapping, schema, converter) = validate_mapping(&arrow_schema, mapping)?;
    Ok(Box::pin(InputStream {
        source: Some(source),
        arrow_schema,
        mapping,
        schema,
        converter,
        batch: None,
        row: 0,
        pending: None,
        label_source: RawLabelSource::Explicit,
    }))
}

fn validate_mapping(
    schema: &SchemaRef,
    mut mapping: InputMapping,
) -> DfResult<(InputMapping, Arc<SeriesSchema>, RowConverter)> {
    let mut used = HashSet::new();
    let mut use_index = |index: usize| -> DfResult<()> {
        if index >= schema.fields().len() {
            return Err(invalid("mapped column index is out of bounds"));
        }
        if !used.insert(index) {
            return Err(invalid("source column is mapped to multiple roles"));
        }
        Ok(())
    };
    use_index(mapping.time_index)?;
    if schema.field(mapping.time_index).data_type()
        != &DataType::Timestamp(TimeUnit::Millisecond, None)
    {
        return Err(invalid("time column must be Timestamp(Millisecond, None)"));
    }
    for (_, index) in &mapping.tags {
        use_index(*index)?;
        let ty = schema.field(*index).data_type();
        if !string_type(ty) && !integer_key_type(ty) {
            return Err(invalid(
                "tag column must be string or integer-key string dictionary",
            ));
        }
        if matches!(ty, DataType::Dictionary(_, value) if !string_type(value)) {
            return Err(invalid("tag dictionary values must be strings"));
        }
    }
    if mapping.fields.is_empty() {
        return Err(invalid("at least one field is required"));
    }
    let mut field_names = HashSet::new();
    for (spec, index) in &mapping.fields {
        use_index(*index)?;
        if !field_names.insert(spec.name.clone()) {
            return Err(invalid("duplicate logical field name"));
        }
        let ty = schema.field(*index).data_type();
        if ty != &DataType::Float64 && ty != &native_histogram_arrow_type() {
            return Err(invalid("field must be Float64 or native histogram"));
        }
    }
    // MetricName validates logical label names and duplicate tags through its canonical path.
    MetricName::try_new(
        mapping.metric_name.clone(),
        mapping
            .tags
            .iter()
            .map(|(name, _)| Label {
                name: name.clone(),
                value: Arc::from(""),
            })
            .collect(),
    )
    .map_err(codec_error)?;
    mapping.tags.sort_by(|a, b| a.0.cmp(&b.0));
    let raw_schema = Arc::new(SeriesSchema {
        fields: mapping
            .fields
            .iter()
            .map(|(spec, _)| spec.clone())
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    });
    let mut sorts = Vec::with_capacity(1 + mapping.tags.len() * 2);
    sorts.push(SortField::new(DataType::Utf8));
    for (_, index) in &mapping.tags {
        sorts.push(SortField::new(DataType::Utf8));
        sorts.push(SortField::new(schema.field(*index).data_type().clone()));
    }
    let converter = RowConverter::new(sorts)?;
    Ok((mapping, raw_schema, converter))
}

fn string_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}
fn integer_key_type(ty: &DataType) -> bool {
    matches!(ty, DataType::Dictionary(key, value) if matches!(key.as_ref(), DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64) && string_type(value))
}

fn stream_error(error: DataFusionError) -> BoxedError {
    BoxedError::new(Error::GeneralDataFusion {
        error,
        location: Location::default(),
    })
}

struct InputStream {
    source: Option<DfSendableRecordBatchStream>,
    arrow_schema: SchemaRef,
    mapping: InputMapping,
    schema: Arc<SeriesSchema>,
    converter: RowConverter,
    batch: Option<(DfRecordBatch, Rows)>,
    row: usize,
    pending: Option<RawSeriesChunk>,
    label_source: RawLabelSource,
}
impl RawSeriesStream for InputStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }
    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::Contiguous
    }
}
impl InputStream {
    fn finish(&mut self) {
        self.source = None;
        self.batch = None;
        self.row = 0;
        self.pending = None;
    }
    fn batch_key_columns(&self, batch: &DfRecordBatch) -> Vec<ArrayRef> {
        let n = batch.num_rows();
        let metric: ArrayRef = match &self.mapping.metric_name {
            Some(name) => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                name.as_ref(),
                n,
            ))),
            None => Arc::new(StringArray::new_null(n)),
        };
        let mut cols = vec![metric];
        for (tag, index) in &self.mapping.tags {
            cols.push(Arc::new(StringArray::from_iter_values(
                std::iter::repeat_n(tag.as_ref(), n),
            )));
            cols.push(batch.column(*index).clone());
        }
        cols
    }
    fn read_chunk(
        &self,
        batch: &DfRecordBatch,
        start: usize,
        end: usize,
        key: Vec<u8>,
    ) -> DfResult<RawSeriesChunk> {
        let len = end - start;
        let time = batch
            .column(self.mapping.time_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| invalid("invalid timestamp array"))?;
        if time.slice(start, len).null_count() != 0 {
            return Err(invalid("time values must not be null"));
        }
        let axis = RawTimestamps::try_new(time.values().slice(start, len)).map_err(codec_error)?;
        let mut fields = Vec::with_capacity(self.mapping.fields.len());
        for (_, index) in &self.mapping.fields {
            let array = batch.column(*index);
            if array.data_type() == &DataType::Float64 {
                let values = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| invalid("invalid Float64 array"))?;
                fields.push(Samples::from_float(values.slice(start, len)));
            } else {
                let hist = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| invalid("invalid histogram array"))?;
                fields.push(histogram_samples_from_arrow(&hist.slice(start, len))?);
            }
        }
        let fields = SeriesFields::try_from_vec(fields).map_err(codec_error)?;
        let metric_name = self.mapping.metric_name.clone();
        let mut labels = Vec::new();
        for (name, index) in &self.mapping.tags {
            if let Some(value) = string_array_value_at_index(batch.column(*index), start) {
                labels.push(Label {
                    name: name.clone(),
                    value: Arc::from(value),
                });
            }
        }
        let metric = MetricName::try_new(metric_name, labels).map_err(codec_error)?;
        RawSeriesChunk::try_new(
            &self.schema,
            Buffer::from(key),
            Some(metric),
            axis,
            fields,
            true,
        )
        .map_err(codec_error)
    }
}
impl Stream for InputStream {
    type Item = Result<RawSeriesChunk, BoxedError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        const MAX_EMPTY_BATCHES: usize = 32;
        let this = &mut *self;
        let mut empties = 0;
        loop {
            if this.batch.is_none() {
                match this.source.as_mut().map(|s| s.as_mut().poll_next(cx)) {
                    None | Some(Poll::Ready(None)) => {
                        if let Some(mut final_chunk) = this.pending.take() {
                            final_chunk.is_last = true;
                            this.finish();
                            return Poll::Ready(Some(Ok(final_chunk)));
                        }
                        this.finish();
                        return Poll::Ready(None);
                    }
                    Some(Poll::Pending) => return Poll::Pending,
                    Some(Poll::Ready(Some(Err(error)))) => {
                        this.finish();
                        return Poll::Ready(Some(Err(stream_error(error))));
                    }
                    Some(Poll::Ready(Some(Ok(batch)))) => {
                        if batch.schema() != this.arrow_schema {
                            this.finish();
                            return Poll::Ready(Some(Err(stream_error(invalid(
                                "record batch schema mismatch",
                            )))));
                        }
                        if batch.num_rows() == 0 {
                            empties += 1;
                            if empties >= MAX_EMPTY_BATCHES {
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                            continue;
                        }
                        let columns = this.batch_key_columns(&batch);
                        let rows = match this.converter.convert_columns(&columns) {
                            Ok(rows) => rows,
                            Err(error) => {
                                this.finish();
                                return Poll::Ready(Some(Err(stream_error(error.into()))));
                            }
                        };
                        this.batch = Some((batch, rows));
                        this.row = 0;
                    }
                }
            }
            let Some((batch, rows)) = this.batch.as_ref() else {
                continue;
            };
            let start = this.row;
            let key = rows.row(start).as_ref().to_vec();
            if this
                .pending
                .as_ref()
                .is_some_and(|previous| previous.primary_key.as_slice() != key.as_slice())
                && let Some(mut complete) = this.pending.take()
            {
                complete.is_last = true;
                return Poll::Ready(Some(Ok(complete)));
            }
            let mut end = start + 1;
            while end < batch.num_rows() && rows.row(end).as_ref() == key {
                end += 1;
            }
            let chunk = match this.read_chunk(batch, start, end, key) {
                Ok(chunk) => chunk,
                Err(error) => {
                    this.finish();
                    return Poll::Ready(Some(Err(stream_error(error))));
                }
            };
            this.row = end;
            if end == batch.num_rows() {
                this.batch = None;
            }
            if let Some(mut previous) = this.pending.take() {
                let Some(last) = previous.time.timestamp(previous.time.len() - 1) else {
                    this.finish();
                    return Poll::Ready(Some(Err(stream_error(invalid(
                        "empty pending raw chunk",
                    )))));
                };
                let Some(first) = chunk.time.timestamp(0) else {
                    this.finish();
                    return Poll::Ready(Some(Err(stream_error(invalid("empty raw chunk")))));
                };
                if first <= last {
                    this.finish();
                    return Poll::Ready(Some(Err(stream_error(invalid(
                        "timestamps are not increasing across adjacent batches",
                    )))));
                }
                previous.is_last = false;
                this.pending = Some(chunk);
                return Poll::Ready(Some(Ok(previous)));
            }
            this.pending = Some(chunk);
        }
    }
}
