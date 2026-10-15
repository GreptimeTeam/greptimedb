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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use common_error::ext::BoxedError;
use common_query::native_histogram::{
    histogram_samples_from_arrow, is_native_histogram_value_type,
};
use datatypes::arrow::array::{
    Array, BinaryArray, DictionaryArray, Float64Array, StructArray, TimestampMillisecondArray,
};
use datatypes::arrow::buffer::Buffer;
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::series::raw::{RawSeriesChunk, RawSeriesOrdering, RawTimeAxis};
use datatypes::series::samples::{Samples, SeriesFields, SeriesSchema};
use futures::{Stream, StreamExt};
use snafu::{IntoError, ResultExt};
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

use crate::error::{ComputeArrowSnafu, DataTypeMismatchSnafu, InvalidRecordBatchSnafu};
use crate::read::scan_util::PartitionMetrics;
use crate::read::series_scan::SeriesBatch;
use crate::read::stream::{ScanBatch, ScanBatchStream};
use crate::sst::parquet::flat_format::{primary_key_column_index, time_index_column_index};

fn boxed(error: crate::error::Error) -> BoxedError {
    BoxedError::new(error)
}

fn invalid(reason: impl Into<String>) -> BoxedError {
    boxed(
        InvalidRecordBatchSnafu {
            reason: reason.into(),
        }
        .build(),
    )
}

fn read_key(batch: &RecordBatch, row: usize) -> Result<&[u8], BoxedError> {
    let column = batch.column(primary_key_column_index(batch.num_columns()));
    if let Some(binary) = column.as_any().downcast_ref::<BinaryArray>() {
        return binary
            .is_valid(row)
            .then(|| binary.value(row))
            .ok_or_else(|| invalid("null primary key"));
    }
    let dictionary = column
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()
        .ok_or_else(|| invalid("invalid primary-key array"))?;
    let values = dictionary
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| invalid("invalid primary-key dictionary"))?;
    if !dictionary.keys().is_valid(row) {
        return Err(invalid("null primary key"));
    }
    let index = dictionary.keys().value(row) as usize;
    if index >= values.len() || !values.is_valid(index) {
        return Err(invalid("invalid primary-key dictionary value"));
    }
    Ok(values.value(index))
}

fn chunk(
    batch: &RecordBatch,
    start: usize,
    end: usize,
    schema: &SeriesSchema,
    fields: &[(usize, ConcreteDataType)],
) -> Result<RawSeriesChunk, BoxedError> {
    let key = read_key(batch, start)?;
    let timestamp = batch
        .column(time_index_column_index(batch.num_columns()))
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| invalid("non-millisecond timestamp array"))?;
    if (start..end).any(|row| !timestamp.is_valid(row)) {
        return Err(invalid("null timestamp"));
    }
    let time = RawTimeAxis::try_new(timestamp.values().slice(start, end - start))
        .context(DataTypeMismatchSnafu)
        .map_err(BoxedError::new)?;
    let mut samples = Vec::with_capacity(fields.len());
    for (index, data_type) in fields {
        let array = batch.column(*index).slice(start, end - start);
        if is_native_histogram_value_type(data_type) {
            let hist = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("invalid native histogram array"))?;
            samples.push(histogram_samples_from_arrow(hist).map_err(|error| {
                let source = common_query::error::Error::GeneralDataFusion {
                    error,
                    location: snafu::Location::new(file!(), line!(), column!()),
                };
                BoxedError::new(
                    crate::error::ExternalSnafu {
                        context: "native histogram conversion".to_string(),
                    }
                    .into_error(BoxedError::new(source)),
                )
            })?);
        } else {
            let values = cast(&array, &datatypes::arrow::datatypes::DataType::Float64)
                .context(ComputeArrowSnafu)
                .map_err(boxed)?;
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| invalid("invalid numeric field array"))?;
            samples.push(Samples::from_float(values.clone()));
        }
    }
    let fields = SeriesFields::try_from_vec(samples)
        .context(DataTypeMismatchSnafu)
        .map_err(BoxedError::new)?;
    RawSeriesChunk::try_new(
        schema,
        Buffer::from(key.to_vec()),
        None,
        time,
        fields,
        false,
    )
    .context(DataTypeMismatchSnafu)
    .map_err(BoxedError::new)
}

struct RawStream {
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    inner: futures::stream::BoxStream<'static, Result<RawSeriesChunk, BoxedError>>,
}

impl Stream for RawStream {
    type Item = Result<RawSeriesChunk, BoxedError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RawSeriesStream for RawStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }
    fn ordering(&self) -> RawSeriesOrdering {
        RawSeriesOrdering::Contiguous
    }
}

pub(crate) fn make_stream(
    inner: ScanBatchStream,
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    fields: Vec<(usize, ConcreteDataType)>,
    metrics: PartitionMetrics,
) -> SendableRawSeriesStream {
    let output_schema = schema.clone();
    let stream = async_stream::stream! {
        let mut input = inner;
        let mut pending: Option<RawSeriesChunk> = None;
        while let Some(item) = input.next().await {
            let records = match item {
                Ok(ScanBatch::Series(SeriesBatch::Flat(flat))) => flat.batches.into_vec(),
                Ok(ScanBatch::RecordBatch(batch)) => vec![batch],
                Err(error) => { yield Err(BoxedError::new(error)); return; }
            };
            for record in records {
                let mut start = 0;
                while start < record.num_rows() {
                    let key = match read_key(&record, start) { Ok(key) => key, Err(error) => { yield Err(error); return; } };
                    let mut end = start + 1;
                    while end < record.num_rows() {
                        match read_key(&record, end) {
                            Ok(next) if next == key => end += 1,
                            Ok(_) => break,
                            Err(error) => { yield Err(error); return; }
                        }
                    }
                    let begun = Instant::now();
                    let current = match chunk(&record, start, end, &output_schema, &fields) {
                        Ok(chunk) => chunk, Err(error) => { yield Err(error); return; }
                    };
                    metrics.inc_convert_batch_cost(begun.elapsed());
                    if let Some(old) = pending.as_ref() {
                        if old.read_key.as_slice() == current.read_key.as_slice()
                            && current.time.timestamp(0).zip(old.time.timestamp(old.time.len() - 1))
                                .is_some_and(|(next, previous)| next <= previous) {
                            yield Err(invalid("raw timestamps are not strictly increasing")); return;
                        }
                    }
                    if let Some(mut old) = pending.take() {
                        if old.read_key.as_slice() != current.read_key.as_slice() { old.is_last = true; }
                        yield Ok(old);
                    }
                    pending = Some(current);
                    start = end;
                }
            }
        }
        if let Some(mut last) = pending { last.is_last = true; yield Ok(last); }
    };
    Box::pin(RawStream {
        schema,
        label_source,
        inner: Box::pin(stream),
    })
}
