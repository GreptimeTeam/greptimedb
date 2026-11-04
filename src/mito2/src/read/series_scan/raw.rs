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

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use common_error::ext::BoxedError;
use common_query::error::Error as QueryError;
use common_query::native_histogram::{
    histogram_samples_from_arrow, is_native_histogram_value_type,
};
use common_query::series_frame::label_decode_spec;
use common_recordbatch::filter;
use datafusion::physical_plan::PhysicalExpr;
use datatypes::arrow::array::{
    Array, BinaryArray, Float64Array, StructArray, TimestampMillisecondArray,
};
use datatypes::arrow::buffer::Buffer;
use datatypes::arrow::compute::{cast, concat_batches};
use datatypes::arrow::datatypes::ArrowNativeType;
use datatypes::arrow::datatypes::DataType;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::LabelDecodeSpec;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{Samples, SeriesFields, SeriesSchema};
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use snafu::Location;
use snafu::{IntoError, ResultExt};
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

use crate::error::{
    ComputeArrowSnafu, DataTypeMismatchSnafu, Error, ExternalSnafu, InvalidRecordBatchSnafu,
};
use crate::read::scan_util::PartitionMetrics;
use crate::read::series_scan::SeriesBatch;
use crate::read::stream::{ScanBatch, ScanBatchStream};
use crate::sst::parquet::flat_format::{primary_key_column_index, time_index_column_index};
use crate::sst::parquet::format::PrimaryKeyArray;

fn boxed(error: Error) -> BoxedError {
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

fn chunk(
    batch: &RecordBatch,
    start: usize,
    end: usize,
    key: Buffer,
    schema: &SeriesSchema,
    fields: &[(usize, ConcreteDataType)],
) -> Result<RawSeriesChunk, BoxedError> {
    let timestamp = batch
        .column(time_index_column_index(batch.num_columns()))
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| invalid("non-millisecond timestamp array"))?;
    if (start..end).any(|row| !timestamp.is_valid(row)) {
        return Err(invalid("null timestamp"));
    }
    let time = RawTimestamps::try_new(timestamp.values().slice(start, end - start))
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
                let source = QueryError::GeneralDataFusion {
                    error,
                    location: Location::new(file!(), line!(), column!()),
                };
                BoxedError::new(
                    ExternalSnafu {
                        context: "native histogram conversion".to_string(),
                    }
                    .into_error(BoxedError::new(source)),
                )
            })?);
        } else {
            let values = cast(&array, &DataType::Float64)
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
    RawSeriesChunk::try_new(schema, key, None, time, fields, false)
        .context(DataTypeMismatchSnafu)
        .map_err(BoxedError::new)
}

struct RawStream {
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    /// The label decode description this stream declares, derived from `label_source` once.
    ///
    /// Key-encoded chunks leave `metric` as `None`, so a consumer that holds only the stream needs
    /// the description to decode them; deriving it here makes the producer self-describing instead
    /// of leaving the derivation to whoever packs the chunks. An invalid binding is cached as
    /// derived and rejected by the decoder downstream, never silently turned into `None`.
    label_decode_spec: Option<LabelDecodeSpec>,
    ordering: SeriesOrdering,
    inner: BoxStream<'static, Result<RawSeriesChunk, BoxedError>>,
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
    /// Returns the description derived from this stream's binding before the first poll; chunks
    /// carry key-encoded labels, so the description is what decoding needs.
    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        self.label_decode_spec.as_ref()
    }
    fn ordering(&self) -> SeriesOrdering {
        self.ordering
    }
}

/// Rows accumulated before the residual predicates are evaluated over one record.
///
/// A predicate pass is dominated by per-call setup (Arc refcount traffic, `DataType` clones and
/// the filtered `RecordBatch` allocation) rather than by per-row comparison, and one `ScanBatch`
/// carries the rows of a single series. Buffering up to this many rows and evaluating the
/// predicates once per group pays that setup once per group of series instead of once per series.
///
/// A record that alone reaches this many rows is not concatenated with its neighbours: it heads its
/// own group and is taken unchanged. A smaller record can still push a group past the threshold, so
/// a group holds fewer than this many rows plus one such record's rows, and both the buffered and
/// the concatenated arrays are alive while it is built. The value is a tuning choice, not a memory
/// bound; it also does not bound the bytes, since a record's rows are of variable width.
const PREDICATE_BATCH_ROWS: usize = 4096;

/// Returns the primary key at `row` of `record`, or `None` when the row cannot be decoded.
///
/// A `None` leaves reporting the malformed row to the chunk loop; it only decides how records are
/// grouped, never whether a record is valid.
fn primary_key_at_row(record: &RecordBatch, row: usize) -> Option<&[u8]> {
    let primary_key = record
        .column(primary_key_column_index(record.num_columns()))
        .as_any()
        .downcast_ref::<PrimaryKeyArray>()?;
    let values = primary_key
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()?;
    if !primary_key.keys().is_valid(row) {
        return None;
    }
    let index = primary_key.keys().value(row) as usize;
    if index >= values.len() || !values.is_valid(index) {
        return None;
    }
    Some(values.value(index))
}

/// Whether `next` continues the series that ends in `previous`.
///
/// Such a record must head its own group: concatenation merges a series' rows into one
/// primary-key run, which would fuse the two records' runs and change the chunk boundaries.
/// A record whose boundary key cannot be decoded is treated as a continuation, which isolates it
/// in its own group so the records before it keep their chunks and their error order.
fn continues_series(previous: &RecordBatch, next: &RecordBatch) -> bool {
    match (
        primary_key_at_row(previous, previous.num_rows() - 1),
        primary_key_at_row(next, 0),
    ) {
        (Some(previous), Some(next)) => previous == next,
        _ => true,
    }
}

/// A scanned record together with whether it ends the flat package it came from.
///
/// The divider only emits a [`SeriesBatch::Flat`] once it holds every row of the series in it, so
/// the record that ends such a package also ends the last series in it. A plain
/// [`ScanBatch::RecordBatch`] carries no such fact: an Arrow batch boundary is not a series
/// boundary, so its records always leave the flag clear.
struct IncomingRecord {
    batch: RecordBatch,
    ends_package: bool,
}

pub(crate) fn make_stream(
    inner: ScanBatchStream,
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    fields: Vec<(usize, ConcreteDataType)>,
    metrics: PartitionMetrics,
    predicates: Vec<Arc<dyn PhysicalExpr>>,
    ordering: SeriesOrdering,
) -> SendableRawSeriesStream {
    let output_schema = schema.clone();
    let label_decode_spec = label_decode_spec(&label_source);
    let stream = async_stream::stream! {
        let mut input = inner;
        let mut pending: Option<RawSeriesChunk> = None;
        let mut last_package_key: Option<Buffer> = None;
        // Records waiting for one predicate pass, and the number of rows they hold.
        let mut buffered: Vec<RecordBatch> = Vec::new();
        let mut buffered_rows = 0;
        // Without a predicate there is no per-call cost to amortize, so the threshold drops to one
        // record and buffering only reproduces the per-record stream this replaced.
        let coalesce_rows = if predicates.is_empty() { 1 } else { PREDICATE_BATCH_ROWS };
        // Records of the last scanned batch that are not buffered yet, and a record held back
        // because it continues the series the buffer ends with.
        let mut incoming: VecDeque<IncomingRecord> = VecDeque::new();
        let mut held: Option<IncomingRecord> = None;
        let mut exhausted = false;
        // An upstream error is reported only after the records buffered before it are processed,
        // so the chunks that precede it are still yielded first.
        let mut upstream_error: Option<Error> = None;
        loop {
            // The last complete package tail admitted to this predicate group, if any.
            let mut group_last_package_key: Option<Buffer> = None;
            // Fill: pull records until one predicate pass is amortized by the group.
            while !exhausted && upstream_error.is_none() && buffered_rows < coalesce_rows {
                let record = match held.take() {
                    Some(record) => record,
                    None => match incoming.pop_front() {
                        Some(record) => record,
                        None => {
                            let next = if group_last_package_key.is_none() {
                                input.next().await
                            } else {
                                match input.next().now_or_never() {
                                    Some(next) => next,
                                    None => break,
                                }
                            };
                            match next {
                            Some(Ok(ScanBatch::Series(SeriesBatch::Flat(flat)))) => {
                                // A package holds every row of the series in it, so the record that
                                // ends it closes the last of them. Empty records carry no rows and no
                                // series, which puts the flag on the last record that has rows; a
                                // package without any ends no series and closes nothing.
                                let ends_package = if ordering == SeriesOrdering::KeyThenTime {
                                    flat.batches
                                        .iter()
                                        .rposition(|batch| batch.num_rows() > 0)
                                } else {
                                    None
                                };
                                for (index, batch) in flat.batches.into_iter().enumerate() {
                                    if batch.num_rows() == 0 {
                                        continue;
                                    }
                                    incoming.push_back(IncomingRecord {
                                        batch,
                                        ends_package: Some(index) == ends_package,
                                    });
                                }
                                continue;
                            }
                            Some(Ok(ScanBatch::RecordBatch(batch))) => IncomingRecord {
                                batch,
                                ends_package: false,
                            },
                            Some(Err(error)) => { upstream_error = Some(error); break; }
                            None => { exhausted = true; break; }
                            }
                        },
                    },
                };
                if record.batch.num_rows() == 0 { continue; }
                // A record that reaches the threshold alone is a full batch: concatenating it with
                // what is buffered would copy it for no saving, since its predicate pass is paid
                // once either way. Let it head its own group, where the single-record path takes it
                // unchanged. A record that continues the buffered series is held back for the
                // reason `continues_series` documents.
                if let Some(previous) = buffered.last()
                    && (record.batch.num_rows() >= coalesce_rows
                        || continues_series(previous, &record.batch))
                {
                    held = Some(record);
                    break;
                }
                if ordering == SeriesOrdering::KeyThenTime
                    && let Some(previous) = last_package_key.as_ref()
                    && let Some(first) = primary_key_at_row(&record.batch, 0)
                    && first <= previous.as_slice()
                {
                    upstream_error = Some(InvalidRecordBatchSnafu {
                        reason: "raw series primary keys are not strictly increasing".to_string(),
                    }.build());
                    break;
                }
                buffered_rows += record.batch.num_rows();
                if record.ends_package
                    && let Some(key) = record.batch.num_rows().checked_sub(1)
                        .and_then(|row| primary_key_at_row(&record.batch, row))
                {
                    let key = Buffer::from(key);
                    last_package_key = Some(key.clone());
                    group_last_package_key = Some(key);
                }
                buffered.push(record.batch);
            }
            if buffered.is_empty() { break; }
            // Take the rows out of the buffer before processing them, so an early return cannot
            // replay them. A pending upstream error is reported only after these records, so the
            // chunks they hold are still yielded first; a coalesced group whose predicate fails
            // also withholds its `pending` chunk, and the two paths agree on that.
            let mut group: Vec<RecordBatch> = std::mem::take(&mut buffered);
            buffered_rows = 0;
            // A group of one record needs no concatenation, and copying it would only add work to
            // the case coalescing cannot help with.
            let mut record = if group.len() == 1 {
                group.pop().expect("group is non-empty")
            } else {
                match concat_batches(&group[0].schema(), &group) {
                    Ok(record) => record,
                    Err(error) => { yield Err(boxed(ComputeArrowSnafu.into_error(error))); return; }
                }
            };
            // The concatenated arrays own their buffers, so the source records can go now instead of
            // staying alive across this round's filtering and per-chunk yields.
            drop(group);
            // Completion metadata was captured before predicates, so a filtered tail still closes its package.
            for predicate in &predicates {
                record = match filter::batch_filter(&record, predicate) {
                    Ok(record) => record,
                    Err(error) => {
                        let source = QueryError::GeneralDataFusion {
                            error,
                            location: Location::new(file!(), line!(), column!()),
                        };
                        yield Err(BoxedError::new(ExternalSnafu {
                            context: "raw predicate evaluation".to_string(),
                        }.into_error(BoxedError::new(source))));
                        return;
                    }
                };
                if record.num_rows() == 0 {
                    break;
                }
            }
            // An emptied group contributes no chunk, and the chunk loop below yields nothing for it.
            if record.num_rows() > 0 {
                let pk_column = record.column(primary_key_column_index(record.num_columns()));
                let primary_key = match pk_column.as_any().downcast_ref::<PrimaryKeyArray>() {
                    Some(primary_key) => primary_key,
                    None => { yield Err(invalid("invalid primary-key array")); return; }
                };
                let values = match primary_key.values().as_any().downcast_ref::<BinaryArray>() {
                    Some(values) => values,
                    None => { yield Err(invalid("invalid primary-key dictionary")); return; }
                };
                let mut start = 0;
                while start < record.num_rows() {
                    let index = if primary_key.keys().is_valid(start) {
                        primary_key.keys().value(start).as_usize()
                    } else {
                        yield Err(invalid("null primary key")); return;
                    };
                    if index >= values.len() || !values.is_valid(index) {
                        yield Err(invalid("invalid primary-key dictionary value")); return;
                    }
                    let mut end = start + 1;
                    while end < record.num_rows() {
                        let next_index = if primary_key.keys().is_valid(end) {
                            primary_key.keys().value(end).as_usize()
                        } else {
                            yield Err(invalid("null primary key")); return;
                        };
                        if next_index >= values.len() || !values.is_valid(next_index) {
                            yield Err(invalid("invalid primary-key dictionary value")); return;
                        }
                        if super::primary_key_at(primary_key, values, end)
                            == super::primary_key_at(primary_key, values, start)
                        {
                            end += 1;
                        } else {
                            break;
                        }
                    }
                    let offsets = values.value_offsets();
                    let value_start = offsets[index].as_usize();
                    let value_len = offsets[index + 1].as_usize() - value_start;
                    let key = values.values().slice_with_length(value_start, value_len);
                    let begun = Instant::now();
                    let current = match chunk(&record, start, end, key, &output_schema, &fields) {
                        Ok(chunk) => chunk, Err(error) => { yield Err(error); return; }
                    };
                    metrics.inc_convert_batch_cost(begun.elapsed());
                    if let Some(old) = pending.as_ref()
                        && ordering == SeriesOrdering::KeyThenTime
                        && old.primary_key.as_slice() != current.primary_key.as_slice()
                        && current.primary_key.as_slice() <= old.primary_key.as_slice()
                    {
                        yield Err(invalid("raw series primary keys are not strictly increasing")); return;
                    }
                    if let Some(old) = pending.as_ref()
                        && old.primary_key.as_slice() == current.primary_key.as_slice()
                        && current.time.timestamp(0).zip(old.time.timestamp(old.time.len() - 1))
                            .is_some_and(|(next, previous)| next <= previous)
                    {
                        yield Err(invalid("raw timestamps are not strictly increasing")); return;
                    }
                    // Legacy flat producers declare `KeyThenTime`; the ordered-key comparison
                    // rejects a key that reopens after a package has completed it.
                    if let Some(mut old) = pending.take() {
                        if old.primary_key.as_slice() != current.primary_key.as_slice() {
                            old.is_last = true;
                        }
                        yield Ok(old);
                    }
                    pending = Some(current);
                    start = end;
                }
            }
            if let Some(package_last_key) = group_last_package_key
                && pending.as_ref().is_some_and(|last| last.primary_key.as_slice() <= package_last_key.as_slice())
                && let Some(mut last) = pending.take()
            {
                last.is_last = true;
                yield Ok(last);
            }
        }
        if let Some(error) = upstream_error { yield Err(BoxedError::new(error)); return; }
        if let Some(mut last) = pending { last.is_last = true; yield Ok(last); }
    };
    Box::pin(RawStream {
        schema,
        label_source,
        label_decode_spec,
        ordering,
        inner: Box::pin(stream),
    })
}

/// Wraps deferred preparation while exposing the scan contract before polling.
pub(crate) fn from_stream(
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    ordering: SeriesOrdering,
    inner: BoxStream<'static, Result<RawSeriesChunk, BoxedError>>,
) -> SendableRawSeriesStream {
    Box::pin(RawStream {
        schema,
        label_decode_spec: label_decode_spec(&label_source),
        label_source,
        ordering,
        inner,
    })
}
