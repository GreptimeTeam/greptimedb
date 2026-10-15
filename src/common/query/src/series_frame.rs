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

//! Streaming boundary between finite raw-series chunks and DataFusion record batches.
//!
//! Schema fields identify codec layout and ordinal payload fields; additional application metadata
//! is permitted when stable across the stream. The caller-provided [`RawLabelSource`] is the label
//! binding, not authenticated by the frame schema or field provenance. This adapter preserves scan
//! candidates only: it is not a residual filter, authorization check, remote transport, or
//! compaction boundary.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
use datafusion_common::DataFusionError;
use datatypes::error::InvalidSeriesSnafu;
use datatypes::series::MetricName;
use datatypes::series::frame::{
    RawLabelMode, pack_raw_series_chunk, raw_series_frame_schema, unpack_raw_series_frame,
};
use datatypes::series::raw::{RawSeriesChunk, RawSeriesOrdering};
use datatypes::series::samples::SeriesSchema;
use futures_util::Stream;
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

#[cfg(test)]
mod tests;

/// Packs a storage raw-series stream into DataFusion record batches incrementally.
pub fn pack_raw_series(
    source: SendableRawSeriesStream,
) -> datafusion_common::Result<DfSendableRecordBatchStream> {
    let schema = source.schema().clone();
    let label_source = source.label_source().clone();
    let ordering = source.ordering();
    let mode = label_mode(&label_source);
    let frame_schema = raw_series_frame_schema(&schema, mode).map_err(codec_error)?;
    Ok(Box::pin(PackStream {
        source: Some(source),
        schema,
        frame_schema,
        mode,
        validator: SequenceValidator::new(ordering),
        pending: Vec::<DfRecordBatch>::new().into_iter(),
    }))
}

/// Unpacks DataFusion batches using the caller's explicit scan binding and ordering.
///
/// The supplied binding is retained unchanged and is not validated as field provenance.
pub fn unpack_raw_series(
    source: DfSendableRecordBatchStream,
    expected: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    ordering: RawSeriesOrdering,
) -> datafusion_common::Result<SendableRawSeriesStream> {
    let mode = label_mode(&label_source);
    let frame_schema = source.schema();
    // Validate even empty streams' declared schema and layout before returning the stream.
    let empty = DfRecordBatch::new_empty(frame_schema.clone());
    unpack_raw_series_frame(&expected, mode, &empty).map_err(codec_error)?;
    Ok(Box::pin(UnpackStream {
        source: Some(source),
        schema: expected,
        frame_schema,
        label_source,
        ordering,
        validator: SequenceValidator::new(ordering),
        pending: Vec::new().into_iter(),
    }))
}

fn label_mode(source: &RawLabelSource) -> RawLabelMode {
    match source {
        RawLabelSource::PrimaryKey { .. } => RawLabelMode::Key,
        RawLabelSource::Explicit => RawLabelMode::Explicit,
    }
}

fn codec_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(BoxedError::new(error)))
}

fn upstream_error(error: BoxedError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn invalid(reason: impl Into<String>) -> DataFusionError {
    DataFusionError::External(Box::new(BoxedError::new(
        InvalidSeriesSnafu {
            reason: reason.into(),
        }
        .build(),
    )))
}

/// Cross-chunk validation state. Contiguous ordering retains complete owned keys for recurrence
/// detection, so memory is O(total distinct complete-key bytes + distinct keys), plus current-key
/// and metric state, until stream termination; there is no eviction or cap. This owns key bytes,
/// never Arrow buffers that might pin larger payloads.
struct SequenceValidator {
    ordering: RawSeriesOrdering,
    previous_key: Option<Vec<u8>>,
    previous_timestamp: Option<i64>,
    previous_metric: Option<MetricName>,
    previous_is_last: bool,
    closed: HashSet<Vec<u8>>,
}

impl SequenceValidator {
    fn new(ordering: RawSeriesOrdering) -> Self {
        Self {
            ordering,
            previous_key: None,
            previous_timestamp: None,
            previous_metric: None,
            previous_is_last: false,
            closed: HashSet::new(),
        }
    }

    fn finish(&self) -> datafusion_common::Result<()> {
        if self.previous_key.is_some() && !self.previous_is_last {
            return Err(invalid("raw series stream ended before key was final"));
        }
        Ok(())
    }

    fn validate(&mut self, chunk: &RawSeriesChunk) -> datafusion_common::Result<()> {
        let key = chunk.read_key.as_slice();
        let timestamp = chunk
            .time
            .timestamp(chunk.time.len() - 1)
            .ok_or_else(|| invalid("empty raw series chunk"))?;
        if let Some(previous) = &self.previous_key {
            match previous.as_slice().cmp(key) {
                std::cmp::Ordering::Equal => {
                    if self.previous_is_last {
                        return Err(invalid("raw series key recurred after final chunk"));
                    }
                    if self.previous_metric != chunk.metric {
                        return Err(invalid("raw series metric changed within key"));
                    }
                    let first = chunk
                        .time
                        .timestamp(0)
                        .ok_or_else(|| invalid("empty raw series chunk"))?;
                    if self.previous_timestamp.is_some_and(|last| first <= last) {
                        return Err(invalid(
                            "raw series timestamps are not increasing across chunks",
                        ));
                    }
                }
                _ => {
                    if !self.previous_is_last {
                        return Err(invalid("raw series key changed before final chunk"));
                    }
                    if self.ordering == RawSeriesOrdering::ReadKeyThenTime
                        && previous.as_slice() > key
                    {
                        return Err(invalid("raw series keys are not ordered"));
                    }
                    if self.ordering == RawSeriesOrdering::Contiguous && self.closed.contains(key) {
                        return Err(invalid("raw series key recurred after its contiguous run"));
                    }
                }
            }
        }
        let owned_key = key.to_vec();
        if chunk.is_last && self.ordering == RawSeriesOrdering::Contiguous {
            self.closed.insert(owned_key.clone());
        }
        self.previous_key = Some(owned_key);
        self.previous_timestamp = Some(timestamp);
        self.previous_metric = chunk.metric.clone();
        self.previous_is_last = chunk.is_last;
        Ok(())
    }
}

struct PackStream {
    source: Option<SendableRawSeriesStream>,
    schema: Arc<SeriesSchema>,
    frame_schema: datatypes::arrow::datatypes::SchemaRef,
    mode: RawLabelMode,
    validator: SequenceValidator,
    pending: std::vec::IntoIter<DfRecordBatch>,
}

impl PackStream {
    fn finish(&mut self) {
        self.source = None;
        self.pending = Vec::new().into_iter();
        self.validator = SequenceValidator::new(self.validator.ordering);
    }
}

impl Stream for PackStream {
    type Item = datafusion_common::Result<DfRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(batch) = this.pending.next() {
                return Poll::Ready(Some(Ok(batch)));
            }
            let polled = match this.source.as_mut() {
                Some(source) => source.as_mut().poll_next(cx),
                None => return Poll::Ready(None),
            };
            match polled {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    let result = this.validator.finish();
                    this.finish();
                    return Poll::Ready(result.err().map(Err));
                }
                Poll::Ready(Some(Err(error))) => {
                    this.finish();
                    return Poll::Ready(Some(Err(upstream_error(error))));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    let result = (|| {
                        let batches = pack_raw_series_chunk(&this.schema, this.mode, &chunk)
                            .map_err(codec_error)?;
                        this.validator.validate(&chunk)?;
                        Ok(batches)
                    })();
                    match result {
                        Ok(batches) => this.pending = batches.into_iter(),
                        Err(error) => {
                            this.finish();
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                }
            }
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for PackStream {
    fn schema(&self) -> datatypes::arrow::datatypes::SchemaRef {
        self.frame_schema.clone()
    }
}

struct UnpackStream {
    source: Option<DfSendableRecordBatchStream>,
    schema: Arc<SeriesSchema>,
    frame_schema: datatypes::arrow::datatypes::SchemaRef,
    label_source: RawLabelSource,
    ordering: RawSeriesOrdering,
    validator: SequenceValidator,
    pending: std::vec::IntoIter<RawSeriesChunk>,
}

impl RawSeriesStream for UnpackStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }

    fn ordering(&self) -> RawSeriesOrdering {
        self.ordering
    }
}

impl UnpackStream {
    fn finish(&mut self) {
        self.source = None;
        self.pending = Vec::new().into_iter();
        self.validator = SequenceValidator::new(self.ordering);
    }
}

impl Stream for UnpackStream {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(chunk) = this.pending.next() {
                return Poll::Ready(Some(Ok(chunk)));
            }
            let polled = match this.source.as_mut() {
                Some(source) => source.as_mut().poll_next(cx),
                None => return Poll::Ready(None),
            };
            match polled {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    let result = this.validator.finish();
                    this.finish();
                    return Poll::Ready(result.err().map(|error| {
                        Err(BoxedError::new(crate::error::Error::GeneralDataFusion {
                            error,
                            location: snafu::Location::default(),
                        }))
                    }));
                }
                Poll::Ready(Some(Err(error))) => {
                    this.finish();
                    return Poll::Ready(Some(Err(BoxedError::new(
                        crate::error::Error::GeneralDataFusion {
                            error,
                            location: snafu::Location::default(),
                        },
                    ))));
                }
                Poll::Ready(Some(Ok(batch))) => {
                    let result = (|| {
                        if batch.schema() != this.frame_schema {
                            return Err(invalid("raw frame schema mismatch"));
                        }
                        unpack_raw_series_frame(
                            &this.schema,
                            label_mode(&this.label_source),
                            &batch,
                        )
                        .map_err(codec_error)
                        .and_then(|chunks| {
                            for chunk in &chunks {
                                this.validator.validate(chunk)?;
                            }
                            Ok(chunks)
                        })
                    })();
                    match result {
                        Ok(chunks) => this.pending = chunks.into_iter(),
                        Err(error) => {
                            this.finish();
                            return Poll::Ready(Some(Err(BoxedError::new(
                                crate::error::Error::GeneralDataFusion {
                                    error,
                                    location: snafu::Location::default(),
                                },
                            ))));
                        }
                    }
                }
            }
        }
    }
}
