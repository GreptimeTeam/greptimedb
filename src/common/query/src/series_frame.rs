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
//! Schema fields identify the ordered logical payload fields and codec layout; additional
//! application metadata is permitted when stable across the stream. A key-encoded frame also
//! declares its own label decode description, so a consumer can decode labels from the received
//! schema; the caller-provided [`RawLabelSource`] is carried as the scan-bound identity — the
//! physical region key layout a decoded identity came from — and must describe the same labels as
//! that description. Neither is authenticated as field provenance. This adapter preserves
//! scan candidates only; it is not a residual filter, authorization check, remote transport, or
//! compaction boundary. Only
//! [`SeriesOrdering::Contiguous`] and [`SeriesOrdering::KeyThenTime`] are accepted;
//! `Interleaved` is rejected before any poll.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
use datafusion_common::DataFusionError;
use datatypes::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datatypes::error::InvalidSeriesSnafu;
use datatypes::series::frame::{
    LABEL_DECODE_VERSION, LabelDecodeColumn, LabelDecodeEncoding, LabelDecodeSpec, LabelDecodeTag,
    RawLabelMode, RawSeriesChunkCursor, RawSeriesFrameBuilder, RawSeriesFrameDecoder,
    raw_series_frame_label_decode, raw_series_frame_schema_with_label_decode,
};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SeriesSchema;
use datatypes::series::{MetricName, SeriesOrdering};
use futures_util::Stream;
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

mod record_batches;

pub use record_batches::{InputMapping, record_batches_to_raw_series};

#[cfg(test)]
mod tests;

const ORDERING_KEY: &str = "greptime.series.ordering";

/// Builds the canonical raw frame schema with the producer's label binding and ordering.
///
/// Ordering records a producer guarantee; it is not inferred from frame contents. `Interleaved`
/// has no v1 wire representation and is rejected before the schema is assembled. The label binding
/// determines both the label mode and, for a primary-key binding, the self-describing label decode
/// description written into the schema, so a frame can never declare a label layout it does not
/// describe.
pub fn raw_series_frame_schema_with_ordering(
    schema: &SeriesSchema,
    label_source: &RawLabelSource,
    ordering: SeriesOrdering,
) -> datafusion_common::Result<ArrowSchemaRef> {
    let ordering = match ordering {
        SeriesOrdering::Contiguous => "contiguous",
        SeriesOrdering::KeyThenTime => "read_key_then_time",
        SeriesOrdering::Interleaved => {
            return Err(invalid(
                "interleaved raw series ordering has no wire representation",
            ));
        }
    };
    let decode = label_decode_spec(label_source);
    let schema = raw_series_frame_schema_with_label_decode(
        schema,
        label_mode(label_source),
        decode.as_ref(),
    )
    .map_err(codec_error)?;
    let mut metadata = schema.metadata().clone();
    metadata.insert(ORDERING_KEY.to_string(), ordering.to_string());
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        schema.fields().clone(),
        metadata,
    )))
}

/// Derives the label decode description a frame must carry for this binding.
///
/// A primary-key binding reduces to exactly what decoding reads: the metric name, the primary-key
/// encoding, the ordered key columns with their physical types, and the logical tags. Region
/// metadata guarantees the key list holds every tag column and nothing else, so the description
/// needs no per-column role. Explicit bindings already carry complete labels and declare no
/// description. Derivation cannot fail; a description a reader cannot honour is rejected when it is
/// read, and a decoder rejects the logical tags it cannot bind.
pub fn label_decode_spec(source: &RawLabelSource) -> Option<LabelDecodeSpec> {
    let RawLabelSource::PrimaryKey {
        metric_name,
        physical_metadata,
        logical_tags,
    } = source
    else {
        return None;
    };
    Some(LabelDecodeSpec {
        version: LABEL_DECODE_VERSION,
        metric_name: metric_name.to_string(),
        encoding: match physical_metadata.primary_key_encoding {
            PrimaryKeyEncoding::Dense => LabelDecodeEncoding::Dense,
            PrimaryKeyEncoding::Sparse => LabelDecodeEncoding::Sparse,
        },
        primary_key: physical_metadata
            .primary_key_columns()
            .map(|column| LabelDecodeColumn {
                column_id: column.column_id,
                name: column.column_schema.name.clone(),
                physical_type: column.column_schema.data_type.clone(),
            })
            .collect(),
        logical_tags: logical_tags
            .iter()
            .map(|tag| LabelDecodeTag {
                column_id: tag.column_id,
                name: tag.name.to_string(),
            })
            .collect(),
    })
}

/// Reads the declared producer ordering, conservatively treating legacy frames as contiguous.
///
/// Only the two orderings representable on the wire can be returned.
pub fn raw_series_frame_ordering(
    schema: &ArrowSchema,
) -> datafusion_common::Result<SeriesOrdering> {
    match schema.metadata().get(ORDERING_KEY).map(String::as_str) {
        None | Some("contiguous") => Ok(SeriesOrdering::Contiguous),
        Some("read_key_then_time") => Ok(SeriesOrdering::KeyThenTime),
        Some(_) => Err(invalid("unknown raw series ordering")),
    }
}

/// Packs a storage raw-series stream into DataFusion record batches incrementally.
///
/// Only [`SeriesOrdering::Contiguous`] and [`SeriesOrdering::KeyThenTime`] streams are
/// accepted; an `Interleaved` declaration is rejected before the first poll, including for an
/// empty stream. Rows accumulate across chunks into batches bounded by the builder's row and
/// estimated payload limits; one legal oversized row may be emitted alone, and these limits are a
/// work bound, not a resident-memory cap. A ready batch is yielded without polling the source
/// again. Producers must keep each series' chunks contiguous. Validation checks adjacent chunk
/// conditions but does not certify nonadjacent recurrence for `Contiguous` ordering.
pub fn pack_raw_series(
    source: SendableRawSeriesStream,
) -> datafusion_common::Result<DfSendableRecordBatchStream> {
    let ordering = source.ordering();
    ensure_supported_ordering(ordering)?;
    let schema = source.schema().clone();
    let label_source = source.label_source().clone();
    let mode = label_mode(&label_source);
    // The declared frame schema carries the producer ordering and the label decode description;
    // the shared builder's schema intentionally omits both, so every ready batch is declared under
    // this schema before it is yielded, including when no row is ever produced.
    let frame_schema = raw_series_frame_schema_with_ordering(&schema, &label_source, ordering)?;
    let builder = RawSeriesFrameBuilder::try_new(
        &schema,
        mode,
        RawSeriesFrameBuilder::DEFAULT_MAX_ROWS,
        RawSeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )
    .map_err(codec_error)?;
    Ok(Box::pin(PackStream {
        source: Some(source),
        schema,
        mode,
        frame_schema,
        ordering,
        validator: SequenceValidator::new(ordering),
        builder: Some(builder),
        cursor: None,
    }))
}

/// Unpacks DataFusion batches declared by [`pack_raw_series`], checking them against the binding.
///
/// Only [`SeriesOrdering::Contiguous`] and [`SeriesOrdering::KeyThenTime`] streams are
/// accepted; an `Interleaved` declaration is rejected before the stream is returned, including
/// for an empty stream. Producers must keep each series' chunks contiguous. Validation checks
/// adjacent chunk conditions but does not certify nonadjacent recurrence for `Contiguous`
/// ordering.
///
/// Decoding takes the label layout from the frame schema's own description, never from this
/// binding: a frame that omits the description for a key binding, or describes a different label
/// layout, is rejected instead of being decoded under either side silently. The binding is kept on
/// the returned stream because it establishes identity — the physical region key layout a decoded
/// identity came from — so a consumer can compare it before polling. It is not authenticated as
/// field provenance.
///
/// "A different label layout" is compared the way each part of the description allows. The ordered
/// `primary_key` columns are compared exactly as [`label_decode_spec`] writes them, because their
/// array position *is* the encoding position: the same columns in another order are a different key,
/// not the same key in another order. `logical_tags` are compared as an unordered set of tag
/// identities, because their order carries no meaning to a decoder; a description listing the same
/// tags in another order denotes the same labels and is accepted.
pub fn unpack_raw_series(
    source: DfSendableRecordBatchStream,
    expected: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    ordering: SeriesOrdering,
) -> datafusion_common::Result<SendableRawSeriesStream> {
    ensure_supported_ordering(ordering)?;
    let mode = label_mode(&label_source);
    let frame_schema = source.schema();
    let declared = raw_series_frame_label_decode(frame_schema.as_ref()).map_err(codec_error)?;
    if !same_label_layout(&declared, &label_decode_spec(&label_source)) {
        return Err(invalid(
            "raw frame label decode description does not match the supplied binding",
        ));
    }
    let decoder = RawSeriesFrameDecoder::try_new(&expected, mode).map_err(codec_error)?;
    // Validate even empty streams' declared schema and layout before returning the stream.
    let empty = DfRecordBatch::new_empty(frame_schema.clone());
    decoder.decode(&empty).map_err(codec_error)?;
    Ok(Box::pin(UnpackStream {
        source: Some(source),
        schema: expected,
        frame_schema,
        label_source,
        ordering,
        validator: SequenceValidator::new(ordering),
        pending: Vec::new().into_iter(),
        decoder,
    }))
}

fn label_mode(source: &RawLabelSource) -> RawLabelMode {
    match source {
        RawLabelSource::PrimaryKey { .. } => RawLabelMode::Key,
        RawLabelSource::Explicit => RawLabelMode::Explicit,
    }
}

/// Compares a frame's declared description with the one a binding derives.
///
/// The two are compared for structural equality after canonicalising the one part whose order
/// carries no meaning. `primary_key` is left exactly in place: for a dense key the array position
/// *is* the encoding position, so the same columns in another order are a different key layout and
/// must not compare equal. `logical_tags` are sorted, because a decoder binds them sorted and their
/// order is only what the producer happened to write.
fn same_label_layout(declared: &Option<LabelDecodeSpec>, bound: &Option<LabelDecodeSpec>) -> bool {
    match (declared, bound) {
        (None, None) => true,
        (Some(declared), Some(bound)) => canonical_layout(declared) == canonical_layout(bound),
        (Some(_), None) | (None, Some(_)) => false,
    }
}

/// Returns the description with its order-insensitive part in canonical order.
///
/// Sorting by `(column_id, name)` is a total order: a validated description names each tag column
/// and each tag name at most once. Comparing the canonical forms structurally keeps every other
/// field, including any added later, part of the equality check.
fn canonical_layout(spec: &LabelDecodeSpec) -> LabelDecodeSpec {
    let mut canonical = spec.clone();
    canonical
        .logical_tags
        .sort_unstable_by(|a, b| (a.column_id, &a.name).cmp(&(b.column_id, &b.name)));
    canonical
}

/// Rejects orderings this adapter cannot validate; a bad declaration is an error, not a fallback.
fn ensure_supported_ordering(ordering: SeriesOrdering) -> datafusion_common::Result<()> {
    match ordering {
        SeriesOrdering::Contiguous | SeriesOrdering::KeyThenTime => Ok(()),
        SeriesOrdering::Interleaved => {
            Err(invalid("interleaved raw series ordering is not supported"))
        }
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

/// Cross-chunk validation state. The producer must keep each series' chunks contiguous; this
/// validator checks adjacent chunks only and never sees `Interleaved` streams. It retains only
/// the previous complete owned key (not an Arrow buffer), timestamp, metric, and finality.
struct SequenceValidator {
    ordering: SeriesOrdering,
    previous_key: Option<Vec<u8>>,
    previous_timestamp: Option<i64>,
    previous_metric: Option<MetricName>,
    previous_is_last: bool,
}

impl SequenceValidator {
    fn new(ordering: SeriesOrdering) -> Self {
        Self {
            ordering,
            previous_key: None,
            previous_timestamp: None,
            previous_metric: None,
            previous_is_last: false,
        }
    }

    fn finish(&self) -> datafusion_common::Result<()> {
        if self.previous_key.is_some() && !self.previous_is_last {
            return Err(invalid("raw series stream ended before key was final"));
        }
        Ok(())
    }

    fn validate(&mut self, chunk: &RawSeriesChunk) -> datafusion_common::Result<()> {
        let key = chunk.primary_key.as_slice();
        let timestamp = chunk
            .time
            .len()
            .checked_sub(1)
            .and_then(|last| chunk.time.timestamp(last))
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
                    if self.ordering == SeriesOrdering::KeyThenTime && previous.as_slice() > key {
                        return Err(invalid("raw series keys are not ordered"));
                    }
                }
            }
        }
        self.previous_key = Some(key.to_vec());
        self.previous_timestamp = Some(timestamp);
        self.previous_metric = chunk.metric.clone();
        self.previous_is_last = chunk.is_last;
        Ok(())
    }
}

struct PackStream {
    source: Option<SendableRawSeriesStream>,
    schema: Arc<SeriesSchema>,
    mode: RawLabelMode,
    frame_schema: datatypes::arrow::datatypes::SchemaRef,
    ordering: SeriesOrdering,
    validator: SequenceValidator,
    builder: Option<RawSeriesFrameBuilder>,
    cursor: Option<RawSeriesChunkCursor>,
}

impl PackStream {
    /// Releases the source and drops every buffered owner, leaving the stream terminal; batches
    /// already yielded are unaffected.
    fn finish(&mut self) {
        self.source = None;
        self.cursor = None;
        self.builder = None;
        self.validator = SequenceValidator::new(self.ordering);
    }

    /// Advances the installed cursor until it yields one ready batch or is exhausted, dropping an
    /// exhausted cursor. `None` means no batch is ready, so the source may be polled again.
    fn advance_cursor(&mut self) -> datatypes::error::Result<Option<DfRecordBatch>> {
        let ready = match (self.cursor.as_mut(), self.builder.as_mut()) {
            (Some(cursor), Some(builder)) => cursor.append_to(builder)?,
            _ => return Ok(None),
        };
        match ready {
            Some(batch) => Ok(Some(batch)),
            None => {
                self.cursor = None;
                Ok(None)
            }
        }
    }

    /// Flushes the rows buffered so far, if any, so a pending source can still deliver data.
    fn flush_buffered(&mut self) -> datatypes::error::Result<Option<DfRecordBatch>> {
        match self.builder.as_mut() {
            Some(builder) => builder.flush(),
            None => Ok(None),
        }
    }

    /// Publishes one ready builder batch under the declared frame schema, which is the shared
    /// builder's schema plus the producer's ordering metadata. A batch whose fields cannot be
    /// declared releases the stream instead of yielding rows with an undeclared layout.
    fn declare_batch(
        &mut self,
        batch: DfRecordBatch,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        match batch.with_schema(self.frame_schema.clone()) {
            Ok(batch) => Poll::Ready(Some(Ok(batch))),
            Err(error) => {
                self.finish();
                Poll::Ready(Some(Err(DataFusionError::from(error))))
            }
        }
    }
}

impl Stream for PackStream {
    type Item = datafusion_common::Result<DfRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            match this.advance_cursor() {
                Ok(Some(batch)) => return this.declare_batch(batch),
                Ok(None) => {}
                Err(error) => {
                    this.finish();
                    return Poll::Ready(Some(Err(codec_error(error))));
                }
            }
            let polled = match this.source.as_mut() {
                Some(source) => source.as_mut().poll_next(cx),
                None => return Poll::Ready(None),
            };
            match polled {
                Poll::Pending => {
                    let flushed = this.flush_buffered();
                    return match flushed {
                        Ok(Some(batch)) => this.declare_batch(batch),
                        Ok(None) => Poll::Pending,
                        Err(error) => {
                            this.finish();
                            Poll::Ready(Some(Err(codec_error(error))))
                        }
                    };
                }
                Poll::Ready(None) => {
                    if let Err(error) = this.validator.finish() {
                        this.finish();
                        return Poll::Ready(Some(Err(error)));
                    }
                    let flushed = this.flush_buffered();
                    this.finish();
                    return match flushed {
                        Ok(Some(batch)) => this.declare_batch(batch),
                        Ok(None) => Poll::Ready(None),
                        Err(error) => Poll::Ready(Some(Err(codec_error(error)))),
                    };
                }
                Poll::Ready(Some(Err(error))) => {
                    this.finish();
                    return Poll::Ready(Some(Err(upstream_error(error))));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    if let Err(error) = this.validator.validate(&chunk) {
                        this.finish();
                        return Poll::Ready(Some(Err(error)));
                    }
                    match RawSeriesChunkCursor::try_new(&this.schema, this.mode, chunk) {
                        Ok(cursor) => this.cursor = Some(cursor),
                        Err(error) => {
                            this.finish();
                            return Poll::Ready(Some(Err(codec_error(error))));
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
    ordering: SeriesOrdering,
    validator: SequenceValidator,
    pending: std::vec::IntoIter<RawSeriesChunk>,
    decoder: RawSeriesFrameDecoder,
}

impl RawSeriesStream for UnpackStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }

    fn ordering(&self) -> SeriesOrdering {
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
        const MAX_EMPTY_BATCHES_PER_POLL: usize = 32;
        let this = &mut *self;
        let mut empty_batches = 0;
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
                        this.decoder
                            .decode(&batch)
                            .map_err(codec_error)
                            .and_then(|chunks| {
                                for chunk in &chunks {
                                    this.validator.validate(chunk)?;
                                }
                                Ok(chunks)
                            })
                    })();
                    match result {
                        Ok(chunks) => {
                            if chunks.is_empty() {
                                empty_batches += 1;
                                if empty_batches == MAX_EMPTY_BATCHES_PER_POLL {
                                    cx.waker().wake_by_ref();
                                    return Poll::Pending;
                                }
                            } else {
                                this.pending = chunks.into_iter();
                            }
                        }
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
