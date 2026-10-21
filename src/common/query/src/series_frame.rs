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
//! declares its own label decode description, and the frame is the sole source of decode
//! information: [`unpack_raw_series`] reads the fields, the label mode, the description, and the
//! producer ordering from the received schema, keeps the description on the returned stream, and
//! therefore needs no [`RawLabelSource`]. A caller that holds a scan-bound identity — the physical
//! region key layout a decoded identity came from — compares that identity's declared label
//! decode layout with [`ensure_frame_matches_label_source`] instead of passing it as a decode
//! input; that comparison is equality of the description alone and proves nothing about region
//! identity or provenance, and neither side is authenticated as field provenance. This adapter
//! preserves
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
    raw_series_frame_series_schema,
};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SeriesSchema;
use datatypes::series::{MetricName, SeriesOrdering};
use futures_util::Stream;
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_engine::{
    NO_BOUND_LABEL_SOURCE, RawLabelSource, RawSeriesStream, SendableRawSeriesStream,
};

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
    frame_schema_with(
        schema,
        label_mode(label_source),
        label_decode_spec(label_source).as_ref(),
        ordering,
    )
}

/// Assembles a declared frame schema from an explicit label mode and decode description.
///
/// The mode and description pair is checked by
/// [`raw_series_frame_schema_with_label_decode`], so a key mode without a description and an
/// explicit mode with one are both rejected here instead of reaching the wire.
fn frame_schema_with(
    schema: &SeriesSchema,
    mode: RawLabelMode,
    decode: Option<&LabelDecodeSpec>,
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
    let schema =
        raw_series_frame_schema_with_label_decode(schema, mode, decode).map_err(codec_error)?;
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
/// description.
///
/// The result is canonical: its logical tags are in [`LabelDecodeSpec::canonicalize`] order, which
/// is what the writer serializes, so a derived description and a received one for the same labels
/// are the same value and the same text. Derivation cannot fail, because a physical type is copied
/// rather than translated; a description a reader cannot honour is rejected when it is read, and a
/// decoder rejects the tags it cannot bind.
pub fn label_decode_spec(source: &RawLabelSource) -> Option<LabelDecodeSpec> {
    let RawLabelSource::PrimaryKey {
        metric_name,
        physical_metadata,
        logical_tags,
    } = source
    else {
        return None;
    };
    let primary_key = physical_metadata
        .primary_key_columns()
        .map(|column| LabelDecodeColumn {
            column_id: column.column_id,
            name: column.column_schema.name.clone(),
            physical_type: column.column_schema.data_type.clone(),
        })
        .collect();
    let mut description = LabelDecodeSpec {
        version: LABEL_DECODE_VERSION,
        metric_name: metric_name.to_string(),
        encoding: match physical_metadata.primary_key_encoding {
            PrimaryKeyEncoding::Dense => LabelDecodeEncoding::Dense,
            PrimaryKeyEncoding::Sparse => LabelDecodeEncoding::Sparse,
        },
        primary_key,
        logical_tags: logical_tags
            .iter()
            .map(|tag| LabelDecodeTag {
                column_id: tag.column_id,
                name: tag.name.to_string(),
            })
            .collect(),
    };
    description.canonicalize();
    Some(description)
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
///
/// The stream's own declaration is the only decode input: a frame unpacked without a scan-bound
/// [`RawLabelSource`] round trips unchanged instead of being re-declared from a binding it does not
/// have, and nothing is derived from a binding here. The declaration also decides the frame's label
/// mode, so a chunk whose labels disagree with it is rejected when that chunk is coded.
pub fn pack_raw_series(
    source: SendableRawSeriesStream,
) -> datafusion_common::Result<DfSendableRecordBatchStream> {
    let ordering = source.ordering();
    ensure_supported_ordering(ordering)?;
    let schema = source.schema().clone();
    let decode = source.label_decode_spec().cloned();
    let mode = declared_label_mode(&decode);
    // The declared frame schema carries the producer ordering and the label decode description;
    // the shared builder's schema intentionally omits both, so every ready batch is declared under
    // this schema before it is yielded, including when no row is ever produced.
    let frame_schema = frame_schema_with(&schema, mode, decode.as_ref(), ordering)?;
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

/// Unpacks DataFusion batches declared by [`pack_raw_series`].
///
/// The frame is the sole source of decode information: the payload fields, the label mode, the
/// label decode description, and the producer ordering are all read from the stream's own schema,
/// so unpacking a frame a producer wrote needs no local copy of its binding. Only
/// [`SeriesOrdering::Contiguous`] and [`SeriesOrdering::KeyThenTime`] streams are accepted; an
/// `Interleaved` declaration is rejected before the stream is returned, including for an empty
/// stream. Producers must keep each series' chunks contiguous. Validation checks adjacent chunk
/// conditions but does not certify nonadjacent recurrence for `Contiguous` ordering.
///
/// Every frame is validated here, unconditionally. The declared layout must be this revision's, the
/// label decode description must parse, name a known version, and agree with the declared label
/// mode — a key frame must declare one and an explicit frame must not, since explicit labels are
/// already complete. The declared fields and the empty batch are then decoded once, so a stream that
/// could not be decoded is rejected before it is returned.
///
/// `expected` is a check, never an input. When it is given it must match the schema the frame
/// declares — the same logical fields in the same order, and no evaluation grid — and a mismatch is
/// an error. The decoder and the returned stream's schema are built from the frame either way, so a
/// caller cannot supply a field layout the frame does not declare.
pub fn unpack_raw_series(
    source: DfSendableRecordBatchStream,
    expected: Option<Arc<SeriesSchema>>,
) -> datafusion_common::Result<SendableRawSeriesStream> {
    let frame_schema = source.schema();
    let declared = raw_series_frame_label_decode(frame_schema.as_ref()).map_err(codec_error)?;
    let mode = declared_label_mode(&declared);
    let schema =
        Arc::new(raw_series_frame_series_schema(frame_schema.as_ref()).map_err(codec_error)?);
    if let Some(expected) = &expected {
        ensure_declared_schema(&schema, expected)?;
    }
    let ordering = raw_series_frame_ordering(frame_schema.as_ref())?;
    let decoder = RawSeriesFrameDecoder::try_new(&schema, mode).map_err(codec_error)?;
    // Validate even empty streams' declared schema and layout before returning the stream.
    let empty = DfRecordBatch::new_empty(frame_schema.clone());
    decoder.decode(&empty).map_err(codec_error)?;
    Ok(Box::pin(UnpackStream {
        source: Some(source),
        schema,
        frame_schema,
        label_decode_spec: declared,
        ordering,
        validator: SequenceValidator::new(ordering),
        pending: Vec::new().into_iter(),
        decoder,
    }))
}

/// Rejects an `expected` series schema the frame does not declare.
///
/// Only the two things a caller can get wrong are checked, because those are the only two things
/// [`SeriesSchema`] holds: the logical fields and their order, and an evaluation grid a raw frame
/// cannot carry.
fn ensure_declared_schema(
    declared: &SeriesSchema,
    expected: &SeriesSchema,
) -> datafusion_common::Result<()> {
    if expected.eval_grid.is_some() {
        return Err(invalid(
            "expected raw series schema declares an evaluation grid the frame cannot carry",
        ));
    }
    if expected.fields.len() != declared.fields.len()
        || expected
            .fields
            .iter()
            .zip(declared.fields.iter())
            .any(|(expected, declared)| expected.name != declared.name)
    {
        return Err(invalid(
            "raw frame fields do not match the expected series schema",
        ));
    }
    Ok(())
}

/// Checks that a frame declares the label decode layout a scan-bound binding describes.
///
/// This compares one thing: the description parsed from the frame's
/// `greptime.series.label_decode` metadata against the description derived from `label_source`, by
/// plain equality. A difference in the version, the metric name, the primary-key encoding, the
/// ordered key columns and their physical types, or the logical tags is a mismatch. Nothing else is
/// compared, so agreeing descriptions do **not** establish that these are the region's chunks: no
/// region id, table id, or provenance is part of either side, and two regions whose tags have the
/// same ids, names, and types produce equal descriptions. The check answers "does this frame describe
/// the label layout this binding expects", never "is this the region I asked for".
///
/// It belongs to the caller that holds both sides: a frame describes its own labels, so
/// [`unpack_raw_series`] decodes it without a binding, and a caller that received the frame from a
/// peer compares the declaration here instead of decoding under whichever side it trusts.
///
/// The comparison is plain equality. It used to canonicalize logical tag order on both sides, which
/// gave local comparisons one rule and the byte-exact remote comparison another; the writer now
/// canonicalizes instead (see [`LabelDecodeSpec::canonicalize`]), so a description that reaches a
/// reader already has one spelling and equality says everything there is to say about it.
pub fn ensure_frame_matches_label_source(
    frame: &ArrowSchema,
    label_source: &RawLabelSource,
) -> datafusion_common::Result<()> {
    let declared = raw_series_frame_label_decode(frame).map_err(codec_error)?;
    if declared != label_decode_spec(label_source) {
        return Err(invalid(
            "raw frame label decode description does not match the supplied binding",
        ));
    }
    Ok(())
}

fn label_mode(source: &RawLabelSource) -> RawLabelMode {
    match source {
        RawLabelSource::PrimaryKey { .. } => RawLabelMode::Key,
        RawLabelSource::Explicit => RawLabelMode::Explicit,
    }
}

/// Returns the label mode a frame declares: a key frame describes its key, an explicit frame
/// carries complete labels and declares no description.
fn declared_label_mode(declared: &Option<LabelDecodeSpec>) -> RawLabelMode {
    match declared {
        Some(_) => RawLabelMode::Key,
        None => RawLabelMode::Explicit,
    }
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
    /// The description parsed from the frame schema; the only input decoding needs.
    label_decode_spec: Option<LabelDecodeSpec>,
    ordering: SeriesOrdering,
    validator: SequenceValidator,
    pending: std::vec::IntoIter<RawSeriesChunk>,
    decoder: RawSeriesFrameDecoder,
}

impl RawSeriesStream for UnpackStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    /// Returns [`NO_BOUND_LABEL_SOURCE`]: a frame establishes its own label layout and carries no
    /// scan-bound identity, so decoding reads [`Self::label_decode_spec`] and never this. There is
    /// no physical region key layout to report and none is invented from the description, which
    /// deliberately carries no region metadata.
    fn label_source(&self) -> &RawLabelSource {
        &NO_BOUND_LABEL_SOURCE
    }

    /// Returns the label decode description the frame declared, which is what decoding reads; an
    /// explicit-label frame declares none.
    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        self.label_decode_spec.as_ref()
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
