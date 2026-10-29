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
//! [`SeriesOrdering::Contiguous`] and [`SeriesOrdering::KeyThenTime`] are accepted for raw frames and
//! only `Contiguous` for evaluated ones; `Interleaved` is rejected before any poll.
//!
//! An evaluated frame is the same physical family: [`pack_evaluated_series`] and
//! [`unpack_evaluated_series`] carry computed values under complete labels and grid positions, so a
//! compute consumer needs neither a storage key nor a scan binding to read them.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
use datafusion_common::DataFusionError;
use datatypes::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datatypes::error::InvalidSeriesSnafu;
use datatypes::series::frame::{
    FrameContent, FrameLabelMode, LABEL_DECODE_VERSION, LabelDecodeColumn, LabelDecodeEncoding,
    LabelDecodeSpec, LabelDecodeTag, SeriesFrameBuilder, SeriesFrameCursor, SeriesFrameDeclaration,
    SeriesFrameDecoder, series_frame_label_decode, series_frame_schema,
};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{Series, SeriesSchema};
use datatypes::series::{SeriesLabels, SeriesOrdering};
use futures_util::Stream;
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_engine::{
    NO_BOUND_LABEL_SOURCE, RawLabelSource, RawSeriesStream, SendableRawSeriesStream,
};

#[cfg(test)]
mod tests;

const ORDERING_KEY: &str = "greptime.series.ordering";

/// A stream of evaluated series chunks and their finality.
///
/// The pair is what an evaluated producer has and an evaluated consumer needs: the values on their
/// absolute grid positions, and whether this chunk closed the chain it belongs to. It is deliberately
/// not a third runtime chunk type - `Series` is the runtime value, and finality is the one fact a
/// chunk has to carry besides it.
pub type SendableEvaluatedSeriesStream =
    Pin<Box<dyn Stream<Item = Result<(Series, bool), BoxedError>> + Send>>;

/// Builds the canonical raw frame schema with the producer's label binding and ordering.
///
/// Ordering records a producer guarantee; it is not inferred from frame contents. `Interleaved`
/// has no wire representation and is rejected before the schema is assembled. The label binding
/// determines both the label mode and, for a primary-key binding, the self-describing label decode
/// description written into the schema, so a frame can never declare a label layout it does not
/// describe.
pub fn raw_series_frame_schema_with_ordering(
    schema: &SeriesSchema,
    label_source: &RawLabelSource,
    ordering: SeriesOrdering,
) -> datafusion_common::Result<ArrowSchemaRef> {
    if schema.eval_grid.is_some() {
        return Err(invalid(
            "a raw frame cannot carry a schema with an evaluation grid",
        ));
    }
    frame_schema_with(
        schema,
        label_mode(label_source),
        label_decode_spec(label_source).as_ref(),
        ordering,
    )
}

/// Assembles a declared frame schema from an explicit label mode and decode description.
///
/// The mode and description pair is checked by [`series_frame_schema`], so a key mode without a
/// description and an explicit mode with one are both rejected here instead of reaching the wire.
fn frame_schema_with(
    schema: &SeriesSchema,
    mode: FrameLabelMode,
    decode: Option<&LabelDecodeSpec>,
    ordering: SeriesOrdering,
) -> datafusion_common::Result<ArrowSchemaRef> {
    // An evaluated frame has no storage key to be ordered by, so the only claim it can honour is
    // that each chain is contiguous. Writing a stronger one would produce a frame the reader refuses
    // and a plan that fails where it could have been refused.
    if schema.eval_grid.is_some() {
        ensure_contiguous_ordering(ordering)?;
    }
    let ordering = match ordering {
        SeriesOrdering::Contiguous => "contiguous",
        SeriesOrdering::KeyThenTime => "read_key_then_time",
        SeriesOrdering::Interleaved => {
            return Err(invalid(
                "interleaved series ordering has no wire representation",
            ));
        }
    };
    let schema = series_frame_schema(schema, mode, decode).map_err(codec_error)?;
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

/// Reads the producer ordering declared by a complete v2 frame schema.
///
/// Ordering is mandatory stream metadata; it is never inferred from frame contents.
pub fn series_frame_ordering(schema: &ArrowSchema) -> datafusion_common::Result<SeriesOrdering> {
    match schema.metadata().get(ORDERING_KEY).map(String::as_str) {
        Some("contiguous") => Ok(SeriesOrdering::Contiguous),
        Some("read_key_then_time") => Ok(SeriesOrdering::KeyThenTime),
        None | Some(_) => Err(invalid("missing or unknown series ordering")),
    }
}

/// Packs a storage raw-series stream into DataFusion record batches incrementally.
///
/// Only [`SeriesOrdering::Contiguous`] and [`SeriesOrdering::KeyThenTime`] streams are accepted; an
/// `Interleaved` declaration is rejected before the first poll, including for an empty stream. Rows
/// accumulate across chunks into batches bounded by the builder's row and estimated payload limits;
/// one legal oversized row may be emitted alone, and these limits are a work bound, not a
/// resident-memory cap. A ready batch is yielded without polling the source again. Producers must
/// keep each series' chunks contiguous. Validation checks adjacent chunk conditions but does not
/// certify nonadjacent recurrence for `Contiguous` ordering.
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
    let labels = declared_label_mode(&decode);
    // The declared frame schema carries the producer ordering and the label decode description;
    // every ready batch is published under it, including when no row is ever produced.
    let frame_schema = frame_schema_with(&schema, labels, decode.as_ref(), ordering)?;
    let builder = SeriesFrameBuilder::try_new(
        &schema,
        labels,
        decode.as_ref(),
        SeriesFrameBuilder::DEFAULT_MAX_ROWS,
        SeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )
    .map_err(codec_error)?;
    Ok(Box::pin(PackStream {
        source: Some(source),
        schema,
        labels,
        batches: PackBatches::new(builder, frame_schema),
        ordering,
        validator: SequenceValidator::new(ordering),
    }))
}

/// Packs a stream of evaluated chunks into DataFusion record batches incrementally.
///
/// An evaluated frame names its series by their complete labels and addresses time on the evaluation
/// grid `schema` declares, so there is nothing to decode and no storage key to carry: the frame
/// declares `explicit` identity and `grid_spans` time, and the grid is written once instead of
/// repeating timestamps on every row.
///
/// Only [`SeriesOrdering::Contiguous`] is declared, because that is the one ordering this layer can
/// promise and check: each chunk chain is contiguous with itself. A caller that has reason to claim
/// [`SeriesOrdering::KeyThenTime`] about computed values would have to justify it, and a consumer
/// must never infer it from the presence of labels.
///
/// Bounds, backpressure, and error handling are the raw path's: rows accumulate into batches bounded
/// by the builder's row and estimated payload limits, a ready batch is yielded before the source is
/// polled again, and a `Pending` source still delivers what is buffered.
pub fn pack_evaluated_series(
    source: SendableEvaluatedSeriesStream,
    schema: Arc<SeriesSchema>,
) -> datafusion_common::Result<DfSendableRecordBatchStream> {
    if schema.eval_grid.is_none() {
        return Err(invalid(
            "an evaluated frame needs a schema with an evaluation grid",
        ));
    }
    let frame_schema = frame_schema_with(
        &schema,
        FrameLabelMode::Explicit,
        None,
        SeriesOrdering::Contiguous,
    )?;
    let builder = SeriesFrameBuilder::try_new(
        &schema,
        FrameLabelMode::Explicit,
        None,
        SeriesFrameBuilder::DEFAULT_MAX_ROWS,
        SeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )
    .map_err(codec_error)?;
    Ok(Box::pin(PackEvaluatedStream {
        source: Some(source),
        schema,
        batches: PackBatches::new(builder, frame_schema),
        validator: EvaluatedSequenceValidator::new(),
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
/// mode - a key frame must declare one and an explicit frame must not, since explicit labels are
/// already complete. The declared fields and the empty batch are then decoded once, so a stream that
/// could not be decoded is rejected before it is returned.
///
/// A frame that declares evaluated content is refused here, before anything is read: a raw consumer
/// reads timestamps rather than grid positions, so the two contents are not interchangeable.
///
/// `expected` is a check, never an input. When it is given it must name the same logical fields in
/// the same order and the same evaluation grid as the frame declares - which for a raw frame is that
/// both sides carry none - and a mismatch is an error. The decoder and the returned stream's schema
/// are built from the frame either way, so a caller cannot supply a field layout the frame does not
/// declare.
pub fn unpack_raw_series(
    source: DfSendableRecordBatchStream,
    expected: Option<Arc<SeriesSchema>>,
) -> datafusion_common::Result<SendableRawSeriesStream> {
    let frame_schema = source.schema();
    let declaration = SeriesFrameDeclaration::parse(frame_schema.as_ref()).map_err(codec_error)?;
    if declaration.content() != FrameContent::Raw {
        return Err(invalid("expected a raw frame, found an evaluated one"));
    }
    let labels = declaration.labels();
    let declared = declaration.label_decode().cloned();
    let schema = Arc::new(declaration.series().clone());
    if let Some(expected) = &expected {
        ensure_declared_schema(&schema, expected)?;
    }
    let ordering = series_frame_ordering(frame_schema.as_ref())?;
    ensure_supported_ordering(ordering)?;
    let decoder =
        SeriesFrameDecoder::try_new(&schema, labels, declared.as_ref()).map_err(codec_error)?;
    // Validate even empty streams' declared schema and layout before returning the stream.
    let empty = DfRecordBatch::new_empty(frame_schema.clone());
    decoder.decode_raw(&empty).map_err(codec_error)?;
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

/// Unpacks DataFusion batches declared by [`pack_evaluated_series`].
///
/// The frame declares its grid, its field list, and its label mode, so the returned stream carries
/// the [`SeriesSchema`] the rows are addressed on and needs no planning-side copy of it. The grid is
/// the frame's own declaration: the timestamps stay unmaterialized until a consumer asks for one.
///
/// A frame that declares raw samples, keyed identity, or an ordering other than `contiguous` is
/// refused before the stream is returned, as is a grid the declaration check cannot make sense of.
/// `expected` is a check and never an input, exactly as on the raw path: when given it must name the
/// same fields and the same grid the frame declares.
pub fn unpack_evaluated_series(
    source: DfSendableRecordBatchStream,
    expected: Option<Arc<SeriesSchema>>,
) -> datafusion_common::Result<SendableEvaluatedSeriesStream> {
    let frame_schema = source.schema();
    let declaration = SeriesFrameDeclaration::parse(frame_schema.as_ref()).map_err(codec_error)?;
    if declaration.content() != FrameContent::Evaluated {
        return Err(invalid("expected an evaluated frame, found a raw one"));
    }
    ensure_contiguous_ordering(series_frame_ordering(frame_schema.as_ref())?)?;
    let schema = Arc::new(declaration.series().clone());
    if let Some(expected) = &expected {
        ensure_declared_schema(&schema, expected)?;
    }
    let decoder = SeriesFrameDecoder::try_new(&schema, FrameLabelMode::Explicit, None)
        .map_err(codec_error)?;
    let empty = DfRecordBatch::new_empty(frame_schema.clone());
    decoder.decode_evaluated(&empty).map_err(codec_error)?;
    Ok(Box::pin(UnpackEvaluatedStream {
        source: Some(source),
        frame_schema,
        validator: EvaluatedSequenceValidator::new(),
        pending: Vec::new().into_iter(),
        decoder,
    }))
}

/// Rejects an `expected` series schema the frame does not declare.
///
/// Only the two things a caller can get wrong are checked, because those are the only two things
/// [`SeriesSchema`] holds: the logical fields and their order, and the evaluation grid. A raw frame
/// carries no grid and an evaluated one carries exactly its own, so the grid comparison is also what
/// keeps a raw expectation from decoding an evaluated frame.
fn ensure_declared_schema(
    declared: &SeriesSchema,
    expected: &SeriesSchema,
) -> datafusion_common::Result<()> {
    if expected.fields.len() != declared.fields.len()
        || expected
            .fields
            .iter()
            .zip(declared.fields.iter())
            .any(|(expected, declared)| expected.name != declared.name)
    {
        return Err(invalid(
            "frame fields do not match the expected series schema",
        ));
    }
    match (&expected.eval_grid, &declared.eval_grid) {
        (None, None) => Ok(()),
        (Some(expected), Some(declared)) if expected.spec() == declared.spec() => Ok(()),
        _ => Err(invalid(
            "frame evaluation grid does not match the expected series schema",
        )),
    }
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
/// The comparison is plain equality. The writer canonicalizes the description (see
/// [`LabelDecodeSpec::canonicalize`]), so a description that reaches a reader already has one
/// spelling and equality says everything there is to say about it.
pub fn ensure_frame_matches_label_source(
    frame: &ArrowSchema,
    label_source: &RawLabelSource,
) -> datafusion_common::Result<()> {
    let declared = series_frame_label_decode(frame).map_err(codec_error)?;
    if declared != label_decode_spec(label_source) {
        return Err(invalid(
            "raw frame label decode description does not match the supplied binding",
        ));
    }
    Ok(())
}

fn label_mode(source: &RawLabelSource) -> FrameLabelMode {
    match source {
        RawLabelSource::PrimaryKey { .. } => FrameLabelMode::Key,
        RawLabelSource::Explicit => FrameLabelMode::Explicit,
    }
}

/// Returns the label mode a frame declares: a key frame describes its key, an explicit frame
/// carries complete labels and declares no description.
fn declared_label_mode(declared: &Option<LabelDecodeSpec>) -> FrameLabelMode {
    match declared {
        Some(_) => FrameLabelMode::Key,
        None => FrameLabelMode::Explicit,
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

/// Rejects an evaluated frame that claims more order than its producer promised.
///
/// Computed values have no storage key to be ordered by, so `read_key_then_time` is not a claim this
/// layer can honour; `Interleaved` is not consumable at all. Treating either as `contiguous` would
/// silently weaken a guarantee a consumer may rely on.
fn ensure_contiguous_ordering(ordering: SeriesOrdering) -> datafusion_common::Result<()> {
    match ordering {
        SeriesOrdering::Contiguous => Ok(()),
        SeriesOrdering::KeyThenTime => Err(invalid(
            "an evaluated frame cannot claim key-then-time ordering",
        )),
        SeriesOrdering::Interleaved => Err(invalid("interleaved series ordering is not supported")),
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

/// Cross-chunk validation state for raw chains. The producer must keep each series' chunks
/// contiguous; this validator checks adjacent chunks only and never sees `Interleaved` streams. It
/// retains only the previous complete owned key (not an Arrow buffer), timestamp, metric, and
/// finality.
struct SequenceValidator {
    ordering: SeriesOrdering,
    previous_key: Option<Vec<u8>>,
    previous_timestamp: Option<i64>,
    previous_metric: Option<SeriesLabels>,
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

/// Cross-chunk validation state for evaluated chains.
///
/// This is a **chain-local** validator. It checks exactly three things: that a chunk does not change
/// identity while its chain is open, that a chain closes exactly once, and that the positions of an
/// open chain keep increasing across adjacent chunks. It keeps at most the one open chain, so it
/// never certifies that a complete identity does not recur later in the stream; that global
/// non-recurrence is a producer contract enforced where identities are completed (the repartition
/// owner), not here.
///
/// `is_last` closes one chunk chain and nothing more. It is not the end of a label set: a range
/// function or a label rewrite can make two input series show the same complete labels, and each of
/// them closes itself, so a closed chain may be followed by another chain whose labels happen to be
/// equal. Only the still-open chain is remembered, which is what keeps such a contribution from being
/// refused as a re-opened series while still refusing a chain that changes identity before it closes.
struct EvaluatedSequenceValidator {
    /// The labels of the open chain and the last grid position it carried.
    open: Option<(SeriesLabels, usize)>,
}

impl EvaluatedSequenceValidator {
    fn new() -> Self {
        Self { open: None }
    }

    fn finish(&self) -> datafusion_common::Result<()> {
        if self.open.is_some() {
            return Err(invalid(
                "evaluated series stream ended before a chain closed",
            ));
        }
        Ok(())
    }

    fn validate(&mut self, series: &Series, is_last: bool) -> datafusion_common::Result<()> {
        let (Some(first), Some(last)) = (series.time.first_position(), series.time.last_position())
        else {
            // No carried position: only a closing chunk may be empty, and it closes the chain it
            // belongs to without advancing it.
            if !is_last {
                return Err(invalid(
                    "an evaluated chunk without positions must close its series",
                ));
            }
            match &self.open {
                Some((labels, _)) if labels == &series.metric => self.open = None,
                Some(_) => {
                    return Err(invalid(
                        "evaluated series identity changed before its chain closed",
                    ));
                }
                None => {}
            }
            return Ok(());
        };
        if let Some((labels, previous)) = &self.open {
            if labels != &series.metric {
                return Err(invalid(
                    "evaluated series identity changed before its chain closed",
                ));
            }
            if first <= *previous {
                return Err(invalid(
                    "evaluated series positions are not increasing across chunks",
                ));
            }
        }
        self.open = (!is_last).then(|| (series.metric.clone(), last));
        Ok(())
    }
}

/// The batching half of a pack stream: the builder, the cursor installed in it, and the declared
/// schema every ready batch is published under.
struct PackBatches {
    frame_schema: ArrowSchemaRef,
    builder: Option<SeriesFrameBuilder>,
    cursor: Option<SeriesFrameCursor>,
}

impl PackBatches {
    fn new(builder: SeriesFrameBuilder, frame_schema: ArrowSchemaRef) -> Self {
        Self {
            frame_schema,
            builder: Some(builder),
            cursor: None,
        }
    }

    /// Installs the cursor of the chunk just read, replacing whatever was left of the previous one.
    fn install(&mut self, cursor: SeriesFrameCursor) {
        self.cursor = Some(cursor);
    }

    /// Advances the installed cursor until it yields one ready batch or is exhausted, dropping an
    /// exhausted cursor. `None` means no batch is ready, so the source may be polled again.
    fn advance(&mut self) -> datatypes::error::Result<Option<DfRecordBatch>> {
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
    fn flush(&mut self) -> datatypes::error::Result<Option<DfRecordBatch>> {
        match self.builder.as_mut() {
            Some(builder) => builder.flush(),
            None => Ok(None),
        }
    }

    /// Releases the buffered owners, leaving the stream terminal; batches already yielded are
    /// unaffected.
    fn release(&mut self) {
        self.cursor = None;
        self.builder = None;
    }

    /// Publishes one ready batch under the declared frame schema, which is the builder's own layout
    /// plus the producer's ordering metadata. A batch whose fields cannot be declared releases the
    /// stream instead of yielding rows with an undeclared layout.
    fn declare(
        &mut self,
        batch: DfRecordBatch,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        match DfRecordBatch::try_new(self.frame_schema.clone(), batch.columns().to_vec()) {
            Ok(batch) => Poll::Ready(Some(Ok(batch))),
            Err(error) => {
                self.release();
                Poll::Ready(Some(Err(DataFusionError::from(error))))
            }
        }
    }
}

struct PackStream {
    source: Option<SendableRawSeriesStream>,
    schema: Arc<SeriesSchema>,
    labels: FrameLabelMode,
    batches: PackBatches,
    ordering: SeriesOrdering,
    validator: SequenceValidator,
}

impl PackStream {
    /// Releases the source and drops every buffered owner, leaving the stream terminal.
    fn finish(&mut self) {
        self.source = None;
        self.batches.release();
        self.validator = SequenceValidator::new(self.ordering);
    }
}

impl Stream for PackStream {
    type Item = datafusion_common::Result<DfRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            match this.batches.advance() {
                Ok(Some(batch)) => return this.batches.declare(batch),
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
                    let flushed = this.batches.flush();
                    return match flushed {
                        Ok(Some(batch)) => this.batches.declare(batch),
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
                    let flushed = this.batches.flush();
                    // The declared schema is read before the release below, so the last batch is
                    // published under the same schema as every batch before it.
                    let declared = this.batches.frame_schema.clone();
                    this.finish();
                    return match flushed {
                        Ok(Some(batch)) => {
                            match DfRecordBatch::try_new(declared, batch.columns().to_vec()) {
                                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                                Err(error) => Poll::Ready(Some(Err(DataFusionError::from(error)))),
                            }
                        }
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
                    match SeriesFrameCursor::try_new_raw(&this.schema, this.labels, chunk) {
                        Ok(cursor) => this.batches.install(cursor),
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
    fn schema(&self) -> ArrowSchemaRef {
        self.batches.frame_schema.clone()
    }
}

/// The pack stream of [`pack_evaluated_series`].
struct PackEvaluatedStream {
    source: Option<SendableEvaluatedSeriesStream>,
    schema: Arc<SeriesSchema>,
    batches: PackBatches,
    validator: EvaluatedSequenceValidator,
}

impl PackEvaluatedStream {
    /// Releases the source and drops every buffered owner, leaving the stream terminal.
    fn finish(&mut self) {
        self.source = None;
        self.batches.release();
        self.validator = EvaluatedSequenceValidator::new();
    }
}

impl Stream for PackEvaluatedStream {
    type Item = datafusion_common::Result<DfRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            match this.batches.advance() {
                Ok(Some(batch)) => return this.batches.declare(batch),
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
                    let flushed = this.batches.flush();
                    return match flushed {
                        Ok(Some(batch)) => this.batches.declare(batch),
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
                    let flushed = this.batches.flush();
                    // The declared schema is read before the release below, so the last batch is
                    // published under the same schema as every batch before it.
                    let declared = this.batches.frame_schema.clone();
                    this.finish();
                    return match flushed {
                        Ok(Some(batch)) => {
                            match DfRecordBatch::try_new(declared, batch.columns().to_vec()) {
                                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                                Err(error) => Poll::Ready(Some(Err(DataFusionError::from(error)))),
                            }
                        }
                        Ok(None) => Poll::Ready(None),
                        Err(error) => Poll::Ready(Some(Err(codec_error(error)))),
                    };
                }
                Poll::Ready(Some(Err(error))) => {
                    this.finish();
                    return Poll::Ready(Some(Err(upstream_error(error))));
                }
                Poll::Ready(Some(Ok((series, is_last)))) => {
                    if let Err(error) = this.validator.validate(&series, is_last) {
                        this.finish();
                        return Poll::Ready(Some(Err(error)));
                    }
                    match SeriesFrameCursor::try_new_evaluated(&this.schema, series, is_last) {
                        Ok(cursor) => this.batches.install(cursor),
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

impl datafusion::physical_plan::RecordBatchStream for PackEvaluatedStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.batches.frame_schema.clone()
    }
}

struct UnpackStream {
    source: Option<DfSendableRecordBatchStream>,
    schema: Arc<SeriesSchema>,
    frame_schema: ArrowSchemaRef,
    /// The description parsed from the frame schema; the only input decoding needs.
    label_decode_spec: Option<LabelDecodeSpec>,
    ordering: SeriesOrdering,
    validator: SequenceValidator,
    pending: std::vec::IntoIter<RawSeriesChunk>,
    decoder: SeriesFrameDecoder,
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
                    return Poll::Ready(result.err().map(|error| Err(unpack_error(error))));
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
                            .decode_raw(&batch)
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
                            return Poll::Ready(Some(Err(unpack_error(error))));
                        }
                    }
                }
            }
        }
    }
}

/// The unpack stream of [`unpack_evaluated_series`].
struct UnpackEvaluatedStream {
    source: Option<DfSendableRecordBatchStream>,
    frame_schema: ArrowSchemaRef,
    validator: EvaluatedSequenceValidator,
    pending: std::vec::IntoIter<(Series, bool)>,
    decoder: SeriesFrameDecoder,
}

impl UnpackEvaluatedStream {
    fn finish(&mut self) {
        self.source = None;
        self.pending = Vec::new().into_iter();
        self.validator = EvaluatedSequenceValidator::new();
    }
}

impl Stream for UnpackEvaluatedStream {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
                    return Poll::Ready(result.err().map(|error| Err(unpack_error(error))));
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
                            return Err(invalid("evaluated frame schema mismatch"));
                        }
                        this.decoder
                            .decode_evaluated(&batch)
                            .map_err(codec_error)
                            .and_then(|chunks| {
                                for (series, is_last) in &chunks {
                                    this.validator.validate(series, *is_last)?;
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
                            return Poll::Ready(Some(Err(unpack_error(error))));
                        }
                    }
                }
            }
        }
    }
}

/// How many empty batches one poll reads before yielding, so a stream of them cannot hold the task.
const MAX_EMPTY_BATCHES_PER_POLL: usize = 32;

/// Wraps a decode or validation error as the boxed error an unpack stream reports.
fn unpack_error(error: DataFusionError) -> BoxedError {
    BoxedError::new(crate::error::Error::GeneralDataFusion {
        error,
        location: snafu::Location::default(),
    })
}
