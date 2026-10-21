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

//! Finite Arrow batches for raw series chunks. Field names preserve the logical series schema in order.
//!
//! Packing accumulates bounded multi-row batches: [`RawSeriesChunkCursor`] walks one validated
//! chunk and [`RawSeriesFrameBuilder`] emits rows directly as Arrow arrays once a row or estimated
//! byte bound is reached. Decoding reuses one prepared schema per stream through
//! [`RawSeriesFrameDecoder`]. Column layout, metadata, and wire strings stay on the frozen `v1`
//! contract; a key-encoded frame additionally declares a versioned [`LabelDecodeSpec`] so its
//! labels can be decoded from the received schema alone.
//!
//! # Label decode description versioning
//!
//! The description is one `greptime.series.label_decode` metadata entry, and remote plans compare
//! `greptime.series.*` metadata **as exact text, byte for byte**: a frontend and a datanode that
//! write different text for the same labels disagree about the schema, and the query fails instead
//! of falling back. The rules that follow from that are:
//!
//! - A reader rejects a version it does not implement, rather than guessing. This module writes and
//!   reads [`LABEL_DECODE_VERSION`] only.
//! - [`LABEL_DECODE_VERSION`] covers the whole text, including each key column's
//!   [`LabelDecodeColumn::physical_type`]. That field is a [`ConcreteDataType`], so the text embeds
//!   [`ConcreteDataType`]'s serde shape: it is not spelled here, and a change in that type's
//!   serialization does alter this text. What keeps such a change from going unnoticed is the
//!   frozen fixture `tests/compatibility/wire/raw_series_label_decode/v1_physical_types.json`,
//!   which records the text of every datatype variant and fails the crate's tests when any of them
//!   is serialized differently.
//! - Any change to the shape of this text — a renamed or added field, a different
//!   [`ConcreteDataType`] spelling, a different `logical_tags` order — is a **breaking** change at
//!   the same version, because it makes two nodes of different builds disagree. Such a change must
//!   bump [`LABEL_DECODE_VERSION`] and update that fixture with it.
//! - The text is canonical: [`LabelDecodeSpec::canonicalize`] fixes the only part whose order
//!   carries no meaning, so equal labels always write equal bytes.
//!
//! Deployment rule: during a rolling upgrade a frame may carry a version the receiving node does not
//! implement, and a byte-for-byte schema comparison may fail for that reason alone. The intended
//! behaviour is that such a mismatch disables the native series path at planning time and falls back
//! to the generic path, instead of failing the query at execution.
//!
//! TODO(narthex): implement that planning-time fallback. No planner reads
//! `raw_series_frame_label_decode` to decide support today, so a version or text mismatch currently
//! surfaces as an execution-time error from the raw-frame decode or the remote schema comparison.

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, StringArray,
    StructArray, UInt32Array,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use serde::{Deserialize, Serialize};

use crate::data_type::ConcreteDataType;
use crate::error::{InvalidSeriesSnafu, Result};
use crate::series::histogram::{HistogramBatch, fields as histogram_fields};
use crate::series::raw::{RawSeriesChunk, RawTimestamps};
use crate::series::samples::{
    DenseSamplesRef, FieldSpec, HistogramSamples, Samples, SeriesFields, SeriesSchema,
};
use crate::series::{Label, MetricName, invalid_series};

const LAYOUT: &str = "greptime.series.layout";
const CONTENT: &str = "greptime.series.content";
const LABELS: &str = "greptime.series.labels";
const TIME: &str = "greptime.series.time";
const GRID: &str = "greptime.series.grid";
/// Metadata key of the versioned label decode description carried by key-encoded frames.
const LABEL_DECODE: &str = "greptime.series.label_decode";

/// Version of the label decode description this revision writes and reads.
pub const LABEL_DECODE_VERSION: u32 = 1;

/// Whether a raw frame carries only the complete key or explicit metric labels as well.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawLabelMode {
    /// Labels remain encoded solely in `read_key`.
    Key,
    /// Complete metric name and ordinary labels are present in the frame.
    Explicit,
}

/// Primary-key encoding of the labels a key-encoded frame carries.
///
/// This mirrors the storage contract's `PrimaryKeyEncoding` without making this crate depend on
/// it; Dense and Sparse keys use different decode layouts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LabelDecodeEncoding {
    /// Dense key: every primary-key field is encoded in order, with no ids on the wire.
    Dense,
    /// Sparse key: `(column id, value)` pairs, so the layout only frames the field stream.
    Sparse,
}

/// One ordered key column needed to decode key-encoded labels.
///
/// Only tag columns reach this list: region metadata rejects a primary key that holds the time
/// index or a non-`Tag` column, and it requires the primary key to name every tag column. The
/// array's position is therefore the encoding position, with nothing to disambiguate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelDecodeColumn {
    /// Physical column id of this key column.
    pub column_id: u32,
    /// Physical column name; a logical tag of the same id must repeat it.
    pub name: String,
    /// Physical data type of the encoded value, in [`ConcreteDataType`]'s own serde form. Dense
    /// keys carry no types on the wire, so the codec is reconstructed from this exact ordered type
    /// list.
    pub physical_type: ConcreteDataType,
}

/// One logical tag name bound to a primary-key column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelDecodeTag {
    /// Physical column id this logical tag reads.
    pub column_id: u32,
    /// Logical tag name exposed to queries.
    pub name: String,
}

/// Versioned, self-contained description of how to decode key-encoded labels.
///
/// It carries exactly what decoding a primary-key label source reads and nothing else: no region
/// identity, no field columns, no time index. A consumer that receives the frame schema can
/// therefore reconstruct an equivalent decoder without a scan-side binding, while a description
/// that is missing, of an unknown version, or structurally inconsistent is a hard error instead of
/// a silent fallback.
///
/// It is JSON-encoded in one schema metadata entry, always through
/// [`raw_series_frame_schema_with_label_decode`], so both the producing and the expecting side of a
/// `greptime.series.*` exact metadata comparison see byte-identical text. The text is
/// self-describing but not a general interchange format: readers reject unknown fields here only
/// at this structure's own levels, while [`LabelDecodeColumn::physical_type`] is decoded by
/// [`ConcreteDataType`]'s own representation and is therefore exactly as stable as that type's
/// serde. The frozen text this revision writes is pinned by
/// `tests/compatibility/wire/raw_series_label_decode/v1_physical_types.json`; see the module
/// documentation for what a change to it means.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelDecodeSpec {
    /// Description version; see [`LABEL_DECODE_VERSION`].
    pub version: u32,
    /// Metric name attached to every decoded label set; may be empty.
    pub metric_name: String,
    /// Primary-key encoding that determines the decode layout.
    pub encoding: LabelDecodeEncoding,
    /// Every tag column of the primary key, in primary-key encoding order. This is exactly the
    /// set of encoded key fields: region metadata forbids a primary key from naming the time
    /// index or a field column, and requires it to name every tag column, so no time-index or
    /// field entry can appear here. The order is significant and is compared as written: a dense
    /// decode reads field *n* at position *n*, so the same columns in another order describe a
    /// different key, not the same key in another order.
    pub primary_key: Vec<LabelDecodeColumn>,
    /// Logical tags, as bound by the producer, in [`LabelDecodeSpec::canonicalize`] order.
    ///
    /// The order carries no meaning: the decoder sorts the tags by name when it binds them
    /// (`src/mito-codec/src/labels.rs:235`), so a decoded label set does not depend on it. It is
    /// nevertheless fixed rather than left to the caller, because this description is compared as
    /// exact text across nodes: two producers with the same tags in another order must write
    /// byte-identical text, or one side rejects the other as a schema mismatch.
    ///
    /// The semantic checks on these tags (reserved and internal names, name equality with the bound
    /// column, membership in `primary_key`) are the decoder's, matching what a typed binding gets.
    pub logical_tags: Vec<LabelDecodeTag>,
}

impl LabelDecodeSpec {
    /// Puts the order-insensitive part of the description in its canonical order.
    ///
    /// [`raw_series_frame_schema_with_label_decode`] applies this to a copy before serializing, and
    /// so does the description a binding derives (in `common_query::series_frame`), so both sides of
    /// a text comparison produce the same bytes and a reader can compare descriptions structurally.
    /// `primary_key` is deliberately untouched: its order is the encoding order.
    ///
    /// The writer canonicalizes a **copy** on purpose. A caller's binding keeps its own
    /// `logical_tags` order, and that order is meaningful elsewhere: `src/table/src/table/scan.rs`
    /// compares two bindings' tag lists positionally (`lt.iter().zip(rt.iter())`), so reordering the
    /// caller's list here would make two bindings for the same labels compare unequal. Only the
    /// serialized text is canonical; nothing outside it is rewritten.
    ///
    /// Sorting by `(column_id, name)` is a total order for a validated description, which names
    /// each tag column and each tag name at most once, so the canonical form is unique.
    pub fn canonicalize(&mut self) {
        self.logical_tags
            .sort_unstable_by(|a, b| (a.column_id, &a.name).cmp(&(b.column_id, &b.name)));
    }

    /// Checks the version and the structural invariants a decoder relies on.
    ///
    /// Semantic binding checks (reserved names, internal columns, name equality against the bound
    /// column) stay in the decoder so both a typed binding and a decoded description fail with the
    /// same errors.
    pub fn validate(&self) -> Result<()> {
        if self.version != LABEL_DECODE_VERSION {
            return Err(err(format!(
                "unsupported label decode description version {}",
                self.version
            )));
        }
        let mut column_ids = HashSet::with_capacity(self.primary_key.len());
        for column in &self.primary_key {
            if !column_ids.insert(column.column_id) {
                return Err(err(format!(
                    "duplicate primary-key column id {} in label decode description",
                    column.column_id
                )));
            }
        }
        let mut tag_ids = HashSet::with_capacity(self.logical_tags.len());
        let mut tag_names = HashSet::with_capacity(self.logical_tags.len());
        for tag in &self.logical_tags {
            if !tag_ids.insert(tag.column_id) {
                return Err(err(format!(
                    "duplicate logical tag column id {} in label decode description",
                    tag.column_id
                )));
            }
            if !tag_names.insert(tag.name.as_str()) {
                return Err(err(format!(
                    "duplicate logical tag {} in label decode description",
                    tag.name
                )));
            }
            if !column_ids.contains(&tag.column_id) {
                return Err(err(format!(
                    "logical tag {} is not a primary-key column of the label decode description",
                    tag.name
                )));
            }
        }
        Ok(())
    }
}

/// Builds the typed schema for a raw frame, preserving logical field names and order.
///
/// This is the column layout shared with the frame builder and decoder; it deliberately carries no
/// label decode description. A key-encoded stream must be declared through
/// [`raw_series_frame_schema_with_label_decode`], which refuses such a layout without one.
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
        .map(|field| Field::new(field.name.as_ref(), payload.clone(), false))
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

/// Builds the typed schema for a raw frame together with its label decode description.
///
/// The description is written only for [`RawLabelMode::Key`], where labels stay encoded in
/// `read_key`; explicit-label frames carry complete labels and must not declare one. Both
/// directions are enforced here so a frame cannot claim a label layout it does not describe.
pub fn raw_series_frame_schema_with_label_decode(
    schema: &SeriesSchema,
    mode: RawLabelMode,
    decode: Option<&LabelDecodeSpec>,
) -> Result<SchemaRef> {
    match (mode, decode) {
        (RawLabelMode::Key, None) => {
            return Err(err(
                "key-encoded raw frames must carry a label decode description",
            ));
        }
        (RawLabelMode::Explicit, Some(_)) => {
            return Err(err("explicit raw frames carry no label decode description"));
        }
        (RawLabelMode::Key, Some(_)) | (RawLabelMode::Explicit, None) => {}
    }
    let layout = raw_series_frame_schema(schema, mode)?;
    let Some(decode) = decode else {
        return Ok(layout);
    };
    // Writing is faithful: the description records what the producer declares, and nothing is
    // rewritten or dropped. The one exception is the order of `logical_tags`, which carries no
    // meaning to a decoder and which this text nevertheless has to fix so two producers of the same
    // labels write the same bytes; a copy is canonicalized and the caller's value is untouched. A
    // description this revision cannot honour is rejected when it is read, which is the only
    // direction a consumer can rely on.
    let mut canonical = decode.clone();
    canonical.canonicalize();
    let encoded = serde_json::to_string(&canonical)
        .map_err(|e| err(format!("cannot encode label decode description: {e}")))?;
    let mut metadata = layout.metadata().clone();
    metadata.insert(LABEL_DECODE.to_string(), encoded);
    Ok(Arc::new(Schema::new_with_metadata(
        layout.fields().clone(),
        metadata,
    )))
}

/// Recovers the label decode description a raw frame schema declares.
///
/// Returns `None` for an explicit-label frame, which must not declare one. A key-encoded frame must
/// declare one, so a missing description is an error rather than a reason to reuse a caller-side
/// binding. A present description is parsed strictly: an unknown version, a missing or unknown
/// field at this structure's own levels, a type mismatch, or a description whose logical tags do
/// not name listed key columns is an error, never a silent default. Only the description's own
/// levels reject unknown fields; [`LabelDecodeColumn::physical_type`] is a
/// [`ConcreteDataType`] and accepts whatever that type's own serde accepts.
pub fn raw_series_frame_label_decode(schema: &Schema) -> Result<Option<LabelDecodeSpec>> {
    let mode = match schema.metadata().get(LABELS).map(String::as_str) {
        Some("key") => RawLabelMode::Key,
        Some("explicit") => RawLabelMode::Explicit,
        _ => return Err(err("raw frame schema declares no known label mode")),
    };
    let decode = match schema.metadata().get(LABEL_DECODE) {
        None => None,
        Some(encoded) => {
            let decode: LabelDecodeSpec = serde_json::from_str(encoded)
                .map_err(|e| err(format!("malformed label decode description: {e}")))?;
            decode.validate()?;
            Some(decode)
        }
    };
    match (mode, &decode) {
        (RawLabelMode::Key, None) => Err(err(
            "key-encoded raw frame declares no label decode description",
        )),
        (RawLabelMode::Explicit, Some(_)) => Err(err(
            "explicit raw frame declares a label decode description",
        )),
        (RawLabelMode::Key, Some(_)) | (RawLabelMode::Explicit, None) => Ok(decode),
    }
}

/// Reconstructs the series schema a raw frame declares.
///
/// A raw frame names its payload fields by name only: every member of the `fields` struct carries
/// the same payload type ([`raw_series_frame_schema`]), and [`SeriesSchema`] holds nothing else per
/// field. The reconstruction is therefore exact rather than a projection — the declared names in
/// declared order, and no evaluation grid, which a raw frame cannot carry.
///
/// The reserved layout, content, and time metadata are checked here together with the absence of an
/// evaluation grid, so a frame this revision cannot read is rejected instead of being silently
/// interpreted as the current layout.
pub fn raw_series_frame_series_schema(schema: &Schema) -> Result<SeriesSchema> {
    for (key, expected) in [(LAYOUT, "v1"), (CONTENT, "raw"), (TIME, "timestamps")] {
        if schema.metadata().get(key).map(String::as_str) != Some(expected) {
            return Err(err(format!("raw frame declares no known {key} value")));
        }
    }
    if schema
        .metadata()
        .keys()
        .any(|key| key == GRID || key.starts_with("greptime.series.grid."))
    {
        return Err(err("raw series schema cannot have an evaluation grid"));
    }
    let fields = schema
        .fields()
        .iter()
        .find(|field| field.name() == "fields")
        .ok_or_else(|| err("raw frame declares no fields column"))?;
    let DataType::Struct(members) = fields.data_type() else {
        return Err(err("raw frame fields column is not a struct"));
    };
    if members.is_empty() {
        return Err(err("raw series schema must have fields"));
    }
    Ok(SeriesSchema {
        fields: members
            .iter()
            .map(|member| FieldSpec {
                name: Arc::from(member.name().as_str()),
            })
            .collect(),
        eval_grid: None,
    })
}

/// Bounded multi-row builder for finite raw frame batches.
///
/// Rows accumulate until the row limit or the estimated byte limit is reached; the next
/// [`RawSeriesChunkCursor::append_to`] call then emits them as one batch. `max_bytes` is stored as
/// the effective group cap `min(requested, i32::MAX)`, and each row carries a measure that
/// dominates every cumulative Arrow offset domain, so any admitted group can be encoded. The
/// measure is an internal work estimate, not an exact IPC size and not a resident-memory cap: one
/// individually representable row that exceeds the requested cap is still accepted and emitted
/// alone, because that slice cannot be split into smaller legal rows.
pub struct RawSeriesFrameBuilder {
    schema: SeriesSchema,
    frame_schema: SchemaRef,
    mode: RawLabelMode,
    max_rows: usize,
    max_bytes: usize,
    rows: Vec<RawFrameRow>,
    bytes: usize,
}

impl RawSeriesFrameBuilder {
    /// Default maximum rows in one packed batch when no explicit bound is needed.
    pub const DEFAULT_MAX_ROWS: usize = 256;
    /// Default maximum estimated encoded bytes in one packed batch when no explicit bound is
    /// needed.
    pub const DEFAULT_MAX_BYTES: usize = 1024 * 1024;

    /// Creates an empty builder for the given schema, label mode, and nonzero bounds.
    ///
    /// The frame schema is built once here and reused for every emitted batch.
    pub fn try_new(
        schema: &SeriesSchema,
        mode: RawLabelMode,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self> {
        if max_rows == 0 || max_bytes == 0 {
            return invalid_series("raw series frame limits must be nonzero");
        }
        Ok(Self {
            schema: schema.clone(),
            frame_schema: raw_series_frame_schema(schema, mode)?,
            mode,
            max_rows,
            max_bytes: max_bytes.min(i32::MAX as usize),
            rows: Vec::new(),
            bytes: 0,
        })
    }

    /// The frame schema of every batch this builder emits.
    pub fn schema(&self) -> &SchemaRef {
        &self.frame_schema
    }

    /// Emits the accumulated rows as one batch, or `None` when nothing is buffered.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows.is_empty() {
            return Ok(None);
        }
        let batch = encode_rows(&self.frame_schema, self.mode, &self.rows)?;
        self.rows.clear();
        self.bytes = 0;
        Ok(Some(batch))
    }

    /// Whether the buffered rows reached either bound.
    fn is_full(&self) -> bool {
        self.rows.len() >= self.max_rows || self.bytes >= self.max_bytes
    }

    /// Whether `row` may join the buffered rows without crossing either bound.
    ///
    /// An empty builder accepts any individually measured row; a group total that would overflow
    /// or exceed the cap flushes the buffered rows before this row is consumed, never dropping or
    /// splitting the row.
    fn accepts(&self, row: &RawFrameRow) -> bool {
        self.rows.is_empty()
            || (self.bytes < self.max_bytes
                && self.rows.len() < self.max_rows
                && self
                    .bytes
                    .checked_add(row.bytes)
                    .is_some_and(|total| total <= self.max_bytes))
    }

    /// Buffers one individually measured row.
    fn push(&mut self, row: RawFrameRow) -> Result<()> {
        self.bytes = add_checked(self.bytes, row.bytes)?;
        self.rows.push(row);
        Ok(())
    }
}

/// One validated chunk walked segment by segment into a bounded builder.
pub struct RawSeriesChunkCursor {
    schema: SeriesSchema,
    mode: RawLabelMode,
    chunk: RawSeriesChunk,
    segments: Vec<usize>,
    offsets: Vec<usize>,
    pos: usize,
    pending: Option<RawFrameRow>,
}

impl RawSeriesChunkCursor {
    /// Validates one public chunk's shape and label mode once, taking ownership of it.
    pub fn try_new(
        schema: &SeriesSchema,
        mode: RawLabelMode,
        chunk: RawSeriesChunk,
    ) -> Result<Self> {
        let RawSeriesChunk {
            primary_key,
            metric,
            time,
            fields,
            is_last,
        } = chunk;
        let chunk = RawSeriesChunk::try_new(schema, primary_key, metric, time, fields, is_last)?;
        match (mode, &chunk.metric) {
            (RawLabelMode::Key, None) | (RawLabelMode::Explicit, Some(_)) => {}
            _ => return invalid_series("raw chunk label mode does not match frame mode"),
        }
        let segments = vec![0usize; chunk.fields.len()];
        let offsets = vec![0usize; chunk.fields.len()];
        Ok(Self {
            schema: schema.clone(),
            mode,
            chunk,
            segments,
            offsets,
            pos: 0,
            pending: None,
        })
    }

    /// Appends rows to the builder until one batch is ready or this chunk is exhausted.
    ///
    /// `None` means the cursor is exhausted; calling again returns `None` without touching the
    /// builder. The single pending row is measured once by [`Self::next_row`] and retried after the
    /// buffered rows are emitted.
    pub fn append_to(
        &mut self,
        builder: &mut RawSeriesFrameBuilder,
    ) -> Result<Option<RecordBatch>> {
        self.compatible(builder)?;
        loop {
            let row = match self.pending.take() {
                Some(row) => row,
                None => match self.next_row()? {
                    Some(row) => {
                        self.pending = Some(row);
                        continue;
                    }
                    None => return Ok(None),
                },
            };
            if !builder.accepts(&row) {
                // Reinstate the row before flushing so a flush error cannot consume it.
                self.pending = Some(row);
                let batch = builder
                    .flush()?
                    .ok_or_else(|| err("raw frame builder rejected a row without buffered rows"))?;
                return Ok(Some(batch));
            }
            builder.push(row)?;
            if builder.is_full()
                && let Some(batch) = builder.flush()?
            {
                return Ok(Some(batch));
            }
        }
    }

    /// The cursor and builder must describe the same fields and label mode.
    fn compatible(&self, builder: &RawSeriesFrameBuilder) -> Result<()> {
        let same_fields = self.schema.fields.len() == builder.schema.fields.len()
            && self
                .schema
                .fields
                .iter()
                .zip(builder.schema.fields.iter())
                .all(|(left, right)| left.name == right.name);
        if self.mode != builder.mode
            || self.schema.eval_grid.is_some() != builder.schema.eval_grid.is_some()
            || !same_fields
        {
            return invalid_series("raw frame cursor and builder schema mismatch");
        }
        Ok(())
    }

    /// Builds and measures the next union-of-segment row, or `None` when the chunk is exhausted.
    ///
    /// The chunk fields are walked three times so no per-row temporary vectors are allocated; the
    /// offsets advance only after the row measure succeeded.
    fn next_row(&mut self) -> Result<Option<RawFrameRow>> {
        if self.pos >= self.chunk.time.len() {
            return Ok(None);
        }
        let time_axis = self.chunk.time.as_buffer();
        let mut take = time_axis.len() - self.pos;
        for (index, field) in self.chunk.fields.iter().enumerate() {
            let part = segment(field, self.segments[index])?;
            take = take.min(part.len().saturating_sub(self.offsets[index]));
        }
        if take == 0 {
            return invalid_series("empty sample segment");
        }
        let mut samples = Vec::with_capacity(self.chunk.fields.len());
        for (index, field) in self.chunk.fields.iter().enumerate() {
            let part = segment(field, self.segments[index])?;
            samples.push(part.slice(self.offsets[index], take)?);
        }
        let mut row = RawFrameRow {
            primary_key: self.chunk.primary_key.clone(),
            metric: self.chunk.metric.clone(),
            is_last: self.chunk.is_last && self.pos + take == self.chunk.time.len(),
            times: time_axis.slice(self.pos, take),
            samples,
            bytes: 0,
        };
        row.bytes = measure_row(&row)?;
        self.pos += take;
        for (index, field) in self.chunk.fields.iter().enumerate() {
            let length = segment(field, self.segments[index])?.len();
            self.offsets[index] += take;
            if self.offsets[index] == length {
                self.segments[index] += 1;
                self.offsets[index] = 0;
            }
        }
        Ok(Some(row))
    }
}

/// The next dense segment of one field, or an error when the walk left the segment list.
fn segment(field: &Samples, index: usize) -> Result<&Samples> {
    field
        .segments()
        .get(index)
        .ok_or_else(|| err("empty sample segment"))
}

/// Packs one raw chunk, retaining the `Vec<RecordBatch>` compatibility signature.
///
/// The chunk packs into bounded multi-row batches; callers that need streaming behavior should
/// drive [`RawSeriesChunkCursor`] and [`RawSeriesFrameBuilder`] directly instead of collecting.
pub fn pack_raw_series_chunk(
    schema: &SeriesSchema,
    mode: RawLabelMode,
    chunk: &RawSeriesChunk,
) -> Result<Vec<RecordBatch>> {
    let mut builder = RawSeriesFrameBuilder::try_new(
        schema,
        mode,
        RawSeriesFrameBuilder::DEFAULT_MAX_ROWS,
        RawSeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )?;
    let mut cursor = RawSeriesChunkCursor::try_new(schema, mode, chunk.clone())?;
    let mut batches = Vec::new();
    while let Some(batch) = cursor.append_to(&mut builder)? {
        batches.push(batch);
    }
    if let Some(batch) = builder.flush()? {
        batches.push(batch);
    }
    Ok(batches)
}

/// One pending output row: a complete key with one dense sample slice per field and its checked
/// encoded-size measure.
struct RawFrameRow {
    primary_key: Buffer,
    metric: Option<MetricName>,
    is_last: bool,
    times: ScalarBuffer<i64>,
    samples: Vec<Samples>,
    bytes: usize,
}

/// Per-row encoded-size measure, computed once per row when the cursor produces it.
///
/// The measure dominates every cumulative offset domain the encoder writes: binary/UTF-8 child
/// bytes, list element counts, bitmap bytes, and the visible extent of each histogram table list.
/// A group total of `min(max_bytes, i32::MAX)` therefore guarantees that all Arrow offsets and
/// lengths stay representable, without tracking a separate offset counter. This is a bound used
/// for batching work, not the exact encoded size.
fn measure_row(row: &RawFrameRow) -> Result<usize> {
    checked_offset(row.primary_key.len())?;
    checked_offset(row.times.len())?;
    let mut bytes = add_checked(ROW_CHARGE, row.primary_key.len())?;
    bytes = add_checked(bytes, mul_checked(row.times.len(), 8)?)?;
    for samples in &row.samples {
        bytes = add_checked(bytes, FIELD_CHARGE)?;
        match samples
            .dense()
            .ok_or_else(|| err("sample segment is not dense"))?
        {
            DenseSamplesRef::Float(values) => {
                bytes = add_checked(bytes, float_charge(values.len())?)?
            }
            DenseSamplesRef::Histogram(values) => {
                bytes = add_checked(bytes, histogram_charge(values)?)?
            }
            DenseSamplesRef::Mixed(values) => {
                bytes = add_checked(bytes, float_charge(values.floats().len())?)?;
                bytes = add_checked(bytes, histogram_charge(values.histograms())?)?;
            }
        }
    }
    if let Some(metric) = &row.metric {
        if let Some(name) = metric.name() {
            checked_offset(name.len())?;
            bytes = add_checked(bytes, name.len())?;
        }
        checked_offset(metric.tags().len())?;
        bytes = add_checked(bytes, mul_checked(metric.tags().len(), 16)?)?;
        let mut names = 0usize;
        let mut values = 0usize;
        for label in metric.tags() {
            names = add_checked(names, label.name.len())?;
            values = add_checked(values, label.value.len())?;
        }
        checked_offset(names)?;
        checked_offset(values)?;
        bytes = add_checked(bytes, names)?;
        bytes = add_checked(bytes, values)?;
    }
    Ok(bytes)
}

/// Float values plus their validity bitmap.
fn float_charge(len: usize) -> Result<usize> {
    checked_offset(len)?;
    add_checked(mul_checked(len, 8)?, bitmap_bytes(len))
}

/// Reference indices plus their bitmap and the full referenced histogram table.
fn histogram_charge(values: &HistogramSamples) -> Result<usize> {
    let indices = values.indices();
    checked_offset(indices.len())?;
    let mut bytes = add_checked(mul_checked(indices.len(), 4)?, bitmap_bytes(indices.len()))?;
    let table = values.batch().as_arrow();
    checked_offset(table.len())?;
    bytes = add_checked(
        bytes,
        add_checked(
            HISTOGRAM_TABLE_CHARGE,
            mul_checked(table.len(), HISTOGRAM_ROW_CHARGE)?,
        )?,
    )?;
    for (position, width) in HISTOGRAM_LIST_WIDTHS.iter().enumerate() {
        let list = table
            .column(7 + position)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| err("histogram table schema mismatch"))?;
        // `value_offsets` is already a view of the visible slice; the retained child array may be
        // wider than the offsets span, so charge the visible extent rather than its length.
        let offsets = list.value_offsets();
        let first = offsets
            .first()
            .copied()
            .ok_or_else(|| err("histogram table schema mismatch"))?;
        let last = offsets
            .get(list.len())
            .copied()
            .ok_or_else(|| err("histogram table schema mismatch"))?;
        let span = last
            .checked_sub(first)
            .and_then(|span| usize::try_from(span).ok())
            .ok_or_else(|| err("histogram table offsets are invalid"))?;
        bytes = add_checked(bytes, mul_checked(span, *width)?)?;
    }
    Ok(bytes)
}

/// Encodes buffered rows as one batch with direct N-row Arrow array construction.
///
/// Row-local histogram indices stay exactly as encoded: physical tables are concatenated under
/// cumulative row list offsets, so no index is remapped. Each histogram-bearing row carries its
/// complete table, including unreferenced entries; many segments referencing one large table can
/// therefore repeat that table in the encoded batch. A single constituent child array is reused
/// by clone (sharing its buffers); multiple constituents are concatenated, which copies.
fn encode_rows(
    schema: &SchemaRef,
    mode: RawLabelMode,
    rows: &[RawFrameRow],
) -> Result<RecordBatch> {
    let count = rows.len();
    let DataType::Struct(time_fields) = schema.field(2).data_type() else {
        return invalid_series("time schema mismatch");
    };
    let DataType::List(time_item) = time_fields[0].data_type() else {
        return invalid_series("time schema mismatch");
    };
    let DataType::Struct(field_types) = schema.field(3).data_type() else {
        return invalid_series("fields schema mismatch");
    };
    let mut time_lengths = Vec::with_capacity(count);
    let mut time_parts: Vec<ArrayRef> = Vec::with_capacity(count);
    for row in rows {
        time_lengths.push(row.times.len());
        time_parts.push(Arc::new(Int64Array::new(row.times.clone(), None)));
    }
    let time_child = materialize_parts(&time_parts, || {
        Ok(Arc::new(Int64Array::new_null(0)) as ArrayRef)
    })?;
    let times = list_array(&time_lengths, time_child, time_item.as_ref().clone(), None)?;
    let time = struct_array(time_fields.clone(), vec![Arc::new(times)])?;

    let mut empty_table: Option<ArrayRef> = None;
    let mut field_arrays = Vec::with_capacity(field_types.len());
    for (index, field) in field_types.iter().enumerate() {
        let DataType::Struct(payload) = field.data_type() else {
            return invalid_series("payload schema mismatch");
        };
        let DataType::List(float_item) = payload[0].data_type() else {
            return invalid_series("float values schema mismatch");
        };
        let DataType::Struct(histogram_fields) = payload[1].data_type() else {
            return invalid_series("histogram schema mismatch");
        };
        let DataType::List(index_item) = histogram_fields[0].data_type() else {
            return invalid_series("histogram index schema mismatch");
        };
        let DataType::List(table_item) = histogram_fields[1].data_type() else {
            return invalid_series("histogram table schema mismatch");
        };
        let DataType::Struct(table_fields) = table_item.data_type() else {
            return invalid_series("histogram table schema mismatch");
        };
        let mut float_lengths = Vec::with_capacity(count);
        let mut float_parts: Vec<ArrayRef> = Vec::new();
        let mut float_valid = Vec::with_capacity(count);
        let mut index_lengths = Vec::with_capacity(count);
        let mut index_parts: Vec<ArrayRef> = Vec::new();
        let mut table_lengths = Vec::with_capacity(count);
        let mut table_parts: Vec<ArrayRef> = Vec::new();
        let mut histogram_valid = Vec::with_capacity(count);
        for row in rows {
            let dense = row
                .samples
                .get(index)
                .ok_or_else(|| err("raw frame row field count mismatch"))?
                .dense()
                .ok_or_else(|| err("sample segment is not dense"))?;
            match dense {
                DenseSamplesRef::Float(values) => {
                    float_lengths.push(values.len());
                    float_parts.push(Arc::new(values.clone()));
                    float_valid.push(true);
                    index_lengths.push(0);
                    table_lengths.push(0);
                    histogram_valid.push(false);
                }
                DenseSamplesRef::Histogram(values) => {
                    float_lengths.push(0);
                    float_valid.push(false);
                    index_lengths.push(values.len());
                    index_parts.push(Arc::new(values.indices().clone()));
                    table_lengths.push(values.batch().len());
                    table_parts.push(Arc::new(values.batch().as_arrow().clone()));
                    histogram_valid.push(true);
                }
                DenseSamplesRef::Mixed(values) => {
                    float_lengths.push(values.floats().len());
                    float_parts.push(Arc::new(values.floats().clone()));
                    float_valid.push(true);
                    index_lengths.push(values.histograms().len());
                    index_parts.push(Arc::new(values.histograms().indices().clone()));
                    table_lengths.push(values.histograms().batch().len());
                    table_parts.push(Arc::new(values.histograms().batch().as_arrow().clone()));
                    histogram_valid.push(true);
                }
            }
        }
        let float_child = materialize_parts(&float_parts, || {
            Ok(Arc::new(Float64Array::new_null(0)) as ArrayRef)
        })?;
        let floats = list_array(
            &float_lengths,
            float_child,
            float_item.as_ref().clone(),
            validity(&float_valid),
        )?;
        let index_child = materialize_parts(&index_parts, || {
            Ok(Arc::new(UInt32Array::new_null(0)) as ArrayRef)
        })?;
        let indices = list_array(
            &index_lengths,
            index_child,
            index_item.as_ref().clone(),
            None,
        )?;
        // A batch without histogram tables shares one empty schema-typed table, built once here
        // instead of per float-only row.
        let table_child = if table_parts.is_empty() {
            match &empty_table {
                Some(table) => table.clone(),
                None => {
                    let table = empty_histogram_table(table_fields)?;
                    empty_table = Some(table.clone());
                    table
                }
            }
        } else {
            materialize_parts(&table_parts, || Err(err("missing histogram table values")))?
        };
        let tables = list_array(
            &table_lengths,
            table_child,
            table_item.as_ref().clone(),
            None,
        )?;
        let histogram = arrow(StructArray::try_new(
            histogram_fields.clone(),
            vec![Arc::new(indices), Arc::new(tables)],
            validity(&histogram_valid),
        ))?;
        field_arrays.push(Arc::new(struct_array(
            payload.clone(),
            vec![Arc::new(floats), Arc::new(histogram)],
        )?) as ArrayRef);
    }
    let fields = struct_array(field_types.clone(), field_arrays)?;

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(binary_array(rows)?),
        Arc::new(BooleanArray::from(
            rows.iter().map(|row| row.is_last).collect::<Vec<_>>(),
        )),
        Arc::new(time),
        Arc::new(fields),
    ];
    if mode == RawLabelMode::Explicit {
        columns.push(Arc::new(
            rows.iter()
                .map(|row| row.metric.as_ref().and_then(MetricName::name))
                .collect::<StringArray>(),
        ));
        let DataType::List(label_item) = schema.field(5).data_type() else {
            return invalid_series("labels schema mismatch");
        };
        let DataType::Struct(label_fields) = label_item.data_type() else {
            return invalid_series("labels schema mismatch");
        };
        let mut label_lengths = Vec::with_capacity(count);
        let mut tag_names: Vec<&str> = Vec::new();
        let mut tag_values: Vec<&str> = Vec::new();
        for row in rows {
            let metric = row
                .metric
                .as_ref()
                .ok_or_else(|| err("explicit labels missing"))?;
            label_lengths.push(metric.tags().len());
            for label in metric.tags() {
                tag_names.push(label.name.as_ref());
                tag_values.push(label.value.as_ref());
            }
        }
        let labels = struct_array(
            label_fields.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(tag_names)),
                Arc::new(StringArray::from_iter_values(tag_values)),
            ],
        )?;
        columns.push(Arc::new(list_array(
            &label_lengths,
            Arc::new(labels),
            label_item.as_ref().clone(),
            None,
        )?));
    }

    arrow(RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(count)),
    ))
}

/// Row-level binary keys; keys are copied exactly like the previous single-row encoder.
fn binary_array(rows: &[RawFrameRow]) -> Result<BinaryArray> {
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    let mut total = 0usize;
    offsets.push(0);
    for row in rows {
        total = add_checked(total, row.primary_key.len())?;
        offsets.push(checked_offset(total)?);
    }
    let mut values = Vec::with_capacity(total);
    for row in rows {
        values.extend_from_slice(row.primary_key.as_slice());
    }
    arrow(BinaryArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Buffer::from_vec(values),
        None,
    ))
}

/// Per-row charge for key/time/nullable overhead shared by every row.
const ROW_CHARGE: usize = 64;
/// Per-field charge for the payload struct, nested list and validity overhead.
const FIELD_CHARGE: usize = 64;
/// Conservative per-table-row charge: the fixed fourteen-column histogram schema has seven
/// primitive columns plus seven list columns whose item widths are [`HISTOGRAM_LIST_WIDTHS`].
const HISTOGRAM_ROW_CHARGE: usize = 128;
/// Fixed per-histogram charge for the seven list columns' offset and validity overhead.
const HISTOGRAM_TABLE_CHARGE: usize = 28;
/// Item widths of histogram table list columns `7..=13`, in that order.
const HISTOGRAM_LIST_WIDTHS: [usize; 7] = [8, 4, 4, 8, 4, 4, 8];

fn list_array(
    lengths: &[usize],
    values: ArrayRef,
    item: Field,
    nulls: Option<NullBuffer>,
) -> Result<ListArray> {
    arrow(ListArray::try_new(
        Arc::new(item),
        offset_buffer(lengths)?,
        values,
        nulls,
    ))
}

fn offset_buffer(lengths: &[usize]) -> Result<OffsetBuffer<i32>> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    let mut total = 0usize;
    offsets.push(0);
    for length in lengths {
        total = add_checked(total, *length)?;
        offsets.push(checked_offset(total)?);
    }
    Ok(OffsetBuffer::new(ScalarBuffer::from(offsets)))
}

/// Materializes one list child: a single constituent is cloned to keep its buffers shared.
fn materialize_parts(
    parts: &[ArrayRef],
    empty: impl FnOnce() -> Result<ArrayRef>,
) -> Result<ArrayRef> {
    match parts {
        [] => empty(),
        [only] => Ok(only.clone()),
        _ => arrow(concat(
            &parts.iter().map(|array| &**array).collect::<Vec<_>>(),
        )),
    }
}

fn empty_histogram_table(fields: &Fields) -> Result<ArrayRef> {
    let columns = fields
        .iter()
        .map(|field| arrow::array::new_null_array(field.data_type(), 0))
        .collect();
    arrow(StructArray::try_new(fields.clone(), columns, None))
        .map(|table| Arc::new(table) as ArrayRef)
}

fn validity(valid: &[bool]) -> Option<NullBuffer> {
    if valid.iter().all(|valid| *valid) {
        None
    } else {
        Some(NullBuffer::from(valid.to_vec()))
    }
}

fn bitmap_bytes(len: usize) -> usize {
    len.div_ceil(8)
}

fn add_checked(total: usize, amount: usize) -> Result<usize> {
    total
        .checked_add(amount)
        .ok_or_else(|| err("raw series frame offset overflow"))
}

fn mul_checked(len: usize, width: usize) -> Result<usize> {
    len.checked_mul(width)
        .ok_or_else(|| err("raw series frame offset overflow"))
}

/// Reuses one prepared frame schema while decoding all batches of a stream.
pub struct RawSeriesFrameDecoder {
    schema: SeriesSchema,
    mode: RawLabelMode,
    expected: SchemaRef,
}

impl RawSeriesFrameDecoder {
    /// Builds the expected frame schema once from the series schema and label mode.
    pub fn try_new(schema: &SeriesSchema, mode: RawLabelMode) -> Result<Self> {
        Ok(Self {
            schema: schema.clone(),
            mode,
            expected: raw_series_frame_schema(schema, mode)?,
        })
    }

    /// Decodes all rows of one batch, checking its reserved metadata, declared fields, and rows.
    pub fn decode(&self, batch: &RecordBatch) -> Result<Vec<RawSeriesChunk>> {
        decode_raw_series_frame(&self.schema, self.mode, &self.expected, batch)
    }
}

/// Decodes all rows of a raw frame; any malformed row makes the entire call fail.
pub fn unpack_raw_series_frame(
    schema: &SeriesSchema,
    mode: RawLabelMode,
    batch: &RecordBatch,
) -> Result<Vec<RawSeriesChunk>> {
    RawSeriesFrameDecoder::try_new(schema, mode)?.decode(batch)
}

fn decode_raw_series_frame(
    schema: &SeriesSchema,
    mode: RawLabelMode,
    expected: &SchemaRef,
    batch: &RecordBatch,
) -> Result<Vec<RawSeriesChunk>> {
    let actual = batch.schema();
    for key in [LAYOUT, CONTENT, LABELS, TIME] {
        if actual.metadata().get(key) != expected.metadata().get(key) {
            return invalid_series("raw frame metadata mismatch");
        }
    }
    if actual
        .metadata()
        .keys()
        .any(|key| key == GRID || key.starts_with("greptime.series.grid."))
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
                if has_float {
                    Samples::try_from_mixed(floats, HistogramSamples::try_new(indices, batch)?)?
                } else {
                    Samples::try_from_histogram(indices, batch)?
                }
            };
            decoded.push(samples);
        }
        let fields = SeriesFields::try_from_vec(decoded)?;
        let key = binary_slice(keys, row)?;
        let raw_time = RawTimestamps::try_new(time)?;
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

fn binary_slice(array: &BinaryArray, row: usize) -> Result<Buffer> {
    let offsets = array.value_offsets();
    let start = usize::try_from(offsets[row]).map_err(|_| err("negative binary offset"))?;
    let end = usize::try_from(offsets[row + 1]).map_err(|_| err("negative binary offset"))?;
    let len = end
        .checked_sub(start)
        .ok_or_else(|| err("invalid binary offsets"))?;
    Ok(array.values().slice_with_length(start, len))
}

fn checked_offset(len: usize) -> Result<i32> {
    i32::try_from(len).map_err(|_| err("Arrow offset limit exceeded"))
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
