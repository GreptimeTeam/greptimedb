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

//! One Arrow frame family for raw and evaluated series chunks.
//!
//! A frame carries the same thing whichever sample shape it holds: one row per chunk of one series,
//! with that series' identity, its finality, its time axis, and one column of samples per logical
//! field. Field names preserve the logical series schema in order. What varies is declared rather
//! than inferred, because the receiver of a frame has no scanner, region, or planning context to
//! infer it from:
//!
//! | Content | Identity | Time | Producer |
//! | --- | --- | --- | --- |
//! | `raw` | `key` | `timestamps` | a storage raw-series scan |
//! | `raw` | `explicit` | `timestamps` | a raw input whose labels are already decoded |
//! | `evaluated` | `explicit` | `grid_spans` | a compute node's evaluated output |
//!
//! Any other combination is rejected here, because no producer can mean it: an evaluated value's
//! labels may be rewritten by the computation that produced it, so an evaluated frame cannot claim
//! a storage key as its identity, and raw samples have no evaluation grid to be addressed on.
//!
//! Packing accumulates bounded multi-row batches: [`SeriesFrameCursor`] walks one validated chunk
//! and [`SeriesFrameBuilder`] emits rows directly as Arrow arrays once a row or estimated byte bound
//! is reached. Decoding reuses one prepared
//! schema per stream through [`SeriesFrameDecoder`]. Column layout, metadata, and wire strings stay
//! on the frozen `v2` contract; a key-encoded frame additionally declares a versioned
//! [`LabelDecodeSpec`] so its labels can be decoded from the received schema alone, and an evaluated
//! frame declares its [`GridSpec`] once instead of repeating timestamps.
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
    StructArray, UInt32Array, UInt64Array,
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
    DenseSamplesRef, FieldSpec, HistogramSamples, Samples, Series, SeriesFields, SeriesSchema,
};
use crate::series::time::{EvalGrid, GridRun, GridSpan, GridSpec, TimeAxis};
use crate::series::{Label, SeriesLabels, invalid_series};

const LAYOUT: &str = "greptime.series.layout";
const CONTENT: &str = "greptime.series.content";
const LABELS: &str = "greptime.series.labels";
const TIME: &str = "greptime.series.time";
const GRID: &str = "greptime.series.grid";
/// Metadata key of the versioned label decode description carried by key-encoded frames.
const LABEL_DECODE: &str = "greptime.series.label_decode";
/// Metadata key of the producer ordering carried by a complete frame declaration.
const ORDERING: &str = "greptime.series.ordering";

/// Every `greptime.series.*` key a frame this revision writes may carry.
///
/// Frame schemas carry an explicit `contiguous` default; streaming producers may replace it with
/// their stronger guarantee.
const RESERVED_KEYS: [&str; 7] = [LAYOUT, CONTENT, LABELS, TIME, GRID, LABEL_DECODE, ORDERING];

/// The only layout this revision writes and reads.
///
/// A frame declaring any other value is refused instead of decoded: the sample representation of an
/// unreleased format is not worth a second decoder, and silently guessing would turn a format change
/// into wrong values.
pub const FRAME_LAYOUT_VERSION: &str = "v2";

/// Version of the label decode description this revision writes and reads.
///
/// This is independent of [`FRAME_LAYOUT_VERSION`]: the description versions its own text, which a
/// reader refuses to guess at, while the layout versions the column and metadata set around it.
pub const LABEL_DECODE_VERSION: u32 = 1;

/// What one frame's rows hold: raw samples with their own timestamps, or evaluated samples
/// addressed on the grid the frame declares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameContent {
    /// Samples read from storage, each row carrying the timestamps it was read at.
    Raw,
    /// Values computed on an evaluation grid, each row carrying the grid positions it holds.
    Evaluated,
}

impl FrameContent {
    /// The `greptime.series.content` text of this content.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Evaluated => "evaluated",
        }
    }

    /// Parses a `greptime.series.content` value this revision implements.
    const fn parse(text: &str) -> Option<Self> {
        match text.as_bytes() {
            b"raw" => Some(Self::Raw),
            b"evaluated" => Some(Self::Evaluated),
            _ => None,
        }
    }

    /// The `greptime.series.time` text this content's rows are addressed by.
    ///
    /// Time mode is a function of content and never an independent choice: raw samples carry their
    /// own timestamps and evaluated ones sit on a grid, so a frame has no way to hold one without
    /// the other. Writing it as a derived text rather than a second enum keeps the two from ever
    /// disagreeing.
    const fn time_text(self) -> &'static str {
        match self {
            Self::Raw => "timestamps",
            Self::Evaluated => "grid_spans",
        }
    }

    /// The struct member a row's time column carries for this content.
    const fn time_member(self) -> &'static str {
        match self {
            Self::Raw => "timestamps",
            Self::Evaluated => "spans",
        }
    }
}

/// How a frame names the series of its rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameLabelMode {
    /// Identity is only the encoded storage key in `read_key`, decoded through the frame's own
    /// [`LabelDecodeSpec`].
    Key,
    /// The complete metric name and ordinary labels are present in the frame as well.
    Explicit,
}

impl FrameLabelMode {
    /// The `greptime.series.labels` text of this mode.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Key => "key",
            Self::Explicit => "explicit",
        }
    }

    /// Parses a `greptime.series.labels` value this revision implements.
    const fn parse(text: &str) -> Option<Self> {
        match text.as_bytes() {
            b"key" => Some(Self::Key),
            b"explicit" => Some(Self::Explicit),
            _ => None,
        }
    }
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
/// [`series_frame_schema`], so both the producing and the expecting side of a
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
    /// [`series_frame_schema`] applies this to a copy before serializing, and
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

    /// Checks the version and structural invariants of the description.
    ///
    /// Storage-region binding semantics are deliberately outside this schema-level validation.
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

/// Builds the one frame schema family: raw or evaluated, keyed or explicit.
///
/// Everything the wire states is derived from the two things a producer actually holds. The
/// evaluation grid comes from `schema.eval_grid`, where a schema without one describes raw samples
/// and a schema with one describes values addressed on that grid, so content and time mode can never
/// contradict the columns they describe. `labels` decides identity, and with it whether the frame
/// carries the `name`/`labels` columns: a key frame keeps its labels encoded in `read_key` and must
/// declare the [`LabelDecodeSpec`] that decodes them, while an explicit frame carries complete
/// labels and must not declare one.
///
/// This is the only place the reserved `greptime.series.*` text is written, so a producer and a
/// reader cannot spell one frame two ways.
pub fn series_frame_schema(
    schema: &SeriesSchema,
    labels: FrameLabelMode,
    decode: Option<&LabelDecodeSpec>,
) -> Result<SchemaRef> {
    if schema.fields.is_empty() {
        return invalid_series("series frame schema must have fields");
    }
    let content = match &schema.eval_grid {
        Some(_) => FrameContent::Evaluated,
        None => FrameContent::Raw,
    };
    match (labels, decode) {
        (FrameLabelMode::Key, None) => {
            return Err(err(
                "key-encoded frames must carry a label decode description",
            ));
        }
        (FrameLabelMode::Explicit, Some(_)) => {
            return Err(err(
                "explicit-label frames carry no label decode description",
            ));
        }
        (FrameLabelMode::Key, Some(_)) | (FrameLabelMode::Explicit, None) => {}
    }
    if content == FrameContent::Evaluated && labels == FrameLabelMode::Key {
        return Err(err(
            "an evaluated frame names its series by label, not by a storage key",
        ));
    }
    let list = |ty, nullable| DataType::List(Arc::new(Field::new("item", ty, nullable)));
    let time = match content {
        FrameContent::Raw => DataType::Struct(
            vec![Field::new(
                "timestamps",
                list(DataType::Int64, false),
                false,
            )]
            .into(),
        ),
        FrameContent::Evaluated => DataType::Struct(
            vec![Field::new(
                "spans",
                list(
                    DataType::Struct(
                        vec![
                            Field::new("start", DataType::UInt64, false),
                            Field::new("len", DataType::UInt64, false),
                        ]
                        .into(),
                    ),
                    false,
                ),
                false,
            )]
            .into(),
        ),
    };
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
        // A key frame *is* its key, so the column is required. An explicit frame names its series
        // by label and keeps the column for the runtime chunk's own contract: a raw chunk still
        // carries the key it was read under, an evaluated one carries none.
        Field::new(
            "read_key",
            DataType::Binary,
            labels == FrameLabelMode::Explicit,
        ),
        Field::new("is_last", DataType::Boolean, false),
        Field::new("time", time, false),
        Field::new("fields", DataType::Struct(fields), false),
    ];
    if labels == FrameLabelMode::Explicit {
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
    let mut metadata = vec![
        (LAYOUT.to_string(), FRAME_LAYOUT_VERSION.to_string()),
        (CONTENT.to_string(), content.as_str().to_string()),
        (LABELS.to_string(), labels.as_str().to_string()),
        (TIME.to_string(), content.time_text().to_string()),
        (ORDERING.to_string(), "contiguous".to_string()),
    ];
    if let Some(grid) = &schema.eval_grid {
        metadata.push((GRID.to_string(), grid_text(grid.spec())?));
    }
    if let Some(decode) = decode {
        // Writing is faithful: the description records what the producer declares, and nothing is
        // rewritten or dropped. The one exception is the order of `logical_tags`, which carries no
        // meaning to a decoder and which this text nevertheless has to fix so two producers of the
        // same labels write the same bytes; a copy is canonicalized and the caller's value is
        // untouched.
        //
        // What is not accepted is a description this revision would refuse to read. A frame is worth
        // writing only if it can be decoded, so the same check runs here: a duplicate key column, a
        // logical tag naming no listed column, or a description of an unknown version is refused at
        // the producer rather than when the frame reaches its consumer, where the producer is no
        // longer in a position to fix it.
        let mut canonical = decode.clone();
        canonical.canonicalize();
        canonical.validate()?;
        metadata.push((
            LABEL_DECODE.to_string(),
            serde_json::to_string(&canonical)
                .map_err(|e| err(format!("cannot encode label decode description: {e}")))?,
        ));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        columns,
        metadata.into_iter().collect(),
    )))
}

/// Builds the typed schema for a raw frame, refusing a series schema with an evaluation grid.
///
/// A key frame is unreadable without its description, so this constructor serves explicit-label
/// frames only; a caller that holds a binding declares through [`series_frame_schema`].
pub fn raw_series_frame_schema(schema: &SeriesSchema, labels: FrameLabelMode) -> Result<SchemaRef> {
    if schema.eval_grid.is_some() {
        return invalid_series("raw series schema cannot have an evaluation grid");
    }
    match labels {
        FrameLabelMode::Key => Err(err(
            "raw key frames must declare a label decode description",
        )),
        FrameLabelMode::Explicit => series_frame_schema(schema, labels, None),
    }
}

/// The canonical `greptime.series.grid` text of one grid.
///
/// Field order is the struct's declaration order and the text is compact, so two producers of the
/// same grid write the same bytes; nothing else in the tree spells this text.
fn grid_text(spec: GridSpec) -> Result<String> {
    serde_json::to_string(&spec).map_err(|e| err(format!("cannot encode evaluation grid: {e}")))
}

/// Parses one `greptime.series.grid` text back into a validated, deferrable grid.
///
/// Validation is [`GridSpec::try_new`]'s plus the unknown-field refusal serde performs, so a frame
/// cannot declare a grid this revision would compute differently. The timestamps themselves stay
/// unmaterialized until a consumer asks for them.
fn parse_grid(text: &str) -> Result<Arc<EvalGrid>> {
    let spec: GridSpec = serde_json::from_str(text)
        .map_err(|e| err(format!("cannot decode evaluation grid: {e}")))?;
    Ok(Arc::new(EvalGrid::try_deferred(spec)?))
}

/// A frame schema's reserved declarations, parsed and cross-checked.
///
/// The three facts a receiver needs are not independent - the content decides the time mode, the
/// label mode decides whether a description must be present, and only an evaluated frame may name a
/// grid - so they are parsed together and every contradictory combination is refused instead of one
/// side of it being picked.
#[derive(Clone, Debug)]
pub struct SeriesFrameDeclaration {
    series: SeriesSchema,
    labels: FrameLabelMode,
    label_decode: Option<LabelDecodeSpec>,
}

fn validate_ordering(schema: &Schema, content: FrameContent) -> Result<()> {
    match schema.metadata().get(ORDERING).map(String::as_str) {
        Some("contiguous") => Ok(()),
        Some("read_key_then_time") if content == FrameContent::Raw => Ok(()),
        Some("read_key_then_time") => Err(err(
            "an evaluated frame cannot claim key-then-time ordering",
        )),
        None => Err(err(format!("frame declares no {ORDERING} value"))),
        Some(_) => Err(err("unknown series ordering")),
    }
}

impl SeriesFrameDeclaration {
    /// Parses and cross-checks one frame schema.
    pub fn parse(schema: &Schema) -> Result<Self> {
        for key in schema.metadata().keys() {
            if key.starts_with("greptime.series.") && !RESERVED_KEYS.contains(&key.as_str()) {
                return Err(err(format!(
                    "frame declares unknown reserved metadata {key}"
                )));
            }
        }
        if schema.metadata().get(LAYOUT).map(String::as_str) != Some(FRAME_LAYOUT_VERSION) {
            return Err(err(format!("frame declares no known {LAYOUT} value")));
        }
        let content = schema
            .metadata()
            .get(CONTENT)
            .and_then(|text| FrameContent::parse(text))
            .ok_or_else(|| err(format!("frame declares no known {CONTENT} value")))?;
        validate_ordering(schema, content)?;
        let labels = schema
            .metadata()
            .get(LABELS)
            .and_then(|text| FrameLabelMode::parse(text))
            .ok_or_else(|| err(format!("frame declares no known {LABELS} value")))?;
        // Time mode is read as the content's own text: a frame whose time column is addressed the
        // other way contradicts itself, and there is no third time mode to fall back to.
        if schema.metadata().get(TIME).map(String::as_str) != Some(content.time_text()) {
            return Err(err(format!(
                "frame declares no {TIME} value this content can carry"
            )));
        }
        if content == FrameContent::Evaluated && labels == FrameLabelMode::Key {
            return Err(err(
                "an evaluated frame names its series by label, not by a storage key",
            ));
        }
        let grid = match (content, schema.metadata().get(GRID)) {
            (FrameContent::Raw, None) => None,
            (FrameContent::Raw, Some(_)) => {
                return Err(err("a raw frame cannot declare an evaluation grid"));
            }
            (FrameContent::Evaluated, None) => {
                return Err(err("an evaluated frame must declare its evaluation grid"));
            }
            (FrameContent::Evaluated, Some(text)) => Some(parse_grid(text)?),
        };
        let label_decode = match schema.metadata().get(LABEL_DECODE) {
            None => None,
            Some(text) => {
                let decode: LabelDecodeSpec = serde_json::from_str(text)
                    .map_err(|e| err(format!("cannot decode label decode description: {e}")))?;
                decode.validate()?;
                Some(decode)
            }
        };
        match (labels, &label_decode) {
            (FrameLabelMode::Key, None) => {
                return Err(err(
                    "key-encoded frames must carry a label decode description",
                ));
            }
            (FrameLabelMode::Explicit, Some(_)) => {
                return Err(err(
                    "explicit-label frames carry no label decode description",
                ));
            }
            (FrameLabelMode::Key, Some(_)) | (FrameLabelMode::Explicit, None) => {}
        }
        let fields = schema
            .fields()
            .iter()
            .find(|field| field.name() == "fields")
            .ok_or_else(|| err("frame declares no fields column"))?;
        let DataType::Struct(members) = fields.data_type() else {
            return Err(err("frame fields column is not a struct"));
        };
        if members.is_empty() {
            return Err(err("series frame schema must have fields"));
        }
        let declared_time = schema
            .fields()
            .iter()
            .find(|field| field.name() == "time")
            .ok_or_else(|| err("frame declares no time column"))?;
        let DataType::Struct(time_members) = declared_time.data_type() else {
            return Err(err("frame time column is not a struct"));
        };
        if time_members.len() != 1 || time_members[0].name() != content.time_member() {
            return Err(err(
                "frame time column does not match its declared time mode",
            ));
        }
        Ok(Self {
            series: SeriesSchema {
                fields: members
                    .iter()
                    .map(|member| FieldSpec {
                        name: Arc::from(member.name().as_str()),
                    })
                    .collect(),
                eval_grid: grid,
            },
            labels,
            label_decode,
        })
    }

    /// The logical fields and evaluation grid the frame declares.
    pub fn series(&self) -> &SeriesSchema {
        &self.series
    }

    /// What the frame's rows hold.
    pub fn content(&self) -> FrameContent {
        match self.series.eval_grid {
            Some(_) => FrameContent::Evaluated,
            None => FrameContent::Raw,
        }
    }

    /// How the frame names its series.
    pub fn labels(&self) -> FrameLabelMode {
        self.labels
    }

    /// The label decode description the frame declares, present exactly for key identity.
    pub fn label_decode(&self) -> Option<&LabelDecodeSpec> {
        self.label_decode.as_ref()
    }
}

/// Recovers the label decode description a frame schema declares.
///
/// Returns `None` for an explicit-label frame, which must not declare one. A key-encoded frame must
/// declare one, so a missing description is an error rather than a reason to reuse a caller-side
/// binding; [`series_frame_schema`] and [`SeriesFrameDeclaration::parse`] enforce that both ways.
pub fn series_frame_label_decode(schema: &Schema) -> Result<Option<LabelDecodeSpec>> {
    Ok(SeriesFrameDeclaration::parse(schema)?.label_decode)
}

/// The two declarations the encoder and decoder dispatch on.
///
/// A frame's time mode follows from its content and its identity is the label mode, so those two
/// are all that has to travel next to the column layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FrameShape {
    content: FrameContent,
    labels: FrameLabelMode,
}

impl FrameShape {
    fn of(schema: &SeriesSchema, labels: FrameLabelMode) -> Self {
        let content = match schema.eval_grid {
            Some(_) => FrameContent::Evaluated,
            None => FrameContent::Raw,
        };
        Self { content, labels }
    }
}

/// Bounded multi-row builder for finite series frame batches.
///
/// Rows accumulate until the row limit or the estimated byte limit is reached; the next cursor call
/// then emits them as one batch. `max_bytes` is stored as the effective group cap
/// `min(requested, i32::MAX)`, and each row carries a measure that dominates every cumulative Arrow
/// offset domain, so any admitted group can be encoded. The measure is an internal work estimate, not
/// an exact IPC size and not a resident-memory cap: one individually representable row that exceeds
/// the requested cap is still accepted and emitted alone, because that slice cannot be split into
/// smaller legal rows.
pub struct SeriesFrameBuilder {
    schema: SeriesSchema,
    frame_schema: SchemaRef,
    shape: FrameShape,
    max_rows: usize,
    max_bytes: usize,
    rows: Vec<FrameRow>,
    bytes: usize,
}

impl SeriesFrameBuilder {
    /// Default maximum rows in one packed batch when no explicit bound is needed.
    pub const DEFAULT_MAX_ROWS: usize = 256;
    /// Default maximum estimated encoded bytes in one packed batch when no explicit bound is
    /// needed.
    pub const DEFAULT_MAX_BYTES: usize = 1024 * 1024;

    /// Creates an empty builder for the given schema, identity declaration, and nonzero bounds.
    ///
    /// The frame schema is built once here and reused for every emitted batch. It declares
    /// everything the rows carry except the stream's producer ordering, which only the streaming
    /// layer knows and adds when it declares the batch.
    pub fn try_new(
        schema: &SeriesSchema,
        labels: FrameLabelMode,
        decode: Option<&LabelDecodeSpec>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self> {
        if max_rows == 0 || max_bytes == 0 {
            return invalid_series("series frame limits must be nonzero");
        }
        Ok(Self {
            schema: schema.clone(),
            frame_schema: series_frame_schema(schema, labels, decode)?,
            shape: FrameShape::of(schema, labels),
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
        let batch = encode_rows(&self.frame_schema, self.shape, &self.rows)?;
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
    fn accepts(&self, row: &FrameRow) -> bool {
        self.rows.is_empty()
            || (self.bytes < self.max_bytes
                && self.rows.len() < self.max_rows
                && self
                    .bytes
                    .checked_add(row.bytes)
                    .is_some_and(|total| total <= self.max_bytes))
    }

    /// Buffers one individually measured row.
    fn push(&mut self, row: FrameRow) -> Result<()> {
        self.bytes = add_checked(self.bytes, row.bytes)?;
        self.rows.push(row);
        Ok(())
    }

    /// Checks that a cursor walks the same fields, identity, and time of what this builder encodes.
    ///
    /// The grid is compared by its specification rather than by whether one is present: a cursor
    /// holding positions on one grid and a builder encoding them under another would write the same
    /// numbers meaning different instants, and nothing downstream could tell. Two grids with equal
    /// bounds are the same grid, so they compare equal however they were allocated.
    fn matches(&self, schema: &SeriesSchema, shape: FrameShape) -> Result<()> {
        let same_fields = schema.fields.len() == self.schema.fields.len()
            && schema
                .fields
                .iter()
                .zip(self.schema.fields.iter())
                .all(|(left, right)| left.name == right.name);
        let same_grid = match (&schema.eval_grid, &self.schema.eval_grid) {
            (None, None) => true,
            (Some(left), Some(right)) => left.spec() == right.spec(),
            _ => false,
        };
        if shape != self.shape || !same_grid || !same_fields {
            return invalid_series("series frame cursor and builder schema mismatch");
        }
        Ok(())
    }
}

/// One validated chunk walked into a bounded builder, one row at a time.
///
/// The two sample shapes differ only in how a row's positions are addressed and where its identity
/// comes from, so one cursor serves both: a raw chunk is walked segment by segment over the
/// timestamps it carries, an evaluated chunk run by run over the grid positions it holds. Each row is
/// measured once and retried after a flush, so a flush error never consumes input.
pub struct SeriesFrameCursor {
    schema: SeriesSchema,
    shape: FrameShape,
    walk: ChunkWalk,
    pending: Option<FrameRow>,
}

/// Where a cursor is in the chunk it walks.
enum ChunkWalk {
    /// A raw chunk: one timestamp per row of samples.
    Raw(RawWalk),
    /// An evaluated chunk: absolute grid positions, carried as runs.
    Evaluated(EvaluatedWalk),
}

/// The walk state of one raw chunk.
///
/// The chunk's fields are walked segment by segment, so a row never spans two segments of one field
/// and no per-row temporary vectors are allocated.
struct RawWalk {
    chunk: RawSeriesChunk,
    /// The dense segment each field is being read from.
    segments: Vec<usize>,
    /// How much of each field's current segment has been consumed.
    offsets: Vec<usize>,
    /// How many timestamps have been consumed.
    pos: usize,
}

/// The walk state of one evaluated chunk.
///
/// The runs are collected once, so a row is one intersection of a run with a sample segment and the
/// walk never re-derives the runs per row. A chunk that carries no position keeps a single `closing`
/// flag: it still owes one row, and that row is what tells its consumer the chain ended.
struct EvaluatedWalk {
    series: Series,
    is_last: bool,
    /// The runs of carried rows, in absolute grid order and never empty.
    runs: Vec<GridRun>,
    /// The run being read.
    run: usize,
    /// How much of the current run has been consumed.
    offset: usize,
    /// The dense segment each field is being read from.
    segments: Vec<usize>,
    offsets: Vec<usize>,
    /// Whether the one row a chunk without positions still owes is yet to be emitted.
    closing: bool,
}

impl SeriesFrameCursor {
    /// Validates one public raw chunk's shape and label mode once, taking ownership of it.
    pub fn try_new_raw(
        schema: &SeriesSchema,
        mode: FrameLabelMode,
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
            (FrameLabelMode::Key, None) | (FrameLabelMode::Explicit, Some(_)) => {}
            _ => return invalid_series("raw chunk label mode does not match frame mode"),
        }
        let segments = vec![0usize; chunk.fields.len()];
        let offsets = vec![0usize; chunk.fields.len()];
        Ok(Self {
            schema: schema.clone(),
            shape: FrameShape::of(schema, mode),
            walk: ChunkWalk::Raw(RawWalk {
                chunk,
                segments,
                offsets,
                pos: 0,
            }),
            pending: None,
        })
    }

    /// Validates one evaluated chunk against the frame's schema, grid, and finality rules.
    ///
    /// A chunk that carries no position is legal only when it closes its series: it is how a
    /// consumer tells "nothing more here" from "no more input", and a chain of empty chunks that
    /// close nothing would never make progress. It still occupies one frame row, because dropping it
    /// would drop the finality it exists to carry.
    pub fn try_new_evaluated(schema: &SeriesSchema, series: Series, is_last: bool) -> Result<Self> {
        if schema.eval_grid.is_none() {
            return invalid_series("an evaluated chunk needs a schema with an evaluation grid");
        }
        let series = Series::try_new(
            schema,
            series.metric.clone(),
            series.time.clone(),
            series.fields.clone(),
        )?;
        if series.time.grid().is_none() {
            return invalid_series("an evaluated chunk must be addressed on a grid");
        }
        if series.time.is_empty() && !is_last {
            return invalid_series("an evaluated chunk without positions must close its series");
        }
        // The runs are the only runs this chunk carries, so collecting them once is what keeps the
        // walk from re-deriving them per row; a maximal run is merged by the axis itself.
        let runs: Vec<GridRun> = series.time.runs().collect();
        let segments = vec![0usize; series.fields.len()];
        let offsets = vec![0usize; series.fields.len()];
        Ok(Self {
            schema: schema.clone(),
            shape: FrameShape::of(schema, FrameLabelMode::Explicit),
            walk: ChunkWalk::Evaluated(EvaluatedWalk {
                closing: runs.is_empty(),
                series,
                is_last,
                runs,
                run: 0,
                offset: 0,
                segments,
                offsets,
            }),
            pending: None,
        })
    }

    /// Appends rows to the builder until one batch is ready or this chunk is exhausted.
    ///
    /// `None` means the cursor is exhausted; calling again returns `None` without touching the
    /// builder. The single pending row is measured once by [`Self::next_row`] and retried after the
    /// buffered rows are emitted.
    pub fn append_to(&mut self, builder: &mut SeriesFrameBuilder) -> Result<Option<RecordBatch>> {
        builder.matches(&self.schema, self.shape)?;
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
                    .ok_or_else(|| err("frame builder rejected a row without buffered rows"))?;
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

    /// Builds and measures the next row, or `None` when the chunk is exhausted.
    fn next_row(&mut self) -> Result<Option<FrameRow>> {
        match &mut self.walk {
            ChunkWalk::Raw(walk) => walk.next_row(),
            ChunkWalk::Evaluated(walk) => walk.next_row(),
        }
    }
}

impl RawWalk {
    /// Builds the next row, or `None` when the chunk is exhausted.
    ///
    /// The chunk fields are walked twice: once to find how many positions every field can cover, and
    /// once to slice them. A row does collect one slice per field, so the saving here is not
    /// allocation-free rows but no row spanning two segments of one field, which is what keeps the
    /// slice arithmetic per row a single offset. The offsets advance only after the row measure
    /// succeeded.
    fn next_row(&mut self) -> Result<Option<FrameRow>> {
        let Self {
            chunk,
            segments,
            offsets,
            pos,
        } = self;
        if *pos >= chunk.time.len() {
            return Ok(None);
        }
        let time_axis = chunk.time.as_buffer();
        let mut take = time_axis.len() - *pos;
        for (index, field) in chunk.fields.iter().enumerate() {
            let part = segment(field, segments[index])?;
            take = take.min(part.len().saturating_sub(offsets[index]));
        }
        if take == 0 {
            return invalid_series("empty sample segment");
        }
        let mut samples = Vec::with_capacity(chunk.fields.len());
        for (index, field) in chunk.fields.iter().enumerate() {
            let part = segment(field, segments[index])?;
            samples.push(part.slice(offsets[index], take)?);
        }
        let mut row = FrameRow {
            key: Some(chunk.primary_key.clone()),
            metric: chunk.metric.clone(),
            is_last: chunk.is_last && *pos + take == chunk.time.len(),
            time: RowTime::Timestamps(time_axis.slice(*pos, take)),
            samples,
            bytes: 0,
        };
        row.bytes = measure_row(&row)?;
        *pos += take;
        for (index, field) in chunk.fields.iter().enumerate() {
            let length = segment(field, segments[index])?.len();
            offsets[index] += take;
            if offsets[index] == length {
                segments[index] += 1;
                offsets[index] = 0;
            }
        }
        Ok(Some(row))
    }
}

impl EvaluatedWalk {
    /// Builds the next row, or `None` when the chunk is exhausted.
    ///
    /// A row covers one `(run of carried positions, sample segment)` intersection, which is why its
    /// positions are always one span: the holes between runs are not rows, so nothing is ever filled
    /// in for a grid position the chunk does not hold. The `is_last` a producer declared is carried
    /// by the chunk's final row only, so a consumer that sees it knows the chain ended exactly there.
    fn next_row(&mut self) -> Result<Option<FrameRow>> {
        let Self {
            series,
            is_last,
            runs,
            run,
            offset,
            segments,
            offsets,
            closing,
        } = self;
        if *closing {
            *closing = false;
            // A chunk that carries no position still owes its consumer one row: the row that says the
            // chain ends here. It holds no grid position and no sample, and each field is kept as the
            // producer built it - an empty float stays float, an empty histogram stays histogram - so a
            // decoder sees an empty closing chunk of the same payload kind rather than a missing one or
            // one silently converted.
            let mut samples = Vec::with_capacity(series.fields.len());
            for field in series.fields.iter() {
                samples.push(field.clone());
            }
            let mut row = FrameRow {
                key: None,
                metric: Some(series.metric.clone()),
                is_last: *is_last,
                time: RowTime::Spans(Vec::new()),
                samples,
                bytes: 0,
            };
            row.bytes = measure_row(&row)?;
            return Ok(Some(row));
        }
        let Some(current) = runs.get(*run) else {
            return Ok(None);
        };
        let remaining = current.len - *offset;
        let mut take = remaining;
        for (index, field) in series.fields.iter().enumerate() {
            let part = segment(field, segments[index])?;
            take = take.min(part.len().saturating_sub(offsets[index]));
        }
        if take == 0 {
            return invalid_series("empty sample segment");
        }
        let row_offset = current.row + *offset;
        let mut samples = Vec::with_capacity(series.fields.len());
        for (index, field) in series.fields.iter().enumerate() {
            let part = segment(field, segments[index])?;
            samples.push(part.slice(offsets[index], take)?);
        }
        let mut row = FrameRow {
            key: None,
            metric: Some(series.metric.clone()),
            is_last: *is_last && *run + 1 == runs.len() && take == remaining,
            time: RowTime::Spans(vec![GridSpan::new(current.position + *offset, take)]),
            samples,
            bytes: 0,
        };
        row.bytes = measure_row(&row)?;
        *offset += take;
        if *offset == current.len {
            *run += 1;
            *offset = 0;
        }
        for (index, field) in series.fields.iter().enumerate() {
            let length = segment(field, segments[index])?.len();
            offsets[index] += take;
            if offsets[index] == length {
                segments[index] += 1;
                offsets[index] = 0;
            }
        }
        debug_assert!(
            row_offset + take <= series.time.len(),
            "an evaluated row exceeds the axis it walks"
        );
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
/// drive [`SeriesFrameCursor`] and [`SeriesFrameBuilder`] directly instead of collecting.
pub fn pack_raw_series_chunk(
    schema: &SeriesSchema,
    mode: FrameLabelMode,
    decode: Option<&LabelDecodeSpec>,
    chunk: &RawSeriesChunk,
) -> Result<Vec<RecordBatch>> {
    let mut builder = SeriesFrameBuilder::try_new(
        schema,
        mode,
        decode,
        SeriesFrameBuilder::DEFAULT_MAX_ROWS,
        SeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )?;
    let mut cursor = SeriesFrameCursor::try_new_raw(schema, mode, chunk.clone())?;
    let mut batches = Vec::new();
    while let Some(batch) = cursor.append_to(&mut builder)? {
        batches.push(batch);
    }
    if let Some(batch) = builder.flush()? {
        batches.push(batch);
    }
    Ok(batches)
}

/// One pending output row: one chunk's identity, finality, time slice, and one dense sample slice per
/// field, with its checked encoded-size measure.
///
/// This is the codec's own row, not a third runtime chunk type: `RawSeriesChunk` and `Series` are
/// what a producer and a consumer see, and this is what the encoder writes between them.
struct FrameRow {
    /// The encoded storage key. A raw row always carries the key it was read under, a key-encoded
    /// frame names its series only by it, and an evaluated row has none.
    key: Option<Buffer>,
    /// Complete labels, present exactly for explicit identity.
    metric: Option<SeriesLabels>,
    /// Whether this row closes the chunk chain it belongs to.
    is_last: bool,
    /// The positions this row's samples are addressed at.
    time: RowTime,
    samples: Vec<Samples>,
    bytes: usize,
}

/// How one row addresses the time of its samples.
enum RowTime {
    /// The timestamps of this row's samples, strictly increasing within the row.
    Timestamps(ScalarBuffer<i64>),
    /// Runs of absolute grid positions, ordered, non-overlapping, and never zero length.
    Spans(Vec<GridSpan>),
}

impl RowTime {
    /// How many grid positions or timestamps this row's samples cover.
    ///
    /// The sum is checked because this is the first place a decoded row's positions are counted, and
    /// a frame can name spans whose lengths overflow `usize` long before any span is validated
    /// against the grid it claims to sit on. Adding them up unchecked would panic on a hostile frame
    /// in a debug build and wrap in a release build, both before the grid check that should have
    /// refused it.
    fn len(&self) -> Result<usize> {
        match self {
            Self::Timestamps(timestamps) => Ok(timestamps.len()),
            Self::Spans(spans) => spans.iter().try_fold(0usize, |total, span| {
                total
                    .checked_add(span.len())
                    .ok_or_else(|| err("grid span length overflow"))
            }),
        }
    }
}

/// Per-row encoded-size measure, computed once per row when the cursor produces it.
///
/// The measure dominates every cumulative offset domain the encoder writes: binary/UTF-8 child
/// bytes, list element counts, bitmap bytes, and the visible extent of each histogram table list.
/// A group total of `min(max_bytes, i32::MAX)` therefore guarantees that all Arrow offsets and
/// lengths stay representable, without tracking a separate offset counter. This is a bound used
/// for batching work, not the exact encoded size.
fn measure_row(row: &FrameRow) -> Result<usize> {
    let key_len = row.key.as_ref().map(Buffer::len).unwrap_or(0);
    checked_offset(key_len)?;
    checked_offset(row.time.len()?)?;
    let mut bytes = add_checked(ROW_CHARGE, key_len)?;
    bytes = match &row.time {
        RowTime::Timestamps(timestamps) => add_checked(bytes, mul_checked(timestamps.len(), 8)?)?,
        // Two `u64` per span, plus the list's own offsets and validity.
        RowTime::Spans(spans) => add_checked(
            add_checked(bytes, mul_checked(spans.len(), SPAN_CHARGE)?)?,
            mul_checked(spans.len(), 2)?,
        )?,
    };
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
fn encode_rows(schema: &SchemaRef, shape: FrameShape, rows: &[FrameRow]) -> Result<RecordBatch> {
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
    let time = match shape.content {
        FrameContent::Raw => {
            let mut time_lengths = Vec::with_capacity(count);
            let mut time_parts: Vec<ArrayRef> = Vec::with_capacity(count);
            for row in rows {
                let RowTime::Timestamps(timestamps) = &row.time else {
                    return invalid_series("a raw frame row carries grid positions");
                };
                time_lengths.push(timestamps.len());
                time_parts.push(Arc::new(Int64Array::new(timestamps.clone(), None)));
            }
            let time_child = materialize_parts(&time_parts, || {
                Ok(Arc::new(Int64Array::new_null(0)) as ArrayRef)
            })?;
            let times = list_array(&time_lengths, time_child, time_item.as_ref().clone(), None)?;
            struct_array(time_fields.clone(), vec![Arc::new(times)])?
        }
        FrameContent::Evaluated => {
            let DataType::Struct(span_fields) = time_item.data_type() else {
                return invalid_series("grid span schema mismatch");
            };
            let mut span_lengths = Vec::with_capacity(count);
            let mut starts = Vec::new();
            let mut lengths = Vec::new();
            for row in rows {
                let RowTime::Spans(spans) = &row.time else {
                    return invalid_series("an evaluated frame row carries timestamps");
                };
                span_lengths.push(spans.len());
                for span in spans {
                    starts.push(u64::try_from(span.start()).map_err(|_| {
                        err("a grid position does not fit the frame's unsigned span")
                    })?);
                    lengths.push(u64::try_from(span.len()).map_err(|_| {
                        err("a grid span length does not fit the frame's unsigned span")
                    })?);
                }
            }
            // A batch with no span at all still needs a typed child, so the empty case is spelled
            // out instead of being left to whatever `concat` does with nothing.
            let span_struct = if starts.is_empty() {
                struct_array(
                    span_fields.clone(),
                    vec![
                        Arc::new(UInt64Array::new_null(0)),
                        Arc::new(UInt64Array::new_null(0)),
                    ],
                )?
            } else {
                struct_array(
                    span_fields.clone(),
                    vec![
                        Arc::new(UInt64Array::from(starts)),
                        Arc::new(UInt64Array::from(lengths)),
                    ],
                )?
            };
            let spans = list_array(
                &span_lengths,
                Arc::new(span_struct),
                time_item.as_ref().clone(),
                None,
            )?;
            struct_array(time_fields.clone(), vec![Arc::new(spans)])?
        }
    };

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
        Arc::new(binary_array(rows, shape)?),
        Arc::new(BooleanArray::from(
            rows.iter().map(|row| row.is_last).collect::<Vec<_>>(),
        )),
        Arc::new(time),
        Arc::new(fields),
    ];
    if shape.labels == FrameLabelMode::Explicit {
        columns.push(Arc::new(
            rows.iter()
                .map(|row| row.metric.as_ref().and_then(SeriesLabels::name))
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

/// Row-level binary keys.
///
/// A raw row always carries the key its chunk was read under, so the column is complete and
/// non-null for a key frame and for a raw explicit frame alike; only an evaluated row has none, and
/// it is a null entry rather than an empty key. A null and `b""` stay distinct, because the second is
/// a key a producer could hold and the first is the absence of one.
fn binary_array(rows: &[FrameRow], shape: FrameShape) -> Result<BinaryArray> {
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    let mut total = 0usize;
    let mut valid = Vec::with_capacity(rows.len());
    offsets.push(0);
    for row in rows {
        match (&row.key, shape) {
            (Some(key), _) => {
                total = add_checked(total, key.len())?;
                valid.push(true);
            }
            (None, shape) if shape.content == FrameContent::Raw => {
                // `RawSeriesChunk` still carries the key it was read under, and an explicit frame
                // only adds the decoded labels beside it. Dropping the key would lose the identity
                // a consumer of the raw chunk needs.
                return invalid_series("a raw frame row carries no key");
            }
            (None, _) => valid.push(false),
        }
        offsets.push(checked_offset(total)?);
    }
    let mut values = Vec::with_capacity(total);
    for row in rows {
        if let Some(key) = &row.key {
            values.extend_from_slice(key.as_slice());
        }
    }
    let nulls = if valid.iter().all(|valid| *valid) {
        None
    } else {
        validity(&valid)
    };
    arrow(BinaryArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Buffer::from_vec(values),
        nulls,
    ))
}

/// Per-row charge for key/time/nullable overhead shared by every row.
const ROW_CHARGE: usize = 64;
/// Per-span charge for the two 64-bit coordinates one grid span carries.
const SPAN_CHARGE: usize = 16;
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
///
/// The decoder is built from the schema a consumer expects and then checks every batch against it:
/// the reserved declarations are compared as text, the column layout as Arrow fields, and every row
/// on its own. A batch that disagrees anywhere is an error rather than a partial decode, because a
/// receiver has no way to tell which half of a contradictory frame is the true one.
pub struct SeriesFrameDecoder {
    schema: SeriesSchema,
    shape: FrameShape,
    expected: SchemaRef,
}

impl SeriesFrameDecoder {
    /// Builds the expected frame schema once from the series schema and identity declaration.
    pub fn try_new(
        schema: &SeriesSchema,
        labels: FrameLabelMode,
        decode: Option<&LabelDecodeSpec>,
    ) -> Result<Self> {
        Ok(Self {
            schema: schema.clone(),
            shape: FrameShape::of(schema, labels),
            expected: series_frame_schema(schema, labels, decode)?,
        })
    }

    /// Decodes all rows of one batch as raw chunks.
    pub fn decode_raw(&self, batch: &RecordBatch) -> Result<Vec<RawSeriesChunk>> {
        if self.shape.content != FrameContent::Raw {
            return invalid_series("a raw frame decoder cannot read an evaluated frame");
        }
        let rows = self.decode_rows(batch)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let RowTime::Timestamps(timestamps) = row.time else {
                return invalid_series("a raw frame row carries grid positions");
            };
            let key = row
                .key
                .ok_or_else(|| err("a raw frame row carries no key"))?;
            let time = RawTimestamps::try_new(timestamps)?;
            let fields = SeriesFields::try_from_vec(row.samples)?;
            out.push(RawSeriesChunk::try_new(
                &self.schema,
                key,
                row.metric,
                time,
                fields,
                row.is_last,
            )?);
        }
        Ok(out)
    }

    /// Decodes all rows of one batch as evaluated series and their finality.
    ///
    /// The pair is what a compute consumer needs and nothing more: the values on their absolute grid
    /// positions, and whether this chunk closed the chain. A row that carries no position is kept, so
    /// a closing chunk with no value still reaches the consumer that has to see it.
    pub fn decode_evaluated(&self, batch: &RecordBatch) -> Result<Vec<(Series, bool)>> {
        if self.shape.content != FrameContent::Evaluated {
            return invalid_series("an evaluated frame decoder cannot read a raw frame");
        }
        let grid = self
            .schema
            .eval_grid
            .clone()
            .ok_or_else(|| err("an evaluated frame needs an evaluation grid"))?;
        let rows = self.decode_rows(batch)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let RowTime::Spans(spans) = row.time else {
                return invalid_series("an evaluated frame row carries timestamps");
            };
            let metric = row
                .metric
                .ok_or_else(|| err("an evaluated frame row carries no labels"))?;
            let time = TimeAxis::from_grid_spans(grid.clone(), spans)?;
            // A row that covers no position is the closing chunk's shape and nothing else. The
            // streaming validator refuses one that closes nothing, but this decoder is public and a
            // caller driving it directly must not be handed a chunk it could never have produced
            // either: an empty chunk that opens a chain carries no value and no finality, so no
            // consumer could act on it.
            if time.is_empty() && !row.is_last {
                return invalid_series(
                    "an evaluated chunk without positions must close its series",
                );
            }
            let fields = SeriesFields::try_from_vec(row.samples)?;
            out.push((
                Series::try_new(&self.schema, metric, time, fields)?,
                row.is_last,
            ));
        }
        Ok(out)
    }

    /// Decodes all rows of one batch, checking its reserved metadata, declared fields, and rows.
    fn decode_rows(&self, batch: &RecordBatch) -> Result<Vec<FrameRow>> {
        let actual = batch.schema();
        validate_ordering(actual.as_ref(), self.shape.content)?;
        for key in [LAYOUT, CONTENT, LABELS, TIME, GRID, LABEL_DECODE] {
            if actual.metadata().get(key) != self.expected.metadata().get(key) {
                return invalid_series("frame metadata mismatch");
            }
        }
        // The same reservation rule the declaration parser applies, because a frame's protocol
        // legality must not depend on which entry point reads it: a reserved key nobody checks would
        // travel as dead metadata here while being refused a layer up.
        for key in actual.metadata().keys() {
            if key.starts_with("greptime.series.") && !RESERVED_KEYS.contains(&key.as_str()) {
                return invalid_series("frame declares unknown reserved metadata");
            }
        }
        if actual.fields() != self.expected.fields() {
            return invalid_series("frame schema mismatch");
        }
        let cols = batch.columns();
        let keys = down::<BinaryArray>(&cols[0], "read_key")?;
        let lasts = down::<BooleanArray>(&cols[1], "is_last")?;
        let time_struct = down::<StructArray>(&cols[2], "time")?;
        let fs = down::<StructArray>(&cols[3], "fields")?;
        let (names, labels) = if self.shape.labels == FrameLabelMode::Explicit {
            (
                Some(down::<StringArray>(&cols[4], "name")?),
                Some(down::<ListArray>(&cols[5], "labels")?),
            )
        } else {
            (None, None)
        };
        let mut out = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if lasts.is_null(row) || time_struct.is_null(row) || fs.is_null(row) {
                return invalid_series("null required frame parent");
            }
            match self.shape.labels {
                FrameLabelMode::Key => {
                    if keys.is_null(row) {
                        return invalid_series("null required frame parent");
                    }
                }
                // An explicit row names its series by label; the key column is the raw chunk's own
                // contract, and an evaluated row has none at all.
                FrameLabelMode::Explicit => {
                    if self.shape.content == FrameContent::Raw && keys.is_null(row) {
                        return invalid_series("a raw explicit frame row carries no key");
                    }
                    if self.shape.content == FrameContent::Evaluated && !keys.is_null(row) {
                        return invalid_series("an evaluated frame row carries a storage key");
                    }
                }
            }
            let time = match self.shape.content {
                FrameContent::Raw => {
                    let times = down::<ListArray>(time_struct.column(0), "timestamps")?;
                    if times.is_null(row) {
                        return invalid_series("null required raw frame parent");
                    }
                    let values = times.value(row);
                    let tv = down::<Int64Array>(&values, "timestamp values")?;
                    if tv.null_count() != 0 {
                        return invalid_series("null timestamp");
                    }
                    RowTime::Timestamps(tv.values().clone())
                }
                FrameContent::Evaluated => {
                    let typed = down::<ListArray>(time_struct.column(0), "spans")?;
                    if typed.is_null(row) {
                        return invalid_series("null required evaluated frame parent");
                    }
                    let values = typed.value(row);
                    let span_struct = down::<StructArray>(&values, "spans")?;
                    if span_struct.null_count() != 0 {
                        return invalid_series("null grid span");
                    }
                    let starts = down::<UInt64Array>(span_struct.column(0), "span starts")?;
                    let lengths = down::<UInt64Array>(span_struct.column(1), "span lengths")?;
                    if starts.null_count() != 0 || lengths.null_count() != 0 {
                        return invalid_series("null grid span bound");
                    }
                    let mut spans = Vec::with_capacity(starts.len());
                    for index in 0..starts.len() {
                        let start = usize::try_from(starts.value(index))
                            .map_err(|_| err("grid span start exceeds the supported range"))?;
                        let len = usize::try_from(lengths.value(index))
                            .map_err(|_| err("grid span length exceeds the supported range"))?;
                        if len == 0 {
                            return invalid_series("grid spans must not be empty");
                        }
                        spans.push(GridSpan::new(start, len));
                    }
                    RowTime::Spans(spans)
                }
            };
            let positions = time.len()?;
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
                    Some(SeriesLabels::try_new(
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
                if has_float && floats.len() != positions {
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
                    let indices =
                        down::<UInt32Array>(&inds.value(row), "histogram indices")?.clone();
                    if indices.len() != positions {
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
            let key = if keys.is_null(row) {
                None
            } else {
                Some(binary_slice(keys, row)?)
            };
            out.push(FrameRow {
                key,
                metric,
                is_last: lasts.value(row),
                time,
                samples: decoded,
                bytes: 0,
            });
        }
        Ok(out)
    }
}

/// Decodes all rows of one raw frame; any malformed row makes the entire call fail.
pub fn unpack_raw_series_frame(
    schema: &SeriesSchema,
    mode: FrameLabelMode,
    decode: Option<&LabelDecodeSpec>,
    batch: &RecordBatch,
) -> Result<Vec<RawSeriesChunk>> {
    SeriesFrameDecoder::try_new(schema, mode, decode)?.decode_raw(batch)
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
