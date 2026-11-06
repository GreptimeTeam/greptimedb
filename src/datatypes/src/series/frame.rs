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
//! schema per stream through [`SeriesFrameDecoder`]. The column layout and existing metadata stay
//! on the `v2` contract. Raw frames may also declare `greptime.series.ordering=interleaved`; older
//! readers that do not recognize this ordering reject it. A key-encoded frame declares a versioned
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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, StringArray,
    StructArray, UInt32Array, UInt64Array,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::{concat, take};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use serde::{Deserialize, Serialize};

use crate::data_type::ConcreteDataType;
use crate::error::{InvalidSeriesSnafu, Result};
use crate::series::aggregate_state::{
    AggregateStateSchema, AggregateStateValues, StateComponent, StateFieldsBatch, StateSeries,
};
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
/// Metadata key naming the aggregate whose accumulator a state frame carries.
const AGGREGATE_KIND: &str = "greptime.series.aggregate.kind";
/// Metadata key listing the accumulator components a state frame carries, in column order.
const AGGREGATE_COMPONENTS: &str = "greptime.series.aggregate.components";

/// Every `greptime.series.*` key a frame this revision writes may carry.
///
/// Frame schemas carry an explicit `contiguous` default; streaming producers may replace it with
/// their stronger guarantee.
const RESERVED_KEYS: [&str; 9] = [
    LAYOUT,
    CONTENT,
    LABELS,
    TIME,
    GRID,
    LABEL_DECODE,
    ORDERING,
    AGGREGATE_KIND,
    AGGREGATE_COMPONENTS,
];

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

/// What one frame's rows hold.
///
/// The content decides the row layout, the time mode and the payload columns together, so a frame
/// states it once and every other declaration is derived from it rather than chosen separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameContent {
    /// Samples read from storage, each row carrying the timestamps it was read at.
    Raw,
    /// Values computed on an evaluation grid, each row carrying the grid positions it holds.
    Evaluated,
    /// Partial aggregate accumulator state, addressed on the grid the frame declares.
    ///
    /// A state row is not a query value: it names a group and the positions it has contributions
    /// at, and its payload carries the accumulator components that aggregate declared. It is
    /// addressed like an evaluated row because a group's state lives at the same grid positions a
    /// value would, but the two are not interchangeable — a consumer that folds values must refuse
    /// state, and one that merges state must refuse values.
    AggregateState,
}

impl FrameContent {
    /// The `greptime.series.content` text of this content.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Evaluated => "evaluated",
            Self::AggregateState => "aggregate_state",
        }
    }

    /// Parses a `greptime.series.content` value this revision implements.
    const fn parse(text: &str) -> Option<Self> {
        match text.as_bytes() {
            b"raw" => Some(Self::Raw),
            b"evaluated" => Some(Self::Evaluated),
            b"aggregate_state" => Some(Self::AggregateState),
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
            Self::Evaluated | Self::AggregateState => "grid_spans",
        }
    }

    /// The struct member a row's time column carries for this content.
    const fn time_member(self) -> &'static str {
        match self {
            Self::Raw => "timestamps",
            Self::Evaluated | Self::AggregateState => "spans",
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
        FrameContent::Evaluated | FrameContent::AggregateState => DataType::Struct(
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

/// Builds an aggregate state frame schema with explicit labels and a declared grid.
pub fn aggregate_state_frame_schema(
    state: &AggregateStateSchema,
    schema: &SeriesSchema,
    labels: FrameLabelMode,
) -> Result<SchemaRef> {
    if schema.eval_grid.is_none() {
        return invalid_series("an aggregate state frame needs an evaluation grid");
    }
    if labels != FrameLabelMode::Explicit {
        return invalid_series("an aggregate state frame requires explicit labels");
    }
    let components: Fields = state
        .components
        .iter()
        .map(|component| {
            let (ty, nullable) = match component {
                StateComponent::IncrementalMean => (DataType::Boolean, true),
                _ => (DataType::Float64, true),
            };
            Field::new(component.as_str(), ty, nullable)
        })
        .collect();
    let member = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(components),
        true,
    )));
    let fields: Fields = schema
        .fields
        .iter()
        .map(|field| Field::new(field.name.as_ref(), member.clone(), false))
        .collect();
    let mut ordinary = series_frame_schema(schema, labels, None)?;
    let mut metadata = ordinary.metadata().clone();
    metadata.insert(
        CONTENT.to_string(),
        FrameContent::AggregateState.as_str().to_string(),
    );
    metadata.insert(
        TIME.to_string(),
        FrameContent::AggregateState.time_text().to_string(),
    );
    metadata.insert(AGGREGATE_KIND.to_string(), state.kind.as_str().to_string());
    metadata.insert(AGGREGATE_COMPONENTS.to_string(), state.components_text());
    let mut columns = ordinary.fields().to_vec();
    columns[3] = Field::new("fields", DataType::Struct(fields), false).into();
    ordinary = Arc::new(Schema::new_with_metadata(columns, metadata));
    Ok(ordinary)
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
    content: FrameContent,
    labels: FrameLabelMode,
    label_decode: Option<LabelDecodeSpec>,
    aggregate_state: Option<crate::series::aggregate_state::AggregateStateSchema>,
}

fn validate_ordering(schema: &Schema, content: FrameContent) -> Result<()> {
    match schema.metadata().get(ORDERING).map(String::as_str) {
        Some("contiguous") => Ok(()),
        Some("read_key_then_time" | "interleaved") if content == FrameContent::Raw => Ok(()),
        Some("read_key_then_time") => Err(err(
            "an evaluated frame cannot claim key-then-time ordering",
        )),
        Some("interleaved") => Err(err("an evaluated frame cannot claim interleaved ordering")),
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
            (FrameContent::AggregateState, None) => {
                return Err(err(
                    "an aggregate state frame must declare its evaluation grid",
                ));
            }
            (FrameContent::AggregateState, Some(text)) => Some(parse_grid(text)?),
        };
        let has_kind = schema.metadata().contains_key(AGGREGATE_KIND);
        let has_components = schema.metadata().contains_key(AGGREGATE_COMPONENTS);
        let aggregate_state = match content {
            FrameContent::AggregateState => {
                if !has_kind || !has_components {
                    return Err(err(
                        "an aggregate state frame must declare kind and components",
                    ));
                }
                if schema.metadata().contains_key(LABEL_DECODE) {
                    return Err(err(
                        "an aggregate state frame cannot carry a label decode description",
                    ));
                }
                Some(AggregateStateSchema::from_text(
                    schema.metadata()[AGGREGATE_KIND].as_str(),
                    schema.metadata()[AGGREGATE_COMPONENTS].as_str(),
                )?)
            }
            FrameContent::Raw | FrameContent::Evaluated => {
                if has_kind || has_components {
                    return Err(err(
                        "raw or evaluated frames cannot carry aggregate state metadata",
                    ));
                }
                None
            }
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
            content,
            aggregate_state,
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
        self.content
    }

    /// The validated accumulator kind and component layout, present only on state frames.
    pub fn aggregate_state(&self) -> Option<&AggregateStateSchema> {
        self.aggregate_state.as_ref()
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

    fn aggregate_state(labels: FrameLabelMode) -> Self {
        Self {
            content: FrameContent::AggregateState,
            labels,
        }
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

    /// Creates a bounded builder for validated aggregate state rows.
    pub fn try_new_aggregate_state(
        state: &AggregateStateSchema,
        schema: &SeriesSchema,
        labels: FrameLabelMode,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self> {
        if max_rows == 0 || max_bytes == 0 {
            return invalid_series("series frame limits must be nonzero");
        }
        Ok(Self {
            schema: schema.clone(),
            frame_schema: aggregate_state_frame_schema(state, schema, labels)?,
            shape: FrameShape::aggregate_state(labels),
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

    /// Bytes available to an evaluated row that is being assembled before cursor advancement.
    fn available_bytes(&self) -> Option<usize> {
        if self.rows.len() >= self.max_rows || self.bytes >= self.max_bytes {
            None
        } else {
            Some(self.max_bytes - self.bytes)
        }
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
    State(StateWalk),
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

struct StateWalk {
    series: StateSeries,
    is_last: bool,
    runs: Vec<GridRun>,
    done: bool,
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

    /// Validates and walks one aggregate state chunk.
    pub fn try_new_aggregate_state(
        state: &AggregateStateSchema,
        schema: &SeriesSchema,
        series: StateSeries,
        is_last: bool,
    ) -> Result<Self> {
        if schema.eval_grid.is_none() {
            return invalid_series("an aggregate state chunk needs an evaluation grid");
        }
        if series.states.len() != schema.fields.len()
            || series
                .states
                .iter()
                .any(|field| field.len() != series.time.len())
        {
            return invalid_series("aggregate state field shape mismatch");
        }
        if series.time.grid().map(|grid| grid.spec())
            != schema.eval_grid.as_ref().map(|grid| grid.spec())
        {
            return invalid_series("aggregate state chunk grid does not match schema");
        }
        let mut states = series.states;
        // A position is carried when some field of it was folded, which is what the axis says. When
        // every position is carried - the shape a fold produces whenever each group has a sample at
        // each of its points - the axis and the states are already the ones this walk reads, so
        // nothing is rebuilt. Deciding that first keeps the common case free of a mask, a position
        // list and a second axis.
        let every_position_carried = state.kind
            == crate::series::aggregate_state::AggregateStateKind::Group
            || (0..series.time.len()).all(|row| states.iter().any(|field| field[row].is_some()));
        let time = if every_position_carried {
            series.time
        } else {
            let contributing = (0..series.time.len())
                .map(|row| states.iter().any(|field| field[row].is_some()))
                .collect::<Vec<_>>();
            let carried = series
                .time
                .positions()
                .filter_map(|(position, row)| contributing[row].then_some(position))
                .collect::<Vec<_>>();
            let spans = positions_to_spans(&carried);
            for field in &mut states {
                let mut row = 0;
                field.retain(|_| {
                    let keep = contributing[row];
                    row += 1;
                    keep
                });
            }
            TimeAxis::from_grid_spans(series.time.grid().expect("validated grid").clone(), spans)?
        };
        let runs: Vec<GridRun> = time.runs().collect();
        let series = StateSeries {
            metric: series.metric,
            time,
            states,
        };
        Ok(Self {
            schema: schema.clone(),
            shape: FrameShape::aggregate_state(FrameLabelMode::Explicit),
            walk: ChunkWalk::State(StateWalk {
                series,
                is_last,
                runs,
                done: false,
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
                None => match self.next_row(builder.available_bytes())? {
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
    fn next_row(&mut self, available_bytes: Option<usize>) -> Result<Option<FrameRow>> {
        match &mut self.walk {
            ChunkWalk::Raw(walk) => walk.next_row(),
            ChunkWalk::Evaluated(walk) => walk.next_row(available_bytes),
            ChunkWalk::State(walk) => walk.next_row(),
        }
    }
}

impl StateWalk {
    fn next_row(&mut self) -> Result<Option<FrameRow>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let states = std::mem::take(&mut self.series.states);
        let time = RowTime::Spans(
            self.runs
                .iter()
                .map(|run| GridSpan::new(run.position, run.len))
                .collect(),
        );
        let mut row = FrameRow {
            key: None,
            metric: Some(self.series.metric.clone()),
            is_last: self.is_last,
            time,
            payload: RowPayload::States(states),
            bytes: 0,
        };
        row.bytes = measure_row(&row)?;
        Ok(Some(row))
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
            payload: RowPayload::Samples(samples),
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
    /// A row may cover several run/segment intersections, but never crosses a dense segment
    /// boundary. Grid holes are represented by separate spans while the sample slice stays
    /// contiguous. Cursor state is committed only after the complete row has been measured.
    fn next_row(&mut self, available_bytes: Option<usize>) -> Result<Option<FrameRow>> {
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
                payload: RowPayload::Samples(samples),
                bytes: 0,
            };
            row.bytes = measure_row(&row)?;
            *closing = false;
            return Ok(Some(row));
        }
        let Some(_) = runs.get(*run) else {
            return Ok(None);
        };
        let mut next_run = *run;
        let mut next_offset = *offset;
        let mut next_segments = segments.clone();
        let mut next_offsets = offsets.clone();
        let mut total = 0usize;
        let mut row: Option<FrameRow> = None;
        while let Some(current) = runs.get(next_run) {
            let remaining = current.len - next_offset;
            let mut take = remaining;
            let mut segment_remaining_lengths = Vec::with_capacity(series.fields.len());
            for (index, field) in series.fields.iter().enumerate() {
                let part = segment(field, next_segments[index])?;
                let segment_remaining = part.len().saturating_sub(next_offsets[index]);
                segment_remaining_lengths.push(segment_remaining);
                take = take.min(segment_remaining);
            }
            let reaches_segment_boundary = segment_remaining_lengths.contains(&take);
            if take == 0 {
                return invalid_series("empty sample segment");
            }
            let proposed_total = add_checked(total, take)?;
            let sample_growth = sample_range_growth(&series.fields, segments, total, take)?;
            if let Some(row) = &mut row {
                let RowTime::Spans(row_spans) = &mut row.time else {
                    unreachable!("evaluated row has spans")
                };
                let proposed_span_count = row_spans
                    .len()
                    .checked_add(1)
                    .ok_or_else(|| err("raw series frame offset overflow"))?;
                checked_offset(proposed_span_count)?;
                checked_offset(proposed_total)?;
                let proposed_bytes = add_checked(
                    add_checked(row.bytes, add_checked(SPAN_CHARGE, 2)?)?,
                    sample_growth,
                )?;
                if available_bytes.is_some_and(|available| proposed_bytes > available) {
                    break;
                }
                row_spans.push(GridSpan::new(current.position + next_offset, take));
                row.is_last = *is_last && next_run + 1 == runs.len() && take == remaining;
                row.bytes = proposed_bytes;
            } else {
                let mut candidate = FrameRow {
                    key: None,
                    metric: Some(series.metric.clone()),
                    is_last: *is_last && next_run + 1 == runs.len() && take == remaining,
                    time: RowTime::Spans(vec![GridSpan::new(current.position + next_offset, take)]),
                    payload: RowPayload::Samples(Vec::new()),
                    bytes: 0,
                };
                candidate.bytes = add_checked(measure_row(&candidate)?, sample_growth)?;
                row = Some(candidate);
            }
            total = proposed_total;
            next_offset += take;
            if next_offset == current.len {
                next_run += 1;
                next_offset = 0;
            }
            for (index, field) in series.fields.iter().enumerate() {
                let length = segment(field, next_segments[index])?.len();
                next_offsets[index] += take;
                if next_offsets[index] == length {
                    next_segments[index] += 1;
                    next_offsets[index] = 0;
                }
            }
            // An individually oversized atomic intersection must be emitted alone.
            if available_bytes.is_none()
                || available_bytes.is_some_and(|available| row.as_ref().unwrap().bytes > available)
                || reaches_segment_boundary
            {
                break;
            }
        }
        if let Some(row) = &mut row {
            row.payload = RowPayload::Samples(
                series
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        segment(field, segments[index])?.slice(offsets[index], total)
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        *run = next_run;
        *offset = next_offset;
        segments.clone_from(&next_segments);
        offsets.clone_from(&next_offsets);
        Ok(row)
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
    payload: RowPayload,
    bytes: usize,
}

enum RowPayload {
    Samples(Vec<Samples>),
    States(Vec<Vec<Option<AggregateStateValues>>>),
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

/// Measures the added dense positions without slicing Arrow arrays or rescanning histogram tables.
fn sample_range_growth(
    fields: &SeriesFields,
    segments: &[usize],
    previous: usize,
    added: usize,
) -> Result<usize> {
    let end = previous
        .checked_add(added)
        .ok_or_else(|| err("raw series frame offset overflow"))?;
    checked_offset(end)?;
    let bitmap_growth = bitmap_bytes(end).saturating_sub(bitmap_bytes(previous));
    fields
        .iter()
        .enumerate()
        .try_fold(0usize, |bytes, (index, field)| {
            let part = segment(field, segments[index])?;
            let (value_width, bitmap_count, table_charge) = match part
                .dense()
                .ok_or_else(|| err("sample segment is not dense"))?
            {
                DenseSamplesRef::Float(_) => (8, 1, None),
                DenseSamplesRef::Histogram(values) => (4, 1, Some(values)),
                DenseSamplesRef::Mixed(values) => (12, 2, Some(values.histograms())),
            };
            let mut charge = mul_checked(added, value_width)?;
            charge = add_checked(charge, mul_checked(bitmap_growth, bitmap_count)?)?;
            if previous == 0 {
                charge = add_checked(charge, FIELD_CHARGE)?;
                if let Some(values) = table_charge {
                    charge = add_checked(charge, histogram_table_charge(values)?)?;
                }
            }
            add_checked(bytes, charge)
        })
}

fn histogram_table_charge(values: &HistogramSamples) -> Result<usize> {
    let table = values.batch().as_arrow();
    checked_offset(table.len())?;
    let mut bytes = add_checked(
        HISTOGRAM_TABLE_CHARGE,
        mul_checked(table.len(), HISTOGRAM_ROW_CHARGE)?,
    )?;
    for (position, width) in HISTOGRAM_LIST_WIDTHS.iter().enumerate() {
        let list = table
            .column(7 + position)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| err("histogram table schema mismatch"))?;
        // Charge the visible offset range, not the backing child array: the table may be sliced.
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

fn state_charges(fields: &[Vec<Option<AggregateStateValues>>]) -> Result<usize> {
    fields.iter().try_fold(0usize, |bytes, values| {
        let mut charge = FIELD_CHARGE;
        for _ in values.iter().flatten() {
            charge = add_checked(charge, 8 * 4 + 1)?;
        }
        add_checked(bytes, charge)
    })
}

fn sample_charges(samples: &[Samples]) -> Result<usize> {
    samples.iter().try_fold(0usize, |bytes, sample| {
        add_checked(bytes, sample_charge(sample)?)
    })
}

fn sample_charge(sample: &Samples) -> Result<usize> {
    let mut bytes = FIELD_CHARGE;
    match sample
        .dense()
        .ok_or_else(|| err("sample segment is not dense"))?
    {
        DenseSamplesRef::Float(values) => add_checked(bytes, float_charge(values.len())?),
        DenseSamplesRef::Histogram(values) => add_checked(bytes, histogram_charge(values)?),
        DenseSamplesRef::Mixed(values) => {
            bytes = add_checked(bytes, float_charge(values.floats().len())?)?;
            add_checked(bytes, histogram_charge(values.histograms())?)
        }
    }
}

fn measure_row(row: &FrameRow) -> Result<usize> {
    let time_len = row.time.len()?;
    let key_len = row.key.as_ref().map(Buffer::len).unwrap_or(0);
    checked_offset(key_len)?;
    checked_offset(time_len)?;
    let mut bytes = add_checked(ROW_CHARGE, key_len)?;
    bytes = match &row.time {
        RowTime::Timestamps(timestamps) => add_checked(bytes, mul_checked(timestamps.len(), 8)?)?,
        // Two `u64` per span, plus the list's own offsets and validity.
        RowTime::Spans(spans) => add_checked(
            add_checked(bytes, mul_checked(spans.len(), SPAN_CHARGE)?)?,
            mul_checked(spans.len(), 2)?,
        )?,
    };
    bytes = add_checked(
        bytes,
        match &row.payload {
            RowPayload::Samples(samples) => sample_charges(samples)?,
            RowPayload::States(fields) => state_charges(fields)?,
        },
    )?;
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
    add_checked(
        add_checked(mul_checked(indices.len(), 4)?, bitmap_bytes(indices.len()))?,
        histogram_table_charge(values)?,
    )
}

fn compact_histogram(values: &HistogramSamples) -> Result<(ArrayRef, UInt32Array)> {
    let indices = values.indices();
    // One encoded entry per *distinct* referenced row: a segment that references the same row many
    // times would otherwise copy that row once per reference, so a small table plus a long run of
    // equal indices would still grow the payload with every position.
    let mut carried: HashMap<u32, u32> = HashMap::new();
    let mut selected = Vec::new();
    let mut remapped = Vec::with_capacity(indices.len());
    for index in 0..indices.len() {
        if indices.is_valid(index) {
            let original = indices.value(index);
            let local_index = match carried.get(&original) {
                Some(local_index) => *local_index,
                None => {
                    let local_index = u32::try_from(selected.len())
                        .map_err(|_| err("too many referenced histograms in one frame segment"))?;
                    selected.push(original);
                    carried.insert(original, local_index);
                    local_index
                }
            };
            remapped.push(Some(local_index));
        } else {
            remapped.push(None);
        }
    }
    let selected = UInt32Array::from(selected);
    let table = arrow(take(values.batch().as_arrow(), &selected, None))?;
    Ok((table, UInt32Array::from(remapped)))
}

/// Encodes buffered rows as one batch with direct N-row Arrow array construction.
///
/// Each histogram segment carries each *distinct* table row it references exactly once, and its
/// local indices are remapped into that compact table, so the payload grows with the rows a segment
/// actually names rather than with its positions. A single constituent child array is reused by
/// clone (sharing its buffers); multiple constituents are concatenated, which copies.
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
        FrameContent::Evaluated | FrameContent::AggregateState => {
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

    if shape.content == FrameContent::AggregateState {
        // A state frame's per-field list carries one item per grid position the row holds, and an
        // item is absent exactly where that field has no contribution at that position. The
        // absence lives on the item — the child struct's validity — not on the list's own
        // validity, which is per row: a row always has a list, with zero or more items in it.
        let mut field_arrays: Vec<ArrayRef> = Vec::with_capacity(field_types.len());
        for (index, field) in field_types.iter().enumerate() {
            let mut lengths = Vec::with_capacity(count);
            let mut positions = 0usize;
            for row in rows {
                let RowPayload::States(fields) = &row.payload else {
                    return invalid_series("state frame row carries sample payload");
                };
                let values = fields
                    .get(index)
                    .ok_or_else(|| err("state field count mismatch"))?;
                if values.len() != row.time.len()? {
                    return invalid_series("state list length mismatch");
                }
                lengths.push(values.len());
                positions = add_checked(positions, values.len())?;
            }

            let item = match field.data_type() {
                DataType::List(item) => item,
                _ => return invalid_series("state field schema mismatch"),
            };
            let DataType::Struct(components) = item.data_type() else {
                return invalid_series("state list item is not a component struct");
            };
            // Absence lives on the item, so the child struct's validity is one flag per position.
            // A row of a state frame always carries states; a caller that hands in the wrong payload
            // is refused here rather than panicking inside an iterator.
            let mut present = Vec::with_capacity(positions);
            for row in rows {
                let RowPayload::States(fields) = &row.payload else {
                    return invalid_series("state frame row carries sample payload");
                };
                present.extend(fields[index].iter().map(Option::is_some));
            }
            debug_assert_eq!(present.len(), positions);

            // Every component of a field is absent at exactly the same positions - the field's own -
            // so they share the one mask, which the child struct already needs.
            let nulls = present
                .iter()
                .any(|present| !present)
                .then(|| NullBuffer::from(present.clone()));
            let arrays: Vec<ArrayRef> = components
                .iter()
                .map(|component| {
                    let component_name = component.name();
                    let array: ArrayRef = match component_name.as_str() {
                        "value" | "mean" | "compensation" | "count" => {
                            let mut floats = Vec::with_capacity(positions);
                            for row in rows {
                                let RowPayload::States(fields) = &row.payload else {
                                    return invalid_series(
                                        "state frame row carries sample payload",
                                    );
                                };
                                for value in &fields[index] {
                                    // A missing position's value is unreadable, so it holds the same
                                    // zero a null slot has always held here.
                                    floats.push(value.map_or(0.0, |state| {
                                        match component_name.as_str() {
                                            "value" => state.value,
                                            "mean" => state.mean,
                                            "compensation" => state.compensation,
                                            "count" => state.count,
                                            _ => unreachable!(),
                                        }
                                    }));
                                }
                            }
                            Arc::new(Float64Array::new(ScalarBuffer::from(floats), nulls.clone()))
                        }
                        "incremental_mean" => {
                            let mut booleans = Vec::with_capacity(positions);
                            for row in rows {
                                let RowPayload::States(fields) = &row.payload else {
                                    return invalid_series(
                                        "state frame row carries sample payload",
                                    );
                                };
                                booleans.extend(fields[index].iter().map(|value| {
                                    value.is_some_and(|state| state.incremental_mean)
                                }));
                            }
                            Arc::new(BooleanArray::new(
                                BooleanBuffer::from(booleans),
                                nulls.clone(),
                            ))
                        }
                        _ => return invalid_series("unknown state component"),
                    };
                    Ok(array)
                })
                .collect::<Result<Vec<_>>>()?;

            let child = if components.is_empty() {
                Arc::new(StructArray::new_empty_fields(
                    positions,
                    Some(NullBuffer::from(present)),
                )) as ArrayRef
            } else {
                Arc::new(
                    StructArray::try_new(
                        components.clone(),
                        arrays,
                        Some(NullBuffer::from(present)),
                    )
                    .map_err(|error| err(error.to_string()))?,
                ) as ArrayRef
            };
            // The list itself is valid for every row: absence is an absent item, never an absent
            // list, so a reader can tell "this field holds nothing here" from "this row has no
            // list at all".
            let list = list_array(&lengths, child, item.as_ref().clone(), None)?;
            field_arrays.push(Arc::new(list) as ArrayRef);
        }
        let fields = struct_array(field_types.clone(), field_arrays)?;
        return assemble_frame_batch(schema, shape, rows, time, fields);
    }
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
            let dense = match &row.payload {
                RowPayload::Samples(samples) => samples.get(index),
                _ => return invalid_series("sample frame row carries state payload"),
            }
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
                    let (table, indices) = compact_histogram(values)?;
                    index_lengths.push(indices.len());
                    index_parts.push(Arc::new(indices));
                    table_lengths.push(table.len());
                    table_parts.push(table);
                    histogram_valid.push(true);
                }
                DenseSamplesRef::Mixed(values) => {
                    float_lengths.push(values.floats().len());
                    float_parts.push(Arc::new(values.floats().clone()));
                    float_valid.push(true);
                    let (table, indices) = compact_histogram(values.histograms())?;
                    index_lengths.push(indices.len());
                    index_parts.push(Arc::new(indices));
                    table_lengths.push(table.len());
                    table_parts.push(table);
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
    assemble_frame_batch(schema, shape, rows, time, fields)
}

/// Builds the batch from the parts every content shares: the key, finality, time and field
/// columns, plus the labels an explicit frame carries.
///
/// Keeping the assembly in one place is what makes a state row and an evaluated row leave the
/// encoder under the same column order and the same options; only how their field columns were
/// built differs, and that is decided before this call.
fn assemble_frame_batch(
    schema: &SchemaRef,
    shape: FrameShape,
    rows: &[FrameRow],
    time: StructArray,
    fields: StructArray,
) -> Result<RecordBatch> {
    let count = rows.len();
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

fn positions_to_spans(positions: &[usize]) -> Vec<GridSpan> {
    let mut spans: Vec<GridSpan> = Vec::new();
    for position in positions {
        if let Some(last) = spans.last_mut()
            && last.start() + last.len() == *position
        {
            *last = GridSpan::new(last.start(), last.len() + 1);
            continue;
        }
        spans.push(GridSpan::new(*position, 1));
    }
    spans
}

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

/// One decoded state row retaining Arrow handles for its per-field state columns.
///
/// The arrays remain columnar so consumers can read typed components in place rather than building
/// a nested accumulator tree. Null list items remain field-local absence, not numeric zero.
pub struct StateFrameRow {
    metric: SeriesLabels,
    time: TimeAxis,
    is_last: bool,
    fields: StructArray,
    state: AggregateStateSchema,
}

impl StateFrameRow {
    /// Complete group identity for this row.
    pub fn metric(&self) -> &SeriesLabels {
        &self.metric
    }
    /// Grid positions carried by this row.
    pub fn time(&self) -> &TimeAxis {
        &self.time
    }
    /// Whether this row closes its producer-local chunk chain.
    pub fn is_last(&self) -> bool {
        self.is_last
    }
    /// Columnar state arrays validated against the declared aggregate layout.
    pub fn fields(&self) -> Result<StateFieldsBatch> {
        StateFieldsBatch::try_from_arrow(self.fields.clone(), &self.state)
    }

    /// The export-side form of this row: one group, its carried positions, and its per-field state.
    ///
    /// This is the dual of `decode_aggregate_state`: it reads the row's columnar arrays back into the
    /// form a producer hands to `pack_aggregate_state`. Absence stays per field — a null item is
    /// `None` for that field at that position, never a zero and never promoted to a sibling field.
    pub fn to_state_series(&self) -> Result<StateSeries> {
        let fields = self.fields()?;
        let mut states = Vec::with_capacity(fields.len());
        for field in 0..fields.len() {
            let mut values = Vec::with_capacity(self.time.len());
            for position in 0..self.time.len() {
                if !fields.is_present(field, position)? {
                    values.push(None);
                    continue;
                }
                let mut value = AggregateStateValues::default();
                for &component in fields.components() {
                    match component {
                        StateComponent::Value => {
                            value.value = state_float(&fields, field, component, position)?
                        }
                        StateComponent::Mean => {
                            value.mean = state_float(&fields, field, component, position)?
                        }
                        StateComponent::Compensation => {
                            value.compensation = state_float(&fields, field, component, position)?
                        }
                        StateComponent::Count => {
                            value.count = state_float(&fields, field, component, position)?
                        }
                        StateComponent::IncrementalMean => {
                            value.incremental_mean = fields
                                .field_bool_values(field, component)?
                                .and_then(|array| {
                                    (!array.is_null(position)).then(|| array.value(position))
                                })
                                .ok_or_else(|| {
                                    InvalidSeriesSnafu {
                                        reason: "present aggregate state has a missing component"
                                            .to_string(),
                                    }
                                    .build()
                                })?;
                        }
                    }
                }
                values.push(Some(value));
            }
            states.push(values);
        }
        Ok(StateSeries {
            metric: self.metric.clone(),
            time: self.time.clone(),
            states,
        })
    }
}

fn state_float(
    fields: &StateFieldsBatch,
    field: usize,
    component: StateComponent,
    position: usize,
) -> Result<f64> {
    fields
        .field_values(field, component)?
        .and_then(|array| (!array.is_null(position)).then(|| array.value(position)))
        .ok_or_else(|| {
            InvalidSeriesSnafu {
                reason: "present aggregate state has a missing component".to_string(),
            }
            .build()
        })
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
    aggregate_state: Option<AggregateStateSchema>,
}

impl SeriesFrameDecoder {
    /// Builds the expected frame schema once from the series schema and identity declaration.
    pub fn try_new(
        schema: &SeriesSchema,
        labels: FrameLabelMode,
        decode: Option<&LabelDecodeSpec>,
    ) -> Result<Self> {
        let shape = FrameShape::of(schema, labels);
        Ok(Self {
            schema: schema.clone(),
            shape,
            expected: series_frame_schema(schema, labels, decode)?,
            aggregate_state: None,
        })
    }

    /// Builds a decoder for the declared aggregate state and its grid-addressed series fields.
    pub fn try_new_aggregate_state(
        state: &AggregateStateSchema,
        schema: &SeriesSchema,
        labels: FrameLabelMode,
    ) -> Result<Self> {
        if schema.eval_grid.is_none() {
            return invalid_series("an aggregate state decoder requires an evaluation grid");
        }
        Ok(Self {
            schema: schema.clone(),
            shape: FrameShape::aggregate_state(labels),
            expected: aggregate_state_frame_schema(state, schema, labels)?,
            aggregate_state: Some(state.clone()),
        })
    }

    /// Reads only the explicit labels of each aggregate-state row.
    ///
    /// This validates the declared frame schema and label columns without inspecting time or state.
    pub fn decode_aggregate_state_labels(&self, batch: &RecordBatch) -> Result<Vec<SeriesLabels>> {
        if self.shape.labels != FrameLabelMode::Explicit {
            return invalid_series("aggregate state routing requires explicit labels");
        }
        let cols = batch.columns();
        let names = down::<StringArray>(&cols[4], "name")?;
        let labels = down::<ListArray>(&cols[5], "labels")?;
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if labels.is_null(row) {
                return invalid_series("null required aggregate state label");
            }
            let values = labels.value(row);
            let label_struct = down::<StructArray>(&values, "labels")?;
            if label_struct.null_count() != 0 {
                return invalid_series("null label struct element");
            }
            let label_names = down::<StringArray>(label_struct.column(0), "label names")?;
            let label_values = down::<StringArray>(label_struct.column(1), "label values")?;
            if label_names.null_count() != 0 || label_values.null_count() != 0 {
                return invalid_series("null label element");
            }
            let tags = (0..label_names.len())
                .map(|i| Label {
                    name: Arc::from(label_names.value(i)),
                    value: Arc::from(label_values.value(i)),
                })
                .collect();
            // An aggregation drops the metric name, and the frame says so by leaving it null. That
            // is an identity with no name, not a malformed row.
            let name = (!names.is_null(row)).then(|| Arc::from(names.value(row)));
            rows.push(SeriesLabels::try_new(name, tags)?);
        }
        Ok(rows)
    }

    /// Decodes one aggregate-state row without materializing its accumulator values.
    pub fn decode_aggregate_state(&self, batch: &RecordBatch) -> Result<Vec<StateFrameRow>> {
        let state = self
            .aggregate_state
            .as_ref()
            .ok_or_else(|| err("decoder has no aggregate state declaration"))?;
        if batch.schema().as_ref() != self.expected.as_ref() {
            return invalid_series("aggregate state frame schema mismatch");
        }
        validate_ordering(batch.schema().as_ref(), FrameContent::AggregateState)?;
        let cols = batch.columns();
        let lasts = down::<BooleanArray>(&cols[1], "is_last")?;
        let time = down::<StructArray>(&cols[2], "time")?;
        let fields = down::<StructArray>(&cols[3], "fields")?;
        let names = down::<StringArray>(&cols[4], "name")?;
        let labels = down::<ListArray>(&cols[5], "labels")?;
        let grid = self
            .schema
            .eval_grid
            .clone()
            .ok_or_else(|| err("an aggregate state frame needs an evaluation grid"))?;
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if lasts.is_null(row) || time.is_null(row) || fields.is_null(row) || labels.is_null(row)
            {
                return invalid_series("null required aggregate state parent");
            }
            let spans_array = down::<ListArray>(time.column(0), "spans")?;
            if spans_array.is_null(row) {
                return invalid_series("null aggregate state spans");
            }
            let spans_values = spans_array.value(row);
            let spans_struct = down::<StructArray>(&spans_values, "spans")?;
            if spans_struct.null_count() != 0 {
                return invalid_series("null aggregate state span");
            }
            let starts = down::<UInt64Array>(spans_struct.column(0), "span starts")?;
            let lengths = down::<UInt64Array>(spans_struct.column(1), "span lengths")?;
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
            let axis = TimeAxis::from_grid_spans(grid.clone(), spans)?;
            let label_values = labels.value(row);
            let label_struct = down::<StructArray>(&label_values, "labels")?;
            if label_struct.null_count() != 0 {
                return invalid_series("null label struct element");
            }
            let label_names = down::<StringArray>(label_struct.column(0), "label names")?;
            let label_values = down::<StringArray>(label_struct.column(1), "label values")?;
            if label_names.null_count() != 0 || label_values.null_count() != 0 {
                return invalid_series("null label element");
            }
            let tags = (0..label_names.len())
                .map(|i| Label {
                    name: Arc::from(label_names.value(i)),
                    value: Arc::from(label_values.value(i)),
                })
                .collect();
            let metric = SeriesLabels::try_new(
                (!names.is_null(row)).then(|| Arc::from(names.value(row))),
                tags,
            )?;
            let row_fields = fields.slice(row, 1);
            let validated = StateFieldsBatch::try_from_arrow(row_fields.clone(), state)?;
            if validated.len() != self.schema.fields.len() {
                return invalid_series("aggregate state field count mismatch");
            }
            for field in 0..validated.len() {
                let items = validated
                    .field(field)
                    .ok_or_else(|| err("null state field list"))?;
                if items.len() != axis.len() {
                    return invalid_series("state list length does not match carried positions");
                }
                if state.kind != crate::series::aggregate_state::AggregateStateKind::Group {
                    for component in 0..state.components.len() {
                        let values = items.column(component);
                        for position in 0..items.len() {
                            if !items.is_null(position) && values.is_null(position) {
                                return invalid_series(
                                    "present state item has a missing component",
                                );
                            }
                        }
                    }
                }
            }
            rows.push(StateFrameRow {
                metric,
                time: axis,
                is_last: lasts.value(row),
                fields: row_fields,
                state: state.clone(),
            });
        }
        Ok(rows)
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
            let RowPayload::Samples(samples) = row.payload else {
                return invalid_series("raw frame row carries state payload");
            };
            let fields = SeriesFields::try_from_vec(samples)?;
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
            let RowPayload::Samples(samples) = row.payload else {
                return invalid_series("evaluated frame row carries state payload");
            };
            let fields = SeriesFields::try_from_vec(samples)?;
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
                FrameContent::Evaluated | FrameContent::AggregateState => {
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
                payload: RowPayload::Samples(decoded),
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

#[cfg(test)]
mod aggregate_state_contribution_tests {
    use std::sync::Arc;

    use super::*;
    use crate::series::aggregate_state::{
        AggregateStateKind as Kind, AggregateStateValues as Values, StateComponent as Component,
    };
    use crate::series::samples::FieldSpec;

    fn schema(len: usize) -> SeriesSchema {
        SeriesSchema {
            fields: ["a", "b"]
                .into_iter()
                .map(|name| FieldSpec {
                    name: Arc::from(name),
                })
                .collect::<Vec<_>>()
                .into(),
            eval_grid: Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(0, 1, len as u64).unwrap()).unwrap(),
            )),
        }
    }

    fn cursor(
        kind: Kind,
        schema: &SeriesSchema,
        spans: Vec<GridSpan>,
        states: Vec<Vec<Option<Values>>>,
    ) -> SeriesFrameCursor {
        let components = match kind {
            Kind::Group => vec![],
            _ => vec![Component::Value, Component::Compensation],
        };
        let state = AggregateStateSchema::try_new(kind, components).unwrap();
        SeriesFrameCursor::try_new_aggregate_state(
            &state,
            schema,
            StateSeries {
                metric: SeriesLabels::try_new(None, vec![]).unwrap(),
                time: TimeAxis::from_grid_spans(schema.eval_grid.clone().unwrap(), spans).unwrap(),
                states,
            },
            true,
        )
        .unwrap()
    }

    #[test]
    fn sparse_rows_are_filtered_and_group_keeps_all_rows() {
        let schema = schema(12);
        let sparse = cursor(
            Kind::Sum,
            &schema,
            vec![GridSpan::new(0, 5), GridSpan::new(7, 4)],
            vec![
                vec![
                    None,
                    Some(Values::default()),
                    None,
                    None,
                    Some(Values::default()),
                    None,
                    None,
                    None,
                    None,
                ],
                vec![
                    None,
                    None,
                    None,
                    Some(Values::default()),
                    None,
                    None,
                    Some(Values::default()),
                    None,
                    None,
                ],
            ],
        );
        let ChunkWalk::State(walk) = sparse.walk else {
            panic!("expected state walk")
        };
        assert_eq!(
            walk.series.time.positions().collect::<Vec<_>>(),
            vec![(1, 0), (3, 1), (4, 2), (8, 3)]
        );
        assert_eq!(
            walk.series
                .time
                .runs()
                .map(|run| (run.position, run.len))
                .collect::<Vec<_>>(),
            vec![(1, 1), (3, 2), (8, 1)]
        );
        assert_eq!(
            walk.series.states[0],
            vec![Some(Values::default()), None, Some(Values::default()), None]
        );
        assert_eq!(
            walk.series.states[1],
            vec![None, Some(Values::default()), None, Some(Values::default())]
        );

        let group = cursor(
            Kind::Group,
            &schema,
            vec![GridSpan::new(2, 3), GridSpan::new(8, 2)],
            vec![vec![None; 5], vec![None; 5]],
        );
        let ChunkWalk::State(walk) = group.walk else {
            panic!("expected state walk")
        };
        assert_eq!(
            walk.series.time.positions().collect::<Vec<_>>(),
            vec![(2, 0), (3, 1), (4, 2), (8, 3), (9, 4)]
        );
        assert_eq!(walk.series.states, vec![vec![None; 5], vec![None; 5]]);
    }
}
