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

//! Streaming sample writing with bounded blocks and owned outputs.

use std::sync::Arc;

use async_trait::async_trait;
use datatypes::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, DictionaryArray, Float64Array, StructArray, UInt8Array,
    UInt64Array,
};
use datatypes::arrow::compute::concat;
use datatypes::arrow::datatypes::{DataType, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::DataType as _;
use datatypes::timestamp::timestamp_array_to_primitive;
use mito_codec::row_converter::SparseOffsetsCache;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{FileId, consts};

use crate::error::{ComputeArrowSnafu, Result, SerdeJsonSnafu};
use crate::series_index::artifact::{ArtifactFormat, ArtifactMetadata, ArtifactObject};
use crate::series_index::artifact_writer::Outputs;
use crate::series_index::format::FormatWriter;
use crate::series_index::identity::{ExactSeriesKey, IdentityEncoding, decode_series_id};
use crate::series_index::samples::histogram::ChunkBound;
use crate::series_index::samples::metadata::{
    self, Block, Column, KeyRef, Page, PageReference, Trailer, pack,
};
use crate::series_index::samples::{
    ColumnarSamplesFormat, FLOAT64_ENCODING, MANIFEST_LIMIT, Manifest, MetricSeriesId, Part,
    TimestampColumn, TimestampUnit, VERSION, ValueColumn, codec, histogram, invalid, require,
    valid_rows,
};

use crate::sst::DEFAULT_WRITE_BUFFER_SIZE;

/// Coalesces encoded columns across blocks before handing them to OpenDAL.
#[derive(Default)]
struct SampleBuffer {
    bytes: Vec<u8>,
}

impl SampleBuffer {
    async fn write(&mut self, outputs: &mut Outputs, mut bytes: &[u8]) -> Result<()> {
        let capacity = DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize;
        while !bytes.is_empty() {
            if self.bytes.capacity() == 0 {
                self.bytes.reserve_exact(capacity);
            }
            let count = bytes.len().min(capacity - self.bytes.len());
            self.bytes.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.bytes.len() == capacity {
                self.flush(outputs).await?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self, outputs: &mut Outputs) -> Result<()> {
        if !self.bytes.is_empty() {
            outputs.write(std::mem::take(&mut self.bytes)).await?;
        }
        Ok(())
    }
}

struct Row {
    id: MetricSeriesId,
    time: i64,
    sequence: u64,
    operation: u8,
}
/// One field's values for the rows of the pending block, in row order.
enum PendingField {
    Float(Vec<Option<u64>>),
    /// Runs of rows borrowed from input batches, concatenated once when the block is encoded.
    Histogram {
        runs: Vec<(ArrayRef, usize, usize)>,
        bound: ChunkBound,
    },
}
impl PendingField {
    fn new(field: &ValueColumn) -> Self {
        if field.is_histogram() {
            Self::Histogram {
                runs: Vec::new(),
                bound: ChunkBound::default(),
            }
        } else {
            Self::Float(Vec::new())
        }
    }
}
/// One input batch's column for a field.
enum FieldInput<'a> {
    Float(&'a Float64Array),
    Histogram(&'a ArrayRef, &'a StructArray),
}
/// A block of a part that is still pending: a descriptor and the columns it holds.
///
/// The exact key is not stored here. A block names its series with an index into the part's key
/// table, and that series' bytes are written once, when the part ends: no descriptor — and
/// therefore no metadata page — ever has to fit a key, and a block costs no key bytes of its own.
/// The columns are the block's own vector, which its leaf moves into the rendered descriptor
/// instead of copying, so a block owns exactly the descriptors it is written with.
struct PendingBlock {
    min: i64,
    max: i64,
    ordinal: u64,
    /// Index of this block's series in the part's key table.
    key_id: u32,
    rows: u32,
    /// This block's column descriptors, in the order the encoder pushed them.
    columns: Vec<Column>,
}

/// Turns one leaf's pending blocks into the descriptors its metadata page stores.
///
/// Only the leaf being written is rendered: the part's other leaves stay pending, so a part never
/// holds a second copy of every descriptor while its pages are written.
fn render_leaf(
    leaf: Vec<PendingBlock>,
    keys: &[ExactSeriesKey],
    references: &[KeyRef],
) -> Result<Vec<Block>> {
    let mut rendered = Vec::with_capacity(leaf.len());
    for block in leaf {
        let key_id = block.key_id as usize;
        let key = keys
            .get(key_id)
            .ok_or_else(|| invalid("sample block key outside its part"))?;
        let reference = references
            .get(key_id)
            .ok_or_else(|| invalid("sample block key outside its part"))?;
        rendered.push(Block {
            id: key.id(),
            key: *reference,
            min: block.min,
            max: block.max,
            ordinal: block.ordinal,
            rows: block.rows,
            columns: block.columns,
        });
    }
    Ok(rendered)
}
pub(super) struct SamplesWriter {
    options: ColumnarSamplesFormat,
    manifest: Manifest,
    outputs: Outputs,
    output_buffer: SampleBuffer,
    offsets: SparseOffsetsCache,
    /// Canonical sparse key of the series that owns `rows` and `last`.
    series_key: Option<ExactSeriesKey>,
    rows: Vec<Row>,
    /// Field values of `rows`, one entry per manifest field.
    fields: Vec<PendingField>,
    last: Option<(MetricSeriesId, i64, u64)>,
    ordinal: u64,
    /// One metadata page's worth of pending blocks per leaf, in the order they were flushed.
    ///
    /// A part renders one leaf at a time, so the rest of its blocks stay pending until their page
    /// is written.
    leaves: Vec<Vec<PendingBlock>>,
    /// One entry per distinct series of the current part, in the order the series first appear.
    ///
    /// Pending blocks hold an index into this table instead of a copy of their key, so the part
    /// keeps one copy of a key however many blocks the series fills, and the key area is written
    /// from here without comparing keys block by block.
    keys: Vec<ExactSeriesKey>,
    /// Table entry of the series of the last block pushed into this part.
    ///
    /// Blocks arrive sorted by exact key, so a series is already in this part's table exactly
    /// when the previous block of the part belongs to it: no key bytes are compared, and a part
    /// boundary clears the entry because the new part starts its own table.
    last_key_id: Option<u32>,
    leaf_bytes: usize,
    descriptor_bytes: usize,
    position: u64,
    part_rows: u64,
    part_blocks: u64,
    /// Bytes this part's key area is charged, one copy per series with its checksum, counted
    /// before they are written.
    part_key_bytes: u64,
    /// The last series whose key this part already counts, to charge one copy per series.
    part_last_key: Option<ExactSeriesKey>,
    manifest_bytes: usize,
    failed: bool,
}
impl SamplesWriter {
    pub fn new(
        options: ColumnarSamplesFormat,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        prefix: &str,
        source_file_id: FileId,
        generation: FileId,
    ) -> Result<Self> {
        super::validate_narthex_metadata(&metadata)?;
        require(
            (1..=super::MAX_ROWS).contains(&options.block_rows)
                && (1..=metadata::ROOT_CAPACITY).contains(&options.root_capacity)
                && (188..=metadata::PAGE_LIMIT).contains(&options.page_bytes)
                && options.part_bytes > 0
                && options.metadata_bytes > 0,
            "invalid samples packing options",
        )?;
        let timestamp = metadata.time_index_column();
        let DataType::Timestamp(unit, None) = timestamp.column_schema.data_type.as_arrow_type()
        else {
            return Err(invalid("samples require a timestamp time index"));
        };
        let fields = metadata
            .field_columns()
            .map(ValueColumn::new)
            .collect::<Result<Vec<_>>>()?;
        let pending_fields = fields.iter().map(PendingField::new).collect();
        let manifest = Manifest {
            prefix: Some(prefix.to_string()),
            format: ArtifactFormat::ColumnarSamples,
            version: VERSION,
            identity_encoding: IdentityEncoding::CURRENT_SAMPLES,
            region_id: metadata.region_id.as_u64(),
            source_file_id,
            generation,
            timestamp: TimestampColumn {
                column_id: timestamp.column_id,
                name: timestamp.column_schema.name.clone(),
                unit: TimestampUnit::from_arrow(&unit),
            },
            fields,
            files: Vec::new(),
        };
        manifest.validate_schema()?;
        require(
            prefix.ends_with('/') && !prefix.split('/').any(|part| part == ".."),
            "invalid samples output prefix",
        )?;
        let manifest_bytes = serde_json::to_vec(&manifest).context(SerdeJsonSnafu)?.len();
        require(
            manifest_bytes <= MANIFEST_LIMIT,
            "samples schema exceeds manifest limit",
        )?;
        Ok(Self {
            options,
            manifest,
            outputs: Outputs {
                store,
                active: None,
                paths: Vec::new(),
                committed: false,
            },
            output_buffer: SampleBuffer::default(),
            offsets: SparseOffsetsCache::new(),
            series_key: None,
            rows: Vec::new(),
            fields: pending_fields,
            last: None,
            ordinal: 0,
            leaves: Vec::new(),
            keys: Vec::new(),
            last_key_id: None,
            leaf_bytes: 12,
            descriptor_bytes: 0,
            position: 0,
            part_rows: 0,
            part_blocks: 0,
            part_key_bytes: 0,
            part_last_key: None,
            manifest_bytes,
            failed: false,
        })
    }
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        require(batch.num_columns() >= 4, "samples require flat batches")?;
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .ok_or_else(|| invalid(format!("missing sample input column {name}")))
        };
        let time = column(&self.manifest.timestamp.name)?;
        require(
            time.data_type() == &DataType::Timestamp(self.manifest.timestamp.unit.arrow(), None)
                && time.null_count() == 0,
            "samples timestamp type/null mismatch",
        )?;
        let (times, _) = timestamp_array_to_primitive(time)
            .ok_or_else(|| invalid("invalid sample timestamp array"))?;
        let sequence = column(consts::SEQUENCE_COLUMN_NAME)?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("invalid sample sequence type"))?;
        let operations = column(consts::OP_TYPE_COLUMN_NAME)?
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("invalid sample operation type"))?;
        require(
            sequence.null_count() == 0 && operations.null_count() == 0,
            "null sample sequence/operation",
        )?;
        let fields = self
            .manifest
            .fields
            .iter()
            .map(|field| {
                let array = column(&field.name)?;
                let input = if field.is_histogram() {
                    array
                        .as_struct_opt()
                        .map(|s| FieldInput::Histogram(array, s))
                } else {
                    array.as_any().downcast_ref().map(FieldInput::Float)
                };
                input.ok_or_else(|| invalid("invalid sample field array"))
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = column(consts::PRIMARY_KEY_COLUMN_NAME)?;
        require(keys.null_count() == 0, "null sample primary key")?;
        let binary = keys.as_any().downcast_ref::<BinaryArray>();
        let dictionary = keys.as_any().downcast_ref::<DictionaryArray<UInt32Type>>();
        let values = dictionary.and_then(|d| d.values().as_any().downcast_ref::<BinaryArray>());
        require(
            binary.is_some() || values.is_some(),
            "samples require binary or UInt32 dictionary primary keys",
        )?;
        for i in 0..batch.num_rows() {
            let key = if let Some(binary) = binary {
                binary.value(i)
            } else {
                let dictionary = dictionary.ok_or_else(|| invalid("missing sample dictionary"))?;
                let values = values.ok_or_else(|| invalid("invalid sample dictionary values"))?;
                let index = dictionary.keys().value(i) as usize;
                require(
                    index < values.len() && !values.is_null(index),
                    "invalid sample dictionary key",
                )?;
                values.value(index)
            };
            let time = times.value(i);
            let seq = sequence.value(i);
            let op = operations.value(i);
            require(op <= 1, "unknown sample operation")?;
            // Identity is the exact sparse key, not `(table_id, tsid)`: two distinct
            // series may share a `tsid`, and every block records which exact key owns it.
            let new_series = self
                .series_key
                .as_ref()
                .is_none_or(|current| current.bytes() != key);
            if new_series {
                // Validate the whole key once per series: identity is the exact key, and a
                // malformed one must not reach a block descriptor.
                let exact = ExactSeriesKey::from_sparse_key(key, &mut self.offsets)?;
                if self.last.is_some() {
                    // Rows arrive in canonical sparse-key order.
                    let previous = self
                        .series_key
                        .as_ref()
                        .ok_or_else(|| invalid("missing sample series key"))?;
                    require(
                        exact > *previous,
                        "sample input is not sorted by primary key",
                    )?;
                    // A different key that shares `(table_id, tsid)` is a different
                    // series: it opens its own block, which records its exact key, so
                    // timestamps and ordinals may restart.
                    self.flush_block().await?;
                    self.ordinal = 0;
                }
                self.series_key = Some(exact);
                // A series a part has not seen yet: its first block there adds its key.
                self.last_key_id = None;
            }
            let id = match self.last {
                Some((id, last_time, last_seq)) if !new_series => {
                    // The same exact series continues: the existing monotonic rules hold.
                    require(
                        time > last_time || (time == last_time && seq <= last_seq),
                        "sample input is not sorted",
                    )?;
                    id
                }
                _ => decode_series_id(key, &mut self.offsets)?,
            };
            let mut bounds = self.histogram_bounds(&fields, i);
            if self.rows.len() == self.options.block_rows
                || !valid_rows(self.rows.len() + 1, fields.len())
                || bounds.is_none()
            {
                self.flush_block().await?;
                bounds = self.histogram_bounds(&fields, i);
            }
            let mut bounds = bounds
                .ok_or_else(|| invalid("native histogram sample exceeds the chunk limit"))?
                .into_iter();
            self.rows.push(Row {
                id,
                time,
                sequence: seq,
                operation: op,
            });
            for (pending, input) in self.fields.iter_mut().zip(&fields) {
                match (pending, input) {
                    (PendingField::Float(values), FieldInput::Float(array)) => {
                        values.push(array.is_valid(i).then(|| array.value(i).to_bits()));
                    }
                    (PendingField::Histogram { runs, bound }, FieldInput::Histogram(array, _)) => {
                        *bound = bounds
                            .next()
                            .ok_or_else(|| invalid("missing native histogram bound"))?;
                        match runs.last_mut() {
                            Some((run, start, len))
                                if Arc::ptr_eq(run, array) && *start + *len == i =>
                            {
                                *len += 1
                            }
                            _ => runs.push(((*array).clone(), i, 1)),
                        }
                    }
                    _ => return Err(invalid("sample field kind changed between rows")),
                }
            }
            self.last = Some((id, time, seq));
        }
        Ok(())
    }
    /// Each histogram field's chunk bound with row `i` appended to the pending block, in field
    /// order, or `None` when one of those chunks would no longer fit.
    fn histogram_bounds(&self, fields: &[FieldInput<'_>], i: usize) -> Option<Vec<ChunkBound>> {
        self.fields
            .iter()
            .zip(fields)
            .filter_map(|(pending, input)| match (pending, input) {
                (PendingField::Histogram { bound, .. }, FieldInput::Histogram(_, array)) => {
                    Some(bound.with_row(array, i))
                }
                _ => None,
            })
            .collect()
    }
    /// Index of the current series' key in the part's key table, adding it on first use.
    ///
    /// Entries are appended in the order their series first appear, which is block order, so
    /// writing the table in order reproduces the key area a per-block key comparison would
    /// place. Two blocks share a key reference exactly when they share a table entry.
    fn intern_key(&mut self) -> Result<u32> {
        if let Some(id) = self.last_key_id {
            return Ok(id);
        }
        let key = self
            .series_key
            .as_ref()
            .ok_or_else(|| invalid("sample block has no series key"))?;
        let id = u32::try_from(self.keys.len())
            .map_err(|_| invalid("too many sample series in a part"))?;
        self.keys.push(key.clone());
        self.last_key_id = Some(id);
        Ok(id)
    }
    async fn flush_block(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        let fields = std::mem::replace(
            &mut self.fields,
            self.manifest.fields.iter().map(PendingField::new).collect(),
        );
        let key = self
            .series_key
            .as_ref()
            .ok_or_else(|| invalid("sample block has no series key"))?;
        // The block's fields are fixed by the rows it holds: its span is their first and last
        // time, and its ordinal is the order it was flushed in.
        let min = rows
            .first()
            .ok_or_else(|| invalid("empty sample block"))?
            .time;
        let max = rows
            .last()
            .ok_or_else(|| invalid("empty sample block"))?
            .time;
        // The encoder consumes this block's field values, so a histogram field hands its runs over
        // here and the writer starts the next block's values empty.
        let (mut columns, chunks) = encode_block(&rows, fields, key.id(), &self.manifest)?;
        let ordinal = self.ordinal;
        // Bytes one copy of this series' key costs in a part's key area: the key and its stored
        // checksum.
        let key_len = key.bytes().len() as u64 + metadata::KEY_CHECKSUM_BYTES;
        self.ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("sample ordinal overflow"))?;
        let size = metadata::descriptor_bytes(columns.len());
        // The descriptor carries a fixed-size key reference, not the key, so this bound is a
        // function of the field count only: a key longer than a whole metadata page is no
        // longer a build failure, and only a narrower configured page can reject a descriptor.
        require(
            size + 12 <= self.options.page_bytes,
            "sample descriptor exceeds leaf limit",
        )?;
        if !self.leaves.is_empty()
            && self.leaf_bytes + size > self.options.page_bytes
            && self.leaves.len() == self.options.root_capacity
        {
            self.finish_part().await?;
        }
        if self.outputs.active.is_none() {
            let id = u32::try_from(self.manifest.files.len())
                .map_err(|_| invalid("too many sample parts"))?;
            self.outputs.open(self.manifest.part_path(id)).await?;
            self.output_buffer
                .write(&mut self.outputs, &metadata::header())
                .await?;
            self.position = metadata::HEADER_SIZE;
        }
        for (column, chunk) in columns.iter_mut().zip(chunks) {
            column.payload.offset = self.position;
            self.position = column.payload.end()?;
            self.output_buffer.write(&mut self.outputs, &chunk).await?;
        }
        if self.leaves.is_empty() || self.leaf_bytes + size > self.options.page_bytes {
            self.leaves.push(Vec::new());
            self.leaf_bytes = 12;
        }
        self.leaf_bytes += size;
        self.descriptor_bytes += size;
        self.part_rows = self
            .part_rows
            .checked_add(rows.len() as u64)
            .ok_or_else(|| invalid("sample row count overflow"))?;
        self.part_blocks += 1;
        // The key table belongs to the part, and the rollover above emptied it: a series that
        // starts a new part names its key in that part's table instead of the one just closed.
        let key_id = self.intern_key()?;
        self.leaves
            .last_mut()
            .ok_or_else(|| invalid("missing sample leaf"))?
            .push(PendingBlock {
                min,
                max,
                ordinal,
                key_id,
                rows: rows.len() as u32,
                columns,
            });
        // Keys live in their own area but are still bytes of this part: count one copy per
        // series so the part target covers them too, and a key-heavy part still rolls over.
        // This is decided after the rollover above, which cleared the part's accounting: a series
        // that starts a new part still needs its copy counted in that part.
        if self.part_last_key.as_ref() != self.series_key.as_ref() {
            self.part_key_bytes = self
                .part_key_bytes
                .checked_add(key_len)
                .ok_or_else(|| invalid("sample key area overflow"))?;
            self.part_last_key = self.series_key.clone();
        }
        if self.position.saturating_add(self.part_key_bytes) >= self.options.part_bytes
            || self.descriptor_bytes >= self.options.metadata_bytes
        {
            self.finish_part().await?;
        }
        Ok(())
    }
    async fn write_page(&mut self, page: Page) -> Result<PageReference> {
        let bounds = page.bounds()?;
        let (payload, bytes) = pack(&page.encode(), self.position, 1, true)?;
        self.position = payload.end()?;
        self.output_buffer.write(&mut self.outputs, &bytes).await?;
        Ok(PageReference { bounds, payload })
    }
    /// Writes one copy of each key of the part's table to the part's key area.
    ///
    /// Copies are written in table order, which is the order their series appear in the blocks,
    /// so every block of one series is adjacent and shares one copy. Consecutive blocks of one
    /// series therefore reuse the same [`KeyRef`], while every new series appends its bytes after
    /// the previous copy, exactly as a per-block key comparison would: references are ordered,
    /// disjoint, and unique per series, which is what lets a reader treat an equal reference as
    /// the same series without reading the bytes. Each copy is followed by its XXH64, which the
    /// reference does not count: a reader verifies the bytes before they can name a series.
    async fn write_key_area(&mut self, keys: &[ExactSeriesKey]) -> Result<Vec<KeyRef>> {
        let mut references = Vec::with_capacity(keys.len());
        for key in keys {
            let length = u32::try_from(key.bytes().len())
                .map_err(|_| invalid("sample series key exceeds 4 GiB"))?;
            references.push(KeyRef {
                offset: self.position,
                length,
            });
            self.output_buffer
                .write(&mut self.outputs, key.bytes())
                .await?;
            self.output_buffer
                .write(
                    &mut self.outputs,
                    &codec::checksum(key.bytes()).to_le_bytes(),
                )
                .await?;
            self.position = self
                .position
                .checked_add(u64::from(length) + metadata::KEY_CHECKSUM_BYTES)
                .ok_or_else(|| invalid("sample file length overflow"))?;
        }
        Ok(references)
    }
    async fn finish_part(&mut self) -> Result<()> {
        if self.leaves.is_empty() {
            return Ok(());
        }
        let payload_end = self.position;
        // Keys go to their own area after the payload. The part's table holds one entry per
        // series it uses, so the area holds one copy per distinct series of this part.
        let keys = std::mem::take(&mut self.keys);
        self.last_key_id = None;
        let references = self.write_key_area(&keys).await?;
        let key_end = self.position;
        let mut children = Vec::new();
        // One leaf at a time: the rest stay pending until their turn, so rendering a part never
        // holds a second copy of every descriptor it is writing.
        for leaf in std::mem::take(&mut self.leaves) {
            children.push(
                self.write_page(Page::Leaf(render_leaf(leaf, &keys, &references)?))
                    .await?,
            );
        }
        let depth = if children.len() == 1 { 1 } else { 2 };
        let root = if depth == 1 {
            children.remove(0)
        } else {
            self.write_page(Page::Internal(children)).await?
        };
        let bounds = root.bounds;
        let file_length = self
            .position
            .checked_add(metadata::TRAILER_SIZE)
            .ok_or_else(|| invalid("sample file length overflow"))?;
        self.output_buffer
            .write(
                &mut self.outputs,
                &Trailer {
                    root,
                    depth,
                    file_length,
                    payload_end,
                    key_end,
                }
                .encode(),
            )
            .await?;
        self.output_buffer.flush(&mut self.outputs).await?;
        self.outputs.close().await?;
        let part = Part {
            part_id: u32::try_from(self.manifest.files.len())
                .map_err(|_| invalid("too many sample parts"))?,
            file_size: file_length,
            num_rows: self.part_rows,
            num_blocks: self.part_blocks,
            first: bounds.first,
            last: bounds.last,
            min_timestamp: bounds.min,
            max_timestamp: bounds.max,
        };
        self.manifest_bytes += serde_json::to_vec(&part).context(SerdeJsonSnafu)?.len()
            + usize::from(!self.manifest.files.is_empty());
        require(
            self.manifest_bytes <= MANIFEST_LIMIT,
            "samples manifest exceeds size limit",
        )?;
        self.manifest.files.push(part);
        self.position = 0;
        self.part_rows = 0;
        self.part_blocks = 0;
        self.part_key_bytes = 0;
        self.part_last_key = None;
        self.descriptor_bytes = 0;
        self.leaf_bytes = 12;
        Ok(())
    }
    async fn complete(&mut self) -> Result<ArtifactMetadata> {
        require(!self.failed, "sample writer failed previously")?;
        self.flush_block().await?;
        self.finish_part().await?;
        let bytes = serde_json::to_vec(&self.manifest).context(SerdeJsonSnafu)?;
        require(
            bytes.len() <= MANIFEST_LIMIT,
            "samples manifest exceeds limit",
        )?;
        let mut file_size = bytes.len() as u64;
        let mut objects = vec![ArtifactObject {
            path: self.manifest.path(),
            file_size,
        }];
        for part in &self.manifest.files {
            objects.push(ArtifactObject {
                path: self.manifest.part_path(part.part_id),
                file_size: part.file_size,
            });
            file_size = file_size
                .checked_add(part.file_size)
                .ok_or_else(|| invalid("sample artifact size overflow"))?;
        }
        self.outputs.open(self.manifest.path()).await?;
        self.outputs.write(bytes).await?;
        self.outputs.close().await?;
        self.outputs.committed = true;
        Ok(ArtifactMetadata {
            format: ArtifactFormat::ColumnarSamples,
            generation: self.manifest.generation,
            objects,
            file_size,
        })
    }
}

#[cfg(test)]
impl SamplesWriter {
    /// Bytes this part's pending containers hold: the leaf lists, the blocks with their column
    /// descriptors, the key table and one copy of each table key's bytes.
    ///
    /// Spare capacity is counted, because a part holds its containers for its whole life. Leaf
    /// headers, the output buffer, the buffered row and allocator overhead are not, so this is
    /// the writer's own containers and nothing else.
    pub(super) fn pending_container_bytes(&self) -> usize {
        self.leaves.capacity() * std::mem::size_of::<Vec<PendingBlock>>()
            + self
                .leaves
                .iter()
                .map(|leaf| {
                    leaf.capacity() * std::mem::size_of::<PendingBlock>()
                        + leaf
                            .iter()
                            .map(|block| block.columns.capacity() * std::mem::size_of::<Column>())
                            .sum::<usize>()
                })
                .sum::<usize>()
            + self.keys.capacity() * std::mem::size_of::<ExactSeriesKey>()
            + self.keys.iter().map(|key| key.bytes().len()).sum::<usize>()
    }

    /// The current part's accounting, for the rollover tests: `(position, counted key bytes,
    /// blocks in the current part, finished parts)`.
    ///
    /// A key copy that is not counted defers the part boundary silently, and the parts it makes
    /// are still readable, so only the accounting itself shows the difference.
    pub(super) fn part_accounting(&self) -> (u64, u64, u64, usize) {
        (
            self.position,
            self.part_key_bytes,
            self.part_blocks,
            self.manifest.files.len(),
        )
    }
}
#[async_trait]
impl FormatWriter for SamplesWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        require(!self.failed, "sample writer failed previously")?;
        self.failed = true;
        let result = self.write_batch(batch).await;
        if result.is_ok() {
            self.failed = false;
        }
        result
    }
    async fn finish(mut self: Box<Self>) -> Result<ArtifactMetadata> {
        let result = self.complete().await;
        if result.is_err()
            && let Err(error) = self.outputs.cleanup().await
        {
            common_telemetry::warn!(error; "Cleaning failed samples build");
        }
        result
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.outputs.cleanup().await
    }
}
/// Encodes one block's columns and returns its descriptors with the encoded chunks to write.
fn encode_block(
    rows: &[Row],
    fields: Vec<PendingField>,
    id: MetricSeriesId,
    manifest: &Manifest,
) -> Result<(Vec<Column>, Vec<Vec<u8>>)> {
    require(
        rows.iter().all(|row| row.id == id),
        "sample block mixes series",
    )?;
    let mut columns = Vec::new();
    let mut chunks = Vec::new();
    let mut push = |role, id, encoding, bytes: Vec<u8>, nulls| -> Result<()> {
        let (payload, bytes) = pack(&bytes, 0, encoding, false)?;
        columns.push(Column {
            role,
            id,
            payload,
            nulls,
        });
        chunks.push(bytes);
        Ok(())
    };
    let (encoding, bytes) = codec::encode_time(&rows.iter().map(|r| r.time).collect::<Vec<_>>());
    push(1, manifest.timestamp.column_id, encoding, bytes, 0)?;
    let (encoding, bytes) =
        codec::encode_sequence(&rows.iter().map(|r| r.sequence).collect::<Vec<_>>());
    push(2, 0, encoding, bytes, 0)?;
    let (encoding, bytes) =
        codec::encode_operations(&rows.iter().map(|r| r.operation).collect::<Vec<_>>());
    push(3, 0, encoding, bytes, 0)?;
    for (field, pending) in manifest.fields.iter().zip(fields) {
        match pending {
            PendingField::Float(values) => {
                require(
                    values.len() == rows.len(),
                    "sample field row count mismatch",
                )?;
                let nulls = values.iter().filter(|v| v.is_none()).count();
                if nulls != rows.len() {
                    push(
                        4,
                        field.column_id,
                        FLOAT64_ENCODING,
                        codec::encode_float(&values),
                        nulls as u32,
                    )?;
                }
            }
            PendingField::Histogram { runs, .. } => {
                let slices = runs
                    .iter()
                    .map(|(array, start, len)| array.slice(*start, *len))
                    .collect::<Vec<_>>();
                let array = concat(&slices.iter().map(|a| a.as_ref()).collect::<Vec<_>>())
                    .context(ComputeArrowSnafu)?;
                let array = array
                    .as_struct_opt()
                    .ok_or_else(|| invalid("invalid native histogram sample array"))?;
                require(array.len() == rows.len(), "sample field row count mismatch")?;
                // An all-null field has no chunk, as for Float64 fields.
                if array.null_count() != rows.len() {
                    push(
                        4,
                        field.column_id,
                        histogram::ENCODING,
                        histogram::encode(array)?,
                        array.null_count() as u32,
                    )?;
                }
            }
        }
    }
    Ok((columns, chunks))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::ArrayRef;
    use datatypes::arrow::datatypes::{Field, Schema};
    use datatypes::prelude::ConcreteDataType;
    use store_api::codec::PrimaryKeyEncoding;

    use super::*;
    use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

    fn outputs() -> Outputs {
        Outputs {
            store: ObjectStore::new(object_store::services::Memory::default()).unwrap(),
            active: None,
            paths: Vec::new(),
            committed: false,
        }
    }

    #[tokio::test]
    async fn buffered_output_boundaries_and_parts() {
        let capacity = DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize;
        let mut outputs = outputs();
        let mut buffer = SampleBuffer::default();
        for size in [0, capacity - 1, capacity, capacity + 1, capacity * 2 + 13] {
            let path = format!("part-{size}");
            outputs.open(path.clone()).await.unwrap();
            let bytes: Vec<_> = (0..size).map(|i| (i % 251) as u8).collect();
            // Exercise both tiny fragments and an input larger than the staging buffer.
            let split = size.min(1000);
            for chunk in bytes[..split].chunks(7) {
                buffer.write(&mut outputs, chunk).await.unwrap();
            }
            buffer.write(&mut outputs, &bytes[split..]).await.unwrap();
            assert_eq!(buffer.bytes.len(), size % capacity);
            buffer.flush(&mut outputs).await.unwrap();
            outputs.close().await.unwrap();
            assert!(buffer.bytes.is_empty());
            assert_eq!(outputs.store.read(&path).await.unwrap().to_vec(), bytes);
        }
        outputs.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn abort_discards_pending_bytes() {
        let mut outputs = outputs();
        let mut buffer = SampleBuffer::default();
        outputs.open("part".into()).await.unwrap();
        buffer.write(&mut outputs, b"pending").await.unwrap();
        outputs.cleanup().await.unwrap();
        drop(buffer);
        assert!(!outputs.store.exists("part").await.unwrap());
    }

    /// Synthetic small-write benchmark; run explicitly with --ignored --nocapture.
    #[tokio::test]
    #[ignore]
    async fn benchmark_small_sample_writes() {
        common_telemetry::init_default_ut_logging();
        let capacity = DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize;
        let fragment = vec![42; 100];
        let count = capacity / fragment.len() + 1;
        for buffered in [false, true] {
            let mut outputs = outputs();
            outputs.open("bench".into()).await.unwrap();
            let mut buffer = SampleBuffer::default();
            let started = std::time::Instant::now();
            for _ in 0..count {
                if buffered {
                    buffer.write(&mut outputs, &fragment).await.unwrap();
                } else {
                    outputs.write(fragment.clone()).await.unwrap();
                }
            }
            buffer.flush(&mut outputs).await.unwrap();
            outputs.close().await.unwrap();
            common_telemetry::info!(
                "buffered={buffered}, elapsed={:?}, submitted_fragments={}",
                started.elapsed(),
                if buffered { 2 } else { count }
            );
            assert_eq!(
                outputs.store.read("bench").await.unwrap().len(),
                count * fragment.len()
            );
            outputs.cleanup().await.unwrap();
        }
    }

    /// The region metadata the sample fixtures are built for, with one nullable `f64` field.
    fn metadata() -> RegionMetadataRef {
        let mut metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
        metadata
            .column_metadatas
            .iter_mut()
            .find(|column| column.column_id == 2)
            .unwrap()
            .column_schema
            .data_type = ConcreteDataType::float64_datatype();
        Arc::new(metadata)
    }

    /// Builds one batch from explicit sparse keys and times, one row per key.
    fn sparse_batch(metadata: &RegionMetadataRef, keys: &[Vec<u8>], times: &[i64]) -> RecordBatch {
        let count = keys.len();
        let keys: ArrayRef = Arc::new(BinaryArray::from_iter_values(
            keys.iter().map(Vec::as_slice),
        ));
        let unit = metadata
            .time_index_column()
            .column_schema
            .data_type
            .as_timestamp()
            .unwrap()
            .unit();
        let ts: ArrayRef = match unit {
            common_time::timestamp::TimeUnit::Second => Arc::new(
                datatypes::arrow::array::TimestampSecondArray::from_iter_values(
                    times.iter().copied(),
                ),
            ),
            common_time::timestamp::TimeUnit::Millisecond => Arc::new(
                datatypes::arrow::array::TimestampMillisecondArray::from_iter_values(
                    times.iter().copied(),
                ),
            ),
            common_time::timestamp::TimeUnit::Microsecond => Arc::new(
                datatypes::arrow::array::TimestampMicrosecondArray::from_iter_values(
                    times.iter().copied(),
                ),
            ),
            common_time::timestamp::TimeUnit::Nanosecond => Arc::new(
                datatypes::arrow::array::TimestampNanosecondArray::from_iter_values(
                    times.iter().copied(),
                ),
            ),
        };
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("field_0", DataType::Float64, true),
                Field::new("ts", ts.data_type().clone(), false),
                Field::new(
                    consts::PRIMARY_KEY_COLUMN_NAME,
                    keys.data_type().clone(),
                    false,
                ),
                Field::new(consts::SEQUENCE_COLUMN_NAME, DataType::UInt64, false),
                Field::new(consts::OP_TYPE_COLUMN_NAME, DataType::UInt8, false),
            ])),
            vec![
                Arc::new(Float64Array::from_iter_values(vec![0.0; count])),
                ts,
                keys,
                Arc::new(UInt64Array::from_iter_values(vec![1; count])),
                Arc::new(UInt8Array::from_iter_values(vec![0; count])),
            ],
        )
        .unwrap()
    }

    /// A filesystem store in the system temporary directory.
    ///
    /// The writer's memory is what these tests measure, so a written part must not stay in
    /// memory the way a memory store would keep it.
    fn fs_store(name: &str) -> (ObjectStore, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("samples-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let store =
            ObjectStore::new(object_store::services::Fs::default().root(root.to_str().unwrap()))
                .unwrap();
        (store, root)
    }

    /// One resident-memory field of `/proc/self/status`, in KiB.
    fn rss_kib(field: &str) -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix(field)?.trim().strip_suffix("kB"))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0)
    }

    /// Reports the sizes of the in-flight block representation and how many series one part's
    /// key table holds. Run with `--nocapture` to see the numbers.
    #[allow(clippy::print_stdout)] // The sizes exist to be read off stdout.
    #[tokio::test]
    async fn block_representation_footprint() {
        let metadata = metadata();
        let options = ColumnarSamplesFormat {
            block_rows: 1,
            page_bytes: 1868,
            ..Default::default()
        };
        let (store, root) = fs_store("footprint");
        let mut writer = SamplesWriter::new(
            options,
            metadata.clone(),
            store.clone(),
            "footprint/",
            FileId::random(),
            FileId::random(),
        )
        .unwrap();
        // Two series, two blocks each: a key table holds one entry per series, not per block.
        let keys = vec![
            new_sparse_primary_key(&["a", "b"], &metadata, 1, 7),
            new_sparse_primary_key(&["a", "b"], &metadata, 1, 7),
            new_sparse_primary_key(&["a", "b"], &metadata, 1, 8),
            new_sparse_primary_key(&["a", "b"], &metadata, 1, 8),
        ];
        let batch = sparse_batch(&metadata, &keys, &[1, 2, 3, 4]);
        writer.write(&batch).await.unwrap();
        let blocks: usize = writer.leaves.iter().map(Vec::len).sum();
        println!(
            "size_of::<PendingBlock>()={} size_of::<Block>()={} size_of::<Column>()={} \
             key_table_entries={} key_bytes={} blocks={} columns={} descriptor_bytes={}",
            std::mem::size_of::<PendingBlock>(),
            std::mem::size_of::<Block>(),
            std::mem::size_of::<Column>(),
            writer.keys.len(),
            keys[0].len(),
            blocks,
            writer
                .leaves
                .iter()
                .flatten()
                .map(|block| block.columns.len())
                .sum::<usize>(),
            metadata::descriptor_bytes(4),
        );
        assert_eq!(
            2,
            writer.keys.len(),
            "one key table entry per series, not per block"
        );
        assert_eq!(3, blocks, "the last row is still buffered");
        assert_eq!(1, writer.leaves.len());
        drop(writer);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Memory benchmark for the acceptance configuration: 270k one-row blocks.
    ///
    /// A part holds [`metadata::ROOT_CAPACITY`] leaves of eight blocks each at this page size, so
    /// the blocks span several parts and the pending containers are sampled after every batch:
    /// the largest sample, one nearly full part, is where the bytes per block are read off.
    ///
    /// Run with `--ignored --nocapture`, ideally under `/usr/bin/time -v` for the kernel's own
    /// `Maximum resident set size`; the test also prints `VmHWM` and the bytes per block it
    /// implies over the residency it started with.
    #[allow(clippy::print_stdout)] // The peak exists to be read off stdout at the end of a run.
    #[tokio::test]
    #[ignore]
    async fn benchmark_block_memory() {
        common_telemetry::init_default_ut_logging();
        const BLOCKS: usize = 270_000;
        const SERIES_PER_BATCH: usize = 1000;
        let metadata = metadata();
        let options = ColumnarSamplesFormat {
            block_rows: 1,
            page_bytes: 1868,
            metadata_bytes: 96 << 20,
            ..Default::default()
        };
        let (store, root) = fs_store("block-memory");
        let mut writer = SamplesWriter::new(
            options,
            metadata.clone(),
            store.clone(),
            "bench/",
            FileId::random(),
            FileId::random(),
        )
        .unwrap();
        // Readers of one series must see increasing times; one row per series sidesteps it.
        let start_kib = rss_kib("VmRSS:");
        let started = std::time::Instant::now();
        // `(bytes, blocks)` of the largest fill the pending containers reached: a part is
        // rendered only once it is full, so this is the most a part ever holds.
        let mut peak_pending = (0, 0);
        let mut written = 0;
        while written < BLOCKS {
            let count = SERIES_PER_BATCH.min(BLOCKS - written);
            let keys: Vec<_> = (written..written + count)
                .map(|series| new_sparse_primary_key(&["a", "b"], &metadata, 1, series as u64))
                .collect();
            let times: Vec<_> = (written..written + count).map(|row| row as i64).collect();
            let batch = sparse_batch(&metadata, &keys, &times);
            writer.write(&batch).await.unwrap();
            written += count;
            let bytes = writer.pending_container_bytes();
            if bytes > peak_pending.0 {
                peak_pending = (bytes, writer.part_blocks);
            }
        }
        let write_elapsed = started.elapsed();
        // Flush the buffered block first, so the part counters cover every block written.
        writer.flush_block().await.unwrap();
        let pending_blocks = writer.part_blocks;
        let blocks: usize = writer.leaves.iter().map(Vec::len).sum();
        // The peak is reached while every block of the part is still pending, before the part
        // is rendered, so it is read before `finish` starts freeing the pending blocks.
        let peak_kib = rss_kib("VmHWM:");
        let artifact = writer.complete().await.unwrap();
        let elapsed = started.elapsed();
        assert_eq!(
            BLOCKS as u64,
            writer
                .manifest
                .files
                .iter()
                .map(|part| part.num_blocks)
                .sum::<u64>(),
            "every block is written once"
        );
        assert_eq!(
            pending_blocks as usize, blocks,
            "every part block is pending"
        );
        println!(
            "blocks={BLOCKS} parts={} peak_pending_bytes={} peak_pending_blocks={} \
             peak_container_bytes_per_block={} peak_rss={:.1}MiB start_rss={:.1}MiB \
             rss_bytes_per_block={} write={write_elapsed:?} total={elapsed:?}",
            artifact.objects.len() - 1,
            peak_pending.0,
            peak_pending.1,
            peak_pending.0 as u64 / peak_pending.1.max(1),
            peak_kib as f64 / 1024.0,
            start_kib as f64 / 1024.0,
            peak_kib.saturating_sub(start_kib) * 1024 / BLOCKS as u64,
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Memory benchmark for the shape the key table is for: series that outlive one block.
    ///
    /// Ten thousand series with eight one-row blocks each, so a part's key table holds one entry
    /// per eight blocks. Run with `--ignored --nocapture`; it prints the bytes the pending
    /// containers hold per block for the last part, which `root_capacity` fills to nearly the
    /// same fraction as the one-block-per-series shape above.
    #[allow(clippy::print_stdout)] // The ratio exists to be read off stdout.
    #[tokio::test]
    #[ignore]
    async fn benchmark_block_memory_shared_keys() {
        common_telemetry::init_default_ut_logging();
        const SERIES: usize = 10_000;
        const POINTS: usize = 8;
        const SERIES_PER_BATCH: usize = 1000;
        let metadata = metadata();
        let options = ColumnarSamplesFormat {
            block_rows: 1,
            page_bytes: 1868,
            metadata_bytes: 96 << 20,
            ..Default::default()
        };
        let (store, root) = fs_store("block-memory-shared");
        let mut writer = SamplesWriter::new(
            options,
            metadata.clone(),
            store.clone(),
            "bench/",
            FileId::random(),
            FileId::random(),
        )
        .unwrap();
        // Rows arrive in primary-key order, so one series' points are adjacent and its blocks
        // are too: they share a key reference.
        let mut written = 0;
        while written < SERIES {
            let count = SERIES_PER_BATCH.min(SERIES - written);
            let mut keys = Vec::with_capacity(count * POINTS);
            let mut times = Vec::with_capacity(count * POINTS);
            for series in written..written + count {
                let key = new_sparse_primary_key(&["a", "b"], &metadata, 1, series as u64);
                for point in 0..POINTS {
                    keys.push(key.clone());
                    times.push(point as i64);
                }
            }
            let batch = sparse_batch(&metadata, &keys, &times);
            writer.write(&batch).await.unwrap();
            written += count;
        }
        writer.flush_block().await.unwrap();
        let container_bytes = writer.pending_container_bytes();
        let blocks: usize = writer.leaves.iter().map(Vec::len).sum();
        let part_keys = writer.keys.len();
        let part_blocks = writer.part_blocks;
        let artifact = writer.complete().await.unwrap();
        assert_eq!(blocks, part_blocks as usize, "every part block is pending");
        assert_eq!(
            SERIES,
            writer
                .manifest
                .files
                .iter()
                .map(|part| part.num_rows as usize / POINTS)
                .sum::<usize>(),
            "every series is written"
        );
        println!(
            "series={SERIES} points={POINTS} blocks={blocks} parts={} part_keys={part_keys} \
             container_bytes={container_bytes} container_bytes_per_block={} \
             key_table_entries_per_block={:.2}",
            artifact.objects.len() - 1,
            container_bytes / blocks,
            part_keys as f64 / blocks as f64,
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
