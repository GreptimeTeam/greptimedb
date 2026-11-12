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

//! Streaming per-SST sample writing with bounded blocks and owned outputs.

use async_trait::async_trait;
use datatypes::arrow::array::{
    Array, BinaryArray, DictionaryArray, Float64Array, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::DataType as _;
use datatypes::timestamp::timestamp_array_to_primitive;
use mito_codec::row_converter::SparseOffsetsCache;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{FileId, consts};

use crate::error::{Result, SerdeJsonSnafu};
use crate::series_index::artifact::{ArtifactFormat, ArtifactMetadata, ArtifactObject};
use crate::series_index::artifact_writer::Outputs;
use crate::series_index::format::FormatWriter;
use crate::series_index::identity::{ExactSeriesKey, IdentityEncoding, decode_series_id};
use crate::series_index::samples::codec;
use crate::series_index::samples::metadata::{
    self, Block, Column, KeyRef, Page, PageReference, Trailer, pack,
};
use crate::series_index::samples::{
    ColumnarSamplesFormat, MANIFEST_LIMIT, Manifest, MetricSeriesId, Part, TimestampColumn,
    TimestampUnit, VERSION, ValueColumn, invalid, require, valid_rows,
};
use crate::series_index::writer::validate_metadata;

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
    fields: Vec<Option<u64>>,
}
/// A block descriptor whose exact key has not been placed in the part's key area yet.
///
/// The key bytes are held here until the part ends, when they are written to their own area
/// and replaced by a fixed-size [`KeyRef`]. That is why no descriptor — and therefore no
/// metadata page — ever has to fit a key.
struct PendingBlock {
    key: ExactSeriesKey,
    min: i64,
    max: i64,
    ordinal: u64,
    rows: u32,
    columns: Vec<Column>,
}

impl PendingBlock {
    /// Descriptor bytes once the key reference replaces the key bytes.
    fn size(&self) -> usize {
        metadata::descriptor_bytes(self.columns.len())
    }
}

/// Places one copy of each series key in the part's key area and turns pending blocks into
/// descriptors that reference it.
///
/// Blocks are ordered by exact key, so every block of one series is adjacent and shares one
/// copy of the key. Consecutive equal keys therefore reuse the previous reference, while every
/// new series appends its bytes after the previous copy: references are ordered, disjoint, and
/// unique per series, which is what lets a reader treat an equal reference as the same series
/// without reading the bytes. Each copy is followed by its XXH64, which the reference does not
/// count: a reader verifies the bytes before they can name a series.
fn place_keys(leaves: Vec<Vec<PendingBlock>>, start: u64) -> Result<(Vec<Vec<Block>>, Vec<u8>)> {
    let mut area: Vec<u8> = Vec::new();
    let mut previous: Option<(Vec<u8>, KeyRef)> = None;
    let mut placed = Vec::with_capacity(leaves.len());
    for pending in leaves {
        let mut blocks = Vec::with_capacity(pending.len());
        for block in pending {
            let reuse = previous
                .as_ref()
                .filter(|(key, _)| key.as_slice() == block.key.bytes())
                .map(|(_, reference)| *reference);
            let reference = match reuse {
                Some(reference) => reference,
                None => {
                    let length = u32::try_from(block.key.bytes().len())
                        .map_err(|_| invalid("sample series key exceeds 4 GiB"))?;
                    let offset = start
                        .checked_add(
                            u64::try_from(area.len())
                                .map_err(|_| invalid("sample key area overflow"))?,
                        )
                        .ok_or_else(|| invalid("sample key offset overflow"))?;
                    area.extend_from_slice(block.key.bytes());
                    area.extend_from_slice(&codec::checksum(block.key.bytes()).to_le_bytes());
                    let reference = KeyRef { offset, length };
                    previous = Some((block.key.bytes().to_vec(), reference));
                    reference
                }
            };
            blocks.push(Block {
                id: block.key.id(),
                key: reference,
                min: block.min,
                max: block.max,
                ordinal: block.ordinal,
                rows: block.rows,
                columns: block.columns,
            });
        }
        placed.push(blocks);
    }
    Ok((placed, area))
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
    last: Option<(MetricSeriesId, i64, u64)>,
    ordinal: u64,
    leaves: Vec<Vec<PendingBlock>>,
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
        validate_metadata(&metadata)?;
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
            .map(|column| {
                require(
                    column.column_schema.data_type.as_arrow_type() == DataType::Float64,
                    "samples support only Float64 fields",
                )?;
                Ok(ValueColumn {
                    column_id: column.column_id,
                    name: column.column_schema.name.clone(),
                    data_type: "float64".into(),
                    nullable: true,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let manifest = Manifest {
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
            prefix == manifest.prefix(),
            "samples output prefix mismatch",
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
            last: None,
            ordinal: 0,
            leaves: Vec::new(),
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
                column(&field.name)?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| invalid("invalid sample field array"))
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
            if self.rows.len() == self.options.block_rows
                || !valid_rows(self.rows.len() + 1, fields.len())
            {
                self.flush_block().await?;
            }
            self.rows.push(Row {
                id,
                time,
                sequence: seq,
                operation: op,
                fields: fields
                    .iter()
                    .map(|field| {
                        if field.is_null(i) {
                            None
                        } else {
                            Some(field.value(i).to_bits())
                        }
                    })
                    .collect(),
            });
            self.last = Some((id, time, seq));
        }
        Ok(())
    }
    async fn flush_block(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        let key = self
            .series_key
            .as_ref()
            .ok_or_else(|| invalid("sample block has no series key"))?;
        let (mut block, chunks) = encode_block(&rows, key, &self.manifest, self.ordinal)?;
        // Bytes one copy of this series' key costs in a part's key area: the key and its stored
        // checksum.
        let key_len = key.bytes().len() as u64 + metadata::KEY_CHECKSUM_BYTES;
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("sample ordinal overflow"))?;
        let size = block.size();
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
        for (column, chunk) in block.columns.iter_mut().zip(chunks) {
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
            .checked_add(u64::from(block.rows))
            .ok_or_else(|| invalid("sample row count overflow"))?;
        self.part_blocks += 1;
        self.leaves
            .last_mut()
            .ok_or_else(|| invalid("missing sample leaf"))?
            .push(block);
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
    async fn finish_part(&mut self) -> Result<()> {
        if self.leaves.is_empty() {
            return Ok(());
        }
        let payload_end = self.position;
        // Keys go to their own area after the payload. Every block of one series shares one
        // copy, so the area holds one key per distinct series of this part.
        let (leaves, key_area) = place_keys(std::mem::take(&mut self.leaves), payload_end)?;
        if !key_area.is_empty() {
            self.position = self
                .position
                .checked_add(
                    u64::try_from(key_area.len())
                        .map_err(|_| invalid("sample key area overflow"))?,
                )
                .ok_or_else(|| invalid("sample file length overflow"))?;
            self.output_buffer
                .write(&mut self.outputs, &key_area)
                .await?;
        }
        let key_end = self.position;
        let mut children = Vec::new();
        for leaf in leaves {
            children.push(self.write_page(Page::Leaf(leaf)).await?);
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
fn encode_block(
    rows: &[Row],
    key: &ExactSeriesKey,
    manifest: &Manifest,
    ordinal: u64,
) -> Result<(PendingBlock, Vec<Vec<u8>>)> {
    let first = rows.first().ok_or_else(|| invalid("empty sample block"))?;
    let last = rows.last().ok_or_else(|| invalid("empty sample block"))?;
    require(
        rows.iter().all(|row| row.id == key.id()),
        "sample block mixes series",
    )?;
    let mut block = PendingBlock {
        key: key.clone(),
        min: first.time,
        max: last.time,
        ordinal,
        rows: rows.len() as u32,
        columns: Vec::new(),
    };
    let mut chunks = Vec::new();
    let mut push = |role, id, encoding, bytes: Vec<u8>, nulls| -> Result<()> {
        let (payload, bytes) = pack(&bytes, 0, encoding, false)?;
        block.columns.push(Column {
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
    for (index, field) in manifest.fields.iter().enumerate() {
        let values: Vec<_> = rows.iter().map(|r| r.fields[index]).collect();
        let nulls = values.iter().filter(|v| v.is_none()).count();
        if nulls != rows.len() {
            push(
                4,
                field.column_id,
                5,
                codec::encode_float(&values),
                nulls as u32,
            )?;
        }
    }
    Ok((block, chunks))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
