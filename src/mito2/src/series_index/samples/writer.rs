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
    self, Block, Column, KeyRef, Page, PageReference, pack,
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
pub(super) struct SamplesWriter {
    options: ColumnarSamplesFormat,
    manifest: Manifest,
    outputs: Outputs,
    compressor: zstd::bulk::Compressor<'static>,
    output_buffer: SampleBuffer,
    offsets: SparseOffsetsCache,
    /// Canonical sparse key of the series that owns `rows` and `last`.
    series_key: Option<ExactSeriesKey>,
    rows: Vec<Row>,
    /// Field values of `rows`, one entry per manifest field.
    fields: Vec<PendingField>,
    last: Option<(MetricSeriesId, i64, u64)>,
    ordinal: u64,
    leaves: Vec<Vec<Block>>,
    leaf_bytes: usize,
    descriptor_bytes: usize,
    position: u64,
    groups: Vec<metadata::Group>,
    group_rows: u64,
    group_blocks: u64,
    /// Keys and their checksums for the active group. Pending blocks reference offsets
    /// within this buffer until the final payload position is known.
    keys: Vec<u8>,
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
            compressor: zstd::bulk::Compressor::new(3)
                .map_err(|e| invalid(format!("sample compression: {e}")))?,
            output_buffer: SampleBuffer::default(),
            offsets: SparseOffsetsCache::new(),
            series_key: None,
            rows: Vec::new(),
            fields: pending_fields,
            last: None,
            ordinal: 0,
            leaves: Vec::new(),
            leaf_bytes: 12,
            descriptor_bytes: 0,
            position: 0,
            groups: Vec::new(),
            group_rows: 0,
            group_blocks: 0,
            keys: Vec::new(),
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
        let (mut block, chunks) = encode_block(
            &rows,
            fields,
            key,
            &self.manifest,
            self.ordinal,
            &mut self.compressor,
        )?;
        // Bytes one copy of this series' key costs in a part's key area: the key and its stored
        // checksum.
        let key_len = key.bytes().len() as u64 + metadata::KEY_CHECKSUM_BYTES;
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("sample ordinal overflow"))?;
        let size = metadata::descriptor_bytes(block.columns.len());
        // The descriptor carries a fixed-size key reference, not the key, so this bound is a
        // function of the field count only: a key longer than a whole metadata page is no
        // longer a build failure, and only a narrower configured page can reject a descriptor.
        require(
            size + 12 <= self.options.page_bytes,
            "sample descriptor exceeds leaf limit",
        )?;
        let budget = self.options.metadata_bytes as u64;
        let new_series = block.ordinal == 0;
        let pending = self.descriptor_bytes as u64 + self.keys.len() as u64;
        // Prefer a series boundary after three quarters of the budget; a long series
        // may continue into the next group when the hard budget or leaf fanout is reached.
        let projected = pending + size as u64 + if new_series { key_len } else { 0 };
        if !self.leaves.is_empty()
            && ((new_series && pending >= budget - budget / 4)
                || projected > budget
                || (self.leaf_bytes + size > self.options.page_bytes
                    && self.leaves.len() == self.options.root_capacity))
        {
            self.finish_group().await?;
            if self.groups.len() == metadata::MAX_GROUPS {
                self.finish_part().await?;
            }
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
        if !new_series && let Some(previous) = self.leaves.last().and_then(|leaf| leaf.last()) {
            block.key = previous.key;
        } else {
            let key = self
                .series_key
                .as_ref()
                .ok_or_else(|| invalid("sample block has no series key"))?
                .bytes();
            block.key.offset = self.keys.len() as u64;
            self.keys.extend_from_slice(key);
            self.keys
                .extend_from_slice(&codec::checksum(key).to_le_bytes());
        }
        if self.leaves.is_empty() || self.leaf_bytes + size > self.options.page_bytes {
            self.leaves.push(Vec::new());
            self.leaf_bytes = 12;
        }
        self.leaf_bytes += size;
        self.descriptor_bytes += size;
        self.group_rows = self
            .group_rows
            .checked_add(u64::from(block.rows))
            .ok_or_else(|| invalid("sample row count overflow"))?;
        self.group_blocks += 1;
        self.leaves
            .last_mut()
            .ok_or_else(|| invalid("missing sample leaf"))?
            .push(block);
        if self.position.saturating_add(self.keys.len() as u64) >= self.options.part_bytes {
            self.finish_part().await?;
        }
        Ok(())
    }
    async fn write_page(&mut self, page: Page) -> Result<PageReference> {
        let bounds = page.bounds()?;
        let (payload, bytes) = pack(&mut self.compressor, &page.encode(), self.position, 1, true)?;
        self.position = payload.end()?;
        self.output_buffer.write(&mut self.outputs, &bytes).await?;
        Ok(PageReference { bounds, payload })
    }
    async fn finish_group(&mut self) -> Result<()> {
        if self.leaves.is_empty() {
            return Ok(());
        }
        let payload_end = self.position;
        let key_end = payload_end
            .checked_add(self.keys.len() as u64)
            .ok_or_else(|| invalid("sample key area overflow"))?;
        self.output_buffer
            .write(&mut self.outputs, &std::mem::take(&mut self.keys))
            .await?;
        self.position = key_end;
        let mut children = Vec::with_capacity(self.leaves.len());
        for mut leaf in std::mem::take(&mut self.leaves) {
            for block in &mut leaf {
                // All relative offsets lie within the key buffer whose end was checked above.
                block.key.offset += payload_end;
            }
            children.push(self.write_page(Page::Leaf(leaf)).await?);
        }
        let depth = if children.len() == 1 { 1 } else { 2 };
        let root = if depth == 1 {
            children.remove(0)
        } else {
            self.write_page(Page::Internal(children)).await?
        };
        let start = match self.groups.last() {
            Some(group) => group.root.payload.end()?,
            None => metadata::HEADER_SIZE,
        };
        self.groups.push(metadata::Group {
            start,
            rows: self.group_rows,
            blocks: self.group_blocks,
            root,
            depth,
            payload_end,
            key_end,
        });
        self.group_rows = 0;
        self.group_blocks = 0;
        self.descriptor_bytes = 0;
        self.leaf_bytes = 12;
        Ok(())
    }
    async fn finish_part(&mut self) -> Result<()> {
        self.finish_group().await?;
        let groups = std::mem::take(&mut self.groups);
        if groups.is_empty() {
            return Ok(());
        }
        let bytes = metadata::encode_directory(&groups);
        let file_length = self
            .position
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("sample part length overflow"))?;
        self.output_buffer.write(&mut self.outputs, &bytes).await?;
        self.output_buffer.flush(&mut self.outputs).await?;
        self.outputs.close().await?;
        let part_id = u32::try_from(self.manifest.files.len())
            .map_err(|_| invalid("too many sample parts"))?;
        let part = Part::from_groups(part_id, file_length, &groups)?;
        self.manifest_bytes += serde_json::to_vec(&part).context(SerdeJsonSnafu)?.len()
            + usize::from(!self.manifest.files.is_empty());
        require(
            self.manifest_bytes <= MANIFEST_LIMIT,
            "samples manifest exceeds size limit",
        )?;
        self.manifest.files.push(part);
        self.position = 0;
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
    /// blocks in the current group, finished parts)`.
    ///
    /// A key copy that is not counted defers the part boundary silently, and the parts it makes
    /// are still readable, so only the accounting itself shows the difference.
    pub(super) fn part_accounting(&self) -> (u64, u64, u64, usize) {
        (
            self.position,
            self.keys.len() as u64,
            self.group_blocks,
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
    fields: Vec<PendingField>,
    key: &ExactSeriesKey,
    manifest: &Manifest,
    ordinal: u64,
    compressor: &mut zstd::bulk::Compressor<'_>,
) -> Result<(Block, Vec<Vec<u8>>)> {
    let first = rows.first().ok_or_else(|| invalid("empty sample block"))?;
    let last = rows.last().ok_or_else(|| invalid("empty sample block"))?;
    require(
        rows.iter().all(|row| row.id == key.id()),
        "sample block mixes series",
    )?;
    let mut block = Block {
        id: key.id(),
        key: KeyRef {
            offset: 0,
            length: u32::try_from(key.bytes().len())
                .map_err(|_| invalid("sample series key exceeds 4 GiB"))?,
        },
        min: first.time,
        max: last.time,
        ordinal,
        rows: rows.len() as u32,
        columns: Vec::new(),
    };
    let mut chunks = Vec::new();
    let mut push = |role, id, encoding, bytes: Vec<u8>, nulls| -> Result<()> {
        let (payload, bytes) = pack(compressor, &bytes, 0, encoding, false)?;
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
