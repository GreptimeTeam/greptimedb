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
use store_api::storage::FileId;

use super::codec;
use super::metadata::{self, Block, Column, Page, PageReference, Trailer, pack};
use super::{
    ColumnarSamplesFormat, MANIFEST_LIMIT, Manifest, MetricSeriesId, OPERATION, Part, SEQUENCE,
    TimestampColumn, TimestampUnit, VERSION, ValueColumn, invalid, require, valid_rows,
};
use crate::error::{Result, SerdeJsonSnafu};
use crate::series_index::artifact::{ArtifactFormat, ArtifactMetadata, ArtifactObject};
use crate::series_index::artifact_writer::Outputs;
use crate::series_index::format::FormatWriter;
use crate::series_index::writer::{decode_series_identity, validate_metadata};

struct Row {
    id: MetricSeriesId,
    time: i64,
    sequence: u64,
    operation: u8,
    fields: Vec<Option<u64>>,
}
pub(super) struct SamplesWriter {
    options: ColumnarSamplesFormat,
    manifest: Manifest,
    outputs: Outputs,
    offsets: SparseOffsetsCache,
    key_buffer: Vec<u8>,
    rows: Vec<Row>,
    last: Option<(MetricSeriesId, i64, u64)>,
    ordinal: u64,
    leaves: Vec<Vec<Block>>,
    leaf_bytes: usize,
    descriptor_bytes: usize,
    position: u64,
    part_rows: u64,
    part_blocks: u64,
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
            offsets: SparseOffsetsCache::new(),
            key_buffer: Vec::new(),
            rows: Vec::new(),
            last: None,
            ordinal: 0,
            leaves: Vec::new(),
            leaf_bytes: 12,
            descriptor_bytes: 0,
            position: 0,
            part_rows: 0,
            part_blocks: 0,
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
        let sequence = column(SEQUENCE)?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("invalid sample sequence type"))?;
        let operations = column(OPERATION)?
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
        let keys = column("__primary_key")?;
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
            // TODO: coordinate exact keys and future job/instance identity components
            // with the series format. This port assumes (table_id, tsid) is unique.
            let (id, _) =
                decode_series_identity(key, &[], &mut self.offsets, &mut self.key_buffer)?;
            let time = times.value(i);
            let seq = sequence.value(i);
            let op = operations.value(i);
            require(op <= 1, "unknown sample operation")?;
            if let Some((last_id, last_time, last_seq)) = self.last {
                require(
                    id >= last_id
                        && (id != last_id
                            || time > last_time
                            || (time == last_time && seq <= last_seq)),
                    "sample input is not sorted",
                )?;
                if id != last_id {
                    self.flush_block().await?;
                    self.ordinal = 0;
                }
            }
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
        let (mut block, chunks) = encode_block(&rows, &self.manifest, self.ordinal)?;
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("sample ordinal overflow"))?;
        let size = block.size();
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
            self.outputs.write(metadata::header()).await?;
            self.position = metadata::HEADER_SIZE;
        }
        for (column, chunk) in block.columns.iter_mut().zip(chunks) {
            column.payload.offset = self.position;
            self.position = column.payload.end()?;
            self.outputs.write(chunk).await?;
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
        if self.position >= self.options.part_bytes
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
        self.outputs.write(bytes).await?;
        Ok(PageReference { bounds, payload })
    }
    async fn finish_part(&mut self) -> Result<()> {
        if self.leaves.is_empty() {
            return Ok(());
        }
        let payload_end = self.position;
        let mut children = Vec::new();
        for leaf in std::mem::take(&mut self.leaves) {
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
        self.outputs
            .write(
                Trailer {
                    root,
                    depth,
                    file_length,
                    payload_end,
                }
                .encode(),
            )
            .await?;
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
fn encode_block(rows: &[Row], manifest: &Manifest, ordinal: u64) -> Result<(Block, Vec<Vec<u8>>)> {
    let first = rows.first().ok_or_else(|| invalid("empty sample block"))?;
    let last = rows.last().ok_or_else(|| invalid("empty sample block"))?;
    let mut block = Block {
        id: first.id,
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
