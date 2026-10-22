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

//! Streaming aggregation and owned OpenDAL outputs.

use async_trait::async_trait;
use datatypes::arrow::array::{Array, BinaryArray, DictionaryArray};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use mito_codec::row_converter::SparseOffsetsCache;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;

use crate::error::{Result, SerdeJsonSnafu};
use crate::series_index::artifact::{ArtifactFormat, ArtifactMetadata, ArtifactObject};
use crate::series_index::artifact_writer::Outputs;
use crate::series_index::catalog::SeriesIndexEntry;
use crate::series_index::columnar::codec::{self, encode_dictionary, encode_integers};
use crate::series_index::columnar::metadata::{
    self, Block, Column, Page, PageReference, Trailer, pack,
};
use crate::series_index::columnar::{
    ColumnarSeriesFormat, MANIFEST_LIMIT, Manifest, Part, Tag, VERSION, invalid, require,
};
use crate::series_index::format::FormatWriter;
use crate::series_index::writer::{decode_series_identity, tag_columns, validate_metadata};
use crate::series_index::{MetricSeriesId, ROW_COUNT_COLUMN, TSID_COLUMN};

/// Maximum owned row bytes per block; a single larger row is flushed on its own.
const BLOCK_BYTE_BUDGET: usize = 8 * 1024 * 1024;

#[derive(Debug)]
struct Row {
    id: MetricSeriesId,
    count: u64,
    tags: Vec<Option<String>>,
}

pub(crate) struct ColumnarWriter {
    options: ColumnarSeriesFormat,
    manifest: Manifest,
    outputs: Outputs,
    tags: Vec<(u32, String)>,
    offsets: SparseOffsetsCache,
    buffer: Vec<u8>,
    last_key: Vec<u8>,
    current: Option<Row>,
    rows: Vec<Row>,
    row_bytes: usize,
    leaves: Vec<Vec<Block>>,
    leaf_bytes: usize,
    descriptor_bytes: usize,
    position: u64,
    part_rows: u64,
    failed: bool,
}
impl ColumnarWriter {
    pub(crate) fn new(
        options: ColumnarSeriesFormat,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        entry: &SeriesIndexEntry,
    ) -> Result<Self> {
        validate_metadata(&metadata)?;
        require(
            options.block_rows > 0
                && options.block_rows <= codec::COLUMN_LIMIT / 10
                && options.root_capacity > 0
                && options.root_capacity <= metadata::ROOT_CAPACITY
                && (12..=metadata::PAGE_TARGET).contains(&options.page_bytes),
            "invalid columnar writer options",
        )?;
        let tags = tag_columns(&metadata);
        let mut coverage = entry.clone();
        coverage.file_size = 0;
        coverage.artifact = None;
        let manifest = Manifest {
            format: ArtifactFormat::ColumnarSeries,
            version: VERSION,
            region_id: metadata.region_id.as_u64(),
            generation: entry.index_uuid,
            tags: tags
                .iter()
                .map(|(id, name)| Tag {
                    column_id: *id,
                    name: name.clone(),
                })
                .collect(),
            coverage,
            files: Vec::new(),
        };
        Ok(Self {
            options,
            manifest,
            outputs: Outputs {
                store,
                active: None,
                paths: Vec::new(),
                committed: false,
            },
            tags,
            offsets: SparseOffsetsCache::new(),
            buffer: Vec::new(),
            last_key: Vec::new(),
            current: None,
            rows: Vec::new(),
            row_bytes: 0,
            leaves: Vec::new(),
            leaf_bytes: 12,
            descriptor_bytes: 0,
            position: 0,
            part_rows: 0,
            failed: false,
        })
    }

    async fn push_current(&mut self) -> Result<()> {
        if let Some(row) = self.current.take() {
            let row_bytes = std::mem::size_of::<Row>()
                + row.tags.capacity() * std::mem::size_of::<Option<String>>()
                + row
                    .tags
                    .iter()
                    .flatten()
                    .map(String::capacity)
                    .sum::<usize>();
            if self.row_bytes.saturating_add(row_bytes) > BLOCK_BYTE_BUDGET
                || self
                    .rows
                    .first()
                    .is_some_and(|first| first.id.table_id != row.id.table_id)
            {
                self.flush_block().await?;
            }
            self.row_bytes += row_bytes;
            self.rows.push(row);
            if self.rows.len() >= self.options.block_rows || self.row_bytes >= BLOCK_BYTE_BUDGET {
                self.flush_block().await?;
            }
        }
        Ok(())
    }

    async fn append_key(&mut self, key: &[u8]) -> Result<()> {
        if key == self.last_key && self.current.is_some() {
            if let Some(row) = self.current.as_mut() {
                row.count = row
                    .count
                    .checked_add(1)
                    .ok_or_else(|| invalid("series row count overflow"))?;
            }
            return Ok(());
        }
        let (id, tags) =
            decode_series_identity(key, &self.tags, &mut self.offsets, &mut self.buffer)?;
        if let Some(row) = self.current.as_mut() {
            require(id >= row.id, "series identifiers are not sorted")?;
            if id == row.id {
                row.count = row
                    .count
                    .checked_add(1)
                    .ok_or_else(|| invalid("series row count overflow"))?;
                self.last_key.clear();
                self.last_key.extend(key);
                return Ok(());
            }
        }
        self.push_current().await?;
        self.current = Some(Row { id, count: 1, tags });
        self.last_key.clear();
        self.last_key.extend(key);
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        require(batch.num_columns() >= 4, "series input is not a flat batch")?;
        let pk = batch.column(crate::sst::parquet::flat_format::primary_key_column_index(
            batch.num_columns(),
        ));
        if let Some(keys) = pk.as_any().downcast_ref::<BinaryArray>() {
            require(keys.null_count() == 0, "null series primary key")?;
            for key in keys.iter().flatten() {
                self.append_key(key).await?;
            }
        } else if let Some(keys) = pk.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
            require(keys.null_count() == 0, "null series primary key")?;
            let values = keys
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("series dictionary values must be binary"))?;
            for key in keys.keys().values() {
                require(
                    (*key as usize) < values.len() && !values.is_null(*key as usize),
                    "invalid series dictionary primary key",
                )?;
                self.append_key(values.value(*key as usize)).await?;
            }
        } else {
            return Err(invalid(
                "series primary key must be binary or UInt32 dictionary",
            ));
        }
        Ok(())
    }

    async fn flush_block(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        self.row_bytes = 0;
        // TODO: benchmark inline versus offloaded codecs for latency, throughput,
        // and async-worker responsiveness before adding scheduling overhead.
        let (mut block, chunks) = encode_block(rows, &self.tags, &self.options)?;
        let mut descriptor = Vec::new();
        block.encode(&mut descriptor);
        require(
            descriptor.len() + 12 <= metadata::PAGE_LIMIT,
            "series descriptor exceeds leaf limit",
        )?;
        let needs_leaf =
            !self.leaves.is_empty() && self.leaf_bytes + descriptor.len() > self.options.page_bytes;
        if needs_leaf && self.leaves.len() == self.options.root_capacity {
            self.finish_part().await?;
        }
        if self.outputs.active.is_none() {
            let id = u32::try_from(self.manifest.files.len())
                .map_err(|_| invalid("too many series parts"))?;
            self.outputs.open(self.manifest.part_path(id)).await?;
            self.outputs.write(metadata::header()).await?;
            self.position = metadata::HEADER_SIZE;
        }
        for (column, chunk) in block.columns.iter_mut().zip(chunks) {
            column.payload.offset = self.position;
            self.position = column.payload.end()?;
            self.outputs.write(chunk).await?;
        }
        if self.leaves.is_empty() || self.leaf_bytes + descriptor.len() > self.options.page_bytes {
            self.leaves.push(Vec::new());
            self.leaf_bytes = 12;
        }
        self.leaf_bytes += descriptor.len();
        self.descriptor_bytes += descriptor.len();
        self.part_rows += u64::from(block.rows);
        self.leaves
            .last_mut()
            .ok_or_else(|| invalid("missing series leaf"))?
            .push(block);
        if self.position >= self.options.part_bytes
            || self.descriptor_bytes >= self.options.metadata_bytes
        {
            self.finish_part().await?;
        }
        Ok(())
    }

    async fn write_page(
        &mut self,
        page: Page,
        first: MetricSeriesId,
        last: MetricSeriesId,
    ) -> Result<PageReference> {
        let position = self.position;
        let (payload, bytes) = pack(
            &page.encode(),
            position,
            1,
            self.options.compression,
            self.options.zstd_level,
            true,
        )?;
        self.position = payload.end()?;
        self.outputs.write(bytes).await?;
        Ok(PageReference {
            first,
            last,
            payload,
        })
    }

    async fn finish_part(&mut self) -> Result<()> {
        if self.leaves.is_empty() {
            return Ok(());
        }
        let payload_end = self.position;
        let mut children = Vec::new();
        for leaf in std::mem::take(&mut self.leaves) {
            let first = leaf
                .first()
                .ok_or_else(|| invalid("empty series leaf"))?
                .first_id();
            let last = leaf
                .last()
                .ok_or_else(|| invalid("empty series leaf"))?
                .last_id();
            children.push(self.write_page(Page::Leaf(leaf), first, last).await?);
        }
        let first = children[0].first;
        let last = children[children.len() - 1].last;
        let depth = if children.len() == 1 { 1 } else { 2 };
        let root = if depth == 1 {
            children.remove(0)
        } else {
            self.write_page(Page::Internal(children), first, last)
                .await?
        };
        let file_length = self
            .position
            .checked_add(metadata::TRAILER_SIZE)
            .ok_or_else(|| invalid("series file length overflow"))?;
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
        self.manifest.files.push(Part {
            part_id: u32::try_from(self.manifest.files.len())
                .map_err(|_| invalid("too many series parts"))?,
            file_size: file_length,
            num_series: self.part_rows,
            first,
            last,
        });
        self.position = 0;
        self.part_rows = 0;
        self.descriptor_bytes = 0;
        self.leaf_bytes = 12;
        Ok(())
    }

    async fn complete(&mut self) -> Result<ArtifactMetadata> {
        require(!self.failed, "series writer failed previously")?;
        self.push_current().await?;
        self.flush_block().await?;
        self.finish_part().await?;
        let bytes = serde_json::to_vec(&self.manifest).context(SerdeJsonSnafu)?;
        require(
            bytes.len() <= MANIFEST_LIMIT,
            "series manifest exceeds size limit",
        )?;
        let mut file_size = bytes.len() as u64;
        let manifest_size = file_size;
        let mut objects = Vec::with_capacity(self.manifest.files.len() + 1);
        for part in &self.manifest.files {
            objects.push(ArtifactObject {
                path: self.manifest.part_path(part.part_id),
                file_size: part.file_size,
            });
            file_size = file_size
                .checked_add(part.file_size)
                .ok_or_else(|| invalid("series artifact length overflow"))?;
        }
        objects.push(ArtifactObject {
            path: self.manifest.path(),
            file_size: manifest_size,
        });
        self.outputs.open(self.manifest.path()).await?;
        self.outputs.write(bytes).await?;
        self.outputs.close().await?;
        self.outputs.committed = true;
        Ok(ArtifactMetadata {
            format: ArtifactFormat::ColumnarSeries,
            generation: self.manifest.generation,
            objects,
            file_size,
        })
    }
}

#[async_trait]
impl FormatWriter for ColumnarWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        require(!self.failed, "series writer failed previously")?;
        // A cancelled write must not allow a later finish to publish partial input.
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
            common_telemetry::warn!(error; "Cleaning failed series build");
        }
        result
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.outputs.cleanup().await
    }
}

fn encode_block(
    rows: Vec<Row>,
    tags: &[(u32, String)],
    options: &ColumnarSeriesFormat,
) -> Result<(Block, Vec<Vec<u8>>)> {
    let first = rows
        .first()
        .ok_or_else(|| invalid("empty series block"))?
        .id;
    let last = rows.last().ok_or_else(|| invalid("empty series block"))?.id;
    let mut block = Block {
        table_id: first.table_id,
        first: first.tsid,
        last: last.tsid,
        rows: rows.len() as u32,
        columns: Vec::new(),
    };
    let mut chunks = Vec::new();
    for (name, values) in [
        (
            TSID_COLUMN,
            rows.iter().map(|r| r.id.tsid).collect::<Vec<_>>(),
        ),
        (ROW_COUNT_COLUMN, rows.iter().map(|r| r.count).collect()),
    ] {
        let (payload, bytes) = pack(
            &encode_integers(&values),
            0,
            codec::INTEGER,
            options.compression,
            options.zstd_level,
            false,
        )?;
        block.columns.push(Column {
            name: name.into(),
            payload,
            null_count: 0,
            bounds: None,
        });
        chunks.push(bytes);
    }
    for (index, (_, name)) in tags.iter().enumerate() {
        let values: Vec<_> = rows.iter().map(|row| row.tags[index].as_deref()).collect();
        let null_count = values.iter().filter(|v| v.is_none()).count() as u32;
        if null_count == block.rows {
            continue;
        }
        let min = values
            .iter()
            .flatten()
            .min()
            .copied()
            .ok_or_else(|| invalid("missing tag minimum"))?;
        let max = values
            .iter()
            .flatten()
            .max()
            .copied()
            .ok_or_else(|| invalid("missing tag maximum"))?;
        let (payload, bytes) = pack(
            &encode_dictionary(&values)?,
            0,
            codec::DICTIONARY,
            options.compression,
            options.zstd_level,
            false,
        )?;
        block.columns.push(Column {
            name: name.clone(),
            payload,
            null_count,
            bounds: Some((min.to_owned(), max.to_owned())),
        });
        chunks.push(bytes);
    }
    Ok((block, chunks))
}
