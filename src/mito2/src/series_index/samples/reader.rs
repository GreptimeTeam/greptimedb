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

//! Lazy, selective sample reads with independent traversal and pinned lifetimes.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use datatypes::arrow::array::{
    ArrayRef, Float64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt32Array, UInt64Array,
};
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use futures::stream::BoxStream;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{NewRecordBatchSnafu, OpenDalSnafu, Result, SerdeJsonSnafu};
use crate::series_index::artifact::{ArtifactFormat, SamplesFileHandle};
use crate::series_index::format::{SamplesFormatReader, SamplesReadRequest};
use crate::series_index::samples::codec;
use crate::series_index::samples::metadata::{
    self, Block, Bounds, Column, Page, PageReference, Trailer,
};
use crate::series_index::samples::{
    MANIFEST_LIMIT, Manifest, MetricSeriesId, TimestampUnit, VERSION, invalid, require, unsupported,
};
use crate::sst::parquet::helper::fetch_byte_ranges;

pub(super) struct SamplesReader {
    store: ObjectStore,
    manifest: Arc<Manifest>,
    handle: SamplesFileHandle,
}
impl SamplesReader {
    pub async fn open(store: ObjectStore, handle: SamplesFileHandle) -> Result<Self> {
        let entry = handle.entry();
        let artifact = &entry.artifact;
        require(
            artifact.format == ArtifactFormat::ColumnarSamples,
            "samples artifact format mismatch",
        )?;
        let region = handle.file_id().region_id().as_u64();
        require(
            handle.file_id().file_id() == entry.file_id,
            "samples source handle mismatch",
        )?;
        let path = format!(
            "{region}/samples/{}/{}/manifest.json",
            entry.file_id, artifact.generation
        );
        let mut objects = HashMap::new();
        for object in &artifact.objects {
            require(
                objects
                    .insert(object.path.as_str(), object.file_size)
                    .is_none(),
                "duplicate sample artifact object",
            )?;
        }
        let length = *objects
            .get(path.as_str())
            .ok_or_else(|| invalid("missing samples manifest"))?;
        require(
            length > 0 && length <= MANIFEST_LIMIT as u64,
            "samples manifest exceeds limit",
        )?;
        let bytes = read_range(&store, &path, 0, length).await?;
        let manifest: Manifest = serde_json::from_slice(&bytes).context(SerdeJsonSnafu)?;
        if manifest.version != VERSION {
            return Err(unsupported("samples manifest version"));
        }
        require(
            manifest.format == artifact.format
                && manifest.region_id == region
                && manifest.source_file_id == entry.file_id
                && manifest.generation == artifact.generation,
            "samples manifest identity mismatch",
        )?;
        manifest.validate_schema()?;
        let mut paths = HashSet::from([path]);
        let mut total = length;
        let mut previous = None;
        for (index, part) in manifest.files.iter().enumerate() {
            require(
                index as u64 == u64::from(part.part_id)
                    && part.num_rows > 0
                    && part.num_blocks > 0
                    && part.num_blocks <= part.num_rows
                    && part.first <= part.last
                    && part.min_timestamp <= part.max_timestamp
                    && part.file_size >= metadata::HEADER_SIZE + metadata::TRAILER_SIZE,
                "invalid sample part descriptor",
            )?;
            if let Some(last) = previous {
                require(last <= part.first, "unordered sample parts")?;
            }
            previous = Some(part.last);
            let part_path = manifest.part_path(part.part_id);
            require(
                objects.get(part_path.as_str()) == Some(&part.file_size),
                "sample part size mismatch",
            )?;
            paths.insert(part_path);
            total = total
                .checked_add(part.file_size)
                .ok_or_else(|| invalid("sample artifact size overflow"))?;
        }
        require(
            total == artifact.file_size
                && paths.len() == artifact.objects.len()
                && artifact.objects.iter().all(|o| paths.contains(&o.path)),
            "sample artifact objects/size mismatch",
        )?;
        Ok(Self {
            store,
            manifest: Arc::new(manifest),
            handle,
        })
    }
}
impl SamplesFormatReader for SamplesReader {
    fn schema(&self) -> SchemaRef {
        self.manifest.schema()
    }
    fn read(&self, request: SamplesReadRequest) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let schema = self.schema();
        let mut projection = Vec::new();
        let mut seen = HashSet::new();
        for name in &request.projection {
            require(seen.insert(name), "duplicate sample projection")?;
            projection.push(
                schema
                    .index_of(name)
                    .map_err(|_| invalid(format!("unknown sample projection {name}")))?,
            );
        }
        require(
            request
                .start
                .zip(request.end)
                .is_none_or(|(start, end)| start <= end),
            "reversed sample time range",
        )?;
        let empty = request.ids.as_ref().is_some_and(|ids| ids.is_empty())
            || request.start.zip(request.end).is_some_and(|(a, b)| a == b);
        let complete = request.ids.is_none()
            && request.table_id.is_none()
            && request.start.is_none()
            && request.end.is_none();
        let projected = Arc::new(
            schema
                .project(&projection)
                .map_err(|e| invalid(format!("sample projection: {e}")))?,
        );
        let manifest = self.manifest.clone();
        let store = self.store.clone();
        let handle = self.handle.clone();
        Ok(Box::pin(async_stream::try_stream! {
            let _handle = handle;
            if empty { return; }
            let mut previous: Option<Block> = None;
            let mut adjacent = true;
            for part in &manifest.files {
                let bounds = Bounds { first: part.first, last: part.last, min: part.min_timestamp, max: part.max_timestamp };
                if !matches(bounds, &request) { adjacent = false; continue; }
                let path = manifest.part_path(part.part_id);
                let header = read_range(&store, &path, 0, metadata::HEADER_SIZE).await?;
                metadata::validate_header(&header)?;
                let bytes = read_range(&store, &path, part.file_size - metadata::TRAILER_SIZE, part.file_size).await?;
                let trailer = Trailer::decode(&bytes, part.file_size)?;
                require(trailer.root.bounds == bounds, "sample root/part bounds mismatch")?;
                let root = load_metadata_page(&store, &path, &trailer.root, trailer.depth, trailer.payload_end, &manifest).await?;
                let (mut active, children) = match root { Page::Leaf(blocks) => (Some(blocks), Vec::new()), Page::Internal(children) => (None, children) };
                let mut children = children.into_iter(); let mut row_count = 0u64; let mut block_count = 0u64;
                let mut payload_position = metadata::HEADER_SIZE;
                loop {
                    let blocks = if let Some(blocks) = active.take() { blocks } else {
                        let Some(child) = children.next() else { break; };
                        if !matches(child.bounds, &request) { adjacent = false; continue; }
                        match load_metadata_page(&store, &path, &child, 1, trailer.payload_end, &manifest).await? {
                            Page::Leaf(blocks) => blocks,
                            Page::Internal(_) => Err(unsupported("internal child in sample depth-two tree"))?,
                        }
                    };
                    for block in blocks {
                        row_count = row_count.checked_add(u64::from(block.rows)).ok_or_else(|| invalid("samples row count overflow"))?;
                        block_count += 1;
                        if let Some(prior) = &previous { block.follows(prior, adjacent)?; }
                        else if complete { require(block.ordinal == 0, "initial sample ordinal is not zero")?; }
                        for column in &block.columns {
                            column.payload.validate(payload_position, trailer.payload_end, false)?;
                            payload_position = column.payload.end()?;
                        }
                        let batch = if matches(block.bounds(), &request) {
                            read_block(&store, &path, &block, &manifest, &request, &projection, projected.clone()).await?
                        } else { None };
                        if let Some(batch) = batch { yield batch; }
                        previous = Some(block); adjacent = true;
                    }
                }
                if complete { require(row_count == part.num_rows && block_count == part.num_blocks, "sample part totals mismatch")?; }
            }
        }))
    }
}
fn matches(bounds: Bounds, request: &SamplesReadRequest) -> bool {
    if request.start.is_some_and(|start| bounds.max < start)
        || request.end.is_some_and(|end| bounds.min >= end)
    {
        return false;
    }
    let mut first = bounds.first;
    let mut last = bounds.last;
    if let Some(table_id) = request.table_id {
        first = first.max(MetricSeriesId { table_id, tsid: 0 });
        last = last.min(MetricSeriesId {
            table_id,
            tsid: u64::MAX,
        });
    }
    first <= last
        && request
            .ids
            .as_ref()
            .is_none_or(|ids| ids.range(first..=last).next().is_some())
}
async fn read_range(store: &ObjectStore, path: &str, start: u64, end: u64) -> Result<bytes::Bytes> {
    require(start <= end, "reversed sample byte range")?;
    let mut buffers = fetch_byte_ranges(path, store.clone(), std::slice::from_ref(&(start..end)))
        .await
        .context(OpenDalSnafu)?;
    let bytes = buffers
        .pop()
        .ok_or_else(|| invalid("missing sample range response"))?;
    require(
        buffers.is_empty() && bytes.len() as u64 == end - start,
        "sample range length mismatch",
    )?;
    Ok(bytes)
}
/// Cache integration can key this entrypoint by immutable generation, range and codecs.
async fn load_metadata_page(
    store: &ObjectStore,
    path: &str,
    reference: &PageReference,
    depth: u32,
    payload_end: u64,
    manifest: &Manifest,
) -> Result<Page> {
    if reference.payload.encoding != 1 {
        return Err(unsupported("sample metadata codec"));
    }
    let bytes = read_range(
        store,
        path,
        reference.payload.offset,
        reference.payload.end()?,
    )
    .await?;
    Page::decode(
        &reference.payload.unpack(&bytes)?,
        reference,
        depth,
        payload_end,
        manifest,
    )
}
/// Keep column loads separate from metadata for future Mito cache integration.
async fn load_column_chunk(store: &ObjectStore, path: &str, column: &Column) -> Result<Vec<u8>> {
    let allowed = match column.role {
        1 => matches!(column.payload.encoding, 1 | 2),
        2 => matches!(column.payload.encoding, 3 | 4),
        3 => matches!(column.payload.encoding, 6 | 7),
        4 => column.payload.encoding == 5,
        _ => false,
    };
    if !allowed {
        return Err(unsupported("samples column encoding"));
    }
    let bytes = read_range(store, path, column.payload.offset, column.payload.end()?).await?;
    column.payload.unpack(&bytes)
}
fn timestamp_array(values: impl Iterator<Item = i64>, unit: TimestampUnit) -> ArrayRef {
    match unit {
        TimestampUnit::Second => Arc::new(TimestampSecondArray::from_iter_values(values)),
        TimestampUnit::Millisecond => Arc::new(TimestampMillisecondArray::from_iter_values(values)),
        TimestampUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from_iter_values(values)),
        TimestampUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from_iter_values(values)),
    }
}
async fn read_block(
    store: &ObjectStore,
    path: &str,
    block: &Block,
    manifest: &Manifest,
    request: &SamplesReadRequest,
    projection: &[usize],
    schema: SchemaRef,
) -> Result<Option<RecordBatch>> {
    let mut selected: Vec<usize> = (0..block.rows as usize).collect();
    let mut times = None;
    if request.start.is_some() || request.end.is_some() || projection.contains(&2) {
        let column = &block.columns[0];
        let values = codec::decode_numbers(
            &load_column_chunk(store, path, column).await?,
            column.payload.encoding,
            block.rows as usize,
        )?;
        require(
            values.first().map(|v| *v as i64) == Some(block.min)
                && values.last().map(|v| *v as i64) == Some(block.max),
            "sample timestamp bounds mismatch",
        )?;
        selected.retain(|&i| {
            request
                .start
                .is_none_or(|start| (values[i] as i64) >= start)
                && request.end.is_none_or(|end| (values[i] as i64) < end)
        });
        if selected.is_empty() {
            return Ok(None);
        }
        times = Some(values);
    }
    let mut arrays: BTreeMap<usize, ArrayRef> = BTreeMap::new();
    // Visit columns in physical order, regardless of requested output order.
    let mut sorted = projection.to_vec();
    sorted.sort_unstable();
    for index in sorted {
        let array: ArrayRef = match index {
            0 => Arc::new(UInt32Array::from(vec![block.id.table_id; selected.len()])),
            1 => Arc::new(UInt64Array::from(vec![block.id.tsid; selected.len()])),
            2 => {
                let values = times
                    .as_ref()
                    .ok_or_else(|| invalid("missing decoded sample time"))?;
                timestamp_array(
                    selected.iter().map(|&i| values[i] as i64),
                    manifest.timestamp.unit,
                )
            }
            3 => {
                let column = &block.columns[1];
                let values = codec::decode_numbers(
                    &load_column_chunk(store, path, column).await?,
                    column.payload.encoding,
                    block.rows as usize,
                )?;
                if let Some(times) = &times {
                    require(
                        (1..values.len())
                            .all(|i| times[i - 1] != times[i] || values[i - 1] >= values[i]),
                        "unordered sample sequences",
                    )?;
                }
                Arc::new(UInt64Array::from_iter_values(
                    selected.iter().map(|&i| values[i]),
                ))
            }
            4 => {
                let column = &block.columns[2];
                let values = codec::decode_operations(
                    &load_column_chunk(store, path, column).await?,
                    column.payload.encoding,
                    block.rows as usize,
                )?;
                Arc::new(UInt8Array::from_iter_values(
                    selected.iter().map(|&i| values[i]),
                ))
            }
            field => {
                let id = manifest.fields[field - 5].column_id;
                if let Some(column) = block.columns.iter().find(|c| c.role == 4 && c.id == id) {
                    let values = codec::decode_float(
                        &load_column_chunk(store, path, column).await?,
                        block.rows as usize,
                        column.nulls,
                    )?;
                    Arc::new(Float64Array::from_iter(
                        selected.iter().map(|&i| values[i].map(f64::from_bits)),
                    ))
                } else {
                    Arc::new(Float64Array::from(vec![None::<f64>; selected.len()]))
                }
            }
        };
        arrays.insert(index, array);
    }
    let output = projection
        .iter()
        .map(|i| {
            arrays
                .remove(i)
                .ok_or_else(|| invalid("missing sample projected column"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(
        RecordBatch::try_new_with_options(
            schema,
            output,
            &RecordBatchOptions::new().with_row_count(Some(selected.len())),
        )
        .context(NewRecordBatchSnafu)?,
    ))
}
