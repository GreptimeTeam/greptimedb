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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datatypes::arrow::array::{
    ArrayRef, BinaryArray, Float64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt32Array, UInt64Array,
};
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use futures::stream::BoxStream;
use object_store::{ObjectStore, Reader};
use snafu::ResultExt;
use tokio::sync::OnceCell;

use crate::cache::columnar::{
    self as metadata_cache, MetadataCache, PayloadCache, PayloadPartCache,
};
use crate::error::{NewRecordBatchSnafu, OpenDalSnafu, Result};
use crate::metrics::ColumnarReadMetrics;
use crate::series_index::artifact::{ArtifactFormat, SamplesFileHandle};
use crate::series_index::format::{SamplesFormatReader, SamplesReadRequest};
use crate::series_index::identity::{ExactSeriesKey, IdentityEncoding};
use crate::series_index::samples::codec;
use crate::series_index::samples::metadata::{
    self, Block, BlockOrder, Bounds, Column, Page, PageReference, Trailer,
};
use crate::series_index::samples::{
    MANIFEST_LIMIT, Manifest, MetricSeriesId, TimestampUnit, VERSION, invalid, require, unsupported,
};
use crate::sst::parquet::helper::fetch_byte_ranges;

pub(crate) struct SamplesReader {
    store: ObjectStore,
    manifest: Arc<Manifest>,
    schema: SchemaRef,
    cache: Option<MetadataCache>,
    payload_cache: Option<PayloadCache>,
    handle: SamplesFileHandle,
    parts: Arc<Vec<OnceCell<Reader>>>,
}
impl SamplesReader {
    pub(crate) async fn open_cached(
        store: ObjectStore,
        handle: SamplesFileHandle,
        cache: Option<MetadataCache>,
    ) -> Result<Self> {
        let identity = format!("{:?}/{:?}", handle.file_id(), handle.entry());
        let descriptor = metadata_cache::load(
            cache.as_ref(),
            identity,
            |(manifest, schema): &(Arc<Manifest>, SchemaRef)| {
                manifest.retained_size()
                    + 8 * std::mem::size_of::<usize>()
                    + std::mem::size_of::<datatypes::arrow::datatypes::Schema>()
                    + schema
                        .fields()
                        .iter()
                        .map(|field| field.size() + std::mem::size_of::<usize>())
                        .sum::<usize>()
            },
            async {
                let reader = Self::open(store.clone(), handle.clone()).await?;
                Ok((reader.manifest, reader.schema))
            },
        )
        .await?;
        Ok(Self {
            store,
            handle,
            parts: Arc::new(
                (0..descriptor.0.files.len())
                    .map(|_| OnceCell::new())
                    .collect(),
            ),
            manifest: descriptor.0.clone(),
            schema: descriptor.1.clone(),
            cache,
            payload_cache: None,
        })
    }

    pub(crate) fn with_payload_cache(mut self, cache: Option<PayloadCache>) -> Self {
        self.payload_cache = cache;
        self
    }

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
            "{}manifest.json",
            crate::series_index::samples::samples_prefix(region, artifact.generation)
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
        // A manifest that the current format cannot read predates exact identity or is
        // corrupt; either way it must never be reused.
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|error| {
            invalid(format!(
                "samples manifest is not readable; rebuild the artifact: {error}"
            ))
        })?;
        if manifest.version != VERSION {
            return Err(unsupported("samples manifest version"));
        }
        // Old artifacts can be structurally valid and perfectly ordered, so identity
        // encoding is the only thing that keeps their lost labels out of reads.
        if manifest.identity_encoding != IdentityEncoding::CURRENT {
            return Err(unsupported(
                "samples manifest identity encoding; rebuild the samples artifact",
            ));
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
            parts: Arc::new((0..manifest.files.len()).map(|_| OnceCell::new()).collect()),
            schema: manifest.schema(),
            cache: None,
            payload_cache: None,
            manifest: Arc::new(manifest),
            handle,
        })
    }
}
impl SamplesFormatReader for SamplesReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        let empty = request.keys.as_ref().is_some_and(|keys| keys.is_empty())
            || request.start.zip(request.end).is_some_and(|(a, b)| a == b);
        let complete = request.keys.is_none()
            && request.table_id.is_none()
            && request.start.is_none()
            && request.end.is_none();
        // Coarse pruning bounds come from the exact selection itself, so a reader can never
        // prune away a part or page that owns one of the selected series.
        let ids: Option<BTreeSet<MetricSeriesId>> = request
            .keys
            .as_ref()
            .map(|keys| keys.iter().map(ExactSeriesKey::id).collect());
        let projected = Arc::new(
            schema
                .project(&projection)
                .map_err(|e| invalid(format!("sample projection: {e}")))?,
        );
        let field_ids: HashSet<_> = projection
            .iter()
            .filter(|&&index| index >= 5)
            // The exact series key column is appended after the data fields and has no
            // column id of its own.
            .filter_map(|&index| self.manifest.fields.get(index - 5))
            .map(|field| field.column_id)
            .collect();
        let cache = self.cache.clone();
        let manifest = self.manifest.clone();
        let store = self.store.clone();
        let handle = self.handle.clone();
        let parts = self.parts.clone();
        let payload_cache = self.payload_cache.clone();
        Ok(Box::pin(async_stream::try_stream! {
            let _handle = &handle;
            let mut metrics = ColumnarReadMetrics::default();
            if empty { return; }
            let mut previous: Option<BlockOrder> = None;
            let mut adjacent = true;
            for part in &manifest.files {
                let bounds = Bounds { first: part.first, last: part.last, min: part.min_timestamp, max: part.max_timestamp };
                if !matches(bounds, &request, ids.as_ref()) { adjacent = false; continue; }
                let path = manifest.part_path(part.part_id);
                // One lazy reader per accessed part, shared across this query's streams.
                let reader = parts[part.part_id as usize].get_or_try_init(|| async {
                    store.reader_with(&path).concurrent(8).gap(0).await.context(OpenDalSnafu)
                }).await?;
                let size = part.file_size;
                let metadata_started = Instant::now();
                let trailer = metadata_cache::load(cache.as_ref(), format!("{path}/{size}/framing"), |_| std::mem::size_of::<Trailer>(), async {
                let header = read_part_range(reader, 0, metadata::HEADER_SIZE).await?;
                metadata::validate_header(&header)?;
                let bytes = read_part_range(reader, part.file_size - metadata::TRAILER_SIZE, part.file_size).await?;
                let trailer = Trailer::decode(&bytes, part.file_size)?;
                    Ok(trailer)
                }).await?;
                let payload_reader = PayloadReader {
                    reader,
                    cache: payload_cache.as_ref().map(|cache| cache.part(handle.entry().artifact.generation, part.part_id, size)),
                    end: trailer.payload_end,
                };
                require(trailer.root.bounds == bounds, "sample root/part bounds mismatch")?;
                let root = load_metadata_page(cache.as_ref(), reader, (&path, size), &trailer.root, trailer.depth, trailer.payload_end, &manifest).await?;
                metrics.add_elapsed("columnar_metadata_wall", metadata_started.elapsed());
                let (mut active, children) = match root.as_ref() { Page::Leaf(_) => (Some(root.clone()), Vec::new()), Page::Internal(children) => (None, children.clone()) };
                let mut children = children.into_iter(); let mut row_count = 0u64; let mut block_count = 0u64;
                let mut payload_position = metadata::HEADER_SIZE;
                loop {
                    let page = if let Some(page) = active.take() { page } else {
                        let Some(child) = children.next() else { break; };
                        if !matches(child.bounds, &request, ids.as_ref()) { adjacent = false; continue; }
                        let started = Instant::now();
                        let page = load_metadata_page(cache.as_ref(), reader, (&path, size), &child, 1, trailer.payload_end, &manifest).await?;
                        metrics.add_elapsed("columnar_metadata_wall", started.elapsed());
                        page
                    };
                    let blocks = match page.as_ref() { Page::Leaf(blocks) => blocks, Page::Internal(_) => Err(unsupported("internal child in sample depth-two tree"))? };
                    let mut prefetched = PrefetchedBlocks::default();
                    for (block_index, block) in blocks.iter().enumerate() {
                        row_count = row_count.checked_add(u64::from(block.rows)).ok_or_else(|| invalid("samples row count overflow"))?;
                        block_count += 1;
                        if let Some(prior) = &previous { block.follows(prior, adjacent)?; }
                        else if complete { require(block.ordinal == 0, "initial sample ordinal is not zero")?; }
                        for column in &block.columns {
                            column.payload.validate(payload_position, trailer.payload_end, false)?;
                            payload_position = column.payload.end()?;
                        }
                        let batch = if matches(block.bounds(), &request, ids.as_ref()) && owns(block, &request) {
                            if block_index >= prefetched.end {
                                // Release the previous window before reserving the next one.
                                drop(std::mem::take(&mut prefetched));
                                prefetched = prefetch_blocks(&payload_reader, blocks, block_index, &field_ids, &request, ids.as_ref(), &projection, trailer.payload_end, &mut metrics).await?;
                            }
                            *metrics.counts.entry("blocks").or_default() += 1;
                            *metrics.counts.entry("decoded_rows").or_default() += u64::from(block.rows);
                            read_block(&payload_reader, block, &manifest, &request, &projection, &field_ids, projected.clone(), &mut prefetched, &mut metrics).await?
                        } else { None };
                        if let Some((batch, _reservation)) = batch {
                            *metrics.counts.entry("batches").or_default() += 1;
                            *metrics.counts.entry("output_rows").or_default() += batch.num_rows() as u64;
                            yield batch;
                        }
                        previous = Some(block.order()); adjacent = true;
                    }
                }
                if complete { require(row_count == part.num_rows && block_count == part.num_blocks, "sample part totals mismatch")?; }
            }
        }))
    }
}
/// Every block records the exact series key that owns its rows. A coarse identifier
/// match is not ownership: a colliding partner shares the identifier.
fn owns(block: &Block, request: &SamplesReadRequest) -> bool {
    request
        .keys
        .as_ref()
        .is_none_or(|keys| keys.contains(&block.key))
}

/// Bounds-only pruning. Selection itself is exact and decided by [`owns`].
fn matches(
    bounds: Bounds,
    request: &SamplesReadRequest,
    ids: Option<&BTreeSet<MetricSeriesId>>,
) -> bool {
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
    first <= last && ids.is_none_or(|ids| ids.range(first..=last).next().is_some())
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
/// Reuses validated immutable metadata; traversal remains local to each stream.
async fn load_metadata_page(
    cache: Option<&MetadataCache>,
    reader: &Reader,
    object: (&str, u64),
    reference: &PageReference,
    depth: u32,
    payload_end: u64,
    manifest: &Manifest,
) -> Result<Arc<Page>> {
    let (path, size) = object;
    metadata_cache::load(
        cache,
        format!("{path}/{size}/{reference:?}/{depth}/{payload_end}"),
        Page::retained_size,
        async {
            if reference.payload.encoding != 1 {
                return Err(unsupported("sample metadata codec"));
            }
            let bytes =
                read_part_range(reader, reference.payload.offset, reference.payload.end()?).await?;
            Page::decode(
                &reference.payload.unpack(&bytes)?,
                reference,
                depth,
                payload_end,
                manifest,
            )
        },
    )
    .await
}
/// Keep column loads separate from metadata for future Mito cache integration.
fn load_column_chunk<'a>(
    buffers: &'a HashMap<u64, bytes::Bytes>,
    column: &Column,
    decoder: &'a mut codec::PayloadDecoder,
) -> Result<&'a [u8]> {
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
    let bytes = buffers
        .get(&column.payload.offset)
        .ok_or_else(|| invalid("missing prefetched sample column"))?;
    column.payload.unpack_with_decoder(bytes, decoder)
}
fn timestamp_array(values: impl Iterator<Item = i64>, unit: TimestampUnit) -> ArrayRef {
    match unit {
        TimestampUnit::Second => Arc::new(TimestampSecondArray::from_iter_values(values)),
        TimestampUnit::Millisecond => Arc::new(TimestampMillisecondArray::from_iter_values(values)),
        TimestampUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from_iter_values(values)),
        TimestampUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from_iter_values(values)),
    }
}
#[allow(clippy::too_many_arguments)]
async fn read_block(
    reader: &PayloadReader<'_>,
    block: &Block,
    manifest: &Manifest,
    request: &SamplesReadRequest,
    projection: &[usize],
    field_ids: &HashSet<u32>,
    schema: SchemaRef,
    prefetched: &mut PrefetchedBlocks,
    metrics: &mut ColumnarReadMetrics,
) -> Result<Option<(RecordBatch, Option<MemoryReservation>)>> {
    let boundary = request.start.is_some_and(|start| start > block.min)
        || request.end.is_some_and(|end| end <= block.max);
    let needs_time = boundary || projection.contains(&2);
    let columns: Vec<_> = block
        .columns
        .iter()
        .filter(|column| match column.role {
            1 => needs_time,
            2 => projection.contains(&3),
            3 => projection.contains(&4),
            4 => field_ids.contains(&column.id),
            _ => false,
        })
        .collect();
    let reservation = request
        .memory_pool
        .as_ref()
        .map(|pool| MemoryConsumer::new("ColumnarSamplesBuffers").register(pool));
    // Stored buffers, decompression, typed values, selection, and Arrow output coexist.
    // Reserve before any payload I/O/allocation. The reservation survives the yield.
    let ranges = columns
        .iter()
        .map(|c| Ok(c.payload.offset..c.payload.end()?))
        .collect::<Result<Vec<_>>>()?;
    let gap_bytes = merge_read_ranges(&ranges)
        .iter()
        .map(|r| r.end - r.start)
        .sum::<u64>()
        - ranges.iter().map(|r| r.end - r.start).sum::<u64>();
    // The exact key column copies one key per output row, so its per-row budget is the
    // key itself rather than a fixed estimate: an 8 KiB key over one block of 8192 rows
    // allocates 64 MiB. Every row is budgeted because time pruning has not happened yet
    // and can only lower the count.
    let key_bytes = if projection.contains(&manifest.primary_key_index()) {
        block.key.bytes().len()
    } else {
        0
    };
    let base_bytes = 2 * gap_bytes as usize
        + columns
            .iter()
            .map(|column| column.payload.stored as usize + column.payload.decoded as usize)
            .sum::<usize>()
        + block.rows as usize
            * (32 * (projection.len() + 1) + std::mem::size_of::<usize>() + key_bytes)
        + columns.len() * 128;
    let bytes = base_bytes + reader.reservation_bytes(&ranges);
    let mut bypass_cache = false;
    if let Some(reservation) = &reservation {
        let mut result = reservation.try_grow(bytes);
        if result.is_err() && !prefetched.buffers.is_empty() {
            // Read-ahead must not prevent a block that fits on its own from being read.
            // Drop every shared slice and its reservation before retrying, and avoid
            // attempting the same window again on the next block.
            *prefetched = PrefetchedBlocks {
                end: prefetched.end,
                ..Default::default()
            };
            result = reservation.try_grow(bytes);
        }
        if result.is_err() && reader.cache.is_some() {
            // Alignment is optional. A block that fits without it must remain readable,
            // including on cache hits, whose shared backing buffers still need accounting.
            result = reservation.try_grow(base_bytes);
            bypass_cache = result.is_ok();
        }
        result.context(crate::error::ScanMemoryExhaustedSnafu)?;
    }
    let uncached = PayloadReader {
        reader: reader.reader,
        cache: None,
        end: reader.end,
    };
    let reader = if bypass_cache { &uncached } else { reader };
    let mut buffers = HashMap::new();
    for column in &columns {
        if let Some(bytes) = prefetched.buffers.get(&column.payload.offset) {
            buffers.insert(column.payload.offset, bytes.clone());
        }
    }
    if boundary {
        fetch_columns(reader, &[&block.columns[0]], &mut buffers, metrics).await?;
    } else {
        fetch_columns(reader, &columns, &mut buffers, metrics).await?;
    }
    let mut decoder = codec::PayloadDecoder::default();
    let decode_started = Instant::now();
    let mut selected: Vec<usize> = (0..block.rows as usize).collect();
    let mut times = None;
    if needs_time {
        let column = &block.columns[0];
        let values = codec::decode_numbers(
            load_column_chunk(&buffers, column, &mut decoder)?,
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
            metrics.add_elapsed("columnar_decode", decode_started.elapsed());
            return Ok(None);
        }
        times = Some(values);
    }
    metrics.add_elapsed("columnar_decode", decode_started.elapsed());
    if boundary {
        // Release decoder scratch and context before waiting for the remaining payloads.
        drop(decoder);
        let remaining = columns
            .into_iter()
            .filter(|column| column.role != 1)
            .collect::<Vec<_>>();
        fetch_columns(reader, &remaining, &mut buffers, metrics).await?;
        decoder = codec::PayloadDecoder::default();
    }
    let decode_started = Instant::now();
    let mut arrays: BTreeMap<usize, ArrayRef> = BTreeMap::new();
    // Visit columns in physical order, regardless of requested output order.
    let mut sorted = projection.to_vec();
    sorted.sort_unstable();
    let key_index = manifest.primary_key_index();
    for index in sorted {
        let array: ArrayRef = if index == key_index {
            // One block belongs to exactly one exact series; the value is repeated as a
            // plain binary column so consumers can attribute every row.
            Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
                block.key.bytes(),
                selected.len(),
            )))
        } else if index == 0 {
            Arc::new(UInt32Array::from(vec![
                block.key.id().table_id;
                selected.len()
            ]))
        } else if index == 1 {
            Arc::new(UInt64Array::from(vec![block.key.id().tsid; selected.len()]))
        } else {
            match index {
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
                        load_column_chunk(&buffers, column, &mut decoder)?,
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
                        load_column_chunk(&buffers, column, &mut decoder)?,
                        column.payload.encoding,
                        block.rows as usize,
                    )?;
                    Arc::new(UInt8Array::from_iter_values(
                        selected.iter().map(|&i| values[i]),
                    ))
                }
                field => {
                    let field = manifest
                        .fields
                        .get(field - 5)
                        .ok_or_else(|| invalid("unknown sample field"))?;
                    let id = field.column_id;
                    if let Some(column) = block.columns.iter().find(|c| c.role == 4 && c.id == id) {
                        let values = codec::decode_float(
                            load_column_chunk(&buffers, column, &mut decoder)?,
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
    let batch = RecordBatch::try_new_with_options(
        schema,
        output,
        &RecordBatchOptions::new().with_row_count(Some(selected.len())),
    )
    .context(NewRecordBatchSnafu)?;
    metrics.add_elapsed("columnar_decode", decode_started.elapsed());
    Ok(Some((batch, reservation)))
}

async fn read_part_range(reader: &Reader, start: u64, end: u64) -> Result<bytes::Bytes> {
    require(start <= end, "reversed sample byte range")?;
    let bytes = reader
        .read(start..end)
        .await
        .context(OpenDalSnafu)?
        .to_bytes();
    require(
        bytes.len() as u64 == end - start,
        "sample range length mismatch",
    )?;
    Ok(bytes)
}

/// Fetch selected ranges with bounded gap coalescing and shared-buffer slices.
/// The read window and scan reservation bound retained buffers;
/// OpenDAL bounds concurrency to 8.
async fn fetch_columns(
    reader: &PayloadReader<'_>,
    columns: &[&Column],
    buffers: &mut HashMap<u64, bytes::Bytes>,
    metrics: &mut ColumnarReadMetrics,
) -> Result<()> {
    let columns: Vec<_> = columns
        .iter()
        .copied()
        .filter(|c| !buffers.contains_key(&c.payload.offset))
        .collect();
    if columns.is_empty() {
        return Ok(());
    }
    let ranges = columns
        .iter()
        .map(|column| Ok(column.payload.offset..column.payload.end()?))
        .collect::<Result<Vec<_>>>()?;
    let merged = merge_read_ranges(&ranges);
    *metrics.counts.entry("column_ranges").or_default() += ranges.len() as u64;
    *metrics.bytes.entry("requested").or_default() +=
        ranges.iter().map(|r| r.end - r.start).sum::<u64>();
    if reader.cache.is_some() {
        return fetch_cached_columns(reader, &columns, &ranges, buffers, metrics).await;
    }
    *metrics.counts.entry("payload_reads").or_default() += merged.len() as u64;
    let started = Instant::now();
    let fetched = reader
        .reader
        .fetch(merged.clone())
        .await
        .context(OpenDalSnafu)?;
    metrics.add_elapsed("columnar_payload_fetch_wall", started.elapsed());
    require(
        fetched.len() == merged.len(),
        "sample fetch response count mismatch",
    )?;
    let mut column_index = 0;
    for (range, buffer) in merged.iter().zip(fetched) {
        let bytes = buffer.to_bytes();
        require(
            bytes.len() as u64 == range.end - range.start,
            "sample range length mismatch",
        )?;
        *metrics.bytes.entry("fetched").or_default() += bytes.len() as u64;
        while let Some(column) = columns.get(column_index) {
            if column.payload.offset >= range.end {
                break;
            }
            require(
                column.payload.offset >= range.start && column.payload.end()? <= range.end,
                "sample column outside fetched range",
            )?;
            let start = (column.payload.offset - range.start) as usize;
            let end = (column.payload.end()? - range.start) as usize;
            require(end <= bytes.len(), "sample column outside fetched range")?;
            buffers.insert(column.payload.offset, bytes.slice(start..end));
            column_index += 1;
        }
    }
    require(
        column_index == columns.len(),
        "missing sample column response",
    )?;
    Ok(())
}

/// Stable cache units; adjacent missing units still use bounded merged reads.
const PAYLOAD_BLOCK_BYTES: u64 = 16 * 1024;

struct PayloadReader<'a> {
    reader: &'a Reader,
    cache: Option<PayloadPartCache>,
    end: u64,
}

impl PayloadReader<'_> {
    fn reservation_bytes(&self, ranges: &[std::ops::Range<u64>]) -> usize {
        if self.cache.is_none() {
            return 0;
        }
        // Aligned read allocations and assembly can coexist. Cache-owned copies
        // are separately charged to the shared page-cache budget.
        2 * aligned_payload_ranges(ranges, self.end)
            .iter()
            .map(|r| (r.end - r.start) as usize)
            .sum::<usize>()
    }
}

fn aligned_payload_ranges(ranges: &[std::ops::Range<u64>], end: u64) -> Vec<std::ops::Range<u64>> {
    let mut offsets = std::collections::BTreeSet::new();
    for range in ranges {
        if range.start >= range.end {
            continue;
        }
        for block in range.start / PAYLOAD_BLOCK_BYTES..=(range.end - 1) / PAYLOAD_BLOCK_BYTES {
            offsets.insert(block * PAYLOAD_BLOCK_BYTES);
        }
    }
    offsets
        .into_iter()
        .map(|start| start..start.saturating_add(PAYLOAD_BLOCK_BYTES).min(end))
        .collect()
}

async fn fetch_cached_columns(
    reader: &PayloadReader<'_>,
    columns: &[&Column],
    ranges: &[std::ops::Range<u64>],
    buffers: &mut HashMap<u64, bytes::Bytes>,
    metrics: &mut ColumnarReadMetrics,
) -> Result<()> {
    let Some(cache) = &reader.cache else {
        return Err(invalid("missing payload cache"));
    };
    let aligned = aligned_payload_ranges(ranges, reader.end);
    let mut blocks = HashMap::new();
    let mut missing = Vec::new();
    for range in &aligned {
        if let Some(bytes) = cache.get(range) {
            *metrics.bytes.entry("cache_hit").or_default() += bytes.len() as u64;
            blocks.insert(range.start, bytes);
        } else {
            missing.push(range.clone());
        }
    }
    let merged = merge_read_ranges(&missing);
    if !merged.is_empty() {
        *metrics.counts.entry("payload_reads").or_default() += merged.len() as u64;
        let started = Instant::now();
        // No shared guard or pending-owner dependency crosses this await. A
        // backpressured or cancelled stream cannot stall another partition.
        let fetched = reader
            .reader
            .fetch(merged.clone())
            .await
            .context(OpenDalSnafu)?;
        metrics.add_elapsed("columnar_payload_fetch_wall", started.elapsed());
        require(
            fetched.len() == merged.len(),
            "sample fetch response count mismatch",
        )?;
        let mut missing = missing.iter().peekable();
        for (range, buffer) in merged.iter().zip(fetched) {
            let bytes = buffer.to_bytes();
            require(
                bytes.len() as u64 == range.end - range.start,
                "sample range length mismatch",
            )?;
            *metrics.bytes.entry("fetched").or_default() += bytes.len() as u64;
            while let Some(block) = missing.peek() {
                if block.start >= range.end {
                    break;
                }
                require(
                    block.start >= range.start && block.end <= range.end,
                    "aligned block outside fetched range",
                )?;
                let data = bytes.slice(
                    (block.start - range.start) as usize..(block.end - range.start) as usize,
                );
                cache.insert(block, &data);
                blocks.insert(block.start, data);
                missing.next();
            }
        }
        require(missing.next().is_none(), "missing aligned block response")?;
    }
    for (column, range) in columns.iter().zip(ranges) {
        let start = range.start / PAYLOAD_BLOCK_BYTES * PAYLOAD_BLOCK_BYTES;
        let first = blocks
            .get(&start)
            .ok_or_else(|| invalid("missing cached payload block"))?;
        let offset = (range.start - start) as usize;
        let len = (range.end - range.start) as usize;
        let bytes = if offset + len <= first.len() {
            first.slice(offset..offset + len)
        } else {
            let mut assembled = bytes::BytesMut::with_capacity(len);
            let mut position = range.start;
            while position < range.end {
                let start = position / PAYLOAD_BLOCK_BYTES * PAYLOAD_BLOCK_BYTES;
                let block = blocks
                    .get(&start)
                    .ok_or_else(|| invalid("missing cached payload block"))?;
                let offset = (position - start) as usize;
                let take = ((range.end - position) as usize).min(block.len() - offset);
                require(take > 0, "empty cached payload block")?;
                assembled.extend_from_slice(&block[offset..offset + take]);
                position += take as u64;
            }
            assembled.freeze()
        };
        buffers.insert(column.payload.offset, bytes);
    }
    Ok(())
}

const MAX_READ_GAP: u64 = 4 * 1024;
const MAX_READ_BYTES: u64 = 256 * 1024;

/// Allows bounded over-read to amortize filesystem and scheduling costs.
fn can_merge_ranges(previous: &std::ops::Range<u64>, next: &std::ops::Range<u64>) -> bool {
    next.start
        .checked_sub(previous.end)
        .is_some_and(|gap| gap <= MAX_READ_GAP)
        && next
            .end
            .checked_sub(previous.start)
            .is_some_and(|bytes| bytes <= MAX_READ_BYTES)
}

fn merge_read_ranges(ranges: &[std::ops::Range<u64>]) -> Vec<std::ops::Range<u64>> {
    let mut merged: Vec<std::ops::Range<u64>> = Vec::new();
    for range in ranges {
        if let Some(previous) = merged.last_mut()
            && can_merge_ranges(previous, range)
        {
            previous.end = range.end;
        } else {
            merged.push(range.clone());
        }
    }
    merged
}

/// A bounded read-ahead window. Byte slices retain the shared fetched allocation;
/// the reservation therefore lives until the whole window is replaced or cancelled.
#[derive(Default)]
struct PrefetchedBlocks {
    end: usize,
    buffers: HashMap<u64, bytes::Bytes>,
    _reservation: Option<MemoryReservation>,
}

#[allow(clippy::too_many_arguments)]
async fn prefetch_blocks(
    reader: &PayloadReader<'_>,
    blocks: &[Block],
    start: usize,
    field_ids: &HashSet<u32>,
    request: &SamplesReadRequest,
    ids: Option<&BTreeSet<MetricSeriesId>>,
    projection: &[usize],
    payload_end: u64,
    metrics: &mut ColumnarReadMetrics,
) -> Result<PrefetchedBlocks> {
    const MAX_BLOCKS: usize = 128;
    let mut columns = Vec::new();
    let mut bytes = 0usize;
    let mut last_range: Option<std::ops::Range<u64>> = None;
    let mut payload_ranges = Vec::new();
    let mut end = start;
    for block in blocks.iter().skip(start).take(MAX_BLOCKS) {
        if !matches(block.bounds(), request, ids) {
            end += 1;
            continue;
        }
        let boundary = request.start.is_some_and(|t| t > block.min)
            || request.end.is_some_and(|t| t <= block.max);
        // Boundary blocks request timestamps first. Coalescing may include value
        // bytes in gaps, but values are only decoded after timestamp pruning.
        let selected: Vec<_> = block
            .columns
            .iter()
            .filter(|column| {
                if boundary {
                    return column.role == 1;
                }
                match column.role {
                    1 => projection.contains(&2),
                    2 => projection.contains(&3),
                    3 => projection.contains(&4),
                    4 => field_ids.contains(&column.id),
                    _ => false,
                }
            })
            .collect();
        let mut next_bytes = bytes;
        let mut next_range = last_range.clone();
        for column in &selected {
            column
                .payload
                .validate(metadata::HEADER_SIZE, payload_end, false)?;
            let range = column.payload.offset..column.payload.end()?;
            if let Some(previous) = next_range.as_mut()
                && can_merge_ranges(previous, &range)
            {
                next_bytes += (range.end - previous.end) as usize;
                previous.end = range.end;
            } else {
                next_bytes += column.payload.stored as usize;
                next_range = Some(range);
            }
        }
        let previous_len = payload_ranges.len();
        for column in &selected {
            payload_ranges.push(column.payload.offset..column.payload.end()?);
        }
        let fetch_bytes = if reader.cache.is_some() {
            aligned_payload_ranges(&payload_ranges, reader.end)
                .iter()
                .map(|range| (range.end - range.start) as usize)
                .sum()
        } else {
            next_bytes
        };
        if fetch_bytes > MAX_READ_BYTES as usize {
            payload_ranges.truncate(previous_len);
            break;
        }
        bytes = next_bytes;
        last_range = next_range;
        columns.extend(selected);
        end += 1;
    }
    // A single oversized block uses the existing per-block reservation and read path.
    let end = end.max(start + 1);
    let reservation = request
        .memory_pool
        .as_ref()
        .map(|pool| MemoryConsumer::new("ColumnarSamplesPrefetch").register(pool));
    // Include aligned payload buffers in the optional read-ahead reservation.
    if let Some(reservation) = &reservation
        && reservation
            .try_grow(
                2 * bytes
                    + columns.len() * 256
                    + reader.reservation_bytes(
                        &columns
                            .iter()
                            .map(|c| Ok(c.payload.offset..c.payload.end()?))
                            .collect::<Result<Vec<_>>>()?,
                    ),
            )
            .is_err()
    {
        return Ok(PrefetchedBlocks {
            end,
            ..Default::default()
        });
    }
    let mut buffers = HashMap::new();
    fetch_columns(reader, &columns, &mut buffers, metrics).await?;
    Ok(PrefetchedBlocks {
        end,
        buffers,
        _reservation: reservation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesces_bounded_gaps_and_caps_reads() {
        assert_eq!(vec![0..24], merge_read_ranges(&[0..4, 4..8, 8..12, 20..24]));
        assert!(merge_read_ranges(&[]).is_empty());
        assert_eq!(vec![0..8], merge_read_ranges(&[0..4, 5..8]));
        assert_eq!(
            vec![0..4, 8192..8200],
            merge_read_ranges(&[0..4, 8192..8200])
        );
        assert_eq!(
            vec![0..MAX_READ_BYTES, MAX_READ_BYTES..MAX_READ_BYTES + 1],
            merge_read_ranges(&[0..MAX_READ_BYTES, MAX_READ_BYTES..MAX_READ_BYTES + 1])
        );
    }
}
