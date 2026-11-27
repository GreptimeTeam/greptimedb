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
use datatypes::arrow::compute::take;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use futures::stream::BoxStream;
use object_store::{ObjectStore, Reader};
use snafu::ResultExt;
use tokio::sync::OnceCell;

use super::index::{self, Footer, LeafEntry};
use crate::cache::columnar::{self as metadata_cache, MetadataCache};
use crate::error::{ComputeArrowSnafu, NewRecordBatchSnafu, OpenDalSnafu, Result};
use crate::metrics::ColumnarReadMetrics;
use crate::series_index::artifact::{ArtifactFormat, SamplesFileHandle};
use crate::series_index::format::{SamplesBatch, SamplesFormatReader, SamplesReadRequest};
use crate::series_index::identity::ExactSeriesKey;
use crate::series_index::samples::codec;
use crate::series_index::samples::metadata::{
    self, Block, BlockOrder, Bounds, Column, KeyArea, Page, PageReference,
};
use crate::series_index::samples::{
    MetricSeriesId, SamplesMetadata, TimestampUnit, histogram, invalid, require, unsupported,
};
use crate::sst::parquet::helper::fetch_byte_ranges;

pub(crate) struct SamplesReader {
    store: ObjectStore,
    manifest: Arc<SamplesMetadata>,
    schema: SchemaRef,
    cache: Option<MetadataCache>,
    handle: SamplesFileHandle,
    parts: Arc<Vec<OnceCell<Reader>>>,
    footer: Footer,
    index_size: u64,
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
            |(manifest, schema, _, _): &(Arc<SamplesMetadata>, SchemaRef, Footer, u64)| {
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
                Ok((
                    reader.manifest,
                    reader.schema,
                    reader.footer,
                    reader.index_size,
                ))
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
            footer: descriptor.2,
            index_size: descriptor.3,
            manifest: descriptor.0.clone(),
            schema: descriptor.1.clone(),
            cache,
        })
    }

    pub async fn open(store: ObjectStore, handle: SamplesFileHandle) -> Result<Self> {
        let entry = handle.entry();
        let artifact = &entry.artifact;
        require(
            artifact.format == ArtifactFormat::ColumnarSamples,
            "samples artifact format mismatch",
        )?;
        require(
            handle.file_id().file_id() == entry.file_id,
            "samples source handle mismatch",
        )?;
        let path = handle.index_path();
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
            .ok_or_else(|| invalid("missing samples index"))?;
        require(
            length >= index::FOOTER_SIZE as u64,
            "truncated samples index",
        )?;
        let tail_start = length.saturating_sub(8192);
        let tail = read_range(&store, &path, tail_start, length).await?;
        require(
            tail.len() as u64 == length - tail_start,
            "truncated samples index tail",
        )?;
        let footer = Footer::decode(&tail[tail.len() - index::FOOTER_SIZE..], length)?;
        require(
            footer.source == entry.file_id && footer.generation == artifact.generation,
            "samples index identity mismatch",
        )?;
        let reference = footer.metadata;
        let bytes = if reference.offset >= tail_start {
            tail.slice(
                (reference.offset - tail_start) as usize..(reference.end()? - tail_start) as usize,
            )
        } else {
            read_range(&store, &path, reference.offset, reference.end()?).await?
        };
        let (schema, files) = index::decode_metadata(&reference, &bytes)?;
        let manifest = SamplesMetadata {
            prefix: path
                .strip_suffix("samples.idx")
                .ok_or_else(|| invalid("invalid samples index path"))?
                .to_owned(),
            generation: footer.generation,
            source_file_id: footer.source,
            timestamp: schema.timestamp,
            fields: schema.fields,
            files,
        };
        manifest.validate_schema()?;
        require(
            manifest.files.is_empty() == (footer.leaves == 0),
            "inconsistent samples inventory/leaf count",
        )?;
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
                    && part.file_size > 0,
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
            footer,
            index_size: length,
            cache: None,
            manifest: Arc::new(manifest),
            handle,
        })
    }
}
impl SamplesFormatReader for SamplesReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn read(
        &self,
        request: SamplesReadRequest,
    ) -> Result<BoxStream<'static, Result<SamplesBatch>>> {
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
            !projection.contains(&self.manifest.primary_key_index()),
            "the exact series key is returned once per batch, not projected as a column",
        )?;
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
        let footer = self.footer;
        let size = self.index_size;
        Ok(Box::pin(async_stream::try_stream! {
            let _handle = &handle;
            let mut metrics = ColumnarReadMetrics::default();
            if empty {
                return;
            }
            // One decoder and one scratch per stream, reused by every block. The decoder holds
            // the only zstd context this stream builds, and the scratch keeps its containers
            // instead of rebuilding them per block. Neither is shared with another stream.
            //
            // What the stream keeps between blocks is one decompression context (bounded by a
            // frame's window, which the writer caps at the block's chunk limit) plus the output
            // scratch its release keeps. It is deliberately not charged to the pool: the native
            // prepared-samples path supplies an unbounded pool today (TODO: forward
            // `QueryScanContext`), so a charge here would report rather than enforce.
            let mut decoder = codec::PayloadDecoder::default();
            let mut scratch = ReadScratch::for_stream();
            // Only the consumer identity is hoisted: a reservation still belongs to one block's
            // batch, so it is registered per block and released with that batch.
            let buffers_consumer = request
                .memory_pool
                .as_ref()
                .map(|_| MemoryConsumer::new("ColumnarSamplesBuffers"));
            // Traversal state: the last block this stream visited, whatever it selected, and
            // whether it is the block immediately before the next one in the artifact.
            let mut previous: Option<BlockOrder> = None;
            let mut adjacent = true;
            // Content state: the last block whose exact key this stream loaded. Selection can
            // skip a block inside a page, a whole page, or a whole part, so this state is not
            // advanced by a block whose key was pruned: the two loaded keys on either side of a
            // gap are still compared with each other.
            let mut loaded_key: Option<BlockOrder> = None;
            // Carry one exact key across leaves to validate a continuing series.
            let boundary_reservation = request
                .memory_pool
                .as_ref()
                .map(|pool| MemoryConsumer::new("ColumnarSamplesBoundaryKey").register(pool));
            let mut spanning_key: Option<ExactSeriesKey> = None;
            let mut spanning_bytes = 0usize;
            let path = manifest.path();
            let index_reader = store
                .reader_with(&path)
                .concurrent(8)
                .gap(0)
                .await
                .context(OpenDalSnafu)?;
            let reader = &index_reader;
            let directory = Directory {
                cache: cache.as_ref(),
                reader,
                path: &path,
                footer,
            };
            let first_id = ids.as_ref().and_then(BTreeSet::first).copied().or_else(|| {
                request
                    .table_id
                    .map(|table_id| MetricSeriesId { table_id, tsid: 0 })
            });
            let last_id = ids.as_ref().and_then(BTreeSet::last).copied().or_else(|| {
                request.table_id.map(|table_id| MetricSeriesId {
                    table_id,
                    tsid: u64::MAX,
                })
            });
            let first_page = directory.first_page(first_id).await?;
            let mut totals = vec![(0u64, 0u64); manifest.files.len()];
            let mut previous_leaf = None;
            let mut payload_boundary = None;
            'pages: for number in first_page..directory.page_count() {
                let entries = directory.page(number).await?;
                for (i, entry) in entries.iter().enumerate() {
                    if last_id.is_some_and(|last| entry.page.bounds.first > last) {
                        break 'pages;
                    }
                    let leaf = number * DIRECTORY_PAGE_ENTRIES + i as u64;
                    if leaf == 0 {
                        require(entry.keys_start == 0, "invalid first key area")?;
                    }
                    if leaf + 1 == footer.leaves {
                        require(
                            entry.page.payload.end()? == footer.directory,
                            "invalid final leaf extent",
                        )?;
                    }
                    if let Some((last, end)) = previous_leaf {
                        require(
                            last <= entry.page.bounds.first && end == entry.keys_start,
                            "unordered samples leaves",
                        )?;
                    }
                    previous_leaf = Some((entry.page.bounds.last, entry.page.payload.end()?));
                    if !matches(entry.page.bounds, &request, ids.as_ref()) {
                        adjacent = false;
                        continue;
                    }
                    let keys = entry.keys();
                    let keys_reservation = request
                        .memory_pool
                        .as_ref()
                        .map(|pool| MemoryConsumer::new("ColumnarSamplesKeys").register(pool));
                    // `KeyRef` is the cache key; its fields are readable, so the map needs no
                    // ordering or hashing of the reference type itself.
                    let mut leaf_keys = HashMap::<(u64, u32), ExactSeriesKey>::new();
                    let mut key_offsets = mito_codec::row_converter::SparseOffsetsCache::new();
                    {
                        let page = load_metadata_page(
                            cache.as_ref(),
                            reader,
                            (&path, size),
                            &entry.page,
                            keys,
                            &manifest,
                        )
                        .await?;
                        let blocks = &page.0;
                        // One merged read for the keys of this page's selected blocks: a page can
                        // hold hundreds of short series, and one read per series would multiply
                        // this scan's I/O by the number of series it touches.
                        load_page_keys(
                            reader,
                            keys,
                            blocks,
                            &request,
                            ids.as_ref(),
                            &mut leaf_keys,
                            keys_reservation.as_ref(),
                            &mut key_offsets,
                            &mut metrics,
                        )
                        .await?;
                        let mut prefetched = PrefetchedBlocks::default();
                        for (block_index, block) in blocks.iter().enumerate() {
                            let part = &manifest.files[block.part_id as usize];
                            if let Some((prior_part, end)) = payload_boundary {
                                require(
                                    prior_part < block.part_id
                                        || (prior_part == block.part_id
                                            && end <= block.columns[0].payload.offset),
                                    "unordered samples payload references",
                                )?;
                            }
                            payload_boundary =
                                Some((block.part_id, block.payload_end()?));
                            let counts = &mut totals[block.part_id as usize];
                            counts.0 = counts
                                .0
                                .checked_add(u64::from(block.rows))
                                .ok_or_else(|| invalid("sample row count overflow"))?;
                            counts.1 += 1;
                            let order = block.order(leaf);
                            if let Some(prior) = &previous {
                                if prior.leaf() == order.leaf() {
                                    prior.precedes(&order, adjacent)?;
                                } else {
                                    require(prior.id() <= order.id(), "unordered sample series keys")?;
                                }
                            } else if complete {
                                require(block.ordinal == 0, "initial sample ordinal is not zero")?;
                            }
                            // Ownership is exact, so it needs the block's key rather than its
                            // coarse identifier. Pruned blocks never load one, and every key this
                            // page selects was already fetched in one merged read.
                            let key_id = (block.key.offset, block.key.length);
                            let mut owner = false;
                            if matches(block.bounds(), &request, ids.as_ref()) {
                                if let std::collections::hash_map::Entry::Vacant(slot) =
                                    leaf_keys.entry(key_id)
                                {
                                    // The page-level prefetch was refused by the memory pool, or
                                    // this series has no copy cached for the leaf yet. This path
                                    // reserves its own read before issuing it.
                                    let key = load_block_key(
                                        reader,
                                        keys,
                                        block,
                                        keys_reservation.as_ref(),
                                        &mut key_offsets,
                                        &mut metrics,
                                    )
                                    .await?;
                                    slot.insert(key);
                                }
                                let key = leaf_keys
                                    .get(&key_id)
                                    .ok_or_else(|| invalid("missing sample key"))?;
                                // A key that parses but claims another identifier is a malformed
                                // reference: ownership would silently point at the wrong series.
                                require(
                                    key.id() == block.id,
                                    "sample block series key disagrees with its identifier",
                                )?;
                                // Only the keys this stream loaded can be checked here: a reference
                                // is a location, and selection prunes blocks without reading theirs,
                                // so unvisited ranges are validated structurally only. The check is
                                // chained across such gaps - it compares the last *loaded* key, not
                                // the last visited block - so skipping a block cannot hide the keys
                                // on either side of it. Loaded keys must be nondecreasing, and a
                                // repeated key must continue its series instead of restarting it.
                                // That is also the only way a series that spans a leaf boundary is
                                // recognised: its two references name independent key copies.
                                if let Some(prior) = &loaded_key {
                                    let prior_key = if prior.leaf() == order.leaf() {
                                        let reference = prior.key();
                                        leaf_keys.get(&(reference.offset, reference.length))
                                    } else {
                                        spanning_key.as_ref()
                                    };
                                    if let Some(prior_key) = prior_key {
                                        // The exact-next-block rules only hold when nothing was
                                        // traversed in between; a gap still requires the order and
                                        // the ordinal to keep increasing.
                                        let consecutive = adjacent && previous.as_ref() == Some(prior);
                                        require(key >= prior_key, "unordered sample exact series keys")?;
                                        if key == prior_key {
                                            prior.continues(&order, consecutive)?;
                                        } else if consecutive {
                                            // A new series starts at ordinal zero, which the
                                            // structural check above only covers inside one leaf.
                                            require(
                                                block.ordinal == 0,
                                                "first sample block has nonzero ordinal",
                                            )?;
                                        }
                                    }
                                }
                                loaded_key = Some(order);
                                owner = owns(key.bytes(), &request);
                            }
                            let batch = if owner {
                                let payload_reader = parts[block.part_id as usize]
                                    .get_or_try_init(|| async {
                                        store
                                            .reader_with(&manifest.part_path(block.part_id))
                                            .concurrent(8)
                                            .gap(0)
                                            .await
                                            .context(OpenDalSnafu)
                                    })
                                    .await?;
                                if block_index >= prefetched.end {
                                    // Release the previous window before reserving the next one.
                                    drop(std::mem::take(&mut prefetched));
                                    prefetched = prefetch_blocks(
                                        payload_reader,
                                        blocks,
                                        block_index,
                                        &leaf_keys,
                                        &field_ids,
                                        &request,
                                        ids.as_ref(),
                                        &projection,
                                        part.file_size,
                                        &mut metrics,
                                    )
                                    .await?;
                                }
                                *metrics.counts.entry("blocks").or_default() += 1;
                                *metrics.counts.entry("decoded_rows").or_default() += u64::from(block.rows);
                                read_block(
                                    payload_reader,
                                    block,
                                    &manifest,
                                    &request,
                                    buffers_consumer.as_ref(),
                                    &projection,
                                    &field_ids,
                                    projected.clone(),
                                    &mut prefetched,
                                    &mut scratch,
                                    &mut decoder,
                                    &mut metrics,
                                )
                                .await?
                            } else {
                                None
                            };
                            // The block's payload slices are shared with the read-ahead window and
                            // the part cache, so they are dropped before the yield: the block's
                            // reservation is then the only charge alive with the batch.
                            scratch.release();
                            if let Some((batch, _reservation)) = batch {
                                // The key is copied only now: `read_block` has just reserved the
                                // bytes the batch retains, so the clone is paid for before it
                                // happens instead of being allocated first and charged after.
                                let key = leaf_keys
                                    .get(&key_id)
                                    .ok_or_else(|| invalid("missing sample key"))?
                                    .clone();
                                *metrics.counts.entry("batches").or_default() += 1;
                                *metrics.counts.entry("output_rows").or_default() +=
                                    batch.num_rows() as u64;
                                // The block's key is emitted once, not once per row.
                                yield SamplesBatch { batch, key };
                            }
                            previous = Some(order);
                            adjacent = true;
                        }
                    }
                    if let Some(loaded) = loaded_key.filter(|loaded| loaded.leaf() == leaf) {
                        let retain = (leaf + 1 < footer.leaves)
                            .then(|| {
                                let reference = loaded.key();
                                leaf_keys.get(&(reference.offset, reference.length))
                            })
                            .flatten();
                        retain_boundary_key(
                            boundary_reservation.as_ref(),
                            &mut spanning_bytes,
                            &mut spanning_key,
                            retain,
                        )?;
                    }
                }
            }
            if complete {
                for ((rows, blocks), part) in totals.into_iter().zip(&manifest.files) {
                    require(
                        rows == part.num_rows && blocks == part.num_blocks,
                        "sample part totals mismatch",
                    )?;
                }
            }
        }))
    }
}
const DIRECTORY_PAGE_ENTRIES: u64 = 256;

struct Directory<'a> {
    cache: Option<&'a MetadataCache>,
    reader: &'a Reader,
    path: &'a str,
    footer: Footer,
}

impl Directory<'_> {
    fn page_count(&self) -> u64 {
        self.footer.leaves.div_ceil(DIRECTORY_PAGE_ENTRIES)
    }

    async fn page(&self, number: u64) -> Result<Arc<Vec<LeafEntry>>> {
        let first = number * DIRECTORY_PAGE_ENTRIES;
        let start = self.footer.directory + first * index::ENTRY_SIZE;
        let count = (self.footer.leaves - first).min(DIRECTORY_PAGE_ENTRIES);
        metadata_cache::load(
            self.cache,
            format!("{}/directory/{start}/{count}", self.path),
            |entries: &Vec<LeafEntry>| {
                std::mem::size_of::<Vec<LeafEntry>>()
                    + entries.capacity() * std::mem::size_of::<LeafEntry>()
            },
            async {
                let bytes =
                    read_part_range(self.reader, start, start + count * index::ENTRY_SIZE).await?;
                let (entries, remainder) = bytes.as_chunks::<{ index::ENTRY_SIZE as usize }>();
                require(
                    remainder.is_empty() && entries.len() as u64 == count,
                    "truncated samples directory page",
                )?;
                entries
                    .iter()
                    .map(|bytes| LeafEntry::decode(bytes, self.footer.directory))
                    .collect()
            },
        )
        .await
    }

    /// Locate the earliest directory page which can contain the requested identifier.
    /// A series may span several pages, so equality must retain the earlier page.
    async fn first_page(&self, first: Option<MetricSeriesId>) -> Result<u64> {
        let Some(first) = first else {
            return Ok(0);
        };
        let mut lo = 0;
        let mut hi = self.page_count();
        while hi.saturating_sub(lo) > 1 {
            let mid = lo + (hi - lo) / 2;
            let page = self.page(mid - 1).await?;
            if page
                .last()
                .is_some_and(|entry| entry.page.bounds.last < first)
            {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }
}

/// Every block records the exact series key that owns its rows. A coarse identifier
/// match is not ownership: a colliding partner shares the identifier.
fn owns(key: &[u8], request: &SamplesReadRequest) -> bool {
    request.keys.as_ref().is_none_or(|keys| keys.contains(key))
}

/// Retained bytes per cached key beyond the key bytes themselves: the boxed key and the
/// allocator's rounding of the key's own allocation. The map entry is not charged, matching the
/// per-block identity allowance, so a page of short keys still fits a small scan pool.
const KEY_ENTRY_BYTES: usize = std::mem::size_of::<ExactSeriesKey>() + BUFFER_ALIGNMENT;

/// Bytes of the checksum stored after each key in the key area.
const KEY_CHECKSUM_BYTES: usize = metadata::KEY_CHECKSUM_BYTES as usize;

/// Retained bytes of one cached key: its bytes, the boxed key, and the allocator's rounding.
fn retained_key_bytes(length: u32) -> usize {
    length as usize + KEY_ENTRY_BYTES
}

/// Replaces the exact key that carries one series across a leaf boundary.
///
/// This copy survives traversal to the next leaf and outlives the leaf's key reservation, so it has a
/// consumer of its own: a replacement is charged before it is made, and the previous charge goes
/// back as soon as it is replaced or no longer needed.
fn retain_boundary_key(
    reservation: Option<&MemoryReservation>,
    charged: &mut usize,
    slot: &mut Option<ExactSeriesKey>,
    key: Option<&ExactSeriesKey>,
) -> Result<()> {
    *slot = None;
    if let Some(reservation) = reservation {
        let _ = reservation.try_shrink(*charged);
    }
    *charged = 0;
    if let Some(key) = key {
        let bytes = retained_key_bytes(key.bytes().len() as u32);
        reserve_keys(reservation, bytes)?;
        *slot = Some(key.clone());
        *charged = bytes;
    }
    Ok(())
}

/// Reserves `bytes` for a key fetch, or reports scan-memory exhaustion.
///
/// The reservation is taken before the read, so a pool that cannot hold the fetched key entry and
/// the owned key it becomes fails without issuing I/O.
fn reserve_keys(reservation: Option<&MemoryReservation>, bytes: usize) -> Result<()> {
    if let Some(reservation) = reservation {
        reservation
            .try_grow(bytes)
            .context(crate::error::ScanMemoryExhaustedSnafu)?;
    }
    Ok(())
}

/// Reserves `bytes` for a key fetch if the pool can pay for it.
///
/// A prefetch is optional: refusing it leaves the keys to [`load_block_key`], which is where an
/// unpayable key reports exhaustion.
fn try_reserve_keys(reservation: Option<&MemoryReservation>, bytes: usize) -> bool {
    reservation.is_none_or(|reservation| reservation.try_grow(bytes).is_ok())
}

/// Releases the transient part of a key-fetch reservation.
///
/// The fetched bytes and the owned keys decoded from them coexist, so the pool is charged the
/// peak up front. Once the buffers are dropped only the owned keys remain, and the difference
/// goes back to the pool.
struct KeyPeak<'a> {
    reservation: Option<&'a MemoryReservation>,
    transient: usize,
}
impl Drop for KeyPeak<'_> {
    fn drop(&mut self) {
        if let Some(reservation) = self.reservation
            && self.transient > 0
        {
            let _ = reservation.try_shrink(self.transient);
        }
    }
}

/// Splits one stored key entry into its key bytes after verifying the stored checksum.
///
/// The reference's length covers the key only, so the entry is exactly one key followed by
/// [`KEY_CHECKSUM_BYTES`]. A flipped label byte keeps the key's framing and identifier valid, so
/// this is the only place that notices it before the bytes attribute rows to a series.
fn verified_key(entry: &[u8], length: u32) -> Result<&[u8]> {
    let length = length as usize;
    require(
        entry.len() == length + KEY_CHECKSUM_BYTES,
        "sample key entry length mismatch",
    )?;
    let (key, stored) = entry.split_at(length);
    let stored: [u8; KEY_CHECKSUM_BYTES] = stored
        .try_into()
        .map_err(|_| invalid("sample key checksum framing"))?;
    require(
        codec::checksum(key) == u64::from_le_bytes(stored),
        "sample key checksum mismatch",
    )?;
    Ok(key)
}

/// Reads one block's exact key from the leaf's key area.
///
/// The page-level prefetch serves a whole page in one read, but it is charged up front. When the
/// scan pool cannot take that, this path reads one key at a time, so a block that fits the pool
/// stays readable. Either way the pool is charged the same coexisting peak before any I/O: the
/// fetched entry, whatever alignment or assembly that fetch costs, and the owned key built from
/// it. Reading first and refusing the key afterwards would leave the bytes already fetched.
async fn load_block_key(
    reader: &Reader,
    keys: KeyArea,
    block: &Block,
    reservation: Option<&MemoryReservation>,
    offsets: &mut mito_codec::row_converter::SparseOffsetsCache,
    metrics: &mut ColumnarReadMetrics,
) -> Result<ExactSeriesKey> {
    block.key.validate(keys)?;
    let retained = retained_key_bytes(block.key.length);
    let range = block.key.offset..block.key.checksum_end()?;
    let ranges = std::slice::from_ref(&range);
    let peak = fetch_peak(ranges) + retained;
    reserve_keys(reservation, peak)?;
    let _peak = KeyPeak {
        reservation,
        transient: peak - retained,
    };
    let mut fetched = fetch_ranges(reader, ranges, metrics).await?;
    let entry = fetched
        .pop()
        .ok_or_else(|| invalid("missing sample key response"))?;
    let key = ExactSeriesKey::from_sparse_key(verified_key(&entry, block.key.length)?, offsets)?;
    // A key that parses but claims another identifier is a malformed reference: ownership would
    // silently point at the wrong series.
    require(
        key.id() == block.id,
        "sample block series key disagrees with its identifier",
    )?;
    Ok(key)
}

/// Peak bytes [`fetch_ranges`] can hold at once while serving `ranges`.
///
/// The merge rule bounds one read, not a whole fetch: every merged range comes back as its own
/// buffer and all of them are live together. With the aligned cache, the aligned copies and any
/// range assembled from several of them coexist as well.
fn fetch_peak(ranges: &[std::ops::Range<u64>]) -> usize {
    merge_read_ranges(ranges)
        .iter()
        .map(|r| (r.end - r.start) as usize)
        .sum()
}

/// Fetches the exact keys of the blocks of one metadata page that survive coarse selection.
///
/// A descriptor only references its key, so identity never costs a copy per row or per block
/// descriptor. Keys are read only for selected blocks and at most once per series, and the
/// selected blocks of a page are merged into runs bounded by [`MAX_READ_BYTES`], so a page of
/// many short series costs one read rather than one per series.
///
/// The pool is charged the coexisting peak before the read - the fetched or aligned and assembled
/// bytes plus the owned keys decoded from them - and shrunk back to the retained keys afterwards.
/// A pool that cannot pay the aligned peak is asked for the unbuffered one, then for a narrower
/// window; the keys left out are read one at a time, which is where a key that does not fit at all
/// reports exhaustion. Reading first and refusing the key afterwards would leave the bytes already
/// fetched.
#[allow(clippy::too_many_arguments)]
async fn load_page_keys(
    reader: &Reader,
    key_area: KeyArea,
    blocks: &[Block],
    request: &SamplesReadRequest,
    ids: Option<&BTreeSet<MetricSeriesId>>,
    keys: &mut HashMap<(u64, u32), ExactSeriesKey>,
    reservation: Option<&MemoryReservation>,
    offsets: &mut mito_codec::row_converter::SparseOffsetsCache,
    metrics: &mut ColumnarReadMetrics,
) -> Result<()> {
    // Selected blocks whose key is not cached yet, one per reference: blocks of one series
    // share a copy, and blocks are ordered by series, so the references increase.
    let mut wanted: Vec<&Block> = Vec::new();
    for block in blocks {
        if !matches(block.bounds(), request, ids)
            || keys.contains_key(&(block.key.offset, block.key.length))
        {
            continue;
        }
        block.key.validate(key_area)?;
        if wanted.last().is_some_and(|last| last.key == block.key) {
            continue;
        }
        wanted.push(block);
    }
    if wanted.is_empty() {
        return Ok(());
    }
    let mut window = wanted.len();
    let mut attempt = None;
    while window > 0 {
        let selected = &wanted[..window];
        let ranges = selected
            .iter()
            .map(|block| Ok(block.key.offset..block.key.checksum_end()?))
            .collect::<Result<Vec<_>>>()?;
        let retained = selected
            .iter()
            .map(|block| retained_key_bytes(block.key.length))
            .sum::<usize>();
        let peak = fetch_peak(&ranges) + retained;
        if try_reserve_keys(reservation, peak) {
            attempt = Some((peak, retained, ranges));
            break;
        }
        // Halve the window rather than give up: the keys left out keep their own, smaller,
        // on-demand reservation.
        window /= 2;
    }
    let Some((peak, retained, ranges)) = attempt else {
        return Ok(());
    };
    let _peak = KeyPeak {
        reservation,
        transient: peak - retained,
    };
    let fetched = fetch_ranges(reader, &ranges, metrics).await?;
    for (block, entry) in wanted.iter().take(window).zip(fetched) {
        let key =
            ExactSeriesKey::from_sparse_key(verified_key(&entry, block.key.length)?, offsets)?;
        // A key that parses but claims another identifier is a malformed reference: ownership
        // would silently point at the wrong series.
        require(
            key.id() == block.id,
            "sample block series key disagrees with its identifier",
        )?;
        keys.insert((block.key.offset, block.key.length), key);
    }
    Ok(())
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
    keys: KeyArea,
    manifest: &SamplesMetadata,
) -> Result<Arc<Page>> {
    let (path, size) = object;
    metadata_cache::load(
        cache,
        format!("{path}/{size}/{reference:?}/{keys:?}"),
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
                keys,
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
        4 => matches!(column.payload.encoding, 5 | histogram::ENCODING),
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
/// Fixed per-column charge `arrow` reports beyond the column's own data.
///
/// `Array::get_array_memory_size` is `size_of::<Self>()` plus the buffer capacities, and
/// `RecordBatch::get_array_memory_size` sums those with no other term. `BinaryArray` is the
/// widest array type this reader can build, so its struct size bounds every projected column.
const ARRAY_HEADER_BYTES: usize = std::mem::size_of::<BinaryArray>();
/// Granularity `arrow_buffer` rounds every buffer allocation up to, so each column can retain up
/// to this much more than the bytes it holds.
const BUFFER_ALIGNMENT: usize = 64;

#[cfg(test)]
std::thread_local! {
    /// Scratch buffers this thread has built. A stream must build one and empty it between
    /// blocks, which nothing outside that stream reveals, so tests count the allocations
    /// directly. Thread-local for the same reason as the codec's context counter: a
    /// current-thread runtime polls a stream on one thread.
    static SCRATCHES_BUILT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Test-only: scratch buffers this thread has built so far.
#[cfg(test)]
pub(crate) fn scratches_built() -> usize {
    SCRATCHES_BUILT.with(std::cell::Cell::get)
}

/// Per-stream scratch for decoding one block.
///
/// A block needs a handful of small containers. They keep their allocations across blocks, so a
/// stream builds them once instead of once per block; [`ReadScratch::release`] empties them
/// without giving the allocations back.
#[derive(Default)]
struct ReadScratch {
    /// Stored ranges of the columns this block decodes.
    ranges: Vec<std::ops::Range<u64>>,
    /// [`Self::ranges`] after bounded gap coalescing.
    merged: Vec<std::ops::Range<u64>>,
    /// This block's payload slices, keyed by their stored offset.
    buffers: HashMap<u64, bytes::Bytes>,
    /// Row indices this block keeps.
    selected: Vec<usize>,
    /// Projection indices in physical order.
    sorted: Vec<usize>,
    /// Projected arrays waiting for the batch's column order.
    arrays: BTreeMap<usize, ArrayRef>,
}

impl ReadScratch {
    /// Builds the one scratch a stream empties and reuses for every block it decodes. Reuse
    /// leaves no other trace outside the stream, so a test counts the constructions.
    fn for_stream() -> Self {
        #[cfg(test)]
        SCRATCHES_BUILT.with(|built| built.set(built.get() + 1));
        Self::default()
    }

    /// Empties every container and keeps the allocations for the next block.
    ///
    /// The payload slices are handed out before the batch is yielded, so they are dropped here
    /// rather than carried across the yield.
    fn release(&mut self) {
        self.ranges.clear();
        self.merged.clear();
        self.buffers.clear();
        self.selected.clear();
        self.sorted.clear();
        self.arrays.clear();
    }
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
    reader: &Reader,
    block: &Block,
    manifest: &SamplesMetadata,
    request: &SamplesReadRequest,
    buffers_consumer: Option<&MemoryConsumer>,
    projection: &[usize],
    field_ids: &HashSet<u32>,
    schema: SchemaRef,
    prefetched: &mut PrefetchedBlocks,
    scratch: &mut ReadScratch,
    decoder: &mut codec::PayloadDecoder,
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
    let reservation = buffers_consumer
        .zip(request.memory_pool.as_ref())
        .map(|(consumer, pool)| consumer.clone_with_new_id().register(pool));
    // Stored buffers, decompression, typed values, selection, and Arrow output coexist.
    // Reserve before any payload I/O/allocation. The reservation survives the yield.
    scratch.ranges.clear();
    for column in &columns {
        scratch
            .ranges
            .push(column.payload.offset..column.payload.end()?);
    }
    merge_read_ranges_into(&scratch.ranges, &mut scratch.merged);
    let gap_bytes = scratch.merged.iter().map(|r| r.end - r.start).sum::<u64>()
        - scratch.ranges.iter().map(|r| r.end - r.start).sum::<u64>();
    // The block's exact key is returned with the batch, once per block rather than once per
    // row, so it costs one key regardless of how many rows the block holds. Time pruning
    // cannot add output rows, so no per-row term is needed for identity at all.
    let key_bytes =
        std::mem::size_of::<ExactSeriesKey>() + block.key.length as usize + BUFFER_ALIGNMENT;
    let bytes = 2 * gap_bytes as usize
        + columns
            .iter()
            .map(|column| column.payload.stored as usize + column.payload.decoded as usize)
            .sum::<usize>()
        + block.rows as usize * (32 * (projection.len() + 1) + std::mem::size_of::<usize>())
        + key_bytes
        // Each projected column also retains one array header and one allocation rounded up to
        // the allocator's granularity, which the per-row bytes above do not cover. A block with
        // one row and an empty projection selects no payload column at all, so this is the only
        // term that keeps its array accounted for.
        + projection.len() * (ARRAY_HEADER_BYTES + BUFFER_ALIGNMENT)
        + columns.len() * 128;
    if let Some(reservation) = &reservation {
        let mut result = reservation.try_grow(bytes);
        if result.is_err() && !prefetched.buffers.is_empty() {
            release_read_ahead(prefetched, &mut scratch.buffers);
            result = reservation.try_grow(bytes);
        }
        result.context(crate::error::ScanMemoryExhaustedSnafu)?;
    }
    // The map keeps its allocation; the previous block's slices were already released.
    scratch.buffers.clear();
    for column in &columns {
        if let Some(bytes) = prefetched.buffers.get(&column.payload.offset) {
            scratch.buffers.insert(column.payload.offset, bytes.clone());
        }
    }
    if boundary {
        fetch_columns(reader, &[&block.columns[0]], &mut scratch.buffers, metrics).await?;
    } else {
        fetch_columns(reader, &columns, &mut scratch.buffers, metrics).await?;
    }
    let decode_started = Instant::now();
    scratch.selected.clear();
    scratch.selected.reserve(block.rows as usize);
    scratch.selected.extend(0..block.rows as usize);
    let mut times = None;
    if needs_time {
        let column = &block.columns[0];
        let values = codec::decode_numbers(
            load_column_chunk(&scratch.buffers, column, decoder)?,
            column.payload.encoding,
            block.rows as usize,
        )?;
        require(
            values.first().map(|v| *v as i64) == Some(block.min)
                && values.last().map(|v| *v as i64) == Some(block.max),
            "sample timestamp bounds mismatch",
        )?;
        scratch.selected.retain(|&i| {
            request
                .start
                .is_none_or(|start| (values[i] as i64) >= start)
                && request.end.is_none_or(|end| (values[i] as i64) < end)
        });
        if scratch.selected.is_empty() {
            metrics.add_elapsed("columnar_decode", decode_started.elapsed());
            return Ok(None);
        }
        times = Some(values);
    }
    metrics.add_elapsed("columnar_decode", decode_started.elapsed());
    if boundary {
        // Release the decoder's context and oversized scratch before waiting for the remaining
        // payloads, and keep the decoder itself: the stream owns one, it is not rebuilt.
        decoder.release();
        let remaining = columns
            .into_iter()
            .filter(|column| column.role != 1)
            .collect::<Vec<_>>();
        fetch_columns(reader, &remaining, &mut scratch.buffers, metrics).await?;
    }
    let decode_started = Instant::now();
    scratch.arrays.clear();
    // Visit columns in physical order, regardless of requested output order.
    scratch.sorted.clear();
    scratch.sorted.extend_from_slice(projection);
    scratch.sorted.sort_unstable();
    // The exact key column is never projected: it is returned with the batch instead,
    // so a block of any row count carries one key in total.
    for &index in scratch.sorted.iter() {
        let selected = &scratch.selected;
        let array: ArrayRef = if index == 0 {
            Arc::new(UInt32Array::from(vec![block.id.table_id; selected.len()]))
        } else if index == 1 {
            Arc::new(UInt64Array::from(vec![block.id.tsid; selected.len()]))
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
                        load_column_chunk(&scratch.buffers, column, decoder)?,
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
                        load_column_chunk(&scratch.buffers, column, decoder)?,
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
                    let column = block.columns.iter().find(|c| c.role == 4 && c.id == id);
                    if field.is_histogram() {
                        let data_type = schema
                            .field(
                                projection
                                    .iter()
                                    .position(|&p| p == index)
                                    .ok_or_else(|| invalid("unprojected sample field"))?,
                            )
                            .data_type();
                        match column {
                            Some(column) => {
                                let mut bytes =
                                    load_column_chunk(&scratch.buffers, column, decoder)?;
                                // Decoded histograms outgrow their chunk by far more than the
                                // per-row terms reserved above, so their values are charged
                                // from the chunk's own counts before they are built.
                                if let Some(reservation) = &reservation {
                                    let decoded =
                                        histogram::decoded_bytes(bytes, block.rows as usize)?;
                                    if reservation.try_grow(decoded).is_err() {
                                        release_read_ahead(prefetched, &mut scratch.buffers);
                                        reservation
                                            .try_grow(decoded)
                                            .context(crate::error::ScanMemoryExhaustedSnafu)?;
                                        // The release replaced the slices `bytes` borrowed.
                                        bytes =
                                            load_column_chunk(&scratch.buffers, column, decoder)?;
                                    }
                                }
                                let array: ArrayRef = Arc::new(histogram::decode(
                                    bytes,
                                    data_type,
                                    block.rows as usize,
                                    column.nulls,
                                )?);
                                if selected.len() == array.len() {
                                    array
                                } else {
                                    take(
                                        &array,
                                        &UInt32Array::from_iter_values(
                                            selected.iter().map(|&i| i as u32),
                                        ),
                                        None,
                                    )
                                    .context(ComputeArrowSnafu)?
                                }
                            }
                            None => {
                                datatypes::arrow::array::new_null_array(data_type, selected.len())
                            }
                        }
                    } else if let Some(column) = column {
                        let values = codec::decode_float(
                            load_column_chunk(&scratch.buffers, column, decoder)?,
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
        scratch.arrays.insert(index, array);
    }
    let output = projection
        .iter()
        .map(|i| {
            scratch
                .arrays
                .remove(i)
                .ok_or_else(|| invalid("missing sample projected column"))
        })
        .collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new_with_options(
        schema,
        output,
        &RecordBatchOptions::new().with_row_count(Some(scratch.selected.len())),
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
    reader: &Reader,
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
    let fetched = fetch_ranges(reader, &ranges, metrics).await?;
    for (column, bytes) in columns.iter().zip(fetched) {
        buffers.insert(column.payload.offset, bytes);
    }
    Ok(())
}

/// Fetches nearby ranges in bounded merged reads.
async fn fetch_ranges(
    reader: &Reader,
    ranges: &[std::ops::Range<u64>],
    metrics: &mut ColumnarReadMetrics,
) -> Result<Vec<bytes::Bytes>> {
    *metrics.counts.entry("column_ranges").or_default() += ranges.len() as u64;
    *metrics.bytes.entry("requested").or_default() +=
        ranges.iter().map(|r| r.end - r.start).sum::<u64>();
    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    // Merging is bounded by [`MAX_READ_BYTES`], and a requested range never exceeds that, so
    // every range lies inside exactly one merged read.
    let merged = merge_read_ranges(ranges);
    *metrics.counts.entry("payload_reads").or_default() += merged.len() as u64;
    let started = Instant::now();
    let fetched = reader.fetch(merged.clone()).await.context(OpenDalSnafu)?;
    metrics.add_elapsed("columnar_payload_fetch_wall", started.elapsed());
    require(
        fetched.len() == merged.len(),
        "sample fetch response count mismatch",
    )?;
    let mut result = Vec::with_capacity(ranges.len());
    let mut index = 0;
    for (range, buffer) in merged.iter().zip(fetched) {
        let bytes = buffer.to_bytes();
        require(
            bytes.len() as u64 == range.end - range.start,
            "sample range length mismatch",
        )?;
        *metrics.bytes.entry("fetched").or_default() += bytes.len() as u64;
        while let Some(requested) = ranges.get(index) {
            if requested.start >= range.end {
                break;
            }
            require(
                requested.start >= range.start && requested.end <= range.end,
                "sample range outside fetched range",
            )?;
            result.push(bytes.slice(
                (requested.start - range.start) as usize..(requested.end - range.start) as usize,
            ));
            index += 1;
        }
    }
    require(index == ranges.len(), "missing sample range response")?;
    Ok(result)
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
    let mut merged = Vec::new();
    merge_read_ranges_into(ranges, &mut merged);
    merged
}

/// [`merge_read_ranges`] into a caller-owned buffer, so a stream coalesces into one allocation.
fn merge_read_ranges_into(ranges: &[std::ops::Range<u64>], merged: &mut Vec<std::ops::Range<u64>>) {
    merged.clear();
    for range in ranges {
        if let Some(previous) = merged.last_mut()
            && can_merge_ranges(previous, range)
        {
            previous.end = range.end;
        } else {
            merged.push(range.clone());
        }
    }
}

/// Drops the read-ahead window so a block that fits on its own can retry its reservation, and
/// keeps the next blocks from attempting the same window again.
///
/// A slice retains its whole shared fetch, whose charge leaves with the window, so the current
/// block's slices are copied out first. The block's own reservation already covers them.
fn release_read_ahead(prefetched: &mut PrefetchedBlocks, buffers: &mut HashMap<u64, bytes::Bytes>) {
    for (offset, bytes) in buffers.iter_mut() {
        if prefetched.buffers.contains_key(offset) {
            *bytes = bytes::Bytes::copy_from_slice(bytes);
        }
    }
    *prefetched = PrefetchedBlocks {
        end: prefetched.end,
        ..Default::default()
    };
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
    reader: &Reader,
    blocks: &[Block],
    start: usize,
    leaf_keys: &HashMap<(u64, u32), ExactSeriesKey>,
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

    let mut end = start;
    for block in blocks
        .iter()
        .skip(start)
        .take(MAX_BLOCKS)
        .take_while(|b| b.part_id == blocks[start].part_id)
    {
        if !matches(block.bounds(), request, ids)
            || request.keys.as_ref().is_some_and(|wanted| {
                leaf_keys
                    .get(&(block.key.offset, block.key.length))
                    .is_none_or(|key| !wanted.contains(key.bytes()))
            })
        {
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
            column.payload.validate(0, payload_end, false)?;
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
        if next_bytes > MAX_READ_BYTES as usize {
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
            .try_grow(2 * bytes + columns.len() * 256)
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

    #[test]
    fn coalescing_reuses_the_caller_buffer() {
        let ranges = [0..4, 4..8, 100..104];
        // A buffer left full by a previous block must not leak into the next result.
        let mut merged = vec![0..1, 20..24, 60..64];
        merge_read_ranges_into(&ranges, &mut merged);
        assert_eq!(merge_read_ranges(&ranges), merged);
    }

    #[test]
    fn scratch_release_drops_contents_and_keeps_allocations() {
        let mut scratch = ReadScratch::default();
        scratch.ranges.push(0..4);
        scratch.merged.push(0..8);
        scratch
            .buffers
            .insert(0, bytes::Bytes::from_static(b"payload"));
        scratch.selected.extend([0, 1, 2]);
        scratch.sorted.extend([1, 2, 5]);
        scratch
            .arrays
            .insert(2, Arc::new(UInt32Array::from(vec![1])) as ArrayRef);
        let ranges = scratch.ranges.capacity();
        let merged = scratch.merged.capacity();
        let buffers = scratch.buffers.capacity();
        let selected = scratch.selected.capacity();
        let sorted = scratch.sorted.capacity();

        scratch.release();

        assert!(scratch.ranges.is_empty() && scratch.ranges.capacity() == ranges);
        assert!(scratch.merged.is_empty() && scratch.merged.capacity() == merged);
        assert!(scratch.buffers.is_empty() && scratch.buffers.capacity() == buffers);
        assert!(scratch.selected.is_empty() && scratch.selected.capacity() == selected);
        assert!(scratch.sorted.is_empty() && scratch.sorted.capacity() == sorted);
        assert!(scratch.arrays.is_empty());
    }
}
