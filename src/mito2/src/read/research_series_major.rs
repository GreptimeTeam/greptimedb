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

//! Experimental immutable series directory over sealed local SST sidecars.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::value::Value;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, SparsePrimaryKeyCodec};
use moka::sync::Cache;
use serde::Deserialize;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;

use crate::error::{Result, UnexpectedSnafu};
use crate::read::research_series_coalesced::CoalescedDirectory;
use crate::read::research_series_native::{NativeDirectory, SelectedNativeBlock};
use crate::read::scan_region::StreamContext;
use crate::series_index::MetricSeriesId;
use crate::sst::file::FileHandle;
use crate::sst::parquet::file_range::SeriesBlockContext;
use crate::sst::parquet::research_sidecar::{PreparedSeriesFile, SelectedSeriesBlock};

const MAX_BYTES: usize = 1024 * 1024 * 1024;
const HEADER_BYTES: usize = 16;
const ENTRY_BYTES: usize = 32;
const ADDRESS_BYTES: usize = 16;

type Checked<T> = std::result::Result<T, String>;

pub(crate) struct NativeSeriesScan {
    directory: Directory,
    files: Vec<Option<(SeriesBlockContext, PreparedSeriesFile)>>,
    filters: Vec<SimpleFilterEvaluator>,
    coalesced: Option<Arc<CoalescedDirectory>>,
}

struct PreparedNativeSeriesSource {
    scan: Arc<NativeSeriesScan>,
    ids: Arc<Vec<MetricSeriesId>>,
    memory: datafusion::execution::memory_pool::MemoryReservation,
    cache_chunks: bool,
}

#[derive(Clone)]
pub(crate) struct NativeSourceTemplate {
    scan: Arc<NativeSeriesScan>,
    ids: Arc<Vec<MetricSeriesId>>,
}

impl NativeSourceTemplate {
    pub(crate) fn num_series(&self) -> usize {
        self.ids.len()
    }

    pub(crate) fn bind(
        &self,
        pool: &Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    ) -> Result<Arc<dyn store_api::region_engine::PreparedSeriesSource>> {
        let memory =
            datafusion::execution::memory_pool::MemoryConsumer::new("research-series-program")
                .register(pool);
        memory
            .try_grow(self.ids.len().saturating_mul(40))
            .map_err(error)?;
        Ok(Arc::new(PreparedNativeSeriesSource {
            scan: self.scan.clone(),
            cache_chunks: self.scan.caches_selection(&self.ids),
            ids: self.ids.clone(),
            memory,
        }))
    }
}

impl store_api::region_engine::PreparedSeriesSource for PreparedNativeSeriesSource {
    fn num_series(&self) -> usize {
        self.ids.len()
    }

    fn read_series(
        &self,
        range: std::ops::Range<usize>,
        processor: &mut dyn store_api::region_engine::SeriesBlockProcessor,
    ) -> std::result::Result<(), common_error::ext::BoxedError> {
        let ids = self.ids.get(range).ok_or_else(|| {
            common_error::ext::BoxedError::new(error("Series source range out of bounds"))
        })?;
        self.scan
            .process_coalesced(ids, &self.memory, processor, self.cache_chunks)
            .map_err(common_error::ext::BoxedError::new)
    }
}

enum Directory {
    Legacy(Arc<SeriesDirectory>),
    Native(Arc<NativeDirectory>),
}

struct ValidatedSources {
    directory: Arc<NativeDirectory>,
    sidecar_root: PathBuf,
    handles: Vec<FileHandle>,
    prepared: Vec<(usize, PreparedSeriesFile)>,
}

// Retain only one immutable source set, never a predicate, scanner, or result.
// Expired entries are not reused; eviction releases their source pins.
static VALIDATED_SOURCES: LazyLock<Cache<(), Arc<ValidatedSources>>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(1)
        .time_to_live(Duration::from_secs(30))
        .build()
});

fn same_source_handles(left: &[FileHandle], right: &[FileHandle]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| std::ptr::eq(left.meta_ref(), right.meta_ref()))
}

impl ValidatedSources {
    async fn bind(
        &self,
        context: &StreamContext,
        directory: &Arc<NativeDirectory>,
        sidecar_root: &Path,
    ) -> Result<Option<Vec<Option<(SeriesBlockContext, PreparedSeriesFile)>>>> {
        let input = &context.input;
        if !Arc::ptr_eq(&self.directory, directory)
            || self.sidecar_root != sidecar_root
            || !same_source_handles(&self.handles, &input.files)
        {
            return Ok(None);
        }
        let Some(first) = input.files.first() else {
            return Ok(None);
        };
        let predicate = input.predicate_for_file(first);
        // Region partition pruning can give different predicates to different SSTs.
        if input.files.iter().any(|file| {
            input.predicate_for_file(file).as_ref().map(|p| p.exprs())
                != predicate.as_ref().map(|p| p.exprs())
        }) {
            return Ok(None);
        }
        let Some((SeriesBlockContext::File(current), prepared)) =
            input.prepare_file_series_blocks(first, None).await?
        else {
            return Ok(None);
        };
        let source = &directory.files[self.prepared[0].0];
        if !prepared.matches_source(&source.source_sha256, source.source_index_crc32) {
            return Err(error("Directory source identity mismatch"));
        }
        let mut files = vec![None; self.prepared.len()];
        for ((ordinal, prepared), handle) in self.prepared.iter().zip(&input.files) {
            let mut rebound = current.as_ref().clone();
            rebound._file = handle.clone();
            files[*ordinal] = Some((
                SeriesBlockContext::File(Arc::new(rebound)),
                prepared.clone(),
            ));
        }
        common_telemetry::debug!(files = files.len(), "Research native sources rebound");
        Ok(Some(files))
    }

    fn capture(
        context: &StreamContext,
        directory: Arc<NativeDirectory>,
        sidecar_root: PathBuf,
        files: &[Option<(SeriesBlockContext, PreparedSeriesFile)>],
    ) -> Option<Self> {
        let Some((SeriesBlockContext::File(first), _)) = files.first()?.as_ref() else {
            return None;
        };
        // Shared bases prove identical schema, metadata, partition expression and
        // read semantics for this complete immutable source set. Only the source
        // descriptors below survive; the next request builds its own base.
        if files.iter().any(|file| {
            !matches!(file, Some((SeriesBlockContext::File(current), _))
                if Arc::ptr_eq(&first.base, &current.base))
        }) || files.len() != context.input.files.len()
        {
            return None;
        }
        let prepared = context
            .input
            .files
            .iter()
            .map(|handle| {
                let id = handle.file_id().file_id().to_string();
                let ordinal = directory
                    .files
                    .binary_search_by(|file| file.sst.cmp(&id))
                    .ok()?;
                Some((ordinal, files[ordinal].as_ref()?.1.clone()))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            directory,
            sidecar_root,
            handles: context.input.files.clone(),
            prepared,
        })
    }
}

impl Directory {
    fn files(&self) -> &[SourceFile] {
        match self {
            Self::Legacy(d) => &d.files,
            Self::Native(d) => &d.files,
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Legacy(d) => d.ids.len(),
            Self::Native(d) => d.len(),
        }
    }
    fn select(
        &self,
        filters: &[SimpleFilterEvaluator],
        active: &[bool],
    ) -> Result<Vec<MetricSeriesId>> {
        match self {
            Self::Legacy(d) => d.select(filters, active),
            Self::Native(d) => d.select(filters, active),
        }
    }
}

impl StreamContext {
    pub(crate) async fn native_series_scan(&self) -> Result<Option<Arc<NativeSeriesScan>>> {
        self.native_series_scan
            .get_or_try_init(|| NativeSeriesScan::prepare(self))
            .await
            .cloned()
    }
}

impl NativeSeriesScan {
    async fn prepare(context: &StreamContext) -> Result<Option<Arc<Self>>> {
        use api::v1::SemanticType;
        use datatypes::data_type::DataType;

        let native_root = std::env::var_os("GT_RESEARCH_NATIVE_SERIES");
        let Some(root) = native_root
            .clone()
            .or_else(|| std::env::var_os("GT_RESEARCH_SERIES_MAJOR"))
        else {
            return Ok(None);
        };
        let input = &context.input;
        if std::env::var_os("GT_RESEARCH_BLOCK_SCAN").is_none()
            || !input.memtables.is_empty()
            || input.compaction
            || input.append_mode
            || input.sequence_range.is_some()
            || input.snapshot_sequence.is_some()
            || !super::series_candidate::is_sparse_metric_metadata(input.region_metadata())
        {
            return Ok(None);
        }
        #[cfg(feature = "enterprise")]
        if !input.extension_ranges().is_empty() {
            return Ok(None);
        }
        let metadata = input.region_metadata();
        let mut filters = Vec::new();
        if let Some(predicate) = input.predicate_group().predicate() {
            if !predicate.dyn_filters().is_empty() {
                return Ok(None);
            }
            for expr in predicate.exprs() {
                let Some(filter) = SimpleFilterEvaluator::try_new_with_column_type(expr, &|name| {
                    metadata
                        .column_by_name(name)
                        .map(|c| c.column_schema.data_type.as_arrow_type())
                }) else {
                    return Ok(None);
                };
                let Some(column) = metadata.column_by_name(filter.column_name()) else {
                    return Ok(None);
                };
                match column.semantic_type {
                    SemanticType::Tag => filters.push(filter),
                    // The shared file context applies these before native processing.
                    SemanticType::Timestamp => {}
                    SemanticType::Field => return Ok(None),
                }
            }
        }
        let directory = if native_root.is_some() {
            let Some(directory) =
                NativeDirectory::load(PathBuf::from(root), metadata.clone()).await?
            else {
                return Ok(None);
            };
            if !directory.covers(&filters) {
                return Ok(None);
            }
            Directory::Native(directory)
        } else {
            let Some(directory) =
                SeriesDirectory::load(PathBuf::from(root), metadata.clone()).await?
            else {
                return Ok(None);
            };
            Directory::Legacy(directory)
        };
        let binding_root = (std::env::var_os("GT_RESEARCH_NATIVE_SOURCE_BINDING").is_some()
            && std::env::var_os("GT_RESEARCH_COALESCED_SERIES").is_some())
        .then(|| std::env::var_os("GT_RESEARCH_SST_SIDECAR").map(PathBuf::from))
        .flatten();
        let rebound = if let (Some(root), Directory::Native(native), Some(cached)) =
            (&binding_root, &directory, VALIDATED_SOURCES.get(&()))
        {
            cached.bind(context, native, root).await?
        } else {
            None
        };
        let source_binding_hit = rebound.is_some();
        let mut files =
            rebound.unwrap_or_else(|| (0..directory.files().len()).map(|_| None).collect());
        let mut reusable = None;
        for file in input.files.iter().filter(|_| !source_binding_hit) {
            let id = file.file_id().file_id().to_string();
            let Ok(ordinal) = directory.files().binary_search_by(|f| f.sst.cmp(&id)) else {
                return Ok(None);
            };
            let Some((context, prepared)) = input
                .prepare_file_series_blocks(file, reusable.as_ref())
                .await?
            else {
                return Ok(None);
            };
            let source = &directory.files()[ordinal];
            if !prepared.matches_source(&source.source_sha256, source.source_index_crc32) {
                return Err(error("Directory source identity mismatch"));
            }
            reusable = Some(context.clone());
            files[ordinal] = Some((context, prepared));
        }
        // Directory files absent from the scan were pruned by the scan's time range: every
        // scanned file was matched to the directory above, so a replacement file inside the
        // range would have failed that match. Coalesced chunks are time-filtered later.
        let coalesced = if native_root.is_some() && files.iter().any(Option::is_some) {
            if let Some(root) = std::env::var_os("GT_RESEARCH_COALESCED_SERIES") {
                let value = CoalescedDirectory::load(root.into(), metadata.clone()).await?;
                if value
                    .as_ref()
                    .is_some_and(|d| !d.matches_sources(directory.files()))
                {
                    return Err(error("Coalesced source identity mismatch"));
                }
                value
            } else {
                None
            }
        } else {
            None
        };
        // Without key-aware coalesced chunks, keys sharing an id cannot be kept apart.
        if coalesced.is_none() && matches!(&directory, Directory::Native(d) if d.repeated_ids) {
            return Ok(None);
        }
        if !source_binding_hit
            && coalesced.is_some()
            && let (Some(root), Directory::Native(native)) = (binding_root, &directory)
            && let Some(sources) = ValidatedSources::capture(context, native.clone(), root, &files)
        {
            VALIDATED_SOURCES.insert((), Arc::new(sources));
        }
        Ok(Some(Arc::new(Self {
            coalesced,
            directory,
            files,
            filters,
        })))
    }

    pub(crate) async fn prepare_program_source(
        context: Arc<StreamContext>,
    ) -> Result<Option<Arc<dyn store_api::region_engine::PreparedSeriesSource>>> {
        let pool = context.input.scan_memory_pool.clone();
        Self::prepare_program_template(context)
            .await?
            .map(|template| template.bind(&pool))
            .transpose()
    }

    pub(crate) async fn prepare_program_template(
        context: Arc<StreamContext>,
    ) -> Result<Option<NativeSourceTemplate>> {
        use datafusion::execution::memory_pool::MemoryConsumer;

        let Some(scan) = context.native_series_scan().await? else {
            return Ok(None);
        };
        if !scan.supports_coalesced(&[]) {
            return Ok(None);
        }
        let pool = context.input.scan_memory_pool.clone();
        let memory = MemoryConsumer::new("research-series-program").register(&pool);
        memory
            .try_grow(
                scan.directory
                    .len()
                    .checked_mul(40)
                    .ok_or_else(|| error("Series program reservation overflow"))?,
            )
            .map_err(error)?;
        let selected_scan = scan.clone();
        let (ids, _memory, supported) = common_runtime::spawn_blocking_query(move || {
            let active = selected_scan
                .files
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>();
            let ids = selected_scan
                .directory
                .select(&selected_scan.filters, &active)?;
            let supported = selected_scan.supports_coalesced(&ids);
            Ok::<_, crate::error::Error>((ids, memory, supported))
        })
        .await
        .map_err(error)??;
        if !supported {
            return Ok(None);
        }
        Ok(Some(NativeSourceTemplate {
            scan,
            ids: Arc::new(ids),
        }))
    }

    pub(crate) fn execute_program(
        source: Arc<dyn store_api::region_engine::PreparedSeriesSource>,
        factory: store_api::region_engine::ScanProcessorFactory,
        workers: usize,
        claimed: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Option<common_recordbatch::SendableRecordBatchStream>> {
        use std::sync::atomic::Ordering;

        use common_recordbatch::RecordBatchStreamWrapper;
        use futures::{StreamExt, TryStreamExt};
        use snafu::IntoError;

        let mut first = factory().map_err(error)?;
        if first.series_blocks().is_none() {
            return Ok(None);
        }
        let schema = first.schema();
        if claimed.swap(true, Ordering::AcqRel) {
            return Err(error("Series program scanner already consumed"));
        }
        // Partition by complete series, independently of the SST row-group layout.
        // Small selections execute in one task; large selections retain bounded parallelism.
        let ranges = program_ranges(source.num_series(), workers);
        common_telemetry::debug!(
            series = source.num_series(),
            tasks = ranges.len(),
            "Research series program activated"
        );
        let work = async_stream::try_stream! {
            let mut pending = futures::stream::FuturesUnordered::new();
            let mut first = Some(first);
            for range in ranges {
                let mut processor = match first.take() {
                    Some(processor) => processor,
                    None => factory().map_err(error)?,
                };
                let source = source.clone();
                pending.push(common_runtime::spawn_blocking_query(move || {
                    let consumer = processor.series_blocks()
                        .ok_or_else(|| error("Series program consumer changed capabilities"))?;
                    source.read_series(range, consumer).map_err(error)?;
                    let batches = processor.finish().map_err(error)?;
                    Ok::<_, crate::error::Error>((batches, processor))
                }));
            }
            while let Some(result) = pending.next().await {
                let (batches, processor) = result.map_err(error)??;
                for batch in batches {
                    yield batch;
                }
                // Retained output remains charged until the downstream consumes it.
                drop(processor);
            }
        };
        let work = work.map_err(|error: crate::error::Error| {
            common_recordbatch::error::ExternalSnafu
                .into_error(common_error::ext::BoxedError::new(error))
        });
        Ok(Some(Box::pin(RecordBatchStreamWrapper::new(
            schema,
            Box::pin(work),
        ))))
    }

    pub(crate) async fn candidates(
        self: Arc<Self>,
        pool: &Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    ) -> Result<crate::series_index::MetricSeriesIdStream> {
        use datafusion::execution::memory_pool::MemoryConsumer;
        use futures::StreamExt;

        let reservation = MemoryConsumer::new("research-series-major-candidates").register(pool);
        // Bounds both filter intersections and the final typed candidate vector.
        reservation
            .try_grow(self.directory.len() * 40)
            .map_err(error)?;
        let (ids, reservation) = common_runtime::spawn_blocking_query(move || {
            let active = self.files.iter().map(Option::is_some).collect::<Vec<_>>();
            let started = std::time::Instant::now();
            let ids = self.directory.select(&self.filters, &active)?;
            common_telemetry::debug!(
                series = ids.len(),
                select_elapsed = ?started.elapsed(),
                "Research series-major candidates activated"
            );
            Ok::<_, crate::error::Error>((ids, reservation))
        })
        .await
        .map_err(error)??;
        Ok(async_stream::try_stream! {
            let _reservation = reservation;
            for chunk in ids.chunks(crate::series_index::METRIC_SERIES_ID_BATCH_SIZE) {
                yield chunk.to_vec();
            }
        }
        .boxed())
    }

    pub(crate) fn block_count(&self, ids: &[MetricSeriesId]) -> Result<usize> {
        ids.iter().try_fold(0usize, |total, id| {
            let count = match &self.directory {
                Directory::Legacy(directory) => {
                    let ordinal = directory
                        .ordinal(id)
                        .ok_or_else(|| error("Assigned series missing from directory"))?;
                    directory
                        .blocks(ordinal)
                        .filter(|b| self.files[b.file as usize].is_some())
                        .count()
                }
                Directory::Native(directory) => {
                    let ordinal = directory
                        .ordinal(id)
                        .ok_or_else(|| error("Assigned native series missing from directory"))?;
                    directory.block_count(ordinal)
                }
            };
            total
                .checked_add(count)
                .ok_or_else(|| error("Selected block count overflow"))
        })
    }

    pub(crate) fn blocks(
        &self,
        ids: &[MetricSeriesId],
    ) -> Result<Vec<(SeriesBlockContext, Vec<SelectedSeriesBlock>)>> {
        let Directory::Legacy(directory) = &self.directory else {
            return Err(error("Native records must retain series grouping"));
        };
        let mut selected = self.files.iter().map(|_| Vec::new()).collect::<Vec<_>>();
        for id in ids {
            let ordinal = directory
                .ordinal(id)
                .ok_or_else(|| error("Assigned series missing from directory"))?;
            for address in directory.blocks(ordinal) {
                let Some((_, file)) = &self.files[address.file as usize] else {
                    continue;
                };
                selected[address.file as usize].push(file.select_address(
                    address.group,
                    address.page,
                    address.block,
                    directory.key(ordinal),
                )?);
            }
        }
        Ok(selected
            .into_iter()
            .enumerate()
            .filter_map(|(index, blocks)| {
                (!blocks.is_empty())
                    .then(|| (self.files[index].as_ref().unwrap().0.clone(), blocks))
            })
            .collect())
    }

    pub(crate) fn supports_coalesced(&self, ids: &[MetricSeriesId]) -> bool {
        let Some(directory) = &self.coalesced else {
            return false;
        };
        let mut present = self.files.iter().flatten();
        let Some((first_context @ SeriesBlockContext::File(first), _)) = present.next() else {
            return false;
        };
        // A merged chunk cannot use a context specialized to only one source file.
        present.all(|(context, _)| {
            matches!(context,
            SeriesBlockContext::File(context) if Arc::ptr_eq(&first.base, &context.base))
        }) && first_context.grid_row_bounds(0, 1, 1).is_some()
            && {
                let mut cursor = 0;
                ids.iter()
                    .all(|id| directory.chunks_from(id, &mut cursor).is_some())
            }
    }

    /// Whether a coalesced read of `ids` should populate the chunk cache.
    pub(crate) fn caches_selection(&self, ids: &[MetricSeriesId]) -> bool {
        // Ablation switch for measurements only.
        std::env::var_os("GT_RESEARCH_ABLATE_CHUNK_CACHE_ALWAYS").is_some()
            || self
                .coalesced
                .as_ref()
                .is_some_and(|directory| directory.fits_cache(ids))
    }

    /// Whether a key's own labels pass the scan's tag filters.
    fn key_matches(&self, directory: &CoalescedDirectory, key: &[u8]) -> Result<bool> {
        let tags = directory.tags(key, None)?;
        for filter in &self.filters {
            let Some((_, value)) = tags.iter().find(|(name, _)| name == filter.column_name())
            else {
                return Err(error("Filter column missing from series key"));
            };
            let Some(column) = directory.column(filter.column_name()) else {
                return Err(error("Unknown filter column"));
            };
            let scalar = value
                .try_to_scalar_value(&column.column_schema.data_type)
                .map_err(error)?;
            if !filter.evaluate_scalar(&scalar).map_err(error)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn process_coalesced(
        &self,
        ids: &[MetricSeriesId],
        memory: &datafusion::execution::memory_pool::MemoryReservation,
        processor: &mut dyn store_api::region_engine::SeriesBlockProcessor,
        cache_chunks: bool,
    ) -> Result<()> {
        let directory = self
            .coalesced
            .as_ref()
            .ok_or_else(|| error("Missing coalesced directory"))?;
        let context = &self
            .files
            .iter()
            .flatten()
            .next()
            .ok_or_else(|| error("Missing coalesced source context"))?
            .0;
        let native = std::env::var_os("GT_RESEARCH_NATIVE_VALUES").is_some();
        // Ablation switch for measurements only.
        let tag_columns = processor
            .tag_names()
            .filter(|_| std::env::var_os("GT_RESEARCH_ABLATE_ALL_TAGS").is_none())
            .map(|names| directory.tag_columns(&names));
        let mut total_chunks = 0;
        let mut total_rows = 0;
        let mut cursor = 0;
        for id in ids {
            let ordinals = directory
                .chunks_from(id, &mut cursor)
                .ok_or_else(|| error("Missing coalesced series"))?;
            let bytes = ordinals
                .clone()
                .try_fold(0usize, |sum, ordinal| {
                    sum.checked_add(directory.decode_bytes(ordinal))
                })
                .ok_or_else(|| error("Coalesced reservation overflow"))?;
            let reservation = memory.new_empty();
            reservation.try_grow(bytes).map_err(error)?;
            let chunks = ordinals
                .map(|ordinal| {
                    if cache_chunks {
                        directory.chunk(ordinal)
                    } else {
                        directory.chunk_uncached(ordinal)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            if chunks.is_empty() {
                return Err(error("Empty coalesced series"));
            }
            // Chunks are ordered by key; keys sharing an id (tsid hash collisions) are
            // separate series and are rechecked against the tag filters one by one.
            let shared = chunks.first().map(|c| &c.key) != chunks.last().map(|c| &c.key);
            for series in chunks.chunk_by(|a, b| a.key == b.key) {
                let key = &series[0].key;
                if shared && !self.key_matches(directory, key)? {
                    continue;
                }
                let mut blocks = Vec::new();
                let mut previous = None;
                for chunk in series {
                    total_chunks += 1;
                    for (start, step, range) in &chunk.runs {
                        let last = *start as i128 + *step * (range.len() - 1) as i128;
                        if previous.is_some_and(|previous| previous >= *start as i128) {
                            return Err(error("Coalesced chunks overlap"));
                        }
                        previous = Some(last);
                        let selected = context
                            .grid_row_bounds(*start, *step, range.len())
                            .ok_or_else(|| error("Unsupported coalesced time filter"))?;
                        if selected.is_empty() {
                            continue;
                        }
                        total_rows += selected.len();
                        let values = chunk
                            .values
                            .slice(range.start + selected.start, selected.len());
                        let values = if native {
                            values
                        } else {
                            store_api::region_engine::VisibleSeriesValues::Dense(
                                values.materialize(),
                            )
                        };
                        blocks.push(Ok(store_api::region_engine::VisibleSeriesBlock {
                            first_timestamp: (*start as i128 + *step * selected.start as i128)
                                as i64,
                            timestamp_step: *step,
                            values,
                        }));
                    }
                }
                let tags = directory.tags(key, tag_columns.as_deref())?;
                let mut blocks = blocks.into_iter();
                processor
                    .process_series(&tags, &mut blocks)
                    .map_err(error)?;
                if blocks.next().is_some() {
                    return Err(error("Coalesced processor left unread blocks"));
                }
            }
        }
        common_telemetry::debug!(
            series = ids.len(),
            chunks = total_chunks,
            rows = total_rows,
            "Research coalesced series activated"
        );
        Ok(())
    }

    pub(crate) fn uses_records(&self) -> bool {
        matches!(self.directory, Directory::Native(_))
    }

    /// Records are looked up by id, so keys sharing an id cannot be read through them.
    pub(crate) fn has_repeated_ids(&self) -> bool {
        matches!(&self.directory, Directory::Native(directory) if directory.repeated_ids)
    }

    pub(crate) fn records(
        &self,
        ids: &[MetricSeriesId],
        memory: &datafusion::execution::memory_pool::MemoryReservation,
    ) -> Result<Vec<(Vec<u8>, Vec<(SeriesBlockContext, SelectedNativeBlock)>)>> {
        let Directory::Native(directory) = &self.directory else {
            return Err(error("Series records require native directory"));
        };
        let mut selected = Vec::new();
        for id in ids {
            let ordinal = directory
                .ordinal(id)
                .ok_or_else(|| error("Assigned native series missing from directory"))?;
            let record = directory.record(ordinal)?;
            memory
                .try_grow(record.weight + record.blocks.len() * 256)
                .map_err(error)?;
            let mut blocks = Vec::new();
            for (index, block) in record.blocks.iter().enumerate() {
                let Some((context, prepared)) = &self.files[block.file] else {
                    continue;
                };
                let file = prepared.native_values()?;
                file.validate(&block.values)?;
                blocks.push((
                    context.clone(),
                    SelectedNativeBlock {
                        record: record.clone(),
                        index,
                        file: file.clone(),
                    },
                ));
            }
            if !blocks.is_empty() {
                selected.push((record.key.clone(), blocks));
            }
        }
        Ok(selected)
    }
}

static DIRECTORIES: LazyLock<Cache<PathBuf, Arc<SeriesDirectory>>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(MAX_BYTES as u64)
        .weigher(|_, directory: &Arc<SeriesDirectory>| directory.weight)
        .build()
});

#[derive(Deserialize)]
struct Manifest {
    version: u32,
    status: String,
    files: Vec<SourceFile>,
    counts: Counts,
    artifacts: HashMap<String, Artifact>,
}

#[derive(Deserialize)]
struct Counts {
    keys: usize,
    series: usize,
    blocks: usize,
}

#[derive(Deserialize)]
struct Artifact {
    bytes: usize,
    crc32: u32,
}

#[derive(Deserialize)]
pub(crate) struct SourceFile {
    pub(crate) sst: String,
    pub(crate) source_sha256: String,
    pub(crate) source_index_crc32: u32,
    pub(crate) blocks: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct BlockAddress {
    pub(crate) file: u32,
    pub(crate) group: u32,
    pub(crate) page: u32,
    pub(crate) block: u32,
}

pub(crate) struct SeriesDirectory {
    metadata: RegionMetadataRef,
    pub(crate) files: Vec<SourceFile>,
    keys: Vec<u8>,
    entries: Vec<u8>,
    addresses: Vec<u8>,
    ids: Vec<MetricSeriesId>,
    postings: HashMap<String, BTreeMap<Value, Vec<u32>>>,
    weight: u32,
}

fn error(reason: impl std::fmt::Display) -> crate::error::Error {
    UnexpectedSnafu {
        reason: format!("Research series directory: {reason}"),
    }
    .build()
}

fn check(condition: bool, reason: &str) -> Checked<()> {
    if condition {
        Ok(())
    } else {
        Err(reason.into())
    }
}

fn word(bytes: &[u8], offset: usize) -> usize {
    // Only called on validated, fixed-width records on the supported 64-bit target.
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize
}

fn address(bytes: &[u8]) -> BlockAddress {
    let part = |offset| u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    BlockAddress {
        file: part(0),
        group: part(4),
        page: part(8),
        block: part(12),
    }
}

fn read_artifact(
    root: &Path,
    manifest: &Manifest,
    name: &str,
    magic: &[u8; 8],
    count: usize,
    width: usize,
) -> Checked<Vec<u8>> {
    let descriptor = manifest
        .artifacts
        .get(name)
        .ok_or("Missing artifact descriptor")?;
    let expected = count
        .checked_mul(width)
        .and_then(|n| n.checked_add(HEADER_BYTES))
        .ok_or("Artifact size overflow")?;
    check(
        expected <= MAX_BYTES && descriptor.bytes == expected,
        "Artifact size mismatch",
    )?;
    let path = root.join(name);
    check(
        std::fs::metadata(&path).map_err(|e| e.to_string())?.len() == expected as u64,
        "Artifact file length mismatch",
    )?;
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    check(
        bytes.len() == expected && crc32fast::hash(&bytes) == descriptor.crc32,
        "Artifact checksum mismatch",
    )?;
    check(
        &bytes[..8] == magic && word(&bytes, 8) == count,
        "Artifact header mismatch",
    )?;
    Ok(bytes)
}

impl SeriesDirectory {
    pub(crate) async fn load(
        root: PathBuf,
        metadata: RegionMetadataRef,
    ) -> Result<Option<Arc<Self>>> {
        common_runtime::spawn_blocking_query(move || {
            let directory = DIRECTORIES
                .try_get_with(root.clone(), || {
                    Self::read(&root, metadata.clone()).map(Arc::new)
                })
                .map_err(error)?;
            // The cache stores storage metadata, never a predicate's candidate set.
            Ok((directory.metadata.as_ref() == metadata.as_ref()).then_some(directory))
        })
        .await
        .map_err(error)?
    }

    fn read(root: &Path, metadata: RegionMetadataRef) -> Checked<Self> {
        check(usize::BITS == 64, "Directory requires a 64-bit target")?;
        let manifest_bytes =
            std::fs::read(root.join("manifest.json")).map_err(|e| e.to_string())?;
        let manifest: Manifest =
            serde_json::from_slice(&manifest_bytes).map_err(|e| e.to_string())?;
        check(
            manifest.version == 1 && manifest.status == "pass",
            "Unvalidated directory version",
        )?;
        check(
            manifest.counts.series <= u32::MAX as usize,
            "Too many series",
        )?;
        check(
            manifest.files.windows(2).all(|p| p[0].sst < p[1].sst),
            "Unsorted source files",
        )?;
        let total = manifest
            .artifacts
            .values()
            .try_fold(0usize, |total, a| total.checked_add(a.bytes))
            .ok_or("Directory size overflow")?;
        check(total <= MAX_BYTES, "Directory exceeds metadata budget")?;
        let keys = read_artifact(
            root,
            &manifest,
            "keys.bin",
            b"GTSKEY01",
            manifest.counts.keys,
            1,
        )?;
        let entries = read_artifact(
            root,
            &manifest,
            "series.bin",
            b"GTSSER01",
            manifest.counts.series,
            ENTRY_BYTES,
        )?;
        let addresses = read_artifact(
            root,
            &manifest,
            "blocks.bin",
            b"GTSBLK01",
            manifest.counts.blocks,
            ADDRESS_BYTES,
        )?;
        let mut directory = Self {
            metadata,
            files: manifest.files,
            keys,
            entries,
            addresses,
            ids: Vec::with_capacity(manifest.counts.series),
            postings: HashMap::new(),
            weight: 0,
        };
        directory.validate(manifest.counts)?;
        directory.build_postings()?;
        Ok(directory)
    }

    fn validate(&mut self, counts: Counts) -> Checked<()> {
        let codec = SparsePrimaryKeyCodec::new(&self.metadata);
        let mut next_key = 0usize;
        let mut next_block = 0usize;
        let mut file_counts = vec![0usize; self.files.len()];
        for ordinal in 0..counts.series {
            let entry = self.entry(ordinal);
            let key_start = word(entry, 0);
            let key_len = word(entry, 8);
            let block_start = word(entry, 16);
            let block_len = word(entry, 24);
            check(
                key_start == next_key
                    && block_start == next_block
                    && key_len >= 22
                    && block_len > 0,
                "Noncontiguous or empty series entry",
            )?;
            next_key = next_key.checked_add(key_len).ok_or("Key extent overflow")?;
            next_block = next_block
                .checked_add(block_len)
                .ok_or("Block extent overflow")?;
            check(
                next_key <= counts.keys && next_block <= counts.blocks,
                "Series extent out of bounds",
            )?;
            let key = self.key(ordinal);
            check(
                ordinal == 0 || self.key(ordinal - 1) < key,
                "Unsorted series keys",
            )?;
            let (table_id, tsid) = codec.decode_ids(key).map_err(|e| e.to_string())?;
            let id = MetricSeriesId { table_id, tsid };
            check(
                self.ids.last().is_none_or(|previous| previous < &id),
                "Duplicate or unsorted series identity",
            )?;
            self.ids.push(id);
            let mut previous = None;
            for block in self.blocks(ordinal) {
                check(
                    previous.is_none_or(|p| p < block),
                    "Duplicate or unsorted block address",
                )?;
                previous = Some(block);
                let count = file_counts
                    .get_mut(block.file as usize)
                    .ok_or("Invalid source file ordinal")?;
                *count += 1;
            }
        }
        check(
            next_key == counts.keys && next_block == counts.blocks,
            "Incomplete directory coverage",
        )?;
        check(
            self.files
                .iter()
                .zip(file_counts)
                .all(|(file, count)| file.blocks == count),
            "Source block count mismatch",
        )
    }

    fn build_postings(&mut self) -> Checked<()> {
        let codec = SparsePrimaryKeyCodec::new(&self.metadata);
        let columns = self
            .metadata
            .primary_key_columns()
            .filter(|c| c.column_schema.name != DATA_SCHEMA_TSID_COLUMN_NAME)
            .collect::<Vec<_>>();
        let mut postings: HashMap<String, BTreeMap<Value, Vec<u32>>> = columns
            .iter()
            .map(|c| (c.column_schema.name.clone(), BTreeMap::new()))
            .collect();
        let mut weight = self.keys.capacity()
            + self.entries.capacity()
            + self.addresses.capacity()
            + self.ids.capacity() * std::mem::size_of::<MetricSeriesId>()
            + 4096;
        for ordinal in 0..self.ids.len() {
            let CompositeValues::Sparse(values) =
                codec.decode(self.key(ordinal)).map_err(|e| e.to_string())?
            else {
                return Err("Expected sparse primary key".into());
            };
            for column in &columns {
                let value = values.get_or_null(column.column_id);
                let index = postings.get_mut(&column.column_schema.name).unwrap();
                let list = match index.entry(value.clone()) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        // Includes conservative B-tree node and scalar allocation overhead.
                        weight += value.as_value_ref().data_size() + 256;
                        entry.insert(Vec::new())
                    }
                };
                let old_capacity = list.capacity();
                list.push(ordinal as u32);
                weight += (list.capacity() - old_capacity) * std::mem::size_of::<u32>();
                check(weight <= MAX_BYTES, "Label postings exceed metadata budget")?;
            }
        }
        self.postings = postings;
        self.weight = weight as u32;
        common_telemetry::info!(
            series = self.ids.len(),
            bytes = weight,
            "Research series-major directory loaded"
        );
        Ok(())
    }

    fn entry(&self, ordinal: usize) -> &[u8] {
        let offset = HEADER_BYTES + ordinal * ENTRY_BYTES;
        &self.entries[offset..offset + ENTRY_BYTES]
    }

    pub(crate) fn key(&self, ordinal: usize) -> &[u8] {
        let entry = self.entry(ordinal);
        let start = HEADER_BYTES + word(entry, 0);
        &self.keys[start..start + word(entry, 8)]
    }

    pub(crate) fn blocks(&self, ordinal: usize) -> impl Iterator<Item = BlockAddress> + '_ {
        let entry = self.entry(ordinal);
        let start = HEADER_BYTES + word(entry, 16) * ADDRESS_BYTES;
        let end = start + word(entry, 24) * ADDRESS_BYTES;
        self.addresses[start..end]
            .chunks_exact(ADDRESS_BYTES)
            .map(address)
    }

    pub(crate) fn ordinal(&self, id: &MetricSeriesId) -> Option<usize> {
        self.ids.binary_search(id).ok()
    }

    pub(crate) fn select(
        &self,
        filters: &[SimpleFilterEvaluator],
        active_files: &[bool],
    ) -> Result<Vec<MetricSeriesId>> {
        if active_files.len() != self.files.len() {
            return Err(error("Invalid active file set"));
        }
        let mut candidates: Option<Vec<u32>> = None;
        for filter in filters {
            let column = self
                .metadata
                .column_by_name(filter.column_name())
                .ok_or_else(|| error("Unknown tag column"))?;
            let mut matched = Vec::new();
            if filter.column_name() == DATA_SCHEMA_TSID_COLUMN_NAME {
                for (ordinal, id) in self.ids.iter().enumerate() {
                    if filter
                        .evaluate_scalar(
                            &Value::UInt64(id.tsid)
                                .try_to_scalar_value(&column.column_schema.data_type)
                                .map_err(error)?,
                        )
                        .map_err(error)?
                    {
                        matched.push(ordinal as u32);
                    }
                }
            } else {
                let index = self
                    .postings
                    .get(filter.column_name())
                    .ok_or_else(|| error("Unindexed tag column"))?;
                for (value, ordinals) in index {
                    if filter
                        .evaluate_scalar(
                            &value
                                .try_to_scalar_value(&column.column_schema.data_type)
                                .map_err(error)?,
                        )
                        .map_err(error)?
                    {
                        matched.extend_from_slice(ordinals);
                    }
                }
                matched.sort_unstable();
            }
            candidates = Some(match candidates {
                None => matched,
                Some(mut current) => {
                    let mut next = 0;
                    current.retain(|id| {
                        while next < matched.len() && matched[next] < *id {
                            next += 1;
                        }
                        matched.get(next) == Some(id)
                    });
                    current
                }
            });
            if candidates.as_ref().is_some_and(Vec::is_empty) {
                break;
            }
        }
        let candidates = candidates.unwrap_or_else(|| (0..self.ids.len() as u32).collect());
        Ok(candidates
            .into_iter()
            .filter_map(|ordinal| {
                let ordinal = ordinal as usize;
                self.blocks(ordinal)
                    .any(|b| active_files[b.file as usize])
                    .then_some(self.ids[ordinal])
            })
            .collect())
    }
}

fn program_ranges(series: usize, workers: usize) -> Vec<std::ops::Range<usize>> {
    let tasks = series.div_ceil(128).max(1).min(workers.max(1));
    let size = series.div_ceil(tasks).max(1);
    (0..tasks)
        .map(|task| {
            let start = (task * size).min(series);
            start..start.saturating_add(size).min(series)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datafusion_expr::{col, lit};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::ValueRef;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
    use store_api::storage::RegionId;
    use store_api::storage::consts::ReservedColumnId;

    use super::*;

    #[test]
    fn program_tasks_cover_each_complete_series_once() {
        for series in [0, 1, 127, 128, 129, 255, 256, 257, 1747610] {
            for workers in [0, 1, 2, 8, 32] {
                let ranges = program_ranges(series, workers);
                assert!(!ranges.is_empty());
                assert!(ranges.len() <= workers.max(1));
                if series <= 128 {
                    assert_eq!(ranges.len(), 1);
                }
                let mut expected = 0;
                for range in ranges {
                    for id in range {
                        assert_eq!(id, expected);
                        expected += 1;
                    }
                }
                assert_eq!(expected, series);
            }
        }
    }

    #[test]
    fn source_binding_requires_the_same_pinned_snapshot_handles() {
        use crate::sst::file::FileMeta;
        use crate::test_util::new_noop_file_purger;

        let first = FileHandle::new(FileMeta::default(), new_noop_file_purger());
        let second = FileHandle::new(FileMeta::default(), new_noop_file_purger());
        let original = vec![first.clone(), second.clone()];
        assert!(same_source_handles(&original, &original.clone()));
        // Reopening identical metadata must not reuse a proof tied to an older handle.
        let reopened = FileHandle::new(first.meta_ref().clone(), new_noop_file_purger());
        assert!(!same_source_handles(&original, &[reopened, second.clone()]));
        assert!(!same_source_handles(&original, &[second, first]));
        assert!(!same_source_handles(&original, &original[..1]));
    }

    fn fixture(root: &Path) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        for (name, data_type, column_id, semantic_type) in [
            (
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                ConcreteDataType::uint32_datatype(),
                ReservedColumnId::table_id(),
                SemanticType::Tag,
            ),
            (
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                ReservedColumnId::tsid(),
                SemanticType::Tag,
            ),
            (
                "pod",
                ConcreteDataType::string_datatype(),
                1,
                SemanticType::Tag,
            ),
            (
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                2,
                SemanticType::Timestamp,
            ),
        ] {
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(name, data_type, name == "pod"),
                column_id,
                semantic_type,
            });
        }
        builder
            .primary_key(vec![
                ReservedColumnId::table_id(),
                ReservedColumnId::tsid(),
                1,
            ])
            .primary_key_encoding(PrimaryKeyEncoding::Sparse);
        let metadata = Arc::new(builder.build().unwrap());
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let mut keys = Vec::new();
        let mut entries = Vec::new();
        let mut addresses = Vec::new();
        for (table, tsid, pod, files) in [
            (1, 1, Some("a"), vec![0u32, 1]),
            (1, 2, None, vec![0]),
            (2, 3, Some("b"), vec![1]),
        ] {
            let mut key = Vec::new();
            codec.encode_internal(table, tsid, &mut key).unwrap();
            if let Some(pod) = pod {
                codec
                    .encode_to_vec([(1, ValueRef::String(pod))].into_iter(), &mut key)
                    .unwrap();
            }
            for value in [
                keys.len(),
                key.len(),
                addresses.len() / ADDRESS_BYTES,
                files.len(),
            ] {
                entries.extend_from_slice(&(value as u64).to_le_bytes());
            }
            keys.extend(key);
            for file in files {
                for value in [file, 0, 0, if tsid == 1 { 0 } else { 1 }] {
                    addresses.extend_from_slice(&value.to_le_bytes());
                }
            }
        }
        let key_count = keys.len();
        let mut artifacts = serde_json::Map::new();
        for (name, magic, count, body) in [
            ("keys.bin", b"GTSKEY01", key_count, keys),
            ("series.bin", b"GTSSER01", 3, entries),
            ("blocks.bin", b"GTSBLK01", 4, addresses),
        ] {
            let mut data = magic.to_vec();
            data.extend_from_slice(&(count as u64).to_le_bytes());
            data.extend(body);
            std::fs::write(root.join(name), &data).unwrap();
            artifacts.insert(
                name.into(),
                serde_json::json!({"bytes": data.len(), "crc32": crc32fast::hash(&data)}),
            );
        }
        let manifest = serde_json::json!({"version": 1, "status": "pass",
            "files": [{"sst": "a", "source_sha256": "a", "source_index_crc32": 1, "blocks": 2},
                      {"sst": "b", "source_sha256": "b", "source_index_crc32": 2, "blocks": 2}],
            "counts": {"keys": key_count, "series": 3, "blocks": 4}, "artifacts": artifacts});
        std::fs::write(
            root.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        metadata
    }

    #[test]
    fn postings_preserve_nulls_intersections_and_source_membership() {
        let temp = common_test_util::temp_dir::create_temp_dir("series-major-postings");
        let metadata = fixture(temp.path());
        let directory = SeriesDirectory::read(temp.path(), metadata).unwrap();
        let filter = |expr| SimpleFilterEvaluator::try_new(&expr).unwrap();
        let id = |table_id, tsid| MetricSeriesId { table_id, tsid };
        assert_eq!(
            directory.select(&[], &[true, true]).unwrap(),
            vec![id(1, 1), id(1, 2), id(2, 3)]
        );
        assert_eq!(
            directory.select(&[], &[false, true]).unwrap(),
            vec![id(1, 1), id(2, 3)]
        );
        assert_eq!(
            directory
                .select(&[filter(col("pod").is_null())], &[true, true])
                .unwrap(),
            vec![id(1, 2)]
        );
        assert_eq!(
            directory
                .select(&[filter(col("pod").not_eq(lit("a")))], &[true, true])
                .unwrap(),
            vec![id(2, 3)]
        );
        assert!(
            directory
                .select(&[filter(col("pod").is_null())], &[false, true])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            directory
                .select(
                    &[
                        filter(col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME).eq(lit(1u32))),
                        filter(col("pod").eq(lit("a"))),
                        filter(col(DATA_SCHEMA_TSID_COLUMN_NAME).eq(lit(1u64))),
                    ],
                    &[true, true]
                )
                .unwrap(),
            vec![id(1, 1)]
        );
        assert_eq!(directory.ordinal(&id(2, 3)), Some(2));
        assert_eq!(directory.ordinal(&id(2, 2)), None);
        assert!(directory.select(&[], &[true]).is_err());
    }

    #[test]
    fn source_template_binds_and_releases_request_memory_independently() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

        let temp = common_test_util::temp_dir::create_temp_dir("series-source-reservation");
        let metadata = fixture(temp.path());
        let directory = SeriesDirectory::read(temp.path(), metadata).unwrap();
        let template = NativeSourceTemplate {
            scan: Arc::new(NativeSeriesScan {
                directory: Directory::Legacy(Arc::new(directory)),
                files: Vec::new(),
                filters: Vec::new(),
                coalesced: None,
            }),
            ids: Arc::new(vec![MetricSeriesId {
                table_id: 1,
                tsid: 1,
            }]),
        };
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(40));
        let first = template.bind(&pool).unwrap();
        assert!(template.bind(&pool).is_err());
        drop(first);
        assert_eq!(pool.reserved(), 0);
        let second = template.bind(&pool).unwrap();
        assert_eq!(second.num_series(), 1);
        drop(second);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn directory_rejects_corruption_and_invalid_extents() {
        let temp = common_test_util::temp_dir::create_temp_dir("series-major-corruption");
        let metadata = fixture(temp.path());
        let original = std::fs::read(temp.path().join("series.bin")).unwrap();
        let mut corrupted = original.clone();
        corrupted[HEADER_BYTES + 8..HEADER_BYTES + 16].copy_from_slice(&u64::MAX.to_le_bytes());
        std::fs::write(temp.path().join("series.bin"), &corrupted).unwrap();
        assert!(
            SeriesDirectory::read(temp.path(), metadata.clone())
                .err()
                .unwrap()
                .contains("checksum")
        );
        // A valid checksum must not allow an overflowing extent to reach a slice operation.
        let path = temp.path().join("manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        manifest["artifacts"]["series.bin"]["crc32"] = crc32fast::hash(&corrupted).into();
        std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(SeriesDirectory::read(temp.path(), metadata.clone()).is_err());
        std::fs::write(
            temp.path().join("series.bin"),
            &original[..original.len() - 1],
        )
        .unwrap();
        assert!(
            SeriesDirectory::read(temp.path(), metadata)
                .err()
                .unwrap()
                .contains("length")
        );
    }
}
