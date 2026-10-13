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

//! Local, read-only experimental column blocks. Original SSTs remain authoritative.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, Weak};
use std::time::Instant;

use datatypes::arrow::array::{
    ArrayRef, BinaryArray, DictionaryArray, Float64Array, Int32Array, TimestampMillisecondArray,
    UInt8Array, UInt32Array, UInt64Array,
};
use datatypes::arrow::buffer::{NullBuffer, ScalarBuffer};
use datatypes::arrow::compute::concat_batches;
use datatypes::arrow::datatypes::{DataType, Int32Type, SchemaRef, TimeUnit, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use moka::sync::Cache;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::file::metadata::ParquetMetaData;
use serde::Deserialize;
use store_api::region_engine::{ExactIntegerSeries, VisibleSeriesValues};

use crate::error::{Result, UnexpectedSnafu};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::json_align::ProjectedRecordBatchStream;
use crate::sst::parquet::row_group::ParquetFetchMetrics;

type Checked<T> = std::result::Result<T, String>;

#[derive(Clone, Hash, PartialEq, Eq)]
enum CacheKey {
    Index(PathBuf, PathBuf),
    Page(PathBuf, u64, u32),
}

#[derive(Clone)]
enum Cached {
    Index(Arc<Sidecar>),
    Page(Arc<DirectoryPage>),
}

static INDEXES: LazyLock<Cache<CacheKey, Cached>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(512 * 1024 * 1024)
        .weigher(|_, value: &Cached| match value {
            Cached::Index(value) => value.weight,
            Cached::Page(value) => value.weight,
        })
        .build()
});

#[derive(Clone, Hash, PartialEq, Eq)]
struct ValueCacheKey {
    native: bool,
    path: PathBuf,
    source: String,
    column: Column,
}

fn dense_values(bits: ScalarBuffer<u64>) -> VisibleSeriesValues {
    let count = bits.len();
    Float64Array::new(ScalarBuffer::new(bits.into_inner(), 0, count), None).into()
}

fn materialized_bits(values: VisibleSeriesValues) -> ScalarBuffer<u64> {
    let values = values.materialize();
    ScalarBuffer::new(values.values().inner().clone(), 0, values.len())
}

type ValueCache = Cache<ValueCacheKey, VisibleSeriesValues>;

fn value_cache(bytes: u64) -> ValueCache {
    Cache::builder()
        .max_capacity(bytes)
        .weigher(|key: &ValueCacheKey, values: &VisibleSeriesValues| {
            let column = &key.column;
            (std::mem::size_of::<ValueCacheKey>()
                + key.path.as_os_str().len()
                + key.source.len()
                + column.dtype.len()
                + column.mode.len()
                + column.first.as_ref().map_or(0, String::len)
                + column.step.as_ref().map_or(0, String::len)
                + values.retained_bytes()
                + 128) as u32
        })
        .build()
}

static VALUES: LazyLock<Checked<Option<ValueCache>>> =
    LazyLock::new(|| match std::env::var("GT_RESEARCH_BLOCK_CACHE_BYTES") {
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.to_string()),
        Ok(value) => {
            let bytes = value.parse::<u64>().map_err(|e| e.to_string())?;
            Ok((bytes != 0).then(|| value_cache(bytes)))
        }
    });

#[derive(Deserialize)]
struct Seal {
    status: String,
    source_crc32: u32,
    source_sha256: String,
    source_bytes: u64,
    index_crc32: u32,
}

#[derive(Deserialize)]
struct Index {
    version: u32,
    source_sha256: String,
    source_bytes: u64,
    rows: usize,
    row_groups: usize,
    data_bytes: u64,
    #[serde(default)]
    blocks: Vec<Block>,
    #[serde(default)]
    pages: Vec<Page>,
    #[serde(default)]
    pages_bytes: u64,
}

#[derive(Deserialize)]
struct Page {
    row_group: usize,
    row_start: usize,
    count: usize,
    blocks: usize,
    offset: u64,
    length: usize,
    raw_length: usize,
    crc32: u32,
    #[serde(skip)]
    series_bounds: Option<([u8; 22], [u8; 22])>,
}

#[derive(Deserialize)]
struct SeriesDirectoryManifest {
    status: String,
    files: Vec<SeriesDirectoryFile>,
}

#[derive(Deserialize)]
struct SeriesDirectoryFile {
    sst: String,
    crc32: u32,
}

#[derive(Deserialize)]
struct SeriesDirectory {
    version: u32,
    source_sha256: String,
    source_index_crc32: u32,
    pages: Vec<SeriesPage>,
}

#[derive(Deserialize)]
struct SeriesPage {
    row_group: usize,
    offset: u64,
    crc32: u32,
    min_key: String,
    max_key: String,
    sorted_keys: bool,
}

fn decode_series_prefix(value: &str) -> Checked<[u8; 22]> {
    check(
        value.len() == 44 && value.is_ascii(),
        "Invalid series prefix",
    )?;
    let mut prefix = [0; 22];
    for (index, byte) in prefix.iter_mut().enumerate() {
        *byte =
            u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).map_err(|e| e.to_string())?;
    }
    check(
        prefix[..5] == [0x80, 0, 0, 4, 1] && prefix[9..14] == [0x80, 0, 0, 3, 1],
        "Invalid reserved series prefix",
    )?;
    Ok(prefix)
}

fn load_series_directory(directory: &Path, seal: &Seal, pages: &mut [Page]) -> Checked<()> {
    let Some(root) = std::env::var_os("GT_RESEARCH_SERIES_DIRECTORY") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    read_series_directory(&root, directory, seal, pages)
}

fn read_series_directory(
    root: &Path,
    directory: &Path,
    seal: &Seal,
    pages: &mut [Page],
) -> Checked<()> {
    let manifest: SeriesDirectoryManifest =
        serde_json::from_slice(&read_bounded(&root.join("manifest.json"), 4 * 1024 * 1024)?)
            .map_err(|e| e.to_string())?;
    check(manifest.status == "pass", "Unvalidated series directory")?;
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("Invalid sidecar directory name")?;
    let entries: Vec<_> = manifest
        .files
        .iter()
        .filter(|file| file.sst == name)
        .collect();
    check(entries.len() == 1, "Missing or duplicate series directory")?;
    let raw = read_bounded(&root.join(format!("{name}.json")), 16 * 1024 * 1024)?;
    check(
        crc32fast::hash(&raw) == entries[0].crc32,
        "Series directory checksum mismatch",
    )?;
    let index: SeriesDirectory = serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
    check(
        index.version == 1
            && index.source_sha256 == seal.source_sha256
            && index.source_index_crc32 == seal.index_crc32
            && index.pages.len() == pages.len(),
        "Series directory source mismatch",
    )?;
    for (page, bound) in pages.iter_mut().zip(index.pages) {
        check(
            page.row_group == bound.row_group
                && page.offset == bound.offset
                && page.crc32 == bound.crc32
                && bound.sorted_keys,
            "Series directory page mismatch",
        )?;
        let min = decode_series_prefix(&bound.min_key)?;
        let max = decode_series_prefix(&bound.max_key)?;
        check(min <= max, "Reversed series bounds")?;
        page.series_bounds = Some((min, max));
    }
    Ok(())
}

struct DirectoryPage {
    blocks: Vec<Block>,
    weight: u32,
}

#[derive(Deserialize)]
struct Block {
    row_group: usize,
    row_start: usize,
    count: usize,
    key_hex: String,
    #[serde(skip)]
    key: Vec<u8>,
    #[serde(skip)]
    native_values: OnceLock<ExactIntegerSeries>,
    columns: HashMap<String, Column>,
}

#[derive(Clone, Deserialize, Hash, PartialEq, Eq)]
struct Column {
    dtype: String,
    count: usize,
    crc32: u32,
    mode: String,
    first: Option<String>,
    step: Option<String>,
    offset: u64,
    length: usize,
}

struct PageInterval {
    lower: [u8; 22],
    prefix_upper: [u8; 22],
    group: usize,
    page: usize,
}

impl PageInterval {
    fn build(pages: &[Vec<Page>]) -> Vec<Self> {
        let mut intervals = Vec::with_capacity(pages.iter().map(Vec::len).sum());
        for (group, pages) in pages.iter().enumerate() {
            for (page, descriptor) in pages.iter().enumerate() {
                let Some((lower, upper)) = descriptor.series_bounds else {
                    return Vec::new();
                };
                intervals.push(Self {
                    lower,
                    prefix_upper: upper,
                    group,
                    page,
                });
            }
        }
        intervals.sort_unstable_by_key(|entry| entry.lower);
        let mut upper = [0; 22];
        for entry in &mut intervals {
            upper = upper.max(entry.prefix_upper);
            entry.prefix_upper = upper;
        }
        intervals
    }

    fn candidates<'a>(intervals: &'a [Self], series: &[[u8; 22]]) -> &'a [Self] {
        let (Some(first), Some(last)) = (series.first(), series.last()) else {
            return &[];
        };
        // Pages need not be globally ordered or disjoint. Prefix maxima retain
        // earlier intervals that overlap the selected series range.
        let start = intervals.partition_point(|entry| entry.prefix_upper < *first);
        let end = intervals.partition_point(|entry| entry.lower <= *last);
        &intervals[start..end]
    }
}

struct Sidecar {
    source_identity: String,
    source_index_crc32: u32,
    data_path: PathBuf,
    data_file: Mutex<Weak<File>>,
    groups: Vec<Vec<Block>>,
    pages_path: PathBuf,
    pages: Vec<Vec<Page>>,
    page_intervals: Vec<PageInterval>,
    group_rows: Vec<usize>,
    data_bytes: u64,
    weight: u32,
}

#[derive(Deserialize)]
struct MaxSummaryEntry {
    row_group: usize,
    row_start: usize,
    count: usize,
    first_timestamp: i64,
    timestamp_step: i64,
    columns: HashMap<String, Column>,
}

#[derive(Deserialize)]
struct MaxSummaryIndex {
    version: u32,
    window_ms: i64,
    source_sha256: String,
    source_index_crc32: u32,
    data_bytes: u64,
    blocks: Vec<MaxSummaryEntry>,
}

#[derive(Deserialize)]
struct MaxSummarySeal {
    status: String,
    index_crc32: u32,
}

struct MaxSummaryFile {
    entries: HashMap<(usize, usize), MaxSummaryEntry>,
    file: File,
    path: PathBuf,
    weight: u32,
}

static MAX_SUMMARIES: LazyLock<Cache<(PathBuf, String), Arc<MaxSummaryFile>>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(256 * 1024 * 1024)
            .weigher(|_, value: &Arc<MaxSummaryFile>| value.weight)
            .build()
    });

static MAX_VALIDITY: LazyLock<Cache<ValueCacheKey, Arc<[u8]>>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(64 * 1024 * 1024)
        .weigher(|key: &ValueCacheKey, value: &Arc<[u8]>| {
            (value.len() + key.path.as_os_str().len() + key.source.len() + 512) as u32
        })
        .build()
});

pub(crate) struct PreparedRangeMaxRequest<'a> {
    request: &'a store_api::region_engine::RangeMaxRequest,
    root: PathBuf,
}

impl<'a> PreparedRangeMaxRequest<'a> {
    pub(crate) fn new(
        request: &'a store_api::region_engine::RangeMaxRequest,
        root: PathBuf,
    ) -> Result<Self> {
        check(
            request.window != 300_000
                || (!request.instants.is_empty()
                    && request.instants.windows(2).all(|p| {
                        i128::from(p[1]) - i128::from(p[0]) >= i128::from(request.window)
                    })),
            "Invalid max summary request",
        )
        .map_err(error)?;
        Ok(Self { request, root })
    }
}

fn pack_max_validity(raw: &[u8]) -> Checked<Arc<[u8]>> {
    let mut packed = vec![0u8; raw.len().div_ceil(8)];
    for (index, &value) in raw.iter().enumerate() {
        check(value <= 1, "Invalid max summary validity")?;
        packed[index / 8] |= value << (index % 8);
    }
    Ok(packed.into())
}

/// A selected part of one immutable series block, without materialized row columns.
pub(crate) struct SelectedSeriesBlock {
    _sidecar: Arc<Sidecar>,
    data_file: Arc<File>,
    page: Arc<DirectoryPage>,
    block_index: usize,
    rows: Range<usize>,
}

impl SelectedSeriesBlock {
    pub(crate) fn read_range_max(
        &self,
        prepared: &PreparedRangeMaxRequest<'_>,
        rows: Range<usize>,
    ) -> Result<Option<Vec<store_api::region_engine::VisibleSeriesBlock>>> {
        self.read_prepared_range_max(prepared, rows).map_err(error)
    }

    #[cfg(test)]
    fn read_range_max_checked(
        &self,
        request: &store_api::region_engine::RangeMaxRequest,
        rows: Range<usize>,
        root: &Path,
    ) -> Checked<Option<Vec<store_api::region_engine::VisibleSeriesBlock>>> {
        let prepared =
            PreparedRangeMaxRequest::new(request, root.to_path_buf()).map_err(|e| e.to_string())?;
        self.read_prepared_range_max(&prepared, rows)
    }

    fn read_prepared_range_max(
        &self,
        prepared: &PreparedRangeMaxRequest<'_>,
        rows: Range<usize>,
    ) -> Checked<Option<Vec<store_api::region_engine::VisibleSeriesBlock>>> {
        let request = prepared.request;
        if request.window != 300_000 {
            return Ok(None);
        }
        check(
            rows.start <= rows.end && rows.end <= self.rows.len(),
            "Invalid max summary request",
        )?;
        let directory = self
            ._sidecar
            .data_path
            .parent()
            .ok_or("Missing source directory")?;
        let path = prepared
            .root
            .join(directory.file_name().ok_or("Missing source name")?);
        let identity = self._sidecar.source_identity.clone();
        let key = (path.clone(), identity.clone());
        let summary = if let Some(summary) = MAX_SUMMARIES.get(&key) {
            summary
        } else {
            if !path.exists() {
                return Ok(None);
            }
            MAX_SUMMARIES
                .try_get_with(key, || -> Checked<Arc<MaxSummaryFile>> {
                    let raw = read_bounded(&path.join("index.json"), 64 * 1024 * 1024)?;
                    let seal: MaxSummarySeal = serde_json::from_slice(&read_bounded(
                        &path.join("validation.json"),
                        1024 * 1024,
                    )?)
                    .map_err(|e| e.to_string())?;
                    check(
                        seal.status == "pass" && crc32fast::hash(&raw) == seal.index_crc32,
                        "Invalid max summary seal",
                    )?;
                    let index: MaxSummaryIndex =
                        serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
                    let source_index =
                        read_bounded(&directory.join("index.json"), 64 * 1024 * 1024)?;
                    check(
                        index.version == 1
                            && index.window_ms == request.window
                            && index.source_sha256 == identity
                            && index.source_index_crc32 == crc32fast::hash(&source_index),
                        "Max summary source mismatch",
                    )?;
                    let data_path = path.join("summary.bin");
                    let file = File::open(&data_path).map_err(|e| e.to_string())?;
                    check(
                        file.metadata().map_err(|e| e.to_string())?.len() == index.data_bytes,
                        "Max summary length mismatch",
                    )?;
                    let mut entries = HashMap::new();
                    for entry in index.blocks {
                        check(
                            entry.count > 0 && entry.count <= 8192 && entry.timestamp_step == 30000,
                            "Invalid max summary block",
                        )?;
                        for (name, column) in &entry.columns {
                            let expected = match name.as_str() {
                                "prefix" | "suffix" => entry.count.min(9),
                                "summary" | "summary_valid" => entry.count.saturating_sub(9),
                                _ => return Err("Unknown max summary column".into()),
                            };
                            check(
                                column.count == expected
                                    && expected > 0
                                    && column.dtype
                                        == if name == "summary_valid" {
                                            "|u1"
                                        } else {
                                            "<u8"
                                        }
                                    && column.length <= expected * 8
                                    && column
                                        .offset
                                        .checked_add(column.length as u64)
                                        .is_some_and(|end| end <= index.data_bytes),
                                "Invalid max summary column",
                            )?;
                        }
                        check(
                            entries
                                .insert((entry.row_group, entry.row_start), entry)
                                .is_none(),
                            "Duplicate max summary block",
                        )?;
                    }
                    Ok(Arc::new(MaxSummaryFile {
                        entries,
                        file,
                        path: data_path,
                        weight: u32::try_from(
                            raw.len().checked_mul(6).ok_or("Summary weight overflow")?,
                        )
                        .map_err(|e| e.to_string())?,
                    }))
                })
                .map_err(|e| e.to_string())?
        };
        let block = self.block();
        let Some(entry) = summary.entries.get(&(block.row_group, block.row_start)) else {
            return Ok(None);
        };
        let Some((first, _, step)) = regular_put_range(block, 0..block.count)? else {
            return Ok(None);
        };
        if step != 30000 {
            return Ok(None);
        }
        check(
            entry.count == block.count && entry.first_timestamp == first,
            "Max summary physical range mismatch",
        )?;
        let read = |name: &str| -> Checked<Vec<u8>> {
            let column = entry
                .columns
                .get(name)
                .ok_or("Missing max summary column")?;
            let mut payload = vec![0; column.length];
            read_payload_at(&summary.file, &mut payload, column.offset)
                .map_err(|e| e.to_string())?;
            decode(column, &payload)
        };
        let read_words = |name: &str| -> Checked<ScalarBuffer<u64>> {
            let column = entry
                .columns
                .get(name)
                .ok_or("Missing max summary column")?;
            let load = || -> Checked<ScalarBuffer<u64>> {
                Ok(words(&read(name)?).collect::<Vec<_>>().into())
            };
            match VALUES.as_ref().map_err(Clone::clone)? {
                Some(cache) => cache
                    .try_get_with(
                        ValueCacheKey {
                            native: false,
                            path: summary.path.clone(),
                            source: identity.clone(),
                            column: column.clone(),
                        },
                        || load().map(dense_values),
                    )
                    .map(materialized_bits)
                    .map_err(|e| e.to_string()),
                None => load(),
            }
        };
        let prefix = read_words("prefix")?;
        let suffix = if entry.count > 9 {
            Some(read_words("suffix")?)
        } else {
            None
        };
        let maxima = if entry.count >= 10 {
            Some(read_words("summary")?)
        } else {
            None
        };
        let valid = if entry.count >= 10 {
            let column = entry
                .columns
                .get("summary_valid")
                .ok_or("Missing max summary validity")?;
            let load = || pack_max_validity(&read("summary_valid")?);
            if VALUES.as_ref().map_err(Clone::clone)?.is_some() {
                MAX_VALIDITY
                    .try_get_with(
                        ValueCacheKey {
                            native: false,
                            path: summary.path.clone(),
                            source: identity.clone(),
                            column: column.clone(),
                        },
                        load,
                    )
                    .map_err(|e| e.to_string())?
            } else {
                load()?
            }
        } else {
            Arc::from([])
        };
        let selected = self.rows.start + rows.start..self.rows.start + rows.end;
        let mut output = Vec::new();
        let mut times = Vec::new();
        let mut values = Vec::new();
        let lower = i128::from(first) + selected.start as i128 * step;
        let upper = i128::from(first) + selected.end as i128 * step;
        let start = request
            .instants
            .partition_point(|&end| i128::from(end) - i128::from(request.offset) < lower);
        for &instant in &request.instants[start..] {
            let end = i128::from(instant) - i128::from(request.offset);
            if end - i128::from(request.window) >= upper {
                break;
            }
            let left = ((end - i128::from(request.window) - i128::from(first)).div_euclid(step)
                + 1)
            .max(selected.start as i128);
            let right = ((end - i128::from(first)).div_euclid(step) + 1).min(selected.end as i128);
            if right <= left {
                continue;
            }
            let (left, right) = (left as usize, right as usize);
            let value = if right - left == 10 {
                let maxima = maxima.as_ref().ok_or("Missing full max summary")?;
                (valid[left / 8] & (1 << (left % 8)) != 0).then(|| f64::from_bits(maxima[left]))
            } else {
                let mut maximum: Option<f64> = None;
                for index in left..right {
                    if index >= 9 && index < entry.count.saturating_sub(9) {
                        return Ok(None);
                    }
                    let bits = if let Some(&bits) = prefix.get(index) {
                        bits
                    } else {
                        let suffix = suffix.as_ref().ok_or("Missing max boundary")?;
                        *suffix
                            .get(
                                index
                                    .checked_sub(entry.count - suffix.len())
                                    .ok_or("Interior boundary sample")?,
                            )
                            .ok_or("Missing boundary sample")?
                    };
                    let value = f64::from_bits(bits);
                    if bits != 0x7ff0000000000002
                        && maximum.is_none_or(|current| current.is_nan() || value > current)
                    {
                        maximum = Some(value);
                    }
                }
                maximum
            };
            if let Some(value) = value {
                times.push(
                    i64::try_from(i128::from(first) + (right - 1) as i128 * step)
                        .map_err(|e| e.to_string())?,
                );
                values.push(value);
            }
        }
        let mut start = 0;
        while start < times.len() {
            let stride = if start + 1 < times.len() {
                i128::from(times[start + 1]) - i128::from(times[start])
            } else {
                0
            };
            let mut end = start + 1;
            while end < times.len() && i128::from(times[end]) - i128::from(times[end - 1]) == stride
            {
                end += 1;
            }
            output.push(store_api::region_engine::VisibleSeriesBlock {
                first_timestamp: times[start],
                timestamp_step: stride,
                values: Float64Array::from(values[start..end].to_vec()).into(),
            });
            start = end;
        }
        Ok(Some(output))
    }

    fn block(&self) -> &Block {
        &self.page.blocks[self.block_index]
    }

    pub(crate) fn primary_key(&self) -> &[u8] {
        &self.block().key
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn retained_page(&self) -> (usize, usize) {
        (Arc::as_ptr(&self.page) as usize, self.page.weight as usize)
    }

    pub(crate) fn decode_bytes(&self) -> usize {
        let column = &self.block().columns["value_bits"];
        // Transformed decoding and cache filling can each hold two full arrays.
        column.length + column.count * 16
    }

    /// This describes this block only; it is not proof of cross-source visibility.
    pub(crate) fn regular_put_range(&self) -> Result<Option<(i64, i64, i128)>> {
        regular_put_range(self.block(), self.rows.clone()).map_err(error)
    }

    /// Reads only the value payload. Sequence, operation, validity and key arrays stay absent.
    /// The caller must establish visibility and non-nullness before using these values.
    pub(crate) fn read_value_bits(&self) -> Result<ScalarBuffer<u64>> {
        self.read_value_bits_checked().map_err(error)
    }

    fn read_value_bits_checked(&self) -> Checked<ScalarBuffer<u64>> {
        let cache = VALUES.as_ref().map_err(Clone::clone)?;
        self.read_value_bits_with_cache(cache.as_ref())
    }

    fn read_value_bits_with_cache(&self, cache: Option<&ValueCache>) -> Checked<ScalarBuffer<u64>> {
        self.read_visible_values_with_cache(cache, false)
            .map(materialized_bits)
    }

    pub(crate) fn read_visible_values(&self, native: bool) -> Result<VisibleSeriesValues> {
        let cache = VALUES.as_ref().map_err(Clone::clone).map_err(error)?;
        self.read_visible_values_with_cache(cache.as_ref(), native)
            .map_err(error)
    }

    fn read_visible_values_with_cache(
        &self,
        cache: Option<&ValueCache>,
        native: bool,
    ) -> Checked<VisibleSeriesValues> {
        if native
            && cache.is_some()
            && let Some(values) = self.block().native_values.get()
        {
            return Ok(VisibleSeriesValues::Integer(values.clone())
                .slice(self.rows.start, self.rows.len()));
        }
        let column = self
            .block()
            .columns
            .get("value_bits")
            .ok_or("Missing values")?;
        read_block_values(
            &self._sidecar,
            &self.data_file,
            column,
            &self.block().native_values,
            cache,
            native,
        )
        .map(|values| values.slice(self.rows.start, self.rows.len()))
    }
}

fn read_block_values(
    sidecar: &Sidecar,
    data_file: &File,
    column: &Column,
    native_values: &OnceLock<ExactIntegerSeries>,
    cache: Option<&ValueCache>,
    native: bool,
) -> Checked<VisibleSeriesValues> {
    // Compact values are charged to the owning metadata cache. Dense values
    // remain in the bounded payload cache.
    if native
        && cache.is_some()
        && let Some(values) = native_values.get()
    {
        return Ok(VisibleSeriesValues::Integer(values.clone()));
    }
    let read = || -> Checked<VisibleSeriesValues> {
        let mut payload = vec![0; column.length];
        if !payload.is_empty() {
            read_payload_at(data_file, &mut payload, column.offset).map_err(|e| e.to_string())?;
        }
        let raw = decode(column, &payload)?;
        let values = dense_values(words(&raw).collect::<Vec<_>>().into());
        Ok(if native {
            VisibleSeriesValues::compact(values.materialize())
        } else {
            values
        })
    };
    let values = if let Some(cache) = cache {
        let key = ValueCacheKey {
            native,
            path: sidecar.data_path.clone(),
            source: sidecar.source_identity.clone(),
            column: column.clone(),
        };
        cache.try_get_with(key, read).map_err(|e| e.to_string())?
    } else {
        read()?
    };
    if native
        && cache.is_some()
        && let VisibleSeriesValues::Integer(integer) = &values
    {
        let _ = native_values.set(integer.clone());
    }
    Ok(values)
}

#[cfg(unix)]
pub(crate) fn read_payload_at(file: &File, payload: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(payload, offset)
}

#[cfg(windows)]
pub(crate) fn read_payload_at(
    file: &File,
    mut payload: &mut [u8],
    mut offset: u64,
) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !payload.is_empty() {
        match file.seek_read(payload, offset) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(count) => {
                payload = &mut payload[count..];
                offset += count as u64;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn regular_put_range(block: &Block, rows: Range<usize>) -> Checked<Option<(i64, i64, i128)>> {
    check(
        rows.start < rows.end && rows.end <= block.count,
        "Invalid selected block rows",
    )?;
    for name in ["op", "valid"] {
        let column = block.columns.get(name).ok_or("Missing block column")?;
        if column.mode != "constant" || column.first.as_deref() != Some("1") {
            return Ok(None);
        }
        check(column.length == 0, "Unexpected constant payload")?;
    }
    let timestamp = block.columns.get("timestamp").ok_or("Missing timestamp")?;
    if !matches!(timestamp.mode.as_str(), "constant" | "linear") {
        return Ok(None);
    }
    check(
        timestamp.length == 0,
        "Unexpected arithmetic timestamp payload",
    )?;
    let first = timestamp
        .first
        .as_deref()
        .ok_or("Missing first timestamp")?
        .parse::<i128>()
        .map_err(|e| e.to_string())?;
    let step = if timestamp.mode == "constant" {
        0
    } else {
        timestamp
            .step
            .as_deref()
            .ok_or("Missing timestamp step")?
            .parse::<i128>()
            .map_err(|e| e.to_string())?
    };
    if step < 0 || (block.count > 1 && step == 0) {
        return Ok(None);
    }
    let at = |index: usize| -> Checked<i64> {
        let value = step
            .checked_mul(index as i128)
            .and_then(|offset| first.checked_add(offset))
            .ok_or("Timestamp arithmetic overflow")?;
        i64::try_from(value).map_err(|e| e.to_string())
    };
    // Validate the whole block before using a selected subrange as an ordering proof.
    at(0)?;
    at(block.count - 1)?;
    Ok(Some((at(rows.start)?, at(rows.end - 1)?, step)))
}

fn error(reason: impl std::fmt::Display) -> crate::error::Error {
    UnexpectedSnafu {
        reason: format!("Research SST sidecar: {reason}"),
    }
    .build()
}

fn check(condition: bool, reason: &str) -> Checked<()> {
    if condition {
        Ok(())
    } else {
        Err(reason.to_string())
    }
}

fn read_bounded(path: &Path, limit: u64) -> Checked<Vec<u8>> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    check(
        file.metadata().map_err(|e| e.to_string())?.len() <= limit,
        "Oversized metadata",
    )?;
    let mut data = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut data)
        .map_err(|e| e.to_string())?;
    check(data.len() as u64 <= limit, "Metadata grew while reading")?;
    Ok(data)
}

impl Sidecar {
    fn open_data_file(&self) -> Checked<Arc<File>> {
        let mut weak = self.data_file.lock().map_err(|e| e.to_string())?;
        if let Some(file) = weak.upgrade() {
            return Ok(file);
        }
        let file = Arc::new(File::open(&self.data_path).map_err(|e| e.to_string())?);
        // Metadata caching must not retain file descriptors after the scan ends.
        *weak = Arc::downgrade(&file);
        Ok(file)
    }

    fn select_blocks(
        self: &Arc<Self>,
        group: usize,
        selection: Option<&RowSelection>,
        series: Option<&[[u8; 22]]>,
    ) -> Checked<Option<Vec<SelectedSeriesBlock>>> {
        if self.pages.is_empty() {
            return Ok(None);
        }
        let rows = *self.group_rows.get(group).ok_or("Missing row group")?;
        let ranges = selected_ranges(selection, rows)?;
        let mut selected: Vec<SelectedSeriesBlock> = Vec::new();
        for descriptor in self.pages.get(group).ok_or("Missing directory row group")? {
            self.select_page_blocks(descriptor, &ranges, series, &mut selected)?;
        }
        Ok(Some(selected))
    }

    fn select_file_blocks(
        self: &Arc<Self>,
        series: &[[u8; 22]],
    ) -> Checked<Option<Vec<SelectedSeriesBlock>>> {
        if self.page_intervals.is_empty() {
            return Ok(None);
        }
        let mut selected = Vec::new();
        for entry in PageInterval::candidates(&self.page_intervals, series) {
            let descriptor = &self.pages[entry.group][entry.page];
            self.select_page_blocks(
                descriptor,
                &[0..self.group_rows[entry.group]],
                Some(series),
                &mut selected,
            )?;
        }
        Ok(Some(selected))
    }

    fn select_page_blocks(
        self: &Arc<Self>,
        descriptor: &Page,
        ranges: &[std::ops::Range<usize>],
        series: Option<&[[u8; 22]]>,
        selected: &mut Vec<SelectedSeriesBlock>,
    ) -> Checked<()> {
        let page_series = match (series, descriptor.series_bounds) {
            (Some(series), Some((min, max))) => {
                let start = series.partition_point(|key| key < &min);
                let end = series.partition_point(|key| key <= &max);
                if start == end {
                    return Ok(());
                }
                Some(&series[start..end])
            }
            (Some(_), None) => return Err("Missing series bounds for selected page".into()),
            (None, _) => None,
        };
        if !ranges.iter().any(|range| {
            range.start < descriptor.row_start + descriptor.count
                && range.end > descriptor.row_start
        }) {
            return Ok(());
        }
        let page = self.read_page(descriptor)?;
        for (block_index, block) in page.blocks.iter().enumerate() {
            if let Some(series) = page_series {
                let prefix: [u8; 22] = block
                    .key
                    .get(..22)
                    .ok_or("Invalid block prefix")?
                    .try_into()
                    .map_err(|_| "Invalid block prefix")?;
                if series.binary_search(&prefix).is_err() {
                    continue;
                }
            }
            for range in ranges {
                let start = range.start.max(block.row_start);
                let end = range.end.min(block.row_start + block.count);
                if start < end {
                    let data_file = match selected.first() {
                        Some(block) => block.data_file.clone(),
                        None => self.open_data_file()?,
                    };
                    selected.push(SelectedSeriesBlock {
                        _sidecar: self.clone(),
                        data_file,
                        page: page.clone(),
                        block_index,
                        rows: start - block.row_start..end - block.row_start,
                    });
                }
            }
        }
        Ok(())
    }

    fn open(directory: &Path, source: &Path, metadata: &ParquetMetaData) -> Checked<Self> {
        let seal: Seal =
            serde_json::from_slice(&read_bounded(&directory.join("validation.json"), 4096)?)
                .map_err(|e| e.to_string())?;
        check(seal.status == "pass", "Unvalidated sidecar")?;
        let bytes = read_bounded(&directory.join("index.json"), 64 * 1024 * 1024)?;
        check(
            crc32fast::hash(&bytes) == seal.index_crc32,
            "Index checksum mismatch",
        )?;
        let mut index: Index = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        check(matches!(index.version, 2 | 3), "Unsupported index version")?;
        check(
            index.source_sha256 == seal.source_sha256 && index.source_bytes == seal.source_bytes,
            "Source seal mismatch",
        )?;
        check(
            index.rows == metadata.file_metadata().num_rows() as usize
                && index.row_groups == metadata.num_row_groups(),
            "Source row count mismatch",
        )?;
        let mut source = File::open(source).map_err(|e| e.to_string())?;
        check(
            source.metadata().map_err(|e| e.to_string())?.len() == index.source_bytes,
            "Source size mismatch",
        )?;
        let mut hasher = crc32fast::Hasher::new();
        let mut buffer = vec![0; 1024 * 1024];
        loop {
            let count = source.read(&mut buffer).map_err(|e| e.to_string())?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        check(
            hasher.finalize() == seal.source_crc32,
            "Source checksum mismatch",
        )?;
        let data_path = directory.join("columns.bin");
        check(
            std::fs::metadata(&data_path)
                .map_err(|e| e.to_string())?
                .len()
                == index.data_bytes,
            "Sidecar size mismatch",
        )?;
        let group_rows: Vec<_> = metadata
            .row_groups()
            .iter()
            .map(|group| group.num_rows() as usize)
            .collect();
        let pages_path = directory.join("index.pages");
        if index.version == 3 {
            load_series_directory(directory, &seal, &mut index.pages)?;
            check(
                index.blocks.is_empty(),
                "Paged directory contains inline blocks",
            )?;
            check(
                std::fs::metadata(&pages_path)
                    .map_err(|e| e.to_string())?
                    .len()
                    == index.pages_bytes,
                "Directory pages size mismatch",
            )?;
            let mut pages: Vec<Vec<Page>> = (0..index.row_groups).map(|_| Vec::new()).collect();
            let mut offset = 0u64;
            for page in index.pages {
                check(
                    page.row_group < pages.len()
                        && page.blocks > 0
                        && page.blocks <= 128
                        && page.count > 0
                        && page.count <= page.blocks * DEFAULT_READ_BATCH_SIZE
                        && page.raw_length > 0
                        && page.raw_length <= 8 * 1024 * 1024
                        && page.length > 0
                        && page.length <= 8 * 1024 * 1024
                        && page.offset == offset,
                    "Invalid directory page",
                )?;
                offset = offset
                    .checked_add(page.length as u64)
                    .ok_or("Page offset overflow")?;
                pages[page.row_group].push(page);
            }
            check(
                offset == index.pages_bytes,
                "Directory page coverage mismatch",
            )?;
            for (group, entries) in pages.iter().enumerate() {
                let mut row = 0usize;
                for page in entries {
                    check(page.row_start == row, "Directory row coverage mismatch")?;
                    row = row
                        .checked_add(page.count)
                        .ok_or("Directory row overflow")?;
                }
                check(row == group_rows[group], "Directory row-group mismatch")?;
            }
            let page_intervals = PageInterval::build(&pages);
            let weight = std::mem::size_of::<Self>()
                + page_intervals.capacity() * std::mem::size_of::<PageInterval>()
                + seal.source_sha256.capacity()
                + data_path.as_os_str().len()
                + pages_path.as_os_str().len()
                + pages.capacity() * std::mem::size_of::<Vec<Page>>()
                + pages
                    .iter()
                    .map(|p| p.capacity() * std::mem::size_of::<Page>())
                    .sum::<usize>()
                + group_rows.capacity() * std::mem::size_of::<usize>();
            common_telemetry::info!("Research paged sidecar validated: {}", directory.display());
            return Ok(Self {
                source_identity: seal.source_sha256,
                source_index_crc32: seal.index_crc32,
                data_path,
                data_file: Mutex::new(Weak::new()),
                groups: Vec::new(),
                pages_path,
                pages,
                page_intervals,
                group_rows,
                data_bytes: index.data_bytes,
                weight: u32::try_from(weight).map_err(|e| e.to_string())?,
            });
        }
        let mut groups: Vec<Vec<Block>> = (0..index.row_groups).map(|_| Vec::new()).collect();
        for mut block in index.blocks {
            check(
                block.row_group < groups.len()
                    && block.count > 0
                    && block.count <= DEFAULT_READ_BATCH_SIZE,
                "Invalid block bounds",
            )?;
            check(
                block.key_hex.len() % 2 == 0 && block.key_hex.is_ascii(),
                "Invalid primary key",
            )?;
            block.key = (0..block.key_hex.len())
                .step_by(2)
                .map(|i| {
                    u8::from_str_radix(&block.key_hex[i..i + 2], 16).map_err(|e| e.to_string())
                })
                .collect::<Checked<Vec<_>>>()?;
            block.key_hex = String::new();
            for (name, dtype) in [
                ("timestamp", "<i8"),
                ("value_bits", "<u8"),
                ("sequence", "<u8"),
                ("valid", "|u1"),
                ("op", "|u1"),
            ] {
                let column = block.columns.get(name).ok_or("Missing column")?;
                check(
                    column.count == block.count
                        && column.dtype == dtype
                        && column.length <= block.count * 8
                        && column
                            .offset
                            .checked_add(column.length as u64)
                            .is_some_and(|end| end <= index.data_bytes),
                    "Invalid column bounds or type",
                )?;
            }
            groups[block.row_group].push(block);
        }
        for (i, blocks) in groups.iter().enumerate() {
            let mut position = 0;
            for block in blocks {
                check(block.row_start == position, "Block coverage mismatch")?;
                position += block.count;
            }
            check(
                position == metadata.row_group(i).num_rows() as usize,
                "Row-group coverage mismatch",
            )?;
        }
        common_telemetry::info!("Research sidecar validated: {}", directory.display());
        let mut weight = std::mem::size_of::<Self>()
            + seal.source_sha256.capacity()
            + data_path.as_os_str().len()
            + pages_path.as_os_str().len()
            + group_rows.capacity() * std::mem::size_of::<usize>()
            + groups.capacity() * std::mem::size_of::<Vec<Block>>();
        for blocks in &groups {
            weight += blocks.capacity() * std::mem::size_of::<Block>();
            for block in blocks {
                weight += block.key.capacity()
                    + (block.columns.capacity() + 1).next_power_of_two()
                        * (std::mem::size_of::<(String, Column)>() + 1);
                for (name, column) in &block.columns {
                    weight += name.capacity()
                        + column.dtype.capacity()
                        + column.mode.capacity()
                        + column.first.as_ref().map_or(0, String::capacity)
                        + column.step.as_ref().map_or(0, String::capacity);
                }
            }
        }
        let weight = u32::try_from(weight).map_err(|e| e.to_string())?;
        Ok(Self {
            source_identity: seal.source_sha256,
            source_index_crc32: seal.index_crc32,
            data_path,
            data_file: Mutex::new(Weak::new()),
            groups,
            pages_path,
            pages: Vec::new(),
            page_intervals: Vec::new(),
            group_rows,
            data_bytes: index.data_bytes,
            weight,
        })
    }

    fn read_page(&self, page: &Page) -> Checked<Arc<DirectoryPage>> {
        let key = CacheKey::Page(self.pages_path.clone(), page.offset, page.crc32);
        let value = INDEXES
            .try_get_with(key, || -> Checked<Cached> {
                let mut file = File::open(&self.pages_path).map_err(|e| e.to_string())?;
                file.seek(SeekFrom::Start(page.offset))
                    .map_err(|e| e.to_string())?;
                let mut compressed = vec![0; page.length];
                file.read_exact(&mut compressed)
                    .map_err(|e| e.to_string())?;
                let raw = zstd::bulk::decompress(&compressed, page.raw_length)
                    .map_err(|e| e.to_string())?;
                check(
                    raw.len() == page.raw_length && crc32fast::hash(&raw) == page.crc32,
                    "Directory page checksum mismatch",
                )?;
                let mut blocks: Vec<Block> =
                    serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
                check(
                    blocks.len() == page.blocks,
                    "Directory block count mismatch",
                )?;
                let mut row = page.row_start;
                for block in &mut blocks {
                    check(
                        block.row_group == page.row_group
                            && block.row_start == row
                            && block.count > 0
                            && block.count <= DEFAULT_READ_BATCH_SIZE,
                        "Directory block coverage mismatch",
                    )?;
                    row = row.checked_add(block.count).ok_or("Block row overflow")?;
                    check(
                        block.key_hex.len() % 2 == 0 && block.key_hex.is_ascii(),
                        "Invalid primary key",
                    )?;
                    block.key = (0..block.key_hex.len())
                        .step_by(2)
                        .map(|i| {
                            u8::from_str_radix(&block.key_hex[i..i + 2], 16)
                                .map_err(|e| e.to_string())
                        })
                        .collect::<Checked<Vec<_>>>()?;
                    block.key_hex = String::new();
                    for (name, dtype) in [
                        ("timestamp", "<i8"),
                        ("value_bits", "<u8"),
                        ("sequence", "<u8"),
                        ("valid", "|u1"),
                        ("op", "|u1"),
                    ] {
                        let column = block.columns.get(name).ok_or("Missing column")?;
                        check(
                            column.count == block.count
                                && column.dtype == dtype
                                && column.length <= block.count * 8
                                && column
                                    .offset
                                    .checked_add(column.length as u64)
                                    .is_some_and(|end| end <= self.data_bytes),
                            "Invalid column bounds or type",
                        )?;
                    }
                }
                if let Some((min, max)) = page.series_bounds {
                    check(
                        blocks.iter().all(|block| block.key.len() >= 22)
                            && blocks.windows(2).all(|pair| pair[0].key <= pair[1].key)
                            && blocks.first().is_some_and(|block| block.key[..22] == min)
                            && blocks.last().is_some_and(|block| block.key[..22] == max),
                        "Series directory bounds do not match page",
                    )?;
                }
                check(
                    row.checked_sub(page.row_start) == Some(page.count),
                    "Directory page row count mismatch",
                )?;
                let mut weight = std::mem::size_of::<DirectoryPage>()
                    + blocks.capacity() * std::mem::size_of::<Block>();
                for block in &blocks {
                    weight += block.key.capacity()
                        + (block.columns.capacity() + 1).next_power_of_two()
                            * (std::mem::size_of::<(String, Column)>() + 1);
                    for (name, column) in &block.columns {
                        weight += name.capacity()
                            + column.dtype.capacity()
                            + column.mode.capacity()
                            + column.first.as_ref().map_or(0, String::capacity)
                            + column.step.as_ref().map_or(0, String::capacity);
                    }
                }
                Ok(Cached::Page(Arc::new(DirectoryPage {
                    blocks,
                    weight: u32::try_from(weight).map_err(|e| e.to_string())?,
                })))
            })
            .map_err(|e| e.to_string())?;
        match value {
            Cached::Page(page) => Ok(page),
            Cached::Index(_) => Err("Directory cache key mismatch".into()),
        }
    }

    fn read_group(
        &self,
        group: usize,
        schema: SchemaRef,
        selection: Option<RowSelection>,
        metrics: Option<ParquetFetchMetrics>,
    ) -> Checked<Vec<RecordBatch>> {
        let started = Instant::now();
        let rows = *self.group_rows.get(group).ok_or("Missing row group")?;
        let ranges = selected_ranges(selection.as_ref(), rows)?;
        let mut loaded = Vec::new();
        let blocks: Vec<&Block> = if self.pages.is_empty() {
            self.groups
                .get(group)
                .ok_or("Missing row group")?
                .iter()
                .collect()
        } else {
            for page in self.pages.get(group).ok_or("Missing directory row group")? {
                if ranges.iter().any(|range| {
                    range.start < page.row_start + page.count && range.end > page.row_start
                }) {
                    loaded.push(self.read_page(page)?);
                }
            }
            loaded.iter().flat_map(|page| page.blocks.iter()).collect()
        };
        let mut file = File::open(&self.data_path).map_err(|e| e.to_string())?;
        let mut batches = Vec::new();
        let mut bytes_read = 0;
        let mut reads = 0;
        for block in blocks {
            let slices: Vec<_> = ranges
                .iter()
                .filter_map(|r| {
                    let start = r.start.max(block.row_start);
                    let end = r.end.min(block.row_start + block.count);
                    (start < end)
                        .then_some(start - block.row_start..end.saturating_sub(block.row_start))
                })
                .collect();
            if slices.is_empty() {
                continue;
            }
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
            for field in schema.fields() {
                let mut read = |name: &str| -> Checked<Vec<u8>> {
                    let column = block.columns.get(name).ok_or("Missing column")?;
                    let mut payload = vec![0; column.length];
                    if column.length != 0 {
                        file.seek(SeekFrom::Start(column.offset))
                            .map_err(|e| e.to_string())?;
                        file.read_exact(&mut payload).map_err(|e| e.to_string())?;
                        bytes_read += column.length;
                        reads += 1;
                    }
                    decode(column, &payload)
                };
                let array: ArrayRef = match field.name().as_str() {
                    "greptime_value" => {
                        let raw = read("value_bits")?;
                        let valid = read("valid")?;
                        check(valid.iter().all(|b| *b <= 1), "Invalid validity byte")?;
                        let values: Vec<_> = words(&raw).map(f64::from_bits).collect();
                        let nulls = valid.contains(&0).then(|| {
                            NullBuffer::from(valid.iter().map(|b| *b != 0).collect::<Vec<_>>())
                        });
                        Arc::new(Float64Array::new(values.into(), nulls))
                    }
                    "greptime_timestamp" => Arc::new(TimestampMillisecondArray::from(
                        words(&read("timestamp")?)
                            .map(|v| v as i64)
                            .collect::<Vec<_>>(),
                    )),
                    "__sequence" => Arc::new(UInt64Array::from(
                        words(&read("sequence")?).collect::<Vec<_>>(),
                    )),
                    "__op_type" => Arc::new(UInt8Array::from(read("op")?)),
                    "__primary_key" => match field.data_type() {
                        DataType::Binary => Arc::new(BinaryArray::from_iter_values(
                            std::iter::repeat_n(block.key.as_slice(), block.count),
                        )),
                        DataType::Dictionary(key, _) if key.as_ref() == &DataType::UInt32 => {
                            Arc::new(
                                DictionaryArray::<UInt32Type>::try_new(
                                    UInt32Array::from(vec![0; block.count]),
                                    Arc::new(BinaryArray::from_iter_values([block.key.as_slice()])),
                                )
                                .map_err(|e| e.to_string())?,
                            )
                        }
                        _ => Arc::new(
                            DictionaryArray::<Int32Type>::try_new(
                                Int32Array::from(vec![0; block.count]),
                                Arc::new(BinaryArray::from_iter_values([block.key.as_slice()])),
                            )
                            .map_err(|e| e.to_string())?,
                        ),
                    },
                    _ => return Err("Unsupported projected field".to_string()),
                };
                columns.push(array);
            }
            let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| e.to_string())?;
            for slice in slices {
                batches.push(batch.slice(slice.start, slice.len()));
            }
        }
        if let Some(metrics) = metrics {
            let mut data = metrics.data.lock().map_err(|e| e.to_string())?;
            data.pages_to_fetch_store += reads;
            data.page_size_to_fetch_store += bytes_read as u64;
            data.page_size_needed += bytes_read as u64;
            data.total_fetch_elapsed += started.elapsed();
        }
        // Preserve the ordinary reader's <=8192 row contract for sequence overrides.
        let mut output = Vec::new();
        let mut pending = Vec::new();
        let mut count = 0;
        for batch in batches {
            let mut offset = 0;
            while offset < batch.num_rows() {
                let take = (DEFAULT_READ_BATCH_SIZE - count).min(batch.num_rows() - offset);
                pending.push(batch.slice(offset, take));
                offset += take;
                count += take;
                if count == DEFAULT_READ_BATCH_SIZE {
                    output.push(concat_batches(&schema, &pending).map_err(|e| e.to_string())?);
                    pending.clear();
                    count = 0;
                }
            }
        }
        if count != 0 {
            output.push(concat_batches(&schema, &pending).map_err(|e| e.to_string())?);
        }
        Ok(output)
    }
}

fn words(raw: &[u8]) -> impl Iterator<Item = u64> + '_ {
    raw.chunks_exact(8)
        .map(|b| u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
}

fn decode(column: &Column, payload: &[u8]) -> Checked<Vec<u8>> {
    let width = match column.dtype.as_str() {
        "<i8" | "<u8" => 8,
        "|u1" => 1,
        _ => return Err("Unknown dtype".to_string()),
    };
    check(
        column.count <= DEFAULT_READ_BATCH_SIZE && column.count > 0,
        "Invalid column count",
    )?;
    let size = column.count * width;
    let raw = match column.mode.as_str() {
        "constant" | "linear" => {
            check(payload.is_empty(), "Unexpected arithmetic payload")?;
            let first: i128 = column
                .first
                .as_ref()
                .ok_or("Missing first value")?
                .parse::<i128>()
                .map_err(|e| e.to_string())?;
            let step = if column.mode == "linear" {
                column
                    .step
                    .as_ref()
                    .ok_or("Missing step")?
                    .parse::<i128>()
                    .map_err(|e| e.to_string())?
            } else {
                0
            };
            let (min, max) = match column.dtype.as_str() {
                "<i8" => (i64::MIN as i128, i64::MAX as i128),
                "<u8" => (0, u64::MAX as i128),
                _ => (0, 255),
            };
            let mut raw = Vec::with_capacity(size);
            for i in 0..column.count {
                let value = step
                    .checked_mul(i as i128)
                    .and_then(|v| first.checked_add(v))
                    .ok_or("Arithmetic overflow")?;
                check(
                    (min..=max).contains(&value),
                    "Arithmetic value out of bounds",
                )?;
                raw.extend_from_slice(&(value as u64).to_le_bytes()[..width]);
            }
            raw
        }
        "raw" => payload.to_vec(),
        "zstd" => zstd::bulk::decompress(payload, size).map_err(|e| e.to_string())?,
        "delta-zstd" | "xor-zstd" => {
            check(width == 8, "Invalid transformed column")?;
            let shuffled = zstd::bulk::decompress(payload, size).map_err(|e| e.to_string())?;
            check(shuffled.len() == size, "Decoded length mismatch")?;
            let mut raw = Vec::with_capacity(size);
            let mut previous = 0u64;
            for i in 0..column.count {
                let mut bytes = [0u8; 8];
                for j in 0..8 {
                    bytes[j] = shuffled[j * column.count + i];
                }
                let packed = u64::from_le_bytes(bytes);
                previous = if column.mode == "delta-zstd" {
                    previous.wrapping_add(packed)
                } else {
                    previous ^ packed
                };
                raw.extend_from_slice(&previous.to_le_bytes());
            }
            raw
        }
        _ => return Err("Unknown encoding".to_string()),
    };
    check(
        raw.len() == size && crc32fast::hash(&raw) == column.crc32,
        "Column checksum or length mismatch",
    )?;
    Ok(raw)
}

fn selected_ranges(selection: Option<&RowSelection>, rows: usize) -> Checked<Vec<Range<usize>>> {
    let Some(selection) = selection else {
        return Ok(vec![0..rows]);
    };
    let mut result = Vec::new();
    let mut position: usize = 0;
    for selector in selection.iter() {
        let end = position
            .checked_add(selector.row_count)
            .ok_or("Selection overflow")?;
        check(end <= rows, "Selection exceeds row group")?;
        if !selector.skip && position < end {
            result.push(position..end);
        }
        position = end;
    }
    Ok(result)
}

/// Returns a local auxiliary stream only for the exact supported metric schema.
async fn load_sidecar(
    source: PathBuf,
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
) -> Result<Option<Arc<Sidecar>>> {
    let Some(root) = std::env::var_os("GT_RESEARCH_SST_SIDECAR") else {
        return Ok(None);
    };
    let fields = schema.fields();
    let names = [
        "greptime_value",
        "greptime_timestamp",
        "__primary_key",
        "__sequence",
        "__op_type",
    ];
    if fields.len() != 5
        || fields.iter().zip(names).any(|(f, name)| f.name() != name)
        || fields[0].data_type() != &DataType::Float64
        || fields[1].data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
        || fields[3].data_type() != &DataType::UInt64
        || fields[4].data_type() != &DataType::UInt8
        || !matches!(
            fields[2].data_type(),
            DataType::Binary | DataType::Dictionary(_, _)
        )
    {
        return Ok(None);
    }
    if let DataType::Dictionary(key, value) = fields[2].data_type()
        && (!matches!(key.as_ref(), DataType::Int32 | DataType::UInt32)
            || value.as_ref() != &DataType::Binary)
    {
        return Ok(None);
    }
    let stem = source
        .file_stem()
        .ok_or_else(|| error("Missing source file name"))?;
    let directory = PathBuf::from(root).join(stem);
    let cache_key = CacheKey::Index(directory.clone(), source.clone());
    let sidecar = if let Some(Cached::Index(value)) = INDEXES.get(&cache_key) {
        value
    } else {
        let loaded =
            common_runtime::spawn_blocking_query(move || -> Checked<Option<Arc<Sidecar>>> {
                match std::fs::metadata(directory.join("index.json")) {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(e.to_string()),
                    Ok(_) => {}
                }
                INDEXES
                    .try_get_with(cache_key, || {
                        Sidecar::open(&directory, &source, &metadata)
                            .map(|value| Cached::Index(Arc::new(value)))
                    })
                    .map_err(|e| e.to_string())
                    .and_then(|value| match value {
                        Cached::Index(value) => Ok(Some(value)),
                        Cached::Page(_) => Err("Directory cache key mismatch".into()),
                    })
            })
            .await
            .map_err(error)?
            .map_err(error)?;
        let Some(loaded) = loaded else {
            return Ok(None);
        };
        loaded
    };
    Ok(Some(sidecar))
}

pub(crate) struct PreparedSeriesSelection {
    sidecar: Arc<Sidecar>,
    group: Option<usize>,
    selection: Option<RowSelection>,
    series: Option<Arc<Vec<[u8; 22]>>>,
}

impl PreparedSeriesSelection {
    pub(crate) fn select(self) -> Result<Option<Vec<SelectedSeriesBlock>>> {
        if let Some(group) = self.group {
            return self
                .sidecar
                .select_blocks(
                    group,
                    self.selection.as_ref(),
                    self.series.as_deref().map(Vec::as_slice),
                )
                .map_err(error);
        }
        let series = self
            .series
            .as_deref()
            .ok_or_else(|| error("Missing file series selection"))?;
        self.sidecar.select_file_blocks(series).map_err(error)
    }
}

pub(crate) async fn prepare_series_blocks(
    source: PathBuf,
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
    group: usize,
    selection: Option<RowSelection>,
    series: Option<Arc<Vec<[u8; 22]>>>,
) -> Result<Option<PreparedSeriesSelection>> {
    let Some(sidecar) = load_sidecar(source, schema, metadata).await? else {
        return Ok(None);
    };
    Ok(Some(PreparedSeriesSelection {
        sidecar,
        group: Some(group),
        selection,
        series,
    }))
}

#[derive(Clone)]
pub(crate) struct PreparedSeriesFile {
    sidecar: Arc<Sidecar>,
    native_file: OnceLock<Arc<NativeValueFile>>,
}

pub(crate) struct NativeValueColumn {
    column: Column,
    native_values: OnceLock<ExactIntegerSeries>,
}

impl NativeValueColumn {
    pub(crate) fn new(
        count: usize,
        mode: u8,
        crc32: u32,
        offset: u64,
        length: usize,
        first: u64,
        step: i128,
    ) -> Checked<Self> {
        check(
            count > 0 && count <= DEFAULT_READ_BATCH_SIZE,
            "Invalid native value count",
        )?;
        check(
            length <= count * 16 + 4096,
            "Native payload exceeds decode bound",
        )?;
        let mode = match mode {
            0 => "constant",
            1 => "linear",
            2 => "raw",
            3 => "zstd",
            4 => "delta-zstd",
            5 => "xor-zstd",
            _ => return Err("Unknown native value codec".into()),
        };
        let arithmetic = matches!(mode, "constant" | "linear");
        if arithmetic {
            let last = step
                .checked_mul((count - 1) as i128)
                .and_then(|delta| i128::from(first).checked_add(delta))
                .ok_or("Native arithmetic overflow")?;
            check(
                length == 0
                    && (0..=u64::MAX as i128).contains(&last)
                    && (mode != "constant" || step == 0),
                "Invalid native arithmetic values",
            )?;
        } else {
            check(
                first == 0 && step == 0,
                "Unexpected native arithmetic parameters",
            )?;
        }
        Ok(Self {
            column: Column {
                dtype: "<u8".into(),
                count,
                crc32,
                mode: mode.into(),
                first: arithmetic.then(|| first.to_string()),
                step: (mode == "linear").then(|| step.to_string()),
                offset,
                length,
            },
            native_values: OnceLock::new(),
        })
    }

    pub(crate) fn decode_bytes(&self) -> usize {
        self.column.length + self.column.count * 16
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self.column.dtype.capacity()
            + self.column.mode.capacity()
            + self.column.first.as_ref().map_or(0, String::capacity)
            + self.column.step.as_ref().map_or(0, String::capacity)
    }
}

pub(crate) struct NativeValueFile {
    sidecar: Arc<Sidecar>,
    data_file: Arc<File>,
}

impl NativeValueFile {
    pub(crate) fn validate(&self, column: &NativeValueColumn) -> Result<()> {
        let column = &column.column;
        check(
            column
                .offset
                .checked_add(column.length as u64)
                .is_some_and(|end| end <= self.sidecar.data_bytes),
            "Native value extent outside source",
        )
        .map_err(error)
    }

    pub(crate) fn read(
        &self,
        column: &NativeValueColumn,
        native: bool,
    ) -> Result<VisibleSeriesValues> {
        self.validate(column)?;
        let cache = VALUES.as_ref().map_err(Clone::clone).map_err(error)?;
        read_block_values(
            &self.sidecar,
            &self.data_file,
            &column.column,
            &column.native_values,
            cache.as_ref(),
            native,
        )
        .map_err(error)
    }
}

impl PreparedSeriesFile {
    pub(crate) fn native_values(&self) -> Result<Arc<NativeValueFile>> {
        if let Some(file) = self.native_file.get() {
            return Ok(file.clone());
        }
        // Keep payload handles in the scan, not the shared metadata cache.
        // Coalesced reads validate source metadata without opening this payload.
        let file = Arc::new(NativeValueFile {
            sidecar: self.sidecar.clone(),
            data_file: self.sidecar.open_data_file().map_err(error)?,
        });
        Ok(self.native_file.get_or_init(|| file).clone())
    }

    pub(crate) fn matches_source(&self, sha256: &str, index_crc32: u32) -> bool {
        self.sidecar.source_identity == sha256 && self.sidecar.source_index_crc32 == index_crc32
    }

    pub(crate) fn select_address(
        &self,
        group: u32,
        page: u32,
        block_index: u32,
        expected_key: &[u8],
    ) -> Result<SelectedSeriesBlock> {
        let descriptor = self
            .sidecar
            .pages
            .get(group as usize)
            .and_then(|pages| pages.get(page as usize))
            .ok_or_else(|| error("Series block page address out of bounds"))?;
        let page = self.sidecar.read_page(descriptor).map_err(error)?;
        let block = page
            .blocks
            .get(block_index as usize)
            .ok_or_else(|| error("Series block address out of bounds"))?;
        if block.key != expected_key {
            return Err(error("Series directory primary key mismatch"));
        }
        let rows = 0..block.count;
        Ok(SelectedSeriesBlock {
            _sidecar: self.sidecar.clone(),
            data_file: self.sidecar.open_data_file().map_err(error)?,
            page,
            block_index: block_index as usize,
            rows,
        })
    }

    pub(crate) fn selection(
        &self,
        group: Option<usize>,
        series: Arc<Vec<[u8; 22]>>,
    ) -> PreparedSeriesSelection {
        PreparedSeriesSelection {
            sidecar: self.sidecar.clone(),
            group,
            selection: None,
            series: Some(series),
        }
    }
}

pub(crate) async fn prepare_file_series_blocks(
    source: PathBuf,
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
) -> Result<Option<PreparedSeriesFile>> {
    let Some(sidecar) = load_sidecar(source, schema, metadata).await? else {
        return Ok(None);
    };
    if sidecar.page_intervals.is_empty() {
        return Ok(None);
    }
    Ok(Some(PreparedSeriesFile {
        sidecar,
        native_file: OnceLock::new(),
    }))
}

pub(crate) async fn try_stream(
    source: PathBuf,
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
    group: usize,
    projection: &ProjectionMask,
    selection: Option<RowSelection>,
    metrics: Option<ParquetFetchMetrics>,
) -> Result<Option<ProjectedRecordBatchStream>> {
    let Some(sidecar) = load_sidecar(source, schema.clone(), metadata).await? else {
        return Ok(None);
    };
    let columns: Vec<_> = (0..5).filter(|i| projection.leaf_included(*i)).collect();
    if columns.is_empty() {
        return Ok(None);
    }
    let schema = Arc::new(schema.project(&columns).map_err(error)?);
    let stream = async_stream::try_stream! {
        let batches = common_runtime::spawn_blocking_query(move || sidecar.read_group(group, schema, selection, metrics))
            .await.map_err(error)?.map_err(error)?;
        for batch in batches { yield batch; }
    };
    Ok(Some(stream.boxed()))
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::array::Array;
    use datatypes::arrow::compute::cast;
    use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelector};

    use super::*;

    #[test]
    fn test_max_summary_preparation_and_validity() {
        for instants in [vec![], vec![300_000, 0], vec![0, 299_999]] {
            let request = store_api::region_engine::RangeMaxRequest {
                instants,
                window: 300_000,
                offset: 0,
            };
            assert!(PreparedRangeMaxRequest::new(&request, PathBuf::new()).is_err());
        }
        let request = store_api::region_engine::RangeMaxRequest {
            instants: vec![i64::MIN, 0, i64::MAX],
            window: 300_000,
            offset: 15_000,
        };
        assert!(PreparedRangeMaxRequest::new(&request, PathBuf::new()).is_ok());
        assert_eq!(
            pack_max_validity(&[1, 0, 0, 0, 0, 0, 0, 1, 1])
                .unwrap()
                .as_ref(),
            &[129, 1]
        );
        assert!(pack_max_validity(&[1, 2, 0]).is_err());
    }

    #[test]
    fn test_series_directory_selection_and_identity() {
        let temp = common_test_util::temp_dir::create_temp_dir("series-directory");
        let path = temp.path();
        let key = |id: u64| format!("8000000401000000018000000301{id:016x}");
        let mut bytes = Vec::new();
        let mut pages = Vec::new();
        let mut bounds = Vec::new();
        for (page_index, ids) in [[1, 2], [2, 3]].into_iter().enumerate() {
            let blocks: Vec<_> = ids
                .iter()
                .enumerate()
                .map(|(index, id)| {
                    let columns: serde_json::Map<String, serde_json::Value> = [
                        ("timestamp", "<i8"),
                        ("value_bits", "<u8"),
                        ("sequence", "<u8"),
                        ("valid", "|u1"),
                        ("op", "|u1"),
                    ]
                    .into_iter()
                    .map(|(name, dtype)| {
                        (
                            name.into(),
                            serde_json::json!({
                                "dtype": dtype, "count": 2, "crc32": 0, "mode": "constant",
                                "first": "1", "offset": 0, "length": 0,
                            }),
                        )
                    })
                    .collect();
                    serde_json::json!({"row_group": 0, "row_start": page_index * 4 + index * 2,
                    "count": 2, "key_hex": key(*id), "columns": columns})
                })
                .collect();
            let raw = serde_json::to_vec(&blocks).unwrap();
            let compressed = zstd::bulk::compress(&raw, 1).unwrap();
            let page = Page {
                row_group: 0,
                row_start: page_index * 4,
                count: 4,
                blocks: 2,
                offset: bytes.len() as u64,
                length: compressed.len(),
                raw_length: raw.len(),
                crc32: crc32fast::hash(&raw),
                series_bounds: None,
            };
            bounds.push(serde_json::json!({"row_group": 0, "offset": page.offset,
                "crc32": page.crc32, "min_key": key(ids[0]), "max_key": key(ids[1]),
                "sorted_keys": true}));
            bytes.extend(compressed);
            pages.push(page);
        }
        std::fs::write(path.join("index.pages"), bytes).unwrap();
        std::fs::write(path.join("columns.bin"), []).unwrap();
        let directory = serde_json::to_vec(&serde_json::json!({"version": 1,
            "source_sha256": "source-a", "source_index_crc32": 123, "pages": bounds}))
        .unwrap();
        std::fs::write(path.join("sample.json"), &directory).unwrap();
        std::fs::write(
            path.join("manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "status": "pass", "files": [{"sst": "sample", "crc32": crc32fast::hash(&directory)}]
            }))
            .unwrap(),
        )
        .unwrap();
        let mut seal = Seal {
            status: "pass".into(),
            source_crc32: 0,
            source_sha256: "source-b".into(),
            source_bytes: 0,
            index_crc32: 123,
        };
        assert!(read_series_directory(path, Path::new("sample"), &seal, &mut pages).is_err());
        seal.source_sha256 = "source-a".into();
        read_series_directory(path, Path::new("sample"), &seal, &mut pages).unwrap();
        assert!(read_series_directory(path, Path::new("missing"), &seal, &mut pages).is_err());
        std::fs::write(path.join("sample.json"), b"corrupt").unwrap();
        assert!(read_series_directory(path, Path::new("sample"), &seal, &mut pages).is_err());
        let sidecar = Arc::new(Sidecar {
            source_identity: "source-a".into(),
            source_index_crc32: 0,
            data_path: path.join("columns.bin"),
            data_file: Mutex::new(Weak::new()),
            groups: vec![],
            pages_path: path.join("index.pages"),
            page_intervals: PageInterval::build(std::slice::from_ref(&pages)),
            pages: vec![pages],
            group_rows: vec![8],
            data_bytes: 0,
            weight: 0,
        });
        let keys = [decode_series_prefix(&key(2)).unwrap()];
        let file = PreparedSeriesFile {
            sidecar: sidecar.clone(),
            native_file: OnceLock::new(),
        };
        assert!(file.matches_source("source-a", 0));
        assert!(!file.matches_source("source-a", 123));
        assert!(!file.matches_source("source-b", 0));
        let exact = file.select_address(0, 1, 0, &keys[0]).unwrap();
        assert_eq!(exact.block().row_start, 4);
        assert!(file.select_address(0, 0, 0, &keys[0]).is_err());
        assert!(file.select_address(1, 0, 0, &keys[0]).is_err());
        assert!(file.select_address(0, 2, 0, &keys[0]).is_err());
        assert!(file.select_address(0, 0, 2, &keys[0]).is_err());
        let all = sidecar.select_blocks(0, None, None).unwrap().unwrap();
        let selected = sidecar
            .select_blocks(0, None, Some(&keys))
            .unwrap()
            .unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(
            selected
                .iter()
                .map(|b| (b.block().row_start, b.rows.clone()))
                .collect::<Vec<_>>(),
            all.iter()
                .filter(|b| b.primary_key() == keys[0])
                .map(|b| (b.block().row_start, b.rows.clone()))
                .collect::<Vec<_>>()
        );
        let selection = RowSelection::from(vec![
            RowSelector::skip(3),
            RowSelector::select(2),
            RowSelector::skip(3),
        ]);
        let selected = sidecar
            .select_blocks(0, Some(&selection), Some(&keys))
            .unwrap()
            .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|b| (b.block().row_start, b.rows.clone()))
                .collect::<Vec<_>>(),
            vec![(2, 1..2), (4, 0..1)]
        );
        std::fs::rename(path.join("columns.bin"), path.join("unavailable.bin")).unwrap();
        *sidecar.data_file.lock().unwrap() = Weak::new();
        let absent = [decode_series_prefix(&key(9)).unwrap()];
        assert!(
            sidecar
                .select_blocks(0, None, Some(&absent))
                .unwrap()
                .unwrap()
                .is_empty()
        );
        assert!(
            sidecar
                .select_blocks(0, None, Some(&[]))
                .unwrap()
                .unwrap()
                .is_empty()
        );
        assert!(sidecar.select_blocks(0, None, Some(&keys)).is_err());
        assert!(decode_series_prefix("00").is_err());
        assert!(decode_series_prefix(&"00".repeat(22)).is_err());
    }

    #[test]
    fn page_interval_lookup_retains_nested_and_unordered_overlaps() {
        let prefix = |value: u8| {
            let mut key = [0; 22];
            key[21] = value;
            key
        };
        let bounds = [(8, 11), (1, 14), (3, 4), (8, 8), (15, 16), (1, 2)];
        let pages: Vec<_> = bounds
            .iter()
            .enumerate()
            .map(|(group, &(min, max))| {
                vec![Page {
                    row_group: group,
                    row_start: 0,
                    count: 1,
                    blocks: 1,
                    offset: 0,
                    length: 0,
                    raw_length: 0,
                    crc32: 0,
                    series_bounds: Some((prefix(min), prefix(max))),
                }]
            })
            .collect();
        let intervals = PageInterval::build(&pages);
        for first in 0..=18 {
            for last in first..=18 {
                let keys = [prefix(first), prefix(last)];
                let mut actual: Vec<_> = PageInterval::candidates(&intervals, &keys)
                    .iter()
                    .filter(|entry| {
                        let (min, max) = pages[entry.group][entry.page].series_bounds.unwrap();
                        keys.iter().any(|key| min <= *key && *key <= max)
                    })
                    .map(|entry| entry.group)
                    .collect();
                actual.sort_unstable();
                let expected: Vec<_> = bounds
                    .iter()
                    .enumerate()
                    .filter(|(_, (min, max))| {
                        [first, last].iter().any(|key| min <= key && key <= max)
                    })
                    .map(|(index, _)| index)
                    .collect();
                assert_eq!(actual, expected, "selected keys {first}, {last}");
            }
        }
        assert!(PageInterval::candidates(&intervals, &[]).is_empty());
    }

    #[test]
    fn file_series_selection_preserves_overlapping_group_key_ranges() {
        let temp = common_test_util::temp_dir::create_temp_dir("file-series-selection");
        let path = temp.path();
        let key = |id: u64| format!("8000000401000000018000000301{id:016x}");
        let mut bytes = Vec::new();
        let mut pages = Vec::new();
        for (group, ids) in [[2, 3], [1, 2]].into_iter().enumerate() {
            let blocks: Vec<_> = ids
                .iter()
                .enumerate()
                .map(|(row, id)| {
                    let columns: serde_json::Map<String, serde_json::Value> = [
                        ("timestamp", "<i8"),
                        ("value_bits", "<u8"),
                        ("sequence", "<u8"),
                        ("valid", "|u1"),
                        ("op", "|u1"),
                    ]
                    .into_iter()
                    .map(|(name, dtype)| {
                        (
                            name.into(),
                            serde_json::json!({
                                "dtype": dtype, "count": 1, "crc32": 0, "mode": "constant",
                                "first": "1", "offset": 0, "length": 0,
                            }),
                        )
                    })
                    .collect();
                    serde_json::json!({"row_group": group, "row_start": row,
                    "count": 1, "key_hex": key(*id), "columns": columns})
                })
                .collect();
            let raw = serde_json::to_vec(&blocks).unwrap();
            let compressed = zstd::bulk::compress(&raw, 1).unwrap();
            pages.push(vec![Page {
                row_group: group,
                row_start: 0,
                count: 2,
                blocks: 2,
                offset: bytes.len() as u64,
                length: compressed.len(),
                raw_length: raw.len(),
                crc32: crc32fast::hash(&raw),
                series_bounds: Some((
                    decode_series_prefix(&key(ids[0])).unwrap(),
                    decode_series_prefix(&key(ids[1])).unwrap(),
                )),
            }]);
            bytes.extend(compressed);
        }
        std::fs::write(path.join("pages"), bytes).unwrap();
        std::fs::write(path.join("data"), []).unwrap();
        let sidecar = Arc::new(Sidecar {
            source_identity: "overlapping-file-series-test".into(),
            source_index_crc32: 0,
            data_path: path.join("data"),
            data_file: Mutex::new(Weak::new()),
            groups: vec![],
            pages_path: path.join("pages"),
            page_intervals: PageInterval::build(&pages),
            pages,
            group_rows: vec![2, 2],
            data_bytes: 0,
            weight: 0,
        });
        let prepared = PreparedSeriesFile {
            sidecar,
            native_file: OnceLock::new(),
        };
        let select = |group, ids: &[u64]| {
            prepared
                .selection(
                    group,
                    Arc::new(
                        ids.iter()
                            .map(|id| decode_series_prefix(&key(*id)).unwrap())
                            .collect(),
                    ),
                )
                .select()
                .unwrap()
                .unwrap()
        };
        let positions = |blocks: Vec<SelectedSeriesBlock>| {
            let mut result = blocks
                .into_iter()
                .map(|b| (b.block().row_group, b.block().row_start, b.rows.clone()))
                .collect::<Vec<_>>();
            result.sort_unstable_by_key(|entry| (entry.0, entry.1));
            result
        };
        assert_eq!(
            positions(select(None, &[2])),
            vec![(0, 0, 0..1), (1, 1, 0..1)]
        );
        assert_eq!(positions(select(Some(1), &[2])), vec![(1, 1, 0..1)]);
        assert_eq!(
            positions(select(None, &[1, 3])),
            vec![(0, 1, 0..1), (1, 0, 0..1)]
        );
        assert!(select(None, &[4]).is_empty());
        assert!(select(None, &[]).is_empty());
    }

    #[test]
    fn test_sidecar_shared_positional_reads() {
        let directory = common_test_util::temp_dir::create_temp_dir("sidecar-pread");
        let path = directory.path().join("payload");
        let expected: Vec<u8> = (0..8192).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &expected).unwrap();
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(7)).unwrap();
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let file = &file;
                let expected = &expected;
                scope.spawn(move || {
                    for iteration in 0..128 {
                        let offset = (worker * 877 + iteration * 37) % (8192 - 128);
                        let mut bytes = [0; 128];
                        read_payload_at(file, &mut bytes, offset as u64).unwrap();
                        assert_eq!(&bytes, &expected[offset..offset + 128]);
                    }
                });
            }
        });
        #[cfg(unix)]
        assert_eq!(file.stream_position().unwrap(), 7);
        assert_eq!(
            read_payload_at(&file, &mut [0; 2], 8191)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::UnexpectedEof,
        );
    }

    fn column(raw: &[u8], dtype: &str, mode: &str) -> Column {
        Column {
            dtype: dtype.to_string(),
            count: raw.len() / if dtype == "|u1" { 1 } else { 8 },
            crc32: crc32fast::hash(raw),
            mode: mode.to_string(),
            first: None,
            step: None,
            offset: 0,
            length: 0,
        }
    }

    #[test]
    fn test_max_summary_reads_match_raw_windows_and_reject_identity() {
        let temp = common_test_util::temp_dir::create_temp_dir("max-summary");
        let source = temp.path().join("source").join("sst");
        let root = temp.path().join("summaries");
        let summary = root.join("sst");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&summary).unwrap();
        let bits: Vec<u64> = [
            0,
            1 << 63,
            0x7ff0000000000002,
            0x7ff8000000000001,
            0x7ff8000000000007,
            0x3ff0000000000000,
            0x7ff0000000000000,
        ]
        .into_iter()
        .cycle()
        .take(35)
        .collect();
        let max = |bits: &[u64]| -> Option<u64> {
            let mut result: Option<f64> = None;
            for &bits in bits {
                let value = f64::from_bits(bits);
                if bits != 0x7ff0000000000002 && result.is_none_or(|v| v.is_nan() || value > v) {
                    result = Some(value);
                }
            }
            result.map(f64::to_bits)
        };
        let raw: Vec<_> = bits.iter().flat_map(|v| v.to_le_bytes()).collect();
        let times: Vec<_> = (0..bits.len())
            .flat_map(|i| ((i * 30000) as i64).to_le_bytes())
            .collect();
        std::fs::write(source.join("columns.bin"), &raw).unwrap();
        std::fs::write(source.join("index.json"), b"source index").unwrap();
        let mut data = Vec::new();
        let mut columns = serde_json::Map::new();
        for (name, values, dtype) in [
            ("prefix", raw[..72].to_vec(), "<u8"),
            ("suffix", raw[raw.len() - 72..].to_vec(), "<u8"),
            (
                "summary",
                bits.windows(10)
                    .flat_map(|v| max(v).unwrap_or(0).to_le_bytes())
                    .collect(),
                "<u8",
            ),
            (
                "summary_valid",
                bits.windows(10)
                    .map(|v| u8::from(max(v).is_some()))
                    .collect(),
                "|u1",
            ),
        ] {
            columns.insert(
                name.into(),
                serde_json::json!({"dtype": dtype,
                "count": values.len() / if dtype == "|u1" {1} else {8},
                "crc32": crc32fast::hash(&values), "mode": "raw",
                "offset": data.len(), "length": values.len()}),
            );
            data.extend(values);
        }
        std::fs::write(summary.join("summary.bin"), &data).unwrap();
        let index = serde_json::to_vec(&serde_json::json!({"version":1, "window_ms":300000,
            "source_sha256":"identity", "source_index_crc32":crc32fast::hash(b"source index"),
            "data_bytes":data.len(), "blocks":[{"row_group":0,"row_start":0,"count":bits.len(),
                "first_timestamp":0,"timestamp_step":30000,"columns":columns}]}))
        .unwrap();
        std::fs::write(summary.join("index.json"), &index).unwrap();
        std::fs::write(
            summary.join("validation.json"),
            serde_json::to_vec(&serde_json::json!({
            "status":"pass", "index_crc32":crc32fast::hash(&index)}))
            .unwrap(),
        )
        .unwrap();
        let mut timestamp = column(&times, "<i8", "linear");
        timestamp.first = Some("0".into());
        timestamp.step = Some("30000".into());
        let mut ones = column(&vec![1; bits.len()], "|u1", "constant");
        ones.first = Some("1".into());
        let make = |identity: &str| SelectedSeriesBlock {
            _sidecar: Arc::new(Sidecar {
                source_identity: identity.into(),
                source_index_crc32: 0,
                data_path: source.join("columns.bin"),
                data_file: Mutex::new(Weak::new()),
                groups: vec![],
                pages_path: PathBuf::new(),
                pages: vec![],
                page_intervals: Vec::new(),
                group_rows: vec![bits.len()],
                data_bytes: raw.len() as u64,
                weight: 0,
            }),
            data_file: Arc::new(File::open(source.join("columns.bin")).unwrap()),
            page: Arc::new(DirectoryPage {
                blocks: vec![Block {
                    row_group: 0,
                    row_start: 0,
                    count: bits.len(),
                    key_hex: String::new(),
                    key: vec![],
                    native_values: OnceLock::new(),
                    columns: HashMap::from([
                        ("timestamp".into(), timestamp.clone()),
                        ("valid".into(), ones.clone()),
                        ("op".into(), ones.clone()),
                    ]),
                }],
                weight: 0,
            }),
            block_index: 0,
            rows: 0..bits.len(),
        };
        for phase in [0, 1, 15000, 29999] {
            for offset in [-15000, 0, 15000] {
                let request = store_api::region_engine::RangeMaxRequest {
                    instants: (0..5).map(|i| i * 300000 + phase).collect(),
                    window: 300000,
                    offset,
                };
                let reduced = make("identity")
                    .read_range_max_checked(&request, 0..bits.len(), &root)
                    .unwrap()
                    .unwrap();
                let samples: Vec<_> = reduced
                    .iter()
                    .flat_map(|b| {
                        b.values.iter_values().enumerate().map(|(i, v)| {
                            (
                                i128::from(b.first_timestamp) + i as i128 * b.timestamp_step,
                                v.to_bits(),
                            )
                        })
                    })
                    .collect();
                for end in &request.instants {
                    let end = i128::from(*end) - i128::from(offset);
                    let expected: Vec<_> = bits
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| {
                            (*i as i128 * 30000) > end - 300000 && (*i as i128 * 30000) <= end
                        })
                        .map(|(_, v)| *v)
                        .collect();
                    let actual: Vec<_> = samples
                        .iter()
                        .filter(|(t, _)| *t > end - 300000 && *t <= end)
                        .map(|(_, v)| *v)
                        .collect();
                    assert_eq!(max(&actual), max(&expected));
                }
                assert!(
                    make("wrong identity")
                        .read_range_max_checked(&request, 0..bits.len(), &root)
                        .is_err()
                );
            }
        }
        let request = store_api::region_engine::RangeMaxRequest {
            instants: vec![600000],
            window: 300000,
            offset: 0,
        };
        assert!(
            make("identity")
                .read_range_max_checked(&request, 15..20, &root)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn native_payload_is_lazy_retryable_and_scoped_to_the_scan() {
        let directory = common_test_util::temp_dir::create_temp_dir("lazy-native-payload");
        let path = directory.path().join("values.bin");
        let sidecar = Arc::new(Sidecar {
            source_identity: "source".into(),
            source_index_crc32: 0,
            data_path: path.clone(),
            data_file: Mutex::new(Weak::new()),
            groups: vec![],
            pages_path: PathBuf::new(),
            pages: vec![],
            page_intervals: vec![],
            group_rows: vec![],
            data_bytes: 8,
            weight: 0,
        });
        let prepared = PreparedSeriesFile {
            sidecar: sidecar.clone(),
            native_file: OnceLock::new(),
        };
        assert!(prepared.matches_source("source", 0));
        assert!(prepared.native_values().is_err());
        std::fs::write(&path, 42_u64.to_le_bytes()).unwrap();
        let first = prepared.native_values().unwrap();
        let second = prepared.native_values().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let weak = Arc::downgrade(&first.data_file);
        drop((first, second));
        assert!(weak.upgrade().is_some());
        drop(prepared);
        assert!(weak.upgrade().is_none());
        assert!(sidecar.data_file.lock().unwrap().upgrade().is_none());
    }

    #[test]
    fn test_sidecar_value_cache_selection_identity_and_corruption() {
        let directory = common_test_util::temp_dir::create_temp_dir("sidecar-values-cache");
        let path = directory.path().join("values.bin");
        let values = [0u64, 1 << 63, 0x7ff0000000000002, u64::MAX];
        let raw: Vec<_> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        std::fs::write(&path, &raw).unwrap();
        let mut metadata = column(&raw, "<u8", "raw");
        metadata.length = raw.len();
        let make_block = |identity: &str, column: Column, rows: Range<usize>| SelectedSeriesBlock {
            _sidecar: Arc::new(Sidecar {
                source_identity: identity.into(),
                source_index_crc32: 0,
                data_path: path.clone(),
                data_file: Mutex::new(Weak::new()),
                groups: Vec::new(),
                pages_path: PathBuf::new(),
                pages: Vec::new(),
                page_intervals: Vec::new(),
                group_rows: vec![values.len()],
                data_bytes: raw.len() as u64,
                weight: 0,
            }),
            data_file: Arc::new(File::open(&path).unwrap()),
            page: Arc::new(DirectoryPage {
                blocks: vec![Block {
                    row_group: 0,
                    row_start: 0,
                    count: values.len(),
                    key_hex: String::new(),
                    key: vec![],
                    native_values: OnceLock::new(),
                    columns: HashMap::from([("value_bits".into(), column)]),
                }],
                weight: 0,
            }),
            block_index: 0,
            rows,
        };
        let cache = value_cache(4096);
        let first = make_block("first-sst", metadata.clone(), 0..2);
        let native_file = PreparedSeriesFile {
            sidecar: first._sidecar.clone(),
            native_file: OnceLock::new(),
        }
        .native_values()
        .unwrap();
        let native_column =
            NativeValueColumn::new(values.len(), 2, crc32fast::hash(&raw), 0, raw.len(), 0, 0)
                .unwrap();
        assert_eq!(
            materialized_bits(native_file.read(&native_column, false).unwrap()).as_ref(),
            values.as_slice()
        );
        let selected = crate::read::research_series_native::SelectedNativeBlock {
            record: Arc::new(crate::read::research_series_native::NativeRecord {
                key: Vec::new(),
                tags: Vec::new(),
                blocks: vec![crate::read::research_series_native::NativeBlock {
                    file: 0,
                    count: 2,
                    first: 10,
                    last: 20,
                    step: 10,
                    value_offset: 1,
                    values: native_column,
                }],
                weight: 0,
            }),
            index: 0,
            file: native_file.clone(),
        };
        for native in [false, true] {
            assert_eq!(
                materialized_bits(selected.read_values(native).unwrap()).as_ref(),
                &values[1..3]
            );
        }
        let invalid_extent = NativeValueColumn::new(
            values.len(),
            2,
            crc32fast::hash(&raw),
            u64::MAX,
            raw.len(),
            0,
            0,
        )
        .unwrap();
        assert!(native_file.validate(&invalid_extent).is_err());
        let invalid_crc = NativeValueColumn::new(
            values.len(),
            2,
            crc32fast::hash(&raw) ^ 1,
            0,
            raw.len(),
            0,
            0,
        )
        .unwrap();
        assert!(native_file.read(&invalid_crc, false).is_err());
        assert_eq!(
            first
                .read_value_bits_with_cache(Some(&cache))
                .unwrap()
                .as_ref(),
            &values[..2]
        );
        let second = make_block("first-sst", metadata.clone(), 2..4);
        assert_eq!(
            second
                .read_value_bits_with_cache(Some(&cache))
                .unwrap()
                .as_ref(),
            &values[2..]
        );
        cache.run_pending_tasks();
        assert_eq!(cache.entry_count(), 1);

        let retained = second.read_value_bits_with_cache(Some(&cache)).unwrap();
        let full = make_block("first-sst", metadata.clone(), 0..4)
            .read_value_bits_with_cache(Some(&cache))
            .unwrap();
        assert_eq!(retained.as_ptr(), full[2..].as_ptr());
        let floats =
            Float64Array::new(ScalarBuffer::new(retained.clone().into_inner(), 0, 2), None);
        assert_eq!(floats.value(0).to_bits(), values[2]);
        assert_eq!(floats.value(1).to_bits(), values[3]);

        let mut corrupt = metadata.clone();
        corrupt.crc32 ^= 1;
        assert!(
            make_block("first-sst", corrupt, 0..4)
                .read_value_bits_with_cache(Some(&cache))
                .is_err()
        );
        cache.run_pending_tasks();
        assert_eq!(cache.entry_count(), 1);

        // A replacement source cannot reuse a previous source's validated block.
        std::fs::write(&path, [0; 32]).unwrap();
        assert!(
            make_block("replacement-sst", metadata.clone(), 0..4)
                .read_value_bits_with_cache(Some(&cache))
                .is_err()
        );
        cache.invalidate_all();
        cache.run_pending_tasks();
        assert_eq!(retained.as_ref(), &values[2..]);
        assert!(first.read_value_bits_with_cache(Some(&cache)).is_err());

        std::fs::write(&path, &raw).unwrap();
        let tiny = value_cache(1);
        assert_eq!(
            second
                .read_value_bits_with_cache(Some(&tiny))
                .unwrap()
                .as_ref(),
            &values[2..]
        );
        tiny.run_pending_tasks();
        assert_eq!(tiny.weighted_size(), 0);

        let affine: Vec<_> = [10.0f64, 20.0, 30.0, 40.0]
            .iter()
            .flat_map(|v| v.to_bits().to_le_bytes())
            .collect();
        std::fs::write(&path, &affine).unwrap();
        let mut metadata = column(&affine, "<u8", "raw");
        metadata.length = affine.len();
        let mut selected = make_block("affine-sst", metadata.clone(), 1..4);
        let native = selected
            .read_visible_values_with_cache(Some(&cache), true)
            .unwrap();
        assert!(matches!(native, VisibleSeriesValues::Integer(_)));
        assert_eq!(
            native.iter_values().collect::<Vec<_>>(),
            vec![20.0, 30.0, 40.0]
        );
        let dense = selected
            .read_visible_values_with_cache(Some(&cache), false)
            .unwrap();
        assert!(matches!(dense, VisibleSeriesValues::Dense(_)));
        assert_eq!(
            dense.iter_values().collect::<Vec<_>>(),
            vec![20.0, 30.0, 40.0]
        );
        cache.run_pending_tasks();
        assert_eq!(cache.entry_count(), 2);
        metadata.crc32 ^= 1;
        assert!(
            make_block("affine-sst", metadata, 0..4)
                .read_visible_values_with_cache(Some(&cache), true)
                .is_err()
        );
        cache.invalidate_all();
        cache.run_pending_tasks();
        std::fs::write(&path, [0; 32]).unwrap();
        selected.rows = 0..2;
        assert_eq!(
            selected
                .read_visible_values_with_cache(Some(&cache), true)
                .unwrap()
                .iter_values()
                .collect::<Vec<_>>(),
            vec![10.0, 20.0]
        );
        assert!(
            selected
                .read_visible_values_with_cache(Some(&cache), false)
                .is_err()
        );
        assert!(selected.read_visible_values_with_cache(None, true).is_err());
    }

    #[test]
    fn test_sidecar_regular_put_proof() {
        let mut timestamp = column(&[0; 24], "<i8", "linear");
        timestamp.first = Some("-10".into());
        timestamp.step = Some("10".into());
        let one = || {
            let mut result = column(&[1; 3], "|u1", "constant");
            result.first = Some("1".into());
            result
        };
        let mut block = Block {
            row_group: 0,
            row_start: 0,
            count: 3,
            key_hex: String::new(),
            native_values: OnceLock::new(),
            key: vec![7],
            columns: HashMap::from([
                ("timestamp".into(), timestamp),
                ("valid".into(), one()),
                ("op".into(), one()),
            ]),
        };
        assert_eq!(regular_put_range(&block, 1..3).unwrap(), Some((0, 10, 10)));
        assert!(regular_put_range(&block, 0..0).is_err());
        assert!(regular_put_range(&block, 0..4).is_err());
        for name in ["op", "valid"] {
            block.columns.get_mut(name).unwrap().first = Some("0".into());
            assert_eq!(regular_put_range(&block, 0..3).unwrap(), None);
            block.columns.get_mut(name).unwrap().first = Some("1".into());
        }
        for step in ["0", "-1"] {
            block.columns.get_mut("timestamp").unwrap().step = Some(step.into());
            assert_eq!(regular_put_range(&block, 0..3).unwrap(), None);
        }
        let time = block.columns.get_mut("timestamp").unwrap();
        time.first = Some(i64::MAX.to_string());
        time.step = Some("1".into());
        // Even a valid selected first row must not certify an overflowing block.
        assert!(regular_put_range(&block, 0..1).is_err());
        let time = block.columns.get_mut("timestamp").unwrap();
        time.first = Some(i64::MIN.to_string());
        time.step = Some(u64::MAX.to_string());
        block.count = 2;
        for column in block.columns.values_mut() {
            column.count = 2;
        }
        assert_eq!(
            regular_put_range(&block, 0..2).unwrap(),
            Some((i64::MIN, i64::MAX, u64::MAX as i128)),
        );
    }

    #[test]
    fn test_sidecar_transformed_bits_and_corruption() {
        let values: Vec<u64> = vec![
            0,
            1 << 63,
            0x7ff0000000000002,
            0x7ff8000000001234,
            u64::MAX,
            1,
            3,
            u64::MAX - 1,
        ];
        let raw: Vec<_> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        for mode in ["delta-zstd", "xor-zstd"] {
            let mut shuffled = vec![0; raw.len()];
            for (i, value) in values.iter().enumerate() {
                let previous = if i == 0 { 0 } else { values[i - 1] };
                let encoded = if mode == "delta-zstd" {
                    value.wrapping_sub(previous)
                } else {
                    value ^ previous
                };
                for (j, byte) in encoded.to_le_bytes().into_iter().enumerate() {
                    shuffled[j * values.len() + i] = byte;
                }
            }
            let payload = zstd::bulk::compress(&shuffled, 1).unwrap();
            let native = NativeValueColumn::new(
                values.len(),
                if mode == "delta-zstd" { 4 } else { 5 },
                crc32fast::hash(&raw),
                0,
                payload.len(),
                0,
                0,
            )
            .unwrap();
            assert_eq!(decode(&native.column, &payload).unwrap(), raw);
            let mut metadata = column(&raw, "<u8", mode);
            assert_eq!(decode(&metadata, &payload).unwrap(), raw);
            assert!(decode(&metadata, &payload[..payload.len() - 1]).is_err());
            metadata.crc32 ^= 1;
            assert!(decode(&metadata, &payload).is_err());
        }
    }

    #[test]
    fn test_sidecar_arithmetic_bounds() {
        let raw: Vec<_> = [u64::MAX, u64::MAX - 1, u64::MAX - 2]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let mut metadata = column(&raw, "<u8", "linear");
        metadata.first = Some(u64::MAX.to_string());
        metadata.step = Some("-1".to_string());
        let native =
            NativeValueColumn::new(3, 1, crc32fast::hash(&raw), 0, 0, u64::MAX, -1).unwrap();
        assert_eq!(decode(&native.column, &[]).unwrap(), raw);
        assert!(NativeValueColumn::new(3, 1, crc32fast::hash(&raw), 0, 0, u64::MAX, 1).is_err());
        assert_eq!(decode(&metadata, &[]).unwrap(), raw);
        metadata.step = Some("1".to_string());
        assert!(decode(&metadata, &[]).is_err());
        metadata.step = Some(i128::MAX.to_string());
        assert!(decode(&metadata, &[]).is_err());
        metadata.mode = "unknown".to_string();
        assert!(decode(&metadata, &raw).is_err());
        let metadata = column(&[1, 0, 1], "|u1", "raw");
        assert_eq!(decode(&metadata, &[1, 0, 1]).unwrap(), vec![1, 0, 1]);
        assert!(decode(&metadata, &[1, 1, 1]).is_err());
    }

    #[test]
    fn test_sidecar_absolute_selection() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(3),
            RowSelector::select(7),
            RowSelector::skip(2),
            RowSelector::select(1),
        ]);
        assert_eq!(
            selected_ranges(Some(&selection), 15).unwrap(),
            vec![3..10, 12..13]
        );
        assert!(selected_ranges(Some(&selection), 12).is_err());
        assert!(
            selected_ranges(Some(&RowSelection::from(vec![RowSelector::skip(15)])), 15)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    #[ignore = "Requires the frozen real-SST experiment files"]
    fn test_sidecar_real_sst_parquet_differential() {
        let source = PathBuf::from(std::env::var_os("GT_TEST_SIDECAR_SOURCE").expect("source"));
        let directory =
            PathBuf::from(std::env::var_os("GT_TEST_SIDECAR_DIRECTORY").expect("directory"));
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&source).unwrap()).unwrap();
        let schema = builder.schema().clone();
        let metadata = builder.metadata().clone();
        let sidecar = Arc::new(Sidecar::open(&directory, &source, &metadata).unwrap());
        {
            let first = sidecar.open_data_file().unwrap();
            let second = sidecar.open_data_file().unwrap();
            assert!(Arc::ptr_eq(&first, &second));
        }
        assert!(sidecar.data_file.lock().unwrap().upgrade().is_none());
        for group in [
            0,
            1,
            metadata.num_row_groups() / 2,
            metadata.num_row_groups() - 1,
        ] {
            let rows = metadata.row_group(group).num_rows() as usize;
            let sparse = RowSelection::from(
                (0..rows)
                    .map(|i| {
                        if i % 97 < 7 {
                            RowSelector::select(1)
                        } else {
                            RowSelector::skip(1)
                        }
                    })
                    .collect::<Vec<_>>(),
            );
            for selection in [
                None,
                Some(sparse),
                Some(RowSelection::from(vec![
                    RowSelector::skip(rows - 1),
                    RowSelector::select(1),
                ])),
            ] {
                for indices in [vec![0, 1, 2, 3, 4], vec![2], vec![0, 1, 3, 4]] {
                    let expected =
                        ParquetRecordBatchReaderBuilder::try_new(File::open(&source).unwrap())
                            .unwrap()
                            .with_row_groups(vec![group]);
                    let expected = if let Some(selection) = &selection {
                        expected.with_row_selection(selection.clone())
                    } else {
                        expected
                    };
                    let expected = expected
                        .build()
                        .unwrap()
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .unwrap();
                    let expected = concat_batches(&schema, &expected)
                        .unwrap()
                        .project(&indices)
                        .unwrap();
                    if indices == [0, 1, 2, 3, 4]
                        && let Some(blocks) = sidecar
                            .select_blocks(group, selection.as_ref(), None)
                            .unwrap()
                    {
                        let expected_values = expected
                            .column(0)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap();
                        let expected_times = expected
                            .column(1)
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        let keys = cast(expected.column(2), &DataType::Binary).unwrap();
                        let keys = keys.as_any().downcast_ref::<BinaryArray>().unwrap();
                        let mut row = 0;
                        for block in blocks {
                            let values = block.read_value_bits().unwrap();
                            assert_eq!(values.len(), block.num_rows());
                            let regular = block.regular_put_range().unwrap();
                            for (offset, bits) in values.iter().copied().enumerate() {
                                assert_eq!(block.primary_key(), keys.value(row));
                                if !expected_values.is_null(row) {
                                    assert_eq!(bits, expected_values.value(row).to_bits());
                                }
                                if let Some((first, last, step)) = regular {
                                    assert_eq!(
                                        first as i128 + offset as i128 * step,
                                        expected_times.value(row) as i128,
                                    );
                                    assert!(expected_times.value(row) <= last);
                                }
                                row += 1;
                            }
                        }
                        assert_eq!(row, expected.num_rows());
                    }
                    let projected = expected.schema();
                    let actual = sidecar
                        .read_group(group, projected.clone(), selection.clone(), None)
                        .unwrap();
                    assert!(
                        actual
                            .iter()
                            .all(|b| b.num_rows() <= DEFAULT_READ_BATCH_SIZE)
                    );
                    let actual = concat_batches(&projected, &actual).unwrap();
                    assert_eq!(actual.num_rows(), expected.num_rows());
                    for (a, b) in actual.columns().iter().zip(expected.columns()) {
                        if a.data_type() == &DataType::Float64 {
                            let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
                            let b = b.as_any().downcast_ref::<Float64Array>().unwrap();
                            for i in 0..a.len() {
                                assert_eq!(a.is_null(i), b.is_null(i));
                                if !a.is_null(i) {
                                    assert_eq!(a.value(i).to_bits(), b.value(i).to_bits());
                                }
                            }
                        } else if matches!(a.data_type(), DataType::Dictionary(_, _)) {
                            assert_eq!(
                                cast(a, &DataType::Binary).unwrap().to_data(),
                                cast(b, &DataType::Binary).unwrap().to_data()
                            );
                        } else {
                            assert_eq!(a.to_data(), b.to_data());
                        }
                    }
                }
            }
        }
    }
}
