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

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use datatypes::arrow::array::Float64Array;
use datatypes::value::Value;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, SparsePrimaryKeyCodec};
use moka::sync::Cache;
use serde::Deserialize;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::VisibleSeriesValues;

use crate::error::{Result, UnexpectedSnafu};
use crate::read::research_series_major::SourceFile;
use crate::series_index::MetricSeriesId;
use crate::sst::parquet::research_sidecar::read_payload_at;

const HEADER: usize = 16;
const ENTRY: usize = 44;
const MAX_INDEX: usize = 1024 * 1024 * 1024;
const MAX_RAW: usize = 1024 * 1024;
const CACHE_BYTES: u64 = 256 * 1024 * 1024;

type Checked<T> = std::result::Result<T, String>;

/// Ablation switch for measurements only: forces a full binary search per lookup.
static ABLATE_CURSOR: LazyLock<bool> =
    LazyLock::new(|| std::env::var_os("GT_RESEARCH_ABLATE_CHUNK_BINARY_SEARCH").is_some());

#[derive(Deserialize)]
struct Artifact {
    bytes: u64,
    crc32: u32,
}

#[derive(Deserialize)]
struct Counts {
    series: usize,
    chunks: usize,
    rows: usize,
}

#[derive(Deserialize)]
struct Manifest {
    version: u32,
    status: String,
    files: Vec<SourceFile>,
    counts: Counts,
    artifacts: HashMap<String, Artifact>,
}

pub(crate) struct CoalescedChunk {
    pub(crate) key: Vec<u8>,
    pub(crate) runs: Vec<(i64, i128, Range<usize>)>,
    pub(crate) values: VisibleSeriesValues,
    weight: usize,
}

pub(crate) struct CoalescedDirectory {
    version: u32,
    metadata: RegionMetadataRef,
    files: Vec<SourceFile>,
    index: Vec<u8>,
    samples: File,
    cache: Cache<usize, Arc<CoalescedChunk>>,
    codec: SparsePrimaryKeyCodec,
}

static DIRECTORIES: LazyLock<Cache<PathBuf, Arc<CoalescedDirectory>>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(2 * 1024 * 1024 * 1024)
        .weigher(|_, directory: &Arc<CoalescedDirectory>| {
            (CACHE_BYTES as usize + directory.index.len()) as u32
        })
        .build()
});

fn error(reason: impl std::fmt::Display) -> crate::error::Error {
    UnexpectedSnafu {
        reason: format!("Research coalesced series: {reason}"),
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

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

impl CoalescedDirectory {
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
            Ok((directory.metadata.as_ref() == metadata.as_ref()).then_some(directory))
        })
        .await
        .map_err(error)?
    }

    pub(crate) fn matches_sources(&self, sources: &[SourceFile]) -> bool {
        self.files.len() == sources.len()
            && self.files.iter().zip(sources).all(|(a, b)| {
                a.sst == b.sst
                    && a.source_sha256 == b.source_sha256
                    && a.source_index_crc32 == b.source_index_crc32
                    && a.blocks == b.blocks
            })
    }

    fn id(&self, ordinal: usize) -> (u32, u64) {
        let entry = self.entry(ordinal);
        (u32_at(entry, 0), u64_at(entry, 4))
    }

    fn entry(&self, ordinal: usize) -> &[u8] {
        &self.index[HEADER + ordinal * ENTRY..HEADER + (ordinal + 1) * ENTRY]
    }

    pub(crate) fn chunks(&self, id: &MetricSeriesId) -> Option<Range<usize>> {
        let count = (self.index.len() - HEADER) / ENTRY;
        let needle = (id.table_id, id.tsid);
        let bound = |inclusive: bool| {
            let (mut low, mut high) = (0, count);
            while low < high {
                let middle = low + (high - low) / 2;
                let value = self.id(middle);
                if value < needle || (inclusive && value == needle) {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            low
        };
        let start = bound(false);
        (start < count && self.id(start) == needle).then(|| start..bound(true))
    }

    /// Same result as `chunks`, searching forward from `cursor` and advancing it.
    /// Ascending lookups touch only nearby index entries instead of a full binary
    /// search over an index that can be hundreds of MiB; a lookup behind the cursor
    /// falls back to `chunks`.
    pub(crate) fn chunks_from(
        &self,
        id: &MetricSeriesId,
        cursor: &mut usize,
    ) -> Option<Range<usize>> {
        let count = (self.index.len() - HEADER) / ENTRY;
        let needle = (id.table_id, id.tsid);
        let from = (*cursor).min(count);
        if *ABLATE_CURSOR || (from > 0 && self.id(from - 1) >= needle) {
            let found = self.chunks(id);
            if let Some(range) = &found {
                *cursor = range.end;
            }
            return found;
        }
        // Gallop to a bound whose entry is not below the needle, then bisect.
        let (mut low, mut high, mut step) = (from, from, 1);
        while high < count && self.id(high) < needle {
            low = high + 1;
            high = high.saturating_add(step).min(count);
            step *= 2;
        }
        while low < high {
            let middle = low + (high - low) / 2;
            if self.id(middle) < needle {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let mut end = low;
        while end < count && self.id(end) == needle {
            end += 1;
        }
        *cursor = end;
        (end > low).then_some(low..end)
    }

    pub(crate) fn decode_bytes(&self, ordinal: usize) -> usize {
        let entry = self.entry(ordinal);
        // Covers compressed input, decoded arrays, keys, tags and worst-case runs.
        u32_at(entry, 20) as usize
            + u32_at(entry, 24) as usize * 8
            + self.metadata.primary_key.len() * 512
            + 4096
            // The non-native path materializes affine values after decoding.
            + if self.version == 2 {
                u32_at(entry, 32) as usize * size_of::<f64>()
            } else {
                0
            }
    }

    pub(crate) fn chunk(&self, ordinal: usize) -> Result<Arc<CoalescedChunk>> {
        self.cache
            .try_get_with(ordinal, || self.read_chunk(ordinal).map(Arc::new))
            .map_err(error)
    }

    /// Reads a chunk without inserting it. A cyclic scan larger than an LRU cache
    /// never hits, so inserting only pays for admission and eviction.
    pub(crate) fn chunk_uncached(&self, ordinal: usize) -> Result<Arc<CoalescedChunk>> {
        match self.cache.get(&ordinal) {
            Some(chunk) => Ok(chunk),
            None => self.read_chunk(ordinal).map(Arc::new).map_err(error),
        }
    }

    /// Whether the decoded chunks of `ids` fit in the chunk cache.
    pub(crate) fn fits_cache(&self, ids: &[MetricSeriesId]) -> bool {
        let (mut bytes, mut cursor) = (0u64, 0);
        for id in ids {
            for ordinal in self.chunks_from(id, &mut cursor).into_iter().flatten() {
                let entry = self.entry(ordinal);
                bytes += u64::from(u32_at(entry, 24)) + u64::from(u32_at(entry, 32)) * 8;
                if bytes > CACHE_BYTES {
                    return false;
                }
            }
        }
        true
    }

    pub(crate) fn column(&self, name: &str) -> Option<&store_api::metadata::ColumnMetadata> {
        self.metadata.column_by_name(name)
    }

    /// Resolves requested tag names to primary key columns; unknown names are dropped,
    /// so processors report them as missing exactly as with the full tag list.
    pub(crate) fn tag_columns(&self, names: &[String]) -> Vec<(String, u32)> {
        names
            .iter()
            .filter_map(|name| self.metadata.column_by_name(name))
            .filter(|column| column.semantic_type == api::v1::SemanticType::Tag)
            .map(|column| (column.column_schema.name.clone(), column.column_id))
            .collect()
    }

    /// Tags of a chunk key: the given columns (absent ones as null), or every
    /// primary key column.
    pub(crate) fn tags(
        &self,
        key: &[u8],
        columns: Option<&[(String, u32)]>,
    ) -> Result<Vec<(String, Value)>> {
        let Some(columns) = columns else {
            let CompositeValues::Sparse(decoded) = self.codec.decode(key).map_err(error)? else {
                return Err(error("Expected sparse primary key"));
            };
            return Ok(self
                .metadata
                .primary_key_columns()
                .map(|column| {
                    (
                        column.column_schema.name.clone(),
                        decoded.get_or_null(column.column_id).clone(),
                    )
                })
                .collect());
        };
        let ids = columns.iter().map(|(_, id)| *id).collect::<Vec<_>>();
        let decoded = self.codec.decode_projected(key, &ids).map_err(error)?;
        Ok(columns
            .iter()
            .map(|(name, id)| {
                let value = decoded
                    .iter()
                    .find(|(column, _)| column == id)
                    .map_or(Value::Null, |(_, value)| value.clone());
                (name.clone(), value)
            })
            .collect())
    }

    fn read(root: &Path, metadata: RegionMetadataRef) -> Checked<Self> {
        let manifest = File::open(root.join("manifest.json")).map_err(|e| e.to_string())?;
        check(
            manifest.metadata().map_err(|e| e.to_string())?.len() <= MAX_RAW as u64,
            "Manifest exceeds limit",
        )?;
        let manifest: Manifest = serde_json::from_reader(manifest).map_err(|e| e.to_string())?;
        check(
            matches!(manifest.version, 1 | 2) && manifest.status == "pass",
            "Unvalidated layout",
        )?;
        check(
            !manifest.files.is_empty()
                && manifest.files.len() <= 64
                && manifest.files.windows(2).all(|p| p[0].sst < p[1].sst),
            "Invalid source set",
        )?;
        let descriptor = manifest
            .artifacts
            .get("series.bin")
            .ok_or("Missing index")?;
        check(
            descriptor.bytes >= HEADER as u64 && descriptor.bytes <= MAX_INDEX as u64,
            "Index exceeds limit",
        )?;
        let mut index = Vec::new();
        File::open(root.join("series.bin"))
            .map_err(|e| e.to_string())?
            .take(descriptor.bytes + 1)
            .read_to_end(&mut index)
            .map_err(|e| e.to_string())?;
        check(
            index.len() as u64 == descriptor.bytes && crc32fast::hash(&index) == descriptor.crc32,
            "Index checksum mismatch",
        )?;
        let count = manifest.counts.chunks;
        check(
            count <= (MAX_INDEX - HEADER) / ENTRY
                && index.len() == HEADER + count * ENTRY
                && &index[..8]
                    == if manifest.version == 1 {
                        b"GTCSER01"
                    } else {
                        b"GTCSER02"
                    }
                && u64_at(&index, 8) == count as u64,
            "Invalid index header",
        )?;
        let samples = File::open(root.join("samples.bin")).map_err(|e| e.to_string())?;
        let size = manifest
            .artifacts
            .get("samples.bin")
            .ok_or("Missing samples")?
            .bytes;
        check(
            samples.metadata().map_err(|e| e.to_string())?.len() == size,
            "Samples length mismatch",
        )?;
        let mut header = [0; HEADER];
        read_payload_at(&samples, &mut header, 0).map_err(|e| e.to_string())?;
        check(
            &header[..8]
                == if manifest.version == 1 {
                    b"GTCDAT01"
                } else {
                    b"GTCDAT02"
                }
                && u64_at(&header, 8) == count as u64,
            "Invalid samples header",
        )?;
        let (mut next, mut rows, mut series) = (HEADER as u64, 0usize, 0usize);
        let mut previous = None;
        let valid_mask = u64::MAX >> (64 - manifest.files.len());
        for entry in index[HEADER..].chunks_exact(ENTRY) {
            let id = (u32_at(entry, 0), u64_at(entry, 4));
            let length = u32_at(entry, 20) as usize;
            let raw = u32_at(entry, 24) as usize;
            let count = u32_at(entry, 32) as usize;
            let mask = u64_at(entry, 36);
            check(
                previous.is_none_or(|p| p <= id) && u64_at(entry, 12) == next,
                "Unsorted or noncontiguous chunks",
            )?;
            check(
                count > 0
                    && count <= 8192
                    && raw
                        >= if manifest.version == 1 {
                            30 + count * 16
                        } else {
                            74
                        }
                    && raw <= MAX_RAW
                    && length > 0
                    && length <= raw + 4096
                    && mask != 0
                    && mask & !valid_mask == 0,
                "Invalid chunk bounds",
            )?;
            next = next
                .checked_add(length as u64)
                .ok_or("Payload extent overflow")?;
            rows = rows.checked_add(count).ok_or("Row count overflow")?;
            series += usize::from(previous != Some(id));
            previous = Some(id);
        }
        check(
            // The index holds ids, and distinct keys may share an id (tsid collisions),
            // so counting id changes can undercount the manifest's per-key series.
            next == size && rows == manifest.counts.rows && series <= manifest.counts.series,
            "Incomplete chunk coverage",
        )?;
        Ok(Self {
            version: manifest.version,
            codec: SparsePrimaryKeyCodec::new(&metadata),
            metadata,
            files: manifest.files,
            index,
            samples,
            cache: Cache::builder()
                .eviction_policy(moka::policy::EvictionPolicy::lru())
                .max_capacity(CACHE_BYTES)
                .weigher(|_, chunk: &Arc<CoalescedChunk>| chunk.weight as u32)
                .build(),
        })
    }

    fn read_chunk(&self, ordinal: usize) -> Checked<CoalescedChunk> {
        let entry = self.entry(ordinal);
        let mut compressed = vec![0; u32_at(entry, 20) as usize];
        read_payload_at(&self.samples, &mut compressed, u64_at(entry, 12))
            .map_err(|e| e.to_string())?;
        let size = u32_at(entry, 24) as usize;
        let raw = zstd::bulk::decompress(&compressed, size).map_err(|e| e.to_string())?;
        check(
            raw.len() == size && crc32fast::hash(&raw) == u32_at(entry, 28),
            "Chunk checksum mismatch",
        )?;
        let key_length = u32_at(&raw, 0) as usize;
        let rows = u32_at(&raw, 4) as usize;
        let header = if self.version == 1 { 8 } else { 16 };
        check(
            rows == u32_at(entry, 32) as usize
                && key_length >= 22
                && key_length
                    .checked_add(header)
                    .is_some_and(|end| end <= raw.len()),
            "Chunk payload bounds mismatch",
        )?;
        let key = raw[header..header + key_length].to_vec();
        check(
            self.codec.decode_ids(&key).map_err(|e| e.to_string())? == self.id(ordinal),
            "Chunk identity mismatch",
        )?;
        let (runs, values) = if self.version == 1 {
            check(
                key_length.checked_add(8 + rows * 16) == Some(raw.len()),
                "Chunk payload bounds mismatch",
            )?;
            let offset = 8 + key_length;
            let times = raw[offset..offset + rows * 8]
                .chunks_exact(8)
                .map(|bytes| i64::from_le_bytes(bytes.try_into().unwrap()))
                .collect::<Vec<_>>();
            check(
                times.windows(2).all(|p| p[0] < p[1]),
                "Unsorted chunk timestamps",
            )?;
            let mut runs = Vec::new();
            let mut start = 0;
            while start < rows {
                let step = if start + 1 < rows {
                    times[start + 1] as i128 - times[start] as i128
                } else {
                    0
                };
                let mut end = (start + 2).min(rows);
                while end < rows && times[end] as i128 - times[end - 1] as i128 == step {
                    end += 1;
                }
                runs.push((times[start], step, start..end));
                start = end;
            }
            let values = Float64Array::from_iter_values(
                raw[offset + rows * 8..]
                    .chunks_exact(8)
                    .map(|bytes| f64::from_bits(u64::from_le_bytes(bytes.try_into().unwrap()))),
            );
            let values = VisibleSeriesValues::compact(values);
            (runs, values)
        } else {
            let run_count = u32_at(&raw, 8) as usize;
            let mode = u32_at(&raw, 12);
            check(
                run_count > 0 && run_count <= rows && mode <= 1,
                "Invalid ready block header",
            )?;
            let offset = header + key_length;
            let value_offset = offset + run_count * 28;
            let value_bytes = if mode == 0 { rows * 8 } else { 16 };
            check(
                value_offset + value_bytes == raw.len(),
                "Invalid ready block extent",
            )?;
            let mut runs = Vec::with_capacity(run_count);
            let mut covered = 0;
            let mut previous = None;
            for run in raw[offset..value_offset].chunks_exact(28) {
                let first = i64::from_le_bytes(run[..8].try_into().unwrap());
                let step = i128::from_le_bytes(run[8..24].try_into().unwrap());
                let count = u32_at(run, 24) as usize;
                check(
                    count > 0 && count <= rows - covered && step >= 0 && (count == 1 || step > 0),
                    "Invalid ready timestamp run",
                )?;
                let last = step
                    .checked_mul((count - 1) as i128)
                    .and_then(|delta| i128::from(first).checked_add(delta))
                    .and_then(|last| i64::try_from(last).ok())
                    .ok_or("Ready timestamp overflow")?;
                check(
                    previous.is_none_or(|previous| previous < first),
                    "Overlapping ready timestamp runs",
                )?;
                previous = Some(last);
                runs.push((first, step, covered..covered + count));
                covered += count;
            }
            check(covered == rows, "Incomplete ready timestamp coverage")?;
            let values = if mode == 0 {
                VisibleSeriesValues::Dense(Float64Array::from_iter_values(
                    raw[value_offset..]
                        .chunks_exact(8)
                        .map(|bytes| f64::from_bits(u64::from_le_bytes(bytes.try_into().unwrap()))),
                ))
            } else {
                let first =
                    i64::from_le_bytes(raw[value_offset..value_offset + 8].try_into().unwrap());
                let step = i64::from_le_bytes(raw[value_offset + 8..].try_into().unwrap());
                VisibleSeriesValues::from_exact_integer(first, step, rows)
                    .ok_or("Inexact ready integer sequence")?
            };
            (runs, values)
        };
        let weight = key.capacity()
            + runs.capacity() * size_of::<(i64, i128, Range<usize>)>()
            + values.retained_bytes()
            + size_of::<CoalescedChunk>();
        Ok(CoalescedChunk {
            key,
            runs,
            values,
            weight,
        })
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::metric_engine_consts::{
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
    };
    use store_api::storage::RegionId;
    use store_api::storage::consts::ReservedColumnId;

    use super::*;

    fn fixture(root: &Path, times: &[i64], bits: &[u64]) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        for (name, data_type, id, semantic_type) in [
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
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                1,
                SemanticType::Timestamp,
            ),
        ] {
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(name, data_type, false),
                column_id: id,
                semantic_type,
            });
        }
        builder
            .primary_key(vec![ReservedColumnId::table_id(), ReservedColumnId::tsid()])
            .primary_key_encoding(PrimaryKeyEncoding::Sparse);
        let metadata = Arc::new(builder.build().unwrap());
        let mut key = Vec::new();
        SparsePrimaryKeyCodec::new(&metadata)
            .encode_internal(7, 9, &mut key)
            .unwrap();
        let header = |magic: &[u8; 8]| {
            let mut raw = magic.to_vec();
            raw.extend_from_slice(&2u64.to_le_bytes());
            raw
        };
        let mut index = header(b"GTCSER01");
        let mut data = header(b"GTCDAT01");
        // Two chunks for the same series exercise lower/upper-bound lookup.
        for (times, bits) in [(times, bits), (&[i64::MAX][..], &[0x8000000000000000][..])] {
            let mut raw = (key.len() as u32).to_le_bytes().to_vec();
            raw.extend_from_slice(&(times.len() as u32).to_le_bytes());
            raw.extend_from_slice(&key);
            for time in times {
                raw.extend_from_slice(&time.to_le_bytes());
            }
            for bits in bits {
                raw.extend_from_slice(&bits.to_le_bytes());
            }
            let compressed = zstd::bulk::compress(&raw, 1).unwrap();
            index.extend_from_slice(&7u32.to_le_bytes());
            index.extend_from_slice(&9u64.to_le_bytes());
            index.extend_from_slice(&(data.len() as u64).to_le_bytes());
            index.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
            index.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            index.extend_from_slice(&crc32fast::hash(&raw).to_le_bytes());
            index.extend_from_slice(&(times.len() as u32).to_le_bytes());
            index.extend_from_slice(&1u64.to_le_bytes());
            data.extend_from_slice(&compressed);
        }
        let manifest = serde_json::json!({"version":1,"status":"pass",
            "files":[{"sst":"a","source_sha256":"a","source_index_crc32":1,"blocks":2}],
            "counts":{"series":1,"chunks":2,"rows":times.len()+1},
            "artifacts":{"series.bin":{"bytes":index.len(),"crc32":crc32fast::hash(&index)},
                         "samples.bin":{"bytes":data.len(),"crc32":crc32fast::hash(&data)}}});
        std::fs::write(root.join("series.bin"), index).unwrap();
        std::fs::write(root.join("samples.bin"), data).unwrap();
        std::fs::write(
            root.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        metadata
    }

    fn ready_fixture(
        root: &Path,
        times: &[i64],
        bits: &[u64],
        integer: Option<(i64, i64)>,
        mutate: impl Fn(&mut Vec<u8>),
    ) -> RegionMetadataRef {
        let metadata = fixture(root, times, bits);
        let mut index = std::fs::read(root.join("series.bin")).unwrap();
        let old_data = std::fs::read(root.join("samples.bin")).unwrap();
        index[..8].copy_from_slice(b"GTCSER02");
        let mut data = old_data[..HEADER].to_vec();
        data[..8].copy_from_slice(b"GTCDAT02");
        for (ordinal, entry) in index[HEADER..].chunks_exact_mut(ENTRY).enumerate() {
            let start = u64_at(entry, 12) as usize;
            let end = start + u32_at(entry, 20) as usize;
            let old =
                zstd::bulk::decompress(&old_data[start..end], u32_at(entry, 24) as usize).unwrap();
            let key_len = u32_at(&old, 0) as usize;
            let rows = u32_at(&old, 4) as usize;
            let integer = if ordinal == 0 { integer } else { None };
            let mut raw = old[..8].to_vec();
            raw.extend_from_slice(&(rows as u32).to_le_bytes());
            raw.extend_from_slice(&u32::from(integer.is_some()).to_le_bytes());
            raw.extend_from_slice(&old[8..8 + key_len]);
            for timestamp in old[8 + key_len..8 + key_len + rows * 8].chunks_exact(8) {
                raw.extend_from_slice(timestamp);
                raw.extend_from_slice(&0i128.to_le_bytes());
                raw.extend_from_slice(&1u32.to_le_bytes());
            }
            if let Some((first, step)) = integer {
                raw.extend_from_slice(&first.to_le_bytes());
                raw.extend_from_slice(&step.to_le_bytes());
            } else {
                raw.extend_from_slice(&old[8 + key_len + rows * 8..]);
            }
            if ordinal == 0 {
                mutate(&mut raw);
            }
            let compressed = zstd::bulk::compress(&raw, 1).unwrap();
            entry[12..20].copy_from_slice(&(data.len() as u64).to_le_bytes());
            entry[20..24].copy_from_slice(&(compressed.len() as u32).to_le_bytes());
            entry[24..28].copy_from_slice(&(raw.len() as u32).to_le_bytes());
            entry[28..32].copy_from_slice(&crc32fast::hash(&raw).to_le_bytes());
            data.extend_from_slice(&compressed);
        }
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
        manifest["version"] = 2.into();
        for (name, bytes) in [("series.bin", &index), ("samples.bin", &data)] {
            manifest["artifacts"][name] =
                serde_json::json!({"bytes":bytes.len(),"crc32":crc32fast::hash(bytes)});
            std::fs::write(root.join(name), bytes).unwrap();
        }
        std::fs::write(
            root.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        metadata
    }

    #[test]
    fn ready_chunks_preserve_timestamp_gaps_special_bits_and_integer_values() {
        let times = [i64::MIN, -1, 0, i64::MAX - 1];
        for (bits, integer) in [
            (
                [
                    0x8000000000000000,
                    0x7ff0000000000002,
                    0x7ff8000000001234,
                    0x7ff0000000000000,
                ],
                None,
            ),
            (
                [
                    (-3.0f64).to_bits(),
                    (-1.0f64).to_bits(),
                    1.0f64.to_bits(),
                    3.0f64.to_bits(),
                ],
                Some((-3, 2)),
            ),
        ] {
            let root = common_test_util::temp_dir::create_temp_dir("ready-series");
            let metadata = ready_fixture(root.path(), &times, &bits, integer, |_| {});
            let directory = CoalescedDirectory::read(root.path(), metadata).unwrap();
            let chunk = directory.chunk(0).unwrap();
            assert_eq!(
                chunk
                    .values
                    .iter_values()
                    .map(f64::to_bits)
                    .collect::<Vec<_>>(),
                bits
            );
            let actual = chunk
                .runs
                .iter()
                .flat_map(|(first, step, range)| {
                    (0..range.len()).map(|i| (*first as i128 + *step * i as i128) as i64)
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, times);
            assert_eq!(
                directory.chunk(1).unwrap().values.value(0).to_bits(),
                0x8000000000000000
            );
        }
    }

    #[test]
    fn requested_tags_keep_absent_labels_as_null_and_uncached_reads_skip_the_cache() {
        let root = common_test_util::temp_dir::create_temp_dir("ready-tags");
        let fixture_metadata = ready_fixture(root.path(), &[0, 10], &[0; 2], None, |_| {});
        // Same identity columns plus a label the stored key does not carry.
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        for column in fixture_metadata
            .column_metadatas
            .iter()
            .cloned()
            .chain([ColumnMetadata {
                column_schema: ColumnSchema::new("pod", ConcreteDataType::string_datatype(), true),
                column_id: 2,
                semantic_type: SemanticType::Tag,
            }])
        {
            builder.push_column_metadata(column);
        }
        builder
            .primary_key(vec![
                ReservedColumnId::table_id(),
                ReservedColumnId::tsid(),
                2,
            ])
            .primary_key_encoding(PrimaryKeyEncoding::Sparse);
        let directory =
            CoalescedDirectory::read(root.path(), Arc::new(builder.build().unwrap())).unwrap();

        let key = directory.chunk_uncached(0).unwrap().key.clone();
        directory.cache.run_pending_tasks();
        assert_eq!(directory.cache.entry_count(), 0);
        let names = ["pod".to_string(), DATA_SCHEMA_TSID_COLUMN_NAME.to_string()];
        let columns = directory.tag_columns(&names);
        assert_eq!(
            directory.tags(&key, Some(&columns)).unwrap(),
            vec![
                ("pod".to_string(), Value::Null),
                (DATA_SCHEMA_TSID_COLUMN_NAME.to_string(), Value::UInt64(9)),
            ]
        );
        assert_eq!(directory.tags(&key, None).unwrap().len(), 3);
    }

    #[test]
    fn cursor_lookup_matches_binary_search_in_any_order() {
        let root = common_test_util::temp_dir::create_temp_dir("ready-cursor");
        let metadata = ready_fixture(root.path(), &[0, 10], &[0; 2], None, |_| {});
        let mut directory = CoalescedDirectory::read(root.path(), metadata).unwrap();
        // Replace the identities with a sorted multi-chunk layout; lookups read only the index.
        let ids = [(1u32, 5u64), (1, 5), (1, 9), (2, 1), (2, 1), (2, 1), (7, 0)];
        directory.index.truncate(HEADER);
        for (table, tsid) in ids {
            let mut entry = vec![0; ENTRY];
            entry[..4].copy_from_slice(&table.to_le_bytes());
            entry[4..12].copy_from_slice(&tsid.to_le_bytes());
            directory.index.extend(entry);
        }
        let probes = [
            (0, 0),
            (1, 5),
            (1, 6),
            (1, 9),
            (2, 1),
            (3, 0),
            (7, 0),
            (9, 9),
        ];
        let id = |(table_id, tsid): (u32, u64)| MetricSeriesId { table_id, tsid };
        for order in [
            probes.to_vec(),
            probes.iter().rev().copied().collect(),
            vec![(2, 1), (1, 5), (7, 0), (1, 9)],
        ] {
            let mut cursor = 0;
            for probe in order {
                assert_eq!(
                    directory.chunks_from(&id(probe), &mut cursor),
                    directory.chunks(&id(probe)),
                    "{probe:?}"
                );
            }
        }
    }

    #[test]
    fn ready_chunks_read_a_shared_timestamp_grid_without_materialization() {
        let root = common_test_util::temp_dir::create_temp_dir("ready-grid");
        let metadata = ready_fixture(
            root.path(),
            &[-30, -20, -10, 0],
            &[0; 4],
            Some((0, 0)),
            |raw| {
                let offset = 16 + u32_at(raw, 0) as usize;
                raw[8..12].copy_from_slice(&1u32.to_le_bytes());
                raw[offset + 8..offset + 24].copy_from_slice(&10i128.to_le_bytes());
                raw[offset + 24..offset + 28].copy_from_slice(&4u32.to_le_bytes());
                raw.drain(offset + 28..offset + 4 * 28);
            },
        );
        let directory = CoalescedDirectory::read(root.path(), metadata).unwrap();
        let chunk = directory.chunk(0).unwrap();
        assert_eq!(chunk.runs, vec![(-30, 10, 0..4)]);
        assert!(matches!(chunk.values, VisibleSeriesValues::Integer(_)));
        assert_eq!(
            chunk
                .values
                .iter_values()
                .map(f64::to_bits)
                .collect::<Vec<_>>(),
            vec![0; 4]
        );
    }

    #[test]
    fn ready_chunk_reservation_covers_dense_materialization() {
        use datatypes::arrow::array::Array;

        let root = common_test_util::temp_dir::create_temp_dir("ready-materialize");
        let times = (0..8192).collect::<Vec<i64>>();
        let metadata = ready_fixture(
            root.path(),
            &times,
            &vec![0; times.len()],
            Some((0, 0)),
            |raw| {
                let offset = 16 + u32_at(raw, 0) as usize;
                raw[8..12].copy_from_slice(&1u32.to_le_bytes());
                raw[offset + 8..offset + 24].copy_from_slice(&1i128.to_le_bytes());
                raw[offset + 24..offset + 28].copy_from_slice(&8192u32.to_le_bytes());
                raw.drain(offset + 28..offset + 8192 * 28);
            },
        );
        let directory = CoalescedDirectory::read(root.path(), metadata).unwrap();
        let chunk = directory.chunk(0).unwrap();
        assert!(matches!(chunk.values, VisibleSeriesValues::Integer(_)));
        let materialized = chunk.values.materialize();
        assert_eq!(materialized.len(), times.len());
        assert!(directory.decode_bytes(0) >= materialized.get_buffer_memory_size() + chunk.weight);
    }

    #[test]
    fn ready_chunks_reject_malformed_runs_and_values_with_valid_checksums() {
        for case in 0..8 {
            let root = common_test_util::temp_dir::create_temp_dir("ready-corrupt");
            let metadata =
                ready_fixture(root.path(), &[0, 1, 2, 3], &[0; 4], Some((0, 0)), |raw| {
                    let runs = 16 + u32_at(raw, 0) as usize;
                    match case {
                        0 => raw[8..12].copy_from_slice(&0u32.to_le_bytes()),
                        1 => raw[12..16].copy_from_slice(&2u32.to_le_bytes()),
                        2 => raw[runs + 24..runs + 28].copy_from_slice(&0u32.to_le_bytes()),
                        3 => raw[runs + 28..runs + 36].copy_from_slice(&0i64.to_le_bytes()),
                        4 => raw[runs + 8..runs + 24].copy_from_slice(&(-1i128).to_le_bytes()),
                        5 => {
                            raw[runs + 8..runs + 24].copy_from_slice(&i128::MAX.to_le_bytes());
                            raw[runs + 24..runs + 28].copy_from_slice(&3u32.to_le_bytes());
                        }
                        6 => {
                            let offset = raw.len() - 16;
                            raw[offset..offset + 8].copy_from_slice(&(1i64 << 54).to_le_bytes());
                        }
                        7 => {
                            raw.pop();
                        }
                        _ => unreachable!(),
                    }
                });
            let directory = CoalescedDirectory::read(root.path(), metadata).unwrap();
            assert!(directory.chunk(0).is_err(), "case {case}");
        }
    }

    #[test]
    fn coalesced_chunks_preserve_irregular_times_bits_and_source_identity() {
        let root = common_test_util::temp_dir::create_temp_dir("coalesced-series");
        let times = [i64::MIN, -12, -10, -8, 0, 3, 6];
        let bits = [
            0x8000000000000000,
            0x7ff0000000000002,
            0x7ff8000000001234,
            0x7ff0000000000000,
            0xfff0000000000000,
            0,
            0x3ff0000000000000,
        ];
        let metadata = fixture(root.path(), &times, &bits);
        let directory = CoalescedDirectory::read(root.path(), metadata.clone()).unwrap();
        assert_eq!(
            Some(0..2),
            directory.chunks(&MetricSeriesId {
                table_id: 7,
                tsid: 9
            })
        );
        assert!(
            directory
                .chunks(&MetricSeriesId {
                    table_id: 7,
                    tsid: 8
                })
                .is_none()
        );
        assert!(
            directory
                .chunks(&MetricSeriesId {
                    table_id: 8,
                    tsid: 9
                })
                .is_none()
        );
        let chunk = directory.chunk(0).unwrap();
        let actual = chunk
            .runs
            .iter()
            .flat_map(|(first, step, rows)| {
                (0..rows.len()).map(|i| (*first as i128 + *step * i as i128) as i64)
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, times);
        assert_eq!(
            chunk
                .values
                .iter_values()
                .map(f64::to_bits)
                .collect::<Vec<_>>(),
            bits
        );
        assert_eq!(
            directory.chunk(1).unwrap().values.value(0).to_bits(),
            0x8000000000000000
        );
        assert!(directory.matches_sources(&directory.files));
        assert!(!directory.matches_sources(&[]));
        assert!(!directory.matches_sources(&[SourceFile {
            sst: "a".into(),
            source_sha256: "b".into(),
            source_index_crc32: 1,
            blocks: 2
        }]));
        let mut data = std::fs::read(root.path().join("samples.bin")).unwrap();
        let offset = u64_at(directory.entry(1), 12) as usize;
        data[offset] ^= 1;
        std::fs::write(root.path().join("samples.bin"), data).unwrap();
        let directory = CoalescedDirectory::read(root.path(), metadata).unwrap();
        assert!(directory.chunk(1).is_err());
    }

    #[test]
    fn coalesced_chunks_reject_nonincreasing_timestamps_and_invalid_index() {
        let root = common_test_util::temp_dir::create_temp_dir("coalesced-series");
        for times in [[1, 1], [2, 1]] {
            let metadata = fixture(root.path(), &times, &[0, 1]);
            let directory = CoalescedDirectory::read(root.path(), metadata).unwrap();
            assert!(directory.chunk(0).is_err());
        }
        let metadata = fixture(root.path(), &[0, 1], &[0, 1]);
        let mut index = std::fs::read(root.path().join("series.bin")).unwrap();
        index[HEADER + 36..HEADER + 44].copy_from_slice(&2u64.to_le_bytes());
        std::fs::write(root.path().join("series.bin"), &index).unwrap();
        let path = root.path().join("manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        manifest["artifacts"]["series.bin"]["crc32"] = crc32fast::hash(&index).into();
        std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(CoalescedDirectory::read(root.path(), metadata).is_err());
    }
}
