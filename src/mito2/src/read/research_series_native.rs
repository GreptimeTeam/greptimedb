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

use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::value::Value;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, SparsePrimaryKeyCodec};
use moka::sync::Cache;
use serde::Deserialize;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};

use super::research_series_major::SourceFile;
use crate::error::{Result, UnexpectedSnafu};
use crate::series_index::MetricSeriesId;
use crate::sst::parquet::research_sidecar::{NativeValueColumn, NativeValueFile, read_payload_at};

const HEADER: usize = 16;
const ENTRY: usize = 44;
const BLOCK: usize = 80;
// Bounded by the u32 cache weigher; >100M-series datasets need a table subset.
const MAX_METADATA: usize = 3 * 1024 * 1024 * 1024;
const MAX_RECORD: usize = 16 * 1024 * 1024;
const RECORD_CACHE: u64 = 128 * 1024 * 1024;

type Checked<T> = std::result::Result<T, String>;

fn error(reason: impl std::fmt::Display) -> crate::error::Error {
    UnexpectedSnafu {
        reason: format!("Research native series: {reason}"),
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

// All callers validate the containing fixed-width record before reading fields.
fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

#[derive(Deserialize)]
struct Artifact {
    bytes: usize,
    crc32: u32,
}

#[derive(Deserialize)]
struct Counts {
    series: usize,
    blocks: usize,
    source_blocks: Option<usize>,
}

#[derive(Deserialize)]
struct Manifest {
    version: u32,
    status: String,
    files: Vec<SourceFile>,
    counts: Counts,
    artifacts: HashMap<String, Artifact>,
    /// Metric table ids whose series are all present; absent means every table.
    #[serde(default)]
    tables: Option<Vec<u32>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum LabelValue {
    Number(u32),
    Text(String),
}

#[derive(Deserialize)]
struct LabelPosting {
    value: LabelValue,
    offset: usize,
    count: usize,
    crc32: u32,
}

#[derive(Deserialize)]
struct LabelColumn {
    column_id: u32,
    dtype: String,
    values: Vec<LabelPosting>,
}

pub(crate) struct NativeBlock {
    pub(crate) file: usize,
    pub(crate) count: usize,
    pub(crate) first: i64,
    pub(crate) last: i64,
    pub(crate) step: i128,
    pub(crate) value_offset: usize,
    pub(crate) values: NativeValueColumn,
}

// This format admits only non-null Put blocks. The writer verifies that property
// against the sealed source index; the scan must still prove cross-source disjointness.
pub(crate) struct NativeRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) tags: Vec<(String, Value)>,
    pub(crate) blocks: Vec<NativeBlock>,
    pub(crate) weight: usize,
}

pub(crate) struct SelectedNativeBlock {
    pub(crate) record: Arc<NativeRecord>,
    pub(crate) index: usize,
    pub(crate) file: Arc<NativeValueFile>,
}

impl SelectedNativeBlock {
    pub(crate) fn block(&self) -> &NativeBlock {
        &self.record.blocks[self.index]
    }

    pub(crate) fn read_values(
        &self,
        native: bool,
    ) -> Result<store_api::region_engine::VisibleSeriesValues> {
        let block = self.block();
        Ok(self
            .file
            .read(&block.values, native)?
            .slice(block.value_offset, block.count))
    }
}

pub(crate) struct NativeDirectory {
    metadata: RegionMetadataRef,
    pub(crate) files: Vec<SourceFile>,
    index: Vec<u8>,
    postings: Vec<u8>,
    labels: HashMap<u32, Vec<(Value, Range<usize>)>>,
    records: File,
    block_bytes: usize,
    cache: Cache<usize, Arc<NativeRecord>>,
    weight: u32,
    tables: Option<Vec<u32>>,
    /// Some keys share an id; only key-aware readers (coalesced chunks) may serve it.
    pub(crate) repeated_ids: bool,
}

static DIRECTORIES: LazyLock<Cache<PathBuf, Arc<NativeDirectory>>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(MAX_METADATA as u64)
        .weigher(|_, directory: &Arc<NativeDirectory>| directory.weight)
        .build()
});

fn read_artifact(root: &Path, manifest: &Manifest, name: &str) -> Checked<Vec<u8>> {
    let descriptor = manifest.artifacts.get(name).ok_or("Missing artifact")?;
    check(
        descriptor.bytes <= MAX_METADATA,
        "Native metadata exceeds budget",
    )?;
    let file = File::open(root.join(name)).map_err(|e| e.to_string())?;
    check(
        file.metadata().map_err(|e| e.to_string())?.len() == descriptor.bytes as u64,
        "Native artifact length mismatch",
    )?;
    let mut bytes = Vec::with_capacity(descriptor.bytes);
    file.take(descriptor.bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    check(
        bytes.len() == descriptor.bytes && crc32fast::hash(&bytes) == descriptor.crc32,
        "Native artifact checksum mismatch",
    )?;
    Ok(bytes)
}

impl NativeDirectory {
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

    fn read(root: &Path, metadata: RegionMetadataRef) -> Checked<Self> {
        check(usize::BITS == 64, "Native directory requires 64-bit target")?;
        let manifest_file = File::open(root.join("manifest.json")).map_err(|e| e.to_string())?;
        check(
            manifest_file.metadata().map_err(|e| e.to_string())?.len() <= 1024 * 1024,
            "Native manifest exceeds budget",
        )?;
        let manifest: Manifest =
            serde_json::from_reader(manifest_file).map_err(|e| e.to_string())?;
        check(
            matches!(manifest.version, 1 | 2) && manifest.status == "pass",
            "Unvalidated native directory",
        )?;
        let block_bytes = if manifest.version == 1 {
            BLOCK
        } else {
            BLOCK + 8
        };
        let resident = ["series.bin", "postings.bin", "labels.json"]
            .iter()
            .try_fold(RECORD_CACHE as usize, |total, name| {
                total.checked_add(manifest.artifacts.get(*name)?.bytes)
            })
            .ok_or("Native metadata size overflow or missing artifact")?;
        check(resident <= MAX_METADATA, "Native metadata exceeds budget")?;
        check(
            !manifest.files.is_empty()
                && manifest.files.len() <= 64
                && manifest
                    .files
                    .windows(2)
                    .all(|pair| pair[0].sst < pair[1].sst),
            "Invalid native sources",
        )?;
        let count = manifest.counts.series;
        check(
            count <= u32::MAX as usize && count <= (MAX_METADATA - HEADER) / ENTRY,
            "Native series count exceeds budget",
        )?;
        let index = read_artifact(root, &manifest, "series.bin")?;
        check(
            index.len() == HEADER + count * ENTRY
                && &index[..8] == b"GTNSER01"
                && u64_at(&index, 8) == count as u64,
            "Invalid native index header",
        )?;
        let descriptor = manifest
            .artifacts
            .get("records.bin")
            .ok_or("Missing records artifact")?;
        let records = File::open(root.join("records.bin")).map_err(|e| e.to_string())?;
        check(
            records.metadata().map_err(|e| e.to_string())?.len() == descriptor.bytes as u64,
            "Native records length mismatch",
        )?;
        let mut header = [0; HEADER];
        read_payload_at(&records, &mut header, 0).map_err(|e| e.to_string())?;
        check(
            &header[..8] == b"GTNREC01" && u64_at(&header, 8) == count as u64,
            "Invalid native records header",
        )?;
        let valid_mask = u64::MAX >> (64 - manifest.files.len());
        let mut next = HEADER as u64;
        let mut previous = None;
        let mut repeated_ids = false;
        let mut blocks = 0usize;
        for entry in index[HEADER..].chunks_exact(ENTRY) {
            let id = (u32_at(entry, 0), u64_at(entry, 4));
            let length = u32_at(entry, 20) as usize;
            let raw_length = u32_at(entry, 24) as usize;
            let count = u32_at(entry, 32) as usize;
            let mask = u64_at(entry, 36);
            check(
                // Distinct keys may share (table id, __tsid): the engine's tsid hash collides.
                previous.is_none_or(|previous| previous <= id) && u64_at(entry, 12) == next,
                "Unsorted or noncontiguous native index",
            )?;
            check(
                count > 0
                    && raw_length <= MAX_RECORD
                    && count <= (MAX_RECORD - 30) / block_bytes
                    && raw_length >= 30 + count * block_bytes
                    && length > 0
                    && length <= MAX_RECORD + 65536,
                "Invalid native record extent",
            )?;
            check(
                mask != 0 && mask & !valid_mask == 0,
                "Invalid native file mask",
            )?;
            repeated_ids |= previous == Some(id);
            next = next
                .checked_add(length as u64)
                .ok_or("Native record offset overflow")?;
            blocks = blocks
                .checked_add(count)
                .ok_or("Native block count overflow")?;
            previous = Some(id);
        }
        check(
            next == descriptor.bytes as u64 && blocks == manifest.counts.blocks,
            "Incomplete native record coverage",
        )?;
        let source_blocks = manifest
            .files
            .iter()
            .try_fold(0usize, |total, file| total.checked_add(file.blocks));
        let covered_blocks = if manifest.version == 1 {
            Some(blocks)
        } else {
            manifest.counts.source_blocks
        };
        check(
            // A table subset covers only the blocks of its tables.
            if manifest.tables.is_some() {
                covered_blocks.is_some() && covered_blocks <= source_blocks
            } else {
                covered_blocks == source_blocks
            },
            "Native source block count mismatch",
        )?;
        let postings = read_artifact(root, &manifest, "postings.bin")?;
        check(
            postings.len() >= HEADER && &postings[..8] == b"GTNPST01",
            "Invalid native postings header",
        )?;
        let dictionary = read_artifact(root, &manifest, "labels.json")?;
        let columns: Vec<LabelColumn> =
            serde_json::from_slice(&dictionary).map_err(|e| e.to_string())?;
        let mut labels = HashMap::new();
        let mut next = HEADER;
        let mut posting_count = 0;
        let mut label_bytes = 0usize;
        for column in columns {
            let metadata_column = metadata
                .column_by_id(column.column_id)
                .ok_or("Unknown indexed label column")?;
            check(
                metadata_column.semantic_type == api::v1::SemanticType::Tag,
                "Indexed column is not a tag",
            )?;
            let mut seen = vec![false; count];
            let mut values = Vec::new();
            for posting in column.values {
                let value = match posting.value {
                    LabelValue::Number(value) if column.dtype == "u32" => Value::UInt32(value),
                    LabelValue::Text(value) if column.dtype == "string" => {
                        Value::String(value.into())
                    }
                    _ => return Err("Invalid indexed label type".into()),
                };
                value
                    .try_to_scalar_value(&metadata_column.column_schema.data_type)
                    .map_err(|e| e.to_string())?;
                check(
                    values.last().is_none_or(|(previous, _)| previous < &value),
                    "Unsorted indexed label values",
                )?;
                let end = posting
                    .count
                    .checked_mul(4)
                    .and_then(|bytes| next.checked_add(bytes))
                    .ok_or("Postings size overflow")?;
                check(
                    posting.count > 0 && posting.offset == next && end <= postings.len(),
                    "Invalid posting extent",
                )?;
                let bytes = &postings[next..end];
                check(
                    crc32fast::hash(bytes) == posting.crc32,
                    "Posting checksum mismatch",
                )?;
                let mut previous = None;
                for bytes in bytes.chunks_exact(4) {
                    let ordinal = u32_at(bytes, 0) as usize;
                    check(
                        ordinal < count && previous.is_none_or(|previous| previous < ordinal),
                        "Invalid posting order",
                    )?;
                    check(
                        !seen[ordinal],
                        "Series appears in multiple values of one label",
                    )?;
                    seen[ordinal] = true;
                    previous = Some(ordinal);
                }
                label_bytes += value.as_value_ref().data_size() + 256;
                values.push((value, next..end));
                next = end;
                posting_count += 1;
            }
            check(
                labels.insert(column.column_id, values).is_none(),
                "Duplicate indexed column",
            )?;
        }
        check(
            next == postings.len() && u64_at(&postings, 8) == posting_count,
            "Incomplete posting coverage",
        )?;
        let weight =
            index.capacity() + postings.capacity() + label_bytes + RECORD_CACHE as usize + 4096;
        check(
            weight <= MAX_METADATA,
            "Native directory exceeds cache budget",
        )?;
        common_telemetry::info!(
            series = count,
            metadata_bytes = weight - RECORD_CACHE as usize,
            record_cache_bytes = RECORD_CACHE,
            "Research native series directory loaded"
        );
        Ok(Self {
            metadata,
            files: manifest.files,
            index,
            postings,
            labels,
            records,
            block_bytes,
            cache: Cache::builder()
                .max_capacity(RECORD_CACHE)
                .weigher(|_, record: &Arc<NativeRecord>| record.weight as u32)
                .build(),
            weight: weight as u32,
            tables: manifest.tables,
            repeated_ids,
        })
    }

    /// Whether the filters restrict the scan to one table this directory fully covers.
    /// A subset directory must not answer for other tables: it would return no series.
    pub(crate) fn covers(&self, filters: &[SimpleFilterEvaluator]) -> bool {
        let Some(tables) = &self.tables else {
            return true;
        };
        filters.iter().any(|filter| {
            filter.column_name() == DATA_SCHEMA_TABLE_ID_COLUMN_NAME
                && filter.is_eq()
                && matches!(filter.literal_value(), Some(Value::UInt32(id)) if tables.contains(&id))
        })
    }

    pub(crate) fn len(&self) -> usize {
        (self.index.len() - HEADER) / ENTRY
    }

    pub(crate) fn block_count(&self, ordinal: usize) -> usize {
        u32_at(self.entry(ordinal), 32) as usize
    }

    fn entry(&self, ordinal: usize) -> &[u8] {
        &self.index[HEADER + ordinal * ENTRY..HEADER + (ordinal + 1) * ENTRY]
    }

    fn id(&self, ordinal: usize) -> MetricSeriesId {
        let entry = self.entry(ordinal);
        MetricSeriesId {
            table_id: u32_at(entry, 0),
            tsid: u64_at(entry, 4),
        }
    }

    pub(crate) fn ordinal(&self, id: &MetricSeriesId) -> Option<usize> {
        let (mut low, mut high) = (0, self.len());
        while low < high {
            let middle = low + (high - low) / 2;
            match self.id(middle).cmp(id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Some(middle),
            }
        }
        None
    }

    pub(crate) fn select(
        &self,
        filters: &[SimpleFilterEvaluator],
        active: &[bool],
    ) -> Result<Vec<MetricSeriesId>> {
        if active.len() != self.files.len() {
            return Err(error("Invalid active source set"));
        }
        let mask = active
            .iter()
            .enumerate()
            .fold(0u64, |mask, (index, active)| {
                mask | (u64::from(*active) << index)
            });
        let mut candidates: Option<Vec<u32>> = None;
        let mut total_postings = 0usize;
        let mut visited_postings = 0usize;
        for filter in filters {
            let bounds = candidates
                .as_ref()
                .and_then(|ids| ids.first().zip(ids.last()))
                .map_or(0..self.len(), |(&first, &last)| {
                    first as usize..last as usize + 1
                });
            let column = self
                .metadata
                .column_by_name(filter.column_name())
                .ok_or_else(|| error("Unknown filter column"))?;
            let accepts = |value: &Value| -> Result<bool> {
                filter
                    .evaluate_scalar(
                        &value
                            .try_to_scalar_value(&column.column_schema.data_type)
                            .map_err(error)?,
                    )
                    .map_err(error)
            };
            let mut matched = Vec::new();
            if filter.column_name() == DATA_SCHEMA_TSID_COLUMN_NAME {
                if let Some(current) = &candidates {
                    for &ordinal in current {
                        if accepts(&Value::UInt64(self.id(ordinal as usize).tsid))? {
                            matched.push(ordinal);
                        }
                    }
                } else {
                    for ordinal in bounds.clone() {
                        if accepts(&Value::UInt64(self.id(ordinal).tsid))? {
                            matched.push(ordinal as u32);
                        }
                    }
                }
            } else {
                let nulls = accepts(&Value::Null)?;
                let mut present = nulls.then(|| vec![false; bounds.len()]);
                if let Some(values) = self.labels.get(&column.column_id) {
                    for (value, range) in values {
                        let include = accepts(value)?;
                        if !include && !nulls {
                            continue;
                        }
                        // Loading validated strict ordinal order and complete four-byte entries.
                        let (ordinals, _) = self.postings[range.clone()].as_chunks::<4>();
                        let start = ordinals.partition_point(|bytes| {
                            (u32::from_le_bytes(*bytes) as usize) < bounds.start
                        });
                        let end = ordinals.partition_point(|bytes| {
                            (u32::from_le_bytes(*bytes) as usize) < bounds.end
                        });
                        total_postings += ordinals.len();
                        visited_postings += end - start;
                        for bytes in &ordinals[start..end] {
                            let ordinal = u32::from_le_bytes(*bytes);
                            if include {
                                matched.push(ordinal);
                            }
                            if let Some(present) = &mut present {
                                present[ordinal as usize - bounds.start] = true;
                            }
                        }
                    }
                }
                if let Some(present) = present {
                    matched.extend(present.iter().enumerate().filter_map(|(ordinal, present)| {
                        (!present).then_some((bounds.start + ordinal) as u32)
                    }));
                }
                matched.sort_unstable();
            }
            candidates = Some(match candidates {
                None => matched,
                Some(mut current) => {
                    let mut position = 0;
                    current.retain(|id| {
                        while position < matched.len() && matched[position] < *id {
                            position += 1;
                        }
                        matched.get(position) == Some(id)
                    });
                    current
                }
            });
            if candidates.as_ref().is_some_and(Vec::is_empty) {
                break;
            }
        }
        common_telemetry::debug!(
            total_postings,
            visited_postings,
            "Research native postings bounded"
        );
        Ok(candidates
            .unwrap_or_else(|| (0..self.len() as u32).collect())
            .into_iter()
            .filter(|ordinal| u64_at(self.entry(*ordinal as usize), 36) & mask != 0)
            .map(|ordinal| self.id(ordinal as usize))
            // Ordinals ascend, so keys sharing an id are adjacent.
            .fold(Vec::new(), |mut ids, id| {
                if ids.last() != Some(&id) {
                    ids.push(id);
                }
                ids
            }))
    }

    pub(crate) fn record(&self, ordinal: usize) -> Result<Arc<NativeRecord>> {
        if ordinal >= self.len() {
            return Err(error("Native series ordinal outside directory"));
        }
        self.cache
            .try_get_with(ordinal, || self.read_record(ordinal).map(Arc::new))
            .map_err(error)
    }

    fn read_record(&self, ordinal: usize) -> Checked<NativeRecord> {
        let entry = self.entry(ordinal);
        let mut compressed = vec![0; u32_at(entry, 20) as usize];
        read_payload_at(&self.records, &mut compressed, u64_at(entry, 12))
            .map_err(|e| e.to_string())?;
        let raw_length = u32_at(entry, 24) as usize;
        let raw = zstd::bulk::decompress(&compressed, raw_length).map_err(|e| e.to_string())?;
        check(
            raw.len() == raw_length && crc32fast::hash(&raw) == u32_at(entry, 28),
            "Native record checksum mismatch",
        )?;
        let key_length = u32_at(&raw, 0) as usize;
        let count = u32_at(&raw, 4) as usize;
        check(
            count == u32_at(entry, 32) as usize
                && key_length >= 22
                && key_length.checked_add(8 + count * self.block_bytes) == Some(raw.len()),
            "Native record bounds mismatch",
        )?;
        let key = raw[8..8 + key_length].to_vec();
        let codec = SparsePrimaryKeyCodec::new(&self.metadata);
        let (table_id, tsid) = codec.decode_ids(&key).map_err(|e| e.to_string())?;
        check(
            self.id(ordinal) == MetricSeriesId { table_id, tsid },
            "Native record identity mismatch",
        )?;
        let CompositeValues::Sparse(decoded) = codec.decode(&key).map_err(|e| e.to_string())?
        else {
            return Err("Expected sparse native key".into());
        };
        let tags = self
            .metadata
            .primary_key_columns()
            .map(|column| {
                (
                    column.column_schema.name.clone(),
                    decoded.get_or_null(column.column_id).clone(),
                )
            })
            .collect::<Vec<_>>();
        let mut weight = key.capacity()
            + size_of::<NativeRecord>()
            + count * size_of::<NativeBlock>()
            + tags
                .iter()
                .map(|(name, value)| name.capacity() + value.as_value_ref().data_size() + 256)
                .sum::<usize>();
        let mut blocks = Vec::with_capacity(count);
        let mut mask = 0u64;
        for block in raw[8 + key_length..].chunks_exact(self.block_bytes) {
            let file = u32_at(block, 0) as usize;
            let count = u32_at(block, 4) as usize;
            let first = u64_at(block, 8) as i64;
            let step = i128::from_le_bytes(block[16..32].try_into().unwrap());
            check(
                file < self.files.len() && block[33..36] == [0; 3],
                "Invalid native block source or padding",
            )?;
            check(
                count > 0 && step >= 0 && (count == 1 || step > 0),
                "Invalid native timestamp progression",
            )?;
            let last = step
                .checked_mul((count - 1) as i128)
                .and_then(|delta| i128::from(first).checked_add(delta))
                .and_then(|last| i64::try_from(last).ok())
                .ok_or("Native timestamp overflow")?;
            let (value_count, value_offset) = if self.block_bytes == BLOCK {
                (count, 0)
            } else {
                (u32_at(block, 80) as usize, u32_at(block, 84) as usize)
            };
            check(
                value_offset <= value_count && count <= value_count - value_offset,
                "Native value slice outside source block",
            )?;
            let values = NativeValueColumn::new(
                value_count,
                block[32],
                u32_at(block, 36),
                u64_at(block, 40),
                usize::try_from(u64_at(block, 48)).map_err(|e| e.to_string())?,
                u64_at(block, 56),
                i128::from_le_bytes(block[64..80].try_into().unwrap()),
            )?;
            weight += values.retained_bytes();
            blocks.push(NativeBlock {
                file,
                count,
                first,
                last,
                step,
                value_offset,
                values,
            });
            mask |= 1 << file;
        }
        check(
            mask == u64_at(entry, 36) && weight <= RECORD_CACHE as usize,
            "Native record membership or size mismatch",
        )?;
        Ok(NativeRecord {
            key,
            tags,
            blocks,
            weight,
        })
    }
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
    use store_api::storage::RegionId;
    use store_api::storage::consts::ReservedColumnId;

    use super::*;

    const LABEL: &str = "a\0汉abcdefgh";

    #[test]
    #[ignore = "Requires GT_NATIVE_DIRECTORY_VERIFY_PATH pointing to the full prototype directory"]
    fn native_directory_matches_all_persisted_labels() {
        let root = PathBuf::from(
            std::env::var_os("GT_NATIVE_DIRECTORY_VERIFY_PATH").expect("directory path"),
        );
        let columns: Vec<LabelColumn> =
            serde_json::from_slice(&std::fs::read(root.join("labels.json")).unwrap()).unwrap();
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        let mut primary = vec![ReservedColumnId::table_id(), ReservedColumnId::tsid()];
        for column in &columns {
            let name = if column.column_id == ReservedColumnId::table_id() {
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string()
            } else {
                format!("tag_{}", column.column_id)
            };
            let datatype = match column.dtype.as_str() {
                "u32" => ConcreteDataType::uint32_datatype(),
                "string" => ConcreteDataType::string_datatype(),
                _ => panic!("unsupported label type"),
            };
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(name, datatype, true),
                column_id: column.column_id,
                semantic_type: SemanticType::Tag,
            });
            if column.column_id != ReservedColumnId::table_id() {
                primary.push(column.column_id);
            }
        }
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                false,
            ),
            column_id: ReservedColumnId::tsid(),
            semantic_type: SemanticType::Tag,
        });
        let time_id = (0..u32::MAX).find(|id| !primary.contains(id)).unwrap();
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            column_id: time_id,
            semantic_type: SemanticType::Timestamp,
        });
        builder
            .primary_key(primary)
            .primary_key_encoding(PrimaryKeyEncoding::Sparse);
        let metadata = Arc::new(builder.build().unwrap());
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let directory = NativeDirectory::read(&root, metadata).unwrap();
        let mut positions = directory
            .labels
            .iter()
            .map(|(id, values)| {
                (
                    *id,
                    values
                        .iter()
                        .map(|(_, range)| range.start)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut file_blocks = vec![0usize; directory.files.len()];
        let mut slice_count = 0usize;
        for ordinal in 0..directory.len() {
            let record = directory.read_record(ordinal).unwrap();
            let CompositeValues::Sparse(decoded) = codec.decode(&record.key).unwrap() else {
                panic!("sparse key");
            };
            for (column, value) in decoded.iter() {
                if *column == ReservedColumnId::tsid() {
                    continue;
                }
                let values = directory
                    .labels
                    .get(column)
                    .expect("missing indexed column");
                let index = values
                    .binary_search_by(|(indexed, _)| indexed.cmp(value))
                    .expect("missing indexed value");
                let position = &mut positions.get_mut(column).unwrap()[index];
                assert!(*position < values[index].1.end, "missing posting");
                assert_eq!(
                    u32_at(&directory.postings, *position) as usize,
                    ordinal,
                    "posting differs from Rust primary-key decoder"
                );
                *position += 4;
            }
            for block in record.blocks {
                slice_count += 1;
                // Each source block has exactly one slice starting at row zero.
                if block.value_offset == 0 {
                    file_blocks[block.file] += 1;
                }
            }
        }
        for (id, positions) in positions {
            for (position, (_, range)) in positions.iter().zip(&directory.labels[&id]) {
                assert_eq!(*position, range.end);
            }
        }
        let manifest: Manifest =
            serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(slice_count, manifest.counts.blocks);
        assert_eq!(
            file_blocks,
            directory
                .files
                .iter()
                .map(|file| file.blocks)
                .collect::<Vec<_>>()
        );
        eprintln!(
            "Validated {} native series with Rust primary-key decoding",
            directory.len()
        );
    }

    fn fixture(root: &Path) -> RegionMetadataRef {
        fixture_with_slice(root, None)
    }

    fn fixture_with_slice(root: &Path, slice_offset: Option<u32>) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        for (name, datatype, id, semantic) in [
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
            (
                "zone",
                ConcreteDataType::string_datatype(),
                3,
                SemanticType::Tag,
            ),
        ] {
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(name, datatype, matches!(name, "pod" | "zone")),
                column_id: id,
                semantic_type: semantic,
            });
        }
        builder
            .primary_key(vec![
                ReservedColumnId::table_id(),
                ReservedColumnId::tsid(),
                1,
                3,
            ])
            .primary_key_encoding(PrimaryKeyEncoding::Sparse);
        let metadata = Arc::new(builder.build().unwrap());
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let header = |magic: &[u8; 8], count: u64| {
            let mut bytes = magic.to_vec();
            bytes.extend_from_slice(&count.to_le_bytes());
            bytes
        };
        let mut index = header(b"GTNSER01", 3);
        let mut records = header(b"GTNREC01", 3);
        for (table, tsid, pod, files) in [
            (1, 1, Some(LABEL), vec![0u32, 1]),
            (1, 2, None, vec![0u32]),
            (2, 3, Some(""), vec![1u32]),
        ] {
            let mut key = Vec::new();
            codec.encode_internal(table, tsid, &mut key).unwrap();
            if let Some(pod) = pod {
                codec
                    .encode_to_vec([(1, ValueRef::String(pod))].into_iter(), &mut key)
                    .unwrap();
            }
            let mut raw = (key.len() as u32).to_le_bytes().to_vec();
            raw.extend_from_slice(&(files.len() as u32).to_le_bytes());
            raw.extend_from_slice(&key);
            let mut mask = 0u64;
            for file in &files {
                let first: i64 = if *file == 0 { -30_000 } else { 0 };
                raw.extend_from_slice(&file.to_le_bytes());
                raw.extend_from_slice(&if slice_offset.is_some() { 2u32 } else { 4 }.to_le_bytes());
                raw.extend_from_slice(&first.to_le_bytes());
                raw.extend_from_slice(&1000i128.to_le_bytes());
                raw.extend_from_slice(&[2, 0, 0, 0]);
                raw.extend_from_slice(&0u32.to_le_bytes());
                raw.extend_from_slice(&0u64.to_le_bytes());
                raw.extend_from_slice(&32u64.to_le_bytes());
                raw.extend_from_slice(&0u64.to_le_bytes());
                raw.extend_from_slice(&0i128.to_le_bytes());
                if let Some(offset) = slice_offset {
                    raw.extend_from_slice(&4u32.to_le_bytes());
                    raw.extend_from_slice(&offset.to_le_bytes());
                }
                mask |= 1 << file;
            }
            let compressed = zstd::bulk::compress(&raw, 3).unwrap();
            index.extend_from_slice(&table.to_le_bytes());
            index.extend_from_slice(&tsid.to_le_bytes());
            index.extend_from_slice(&(records.len() as u64).to_le_bytes());
            index.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
            index.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            index.extend_from_slice(&crc32fast::hash(&raw).to_le_bytes());
            index.extend_from_slice(&(files.len() as u32).to_le_bytes());
            index.extend_from_slice(&mask.to_le_bytes());
            records.extend(compressed);
        }
        let mut postings = header(b"GTNPST01", 4);
        let mut dictionary = Vec::new();
        for (id, dtype, entries) in [
            (
                1,
                "string",
                vec![
                    (serde_json::json!(""), vec![2u32]),
                    (serde_json::json!(LABEL), vec![0u32]),
                ],
            ),
            (
                ReservedColumnId::table_id(),
                "u32",
                vec![
                    (serde_json::json!(1), vec![0u32, 1]),
                    (serde_json::json!(2), vec![2u32]),
                ],
            ),
        ] {
            let mut values = Vec::new();
            for (value, ordinals) in entries {
                let raw = ordinals
                    .iter()
                    .flat_map(|id| id.to_le_bytes())
                    .collect::<Vec<_>>();
                values.push(serde_json::json!({"value": value, "offset": postings.len(), "count": ordinals.len(), "crc32": crc32fast::hash(&raw)}));
                postings.extend(raw);
            }
            dictionary.push(serde_json::json!({"column_id": id, "dtype": dtype, "values": values}));
        }
        let mut artifacts = serde_json::Map::new();
        for (name, bytes) in [
            ("series.bin", index),
            ("records.bin", records),
            ("postings.bin", postings),
            ("labels.json", serde_json::to_vec(&dictionary).unwrap()),
        ] {
            std::fs::write(root.join(name), &bytes).unwrap();
            artifacts.insert(
                name.into(),
                serde_json::json!({"bytes": bytes.len(), "crc32": crc32fast::hash(&bytes)}),
            );
        }
        std::fs::write(
            root.join("manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "version": if slice_offset.is_some() { 2 } else { 1 },
                "status": "pass", "counts": {"series": 3, "blocks": 4, "source_blocks": 4},
                "files": [{"sst": "a", "source_sha256": "a", "source_index_crc32": 1, "blocks": 2},
                          {"sst": "b", "source_sha256": "b", "source_index_crc32": 2, "blocks": 2}],
                "artifacts": artifacts
            }))
            .unwrap(),
        )
        .unwrap();
        metadata
    }

    fn replace_artifact(root: &Path, name: &str, bytes: &[u8]) {
        std::fs::write(root.join(name), bytes).unwrap();
        let path = root.join("manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        manifest["artifacts"][name] =
            serde_json::json!({"bytes": bytes.len(), "crc32": crc32fast::hash(bytes)});
        std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    }

    #[test]
    fn table_subset_directory_only_answers_equality_on_covered_tables() {
        let temp = common_test_util::temp_dir::create_temp_dir("native-table-subset");
        let metadata = fixture(temp.path());
        let path = temp.path().join("manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        manifest["tables"] = serde_json::json!([1]);
        std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let directory = NativeDirectory::read(temp.path(), metadata).unwrap();
        let covers = |exprs: Vec<datafusion_expr::Expr>| {
            let filters = exprs
                .iter()
                .map(|expr| SimpleFilterEvaluator::try_new(expr).unwrap())
                .collect::<Vec<_>>();
            directory.covers(&filters)
        };
        let table = || col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME);
        assert!(covers(vec![
            table().eq(lit(1u32)),
            col("pod").eq(lit(LABEL))
        ]));
        assert!(!covers(vec![table().eq(lit(2u32))]));
        assert!(!covers(vec![table().not_eq(lit(2u32))]));
        assert!(!covers(vec![col("pod").eq(lit(LABEL))]));
    }

    #[test]
    fn native_slices_retain_full_value_extent_and_reject_out_of_bounds() {
        for (offset, valid) in [
            (0, true),
            (1, true),
            (2, true),
            (3, false),
            (u32::MAX, false),
        ] {
            let temp = common_test_util::temp_dir::create_temp_dir("native-series-slice");
            let metadata = fixture_with_slice(temp.path(), Some(offset));
            let directory = NativeDirectory::read(temp.path(), metadata).unwrap();
            let record = directory.read_record(0);
            if valid {
                let record = record.unwrap();
                assert_eq!(record.blocks.len(), 2);
                for block in &record.blocks {
                    assert_eq!(block.count, 2);
                    assert_eq!(block.value_offset, offset as usize);
                    assert_eq!(block.last - block.first, 1000);
                    assert_eq!(block.values.decode_bytes(), 32 + 4 * 16);
                }
            } else {
                assert!(record.err().unwrap().contains("slice outside"));
            }
        }
    }

    #[test]
    fn bounded_postings_match_decoded_series_for_filter_orders_and_null_complements() {
        let temp = common_test_util::temp_dir::create_temp_dir("native-bounded-postings");
        let metadata = fixture(temp.path());
        let directory = NativeDirectory::read(temp.path(), metadata.clone()).unwrap();
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let filters = [
            col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME).eq(lit(1u32)),
            col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME).eq(lit(2u32)),
            col("pod").is_null(),
            col("pod").is_not_null(),
            col("pod").eq(lit("")),
            col("pod").not_eq(lit(LABEL)),
            col("zone").is_null(),
            col("zone").eq(lit("")),
            col(DATA_SCHEMA_TSID_COLUMN_NAME).gt_eq(lit(2u64)),
            col(DATA_SCHEMA_TSID_COLUMN_NAME).not_eq(lit(2u64)),
            col("pod").is_null().or(col("pod").eq(lit(""))),
        ]
        .map(|expr| SimpleFilterEvaluator::try_new(&expr).unwrap());
        let rows = (0..directory.len())
            .map(|ordinal| {
                let record = directory.read_record(ordinal).unwrap();
                let (table_id, tsid) = codec.decode_ids(&record.key).unwrap();
                let accepted = filters
                    .iter()
                    .map(|filter| {
                        let value = &record
                            .tags
                            .iter()
                            .find(|(name, _)| name == filter.column_name())
                            .unwrap()
                            .1;
                        let column = metadata.column_by_name(filter.column_name()).unwrap();
                        filter
                            .evaluate_scalar(
                                &value
                                    .try_to_scalar_value(&column.column_schema.data_type)
                                    .unwrap(),
                            )
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                (MetricSeriesId { table_id, tsid }, record, accepted)
            })
            .collect::<Vec<_>>();
        for active in [[false, false], [false, true], [true, false], [true, true]] {
            for first in 0..filters.len() {
                for second in 0..filters.len() {
                    for third in 0..filters.len() {
                        let order = [first, second, third];
                        let expected = rows
                            .iter()
                            .filter(|(_, record, accepted)| {
                                record.blocks.iter().any(|block| active[block.file])
                                    && order.iter().all(|&index| accepted[index])
                            })
                            .map(|(id, _, _)| *id)
                            .collect::<Vec<_>>();
                        let predicates = order.map(|index| filters[index].clone());
                        assert_eq!(
                            directory.select(&predicates, &active).unwrap(),
                            expected,
                            "filter order {order:?}, active files {active:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn native_postings_preserve_null_empty_labels_and_source_membership() {
        let temp = common_test_util::temp_dir::create_temp_dir("native-series-postings");
        let metadata = fixture(temp.path());
        let directory = NativeDirectory::read(temp.path(), metadata).unwrap();
        let filter = |expr| SimpleFilterEvaluator::try_new(&expr).unwrap();
        let id = |table_id, tsid| MetricSeriesId { table_id, tsid };
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
                .select(&[filter(col("pod").eq(lit("")))], &[true, true])
                .unwrap(),
            vec![id(2, 3)]
        );
        assert_eq!(
            directory
                .select(&[filter(col("pod").not_eq(lit(LABEL)))], &[true, true])
                .unwrap(),
            vec![id(2, 3)]
        );
        assert_eq!(
            directory
                .select(&[filter(col("zone").is_null())], &[true, true])
                .unwrap(),
            vec![id(1, 1), id(1, 2), id(2, 3)]
        );
        assert!(
            directory
                .select(&[filter(col("zone").eq(lit("")))], &[true, true])
                .unwrap()
                .is_empty()
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
                        filter(col(DATA_SCHEMA_TSID_COLUMN_NAME).eq(lit(1u64))),
                        filter(col("pod").eq(lit(LABEL)))
                    ],
                    &[true, true]
                )
                .unwrap(),
            vec![id(1, 1)]
        );
        let record = directory
            .record(directory.ordinal(&id(1, 1)).unwrap())
            .unwrap();
        assert_eq!(
            record
                .tags
                .iter()
                .find(|(name, _)| name == "pod")
                .unwrap()
                .1,
            Value::String(LABEL.into())
        );
        assert_eq!(
            record
                .blocks
                .iter()
                .map(|b| (b.file, b.first, b.last, b.step))
                .collect::<Vec<_>>(),
            vec![(0, -30_000, -27_000, 1000), (1, 0, 3000, 1000)]
        );
        assert!(Arc::ptr_eq(&record, &directory.record(0).unwrap()));
        assert!(directory.record(3).is_err());
    }

    #[test]
    fn native_directory_rejects_corruption_extents_and_record_identity_mismatch() {
        let temp = common_test_util::temp_dir::create_temp_dir("native-series-corruption");
        let metadata = fixture(temp.path());
        let index = std::fs::read(temp.path().join("series.bin")).unwrap();
        let mut corrupt = index.clone();
        corrupt[HEADER + 24..HEADER + 28].copy_from_slice(&0u32.to_le_bytes());
        replace_artifact(temp.path(), "series.bin", &corrupt);
        assert!(
            NativeDirectory::read(temp.path(), metadata.clone())
                .err()
                .unwrap()
                .contains("extent")
        );
        corrupt = index.clone();
        corrupt[HEADER + 2 * ENTRY + 4..HEADER + 2 * ENTRY + 12]
            .copy_from_slice(&99u64.to_le_bytes());
        replace_artifact(temp.path(), "series.bin", &corrupt);
        let directory = NativeDirectory::read(temp.path(), metadata.clone()).unwrap();
        assert!(
            directory
                .record(2)
                .err()
                .unwrap()
                .to_string()
                .contains("identity")
        );
        replace_artifact(temp.path(), "series.bin", &index);
        let mut records = std::fs::read(temp.path().join("records.bin")).unwrap();
        records[HEADER] ^= 1;
        std::fs::write(temp.path().join("records.bin"), records).unwrap();
        let directory = NativeDirectory::read(temp.path(), metadata.clone()).unwrap();
        assert!(directory.record(0).is_err());
        let mut postings = std::fs::read(temp.path().join("postings.bin")).unwrap();
        postings[HEADER] ^= 1;
        std::fs::write(temp.path().join("postings.bin"), postings).unwrap();
        assert!(
            NativeDirectory::read(temp.path(), metadata)
                .err()
                .unwrap()
                .contains("checksum")
        );
    }
}
