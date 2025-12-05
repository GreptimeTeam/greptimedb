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

use std::convert::TryFrom;

use common_base::range_read::RangeReader;
use common_telemetry::warn;
use greptime_proto::v1::index::{BloomFilterMeta, InvertedIndexMeta, InvertedIndexMetas};
use index::bitmap::BitmapType;
use index::bloom_filter::reader::{BloomFilterReader, BloomFilterReaderImpl};
use index::fulltext_index::Config as FulltextConfig;
use index::inverted_index::format::reader::{InvertedIndexBlobReader, InvertedIndexReader};
use index::target::IndexTarget;
use puffin::blob_metadata::BlobMetadata;
use puffin::puffin_manager::{PuffinManager, PuffinReader};
use serde_json::{Map, Value, json};
use store_api::sst_entry::PuffinIndexMetaEntry;
use store_api::storage::{ColumnId, RegionGroup, RegionId, RegionNumber, RegionSeq, TableId};

use crate::cache::index::bloom_filter_index::{
    BloomFilterIndexCacheRef, CachedBloomFilterIndexBlobReader, Tag,
};
use crate::cache::index::inverted_index::{CachedInvertedIndexBlobReader, InvertedIndexCacheRef};
use crate::sst::file::RegionFileId;
use crate::sst::index::bloom_filter::INDEX_BLOB_TYPE as BLOOM_BLOB_TYPE;
use crate::sst::index::fulltext_index::{
    INDEX_BLOB_TYPE_BLOOM as FULLTEXT_BLOOM_BLOB_TYPE,
    INDEX_BLOB_TYPE_TANTIVY as FULLTEXT_TANTIVY_BLOB_TYPE,
};
use crate::sst::index::inverted_index::INDEX_BLOB_TYPE as INVERTED_BLOB_TYPE;
use crate::sst::index::puffin_manager::{SstPuffinManager, SstPuffinReader};

const INDEX_TYPE_BLOOM: &str = "bloom_filter";
const INDEX_TYPE_FULLTEXT_BLOOM: &str = "fulltext_bloom";
const INDEX_TYPE_FULLTEXT_TANTIVY: &str = "fulltext_tantivy";
const INDEX_TYPE_INVERTED: &str = "inverted";

const TARGET_TYPE_UNKNOWN: &str = "unknown";

const TARGET_TYPE_COLUMN: &str = "column";

pub(crate) struct IndexEntryContext<'a> {
    pub(crate) table_dir: &'a str,
    pub(crate) index_file_path: &'a str,
    pub(crate) region_id: RegionId,
    pub(crate) table_id: TableId,
    pub(crate) region_number: RegionNumber,
    pub(crate) region_group: RegionGroup,
    pub(crate) region_sequence: RegionSeq,
    pub(crate) file_id: &'a str,
    pub(crate) index_file_size: Option<u64>,
    pub(crate) node_id: Option<u64>,
}

/// Collect index metadata entries present in the SST puffin file.
pub(crate) async fn collect_index_entries_from_puffin(
    manager: SstPuffinManager,
    region_file_id: RegionFileId,
    context: IndexEntryContext<'_>,
    bloom_filter_cache: Option<BloomFilterIndexCacheRef>,
    inverted_index_cache: Option<InvertedIndexCacheRef>,
) -> Vec<PuffinIndexMetaEntry> {
    let mut entries = Vec::new();

    let reader = match manager.reader(&region_file_id).await {
        Ok(reader) => reader,
        Err(err) => {
            warn!(
                err;
                "Failed to open puffin index file, table_dir: {}, file_id: {}",
                context.table_dir,
                context.file_id
            );
            return entries;
        }
    };

    let file_metadata = match reader.metadata().await {
        Ok(metadata) => metadata,
        Err(err) => {
            warn!(
                err;
                "Failed to read puffin file metadata, table_dir: {}, file_id: {}",
                context.table_dir,
                context.file_id
            );
            return entries;
        }
    };

    for blob in &file_metadata.blobs {
        match BlobIndexTypeTargetKey::from_blob_type(&blob.blob_type) {
            Some(BlobIndexTypeTargetKey::BloomFilter(target_key)) => {
                let bloom_meta = try_read_bloom_meta(
                    &reader,
                    region_file_id,
                    blob.blob_type.as_str(),
                    target_key,
                    bloom_filter_cache.as_ref(),
                    Tag::Skipping,
                    &context,
                )
                .await;

                let bloom_value = bloom_meta.as_ref().map(bloom_meta_value);
                let (target_type, target_json) = decode_target_info(target_key);
                let meta_json = build_meta_json(bloom_value, None, None);
                let entry = build_index_entry(
                    &context,
                    INDEX_TYPE_BLOOM,
                    target_type,
                    target_key.to_string(),
                    target_json,
                    blob.length as u64,
                    meta_json,
                );
                entries.push(entry);
            }
            Some(BlobIndexTypeTargetKey::FulltextBloom(target_key)) => {
                let bloom_meta = try_read_bloom_meta(
                    &reader,
                    region_file_id,
                    blob.blob_type.as_str(),
                    target_key,
                    bloom_filter_cache.as_ref(),
                    Tag::Fulltext,
                    &context,
                )
                .await;

                let bloom_value = bloom_meta.as_ref().map(bloom_meta_value);
                let fulltext_value = Some(fulltext_meta_value(blob));
                let (target_type, target_json) = decode_target_info(target_key);
                let meta_json = build_meta_json(bloom_value, fulltext_value, None);
                let entry = build_index_entry(
                    &context,
                    INDEX_TYPE_FULLTEXT_BLOOM,
                    target_type,
                    target_key.to_string(),
                    target_json,
                    blob.length as u64,
                    meta_json,
                );
                entries.push(entry);
            }
            Some(BlobIndexTypeTargetKey::FulltextTantivy(target_key)) => {
                let fulltext_value = Some(fulltext_meta_value(blob));
                let (target_type, target_json) = decode_target_info(target_key);
                let meta_json = build_meta_json(None, fulltext_value, None);
                let entry = build_index_entry(
                    &context,
                    INDEX_TYPE_FULLTEXT_TANTIVY,
                    target_type,
                    target_key.to_string(),
                    target_json,
                    blob.length as u64,
                    meta_json,
                );
                entries.push(entry);
            }
            Some(BlobIndexTypeTargetKey::Inverted) => {
                let mut inverted_entries = collect_inverted_entries(
                    &reader,
                    region_file_id,
                    inverted_index_cache.as_ref(),
                    &context,
                )
                .await;
                entries.append(&mut inverted_entries);
            }
            None => {}
        }
    }

    entries
}

async fn collect_inverted_entries(
    reader: &SstPuffinReader,
    region_file_id: RegionFileId,
    cache: Option<&InvertedIndexCacheRef>,
    context: &IndexEntryContext<'_>,
) -> Vec<PuffinIndexMetaEntry> {
    // Read the inverted index blob and surface its per-column metadata entries.
    let file_id = region_file_id.file_id();

    let guard = match reader.blob(INVERTED_BLOB_TYPE).await {
        Ok(guard) => guard,
        Err(err) => {
            warn!(
                err;
                "Failed to open inverted index blob, table_dir: {}, file_id: {}",
                context.table_dir,
                context.file_id
            );
            return Vec::new();
        }
    };

    let blob_reader = match guard.reader().await {
        Ok(reader) => reader,
        Err(err) => {
            warn!(
                err;
                "Failed to build inverted index blob reader, table_dir: {}, file_id: {}",
                context.table_dir,
                context.file_id
            );
            return Vec::new();
        }
    };

    let blob_size = blob_reader
        .metadata()
        .await
        .ok()
        .map(|meta| meta.content_length);
    let metas = if let (Some(cache), Some(blob_size)) = (cache, blob_size) {
        let reader = CachedInvertedIndexBlobReader::new(
            file_id,
            blob_size,
            InvertedIndexBlobReader::new(blob_reader),
            cache.clone(),
        );
        match reader.metadata(None).await {
            Ok(metas) => metas,
            Err(err) => {
                warn!(
                    err;
                    "Failed to read inverted index metadata, table_dir: {}, file_id: {}",
                    context.table_dir,
                    context.file_id
                );
                return Vec::new();
            }
        }
    } else {
        let reader = InvertedIndexBlobReader::new(blob_reader);
        match reader.metadata(None).await {
            Ok(metas) => metas,
            Err(err) => {
                warn!(
                    err;
                    "Failed to read inverted index metadata, table_dir: {}, file_id: {}",
                    context.table_dir,
                    context.file_id
                );
                return Vec::new();
            }
        }
    };

    build_inverted_entries(context, metas.as_ref())
}

fn build_inverted_entries(
    context: &IndexEntryContext<'_>,
    metas: &InvertedIndexMetas,
) -> Vec<PuffinIndexMetaEntry> {
    let mut entries = Vec::new();
    for (name, meta) in &metas.metas {
        let (target_type, target_json) = decode_target_info(name);
        let inverted_value = inverted_meta_value(meta, metas);
        let meta_json = build_meta_json(None, None, Some(inverted_value));
        let entry = build_index_entry(
            context,
            INDEX_TYPE_INVERTED,
            target_type,
            name.clone(),
            target_json,
            meta.inverted_index_size,
            meta_json,
        );
        entries.push(entry);
    }
    entries
}

async fn try_read_bloom_meta(
    reader: &SstPuffinReader,
    region_file_id: RegionFileId,
    blob_type: &str,
    target_key: &str,
    cache: Option<&BloomFilterIndexCacheRef>,
    tag: Tag,
    context: &IndexEntryContext<'_>,
) -> Option<BloomFilterMeta> {
    let column_id = decode_column_id(target_key);

    // Failures are logged but do not abort the overall metadata collection.
    match reader.blob(blob_type).await {
        Ok(guard) => match guard.reader().await {
            Ok(blob_reader) => {
                let blob_size = blob_reader
                    .metadata()
                    .await
                    .ok()
                    .map(|meta| meta.content_length);
                let bloom_reader = BloomFilterReaderImpl::new(blob_reader);
                let result = match (cache, column_id, blob_size) {
                    (Some(cache), Some(column_id), Some(blob_size)) => {
                        CachedBloomFilterIndexBlobReader::new(
                            region_file_id.file_id(),
                            column_id,
                            tag,
                            blob_size,
                            bloom_reader,
                            cache.clone(),
                        )
                        .metadata(None)
                        .await
                    }
                    _ => bloom_reader.metadata(None).await,
                };

                match result {
                    Ok(meta) => Some(meta),
                    Err(err) => {
                        warn!(
                            err;
                            "Failed to read index metadata, table_dir: {}, file_id: {}, blob: {}",
                            context.table_dir,
                            context.file_id,
                            blob_type
                        );
                        None
                    }
                }
            }
            Err(err) => {
                warn!(
                    err;
                    "Failed to open index blob reader, table_dir: {}, file_id: {}, blob: {}",
                    context.table_dir,
                    context.file_id,
                    blob_type
                );
                None
            }
        },
        Err(err) => {
            warn!(
                err;
                "Failed to open index blob, table_dir: {}, file_id: {}, blob: {}",
                context.table_dir,
                context.file_id,
                blob_type
            );
            None
        }
    }
}

fn decode_target_info(target_key: &str) -> (String, String) {
    match IndexTarget::decode(target_key) {
        Ok(IndexTarget::ColumnId(id)) => (
            TARGET_TYPE_COLUMN.to_string(),
            json!({ "column": id }).to_string(),
        ),
        _ => (
            TARGET_TYPE_UNKNOWN.to_string(),
            json!({ "error": "failed_to_decode" }).to_string(),
        ),
    }
}

fn decode_column_id(target_key: &str) -> Option<ColumnId> {
    match IndexTarget::decode(target_key) {
        Ok(IndexTarget::ColumnId(id)) => Some(id),
        _ => None,
    }
}

fn bloom_meta_value(meta: &BloomFilterMeta) -> Value {
    json!({
        "rows_per_segment": meta.rows_per_segment,
        "segment_count": meta.segment_count,
        "row_count": meta.row_count,
        "bloom_filter_size": meta.bloom_filter_size,
    })
}

fn fulltext_meta_value(blob: &BlobMetadata) -> Value {
    let config = FulltextConfig::from_blob_metadata(blob).unwrap_or_default();
    json!({
        "analyzer": config.analyzer.to_str(),
        "case_sensitive": config.case_sensitive,
    })
}

fn inverted_meta_value(meta: &InvertedIndexMeta, metas: &InvertedIndexMetas) -> Value {
    let bitmap_type = BitmapType::try_from(meta.bitmap_type)
        .map(|bt| format!("{:?}", bt))
        .unwrap_or_else(|_| meta.bitmap_type.to_string());
    json!({
        "bitmap_type": bitmap_type,
        "base_offset": meta.base_offset,
        "inverted_index_size": meta.inverted_index_size,
        "relative_fst_offset": meta.relative_fst_offset,
        "fst_size": meta.fst_size,
        "relative_null_bitmap_offset": meta.relative_null_bitmap_offset,
        "null_bitmap_size": meta.null_bitmap_size,
        "segment_row_count": metas.segment_row_count,
        "total_row_count": metas.total_row_count,
    })
}

fn build_meta_json(
    bloom: Option<Value>,
    fulltext: Option<Value>,
    inverted: Option<Value>,
) -> Option<String> {
    let mut map = Map::new();
    if let Some(value) = bloom {
        map.insert("bloom".to_string(), value);
    }
    if let Some(value) = fulltext {
        map.insert("fulltext".to_string(), value);
    }
    if let Some(value) = inverted {
        map.insert("inverted".to_string(), value);
    }
    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map).to_string())
    }
}

enum BlobIndexTypeTargetKey<'a> {
    BloomFilter(&'a str),
    FulltextBloom(&'a str),
    FulltextTantivy(&'a str),
    Inverted,
}

impl<'a> BlobIndexTypeTargetKey<'a> {
    fn from_blob_type(blob_type: &'a str) -> Option<Self> {
        if let Some(target_key) = Self::target_key_from_blob(blob_type, BLOOM_BLOB_TYPE) {
            Some(BlobIndexTypeTargetKey::BloomFilter(target_key))
        } else if let Some(target_key) =
            Self::target_key_from_blob(blob_type, FULLTEXT_BLOOM_BLOB_TYPE)
        {
            Some(BlobIndexTypeTargetKey::FulltextBloom(target_key))
        } else if let Some(target_key) =
            Self::target_key_from_blob(blob_type, FULLTEXT_TANTIVY_BLOB_TYPE)
        {
            Some(BlobIndexTypeTargetKey::FulltextTantivy(target_key))
        } else if blob_type == INVERTED_BLOB_TYPE {
            Some(BlobIndexTypeTargetKey::Inverted)
        } else {
            None
        }
    }

    fn target_key_from_blob(blob_type: &'a str, prefix: &str) -> Option<&'a str> {
        // Blob types encode their target as "<prefix>-<target>".
        blob_type
            .strip_prefix(prefix)
            .and_then(|suffix| suffix.strip_prefix('-'))
    }
}

fn build_index_entry(
    context: &IndexEntryContext<'_>,
    index_type: &str,
    target_type: String,
    target_key: String,
    target_json: String,
    blob_size: u64,
    meta_json: Option<String>,
) -> PuffinIndexMetaEntry {
    PuffinIndexMetaEntry {
        table_dir: context.table_dir.to_string(),
        index_file_path: context.index_file_path.to_string(),
        region_id: context.region_id,
        table_id: context.table_id,
        region_number: context.region_number,
        region_group: context.region_group,
        region_sequence: context.region_sequence,
        file_id: context.file_id.to_string(),
        index_file_size: context.index_file_size,
        index_type: index_type.to_string(),
        target_type,
        target_key,
        target_json,
        blob_size,
        meta_json,
        node_id: context.node_id,
    }
}
