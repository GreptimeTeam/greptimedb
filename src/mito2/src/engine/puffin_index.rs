use std::convert::TryFrom;

use common_telemetry::warn;
use greptime_proto::v1::index::{BloomFilterMeta, InvertedIndexMeta, InvertedIndexMetas};
use index::bitmap::BitmapType;
use index::bloom_filter::reader::{BloomFilterReader, BloomFilterReaderImpl};
use index::fulltext_index::{Analyzer, Config as FulltextConfig};
use index::inverted_index::format::reader::{InvertedIndexBlobReader, InvertedIndexReader};
use index::target::IndexTarget;
use puffin::blob_metadata::BlobMetadata;
use puffin::puffin_manager::{PuffinManager, PuffinReader};
use serde_json::{Map, Value, json};
use store_api::sst_entry::PuffinIndexMetaEntry;
use store_api::storage::{RegionGroup, RegionId, RegionNumber, RegionSeq, TableId};

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

pub(crate) struct IndexEntryContext<'a> {
    pub(crate) table_dir: &'a str,
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
        if let Some(target_key) = target_key_from_blob(&blob.blob_type, BLOOM_BLOB_TYPE) {
            let bloom_meta =
                try_read_bloom_meta(&reader, blob.blob_type.as_str(), &context, "bloom filter")
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
            continue;
        }

        if let Some(target_key) = target_key_from_blob(&blob.blob_type, FULLTEXT_BLOOM_BLOB_TYPE) {
            let bloom_meta: Option<BloomFilterMeta> =
                try_read_bloom_meta(&reader, blob.blob_type.as_str(), &context, "fulltext bloom")
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
            continue;
        }

        if let Some(target_key) = target_key_from_blob(&blob.blob_type, FULLTEXT_TANTIVY_BLOB_TYPE)
        {
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
            continue;
        }

        if blob.blob_type == INVERTED_BLOB_TYPE {
            let mut inverted_entries = collect_inverted_entries(&reader, &context).await;
            entries.append(&mut inverted_entries);
        }
    }

    entries
}

async fn collect_inverted_entries(
    reader: &SstPuffinReader,
    context: &IndexEntryContext<'_>,
) -> Vec<PuffinIndexMetaEntry> {
    // Read the inverted index blob and surface its per-column metadata entries.
    let mut entries = Vec::new();

    let guard = match reader.blob(INVERTED_BLOB_TYPE).await {
        Ok(guard) => guard,
        Err(err) => {
            warn!(
                err;
                "Failed to open inverted index blob, table_dir: {}, file_id: {}",
                context.table_dir,
                context.file_id
            );
            return entries;
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
            return entries;
        }
    };

    let reader = InvertedIndexBlobReader::new(blob_reader);
    let metas = match reader.metadata().await {
        Ok(metas) => metas,
        Err(err) => {
            warn!(
                err;
                "Failed to read inverted index metadata, table_dir: {}, file_id: {}",
                context.table_dir,
                context.file_id
            );
            return entries;
        }
    };

    for (name, meta) in &metas.metas {
        let (target_type, target_json) = decode_target_info(name);
        let inverted_value = inverted_meta_value(meta, metas.as_ref());
        let meta_json = build_meta_json(None, None, Some(inverted_value));
        let entry = build_index_entry(
            context,
            INDEX_TYPE_INVERTED,
            target_type,
            name.to_string(),
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
    blob_type: &str,
    context: &IndexEntryContext<'_>,
    label: &str,
) -> Option<BloomFilterMeta> {
    // Failures are logged but do not abort the overall metadata collection.
    match reader.blob(blob_type).await {
        Ok(guard) => match guard.reader().await {
            Ok(blob_reader) => {
                let bloom_reader = BloomFilterReaderImpl::new(blob_reader);
                match bloom_reader.metadata().await {
                    Ok(meta) => Some(meta),
                    Err(err) => {
                        warn!(
                            err;
                            "Failed to read {} metadata, table_dir: {}, file_id: {}, blob: {}",
                            label,
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
                    "Failed to open {} blob reader, table_dir: {}, file_id: {}, blob: {}",
                    label,
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
                "Failed to open {} blob, table_dir: {}, file_id: {}, blob: {}",
                label,
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
        Ok(IndexTarget::ColumnId(id)) => {
            ("column".to_string(), json!({ "column": id }).to_string())
        }
        _ => ("unknown".to_string(), "unknown".to_string()),
    }
}

fn analyzer_to_str(analyzer: Analyzer) -> &'static str {
    match analyzer {
        Analyzer::English => "English",
        Analyzer::Chinese => "Chinese",
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
        "analyzer": analyzer_to_str(config.analyzer),
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

fn target_key_from_blob<'a>(blob_type: &'a str, prefix: &str) -> Option<&'a str> {
    // Blob types encode their target as "<prefix>-<target>".
    blob_type
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.strip_prefix('-'))
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
