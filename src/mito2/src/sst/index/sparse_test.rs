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

use std::collections::BTreeMap;
use std::sync::Arc;

use api::v1::SemanticType;
use datatypes::arrow::array::{
    ArrayRef, BinaryDictionaryBuilder, TimestampMillisecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::SkippingIndexOptions;
use datatypes::value::ValueRef;
use index::bloom_filter::reader::{BloomFilterReader, BloomFilterReaderImpl};
use index::inverted_index::format::reader::{InvertedIndexBlobReader, InvertedIndexReader};
use mito_codec::row_converter::{PrimaryKeyCodec, SparsePrimaryKeyCodec};
use object_store::ObjectStore;
use object_store::services::Memory;
use prost::Message;
use puffin::puffin_manager::{PuffinManager, PuffinReader};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::FileId;
use store_api::storage::consts::ReservedColumnId;

use super::{IndexBuildType, IndexerBuilder, IndexerBuilderImpl};
use crate::region::options::IndexOptions;
use crate::sst::file::{RegionFileId, RegionIndexId};
use crate::sst::index::bloom_filter::creator::tests::TestPathProvider;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::sst::parquet::flat_format::FlatReadFormat;
use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use crate::test_util::sst_util::sst_region_metadata_with_encoding;

fn sparse_input() -> (RegionMetadataRef, RecordBatch) {
    let mut metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    for column in &mut metadata.column_metadatas {
        if column.semantic_type == SemanticType::Tag {
            column.column_schema.set_inverted_index(true);
            column
                .column_schema
                .set_skipping_options(&SkippingIndexOptions {
                    granularity: 3,
                    ..Default::default()
                })
                .unwrap();
        }
    }
    let metadata = Arc::new(
        RegionMetadataBuilder::from_existing(metadata)
            .build()
            .unwrap(),
    );
    let codec = SparsePrimaryKeyCodec::new(&metadata);
    let mut keys = BinaryDictionaryBuilder::<UInt32Type>::new();
    for (series, (first, second, count)) in [
        (Some("中文12345678"), Some(""), 5),
        (None, Some("abcdefgh"), 2),
        (Some(""), None, 4),
    ]
    .into_iter()
    .enumerate()
    {
        let mut key = Vec::new();
        codec
            .encode_value_refs(
                &[
                    (ReservedColumnId::table_id(), ValueRef::UInt32(42)),
                    (ReservedColumnId::tsid(), ValueRef::UInt64(series as u64)),
                    (0, first.map_or(ValueRef::Null, ValueRef::String)),
                    (1, second.map_or(ValueRef::Null, ValueRef::String)),
                ],
                &mut key,
            )
            .unwrap();
        for _ in 0..count {
            keys.append(&key).unwrap();
        }
    }
    let schema = to_flat_sst_arrow_schema(
        &metadata,
        &FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Sparse),
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![1; 11])),
        Arc::new(TimestampMillisecondArray::from_iter_values(0..11)),
        Arc::new(keys.finish()),
        Arc::new(UInt64Array::from(vec![1; 11])),
        Arc::new(UInt8Array::from(vec![1; 11])),
    ];
    (metadata, RecordBatch::try_new(schema, columns).unwrap())
}

#[tokio::test]
async fn sparse_and_materialized_tags_produce_identical_indexes() {
    let (metadata, sparse) = sparse_input();
    // Use the independent whole-PK read conversion as the oracle for both index types.
    let materialized = FlatReadFormat::new_with_all_columns(metadata.clone())
        .convert_batch(sparse.clone(), None)
        .unwrap();
    let projection = materialized
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, field)| {
            (field.name() == "tag_0" || sparse.column_by_name(field.name()).is_some()).then_some(i)
        })
        .collect::<Vec<_>>();
    let mixed = materialized.project(&projection).unwrap();

    let (dir, factory) = PuffinManagerFactory::new_for_test_async("sparse_index_bytes").await;
    let puffin = factory.build(
        ObjectStore::new(Memory::default()).unwrap(),
        TestPathProvider,
    );
    let mut options = IndexOptions::default();
    options.inverted_index.segment_row_count = 3;
    let builder = IndexerBuilderImpl {
        build_type: IndexBuildType::Flush,
        metadata: metadata.clone(),
        puffin_manager: puffin.clone(),
        write_cache_enabled: false,
        intermediate_manager: IntermediateManager::init_fs(dir.path().to_str().unwrap())
            .await
            .unwrap(),
        index_options: options,
        inverted_index_config: Default::default(),
        fulltext_index_config: Default::default(),
        bloom_filter_index_config: Default::default(),
    };
    let mut expected = None;
    for batch in [materialized, sparse, mixed] {
        let file = RegionFileId::new(metadata.region_id, FileId::random());
        let mut indexer = builder.build(file, 0, None).await;
        // Split a repeated PK across batches; runs also cross 3-row segment boundaries.
        indexer.update_flat(&batch.slice(0, 2)).await;
        indexer.update_flat(&batch.slice(2, 9)).await;
        let output = indexer.finish().await;
        assert_eq!(output.inverted_index.row_count, 11);
        assert_eq!(output.bloom_filter.row_count, 11);
        assert_eq!(output.inverted_index.columns.len(), 4);
        assert_eq!(output.bloom_filter.columns.len(), 4);

        let reader = puffin.reader(&RegionIndexId::new(file, 0)).await.unwrap();
        let blob = reader.blob("greptime-inverted-index-v1").await.unwrap();
        let inverted = InvertedIndexBlobReader::new(blob.reader().await.unwrap());
        let metas = inverted.metadata(None).await.unwrap();
        let mut index_bytes = BTreeMap::new();
        // Columns can be emitted in a different order; compare each column's FST and bitmaps.
        for (name, meta) in &metas.metas {
            index_bytes.insert(
                format!("inverted/{name}"),
                inverted
                    .range_read(meta.base_offset, meta.inverted_index_size as u32, None)
                    .await
                    .unwrap(),
            );
        }
        for id in &metadata.primary_key {
            let blob = reader
                .blob(&format!("greptime-bloom-filter-v1-{id}"))
                .await
                .unwrap();
            let bloom = BloomFilterReaderImpl::new(blob.reader().await.unwrap());
            let meta = bloom.metadata(None).await.unwrap();
            assert_eq!(meta.row_count, 11);
            assert_eq!(meta.segment_count, 4);
            let mut bytes = bloom
                .range_read(0, meta.bloom_filter_size as u32, None)
                .await
                .unwrap()
                .to_vec();
            bytes.extend_from_slice(&meta.encode_to_vec());
            index_bytes.insert(format!("bloom/{id}"), bytes);
        }
        if let Some(expected) = &expected {
            assert_eq!(&index_bytes, expected);
        } else {
            expected = Some(index_bytes);
        }
    }
}
