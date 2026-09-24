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
    Array, ArrayRef, BinaryDictionaryBuilder, DictionaryArray, StringArray,
    TimestampMillisecondArray, UInt8Array, UInt32Array, UInt64Array,
};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SkippingIndexOptions};
use datatypes::value::ValueRef;
use index::bitmap::{Bitmap, BitmapType};
use index::bloom_filter::reader::{BloomFilterReader, BloomFilterReaderImpl};
use index::inverted_index::format::reader::{InvertedIndexBlobReader, InvertedIndexReader};
use mito_codec::index::IndexValueCodec;
use mito_codec::row_converter::{
    DensePrimaryKeyCodec, PrimaryKeyCodec, PrimaryKeyCodecExt, SortField,
};
use object_store::ObjectStore;
use object_store::services::Memory;
use prost::Message;
use puffin::puffin_manager::{PuffinManager, PuffinReader};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::{FileId, RegionId};

use super::{IndexBuildType, IndexerBuilder, IndexerBuilderImpl};
use crate::read::BatchBuilder;
use crate::region::options::IndexOptions;
use crate::sst::file::{RegionFileId, RegionIndexId};
use crate::sst::index::bloom_filter::creator::tests::TestPathProvider;
use crate::sst::index::column::column_index_rows;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};

fn input() -> (RegionMetadataRef, RecordBatch, Vec<Vec<u8>>) {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    for (id, name, data_type, semantic_type) in [
        (
            0,
            "tag_str",
            ConcreteDataType::string_datatype(),
            SemanticType::Tag,
        ),
        (
            1,
            "tag_num",
            ConcreteDataType::uint64_datatype(),
            SemanticType::Tag,
        ),
        (
            2,
            "field",
            ConcreteDataType::uint64_datatype(),
            SemanticType::Field,
        ),
        (
            3,
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            SemanticType::Timestamp,
        ),
    ] {
        let mut schema = ColumnSchema::new(name, data_type, id != 3);
        if id != 3 {
            schema.set_inverted_index(true);
            schema
                .set_skipping_options(&SkippingIndexOptions {
                    granularity: 3,
                    ..Default::default()
                })
                .unwrap();
        }
        builder.push_column_metadata(ColumnMetadata {
            column_schema: schema,
            semantic_type,
            column_id: id,
        });
    }
    builder.primary_key(vec![0, 1]);
    let metadata = Arc::new(builder.build().unwrap());
    // Null dictionary keys AND valid keys referencing null dictionary values.
    // Repeated PKs cross batches/segments and recur non-consecutively. Fields vary within PKs.
    let tag_keys = vec![
        Some(2),
        Some(4),
        Some(2),
        Some(4),
        Some(2),
        None,
        Some(0),
        None,
        Some(1),
        Some(1),
        Some(1),
        Some(1),
        Some(1),
        Some(1),
        Some(3),
        Some(2),
    ];
    let labels = [
        None,
        Some(""),
        Some("中文\0abcdefgh"),
        Some("a-longer-string-value"),
        Some("中文\0abcdefgh"),
    ];
    let mut encoded = Vec::new();
    let mut pk = BinaryDictionaryBuilder::<UInt32Type>::new();
    let codec = DensePrimaryKeyCodec::new(&metadata);
    let numbers: Vec<Option<u64>> = tag_keys
        .iter()
        .map(|key| {
            if key.is_none() || *key == Some(0) {
                None
            } else {
                Some(42)
            }
        })
        .collect();
    for (key, number) in tag_keys.iter().zip(&numbers) {
        let label = key.and_then(|key| labels[key as usize]);
        let bytes = codec
            .encode(
                [
                    label.map_or(ValueRef::Null, ValueRef::String),
                    number.map_or(ValueRef::Null, ValueRef::UInt64),
                ]
                .into_iter(),
            )
            .unwrap();
        pk.append(&bytes).unwrap();
        encoded.push(bytes);
    }
    let n = tag_keys.len();
    let tag = DictionaryArray::<UInt32Type>::new(
        UInt32Array::from(tag_keys),
        Arc::new(StringArray::from(labels.to_vec())),
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(tag),
        Arc::new(UInt64Array::from(numbers)),
        Arc::new(UInt64Array::from_iter(
            (0..n).map(|i| (i % 4 != 0).then_some(i as u64)),
        )),
        Arc::new(TimestampMillisecondArray::from_iter_values(0..n as i64)),
        Arc::new(pk.finish()),
        Arc::new(UInt64Array::from(vec![1; n])),
        Arc::new(UInt8Array::from(vec![1; n])),
    ];
    let schema = to_flat_sst_arrow_schema(
        &metadata,
        &FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Dense),
    );
    (
        metadata,
        RecordBatch::try_new(schema, columns).unwrap(),
        encoded,
    )
}

#[tokio::test]
async fn materialized_runs_match_legacy_index_bytes_and_bloom_lookups() {
    let (metadata, batch, encoded) = input();
    let (dir, factory) = PuffinManagerFactory::new_for_test_async("materialized_index_bytes").await;
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
    // Demand is the union of active creators' PK columns, not their counts or
    // all columns in the metadata. A malformed unneeded suffix distinguishes
    // the lazy path from a premature whole-key decode.
    for (inverted, bloom, disable_bloom, all_pk) in [
        (vec![0, 1], vec![], false, true),
        (vec![], vec![0, 1], false, true),
        (vec![0], vec![1], false, true),
        (vec![0, 2], vec![0, 2], false, false),
        (vec![0], vec![1], true, false),
        (vec![2], vec![2], false, false),
    ] {
        let mut case_metadata = RegionMetadataBuilder::new(metadata.region_id);
        for column in &metadata.column_metadatas {
            let mut column = column.clone();
            column
                .column_schema
                .set_inverted_index(inverted.contains(&column.column_id));
            if !bloom.contains(&column.column_id) {
                column.column_schema.unset_skipping_options().unwrap();
            }
            case_metadata.push_column_metadata(column);
        }
        case_metadata.primary_key(metadata.primary_key.clone());
        let mut case_builder = builder.clone();
        case_builder.metadata = Arc::new(case_metadata.build().unwrap());
        if disable_bloom {
            case_builder.bloom_filter_index_config.create_on_flush = crate::config::Mode::Disable;
        }
        let mut indexer = case_builder
            .build(
                RegionFileId::new(metadata.region_id, FileId::random()),
                0,
                None,
            )
            .await;
        let make_batch = |key| {
            let mut batch_builder = BatchBuilder::new(key);
            batch_builder
                .timestamps_array(batch.column(3).slice(0, 1))
                .unwrap();
            batch_builder
                .sequences_array(batch.column(5).slice(0, 1))
                .unwrap();
            batch_builder
                .op_types_array(batch.column(6).slice(0, 1))
                .unwrap();
            batch_builder.build().unwrap()
        };
        let mut valid = make_batch(encoded[0].clone());
        indexer.prepare_primary_key(&mut valid).unwrap();
        if all_pk {
            assert_eq!(
                valid.pk_values(),
                Some(
                    &DensePrimaryKeyCodec::new(&metadata)
                        .decode(&encoded[0])
                        .unwrap()
                )
            );
        } else {
            assert!(valid.pk_values().is_none());
        }
        let mut truncated = make_batch(vec![0, 1]);
        assert_eq!(indexer.prepare_primary_key(&mut truncated).is_err(), all_pk);
        assert!(truncated.pk_values().is_none());
        indexer.abort().await;
        // Aborted creators must not retain their previous all-PK demand.
        indexer.prepare_primary_key(&mut truncated).unwrap();
    }
    // Explicit full decoding is the oracle for lazy encoded-only Batch inputs.
    // One-row slices disable run merging; a tag-only projection also exercises the no-PK fallback.
    for mode in ["eager", "legacy", "lazy", "runs", "single_rows", "no_pk"] {
        let file = RegionFileId::new(metadata.region_id, FileId::random());
        let mut indexer = builder.build(file, 0, None).await;
        if mode == "lazy" {
            indexer.dense_pk_decoder = None;
        }
        match mode {
            "eager" | "legacy" | "lazy" => {
                for (row, key) in encoded.iter().enumerate() {
                    let mut old = BatchBuilder::new(key.clone());
                    old.push_field_array(2, batch.column(2).slice(row, 1))
                        .unwrap();
                    old.timestamps_array(batch.column(3).slice(row, 1)).unwrap();
                    old.sequences_array(batch.column(5).slice(row, 1)).unwrap();
                    old.op_types_array(batch.column(6).slice(row, 1)).unwrap();
                    let mut old = old.build().unwrap();
                    if mode == "eager" {
                        old.set_pk_values(
                            DensePrimaryKeyCodec::new(&metadata).decode(key).unwrap(),
                        );
                    }
                    indexer.update(&mut old).await;
                }
            }
            "single_rows" => {
                for row in 0..batch.num_rows() {
                    indexer.update_flat(&batch.slice(row, 1)).await;
                }
            }
            _ => {
                let batch = if mode == "no_pk" {
                    batch.project(&[0, 1, 2, 3]).unwrap()
                } else {
                    batch.clone()
                };
                for (offset, count) in [(0, 0), (0, 2), (2, 7), (9, 7)] {
                    indexer.update_flat(&batch.slice(offset, count)).await;
                }
            }
        }
        let output = indexer.finish().await;
        assert_eq!(output.inverted_index.row_count, batch.num_rows(), "{mode}");
        assert_eq!(output.bloom_filter.row_count, batch.num_rows(), "{mode}");
        let reader = puffin.reader(&RegionIndexId::new(file, 0)).await.unwrap();
        let blob = reader.blob("greptime-inverted-index-v1").await.unwrap();
        let inverted = InvertedIndexBlobReader::new(blob.reader().await.unwrap());
        let metas = inverted.metadata(None).await.unwrap();
        assert_eq!(metas.metas.len(), 3);
        assert_eq!(metas.total_row_count, batch.num_rows() as u64);
        assert_eq!(metas.segment_row_count, 3);
        // Equality on the empty string must exclude NULL-only segments.
        let string_meta = &metas.metas["0"];
        let fst = inverted
            .fst(
                string_meta.base_offset + string_meta.relative_fst_offset as u64,
                string_meta.fst_size,
                None,
            )
            .await
            .unwrap();
        let [offset, size] = bytemuck::cast::<u64, [u32; 2]>(fst.get(b"").unwrap());
        let bitmap = inverted
            .bitmap(
                string_meta.base_offset + offset as u64,
                size,
                BitmapType::Roaring,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            bitmap,
            Bitmap::from_lsb0_bytes(&[0b0001_1100], BitmapType::Roaring)
        );
        let mut index_bytes = BTreeMap::new();
        // Blob column order is unspecified; compare each column's complete FST and bitmaps.
        for (name, meta) in &metas.metas {
            index_bytes.insert(
                format!("inverted/{name}"),
                inverted
                    .range_read(meta.base_offset, meta.inverted_index_size as u32, None)
                    .await
                    .unwrap(),
            );
        }
        for id in 0..3 {
            let blob = reader
                .blob(&format!("greptime-bloom-filter-v1-{id}"))
                .await
                .unwrap();
            let bloom = BloomFilterReaderImpl::new(blob.reader().await.unwrap());
            let meta = bloom.metadata(None).await.unwrap();
            assert_eq!(meta.row_count, batch.num_rows() as u64);
            assert_eq!(meta.segment_count, 6);
            let mut bytes = bloom
                .range_read(0, meta.bloom_filter_size as u32, None)
                .await
                .unwrap()
                .to_vec();
            bytes.extend_from_slice(&meta.encode_to_vec());
            index_bytes.insert(format!("bloom/{id}"), bytes);
            let vector =
                datatypes::vectors::Helper::try_into_vector(batch.column(id).clone()).unwrap();
            let field = SortField::new(vector.data_type());
            for row in 0..batch.num_rows() {
                let value = vector.get_ref(row);
                if !value.is_null() {
                    let mut bytes = Vec::new();
                    IndexValueCodec::encode_nonnull_value(value, &field, &mut bytes).unwrap();
                    let loc = &meta.bloom_filter_locs[meta.segment_loc_indices[row / 3] as usize];
                    assert!(
                        bloom
                            .bloom_filter(loc, None)
                            .await
                            .unwrap()
                            .contains(&bytes),
                        "{mode}: {id}/{row}"
                    );
                }
            }
        }
        if let Some(expected) = &expected {
            assert_eq!(&index_bytes, expected, "{mode}");
        } else {
            expected = Some(index_bytes);
        }
    }
}

#[test]
fn tag_runs_preserve_sliced_row_positions_and_field_rows() {
    let (_, batch, _) = input();
    let sliced = batch.slice(2, 12);
    assert_eq!(
        column_index_rows(&sliced, SemanticType::Tag).collect::<Vec<_>>(),
        vec![(0, 3), (3, 3), (6, 6)]
    );
    for semantic_type in [SemanticType::Field, SemanticType::Timestamp] {
        assert_eq!(
            column_index_rows(&sliced, semantic_type).collect::<Vec<_>>(),
            (0..12).map(|i| (i, 1)).collect::<Vec<_>>()
        );
    }
}
