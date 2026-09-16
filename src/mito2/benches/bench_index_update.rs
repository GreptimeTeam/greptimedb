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

//! Measures real index updates, excluding input generation, creator setup and cleanup.
//!
//! Run on the baseline revision with `--save-baseline before`, then on the candidate
//! with `--baseline before` (arguments after `--` in the command below):
//! `CARGO_PROFILE_BENCH_DEBUG=0 cargo bench -p mito2 --features testing --bench bench_index_update`.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use common_test_util::temp_dir::create_temp_dir;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datatypes::arrow::array::{
    ArrayRef, BinaryDictionaryBuilder, TimestampMillisecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SkippingIndexOptions};
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use mito2::sst::index::intermediate::IntermediateManager;
use mito2::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use mito2::test_util::bench_util::{BloomFilterIndexer, InvertedIndexer};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{ColumnId, FileId, RegionId};

const ROWS: usize = 4096;
const SEGMENT_ROWS: usize = 1024;

struct Shape {
    name: &'static str,
    tags: u32,
    indexed_tags: u32,
    inverted: bool,
    bloom: bool,
    rows_per_key: usize,
}

fn input(shape: &Shape) -> (RegionMetadataRef, HashSet<ColumnId>, RecordBatch) {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    let mut pk = vec![ReservedColumnId::table_id(), ReservedColumnId::tsid()];
    let mut inverted_columns = HashSet::new();
    let mut add_tag = |id, name: String, data_type, indexed| {
        let mut schema = ColumnSchema::new(name, data_type, true).with_inverted_index(false);
        if indexed && shape.inverted {
            inverted_columns.insert(id);
        }
        if indexed && shape.bloom {
            schema = schema
                .with_skipping_options(SkippingIndexOptions {
                    granularity: SEGMENT_ROWS as _,
                    ..Default::default()
                })
                .unwrap();
        }
        builder.push_column_metadata(ColumnMetadata {
            column_schema: schema,
            semantic_type: SemanticType::Tag,
            column_id: id,
        });
    };
    add_tag(
        pk[0],
        "__table_id".into(),
        ConcreteDataType::uint32_datatype(),
        shape.indexed_tags == 0,
    );
    add_tag(
        pk[1],
        "__tsid".into(),
        ConcreteDataType::uint64_datatype(),
        false,
    );
    for id in 0..shape.tags {
        // A single indexed label is deliberately last in the key.
        add_tag(
            id,
            format!("tag_{id:03}"),
            ConcreteDataType::string_datatype(),
            id >= shape.tags - shape.indexed_tags,
        );
        pk.push(id);
    }
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: shape.tags,
        })
        .primary_key(pk)
        .primary_key_encoding(PrimaryKeyEncoding::Sparse);
    let metadata = Arc::new(builder.build().unwrap());
    let codec = SparsePrimaryKeyCodec::schemaless();
    let mut keys = BinaryDictionaryBuilder::<UInt32Type>::new();
    for series in 0..ROWS / shape.rows_per_key {
        let mut key = Vec::new();
        codec
            .encode_internal((series / 128) as u32, series as u64, &mut key)
            .unwrap();
        let labels: Vec<_> = (0..shape.tags)
            .map(|id| (id, format!("tag-{id:03}-value-{series:010}")))
            .collect();
        codec
            .encode_raw_tag_value(
                labels.iter().map(|(id, value)| (*id, value.as_bytes())),
                &mut key,
            )
            .unwrap();
        for _ in 0..shape.rows_per_key {
            keys.append(&key).unwrap();
        }
    }
    let schema = to_flat_sst_arrow_schema(
        &metadata,
        &FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Sparse),
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampMillisecondArray::from_iter_values(0..ROWS as i64)),
        Arc::new(keys.finish()),
        Arc::new(UInt64Array::from(vec![1; ROWS])),
        Arc::new(UInt8Array::from(vec![1; ROWS])),
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    (metadata, inverted_columns, batch)
}

fn bench_index_update(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = create_temp_dir("bench_index_update");
    let intermediate = runtime
        .block_on(IntermediateManager::init_fs(dir.path().to_str().unwrap()))
        .unwrap();
    let mut group = c.benchmark_group("sparse_index_update");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(ROWS as u64));
    for shape in [
        Shape {
            name: "table_id_bloom_40tags",
            tags: 40,
            indexed_tags: 0,
            inverted: false,
            bloom: true,
            rows_per_key: 1,
        },
        Shape {
            name: "inverted_one_40tags",
            tags: 40,
            indexed_tags: 1,
            inverted: true,
            bloom: false,
            rows_per_key: 1,
        },
        Shape {
            name: "bloom_one_40tags",
            tags: 40,
            indexed_tags: 1,
            inverted: false,
            bloom: true,
            rows_per_key: 1,
        },
        Shape {
            name: "both_one_40tags",
            tags: 40,
            indexed_tags: 1,
            inverted: true,
            bloom: true,
            rows_per_key: 1,
        },
        Shape {
            name: "both_all_40tags",
            tags: 40,
            indexed_tags: 40,
            inverted: true,
            bloom: true,
            rows_per_key: 1,
        },
        Shape {
            name: "both_all_40tags_8rpk",
            tags: 40,
            indexed_tags: 40,
            inverted: true,
            bloom: true,
            rows_per_key: 8,
        },
        Shape {
            name: "both_one_10tags",
            tags: 10,
            indexed_tags: 1,
            inverted: true,
            bloom: true,
            rows_per_key: 1,
        },
        Shape {
            name: "both_all_10tags",
            tags: 10,
            indexed_tags: 10,
            inverted: true,
            bloom: true,
            rows_per_key: 1,
        },
    ] {
        let (metadata, inverted_columns, batch) = input(&shape);
        group.bench_function(shape.name, |b| {
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let file_id = FileId::random();
                        let mut inverted = shape.inverted.then(|| {
                            InvertedIndexer::new(
                                file_id,
                                &metadata,
                                intermediate.clone(),
                                None,
                                NonZeroUsize::new(SEGMENT_ROWS).unwrap(),
                                inverted_columns.clone(),
                            )
                        });
                        let mut bloom = if shape.bloom {
                            BloomFilterIndexer::new(file_id, &metadata, intermediate.clone(), None)
                                .unwrap()
                        } else {
                            None
                        };
                        let start = Instant::now();
                        if let Some(indexer) = &mut inverted {
                            indexer.update_flat(&batch).await.unwrap();
                        }
                        if let Some(indexer) = &mut bloom {
                            indexer.update_flat(&batch).await.unwrap();
                        }
                        elapsed += start.elapsed();
                        if let Some(indexer) = &mut inverted {
                            indexer.abort().await.unwrap();
                        }
                        if let Some(indexer) = &mut bloom {
                            indexer.abort().await.unwrap();
                        }
                    }
                    elapsed
                })
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_index_update);
criterion_main!(benches);
