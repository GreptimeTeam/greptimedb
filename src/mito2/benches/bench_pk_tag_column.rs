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

//! Measures flat-format tag column extraction from encoded sparse primary keys:
//! `decode_primary_keys` plus one `get_tag_column` call per projected tag.
//! The `pk_tag_filters` group measures precise filtering with 1/2/4/8/16/32
//! predicates over 40 tags, using both one and 32 rows per primary key.
//! Input generation is excluded from the timed section.
//!
//! Run on the baseline revision with `--save-baseline before`, then on the candidate
//! with `--baseline before` (arguments after `--` in the command below):
//! `CARGO_PROFILE_BENCH_DEBUG=0 cargo bench -p mito2 --features testing --bench bench_pk_tag_column`.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use api::v1::SemanticType;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_expr::{col, lit};
use datatypes::arrow::array::{
    ArrayRef, BinaryDictionaryBuilder, TimestampMillisecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;
use mito_codec::row_converter::{
    DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField, SparsePrimaryKeyCodec,
};
use mito2::sst::parquet::flat_format::decode_primary_keys;
use mito2::test_util::bench_util::{pk_materializer_for_bench, tag_filter_for_bench};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::RegionId;
use store_api::storage::consts::ReservedColumnId;

const ROWS: usize = 4096;

struct Shape {
    name: &'static str,
    tags: u32,
    projected_tags: u32,
    rows_per_key: usize,
}

fn metadata(tags: u32) -> RegionMetadataRef {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    let mut primary_key = Vec::new();
    let tag_columns = [
        (
            ReservedColumnId::table_id(),
            "__table_id".into(),
            ConcreteDataType::uint32_datatype(),
        ),
        (
            ReservedColumnId::tsid(),
            "__tsid".into(),
            ConcreteDataType::uint64_datatype(),
        ),
    ]
    .into_iter()
    .chain((0..tags).map(|id| (id, format!("tag_{id}"), ConcreteDataType::string_datatype())));
    for (column_id, name, data_type) in tag_columns {
        primary_key.push(column_id);
        builder.push_column_metadata(ColumnMetadata {
            column_id,
            column_schema: ColumnSchema::new(name, data_type, true),
            semantic_type: SemanticType::Tag,
        });
    }
    builder.push_column_metadata(ColumnMetadata {
        column_id: tags,
        column_schema: ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: SemanticType::Timestamp,
    });
    builder
        .primary_key(primary_key)
        .primary_key_encoding(PrimaryKeyEncoding::Sparse);
    Arc::new(builder.build().unwrap())
}

/// Builds a sparse flat batch whose primary key dictionary holds
/// `ROWS / rows_per_key` distinct keys with `tags` labels each.
fn input(tags: u32, rows_per_key: usize) -> RecordBatch {
    input_with_label_len(tags, rows_per_key, 24)
}

fn input_with_label_len(tags: u32, rows_per_key: usize, label_len: usize) -> RecordBatch {
    let codec = SparsePrimaryKeyCodec::schemaless();
    let mut keys = BinaryDictionaryBuilder::<UInt32Type>::new();
    for series in 0..ROWS / rows_per_key {
        let mut key = Vec::new();
        codec
            .encode_internal((series / 128) as u32, series as u64, &mut key)
            .unwrap();
        let labels: Vec<_> = (0..tags)
            .map(|id| {
                let mut value = format!("tag-{id:03}-value-{series:010}");
                value.extend(std::iter::repeat_n(
                    'x',
                    label_len.saturating_sub(value.len()),
                ));
                (id, value)
            })
            .collect();
        codec
            .encode_raw_tag_value(
                labels.iter().map(|(id, value)| (*id, value.as_bytes())),
                &mut key,
            )
            .unwrap();
        for _ in 0..rows_per_key {
            keys.append(&key).unwrap();
        }
    }
    RecordBatch::try_from_iter([
        (
            "ts",
            Arc::new(TimestampMillisecondArray::from_iter_values(0..ROWS as i64)) as ArrayRef,
        ),
        ("__primary_key", Arc::new(keys.finish()) as ArrayRef),
        (
            "__sequence",
            Arc::new(UInt64Array::from(vec![1; ROWS])) as ArrayRef,
        ),
        (
            "__op_type",
            Arc::new(UInt8Array::from(vec![1; ROWS])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn bench_pk_tag_column(c: &mut Criterion) {
    let codec = SparsePrimaryKeyCodec::schemaless();
    let string_type = ConcreteDataType::string_datatype();
    let mut group = c.benchmark_group("pk_tag_column");
    for shape in [
        Shape {
            name: "1of40tags_1rpk",
            tags: 40,
            projected_tags: 1,
            rows_per_key: 1,
        },
        Shape {
            name: "1of40tags_32rpk",
            tags: 40,
            projected_tags: 1,
            rows_per_key: 32,
        },
        Shape {
            name: "10of40tags_32rpk",
            tags: 40,
            projected_tags: 10,
            rows_per_key: 32,
        },
        Shape {
            name: "40of40tags_32rpk",
            tags: 40,
            projected_tags: 40,
            rows_per_key: 32,
        },
        Shape {
            name: "1of10tags_1rpk",
            tags: 10,
            projected_tags: 1,
            rows_per_key: 1,
        },
    ] {
        let batch = input(shape.tags, shape.rows_per_key);
        // Project the last `projected_tags` tag columns.
        let projected: Vec<u32> = (shape.tags - shape.projected_tags..shape.tags).collect();
        group.bench_function(shape.name, |b| {
            b.iter(|| {
                let mut decoded = decode_primary_keys(&codec, black_box(&batch)).unwrap();
                for &column_id in &projected {
                    black_box(
                        decoded
                            .get_tag_column(column_id, None, &string_type)
                            .unwrap(),
                    );
                }
            });
        });

        let columns: Vec<_> = projected
            .iter()
            .map(|&column_id| (column_id, string_type.clone()))
            .collect();
        group.bench_function(format!("{}/one_pass", shape.name).as_str(), |b| {
            b.iter(|| {
                let mut decoded = decode_primary_keys(&codec, black_box(&batch)).unwrap();
                black_box(decoded.get_sparse_tag_columns(black_box(&columns)).unwrap());
            });
        });
    }
    group.finish();
}

fn bench_pk_tag_filters(c: &mut Criterion) {
    const TAGS: u32 = 40;
    let metadata = metadata(TAGS);
    let mut group = c.benchmark_group("pk_tag_filters");
    for (rows_per_key, label_len) in [(1, 24), (32, 24), (1, 1024), (32, 1024)] {
        let batch = input_with_label_len(TAGS, rows_per_key, label_len);
        for predicate_count in [1, 2, 4, 8, 16, 32] {
            // Use distinct tags at the end of the key to exercise offset discovery.
            // Every row matches, so increasing the predicate count does not change selectivity.
            let filters: Vec<_> = (TAGS - predicate_count..TAGS)
                .map(|id| col(format!("tag_{id}")).gt_eq(lit("")))
                .collect();
            let filter = tag_filter_for_bench(metadata.clone(), &filters);
            // Validate the workload outside the timed section.
            assert_eq!(filter(batch.clone()).unwrap().unwrap().num_rows(), ROWS);
            group.bench_function(
                BenchmarkId::new(
                    format!("{rows_per_key}rpk_{label_len}bytes"),
                    predicate_count,
                ),
                |b| b.iter(|| black_box(filter(black_box(batch.clone())).unwrap())),
            );
        }
    }
    group.finish();
}

/// Measures encoded-only Dense inputs, including the cost of discovering offsets.
fn bench_dense_pk_tag_column(c: &mut Criterion) {
    const TAGS: usize = 40;
    let mut group = c.benchmark_group("dense_pk_tag_column");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    for numeric in [false, true] {
        let ty = if numeric {
            ConcreteDataType::uint64_datatype()
        } else {
            ConcreteDataType::string_datatype()
        };
        let codec = DensePrimaryKeyCodec::with_fields(
            (0..TAGS)
                .map(|id| (id as u32, SortField::new(ty.clone())))
                .collect(),
        );
        let mut metadata = RegionMetadataBuilder::new(RegionId::new(1, 1));
        for id in 0..TAGS {
            metadata.push_column_metadata(ColumnMetadata {
                column_id: id as u32,
                column_schema: ColumnSchema::new(format!("tag_{id}"), ty.clone(), true),
                semantic_type: SemanticType::Tag,
            });
        }
        metadata
            .push_column_metadata(ColumnMetadata {
                column_id: TAGS as u32,
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
            })
            .primary_key((0..TAGS as u32).collect());
        let metadata = Arc::new(metadata.build().unwrap());
        for rows_per_key in [1, 32] {
            let mut keys = BinaryDictionaryBuilder::<UInt32Type>::new();
            for series in 0..ROWS / rows_per_key {
                let values: Vec<_> = (0..TAGS)
                    .map(|id| {
                        if numeric {
                            Value::UInt64((series + id) as u64)
                        } else {
                            Value::from(format!(
                                "tag-{id:03}-value-{series:010}-abcdefghijklmnopqrstuvwxyz"
                            ))
                        }
                    })
                    .collect();
                let pk = codec
                    .encode(values.iter().map(Value::as_value_ref))
                    .unwrap();
                for _ in 0..rows_per_key {
                    keys.append(&pk).unwrap();
                }
            }
            let batch = RecordBatch::try_from_iter([
                (
                    "ts",
                    Arc::new(TimestampMillisecondArray::from_iter_values(0..ROWS as i64))
                        as ArrayRef,
                ),
                ("__primary_key", Arc::new(keys.finish()) as ArrayRef),
                (
                    "__sequence",
                    Arc::new(UInt64Array::from(vec![1; ROWS])) as ArrayRef,
                ),
                (
                    "__op_type",
                    Arc::new(UInt8Array::from(vec![1; ROWS])) as ArrayRef,
                ),
            ])
            .unwrap();
            for (projection, start, count) in [
                ("first", 0, 1),
                ("last", 39, 1),
                ("4last", 36, 4),
                ("all", 0, 40),
            ] {
                let kind = if numeric { "numeric" } else { "string" };
                let materialize = pk_materializer_for_bench(
                    metadata.clone(),
                    (start as u32..(start + count) as u32)
                        .chain([TAGS as u32])
                        .collect(),
                    batch.schema(),
                );
                assert_eq!(materialize(batch.clone()).unwrap().num_columns(), count + 4);
                group.bench_function(format!("{kind}_{projection}_{rows_per_key}rpk"), |b| {
                    b.iter(|| {
                        black_box(materialize(black_box(batch.clone())).unwrap());
                    });
                });
            }
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_pk_tag_column,
    bench_pk_tag_filters,
    bench_dense_pk_tag_column
);
criterion_main!(benches);
