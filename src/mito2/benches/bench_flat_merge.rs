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

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datatypes::arrow::array::builder::BinaryDictionaryBuilder;
use datatypes::arrow::array::{
    ArrayRef, Float64Array, TimestampMillisecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use mito2::memtable::BoxedRecordBatchIterator;
use mito2::read::flat_merge::FlatMergeIterator;

struct Shape {
    name: &'static str,
    num_iters: usize,
    rows_per_iter: usize,
    rows_per_series: usize,
    num_pk_tags: u32,
}

const TABLE_ID: u32 = 1024;

fn make_common_tag_suffix(codec: &SparsePrimaryKeyCodec, num_pk_tags: u32) -> Vec<u8> {
    let mut suffix = Vec::new();
    codec
        .encode_raw_tag_value(
            (1..num_pk_tags).map(|column_id| (column_id + 1, b"tagvalue".as_slice())),
            &mut suffix,
        )
        .unwrap();
    suffix
}

fn make_key(
    codec: &SparsePrimaryKeyCodec,
    series: u64,
    num_pk_tags: u32,
    common_tag_suffix: &[u8],
) -> Vec<u8> {
    let mut key = Vec::new();
    codec.encode_internal(TABLE_ID, series, &mut key).unwrap();
    if num_pk_tags > 0 {
        let series_tag = series.to_be_bytes();
        codec
            .encode_raw_tag_value(std::iter::once((1, series_tag.as_slice())), &mut key)
            .unwrap();
        key.extend_from_slice(common_tag_suffix);
    }
    key
}

fn build_input(shape: &Shape) -> (SchemaRef, Vec<RecordBatch>) {
    let fields = vec![
        Field::new("value", DataType::Float64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "__primary_key",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
            false,
        ),
        Field::new("__sequence", DataType::UInt64, false),
        Field::new("__op_type", DataType::UInt8, false),
    ];
    let schema = Arc::new(Schema::new(fields));
    assert_eq!(
        schema.fields().len(),
        5,
        "sparse SST input must not contain raw tag columns"
    );

    let num_series = shape.rows_per_iter / shape.rows_per_series;
    let num_rows = num_series * shape.rows_per_series;
    let codec = SparsePrimaryKeyCodec::schemaless();
    let common_tag_suffix = make_common_tag_suffix(&codec, shape.num_pk_tags);
    let keys: Vec<_> = (0..num_series as u64)
        .map(|series| make_key(&codec, series, shape.num_pk_tags, &common_tag_suffix))
        .collect();
    for series in [0, keys.len() - 1] {
        let key = &keys[series];
        let (table_id, tsid) = codec
            .decode_ids(key)
            .expect("benchmark primary keys must use sparse encoding");
        assert_eq!(TABLE_ID, table_id);
        assert_eq!(series as u64, tsid);
    }
    let mut batches = Vec::with_capacity(shape.num_iters);
    for iter_idx in 0..shape.num_iters {
        let mut primary_key = BinaryDictionaryBuilder::<UInt32Type>::new();
        let mut timestamps = Vec::with_capacity(num_rows);
        for key in &keys {
            for row in 0..shape.rows_per_series {
                primary_key.append(key).unwrap();
                timestamps.push(((iter_idx * shape.rows_per_series + row) as i64) * 1000);
            }
        }

        let mut columns = Vec::with_capacity(schema.fields().len());
        columns.push(Arc::new(Float64Array::from(vec![1.0; num_rows])) as ArrayRef);
        columns.push(Arc::new(TimestampMillisecondArray::from(timestamps)) as ArrayRef);
        columns.push(Arc::new(primary_key.finish()) as ArrayRef);
        columns.push(Arc::new(UInt64Array::from(vec![1; num_rows])) as ArrayRef);
        columns.push(Arc::new(UInt8Array::from(vec![1; num_rows])) as ArrayRef);

        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        batches.push(batch);
    }

    (schema, batches)
}

fn run_merge(
    schema: SchemaRef,
    iters: Vec<BoxedRecordBatchIterator>,
    expected_rows: usize,
) -> usize {
    let iter = FlatMergeIterator::new(schema, iters, 8192).unwrap();
    let output_rows = iter.map(|batch| batch.unwrap().num_rows()).sum();
    assert_eq!(expected_rows, output_rows);
    black_box(output_rows)
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("flat_merge");
    group.sample_size(10);

    let shapes = [
        Shape {
            name: "sparse_1rps_32way_40tags",
            num_iters: 32,
            rows_per_iter: 100_000,
            rows_per_series: 1,
            num_pk_tags: 40,
        },
        Shape {
            name: "sparse_64rps_32way_40tags",
            num_iters: 32,
            rows_per_iter: 100_000,
            rows_per_series: 64,
            num_pk_tags: 40,
        },
        Shape {
            name: "sparse_1rps_32way_0tags",
            num_iters: 32,
            rows_per_iter: 100_000,
            rows_per_series: 1,
            num_pk_tags: 0,
        },
        Shape {
            name: "sparse_1rps_8way_40tags",
            num_iters: 8,
            rows_per_iter: 400_000,
            rows_per_series: 1,
            num_pk_tags: 40,
        },
        Shape {
            name: "single_iter_1rps_40tags",
            num_iters: 1,
            rows_per_iter: 3_200_000,
            rows_per_series: 1,
            num_pk_tags: 40,
        },
        // Rows-per-series sweep, all 32-way with 40 encoded tags.
        Shape {
            name: "sweep_1rps_32way_40tags",
            num_iters: 32,
            rows_per_iter: 100_000,
            rows_per_series: 1,
            num_pk_tags: 40,
        },
        Shape {
            name: "sweep_10rps_32way_40tags",
            num_iters: 32,
            rows_per_iter: 100_000,
            rows_per_series: 10,
            num_pk_tags: 40,
        },
        Shape {
            name: "sweep_100rps_32way_40tags",
            num_iters: 32,
            rows_per_iter: 100_000,
            rows_per_series: 100,
            num_pk_tags: 40,
        },
        Shape {
            name: "sweep_1000rps_32way_40tags",
            num_iters: 32,
            rows_per_iter: 100_000,
            rows_per_series: 1000,
            num_pk_tags: 40,
        },
        Shape {
            name: "sweep_10000rps_32way_40tags",
            num_iters: 32,
            rows_per_iter: 100_000,
            rows_per_series: 10000,
            num_pk_tags: 40,
        },
    ];

    let selected_shape = std::env::var("FLAT_MERGE_BENCH_SHAPE").ok();
    for shape in &shapes {
        if selected_shape
            .as_deref()
            .is_some_and(|selected| !shape.name.starts_with(selected))
        {
            continue;
        }
        let (schema, batches) = build_input(shape);
        let expected_rows =
            shape.num_iters * (shape.rows_per_iter / shape.rows_per_series) * shape.rows_per_series;
        group.bench_function(BenchmarkId::from_parameter(shape.name), |b| {
            b.iter_batched(
                || {
                    let iters = batches
                        .iter()
                        .cloned()
                        .map(|batch| {
                            Box::new(std::iter::once(Ok(batch))) as BoxedRecordBatchIterator
                        })
                        .collect();
                    (Arc::clone(&schema), iters)
                },
                |(schema, iters)| run_merge(schema, iters, expected_rows),
                criterion::BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
