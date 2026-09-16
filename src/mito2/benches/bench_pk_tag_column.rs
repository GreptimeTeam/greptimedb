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
//! Input generation is excluded from the timed section.
//!
//! Run on the baseline revision with `--save-baseline before`, then on the candidate
//! with `--baseline before` (arguments after `--` in the command below):
//! `CARGO_PROFILE_BENCH_DEBUG=0 cargo bench -p mito2 --features testing --bench bench_pk_tag_column`.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datatypes::arrow::array::{
    ArrayRef, BinaryDictionaryBuilder, TimestampMillisecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use mito2::sst::parquet::flat_format::decode_primary_keys;

const ROWS: usize = 4096;

struct Shape {
    name: &'static str,
    tags: u32,
    projected_tags: u32,
    rows_per_key: usize,
}

/// Builds a sparse flat batch whose primary key dictionary holds
/// `ROWS / rows_per_key` distinct keys with `tags` labels each.
fn input(shape: &Shape) -> RecordBatch {
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
        let batch = input(&shape);
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

criterion_group!(benches, bench_pk_tag_column);
criterion_main!(benches);
