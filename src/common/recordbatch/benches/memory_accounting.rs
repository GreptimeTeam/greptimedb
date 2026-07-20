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

use common_recordbatch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Int32Vector, StringVector, UInt64Vector, VectorRef};

const LARGE_ROWS: usize = 8_192;

fn batch(columns: Vec<(&str, ConcreteDataType, bool, VectorRef)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, data_type, nullable, _)| {
                ColumnSchema::new(*name, data_type.clone(), *nullable)
            })
            .collect(),
    ));
    let vectors: Vec<VectorRef> = columns
        .into_iter()
        .map(|(_, _, _, vector)| vector)
        .collect();
    RecordBatch::new(schema, vectors).unwrap()
}

fn primitive_string_batch(rows: usize) -> RecordBatch {
    let strings = (0..rows)
        .map(|row| (row % 5 != 0).then(|| format!("value-{row:08}-with-payload")))
        .collect::<Vec<_>>();
    batch(vec![
        (
            "i32",
            ConcreteDataType::int32_datatype(),
            false,
            Arc::new(Int32Vector::from_slice(
                (0..rows).map(|value| value as i32).collect::<Vec<_>>(),
            )),
        ),
        (
            "u64",
            ConcreteDataType::uint64_datatype(),
            false,
            Arc::new(UInt64Vector::from_slice(
                (0..rows).map(|value| value as u64).collect::<Vec<_>>(),
            )),
        ),
        (
            "string",
            ConcreteDataType::string_datatype(),
            true,
            Arc::new(StringVector::from(strings)),
        ),
    ])
}

fn bench_one_call(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_accounting/one_call");
    for rows in [128, LARGE_ROWS] {
        let batch = primitive_string_batch(rows);
        group.bench_function(BenchmarkId::new("buffer", rows), |b| {
            b.iter(|| black_box(black_box(&batch).buffer_memory_size()))
        });
        group.bench_function(BenchmarkId::new("logical", rows), |b| {
            b.iter(|| black_box(black_box(&batch).logical_slice_memory_size()))
        });
    }
    group.finish();
}

fn bench_shared_buffer_slices(c: &mut Criterion) {
    let backing = primitive_string_batch(LARGE_ROWS);
    let mut group = c.benchmark_group("memory_accounting/shared_buffer_slices");
    for slice_len in [1, 16] {
        let slices = (0..512)
            .map(|index| backing.slice(index * slice_len, slice_len).unwrap())
            .collect::<Vec<_>>();
        group.throughput(Throughput::Elements(slices.len() as u64));
        group.bench_function(BenchmarkId::new("buffer", slice_len), |b| {
            b.iter(|| {
                let total = black_box(&slices)
                    .iter()
                    .map(RecordBatch::buffer_memory_size)
                    .fold(0usize, usize::saturating_add);
                black_box(total)
            })
        });
        group.bench_function(BenchmarkId::new("logical", slice_len), |b| {
            b.iter(|| {
                let total = black_box(&slices)
                    .iter()
                    .map(RecordBatch::logical_slice_memory_size)
                    .fold(0usize, usize::saturating_add);
                black_box(total)
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_one_call, bench_shared_buffer_slices);
criterion_main!(benches);
