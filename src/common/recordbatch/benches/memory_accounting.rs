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
use datafusion::arrow::array::{BinaryViewArray, StringViewArray};
use datafusion::arrow::buffer::NullBuffer;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{BinaryVector, Int32Vector, StringVector, UInt64Vector, VectorRef};

const LARGE_ROWS: usize = 8_192;

#[derive(Clone, Copy)]
enum NullShape {
    None,
    All,
    Mixed,
}

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
                (0..rows).map(|v| v as i32).collect::<Vec<_>>(),
            )),
        ),
        (
            "u64",
            ConcreteDataType::uint64_datatype(),
            false,
            Arc::new(UInt64Vector::from_slice(
                (0..rows).map(|v| v as u64).collect::<Vec<_>>(),
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

fn string_view_batch(
    rows: usize,
    value: &str,
    null_shape: NullShape,
    columns: usize,
) -> RecordBatch {
    let column_names = [
        "string_view_0",
        "string_view_1",
        "string_view_2",
        "string_view_3",
    ];
    let columns = (0..columns)
        .map(|column| {
            let values = if value.len() <= 12 {
                vec![value.to_string(); rows]
            } else {
                (0..rows)
                    .map(|row| format!("{value}-{column}-{row:08}"))
                    .collect()
            };
            let array = StringViewArray::from(values);
            let array = if matches!(null_shape, NullShape::None) {
                array
            } else {
                let (views, buffers, _) = array.into_parts();
                let nulls = match null_shape {
                    NullShape::None => unreachable!(),
                    NullShape::All => NullBuffer::new_null(rows),
                    NullShape::Mixed => {
                        NullBuffer::from((0..rows).map(|row| row % 5 != 0).collect::<Vec<_>>())
                    }
                };
                StringViewArray::new(views, buffers, Some(nulls))
            };
            (
                column_names[column],
                ConcreteDataType::utf8_view_datatype(),
                !matches!(null_shape, NullShape::None),
                Arc::new(StringVector::from(array)) as VectorRef,
            )
        })
        .collect();
    batch(columns)
}

fn binary_view_batch(rows: usize, value: &[u8], null_shape: NullShape) -> RecordBatch {
    let values = if value.len() <= 12 {
        vec![value.to_vec(); rows]
    } else {
        (0..rows)
            .map(|row| {
                let mut bytes = value.to_vec();
                bytes.extend_from_slice(&(row as u64).to_le_bytes());
                bytes
            })
            .collect()
    };
    let refs = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let array = BinaryViewArray::from(refs);
    let array = if matches!(null_shape, NullShape::None) {
        array
    } else {
        let (views, buffers, _) = array.into_parts();
        let nulls = match null_shape {
            NullShape::None => unreachable!(),
            NullShape::All => NullBuffer::new_null(rows),
            NullShape::Mixed => {
                NullBuffer::from((0..rows).map(|row| row % 5 != 0).collect::<Vec<_>>())
            }
        };
        BinaryViewArray::new(views, buffers, Some(nulls))
    };
    batch(vec![(
        "binary_view",
        ConcreteDataType::binary_view_datatype(),
        !matches!(null_shape, NullShape::None),
        Arc::new(BinaryVector::from(array)),
    )])
}

fn bench_one_call(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_accounting/one_call");
    for rows in [1, 128, LARGE_ROWS] {
        let cases = [
            ("primitive_string", primitive_string_batch(rows)),
            (
                "utf8_view_inline",
                string_view_batch(rows, "x", NullShape::None, 1),
            ),
            (
                "utf8_view_out_of_line",
                string_view_batch(rows, "out-of-line-payload", NullShape::None, 1),
            ),
            (
                "utf8_view_all_null_out_of_line",
                string_view_batch(rows, "null-out-of-line-payload", NullShape::All, 1),
            ),
            (
                "utf8_view_mixed_null_out_of_line",
                string_view_batch(rows, "mixed-null-out-of-line-payload", NullShape::Mixed, 1),
            ),
            (
                "binary_view_inline",
                binary_view_batch(rows, b"x", NullShape::None),
            ),
            (
                "binary_view_out_of_line",
                binary_view_batch(rows, b"out-of-line-payload", NullShape::None),
            ),
            (
                "binary_view_all_null_out_of_line",
                binary_view_batch(rows, b"null-out-of-line-payload", NullShape::All),
            ),
            (
                "binary_view_mixed_null_out_of_line",
                binary_view_batch(rows, b"mixed-null-out-of-line-payload", NullShape::Mixed),
            ),
        ];
        for (case, batch) in cases {
            group.bench_function(BenchmarkId::new("buffer", format!("{case}/{rows}")), |b| {
                b.iter(|| black_box(batch.buffer_memory_size()))
            });
            group.bench_function(BenchmarkId::new("logical", format!("{case}/{rows}")), |b| {
                b.iter(|| black_box(batch.logical_slice_memory_size()))
            });
        }
    }
    group.finish();
}

fn bench_view_column_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_accounting/view_column_scaling");
    for rows in [128, LARGE_ROWS] {
        for columns in [1, 4] {
            let batch = string_view_batch(rows, "out-of-line-payload", NullShape::None, columns);
            group.bench_function(BenchmarkId::new(format!("{columns}_columns"), rows), |b| {
                b.iter(|| black_box(batch.logical_slice_memory_size()))
            });
        }
    }
    group.finish();
}

fn bench_pipeline_double_call(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_accounting/pipeline_double_call");
    for rows in [128, LARGE_ROWS] {
        let batch = string_view_batch(rows, "out-of-line-payload", NullShape::None, 1);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &batch, |b, batch| {
            b.iter(|| {
                black_box(batch.logical_slice_memory_size());
                black_box(batch.logical_slice_memory_size())
            })
        });
    }
    group.finish();
}

fn bench_shared_buffer_slices(c: &mut Criterion) {
    let backing = string_view_batch(LARGE_ROWS, "shared-out-of-line-payload", NullShape::None, 1);
    let mut group = c.benchmark_group("memory_accounting/shared_buffer_slices");
    for slice_len in [1, 16] {
        let slices = (0..512)
            .map(|index| backing.slice(index * slice_len, slice_len).unwrap())
            .collect::<Vec<_>>();
        group.throughput(Throughput::Elements(slices.len() as u64));
        group.bench_function(BenchmarkId::new("buffer", slice_len), |b| {
            b.iter(|| {
                let total = slices
                    .iter()
                    .map(RecordBatch::buffer_memory_size)
                    .fold(0usize, usize::saturating_add);
                black_box(total)
            })
        });
        group.bench_function(BenchmarkId::new("logical", slice_len), |b| {
            b.iter(|| {
                let total = slices
                    .iter()
                    .map(RecordBatch::logical_slice_memory_size)
                    .fold(0usize, usize::saturating_add);
                black_box(total)
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_one_call,
    bench_view_column_scaling,
    bench_pipeline_double_call,
    bench_shared_buffer_slices
);
criterion_main!(benches);
