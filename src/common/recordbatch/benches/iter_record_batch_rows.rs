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
use datafusion::arrow::array::{Int32Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion_common::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion_common::arrow::datatypes::Schema;
use datafusion_common::{ScalarValue, utils};
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::{
    Int32Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};

fn prepare_record_batch(rows: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("i", DataType::Int32, true),
        Field::new("s", DataType::Utf8, true),
    ]);

    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampMillisecondArray::from_iter_values(
            (0..rows).map(|x| (1760313600000 + x) as i64),
        )),
        Arc::new(Int32Array::from_iter_values((0..rows).map(|x| x as i32))),
        Arc::new(StringArray::from_iter((0..rows).map(|x| {
            if x % 2 == 0 {
                Some(format!("s_{x}"))
            } else {
                None
            }
        }))),
    ];

    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn iter_by_loop_rows_and_columns(record_batch: RecordBatch) {
    for i in 0..record_batch.num_rows() {
        for column in record_batch.columns() {
            match column.data_type() {
                DataType::Timestamp(time_unit, _) => {
                    let v = match time_unit {
                        TimeUnit::Second => {
                            let array = column.as_primitive::<TimestampSecondType>();
                            array.value(i)
                        }
                        TimeUnit::Millisecond => {
                            let array = column.as_primitive::<TimestampMillisecondType>();
                            array.value(i)
                        }
                        TimeUnit::Microsecond => {
                            let array = column.as_primitive::<TimestampMicrosecondType>();
                            array.value(i)
                        }
                        TimeUnit::Nanosecond => {
                            let array = column.as_primitive::<TimestampNanosecondType>();
                            array.value(i)
                        }
                    };
                    black_box(v);
                }
                DataType::Int32 => {
                    let array = column.as_primitive::<Int32Type>();
                    let v = array.value(i);
                    black_box(v);
                }
                DataType::Utf8 => {
                    let array = column.as_string::<i32>();
                    let v = array.value(i);
                    black_box(v);
                }
                _ => unreachable!(),
            }
        }
    }
}

fn iter_by_datafusion_scalar_values(record_batch: RecordBatch) {
    let columns = record_batch.columns();
    for i in 0..record_batch.num_rows() {
        let row = utils::get_row_at_idx(columns, i).unwrap();
        black_box(row);
    }
}

fn iter_by_datafusion_scalar_values_with_buf(record_batch: RecordBatch) {
    let columns = record_batch.columns();
    let mut buf = vec![ScalarValue::Null; columns.len()];
    for i in 0..record_batch.num_rows() {
        utils::extract_row_at_idx_to_buf(columns, i, &mut buf).unwrap();
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_record_batch");

    for rows in [1usize, 10, 100, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("by_loop_rows_and_columns", rows),
            &rows,
            |b, rows| {
                let record_batch = prepare_record_batch(*rows);
                b.iter(|| {
                    iter_by_loop_rows_and_columns(record_batch.clone());
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("by_datafusion_scalar_values", rows),
            &rows,
            |b, rows| {
                let record_batch = prepare_record_batch(*rows);
                b.iter(|| {
                    iter_by_datafusion_scalar_values(record_batch.clone());
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("by_datafusion_scalar_values_with_buf", rows),
            &rows,
            |b, rows| {
                let record_batch = prepare_record_batch(*rows);
                b.iter(|| {
                    iter_by_datafusion_scalar_values_with_buf(record_batch.clone());
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
