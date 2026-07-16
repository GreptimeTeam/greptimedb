// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tracks the cost of splitting, reconstructing, and filtering metric values.
//!
//! Run with `cargo bench -p metric-engine --features test --bench bench_value_split`.

use std::hint::black_box;
use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType};
use common_recordbatch::filter::SimpleFilterEvaluator;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::logical_expr::{col, lit};
use datatypes::arrow::array::{ArrayRef, Float64Array, Int64Array};
use metric_engine::{benchmark_split_metric_values, benchmark_value_split};
use store_api::metric_engine_consts::DATA_SCHEMA_VALUE_INT_COLUMN_PREFIX;

const ROWS: usize = 64 * 1024;

fn split_arrays() -> (ArrayRef, ArrayRef) {
    let mut floats = Vec::with_capacity(ROWS);
    let mut integers = Vec::with_capacity(ROWS);
    for row in 0..ROWS {
        if row % 2 == 0 {
            floats.push(None);
            integers.push(Some(row as i64));
        } else {
            floats.push(Some(row as f64 + 0.5));
            integers.push(None);
        }
    }

    (
        Arc::new(Float64Array::from(floats)),
        Arc::new(Int64Array::from(integers)),
    )
}

fn write_rows() -> Rows {
    let value_column = "value";
    Rows {
        schema: vec![
            ColumnSchema {
                column_name: value_column.to_string(),
                datatype: ColumnDataType::Float64 as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: format!("{DATA_SCHEMA_VALUE_INT_COLUMN_PREFIX}{value_column}"),
                datatype: ColumnDataType::Int64 as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
        ],
        rows: (0..ROWS)
            .map(|row| Row {
                values: vec![
                    ValueData::F64Value(if row % 20 == 0 {
                        row as f64 + 0.5
                    } else {
                        row as f64
                    })
                    .into(),
                    Default::default(),
                ],
            })
            .collect(),
    }
}

fn value_split(c: &mut Criterion) {
    let (floats, integers) = split_arrays();
    let filter =
        SimpleFilterEvaluator::try_new(&col("value").gt_eq(lit((ROWS / 2) as f64))).unwrap();
    let mut group = c.benchmark_group("metric_value_split");
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("write", |b| {
        b.iter_batched(
            write_rows,
            |mut rows| {
                benchmark_split_metric_values(black_box(&mut rows));
                black_box(rows);
            },
            BatchSize::LargeInput,
        );
    });
    group.bench_function("coalesce", |b| {
        b.iter(|| {
            black_box(benchmark_value_split(&floats, &integers, None).unwrap());
        });
    });
    group.bench_function("coalesce_and_simple_filter", |b| {
        b.iter(|| {
            black_box(benchmark_value_split(&floats, &integers, Some(&filter)).unwrap());
        });
    });
    group.finish();
}

criterion_group!(benches, value_split);
criterion_main!(benches);
