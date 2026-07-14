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

//! Tracks the cost of reconstructing and filtering split metric values.
//!
//! Run with:
//! ```sh
//! cargo bench -p mito2 --features test --bench bench_value_split
//! ```

use std::hint::black_box;
use std::sync::Arc;

use common_recordbatch::filter::SimpleFilterEvaluator;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::logical_expr::{col, lit};
use datatypes::arrow::array::{ArrayRef, Float64Array, Int64Array};
use mito2::read::value_split::benchmark_value_split;

const ROWS: usize = 64 * 1024;

fn split_columns() -> (ArrayRef, ArrayRef) {
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

fn value_split(c: &mut Criterion) {
    let (floats, integers) = split_columns();
    let filter =
        SimpleFilterEvaluator::try_new(&col("value").gt_eq(lit((ROWS / 2) as f64))).unwrap();
    let mut group = c.benchmark_group("metric_value_split");
    group.throughput(Throughput::Elements(ROWS as u64));

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
