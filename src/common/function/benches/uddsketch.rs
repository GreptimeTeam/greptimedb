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

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use common_function::aggrs::approximate::uddsketch::UddSketchState;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Accumulator;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};

const BATCH_SIZES: [usize; 4] = [128, 256, 1024, 2048];
const BUCKET_SIZE: i64 = 128;
const ERROR_RATE: f64 = 0.01;

struct AccumulatorFactory {
    udf: datafusion::logical_expr::AggregateUDF,
    schema: Schema,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    expr_fields: Vec<Arc<Field>>,
    return_field: Arc<Field>,
}

impl AccumulatorFactory {
    fn new() -> Self {
        let udf = UddSketchState::state_udf_impl();
        let schema = Schema::new(vec![
            Field::new("bucket_size", DataType::Int64, false),
            Field::new("error", DataType::Float64, false),
            Field::new("value", DataType::Float64, false),
        ]);
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Literal::new(ScalarValue::Int64(Some(BUCKET_SIZE)))),
            Arc::new(Literal::new(ScalarValue::Float64(Some(ERROR_RATE)))),
            Arc::new(Column::new("value", 2)),
        ];
        let expr_fields = exprs
            .iter()
            .map(|expr| expr.return_field(&schema).unwrap())
            .collect::<Vec<_>>();
        let return_type = udf
            .return_type(&[DataType::Int64, DataType::Float64, DataType::Float64])
            .unwrap();

        Self {
            udf,
            schema,
            exprs,
            expr_fields,
            return_field: Arc::new(Field::new("uddsketch_state", return_type, true)),
        }
    }

    fn create(&self) -> Box<dyn Accumulator> {
        self.udf
            .accumulator(AccumulatorArgs {
                return_field: Arc::clone(&self.return_field),
                schema: &self.schema,
                ignore_nulls: false,
                order_bys: &[],
                is_reversed: false,
                name: "uddsketch_state",
                is_distinct: false,
                exprs: &self.exprs,
                expr_fields: &self.expr_fields,
            })
            .unwrap()
    }
}

fn input_arrays(batch_size: usize) -> Vec<ArrayRef> {
    let bucket_sizes = Arc::new(Int64Array::from_value(BUCKET_SIZE, batch_size)) as ArrayRef;
    let errors = Arc::new(Float64Array::from_value(ERROR_RATE, batch_size)) as ArrayRef;
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let values = (0..batch_size)
        .map(|index| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407)
                .wrapping_add(index as u64);
            let unit = (state >> 11) as f64 * (1.0 / (1_u64 << 53) as f64);
            let magnitude = 10_f64.powf(-9.0 + 18.0 * unit);
            if state & 1 == 0 {
                magnitude
            } else {
                -magnitude
            }
        })
        .collect::<Vec<_>>();
    let values = Arc::new(Float64Array::from(values)) as ArrayRef;

    vec![bucket_sizes, errors, values]
}

fn validate(factory: &AccumulatorFactory, values: &[ArrayRef]) {
    let mut accumulator = factory.create();
    accumulator.update_batch(values).unwrap();
    match accumulator.evaluate().unwrap() {
        ScalarValue::Binary(Some(encoded)) => assert!(!encoded.is_empty()),
        encoded => panic!("expected non-empty Binary, got {encoded:?}"),
    }
}

fn bench_uddsketch(c: &mut Criterion) {
    let factory = AccumulatorFactory::new();
    let inputs = BATCH_SIZES
        .into_iter()
        .map(|batch_size| {
            let values = input_arrays(batch_size);
            validate(&factory, &values);
            (batch_size, values)
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("uddsketch/ingest/fresh");
    for (batch_size, values) in &inputs {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            values,
            |b, values| {
                b.iter(|| {
                    let mut accumulator = factory.create();
                    accumulator.update_batch(black_box(values)).unwrap();
                    black_box(accumulator);
                });
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("uddsketch/ingest/reused");
    for (batch_size, values) in &inputs {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            values,
            |b, values| {
                let mut accumulator = factory.create();
                b.iter(|| {
                    accumulator.update_batch(black_box(values)).unwrap();
                    black_box(&mut accumulator);
                });
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("uddsketch/ingest_evaluate/fresh");
    for (batch_size, values) in &inputs {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            values,
            |b, values| {
                b.iter(|| {
                    let mut accumulator = factory.create();
                    accumulator.update_batch(black_box(values)).unwrap();
                    black_box(accumulator.evaluate().unwrap());
                });
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("uddsketch/ingest_evaluate/reused");
    for (batch_size, values) in &inputs {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            values,
            |b, values| {
                let mut accumulator = factory.create();
                b.iter(|| {
                    accumulator.update_batch(black_box(values)).unwrap();
                    black_box(accumulator.evaluate().unwrap());
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_uddsketch);
criterion_main!(benches);
