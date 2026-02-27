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
use std::vec;

use criterion::{Criterion, criterion_group, criterion_main};
use datatypes::arrow::array::{ArrayRef, Int32Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use partition::PartitionRule;
use partition::expr::{Operand, col};
use partition::multi_dim::MultiDimPartitionRule;
use rand::Rng;
use store_api::storage::RegionNumber;

fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a0", DataType::Int32, false),
        Field::new("a1", DataType::Utf8, false),
        Field::new("a2", DataType::Int32, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

fn create_test_rule(num_columns: usize) -> MultiDimPartitionRule {
    let (columns, exprs) = match num_columns {
        1 => {
            let exprs = vec![
                col("a0").lt(Value::Int32(50)),
                col("a0").gt_eq(Value::Int32(50)),
            ];
            (vec!["a0".to_string()], exprs)
        }
        2 => {
            let exprs = vec![
                col("a0")
                    .lt(Value::Int32(50))
                    .and(col("a1").lt(Value::String("server50".into()))),
                col("a0")
                    .lt(Value::Int32(50))
                    .and(col("a1").gt_eq(Value::String("server50".into()))),
                col("a0")
                    .gt_eq(Value::Int32(50))
                    .and(col("a1").lt(Value::String("server50".into()))),
                col("a0")
                    .gt_eq(Value::Int32(50))
                    .and(col("a1").gt_eq(Value::String("server50".into()))),
            ];
            (vec!["a0".to_string(), "a1".to_string()], exprs)
        }
        3 => {
            let expr = vec![
                col("a0")
                    .lt(Value::Int32(50))
                    .and(col("a1").lt(Value::String("server50".into())))
                    .and(col("a2").lt(Value::Int32(50))),
                col("a0")
                    .lt(Operand::Value(Value::Int32(50)))
                    .and(col("a1").lt(Value::String("server50".into())))
                    .and(col("a2").gt_eq(Value::Int32(50))),
                col("a0")
                    .lt(Value::Int32(50))
                    .and(col("a1").gt_eq(Value::String("server50".into())))
                    .and(col("a2").lt(Value::Int32(50))),
                col("a0")
                    .lt(Value::Int32(50))
                    .and(col("a1").gt_eq(Value::String("server50".into())))
                    .and(col("a2").gt_eq(Value::Int32(50))),
                col("a0")
                    .gt_eq(Value::Int32(50))
                    .and(col("a1").lt(Value::String("server50".into())))
                    .and(col("a2").lt(Value::Int32(50))),
                col("a0")
                    .gt_eq(Operand::Value(Value::Int32(50)))
                    .and(col("a1").lt(Value::String("server50".into())))
                    .and(col("a2").gt_eq(Value::Int32(50))),
                col("a0")
                    .gt_eq(Value::Int32(50))
                    .and(col("a1").gt_eq(Value::String("server50".into())))
                    .and(col("a2").lt(Value::Int32(50))),
                col("a0")
                    .gt_eq(Value::Int32(50))
                    .and(col("a1").gt_eq(Value::String("server50".into())))
                    .and(col("a2").gt_eq(Value::Int32(50))),
            ];

            (
                vec!["a0".to_string(), "a1".to_string(), "a2".to_string()],
                expr,
            )
        }
        _ => {
            panic!("invalid number of columns, only 1-3 are supported");
        }
    };

    let regions = (0..exprs.len()).map(|v| v as u32).collect();
    MultiDimPartitionRule::try_new(columns, regions, exprs, true).unwrap()
}

fn create_test_batch(size: usize) -> RecordBatch {
    let mut rng = rand::thread_rng();

    let schema = table_schema();
    let arrays: Vec<ArrayRef> = (0..3)
        .map(|col_idx| {
            if col_idx % 2 == 0 {
                // Integer columns (a0, a2)
                Arc::new(Int32Array::from_iter_values(
                    (0..size).map(|_| rng.gen_range(0..100)),
                )) as ArrayRef
            } else {
                // String columns (a1)
                let values: Vec<String> = (0..size)
                    .map(|_| {
                        let server_id: i32 = rng.gen_range(0..100);
                        format!("server{}", server_id)
                    })
                    .collect();
                Arc::new(StringArray::from(values)) as ArrayRef
            }
        })
        .chain(std::iter::once({
            // Timestamp column (ts)
            Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..size).map(|idx| idx as i64),
            )) as ArrayRef
        }))
        .collect();
    RecordBatch::try_new(schema, arrays).unwrap()
}

fn bench_split_record_batch_naive_vs_optimized(c: &mut Criterion) {
    let mut group = c.benchmark_group("split_record_batch");

    for num_columns in [1, 2, 3].iter() {
        for num_rows in [100, 1000, 10000, 100000].iter() {
            let rule = create_test_rule(*num_columns);
            let batch = create_test_batch(*num_rows);

            group.bench_function(format!("naive_{}_{}", num_columns, num_rows), |b| {
                b.iter(|| {
                    black_box(rule.split_record_batch_naive(black_box(&batch))).unwrap();
                });
            });
            group.bench_function(format!("optimized_{}_{}", num_columns, num_rows), |b| {
                b.iter(|| {
                    black_box(rule.split_record_batch(black_box(&batch))).unwrap();
                });
            });
        }
    }

    group.finish();
}

fn record_batch_to_rows(
    rule: &MultiDimPartitionRule,
    record_batch: &RecordBatch,
) -> Vec<Vec<Value>> {
    let num_rows = record_batch.num_rows();
    let vectors = rule.record_batch_to_cols(record_batch).unwrap();
    let mut res = Vec::with_capacity(num_rows);
    let mut current_row = vec![Value::Null; vectors.len()];

    for row in 0..num_rows {
        rule.row_at(&vectors, row, &mut current_row).unwrap();
        res.push(current_row.clone());
    }
    res
}

fn find_all_regions(rule: &MultiDimPartitionRule, rows: &[Vec<Value>]) -> Vec<RegionNumber> {
    rows.iter()
        .map(|row| rule.find_region(row).unwrap())
        .collect()
}

fn bench_split_record_batch_vs_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_split_record_batch_vs_row");

    for num_columns in [1, 2, 3].iter() {
        for num_rows in [100, 1000, 10000, 100000].iter() {
            let rule = create_test_rule(*num_columns);
            let batch = create_test_batch(*num_rows);
            let rows = record_batch_to_rows(&rule, &batch);

            group.bench_function(format!("split_by_row_{}_{}", num_columns, num_rows), |b| {
                b.iter(|| {
                    black_box(find_all_regions(&rule, &rows));
                });
            });
            group.bench_function(format!("split_by_col_{}_{}", num_columns, num_rows), |b| {
                b.iter(|| {
                    black_box(rule.split_record_batch(black_box(&batch))).unwrap();
                });
            });
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_split_record_batch_naive_vs_optimized,
    bench_split_record_batch_vs_row
);
criterion_main!(benches);
