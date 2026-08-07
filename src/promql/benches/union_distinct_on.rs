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

//! Benchmarks for `UnionDistinctOnExec` dedup throughput.
//!
//! `UnionDistinctOnExec` streams 1M left rows while hashing each row's
//! (compare_keys, ts) into a 128-bit signature (columnar null-safe double-hash
//! over the raw arrays, no row materialization), then filters the right rows
//! against the accumulated left signature set. These benchmarks measure the
//! end-to-end dedup throughput (rows processed per second) under realistic
//! dedup load: LHS 1M rows + RHS 1M rows, where ~30% of the RHS rows share the
//! exact (compare_keys, ts) of an LHS row and are deduped away.

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ToDFSchema;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use promql::extension_plan::UnionDistinctOn;

/// Number of rows on each input side.
const ROWS_PER_SIDE: usize = 1_000_000;
/// Fraction of RHS rows whose (compare_keys, ts) exactly match an LHS row and
/// are therefore deduped away.
const RHS_DUP_FRACTION: f64 = 0.30;
/// Target number of rows per RecordBatch.
const BATCH_ROWS: usize = 65_536;

fn empty_plan(schema: datafusion::common::DFSchemaRef) -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema,
    })
}

/// Builds the `UnionDistinctOn` logical node the same way the planner does
/// (from the two child logical plans plus compare-key / ts indices), then
/// converts it into its execution plan over the given in-memory sources.
fn build_exec(
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    compare_key_indices: Vec<usize>,
    ts_col_idx: usize,
    lhs: Vec<RecordBatch>,
    rhs: Vec<RecordBatch>,
) -> Arc<dyn ExecutionPlan> {
    let plan = UnionDistinctOn::try_new(
        empty_plan(left_schema.clone().to_dfschema_ref().unwrap()),
        empty_plan(right_schema.clone().to_dfschema_ref().unwrap()),
        compare_key_indices,
        ts_col_idx,
    )
    .unwrap();
    plan.to_execution_plan(
        source_exec(left_schema, lhs),
        source_exec(right_schema, rhs),
    )
}

/// Wraps in-memory batches in a `DataSourceExec`, the same source the unit
/// tests use for `UnionDistinctOnExec`.
fn source_exec(schema: SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    let source = MemorySourceConfig::try_new(&[batches], schema, None).unwrap();
    Arc::new(DataSourceExec::new(Arc::new(source)))
}

/// Splits `columns` (all `num_rows` rows) into `BATCH_ROWS`-sized batches by
/// slicing the arrays, so no data is copied.
fn split_into_batches(schema: SchemaRef, columns: Vec<ArrayRef>) -> Vec<RecordBatch> {
    let num_rows = columns.first().expect("at least one column").len();
    (0..num_rows)
        .step_by(BATCH_ROWS)
        .map(|start| {
            let end = (start + BATCH_ROWS).min(num_rows);
            let sliced = columns
                .iter()
                .map(|column| column.slice(start, end - start))
                .collect::<Vec<_>>();
            RecordBatch::try_new(schema.clone(), sliced).unwrap()
        })
        .collect()
}

/// Builds LHS and RHS batches for a `(ts, tag, value)` schema.
///
/// `tag_len` is the hex width of the generated tags (`8` -> ~8 chars, `64` ->
/// ~64 chars); `nullable` makes ~10% of the tags null. The first
/// `RHS_DUP_FRACTION` RHS rows are exact (tag, ts) copies of the matching LHS
/// rows; the remaining rows use keys that never appear on the LHS.
fn build_simple_input(
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    tag_len: usize,
    nullable: bool,
) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let dup_rows = (ROWS_PER_SIDE as f64 * RHS_DUP_FRACTION) as usize;

    let tag = |i: usize| -> Option<String> {
        if nullable && i.is_multiple_of(10) {
            None
        } else {
            Some(format!("{:0tag_len$x}", i))
        }
    };

    let lhs_ts: Vec<i64> = (0..ROWS_PER_SIDE as i64).collect();
    let lhs_tags: Vec<Option<String>> = (0..ROWS_PER_SIDE).map(tag).collect();
    let lhs_values: Vec<f64> = (0..ROWS_PER_SIDE).map(|i| i as f64).collect();

    // RHS: first `dup_rows` rows duplicate LHS (tag, ts); the rest are new.
    let mut rhs_ts: Vec<i64> = Vec::with_capacity(ROWS_PER_SIDE);
    let mut rhs_tags: Vec<Option<String>> = Vec::with_capacity(ROWS_PER_SIDE);
    rhs_ts.extend(0..dup_rows as i64);
    rhs_tags.extend(lhs_tags.iter().take(dup_rows).cloned());
    rhs_ts.extend((ROWS_PER_SIDE as i64)..((ROWS_PER_SIDE + ROWS_PER_SIDE - dup_rows) as i64));
    rhs_tags.extend((dup_rows..ROWS_PER_SIDE).map(|j| tag(ROWS_PER_SIDE + j)));
    let rhs_values: Vec<f64> = (0..ROWS_PER_SIDE).map(|i| i as f64).collect();

    let lhs_batches = split_into_batches(
        left_schema,
        vec![
            Arc::new(Int64Array::from(lhs_ts)),
            Arc::new(StringArray::from_iter(
                lhs_tags.iter().map(|t| t.as_deref()),
            )),
            Arc::new(Float64Array::from(lhs_values)),
        ],
    );
    let rhs_batches = split_into_batches(
        right_schema,
        vec![
            Arc::new(Int64Array::from(rhs_ts)),
            Arc::new(StringArray::from_iter(
                rhs_tags.iter().map(|t| t.as_deref()),
            )),
            Arc::new(Float64Array::from(rhs_values)),
        ],
    );
    (lhs_batches, rhs_batches)
}

/// Builds LHS and RHS batches for a wide schema with 4 compare keys
/// (`tag1`, `tag2`, `num1`, `num2`) plus the ts column.
fn build_four_key_input(
    left_schema: SchemaRef,
    right_schema: SchemaRef,
) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let dup_rows = (ROWS_PER_SIDE as f64 * RHS_DUP_FRACTION) as usize;

    let lhs_ts: Vec<i64> = (0..ROWS_PER_SIDE as i64).collect();
    let lhs_tag1: Vec<String> = (0..ROWS_PER_SIDE).map(|i| format!("{i:08x}")).collect();
    let lhs_tag2: Vec<String> = (0..ROWS_PER_SIDE)
        .map(|i| format!("{:08x}", 0x1000_0000 + i))
        .collect();
    let lhs_num1: Vec<i64> = (0..ROWS_PER_SIDE).map(|i| (i % 1000) as i64).collect();
    let lhs_num2: Vec<f64> = (0..ROWS_PER_SIDE)
        .map(|i| (i % 1000) as f64 * 0.25)
        .collect();
    let lhs_value: Vec<f64> = (0..ROWS_PER_SIDE).map(|i| i as f64).collect();

    // RHS: first `dup_rows` rows duplicate every LHS key column; the rest are new.
    let mut rhs_ts: Vec<i64> = Vec::with_capacity(ROWS_PER_SIDE);
    let mut rhs_tag1: Vec<String> = Vec::with_capacity(ROWS_PER_SIDE);
    let mut rhs_tag2: Vec<String> = Vec::with_capacity(ROWS_PER_SIDE);
    let mut rhs_num1: Vec<i64> = Vec::with_capacity(ROWS_PER_SIDE);
    let mut rhs_num2: Vec<f64> = Vec::with_capacity(ROWS_PER_SIDE);
    let rhs_value: Vec<f64> = (0..ROWS_PER_SIDE).map(|i| i as f64).collect();

    rhs_ts.extend(lhs_ts.iter().take(dup_rows).copied());
    rhs_tag1.extend(lhs_tag1.iter().take(dup_rows).cloned());
    rhs_tag2.extend(lhs_tag2.iter().take(dup_rows).cloned());
    rhs_num1.extend(lhs_num1.iter().take(dup_rows).copied());
    rhs_num2.extend(lhs_num2.iter().take(dup_rows).copied());

    rhs_ts.extend((ROWS_PER_SIDE as i64)..((ROWS_PER_SIDE + ROWS_PER_SIDE - dup_rows) as i64));
    rhs_tag1.extend((dup_rows..ROWS_PER_SIDE).map(|j| format!("{:08x}", ROWS_PER_SIDE + j)));
    rhs_tag2.extend((dup_rows..ROWS_PER_SIDE).map(|j| format!("{:08x}", 0x2000_0000 + j)));
    rhs_num1.extend((dup_rows..ROWS_PER_SIDE).map(|j| (j % 997) as i64));
    rhs_num2.extend((dup_rows..ROWS_PER_SIDE).map(|j| (j % 997) as f64 * 0.5));

    let lhs_batches = split_into_batches(
        left_schema,
        vec![
            Arc::new(Int64Array::from(lhs_ts)),
            Arc::new(StringArray::from(lhs_tag1)),
            Arc::new(StringArray::from(lhs_tag2)),
            Arc::new(Int64Array::from(lhs_num1)),
            Arc::new(Float64Array::from(lhs_num2)),
            Arc::new(Float64Array::from(lhs_value)),
        ],
    );
    let rhs_batches = split_into_batches(
        right_schema,
        vec![
            Arc::new(Int64Array::from(rhs_ts)),
            Arc::new(StringArray::from(rhs_tag1)),
            Arc::new(StringArray::from(rhs_tag2)),
            Arc::new(Int64Array::from(rhs_num1)),
            Arc::new(Float64Array::from(rhs_num2)),
            Arc::new(Float64Array::from(rhs_value)),
        ],
    );
    (lhs_batches, rhs_batches)
}

fn simple_schemas(
    left_prefix: &str,
    right_prefix: &str,
    tag_nullable: bool,
) -> (SchemaRef, SchemaRef) {
    let make = |prefix: &str| {
        Arc::new(Schema::new(vec![
            Field::new(format!("{prefix}_ts"), DataType::Int64, false),
            Field::new(format!("{prefix}_tag"), DataType::Utf8, tag_nullable),
            Field::new(format!("{prefix}_value"), DataType::Float64, false),
        ]))
    };
    (make(left_prefix), make(right_prefix))
}

fn four_key_schemas() -> (SchemaRef, SchemaRef) {
    let make = |prefix: &str| {
        Arc::new(Schema::new(vec![
            Field::new(format!("{prefix}_ts"), DataType::Int64, false),
            Field::new(format!("{prefix}_tag1"), DataType::Utf8, false),
            Field::new(format!("{prefix}_tag2"), DataType::Utf8, false),
            Field::new(format!("{prefix}_num1"), DataType::Int64, false),
            Field::new(format!("{prefix}_num2"), DataType::Float64, false),
            Field::new(format!("{prefix}_value"), DataType::Float64, false),
        ]))
    };
    (make("left"), make("right"))
}

/// Executes `exec` end to end (execute partition 0 + drain every batch) on the
/// given runtime and returns the total number of output rows, so the dedup
/// result is observable and cannot be optimized away.
///
/// Every scenario feeds 1M LHS rows (all distinct) and 1M RHS rows of which
/// `RHS_DUP_FRACTION` are exact (compare_keys, ts) duplicates of LHS rows. So a
/// correct dedup must emit exactly `1M + (1 - RHS_DUP_FRACTION) * 1M` rows; the
/// assertion turns each timed run into a correctness check.
fn run_dedup(
    exec: &Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    rt: &tokio::runtime::Runtime,
) -> usize {
    let total = rt.block_on(async {
        let mut stream = exec.execute(0, task_ctx).unwrap();
        let mut total = 0usize;
        while let Some(batch) = stream.next().await {
            total += batch.unwrap().num_rows();
        }
        total
    });
    let expected = ROWS_PER_SIDE + (ROWS_PER_SIDE as f64 * (1.0 - RHS_DUP_FRACTION)) as usize;
    assert_eq!(
        total, expected,
        "dedup dropped or kept the wrong number of rows (produced {total}, expected {expected})"
    );
    total
}

fn bench_union_distinct_on_dedup(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("union_distinct_on");
    // Each iteration processes LHS + RHS rows, i.e. 2M rows per run.
    group.sample_size(10);
    group.throughput(Throughput::Elements((2 * ROWS_PER_SIDE) as u64));

    // (a) short tag (~8 chars) + ts: 2 signature columns.
    let (left_schema, right_schema) = simple_schemas("left", "right", false);
    let (lhs, rhs) = build_simple_input(left_schema.clone(), right_schema.clone(), 8, false);
    let exec = build_exec(
        left_schema.clone(),
        right_schema.clone(),
        vec![1],
        0,
        lhs,
        rhs,
    );
    let task_ctx = SessionContext::default().task_ctx();
    group.bench_function("short_tag_2keys", |b| {
        b.iter(|| {
            let total = run_dedup(&exec, task_ctx.clone(), &rt);
            std::hint::black_box(total);
        });
    });

    // (b) long tag (~64 chars) + ts: 2 signature columns.
    let (left_schema, right_schema) = simple_schemas("left", "right", false);
    let (lhs, rhs) = build_simple_input(left_schema.clone(), right_schema.clone(), 64, false);
    let exec = build_exec(
        left_schema.clone(),
        right_schema.clone(),
        vec![1],
        0,
        lhs,
        rhs,
    );
    let task_ctx = SessionContext::default().task_ctx();
    group.bench_function("long_tag_2keys", |b| {
        b.iter(|| {
            let total = run_dedup(&exec, task_ctx.clone(), &rt);
            std::hint::black_box(total);
        });
    });

    // (c) ~10% null tags + ts: 2 signature columns with nulls.
    let (left_schema, right_schema) = simple_schemas("left", "right", true);
    let (lhs, rhs) = build_simple_input(left_schema.clone(), right_schema.clone(), 8, true);
    let exec = build_exec(
        left_schema.clone(),
        right_schema.clone(),
        vec![1],
        0,
        lhs,
        rhs,
    );
    let task_ctx = SessionContext::default().task_ctx();
    group.bench_function("null_tag_2keys", |b| {
        b.iter(|| {
            let total = run_dedup(&exec, task_ctx.clone(), &rt);
            std::hint::black_box(total);
        });
    });

    // (d) 4 compare keys (2 tags + 2 numeric) + ts: 5 signature columns.
    let (left_schema, right_schema) = four_key_schemas();
    let (lhs, rhs) = build_four_key_input(left_schema.clone(), right_schema.clone());
    let exec = build_exec(
        left_schema.clone(),
        right_schema.clone(),
        vec![1, 2, 3, 4],
        0,
        lhs,
        rhs,
    );
    let task_ctx = SessionContext::default().task_ctx();
    group.bench_function("four_keys_plus_ts", |b| {
        b.iter(|| {
            let total = run_dedup(&exec, task_ctx.clone(), &rt);
            std::hint::black_box(total);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_union_distinct_on_dedup);
criterion_main!(benches);
