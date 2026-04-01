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

//! Benchmarks for HistogramFold plan.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group};
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ToDFSchema;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::Column as PhyColumn;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, Partitioning, PlanProperties,
};
use datafusion::prelude::SessionContext;
use datatypes::arrow::array::TimestampMillisecondArray;
use datatypes::arrow_array::StringArray;
use promql::extension_plan::{HistogramFold, HistogramFoldExec};

/// Standard Prometheus histogram bucket bounds.
const STANDARD_BUCKETS: &[&str] = &[
    "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf",
];

/// Build histogram data: `num_series` series, `num_timestamps` timestamps each,
/// each having `bucket_bounds.len()` buckets.
fn build_histogram_input(
    num_series: usize,
    num_timestamps: usize,
    bucket_bounds: &[&str],
) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("le", DataType::Utf8, true),
        Field::new("val", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
    ]));

    let bucket_count = bucket_bounds.len();
    let total_rows = num_series * num_timestamps * bucket_count;

    let mut hosts = Vec::with_capacity(total_rows);
    let mut les = Vec::with_capacity(total_rows);
    let mut vals = Vec::with_capacity(total_rows);
    let mut timestamps = Vec::with_capacity(total_rows);

    for s in 0..num_series {
        let host = format!("host_{}", s);
        for t in 0..num_timestamps {
            let ts = (s * num_timestamps + t) as i64 * 15_000; // 15s intervals
            let mut cumulative = 0.0;
            for (b, _le_str) in bucket_bounds.iter().enumerate() {
                hosts.push(host.clone());
                les.push(bucket_bounds[b].to_string());
                timestamps.push(ts);
                // Monotonically increasing counters per bucket
                cumulative += (b + 1) as f64 * 10.0 + (t % 5) as f64;
                vals.push(cumulative);
            }
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(hosts)) as _,
            Arc::new(StringArray::from(les)) as _,
            Arc::new(Float64Array::from(vals)) as _,
            Arc::new(TimestampMillisecondArray::from(timestamps)) as _,
        ],
    )
    .unwrap();

    Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap(),
    )))
}

fn build_exec(input: Arc<dyn ExecutionPlan>, quantile: f64) -> Arc<dyn ExecutionPlan> {
    let input_schema = input.schema();
    let output_schema: SchemaRef = {
        let df_schema = Arc::new(input_schema.as_ref().clone().to_dfschema().unwrap());
        Arc::new(
            HistogramFold::convert_schema(&df_schema, "le")
                .unwrap()
                .as_arrow()
                .clone(),
        )
    };

    // le=1, val=2, ts=3; tag columns = [host(0)]
    let tag_columns: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> =
        vec![Arc::new(PhyColumn::new("host", 0)) as _];

    let mut partition_exprs = tag_columns.clone();
    partition_exprs.push(Arc::new(PhyColumn::new("ts", 3)) as _);

    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(output_schema.clone()),
        Partitioning::Hash(
            partition_exprs.clone(),
            input.output_partitioning().partition_count(),
        ),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ));

    Arc::new(HistogramFoldExec::new(
        1, // le_column_index
        2, // field_column_index
        3, // ts_column_index
        quantile,
        input,
        output_schema,
        tag_columns,
        partition_exprs,
        properties,
    ))
}

fn run(exec: Arc<dyn ExecutionPlan>, rt: &tokio::runtime::Runtime) {
    let ctx = SessionContext::default();
    rt.block_on(async {
        let result = datafusion::physical_plan::collect(exec, ctx.task_ctx())
            .await
            .unwrap();
        std::hint::black_box(result);
    });
}

fn bench_histogram_fold(c: &mut Criterion) {
    let mut group = c.benchmark_group("histogram_fold");
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Small: 10 series × 10 timestamps × 12 buckets = 1,200 rows → 100 output
    {
        let input = build_histogram_input(10, 10, STANDARD_BUCKETS);
        let exec = build_exec(input, 0.9);
        group.bench_with_input(BenchmarkId::new("small", "10s_10t_12b"), &(), |b, _| {
            b.iter(|| run(exec.clone(), &rt))
        });
    }

    // Medium: 50 series × 100 timestamps × 12 buckets = 60,000 rows → 5,000 output
    {
        let input = build_histogram_input(50, 100, STANDARD_BUCKETS);
        let exec = build_exec(input, 0.9);
        group.bench_with_input(BenchmarkId::new("medium", "50s_100t_12b"), &(), |b, _| {
            b.iter(|| run(exec.clone(), &rt))
        });
    }

    // Large: 100 series × 500 timestamps × 12 buckets = 600,000 rows → 50,000 output
    {
        let input = build_histogram_input(100, 500, STANDARD_BUCKETS);
        let exec = build_exec(input, 0.9);
        group.bench_with_input(BenchmarkId::new("large", "100s_500t_12b"), &(), |b, _| {
            b.iter(|| run(exec.clone(), &rt))
        });
    }

    // Many buckets: 10 series × 100 timestamps × 50 buckets = 50,000 rows
    {
        let mut many_buckets: Vec<&str> = Vec::with_capacity(50);
        let bucket_strs: Vec<String> = (0..49)
            .map(|i| format!("{}", (i + 1) as f64 * 0.5))
            .collect();
        for s in &bucket_strs {
            many_buckets.push(s.as_str());
        }
        many_buckets.push("+Inf");

        let input = build_histogram_input(10, 100, &many_buckets);
        let exec = build_exec(input, 0.5);
        group.bench_with_input(
            BenchmarkId::new("many_buckets", "10s_100t_50b"),
            &(),
            |b, _| b.iter(|| run(exec.clone(), &rt)),
        );
    }

    // Different quantiles on medium data
    for &q in &[0.1, 0.5, 0.9, 0.99] {
        let input = build_histogram_input(50, 100, STANDARD_BUCKETS);
        let exec = build_exec(input, q);
        group.bench_with_input(
            BenchmarkId::new("quantile", format!("q{:.2}", q)),
            &(),
            |b, _| b.iter(|| run(exec.clone(), &rt)),
        );
    }

    group.finish();
}

criterion_group!(benches, bench_histogram_fold);
