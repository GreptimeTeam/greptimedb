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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::{Extension, Form};
use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use datafusion_expr::LogicalPlan;
use datatypes::schema::SchemaRef;
use datatypes::vectors::StringVector;
use futures::stream;
use query::parser::PromQuery;
use query::query_engine::DescribeResult;
use servers::error::Result;
use servers::http::handler::{SqlQuery, sql};
use servers::http::{ApiState, HttpRecordsOutput};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use sql::statements::statement::Statement;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn mock_schema() -> SchemaRef {
    let mut fields = Vec::with_capacity(10);
    for i in 0..10 {
        fields.push(Field::new(format!("field{}", i), DataType::Utf8, true));
    }
    let arrow_schema = Arc::new(Schema::new(fields));
    Arc::new(arrow_schema.try_into().unwrap())
}

fn mock_input_record_batch(batch_size: usize, num_batches: usize) -> Vec<RecordBatch> {
    let mut result = Vec::with_capacity(num_batches);
    for _ in 0..num_batches {
        let mut vectors = Vec::with_capacity(10);
        for _ in 0..10 {
            let vector: StringVector = StringArray::from(
                (0..batch_size)
                    .map(|_| String::from("Copyright 2024 Greptime Team"))
                    .collect::<Vec<_>>(),
            )
            .into();
            vectors.push(Arc::new(vector) as _);
        }

        let schema = mock_schema();
        let record_batch = RecordBatch::new(schema, vectors).unwrap();
        result.push(record_batch);
    }

    result
}

fn bench_convert_record_batch_to_http_output(c: &mut Criterion) {
    let record_batches = mock_input_record_batch(4096, 100);
    c.bench_function("convert_record_batch_to_http_output", |b| {
        b.iter_custom(|iters| {
            let mut elapsed_sum = std::time::Duration::new(0, 0);
            for _ in 0..iters {
                let record_batches = record_batches.clone();
                let start = Instant::now();
                let _result = HttpRecordsOutput::try_new(mock_schema(), record_batches);
                elapsed_sum += start.elapsed();
            }
            elapsed_sum
        });
    });
}

struct StreamingSqlHandler {
    rows: usize,
    schema: SchemaRef,
    consumed: Arc<AtomicUsize>,
}

impl StreamingSqlHandler {
    fn new(rows: usize) -> Self {
        let fields = std::iter::once(Field::new("id", DataType::UInt64, false))
            .chain((1..10).map(|i| Field::new(format!("field{i}"), DataType::Utf8, false)));
        let schema = Arc::new(Schema::new(fields.collect::<Vec<_>>()));
        Self {
            rows,
            schema: Arc::new(schema.try_into().unwrap()),
            consumed: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl SqlQueryHandler for StreamingSqlHandler {
    async fn do_query(&self, _: &str, _: QueryContextRef) -> Vec<Result<Output>> {
        let schema = self.schema.clone();
        let consumed = self.consumed.clone();
        let rows = self.rows;
        consumed.store(0, Ordering::Relaxed);
        // Allocate independent buffers lazily, as a query stream would. Prebuilding
        // all batches or sharing one batch's buffers would hide retention costs.
        let batches = (0..rows).step_by(4096).map(move |offset| {
            let end = (offset + 4096).min(rows);
            let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt64Array::from_iter_values(
                offset as u64..end as u64,
            ))];
            columns.extend((1..10).map(|_| {
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    "0123456789abcdef0123456789abcdef",
                    end - offset,
                ))) as ArrayRef
            }));
            let batch =
                arrow::record_batch::RecordBatch::try_new(schema.arrow_schema().clone(), columns)
                    .unwrap();
            consumed.fetch_add(end - offset, Ordering::Relaxed);
            Ok(RecordBatch::from_df_record_batch(schema.clone(), batch))
        });
        vec![Ok(Output::new_with_stream(Box::pin(
            RecordBatchStreamWrapper::new(self.schema.clone(), stream::iter(batches)),
        )))]
    }

    async fn do_analyze_stream_query(&self, _: &str, _: QueryContextRef) -> Result<Output> {
        unimplemented!()
    }

    async fn do_exec_plan(
        &self,
        _: LogicalPlan,
        _: Option<Statement>,
        _: QueryContextRef,
    ) -> Result<Output> {
        unimplemented!()
    }

    async fn do_promql_query(&self, _: &PromQuery, _: QueryContextRef) -> Vec<Result<Output>> {
        unimplemented!()
    }

    async fn do_describe(
        &self,
        _: Statement,
        _: QueryContextRef,
    ) -> Result<Option<DescribeResult>> {
        unimplemented!()
    }

    async fn is_valid_schema(&self, _: &str, _: &str) -> Result<bool> {
        Ok(true)
    }
}

#[derive(Clone, Copy)]
enum LimitPlacement {
    Late,
    Early,
}

async fn limited_response(
    handler: Arc<StreamingSqlHandler>,
    limit: usize,
    placement: LimitPlacement,
) -> bytes::Bytes {
    let response = sql(
        State(ApiState {
            sql_handler: handler.clone(),
        }),
        Query(SqlQuery {
            sql: Some("select * from benchmark_input".to_string()),
            limit: matches!(placement, LimitPlacement::Early).then_some(limit),
            ..Default::default()
        }),
        Extension(QueryContext::with_db_name(None)),
        Form(SqlQuery::default()),
    )
    .await;
    // Reconstruct the old path with the existing post-conversion truncation API.
    let response = match placement {
        LimitPlacement::Late => response.with_limit(limit),
        LimitPlacement::Early => response,
    };
    let response = response.with_execution_time(0).into_response();
    assert!(response.status().is_success());
    assert_eq!(handler.consumed.load(Ordering::Relaxed), handler.rows);
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
}

fn bench_http_sql_limit(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("http_sql_limit");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    for (rows, limit) in [
        (10_000, 100),
        (100_000, 100),
        (1_000_000, 100),
        (1_000_000, 0),
        (100_000, 100_000),
    ] {
        let handler = Arc::new(StreamingSqlHandler::new(rows));
        {
            let late = runtime.block_on(limited_response(
                handler.clone(),
                limit,
                LimitPlacement::Late,
            ));
            let early = runtime.block_on(limited_response(
                handler.clone(),
                limit,
                LimitPlacement::Early,
            ));
            assert_eq!(late, early);
            let json: serde_json::Value = serde_json::from_slice(&early).unwrap();
            assert_eq!(
                json["output"][0]["records"]["rows"]
                    .as_array()
                    .unwrap()
                    .len(),
                rows.min(limit)
            );
        }
        group.throughput(Throughput::Elements(rows as u64));
        for (name, placement) in [
            ("late", LimitPlacement::Late),
            ("early", LimitPlacement::Early),
        ] {
            group.bench_function(
                BenchmarkId::new(name, format!("rows_{rows}_limit_{limit}")),
                |b| {
                    b.iter(|| {
                        runtime.block_on(limited_response(handler.clone(), limit, placement))
                    });
                },
            );
        }
    }
    group.finish();
}

#[cfg(not(windows))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(pprof::criterion::PProfProfiler::new(101, pprof::criterion::Output::Flamegraph(None)));
    targets = bench_convert_record_batch_to_http_output, bench_http_sql_limit
}
#[cfg(windows)]
criterion_group!(
    benches,
    bench_convert_record_batch_to_http_output,
    bench_http_sql_limit
);

fn main() {
    // Run each memory case in a fresh process, e.g. with `/usr/bin/time -v`:
    // GREPTIME_HTTP_LIMIT_MEMORY_CASE=late,1000000,100 <bench executable>
    if let Ok(case) = std::env::var("GREPTIME_HTTP_LIMIT_MEMORY_CASE") {
        let parts: Vec<_> = case.split(',').collect();
        assert_eq!(parts.len(), 3, "expected late|early,rows,limit");
        let placement = match parts[0] {
            "late" => LimitPlacement::Late,
            "early" => LimitPlacement::Early,
            _ => panic!("expected late or early"),
        };
        let rows = parts[1].parse().unwrap();
        let limit = parts[2].parse().unwrap();
        let handler = Arc::new(StreamingSqlHandler::new(rows));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        #[cfg(not(windows))]
        let allocated = tikv_jemalloc_ctl::thread::allocatedp::read().unwrap();
        #[cfg(not(windows))]
        let before = allocated.get();
        let start = Instant::now();
        let response = runtime.block_on(limited_response(handler, limit, placement));
        let elapsed = start.elapsed();
        // Memory mode writes measurements to stdout for external benchmark tools.
        #[allow(clippy::print_stdout)]
        {
            #[cfg(not(windows))]
            println!("allocated_bytes={}", allocated.get() - before);
            println!(
                "case={case} elapsed={elapsed:?} response_bytes={}",
                response.len()
            );
        }
        return;
    }
    benches();
    Criterion::default().configure_from_args().final_summary();
}
