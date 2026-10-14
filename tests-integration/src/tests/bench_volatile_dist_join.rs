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

//! Local A/C latency comparison for volatile-side distributed broadcast join.
//!
//! Not a CI test — run manually with:
//! ```sh
//! cargo nextest run -p tests-integration bench_volatile_dist_join --run-ignored all
//! ```

use std::sync::Arc;
use std::time::Instant;

use common_query::Output;
use common_recordbatch::RecordBatches;
use frontend::instance::Instance;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};

use crate::cluster::GreptimeDbClusterBuilder;
use crate::test_util::execute_sql;

const BIG_TABLE: &str = "bench_big";
const SMALL_TABLE: &str = "bench_small";
const WARMUP: usize = 2;
const ITERATIONS: usize = 5;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "local benchmark only"]
async fn bench_volatile_dist_join() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("bench_volatile_dist_join")
        .await
        .with_real_datanode_grpc_addr(true)
        .with_datanodes(2)
        .build(false)
        .await;
    let frontend = cluster.fe_instance().clone();

    // BIG: 4 partitions, 10K rows total
    execute_sql(
        &frontend,
        &format!(
            "CREATE TABLE {BIG_TABLE} (
                row_id INT,
                join_key INT,
                payload STRING,
                ts TIMESTAMP,
                TIME INDEX (ts)
            ) PARTITION ON COLUMNS (row_id) (
                row_id < 2500,
                row_id >= 2500 AND row_id < 5000,
                row_id >= 5000 AND row_id < 7500,
                row_id >= 7500
            ) ENGINE=mito"
        ),
    )
    .await;

    // SMALL: 1 partition, 50 rows
    execute_sql(
        &frontend,
        &format!(
            "CREATE TABLE {SMALL_TABLE} (
                row_id INT,
                label STRING,
                ts TIMESTAMP,
                TIME INDEX (ts),
                PRIMARY KEY (row_id)
            ) ENGINE=mito"
        ),
    )
    .await;

    // Insert BIG data: 2500 rows per partition
    for partition_start in [0, 2500, 5000, 7500] {
        let mut values = Vec::new();
        for i in 0..2500 {
            let id = partition_start + i;
            let join_key = id % 50; // match SMALL ids
            values.push(format!(
                "({id}, {join_key}, 'payload-{id}', {ts})",
                ts = 1000 + i64::from(i)
            ));
        }
        execute_sql(
            &frontend,
            &format!(
                "INSERT INTO {BIG_TABLE}(row_id, join_key, payload, ts) VALUES {}",
                values.join(",")
            ),
        )
        .await;
    }

    // Insert SMALL data: 50 rows
    let values: Vec<String> = (0..50)
        .map(|i| format!("({i}, 'label-{i}', {})", 1000 + i64::from(i)))
        .collect();
    execute_sql(
        &frontend,
        &format!(
            "INSERT INTO {SMALL_TABLE}(row_id, label, ts) VALUES {}",
            values.join(",")
        ),
    )
    .await;

    let sql = format!(
        "SELECT b.row_id, s.rand_val FROM {BIG_TABLE} b \
         JOIN (SELECT row_id, random() AS rand_val FROM {SMALL_TABLE}) s \
         ON b.join_key = s.row_id"
    );

    // --- Path A: no dist_join (frontend join) ---
    let ctx_a = QueryContext::arc();
    let latencies_a = measure(&frontend, &sql, &ctx_a, "A (FE join)").await;

    // --- Path C: dist_join=true (materialized broadcast) ---
    let ctx_c = QueryContext::arc();
    ctx_c
        .configuration_parameter()
        .set_experimental_dist_join(true);
    let latencies_c = measure(&frontend, &sql, &ctx_c, "C (materialized broadcast)").await;

    // Report
    println!("\n=== Volatile-side broadcast join latency comparison ===");
    println!("BIG: 10K rows (4 partitions across 2 DNs), SMALL: 50 rows");
    println!("Warmup: {WARMUP}, Iterations: {ITERATIONS}");
    println!();
    report("A (FE join)", &latencies_a);
    report("C (materialized broadcast)", &latencies_c);
    println!();

    if !latencies_a.is_empty() && !latencies_c.is_empty() {
        let median_a = median(&latencies_a);
        let median_c = median(&latencies_c);
        let speedup = median_a / median_c;
        println!("Median speedup (A/C): {speedup:.2}x");
        if speedup > 1.0 {
            println!("C is faster than A");
        } else {
            println!("A is faster than C (materialization overhead dominates at this scale)");
        }
    }
}

async fn measure(
    frontend: &Arc<Instance>,
    sql: &str,
    ctx: &QueryContextRef,
    label: &str,
) -> Vec<f64> {
    // Warmup
    for i in 0..WARMUP {
        let start = Instant::now();
        let output = SqlQueryHandler::do_query(frontend.as_ref(), sql, ctx.clone())
            .await
            .remove(0)
            .unwrap();
        let batches = output_batches(output).await;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        eprintln!("  {label} warmup {i}: {:?}, {rows} rows", start.elapsed());
    }

    // Measured iterations
    let mut latencies = Vec::new();
    for i in 0..ITERATIONS {
        let start = Instant::now();
        let output = SqlQueryHandler::do_query(frontend.as_ref(), sql, ctx.clone())
            .await
            .remove(0)
            .unwrap();
        let batches = output_batches(output).await;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let elapsed = start.elapsed();
        latencies.push(elapsed.as_secs_f64() * 1000.0);
        eprintln!("  {label} iter {i}: {elapsed:.1?}, {rows} rows");
    }
    latencies
}

async fn output_batches(output: Output) -> RecordBatches {
    match output.data {
        common_query::OutputData::Stream(stream) => RecordBatches::try_collect(stream)
            .await
            .expect("the output stream must be collectable"),
        common_query::OutputData::RecordBatches(batches) => batches,
        common_query::OutputData::AffectedRows(rows) => {
            panic!("expected a query output, got {rows} affected rows")
        }
    }
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    sorted[sorted.len() / 2]
}

fn report(label: &str, latencies: &[f64]) {
    if latencies.is_empty() {
        println!("{label}: no samples");
        return;
    }
    let m = median(latencies);
    let min = latencies.iter().copied().fold(f64::INFINITY, f64::min);
    let max = latencies.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    println!("{label}:");
    println!("  median: {m:.1}ms");
    println!("  min:    {min:.1}ms");
    println!("  max:    {max:.1}ms");
    println!("  samples: {:?}", latencies);
}
