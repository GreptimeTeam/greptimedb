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

use common_query::Output;
use frontend::instance::Instance;
use query::datafusion::QUERY_PARALLELISM_HINT;
use query::options::QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;

use crate::test_util::execute_sql;
use crate::tests;

#[tokio::test(flavor = "multi_thread")]
async fn test_remote_dyn_filter_join_e2e() {
    common_telemetry::init_default_ut_logging();

    let distributed = tests::create_distributed_instance("test_remote_dyn_filter_join_e2e").await;
    let frontend = distributed.frontend();

    prepare_remote_dyn_filter_tables(&frontend).await;

    let join_sql = remote_dyn_filter_join_sql();
    let result = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, join_sql, true).await,
    )
    .await;
    assert_eq!(
        result,
        r#"+---+------+
| k | v    |
+---+------+
| 2 | 20.0 |
| 4 | 40.0 |
+---+------+"#
    );

    let explain_sql = format!("EXPLAIN ANALYZE VERBOSE {join_sql}");
    let explain = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, &explain_sql, true).await,
    )
    .await;

    assert_contains(&explain, "HashJoinExec: mode=CollectLeft");
    assert_contains(&explain, "MergeScanExec");
    assert_seq_scan_has_dyn_filter(&explain);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remote_dyn_filter_left_join_e2e() {
    common_telemetry::init_default_ut_logging();

    let distributed =
        tests::create_distributed_instance("test_remote_dyn_filter_left_join_e2e").await;
    let frontend = distributed.frontend();

    prepare_remote_dyn_filter_tables(&frontend).await;
    execute_sql(
        &frontend,
        r#"
        INSERT INTO rdf_build(k, ts) VALUES (9, 9000)
        "#,
    )
    .await;

    let join_sql = remote_dyn_filter_left_join_sql();
    let result = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, join_sql, true).await,
    )
    .await;
    assert_eq!(
        result,
        r#"+---+------+
| k | v    |
+---+------+
| 2 | 20.0 |
| 4 | 40.0 |
| 9 | -1.0 |
+---+------+"#
    );

    let explain_sql = format!("EXPLAIN ANALYZE VERBOSE {join_sql}");
    let explain = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, &explain_sql, true).await,
    )
    .await;

    assert_contains(&explain, "HashJoinExec: mode=CollectLeft, join_type=Left");
    assert_contains(&explain, "MergeScanExec");
    assert_seq_scan_dyn_filter_contains(
        &explain,
        &[
            "DynamicFilter [ k@0 >= 2 AND k@0 <= 9",
            "k@0 IN (SET) ([2, 4, 9])",
        ],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remote_dyn_filter_multi_column_join_e2e() {
    common_telemetry::init_default_ut_logging();

    let distributed =
        tests::create_distributed_instance("test_remote_dyn_filter_multi_column_join_e2e").await;
    let frontend = distributed.frontend();

    prepare_remote_dyn_filter_multi_column_tables(&frontend).await;

    let join_sql = remote_dyn_filter_multi_column_join_sql();
    let result = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, join_sql, true).await,
    )
    .await;
    assert_eq!(
        result,
        r#"+---+----+-------+
| a | k  | v     |
+---+----+-------+
| 1 | 10 | 110.0 |
| 2 | 20 | 220.0 |
+---+----+-------+"#
    );

    let explain_sql = format!("EXPLAIN ANALYZE VERBOSE {join_sql}");
    let explain = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, &explain_sql, true).await,
    )
    .await;

    assert_contains(&explain, "HashJoinExec: mode=CollectLeft");
    assert_contains(&explain, "MergeScanExec");
    assert_seq_scan_has_dyn_filter(&explain);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remote_dyn_filter_large_join_e2e() {
    common_telemetry::init_default_ut_logging();

    let distributed =
        tests::create_distributed_instance("test_remote_dyn_filter_large_join_e2e").await;
    let frontend = distributed.frontend();

    prepare_remote_dyn_filter_large_tables(&frontend).await;

    let join_sql = remote_dyn_filter_large_join_sql();
    let result = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, join_sql, true).await,
    )
    .await;
    assert_eq!(
        result,
        r#"+------+---------+
| k    | v       |
+------+---------+
| 3    | 30.0    |
| 129  | 1290.0  |
| 511  | 5110.0  |
| 900  | 9000.0  |
| 8195 | 81950.0 |
+------+---------+"#
    );

    let result_without_rdf = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, join_sql, false).await,
    )
    .await;
    assert_eq!(result_without_rdf, result);

    let explain_sql = format!("EXPLAIN ANALYZE VERBOSE {join_sql}");
    let explain = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, &explain_sql, true).await,
    )
    .await;

    assert_contains(&explain, "HashJoinExec: mode=CollectLeft");
    assert_contains(&explain, "MergeScanExec");
    assert_seq_scan_dyn_filter_contains(
        &explain,
        &[
            "DynamicFilter [ k@0 >= 3 AND k@0 <= 8195",
            "k@0 IN (SET) ([3, 129, 511, 900, 8195])",
        ],
    );

    let explain_without_rdf = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, &explain_sql, false).await,
    )
    .await;
    assert_no_seq_scan_dyn_filter(&explain_without_rdf);
}

async fn prepare_remote_dyn_filter_tables(frontend: &Arc<Instance>) {
    execute_sql(
        frontend,
        r#"
        CREATE TABLE rdf_probe(
            k INT,
            ts TIMESTAMP,
            v DOUBLE,
            TIME INDEX (ts),
            PRIMARY KEY(k)
        )
        PARTITION ON COLUMNS (k) (
            k < 2,
            k >= 2 AND k < 4,
            k >= 4 AND k < 6,
            k >= 6
        )
        engine=mito
        "#,
    )
    .await;

    execute_sql(
        frontend,
        r#"
        CREATE TABLE rdf_build(
            k INT,
            ts TIMESTAMP,
            TIME INDEX (ts),
            PRIMARY KEY(k)
        ) engine=mito
        "#,
    )
    .await;

    execute_sql(
        frontend,
        r#"
        INSERT INTO rdf_probe(k, ts, v) VALUES
            (1, 1000, 10.0),
            (2, 2000, 20.0),
            (3, 3000, 30.0),
            (4, 4000, 40.0),
            (7, 5000, 50.0)
        "#,
    )
    .await;

    execute_sql(
        frontend,
        r#"
        INSERT INTO rdf_build(k, ts) VALUES
            (2, 1000),
            (4, 2000)
        "#,
    )
    .await;
}

async fn prepare_remote_dyn_filter_multi_column_tables(frontend: &Arc<Instance>) {
    execute_sql(
        frontend,
        r#"
        CREATE TABLE rdf_multi_probe(
            a INT,
            k INT,
            ts TIMESTAMP,
            v DOUBLE,
            TIME INDEX (ts),
            PRIMARY KEY(a, k)
        )
        PARTITION ON COLUMNS (a) (
            a < 2,
            a >= 2 AND a < 3,
            a >= 3 AND a < 4,
            a >= 4
        )
        engine=mito
        "#,
    )
    .await;

    execute_sql(
        frontend,
        r#"
        CREATE TABLE rdf_multi_build(
            a INT,
            k INT,
            ts TIMESTAMP,
            TIME INDEX (ts),
            PRIMARY KEY(a, k)
        ) engine=mito
        "#,
    )
    .await;

    execute_sql(
        frontend,
        r#"
        INSERT INTO rdf_multi_probe(a, k, ts, v) VALUES
            (1, 10, 1000, 110.0),
            (1, 11, 1100, 111.0),
            (2, 10, 2000, 210.0),
            (2, 20, 2200, 220.0),
            (3, 30, 3000, 330.0),
            (4, 40, 4000, 440.0)
        "#,
    )
    .await;

    execute_sql(
        frontend,
        r#"
        INSERT INTO rdf_multi_build(a, k, ts) VALUES
            (1, 10, 1000),
            (2, 20, 2000)
        "#,
    )
    .await;
}

async fn prepare_remote_dyn_filter_large_tables(frontend: &Arc<Instance>) {
    execute_sql(
        frontend,
        r#"
        CREATE TABLE rdf_large_probe(
            k INT,
            ts TIMESTAMP,
            v DOUBLE,
            TIME INDEX (ts),
            PRIMARY KEY(k)
        )
        PARTITION ON COLUMNS (k) (
            k < 1024,
            k >= 1024 AND k < 2048,
            k >= 2048 AND k < 3072,
            k >= 3072 AND k < 4096,
            k >= 4096 AND k < 5120,
            k >= 5120 AND k < 6144,
            k >= 6144 AND k < 7168,
            k >= 7168
        )
        engine=mito
        "#,
    )
    .await;

    execute_sql(
        frontend,
        r#"
        CREATE TABLE rdf_large_build(
            k INT,
            ts TIMESTAMP,
            TIME INDEX (ts),
            PRIMARY KEY(k)
        ) engine=mito
        "#,
    )
    .await;

    for start in (0..8192).step_by(1024) {
        insert_remote_dyn_filter_large_probe_range(frontend, start, start + 1024).await;
    }
    execute_sql(frontend, "ADMIN FLUSH_TABLE('rdf_large_probe')").await;

    // Keep a few rows in memtable after flush so the same query covers both
    // flushed SST/file data and newly written memtable data.
    insert_remote_dyn_filter_large_probe_range(frontend, 8192, 8200).await;

    execute_sql(
        frontend,
        r#"
        INSERT INTO rdf_large_build(k, ts) VALUES
            (3, 3000),
            (129, 129000),
            (511, 511000),
            (900, 900000),
            (8195, 8195000)
        "#,
    )
    .await;
}

async fn prepare_remote_dyn_filter_bloom_tables(frontend: &Arc<Instance>) {
    // Probe table: 8192 sparse keys spread across 4 partitions. Build table:
    // 302 keys distributed across the full probe key range. Keeping the build
    // side small makes the Bloom update arrive quickly, while the larger
    // flushed probe-side SST scan gives the non-pushdown FilterExec enough
    // rows to show the Bloom filtering effect in EXPLAIN ANALYZE metrics.
    execute_sql(
        frontend,
        r#"
        CREATE TABLE rdf_bloom_probe(
            k INT,
            ts TIMESTAMP,
            v DOUBLE,
            TIME INDEX (ts),
            PRIMARY KEY(k)
        )
        PARTITION ON COLUMNS (k) (
            k < 2097152,
            k >= 2097152 AND k < 4194304,
            k >= 4194304 AND k < 6291456,
            k >= 6291456
        )
        engine=mito
        "#,
    )
    .await;

    execute_sql(
        frontend,
        r#"
        CREATE TABLE rdf_bloom_build(
            k INT,
            ts TIMESTAMP,
            TIME INDEX (ts),
            PRIMARY KEY(k)
        ) engine=mito
        "#,
    )
    .await;

    const PROBE_COUNT: usize = 8192;
    const BUILD_COUNT: usize = 302;

    let probe_values = (0..PROBE_COUNT)
        .map(|i| {
            let k = i * 1024;
            format!("({k}, {k}, {}.0)", k * 10)
        })
        .collect::<Vec<_>>()
        .join(",");
    let insert_probe_sql = format!("INSERT INTO rdf_bloom_probe(k, ts, v) VALUES {probe_values}");
    execute_sql(frontend, &insert_probe_sql).await;

    // Flush so the scan reads from SST files, giving the async Bloom
    // gRPC update more time to arrive before the scan finishes.
    execute_sql(frontend, "ADMIN FLUSH_TABLE('rdf_bloom_probe')").await;

    // Write a few extra probe-only rows into memtable after flush.
    // These are probe keys that do NOT appear in the build side,
    // so the Bloom filter must reject them. This increases the gap
    // between scan volume and join output, making the filtering effect
    // easier to observe in metrics.
    let extra_start = PROBE_COUNT;
    let extra_end = extra_start + 8;
    let extra_probe_values = (extra_start..extra_end)
        .map(|i| {
            let k = i * 1024;
            format!("({k}, {k}, {}.0)", k * 10)
        })
        .collect::<Vec<_>>()
        .join(",");
    if !extra_probe_values.is_empty() {
        let extra_sql =
            format!("INSERT INTO rdf_bloom_probe(k, ts, v) VALUES {extra_probe_values}");
        execute_sql(frontend, &extra_sql).await;
    }

    let build_values = (0..BUILD_COUNT)
        .map(|i| {
            let probe_index = i * (PROBE_COUNT - 1) / (BUILD_COUNT - 1);
            let k = probe_index * 1024;
            format!("({k}, {k})")
        })
        .collect::<Vec<_>>()
        .join(",");
    let insert_build_sql = format!("INSERT INTO rdf_bloom_build(k, ts) VALUES {build_values}");
    execute_sql(frontend, &insert_build_sql).await;
}

async fn insert_remote_dyn_filter_large_probe_range(
    frontend: &Arc<Instance>,
    start: usize,
    end: usize,
) {
    let values = (start..end)
        .map(|k| format!("({k}, {k}, {}.0)", k * 10))
        .collect::<Vec<_>>()
        .join(",");
    let insert_probe_sql = format!("INSERT INTO rdf_large_probe(k, ts, v) VALUES {values}");
    execute_sql(frontend, &insert_probe_sql).await;
}

fn remote_dyn_filter_join_sql() -> &'static str {
    r#"
    SELECT p.k, p.v
    FROM rdf_build b
    JOIN rdf_probe p ON p.k = b.k
    ORDER BY p.k
    "#
}

fn remote_dyn_filter_left_join_sql() -> &'static str {
    r#"
    SELECT b.k,
           CASE WHEN p.v IS NULL THEN -1.0 ELSE p.v END AS v
    FROM rdf_build b
    LEFT JOIN rdf_probe p ON p.k = b.k
    ORDER BY b.k
    "#
}

fn remote_dyn_filter_multi_column_join_sql() -> &'static str {
    r#"
    SELECT p.a, p.k, p.v
    FROM rdf_multi_build b
    JOIN rdf_multi_probe p ON p.a = b.a AND p.k = b.k
    ORDER BY p.a, p.k
    "#
}

fn remote_dyn_filter_large_join_sql() -> &'static str {
    r#"
    SELECT p.k, p.v
    FROM rdf_large_build b
    JOIN rdf_large_probe p ON p.k = b.k
    ORDER BY p.k
    "#
}

fn remote_dyn_filter_bloom_join_sql() -> &'static str {
    r#"
    SELECT COUNT(*) AS cnt
    FROM rdf_bloom_build b
    JOIN rdf_bloom_probe p ON p.k = b.k
    "#
}

async fn output_to_pretty_string(output: Output) -> String {
    output.data.pretty_print().await
}

async fn execute_sql_with_query_parallelism_one(
    instance: &Arc<Instance>,
    sql: &str,
    remote_dyn_filter_enabled: bool,
) -> Output {
    let mut query_ctx = QueryContext::with_db_name(None);
    query_ctx.set_extension(QUERY_PARALLELISM_HINT, "1");
    if !remote_dyn_filter_enabled {
        query_ctx.set_extension(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN, "false");
    }
    SqlQueryHandler::do_query(instance.as_ref(), sql, Arc::new(query_ctx))
        .await
        .remove(0)
        .unwrap()
}

fn assert_no_seq_scan_dyn_filter(explain: &str) {
    let seq_scan_dyn_filter_lines = explain
        .lines()
        .filter(|line| line.contains("SeqScan: region=") && line.contains("\"dyn_filters\""))
        .collect::<Vec<_>>();

    assert!(
        seq_scan_dyn_filter_lines.is_empty(),
        "expected no region SeqScan line with dyn_filters; actual SeqScan dyn_filters lines:\n{}\n\nfull explain:\n{explain}",
        seq_scan_dyn_filter_lines.join("\n")
    );
}

fn assert_contains(haystack: &str, needle: &str) {
    assert!(
        haystack.contains(needle),
        "expected to find {needle:?} in:\n{haystack}"
    );
}

fn assert_seq_scan_dyn_filter_contains(explain: &str, needles: &[&str]) {
    let seq_scan_dyn_filter_lines = explain
        .lines()
        .filter(|line| line.contains("SeqScan: region=") && line.contains("\"dyn_filters\""))
        .collect::<Vec<_>>();

    assert!(
        !seq_scan_dyn_filter_lines.is_empty(),
        "expected at least one region SeqScan line with dyn_filters in:\n{explain}"
    );

    let matched_line = seq_scan_dyn_filter_lines
        .iter()
        .find(|line| needles.iter().all(|needle| line.contains(needle)));

    assert!(
        matched_line.is_some(),
        "expected one region SeqScan dyn_filters line containing all of {needles:?}; actual SeqScan dyn_filters lines:\n{}\n\nfull explain:\n{explain}",
        seq_scan_dyn_filter_lines.join("\n")
    );
}

fn assert_seq_scan_has_dyn_filter(explain: &str) {
    let has_dyn_filter = explain
        .lines()
        .any(|line| line.contains("SeqScan: region=") && line.contains("\"dyn_filters\""));

    assert!(
        has_dyn_filter,
        "expected at least one region SeqScan line with dyn_filters in:\n{explain}"
    );
}

fn assert_seq_scan_dyn_filter_not_contains(explain: &str, needle: &str) {
    let seq_scan_dyn_filter_lines = explain
        .lines()
        .filter(|line| line.contains("SeqScan: region=") && line.contains("\"dyn_filters\""))
        .collect::<Vec<_>>();

    assert!(
        seq_scan_dyn_filter_lines
            .iter()
            .all(|line| !line.contains(needle)),
        "expected no region SeqScan dyn_filters line to contain {needle:?}; actual SeqScan dyn_filters lines:\n{}\n\nfull explain:\n{explain}",
        seq_scan_dyn_filter_lines.join("\n")
    );
}

/// Extracts the value of a named metric from an explain metrics line.
///
/// The line is expected to contain `metrics=[..., metric_name: value, ...]`.
/// Returns `None` if the metric name is not found or the value cannot be
/// parsed as `usize`.
fn parse_metric_value(line: &str, metric_name: &str) -> Option<usize> {
    let metrics_segment = line.split("metrics=[").nth(1)?;
    // The metrics block ends with `]` (potentially followed by extra output).
    let metrics_body = metrics_segment.split(']').next()?;
    let prefix = format!("{}: ", metric_name);
    for part in metrics_body.split(", ") {
        if let Some(val_str) = part.strip_prefix(&prefix) {
            return val_str.trim().parse::<usize>().ok();
        }
    }
    None
}

/// Pairs each `FilterExec` line carrying `bloom_probe` with the next region
/// `SeqScan` line under it, returning `(filter_output_rows, scan_output_rows)`.
fn bloom_filter_exec_scan_row_pairs(explain: &str) -> Vec<(usize, usize)> {
    let mut pending_bloom_filter_rows = None;
    let mut pairs = Vec::new();

    for line in explain.lines() {
        if line.contains("FilterExec")
            && line.contains("non_pushdown(")
            && line.contains("bloom_probe")
        {
            pending_bloom_filter_rows = parse_metric_value(line, "output_rows");
            continue;
        }

        if line.contains("SeqScan: region=")
            && let Some(filter_rows) = pending_bloom_filter_rows.take()
            && let Some(scan_rows) = parse_metric_value(line, "output_rows")
        {
            pairs.push((filter_rows, scan_rows));
        }
    }

    pairs
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remote_dyn_filter_bloom_typed_payload_e2e() {
    common_telemetry::init_default_ut_logging();

    let distributed =
        tests::create_distributed_instance("test_remote_dyn_filter_bloom_typed_payload_e2e").await;
    let frontend = distributed.frontend();

    prepare_remote_dyn_filter_bloom_tables(&frontend).await;

    let join_sql = remote_dyn_filter_bloom_join_sql();
    let result = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, join_sql, true).await,
    )
    .await;
    // 302 build keys distributed across the 8192 probe keys → 302 join rows.
    assert_eq!(
        result,
        r#"+-----+
| cnt |
+-----+
| 302 |
+-----+"#
    );

    let explain_sql = format!("EXPLAIN ANALYZE VERBOSE {join_sql}");
    let explain = output_to_pretty_string(
        execute_sql_with_query_parallelism_one(&frontend, &explain_sql, true).await,
    )
    .await;

    // ── Plan-shape invariants ──────────────────────────────────────
    assert_contains(&explain, "HashJoinExec: mode=CollectLeft");
    assert_contains(&explain, "MergeScanExec");
    assert_contains(&explain, "FilterExec");
    assert_contains(&explain, "non_pushdown(");
    // The exact Bloom probe must never leak into SeqScan dyn_filters
    // (it stays in the non-pushdown FilterExec wrapper).
    assert_seq_scan_dyn_filter_not_contains(&explain, "bloom_probe");

    // ── Bloom filtering-effect assertions ──────────────────────────
    //
    // The Bloom gRPC update is asynchronous per region.  The explain may show
    // some region FilterExec nodes already carrying `bloom_probe`, while other
    // regions still carry the initial `true` exact wrapper.  For regions where
    // Bloom arrived, the FilterExec output must be smaller than the underlying
    // SeqScan output, proving row-level filtering outside scan.
    let bloom_pairs = bloom_filter_exec_scan_row_pairs(&explain);
    assert!(
        !bloom_pairs.is_empty(),
        "expected at least one non-pushdown FilterExec with bloom_probe in:\n{explain}"
    );
    assert!(
        bloom_pairs
            .iter()
            .any(|(filter_rows, scan_rows)| filter_rows < scan_rows),
        "expected at least one Bloom FilterExec to reduce rows; pairs={bloom_pairs:?};\
         explain:\n{explain}"
    );
}
