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
use session::Session;
use session::context::{Channel, QueryContext};

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
async fn test_query_options_session_scope_and_remote_filter_e2e() {
    common_telemetry::init_default_ut_logging();

    let distributed = tests::create_distributed_instance("test_query_options_session_scope").await;
    let frontend = distributed.frontend();
    prepare_remote_dyn_filter_large_tables(&frontend).await;

    let first = Session::new(None, Channel::Mysql, Default::default(), 1);
    let second = Session::new(None, Channel::Mysql, Default::default(), 2);

    // Capture the second session's initial value before changing the first
    // session, without assuming a host-specific default.
    let second_initial = SqlQueryHandler::do_query(
        frontend.as_ref(),
        "SHOW VARIABLES query.parallelism",
        second.new_query_context(),
    )
    .await;
    assert!(
        second_initial.iter().all(Result::is_ok),
        "{second_initial:?}"
    );
    let second_initial =
        output_to_pretty_string(second_initial.into_iter().next().unwrap().unwrap()).await;

    // SET is applied on the connection's session state; SHOW VARIABLES reports
    // the effective value, and a separate connection retains its own value.
    let set_and_show = SqlQueryHandler::do_query(
        frontend.as_ref(),
        "SET query_parallelism = 2; SHOW VARIABLES query.parallelism; SET query.enable_remote_dynamic_filter_pushdown = false; SHOW VARIABLES query.enable_remote_dynamic_filter_pushdown",
        first.new_query_context(),
    )
    .await;
    assert!(set_and_show.iter().all(Result::is_ok), "{set_and_show:?}");
    let mut results = set_and_show.into_iter();
    let _ = results.next().unwrap().unwrap();
    let values = output_to_pretty_string(results.next().unwrap().unwrap()).await;
    assert!(
        values
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "2"),
        "{values}"
    );
    let _ = results.next().unwrap().unwrap();
    let remote_filter = output_to_pretty_string(results.next().unwrap().unwrap()).await;
    assert_contains(&remote_filter, "false");

    let fallback = SqlQueryHandler::do_query(
        frontend.as_ref(),
        "SET query_fallback = true; SHOW VARIABLES query.allow_query_fallback; SET ALLOW_QUERY_FALLBACK = false; SHOW VARIABLES query_fallback",
        first.new_query_context(),
    )
    .await;
    assert!(fallback.iter().all(Result::is_ok), "{fallback:?}");
    let mut fallback = fallback.into_iter();
    let _ = fallback.next().unwrap().unwrap();
    assert_contains(
        &output_to_pretty_string(fallback.next().unwrap().unwrap()).await,
        "true",
    );
    let _ = fallback.next().unwrap().unwrap();
    assert_contains(
        &output_to_pretty_string(fallback.next().unwrap().unwrap()).await,
        "false",
    );

    let second_after_first_set = SqlQueryHandler::do_query(
        frontend.as_ref(),
        "SHOW VARIABLES query.parallelism",
        second.new_query_context(),
    )
    .await;
    assert!(
        second_after_first_set.iter().all(Result::is_ok),
        "{second_after_first_set:?}"
    );
    let second_after_first_set =
        output_to_pretty_string(second_after_first_set.into_iter().next().unwrap().unwrap()).await;
    assert_eq!(second_after_first_set, second_initial);

    let independent = SqlQueryHandler::do_query(
        frontend.as_ref(),
        "SET query_parallelism = 7; SHOW VARIABLES query.parallelism",
        second.new_query_context(),
    )
    .await;
    assert!(independent.iter().all(Result::is_ok), "{independent:?}");
    let mut independent = independent.into_iter();
    let _ = independent.next().unwrap().unwrap();
    let independent = output_to_pretty_string(independent.next().unwrap().unwrap()).await;
    assert!(
        independent
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "7"),
        "{independent}"
    );
    let first_after_second_set = SqlQueryHandler::do_query(
        frontend.as_ref(),
        "SHOW VARIABLES query.parallelism",
        first.new_query_context(),
    )
    .await;
    assert!(
        first_after_second_set.iter().all(Result::is_ok),
        "{first_after_second_set:?}"
    );
    let first_after_second_set =
        output_to_pretty_string(first_after_second_set.into_iter().next().unwrap().unwrap()).await;
    assert_eq!(first_after_second_set, values);

    // Model request hints on the query context used by an HTTP request. A
    // request-scoped value wins even after a statement changes the session.
    let mut request_context = first.new_query_context().as_ref().clone();
    request_context.set_extension("query.enable_remote_dynamic_filter_pushdown", "false");
    let hinted = SqlQueryHandler::do_query(
        frontend.as_ref(),
        "SET query.enable_remote_dynamic_filter_pushdown = true; SHOW VARIABLES query.enable_remote_dynamic_filter_pushdown",
        Arc::new(request_context),
    )
    .await;
    assert!(hinted.iter().all(Result::is_ok), "{hinted:?}");
    let mut hinted = hinted.into_iter();
    let _ = hinted.next().unwrap().unwrap();
    let effective = output_to_pretty_string(hinted.next().unwrap().unwrap()).await;
    assert_contains(&effective, "false");
    let no_hint = SqlQueryHandler::do_query(
        frontend.as_ref(),
        "SHOW VARIABLES query.enable_remote_dynamic_filter_pushdown",
        first.new_query_context(),
    )
    .await;
    assert!(no_hint.iter().all(Result::is_ok), "{no_hint:?}");
    let no_hint = output_to_pretty_string(no_hint.into_iter().next().unwrap().unwrap()).await;
    assert_contains(&no_hint, "true");

    // Exercise the session setting through a real distributed FE->DN query:
    // its physical scan plan exposes whether the remote predicate arrived.
    let join_sql = remote_dyn_filter_large_join_sql();
    let explain_sql = format!("EXPLAIN ANALYZE VERBOSE {join_sql}");
    let enabled = SqlQueryHandler::do_query(
        frontend.as_ref(),
        &format!("SET query.enable_remote_dynamic_filter_pushdown = true; {explain_sql}"),
        first.new_query_context(),
    )
    .await;
    assert!(enabled.iter().all(Result::is_ok), "{enabled:?}");
    let mut enabled = enabled.into_iter();
    let _ = enabled.next().unwrap().unwrap();
    let enabled = output_to_pretty_string(enabled.next().unwrap().unwrap()).await;
    assert_seq_scan_has_dyn_filter(&enabled);

    let disabled = SqlQueryHandler::do_query(
        frontend.as_ref(),
        &format!("SET query.enable_remote_dynamic_filter_pushdown = false; {explain_sql}"),
        first.new_query_context(),
    )
    .await;
    assert!(disabled.iter().all(Result::is_ok), "{disabled:?}");
    let mut disabled = disabled.into_iter();
    let _ = disabled.next().unwrap().unwrap();
    let disabled = output_to_pretty_string(disabled.next().unwrap().unwrap()).await;
    assert_no_seq_scan_dyn_filter(&disabled);

    let enabled_result = SqlQueryHandler::do_query(
        frontend.as_ref(),
        &format!("SET query.enable_remote_dynamic_filter_pushdown = true; {join_sql}"),
        first.new_query_context(),
    )
    .await;
    let mut enabled_result = enabled_result.into_iter();
    let _ = enabled_result.next().unwrap().unwrap();
    let enabled_result = output_to_pretty_string(enabled_result.next().unwrap().unwrap()).await;
    let disabled_result = SqlQueryHandler::do_query(
        frontend.as_ref(),
        &format!("SET query.enable_remote_dynamic_filter_pushdown = false; {join_sql}"),
        first.new_query_context(),
    )
    .await;
    let mut disabled_result = disabled_result.into_iter();
    let _ = disabled_result.next().unwrap().unwrap();
    let disabled_result = output_to_pretty_string(disabled_result.next().unwrap().unwrap()).await;
    assert_eq!(enabled_result, disabled_result);

    // Native DataFusion pushdown is independent: disabling it keeps the result
    // while suppressing the scan dynamic-filter evidence despite remote=true.
    let native_disabled = SqlQueryHandler::do_query(
        frontend.as_ref(),
        &format!(
            "SET query.enable_remote_dynamic_filter_pushdown = true; SET datafusion.optimizer.enable_dynamic_filter_pushdown = false; {explain_sql}"
        ),
        first.new_query_context(),
    )
    .await;
    assert!(
        native_disabled.iter().all(Result::is_ok),
        "{native_disabled:?}"
    );
    let mut native_disabled = native_disabled.into_iter();
    let _ = native_disabled.next().unwrap().unwrap();
    let _ = native_disabled.next().unwrap().unwrap();
    let native_disabled = output_to_pretty_string(native_disabled.next().unwrap().unwrap()).await;
    assert_no_seq_scan_dyn_filter(&native_disabled);
    let native_disabled_result = SqlQueryHandler::do_query(
        frontend.as_ref(),
        &format!(
            "SET query.enable_remote_dynamic_filter_pushdown = true; SET datafusion.optimizer.enable_dynamic_filter_pushdown = false; {join_sql}"
        ),
        first.new_query_context(),
    )
    .await;
    let mut native_disabled_result = native_disabled_result.into_iter();
    let _ = native_disabled_result.next().unwrap().unwrap();
    let _ = native_disabled_result.next().unwrap().unwrap();
    let native_disabled_result =
        output_to_pretty_string(native_disabled_result.next().unwrap().unwrap()).await;
    assert_eq!(native_disabled_result, enabled_result);

    // Parallel execution plus the native join-repartition optimizer switch
    // changes the physical plan while preserving the query result.
    let native_explain_sql = format!("EXPLAIN {join_sql}");
    let plan_with_repartition = execute_with_session_options(
        frontend.as_ref(),
        &first,
        native_explain_sql.as_str(),
        "true",
    )
    .await;
    let plan_with_repartition = output_to_pretty_string(plan_with_repartition).await;
    let plan_without_repartition = execute_with_session_options(
        frontend.as_ref(),
        &first,
        native_explain_sql.as_str(),
        "false",
    )
    .await;
    let plan_without_repartition = output_to_pretty_string(plan_without_repartition).await;
    assert_contains(&plan_with_repartition, "HashJoinExec: mode=Partitioned");
    assert_contains(&plan_without_repartition, "HashJoinExec: mode=CollectLeft");
    let result_with_repartition =
        execute_with_session_options(frontend.as_ref(), &first, join_sql, "true").await;
    let result_without_repartition =
        execute_with_session_options(frontend.as_ref(), &first, join_sql, "false").await;
    assert_eq!(
        output_to_pretty_string(result_with_repartition).await,
        output_to_pretty_string(result_without_repartition).await
    );
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
    // RDF is best-effort, so this tiny query may finish before the runtime
    // predicate is fanned out. Only assert that RDF was planned for the scan.
    assert_seq_scan_has_dyn_filter(&explain);
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

async fn execute_with_session_options(
    frontend: &Instance,
    session: &Session,
    sql: &str,
    repartition_joins: &str,
) -> Output {
    let statement = format!(
        "SET query_parallelism = 2; SET query.enable_remote_dynamic_filter_pushdown = true; SET datafusion.optimizer.enable_dynamic_filter_pushdown = true; SET datafusion.optimizer.repartition_joins = {repartition_joins}; {sql}"
    );
    let outputs =
        SqlQueryHandler::do_query(frontend, &statement, session.new_query_context()).await;
    assert!(outputs.iter().all(Result::is_ok), "{outputs:?}");
    outputs.into_iter().last().unwrap().unwrap()
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
