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

use common_query::Output;
use frontend::instance::Instance;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;

async fn execute_sql(instance: &Instance, sql: &str) -> Output {
    SqlQueryHandler::do_query(instance, sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dict_groupby_sst() {
    common_telemetry::init_default_ut_logging();

    let standalone = GreptimeDbStandaloneBuilder::new("dict_groupby_sst")
        .build()
        .await;
    let frontend = standalone.fe_instance();

    execute_sql(
        frontend,
        r#"
        CREATE TABLE cpu (
            hostname STRING PRIMARY KEY,
            usage_user DOUBLE,
            usage_system DOUBLE,
            usage_idle DOUBLE,
            greptime_timestamp TIMESTAMP TIME INDEX
        ) WITH (append_mode = 'true', sst_format = 'flat')
        "#,
    )
    .await;

    let mut rows = String::new();
    for index in 0..1200 {
        if index != 0 {
            rows.push(',');
        }
        let hostname = match index % 3 {
            0 => "host_a",
            1 => "host_b",
            _ => "host_c",
        };
        let usage_user = match index % 3 {
            0 => 10.0,
            1 => 20.0,
            _ => 30.0,
        };
        let hour = 11 + index / 600;
        let minute = (index % 600) / 60;
        let second = index % 60;
        rows.push_str(&format!(
            "('{hostname}', {usage_user}, 1.0, 0.0, '2023-06-12T{hour:02}:{minute:02}:{second:02}Z')"
        ));
    }
    execute_sql(frontend, &format!("INSERT INTO cpu VALUES {rows}")).await;

    execute_sql(frontend, "ADMIN FLUSH_TABLE('cpu')").await;

    let count = execute_sql(frontend, "SELECT count(*) FROM cpu")
        .await
        .data
        .pretty_print()
        .await;
    assert!(
        count.contains("| 1200     |"),
        "unexpected row count:\n{count}"
    );

    let query = r#"
        SELECT hostname, avg(usage_user), date_trunc('hour', greptime_timestamp)
        FROM cpu
        WHERE greptime_timestamp >= '2023-06-12T00:00:00Z'
          AND greptime_timestamp < '2023-06-14T00:00:00Z'
        GROUP BY date_trunc('hour', greptime_timestamp), hostname
        ORDER BY hostname, date_trunc('hour', greptime_timestamp)
    "#;
    let groups = execute_sql(frontend, query).await.data.pretty_print().await;
    assert_eq!(
        6,
        groups
            .lines()
            .filter(|line| line.starts_with("| host_"))
            .count(),
        "expected one group per hostname and hour:\n{groups}"
    );
    for (hostname, average) in [("host_a", "10.0"), ("host_b", "20.0"), ("host_c", "30.0")] {
        assert_eq!(
            2,
            groups.matches(&format!("| {hostname} ")).count(),
            "expected two hourly groups for {hostname}:\n{groups}"
        );
        assert!(
            groups.contains(average),
            "expected average {average} for {hostname}:\n{groups}"
        );
    }

    let explain = execute_sql(
        frontend,
        r#"
        EXPLAIN ANALYZE VERBOSE
        SELECT avg(usage_user), date_trunc('hour', greptime_timestamp)
        FROM cpu
        WHERE greptime_timestamp >= '2023-06-12T00:00:00Z'
          AND greptime_timestamp < '2023-06-14T00:00:00Z'
        GROUP BY date_trunc('hour', greptime_timestamp), hostname
        "#,
    )
    .await
    .data
    .pretty_print()
    .await;
    assert!(
        explain.contains("AggregateExec"),
        "expected aggregate in physical plan:\n{explain}"
    );
    assert!(
        !explain.contains("CastExec") && !explain.contains("CAST(hostname"),
        "dictionary hostname must not be cast before aggregation:\n{explain}"
    );
}
