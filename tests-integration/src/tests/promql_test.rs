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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType,
};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use common_query::{Output, OutputData};
use common_recordbatch::util::collect_batches;
use datatypes::arrow::array::{Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::DataType;
use frontend::instance::Instance;
use query::datafusion::QUERY_PARALLELISM_HINT;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use rstest::rstest;
use rstest_reuse::apply;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;

use super::test_util::{
    both_instances_cases, check_unordered_output_stream, distributed, standalone,
};
use crate::tests::test_util::MockInstance;

#[allow(clippy::too_many_arguments)]
async fn promql_query(
    ins: Arc<Instance>,
    promql: &str,
    alias: Option<String>,
    query_ctx: Arc<QueryContext>,
    start: SystemTime,
    end: SystemTime,
    interval: Duration,
    lookback: Duration,
) -> operator::error::Result<Output> {
    let query = PromQuery {
        query: promql.to_string(),
        alias,
        ..PromQuery::default()
    };
    let QueryStatement::Promql(mut eval_stmt, alias) =
        QueryLanguageParser::parse_promql(&query, &query_ctx).unwrap()
    else {
        unreachable!()
    };
    eval_stmt.start = start;
    eval_stmt.end = end;
    eval_stmt.interval = interval;
    eval_stmt.lookback_delta = lookback;

    ins.statement_executor()
        .execute_stmt(QueryStatement::Promql(eval_stmt, alias), query_ctx)
        .await
}

#[allow(clippy::too_many_arguments)]
async fn create_insert_query_assert(
    instance: Arc<Instance>,
    create: &str,
    insert: &str,
    promql: &str,
    start: SystemTime,
    end: SystemTime,
    interval: Duration,
    lookback: Duration,
    expected: &str,
) {
    create_insert_query_assert_with_alias(
        instance, create, insert, promql, None, start, end, interval, lookback, expected,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_insert_query_assert_with_alias(
    instance: Arc<Instance>,
    create: &str,
    insert: &str,
    promql: &str,
    alias: Option<String>,
    start: SystemTime,
    end: SystemTime,
    interval: Duration,
    lookback: Duration,
    expected: &str,
) {
    instance
        .do_query(create, QueryContext::arc())
        .await
        .into_iter()
        .for_each(|v| {
            let _ = v.unwrap();
        });
    instance
        .do_query(insert, QueryContext::arc())
        .await
        .into_iter()
        .for_each(|v| {
            let _ = v.unwrap();
        });

    let query_output = promql_query(
        instance,
        promql,
        alias,
        QueryContext::arc(),
        start,
        end,
        interval,
        lookback,
    )
    .await
    .unwrap();
    check_unordered_output_stream(query_output, expected).await;
}

#[allow(clippy::too_many_arguments)]
async fn create_insert_tql_assert(
    instance: Arc<Instance>,
    create: &str,
    insert: &str,
    tql: &str,
    expected: &str,
) {
    instance
        .do_query(create, QueryContext::arc())
        .await
        .into_iter()
        .for_each(|v| {
            let _ = v.unwrap();
        });
    instance
        .do_query(insert, QueryContext::arc())
        .await
        .into_iter()
        .for_each(|v| {
            let _ = v.unwrap();
        });

    let query_output = instance
        .do_query(tql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    check_unordered_output_stream(query_output, expected).await;
}

async fn execute_all(instance: &Arc<Instance>, sql: &str, query_ctx: Arc<QueryContext>) {
    instance
        .do_query(sql, query_ctx)
        .await
        .into_iter()
        .for_each(|v| {
            let _ = v.unwrap();
        });
}

#[allow(clippy::too_many_arguments)]
async fn promql_query_as_batches(
    ins: Arc<Instance>,
    promql: &str,
    alias: Option<String>,
    query_ctx: Arc<QueryContext>,
    start: SystemTime,
    end: SystemTime,
    interval: Duration,
    lookback: Duration,
) -> common_recordbatch::RecordBatches {
    let output = promql_query(
        ins, promql, alias, query_ctx, start, end, interval, lookback,
    )
    .await
    .unwrap();
    match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(recordbatches) => recordbatches,
        _ => unreachable!(),
    }
}

const ANON_PROMQL_RATIO_REPRO_DB: &str = "repro_db";

const ANON_PROMQL_RATIO_REPRO_CREATE: &str = r#"
CREATE TABLE phy (
    t TIMESTAMP TIME INDEX,
    v DOUBLE
) ENGINE=metric WITH ("physical_metric_table" = "");

CREATE TABLE metric_a (
    l1 STRING NULL,
    l2 STRING NULL,
    l3 STRING NULL,
    l4 STRING NULL,
    l5 STRING NULL,
    t TIMESTAMP NOT NULL,
    v DOUBLE NULL,
    TIME INDEX (t),
    PRIMARY KEY (l1, l2, l3, l4, l5)
) ENGINE=metric WITH (on_physical_table = 'phy');

CREATE TABLE metric_b (
    l6 STRING NULL,
    l1 STRING NULL,
    l2 STRING NULL,
    l3 STRING NULL,
    l4 STRING NULL,
    t TIMESTAMP NOT NULL,
    v DOUBLE NULL,
    TIME INDEX (t),
    PRIMARY KEY (l6, l1, l2, l3, l4)
) ENGINE=metric WITH (on_physical_table = 'phy');
"#;

const ANON_PROMQL_RATIO_REPRO_INSERT: &str = r#"
INSERT INTO metric_a (l1, l2, l3, l4, l5, t, v) VALUES
    ('v1', 'v2', 'v3', 'v4a', 'v5a', 1, 0),
    ('v1', 'v2', 'v3', 'v4a', 'v5a', 180000, 120),
    ('v1', 'v2', 'v3', 'v4a', 'v5a', 360000, 240),
    ('v1', 'v2', 'v3', 'v4a', 'v5b', 1, 0),
    ('v1', 'v2', 'v3', 'v4a', 'v5b', 180000, 30),
    ('v1', 'v2', 'v3', 'v4a', 'v5b', 360000, 60),
    ('v1', 'v2', 'v3-b', 'v4b', 'v5c', 1, 0),
    ('v1', 'v2', 'v3-b', 'v4b', 'v5c', 180000, 60),
    ('v1', 'v2', 'v3-b', 'v4b', 'v5c', 360000, 120);

INSERT INTO metric_b (l6, l1, l2, l3, l4, t, v) VALUES
    ('v6', 'v1', 'v2', 'v3', 'v4a', 1, 1),
    ('v6', 'v1', 'v2', 'v3', 'v4a', 180000, 1),
    ('v6', 'v1', 'v2', 'v3', 'v4a', 360000, 1),
    ('v6', 'v1', 'v2', 'v3-b', 'v4b', 1, 2),
    ('v6', 'v1', 'v2', 'v3-b', 'v4b', 180000, 2),
    ('v6', 'v1', 'v2', 'v3-b', 'v4b', 360000, 2);
"#;

const ANON_PROMQL_RATIO_REPRO_NUMERATOR: &str = r#"count(((rate(metric_a{l1="v1",l2="v2",l3=~"v3(|-a|-b)",__schema__="repro_db"}[3m]) / on(l3,l4) group_left metric_b{l6="v6",l1="v1",l2="v2",l3=~"v3(|-a|-b)",__schema__="repro_db"}) > 0.50))"#;

const ANON_PROMQL_RATIO_REPRO_DENOMINATOR: &str =
    r#"count(rate(metric_a{l1="v1",l2="v2",l3=~"v3(|-a|-b)",__schema__="repro_db"}[3m]))"#;

const ANON_PROMQL_RATIO_REPRO_WHOLE: &str = r#"(count(((rate(metric_a{l1="v1",l2="v2",l3=~"v3(|-a|-b)",__schema__="repro_db"}[3m]) / on(l3,l4) group_left metric_b{l6="v6",l1="v1",l2="v2",l3=~"v3(|-a|-b)",__schema__="repro_db"}) > 0.50)) / count(rate(metric_a{l1="v1",l2="v2",l3=~"v3(|-a|-b)",__schema__="repro_db"}[3m]))) * 100"#;

const ANON_PROMQL_RATIO_REPRO_SCALAR_DIV: &str =
    r#"count(rate(metric_a{l1="v1",l2="v2",l3=~"v3(|-a|-b)",__schema__="repro_db"}[3m])) / 2"#;

#[apply(both_instances_cases)]
async fn sql_insert_tql_query_ceil(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_tql_assert(
        instance,
        r#"create table http_requests_total (
            host string,
            cpu double,
            memory double,
            ts timestamp TIME INDEX,
            PRIMARY KEY (host),
        );"#,
        r#"insert into http_requests_total(host, cpu, memory, ts) values
            ('host1', 66.6, 1024, 0),
            ('host1', 66.6, 2048, 2000),
            ('host1', 66.6, 4096, 5000),
            ('host1', 43.1, 8192, 7000),
            ('host1', 19.1, 10240, 9000),
            ('host1', 99.1, 20480, 10000),
            ('host1', 999.9, 40960, 21000),
            ('host1', 31.9,  8192, 22000),
            ('host1', 95.4,  333.3, 32000),
            ('host1', 12423.1,  1333.3, 49000),
            ('host1', 0,  2333.3, 80000),
            ('host1', 49,  3333.3, 99000);
        "#,
        "TQL EVAL (0,100,10) ceil(http_requests_total{host=\"host1\"})",
        "+---------------------+-----------+--------------+-------+\
        \n| ts                  | ceil(cpu) | ceil(memory) | host  |\
        \n+---------------------+-----------+--------------+-------+\
        \n| 1970-01-01T00:00:00 | 67.0      | 1024.0       | host1 |\
        \n| 1970-01-01T00:00:10 | 100.0     | 20480.0      | host1 |\
        \n| 1970-01-01T00:00:20 | 100.0     | 20480.0      | host1 |\
        \n| 1970-01-01T00:00:30 | 32.0      | 8192.0       | host1 |\
        \n| 1970-01-01T00:00:40 | 96.0      | 334.0        | host1 |\
        \n| 1970-01-01T00:00:50 | 12424.0   | 1334.0       | host1 |\
        \n| 1970-01-01T00:01:00 | 12424.0   | 1334.0       | host1 |\
        \n| 1970-01-01T00:01:10 | 12424.0   | 1334.0       | host1 |\
        \n| 1970-01-01T00:01:20 | 0.0       | 2334.0       | host1 |\
        \n| 1970-01-01T00:01:30 | 0.0       | 2334.0       | host1 |\
        \n| 1970-01-01T00:01:40 | 49.0      | 3334.0       | host1 |\
        \n+---------------------+-----------+--------------+-------+",
    )
    .await;
}

#[apply(both_instances_cases)]
async fn sql_insert_promql_query_ceil(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        r#"create table http_requests_total (
            host string,
            cpu double,
            memory double,
            ts timestamp TIME INDEX,
            PRIMARY KEY (host),
        );"#,
        r#"insert into http_requests_total(host, cpu, memory, ts) values
            ('host1', 66.6, 1024, 0),
            ('host1', 66.6, 2048, 2000),
            ('host1', 66.6, 4096, 5000),
            ('host1', 43.1, 8192, 7000),
            ('host1', 19.1, 10240, 9000),
            ('host1', 99.1, 20480, 10000),
            ('host1', 999.9, 40960, 21000),
            ('host1', 31.9,  8192, 22000),
            ('host1', 95.4,  333.3, 32000),
            ('host1', 12423.1,  1333.3, 49000),
            ('host1', 0,  2333.3, 80000),
            ('host1', 49,  3333.3, 99000);
        "#,
        "ceil(http_requests_total{host=\"host1\"})",
        UNIX_EPOCH,
        UNIX_EPOCH.checked_add(Duration::from_secs(100)).unwrap(),
        Duration::from_secs(5),
        Duration::from_secs(1),
        "+---------------------+-----------+--------------+-------+\
        \n| ts                  | ceil(cpu) | ceil(memory) | host  |\
        \n+---------------------+-----------+--------------+-------+\
        \n| 1970-01-01T00:00:00 | 67.0      | 1024.0       | host1 |\
        \n| 1970-01-01T00:00:05 | 67.0      | 4096.0       | host1 |\
        \n| 1970-01-01T00:00:10 | 100.0     | 20480.0      | host1 |\
        \n| 1970-01-01T00:01:20 | 0.0       | 2334.0       | host1 |\
        \n+---------------------+-----------+--------------+-------+",
    )
    .await;
}

const AGGREGATORS_CREATE_TABLE: &str = r#"create table http_requests (
    job string,
    instance string,
    "group" string,
    "value" double,
    ts timestamp TIME INDEX,
    PRIMARY KEY (job, instance, "group"),
);"#;

// load 5m
// http_requests{job="api-server", instance="0", group="production"} 0+10x10
// http_requests{job="api-server", instance="1", group="production"} 0+20x10
// http_requests{job="api-server", instance="0", group="canary"}   0+30x10
// http_requests{job="api-server", instance="1", group="canary"}   0+40x10
// http_requests{job="app-server", instance="0", group="production"} 0+50x10
// http_requests{job="app-server", instance="1", group="production"} 0+60x10
// http_requests{job="app-server", instance="0", group="canary"}   0+70x10
// http_requests{job="app-server", instance="1", group="canary"}   0+80x10
const AGGREGATORS_INSERT_DATA: &str = r#"insert into http_requests(job, instance, "group", value, ts) values
    ('api-server', '0', 'production', 100, 0),
    ('api-server', '1', 'production', 200, 0),
    ('api-server', '0', 'canary', 300, 0),
    ('api-server', '1', 'canary', 400, 0),
    ('app-server', '0', 'production', 500, 0),
    ('app-server', '1', 'production', 600, 0),
    ('app-server', '0', 'canary', 700, 0),
    ('app-server', '1', 'canary', 800, 0);"#;

fn unix_epoch_plus_100s() -> SystemTime {
    UNIX_EPOCH.checked_add(Duration::from_secs(100)).unwrap()
}

// # Simple sum.
// eval instant at 50m SUM BY (group) (http_requests{job="api-server"})
//   {group="canary"} 700
//   {group="production"} 300
#[apply(both_instances_cases)]
async fn aggregators_simple_sum(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        "SUM BY (group) (http_requests{job=\"api-server\"})",
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+------------+---------------------+--------------------------+\
        \n| group      | ts                  | sum(http_requests.value) |\
        \n+------------+---------------------+--------------------------+\
        \n| production | 1970-01-01T00:00:00 | 300.0                    |\
        \n| canary     | 1970-01-01T00:00:00 | 700.0                    |\
        \n+------------+---------------------+--------------------------+",
    )
    .await;
}

// # Simple average.
// eval instant at 50m avg by (group) (http_requests{job="api-server"})
//   {group="canary"} 350
//   {group="production"} 150
#[apply(both_instances_cases)]
async fn aggregators_simple_avg(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        "AVG BY (group) (http_requests{job=\"api-server\"})",
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+------------+---------------------+--------------------------+\
        \n| group      | ts                  | avg(http_requests.value) |\
        \n+------------+---------------------+--------------------------+\
        \n| production | 1970-01-01T00:00:00 | 150.0                    |\
        \n| canary     | 1970-01-01T00:00:00 | 350.0                    |\
        \n+------------+---------------------+--------------------------+",
    )
    .await;
}

// # Simple count.
// eval instant at 50m count by (group) (http_requests{job="api-server"})
//   {group="canary"} 2
//   {group="production"} 2
#[apply(both_instances_cases)]
async fn aggregators_simple_count(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        "COUNT BY (group) (http_requests{job=\"api-server\"})",
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+------------+---------------------+----------------------------+\
        \n| group      | ts                  | count(http_requests.value) |\
        \n+------------+---------------------+----------------------------+\
        \n| canary     | 1970-01-01T00:00:00 | 2                          |\
        \n| production | 1970-01-01T00:00:00 | 2                          |\
        \n+------------+---------------------+----------------------------+",
    )
    .await;
}

// Test like `aggregators_simple_count` but with value aliasing.
#[apply(both_instances_cases)]
async fn value_alias(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert_with_alias(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        "COUNT BY (group) (http_requests{job=\"api-server\"})",
        Some("my_series".to_string()),
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+-----------+------------+---------------------+\
        \n| my_series | group      | ts                  |\
        \n+-----------+------------+---------------------+\
        \n| 2         | canary     | 1970-01-01T00:00:00 |\
        \n| 2         | production | 1970-01-01T00:00:00 |\
        \n+-----------+------------+---------------------+",
    )
    .await;
}

// # Simple without.
// eval instant at 50m sum without (instance) (http_requests{job="api-server"})
//   {group="canary",job="api-server"} 700
//   {group="production",job="api-server"} 300
#[apply(both_instances_cases)]
async fn aggregators_simple_without(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        "sum without (instance) (http_requests{job=\"api-server\"})",
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+------------+------------+---------------------+--------------------------+\
        \n| group      | job        | ts                  | sum(http_requests.value) |\
        \n+------------+------------+---------------------+--------------------------+\
        \n| production | api-server | 1970-01-01T00:00:00 | 300.0                    |\
        \n| canary     | api-server | 1970-01-01T00:00:00 | 700.0                    |\
        \n+------------+------------+---------------------+--------------------------+",
    )
    .await;
}

// # Empty by.
// eval instant at 50m sum by () (http_requests{job="api-server"})
//   {} 1000
#[apply(both_instances_cases)]
async fn aggregators_empty_by(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        "sum by () (http_requests{job=\"api-server\"})",
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+---------------------+--------------------------+\
        \n| ts                  | sum(http_requests.value) |\
        \n+---------------------+--------------------------+\
        \n| 1970-01-01T00:00:00 | 1000.0                   |\
        \n+---------------------+--------------------------+",
    )
    .await;
}

// # No by/without.
// eval instant at 50m sum(http_requests{job="api-server"})
//   {} 1000
#[apply(both_instances_cases)]
async fn aggregators_no_by_without(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        r#"sum (http_requests{job="api-server"})"#,
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+---------------------+--------------------------+\
        \n| ts                  | sum(http_requests.value) |\
        \n+---------------------+--------------------------+\
        \n| 1970-01-01T00:00:00 | 1000.0                   |\
        \n+---------------------+--------------------------+",
    )
    .await;
}

// # Empty without.
// eval instant at 50m sum without () (http_requests{job="api-server",group="production"})
//   {group="production",job="api-server",instance="0"} 100
//   {group="production",job="api-server",instance="1"} 200
#[apply(both_instances_cases)]
async fn aggregators_empty_without(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        r#"sum without () (http_requests{job="api-server",group="production"})"#,
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+------------+----------+------------+---------------------+--------------------------+\
        \n| group      | instance | job        | ts                  | sum(http_requests.value) |\
        \n+------------+----------+------------+---------------------+--------------------------+\
        \n| production | 0        | api-server | 1970-01-01T00:00:00 | 100.0                    |\
        \n| production | 1        | api-server | 1970-01-01T00:00:00 | 200.0                    |\
        \n+------------+----------+------------+---------------------+--------------------------+",
    )
    .await;
}

// # Lower-cased aggregation operators should work too.
// eval instant at 50m sum(http_requests) by (job) + min(http_requests) by (job) + max(http_requests) by (job) + avg(http_requests) by (job)
//   {job="app-server"} 4550
//   {job="api-server"} 1750
#[apply(both_instances_cases)]
async fn aggregators_complex_combined_aggrs(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        "sum(http_requests) by (job) + min(http_requests) by (job) + max(http_requests) by (job) + avg(http_requests) by (job)",
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------+\
        \n| job        | ts                  | lhs.rhs.lhs.sum(http_requests.value) + rhs.min(http_requests.value) + http_requests.max(http_requests.value) + rhs.avg(http_requests.value) |\
        \n+------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------+\
        \n| api-server | 1970-01-01T00:00:00 | 1750.0                                                                                                                                      |\
        \n| app-server | 1970-01-01T00:00:00 | 4550.0                                                                                                                                      |\
        \n+------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------+",
    )
    .await;
}

// This is not from prometheus test set. It's derived from `aggregators_complex_combined_aggrs()`
#[apply(both_instances_cases)]
async fn two_aggregators_combined_aggrs(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        "sum(http_requests) by (job) + min(http_requests) by (job) ",
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+------------+---------------------+-------------------------------------------------------------+\
        \n| job        | ts                  | lhs.sum(http_requests.value) + rhs.min(http_requests.value) |\
        \n+------------+---------------------+-------------------------------------------------------------+\
        \n| api-server | 1970-01-01T00:00:00 | 1100.0                                                      |\
        \n| app-server | 1970-01-01T00:00:00 | 3100.0                                                      |\
        \n+------------+---------------------+-------------------------------------------------------------+",
    )
    .await;
}

// eval instant at 50m stddev by (instance)(http_requests)
//   {instance="0"} 223.60679774998
//   {instance="1"} 223.60679774998
//
// The values are compared with a tolerance rather than against a rendered table:
// `stddev_pop` merges partial aggregates in a parallelism-dependent order, so the
// last digits of its output are not stable across runs.
#[apply(both_instances_cases)]
async fn stddev_by_label(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    execute_all(&instance, AGGREGATORS_CREATE_TABLE, QueryContext::arc()).await;
    execute_all(&instance, AGGREGATORS_INSERT_DATA, QueryContext::arc()).await;

    let batches = promql_query_as_batches(
        instance,
        r#"stddev by (instance)(http_requests)"#,
        None,
        QueryContext::arc(),
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
    )
    .await;

    let batches = batches.iter().collect::<Vec<_>>();
    assert_eq!(batches.len(), 1);
    // Tag columns come back dictionary-encoded.
    let instances = cast(
        batches[0].column_by_name("instance").unwrap(),
        &DataType::Utf8,
    )
    .unwrap();
    let instances = instances.as_any().downcast_ref::<StringArray>().unwrap();
    let values = batches[0]
        .column_by_name("stddev_pop(http_requests.value)")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    let mut actual = (0..values.len())
        .map(|i| (instances.value(i), values.value(i)))
        .collect::<Vec<_>>();
    actual.sort_by(|a, b| a.0.cmp(b.0));
    assert_eq!(actual.len(), 2, "{}", batches[0].pretty_print());
    for (expected_instance, (instance, value)) in ["0", "1"].into_iter().zip(actual) {
        assert_eq!(instance, expected_instance);
        assert!(
            (value - 223.606_797_749_979).abs() < 1e-9,
            "instance {instance}: {value}"
        );
    }
}

// This is not derived from prometheus
#[apply(both_instances_cases)]
async fn binary_op_plain_columns(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        r#"http_requests - http_requests"#,
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+------------+----------+------------+---------------------+-----------------------+\
        \n| job        | instance | group      | ts                  | lhs.value - rhs.value |\
        \n+------------+----------+------------+---------------------+-----------------------+\
        \n| api-server | 0        | canary     | 1970-01-01T00:00:00 | 0.0                   |\
        \n| api-server | 0        | production | 1970-01-01T00:00:00 | 0.0                   |\
        \n| api-server | 1        | canary     | 1970-01-01T00:00:00 | 0.0                   |\
        \n| api-server | 1        | production | 1970-01-01T00:00:00 | 0.0                   |\
        \n| app-server | 0        | canary     | 1970-01-01T00:00:00 | 0.0                   |\
        \n| app-server | 0        | production | 1970-01-01T00:00:00 | 0.0                   |\
        \n| app-server | 1        | canary     | 1970-01-01T00:00:00 | 0.0                   |\
        \n| app-server | 1        | production | 1970-01-01T00:00:00 | 0.0                   |\
        \n+------------+----------+------------+---------------------+-----------------------+",
    )
    .await;
}

#[apply(both_instances_cases)]
async fn cross_schema_query(instance: Arc<dyn MockInstance>) {
    let ins = instance.frontend();

    ins.do_query(
        AGGREGATORS_CREATE_TABLE,
        QueryContext::with_db_name(Some("greptime_private")).into(),
    )
    .await
    .into_iter()
    .for_each(|v| {
        let _ = v.unwrap();
    });
    ins.do_query(
        AGGREGATORS_INSERT_DATA,
        QueryContext::with_db_name(Some("greptime_private")).into(),
    )
    .await
    .into_iter()
    .for_each(|v| {
        let _ = v.unwrap();
    });

    let start = UNIX_EPOCH;
    let end = unix_epoch_plus_100s();
    let interval = Duration::from_secs(60);
    let lookback_delta = Duration::from_secs(0);

    let query_output = promql_query(
        ins.clone(),
        r#"http_requests{__schema__="greptime_private"}"#,
        None,
        QueryContext::arc(),
        start,
        end,
        interval,
        lookback_delta,
    )
    .await
    .unwrap();

    let expected = r#"+------------+----------+------------+-------+---------------------+
| job        | instance | group      | value | ts                  |
+------------+----------+------------+-------+---------------------+
| api-server | 0        | production | 100.0 | 1970-01-01T00:00:00 |
| api-server | 0        | canary     | 300.0 | 1970-01-01T00:00:00 |
| api-server | 1        | production | 200.0 | 1970-01-01T00:00:00 |
| api-server | 1        | canary     | 400.0 | 1970-01-01T00:00:00 |
| app-server | 0        | canary     | 700.0 | 1970-01-01T00:00:00 |
| app-server | 0        | production | 500.0 | 1970-01-01T00:00:00 |
| app-server | 1        | canary     | 800.0 | 1970-01-01T00:00:00 |
| app-server | 1        | production | 600.0 | 1970-01-01T00:00:00 |
+------------+----------+------------+-------+---------------------+"#;

    check_unordered_output_stream(query_output, expected).await;

    let query_output = promql_query(
        ins.clone(),
        r#"http_requests{__database__="greptime_private"}"#,
        None,
        QueryContext::arc(),
        start,
        end,
        interval,
        lookback_delta,
    )
    .await
    .unwrap();

    let expected = r#"+------------+----------+------------+-------+---------------------+
| job        | instance | group      | value | ts                  |
+------------+----------+------------+-------+---------------------+
| api-server | 0        | production | 100.0 | 1970-01-01T00:00:00 |
| api-server | 0        | canary     | 300.0 | 1970-01-01T00:00:00 |
| api-server | 1        | production | 200.0 | 1970-01-01T00:00:00 |
| api-server | 1        | canary     | 400.0 | 1970-01-01T00:00:00 |
| app-server | 0        | canary     | 700.0 | 1970-01-01T00:00:00 |
| app-server | 0        | production | 500.0 | 1970-01-01T00:00:00 |
| app-server | 1        | canary     | 800.0 | 1970-01-01T00:00:00 |
| app-server | 1        | production | 600.0 | 1970-01-01T00:00:00 |
+------------+----------+------------+-------+---------------------+"#;

    check_unordered_output_stream(query_output, expected).await;

    let query_output = promql_query(
        ins.clone(),
        r#"http_requests"#,
        None,
        QueryContext::arc(),
        start,
        end,
        interval,
        lookback_delta,
    )
    .await
    .unwrap();
    let empty_result = r#"+------+-------+
| time | value |
+------+-------+
+------+-------+"#;
    check_unordered_output_stream(query_output, empty_result).await;

    let query_output = promql_query(
        ins.clone(),
        r#"http_requests"#,
        None,
        QueryContext::with_db_name(Some("greptime_private")).into(),
        start,
        end,
        interval,
        lookback_delta,
    )
    .await
    .unwrap();

    check_unordered_output_stream(query_output, expected).await;
}

#[apply(both_instances_cases)]
async fn anon_promql_ratio_repro(instance: Arc<dyn MockInstance>) {
    let ins = instance.frontend();

    execute_all(
        &ins,
        &format!("CREATE DATABASE {ANON_PROMQL_RATIO_REPRO_DB}"),
        QueryContext::arc(),
    )
    .await;

    let repro_ctx: Arc<QueryContext> =
        QueryContext::with_db_name(Some(ANON_PROMQL_RATIO_REPRO_DB)).into();
    execute_all(&ins, ANON_PROMQL_RATIO_REPRO_CREATE, repro_ctx.clone()).await;
    execute_all(&ins, ANON_PROMQL_RATIO_REPRO_INSERT, repro_ctx).await;

    let start = UNIX_EPOCH.checked_add(Duration::from_secs(180)).unwrap();
    let end = UNIX_EPOCH.checked_add(Duration::from_secs(360)).unwrap();
    let interval = Duration::from_secs(180);
    let lookback = Duration::from_secs(1);

    let numerator = promql_query_as_batches(
        ins.clone(),
        ANON_PROMQL_RATIO_REPRO_NUMERATOR,
        Some("num".to_string()),
        QueryContext::arc(),
        start,
        end,
        interval,
        lookback,
    )
    .await;
    let denominator = promql_query_as_batches(
        ins.clone(),
        ANON_PROMQL_RATIO_REPRO_DENOMINATOR,
        Some("den".to_string()),
        QueryContext::arc(),
        start,
        end,
        interval,
        lookback,
    )
    .await;
    let whole = promql_query_as_batches(
        ins.clone(),
        ANON_PROMQL_RATIO_REPRO_WHOLE,
        Some("pct".to_string()),
        QueryContext::arc(),
        start,
        end,
        interval,
        lookback,
    )
    .await;
    let scalar_div = promql_query_as_batches(
        ins,
        ANON_PROMQL_RATIO_REPRO_SCALAR_DIV,
        Some("half_den".to_string()),
        QueryContext::arc(),
        start,
        end,
        interval,
        lookback,
    )
    .await;

    let numerator = numerator.iter().collect::<Vec<_>>();
    let denominator = denominator.iter().collect::<Vec<_>>();
    let whole = whole.iter().collect::<Vec<_>>();
    let scalar_div = scalar_div.iter().collect::<Vec<_>>();

    let numerator_values = numerator[0]
        .column_by_name("num")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let denominator_values = denominator[0]
        .column_by_name("den")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let percentage_values = whole[0]
        .column_by_name("pct")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let scalar_div_values = scalar_div[0]
        .column_by_name("half_den")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(numerator_values.len(), 1, "{}", numerator[0].pretty_print());
    assert_eq!(
        denominator_values.len(),
        1,
        "{}",
        denominator[0].pretty_print()
    );
    assert_eq!(percentage_values.len(), 1, "{}", whole[0].pretty_print());
    assert_eq!(
        scalar_div_values.len(),
        1,
        "{}",
        scalar_div[0].pretty_print()
    );

    assert_eq!(
        numerator_values.value(0),
        1,
        "{}",
        numerator[0].pretty_print()
    );
    assert_eq!(
        denominator_values.value(0),
        3,
        "{}",
        denominator[0].pretty_print()
    );
    assert!(
        (scalar_div_values.value(0) - 1.5).abs() < 1e-9,
        "{}",
        scalar_div[0].pretty_print()
    );

    let expected = 100.0 / 3.0;
    assert!(
        (percentage_values.value(0) - expected).abs() < 1e-9,
        "{}",
        whole[0].pretty_print()
    );
}

#[apply(both_instances_cases)]
async fn promql_stale_marker_excludes_series_across_flushes(instance: Arc<dyn MockInstance>) {
    let ins = instance.frontend();
    let table = "promql_stale_marker";
    execute_all(
        &ins,
        &format!(
            "CREATE TABLE {table} (\
                series STRING, \
                ts TIMESTAMP TIME INDEX, \
                val DOUBLE, \
                PRIMARY KEY (series)\
            ) WITH (sst_format = 'flat')"
        ),
        QueryContext::arc(),
    )
    .await;

    let rows = |samples: &[(&str, f64, i64)]| Rows {
        schema: vec![
            ColumnSchema {
                column_name: "series".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "ts".to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "val".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
        ],
        rows: samples
            .iter()
            .map(|(series, value, timestamp)| Row {
                values: vec![
                    ValueData::StringValue((*series).to_string()).into(),
                    ValueData::TimestampMillisecondValue(*timestamp).into(),
                    ValueData::F64Value(*value).into(),
                ],
            })
            .collect(),
    };
    let insert = |samples: &[(&str, f64, i64)]| RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: table.to_string(),
            rows: Some(rows(samples)),
        }],
    };

    ins.handle_row_inserts(
        insert(&[("stale", 10.0, 1_000), ("ordinary", 20.0, 1_000)]),
        QueryContext::arc(),
        false,
        false,
    )
    .await
    .unwrap();
    execute_all(
        &ins,
        &format!("ADMIN FLUSH_TABLE('{table}')"),
        QueryContext::arc(),
    )
    .await;

    let ordinary_nan = f64::from_bits(0x7ff8_0000_0000_0000);
    ins.handle_row_inserts(
        insert(&[
            ("stale", f64::from_bits(PROMETHEUS_STALE_NAN_BITS), 2_000),
            ("ordinary", ordinary_nan, 2_000),
        ]),
        QueryContext::arc(),
        false,
        false,
    )
    .await
    .unwrap();

    let extract_samples = |batches: common_recordbatch::RecordBatches| {
        let mut samples = batches
            .iter()
            .flat_map(|batch| {
                let series = cast(
                    batch.column_by_name("series").unwrap().as_ref(),
                    &DataType::Utf8,
                )
                .unwrap();
                let series = series.as_any().downcast_ref::<StringArray>().unwrap();
                let values = batch
                    .column_by_name("val")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let timestamps = batch
                    .column_by_name("ts")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| {
                        (
                            series.value(row).to_string(),
                            values.value(row),
                            timestamps.value(row),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        samples.sort_by(|left, right| left.0.cmp(&right.0));
        samples
    };

    let raw_output = ins
        .do_query(
            &format!("SELECT series, val, ts FROM {table} WHERE ts = 2000"),
            QueryContext::arc(),
        )
        .await
        .remove(0)
        .unwrap();
    let raw_batches = match raw_output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(recordbatches) => recordbatches,
        _ => unreachable!(),
    };
    let raw_samples = extract_samples(raw_batches);
    assert_eq!(raw_samples.len(), 2, "{raw_samples:?}");
    let stale_marker = raw_samples
        .iter()
        .find(|(series, _, _)| series == "stale")
        .unwrap();
    assert_eq!(stale_marker.1.to_bits(), PROMETHEUS_STALE_NAN_BITS);
    let ordinary_marker = raw_samples
        .iter()
        .find(|(series, _, _)| series == "ordinary")
        .unwrap();
    assert_eq!(ordinary_marker.1.to_bits(), ordinary_nan.to_bits());

    // Keep the finite 1000ms sample eligible so a stale marker cannot fall back to it.
    let query_at = |at| {
        let time = UNIX_EPOCH.checked_add(Duration::from_millis(at)).unwrap();
        promql_query_as_batches(
            ins.clone(),
            table,
            None,
            QueryContext::arc(),
            time,
            time,
            Duration::from_secs(1),
            Duration::from_secs(3),
        )
    };
    let assert_nan_series = |samples: Vec<(String, f64, i64)>, at| {
        assert_eq!(samples.len(), 1, "{samples:?}");
        assert_eq!(samples[0].0, "ordinary", "{samples:?}");
        assert_eq!(samples[0].2, at, "{samples:?}");
        assert_eq!(
            samples[0].1.to_bits(),
            ordinary_nan.to_bits(),
            "{samples:?}"
        );
    };

    for flush_markers in [false, true] {
        if flush_markers {
            execute_all(
                &ins,
                &format!("ADMIN FLUSH_TABLE('{table}')"),
                QueryContext::arc(),
            )
            .await;
        }

        assert_eq!(
            extract_samples(query_at(1_500).await),
            vec![
                ("ordinary".to_string(), 20.0, 1_500),
                ("stale".to_string(), 10.0, 1_500)
            ]
        );
        assert_nan_series(extract_samples(query_at(2_000).await), 2_000);
        assert_nan_series(extract_samples(query_at(2_500).await), 2_500);
    }
}

// ---------------------------------------------------------------------------
// F06: `ScalarCalculate` (`scalar()` in TQL) order contract
// ---------------------------------------------------------------------------
//
// `scalar()` inside a TQL CTE is planned as `ScalarCalculate`
// (`src/promql/src/extension_plan/scalar_calculate.rs`). Its execution plan
// claims `maintains_input_order() == true` even though the node projects its
// input down to `(time_index, scalar(field))`, so its output column `ts@0` is
// not the input column `0`. DataFusion's sort push-down maps the sort key by
// column index, so the SQL-level sort over `scalar()` reads the wrong column:
//
// * `ORDER BY ts DESC` really sorts by `host` (ordinary Mito table) or by `val`
//   (metric logical table) and silently returns the wrong row order.
// * `ORDER BY "scalar(val)" ASC` does not order by the scalar value at all.
// * `LAG(...) OVER (ORDER BY ts DESC)` cannot be planned any more: the window
//   needs `ts@0 DESC` while the pushed-down sort provides `host@0 DESC` /
//   `val@0 DESC`, so planning fails with `StatusCode::PlanQuery` (code 3000).
//
// The controls below must keep passing: a bare selector keeps the `ts` order of
// its input, and `ORDER BY ts DESC LIMIT 3` keeps the correct `TopK` above
// `ScalarCalculate` (the fetch blocks the wrong push-down).
//
// Every comparison below is row-by-row in result order: sorting the rows before
// comparing them would hide exactly this bug.

/// `(ts_millis, val)` fixture shared by both engines.
///
/// The values are deliberately neither ascending nor descending, so a wrong row
/// order is visible in the value column too: `ORDER BY ts DESC` must pair
/// `ts = 3000` with `2.0`, not with `4.0`.
const F06_FIXTURE_ROWS: &[(i64, f64)] = &[(0, 4.0), (1000, 1.0), (2000, 3.0), (3000, 2.0)];

/// Truth of `ORDER BY ts DESC` over the F06 fixture.
const F06_ROWS_BY_TS_DESC: &[(i64, f64)] = &[(3000, 2.0), (2000, 3.0), (1000, 1.0), (0, 4.0)];

/// Truth of `ORDER BY ts DESC LIMIT 3` over the F06 fixture.
const F06_ROWS_BY_TS_DESC_LIMIT_3: &[(i64, f64)] = &[(3000, 2.0), (2000, 3.0), (1000, 1.0)];

/// Truth of `ORDER BY "scalar(val)" ASC` over the F06 fixture.
const F06_ROWS_BY_VALUE_ASC: &[(i64, f64)] = &[(1000, 1.0), (3000, 2.0), (2000, 3.0), (0, 4.0)];

/// Output column of a `scalar()` TQL query; it has to be quoted in SQL.
const F06_SCALAR_VALUE_COLUMN: &str = "scalar(val)";

/// Message fragment DataFusion uses when a window's required input ordering is
/// missing; it identifies the failure as the order-contract bug rather than an
/// unrelated planning error.
const F06_UNSATISFIED_ORDER_REQUIREMENT: &str = "does not satisfy order requirements";

/// Ordinary Mito fixture. `host` stays the first column on purpose: the wrong
/// push-down reuses output index `0` (`ts`) as input column `0` (`host`).
const F06_ORDINARY_TABLE: &str = "f06_scalar_plain";

const F06_ORDINARY_CREATE: &str = r#"
CREATE TABLE f06_scalar_plain (
    host STRING PRIMARY KEY,
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
);
"#;

/// Metric fixture; the physical table only carries the logical table, all
/// `INSERT`s, reads and TQL queries go to the logical `f06_scalar_metric`.
/// `ScalarCalculate` sees its input columns as `(val, host, ts)`, so the wrong
/// push-down turns `ts@0` into `val@0`.
const F06_METRIC_TABLE: &str = "f06_scalar_metric";

const F06_METRIC_CREATE: &str = r#"
CREATE TABLE f06_scalar_phy (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
) ENGINE=metric WITH ("physical_metric_table" = "");

CREATE TABLE f06_scalar_metric (
    host STRING,
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    PRIMARY KEY (host)
) ENGINE=metric WITH (on_physical_table = 'f06_scalar_phy');
"#;

fn f06_insert_sql(table: &str) -> String {
    format!(
        "INSERT INTO {table} (host, ts, val) VALUES \
         ('h', 0, 4), ('h', 1000, 1), ('h', 2000, 3), ('h', 3000, 2)"
    )
}

fn f06_fixture_sql(table: &str) -> String {
    format!("SELECT ts, val FROM {table} ORDER BY ts")
}

/// `ORDER BY ts DESC` over `scalar()`; this is the repro of the wrong sort
/// push-down.
fn f06_scalar_order_by_ts_desc_sql(table: &str) -> String {
    format!("WITH q AS (TQL EVAL (0, 3, '1s') scalar({table})) SELECT * FROM q ORDER BY ts DESC")
}

/// Same as [`f06_scalar_order_by_ts_desc_sql`] but with a fetch, which keeps the
/// correct `TopK` above `ScalarCalculate`.
fn f06_scalar_order_by_ts_desc_limit_3_sql(table: &str) -> String {
    format!(
        "WITH q AS (TQL EVAL (0, 3, '1s') scalar({table})) SELECT * FROM q \
         ORDER BY ts DESC LIMIT 3"
    )
}

/// `ORDER BY` on the scalar value instead of on the time index.
fn f06_scalar_order_by_value_asc_sql(table: &str) -> String {
    format!(
        r#"WITH q AS (TQL EVAL (0, 3, '1s') scalar({table})) SELECT * FROM q ORDER BY "scalar(val)" ASC"#
    )
}

/// A SQL window over `scalar()`; the window requires the `ts` order that the
/// wrong push-down removes.
fn f06_scalar_lag_order_by_ts_desc_sql(table: &str) -> String {
    format!(
        r#"WITH q AS (TQL EVAL (0, 3, '1s') scalar({table})) SELECT ts, "scalar(val)" AS val, LAG("scalar(val)") OVER (ORDER BY ts DESC) AS prev FROM q ORDER BY ts DESC"#
    )
}

/// Selector control: the `ts` order of a bare selector must stay correct.
fn f06_selector_order_by_ts_desc_sql(table: &str) -> String {
    format!("WITH q AS (TQL EVAL (0, 3, '1s') {table}) SELECT ts, val FROM q ORDER BY ts DESC")
}

/// Creates and fills one F06 fixture and also guards the fixture itself: an
/// ordering regression is only meaningful if the stored rows read back in `ts`
/// order.
async fn f06_setup(instance: &Arc<Instance>, create: &str, table: &str) {
    let insert = f06_insert_sql(table);
    for sql in [create, insert.as_str()] {
        for result in instance.do_query(sql, QueryContext::arc()).await {
            if let Err(err) = result {
                panic!("F06: setup statement failed: {err}\n{sql}");
            }
        }
    }

    let sql = f06_fixture_sql(table);
    for parallelism in [1, 4] {
        let rows = f06_ordered_rows(instance, &sql, parallelism, "val").await;
        assert_eq!(
            rows.as_slice(),
            F06_FIXTURE_ROWS,
            "F06: fixture `{table}` must read back in `ts` order (parallelism={parallelism})\n{sql}"
        );
    }
}

/// Runs one F06 query with a query parallelism hint and returns its rows in
/// result order as `(ts_millis, value_column)`; the rows are never sorted here.
async fn f06_ordered_rows(
    instance: &Arc<Instance>,
    sql: &str,
    parallelism: usize,
    value_column: &str,
) -> Vec<(i64, f64)> {
    let mut query_ctx = QueryContext::with_db_name(None);
    query_ctx.set_extension(QUERY_PARALLELISM_HINT, parallelism.to_string());
    let output = instance
        .do_query(sql, Arc::new(query_ctx))
        .await
        .remove(0)
        .unwrap_or_else(|err| panic!("F06: query failed: {err}\n{sql}"));
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("F06: expected a query result"),
    };

    let mut rows = Vec::new();
    for batch in batches.iter() {
        let ts = cast(
            batch.column_by_name("ts").unwrap().as_ref(),
            &DataType::Int64,
        )
        .unwrap();
        let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
        let values = batch.column_by_name(value_column).unwrap();
        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        for row in 0..batch.num_rows() {
            rows.push((ts.value(row), values.value(row)));
        }
    }
    rows
}

/// Runs an F06 query that is expected to fail and returns its status code with
/// the error message.
async fn f06_query_error(
    instance: &Arc<Instance>,
    sql: &str,
    parallelism: usize,
) -> (StatusCode, String) {
    let mut query_ctx = QueryContext::with_db_name(None);
    query_ctx.set_extension(QUERY_PARALLELISM_HINT, parallelism.to_string());
    match instance.do_query(sql, Arc::new(query_ctx)).await.remove(0) {
        Err(err) => (err.status_code(), err.output_msg()),
        Ok(_) => panic!("F06: expected `{sql}` to fail (parallelism={parallelism})"),
    }
}

/// Repro of the wrong sort push-down: `ORDER BY ts DESC` over `scalar()` is
/// pushed into the TQL input as `host@0 DESC` and silently returns the wrong row
/// order (the fixture order, not `ts`-descending).
#[apply(both_instances_cases)]
async fn f06_scalar_ordinary_table_order_by_ts_desc(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_ORDINARY_CREATE, F06_ORDINARY_TABLE).await;

    let sql = f06_scalar_order_by_ts_desc_sql(F06_ORDINARY_TABLE);
    for parallelism in [1, 4] {
        let rows = f06_ordered_rows(&instance, &sql, parallelism, F06_SCALAR_VALUE_COLUMN).await;
        assert_eq!(
            rows.as_slice(),
            F06_ROWS_BY_TS_DESC,
            "F06: `ORDER BY ts DESC` over scalar() must return `ts`-descending rows \
             (parallelism={parallelism})\n{sql}"
        );
    }
}

/// Same repro as [`f06_scalar_ordinary_table_order_by_ts_desc`] for a metric
/// logical table, where the wrong push-down uses `val@0 DESC`.
#[apply(both_instances_cases)]
async fn f06_scalar_metric_table_order_by_ts_desc(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_METRIC_CREATE, F06_METRIC_TABLE).await;

    let sql = f06_scalar_order_by_ts_desc_sql(F06_METRIC_TABLE);
    for parallelism in [1, 4] {
        let rows = f06_ordered_rows(&instance, &sql, parallelism, F06_SCALAR_VALUE_COLUMN).await;
        assert_eq!(
            rows.as_slice(),
            F06_ROWS_BY_TS_DESC,
            "F06: `ORDER BY ts DESC` over scalar() must return `ts`-descending rows \
             (parallelism={parallelism})\n{sql}"
        );
    }
}

/// Repro: ordering by the scalar value is broken as well, the rows come back in
/// the fixture order instead of `1.0, 2.0, 3.0, 4.0`.
#[apply(both_instances_cases)]
async fn f06_scalar_ordinary_table_order_by_value_asc(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_ORDINARY_CREATE, F06_ORDINARY_TABLE).await;

    let sql = f06_scalar_order_by_value_asc_sql(F06_ORDINARY_TABLE);
    for parallelism in [1, 4] {
        let rows = f06_ordered_rows(&instance, &sql, parallelism, F06_SCALAR_VALUE_COLUMN).await;
        assert_eq!(
            rows.as_slice(),
            F06_ROWS_BY_VALUE_ASC,
            "F06: `ORDER BY \"scalar(val)\" ASC` must order by the scalar value \
             (parallelism={parallelism})\n{sql}"
        );
    }
}

/// Same as [`f06_scalar_ordinary_table_order_by_value_asc`] for a metric logical
/// table.
#[apply(both_instances_cases)]
async fn f06_scalar_metric_table_order_by_value_asc(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_METRIC_CREATE, F06_METRIC_TABLE).await;

    let sql = f06_scalar_order_by_value_asc_sql(F06_METRIC_TABLE);
    for parallelism in [1, 4] {
        let rows = f06_ordered_rows(&instance, &sql, parallelism, F06_SCALAR_VALUE_COLUMN).await;
        assert_eq!(
            rows.as_slice(),
            F06_ROWS_BY_VALUE_ASC,
            "F06: `ORDER BY \"scalar(val)\" ASC` must order by the scalar value \
             (parallelism={parallelism})\n{sql}"
        );
    }
}

/// Control that must stay correct: the fetch keeps the `TopK` above
/// `ScalarCalculate`, so the `ts` order is right although the plain
/// `ORDER BY ts DESC` repro is wrong.
#[apply(both_instances_cases)]
async fn f06_scalar_ordinary_table_order_by_ts_desc_limit_3_control(
    instance: Arc<dyn MockInstance>,
) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_ORDINARY_CREATE, F06_ORDINARY_TABLE).await;

    let sql = f06_scalar_order_by_ts_desc_limit_3_sql(F06_ORDINARY_TABLE);
    for parallelism in [1, 4] {
        let rows = f06_ordered_rows(&instance, &sql, parallelism, F06_SCALAR_VALUE_COLUMN).await;
        assert_eq!(
            rows.as_slice(),
            F06_ROWS_BY_TS_DESC_LIMIT_3,
            "F06: `ORDER BY ts DESC LIMIT 3` over scalar() must keep returning the newest rows \
             (parallelism={parallelism})\n{sql}"
        );
    }
}

/// Metric logical table variant of
/// [`f06_scalar_ordinary_table_order_by_ts_desc_limit_3_control`].
#[apply(both_instances_cases)]
async fn f06_scalar_metric_table_order_by_ts_desc_limit_3_control(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_METRIC_CREATE, F06_METRIC_TABLE).await;

    let sql = f06_scalar_order_by_ts_desc_limit_3_sql(F06_METRIC_TABLE);
    for parallelism in [1, 4] {
        let rows = f06_ordered_rows(&instance, &sql, parallelism, F06_SCALAR_VALUE_COLUMN).await;
        assert_eq!(
            rows.as_slice(),
            F06_ROWS_BY_TS_DESC_LIMIT_3,
            "F06: `ORDER BY ts DESC LIMIT 3` over scalar() must keep returning the newest rows \
             (parallelism={parallelism})\n{sql}"
        );
    }
}

/// Selector control that must stay correct: a bare selector does not go through
/// `ScalarCalculate`, so `ORDER BY ts DESC` over it is right.
#[apply(both_instances_cases)]
async fn f06_selector_ordinary_table_order_by_ts_desc_control(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_ORDINARY_CREATE, F06_ORDINARY_TABLE).await;

    let sql = f06_selector_order_by_ts_desc_sql(F06_ORDINARY_TABLE);
    for parallelism in [1, 4] {
        let rows = f06_ordered_rows(&instance, &sql, parallelism, "val").await;
        assert_eq!(
            rows.as_slice(),
            F06_ROWS_BY_TS_DESC,
            "F06: a bare selector must keep its `ts` order under `ORDER BY ts DESC` \
             (parallelism={parallelism})\n{sql}"
        );
    }
}

/// Metric logical table variant of
/// [`f06_selector_ordinary_table_order_by_ts_desc_control`].
#[apply(both_instances_cases)]
async fn f06_selector_metric_table_order_by_ts_desc_control(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_METRIC_CREATE, F06_METRIC_TABLE).await;

    let sql = f06_selector_order_by_ts_desc_sql(F06_METRIC_TABLE);
    for parallelism in [1, 4] {
        let rows = f06_ordered_rows(&instance, &sql, parallelism, "val").await;
        assert_eq!(
            rows.as_slice(),
            F06_ROWS_BY_TS_DESC,
            "F06: a bare selector must keep its `ts` order under `ORDER BY ts DESC` \
             (parallelism={parallelism})\n{sql}"
        );
    }
}

/// Repro of the planning failure caused by the same wrong push-down: the window
/// needs `ts@0 DESC`, the pushed-down sort only provides `host@0 DESC`, so this
/// query currently fails with code 3000.
///
/// Once `ScalarCalculate` stops claiming `maintains_input_order`, the query must
/// succeed and return exactly `(3000, 2.0, NULL)`, `(2000, 3.0, 2.0)`,
/// `(1000, 1.0, 3.0)`, `(0, 4.0, 1.0)`; this assertion then has to be replaced
/// by that ordered-row check together with the fix.
#[apply(both_instances_cases)]
async fn f06_scalar_ordinary_table_lag_order_by_ts_desc(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_ORDINARY_CREATE, F06_ORDINARY_TABLE).await;

    let sql = f06_scalar_lag_order_by_ts_desc_sql(F06_ORDINARY_TABLE);
    for parallelism in [1, 4] {
        let (code, message) = f06_query_error(&instance, &sql, parallelism).await;
        assert_eq!(
            code,
            StatusCode::PlanQuery,
            "F06: `LAG(...) OVER (ORDER BY ts DESC)` over scalar() must fail with a planning \
             error while the sort push-down is wrong (parallelism={parallelism}): {message}"
        );
        assert!(
            message.contains(F06_UNSATISFIED_ORDER_REQUIREMENT),
            "F06: the planning error must be the missing `ts` order of the window input, not an \
             unrelated planning failure (parallelism={parallelism}): {message}"
        );
    }
}

/// Metric logical table variant of
/// [`f06_scalar_ordinary_table_lag_order_by_ts_desc`].
#[apply(both_instances_cases)]
async fn f06_scalar_metric_table_lag_order_by_ts_desc(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_METRIC_CREATE, F06_METRIC_TABLE).await;

    let sql = f06_scalar_lag_order_by_ts_desc_sql(F06_METRIC_TABLE);
    for parallelism in [1, 4] {
        let (code, message) = f06_query_error(&instance, &sql, parallelism).await;
        assert_eq!(
            code,
            StatusCode::PlanQuery,
            "F06: `LAG(...) OVER (ORDER BY ts DESC)` over scalar() must fail with a planning \
             error while the sort push-down is wrong (parallelism={parallelism}): {message}"
        );
        assert!(
            message.contains(F06_UNSATISFIED_ORDER_REQUIREMENT),
            "F06: the planning error must be the missing `ts` order of the window input, not an \
             unrelated planning failure (parallelism={parallelism}): {message}"
        );
    }
}
