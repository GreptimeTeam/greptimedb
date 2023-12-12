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

use frontend::instance::Instance;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use rstest::rstest;
use rstest_reuse::apply;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;

use super::test_util::{check_unordered_output_stream, standalone, standalone_instance_case};
use crate::tests::test_util::MockInstance;

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

    let query = PromQuery {
        query: promql.to_string(),
        ..PromQuery::default()
    };
    let QueryStatement::Promql(mut eval_stmt) = QueryLanguageParser::parse_promql(&query).unwrap()
    else {
        unreachable!()
    };
    eval_stmt.start = start;
    eval_stmt.end = end;
    eval_stmt.interval = interval;
    eval_stmt.lookback_delta = lookback;

    let query_output = instance
        .statement_executor()
        .execute_stmt(QueryStatement::Promql(eval_stmt), QueryContext::arc())
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

// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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

// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        \n| 1970-01-01T00:00:50 | 12424.0   | 1334.0       | host1 |\
        \n| 1970-01-01T00:01:20 | 0.0       | 2334.0       | host1 |\
        \n| 1970-01-01T00:01:40 | 49.0      | 3334.0       | host1 |\
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
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        \n| group      | ts                  | SUM(http_requests.value) |\
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
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        \n| group      | ts                  | AVG(http_requests.value) |\
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
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        \n| group      | ts                  | COUNT(http_requests.value) |\
        \n+------------+---------------------+----------------------------+\
        \n| canary     | 1970-01-01T00:00:00 | 2                          |\
        \n| production | 1970-01-01T00:00:00 | 2                          |\
        \n+------------+---------------------+----------------------------+",
    )
    .await;
}

// # Simple without.
// eval instant at 50m sum without (instance) (http_requests{job="api-server"})
//   {group="canary",job="api-server"} 700
//   {group="production",job="api-server"} 300
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        \n| group      | job        | ts                  | SUM(http_requests.value) |\
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
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        \n| ts                  | SUM(http_requests.value) |\
        \n+---------------------+--------------------------+\
        \n| 1970-01-01T00:00:00 | 1000.0                   |\
        \n+---------------------+--------------------------+",
    )
    .await;
}

// # No by/without.
// eval instant at 50m sum(http_requests{job="api-server"})
//   {} 1000
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        \n| ts                  | SUM(http_requests.value) |\
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
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        \n| group      | instance | job        | ts                  | SUM(http_requests.value) |\
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
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        "+------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\
                \n| job        | ts                  | http_requests.http_requests.http_requests.SUM(http_requests.value) + http_requests.MIN(http_requests.value) + http_requests.MAX(http_requests.value) + http_requests.AVG(http_requests.value) |\
                \n+------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\
                \n| api-server | 1970-01-01T00:00:00 | 1750.0                                                                                                                                                                                        |\
                \n| app-server | 1970-01-01T00:00:00 | 4550.0                                                                                                                                                                                        |\
                \n+------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
    )
    .await;
}

// This is not from prometheus test set. It's derived from `aggregators_complex_combined_aggrs()`
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        "+------------+---------------------+---------------------------------------------------------------------------------+\
        \n| job        | ts                  | http_requests.SUM(http_requests.value) + http_requests.MIN(http_requests.value) |\
        \n+------------+---------------------+---------------------------------------------------------------------------------+\
        \n| api-server | 1970-01-01T00:00:00 | 1100.0                                                                          |\
        \n| app-server | 1970-01-01T00:00:00 | 3100.0                                                                          |\
        \n+------------+---------------------+---------------------------------------------------------------------------------+",
    )
    .await;
}

// eval instant at 50m stddev by (instance)(http_requests)
//   {instance="0"} 223.60679774998
//   {instance="1"} 223.60679774998
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
#[ignore = "TODO(ruihang): fix this case"]
async fn stddev_by_label(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    create_insert_query_assert(
        instance,
        AGGREGATORS_CREATE_TABLE,
        AGGREGATORS_INSERT_DATA,
        r#"stddev by (instance)(http_requests)"#,
        UNIX_EPOCH,
        unix_epoch_plus_100s(),
        Duration::from_secs(60),
        Duration::from_secs(0),
        "+----------+---------------------+--------------------------------+\
        \n| instance | ts                  | STDDEVPOP(http_requests.value) |\
        \n+----------+---------------------+--------------------------------+\
        \n| 0        | 1970-01-01T00:00:00 | 223.606797749979               |\
        \n| 1        | 1970-01-01T00:00:00 | 223.606797749979               |\
        \n+----------+---------------------+--------------------------------+",
    )
    .await;
}

// This is not derived from prometheus
// should apply to both instances. tracked in #1296
#[apply(standalone_instance_case)]
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
        "+------------+----------+------------+---------------------+-------------------------------------------+\
        \n| job        | instance | group      | ts                  | http_requests.value - http_requests.value |\
        \n+------------+----------+------------+---------------------+-------------------------------------------+\
        \n| api-server | 0        | canary     | 1970-01-01T00:00:00 | 0.0                                       |\
        \n| api-server | 0        | production | 1970-01-01T00:00:00 | 0.0                                       |\
        \n| api-server | 1        | canary     | 1970-01-01T00:00:00 | 0.0                                       |\
        \n| api-server | 1        | production | 1970-01-01T00:00:00 | 0.0                                       |\
        \n| app-server | 0        | canary     | 1970-01-01T00:00:00 | 0.0                                       |\
        \n| app-server | 0        | production | 1970-01-01T00:00:00 | 0.0                                       |\
        \n| app-server | 1        | canary     | 1970-01-01T00:00:00 | 0.0                                       |\
        \n| app-server | 1        | production | 1970-01-01T00:00:00 | 0.0                                       |\
        \n+------------+----------+------------+---------------------+-------------------------------------------+",
    )
    .await;
}
