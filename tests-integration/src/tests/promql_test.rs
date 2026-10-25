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

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType,
};
use common_base::Plugins;
use common_query::prelude::{GREPTIME_TEMPORALITY_DELTA, OTLP_AGGREGATION_TEMPORALITY_LABEL};
use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use common_query::{Output, OutputData};
use common_recordbatch::util::collect_batches;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::coop::CooperativeExec;
use datatypes::arrow::array::{
    Array, Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::DataType;
use frontend::instance::Instance;
use query::datafusion::{QUERY_FALLBACK_HINT, QUERY_PARALLELISM_HINT};
use query::options::QueryOptions;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::physical_wrapper::{PhysicalPlanWrapper, PhysicalPlanWrapperRef};
use query::promql::planner::NATIVE_PROMQL_EXTENSION_KEY;
use rstest::rstest;
use rstest_reuse::apply;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use sql::ast::{Ident, ObjectName, ObjectNamePart};
use sql::statements::create::CreateView;
use sql::statements::statement::Statement as SqlStatement;
use sql::statements::tql::{Tql, TqlEval};
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_engine::RawLabelSource;
use table::table::scan::SeriesScanExec;

use super::test_util::{
    both_instances_cases, check_unordered_output_stream, distributed, standalone,
};
use crate::standalone::GreptimeDbStandaloneBuilder;
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

#[tokio::test(flavor = "multi_thread")]
async fn histogram_fraction_binary_join_preserves_partitioned_rows() {
    common_telemetry::init_default_ut_logging();

    let distributed = crate::tests::create_distributed_instance(
        "histogram_fraction_binary_join_preserves_partitioned_rows",
    )
    .await;
    let instance = distributed.frontend();

    for (table, create) in [
        (
            "pa_promql_m",
            r#"CREATE TABLE pa_promql_m (
                ts TIMESTAMP TIME INDEX,
                host STRING,
                shard STRING,
                val DOUBLE,
                PRIMARY KEY (host, shard)
            ) PARTITION ON COLUMNS (shard) (
                shard < 'b',
                shard >= 'b' AND shard < 'c',
                shard >= 'c' AND shard < 'd',
                shard >= 'd'
            );"#,
        ),
        (
            "lg_hist_m_2",
            r#"CREATE TABLE phy_hist_m_2 (
                ts TIMESTAMP TIME INDEX,
                host STRING,
                shard STRING,
                val DOUBLE,
                PRIMARY KEY (host, shard)
            ) PARTITION ON COLUMNS (shard) (
                shard < 'b',
                shard >= 'b' AND shard < 'c',
                shard >= 'c' AND shard < 'd',
                shard >= 'd'
            ) ENGINE=metric WITH ('physical_metric_table' = 'true');
            CREATE TABLE lg_hist_m_2 (
                ts TIMESTAMP TIME INDEX,
                host STRING,
                shard STRING,
                val DOUBLE,
                PRIMARY KEY (host, shard)
            ) PARTITION ON COLUMNS (shard) (
                shard < 'b',
                shard >= 'b' AND shard < 'c',
                shard >= 'c' AND shard < 'd',
                shard >= 'd'
            ) ENGINE=metric WITH ('on_physical_table' = 'phy_hist_m_2');"#,
        ),
    ] {
        execute_all(
            &instance,
            &format!(
                "{create}\nINSERT INTO {table} (ts, host, shard, val) VALUES\n                    (0, 'h', 'a', 1),\n                    (0, 'h', 'b', 1),\n                    (0, 'h', 'c', 1),\n                    (0, 'h', 'd', 1);"
            ),
            QueryContext::arc(),
        )
        .await;

        let expr = format!(
            r#"histogram_fraction(-Inf,+Inf,abs(label_replace(({table} + on(host) group_left() (sum by(host)({table}))),"le","+Inf","missing_label",".*")))"#
        );
        for parallelism in [1, 4] {
            let mut query_ctx = QueryContext::with_db_name(None);
            query_ctx.set_extension(QUERY_PARALLELISM_HINT, parallelism.to_string());
            let query_ctx = Arc::new(query_ctx);
            for (query, expected_value) in [
                (format!("TQL EVAL (0, 0, '1s') {expr}"), 1.0),
                (
                    format!("TQL EVAL (0, 0, '1s') {expr} + on(host, shard) {table}"),
                    2.0,
                ),
            ] {
                let output = instance
                    .do_query(&query, query_ctx.clone())
                    .await
                    .remove(0)
                    .unwrap();
                let batches = match output.data {
                    OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
                    OutputData::RecordBatches(batches) => batches,
                    other => panic!("expected result batches, got {other:?}"),
                };
                let mut rows = batches
                    .iter()
                    .flat_map(|batch| {
                        let ts = batch
                            .column_by_name("ts")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        let host_array =
                            cast(batch.column_by_name("host").unwrap(), &DataType::Utf8).unwrap();
                        let host = host_array.as_any().downcast_ref::<StringArray>().unwrap();
                        let shard_array =
                            cast(batch.column_by_name("shard").unwrap(), &DataType::Utf8).unwrap();
                        let shard = shard_array.as_any().downcast_ref::<StringArray>().unwrap();
                        let value = batch
                            .columns()
                            .iter()
                            .find_map(|column| column.as_any().downcast_ref::<Float64Array>())
                            .unwrap();
                        (0..batch.num_rows())
                            .map(move |row| {
                                (
                                    ts.value(row),
                                    host.value(row).to_owned(),
                                    shard.value(row).to_owned(),
                                    value.value(row),
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                rows.sort_by(|left, right| left.2.cmp(&right.2));
                assert_eq!(
                    rows,
                    ["a", "b", "c", "d"]
                        .into_iter()
                        .map(|shard| (0, "h".to_owned(), shard.to_owned(), expected_value))
                        .collect::<Vec<_>>(),
                    "query_parallelism={parallelism}, query={query}"
                );
            }
        }
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
// (`src/promql/src/extension_plan/scalar_calculate.rs`). Before this fix, its execution plan
// claimed `maintains_input_order() == true` even though the node projects its
// input down to `(time_index, scalar(field))`, so its output column `ts@0` is
// not the input column `0`. DataFusion's sort push-down mapped the sort key by
// column index, so the SQL-level sort over `scalar()` read the wrong column:
//
// * `ORDER BY ts DESC` really sorted by `host` (ordinary Mito table) or by `val`
//   (metric logical table) and silently returned the wrong row order.
// * `ORDER BY "scalar(val)" ASC` did not order by the scalar value at all.
// * `LAG(...) OVER (ORDER BY ts DESC)` could not be planned before this fix:
//   the window needed `ts@0 DESC` while the pushed-down sort provided
//   `host@0 DESC` / `val@0 DESC`, so planning failed with code 3000.
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

/// Truth of `LAG("scalar(val)") OVER (ORDER BY ts DESC)` over the F06 fixture:
/// the row order, the scalar values and the lagged values `(ts_millis, val,
/// prev)`. The window's first row has no predecessor, so its `prev` is `NULL`.
const F06_ROWS_LAG_BY_TS_DESC: &[(i64, f64, Option<f64>)] = &[
    (3000, 2.0, None),
    (2000, 3.0, Some(2.0)),
    (1000, 1.0, Some(3.0)),
    (0, 4.0, Some(1.0)),
];

/// Output column of a `scalar()` TQL query; it has to be quoted in SQL.
const F06_SCALAR_VALUE_COLUMN: &str = "scalar(val)";

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

/// Runs the F06 `LAG` query and returns its rows in result order as
/// `(ts_millis, val, prev)`; `prev` is `None` for the first row of the window.
/// The rows are never sorted here.
async fn f06_ordered_lag_rows(
    instance: &Arc<Instance>,
    sql: &str,
    parallelism: usize,
) -> Vec<(i64, f64, Option<f64>)> {
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
        let values = batch.column_by_name("val").unwrap();
        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        let prevs = batch.column_by_name("prev").unwrap();
        let prevs = prevs.as_any().downcast_ref::<Float64Array>().unwrap();
        for row in 0..batch.num_rows() {
            let prev = if prevs.is_null(row) {
                None
            } else {
                Some(prevs.value(row))
            };
            rows.push((ts.value(row), values.value(row), prev));
        }
    }
    rows
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

/// Repro of the planning failure that the wrong sort push-down used to cause:
/// the window needs `ts@0 DESC`, the pushed-down sort only provided
/// `host@0 DESC`, so this query failed with code 3000 while `ScalarCalculate`
/// claimed `maintains_input_order`.
///
/// Now that the node stops claiming the input order, the window is planned
/// above the correct sort and the query must return exactly
/// `(3000, 2.0, NULL)`, `(2000, 3.0, 2.0)`, `(1000, 1.0, 3.0)`,
/// `(0, 4.0, 1.0)`.
#[apply(both_instances_cases)]
async fn f06_scalar_ordinary_table_lag_order_by_ts_desc(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    f06_setup(&instance, F06_ORDINARY_CREATE, F06_ORDINARY_TABLE).await;

    let sql = f06_scalar_lag_order_by_ts_desc_sql(F06_ORDINARY_TABLE);
    for parallelism in [1, 4] {
        let rows = f06_ordered_lag_rows(&instance, &sql, parallelism).await;
        assert_eq!(
            rows.as_slice(),
            F06_ROWS_LAG_BY_TS_DESC,
            "F06: `LAG(...) OVER (ORDER BY ts DESC)` over scalar() must return `ts`-descending \
             rows with the lagged scalar values (parallelism={parallelism})\n{sql}"
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
        let rows = f06_ordered_lag_rows(&instance, &sql, parallelism).await;
        assert_eq!(
            rows.as_slice(),
            F06_ROWS_LAG_BY_TS_DESC,
            "F06: `LAG(...) OVER (ORDER BY ts DESC)` over scalar() must return `ts`-descending \
             rows with the lagged scalar values (parallelism={parallelism})\n{sql}"
        );
    }
}

#[derive(Default)]
struct NativePlanObserver(Mutex<Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>>);

impl NativePlanObserver {
    fn clear(&self) {
        *self.0.lock().unwrap() = None;
    }

    fn take(&self) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
        self.0
            .lock()
            .unwrap()
            .take()
            .expect("native fused plan was captured")
    }

    /// The fused node the last wrapped plan contained, if any; `take` requires one instead.
    fn fused(&self) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.0.lock().unwrap().clone()
    }
}

impl PhysicalPlanWrapper for NativePlanObserver {
    fn wrap(
        &self,
        origin: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _ctx: session::context::QueryContextRef,
    ) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
        let mut stack = vec![origin.clone()];
        while let Some(node) = stack.pop() {
            if node.name() == "FusedSeriesExec" {
                *self.0.lock().unwrap() = Some(node);
                break;
            }
            stack.extend(node.children().into_iter().cloned());
        }
        origin
    }
}

fn native_query_context(enabled: bool, parallelism: usize) -> Arc<QueryContext> {
    let mut ctx = QueryContext::with_db_name(None);
    ctx.set_extension(QUERY_PARALLELISM_HINT, parallelism.to_string());
    ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, enabled.to_string());
    Arc::new(ctx)
}

async fn native_tql_output(instance: &Arc<Instance>, sql: &str, enabled: bool) -> Output {
    instance
        .do_query(sql, native_query_context(enabled, 4))
        .await
        .remove(0)
        .unwrap()
}

async fn native_output_text(output: Output) -> String {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("query returns record batches"),
    };
    batches.pretty_print().unwrap()
}

async fn native_result_rows(output: Output) -> Vec<(i64, String, f64)> {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let timestamps = batch
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let hosts = cast(
                batch.column_by_name("host").unwrap().as_ref(),
                &DataType::Utf8,
            )
            .unwrap();
            let hosts = hosts.as_any().downcast_ref::<StringArray>().unwrap();
            let value_columns = batch
                .columns()
                .iter()
                .filter(|column| column.data_type() == &DataType::Float64)
                .collect::<Vec<_>>();
            assert_eq!(value_columns.len(), 1, "expected one Float64 value column");
            let values = value_columns[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(timestamps.null_count(), 0);
            assert_eq!(hosts.null_count(), 0);
            assert_eq!(values.null_count(), 0);
            (0..batch.num_rows())
                .map(|row| {
                    (
                        timestamps.value(row),
                        hosts.value(row).to_string(),
                        values.value(row),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
    rows
}

/// [`native_result_rows`] plus each row's OTLP temporality marker, sorted by label then time.
async fn native_marker_rows(output: Output) -> Vec<(i64, String, String, f64)> {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let timestamps = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let hosts = cast(
            batch.column_by_name("host").unwrap().as_ref(),
            &DataType::Utf8,
        )
        .unwrap();
        let hosts = hosts.as_any().downcast_ref::<StringArray>().unwrap();
        let markers = cast(
            batch
                .column_by_name(OTLP_AGGREGATION_TEMPORALITY_LABEL)
                .unwrap()
                .as_ref(),
            &DataType::Utf8,
        )
        .unwrap();
        let markers = markers.as_any().downcast_ref::<StringArray>().unwrap();
        let values = batch
            .columns()
            .iter()
            .filter(|column| column.data_type() == &DataType::Float64)
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 1, "expected one Float64 value column");
        let values = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(
            markers.null_count(),
            0,
            "every row must keep its temporality label"
        );
        assert_eq!(values.null_count(), 0);
        for row in 0..batch.num_rows() {
            rows.push((
                timestamps.value(row),
                hosts.value(row).to_string(),
                markers.value(row).to_string(),
                values.value(row),
            ));
        }
    }
    rows.sort_by(|left, right| (left.1.as_str(), left.0).cmp(&(right.1.as_str(), right.0)));
    rows
}

/// Compares the whole answer against `expected`, in the same label order, key by key and value by
/// value; a duplicated or missing series therefore fails on the keys rather than on the count.
fn assert_delta_rows(
    rows: &[(i64, String, String, f64)],
    expected: &[(i64, &str, &str, f64)],
    context: &str,
) {
    let actual = rows
        .iter()
        .map(|(ts, host, marker, value)| (*ts, host.as_str(), marker.as_str(), *value))
        .collect::<Vec<_>>();
    assert_eq!(actual.len(), expected.len(), "{context}: rows {actual:?}");
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(
            (actual.0, actual.1, actual.2),
            (expected.0, expected.1, expected.2),
            "{context}: row {actual:?}"
        );
        assert!(
            (actual.3 - expected.3).abs() <= 1e-9 * expected.3.abs().max(1.0),
            "{context}: series {:?} must report {}, found {}",
            actual.1,
            expected.3,
            actual.3
        );
    }
}

fn assert_four_native_partitions(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    use datafusion::physical_plan::metrics::MetricValue;

    assert_eq!(plan.name(), "FusedSeriesExec");
    assert_eq!(plan.output_partitioning().partition_count(), 4);
    assert_eq!(
        plan.children()[0].output_partitioning().partition_count(),
        4,
        "fused scan must have four partitions"
    );
    let mut values_by_partition = [0_u64; 4];
    for metric in plan.metrics().expect("fused plan metrics").iter() {
        if metric.value().name() == "native.root.output_values"
            && let MetricValue::Count { count, .. } = metric.value()
            && let Some(partition) = metric.partition()
            && let Some(value) = values_by_partition.get_mut(partition)
        {
            *value += count.value() as u64;
        }
    }
    assert!(
        values_by_partition.iter().all(|value| *value > 0),
        "every native execution partition must produce values: {values_by_partition:?}"
    );
}

/// A standalone instance with the native route opted in, and an observer of the plans it runs.
async fn native_test_instance(
    builder: GreptimeDbStandaloneBuilder,
) -> (
    crate::standalone::GreptimeDbStandalone,
    Arc<NativePlanObserver>,
) {
    native_test_instance_with_options(
        builder,
        QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        },
    )
    .await
}

/// [`native_test_instance`] with the engine-wide query options spelled out by the caller.
async fn native_test_instance_with_options(
    builder: GreptimeDbStandaloneBuilder,
    options: QueryOptions,
) -> (
    crate::standalone::GreptimeDbStandalone,
    Arc<NativePlanObserver>,
) {
    let observer = Arc::new(NativePlanObserver::default());
    let plugins = Plugins::new();
    let wrapper: PhysicalPlanWrapperRef = observer.clone();
    plugins.insert(wrapper);
    let standalone = builder
        .with_plugin(plugins)
        .with_query_options(options)
        .build()
        .await;
    (standalone, observer)
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_executes_fused_node_and_matches_legacy() {
    common_telemetry::init_default_ut_logging();
    // The fixture opts in so its single bulk insert is distributed across real TSID scan
    // partitions; legacy may send its one batch only to partition 0.
    let mito_config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let builder =
        GreptimeDbStandaloneBuilder::new("native_promql_fused_e2e").with_mito_config(mito_config);
    let (standalone, observer) = native_test_instance(builder).await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_e2e_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH (\"physical_metric_table\" = \"true\"); CREATE TABLE native_e2e (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH (\"on_physical_table\" = 'native_e2e_phy')",
        QueryContext::arc(),
    )
    .await;
    execute_all(&instance, "INSERT INTO native_e2e (host, ts, val) VALUES ('a', 0, 2), ('b', 0, 4), ('c', 0, 6), ('d', 0, 8), ('e', 0, 10), ('f', 0, 12), ('g', 0, 14), ('h', 0, 16), ('i', 0, 18), ('j', 0, 20), ('k', 0, 22), ('l', 0, 24), ('m', 0, 26), ('n', 0, 28), ('o', 0, 30), ('p', 0, 32)", QueryContext::arc()).await;
    let query = PromQuery {
        query: "native_e2e".to_string(),
        start: "0".to_string(),
        end: "0".to_string(),
        step: "1s".to_string(),
        ..PromQuery::default()
    };
    observer.clear();
    let native = instance
        .do_promql_query(&query, native_query_context(true, 4))
        .await
        .remove(0)
        .unwrap();
    let native_rows = native_result_rows(native).await;
    let plan = observer.take();
    assert_four_native_partitions(&plan);
    let expected = (b'a'..=b'p')
        .zip((1..=16).map(|index| index as f64 * 2.0))
        .map(|(host, value)| (0, (host as char).to_string(), value))
        .collect::<Vec<_>>();
    let legacy_rows = native_result_rows(
        instance
            .do_promql_query(&query, native_query_context(false, 4))
            .await
            .remove(0)
            .unwrap(),
    )
    .await;
    assert_eq!(native_rows, expected);
    assert_eq!(native_rows, legacy_rows);
}

/// Regression for the two occurrences of one selector that read one scan (#9896).
///
/// `m{host!=""} - (m{host!=""} * 2)` names one selector twice; the native route binds both tree
/// inputs to the one physical child it prepares, which needs an ordinary Mito table keyed by the
/// whole label set (`host` NOT NULL, empty match rejected). Series scans read 8,192-row batches
/// and hand whole series to successive partitions, so the fixture must exceed one batch: 512
/// hosts x 64 points is 32,768 rows, i.e. at least four batches reaching all four partitions.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_two_occurrences_route_one_scan_per_partition() {
    // 512 hosts * 64 points = 32,768 rows, generated in 512-row insert batches: at least four
    // 8,192-row read batches, so the ordinary scan spreads the series over all four partitions.
    const HOSTS: usize = 512;
    const POINTS_PER_HOST: i64 = 64;
    const INSERT_BATCH_ROWS: usize = 512;

    fn value(host: usize, point: i64) -> f64 {
        host as f64 * 1_000.0 + point as f64
    }

    fn host_name(host: usize) -> String {
        // Three digits keep the names in numeric order for every host below 1000: the row
        // comparison sorts by label text, and the expected rows are generated in numeric order.
        format!("h{host:03}")
    }

    fn insert_request(table: &str, schema: &[ColumnSchema], rows: Vec<Row>) -> RowInsertRequests {
        RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: table.to_string(),
                rows: Some(Rows {
                    schema: schema.to_vec(),
                    rows,
                }),
            }],
        }
    }

    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_two_occurrences_e2e",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    let table = "native_pair_route";

    execute_all(
        &instance,
        &format!(
            r#"CREATE TABLE {table} (
                host STRING NOT NULL,
                ts TIMESTAMP TIME INDEX,
                val DOUBLE,
                PRIMARY KEY(host)
            )"#
        ),
        QueryContext::arc(),
    )
    .await;

    let schema = vec![
        ColumnSchema {
            column_name: "host".to_string(),
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
    ];
    let mut batch = Vec::with_capacity(INSERT_BATCH_ROWS);
    for host in 0..HOSTS {
        for point in 0..POINTS_PER_HOST {
            batch.push(Row {
                values: vec![
                    ValueData::StringValue(host_name(host)).into(),
                    ValueData::TimestampMillisecondValue(point * 1_000).into(),
                    ValueData::F64Value(value(host, point)).into(),
                ],
            });
            if batch.len() == INSERT_BATCH_ROWS {
                let rows = std::mem::replace(&mut batch, Vec::with_capacity(INSERT_BATCH_ROWS));
                instance
                    .handle_row_inserts(
                        insert_request(table, &schema, rows),
                        QueryContext::arc(),
                        false,
                        false,
                    )
                    .await
                    .unwrap();
            }
        }
    }
    if !batch.is_empty() {
        instance
            .handle_row_inserts(
                insert_request(table, &schema, batch),
                QueryContext::arc(),
                false,
                false,
            )
            .await
            .unwrap();
    }
    execute_all(
        &instance,
        &format!("ADMIN FLUSH_TABLE('{table}')"),
        QueryContext::arc(),
    )
    .await;

    let expected = (0..POINTS_PER_HOST)
        .flat_map(|point| {
            (0..HOSTS).map(move |host| (point * 1_000, host_name(host), -value(host, point)))
        })
        .collect::<Vec<_>>();

    let last = POINTS_PER_HOST - 1;
    let selector = format!(r#"{table}{{host!=""}}"#);
    let sql = format!("TQL EVAL (0, {last}, '1s') {selector} - ({selector} * 2)");
    observer.clear();
    let native_rows = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
    let plan = observer.take();
    assert_eq!(plan.name(), "FusedSeriesExec");
    assert_eq!(
        plan.children().len(),
        1,
        "the two occurrences must read one physical child"
    );
    assert_four_native_partitions(&plan);
    let display = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert_eq!(
        display.matches("source=0").count(),
        2,
        "both tree inputs must route to the one child:\n{display}"
    );
    assert!(
        !display.contains("source=1"),
        "no tree input may route to a child that does not exist:\n{display}"
    );
    assert_eq!(native_rows, expected);
    let legacy_rows = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
    assert_eq!(native_rows, legacy_rows);

    // Direct x+x exercises the other repeated-selector form without relying on subtraction's
    // order-sensitive arithmetic to reveal a swapped input.
    let direct_sql = format!("TQL EVAL (0, {last}, '1s') {selector} + {selector}");
    observer.clear();
    let direct_rows =
        native_result_rows(native_tql_output(&instance, &direct_sql, true).await).await;
    let direct_plan = observer.take();
    assert_eq!(direct_plan.name(), "FusedSeriesExec");
    assert_eq!(direct_plan.children().len(), 1);
    assert_four_native_partitions(&direct_plan);
    let direct_display = datafusion::physical_plan::displayable(direct_plan.as_ref())
        .indent(false)
        .to_string();
    assert_eq!(
        direct_display.matches("source=0").count(),
        2,
        "{direct_display}"
    );
    assert!(!direct_display.contains("source=1"), "{direct_display}");
    let direct_expected = (0..POINTS_PER_HOST)
        .flat_map(|point| {
            (0..HOSTS).map(move |host| (point * 1_000, host_name(host), value(host, point) * 2.0))
        })
        .collect::<Vec<_>>();
    assert_eq!(direct_rows, direct_expected);

    // A single selector with an equality matcher and an explicit range anchor/offset must push
    // down both identity and time bounds. For h000, values equal the sample index, so rate is 1/s.
    let bounded_sql =
        format!("TQL EVAL (4, 4, '1s') rate({table}{{host=\"h000\"}}[3s] @ 3 offset 1s)");
    observer.clear();
    let bounded_rows =
        native_result_rows(native_tql_output(&instance, &bounded_sql, true).await).await;
    let bounded_plan = observer.take();
    assert_eq!(bounded_plan.name(), "FusedSeriesExec");
    assert_eq!(bounded_rows, vec![(4_000, "h000".to_string(), 1.0)]);
    let scan_frame_rows = bounded_plan.children()[0]
        .metrics()
        .expect("series scan metrics")
        .output_rows()
        .expect("series scan reports output rows");
    assert_eq!(
        scan_frame_rows, 1,
        "the anchored range returns one packed raw-series frame"
    );
    let root_values = bounded_plan
        .metrics()
        .expect("fused metrics")
        .iter()
        .filter_map(|metric| match metric.value() {
            datafusion::physical_plan::metrics::MetricValue::Count { name, count }
                if name.as_ref() == "native.root.output_values" =>
            {
                Some(count.value())
            }
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(root_values, 1, "the bounded native result was executed");
    let root_input_samples = bounded_plan
        .metrics()
        .expect("fused metrics")
        .iter()
        .filter_map(|metric| match metric.value() {
            datafusion::physical_plan::metrics::MetricValue::Count { name, count }
                if name.as_ref() == "native.root.input_samples" =>
            {
                Some(count.value())
            }
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(
        root_input_samples, 3,
        "matcher and anchored range must scan only the three h000 samples"
    );

    // Different bounds/offsets must not be fused together: h000 at t=1 is 1, while the
    // one-second-offset occurrence reads t=0, so the exact answer is 1 - (0 * 2) = 1.
    let different_bounds = format!(
        "TQL EVAL (1, 1, '1s') {table}{{host=\"h000\"}} - ({table}{{host=\"h000\"}} offset 1s * 2)"
    );
    observer.clear();
    let different_bounds_rows =
        native_result_rows(native_tql_output(&instance, &different_bounds, true).await).await;
    assert!(observer.fused().is_none(), "{different_bounds}");
    assert_eq!(
        different_bounds_rows,
        vec![(1_000, "h000".to_string(), 1.0)]
    );

    // on() drops host from the output labels; inspect that shape directly instead of using the
    // host-requiring native_result_rows helper.
    let on_empty_sql = format!(
        r#"TQL EVAL (1, 1, '1s') {table}{{host="h001"}} - on() ({table}{{host="h001"}} * 2)"#
    );
    async fn unlabeled_rows(output: Output) -> Vec<(i64, f64, bool)> {
        let batches = match output.data {
            OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
            OutputData::RecordBatches(batches) => batches,
            _ => unreachable!("TQL EVAL returns record batches"),
        };
        batches
            .iter()
            .flat_map(|batch| {
                assert!(batch.column_by_name("host").is_none());
                let timestamps = batch
                    .column_by_name("ts")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let values = batch
                    .columns()
                    .iter()
                    .filter(|column| column.data_type() == &DataType::Float64)
                    .collect::<Vec<_>>();
                assert_eq!(values.len(), 1);
                let values = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
                (0..batch.num_rows())
                    .map(|row| {
                        (
                            timestamps.value(row),
                            values.value(row),
                            batch.num_columns() == 2,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }
    observer.clear();
    let on_empty_rows =
        unlabeled_rows(native_tql_output(&instance, &on_empty_sql, true).await).await;
    assert!(observer.fused().is_none(), "{on_empty_sql}");
    assert_eq!(on_empty_rows, vec![(1_000, -1_001.0, true)]);
    let on_empty_legacy =
        unlabeled_rows(native_tql_output(&instance, &on_empty_sql, false).await).await;
    assert_eq!(on_empty_rows, on_empty_legacy);

    // A group match drops the full series identity; different matchers, group matching, and a
    // subquery all stay on the ordinary planner path.
    let fallback_expected = expected[..HOSTS].to_vec();
    for (sql, expected_rows) in [
        (
            format!(r#"TQL EVAL (0, 0, '1s') {table}{{host!=""}} - ({table}{{host=~"h.+"}} * 2)"#),
            fallback_expected.clone(),
        ),
        (
            format!(
                r#"TQL EVAL (0, 0, '1s') {table}{{host!=""}} - on(host) group_left() ({table}{{host!=""}} * 2)"#
            ),
            fallback_expected.clone(),
        ),
        (
            format!(r#"TQL EVAL (1, 1, '1s') rate({table}{{host="h000"}}[2s:1s])"#),
            vec![(1_000, "h000".to_string(), 1.0)],
        ),
    ] {
        observer.clear();
        let rows = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
        assert!(
            observer.fused().is_none(),
            "a fallback shape must not plan a fused node: {sql}"
        );
        assert_eq!(rows, expected_rows, "{sql}");
        let legacy_rows = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(rows, legacy_rows, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_rate_and_abs_rate_match_legacy_across_storage_encodings() {
    common_telemetry::init_default_ut_logging();
    let mito_config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_rate_storage_encodings")
            .with_mito_config(mito_config),
    )
    .await;
    let instance = standalone.fe_instance().clone();

    for (table, ddl, flush_table, encoding, sparse) in [
        (
            "native_rate_dense",
            "CREATE TABLE native_rate_dense (host STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host)) ENGINE=mito WITH ('primary_key_encoding' = 'dense')",
            "native_rate_dense",
            PrimaryKeyEncoding::Dense,
            false,
        ),
        (
            "native_rate_sparse",
            "CREATE TABLE native_rate_sparse_phy (ts TIMESTAMP(3) TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table' = 'true', 'primary_key_encoding' = 'sparse'); CREATE TABLE native_rate_sparse (host STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host)) ENGINE=metric WITH ('on_physical_table' = 'native_rate_sparse_phy')",
            "native_rate_sparse_phy",
            PrimaryKeyEncoding::Sparse,
            true,
        ),
    ] {
        execute_all(&instance, ddl, QueryContext::arc()).await;
        for (start, end) in [(0, 60), (120, 300)] {
            let values = (b'a'..=b'p')
                .flat_map(|host| {
                    (start..=end).step_by(60).map(move |seconds| {
                        format!(
                            "('{}', {}, {})",
                            host as char,
                            seconds * 1000,
                            seconds as f64 * (host - b'a' + 1) as f64
                        )
                    })
                })
                .collect::<Vec<_>>()
                .join(", ");
            execute_all(
                &instance,
                &format!("INSERT INTO {table} (host, ts, val) VALUES {values}"),
                QueryContext::arc(),
            )
            .await;
            if start == 0 {
                execute_all(
                    &instance,
                    &format!("ADMIN FLUSH_TABLE('{flush_table}')"),
                    QueryContext::arc(),
                )
                .await;
            }
        }

        let expected = (b'a'..=b'p')
            .map(|host| {
                (
                    300_000,
                    (host as char).to_string(),
                    (host - b'a' + 1) as f64,
                )
            })
            .collect::<Vec<_>>();
        for expression in [
            format!("rate({table}[5m])"),
            format!("abs(rate({table}[5m]))"),
        ] {
            let sql = format!("TQL EVAL (300, 300, '1m') {expression}");
            let legacy_rows =
                native_result_rows(native_tql_output(&instance, &sql, false).await).await;
            assert_eq!(
                legacy_rows, expected,
                "native-off `{expression}` on {table}"
            );

            observer.clear();
            let native_rows =
                native_result_rows(native_tql_output(&instance, &sql, true).await).await;
            assert_eq!(native_rows, expected, "native-on `{expression}` on {table}");
            assert_eq!(native_rows, legacy_rows, "`{expression}` on {table}");
            let plan = observer.take();
            assert_eq!(plan.name(), "FusedSeriesExec");
            assert_eq!(
                plan.children()[0].output_partitioning().partition_count(),
                plan.output_partitioning().partition_count(),
                "fused child and output partitions agree for {table}"
            );
            assert!(
                plan.output_partitioning().partition_count() > 1,
                "fused execution must retain partitioning for {table}"
            );
            if sparse {
                assert_four_native_partitions(&plan);
            }
            let child = plan.children()[0];
            let child = child
                .downcast_ref::<CooperativeExec>()
                .map_or(child, |cooperative| cooperative.input());
            let series_scan = child
                .downcast_ref::<SeriesScanExec>()
                .expect("fused node reads a prepared series scan");
            let RawLabelSource::PrimaryKey {
                physical_metadata, ..
            } = series_scan.label_source()
            else {
                panic!("fused scan must materialize labels from the primary key");
            };
            assert_eq!(physical_metadata.primary_key_encoding, encoding);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_explain_and_analyze_execute_fused_plan() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_explain_analyze_e2e",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(&instance, "CREATE TABLE native_explain (ts TIMESTAMP TIME INDEX, host STRING, val DOUBLE, PRIMARY KEY(host)); INSERT INTO native_explain VALUES (0, 'a', 2)", QueryContext::arc()).await;
    for (statement, enabled) in [
        ("TQL EXPLAIN (0, 0, '1s') native_explain", true),
        ("TQL EXPLAIN VERBOSE (0, 0, '1s') native_explain", true),
        ("TQL ANALYZE (0, 0, '1s') native_explain", true),
        ("TQL ANALYZE VERBOSE (0, 0, '1s') native_explain", true),
        ("TQL EXPLAIN (0, 0, '1s') native_explain", false),
    ] {
        observer.clear();
        let output = native_tql_output(&instance, statement, enabled).await;
        let text = native_output_text(output).await;
        if statement.contains("ANALYZE") && enabled {
            assert!(text.contains("FusedSeriesExec"));
            assert!(text.contains("output_values"));
            let plan = observer.take();
            let output_values = plan
                .metrics()
                .expect("fused plan metrics")
                .iter()
                .filter_map(|metric| match metric.value() {
                    datafusion::physical_plan::metrics::MetricValue::Count { name, count }
                        if name.as_ref() == "native.root.output_values" =>
                    {
                        Some(count.value())
                    }
                    _ => None,
                })
                .sum::<usize>();
            assert_eq!(output_values, 1, "ANALYZE executes the fused series");
        } else if statement.contains("EXPLAIN") && enabled {
            assert!(text.contains("FusedSeries: native_explain"));
            assert!(text.contains("MergeScanExec"));
        } else if !enabled {
            assert!(!text.contains("FusedSeries:"));
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_view_fallback_preserves_native_hint() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_view_fallback_e2e",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(&instance, "CREATE TABLE native_view_source (ts TIMESTAMP TIME INDEX, host STRING, val DOUBLE, PRIMARY KEY(host)); INSERT INTO native_view_source VALUES (0, 'a', 2)", QueryContext::arc()).await;
    let ctx = native_query_context(true, 4);
    let expected_extensions = ctx.extensions();
    // Build the TQL view programmatically; the SQL syntax and SHOW CREATE round trip belong to the
    // independent view-persistence PR.
    let create_view = SqlStatement::CreateView(CreateView {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("native_view"))]),
        columns: vec![],
        query: Box::new(SqlStatement::Tql(Tql::Eval(TqlEval {
            start: "0".to_string(),
            end: "0".to_string(),
            step: "1s".to_string(),
            lookback: Some("5m".to_string()),
            query: "native_view_source".to_string(),
            alias: None,
        }))),
        or_replace: false,
        if_not_exists: false,
    });
    instance
        .statement_executor()
        .execute_stmt(QueryStatement::Sql(create_view), ctx.clone())
        .await
        .unwrap();
    // The view's stored plan must not depend on the caller's native hint, so reading it runs the
    // plan the view was created with rather than one that needs the hint to be readable.
    observer.clear();
    let view = instance
        .do_query("SELECT * FROM native_view", ctx.clone())
        .await
        .remove(0)
        .unwrap();
    let view_rows = native_result_rows(view).await;
    assert_eq!(ctx.extensions(), expected_extensions);
    assert_eq!(view_rows, vec![(0, "a".to_string(), 2.0)]);
    assert!(
        observer.fused().is_none(),
        "a view plan must be readable without the native hint"
    );
    // The hint is still the caller's: the same context keeps folding a direct query.
    observer.clear();
    let direct = instance
        .do_query("TQL EVAL (0, 0, '1s') native_view_source", ctx)
        .await
        .remove(0)
        .unwrap();
    let direct_rows = native_result_rows(direct).await;
    assert_eq!(direct_rows, vec![(0, "a".to_string(), 2.0)]);
    assert_eq!(observer.take().name(), "FusedSeriesExec");
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_metric_logical_tables_are_isolated() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_metric_isolation_e2e",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_metric_phy (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE
        ) ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE native_metric_a (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_metric_phy');
        CREATE TABLE native_metric_b (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_metric_phy');
        INSERT INTO native_metric_a (host, ts, val) VALUES ('only_a', 0, 101), ('only_a', 1000, 102);
        INSERT INTO native_metric_b (host, ts, val) VALUES ('only_b', 0, 202), ('only_b', 1000, 204);"#,
        QueryContext::arc(),
    )
    .await;
    for (table, host, value) in [
        ("native_metric_a", "only_a", 1.0),
        ("native_metric_b", "only_b", 2.0),
    ] {
        let sql = format!("TQL EVAL (1, 1, '1s') rate({table}[2s])");
        observer.clear();
        let native = native_tql_output(&instance, &sql, true).await;
        let native_rows = native_result_rows(native).await;
        assert_eq!(observer.take().name(), "FusedSeriesExec");
        let legacy_rows = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(native_rows, vec![(1000, host.to_string(), value)]);
        assert_eq!(native_rows, legacy_rows);
    }
}

/// OTLP delta temporality on the public TQL route: a `delta`-marked series answers the raw sample
/// sum (10 + 10 = 20) while a cumulative series with the same samples answers 0. The fused node
/// computes one cumulative window for every series, so it must decline a marker-bearing table
/// instead of reporting 0 for the delta series.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_delta_temporality_rate_and_increase_match_legacy() {
    common_telemetry::init_default_ut_logging();
    const TABLE: &str = "native_delta_temporality";
    const CONTROL: &str = "native_delta_control";
    // Samples at 60s and 120s, evaluated at 120s over a 2m window: the `delta` series' samples are
    // raw deltas, so its increase is their sum 10 + 10; the `cumulative` one did not rise, so 0.
    const DELTA_INCREASE: f64 = 20.0;
    const CUMULATIVE_INCREASE: f64 = 0.0;
    const DELTA_RATE: f64 = 20.0 / 120.0;
    const CUMULATIVE_RATE: f64 = 0.0;

    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_delta_temporality",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        &format!(
            r#"CREATE TABLE {TABLE} (
                host STRING,
                ts TIMESTAMP(3) TIME INDEX,
                val DOUBLE,
                otlp_aggregation_temporality STRING,
                PRIMARY KEY (host, otlp_aggregation_temporality)
            ) ENGINE=mito;
            INSERT INTO {TABLE} (host, ts, val, otlp_aggregation_temporality) VALUES
                ('delta', 60000, 10, '{GREPTIME_TEMPORALITY_DELTA}'),
                ('delta', 120000, 10, '{GREPTIME_TEMPORALITY_DELTA}'),
                ('cumulative', 60000, 10, 'cumulative'),
                ('cumulative', 120000, 10, 'cumulative');
            CREATE TABLE {CONTROL} (host STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host)) ENGINE=mito;
            INSERT INTO {CONTROL} (host, ts, val) VALUES ('a', 60000, 10), ('a', 120000, 30);"#
        ),
        QueryContext::arc(),
    )
    .await;

    for (query, delta, cumulative) in [
        (
            format!("increase({TABLE}[2m])"),
            DELTA_INCREASE,
            CUMULATIVE_INCREASE,
        ),
        (format!("rate({TABLE}[2m])"), DELTA_RATE, CUMULATIVE_RATE),
        (
            format!("abs(increase({TABLE}[2m]))"),
            DELTA_INCREASE,
            CUMULATIVE_INCREASE,
        ),
        (
            format!("abs(rate({TABLE}[2m]))"),
            DELTA_RATE,
            CUMULATIVE_RATE,
        ),
    ] {
        let sql = format!("TQL EVAL (120, 120, '1m') {query}");
        // Both series are in the window, in label order, with their own marker and value.
        let expected = [
            (120_000, "cumulative", "cumulative", cumulative),
            (120_000, "delta", GREPTIME_TEMPORALITY_DELTA, delta),
        ];
        // Values before routes: a wrong number is the regression, a declined plan is not.
        let legacy_rows = native_marker_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_delta_rows(&legacy_rows, &expected, &format!("native-off `{query}`"));
        observer.clear();
        let native_rows = native_marker_rows(native_tql_output(&instance, &sql, true).await).await;
        assert_delta_rows(&native_rows, &expected, &format!("native-on `{query}`"));
        assert_eq!(
            native_rows, legacy_rows,
            "`{query}`: same rows on both routes"
        );
        assert!(
            observer.fused().is_none(),
            "`{query}`: the native route must decline a table that carries the temporality marker"
        );
    }

    // The control: a marker-free table (samples 10 and 30 here, unlike the table above) still fuses
    // and agrees with the existing plan, so the decline is about the marker, not the table shape.
    let control_sql = format!("TQL EVAL (120, 120, '1m') sum_over_time({CONTROL}[2m])");
    observer.clear();
    let control_native =
        native_result_rows(native_tql_output(&instance, &control_sql, true).await).await;
    assert_eq!(
        control_native,
        vec![(120_000, "a".to_string(), 40.0)],
        "the fused control reports the sampled sum"
    );
    assert_eq!(observer.take().name(), "FusedSeriesExec");
    let control_legacy =
        native_result_rows(native_tql_output(&instance, &control_sql, false).await).await;
    assert_eq!(
        control_native, control_legacy,
        "the control agrees with the existing plan"
    );
}

/// Enabling query fallback — by the engine option or by the per-request `query_fallback` hint — must
/// keep an otherwise fusible query on the existing plan without changing its answer.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_fallback_options_keep_expected_answer() {
    common_telemetry::init_default_ut_logging();
    const TABLE: &str = "native_fallback_control";
    // One eligible selector, so only the fallback settings decide the route. `sum_over_time` is
    // exact over the two samples in the window: 10 + 30 for `a`, 10 + 70 for `b`.
    let query = format!("sum_over_time({TABLE}[2m])");
    let expected = vec![
        (120_000, "a".to_string(), 40.0),
        (120_000, "b".to_string(), 80.0),
    ];
    for (tag, allow_query_fallback) in [("engine_off", false), ("engine_on", true)] {
        let (standalone, observer) = native_test_instance_with_options(
            GreptimeDbStandaloneBuilder::new(&format!("native_promql_fallback_{tag}")),
            QueryOptions {
                experimental_native_promql: true,
                allow_query_fallback,
                ..Default::default()
            },
        )
        .await;
        let instance = standalone.fe_instance().clone();
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {TABLE} (host STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host)) ENGINE=mito; \
                 INSERT INTO {TABLE} (host, ts, val) VALUES ('a', 60000, 10), ('a', 120000, 30), \
                 ('b', 60000, 10), ('b', 120000, 70);"
            ),
            QueryContext::arc(),
        )
        .await;
        let mut hinted = native_query_context(true, 4).as_ref().clone();
        hinted.set_extension(QUERY_FALLBACK_HINT, "true");
        for (route, ctx) in [
            ("plain", native_query_context(true, 4)),
            ("fallback-hint", Arc::new(hinted)),
        ] {
            let sql = format!("TQL EVAL (120, 120, '1m') {query}");
            observer.clear();
            let rows =
                native_result_rows(instance.do_query(&sql, ctx).await.remove(0).unwrap()).await;
            assert_eq!(
                rows, expected,
                "{tag} {route}: the answer must not depend on the fallback settings"
            );
            if allow_query_fallback || route == "fallback-hint" {
                // A fallback rewrites every table scan into `MergeScan`, which the fused node
                // cannot read, so a query allowed to fall back keeps the existing plan.
                assert!(
                    observer.fused().is_none(),
                    "{tag} {route}: enabling the fallback must keep the existing plan"
                );
            } else {
                assert_eq!(
                    observer.take().name(),
                    "FusedSeriesExec",
                    "{tag} {route}: with no fallback enabled this query must be fused"
                );
            }
        }
    }
}

/// A store configured for the columnar series format must not change the answer of a query over a
/// field that format cannot serve: the columnar samples carry `Float64` series only, so an integer
/// field has no raw frame to fold and the route has to decline it while the existing plan answers.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_columnar_config_falls_back_for_integer_field() {
    common_telemetry::init_default_ut_logging();
    const TABLE: &str = "native_columnar_int";
    // Both functions are exact over the two samples inside the window: 10 + 30 and their mean.
    const SUM: f64 = 40.0;
    const AVG: f64 = 20.0;
    let mito_config = mito2::config::MitoConfig {
        experimental_enable_series_index: true,
        experimental_series_index_format: mito2::config::SeriesIndexFormat::Columnar,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_columnar_fallback")
            .with_mito_config(mito_config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE {TABLE} (host STRING, ts TIMESTAMP(3) TIME INDEX, val BIGINT, PRIMARY KEY (host)) ENGINE=mito; \
             INSERT INTO {TABLE} (host, ts, val) VALUES ('a', 60000, 10), ('a', 120000, 30);"
        ),
        QueryContext::arc(),
    )
    .await;
    for (query, expected) in [
        (format!("sum_over_time({TABLE}[2m])"), SUM),
        (format!("avg_over_time({TABLE}[2m])"), AVG),
    ] {
        let sql = format!("TQL EVAL (120, 120, '1m') {query}");
        observer.clear();
        let native_rows = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
        assert_eq!(
            native_rows.len(),
            1,
            "native-on `{query}`: one row for the one series"
        );
        assert_eq!(native_rows[0].0, 120_000, "native-on `{query}`: ts");
        assert_eq!(native_rows[0].1, "a", "native-on `{query}`: label");
        assert!(
            (native_rows[0].2 - expected).abs() <= 1e-9 * expected.abs().max(1.0),
            "native-on `{query}`: expected {expected}, found {}",
            native_rows[0].2
        );
        let legacy_rows = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(
            native_rows, legacy_rows,
            "`{query}`: same rows on both routes"
        );
        assert!(
            observer.fused().is_none(),
            "`{query}`: the native route must decline a field the columnar samples cannot serve"
        );
    }
}

/// The positive counterpart of the fallback above: a sparse physical metric table whose series
/// index is published in the columnar format fuses a `Float64` selector and matches the old plan.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_columnar_fused_series_matches_legacy() {
    common_telemetry::init_default_ut_logging();
    const PHYSICAL: &str = "native_columnar_fused_phy";
    const TABLE: &str = "native_columnar_fused";
    let mito_config = mito2::config::MitoConfig {
        experimental_enable_series_index: true,
        experimental_series_index_format: mito2::config::SeriesIndexFormat::Columnar,
        // Slow the periodic pass, so the manual build below is what publishes the catalog.
        experimental_series_index_maintenance_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let builder = GreptimeDbStandaloneBuilder::new("native_promql_columnar_fused")
        .with_mito_config(mito_config);
    let (standalone, observer) = native_test_instance(builder).await;
    let instance = standalone.fe_instance().clone();
    // One flushed SST is enough for a manual build, which skips the periodic four-file trigger.
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE {PHYSICAL} (ts TIMESTAMP(3) TIME INDEX, val DOUBLE) ENGINE=metric \
             WITH ('physical_metric_table' = 'true', 'primary_key_encoding' = 'sparse'); \
             CREATE TABLE {TABLE} (host STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host)) \
             ENGINE=metric WITH ('on_physical_table' = '{PHYSICAL}'); \
             INSERT INTO {TABLE} (host, ts, val) VALUES ('a', 60000, 10), ('a', 120000, 30), \
             ('b', 60000, 20), ('b', 120000, 60); \
             ADMIN FLUSH_TABLE('{PHYSICAL}'); \
             ADMIN BUILD_SERIES_INDEX('{PHYSICAL}');"
        ),
        QueryContext::arc(),
    )
    .await;
    // Both samples of each series fall in the 2m window: 10 + 30 and 20 + 60.
    let expected = vec![
        (120_000, "a".to_string(), 40.0),
        (120_000, "b".to_string(), 80.0),
    ];
    let sql = format!("TQL EVAL (120, 120, '1m') sum_over_time({TABLE}[2m])");
    observer.clear();
    let native_rows = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
    // The answer is drained first, so the counters below describe execution, not planning.
    let plan = observer.take();
    assert_eq!(native_rows, expected, "the fused route reports the sums");
    // `EnsureCooperative` wraps a leaf that does not ask for cooperative scheduling, and the fused
    // node reads the same series scan through it.
    let child = plan.children()[0];
    let child = child
        .downcast_ref::<CooperativeExec>()
        .map_or(child, |cooperative| cooperative.input());
    let series_scan = child
        .downcast_ref::<SeriesScanExec>()
        .expect("the fused node reads a prepared series scan");
    let RawLabelSource::PrimaryKey {
        metric_name,
        physical_metadata,
        logical_tags,
    } = series_scan.label_source()
    else {
        panic!("the fused scan must materialize labels from the primary key");
    };
    assert_eq!(metric_name.as_ref(), TABLE);
    assert_eq!(
        logical_tags
            .iter()
            .map(|tag| tag.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["host"],
        "the logical tags are the primary-key columns of the scanned region"
    );
    for tag in logical_tags.iter() {
        let column = physical_metadata.column_by_id(tag.column_id).unwrap();
        assert_eq!(column.column_schema.name, "host");
        assert_eq!(column.semantic_type, SemanticType::Tag);
    }
    assert_eq!(
        physical_metadata.primary_key_encoding,
        PrimaryKeyEncoding::Sparse
    );
    let series_schema = series_scan.series_schema();
    assert_eq!(series_schema.fields.len(), 1);
    assert_eq!(series_schema.fields[0].name.as_ref(), "val");
    // Only the columnar writer publishes these formats, so the scanned region's catalog rules out
    // a configuration-only run.
    let samples = std::path::Path::new(&standalone.opts.storage.data_home)
        .join("series_index")
        .join(physical_metadata.region_id.as_u64().to_string())
        .join("samples.json");
    let catalog: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(samples).unwrap()).unwrap();
    let entries = catalog["entries"].as_array().unwrap();
    let series = catalog["series"].as_array().unwrap();
    assert!(!entries.is_empty() && !series.is_empty(), "published");
    assert!(
        entries
            .iter()
            .all(|entry| entry["artifact"]["format"] == "columnar-samples")
    );
    assert!(
        series
            .iter()
            .all(|entry| entry["artifact"]["format"] == "columnar-series")
    );
    // One value per series: the node ran, rather than being merely planned.
    let output_values = plan
        .metrics()
        .expect("fused plan metrics")
        .iter()
        .filter_map(|metric| match metric.value() {
            datafusion::physical_plan::metrics::MetricValue::Count { name, count }
                if name.as_ref() == "native.root.output_values" =>
            {
                Some(count.value())
            }
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(output_values, 2, "both series must report one value");
    // Native off: the same rows must come from the existing plan.
    observer.clear();
    let legacy_rows = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
    assert_eq!(native_rows, legacy_rows, "same rows on both routes");
    assert!(observer.fused().is_none(), "the legacy route must not fuse");
}

/// No published series index: the columnar `Float64` selector must answer from the raw memtable
/// and from an unindexed SST instead of failing to plan.
#[rstest]
#[case::memtable("memtable", false)]
#[case::unindexed_sst("unindexed_sst", true)]
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_columnar_unindexed_float64_matches_legacy(
    #[case] stage: &str,
    #[case] flush: bool,
) {
    common_telemetry::init_default_ut_logging();
    const PHYSICAL: &str = "native_columnar_unindexed_phy";
    const TABLE: &str = "native_columnar_unindexed";
    let mito_config = mito2::config::MitoConfig {
        experimental_enable_series_index: true,
        experimental_series_index_format: mito2::config::SeriesIndexFormat::Columnar,
        // No periodic build, so neither case ever gets an index.
        experimental_series_index_maintenance_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new(&format!("native_promql_columnar_{stage}"))
            .with_mito_config(mito_config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE {PHYSICAL} (ts TIMESTAMP(3) TIME INDEX, val DOUBLE) ENGINE=metric \
             WITH ('physical_metric_table' = 'true', 'primary_key_encoding' = 'sparse'); \
             CREATE TABLE {TABLE} (host STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host)) \
             ENGINE=metric WITH ('on_physical_table' = '{PHYSICAL}'); \
             INSERT INTO {TABLE} (host, ts, val) VALUES ('a', 60000, 10), ('a', 120000, 30), \
             ('b', 60000, 20), ('b', 120000, 60);"
        ),
        QueryContext::arc(),
    )
    .await;
    if flush {
        execute_all(
            &instance,
            &format!("ADMIN FLUSH_TABLE('{PHYSICAL}');"),
            QueryContext::arc(),
        )
        .await;
    }
    // Both samples of each series fall in the 2m window: 10 + 30 and 20 + 60.
    let expected = vec![
        (120_000, "a".to_string(), 40.0),
        (120_000, "b".to_string(), 80.0),
    ];
    let sql = format!("TQL EVAL (120, 120, '1m') sum_over_time({TABLE}[2m])");
    // Legacy off first, so a red native query is about the fused route.
    observer.clear();
    let legacy_rows = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
    assert_eq!(legacy_rows, expected, "{stage}: existing plan must answer");
    assert!(observer.fused().is_none(), "{stage}: legacy must not fuse");
    observer.clear();
    let native_rows = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
    // Drained first, so the counter below describes execution, not planning; `take` requires the
    // fused node to have run.
    let plan = observer.take();
    assert_eq!(native_rows, expected, "{stage}: fused route must answer");
    let output_values = plan
        .metrics()
        .expect("fused plan metrics")
        .iter()
        .filter_map(|metric| match metric.value() {
            datafusion::physical_plan::metrics::MetricValue::Count { name, count }
                if name.as_ref() == "native.root.output_values" =>
            {
                Some(count.value())
            }
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(output_values, 2, "{stage}: two values, one per series");
    assert_eq!(
        native_rows, legacy_rows,
        "{stage}: same rows on both routes"
    );
}

/// An index over the first SST only: the newer rows still in the memtable, then flushed into a
/// second unindexed SST, must answer the same sums.
#[rstest]
#[case::partial_memtable("partial_memtable", false)]
#[case::partial_sst("partial_sst", true)]
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_columnar_partial_index_float64_matches_legacy(
    #[case] stage: &str,
    #[case] flush: bool,
) {
    common_telemetry::init_default_ut_logging();
    const PHYSICAL: &str = "native_columnar_partial_phy";
    const TABLE: &str = "native_columnar_partial";
    let mito_config = mito2::config::MitoConfig {
        experimental_enable_series_index: true,
        experimental_series_index_format: mito2::config::SeriesIndexFormat::Columnar,
        // Slow the periodic pass, so only the manual build below publishes an index.
        experimental_series_index_maintenance_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new(&format!("native_promql_columnar_{stage}"))
            .with_mito_config(mito_config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    // The first samples are flushed and indexed before the second ones are inserted, so the index
    // covers one SST while the later rows start out unflushed.
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE {PHYSICAL} (ts TIMESTAMP(3) TIME INDEX, val DOUBLE) ENGINE=metric \
             WITH ('physical_metric_table' = 'true', 'primary_key_encoding' = 'sparse'); \
             CREATE TABLE {TABLE} (host STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host)) \
             ENGINE=metric WITH ('on_physical_table' = '{PHYSICAL}'); \
             INSERT INTO {TABLE} (host, ts, val) VALUES ('a', 60000, 10), ('b', 60000, 20); \
             ADMIN FLUSH_TABLE('{PHYSICAL}'); \
             ADMIN BUILD_SERIES_INDEX('{PHYSICAL}'); \
             INSERT INTO {TABLE} (host, ts, val) VALUES ('a', 120000, 30), ('b', 120000, 60);"
        ),
        QueryContext::arc(),
    )
    .await;
    if flush {
        // The new SST stays unindexed: the build above is not repeated.
        execute_all(
            &instance,
            &format!("ADMIN FLUSH_TABLE('{PHYSICAL}');"),
            QueryContext::arc(),
        )
        .await;
    }
    // Both samples of each series fall in the 2m window: 10 + 30 and 20 + 60.
    let expected = vec![
        (120_000, "a".to_string(), 40.0),
        (120_000, "b".to_string(), 80.0),
    ];
    let sql = format!("TQL EVAL (120, 120, '1m') sum_over_time({TABLE}[2m])");
    // Legacy off first, so a red native query is about the fused route.
    observer.clear();
    let legacy_rows = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
    assert_eq!(legacy_rows, expected, "{stage}: existing plan must answer");
    assert!(observer.fused().is_none(), "{stage}: legacy must not fuse");
    observer.clear();
    let native_rows = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
    // Drained first, so the counter below describes execution, not planning; `take` requires the
    // fused node to have run.
    let plan = observer.take();
    assert_eq!(native_rows, expected, "{stage}: fused route must answer");
    let output_values = plan
        .metrics()
        .expect("fused plan metrics")
        .iter()
        .filter_map(|metric| match metric.value() {
            datafusion::physical_plan::metrics::MetricValue::Count { name, count }
                if name.as_ref() == "native.root.output_values" =>
            {
                Some(count.value())
            }
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(output_values, 2, "{stage}: two values, one per series");
    assert_eq!(
        native_rows, legacy_rows,
        "{stage}: same rows on both routes"
    );
}
