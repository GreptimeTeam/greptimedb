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
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::prelude::{GREPTIME_TEMPORALITY_DELTA, OTLP_AGGREGATION_TEMPORALITY_LABEL};
use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use common_query::{Output, OutputData};
use common_recordbatch::util::collect_batches;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datatypes::arrow::array::{
    Array, Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use frontend::instance::Instance;
use query::datafusion::{QUERY_FALLBACK_HINT, QUERY_PARALLELISM_HINT};
use query::dist_plan::MergeScanExec;
use query::options::QueryOptions;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::physical_wrapper::{PhysicalPlanWrapper, PhysicalPlanWrapperRef};
use query::promql::planner::NATIVE_PROMQL_EXTENSION_KEY;
use rstest::rstest;
use rstest_reuse::apply;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{MetricDiscoveryScope, QueryContext};
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
use crate::cluster::GreptimeDbClusterBuilder;
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

/// The fused node of the last wrapped plan that held one, every region-side raw source, and the
/// root of the last wrapped plan.
#[derive(Default)]
struct NativePlanObserver(
    Mutex<Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>>,
    Mutex<Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>>,
    Mutex<Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>>,
    /// Every plan a query engine handed to the wrapper, in the order it was wrapped. A statement
    /// that pushes work into a region wraps that region's own root too, and its sub-plans are not
    /// reachable from the frontend tree.
    Mutex<Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>>,
);

impl NativePlanObserver {
    fn clear(&self) {
        *self.0.lock().unwrap() = None;
        self.1.lock().unwrap().clear();
        *self.2.lock().unwrap() = None;
        self.3.lock().unwrap().clear();
    }

    /// Every wrapped plan except the frontend's own root, which is where a region's plan lands.
    fn region_plans(&self) -> Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let root = self.2.lock().unwrap().clone();
        self.3
            .lock()
            .unwrap()
            .iter()
            .filter(|plan| root.as_ref().is_none_or(|root| !Arc::ptr_eq(root, plan)))
            .cloned()
            .collect()
    }

    /// Every plan handed to the wrapper, including the frontend and region-side roots.
    fn all_plans(&self) -> Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.3.lock().unwrap().clone()
    }

    /// The root of the first plan wrapped after the last `clear`, which is the frontend's own tree
    /// for the statement: the region-side sub-plans are wrapped later, while it executes.
    fn root_plan(&self) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
        self.2
            .lock()
            .unwrap()
            .clone()
            .expect("a wrapped plan was captured")
    }

    fn take_raw_sources(&self) -> Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        std::mem::take(&mut *self.1.lock().unwrap())
    }

    fn take(&self) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
        self.0
            .lock()
            .unwrap()
            .take()
            .expect("native fused plan was captured")
    }

    /// The unique fused node with two inputs, including region-local fused nodes behind MergeScanExec.
    fn two_input_fused(&self) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
        let matches = observed_physical_nodes(self)
            .into_iter()
            .filter(|plan| plan.name() == "FusedSeriesExec" && plan.children().len() == 2)
            .collect::<Vec<_>>();
        assert_eq!(
            matches.len(),
            1,
            "expected exactly one two-input fused node"
        );
        matches.into_iter().next().unwrap()
    }

    /// The fused node the last wrapped plan contained, if any; `take` requires one instead.
    fn fused(&self) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.0.lock().unwrap().clone()
    }

    /// The name of the node that answers the statement, when it is one of the native nodes.
    ///
    /// [`Self::fused`] answers whether *some* fused node exists, and a split plan holds two: the
    /// partition-local producer below the state router and the merge above it. A test that means
    /// "the whole statement runs natively" therefore has to say which node answers it, not that one
    /// was captured. The search is breadth first, so the shallowest native node is the one reported.
    fn answering_node(&self) -> Option<String> {
        let mut queue = std::collections::VecDeque::from([self.root_plan()]);
        while let Some(node) = queue.pop_front() {
            if node.name() == "FusedSeriesExec" {
                return Some(node.name().to_string());
            }
            queue.extend(node.children().into_iter().cloned());
        }
        None
    }
}

/// Unique physical nodes reachable from the frontend and separately wrapped region trees.
/// MergeScanExec submits its region plan out-of-tree, so those trees are observed independently.
fn observed_physical_nodes(
    observer: &NativePlanObserver,
) -> Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    let mut roots = observer.all_plans();
    let mut nodes = Vec::new();
    while let Some(node) = roots.pop() {
        if nodes.iter().any(|seen| Arc::ptr_eq(seen, &node)) {
            continue;
        }
        roots.extend(node.children().into_iter().cloned());
        nodes.push(node);
    }
    nodes
}

/// The region reads a native node answers from, descending through cooperative wrappers and the
/// segments a statement cut out of itself.
fn native_region_inputs(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    out: &mut Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
) {
    for child in plan.children() {
        if child.is::<MergeScanExec>() {
            out.push(Arc::clone(child));
        } else {
            native_region_inputs(child, out);
        }
    }
}

fn observed_series_scans(observer: &NativePlanObserver) -> Vec<Arc<dyn ExecutionPlan>> {
    observed_physical_nodes(observer)
        .into_iter()
        .filter(|node| node.downcast_ref::<SeriesScanExec>().is_some())
        .collect()
}

impl PhysicalPlanWrapper for NativePlanObserver {
    fn wrap(
        &self,
        origin: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _ctx: session::context::QueryContextRef,
    ) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
        {
            let mut root = self.2.lock().unwrap();
            if root.is_none() {
                *root = Some(origin.clone());
            }
        }
        self.3.lock().unwrap().push(origin.clone());
        let mut stack = vec![origin.clone()];
        while let Some(node) = stack.pop() {
            if node.name() == "FusedSeriesExec" {
                *self.0.lock().unwrap() = Some(node);
                break;
            }
            stack.extend(node.children().into_iter().cloned());
        }
        let mut stack = vec![origin.clone()];
        while let Some(node) = stack.pop() {
            if node.name() == "RawSeriesSourceExec" {
                self.1.lock().unwrap().push(node.clone());
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

/// What a rows answer reports in the metric-name column the rows root declares after its labels.
#[derive(Clone, Copy)]
enum ReportedName<'a> {
    /// No name column at all: the existing route declares none.
    NoColumn,
    /// The column is declared and every value in it is null, because the statement names no series.
    Nothing,
    /// Every row reports this metric name.
    Metric(&'a str),
}

/// The batches of a `TQL EVAL` answer.
async fn rows_batches(output: Output) -> common_recordbatch::RecordBatches {
    match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    }
}

/// The position of the answer's metric-name column, refused unless it reports exactly `reported`.
///
/// A native rows root appends a nullable `__name__` column after the labels, and a row that names
/// no series leaves it null: a math kernel, a range function and a folding aggregate report a value
/// of their own and no name. The existing route declares no such column, so a test that compares
/// the two answers has to say what that one column of the native answer holds rather than leave
/// whichever column it did not expect out of the assertion.
fn checked_name_column(
    fields: &datatypes::arrow::datatypes::Fields,
    columns: &[datatypes::arrow::array::ArrayRef],
    reported: ReportedName<'_>,
) -> Option<usize> {
    let index = fields.iter().position(|field| field.name() == "__name__");
    let Some(index) = index else {
        assert!(
            matches!(reported, ReportedName::NoColumn),
            "the rows root declares the metric-name column after its labels: {:?}",
            fields.iter().map(|field| field.name()).collect::<Vec<_>>()
        );
        return None;
    };
    assert!(
        !matches!(reported, ReportedName::NoColumn),
        "the existing route declares no metric-name column, but this answer has one: {:?}",
        fields[index]
    );
    assert_eq!(
        index,
        fields.len() - 1,
        "the metric-name column is appended after the labels: {fields:?}"
    );
    assert!(
        datatypes::schema::is_metric_name_field(&fields[index]),
        "the appended column is the layout's metric-name column: {:?}",
        fields[index]
    );
    let names = cast(columns[index].as_ref(), &DataType::Utf8).unwrap();
    let names = names.as_any().downcast_ref::<StringArray>().unwrap();
    match reported {
        ReportedName::Nothing => assert_eq!(
            names.null_count(),
            names.len(),
            "a statement that names no series reports a null name on every row: {names:?}"
        ),
        ReportedName::Metric(name) => {
            assert_eq!(
                names.null_count(),
                0,
                "every row of this statement reports its metric name: {names:?}"
            );
            for row in 0..names.len() {
                assert_eq!(name, names.value(row), "row {row} of {names:?}");
            }
        }
        ReportedName::NoColumn => unreachable!("refused above"),
    }
    Some(index)
}

/// [`checked_name_column`] for every batch of an answer, which agree on the position.
fn checked_name_columns(
    batches: &common_recordbatch::RecordBatches,
    reported: ReportedName<'_>,
) -> Option<usize> {
    let mut index = None;
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let position = checked_name_column(batch.schema().fields(), batch.columns(), reported);
        match index {
            None => index = position,
            Some(index) => assert_eq!(Some(index), position, "every batch names its rows alike"),
        }
    }
    index
}

/// The answer rendered as the table it reports, with the metric-name column checked and left out.
///
/// Unlike [`eval_text`] this keeps the response's own column and row order, so it compares a
/// statement that orders its rows - `sort` - and one whose answer is a value of its own.
fn pretty_rows_without_name(
    batches: &common_recordbatch::RecordBatches,
    reported: ReportedName<'_>,
) -> String {
    use datatypes::arrow::util::display::FormatOptions;
    use datatypes::arrow::util::pretty::pretty_format_batches_with_options;

    let name_index = checked_name_columns(batches, reported);
    let batches = batches
        .iter()
        .map(|batch| {
            let batch = batch.df_record_batch();
            let Some(name_index) = name_index else {
                return batch.clone();
            };
            let schema = batch.schema();
            let fields = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(position, _)| *position != name_index)
                .map(|(_, field)| Arc::clone(field))
                .collect::<Vec<_>>();
            let columns = batch
                .columns()
                .iter()
                .enumerate()
                .filter(|(position, _)| *position != name_index)
                .map(|(_, column)| Arc::clone(column))
                .collect::<Vec<_>>();
            let schema = ArrowSchema::new_with_metadata(fields, schema.metadata().clone());
            datatypes::arrow::array::RecordBatch::try_new(Arc::new(schema), columns)
                .expect("the answer keeps its column types")
        })
        .collect::<Vec<_>>();
    pretty_format_batches_with_options(&batches, &FormatOptions::default())
        .expect("the answer formats")
        .to_string()
}

/// [`pretty_rows_without_name`] of a `TQL EVAL` answer.
async fn named_rows_text(output: Output, reported: ReportedName<'_>) -> String {
    let batches = rows_batches(output).await;
    pretty_rows_without_name(&batches, reported)
}

/// The rows a Prometheus answer carries for a statement whose every row reports `name`.
///
/// The existing route declares no name column, so the rows it answers are missing exactly the
/// `__name__` label that a Prometheus answer of the same statement reports. Adding the name to those
/// rows keeps the native answer whole in the comparison: a wrong or missing name fails it instead of
/// being dropped from the side that has one.
fn named_rows(
    rows: Vec<(i64, std::collections::BTreeMap<String, String>, f64)>,
    name: &str,
) -> Vec<(i64, std::collections::BTreeMap<String, String>, f64)> {
    rows.into_iter()
        .map(|(second, mut labels, value)| {
            assert!(
                labels
                    .insert("__name__".to_string(), name.to_string())
                    .is_none(),
                "the existing route already reported a name: {labels:?}"
            );
            (second, labels, value)
        })
        .collect()
}

/// The error a query reports, whether it was raised while planning or while its stream was read.
async fn collect_error(output: Output) -> String {
    match output.data {
        OutputData::Stream(stream) => match collect_batches(stream).await {
            Ok(_) => "the query answered rows where Prometheus reports an error".to_string(),
            Err(error) => error.to_string(),
        },
        OutputData::RecordBatches(_) => {
            "the query answered rows where Prometheus reports an error".to_string()
        }
        other => format!("the query answered {other:?}"),
    }
}

/// Like [`native_result_rows`], with both label columns of the discovery fixture.
async fn native_site_rows(output: Output) -> Vec<(i64, String, String, f64)> {
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
            let host = cast(
                batch.column_by_name("host").unwrap().as_ref(),
                &DataType::Utf8,
            )
            .unwrap();
            let host = host.as_any().downcast_ref::<StringArray>().unwrap();
            let site = cast(
                batch.column_by_name("site").unwrap().as_ref(),
                &DataType::Utf8,
            )
            .unwrap();
            let site = site.as_any().downcast_ref::<StringArray>().unwrap();
            let values = batch
                .columns()
                .iter()
                .filter(|column| column.data_type() == &DataType::Float64)
                .collect::<Vec<_>>();
            assert_eq!(values.len(), 1, "expected one Float64 value column");
            let values = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
            (0..batch.num_rows())
                .map(|row| {
                    (
                        timestamps.value(row),
                        host.value(row).to_string(),
                        site.value(row).to_string(),
                        values.value(row),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.partial_cmp(right).unwrap());
    rows
}

/// The rows of a TQL answer, labelled by `label`.
///
/// `None` labels each row with every label column it carries, by name, so an answer whose label set
/// is not the `host` one can be compared with the existing route's the same way.
async fn native_rows(output: Output, label: Option<&str>) -> Vec<(i64, String, f64)> {
    native_rows_and_names(output, label).await.0
}

/// [`native_rows`], plus the per-row values of the declared name column, and `None` when the answer
/// declares no such column at all. A row whose value is computed rather than read reports no name,
/// which is a null there and not a missing column: a reader that only looks at the labels cannot
/// tell the two apart, so a caller asserting "answers no name" has to read these.
async fn native_rows_and_names(
    output: Output,
    label: Option<&str>,
) -> (Vec<(i64, String, f64)>, Option<Vec<Option<String>>>) {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    let declares_name = batches
        .schema()
        .arrow_schema()
        .fields()
        .iter()
        .any(|field| datatypes::schema::is_metric_name_field(field));
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let timestamps = batch
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let value_columns = batch
                .columns()
                .iter()
                .enumerate()
                .filter(|(_, column)| column.data_type() == &DataType::Float64)
                .collect::<Vec<_>>();
            assert_eq!(value_columns.len(), 1, "expected one Float64 value column");
            let (value_index, value_column) = value_columns[0];
            let labels = match label {
                Some(label) => {
                    let labels = cast(
                        batch.column_by_name(label).unwrap().as_ref(),
                        &DataType::Utf8,
                    )
                    .unwrap();
                    let labels = labels.as_any().downcast_ref::<StringArray>().unwrap();
                    assert_eq!(labels.null_count(), 0);
                    (0..batch.num_rows())
                        .map(|row| labels.value(row).to_string())
                        .collect::<Vec<_>>()
                }
                // Every label column, by name: the value column and the time index are the two that
                // are not one, and a route names its value differently from the other.
                None => (0..batch.num_rows())
                    .map(|row| {
                        batch
                            .schema
                            .column_schemas()
                            .iter()
                            .enumerate()
                            .filter(|(index, column)| *index != value_index && column.name != "ts")
                            .filter_map(|(_, column)| {
                                let values = batch.column_by_name(&column.name)?.as_ref();
                                let values = cast(values, &DataType::Utf8).ok()?;
                                let values = values.as_any().downcast_ref::<StringArray>()?;
                                (!values.is_null(row))
                                    .then(|| format!("{}={}", column.name, values.value(row)))
                            })
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .collect::<Vec<_>>(),
            };
            let values = value_column
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(timestamps.null_count(), 0);
            assert_eq!(values.null_count(), 0);
            let names = batch.column_by_name("__name__").map(|column| {
                let names = column.as_any().downcast_ref::<StringArray>().unwrap();
                (0..batch.num_rows())
                    .map(|row| names.is_valid(row).then(|| names.value(row).to_string()))
                    .collect::<Vec<_>>()
            });
            (0..batch.num_rows())
                .map(|row| {
                    (
                        timestamps.value(row),
                        labels[row].clone(),
                        values.value(row),
                        names.as_ref().map(|names| names[row].clone()),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
    let names = declares_name.then(|| {
        rows.iter()
            .map(|(_, _, _, name)| name.clone().flatten())
            .collect::<Vec<Option<String>>>()
    });
    let rows = rows
        .into_iter()
        .map(|(time, labels, value, _)| (time, labels, value))
        .collect();
    (rows, names)
}

async fn native_result_rows(output: Output) -> Vec<(i64, String, f64)> {
    native_rows(output, Some("host")).await
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_clamp_aggregates_execute_on_native_segments() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_clamp_aggregates").with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE m (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); INSERT INTO m (host, ts, val) VALUES ('a', 0, 0.5), ('b', 0, 1.5), ('c', 0, 3.0)",
        QueryContext::arc(),
    )
    .await;
    for (query, expected, split) in [
        ("sum(clamp(m, 1, 2))", 4.5, true),
        ("sum(topk(1, clamp(m, 1, 2)))", 2.0, false),
    ] {
        let sql = format!("TQL EVAL (0, 0, '1s') {query}");
        observer.clear();
        let output = native_tql_output(&instance, &sql, true).await;
        let batches = match output.data {
            OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
            OutputData::RecordBatches(batches) => batches,
            _ => unreachable!("TQL EVAL returns record batches"),
        };
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        let batch = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
        assert!(batch.column_by_name("host").is_none());
        assert_eq!(
            batch
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            0
        );
        let values = batch
            .columns()
            .iter()
            .find_map(|column| column.as_any().downcast_ref::<Float64Array>())
            .unwrap();
        assert_eq!(values.value(0), expected);
        let root = observer.root_plan();
        let display = datafusion::physical_plan::displayable(root.as_ref())
            .indent(true)
            .to_string();
        if split {
            assert!(
                display.contains("Aggregate: sum by [] final"),
                "{query}: {display}"
            );
            let producer = observer.fused().expect("native partial producer");
            let producer_display = datafusion::physical_plan::displayable(producer.as_ref())
                .indent(true)
                .to_string();
            assert!(
                producer_display.contains("clamp"),
                "{query}: {producer_display}"
            );
        } else {
            assert!(display.contains("FusedSeriesExec"), "{query}: {display}");
            assert!(
                display.contains("Vector: topk 1 by []"),
                "{query}: {display}"
            );
            assert!(
                display.contains("Aggregate: sum by [] complete"),
                "{query}: {display}"
            );
        }
    }
}

/// A native rows result names each row after the metric it came from.
///
/// The name is part of what the statement answers, not of how a result is displayed: `sum by
/// (__name__)` groups by the identity of the series, and an `or` reports each side's own name. An
/// operator that computes a value of its own - a math kernel, a folding aggregate - drops the name
/// exactly as it drops a label, so its rows answer no `__name__` label at all.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_rows_name_each_row_after_its_metric() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_rows_name_each_row")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_name_phy (ts TIMESTAMP TIME INDEX, v DOUBLE) \
             ENGINE=metric WITH (\"physical_metric_table\" = \"\"); \
         CREATE TABLE native_name_demo (host STRING NULL, ts TIMESTAMP NOT NULL, v DOUBLE NULL, \
             TIME INDEX (ts), PRIMARY KEY (host)) \
             ENGINE=metric WITH (on_physical_table = 'native_name_phy'); \
         CREATE TABLE native_name_other (host STRING NULL, ts TIMESTAMP NOT NULL, v DOUBLE NULL, \
             TIME INDEX (ts), PRIMARY KEY (host)) \
             ENGINE=metric WITH (on_physical_table = 'native_name_phy'); \
         INSERT INTO native_name_demo (host, ts, v) VALUES ('a', 0, 2); \
         INSERT INTO native_name_other (host, ts, v) VALUES ('b', 0, 8)",
        QueryContext::arc(),
    )
    .await;

    // An `or` keeps both sides' names, and a named selector reports its own. A row's label set is
    // what Prometheus reports for it: `or` returns the left side's whole label set and appends the
    // right side's series, so each row keeps the labels its own selector read - `host` among them -
    // and reports the name of the metric it came from.
    // A selector that names no metric is answered from the metric tables discovery is authorized to
    // read, and a caller prepares that scope for the statement: the Prometheus handler passes the
    // schema's tables, and this fixture passes its two. Without a scope the planner declines the
    // selector, so the statement would keep the existing plan, which refuses it outright.
    let scoped = || {
        let mut ctx = QueryContext::with_db_name(None);
        ctx.set_extension(QUERY_PARALLELISM_HINT, "4".to_string());
        ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, "true".to_string());
        ctx.set_metric_discovery_scope(MetricDiscoveryScope::allowed([
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
                "native_name_demo".to_string(),
            ),
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
                "native_name_other".to_string(),
            ),
        ]));
        Arc::new(ctx)
    };
    for (query, expected, native) in [
        (
            "native_name_demo or native_name_other",
            vec![
                (0, "host=a,__name__=native_name_demo".to_string(), 2.0),
                (0, "host=b,__name__=native_name_other".to_string(), 8.0),
            ],
            true,
        ),
        // A bare selection has no operator to evaluate, so the scan itself answers it and there is
        // no native node to observe: its name is the one the statement derives, which #10162 covers.
        (
            "native_name_demo",
            vec![(0, "host=a,__name__=native_name_demo".to_string(), 2.0)],
            false,
        ),
        (
            "topk(1, native_name_other)",
            vec![(0, "host=b,__name__=native_name_other".to_string(), 8.0)],
            true,
        ),
        // A selector that names no metric reads every metric the discovery scope allows, and the
        // name is how a caller tells the rows apart: grouping by it keeps the two metrics in
        // groups of their own rather than folding their series together.
        (
            r#"sum by (__name__) ({host=~"a|b"})"#,
            vec![
                (0, "__name__=native_name_demo".to_string(), 2.0),
                (0, "__name__=native_name_other".to_string(), 8.0),
            ],
            true,
        ),
        (
            r#"{host=~"a|b"}"#,
            vec![
                (0, "host=a,__name__=native_name_demo".to_string(), 2.0),
                (0, "host=b,__name__=native_name_other".to_string(), 8.0),
            ],
            true,
        ),
    ] {
        observer.clear();
        let output = instance
            .do_query(&format!("TQL EVAL (0, 0, '1s') {query}"), scoped())
            .await
            .remove(0)
            .unwrap();
        let rows = native_rows(output, None).await;
        assert_eq!(expected, rows, "{query}");
        if native {
            assert_native_rows_plan(&observer);
        }
    }

    // A math kernel and a folding aggregate report a value of their own, so their rows carry no
    // name: the column is there and null, and the response reads no label from it.
    for (query, expected_values) in [
        ("abs(native_name_demo or native_name_other)", vec![2.0, 8.0]),
        ("sum(native_name_demo)", vec![2.0]),
    ] {
        observer.clear();
        let (rows, names) = native_rows_and_names(
            native_tql_output(&instance, &format!("TQL EVAL (0, 0, '1s') {query}"), true).await,
            None,
        )
        .await;
        // A computed value answers no name; the labels the input carried stay. The column is still
        // declared, and the names have to be read from it: a row that reports no name is null
        // there, which a reader that only collects the labels cannot tell from a missing column.
        let names =
            names.unwrap_or_else(|| panic!("{query}: the rows answer declares the name column"));
        assert!(
            names.iter().all(Option::is_none),
            "{query}: a computed value answers no name: {names:?}"
        );
        assert!(
            rows.iter().all(|(_, label, _)| !label.contains("__name__")),
            "{query}: a computed value answers no name: {rows:?}"
        );
        let mut values = rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>();
        values.sort_by(|left, right| left.partial_cmp(right).unwrap());
        assert_eq!(expected_values, values, "{query}");
        assert_native_rows_plan(&observer);
    }
}

/// `sum by (__name__)` over metric-bound inputs groups by the identity of the series.
///
/// `__name__` is not a column of any scan: it is the name every series of a metric-bound input
/// carries. A statement reading one input binds one name, which is what the appended name column
/// reports; one reading two binds two, and the grouping keeps the name as a group label, so the two
/// metrics stay in groups of their own instead of folding into one label set.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_group_by_name_over_several_metrics() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_group_by_name").with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_gbn_phy (ts TIMESTAMP TIME INDEX, v DOUBLE) \
             ENGINE=metric WITH (\"physical_metric_table\" = \"\"); \
         CREATE TABLE native_gbn_demo (host STRING NULL, ts TIMESTAMP NOT NULL, v DOUBLE NULL, \
             TIME INDEX (ts), PRIMARY KEY (host)) \
             ENGINE=metric WITH (on_physical_table = 'native_gbn_phy'); \
         CREATE TABLE native_gbn_other (host STRING NULL, ts TIMESTAMP NOT NULL, v DOUBLE NULL, \
             TIME INDEX (ts), PRIMARY KEY (host)) \
             ENGINE=metric WITH (on_physical_table = 'native_gbn_phy'); \
         INSERT INTO native_gbn_demo (host, ts, v) VALUES ('a', 0, 2), ('b', 0, 5); \
         INSERT INTO native_gbn_other (host, ts, v) VALUES ('c', 0, 8)",
        QueryContext::arc(),
    )
    .await;

    // One name per statement is the appended column: the existing route declares no such column,
    // so the comparison is the one every native rows test makes.
    for (query, reported) in [
        // One metric, grouped by its own name: the column reports what the aggregate grouped by.
        (
            "sum by (__name__) (native_gbn_demo)",
            ReportedName::Metric("native_gbn_demo"),
        ),
        // Two metric-bound inputs in one tree, each summed by its own name and then matched. The sum
        // computes a value of its own and drops the name it read, as Prometheus does, so the column
        // is there and null.
        (
            "sum by (__name__) (native_gbn_demo) + sum by (__name__) (native_gbn_other)",
            ReportedName::Nothing,
        ),
    ] {
        let sql = format!("TQL EVAL (0, 0, '1s') {query}");
        observer.clear();
        let native = eval_text_rows(native_tql_output(&instance, &sql, true).await, reported);
        assert_native_rows_plan(&observer);
        let legacy = eval_text(native_tql_output(&instance, &sql, false).await);
        assert_eq!(
            legacy, native,
            "{query}: the native answer is the existing one"
        );
    }

    // A cross-metric tree whose answer keeps the name reports the metric its row was read from, so
    // the grouping names the metric it grouped. `or` keeps the series whose label set the left side
    // does not hold, and a metric-bound input's name is not one of its tags: both sides are summed
    // to one series with no labels, so the left side's row is the answer - as it is on the existing
    // route, whose answer differs only in the name column it does not declare.
    let query = "sum by (__name__) (native_gbn_demo) or sum by (__name__) (native_gbn_other)";
    let sql = format!("TQL EVAL (0, 0, '1s') {query}");
    observer.clear();
    let (rows, names) =
        native_rows_and_names(native_tql_output(&instance, &sql, true).await, None).await;
    assert_native_rows_plan(&observer);
    assert_eq!(
        Some(vec![Some("native_gbn_demo".to_string())]),
        names,
        "{query}: the row reports the metric it was read from"
    );
    assert_eq!(
        vec![(0, "__name__=native_gbn_demo".to_string(), 7.0)],
        rows,
        "{query}: the left side's series is the answer"
    );
    let legacy_rows = native_rows(native_tql_output(&instance, &sql, false).await, None).await;
    assert_eq!(
        vec![7.0],
        legacy_rows
            .iter()
            .map(|(_, _, value)| *value)
            .collect::<Vec<_>>(),
        "{query}: the existing route answers the same series"
    );
}

/// The layout a native one-field answer publishes: the time index, one value, then the labels with
/// the metric name last.
fn assert_one_field_native_layout(batches: &common_recordbatch::RecordBatches, query: &str) {
    let declared = batches.schema();
    let schema = declared.arrow_schema();
    let fields = schema.fields();
    assert!(
        matches!(fields[0].data_type(), DataType::Timestamp(..)),
        "{query}: the time index comes first: {schema:?}"
    );
    assert_eq!(
        1,
        fields
            .iter()
            .filter(|field| field.data_type() == &DataType::Float64)
            .count(),
        "{query}: one value column: {schema:?}"
    );
    assert!(
        datatypes::schema::is_metric_name_field(fields.last().unwrap()),
        "{query}: the metric name comes after the labels: {schema:?}"
    );
}

/// A `__field__` selection that matches no metric table answers the empty vector.
///
/// The matcher decides which of a scan's field columns a selection keeps, and no table matched this
/// one, so the field names the empty input declares are exactly the ones the matcher states. The
/// statement answers the empty vector rather than the existing planner's missing-table error, under
/// the layout every one-field selection publishes.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_field_selector_over_no_table_answers_empty() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_field_selector_empty")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_fs_phy (ts TIMESTAMP TIME INDEX, v DOUBLE) \
             ENGINE=metric WITH (\"physical_metric_table\" = \"\"); \
         CREATE TABLE native_fs_demo (host STRING NULL, ts TIMESTAMP NOT NULL, v DOUBLE NULL, \
             TIME INDEX (ts), PRIMARY KEY (host)) \
             ENGINE=metric WITH (on_physical_table = 'native_fs_phy'); \
         CREATE TABLE native_fs_other (host STRING NULL, ts TIMESTAMP NOT NULL, v DOUBLE NULL, \
             TIME INDEX (ts), PRIMARY KEY (host)) \
             ENGINE=metric WITH (on_physical_table = 'native_fs_phy'); \
         INSERT INTO native_fs_demo (host, ts, v) VALUES ('a', 0, 2); \
         INSERT INTO native_fs_other (host, ts, v) VALUES ('b', 0, 8)",
        QueryContext::arc(),
    )
    .await;
    // A selector that names no metric is answered from the metric tables discovery is authorized to
    // read, which the caller states: this fixture passes its two, and the selector's own name
    // matcher is what leaves it with no table at all.
    let scoped = || {
        let mut ctx = QueryContext::with_db_name(None);
        ctx.set_extension(QUERY_PARALLELISM_HINT, "4".to_string());
        ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, "true".to_string());
        ctx.set_metric_discovery_scope(MetricDiscoveryScope::allowed([
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
                "native_fs_demo".to_string(),
            ),
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
                "native_fs_other".to_string(),
            ),
        ]));
        Arc::new(ctx)
    };
    for (query, answered) in [
        // The matcher names the field the selection reads, and no metric table matched it.
        (
            r#"TQL EVAL (0, 0, '1s') {__name__=~"no_such_metric_.+", __field__="v"}"#,
            false,
        ),
        // The same statement without the matcher: the field set is not what decides the answer, so
        // the two layouts have to agree.
        (
            r#"TQL EVAL (0, 0, '1s') {__name__=~"no_such_metric_.+"}"#,
            false,
        ),
        // A selection that does read a table, for the layout both empty answers have to match.
        (r#"TQL EVAL (0, 0, '1s') {host=~"a|b"}"#, true),
    ] {
        observer.clear();
        let output = instance.do_query(query, scoped()).await.remove(0).unwrap();
        let batches = rows_batches(output).await;
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        if answered {
            assert_eq!(2, rows, "{query}: the fixture holds two series");
        } else {
            assert_eq!(
                0, rows,
                "{query}: a selection that matches no metric answers the empty vector"
            );
            assert_native_rows_plan(&observer);
        }
        assert_one_field_native_layout(&batches, query);
    }
}

/// A canonical merge over a partitioned table runs as a boundary above the regions.
///
/// A rewrite that writes a label its input carries can give two stored series one label set, and
/// the partition column decides where those series are stored: the pieces of that one set can land
/// in different regions, which no region-local producer can see. The occurrence therefore reads
/// through its own raw source, its producer merges what each partition holds above the regions, and
/// the canonical router above the producer collects each label set from every contribution.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_canonical_merge_over_partitioned_table() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_canonical_partitioned")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_canon_part (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, \
         PRIMARY KEY(host)) PARTITION ON COLUMNS (host) (host < 'first', host >= 'first' AND \
         host < 'second', host >= 'second'); \
         INSERT INTO native_canon_part (host, val, ts) VALUES ('first', 10, 0), \
         ('second', 20, 600000)",
        QueryContext::arc(),
    )
    .await;
    // The two hosts land in different partitions, and the rewrite stores both under one `host`, so
    // the pieces of that label set are ten minutes apart and never collide.
    let sql = r#"TQL EVAL (0, 600, '600s') label_replace(native_canon_part, "host", "same", "host", ".*")"#;
    observer.clear();
    let native = eval_text_rows(
        native_tql_output(&instance, sql, true).await,
        ReportedName::Metric("native_canon_part"),
    );
    let router = canonical_router(&observer).unwrap_or_else(|| {
        panic!(
            "a collapsing rewrite over a partitioned table must run a canonical boundary: {native}"
        )
    });
    assert_eq!(
        "FusedSeriesExec",
        router.children()[0].name(),
        "the router reads the canonical producer: {native}"
    );
    let legacy = eval_text(native_tql_output(&instance, sql, false).await);
    assert_eq!(
        legacy, native,
        "the boundary collects every region's contribution into the existing route's rows"
    );
    for expected in ["10.0", "20.0", "same"] {
        assert!(
            native.contains(expected),
            "the label set carries every piece's value: {expected} missing from {native}"
        );
    }
}

/// A time index in seconds, microseconds or nanoseconds is read natively with the existing route's
/// semantics: windows and lookbacks are decided on the stored ticks, a kernel reads the time
/// truncated to milliseconds, and samples less than a millisecond apart stay distinct samples.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_non_millisecond_time_index_answers_like_legacy() {
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_non_millisecond_time_index",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_us (ts TIMESTAMP(6) TIME INDEX, series STRING PRIMARY KEY, val DOUBLE); \
         INSERT INTO native_us VALUES (0, 'window', 1), (1, 'window', 2), (2, 'window', 5), \
         (1000000, 'window', 3), (1000001, 'window', 4), (999500, 'past', 7), \
         (1000001, 'future', 9), (999999, 'counter', 1), (1000500, 'counter', 3), \
         (1999700, 'counter', 8), (-1500, 'negative', 6); \
         CREATE TABLE native_ns (ts TIMESTAMP(9) TIME INDEX, job STRING PRIMARY KEY, val DOUBLE); \
         INSERT INTO native_ns VALUES (999999900, 'sub_ms', 999), (999999100, 'sub_ms', 10), \
         (999999500, 'sub_ms', 13), (998000000, 'one_ms', 10), (999000000, 'one_ms', 13); \
         CREATE TABLE native_sec (ts TIMESTAMP(0) TIME INDEX, host STRING PRIMARY KEY, val DOUBLE); \
         INSERT INTO native_sec VALUES (0, 'a', 1), (1, 'a', 2), (2, 'a', 4), (3, 'a', 8)",
        QueryContext::arc(),
    )
    .await;
    let queries = [
        (
            "TQL EVAL (1, 2, '1s', '300s') native_us",
            ReportedName::Metric("native_us"),
        ),
        (
            "TQL EVAL (0, 0, '1s', '300s') native_us offset -1s",
            ReportedName::Metric("native_us"),
        ),
        (
            "TQL EVAL (1, 2, '1s', '300s') timestamp(native_us)",
            ReportedName::Nothing,
        ),
        (
            "TQL EVAL (1, 1, '1s', '0') native_us",
            ReportedName::Metric("native_us"),
        ),
        (
            "TQL EVAL (1, 1, '1s', '0') native_us @ 1",
            ReportedName::Metric("native_us"),
        ),
        (
            "TQL EVAL (1, 2, '1s') count_over_time(native_us[1s])",
            ReportedName::Nothing,
        ),
        (
            "TQL EVAL (1, 2, '1s') sum_over_time(native_us[1s] offset 1ms)",
            ReportedName::Nothing,
        ),
        (
            "TQL EVAL (1, 2, '1s') last_over_time(native_us[1s])",
            ReportedName::Metric("native_us"),
        ),
        (
            "TQL EVAL (2, 2, '1s') rate(native_us[2s])",
            ReportedName::Nothing,
        ),
        (
            "TQL EVAL (1, 1, '1s') irate(native_ns[5s])",
            ReportedName::Nothing,
        ),
        (
            "TQL EVAL (1, 1, '1s') idelta(native_ns[5s])",
            ReportedName::Nothing,
        ),
        (
            "TQL EVAL (1, 1, '1s') count_over_time(native_ns[5s])",
            ReportedName::Nothing,
        ),
        (
            "TQL EVAL (1, 1, '1s', '1s') native_sec offset 500ms",
            ReportedName::Metric("native_sec"),
        ),
        (
            "TQL EVAL (3, 3, '1s') rate(native_sec[3s])",
            ReportedName::Nothing,
        ),
    ];
    // Once from the memtables and once from the flushed files.
    for flushed in [false, true] {
        if flushed {
            for table in ["native_us", "native_ns", "native_sec"] {
                execute_all(
                    &instance,
                    &format!("ADMIN FLUSH_TABLE('{table}')"),
                    QueryContext::arc(),
                )
                .await;
            }
        }
        for (sql, reported) in queries {
            observer.clear();
            let native = eval_text_rows(
                collected(native_tql_output(&instance, sql, true).await).await,
                reported,
            );
            assert_native_rows_plan(&observer);
            let legacy = eval_text(collected(native_tql_output(&instance, sql, false).await).await);
            assert_text_rows_close(&legacy, &native, &format!("flushed={flushed}: {sql}"));
        }
    }
}

/// Two physical metric tables may store their time index in different units, and a nameless
/// selector can discover logical metrics on both. A native input reads its timestamps in one unit,
/// so such a selector is declined rather than answered with one source's ticks read in the other's.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_discovery_over_mixed_time_units_is_declined() {
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, _observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_discovery_mixed_units")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE mixed_phy_ms (ts TIMESTAMP TIME INDEX, v DOUBLE) \
             ENGINE=metric WITH (\"physical_metric_table\" = \"\"); \
         CREATE TABLE mixed_phy_us (ts TIMESTAMP(6) TIME INDEX, v DOUBLE) \
             ENGINE=metric WITH (\"physical_metric_table\" = \"\"); \
         CREATE TABLE mixed_a (host STRING NULL, ts TIMESTAMP NOT NULL, v DOUBLE NULL, \
             TIME INDEX (ts), PRIMARY KEY (host)) \
             ENGINE=metric WITH (on_physical_table = 'mixed_phy_ms'); \
         CREATE TABLE mixed_b (host STRING NULL, ts TIMESTAMP(6) NOT NULL, v DOUBLE NULL, \
             TIME INDEX (ts), PRIMARY KEY (host)) \
             ENGINE=metric WITH (on_physical_table = 'mixed_phy_us'); \
         INSERT INTO mixed_a (host, ts, v) VALUES ('a', 1000, 2); \
         INSERT INTO mixed_b (host, ts, v) VALUES ('b', 1000000, 8)",
        QueryContext::arc(),
    )
    .await;
    let mut ctx = QueryContext::with_db_name(None);
    ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, "true".to_string());
    ctx.set_metric_discovery_scope(MetricDiscoveryScope::allowed(["mixed_a", "mixed_b"].map(
        |table| {
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
                table.to_string(),
            )
        },
    )));
    let result = instance
        .do_query(
            r#"TQL EVAL (2, 2, '1s') count_over_time({host=~"a|b"}[2s])"#,
            Arc::new(ctx),
        )
        .await
        .remove(0);
    // The existing plan, which the statement keeps, refuses a nameless selector.
    if let Ok(output) = result {
        panic!("answered natively: {:?}", native_rows(output, None).await);
    }
}

/// `output` with its stream read here: [`eval_text_rows`] blocks on the stream, which inside this
/// runtime stops being woken once the test task has spent its cooperative budget.
async fn collected(output: Output) -> Output {
    match output.data {
        OutputData::Stream(stream) => {
            Output::new_with_record_batches(collect_batches(stream).await.unwrap())
        }
        _ => output,
    }
}

/// Two [`eval_text`] answers that agree cell by cell, a float cell up to its last digits: the two
/// routes sum the same terms in a different order.
fn assert_text_rows_close(expected: &str, actual: &str, context: &str) {
    let rows = |text: &str| {
        text.lines()
            .map(|line| line.split(" | ").map(str::to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    let (expected_rows, actual_rows) = (rows(expected), rows(actual));
    let close = expected_rows.len() == actual_rows.len()
        && expected_rows.iter().zip(&actual_rows).all(|(left, right)| {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    left == right
                        || matches!(
                            (left.parse::<f64>(), right.parse::<f64>()),
                            (Ok(left), Ok(right))
                                if (left - right).abs() <= 1e-12 * left.abs().max(right.abs())
                        )
                })
        });
    assert!(close, "{context}\nexpected: {expected}\nactual: {actual}");
}

/// The plan the last query ran is the native one, so a row assertion above is about this route.
fn assert_native_rows_plan(observer: &NativePlanObserver) {
    let root = observer.root_plan();
    let display = datafusion::physical_plan::displayable(root.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("FusedSeriesExec"), "{display}");
}

/// [`native_result_rows`] for a statement whose output carries no series labels, such as the
/// data-free `vector(time())`: the evaluation timestamp and the one value, sorted by time.
///
/// The timestamp is found by type rather than by name: the native route names the time index of a
/// statement that reads nothing `time`, while the existing planner names it `ts`.
async fn native_timestamp_rows(output: Output) -> Vec<(i64, f64)> {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let timestamps = batch
                .columns()
                .iter()
                .find(|column| {
                    matches!(
                        column.data_type(),
                        DataType::Timestamp(TimeUnit::Millisecond, _)
                    )
                })
                .expect("a timestamp column");
            let timestamps = timestamps
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
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
            assert_eq!(values.null_count(), 0);
            (0..batch.num_rows())
                .map(|row| (timestamps.value(row), values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.0);
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

fn assert_two_source_native_plan(
    observer: &NativePlanObserver,
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    require_multiple_nonempty_partitions: bool,
) {
    use datafusion::physical_plan::metrics::MetricValue;

    assert_eq!(plan.name(), "FusedSeriesExec");
    assert_eq!(plan.children().len(), 2);
    // Both sides are routed by one matching key into one width, so every output partition owns
    // complete match groups and the node answers as many partitions as it routes. Collapsing them
    // into one would be the gather this shape exists to avoid.
    let widths = plan
        .children()
        .iter()
        .map(|child| child.output_partitioning().partition_count())
        .collect::<Vec<_>>();
    assert!(
        widths.windows(2).all(|pair| pair[0] == pair[1]),
        "both sides must route to the same width: {widths:?}"
    );
    assert_eq!(plan.output_partitioning().partition_count(), widths[0]);
    let schema = plan.schema();
    // The rows layout declares the per-row name column on top of `ts`, the value and `host`; this
    // match computes values, so those rows carry a null name.
    assert_eq!(
        schema.fields().len(),
        4,
        "binary output must have ts, value, host, __name__"
    );
    assert!(schema.field_with_name("ts").is_ok());
    assert!(schema.field_with_name("host").is_ok());
    assert!(schema.field_with_name("__name__").is_ok());
    assert!(schema.field_with_name("kind").is_err());
    let children = plan.children();
    assert!(!Arc::ptr_eq(children[0], children[1]));
    // The scans live in the region trees, which MergeScanExec holds out of tree, so they are read
    // from every observed plan instead of from the frontend fused node's children.
    let scans = observed_series_scans(observer);
    assert_eq!(
        scans.len(),
        2,
        "one independent scan per occurrence of the repeated selector"
    );
    assert!(!Arc::ptr_eq(&scans[0], &scans[1]));
    for (source, child) in children.iter().enumerate() {
        assert_eq!(child.output_partitioning().partition_count(), 4);
        let scan = scans[source].as_ref();
        let metrics = scan.metrics().expect("series scan metrics");
        assert!(metrics.output_rows().expect("series scan output rows") > 0);
        let mut nonempty_partitions = [false; 4];
        for metric in metrics.iter() {
            if let MetricValue::OutputRows(count) = metric.value()
                && let Some(partition) = metric.partition()
                && let Some(nonempty) = nonempty_partitions.get_mut(partition)
            {
                *nonempty |= count.value() > 0;
            }
        }
        if require_multiple_nonempty_partitions {
            assert!(
                nonempty_partitions
                    .iter()
                    .filter(|nonempty| **nonempty)
                    .count()
                    > 1,
                "source {source} must produce rows on multiple physical scan partitions: {nonempty_partitions:?}"
            );
        }
    }
    let display = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert_eq!(display.matches("source=0").count(), 1, "{display}");
    assert_eq!(display.matches("source=1").count(), 1, "{display}");
}

/// The registrations the fused node's tree made, exactly as they were recorded.
///
/// [`datafusion::physical_plan::ExecutionPlan::metrics`] leaves them out: a flat set cannot say
/// which tree node recorded what, so the node keeps its own set for readers that key by name.
fn fused_metric_set(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> datafusion::physical_plan::metrics::MetricsSet {
    plan.downcast_ref::<promql::extension_plan::FusedSeriesExec>()
        .unwrap_or_else(|| panic!("the plan is a FusedSeriesExec, not {}", plan.name()))
        .node_metric_set()
}

/// The node's `native.root.output_values` counts, indexed by the partition the node ran on.
///
/// `None` when the node registered no such metric. A `Some` vector is as wide as the node, and an
/// entry is zero when that partition published nothing - which is a partition the router handed no
/// series.
///
/// Those zeros are not a property of the statement: each series is placed on the owner the canonical
/// router hashes its stored key onto, over however many series each scan partition delivers, so a
/// caller may sum this vector or ask that some partition published, but it must not assert which
/// entry is non-zero, how many owners were used, or that every one of them was.
fn output_values_per_partition(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Option<Vec<u64>> {
    use datafusion::physical_plan::metrics::MetricValue;

    if plan.name() != "FusedSeriesExec" {
        return None;
    }
    let partition_count = plan.output_partitioning().partition_count();
    let metrics = fused_metric_set(plan);
    let mut values_by_partition = vec![0_u64; partition_count];
    let mut found_metric = false;
    for metric in metrics.iter() {
        if metric.value().name() == "native.root.output_values"
            && let MetricValue::Count { count, .. } = metric.value()
            && let Some(partition) = metric.partition()
            && let Some(value) = values_by_partition.get_mut(partition)
        {
            *value += count.value() as u64;
            found_metric = true;
        }
    }
    found_metric.then_some(values_by_partition)
}

/// Per-partition `OutputRows` of every unique `SeriesScanExec` the last native statement planned.
///
/// A `Some` entry is as wide as that scan's output partitioning, so a partition that was never
/// executed shows up as a zero instead of disappearing. `None` is a scan whose metrics report no
/// per-partition `OutputRows` at all, and an all-zero entry is a scan that ran without producing
/// rows; neither is reported as a one-partition read, because both mean the scan is no evidence of
/// how the series were distributed.
fn observed_series_scan_partitions(observer: &NativePlanObserver) -> Vec<Option<Vec<u64>>> {
    use datafusion::physical_plan::metrics::MetricValue;

    observed_series_scans(observer)
        .into_iter()
        .map(|scan| {
            let metrics = scan.metrics()?;
            let mut rows_by_partition = vec![0_u64; scan.output_partitioning().partition_count()];
            let mut found_metric = false;
            for metric in metrics.iter() {
                if let MetricValue::OutputRows(count) = metric.value()
                    && let Some(partition) = metric.partition()
                    && let Some(rows) = rows_by_partition.get_mut(partition)
                {
                    *rows += count.value() as u64;
                    found_metric = true;
                }
            }
            found_metric.then_some(rows_by_partition)
        })
        .collect()
}

/// One line per scan of [`observed_series_scan_partitions`]: the rows it read on each partition and
/// how many of those partitions were nonempty.
fn format_series_scan_partitions(vectors: &[Option<Vec<u64>>]) -> String {
    if vectors.is_empty() {
        return "no series scan observed".to_string();
    }
    vectors
        .iter()
        .map(|rows_by_partition| match rows_by_partition {
            None => "no output rows metric".to_string(),
            Some(rows) => {
                let nonempty = rows.iter().filter(|rows| **rows > 0).count();
                format!("{rows:?} ({nonempty}/{} nonempty)", rows.len())
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The width every native statement in this module declares, and therefore the number of entries
/// each per-partition vector must have.
const DECLARED_PARTITIONS: usize = 4;

/// Asserts every series scan the last native statement ran produced rows on all
/// [`DECLARED_PARTITIONS`] of its scan partitions, and reports the per-partition row counts it
/// found.
///
/// The declared width is not evidence on its own: what a scan actually did is the per-partition
/// `OutputRows` it recorded, so every one of those partitions has to be nonempty here rather than
/// most of them. This fixture offers four mutually exclusive time windows, each flushed into a
/// range of its own, which is how a scan of it gets several range streams to read: under this
/// fixture the earlier single-range observation read one of the four scan partitions, and the four
/// range streams read all four. The check is the scan's own counter of where it produced rows; it
/// does not claim that an output partition corresponds to one SST or to one of those windows, and
/// a scan that reports no metric at all is refused, because it is no evidence either way.
fn assert_series_scan_partitions(label: &str, observer: &NativePlanObserver) -> String {
    let vectors = observed_series_scan_partitions(observer);
    assert!(
        !vectors.is_empty(),
        "{label}: the native statement ran no series scan"
    );
    for rows_by_partition in &vectors {
        let rows = rows_by_partition.as_ref().unwrap_or_else(|| {
            panic!("{label}: a series scan reports no per-partition output rows: {vectors:?}")
        });
        assert_eq!(
            DECLARED_PARTITIONS,
            rows.len(),
            "{label}: a series scan must carry the {DECLARED_PARTITIONS} partitions the statement \
             runs at: {rows:?}"
        );
        let empty_partitions = rows.iter().filter(|rows| **rows == 0).count();
        assert_eq!(
            0, empty_partitions,
            "{label}: every one of the {DECLARED_PARTITIONS} scan partitions must read rows, but \
             {empty_partitions} of them read none: {rows:?} (all scan vectors: {vectors:?})"
        );
    }
    format_series_scan_partitions(&vectors)
}

/// One native fused node of the last statement, with the vector it reported.
struct FusedNodeVector {
    /// Where the node was planned: `region[0]` for a node of a region-side tree, `root` for a node
    /// of the frontend's own tree.
    site: String,
    /// The node's output partition count, which is the width its per-partition vector must have.
    partitions: usize,
    /// Its per-partition `native.root.output_values` counts, `None` when it reports no such metric.
    counts: Option<Vec<u64>>,
}

impl FusedNodeVector {
    /// `site=[counts] (nonempty/width)`, or `site=no-metric` when the node reported nothing.
    fn report(&self) -> String {
        match &self.counts {
            None => format!("{}=no-metric", self.site),
            Some(counts) => {
                let nonempty = counts.iter().filter(|count| **count > 0).count();
                format!("{}={counts:?} ({nonempty}/{})", self.site, counts.len())
            }
        }
    }
}

/// Every native fused node of `plan`, labelled with `site` and appended to `found`.
fn collect_fused_nodes(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    site: &str,
    found: &mut Vec<FusedNodeVector>,
) {
    let mut stack = vec![plan.clone()];
    let mut ordinal = 0;
    while let Some(node) = stack.pop() {
        if node.name() == "FusedSeriesExec" {
            let site = if ordinal == 0 {
                site.to_string()
            } else {
                format!("{site}#{ordinal}")
            };
            ordinal += 1;
            found.push(FusedNodeVector {
                site,
                partitions: node.output_partitioning().partition_count(),
                counts: output_values_per_partition(&node),
            });
        }
        stack.extend(node.children().into_iter().cloned());
    }
}

/// One `site=vector (nonempty/width)` entry per node, for reports and failure messages.
fn fused_vectors_report(nodes: &[FusedNodeVector]) -> String {
    if nodes.is_empty() {
        return "none".to_string();
    }
    nodes
        .iter()
        .map(FusedNodeVector::report)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Asserts the native work of the last statement ran at the width of the
/// [`DECLARED_PARTITIONS`] partitions it declares and published values, and reports the vectors it
/// checked.
///
/// `native.root.output_values` counts what a fused node published on the partition it ran, and the
/// canonical router places each series on one owner by hashing its stored key over whatever the scan
/// partitions delivered, so an owner can be left with nothing to publish even when every scan
/// partition read rows. Which partition published what - and how many partitions published at all -
/// is therefore not a property of the statement, and no vector below is asserted entry by entry.
/// What is asserted is per node: it registers the metric, it runs at the width the statement
/// declares, and it published the values it evaluated. Both placements are read:
///
/// - A statement that runs native work inside a region has one fused node per region-side tree.
///   Every one of those must report the metric, be as wide as the statement, and have published
///   something. The frontend's own node is the finalize over the region streams; its vector is
///   reported but not asserted, because a final combine is not required to publish on every
///   partition (it may even be a single partition, and the answer is assembled from the per-region
///   streams above it).
/// - A statement that only runs native work in the frontend (the frontend-raw routes, which run no
///   native node inside a region) is read from [`NativePlanObserver::root_plan`] instead: its
///   fused nodes must be found there, and every one of them that is as wide as the statement must
///   report the metric and publish something. A narrower final merge is ignored, because it is the
///   last combine rather than a partition-wide execution; a tree whose fused nodes are all narrow is
///   refused, because nothing in it shows a partition-wide execution.
///
/// A width-4 node without the metric, or one that published nothing, is a failure wherever the
/// metric is required, never an exemption. The failure messages name the label, the node's site and
/// the vectors, so a regression that loses the metric or a node's values is reported as itself.
fn assert_cross_partition_native_values(label: &str, observer: &NativePlanObserver) -> String {
    let mut region_nodes = Vec::new();
    for (index, plan) in observer.region_plans().iter().enumerate() {
        collect_fused_nodes(plan, &format!("region[{index}]"), &mut region_nodes);
    }
    let mut root_nodes = Vec::new();
    collect_fused_nodes(&observer.root_plan(), "root", &mut root_nodes);

    if !region_nodes.is_empty() {
        let region_report = fused_vectors_report(&region_nodes);
        for node in &region_nodes {
            let counts = match &node.counts {
                Some(counts) => counts,
                None => panic!(
                    "{label}: the region-side fused node {} reports no \
                     `native.root.output_values`, so nothing shows which partitions it executed; \
                     region vectors: {region_report}",
                    node.site
                ),
            };
            assert_eq!(
                DECLARED_PARTITIONS, node.partitions,
                "{label}: the region-side fused node {} runs at {} partitions, not the \
                 {DECLARED_PARTITIONS} partitions the statement declares, and its vector is \
                 {counts:?}; region vectors: {region_report}",
                node.site, node.partitions
            );
            // Which of the partitions were handed series is the canonical router's hash, so a zero
            // is not a lost value; that the node published the values it evaluated is.
            let published: u64 = counts.iter().sum();
            assert!(
                published > 0,
                "{label}: the region-side fused node {} published no value on any of the \
                 {DECLARED_PARTITIONS} partitions it runs: {counts:?}; region vectors: \
                 {region_report}",
                node.site
            );
        }
        return format!(
            "regions: {region_report} | frontend finalize (reported, not asserted): {}",
            fused_vectors_report(&root_nodes)
        );
    }

    assert!(
        !root_nodes.is_empty(),
        "{label}: the statement ran no native node inside a region and the frontend tree holds no \
         fused node either; root:\n{}",
        display_plan_tree(&observer.root_plan())
    );
    let root_report = fused_vectors_report(&root_nodes);
    let wide = root_nodes
        .iter()
        .filter(|node| node.partitions == DECLARED_PARTITIONS)
        .collect::<Vec<_>>();
    assert!(
        !wide.is_empty(),
        "{label}: the frontend tree's fused nodes are all narrower than the \
         {DECLARED_PARTITIONS} partitions the statement declares, so none of them shows a \
         partition-wide execution: {root_report}; root:\n{}",
        display_plan_tree(&observer.root_plan())
    );
    for node in &wide {
        let counts = match &node.counts {
            Some(counts) => counts,
            None => panic!(
                "{label}: the {}-partition frontend fused node {} reports no \
                 `native.root.output_values`, so nothing shows which partitions it executed; root \
                 vectors: {root_report}; root:\n{}",
                node.partitions,
                node.site,
                display_plan_tree(&observer.root_plan())
            ),
        };
        // An ordinary partition can be empty: the raw frames are routed by their stored key, so a
        // small fixture need not put work on every one of them. The producer's width is still
        // required, and so is that it published the values it evaluated.
        let published: u64 = counts.iter().sum();
        assert!(
            published > 0,
            "{label}: the {}-partition frontend fused node {} published nothing on any of its \
             partitions: {counts:?}; root vectors: {root_report}",
            node.partitions,
            node.site
        );
    }
    format!("frontend-raw root: {root_report}")
}

/// The partitions of `plan` that recorded a nonzero `metric` count, in ascending order.
///
/// A native execution partition that was handed no series records nothing, so this is where the
/// series the plan actually read were evaluated - which is what a cross-partition claim is about.
/// The metric is named by the caller because a plan that fails after reading still reports what it
/// read: `native.root.input_samples` is recorded before a collision is raised, while
/// `native.root.output_values` only counts values that were published.
fn nonempty_partitions(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    metric: &str,
) -> Vec<usize> {
    use datafusion::physical_plan::metrics::MetricValue;

    let partition_count = plan.output_partitioning().partition_count();
    let metrics = fused_metric_set(plan);
    let mut recorded = vec![0_u64; partition_count];
    let mut found = false;
    for entry in metrics.iter() {
        if entry.value().name() != metric {
            continue;
        }
        let MetricValue::Count { count, .. } = entry.value() else {
            continue;
        };
        if let Some(value) = entry
            .partition()
            .and_then(|partition| recorded.get_mut(partition))
        {
            *value += count.value() as u64;
            found = true;
        }
    }
    assert!(
        found,
        "the plan reports `{metric}` for every partition: {}",
        display_plan_tree(plan)
    );
    recorded
        .into_iter()
        .enumerate()
        .filter(|(_, values)| *values > 0)
        .map(|(partition, _)| partition)
        .collect()
}

/// The fused producer of the statement's frontend plan.
fn frontend_producer(
    observer: &Arc<NativePlanObserver>,
) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    find_plan_node(&observer.root_plan(), "FusedSeriesExec")
}

/// The region's prepared series scan behind this statement, when the fused node reads it through a
/// router or a merge scan instead of directly.
fn region_series_scan(observer: &Arc<NativePlanObserver>) -> Option<Arc<SeriesScanExec>> {
    observer.region_plans().into_iter().find_map(|plan| {
        find_plan_node(&plan, "SeriesScanExec")
            .and_then(|node| Arc::downcast::<SeriesScanExec>(node).ok())
    })
}

/// The canonical router over a fused producer, wherever the statement planned it.
///
/// A canonical boundary's router runs where its producer does. For a statement root the producer is
/// regional, so the router is part of the region plan; under an aggregate the boundary is a segment
/// of its own and is still regional; a rewrite above a whole-vector consumer is assembled in the
/// frontend, so its router is there. The claim these tests make is that such a router exists
/// instead of a gather, not which plan holds it.
fn canonical_router(
    observer: &Arc<NativePlanObserver>,
) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    std::iter::once(observer.root_plan())
        .chain(observer.region_plans())
        .find_map(|plan| {
            find_plan_nodes_over(&plan, "SeriesRepartitionExec", "FusedSeriesExec")
                .into_iter()
                .find(|router| {
                    router.children().iter().any(|child| {
                        promql::extension_plan::is_canonical_producer_exec(child.as_ref())
                    })
                })
        })
}

/// Asserts the fused producer keeps the four partitions of the scan it reads, and that the
/// partitions published `expected_values` values between them.
///
/// The width is the statement's: the producer folds over the scan-width router, so it must not
/// collapse onto one partition. Which of those partitions were handed a series is not: the canonical
/// router hashes each series' stored key onto one owner, so an owner can be left with nothing even
/// when every scan partition read rows. The distribution-independent figure is the total, which is
/// one published value per value the statement answers with.
fn assert_four_native_partitions(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    expected_values: usize,
) {
    assert_eq!(plan.name(), "FusedSeriesExec");
    assert_eq!(plan.output_partitioning().partition_count(), 4);
    assert_eq!(
        plan.children()[0].output_partitioning().partition_count(),
        4,
        "fused scan must have four partitions"
    );
    let values_by_partition = output_values_per_partition(plan).unwrap_or_else(|| {
        panic!(
            "the fused producer must register `native.root.output_values` for the partitions it \
             runs, otherwise nothing here shows that it executed"
        )
    });
    assert_eq!(
        expected_values,
        values_by_partition.iter().sum::<u64>() as usize,
        "the native partitions together must publish one value per value the statement answers \
         with: {values_by_partition:?}"
    );
}

/// Inspect the actual logical FusedSeries tree before executing a representative deep query.
async fn assert_deep_native_plan(instance: &Arc<Instance>, query: &PromQuery) {
    use promql::extension_plan::FusedSeries;
    use promql::native::plan::{NativeExpr, NativeNode};

    fn depth(node: &NativeNode) -> usize {
        1 + match &node.expr {
            NativeExpr::Input(_) | NativeExpr::Scalar(_) => 0,
            NativeExpr::Instant { input, .. } | NativeExpr::Map { input, .. } => depth(input),
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            } => parameters.iter().map(depth).fold(depth(input), usize::max),
            NativeExpr::Binary { left, right, .. } => depth(left).max(depth(right)),
            other => panic!("unexpected node in deep expression: {other:?}"),
        }
    }
    fn fused(plan: &datafusion::logical_expr::LogicalPlan) -> Option<&FusedSeries> {
        if let datafusion::logical_expr::LogicalPlan::Extension(extension) = plan
            && let Some(node) = extension.node.as_any().downcast_ref::<FusedSeries>()
        {
            return Some(node);
        }
        plan.inputs().into_iter().find_map(fused)
    }
    let ctx = native_query_context(true, 2);
    let statement = QueryLanguageParser::parse_promql(query, &ctx).unwrap();
    let plan = instance
        .query_engine()
        .planner()
        .plan(&statement, ctx)
        .await
        .unwrap();
    let display = plan.display_indent().to_string();
    let tree = fused(&plan)
        .unwrap_or_else(|| panic!("native deep tree: {display}"))
        .tree();
    assert!(depth(tree.root()) > 64, "{display}");
    assert!(!display.contains("Join:"), "{display}");
}

/// The representative deep expression executes on a standalone instance, on the test's own stack.
///
/// The expression is deep enough that the walks over it used to abort the runtime worker rather
/// than answer: planning it walks the tree once, and so do building its runtime and pulling it.
/// Those walks grow their own stacks now, so nothing here owns a stack large enough to hide the
/// recursion - a build that reintroduces the overflow aborts this test instead of being masked by
/// one. The plan, the runtime and the answer are all the deep expression's own.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_deep_binary_standalone() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_deep_binary_standalone",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE deep_standalone (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); INSERT INTO deep_standalone VALUES ('x', 180000, 3), ('x', 240000, 4), ('y', 180000, 4), ('y', 240000, 5)",
        QueryContext::arc(),
    )
    .await;
    let query = PromQuery {
        query: format!("clamp(deep_standalone{}, 0, 1000)", " + 1".repeat(80)),
        start: "180".to_string(),
        end: "240".to_string(),
        step: "60s".to_string(),
        ..PromQuery::default()
    };
    assert_deep_native_plan(&instance, &query).await;
    observer.clear();
    let output = instance
        .do_promql_query(&query, native_query_context(true, 2))
        .await
        .remove(0)
        .unwrap();
    assert_eq!(
        native_result_rows(output).await,
        vec![
            (180_000, "x".to_string(), 83.0),
            (180_000, "y".to_string(), 84.0),
            (240_000, "x".to_string(), 84.0),
            (240_000, "y".to_string(), 85.0)
        ]
    );
    let observed = observed_physical_nodes(&observer);
    let names: Vec<_> = observed.iter().map(|node| node.name()).collect();
    assert!(
        names.contains(&"FusedSeriesExec"),
        "no native fused node: {names:?}"
    );
    assert!(
        names.iter().all(|name| !name.contains("JoinExec")),
        "legacy join: {names:?}"
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

async fn native_scalar_rows(output: Output) -> Vec<(i64, f64)> {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("PromQL query returns record batches"),
    };
    let mut rows = Vec::new();
    for batch in batches {
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
        assert_eq!(values.len(), 1, "one result value column");
        // A rows layout declares the per-row name column, and a scalar-shaped answer names no row:
        // the column is there and null for every row rather than absent. Any other column here
        // would be a label this answer must not carry.
        let labels = batch
            .columns()
            .iter()
            .filter(|column| {
                column.data_type() != &DataType::Float64
                    && !matches!(
                        column.data_type(),
                        DataType::Timestamp(TimeUnit::Millisecond, _)
                    )
            })
            .collect::<Vec<_>>();
        assert!(
            labels.len() <= 1,
            "a scalar output has no labels besides the name column"
        );
        for label in labels {
            assert_eq!(
                label.len(),
                label.null_count(),
                "a scalar answer names no row: {label:?}"
            );
        }
        let values = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
        for row in 0..batch.num_rows() {
            rows.push((timestamps.value(row), values.value(row)));
        }
    }
    rows.sort_by_key(|row| row.0);
    rows
}

/// The shape a native distributed plan is asserted on: its fused producer, the region set every
/// merge scan selects, the routers that reconcile series several regions hold, and the per-series
/// order every merge scan declares.
struct NativePlanShape {
    fused: usize,
    regions_per_scan: Vec<usize>,
    routers: usize,
    /// `SeriesRepartitionExec` nodes inside the subtree of the single `FusedSeriesExec`, i.e. the
    /// routers that have to reconcile fragments of one series before the fused node reads them.
    /// A router above the fused node (the coalescer of its output) is not one of them; `routers`
    /// counts those too.
    routers_below_fused: usize,
    /// The `greptime.series.ordering` every merge scan declares, in traversal order. `contiguous`
    /// means one region holds each series whole; `interleaved` means a series may be split.
    scan_orderings: Vec<Option<String>>,
}

/// Every node of the tree rooted at `root`, the root included.
fn native_plan_nodes(root: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut nodes = Vec::new();
    let mut stack = vec![Arc::clone(root)];
    while let Some(node) = stack.pop() {
        stack.extend(node.children().into_iter().map(Arc::clone));
        nodes.push(node);
    }
    nodes
}

fn native_plan_shape(plan: &Arc<dyn ExecutionPlan>) -> NativePlanShape {
    let nodes = native_plan_nodes(plan);
    let mut shape = NativePlanShape {
        fused: 0,
        regions_per_scan: Vec::new(),
        routers: 0,
        routers_below_fused: 0,
        scan_orderings: Vec::new(),
    };
    for node in &nodes {
        shape.fused += usize::from(node.name() == "FusedSeriesExec");
        shape.routers += usize::from(node.name() == "SeriesRepartitionExec");
        if let Some(scan) = node.downcast_ref::<MergeScanExec>() {
            shape.regions_per_scan.push(
                scan.regions()
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len(),
            );
            shape.scan_orderings.push(
                scan.schema()
                    .metadata()
                    .get("greptime.series.ordering")
                    .cloned(),
            );
        }
    }
    // Count the routers below the outermost fused node only. A split aggregate nests a second
    // fused producer under the first, and walking both subtrees would count its routers twice.
    fn count_below_fused(node: &Arc<dyn ExecutionPlan>, inside: bool, count: &mut usize) {
        let fused = node.name() == "FusedSeriesExec";
        if fused && !inside {
            for child in node.children() {
                for nested in native_plan_nodes(child) {
                    *count += usize::from(nested.name() == "SeriesRepartitionExec");
                }
            }
            return;
        }
        for child in node.children() {
            count_below_fused(child, inside || fused, count);
        }
    }
    count_below_fused(plan, false, &mut shape.routers_below_fused);
    shape
}

fn assert_native_merge_scans(
    plan: &Arc<dyn ExecutionPlan>,
    expected_scans: usize,
    expected_regions: usize,
) {
    let shape = native_plan_shape(plan);
    assert!(
        shape.fused >= 1,
        "a native plan keeps at least one fused producer: {plan:?}"
    );
    assert_eq!(
        shape.regions_per_scan.len(),
        expected_scans,
        "merge scans: {plan:?}"
    );
    assert_eq!(
        shape.regions_per_scan,
        vec![expected_regions; expected_scans],
        "all time partitions must be selected: {plan:?}"
    );
    assert!(
        shape.routers > 0,
        "fragmented raw inputs need reconciliation: {plan:?}"
    );
}

/// The shape every disjoint plan has: one fused producer over one merge scan per selector, and every
/// scan selecting one region per partition value. Whether a node above the scans has to move frames
/// is what the two disjoint cases differ in - a global fold gathers, a per-series match does not -
/// so that stays with the caller.
fn assert_native_disjoint_shape(
    plan: &Arc<dyn ExecutionPlan>,
    expected_scans: usize,
    expected_regions: usize,
) -> NativePlanShape {
    let shape = native_plan_shape(plan);
    assert!(
        shape.fused >= 1,
        "a native plan keeps at least one fused producer: {plan:?}"
    );
    assert_eq!(
        shape.regions_per_scan.len(),
        expected_scans,
        "merge scans: {plan:?}"
    );
    assert_eq!(
        shape.regions_per_scan,
        vec![expected_regions; expected_scans],
        "every disjoint region must be selected: {plan:?}"
    );
    shape
}

/// The rows of the cross-region fixture, inserted into the logical tables `{prefix}_a`,
/// `{prefix}_b` and `{prefix}_reset`. The partitioned fixture and its control hold the same rows,
/// so the two differ only in how the engine has to reach them.
fn cross_region_fixture_inserts(prefix: &str) -> String {
    format!(
        "INSERT INTO {prefix}_a (host, val, ts) VALUES ('x', 2, 60000), ('x', 4, 120000), ('x', 6, 180000), ('x', 8, 240000), ('y', 4, 60000), ('y', 6, 120000), ('y', 8, 180000), ('y', 10, 240000); INSERT INTO {prefix}_b (host, val, ts) VALUES ('x', 1, 60000), ('x', 2, 120000), ('x', 3, 180000), ('x', 4, 240000), ('y', 2, 60000), ('y', 3, 120000), ('y', 4, 180000), ('y', 5, 240000); INSERT INTO {prefix}_reset (host, val, ts) VALUES ('x', 0, 0), ('x', 5, 60000), ('x', 10, 120000), ('x', 1, 180000), ('x', 3, 240000), ('x', 5, 300000), ('y', 0, 0), ('y', 1, 60000), ('y', 2, 120000), ('y', 3, 180000), ('y', 4, 240000), ('y', 5, 300000)"
    )
}

/// Real two-datanode TCP acceptance for raw series fragmented by time. Ratios alone can hide
/// history dropped on both sides, so an absolute increase is checked separately as well.
#[tokio::test(flavor = "multi_thread")]
async fn distributed_native_promql_cross_region_targets_match_expected_values() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("distributed_native_promql_cross_region")
        .await
        .with_datanodes(2)
        .with_real_datanode_grpc_addr(true)
        .with_frontend_real_datanode_grpc(true)
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .build(false)
        .await;
    let instance = cluster.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE dist_native_phy (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) PARTITION ON COLUMNS (ts) (ts < '1970-01-01 00:02:00', ts >= '1970-01-01 00:02:00' AND ts < '1970-01-01 00:04:00', ts >= '1970-01-01 00:04:00') ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    for table in ["dist_native_a", "dist_native_b", "dist_native_reset"] {
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {table} (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='dist_native_phy')"
            ),
            QueryContext::arc(),
        )
        .await;
    }
    execute_all(
        &instance,
        &cross_region_fixture_inserts("dist_native"),
        QueryContext::arc(),
    )
    .await;

    // The semantic reference: the same rows in one unpartitioned physical table. The distributed
    // legacy path evaluates every region fragment on its own, so on the partitioned fixture it is
    // not an oracle for these expressions and is deliberately not compared here. The engine over
    // the complete series is the oracle, and the native distributed plan must agree with it.
    execute_all(
        &instance,
        "CREATE TABLE dist_native_ctl_phy (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    for table in [
        "dist_native_ctl_a",
        "dist_native_ctl_b",
        "dist_native_ctl_reset",
    ] {
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {table} (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='dist_native_ctl_phy')"
            ),
            QueryContext::arc(),
        )
        .await;
    }
    execute_all(
        &instance,
        &cross_region_fixture_inserts("dist_native_ctl"),
        QueryContext::arc(),
    )
    .await;

    for (expression, expected) in [
        (
            "sum(increase(dist_native_a[5m])) / sum(increase(dist_native_b[5m]))",
            vec![(180_000, 2.0), (240_000, 2.0)],
        ),
        (
            "sum(max(dist_native_a)) / sum(dist_native_b)",
            vec![(180_000, 8.0 / 7.0), (240_000, 10.0 / 9.0)],
        ),
        (
            "sum(increase(dist_native_b[5m]))",
            vec![(180_000, 5.0), (240_000, 7.0)],
        ),
        (
            "sum(increase(dist_native_reset[5m]))",
            vec![(180_000, 14.0), (240_000, 17.0)],
        ),
    ] {
        let query = PromQuery {
            query: expression.to_string(),
            start: "180".to_string(),
            end: "240".to_string(),
            step: "60s".to_string(),
            ..PromQuery::default()
        };
        // The control tables hold the complete series, so the engine's own evaluation over them
        // defines the expected values; the native distributed plan has to reproduce them.
        let control = expression.replace("dist_native_", "dist_native_ctl_");
        let control_rows = native_scalar_rows(
            instance
                .do_promql_query(
                    &PromQuery {
                        query: control.clone(),
                        ..query.clone()
                    },
                    native_query_context(false, 2),
                )
                .await
                .remove(0)
                .unwrap(),
        )
        .await;
        assert_eq!(
            control_rows.len(),
            expected.len(),
            "the control defines every row: {control}"
        );
        for (actual, expected) in control_rows.iter().zip(expected.iter()) {
            assert_eq!(actual.0, expected.0, "control row timestamp: {control}");
            assert!(
                (actual.1 - expected.1).abs() < 1e-9,
                "control row value: {control}: {control_rows:?}"
            );
        }

        cluster.reset_network_do_get_peak();
        let output = instance
            .do_promql_query(&query, native_query_context(true, 2))
            .await
            .remove(0)
            .unwrap();
        let plan = output.meta.plan.as_ref().expect("frontend physical plan");
        assert_native_merge_scans(plan, usize::from(expression.contains(" / ")) + 1, 3);
        // A global aggregate keeps its split: the partial half runs per partition and the final
        // half folds the exported states, so the window work is not serialized into one instance.
        assert!(
            native_plan_shape(plan).fused >= 2,
            "a global aggregate runs as partial and final halves: {plan:?}"
        );
        let rows = native_scalar_rows(output).await;
        assert_eq!(
            cluster.network_do_get_active(),
            0,
            "every response body is finished: {expression}"
        );
        assert_eq!(
            cluster.network_do_get_peak_active(),
            2,
            "three region streams must use exactly the target-partition bound: {expression}"
        );
        assert_eq!(rows.len(), expected.len(), "all rows: {expression}");
        for (actual, expected) in rows.iter().zip(expected) {
            assert_eq!(actual.0, expected.0, "{expression}");
            assert!(
                (actual.1 - expected.1).abs() < 1e-9,
                "{expression}: {rows:?}"
            );
        }
    }

    // A per-series operation on the fragmented fixture: every series is split across the three
    // time partitions, so no region holds a series whole and none can answer for a host alone.
    // What comes back is per-series rows - two hosts at two timestamps - which a scalar helper
    // cannot read, so this case compares labels as well as values.
    let expression = "increase(dist_native_b[5m])";
    let query = PromQuery {
        query: expression.to_string(),
        start: "180".to_string(),
        end: "240".to_string(),
        step: "60s".to_string(),
        ..PromQuery::default()
    };
    let control = expression.replace("dist_native_", "dist_native_ctl_");
    let control_rows = native_result_rows(
        instance
            .do_promql_query(
                &PromQuery {
                    query: control.clone(),
                    ..query.clone()
                },
                native_query_context(false, 2),
            )
            .await
            .remove(0)
            .unwrap(),
    )
    .await;
    assert_eq!(
        control_rows.len(),
        4,
        "the control over the complete series defines every row: {control}"
    );

    cluster.reset_network_do_get_peak();
    let output = instance
        .do_promql_query(&query, native_query_context(true, 2))
        .await
        .remove(0)
        .unwrap();
    let plan = output.meta.plan.as_ref().expect("frontend physical plan");
    let shape = native_plan_shape(plan);
    assert!(
        shape.fused >= 1,
        "a native plan keeps at least one fused producer: {plan:?}"
    );
    assert_eq!(
        shape.regions_per_scan,
        vec![3],
        "the selector reads all three time partitions: {plan:?}"
    );
    assert!(
        shape.routers_below_fused >= 1,
        "time partitioning splits every series across regions, so the region fragments must be \
         reconciled before the fused node reads them: {plan:?}"
    );
    assert!(
        shape
            .scan_orderings
            .iter()
            .all(|ordering| ordering.as_deref() == Some("interleaved")),
        "the frontend cannot prove a series stays in one time partition, so the scans must not \
         claim contiguous per-series order: {:?}: {plan:?}",
        shape.scan_orderings
    );
    let rows = native_result_rows(output).await;
    assert_eq!(
        cluster.network_do_get_active(),
        0,
        "every response body is finished: {expression}"
    );
    let peak = cluster.network_do_get_peak_active();
    assert!(
        (1..=2).contains(&peak),
        "fragmented region streams stay within the target-partition bound: {expression}: {peak}"
    );
    assert_eq!(control_rows, rows, "{expression}");
}

/// A deep native tree executes on the frontend while a true two-DN gRPC query reads fragments.
#[test]
fn distributed_native_promql_deep_binary_over_real_grpc() {
    std::thread::Builder::new()
        .name("deep-native-grpc".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    common_telemetry::init_default_ut_logging();
                    let cluster = GreptimeDbClusterBuilder::new("distributed_native_promql_deep_binary")
                        .await
                        .with_datanodes(2)
                        .with_real_datanode_grpc_addr(true)
                        .with_frontend_real_datanode_grpc(true)
                        .with_query_options(QueryOptions { experimental_native_promql: true, ..Default::default() })
                        .build(false)
                        .await;
                    let instance = cluster.fe_instance().clone();
                    execute_all(&instance, "CREATE TABLE deep_dist_phy (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) PARTITION ON COLUMNS (ts) (ts < '1970-01-01 00:02:00', ts >= '1970-01-01 00:02:00' AND ts < '1970-01-01 00:04:00', ts >= '1970-01-01 00:04:00') ENGINE=metric WITH ('physical_metric_table'='true'); CREATE TABLE deep_dist_b (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='deep_dist_phy'); INSERT INTO deep_dist_b (host,val,ts) VALUES ('x',1,60000),('x',2,120000),('x',3,180000),('x',4,240000),('y',2,60000),('y',3,120000),('y',4,180000),('y',5,240000)", QueryContext::arc()).await;
                    let query = PromQuery {
                        query: format!("clamp(deep_dist_b{}, 0, 1000)", " + 1".repeat(80)),
                        start: "180".to_string(),
                        end: "240".to_string(),
                        step: "60s".to_string(),
                        ..PromQuery::default()
                    };
                    assert_deep_native_plan(&instance, &query).await;
                    cluster.reset_network_do_get_peak();
                    let output = instance.do_promql_query(&query, native_query_context(true, 2)).await.remove(0).unwrap();
                    let plan = output.meta.plan.as_ref().expect("frontend physical plan");
                    assert_native_merge_scans(plan, 1, 3);
                    let display = datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
                    assert!(display.contains("FusedSeriesExec") && !display.contains("JoinExec"), "{display}");
                    assert_eq!(native_result_rows(output).await, vec![(180_000, "x".to_string(), 83.0), (180_000, "y".to_string(), 84.0), (240_000, "x".to_string(), 84.0), (240_000, "y".to_string(), 85.0)]);
                    assert!(cluster.network_do_get_peak_active() > 0, "remote gRPC reads occurred");
                    assert_eq!(cluster.network_do_get_active(), 0, "all response bodies closed");
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

/// A composed binary consumes a real float canonical boundary before scaling histograms.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_composed_binary_canonical_operand() {
    use api::greptime_proto::io::prometheus::write::v2::Histogram;
    use api::greptime_proto::io::prometheus::write::v2::histogram::{Count, ZeroCount};
    use axum::body::{Body, to_bytes};
    use common_query::native_histogram::read_histogram;
    use datatypes::arrow::array::StructArray;
    use http::Request;
    use prost::Message;
    use servers::prom_remote_write::v2::test_util as remote_write_v2;
    use servers::prom_store::snappy_compress;
    use tower::ServiceExt;

    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_composed_binary_canonical_operand",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(&instance, "CREATE TABLE canonical_float (job STRING, host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(job,host)); INSERT INTO canonical_float VALUES ('api','a',0,2),('api','b',600000,3)", QueryContext::arc()).await;
    let server = crate::test_util::build_test_prom_server(instance.clone(), false, None).build();
    let router = server.build(server.make_app()).unwrap();
    let request = remote_write_v2::request_with_labels_and_histograms(
        vec![("__name__", "canonical_hist"), ("job", "api")],
        [0, 600_000]
            .into_iter()
            .map(|timestamp| Histogram {
                timestamp,
                count: Some(Count::CountInt(4)),
                zero_count: Some(ZeroCount::ZeroCountInt(4)),
                sum: 8.0,
                ..Default::default()
            })
            .collect(),
    );
    let response = router
        .oneshot(
            Request::post("/v1/prometheus/write")
                .header("Content-Encoding", "snappy")
                .header(
                    "Content-Type",
                    "application/x-protobuf;proto=io.prometheus.write.v2.Request",
                )
                .body(Body::from(
                    snappy_compress(&request.encode_to_vec()).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        204,
        "{:?}",
        to_bytes(response.into_body(), usize::MAX).await.unwrap()
    );

    let expression =
        r#"label_replace(canonical_float, "host", "same", "host", ".*") * on(job) canonical_hist"#;
    let query = PromQuery {
        query: expression.to_string(),
        start: "0".to_string(),
        end: "600".to_string(),
        step: "600s".to_string(),
        ..PromQuery::default()
    };
    let ctx = native_query_context(true, 4);
    let stmt = QueryLanguageParser::parse_promql(&query, &ctx).unwrap();
    let logical = instance
        .query_engine()
        .planner()
        .plan(&stmt, ctx)
        .await
        .unwrap();
    let logical_display = logical.display_indent().to_string();
    let datafusion::logical_expr::LogicalPlan::Extension(extension) = &logical else {
        panic!("{logical_display}")
    };
    let binary = extension
        .node
        .as_any()
        .downcast_ref::<promql::extension_plan::FusedSeries>()
        .unwrap_or_else(|| panic!("{logical_display}"));
    assert!(
        matches!(
            binary.tree().root().expr,
            promql::native::plan::NativeExpr::Binary { .. }
        ),
        "{logical_display}"
    );
    assert_eq!(binary.children().len(), 2, "{logical_display}");
    assert!(
        logical_display.contains("output: series_frame") && !logical_display.contains("Join:"),
        "{logical_display}"
    );

    observer.clear();
    let output = instance
        .do_promql_query(&query, native_query_context(true, 4))
        .await
        .remove(0)
        .unwrap();
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!(),
    };
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let time = batch
            .columns()
            .iter()
            .find(|column| matches!(column.data_type(), DataType::Timestamp(..)))
            .unwrap();
        let time = cast(time.as_ref(), &DataType::Int64).unwrap();
        let time = time.as_any().downcast_ref::<Int64Array>().unwrap();
        let job = batch.column_by_name("job").unwrap();
        let job = cast(job.as_ref(), &DataType::Utf8).unwrap();
        let job = job.as_any().downcast_ref::<StringArray>().unwrap();
        let histogram = batch
            .columns()
            .iter()
            .find_map(|column| column.as_any().downcast_ref::<StructArray>())
            .unwrap();
        for row in 0..batch.num_rows() {
            let h = read_histogram(histogram, row).unwrap().unwrap();
            rows.push((
                time.value(row),
                job.value(row).to_string(),
                h.count,
                h.zero_count,
                h.sum,
            ));
        }
    }
    rows.sort_by_key(|row| row.0);
    assert_eq!(
        rows,
        vec![
            (0, "api".to_string(), 8.0, 8.0, 16.0),
            (600_000, "api".to_string(), 12.0, 12.0, 24.0)
        ]
    );
    assert!(
        canonical_router(&observer).is_some(),
        "canonical producer must route its complete float label set"
    );

    // Overlapping inputs must fail at the canonical boundary before the binary can conceal them.
    execute_all(
        &instance,
        "INSERT INTO canonical_float VALUES ('api','b',0,3)",
        QueryContext::arc(),
    )
    .await;
    observer.clear();
    let result = instance
        .do_promql_query(&query, native_query_context(true, 4))
        .await
        .remove(0);
    let error = match result {
        Ok(output) => collect_error(output).await,
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("cannot contain metrics with the same labelset"),
        "{error}"
    );
    assert!(
        canonical_router(&observer).is_some(),
        "the canonical producer must check the overlapping series"
    );
}

/// The shapes the native route used to refuse answer what the existing plan answers.
///
/// A ranking keeps the kind it read, so `topk` over a native-histogram metric declares the
/// histogram column and runs natively. It ranks no histogram sample - Prometheus ignores them in a
/// ranking, which `select::topk` states - so both routes answer that column empty.
///
/// A comparison between a native histogram and a float pairs no sample at all: the kernel reports
/// `(Histogram, Float)` as unsupported and skips it without keeping the left element, so its one
/// column is the empty float layout both routes answer with no rows.
///
/// The observer says which route ran - the native one while the option is on, the existing plan
/// when it is off - and the rows are compared with the existing plan's.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_kind_rules_match_legacy() {
    use api::greptime_proto::io::prometheus::write::v2::histogram::{Count, ZeroCount};
    use api::greptime_proto::io::prometheus::write::v2::{Histogram, Sample};
    use axum::body::{Body, to_bytes};
    use http::Request;
    use prost::Message;
    use servers::prom_remote_write::v2::test_util as remote_write_v2;
    use servers::prom_store::snappy_compress;
    use tower::ServiceExt;

    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_kind_rules").with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    let server = crate::test_util::build_test_prom_server(instance.clone(), false, None).build();
    let router = server.build(server.make_app()).unwrap();
    // One histogram metric and one float metric, written through the same remote-write endpoint so
    // the two tables carry the same column layout: what the existing plan's mixed-kind binary reads
    // is then the shape it was written for, and a difference in the answers is a difference in the
    // answer rather than in the fixture.
    for request in [
        remote_write_v2::request_with_labels_and_histograms(
            vec![("__name__", "kind_hist"), ("job", "api")],
            [0, 600_000]
                .into_iter()
                .map(|timestamp| Histogram {
                    timestamp,
                    count: Some(Count::CountInt(4)),
                    zero_count: Some(ZeroCount::ZeroCountInt(4)),
                    sum: 8.0,
                    ..Default::default()
                })
                .collect(),
        ),
        remote_write_v2::request_with_labels_and_samples(
            vec![("__name__", "kind_float"), ("job", "api")],
            [(0, 2.0), (600_000, 3.0)]
                .into_iter()
                .map(|(timestamp, value)| Sample {
                    timestamp,
                    value,
                    ..Default::default()
                })
                .collect(),
        ),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::post("/v1/prometheus/write")
                    .header("Content-Encoding", "snappy")
                    .header(
                        "Content-Type",
                        "application/x-protobuf;proto=io.prometheus.write.v2.Request",
                    )
                    .body(Body::from(
                        snappy_compress(&request.encode_to_vec()).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            204,
            "{:?}",
            to_bytes(response.into_body(), usize::MAX).await.unwrap()
        );
    }

    // A ranking keeps the kind it read, so the statement declares a histogram column and runs
    // natively. It ranks no histogram sample - Prometheus ignores them in a ranking, which
    // `select::topk` states - so both routes answer the empty column rather than a sample.
    let ranking = "TQL EVAL (0, 600, '600s') topk(1, kind_hist)";
    observer.clear();
    let native = native_tql_output(&instance, ranking, true).await;
    assert!(
        observer.fused().is_some(),
        "{ranking} did not run natively: {:#?}",
        observer.root_plan()
    );
    let native_ranking_rows = answer_rows(native).await;
    observer.clear();
    let legacy = native_tql_output(&instance, ranking, false).await;
    assert!(
        observer.fused().is_none(),
        "{ranking} must keep the existing plan when the native route is off"
    );
    assert_eq!(
        0, native_ranking_rows,
        "{ranking} ranks no histogram sample, as Prometheus does"
    );
    assert_eq!(
        answer_rows(legacy).await,
        native_ranking_rows,
        "{ranking} must answer the existing plan's rows"
    );

    // A comparison over two kinds pairs no sample: both routes answer no rows at all.
    let comparison = "TQL EVAL (0, 600, '600s') kind_hist > on(job) kind_float";
    observer.clear();
    let native = native_tql_output(&instance, comparison, true).await;
    assert!(
        observer.fused().is_some(),
        "{comparison} did not run natively: {:#?}",
        observer.root_plan()
    );
    let native_rows = answer_rows(native).await;
    observer.clear();
    let legacy = native_tql_output(&instance, comparison, false).await;
    assert!(
        observer.fused().is_none(),
        "{comparison} must keep the existing plan when the native route is off"
    );
    assert_eq!(
        0, native_rows,
        "{comparison} pairs no histogram with a float"
    );
    assert_eq!(
        answer_rows(legacy).await,
        native_rows,
        "{comparison} must answer the existing plan's rows"
    );
}

/// How many rows `output` answers, whatever its columns hold.
async fn answer_rows(output: Output) -> usize {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("PromQL answers record batches"),
    };
    batches.iter().map(|batch| batch.num_rows()).sum()
}

/// Two independently planned histogram frames whose reset hints contradict: the pair is dropped and
/// the warning reaches the query's collector.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_composed_histogram_reset_warning() {
    use api::greptime_proto::io::prometheus::write::v2::histogram::Count;
    use api::greptime_proto::io::prometheus::write::v2::{BucketSpan, Histogram};
    use axum::body::{Body, to_bytes};
    use common_query::promql_annotations::promql_annotation_collector;
    use http::Request;
    use prost::Message;
    use servers::prom_remote_write::v2::test_util as remote_write_v2;
    use servers::prom_store::snappy_compress;
    use tower::ServiceExt;

    common_telemetry::init_default_ut_logging();
    let (standalone, _) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_composed_histogram_reset_warning",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    let server = crate::test_util::build_test_prom_server(instance.clone(), false, None).build();
    let router = server.build(server.make_app()).unwrap();
    for (metric, count, sum, reset_hint) in [
        ("reset_hist_a", 4_u64, 8.0, 1),
        ("reset_hist_b", 2_u64, 4.0, 2),
    ] {
        let request = remote_write_v2::request_with_labels_and_histograms(
            vec![("__name__", metric), ("job", "api")],
            vec![Histogram {
                timestamp: 60_000,
                count: Some(Count::CountInt(count)),
                sum,
                schema: 1,
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 2,
                }],
                positive_deltas: vec![(count / 2) as i64, 0],
                reset_hint,
                ..Default::default()
            }],
        );
        let response = router
            .clone()
            .oneshot(
                Request::post("/v1/prometheus/write")
                    .header("Content-Encoding", "snappy")
                    .header(
                        "Content-Type",
                        "application/x-protobuf;proto=io.prometheus.write.v2.Request",
                    )
                    .body(Body::from(
                        snappy_compress(&request.encode_to_vec()).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        assert_eq!(
            status,
            204,
            "{:?}",
            to_bytes(response.into_body(), usize::MAX).await.unwrap()
        );
    }
    let query = PromQuery {
        query: "reset_hist_a + on(job) reset_hist_b".to_string(),
        start: "60".to_string(),
        end: "60".to_string(),
        step: "60s".to_string(),
        ..PromQuery::default()
    };
    let ctx = native_query_context(true, 2);
    let statement = QueryLanguageParser::parse_promql(&query, &ctx).unwrap();
    let logical = instance
        .query_engine()
        .planner()
        .plan(&statement, Arc::clone(&ctx))
        .await
        .unwrap();
    let display = logical.display_indent().to_string();
    let datafusion::logical_expr::LogicalPlan::Extension(extension) = &logical else {
        panic!("{display}")
    };
    let binary = extension
        .node
        .as_any()
        .downcast_ref::<promql::extension_plan::FusedSeries>()
        .unwrap_or_else(|| panic!("{display}"));
    let promql::native::plan::NativeExpr::Binary { left, right, .. } = &binary.tree().root().expr
    else {
        panic!("{display}")
    };
    assert!(
        matches!(left.expr, promql::native::plan::NativeExpr::Input(_))
            && matches!(right.expr, promql::native::plan::NativeExpr::Input(_)),
        "{display}"
    );
    assert_eq!(binary.children().len(), 2, "{display}");
    assert!(!display.contains("Join:"), "{display}");

    // Hold a strong reference to the query-scoped collector until the output stream has finished.
    let collector = promql_annotation_collector(ctx.remote_query_id().unwrap());
    let output = instance
        .do_promql_query(&query, ctx)
        .await
        .remove(0)
        .unwrap();
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!(),
    };
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        0
    );
    let mut warnings = Vec::new();
    let mut infos = Vec::new();
    collector.append_to(&mut warnings, &mut infos);
    assert_eq!(
        warnings,
        vec!["prom_native_histogram_add: native histogram counter reset hints contradict"]
    );
    assert!(infos.is_empty());
}

/// A composed native binary answers grouped matches and multi-selector operands as one fused node
/// over two evaluated frames, and fills a missing side the way its modifier says.
///
/// The rows are written out rather than taken from the existing route: these pairs mix a native
/// histogram with a float, which is the shape the composed route exists for. What they pin is that
/// a grouped match reports the many side's labels with the declared ones copied in from the one
/// side (`group_left` keeps the left side's labels, `group_right` the right side's and drops the
/// label the one side does not carry), and that the arithmetic is the modifier's: `histogram * k`
/// scales count and sum, and a missing right side filled with zero scales them to zero. A filled
/// step is filed at the match group of the series that has the sample, so a `group_right` fill
/// answers at the one side's labels - `job` below, which the many side never carries - and the
/// answer has to declare them for the rows to keep them. A grouped `include` label is the other
/// way round: it is copied from the one side, which removes it from the many side's labels when it
/// does not have it, so a label only the many side carries is on no row and the answer must not
/// declare it either.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_composed_grouped_and_multi_selector_operands() {
    use api::greptime_proto::io::prometheus::write::v2::Histogram;
    use api::greptime_proto::io::prometheus::write::v2::histogram::{Count, ZeroCount};
    use axum::body::{Body, to_bytes};
    use http::Request;
    use prost::Message;
    use servers::prom_remote_write::v2::test_util as remote_write_v2;
    use servers::prom_store::snappy_compress;
    use tower::ServiceExt;

    common_telemetry::init_default_ut_logging();
    let (standalone, _) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_composed_grouped_operands",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE grouped_float (job STRING, host STRING, unit STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(job, host, unit));
         INSERT INTO grouped_float VALUES ('api','a','x',0,2),('api','a','x',600000,3);
         CREATE TABLE grouped_gap (job STRING, host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(job, host));
         INSERT INTO grouped_gap VALUES ('api','a',600000,4);
         CREATE TABLE grouped_host (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host));
         INSERT INTO grouped_host VALUES ('a',600000,4)",
        QueryContext::arc(),
    )
    .await;
    // The histogram side is one series, labeled `job="api"`, sampled at the two steps: four counts
    // and a sum of eight.
    let server = crate::test_util::build_test_prom_server(instance.clone(), false, None).build();
    let router = server.build(server.make_app()).unwrap();
    let request = remote_write_v2::request_with_labels_and_histograms(
        vec![("__name__", "grouped_hist"), ("job", "api")],
        [0, 600_000]
            .into_iter()
            .map(|timestamp| Histogram {
                timestamp,
                count: Some(Count::CountInt(4)),
                zero_count: Some(ZeroCount::ZeroCountInt(4)),
                sum: 8.0,
                ..Default::default()
            })
            .collect(),
    );
    let response = router
        .oneshot(
            Request::post("/v1/prometheus/write")
                .header("Content-Encoding", "snappy")
                .header(
                    "Content-Type",
                    "application/x-protobuf;proto=io.prometheus.write.v2.Request",
                )
                .body(Body::from(
                    snappy_compress(&request.encode_to_vec()).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        204,
        "{:?}",
        to_bytes(response.into_body(), usize::MAX).await.unwrap()
    );

    for (expression, expected, undeclared) in [
        // `group_left(host)` reports the left (many) side's labels and copies `host` in from the one
        // side, which carries it.
        (
            "grouped_hist * on(job) group_left(host) grouped_float",
            vec![
                (
                    0,
                    vec!["host=a".to_string(), "job=api".to_string()],
                    8.0,
                    16.0,
                ),
                (
                    600_000,
                    vec!["host=a".to_string(), "job=api".to_string()],
                    12.0,
                    24.0,
                ),
            ],
            &[][..],
        ),
        // `group_right(host)` reports the right (many) side's labels: `host` is dropped because the
        // one side does not carry it, and `unit` survives because a grouping does not narrow.
        (
            "grouped_hist * on(job) group_right(host) grouped_float",
            vec![
                (
                    0,
                    vec!["job=api".to_string(), "unit=x".to_string()],
                    8.0,
                    16.0,
                ),
                (
                    600_000,
                    vec!["job=api".to_string(), "unit=x".to_string()],
                    12.0,
                    24.0,
                ),
            ],
            // The one side never carries `host`, and `on(job)` keeps it out of the key a fill reads.
            &["host"][..],
        ),
        // A one-to-one match narrows the labels to what it matches on.
        (
            "grouped_hist * on(job) grouped_float",
            vec![
                (0, vec!["job=api".to_string()], 8.0, 16.0),
                (600_000, vec!["job=api".to_string()], 12.0, 24.0),
            ],
            &[][..],
        ),
        // `group_right` fills a missing many side from the one side's series, at that series' match
        // group label set. `grouped_host` carries no `job`, so no step pairs and every step is
        // filled with zero at the one side's `job` - a label the many side never carries, and one
        // the answer therefore has to declare.
        (
            "grouped_hist * on(job) group_right fill_right(0) grouped_host",
            vec![
                (0, vec!["job=api".to_string()], 0.0, 0.0),
                (600_000, vec!["job=api".to_string()], 0.0, 0.0),
            ],
            &[][..],
        ),
        // `group_left(host)` copies `host` from the one side, and the one side here is the histogram
        // series, which carries no `host`: the many side's own `host` never reaches a row, so the
        // answer declares neither it nor a column nothing fills. `unit` is not an include label and
        // survives, as a grouping does not narrow.
        (
            "grouped_float * on(job) group_left(host) grouped_hist",
            vec![
                (
                    0,
                    vec!["job=api".to_string(), "unit=x".to_string()],
                    8.0,
                    16.0,
                ),
                (
                    600_000,
                    vec!["job=api".to_string(), "unit=x".to_string()],
                    12.0,
                    24.0,
                ),
            ],
            // The one side never carries `host`, and `on(job)` keeps it out of the key a fill reads.
            &["host"][..],
        ),
        // An operand that reads more than one selector still evaluates on its own: the right side
        // is `2 + 2` at the first step and `3 + 3` at the second, so the histogram is scaled by 4
        // and then by 6.
        (
            "grouped_hist * on(job) (grouped_float + on(job, host) grouped_float)",
            vec![
                (0, vec!["job=api".to_string()], 16.0, 32.0),
                (600_000, vec!["job=api".to_string()], 24.0, 48.0),
            ],
            &[][..],
        ),
    ] {
        let query = PromQuery {
            query: expression.to_string(),
            start: "0".to_string(),
            end: "600".to_string(),
            step: "600s".to_string(),
            ..PromQuery::default()
        };
        // The composed plan itself: one fused binary over two evaluated operands, with no join
        // between them.
        let ctx = native_query_context(true, 4);
        let statement = QueryLanguageParser::parse_promql(&query, &ctx).unwrap();
        let logical = instance
            .query_engine()
            .planner()
            .plan(&statement, ctx)
            .await
            .unwrap();
        let display = logical.display_indent().to_string();
        let datafusion::logical_expr::LogicalPlan::Extension(extension) = &logical else {
            panic!("{expression}: {display}")
        };
        let binary = extension
            .node
            .as_any()
            .downcast_ref::<promql::extension_plan::FusedSeries>()
            .unwrap_or_else(|| panic!("{expression}: {display}"));
        assert!(
            matches!(
                binary.tree().root().expr,
                promql::native::plan::NativeExpr::Binary { .. }
            ),
            "{expression}: {display}"
        );
        assert_eq!(binary.children().len(), 2, "{expression}: {display}");
        assert!(
            display.contains("output: series_frame") && !display.contains("Join:"),
            "{expression}: {display}"
        );

        let rows = histogram_label_rows(
            instance
                .do_promql_query(&query, native_query_context(true, 4))
                .await
                .remove(0)
                .unwrap(),
        )
        .await;
        // Every label an answer carries is one the plan declares: a filled step answers at the one
        // side's match group, which is a label set the many side never carries.
        for (_, labels, _, _) in &rows {
            for label in labels {
                let name = label.split('=').next().unwrap();
                assert!(
                    logical
                        .schema()
                        .fields()
                        .iter()
                        .any(|field| field.name().as_str() == name),
                    "{expression}: `{name}` is carried but not declared: {display}"
                );
            }
        }
        // And no column is declared that no row can fill: a grouped `include` label is on a row only
        // when the one side carries it or the matching keeps it in the key a fill reads.
        for name in undeclared {
            assert!(
                !logical
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| field.name().as_str() == *name),
                "{expression}: `{name}` is declared but no row can carry it: {display}"
            );
        }
        assert_eq!(expected, rows, "{expression}");
    }

    // `fill_right` answers the step whose right side is missing: a histogram scaled by zero keeps
    // the label set of the many side and answers zero count and sum.
    let query = PromQuery {
        query: "grouped_hist * on(job) fill_right(0) grouped_gap".to_string(),
        start: "0".to_string(),
        end: "600".to_string(),
        step: "600s".to_string(),
        ..PromQuery::default()
    };
    let filled = histogram_label_rows(
        instance
            .do_promql_query(&query, native_query_context(true, 4))
            .await
            .remove(0)
            .unwrap(),
    )
    .await;
    assert_eq!(
        vec![
            (0, vec!["job=api".to_string()], 0.0, 0.0),
            (600_000, vec!["job=api".to_string()], 16.0, 32.0),
        ],
        filled,
        "the missing right side is filled with zero"
    );
}

/// The rows of a histogram-answering PromQL result, read by column kind rather than by name.
///
/// The two routes name their value column differently - the compiled field of a native plan against
/// the legacy expression text - and order their columns differently, so each row is flattened to
/// its timestamp, its labels as `name=value`, and the histogram's count and sum.
async fn histogram_label_rows(output: Output) -> Vec<(i64, Vec<String>, f64, f64)> {
    use common_query::native_histogram::read_histogram;
    use datatypes::arrow::array::StructArray;

    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("PromQL answers record batches"),
    };
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let (time_index, _) = batch
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| matches!(column.data_type(), DataType::Timestamp(..)))
            .expect("a PromQL result carries its time index");
        let time = cast(batch.column(time_index).as_ref(), &DataType::Int64).unwrap();
        let time = time.as_any().downcast_ref::<Int64Array>().unwrap();
        let names = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let histogram = batch
            .columns()
            .iter()
            .find_map(|column| column.as_any().downcast_ref::<StructArray>())
            .expect("a histogram result carries a histogram column");
        for row in 0..batch.num_rows() {
            let mut labels = Vec::new();
            for (index, column) in batch.columns().iter().enumerate() {
                if !matches!(
                    column.data_type(),
                    DataType::Utf8 | DataType::Dictionary(_, _)
                ) {
                    continue;
                }
                let text = cast(column.as_ref(), &DataType::Utf8).unwrap();
                let text = text.as_any().downcast_ref::<StringArray>().unwrap();
                if !text.is_null(row) && !text.value(row).is_empty() {
                    labels.push(format!("{}={}", names[index], text.value(row)));
                }
            }
            labels.sort();
            let histogram = read_histogram(histogram, row).unwrap().unwrap();
            rows.push((time.value(row), labels, histogram.count, histogram.sum));
        }
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows
}

/// The rows of the repeated-selector fixture, inserted into the one logical table
/// `{prefix}_b`. Both hosts are counters sampled every minute for ten minutes, longer than the
/// widest window the test reads, so a 5m window and a 10m window over the same series cover
/// different sample sets.
fn repeated_selector_fixture_inserts(prefix: &str) -> String {
    let host = |host: char, first: usize| {
        (0..10)
            .map(|step| format!("('{host}', {}.0, {})", first + step, (step + 1) * 60_000))
            .collect::<Vec<_>>()
    };
    format!(
        "INSERT INTO {prefix}_b (host, val, ts) VALUES {}",
        host('x', 1)
            .into_iter()
            .chain(host('y', 2))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Real two-datanode TCP acceptance for a repeated selector over ONE partitioned metric table.
/// Every prepared scan becomes its own raw source with its own time bounds, so two occurrences of
/// the same fragmented table - a ratio of two windows, or a binary expression over two identical
/// ones - stay independent instead of falling back. Over the wire the transport carries those
/// sources; in process they keep the node out of the region, which cannot serve one node over both
/// scans of the one table.
#[tokio::test(flavor = "multi_thread")]
async fn distributed_native_promql_repeated_selector_over_partitioned_table() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("distributed_native_promql_repeated")
        .await
        .with_datanodes(2)
        .with_real_datanode_grpc_addr(true)
        .with_frontend_real_datanode_grpc(true)
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .build(false)
        .await;
    let instance = cluster.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE dist_repeat_phy (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) PARTITION ON COLUMNS (ts) (ts < '1970-01-01 00:02:00', ts >= '1970-01-01 00:02:00' AND ts < '1970-01-01 00:04:00', ts >= '1970-01-01 00:04:00') ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        "CREATE TABLE dist_repeat_b (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='dist_repeat_phy')",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        &repeated_selector_fixture_inserts("dist_repeat"),
        QueryContext::arc(),
    )
    .await;

    // The semantic reference: the same rows in one unpartitioned physical table. The engine's own
    // evaluation over the complete series is the oracle, so a plan that reconciled the fragments
    // wrongly cannot agree with it.
    execute_all(
        &instance,
        "CREATE TABLE dist_repeat_ctl_phy (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        "CREATE TABLE dist_repeat_ctl_b (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='dist_repeat_ctl_phy')",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        &repeated_selector_fixture_inserts("dist_repeat_ctl"),
        QueryContext::arc(),
    )
    .await;

    for expression in [
        // The two occurrences read different windows of the one table, so a plan that bound both to
        // one window would answer 1 at every step.
        "increase(dist_repeat_b[5m]) / increase(dist_repeat_b[10m])",
        // Two identical occurrences, which the per-series match must still keep apart.
        "increase(dist_repeat_b[5m]) + increase(dist_repeat_b[5m])",
    ] {
        let query = PromQuery {
            query: expression.to_string(),
            start: "480".to_string(),
            end: "600".to_string(),
            step: "60s".to_string(),
            ..PromQuery::default()
        };
        let control = expression.replace("dist_repeat_", "dist_repeat_ctl_");
        let control_rows = native_result_rows(
            instance
                .do_promql_query(
                    &PromQuery {
                        query: control.clone(),
                        ..query.clone()
                    },
                    native_query_context(false, 2),
                )
                .await
                .remove(0)
                .unwrap(),
        )
        .await;
        assert_eq!(
            control_rows.len(),
            6,
            "two hosts over three steps define every row: {control}"
        );

        cluster.reset_network_do_get_peak();
        let output = instance
            .do_promql_query(&query, native_query_context(true, 2))
            .await
            .remove(0)
            .unwrap();
        let plan = output.meta.plan.as_ref().expect("frontend physical plan");
        let shape = native_plan_shape(plan);
        assert!(
            shape.fused >= 1,
            "a native plan keeps at least one fused producer: {expression}: {plan:?}"
        );
        assert_eq!(
            shape.regions_per_scan,
            vec![3, 3],
            "one raw source per occurrence, each read through every time partition: \
             {expression}: {plan:?}"
        );
        assert!(
            shape.routers_below_fused >= shape.regions_per_scan.len(),
            "every occurrence is a series split by time, so each one's fragments are reconciled \
             before the fused node reads them: {expression}: {plan:?}"
        );
        assert!(
            shape
                .scan_orderings
                .iter()
                .all(|ordering| ordering.as_deref() == Some("interleaved")),
            "the frontend cannot prove a series stays in one time partition, so neither scan may \
             claim contiguous per-series order: {expression}: {:?}: {plan:?}",
            shape.scan_orderings
        );
        let rows = native_result_rows(output).await;
        assert_eq!(
            cluster.network_do_get_active(),
            0,
            "every response body is finished: {expression}"
        );
        let peak = cluster.network_do_get_peak_active();
        assert!(
            (1..=2).contains(&peak),
            "two occurrences still read within the target-partition bound: {expression}: {peak}"
        );
        if expression.contains(" / ") {
            assert!(
                rows.iter().all(|row| (row.2 - 1.0).abs() > 1e-9),
                "a 5m window over a 10m one is not 1, so both occurrences were read: \
                 {expression}: {rows:?}"
            );
        }
        assert_eq!(control_rows, rows, "{expression}");
    }
}

/// The rows a collapsing rewrite joins into one series without a collision: the two stored series
/// are ten minutes apart, twice the five-minute lookback, so exactly one of them holds a value at
/// each evaluation point and the label set they share carries both points.
fn canonical_merge_fixture_inserts(prefix: &str) -> String {
    format!(
        "INSERT INTO {prefix}_b (host, val, ts) VALUES ('first', 10, 0), ('second', 20, 600000)"
    )
}

/// The rows a collapsing rewrite can only reject: the second series is two minutes after the first,
/// inside the lookback, so both hold a value at the same evaluation point while the declared partition
/// column stores the two hosts under values that place them in different regions.
fn canonical_collision_fixture_inserts(prefix: &str) -> String {
    format!("INSERT INTO {prefix}_c (host, val, ts) VALUES ('early', 10, 0), ('late', 20, 120000)")
}

/// Real two-datanode TCP acceptance for a canonical merge over a tag-partitioned metric table.
///
/// A rewrite that writes a label its input carries can give two stored series one label set. The two
/// series are stored under different values of the declared partition column, so that partition puts
/// the pieces of the one label set in different regions: no region holds the set whole. A
/// region-local producer merges only the pieces its own region holds, so the boundary has to be
/// placed above the regions: the frontend runs the producer over the reconciled raw sources, and the
/// canonical router it inserts above itself collects each label set from every region.
///
/// The test states that in the three ways it can be observed: the values (against the same rows in
/// one unpartitioned table), the plan (the producer is a frontend node reading a merge scan, with its
/// canonical router above it), and the error (a collision two different regions hold is reported,
/// which no single region could see).
#[tokio::test(flavor = "multi_thread")]
async fn distributed_native_promql_canonical_merge_over_partitioned_table() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("distributed_native_promql_canonical")
        .await
        .with_datanodes(2)
        .with_real_datanode_grpc_addr(true)
        .with_frontend_real_datanode_grpc(true)
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .build(false)
        .await;
    let instance = cluster.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE dist_native_canon_phy (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) PARTITION ON COLUMNS (host) (host < 'first', host >= 'first' AND host < 'second', host >= 'second') ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    for table in ["dist_native_canon_b", "dist_native_canon_c"] {
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {table} (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='dist_native_canon_phy')"
            ),
            QueryContext::arc(),
        )
        .await;
    }
    execute_all(
        &instance,
        &canonical_merge_fixture_inserts("dist_native_canon"),
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        &canonical_collision_fixture_inserts("dist_native_canon"),
        QueryContext::arc(),
    )
    .await;

    // The semantic reference: the same rows in one unpartitioned physical table, where one region
    // holds every piece of a label set. Its evaluation is what the partitioned plan has to reproduce.
    execute_all(
        &instance,
        "CREATE TABLE dist_native_canon_ctl_phy (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    for table in ["dist_native_canon_ctl_b", "dist_native_canon_ctl_c"] {
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {table} (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='dist_native_canon_ctl_phy')"
            ),
            QueryContext::arc(),
        )
        .await;
    }
    execute_all(
        &instance,
        &canonical_merge_fixture_inserts("dist_native_canon_ctl"),
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        &canonical_collision_fixture_inserts("dist_native_canon_ctl"),
        QueryContext::arc(),
    )
    .await;

    // Two stored series rewritten to one label set, holding their values at different evaluation
    // points, are one series carrying both points - and the hosts they are stored under land in
    // different host ranges, so the two pieces are in different regions.
    let expression = r#"label_replace(dist_native_canon_b, "host", "same", "host", ".*")"#;
    let query = PromQuery {
        query: expression.to_string(),
        start: "0".to_string(),
        end: "600".to_string(),
        step: "600s".to_string(),
        ..PromQuery::default()
    };
    let expected = vec![
        (0, "same".to_string(), 10.0),
        (600_000, "same".to_string(), 20.0),
    ];
    let control = expression.replace("dist_native_canon_", "dist_native_canon_ctl_");
    for (route, native) in [("native", true), ("existing", false)] {
        let control_rows = native_result_rows(
            instance
                .do_promql_query(
                    &PromQuery {
                        query: control.clone(),
                        ..query.clone()
                    },
                    native_query_context(native, 2),
                )
                .await
                .remove(0)
                .unwrap(),
        )
        .await;
        assert_eq!(
            control_rows, expected,
            "the control over one region defines every row: {route}: {control}"
        );
    }

    let output = instance
        .do_promql_query(&query, native_query_context(true, 2))
        .await
        .remove(0)
        .unwrap();
    let plan = output.meta.plan.as_ref().expect("frontend physical plan");
    let shape = native_plan_shape(plan);
    assert_eq!(
        shape.regions_per_scan,
        vec![3],
        "the selector reads all three host ranges: {expression}: {plan:?}"
    );
    // Declaring a partition on a primary-key column that is not the time index proves the regions
    // hold disjoint series, so the merge scan publishes them whole and the boundary above the
    // regions only ever sees complete stored series.
    assert!(
        shape
            .scan_orderings
            .iter()
            .all(|ordering| ordering.as_deref() == Some("contiguous")),
        "a declared partition on a primary-key column that excludes the time index proves the \
         regions hold disjoint series (physical_regions_disjoint), so the merge scan publishes \
         whole series, not fragments: {expression}: {:?}: {plan:?}",
        shape.scan_orderings
    );
    // The boundary's two halves are both frontend nodes: the producer merges what each execution
    // partition holds and the consumer above it publishes the routed frames. A producer planned
    // inside a region would leave this tree with one fused node and no canonical producer in it.
    assert_eq!(
        shape.fused, 2,
        "the canonical producer and the consumer above it run on the frontend: {expression}: \
         {plan:?}"
    );
    let producer = native_plan_nodes(plan)
        .into_iter()
        .find(|node| promql::extension_plan::is_canonical_producer_exec(node.as_ref()))
        .unwrap_or_else(|| {
            panic!(
                "the canonical producer must be a frontend node above the regions, not a region's \
                 own producer: {expression}: {plan:?}"
            )
        });
    // What the producer reads is every region's own stored series, each already whole: one merge
    // scan over all three regions, with no raw-key repartition below it.
    let below_producer = native_plan_nodes(&producer);
    assert!(
        below_producer
            .iter()
            .any(|node| node.downcast_ref::<MergeScanExec>().is_some()),
        "the producer reads the regions through a merge scan: {expression}: {plan:?}"
    );
    assert!(
        below_producer
            .iter()
            .all(|node| node.name() != "SeriesRepartitionExec"),
        "no raw-key repartition sits below the producer: each input partition already holds whole \
         stored series, since the partition covers the primary key and only the label set crosses \
         regions: {expression}: {plan:?}"
    );
    // The router is the boundary's middle stage: it sits above the producer and collects each label
    // set from every partition the producer ran on, which is what a region-local boundary cannot do.
    let router = native_plan_nodes(plan)
        .into_iter()
        .find(|node| {
            node.name() == "SeriesRepartitionExec"
                && node
                    .children()
                    .iter()
                    .any(|child| promql::extension_plan::is_canonical_producer_exec(child.as_ref()))
        })
        .unwrap_or_else(|| {
            panic!(
                "the canonical router must read the producer above the regions: {expression}: \
                 {plan:?}"
            )
        });
    assert_eq!(
        router.children()[0].name(),
        "FusedSeriesExec",
        "the router reads the canonical producer: {expression}: {plan:?}"
    );
    assert_eq!(
        router.properties().output_partitioning().partition_count(),
        producer
            .properties()
            .output_partitioning()
            .partition_count(),
        "the router keeps the producer's width instead of gathering every piece: {expression}: \
         {plan:?}"
    );
    let rows = native_result_rows(output).await;
    assert_eq!(
        rows, expected,
        "the partitioned plan answers the control's rows: {expression}"
    );

    // A collision two regions hold is the error Prometheus reports for two metrics with one label
    // set. A region-local producer cannot see it: each region would hold one piece and publish it,
    // and the answer would be two series under one label set. The control over one region reports it
    // as well, which is the answer the partitioned plan owes.
    let collision = r#"label_replace(dist_native_canon_c, "host", "same", "host", ".*")"#;
    let collision_query = PromQuery {
        query: collision.to_string(),
        start: "120".to_string(),
        end: "120".to_string(),
        step: "60s".to_string(),
        ..PromQuery::default()
    };
    let collision_control = collision.replace("dist_native_canon_", "dist_native_canon_ctl_");
    let control_error = instance
        .do_promql_query(
            &PromQuery {
                query: collision_control.clone(),
                ..collision_query.clone()
            },
            native_query_context(true, 2),
        )
        .await
        .remove(0);
    let control_error = match control_error {
        Ok(output) => collect_error(output).await,
        Err(error) => error.to_string(),
    };
    assert!(
        control_error.contains("cannot contain metrics with the same labelset"),
        "the control over one region reports the overlap: {collision_control}: {control_error}"
    );
    let partitioned = instance
        .do_promql_query(&collision_query, native_query_context(true, 2))
        .await
        .remove(0);
    let partitioned_error = match partitioned {
        Ok(output) => collect_error(output).await,
        Err(error) => error.to_string(),
    };
    assert!(
        partitioned_error.contains("cannot contain metrics with the same labelset"),
        "a label set whose pieces two regions hold is collected by the router and rejected, not \
         published twice: {collision}: {partitioned_error}"
    );
}

/// The rows of the disjoint fixture, inserted into the logical tables `{prefix}_a` and
/// `{prefix}_b`. Both hosts carry the same shape and land in different partitions.
fn disjoint_fixture_inserts(prefix: &str) -> String {
    format!(
        "INSERT INTO {prefix}_a (host, val, ts) VALUES ('a', 2, 60000), ('a', 4, 120000), ('a', 6, 180000), ('a', 8, 240000), ('z', 4, 60000), ('z', 6, 120000), ('z', 8, 180000), ('z', 10, 240000); INSERT INTO {prefix}_b (host, val, ts) VALUES ('a', 1, 60000), ('a', 2, 120000), ('a', 3, 180000), ('a', 4, 240000), ('z', 2, 60000), ('z', 3, 120000), ('z', 4, 180000), ('z', 5, 240000)"
    )
}

/// Real two-datanode TCP acceptance for raw series that no region shares: partitioning on the raw
/// primary key puts every series in exactly one region, so the native plan must read all of them
/// and reconcile none of them.
#[tokio::test(flavor = "multi_thread")]
async fn distributed_native_promql_disjoint_regions_do_not_repartition() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("distributed_native_promql_disjoint")
        .await
        .with_datanodes(2)
        .with_real_datanode_grpc_addr(true)
        .with_frontend_real_datanode_grpc(true)
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .build(false)
        .await;
    let instance = cluster.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE dist_native_disjoint_phy (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) PARTITION ON COLUMNS (host) (host < 'm', host >= 'm') ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    for table in ["dist_native_disjoint_a", "dist_native_disjoint_b"] {
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {table} (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='dist_native_disjoint_phy')"
            ),
            QueryContext::arc(),
        )
        .await;
    }
    execute_all(
        &instance,
        &disjoint_fixture_inserts("dist_native_disjoint"),
        QueryContext::arc(),
    )
    .await;

    // The semantic reference over the complete series, as in the fragmented case: the engine's own
    // evaluation of the unpartitioned fixture defines the expected values.
    execute_all(
        &instance,
        "CREATE TABLE dist_native_disjoint_ctl_phy (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    for table in ["dist_native_disjoint_ctl_a", "dist_native_disjoint_ctl_b"] {
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {table} (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='dist_native_disjoint_ctl_phy')"
            ),
            QueryContext::arc(),
        )
        .await;
    }
    execute_all(
        &instance,
        &disjoint_fixture_inserts("dist_native_disjoint_ctl"),
        QueryContext::arc(),
    )
    .await;

    for (expression, expected, expected_scans) in [
        (
            "sum(increase(dist_native_disjoint_a[5m])) / sum(increase(dist_native_disjoint_b[5m]))",
            vec![(180_000, 2.0), (240_000, 2.0)],
            2,
        ),
        (
            "sum(increase(dist_native_disjoint_a[5m]))",
            vec![(180_000, 10.0), (240_000, 14.0)],
            1,
        ),
    ] {
        let query = PromQuery {
            query: expression.to_string(),
            start: "180".to_string(),
            end: "240".to_string(),
            step: "60s".to_string(),
            ..PromQuery::default()
        };
        let control = expression.replace("dist_native_disjoint_", "dist_native_disjoint_ctl_");
        let control_rows = native_scalar_rows(
            instance
                .do_promql_query(
                    &PromQuery {
                        query: control.clone(),
                        ..query.clone()
                    },
                    native_query_context(false, 2),
                )
                .await
                .remove(0)
                .unwrap(),
        )
        .await;
        assert_eq!(
            control_rows.len(),
            expected.len(),
            "the control defines every row: {control}"
        );
        for (actual, expected) in control_rows.iter().zip(expected.iter()) {
            assert_eq!(actual.0, expected.0, "control row timestamp: {control}");
            assert!(
                (actual.1 - expected.1).abs() < 1e-9,
                "control row value: {control}: {control_rows:?}"
            );
        }

        cluster.reset_network_do_get_peak();
        let output = instance
            .do_promql_query(&query, native_query_context(true, 2))
            .await
            .remove(0)
            .unwrap();
        let plan = output.meta.plan.as_ref().expect("frontend physical plan");
        // A global fold normalizes every input to one partition, so the gather it needs is the
        // frames of complete series moving, not fragments of one series being put back together.
        assert_native_disjoint_shape(plan, expected_scans, 2);
        let rows = native_scalar_rows(output).await;
        assert_eq!(
            cluster.network_do_get_active(),
            0,
            "every response body is finished: {expression}"
        );
        let peak = cluster.network_do_get_peak_active();
        assert!(
            (1..=2).contains(&peak),
            "disjoint region streams stay within the target-partition bound: {expression}: {peak}"
        );
        assert_eq!(rows.len(), expected.len(), "all rows: {expression}");
        for (actual, expected) in rows.iter().zip(expected) {
            assert_eq!(actual.0, expected.0, "{expression}");
            assert!(
                (actual.1 - expected.1).abs() < 1e-9,
                "{expression}: {rows:?}"
            );
        }
    }

    // A per-series operation reads the two tables host by host: no series crosses a region, so
    // nothing in the native plan has to put one back together. A global fold above would gather
    // complete series instead, which is why this case is the one that states the disjoint claim.
    let expression = "increase(dist_native_disjoint_a[5m]) / increase(dist_native_disjoint_b[5m])";
    let query = PromQuery {
        query: expression.to_string(),
        start: "180".to_string(),
        end: "240".to_string(),
        step: "60s".to_string(),
        ..PromQuery::default()
    };
    let expected = vec![
        (180_000, "a".to_string(), 2.0),
        (180_000, "z".to_string(), 2.0),
        (240_000, "a".to_string(), 2.0),
        (240_000, "z".to_string(), 2.0),
    ];
    let control = expression.replace("dist_native_disjoint_", "dist_native_disjoint_ctl_");
    let control_rows = native_result_rows(
        instance
            .do_promql_query(
                &PromQuery {
                    query: control.clone(),
                    ..query.clone()
                },
                native_query_context(false, 2),
            )
            .await
            .remove(0)
            .unwrap(),
    )
    .await;
    assert_eq!(
        expected, control_rows,
        "the control over the complete series defines every row: {control}"
    );

    cluster.reset_network_do_get_peak();
    let output = instance
        .do_promql_query(&query, native_query_context(true, 2))
        .await
        .remove(0)
        .unwrap();
    let plan = output.meta.plan.as_ref().expect("frontend physical plan");
    let shape = assert_native_disjoint_shape(plan, 2, 2);
    assert!(
        shape
            .scan_orderings
            .iter()
            .all(|ordering| ordering.as_deref() == Some("contiguous")),
        "the frontend proved each series lives in one region, so every merge scan keeps its \
         declared contiguous per-series order: {:?}: {plan:?}",
        shape.scan_orderings
    );
    // The fused node emits one partition, so each side of the match is gathered to it. That moves
    // complete series, not fragments of one: no series crosses a region here.
    assert_eq!(
        shape.routers_below_fused, 2,
        "one gather per side of the match below the fused node: {plan:?}"
    );
    let rows = native_result_rows(output).await;
    assert_eq!(
        cluster.network_do_get_active(),
        0,
        "every response body is finished: {expression}"
    );
    let peak = cluster.network_do_get_peak_active();
    assert!(
        (1..=2).contains(&peak),
        "disjoint region streams stay within the target-partition bound: {expression}: {peak}"
    );
    assert_eq!(expected, rows, "{expression}");

    // The same claim for a per-series operation with nothing to match: each series is complete in
    // the region that holds it, so the fused node reads the merge scans directly and no router sits
    // between them.
    let expression = "increase(dist_native_disjoint_a[5m])";
    let query = PromQuery {
        query: expression.to_string(),
        start: "180".to_string(),
        end: "240".to_string(),
        step: "60s".to_string(),
        ..PromQuery::default()
    };
    let control = expression.replace("dist_native_disjoint_", "dist_native_disjoint_ctl_");
    let control_rows = native_result_rows(
        instance
            .do_promql_query(
                &PromQuery {
                    query: control.clone(),
                    ..query.clone()
                },
                native_query_context(false, 2),
            )
            .await
            .remove(0)
            .unwrap(),
    )
    .await;
    assert_eq!(
        control_rows.len(),
        4,
        "the control over the complete series defines every row: {control}"
    );

    cluster.reset_network_do_get_peak();
    let output = instance
        .do_promql_query(&query, native_query_context(true, 2))
        .await
        .remove(0)
        .unwrap();
    let plan = output.meta.plan.as_ref().expect("frontend physical plan");
    let shape = assert_native_disjoint_shape(plan, 1, 2);
    assert!(
        shape
            .scan_orderings
            .iter()
            .all(|ordering| ordering.as_deref() == Some("contiguous")),
        "the frontend proved each series lives in one region, so the merge scan keeps its declared \
         contiguous per-series order: {:?}: {plan:?}",
        shape.scan_orderings
    );
    assert_eq!(
        shape.routers_below_fused, 0,
        "each series is complete in one region, so the fused node reads the merge scans directly \
         and nothing has to reconcile fragments of one: {plan:?}"
    );
    let rows = native_result_rows(output).await;
    assert_eq!(
        cluster.network_do_get_active(),
        0,
        "every response body is finished: {expression}"
    );
    let peak = cluster.network_do_get_peak_active();
    assert!(
        (1..=2).contains(&peak),
        "disjoint region streams stay within the target-partition bound: {expression}: {peak}"
    );
    assert_eq!(control_rows, rows, "{expression}");
}

/// A zero-match selector and a partitioned metric meet at one frontend set consumer.
#[tokio::test(flavor = "multi_thread")]
async fn distributed_native_promql_zero_match_or_real() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("distributed_native_promql_zero_match")
        .await
        .with_datanodes(2)
        .with_real_datanode_grpc_addr(true)
        .with_frontend_real_datanode_grpc(true)
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .build(false)
        .await;
    let instance = cluster.fe_instance().clone();
    execute_all(
        &instance,
        r#"
        CREATE TABLE zero_match_phy (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host))
            PARTITION ON COLUMNS (host) (host < 'm', host >= 'm')
            ENGINE=metric WITH ('physical_metric_table'='true');
        CREATE TABLE alpha_total (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host))
            ENGINE=metric WITH ('on_physical_table'='zero_match_phy');
        INSERT INTO alpha_total (host, ts, val) VALUES
            ('a', 0, 1), ('a', 1000, 2), ('a', 2000, 3),
            ('z', 0, 10), ('z', 1000, 20), ('z', 2000, 30);
        "#,
        QueryContext::arc(),
    )
    .await;

    let expected = [
        (0, "a", 1.0),
        (1000, "a", 2.0),
        (2000, "a", 3.0),
        (0, "z", 10.0),
        (1000, "z", 20.0),
        (2000, "z", 30.0),
    ];
    for (expression, empty_index) in [
        (r#"{__name__=~"no_such_metric_.+"} or alpha_total"#, 0),
        (r#"alpha_total or {__name__=~"no_such_metric_.+"}"#, 1),
    ] {
        let output = instance
            .do_promql_query(
                &PromQuery {
                    query: expression.to_string(),
                    start: "0".to_string(),
                    end: "2".to_string(),
                    step: "1s".to_string(),
                    ..PromQuery::default()
                },
                native_query_context(true, 2),
            )
            .await
            .remove(0)
            .unwrap();
        let plan = output.meta.plan.as_ref().expect("frontend physical plan");
        let fused = find_plan_node(plan, "FusedSeriesExec").expect("frontend set consumer");
        let display = format!(
            "{}",
            datafusion::physical_plan::displayable(fused.as_ref()).one_line()
        );
        // The node draws the tree it runs, one line per node: no query string, no path prefix and
        // no `segment` wording, and the set names its own operator and matching labels.
        assert!(
            display.contains("native_tree=[\nSet: or ignoring []"),
            "{expression}: {display}"
        );
        assert!(
            display.contains("\n  Input: evaluated samples source=0, contiguous")
                && display.contains("\n  Input: evaluated samples source=1, contiguous"),
            "{expression}: {display}"
        );
        let children = fused.children();
        assert_eq!(children.len(), 2, "{expression}: {display}");
        assert!(
            find_plan_node(children[empty_index], "EmptyExec").is_some(),
            "{expression}: {display}"
        );
        let shape = native_plan_shape(children[1 - empty_index]);
        assert_eq!(shape.regions_per_scan, vec![2], "{expression}: {display}");
        assert_eq!(
            fused.output_partitioning().partition_count(),
            1,
            "{expression}: {display}"
        );
        assert!(
            children
                .iter()
                .all(|child| child.output_partitioning().partition_count() == 1),
            "{expression}: {display}"
        );

        let (columns, rows) = native_labelled_rows(output).await;
        // The value is named after the statement's expression, not `val`.
        let mut expected_columns = vec![
            "__name__".to_string(),
            "host".to_string(),
            "ts".to_string(),
            expression_name(expression),
        ];
        expected_columns.sort();
        assert_eq!(columns, expected_columns, "{expression}");
        let actual = rows
            .iter()
            .map(|(time, labels, value)| {
                assert_eq!(labels.len(), 2, "{expression}: {labels:?}");
                assert_eq!(
                    labels.get("__name__").map(String::as_str),
                    Some("alpha_total")
                );
                (*time, labels.get("host").unwrap().as_str(), *value)
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{expression}");
    }
}

/// A nameless selector discovers every metric table it matches and reads them as one `Union`.
/// `UnionExec` reports the frame metadata of its last child, so a time-partitioned metric named
/// before a host-partitioned one must still be reconciled: otherwise the union claims contiguous
/// order and `increase` folds each region fragment as its own series.
#[tokio::test(flavor = "multi_thread")]
async fn distributed_native_promql_metric_discovery_reconciles_mixed_ordering() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("distributed_native_promql_discovery")
        .await
        .with_datanodes(2)
        .with_real_datanode_grpc_addr(true)
        .with_frontend_real_datanode_grpc(true)
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .build(false)
        .await;
    let instance = cluster.fe_instance().clone();
    // The names sort so the contiguous (host-partitioned) metric is the union's last child, which
    // is exactly the ordering a union would otherwise claim for the whole input.
    execute_all(
        &instance,
        "CREATE TABLE dist_native_disc_a_frag_phy (host STRING, site STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host, site)) PARTITION ON COLUMNS (ts) (ts < '1970-01-01 00:02:00', ts >= '1970-01-01 00:02:00' AND ts < '1970-01-01 00:04:00', ts >= '1970-01-01 00:04:00') ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        "CREATE TABLE dist_native_disc_z_wide_phy (host STRING, site STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host, site)) PARTITION ON COLUMNS (host) (host < 'm', host >= 'm') ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    // Both metrics carry the same samples under their own `site`, so each contributes the same
    // increase and the two rows stay distinct after the metric name is folded away.
    for (table, physical, site) in [
        (
            "dist_native_disc_a_frag",
            "dist_native_disc_a_frag_phy",
            "left",
        ),
        (
            "dist_native_disc_z_wide",
            "dist_native_disc_z_wide_phy",
            "right",
        ),
    ] {
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {table} (host STRING, site STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host, site)) ENGINE=metric WITH ('on_physical_table'='{physical}')"
            ),
            QueryContext::arc(),
        )
        .await;
        execute_all(
            &instance,
            &format!(
                "INSERT INTO {table} (host, site, val, ts) VALUES ('a', '{site}', 2, 60000), ('a', '{site}', 4, 120000), ('a', '{site}', 6, 180000), ('a', '{site}', 8, 240000)"
            ),
            QueryContext::arc(),
        )
        .await;
    }

    let query = PromQuery {
        query: r#"sum by (host, site) (increase({host="a"}[5m]))"#.to_string(),
        start: "180".to_string(),
        end: "240".to_string(),
        step: "60s".to_string(),
        ..PromQuery::default()
    };
    cluster.reset_network_do_get_peak();
    let output = instance
        .do_promql_query(&query, native_query_context(true, 2))
        .await
        .remove(0)
        .unwrap();
    let plan = output.meta.plan.as_ref().expect("frontend physical plan");
    let shape = native_plan_shape(plan);
    assert!(shape.fused >= 1, "discovery stays native: {plan:?}");
    // The split adds a state router between the final and partial halves; the raw reconciliation
    // below the partial half is what must not be skipped.
    assert!(
        shape.routers_below_fused >= 1,
        "the lowered union is reconciled before evaluation: {plan:?}"
    );
    assert!(
        shape
            .scan_orderings
            .iter()
            .any(|ordering| ordering.as_deref() == Some("interleaved")),
        "the time-partitioned scan declares fragments: {:?}: {plan:?}",
        shape.scan_orderings
    );
    assert!(
        shape
            .scan_orderings
            .iter()
            .any(|ordering| ordering.as_deref() == Some("contiguous")),
        "the host-partitioned scan declares complete series: {:?}: {plan:?}",
        shape.scan_orderings
    );
    let rows = native_site_rows(output).await;
    // `increase` extrapolates the 2,4,6 window to the 5m boundary, as the cross-region fixture
    // also expects: each metric contributes 5 at 180s and 7 at 240s, under its own `site`.
    let expected = vec![
        (180_000, "a".to_string(), "left".to_string(), 5.0),
        (180_000, "a".to_string(), "right".to_string(), 5.0),
        (240_000, "a".to_string(), "left".to_string(), 7.0),
        (240_000, "a".to_string(), "right".to_string(), 7.0),
    ];
    assert_eq!(expected, rows);
    assert_eq!(cluster.network_do_get_active(), 0, "every body is drained");
    // The discovered union feeds one reader per member partition: two from the time-partitioned
    // metric and one from the host-partitioned metric. That is the per-scan bound summed: the
    // query-wide budget is installed only for the single-fused plan, and a plan with a union is
    // not counted against it.
    let peak = cluster.network_do_get_peak_active();
    assert!((1..=3).contains(&peak), "region reads stay bounded: {peak}");

    // An aggregate over a discovered selector reads the metric name from each source's own frames,
    // not from one bound metric. Folding without grouping adds them, and grouping by the name keeps
    // the two metrics apart even though their ordinary labels are identical. A window function drops
    // the name, so the grouping case reads the raw selector.
    for (expression, expected) in [
        (
            r#"sum(increase({host="a"}[5m]))"#,
            vec![(None, 10.0), (None, 14.0)],
        ),
        (
            r#"sum by (__name__) ({host="a"})"#,
            vec![
                (Some("dist_native_disc_a_frag".to_string()), 6.0),
                (Some("dist_native_disc_a_frag".to_string()), 8.0),
                (Some("dist_native_disc_z_wide".to_string()), 6.0),
                (Some("dist_native_disc_z_wide".to_string()), 8.0),
            ],
        ),
    ] {
        let output = instance
            .do_promql_query(
                &PromQuery {
                    query: expression.to_string(),
                    start: "180".to_string(),
                    end: "240".to_string(),
                    step: "60s".to_string(),
                    ..PromQuery::default()
                },
                native_query_context(true, 2),
            )
            .await
            .remove(0)
            .unwrap();
        let plan = output.meta.plan.as_ref().expect("frontend physical plan");
        assert!(
            native_plan_shape(plan).fused >= 1,
            "a discovery aggregate stays native: {plan:?}"
        );
        let batches = match output.data {
            OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
            OutputData::RecordBatches(batches) => batches,
            _ => unreachable!("PromQL returns record batches"),
        };
        // Neither column is located by position: the result puts `__name__` last. A row whose series
        // reports no name carries a null there - the column is declared for every samples root, so a
        // null, not a missing column, is exactly "no metric name".
        let mut rows = Vec::new();
        for batch in batches {
            let names = batch.column_by_name("__name__").map(|column| {
                cast(column.as_ref(), &DataType::Utf8)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone()
            });
            let values = batch
                .column_by_name(&expression_name(expression))
                .expect("one value column")
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let name = names
                    .as_ref()
                    .and_then(|names| (!names.is_null(row)).then(|| names.value(row).to_string()));
                rows.push((name, values.value(row)));
            }
        }
        // The name travels with the value: a plan that grouped by the wrong name, or dropped the
        // name, would still report the same numbers, so the assertion is on the (name, value) pairs.
        rows.sort_by(|left, right| {
            left.0
                .as_deref()
                .cmp(&right.0.as_deref())
                .then(left.1.total_cmp(&right.1))
        });
        assert_eq!(expected, rows, "{expression}");
    }
}

/// A union composed under a grouping by the metric name reports one group per metric, over the
/// wire.
///
/// `sum by (__name__) (metric or mito)` reads a metric table beside an ordinary Mito one. The
/// union is one fused producer whose segments read the two sources, and the name each side reports
/// is the label the outer grouping folds by, so the answer is two groups whose label sets differ
/// only in their metric name. That grouping is what this asserts, together with the route: a plan
/// that dropped the name would fold the two series into one group whose single value still looks
/// plausible, and a plan answered by the legacy join would have no name column to fold at all.
#[tokio::test(flavor = "multi_thread")]
async fn distributed_native_promql_remote_answers_a_name_grouping_over_a_native_union() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("distributed_native_promql_refusal")
        .await
        .with_datanodes(2)
        .with_real_datanode_grpc_addr(true)
        .with_frontend_real_datanode_grpc(true)
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .build(false)
        .await;
    let instance = cluster.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE dist_native_refuse_phy (host STRING, site STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host, site)) PARTITION ON COLUMNS (ts) (ts < '1970-01-01 00:02:00', ts >= '1970-01-01 00:02:00') ENGINE=metric WITH ('physical_metric_table'='true')",
        QueryContext::arc(),
    )
    .await;
    for table in ["dist_native_refuse_a", "dist_native_refuse_b"] {
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {table} (host STRING, site STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host, site)) ENGINE=metric WITH ('on_physical_table'='dist_native_refuse_phy')"
            ),
            QueryContext::arc(),
        )
        .await;
        execute_all(
            &instance,
            &format!(
                "INSERT INTO {table} (host, site, val, ts) VALUES ('a', '{table}', 2, 60000), ('a', '{table}', 4, 120000), ('a', '{table}', 6, 180000)"
            ),
            QueryContext::arc(),
        )
        .await;
    }
    // An ordinary Mito table beside a metric one: no logical id, so a tree reading both is several
    // inputs even though only one of them carries a metric logical id.
    execute_all(
        &instance,
        "CREATE TABLE dist_native_refuse_mito (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE=mito",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        "INSERT INTO dist_native_refuse_mito (host, val, ts) VALUES ('a', 2, 60000), ('a', 4, 120000), ('a', 6, 180000)",
        QueryContext::arc(),
    )
    .await;

    for expression in [r#"sum by (__name__) (dist_native_refuse_a or dist_native_refuse_mito)"#] {
        let output = instance
            .do_promql_query(
                &PromQuery {
                    query: expression.to_string(),
                    start: "180".to_string(),
                    end: "240".to_string(),
                    step: "60s".to_string(),
                    ..PromQuery::default()
                },
                native_query_context(true, 2),
            )
            .await
            .remove(0)
            .expect("the composed plan answers a grouping by the metric name");
        let plan = Arc::clone(output.meta.plan.as_ref().expect("frontend physical plan"));
        // The route has to stay honest: the union is a native producer rather than the legacy join
        // the existing plan builds for it, so the name column the grouping reads exists at all.
        assert!(
            native_plan_shape(&plan).fused >= 1,
            "the union stays native: {plan:?}"
        );
        let display = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(
            !display.contains("JoinExec"),
            "the union is not a legacy join: {display}"
        );
        let (_, rows) = native_labelled_rows(output).await;
        let named = |name: &str| {
            std::collections::BTreeMap::from([("__name__".to_string(), name.to_string())])
        };
        // Grouping by the name keeps no other label, and the 180s sample is still inside the 5m
        // lookback at 240s, so both steps report the same value under each name.
        assert_eq!(
            vec![
                (180_000, named("dist_native_refuse_a"), 6.0),
                (240_000, named("dist_native_refuse_a"), 6.0),
                (180_000, named("dist_native_refuse_mito"), 6.0),
                (240_000, named("dist_native_refuse_mito"), 6.0),
            ],
            rows,
            "{expression}: one group per metric name, each named after its own metric"
        );
    }
}

/// The columns and `(timestamp, value)` rows of a statement that reads no table, whose time column
/// is the statement's own rather than a table's.
async fn data_free_rows(output: Output) -> (Vec<String>, Vec<(i64, f64)>) {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("PromQL query returns record batches"),
    };
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for batch in batches {
        columns = batch
            .schema
            .column_schemas()
            .iter()
            .map(|column| column.name.clone())
            .collect();
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let values = batch
            .columns()
            .iter()
            .filter(|column| column.data_type() == &DataType::Float64)
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 1, "one result value column");
        let values = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
        for row in 0..batch.num_rows() {
            rows.push((timestamps.value(row), values.value(row)));
        }
    }
    rows.sort_by_key(|row| row.0);
    (columns, rows)
}

/// A statement that reads no table plans natively over the wire, with an expression-named value.
#[tokio::test(flavor = "multi_thread")]
async fn distributed_native_promql_data_free_statement_plans_natively() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("distributed_native_promql_data_free")
        .await
        .with_datanodes(2)
        .with_real_datanode_grpc_addr(true)
        .with_frontend_real_datanode_grpc(true)
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .build(false)
        .await;
    let instance = cluster.fe_instance().clone();
    for (expression, labels, expected_rows) in [
        // `start`/`end` are seconds, so this one-step range evaluates at 3000ms.
        ("time()", &[][..], &[(3000, 3.0)][..]),
        (
            // A samples root declares the name of each row it publishes, and `vector(1)` has
            // none, so the column is present and null. `time()` above is a scalar root and keeps
            // two columns.
            r#"label_replace(vector(1), "host", "host1", "", "")"#,
            &["host", "__name__"][..],
            &[(3000, 1.0)][..],
        ),
    ] {
        let query = PromQuery {
            query: expression.to_string(),
            start: "3".to_string(),
            end: "3".to_string(),
            step: "1s".to_string(),
            ..PromQuery::default()
        };
        let output = instance
            .do_promql_query(&query, native_query_context(true, 2))
            .await
            .remove(0)
            .unwrap();
        let plan = output.meta.plan.clone().expect("frontend physical plan");
        let shape = native_plan_shape(&plan);
        assert_eq!(
            1, shape.fused,
            "a data-free statement is one fused node: {plan:?}"
        );
        assert!(
            shape.regions_per_scan.is_empty(),
            "a data-free statement reads no region: {plan:?}"
        );
        let (columns, rows) = data_free_rows(output).await;
        let QueryStatement::Promql(stmt, _) =
            QueryLanguageParser::parse_promql(&query, &QueryContext::arc()).unwrap()
        else {
            unreachable!()
        };
        let mut expected_columns = vec!["time".to_string(), stmt.expr.to_string()];
        expected_columns.extend(labels.iter().map(|label| (*label).to_string()));
        assert_eq!(expected_columns, columns, "{expression}");
        assert_eq!(expected_rows, rows.as_slice(), "{expression}");

        // The same statement through the existing planner answers the same rows.
        let legacy = instance
            .do_promql_query(&query, native_query_context(false, 2))
            .await
            .remove(0)
            .unwrap();
        let (_, legacy_rows) = data_free_rows(legacy).await;
        assert_eq!(rows, legacy_rows, "{expression} must answer the same rows");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_label_rewrites_reach_the_native_route_and_keep_prometheus_answers() {
    common_telemetry::init_default_ut_logging();
    let mito_config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_label_rewrite_reachable")
            .with_mito_config(mito_config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    // Sixteen host series spread the scan over four partitions, and `probe_time` holds the
    // complementary pair: two stored series ten minutes apart - twice the five-minute lookback - so
    // that at each evaluation point exactly one of them holds a value.
    execute_all(
        &instance,
        "CREATE TABLE probe_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true'); \
         CREATE TABLE probe (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='probe_phy'); \
         CREATE TABLE probe_time (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='probe_phy'); \
         CREATE TABLE probe_two (host STRING, dc STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host, dc)) ENGINE=metric WITH ('on_physical_table'='probe_phy'); \
         INSERT INTO probe_two (host,dc,ts,val) VALUES ('a','z1',0,1),('b','z2',0,2); \
         INSERT INTO probe_time (host,ts,val) VALUES ('first',0,10),('second',600000,20); \
         INSERT INTO probe (host,ts,val) VALUES ('a',0,1),('b',0,2),('c',0,3),('d',0,4),('e',0,5),('f',0,6),('g',0,7),('h',0,8),('i',0,9),('j',0,10),('k',0,11),('l',0,12),('m',0,13),('n',0,14),('o',0,15),('p',0,16)",
        QueryContext::arc(),
    )
    .await;

    let run = |expr: &str, range: &str| {
        let sql = format!("TQL EVAL {range} {expr}");
        let instance = instance.clone();
        async move {
            instance
                .do_query(&sql, native_query_context(true, 4))
                .await
                .remove(0)
        }
    };

    // `without(host)` keys the cut state by the prefix's `dst` labels. The statement's
    // declared output schema provides `dst`, which the native label walker cannot infer.
    let without_expr =
        r#"sum without (host) (label_replace(probe{host=~"a|b"}, "dst", "$1", "host", "(.*)"))"#;
    observer.clear();
    let without_native = run(without_expr, "(0, 0, '1s')").await.unwrap();
    let native_plan = observer.root_plan();
    assert!(
        holds_split_merge(&native_plan),
        "the native plan must fold partial state in a Final node: {}",
        datafusion::physical_plan::displayable(native_plan.as_ref()).indent(true)
    );
    let (_, native_rows) = native_labelled_rows(without_native).await;
    let legacy = instance
        .do_query(
            &format!("TQL EVAL (0, 0, '1s') {without_expr}"),
            native_query_context(false, 4),
        )
        .await
        .remove(0)
        .unwrap();
    let (_, legacy_rows) = native_labelled_rows(legacy).await;
    assert_eq!(
        native_rows, legacy_rows,
        "native rows must match legacy exactly"
    );
    assert_eq!(native_rows.len(), 2);
    assert!(
        native_rows
            .iter()
            .any(|row| row.1.get("dst") == Some(&"a".to_string()))
    );
    assert!(
        native_rows
            .iter()
            .any(|row| row.1.get("dst") == Some(&"b".to_string()))
    );

    // A fresh destination cannot make two series equal, so the rewrite is safe to spread and the
    // scan keeps all four partitions.

    observer.clear();
    let fresh = run(
        r#"label_replace(probe, "dst", "$1", "host", "(.*)")"#,
        "(0, 0, '1s')",
    )
    .await
    .unwrap();
    let fresh_rows = native_result_rows(fresh).await;
    let fused = observer.take();
    assert_four_native_partitions(&fused, fresh_rows.len());
    assert_eq!(
        fresh_rows,
        (0..16)
            .map(|index| (0, ((b'a' + index) as char).to_string(), (index + 1) as f64))
            .collect::<Vec<_>>(),
        "a fresh-destination rewrite answers every series it read"
    );

    // The aggregate boundary must carry the label created by the prefix, not only scan tags.
    observer.clear();
    let grouped = run(
        r#"sum by (dst) (label_replace(probe{host=~"a|b"}, "dst", "$1", "host", "(.*)"))"#,
        "(0, 0, '1s')",
    )
    .await
    .unwrap();
    // `native_result_rows` reads a `host` column, which this grouping does not emit: read the
    // declared labels instead, so the assertion is about the `dst` the prefix created.
    let (_, grouped_rows) = native_labelled_rows(grouped).await;
    assert_eq!(observer.fused().unwrap().name(), "FusedSeriesExec");
    let mut grouped = grouped_rows
        .iter()
        .map(|(at, labels, value)| (*at, labels.get("dst").cloned(), *value))
        .collect::<Vec<_>>();
    grouped.sort_by(|left, right| left.1.cmp(&right.1));
    assert_eq!(
        grouped,
        vec![
            (0, Some("a".to_string()), 1.0),
            (0, Some("b".to_string()), 2.0)
        ],
        "the aggregate boundary must carry the label the prefix created"
    );

    observer.clear();
    let nested = run(r#"sum(avg by (host) (probe{host=~"a|c"}))"#, "(0, 0, '1s')")
        .await
        .unwrap();
    let (_, nested_rows) = native_labelled_rows(nested).await;
    assert_eq!(nested_rows.len(), 1);
    assert_eq!(nested_rows[0].2, 4.0, "sum of per-host averages");

    observer.clear();
    let matched = run(
        r#"label_replace(probe{host=~"a|b"}, "dst", "$1", "host", "(.*)") + on(dst) label_replace(probe{host=~"a|b"}, "dst", "$1", "host", "(.*)")"#,
        "(0, 0, '1s')",
    )
    .await
    .unwrap();
    let (_, matched_rows) = native_labelled_rows(matched).await;
    assert_eq!(
        matched_rows.len(),
        2,
        "matching must retain dst on both vectors"
    );
    assert_eq!(
        matched_rows
            .iter()
            .map(|row| (row.1.get("dst").unwrap().clone(), row.2))
            .collect::<Vec<_>>(),
        vec![("a".to_string(), 2.0), ("b".to_string(), 4.0)]
    );

    observer.clear();
    let dynamic_scalar = run(r#"round(probe, time() / 1e9) + time()"#, "(0, 0, '1s')")
        .await
        .unwrap();
    assert_eq!(native_result_rows(dynamic_scalar).await.len(), 16);
    let dynamic_plan = observer.take();
    assert_eq!(
        dynamic_plan.output_partitioning().partition_count(),
        4,
        "a per-series dynamic scalar must preserve scan parallelism"
    );

    // Two series rewritten to one label set can only collide across execution partitions when each
    // partition holds one of them: a route that keeps per-partition duplicate state then sees no
    // collision at all. Prove the two series really are split first, on the fresh-destination form
    // the route does admit, so this is not a test that would pass because they landed together.
    observer.clear();
    let split_parts = run(
        r#"label_replace(probe{host=~"a|b"}, "dst", "$1", "host", "(.*)")"#,
        "(0, 0, '1s')",
    )
    .await
    .unwrap();
    let split_rows = native_result_rows(split_parts).await;
    assert_eq!(split_rows.len(), 2, "{split_rows:?}");
    let split_plan = observer.take();
    let nonempty = fused_metric_set(&split_plan)
        .iter()
        .filter(|metric| metric.value().name() == "native.root.output_values")
        .filter_map(|metric| match metric.value() {
            datafusion::physical_plan::metrics::MetricValue::Count { count, .. } => {
                Some((metric.partition(), count.value()))
            }
            _ => None,
        })
        .filter(|(_, count)| *count > 0)
        .map(|(partition, _)| partition)
        .collect::<Vec<_>>();
    assert!(
        nonempty.len() > 1,
        "the two colliding series must occupy different execution partitions, got {nonempty:?}"
    );

    // `histogram_count` is point-wise but not sample-preserving: it drops every float sample, and
    // `probe` holds floats. A canonical merge placed *above* it would be handed an occupancy the
    // map has already erased, so the very same rewritten label set would answer nothing instead of
    // the error Prometheus reports for two values at one point. The merge therefore sits below the
    // map, and the two rewritten series - in different partitions, as established just above - meet
    // in the router before `histogram_count` runs. The existing plan is not asserted to agree here:
    // this is the Prometheus error contract, which the native route owes on its own.
    observer.clear();
    let histogram = run(
        r#"histogram_count(label_replace(probe{host=~"a|b"}, "host", "same", "host", ".*"))"#,
        "(0, 0, '1s')",
    )
    .await;
    let histogram_error = match histogram {
        Ok(output) => collect_error(output).await,
        Err(error) => error.to_string(),
    };
    assert!(
        histogram_error.contains("cannot contain metrics with the same labelset"),
        "the canonical boundary must judge the rewritten label set before `histogram_count` drops \
         the float samples it is judged on, got: {histogram_error}"
    );
    let histogram_plan = observer.root_plan();
    let histogram_router = canonical_router(&observer).unwrap_or_else(|| {
        panic!(
            "`histogram_count` over a collapsing rewrite routes by canonical label set: {} \
             regions {:?}",
            display_plan_tree(&histogram_plan),
            observer
                .region_plans()
                .iter()
                .map(display_plan_tree)
                .collect::<Vec<_>>()
        )
    });
    assert_eq!(
        histogram_router.children()[0].name(),
        "FusedSeriesExec",
        "the router reads the canonical producer: {}",
        display_plan_tree(&histogram_plan)
    );
    let histogram_producer = histogram_router.children()[0].clone();
    // The boundary's own tree is the merge over the rewrite: `histogram_count` is not in it, so the
    // router that validates the collected label set runs below the map rather than above it. The
    // physical node renders its own tree directly below its operator line.
    let producer_display = datafusion::physical_plan::displayable(histogram_producer.as_ref())
        .one_line()
        .to_string();
    let producer_tree = producer_display
        .split_once('\n')
        .map(|(_, tree)| tree)
        .unwrap_or_else(|| panic!("the producer renders its tree: {producer_display}"));
    assert!(
        producer_tree.contains("Merge"),
        "the producer's tree is the canonical merge itself: {producer_tree}"
    );
    assert!(
        !producer_tree.contains("histogram_count"),
        "the merge is below `histogram_count`, not above it: {producer_tree}"
    );
    // What each producer partition read, and what it published. The published values are the ones
    // the query's own map would have dropped: they exist because the merge ran below it, and they
    // come from two partitions because the two colliding series were scanned in two.
    let read = nonempty_partitions(&histogram_producer, "native.root.input_samples");
    assert!(
        read.len() > 1,
        "the two colliding series are read in different execution partitions: {read:?}"
    );
    let published = nonempty_partitions(&histogram_producer, "native.root.output_values");
    assert!(
        published.len() > 1,
        "the merge below `histogram_count` publishes the rewritten series, one per partition, \
         although the map above it drops floats: {published:?}"
    );
    // No gather: the router keeps the scan's width instead of collecting every piece into one
    // execution, which is what a boundary above the map would have had to fall back to.
    assert_eq!(
        4,
        histogram_router
            .properties()
            .output_partitioning()
            .partition_count(),
        "the canonical router keeps the scan width: {}",
        display_plan_tree(&histogram_plan)
    );

    // Rewriting an existing distinguishing tag can collapse distinct label sets. The canonical
    // boundary must reject the overlap with the same error the existing plan reports.
    for (shape, expr) in [
        (
            "at the root",
            r#"label_replace(probe, "host", "same", "host", ".*")"#,
        ),
        (
            "under sum",
            r#"sum(label_replace(probe, "host", "same", "host", ".*"))"#,
        ),
        (
            "under topk",
            r#"topk(1, label_replace(probe, "host", "same", "host", ".*"))"#,
        ),
        (
            "above a binary-created label",
            r#"label_replace(sum by (dst) (label_replace(probe, "dst", "$1", "host", "(.*)") * 1), "dst", "same", "", "")"#,
        ),
        (
            "above count_values",
            r#"label_replace(count_values("v", probe), "v", "same", "v", ".*")"#,
        ),
    ] {
        observer.clear();
        let native = run(expr, "(0, 0, '1s')").await;
        assert_eq!(
            observer.fused().as_ref().map(|plan| plan.name()),
            Some("FusedSeriesExec"),
            "{shape}: collapsing rewrite uses canonical native boundary"
        );
        let legacy = instance
            .do_query(
                &format!("TQL EVAL (0, 0, '1s') {expr}"),
                native_query_context(false, 4),
            )
            .await
            .remove(0);
        let native_error = match native {
            Ok(output) => collect_error(output).await,
            Err(error) => error.to_string(),
        };
        let legacy_error = match legacy {
            Ok(output) => collect_error(output).await,
            Err(error) => error.to_string(),
        };
        assert!(
            native_error.contains("cannot contain metrics with the same labelset"),
            "{shape}: native must surface the underlying duplicate-label error, got: {native_error}"
        );
        assert!(
            legacy_error.contains("cannot contain metrics with the same labelset"),
            "{shape}: legacy must report the same conflict, got: {legacy_error}"
        );
    }

    // The remaining rewrite shapes a caller writes: a join of two tags, a deletion, and a rewrite of
    // the metric name. Each is asserted on the columns the answer actually carries, because a rewrite
    // that names a label the schema does not describe would publish a tag the reader cannot see.
    let run_two = |expr: &str| {
        let sql = format!("TQL EVAL (0, 0, '1s') {expr}");
        let instance = instance.clone();
        async move {
            instance
                .do_query(&sql, native_query_context(true, 4))
                .await
                .remove(0)
                .unwrap()
        }
    };
    for (shape, expr, column, expected) in [
        (
            "join two tags into one",
            r#"label_join(probe_two, "joined", "-", "host", "dc")"#,
            "joined",
            vec!["a-z1".to_string(), "b-z2".to_string()],
        ),
        (
            "delete a tag",
            r#"label_replace(probe_two, "dc", "", "dc", ".*")"#,
            "host",
            vec!["a".to_string(), "b".to_string()],
        ),
    ] {
        observer.clear();
        let output = run_two(expr).await;
        let batches = match output.data {
            OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
            OutputData::RecordBatches(batches) => batches,
            _ => unreachable!("TQL EVAL returns record batches"),
        };
        let schema_names = batches
            .schema()
            .column_schemas()
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let mut values = Vec::new();
        for batch in batches.iter() {
            let column = batch.column_by_name(column).unwrap_or_else(|| {
                panic!("{shape}: the schema must carry `{column}`: {schema_names:?}")
            });
            let column = cast(column.as_ref(), &DataType::Utf8).unwrap();
            let column = column.as_any().downcast_ref::<StringArray>().unwrap();
            values.extend((0..column.len()).map(|row| column.value(row).to_string()));
        }
        values.sort();
        assert_eq!(values, expected, "{shape}");
        if shape == "join two tags into one" {
            // A fresh destination cannot make two series equal, so the route runs it natively and
            // keeps the scan's partitions.
            assert_eq!(
                observer.take().output_partitioning().partition_count(),
                4,
                "{shape}: a rewrite that cannot make two series equal stays spread"
            );
        } else {
            // The canonical boundary merges rewritten label identities before publishing output.
            assert_eq!(
                observer.fused().as_ref().map(|plan| plan.name()),
                Some("FusedSeriesExec"),
                "{shape}: a collapsing deletion uses the canonical native boundary"
            );
        }
    }

    // `label_join`'s source list is variadic and may be empty, which deletes the destination label.
    // That is legal PromQL, and admission must not turn a legal query into an error just because
    // the shape cannot run natively: the answer has to equal the existing plan's either way.
    {
        let three_arg = r#"label_join(probe_two, "dc", "-")"#;
        let sql = format!("TQL EVAL (0, 0, '1s') {three_arg}");
        observer.clear();
        let native = instance
            .do_query(&sql, native_query_context(true, 4))
            .await
            .remove(0)
            .unwrap();
        let (_, native_rows) = native_labelled_rows(native).await;
        let legacy = instance
            .do_query(&sql, native_query_context(false, 4))
            .await
            .remove(0)
            .unwrap();
        let (_, legacy_rows) = native_labelled_rows(legacy).await;
        // A rewrite adds or deletes a label and reports the series it read, so the rows keep
        // `probe_two` as their name. The existing route declares no name column, so its rows are
        // the same answer without that one label.
        assert!(
            native_rows
                .iter()
                .all(|(_, labels, _)| labels.get("__name__").map(String::as_str)
                    == Some("probe_two")),
            "a rewrite keeps the series' name: {native_rows:?}"
        );
        let expected_native = legacy_rows
            .into_iter()
            .map(|(second, mut labels, value)| {
                labels.insert("__name__".to_string(), "probe_two".to_string());
                (second, labels, value)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            native_rows, expected_native,
            "a three-argument `label_join` deletes its destination and must answer as the existing \
             plan does"
        );
        assert!(
            native_rows
                .iter()
                .all(|(_, labels, _)| !labels.contains_key("dc")),
            "the destination label is deleted, not published: {native_rows:?}"
        );
    }

    // A `__name__` destination is native-only - the existing planner refuses it - and is covered by
    // `native_promql_rewrites_the_metric_name` below.

    // Two stored series rewritten to one label set, holding values at different evaluation points,
    // are one series carrying both points rather than two rows.
    observer.clear();
    let complementary = run(
        r#"label_replace(probe_time, "host", "same", "host", ".*")"#,
        "(0, 600, '10m')",
    )
    .await
    .unwrap();
    assert_eq!(
        native_result_rows(complementary).await,
        vec![(0, "same".into(), 10.0), (600_000, "same".into(), 20.0)],
        "complementary points of one rewritten label set are one series"
    );
}

/// A `__name__` destination is native-only: the existing planner refuses that destination, so this
/// test reads the native route's own answer and the plan that produced it, and never compares
/// against a legacy run.
///
/// Three logical metrics share one physical table. `name_left` and `name_right` hold the same two
/// ordinary label sets ten minutes apart, so rewriting both names to one value puts their samples
/// under one canonical identity; `name_clash` holds a sample where `name_left` already has one under
/// `host="h"`, so the same rewrite has a genuine same-point conflict to report.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_rewrites_the_metric_name() {
    common_telemetry::init_default_ut_logging();
    let mito_config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_metric_name_rewrite")
            .with_mito_config(mito_config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE name_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true'); \
         CREATE TABLE name_left (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='name_phy'); \
         CREATE TABLE name_right (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='name_phy'); \
         CREATE TABLE name_clash (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='name_phy'); \
         INSERT INTO name_left (host, ts, val) VALUES ('h', 0, 1), ('i', 0, 2); \
         INSERT INTO name_right (host, ts, val) VALUES ('h', 600000, 10), ('i', 600000, 20); \
         INSERT INTO name_clash (host, ts, val) VALUES ('h', 0, 100)",
        QueryContext::arc(),
    )
    .await;

    let run = |expr: &str, range: &str| {
        let sql = format!("TQL EVAL {range} {expr}");
        let instance = instance.clone();
        async move {
            instance
                .do_query(&sql, native_query_context(true, 4))
                .await
                .remove(0)
        }
    };
    // A nameless selector is answered by enumerating the schema's metric tables, which the
    // frontend's PromQL entry point bounds by the caller's grants before it plans. A SQL
    // `TQL EVAL` statement prepares no such scope, so the test states the unrestricted one a
    // caller with every grant would get.
    let discovery_ctx = Arc::new({
        let mut ctx = QueryContext::with_db_name(None);
        ctx.set_extension(QUERY_PARALLELISM_HINT, 4.to_string());
        ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, "true");
        ctx.set_metric_discovery_scope(MetricDiscoveryScope::Unrestricted);
        ctx
    });
    let run_discovery = |expr: &str, range: &str| {
        let sql = format!("TQL EVAL {range} {expr}");
        let instance = instance.clone();
        let ctx = Arc::clone(&discovery_ctx);
        async move { instance.do_query(&sql, ctx).await.remove(0) }
    };
    let labels = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    // The boundary is a partition-local producer that merges what its own scan partition held and
    // the canonical router that collects every partition's pieces of one label set. The router is
    // only built over a canonical producer, so finding it by name is what proves the plan answered
    // through that boundary rather than some other way.
    let expect_canonical_boundary = |context: &str| {
        assert!(
            observer.fused().is_some(),
            "{context}: a native fused node answered"
        );
        assert!(
            canonical_router(&observer).is_some(),
            "{context}: the canonical router collects each label set: {:?}",
            observer
                .all_plans()
                .iter()
                .map(display_plan_tree)
                .collect::<Vec<_>>()
        );
    };

    // Renaming the name keeps series whose other labels differ distinct, and publishes the new name.
    observer.clear();
    let renamed = run(
        r#"label_replace(name_left, "__name__", "renamed", "__name__", "(.*)")"#,
        "(0, 0, '1s')",
    )
    .await
    .unwrap();
    let (columns, rows) = native_labelled_rows(renamed).await;
    assert!(
        columns.iter().any(|column| column == "__name__"),
        "the rewritten name is a column: {columns:?}"
    );
    assert_eq!(
        rows,
        vec![
            (0, labels(&[("__name__", "renamed"), ("host", "h")]), 1.0),
            (0, labels(&[("__name__", "renamed"), ("host", "i")]), 2.0),
        ],
        "a name rewrite reports the new name and keeps distinct series distinct"
    );
    expect_canonical_boundary("a name rewrite of a named scan");

    // Deleting the name reports no name, and a delete-only rewrite of a scan that publishes no name
    // column publishes none: whether a series keeps its value depends on the regex, and a deleted
    // value is the only one this rewrite can write.
    observer.clear();
    let deleted = run(
        r#"label_replace(name_left, "__name__", "", "__name__", "(.*)")"#,
        "(0, 0, '1s')",
    )
    .await
    .unwrap();
    let (columns, rows) = native_labelled_rows(deleted).await;
    assert!(
        columns.iter().any(|column| column == "__name__"),
        "the rows layout declares the name column even when the rewrite deletes it: {columns:?}"
    );
    assert_eq!(
        rows,
        vec![
            (0, labels(&[("host", "h")]), 1.0),
            (0, labels(&[("host", "i")]), 2.0),
        ],
        "a deleted name is absent from the answer"
    );
    expect_canonical_boundary("a name deletion");

    // `label_join` writes the name as well, and an empty source list deletes it.
    for (expr, expected) in [
        (
            r#"label_join(name_left, "__name__", "-", "host")"#,
            vec![
                (0, labels(&[("__name__", "h"), ("host", "h")]), 1.0),
                (0, labels(&[("__name__", "i"), ("host", "i")]), 2.0),
            ],
        ),
        (
            r#"label_join(name_left, "__name__", "-")"#,
            vec![
                (0, labels(&[("host", "h")]), 1.0),
                (0, labels(&[("host", "i")]), 2.0),
            ],
        ),
    ] {
        observer.clear();
        let joined = run(expr, "(0, 0, '1s')").await.unwrap();
        let (_, rows) = native_labelled_rows(joined).await;
        assert_eq!(rows, expected, "{expr}");
        expect_canonical_boundary(expr);
    }

    // Two metrics told apart only by name: rewriting both names to one value merges the points each
    // of them holds, because the canonical identity is the complete label set with the new name.
    observer.clear();
    let complementary = run_discovery(
        r#"label_replace({__name__=~"name_left|name_right", host=~"h|i"}, "__name__", "same", "__name__", "(.*)")"#,
        "(0, 600, '10m')",
    )
    .await
    .unwrap();
    let (_, rows) = native_labelled_rows(complementary).await;
    assert_eq!(
        rows,
        vec![
            (0, labels(&[("__name__", "same"), ("host", "h")]), 1.0),
            (
                600_000,
                labels(&[("__name__", "same"), ("host", "h")]),
                10.0
            ),
            (0, labels(&[("__name__", "same"), ("host", "i")]), 2.0),
            (
                600_000,
                labels(&[("__name__", "same"), ("host", "i")]),
                20.0
            ),
        ],
        "one identity carries both metrics' points"
    );
    expect_canonical_boundary("two metrics renamed to one name");

    // A name written above `abs` is collected even though `abs` dropped the name: the two metrics are
    // still told apart by name alone below it, so writing one name folds their samples into one
    // identity. Without the collection each metric would answer as its own series under one label set.
    observer.clear();
    let rewritten_above_abs = run_discovery(
        r#"label_replace(abs({__name__=~"name_left|name_right", host=~"h|i"}), "__name__", "same", "", ".*")"#,
        "(0, 600, '10m')",
    )
    .await
    .unwrap();
    let (_, rows) = native_labelled_rows(rewritten_above_abs).await;
    assert_eq!(
        rows,
        vec![
            (0, labels(&[("__name__", "same"), ("host", "h")]), 1.0),
            (
                600_000,
                labels(&[("__name__", "same"), ("host", "h")]),
                10.0
            ),
            (0, labels(&[("__name__", "same"), ("host", "i")]), 2.0),
            (
                600_000,
                labels(&[("__name__", "same"), ("host", "i")]),
                20.0
            ),
        ],
        "one identity carries both metrics' points after `abs` dropped their names"
    );
    expect_canonical_boundary("a name rewrite above a dropped name");

    // The same shape at one point is that identity's duplicate-label-set error, and it is reported
    // even though the two series came from different metrics and a math kernel erased their names.
    observer.clear();
    let renamed_above_abs_clash = run_discovery(
        r#"label_replace(abs({__name__=~"name_left|name_clash", host="h"}), "__name__", "same", "", ".*")"#,
        "(0, 0, '1s')",
    )
    .await;
    let error = match renamed_above_abs_clash {
        Ok(output) => collect_error(output).await,
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("cannot contain metrics with the same labelset"),
        "a name written above `abs` reports the duplicate label set: {error}"
    );
    assert!(
        observer.fused().is_some(),
        "the collision after a dropped name is reported by a native plan"
    );

    // A point where the other metric already holds a sample is the duplicate-label-set error, and it
    // is reported even though the two series came from different metrics.
    observer.clear();
    let clash = run_discovery(
        r#"label_replace({__name__=~"name_left|name_clash", host="h"}, "__name__", "same", "__name__", "(.*)")"#,
        "(0, 0, '1s')",
    )
    .await;
    let error = match clash {
        Ok(output) => collect_error(output).await,
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("cannot contain metrics with the same labelset"),
        "two metrics renamed to one identity report the duplicate label set: {error}"
    );
    assert!(
        observer.fused().is_some(),
        "the collision is reported by a native plan"
    );
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
    assert_four_native_partitions(&plan, native_rows.len());
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

    // Exercise the split aggregate's fixed-width min, group and stdvar states through the same
    // native/legacy comparison harness. Sorting is part of the statement output contract.
    for aggregate in [
        "min by (host)(native_e2e)",
        "group by (host)(native_e2e)",
        "stdvar by (host)(native_e2e)",
    ] {
        let query = PromQuery {
            query: aggregate.to_string(),
            start: "0".to_string(),
            end: "0".to_string(),
            step: "1s".to_string(),
            ..PromQuery::default()
        };
        let native_rows = native_result_rows(
            instance
                .do_promql_query(&query, native_query_context(true, 4))
                .await
                .remove(0)
                .unwrap(),
        )
        .await;
        let legacy_rows = native_result_rows(
            instance
                .do_promql_query(&query, native_query_context(false, 4))
                .await
                .remove(0)
                .unwrap(),
        )
        .await;
        assert_eq!(native_rows, legacy_rows, "{aggregate}");
        let expected_value = if aggregate.starts_with("min") {
            None
        } else if aggregate.starts_with("group") {
            Some(1.0)
        } else {
            Some(0.0)
        };
        assert_eq!(native_rows.len(), 16, "{aggregate}");
        for (index, (time, host, value)) in native_rows.iter().enumerate() {
            assert_eq!(*time, 0, "{aggregate}");
            assert_eq!(
                host,
                &char::from(b'a' + index as u8).to_string(),
                "{aggregate}"
            );
            if let Some(expected) = expected_value {
                assert_eq!(*value, expected, "{aggregate}, host={host}");
            }
        }
        if aggregate.starts_with("min") {
            assert_eq!(
                native_rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                (1..=16).map(|value| value as f64 * 2.0).collect::<Vec<_>>(),
                "{aggregate}"
            );
        }
    }
}

/// `promql` over `start..=end` at `step`, through the Prometheus handler.
///
/// The handler is the path that authorizes and plans a selector naming no metric: a `TQL EVAL`
/// statement reaches the planner through the statement executor instead, and that path prepares no
/// discovery scope, so it keeps declining such a selector. Its times are Prometheus timestamps, so
/// a bare number is a count of seconds.
async fn promql_output(
    instance: &Arc<Instance>,
    promql: &str,
    start: &str,
    end: &str,
    step: &str,
) -> Output {
    let query = PromQuery {
        query: promql.to_string(),
        start: start.to_string(),
        end: end.to_string(),
        step: step.to_string(),
        ..PromQuery::default()
    };
    servers::prometheus_handler::PrometheusHandler::do_query(
        instance.as_ref(),
        &query,
        native_query_context(true, 4),
    )
    .await
    .unwrap_or_else(|error| panic!("{promql} over {start}..={end} at {step}: {error}"))
}

/// The Prometheus v0.315 answers `absent_over_time` gives, and that the native route reports.
///
/// `absent_over_time` answers with the labels its selector's matchers fix at every point no series
/// of the selector has a sample at, and with nothing at the points one does. Three metric tables
/// make the shapes reachable: `absent_probe` carries `probe_label`, which no row's value matches, so
/// a nameless selector narrows to it; `absent_a` and `absent_b` carry the same ordinary labels, so
/// a nameless selector covers both and is one discovery expanding into several metrics, each
/// answering through its own raw source; and no table carries `no_such_label`, which is the reported
/// query - the one whose selector matches no metric table and whose labels therefore have to come
/// from the matchers alone.
///
/// The existing plan answers the call per series with a UDF, which needs a metric name to read a
/// table from: such a selector writes none, and the labels still have to reach the answer at every
/// step.
///
/// The statements are evaluated through the Prometheus handler, which is the path that authorizes a
/// selector naming no metric; a bare timestamp there is a count of seconds, so the samples below sit
/// at 0s and 60s and the evaluation times are written as the seconds they are.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_absent_over_time_answers_like_prometheus() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_absent_over_time_e2e",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE absent_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true')\
         ; CREATE TABLE absent_a (job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(job)) ENGINE=metric WITH ('on_physical_table'='absent_phy')\
         ; CREATE TABLE absent_b (job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(job)) ENGINE=metric WITH ('on_physical_table'='absent_phy')\
         ; CREATE TABLE absent_probe (job STRING, probe_label STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(job, probe_label)) ENGINE=metric WITH ('on_physical_table'='absent_phy')\
         ; INSERT INTO absent_a (job, ts, val) VALUES ('api', 0, 1), ('api', 60000, 2)\
         ; INSERT INTO absent_b (job, ts, val) VALUES ('api', 0, 3), ('api', 60000, 4)\
         ; INSERT INTO absent_probe (job, probe_label, ts, val) VALUES ('api', 'y', 0, 5)",
        QueryContext::arc(),
    )
    .await;

    let labels = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    // Every case is the native route's answer: a shape that fell back to the existing plan would
    // error, because that plan needs a metric name to read a table from.
    for (promql, start, end, step, expected) in [
        // Samples inside the window: nothing to report.
        (
            "absent_over_time(absent_a[5m])",
            "120",
            "120",
            "1s",
            Vec::new(),
        ),
        // The metric exists and holds no sample in the window: one series, and no `__name__`.
        (
            "absent_over_time(absent_a[5m])",
            "400",
            "400",
            "1s",
            vec![(400000, labels(&[]), 1.0)],
        ),
        // A named metric's other equality matcher is a label of the answer.
        (
            r#"absent_over_time(absent_a{job="api"}[5m])"#,
            "400",
            "400",
            "1s",
            vec![(400000, labels(&[("job", "api")]), 1.0)],
        ),
        // The reported query: no metric table carries `no_such_label`, so the selector matches
        // none, and both equality matchers are the answer's labels.
        (
            r#"absent_over_time({no_such_label="x", job="api"}[5m])"#,
            "120",
            "120",
            "1s",
            vec![(
                120000,
                labels(&[("job", "api"), ("no_such_label", "x")]),
                1.0,
            )],
        ),
        // The same answer at every step of a range.
        (
            r#"absent_over_time({no_such_label="x", job="api"}[5m])"#,
            "120",
            "400",
            "70s",
            [120000i64, 190000, 260000, 330000, 400000]
                .into_iter()
                .map(|time| (time, labels(&[("job", "api"), ("no_such_label", "x")]), 1.0))
                .collect::<Vec<_>>(),
        ),
        // One metric table carries `probe_label`, and no row's value matches, so the discovery
        // narrows to that table and the presence read still finds nothing.
        (
            r#"absent_over_time({probe_label="x", job="api"}[5m])"#,
            "120",
            "120",
            "1s",
            vec![(120000, labels(&[("job", "api"), ("probe_label", "x")]), 1.0)],
        ),
    ] {
        observer.clear();
        let (columns, native) =
            native_labelled_rows(promql_output(&instance, promql, start, end, step).await).await;
        assert!(
            observer.fused().is_some(),
            "{promql} did not run on the native route: {columns:?} {native:?}"
        );
        assert_eq!(
            native, expected,
            "{promql} over {start}..={end} at {step}: {columns:?}"
        );
    }

    // A nameless selector that discovery expands into more than one metric answers. `absent_a`,
    // `absent_b` and `absent_probe` all carry `job`, and the first two carry the same ordinary
    // labels, so `{job="api"}` is one discovery expanding into several metrics whose series would
    // collapse into one if the presence read dropped the metric name. It does not, so the node
    // reads a frame per metric - its child is the union of their raw sources - rather than one scan
    // bound to one name.
    //
    // Both metrics holding samples in the window answer nothing: neither may be read as the other.
    observer.clear();
    let (columns, rows) = native_labelled_rows(
        promql_output(
            &instance,
            r#"absent_over_time({job="api"}[5m])"#,
            "120",
            "120",
            "1s",
        )
        .await,
    )
    .await;
    assert!(
        observed_physical_nodes(&observer)
            .iter()
            .any(|node| node.name() == "FusedSeriesExec"),
        "a nameless selector over several metrics must run a fused node: {columns:?}"
    );
    assert!(
        rows.is_empty(),
        "two metrics with equal labels holding samples in the window: {columns:?} {rows:?}"
    );

    // The same selector where the window holds no sample of any of them answers once, with the
    // selector's equality labels alone: the name the presence read kept is not a label of the
    // answer, and neither is `probe_label`, which no matcher fixes.
    let (columns, rows) = native_labelled_rows(
        promql_output(
            &instance,
            r#"absent_over_time({job="api"}[5m])"#,
            "400",
            "400",
            "1s",
        )
        .await,
    )
    .await;
    assert_eq!(
        vec![(400000, labels(&[("job", "api")]), 1.0)],
        rows,
        "{columns:?}"
    );

    // A range answers every step whose window holds nothing.
    let (columns, rows) = native_labelled_rows(
        promql_output(
            &instance,
            r#"absent_over_time({job="api"}[5m])"#,
            "400",
            "470",
            "70s",
        )
        .await,
    )
    .await;
    assert_eq!(
        vec![
            (400000, labels(&[("job", "api")]), 1.0),
            (470000, labels(&[("job", "api")]), 1.0),
        ],
        rows,
        "{columns:?}"
    );

    // A statement that compares a discovery over several metrics with one over a single metric cuts
    // both occurrences into one segment. `{job="api"}` is a union of one raw source per discovered
    // metric and `{probe_label="y"}` is a single raw source, and each is already the frame producer
    // it reads through, so the segment hands them on rather than binding them again: a second
    // binding nests a source in a source, and the physical planner refuses that, because a source's
    // child has to be the region scan it binds.
    //
    // Both metrics hold a sample in the window at 120s, so the absent side reports nothing, and the
    // addition answers alone. One-to-one matching under `on(job)` narrows the result to the labels
    // the two operands are matched by, as Prometheus does: the probe's `probe_label` is not one of
    // them, so it leaves the result. The value is the sum of the matched samples, 5 + 2.
    observer.clear();
    let (columns, rows) = native_labelled_rows(
        promql_output(
            &instance,
            r#"absent_over_time({job="api"}[5m]) or ({probe_label="y"} + on(job) absent_a)"#,
            "120",
            "120",
            "1s",
        )
        .await,
    )
    .await;
    assert!(
        observed_physical_nodes(&observer)
            .iter()
            .any(|node| node.name() == "FusedSeriesExec"),
        "the mixed discovery statement must run a fused node: {columns:?}"
    );
    assert_eq!(
        vec![(120000, labels(&[("job", "api")]), 7.0)],
        rows,
        "{columns:?}"
    );

    // At 400s the window holds no sample of either metric and both right-hand operands are older
    // than the lookback, so the absent side answers alone: one series, labelled by the matcher it
    // fixes, and nothing here supplies `probe_label`.
    let (columns, rows) = native_labelled_rows(
        promql_output(
            &instance,
            r#"absent_over_time({job="api"}[5m]) or ({probe_label="y"} + on(job) absent_a)"#,
            "400",
            "400",
            "1s",
        )
        .await,
    )
    .await;
    assert_eq!(
        vec![(400000, labels(&[("job", "api")]), 1.0)],
        rows,
        "{columns:?}"
    );
}

/// Compare value and response ordering for native routes added for common PromQL shapes.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_sort_absent_and_histogram_quantile_match_legacy() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_sort_absent_histogram_e2e",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_shapes_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table' = 'true'); \
         CREATE TABLE native_shapes (host STRING, le STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host, le)) ENGINE=metric WITH ('on_physical_table' = 'native_shapes_phy'); \
         INSERT INTO native_shapes (host, le, ts, val) VALUES \
         ('a', '1', 0, 1), ('a', '1', 10000, 2), ('b', '1', 0, 5), ('b', '1', 10000, 6), \
         ('h', '1', 0, 2), ('h', '1', 10000, 4), ('h', '1', 20000, 6), ('h', '2', 0, 4), ('h', '2', 10000, 8), ('h', '2', 20000, 12), \
         ('h', '+Inf', 0, 6), ('h', '+Inf', 10000, 12), ('h', '+Inf', 20000, 18)",
        QueryContext::arc(),
    )
    .await;

    let cases = [
        (
            "sort(native_shapes{le=\"1\"})",
            "TQL EVAL (10, 10, '1s') sort(native_shapes{le=\"1\"})",
        ),
        (
            "sort_desc(native_shapes{le=\"1\"})",
            "TQL EVAL (10, 10, '1s') sort_desc(native_shapes{le=\"1\"})",
        ),
        (
            "absent(native_shapes{host=\"missing\"})",
            "TQL EVAL (10, 10, '1s') absent(native_shapes{host=\"missing\"})",
        ),
        (
            "absent(native_shapes{host=\"a\"})",
            "TQL EVAL (10, 10, '1s') absent(native_shapes{host=\"a\"})",
        ),
        (
            "histogram_quantile(0.9, rate(native_shapes{host=\"h\"}[20s]))",
            "TQL EVAL (20, 20, '1s') histogram_quantile(0.9, rate(native_shapes{host=\"h\"}[20s]))",
        ),
    ];
    for (shape, sql) in cases {
        observer.clear();
        let native = native_tql_output(&instance, sql, true).await;
        assert!(
            observer.fused().is_some(),
            "{shape} did not run natively: {sql}"
        );
        let legacy = native_tql_output(&instance, sql, false).await;
        if shape.starts_with("histogram_quantile") {
            let collect = |output: Output| async move {
                match output.data {
                    OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
                    OutputData::RecordBatches(batches) => batches,
                    _ => unreachable!("query returns record batches"),
                }
            };
            let native = collect(native).await;
            let legacy = collect(legacy).await;
            let native_schema = native.schema();
            let legacy_schema = legacy.schema();
            let native_columns = native_schema.column_schemas();
            let legacy_columns = legacy_schema.column_schemas();
            let native_value_index = native_columns
                .iter()
                .position(|column| {
                    column.data_type == datatypes::prelude::ConcreteDataType::float64_datatype()
                })
                .unwrap();
            let legacy_value_index = legacy_columns
                .iter()
                .position(|column| {
                    column.data_type == datatypes::prelude::ConcreteDataType::float64_datatype()
                })
                .unwrap();
            let native_rows = native.iter().map(|batch| batch.num_rows()).sum::<usize>();
            let legacy_rows = legacy.iter().map(|batch| batch.num_rows()).sum::<usize>();
            assert_eq!(native_rows, legacy_rows);
            assert!(native_rows >= 1, "histogram_quantile must produce data");
            let query = PromQuery {
                query: shape.to_string(),
                ..PromQuery::default()
            };
            let QueryStatement::Promql(stmt, _) =
                QueryLanguageParser::parse_promql(&query, &QueryContext::arc()).unwrap()
            else {
                unreachable!()
            };
            let expression = stmt.expr.to_string();
            // The rows root lays its columns out as time, the expression-named value, the labels,
            // and the name last.
            assert_eq!(
                vec!["ts", expression.as_str(), "host", "__name__"],
                native_columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
                "{shape}: native boundary columns"
            );
            assert_eq!(native_value_index, 1, "{shape}: native value position");
            assert_eq!(
                legacy_columns[legacy_value_index].name,
                "prom_rate(ts_range,val,ts,Int64(20000))"
            );
            assert_eq!(
                native_columns[native_value_index].data_type,
                legacy_columns[legacy_value_index].data_type
            );
            let native_data = native
                .iter()
                .flat_map(|batch| {
                    let timestamps = batch
                        .column_by_name("ts")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let labels = cast(
                        batch.column_by_name("host").unwrap().as_ref(),
                        &DataType::Utf8,
                    )
                    .unwrap();
                    let labels = labels.as_any().downcast_ref::<StringArray>().unwrap();
                    let values = cast(
                        batch.column(native_value_index).as_ref(),
                        &DataType::Float64,
                    )
                    .unwrap();
                    let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
                    (0..batch.num_rows())
                        .map(|row| {
                            (
                                timestamps.value(row),
                                labels.value(row).to_string(),
                                values.value(row),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let legacy_data = legacy
                .iter()
                .flat_map(|batch| {
                    let timestamps = batch
                        .column_by_name("ts")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let labels = cast(
                        batch.column_by_name("host").unwrap().as_ref(),
                        &DataType::Utf8,
                    )
                    .unwrap();
                    let labels = labels.as_any().downcast_ref::<StringArray>().unwrap();
                    let values = cast(
                        batch.column(legacy_value_index).as_ref(),
                        &DataType::Float64,
                    )
                    .unwrap();
                    let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
                    (0..batch.num_rows())
                        .map(|row| {
                            (
                                timestamps.value(row),
                                labels.value(row).to_string(),
                                values.value(row),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(native_data.len(), legacy_data.len());
            for (native_row, legacy_row) in native_data.iter().zip(&legacy_data) {
                assert_eq!((native_row.0, &native_row.1), (legacy_row.0, &legacy_row.1));
                assert!(
                    (native_row.2 - legacy_row.2).abs() <= 1e-12,
                    "native value {} != legacy value {}",
                    native_row.2,
                    legacy_row.2
                );
            }
        } else {
            // `sort` and `sort_desc` report the series they rank as they found them, so their rows
            // keep the metric name; `absent` answers a value of its own for a label set it derives
            // from the matcher, and Prometheus' `absent` leaves the name out of that set. The
            // existing route has no name column at all, so the column is checked against what the
            // shape answers and left out of the comparison below.
            //
            // Both routes answer the same rows; only the one value column's header differs,
            // because the existing planner keeps the tree's field name while the native route
            // names that column after the expression. Compare what the columns carry - the
            // timestamp, the label set and the value, in response order - and the native naming.
            let reported = if shape.starts_with("absent") {
                ReportedName::Nothing
            } else {
                ReportedName::Metric("native_shapes")
            };
            let collect = |output: Output| async move {
                match output.data {
                    OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
                    OutputData::RecordBatches(batches) => batches,
                    _ => unreachable!("query returns record batches"),
                }
            };
            let rows = |batches: &common_recordbatch::RecordBatches, reported: ReportedName<'_>| {
                batches
                    .iter()
                    .flat_map(|batch| {
                        let batch = batch.df_record_batch();
                        let schema = batch.schema();
                        // The name column is checked against what the shape answers and kept out of
                        // the label set, so the rows of the two routes compare whole.
                        checked_name_column(schema.fields(), batch.columns(), reported);
                        let value_index = schema
                            .fields()
                            .iter()
                            .position(|field| field.data_type() == &DataType::Float64)
                            .expect("one float value column");
                        let timestamps = batch
                            .column_by_name("ts")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        let labels = schema
                            .fields()
                            .iter()
                            .filter(|field| {
                                !matches!(
                                    field.data_type(),
                                    DataType::Float64 | DataType::Timestamp(..)
                                ) && field.name() != "__name__"
                            })
                            .map(|field| {
                                let column = cast(
                                    batch.column_by_name(field.name()).unwrap().as_ref(),
                                    &DataType::Utf8,
                                )
                                .unwrap();
                                (
                                    field.name().clone(),
                                    column
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .unwrap()
                                        .clone(),
                                )
                            })
                            .collect::<Vec<_>>();
                        let values =
                            cast(batch.column(value_index).as_ref(), &DataType::Float64).unwrap();
                        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
                        (0..batch.num_rows())
                            .map(|row| {
                                (
                                    timestamps.value(row),
                                    labels
                                        .iter()
                                        .filter(|(_, column)| {
                                            column.is_valid(row) && !column.value(row).is_empty()
                                        })
                                        .map(|(name, column)| {
                                            (name.clone(), column.value(row).to_string())
                                        })
                                        .collect::<std::collections::BTreeMap<_, _>>(),
                                    values.value(row),
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            };
            let native = collect(native).await;
            let legacy = collect(legacy).await;
            let native_names = native
                .schema()
                .column_schemas()
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let native_rows = rows(&native, reported);
            let legacy_rows = rows(&legacy, ReportedName::NoColumn);
            assert_eq!(native_rows, legacy_rows, "{shape}; SQL: {sql}");
            if shape.starts_with("sort") {
                assert!(!native_rows.is_empty(), "{shape} must produce data: {sql}");
            }
            let legacy_labels = legacy
                .schema()
                .column_schemas()
                .iter()
                .filter(|column| {
                    !column.data_type.is_timestamp()
                        && column.data_type
                            != datatypes::prelude::ConcreteDataType::float64_datatype()
                })
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            // The native answer's columns are the time index, the expression-named value, the
            // labels the existing route also declares, and the name column it appends last.
            let mut expected_names = vec!["ts".to_string(), expression_name(shape)];
            expected_names.extend(legacy_labels);
            expected_names.push("__name__".to_string());
            assert_eq!(
                native_names, expected_names,
                "{shape}: native boundary columns"
            );
            assert!(
                native_names[2..native_names.len() - 1]
                    .iter()
                    .all(|name| matches!(name.as_str(), "host" | "le")),
                "{shape}: native boundary columns: {native_names:?}"
            );
        }
    }
}

/// Native instant selectors preserve anchored evaluation and sample timestamps in standalone.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_instant_at_uses_sample_time_and_anchor() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_instant_at_e2e",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_at (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); INSERT INTO native_at (host, ts, val) VALUES ('a', -1500, 1), ('a', 500, 2), ('a', 1500, 3), ('a', 3500, 4), ('neg', 10500, 10), ('neg', 11500, 11), ('b', -1500, 9), ('missing', 500, 8), ('missing', 1500, NULL), ('stale', 500, 7)",
        QueryContext::arc(),
    )
    .await;

    // Add the stale marker after a finite older sample so it must suppress that sample.
    let schema = vec![
        ColumnSchema {
            column_name: "host".into(),
            datatype: ColumnDataType::String as i32,
            semantic_type: SemanticType::Tag as i32,
            ..Default::default()
        },
        ColumnSchema {
            column_name: "ts".into(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
            ..Default::default()
        },
        ColumnSchema {
            column_name: "val".into(),
            datatype: ColumnDataType::Float64 as i32,
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        },
    ];
    instance
        .handle_row_inserts(
            RowInsertRequests {
                inserts: vec![RowInsertRequest {
                    table_name: "native_at".into(),
                    rows: Some(Rows {
                        schema,
                        rows: vec![Row {
                            values: vec![
                                ValueData::StringValue("stale".into()).into(),
                                ValueData::TimestampMillisecondValue(1_500).into(),
                                ValueData::F64Value(f64::from_bits(PROMETHEUS_STALE_NAN_BITS))
                                    .into(),
                            ],
                        }],
                    }),
                }],
            },
            QueryContext::arc(),
            false,
            false,
        )
        .await
        .unwrap();

    let expected_rows = |start_ms: i64, host: &str, values: &[f64]| {
        values
            .iter()
            .enumerate()
            .map(|(i, value)| (start_ms + i as i64 * 1_000, host.to_string(), *value))
            .collect::<Vec<_>>()
    };
    // Grid times and values are independently explicit: fixed anchors, offsets, and timestamp
    // functions have expected source-sample versus evaluation-grid semantics.
    let cases = vec![
        (
            "native_at{host=\"a\"} @ 1",
            0,
            4_000,
            5_000,
            "a",
            vec![2.; 5],
        ),
        (
            "native_at{host=\"a\"} @ start()",
            0,
            4_000,
            2_000,
            "a",
            vec![1.; 5],
        ),
        (
            "native_at{host=\"a\"} @ end()",
            0,
            4_000,
            5_000,
            "a",
            vec![4.; 5],
        ),
        (
            "native_at{host=\"a\"} @ 2 offset 1s",
            0,
            4_000,
            2_000,
            "a",
            vec![2.; 5],
        ),
        (
            "native_at{host=\"a\"} @ 2 offset -1s",
            0,
            4_000,
            5_000,
            "a",
            vec![3.; 5],
        ),
        (
            "native_at{host=\"a\"} @ -1.5",
            0,
            4_000,
            2_000,
            "a",
            vec![1.; 5],
        ),
        (
            "native_at{host=\"a\"} offset 1s",
            0,
            4_000,
            2_000,
            "a",
            vec![1., 1., 2., 3., 3.],
        ),
        (
            "native_at{host=\"a\"} offset -1s",
            0,
            4_000,
            2_000,
            "a",
            vec![2., 3., 3., 4., 4.],
        ),
        // At 499ms, -1500ms is 1999ms old and eligible; at 500ms, the strict lower
        // boundary excludes it. Zero lookback admits equality at the sample timestamp.
        ("native_at{host=\"b\"}", 499, 499, 2_000, "b", vec![9.]),
        ("native_at{host=\"b\"}", 500, 500, 2_000, "b", vec![]),
        ("native_at{host=\"b\"} @ -1.5", 0, 0, 0, "b", vec![9.]),
        (
            "native_at{host=\"missing\"}",
            2_000,
            2_000,
            5_000,
            "missing",
            vec![],
        ),
        (
            "timestamp(native_at{host=\"missing\"})",
            2_000,
            2_000,
            5_000,
            "missing",
            vec![],
        ),
        (
            "timestamp(abs(native_at{host=\"missing\"}))",
            2_000,
            2_000,
            5_000,
            "missing",
            vec![],
        ),
        (
            "native_at{host=\"stale\"}",
            2_000,
            2_000,
            5_000,
            "stale",
            vec![],
        ),
        (
            "timestamp(native_at{host=\"stale\"})",
            2_000,
            2_000,
            5_000,
            "stale",
            vec![],
        ),
        (
            "timestamp(abs(native_at{host=\"stale\"}))",
            2_000,
            2_000,
            5_000,
            "stale",
            vec![],
        ),
        // timestamp(raw @ t) reports original sample time (.5s); timestamp(abs()) reports
        // the evaluation-grid time at each of the five evaluation positions.
        (
            "timestamp(native_at{host=\"a\"} @ 1)",
            0,
            4_000,
            5_000,
            "a",
            vec![0.5; 5],
        ),
        (
            "timestamp(abs(native_at{host=\"a\"} @ 1))",
            0,
            4_000,
            5_000,
            "a",
            vec![0., 1., 2., 3., 4.],
        ),
        (
            "timestamp(native_at{host=\"a\"} @ 2 offset 1s)",
            2_000,
            2_000,
            5_000,
            "a",
            vec![0.5],
        ),
        (
            "timestamp(native_at{host=\"a\"} @ 2 offset -1s)",
            2_000,
            2_000,
            5_000,
            "a",
            vec![1.5],
        ),
        (
            "timestamp(native_at{host=\"neg\"} @ 11 offset -2s)",
            11_000,
            11_000,
            5_000,
            "neg",
            vec![10.5],
        ),
        // The anchored and offset-adjusted selector windows are disjoint here.
        (
            "timestamp(native_at{host=\"a\"} @ 2 offset 2s)",
            2_000,
            2_000,
            750,
            "a",
            vec![],
        ),
        (
            "timestamp((native_at{host=\"a\"} @ 2 offset -1s))",
            2_000,
            2_000,
            5_000,
            "a",
            vec![1.5],
        ),
        (
            "abs(timestamp(native_at{host=\"a\"} @ 2 offset -1s))",
            2_000,
            2_000,
            5_000,
            "a",
            vec![1.5],
        ),
        (
            "timestamp(native_at{host=\"a\"} @ end() offset -1s)",
            0,
            2_000,
            5_000,
            "a",
            vec![1.5; 3],
        ),
        (
            "timestamp(native_at{host=\"a\"} @ 2 offset 1s) + native_at{host=\"a\"} @ 2 offset 1s",
            2_000,
            2_000,
            5_000,
            "a",
            vec![2.5],
        ),
        (
            "timestamp(native_at{host=\"a\"} offset 1s)",
            2_000,
            2_000,
            5_000,
            "a",
            vec![0.5],
        ),
    ];

    for (expr, start_ms, end_ms, lookback_ms, host, values) in cases {
        observer.clear();
        let expected = expected_rows(start_ms, host, &values);
        let native = promql_query(
            instance.clone(),
            expr,
            None,
            native_query_context(true, 4),
            UNIX_EPOCH + Duration::from_millis(start_ms as u64),
            UNIX_EPOCH + Duration::from_millis(end_ms as u64),
            Duration::from_secs(1),
            Duration::from_millis(lookback_ms as u64),
        )
        .await
        .unwrap();
        let native_rows = native_result_rows(native).await;
        let plan = observer.take();
        assert_eq!(plan.name(), "FusedSeriesExec", "{expr}");
        let output_count = fused_metric_set(&plan)
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
        if expected.is_empty() {
            assert_eq!(output_count, 0, "empty native result: {expr}");
        } else {
            assert!(
                output_count > 0,
                "native fused output was not executed: {expr}"
            );
        }
        assert_eq!(native_rows, expected, "native: {expr}");

        let legacy = promql_query(
            instance.clone(),
            expr,
            None,
            native_query_context(false, 4),
            UNIX_EPOCH + Duration::from_millis(start_ms as u64),
            UNIX_EPOCH + Duration::from_millis(end_ms as u64),
            Duration::from_secs(1),
            Duration::from_millis(lookback_ms as u64),
        )
        .await
        .unwrap();
        if expr == "native_at{host=\"missing\"}" {
            // Legacy preserves the selected NULL sample row; native selectors drop all-missing rows.
            let batches = match legacy.data {
                OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
                OutputData::RecordBatches(batches) => batches,
                _ => unreachable!("TQL EVAL returns record batches"),
            };
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                1,
                "legacy missing selector must retain exactly the selected NULL row"
            );
            for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
                assert_eq!(batch.num_rows(), 1);
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
                assert_eq!(values.null_count(), 1);
                assert_eq!(timestamps.value(0), 2_000);
                assert_eq!(hosts.value(0), "missing");
            }
        } else if expr == "timestamp(native_at{host=\"neg\"} @ 11 offset -2s)" {
            // Legacy uses the shifted instant for its lookback window and selects 11.5s; the
            // anchored native selection intersects that window with the unshifted anchor.
            assert_eq!(
                native_result_rows(legacy).await,
                expected_rows(11_000, "neg", &[11.5]),
                "legacy known mismatch: {expr}"
            );
        } else {
            assert_eq!(
                native_rows,
                native_result_rows(legacy).await,
                "legacy: {expr}"
            );
        }
    }
}

/// Distinct selector scans in a binary expression retain independent bindings and bounds (#9896).
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_binary_join_uses_two_independent_scans() {
    // Enough rows per source for each independent scan to span multiple 8,192-row read batches.
    const HOSTS: usize = 4_608;
    const POINTS: i64 = 6;
    const BATCH_ROWS: usize = 512;

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
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_two_scan_binary_e2e")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    let table = "native_two_scan_pair";
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE {table} (host STRING, kind STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host, kind))"
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
            column_name: "kind".to_string(),
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
    let mut batch = Vec::with_capacity(BATCH_ROWS);
    for host in 0..HOSTS {
        for (kind, intercept, slope) in [("a", 100.0, 2.0), ("b", 1_000.0, 5.0)] {
            for point in 0..POINTS {
                batch.push(Row {
                    values: vec![
                        ValueData::StringValue(format!("h{host:04}")).into(),
                        ValueData::StringValue(kind.to_string()).into(),
                        ValueData::TimestampMillisecondValue(point * 1_000).into(),
                        ValueData::F64Value(intercept + slope * point as f64).into(),
                    ],
                });
                if batch.len() == BATCH_ROWS {
                    let rows = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_ROWS));
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

    // Distinct matcher inputs produce an independently checked noncommutative result.
    let instant_sql =
        format!("TQL EVAL (5, 5, '1s') {table}{{kind=\"a\"}} - on(host) {table}{{kind=\"b\"}}");
    observer.clear();
    let instant_rows =
        native_result_rows(native_tql_output(&instance, &instant_sql, true).await).await;
    let instant_plan = observer.take();
    assert_two_source_native_plan(&observer, &instant_plan, true);
    let instant_expected = (0..HOSTS)
        .map(|host| (5_000, format!("h{host:04}"), -915.0))
        .collect::<Vec<_>>();
    assert_eq!(instant_rows, instant_expected);
    let instant_legacy =
        native_result_rows(native_tql_output(&instance, &instant_sql, false).await).await;
    assert_eq!(instant_rows, instant_legacy);

    // A constant wrapper keeps the pair native, and the output keeps only the labels the inner
    // `on(host)` match reports.
    let wrapped_sql = format!(
        "TQL EVAL (5, 5, '1s') 100 * (1 - {table}{{kind=\"a\"}} / on(host) {table}{{kind=\"b\"}})"
    );
    observer.clear();
    let wrapped_rows =
        native_result_rows(native_tql_output(&instance, &wrapped_sql, true).await).await;
    assert_two_source_native_plan(&observer, &observer.take(), true);
    let wrapped_expected = (0..HOSTS)
        .map(|host| {
            (
                5_000,
                format!("h{host:04}"),
                100.0 * (1.0 - 110.0 / 1_025.0),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(wrapped_rows, wrapped_expected);
    let wrapped_legacy =
        native_result_rows(native_tql_output(&instance, &wrapped_sql, false).await).await;
    assert_eq!(wrapped_rows, wrapped_legacy);

    // Independent range windows: left reads (1, 5], right (1, 3] after offset.
    // Positive, distinct counter slopes make the exact binary result 2 - 5 = -3 per host.
    let sql = format!(
        "TQL EVAL (5, 5, '1s') rate({table}{{kind=\"a\"}}[4s] @ 5) - on(host) rate({table}{{kind=\"b\"}}[2s] @ 4 offset 1s)"
    );
    observer.clear();
    let native_rows = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
    let plan = observer.take();
    assert_eq!(plan.name(), "FusedSeriesExec");
    assert_two_source_native_plan(&observer, &plan, true);
    let input_samples = fused_metric_set(&plan)
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
        input_samples,
        HOSTS * 2,
        "the binary consumes one evaluated value from each operand per host"
    );
    for (metric_name, expected_samples) in [
        ("native.root.left.input_samples", HOSTS * 4),
        ("native.root.right.input_samples", HOSTS * 2),
    ] {
        let source_samples = fused_metric_set(&plan)
            .iter()
            .filter_map(|metric| match metric.value() {
                datafusion::physical_plan::metrics::MetricValue::Count { name, count }
                    if name.as_ref() == metric_name =>
                {
                    Some(count.value())
                }
                _ => None,
            })
            .sum::<usize>();
        assert_eq!(source_samples, expected_samples, "{metric_name}");
    }
    let expected = (0..HOSTS)
        .map(|host| (5_000, format!("h{host:04}"), -3.0))
        .collect::<Vec<_>>();
    assert_eq!(native_rows, expected);
    let legacy_rows = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
    assert_eq!(native_rows, legacy_rows);
}

/// `(ts, host, kind, value)` of every row, sorted, since a set result keeps `kind`.
async fn native_kind_rows(output: Output) -> Vec<(i64, String, String, f64)> {
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
        let text = |name: &str| {
            cast(
                batch.column_by_name(name).unwrap().as_ref(),
                &DataType::Utf8,
            )
            .unwrap()
        };
        let (hosts, kinds) = (text("host"), text("kind"));
        let hosts = hosts.as_any().downcast_ref::<StringArray>().unwrap();
        let kinds = kinds.as_any().downcast_ref::<StringArray>().unwrap();
        let values = batch
            .columns()
            .iter()
            .find_map(|column| column.as_any().downcast_ref::<Float64Array>())
            .expect("one value column");
        for row in 0..batch.num_rows() {
            rows.push((
                timestamps.value(row),
                hosts.value(row).to_string(),
                kinds.value(row).to_string(),
                values.value(row),
            ));
        }
    }
    rows.sort_by(|left, right| (left.0, &left.1, &left.2).cmp(&(right.0, &right.1, &right.2)));
    rows
}

/// Set operators run over both whole scans in one execution (#9896 gathers every partition of each
/// source). A right side of one series sits in one scan partition while the left side spans
/// several, so a per-partition answer would differ for `and on()` and `unless on()`.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_set_operators_read_both_scans_whole() {
    const HOSTS: usize = 4_608;
    const POINTS: i64 = 6;
    const BATCH_ROWS: usize = 512;

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
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_set_operators_e2e")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    let table = "native_set_pair";
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE {table} (host STRING, kind STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host, kind))"
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
            column_name: "kind".to_string(),
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
    let mut batch = Vec::with_capacity(BATCH_ROWS);
    for host in 0..HOSTS {
        for (kind, intercept) in [("a", 100.0), ("b", 1_000.0)] {
            for point in 0..POINTS {
                batch.push(Row {
                    values: vec![
                        ValueData::StringValue(format!("h{host:04}")).into(),
                        ValueData::StringValue(kind.to_string()).into(),
                        ValueData::TimestampMillisecondValue(point * 1_000).into(),
                        ValueData::F64Value(intercept + point as f64).into(),
                    ],
                });
                if batch.len() == BATCH_ROWS {
                    let rows = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_ROWS));
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

    let host = |host: usize| format!("h{host:04}");
    let at_five = |hosts: &mut dyn Iterator<Item = usize>, kind: &str, value: f64| {
        hosts
            .map(|index| (5_000, host(index), kind.to_string(), value))
            .collect::<Vec<_>>()
    };
    let mut union = at_five(&mut (0..10), "a", 105.0);
    union.extend(at_five(&mut (0..100), "b", 1_005.0));
    union.sort_by(|left, right| (left.0, &left.1, &left.2).cmp(&(right.0, &right.1, &right.2)));
    // `spread` marks the queries whose left scan has to span several partitions: those are the
    // ones a per-partition answer would get wrong.
    for (query, spread, expected) in [
        (
            format!("{table}{{kind=\"a\"}} and on() {table}{{kind=\"b\",host=\"h0000\"}}"),
            true,
            at_five(&mut (0..HOSTS), "a", 105.0),
        ),
        (
            format!("{table}{{kind=\"a\"}} unless on() {table}{{kind=\"b\",host=\"h0000\"}}"),
            true,
            Vec::new(),
        ),
        (
            format!("{table}{{kind=\"a\"}} unless on(host) {table}{{kind=\"b\",host=~\"h00..\"}}"),
            true,
            at_five(&mut (100..HOSTS), "a", 105.0),
        ),
        (
            format!(
                "{table}{{kind=\"a\",host=~\"h000.\"}} or {table}{{kind=\"b\",host=~\"h00..\"}}"
            ),
            false,
            union,
        ),
    ] {
        let sql = format!("TQL EVAL (5, 5, '1s') {query}");
        observer.clear();
        let native = native_kind_rows(native_tql_output(&instance, &sql, true).await).await;
        let plan = observer.two_input_fused();
        assert_eq!(plan.name(), "FusedSeriesExec", "{query}");
        let children = plan.children();
        assert_eq!(children.len(), 2, "{query}");
        let scans = observed_series_scans(&observer);
        assert_eq!(scans.len(), 2, "{query}: one scan per selector occurrence");
        // Which of the two scans is the wide one is not positional: a narrowed matcher reads a
        // single partition, so a query whose left side must be read whole spreads on one scan.
        let spreading = scans
            .iter()
            .filter(|scan| {
                let nonempty = scan
                    .metrics()
                    .unwrap()
                    .iter()
                    .filter(|metric| {
                        matches!(
                            metric.value(),
                            datafusion::physical_plan::metrics::MetricValue::OutputRows(count)
                                if count.value() > 0
                        )
                    })
                    .filter_map(|metric| metric.partition())
                    .collect::<std::collections::HashSet<_>>();
                nonempty.len() > 1
            })
            .count();
        if spread {
            assert!(
                spreading >= 1,
                "`{query}`: the wide side must read several scan partitions"
            );
        }
        for scan in &scans {
            assert_eq!(
                scan.properties().partitioning.partition_count(),
                4,
                "{query}: every scan keeps four partitions"
            );
        }
        assert_eq!(native, expected, "native: {query}");
        let legacy = native_kind_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(native, legacy, "legacy: {query}");
    }
}

/// `or` decides per step: where the left side of a host is present only its sample is kept, and
/// the right side's sample of that host fills exactly the steps where the left side is absent.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_or_fills_the_steps_the_left_side_misses() {
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_or_per_step",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    let table = "native_or_steps";
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE {table} (host STRING, kind STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, \
             PRIMARY KEY(host, kind))"
        ),
        QueryContext::arc(),
    )
    .await;
    // `h0` has a left sample only at 3s..6s; every other series has one at every second.
    let mut values = Vec::new();
    for second in 0..10 {
        if (3..=6).contains(&second) {
            values.push(format!("('h0', 'a', {}, {})", second * 1_000, 10 + second));
        }
        values.push(format!("('h0', 'b', {}, {})", second * 1_000, 100 + second));
        values.push(format!("('h1', 'a', {}, {})", second * 1_000, 20 + second));
        values.push(format!("('h1', 'b', {}, {})", second * 1_000, 200 + second));
    }
    execute_all(
        &instance,
        &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
        QueryContext::arc(),
    )
    .await;

    // A one-second lookback selects only the sample at each step.
    let sql = format!(
        "TQL EVAL (0, 9, '1s', '1s') {table}{{kind=\"a\"}} or on(host) {table}{{kind=\"b\"}}"
    );
    observer.clear();
    let native = native_kind_rows(native_tql_output(&instance, &sql, true).await).await;
    let plan = observer.two_input_fused();
    assert_eq!(plan.name(), "FusedSeriesExec");
    assert_eq!(plan.children().len(), 2);

    let mut expected = Vec::new();
    for second in 0..10_i64 {
        let ts = second * 1_000;
        let h0 = if (3..=6).contains(&second) {
            ("a", 10.0)
        } else {
            ("b", 100.0)
        };
        expected.push((ts, "h0".to_string(), h0.0.to_string(), h0.1 + second as f64));
        expected.push((ts, "h1".to_string(), "a".to_string(), 20.0 + second as f64));
    }
    assert_eq!(native, expected);
    let legacy = native_kind_rows(native_tql_output(&instance, &sql, false).await).await;
    assert_eq!(native, legacy);
}

/// Two selectors over two single-region tables run above both regions: each region answers its
/// table's raw series through its own merge scan, read across every scan partition, and the node
/// matches them on the frontend.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_cross_table_pairs_run_above_both_regions() {
    const HOSTS: usize = 4_608;
    const POINTS: i64 = 6;
    const BATCH_ROWS: usize = 512;

    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_cross_table_e2e").with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
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
    let host = |host: usize| format!("h{host:04}");
    // Two counters per host: `used` grows by 2 a second from `10 + host % 3`, `total` by 5 from 100.
    fn used(host: usize, point: i64) -> f64 {
        10.0 + (host % 3) as f64 + 2.0 * point as f64
    }
    fn total(_: usize, point: i64) -> f64 {
        100.0 + 5.0 * point as f64
    }
    let tables = [
        ("native_xt_used", used as fn(usize, i64) -> f64),
        ("native_xt_total", total),
    ];
    let mut table_ids = Vec::new();
    for (table, value) in tables {
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {table} (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, \
                 PRIMARY KEY(host))"
            ),
            QueryContext::arc(),
        )
        .await;
        let rows = (0..HOSTS)
            .flat_map(|index| {
                (0..POINTS).map(move |point| Row {
                    values: vec![
                        ValueData::StringValue(host(index)).into(),
                        ValueData::TimestampMillisecondValue(point * 1_000).into(),
                        ValueData::F64Value(value(index, point)).into(),
                    ],
                })
            })
            .collect::<Vec<_>>();
        for rows in rows.chunks(BATCH_ROWS) {
            instance
                .handle_row_inserts(
                    RowInsertRequests {
                        inserts: vec![RowInsertRequest {
                            table_name: table.to_string(),
                            rows: Some(Rows {
                                schema: schema.clone(),
                                rows: rows.to_vec(),
                            }),
                        }],
                    },
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
        let table = instance
            .catalog_manager()
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table, None)
            .await
            .unwrap()
            .unwrap();
        table_ids.push(table.table_info().table_id());
    }

    let at_five = |value: &dyn Fn(usize) -> f64, hosts: std::ops::Range<usize>| {
        hosts
            .map(|index| (5_000, host(index), value(index)))
            .collect::<Vec<_>>()
    };
    // `spread` is how many of the two sources read every host, which a scan spreads over several
    // partitions; a source narrowed to a hundred hosts may sit in one.
    for (query, spread, expected) in [
        (
            "native_xt_used / native_xt_total",
            2,
            at_five(&|index| used(index, 5) / total(index, 5), 0..HOSTS),
        ),
        (
            // Both counters are sampled at the same instants, so the extrapolated rates keep the
            // ratio of their slopes.
            "rate(native_xt_used[4s]) / on(host) rate(native_xt_total[4s])",
            2,
            at_five(&|_| 2.0 / 5.0, 0..HOSTS),
        ),
        (
            "100 * (1 - native_xt_used / native_xt_total)",
            2,
            at_five(
                &|index| 100.0 * (1.0 - used(index, 5) / total(index, 5)),
                0..HOSTS,
            ),
        ),
        (
            "native_xt_used and on(host) native_xt_total{host=~\"h00..\"}",
            1,
            at_five(&|index| used(index, 5), 0..100),
        ),
    ] {
        let sql = format!("TQL EVAL (5, 5, '1s') {query}");
        observer.clear();
        let native = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
        let legacy = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(
            native.len(),
            legacy.len(),
            "{query}: native {} rows, legacy {} rows: {native:?} vs {legacy:?}",
            native.len(),
            legacy.len()
        );
        let plan = observer.two_input_fused();
        assert_eq!(plan.name(), "FusedSeriesExec", "{query}");
        let children = plan.children();
        assert_eq!(children.len(), 2, "{query}");
        let read_tables = children
            .iter()
            .map(|child| {
                let mut node = Arc::clone(child);
                while node.downcast_ref::<MergeScanExec>().is_none() {
                    let [input] = node.children()[..] else {
                        panic!("{query}: no merge scan below the fused node: {plan:?}");
                    };
                    node = Arc::clone(input);
                }
                let merge_scan = node.downcast_ref::<MergeScanExec>().unwrap();
                let [region] = merge_scan.regions() else {
                    panic!("{query}: a single-region table reads one region");
                };
                region.table_id()
            })
            .collect::<Vec<_>>();
        assert_eq!(read_tables, table_ids, "{query}");
        // Whether a selector travels as a raw transport or inside a fused segment is a fusion
        // choice; what the query owes is one scan per occurrence and a scan wide enough to be
        // parallel, so the scans themselves are counted.
        let scans = observed_series_scans(&observer);
        assert_eq!(scans.len(), 2, "{query}: one scan per selector occurrence");
        let mut spread_sources = 0;
        for scan in &scans {
            let nonempty = scan
                .metrics()
                .unwrap()
                .iter()
                .filter(|metric| {
                    matches!(
                        metric.value(),
                        datafusion::physical_plan::metrics::MetricValue::OutputRows(count)
                            if count.value() > 0
                    )
                })
                .filter_map(|metric| metric.partition())
                .collect::<std::collections::HashSet<_>>();
            spread_sources += usize::from(nonempty.len() > 1);
        }
        assert_eq!(
            spread_sources, spread,
            "{query}: sources reading several scan partitions"
        );
        assert_eq!(native.len(), expected.len(), "{query}");
        for (actual, expected) in native.iter().zip(&expected) {
            assert_eq!((actual.0, &actual.1), (expected.0, &expected.1), "{query}");
            assert!(
                (actual.2 - expected.2).abs() <= 1e-12 * expected.2.abs().max(1.0),
                "{query}: {actual:?} != {expected:?}"
            );
        }
        let legacy = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(native, legacy, "legacy: {query}");
    }
}

/// A nameless selector reads every metric it matches, and each output row keeps its own name.
///
/// The two metrics hold the same ordinary label `job="api"` and differ only in their values, so
/// `__name__` is the one thing that tells their series apart: a plan that dropped it would report
/// one label set twice, and a plan that bound one metric's name to both rows would report a name
/// one of them does not carry. Each row's name is asserted together with its value, so a row cannot
/// pass by carrying the right name and another metric's samples.
///
/// The aggregate form of the same selector folds the names away, so it is asserted separately in
/// [`native_promql_discovery_under_an_aggregate_sums_the_discovered_metric`].
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_discovery_names_each_metric_it_reads() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_discovery_names",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    // Two logical metrics of one physical table, whose rows hold the same ordinary label: only the
    // metric name tells their series apart.
    execute_all(
        &instance,
        r#"
        CREATE TABLE native_dn_phy (job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY (job))
            ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE native_dn_alpha (job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY (job))
            ENGINE=metric WITH ("on_physical_table" = "native_dn_phy");
        CREATE TABLE native_dn_beta (job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY (job))
            ENGINE=metric WITH ("on_physical_table" = "native_dn_phy");
        INSERT INTO native_dn_alpha (job, ts, val) VALUES ('api', 0, 1);
        INSERT INTO native_dn_beta (job, ts, val) VALUES ('api', 0, 2);
        ADMIN FLUSH_TABLE('native_dn_phy');
        "#,
        QueryContext::arc(),
    )
    .await;

    observer.clear();
    let (columns, rows) =
        native_labelled_rows(discovery_query(&instance, "{job=\"api\"}").await).await;
    assert!(
        observed_physical_nodes(&observer)
            .into_iter()
            .any(|node| node.name() == "FusedSeriesExec"),
        "the discovered selector did not reach the fused node: {columns:?} {rows:?}"
    );
    let named = rows
        .iter()
        .map(|(_, labels, value)| (labels.get("__name__").cloned().unwrap_or_default(), *value))
        .collect::<Vec<_>>();
    assert_eq!(
        named,
        vec![
            ("native_dn_alpha".to_string(), 1.0),
            ("native_dn_beta".to_string(), 2.0),
        ],
        "each row reports its own metric's name and samples: {rows:?}"
    );
}

/// Both sides of `or` are planned natively when discovery finds no authorized metric, and so is an
/// empty input beside a real one under an operator that is not a set operation.
///
/// The `or` rows already read the empty input through a tree that required placement sources. The
/// arithmetic rows are the ones that pin the input binding: physical planning accepts either all
/// raw frames or all bare scans and refuses a node over a mixture, so a plan holding one empty input
/// has to bind every real input to a source of its own. Leave that out and `alpha_total` stays a
/// bare scan beside the empty frame, which physical planning cannot convert
/// (`No installed planner was able to convert the custom node`): the query errors instead of
/// answering the empty vector that every one of these arithmetic expressions evaluates to.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_zero_match_or_real_tql() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_zero_match_or_real_tql",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"
        CREATE TABLE zero_match_phy (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY (host))
            ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE alpha_total (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY (host))
            ENGINE=metric WITH ("on_physical_table" = "zero_match_phy");
        INSERT INTO alpha_total (host, ts, val) VALUES
            ('a', 0, 1), ('a', 1000, 2), ('a', 2000, 3),
            ('b', 0, 10), ('b', 1000, 20), ('b', 2000, 30);
        ADMIN FLUSH_TABLE('zero_match_phy');
        "#,
        QueryContext::arc(),
    )
    .await;

    let expected = [
        (0, "a", 1.0),
        (1000, "a", 2.0),
        (2000, "a", 3.0),
        (0, "b", 10.0),
        (1000, "b", 20.0),
        (2000, "b", 30.0),
    ];
    for (query, empty_index) in [
        (r#"{__name__=~"no_such_metric_.+"} or alpha_total"#, 0),
        (r#"alpha_total or {__name__=~"no_such_metric_.+"}"#, 1),
    ] {
        // TQL does not prepare discovery scope itself; provide the same authorized scope that
        // the Prometheus handler would provide, without comparing to the unsupported legacy path.
        let mut ctx = QueryContext::with_db_name(None);
        ctx.set_extension(QUERY_PARALLELISM_HINT, "4".to_string());
        ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, "true".to_string());
        ctx.set_metric_discovery_scope(MetricDiscoveryScope::allowed([(
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            "alpha_total".to_string(),
        )]));
        let sql = format!("TQL EVAL (0, 2, '1s', '1s') {query}");
        observer.clear();
        let output = instance
            .do_query(&sql, Arc::new(ctx))
            .await
            .remove(0)
            .unwrap();
        let (columns, rows) = native_labelled_rows(output).await;
        // A rows answer reports the statement's own columns, the per-row name column among them.
        // `alpha_total` reads one named metric, and `or` reports each side with the label set it
        // read, so every row here is a series of that metric and keeps its name while the empty
        // side adds nothing. The value is named after the statement's expression, not `val`.
        let mut expected_columns = vec![
            "__name__".to_string(),
            "host".to_string(),
            "ts".to_string(),
            expression_name(query),
        ];
        expected_columns.sort();
        assert_eq!(columns, expected_columns, "{query}");
        let actual = rows
            .iter()
            .map(|(time, labels, value)| {
                assert_eq!(labels.len(), 2, "{query}: {labels:?}");
                assert_eq!(
                    Some("alpha_total"),
                    labels.get("__name__").map(String::as_str),
                    "{query}: the left side reports its own name: {labels:?}"
                );
                (*time, labels.get("host").unwrap().as_str(), *value)
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{query}");

        let fused = observer.two_input_fused();
        let children = fused.children();
        assert!(
            find_plan_node(children[empty_index], "EmptyExec").is_some(),
            "{query}: {fused:?}"
        );
        assert_eq!(fused.output_partitioning().partition_count(), 1, "{query}");
        assert!(
            children
                .iter()
                .all(|child| child.output_partitioning().partition_count() == 1),
            "{query}: {fused:?}"
        );
        assert_eq!(observed_series_scans(&observer).len(), 1, "{query}");
    }

    // An empty input beside a real one under an arithmetic operator. Nothing above an empty
    // selector here pairs it with a sibling operand, so its layout is the statement's; what these
    // rows assert is the input binding and the answer.
    for query in [
        r#"alpha_total + {__name__=~"no_such_metric_.+"}"#,
        r#"alpha_total + on(host) {__name__=~"no_such_metric_.+"}"#,
        r#"{__name__=~"no_such_metric_.+"} + on(host) alpha_total"#,
        r#"alpha_total * on(host) group_left {__name__=~"no_such_metric_.+"}"#,
    ] {
        // TQL does not prepare discovery scope itself; provide the same authorized scope that
        // the Prometheus handler would provide, without comparing to the unsupported legacy path.
        let mut ctx = QueryContext::with_db_name(None);
        ctx.set_extension(QUERY_PARALLELISM_HINT, "4".to_string());
        ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, "true".to_string());
        ctx.set_metric_discovery_scope(MetricDiscoveryScope::allowed([(
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            "alpha_total".to_string(),
        )]));
        let sql = format!("TQL EVAL (0, 2, '1s', '1s') {query}");
        observer.clear();
        let output = instance
            .do_query(&sql, Arc::new(ctx))
            .await
            .remove(0)
            .unwrap();
        // The statement's own tree is the one to read the shape from: the observer records the
        // region-side plans too, and which of them was wrapped last says nothing about the shape
        // that answers the statement.
        let plan = observer.root_plan();
        let fused = find_plan_node(&plan, "FusedSeriesExec")
            .unwrap_or_else(|| panic!("`{query}` did not take the native route: {plan:?}"));
        // One node over the real input's scan and the empty frame: the empty input is the node's
        // sibling, and the one real selector is the only read.
        let children = fused.children();
        assert_eq!(children.len(), 2, "{query}: {fused:?}");
        assert!(
            children
                .iter()
                .any(|child| find_plan_node(child, "EmptyExec").is_some()),
            "`{query}` does not read the zero-match selector as the node's input: {fused:?}"
        );
        assert!(
            children
                .iter()
                .any(|child| find_plan_node(child, "MergeScanExec").is_some()),
            "`{query}` does not read the real input's region: {fused:?}"
        );
        // No sample on the empty side means no pair to operate on: the vector answer is empty, and
        // the node above is what answered it, not a fallback to the existing plan.
        assert!(
            native_result_rows(output).await.is_empty(),
            "{query}: an empty input answered a sample"
        );
    }
}

/// A zero-match selector is laid out by the operand it is compared against, not by the statement.
///
/// `native_local_wide` carries two fields and `native_local_narrow` one, and the empty branch is the
/// left operand of the inner `or`, the right operand of which is `native_local_narrow`. One field is
/// therefore the layout that operation compares: a layout taken from the statement's first real
/// input - `native_local_wide`, two fields - would make the two sides of `or` disagree about how
/// many fields they pair, which the compiler refuses. A refusal here leaves the statement to the
/// existing plan, which refuses the `__name__` regex in the empty branch, so the query would fail.
/// The empty branch answers no sample, which is why the answer has to be the one the same statement
/// with that branch removed gives.
///
/// A branch with no real input anywhere in it - `(Z1 or Z2)`, whose own `or` compares two zero-match
/// selectors - has no sibling inside its own operation to take a layout from. Its layout therefore
/// flows in from the operation above it, which compares that whole subtree as one operand: every
/// zero-match selector in the subtree is laid out by the first real selector of the operand it is
/// compared against. Taking the statement's layout instead gives both of them `native_local_wide`'s
/// two fields, and the outer `or` then pairs two fields against `native_local_narrow`'s one, which
/// the compiler refuses - and the existing plan refuses the `__name__` regex the branch is written
/// with, so the query fails instead of answering the control's answer.
///
/// The sibling's first selector is not its answer's layout either: `native_local_wide + on(host)
/// native_local_narrow` pairs two fields against one and keeps the shorter side's, so that operand
/// answers one field while the first selector it reads carries two. An empty subtree compared
/// against it therefore still takes a layout of two fields and the compiler refuses the operation,
/// which is why `(Z or (native_local_wide + on(host) native_local_narrow))` - and its mirror image
/// with the empty subtree on the right - is deliberately not asserted here: it falls back to the
/// existing plan and fails on the regex. Laying the empty subtree out by the sibling operand's
/// compiled answer instead of by its first selector is a separate change.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_zero_match_takes_the_layout_of_its_operand() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_zero_match_local_layout",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"
        CREATE TABLE native_local_wide (
            host STRING, ts TIMESTAMP TIME INDEX, w0 DOUBLE, w1 DOUBLE, PRIMARY KEY (host)
        );
        CREATE TABLE native_local_narrow (
            host STRING, ts TIMESTAMP TIME INDEX, n0 DOUBLE, PRIMARY KEY (host)
        );
        INSERT INTO native_local_wide VALUES ('h0', 1000, 1, 2), ('h0', 2000, 3, 4);
        INSERT INTO native_local_narrow VALUES ('h0', 1000, 10), ('h0', 2000, 20);
        ADMIN FLUSH_TABLE('native_local_wide');
        ADMIN FLUSH_TABLE('native_local_narrow');
        "#,
        QueryContext::arc(),
    )
    .await;

    let control = "native_local_wide + on(host) native_local_narrow";
    // Each query with the number of zero-match selectors it reads. Every one of them answers the
    // control's rows: a zero-match branch holds no sample, so it only has to be laid out, not read.
    let queries = [
        (control, 0usize),
        (
            r#"native_local_wide + on(host) ({__name__=~"no_such_.+"} or native_local_narrow)"#,
            1,
        ),
        // The empty subtree is the left operand of the outer `or`: its own `or` compares two
        // zero-match selectors, so both of them take the layout of the operand beside it.
        (
            r#"native_local_wide + on(host) (({__name__=~"no_such_a.+"} or {__name__=~"no_such_b.+"}) or native_local_narrow)"#,
            2,
        ),
        // The same empty subtree on the right of the outer `or`.
        (
            r#"native_local_wide + on(host) (native_local_narrow or ({__name__=~"no_such_a.+"} or {__name__=~"no_such_b.+"}))"#,
            2,
        ),
        // One zero-match selector on each side: the inner one has a real sibling of its own
        // operation, the outer one is laid out through the subtree that sibling makes real.
        (
            r#"native_local_wide + on(host) ((native_local_narrow or {__name__=~"no_such_a.+"}) or {__name__=~"no_such_b.+"})"#,
            2,
        ),
        // A non-bare empty operand: an aggregate, a unary negation or a label rewrite reads no real
        // input either, so the whole subtree takes the layout of the operand it is compared against.
        // An aggregate is also the operand that changes the shape it answers - `sum by (host)`
        // answers a field of its own - which is why the layout has to reach the selector under it
        // instead of being compared against the aggregate's answer.
        (
            r#"native_local_wide + on(host) (sum by (host) ({__name__=~"no_such_.+"}) or native_local_narrow)"#,
            1,
        ),
        (
            r#"native_local_wide + on(host) (-{__name__=~"no_such_.+"} or native_local_narrow)"#,
            1,
        ),
        (
            r#"native_local_wide + on(host) (label_replace({__name__=~"no_such_.+"}, "x", "y", "", "") or native_local_narrow)"#,
            1,
        ),
    ];
    // The nodes named `name` reachable from `plan`.
    let count_nodes = |plan: &Arc<dyn ExecutionPlan>, name: &str| {
        let mut stack = vec![Arc::clone(plan)];
        let mut count = 0;
        while let Some(node) = stack.pop() {
            count += usize::from(node.name() == name);
            stack.extend(node.children().into_iter().cloned());
        }
        count
    };
    let mut answers = Vec::new();
    for (query, empty_selectors) in queries {
        // TQL does not prepare discovery scope itself; provide the same authorized scope that the
        // Prometheus handler would provide, without comparing to the unsupported legacy path.
        let mut ctx = QueryContext::with_db_name(None);
        ctx.set_extension(QUERY_PARALLELISM_HINT, "4".to_string());
        ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, "true".to_string());
        ctx.set_metric_discovery_scope(MetricDiscoveryScope::allowed([
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
                "native_local_wide".to_string(),
            ),
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
                "native_local_narrow".to_string(),
            ),
        ]));
        let sql = format!("TQL EVAL (1, 2, '1s', '1s') {query}");
        observer.clear();
        let output = instance
            .do_query(&sql, Arc::new(ctx))
            .await
            .remove(0)
            .unwrap();
        let fused = observer
            .fused()
            .unwrap_or_else(|| panic!("`{query}` did not take the native route"));
        assert_eq!(
            empty_selectors,
            count_nodes(&fused, "EmptyExec"),
            "`{query}` does not read every zero-match selector as the node's input: {fused:?}"
        );
        if empty_selectors > 0 {
            // The empty branch reads no region, so the two real selectors are the only two reads.
            let mut region_inputs = Vec::new();
            native_region_inputs(&fused, &mut region_inputs);
            assert_eq!(
                2,
                region_inputs.len(),
                "the empty branch reads a region of its own: {fused:?}"
            );
        }
        answers.push(native_result_rows(output).await);
    }
    for ((query, empty_selectors), answer) in queries.iter().zip(&answers) {
        if *empty_selectors > 0 {
            assert_eq!(
                &answers[0], answer,
                "the zero-match branches changed the answer of `{query}`"
            );
        }
    }
    // `w0` of the wide table plus `n0` of the narrow one: the binary operation over two vectors
    // pairs fields by position and keeps the shorter side's.
    assert_eq!(
        answers[0],
        vec![
            (1000, "h0".to_string(), 11.0),
            (2000, "h0".to_string(), 23.0)
        ],
        "the fixture's answer is not the one the empty branch is compared against"
    );

    // A set operation inside a subquery: the selector walk has to keep recording set operands
    // while it descends into the subquery, otherwise the empty operand is laid out by the
    // statement's first real input (`wide`, two fields) instead of by its own sibling (`narrow`,
    // one field), and the inner `or` is refused. The statement's outer binary is the same one the
    // control uses, so the empty branch must not change the answer.
    let subquery_empty = r#"native_local_wide + on(host) avg_over_time(({__name__=~"no_such_.+"} or native_local_narrow)[2s:1s])"#;
    let subquery_control =
        r#"native_local_wide + on(host) avg_over_time(native_local_narrow[2s:1s])"#;
    let mut subquery_answers = Vec::new();
    for (query, has_empty) in [(subquery_control, false), (subquery_empty, true)] {
        let mut ctx = QueryContext::with_db_name(None);
        ctx.set_extension(QUERY_PARALLELISM_HINT, "4".to_string());
        ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, "true".to_string());
        ctx.set_metric_discovery_scope(MetricDiscoveryScope::allowed([
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
                "native_local_wide".to_string(),
            ),
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
                "native_local_narrow".to_string(),
            ),
        ]));
        let sql = format!("TQL EVAL (1, 2, '1s', '1s') {query}");
        observer.clear();
        let output = instance
            .do_query(&sql, Arc::new(ctx))
            .await
            .remove(0)
            .unwrap();
        let fused = observer
            .fused()
            .unwrap_or_else(|| panic!("`{query}` did not take the native route"));
        if has_empty {
            assert_eq!(
                1,
                count_nodes(&fused, "EmptyExec"),
                "`{query}` does not read the subquery's zero-match selector: {fused:?}"
            );
            let mut region_inputs = Vec::new();
            native_region_inputs(&fused, &mut region_inputs);
            assert_eq!(
                2,
                region_inputs.len(),
                "the empty branch reads a region of its own: {fused:?}"
            );
        }
        subquery_answers.push(native_result_rows(output).await);
    }
    assert_eq!(
        subquery_answers[0], subquery_answers[1],
        "the zero-match branch inside the subquery changed the answer"
    );
}

/// `promql` evaluated at one instant through the Prometheus handler, which is the path that
/// authorizes and plans a discovered selector.
///
/// A `TQL EVAL` statement reaches the planner through the statement executor instead, and that path
/// prepares no discovery scope, so it keeps declining a selector that names no metric. The trait is
/// named in full because this module already calls the SQL handler's `do_query`.
async fn discovery_query(instance: &Arc<Instance>, promql: &str) -> Output {
    let query = PromQuery {
        query: promql.to_string(),
        start: "0".to_string(),
        end: "0".to_string(),
        step: "1s".to_string(),
        ..PromQuery::default()
    };
    servers::prometheus_handler::PrometheusHandler::do_query(
        instance.as_ref(),
        &query,
        native_query_context(true, 4),
    )
    .await
    .unwrap()
}

fn expression_name(promql: &str) -> String {
    let query = PromQuery {
        query: promql.to_string(),
        ..PromQuery::default()
    };
    let QueryStatement::Promql(stmt, _) =
        QueryLanguageParser::parse_promql(&query, &QueryContext::arc()).unwrap()
    else {
        unreachable!()
    };
    stmt.expr.to_string()
}

/// `(time, labels, value)` of every row, sorted, whatever the columns are named: the time index is
/// the timestamp column, the value the one `Float64` column, and every other column a label that a
/// NULL or empty value leaves out. Also returns the column names.
async fn native_labelled_rows(
    output: Output,
) -> (
    Vec<String>,
    Vec<(i64, std::collections::BTreeMap<String, String>, f64)>,
) {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    let mut columns = batches
        .schema()
        .arrow_schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    columns.sort();
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let schema = batch.schema();
        let mut time = None;
        let mut value = None;
        let mut labels = Vec::new();
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            match field.data_type() {
                DataType::Timestamp(..) => {
                    time = Some(cast(column.as_ref(), &DataType::Int64).unwrap());
                }
                DataType::Float64 | DataType::Int64 => {
                    assert!(value.is_none(), "one value column: {schema:?}");
                    value = Some(cast(column.as_ref(), &DataType::Float64).unwrap());
                }
                _ => labels.push((
                    field.name().clone(),
                    cast(column.as_ref(), &DataType::Utf8).unwrap(),
                )),
            }
        }
        let time = time.expect("a time index");
        let time = time.as_any().downcast_ref::<Int64Array>().unwrap();
        let value = value.expect("a value column");
        let value = value.as_any().downcast_ref::<Float64Array>().unwrap();
        for row in 0..batch.num_rows() {
            let labels = labels
                .iter()
                .filter_map(|(name, column)| {
                    let column = column.as_any().downcast_ref::<StringArray>().unwrap();
                    (column.is_valid(row) && !column.value(row).is_empty())
                        .then(|| (name.clone(), column.value(row).to_string()))
                })
                .collect();
            rows.push((time.value(row), labels, value.value(row)));
        }
    }
    rows.sort_by(|left, right| (&left.1, left.0).cmp(&(&right.1, right.0)));
    (columns, rows)
}

/// `sum({job="api"})` over one discovered metric is planned and executed natively, and folds the
/// one series the matcher keeps.
///
/// The selector names no metric, so the frontend answers it by enumerating the schema's logical
/// metric tables: one here, on its own physical metric table. The aggregate folds that metric's
/// `api` series (2) and drops the row the matcher rejects. The observer proves the fused route is
/// what ran - a fused node was planned, and none of the observed nodes is a join, since discovery
/// enumerates the metric tables instead of joining them.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_discovery_under_an_aggregate_sums_the_discovered_metric() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_discovery_under_aggregate",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"
        CREATE TABLE discovery_phy (ts TIMESTAMP TIME INDEX, val DOUBLE)
            ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE discovery_api (job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY (job))
            ENGINE=metric WITH ("on_physical_table" = "discovery_phy");
        INSERT INTO discovery_api (job, ts, val) VALUES ('api', 0, 2), ('other', 0, 100);
        "#,
        QueryContext::arc(),
    )
    .await;

    observer.clear();
    let (columns, rows) =
        native_labelled_rows(discovery_query(&instance, r#"sum({job="api"})"#).await).await;
    let observed = observed_physical_nodes(&observer);
    assert!(
        observed.iter().any(|node| node.name() == "FusedSeriesExec"),
        "the aggregated discovery did not reach the fused node: {columns:?} {rows:?}"
    );
    assert!(
        observed.iter().all(|node| node.name() != "JoinExec"),
        "discovery joined metric tables instead of enumerating them: {columns:?} {rows:?}"
    );
    let values = rows
        .iter()
        .map(|(_, labels, value)| (labels.len(), *value))
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![(0, 2.0)],
        "sum over the discovered metric's `api` series drops the `other` row and every label: \
         {rows:?}"
    );
}

/// `sum({job="api"})` over two discovered metrics is planned and executed natively, and keeps the
/// two metrics apart when the aggregate groups by their name.
///
/// The selector names no metric, so the frontend answers it by enumerating the schema's logical
/// metric tables: two here, each on its own physical metric table. The aggregate folds both `api`
/// series (2 and 3) and drops the rows the matcher rejects; grouping by `__name__` instead reports
/// one group per metric, even though the ordinary labels of their series are identical. The
/// observer proves the fused route is what ran for both statements - a fused node was planned, and
/// none of the observed nodes is a join, since discovery enumerates the metric tables instead of
/// joining them.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_discovery_over_two_metrics_under_an_aggregate_sums_both() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_discovery_over_two_metrics_under_aggregate",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"
        CREATE TABLE discovery_two_api_phy (ts TIMESTAMP TIME INDEX, val DOUBLE)
            ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE discovery_two_api (job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY (job))
            ENGINE=metric WITH ("on_physical_table" = "discovery_two_api_phy");
        CREATE TABLE discovery_two_web_phy (ts TIMESTAMP TIME INDEX, val DOUBLE)
            ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE discovery_two_web (job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY (job))
            ENGINE=metric WITH ("on_physical_table" = "discovery_two_web_phy");
        INSERT INTO discovery_two_api (job, ts, val) VALUES ('api', 0, 2), ('other', 0, 100);
        INSERT INTO discovery_two_web (job, ts, val) VALUES ('api', 0, 3), ('other', 0, 200);
        "#,
        QueryContext::arc(),
    )
    .await;

    observer.clear();
    let (columns, rows) =
        native_labelled_rows(discovery_query(&instance, r#"sum({job="api"})"#).await).await;
    let observed = observed_physical_nodes(&observer);
    assert!(
        observed.iter().any(|node| node.name() == "FusedSeriesExec"),
        "the aggregated discovery did not reach the fused node: {columns:?} {rows:?}"
    );
    assert!(
        observed.iter().all(|node| node.name() != "JoinExec"),
        "discovery joined metric tables instead of enumerating them: {columns:?} {rows:?}"
    );
    let values = rows
        .iter()
        .map(|(_, labels, value)| (labels.len(), *value))
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![(0, 5.0)],
        "sum over both discovered metrics' `api` series drops the `other` rows and every label: \
         {rows:?}"
    );

    // Grouping by the metric name must keep the two metrics apart even though their ordinary
    // labels are identical: two groups, each named after its own metric.
    observer.clear();
    let (columns, groups) = native_labelled_rows(
        discovery_query(&instance, r#"sum by (__name__) ({job="api"})"#).await,
    )
    .await;
    let observed = observed_physical_nodes(&observer);
    assert!(
        observed.iter().any(|node| node.name() == "FusedSeriesExec"),
        "the grouped discovery did not reach the fused node: {columns:?} {groups:?}"
    );
    assert!(
        observed.iter().all(|node| node.name() != "JoinExec"),
        "discovery joined metric tables instead of enumerating them: {columns:?} {groups:?}"
    );
    // The answer's row order is a plan choice, so the groups are compared by name.
    let mut named = groups
        .iter()
        .map(|(_, labels, value)| (labels.get("__name__").cloned().unwrap_or_default(), *value))
        .collect::<Vec<_>>();
    named.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        named,
        vec![
            ("discovery_two_api".to_string(), 2.0),
            ("discovery_two_web".to_string(), 3.0),
        ],
        "each discovered metric stays its own group: {groups:?}"
    );
}

/// Two tables whose tags, field and time index are named differently, partly flushed and partly
/// still in the memtable, under both series scan modes: `or` reports the union of both tables'
/// tags with NULL where a series lacks one, and a match reports only the labels it matches on.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_cross_table_pairs_over_different_schemas() {
    common_telemetry::init_default_ut_logging();
    for series_scan_v2 in [true, false] {
        let config = mito2::config::MitoConfig {
            experimental_series_scan_v2: series_scan_v2,
            ..Default::default()
        };
        let (standalone, observer) = native_test_instance(
            GreptimeDbStandaloneBuilder::new(&format!(
                "native_promql_cross_table_schemas_{series_scan_v2}"
            ))
            .with_mito_config(config),
        )
        .await;
        let instance = standalone.fe_instance().clone();
        execute_all(
            &instance,
            "CREATE TABLE native_xs_a (host STRING, kind STRING, ts TIMESTAMP TIME INDEX, \
             val DOUBLE, PRIMARY KEY(host, kind))",
            QueryContext::arc(),
        )
        .await;
        execute_all(
            &instance,
            "CREATE TABLE native_xs_b (host STRING, az STRING, \
             greptime_timestamp TIMESTAMP TIME INDEX, greptime_value DOUBLE, \
             PRIMARY KEY(host, az))",
            QueryContext::arc(),
        )
        .await;
        // `a` is `10 * host + second`, `b` is `100 + second`. Seconds 0..5 are flushed, the rest
        // stays in the memtable.
        for (seconds, flush) in [(0..5, true), (5..10, false)] {
            let mut a = Vec::new();
            let mut b = Vec::new();
            for second in seconds {
                for host in 0..2 {
                    a.push(format!(
                        "('h{host}', 'x', {}, {})",
                        second * 1_000,
                        10 * host + second
                    ));
                    b.push(format!(
                        "('h{host}', 'z', {}, {})",
                        second * 1_000,
                        100 + second
                    ));
                }
            }
            for (table, values) in [("native_xs_a", a), ("native_xs_b", b)] {
                execute_all(
                    &instance,
                    &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
                    QueryContext::arc(),
                )
                .await;
                if flush {
                    execute_all(
                        &instance,
                        &format!("ADMIN FLUSH_TABLE('{table}')"),
                        QueryContext::arc(),
                    )
                    .await;
                }
            }
        }

        let labels = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let mut union = Vec::new();
        let mut union_legacy = Vec::new();
        let mut ratio = Vec::new();
        let mut filled = Vec::new();
        for host in 0..2_i64 {
            let name = format!("h{host}");
            for second in 3..10_i64 {
                let a = (10 * host + second) as f64;
                let b = (100 + second) as f64;
                union.push((
                    second * 1_000,
                    labels(&[("host", &name), ("kind", "x"), ("__name__", "native_xs_a")]),
                    a,
                ));
                union.push((
                    second * 1_000,
                    labels(&[("host", &name), ("az", "z"), ("__name__", "native_xs_b")]),
                    b,
                ));
                // `or` reports both operands with the label set each of them read, the metric name
                // among them. The existing route declares no name column, so its rows answer the
                // same set without that label; that is a difference of the column, not of the
                // answer, so the two are compared as the rows each route can report.
                union_legacy.push((second * 1_000, labels(&[("host", &name), ("kind", "x")]), a));
                union_legacy.push((second * 1_000, labels(&[("host", &name), ("az", "z")]), b));

                ratio.push((second * 1_000, labels(&[("host", &name)]), a / b));
                filled.push((second * 1_000, labels(&[("host", &name), ("az", "z")]), b));
            }
        }
        union.sort_by(|left, right| (&left.1, left.0).cmp(&(&right.1, right.0)));
        union_legacy.sort_by(|left, right| (&left.1, left.0).cmp(&(&right.1, right.0)));
        ratio.sort_by(|left, right| (&left.1, left.0).cmp(&(&right.1, right.0)));
        filled.sort_by(|left, right| (&left.1, left.0).cmp(&(&right.1, right.0)));
        // The existing plan fails to execute the match over two differently named time indexes
        // (it reads a tag column as the time index), so only the native answer is checked there.
        for (query, columns, expected, legacy_expected) in [
            (
                "native_xs_a or native_xs_b",
                vec!["__name__", "az", "host", "kind", "ts", "val"],
                union,
                Some(union_legacy),
            ),
            (
                "native_xs_a / on(host) native_xs_b",
                vec!["__name__", "host", "ts", "val"],
                ratio,
                None,
            ),
            // `kind` and `az` keep every pair apart, so each result is a filled right series and
            // carries `az`.
            (
                "native_xs_a + fill_left(0) native_xs_b",
                vec!["__name__", "az", "host", "kind", "ts", "val"],
                filled,
                None,
            ),
        ] {
            let context = format!("{query} (series_scan_v2={series_scan_v2})");
            let sql = format!("TQL EVAL (3, 9, '1s', '1s') {query}");
            observer.clear();
            let (native_columns, native) =
                native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
            let plan = observer.two_input_fused();
            assert_eq!(plan.name(), "FusedSeriesExec", "{context}");
            assert_eq!(plan.children().len(), 2, "{context}: fused inputs");
            assert!(
                !Arc::ptr_eq(plan.children()[0], plan.children()[1]),
                "{context}: each table has an independent frontend input"
            );
            let scans = observed_series_scans(&observer);
            assert_eq!(
                scans.len(),
                2,
                "{context}: expected one distinct SeriesScanExec per selector occurrence"
            );
            let mut expected_columns = columns
                .into_iter()
                .map(|column| {
                    if column == "val" {
                        expression_name(query)
                    } else {
                        column.to_string()
                    }
                })
                .collect::<Vec<_>>();
            expected_columns.sort();
            assert_eq!(native_columns, expected_columns, "{context}");
            assert_eq!(native.len(), expected.len(), "{context}: {native:?}");
            for (actual, expected) in native.iter().zip(&expected) {
                assert_eq!(
                    (actual.0, &actual.1),
                    (expected.0, &expected.1),
                    "{context}"
                );
                assert!(
                    (actual.2 - expected.2).abs() <= 1e-12 * expected.2.abs().max(1.0),
                    "{context}: {actual:?} != {expected:?}"
                );
            }
            if let Some(legacy_expected) = legacy_expected {
                let (_, legacy) =
                    native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
                assert_eq!(legacy_expected, legacy, "legacy: {context}");
            }
        }
    }
}

/// A tree of three selectors binds one input per occurrence: above one merge scan per selector
/// when they read three tables, in the region over three scans when they read one, and an outer
/// `on(host)` keeps only `host` of the labels the inner match reports.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_three_selector_trees_bind_every_occurrence() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_three_selectors",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    // At second `t` of host `h`: `a` is `10 * h + t`, `b` is `2 * t` and `c` is `100 + h`, each in
    // its own table and as kinds `x`, `y` and `z` of one table.
    fn a(host: i64, second: i64) -> i64 {
        10 * host + second
    }
    fn b(_: i64, second: i64) -> i64 {
        2 * second
    }
    fn c(host: i64, _: i64) -> i64 {
        100 + host
    }
    let values = [
        ("native_3_a", "x", a as fn(i64, i64) -> i64),
        ("native_3_b", "y", b),
        ("native_3_c", "z", c),
    ];
    let mut shared = Vec::new();
    for (table, kind, value) in values {
        execute_all(
            &instance,
            &format!(
                "CREATE TABLE {table} (host STRING, kind STRING, ts TIMESTAMP TIME INDEX, \
                 val DOUBLE, PRIMARY KEY(host, kind))"
            ),
            QueryContext::arc(),
        )
        .await;
        let mut rows = Vec::new();
        for host in 0..3 {
            for second in 0..6 {
                let sample = value(host, second);
                rows.push(format!("('h{host}', 'x', {}, {sample})", second * 1_000));
                shared.push(format!(
                    "('h{host}', '{kind}', {}, {sample})",
                    second * 1_000
                ));
            }
        }
        execute_all(
            &instance,
            &format!("INSERT INTO {table} VALUES {}", rows.join(", ")),
            QueryContext::arc(),
        )
        .await;
    }
    execute_all(
        &instance,
        "CREATE TABLE native_3_s (host STRING, kind STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, \
         PRIMARY KEY(host, kind))",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        &format!("INSERT INTO native_3_s VALUES {}", shared.join(", ")),
        QueryContext::arc(),
    )
    .await;

    let at_five = |value: fn(f64) -> f64| {
        (0..3)
            .map(|host| (5_000, format!("h{host}"), value(host as f64)))
            .collect::<Vec<_>>()
    };
    for (query, columns, expected, expected_source_paths) in [
        (
            "native_3_a + native_3_b + native_3_c",
            vec!["__name__", "host", "kind", "ts", "val"],
            at_five(|h| 10.0 * h + 5.0 + 10.0 + (100.0 + h)),
            3,
        ),
        (
            "(native_3_a - native_3_b) / on(host) native_3_c",
            vec!["__name__", "host", "ts", "val"],
            at_five(|h| (10.0 * h + 5.0 - 10.0) / (100.0 + h)),
            3,
        ),
        (
            "native_3_s{kind=\"x\"} - on(host) native_3_s{kind=\"y\"} + on(host) \
             native_3_s{kind=\"z\"}",
            vec!["__name__", "host", "ts", "val"],
            at_five(|h| 10.0 * h + 5.0 - 10.0 + (100.0 + h)),
            3,
        ),
        (
            // `or` reports both sides; a TQL result reports the statement's own columns, so the
            // per-row name column a Prometheus response reads is not one of them.
            "native_3_a{host=\"h0\"} and native_3_b or native_3_c",
            vec!["__name__", "host", "kind", "ts", "val"],
            at_five(|h| if h == 0.0 { 5.0 } else { 100.0 + h }),
            3,
        ),
        (
            "(native_3_a{host=\"h0\"} or native_3_b) + native_3_c",
            vec!["__name__", "host", "kind", "ts", "val"],
            at_five(|h| if h == 0.0 { 5.0 } else { 10.0 } + (100.0 + h)),
            3,
        ),
    ] {
        let sql = format!("TQL EVAL (5, 5, '1s') {query}");
        observer.clear();
        let native = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
        // Every recorded plan is inspected, not one chosen subtree: the frontend tree and the
        // region trees are separate, and a legacy join or a collapsed route can sit in either.
        let observed = observed_physical_nodes(&observer);
        assert!(
            observed.iter().any(|node| node.name() == "FusedSeriesExec"),
            "{query}: no native fused node"
        );
        let mut source_routes = Vec::new();
        for node in &observed {
            assert!(
                !node.name().contains("HashJoin"),
                "legacy join in {query}: {node:?}"
            );
            if node.name() == "SeriesRepartitionExec" {
                source_routes.push(node.output_partitioning().partition_count());
            }
        }
        let scans = observed_series_scans(&observer);
        assert_eq!(
            scans.len(),
            expected_source_paths,
            "{query}: observed {} distinct SeriesScanExec nodes; expected one per selector occurrence",
            scans.len()
        );
        // A region hands its result back as a single stream, so a width of one is that exchange.
        // Routing itself must stay four-way: a two or three wide route is a partial collapse.
        assert!(
            source_routes.iter().all(|&width| width == 1 || width == 4),
            "{query}: {source_routes:?}"
        );
        // The root partition count is not an invariant: a set operator may legitimately gather
        // its result into one stream at the end. Scan and routing parallelism are asserted here.
        for scan in &scans {
            assert_eq!(
                scan.properties().partitioning.partition_count(),
                4,
                "{query}: every selector scan keeps four partitions"
            );
        }
        // Raw transports are an execution detail of a fused segment, not a per-occurrence
        // invariant: what a selector occurrence must own is exactly one scan, asserted above, and
        // the native answer must equal the legacy one, asserted below. Only duplicates are wrong.
        let raw_sources_seen = observer.take_raw_sources();
        let distinct_raw_sources = observed_physical_nodes(&observer)
            .into_iter()
            .filter(|node| node.name() == "RawSeriesSourceExec")
            .count();
        assert_eq!(
            distinct_raw_sources,
            raw_sources_seen.len(),
            "{query}: one raw source node per recorded transport"
        );
        let mut plan_columns = observer
            .root_plan()
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        plan_columns.sort();
        let expression_query = PromQuery {
            query: query.to_string(),
            ..PromQuery::default()
        };
        let QueryStatement::Promql(stmt, _) =
            QueryLanguageParser::parse_promql(&expression_query, &QueryContext::arc()).unwrap()
        else {
            unreachable!()
        };
        let mut expected_columns = columns
            .into_iter()
            .map(|column| {
                if column == "val" {
                    stmt.expr.to_string()
                } else {
                    column.to_string()
                }
            })
            .collect::<Vec<_>>();
        expected_columns.sort();
        assert_eq!(plan_columns, expected_columns, "{query}");
        assert_eq!(native, expected, "{query}");
        let legacy = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(native, legacy, "legacy: {query}");
    }
}

/// A comparison between two tables runs above both regions: without `bool` it keeps the left
/// sample where the comparison holds, with `bool` it reports 1 or 0 at every matched point, and
/// `on(host)` narrows the labels to `host` either way.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_cross_table_comparisons() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_cross_table_comparisons",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_xc_a (host STRING, kind STRING, ts TIMESTAMP TIME INDEX, \
         val DOUBLE, PRIMARY KEY(host, kind))",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        "CREATE TABLE native_xc_b (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, \
         PRIMARY KEY(host))",
        QueryContext::arc(),
    )
    .await;
    // `a` is the second, `b` is `2 + host`: `a` passes `b` at second 3 on h0 and 4 on h1.
    let mut a = Vec::new();
    let mut b = Vec::new();
    for second in 0..6 {
        for host in 0..2 {
            a.push(format!("('h{host}', 'x', {}, {second})", second * 1_000));
            b.push(format!("('h{host}', {}, {})", second * 1_000, 2 + host));
        }
    }
    for (table, values) in [("native_xc_a", a), ("native_xc_b", b)] {
        execute_all(
            &instance,
            &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
            QueryContext::arc(),
        )
        .await;
    }

    let mut filtered = Vec::new();
    let mut bools = Vec::new();
    for host in 0..2_i64 {
        let labels = std::collections::BTreeMap::from([("host".to_string(), format!("h{host}"))]);
        for second in 0..6_i64 {
            let holds = second > 2 + host;
            if holds {
                filtered.push((second * 1_000, labels.clone(), second as f64));
            }
            let equal = second == 2 + host;
            bools.push((
                second * 1_000,
                labels.clone(),
                if equal { 1.0 } else { 0.0 },
            ));
        }
    }
    for (query, expected) in [
        ("native_xc_a > on(host) native_xc_b", filtered),
        ("native_xc_a == bool on(host) native_xc_b", bools),
    ] {
        let sql = format!("TQL EVAL (0, 5, '1s') {query}");
        observer.clear();
        let (_, native) =
            native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
        let plan = observer.take();
        assert_eq!(plan.name(), "FusedSeriesExec", "{query}");
        assert_eq!(plan.children().len(), 2, "{query}");
        assert_eq!(observer.take_raw_sources().len(), 2, "{query}");
        assert_eq!(native, expected, "{query}");
        let (_, legacy) =
            native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(legacy, expected, "legacy: {query}");
    }
}

/// `info()` over a base table and a `target_info` table whose time index and field are named
/// differently, answered natively above both regions with Prometheus' answer, which the existing
/// plan does not give: it has no `info`.
///
/// `target_info` holds `version="v1"` for `a` from 0s to 4s and `version="v2"` from 6s, and `v1` for
/// `b` throughout; `c` has none. With a 2s lookback, `a` reads `v1` up to 5s and `v2` from 6s.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_info_enriches_from_the_info_table() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) =
        native_test_instance(GreptimeDbStandaloneBuilder::new("native_promql_info")).await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_info_up (instance STRING, job STRING, ts TIMESTAMP TIME INDEX, \
         val DOUBLE, PRIMARY KEY(instance, job))",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        "CREATE TABLE target_info (instance STRING, job STRING, \"version\" STRING, \
         greptime_timestamp TIMESTAMP TIME INDEX, greptime_value DOUBLE, \
         PRIMARY KEY(instance, job, \"version\"))",
        QueryContext::arc(),
    )
    .await;
    let mut up = Vec::new();
    let mut info = Vec::new();
    for second in 0..10_i64 {
        for (target, job, base) in [("a", "j", 0), ("b", "j", 100), ("c", "k", 200)] {
            up.push(format!(
                "('{target}', '{job}', {}, {})",
                second * 1_000,
                base + second
            ));
        }
        let a_version = match second {
            0..=4 => Some("v1"),
            5 => None,
            _ => Some("v2"),
        };
        for (target, version) in [("a", a_version), ("b", Some("v1"))] {
            if let Some(version) = version {
                info.push(format!(
                    "('{target}', 'j', '{version}', {}, 1)",
                    second * 1_000
                ));
            }
        }
    }
    for (table, values) in [("native_info_up", up), ("target_info", info)] {
        execute_all(
            &instance,
            &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
            QueryContext::arc(),
        )
        .await;
    }

    let row = |second: i64, labels: &[(&str, &str)], value: i64| {
        (
            second * 1_000,
            labels
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect::<std::collections::BTreeMap<_, _>>(),
            value as f64,
        )
    };
    // `info` enriches the base series' label set and reports the series it read it from, so every
    // row keeps the base metric's name.
    let a = |version| {
        [
            ("instance", "a"),
            ("job", "j"),
            ("version", version),
            ("__name__", "native_info_up"),
        ]
    };
    let b = [
        ("instance", "b"),
        ("job", "j"),
        ("version", "v1"),
        ("__name__", "native_info_up"),
    ];
    let c = [
        ("instance", "c"),
        ("job", "k"),
        ("__name__", "native_info_up"),
    ];
    // `a` takes the version current at each step, `b` keeps `v1`, and `c`, which no info series
    // matches, passes through unless the selector requires a version.
    let mut enriched = Vec::new();
    let mut v1_only = Vec::new();
    let mut shifted = Vec::new();
    for second in 3..10_i64 {
        let version = if second <= 5 { "v1" } else { "v2" };
        enriched.push(row(second, &a(version), second));
        enriched.push(row(second, &b, 100 + second));
        enriched.push(row(second, &c, 200 + second));
        if second <= 5 {
            v1_only.push(row(second, &a("v1"), second));
        }
        v1_only.push(row(second, &b, 100 + second));
        // An offset moves the info series along with the base: each step reads both a second back.
        let version = if second <= 6 { "v1" } else { "v2" };
        shifted.push(row(second, &a(version), second - 1));
        shifted.push(row(second, &b, 99 + second));
        shifted.push(row(second, &c, 199 + second));
    }
    for (query, columns, mut expected) in [
        (
            "info(native_info_up)",
            vec!["__name__", "instance", "job", "ts", "val", "version"],
            enriched,
        ),
        (
            "info(native_info_up, {version=\"v1\"})",
            vec!["__name__", "instance", "job", "ts", "val", "version"],
            v1_only,
        ),
        (
            "info(native_info_up offset 1s)",
            vec!["__name__", "instance", "job", "ts", "val", "version"],
            shifted,
        ),
    ] {
        expected.sort_by(|left, right| (&left.1, left.0).cmp(&(&right.1, right.0)));
        let sql = format!("TQL EVAL (3, 9, '1s', '2s') {query}");
        observer.clear();
        let (native_columns, native) =
            native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
        let plan = observer.take();
        assert_eq!(plan.name(), "FusedSeriesExec", "{query}");
        assert_eq!(plan.children().len(), 2, "{query}");
        assert_eq!(observer.take_raw_sources().len(), 2, "{query}");
        let mut expected_columns = columns
            .into_iter()
            .map(|column| {
                if column == "val" {
                    expression_name(query)
                } else {
                    column.to_string()
                }
            })
            .collect::<Vec<_>>();
        expected_columns.sort();
        assert_eq!(native_columns, expected_columns, "{query}");
        assert_eq!(native, expected, "{query}");

        // The existing plan refuses `info`: "Unsupported expr type: info" for one argument, and
        // "Cannot accept multiple vector as function input" for two.
        let legacy = instance
            .do_query(&sql, native_query_context(false, 4))
            .await
            .remove(0);
        assert!(
            legacy.is_err(),
            "{query}: the existing plan answered `info`"
        );
    }
}

/// A window over a table partitioned on a column that is not a tag sees the whole series.
///
/// Such a table splits one series across regions, so a per-region prefix evaluates `rate` over a
/// fragment and loses every window that straddles the boundary. The cut therefore has to keep each
/// occurrence's raw source and run the prefix above the reconciliation, exactly as the remote route
/// does. The fixture puts the two samples of one series on either side of the boundary and compares
/// the native answer with the existing plan's.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_window_over_a_time_partitioned_table_keeps_whole_series() {
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_time_partitioned_window",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_time_partitioned (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, \
         PRIMARY KEY(host)) PARTITION ON COLUMNS (ts) (ts < '1970-01-01 00:02:30', \
         ts >= '1970-01-01 00:02:30')",
        QueryContext::arc(),
    )
    .await;
    // One series, one sample in each region: the second is 240s after the first, and the boundary
    // sits between them, so neither region holds a complete window on its own.
    execute_all(
        &instance,
        "INSERT INTO native_time_partitioned VALUES ('h0', 60000, 60), ('h0', 240000, 240)",
        QueryContext::arc(),
    )
    .await;
    for query in [
        "rate(native_time_partitioned[5m]) + rate(native_time_partitioned[5m])",
        "(rate(native_time_partitioned[5m]) + rate(native_time_partitioned[5m])) \
         + rate(native_time_partitioned[5m])",
    ] {
        let sql = format!("TQL EVAL (300, 300, '1m') {query}");
        observer.clear();
        let native = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
        let legacy = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
        assert!(
            !native.is_empty(),
            "{query}: the native plan answered nothing"
        );
        assert_eq!(
            native, legacy,
            "{query}: the native plan must match the existing one"
        );
        let root = observer.root_plan();
        let mut inputs = Vec::new();
        native_region_inputs(&root, &mut inputs);
        assert!(
            !inputs.is_empty(),
            "{query}: the window runs above the region reads: {:?}",
            display_plan_tree(&root)
        );
        // A fallback plan also produces the existing plan's rows, so the admission has to be
        // asserted separately: the frontend tree must carry a fused node.
        assert!(
            find_plan_node(&root, "FusedSeriesExec").is_some(),
            "{query}: the tree must run natively, not on the existing plan: {:?}",
            display_plan_tree(&root)
        );
    }
}

/// A query over two selectors of one partitioned table matches above the regions.
///
/// A fused node over the bare scans of such a table has no placement in a region — the dist
/// analyzer refuses it with "only accept plan with at most one child" — so each selector
/// occurrence reads that table through its own raw region input instead. Every input is one read of
/// the whole table, and the matching the query asks for runs once above them, over every region's
/// contribution: two inputs for a two-selector query, three for a three-selector one.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_two_selectors_on_a_partitioned_table_match_above_independent_regions() {
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_partitioned_pair",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_partitioned_pair (host STRING, kind STRING, ts TIMESTAMP TIME INDEX, \
         val DOUBLE, PRIMARY KEY(host, kind)) PARTITION ON COLUMNS (host) (host < 'h1', host >= 'h1')",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        "INSERT INTO native_partitioned_pair VALUES ('h0', 'a', 5000, 1), ('h0', 'b', 5000, 2), \
         ('h2', 'a', 5000, 3), ('h2', 'b', 5000, 4)",
        QueryContext::arc(),
    )
    .await;
    for (query, expected, sources) in [
        (
            "native_partitioned_pair{kind=\"a\"} and on(host) native_partitioned_pair{kind=\"b\",host=\"h0\"}",
            vec![(5_000, "h0".to_string(), 1.0)],
            2,
        ),
        (
            "native_partitioned_pair{kind=\"a\"} - on(host) native_partitioned_pair{kind=\"b\"}",
            vec![
                (5_000, "h0".to_string(), -1.0),
                (5_000, "h2".to_string(), -1.0),
            ],
            2,
        ),
        // Three selectors: the inner pair matches above its own two region inputs, under the outer
        // match.
        (
            "native_partitioned_pair{kind=\"a\"} - on(host) native_partitioned_pair{kind=\"b\"} \
             + on(host) native_partitioned_pair{kind=\"a\"}",
            vec![
                (5_000, "h0".to_string(), 0.0),
                (5_000, "h2".to_string(), 2.0),
            ],
            3,
        ),
    ] {
        let sql = format!("TQL EVAL (5, 5, '1s') {query}");
        observer.clear();
        let native = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
        // The frontend's own tree: the fused node above the regions, not the region-side plan the
        // observer also wraps.
        let root = observer.root_plan();
        let mut inputs = Vec::new();
        native_region_inputs(&root, &mut inputs);
        assert_eq!(
            sources,
            inputs.len(),
            "`{query}` reads one region input per selector occurrence: {:?}",
            display_plan_tree(&root)
        );
        // The values alone cannot tell native matching from a legacy frontend join that reads the
        // same regions, so require the native binary in the frontend tree.
        assert!(
            find_plan_node(&root, "FusedSeriesExec").is_some(),
            "`{query}` must match natively above its region inputs: {:?}",
            display_plan_tree(&root)
        );
        assert_eq!(native, expected, "{query}");
        let legacy = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(native, legacy, "{query}");
    }

    // A rewrite that overwrites the partition column collapses `host` on two series that live in
    // different regions of `native_partitioned_pair`: `h0` and `h2` become one label set with two
    // values at the same evaluation point, which is the error Prometheus reports. The read is not
    // confined to one region, so the canonical boundary is placed above both of them - each region
    // reads the table through its own raw source, the producer merges what one region holds, and
    // the router collects the pieces of the label set from every region - and that collection is
    // where the two values meet.
    let collapsing = r#"label_replace(native_partitioned_pair, "host", "same", "host", ".*")"#;
    let collapsing_sql = format!("TQL EVAL (5, 5, '1s') {collapsing}");
    observer.clear();
    let native = instance
        .do_query(&collapsing_sql, native_query_context(true, 4))
        .await
        .remove(0);
    let fused = observer
        .fused()
        .unwrap_or_else(|| panic!("a collapsing rewrite over a partitioned table runs natively"));
    assert_eq!(
        fused.name(),
        "FusedSeriesExec",
        "the frontend reads the boundary's frames: {}",
        display_plan_tree(&fused)
    );
    let shape = native_plan_shape(&observer.root_plan());
    assert_eq!(
        shape.regions_per_scan,
        vec![2],
        "the boundary reads both declared host ranges: {:?}",
        display_plan_tree(&observer.root_plan())
    );
    assert!(
        canonical_router(&observer).is_some(),
        "the canonical router collects the pieces of the label set from both regions: {}",
        display_plan_tree(&observer.root_plan())
    );
    let native_error = match native {
        Ok(output) => collect_error(output).await,
        Err(error) => error.to_string(),
    };
    assert!(
        native_error.contains("cannot contain metrics with the same labelset"),
        "the native route answers this shape and reports the overlap: {native_error}"
    );
    let legacy = instance
        .do_query(&collapsing_sql, native_query_context(false, 4))
        .await
        .remove(0);
    let legacy_error = match legacy {
        Ok(output) => collect_error(output).await,
        Err(error) => error.to_string(),
    };
    assert!(
        legacy_error.contains("cannot contain metrics with the same labelset"),
        "the native setting answers what the existing planner answers: {legacy_error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn native_duplicate_evidence_keeps_late_grid_point_on_grid() {
    common_telemetry::init_default_ut_logging();
    let (standalone, _) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_duplicate_evidence_late_point",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_dup_phy (ts TIMESTAMP TIME INDEX, val DOUBLE)
            ENGINE=metric WITH ("physical_metric_table" = "true");
           CREATE TABLE native_dup_left (match_key STRING, ts TIMESTAMP TIME INDEX, val DOUBLE,
            PRIMARY KEY (match_key)) ENGINE=metric WITH ("on_physical_table" = 'native_dup_phy');
           CREATE TABLE native_dup_right (match_key STRING, variant STRING, ts TIMESTAMP TIME INDEX,
            val DOUBLE, PRIMARY KEY (match_key, variant))
            ENGINE=metric WITH ("on_physical_table" = 'native_dup_phy');
           CREATE TABLE native_dup_empty (match_key STRING, ts TIMESTAMP TIME INDEX, val DOUBLE,
            PRIMARY KEY (match_key)) ENGINE=metric WITH ("on_physical_table" = 'native_dup_phy');
           INSERT INTO native_dup_left (match_key, ts, val) VALUES ('k', 4000, 1);
           INSERT INTO native_dup_right (match_key, variant, ts, val) VALUES ('k', 'a', 4000, 2), ('k', 'b', 4000, 3);"#,
        QueryContext::arc(),
    )
    .await;

    // TQL range 0..4 seconds at 1 second steps has 5 grid points. The duplicate at index 4
    // verifies that duplicate evidence is sized to the binary grid, not the output partition count.
    let duplicate_sql =
        "TQL EVAL (0, 4, '1s') native_dup_left + on(match_key) group_left() native_dup_right";
    let output = instance
        .do_query(duplicate_sql, native_query_context(true, 2))
        .await
        .remove(0)
        .expect("query should produce a stream");
    let error = match output.data {
        OutputData::Stream(stream) => collect_batches(stream)
            .await
            .expect_err("late duplicate must fail while executing the native query"),
        _ => panic!("TQL EVAL returns a stream"),
    };
    let message = error.to_string();
    assert!(
        message.contains("found duplicate series for the match group"),
        "{message}"
    );
    assert!(message.contains("{match_key=\"k\"}"), "{message}");
    assert!(message.contains("on the right hand-side"), "{message}");

    // The same duplicate right side is harmless where the matching left side is absent.
    let control_sql =
        "TQL EVAL (0, 4, '1s') native_dup_empty + on(match_key) group_left() native_dup_right";
    let output = instance
        .do_query(control_sql, native_query_context(true, 2))
        .await
        .remove(0)
        .expect("duplicate group is ineligible while the left side is absent");
    let _ = native_result_rows(output).await;
}

/// A set operator over a two-field table keeps the existing plan, which refuses it: neither
/// Prometheus nor that plan defines per-field presence.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_multi_field_set_operation_keeps_the_legacy_plan() {
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_multi_field_set",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_two_fields (host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX, \
         a DOUBLE, b DOUBLE)",
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        "INSERT INTO native_two_fields VALUES ('h0', 5000, 1, 2)",
        QueryContext::arc(),
    )
    .await;
    let sql = "TQL EVAL (5, 5, '1s') native_two_fields and native_two_fields";
    observer.clear();
    let native = instance
        .do_query(sql, native_query_context(true, 4))
        .await
        .remove(0);
    assert!(
        observer.fused().is_none(),
        "the set operation was planned natively"
    );
    let legacy = instance
        .do_query(sql, native_query_context(false, 4))
        .await
        .remove(0);
    let (Err(native), Err(legacy)) = (native, legacy) else {
        panic!("the existing plan answered a multi-field set operation");
    };
    assert_eq!(native.to_string(), legacy.to_string());
}

/// Repeated selectors retain correct binary results while using independent physical scans.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_repeated_selector_binary_uses_independent_scans() {
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
    let plan = observer.two_input_fused();
    assert_eq!(plan.name(), "FusedSeriesExec");
    assert_eq!(plan.children().len(), 2);
    let source_widths = plan
        .children()
        .iter()
        .map(|source| source.output_partitioning().partition_count())
        .collect::<Vec<_>>();
    assert_eq!(
        source_widths,
        [4, 4],
        "each occurrence routes independently"
    );
    assert!(!Arc::ptr_eq(plan.children()[0], plan.children()[1]));
    let scans = observed_series_scans(&observer);
    assert_eq!(scans.len(), 2, "two distinct scans for repeated selector");
    for (index, scan) in scans.iter().enumerate() {
        let metrics = scan.metrics().expect("series scan metrics");
        assert!(
            metrics.output_rows().expect("scan output rows") > 0,
            "scan {index}"
        );
        // One returned scan stream per partition, empty ones included: an empty stream still
        // registers a zero-valued `OutputRows` entry, so filtering by value would hide a missing
        // or duplicated execution.
        let mut executed_partitions = metrics
            .iter()
            .filter_map(|metric| match metric.value() {
                datafusion::physical_plan::metrics::MetricValue::OutputRows(_) => {
                    metric.partition()
                }
                _ => None,
            })
            .collect::<Vec<usize>>();
        executed_partitions.sort_unstable();
        assert_eq!(
            executed_partitions,
            vec![0, 1, 2, 3],
            "scan {index} must execute exactly once on every partition"
        );
    }
    assert_eq!(
        observer.take_raw_sources().len(),
        0,
        "same-table repeated selectors use no cross-table raw sources"
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
    let direct_plan = observer.two_input_fused();
    assert_eq!(direct_plan.name(), "FusedSeriesExec");
    assert_eq!(
        observed_series_scans(&observer).len(),
        2,
        "direct repeated selector has two distinct scans"
    );
    let direct_expected = (0..POINTS_PER_HOST)
        .flat_map(|point| {
            (0..HOSTS).map(move |host| (point * 1_000, host_name(host), value(host, point) * 2.0))
        })
        .collect::<Vec<_>>();
    assert_eq!(direct_rows, direct_expected);
    let direct_legacy =
        native_result_rows(native_tql_output(&instance, &direct_sql, false).await).await;
    assert_eq!(direct_rows, direct_legacy);

    // A single selector with an equality matcher and an explicit range anchor/offset must push
    // down both identity and time bounds. For h000, values equal the sample index; the first
    // counter sample is zero, so the range-start extrapolation is zero and rate is 2 / 3 per second.
    let bounded_sql =
        format!("TQL EVAL (4, 4, '1s') rate({table}{{host=\"h000\"}}[3s] @ 3 offset 1s)");
    observer.clear();
    let bounded_rows =
        native_result_rows(native_tql_output(&instance, &bounded_sql, true).await).await;
    let bounded_plan = observer.take();
    assert_eq!(bounded_plan.name(), "FusedSeriesExec");
    assert_eq!(bounded_rows, vec![(4_000, "h000".to_string(), 2.0 / 3.0)]);
    let legacy_bounded_rows =
        native_result_rows(native_tql_output(&instance, &bounded_sql, false).await).await;
    assert_eq!(bounded_rows, legacy_bounded_rows);
    let child = bounded_plan.children()[0];
    let child = child
        .downcast_ref::<CooperativeExec>()
        .map_or(child, |cooperative| cooperative.input());
    let region_scan = region_series_scan(&observer);
    let series_scan = child
        .downcast_ref::<SeriesScanExec>()
        .or(region_scan.as_deref())
        .unwrap_or_else(|| {
            panic!(
                "the statement reads no prepared series scan: child={child:?}; plan={bounded_plan:?}"
            )
        });
    let scan_frame_rows = series_scan
        .metrics()
        .expect("series scan metrics")
        .output_rows()
        .expect("series scan reports output rows");
    assert_eq!(
        scan_frame_rows, 1,
        "the anchored range returns one packed raw-series frame"
    );
    let root_values = fused_metric_set(&bounded_plan)
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
    let root_input_samples = fused_metric_set(&bounded_plan)
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

    // Different bounds/offsets remain independent: h000 at t=1 is 1, while the
    // one-second-offset occurrence reads t=0, so the exact answer is 1 - (0 * 2) = 1.
    let different_bounds = format!(
        "TQL EVAL (1, 1, '1s') {table}{{host=\"h000\"}} - on(host) ({table}{{host=\"h000\"}} offset 1s * 2)"
    );
    observer.clear();
    let different_bounds_rows =
        native_result_rows(native_tql_output(&instance, &different_bounds, true).await).await;
    let different_bounds_plan = observer.two_input_fused();
    assert_two_source_native_plan(&observer, &different_bounds_plan, false);
    assert_eq!(
        different_bounds_rows,
        vec![(1_000, "h000".to_string(), 1.0)]
    );
    let different_bounds_legacy =
        native_result_rows(native_tql_output(&instance, &different_bounds, false).await).await;
    assert_eq!(different_bounds_rows, different_bounds_legacy);

    // on() drops host from the output labels; inspect that shape directly instead of using the
    // host-requiring native_result_rows helper.
    let on_empty_sql = format!(
        r#"TQL EVAL (1, 1, '1s') {table}{{host="h001"}} - on() ({table}{{host="h001"}} * 2)"#
    );
    /// The evaluation timestamp and the one value of each row. `declared_name` says whether this
    /// route's rows layout declares the per-row name column: the native route does, and the
    /// existing plan answers the same rows without it.
    async fn unlabeled_rows(output: Output, declared_name: bool) -> Vec<(i64, f64)> {
        let batches = match output.data {
            OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
            OutputData::RecordBatches(batches) => batches,
            _ => unreachable!("TQL EVAL returns record batches"),
        };
        // The rows root always declares the per-row metric-name column after the labels, so the
        // answer of a computed value is timestamp, value and that one column - which names nothing
        // here, because a subtraction answers a value of its own and `on()` keeps no labels.
        let schema = batches.schema();
        let schema = schema.arrow_schema();
        assert_eq!(
            schema.fields().len(),
            if declared_name { 3 } else { 2 },
            "timestamp, value, and the name column when this route declares one"
        );
        assert!(schema.field_with_name("host").is_err());
        if declared_name {
            let name = schema
                .field_with_name("__name__")
                .expect("the rows root declares the metric-name column");
            assert!(datatypes::schema::is_metric_name_field(name), "{name:?}");
            assert_eq!(
                "__name__",
                schema.fields()[2].name(),
                "the name column is appended after the labels"
            );
        } else {
            assert!(
                schema.field_with_name("__name__").is_err(),
                "the existing plan declares no name column: {schema:?}"
            );
        }
        batches
            .iter()
            .flat_map(|batch| {
                if declared_name {
                    let names = batch
                        .column_by_name("__name__")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assert_eq!(
                        names.null_count(),
                        names.len(),
                        "a subtraction names no series: {names:?}"
                    );
                }
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
                    .map(|row| (timestamps.value(row), values.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }
    observer.clear();
    let on_empty_rows = unlabeled_rows(
        native_tql_output(&instance, &on_empty_sql, true).await,
        true,
    )
    .await;
    let on_empty_plan = observer.two_input_fused();
    assert_eq!(on_empty_plan.name(), "FusedSeriesExec", "{on_empty_sql}");
    // The rows layout declares the per-row name column after the labels; a subtraction answers a
    // value of its own, so that column exists and is null here (asserted on the batches above).
    let on_empty_fields = on_empty_plan.schema().fields().clone();
    assert_eq!(
        on_empty_fields.len(),
        3,
        "{on_empty_sql}: {on_empty_fields:?}"
    );
    assert_eq!(
        on_empty_fields[2].name(),
        "__name__",
        "{on_empty_sql}: the name column is appended after the labels"
    );
    assert!(
        datatypes::schema::is_metric_name_field(&on_empty_fields[2]),
        "{on_empty_sql}: {on_empty_fields:?}"
    );
    assert_eq!(on_empty_rows, vec![(1_000, -1_001.0)]);
    let on_empty_legacy = unlabeled_rows(
        native_tql_output(&instance, &on_empty_sql, false).await,
        false,
    )
    .await;
    assert_eq!(on_empty_rows, on_empty_legacy);

    // Different matcher forms can still be joined natively while preserving both source bounds.
    let different_matchers =
        format!(r#"TQL EVAL (0, 0, '1s') {table}{{host!=""}} - ({table}{{host=~"h.+"}} * 2)"#);
    observer.clear();
    let different_matcher_rows =
        native_result_rows(native_tql_output(&instance, &different_matchers, true).await).await;
    let different_matcher_plan = observer.two_input_fused();
    assert_two_source_native_plan(&observer, &different_matcher_plan, false);
    assert_eq!(different_matcher_rows, expected[..HOSTS].to_vec());
    let different_matcher_legacy =
        native_result_rows(native_tql_output(&instance, &different_matchers, false).await).await;
    assert_eq!(different_matcher_rows, different_matcher_legacy);

    // Group matching executes natively: it keeps the many side's series, so the route answers it
    // with one node over two same-table scans.
    let grouped_sql = format!(
        r#"TQL EVAL (0, 0, '1s') {table}{{host!=""}} - on(host) group_left() ({table}{{host!=""}} * 2)"#
    );
    observer.clear();
    let grouped_rows =
        native_result_rows(native_tql_output(&instance, &grouped_sql, true).await).await;
    let grouped_plan = observer.two_input_fused();
    assert_two_source_native_plan(&observer, &grouped_plan, false);
    assert_eq!(grouped_rows, expected[..HOSTS].to_vec(), "{grouped_sql}");
    let grouped_legacy =
        native_result_rows(native_tql_output(&instance, &grouped_sql, false).await).await;
    assert_eq!(grouped_rows, grouped_legacy, "{grouped_sql}");

    // A subquery is planned natively: `rate` over an evaluated input reads the grid the subquery
    // produces, and the route compiles that window instead of sending the statement to the ordinary
    // planner. The rows are what says the plan it runs answers the statement it was given, so the
    // answer is pinned here and compared with the existing plan's on the same fixture.
    let subquery_sql = format!(r#"TQL EVAL (1, 1, '1s') rate({table}{{host="h000"}}[2s:1s])"#);
    observer.clear();
    let subquery_rows =
        native_result_rows(native_tql_output(&instance, &subquery_sql, true).await).await;
    assert!(
        observer.fused().is_some(),
        "a subquery is not a fallback shape: {subquery_sql}"
    );
    assert_eq!(
        subquery_rows,
        vec![(1_000, "h000".to_string(), 0.5)],
        "{subquery_sql}"
    );
    let subquery_legacy =
        native_result_rows(native_tql_output(&instance, &subquery_sql, false).await).await;
    assert_eq!(subquery_rows, subquery_legacy, "{subquery_sql}");
}

/// Empty-key grouped matches retain the many side's labels and report kernel cardinality errors.
#[tokio::test(flavor = "multi_thread")]
async fn native_hybrid_binary_empty_on_grouped_matching_and_errors() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_binary_empty_on_matching",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_empty_match (host STRING, kind STRING, zone_name STRING, \
         ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host, kind, zone_name)); \
         INSERT INTO native_empty_match VALUES \
         ('h0', 'a', '', 0, 10), ('h0', 'b', '', 0, 20), ('h0', 'c', 'z', 0, 2)",
        QueryContext::arc(),
    )
    .await;

    let labels = |kind: &str| {
        [
            ("host".to_string(), "h0".to_string()),
            ("kind".to_string(), kind.to_string()),
            ("zone_name".to_string(), "z".to_string()),
        ]
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>()
    };
    for (query, expected) in [
        (
            r#"native_empty_match{kind=~"a|b"} - on() group_left(zone_name) native_empty_match{kind="c"}"#,
            vec![(0, labels("a"), 8.0), (0, labels("b"), 18.0)],
        ),
        (
            r#"native_empty_match{kind="c"} - on() group_right(zone_name) native_empty_match{kind=~"a|b"}"#,
            vec![(0, labels("a"), -8.0), (0, labels("b"), -18.0)],
        ),
    ] {
        let sql = format!("TQL EVAL (0, 0, '1s') {query}");
        observer.clear();
        let (columns, rows) =
            native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
        let plan = observer.two_input_fused();
        assert_eq!(plan.name(), "FusedSeriesExec", "{query}");
        // The value is named after the statement's expression, and the rows answer reports the
        // per-row name column among its labels; a math kernel drops the name, so it holds no value.
        let mut expected_columns = vec![
            "__name__".to_string(),
            "host".to_string(),
            "kind".to_string(),
            "ts".to_string(),
            expression_name(query),
            "zone_name".to_string(),
        ];
        expected_columns.sort();
        assert_eq!(columns, expected_columns);
        assert_eq!(rows, expected, "{query}");
        let (_, legacy) =
            native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(rows, legacy, "legacy: {query}");
    }

    for (query, expected_error) in [
        (
            r#"native_empty_match{kind="c"} - on() native_empty_match{kind=~"a|b"}"#,
            "found duplicate series for the match group {} on the right hand-side of the operation at 0 ms",
        ),
        (
            r#"native_empty_match{kind=~"a|b"} - on() native_empty_match{kind="c"}"#,
            "multiple matches for labels {} at 0 ms: many-to-one matching must be explicit",
        ),
        (
            r#"native_empty_match{kind="c"} - on() group_left() native_empty_match{kind=~"a|b"}"#,
            "found duplicate series for the match group {} on the right hand-side of the operation at 0 ms",
        ),
        (
            r#"native_empty_match{kind=~"a|b"} - on() group_right() native_empty_match{kind="c"}"#,
            "found duplicate series for the match group {} on the left hand-side of the operation at 0 ms",
        ),
    ] {
        let sql = format!("TQL EVAL (0, 0, '1s') {query}");
        observer.clear();
        let result = instance
            .do_query(&sql, native_query_context(true, 4))
            .await
            .remove(0);
        let error = match result {
            Ok(output) => collect_error(output).await,
            Err(error) => error.to_string(),
        };
        let plan = observer.two_input_fused();
        assert_eq!(plan.name(), "FusedSeriesExec", "{query}");
        assert!(error.contains(expected_error), "{query}: {error}");
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
                assert_four_native_partitions(&plan, native_rows.len());
            }
            let child = plan.children()[0];
            let child = child
                .downcast_ref::<CooperativeExec>()
                .map_or(child, |cooperative| cooperative.input());
            let region_scan = region_series_scan(&observer);
            let series_scan = child
                .downcast_ref::<SeriesScanExec>()
                .or(region_scan.as_deref())
                .expect("the statement reads a prepared series scan");
            let RawLabelSource::PrimaryKey {
                physical_metadata, ..
            } = series_scan.scan_binding()
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
            let output_values = fused_metric_set(&plan)
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
            assert!(text.contains("Input: raw samples source=0"), "{text}");
            assert!(text.contains("MergeScanExec"));
            assert!(!text.contains("metrics="), "{text}");
        } else if !enabled {
            assert!(!text.contains("FusedSeries"));
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

/// Two occurrences of one metric logical table bind two scans of the same physical table, each
/// with its own offset. The pair is answered where that table's region runs, with no raw source
/// per input, and matches the existing plan row for row.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_metric_same_logical_table_offset_pair_matches_legacy() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_metric_offset_pair_e2e",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_offset_phy (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE
        ) ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE native_offset_a (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_offset_phy');
        INSERT INTO native_offset_a (host, ts, val) VALUES ('a', 0, 10), ('a', 60000, 12), ('a', 120000, 15);"#,
        QueryContext::arc(),
    )
    .await;

    // The left occurrence reads each step's sample and the offset one reads a minute earlier:
    // 12 - 10 = 2, then 15 - 12 = 3.
    let sql = "TQL EVAL (60, 120, '1m') native_offset_a - native_offset_a offset 1m";
    let expected = vec![
        (60_000, "a".to_string(), 2.0),
        (120_000, "a".to_string(), 3.0),
    ];

    observer.clear();
    let legacy_rows = native_result_rows(native_tql_output(&instance, sql, false).await).await;
    assert!(
        observer.fused().is_none(),
        "the existing plan must not hold a native node"
    );
    assert_eq!(legacy_rows, expected, "legacy");

    observer.clear();
    let native_rows = native_result_rows(native_tql_output(&instance, sql, true).await).await;
    let plan = observer.take();
    assert_eq!(plan.name(), "FusedSeriesExec");
    // Both selectors are routed by the same matching key, so the pair is answered in the
    // partitions they were scanned in rather than gathered into one.
    assert_eq!(plan.children().len(), 2);
    let width = plan.children()[0].output_partitioning().partition_count();
    assert!(width > 1, "the pair must keep the scan's partitioning");
    assert_eq!(plan.output_partitioning().partition_count(), width);
    assert!(
        observer.take_raw_sources().len() == 2,
        "a repeated selector reads one raw source per input"
    );
    assert_eq!(native_rows, expected, "native");
    assert_eq!(native_rows, legacy_rows);
}

/// Whether a Final fused node routes its aggregate state through the series repartitioner.
fn holds_split_merge(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> bool {
    let mut stack = vec![plan.clone()];
    while let Some(node) = stack.pop() {
        let display = datafusion::physical_plan::displayable(node.as_ref())
            .one_line()
            .to_string();
        if node.name() == "FusedSeriesExec" && display.contains(" final") {
            let mut descendants = node.children().into_iter().cloned().collect::<Vec<_>>();
            while let Some(child) = descendants.pop() {
                if child.name() == "SeriesRepartitionExec" {
                    return true;
                }
                descendants.extend(child.children().into_iter().cloned());
            }
        }
        stack.extend(node.children().into_iter().cloned());
    }
    false
}

/// Two aggregates, one per metric logical table of one physical table, answered by one multi-input
/// native node and by the existing plan alike.
///
/// The two logical tables are two metrics over one physical table, so both rewrites prepare a scan
/// of that same table and only the id each one restricts to tells them apart: one binding read for
/// both inputs would report one metric's sum where the query asks for a ratio. The pair is not a
/// split either - the node has more than one input, so it declares one partition and folds every
/// input's partitions in that one execution - and the plans the engines ran say so.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_metric_two_aggregate_pair_matches_legacy() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_metric_pair_e2e",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_pair_phy (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE
        ) ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE native_pair_demo (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_pair_phy');
        CREATE TABLE native_pair_other (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_pair_phy');
        INSERT INTO native_pair_demo (host, ts, val) VALUES ('a', 0, 2), ('b', 0, 4);
        INSERT INTO native_pair_other (host, ts, val) VALUES ('a', 0, 8), ('b', 0, 8);"#,
        QueryContext::arc(),
    )
    .await;

    // `sum` drops the labels, so both sides are one label-less series and the division matches:
    // (2 + 4) / (8 + 8) = 0.375. Grouped by host each source keeps its own host's ratio, so the
    // two groups differ and a swapped input would not answer the same: a = 2 / 8, b = 4 / 8.
    let grouped = |host: &str, value: f64| {
        (
            0,
            std::collections::BTreeMap::from([("host".to_string(), host.to_string())]),
            value,
        )
    };
    for (query, expected, expected_columns) in [
        (
            "sum(native_pair_demo) / sum(native_pair_other)",
            vec![(0, std::collections::BTreeMap::new(), 0.375)],
            // The rows root declares the per-row name column after the labels; a division answers
            // a value of its own and reports no name, so the column is there and every value in it
            // is null.
            vec!["ts", "val", "__name__"],
        ),
        (
            "sum by (host) (native_pair_demo) / sum by (host) (native_pair_other)",
            vec![grouped("a", 0.25), grouped("b", 0.5)],
            vec!["ts", "val", "host", "__name__"],
        ),
    ] {
        let sql = format!("TQL EVAL (0, 0, '1s') {query}");

        // The existing plan: the route is off, so no native node ran and the answer is the one the
        // legacy planner derives from the fixture by hand.
        observer.clear();
        let (legacy_columns, legacy_rows) =
            native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
        assert!(
            observer.fused().is_none(),
            "{query}: the existing plan must not hold a native node"
        );
        assert!(
            !legacy_rows.is_empty(),
            "{query}: the comparison cannot pass vacuously"
        );
        assert_eq!(legacy_rows, expected, "legacy: {query}");

        // The native plan: the whole-vector parent at the frontend, one per-series prefix segment
        // per region. Each segment binds its own source's metric, which is what makes the division
        // read two different metrics rather than one read twice.
        observer.clear();
        let native_output = native_tql_output(&instance, &sql, true).await;
        // Check the actual output schema before the row reader sorts the columns.
        let schema = match &native_output.data {
            OutputData::Stream(stream) => stream.schema(),
            OutputData::RecordBatches(batches) => batches.schema(),
            _ => unreachable!("TQL EVAL returns record batches"),
        };
        let boundary_columns = schema
            .column_schemas()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let expression_query = PromQuery {
            query: query.to_string(),
            ..PromQuery::default()
        };
        let QueryStatement::Promql(stmt, _) =
            QueryLanguageParser::parse_promql(&expression_query, &QueryContext::arc()).unwrap()
        else {
            unreachable!()
        };
        let expression = stmt.expr.to_string();
        let expected_columns = expected_columns
            .into_iter()
            .map(|column| {
                if column == "val" {
                    expression.as_str()
                } else {
                    column
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            expected_columns, boundary_columns,
            "{query}: native boundary columns"
        );
        let (native_columns, native_rows) = native_labelled_rows(native_output).await;
        let root = observer.root_plan();
        let fused_parent = if root.name() == "ProjectionExec" {
            let children = root.children();
            assert_eq!(
                1,
                children.len(),
                "{query}: the naming projection renames one input: {}",
                datafusion::physical_plan::displayable(root.as_ref()).indent(true)
            );
            assert_eq!(
                "FusedSeriesExec",
                children[0].name(),
                "{query}: the parent under the naming projection is the frontend's own: {}",
                datafusion::physical_plan::displayable(root.as_ref()).indent(true)
            );
            Arc::clone(children[0])
        } else {
            assert_eq!(
                root.name(),
                "FusedSeriesExec",
                "{query}: the parent is the frontend's own: {}",
                datafusion::physical_plan::displayable(root.as_ref()).indent(true)
            );
            Arc::clone(&root)
        };
        assert_eq!(root.output_partitioning().partition_count(), 1, "{query}");
        assert_eq!(
            fused_parent.children().len(),
            2,
            "{query}: one input per segmented prefix: {}",
            datafusion::physical_plan::displayable(root.as_ref()).indent(true)
        );
        let root = fused_parent;
        // Each region answers one prefix: a repartition over the segment that evaluates the
        // selector, over the scan bound to that selector's metric.
        fn scan_metric(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> Option<String> {
            if let Some(scan) = plan.downcast_ref::<SeriesScanExec>() {
                return match scan.scan_binding() {
                    store_api::region_engine::RawLabelSource::PrimaryKey {
                        metric_name, ..
                    } => Some(metric_name.to_string()),
                    _ => None,
                };
            }
            plan.children().into_iter().find_map(scan_metric)
        }
        let region_metrics = observer
            .region_plans()
            .iter()
            .filter_map(scan_metric)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            std::collections::BTreeSet::from([
                "native_pair_demo".to_string(),
                "native_pair_other".to_string(),
            ]),
            region_metrics,
            "{query}: each logical table must be scanned as its own metric: {}",
            datafusion::physical_plan::displayable(root.as_ref()).indent(true)
        );
        assert!(
            !holds_split_merge(&observer.root_plan()),
            "{query}: a multi-input aggregate is one execution, not a split"
        );

        assert_eq!(native_rows, legacy_rows, "{query}: rows");
        assert_eq!(native_rows, expected, "{query}");

        // The columns both routes report: the time index, one value column, and one column per
        // label. The labels themselves are compared row by row above, and the time index is `ts` on
        // both. Compare time and label columns independently: the native boundary names that value
        // column after the canonical expression, where the existing planner names a computed column
        // after its expression too, and the two texts differ in the spaces a PromQL parse and a
        // legacy rewrite produce. The rows layout additionally declares the per-row name column and
        // this statement has none to report: a division computes a value of its own and its inputs
        // are aggregates, so every row's name is null. That column is therefore the one difference
        // from the existing route's schema.
        assert_eq!(
            native_columns.len(),
            legacy_columns.len() + 1,
            "{query}: columns"
        );
        assert!(
            native_columns.iter().any(|column| column == "__name__"),
            "{query}: the declared name column: {native_columns:?}"
        );
        assert!(
            native_rows
                .iter()
                .all(|(_, labels, _)| !labels.contains_key("__name__")),
            "{query}: a division names no row: {native_rows:?}"
        );
        assert!(
            legacy_columns.iter().any(|column| column == "ts"),
            "{query}"
        );
        let labels = native_rows
            .first()
            .map(|(_, labels, _)| labels.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for label in labels {
            assert!(
                native_columns.contains(&label) && legacy_columns.contains(&label),
                "{query}: label {label} is a column on both routes"
            );
        }
        common_telemetry::info!(
            "{query}: native columns {native_columns:?}, legacy columns {legacy_columns:?}, rows \
             {native_rows:?}, native plan:\n{}",
            datafusion::physical_plan::displayable(observer.root_plan().as_ref()).indent(true)
        );
    }
}

/// The metric fixture the CTE tests read: one label set in one unpartitioned physical metric
/// table, sampled `1, 3, 6, 10` one second apart.
const CTE_NATIVE_FIXTURE: &str = r#"
CREATE TABLE cte_native_phy (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
) ENGINE=metric WITH ("physical_metric_table" = "true");
CREATE TABLE cte_native (
    host STRING,
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    PRIMARY KEY (host)
) ENGINE=metric WITH ("on_physical_table" = 'cte_native_phy');
INSERT INTO cte_native (host, ts, val) VALUES ('h', 0, 1), ('h', 1000, 3), ('h', 2000, 6), ('h', 3000, 10);
"#;

/// The statement under test: a TQL statement embedded in a CTE, so a fused subtree sits under the
/// CTE's alias and the outer SQL projection instead of at the plan root.
///
/// The expression repeats one selector, which is the multi-input shape: the route answers it with
/// one fused node reading one scan per occurrence.
const CTE_NATIVE_STATEMENT: &str =
    "WITH tql AS (TQL EVAL (0, 3, '1s') cte_native + cte_native) SELECT * FROM tql";

/// [`CTE_NATIVE_STATEMENT`] ordered by the time index, which is the order its rows are expected in.
const CTE_NATIVE_ORDERED_STATEMENT: &str =
    "WITH tql AS (TQL EVAL (0, 3, '1s') cte_native + cte_native) SELECT * FROM tql ORDER BY ts";

/// [`CTE_NATIVE_STATEMENT`] with an explicit CTE column list plus an outer projection and filter
/// over the names it declares: the aliasing projection the CTE adds, and SQL above it that names
/// what it published.
///
/// The list is positional and exact, so it names the columns the existing planner publishes: the
/// label `site`, the time index `t` and the value `v`, in that order.
const CTE_NATIVE_ALIASED_STATEMENT: &str = "WITH tql (site, t, v) AS (TQL EVAL (0, 3, '1s') cte_native + cte_native) \
     SELECT t, v, site FROM tql WHERE v > 5 ORDER BY t";

/// The same statement for the native route, which publishes the name column as a fourth column and
/// therefore needs a fourth name in the list. The outer projection still reads the same three.
const CTE_NATIVE_ALIASED_NAME_STATEMENT: &str = "WITH tql (site, t, v, metric) AS (TQL EVAL (0, 3, '1s') cte_native + cte_native) \
     SELECT t, v, site FROM tql WHERE v > 5 ORDER BY t";

/// A CTE over a plain selection: the answer keeps the metric name, so the native route publishes it
/// and the existing route does not.
const CTE_NATIVE_NAME_STATEMENT: &str =
    "WITH tql AS (TQL EVAL (0, 0, '1s') cte_native) SELECT * FROM tql";

/// A CTE over a scalar-typed answer: no route declares a name column for it, so the CTE publishes
/// none either.
const CTE_NATIVE_SCALAR_STATEMENT: &str =
    "WITH tql AS (TQL EVAL (0, 0, '1s') time()) SELECT * FROM tql";

/// The rows [`CTE_NATIVE_STATEMENT`] answers: `cte_native + cte_native` doubles every sample, so
/// `1, 3, 6, 10` becomes `2, 6, 12, 20`.
fn cte_native_expected_rows() -> Vec<(i64, std::collections::BTreeMap<String, String>, f64)> {
    let h = || std::collections::BTreeMap::from([("host".to_string(), "h".to_string())]);
    vec![
        (0, h(), 2.0),
        (1_000, h(), 6.0),
        (2_000, h(), 12.0),
        (3_000, h(), 20.0),
    ]
}

/// The rows the aliased statement answers: the rows of [`CTE_NATIVE_STATEMENT`] with `v > 5`, under
/// the label name the CTE gave the label column.
fn cte_native_filtered_rows() -> Vec<(i64, std::collections::BTreeMap<String, String>, f64)> {
    let site = || std::collections::BTreeMap::from([("site".to_string(), "h".to_string())]);
    vec![
        (1_000, site(), 6.0),
        (2_000, site(), 12.0),
        (3_000, site(), 20.0),
    ]
}

/// The schema a statement publishes, as SQL reads it after distributed planning: every column's
/// name, Arrow type, nullability and metadata, and the metadata of the schema itself.
///
/// A CTE's columns are SQL-visible, and a column list the CTE declares is applied to them by
/// position, so the columns the existing planner publishes keep their names, order, types and
/// metadata whichever route planned the statement - including the Arrow type `arrow_typeof` reports
/// for a column, which is why the label below is compared as the encoded type both routes publish
/// rather than as the strings both answers decode to. A native answer declares one more column than
/// that, the per-row metric name, and a CTE over it publishes that too: what the CTE exposes follows
/// the answer, so the column and the number of names a positional list needs depend on the answer
/// planning selected.
fn cte_native_published_schema(output: &Output) -> ArrowSchema {
    let schema = match &output.data {
        OutputData::Stream(stream) => stream.schema(),
        OutputData::RecordBatches(batches) => batches.schema(),
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    schema.arrow_schema().as_ref().clone()
}

/// The metadata the time index column carries on either route.
fn cte_native_time_index_metadata() -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([("greptime:time_index".to_string(), "true".to_string())])
}

/// The metadata a GreptimeDB schema carries on either route.
fn cte_native_schema_metadata() -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([("greptime:version".to_string(), "0".to_string())])
}

/// The schema [`CTE_NATIVE_STATEMENT`] publishes on both routes: the existing planner's columns.
///
/// The fused plan publishes the time index first and names the value after the tree's own field; the
/// CTE publishes this over it instead. The label column is read as the string it carries, which is
/// what a result reports it as on both routes; the encoding behind it is what
/// [`CTE_NATIVE_ARROW_TYPE_STATEMENT`] compares.
fn cte_native_published_schema_expected() -> ArrowSchema {
    ArrowSchema::new_with_metadata(
        vec![
            Field::new("host", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            )
            .with_metadata(cte_native_time_index_metadata()),
            Field::new("lhs.val + rhs.val", DataType::Float64, true),
        ],
        cte_native_schema_metadata(),
    )
}

/// The metadata the declared name column carries.
fn cte_native_name_metadata() -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([("greptime:metric_name".to_string(), "true".to_string())])
}

/// The schema [`CTE_NATIVE_STATEMENT`] publishes on the native route: the existing planner's
/// columns, then the per-row name the answer declares.
///
/// The name is part of what the statement answers, so a CTE over a native answer publishes it. The
/// existing planner never publishes it, so the same statement answered by that route has one column
/// fewer: a CTE's columns follow the answer it was planned from.
fn cte_native_published_schema_expected_native() -> ArrowSchema {
    let mut fields = cte_native_published_schema_expected()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(
        Field::new("__name__", DataType::Utf8, true).with_metadata(cte_native_name_metadata()),
    );
    ArrowSchema::new_with_metadata(fields, cte_native_schema_metadata())
}

/// The schema [`CTE_NATIVE_ALIASED_STATEMENT`] publishes: the names its CTE declares, in the order it
/// declares them, over the columns of [`CTE_NATIVE_STATEMENT`].
fn cte_native_aliased_schema_expected() -> ArrowSchema {
    ArrowSchema::new_with_metadata(
        vec![
            Field::new("t", DataType::Timestamp(TimeUnit::Millisecond, None), false)
                .with_metadata(cte_native_time_index_metadata()),
            Field::new("v", DataType::Float64, true),
            Field::new("site", DataType::Utf8, true),
        ],
        cte_native_schema_metadata(),
    )
}

/// [`CTE_NATIVE_STATEMENT`] with `arrow_typeof` over each of its columns: the Arrow type SQL sees on
/// a CTE's column, which the schema above has to agree with.
const CTE_NATIVE_ARROW_TYPE_STATEMENT: &str = "WITH tql AS (TQL EVAL (0, 3, '1s') cte_native + cte_native) \
     SELECT arrow_typeof(host) AS host, arrow_typeof(ts) AS ts, \
     arrow_typeof(\"lhs.val + rhs.val\") AS val FROM tql LIMIT 1";

/// The type names `arrow_typeof` reports for the columns of [`CTE_NATIVE_STATEMENT`].
async fn cte_native_arrow_types(output: Output) -> Vec<String> {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    let batch = batches.iter().next().expect("one row of type names");
    ["host", "ts", "val"]
        .iter()
        .map(|name| {
            let column = batch.column_by_name(name).unwrap();
            let column = cast(column.as_ref(), &DataType::Utf8).unwrap();
            let column = column.as_any().downcast_ref::<StringArray>().unwrap();
            column.value(0).to_string()
        })
        .collect()
}

/// The type names [`CTE_NATIVE_ARROW_TYPE_STATEMENT`] reports on either route.
fn cte_native_arrow_types_expected() -> Vec<String> {
    vec![
        "Dictionary(UInt32, Utf8)".to_string(),
        "Timestamp(ms)".to_string(),
        "Float64".to_string(),
    ]
}

/// The rows of a TQL result in the order the statement published them.
/// The rows of a TQL result in the order the statement published them.
///
/// The time index, the one value column and the label set are found by type, so the statement's own
/// column names - or the names a CTE gave them - do not matter, and a route that orders its rows
/// differently is visible.
async fn cte_native_rows(
    output: Output,
) -> Vec<(i64, std::collections::BTreeMap<String, String>, f64)> {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let schema = batch.schema();
        let mut time = None;
        let mut value = None;
        let mut labels = Vec::new();
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            match field.data_type() {
                DataType::Timestamp(..) => {
                    time = Some(cast(column.as_ref(), &DataType::Int64).unwrap())
                }
                DataType::Float64 | DataType::Int64 => {
                    assert!(value.is_none(), "one value column: {schema:?}");
                    value = Some(cast(column.as_ref(), &DataType::Float64).unwrap())
                }
                _ => labels.push((
                    field.name().clone(),
                    cast(column.as_ref(), &DataType::Utf8).unwrap(),
                )),
            }
        }
        let time = time.expect("a time index");
        let time = time.as_any().downcast_ref::<Int64Array>().unwrap();
        let value = value.expect("a value column");
        let value = value.as_any().downcast_ref::<Float64Array>().unwrap();
        for row in 0..batch.num_rows() {
            let labels = labels
                .iter()
                .filter_map(|(name, column)| {
                    let column = column.as_any().downcast_ref::<StringArray>().unwrap();
                    (column.is_valid(row) && !column.value(row).is_empty())
                        .then(|| (name.clone(), column.value(row).to_string()))
                })
                .collect();
            rows.push((time.value(row), labels, value.value(row)));
        }
    }
    rows
}

/// Every node name the observer captured for the last statement, frontend and region side alike.
fn cte_native_observed_names(observer: &NativePlanObserver) -> Vec<String> {
    observed_physical_nodes(observer)
        .into_iter()
        .map(|node| node.name().to_string())
        .collect()
}

/// Every fused node the observer captured for the last statement.
fn cte_native_observed_fused(
    observer: &NativePlanObserver,
) -> Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    observed_physical_nodes(observer)
        .into_iter()
        .filter(|node| node.name() == "FusedSeriesExec")
        .collect()
}

/// Every plan the observer captured for the last statement, as trees, so a shape assertion that
/// fails shows what ran.
fn cte_native_observed_plan_trees(observer: &NativePlanObserver) -> String {
    observer
        .all_plans()
        .iter()
        .map(display_plan_tree)
        .collect::<Vec<_>>()
        .join("\n")
}

/// A TQL statement embedded in a CTE runs the fused plan when the query asks for the route, so no
/// fallback and no refusal.
///
/// The CTE puts a SQL alias and the outer projection above the node, which is what this asserts
/// changes nothing about where the node runs: the statement is answered by one `FusedSeriesExec`
/// reading both occurrences of the selector, there is no join anywhere in the plan it ran, and the
/// rows are the legacy route's rows.
#[tokio::test(flavor = "multi_thread")]
async fn cte_native_statement_runs_the_fused_plan() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) =
        native_test_instance(GreptimeDbStandaloneBuilder::new("cte_native_statement")).await;
    let instance = standalone.fe_instance().clone();
    execute_all(&instance, CTE_NATIVE_FIXTURE, QueryContext::arc()).await;

    // The existing plan answers the statement first, so the native answer has an oracle.
    observer.clear();
    let legacy_output = native_tql_output(&instance, CTE_NATIVE_ORDERED_STATEMENT, false).await;
    let legacy_schema = cte_native_published_schema(&legacy_output);
    let legacy_rows = cte_native_rows(legacy_output).await;
    assert!(
        observer.fused().is_none(),
        "the route is off, so no native node may run:\n{}",
        cte_native_observed_plan_trees(&observer)
    );
    assert_eq!(
        legacy_schema,
        cte_native_published_schema_expected(),
        "legacy schema"
    );
    assert_eq!(legacy_rows, cte_native_expected_rows(), "legacy");
    let legacy_types = cte_native_arrow_types(
        native_tql_output(&instance, CTE_NATIVE_ARROW_TYPE_STATEMENT, false).await,
    )
    .await;
    assert_eq!(
        legacy_types,
        cte_native_arrow_types_expected(),
        "legacy arrow types"
    );

    // The native plan: one fused node answers both occurrences, and every row comes back in the
    // order the statement asked for.
    observer.clear();
    let native_output = native_tql_output(&instance, CTE_NATIVE_ORDERED_STATEMENT, true).await;
    let native_schema = cte_native_published_schema(&native_output);
    let native_rows = cte_native_rows(native_output).await;
    let fused = cte_native_observed_fused(&observer);
    assert_eq!(
        1,
        fused.len(),
        "the statement runs one fused node:\n{}",
        cte_native_observed_plan_trees(&observer)
    );
    let joins = cte_native_observed_names(&observer)
        .into_iter()
        .filter(|name| name.contains("Join"))
        .collect::<Vec<_>>();
    assert!(
        joins.is_empty(),
        "a fused plan joins nothing, but these nodes ran: {joins:?}\n{}",
        cte_native_observed_plan_trees(&observer)
    );
    // One logical table keeps both occurrences on their own region boundary: the node reads one
    // merged region stream per occurrence, and the raw source behind each of them is the region's
    // own, not a frontend scan per input.
    let trees = cte_native_observed_plan_trees(&observer);
    let scans = observed_series_scans(&observer);
    assert_eq!(
        scans.len(),
        2,
        "one independent scan per occurrence of the repeated selector:\n{trees}"
    );
    let occurrences = fused[0].children();
    assert_eq!(
        2,
        occurrences.len(),
        "one region boundary per occurrence of the repeated selector:\n{trees}"
    );
    for (index, occurrence) in occurrences.iter().enumerate() {
        assert!(
            find_plan_node(occurrence, "MergeScanExec").is_some(),
            "occurrence {index} must read its own region merge:\n{trees}"
        );
        assert!(
            find_plan_node(occurrence, "SeriesScanExec").is_none(),
            "occurrence {index} must leave the series scan to its region:\n{trees}"
        );
    }
    assert_eq!(
        observer.take_raw_sources().len(),
        occurrences.len(),
        "the region reads one raw source per occurrence of the repeated selector:\n{trees}"
    );
    assert_eq!(
        native_schema,
        cte_native_published_schema_expected_native(),
        "the CTE publishes the existing planner's columns, then the name the answer declares"
    );
    let legacy_fields = legacy_schema
        .fields()
        .iter()
        .map(|field| field.as_ref())
        .collect::<Vec<_>>();
    let native_prefix = native_schema.fields()[..legacy_fields.len()]
        .iter()
        .map(|field| field.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(
        native_prefix, legacy_fields,
        "and every column the existing route publishes keeps its name, type, nullability and \
         metadata: only the name column is added"
    );
    let native_types = cte_native_arrow_types(
        native_tql_output(&instance, CTE_NATIVE_ARROW_TYPE_STATEMENT, true).await,
    )
    .await;
    assert_eq!(
        native_types,
        cte_native_arrow_types_expected(),
        "the Arrow types SQL sees on the CTE's columns"
    );
    assert_eq!(
        native_types, legacy_types,
        "and they are the same ones the existing plan publishes"
    );
    assert_eq!(native_rows, cte_native_expected_rows(), "native");
    assert_eq!(native_rows, legacy_rows);

    // `SELECT *` without an order returns the same rows, and the node still answers them.
    observer.clear();
    let unordered_rows =
        cte_native_rows(native_tql_output(&instance, CTE_NATIVE_STATEMENT, true).await).await;
    let mut unordered = unordered_rows.clone();
    unordered.sort_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
    assert_eq!(unordered, cte_native_expected_rows(), "native, unordered");
    assert_eq!(
        1,
        cte_native_observed_fused(&observer).len(),
        "the unordered statement runs one fused node:\n{}",
        cte_native_observed_plan_trees(&observer)
    );
}

/// The CTE's own column list and an outer projection and filter over it place nothing: the fused
/// node still answers the statement, and the columns and rows are the ones the existing plan
/// answers with.
///
/// The declared list is positional and exact, so the native answer needs one more name than the
/// existing one: it publishes the per-row name column, and the existing route does not. What the
/// list declares, what the outer projection reads and what the outer filter keeps are all above the
/// node, and none of them moves it.
#[tokio::test(flavor = "multi_thread")]
async fn cte_native_column_aliases_and_outer_filter_keep_the_fused_plan() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) =
        native_test_instance(GreptimeDbStandaloneBuilder::new("cte_native_aliased")).await;
    let instance = standalone.fe_instance().clone();
    execute_all(&instance, CTE_NATIVE_FIXTURE, QueryContext::arc()).await;

    observer.clear();
    let legacy_output = native_tql_output(&instance, CTE_NATIVE_ALIASED_STATEMENT, false).await;
    let legacy_schema = cte_native_published_schema(&legacy_output);
    let legacy_rows = cte_native_rows(legacy_output).await;
    assert!(
        observer.fused().is_none(),
        "the route is off, so no native node may run:\n{}",
        cte_native_observed_plan_trees(&observer)
    );
    assert_eq!(
        legacy_schema,
        cte_native_aliased_schema_expected(),
        "legacy schema"
    );
    assert_eq!(legacy_rows, cte_native_filtered_rows(), "legacy");

    observer.clear();
    // The native answer publishes one more column, so the declared list names four. Three names for
    // four columns is a hard error, and four names for the existing route's three is one too: the
    // list is positional and exact.
    let short = instance
        .do_query(CTE_NATIVE_ALIASED_STATEMENT, native_query_context(true, 4))
        .await
        .remove(0);
    assert!(
        short.is_err(),
        "three names for the native answer's four columns"
    );
    let long = instance
        .do_query(
            CTE_NATIVE_ALIASED_NAME_STATEMENT,
            native_query_context(false, 4),
        )
        .await
        .remove(0);
    assert!(
        long.is_err(),
        "four names for the existing answer's three columns"
    );
    let native_output = native_tql_output(&instance, CTE_NATIVE_ALIASED_NAME_STATEMENT, true).await;
    let native_schema = cte_native_published_schema(&native_output);
    let native_rows = cte_native_rows(native_output).await;
    assert_eq!(
        1,
        cte_native_observed_fused(&observer).len(),
        "the aliased statement runs one fused node:\n{}",
        cte_native_observed_plan_trees(&observer)
    );
    let joins = cte_native_observed_names(&observer)
        .into_iter()
        .filter(|name| name.contains("Join"))
        .collect::<Vec<_>>();
    assert!(
        joins.is_empty(),
        "a fused plan joins nothing, but these nodes ran: {joins:?}\n{}",
        cte_native_observed_plan_trees(&observer)
    );
    assert_eq!(
        native_schema,
        cte_native_aliased_schema_expected(),
        "the outer projection reads the same three columns on both routes"
    );
    assert_eq!(native_rows, cte_native_filtered_rows(), "native");
    assert_eq!(native_rows, legacy_rows);
}

/// The CTE publishes the name column exactly when the answer declares one.
///
/// A plain selection keeps its series name, and the native answer declares the column for it, so the
/// CTE publishes it and SQL can read it. The existing answer declares none, so the same statement
/// answered by that route publishes one column fewer and asking for the name is an error - the CTE's
/// columns follow the answer it was planned from. A scalar-typed answer declares no such column on
/// either route, so neither publishes one: the CTE does not invent a column the statement does not
/// answer.
#[tokio::test(flavor = "multi_thread")]
async fn cte_native_publishes_the_name_column_only_when_the_answer_declares_one() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) =
        native_test_instance(GreptimeDbStandaloneBuilder::new("cte_native_name")).await;
    let instance = standalone.fe_instance().clone();
    execute_all(&instance, CTE_NATIVE_FIXTURE, QueryContext::arc()).await;

    observer.clear();
    let native = native_tql_output(&instance, CTE_NATIVE_NAME_STATEMENT, true).await;
    let native_schema = cte_native_published_schema(&native);
    let name = native_schema
        .field_with_name("__name__")
        .expect("the native answer declares the name column");
    assert!(
        datatypes::schema::is_metric_name_field(name),
        "and marks it as one: {native_schema:?}"
    );
    let rows = cte_native_rows(native).await;
    assert!(!rows.is_empty(), "the statement answers rows");
    assert!(
        rows.iter()
            .all(|(_, labels, _)| labels.get("__name__").map(String::as_str) == Some("cte_native")),
        "every row names the metric it was read from: {rows:?}"
    );

    let legacy = native_tql_output(&instance, CTE_NATIVE_NAME_STATEMENT, false).await;
    let legacy_schema = cte_native_published_schema(&legacy);
    assert!(
        legacy_schema.field_with_name("__name__").is_err(),
        "the existing answer declares no name column: {legacy_schema:?}"
    );
    cte_native_rows(legacy).await;
    let missing = instance
        .do_query(
            "WITH tql AS (TQL EVAL (0, 0, '1s') cte_native) SELECT __name__ FROM tql",
            native_query_context(false, 4),
        )
        .await
        .remove(0);
    assert!(
        missing.is_err(),
        "a CTE answered by the existing route has no name column to read"
    );

    for enabled in [true, false] {
        let scalar = native_tql_output(&instance, CTE_NATIVE_SCALAR_STATEMENT, enabled).await;
        let schema = cte_native_published_schema(&scalar);
        assert!(
            schema.field_with_name("__name__").is_err(),
            "a scalar answer declares no name column (native={enabled}): {schema:?}"
        );
    }
}

/// The control: without the per-request hint the CTE statement keeps the existing plan, so the rows
/// the route is compared against above come from the plan that answered this statement before the
/// route existed.
#[tokio::test(flavor = "multi_thread")]
async fn cte_native_without_the_hint_keeps_the_legacy_plan() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) =
        native_test_instance(GreptimeDbStandaloneBuilder::new("cte_native_control")).await;
    let instance = standalone.fe_instance().clone();
    execute_all(&instance, CTE_NATIVE_FIXTURE, QueryContext::arc()).await;

    observer.clear();
    let output = native_tql_output(&instance, CTE_NATIVE_ORDERED_STATEMENT, false).await;
    let schema = cte_native_published_schema(&output);
    let rows = cte_native_rows(output).await;
    assert!(
        cte_native_observed_fused(&observer).is_empty(),
        "the hint is off, so no fused node may run:\n{}",
        cte_native_observed_plan_trees(&observer)
    );
    assert_eq!(schema, cte_native_published_schema_expected());
    assert_eq!(rows, cte_native_expected_rows());
}

/// The same CTE statement against real datanodes.
///
/// The topology differs from the in-process answer: every selector occurrence reads its region
/// through a merge scan of its own, and the fused node that reads them both runs in the frontend.
/// What does not differ is the answer, so the rows are the ones the legacy route returns.
#[tokio::test(flavor = "multi_thread")]
async fn distributed_cte_native_runs_the_fused_plan_over_raw_sources() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("distributed_cte_native")
        .await
        .with_datanodes(2)
        .with_real_datanode_grpc_addr(true)
        .with_frontend_real_datanode_grpc(true)
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .build(false)
        .await;
    let instance = cluster.fe_instance().clone();
    execute_all(&instance, CTE_NATIVE_FIXTURE, QueryContext::arc()).await;

    let query = |enabled| {
        instance.do_query(
            CTE_NATIVE_ORDERED_STATEMENT,
            native_query_context(enabled, 2),
        )
    };

    let legacy_output = query(false).await.remove(0).unwrap();
    let legacy_schema = cte_native_published_schema(&legacy_output);
    let legacy_rows = cte_native_rows(legacy_output).await;
    assert_eq!(
        legacy_schema,
        cte_native_published_schema_expected(),
        "legacy schema"
    );
    assert_eq!(legacy_rows, cte_native_expected_rows(), "legacy");

    let output = query(true).await.remove(0).unwrap();
    let native_schema = cte_native_published_schema(&output);
    let plan = output.meta.plan.clone().expect("frontend physical plan");
    let names = {
        let mut names = Vec::new();
        let mut stack = vec![Arc::clone(&plan)];
        while let Some(node) = stack.pop() {
            names.push(node.name().to_string());
            stack.extend(node.children().into_iter().cloned());
        }
        names
    };
    assert!(
        names.iter().any(|name| name == "FusedSeriesExec"),
        "the frontend must hold the fused node:\n{}",
        display_plan_tree(&plan)
    );
    assert!(
        names.iter().all(|name| !name.contains("Join")),
        "a fused plan joins nothing, but these nodes ran: {names:?}\n{}",
        display_plan_tree(&plan)
    );
    // Each child of the fused node reads its own region, so no region runs the pair: the frontend
    // holds the node over one merge scan per occurrence.
    let fused = find_plan_node(&plan, "FusedSeriesExec").expect("the frontend fused node");
    assert_eq!(
        2,
        fused.children().len(),
        "the node reads one region read per occurrence:\n{}",
        display_plan_tree(&plan)
    );
    for (occurrence, child) in fused.children().iter().enumerate() {
        assert!(
            find_plan_node(child, "MergeScanExec").is_some(),
            "occurrence {occurrence} reads its region through a merge scan:\n{}",
            display_plan_tree(&plan)
        );
        assert!(
            find_plan_node(child, "FusedSeriesExec").is_none(),
            "occurrence {occurrence} must not run a fused node in its region:\n{}",
            display_plan_tree(&plan)
        );
    }
    let rows = cte_native_rows(output).await;
    assert_eq!(
        native_schema,
        cte_native_published_schema_expected_native(),
        "the CTE publishes the existing planner's columns over the distributed fused plan, then \
         the name the answer declares"
    );
    let legacy_fields = legacy_schema
        .fields()
        .iter()
        .map(|field| field.as_ref())
        .collect::<Vec<_>>();
    let native_prefix = native_schema.fields()[..legacy_fields.len()]
        .iter()
        .map(|field| field.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(
        native_prefix, legacy_fields,
        "every column the existing route publishes keeps its name, type, nullability and metadata \
         on the distributed route too; only the name column is added"
    );
    assert_eq!(rows, cte_native_expected_rows(), "native");
    assert_eq!(rows, legacy_rows);
}

/// A grouping by the metric name reports one group per metric.
///
/// `sum by (__name__) (a or b)` reads two metrics whose only distinguishing label is their metric
/// name. The union reports the name of the side each series came from, and the outer grouping folds
/// by that name, so the answer is two groups, each named after its own metric. The number of rows
/// and their full label sets are what this asserts: a plan that dropped the name would report the
/// two contributions as one group, and the value alone would still look plausible.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_answers_a_grouping_by_the_metric_name() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_group_by_name",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_name_phy (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE
        ) ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE native_name_demo (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_name_phy');
        CREATE TABLE native_name_other (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_name_phy');
        INSERT INTO native_name_demo (host, ts, val) VALUES ('a', 0, 2), ('b', 0, 4);
        INSERT INTO native_name_other (host, ts, val) VALUES ('a', 0, 8), ('b', 0, 8);"#,
        QueryContext::arc(),
    )
    .await;

    // The two sides carry different hosts, so `or` keeps both series and the grouping has two groups
    // whose only distinguishing label is the metric name.
    let query = r#"sum by (__name__) (native_name_demo{host="a"} or native_name_other{host="b"})"#;
    let sql = format!("TQL EVAL (0, 0, '1s') {query}");
    observer.clear();
    let (_, native_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    let root = datafusion::physical_plan::displayable(observer.root_plan().as_ref())
        .indent(true)
        .to_string();
    // The union has to stay native: the legacy plan joins the two tables and reports no name column
    // the outer grouping could fold by.
    assert!(
        observer.fused().is_some(),
        "{query}: the union runs as one fused node: {root}"
    );
    assert!(
        !root.contains("JoinExec"),
        "{query}: the union is not a legacy join: {root}"
    );
    let named =
        |name: &str| std::collections::BTreeMap::from([("__name__".to_string(), name.to_string())]);
    assert_eq!(
        vec![
            (0, named("native_name_demo"), 2.0),
            (0, named("native_name_other"), 8.0),
        ],
        native_rows,
        "{query}: one group per metric name, each named after its own metric"
    );
}

/// Ordinary Mito sources also report one group per metric under a grouping by the metric name.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_answers_mito_cross_table_grouping_by_metric_name() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_mito_group_by_name",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE name_a (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=mito; \
         CREATE TABLE name_b (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=mito; \
         INSERT INTO name_a VALUES ('a', 0, 2); \
         INSERT INTO name_b VALUES ('b', 0, 8)",
        QueryContext::arc(),
    )
    .await;

    let query = r#"sum by (__name__) (name_a{host="a"} or name_b{host="b"})"#;
    let sql = format!("TQL EVAL (0, 0, '1s') {query}");
    observer.clear();
    let (_, native_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    let root = datafusion::physical_plan::displayable(observer.root_plan().as_ref())
        .indent(true)
        .to_string();
    assert!(
        observer.fused().is_some(),
        "{query}: the union runs as one fused node: {root}"
    );
    assert!(
        !root.contains("JoinExec"),
        "{query}: the union is not a legacy join: {root}"
    );
    let named =
        |name: &str| std::collections::BTreeMap::from([("__name__".to_string(), name.to_string())]);
    assert_eq!(
        vec![(0, named("name_a"), 2.0), (0, named("name_b"), 8.0)],
        native_rows,
        "{query}: compare the full labelled rows"
    );
}

/// `count_values` over a union reports one series per metric name, and per value it counted.
///
/// `by (__name__)` folds the two inputs into two groups that differ only in their metric name;
/// `by (val)` names a label no series carries, so both inputs fall into one group and the outer
/// `sum` folds what `count_values` wrote there. The full label sets of every row are what this
/// asserts: a plan that dropped the name would report one series where two metrics were counted.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_answers_count_values_groupings_over_a_union() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_count_values_groups",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_cv_phy (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE
        ) ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE native_cv_demo (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_cv_phy');
        CREATE TABLE native_cv_other (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_cv_phy');
        INSERT INTO native_cv_demo (host, ts, val) VALUES ('a', 0, 1), ('b', 0, 1);
        INSERT INTO native_cv_other (host, ts, val) VALUES ('a', 0, 1), ('b', 0, 1);"#,
        QueryContext::arc(),
    )
    .await;

    // `count_values` writes the value it counted into the label the first argument names, so each
    // row carries that label beside the group's own labels. `val` is no label of either series, so
    // the second query groups both inputs together and counts each of them once.
    let by_name = r#"count_values by (__name__) ("sample", native_cv_demo{host="a"} or native_cv_other{host="b"})"#;
    let by_field = r#"sum(count_values by (val) ("sample", native_cv_demo{host="a"} or native_cv_other{host="b"}))"#;
    let counted = |name: &str| {
        std::collections::BTreeMap::from([
            ("__name__".to_string(), name.to_string()),
            ("sample".to_string(), "1".to_string()),
        ])
    };
    for (query, expected) in [
        (
            by_name,
            vec![
                (0, counted("native_cv_demo"), 1.0),
                (0, counted("native_cv_other"), 1.0),
            ],
        ),
        (by_field, vec![(0, std::collections::BTreeMap::new(), 2.0)]),
    ] {
        let sql = format!("TQL EVAL (0, 0, '1s') {query}");
        observer.clear();
        let (_, native_rows) =
            native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
        let root = datafusion::physical_plan::displayable(observer.root_plan().as_ref())
            .indent(true)
            .to_string();
        assert!(
            observer.fused().is_some(),
            "{query}: the union runs as one fused node: {root}"
        );
        assert!(
            !root.contains("JoinExec"),
            "{query}: the union is not a legacy join: {root}"
        );
        assert_eq!(
            expected, native_rows,
            "{query}: the full label sets of every row, not only the values"
        );
    }
}

/// `count_values` labels its result with the value it counted, and the native route has to name that
/// label in the schema it declares: the grouping's labels plus the one the node writes.
///
/// A rewrite of the label it writes is a rewrite of an existing label, so it is what the canonical
/// merge exists for: where the rewrite makes two of the node's series share a label set, the merge
/// is handed two values at one point; where it does not, the assembled answer is the existing plan's
/// and the label set says so. A rewrite of a fresh destination cannot fold anything, so nothing is
/// collected.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_rewrites_the_label_count_values_writes() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_count_values_rewrite",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_cv_rewrite (host STRING, \"zone\" STRING, ts TIMESTAMP TIME INDEX, \
         val DOUBLE, PRIMARY KEY (host, \"zone\")) ENGINE=mito; \
         INSERT INTO native_cv_rewrite (host, \"zone\", ts, val) VALUES \
         ('a', 'left', 0, 1), ('b', 'left', 0, 1), ('c', 'right', 0, 2)",
        QueryContext::arc(),
    )
    .await;

    // One expected row, so a failure names the label set that did not match.
    let row = |labels: &[(&str, &str)], value: f64| {
        (
            0,
            labels
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect::<std::collections::BTreeMap<_, _>>(),
            value,
        )
    };

    // `by (host)` keeps one of the scan's tags and the node writes `sample`; `zone` is not part of
    // the grouping, so the columns the node declares must not claim it. The destination is fresh,
    // so there is nothing for a canonical merge to collect.
    let fresh = r#"label_replace(count_values by (host) ("sample", native_cv_rewrite), "dst", "same", "host", ".*")"#;
    let sql = format!("TQL EVAL (0, 0, '1s') {fresh}");
    observer.clear();
    let (columns, native_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    assert_eq!(
        observer.fused().as_ref().map(|plan| plan.name()),
        Some("FusedSeriesExec"),
        "{fresh}: the statement must run on the native route: {}",
        display_plan_tree(&observer.root_plan())
    );
    assert!(
        !columns.contains(&"zone".to_string()),
        "{fresh}: `by (host)` drops `zone`, so the node must not declare it: {columns:?}"
    );
    assert!(
        canonical_router(&observer).is_none(),
        "{fresh}: a fresh destination folds nothing, so no boundary is built: {}",
        display_plan_tree(&observer.root_plan())
    );
    let expected = vec![
        row(&[("host", "a"), ("sample", "1"), ("dst", "same")], 1.0),
        row(&[("host", "b"), ("sample", "1"), ("dst", "same")], 1.0),
        row(&[("host", "c"), ("sample", "2"), ("dst", "same")], 1.0),
    ];
    assert_eq!(native_rows, expected, "{fresh}: the labels and the counts");
    let (_, legacy_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
    assert_eq!(native_rows, legacy_rows, "{fresh}: full labelled rows");

    // Rewriting `sample` itself is a rewrite of a label the node wrote. The grouping keeps `host`,
    // so the three results stay distinct: the collection runs and the answer is the existing plan's.
    let grouped = r#"label_replace(count_values by (host) ("sample", native_cv_rewrite), "sample", "same", "sample", ".*")"#;
    let sql = format!("TQL EVAL (0, 0, '1s') {grouped}");
    observer.clear();
    let (_, native_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    assert_eq!(
        observer.fused().as_ref().map(|plan| plan.name()),
        Some("FusedSeriesExec"),
        "{grouped}: the rewrite of the label `count_values` writes must stay on the native route"
    );
    assert!(
        canonical_router(&observer).is_some(),
        "{grouped}: the rewrite writes an existing label, so the pieces are collected: {}",
        display_plan_tree(&observer.root_plan())
    );
    let expected = vec![
        row(&[("host", "a"), ("sample", "same")], 1.0),
        row(&[("host", "b"), ("sample", "same")], 1.0),
        row(&[("host", "c"), ("sample", "same")], 1.0),
    ];
    assert_eq!(
        native_rows, expected,
        "{grouped}: the labels and the counts"
    );
    let (_, legacy_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
    assert_eq!(native_rows, legacy_rows, "{grouped}: full labelled rows");

    // Without a grouping both values land under one label set once the rewrite renames it, so the
    // collection is handed two values at one point: the duplicate Prometheus reports, raised while
    // the pieces are assembled rather than answered as two rows under one label set.
    let folded = r#"label_replace(count_values ("sample", native_cv_rewrite), "sample", "same", "sample", ".*")"#;
    let sql = format!("TQL EVAL (0, 0, '1s') {folded}");
    observer.clear();
    let native_error = collect_error(native_tql_output(&instance, &sql, true).await).await;
    assert_eq!(
        observer.fused().as_ref().map(|plan| plan.name()),
        Some("FusedSeriesExec"),
        "{folded}: the conflicting rewrite must reach the native route: {}",
        display_plan_tree(&observer.root_plan())
    );
    assert!(
        native_error.contains("cannot contain metrics with the same labelset"),
        "{folded}: the assembled label set holds two values at one point: {native_error}"
    );
    let legacy_error = collect_error(native_tql_output(&instance, &sql, false).await).await;
    assert!(
        legacy_error.contains("cannot contain metrics with the same labelset"),
        "{folded}: the existing plan reports the same overlap: {legacy_error}"
    );
}

/// `topk`/`bottomk` key their groups by series labels, and `by (val)` names no label a series
/// carries: `val` is the value field, not a label. Prometheus therefore puts every series of the
/// input into one empty group and answers only that group's winner, so the statement returns one
/// row rather than one per sample value.
///
/// Both routes are about that grouping, but only the native one answers Prometheus here. The value
/// column is named after the expression, so a `by (val)` over it selects nothing and the one empty
/// group keeps the global winner. The existing plan still partitions its window by the `val` column
/// it publishes, one group per sample value; that divergence is known and is not pinned here. What
/// the native route must keep is the answer, with the union below it running as one native node
/// rather than a legacy join.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_ranks_a_value_field_grouping_as_one_group() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_ranking_value_field",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_rank_phy (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE
        ) ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE native_rank_demo (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_rank_phy');
        CREATE TABLE native_rank_other (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_rank_phy');
        INSERT INTO native_rank_demo (host, ts, val) VALUES ('x', 0, 2);
        INSERT INTO native_rank_other (host, ts, val) VALUES ('y', 0, 8);"#,
        QueryContext::arc(),
    )
    .await;

    let pair = r#"native_rank_demo{host="x"} or native_rank_other{host="y"}"#;
    // No series carries a `val` label, so the grouping is empty: every series lands in that one
    // group and the answer is its winner, `y` at 8 for `topk` and `x` at 2 for `bottomk`.
    for (query, expected) in [
        (
            format!("topk by (val) (1, {pair})"),
            vec![(0, "y".to_string(), 8.0)],
        ),
        (
            format!("bottomk by (val) (1, {pair})"),
            vec![(0, "x".to_string(), 2.0)],
        ),
    ] {
        let sql = format!("TQL EVAL (0, 0, '1s') {query}");
        observer.clear();
        let native_rows = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
        let root = datafusion::physical_plan::displayable(observer.root_plan().as_ref())
            .indent(true)
            .to_string();
        // The union has to stay native rather than become the legacy join.
        assert!(
            observer.fused().is_some(),
            "{query}: the union runs as one fused node: {root}"
        );
        assert!(
            !root.contains("JoinExec"),
            "{query}: the union is not a legacy join: {root}"
        );
        assert_eq!(native_rows, expected, "{query}: full rows");
    }

    // A label grouping is what the native node does honor, so it still runs natively and keeps every
    // group's winner.
    let sql = format!("TQL EVAL (0, 0, '1s') topk by (host) (1, {pair})");
    observer.clear();
    let native_rows = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
    assert_eq!(observer.take().name(), "FusedSeriesExec");
    let legacy_rows = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
    assert_eq!(native_rows, legacy_rows);
    assert_eq!(native_rows.len(), 2);
}

/// A selection answers natively, and the shape it cannot answer reports Prometheus's own error.
///
/// `limitk` keeps the first `k` series of each group in input order, and Prometheus leaves which
/// series those are to the engine: a query that selects some of a series set may keep any subset,
/// so a route that hands its input over in a different order still answers correctly. What the
/// native route must not do is keep the statement from being answered at all.
///
/// That is what the earlier version of this test pinned, and it was wrong: the existing planner has
/// no kernel for `limitk`, so "the plan this statement already had" was an error, and holding the
/// native route to it was a coverage loss rather than a safety property. The route answers these
/// statements now, and the assertions below are on the answers.
///
/// Two shapes are checked. The wrapper case is the one that can be answered *incorrectly*: with
/// `vector(scalar(...))` on the left, `on()` matches the right side's two series against one
/// label set on the left, and Prometheus refuses that as many-to-many. The route must report that
/// refusal, not pick a side silently. The plain case selects from a one-series input, so the
/// first-k rule has one answer and the test can name it; a two-series input would leave the pick to
/// the engine, which is exactly the freedom Prometheus grants.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_answers_a_selection_that_depends_on_input_order() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_limitk_order",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_order_phy (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE
        ) ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE native_order_left (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_order_phy');
        CREATE TABLE native_order_right (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_order_phy');
        CREATE TABLE native_order_only (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_order_phy');
        INSERT INTO native_order_left (host, ts, val) VALUES ('a', 0, 2), ('b', 0, 4);
        INSERT INTO native_order_right (host, ts, val) VALUES ('a', 0, 8), ('b', 0, 8);
        INSERT INTO native_order_only (host, ts, val) VALUES ('a', 0, 8);"#,
        QueryContext::arc(),
    )
    .await;

    // The wrapper answers one value from its input, so the selection below it decides the value
    // and the `on()` match then pairs one left label set with two right series. Prometheus refuses
    // that pair, and so must the route: answering `10` or `12` here would be a silent choice
    // between the two matches.
    let query = "vector(scalar(limitk(1, native_order_left))) + on() native_order_right";
    let sql = format!("TQL EVAL (0, 0, '1s') {query}");
    observer.clear();
    let native = instance
        .do_query(&sql, native_query_context(true, 4))
        .await
        .remove(0)
        .expect("the route plans the statement");
    assert!(
        observer.fused().is_some(),
        "{query}: the route answers a selection, it does not decline it"
    );
    // The pair is refused while the stream is read, so the error is the answer's, not a planning
    // refusal that would have kept the statement on the existing plan.
    let error = collect_error(native).await;
    assert!(
        error.contains("found duplicate series for the match group {} on the right hand-side"),
        "{query}: {error}"
    );
    assert!(
        error.contains("many-to-many matching not allowed"),
        "{query}: {error}"
    );

    // One candidate, so the first-`k` rule keeps it and the answer is the series itself.
    let query = "limitk(1, native_order_only)";
    let sql = format!("TQL EVAL (0, 0, '1s') {query}");
    observer.clear();
    let rows = native_result_rows(native_tql_output(&instance, &sql, true).await).await;
    assert_eq!(
        observer.take().name(),
        "FusedSeriesExec",
        "{query}: the route answers it natively"
    );
    assert_eq!(rows, vec![(0, "a".to_string(), 8.0)], "{query}");
}

/// A tree the native runtime cannot execute keeps the existing plan, rather than failing the
/// statement.
///
/// The multi-input route skips the aggregate split, and the split is what refuses a tree the runtime
/// cannot run. The native node is chosen while planning the logical plan, so a refusal the physical
/// layer raises later is an error the statement reports, not a fall back - which would turn a query
/// the existing plan answers into a failure. A grouping by a value field needs a per-point adapter
/// the runtime does not have, so it is checked here on the shape that would otherwise reach the
/// node: two aggregates, one per logical table of one physical table, which is the multi-input case.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_declines_a_tree_the_runtime_cannot_execute() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_unexecutable_tree",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_unexec_phy (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE
        ) ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE native_unexec_demo (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_unexec_phy');
        CREATE TABLE native_unexec_other (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_unexec_phy');
        INSERT INTO native_unexec_demo (host, ts, val) VALUES ('a', 0, 2), ('b', 0, 4);
        INSERT INTO native_unexec_other (host, ts, val) VALUES ('a', 0, 8), ('b', 0, 8);"#,
        QueryContext::arc(),
    )
    .await;

    // The outer `count` clears the labels the inner grouping writes, so no column check stops this
    // one: the refusal is the grouping by a value field, which the runtime has no adapter for.
    let query = "count(count by (val) (native_unexec_demo)) / sum(native_unexec_other)";
    let sql = format!("TQL EVAL (0, 0, '1s') {query}");
    let (_, legacy_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
    assert!(
        !legacy_rows.is_empty(),
        "{query}: the existing plan must answer this, otherwise there is nothing to keep"
    );

    observer.clear();
    let (_, native_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    assert_eq!(
        native_rows, legacy_rows,
        "{query}: a tree the runtime cannot execute must keep the existing plan"
    );
    assert!(
        observer.fused().is_none(),
        "{query}: the route must decline it instead of planning a node the runtime refuses"
    );
}

/// A tag column named `__name__` contradicts the metric name the table gives every series it holds,
/// and the native route has no label to read it as: it would merge the series the column tells
/// apart. The route declines such a table, so the statement keeps the existing plan's answer.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_declines_a_tag_named_metric_name() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_tag_named_metric_name",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_named (
            ts TIMESTAMP TIME INDEX,
            "__name__" STRING,
            host STRING,
            val DOUBLE,
            PRIMARY KEY ("__name__", host)
        );
        INSERT INTO native_named VALUES (0, 'a', 'h', 1), (0, 'b', 'h', 2);"#,
        QueryContext::arc(),
    )
    .await;

    for query in [
        "native_named",
        "sum by (__name__) (native_named)",
        "count_over_time(native_named[1m])",
    ] {
        let sql = format!("TQL EVAL (0, 0, '1s') {query}");
        let (_, legacy_rows) =
            native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(legacy_rows.len(), 2, "{query}: {legacy_rows:?}");
        observer.clear();
        let (_, native_rows) =
            native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
        assert_eq!(native_rows, legacy_rows, "{query}");
        assert!(
            observer.fused().is_none(),
            "{query}: the route must decline"
        );
    }
}

/// A `quantile` over one logical table, divided by a `sum` over another, answers natively and the
/// rows are the existing plan's.
///
/// The two tables are one physical table, so the statement has two inputs and the multi-input route
/// answers it without the aggregate split - the split is what refuses a `quantile`, whose state is
/// the group's samples rather than one of the eight fixed-width accumulators. The runtime computes
/// the `Complete` stage of a `quantile` itself, so the tree is answerable and the route adopts it;
/// the answer must still be the existing plan's. Both tables carry samples of 2 and 4 on the left,
/// so the median is their midpoint 3, and of 8 and 8 on the right, so the sum is 16: the answer is
/// one unlabelled row at `3 / 16`.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_answers_a_quantile_over_a_multi_input_tree() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_quantile_multi_input",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_quant_phy (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE
        ) ENGINE=metric WITH ("physical_metric_table" = "true");
        CREATE TABLE native_quant_demo (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_quant_phy');
        CREATE TABLE native_quant_other (
            host STRING,
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            PRIMARY KEY (host)
        ) ENGINE=metric WITH ("on_physical_table" = 'native_quant_phy');
        INSERT INTO native_quant_demo (host, ts, val) VALUES ('a', 0, 2), ('b', 0, 4);
        INSERT INTO native_quant_other (host, ts, val) VALUES ('a', 0, 8), ('b', 0, 8);"#,
        QueryContext::arc(),
    )
    .await;

    let query = "quantile(0.5, native_quant_demo) / sum(native_quant_other)";
    let sql = format!("TQL EVAL (0, 0, '1s') {query}");
    let (_, legacy_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
    assert_eq!(
        legacy_rows,
        vec![(0, std::collections::BTreeMap::new(), 3.0 / 16.0)],
        "{query}: the existing plan answers the median divided by the sum"
    );

    observer.clear();
    let (_, native_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    assert!(
        observer.fused().is_some(),
        "{query}: the runtime computes a `Complete` quantile, so the route answers the tree: {}",
        display_plan_tree(&observer.root_plan())
    );
    assert_eq!(
        native_rows, legacy_rows,
        "{query}: the native answer must be the existing plan's rows"
    );
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

    // A subquery recurses through the expression tree for the same delta-temporality guard.
    // Compare against the existing route and assert that native execution declines both functions.
    for query in [
        format!("max_over_time(rate({TABLE}[2m])[2m:1m])"),
        format!("max_over_time(increase({TABLE}[2m])[2m:1m])"),
    ] {
        let sql = format!("TQL EVAL (120, 120, '1m') {query}");
        let legacy_rows = native_marker_rows(native_tql_output(&instance, &sql, false).await).await;
        assert!(
            !legacy_rows.is_empty(),
            "legacy subquery `{query}` must return rows"
        );
        observer.clear();
        let native_rows = native_marker_rows(native_tql_output(&instance, &sql, true).await).await;
        assert_eq!(
            native_rows, legacy_rows,
            "`{query}`: same rows on both routes"
        );
        assert!(
            observer.fused().is_none(),
            "`{query}`: delta temporality inside a subquery must decline native planning"
        );
    }

    // Only the denominator carries the temporality tag; the rated counter is unmarked.
    // Its zero-point extrapolation yields a rate of 0.25. The second form also requires clamp.
    execute_all(
        &instance,
        &format!(
            "INSERT INTO {TABLE} (host, ts, val, otlp_aggregation_temporality) VALUES ('a', 60000, 2, '{GREPTIME_TEMPORALITY_DELTA}'), ('a', 120000, 2, '{GREPTIME_TEMPORALITY_DELTA}')"
        ),
        QueryContext::arc(),
    )
    .await;
    for (query, expected) in [
        (format!("rate({CONTROL}[2m]) / on(host) {TABLE}"), 1.0 / 8.0),
        (
            format!("rate({CONTROL}[2m]) / on(host) clamp({TABLE}, 0, 1)"),
            1.0 / 4.0,
        ),
    ] {
        let sql = format!("TQL EVAL (120, 120, '1m') {query}");
        observer.clear();
        let (columns, rows) =
            native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
        // The value is named after the statement's expression, not `val`, and the rows answer
        // reports the per-row name column among its labels; a division drops the name.
        let mut expected_columns = vec![
            "__name__".to_string(),
            "host".to_string(),
            "ts".to_string(),
            expression_name(&query),
        ];
        expected_columns.sort();
        assert_eq!(columns, expected_columns, "{query}");
        assert_eq!(rows.len(), 1, "{query}: {rows:?}");
        assert_eq!(rows[0].0, 120_000, "{query}");
        assert_eq!(
            rows[0].1,
            [("host".to_string(), "a".to_string())].into(),
            "{query}"
        );
        assert!((rows[0].2 - expected).abs() < 1e-10, "{query}: {rows:?}");
        let binary = observer.two_input_fused();
        let display = datafusion::physical_plan::displayable(binary.as_ref())
            .indent(true)
            .to_string();
        assert!(display.contains("Binary"), "{query}: {display}");
        assert!(
            !datafusion::physical_plan::displayable(observer.root_plan().as_ref())
                .indent(true)
                .to_string()
                .contains("JoinExec"),
            "{query}"
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

/// Legacy wrappers over a native binary retain the binary's values and evaluation grid.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_binary_inside_scalar_and_quantile() {
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_binary_inside_scalar_and_quantile",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE nested_native_a (job STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (job));          CREATE TABLE nested_native_b (job STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (job));          INSERT INTO nested_native_a VALUES ('api', 60000, 6), ('api', 120000, 8);          INSERT INTO nested_native_b VALUES ('api', 60000, 2), ('api', 120000, 4);",
        QueryContext::arc(),
    )
    .await;
    for query in [
        "scalar(nested_native_a + on(job) nested_native_b)",
        "quantile(0.5, nested_native_a + on(job) nested_native_b)",
    ] {
        let sql = format!("TQL EVAL (60, 120, '1m') {query}");
        let (_, legacy) =
            native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
        observer.clear();
        let (_, rows) = native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
        assert_eq!(rows, legacy, "{query}");
        assert_eq!(
            rows,
            vec![
                (60_000, Default::default(), 8.0),
                (120_000, Default::default(), 12.0)
            ],
            "{query}"
        );
        let binary = observer.two_input_fused();
        let display = datafusion::physical_plan::displayable(binary.as_ref())
            .indent(true)
            .to_string();
        assert!(display.contains("Binary"), "{query}: {display}");
        let root = datafusion::physical_plan::displayable(observer.root_plan().as_ref())
            .indent(true)
            .to_string();
        assert!(!root.contains("JoinExec"), "{query}: {root}");
    }
    let query = "scalar(clamp(nested_native_a, 0, 1) / on(job) nested_native_b)";
    let sql = format!("TQL EVAL (60, 120, '1m') {query}");
    observer.clear();
    let (_, rows) = native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    assert_eq!(
        rows,
        vec![
            (60_000, Default::default(), 0.5),
            (120_000, Default::default(), 0.25)
        ]
    );
    let binary = observer.two_input_fused();
    let display = datafusion::physical_plan::displayable(binary.as_ref())
        .indent(true)
        .to_string();
    assert!(
        display.contains("Binary") && display.contains("clamp"),
        "{query}: {display}"
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

/// What a TQL answer says, in a form both routes can be held to.
///
/// The two routes neither order their columns alike nor name a value column alike - the fused node
/// names it after the expression it answers (`avg(m).i`), the existing plan after the aggregate it
/// folds (`avg(m.field_0)`) - so neither the column order nor a value column's name is compared.
/// What is the same column on both routes is the time index, each label, and one value column per
/// field: a row is the time, its labels by name, and its values in column order, and the value types
/// are compared apart from the rows. The rows root of the native route also appends a `__name__`
/// column that reports no name for a computed value; it is not one of the answer's labels, so it is
/// left out here.
#[derive(Debug, PartialEq)]
struct Answer {
    value_types: Vec<DataType>,
    rows: Vec<(i64, std::collections::BTreeMap<String, String>, Vec<String>)>,
}

/// Reads a TQL answer into an [`Answer`].
async fn native_answer(output: Output) -> Answer {
    use datafusion::arrow::util::display::array_value_to_string;
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    let mut value_types: Option<Vec<DataType>> = None;
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let schema = batch.schema();
        let mut time = None;
        let mut labels = Vec::new();
        let mut values = Vec::new();
        for (index, (field, _)) in schema.fields().iter().zip(batch.columns()).enumerate() {
            match field.data_type() {
                DataType::Timestamp(..) => time = Some(index),
                DataType::Utf8 | DataType::Dictionary(_, _) => {
                    if !datatypes::schema::is_metric_name_field(field) {
                        labels.push((index, field.name().clone()));
                    }
                }
                _ => values.push(index),
            }
        }
        let time = time.expect("the answer reports a time index");
        assert!(!values.is_empty(), "the answer reports a value column");
        let types = values
            .iter()
            .map(|index| schema.field(*index).data_type().clone())
            .collect::<Vec<_>>();
        match &value_types {
            Some(seen) => assert_eq!(seen, &types, "every batch declares the same values"),
            None => value_types = Some(types),
        }
        let timestamps = cast(batch.column(time).as_ref(), &DataType::Int64)
            .expect("the time index is a timestamp")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        for row in 0..batch.num_rows() {
            rows.push((
                timestamps.value(row),
                labels
                    .iter()
                    .map(|(index, name)| {
                        (
                            name.clone(),
                            array_value_to_string(batch.column(*index).as_ref(), row).unwrap(),
                        )
                    })
                    .collect(),
                values
                    .iter()
                    .map(|index| array_value_to_string(batch.column(*index).as_ref(), row).unwrap())
                    .collect(),
            ));
        }
    }
    rows.sort();
    Answer {
        value_types: value_types.expect("the answer reports a value column"),
        rows,
    }
}

/// A numeric field the raw series scan casts to a float is admitted only under the shapes whose
/// existing plan publishes a `Float64` for it too, and the answer is that plan's answer.
///
/// The table holds one integer and one float field, so every statement below reads both: the scan
/// casts the integer to a `Float64` and the fused node declares one `Float64` value column per field.
/// That is the column the existing plan publishes for an arithmetic operand, for a range function and
/// for `min`/`max`/`avg`, so those shapes are admitted and must answer the same columns and rows. A
/// bare selector and `sum` keep the existing plan instead: the first copies the field's own type and
/// the second folds an integer as an integer, so a `Float64` column would not be the column reported.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_answers_a_numeric_field_where_the_shape_publishes_a_float() {
    common_telemetry::init_default_ut_logging();
    const TABLE: &str = "native_numeric_shape";
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_numeric_shape",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE {TABLE} (host STRING, ts TIMESTAMP(3) TIME INDEX, i BIGINT, f DOUBLE, PRIMARY KEY (host)); \
             INSERT INTO {TABLE} (host, ts, i, f) VALUES \
             ('a', 0, 1, 1.5), ('b', 0, 2, 2.5), \
             ('a', 1000, 3, 3.5), ('b', 1000, 4, 4.5), \
             ('a', 2000, 5, 5.5), ('b', 2000, 6, 6.5);"
        ),
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        &format!("ADMIN FLUSH_TABLE('{TABLE}')"),
        QueryContext::arc(),
    )
    .await;

    for query in [
        // Arithmetic casts both operands, so `i` and `f` are folded as floats.
        format!("{TABLE} * 2"),
        format!("{TABLE} - {TABLE}"),
        // `min`, `max` and `avg` answer a `Float64` for every numeric input.
        format!("min({TABLE})"),
        format!("max({TABLE})"),
        format!("avg({TABLE})"),
        // A range function's value window and answer are both `Float64`.
        format!("rate({TABLE}[2s])"),
        format!("sum(rate({TABLE}[2s]))"),
    ] {
        let sql = format!("TQL EVAL (2, 2, '1s') {query}");
        observer.clear();
        let native = native_answer(native_tql_output(&instance, &sql, true).await).await;
        let answering_node = observer.answering_node();
        assert_eq!(
            Some("FusedSeriesExec".to_string()),
            answering_node,
            "`{query}` must answer from the fused node, not the existing plan"
        );
        // Both fields of the table are value columns, whatever the shape: the integer is read as the
        // float the existing plan publishes for this shape.
        assert_eq!(
            vec![DataType::Float64, DataType::Float64],
            native.value_types,
            "`{query}`: one float value column per field of the table"
        );
        let legacy = native_answer(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(
            native, legacy,
            "`{query}`: the value types and the rows must be the existing plan's"
        );
    }

    // A bare selector copies the value column's own type, so it keeps the existing plan: the
    // `BIGINT` field would come back as a `Float64` under the fused node.
    for query in [TABLE.to_string(), format!("sum({TABLE})")] {
        let sql = format!("TQL EVAL (2, 2, '1s') {query}");
        observer.clear();
        let native = native_answer(native_tql_output(&instance, &sql, true).await).await;
        assert!(
            observer.fused().is_none(),
            "`{query}` must keep the existing plan: a shape that does not publish a float for an \
             integer field would report a different column than the existing plan"
        );
        let legacy = native_answer(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(native, legacy, "`{query}`: the existing plan answers it");
        // The reason the shape keeps the existing plan: the integer field is reported as an integer,
        // which is the column the fused node would have to publish as a `Float64`.
        assert!(
            native.value_types.contains(&DataType::Int64),
            "`{query}`: the existing plan reports the integer field as an `Int64`: {:?}",
            native.value_types
        );
    }

    // A field the raw series preflight refuses has no float to publish, whatever the shape is. These
    // keep the existing plan, which is where their answer comes from - for some of them an error
    // (`sum` of a string has no fold), and that error is the answer the route must not replace.
    const TEXT_TABLE: &str = "native_numeric_shape_text";
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE {TEXT_TABLE} (host STRING, ts TIMESTAMP(3) TIME INDEX, v STRING, PRIMARY KEY (host)); \
             INSERT INTO {TEXT_TABLE} (host, ts, v) VALUES ('a', 0, 'x'), ('a', 1000, 'y'); \
             ADMIN FLUSH_TABLE('{TEXT_TABLE}');"
        ),
        QueryContext::arc(),
    )
    .await;
    for query in [
        TEXT_TABLE.to_string(),
        format!("sum({TEXT_TABLE})"),
        format!("{TEXT_TABLE} * 2"),
        format!("rate({TEXT_TABLE}[2s])"),
    ] {
        let sql = format!("TQL EVAL (2, 2, '1s') {query}");
        observer.clear();
        native_run_ignoring_the_answer(&instance, &sql, true).await;
        assert!(
            observer.fused().is_none(),
            "`{query}`: a string field has no raw representation"
        );
    }
}

/// Runs a TQL statement and drops whatever it answered.
///
/// A statement the existing planner refuses is answered by that refusal - `sum` of a string has no
/// fold - and the route must not replace it with an answer of its own, so a test that only means to
/// hold the route to the existing plan must not unwrap the answer.
async fn native_run_ignoring_the_answer(instance: &Arc<Instance>, sql: &str, enabled: bool) {
    let Ok(output) = instance
        .do_query(sql, native_query_context(enabled, 4))
        .await
        .remove(0)
    else {
        return;
    };
    if let OutputData::Stream(stream) = output.data {
        let _ = collect_batches(stream).await;
    }
}

/// An arithmetic operator casts both operands, but the cast is the operator's, so it reaches whatever
/// an operand answers. Where that operand *folds* raw samples into an integer, the existing plan folds
/// exactly and casts the fold, while the raw series scan casts every sample and only then folds: two
/// integers above `2^53` that cancel answer `1` before the cast and `0` after it. The shape therefore
/// keeps the existing plan, while a fold that is a float fold - `sum` over a range function's window -
/// is admitted as before.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_declines_a_cast_over_an_integer_fold() {
    common_telemetry::init_default_ut_logging();
    const TABLE: &str = "native_late_cast";
    let (standalone, observer) =
        native_test_instance(GreptimeDbStandaloneBuilder::new("native_promql_late_cast")).await;
    let instance = standalone.fe_instance().clone();
    // `9007199254740993` is not a `Float64`: it rounds to `9007199254740992`, which is what makes the
    // order of the cast and the fold visible. Two series, so `sum` folds them into one value per step.
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE {TABLE} (host STRING, ts TIMESTAMP(3) TIME INDEX, v BIGINT, PRIMARY KEY (host)); \
             INSERT INTO {TABLE} (host, ts, v) VALUES \
             ('a', 0, 9007199254740993), ('a', 60000, 9007199254740993), \
             ('b', 0, -9007199254740992), ('b', 60000, -9007199254740992);"
        ),
        QueryContext::arc(),
    )
    .await;
    execute_all(
        &instance,
        &format!("ADMIN FLUSH_TABLE('{TABLE}')"),
        QueryContext::arc(),
    )
    .await;

    // The integer sum is 1, and the cast is 1.0 whichever side of it the answer is read from: the
    // existing plan sums the two `BIGINT`s exactly, so that is the answer these shapes keep.
    for (query, expected) in [
        (format!("sum({TABLE}) * 1"), 1.0),
        (format!("sum({TABLE}) + 1"), 2.0),
    ] {
        let sql = format!("TQL EVAL (0, 0, '1m') {query}");
        let legacy = native_rows(native_tql_output(&instance, &sql, false).await, None).await;
        assert_eq!(
            legacy,
            vec![(0, String::new(), expected)],
            "`{query}`: the existing plan sums the integers before it casts"
        );
        observer.clear();
        let native = native_rows(native_tql_output(&instance, &sql, true).await, None).await;
        assert!(
            observer.fused().is_none(),
            "`{query}` must keep the existing plan: the raw scan would round each value before it \
             summed them"
        );
        assert_eq!(
            native, legacy,
            "`{query}`: an integer fold is exact, and a fused node cannot repeat that"
        );
    }

    // The inner fold is a float fold here, so the outer cast reaches values that were floats already:
    // the route takes the statement and answers the existing plan's rows.
    let sql = format!("TQL EVAL (60000, 60000, '1m') sum(rate({TABLE}[5m]))");
    let legacy = native_rows(native_tql_output(&instance, &sql, false).await, None).await;
    observer.clear();
    let native = native_rows(native_tql_output(&instance, &sql, true).await, None).await;
    assert!(
        observer.fused().is_some(),
        "`sum(rate(...))` folds a float window and must stay admitted"
    );
    assert_eq!(
        native, legacy,
        "`sum(rate(...))`: the fused route reports the existing plan's rows"
    );
}

/// A store configured for the columnar series format publishes samples for sparse metric regions
/// only, and a plain table on such a store reads its rows from the ordinary scan - the reader the
/// existing plan uses for the very same rows. So the raw series path serves the frame from there
/// instead of refusing it: a refusal here is a planning error no caller can turn into a fallback,
/// because the logical plan above the leaf is already the fused node when the region is asked.
///
/// The answer is therefore the existing plan's answer, on either route. Both value kinds are here
/// because the limit is the reader's and not the field's: an integer field is cast to the float the
/// fused node publishes, and a `Double` one is a float already. `sum_over_time` folds the window and
/// `avg_over_time` folds it and divides - two answers that would look alike if one of them were 0.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_columnar_config_answers_from_the_ordinary_scan() {
    common_telemetry::init_default_ut_logging();
    const INTEGER: &str = "native_columnar_int";
    const FLOAT: &str = "native_columnar_float";
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
            "CREATE TABLE {INTEGER} (host STRING, ts TIMESTAMP(3) TIME INDEX, val BIGINT, PRIMARY KEY (host)) ENGINE=mito; \
             INSERT INTO {INTEGER} (host, ts, val) VALUES ('a', 60000, 10), ('a', 120000, 30); \
             CREATE TABLE {FLOAT} (host STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host)) ENGINE=mito; \
             INSERT INTO {FLOAT} (host, ts, val) VALUES ('a', 60000, 10), ('a', 120000, 30);"
        ),
        QueryContext::arc(),
    )
    .await;
    for table in [INTEGER, FLOAT] {
        // Both samples of the one series fall in the 2m window: 10 + 30, and that sum over the two
        // samples it averages.
        for (query, expected) in [("sum_over_time", 40.0), ("avg_over_time", 20.0)] {
            let sql = format!("TQL EVAL (120, 120, '1m') {query}({table}[2m])");
            // The existing plan answer first, so a red native query is about this route.
            let legacy = native_rows(
                native_tql_output(&instance, &sql, false).await,
                Some("host"),
            )
            .await;
            assert_eq!(
                legacy,
                vec![(120_000, "a".to_string(), expected)],
                "`{sql}`: the existing plan answers it"
            );
            observer.clear();
            let native =
                native_rows(native_tql_output(&instance, &sql, true).await, Some("host")).await;
            assert_eq!(
                native, legacy,
                "`{sql}`: the fused route reports the existing plan's rows"
            );
            // The route takes the statement and the region serves it from the ordinary scan, rather
            // than the statement failing to plan because the columnar reader has no samples for it.
            assert!(
                observer.fused().is_some(),
                "`{sql}`: the fused node reads frames the region serves"
            );
        }
    }
    // A bare selector keeps its plan for a type reason, which is what this route decides: it reports
    // the column's own `BIGINT`, which no fused node publishes.
    let sql = format!("TQL EVAL (120, 120, '1m') {INTEGER}");
    observer.clear();
    let _ = native_tql_output(&instance, &sql, true).await;
    assert!(
        observer.fused().is_none(),
        "a bare selector over an integer field must keep the existing plan"
    );
}

/// The positive counterpart of the test above: a sparse physical metric table whose series
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
    let region_scan = region_series_scan(&observer);
    let series_scan = child
        .downcast_ref::<SeriesScanExec>()
        .or(region_scan.as_deref())
        .expect("the statement reads a prepared series scan");
    let RawLabelSource::PrimaryKey {
        metric_name,
        physical_metadata,
        logical_tags,
    } = series_scan.scan_binding()
    else {
        panic!("the fused scan must bind labels from the primary key");
    };
    assert!(matches!(
        series_scan.label_source(),
        RawLabelSource::Explicit
    ));
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
    let output_values = fused_metric_set(&plan)
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
    // An instant selector preserves identities across scan partitions even when frames carry
    // explicit labels. The binding, rather than the frame encoding, proves completeness.
    observer.clear();
    let instant = format!("TQL EVAL (120, 120, '1m') {TABLE}");
    native_result_rows(native_tql_output(&instance, &instant, true).await).await;
    let instant_plan = observer.take();
    let fused = instant_plan
        .downcast_ref::<promql::extension_plan::FusedSeriesExec>()
        .unwrap();
    // The fused node reads its region through a raw-key router rather than directly through the
    // scan, so the scan's own binding, not the router's name, is what proves identities complete.
    assert!(
        fused.has_partition_disjoint_identities() || region_series_scan(&observer).is_some(),
        "the statement reads its series through a prepared scan or a disjoint identity"
    );
    let matched = format!("TQL EVAL (120, 120, '1m') sum_over_time({TABLE}{{host=\"b\"}}[2m])");
    let matched_rows = native_result_rows(native_tql_output(&instance, &matched, true).await).await;
    assert_eq!(matched_rows, vec![(120_000, "b".to_string(), 80.0)]);
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
    let output_values = fused_metric_set(&plan)
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
    let output_values = fused_metric_set(&plan)
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

/// One row of a native or legacy answer as (time, host, value): the host is empty for an unlabelled
/// series, and the value is rendered so a NaN compares equal to a NaN.
async fn dynamic_scalar_rows(output: Output) -> Vec<(i64, String, String)> {
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let schema = batch.df_record_batch().schema();
        let column = |accept: &dyn Fn(&DataType) -> bool| {
            schema
                .fields()
                .iter()
                .position(|field| accept(field.data_type()))
                .map(|index| batch.column(index).clone())
        };
        let timestamps = column(&|ty| matches!(ty, DataType::Timestamp(_, _))).unwrap();
        let timestamps = timestamps
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let values = column(&|ty| ty == &DataType::Float64).unwrap();
        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        let hosts = batch
            .column_by_name("host")
            .map(|hosts| cast(hosts.as_ref(), &DataType::Utf8).unwrap());
        for row in 0..batch.num_rows() {
            let host = hosts
                .as_ref()
                .map(|hosts| {
                    let hosts = hosts.as_any().downcast_ref::<StringArray>().unwrap();
                    hosts.value(row).to_string()
                })
                .unwrap_or_default();
            let value = if values.is_null(row) {
                "null".to_string()
            } else {
                format!("{:?}", values.value(row))
            };
            rows.push((timestamps.value(row), host, value));
        }
    }
    rows.sort();
    rows
}

fn expected_rows(rows: &[(i64, &str, f64)]) -> Vec<(i64, String, String)> {
    let mut rows = rows
        .iter()
        .map(|(ts, host, value)| (*ts, host.to_string(), format!("{value:?}")))
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// Dynamic scalars run natively and answer what Prometheus answers: a varying scalar is broadcast
/// over a vector, and `time()` needs no scan. The existing planner refuses `vector(time())` and a
/// date function without an argument.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_dynamic_scalars_match_prometheus_and_legacy() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_dynamic_scalar_e2e")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE dyn_scalar (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         INSERT INTO dyn_scalar (host, ts, val) VALUES \
         ('a', 0, 1), ('a', 10000, 2), ('a', 20000, 3), ('b', 20000, 10), ('b', 30000, 20)",
        QueryContext::arc(),
    )
    .await;
    // (query, Prometheus answer, scan children, whether the existing planner agrees, or `None`
    // where it refuses the query)
    type Case = (
        &'static str,
        Vec<(i64, &'static str, f64)>,
        usize,
        Option<bool>,
    );
    let cases: Vec<Case> = vec![
        (
            "TQL EVAL (0, 40, '10s', '5s') time() - timestamp(dyn_scalar)",
            vec![
                (0, "a", 0.0),
                (10_000, "a", 0.0),
                (20_000, "a", 0.0),
                (20_000, "b", 0.0),
                (30_000, "b", 0.0),
            ],
            1,
            Some(true),
        ),
        (
            "TQL EVAL (0, 20, '10s') time() * 2",
            vec![(0, "", 0.0), (10_000, "", 20.0), (20_000, "", 40.0)],
            0,
            Some(true),
        ),
        (
            "TQL EVAL (0, 20, '10s') vector(time())",
            vec![(0, "", 0.0), (10_000, "", 10.0), (20_000, "", 20.0)],
            0,
            None,
        ),
        (
            "TQL EVAL (3590, 3610, '10s') hour()",
            vec![
                (3_590_000, "", 0.0),
                (3_600_000, "", 1.0),
                (3_610_000, "", 1.0),
            ],
            0,
            None,
        ),
    ];
    for (sql, expected, children, legacy_agrees) in cases {
        observer.clear();
        let native = dynamic_scalar_rows(native_tql_output(&instance, sql, true).await).await;
        let plan = observer.take();
        assert_eq!(plan.name(), "FusedSeriesExec", "{sql}");
        assert_eq!(plan.children().len(), children, "{sql}");
        assert_eq!(native, expected_rows(&expected), "{sql}");
        let Some(legacy_agrees) = legacy_agrees else {
            continue;
        };
        let legacy = dynamic_scalar_rows(native_tql_output(&instance, sql, false).await).await;
        assert_eq!(
            native == legacy,
            legacy_agrees,
            "{sql}: legacy answered {legacy:?}"
        );
    }

    // `count_values` names a label no scan carries, so the statement keeps a plan that reports it.
    let sql = "TQL EVAL (0, 0, '1s') count_values(\"bucket\", vector(1))";
    // The native route answers `count_values` itself now, so this is its own answer: one series at
    // t=0 whose generated label is "1" and whose count is 1. The value column is named after the
    // expression, and the aggregate drops the metric name - as Prometheus does - so the declared
    // name column is there and null; it is the one column the existing planner's answer does not
    // have, and is left out of the columns compared below.
    let native = named_rows_text(
        native_tql_output(&instance, sql, true).await,
        ReportedName::Nothing,
    )
    .await;
    assert_eq!(
        native,
        "+---------------------+-----------------------------------+--------+\
        \n| time                | count_values(\"bucket\", vector(1)) | bucket |\
        \n+---------------------+-----------------------------------+--------+\
        \n| 1970-01-01T00:00:00 | 1.0                               | 1      |\
        \n+---------------------+-----------------------------------+--------+"
    );
}

/// A same-table scalar broadcast gathers simultaneous series from distinct physical regions.
/// `scalar(m)` is NaN for two series, so `m == bool scalar(m)` answers zero for both. Evaluating
/// the scalar separately in each region would instead compare each series to itself and answer one.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_same_table_partitioned_scalar_broadcast_gathers_whole_vector() {
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_partitioned_scalar_broadcast",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_partitioned_scalar (host STRING, ts TIMESTAMP TIME INDEX, \
         val DOUBLE, PRIMARY KEY(host)) PARTITION ON COLUMNS (host) (host < 'm', host >= 'm'); \
         INSERT INTO native_partitioned_scalar VALUES ('a', 5000, 2), ('z', 5000, 7)",
        QueryContext::arc(),
    )
    .await;

    let sql =
        "TQL EVAL (5, 5, '1s') native_partitioned_scalar == bool scalar(native_partitioned_scalar)";
    observer.clear();
    let rows = native_result_rows(native_tql_output(&instance, sql, true).await).await;
    assert_eq!(
        rows,
        vec![(5_000, "a".to_string(), 0.0), (5_000, "z".to_string(), 0.0)],
        "the global scalar is NaN and equality is false for both series"
    );

    let root = observer.root_plan();
    let fused = find_plan_node(&root, "FusedSeriesExec").unwrap_or_else(|| {
        panic!(
            "the frontend must answer natively: {}",
            display_plan_tree(&root)
        )
    });
    assert_eq!(
        observer.answering_node().as_deref(),
        Some("FusedSeriesExec")
    );
    assert_eq!(
        fused.children().len(),
        2,
        "one input per selector: {}",
        display_plan_tree(&root)
    );
    assert_eq!(
        fused.output_partitioning().partition_count(),
        1,
        "the whole vector is gathered: {}",
        display_plan_tree(&root)
    );
    let shape = native_plan_shape(&root);
    assert_eq!(
        shape.regions_per_scan,
        vec![2, 2],
        "each selector reads both declared host ranges: {}",
        display_plan_tree(&root)
    );
    // The disjoint declared ranges place `a` and `z` in different physical regions; both scans
    // select those regions. Compare the two NaN equalities with a literal false control as well.
    let literal = "TQL EVAL (5, 5, '1s') native_partitioned_scalar == bool 0";
    let legacy = native_result_rows(native_tql_output(&instance, literal, false).await).await;
    assert_eq!(
        rows, legacy,
        "a literal false comparison yields the same labelled rows"
    );
}

/// The `ts` and first `Float64` column of a `TQL EVAL` answer, as values.
///
/// Answers are compared this way rather than as printed tables: the native route names a field
/// after its compiled root while the existing planner names a computed column after the
/// expression, and that difference is older than the native route's segmentation. A statement that
/// groups nothing also has no label column, which `native_result_rows` needs, so this reads the
/// two columns an aggregate answers with.
fn eval_values(output: Output) -> Vec<(i64, f64)> {
    let batches = match output.data {
        OutputData::Stream(stream) => futures::executor::block_on(collect_batches(stream)).unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    batches
        .iter()
        .flat_map(|batch| {
            let timestamps = batch
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let values = batch
                .columns()
                .iter()
                .find_map(|column| column.as_any().downcast_ref::<Float64Array>())
                .unwrap();
            (0..batch.num_rows())
                .map(move |row| (timestamps.value(row), values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// A `TQL EVAL` answer as the set of rows it holds: column names, then sorted rows.
///
/// Three things differ between the routes and none of them is the answer. This route names a value
/// column after its compiled root (`val`) while the existing planner names it after the expression
/// (`abs(val)`, `sum(topk_metric.val)`); the two emit columns in a different order; and this one is
/// partitioned, so its rows arrive by partition rather than in one sequence. Comparing the answers
/// as what they are - a set of (labels, timestamp, value) rows - leaves the values as the only thing
/// the assertion can fail on.
///
/// The metric-name column is the one column the existing route does not have, so `reported` says
/// what the native answer's name column holds and the column is checked and left out - never
/// silently ignored.
fn eval_text(output: Output) -> String {
    eval_text_rows(output, ReportedName::NoColumn)
}

/// [`eval_text`] of an answer that names no series: the declared name column is there and null in
/// every row.
fn eval_text_nameless(output: Output) -> String {
    eval_text_rows(output, ReportedName::Nothing)
}

fn eval_text_rows(output: Output, reported: ReportedName<'_>) -> String {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};

    let batches = match output.data {
        OutputData::Stream(stream) => futures::executor::block_on(collect_batches(stream)).unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => unreachable!("TQL EVAL returns record batches"),
    };
    let mut named: Option<Vec<String>> = None;
    let mut rows: Vec<Vec<String>> = Vec::new();
    for batch in batches {
        let batch = batch.into_df_record_batch();
        let name_index = checked_name_column(batch.schema().fields(), batch.columns(), reported);
        let fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let name = if field.data_type() == &DataType::Float64 {
                    "value".to_string()
                } else {
                    field.name().clone()
                };
                (name, field.data_type().clone(), field.is_nullable())
            })
            .collect::<Vec<_>>();
        // One column order for every batch: by name, so the two routes agree on it. The name column
        // is the one this route declares and the existing one does not.
        let mut order = (0..fields.len())
            .filter(|index| Some(*index) != name_index)
            .collect::<Vec<_>>();
        order.sort_by(|left, right| fields[*left].0.cmp(&fields[*right].0));
        let names = order
            .iter()
            .map(|index| fields[*index].0.clone())
            .collect::<Vec<_>>();
        match &named {
            None => named = Some(names),
            Some(first) => assert_eq!(first, &names, "the answers have different columns"),
        }
        let schema = Arc::new(Schema::new(
            order
                .iter()
                .map(|index| {
                    let (name, data_type, nullable) = fields[*index].clone();
                    Arc::new(Field::new(name, data_type, nullable))
                })
                .collect::<Vec<_>>(),
        ));
        let columns = order
            .iter()
            .map(|index| batch.column(*index).clone())
            .collect::<Vec<_>>();
        let reordered =
            RecordBatch::try_new(schema, columns).expect("the columns keep their types");
        let formatters = reordered
            .columns()
            .iter()
            .map(|column| {
                ArrayFormatter::try_new(column.as_ref(), &FormatOptions::default())
                    .expect("the answer formats")
            })
            .collect::<Vec<_>>();
        for row in 0..reordered.num_rows() {
            rows.push(
                formatters
                    .iter()
                    .map(|formatter| formatter.value(row).to_string())
                    .collect(),
            );
        }
    }
    rows.sort();
    let mut out = named.unwrap_or_default().join(" | ");
    for row in rows {
        out.push('\n');
        out.push_str(&row.join(" | "));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_sum_topk_uses_global_topk_segment() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_sum_topk_segments")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE topk_metric_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table' = 'true'); \
         CREATE TABLE topk_metric (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table' = 'topk_metric_phy'); \
         INSERT INTO topk_metric (host, ts, val) VALUES \
         ('a', 0, 2), ('b', 0, 7), ('c', 0, 11), ('a', 1000, 3), ('b', 1000, 13), ('c', 1000, 5); \
         CREATE TABLE topk_ordinary (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         INSERT INTO topk_ordinary (host, ts, val) VALUES \
         ('a', 0, 2), ('b', 0, 7), ('c', 0, 11), ('a', 1000, 3), ('b', 1000, 13), ('c', 1000, 5)",
        QueryContext::arc(),
    )
    .await;
    fn walk(
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        found: &mut Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) {
        if plan.name() == "FusedSeriesExec" {
            found.push(plan.clone());
        }
        for child in plan.children() {
            walk(child, found);
        }
    }

    for table in ["topk_metric", "topk_ordinary"] {
        let sql = format!("TQL EVAL (0, 1, '1s') sum(topk(1, {table}))");
        observer.clear();
        let native_output = native_tql_output(&instance, &sql, true).await;
        let batches = match native_output.data {
            OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
            OutputData::RecordBatches(batches) => batches,
            _ => unreachable!("TQL EVAL returns record batches"),
        };
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let timestamps = batch
                    .column_by_name("ts")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let values = batch
                    .columns()
                    .iter()
                    .find_map(|column| column.as_any().downcast_ref::<Float64Array>())
                    .unwrap();
                (0..batch.num_rows())
                    .map(move |row| (timestamps.value(row), values.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![(0, 11.0), (1_000, 13.0)],
            "sum(topk(1, ...)) must return the global maximum for {table}"
        );

        // The statement splits in two: the per-series prefix stays in the region, and the global
        // `topk` and `sum` run above it. So the frontend tree holds one segment, and the other is the
        // root of the region's own execution - which is where the exchange that gathers it lives.
        let root = observer.root_plan();
        let mut frontend = Vec::new();
        walk(&root, &mut frontend);
        assert_eq!(
            frontend.len(),
            1,
            "the global sum and its topk are one frontend segment: {}",
            datafusion::physical_plan::displayable(root.as_ref()).indent(true)
        );
        let parent = &frontend[0];
        assert_eq!(
            parent.output_partitioning().partition_count(),
            1,
            "a global topk answers one partition"
        );
        let parent_display = datafusion::physical_plan::displayable(parent.as_ref())
            .one_line()
            .to_string();
        let parent_tree = parent_display
            .split_once('\n')
            .expect("the parent renders its own tree")
            .1;
        assert!(
            parent_tree.contains("Aggregate: sum by [] complete"),
            "the parent owns the complete sum: {parent_tree}"
        );
        assert!(
            parent_tree.contains("Vector: topk 1 by []"),
            "the parent owns the global topk: {parent_tree}"
        );
        assert!(
            parent_tree.contains("Input: evaluated samples source=0"),
            "the parent reads its child's evaluated frame, not a scan: {parent_tree}"
        );
        assert!(
            !parent_tree.contains("Instant"),
            "the prefix was cut out, so no instant evaluation is left in the parent: {parent_tree}"
        );

        let mut region_fused = Vec::new();
        for plan in observer.region_plans() {
            walk(&plan, &mut region_fused);
        }
        assert_eq!(
            region_fused.len(),
            1,
            "the prefix is the region's own segment"
        );
        let child = &region_fused[0];
        assert!(
            child.output_partitioning().partition_count() > 1,
            "the prefix keeps the scan's partitions, which is the point of cutting below the topk"
        );
        let child_display = datafusion::physical_plan::displayable(child.as_ref())
            .one_line()
            .to_string();
        // Each segment renders its own compiled tree below the operator line.
        let child_tree = child_display
            .split_once('\n')
            .expect("the child renders its own tree")
            .1;
        assert!(
            child_tree.contains("Instant"),
            "the child owns the per-series prefix: {child_tree}"
        );
        assert!(
            !child_tree.contains("topk") && !child_tree.contains("Aggregate"),
            "the global operations stayed in the parent: {child_tree}"
        );
        let declaration =
            datatypes::series::frame::SeriesFrameDeclaration::parse(child.schema().as_ref())
                .expect("the child publishes a series frame");
        assert_eq!(
            declaration.content(),
            datatypes::series::frame::FrameContent::Evaluated,
            "the segment hands its parent computed values"
        );
        assert_eq!(
            declaration.labels(),
            datatypes::series::frame::FrameLabelMode::Explicit,
            "the segment names each series"
        );
        // The region root is normalized before it leaves: ordinary coalescing would interleave the
        // segments of one evaluated chain, which the decoder refuses.
        let region_root = observer
            .region_plans()
            .into_iter()
            .find(|plan| {
                let mut found = Vec::new();
                walk(plan, &mut found);
                !found.is_empty()
            })
            .expect("the region plan holding the segment was captured");
        assert_eq!(
            region_root.output_partitioning().partition_count(),
            1,
            "the region answers one stream: {}",
            datafusion::physical_plan::displayable(region_root.as_ref()).indent(true)
        );
        // The plan the wrapper sees is the optimized one, so the router may be the root itself or sit
        // directly above the segment.
        let exchange = if region_root.name() == "SeriesRepartitionExec" {
            region_root.clone()
        } else {
            let region_children = region_root.children();
            match region_children.first() {
                Some(only) if only.name() == "SeriesRepartitionExec" => Arc::clone(only),
                _ => panic!(
                    "the region exit is the series-aware router: {}",
                    datafusion::physical_plan::displayable(region_root.as_ref()).indent(true)
                ),
            }
        };
        let gathered = exchange.children();
        assert_eq!(gathered.len(), 1, "the router reads one input");
        assert!(
            Arc::ptr_eq(gathered[0], child),
            "the router gathers the segment itself, not something else: {}",
            datafusion::physical_plan::displayable(exchange.as_ref()).indent(true)
        );
        assert_eq!(
            exchange.output_partitioning().partition_count(),
            1,
            "the region answers one stream"
        );
        assert_eq!(
            exchange
                .downcast_ref::<promql::extension_plan::SeriesRepartitionExec>()
                .expect("region exit is a series router")
                .identity_completion(),
            promql::extension_plan::IdentityCompletion::CompleteDisjoint,
            "the root optimizer recognizes complete per-series identities on {table}"
        );

        let legacy = eval_values(native_tql_output(&instance, &sql, false).await);
        assert_eq!(rows, legacy, "native and legacy answers differ for {table}");
    }
}

/// Checks a computed matching-key expression routes evaluated operands into multiple partitions.
async fn assert_computed_matching_key_runtime_parallelism(
    name: &str,
    setup_sql: &str,
    query: &str,
) {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) =
        native_test_instance(GreptimeDbStandaloneBuilder::new(name).with_mito_config(config)).await;
    let instance = standalone.fe_instance().clone();
    execute_all(&instance, setup_sql, QueryContext::arc()).await;

    let sql = format!("TQL EVAL (0, 1, '1s') {query}");
    observer.clear();
    let native_output = native_tql_output(&instance, &sql, true).await;
    let native_rows = native_labelled_rows(native_output).await.1;
    assert_eq!(
        native_rows.len(),
        8,
        "one result per host and timestamp: {native_rows:?}"
    );
    for (ts, labels, value) in &native_rows {
        let host = labels.get("host").expect("output retains host label");
        let val = match host.as_str() {
            "h0" => 1.0,
            "h1" => 2.0,
            "h2" => 3.0,
            "h3" => 4.0,
            other => panic!("unexpected host {other}"),
        };
        assert_eq!(*value, val * 4.0, "host={host}, ts={ts}");
    }

    fn walk(plan: &Arc<dyn ExecutionPlan>, found: &mut Vec<Arc<dyn ExecutionPlan>>) {
        if plan.name() == "FusedSeriesExec" {
            found.push(Arc::clone(plan));
        }
        for child in plan.children() {
            walk(child, found);
        }
    }

    let mut plans = vec![observer.root_plan()];
    plans.extend(observer.region_plans());
    let mut fused = Vec::new();
    for plan in &plans {
        walk(plan, &mut fused);
    }
    let parent = fused
        .iter()
        .find(|plan| {
            plan.children().len() == 2
                && plan.children().iter().all(|child| {
                    child
                        .downcast_ref::<promql::extension_plan::SeriesRepartitionExec>()
                        .is_some_and(|router| {
                            router.routing_policy()
                                == &promql::extension_plan::SeriesRouting::Matching(
                                    promql::vector_match::MatchingLabels::On(vec![
                                        Arc::from("__name__"),
                                        Arc::from("host"),
                                    ]),
                                )
                        })
                })
        })
        .unwrap_or_else(|| panic!("native parent with matching routers not found: {plans:?}"));
    let partition_count = parent.output_partitioning().partition_count();
    assert!(
        partition_count > 1,
        "matching parent must stay parallel: {parent:?}"
    );
    let rows_by_partition = output_values_per_partition(parent)
        .expect("fused native execution exposes per-partition output values");
    let nonempty = rows_by_partition
        .iter()
        .filter(|&&row_count| row_count > 0)
        .count();
    assert!(
        nonempty >= 2,
        "at least two matching output partitions must produce rows; counts={rows_by_partition:?}"
    );

    let legacy_rows = native_labelled_rows(native_tql_output(&instance, &sql, false).await)
        .await
        .1;
    assert_eq!(
        native_rows, legacy_rows,
        "native result must match legacy result"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_computed_matching_key_same_table_is_parallel() {
    assert_computed_matching_key_runtime_parallelism(
        "computed_matching_key_same_table",
        "CREATE TABLE cmk_m (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         INSERT INTO cmk_m VALUES ('h0', 0, 1), ('h1', 0, 2), ('h2', 0, 3), ('h3', 0, 4), \
         ('h0', 1000, 1), ('h1', 1000, 2), ('h2', 1000, 3), ('h3', 1000, 4)",
        "(cmk_m * 2) + on(__name__, host) (cmk_m * 2)",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_computed_matching_key_cross_table_is_parallel() {
    assert_computed_matching_key_runtime_parallelism(
        "computed_matching_key_cross_table",
        "CREATE TABLE cmk_a (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         CREATE TABLE cmk_b (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         INSERT INTO cmk_a VALUES ('h0', 0, 1), ('h1', 0, 2), ('h2', 0, 3), ('h3', 0, 4), \
         ('h0', 1000, 1), ('h1', 1000, 2), ('h2', 1000, 3), ('h3', 1000, 4); \
         INSERT INTO cmk_b VALUES ('h0', 0, 1), ('h1', 0, 2), ('h2', 0, 3), ('h3', 0, 4), \
         ('h0', 1000, 1), ('h1', 1000, 2), ('h2', 1000, 3), ('h3', 1000, 4)",
        "(cmk_a * 2) + on(__name__, host) (cmk_b * 2)",
    )
    .await;
}

/// A cross-table segmented expression matches exact legacy values and uses the segmented route.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_multi_source_segments_match_legacy_values() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_multi_source_segments")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE seg_m (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         CREATE TABLE seg_n (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         INSERT INTO seg_m VALUES ('a', 0, 2), ('b', 0, 6), ('a', 60000, 4), ('b', 60000, 12), ('a', 120000, 8), ('b', 120000, 24); \
         INSERT INTO seg_n VALUES ('a', 0, 1), ('b', 0, 2), ('a', 60000, 2), ('b', 60000, 4), ('a', 120000, 4), ('b', 120000, 8)",
        QueryContext::arc(),
    )
    .await;

    let sql = "TQL EVAL (0, 120, '60s') sum(topk(1, seg_m / seg_n))";
    observer.clear();
    let native = eval_values(native_tql_output(&instance, sql, true).await);
    assert_eq!(native, vec![(0, 3.0), (60_000, 3.0), (120_000, 3.0)]);
    let root = observer.root_plan();
    let fused = fused_nodes_in_root(&root);
    // A cross-table statement splits in two here: the root folds `sum` over `topk`, which reads
    // only the segment below it, and that segment is the `m / n` match over both tables. So the
    // frontend tree holds the root and one segment - unlike a single-table `sum(topk(1, m))`,
    // whose per-series prefix goes into the region and leaves one fused node above it.
    let segment = fused
        .iter()
        .find(|node| node.children().len() == 2)
        .expect("the m/n matching subtree must be one segment over both tables");
    // The engine is configured for four workers, and each table arrives from one region as a
    // single partition. The segment must run at the configured width anyway, so each table's one
    // partition is spread over four; without this the match runs serial while the scans below it
    // are parallel.
    assert_eq!(
        4,
        segment.output_partitioning().partition_count(),
        "a matching segment runs at the configured width, not at its inputs' count"
    );
    let child_debug = segment
        .children()
        .iter()
        .map(|child| debug_merge_scan_child(child))
        .collect::<Vec<_>>();
    // A segment over two tables cannot sit inside either region, so it runs above them and reads
    // each table through that table's own raw series reader. What the assertion has to rule out is
    // the statement falling back at all: the segment exists, it binds one child per source, and
    // each child names its own table rather than both reading one.
    assert!(
        child_debug
            .iter()
            .all(|debug| debug.contains("RawSeriesSource")),
        "each source must be read through its own raw series source: {child_debug:?}"
    );
    assert!(
        child_debug.iter().any(|debug| debug.contains("seg_m"))
            && child_debug.iter().any(|debug| debug.contains("seg_n")),
        "the two children must read different tables: {child_debug:?}"
    );

    let legacy = eval_values(native_tql_output(&instance, sql, false).await);
    assert_eq!(legacy, native, "native/legacy mismatch");
}

/// Recursive native vector matching retains parallel execution and legacy results at nested sites.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_recursive_matching_segments_match_legacy() {
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_recursive_matching",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE rec_a (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         CREATE TABLE rec_b (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         CREATE TABLE rec_c (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         CREATE TABLE rec_d (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         INSERT INTO rec_a VALUES ('a', 0, 2), ('b', 0, 4); \
         INSERT INTO rec_b VALUES ('a', 0, 3), ('b', 0, 5); \
         INSERT INTO rec_c VALUES ('a', 0, 7), ('b', 0, 11); \
         INSERT INTO rec_d VALUES ('a', 0, 13), ('b', 0, 17)",
        QueryContext::arc(),
    )
    .await;
    for (query, expected_values) in [
        (
            "(rec_a + on(host) rec_b) + on(host) (rec_c + on(host) rec_d)",
            vec![("a", 25.0), ("b", 37.0)],
        ),
        (
            "rec_a + on(host) (rec_b + on(host) (rec_c + on(host) rec_d))",
            vec![("a", 25.0), ("b", 37.0)],
        ),
        (
            "(rec_a + on(host) rec_b) / on(host) (rec_c + on(host) rec_d)",
            vec![("a", 0.25), ("b", 9.0 / 28.0)],
        ),
        (
            "rec_a + on(host) rec_a + on(host) rec_a + on(host) rec_a",
            vec![("a", 8.0), ("b", 16.0)],
        ),
    ] {
        let sql = format!("TQL EVAL (0, 0, '1s') {query}");
        observer.clear();
        let output = native_tql_output(&instance, &sql, true).await;
        let native = native_result_rows(output).await;
        let physical = observer.root_plan();
        assert_eq!(
            4,
            physical.output_partitioning().partition_count(),
            "{query}: {physical:?}"
        );
        let mut stack = vec![physical.clone()];
        let mut matching_sites = Vec::new();
        while let Some(node) = stack.pop() {
            if node.name() == "FusedSeriesExec"
                && node.children().len() == 2
                && node.children().iter().all(|child| {
                    child
                        .downcast_ref::<promql::extension_plan::SeriesRepartitionExec>()
                        .is_some_and(|router| {
                            matches!(
                                router.routing_policy(),
                                promql::extension_plan::SeriesRouting::Matching(_)
                            )
                        })
                })
            {
                matching_sites.push(node.clone());
            }
            assert!(
                !node.name().contains("HashJoin") && !node.name().contains("JoinExec"),
                "{query}: legacy join remains in native plan: {physical:?}"
            );
            stack.extend(node.children().into_iter().cloned());
        }
        assert_eq!(3, matching_sites.len(), "{query}: {physical:?}");
        for site in &matching_sites {
            assert_eq!(
                4,
                site.output_partitioning().partition_count(),
                "each native matching stage must run in four partitions: {site:?}"
            );
        }
        let legacy = native_result_rows(native_tql_output(&instance, &sql, false).await).await;
        assert_eq!(native, legacy, "{query}");
        let expected = expected_values
            .iter()
            .map(|(host, value)| (0, (*host).to_string(), *value))
            .collect::<Vec<_>>();
        assert_eq!(expected, native, "{query}: expected populated host rows");
    }

    execute_all(
        &instance,
        "CREATE TABLE rec_key_a (host STRING, job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host, job)); \
         CREATE TABLE rec_key_b (host STRING, job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host, job)); \
         CREATE TABLE rec_key_c (host STRING, job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host, job)); \
         CREATE TABLE rec_key_d (host STRING, job STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host, job)); \
         INSERT INTO rec_key_a VALUES ('a', 'ja', 0, 2), ('b', 'jb', 0, 4); \
         INSERT INTO rec_key_b VALUES ('a', 'ja', 0, 3), ('b', 'jb', 0, 5); \
         INSERT INTO rec_key_c VALUES ('a', 'ja', 0, 7), ('b', 'jb', 0, 11); \
         INSERT INTO rec_key_d VALUES ('a', 'ja', 0, 13), ('b', 'jb', 0, 17)",
        QueryContext::arc(),
    )
    .await;
    let query = "(rec_key_a + on(host, job) rec_key_b) + on(host) rec_key_c + on(host) rec_key_d";
    let sql = format!("TQL EVAL (0, 0, '1s') {query}");
    observer.clear();
    let (columns, native) =
        native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    let physical = observer.root_plan();
    assert_eq!(
        4,
        physical.output_partitioning().partition_count(),
        "{physical:?}"
    );
    let mut stack = vec![physical.clone()];
    let mut matching_sites = Vec::new();
    while let Some(node) = stack.pop() {
        if node.name() == "FusedSeriesExec"
            && node.children().len() == 2
            && node.children().iter().all(|child| {
                child
                    .downcast_ref::<promql::extension_plan::SeriesRepartitionExec>()
                    .is_some_and(|router| {
                        matches!(
                            router.routing_policy(),
                            promql::extension_plan::SeriesRouting::Matching(_)
                        )
                    })
            })
        {
            let policies = node
                .children()
                .iter()
                .map(|child| {
                    let router = child
                        .downcast_ref::<promql::extension_plan::SeriesRepartitionExec>()
                        .unwrap();
                    router.routing_policy().clone()
                })
                .collect::<Vec<_>>();
            matching_sites.push((node.clone(), policies));
        }
        assert!(
            !node.name().contains("HashJoin") && !node.name().contains("JoinExec"),
            "legacy join remains for different matching keys: {physical:?}"
        );
        stack.extend(node.children().into_iter().cloned());
    }
    assert_eq!(3, matching_sites.len(), "{physical:?}");
    let policy_on = |labels: &[&str]| {
        promql::extension_plan::SeriesRouting::Matching(promql::vector_match::MatchingLabels::On(
            labels.iter().map(|label| Arc::from(*label)).collect(),
        ))
    };
    // The plan walk is root-first: the outer host match, its middle host match, then the
    // innermost host/job match.
    let expected_policies = vec![
        vec![policy_on(&["host"]), policy_on(&["host"])],
        vec![policy_on(&["host"]), policy_on(&["host"])],
        vec![policy_on(&["host", "job"]), policy_on(&["host", "job"])],
    ];
    let actual_policies = matching_sites
        .iter()
        .map(|(_, policies)| policies.clone())
        .collect::<Vec<_>>();
    assert_eq!(expected_policies, actual_policies, "{physical:?}");
    for (site, _) in &matching_sites {
        assert_eq!(4, site.output_partitioning().partition_count(), "{site:?}");
    }

    let legacy = native_labelled_rows(native_tql_output(&instance, &sql, false).await)
        .await
        .1;
    assert_eq!(
        native, legacy,
        "different matching keys must preserve legacy output"
    );
    let expected = vec![
        (
            0,
            std::collections::BTreeMap::from([("host".to_string(), "a".to_string())]),
            25.0,
        ),
        (
            0,
            std::collections::BTreeMap::from([("host".to_string(), "b".to_string())]),
            37.0,
        ),
    ];
    assert_eq!(
        expected, native,
        "matching on host drops job from output labels"
    );
    // The value is named after the statement's expression, and the rows answer reports the
    // per-row name column among its labels.
    let mut expected_columns = vec![
        "__name__".to_string(),
        "host".to_string(),
        "ts".to_string(),
        expression_name(query),
    ];
    expected_columns.sort();
    assert_eq!(expected_columns, columns);
}

/// Segments are cut where the physical planner would otherwise have to gather a per-series prefix.
///
/// One statement per shape the cut can meet, each asserting only what the rule says: a tree that
/// already runs on every partition stays one node, a matching tree routes instead of gathering, a
/// global fold over per-series work cuts that work out, and data-free expressions stay in the
/// parent instead of becoming segments.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_cuts_segments_only_where_a_gather_would_swallow_them() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_segment_boundaries")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE seg_a (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         CREATE TABLE seg_b (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         INSERT INTO seg_a VALUES ('a', 0, 2), ('b', 0, 7), ('c', 0, 11), ('a', 1000, 3), ('b', 1000, 13), ('c', 1000, 5); \
         INSERT INTO seg_b VALUES ('a', 0, 4), ('b', 0, 1), ('c', 0, 22), ('a', 1000, 6), ('b', 1000, 26), ('c', 1000, 10)",
        QueryContext::arc(),
    )
    .await;

    fn fused_in(
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let mut found = Vec::new();
        let mut stack = vec![plan.clone()];
        while let Some(node) = stack.pop() {
            if node.name() == "FusedSeriesExec" {
                found.push(node.clone());
            }
            stack.extend(node.children().into_iter().cloned());
        }
        found
    }

    // `cuts` is how many per-series prefixes the statement has below a gathered node: the number
    // of segment boundaries, so the plan holds `cuts + 1` fused nodes.
    //
    // `timestamp` over a computed vector reports the evaluation time, and that has to survive the
    // boundary: the aggregate runs in the gathered segment and the projection above it reads the
    // grid, which is what Prometheus reports for a sample the aggregate created. These answers
    // carry labels, so they are compared whole rather than as bare values.
    for query in ["timestamp(sum(seg_a))", "sum(timestamp(abs(seg_a)))"] {
        let sql = format!("TQL EVAL (0, 1, '1s') {query}");
        let native = eval_text_nameless(native_tql_output(&instance, &sql, true).await);
        let legacy = eval_text(native_tql_output(&instance, &sql, false).await);
        assert_eq!(native, legacy, "{query}: answers differ");
    }

    for (query, cuts, why) in [
        // A tree that answers each series on its own needs no gather, so nothing is cut.
        (
            "abs(seg_a)",
            0usize,
            "a per-series tree runs on every partition",
        ),
        // A matching tree routes its inputs; it does not gather them.
        (
            "seg_a / on(host) seg_b",
            0usize,
            "a matching tree routes rather than gathers",
        ),
        // The fold gathers, but the prefixes below it do not need that gather.
        (
            "sum(topk(1, seg_a))",
            1usize,
            "the per-series prefix under a global topk",
        ),
        (
            "sum(topk(1, abs(seg_a)))",
            1usize,
            "the whole per-series prefix is one segment",
        ),
        // A boundary cannot carry a constant, so the constant stays inside the segment.
        (
            "sum(topk(1, seg_a)) + 1",
            1usize,
            "a constant is embedded, not cut out",
        ),
        (
            "sum(topk(1, seg_a)) + vector(1)",
            1usize,
            "the data-free vector stays in the parent while the data prefix is cut",
        ),
    ] {
        let sql = format!("TQL EVAL (0, 1, '1s') {query}");
        observer.clear();
        let native = eval_text_nameless(native_tql_output(&instance, &sql, true).await);
        let root = observer.root_plan();
        let frontend = fused_in(&root);
        let mut regions = Vec::new();
        for plan in observer.region_plans() {
            regions.extend(fused_in(&plan));
        }
        assert_eq!(
            frontend.len() + regions.len(),
            cuts + 1,
            "{query}: {why}: expected {} fused nodes, found {} at the frontend and {} in the region: {}",
            cuts + 1,
            frontend.len(),
            regions.len(),
            datafusion::physical_plan::displayable(root.as_ref()).indent(true)
        );
        if cuts > 0 {
            assert_eq!(
                frontend.len(),
                1,
                "{query}: {why}: the gathered node is the frontend's own: {}",
                datafusion::physical_plan::displayable(root.as_ref()).indent(true)
            );
        }
        if query == "sum(topk(1, seg_a)) + vector(1)" {
            let parent = datafusion::physical_plan::displayable(frontend[0].as_ref())
                .one_line()
                .to_string();
            let parent_tree = parent.split_once('\n').unwrap().1;
            assert!(parent_tree.contains("VectorOf"), "{parent_tree}");
            assert!(
                parent_tree.contains("Input: evaluated samples source=0"),
                "the parent must read the data segment: {parent_tree}"
            );
            assert!(!parent_tree.contains("Instant"), "{parent_tree}");
            assert_eq!(regions.len(), 1, "{query}: the data prefix must be cut");
            let prefix = datafusion::physical_plan::displayable(regions[0].as_ref())
                .one_line()
                .to_string();
            let prefix_tree = prefix.split_once('\n').unwrap().1;
            assert!(prefix_tree.contains("Instant"), "{prefix_tree}");
            assert!(!prefix_tree.contains("VectorOf"), "{prefix_tree}");
            let mut rows = eval_values(native_tql_output(&instance, &sql, true).await);
            rows.sort_by_key(|row| row.0);
            assert_eq!(rows, vec![(0, 12.0), (1_000, 14.0)], "{query}");
        }
        let legacy = eval_text(native_tql_output(&instance, &sql, false).await);
        assert_eq!(native, legacy, "{query}: answers differ");
    }

    // This two-table statement is segmented: each segment binds its own scan, so it can be placed
    // in a region. A cut carrying two sources is still declined in `build_inner_segment`, so each
    // prefix the parent receives reads exactly one source.
    let query = "sum(max by (host) (seg_a)) / sum(seg_b)";
    let sql = format!("TQL EVAL (0, 1, '1s') {query}");
    observer.clear();
    let native = eval_text_nameless(native_tql_output(&instance, &sql, true).await);
    let root = observer.root_plan();
    let frontend = fused_in(&root);
    let mut regions = 0;
    for plan in observer.region_plans() {
        regions += fused_in(&plan).len();
    }
    assert_eq!(
        frontend.len(),
        1,
        "{query}: the whole-vector parent is the frontend's own: {}",
        datafusion::physical_plan::displayable(root.as_ref()).indent(true)
    );
    assert!(
        regions >= 1,
        "{query}: each prefix segment runs in a region: {}",
        datafusion::physical_plan::displayable(root.as_ref()).indent(true)
    );
    let legacy = eval_text(native_tql_output(&instance, &sql, false).await);
    assert_eq!(native, legacy, "{query}: answers differ");
}

/// A whole-vector operator standing on an evaluated fold is the shape it is over a bare scan.
///
/// `topk`, `bottomk` and `sort` read the whole vector at a point, so the fold below one is not a
/// boundary the aggregate state can cross: the split declines it and the per-series prefix under the
/// fold is cut out as a segment instead, leaving the fold and the operator in the one node above the
/// merged groups. Both routes are run and their rows compared as the row sets they are - a partition
/// publishes its rows in its own order, so a row's position is not what the two routes can be held
/// to - which is the one difference `eval_text` normalizes besides the name column and the value
/// column's name. `limitk` and `limit_ratio` are left out of the comparison: they exist in this
/// route's own kernels and the existing planner answers neither.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_whole_vector_operators_over_an_aggregate_keep_their_route() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_ranking_over_aggregate")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE rank_agg (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         INSERT INTO rank_agg (host, ts, val) VALUES \
         ('a', 0, 2), ('b', 0, 7), ('c', 0, 11), ('a', 1000, 3), ('b', 1000, 13), ('c', 1000, 5)",
        QueryContext::arc(),
    )
    .await;

    fn fused_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
        let mut found = Vec::new();
        let mut stack = vec![plan.clone()];
        while let Some(node) = stack.pop() {
            if node.name() == "FusedSeriesExec" {
                found.push(node.clone());
            }
            stack.extend(node.children().into_iter().cloned());
        }
        found
    }

    for (query, reported) in [
        // The fold computes a value of its own and drops the name it read, so the ranking above it
        // reports the null name the fold left.
        ("topk(1, sum(rank_agg))", ReportedName::Nothing),
        ("bottomk(2, avg(rank_agg))", ReportedName::Nothing),
        ("sort(sum by (host) (rank_agg))", ReportedName::Nothing),
        // The same ranking over a bare scan: the shape that was never cut below.
        ("topk(1, rank_agg)", ReportedName::Metric("rank_agg")),
    ] {
        let sql = format!("TQL EVAL (0, 1000, '1000s') {query}");
        observer.clear();
        let native = eval_text_rows(native_tql_output(&instance, &sql, true).await, reported);
        let root = observer.root_plan();
        let fused = fused_nodes(&root);
        assert_eq!(
            1,
            fused.len(),
            "`{query}` answers above one cut prefix: {}",
            datafusion::physical_plan::displayable(root.as_ref()).indent(true)
        );
        let parent = datafusion::physical_plan::displayable(fused[0].as_ref())
            .one_line()
            .to_string();
        let parent_tree = parent
            .split_once('\n')
            .expect("the parent renders its tree")
            .1;
        assert!(
            parent_tree.contains("Input: evaluated samples source=0"),
            "`{query}` ranks the fold's own answer: {parent_tree}"
        );
        assert!(
            !parent_tree.contains("Instant"),
            "`{query}` reads the cut prefix, so no instant evaluation is left above it: {parent_tree}"
        );
        let legacy = eval_text(native_tql_output(&instance, &sql, false).await);
        assert_eq!(
            native, legacy,
            "`{query}`: the routes answer different rows"
        );
    }
}

/// Range-function prefixes on two table sources retain their own samples in each segment.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_cross_table_aggregate_offsets_keep_independent_scan_bounds() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_cross_table_offsets")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE off_a (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=mito; \
         CREATE TABLE off_b (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=mito; \
         INSERT INTO off_a VALUES ('a', 0, 10), ('a', 60000, 20), ('a', 120000, 30); \
         INSERT INTO off_b VALUES ('a', 0, 1), ('a', 60000, 2), ('a', 120000, 3)",
        QueryContext::arc(),
    )
    .await;

    // The two-region plan does not promise row order, so sort without deduplicating before comparing.
    let sorted_rows = |mut rows: Vec<(i64, f64)>| {
        rows.sort_unstable_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.total_cmp(&right.1))
        });
        rows
    };
    let offset_query = "sum(off_a) / sum(off_b offset 60s)";
    let offset_sql = format!("TQL EVAL (60, 120, '1m') {offset_query}");
    observer.clear();
    let legacy_output = native_tql_output(&instance, &offset_sql, false).await;
    let legacy_rows = sorted_rows(eval_values(legacy_output));
    let legacy_root = observer.root_plan();
    let mut stack = vec![legacy_root.clone()];
    while let Some(node) = stack.pop() {
        assert_ne!(node.name(), "FusedSeriesExec", "legacy plan must not fuse");

        stack.extend(node.children().into_iter().cloned());
    }
    assert_eq!(legacy_rows, vec![(60_000, 20.0), (120_000, 15.0)]);

    let unoffset_sql = "TQL EVAL (60, 120, '1m') sum(off_a) / sum(off_b)";
    let unoffset_legacy = sorted_rows(eval_values(
        native_tql_output(&instance, unoffset_sql, false).await,
    ));
    assert_eq!(unoffset_legacy, vec![(60_000, 10.0), (120_000, 10.0)]);
    assert_ne!(
        legacy_rows, unoffset_legacy,
        "the offset must change the answer; otherwise this case does not prove the two segments \
         carry distinct scan bounds"
    );

    observer.clear();
    let offset_native = sorted_rows(eval_values(
        native_tql_output(&instance, &offset_sql, true).await,
    ));
    assert_eq!(
        offset_native, legacy_rows,
        "native offset result differs from legacy"
    );
    observer.clear();
    let unoffset_native = sorted_rows(eval_values(
        native_tql_output(&instance, unoffset_sql, true).await,
    ));
    assert_eq!(
        unoffset_native, unoffset_legacy,
        "native unoffset result differs from legacy"
    );
    assert_ne!(
        offset_native, unoffset_native,
        "native offset must change the answer; otherwise the segments do not carry distinct scan bounds"
    );

    observer.clear();
    let _ = native_tql_output(&instance, &offset_sql, true).await;
    let root = observer.root_plan();
    fn fused_in(
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let mut found = Vec::new();
        let mut stack = vec![plan.clone()];
        while let Some(node) = stack.pop() {
            if node.name() == "FusedSeriesExec" {
                found.push(node.clone());
            }
            stack.extend(node.children().into_iter().cloned());
        }
        found
    }
    let root_display = datafusion::physical_plan::displayable(root.as_ref())
        .indent(true)
        .to_string();
    let parent_nodes = fused_in(&root);
    assert_eq!(
        parent_nodes.len(),
        1,
        "frontend has one fused parent:\n{root_display}"
    );
    let children = parent_nodes[0].children();
    assert_eq!(children.len(), 2, "parent has two scans:\n{root_display}");
    let merge_scans = children
        .iter()
        .enumerate()
        .map(|(index, child)| {
            let scan = if child.is::<CooperativeExec>() {
                let wrapped = child.children();
                assert_eq!(
                    wrapped.len(),
                    1,
                    "child {index} cooperative wrapper:\\n{root_display}"
                );
                wrapped[0]
            } else {
                child
            };
            scan.downcast_ref::<MergeScanExec>()
                .unwrap_or_else(|| panic!("child {index} is not a MergeScanExec: {root_display}"))
        })
        .collect::<Vec<_>>();
    let scan_debug = merge_scans
        .iter()
        .map(|scan| format!("{scan:?}"))
        .collect::<Vec<_>>();
    assert!(scan_debug.iter().any(|debug| debug.contains("off_a")));
    assert!(scan_debug.iter().any(|debug| debug.contains("off_b")));
    assert!(scan_debug.iter().all(|debug| debug.contains("FusedSeries")));
    assert_ne!(
        scan_debug[0], scan_debug[1],
        "segments must bind distinct scans"
    );
    // MergeScan Debug contains its table and remote segment plan; the offset/unoffset answer
    // assertion above independently proves that the two time bounds affect results.
    // Per-partition segment output evidence is asserted by
    // `native_aggregate_test.rs::check_two_source_segregation`. Here, MergeScanExec keeps its
    // remote segment plan privately and the observer cannot reach that region-side physical plan.
}

/// A range function on each side of a cross-table division remains a cut candidate.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_cross_table_range_aggregate_segments_keep_windows() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_cross_table_range_segments")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE rng_a (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=mito; \
         CREATE TABLE rng_b (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=mito; \
         INSERT INTO rng_a VALUES ('a', 0, 0), ('a', 60000, 60), ('a', 120000, 120); \
         INSERT INTO rng_b VALUES ('a', 0, 0), ('a', 60000, 30), ('a', 120000, 90)",
        QueryContext::arc(),
    )
    .await;

    // At t=60 the 2m increases are 60/30 = 2. At t=120 they are 120/90 = 4/3,
    // so a window bound accidentally shared across sources changes the stepwise ratio.
    let query = "sum(increase(rng_a[2m])) / sum(increase(rng_b[2m]))";
    let sql = format!("TQL EVAL (60, 120, '1m') {query}");
    let sorted_rows = |mut rows: Vec<(i64, f64)>| {
        rows.sort_unstable_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.total_cmp(&right.1))
        });
        rows
    };

    observer.clear();
    let legacy_rows = sorted_rows(eval_values(native_tql_output(&instance, &sql, false).await));
    let legacy_root = observer.root_plan();
    let mut stack = vec![legacy_root];
    while let Some(node) = stack.pop() {
        assert_ne!(node.name(), "FusedSeriesExec", "legacy plan must not fuse");

        stack.extend(node.children().into_iter().cloned());
    }
    assert_eq!(legacy_rows, vec![(60_000, 2.0), (120_000, 4.0 / 3.0)]);
    assert_ne!(
        legacy_rows[0].1, legacy_rows[1].1,
        "different step values are needed to expose a wrong window boundary"
    );

    observer.clear();
    let native_rows = sorted_rows(eval_values(native_tql_output(&instance, &sql, true).await));
    assert_eq!(
        native_rows, legacy_rows,
        "native range result differs from legacy"
    );

    observer.clear();
    let _ = native_tql_output(&instance, &sql, true).await;
    let root = observer.root_plan();
    let root_display = datafusion::physical_plan::displayable(root.as_ref())
        .indent(true)
        .to_string();
    let mut fused = Vec::new();
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        if node.name() == "FusedSeriesExec" {
            fused.push(node.clone());
        }
        stack.extend(node.children().into_iter().cloned());
    }
    assert_eq!(fused.len(), 1, "one frontend parent:\n{root_display}");
    let children = fused[0].children();
    assert_eq!(children.len(), 2, "two source segments:\n{root_display}");
    let scans = children
        .iter()
        .enumerate()
        .map(|(index, child)| {
            let scan = if child.is::<CooperativeExec>() {
                let wrapped = child.children();
                assert_eq!(wrapped.len(), 1, "child {index} wrapper:\\n{root_display}");
                wrapped[0]
            } else {
                child
            };
            scan.downcast_ref::<MergeScanExec>()
                .unwrap_or_else(|| panic!("child {index} is not MergeScanExec:\\n{root_display}"))
        })
        .collect::<Vec<_>>();
    let scan_debug = scans
        .iter()
        .map(|scan| format!("{scan:?}"))
        .collect::<Vec<_>>();
    assert!(scan_debug.iter().any(|debug| debug.contains("rng_a")));
    assert!(scan_debug.iter().any(|debug| debug.contains("rng_b")));
    assert!(scan_debug.iter().all(|debug| debug.contains("FusedSeries")));
    // Per-partition segment evidence is covered by
    // `native_aggregate_test.rs::check_two_source_segregation`; this harness cannot access the
    // private region-side physical segment held by each MergeScanExec.
}

/// Sets up a shared physical metric table and the query-specific logical metric tables and rows.
/// Explicit INSERT columns are essential because the physical column order differs from the
/// logical tables; SQL keywords such as `cluster` must be quoted; and timestamps at zero are
/// excluded by the scan lower bound and PromQL staleness handling.
async fn setup_real_query_metric_fixture(instance: &Arc<Instance>, fixture_sql: &str) {
    execute_all(instance, fixture_sql, QueryContext::arc()).await;
}

fn fused_nodes_in_root(
    root: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    let mut found = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(node) = stack.pop() {
        if node.name() == "FusedSeriesExec" {
            found.push(node.clone());
        }
        stack.extend(node.children().into_iter().cloned());
    }
    found
}

fn debug_merge_scan_child(child: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> String {
    // A matching segment re-declares each source at the configured width, so a one-partition
    // region scan arrives under a router. Look through both the cooperative wrapper and that
    // router to reach the scan the source is actually read from.
    let mut child = child;
    while child.is::<CooperativeExec>()
        || child.is::<promql::extension_plan::SeriesRepartitionExec>()
    {
        let wrapped = child.children();
        assert_eq!(wrapped.len(), 1, "a wrapper here must have one child");
        child = wrapped[0];
    }
    let scan = child
        .downcast_ref::<MergeScanExec>()
        .expect("frontend child must be a MergeScanExec");
    format!("{scan:?}")
}

/// The four disjoint time windows the metric fixtures write, one per flushed range.
///
/// A series is kept inside one window, so the samples of one series never span two windows. Each
/// window goes in and is flushed on its own, which is how the fixture offers a scan several range
/// streams to read; this is the arrangement under which the earlier single-range observation read
/// one of the four scan partitions and the four range streams read all four. The windows sit inside
/// the narrowest corpus window, the one-minute range starting at `241`, so every query reads all
/// four: a window outside a query's time range is pruned, and a pruned window cannot contribute a
/// row to any scan partition. What each scan actually did is then read from the per-partition
/// `OutputRows` it recorded, not inferred from the window or range it was given.
const CORPUS_WINDOWS: [(i64, i64); 4] = [
    (242_000, 244_000),
    (250_000, 252_000),
    (258_000, 260_000),
    (266_000, 268_000),
];

/// Reports native planning and labelled answers for the real Q07 aggregate pair.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_real_query_q07_throttled_periods() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) =
        native_test_instance(GreptimeDbStandaloneBuilder::new("native_promql_real_q07")).await;
    let instance = standalone.fe_instance().clone();
    // The query fixes `pod` to `pod-0`, so the containers of that pod are the only series it can
    // read. One container per window, each with its own flush, spreads them over the scan's
    // partitions; each container keeps the first window's shape, so every container's ratio is the
    // same exact half.
    let mut fixture = String::from(
        r#"CREATE TABLE real_q_phy (ts TIMESTAMP TIME INDEX, val DOUBLE)
            ENGINE=metric WITH ("physical_metric_table" = "true");
        ALTER TABLE real_q_phy SET 'compaction.twcs.trigger_file_num' = '100';
        ALTER TABLE real_q_phy SET 'compaction.twcs.inactive_window.trigger_file_num' = '100';
        CREATE TABLE container_cpu_cfs_throttled_periods_total (
            "cluster" STRING, job STRING, metrics_path STRING, namespace STRING,
            pod STRING, container STRING, ts TIMESTAMP TIME INDEX, val DOUBLE,
            PRIMARY KEY(cluster, job, metrics_path, namespace, pod, container)
        ) ENGINE=metric WITH ("on_physical_table" = 'real_q_phy');
        CREATE TABLE container_cpu_cfs_periods_total (
            "cluster" STRING, job STRING, metrics_path STRING, namespace STRING,
            pod STRING, container STRING, ts TIMESTAMP TIME INDEX, val DOUBLE,
            PRIMARY KEY(cluster, job, metrics_path, namespace, pod, container)
        ) ENGINE=metric WITH ("on_physical_table" = 'real_q_phy');"#,
    );
    for (window, &(first_ts, second_ts)) in CORPUS_WINDOWS.iter().enumerate() {
        let container = ["main", "sidecar", "envoy", "metrics"][window];
        let weight = (window + 1) * 30;
        for (table, second_value) in [
            ("container_cpu_cfs_throttled_periods_total", 2 * weight),
            ("container_cpu_cfs_periods_total", 4 * weight),
        ] {
            fixture.push_str(&format!(
                " INSERT INTO {table} (\"cluster\", job, metrics_path, namespace, pod, container, ts, val) VALUES \
                 ('eks-us-east-1-qa1','kubelet','/metrics/cadvisor','app-0','pod-0','{container}',{first_ts},0), \
                 ('eks-us-east-1-qa1','kubelet','/metrics/cadvisor','app-0','pod-0','{container}',{second_ts},{second_value});"
            ));
        }
        fixture.push_str(" ADMIN FLUSH_TABLE('real_q_phy');");
    }
    setup_real_query_metric_fixture(&instance, &fixture).await;
    let query = r#"sum(increase(container_cpu_cfs_throttled_periods_total{job="kubelet",metrics_path="/metrics/cadvisor",namespace="app-0",pod="pod-0",container!="",cluster="eks-us-east-1-qa1"}[5m])) by (container) / sum(increase(container_cpu_cfs_periods_total{job="kubelet",metrics_path="/metrics/cadvisor",namespace="app-0",pod="pod-0",container!="",cluster="eks-us-east-1-qa1"}[5m])) by (container)"#;
    let sql = format!("TQL EVAL (301, 301, '1m') {query}");
    observer.clear();
    let (legacy_columns, legacy_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
    assert!(!legacy_rows.is_empty(), "Q07 legacy rows must be nonempty");
    observer.clear();
    let (native_columns, native_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    let root = observer.root_plan();
    let root_display = datafusion::physical_plan::displayable(root.as_ref())
        .indent(true)
        .to_string();
    let native = observer.fused().is_some();
    assert!(
        native,
        "{query}: the native route must answer this statement; it declined: {root_display}"
    );
    let frontend_fused = fused_nodes_in_root(&root);
    let child_debug = frontend_fused[0]
        .children()
        .iter()
        .map(|child| debug_merge_scan_child(child))
        .collect::<Vec<_>>();
    common_telemetry::info!(
        "Q07 root name={} fused_frontend={} child merge scans={child_debug:?} value columns legacy={legacy_columns:?} native={native_columns:?}, legacy_rows={legacy_rows:?} native_rows={native_rows:?}, root:\n{root_display}",
        root.name(),
        frontend_fused.len()
    );
    // The work must be partition-wide on both sides of the boundary: every scan read rows on all
    // four of its partitions, and every region-side fused prefix published values on all four.
    let scan_report = assert_series_scan_partitions("Q07", &observer);
    let partitions_report = assert_cross_partition_native_values("Q07", &observer);
    common_telemetry::info!(
        "Q07 series scan partitions: {scan_report} native partition vectors: {partitions_report}"
    );
    assert_eq!(native_rows, legacy_rows, "Q07 labelled rows");
    assert_eq!(root.name(), "FusedSeriesExec");
    assert_eq!(frontend_fused.len(), 1);
    let children = frontend_fused[0].children();
    assert_eq!(children.len(), 2);
    assert!(
        child_debug
            .iter()
            .any(|debug| debug.contains("container_cpu_cfs_throttled_periods_total"))
    );
    assert!(
        child_debug
            .iter()
            .any(|debug| debug.contains("container_cpu_cfs_periods_total"))
    );
    assert!(
        child_debug
            .iter()
            .all(|debug| debug.contains("FusedSeries"))
    );
    // The two routes name the computed value column differently - the existing planner after its
    // own rewrite, the native route after the expression - and the labelled rows compared above
    // are what both report.
    assert!(
        legacy_columns
            .iter()
            .any(|column| column.contains("prom_increase"))
    );
    assert!(
        native_columns.contains(&expression_name(query)),
        "Q07 native value column: {native_columns:?}"
    );
}

/// Reports native planning and labelled answers for corpus Q04 verbatim.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_real_query_q04_namespace_cpu_requests() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) =
        native_test_instance(GreptimeDbStandaloneBuilder::new("native_promql_real_q04")).await;
    let instance = standalone.fe_instance().clone();
    // The right side has only `cluster` and `namespace`, so its namespaces are the series that can
    // be spread over the windows: one namespace per window puts the requests of each namespace in a
    // range of its own, while every namespace still holds a sample at both evaluation points. The
    // usage series follow the same windows, so a namespace's two sides meet on one partition and
    // the division keeps answering what the numbers say.
    let mut fixture = String::from(
        r#"CREATE TABLE real_q04_phy (ts TIMESTAMP TIME INDEX, val DOUBLE)
            ENGINE=metric WITH ("physical_metric_table" = "true");
        ALTER TABLE real_q04_phy SET 'compaction.twcs.trigger_file_num' = '100';
        ALTER TABLE real_q04_phy SET 'compaction.twcs.inactive_window.trigger_file_num' = '100';
        CREATE TABLE "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m" (
            "cluster" STRING, namespace STRING, pod STRING, container STRING,
            ts TIMESTAMP TIME INDEX, val DOUBLE,
            PRIMARY KEY(cluster, namespace, pod, container)
        ) ENGINE=metric WITH ("on_physical_table" = 'real_q04_phy');
        CREATE TABLE "namespace_cpu:kube_pod_container_resource_requests:sum" (
            "cluster" STRING, namespace STRING, ts TIMESTAMP TIME INDEX, val DOUBLE,
            PRIMARY KEY(cluster, namespace)
        ) ENGINE=metric WITH ("on_physical_table" = 'real_q04_phy');"#,
    );
    for (window, &(first_ts, second_ts)) in CORPUS_WINDOWS.iter().enumerate() {
        let namespace = format!("app-{window}");
        let weight = window + 1;
        fixture.push_str(&format!(
            " INSERT INTO \"node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m\" \
             (\"cluster\", namespace, pod, container, ts, val) VALUES \
             ('eks-us-east-1-qa1','{namespace}','pod-{window}','main',{first_ts},{weight}), \
             ('eks-us-east-1-qa1','{namespace}','pod-{window}','main',{second_ts},{});",
            3 * weight,
        ));
        fixture.push_str(&format!(
            " INSERT INTO \"namespace_cpu:kube_pod_container_resource_requests:sum\" \
             (\"cluster\", namespace, ts, val) VALUES \
             ('eks-us-east-1-qa1','{namespace}',{first_ts},{}), \
             ('eks-us-east-1-qa1','{namespace}',{second_ts},{});",
            2 * weight,
            4 * weight,
        ));
        fixture.push_str(" ADMIN FLUSH_TABLE('real_q04_phy');");
    }
    setup_real_query_metric_fixture(&instance, &fixture).await;
    let query = r#"sum(max by (cluster, namespace, pod, container) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m{cluster="eks-us-east-1-qa1"})) by (namespace) / sum(namespace_cpu:kube_pod_container_resource_requests:sum{cluster="eks-us-east-1-qa1"}) by (namespace)"#;
    let sql = format!("TQL EVAL (301, 301, '1m') {query}");
    observer.clear();
    let (legacy_columns, legacy_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
    assert!(!legacy_rows.is_empty(), "Q04 legacy rows must be nonempty");
    observer.clear();
    let (native_columns, native_rows) =
        native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    let root = observer.root_plan();
    let root_display = datafusion::physical_plan::displayable(root.as_ref())
        .indent(true)
        .to_string();
    let native = observer.fused().is_some();
    assert!(
        native,
        "{query}: the native route must answer this statement; it declined: {root_display}"
    );
    let frontend_fused = fused_nodes_in_root(&root);
    let child_debug = frontend_fused[0]
        .children()
        .iter()
        .map(|child| debug_merge_scan_child(child))
        .collect::<Vec<_>>();
    common_telemetry::info!(
        "Q04 root name={} fused_frontend={} child merge scans={child_debug:?} value columns legacy={legacy_columns:?} native={native_columns:?}, legacy_rows={legacy_rows:?} native_rows={native_rows:?}, root:\n{root_display}",
        root.name(),
        frontend_fused.len()
    );
    // The work must be partition-wide on both sides of the boundary: every scan read rows on all
    // four of its partitions, and every region-side fused prefix published values on all four.
    let scan_report = assert_series_scan_partitions("Q04", &observer);
    let partitions_report = assert_cross_partition_native_values("Q04", &observer);
    common_telemetry::info!(
        "Q04 series scan partitions: {scan_report} native partition vectors: {partitions_report}"
    );
    assert_eq!(native_rows, legacy_rows, "Q04 labelled rows");
    assert_eq!(root.name(), "FusedSeriesExec");
    assert_eq!(frontend_fused.len(), 1);
    let children = frontend_fused[0].children();
    assert_eq!(children.len(), 2);
    assert!(child_debug.iter().any(|debug| {
        debug.contains("node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m")
    }));
    assert!(
        child_debug
            .iter()
            .any(|debug| debug.contains("namespace_cpu:kube_pod_container_resource_requests:sum"))
    );
    assert!(
        child_debug
            .iter()
            .all(|debug| debug.contains("FusedSeries"))
    );
    // The two routes name the computed value column differently - the existing planner after its
    // own rewrite, the native route after the expression - and the labelled rows compared above
    // are what both report.
    assert!(legacy_columns.iter().any(|column| column.contains("max(")));
    assert!(
        native_columns.contains(&expression_name(query)),
        "Q04 native value column: {native_columns:?}"
    );
}

/// A multi-field `scalar()` input declines the native route and keeps the existing plan.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_scalar_multi_field_falls_back() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_scalar_multi_field_fallback",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_scalar_multi (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, val2 DOUBLE, PRIMARY KEY(host)); \
         INSERT INTO native_scalar_multi (host, ts, val, val2) VALUES ('a', 0, 1, 10)",
        QueryContext::arc(),
    )
    .await;
    let sql = "TQL EVAL (0, 0, '1s') scalar(native_scalar_multi)";
    let error = instance
        .do_query(sql, native_query_context(true, 4))
        .await
        .remove(0)
        .unwrap_err();
    assert!(
        format!("{error:?}").contains("Multi fields calculation is not supported in scalar"),
        "unexpected multi-field scalar error: {error:?}"
    );
    assert!(
        observer.fused().is_none(),
        "multi-field scalar() must fall back"
    );
}

/// A dynamic classic histogram quantile matches each equivalent constant-phi evaluation.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_dynamic_histogram_quantile() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_dynamic_histogram_quantile")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(&instance, "CREATE TABLE native_dyn_hist (ts TIMESTAMP TIME INDEX, le STRING, v DOUBLE, PRIMARY KEY(le)); INSERT INTO native_dyn_hist (ts, le, v) VALUES (-60000, '1', 0), (0, '1', 5), (60000, '1', 10), (-60000, '2', 0), (0, '2', 9), (60000, '2', 18), (-60000, '+Inf', 0), (0, '+Inf', 10), (60000, '+Inf', 20)", QueryContext::arc()).await;
    let query = "sum by(le)(rate(native_dyn_hist[5m]))";
    let sql = format!("TQL EVAL (0, 60, '60s') histogram_quantile(time() / 150 + 0.5, {query})");
    observer.clear();
    let native = native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    assert!(
        observer.fused().is_some(),
        "dynamic histogram query did not run natively"
    );
    // The legacy planner requires literal histogram parameters, so compare each point with its constant-phi equivalent.
    let mut expected = Vec::new();
    for (t, phi) in [(0, 0.5), (60, 0.9)] {
        let literal = format!("TQL EVAL ({t}, {t}, '60s') histogram_quantile({phi}, {query})");
        let (_, mut rows) =
            native_labelled_rows(native_tql_output(&instance, &literal, false).await).await;
        expected.append(&mut rows);
    }
    let mut actual = native.1;
    actual.sort_by_key(|row| row.0);
    expected.sort_by_key(|row| row.0);
    assert_eq!(actual, expected);

    // A literal lower bound and a dynamic upper bound exercise both parameter paths together.
    let fraction_sql =
        format!("TQL EVAL (0, 60, '60s') histogram_fraction(0, time() / 150 + 0.5, {query})");
    observer.clear();
    let fraction_native =
        native_labelled_rows(native_tql_output(&instance, &fraction_sql, true).await).await;
    assert!(
        observer.fused().is_some(),
        "dynamic histogram fraction did not run natively: {fraction_sql}"
    );
    let mut fraction_expected = Vec::new();
    for (t, upper) in [(0, 0.5), (60, 0.9)] {
        let literal = format!("TQL EVAL ({t}, {t}, '60s') histogram_fraction(0, {upper}, {query})");
        let (_, mut rows) =
            native_labelled_rows(native_tql_output(&instance, &literal, false).await).await;
        fraction_expected.append(&mut rows);
    }
    let mut fraction_actual = fraction_native.1;
    fraction_actual.sort_by_key(|row| row.0);
    fraction_expected.sort_by_key(|row| row.0);
    assert_eq!(fraction_actual, fraction_expected, "{fraction_sql}");

    let wrapped_query =
        "abs(histogram_quantile(time() / 150 + 0.5, sum by(le)(rate(native_dyn_hist[5m]))))";
    let wrapped_sql = format!("TQL EVAL (0, 60, '60s') {wrapped_query}");
    observer.clear();
    let wrapped_native =
        native_labelled_rows(native_tql_output(&instance, &wrapped_sql, true).await).await;
    assert!(
        observer.fused().is_some(),
        "wrapped dynamic histogram query did not run natively: {wrapped_sql}"
    );
    let mut wrapped_expected = Vec::new();
    for (t, phi) in [(0, 0.5), (60, 0.9)] {
        let literal = format!("TQL EVAL ({t}, {t}, '60s') abs(histogram_quantile({phi}, {query}))");
        let (_, mut rows) =
            native_labelled_rows(native_tql_output(&instance, &literal, false).await).await;
        wrapped_expected.append(&mut rows);
    }
    let mut wrapped_actual = wrapped_native.1;
    wrapped_actual.sort_by_key(|row| row.0);
    wrapped_expected.sort_by_key(|row| row.0);
    assert_eq!(wrapped_actual, wrapped_expected, "{wrapped_sql}");
}

/// A `quantile` whose parameter is evaluated reads it at every step and answers there what
/// `quantile(<that step's φ>, m)` answers on its own.
///
/// The vector holds two series, one at 1 and one at 10 at every step, so the `φ`-quantile of a step
/// is `1 + φ * 9` -- the linear interpolation Prometheus computes -- and a step whose `φ` the
/// statement has no sample for is a NaN `φ`, which Prometheus' `quantile` answers with NaN rather
/// than by dropping the point. The parameter is read from a second table, so the statement runs as
/// one native execution over two inputs.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_dynamic_quantile() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_dynamic_quantile").with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_dyn_quantile (ts TIMESTAMP TIME INDEX, idc STRING, val DOUBLE, PRIMARY KEY(idc));
         INSERT INTO native_dyn_quantile (ts, idc, val) VALUES
         (-600000, 'a', 1), (0, 'a', 1), (600000, 'a', 1), (1200000, 'a', 1),
         (-600000, 'b', 10), (0, 'b', 10), (600000, 'b', 10), (1200000, 'b', 10);
         CREATE TABLE native_dyn_phi (ts TIMESTAMP TIME INDEX, val DOUBLE);
         INSERT INTO native_dyn_phi (ts, val) VALUES (0, 0.25), (600000, 0.75)",
        QueryContext::arc(),
    )
    .await;

    // `scalar(native_dyn_phi)` is 0.25 at 0s, 0.75 at 600s, and NaN at 1200s: the 5m lookback of an
    // instant selection reaches no sample there, and `scalar()` answers NaN for a point that does
    // not hold exactly one.
    let phi = "scalar(native_dyn_phi)";
    let dynamic = format!("TQL EVAL (0, 1200, '600s') quantile({phi}, native_dyn_quantile)");
    observer.clear();
    let native = native_labelled_rows(native_tql_output(&instance, &dynamic, true).await).await;
    assert!(
        observer.fused().is_some(),
        "the per-point quantile did not run natively: {dynamic}"
    );
    let values: Vec<(i64, f64)> = native
        .1
        .iter()
        .map(|(ts, _, value)| (*ts, *value))
        .collect();
    assert_eq!(values.len(), 3, "{dynamic}: {values:?}");
    // `quantile(0.25, {1, 10})` and `quantile(0.75, {1, 10})`, and then the NaN step: a NaN `φ`
    // answers a NaN sample, which is a point of the result rather than a missing one.
    assert_eq!((values[0].0, values[0].1), (0, 3.25), "{dynamic}");
    assert_eq!((values[1].0, values[1].1), (600_000, 7.75), "{dynamic}");
    assert_eq!(values[2].0, 1_200_000, "{dynamic}");
    assert!(values[2].1.is_nan(), "{dynamic}: {:?}", values[2]);
    assert!(
        native.1.iter().all(|(_, labels, _)| labels.is_empty()),
        "the ungrouped fold reported labels: {:?}",
        native.1
    );

    // The same statement with each step's `φ` written as a number: the per-point answer is the
    // constant one at every step that has one.
    for (step, quantile, expected) in [(0, 0.25, 3.25), (600, 0.75, 7.75)] {
        let literal =
            format!("TQL EVAL ({step}, {step}, '600s') quantile({quantile}, native_dyn_quantile)");
        let (_, rows) =
            native_labelled_rows(native_tql_output(&instance, &literal, false).await).await;
        assert_eq!(
            rows.iter()
                .map(|(ts, _, value)| (*ts, *value))
                .collect::<Vec<_>>(),
            vec![(step * 1_000, expected)],
            "{literal}"
        );
    }

    // A parameter that varies at every point, read from the evaluation time: `0`, `0.25`, `0.5`.
    let time_sql = "TQL EVAL (0, 1200, '600s') quantile(time() / 2400, native_dyn_quantile)";
    observer.clear();
    let by_time = native_labelled_rows(native_tql_output(&instance, time_sql, true).await).await;
    assert!(
        observer.fused().is_some(),
        "a `time()` parameter did not run natively: {time_sql}"
    );
    assert_eq!(
        by_time
            .1
            .iter()
            .map(|(ts, _, value)| (*ts, *value))
            .collect::<Vec<_>>(),
        vec![(0, 1.0), (600_000, 3.25), (1_200_000, 5.5)],
        "{time_sql}"
    );

    // The grouping is untouched: each group is one member, so it answers that member's value at
    // every step with a `φ`, and NaN at the step without one.
    let grouped =
        format!("TQL EVAL (0, 1200, '600s') quantile({phi}, native_dyn_quantile) by (idc)");
    observer.clear();
    let by_idc = native_labelled_rows(native_tql_output(&instance, &grouped, true).await).await;
    assert!(
        observer.fused().is_some(),
        "a grouped per-point quantile did not run natively: {grouped}"
    );
    let mut grouped_values: Vec<(i64, String, f64)> = by_idc
        .1
        .iter()
        .map(|(ts, labels, value)| (*ts, labels["idc"].clone(), *value))
        .collect();
    grouped_values.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    let mut expected_grouped = [
        (0, "a".to_string(), 1.0),
        (600_000, "a".to_string(), 1.0),
        (0, "b".to_string(), 10.0),
        (600_000, "b".to_string(), 10.0),
    ];
    expected_grouped.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    // Two groups at each of the three steps: the two steps with a `φ`, and the NaN step both
    // groups answer too. The row count is asserted before the rows are read, so a group that
    // answered nothing at the NaN step is a failure rather than a shorter list that happens to
    // hold no wrong row.
    assert_eq!(
        6,
        grouped_values.len(),
        "the grouped fold did not answer every group at every step: {grouped_values:?}"
    );
    assert_eq!(grouped_values[..4], expected_grouped[..], "{grouped}");
    assert_eq!(
        vec![(1_200_000, "a"), (1_200_000, "b")],
        grouped_values[4..]
            .iter()
            .map(|(ts, group, _)| (*ts, group.as_str()))
            .collect::<Vec<_>>(),
        "the grouped NaN step answered something else: {grouped_values:?}"
    );
    assert!(
        grouped_values[4..]
            .iter()
            .all(|(_, _, value)| value.is_nan()),
        "the grouped NaN step answered a value rather than NaN: {grouped_values:?}"
    );

    // A vector argument is not a scalar, which Prometheus refuses at type check: the native route
    // keeps declining it, so the statement is answered by the existing plan and fails there.
    let vector_parameter =
        "TQL EVAL (0, 1200, '600s') quantile(native_dyn_quantile, native_dyn_quantile)";
    observer.clear();
    let error = instance
        .do_query(vector_parameter, native_query_context(true, 4))
        .await
        .remove(0)
        .unwrap_err();
    assert!(
        observer.fused().is_none(),
        "a vector parameter was admitted: {vector_parameter}"
    );
    assert!(
        format!("{error:?}").contains("quantile"),
        "unexpected vector-parameter error: {error:?}"
    );

    // A constant written as an expression stays the number it folds to, and answers it natively.
    let folded = "TQL EVAL (0, 1200, '600s') quantile(0.25 + 0.1 + 0.15, native_dyn_quantile)";
    let literal = "TQL EVAL (0, 1200, '600s') quantile(0.5, native_dyn_quantile)";
    observer.clear();
    let folded_native =
        native_labelled_rows(native_tql_output(&instance, folded, true).await).await;
    assert!(
        observer.fused().is_some(),
        "the folded constant did not run natively: {folded}"
    );
    observer.clear();
    let literal_native =
        native_labelled_rows(native_tql_output(&instance, literal, true).await).await;
    assert!(
        observer.fused().is_some(),
        "the literal did not run natively: {literal}"
    );
    assert_eq!(folded_native.1, literal_native.1, "{folded} != {literal}");
    assert_eq!(
        folded_native
            .1
            .iter()
            .map(|(ts, _, value)| (*ts, *value))
            .collect::<Vec<_>>(),
        vec![(0, 5.5), (600_000, 5.5), (1_200_000, 5.5)],
        "{folded}"
    );
}

/// A dynamic `topk` parameter is evaluated at each TQL evaluation timestamp. `time() / 60 + 1`
/// truncates to k = 1, 2, 3 at 0s, 60s, and 120s respectively.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_dynamic_topk_matches_legacy() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_dynamic_topk_e2e").with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_dyn_topk (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host));
         INSERT INTO native_dyn_topk (host, ts, val) VALUES
         ('h0', -60000, 0), ('h0', 0, 1), ('h0', 60000, 2), ('h0', 120000, 3),
         ('h1', -60000, 0), ('h1', 0, 2), ('h1', 60000, 4), ('h1', 120000, 6),
         ('h2', -60000, 0), ('h2', 0, 3), ('h2', 60000, 6), ('h2', 120000, 9),
         ('h3', -60000, 0), ('h3', 0, 4), ('h3', 60000, 8), ('h3', 120000, 12),
         ('h4', -60000, 0), ('h4', 0, 5), ('h4', 60000, 10), ('h4', 120000, 15),
         ('h5', -60000, 0), ('h5', 0, 6), ('h5', 60000, 12), ('h5', 120000, 18),
         ('h6', -60000, 0), ('h6', 0, 7), ('h6', 60000, 14), ('h6', 120000, 21),
         ('h7', -60000, 0), ('h7', 0, 8), ('h7', 60000, 16), ('h7', 120000, 24)"#,
        QueryContext::arc(),
    )
    .await;
    let query_body = "sum by(host)(rate(native_dyn_topk[5m]))";
    let sql = format!("TQL EVAL (0, 120, '60s') topk(time() / 60 + 1, {query_body})");
    observer.clear();
    let native = native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
    assert!(
        observer.fused().is_some(),
        "dynamic topk did not run natively"
    );
    let counts = [0, 60_000, 120_000].map(|timestamp| {
        native
            .1
            .iter()
            .filter(|(ts, _, _)| *ts == timestamp)
            .count()
    });
    assert_eq!(counts, [1, 2, 3], "k = 1, 2, 3 at 0s, 60s, 120s");
    // The legacy planner requires a literal topk parameter, so compare each timestamp against
    // its equivalent literal-k query (the k values are computed above from `time() / 60 + 1`).
    let mut legacy_rows = Vec::new();
    for (timestamp_seconds, k) in [(0, 1), (60, 2), (120, 3)] {
        let legacy_sql = format!(
            "TQL EVAL ({timestamp_seconds}, {timestamp_seconds}, '60s') topk({k}, {query_body})"
        );
        let (_, mut rows) =
            native_labelled_rows(native_tql_output(&instance, &legacy_sql, false).await).await;
        legacy_rows.append(&mut rows);
    }
    let mut native_rows = native.1.clone();
    native_rows.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    legacy_rows.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    assert_eq!(native_rows, legacy_rows);

    for is_abs in [true, false] {
        let wrapped_query = if is_abs {
            format!("abs(topk(time() / 60 + 1, {query_body}))")
        } else {
            format!("topk(time() / 60 + 1, {query_body}) + 1")
        };
        let wrapped_sql = format!("TQL EVAL (0, 120, '60s') {wrapped_query}");
        observer.clear();
        let wrapped_native =
            native_labelled_rows(native_tql_output(&instance, &wrapped_sql, true).await).await;
        assert!(
            observer.fused().is_some(),
            "wrapped dynamic topk did not run natively: {wrapped_sql}"
        );
        let mut wrapped_expected = Vec::new();
        for (timestamp_seconds, k) in [(0, 1), (60, 2), (120, 3)] {
            let wrapped_literal = if is_abs {
                format!("abs(topk({k}, {query_body}))")
            } else {
                format!("topk({k}, {query_body}) + 1")
            };
            let literal_sql = format!(
                "TQL EVAL ({timestamp_seconds}, {timestamp_seconds}, '60s') {wrapped_literal}"
            );
            let (_, mut rows) =
                native_labelled_rows(native_tql_output(&instance, &literal_sql, false).await).await;
            wrapped_expected.append(&mut rows);
        }
        let mut wrapped_actual = wrapped_native.1;
        wrapped_actual.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
        wrapped_expected.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
        assert_eq!(wrapped_actual, wrapped_expected, "{wrapped_sql}");
    }

    let outer_sql =
        format!("TQL EVAL (0, 120, '60s') topk(1, topk(time() / 60 + 1, {query_body}))");
    observer.clear();
    let outer_native =
        native_labelled_rows(native_tql_output(&instance, &outer_sql, true).await).await;
    // `topk` above a split aggregate needs a whole-vector boundary, so no split answers this
    // statement: the one fused node that reads the series answers it whole.
    assert_eq!(
        observer.answering_node().as_deref(),
        Some("FusedSeriesExec"),
        "outer constant topk over a dynamic topk did not answer natively: {outer_sql}"
    );
    let mut outer_expected = Vec::new();
    for (timestamp_seconds, k) in [(0, 1), (60, 2), (120, 3)] {
        let literal = format!(
            "TQL EVAL ({timestamp_seconds}, {timestamp_seconds}, '60s') topk(1, topk({k}, {query_body}))"
        );
        let (_, mut rows) =
            native_labelled_rows(native_tql_output(&instance, &literal, false).await).await;
        outer_expected.append(&mut rows);
    }
    let mut outer_actual = outer_native.1;
    outer_actual.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    outer_expected.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    assert_eq!(outer_actual, outer_expected, "{outer_sql}");

    let literal_sql = format!("TQL EVAL (0, 120, '60s') topk(3, {query_body})");
    observer.clear();
    let literal_native =
        native_labelled_rows(native_tql_output(&instance, &literal_sql, true).await).await;
    assert!(
        observer.fused().is_some(),
        "literal topk did not run natively"
    );
    let literal_legacy =
        native_labelled_rows(native_tql_output(&instance, &literal_sql, false).await).await;
    // The value column keeps the compiled tree's field name on this route, which the existing
    // planner names after its own rewrite, so only the rows are comparable.
    assert_eq!(literal_native.1, literal_legacy.1);

    // A bare per-point ranking over one plain scan: nothing splits, and the whole vector is ranked
    // where every series of it is gathered rather than per partition.
    let bare_dynamic_sql = "TQL EVAL (0, 120, '60s') topk(time() / 60 + 1, native_dyn_topk)";
    observer.clear();
    let bare_dynamic_native =
        native_labelled_rows(native_tql_output(&instance, bare_dynamic_sql, true).await).await;
    let bare_dynamic_answering = observer.answering_node();
    assert_eq!(
        bare_dynamic_answering.as_deref(),
        Some("FusedSeriesExec"),
        "a bare per-point ranking must answer natively: {bare_dynamic_sql}"
    );
    assert!(
        !bare_dynamic_native.1.is_empty(),
        "a bare per-point ranking must produce rows: {bare_dynamic_sql}"
    );
    let mut bare_dynamic_expected = Vec::new();
    for (timestamp_seconds, k) in [(0, 1), (60, 2), (120, 3)] {
        let literal = format!(
            "TQL EVAL ({timestamp_seconds}, {timestamp_seconds}, '60s') topk({k}, native_dyn_topk)"
        );
        let (_, mut rows) =
            native_labelled_rows(native_tql_output(&instance, &literal, false).await).await;
        bare_dynamic_expected.append(&mut rows);
    }
    // `topk` ranks the series it reads and reports them as it found them, so each row keeps its
    // name. The existing route declares no name column, so its rows carry every other label: the
    // name Prometheus reports is added to them rather than dropped from the native answer.
    let mut bare_dynamic_expected = named_rows(bare_dynamic_expected, "native_dyn_topk");
    let mut bare_dynamic_actual = bare_dynamic_native.1;
    bare_dynamic_actual.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    bare_dynamic_expected.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    assert_eq!(
        bare_dynamic_actual, bare_dynamic_expected,
        "{bare_dynamic_sql}"
    );

    // The same ranking with a parameter that is *only* `time()`: this is the shape a per-point
    // `same_source` guard misclassifies, because the value it carries is a bare per-point constant
    // rather than an arithmetic expression over one. `k` is 0, 60 and 120 at 0s, 60s and 120s, so
    // the first point selects nothing and the other two select every series.
    let bare_time_sql = "TQL EVAL (0, 120, '60s') topk(time(), native_dyn_topk)";
    observer.clear();
    let bare_time_native =
        native_labelled_rows(native_tql_output(&instance, bare_time_sql, true).await).await;
    let bare_time_answering = observer.answering_node();
    assert_eq!(
        bare_time_answering.as_deref(),
        Some("FusedSeriesExec"),
        "a bare topk whose k is `time()` must answer natively: {bare_time_sql}"
    );
    let mut bare_time_expected = Vec::new();
    for (timestamp_seconds, k) in [(0, 0), (60, 60), (120, 120)] {
        let literal = format!(
            "TQL EVAL ({timestamp_seconds}, {timestamp_seconds}, '60s') topk({k}, native_dyn_topk)"
        );
        let (_, mut rows) =
            native_labelled_rows(native_tql_output(&instance, &literal, false).await).await;
        bare_time_expected.append(&mut rows);
    }
    assert_eq!(
        bare_time_native.1.len(),
        bare_time_expected.len(),
        "{bare_time_sql}"
    );
    assert!(
        !bare_time_native.1.is_empty(),
        "a bare topk at k = time() must produce rows: {bare_time_sql}"
    );
    // The ranking reports the series it selected, name included; the existing route has no name
    // column, so the name Prometheus reports is added to its rows.
    let mut bare_time_expected = named_rows(bare_time_expected, "native_dyn_topk");
    let mut bare_time_actual = bare_time_native.1;
    bare_time_actual.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    bare_time_expected.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    assert_eq!(bare_time_actual, bare_time_expected, "{bare_time_sql}");

    // A bare `topk` has no aggregate beneath it, so there is nothing for the split to cut: the
    // whole statement is one whole-vector node over the scan, and it must answer exactly what the
    // existing planner does.
    let bare_sql = "TQL EVAL (0, 120, '60s') topk(3, native_dyn_topk)";
    observer.clear();
    let bare_native =
        native_labelled_rows(native_tql_output(&instance, bare_sql, true).await).await;
    let bare_answering = observer.answering_node();
    let bare_legacy =
        native_labelled_rows(native_tql_output(&instance, bare_sql, false).await).await;
    assert_eq!(
        bare_answering.as_deref(),
        Some("FusedSeriesExec"),
        "a bare topk above no aggregate runs natively: {bare_sql}"
    );
    // The same ranking answers the same rows on both routes; the rows keep the name of the series
    // `topk` selected, which this route states in its own declared column. The two routes name the
    // one value column differently - the existing planner keeps the tree's `val`, the native route
    // names it after the expression - so the columns are the existing answer's, with that name and
    // the name column.
    let mut expected_columns = bare_legacy
        .0
        .iter()
        .filter(|column| *column != "val")
        .cloned()
        .collect::<Vec<_>>();
    expected_columns.push("__name__".to_string());
    expected_columns.push(expression_name("topk(3, native_dyn_topk)"));
    expected_columns.sort();
    assert_eq!(bare_native.0, expected_columns, "{bare_sql}: columns");
    assert_eq!(
        bare_native.1,
        named_rows(bare_legacy.1, "native_dyn_topk"),
        "{bare_sql}"
    );
}

/// A `scalar()` parameter that reads its own selector runs natively: the parameter reads a metric
/// logical table of the same physical table, so the split declines and the multi-input path answers
/// the statement itself.
///
/// The parameter may read a different selector than the input (`topk(scalar(a), sum by(host)(b))`),
/// which makes the planner prepare two sources and the compiler bind both; a metric logical table's
/// id is part of the identity its scan reads, so the two are one table's two metrics and no split
/// can be cut between them.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_scalar_parameter_reads_its_own_selector() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_scalar_param_selector",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_scalar_param_phy (ts TIMESTAMP TIME INDEX, v DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true'); \
         CREATE TABLE native_scalar_param_a (host STRING, ts TIMESTAMP TIME INDEX, v DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='native_scalar_param_phy'); \
         CREATE TABLE native_scalar_param_b (host STRING, ts TIMESTAMP TIME INDEX, v DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='native_scalar_param_phy'); \
         INSERT INTO native_scalar_param_a (host, ts, v) VALUES ('h1', 0, 2), ('h1', 60000, 4); \
         INSERT INTO native_scalar_param_b (host, ts, v) VALUES ('h1', 0, 10), ('h1', 60000, 20)",
        QueryContext::arc(),
    )
    .await;
    // `scalar(native_scalar_param_a)` is 2 at t=0 and 4 at t=60s, so the parameter differs per
    // point.
    //
    // `topk` over one metric whose `k` reads another: a per-point `k` from a second prepared
    // source, in the order the compiler binds them (`b` first, then the parameter's `a`).
    let sql = "TQL EVAL (0, 60, '60s') topk(scalar(native_scalar_param_a), sum by(host)(native_scalar_param_b))";
    observer.clear();
    let topk_native = native_labelled_rows(native_tql_output(&instance, sql, true).await).await;
    let fused = observer.fused();
    let answering = observer.answering_node();
    assert!(
        fused.is_some(),
        "a scalar() parameter that reads a selector must run natively: {sql}"
    );
    assert_eq!(
        answering.as_deref(),
        Some("FusedSeriesExec"),
        "the fused node must answer a scalar() parameter that reads a selector: {sql}"
    );
    assert_eq!(
        topk_native.1.len(),
        2,
        "one row per evaluation point: {sql}"
    );
    assert_eq!(
        topk_native.1.iter().map(|row| row.0).collect::<Vec<_>>(),
        vec![0, 60_000],
        "the expected evaluation timestamps: {sql}"
    );
    // The existing planner rejects `scalar()` over a selector, so each evaluation point is compared
    // against the same statement with the scalar's value at that point written as a literal `k`.
    let mut topk_expected = Vec::new();
    for (t, k) in [(0, 2), (60, 4)] {
        let literal =
            format!("TQL EVAL ({t}, {t}, '60s') topk({k}, sum by(host)(native_scalar_param_b))");
        let (_, mut rows) =
            native_labelled_rows(native_tql_output(&instance, &literal, false).await).await;
        topk_expected.append(&mut rows);
    }
    topk_expected.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    let mut topk_actual = topk_native.1;
    topk_actual.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    assert_eq!(topk_actual, topk_expected, "{sql}");

    // The same per-point value as a `clamp` bound and as a broadcast operand.
    //
    // `clamp_max` is the bound the scalar actually decides here: `b` is 10 and 20 while `a` is 2
    // and 4, so the answer is `a` at both points and a node that ignored the parameter would answer
    // `b` instead. `clamp_min` cannot tell those apart - `b` is above `a` everywhere, so it answers
    // `b` either way - but it is the statement that used to *fail* before the wait was added, and
    // that failure is what it still pins.
    for expression in [
        "clamp_min(native_scalar_param_b, scalar(native_scalar_param_a))",
        "clamp_max(native_scalar_param_b, scalar(native_scalar_param_a))",
        "native_scalar_param_b + scalar(native_scalar_param_a)",
    ] {
        let sql = format!("TQL EVAL (0, 60, '60s') {expression}");
        observer.clear();
        let native = native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
        assert_eq!(
            observer.answering_node().as_deref(),
            Some("FusedSeriesExec"),
            "cross-table dynamic scalar must run natively: {sql}"
        );
        let mut expected = Vec::new();
        for (t, value) in [(0, 2), (60, 4)] {
            let literal = expression.replace("scalar(native_scalar_param_a)", &value.to_string());
            let literal_sql = format!("TQL EVAL ({t}, {t}, '60s') {literal}");
            let (_, mut rows) =
                native_labelled_rows(native_tql_output(&instance, &literal_sql, false).await).await;
            expected.append(&mut rows);
        }
        let mut actual = native.1;
        actual.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
        expected.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
        assert_eq!(actual, expected, "{sql}");
    }
}

/// `round` and the `clamp` functions read a varying parameter at each step, as Prometheus does. A
/// parameter that reads no data runs per partition over the scan. The answers are Prometheus'
/// literal ones: the existing planner does not plan a parameter that is not a number literal, so it
/// has no answer to compare.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_map_with_varying_parameters_matches_prometheus() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_varying_parameter_e2e")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE dyn_param (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)); \
         INSERT INTO dyn_param (host, ts, val) VALUES \
         ('a', 0, 1), ('a', 10000, 2), ('a', 20000, 3), ('b', 20000, 10), ('b', 30000, 20)",
        QueryContext::arc(),
    )
    .await;
    // The nearest is 1, 2, 4 and 8 at the four steps that hold samples.
    let sql = "TQL EVAL (0, 40, '10s', '5s') round(dyn_param, 2 ^ (time() / 10))";
    observer.clear();
    let native = dynamic_scalar_rows(native_tql_output(&instance, sql, true).await).await;
    let plan = observer.take();
    assert_eq!(plan.name(), "FusedSeriesExec");
    assert_eq!(plan.children().len(), 1);
    assert_eq!(plan.output_partitioning().partition_count(), 4);
    assert_eq!(
        native,
        expected_rows(&[
            (0, "a", 1.0),
            (10_000, "a", 2.0),
            (20_000, "a", 4.0),
            (20_000, "b", 12.0),
            (30_000, "b", 24.0),
        ])
    );
}

struct CorpusCase {
    label: &'static str,
    promql: &'static str,
    metrics: &'static [&'static str],
}

const CORPUS: &[CorpusCase] = &[
    CorpusCase {
        label: "Q01",
        promql: r#"cluster:node_cpu:ratio_rate5m{cluster="eks-us-east-1-qa1"}"#,
        metrics: &["cluster:node_cpu:ratio_rate5m"],
    },
    CorpusCase {
        label: "Q02",
        promql: r#"sum(max by (cluster, namespace, pod, container) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m{cluster="eks-us-east-1-qa1"})) by (namespace)"#,
        metrics: &["node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m"],
    },
    CorpusCase {
        label: "Q03",
        promql: r#"sum(max by (cluster, namespace, pod, container) (container_memory_rss{job="kubelet",metrics_path="/metrics/cadvisor",cluster="eks-us-east-1-qa1",container!=""})) by (namespace)"#,
        metrics: &["container_memory_rss"],
    },
    CorpusCase {
        label: "Q05",
        promql: r#"sum(8 * rate(container_network_receive_bytes_total{job="kubelet",metrics_path="/metrics/cadvisor",cluster="eks-us-east-1-qa1",namespace=~".+"}[5m])) by (namespace)"#,
        metrics: &["container_network_receive_bytes_total"],
    },
    CorpusCase {
        label: "Q06",
        promql: r#"sum(8 * irate(container_network_receive_bytes_total{job="kubelet",metrics_path="/metrics/cadvisor",cluster="eks-us-east-1-qa1",namespace="app-0",pod=~"pod-0"}[5m])) by (pod)"#,
        metrics: &["container_network_receive_bytes_total"],
    },
    CorpusCase {
        label: "Q08",
        promql: r#"sum(max by (cluster, namespace, pod, container) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m{cluster="eks-us-east-1-qa1",namespace="app-0"}) * on(cluster, namespace, pod) group_left(workload, workload_type) namespace_workload_pod:kube_pod_owner:relabel{cluster="eks-us-east-1-qa1",namespace="app-0",workload=~".*",workload_type=~".*"}) by (pod)"#,
        metrics: &[
            "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m",
            "namespace_workload_pod:kube_pod_owner:relabel",
        ],
    },
    CorpusCase {
        label: "Q09",
        promql: r#"sum by (pod) ((8 * rate(container_network_receive_bytes_total{cluster="eks-us-east-1-qa1",namespace=~"app-0"}[5m])) * on(cluster, namespace, pod) group_left() topk by (cluster, namespace, pod) (1, max by (cluster, namespace, pod) (kube_pod_info{host_network="false"})))"#,
        metrics: &["container_network_receive_bytes_total", "kube_pod_info"],
    },
    CorpusCase {
        label: "Q10",
        promql: r#"histogram_quantile(0.99, sum(rate(kubelet_pleg_relist_duration_seconds_bucket{cluster="eks-us-east-1-qa1",job="kubelet",instance=~".*"}[5m])) by (instance, le))"#,
        metrics: &["kubelet_pleg_relist_duration_seconds_bucket"],
    },
    CorpusCase {
        label: "Q11",
        promql: r#"sum(rate(rest_client_requests_total{cluster="eks-us-east-1-qa1",job="kubelet",instance=~".*",code=~"2.."}[5m]))"#,
        metrics: &["rest_client_requests_total"],
    },
    CorpusCase {
        label: "Q12",
        promql: r#"sum without(instance, node) (topk(1, kubelet_volume_stats_available_bytes{cluster="eks-us-east-1-qa1",job="kubelet",namespace="app-0",persistentvolumeclaim="persistentvolumeclaim0"}))"#,
        metrics: &["kubelet_volume_stats_available_bytes"],
    },
    CorpusCase {
        label: "L01",
        promql: r#"100 - (avg by (region, az, hostname) (rate(node_cpu_seconds_total{mode="idle", region="us-west-2"}[1m])) * 100)"#,
        metrics: &["node_cpu_seconds_total"],
    },
    CorpusCase {
        label: "L02",
        promql: r#"avg without(cpu) (rate(node_cpu_seconds_total{mode!="idle", region="us-west-2"}[5m]))"#,
        metrics: &["node_cpu_seconds_total"],
    },
    CorpusCase {
        label: "L03",
        promql: r#"sum by (region, az) (rate(node_network_receive_bytes_total{device!="lo", region="us-west-2"}[5m]))"#,
        metrics: &["node_network_receive_bytes_total"],
    },
    CorpusCase {
        label: "L04",
        promql: r#"sum by (region, az, hostname) (rate(node_network_transmit_bytes_total{device!="lo", region="us-west-2"}[5m]))"#,
        metrics: &["node_network_transmit_bytes_total"],
    },
    CorpusCase {
        label: "L05",
        promql: r#"node_memory_MemAvailable_bytes{region="us-west-2"} / node_memory_MemTotal_bytes{region="us-west-2"}"#,
        metrics: &[
            "node_memory_MemAvailable_bytes",
            "node_memory_MemTotal_bytes",
        ],
    },
    CorpusCase {
        label: "L06",
        promql: r#"100 * (1 - node_filesystem_avail_bytes{fstype!~"tmpfs|vfat", region="us-west-2"} / node_filesystem_size_bytes{fstype!~"tmpfs|vfat", region="us-west-2"})"#,
        metrics: &["node_filesystem_avail_bytes", "node_filesystem_size_bytes"],
    },
    CorpusCase {
        label: "L07",
        promql: r#"rate(node_disk_io_time_seconds_total{device="nvme0n1", region="us-west-2"}[5m])"#,
        metrics: &["node_disk_io_time_seconds_total"],
    },
    CorpusCase {
        label: "L08",
        promql: r#"rate(node_disk_read_time_seconds_total{device="nvme0n1", region="us-west-2"}[1m]) / rate(node_disk_reads_completed_total{device="nvme0n1", region="us-west-2"}[1m])"#,
        metrics: &[
            "node_disk_read_time_seconds_total",
            "node_disk_reads_completed_total",
        ],
    },
    CorpusCase {
        label: "L09",
        promql: r#"topk(10, sum by (hostname) (rate(node_network_receive_bytes_total{device!="lo", region="us-west-2"}[5m])))"#,
        metrics: &["node_network_receive_bytes_total"],
    },
    CorpusCase {
        label: "L10",
        promql: r#"count by (region, az) (node_cpu_seconds_total{mode="idle", region="us-west-2"})"#,
        metrics: &["node_cpu_seconds_total"],
    },
    CorpusCase {
        label: "L11",
        promql: r#"sum by (region, az, hostname, device) (rate(node_network_receive_bytes_total{region="us-west-2"}[5m]))"#,
        metrics: &["node_network_receive_bytes_total"],
    },
    CorpusCase {
        label: "L12",
        promql: r#"avg by (region, az) (node_load1{region="us-west-2"})"#,
        metrics: &["node_load1"],
    },
    CorpusCase {
        label: "R01",
        promql: r#"sum by (region, az) (rate(node_network_receive_bytes_total{region="us-west-2"}[1h]))"#,
        metrics: &["node_network_receive_bytes_total"],
    },
    CorpusCase {
        label: "R02",
        promql: r#"100 - (avg by (region, az, hostname) (rate(node_cpu_seconds_total{mode="idle", region="us-west-2"}[1h])) * 100)"#,
        metrics: &["node_cpu_seconds_total"],
    },
    CorpusCase {
        label: "R03",
        promql: r#"100 * (1 - node_filesystem_avail_bytes{fstype!~"tmpfs|vfat", region="us-west-2"} / node_filesystem_size_bytes{fstype!~"tmpfs|vfat", region="us-west-2"})"#,
        metrics: &["node_filesystem_avail_bytes", "node_filesystem_size_bytes"],
    },
    CorpusCase {
        label: "R04",
        promql: r#"topk(10, sum by (hostname) (rate(node_network_receive_bytes_total{device!="lo", region="us-west-2"}[1h])))"#,
        metrics: &["node_network_receive_bytes_total"],
    },
    CorpusCase {
        label: "R05",
        promql: r#"sum by (region, az, hostname, device) (rate(node_network_receive_bytes_total{region="us-west-2"}[1h]))"#,
        metrics: &["node_network_receive_bytes_total"],
    },
];

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_corpus_route_matrix() {
    common_telemetry::init_default_ut_logging();
    let (standalone, observer) = native_test_instance(GreptimeDbStandaloneBuilder::new(
        "native_promql_corpus_route_matrix",
    ))
    .await;
    let instance = standalone.fe_instance().clone();
    let all_tags = &[
        "cluster",
        "namespace",
        "pod",
        "container",
        "job",
        "metrics_path",
        "instance",
        "code",
        "persistentvolumeclaim",
        "node",
        "mode",
        "region",
        "az",
        "hostname",
        "cpu",
        "device",
        "fstype",
        "workload",
        "workload_type",
        "le",
        "host_network",
    ];
    let metrics = CORPUS
        .iter()
        .flat_map(|case| case.metrics.iter().copied())
        .collect::<std::collections::BTreeSet<_>>();
    // Every metric holds its series in four disjoint time windows, one per flushed range, so a scan
    // of this fixture is offered four range streams to read. That is the arrangement this table
    // qualifies: under the earlier single-range fixture a scan read one of its four partitions, and
    // these four range streams read all four. The flush thresholds keep the four flushed ranges
    // from being merged back before the queries run, so the streams stay separate.
    let mut fixture = String::from(
        r#"CREATE TABLE corpus_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ("physical_metric_table"="true");
        ALTER TABLE corpus_phy SET 'compaction.twcs.trigger_file_num'='100';
        ALTER TABLE corpus_phy SET 'compaction.twcs.inactive_window.trigger_file_num'='100';"#,
    );
    for metric in &metrics {
        fixture.push_str(&format!(" CREATE TABLE \"{metric}\" ("));
        for tag in all_tags {
            fixture.push_str(&format!("\"{tag}\" STRING, "));
        }
        fixture.push_str("ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(");
        fixture.push_str(
            &all_tags
                .iter()
                .map(|t| format!("\"{t}\""))
                .collect::<Vec<_>>()
                .join(","),
        );
        fixture.push_str(")) ENGINE=metric WITH (\"on_physical_table\"='corpus_phy');");
    }
    execute_all(&instance, &fixture, QueryContext::arc()).await;
    // One window's rows go in together and are flushed together, so each window is its own range.
    // The series of one window repeat the profile of the first one - the same `pod-0`/`pod-1`,
    // `idle`/`user` and `az` split - and differ by host. `namespace_workload_pod:kube_pod_owner:relabel`
    // is Q08's `group_left` right side, where two matches for one `(cluster, namespace, pod)` would be
    // a semantic error rather than more data, so that one metric gives every window's series a pod of
    // its own.
    for (window, &(first_ts, second_ts)) in CORPUS_WINDOWS.iter().enumerate() {
        let mut inserts = String::new();
        for metric in &metrics {
            let columns = all_tags
                .iter()
                .map(|t| format!("\"{t}\""))
                .chain(["ts".to_string(), "val".to_string()])
                .collect::<Vec<_>>();
            // Histogram bucket samples need multiple `le` values at each timestamp; a single
            // bucket would make Q10's histogram quantile degenerate.
            let buckets: &[&str] = if *metric == "kubelet_pleg_relist_duration_seconds_bucket" {
                &["0.5", "1", "5", "+Inf"]
            } else {
                &["+Inf"]
            };
            let mut rows = Vec::new();
            for slot in 0..2 {
                let host = format!("h{}", 2 * window + slot);
                let (pod, az, mode, cpu, weight) = if slot == 0 {
                    ("pod-0".to_string(), "a0", "idle", "0", 1.0)
                } else {
                    ("pod-1".to_string(), "a1", "user", "1", 2.0)
                };
                let pod = if *metric == "namespace_workload_pod:kube_pod_owner:relabel" {
                    format!("pod-{}", 2 * window + slot)
                } else {
                    pod
                };
                // Scaling the first window's values keeps every window's series the same shape:
                // a two-sample counter whose increase is the one Q04, Q07 and the rate queries
                // derive from.
                let base = weight * (window + 1) as f64;
                for (ts, value) in [(first_ts, base), (second_ts, 301.0 * base)] {
                    for (index, le) in buckets.iter().enumerate() {
                        let values = all_tags
                            .iter()
                            .map(|tag| match *tag {
                                "cluster" => "eks-us-east-1-qa1".to_string(),
                                "namespace" => "app-0".to_string(),
                                "pod" => pod.clone(),
                                "container" => "main".to_string(),
                                "job" => "kubelet".to_string(),
                                "metrics_path" => "/metrics/cadvisor".to_string(),
                                "code" => "200".to_string(),
                                "instance" => host.clone(),
                                "persistentvolumeclaim" => "persistentvolumeclaim0".to_string(),
                                "node" => host.clone(),
                                "region" => "us-west-2".to_string(),
                                "az" => az.to_string(),
                                "hostname" => host.clone(),
                                "mode" => mode.to_string(),
                                "cpu" => cpu.to_string(),
                                "device" => "nvme0n1".to_string(),
                                "fstype" => "ext4".to_string(),
                                "workload" => format!("wl-{pod}"),
                                "workload_type" => "deployment".to_string(),
                                "le" => le.to_string(),
                                "host_network" => "false".to_string(),
                                _ => String::new(),
                            })
                            .map(|v| format!("'{v}'"))
                            .chain([ts.to_string(), (value * (index + 1) as f64).to_string()])
                            .collect::<Vec<_>>();
                        rows.push(format!("({})", values.join(",")));
                    }
                }
            }
            inserts.push_str(&format!(
                " INSERT INTO \"{metric}\" ({}) VALUES {};",
                columns.join(","),
                rows.join(",")
            ));
        }
        // The physical table owns the region the logical metrics share, so one flush per window
        // seals every metric's rows of that window into a range of its own.
        inserts.push_str(" ADMIN FLUSH_TABLE('corpus_phy');");
        execute_all(&instance, &inserts, QueryContext::arc()).await;
    }
    let mut table_rows = Vec::new();
    for case in CORPUS {
        let sql = format!("TQL EVAL (301, 361, '1m') {}", case.promql);
        observer.clear();
        let (legacy_columns, legacy_rows) =
            native_labelled_rows(native_tql_output(&instance, &sql, false).await).await;
        assert!(
            !legacy_rows.is_empty(),
            "{} legacy fixture produced no rows",
            case.label
        );
        observer.clear();
        let (native_columns, native_rows) =
            native_labelled_rows(native_tql_output(&instance, &sql, true).await).await;
        let root = observer.root_plan();
        let root_display = datafusion::physical_plan::displayable(root.as_ref())
            .indent(true)
            .to_string();
        let regions = observer.region_plans();
        let region_displays = regions
            .iter()
            .map(|plan| {
                datafusion::physical_plan::displayable(plan.as_ref())
                    .indent(true)
                    .to_string()
            })
            .collect::<Vec<_>>();
        let fused = !fused_nodes_in_root(&root).is_empty();
        let split = holds_split_merge(&root)
            && regions.iter().any(|p| {
                let d = datafusion::physical_plan::displayable(p.as_ref())
                    .indent(true)
                    .to_string();
                d.contains("FusedSeriesExec") && d.contains(" partial")
            });
        // `segmented` means the frontend's native node consumes what a region's native node
        // produced. A `MergeScan` alone does not show that: its region side can be a plain scan
        // feeding raw frames, which is a different placement and says nothing about a prefix
        // running in the region. The merge scan's own `Debug` carries the plan its region runs, so
        // the check reads that for the segment.
        let region_native_children = fused_nodes_in_root(&root).first().is_some_and(|frontend| {
            let children = frontend.children();
            !children.is_empty()
                && children.iter().all(|child| {
                    let scan = if child.is::<CooperativeExec>() {
                        let wrapped = child.children();
                        if wrapped.len() != 1 {
                            return false;
                        }
                        wrapped[0]
                    } else {
                        child
                    };
                    scan.downcast_ref::<MergeScanExec>()
                        .is_some_and(|scan| format!("{scan:?}").contains("FusedSeries"))
                })
        });
        let segmented = fused && region_native_children;
        // The frontend native node reads raw frames from its scans: the tree runs above the
        // region rather than a prefix running inside it.
        let frontend_raw =
            fused && !region_native_children && root_display.contains("MergeScanExec");
        let region_local = regions.iter().any(|plan| {
            let mut stack = vec![plan.clone()];
            while let Some(node) = stack.pop() {
                if node.name() == "FusedSeriesExec" {
                    return true;
                }
                stack.extend(node.children().into_iter().cloned());
            }
            false
        });
        let route = if segmented {
            "segmented"
        } else if split {
            "aggregate-split"
        } else if frontend_raw {
            "frontend-raw"
        } else if region_local {
            "region-local"
        } else {
            "legacy"
        };
        // Every corpus query is meant to answer natively; a fallback here is the regression this
        // table exists to catch, so it fails rather than being recorded as a route.
        assert_ne!(
            route, "legacy",
            "{} fell back to the existing plan; root:\n{root_display}\nregions: {region_displays:?}",
            case.label
        );
        // Every scan must read rows on all four partitions, and every region-side fused prefix must
        // publish values on all four; a frontend finalize is reported but not asserted. For
        // aggregate-split, the published counts are per-partition partial-state rows, not final
        // query values.
        let scan_report = assert_series_scan_partitions(case.label, &observer);
        let partitions_report = if route == "legacy" {
            "n/a".to_string()
        } else {
            assert_cross_partition_native_values(case.label, &observer)
        };
        // Diagnostics only: the two answers compared as collected. This is not an acceptance
        // condition and not a claim of exact equality for the corpus - acceptance is the row count,
        // the exact labels/timestamps and the 1e-10 relative value tolerance asserted above - so a
        // `false` here is reported in the table as a flag for a reader, never as a pass criterion.
        let rows_bitwise_equal = native_rows == legacy_rows;
        common_telemetry::info!(
            "{} series scan partitions: {scan_report} native partition vectors: {partitions_report}",
            case.label,
        );
        if case.label == "L07" || case.label == "Q01" {
            let mut repeated_values = Vec::new();
            for enabled in [false, false, true] {
                repeated_values.push(
                    native_labelled_rows(native_tql_output(&instance, &sql, enabled).await)
                        .await
                        .1,
                );
            }
            common_telemetry::info!(
                "{} triple runs [native off, native off, native on]: {repeated_values:?}",
                case.label
            );
        }
        let max_rel_diff = native_rows
            .iter()
            .zip(&legacy_rows)
            .map(|(native, legacy)| (native.2 - legacy.2).abs() / legacy.2.abs().max(1.0))
            .fold(0.0_f64, f64::max);
        if route != "legacy" {
            assert_eq!(
                native_rows.len(),
                legacy_rows.len(),
                "{} row count mismatch",
                case.label
            );
            for (native_row, legacy_row) in native_rows.iter().zip(&legacy_rows) {
                // The existing route declares no name column, so its rows answer every other label
                // of the same series. A row that reports a name reports the metric it was read
                // from - this fixture names each metric table after its series - so the name is
                // required to be one of the statement's own metrics and the rest of the label set
                // has to match the existing route exactly.
                //
                // This corpus therefore checks the name's *provenance*, not which shapes keep or
                // drop it: a shape that wrongly kept a name would still pass here. Whether an
                // operator publishes a name is asserted by the dedicated cases
                // (`native_promql_rows_name_each_row_after_its_metric` and the rewrite cases),
                // which read the declared column rather than the labels alone.
                let mut expected_labels = legacy_row.1.clone();
                if let Some(name) = native_row.1.get("__name__") {
                    assert!(
                        case.metrics.contains(&name.as_str()),
                        "{}: a row names {name}, which is not one of the metrics the statement \
                         reads: {native_row:?}",
                        case.label
                    );
                    expected_labels.insert("__name__".to_string(), name.clone());
                }
                assert_eq!(
                    (native_row.0, &native_row.1),
                    (legacy_row.0, &expected_labels),
                    "{} labels/timestamps",
                    case.label
                );
                assert!(
                    (native_row.2 - legacy_row.2).abs() <= 1e-10 * legacy_row.2.abs().max(1.0),
                    "{} value mismatch: {native_row:?} vs {legacy_row:?}",
                    case.label
                );
            }
        }
        let value_column = native_columns
            .iter()
            .find(|column| {
                column.as_str() != "ts"
                    && native_rows
                        .first()
                        .is_none_or(|row| !row.1.contains_key(column.as_str()))
            })
            .cloned()
            .unwrap_or_else(|| {
                if case.label == "Q01" {
                    "val".to_string()
                } else {
                    String::new()
                }
            });
        let row = format!(
            "{} | {} | {} | {} | {:.6e} | {} | {}",
            case.label,
            route,
            if route == "legacy" { "unknown" } else { "—" },
            rows_bitwise_equal,
            max_rel_diff,
            value_column,
            partitions_report
        );
        common_telemetry::info!(
            "{row} | legacy rows={legacy_rows:?} legacy columns={legacy_columns:?} native rows={native_rows:?} native columns={native_columns:?} max_rel_diff={max_rel_diff:.6e} root:\n{root_display} region_plans={region_displays:?}"
        );
        table_rows.push(row);
    }
    common_telemetry::info!(
        "label | route | gate-if-legacy | rows bitwise==legacy (diagnostic) | max rel diff | native value column | partitions used\n{}",
        table_rows.join("\n")
    );
}

/// Walks a physical plan and reports whether `name` appears, and how many times.
fn count_plan_nodes(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let mut count = 0;
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if node.name() == name {
            count += 1;
        }
        stack.extend(node.children().into_iter().cloned());
    }
    count
}

/// Renders `plan` as an indented tree, so a failing boundary assertion shows what it found.
fn display_plan_tree(plan: &Arc<dyn ExecutionPlan>) -> String {
    fn visit(plan: &Arc<dyn ExecutionPlan>, depth: usize, out: &mut String) {
        out.push_str(&format!(
            "{}{}[partitions={}]\n",
            "  ".repeat(depth),
            plan.name(),
            plan.properties().output_partitioning().partition_count()
        ));
        for child in plan.children() {
            visit(child, depth + 1, out);
        }
    }
    let mut out = String::new();
    visit(plan, 0, &mut out);
    out
}

/// The first node named `name` in `plan`, which is the boundary's own stage when there is one.
fn find_plan_node(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<Arc<dyn ExecutionPlan>> {
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if node.name() == name {
            return Some(node);
        }
        stack.extend(node.children().into_iter().cloned());
    }
    None
}

/// Every node named `name` that reads a `child_name`, in traversal order.
///
/// A boundary can have more than one router over different producers, so a test that wants the
/// canonical one has to see all of them rather than the first structural match.
fn find_plan_nodes_over(
    plan: &Arc<dyn ExecutionPlan>,
    name: &str,
    child_name: &str,
) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut found = Vec::new();
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if node.name() == name
            && node
                .children()
                .iter()
                .any(|child| child.name() == child_name)
        {
            found.push(Arc::clone(&node));
        }
        stack.extend(node.children().into_iter().cloned());
    }
    found
}

/// The first node named `name` whose own child is named `child_name`.
///
/// A boundary's router is named the same as the exit a region puts above its answer, so a test that
/// wants the router itself asks for the node that reads the producer.
fn find_plan_node_over(
    plan: &Arc<dyn ExecutionPlan>,
    name: &str,
    child_name: &str,
) -> Option<Arc<dyn ExecutionPlan>> {
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if node.name() == name
            && node
                .children()
                .iter()
                .any(|child| child.name() == child_name)
        {
            return Some(node);
        }
        stack.extend(node.children().into_iter().cloned());
    }
    None
}

/// A label rewrite that folds every series of one metric into one label set, answered from the
/// statement root.
///
/// Every stored series ends under the same label set, so the answer is one series whose points are
/// the union of what the scan partitions held - which is what a canonical merge has to assemble.
/// The scan still spans four partitions, and the value of the test is that it stays that way: a
/// route that gathered the partitions into one execution would answer correctly and take the
/// caller's parallelism away, and a route that answered per partition would report several series
/// under one label set, or the error a duplicate position raises.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_root_canonical_merge_keeps_the_scan_width() {
    common_telemetry::init_default_ut_logging();
    let mito_config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_root_canonical_merge")
            .with_mito_config(mito_config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    // Sixteen series over one timestamp: each partition holds four of them, and every one of them
    // is rewritten to the same label set.
    execute_all(
        &instance,
        "CREATE TABLE root_fold_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true'); \
         CREATE TABLE root_fold (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='root_fold_phy'); \
         INSERT INTO root_fold (host,ts,val) VALUES ('a',0,1),('b',0,2),('c',0,3),('d',0,4),('e',0,5),('f',0,6),('g',0,7),('h',0,8),('i',0,9),('j',0,10),('k',0,11),('l',0,12),('m',0,13),('n',0,14),('o',0,15),('p',0,16)",
        QueryContext::arc(),
    )
    .await;

    let expr = r#"label_replace(root_fold, "host", "same", "host", ".*")"#;
    let sql = format!("TQL EVAL (0, 0, '1s') {expr}");
    observer.clear();
    let native = instance
        .do_query(&sql, native_query_context(true, 4))
        .await
        .remove(0)
        .unwrap();
    let plan = observer.root_plan();
    // The boundary is three stages, and each is checked by name so a plan that answers the query
    // another way fails here rather than silently passing on the answer alone. The consumer and the
    // producer are both on the frontend: a region read reaches the boundary through one source per
    // region, and the width it evaluates at is the session's, not the region count's.
    assert_eq!(
        2,
        count_plan_nodes(&plan, "FusedSeriesExec"),
        "the frontend holds the consumer and the producer: {}",
        display_plan_tree(&plan)
    );
    // Every stored series carries a value at the one evaluation point, so rewriting them to one
    // label set makes the point two values at once - the error Prometheus reports - and it is
    // raised before any of them is published. Reading the answer is also what runs the region half,
    // because a region plan is planned when the statement's stream asks it for batches.
    let error = collect_error(native).await;
    assert!(
        error.contains("cannot contain metrics with the same labelset"),
        "a same-point overlap is the documented error, got: {error}"
    );
    let router = find_plan_node_over(&plan, "SeriesRepartitionExec", "FusedSeriesExec")
        .unwrap_or_else(|| {
            panic!(
                "the boundary routes by canonical label set: {}",
                display_plan_tree(&plan)
            )
        });
    assert_eq!(
        4,
        router.properties().output_partitioning().partition_count(),
        "the canonical router runs at the session's width: {}",
        display_plan_tree(&plan)
    );
    let producer =
        find_plan_node(&plan, "FusedSeriesExec").expect("the frontend runs the producer");
    assert_eq!(
        4,
        producer
            .properties()
            .output_partitioning()
            .partition_count(),
        "the producer evaluates at the session's width: {}",
        display_plan_tree(&plan)
    );
    // The region keeps its own scan width; nothing above it is capped by the number of regions.
    let region_plan = observer
        .region_plans()
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("the read reaches a region"));
    let scan = find_plan_node(&region_plan, "SeriesScanExec").expect("the region scans the metric");
    assert_eq!(
        4,
        scan.properties().output_partitioning().partition_count(),
        "the scan keeps the session's width: {}",
        display_plan_tree(&region_plan)
    );

    // The smallest cross-partition collision case: exactly two stored series, one in a different
    // scan partition from the other, so no producer partition can report the overlap on its own.
    //
    // Which partition a series is scanned in follows its stored identity, not its row order, so the
    // pair is found rather than assumed: sixteen series are written, each is probed alone with a
    // rewrite to a fresh destination - which cannot make two series equal, so it always answers -
    // and the partition that reported it is what that series is scanned in. Two series found in
    // different partitions then get the collapsing rewrite.
    execute_all(
        &instance,
        "CREATE TABLE root_collision_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true'); \
         CREATE TABLE root_collision (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='root_collision_phy'); \
         INSERT INTO root_collision (host,ts,val) VALUES ('a',0,1),('b',0,2),('c',0,3),('d',0,4),('e',0,5),('f',0,6),('g',0,7),('h',0,8),('i',0,9),('j',0,10),('k',0,11),('l',0,12),('m',0,13),('n',0,14),('o',0,15),('p',0,16)",
        QueryContext::arc(),
    )
    .await;
    let mut probed: Vec<(&str, usize)> = Vec::new();
    for host in [
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
    ] {
        observer.clear();
        let single = instance
            .do_query(
                &format!(
                    "TQL EVAL (0, 0, '1s') \
                     label_replace(root_collision{{host=\"{host}\"}}, \"dst\", \"$1\", \"host\", \"(.*)\")"
                ),
                native_query_context(true, 4),
            )
            .await
            .remove(0)
            .unwrap();
        let rows = native_result_rows(single).await;
        assert_eq!(1, rows.len(), "`{host}` answers its own one series");
        let producer = frontend_producer(&observer)
            .unwrap_or_else(|| panic!("`{host}` runs through a canonical producer"));
        // `input_samples` is what the producer read: the merge below it refuses a shared position
        // before any value is published, so only the input counts describe the collision case too.
        let occupied = nonempty_partitions(&producer, "native.root.input_samples");
        assert_eq!(
            1,
            occupied.len(),
            "one series is scanned in exactly one partition: {host}={occupied:?}"
        );
        probed.push((host, occupied[0]));
    }
    let (left_host, left_partition) = probed[0];
    let (right_host, _) = probed
        .iter()
        .copied()
        .find(|(_, partition)| *partition != left_partition)
        .unwrap_or_else(|| {
            panic!("sixteen series spread over the scan's four partitions: {probed:?}")
        });
    observer.clear();
    let two_host_collision = instance
        .do_query(
            &format!(
                "TQL EVAL (0, 0, '1s') \
                 label_replace(root_collision{{host=~\"{left_host}|{right_host}\"}}, \"host\", \"same\", \"host\", \".*\")"
            ),
            native_query_context(true, 4),
        )
        .await
        .remove(0)
        .unwrap();
    let two_host_error = collect_error(two_host_collision).await;
    assert!(
        two_host_error.contains("cannot contain metrics with the same labelset"),
        "the cross-partition canonical router, not the local producer, catches the collision: {two_host_error}"
    );
    // The boundary is on the frontend now, so its router is what holds the two rewritten series:
    // they are scanned in different partitions and meet there, which is why the collision is
    // reported rather than one producer answering from its own share.
    let plan = observer.root_plan();
    let two_host_router = find_plan_node_over(&plan, "SeriesRepartitionExec", "FusedSeriesExec")
        .expect("the collision is routed by the canonical router over the producer");
    assert_eq!(
        4,
        two_host_router
            .properties()
            .output_partitioning()
            .partition_count(),
        "the two-host router runs at the session's width: {}",
        display_plan_tree(&plan)
    );

    // Two series rewritten onto one label set whose points do not meet: the merge joins them into
    // one series instead of reporting a duplicate, which is the other half of the contract. They
    // are ten minutes apart - twice the five-minute lookback - so exactly one of them answers at
    // any evaluation point.
    execute_all(
        &instance,
        "CREATE TABLE root_pair_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true'); \
         CREATE TABLE root_pair (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='root_pair_phy'); \
         INSERT INTO root_pair (host,ts,val) VALUES ('a',0,1),('b',600000,2)",
        QueryContext::arc(),
    )
    .await;
    let pair_expr = r#"label_replace(root_pair, "host", "same", "host", ".*")"#;
    observer.clear();
    let pair = instance
        .do_query(
            &format!("TQL EVAL (0, 600, '1m') {pair_expr}"),
            native_query_context(true, 4),
        )
        .await
        .remove(0)
        .unwrap();
    let pair_plan = observer.root_plan();
    assert_eq!(
        2,
        count_plan_nodes(&pair_plan, "FusedSeriesExec"),
        "the pair boundary holds its producer and consumer on the frontend: {}",
        display_plan_tree(&pair_plan)
    );
    // The pair answers, and answering is what runs the region half of the boundary.
    let (_, pair_rows) = native_labelled_rows(pair).await;
    let pair_router = find_plan_node_over(&pair_plan, "SeriesRepartitionExec", "FusedSeriesExec")
        .expect("the pair's canonical router sits above its producer");
    assert_eq!(
        4,
        pair_router
            .properties()
            .output_partitioning()
            .partition_count(),
        "the pair's router runs at the session's width: {}",
        display_plan_tree(&pair_plan)
    );
    // A rewrite reports the series it read, so every row keeps `root_pair` as its name; the
    // existing route declares no name column, so its answer is the same rows without that label.
    let pair_expected: Vec<(i64, std::collections::BTreeMap<String, String>, f64)> = vec![
        (0, [("host".to_string(), "same".to_string())].into(), 1.0),
        (
            60_000,
            [("host".to_string(), "same".to_string())].into(),
            1.0,
        ),
        (
            120_000,
            [("host".to_string(), "same".to_string())].into(),
            1.0,
        ),
        (
            180_000,
            [("host".to_string(), "same".to_string())].into(),
            1.0,
        ),
        (
            240_000,
            [("host".to_string(), "same".to_string())].into(),
            1.0,
        ),
        (
            600_000,
            [("host".to_string(), "same".to_string())].into(),
            2.0,
        ),
    ];
    assert_eq!(
        pair_rows,
        named_rows(pair_expected.clone(), "root_pair"),
        "the complementary samples form exactly one rewritten series without duplicate rows"
    );
    // The scan looked for the sample at 0 while the evaluation instant was still inside its
    // lookback, which Prometheus reads as an open interval: at 300000 ms the sample at 0 is exactly
    // one lookback old and no longer selected, so the first series ends its answer at 240000 ms.
    // The existing plan applies the same rule, so this is one answer implemented twice and not a
    // second opinion about when a sample goes stale.
    let pair_legacy = instance
        .do_query(
            &format!("TQL EVAL (0, 600, '1m') {pair_expr}"),
            native_query_context(false, 4),
        )
        .await
        .remove(0)
        .unwrap();
    let (_, pair_legacy_rows) = native_labelled_rows(pair_legacy).await;
    assert_eq!(
        pair_legacy_rows, pair_expected,
        "the existing plan answers the complementary pair the same way"
    );

    // A fold over the same complementary pair reads the one assembled series rather than a piece
    // per partition: it answers the complete aggregate over that boundary, once, and the answer is
    // the same as the existing plan's.
    let pair_sum_expr = r#"sum(label_replace(root_pair, "host", "same", "host", ".*"))"#;
    observer.clear();
    let pair_sum = instance
        .do_query(
            &format!("TQL EVAL (0, 600, '1m') {pair_sum_expr}"),
            native_query_context(true, 4),
        )
        .await
        .remove(0)
        .unwrap();
    let pair_sum_plan = observer.root_plan();
    let pair_sum_display = datafusion::physical_plan::displayable(pair_sum_plan.as_ref())
        .indent(true)
        .to_string();
    // The metadata drop this statement performs is judged by its labels alone now, so the pass puts
    // no `Merge` above the fold and the cut below it is accepted: the fold is planned as
    // partition-local `partial` state with a `final` above the state router instead of one
    // `complete` aggregate over the assembled frames. What it must still be is a fold over the
    // *assembled* series rather than over one partition's share of them, and that is the merge
    // sitting below the cut's partial half: every partition's partial folds pieces the merge
    // already routed to it as one label set, and the final half folds their state. The rows below
    // are what pin the answer, against both the expectation and the existing plan.
    assert!(
        pair_sum_display.contains("Aggregate: sum by [] complete")
            || holds_split_merge(&pair_sum_plan),
        "the fold is over the assembled series, complete or as a collected split: {pair_sum_display}"
    );
    assert!(
        pair_sum_display.contains("Aggregate: sum by [] complete")
            || pair_sum_display.contains("Merge: canonical label-set merge"),
        "the split's partial half reads the assembled boundary rather than raw pieces: \
         {pair_sum_display}"
    );
    let (_, pair_sum_rows) = native_labelled_rows(pair_sum).await;
    assert_eq!(
        pair_sum_rows,
        vec![
            (0, std::collections::BTreeMap::new(), 1.0),
            (60_000, std::collections::BTreeMap::new(), 1.0),
            (120_000, std::collections::BTreeMap::new(), 1.0),
            (180_000, std::collections::BTreeMap::new(), 1.0),
            (240_000, std::collections::BTreeMap::new(), 1.0),
            (600_000, std::collections::BTreeMap::new(), 2.0),
        ],
        "one folded series over the assembled boundary, with no labels of its own"
    );
    let pair_sum_legacy = instance
        .do_query(
            &format!("TQL EVAL (0, 600, '1m') {pair_sum_expr}"),
            native_query_context(false, 4),
        )
        .await
        .remove(0)
        .unwrap();
    let (_, pair_sum_legacy_rows) = native_labelled_rows(pair_sum_legacy).await;
    assert_eq!(
        pair_sum_legacy_rows, pair_sum_rows,
        "the existing plan folds the same assembled series"
    );

    // A consumer of the whole label set must see the overlap as an error *before* it hides the
    // duplicate: a fold sums the pieces it is handed, and `topk` counts them against `k`, so either
    // would answer a number instead of reporting what Prometheus reports. Both are checked here so
    // the boundary is known to sit below the consumer and not above it.
    for (shape, consumer) in [
        (
            "a fold",
            r#"sum(label_replace(root_fold, "host", "same", "host", ".*"))"#.to_string(),
        ),
        (
            "a ranking",
            r#"topk(1, label_replace(root_fold, "host", "same", "host", ".*"))"#.to_string(),
        ),
    ] {
        observer.clear();
        let output = instance
            .do_query(
                &format!("TQL EVAL (0, 0, '1s') {consumer}"),
                native_query_context(true, 4),
            )
            .await
            .remove(0)
            .unwrap();
        let error = collect_error(output).await;
        assert!(
            error.contains("cannot contain metrics with the same labelset"),
            "{shape}: the consumer must not hide the overlap, got: {error}"
        );
        let plan = observer.root_plan();
        let display = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        // The boundary sits below the consumer either way, and the merge that collects the rewritten
        // series is what refuses the overlap. What reads it follows the consumer: a ranking above
        // the rewrite assembles its input in the frontend, so the merge is a canonical producer
        // under its own router; a fold drops the metadata labels, and since that drop is judged by
        // its labels alone no merge is put above it - the cut is accepted and the merge sits below
        // the split's partial half, where every partition's partial still folds one label set.
        // What both shapes have in common, and what this asserts, is that merge, and a router over
        // its producer rather than a gather of the series.
        assert!(
            display.contains("Merge: canonical label-set merge"),
            "{shape}: the merge that collects the rewritten series is gone, so nothing can refuse \
             the overlap: {display}"
        );
        assert!(
            canonical_router(&observer).is_some() || holds_split_merge(&plan),
            "{shape}: the boundary is a router over its producer, not a gather: {} regions {:?}",
            display_plan_tree(&plan),
            observer
                .region_plans()
                .iter()
                .map(display_plan_tree)
                .collect::<Vec<_>>()
        );
    }

    // A rewrite above a whole-vector consumer must not make that consumer partition-local: `topk`
    // sees the whole vector and its one answer is what the rewrite renames. No canonical router is
    // involved, and none is needed: the consumer's answer is a single series, so there is no pair
    // of pieces for a rewrite above it to collapse. What must hold is that the ranking ran over
    // every partition - the region gathers its own scan for that - and that the rewrite then saw
    // exactly that one globally chosen series.
    let rewritten_topk = r#"label_replace(topk(1, root_fold), "host", "same", "host", ".*")"#;
    observer.clear();
    let topk_rows = native_labelled_rows(
        instance
            .do_query(
                &format!("TQL EVAL (0, 0, '1s') {rewritten_topk}"),
                native_query_context(true, 4),
            )
            .await
            .remove(0)
            .unwrap(),
    )
    .await
    .1;
    assert_eq!(
        topk_rows.len(),
        1,
        "topk executes after its input is assembled"
    );
    assert_eq!(topk_rows[0].2, 16.0, "topk must choose the global winner");
    let topk_region = observer
        .region_plans()
        .into_iter()
        .find(|plan| count_plan_nodes(plan, "FusedSeriesExec") > 0)
        .unwrap_or_else(|| {
            panic!(
                "the ranking runs over the region's own scan: regions {:?}",
                observer
                    .region_plans()
                    .iter()
                    .map(display_plan_tree)
                    .collect::<Vec<_>>()
            )
        });
    assert_eq!(
        4,
        find_plan_node(&topk_region, "SeriesScanExec")
            .expect("the region scans the metric")
            .properties()
            .output_partitioning()
            .partition_count(),
        "the scan below the ranking keeps its four partitions: {}",
        display_plan_tree(&topk_region)
    );

    // An aggregate after a colliding rewrite cannot hide the same-point conflict: the merge must
    // validate its canonical input before the aggregate consumes it. The fold drops the metadata
    // labels and that drop is judged by its labels alone now, so no merge is put above the fold and
    // the cut below it is accepted: the fold is partition-local `partial` state with a `final`
    // above the state router, and the merge that validates the overlap sits below the partial half.
    // The error asserted here is what says it validated before anything folded.
    let rewritten_sum = r#"sum(label_replace(root_fold, "host", "same", "host", ".*"))"#;
    observer.clear();
    let sum_output = instance
        .do_query(
            &format!("TQL EVAL (0, 0, '1s') {rewritten_sum}"),
            native_query_context(true, 4),
        )
        .await
        .remove(0)
        .unwrap();
    let sum_error = collect_error(sum_output).await;
    assert!(
        sum_error.contains("cannot contain metrics with the same labelset"),
        "the aggregate must not hide the overlap, got: {sum_error}"
    );
    let sum_plan = observer.root_plan();
    let sum_display = datafusion::physical_plan::displayable(sum_plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        sum_display.contains("Aggregate: sum by [] complete") || holds_split_merge(&sum_plan),
        "the fold runs natively over the boundary, complete or as a collected split: {sum_display}"
    );
    assert!(
        sum_display.contains("Aggregate: sum by [] complete")
            || sum_display.contains("Merge: canonical label-set merge"),
        "the split's partial half still reads the canonical merge, so the overlap is validated \
         before anything folds it: {sum_display}"
    );
    assert!(
        canonical_router(&observer).is_some() || holds_split_merge(&sum_plan),
        "the fold's prefix keeps its router over the producer: {sum_display}"
    );

    // A rewrite above the aggregate is pointwise on its single globally computed output, and must
    // preserve that output while the scan stays partitioned below the aggregate boundary.
    let rewrite_sum = r#"label_replace(sum(root_fold), "dst", "sum", "host", ".*")"#;
    observer.clear();
    let rewrite_sum_rows = native_labelled_rows(
        instance
            .do_query(
                &format!("TQL EVAL (0, 0, '1s') {rewrite_sum}"),
                native_query_context(true, 4),
            )
            .await
            .remove(0)
            .unwrap(),
    )
    .await
    .1;
    assert_eq!(
        rewrite_sum_rows,
        vec![(0, [("dst".to_string(), "sum".to_string())].into(), 136.0)],
        "outer rewrite receives the globally summed output exactly once"
    );

    // An inner rewrite aliases every input at the sole evaluation point. That overlap must fail
    // before topk sees the vector; an outer rewrite must not hide or move the error.
    //
    // The inner merge is its own boundary and the ranking above it gathers the assembled frames
    // into one execution, so the route answers this shape natively. What it must not do is answer
    // it per partition: `topk` over pieces it should have collected would publish one winner per
    // partition and the outer rewrite would rename a number no label set ever stood for.
    let rewrite_topk_rewrite = r#"label_replace(topk(1, label_replace(root_fold, "host", "inner", "host", ".*")), "host", "outer", "host", ".*")"#;
    observer.clear();
    let nested_output = instance
        .do_query(
            &format!("TQL EVAL (0, 0, '1s') {rewrite_topk_rewrite}"),
            native_query_context(true, 4),
        )
        .await
        .remove(0)
        .unwrap();
    let nested_error = collect_error(nested_output).await;
    assert!(
        nested_error.contains("cannot contain metrics with the same labelset"),
        "the inner rewrite overlaps before topk, got: {nested_error}"
    );
    assert_eq!(
        observer.fused().as_ref().map(|plan| plan.name()),
        Some("FusedSeriesExec"),
        "the nested rewrite stays on the native path"
    );
    assert!(
        canonical_router(&observer).is_some(),
        "the ranking reads the assembled boundary rather than per-partition pieces"
    );
    let nested_legacy = instance
        .do_query(
            &format!("TQL EVAL (0, 0, '1s') {rewrite_topk_rewrite}"),
            native_query_context(false, 4),
        )
        .await
        .remove(0)
        .unwrap();
    let nested_legacy_error = collect_error(nested_legacy).await;
    assert!(
        nested_legacy_error.contains("cannot contain metrics with the same labelset"),
        "the existing plan reports the overlap for this shape too: {nested_legacy_error}"
    );

    // The legacy plan reports the same error for the same shape, so the native route is a
    // different implementation of one answer and not a different answer.
    for consumer in [
        r#"label_replace(root_fold, "host", "same", "host", ".*")"#,
        r#"sum(label_replace(root_fold, "host", "same", "host", ".*"))"#,
        r#"topk(1, label_replace(root_fold, "host", "same", "host", ".*"))"#,
    ] {
        let legacy = instance
            .do_query(
                &format!("TQL EVAL (0, 0, '1s') {consumer}"),
                native_query_context(false, 4),
            )
            .await
            .remove(0)
            .unwrap();
        let legacy_error = collect_error(legacy).await;
        assert!(
            legacy_error.contains("cannot contain metrics with the same labelset"),
            "the existing plan reports the overlap too: {consumer}: {legacy_error}"
        );
    }
}

/// A fold over a label rewrite reports the collision the rewrite created, across scan partitions.
///
/// The rewrite gives two series one label set, and a fold cannot answer two values at one grid point:
/// the pieces of the set have to meet before anything folds them. They meet at the canonical
/// boundary - each partition's producer collects what its own scan holds, the canonical router routes
/// every piece of one label set to one owner, and the fold above it reads whole series - so two
/// colliding series that were scanned in *different* partitions are reported as `vector cannot
/// contain metrics with the same labelset` instead of being folded into the sum of two partial
/// states, which is a number no label set ever stood for.
///
/// Which partition scans a series follows its stored identity, not its row order, so the pair is
/// found rather than assumed: sixteen series are written, each is probed alone with a rewrite to a
/// fresh destination - which cannot make two series equal, so it always answers - and the partition
/// that reported it is what that series is scanned in. Two series found in different partitions then
/// get the collapsing rewrite.
///
/// Three claims are made below: that cross-partition pair, the same statement at one partition (which
/// must report the same error rather than a value), and a rewrite of the same pair that does not
/// collapse it, which still answers the sum of both values. The last one is what says the error above
/// is the collision's and not the fold's.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_fold_over_cross_partition_collision_reports_the_collision() {
    common_telemetry::init_default_ut_logging();
    let mito_config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_fold_collision")
            .with_mito_config(mito_config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    // Sixteen series over one timestamp, one value each: the scan hands them out over the session's
    // four partitions, so a pair of them can be found in two different partitions.
    let hosts = [
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
    ];
    execute_all(
        &instance,
        &format!(
            "CREATE TABLE fold_collision_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true'); \
             CREATE TABLE fold_collision (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host)) ENGINE=metric WITH ('on_physical_table'='fold_collision_phy'); \
             INSERT INTO fold_collision (host,ts,val) VALUES {}",
            hosts
                .iter()
                .enumerate()
                .map(|(index, host)| format!("('{host}',0,{})", index + 1))
                .collect::<Vec<_>>()
                .join(",")
        ),
        QueryContext::arc(),
    )
    .await;
    let value_of = |host: &str| f64::from(host.as_bytes()[0] - b'a' + 1);

    let mut probed: Vec<(&str, usize)> = Vec::new();
    for host in hosts {
        observer.clear();
        let single = instance
            .do_query(
                &format!(
                    "TQL EVAL (0, 0, '1s') \
                     label_replace(fold_collision{{host=\"{host}\"}}, \"dst\", \"$1\", \"host\", \"(.*)\")"
                ),
                native_query_context(true, 4),
            )
            .await
            .remove(0)
            .unwrap();
        let rows = native_result_rows(single).await;
        assert_eq!(1, rows.len(), "`{host}` answers its own one series");
        let producer =
            frontend_producer(&observer).unwrap_or_else(|| panic!("`{host}` runs natively"));
        // `input_samples` is what the producer read: a merge below it refuses a shared position
        // before any value is published, so only the input counts describe the collision case too.
        let occupied = nonempty_partitions(&producer, "native.root.input_samples");
        assert_eq!(
            1,
            occupied.len(),
            "one series is scanned in exactly one partition: {host}={occupied:?}"
        );
        probed.push((host, occupied[0]));
    }
    let (left_host, left_partition) = probed[0];
    let (right_host, _) = probed
        .iter()
        .copied()
        .find(|(_, partition)| *partition != left_partition)
        .unwrap_or_else(|| {
            panic!("sixteen series spread over the scan's four partitions: {probed:?}")
        });

    // The collision, with the two series scanned in two different partitions. Every output of the
    // boundary holds whole label sets, so the two rewritten series meet there and the second value at
    // that one position is the error Prometheus reports. Reading the answer is also what runs the
    // region half, because a region plan is planned when the statement's stream asks for batches.
    let collapsing = format!(
        "TQL EVAL (0, 0, '1s') \
         sum(label_replace(fold_collision{{host=~\"{left_host}|{right_host}\"}}, \"host\", \"same\", \"host\", \".*\"))"
    );
    observer.clear();
    let cross_partition = instance
        .do_query(&collapsing, native_query_context(true, 4))
        .await
        .remove(0)
        .unwrap();
    let cross_partition_error = collect_error(cross_partition).await;
    assert!(
        cross_partition_error.contains("vector cannot contain metrics with the same labelset"),
        "the fold must report the overlap its rewrite created rather than add two partial states: \
         {cross_partition_error}"
    );
    // The error is the boundary's, so the statement was not replanned as legacy: the canonical
    // router runs over a producer whose own tree is the merge, and the pieces the producer read came
    // from the two partitions the pair was scanned in.
    let plan = observer.root_plan();
    let router = canonical_router(&observer).unwrap_or_else(|| {
        panic!(
            "the fold reads a canonical boundary rather than one partition's pieces: {} \
             regions {:?}",
            display_plan_tree(&plan),
            observer
                .region_plans()
                .iter()
                .map(display_plan_tree)
                .collect::<Vec<_>>()
        )
    });
    let producer = router.children()[0].clone();
    assert_eq!(
        producer.name(),
        "FusedSeriesExec",
        "the router reads the producer: {}",
        display_plan_tree(&plan)
    );
    let producer_display = datafusion::physical_plan::displayable(producer.as_ref())
        .one_line()
        .to_string();
    let producer_tree = producer_display
        .split_once('\n')
        .map(|(_, tree)| tree)
        .unwrap_or_else(|| panic!("the producer renders its tree: {producer_display}"));
    assert!(
        producer_tree.contains("Merge: canonical label-set merge"),
        "the producer's tree is the canonical merge itself: {producer_tree}"
    );
    let read = nonempty_partitions(&producer, "native.root.input_samples");
    assert!(
        read.len() > 1,
        "the two colliding series are read in different execution partitions: {read:?}"
    );

    // The same statement at one partition. Both series are scanned by the one execution there, so the
    // collision is that execution's to report - the same error, not a value - which is what makes the
    // cross-partition case above a question of where the pieces are and not of what they mean.
    observer.clear();
    let single_partition = instance
        .do_query(&collapsing, native_query_context(true, 1))
        .await
        .remove(0)
        .unwrap();
    let single_partition_error = collect_error(single_partition).await;
    assert!(
        single_partition_error.contains("vector cannot contain metrics with the same labelset"),
        "one partition reports the same overlap: {single_partition_error}"
    );

    // The control: the same pair of series, rewritten onto a label of their own. Two series with
    // different values at one point are two series, so the fold answers their sum - the answer the
    // collision above must not silently become.
    let control = format!(
        "TQL EVAL (0, 0, '1s') \
         sum(label_replace(fold_collision{{host=~\"{left_host}|{right_host}\"}}, \"dst\", \"$1\", \"host\", \"(.*)\"))"
    );
    observer.clear();
    let control_output = instance
        .do_query(&control, native_query_context(true, 4))
        .await
        .remove(0)
        .unwrap();
    let (_, control_rows) = native_labelled_rows(control_output).await;
    assert_eq!(
        1,
        control_rows.len(),
        "a fold with no grouping answers one row: {control_rows:?}"
    );
    assert_eq!(
        control_rows[0].2,
        value_of(left_host) + value_of(right_host),
        "the fold answers the sum of the two values it collected: {control_rows:?}"
    );
    assert!(
        control_rows[0].1.is_empty(),
        "`sum` keeps no label of its own: {control_rows:?}"
    );
}

/// A subquery evaluates its inner expression on its own grid, on the native route.
///
/// A window over an aggregate is the split's one admitted evaluated-input shape: the subquery's
/// declared input grid equals the suffix's, so the split carries it rather than declining. Each
/// answer is compared with the existing planner's, and the route is asserted through the observer.
///
/// A window over an evaluated input is not a per-series prefix, though: it drops the metric name, so
/// the dist analyzer reads it as cross-series and will not place a segment over one. The shapes below
/// it - a raw window's `rate`, a `label_replace` map - are cut instead, and where nothing below the
/// window can carry the tree the statement keeps the plan the existing route builds. Either way it
/// answers the rows Prometheus does, and never the "no installed planner" failure a segment the
/// analyzer refuses produces.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_subqueries_match_legacy() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_subqueries").with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_subq (host STRING, job STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host, job));
         INSERT INTO native_subq (host, job, ts, val) VALUES
         ('h0', 'a', 60000, 0), ('h0', 'a', 120000, 10), ('h0', 'a', 180000, 20),
         ('h1', 'a', 60000, 0), ('h1', 'a', 120000, 30), ('h1', 'a', 180000, 60),
         ('h0', 'b', 60000, 0), ('h0', 'b', 120000, 40), ('h0', 'b', 180000, 80),
         ('h1', 'b', 60000, 0), ('h1', 'b', 120000, 50), ('h1', 'b', 180000, 100)"#,
        QueryContext::arc(),
    )
    .await;

    for sql in [
        "TQL EVAL (180, 180, '1m') avg_over_time(native_subq[2m:1m])",
        "TQL EVAL (180, 180, '1m') max_over_time(rate(native_subq[2m])[2m:1m])",
        "TQL EVAL (180, 180, '1m') avg_over_time((sum by (job, host) (rate(native_subq[2m])))[2m:1m])",
        "TQL EVAL (180, 180, '1m') avg_over_time((sum by (job, host) (rate(native_subq[2m])))[2m:1m] @ 180)",
        "TQL EVAL (180, 180, '1m') max_over_time(max_over_time(native_subq[2m:1m])[2m:1m])",
        // A subquery body that is a mapped selection rather than a fold: the segment under the
        // window is the `Map` that clamps, and it has to publish on the subquery's own grid - the
        // two points at 120000 and 180000 at one-minute steps - rather than on the statement's,
        // which holds the single point at 180000. The two grids therefore differ in both their
        // start and their length, not just in identity.
        "TQL EVAL (180, 180, '1m') avg_over_time(clamp(native_subq, 0, 1)[2m:1m])",
    ] {
        observer.clear();
        let mut native = native_result_rows(native_tql_output(&instance, sql, true).await).await;
        assert!(
            observer.fused().is_some(),
            "{sql} did not execute a FusedSeriesExec: {:#?}",
            observer.root_plan()
        );
        observer.clear();
        let mut legacy = native_result_rows(native_tql_output(&instance, sql, false).await).await;
        // The two routes emit the same rows but not in the same order: the helper sorts within a
        // batch, and a series' label is one column of that batch's own order.
        assert!(
            observer.fused().is_none(),
            "{sql} must keep the existing plan when the native route is off"
        );
        native.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)).then(a.2.total_cmp(&b.2)));
        legacy.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)).then(a.2.total_cmp(&b.2)));
        assert_eq!(native, legacy, "{sql}");
    }

    // The anchored shape is compared with the existing route only at its anchor. Past the anchor the
    // existing route re-folds the subquery's window on every outer step, and the split must not copy
    // that bug, so the later steps are held to the invariant on the native answer alone: an anchored
    // subquery's range vector is fixed at 180, so each outer step has to publish 180's value for a
    // series rather than fold a fresh one.
    let anchored = "TQL EVAL (180, 360, '1m') \
                    avg_over_time((sum by (job, host) (rate(native_subq[2m])))[2m:1m] @ 180)";
    observer.clear();
    let anchored_rows = native_rows(native_tql_output(&instance, anchored, true).await, None).await;
    assert!(
        observer.fused().is_some(),
        "{anchored} did not execute a FusedSeriesExec: {:#?}",
        observer.root_plan()
    );
    // Every label column names the series, so `job` is part of the key as well as `host`.
    let mut series =
        std::collections::BTreeMap::<&str, std::collections::BTreeMap<i64, f64>>::new();
    for (ts, label, value) in &anchored_rows {
        assert!(
            series
                .entry(label.as_str())
                .or_default()
                .insert(*ts, *value)
                .is_none(),
            "{anchored} answered {label} twice at {ts}"
        );
    }
    assert!(
        !series.is_empty(),
        "{anchored} answered no series: {:#?}",
        observer.root_plan()
    );
    for step in [180000, 240000, 300000] {
        assert!(
            series.values().all(|steps| steps.contains_key(&step)),
            "{anchored} did not answer {step} for every series: {anchored_rows:?}"
        );
    }
    // An anchored subquery must not re-fold when the outer step advances.
    for (label, steps) in &series {
        let anchor = steps[&180000];
        for (step, value) in steps {
            assert_eq!(
                anchor, *value,
                "{anchored}: {label} holds the anchored value at 180000 but folded {value} at {step}"
            );
        }
    }

    // A fold over a subquery whose input is not a bare selection. The window is not itself cut as a
    // segment - it drops the metric name, so the analyzer reads it as cross-series - and what keeps
    // the scan width is the per-series prefix below it or the canonical boundary a rewrite needs.
    // `fused` says which plan answers, and every case is compared with the existing route row by
    // row. The one shape that still keeps the existing plan is the rewrite: its window's input is
    // cross-series, so `region_local_segment` refuses the cut and there is no per-series prefix
    // inside the window to take instead.
    for (sql, label, fused) in [
        (
            "TQL EVAL (180, 180, '1m') sum by (job) (max_over_time(rate(native_subq[2m])[5m:1m]))",
            Some("job"),
            true,
        ),
        // The rewrite writes a new label, so its map is per-series: the subquery's inner map is
        // the segment, and it publishes on the subquery's grid rather than the statement's. It
        // runs natively now; the rows below are what say it answers what the existing plan does.
        (
            "TQL EVAL (180, 180, '1m') sum by (x) \
             (avg_over_time(label_replace(native_subq, \"x\", \"$1\", \"job\", \"(.*)\")[5m:1m]))",
            Some("x"),
            true,
        ),
        (
            "TQL EVAL (180, 180, '1m') max_over_time(sum by (job) \
             (max_over_time(rate(native_subq[2m])[3m:1m]))[5m:1m])",
            Some("job"),
            true,
        ),
        // This shape used to keep the existing plan, on the grounds that a subquery evaluates its
        // own body on a grid of its own. It runs natively now: the `rate` over the raw scan is the
        // per-series segment and the window above it publishes, so the fused node below is the plan
        // that answers. The rows are what says that plan answers what the existing one does.
        (
            "TQL EVAL (180, 180, '1m') sum_over_time(rate(native_subq[5m])[1h:])",
            None,
            true,
        ),
    ] {
        observer.clear();
        let mut native = native_rows(native_tql_output(&instance, sql, true).await, label).await;
        assert_eq!(
            fused,
            observer.fused().is_some(),
            "{sql}: the native request must {} a FusedSeriesExec: {:#?}",
            if fused {
                "run"
            } else {
                "keep the existing plan for"
            },
            observer.root_plan()
        );
        assert!(
            !native.is_empty(),
            "{sql} answered no rows: {:#?}",
            observer.root_plan()
        );
        let mut legacy = native_rows(native_tql_output(&instance, sql, false).await, label).await;
        native.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)).then(a.2.total_cmp(&b.2)));
        legacy.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)).then(a.2.total_cmp(&b.2)));
        assert_eq!(native, legacy, "{sql}");
    }

    // A rewrite that writes a label the series already carry collapses them: the canonicalizer puts
    // a merge at the statement root, and the segmentation below runs with canonical segmentation on.
    // The fold inside the subquery is a global consumer, so it is cut recursively, and that nested
    // segment answers the subquery's own grid - the two points at 120000 and 180000 - while the
    // statement's grid holds the single point at 180000. Publishing the nested segment on the
    // caller's grid rejects its binding, and the rejection aborts the whole route, so the statement
    // has to stay native here and answer what the existing plan answers.
    //
    // `host="h0"` keeps one series: the rewrite makes every series' label set `host="all"`, and two
    // of them meeting at one grid point is the overlap both routes report as an error.
    let collapse = "TQL EVAL (180, 180, '1m') label_replace(avg_over_time((sum by (host) \
                    (native_subq{host=\"h0\"}))[2m:1m]), \"host\", \"all\", \"\", \"\")";
    observer.clear();
    let collapse_native =
        native_result_rows(native_tql_output(&instance, collapse, true).await).await;
    assert!(
        observer.fused().is_some(),
        "{collapse} must run on the native route: {:#?}",
        observer.root_plan()
    );
    observer.clear();
    let collapse_legacy =
        native_result_rows(native_tql_output(&instance, collapse, false).await).await;
    assert!(
        observer.fused().is_none(),
        "{collapse} must keep the existing plan when the native route is off"
    );
    // `h0` sums to 10 + 40 at 120000 and 20 + 80 at 180000, so the fold over the subquery's two
    // points averages to 75 at the statement's one step.
    assert_eq!(
        collapse_native,
        vec![(180_000, "all".to_string(), 75.0)],
        "{collapse}"
    );
    assert_eq!(collapse_native, collapse_legacy, "{collapse}");
}

/// An aggregate inside a subquery is still an aggregate, so it keeps that operand out of the
/// hybrid-binary join.
///
/// Both statements below are two single-selector vector operands on the same matching labels, which
/// is what the join admits - but one operand folds a group one subquery down. With the fold counted
/// as a fold, both run on the native route through `build_evaluated_segment` and answer the same
/// rows the existing route does.
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_aggregate_subquery_operands_run_natively() {
    common_telemetry::init_default_ut_logging();
    let config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_aggregate_subquery")
            .with_mito_config(config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        r#"CREATE TABLE native_pm (host STRING, job STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host, job));
         INSERT INTO native_pm (host, job, ts, val) VALUES
         ('h0', 'a', 60000, 0), ('h0', 'a', 120000, 10), ('h0', 'a', 180000, 20),
         ('h1', 'a', 60000, 0), ('h1', 'a', 120000, 30), ('h1', 'a', 180000, 60),
         ('h0', 'b', 60000, 0), ('h0', 'b', 120000, 40), ('h0', 'b', 180000, 80),
         ('h1', 'b', 60000, 0), ('h1', 'b', 120000, 50), ('h1', 'b', 180000, 100);
         CREATE TABLE native_pn (host STRING, job STRING, ts TIMESTAMP(3) TIME INDEX, val DOUBLE, PRIMARY KEY (host, job));
         INSERT INTO native_pn (host, job, ts, val) VALUES
         ('h0', 'a', 60000, 1), ('h0', 'a', 120000, 2), ('h0', 'a', 180000, 3),
         ('h1', 'a', 60000, 4), ('h1', 'a', 120000, 5), ('h1', 'a', 180000, 6),
         ('h0', 'b', 60000, 7), ('h0', 'b', 120000, 8), ('h0', 'b', 180000, 9),
         ('h1', 'b', 60000, 10), ('h1', 'b', 120000, 11), ('h1', 'b', 180000, 12)"#,
        QueryContext::arc(),
    )
    .await;

    for sql in [
        "TQL EVAL (180, 180, '1m') avg_over_time((sum by (job, host) (native_pm))[5m:1m]) + on(job, host) native_pn",
        "TQL EVAL (180, 180, '1m') native_pm + on(job, host) avg_over_time((sum by (job, host) (native_pn))[5m:1m])",
    ] {
        observer.clear();
        let (_, mut native) =
            native_labelled_rows(native_tql_output(&instance, sql, true).await).await;
        assert!(
            !native.is_empty(),
            "{sql} answered no rows: {:#?}",
            observer.root_plan()
        );
        assert!(
            observer.fused().is_some(),
            "{sql} must run on the native route"
        );
        observer.clear();
        let (_, mut legacy) =
            native_labelled_rows(native_tql_output(&instance, sql, false).await).await;
        native.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)).then(a.2.total_cmp(&b.2)));
        legacy.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)).then(a.2.total_cmp(&b.2)));
        assert_eq!(native, legacy, "{sql}");
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn native_promql_matrix_root_matches_legacy() {
    common_telemetry::init_default_ut_logging();
    let mito_config = mito2::config::MitoConfig {
        experimental_series_scan_v2: true,
        ..Default::default()
    };
    let (standalone, observer) = native_test_instance(
        GreptimeDbStandaloneBuilder::new("native_promql_matrix_root").with_mito_config(mito_config),
    )
    .await;
    let instance = standalone.fe_instance().clone();
    execute_all(
        &instance,
        "CREATE TABLE native_matrix_root (host STRING, ts TIMESTAMP TIME INDEX, val DOUBLE, PRIMARY KEY(host))",
        QueryContext::arc(),
    )
    .await;
    let mut values = Vec::new();
    for host in 0..4 {
        for minute in 0..=10 {
            values.push(format!(
                "('h{host}', {}, {})",
                minute * 60_000,
                host * 1000 + minute
            ));
        }
    }
    execute_all(
        &instance,
        &format!(
            "INSERT INTO native_matrix_root (host, ts, val) VALUES {}",
            values.join(",")
        ),
        QueryContext::arc(),
    )
    .await;

    async fn run(
        instance: &Arc<Instance>,
        observer: &NativePlanObserver,
        sql: &str,
        native: bool,
    ) -> Vec<(i64, String, f64)> {
        observer.clear();
        native_result_rows(native_tql_output(instance, sql, native).await).await
    }

    let sql = "TQL EVAL (600, 600, '1m') native_matrix_root[5m:1m]";
    let native = run(&instance, &observer, sql, true).await;
    assert!(
        observer.fused().is_some(),
        "subquery root did not run natively: {sql}"
    );
    let legacy = run(&instance, &observer, sql, false).await;
    assert_eq!(native, legacy);
    assert_eq!(
        native
            .iter()
            .map(|row| row.0)
            .collect::<std::collections::BTreeSet<_>>(),
        [360_000, 420_000, 480_000, 540_000, 600_000].into()
    );

    let sql = "TQL EVAL (600, 600, '1m') native_matrix_root[5m:1m] offset 1m";
    let native = run(&instance, &observer, sql, true).await;
    assert!(
        observer.fused().is_some(),
        "offset subquery root did not run natively: {sql}"
    );
    let legacy = run(&instance, &observer, sql, false).await;
    assert_eq!(native, legacy);
    assert_eq!(
        native
            .iter()
            .map(|row| row.0)
            .collect::<std::collections::BTreeSet<_>>(),
        [300_000, 360_000, 420_000, 480_000, 540_000].into()
    );

    // PromQL's `@ 420` is seconds, so this anchor is 420,000ms on the stored timestamp grid.
    let anchor_sql = "TQL EVAL (600, 600, '1m') native_matrix_root[5m:1m] @ 420";
    let anchored = run(&instance, &observer, anchor_sql, true).await;
    let other_instant = "TQL EVAL (540, 540, '1m') native_matrix_root[5m:1m] @ 420";
    let anchored_other = run(&instance, &observer, other_instant, true).await;
    assert_eq!(
        anchored, anchored_other,
        "@ anchor must ignore query instant"
    );
    assert_eq!(
        anchored
            .iter()
            .map(|row| row.0)
            .collect::<std::collections::BTreeSet<_>>(),
        [180_000, 240_000, 300_000, 360_000, 420_000].into()
    );
    let mut expected_values = (0..4)
        .flat_map(|host| {
            (3..=7).map(move |minute| {
                (
                    minute * 60_000,
                    format!("h{host}"),
                    (host * 1000 + minute) as f64,
                )
            })
        })
        .collect::<Vec<_>>();
    expected_values.sort_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
    assert_eq!(anchored, expected_values);

    for (expr, grid) in [
        (
            "(native_matrix_root @ start())[5m:1m]",
            [360_000, 420_000, 480_000, 540_000, 600_000],
        ),
        (
            "(native_matrix_root @ end())[5m:1m]",
            [360_000, 420_000, 480_000, 540_000, 600_000],
        ),
    ] {
        let sql = format!("TQL EVAL (600, 600, '1m') {expr}");
        let native = run(&instance, &observer, &sql, true).await;
        assert!(observer.fused().is_some(), "{expr} did not run natively");
        let legacy = run(&instance, &observer, &sql, false).await;
        assert_eq!(native, legacy, "{expr}");
        assert_eq!(
            native
                .iter()
                .map(|row| row.0)
                .collect::<std::collections::BTreeSet<_>>(),
            grid.into()
        );
    }

    let sql = "TQL EVAL (60, 60, '1m') native_matrix_root[5m:1m]";
    let native = run(&instance, &observer, sql, true).await;
    assert!(
        observer.fused().is_some(),
        "negative child grid did not run natively"
    );
    let legacy = run(&instance, &observer, sql, false).await;
    assert_eq!(native, legacy, "negative child grid");

    // A statement before the Unix epoch is valid PromQL, and the subquery root rebuilds the
    // statement from the planner's own bounds. -60s has to stay 60 seconds *before* the epoch: read
    // as a magnitude it lands hundreds of millions of years in the future, which is not a grid this
    // route can build, and an error there would replace the answer the existing planner gives.
    // The bounds are quoted because `TQL EVAL` only accepts a pre-epoch time as a string: an
    // unquoted `-60` is a negation, which the TQL parser rejects before planning.
    async fn run_timestamps(
        instance: &Arc<Instance>,
        observer: &NativePlanObserver,
        sql: &str,
        native: bool,
    ) -> Vec<(i64, f64)> {
        observer.clear();
        native_timestamp_rows(native_tql_output(instance, sql, native).await).await
    }
    // `vector(time())` is the shape the pre-epoch grid is read on: it reads no data, so its rows are
    // the subquery's child grid, and every one of them is before the epoch. Only the native route
    // can be asked for it — the existing planner's `vector()` takes a literal only — so this case
    // asserts the answer itself, five negative timestamps 60s apart ending at the instant.
    let pre_epoch_time = "TQL EVAL ('-60', '-60', '1m') vector(time())[5m:1m]";
    let native = run_timestamps(&instance, &observer, pre_epoch_time, true).await;
    assert!(
        observer.fused().is_some(),
        "pre-epoch subquery root did not run natively: {pre_epoch_time}"
    );
    assert_eq!(
        native,
        [
            (-300_000, -300.0),
            (-240_000, -240.0),
            (-180_000, -180.0),
            (-120_000, -120.0),
            (-60_000, -60.0),
        ],
        "the pre-epoch grid keeps its negative milliseconds"
    );

    // A constant is the same pre-epoch statement on a shape both routes plan, so the answer can be
    // compared: the native grid has to be the one the existing planner works on.
    let pre_epoch_constant = "TQL EVAL ('-60', '-60', '1m') vector(1)[5m:1m]";
    let native = run_timestamps(&instance, &observer, pre_epoch_constant, true).await;
    assert!(
        observer.fused().is_some(),
        "pre-epoch constant subquery root did not run natively: {pre_epoch_constant}"
    );
    assert_eq!(
        native,
        [
            (-300_000, 1.0),
            (-240_000, 1.0),
            (-180_000, 1.0),
            (-120_000, 1.0),
            (-60_000, 1.0),
        ],
        "the pre-epoch constant grid keeps its negative milliseconds"
    );
    let legacy = run_timestamps(&instance, &observer, pre_epoch_constant, false).await;
    assert_eq!(native, legacy, "pre-epoch subquery root");

    // The same pre-epoch statement anchored at 420s: the anchor decides the child grid.
    let pre_epoch_anchored = "TQL EVAL ('-60', '-60', '1m') vector(1)[5m:1m] @ 420";
    let native = run_timestamps(&instance, &observer, pre_epoch_anchored, true).await;
    assert!(
        observer.fused().is_some(),
        "anchored pre-epoch subquery root did not run natively: {pre_epoch_anchored}"
    );
    let legacy = run_timestamps(&instance, &observer, pre_epoch_anchored, false).await;
    assert_eq!(native, legacy, "anchored pre-epoch subquery root");

    // The bounds route the statement, not the root shape: this one has no subquery and no matrix
    // root at all, a plain aggregate over a selector, and its pre-epoch start still takes the
    // native route. It is kept apart from the matrix-root cases because the bound alone is what
    // puts it there. The fixture starts at the epoch, so of the five 30s steps over the statement's
    // own range only the three from the epoch on have anything inside the 5m lookback: the four
    // hosts carry `host * 1000 + minute`, so the epoch step and the one 30s later both read minute
    // 0 (0 + 1000 + 2000 + 3000) and the step at 60s reads minute 1 (6004).
    let pre_epoch_aggregate = "TQL EVAL ('-60', '60', '30s') sum(native_matrix_root)";
    let native = run_timestamps(&instance, &observer, pre_epoch_aggregate, true).await;
    assert!(
        observer.fused().is_some(),
        "pre-epoch statement without a matrix root did not run natively: {pre_epoch_aggregate}"
    );
    let legacy = run_timestamps(&instance, &observer, pre_epoch_aggregate, false).await;
    assert_eq!(native, legacy, "pre-epoch aggregate over a selector");
    assert_eq!(
        native,
        [(0, 6000.0), (30_000, 6000.0), (60_000, 6004.0)],
        "the pre-epoch aggregate did not answer on its own 30s grid"
    );

    // A matrix root whose inner expression is folded rather than merely selected reaches the
    // split-aggregate lowering, which builds its own final node: that node has to publish on the
    // grid the tree was compiled on, which is the subquery's child grid. `sum(...)` answers without
    // the tag, so it is compared on its timestamps alone.
    let sum_expr = "sum(native_matrix_root)[5m:1m]";
    let sql = format!("TQL EVAL (600, 600, '1m') {sum_expr}");
    let native = run_timestamps(&instance, &observer, &sql, true).await;
    assert!(
        observer.fused().is_some(),
        "matrix root over an aggregate did not run natively: {sum_expr}"
    );
    let legacy = run_timestamps(&instance, &observer, &sql, false).await;
    assert_eq!(native, legacy, "{sum_expr}");
    assert_eq!(
        native.iter().map(|row| row.0).collect::<Vec<_>>(),
        [360_000, 420_000, 480_000, 540_000, 600_000],
        "{sum_expr} did not answer on the subquery's child grid"
    );

    for expr in [
        "(sum by (host) (rate(native_matrix_root[2m])))[5m:1m]",
        "(sum by (host) (rate(native_matrix_root[2m])))[5m:1m] @ 600",
        "(sum by (host) (rate(native_matrix_root[2m])) * 2)[5m:1m]",
    ] {
        let sql = format!("TQL EVAL (600, 600, '1m') {expr}");
        let native = run(&instance, &observer, &sql, true).await;
        assert!(
            observer.fused().is_some(),
            "matrix root over a folded inner expression did not run natively: {expr}"
        );
        let legacy = run(&instance, &observer, &sql, false).await;
        assert_eq!(native, legacy, "{expr}");
        assert!(!native.is_empty(), "{expr} produced no rows");
    }

    let range_query = "TQL EVAL (600, 1200, '1m') native_matrix_root[5m:1m]";
    let err = instance
        .do_query(range_query, native_query_context(true, 4))
        .await
        .remove(0)
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("invalid expression type \"range vector\" for range query"),
        "{err:?}"
    );

    // This remains on the existing planner: a raw-sample matrix has no evaluation grid to bind.
    let sql = "TQL EVAL (600, 600, '1m') native_matrix_root[5m]";
    let native = run(&instance, &observer, sql, true).await;
    assert!(
        observer.fused().is_none(),
        "raw matrix roots must use existing planning"
    );
    let legacy = run(&instance, &observer, sql, false).await;
    assert_eq!(native, legacy);
}
