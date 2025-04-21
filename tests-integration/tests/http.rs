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

use std::collections::BTreeMap;
use std::io::Write;
use std::str::FromStr;

use api::prom_store::remote::WriteRequest;
use auth::user_provider_from_option;
use axum::http::{HeaderName, HeaderValue, StatusCode};
use chrono::Utc;
use common_catalog::consts::{trace_services_table_name, TRACE_TABLE_NAME};
use common_error::status_code::StatusCode as ErrorCode;
use flate2::write::GzEncoder;
use flate2::Compression;
use log_query::{Context, Limit, LogQuery, TimeFilter};
use loki_proto::logproto::{EntryAdapter, LabelPairAdapter, PushRequest, StreamAdapter};
use loki_proto::prost_types::Timestamp;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use pipeline::GREPTIME_INTERNAL_TRACE_PIPELINE_V1_NAME;
use prost::Message;
use serde_json::{json, Value};
use servers::http::handler::HealthResponse;
use servers::http::header::constants::GREPTIME_LOG_TABLE_NAME_HEADER_NAME;
use servers::http::header::{GREPTIME_DB_HEADER_NAME, GREPTIME_TIMEZONE_HEADER_NAME};
use servers::http::jaeger::JAEGER_TIME_RANGE_FOR_OPERATIONS_HEADER;
use servers::http::prometheus::{PrometheusJsonResponse, PrometheusResponse};
use servers::http::result::error_result::ErrorResponse;
use servers::http::result::greptime_result_v1::GreptimedbV1Response;
use servers::http::result::influxdb_result_v1::{InfluxdbOutput, InfluxdbV1Response};
use servers::http::test_helpers::{TestClient, TestResponse};
use servers::http::GreptimeQueryOutput;
use servers::prom_store;
use table::table_name::TableName;
use tests_integration::test_util::{
    setup_test_http_app, setup_test_http_app_with_frontend,
    setup_test_http_app_with_frontend_and_user_provider, setup_test_prom_app_with_frontend,
    StorageType,
};
use yaml_rust::YamlLoader;

#[macro_export]
macro_rules! http_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<integration_http_ $service:lower _test>] {
                $(
                    #[tokio::test(flavor = "multi_thread")]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() {
                        let store_type = tests_integration::test_util::StorageType::$service;
                        if store_type.test_on() {
                            let _ = $crate::http::$test(store_type).await;
                        }
                    }
                )*
            }
        }
    };
}

#[macro_export]
macro_rules! http_tests {
     ($($service:ident),*) => {
        $(
            http_test!(
                $service,

                test_http_auth,
                test_sql_api,
                test_prometheus_promql_api,
                test_prom_http_api,
                test_metrics_api,
                test_health_api,
                test_status_api,
                test_config_api,
                test_dashboard_path,
                test_prometheus_remote_write,
                test_vm_proto_remote_write,

                test_pipeline_api,
                test_test_pipeline_api,
                test_plain_text_ingestion,
                test_identity_pipeline,
                test_identity_pipeline_with_flatten,
                test_identity_pipeline_with_custom_ts,
                test_pipeline_dispatcher,
                test_pipeline_suffix_template,

                test_otlp_metrics,
                test_otlp_traces_v0,
                test_otlp_traces_v1,
                test_otlp_logs,
                test_loki_pb_logs,
                test_loki_json_logs,
                test_elasticsearch_logs,
                test_elasticsearch_logs_with_index,
                test_log_query,
                test_jaeger_query_api,
                test_jaeger_query_api_for_trace_v1,
            );
        )*
    };
}

pub async fn test_http_auth(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();

    let user_provider = user_provider_from_option(
        &"static_user_provider:cmd:greptime_user=greptime_pwd".to_string(),
    )
    .unwrap();

    let (app, mut guard) = setup_test_http_app_with_frontend_and_user_provider(
        store_type,
        "sql_api",
        Some(user_provider),
    )
    .await;
    let client = TestClient::new(app).await;

    // 1. no auth
    let res = client
        .get("/v1/sql?db=public&sql=show tables;")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 2. wrong auth
    let res = client
        .get("/v1/sql?db=public&sql=show tables;")
        .header("Authorization", "basic Z3JlcHRpbWVfdXNlcjp3cm9uZ19wd2Q=")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 3. right auth
    let res = client
        .get("/v1/sql?db=public&sql=show tables;")
        .header(
            "Authorization",
            "basic Z3JlcHRpbWVfdXNlcjpncmVwdGltZV9wd2Q=",
        )
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    guard.remove_all().await;
}

#[tokio::test]
pub async fn test_cors() {
    let (app, mut guard) = setup_test_http_app_with_frontend(StorageType::File, "test_cors").await;
    let client = TestClient::new(app).await;

    let res = client.get("/health").send().await;

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()
            .get(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .expect("expect cors header origin"),
        "*"
    );

    let res = client
        .options("/health")
        .header("Access-Control-Request-Headers", "x-greptime-auth")
        .header("Access-Control-Request-Method", "DELETE")
        .header("Origin", "https://example.com")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()
            .get(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .expect("expect cors header origin"),
        "*"
    );
    assert_eq!(
        res.headers()
            .get(http::header::ACCESS_CONTROL_ALLOW_HEADERS)
            .expect("expect cors header headers"),
        "*"
    );
    assert_eq!(
        res.headers()
            .get(http::header::ACCESS_CONTROL_ALLOW_METHODS)
            .expect("expect cors header methods"),
        "GET,POST,PUT,DELETE,HEAD"
    );

    guard.remove_all().await;
}

pub async fn test_sql_api(store_type: StorageType) {
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "sql_api").await;
    let client = TestClient::new(app).await;
    let res = client.get("/v1/sql").send().await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    let body = serde_json::from_str::<ErrorResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), 1004);
    assert_eq!(body.error(), "sql parameter is required.");

    let res = client
        .get("/v1/sql?sql=select * from numbers limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    let output = body.output();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        serde_json::from_value::<GreptimeQueryOutput>(json!({
            "records" :{"schema":{"column_schemas":[{"name":"number","data_type":"UInt32"}]},"rows":[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]],"total_rows":10}
        })).unwrap()
    );

    // test influxdb_v1 result format
    let res = client
        .get("/v1/sql?format=influxdb_v1&sql=select * from numbers limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<InfluxdbV1Response>(&res.text().await).unwrap();
    let output = body.results();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        serde_json::from_value::<InfluxdbOutput>(json!({
            "statement_id":0,"series":[{"name":"","columns":["number"],"values":[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]}]
        })).unwrap()
    );

    // test json result format
    let res = client
        .get("/v1/sql?format=json&sql=select * from numbers limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.json::<Value>().await;
    let data = body.get("data").expect("Missing 'data' field in response");

    let expected = json!([
        {"number": 0}, {"number": 1}, {"number": 2}, {"number": 3}, {"number": 4},
        {"number": 5}, {"number": 6}, {"number": 7}, {"number": 8}, {"number": 9}
    ]);
    assert_eq!(data, &expected);

    // test insert and select
    let res = client
        .get("/v1/sql?sql=insert into demo values('host', 66.6, 1024, 0)")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // select *
    let res = client
        .get("/v1/sql?sql=select * from demo limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    let output = body.output();
    assert_eq!(output.len(), 1);

    assert_eq!(
        output[0],
        serde_json::from_value::<GreptimeQueryOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"host","data_type":"String"},{"name":"cpu","data_type":"Float64"},{"name":"memory","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[["host",66.6,1024.0,0]],"total_rows":1}
        })).unwrap()
    );

    // select with projections
    let res = client
        .get("/v1/sql?sql=select cpu, ts from demo limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    let output = body.output();
    assert_eq!(output.len(), 1);

    assert_eq!(
        output[0],
        serde_json::from_value::<GreptimeQueryOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]],"total_rows":1}
        })).unwrap()
    );

    // select with column alias
    let res = client
        .get("/v1/sql?sql=select cpu as c, ts as time from demo limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    let output = body.output();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        serde_json::from_value::<GreptimeQueryOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"c","data_type":"Float64"},{"name":"time","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]],"total_rows":1}
        })).unwrap()
    );

    // test multi-statement
    let res = client
        .get("/v1/sql?sql=select cpu, ts from demo limit 1;select cpu, ts from demo where ts > 0;")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    let outputs = body.output();
    assert_eq!(outputs.len(), 2);
    assert_eq!(
        outputs[0],
        serde_json::from_value::<GreptimeQueryOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]],"total_rows":1}
        })).unwrap()
    );
    assert_eq!(
        outputs[1],
        serde_json::from_value::<GreptimeQueryOutput>(json!({
            "records":{"rows":[], "schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]}, "total_rows":0}
        }))
        .unwrap()
    );

    // test multi-statement with error
    let res = client
        .get("/v1/sql?sql=select cpu, ts from demo limit 1;select cpu, ts from demo2 where ts > 0;")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    let body = serde_json::from_str::<ErrorResponse>(&res.text().await).unwrap();
    assert!(body.error().contains("Table not found"));
    assert_eq!(body.code(), ErrorCode::TableNotFound as u32);

    // test database given
    let res = client
        .get("/v1/sql?db=public&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    let outputs = body.output();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0],
        serde_json::from_value::<GreptimeQueryOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]],"total_rows":1}
        })).unwrap()
    );

    // test database not found
    let res = client
        .get("/v1/sql?db=notfound&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = serde_json::from_str::<ErrorResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), ErrorCode::DatabaseNotFound as u32);

    // test catalog-schema given
    let res = client
        .get("/v1/sql?db=greptime-public&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    let outputs = body.output();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0],
        serde_json::from_value::<GreptimeQueryOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]],"total_rows":1}
        })).unwrap()
    );

    // test invalid catalog
    let res = client
        .get("/v1/sql?db=notfound2-schema&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = serde_json::from_str::<ErrorResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), ErrorCode::DatabaseNotFound as u32);

    // test invalid schema
    let res = client
        .get("/v1/sql?db=greptime-schema&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = serde_json::from_str::<ErrorResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), ErrorCode::DatabaseNotFound as u32);

    // test analyze format
    let res = client
        .get("/v1/sql?sql=explain analyze format json select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    let output = body.output();
    assert_eq!(output.len(), 1);
    // this is something only json format can show
    assert!(format!("{:?}", output[0]).contains("\\\"param\\\""));

    // test parse method
    let res = client.get("/v1/sql/parse?sql=desc table t").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.text().await,
        r#"[{"DescribeTable":{"name":[{"value":"t","quote_style":null,"span":{"start":{"line":0,"column":0},"end":{"line":0,"column":0}}}]}}]"#,
    );

    // test timezone header
    let res = client
        .get("/v1/sql?&sql=show variables system_time_zone")
        .header(
            TryInto::<HeaderName>::try_into(GREPTIME_TIMEZONE_HEADER_NAME.to_string()).unwrap(),
            "Asia/Shanghai",
        )
        .send()
        .await
        .text()
        .await;
    assert!(res.contains("SYSTEM_TIME_ZONE") && res.contains("UTC"));
    let res = client
        .get("/v1/sql?&sql=show variables time_zone")
        .header(
            TryInto::<HeaderName>::try_into(GREPTIME_TIMEZONE_HEADER_NAME.to_string()).unwrap(),
            "Asia/Shanghai",
        )
        .send()
        .await
        .text()
        .await;
    assert!(res.contains("TIME_ZONE") && res.contains("Asia/Shanghai"));
    let res = client
        .get("/v1/sql?&sql=show variables system_time_zone")
        .send()
        .await
        .text()
        .await;
    assert!(res.contains("SYSTEM_TIME_ZONE") && res.contains("UTC"));
    let res = client
        .get("/v1/sql?&sql=show variables time_zone")
        .send()
        .await
        .text()
        .await;
    assert!(res.contains("TIME_ZONE") && res.contains("UTC"));
    guard.remove_all().await;
}

pub async fn test_prometheus_promql_api(store_type: StorageType) {
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "sql_api").await;
    let client = TestClient::new(app).await;

    let res = client
        .get("/v1/promql?query=abs(demo{host=\"Hangzhou\"})&start=0&end=100&step=5s")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let _body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    guard.remove_all().await;
}

pub async fn test_prom_http_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_prom_app_with_frontend(store_type, "promql_api").await;
    let client = TestClient::new(app).await;

    // format_query
    let res = client
        .get("/v1/prometheus/api/v1/format_query?query=foo/bar")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.text().await.as_str(),
        r#"{"status":"success","data":"foo / bar"}"#
    );

    // instant query
    let res = client
        .get("/v1/prometheus/api/v1/query?query=up&time=1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .post("/v1/prometheus/api/v1/query?query=up&time=1")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // instant query 1+1
    let res = client
        .get("/v1/prometheus/api/v1/query?query=1%2B1&time=1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(
            json!({"resultType":"scalar","result":[1.0,"2"]})
        )
        .unwrap()
    );

    // range query
    let res = client
        .get("/v1/prometheus/api/v1/query_range?query=up&start=1&end=100&step=5")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .post("/v1/prometheus/api/v1/query_range?query=up&start=1&end=100&step=5")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .post("/v1/prometheus/api/v1/query_range?query=count(count(up))&start=1&end=100&step=5")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // labels
    let res = client
        .get("/v1/prometheus/api/v1/labels?match[]=demo")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .post("/v1/prometheus/api/v1/labels?match[]=up")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .get("/v1/prometheus/api/v1/labels?match[]=demo&start=0")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!(["__name__", "host",])).unwrap()
    );

    // labels without match[] param
    let res = client.get("/v1/prometheus/api/v1/labels").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!(["__name__", "host", "idc", "number",]))
            .unwrap()
    );

    // labels query with multiple match[] params
    let res = client
        .get("/v1/prometheus/api/v1/labels?match[]=up&match[]=down")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .post("/v1/prometheus/api/v1/labels?match[]=up&match[]=down")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // series
    let res = client
        .get("/v1/prometheus/api/v1/series?match[]=demo{}&start=0&end=0")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");

    let PrometheusResponse::Series(mut series) = body.data else {
        unreachable!()
    };
    let actual = series
        .remove(0)
        .into_iter()
        .collect::<BTreeMap<String, String>>();
    let expected = BTreeMap::from([
        ("__name__".to_string(), "demo".to_string()),
        ("host".to_string(), "host1".to_string()),
    ]);
    assert_eq!(actual, expected);

    let res = client
        .post("/v1/prometheus/api/v1/series?match[]=up&match[]=down")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let prom_resp = res.json::<PrometheusJsonResponse>().await;
    assert_eq!(prom_resp.status, "error");
    assert!(prom_resp
        .error_type
        .is_some_and(|err| err.eq_ignore_ascii_case("TableNotFound")));
    assert!(prom_resp
        .error
        .is_some_and(|err| err.eq_ignore_ascii_case("Table not found: greptime.public.up")));

    // label values
    // should return error if there is no match[]
    let res = client
        .get("/v1/prometheus/api/v1/label/instance/values")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let prom_resp = res.json::<PrometheusJsonResponse>().await;
    assert_eq!(prom_resp.status, "error");
    assert!(prom_resp
        .error
        .is_some_and(|err| err.eq_ignore_ascii_case("match[] parameter is required")));
    assert!(prom_resp
        .error_type
        .is_some_and(|err| err.eq_ignore_ascii_case("InvalidArguments")));

    // single match[]
    let res = client
        .get("/v1/prometheus/api/v1/label/host/values?match[]=demo&start=0&end=600")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!(["host1", "host2"])).unwrap()
    );

    // single match[]
    let res = client
        .get("/v1/prometheus/api/v1/label/host/values?match[]=demo&start=0&end=300")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!(["host1"])).unwrap()
    );

    // single match[] with __name__
    let res = client
        .get("/v1/prometheus/api/v1/label/host/values?match[]={__name__%3D%22demo%22}&start=0&end=300")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!(["host1"])).unwrap()
    );

    // single match[]
    let res = client
        .get("/v1/prometheus/api/v1/label/idc/values?match[]=demo_metrics_with_nanos&start=0&end=600")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!(["idc1"])).unwrap()
    );

    // search field name
    let res = client
        .get("/v1/prometheus/api/v1/label/__field__/values?match[]=demo")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!(["val"])).unwrap()
    );

    // query an empty database should return nothing
    let res = client
        .get("/v1/prometheus/api/v1/label/host/values?match[]=demo&start=0&end=600")
        .header(GREPTIME_DB_HEADER_NAME.clone(), "nonexistent")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!([])).unwrap()
    );

    // db header will be overrode by `db` in param
    let res = client
        .get("/v1/prometheus/api/v1/label/host/values?match[]=demo&start=0&end=600&db=public")
        .header(GREPTIME_DB_HEADER_NAME.clone(), "nonexistent")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!(["host1", "host2"])).unwrap()
    );

    // multiple match[]
    let res = client
        .get("/v1/prometheus/api/v1/label/instance/values?match[]=up&match[]=system_metrics")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let prom_resp = res.json::<PrometheusJsonResponse>().await;
    assert_eq!(prom_resp.status, "success");
    assert!(prom_resp.error.is_none());
    assert!(prom_resp.error_type.is_none());

    // query non-string value
    let res = client
        .get("/v1/prometheus/api/v1/label/host/values?match[]=mito")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // query `__name__` without match[]
    // create a physical table and a logical table
    let res = client
        .get("/v1/sql?sql=create table physical_table (`ts` timestamp time index, message string) with ('physical_metric_table' = 'true');")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK, "{:?}", res.text().await);
    let res = client
        .get("/v1/sql?sql=create table logic_table (`ts` timestamp time index, message string) with ('on_physical_table' = 'physical_table');")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK, "{:?}", res.text().await);
    // query `__name__`
    let res = client
        .get("/v1/prometheus/api/v1/label/__name__/values")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let prom_resp = res.json::<PrometheusJsonResponse>().await;
    assert_eq!(prom_resp.status, "success");
    assert!(prom_resp.error.is_none());
    assert!(prom_resp.error_type.is_none());
    assert_eq!(
        prom_resp.data,
        PrometheusResponse::Labels(vec![
            "demo".to_string(),
            "demo_metrics".to_string(),
            "demo_metrics_with_nanos".to_string(),
            "logic_table".to_string(),
            "mito".to_string(),
            "numbers".to_string()
        ])
    );

    // buildinfo
    let res = client
        .get("/v1/prometheus/api/v1/status/buildinfo")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");

    // parse_query
    let res = client
        .get("/v1/prometheus/api/v1/parse_query?query=http_requests")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let data = res.text().await;
    // we don't have deserialization for ast so we keep test simple and compare
    //  the json output directly.
    // the correctness should be covered by parser. In this test we only check
    //  response format.
    let expected = "{\"status\":\"success\",\"data\":{\"type\":\"vectorSelector\",\"name\":\"http_requests\",\"matchers\":[],\"offset\":0,\"startOrEnd\":null,\"timestamp\":null}}";
    assert_eq!(expected, data);

    let res = client
        .get("/v1/prometheus/api/v1/parse_query?query=not http_requests")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let data = res.text().await;
    let expected = "{\"status\":\"error\",\"error\":\"invalid promql query\",\"errorType\":\"InvalidArguments\"}";
    assert_eq!(expected, data);

    // range_query with __name__ not-equal matcher
    let res = client
        .post("/v1/prometheus/api/v1/query_range?query=count by(__name__)({__name__=~'demo.*'})&start=1&end=100&step=5")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let data = res.text().await;
    assert!(
        data.contains("{\"__name__\":\"demo_metrics\"}")
            && data.contains("{\"__name__\":\"demo\"}")
    );

    let res = client
        .post("/v1/prometheus/api/v1/query_range?query=count by(__name__)({__name__=~'demo_metrics'})&start=1&end=100&step=5")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let data = res.text().await;
    assert!(
        data.contains("{\"__name__\":\"demo_metrics\"}")
            && !data.contains("{\"__name__\":\"demo\"}")
    );

    guard.remove_all().await;
}

pub async fn test_metrics_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app(store_type, "metrics_api").await;
    let client = TestClient::new(app).await;

    // Send a sql
    let res = client
        .get("/v1/sql?sql=select * from numbers limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // Call metrics api
    let res = client.get("/metrics").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await;
    // Comment in the metrics text.
    assert!(body.contains("# HELP"));
    guard.remove_all().await;
}

pub async fn test_health_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, _guard) = setup_test_http_app_with_frontend(store_type, "health_api").await;
    let client = TestClient::new(app).await;

    async fn health_api(client: &TestClient, endpoint: &str) {
        // we can call health api with both `GET` and `POST` method.
        let res_post = client.post(endpoint).send().await;
        assert_eq!(res_post.status(), StatusCode::OK);
        let res_get = client.get(endpoint).send().await;
        assert_eq!(res_get.status(), StatusCode::OK);

        // both `GET` and `POST` method return same result
        let body_text = res_post.text().await;
        assert_eq!(body_text, res_get.text().await);

        // currently health api simply returns an empty json `{}`, which can be deserialized to an empty `HealthResponse`
        assert_eq!(body_text, "{}");

        let body = serde_json::from_str::<HealthResponse>(&body_text).unwrap();
        assert_eq!(body, HealthResponse {});
    }

    health_api(&client, "/health").await;
    health_api(&client, "/ready").await;
}

pub async fn test_status_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, _guard) = setup_test_http_app_with_frontend(store_type, "status_api").await;
    let client = TestClient::new(app).await;

    let res_get = client.get("/status").send().await;
    assert_eq!(res_get.status(), StatusCode::OK);

    let res_body = res_get.text().await;
    assert!(res_body.contains("{\"source_time\""));
    assert!(res_body.contains("\"commit\":"));
    assert!(res_body.contains("\"branch\":"));
    assert!(res_body.contains("\"rustc_version\":"));
    assert!(res_body.contains("\"hostname\":"));
    assert!(res_body.contains("\"version\":"));
}

pub async fn test_config_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, _guard) = setup_test_http_app_with_frontend(store_type, "config_api").await;
    let client = TestClient::new(app).await;

    let res_get = client.get("/config").send().await;
    assert_eq!(res_get.status(), StatusCode::OK);

    let storage = if store_type != StorageType::File {
        format!(
            r#"[storage]
type = "{}"
providers = []

[storage.http_client]
pool_max_idle_per_host = 1024
connect_timeout = "30s"
timeout = "30s"
pool_idle_timeout = "1m 30s""#,
            store_type
        )
    } else {
        format!(
            r#"[storage]
type = "{}"
providers = []"#,
            store_type
        )
    };

    let expected_toml_str = format!(
        r#"
enable_telemetry = true
init_regions_in_background = false
init_regions_parallelism = 16

[http]
addr = "127.0.0.1:4000"
timeout = "0s"
body_limit = "64MiB"
is_strict_mode = false
cors_allowed_origins = []
enable_cors = true

[grpc]
bind_addr = "127.0.0.1:4001"
server_addr = "127.0.0.1:4001"
max_recv_message_size = "512MiB"
max_send_message_size = "512MiB"
runtime_size = 8

[grpc.tls]
mode = "disable"
cert_path = ""
key_path = ""
watch = false

[mysql]
enable = true
addr = "127.0.0.1:4002"
runtime_size = 2
keep_alive = "0s"

[mysql.tls]
mode = "disable"
cert_path = ""
key_path = ""
watch = false

[postgres]
enable = true
addr = "127.0.0.1:4003"
runtime_size = 2
keep_alive = "0s"

[postgres.tls]
mode = "disable"
cert_path = ""
key_path = ""
watch = false

[opentsdb]
enable = true

[influxdb]
enable = true

[jaeger]
enable = true

[prom_store]
enable = true
with_metric_engine = true

[wal]
provider = "raft_engine"
file_size = "128MiB"
purge_threshold = "1GiB"
purge_interval = "1m"
read_batch_size = 128
sync_write = false
enable_log_recycle = true
prefill_log_files = false

{storage}

[metadata_store]
file_size = "64MiB"
purge_threshold = "256MiB"
purge_interval = "1m"

[procedure]
max_retry_times = 3
retry_delay = "500ms"
max_running_procedures = 128

[flow]

[logging]
max_log_files = 720
append_stdout = true
enable_otlp_tracing = false

[logging.slow_query]
enable = false

[[region_engine]]

[region_engine.mito]
worker_channel_size = 128
worker_request_batch_size = 64
manifest_checkpoint_distance = 10
compress_manifest = false
auto_flush_interval = "30m"
enable_write_cache = false
write_cache_path = ""
write_cache_size = "5GiB"
sst_write_buffer_size = "8MiB"
parallel_scan_channel_size = 32
allow_stale_entries = false
min_compaction_interval = "0s"

[region_engine.mito.index]
aux_path = ""
staging_size = "2GiB"
staging_ttl = "7days"
write_buffer_size = "8MiB"
content_cache_page_size = "64KiB"

[region_engine.mito.inverted_index]
create_on_flush = "auto"
create_on_compaction = "auto"
apply_on_query = "auto"
mem_threshold_on_create = "auto"

[region_engine.mito.fulltext_index]
create_on_flush = "auto"
create_on_compaction = "auto"
apply_on_query = "auto"
mem_threshold_on_create = "auto"
compress = true

[region_engine.mito.bloom_filter_index]
create_on_flush = "auto"
create_on_compaction = "auto"
apply_on_query = "auto"
mem_threshold_on_create = "auto"

[region_engine.mito.memtable]
type = "time_series"

[[region_engine]]

[region_engine.file]

[export_metrics]
enable = false
write_interval = "30s"

[tracing]"#,
    )
    .trim()
    .to_string();
    let body_text = drop_lines_with_inconsistent_results(res_get.text().await);
    similar_asserts::assert_eq!(expected_toml_str, body_text);
}

fn drop_lines_with_inconsistent_results(input: String) -> String {
    let inconsistent_results = [
        "dir =",
        "log_format =",
        "data_home =",
        "bucket =",
        "root =",
        "endpoint =",
        "region =",
        "enable_virtual_host_style =",
        "cache_path =",
        "cache_capacity =",
        "sas_token =",
        "scope =",
        "num_workers =",
        "scan_parallelism =",
        "global_write_buffer_size =",
        "global_write_buffer_reject_size =",
        "sst_meta_cache_size =",
        "vector_cache_size =",
        "page_cache_size =",
        "selector_result_cache_size =",
        "metadata_cache_size =",
        "content_cache_size =",
        "name =",
        "recovery_parallelism =",
        "max_background_flushes =",
        "max_background_compactions =",
        "max_background_purges =",
    ];

    input
        .lines()
        .filter(|line| {
            // ignores
            let line = line.trim();
            for prefix in inconsistent_results {
                if line.starts_with(prefix) {
                    return false;
                }
            }
            true
        })
        .collect::<Vec<&str>>()
        .join(
            "
",
        )
}

#[cfg(feature = "dashboard")]
pub async fn test_dashboard_path(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, _guard) = setup_test_http_app_with_frontend(store_type, "dashboard_path").await;
    let client = TestClient::new(app).await;

    let res_get = client.get("/dashboard").send().await;
    assert_eq!(res_get.status(), StatusCode::PERMANENT_REDIRECT);
    let res_post = client.post("/dashboard/").send().await;
    assert_eq!(res_post.status(), StatusCode::OK);
    let res_get = client.get("/dashboard/").send().await;
    assert_eq!(res_get.status(), StatusCode::OK);

    // both `GET` and `POST` method return same result
    let body_text = res_post.text().await;
    assert_eq!(body_text, res_get.text().await);
}

#[cfg(not(feature = "dashboard"))]
pub async fn test_dashboard_path(_: StorageType) {}

pub async fn test_prometheus_remote_write(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_prom_app_with_frontend(store_type, "prometheus_remote_write").await;
    let client = TestClient::new(app).await;

    // write snappy encoded data
    let write_request = WriteRequest {
        timeseries: prom_store::mock_timeseries(),
        ..Default::default()
    };
    let serialized_request = write_request.encode_to_vec();
    let compressed_request =
        prom_store::snappy_compress(&serialized_request).expect("failed to encode snappy");

    let res = client
        .post("/v1/prometheus/write")
        .header("Content-Encoding", "snappy")
        .body(compressed_request)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    guard.remove_all().await;
}

pub async fn test_vm_proto_remote_write(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_prom_app_with_frontend(store_type, "vm_proto_remote_write").await;

    // handshake
    let client = TestClient::new(app).await;
    let res = client
        .post("/v1/prometheus/write?get_vm_proto_version=1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await, "1");

    // write zstd encoded data
    let write_request = WriteRequest {
        timeseries: prom_store::mock_timeseries(),
        ..Default::default()
    };
    let serialized_request = write_request.encode_to_vec();
    let compressed_request =
        zstd::stream::encode_all(&serialized_request[..], 1).expect("Failed to encode zstd");

    let res = client
        .post("/v1/prometheus/write")
        .header("Content-Encoding", "zstd")
        .body(compressed_request)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // also test fallback logic, vmagent could sent data in wrong content-type
    // we encode it as zstd but send it as snappy
    let compressed_request =
        zstd::stream::encode_all(&serialized_request[..], 1).expect("Failed to encode zstd");

    let res = client
        .post("/v1/prometheus/write")
        .header("Content-Encoding", "snappy")
        .body(compressed_request)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // reversed
    let compressed_request =
        prom_store::snappy_compress(&serialized_request[..]).expect("Failed to encode snappy");

    let res = client
        .post("/v1/prometheus/write")
        .header("Content-Encoding", "zstd")
        .body(compressed_request)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    guard.remove_all().await;
}

pub async fn test_pipeline_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_pipeline_api").await;

    // handshake
    let client = TestClient::new(app).await;

    let body = r#"
processors:
  - date:
      field: time
      formats:
        - "%Y-%m-%d %H:%M:%S%.3f"
      ignore_missing: true

transform:
  - fields:
      - id1
      - id2
    type: int32
    index: inverted
  - fields:
      - logger
    type: string
  - field: type
    type: string
    index: skipping
    tag: true
  - field: log
    type: string
    index: fulltext
    tag: true
  - field: time
    type: time
    index: timestamp
"#;

    // 1. create pipeline
    let res = client
        .post("/v1/pipelines/greptime_guagua")
        .header("Content-Type", "application/x-yaml")
        .body(body)
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        res.json::<serde_json::Value>().await["error"]
            .as_str()
            .unwrap(),
        "Invalid request parameter: pipeline_name cannot start with greptime_"
    );

    let res = client
        .post("/v1/pipelines/test")
        .header("Content-Type", "application/x-yaml")
        .body(body)
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::OK);

    let content = res.text().await;

    let content = serde_json::from_str(&content);
    assert!(content.is_ok());
    //  {"execution_time_ms":13,"pipelines":[{"name":"test","version":"2024-07-04 08:31:00.987136"}]}
    let content: Value = content.unwrap();

    let execution_time = content.get("execution_time_ms");
    assert!(execution_time.unwrap().is_number());
    let pipelines = content.get("pipelines");
    let pipelines = pipelines.unwrap().as_array().unwrap();
    assert_eq!(pipelines.len(), 1);
    let pipeline = pipelines.first().unwrap();
    assert_eq!(pipeline.get("name").unwrap(), "test");

    let version_str = pipeline
        .get("version")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let encoded_ver_str: String =
        url::form_urlencoded::byte_serialize(version_str.as_bytes()).collect();

    // get pipeline
    let res = client
        .get(format!("/v1/pipelines/test?version={}", encoded_ver_str).as_str())
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::OK);

    let content = res.text().await;
    let content = serde_json::from_str(&content);
    let content: Value = content.unwrap();
    let pipeline_yaml = content
        .get("pipelines")
        .unwrap()
        .as_array()
        .unwrap()
        .first()
        .unwrap()
        .get("pipeline")
        .unwrap()
        .as_str()
        .unwrap();
    let docs = YamlLoader::load_from_str(pipeline_yaml).unwrap();
    let body_yaml = YamlLoader::load_from_str(body).unwrap();
    assert_eq!(docs, body_yaml);

    // Do not specify version, get the latest version
    let res = client.get("/v1/pipelines/test").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    let content = res.text().await;
    let content = serde_json::from_str(&content);
    let content: Value = content.unwrap();
    let pipeline_yaml = content
        .get("pipelines")
        .unwrap()
        .as_array()
        .unwrap()
        .first()
        .unwrap()
        .get("pipeline")
        .unwrap()
        .as_str()
        .unwrap();
    let docs = YamlLoader::load_from_str(pipeline_yaml).unwrap();
    assert_eq!(docs, body_yaml);

    // 2. write data
    let data_body = r#"
[
  {
    "id1": "2436",
    "id2": "2528",
    "logger": "INTERACT.MANAGER",
    "type": "I",
    "time": "2024-05-25 20:16:37.217",
    "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
  }
]
"#;
    let res = client
        .post("/v1/ingest?db=public&table=logs1&pipeline_name=test")
        .header("Content-Type", "application/json")
        .body(data_body)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // 3. check schema
    let expected_schema = "[[\"logs1\",\"CREATE TABLE IF NOT EXISTS \\\"logs1\\\" (\\n  \\\"id1\\\" INT NULL INVERTED INDEX,\\n  \\\"id2\\\" INT NULL INVERTED INDEX,\\n  \\\"logger\\\" STRING NULL,\\n  \\\"type\\\" STRING NULL SKIPPING INDEX WITH(granularity = '10240', type = 'BLOOM'),\\n  \\\"log\\\" STRING NULL FULLTEXT INDEX WITH(analyzer = 'English', backend = 'bloom', case_sensitive = 'false'),\\n  \\\"time\\\" TIMESTAMP(9) NOT NULL,\\n  TIME INDEX (\\\"time\\\"),\\n  PRIMARY KEY (\\\"type\\\", \\\"log\\\")\\n)\\n\\nENGINE=mito\\nWITH(\\n  append_mode = 'true'\\n)\"]]";
    validate_data(
        "pipeline_schema",
        &client,
        "show create table logs1",
        expected_schema,
    )
    .await;

    // 4. remove pipeline
    let encoded_ver_str: String =
        url::form_urlencoded::byte_serialize(version_str.as_bytes()).collect();
    let res = client
        .delete(format!("/v1/pipelines/test?version={}", encoded_ver_str).as_str())
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::OK);

    // {"pipelines":[{"name":"test","version":"2024-07-04 08:55:29.038347"}],"execution_time_ms":22}
    let content = res.text().await;
    let content: Value = serde_json::from_str(&content).unwrap();
    assert!(content.get("execution_time_ms").unwrap().is_number());

    assert_eq!(
        content.get("pipelines").unwrap().to_string(),
        format!(r#"[{{"name":"test","version":"{}"}}]"#, version_str).as_str()
    );

    // 5. write data failed
    let res = client
        .post("/v1/ingest?db=public&table=logs1&pipeline_name=test")
        .header("Content-Type", "application/json")
        .body(data_body)
        .send()
        .await;
    // todo(shuiyisong): refactor http error handling
    assert_ne!(res.status(), StatusCode::OK);

    guard.remove_all().await;
}

pub async fn test_identity_pipeline(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_http_app_with_frontend(store_type, "test_identity_pipeline").await;

    // handshake
    let client = TestClient::new(app).await;
    let body = r#"{"__time__":1453809242,"__topic__":"","__source__":"10.170.***.***","ip":"10.200.**.***","time":"26/Jan/2016:19:54:02 +0800","url":"POST/PutData?Category=YunOsAccountOpLog&AccessKeyId=<yourAccessKeyId>&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=<yourSignature>HTTP/1.1","status":"200","user-agent":"aliyun-sdk-java", "json_object": {"a":1,"b":2}, "json_array":[1,2,3]}
{"__time__":1453809242,"__topic__":"","__source__":"10.170.***.***","ip":"10.200.**.***","time":"26/Jan/2016:19:54:02 +0800","url":"POST/PutData?Category=YunOsAccountOpLog&AccessKeyId=<yourAccessKeyId>&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=<yourSignature>HTTP/1.1","status":"200","user-agent":"aliyun-sdk-java","hasagei":"hasagei","dongdongdong":"guaguagua", "json_object": {"a":1,"b":2}, "json_array":[1,2,3]}"#;
    let res = client
        .post("/v1/ingest?db=public&table=logs&pipeline_name=greptime_identity")
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::OK);

    let body: serde_json::Value = res.json().await;

    assert!(body.get("execution_time_ms").unwrap().is_number());
    assert_eq!(body["output"][0]["affectedrows"], 2);

    let res = client.get("/v1/sql?sql=select * from logs").send().await;

    assert_eq!(res.status(), StatusCode::OK);

    let line1_expected = r#"[null,"10.170.***.***",1453809242,"","10.200.**.***",[1,2,3],{"a":1,"b":2},"200","26/Jan/2016:19:54:02 +0800","POST/PutData?Category=YunOsAccountOpLog&AccessKeyId=<yourAccessKeyId>&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=<yourSignature>HTTP/1.1","aliyun-sdk-java",null,null]"#;
    let line2_expected = r#"[null,"10.170.***.***",1453809242,"","10.200.**.***",[1,2,3],{"a":1,"b":2},"200","26/Jan/2016:19:54:02 +0800","POST/PutData?Category=YunOsAccountOpLog&AccessKeyId=<yourAccessKeyId>&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=<yourSignature>HTTP/1.1","aliyun-sdk-java","guaguagua","hasagei"]"#;
    let res = client.get("/v1/sql?sql=select * from logs").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let resp: serde_json::Value = res.json().await;
    let result = resp["output"][0]["records"]["rows"].as_array().unwrap();
    assert_eq!(result.len(), 2);
    let mut line1 = result[0].as_array().unwrap().clone();
    let mut line2 = result[1].as_array().unwrap().clone();
    assert!(line1.first().unwrap().is_i64());
    assert!(line2.first().unwrap().is_i64());
    // set time index to null for assertion
    *line1.first_mut().unwrap() = serde_json::Value::Null;
    *line2.first_mut().unwrap() = serde_json::Value::Null;

    assert_eq!(
        line1,
        serde_json::from_str::<Vec<Value>>(line1_expected).unwrap()
    );
    assert_eq!(
        line2,
        serde_json::from_str::<Vec<Value>>(line2_expected).unwrap()
    );

    let expected = r#"[["greptime_timestamp","TimestampNanosecond","PRI","NO","","TIMESTAMP"],["__source__","String","","YES","","FIELD"],["__time__","Int64","","YES","","FIELD"],["__topic__","String","","YES","","FIELD"],["ip","String","","YES","","FIELD"],["json_array","Json","","YES","","FIELD"],["json_object","Json","","YES","","FIELD"],["status","String","","YES","","FIELD"],["time","String","","YES","","FIELD"],["url","String","","YES","","FIELD"],["user-agent","String","","YES","","FIELD"],["dongdongdong","String","","YES","","FIELD"],["hasagei","String","","YES","","FIELD"]]"#;
    validate_data("identity_schema", &client, "desc logs", expected).await;

    guard.remove_all().await;
}

pub async fn test_pipeline_dispatcher(storage_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_http_app_with_frontend(storage_type, "test_pipeline_dispatcher").await;

    // handshake
    let client = TestClient::new(app).await;

    let root_pipeline = r#"
processors:
  - date:
      field: time
      formats:
        - "%Y-%m-%d %H:%M:%S%.3f"
      ignore_missing: true

dispatcher:
  field: type
  rules:
    - value: http
      table_suffix: http
      pipeline: http
    - value: db
      table_suffix: db
    - value: not_found
      table_suffix: not_found
      pipeline: not_found

transform:
  - fields:
      - id1, id1_root
      - id2, id2_root
    type: int32
  - fields:
      - type
      - log
      - logger
    type: string
  - field: time
    type: time
    index: timestamp
"#;

    let http_pipeline = r#"
processors:

transform:
  - fields:
      - id1, id1_http
      - id2, id2_http
    type: int32
  - fields:
      - log
      - logger
    type: string
  - field: time
    type: time
    index: timestamp
"#;

    // 1. create pipeline
    let res = client
        .post("/v1/events/pipelines/root")
        .header("Content-Type", "application/x-yaml")
        .body(root_pipeline)
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::OK);

    let res = client
        .post("/v1/events/pipelines/http")
        .header("Content-Type", "application/x-yaml")
        .body(http_pipeline)
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::OK);

    // 2. write data
    let data_body = r#"
[
  {
    "id1": "2436",
    "id2": "2528",
    "logger": "INTERACT.MANAGER",
    "type": "http",
    "time": "2024-05-25 20:16:37.217",
    "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
  }
]
"#;
    let res = client
        .post("/v1/events/logs?db=public&table=logs1&pipeline_name=root")
        .header("Content-Type", "application/json")
        .body(data_body)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let data_body = r#"
[
  {
    "id1": "2436",
    "id2": "2528",
    "logger": "INTERACT.MANAGER",
    "type": "db",
    "time": "2024-05-25 20:16:37.217",
    "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
  }
]
"#;
    let res = client
        .post("/v1/events/logs?db=public&table=logs1&pipeline_name=root")
        .header("Content-Type", "application/json")
        .body(data_body)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let data_body = r#"
[
  {
    "id1": "2436",
    "id2": "2528",
    "logger": "INTERACT.MANAGER",
    "type": "api",
    "time": "2024-05-25 20:16:37.217",
    "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
  }
]
"#;
    let res = client
        .post("/v1/events/logs?db=public&table=logs1&pipeline_name=root")
        .header("Content-Type", "application/json")
        .body(data_body)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let data_body = r#"
[
  {
    "id1": "2436",
    "id2": "2528",
    "logger": "INTERACT.MANAGER",
    "type": "not_found",
    "time": "2024-05-25 20:16:37.217",
    "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
  }
]
"#;
    let res = client
        .post("/v1/events/logs?db=public&table=logs1&pipeline_name=root")
        .header("Content-Type", "application/json")
        .body(data_body)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // 3. verify data
    let expected = "[[2436]]";
    validate_data(
        "test_dispatcher_pipeline default table",
        &client,
        "select id1_root from logs1",
        expected,
    )
    .await;

    let expected = "[[2436]]";
    validate_data(
        "test_dispatcher_pipeline http table",
        &client,
        "select id1_http from logs1_http",
        expected,
    )
    .await;

    let expected = "[[\"2436\"]]";
    validate_data(
        "test_dispatcher_pipeline db table",
        &client,
        "select id1 from logs1_db",
        expected,
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_pipeline_suffix_template(storage_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_http_app_with_frontend(storage_type, "test_pipeline_suffix_template").await;

    // handshake
    let client = TestClient::new(app).await;

    let root_pipeline = r#"
processors:
  - date:
      field: time
      formats:
        - "%Y-%m-%d %H:%M:%S%.3f"
      ignore_missing: true

transform:
  - fields:
      - id1, id1_root
      - id2, id2_root
    type: int32
  - fields:
      - type
      - log
      - logger
    type: string
  - field: time
    type: time
    index: timestamp
table_suffix: _${type}
"#;

    // 1. create pipeline
    let res = client
        .post("/v1/events/pipelines/root")
        .header("Content-Type", "application/x-yaml")
        .body(root_pipeline)
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::OK);

    // 2. write data
    let data_body = r#"
[
  {
    "id1": "2436",
    "id2": "2528",
    "logger": "INTERACT.MANAGER",
    "type": "http",
    "time": "2024-05-25 20:16:37.217",
    "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
  },
  {
    "id1": "2436",
    "id2": "2528",
    "logger": "INTERACT.MANAGER",
    "type": "http",
    "time": "2024-05-25 20:16:37.217",
    "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
  },
  {
    "id1": "2436",
    "id2": "2528",
    "logger": "INTERACT.MANAGER",
    "type": "db",
    "time": "2024-05-25 20:16:37.217",
    "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
  },
  {
    "id1": "2436",
    "id2": "2528",
    "logger": "INTERACT.MANAGER",
    "type": "http",
    "time": "2024-05-25 20:16:37.217",
    "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
  },
  {
    "id1": "2436",
    "id2": "2528",
    "logger": "INTERACT.MANAGER",
    "time": "2024-05-25 20:16:37.217",
    "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
  }
]
"#;
    let res = client
        .post("/v1/events/logs?db=public&table=d_table&pipeline_name=root")
        .header("Content-Type", "application/json")
        .body(data_body)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // 3. check table list
    validate_data(
        "test_pipeline_suffix_template_table_list",
        &client,
        "show tables",
        "[[\"d_table\"],[\"d_table_db\"],[\"d_table_http\"],[\"demo\"],[\"numbers\"]]",
    )
    .await;

    // 4. check each table's data
    validate_data(
        "test_pipeline_suffix_template_default",
        &client,
        "select * from d_table",
        "[[2436,2528,null,\"ClusterAdapter:enter sendTextDataToCluster\\\\n\",\"INTERACT.MANAGER\",1716668197217000000]]",
    )
    .await;

    validate_data(
        "test_pipeline_name_template_db",
        &client,
        "select * from d_table_db",
        "[[2436,2528,\"db\",\"ClusterAdapter:enter sendTextDataToCluster\\\\n\",\"INTERACT.MANAGER\",1716668197217000000]]",
    )
    .await;

    validate_data(
        "test_pipeline_name_template_http",
        &client,
        "select * from d_table_http",
        "[[2436,2528,\"http\",\"ClusterAdapter:enter sendTextDataToCluster\\\\n\",\"INTERACT.MANAGER\",1716668197217000000],[2436,2528,\"http\",\"ClusterAdapter:enter sendTextDataToCluster\\\\n\",\"INTERACT.MANAGER\",1716668197217000000],[2436,2528,\"http\",\"ClusterAdapter:enter sendTextDataToCluster\\\\n\",\"INTERACT.MANAGER\",1716668197217000000]]",
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_identity_pipeline_with_flatten(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_http_app_with_frontend(store_type, "test_identity_pipeline_with_flatten").await;

    let client = TestClient::new(app).await;
    let body = r#"{"__time__":1453809242,"__topic__":"","__source__":"10.170.***.***","ip":"10.200.**.***","time":"26/Jan/2016:19:54:02 +0800","url":"POST/PutData?Category=YunOsAccountOpLog&AccessKeyId=<yourAccessKeyId>&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=<yourSignature>HTTP/1.1","status":"200","user-agent":"aliyun-sdk-java","custom_map":{"value_a":["a","b","c"],"value_b":"b"}}"#;

    let res = send_req(
        &client,
        vec![
            (
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            ),
            (
                HeaderName::from_static("x-greptime-pipeline-params"),
                HeaderValue::from_static("flatten_json_object=true"),
            ),
        ],
        "/v1/ingest?table=logs&pipeline_name=greptime_identity",
        body.as_bytes().to_vec(),
        false,
    )
    .await;

    assert_eq!(StatusCode::OK, res.status());

    let expected = r#"[["greptime_timestamp","TimestampNanosecond","PRI","NO","","TIMESTAMP"],["__source__","String","","YES","","FIELD"],["__time__","Int64","","YES","","FIELD"],["__topic__","String","","YES","","FIELD"],["custom_map.value_a","Json","","YES","","FIELD"],["custom_map.value_b","String","","YES","","FIELD"],["ip","String","","YES","","FIELD"],["status","String","","YES","","FIELD"],["time","String","","YES","","FIELD"],["url","String","","YES","","FIELD"],["user-agent","String","","YES","","FIELD"]]"#;
    validate_data(
        "test_identity_pipeline_with_flatten_desc_logs",
        &client,
        "desc logs",
        expected,
    )
    .await;

    let expected = "[[[\"a\",\"b\",\"c\"]]]";
    validate_data(
        "test_identity_pipeline_with_flatten_select_json",
        &client,
        "select `custom_map.value_a` from logs",
        expected,
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_identity_pipeline_with_custom_ts(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_http_app_with_frontend(store_type, "test_identity_pipeline_with_custom_ts")
            .await;

    let client = TestClient::new(app).await;
    let body = r#"
    [{"__time__":1453809242,"__source__":"10.170.***.***", "__name__":"hello"},
    {"__time__":1453809252,"__source__":"10.170.***.***"}]
    "#;

    let res = send_req(
        &client,
        vec![(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/json"),
        )],
        "/v1/ingest?table=logs&pipeline_name=greptime_identity&custom_time_index=__time__;epoch;s",
        body.as_bytes().to_vec(),
        false,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    let expected = r#"[["__time__","TimestampSecond","PRI","NO","","TIMESTAMP"],["__name__","String","","YES","","FIELD"],["__source__","String","","YES","","FIELD"]]"#;
    validate_data(
        "test_identity_pipeline_with_custom_ts_desc_logs",
        &client,
        "desc logs",
        expected,
    )
    .await;

    let expected = r#"[[1453809242,"hello","10.170.***.***"],[1453809252,null,"10.170.***.***"]]"#;
    validate_data(
        "test_identity_pipeline_with_custom_ts_data",
        &client,
        "select * from logs",
        expected,
    )
    .await;

    // drop table
    let res = client.get("/v1/sql?sql=drop table logs").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = r#"
    [{"__time__":"2019-01-16 02:42:01+08:00","__source__":"10.170.***.***"},
    {"__time__":"2019-01-16 02:42:04+08:00","__source__":"10.170.***.***", "__name__":"hello"}]
    "#;

    let res = send_req(
        &client,
        vec![(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/json"),
        )],
        "/v1/ingest?table=logs&pipeline_name=greptime_identity&custom_time_index=__time__;datestr;%Y-%m-%d %H:%M:%S%z",
        body.as_bytes().to_vec(),
        false,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    let expected = r#"[["__time__","TimestampNanosecond","PRI","NO","","TIMESTAMP"],["__source__","String","","YES","","FIELD"],["__name__","String","","YES","","FIELD"]]"#;
    validate_data(
        "test_identity_pipeline_with_custom_ts_desc_logs",
        &client,
        "desc logs",
        expected,
    )
    .await;

    let expected = r#"[[1547577721000000000,"10.170.***.***",null],[1547577724000000000,"10.170.***.***","hello"]]"#;
    validate_data(
        "test_identity_pipeline_with_custom_ts_data",
        &client,
        "select * from logs",
        expected,
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_test_pipeline_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_pipeline_api").await;

    // handshake
    let client = TestClient::new(app).await;

    let pipeline_content = r#"
processors:
  - date:
      field: time
      formats:
        - "%Y-%m-%d %H:%M:%S%.3f"
      ignore_missing: true

transform:
  - fields:
      - id1
      - id2
    type: int32
  - fields:
      - type
      - log
      - logger
    type: string
  - field: time
    type: time
    index: timestamp
"#;

    // 1. create pipeline
    let res = client
        .post("/v1/pipelines/test")
        .header("Content-Type", "application/x-yaml")
        .body(pipeline_content)
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::OK);

    let content = res.text().await;

    let content = serde_json::from_str(&content);
    assert!(content.is_ok());
    //  {"execution_time_ms":13,"pipelines":[{"name":"test","version":"2024-07-04 08:31:00.987136"}]}
    let content: Value = content.unwrap();

    let execution_time = content.get("execution_time_ms");
    assert!(execution_time.unwrap().is_number());
    let pipelines = content.get("pipelines");
    let pipelines = pipelines.unwrap().as_array().unwrap();
    assert_eq!(pipelines.len(), 1);
    let pipeline = pipelines.first().unwrap();
    assert_eq!(pipeline.get("name").unwrap(), "test");

    let dryrun_schema = json!([
        {
            "colume_type": "FIELD",
            "data_type": "INT32",
            "fulltext": false,
            "name": "id1"
        },
        {
            "colume_type": "FIELD",
            "data_type": "INT32",
            "fulltext": false,
            "name": "id2"
        },
        {
            "colume_type": "FIELD",
            "data_type": "STRING",
            "fulltext": false,
            "name": "type"
        },
        {
            "colume_type": "FIELD",
            "data_type": "STRING",
            "fulltext": false,
            "name": "log"
        },
        {
            "colume_type": "FIELD",
            "data_type": "STRING",
            "fulltext": false,
            "name": "logger"
        },
        {
            "colume_type": "TIMESTAMP",
            "data_type": "TIMESTAMP_NANOSECOND",
            "fulltext": false,
            "name": "time"
        }
    ]);
    let dryrun_rows = json!([
        [
            {
                "data_type": "INT32",
                "key": "id1",
                "semantic_type": "FIELD",
                "value": 2436
            },
            {
                "data_type": "INT32",
                "key": "id2",
                "semantic_type": "FIELD",
                "value": 2528
            },
            {
                "data_type": "STRING",
                "key": "type",
                "semantic_type": "FIELD",
                "value": "I"
            },
            {
                "data_type": "STRING",
                "key": "log",
                "semantic_type": "FIELD",
                "value": "ClusterAdapter:enter sendTextDataToCluster\\n"
            },
            {
                "data_type": "STRING",
                "key": "logger",
                "semantic_type": "FIELD",
                "value": "INTERACT.MANAGER"
            },
            {
                "data_type": "TIMESTAMP_NANOSECOND",
                "key": "time",
                "semantic_type": "TIMESTAMP",
                "value": "2024-05-25 20:16:37.217+0000"
            }
        ]
    ]);
    {
        // test original api
        let data_body = r#"
        [
          {
            "id1": "2436",
            "id2": "2528",
            "logger": "INTERACT.MANAGER",
            "type": "I",
            "time": "2024-05-25 20:16:37.217",
            "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
          }
        ]
        "#;
        let res = client
            .post("/v1/pipelines/_dryrun?pipeline_name=test")
            .header("Content-Type", "application/json")
            .body(data_body)
            .send()
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        let body: Value = res.json().await;
        let schema = &body[0]["schema"];
        let rows = &body[0]["rows"];
        assert_eq!(schema, &dryrun_schema);
        assert_eq!(rows, &dryrun_rows);
    }
    {
        // test new api specify pipeline via pipeline_name
        let body = r#"
            {
            "pipeline_name": "test",
            "data": [
                {
                "id1": "2436",
                "id2": "2528",
                "logger": "INTERACT.MANAGER",
                "type": "I",
                "time": "2024-05-25 20:16:37.217",
                "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
                }
            ]
            }
        "#;
        let res = client
            .post("/v1/pipelines/_dryrun")
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        let body: Value = res.json().await;
        let schema = &body[0]["schema"];
        let rows = &body[0]["rows"];
        assert_eq!(schema, &dryrun_schema);
        assert_eq!(rows, &dryrun_rows);
    }
    {
        // test new api specify pipeline via pipeline raw data
        let mut body = json!({
        "data": [
            {
            "id1": "2436",
            "id2": "2528",
            "logger": "INTERACT.MANAGER",
            "type": "I",
            "time": "2024-05-25 20:16:37.217",
            "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
            }
        ]
        });
        body["pipeline"] = json!(pipeline_content);
        let res = client
            .post("/v1/pipelines/_dryrun")
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        let body: Value = res.json().await;
        let schema = &body[0]["schema"];
        let rows = &body[0]["rows"];
        assert_eq!(schema, &dryrun_schema);
        assert_eq!(rows, &dryrun_rows);
    }
    {
        // failback to old version api
        // not pipeline and pipeline_name in the body
        let body = json!({
        "data": [
            {
            "id1": "2436",
            "id2": "2528",
            "logger": "INTERACT.MANAGER",
            "type": "I",
            "time": "2024-05-25 20:16:37.217",
            "log": "ClusterAdapter:enter sendTextDataToCluster\\n"
            }
        ]
        });
        let res = client
            .post("/v1/pipelines/_dryrun")
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }
    guard.remove_all().await;
}

pub async fn test_plain_text_ingestion(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_pipeline_api").await;

    // handshake
    let client = TestClient::new(app).await;

    let body = r#"
processors:
  - dissect:
      fields:
        - message
      patterns:
        - "%{+ts} %{+ts} %{content}"
  - date:
      fields:
        - ts
      formats:
        - "%Y-%m-%d %H:%M:%S%.3f"

transform:
  - fields:
      - content
    type: string
  - field: ts
    type: time
    index: timestamp
"#;

    // 1. create pipeline
    let res = client
        .post("/v1/pipelines/test")
        .header("Content-Type", "application/x-yaml")
        .body(body)
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::OK);

    let content = res.text().await;

    let content = serde_json::from_str(&content);
    assert!(content.is_ok());
    //  {"execution_time_ms":13,"pipelines":[{"name":"test","version":"2024-07-04 08:31:00.987136"}]}
    let content: Value = content.unwrap();

    let version_str = content
        .get("pipelines")
        .unwrap()
        .as_array()
        .unwrap()
        .first()
        .unwrap()
        .get("version")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    assert!(!version_str.is_empty());

    // 2. write data
    let data_body = r#"
2024-05-25 20:16:37.217 hello
2024-05-25 20:16:37.218 hello world
"#;
    let res = client
        .post("/v1/ingest?db=public&table=logs1&pipeline_name=test")
        .header("Content-Type", "text/plain")
        .body(data_body)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // 3. select data
    let res = client.get("/v1/sql?sql=select * from logs1").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let resp = res.text().await;

    let v = get_rows_from_output(&resp);

    assert_eq!(
        v,
        r#"[["hello",1716668197217000000],["hello world",1716668197218000000]]"#,
    );

    guard.remove_all().await;
}

pub async fn test_otlp_metrics(store_type: StorageType) {
    // init
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_otlp_metrics").await;

    let content = r#"
{"resourceMetrics":[{"resource":{"attributes":[],"droppedAttributesCount":0},"scopeMetrics":[{"scope":{"name":"","version":"","attributes":[],"droppedAttributesCount":0},"metrics":[{"name":"gen","description":"","unit":"","metadata":[],"gauge":{"dataPoints":[{"attributes":[],"startTimeUnixNano":"0","timeUnixNano":"1736489291872539000","exemplars":[],"flags":0,"asInt":0}]}}],"schemaUrl":""}],"schemaUrl":"https://opentelemetry.io/schemas/1.13.0"},{"resource":{"attributes":[],"droppedAttributesCount":0},"scopeMetrics":[{"scope":{"name":"","version":"","attributes":[],"droppedAttributesCount":0},"metrics":[{"name":"gen","description":"","unit":"","metadata":[],"gauge":{"dataPoints":[{"attributes":[],"startTimeUnixNano":"0","timeUnixNano":"1736489291919917000","exemplars":[],"flags":0,"asInt":1}]}}],"schemaUrl":""}],"schemaUrl":"https://opentelemetry.io/schemas/1.13.0"}]}
    "#;

    let req: ExportMetricsServiceRequest = serde_json::from_str(content).unwrap();
    let body = req.encode_to_vec();

    // handshake
    let client = TestClient::new(app).await;

    // write metrics data
    let res = send_req(
        &client,
        vec![(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/x-protobuf"),
        )],
        "/v1/otlp/v1/metrics",
        body.clone(),
        false,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    // select metrics data
    let expected = "[[1736489291872539000,0.0],[1736489291919917000,1.0]]";
    validate_data("otlp_metrics", &client, "select * from gen;", expected).await;

    // drop table
    let res = client.get("/v1/sql?sql=drop table gen;").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    // write metrics data with gzip
    let res = send_req(
        &client,
        vec![(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/x-protobuf"),
        )],
        "/v1/otlp/v1/metrics",
        body.clone(),
        true,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    // select metrics data again
    validate_data(
        "otlp_metrics_with_gzip",
        &client,
        "select * from gen;",
        expected,
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_otlp_traces_v0(store_type: StorageType) {
    // init
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_otlp_traces").await;

    let content = r#"
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"telemetrygen"}}],"droppedAttributesCount":0},"scopeSpans":[{"scope":{"name":"telemetrygen","version":"","attributes":[],"droppedAttributesCount":0},"spans":[{"traceId":"c05d7a4ec8e1f231f02ed6e8da8655b4","spanId":"9630f2916e2f7909","traceState":"","parentSpanId":"d24f921c75f68e23","flags":256,"name":"okey-dokey-0","kind":2,"startTimeUnixNano":"1736480942444376000","endTimeUnixNano":"1736480942444499000","attributes":[{"key":"net.peer.ip","value":{"stringValue":"1.2.3.4"}},{"key":"peer.service","value":{"stringValue":"telemetrygen-client"}}],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"links":[],"droppedLinksCount":0,"status":{"message":"","code":0}},{"traceId":"c05d7a4ec8e1f231f02ed6e8da8655b4","spanId":"d24f921c75f68e23","traceState":"","parentSpanId":"","flags":256,"name":"lets-go","kind":3,"startTimeUnixNano":"1736480942444376000","endTimeUnixNano":"1736480942444499000","attributes":[{"key":"net.peer.ip","value":{"stringValue":"1.2.3.4"}},{"key":"peer.service","value":{"stringValue":"telemetrygen-server"}}],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"links":[],"droppedLinksCount":0,"status":{"message":"","code":0}},{"traceId":"cc9e0991a2e63d274984bd44ee669203","spanId":"8f847259b0f6e1ab","traceState":"","parentSpanId":"eba7be77e3558179","flags":256,"name":"okey-dokey-0","kind":2,"startTimeUnixNano":"1736480942444589000","endTimeUnixNano":"1736480942444712000","attributes":[{"key":"net.peer.ip","value":{"stringValue":"1.2.3.4"}},{"key":"peer.service","value":{"stringValue":"telemetrygen-client"}}],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"links":[],"droppedLinksCount":0,"status":{"message":"","code":0}},{"traceId":"cc9e0991a2e63d274984bd44ee669203","spanId":"eba7be77e3558179","traceState":"","parentSpanId":"","flags":256,"name":"lets-go","kind":3,"startTimeUnixNano":"1736480942444589000","endTimeUnixNano":"1736480942444712000","attributes":[{"key":"net.peer.ip","value":{"stringValue":"1.2.3.4"}},{"key":"peer.service","value":{"stringValue":"telemetrygen-server"}}],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"links":[],"droppedLinksCount":0,"status":{"message":"","code":0}}],"schemaUrl":""}],"schemaUrl":"https://opentelemetry.io/schemas/1.4.0"}]}
"#;

    let req: ExportTraceServiceRequest = serde_json::from_str(content).unwrap();
    let body = req.encode_to_vec();

    // handshake
    let client = TestClient::new(app).await;

    // write traces data
    let res = send_req(
        &client,
        vec![
            (
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/x-protobuf"),
            ),
            (
                HeaderName::from_static("x-greptime-pipeline-name"),
                HeaderValue::from_static("greptime_trace_v0"),
            ),
        ],
        "/v1/otlp/v1/traces",
        body.clone(),
        false,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    let expected = r#"[["telemetrygen"]]"#;
    validate_data(
        "otlp_traces",
        &client,
        &format!(
            "select service_name from {};",
            trace_services_table_name(TRACE_TABLE_NAME)
        ),
        expected,
    )
    .await;

    // select traces data
    let expected = r#"[[1736480942444376000,1736480942444499000,123000,"telemetrygen","c05d7a4ec8e1f231f02ed6e8da8655b4","d24f921c75f68e23","","SPAN_KIND_CLIENT","lets-go","STATUS_CODE_UNSET","","",{"net.peer.ip":"1.2.3.4","peer.service":"telemetrygen-server"},[],[],"telemetrygen","",{},{"service.name":"telemetrygen"}],[1736480942444376000,1736480942444499000,123000,"telemetrygen","c05d7a4ec8e1f231f02ed6e8da8655b4","9630f2916e2f7909","d24f921c75f68e23","SPAN_KIND_SERVER","okey-dokey-0","STATUS_CODE_UNSET","","",{"net.peer.ip":"1.2.3.4","peer.service":"telemetrygen-client"},[],[],"telemetrygen","",{},{"service.name":"telemetrygen"}],[1736480942444589000,1736480942444712000,123000,"telemetrygen","cc9e0991a2e63d274984bd44ee669203","eba7be77e3558179","","SPAN_KIND_CLIENT","lets-go","STATUS_CODE_UNSET","","",{"net.peer.ip":"1.2.3.4","peer.service":"telemetrygen-server"},[],[],"telemetrygen","",{},{"service.name":"telemetrygen"}],[1736480942444589000,1736480942444712000,123000,"telemetrygen","cc9e0991a2e63d274984bd44ee669203","8f847259b0f6e1ab","eba7be77e3558179","SPAN_KIND_SERVER","okey-dokey-0","STATUS_CODE_UNSET","","",{"net.peer.ip":"1.2.3.4","peer.service":"telemetrygen-client"},[],[],"telemetrygen","",{},{"service.name":"telemetrygen"}]]"#;
    validate_data(
        "otlp_traces",
        &client,
        "select * from opentelemetry_traces;",
        expected,
    )
    .await;

    // drop table
    let res = client
        .get("/v1/sql?sql=drop table opentelemetry_traces;")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // write traces data with gzip
    let res = send_req(
        &client,
        vec![
            (
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/x-protobuf"),
            ),
            (
                HeaderName::from_static("x-greptime-pipeline-name"),
                HeaderValue::from_static("greptime_trace_v0"),
            ),
        ],
        "/v1/otlp/v1/traces",
        body.clone(),
        true,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    // write traces data without pipeline
    let res = send_req(
        &client,
        vec![(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/x-protobuf"),
        )],
        "/v1/otlp/v1/traces",
        body.clone(),
        true,
    )
    .await;
    assert_eq!(StatusCode::BAD_REQUEST, res.status());

    // select traces data again
    validate_data(
        "otlp_traces_with_gzip",
        &client,
        "select * from opentelemetry_traces;",
        expected,
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_otlp_traces_v1(store_type: StorageType) {
    // init
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_otlp_traces").await;

    let content = r#"
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"telemetrygen"}}],"droppedAttributesCount":0},"scopeSpans":[{"scope":{"name":"telemetrygen","version":"","attributes":[],"droppedAttributesCount":0},"spans":[{"traceId":"c05d7a4ec8e1f231f02ed6e8da8655b4","spanId":"9630f2916e2f7909","traceState":"","parentSpanId":"d24f921c75f68e23","flags":256,"name":"okey-dokey-0","kind":2,"startTimeUnixNano":"1736480942444376000","endTimeUnixNano":"1736480942444499000","attributes":[{"key":"net.peer.ip","value":{"stringValue":"1.2.3.4"}},{"key":"peer.service","value":{"stringValue":"telemetrygen-client"}}],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"links":[],"droppedLinksCount":0,"status":{"message":"","code":0}},{"traceId":"c05d7a4ec8e1f231f02ed6e8da8655b4","spanId":"d24f921c75f68e23","traceState":"","parentSpanId":"","flags":256,"name":"lets-go","kind":3,"startTimeUnixNano":"1736480942444376000","endTimeUnixNano":"1736480942444499000","attributes":[{"key":"net.peer.ip","value":{"stringValue":"1.2.3.4"}},{"key":"peer.service","value":{"stringValue":"telemetrygen-server"}}],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"links":[],"droppedLinksCount":0,"status":{"message":"","code":0}},{"traceId":"cc9e0991a2e63d274984bd44ee669203","spanId":"8f847259b0f6e1ab","traceState":"","parentSpanId":"eba7be77e3558179","flags":256,"name":"okey-dokey-0","kind":2,"startTimeUnixNano":"1736480942444589000","endTimeUnixNano":"1736480942444712000","attributes":[{"key":"net.peer.ip","value":{"stringValue":"1.2.3.4"}},{"key":"peer.service","value":{"stringValue":"telemetrygen-client"}}],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"links":[],"droppedLinksCount":0,"status":{"message":"","code":0}},{"traceId":"cc9e0991a2e63d274984bd44ee669203","spanId":"eba7be77e3558179","traceState":"","parentSpanId":"","flags":256,"name":"lets-go","kind":3,"startTimeUnixNano":"1736480942444589000","endTimeUnixNano":"1736480942444712000","attributes":[{"key":"net.peer.ip","value":{"stringValue":"1.2.3.4"}},{"key":"peer.service","value":{"stringValue":"telemetrygen-server"}}],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"links":[],"droppedLinksCount":0,"status":{"message":"","code":0}}],"schemaUrl":""}],"schemaUrl":"https://opentelemetry.io/schemas/1.4.0"}]}
"#;

    let trace_table_name = "mytable";
    let req: ExportTraceServiceRequest = serde_json::from_str(content).unwrap();
    let body = req.encode_to_vec();

    // handshake
    let client = TestClient::new(app).await;

    // write traces data
    let res = send_req(
        &client,
        vec![
            (
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/x-protobuf"),
            ),
            (
                HeaderName::from_static("x-greptime-pipeline-name"),
                HeaderValue::from_static(GREPTIME_INTERNAL_TRACE_PIPELINE_V1_NAME),
            ),
            (
                HeaderName::from_static("x-greptime-trace-table-name"),
                HeaderValue::from_static(trace_table_name),
            ),
        ],
        "/v1/otlp/v1/traces",
        body.clone(),
        false,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    let expected = r#"[["telemetrygen"]]"#;
    validate_data(
        "otlp_traces",
        &client,
        &format!(
            "select service_name from {};",
            trace_services_table_name(trace_table_name)
        ),
        expected,
    )
    .await;

    // select traces data
    let expected = r#"[[1736480942444376000,1736480942444499000,123000,null,"c05d7a4ec8e1f231f02ed6e8da8655b4","d24f921c75f68e23","SPAN_KIND_CLIENT","lets-go","STATUS_CODE_UNSET","","","telemetrygen","","telemetrygen","1.2.3.4","telemetrygen-server",[],[]],[1736480942444376000,1736480942444499000,123000,"d24f921c75f68e23","c05d7a4ec8e1f231f02ed6e8da8655b4","9630f2916e2f7909","SPAN_KIND_SERVER","okey-dokey-0","STATUS_CODE_UNSET","","","telemetrygen","","telemetrygen","1.2.3.4","telemetrygen-client",[],[]],[1736480942444589000,1736480942444712000,123000,null,"cc9e0991a2e63d274984bd44ee669203","eba7be77e3558179","SPAN_KIND_CLIENT","lets-go","STATUS_CODE_UNSET","","","telemetrygen","","telemetrygen","1.2.3.4","telemetrygen-server",[],[]],[1736480942444589000,1736480942444712000,123000,"eba7be77e3558179","cc9e0991a2e63d274984bd44ee669203","8f847259b0f6e1ab","SPAN_KIND_SERVER","okey-dokey-0","STATUS_CODE_UNSET","","","telemetrygen","","telemetrygen","1.2.3.4","telemetrygen-client",[],[]]]"#;
    validate_data("otlp_traces", &client, "select * from mytable;", expected).await;

    let expected_ddl = r#"[["mytable","CREATE TABLE IF NOT EXISTS \"mytable\" (\n  \"timestamp\" TIMESTAMP(9) NOT NULL,\n  \"timestamp_end\" TIMESTAMP(9) NULL,\n  \"duration_nano\" BIGINT UNSIGNED NULL,\n  \"parent_span_id\" STRING NULL SKIPPING INDEX WITH(granularity = '10240', type = 'BLOOM'),\n  \"trace_id\" STRING NULL SKIPPING INDEX WITH(granularity = '10240', type = 'BLOOM'),\n  \"span_id\" STRING NULL,\n  \"span_kind\" STRING NULL,\n  \"span_name\" STRING NULL,\n  \"span_status_code\" STRING NULL,\n  \"span_status_message\" STRING NULL,\n  \"trace_state\" STRING NULL,\n  \"scope_name\" STRING NULL,\n  \"scope_version\" STRING NULL,\n  \"service_name\" STRING NULL SKIPPING INDEX WITH(granularity = '10240', type = 'BLOOM'),\n  \"span_attributes.net.peer.ip\" STRING NULL,\n  \"span_attributes.peer.service\" STRING NULL,\n  \"span_events\" JSON NULL,\n  \"span_links\" JSON NULL,\n  TIME INDEX (\"timestamp\"),\n  PRIMARY KEY (\"service_name\")\n)\nPARTITION ON COLUMNS (\"trace_id\") (\n  trace_id < '1',\n  trace_id >= 'f',\n  trace_id >= '1' AND trace_id < '2',\n  trace_id >= '2' AND trace_id < '3',\n  trace_id >= '3' AND trace_id < '4',\n  trace_id >= '4' AND trace_id < '5',\n  trace_id >= '5' AND trace_id < '6',\n  trace_id >= '6' AND trace_id < '7',\n  trace_id >= '7' AND trace_id < '8',\n  trace_id >= '8' AND trace_id < '9',\n  trace_id >= '9' AND trace_id < 'a',\n  trace_id >= 'a' AND trace_id < 'b',\n  trace_id >= 'b' AND trace_id < 'c',\n  trace_id >= 'c' AND trace_id < 'd',\n  trace_id >= 'd' AND trace_id < 'e',\n  trace_id >= 'e' AND trace_id < 'f'\n)\nENGINE=mito\nWITH(\n  append_mode = 'true',\n  table_data_model = 'greptime_trace_v1'\n)"]]"#;
    validate_data(
        "otlp_traces",
        &client,
        "show create table mytable;",
        expected_ddl,
    )
    .await;

    let expected_ddl = r#"[["mytable_services","CREATE TABLE IF NOT EXISTS \"mytable_services\" (\n  \"timestamp\" TIMESTAMP(9) NOT NULL,\n  \"service_name\" STRING NULL,\n  TIME INDEX (\"timestamp\")\n)\n\nENGINE=mito\nWITH(\n  append_mode = 'true'\n)"]]"#;
    validate_data(
        "otlp_traces",
        &client,
        &format!(
            "show create table {};",
            trace_services_table_name(trace_table_name)
        ),
        expected_ddl,
    )
    .await;

    // drop table
    let res = client.get("/v1/sql?sql=drop table mytable;").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    // write traces data with gzip
    let res = send_req(
        &client,
        vec![
            (
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/x-protobuf"),
            ),
            (
                HeaderName::from_static("x-greptime-pipeline-name"),
                HeaderValue::from_static(GREPTIME_INTERNAL_TRACE_PIPELINE_V1_NAME),
            ),
            (
                HeaderName::from_static("x-greptime-trace-table-name"),
                HeaderValue::from_static(trace_table_name),
            ),
        ],
        "/v1/otlp/v1/traces",
        body.clone(),
        true,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    // select traces data again
    validate_data(
        "otlp_traces_with_gzip",
        &client,
        "select * from mytable;",
        expected,
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_otlp_logs(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_otlp_logs").await;

    let client = TestClient::new(app).await;
    let content = r#"
{"resourceLogs":[{"resource":{"attributes":[],"droppedAttributesCount":0},"scopeLogs":[{"scope":{"name":"","version":"","attributes":[{"key":"instance_num","value":{"stringValue":"10"}}],"droppedAttributesCount":0},"logRecords":[{"timeUnixNano":"1736413568497632000","observedTimeUnixNano":"0","severityNumber":9,"severityText":"Info","body":{"stringValue":"the message line one"},"attributes":[{"key":"app","value":{"stringValue":"server1"}}],"droppedAttributesCount":0,"flags":0,"traceId":"f665100a612542b69cc362fe2ae9d3bf","spanId":"e58f01c4c69f4488"}],"schemaUrl":""}],"schemaUrl":"https://opentelemetry.io/schemas/1.4.0"},{"resource":{"attributes":[],"droppedAttributesCount":0},"scopeLogs":[{"scope":{"name":"","version":"","attributes":[],"droppedAttributesCount":0},"logRecords":[{"timeUnixNano":"1736413568538897000","observedTimeUnixNano":"0","severityNumber":9,"severityText":"Info","body":{"stringValue":"the message line two"},"attributes":[{"key":"app","value":{"stringValue":"server2"}}],"droppedAttributesCount":0,"flags":0,"traceId":"f665100a612542b69cc362fe2ae9d3bf","spanId":"e58f01c4c69f4488"}],"schemaUrl":""}],"schemaUrl":"https://opentelemetry.io/schemas/1.4.0"}]}
"#;

    let req: ExportLogsServiceRequest = serde_json::from_str(content).unwrap();
    let body = req.encode_to_vec();

    {
        // write log data
        let res = send_req(
            &client,
            vec![(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/x-protobuf"),
            )],
            "/v1/otlp/v1/logs?db=public",
            body.clone(),
            false,
        )
        .await;
        assert_eq!(StatusCode::OK, res.status());
        let expected = "[[1736413568497632000,\"f665100a612542b69cc362fe2ae9d3bf\",\"e58f01c4c69f4488\",\"Info\",9,\"the message line one\",{\"app\":\"server1\"},0,\"\",\"\",{\"instance_num\":\"10\"},\"\",{},\"https://opentelemetry.io/schemas/1.4.0\"],[1736413568538897000,\"f665100a612542b69cc362fe2ae9d3bf\",\"e58f01c4c69f4488\",\"Info\",9,\"the message line two\",{\"app\":\"server2\"},0,\"\",\"\",{},\"\",{},\"https://opentelemetry.io/schemas/1.4.0\"]]";
        validate_data(
            "otlp_logs",
            &client,
            "select * from opentelemetry_logs;",
            expected,
        )
        .await;
    }

    {
        // write log data with selector
        let res = send_req(
            &client,
            vec![
                (
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_static("application/x-protobuf"),
                ),
                (
                    HeaderName::from_static("x-greptime-log-table-name"),
                    HeaderValue::from_static("cus_logs"),
                ),
                (
                    HeaderName::from_static("x-greptime-log-extract-keys"),
                    HeaderValue::from_static("resource-attr,instance_num,app,not-exist"),
                ),
            ],
            "/v1/otlp/v1/logs?db=public",
            body.clone(),
            false,
        )
        .await;
        assert_eq!(StatusCode::OK, res.status());

        let expected = "[[1736413568538897000,\"f665100a612542b69cc362fe2ae9d3bf\",\"e58f01c4c69f4488\",\"Info\",9,\"the message line two\",{\"app\":\"server2\"},0,\"\",\"\",{},\"\",{},\"https://opentelemetry.io/schemas/1.4.0\",null,\"server2\"],[1736413568497632000,\"f665100a612542b69cc362fe2ae9d3bf\",\"e58f01c4c69f4488\",\"Info\",9,\"the message line one\",{\"app\":\"server1\"},0,\"\",\"\",{\"instance_num\":\"10\"},\"\",{},\"https://opentelemetry.io/schemas/1.4.0\",\"10\",\"server1\"]]";
        validate_data(
            "otlp_logs_with_selector",
            &client,
            "select * from cus_logs;",
            expected,
        )
        .await;
    }

    {
        // test same selector with multiple value
        let content = r#"
        {"resourceLogs":[{"resource":{"attributes":[{"key":"fromwhere","value":{"stringValue":"resource"}}],"droppedAttributesCount":0},"scopeLogs":[{"scope":{"name":"","version":"","attributes":[{"key":"fromwhere","value":{"stringValue":"scope"}}],"droppedAttributesCount":0},"logRecords":[{"timeUnixNano":"1736413568497632000","observedTimeUnixNano":"0","severityNumber":9,"severityText":"Info","body":{"stringValue":"the message line one"},"attributes":[{"key":"app","value":{"stringValue":"server"}},{"key":"fromwhere","value":{"stringValue":"log_attr"}}],"droppedAttributesCount":0,"flags":0,"traceId":"f665100a612542b69cc362fe2ae9d3bf","spanId":"e58f01c4c69f4488"}],"schemaUrl":""}],"schemaUrl":"https://opentelemetry.io/schemas/1.4.0"}]}
        "#;
        let req: ExportLogsServiceRequest = serde_json::from_str(content).unwrap();
        let body = req.encode_to_vec();
        let res = send_req(
            &client,
            vec![
                (
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_static("application/x-protobuf"),
                ),
                (
                    HeaderName::from_static("x-greptime-log-table-name"),
                    HeaderValue::from_static("logs2"),
                ),
                (
                    HeaderName::from_static("x-greptime-log-extract-keys"),
                    HeaderValue::from_static("fromwhere"),
                ),
            ],
            "/v1/otlp/v1/logs?db=public",
            body.clone(),
            false,
        )
        .await;
        assert_eq!(StatusCode::OK, res.status());

        let expected = "[[1736413568497632000,\"f665100a612542b69cc362fe2ae9d3bf\",\"e58f01c4c69f4488\",\"Info\",9,\"the message line one\",{\"app\":\"server\",\"fromwhere\":\"log_attr\"},0,\"\",\"\",{\"fromwhere\":\"scope\"},\"\",{\"fromwhere\":\"resource\"},\"https://opentelemetry.io/schemas/1.4.0\",\"log_attr\"]]";
        validate_data(
            "otlp_logs_with_selector_overlapping",
            &client,
            "select * from logs2;",
            expected,
        )
        .await;
    }

    guard.remove_all().await;
}

pub async fn test_loki_pb_logs(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_loki_pb_logs").await;

    let client = TestClient::new(app).await;

    // init loki request
    let req: PushRequest = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{service="test",source="integration",wadaxi="do anything"}"#.to_string(),
            entries: vec![
                EntryAdapter {
                    timestamp: Some(Timestamp::from_str("2024-11-07T10:53:50").unwrap()),
                    line: "this is a log message".to_string(),
                    structured_metadata: vec![
                        LabelPairAdapter {
                            name: "key1".to_string(),
                            value: "value1".to_string(),
                        },
                        LabelPairAdapter {
                            name: "key2".to_string(),
                            value: "value2".to_string(),
                        },
                    ],
                    parsed: vec![],
                },
                EntryAdapter {
                    timestamp: Some(Timestamp::from_str("2024-11-07T10:53:51").unwrap()),
                    line: "this is a log message 2".to_string(),
                    structured_metadata: vec![LabelPairAdapter {
                        name: "key3".to_string(),
                        value: "value3".to_string(),
                    }],
                    parsed: vec![],
                },
                EntryAdapter {
                    timestamp: Some(Timestamp::from_str("2024-11-07T10:53:52").unwrap()),
                    line: "this is a log message 2".to_string(),
                    structured_metadata: vec![],
                    parsed: vec![],
                },
            ],
            hash: rand::random(),
        }],
    };
    let encode = req.encode_to_vec();
    let body = prom_store::snappy_compress(&encode).unwrap();

    // write to loki
    let res = send_req(
        &client,
        vec![
            (
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/x-protobuf"),
            ),
            (
                HeaderName::from_static("content-encoding"),
                HeaderValue::from_static("snappy"),
            ),
            (
                HeaderName::from_static("accept-encoding"),
                HeaderValue::from_static("identity"),
            ),
            (
                HeaderName::from_static(GREPTIME_LOG_TABLE_NAME_HEADER_NAME),
                HeaderValue::from_static("loki_table_name"),
            ),
        ],
        "/v1/loki/api/v1/push",
        body,
        false,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    // test schema
    let expected = "[[\"loki_table_name\",\"CREATE TABLE IF NOT EXISTS \\\"loki_table_name\\\" (\\n  \\\"greptime_timestamp\\\" TIMESTAMP(9) NOT NULL,\\n  \\\"line\\\" STRING NULL,\\n  \\\"structured_metadata\\\" JSON NULL,\\n  \\\"service\\\" STRING NULL,\\n  \\\"source\\\" STRING NULL,\\n  \\\"wadaxi\\\" STRING NULL,\\n  TIME INDEX (\\\"greptime_timestamp\\\"),\\n  PRIMARY KEY (\\\"service\\\", \\\"source\\\", \\\"wadaxi\\\")\\n)\\n\\nENGINE=mito\\nWITH(\\n  append_mode = 'true'\\n)\"]]";
    validate_data(
        "loki_pb_schema",
        &client,
        "show create table loki_table_name;",
        expected,
    )
    .await;

    // test content
    let expected = "[[1730976830000000000,\"this is a log message\",{\"key1\":\"value1\",\"key2\":\"value2\"},\"test\",\"integration\",\"do anything\"],[1730976831000000000,\"this is a log message 2\",{\"key3\":\"value3\"},\"test\",\"integration\",\"do anything\"],[1730976832000000000,\"this is a log message 2\",{},\"test\",\"integration\",\"do anything\"]]";
    validate_data(
        "loki_pb_content",
        &client,
        "select * from loki_table_name;",
        expected,
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_loki_json_logs(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_http_app_with_frontend(store_type, "test_loki_json_logs").await;

    let client = TestClient::new(app).await;

    let body = r#"
{
  "streams": [
    {
      "stream": {
        "source": "test",
        "sender": "integration"
      },
      "values": [
          [ "1735901380059465984", "this is line one", {"key1":"value1","key2":"value2"}],
          [ "1735901398478897920", "this is line two", {"key3":"value3"}],
          [ "1735901398478897921", "this is line two updated"]
      ]
    }
  ]
}
    "#;

    let body = body.as_bytes().to_vec();

    // write plain to loki
    let res = send_req(
        &client,
        vec![
            (
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            ),
            (
                HeaderName::from_static(GREPTIME_LOG_TABLE_NAME_HEADER_NAME),
                HeaderValue::from_static("loki_table_name"),
            ),
        ],
        "/v1/loki/api/v1/push",
        body,
        false,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    // test schema
    let expected =  "[[\"loki_table_name\",\"CREATE TABLE IF NOT EXISTS \\\"loki_table_name\\\" (\\n  \\\"greptime_timestamp\\\" TIMESTAMP(9) NOT NULL,\\n  \\\"line\\\" STRING NULL,\\n  \\\"structured_metadata\\\" JSON NULL,\\n  \\\"sender\\\" STRING NULL,\\n  \\\"source\\\" STRING NULL,\\n  TIME INDEX (\\\"greptime_timestamp\\\"),\\n  PRIMARY KEY (\\\"sender\\\", \\\"source\\\")\\n)\\n\\nENGINE=mito\\nWITH(\\n  append_mode = 'true'\\n)\"]]";
    validate_data(
        "loki_json_schema",
        &client,
        "show create table loki_table_name;",
        expected,
    )
    .await;

    // test content
    let expected = "[[1735901380059465984,\"this is line one\",{\"key1\":\"value1\",\"key2\":\"value2\"},\"integration\",\"test\"],[1735901398478897920,\"this is line two\",{\"key3\":\"value3\"},\"integration\",\"test\"],[1735901398478897921,\"this is line two updated\",{},\"integration\",\"test\"]]";
    validate_data(
        "loki_json_content",
        &client,
        "select * from loki_table_name;",
        expected,
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_elasticsearch_logs(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_http_app_with_frontend(store_type, "test_elasticsearch_logs").await;

    let client = TestClient::new(app).await;

    let body = r#"
        {"create":{"_index":"test","_id":"1"}}
        {"foo":"foo_value1", "bar":"value1"}
        {"create":{"_index":"test","_id":"2"}}
        {"foo":"foo_value2","bar":"value2"}
    "#;

    let res = send_req(
        &client,
        vec![(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/json"),
        )],
        "/v1/elasticsearch/_bulk",
        body.as_bytes().to_vec(),
        false,
    )
    .await;

    assert_eq!(StatusCode::OK, res.status());

    let expected = "[[\"foo_value1\",\"value1\"],[\"foo_value2\",\"value2\"]]";

    validate_data(
        "test_elasticsearch_logs",
        &client,
        "select foo, bar from test;",
        expected,
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_elasticsearch_logs_with_index(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_http_app_with_frontend(store_type, "test_elasticsearch_logs_with_index").await;

    let client = TestClient::new(app).await;

    // It will write to test_index1 and test_index2(specified in the path).
    let body = r#"
        {"create":{"_index":"test_index1","_id":"1"}}
        {"foo":"foo_value1", "bar":"value1"}
        {"create":{"_id":"2"}}
        {"foo":"foo_value2","bar":"value2"}
    "#;

    let res = send_req(
        &client,
        vec![(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/json"),
        )],
        "/v1/elasticsearch/test_index2/_bulk",
        body.as_bytes().to_vec(),
        false,
    )
    .await;

    assert_eq!(StatusCode::OK, res.status());

    // test content of test_index1
    let expected = "[[\"foo_value1\",\"value1\"]]";
    validate_data(
        "test_elasticsearch_logs_with_index",
        &client,
        "select foo, bar from test_index1;",
        expected,
    )
    .await;

    // test content of test_index2
    let expected = "[[\"foo_value2\",\"value2\"]]";
    validate_data(
        "test_elasticsearch_logs_with_index_2",
        &client,
        "select foo, bar from test_index2;",
        expected,
    )
    .await;

    guard.remove_all().await;
}

pub async fn test_log_query(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_log_query").await;

    let client = TestClient::new(app).await;

    // prepare data with SQL API
    let res = client
        .get("/v1/sql?sql=create table logs (`ts` timestamp time index, message string);")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK, "{:?}", res.text().await);
    let res = client
        .post("/v1/sql?sql=insert into logs values ('2024-11-07 10:53:50', 'hello');")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK, "{:?}", res.text().await);

    // test log query
    let log_query = LogQuery {
        table: TableName {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: "logs".to_string(),
        },
        time_filter: TimeFilter {
            start: Some("2024-11-07".to_string()),
            end: None,
            span: None,
        },
        limit: Limit {
            skip: None,
            fetch: Some(1),
        },
        columns: vec!["ts".to_string(), "message".to_string()],
        filters: vec![],
        context: Context::None,
        exprs: vec![],
    };
    let res = client
        .post("/v1/logs")
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&log_query).unwrap())
        .send()
        .await;

    assert_eq!(res.status(), StatusCode::OK, "{:?}", res.text().await);
    let resp = res.text().await;
    let v = get_rows_from_output(&resp);
    assert_eq!(v, "[[1730976830000,\"hello\"]]");

    guard.remove_all().await;
}

pub async fn test_jaeger_query_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_http_app_with_frontend(store_type, "test_jaeger_query_api").await;

    let client = TestClient::new(app).await;

    // Test empty response for `/api/services` API before writing any traces.
    let res = client.get("/v1/jaeger/api/services").send().await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
    {
        "data": null,
        "total": 0,
        "limit": 0,
        "offset": 0,
        "errors": []
    }
    "#;
    let resp: Value = serde_json::from_str(&res.text().await).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    let content = r#"
    {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {
                                "stringValue": "test-jaeger-query-api"
                            }
                        }
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {
                        "name": "test-jaeger-query-api",
                        "version": "1.0.0"
                        },
                        "spans": [
                            {
                                "traceId": "5611dce1bc9ebed65352d99a027b08ea",
                                "spanId": "008421dbbd33a3e9",
                                "name": "access-mysql",
                                "kind": 2,
                                "startTimeUnixNano": "1738726754492421000",
                                "endTimeUnixNano": "1738726754592421000",
                                "attributes": [
                                    {
                                        "key": "operation.type",
                                        "value": {
                                        "stringValue": "access-mysql"
                                        }
                                    },
                                    {
                                        "key": "net.peer.ip",
                                        "value": {
                                        "stringValue": "1.2.3.4"
                                        }
                                    },
                                    {
                                        "key": "peer.service",
                                        "value": {
                                        "stringValue": "test-jaeger-query-api"
                                        }
                                    }
                                ],
                                "status": {
                                    "message": "success",
                                    "code": 0
                                }
                            }
                        ]
                    },
                    {
                        "scope": {
                        "name": "test-jaeger-query-api",
                        "version": "1.0.0"
                        },
                        "spans": [
                            {
                                "traceId": "5611dce1bc9ebed65352d99a027b08ea",
                                "spanId": "ffa03416a7b9ea48",
                                "name": "access-redis",
                                "kind": 2,
                                "startTimeUnixNano": "1738726754492422000",
                                "endTimeUnixNano": "1738726754592422000",
                                "attributes": [
                                    {
                                        "key": "operation.type",
                                        "value": {
                                        "stringValue": "access-redis"
                                        }
                                    },
                                    {
                                        "key": "net.peer.ip",
                                        "value": {
                                        "stringValue": "1.2.3.4"
                                        }
                                    },
                                    {
                                        "key": "peer.service",
                                        "value": {
                                        "stringValue": "test-jaeger-query-api"
                                        }
                                    }
                                ],
                                "status": {
                                    "message": "success",
                                    "code": 0
                                }
                            }
                        ]
                    }
                ],
                "schemaUrl": "https://opentelemetry.io/schemas/1.4.0"
            }
        ]
    }
    "#;

    let req: ExportTraceServiceRequest = serde_json::from_str(content).unwrap();
    let body = req.encode_to_vec();

    // write traces data.
    let res = send_req(
        &client,
        vec![
            (
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/x-protobuf"),
            ),
            (
                HeaderName::from_static("x-greptime-pipeline-name"),
                HeaderValue::from_static("greptime_trace_v0"),
            ),
        ],
        "/v1/otlp/v1/traces",
        body.clone(),
        false,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    // Test `/api/services` API.
    let res = client.get("/v1/jaeger/api/services").send().await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
    {
        "data": [
            "test-jaeger-query-api"
        ],
        "total": 1,
        "limit": 0,
        "offset": 0,
        "errors": []
    }
    "#;
    let resp: Value = serde_json::from_str(&res.text().await).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    // Test `/api/operations` API.
    let res = client
        .get("/v1/jaeger/api/operations?service=test-jaeger-query-api&start=1738726754492421&end=1738726754642422")
        .send()
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
    {
        "data": [
            {
                "name": "access-mysql",
                "spanKind": "server"
            },
            {
                "name": "access-redis",
                "spanKind": "server"
            }
        ],
        "total": 2,
        "limit": 0,
        "offset": 0,
        "errors": []
    }
    "#;
    let resp: Value = serde_json::from_str(&res.text().await).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    // Test `/api/services/{service_name}/operations` API.
    let res = client
        .get("/v1/jaeger/api/services/test-jaeger-query-api/operations?start=1738726754492421&end=1738726754642422")
        .send()
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
    {
        "data": [
            "access-mysql",
            "access-redis"
        ],
        "total": 2,
        "limit": 0,
        "offset": 0,
        "errors": []
    }
    "#;
    let resp: Value = serde_json::from_str(&res.text().await).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    // Test `/api/traces/{trace_id}` API.
    let res = client
        .get("/v1/jaeger/api/traces/5611dce1bc9ebed65352d99a027b08ea")
        .send()
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
{
  "data": [
    {
      "traceID": "5611dce1bc9ebed65352d99a027b08ea",
      "spans": [
        {
          "traceID": "5611dce1bc9ebed65352d99a027b08ea",
          "spanID": "ffa03416a7b9ea48",
          "operationName": "access-redis",
          "references": [],
          "startTime": 1738726754492422,
          "duration": 100000,
          "tags": [
            {
              "key": "net.peer.ip",
              "type": "string",
              "value": "1.2.3.4"
            },
            {
              "key": "operation.type",
              "type": "string",
              "value": "access-redis"
            },
            {
              "key": "otel.scope.name",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "otel.scope.version",
              "type": "string",
              "value": "1.0.0"
            },
            {
              "key": "peer.service",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "span.kind",
              "type": "string",
              "value": "server"
            }
          ],
          "logs": [],
          "processID": "p1"
        },
        {
          "traceID": "5611dce1bc9ebed65352d99a027b08ea",
          "spanID": "008421dbbd33a3e9",
          "operationName": "access-mysql",
          "references": [],
          "startTime": 1738726754492421,
          "duration": 100000,
          "tags": [
            {
              "key": "net.peer.ip",
              "type": "string",
              "value": "1.2.3.4"
            },
            {
              "key": "operation.type",
              "type": "string",
              "value": "access-mysql"
            },
            {
              "key": "otel.scope.name",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "otel.scope.version",
              "type": "string",
              "value": "1.0.0"
            },
            {
              "key": "peer.service",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "span.kind",
              "type": "string",
              "value": "server"
            }
          ],
          "logs": [],
          "processID": "p1"
        }
      ],
      "processes": {
        "p1": {
          "serviceName": "test-jaeger-query-api",
          "tags": []
        }
      }
    }
  ],
  "total": 0,
  "limit": 0,
  "offset": 0,
  "errors": []
}
    "#;

    let resp_txt = &res.text().await;
    let resp: Value = serde_json::from_str(resp_txt).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    // Test `/api/traces` API.
    let res = client
        .get("/v1/jaeger/api/traces?service=test-jaeger-query-api&operation=access-mysql&start=1738726754492421&end=1738726754642422&tags=%7B%22operation.type%22%3A%22access-mysql%22%7D")
        .send()
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
{
  "data": [
    {
      "traceID": "5611dce1bc9ebed65352d99a027b08ea",
      "spans": [
        {
          "traceID": "5611dce1bc9ebed65352d99a027b08ea",
          "spanID": "008421dbbd33a3e9",
          "operationName": "access-mysql",
          "references": [],
          "startTime": 1738726754492421,
          "duration": 100000,
          "tags": [
            {
              "key": "net.peer.ip",
              "type": "string",
              "value": "1.2.3.4"
            },
            {
              "key": "operation.type",
              "type": "string",
              "value": "access-mysql"
            },
            {
              "key": "otel.scope.name",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "otel.scope.version",
              "type": "string",
              "value": "1.0.0"
            },
            {
              "key": "peer.service",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "span.kind",
              "type": "string",
              "value": "server"
            }
          ],
          "logs": [],
          "processID": "p1"
        }
      ],
      "processes": {
        "p1": {
          "serviceName": "test-jaeger-query-api",
          "tags": []
        }
      }
    }
  ],
  "total": 0,
  "limit": 0,
  "offset": 0,
  "errors": []
}
    "#;

    let resp_txt = &res.text().await;
    let resp: Value = serde_json::from_str(resp_txt).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    guard.remove_all().await;
}

pub async fn test_jaeger_query_api_for_trace_v1(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) =
        setup_test_http_app_with_frontend(store_type, "test_jaeger_query_api_v1").await;

    let client = TestClient::new(app).await;

    // Test empty response for `/api/services` API before writing any traces.
    let res = client.get("/v1/jaeger/api/services").send().await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
    {
        "data": null,
        "total": 0,
        "limit": 0,
        "offset": 0,
        "errors": []
    }
    "#;
    let resp: Value = serde_json::from_str(&res.text().await).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    let content = r#"
    {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {
                                "stringValue": "test-jaeger-query-api"
                            }
                        }
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {
                        "name": "test-jaeger-query-api",
                        "version": "1.0.0"
                        },
                        "spans": [
                            {
                                "traceId": "5611dce1bc9ebed65352d99a027b08ea",
                                "spanId": "008421dbbd33a3e9",
                                "name": "access-mysql",
                                "kind": 2,
                                "startTimeUnixNano": "1738726754492421000",
                                "endTimeUnixNano": "1738726754592421000",
                                "attributes": [
                                    {
                                        "key": "operation.type",
                                        "value": {
                                        "stringValue": "access-mysql"
                                        }
                                    },
                                    {
                                        "key": "net.peer.ip",
                                        "value": {
                                        "stringValue": "1.2.3.4"
                                        }
                                    },
                                    {
                                        "key": "peer.service",
                                        "value": {
                                        "stringValue": "test-jaeger-query-api"
                                        }
                                    }
                                ],
                                "status": {
                                    "message": "success",
                                    "code": 0
                                }
                            }
                        ]
                    },
                    {
                        "scope": {
                        "name": "test-jaeger-query-api",
                        "version": "1.0.0"
                        },
                        "spans": [
                            {
                                "traceId": "5611dce1bc9ebed65352d99a027b08ea",
                                "spanId": "ffa03416a7b9ea48",
                                "name": "access-redis",
                                "kind": 2,
                                "startTimeUnixNano": "1738726754492422000",
                                "endTimeUnixNano": "1738726754592422000",
                                "attributes": [
                                    {
                                        "key": "operation.type",
                                        "value": {
                                        "stringValue": "access-redis"
                                        }
                                    },
                                    {
                                        "key": "net.peer.ip",
                                        "value": {
                                        "stringValue": "1.2.3.4"
                                        }
                                    },
                                    {
                                        "key": "peer.service",
                                        "value": {
                                        "stringValue": "test-jaeger-query-api"
                                        }
                                    }
                                ],
                                "status": {
                                    "message": "success",
                                    "code": 0
                                }
                            }
                        ]
                    },
                    {
                        "scope": {
                        "name": "test-jaeger-get-operations",
                        "version": "1.0.0"
                        },
                        "spans": [
                            {
                                "traceId": "5611dce1bc9ebed65352d99a027b08ff",
                                "spanId": "ffa03416a7b9ea48",
                                "name": "access-pg",
                                "kind": 2,
                                "startTimeUnixNano": "1738726754492422000",
                                "endTimeUnixNano": "1738726754592422000",
                                "attributes": [
                                    {
                                        "key": "operation.type",
                                        "value": {
                                        "stringValue": "access-pg"
                                        }
                                    }
                                ],
                                "status": {
                                    "message": "success",
                                    "code": 0
                                }
                            }
                        ]
                    }
                ],
                "schemaUrl": "https://opentelemetry.io/schemas/1.4.0"
            }
        ]
    }
    "#;

    let mut req: ExportTraceServiceRequest = serde_json::from_str(content).unwrap();
    // Modify timestamp fields
    let now = Utc::now().timestamp_nanos_opt().unwrap() as u64;
    for span in req.resource_spans.iter_mut() {
        for scope_span in span.scope_spans.iter_mut() {
            // Only modify the timestamp fields for the span with the name "test-jaeger-get-operations" to current time.
            if scope_span.scope.as_ref().unwrap().name == "test-jaeger-get-operations" {
                for span in scope_span.spans.iter_mut() {
                    span.start_time_unix_nano = now - 5_000_000_000; // 5 seconds ago
                    span.end_time_unix_nano = now;
                }
            }
        }
    }
    let body = req.encode_to_vec();

    let trace_table_name = "mytable";
    // write traces data.
    let res = send_req(
        &client,
        vec![
            (
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/x-protobuf"),
            ),
            (
                HeaderName::from_static("x-greptime-log-pipeline-name"),
                HeaderValue::from_static(GREPTIME_INTERNAL_TRACE_PIPELINE_V1_NAME),
            ),
            (
                HeaderName::from_static("x-greptime-trace-table-name"),
                HeaderValue::from_static(trace_table_name),
            ),
        ],
        "/v1/otlp/v1/traces",
        body.clone(),
        false,
    )
    .await;
    assert_eq!(StatusCode::OK, res.status());

    // Test `/api/services` API.
    let res = client
        .get("/v1/jaeger/api/services")
        .header("x-greptime-trace-table-name", trace_table_name)
        .send()
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
    {
        "data": [
            "test-jaeger-query-api"
        ],
        "total": 1,
        "limit": 0,
        "offset": 0,
        "errors": []
    }
    "#;
    let resp: Value = serde_json::from_str(&res.text().await).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    // Test `/api/operations` API.
    let res = client
        .get("/v1/jaeger/api/operations?service=test-jaeger-query-api")
        .header("x-greptime-trace-table-name", trace_table_name)
        .header(JAEGER_TIME_RANGE_FOR_OPERATIONS_HEADER, "3 days")
        .send()
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
    {
        "data": [
            {
                "name": "access-pg",
                "spanKind": "server"
            }
        ],
        "total": 1,
        "limit": 0,
        "offset": 0,
        "errors": []
    }
    "#;
    let resp: Value = serde_json::from_str(&res.text().await).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    // Test `/api/services/{service_name}/operations` API.
    let res = client
        .get("/v1/jaeger/api/services/test-jaeger-query-api/operations?start=1738726754492421&end=1738726754642422")
        .header("x-greptime-trace-table-name", trace_table_name)
        .send()
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
    {
        "data": [
            "access-mysql",
            "access-redis"
        ],
        "total": 2,
        "limit": 0,
        "offset": 0,
        "errors": []
    }
    "#;
    let resp: Value = serde_json::from_str(&res.text().await).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    // Test `/api/traces/{trace_id}` API.
    let res = client
        .get("/v1/jaeger/api/traces/5611dce1bc9ebed65352d99a027b08ea")
        .header("x-greptime-trace-table-name", trace_table_name)
        .send()
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"{
  "data": [
    {
      "traceID": "5611dce1bc9ebed65352d99a027b08ea",
      "spans": [
        {
          "traceID": "5611dce1bc9ebed65352d99a027b08ea",
          "spanID": "ffa03416a7b9ea48",
          "operationName": "access-redis",
          "references": [],
          "startTime": 1738726754492422,
          "duration": 100000,
          "tags": [
            {
              "key": "net.peer.ip",
              "type": "string",
              "value": "1.2.3.4"
            },
            {
              "key": "operation.type",
              "type": "string",
              "value": "access-redis"
            },
            {
              "key": "otel.scope.name",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "otel.scope.version",
              "type": "string",
              "value": "1.0.0"
            },
            {
              "key": "peer.service",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "span.kind",
              "type": "string",
              "value": "server"
            }
          ],
          "logs": [],
          "processID": "p1"
        },
        {
          "traceID": "5611dce1bc9ebed65352d99a027b08ea",
          "spanID": "008421dbbd33a3e9",
          "operationName": "access-mysql",
          "references": [],
          "startTime": 1738726754492421,
          "duration": 100000,
          "tags": [
            {
              "key": "net.peer.ip",
              "type": "string",
              "value": "1.2.3.4"
            },
            {
              "key": "operation.type",
              "type": "string",
              "value": "access-mysql"
            },
            {
              "key": "otel.scope.name",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "otel.scope.version",
              "type": "string",
              "value": "1.0.0"
            },
            {
              "key": "peer.service",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "span.kind",
              "type": "string",
              "value": "server"
            }
          ],
          "logs": [],
          "processID": "p1"
        }
      ],
      "processes": {
        "p1": {
          "serviceName": "test-jaeger-query-api",
          "tags": []
        }
      }
    }
  ],
  "total": 0,
  "limit": 0,
  "offset": 0,
  "errors": []
}
"#;

    let resp: Value = serde_json::from_str(&res.text().await).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    // Test `/api/traces` API.
    let res = client
        .get("/v1/jaeger/api/traces?service=test-jaeger-query-api&operation=access-mysql&start=1738726754492421&end=1738726754642422&tags=%7B%22operation.type%22%3A%22access-mysql%22%7D")
        .header("x-greptime-trace-table-name", trace_table_name)
        .send()
        .await;
    assert_eq!(StatusCode::OK, res.status());
    let expected = r#"
{
  "data": [
    {
      "traceID": "5611dce1bc9ebed65352d99a027b08ea",
      "spans": [
        {
          "traceID": "5611dce1bc9ebed65352d99a027b08ea",
          "spanID": "008421dbbd33a3e9",
          "operationName": "access-mysql",
          "references": [],
          "startTime": 1738726754492421,
          "duration": 100000,
          "tags": [
            {
              "key": "net.peer.ip",
              "type": "string",
              "value": "1.2.3.4"
            },
            {
              "key": "operation.type",
              "type": "string",
              "value": "access-mysql"
            },
            {
              "key": "otel.scope.name",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "otel.scope.version",
              "type": "string",
              "value": "1.0.0"
            },
            {
              "key": "peer.service",
              "type": "string",
              "value": "test-jaeger-query-api"
            },
            {
              "key": "span.kind",
              "type": "string",
              "value": "server"
            }
          ],
          "logs": [],
          "processID": "p1"
        }
      ],
      "processes": {
        "p1": {
          "serviceName": "test-jaeger-query-api",
          "tags": []
        }
      }
    }
  ],
  "total": 0,
  "limit": 0,
  "offset": 0,
  "errors": []
}
    "#;

    let resp: Value = serde_json::from_str(&res.text().await).unwrap();
    let expected: Value = serde_json::from_str(expected).unwrap();
    assert_eq!(resp, expected);

    guard.remove_all().await;
}

async fn validate_data(test_name: &str, client: &TestClient, sql: &str, expected: &str) {
    let res = client
        .get(format!("/v1/sql?sql={sql}").as_str())
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK, "validate {test_name} fail");
    let resp = res.text().await;
    let v = get_rows_from_output(&resp);

    assert_eq!(
        expected, v,
        "validate {test_name} fail, expected: {expected}, actual: {v}"
    );
}

async fn send_req(
    client: &TestClient,
    headers: Vec<(HeaderName, HeaderValue)>,
    path: &str,
    body: Vec<u8>,
    with_gzip: bool,
) -> TestResponse {
    let mut req = client.post(path);

    for (k, v) in headers {
        req = req.header(k, v);
    }

    let mut len = body.len();

    if with_gzip {
        let encoded = compress_vec_with_gzip(body);
        len = encoded.len();
        req = req.header("content-encoding", "gzip").body(encoded);
    } else {
        req = req.body(body);
    }

    req.header("content-length", len).send().await
}

fn get_rows_from_output(output: &str) -> String {
    let resp: Value = serde_json::from_str(output).unwrap();
    resp.get("output")
        .and_then(Value::as_array)
        .and_then(|v| v.first())
        .and_then(|v| v.get("records"))
        .and_then(|v| v.get("rows"))
        .unwrap()
        .to_string()
}

fn compress_vec_with_gzip(data: Vec<u8>) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&data).unwrap();
    encoder.finish().unwrap()
}
