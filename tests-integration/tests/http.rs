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
use common_error::status_code::StatusCode as ErrorCode;
use flate2::write::GzEncoder;
use flate2::Compression;
use loki_api::logproto::{EntryAdapter, PushRequest, StreamAdapter};
use loki_api::prost_types::Timestamp;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use prost::Message;
use serde_json::{json, Value};
use servers::http::handler::HealthResponse;
use servers::http::header::constants::GREPTIME_LOG_TABLE_NAME_HEADER_NAME;
use servers::http::header::{GREPTIME_DB_HEADER_NAME, GREPTIME_TIMEZONE_HEADER_NAME};
use servers::http::prometheus::{PrometheusJsonResponse, PrometheusResponse};
use servers::http::result::error_result::ErrorResponse;
use servers::http::result::greptime_result_v1::GreptimedbV1Response;
use servers::http::result::influxdb_result_v1::{InfluxdbOutput, InfluxdbV1Response};
use servers::http::test_helpers::{TestClient, TestResponse};
use servers::http::GreptimeQueryOutput;
use servers::prom_store;
use tests_integration::test_util::{
    setup_test_http_app, setup_test_http_app_with_frontend,
    setup_test_http_app_with_frontend_and_user_provider, setup_test_prom_app_with_frontend,
    StorageType,
};

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
                test_scripts_api,
                test_health_api,
                test_status_api,
                test_config_api,
                test_dashboard_path,
                test_prometheus_remote_write,
                test_vm_proto_remote_write,

                test_pipeline_api,
                test_test_pipeline_api,
                test_plain_text_ingestion,
                test_identify_pipeline,

                test_otlp_metrics,
                test_otlp_traces,
                test_otlp_logs,
                test_loki_logs,
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
    let client = TestClient::new(app);

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

pub async fn test_sql_api(store_type: StorageType) {
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "sql_api").await;
    let client = TestClient::new(app);
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

    // test parse method
    let res = client.get("/v1/sql/parse?sql=desc table t").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.text().await,
        "[{\"DescribeTable\":{\"name\":[{\"value\":\"t\",\"quote_style\":null}]}}]"
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
    let client = TestClient::new(app);

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
    let client = TestClient::new(app);

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
        serde_json::from_value::<PrometheusResponse>(json!(["__name__", "host", "number",]))
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
        serde_json::from_value::<PrometheusResponse>(json!(["cpu", "memory"])).unwrap()
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

    // query `__name__` without match[]
    let res = client
        .get("/v1/prometheus/api/v1/label/__name__/values")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let prom_resp = res.json::<PrometheusJsonResponse>().await;
    assert_eq!(prom_resp.status, "success");
    assert!(prom_resp.error.is_none());
    assert!(prom_resp.error_type.is_none());

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
    let expected = "{\"status\":\"error\",\"data\":{\"resultType\":\"\",\"result\":[]},\"error\":\"invalid promql query\",\"errorType\":\"InvalidArguments\"}";
    assert_eq!(expected, data);

    guard.remove_all().await;
}

pub async fn test_metrics_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app(store_type, "metrics_api").await;
    let client = TestClient::new(app);

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

pub async fn test_scripts_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "script_api").await;
    let client = TestClient::new(app);

    let res = client
        .post("/v1/scripts?db=schema_test&name=test")
        .body(
            r#"
@copr(sql='select number from numbers limit 10', args=['number'], returns=['n'])
def test(n) -> vector[f64]:
    return n + 1;
"#,
        )
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    assert!(body.output().is_empty());

    // call script
    let res = client
        .post("/v1/run-script?db=schema_test&name=test")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<GreptimedbV1Response>(&res.text().await).unwrap();
    let output = body.output();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        serde_json::from_value::<GreptimeQueryOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"n","data_type":"Float64"}]},"rows":[[1.0],[2.0],[3.0],[4.0],[5.0],[6.0],[7.0],[8.0],[9.0],[10.0]],"total_rows": 10}
        })).unwrap()
    );

    guard.remove_all().await;
}

pub async fn test_health_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, _guard) = setup_test_http_app_with_frontend(store_type, "health_api").await;
    let client = TestClient::new(app);

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
    let client = TestClient::new(app);

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
    let client = TestClient::new(app);

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
mode = "standalone"
enable_telemetry = true
init_regions_in_background = false
init_regions_parallelism = 16

[http]
addr = "127.0.0.1:4000"
timeout = "30s"
body_limit = "64MiB"
is_strict_mode = false

[grpc]
addr = "127.0.0.1:4001"
hostname = "127.0.0.1"
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

[mysql.tls]
mode = "disable"
cert_path = ""
key_path = ""
watch = false

[postgres]
enable = true
addr = "127.0.0.1:4003"
runtime_size = 2

[postgres.tls]
mode = "disable"
cert_path = ""
key_path = ""
watch = false

[opentsdb]
enable = true

[influxdb]
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
file_size = "256MiB"
purge_threshold = "4GiB"

[procedure]
max_retry_times = 3
retry_delay = "500ms"

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
enable_experimental_write_cache = false
experimental_write_cache_path = ""
experimental_write_cache_size = "5GiB"
sst_write_buffer_size = "8MiB"
parallel_scan_channel_size = 32
allow_stale_entries = false
min_compaction_interval = "0s"

[region_engine.mito.index]
aux_path = ""
staging_size = "2GiB"
write_buffer_size = "8MiB"

[region_engine.mito.inverted_index]
create_on_flush = "auto"
create_on_compaction = "auto"
apply_on_query = "auto"
mem_threshold_on_create = "auto"
content_cache_page_size = "8MiB"

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
    similar_asserts::assert_eq!(body_text, expected_toml_str);
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
    let client = TestClient::new(app);

    let res_post = client.post("/dashboard").send().await;
    assert_eq!(res_post.status(), StatusCode::OK);
    let res_get = client.get("/dashboard").send().await;
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
    let client = TestClient::new(app);

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
    let client = TestClient::new(app);
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
    let client = TestClient::new(app);

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
        .post("/v1/events/pipelines/greptime_guagua")
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
        .post("/v1/events/pipelines/test")
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
        .post("/v1/events/logs?db=public&table=logs1&pipeline_name=test")
        .header("Content-Type", "application/json")
        .body(data_body)
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let encoded: String = url::form_urlencoded::byte_serialize(version_str.as_bytes()).collect();

    // 3. remove pipeline
    let res = client
        .delete(format!("/v1/events/pipelines/test?version={}", encoded).as_str())
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

    // 4. write data failed
    let res = client
        .post("/v1/events/logs?db=public&table=logs1&pipeline_name=test")
        .header("Content-Type", "application/json")
        .body(data_body)
        .send()
        .await;
    // todo(shuiyisong): refactor http error handling
    assert_ne!(res.status(), StatusCode::OK);

    guard.remove_all().await;
}

pub async fn test_identify_pipeline(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_pipeline_api").await;

    // handshake
    let client = TestClient::new(app);
    let body = r#"{"__time__":1453809242,"__topic__":"","__source__":"10.170.***.***","ip":"10.200.**.***","time":"26/Jan/2016:19:54:02 +0800","url":"POST/PutData?Category=YunOsAccountOpLog&AccessKeyId=<yourAccessKeyId>&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=<yourSignature>HTTP/1.1","status":"200","user-agent":"aliyun-sdk-java"}
{"__time__":1453809242,"__topic__":"","__source__":"10.170.***.***","ip":"10.200.**.***","time":"26/Jan/2016:19:54:02 +0800","url":"POST/PutData?Category=YunOsAccountOpLog&AccessKeyId=<yourAccessKeyId>&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=<yourSignature>HTTP/1.1","status":"200","user-agent":"aliyun-sdk-java","hasagei":"hasagei","dongdongdong":"guaguagua"}"#;
    let res = client
        .post("/v1/events/logs?db=public&table=logs&pipeline_name=greptime_identity")
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

    let line1_expected = r#"["10.170.***.***",1453809242,"","10.200.**.***","200","26/Jan/2016:19:54:02 +0800","POST/PutData?Category=YunOsAccountOpLog&AccessKeyId=<yourAccessKeyId>&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=<yourSignature>HTTP/1.1","aliyun-sdk-java","guaguagua","hasagei",null]"#;
    let line2_expected = r#"["10.170.***.***",1453809242,"","10.200.**.***","200","26/Jan/2016:19:54:02 +0800","POST/PutData?Category=YunOsAccountOpLog&AccessKeyId=<yourAccessKeyId>&Date=Fri%2C%2028%20Jun%202013%2006%3A53%3A30%20GMT&Topic=raw&Signature=<yourSignature>HTTP/1.1","aliyun-sdk-java",null,null,null]"#;
    let res = client.get("/v1/sql?sql=select * from logs").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let resp: serde_json::Value = res.json().await;
    let result = resp["output"][0]["records"]["rows"].as_array().unwrap();
    assert_eq!(result.len(), 2);
    let mut line1 = result[0].as_array().unwrap().clone();
    let mut line2 = result[1].as_array().unwrap().clone();
    assert!(line1.last().unwrap().is_i64());
    assert!(line2.last().unwrap().is_i64());
    *line1.last_mut().unwrap() = serde_json::Value::Null;
    *line2.last_mut().unwrap() = serde_json::Value::Null;

    assert_eq!(
        line1,
        serde_json::from_str::<Vec<Value>>(line1_expected).unwrap()
    );
    assert_eq!(
        line2,
        serde_json::from_str::<Vec<Value>>(line2_expected).unwrap()
    );

    let expected = r#"[["__source__","String","","YES","","FIELD"],["__time__","Int64","","YES","","FIELD"],["__topic__","String","","YES","","FIELD"],["ip","String","","YES","","FIELD"],["status","String","","YES","","FIELD"],["time","String","","YES","","FIELD"],["url","String","","YES","","FIELD"],["user-agent","String","","YES","","FIELD"],["dongdongdong","String","","YES","","FIELD"],["hasagei","String","","YES","","FIELD"],["greptime_timestamp","TimestampNanosecond","PRI","NO","","TIMESTAMP"]]"#;
    validate_data("identity_schema", &client, "desc logs", expected).await;

    guard.remove_all().await;
}

pub async fn test_test_pipeline_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_pipeline_api").await;

    // handshake
    let client = TestClient::new(app);

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
        .post("/v1/events/pipelines/test")
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
            .post("/v1/events/pipelines/dryrun?pipeline_name=test")
            .header("Content-Type", "application/json")
            .body(data_body)
            .send()
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        let body: Value = res.json().await;
        let schema = &body["schema"];
        let rows = &body["rows"];
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
            .post("/v1/events/pipelines/dryrun")
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        let body: Value = res.json().await;
        let schema = &body["schema"];
        let rows = &body["rows"];
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
            .post("/v1/events/pipelines/dryrun")
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        let body: Value = res.json().await;
        let schema = &body["schema"];
        let rows = &body["rows"];
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
            .post("/v1/events/pipelines/dryrun")
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
    let client = TestClient::new(app);

    let body = r#"
processors:
  - dissect:
      fields:
        - line
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
        .post("/v1/events/pipelines/test")
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
        .post("/v1/events/logs?db=public&table=logs1&pipeline_name=test")
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
{"resource":{"attributes":[],"droppedAttributesCount":0},"scopeMetrics":[{"scope":{"name":"","version":"","attributes":[],"droppedAttributesCount":0},"metrics":[{"name":"gen","description":"","unit":"","data":{"gauge":{"dataPoints":[{"attributes":[],"startTimeUnixNano":0,"timeUnixNano":1726053452870391000,"exemplars":[],"flags":0,"value":{"asInt":9471}}]}}}],"schemaUrl":""}],"schemaUrl":"https://opentelemetry.io/schemas/1.13.0"}
    "#;

    let metrics: ResourceMetrics = serde_json::from_str(content).unwrap();
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![metrics],
    };
    let body = req.encode_to_vec();

    // handshake
    let client = TestClient::new(app);

    // write metrics data
    let res = send_req(&client, vec![], "/v1/otlp/v1/metrics", body.clone(), false).await;
    assert_eq!(StatusCode::OK, res.status());

    // select metrics data
    let expected = r#"[[1726053452870391000,9471.0]]"#;
    validate_data("otlp_metrics", &client, "select * from gen;", expected).await;

    // drop table
    let res = client.get("/v1/sql?sql=drop table gen;").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    // write metrics data with gzip
    let res = send_req(&client, vec![], "/v1/otlp/v1/metrics", body.clone(), true).await;
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

pub async fn test_otlp_traces(store_type: StorageType) {
    // init
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_otlp_traces").await;

    let content = r#"
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"telemetrygen"}}],"droppedAttributesCount":0},"scopeSpans":[{"scope":{"name":"telemetrygen","version":"","attributes":[],"droppedAttributesCount":0},"spans":[{"traceId":"b5e5fb572cf0a3335dd194a14145fef5","spanId":"74c82efa6f628e80","traceState":"","parentSpanId":"3364d2da58c9fd2b","flags":0,"name":"okey-dokey-0","kind":2,"startTimeUnixNano":1726631197820927000,"endTimeUnixNano":1726631197821050000,"attributes":[{"key":"net.peer.ip","value":{"stringValue":"1.2.3.4"}},{"key":"peer.service","value":{"stringValue":"telemetrygen-client"}}],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"links":[],"droppedLinksCount":0,"status":{"message":"","code":0}},{"traceId":"b5e5fb572cf0a3335dd194a14145fef5","spanId":"3364d2da58c9fd2b","traceState":"","parentSpanId":"","flags":0,"name":"lets-go","kind":3,"startTimeUnixNano":1726631197820927000,"endTimeUnixNano":1726631197821050000,"attributes":[{"key":"net.peer.ip","value":{"stringValue":"1.2.3.4"}},{"key":"peer.service","value":{"stringValue":"telemetrygen-server"}}],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"links":[],"droppedLinksCount":0,"status":{"message":"","code":0}}],"schemaUrl":""}],"schemaUrl":"https://opentelemetry.io/schemas/1.4.0"}]}
    "#;

    let req: ExportTraceServiceRequest = serde_json::from_str(content).unwrap();
    let body = req.encode_to_vec();

    // handshake
    let client = TestClient::new(app);

    // write traces data
    let res = send_req(&client, vec![], "/v1/otlp/v1/traces", body.clone(), false).await;
    assert_eq!(StatusCode::OK, res.status());

    // select traces data
    let expected = r#"[[1726631197820927000,1726631197821050000,123000,"b5e5fb572cf0a3335dd194a14145fef5","3364d2da58c9fd2b","","SPAN_KIND_CLIENT","lets-go","STATUS_CODE_UNSET","","",{"net.peer.ip":"1.2.3.4","peer.service":"telemetrygen-server"},[],[],"telemetrygen","",{},{"service.name":"telemetrygen"}],[1726631197820927000,1726631197821050000,123000,"b5e5fb572cf0a3335dd194a14145fef5","74c82efa6f628e80","3364d2da58c9fd2b","SPAN_KIND_SERVER","okey-dokey-0","STATUS_CODE_UNSET","","",{"net.peer.ip":"1.2.3.4","peer.service":"telemetrygen-client"},[],[],"telemetrygen","",{},{"service.name":"telemetrygen"}]]"#;
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
    let res = send_req(&client, vec![], "/v1/otlp/v1/traces", body.clone(), true).await;
    assert_eq!(StatusCode::OK, res.status());

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

pub async fn test_otlp_logs(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_otlp_logs").await;

    let client = TestClient::new(app);
    let content = r#"
{"resourceLogs":[{"resource":{"attributes":[{"key":"resource-attr","value":{"stringValue":"resource-attr-val-1"}}]},"schemaUrl":"https://opentelemetry.io/schemas/1.0.0/resourceLogs","scopeLogs":[{"scope":{},"schemaUrl":"https://opentelemetry.io/schemas/1.0.0/scopeLogs","logRecords":[{"flags":1,"timeUnixNano":1581452773000009875,"observedTimeUnixNano":1581452773000009875,"severityNumber":9,"severityText":"Info","body":{"value":{"stringValue":"This is a log message"}},"attributes":[{"key":"app","value":{"stringValue":"server"}},{"key":"instance_num","value":{"intValue":1}}],"droppedAttributesCount":1,"traceId":[48,56,48,52,48,50,48,49,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48],"spanId":[48,49,48,50,48,52,48,56,48,48,48,48,48,48,48,48]},{"flags":1,"timeUnixNano":1581452773000000789,"observedTimeUnixNano":1581452773000000789,"severityNumber":9,"severityText":"Info","body":{"value":{"stringValue":"something happened"}},"attributes":[{"key":"customer","value":{"stringValue":"acme"}},{"key":"env","value":{"stringValue":"dev"}}],"droppedAttributesCount":1,"traceId":[48],"spanId":[48]}]}]}]}
"#;

    let req: ExportLogsServiceRequest = serde_json::from_str(content).unwrap();
    let body = req.encode_to_vec();

    {
        // write log data
        let res = send_req(
            &client,
            vec![(
                HeaderName::from_static("x-greptime-log-table-name"),
                HeaderValue::from_static("logs1"),
            )],
            "/v1/otlp/v1/logs?db=public",
            body.clone(),
            false,
        )
        .await;
        assert_eq!(StatusCode::OK, res.status());
        let expected = r#"[[1581452773000000789,"30","30","Info",9,"something happened",{"customer":"acme","env":"dev"},1,"","",{},"https://opentelemetry.io/schemas/1.0.0/scopeLogs",{"resource-attr":"resource-attr-val-1"},"https://opentelemetry.io/schemas/1.0.0/resourceLogs"],[1581452773000009875,"3038303430323031303030303030303030303030303030303030303030303030","30313032303430383030303030303030","Info",9,"This is a log message",{"app":"server","instance_num":1},1,"","",{},"https://opentelemetry.io/schemas/1.0.0/scopeLogs",{"resource-attr":"resource-attr-val-1"},"https://opentelemetry.io/schemas/1.0.0/resourceLogs"]]"#;
        validate_data("otlp_logs", &client, "select * from logs1;", expected).await;
    }

    {
        // write log data with selector
        let res = send_req(
            &client,
            vec![
                (
                    HeaderName::from_static("x-greptime-log-table-name"),
                    HeaderValue::from_static("logs"),
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

        let expected = r#"[[1581452773000000789,"30","30","Info",9,"something happened",{"customer":"acme","env":"dev"},1,"","",{},"https://opentelemetry.io/schemas/1.0.0/scopeLogs",{"resource-attr":"resource-attr-val-1"},"https://opentelemetry.io/schemas/1.0.0/resourceLogs",null,null,"resource-attr-val-1"],[1581452773000009875,"3038303430323031303030303030303030303030303030303030303030303030","30313032303430383030303030303030","Info",9,"This is a log message",{"app":"server","instance_num":1},1,"","",{},"https://opentelemetry.io/schemas/1.0.0/scopeLogs",{"resource-attr":"resource-attr-val-1"},"https://opentelemetry.io/schemas/1.0.0/resourceLogs","server",1,"resource-attr-val-1"]]"#;
        validate_data(
            "otlp_logs_with_selector",
            &client,
            "select * from logs;",
            expected,
        )
        .await;
    }

    guard.remove_all().await;
}

pub async fn test_loki_logs(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "test_loke_logs").await;

    let client = TestClient::new(app);

    // init loki request
    let req: PushRequest = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{service="test",source="integration","wadaxi"="do anything"}"#.to_string(),
            entries: vec![
                EntryAdapter {
                    timestamp: Some(Timestamp::from_str("2024-11-07T10:53:50").unwrap()),
                    line: "this is a log message".to_string(),
                },
                EntryAdapter {
                    timestamp: Some(Timestamp::from_str("2024-11-07T10:53:50").unwrap()),
                    line: "this is a log message".to_string(),
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
    let expected = "[[\"loki_table_name\",\"CREATE TABLE IF NOT EXISTS \\\"loki_table_name\\\" (\\n  \\\"greptime_timestamp\\\" TIMESTAMP(9) NOT NULL,\\n  \\\"line\\\" STRING NULL,\\n  \\\"service\\\" STRING NULL,\\n  \\\"source\\\" STRING NULL,\\n  \\\"wadaxi\\\" STRING NULL,\\n  TIME INDEX (\\\"greptime_timestamp\\\"),\\n  PRIMARY KEY (\\\"service\\\", \\\"source\\\", \\\"wadaxi\\\")\\n)\\n\\nENGINE=mito\\nWITH(\\n  append_mode = 'true'\\n)\"]]";
    validate_data(
        "loki_schema",
        &client,
        "show create table loki_table_name;",
        expected,
    )
    .await;

    // test content
    let expected = r#"[[1730976830000000000,"this is a log message","test","integration","do anything"],[1730976830000000000,"this is a log message","test","integration","do anything"]]"#;
    validate_data(
        "loki_content",
        &client,
        "select * from loki_table_name;",
        expected,
    )
    .await;

    guard.remove_all().await;
}

async fn validate_data(test_name: &str, client: &TestClient, sql: &str, expected: &str) {
    let res = client
        .get(format!("/v1/sql?sql={sql}").as_str())
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let resp = res.text().await;
    let v = get_rows_from_output(&resp);

    assert_eq!(v, expected, "validate {test_name} fail");
}

async fn send_req(
    client: &TestClient,
    headers: Vec<(HeaderName, HeaderValue)>,
    path: &str,
    body: Vec<u8>,
    with_gzip: bool,
) -> TestResponse {
    let mut req = client
        .post(path)
        .header("content-type", "application/x-protobuf");

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
