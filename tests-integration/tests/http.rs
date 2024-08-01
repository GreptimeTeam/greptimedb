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

use api::prom_store::remote::WriteRequest;
use auth::user_provider_from_option;
use axum::http::{HeaderName, StatusCode};
use common_error::status_code::StatusCode as ErrorCode;
use prost::Message;
use serde_json::{json, Value};
use servers::http::error_result::ErrorResponse;
use servers::http::greptime_result_v1::GreptimedbV1Response;
use servers::http::handler::HealthResponse;
use servers::http::header::{GREPTIME_DB_HEADER_NAME, GREPTIME_TIMEZONE_HEADER_NAME};
use servers::http::influxdb_result_v1::{InfluxdbOutput, InfluxdbV1Response};
use servers::http::prometheus::{PrometheusJsonResponse, PrometheusResponse};
use servers::http::test_helpers::TestClient;
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
                test_plain_text_ingestion,
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

    // we can call health api with both `GET` and `POST` method.
    let res_post = client.post("/health").send().await;
    assert_eq!(res_post.status(), StatusCode::OK);
    let res_get = client.get("/health").send().await;
    assert_eq!(res_get.status(), StatusCode::OK);

    // both `GET` and `POST` method return same result
    let body_text = res_post.text().await;
    assert_eq!(body_text, res_get.text().await);

    // currently health api simply returns an empty json `{}`, which can be deserialized to an empty `HealthResponse`
    assert_eq!(body_text, "{}");

    let body = serde_json::from_str::<HealthResponse>(&body_text).unwrap();
    assert_eq!(body, HealthResponse {});
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

    let expected_toml_str = format!(
        r#"
mode = "standalone"
enable_telemetry = true

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
file_size = "256MiB"
purge_threshold = "4GiB"
purge_interval = "10m"
read_batch_size = 128
sync_write = false
enable_log_recycle = true
prefill_log_files = false

[storage]
type = "{}"
providers = []

[metadata_store]
file_size = "256MiB"
purge_threshold = "4GiB"

[procedure]
max_retry_times = 3
retry_delay = "500ms"

[logging]
enable_otlp_tracing = false
append_stdout = true

[[region_engine]]

[region_engine.mito]
worker_channel_size = 128
worker_request_batch_size = 64
manifest_checkpoint_distance = 10
compress_manifest = false
max_background_jobs = 4
auto_flush_interval = "30m"
enable_experimental_write_cache = false
experimental_write_cache_path = ""
experimental_write_cache_size = "512MiB"
experimental_write_cache_ttl = "1h"
sst_write_buffer_size = "8MiB"
parallel_scan_channel_size = 32
allow_stale_entries = false

[region_engine.mito.index]
aux_path = ""
staging_size = "2GiB"
write_buffer_size = "8MiB"

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

[region_engine.mito.memtable]
type = "time_series"

[[region_engine]]

[region_engine.file]

[export_metrics]
enable = false
write_interval = "30s"

[tracing]"#,
        store_type
    )
    .trim()
    .to_string();
    let body_text = drop_lines_with_inconsistent_results(res_get.text().await);
    assert_eq!(body_text, expected_toml_str);
}

fn drop_lines_with_inconsistent_results(input: String) -> String {
    let inconsistent_results = [
        "dir =",
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
  - timestamp:
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
    type: timestamp, ns
    index: time
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
  - timestamp:
      fields:
        - ts
      formats:
        - "%Y-%m-%d %H:%M:%S%.3f"

transform:
  - fields:
      - content
    type: string
  - field: ts
    type: timestamp, ns
    index: time
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

    let resp: Value = serde_json::from_str(&resp).unwrap();
    let v = resp
        .get("output")
        .unwrap()
        .as_array()
        .unwrap()
        .first()
        .unwrap()
        .get("records")
        .unwrap()
        .get("rows")
        .unwrap()
        .to_string();

    assert_eq!(
        v,
        r#"[["hello",1716668197217000000],["hello world",1716668197218000000]]"#,
    );

    guard.remove_all().await;
}
