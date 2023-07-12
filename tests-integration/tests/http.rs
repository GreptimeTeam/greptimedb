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

use axum::http::StatusCode;
use axum_test_helper::TestClient;
use common_error::status_code::StatusCode as ErrorCode;
use serde_json::json;
use servers::http::handler::HealthResponse;
use servers::http::{JsonOutput, JsonResponse};
use servers::prometheus::{PrometheusJsonResponse, PrometheusResponse};
use tests_integration::test_util::{
    setup_test_http_app, setup_test_http_app_with_frontend, setup_test_prom_app_with_frontend,
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

                test_sql_api,
                test_prometheus_promql_api,
                test_prom_http_api,
                test_metrics_api,
                test_scripts_api,
                test_health_api,
                test_config_api,
                test_dashboard_path,
            );
        )*
    };
}

pub async fn test_sql_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "sql_api").await;
    let client = TestClient::new(app);
    let res = client.get("/v1/sql").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), 1004);
    assert_eq!(body.error().unwrap(), "sql parameter is required.");
    let _ = body.execution_time_ms().unwrap();

    let res = client
        .get("/v1/sql?sql=select * from numbers limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert!(body.success());
    let _ = body.execution_time_ms().unwrap();

    let output = body.output().unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records" :{"schema":{"column_schemas":[{"name":"number","data_type":"UInt32"}]},"rows":[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]}
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

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert!(body.success());
    let _ = body.execution_time_ms().unwrap();
    let output = body.output().unwrap();
    assert_eq!(output.len(), 1);

    assert_eq!(
        output[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"host","data_type":"String"},{"name":"cpu","data_type":"Float64"},{"name":"memory","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[["host",66.6,1024.0,0]]}
        })).unwrap()
    );

    // select with projections
    let res = client
        .get("/v1/sql?sql=select cpu, ts from demo limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert!(body.success());
    let _ = body.execution_time_ms().unwrap();
    let output = body.output().unwrap();
    assert_eq!(output.len(), 1);

    assert_eq!(
        output[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]]}
        })).unwrap()
    );

    // select with column alias
    let res = client
        .get("/v1/sql?sql=select cpu as c, ts as time from demo limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert!(body.success());
    let _ = body.execution_time_ms().unwrap();
    let output = body.output().unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"c","data_type":"Float64"},{"name":"time","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]]}
        })).unwrap()
    );

    // test multi-statement
    let res = client
        .get("/v1/sql?sql=select cpu, ts from demo limit 1;select cpu, ts from demo where ts > 0;")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert!(body.success());
    let _ = body.execution_time_ms().unwrap();
    let outputs = body.output().unwrap();
    assert_eq!(outputs.len(), 2);
    assert_eq!(
        outputs[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]]}
        })).unwrap()
    );
    assert_eq!(
        outputs[1],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"rows":[]}
        }))
        .unwrap()
    );

    // test multi-statement with error
    let res = client
        .get("/v1/sql?sql=select cpu, ts from demo limit 1;select cpu, ts from demo2 where ts > 0;")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert!(!body.success());
    let _ = body.execution_time_ms().unwrap();
    assert!(body.error().unwrap().contains("Table not found"));

    // test database given
    let res = client
        .get("/v1/sql?db=public&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert!(body.success());
    let _ = body.execution_time_ms().unwrap();
    let outputs = body.output().unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]]}
        })).unwrap()
    );

    // test database not found
    let res = client
        .get("/v1/sql?db=notfound&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), ErrorCode::DatabaseNotFound as u32);

    // test catalog-schema given
    let res = client
        .get("/v1/sql?db=greptime-public&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert!(body.success());
    let _ = body.execution_time_ms().unwrap();
    let outputs = body.output().unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]]}
        })).unwrap()
    );

    // test invalid catalog
    let res = client
        .get("/v1/sql?db=notfound2-schema&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), ErrorCode::Internal as u32);

    // test invalid schema
    let res = client
        .get("/v1/sql?db=greptime-schema&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), ErrorCode::DatabaseNotFound as u32);

    guard.remove_all().await;
}

pub async fn test_prometheus_promql_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_http_app_with_frontend(store_type, "sql_api").await;
    let client = TestClient::new(app);

    let res = client
        .get("/v1/promql?query=abs(demo{host=\"Hangzhou\"})&start=0&end=100&step=5s")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert!(body.success());
    let _ = body.execution_time_ms().unwrap();

    guard.remove_all().await;
}

pub async fn test_prom_http_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, mut guard) = setup_test_prom_app_with_frontend(store_type, "promql_api").await;
    let client = TestClient::new(app);

    // instant query
    let res = client.get("/api/v1/query?query=up&time=1").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .post("/api/v1/query?query=up&time=1")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // range query
    let res = client
        .get("/api/v1/query_range?query=up&start=1&end=100&step=5")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .post("/api/v1/query_range?query=up&start=1&end=100&step=5")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // labels
    let res = client.get("/api/v1/labels?match[]=demo").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .post("/api/v1/labels?match[]=up")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .get("/api/v1/labels?match[]=demo&start=0")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!([
            "__name__", "cpu", "host", "memory", "ts"
        ]))
        .unwrap()
    );

    // labels query with multiple match[] params
    let res = client
        .get("/api/v1/labels?match[]=up&match[]=down")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = client
        .post("/api/v1/labels?match[]=up&match[]=down")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // series
    let res = client
        .get("/api/v1/series?match[]=demo&start=0&end=0")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<PrometheusJsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.status, "success");
    assert_eq!(
        body.data,
        serde_json::from_value::<PrometheusResponse>(json!(
            [{"__name__" : "demo","ts":"1970-01-01 00:00:00+0000","cpu":"1.1","host":"host1","memory":"2.2"}]
        ))
        .unwrap()
    );

    let res = client
        .post("/api/v1/series?match[]=up&match[]=down")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // label values
    // should return error if there is no match[]
    let res = client.get("/api/v1/label/instance/values").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let prom_resp = res.json::<PrometheusJsonResponse>().await;
    assert_eq!(prom_resp.status, "error");
    assert!(prom_resp.error.is_some_and(|err| !err.is_empty()));
    assert!(prom_resp.error_type.is_some_and(|err| !err.is_empty()));

    // single match[]
    let res = client
        .get("/api/v1/label/host/values?match[]=demo&start=0&end=600")
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
        .get("/api/v1/label/instance/values?match[]=up&match[]=system_metrics")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let prom_resp = res.json::<PrometheusJsonResponse>().await;
    assert_eq!(prom_resp.status, "success");
    assert!(prom_resp.error.is_none());
    assert!(prom_resp.error_type.is_none());

    guard.remove_all().await;
}

pub async fn test_metrics_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    common_telemetry::init_default_metrics_recorder();
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
    assert!(body.contains("frontend_handle_sql_elapsed"));
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

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), 0);
    assert!(body.output().is_none());

    // call script
    let res = client
        .post("/v1/run-script?db=schema_test&name=test")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();

    assert_eq!(body.code(), 0);
    let _ = body.execution_time_ms().unwrap();
    let output = body.output().unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"n","data_type":"Float64"}]},"rows":[[1.0],[2.0],[3.0],[4.0],[5.0],[6.0],[7.0],[8.0],[9.0],[10.0]]}
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

pub async fn test_config_api(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (app, _guard) = setup_test_http_app_with_frontend(store_type, "config_api").await;
    let client = TestClient::new(app);

    let res_get = client.get("/config").send().await;
    assert_eq!(res_get.status(), StatusCode::OK);
    let expected_toml_str = format!(
        r#"
    mode = "standalone"
    enable_memory_catalog = false
    rpc_addr = "127.0.0.1:3001"
    rpc_runtime_size = 8
    heartbeat_interval_millis = 5000

    [http_opts]
    addr = "127.0.0.1:4000"
    timeout = "30s"
    body_limit = "64MiB"

    [wal]
    file_size = "256MiB"
    purge_threshold = "4GiB"
    purge_interval = "10m"
    read_batch_size = 128
    sync_write = false

    [storage]
    type = "{}"

    [storage.compaction]
    max_inflight_tasks = 4
    max_files_in_level0 = 8
    max_purge_tasks = 32
    sst_write_buffer_size = "8MiB"

    [storage.manifest]
    checkpoint_margin = 10
    gc_duration = "10m"
    checkpoint_on_startup = false
    compress = false

    [storage.flush]
    max_flush_tasks = 8
    region_write_buffer_size = "32MiB"
    picker_schedule_interval = "5m"
    auto_flush_interval = "1h"

    [procedure]
    max_retry_times = 3
    retry_delay = "500ms"

    [logging]
    enable_jaeger_tracing = false"#,
        store_type
    );
    let body_text = drop_lines_with_inconsistent_results(res_get.text().await);
    assert_eq!(
        normalize_str(body_text.as_str()),
        normalize_str(&expected_toml_str)
    );
}

fn drop_lines_with_inconsistent_results(input: String) -> String {
    input
        .lines()
        .filter(|line| {
            // ignores
            !line.trim().starts_with("dir =")
                && !line.trim().starts_with("data_home =")
                && !line.trim().starts_with("bucket =")
                && !line.trim().starts_with("root =")
                && !line.trim().starts_with("endpoint =")
                && !line.trim().starts_with("region =")
                && !line.trim().starts_with("cache_path =")
                && !line.trim().starts_with("cache_capacity =")
                && !line.trim().starts_with("sas_token =")
                && !line.trim().starts_with("scope =")
        })
        .collect::<Vec<&str>>()
        .join("\n")
}

fn normalize_str(s: &str) -> String {
    s.replace([' ', '\n'], "")
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
