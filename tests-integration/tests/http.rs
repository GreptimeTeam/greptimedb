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

use auth::user_provider_from_option;
use axum::http::StatusCode;
use axum_test_helper::TestClient;
use common_error::status_code::StatusCode as ErrorCode;
use serde_json::json;
use servers::http::error_result::ErrorResponse;
use servers::http::greptime_result_v1::GreptimedbV1Response;
use servers::http::handler::HealthResponse;
use servers::http::influxdb_result_v1::{InfluxdbOutput, InfluxdbV1Response};
use servers::http::prometheus::{PrometheusJsonResponse, PrometheusResponse};
use servers::http::GreptimeQueryOutput;
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
    assert_eq!(res.status(), StatusCode::OK);

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
            "records" :{"schema":{"column_schemas":[{"name":"number","data_type":"UInt32"}]},"rows":[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]}
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
            "records":{"schema":{"column_schemas":[{"name":"host","data_type":"String"},{"name":"cpu","data_type":"Float64"},{"name":"memory","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[["host",66.6,1024.0,0]]}
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
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]]}
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
            "records":{"schema":{"column_schemas":[{"name":"c","data_type":"Float64"},{"name":"time","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]]}
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
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]]}
        })).unwrap()
    );
    assert_eq!(
        outputs[1],
        serde_json::from_value::<GreptimeQueryOutput>(json!({
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

    let _body = serde_json::from_str::<ErrorResponse>(&res.text().await).unwrap();
    // TODO(shuiyisong): fix this when return source err msg to client side
    // assert!(body.error().contains("Table not found"));

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
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]]}
        })).unwrap()
    );

    // test database not found
    let res = client
        .get("/v1/sql?db=notfound&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
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
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"TimestampMillisecond"}]},"rows":[[66.6,0]]}
        })).unwrap()
    );

    // test invalid catalog
    let res = client
        .get("/v1/sql?db=notfound2-schema&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<ErrorResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), ErrorCode::DatabaseNotFound as u32);

    // test invalid schema
    let res = client
        .get("/v1/sql?db=greptime-schema&sql=select cpu, ts from demo limit 1")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<ErrorResponse>(&res.text().await).unwrap();
    assert_eq!(body.code(), ErrorCode::DatabaseNotFound as u32);

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
        serde_json::from_value::<PrometheusResponse>(json!([
            "__name__", "cpu", "host", "memory", "ts"
        ]))
        .unwrap()
    );

    // labels without match[] param
    let res = client.get("/v1/prometheus/api/v1/labels").send().await;
    assert_eq!(res.status(), StatusCode::OK);

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
        .get("/v1/prometheus/api/v1/series?match[]=demo&start=0&end=0")
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
        ("ts".to_string(), "1970-01-01 00:00:00+0000".to_string()),
        ("cpu".to_string(), "1.1".to_string()),
        ("host".to_string(), "host1".to_string()),
        ("memory".to_string(), "2.2".to_string()),
    ]);
    assert_eq!(actual, expected);

    let res = client
        .post("/v1/prometheus/api/v1/series?match[]=up&match[]=down")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // label values
    // should return error if there is no match[]
    let res = client
        .get("/v1/prometheus/api/v1/label/instance/values")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    let prom_resp = res.json::<PrometheusJsonResponse>().await;
    assert_eq!(prom_resp.status, "error");
    assert!(prom_resp.error.is_some_and(|err| !err.is_empty()));
    assert!(prom_resp.error_type.is_some_and(|err| !err.is_empty()));

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
[procedure]
max_retry_times = 3
retry_delay = "500ms"

[metadata_store]
file_size = "256MiB"
purge_threshold = "4GiB"

[frontend]
mode = "standalone"

[frontend.heartbeat]
interval = "18s"
retry_interval = "3s"

[frontend.http]
addr = "127.0.0.1:4000"
timeout = "30s"
body_limit = "64MiB"

[frontend.grpc]
addr = "127.0.0.1:4001"
runtime_size = 8
max_recv_message_size = "512MiB"
max_send_message_size = "512MiB"

[frontend.mysql]
enable = true
addr = "127.0.0.1:4002"
runtime_size = 2

[frontend.mysql.tls]
mode = "disable"
cert_path = ""
key_path = ""

[frontend.postgres]
enable = true
addr = "127.0.0.1:4003"
runtime_size = 2

[frontend.postgres.tls]
mode = "disable"
cert_path = ""
key_path = ""

[frontend.opentsdb]
enable = true
addr = "127.0.0.1:4242"
runtime_size = 2

[frontend.influxdb]
enable = true

[frontend.prom_store]
enable = true

[frontend.otlp]
enable = true

[frontend.logging]
enable_otlp_tracing = false
append_stdout = true

[frontend.datanode.client]
timeout = "10s"
connect_timeout = "1s"
tcp_nodelay = true

[frontend.export_metrics]
enable = false
write_interval = "30s"

[datanode]
mode = "standalone"
node_id = 0
require_lease_before_startup = true
initialize_region_in_background = false
rpc_addr = "127.0.0.1:3001"
rpc_runtime_size = 8
rpc_max_recv_message_size = "512MiB"
rpc_max_send_message_size = "512MiB"
enable_telemetry = true

[datanode.heartbeat]
interval = "3s"
retry_interval = "3s"

[datanode.http]
addr = "127.0.0.1:4000"
timeout = "30s"
body_limit = "64MiB"

[datanode.wal]
provider = "raft_engine"
file_size = "256MiB"
purge_threshold = "4GiB"
purge_interval = "10m"
read_batch_size = 128
sync_write = false

[datanode.storage]
type = "{}"
providers = []

[[datanode.region_engine]]

[datanode.region_engine.mito]
worker_channel_size = 128
worker_request_batch_size = 64
manifest_checkpoint_distance = 10
compress_manifest = false
max_background_jobs = 4
auto_flush_interval = "30m"
global_write_buffer_size = "1GiB"
global_write_buffer_reject_size = "2GiB"
sst_meta_cache_size = "128MiB"
vector_cache_size = "512MiB"
page_cache_size = "512MiB"
sst_write_buffer_size = "8MiB"
parallel_scan_channel_size = 32

[[datanode.region_engine]]

[datanode.region_engine.file]

[datanode.logging]
enable_otlp_tracing = false
append_stdout = true

[datanode.export_metrics]
enable = false
write_interval = "30s"

[logging]
enable_otlp_tracing = false
append_stdout = true

[wal_meta]
provider = "raft_engine""#,
        store_type,
    );
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
