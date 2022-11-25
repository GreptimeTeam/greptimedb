// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::Router;
use axum_test_helper::TestClient;
use datanode::instance::{Instance, InstanceRef};
use datatypes::prelude::ConcreteDataType;
use frontend::frontend::FrontendOptions;
use frontend::instance::{FrontendInstance, Instance as FeInstance};
use serde_json::json;
use servers::http::{HttpOptions, HttpServer, JsonOutput, JsonResponse};
use test_util::TestGuard;
use tests_integration::test_util;

async fn build_frontend_instance(datanode_instance: InstanceRef) -> FeInstance {
    let fe_opts = FrontendOptions::default();
    let mut frontend_instance = FeInstance::try_new(&fe_opts).await.unwrap();
    frontend_instance.set_catalog_manager(datanode_instance.catalog_manager().clone());
    frontend_instance.set_script_handler(datanode_instance);
    frontend_instance
}

async fn make_test_app(name: &str) -> (Router, TestGuard) {
    let (opts, guard) = test_util::create_tmp_dir_and_datanode_opts(name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    instance.start().await.unwrap();
    test_util::create_test_table(
        instance.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millis_datatype(),
    )
    .await
    .unwrap();
    let http_server = HttpServer::new(instance, HttpOptions::default());
    (http_server.make_app(), guard)
}

async fn make_test_app_with_frontend(name: &str) -> (Router, TestGuard) {
    let (opts, guard) = test_util::create_tmp_dir_and_datanode_opts(name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    let mut frontend = build_frontend_instance(instance.clone()).await;
    instance.start().await.unwrap();
    test_util::create_test_table(
        frontend.catalog_manager().as_ref().unwrap(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millis_datatype(),
    )
    .await
    .unwrap();
    frontend.start().await.unwrap();
    let mut http_server = HttpServer::new(Arc::new(frontend), HttpOptions::default());
    http_server.set_script_handler(instance.clone());
    let app = http_server.make_app();
    (app, guard)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sql_api() {
    common_telemetry::init_default_ut_logging();
    let (app, _guard) = make_test_app("sql_api").await;
    let client = TestClient::new(app);
    let res = client.get("/v1/sql").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    // body json: r#"{"code":1004,"error":"sql parameter is required."}"#
    assert_eq!(body.code(), 1004);
    assert_eq!(body.error().unwrap(), "sql parameter is required.");
    assert!(body.execution_time_ms().is_some());

    let res = client
        .get("/v1/sql?sql=select * from numbers limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    // body json:
    // r#"{"code":0,"output":[{"records":{"schema":{"column_schemas":[{"name":"number","data_type":"UInt32"}]},"rows":[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]}}]}"#

    assert!(body.success());
    assert!(body.execution_time_ms().is_some());

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
    // body json: r#"{"code":0,"output":[{"records":{"schema":{"column_schemas":[{"name":"host","data_type":"String"},{"name":"cpu","data_type":"Float64"},{"name":"memory","data_type":"Float64"},{"name":"ts","data_type":"Timestamp"}]},"rows":[["host",66.6,1024.0,0]]}}]}"#
    assert!(body.success());
    assert!(body.execution_time_ms().is_some());
    let output = body.output().unwrap();
    assert_eq!(output.len(), 1);

    assert_eq!(
        output[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"host","data_type":"String"},{"name":"cpu","data_type":"Float64"},{"name":"memory","data_type":"Float64"},{"name":"ts","data_type":"Timestamp"}]},"rows":[["host",66.6,1024.0,0]]}
        })).unwrap()
    );

    // select with projections
    let res = client
        .get("/v1/sql?sql=select cpu, ts from demo limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    // body json:
    // r#"{"code":0,"output":[{"records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"Timestamp"}]},"rows":[[66.6,0]]}}]}"#
    assert!(body.success());
    assert!(body.execution_time_ms().is_some());
    let output = body.output().unwrap();
    assert_eq!(output.len(), 1);

    assert_eq!(
        output[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"Timestamp"}]},"rows":[[66.6,0]]}
        })).unwrap()
    );

    // select with column alias
    let res = client
        .get("/v1/sql?sql=select cpu as c, ts as time from demo limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    // body json:
    // r#"{"code":0,"output":[{"records":{"schema":{"column_schemas":[{"name":"c","data_type":"Float64"},{"name":"time","data_type":"Timestamp"}]},"rows":[[66.6,0]]}}]}"#
    assert!(body.success());
    assert!(body.execution_time_ms().is_some());
    let output = body.output().unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"c","data_type":"Float64"},{"name":"time","data_type":"Timestamp"}]},"rows":[[66.6,0]]}
        })).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_api() {
    common_telemetry::init_default_ut_logging();
    common_telemetry::init_default_metrics_recorder();
    let (app, _guard) = make_test_app("metrics_api").await;
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
    assert!(body.contains("datanode_handle_sql_elapsed"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_scripts_api() {
    common_telemetry::init_default_ut_logging();
    let (app, _guard) = make_test_app_with_frontend("script_api").await;
    let client = TestClient::new(app);

    let res = client
        .post("/v1/scripts?name=test")
        .body(
            r#"
@copr(sql='select number from numbers limit 10', args=['number'], returns=['n'])
def test(n):
    return n + 1;
"#,
        )
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();
    // body json: r#"{"code":0}"#
    assert_eq!(body.code(), 0);
    assert!(body.output().is_none());

    // call script
    let res = client.post("/v1/run-script?name=test").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = serde_json::from_str::<JsonResponse>(&res.text().await).unwrap();

    // body json:
    // r#"{"code":0,"output":[{"records":{"schema":{"column_schemas":[{"name":"n","data_type":"Float64"}]},"rows":[[1.0],[2.0],[3.0],[4.0],[5.0],[6.0],[7.0],[8.0],[9.0],[10.0]]}}]}"#
    assert_eq!(body.code(), 0);
    assert!(body.execution_time_ms().is_some());
    let output = body.output().unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        serde_json::from_value::<JsonOutput>(json!({
            "records":{"schema":{"column_schemas":[{"name":"n","data_type":"Float64"}]},"rows":[[1.0],[2.0],[3.0],[4.0],[5.0],[6.0],[7.0],[8.0],[9.0],[10.0]]}
        })).unwrap()
    );
}
