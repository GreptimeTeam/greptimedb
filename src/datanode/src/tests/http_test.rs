use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::Router;
use axum_test_helper::TestClient;
use datatypes::prelude::ConcreteDataType;
use serde_json::json;
use servers::http::{ColumnSchema, HttpServer, JsonOutput, JsonResponse, Schema};
use servers::server::Server;
use test_util::TestGuard;

use crate::instance::Instance;
use crate::tests::test_util;

async fn make_test_app(name: &str) -> (Router, TestGuard) {
    let (opts, guard) = test_util::create_tmp_dir_and_datanode_opts(name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    instance.start().await.unwrap();
    test_util::create_test_table(&instance, ConcreteDataType::timestamp_millis_datatype())
        .await
        .unwrap();
    let http_server = HttpServer::new(instance);
    (http_server.make_app(), guard)
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
    if let JsonOutput::Records(records) = &output[0] {
        assert_eq!(records.num_cols(), 1);
        assert_eq!(records.num_rows(), 10);
        assert_eq!(
            records.schema().unwrap(),
            &Schema::new(vec![ColumnSchema::new(
                "number".to_owned(),
                "UInt32".to_owned()
            )])
        );
        assert_eq!(records.rows()[0][0], json!(0));
        assert_eq!(records.rows()[9][0], json!(9));
    } else {
        unreachable!()
    }

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
    if let JsonOutput::Records(records) = &output[0] {
        assert_eq!(records.num_cols(), 4);
        assert_eq!(records.num_rows(), 1);
        assert_eq!(
            records.schema().unwrap(),
            &Schema::new(vec![
                ColumnSchema::new("host".to_owned(), "String".to_owned()),
                ColumnSchema::new("cpu".to_owned(), "Float64".to_owned()),
                ColumnSchema::new("memory".to_owned(), "Float64".to_owned()),
                ColumnSchema::new("ts".to_owned(), "Timestamp".to_owned())
            ])
        );
        assert_eq!(
            records.rows()[0],
            vec![json!("host"), json!(66.6), json!(1024.0), json!(0)]
        );
    } else {
        unreachable!();
    }

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
    if let JsonOutput::Records(records) = &output[0] {
        assert_eq!(records.num_cols(), 2);
        assert_eq!(records.num_rows(), 1);
        assert_eq!(
            records.schema().unwrap(),
            &Schema::new(vec![
                ColumnSchema::new("cpu".to_owned(), "Float64".to_owned()),
                ColumnSchema::new("ts".to_owned(), "Timestamp".to_owned())
            ])
        );
        assert_eq!(records.rows()[0], vec![json!(66.6), json!(0)]);
    } else {
        unreachable!()
    }

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
    if let JsonOutput::Records(records) = &output[0] {
        assert_eq!(records.num_cols(), 2);
        assert_eq!(records.num_rows(), 1);
        assert_eq!(
            records.schema().unwrap(),
            &Schema::new(vec![
                ColumnSchema::new("c".to_owned(), "Float64".to_owned()),
                ColumnSchema::new("time".to_owned(), "Timestamp".to_owned())
            ])
        );
        assert_eq!(records.rows()[0], vec![json!(66.6), json!(0)]);
    } else {
        unreachable!()
    }
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
    let (app, _guard) = make_test_app("scripts_api").await;
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
    if let JsonOutput::Records(ref records) = output[0] {
        assert_eq!(records.num_cols(), 1);
        assert_eq!(records.num_rows(), 10);
        assert_eq!(
            records.schema().unwrap(),
            &Schema::new(vec![ColumnSchema::new(
                "n".to_owned(),
                "Float64".to_owned()
            )])
        );
        assert_eq!(records.rows()[0][0], json!(1.0));
    } else {
        unreachable!()
    }
}

async fn start_test_app(addr: &str) -> (SocketAddr, TestGuard) {
    let (opts, guard) = test_util::create_tmp_dir_and_datanode_opts("py_side_scripts_api");
    let instance = Arc::new(Instance::new(&opts).await.unwrap());
    instance.start().await.unwrap();
    let http_server = HttpServer::new(instance);
    (
        http_server.start(addr.parse().unwrap()).await.unwrap(),
        guard,
    )
}

#[allow(unused)]
#[tokio::test]
async fn test_py_side_scripts_api() {
    // TODO(discord9): make a working test case, it will require python3 with numpy installed, complex environment setup expected....
    common_telemetry::init_default_ut_logging();
    let server = start_test_app("127.0.0.1:21830");
    // let (app, _guard) = server.await;
    // dbg!(app);
}
