use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::Router;
use axum_test_helper::TestClient;
use datatypes::prelude::ConcreteDataType;
use servers::http::handler::ScriptExecution;
use servers::http::HttpServer;
use servers::server::Server;
use test_util::TestGuard;

use crate::instance::Instance;
use crate::tests::test_util;

async fn make_test_app(name: &str) -> (Router, TestGuard) {
    let (opts, guard) = test_util::create_tmp_dir_and_datanode_opts(name);
    let instance = Arc::new(Instance::new(&opts).await.unwrap());
    instance.start().await.unwrap();
    test_util::create_test_table(&instance, ConcreteDataType::timestamp_millis_datatype())
        .await
        .unwrap();
    let http_server = HttpServer::new(instance);
    (http_server.make_app(), guard)
}

#[tokio::test]
async fn test_sql_api() {
    common_telemetry::init_default_ut_logging();
    let (app, _guard) = make_test_app("sql_api").await;
    let client = TestClient::new(app);
    let res = client.get("/v1/sql").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.text().await;
    assert_eq!(
        body,
        r#"{"code":1004,"error":"sql parameter is required."}"#
    );

    let res = client
        .get("/v1/sql?sql=select * from numbers limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.text().await;
    assert_eq!(
        body,
        r#"{"code":0,"output":{"records":{"schema":{"column_schemas":[{"name":"number","data_type":"UInt32"}]},"rows":[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]}}}"#
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

    let body = res.text().await;
    assert_eq!(
        body,
        r#"{"code":0,"output":{"records":{"schema":{"column_schemas":[{"name":"host","data_type":"String"},{"name":"cpu","data_type":"Float64"},{"name":"memory","data_type":"Float64"},{"name":"ts","data_type":"Timestamp"}]},"rows":[["host",66.6,1024.0,0]]}}}"#
    );

    // select with projections
    let res = client
        .get("/v1/sql?sql=select cpu, ts from demo limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.text().await;
    assert_eq!(
        body,
        r#"{"code":0,"output":{"records":{"schema":{"column_schemas":[{"name":"cpu","data_type":"Float64"},{"name":"ts","data_type":"Timestamp"}]},"rows":[[66.6,0]]}}}"#
    );
}

#[tokio::test]
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
    let res = client.get("/v1/metrics").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await;
    assert!(body.contains("datanode_handle_sql_elapsed"));
}

#[tokio::test]
async fn test_scripts_api() {
    common_telemetry::init_default_ut_logging();
    let (app, _guard) = make_test_app("scripts_api").await;
    let client = TestClient::new(app);
    let res = client
        .post("/v1/scripts")
        .json(&ScriptExecution {
            name: "test".to_string(),
            script: r#"
@copr(sql='select number from numbers limit 10', args=['number'], returns=['n'])
def test(n):
    return n + 1;
"#
            .to_string(),
        })
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.text().await;
    assert_eq!(body, r#"{"code":0}"#,);

    // call script
    let res = client.post("/v1/run-script?name=test").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.text().await;
    assert_eq!(
        body,
        r#"{"code":0,"output":{"records":{"schema":{"column_schemas":[{"name":"n","data_type":"Float64"}]},"rows":[[1.0],[2.0],[3.0],[4.0],[5.0],[6.0],[7.0],[8.0],[9.0],[10.0]]}}}"#,
    );
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
