//! http server test

use std::sync::Arc;

use axum::http::StatusCode;
use axum::Router;
use axum_test_helper::TestClient;

use crate::instance::Instance;
use crate::server::http::HttpServer;
use crate::test_util;

async fn make_test_app() -> Router {
    let catalog_list = catalog::memory::new_memory_catalog_list().unwrap();
    let (opts, _tmp_dir) = test_util::create_tmp_dir_and_datanode_opts();
    let instance = Arc::new(Instance::new(&opts, catalog_list).await.unwrap());
    let http_server = HttpServer::new(instance);
    http_server.make_app()
}

#[tokio::test]
async fn test_sql_api() {
    common_telemetry::init_default_ut_logging();
    let app = make_test_app().await;
    let client = TestClient::new(app);
    let res = client.get("/sql").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.text().await;
    assert_eq!(
        body,
        r#"{"success":false,"error":"sql parameter is required."}"#
    );

    let res = client
        .get("/sql?sql=select * from numbers limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.text().await;
    assert_eq!(
        body,
        r#"{"success":true,"output":{"Rows":[{"schema":{"fields":[{"name":"number","data_type":"UInt32","is_nullable":false,"metadata":{}}],"metadata":{}},"columns":[[0,1,2,3,4,5,6,7,8,9]]}]}}"#
    );
}

#[tokio::test]
async fn test_metrics_api() {
    common_telemetry::init_default_ut_logging();
    common_telemetry::init_default_metrics_recorder();
    let app = make_test_app().await;
    let client = TestClient::new(app);

    // Send a sql
    let res = client
        .get("/sql?sql=select * from numbers limit 10")
        .send()
        .await;
    assert_eq!(res.status(), StatusCode::OK);

    // Call metrics api
    let res = client.get("/metrics").send().await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await;
    assert!(body.contains("datanode_handle_sql_elapsed"));
}
