//! http server test

use std::sync::Arc;

use axum::http::StatusCode;
use axum::Router;
use axum_test_helper::TestClient;
use common_telemetry::logging;
use datanode::{instance::Instance, server::http::HttpServer};
use query::catalog::memory;

fn make_test_app() -> Router {
    let catalog_list = memory::new_memory_catalog_list().unwrap();
    let instance = Arc::new(Instance::new(catalog_list));
    let http_server = HttpServer::new(instance);
    http_server.make_app()
}

#[tokio::test]
async fn test_sql_api() {
    logging::init_default_ut_tracing();
    let app = make_test_app();
    let client = TestClient::new(app);
    let res = client.get("/sql").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    // TODO(dennis): deserialize to json response
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
        r#"{"success":true,"output":{"Rows":[{"schema":{"fields":[{"name":"number","data_type":"UInt32","is_nullable":false,"metadata":{}}],"metadata":{}},"columns":[{"UInt32":[0,1,2,3,4,5,6,7,8,9]}]}]}}"#
    );
}
