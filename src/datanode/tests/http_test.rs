//! http server test

use std::sync::Arc;

use axum::http::StatusCode;
use axum::Router;
use axum_test_helper::TestClient;
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

    let res = client.get("/sql?sql=select * from numbers").send().await;
    assert_eq!(res.status(), StatusCode::OK);

    let body = res.text().await;
    assert_eq!(
        body,
        r#"{"success":true,"output":{"Rows":[{"schema":{"fields":[{"name":"number","data_type":"UInt32","is_nullable":false,"metadata":{}}],"metadata":{}},"columns":[{"UInt32":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99]}]}]}}"#
    );
}
