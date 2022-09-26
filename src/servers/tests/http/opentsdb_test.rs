use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum_test_helper::TestClient;
use common_query::Output;
use servers::error::{self, Result};
use servers::http::HttpServer;
use servers::opentsdb::codec::DataPoint;
use servers::query_handler::{OpentsdbProtocolHandler, SqlQueryHandler};
use tokio::sync::mpsc;

struct DummyInstance {
    tx: mpsc::Sender<String>,
}

#[async_trait]
impl OpentsdbProtocolHandler for DummyInstance {
    async fn exec(&self, data_point: &DataPoint) -> Result<()> {
        if data_point.metric() == "should_failed" {
            return error::InternalSnafu {
                err_msg: "expected",
            }
            .fail();
        }
        let _ = self.tx.send(data_point.metric().to_string()).await;
        Ok(())
    }
}

#[async_trait]
impl SqlQueryHandler for DummyInstance {
    async fn do_query(&self, _query: &str) -> Result<Output> {
        unimplemented!()
    }

    async fn insert_script(&self, _name: &str, _script: &str) -> Result<()> {
        unimplemented!()
    }

    async fn execute_script(&self, _name: &str) -> Result<Output> {
        unimplemented!()
    }
}

fn make_test_app(tx: mpsc::Sender<String>) -> Router {
    let instance = Arc::new(DummyInstance { tx });
    let mut server = HttpServer::new(instance.clone());
    server.set_opentsdb_handler(instance);
    server.make_app()
}

#[tokio::test]
async fn test_opentsdb_put() {
    let (tx, mut rx) = mpsc::channel(100);

    let app = make_test_app(tx);
    let client = TestClient::new(app);

    // single data point put
    let result = client
        .post("/v1/opentsdb/api/put")
        .body(create_data_point("m1"))
        .send()
        .await;
    assert_eq!(result.status(), 204);
    assert!(result.text().await.is_empty());

    // multiple data point put
    let result = client
        .post("/v1/opentsdb/api/put")
        .body(format!(
            "[{},{}]",
            create_data_point("m2"),
            create_data_point("m3")
        ))
        .send()
        .await;
    assert_eq!(result.status(), 204);
    assert!(result.text().await.is_empty());

    // bad data point
    let result = client
        .post("/v1/opentsdb/api/put")
        .body("hello, world")
        .send()
        .await;
    assert_eq!(result.status(), 400);
    assert_eq!(
        result.text().await,
        "{\"error\":\"Invalid OpenTSDB Json request, source: expected value at line 1 column 1\"}"
    );

    // internal server error
    let result = client
        .post("/v1/opentsdb/api/put")
        .body(create_data_point("should_failed"))
        .send()
        .await;
    assert_eq!(result.status(), 500);
    assert_eq!(
        result.text().await,
        "{\"error\":\"Internal error: Internal error: expected\"}"
    );

    let mut metrics = vec![];
    while let Ok(s) = rx.try_recv() {
        metrics.push(s);
    }
    assert_eq!(
        metrics,
        vec!["m1".to_string(), "m2".to_string(), "m3".to_string()]
    );
}

#[tokio::test]
async fn test_opentsdb_debug_put() {
    let (tx, mut rx) = mpsc::channel(100);

    let app = make_test_app(tx);
    let client = TestClient::new(app);

    // single data point summary debug put
    let result = client
        .post("/v1/opentsdb/api/put?summary")
        .body(create_data_point("m11"))
        .send()
        .await;
    assert_eq!(result.status(), 200);
    assert_eq!(result.text().await, "{\"success\":1,\"failed\":0}");

    let result = client
        .post("/v1/opentsdb/api/put?summary")
        .body(create_data_point("should_failed"))
        .send()
        .await;
    assert_eq!(result.status(), 200);
    assert_eq!(result.text().await, "{\"success\":0,\"failed\":1}");

    let result = client
        .post("/v1/opentsdb/api/put?details")
        .body(create_data_point("should_failed"))
        .send()
        .await;
    assert_eq!(result.status(), 200);
    assert_eq!(result.text().await, "{\"success\":0,\"failed\":1,\"errors\":[{\"datapoint\":{\"metric\":\"should_failed\",\"timestamp\":1000,\"value\":1.0,\"tags\":{\"host\":\"web01\"}},\"error\":\"Internal error: expected\"}]}");

    // multiple data point summary debug put
    let result = client
        .post("/v1/opentsdb/api/put?summary")
        .body(format!(
            "[{},{}]",
            create_data_point("should_failed"),
            create_data_point("m22"),
        ))
        .send()
        .await;
    assert_eq!(result.status(), 200);
    assert_eq!(result.text().await, "{\"success\":1,\"failed\":1}");

    let result = client
        .post("/v1/opentsdb/api/put?details")
        .body(format!(
            "[{},{}]",
            create_data_point("should_failed"),
            create_data_point("m33")
        ))
        .send()
        .await;
    assert_eq!(result.status(), 200);
    assert_eq!(result.text().await, "{\"success\":1,\"failed\":1,\"errors\":[{\"datapoint\":{\"metric\":\"should_failed\",\"timestamp\":1000,\"value\":1.0,\"tags\":{\"host\":\"web01\"}},\"error\":\"Internal error: expected\"}]}");

    let mut metrics = vec![];
    while let Ok(s) = rx.try_recv() {
        metrics.push(s);
    }
    assert_eq!(
        metrics,
        vec!["m11".to_string(), "m22".to_string(), "m33".to_string()]
    );
}

fn create_data_point(metric: &str) -> String {
    format!(
        r#"{{
                "metric": "{}",
                "timestamp": 1000,
                "value": 1,
                "tags": {{
                    "host": "web01"
                }}
            }}"#,
        metric
    )
}
