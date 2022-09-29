use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum_test_helper::TestClient;
use common_query::Output;
use servers::error::Result;
use servers::http::HttpServer;
use servers::influxdb::{InfluxdbRequest, InsertBatches};
use servers::query_handler::{InfluxdbLineProtocolHandler, SqlQueryHandler};
use tokio::sync::mpsc;

struct DummyInstance {
    tx: mpsc::Sender<String>,
}

#[async_trait]
impl InfluxdbLineProtocolHandler for DummyInstance {
    async fn exec(&self, request: &InfluxdbRequest) -> Result<()> {
        let batches: InsertBatches = request.try_into()?;

        for (table_name, _) in batches.data {
            let _ = self.tx.send(table_name).await;
        }

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
    server.set_influxdb_handler(instance);
    server.make_app()
}

#[tokio::test]
async fn test_influxdb_write() {
    let (tx, mut rx) = mpsc::channel(100);

    let app = make_test_app(tx);
    let client = TestClient::new(app);

    // right request
    let result = client
        .post("/v1/influxdb/write")
        .body("monitor,host=host1 cpu=1.2 1664370459457010101")
        .send()
        .await;
    assert_eq!(result.status(), 204);
    assert!(result.text().await.is_empty());

    // bad request
    let result = client
        .post("/v1/influxdb/write")
        .body("monitor,   host=host1 cpu=1.2 1664370459457010101")
        .send()
        .await;
    assert_eq!(result.status(), 400);
    assert!(!result.text().await.is_empty());

    let mut metrics = vec![];
    while let Ok(s) = rx.try_recv() {
        metrics.push(s);
    }
    assert_eq!(metrics, vec!["monitor".to_string()]);
}
