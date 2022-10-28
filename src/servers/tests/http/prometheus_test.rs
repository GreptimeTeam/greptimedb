use std::sync::Arc;

use api::prometheus::remote::{
    LabelMatcher, Query, QueryResult, ReadRequest, ReadResponse, WriteRequest,
};
use async_trait::async_trait;
use axum::Router;
use axum_test_helper::TestClient;
use common_query::Output;
use prost::Message;
use servers::error::Result;
use servers::http::HttpServer;
use servers::prometheus;
use servers::prometheus::snappy_compress;
use servers::prometheus::Metrics;
use servers::query_handler::{PrometheusProtocolHandler, PrometheusResponse, SqlQueryHandler};
use tokio::sync::mpsc;

struct DummyInstance {
    tx: mpsc::Sender<Vec<u8>>,
}

#[async_trait]
impl PrometheusProtocolHandler for DummyInstance {
    async fn write(&self, request: WriteRequest) -> Result<()> {
        let _ = self.tx.send(request.encode_to_vec()).await;

        Ok(())
    }
    async fn read(&self, request: ReadRequest) -> Result<PrometheusResponse> {
        let _ = self.tx.send(request.encode_to_vec()).await;

        let response = ReadResponse {
            results: vec![QueryResult {
                timeseries: prometheus::mock_timeseries(),
            }],
        };

        Ok(PrometheusResponse {
            content_type: "application/x-protobuf".to_string(),
            content_encoding: "snappy".to_string(),
            body: response.encode_to_vec(),
        })
    }

    async fn ingest_metrics(&self, _metrics: Metrics) -> Result<()> {
        unimplemented!();
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

fn make_test_app(tx: mpsc::Sender<Vec<u8>>) -> Router {
    let instance = Arc::new(DummyInstance { tx });
    let mut server = HttpServer::new(instance.clone());
    server.set_prom_handler(instance);
    server.make_app()
}

#[tokio::test]
async fn test_prometheus_remote_write_read() {
    let (tx, mut rx) = mpsc::channel(100);

    let app = make_test_app(tx);
    let client = TestClient::new(app);

    let write_request = WriteRequest {
        timeseries: prometheus::mock_timeseries(),
        ..Default::default()
    };

    let result = client
        .post("/v1/prometheus/write")
        .body(snappy_compress(&write_request.clone().encode_to_vec()[..]).unwrap())
        .send()
        .await;
    assert_eq!(result.status(), 204);
    assert!(result.text().await.is_empty());

    let read_request = ReadRequest {
        queries: vec![Query {
            start_timestamp_ms: 1000,
            end_timestamp_ms: 2000,
            matchers: vec![LabelMatcher {
                name: prometheus::METRIC_NAME_LABEL.to_string(),
                value: "metric1".to_string(),
                r#type: 0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut result = client
        .post("/v1/prometheus/read")
        .body(snappy_compress(&read_request.clone().encode_to_vec()[..]).unwrap())
        .send()
        .await;
    assert_eq!(result.status(), 200);
    let headers = result.headers();
    assert_eq!(
        Some("application/x-protobuf"),
        headers.get("content-type").map(|x| x.to_str().unwrap())
    );
    assert_eq!(
        Some("snappy"),
        headers.get("content-encoding").map(|x| x.to_str().unwrap())
    );
    let response = result.chunk().await.unwrap();
    let response = ReadResponse::decode(&response[..]).unwrap();
    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].timeseries,
        prometheus::mock_timeseries()
    );

    let mut requests = vec![];
    while let Ok(s) = rx.try_recv() {
        requests.push(s);
    }

    assert_eq!(2, requests.len());

    assert_eq!(
        write_request,
        WriteRequest::decode(&requests[0][..]).unwrap()
    );
    assert_eq!(read_request, ReadRequest::decode(&requests[1][..]).unwrap());
}
