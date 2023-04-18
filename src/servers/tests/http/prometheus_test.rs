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

use std::sync::Arc;

use api::prometheus::remote::{
    LabelMatcher, Query, QueryResult, ReadRequest, ReadResponse, WriteRequest,
};
use api::v1::greptime_request::Request;
use async_trait::async_trait;
use axum::Router;
use axum_test_helper::TestClient;
use common_query::Output;
use datatypes::schema::Schema;
use prost::Message;
use query::parser::PromQuery;
use servers::error::{Error, Result};
use servers::http::{HttpOptions, HttpServerBuilder};
use servers::prometheus;
use servers::prometheus::{snappy_compress, Metrics};
use servers::query_handler::grpc::GrpcQueryHandler;
use servers::query_handler::sql::SqlQueryHandler;
use servers::query_handler::{PrometheusProtocolHandler, PrometheusResponse};
use session::context::QueryContextRef;
use tokio::sync::mpsc;

struct DummyInstance {
    tx: mpsc::Sender<(String, Vec<u8>)>,
}

#[async_trait]
impl GrpcQueryHandler for DummyInstance {
    type Error = Error;

    async fn do_query(
        &self,
        _query: Request,
        _ctx: QueryContextRef,
    ) -> std::result::Result<Output, Self::Error> {
        unimplemented!()
    }
}

#[async_trait]
impl PrometheusProtocolHandler for DummyInstance {
    async fn write(&self, request: WriteRequest, ctx: QueryContextRef) -> Result<()> {
        let _ = self
            .tx
            .send((ctx.current_schema(), request.encode_to_vec()))
            .await;

        Ok(())
    }
    async fn read(&self, request: ReadRequest, ctx: QueryContextRef) -> Result<PrometheusResponse> {
        let _ = self
            .tx
            .send((ctx.current_schema(), request.encode_to_vec()))
            .await;

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
    type Error = Error;

    async fn do_query(&self, _: &str, _: QueryContextRef) -> Vec<Result<Output>> {
        unimplemented!()
    }

    async fn do_promql_query(
        &self,
        _: &PromQuery,
        _: QueryContextRef,
    ) -> Vec<std::result::Result<Output, Self::Error>> {
        unimplemented!()
    }

    async fn do_describe(
        &self,
        _stmt: sql::statements::statement::Statement,
        _query_ctx: QueryContextRef,
    ) -> Result<Option<Schema>> {
        unimplemented!()
    }

    async fn is_valid_schema(&self, _catalog: &str, _schema: &str) -> Result<bool> {
        Ok(true)
    }
}

fn make_test_app(tx: mpsc::Sender<(String, Vec<u8>)>) -> Router {
    let instance = Arc::new(DummyInstance { tx });
    let server = HttpServerBuilder::new(HttpOptions::default())
        .with_grpc_handler(instance.clone())
        .with_sql_handler(instance.clone())
        .with_prom_handler(instance)
        .build();
    server.build(server.make_app())
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

    // Write to public database
    let result = client
        .post("/v1/prometheus/write")
        .body(snappy_compress(&write_request.clone().encode_to_vec()[..]).unwrap())
        .send()
        .await;
    assert_eq!(result.status(), 204);
    assert!(result.text().await.is_empty());
    // Write to prometheus database
    let result = client
        .post("/v1/prometheus/write?db=prometheus")
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

    // Read from prometheus database
    let mut result = client
        .post("/v1/prometheus/read?db=prometheus")
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

    // Read from public database
    let result = client
        .post("/v1/prometheus/read")
        .body(snappy_compress(&read_request.clone().encode_to_vec()[..]).unwrap())
        .send()
        .await;
    assert_eq!(result.status(), 200);

    let mut requests: Vec<(String, Vec<u8>)> = vec![];
    while let Ok(s) = rx.try_recv() {
        requests.push(s);
    }

    assert_eq!(4, requests.len());

    assert_eq!("public", requests[0].0);
    assert_eq!("prometheus", requests[1].0);
    assert_eq!("prometheus", requests[2].0);
    assert_eq!("public", requests[3].0);

    assert_eq!(
        write_request,
        WriteRequest::decode(&(requests[0].1)[..]).unwrap()
    );
    assert_eq!(
        write_request,
        WriteRequest::decode(&(requests[1].1)[..]).unwrap()
    );

    assert_eq!(
        read_request,
        ReadRequest::decode(&(requests[2].1)[..]).unwrap()
    );
    assert_eq!(
        read_request,
        ReadRequest::decode(&(requests[3].1)[..]).unwrap()
    );
}
