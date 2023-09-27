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

use api::v1::greptime_request::Request;
use async_trait::async_trait;
use axum::Router;
use axum_test_helper::TestClient;
use common_query::Output;
use common_test_util::ports;
use query::parser::PromQuery;
use query::plan::LogicalPlan;
use query::query_engine::DescribeResult;
use servers::error::{self, Result};
use servers::http::{HttpOptions, HttpServerBuilder};
use servers::opentsdb::codec::DataPoint;
use servers::query_handler::grpc::GrpcQueryHandler;
use servers::query_handler::sql::SqlQueryHandler;
use servers::query_handler::OpentsdbProtocolHandler;
use session::context::QueryContextRef;
use tokio::sync::mpsc;

struct DummyInstance {
    tx: mpsc::Sender<String>,
}

#[async_trait]
impl GrpcQueryHandler for DummyInstance {
    type Error = crate::Error;

    async fn do_query(
        &self,
        _query: Request,
        _ctx: QueryContextRef,
    ) -> std::result::Result<Output, Self::Error> {
        unimplemented!()
    }
}

#[async_trait]
impl OpentsdbProtocolHandler for DummyInstance {
    async fn exec(&self, data_point: &DataPoint, _ctx: QueryContextRef) -> Result<()> {
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
    type Error = error::Error;

    async fn do_query(&self, _: &str, _: QueryContextRef) -> Vec<Result<Output>> {
        unimplemented!()
    }

    async fn do_exec_plan(
        &self,
        _plan: LogicalPlan,
        _query_ctx: QueryContextRef,
    ) -> std::result::Result<Output, Self::Error> {
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
    ) -> Result<Option<DescribeResult>> {
        unimplemented!()
    }

    async fn is_valid_schema(&self, _catalog: &str, _schema: &str) -> Result<bool> {
        Ok(true)
    }
}

fn make_test_app(tx: mpsc::Sender<String>) -> Router {
    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };

    let instance = Arc::new(DummyInstance { tx });
    let server = HttpServerBuilder::new(http_opts)
        .with_grpc_handler(instance.clone())
        .with_sql_handler(instance.clone())
        .with_opentsdb_handler(instance)
        .build();
    server.build(server.make_app())
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
        "{\"error\":\"Invalid OpenTSDB Json request: expected value at line 1 column 1\"}"
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
                "metric": "{metric}",
                "timestamp": 1000,
                "value": 1,
                "tags": {{
                    "host": "web01"
                }}
            }}"#,
    )
}
