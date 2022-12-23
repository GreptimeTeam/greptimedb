// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use api::v1::InsertExpr;
use async_trait::async_trait;
use axum::{http, Router};
use axum_test_helper::TestClient;
use common_query::Output;
use servers::auth::user_provider::StaticUserProvider;
use servers::error::Result;
use servers::http::{HttpOptions, HttpServer};
use servers::influxdb::InfluxdbRequest;
use servers::query_handler::{InfluxdbLineProtocolHandler, SqlQueryHandler};
use session::context::QueryContextRef;
use tokio::sync::mpsc;

struct DummyInstance {
    tx: mpsc::Sender<(String, String)>,
}

#[async_trait]
impl InfluxdbLineProtocolHandler for DummyInstance {
    async fn exec(&self, request: &InfluxdbRequest) -> Result<()> {
        let exprs: Vec<InsertExpr> = request.try_into()?;

        for expr in exprs {
            let _ = self.tx.send((expr.schema_name, expr.table_name)).await;
        }

        Ok(())
    }
}

#[async_trait]
impl SqlQueryHandler for DummyInstance {
    async fn do_query(&self, _: &str, _: QueryContextRef) -> Vec<Result<Output>> {
        unimplemented!()
    }

    async fn do_statement_query(
        &self,
        _stmt: sql::statements::statement::Statement,
        _query_ctx: QueryContextRef,
    ) -> Result<Output> {
        unimplemented!()
    }

    fn is_valid_schema(&self, _catalog: &str, _schema: &str) -> Result<bool> {
        Ok(true)
    }
}

fn make_test_app(tx: mpsc::Sender<(String, String)>) -> Router {
    let instance = Arc::new(DummyInstance { tx });
    let mut server = HttpServer::new(instance.clone(), HttpOptions::default());
    let up = StaticUserProvider::try_from("cmd:greptime=greptime").unwrap();
    server.set_user_provider(Arc::new(up));

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
        .header(
            http::header::AUTHORIZATION,
            "basic Z3JlcHRpbWU6Z3JlcHRpbWU=",
        )
        .send()
        .await;
    assert_eq!(result.status(), 204);
    assert!(result.text().await.is_empty());

    let result = client
        .post("/v1/influxdb/write?db=influxdb")
        .body("monitor,host=host1 cpu=1.2 1664370459457010101")
        .header(
            http::header::AUTHORIZATION,
            "basic Z3JlcHRpbWU6Z3JlcHRpbWU=",
        )
        .send()
        .await;
    assert_eq!(result.status(), 204);
    assert!(result.text().await.is_empty());

    // bad request
    let result = client
        .post("/v1/influxdb/write")
        .body("monitor,   host=host1 cpu=1.2 1664370459457010101")
        .header(
            http::header::AUTHORIZATION,
            "basic Z3JlcHRpbWU6Z3JlcHRpbWU=",
        )
        .send()
        .await;
    assert_eq!(result.status(), 400);
    assert!(!result.text().await.is_empty());

    let mut metrics = vec![];
    while let Ok(s) = rx.try_recv() {
        metrics.push(s);
    }
    assert_eq!(
        metrics,
        vec![
            ("public".to_string(), "monitor".to_string()),
            ("influxdb".to_string(), "monitor".to_string())
        ]
    );
}
