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

use async_trait::async_trait;
use axum::Router;
use axum_test_helper::TestClient;
use common_query::Output;
use datatypes::schema::Schema;
use servers::error::{Error, Result};
use servers::http::{HttpOptions, HttpServer};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContextRef;

pub struct DummyInstance {}

#[async_trait]
impl SqlQueryHandler for DummyInstance {
    type Error = Error;

    async fn do_query(&self, _: &str, _: QueryContextRef) -> Vec<Result<Output>> {
        unimplemented!()
    }

    async fn do_promql_query(
        &self,
        _: &str,
        _: QueryContextRef,
    ) -> Vec<std::result::Result<Output, Self::Error>> {
        unimplemented!()
    }

    async fn do_statement_query(
        &self,
        _stmt: sql::statements::statement::Statement,
        _query_ctx: QueryContextRef,
    ) -> Result<Output> {
        unimplemented!()
    }

    fn do_describe(
        &self,
        _stmt: sql::statements::statement::Statement,
        _query_ctx: QueryContextRef,
    ) -> Result<Option<Schema>> {
        unimplemented!()
    }

    fn is_valid_schema(&self, _catalog: &str, _schema: &str) -> Result<bool> {
        Ok(true)
    }
}

fn make_test_app() -> Router {
    let instance = Arc::new(DummyInstance {});
    let server = HttpServer::new(instance, HttpOptions::default());
    server.make_app()
}

#[tokio::test]
async fn test_api_and_doc() {
    let app = make_test_app();
    let client = TestClient::new(app);
    let result = client.get("/v1/private/api.json").send().await;
    assert_eq!(result.status(), 200);
    let result = client.get("/v1/private/docs").send().await;
    assert_eq!(result.status(), 200);
}
