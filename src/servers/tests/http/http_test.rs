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

use axum::Router;
use common_test_util::ports;
use servers::http::test_helpers::TestClient;
use servers::http::{HttpOptions, HttpServerBuilder};
use table::test_util::MemTable;

use crate::create_testing_sql_query_handler;

fn make_test_app() -> Router {
    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };

    let server = HttpServerBuilder::new(http_opts)
        .with_sql_handler(
            create_testing_sql_query_handler(MemTable::default_numbers_table()),
            None,
        )
        .build();
    server.build(server.make_app())
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
