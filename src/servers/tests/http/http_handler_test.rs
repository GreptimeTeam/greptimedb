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

use std::collections::HashMap;

use axum::body::Body;
use axum::extract::{Json, Query, RawBody, State};
use common_telemetry::metric;
use metrics::counter;
use servers::http::{handler as http_handler, script as script_handler, ApiState, JsonOutput};
use session::context::UserInfo;
use table::test_util::MemTable;

use crate::{create_testing_script_handler, create_testing_sql_query_handler};

#[tokio::test]
async fn test_sql_not_provided() {
    let query_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let Json(json) = http_handler::sql(
        State(ApiState {
            query_handler,
            script_handler: None,
        }),
        Query(http_handler::SqlQuery::default()),
        axum::Extension(UserInfo::default()),
    )
    .await;
    assert!(!json.success());
    assert_eq!(
        Some(&"sql parameter is required.".to_string()),
        json.error()
    );
    assert!(json.output().is_none());
}

#[tokio::test]
async fn test_sql_output_rows() {
    common_telemetry::init_default_ut_logging();

    let query = create_query();
    let query_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());

    let Json(json) = http_handler::sql(
        State(ApiState {
            query_handler,
            script_handler: None,
        }),
        query,
        axum::Extension(UserInfo::default()),
    )
    .await;
    assert!(json.success(), "{json:?}");
    assert!(json.error().is_none());
    match &json.output().expect("assertion failed")[0] {
        JsonOutput::Records(records) => {
            assert_eq!(1, records.num_rows());
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_metrics() {
    metric::init_default_metrics_recorder();

    counter!("test_metrics", 1);

    let text = http_handler::metrics(Query(HashMap::default())).await;
    assert!(text.contains("test_metrics counter"));
}

#[tokio::test]
async fn test_scripts() {
    common_telemetry::init_default_ut_logging();

    let script = r#"
@copr(sql='select uint32s as number from numbers', args=['number'], returns=['n'])
def test(n):
    return n;
"#
    .to_string();
    let query_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let script_handler = create_testing_script_handler(MemTable::default_numbers_table());
    let body = RawBody(Body::from(script.clone()));
    let invalid_query = create_invalid_script_query();
    let Json(json) = script_handler::scripts(
        State(ApiState {
            query_handler: query_handler.clone(),
            script_handler: Some(script_handler.clone()),
        }),
        invalid_query,
        body,
    )
    .await;
    assert!(!json.success(), "{json:?}");
    assert_eq!(json.error().unwrap(), "Invalid argument: invalid schema");

    let body = RawBody(Body::from(script));
    let exec = create_script_query();
    let Json(json) = script_handler::scripts(
        State(ApiState {
            query_handler,
            script_handler: Some(script_handler),
        }),
        exec,
        body,
    )
    .await;
    assert!(json.success(), "{json:?}");
    assert!(json.error().is_none());
    assert!(json.output().is_none());
}

fn create_script_query() -> Query<script_handler::ScriptQuery> {
    Query(script_handler::ScriptQuery {
        schema: Some("test".to_string()),
        name: Some("test".to_string()),
    })
}

fn create_invalid_script_query() -> Query<script_handler::ScriptQuery> {
    Query(script_handler::ScriptQuery {
        schema: None,
        name: None,
    })
}

fn create_query() -> Query<http_handler::SqlQuery> {
    Query(http_handler::SqlQuery {
        sql: Some("select sum(uint32s) from numbers limit 20".to_string()),
        db: None,
    })
}

/// Currently the payload of response should be simply an empty json "{}";
#[tokio::test]
async fn test_health() {
    let expected_json = http_handler::HealthResponse {};
    let expected_json_str = "{}".to_string();

    let query = http_handler::HealthQuery {};
    let Json(json) = http_handler::health(Query(query)).await;
    assert_eq!(json, expected_json);
    assert_eq!(
        serde_json::ser::to_string(&json).unwrap(),
        expected_json_str
    );
}
