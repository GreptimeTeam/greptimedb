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

use axum::extract::{Query, State};
use axum::{Form, Json};
use servers::http::{influxdb_result_v1, ApiState};
use session::context::QueryContext;
use table::test_util::MemTable;

use crate::create_testing_sql_query_handler;

#[tokio::test]
async fn test_sql_not_provided() {
    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let ctx = QueryContext::arc();
    ctx.set_current_user(Some(auth::userinfo_by_name(None)));
    let Json(json) = influxdb_result_v1::sql_with_influxdb_v1_result(
        State(ApiState {
            sql_handler,
            script_handler: None,
        }),
        Query(influxdb_result_v1::SqlQuery::default()),
        axum::Extension(ctx),
        Form(influxdb_result_v1::SqlQuery::default()),
    )
    .await;
    assert!(!json.success());
    assert_eq!(
        Some(&"sql parameter is required.".to_string()),
        json.error()
    );
    assert!(json.results().is_none());
}

#[tokio::test]
async fn test_sql_results() {
    common_telemetry::init_default_ut_logging();

    let query = create_query();
    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());

    let ctx = QueryContext::arc();
    ctx.set_current_user(Some(auth::userinfo_by_name(None)));
    let Json(res) = influxdb_result_v1::sql_with_influxdb_v1_result(
        State(ApiState {
            sql_handler,
            script_handler: None,
        }),
        query,
        axum::Extension(ctx),
        Form(influxdb_result_v1::SqlQuery::default()),
    )
    .await;
    assert!(res.success(), "{res:?}");
    assert!(res.error().is_none());

    let json = serde_json::to_string_pretty(&res.results()).unwrap();
    assert_eq!(
        json,
        r#"[
  {
    "statement_id": 0,
    "series": [
      {
        "name": "",
        "columns": [
          "SUM(numbers.uint32s)"
        ],
        "values": [
          [
            4950
          ]
        ]
      }
    ]
  }
]"#
    );
}

#[tokio::test]
async fn test_sql_form() {
    common_telemetry::init_default_ut_logging();

    let form = create_form();
    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());

    let ctx = QueryContext::arc();
    ctx.set_current_user(Some(auth::userinfo_by_name(None)));

    let Json(res) = influxdb_result_v1::sql_with_influxdb_v1_result(
        State(ApiState {
            sql_handler,
            script_handler: None,
        }),
        Query(influxdb_result_v1::SqlQuery::default()),
        axum::Extension(ctx),
        form,
    )
    .await;
    assert!(res.success(), "{res:?}");
    assert!(res.error().is_none());

    let json = serde_json::to_string_pretty(&res.results()).unwrap();
    assert_eq!(
        json,
        r#"[
  {
    "statement_id": 0,
    "series": [
      {
        "name": "",
        "columns": [
          "SUM(numbers.uint32s)"
        ],
        "values": [
          [
            4950
          ]
        ]
      }
    ]
  }
]"#
    );
}

fn create_query() -> Query<influxdb_result_v1::SqlQuery> {
    Query(influxdb_result_v1::SqlQuery {
        sql: Some("select sum(uint32s) from numbers limit 20".to_string()),
        db: None,
        epoch: None,
    })
}

fn create_form() -> Form<influxdb_result_v1::SqlQuery> {
    Form(influxdb_result_v1::SqlQuery {
        sql: Some("select sum(uint32s) from numbers limit 20".to_string()),
        db: None,
        epoch: None,
    })
}
