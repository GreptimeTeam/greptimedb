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

use axum::extract::{Json, Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::Form;
use bytes::Bytes;
use headers::HeaderValue;
use mime_guess::mime;
use servers::http::GreptimeQueryOutput::Records;
use servers::http::{
    handler as http_handler, ApiState, GreptimeOptionsConfigState, GreptimeQueryOutput,
    HttpResponse,
};
use servers::metrics_handler::MetricsHandler;
use session::context::QueryContext;
use table::test_util::MemTable;

use crate::create_testing_sql_query_handler;

#[tokio::test]
async fn test_sql_not_provided() {
    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let ctx = QueryContext::with_db_name(None);
    ctx.set_current_user(auth::userinfo_by_name(None));
    let api_state = ApiState { sql_handler };

    for format in ["greptimedb_v1", "influxdb_v1", "csv", "table"] {
        let query = http_handler::SqlQuery {
            format: Some(format.to_string()),
            ..Default::default()
        };

        let HttpResponse::Error(resp) = http_handler::sql(
            State(api_state.clone()),
            Query(query),
            axum::Extension(ctx.clone()),
            Form(http_handler::SqlQuery::default()),
        )
        .await
        else {
            unreachable!("must be error response")
        };

        assert_eq!("sql parameter is required.", resp.error());
    }
}

#[tokio::test]
async fn test_sql_output_rows() {
    common_telemetry::init_default_ut_logging();

    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());

    let ctx = QueryContext::with_db_name(None);
    ctx.set_current_user(auth::userinfo_by_name(None));
    let api_state = ApiState { sql_handler };

    let query_sql = "select sum(uint32s) from numbers limit 20";
    for format in ["greptimedb_v1", "influxdb_v1", "csv", "table"] {
        let query = create_query(format, query_sql, None);
        let json = http_handler::sql(
            State(api_state.clone()),
            query,
            axum::Extension(ctx.clone()),
            Form(http_handler::SqlQuery::default()),
        )
        .await;

        match json {
            HttpResponse::GreptimedbV1(resp) => match &resp.output()[0] {
                GreptimeQueryOutput::Records(records) => {
                    assert_eq!(1, records.num_rows());
                    let json = serde_json::to_string_pretty(&records).unwrap();
                    assert_eq!(
                        json,
                        r#"{
  "schema": {
    "column_schemas": [
      {
        "name": "sum(numbers.uint32s)",
        "data_type": "UInt64"
      }
    ]
  },
  "rows": [
    [
      4950
    ]
  ],
  "total_rows": 1
}"#
                    );
                }
                _ => unreachable!(),
            },
            HttpResponse::InfluxdbV1(resp) => {
                let json = serde_json::to_string_pretty(&resp.results()).unwrap();
                assert_eq!(
                    json,
                    r#"[
  {
    "statement_id": 0,
    "series": [
      {
        "name": "",
        "columns": [
          "sum(numbers.uint32s)"
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
            HttpResponse::Csv(resp) => {
                let resp = resp.into_response();
                assert_eq!(
                    resp.headers().get(header::CONTENT_TYPE),
                    Some(HeaderValue::from_static(mime::TEXT_CSV_UTF_8.as_ref())).as_ref(),
                );
                assert_eq!(
                    axum::body::to_bytes(resp.into_body(), usize::MAX)
                        .await
                        .unwrap(),
                    Bytes::from_static(b"4950\n"),
                );
            }
            HttpResponse::Table(resp) => {
                let resp = resp.into_response();
                assert_eq!(
                    resp.headers().get(header::CONTENT_TYPE),
                    Some(HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref())).as_ref(),
                );
                assert_eq!(
                    axum::body::to_bytes(resp.into_body(), usize::MAX)
                        .await
                        .unwrap(),
                    Bytes::from(
                        r#"┌─sum(numbers.uint32s)─┐
│ 4950                 │
└──────────────────────┘
"#
                    ),
                );
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn test_dashboard_sql_limit() {
    let sql_handler = create_testing_sql_query_handler(MemTable::specified_numbers_table(2000));
    let ctx = QueryContext::with_db_name(None);
    ctx.set_current_user(auth::userinfo_by_name(None));
    let api_state = ApiState { sql_handler };
    for format in ["greptimedb_v1", "csv", "table"] {
        let query = create_query(format, "select * from numbers", Some(1000));
        let sql_response = http_handler::sql(
            State(api_state.clone()),
            query,
            axum::Extension(ctx.clone()),
            Form(http_handler::SqlQuery::default()),
        )
        .await;

        match sql_response {
            HttpResponse::GreptimedbV1(resp) => match resp.output().first().unwrap() {
                Records(records) => {
                    assert_eq!(records.num_rows(), 1000);
                }
                _ => unreachable!(),
            },
            HttpResponse::Csv(resp) => match resp.output().first().unwrap() {
                Records(records) => {
                    assert_eq!(records.num_rows(), 1000);
                }
                _ => unreachable!(),
            },
            HttpResponse::Table(resp) => match resp.output().first().unwrap() {
                Records(records) => {
                    assert_eq!(records.num_rows(), 1000);
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn test_sql_form() {
    common_telemetry::init_default_ut_logging();

    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());

    let ctx = QueryContext::with_db_name(None);
    ctx.set_current_user(auth::userinfo_by_name(None));
    let api_state = ApiState { sql_handler };

    for format in ["greptimedb_v1", "influxdb_v1", "csv", "table"] {
        let form = create_form(format);
        let json = http_handler::sql(
            State(api_state.clone()),
            Query(http_handler::SqlQuery::default()),
            axum::Extension(ctx.clone()),
            form,
        )
        .await;

        match json {
            HttpResponse::GreptimedbV1(resp) => match &resp.output()[0] {
                GreptimeQueryOutput::Records(records) => {
                    assert_eq!(1, records.num_rows());
                    let json = serde_json::to_string_pretty(&records).unwrap();
                    assert_eq!(
                        json,
                        r#"{
  "schema": {
    "column_schemas": [
      {
        "name": "sum(numbers.uint32s)",
        "data_type": "UInt64"
      }
    ]
  },
  "rows": [
    [
      4950
    ]
  ],
  "total_rows": 1
}"#
                    );
                }
                _ => unreachable!(),
            },
            HttpResponse::InfluxdbV1(resp) => {
                let json = serde_json::to_string_pretty(&resp.results()).unwrap();
                assert_eq!(
                    json,
                    r#"[
  {
    "statement_id": 0,
    "series": [
      {
        "name": "",
        "columns": [
          "sum(numbers.uint32s)"
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
            HttpResponse::Csv(resp) => {
                let resp = resp.into_response();
                assert_eq!(
                    resp.headers().get(header::CONTENT_TYPE),
                    Some(HeaderValue::from_static(mime::TEXT_CSV_UTF_8.as_ref())).as_ref(),
                );
                assert_eq!(
                    axum::body::to_bytes(resp.into_body(), usize::MAX)
                        .await
                        .unwrap(),
                    Bytes::from_static(b"4950\n"),
                );
            }
            HttpResponse::Table(resp) => {
                let resp = resp.into_response();
                assert_eq!(
                    resp.headers().get(header::CONTENT_TYPE),
                    Some(HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref())).as_ref(),
                );
                assert_eq!(
                    axum::body::to_bytes(resp.into_body(), usize::MAX)
                        .await
                        .unwrap(),
                    Bytes::from(
                        r#"┌─sum(numbers.uint32s)─┐
│ 4950                 │
└──────────────────────┘
"#
                    ),
                );
            }
            _ => unreachable!(),
        }
    }
}

lazy_static::lazy_static! {
    static ref TEST_METRIC: prometheus::Counter =
      prometheus::register_counter!("test_metrics", "test metrics").unwrap();
}

#[tokio::test]
async fn test_metrics() {
    TEST_METRIC.inc();
    let stats = MetricsHandler;
    let text = http_handler::metrics(State(stats), Query(HashMap::default())).await;
    assert!(text.contains("test_metrics counter"));
}

fn create_query(format: &str, sql: &str, limit: Option<usize>) -> Query<http_handler::SqlQuery> {
    Query(http_handler::SqlQuery {
        sql: Some(sql.to_string()),
        format: Some(format.to_string()),
        limit,
        ..Default::default()
    })
}

fn create_form(format: &str) -> Form<http_handler::SqlQuery> {
    Form(http_handler::SqlQuery {
        sql: Some("select sum(uint32s) from numbers limit 20".to_string()),
        format: Some(format.to_string()),
        ..Default::default()
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

#[tokio::test]
async fn test_status() {
    let hostname = hostname::get()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let build_info = common_version::build_info();
    let expected_json = http_handler::StatusResponse {
        source_time: build_info.source_time,
        commit: build_info.commit,
        branch: build_info.branch,
        rustc_version: build_info.rustc,
        hostname,
        version: build_info.version,
    };

    let Json(json) = http_handler::status().await;
    assert_eq!(json, expected_json);
}

#[tokio::test]
async fn test_config() {
    let toml_str = r#"
            [http]
            addr = "127.0.0.1:4000"
            timeout = "0s"
            body_limit = "2GB"

            [logging]
            level = "debug"
            dir = "./greptimedb_data/test/logs"
        "#;
    let rs = http_handler::config(State(GreptimeOptionsConfigState {
        greptime_config_options: toml_str.to_string(),
    }))
    .await;
    assert_eq!(200_u16, rs.status().as_u16());
    assert_eq!(get_body(rs).await, toml_str);
}

async fn get_body(response: Response) -> Bytes {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
}
