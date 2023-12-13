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

use axum::body::{Body, Bytes};
use axum::extract::{Json, Query, RawBody, State};
use axum::Form;
use http_body::combinators::UnsyncBoxBody;
use hyper::Response;
use servers::http::{
    handler as http_handler, script as script_handler, ApiState, GreptimeOptionsConfigState,
    JsonOutput, JsonResponse,
};
use servers::metrics_handler::MetricsHandler;
use session::context::QueryContext;
use table::test_util::MemTable;

use crate::{
    create_testing_script_handler, create_testing_sql_query_handler, ScriptHandlerRef,
    ServerSqlQueryHandlerRef,
};

#[tokio::test]
async fn test_sql_not_provided() {
    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let ctx = QueryContext::arc();
    ctx.set_current_user(Some(auth::userinfo_by_name(None)));
    let api_state = ApiState {
        sql_handler,
        script_handler: None,
    };

    for format in ["greptimedb_v1", "influxdb_v1"] {
        let query = http_handler::SqlQuery {
            db: None,
            sql: None,
            format: Some(format.to_string()),
            epoch: None,
        };
        let Json(json) = http_handler::sql(
            State(api_state.clone()),
            Query(query),
            axum::Extension(ctx.clone()),
            Form(http_handler::SqlQuery::default()),
        )
        .await;

        match json {
            JsonResponse::GreptimedbV1(resp) => {
                assert!(!resp.success());
                assert_eq!(
                    Some(&"sql parameter is required.".to_string()),
                    resp.error()
                );
                assert!(resp.output().is_empty());
            }
            JsonResponse::InfluxdbV1(resp) => {
                assert!(!resp.success());
                assert_eq!(
                    Some(&"sql parameter is required.".to_string()),
                    resp.error()
                );
                assert!(resp.results().is_empty());
            }
        }
    }
}

#[tokio::test]
async fn test_sql_output_rows() {
    common_telemetry::init_default_ut_logging();

    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());

    let ctx = QueryContext::arc();
    ctx.set_current_user(Some(auth::userinfo_by_name(None)));
    let api_state = ApiState {
        sql_handler,
        script_handler: None,
    };

    for format in ["greptimedb_v1", "influxdb_v1"] {
        let query = create_query(format);
        let Json(json) = http_handler::sql(
            State(api_state.clone()),
            query,
            axum::Extension(ctx.clone()),
            Form(http_handler::SqlQuery::default()),
        )
        .await;

        match json {
            JsonResponse::GreptimedbV1(resp) => {
                assert!(resp.success(), "{resp:?}");
                assert!(resp.error().is_none());
                match &resp.output()[0] {
                    JsonOutput::Records(records) => {
                        assert_eq!(1, records.num_rows());
                        let json = serde_json::to_string_pretty(&records).unwrap();
                        assert_eq!(
                            json,
                            r#"{
  "schema": {
    "column_schemas": [
      {
        "name": "SUM(numbers.uint32s)",
        "data_type": "UInt64"
      }
    ]
  },
  "rows": [
    [
      4950
    ]
  ]
}"#
                        );
                    }
                    _ => unreachable!(),
                }
            }
            JsonResponse::InfluxdbV1(resp) => {
                assert!(resp.success(), "{resp:?}");
                assert!(resp.error().is_none());

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
        }
    }
}

#[tokio::test]
async fn test_sql_form() {
    common_telemetry::init_default_ut_logging();

    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());

    let ctx = QueryContext::arc();
    ctx.set_current_user(Some(auth::userinfo_by_name(None)));
    let api_state = ApiState {
        sql_handler,
        script_handler: None,
    };

    for format in ["greptimedb_v1", "influxdb_v1"] {
        let form = create_form(format);
        let Json(json) = http_handler::sql(
            State(api_state.clone()),
            Query(http_handler::SqlQuery::default()),
            axum::Extension(ctx.clone()),
            form,
        )
        .await;

        match json {
            JsonResponse::GreptimedbV1(resp) => {
                assert!(resp.success(), "{resp:?}");
                assert!(resp.error().is_none());
                match &resp.output()[0] {
                    JsonOutput::Records(records) => {
                        assert_eq!(1, records.num_rows());
                        let json = serde_json::to_string_pretty(&records).unwrap();
                        assert_eq!(
                            json,
                            r#"{
  "schema": {
    "column_schemas": [
      {
        "name": "SUM(numbers.uint32s)",
        "data_type": "UInt64"
      }
    ]
  },
  "rows": [
    [
      4950
    ]
  ]
}"#
                        );
                    }
                    _ => unreachable!(),
                }
            }
            JsonResponse::InfluxdbV1(resp) => {
                assert!(resp.success(), "{resp:?}");
                assert!(resp.error().is_none());

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
    let text = http_handler::metrics(axum::extract::State(stats), Query(HashMap::default())).await;
    assert!(text.contains("test_metrics counter"));
}

async fn insert_script(
    script: String,
    script_handler: ScriptHandlerRef,
    sql_handler: ServerSqlQueryHandlerRef,
) {
    let body = RawBody(Body::from(script.clone()));
    let invalid_query = create_invalid_script_query();
    let Json(json) = script_handler::scripts(
        State(ApiState {
            sql_handler: sql_handler.clone(),
            script_handler: Some(script_handler.clone()),
        }),
        invalid_query,
        body,
    )
    .await;
    let JsonResponse::GreptimedbV1(json) = json else {
        unreachable!()
    };
    assert!(!json.success(), "{json:?}");
    assert_eq!(json.error().unwrap(), "invalid schema");

    let body = RawBody(Body::from(script.clone()));
    let exec = create_script_query();
    // Insert the script
    let Json(json) = script_handler::scripts(
        State(ApiState {
            sql_handler: sql_handler.clone(),
            script_handler: Some(script_handler.clone()),
        }),
        exec,
        body,
    )
    .await;
    let JsonResponse::GreptimedbV1(json) = json else {
        unreachable!()
    };
    assert!(json.success(), "{json:?}");
    assert!(json.error().is_none());
    assert!(json.output().is_empty());
}

#[tokio::test]
async fn test_scripts() {
    common_telemetry::init_default_ut_logging();

    let script = r#"
@copr(sql='select uint32s as number from numbers limit 5', args=['number'], returns=['n'])
def test(n) -> vector[i64]:
    return n;
"#
    .to_string();
    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let script_handler = create_testing_script_handler(MemTable::default_numbers_table());

    insert_script(script.clone(), script_handler.clone(), sql_handler.clone()).await;
    // Run the script
    let exec = create_script_query();
    let Json(json) = script_handler::run_script(
        State(ApiState {
            sql_handler,
            script_handler: Some(script_handler),
        }),
        exec,
    )
    .await;
    let JsonResponse::GreptimedbV1(json) = json else {
        unreachable!()
    };
    assert!(json.success(), "{json:?}");
    assert!(json.error().is_none());

    match &json.output()[0] {
        JsonOutput::Records(records) => {
            let json = serde_json::to_string_pretty(&records).unwrap();
            assert_eq!(5, records.num_rows());
            assert_eq!(
                json,
                r#"{
  "schema": {
    "column_schemas": [
      {
        "name": "n",
        "data_type": "Int64"
      }
    ]
  },
  "rows": [
    [
      0
    ],
    [
      1
    ],
    [
      2
    ],
    [
      3
    ],
    [
      4
    ]
  ]
}"#
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_scripts_with_params() {
    common_telemetry::init_default_ut_logging();

    let script = r#"
@copr(sql='select uint32s as number from numbers limit 5', args=['number'], returns=['n'])
def test(n, **params)  -> vector[i64]:
    return n + int(params['a'])
"#
    .to_string();
    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let script_handler = create_testing_script_handler(MemTable::default_numbers_table());

    insert_script(script.clone(), script_handler.clone(), sql_handler.clone()).await;
    // Run the script
    let mut exec = create_script_query();
    let _ = exec.0.params.insert("a".to_string(), "42".to_string());
    let Json(json) = script_handler::run_script(
        State(ApiState {
            sql_handler,
            script_handler: Some(script_handler),
        }),
        exec,
    )
    .await;
    let JsonResponse::GreptimedbV1(json) = json else {
        unreachable!()
    };
    assert!(json.success(), "{json:?}");
    assert!(json.error().is_none());

    match &json.output()[0] {
        JsonOutput::Records(records) => {
            let json = serde_json::to_string_pretty(&records).unwrap();
            assert_eq!(5, records.num_rows());
            assert_eq!(
                json,
                r#"{
  "schema": {
    "column_schemas": [
      {
        "name": "n",
        "data_type": "Int64"
      }
    ]
  },
  "rows": [
    [
      42
    ],
    [
      43
    ],
    [
      44
    ],
    [
      45
    ],
    [
      46
    ]
  ]
}"#
            );
        }
        _ => unreachable!(),
    }
}

fn create_script_query() -> Query<script_handler::ScriptQuery> {
    Query(script_handler::ScriptQuery {
        db: Some("test".to_string()),
        name: Some("test".to_string()),
        ..Default::default()
    })
}

fn create_invalid_script_query() -> Query<script_handler::ScriptQuery> {
    Query(script_handler::ScriptQuery {
        db: None,
        name: None,
        ..Default::default()
    })
}

fn create_query(format: &str) -> Query<http_handler::SqlQuery> {
    Query(http_handler::SqlQuery {
        sql: Some("select sum(uint32s) from numbers limit 20".to_string()),
        db: None,
        format: Some(format.to_string()),
        epoch: None,
    })
}

fn create_form(format: &str) -> Form<http_handler::SqlQuery> {
    Form(http_handler::SqlQuery {
        sql: Some("select sum(uint32s) from numbers limit 20".to_string()),
        db: None,
        format: Some(format.to_string()),
        epoch: None,
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
    let expected_json = http_handler::StatusResponse {
        source_time: env!("SOURCE_TIMESTAMP"),
        commit: env!("GIT_COMMIT"),
        branch: env!("GIT_BRANCH"),
        rustc_version: env!("RUSTC_VERSION"),
        hostname,
        version: env!("CARGO_PKG_VERSION"),
    };

    let Json(json) = http_handler::status().await;
    assert_eq!(json, expected_json);
}

#[tokio::test]
async fn test_config() {
    let toml_str = r#"
            mode = "distributed"

            [http]
            addr = "127.0.0.1:4000"
            timeout = "30s"
            body_limit = "2GB"

            [logging]
            level = "debug"
            dir = "/tmp/greptimedb/test/logs"
        "#;
    let rs = http_handler::config(State(GreptimeOptionsConfigState {
        greptime_config_options: toml_str.to_string(),
    }))
    .await;
    assert_eq!(200_u16, rs.status().as_u16());
    assert_eq!(get_body(rs).await, toml_str);
}

async fn get_body(response: Response<UnsyncBoxBody<Bytes, axum::Error>>) -> Bytes {
    hyper::body::to_bytes(response.into_body()).await.unwrap()
}
