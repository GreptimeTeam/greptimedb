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

use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use datafusion::assert_batches_eq;
use datafusion::prelude::{SessionConfig, SessionContext};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::*;
use crate::function::FunctionContext;
use crate::function_registry::FUNCTION_REGISTRY;

struct MockServer {
    endpoint: String,
    task: JoinHandle<()>,
}

impl MockServer {
    async fn start(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/systemone", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self { endpoint, task }
    }

    fn function(&self) -> JevFunction {
        JevFunction {
            enabled: true,
            api_key: Some("test-key".to_string()),
            endpoint: self.endpoint.clone(),
            model: "test-model".to_string(),
            ..Default::default()
        }
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn context(function: JevFunction) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_udf(AsyncScalarUDF::new(Arc::new(function)).into_scalar_udf());
    ctx
}

#[tokio::test]
async fn test_jev_sql_returns_and_filters_noul_probability() {
    let server = MockServer::start(Router::new().route(
        "/v1/systemone",
        post(
            |headers: HeaderMap, Json(request): Json<Value>| async move {
                assert_eq!(headers["authorization"], "Bearer test-key");
                assert_eq!(request["model"], "test-model");
                assert_eq!(request["questions"]["matches"]["type"], "noul");
                // The prompt is passed through verbatim.
                assert_eq!(
                    request["questions"]["matches"]["instructions"],
                    "payment failed"
                );
                let probability = match request["state"].as_str().unwrap() {
                    "failed" => {
                        // Complete this row after later rows to exercise result alignment.
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        0.95
                    }
                    "boundary" => 0.8,
                    "recovered" => 0.1,
                    "impossible" => 0.0,
                    "certain" => 1.0,
                    unexpected => panic!("unexpected state: {unexpected}"),
                };
                Json(json!({"answers": {"matches": {"type": "noul", "noul": probability}}}))
            },
        ),
    ))
    .await;
    let ctx = context(server.function());
    let events = "(VALUES (1, 'failed'), (2, 'boundary'), (3, 'recovered'), (4, NULL), (5, 'impossible'), (6, 'certain')) AS events(id, message)";
    let batches = ctx
        .sql(&format!(
            "SELECT id FROM {events} WHERE jev((message), 'payment failed') >= 0.8 ORDER BY id"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        [
            "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 6  |", "+----+"
        ],
        &batches
    );

    let batches = ctx
        .sql(&format!(
            "SELECT id, jev(message, 'payment failed') AS score FROM {events} ORDER BY score DESC NULLS LAST"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Float64);
    assert_batches_eq!(
        [
            "+----+-------+",
            "| id | score |",
            "+----+-------+",
            "| 6  | 1.0   |",
            "| 1  | 0.95  |",
            "| 2  | 0.8   |",
            "| 3  | 0.1   |",
            "| 5  | 0.0   |",
            "| 4  |       |",
            "+----+-------+"
        ],
        &batches
    );
}

#[tokio::test]
async fn test_jev_nulls_need_no_api_key() {
    let ctx = context(JevFunction {
        enabled: true,
        api_key: None,
        ..Default::default()
    });
    let batches = ctx
        .sql("SELECT jev(NULL, 'condition') AS a, jev('text', NULL) AS b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Float64);
    assert_batches_eq!(
        [
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "|   |   |",
            "+---+---+"
        ],
        &batches
    );

    let error = ctx
        .sql("SELECT jev('text', 'condition')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("JEV_API_KEY"), "{error}");

    let ctx = context(JevFunction {
        enabled: false,
        api_key: None,
        ..Default::default()
    });
    let error = ctx
        .sql("SELECT jev('text', 'condition')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("GREPTIMEDB_EXPERIMENTAL_JEV=true"),
        "{error}"
    );
}

#[tokio::test]
async fn test_jev_bad_api_responses_fail_the_query() {
    for (status, body, expected) in [
        (StatusCode::TOO_MANY_REQUESTS, "{}", "jev request failed"),
        (StatusCode::OK, "not json", "jev response is not valid JSON"),
        (
            StatusCode::OK,
            r#"{"answers":{}}"#,
            "jev response must contain",
        ),
        (
            StatusCode::OK,
            r#"{"answers":{"matches":{"type":"noul","noul":1.1}}}"#,
            "jev response must contain",
        ),
        (
            StatusCode::OK,
            r#"{"answers":{"matches":{"type":"noul","noul":-0.1}}}"#,
            "jev response must contain",
        ),
        (
            StatusCode::OK,
            r#"{"answers":{"matches":{"type":"score","noul":0.9}}}"#,
            "jev response must contain",
        ),
    ] {
        let server = MockServer::start(
            Router::new().route("/v1/systemone", post(move || async move { (status, body) })),
        )
        .await;
        let ctx = context(server.function());
        let error = ctx
            .sql("SELECT jev('text', 'condition')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[tokio::test]
#[ignore = "calls the real Jev API; requires JEV_API_KEY and GREPTIMEDB_EXPERIMENTAL_JEV=true"]
async fn test_jev_live() {
    let ctx = SessionContext::new();
    let function = FUNCTION_REGISTRY.get_function("jev").unwrap();
    ctx.register_udf(function.provide(FunctionContext::default()));
    let batches = ctx.sql(
        "SELECT id FROM (VALUES
            (1, 'Payment failed permanently: all three retries exhausted; the payment is still unsuccessful.'),
            (2, 'Payment succeeded on the second retry; the payment is complete.'),
            (3, 'User logged in successfully.')
         ) AS events(id, message)
         WHERE jev(message, 'The event reports that a payment still failed after retries.') >= 0.8
         ORDER BY id",
    ).await.unwrap().collect().await.unwrap();
    assert_batches_eq!(["+----+", "| id |", "+----+", "| 1  |", "+----+"], &batches);
}
