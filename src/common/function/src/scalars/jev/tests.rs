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

    fn function<Q: JevQuestion>(&self) -> JevFunction<Q> {
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

fn context<Q: JevQuestion>(function: JevFunction<Q>) -> SessionContext {
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
    let ctx = context(server.function::<Noul>());
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
    let ctx = context(JevFunction::<Noul> {
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

    let ctx = context(JevFunction::<Noul> {
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
        let ctx = context(server.function::<Noul>());
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
async fn test_jev_choice_returns_and_filters_supplied_labels() {
    let server = MockServer::start(Router::new().route(
        "/v1/systemone",
        post(|Json(request): Json<Value>| async move {
            let question = &request["questions"]["matches"];
            assert_eq!(question["type"], "choice");
            assert_eq!(question["instructions"], "Route the ticket");
            let choice = match request["state"].as_str().unwrap() {
                "refund" => {
                    assert_eq!(question["criteria"], json!({"billing": null, "technical": {"examples": ["crash"]}}));
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    "billing"
                }
                "crash" => {
                    assert_eq!(question["criteria"], json!({"billing": ["refunds"], "technical": "errors"}));
                    "technical"
                }
                unexpected => panic!("unexpected state: {unexpected}"),
            };
            Json(json!({"answers": {"matches": {"type": "choice", "choice": choice, "confidence": 0.7}}}))
        }),
    ))
    .await;
    let ctx = context(server.function::<Choice>());
    let events = r#"(VALUES
        (1, 'refund', 'Route the ticket', '{"billing":null,"technical":{"examples":["crash"]}}'),
        (2, 'crash', 'Route the ticket', '{"billing":["refunds"],"technical":"errors"}'),
        (3, NULL, 'Route the ticket', 'invalid JSON'),
        (4, 'refund', NULL, 'invalid JSON'),
        (5, 'refund', 'Route the ticket', NULL)
    ) AS events(id, message, prompt, criteria)"#;
    let batches = ctx
        .sql(&format!(
            "SELECT id, jev_choice(message, prompt, criteria) AS team FROM {events} ORDER BY id"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Utf8);
    assert_batches_eq!(
        [
            "+----+-----------+",
            "| id | team      |",
            "+----+-----------+",
            "| 1  | billing   |",
            "| 2  | technical |",
            "| 3  |           |",
            "| 4  |           |",
            "| 5  |           |",
            "+----+-----------+"
        ],
        &batches
    );
    let batches = ctx
        .sql(&format!(
            "SELECT id FROM {events} WHERE jev_choice(message, prompt, criteria) = 'billing'"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(["+----+", "| id |", "+----+", "| 1  |", "+----+"], &batches);
}

#[tokio::test]
async fn test_jev_score_returns_fractional_scores_on_each_rows_scale() {
    let server = MockServer::start(Router::new().route(
        "/v1/systemone",
        post(|Json(request): Json<Value>| async move {
            let question = &request["questions"]["matches"];
            assert_eq!(question["type"], "score");
            assert_eq!(question["instructions"], "Rate severity");
            let score = match request["state"].as_str().unwrap() {
                "low" => {
                    assert_eq!(question["criteria"], json!(["low", {"description": "medium"}, ["high"]]));
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    0.0
                }
                "fractional" => 1.25,
                "high" => 2.0,
                "binary" => {
                    assert_eq!(question["criteria"], json!(["low", "high"]));
                    1.0
                }
                "ten levels" => {
                    assert_eq!(question["criteria"].as_array().unwrap().len(), 10);
                    9.0
                }
                unexpected => panic!("unexpected state: {unexpected}"),
            };
            Json(json!({"answers": {"matches": {"type": "score", "score": score, "confidence": 0.4}}}))
        }),
    ))
    .await;
    let ctx = context(server.function::<Score>());
    let batches = ctx
        .sql(
            r#"
        SELECT id, jev_score(message, 'Rate severity', criteria) AS severity
        FROM (VALUES
            (1, 'low', '["low", {"description":"medium"}, ["high"]]'),
            (2, 'fractional', '["low", "medium", "high"]'),
            (3, 'high', '["low", "medium", "high"]'),
            (4, 'binary', '["low", "high"]'),
            (5, 'ten levels', '["a","b","c","d","e","f","g","h","i","j"]'),
            (6, NULL, 'invalid JSON'),
            (7, 'high', NULL)
        ) AS events(id, message, criteria)
        ORDER BY severity DESC NULLS LAST, id
    "#,
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Float64);
    assert_batches_eq!(
        [
            "+----+----------+",
            "| id | severity |",
            "+----+----------+",
            "| 5  | 9.0      |",
            "| 3  | 2.0      |",
            "| 2  | 1.25     |",
            "| 4  | 1.0      |",
            "| 1  | 0.0      |",
            "| 6  |          |",
            "| 7  |          |",
            "+----+----------+"
        ],
        &batches
    );
}

#[tokio::test]
async fn test_jev_invalid_criteria_fail_before_requests() {
    let choice_ctx = context(JevFunction::<Choice> {
        enabled: true,
        api_key: None,
        ..Default::default()
    });
    let score_ctx = context(JevFunction::<Score> {
        enabled: true,
        api_key: None,
        ..Default::default()
    });
    let too_many_choices: Map<String, Value> = (0..256)
        .map(|index| (index.to_string(), Value::Null))
        .collect();
    for (ctx, name, valid, invalid) in [
        (
            &choice_ctx,
            "jev_choice",
            r#"{"billing":null}"#,
            vec![
                "not json".to_string(),
                "null".to_string(),
                "{}".to_string(),
                "[]".to_string(),
                r#"{"billing":1}"#.to_string(),
                r#"{"billing":true}"#.to_string(),
                Value::Object(too_many_choices).to_string(),
            ],
        ),
        (
            &score_ctx,
            "jev_score",
            r#"["low","high"]"#,
            vec![
                "not json".to_string(),
                "null".to_string(),
                "{}".to_string(),
                "[]".to_string(),
                r#"["low"]"#.to_string(),
                r#"["low",null]"#.to_string(),
                r#"["low",1]"#.to_string(),
                r#"["low",true]"#.to_string(),
                json!(vec!["level"; 11]).to_string(),
            ],
        ),
    ] {
        for criteria in invalid {
            // A later invalid row must fail before even creating a client for the first row.
            let error = ctx.sql(&format!(
                "SELECT {name}('text', 'prompt', criteria) FROM (VALUES ('{valid}'), ('{criteria}')) AS input(criteria)"
            )).await.unwrap().collect().await.unwrap_err();
            assert!(
                error.to_string().contains(&format!("{name} criteria")),
                "{error}"
            );
        }
    }
}

#[tokio::test]
async fn test_jev_choice_and_score_reject_invalid_answers() {
    for (name, criteria, answer) in [
        (
            "jev_choice",
            r#"{"billing":null}"#,
            json!({"type":"score","choice":"billing"}),
        ),
        (
            "jev_choice",
            r#"{"billing":null}"#,
            json!({"type":"choice","choice":"unknown"}),
        ),
        (
            "jev_choice",
            r#"{"billing":null}"#,
            json!({"type":"choice","choice":1}),
        ),
        (
            "jev_choice",
            r#"{"billing":null}"#,
            json!({"type":"choice"}),
        ),
        (
            "jev_score",
            r#"["low","medium","high"]"#,
            json!({"type":"noul","score":0.5}),
        ),
        (
            "jev_score",
            r#"["low","medium","high"]"#,
            json!({"type":"score","score":2.1}),
        ),
        (
            "jev_score",
            r#"["low","high"]"#,
            json!({"type":"score","score":-0.1}),
        ),
        (
            "jev_score",
            r#"["low","high"]"#,
            json!({"type":"score","score":"NaN"}),
        ),
        ("jev_score", r#"["low","high"]"#, json!({"type":"score"})),
    ] {
        let server = MockServer::start(Router::new().route(
            "/v1/systemone",
            post(move || {
                let answer = answer.clone();
                async move { Json(json!({"answers":{"matches":answer}})) }
            }),
        ))
        .await;
        let ctx = context(server.function::<Choice>());
        ctx.register_udf(
            AsyncScalarUDF::new(Arc::new(server.function::<Score>())).into_scalar_udf(),
        );
        let error = ctx
            .sql(&format!("SELECT {name}('text', 'prompt', '{criteria}')"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("{name} response must contain")),
            "{error}"
        );
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
