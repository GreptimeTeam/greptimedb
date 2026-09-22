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

use std::sync::atomic::{AtomicUsize, Ordering};

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

    fn function<Q: AiQuestion>(&self) -> AiFunction<Q> {
        AiFunction {
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

fn context<Q: AiQuestion>(function: AiFunction<Q>) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_udf(AsyncScalarUDF::new(Arc::new(function)).into_scalar_udf());
    let json_get_float = FUNCTION_REGISTRY.get_function("json_get_float").unwrap();
    ctx.register_udf(json_get_float.provide(FunctionContext::default()));
    ctx
}

#[test]
fn test_ai_scalar_criteria_share_allocation_and_skip_null_rows() {
    fn assert_shared_criteria<Q: AiQuestion>(criteria: &str) {
        let function = AiFunction::<Q>::default();
        for (texts, criteria) in [
            (vec![None, Some("first"), Some("second")], criteria),
            (vec![None, None], "not json"),
            (vec![], "not json"),
        ] {
            let number_rows = texts.len();
            let expected_requests = texts.iter().flatten().count();
            let args = vec![
                ColumnarValue::Array(Arc::new(StringViewArray::from(texts))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("prompt".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(criteria.to_string()))),
            ];
            let requests = function.prepare_requests(&args, number_rows).unwrap();
            assert_eq!(requests.len(), number_rows);
            assert_eq!(requests.iter().flatten().count(), expected_requests);

            // Regression: parsed constant criteria must not be duplicated for each row.
            let mut criteria = requests.iter().flatten().map(|request| &request.criteria);
            if let Some(first) = criteria.next() {
                assert!(criteria.all(|other| Arc::ptr_eq(first, other)));
            }
        }
    }

    assert_shared_criteria::<Choice>(r#"{"billing":null,"technical":"errors"}"#);
    assert_shared_criteria::<Score>(r#"["low","high"]"#);
}

#[tokio::test]
async fn test_ai_match_sql_returns_and_filters_noul_probability() {
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
            "SELECT id FROM {events} WHERE ai_match((message), 'payment failed') >= 0.8 ORDER BY id"
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
            "SELECT id, ai_match(message, 'payment failed') AS score FROM {events} ORDER BY score DESC NULLS LAST"
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
async fn test_ai_match_nulls_need_no_api_key() {
    let ctx = context(AiFunction::<Noul> {
        enabled: true,
        api_key: None,
        ..Default::default()
    });
    let batches = ctx
        .sql("SELECT ai_match(NULL, 'condition') AS a, ai_match('text', NULL) AS b")
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
        .sql("SELECT ai_match('text', 'condition')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("JEV_API_KEY"), "{error}");

    let ctx = context(AiFunction::<Noul> {
        enabled: false,
        api_key: None,
        ..Default::default()
    });
    let error = ctx
        .sql("SELECT ai_match('text', 'condition')")
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
async fn test_ai_match_bad_api_responses_fail_the_query() {
    for (status, body, expected) in [
        (
            StatusCode::TOO_MANY_REQUESTS,
            "{}",
            "ai_match request failed",
        ),
        (
            StatusCode::OK,
            "not json",
            "ai_match response is not valid JSON",
        ),
        (
            StatusCode::OK,
            r#"{"answers":{}}"#,
            "ai_match response must contain",
        ),
        (
            StatusCode::OK,
            r#"{"answers":{"matches":{"type":"noul","noul":1.1}}}"#,
            "ai_match response must contain",
        ),
        (
            StatusCode::OK,
            r#"{"answers":{"matches":{"type":"noul","noul":-0.1}}}"#,
            "ai_match response must contain",
        ),
        (
            StatusCode::OK,
            r#"{"answers":{"matches":{"type":"score","noul":0.9}}}"#,
            "ai_match response must contain",
        ),
    ] {
        let server = MockServer::start(
            Router::new().route("/v1/systemone", post(move || async move { (status, body) })),
        )
        .await;
        let ctx = context(server.function::<Noul>());
        let error = ctx
            .sql("SELECT ai_match('text', 'condition')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[tokio::test]
async fn test_ai_choose_returns_and_filters_supplied_labels() {
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
            "SELECT id, ai_choose(message, prompt, criteria) AS team FROM {events} ORDER BY id"
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
            "SELECT id FROM {events} WHERE ai_choose(message, prompt, criteria) = 'billing'"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(["+----+", "| id |", "+----+", "| 1  |", "+----+"], &batches);

    let batches = ctx
        .sql(
            r#"SELECT id, ai_choose(message, 'Route the ticket',
                '{"billing":null,"technical":{"examples":["crash"]}}') AS team
            FROM (VALUES (1, 'refund'), (2, NULL), (3, 'refund')) AS events(id, message)
            ORDER BY id"#,
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        [
            "+----+---------+",
            "| id | team    |",
            "+----+---------+",
            "| 1  | billing |",
            "| 2  |         |",
            "| 3  | billing |",
            "+----+---------+"
        ],
        &batches
    );
}

#[tokio::test]
async fn test_ai_score_returns_fractional_scores_on_each_rows_scale() {
    let server = MockServer::start(Router::new().route(
        "/v1/systemone",
        post(|Json(request): Json<Value>| async move {
            let question = &request["questions"]["matches"];
            assert_eq!(question["type"], "score");
            assert_eq!(question["instructions"], "Rate severity");
            let (score, probabilities) = match request["state"].as_str().unwrap() {
                "low" => {
                    assert_eq!(question["criteria"], json!(["low", {"description": "medium"}, ["high"]]));
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    (0.0, json!({"0":1.0,"1":0.0,"2":0.0}))
                }
                "fractional" => (1.25, json!({"2":0.25,"0":0.0,"1":0.75})),
                "high" => (2.0, json!({"0":0.0,"1":0.0,"2":1.0})),
                "binary" => {
                    assert_eq!(question["criteria"], json!(["low", "high"]));
                    (1.0, json!({"0":0.0,"1":1.0}))
                }
                "ten levels" => {
                    assert_eq!(question["criteria"].as_array().unwrap().len(), 10);
                    let probabilities: Map<String, Value> = (0..10)
                        .map(|level| (level.to_string(), json!(if level == 9 { 1.0 } else { 0.0 })))
                        .collect();
                    (9.0, Value::Object(probabilities))
                }
                unexpected => panic!("unexpected state: {unexpected}"),
            };
            Json(json!({"answers": {"matches": {"type": "score", "score": score, "confidence": 0.4, "probabilities": probabilities}}}))
        }),
    ))
    .await;
    let ctx = context(server.function::<Score>());
    let batches = ctx
        .sql(
            r#"
        SELECT id, json_get_float(ai_score(message, 'Rate severity', criteria), 'score') AS severity
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

    let batches = ctx
        .sql(
            r#"SELECT id, json_get_float(ai_score(message, 'Rate severity',
                '["low",{"description":"medium"},["high"]]'), 'score') AS severity
            FROM (VALUES (1, 'low'), (2, 'fractional'), (3, 'high'), (4, NULL)) AS events(id, message)
            ORDER BY id"#,
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        [
            "+----+----------+",
            "| id | severity |",
            "+----+----------+",
            "| 1  | 0.0      |",
            "| 2  | 1.25     |",
            "| 3  | 2.0      |",
            "| 4  |          |",
            "+----+----------+"
        ],
        &batches
    );
}

#[tokio::test]
async fn test_ai_score_preserves_uncertainty_in_one_evaluation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let request_calls = calls.clone();
    let server = MockServer::start(Router::new().route(
        "/v1/systemone",
        post(move |Json(request): Json<Value>| {
            request_calls.fetch_add(1, Ordering::Relaxed);
            async move {
                let (confidence, probabilities) = match request["state"].as_str().unwrap() {
                    "medium" => (1.0, json!({"2":0.0,"0":0.0,"1":1.0})),
                    "split" => (0.25, json!({"2":0.5,"0":0.5,"1":0.0})),
                    unexpected => panic!("unexpected state: {unexpected}"),
                };
                Json(json!({"answers":{"matches":{
                    "type":"score", "score":1.0, "confidence":confidence,
                    "probabilities":probabilities
                }}}))
            }
        }),
    ))
    .await;
    let ctx = context(server.function::<Score>());
    let rated = r#"(SELECT id, ai_score(message, 'Rate severity', '["low","medium","high"]') AS rating
        FROM (VALUES (1, 'medium'), (2, 'split'), (3, NULL)) AS events(id, message)) AS rated"#;
    let batches = ctx
        .sql(&format!(
            "SELECT id, json_get_float(rating, 'score') AS score,
                json_get_float(rating, 'confidence') AS confidence,
                json_get_float(rating, 'probabilities[0]') AS p0,
                json_get_float(rating, 'probabilities[1]') AS p1,
                json_get_float(rating, 'probabilities[2]') AS p2
             FROM {rated} ORDER BY id"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        [
            "+----+-------+------------+-----+-----+-----+",
            "| id | score | confidence | p0  | p1  | p2  |",
            "+----+-------+------------+-----+-----+-----+",
            "| 1  | 1.0   | 1.0        | 0.0 | 1.0 | 0.0 |",
            "| 2  | 1.0   | 0.25       | 0.5 | 0.0 | 0.5 |",
            "| 3  |       |            |     |     |     |",
            "+----+-------+------------+-----+-----+-----+"
        ],
        &batches
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);

    let batches = ctx
        .sql(&format!(
            "SELECT id FROM {rated} WHERE json_get_float(rating, 'confidence') >= 0.8
             ORDER BY json_get_float(rating, 'score') DESC"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(["+----+", "| id |", "+----+", "| 1  |", "+----+"], &batches);
    assert_eq!(calls.load(Ordering::Relaxed), 4);
}

#[test]
fn test_ai_score_preserves_provider_precision() {
    let answer = json!({
        "score":1.6, "confidence":0.92,
        "probabilities":{"2":0.6999995,"0":0.1,"1":0.2}
    });
    let criteria = vec![json!("low"), json!("medium"), json!("high")];
    let ScalarValue::BinaryView(Some(bytes)) = Score::parse_answer(&answer, &criteria).unwrap()
    else {
        panic!("expected a JSONB object");
    };
    let object: Value =
        serde_json::from_str(&jsonb::from_slice(&bytes).unwrap().to_string()).unwrap();
    assert_eq!(object.as_object().unwrap().len(), 3);
    assert_eq!(object["score"].as_f64(), Some(1.6));
    assert_eq!(object["confidence"].as_f64(), Some(0.92));
    let probabilities: Vec<f64> = object["probabilities"]
        .as_array()
        .unwrap()
        .iter()
        .map(|probability| probability.as_f64().unwrap())
        .collect();
    assert_eq!(probabilities, vec![0.1, 0.2, 0.6999995]);
}

#[tokio::test]
async fn test_ai_score_rejects_invalid_uncertainty() {
    for (field, value) in [
        ("confidence", Value::Null),
        ("confidence", json!(-0.1)),
        ("confidence", json!(1.1)),
        ("confidence", json!("0.9")),
        ("probabilities", Value::Null),
        ("probabilities", json!([0.0, 1.0, 0.0])),
        ("probabilities", json!({"0":1.0})),
        ("probabilities", json!({"0":0.5,"2":0.5,"3":0.0})),
        ("probabilities", json!({"0":0.0,"1":1.0,"2":0.0,"3":0.0})),
        ("probabilities", json!({"0":-0.1,"1":0.6,"2":0.5})),
        ("probabilities", json!({"0":0.0,"1":1.1,"2":0.0})),
        ("probabilities", json!({"0":0.0,"1":0.95,"2":"0.05"})),
        ("probabilities", json!({"0":0.0,"1":0.0,"2":0.0})),
        ("probabilities", json!({"0":0.5,"1":0.5,"2":0.5})),
    ] {
        let mut answer = json!({
            "type":"score", "score":1.0, "confidence":0.9,
            "probabilities":{"0":0.0,"1":1.0,"2":0.0}
        });
        answer[field] = value;
        let server = MockServer::start(Router::new().route(
            "/v1/systemone",
            post(move || {
                let answer = answer.clone();
                async move { Json(json!({"answers":{"matches":answer}})) }
            }),
        ))
        .await;
        let ctx = context(server.function::<Score>());
        let error = ctx
            .sql(r#"SELECT ai_score('text', 'prompt', '["low","medium","high"]')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ai_score response"), "{error}");
    }
}

#[tokio::test]
async fn test_ai_invalid_criteria_fail_before_requests() {
    let choice_ctx = context(AiFunction::<Choice> {
        enabled: true,
        api_key: None,
        ..Default::default()
    });
    let score_ctx = context(AiFunction::<Score> {
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
            "ai_choose",
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
            "ai_score",
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
        let error = ctx
            .sql(&format!(
                "SELECT {name}(message, 'prompt', 'not json') FROM (VALUES (NULL), ('text')) AS input(message)"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(&format!("{name} criteria")),
            "{error}"
        );
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
async fn test_ai_choose_and_score_reject_invalid_answers() {
    for (name, criteria, answer) in [
        (
            "ai_choose",
            r#"{"billing":null}"#,
            json!({"type":"score","choice":"billing"}),
        ),
        (
            "ai_choose",
            r#"{"billing":null}"#,
            json!({"type":"choice","choice":"unknown"}),
        ),
        (
            "ai_choose",
            r#"{"billing":null}"#,
            json!({"type":"choice","choice":1}),
        ),
        ("ai_choose", r#"{"billing":null}"#, json!({"type":"choice"})),
        (
            "ai_score",
            r#"["low","medium","high"]"#,
            json!({"type":"noul","score":0.5}),
        ),
        (
            "ai_score",
            r#"["low","medium","high"]"#,
            json!({"type":"score","score":2.1}),
        ),
        (
            "ai_score",
            r#"["low","high"]"#,
            json!({"type":"score","score":-0.1}),
        ),
        (
            "ai_score",
            r#"["low","high"]"#,
            json!({"type":"score","score":"NaN"}),
        ),
        ("ai_score", r#"["low","high"]"#, json!({"type":"score"})),
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
async fn test_ai_match_live() {
    let ctx = SessionContext::new();
    let function = FUNCTION_REGISTRY.get_function("ai_match").unwrap();
    ctx.register_udf(function.provide(FunctionContext::default()));
    let batches = ctx.sql(
        "SELECT id FROM (VALUES
            (1, 'Payment failed permanently: all three retries exhausted; the payment is still unsuccessful.'),
            (2, 'Payment succeeded on the second retry; the payment is complete.'),
            (3, 'User logged in successfully.')
         ) AS events(id, message)
         WHERE ai_match(message, 'The event reports that a payment still failed after retries.') >= 0.8
         ORDER BY id",
    ).await.unwrap().collect().await.unwrap();
    assert_batches_eq!(["+----+", "| id |", "+----+", "| 1  |", "+----+"], &batches);
}
