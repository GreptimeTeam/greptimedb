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

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use axum::Form;
use axum::extract::{Json, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use common_query::{Output, OutputData};
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::Result as DfResult;
use datafusion_expr::LogicalPlan;
use datatypes::schema::SchemaRef;
use futures::Stream;
use futures_util::StreamExt;
use headers::HeaderValue;
use mime_guess::mime;
use query::parser::PromQuery;
use query::query_engine::DescribeResult;
use serde_json::Value;
use servers::error::Result;
use servers::http::GreptimeQueryOutput::Records;
use servers::http::test_helpers::TestClient;
use servers::http::{
    ApiState, GreptimeOptionsConfigState, GreptimeQueryOutput, HttpOptions, HttpResponse,
    HttpServerBuilder, handler as http_handler,
};
use servers::metrics_handler::MetricsHandler;
use servers::query_handler::sql::{ServerSqlQueryHandlerRef, SqlQueryHandler};
use session::context::{QueryContext, QueryContextRef};
use sql::statements::statement::Statement;
use table::test_util::MemTable;
use tokio::sync::{Notify, watch};

use crate::create_testing_sql_query_handler;

struct DelayedRecordBatchStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    delay: Pin<Box<tokio::time::Sleep>>,
    delayed: bool,
}

impl DelayedRecordBatchStream {
    fn new(inner: SendableRecordBatchStream, delay: Duration) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            delay: Box::pin(tokio::time::sleep(delay)),
            delayed: false,
        }
    }
}

impl Stream for DelayedRecordBatchStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.delayed {
            match self.delay.as_mut().poll(cx) {
                Poll::Ready(()) => self.delayed = true,
                Poll::Pending => return Poll::Pending,
            }
        }

        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl RecordBatchStream for DelayedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.inner.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.inner.metrics()
    }
}

#[derive(Debug)]
struct PanickingMetricsExec {
    properties: Arc<PlanProperties>,
    metrics_calls: Arc<AtomicUsize>,
    panic_after: usize,
}

impl PanickingMetricsExec {
    fn with_panic_after(schema: SchemaRef, panic_after: usize) -> Self {
        Self {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.arrow_schema().clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            metrics_calls: Arc::new(AtomicUsize::new(0)),
            panic_after,
        }
    }
}

impl DisplayAs for PanickingMetricsExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for PanickingMetricsExec {
    fn name(&self) -> &str {
        "PanickingMetricsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<common_recordbatch::DfSendableRecordBatchStream> {
        unimplemented!("test plan is never executed")
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        if self.metrics_calls.fetch_add(1, Ordering::Relaxed) >= self.panic_after {
            panic!("metrics collection panicked")
        }
        Some(datafusion::physical_plan::metrics::MetricsSet::new())
    }
}

struct SlowAnalyzeStreamHandler {
    inner: ServerSqlQueryHandlerRef,
    delay: Duration,
    panic_metrics: bool,
    panic_after: usize,
}

#[async_trait]
impl SqlQueryHandler for SlowAnalyzeStreamHandler {
    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        self.inner.do_query(query, query_ctx).await
    }

    async fn do_analyze_stream_query(
        &self,
        query: &str,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let output = self.inner.do_analyze_stream_query(query, query_ctx).await?;
        let Output { data, mut meta } = output;
        if self.panic_metrics {
            let schema = match &data {
                OutputData::Stream(stream) => stream.schema(),
                _ => unreachable!(),
            };
            meta.plan = Some(Arc::new(PanickingMetricsExec::with_panic_after(
                schema,
                self.panic_after,
            )));
        }
        let data = match data {
            OutputData::Stream(stream) => {
                OutputData::Stream(Box::pin(DelayedRecordBatchStream::new(stream, self.delay)))
            }
            data => data,
        };
        Ok(Output { data, meta })
    }

    async fn do_exec_plan(
        &self,
        plan: LogicalPlan,
        stmt: Option<Statement>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        self.inner.do_exec_plan(plan, stmt, query_ctx).await
    }

    async fn do_promql_query(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> Vec<Result<Output>> {
        self.inner.do_promql_query(query, query_ctx).await
    }

    async fn do_describe(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<DescribeResult>> {
        self.inner.do_describe(stmt, query_ctx).await
    }

    async fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.inner.is_valid_schema(catalog, schema).await
    }
}

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
                    Bytes::from_static(b"4950\r\n"),
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

    for format in ["greptimedb_v1", "influxdb_v1", "csv", "table", "null"] {
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
                    Bytes::from_static(b"4950\r\n"),
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
            HttpResponse::Null(resp) => {
                assert_eq!(resp.rows(), 1);
                let resp = resp.into_response();
                assert_eq!(
                    resp.headers().get(header::CONTENT_TYPE),
                    Some(HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref())).as_ref(),
                );
                let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap();
                let body_str = std::str::from_utf8(&body).unwrap();
                assert!(
                    body_str.starts_with("1 rows in set.\n"),
                    "Body did not start with expected prefix: {}",
                    body_str
                );
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn test_analyze_stream_sse_e2e() {
    common_telemetry::init_default_ut_logging();

    let client = analyze_stream_test_client(create_testing_sql_query_handler(
        MemTable::default_numbers_table(),
    ))
    .await;

    let response = client
        .post("/v1/sql/analyze/stream?snapshot_interval_ms=1000")
        .header(header::ACCEPT, "text/event-stream")
        .form(&http_handler::SqlQuery {
            sql: Some("EXPLAIN ANALYZE VERBOSE SELECT sum(uint32s) FROM numbers".to_string()),
            ..Default::default()
        })
        .send()
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"))
    );

    let body = response.text().await;
    let final_payload = sse_event_payload(&body, "final").expect(&body);
    let final_payload: Value = serde_json::from_str(&final_payload).unwrap();
    assert_eq!(final_payload["state"], "final");
    assert_eq!(final_payload["partial"], false);
    assert!(
        final_payload["metrics"]
            .as_array()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        final_payload["output"]["records"]["rows"]
            .as_array()
            .is_some_and(|v| !v.is_empty())
    );
}

#[tokio::test]
async fn test_analyze_stream_route_rejects_invalid_sql() {
    common_telemetry::init_default_ut_logging();

    let client = analyze_stream_test_client(create_testing_sql_query_handler(
        MemTable::default_numbers_table(),
    ))
    .await;

    for sql in [
        "SELECT 1",
        "EXPLAIN ANALYZE SELECT 1",
        "EXPLAIN ANALYZE VERBOSE FORMAT TEXT SELECT 1",
        "EXPLAIN ANALYZE VERBOSE SELECT 1; SELECT 2",
        "TQL ANALYZE (0, 10, '5s') physical_metric",
        "TQL EXPLAIN VERBOSE (0, 10, '5s') physical_metric",
        "TQL ANALYZE VERBOSE FORMAT TEXT (0, 10, '5s') physical_metric",
    ] {
        let response = client
            .post("/v1/sql/analyze/stream")
            .form(&http_handler::SqlQuery {
                sql: Some(sql.to_string()),
                ..Default::default()
            })
            .send()
            .await;

        assert_ne!(response.status(), StatusCode::OK, "{sql}");
        let body: Value = response.json().await;
        assert!(body.get("error").is_some(), "{sql}: {body}");
    }
}

#[tokio::test]
async fn test_analyze_stream_route_accepts_explicit_format_json() {
    common_telemetry::init_default_ut_logging();

    let client = analyze_stream_test_client(create_testing_sql_query_handler(
        MemTable::default_numbers_table(),
    ))
    .await;

    let response = client
        .post("/v1/sql/analyze/stream")
        .header(header::ACCEPT, "text/event-stream")
        .form(&http_handler::SqlQuery {
            sql: Some(
                "EXPLAIN ANALYZE VERBOSE FORMAT JSON SELECT sum(uint32s) FROM numbers".to_string(),
            ),
            ..Default::default()
        })
        .send()
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await;
    assert!(sse_event_payload(&body, "final").is_some(), "{body}");
}

#[tokio::test]
async fn test_analyze_stream_body_orders_latest_metrics_before_terminal() {
    let (metrics_tx, metrics_rx) = watch::channel::<Option<String>>(None);
    let (terminal_tx, terminal_rx) =
        watch::channel::<Option<http_handler::AnalyzeStreamMessage>>(None);
    let notify = Arc::new(Notify::new());
    metrics_tx.send(Some("latest".to_string())).unwrap();
    terminal_tx
        .send(Some(http_handler::AnalyzeStreamMessage {
            event_name: "final",
            payload: "terminal".to_string(),
        }))
        .unwrap();
    drop(metrics_tx);
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let worker = tokio::spawn(async move {
        let _ = cancel_rx.changed().await;
    });
    let body =
        http_handler::analyze_stream_body(metrics_rx, terminal_rx, notify, cancel_tx, worker);
    futures::pin_mut!(body);
    let first = tokio::time::timeout(Duration::from_secs(1), body.next())
        .await
        .expect("timed out waiting for metrics")
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(1), body.next())
        .await
        .expect("timed out waiting for terminal")
        .unwrap()
        .unwrap();
    assert!(format!("{first:?}").contains("latest"));
    assert!(format!("{second:?}").contains("terminal"));
}

#[tokio::test]
async fn test_analyze_stream_body_drop_cancels_worker() {
    let (_, metrics_rx) = watch::channel::<Option<String>>(None);
    let (_, terminal_rx) = watch::channel::<Option<http_handler::AnalyzeStreamMessage>>(None);
    let notify = Arc::new(Notify::new());
    let (cancel_tx, mut canceled_rx) = watch::channel(false);
    let worker = tokio::spawn(async { futures::future::pending::<()>().await });
    let mut body = Box::pin(http_handler::analyze_stream_body(
        metrics_rx,
        terminal_rx,
        notify,
        cancel_tx,
        worker,
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(1), body.next())
            .await
            .is_err()
    );
    drop(body);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !*canceled_rx.borrow() {
            canceled_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("timed out waiting for worker cancellation");
}

#[tokio::test]
async fn test_analyze_stream_terminal_is_sent_once() {
    let (terminal_tx, mut terminal_rx) =
        watch::channel::<Option<http_handler::AnalyzeStreamMessage>>(None);
    let notify = Notify::new();
    http_handler::send_analyze_terminal(&terminal_tx, &notify, "error", "one".to_string());
    http_handler::send_analyze_terminal(&terminal_tx, &notify, "error", "two".to_string());
    terminal_rx.changed().await.unwrap();
    let terminal = terminal_rx.borrow_and_update().clone().unwrap();
    assert_eq!(terminal.payload, "one");
}

#[tokio::test]
async fn test_analyze_stream_worker_panic_emits_one_error_terminal() {
    common_telemetry::init_default_ut_logging();

    let inner = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let sql_handler = Arc::new(SlowAnalyzeStreamHandler {
        inner,
        delay: Duration::ZERO,
        panic_metrics: true,
        panic_after: 0,
    });
    let ctx = QueryContext::with_db_name(None);
    ctx.set_current_user(auth::userinfo_by_name(None));
    let response = http_handler::sql_analyze_stream(
        State(ApiState { sql_handler }),
        Query(http_handler::SqlQuery {
            sql: Some("EXPLAIN ANALYZE VERBOSE SELECT sum(uint32s) FROM numbers".to_string()),
            snapshot_interval_ms: Some(1000),
            ..Default::default()
        }),
        axum::Extension(ctx),
        Ok(Form(http_handler::SqlQuery::default())),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = tokio::time::timeout(
        Duration::from_secs(3),
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("timed out waiting for panic terminal")
    .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert_eq!(sse_event_payloads(&body, "error").len(), 1, "{body}");
    assert!(sse_event_payload(&body, "final").is_none(), "{body}");
    let payload: Value = serde_json::from_str(&sse_event_payload(&body, "error").unwrap()).unwrap();
    assert_eq!(payload["state"], "error");
    assert_eq!(payload["reason"], "analyze stream worker panicked");
}

#[tokio::test]
async fn test_analyze_stream_worker_panic_after_metrics_uses_current_sequence() {
    common_telemetry::init_default_ut_logging();

    let inner = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let sql_handler = Arc::new(SlowAnalyzeStreamHandler {
        inner,
        delay: Duration::from_millis(2500),
        panic_metrics: true,
        panic_after: 1,
    });
    let ctx = QueryContext::with_db_name(None);
    ctx.set_current_user(auth::userinfo_by_name(None));
    let response = http_handler::sql_analyze_stream(
        State(ApiState { sql_handler }),
        Query(http_handler::SqlQuery {
            sql: Some("EXPLAIN ANALYZE VERBOSE SELECT sum(uint32s) FROM numbers".to_string()),
            snapshot_interval_ms: Some(1000),
            ..Default::default()
        }),
        axum::Extension(ctx),
        Ok(Form(http_handler::SqlQuery::default())),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = tokio::time::timeout(
        Duration::from_secs(5),
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("timed out waiting for panic terminal after metrics")
    .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    let metrics_payload: Value =
        serde_json::from_str(&sse_event_payload(&body, "metrics").expect(&body)).unwrap();
    let error_payloads = sse_event_payloads(&body, "error");
    assert_eq!(error_payloads.len(), 1, "{body}");
    let error_payload: Value = serde_json::from_str(&error_payloads[0]).unwrap();
    assert_eq!(metrics_payload["state"], "metrics");
    assert_eq!(error_payload["state"], "error");
    assert_eq!(error_payload["reason"], "analyze stream worker panicked");
    let metrics_seq = metrics_payload["seq"].as_u64().unwrap();
    let error_seq = error_payload["seq"].as_u64().unwrap();
    assert!(
        error_seq > metrics_seq,
        "terminal sequence should follow metrics sequence: {body}"
    );
}

#[tokio::test]
async fn test_analyze_stream_emits_metrics_before_final_when_stream_is_pending() {
    common_telemetry::init_default_ut_logging();

    let inner = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let sql_handler = Arc::new(SlowAnalyzeStreamHandler {
        inner,
        delay: Duration::from_millis(1500),
        panic_metrics: false,
        panic_after: 0,
    });
    let server = HttpServerBuilder::new(HttpOptions::default())
        .with_sql_handler(sql_handler)
        .build();
    let app = server.build(server.make_app()).unwrap();
    let client = TestClient::new(app).await;

    let response = client
        .post("/v1/sql/analyze/stream?snapshot_interval_ms=1000")
        .header(header::ACCEPT, "text/event-stream")
        .form(&http_handler::SqlQuery {
            sql: Some("EXPLAIN ANALYZE VERBOSE SELECT sum(uint32s) FROM numbers".to_string()),
            ..Default::default()
        })
        .send()
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await;
    let metrics_pos = body.find("event: metrics").expect(&body);
    let final_pos = body.find("event: final").expect(&body);
    assert!(
        metrics_pos < final_pos,
        "metrics event should be emitted before final event: {body}"
    );

    let metrics_payload = sse_event_payload(&body, "metrics").expect(&body);
    let metrics_payload: Value = serde_json::from_str(&metrics_payload).unwrap();
    assert_eq!(metrics_payload["state"], "metrics");
    assert_eq!(metrics_payload["partial"], true);
    assert!(
        metrics_payload["metrics"]
            .as_array()
            .is_some_and(|v| !v.is_empty())
    );
}

fn sse_event_payloads(body: &str, event_name: &str) -> Vec<String> {
    body.split("\n\n")
        .filter_map(|event| {
            let mut found = false;
            let mut data = Vec::new();
            for line in event.lines() {
                if line.strip_prefix("event: ") == Some(event_name) {
                    found = true;
                } else if let Some(value) = line.strip_prefix("data: ") {
                    data.push(value);
                }
            }
            found.then(|| data.join("\n"))
        })
        .collect()
}

fn sse_event_payload(body: &str, event_name: &str) -> Option<String> {
    sse_event_payloads(body, event_name).into_iter().next()
}

async fn analyze_stream_test_client(sql_handler: ServerSqlQueryHandlerRef) -> TestClient {
    let server = HttpServerBuilder::new(HttpOptions::default())
        .with_sql_handler(sql_handler)
        .build();
    let app = server.build(server.make_app()).unwrap();
    TestClient::new(app).await
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
