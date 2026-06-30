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
use std::sync::atomic::{AtomicUsize, Ordering};

use api::greptime_proto::io::prometheus::write::v2::{
    Request as RemoteWriteV2Request, Sample as RemoteWriteV2Sample,
    TimeSeries as RemoteWriteV2TimeSeries,
};
use api::prom_store::remote::{
    LabelMatcher, Query, QueryResult, ReadRequest, ReadResponse, WriteRequest,
};
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, RowInsertRequests, SemanticType};
use async_trait::async_trait;
use axum::Router;
use axum::http::HeaderMap;
use common_query::Output;
use common_query::prelude::{GREPTIME_PHYSICAL_TABLE, greptime_timestamp, greptime_value};
use common_test_util::ports;
use datafusion_expr::LogicalPlan;
use prost::Message;
use query::parser::PromQuery;
use query::query_engine::DescribeResult;
use servers::error::{self, Result};
use servers::http::header::{CONTENT_ENCODING_SNAPPY, CONTENT_TYPE_PROTOBUF};
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::http::test_helpers::{TestClient, TestResponse};
use servers::http::{HttpOptions, HttpServerBuilder};
use servers::prom_remote_write::v2::test_util as remote_write_v2;
use servers::prom_remote_write::validation::PromValidationMode;
use servers::prom_store;
use servers::prom_store::{Metrics, snappy_compress};
use servers::query_handler::sql::SqlQueryHandler;
use servers::query_handler::{PromStoreProtocolHandler, PromStoreResponse};
use session::context::QueryContextRef;
use sql::statements::statement::Statement;
use tokio::sync::mpsc;

const REMOTE_WRITE_V2_CONTENT_TYPE: &str =
    "application/x-protobuf;proto=io.prometheus.write.v2.Request";

struct DummyInstance {
    read_tx: mpsc::Sender<(String, Vec<u8>)>,
    write_tx: mpsc::Sender<RemoteWriteCapture>,
    write_calls: Arc<AtomicUsize>,
    fail_write_call: Option<usize>,
}

struct RemoteWriteCapture {
    schema: String,
    physical_table: Option<String>,
    with_metric_engine: bool,
    request: RowInsertRequests,
}

#[async_trait]
impl PromStoreProtocolHandler for DummyInstance {
    async fn write(
        &self,
        request: RowInsertRequests,
        ctx: QueryContextRef,
        with_metric_engine: bool,
    ) -> Result<Output> {
        let write_call = self.write_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_write_call == Some(write_call) {
            return error::InvalidPromRemoteRequestSnafu {
                msg: "injected prometheus remote write failure".to_string(),
            }
            .fail();
        }

        let _ = self
            .write_tx
            .send(RemoteWriteCapture {
                schema: ctx.current_schema(),
                physical_table: ctx.extension(PHYSICAL_TABLE_PARAM).map(ToString::to_string),
                with_metric_engine,
                request,
            })
            .await;

        Ok(Output::new_with_affected_rows(0))
    }

    async fn read(&self, request: ReadRequest, ctx: QueryContextRef) -> Result<PromStoreResponse> {
        let _ = self
            .read_tx
            .send((ctx.current_schema(), request.encode_to_vec()))
            .await;

        let response = ReadResponse {
            results: vec![QueryResult {
                timeseries: prom_store::mock_timeseries(),
            }],
        };

        Ok(PromStoreResponse {
            content_type: CONTENT_TYPE_PROTOBUF.clone(),
            content_encoding: CONTENT_ENCODING_SNAPPY.clone(),
            resp_metrics: Default::default(),
            body: response.encode_to_vec(),
        })
    }

    async fn ingest_metrics(&self, _metrics: Metrics) -> Result<()> {
        unimplemented!();
    }
}

#[async_trait]
impl SqlQueryHandler for DummyInstance {
    async fn do_query(&self, _: &str, _: QueryContextRef) -> Vec<Result<Output>> {
        unimplemented!()
    }

    async fn do_exec_plan(
        &self,
        _plan: LogicalPlan,
        _stmt: Option<Statement>,
        _query_ctx: QueryContextRef,
    ) -> Result<Output> {
        unimplemented!()
    }

    async fn do_promql_query(&self, _: &PromQuery, _: QueryContextRef) -> Vec<Result<Output>> {
        unimplemented!()
    }

    async fn do_describe(
        &self,
        _stmt: sql::statements::statement::Statement,
        _query_ctx: QueryContextRef,
    ) -> Result<Option<DescribeResult>> {
        unimplemented!()
    }

    async fn is_valid_schema(&self, _catalog: &str, _schema: &str) -> Result<bool> {
        Ok(true)
    }
}

fn make_test_app(tx: mpsc::Sender<(String, Vec<u8>)>) -> Router {
    let (write_tx, _write_rx) = mpsc::channel(100);
    make_test_app_with_write_capture(tx, write_tx)
}

fn make_test_app_with_write_capture(
    read_tx: mpsc::Sender<(String, Vec<u8>)>,
    write_tx: mpsc::Sender<RemoteWriteCapture>,
) -> Router {
    make_test_app_with_write_failure(read_tx, write_tx, None)
}

fn make_test_app_with_write_failure(
    read_tx: mpsc::Sender<(String, Vec<u8>)>,
    write_tx: mpsc::Sender<RemoteWriteCapture>,
    fail_write_call: Option<usize>,
) -> Router {
    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };

    let instance = Arc::new(DummyInstance {
        read_tx,
        write_tx,
        write_calls: Arc::new(AtomicUsize::new(0)),
        fail_write_call,
    });
    let server = HttpServerBuilder::new(http_opts)
        .with_sql_handler(instance.clone())
        .with_prom_handler(instance, None, true, PromValidationMode::Unchecked, None)
        .build();
    server.build(server.make_app()).unwrap()
}

async fn post_remote_write_v2(client: &TestClient, request: &RemoteWriteV2Request) -> TestResponse {
    post_remote_write_v2_body(client, snappy_compress(&request.encode_to_vec()).unwrap()).await
}

async fn post_remote_write_v2_body(client: &TestClient, body: Vec<u8>) -> TestResponse {
    post_remote_write_with_content_type(client, REMOTE_WRITE_V2_CONTENT_TYPE, body).await
}

async fn post_remote_write_with_content_type(
    client: &TestClient,
    content_type: &str,
    body: Vec<u8>,
) -> TestResponse {
    post_remote_write_with_content_type_and_encoding(client, content_type, "snappy", body).await
}

async fn post_remote_write_with_content_type_and_encoding(
    client: &TestClient,
    content_type: &str,
    content_encoding: &str,
    body: Vec<u8>,
) -> TestResponse {
    client
        .post("/v1/prometheus/write")
        .header("content-type", content_type)
        .header("content-encoding", content_encoding)
        .body(body)
        .send()
        .await
}

#[tokio::test]
async fn test_prometheus_remote_write_v2_decode_error_has_written_headers() {
    common_telemetry::init_default_ut_logging();
    let (read_tx, _read_rx) = mpsc::channel(100);
    let (write_tx, mut write_rx) = mpsc::channel(100);

    let app = make_test_app_with_write_capture(read_tx, write_tx);
    let client = TestClient::new(app).await;

    let result = post_remote_write_v2_body(&client, vec![0xff, 0xff]).await;

    assert_eq!(result.status(), 400);
    assert_remote_write_v2_written_headers(&result.headers(), "0");
    assert!(result.text().await.contains("error"));
    assert!(write_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_prometheus_remote_write_v2_convert_error_has_written_headers() {
    common_telemetry::init_default_ut_logging();
    let (read_tx, _read_rx) = mpsc::channel(100);
    let (write_tx, mut write_rx) = mpsc::channel(100);

    let app = make_test_app_with_write_capture(read_tx, write_tx);
    let client = TestClient::new(app).await;

    let write_request = remote_write_v2::request_with_labels_and_samples(
        vec![("job", "api")],
        vec![RemoteWriteV2Sample {
            value: 42.0,
            timestamp: 1000,
            start_timestamp: 0,
        }],
    );

    let result = post_remote_write_v2(&client, &write_request).await;

    assert_eq!(result.status(), 400);
    assert_remote_write_v2_written_headers(&result.headers(), "0");
    assert!(result.text().await.contains("missing '__name__'"));
    assert!(write_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_prometheus_remote_write_v2_write_error_has_partial_written_headers() {
    common_telemetry::init_default_ut_logging();
    let (read_tx, _read_rx) = mpsc::channel(100);
    let (write_tx, mut write_rx) = mpsc::channel(100);

    let app = make_test_app_with_write_failure(read_tx, write_tx, Some(2));
    let client = TestClient::new(app).await;

    let write_request = RemoteWriteV2Request {
        symbols: vec![
            String::new(),
            prom_store::METRIC_NAME_LABEL.to_string(),
            "http_requests_total".to_string(),
            prom_store::DATABASE_LABEL.to_string(),
            "tenant_a".to_string(),
            "tenant_b".to_string(),
        ],
        timeseries: vec![
            RemoteWriteV2TimeSeries {
                labels_refs: vec![1, 2, 3, 4],
                samples: vec![RemoteWriteV2Sample {
                    value: 42.0,
                    timestamp: 1000,
                    start_timestamp: 0,
                }],
                ..Default::default()
            },
            RemoteWriteV2TimeSeries {
                labels_refs: vec![1, 2, 3, 5],
                samples: vec![RemoteWriteV2Sample {
                    value: 43.0,
                    timestamp: 2000,
                    start_timestamp: 0,
                }],
                ..Default::default()
            },
        ],
    };

    let result = post_remote_write_v2(&client, &write_request).await;

    assert_eq!(result.status(), 400);
    assert_remote_write_v2_written_headers(&result.headers(), "1");
    assert!(
        result
            .text()
            .await
            .contains("injected prometheus remote write failure")
    );

    let captured = write_rx.recv().await.unwrap();
    assert_eq!(
        1,
        captured.request.inserts[0]
            .rows
            .as_ref()
            .unwrap()
            .rows
            .len()
    );
    assert!(write_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_prometheus_remote_write_v2_histogram_write_error_has_partial_written_headers() {
    common_telemetry::init_default_ut_logging();
    let (read_tx, _read_rx) = mpsc::channel(100);
    let (write_tx, mut write_rx) = mpsc::channel(100);

    let app = make_test_app_with_write_failure(read_tx, write_tx, Some(2));
    let client = TestClient::new(app).await;

    let mut write_request = remote_write_v2::request_with_labels_and_samples(
        vec![(
            prom_store::METRIC_NAME_LABEL,
            "http_request_duration_seconds",
        )],
        vec![RemoteWriteV2Sample {
            value: 42.0,
            timestamp: 1000,
            start_timestamp: 0,
        }],
    );
    write_request.timeseries[0]
        .histograms
        .push(remote_write_v2::histogram(1000));

    let result = post_remote_write_v2(&client, &write_request).await;

    assert_eq!(result.status(), 400);
    assert_remote_write_v2_written_headers_with_histograms(&result.headers(), "1", "0");
    assert!(
        result
            .text()
            .await
            .contains("injected prometheus remote write failure")
    );

    let captured = write_rx.recv().await.unwrap();
    assert!(captured.with_metric_engine);
    assert_eq!(1, captured.request.inserts.len());
    assert_eq!(
        "http_request_duration_seconds",
        captured.request.inserts[0].table_name
    );
    assert!(write_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_prometheus_remote_write_read() {
    common_telemetry::init_default_ut_logging();
    let (tx, mut rx) = mpsc::channel(100);

    let app = make_test_app(tx);
    let client = TestClient::new(app).await;

    let write_request = WriteRequest {
        timeseries: prom_store::mock_timeseries(),
        ..Default::default()
    };

    // Write to public database
    let result = client
        .post("/v1/prometheus/write")
        .body(snappy_compress(&write_request.clone().encode_to_vec()[..]).unwrap())
        .send()
        .await;
    assert_eq!(result.status(), 204);
    assert!(result.text().await.is_empty());
    // Write to prometheus database
    let result = client
        .post("/v1/prometheus/write?db=prometheus")
        .body(snappy_compress(&write_request.clone().encode_to_vec()[..]).unwrap())
        .send()
        .await;
    assert_eq!(result.status(), 204);
    assert!(result.text().await.is_empty());

    let read_request = ReadRequest {
        queries: vec![Query {
            start_timestamp_ms: 1000,
            end_timestamp_ms: 2000,
            matchers: vec![LabelMatcher {
                name: prom_store::METRIC_NAME_LABEL.to_string(),
                value: "metric1".to_string(),
                r#type: 0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Read from prometheus database
    let mut result = client
        .post("/v1/prometheus/read?db=prometheus")
        .body(snappy_compress(&read_request.clone().encode_to_vec()[..]).unwrap())
        .send()
        .await;
    assert_eq!(result.status(), 200);
    let headers = result.headers();
    assert_eq!(
        Some("application/x-protobuf"),
        headers.get("content-type").map(|x| x.to_str().unwrap())
    );
    assert_eq!(
        Some("snappy"),
        headers.get("content-encoding").map(|x| x.to_str().unwrap())
    );
    let response = result.chunk().await.unwrap();
    let response = ReadResponse::decode(&response[..]).unwrap();
    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].timeseries,
        prom_store::mock_timeseries()
    );

    // Read from public database
    let result = client
        .post("/v1/prometheus/read")
        .body(snappy_compress(&read_request.clone().encode_to_vec()[..]).unwrap())
        .send()
        .await;
    assert_eq!(result.status(), 200);

    let mut requests: Vec<(String, Vec<u8>)> = vec![];
    while let Ok(s) = rx.try_recv() {
        requests.push(s);
    }

    assert_eq!(2, requests.len());

    assert_eq!("prometheus", requests[0].0);
    assert_eq!("public", requests[1].0);

    assert_eq!(
        read_request,
        ReadRequest::decode(&(requests[0].1)[..]).unwrap()
    );
    assert_eq!(
        read_request,
        ReadRequest::decode(&(requests[1].1)[..]).unwrap()
    );
}

#[tokio::test]
async fn test_prometheus_remote_write_v2_samples() {
    common_telemetry::init_default_ut_logging();
    let (read_tx, _read_rx) = mpsc::channel(100);
    let (write_tx, mut write_rx) = mpsc::channel(100);

    let app = make_test_app_with_write_capture(read_tx, write_tx);
    let client = TestClient::new(app).await;

    let write_request = remote_write_v2::request_with_labels_and_samples(
        vec![
            (prom_store::METRIC_NAME_LABEL, "http_requests_total"),
            (prom_store::DATABASE_LABEL, "tenant_a"),
            (prom_store::PHYSICAL_TABLE_LABEL, "metrics_physical"),
            ("job", "api"),
        ],
        vec![
            RemoteWriteV2Sample {
                value: 42.0,
                timestamp: 1000,
                start_timestamp: 0,
            },
            RemoteWriteV2Sample {
                value: 43.0,
                timestamp: 2000,
                start_timestamp: 0,
            },
        ],
    );

    let result = post_remote_write_v2(&client, &write_request).await;

    assert_eq!(result.status(), 204);
    assert_remote_write_v2_written_headers(&result.headers(), "2");
    assert!(result.text().await.is_empty());

    let captured = write_rx.recv().await.unwrap();
    assert_eq!("tenant_a", captured.schema);
    assert_eq!(
        Some("metrics_physical".to_string()),
        captured.physical_table
    );
    assert_eq!(1, captured.request.inserts.len());

    let insert = &captured.request.inserts[0];
    assert_eq!("http_requests_total", insert.table_name);
    let rows = insert.rows.as_ref().unwrap();
    assert_eq!(
        vec![greptime_timestamp(), greptime_value(), "job"],
        rows.schema
            .iter()
            .map(|column| column.column_name.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        vec![
            ColumnDataType::TimestampMillisecond as i32,
            ColumnDataType::Float64 as i32,
            ColumnDataType::String as i32,
        ],
        rows.schema
            .iter()
            .map(|column| column.datatype)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        vec![
            SemanticType::Timestamp as i32,
            SemanticType::Field as i32,
            SemanticType::Tag as i32,
        ],
        rows.schema
            .iter()
            .map(|column| column.semantic_type)
            .collect::<Vec<_>>()
    );
    assert_eq!(2, rows.rows.len());
    assert_eq!(
        Some(ValueData::TimestampMillisecondValue(1000)),
        rows.rows[0].values[0].value_data
    );
    assert_eq!(
        Some(ValueData::F64Value(42.0)),
        rows.rows[0].values[1].value_data
    );
    assert_eq!(
        Some(ValueData::StringValue("api".to_string())),
        rows.rows[0].values[2].value_data
    );
    assert_eq!(
        Some(ValueData::TimestampMillisecondValue(2000)),
        rows.rows[1].values[0].value_data
    );
    assert_eq!(
        Some(ValueData::F64Value(43.0)),
        rows.rows[1].values[1].value_data
    );
    assert_eq!(
        Some(ValueData::StringValue("api".to_string())),
        rows.rows[1].values[2].value_data
    );
    assert!(write_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_prometheus_remote_write_v2_writes_histogram_only_series() {
    common_telemetry::init_default_ut_logging();
    let (read_tx, _read_rx) = mpsc::channel(100);
    let (write_tx, mut write_rx) = mpsc::channel(100);

    let app = make_test_app_with_write_capture(read_tx, write_tx);
    let client = TestClient::new(app).await;

    let write_request = remote_write_v2::request_with_labels_and_histograms(
        vec![(
            prom_store::METRIC_NAME_LABEL,
            "http_request_duration_seconds",
        )],
        vec![remote_write_v2::histogram(1000)],
    );

    let result = post_remote_write_v2(&client, &write_request).await;

    assert_eq!(result.status(), 204);
    assert_remote_write_v2_written_headers_with_histograms(&result.headers(), "0", "1");
    assert!(result.text().await.is_empty());

    let captured = write_rx.recv().await.unwrap();
    assert!(!captured.with_metric_engine);
    assert_eq!(
        Some(GREPTIME_PHYSICAL_TABLE.to_string()),
        captured.physical_table
    );
    assert_eq!(1, captured.request.inserts.len());
    let insert = &captured.request.inserts[0];
    assert_eq!(
        "http_request_duration_seconds_native_histogram",
        insert.table_name
    );
    let rows = insert.rows.as_ref().unwrap();
    assert_eq!(1, rows.rows.len());
    assert!(
        rows.schema
            .iter()
            .any(|column| column.column_name == "positive_buckets_i64"
                && column.datatype == ColumnDataType::List as i32)
    );
    assert!(write_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_prometheus_remote_write_rejects_unsupported_proto() {
    common_telemetry::init_default_ut_logging();
    let (read_tx, _read_rx) = mpsc::channel(100);
    let (write_tx, mut write_rx) = mpsc::channel(100);

    let app = make_test_app_with_write_capture(read_tx, write_tx);
    let client = TestClient::new(app).await;

    let result = post_remote_write_with_content_type(
        &client,
        "application/x-protobuf;proto=io.prometheus.write.v3.Request",
        Vec::new(),
    )
    .await;

    assert_eq!(result.status(), 415);
    assert!(
        result
            .text()
            .await
            .contains("unsupported prometheus remote write content type")
    );
    assert!(write_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_prometheus_remote_write_v2_rejects_unsupported_content_encoding() {
    common_telemetry::init_default_ut_logging();
    let (read_tx, _read_rx) = mpsc::channel(100);
    let (write_tx, mut write_rx) = mpsc::channel(100);

    let app = make_test_app_with_write_capture(read_tx, write_tx);
    let client = TestClient::new(app).await;

    let result = post_remote_write_with_content_type_and_encoding(
        &client,
        REMOTE_WRITE_V2_CONTENT_TYPE,
        "gzip",
        Vec::new(),
    )
    .await;

    assert_eq!(result.status(), 415);
    assert!(
        result
            .text()
            .await
            .contains("unsupported prometheus remote write content encoding")
    );
    assert!(write_rx.try_recv().is_err());
}

fn assert_remote_write_v2_written_headers(headers: &HeaderMap, samples: &str) {
    assert_remote_write_v2_written_headers_with_histograms(headers, samples, "0")
}

fn assert_remote_write_v2_written_headers_with_histograms(
    headers: &HeaderMap,
    samples: &str,
    histograms: &str,
) {
    assert_eq!(
        Some(samples),
        headers
            .get("X-Prometheus-Remote-Write-Samples-Written")
            .map(|x| x.to_str().unwrap())
    );
    assert_eq!(
        Some(histograms),
        headers
            .get("X-Prometheus-Remote-Write-Histograms-Written")
            .map(|x| x.to_str().unwrap())
    );
    assert_eq!(
        Some("0"),
        headers
            .get("X-Prometheus-Remote-Write-Exemplars-Written")
            .map(|x| x.to_str().unwrap())
    );
}
