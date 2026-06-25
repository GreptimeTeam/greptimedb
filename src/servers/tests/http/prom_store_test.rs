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

use api::greptime_proto::io::prometheus::write::v2::{
    Histogram as RemoteWriteV2Histogram, Request as RemoteWriteV2Request,
    Sample as RemoteWriteV2Sample, TimeSeries as RemoteWriteV2TimeSeries,
};
use api::prom_store::remote::{
    LabelMatcher, Query, QueryResult, ReadRequest, ReadResponse, WriteRequest,
};
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, RowInsertRequests, SemanticType};
use async_trait::async_trait;
use axum::Router;
use common_query::Output;
use common_query::prelude::{greptime_timestamp, greptime_value};
use common_test_util::ports;
use datafusion_expr::LogicalPlan;
use prost::Message;
use query::parser::PromQuery;
use query::query_engine::DescribeResult;
use servers::error::Result;
use servers::http::header::{CONTENT_ENCODING_SNAPPY, CONTENT_TYPE_PROTOBUF};
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::http::test_helpers::TestClient;
use servers::http::{HttpOptions, HttpServerBuilder};
use servers::prom_remote_write::validation::PromValidationMode;
use servers::prom_store;
use servers::prom_store::{Metrics, snappy_compress};
use servers::query_handler::sql::SqlQueryHandler;
use servers::query_handler::{PromStoreProtocolHandler, PromStoreResponse};
use session::context::QueryContextRef;
use sql::statements::statement::Statement;
use tokio::sync::mpsc;

struct DummyInstance {
    read_tx: mpsc::Sender<(String, Vec<u8>)>,
    write_tx: mpsc::Sender<RemoteWriteCapture>,
}

struct RemoteWriteCapture {
    schema: String,
    physical_table: Option<String>,
    request: RowInsertRequests,
}

#[async_trait]
impl PromStoreProtocolHandler for DummyInstance {
    async fn write(
        &self,
        request: RowInsertRequests,
        ctx: QueryContextRef,
        _with_metric_engine: bool,
    ) -> Result<Output> {
        let _ = self
            .write_tx
            .send(RemoteWriteCapture {
                schema: ctx.current_schema(),
                physical_table: ctx.extension(PHYSICAL_TABLE_PARAM).map(ToString::to_string),
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
    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };

    let instance = Arc::new(DummyInstance { read_tx, write_tx });
    let server = HttpServerBuilder::new(http_opts)
        .with_sql_handler(instance.clone())
        .with_prom_handler(instance, None, true, PromValidationMode::Unchecked, None)
        .build();
    server.build(server.make_app()).unwrap()
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

    let write_request = remote_write_v2_request_with_labels_and_samples(
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

    let result = client
        .post("/v1/prometheus/write")
        .header(
            "content-type",
            "application/x-protobuf;proto=io.prometheus.write.v2.Request",
        )
        .header("content-encoding", "snappy")
        .body(snappy_compress(&write_request.encode_to_vec()).unwrap())
        .send()
        .await;

    assert_eq!(result.status(), 204);
    assert_eq!(
        Some("2"),
        result
            .headers()
            .get("X-Prometheus-Remote-Write-Samples-Written")
            .map(|x| x.to_str().unwrap())
    );
    assert_eq!(
        Some("0"),
        result
            .headers()
            .get("X-Prometheus-Remote-Write-Histograms-Written")
            .map(|x| x.to_str().unwrap())
    );
    assert_eq!(
        Some("0"),
        result
            .headers()
            .get("X-Prometheus-Remote-Write-Exemplars-Written")
            .map(|x| x.to_str().unwrap())
    );
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
async fn test_prometheus_remote_write_v2_ignores_histogram_only_series() {
    common_telemetry::init_default_ut_logging();
    let (read_tx, _read_rx) = mpsc::channel(100);
    let (write_tx, mut write_rx) = mpsc::channel(100);

    let app = make_test_app_with_write_capture(read_tx, write_tx);
    let client = TestClient::new(app).await;

    let write_request = remote_write_v2_request_with_labels_and_histograms(
        vec![(
            prom_store::METRIC_NAME_LABEL,
            "http_request_duration_seconds",
        )],
        vec![remote_write_v2_histogram(1000)],
    );

    let result = client
        .post("/v1/prometheus/write")
        .header(
            "content-type",
            "application/x-protobuf;proto=io.prometheus.write.v2.Request",
        )
        .header("content-encoding", "snappy")
        .body(snappy_compress(&write_request.encode_to_vec()).unwrap())
        .send()
        .await;

    assert_eq!(result.status(), 204);
    assert_eq!(
        Some("0"),
        result
            .headers()
            .get("X-Prometheus-Remote-Write-Samples-Written")
            .map(|x| x.to_str().unwrap())
    );
    assert_eq!(
        Some("0"),
        result
            .headers()
            .get("X-Prometheus-Remote-Write-Histograms-Written")
            .map(|x| x.to_str().unwrap())
    );
    assert_eq!(
        Some("0"),
        result
            .headers()
            .get("X-Prometheus-Remote-Write-Exemplars-Written")
            .map(|x| x.to_str().unwrap())
    );
    assert!(result.text().await.is_empty());
    assert!(write_rx.try_recv().is_err());
}

fn remote_write_v2_request_with_labels_and_samples(
    labels: Vec<(&str, &str)>,
    samples: Vec<RemoteWriteV2Sample>,
) -> RemoteWriteV2Request {
    let mut symbols = vec!["".to_string()];
    let mut labels_refs = Vec::with_capacity(labels.len() * 2);
    for (name, value) in labels {
        labels_refs.push(push_symbol(&mut symbols, name));
        labels_refs.push(push_symbol(&mut symbols, value));
    }

    RemoteWriteV2Request {
        symbols,
        timeseries: vec![RemoteWriteV2TimeSeries {
            labels_refs,
            samples,
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: None,
        }],
    }
}

fn remote_write_v2_request_with_labels_and_histograms(
    labels: Vec<(&str, &str)>,
    histograms: Vec<RemoteWriteV2Histogram>,
) -> RemoteWriteV2Request {
    let mut symbols = vec!["".to_string()];
    let mut labels_refs = Vec::with_capacity(labels.len() * 2);
    for (name, value) in labels {
        labels_refs.push(push_symbol(&mut symbols, name));
        labels_refs.push(push_symbol(&mut symbols, value));
    }

    RemoteWriteV2Request {
        symbols,
        timeseries: vec![RemoteWriteV2TimeSeries {
            labels_refs,
            samples: Vec::new(),
            histograms,
            exemplars: Vec::new(),
            metadata: None,
        }],
    }
}

fn remote_write_v2_histogram(timestamp: i64) -> RemoteWriteV2Histogram {
    RemoteWriteV2Histogram {
        timestamp,
        ..Default::default()
    }
}

fn push_symbol(symbols: &mut Vec<String>, symbol: &str) -> u32 {
    if let Some(idx) = symbols.iter().position(|s| s == symbol) {
        return idx as u32;
    }

    let idx = symbols.len();
    symbols.push(symbol.to_string());
    idx as u32
}
