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

use api::prom_store::remote::ReadRequest;
use axum::Extension;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum_extra::TypedHeader;
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::tracing;
use mime_guess::mime;
use pipeline::util::to_pipeline_version;
use pipeline::{ContextReq, PipelineDefinition};
use prometheus::HistogramTimer;
use prost::Message;
use serde::{Deserialize, Serialize};
use session::context::{Channel, QueryContext};
use snafu::prelude::*;
use table::requests::{
    METADATA_QUALITY_INFERRED, SEMANTIC_METRIC_METADATA_QUALITY, SEMANTIC_SIGNAL_TYPE,
    SEMANTIC_SOURCE, SEMANTIC_SOURCE_VERSION, SIGNAL_TYPE_METRIC, SOURCE_PROMETHEUS,
};

use crate::error::{self, InternalSnafu, PipelineSnafu, Result};
use crate::http::extractor::PipelineInfo;
use crate::http::header::{
    CONTENT_TYPE_PROTOBUF_STR, GREPTIME_DB_HEADER_METRICS, write_cost_header_map,
};
use crate::pending_rows_batcher::PendingRowsBatcher;
use crate::prom_remote_write::decode::PromSeriesProcessor;
use crate::prom_remote_write::decode_remote_write_request;
use crate::prom_remote_write::v2::{decode_remote_write_v2_request, into_write_requests};
use crate::prom_remote_write::validation::PromValidationMode;
use crate::prom_store::{extract_schema_from_read_request, snappy_decompress};
use crate::query_handler::{PipelineHandlerRef, PromStoreProtocolHandlerRef, PromStoreResponse};

pub const PHYSICAL_TABLE_PARAM: &str = "physical_table";
pub const DEFAULT_ENCODING: &str = "snappy";
pub const VM_ENCODING: &str = "zstd";
pub const VM_PROTO_VERSION: &str = "1";
const REMOTE_WRITE_V1_VERSION: &str = "1.0";
const REMOTE_WRITE_V2_VERSION: &str = "2.0";
const REMOTE_WRITE_V1_PROTO: &str = "prometheus.WriteRequest";
const REMOTE_WRITE_V2_PROTO: &str = "io.prometheus.write.v2.Request";
const CONTENT_TYPE_PROTO_PARAM: &str = "proto";
const REMOTE_WRITE_V2_SAMPLES_WRITTEN_HEADER: &str = "x-prometheus-remote-write-samples-written";
const REMOTE_WRITE_V2_HISTOGRAMS_WRITTEN_HEADER: &str =
    "x-prometheus-remote-write-histograms-written";
const REMOTE_WRITE_V2_EXEMPLARS_WRITTEN_HEADER: &str =
    "x-prometheus-remote-write-exemplars-written";

#[derive(Clone)]
pub struct PromStoreState {
    pub prom_store_handler: PromStoreProtocolHandlerRef,
    pub pipeline_handler: Option<PipelineHandlerRef>,
    pub prom_store_with_metric_engine: bool,
    pub prom_validation_mode: PromValidationMode,
    pub experimental_enable_prometheus_native_histogram: bool,
    pub pending_rows_batcher: Option<Arc<PendingRowsBatcher>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RemoteWriteQuery {
    pub db: Option<String>,
    /// Specify which physical table to use for storing metrics.
    /// This only works on remote write requests.
    pub physical_table: Option<String>,
    /// For VictoriaMetrics modified remote write protocol
    pub get_vm_proto_version: Option<String>,
}

impl Default for RemoteWriteQuery {
    fn default() -> RemoteWriteQuery {
        Self {
            db: Some(DEFAULT_SCHEMA_NAME.to_string()),
            physical_table: Some(GREPTIME_PHYSICAL_TABLE.to_string()),
            get_vm_proto_version: None,
        }
    }
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "remote_write")
)]
pub async fn remote_write(
    State(state): State<PromStoreState>,
    Query(params): Query<RemoteWriteQuery>,
    Extension(query_ctx): Extension<QueryContext>,
    content_type: Option<TypedHeader<headers::ContentType>>,
    pipeline_info: PipelineInfo,
    content_encoding: TypedHeader<headers::ContentEncoding>,
    body: Bytes,
) -> Result<axum::response::Response> {
    let is_zstd = content_encoding.contains(VM_ENCODING);

    match remote_write_proto(content_type) {
        RemoteWriteProto::V1 => {
            remote_write_v1(state, params, query_ctx, pipeline_info, is_zstd, body).await
        }
        RemoteWriteProto::V2 => {
            if let Some(response) = unsupported_remote_write_v2_encoding_response(&content_encoding)
            {
                return Ok(response);
            }
            remote_write_v2(state, params, query_ctx, pipeline_info, is_zstd, body).await
        }
        RemoteWriteProto::Unsupported(content_type) => Ok((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported prometheus remote write content type: {content_type}"),
        )
            .into_response()),
    }
}

async fn remote_write_v1(
    state: PromStoreState,
    params: RemoteWriteQuery,
    query_ctx: QueryContext,
    pipeline_info: PipelineInfo,
    is_zstd: bool,
    body: Bytes,
) -> Result<axum::response::Response> {
    let PromStoreState {
        prom_store_handler,
        pipeline_handler,
        prom_store_with_metric_engine,
        prom_validation_mode,
        experimental_enable_prometheus_native_histogram: _,
        pending_rows_batcher,
    } = state;

    if let Some(response) = vm_proto_version_response(&params) {
        return Ok(response);
    }

    let (db, query_ctx, _timer) =
        prepare_remote_write_context(&params, query_ctx, REMOTE_WRITE_V1_VERSION);

    let mut processor = PromSeriesProcessor::default_processor();

    if let Some(pipeline_name) = pipeline_info.pipeline_name {
        let pipeline_def = PipelineDefinition::from_name(
            &pipeline_name,
            to_pipeline_version(pipeline_info.pipeline_version.as_deref())
                .context(PipelineSnafu)?,
            None,
        )
        .context(PipelineSnafu)?;
        let pipeline_handler = pipeline_handler.context(InternalSnafu {
            err_msg: "pipeline handler is not set".to_string(),
        })?;

        processor.set_pipeline(pipeline_handler, query_ctx.clone(), pipeline_def);
    }

    let mut req = decode_remote_write_request(is_zstd, body, prom_validation_mode, &mut processor)?;

    let req = if processor.use_pipeline {
        processor.exec_pipeline().await?
    } else {
        req.as_insert_requests()
    };

    let outcome = match write_prometheus_rows_with_progress(
        prom_store_handler,
        pending_rows_batcher,
        prom_store_with_metric_engine,
        query_ctx,
        req,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            record_remote_write_samples(&db, error.rows_written);
            return Err(error.error);
        }
    };
    record_remote_write_samples(&db, outcome.rows_written);

    Ok((
        StatusCode::NO_CONTENT,
        write_cost_header_map(outcome.write_cost),
    )
        .into_response())
}

async fn remote_write_v2(
    state: PromStoreState,
    params: RemoteWriteQuery,
    query_ctx: QueryContext,
    pipeline_info: PipelineInfo,
    is_zstd: bool,
    body: Bytes,
) -> Result<axum::response::Response> {
    let PromStoreState {
        prom_store_handler,
        pipeline_handler: _,
        prom_store_with_metric_engine,
        prom_validation_mode: _,
        experimental_enable_prometheus_native_histogram,
        pending_rows_batcher,
    } = state;

    if let Some(response) = vm_proto_version_response(&params) {
        return Ok(response);
    }

    // Pipeline processing is not supported for remote write v2 yet. Ignore the
    // optional pipeline parameter and ingest samples directly.
    let _ = pipeline_info;

    let (db, query_ctx, _timer) =
        prepare_remote_write_context(&params, query_ctx, REMOTE_WRITE_V2_VERSION);

    let request = match decode_remote_write_v2_request(is_zstd, body) {
        Ok(request) => request,
        Err(error) => return Ok(remote_write_v2_error_response(error, 0, 0, 0)),
    };
    if !experimental_enable_prometheus_native_histogram && request_has_native_histograms(&request) {
        return Ok(remote_write_v2_error_response(
            error::InvalidPromRemoteRequestSnafu {
                msg: "prometheus remote write v2 native histogram ingestion is experimental; set http.experimental_enable_prometheus_native_histogram = true to enable it"
                    .to_string(),
            }
            .build(),
            0,
            0,
            0,
        ));
    }
    let req = match into_write_requests(request) {
        Ok(req) => req,
        Err(error) => return Ok(remote_write_v2_error_response(error, 0, 0, 0)),
    };

    let outcome = if req.sample_count > 0 {
        match write_prometheus_rows_with_progress(
            prom_store_handler.clone(),
            pending_rows_batcher.clone(),
            prom_store_with_metric_engine,
            query_ctx.clone(),
            req.samples,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                record_remote_write_samples(&db, error.rows_written);
                return Ok(remote_write_v2_error_response(
                    error.error,
                    error.rows_written,
                    0,
                    0,
                ));
            }
        }
    } else {
        PromWriteOutcome {
            write_cost: 0,
            rows_written: 0,
        }
    };
    let samples_written = outcome.rows_written;
    record_remote_write_samples(&db, samples_written);
    let mut histograms_written = 0;
    let mut write_cost = outcome.write_cost;

    if req.histogram_count > 0 {
        let outcome = match write_prometheus_rows_with_progress(
            prom_store_handler,
            None,
            prom_store_with_metric_engine,
            query_ctx,
            req.histograms,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                record_remote_write_histograms(&db, error.rows_written);
                return Ok(remote_write_v2_error_response(
                    error.error,
                    samples_written,
                    error.rows_written,
                    0,
                ));
            }
        };
        histograms_written = outcome.rows_written;
        record_remote_write_histograms(&db, histograms_written);
        write_cost += outcome.write_cost;
    }

    let mut headers = write_cost_header_map(write_cost);
    append_remote_write_v2_written_headers(&mut headers, samples_written, histograms_written, 0);

    Ok((StatusCode::NO_CONTENT, headers).into_response())
}

fn request_has_native_histograms(
    request: &api::greptime_proto::io::prometheus::write::v2::Request,
) -> bool {
    request
        .timeseries
        .iter()
        .any(|series| !series.histograms.is_empty())
}

fn vm_proto_version_response(params: &RemoteWriteQuery) -> Option<axum::response::Response> {
    params
        .get_vm_proto_version
        .as_ref()
        .map(|_| VM_PROTO_VERSION.into_response())
}

fn prepare_remote_write_context(
    params: &RemoteWriteQuery,
    mut query_ctx: QueryContext,
    remote_write_version: &str,
) -> (String, Arc<QueryContext>, HistogramTimer) {
    let db = params.db.clone().unwrap_or_default();
    query_ctx.set_channel(Channel::Prometheus);
    let physical_table = params
        .physical_table
        .clone()
        .unwrap_or_else(|| GREPTIME_PHYSICAL_TABLE.to_string());
    query_ctx.set_extension(PHYSICAL_TABLE_PARAM, physical_table);
    // Stamp the Prometheus metric identity here, before `as_req_iter` splits into the
    // batched and direct write paths, so both inherit it (the batched path bypasses
    // `PromStoreProtocolHandler::write`). Prometheus remote-write metadata is weak
    // here, so the type is inferred from naming.
    query_ctx.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC);
    query_ctx.set_extension(SEMANTIC_SOURCE, SOURCE_PROMETHEUS);
    query_ctx.set_extension(SEMANTIC_SOURCE_VERSION, remote_write_version);
    query_ctx.set_extension(SEMANTIC_METRIC_METADATA_QUALITY, METADATA_QUALITY_INFERRED);
    let query_ctx = Arc::new(query_ctx);
    let timer = crate::metrics::METRIC_HTTP_PROM_STORE_WRITE_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    (db, query_ctx, timer)
}

struct PromWriteOutcome {
    write_cost: usize,
    rows_written: u64,
}

struct PromWriteError {
    error: error::Error,
    rows_written: u64,
}

/// Writes one decoded PRW batch and keeps the number of persisted rows on error.
///
/// The v2 handler uses that partial progress to return Prometheus' written
/// sample/histogram headers even when a later table write fails.
async fn write_prometheus_rows_with_progress(
    prom_store_handler: PromStoreProtocolHandlerRef,
    pending_rows_batcher: Option<Arc<PendingRowsBatcher>>,
    prom_store_with_metric_engine: bool,
    query_ctx: Arc<QueryContext>,
    req: ContextReq,
) -> std::result::Result<PromWriteOutcome, PromWriteError> {
    if prom_store_with_metric_engine && let Some(batcher) = pending_rows_batcher {
        let mut rows_written = 0;
        for (temp_ctx, reqs) in req.as_req_iter(query_ctx) {
            prom_store_handler
                .pre_write(&reqs, temp_ctx.clone())
                .await
                .map_err(|error| PromWriteError {
                    error,
                    rows_written,
                })?;
            let rows = batcher
                .submit(reqs, temp_ctx)
                .await
                .map_err(|error| PromWriteError {
                    error,
                    rows_written,
                })?;
            rows_written += rows;
        }
        return Ok(PromWriteOutcome {
            write_cost: 0,
            rows_written,
        });
    }

    let mut write_cost = 0;
    let mut rows_written = 0;
    for (temp_ctx, reqs) in req.as_req_iter(query_ctx) {
        let cnt: u64 = reqs
            .inserts
            .iter()
            .filter_map(|s| s.rows.as_ref().map(|r| r.rows.len() as u64))
            .sum();
        let output = prom_store_handler
            .write(reqs, temp_ctx, prom_store_with_metric_engine)
            .await
            .map_err(|error| PromWriteError {
                error,
                rows_written,
            })?;
        write_cost += output.meta.cost;
        rows_written += cnt;
    }

    Ok(PromWriteOutcome {
        write_cost,
        rows_written,
    })
}

fn record_remote_write_samples(db: &str, rows: u64) {
    if rows == 0 {
        return;
    }
    crate::metrics::PROM_STORE_REMOTE_WRITE_SAMPLES
        .with_label_values(&[db])
        .inc_by(rows);
}

fn record_remote_write_histograms(db: &str, rows: u64) {
    if rows == 0 {
        return;
    }
    crate::metrics::PROM_STORE_REMOTE_WRITE_HISTOGRAMS
        .with_label_values(&[db])
        .inc_by(rows);
}

fn remote_write_v2_error_response(
    error: error::Error,
    samples: u64,
    histograms: u64,
    exemplars: u64,
) -> axum::response::Response {
    let mut response = error.into_response();
    append_remote_write_v2_written_headers(response.headers_mut(), samples, histograms, exemplars);
    response
}

fn append_remote_write_v2_written_headers(
    headers: &mut HeaderMap,
    samples: u64,
    histograms: u64,
    exemplars: u64,
) {
    headers.insert(
        REMOTE_WRITE_V2_SAMPLES_WRITTEN_HEADER,
        HeaderValue::from_str(&samples.to_string()).expect("u64 header value is valid"),
    );
    headers.insert(
        REMOTE_WRITE_V2_HISTOGRAMS_WRITTEN_HEADER,
        HeaderValue::from_str(&histograms.to_string()).expect("u64 header value is valid"),
    );
    headers.insert(
        REMOTE_WRITE_V2_EXEMPLARS_WRITTEN_HEADER,
        HeaderValue::from_str(&exemplars.to_string()).expect("u64 header value is valid"),
    );
}

enum RemoteWriteProto {
    V1,
    V2,
    Unsupported(mime::Mime),
}

// ref: https://github.com/prometheus/client_golang/blob/74560058a7af7a695db8196c8e84a0754032c6af/exp/api/remote/remote_api.go#L544
fn remote_write_proto(content_type: Option<TypedHeader<headers::ContentType>>) -> RemoteWriteProto {
    let Some(TypedHeader(content_type)) = content_type else {
        return RemoteWriteProto::V1;
    };

    let mime_type: mime::Mime = content_type.into();
    if !mime_type
        .essence_str()
        .eq_ignore_ascii_case(CONTENT_TYPE_PROTOBUF_STR)
    {
        return RemoteWriteProto::Unsupported(mime_type);
    }

    for (name, value) in mime_type.params() {
        if !name.as_str().eq_ignore_ascii_case(CONTENT_TYPE_PROTO_PARAM) {
            continue;
        }

        return match value.as_str() {
            REMOTE_WRITE_V1_PROTO => RemoteWriteProto::V1,
            REMOTE_WRITE_V2_PROTO => RemoteWriteProto::V2,
            _ => RemoteWriteProto::Unsupported(mime_type.clone()),
        };
    }

    RemoteWriteProto::V1
}

fn unsupported_remote_write_v2_encoding_response(
    content_encoding: &headers::ContentEncoding,
) -> Option<axum::response::Response> {
    if content_encoding.contains(DEFAULT_ENCODING) || content_encoding.contains(VM_ENCODING) {
        return None;
    }

    Some((
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        format!(
            "unsupported prometheus remote write content encoding: only {DEFAULT_ENCODING} and {VM_ENCODING} are supported"
        ),
    )
        .into_response())
}

impl IntoResponse for PromStoreResponse {
    fn into_response(self) -> axum::response::Response {
        let mut header_map = HeaderMap::new();
        header_map.insert(&header::CONTENT_TYPE, self.content_type);
        header_map.insert(&header::CONTENT_ENCODING, self.content_encoding);

        let metrics = if self.resp_metrics.is_empty() {
            None
        } else {
            serde_json::to_string(&self.resp_metrics).ok()
        };
        if let Some(m) = metrics.and_then(|m| HeaderValue::from_str(&m).ok()) {
            header_map.insert(&GREPTIME_DB_HEADER_METRICS, m);
        }

        (header_map, self.body).into_response()
    }
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "remote_read")
)]
pub async fn remote_read(
    State(state): State<PromStoreState>,
    Query(params): Query<RemoteWriteQuery>,
    Extension(mut query_ctx): Extension<QueryContext>,
    body: Bytes,
) -> Result<PromStoreResponse> {
    let db = params.db.clone().unwrap_or_default();
    query_ctx.set_channel(Channel::Prometheus);

    let request = decode_remote_read_request(body).await?;

    // Extract schema from special labels and set it in query context
    if let Some(schema) = extract_schema_from_read_request(&request) {
        query_ctx.set_current_schema(&schema);
    }

    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_READ_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    state.prom_store_handler.read(request, query_ctx).await
}

async fn decode_remote_read_request(body: Bytes) -> Result<ReadRequest> {
    let buf = snappy_decompress(&body[..])?;

    ReadRequest::decode(&buf[..]).context(error::DecodePromRemoteRequestSnafu)
}

#[cfg(test)]
mod tests {
    use api::prom_store::remote::ReadRequest;
    use api::v1::RowInsertRequests;
    use async_trait::async_trait;
    use common_query::Output;
    use pipeline::GreptimePipelineParams;
    use session::context::{QueryContext, QueryContextRef};

    use super::*;
    use crate::prom_remote_write::validation::PromValidationMode;
    use crate::prom_store::Metrics;
    use crate::query_handler::PromStoreProtocolHandler;

    #[test]
    fn test_remote_write_proto() {
        assert!(matches!(
            remote_write_proto(content_type(
                "application/x-protobuf;proto=io.prometheus.write.v2.Request"
            )),
            RemoteWriteProto::V2
        ));
        assert!(matches!(
            remote_write_proto(content_type(
                "application/x-protobuf; proto=\"io.prometheus.write.v2.Request\""
            )),
            RemoteWriteProto::V2
        ));
        assert!(matches!(
            remote_write_proto(content_type(
                "APPLICATION/X-PROTOBUF;proto=io.prometheus.write.v2.Request"
            )),
            RemoteWriteProto::V2
        ));
        assert!(matches!(
            remote_write_proto(content_type("application/x-protobuf")),
            RemoteWriteProto::V1
        ));
        assert!(matches!(
            remote_write_proto(content_type(
                "application/x-protobuf;proto=prometheus.WriteRequest"
            )),
            RemoteWriteProto::V1
        ));
        assert!(matches!(
            remote_write_proto(content_type(
                "application/x-protobuf;proto=unknown.WriteRequest"
            )),
            RemoteWriteProto::Unsupported(_)
        ));
        assert!(matches!(
            remote_write_proto(content_type(
                "application/json;proto=io.prometheus.write.v2.Request"
            )),
            RemoteWriteProto::Unsupported(_)
        ));
        assert!(matches!(remote_write_proto(None), RemoteWriteProto::V1));
    }

    fn content_type(value: &str) -> Option<TypedHeader<headers::ContentType>> {
        Some(TypedHeader(std::str::FromStr::from_str(value).unwrap()))
    }

    #[test]
    fn test_prepare_remote_write_context_stamps_semantics() {
        let (_, query_ctx, _timer) = prepare_remote_write_context(
            &RemoteWriteQuery::default(),
            QueryContext::with("greptime", "public"),
            REMOTE_WRITE_V2_VERSION,
        );

        assert_eq!(
            query_ctx.extension(SEMANTIC_SIGNAL_TYPE),
            Some(SIGNAL_TYPE_METRIC)
        );
        assert_eq!(
            query_ctx.extension(SEMANTIC_SOURCE),
            Some(SOURCE_PROMETHEUS)
        );
        assert_eq!(
            query_ctx.extension(SEMANTIC_SOURCE_VERSION),
            Some(REMOTE_WRITE_V2_VERSION)
        );
        assert_eq!(
            query_ctx.extension(SEMANTIC_METRIC_METADATA_QUALITY),
            Some(METADATA_QUALITY_INFERRED)
        );
    }

    #[tokio::test]
    async fn test_remote_write_v2_ignores_pipeline() {
        let request = api::greptime_proto::io::prometheus::write::v2::Request {
            symbols: vec![String::new()],
            timeseries: Vec::new(),
        };
        let body =
            Bytes::from(crate::prom_store::snappy_compress(&request.encode_to_vec()).unwrap());

        let response = remote_write_v2(
            test_state(),
            RemoteWriteQuery::default(),
            QueryContext::with("greptime", "public"),
            pipeline_info(Some("pipeline")),
            false,
            body,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            Some("0"),
            response
                .headers()
                .get(REMOTE_WRITE_V2_SAMPLES_WRITTEN_HEADER)
                .map(|x| x.to_str().unwrap())
        );
    }

    fn test_state() -> PromStoreState {
        PromStoreState {
            prom_store_handler: Arc::new(NoopPromStoreHandler),
            pipeline_handler: None,
            prom_store_with_metric_engine: false,
            prom_validation_mode: PromValidationMode::Strict,
            experimental_enable_prometheus_native_histogram: false,
            pending_rows_batcher: None,
        }
    }

    fn pipeline_info(pipeline_name: Option<&str>) -> PipelineInfo {
        PipelineInfo {
            pipeline_name: pipeline_name.map(ToString::to_string),
            pipeline_version: None,
            pipeline_params: GreptimePipelineParams::default(),
        }
    }

    struct NoopPromStoreHandler;

    #[async_trait]
    impl PromStoreProtocolHandler for NoopPromStoreHandler {
        async fn write(
            &self,
            _request: RowInsertRequests,
            _ctx: QueryContextRef,
            _with_metric_engine: bool,
        ) -> Result<Output> {
            unreachable!("empty remote write v2 request should not write")
        }

        async fn read(
            &self,
            _request: ReadRequest,
            _ctx: QueryContextRef,
        ) -> Result<PromStoreResponse> {
            unimplemented!()
        }

        async fn ingest_metrics(&self, _metrics: Metrics) -> Result<()> {
            unimplemented!()
        }
    }
}
