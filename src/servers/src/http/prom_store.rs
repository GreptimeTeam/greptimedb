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
    SEMANTIC_SOURCE, SIGNAL_TYPE_METRIC, SOURCE_PROMETHEUS,
};

use crate::error::{self, InternalSnafu, PipelineSnafu, Result};
use crate::http::extractor::PipelineInfo;
use crate::http::header::{
    CONTENT_TYPE_PROTOBUF_STR, GREPTIME_DB_HEADER_METRICS, write_cost_header_map,
};
use crate::pending_rows_batcher::PendingRowsBatcher;
use crate::prom_remote_write::decode::PromSeriesProcessor;
use crate::prom_remote_write::decode_remote_write_request;
use crate::prom_remote_write::v2::{RemoteWriteV2RequestExt, decode_remote_write_v2_request};
use crate::prom_remote_write::validation::PromValidationMode;
use crate::prom_store::{extract_schema_from_read_request, snappy_decompress};
use crate::query_handler::{PipelineHandlerRef, PromStoreProtocolHandlerRef, PromStoreResponse};

pub const PHYSICAL_TABLE_PARAM: &str = "physical_table";
pub const DEFAULT_ENCODING: &str = "snappy";
pub const VM_ENCODING: &str = "zstd";
pub const VM_PROTO_VERSION: &str = "1";
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

    if let Some(ct) = content_type
        && is_remote_write_v2(ct.0)
    {
        return remote_write_v2(state, params, query_ctx, pipeline_info, is_zstd, body).await;
    }

    remote_write_v1(state, params, query_ctx, pipeline_info, is_zstd, body).await
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
        pending_rows_batcher,
    } = state;

    if let Some(response) = vm_proto_version_response(&params) {
        return Ok(response);
    }

    let (db, query_ctx, _timer) = prepare_remote_write_context(&params, query_ctx);

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

    let outcome = write_prometheus_rows(
        prom_store_handler,
        pending_rows_batcher,
        prom_store_with_metric_engine,
        &db,
        query_ctx,
        req,
    )
    .await?;

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
        pending_rows_batcher,
    } = state;

    if let Some(response) = vm_proto_version_response(&params) {
        return Ok(response);
    }

    ensure!(
        pipeline_info.pipeline_name.is_none(),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 pipeline processing is not supported".to_string(),
        }
    );

    let (db, query_ctx, _timer) = prepare_remote_write_context(&params, query_ctx);

    let request = decode_remote_write_v2_request(is_zstd, body)?;
    let req = request.into_context_req()?;

    let outcome = write_prometheus_rows(
        prom_store_handler,
        pending_rows_batcher,
        prom_store_with_metric_engine,
        &db,
        query_ctx,
        req,
    )
    .await?;

    let mut headers = write_cost_header_map(outcome.write_cost);
    append_remote_write_v2_written_headers(&mut headers, outcome.rows_written, 0, 0);

    Ok((StatusCode::NO_CONTENT, headers).into_response())
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

async fn write_prometheus_rows(
    prom_store_handler: PromStoreProtocolHandlerRef,
    pending_rows_batcher: Option<Arc<PendingRowsBatcher>>,
    prom_store_with_metric_engine: bool,
    db: &str,
    query_ctx: Arc<QueryContext>,
    req: ContextReq,
) -> Result<PromWriteOutcome> {
    if prom_store_with_metric_engine && let Some(batcher) = pending_rows_batcher {
        let mut rows_written = 0;
        for (temp_ctx, reqs) in req.as_req_iter(query_ctx) {
            prom_store_handler
                .pre_write(&reqs, temp_ctx.clone())
                .await?;
            let rows = batcher.submit(reqs, temp_ctx).await?;
            crate::metrics::PROM_STORE_REMOTE_WRITE_SAMPLES
                .with_label_values(&[db])
                .inc_by(rows);
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
            .await?;
        crate::metrics::PROM_STORE_REMOTE_WRITE_SAMPLES
            .with_label_values(&[db])
            .inc_by(cnt);
        write_cost += output.meta.cost;
        rows_written += cnt;
    }

    Ok(PromWriteOutcome {
        write_cost,
        rows_written,
    })
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

// ref: https://github.com/prometheus/client_golang/blob/74560058a7af7a695db8196c8e84a0754032c6af/exp/api/remote/remote_api.go#L544
fn is_remote_write_v2(content_type: headers::ContentType) -> bool {
    let mime_type: mime::Mime = content_type.into();
    if !mime_type
        .essence_str()
        .eq_ignore_ascii_case(CONTENT_TYPE_PROTOBUF_STR)
    {
        return false;
    }

    mime_type.params().any(|(name, value)| {
        name.as_str().eq_ignore_ascii_case(CONTENT_TYPE_PROTO_PARAM)
            && value.as_str() == REMOTE_WRITE_V2_PROTO
    })
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
    fn test_is_remote_write_v2() {
        assert!(is_remote_write_v2(content_type(
            "application/x-protobuf;proto=io.prometheus.write.v2.Request"
        )));
        assert!(is_remote_write_v2(content_type(
            "application/x-protobuf; proto=\"io.prometheus.write.v2.Request\""
        )));

        assert!(!is_remote_write_v2(content_type("application/x-protobuf")));
        assert!(!is_remote_write_v2(content_type(
            "application/x-protobuf;proto=prometheus.WriteRequest"
        )));
        assert!(!is_remote_write_v2(content_type(
            "application/json;proto=io.prometheus.write.v2.Request"
        )));
    }

    #[tokio::test]
    async fn test_remote_write_v2_rejects_pipeline() {
        let err = remote_write_v2(
            test_state(),
            RemoteWriteQuery::default(),
            QueryContext::with("greptime", "public"),
            pipeline_info(Some("pipeline")),
            false,
            Bytes::new(),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, error::Error::InvalidPromRemoteRequest { .. }));
        assert!(
            err.to_string()
                .contains("pipeline processing is not supported")
        );
    }

    fn content_type(value: &str) -> headers::ContentType {
        std::str::FromStr::from_str(value).unwrap()
    }

    fn test_state() -> PromStoreState {
        PromStoreState {
            prom_store_handler: Arc::new(NoopPromStoreHandler),
            pipeline_handler: None,
            prom_store_with_metric_engine: false,
            prom_validation_mode: PromValidationMode::Strict,
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
            unreachable!("remote write v2 pipeline rejection must happen before writing")
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
