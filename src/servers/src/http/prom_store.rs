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
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Extension;
use axum_extra::TypedHeader;
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::tracing;
use hyper::HeaderMap;
use lazy_static::lazy_static;
use object_pool::Pool;
use pipeline::util::to_pipeline_version;
use pipeline::{ContextReq, PipelineDefinition};
use prost::Message;
use serde::{Deserialize, Serialize};
use session::context::{Channel, QueryContext};
use snafu::prelude::*;

use crate::error::{self, InternalSnafu, PipelineSnafu, Result};
use crate::http::extractor::PipelineInfo;
use crate::http::header::{write_cost_header_map, GREPTIME_DB_HEADER_METRICS};
use crate::http::PromValidationMode;
use crate::prom_store::{snappy_decompress, zstd_decompress};
use crate::proto::{PromSeriesProcessor, PromWriteRequest};
use crate::query_handler::{PipelineHandlerRef, PromStoreProtocolHandlerRef, PromStoreResponse};

pub const PHYSICAL_TABLE_PARAM: &str = "physical_table";
lazy_static! {
    static ref PROM_WRITE_REQUEST_POOL: Pool<PromWriteRequest> =
        Pool::new(256, PromWriteRequest::default);
}

pub const DEFAULT_ENCODING: &str = "snappy";
pub const VM_ENCODING: &str = "zstd";
pub const VM_PROTO_VERSION: &str = "1";

#[derive(Clone)]
pub struct PromStoreState {
    pub prom_store_handler: PromStoreProtocolHandlerRef,
    pub pipeline_handler: Option<PipelineHandlerRef>,
    pub prom_store_with_metric_engine: bool,
    pub prom_validation_mode: PromValidationMode,
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
    Extension(mut query_ctx): Extension<QueryContext>,
    pipeline_info: PipelineInfo,
    content_encoding: TypedHeader<headers::ContentEncoding>,
    body: Bytes,
) -> Result<impl IntoResponse> {
    let PromStoreState {
        prom_store_handler,
        pipeline_handler,
        prom_store_with_metric_engine,
        prom_validation_mode,
    } = state;

    if let Some(_vm_handshake) = params.get_vm_proto_version {
        return Ok(VM_PROTO_VERSION.into_response());
    }

    let db = params.db.clone().unwrap_or_default();
    query_ctx.set_channel(Channel::Prometheus);
    if let Some(physical_table) = params.physical_table {
        query_ctx.set_extension(PHYSICAL_TABLE_PARAM, physical_table);
    }
    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_WRITE_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let is_zstd = content_encoding.contains(VM_ENCODING);

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

    let req =
        decode_remote_write_request(is_zstd, body, prom_validation_mode, &mut processor).await?;

    let mut cost = 0;
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
            .with_label_values(&[db.as_str()])
            .inc_by(cnt);
        cost += output.meta.cost;
    }

    Ok((StatusCode::NO_CONTENT, write_cost_header_map(cost)).into_response())
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
    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_READ_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let request = decode_remote_read_request(body).await?;

    state.prom_store_handler.read(request, query_ctx).await
}

fn try_decompress(is_zstd: bool, body: &[u8]) -> Result<Bytes> {
    Ok(Bytes::from(if is_zstd {
        zstd_decompress(body)?
    } else {
        snappy_decompress(body)?
    }))
}

async fn decode_remote_write_request(
    is_zstd: bool,
    body: Bytes,
    prom_validation_mode: PromValidationMode,
    processor: &mut PromSeriesProcessor,
) -> Result<ContextReq> {
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_DECODE_ELAPSED.start_timer();

    // due to vmagent's limitation, there is a chance that vmagent is
    // sending content type wrong so we have to apply a fallback with decoding
    // the content in another method.
    //
    // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5301
    // see https://github.com/GreptimeTeam/greptimedb/issues/3929
    let buf = if let Ok(buf) = try_decompress(is_zstd, &body[..]) {
        buf
    } else {
        // fallback to the other compression method
        try_decompress(!is_zstd, &body[..])?
    };

    let mut request = PROM_WRITE_REQUEST_POOL.pull(PromWriteRequest::default);

    request
        .merge(buf, prom_validation_mode, processor)
        .context(error::DecodePromRemoteRequestSnafu)?;

    if processor.use_pipeline {
        processor.exec_pipeline().await
    } else {
        Ok(request.as_row_insert_requests())
    }
}

async fn decode_remote_read_request(body: Bytes) -> Result<ReadRequest> {
    let buf = snappy_decompress(&body[..])?;

    ReadRequest::decode(&buf[..]).context(error::DecodePromRemoteRequestSnafu)
}
