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
use api::v1::RowInsertRequests;
use axum::extract::{Query, RawBody, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::{Extension, TypedHeader};
use bytes::Bytes;
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::tracing;
use hyper::{Body, HeaderMap};
use lazy_static::lazy_static;
use object_pool::Pool;
use prost::Message;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use session::context::QueryContextRef;
use snafu::prelude::*;

use super::header::{write_cost_header_map, GREPTIME_DB_HEADER_METRICS};
use crate::error::{self, Result, UnexpectedPhysicalTableSnafu};
use crate::prom_store::{snappy_decompress, zstd_decompress};
use crate::proto::PromWriteRequest;
use crate::query_handler::{PromStoreProtocolHandlerRef, PromStoreResponse};

pub const PHYSICAL_TABLE_PARAM: &str = "physical_table";
lazy_static! {
    static ref PROM_WRITE_REQUEST_POOL: Pool<PromWriteRequest> =
        Pool::new(256, PromWriteRequest::default);
}

pub const DEFAULT_ENCODING: &str = "snappy";
pub const VM_ENCODING: &str = "zstd";
pub const VM_PROTO_VERSION: &str = "1";

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
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

/// Same with [remote_write] but won't store data to metric engine.
#[axum_macros::debug_handler]
pub async fn route_write_without_metric_engine(
    State(handler): State<PromStoreProtocolHandlerRef>,
    Query(params): Query<RemoteWriteQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
    content_encoding: TypedHeader<headers::ContentEncoding>,
    RawBody(body): RawBody,
) -> Result<impl IntoResponse> {
    // VictoriaMetrics handshake
    if let Some(_vm_handshake) = params.get_vm_proto_version {
        return Ok(VM_PROTO_VERSION.into_response());
    }

    let db = params.db.clone().unwrap_or_default();
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_WRITE_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let is_zstd = content_encoding.contains(VM_ENCODING);
    let (request, samples) = decode_remote_write_request(is_zstd, body, true).await?;
    // reject if physical table is specified when metric engine is disabled
    if params.physical_table.is_some() {
        return UnexpectedPhysicalTableSnafu {}.fail();
    }

    let output = handler.write(request, query_ctx, false).await?;
    crate::metrics::PROM_STORE_REMOTE_WRITE_SAMPLES.inc_by(samples as u64);
    Ok((
        StatusCode::NO_CONTENT,
        write_cost_header_map(output.meta.cost),
    )
        .into_response())
}

/// Same with [remote_write] but won't store data to metric engine.
/// And without strict_mode on will not check invalid UTF-8.
#[axum_macros::debug_handler]
pub async fn route_write_without_metric_engine_and_strict_mode(
    State(handler): State<PromStoreProtocolHandlerRef>,
    Query(params): Query<RemoteWriteQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
    content_encoding: TypedHeader<headers::ContentEncoding>,
    RawBody(body): RawBody,
) -> Result<impl IntoResponse> {
    let db = params.db.clone().unwrap_or_default();
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_WRITE_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let is_zstd = content_encoding.contains(VM_ENCODING);
    let (request, samples) = decode_remote_write_request(is_zstd, body, false).await?;
    // reject if physical table is specified when metric engine is disabled
    if params.physical_table.is_some() {
        return UnexpectedPhysicalTableSnafu {}.fail();
    }

    let output = handler.write(request, query_ctx, false).await?;
    crate::metrics::PROM_STORE_REMOTE_WRITE_SAMPLES.inc_by(samples as u64);
    Ok((
        StatusCode::NO_CONTENT,
        write_cost_header_map(output.meta.cost),
    )
        .into_response())
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "remote_write")
)]
pub async fn remote_write(
    State(handler): State<PromStoreProtocolHandlerRef>,
    Query(params): Query<RemoteWriteQuery>,
    Extension(mut query_ctx): Extension<QueryContextRef>,
    content_encoding: TypedHeader<headers::ContentEncoding>,
    RawBody(body): RawBody,
) -> Result<impl IntoResponse> {
    // VictoriaMetrics handshake
    if let Some(_vm_handshake) = params.get_vm_proto_version {
        return Ok(VM_PROTO_VERSION.into_response());
    }

    let db = params.db.clone().unwrap_or_default();
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_WRITE_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let is_zstd = content_encoding.contains(VM_ENCODING);
    let (request, samples) =
        decode_remote_write_request_to_row_inserts(is_zstd, body, true).await?;

    if let Some(physical_table) = params.physical_table {
        let mut new_query_ctx = query_ctx.as_ref().clone();
        new_query_ctx.set_extension(PHYSICAL_TABLE_PARAM, physical_table);
        query_ctx = Arc::new(new_query_ctx);
    }

    let output = handler.write(request, query_ctx, true).await?;
    crate::metrics::PROM_STORE_REMOTE_WRITE_SAMPLES.inc_by(samples as u64);
    Ok((
        StatusCode::NO_CONTENT,
        write_cost_header_map(output.meta.cost),
    )
        .into_response())
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "remote_write")
)]
pub async fn remote_write_without_strict_mode(
    State(handler): State<PromStoreProtocolHandlerRef>,
    Query(params): Query<RemoteWriteQuery>,
    Extension(mut query_ctx): Extension<QueryContextRef>,
    content_encoding: TypedHeader<headers::ContentEncoding>,
    RawBody(body): RawBody,
) -> Result<impl IntoResponse> {
    // VictoriaMetrics handshake
    if let Some(_vm_handshake) = params.get_vm_proto_version {
        return Ok(VM_PROTO_VERSION.into_response());
    }

    let db = params.db.clone().unwrap_or_default();
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_WRITE_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let is_zstd = content_encoding.contains(VM_ENCODING);
    let (request, samples) =
        decode_remote_write_request_to_row_inserts(is_zstd, body, false).await?;

    if let Some(physical_table) = params.physical_table {
        let mut new_query_ctx = query_ctx.as_ref().clone();
        new_query_ctx.set_extension(PHYSICAL_TABLE_PARAM, physical_table);
        query_ctx = Arc::new(new_query_ctx);
    }

    let output = handler.write(request, query_ctx, false).await?;
    crate::metrics::PROM_STORE_REMOTE_WRITE_SAMPLES.inc_by(samples as u64);
    Ok((
        StatusCode::NO_CONTENT,
        write_cost_header_map(output.meta.cost),
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
    State(handler): State<PromStoreProtocolHandlerRef>,
    Query(params): Query<RemoteWriteQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
    RawBody(body): RawBody,
) -> Result<PromStoreResponse> {
    let db = params.db.clone().unwrap_or_default();
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_READ_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let request = decode_remote_read_request(body).await?;

    handler.read(request, query_ctx).await
}

async fn decode_remote_write_request_to_row_inserts(
    is_zstd: bool,
    body: Body,
    is_strict_mode: bool,
) -> Result<(RowInsertRequests, usize)> {
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_DECODE_ELAPSED.start_timer();
    let body = hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)?;

    let buf = Bytes::from(if is_zstd {
        zstd_decompress(&body[..])?
    } else {
        snappy_decompress(&body[..])?
    });

    let mut request = PROM_WRITE_REQUEST_POOL.pull(PromWriteRequest::default);
    request
        .merge(buf, is_strict_mode)
        .context(error::DecodePromRemoteRequestSnafu)?;
    Ok(request.as_row_insert_requests())
}

async fn decode_remote_write_request(
    is_zstd: bool,
    body: Body,
    is_strict_mode: bool,
) -> Result<(RowInsertRequests, usize)> {
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_DECODE_ELAPSED.start_timer();
    let body = hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)?;

    let buf = Bytes::from(if is_zstd {
        zstd_decompress(&body[..])?
    } else {
        snappy_decompress(&body[..])?
    });

    let mut request = PromWriteRequest::default();
    request
        .merge(buf, is_strict_mode)
        .context(error::DecodePromRemoteRequestSnafu)?;
    Ok(request.as_row_insert_requests())
}

async fn decode_remote_read_request(body: Body) -> Result<ReadRequest> {
    let body = hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)?;

    let buf = snappy_decompress(&body[..])?;

    ReadRequest::decode(&buf[..]).context(error::DecodePromRemoteRequestSnafu)
}
