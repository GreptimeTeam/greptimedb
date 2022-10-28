use api::prometheus::remote::{ReadRequest, WriteRequest};
use axum::extract::{RawBody, State};
use axum::http::StatusCode;
use hyper::Body;
use prost::Message;
use snafu::prelude::*;

use crate::error::Result;
use crate::error::{self};
use crate::http::HttpResponse;
use crate::prometheus::snappy_decompress;
use crate::query_handler::PrometheusProtocolHandlerRef;

#[axum_macros::debug_handler]
pub async fn remote_write(
    State(handler): State<PrometheusProtocolHandlerRef>,
    RawBody(body): RawBody,
) -> Result<(StatusCode, HttpResponse)> {
    let request = decode_remote_write_request(body).await?;

    handler.write(request).await?;

    Ok((StatusCode::NO_CONTENT, HttpResponse::Text("".to_string())))
}

#[axum_macros::debug_handler]
pub async fn remote_read(
    State(handler): State<PrometheusProtocolHandlerRef>,
    RawBody(body): RawBody,
) -> Result<HttpResponse> {
    let request = decode_remote_read_request(body).await?;

    handler.read(request).await
}

async fn decode_remote_write_request(body: Body) -> Result<WriteRequest> {
    let body = hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)?;

    let buf = snappy_decompress(&body[..])?;

    WriteRequest::decode(&buf[..]).context(error::DecodePromRemoteRequestSnafu)
}

async fn decode_remote_read_request(body: Body) -> Result<ReadRequest> {
    let body = hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)?;

    let buf = snappy_decompress(&body[..])?;

    ReadRequest::decode(&buf[..]).context(error::DecodePromRemoteRequestSnafu)
}
