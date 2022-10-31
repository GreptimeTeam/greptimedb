use api::prometheus::remote::{ReadRequest, WriteRequest};
use axum::extract::{RawBody, State};
use axum::http::header;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use hyper::Body;
use prost::Message;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::prometheus::snappy_decompress;
use crate::query_handler::{PrometheusProtocolHandlerRef, PrometheusResponse};

#[axum_macros::debug_handler]
pub async fn remote_write(
    State(handler): State<PrometheusProtocolHandlerRef>,
    RawBody(body): RawBody,
) -> Result<(StatusCode, ())> {
    let request = decode_remote_write_request(body).await?;

    handler.write(request).await?;

    Ok((StatusCode::NO_CONTENT, ()))
}

impl IntoResponse for PrometheusResponse {
    fn into_response(self) -> axum::response::Response {
        (
            [
                (header::CONTENT_TYPE, self.content_type),
                (header::CONTENT_ENCODING, self.content_encoding),
            ],
            self.body,
        )
            .into_response()
    }
}

#[axum_macros::debug_handler]
pub async fn remote_read(
    State(handler): State<PrometheusProtocolHandlerRef>,
    RawBody(body): RawBody,
) -> Result<PrometheusResponse> {
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
