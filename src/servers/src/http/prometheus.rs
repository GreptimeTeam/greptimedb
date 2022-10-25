use api::prometheus::remote::{ReadRequest, WriteRequest};
use axum::extract::{RawBody, State};
use axum::http::StatusCode;
use hyper::Body;
use prost::Message;
use snafu::prelude::*;
use snap::raw::{Decoder, Encoder};

use crate::error::Result;
use crate::error::{self};
use crate::http::HttpResponse;
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

    let response = match handler.read(request).await? {
        HttpResponse::Bytes(mut response) => {
            response.bytes = snappy_compress(&response.bytes)?;

            HttpResponse::Bytes(response)
        }
        _ => unreachable!(),
    };

    Ok(response)
}

#[inline]
fn snappy_decompress(buf: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = Decoder::new();
    decoder
        .decompress_vec(buf)
        .context(error::DecompressPromRemoteRequestSnafu)
}

#[inline]
fn snappy_compress(buf: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    encoder
        .compress_vec(buf)
        .context(error::DecompressPromRemoteRequestSnafu)
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

#[cfg(test)]
mod tests {}
