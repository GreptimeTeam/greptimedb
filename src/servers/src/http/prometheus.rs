use api::prometheus::remote::{ReadRequest, WriteRequest};
use axum::extract::{Query, RawBody, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use hyper::Body;
use prost::Message;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::prometheus::snappy_decompress;
use crate::query_handler::{PrometheusProtocolHandlerRef, PrometheusResponse};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DatabaseQuery {
    pub db: Option<String>,
}

impl Default for DatabaseQuery {
    fn default() -> DatabaseQuery {
        Self {
            db: Some(DEFAULT_SCHEMA_NAME.to_string()),
        }
    }
}

#[axum_macros::debug_handler]
pub async fn remote_write(
    State(handler): State<PrometheusProtocolHandlerRef>,
    Query(params): Query<DatabaseQuery>,
    RawBody(body): RawBody,
) -> Result<(StatusCode, ())> {
    let request = decode_remote_write_request(body).await?;

    handler
        .write(params.db.as_deref().unwrap_or(DEFAULT_SCHEMA_NAME), request)
        .await?;

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
    Query(params): Query<DatabaseQuery>,
    RawBody(body): RawBody,
) -> Result<PrometheusResponse> {
    let request = decode_remote_read_request(body).await?;

    handler
        .read(params.db.as_deref().unwrap_or(DEFAULT_SCHEMA_NAME), request)
        .await
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
