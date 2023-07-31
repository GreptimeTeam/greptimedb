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

use api::prom_store::remote::{ReadRequest, WriteRequest};
use axum::extract::{Query, RawBody, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_telemetry::timer;
use hyper::Body;
use prost::Message;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use session::context::QueryContext;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::parse_catalog_and_schema_from_client_database_name;
use crate::prom_store::snappy_decompress;
use crate::query_handler::{PromStoreProtocolHandlerRef, PromStoreResponse};

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
    State(handler): State<PromStoreProtocolHandlerRef>,
    Query(params): Query<DatabaseQuery>,
    RawBody(body): RawBody,
) -> Result<(StatusCode, ())> {
    let request = decode_remote_write_request(body).await?;

    let _timer = timer!(
        crate::metrics::METRIC_HTTP_PROM_STORE_WRITE_ELAPSED,
        &[(
            crate::metrics::METRIC_DB_LABEL,
            params.db.clone().unwrap_or_default()
        )]
    );
    let ctx = if let Some(db) = params.db {
        let (catalog, schema) = parse_catalog_and_schema_from_client_database_name(&db);
        QueryContext::with(catalog, schema)
    } else {
        QueryContext::arc()
    };

    // TODO(shuiyisong): add more error log
    handler.write(request, ctx).await?;
    Ok((StatusCode::NO_CONTENT, ()))
}

impl IntoResponse for PromStoreResponse {
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
    State(handler): State<PromStoreProtocolHandlerRef>,
    Query(params): Query<DatabaseQuery>,
    RawBody(body): RawBody,
) -> Result<PromStoreResponse> {
    let request = decode_remote_read_request(body).await?;

    let _timer = timer!(
        crate::metrics::METRIC_HTTP_PROM_STORE_READ_ELAPSED,
        &[(
            crate::metrics::METRIC_DB_LABEL,
            params.db.clone().unwrap_or_default()
        )]
    );
    let ctx = if let Some(db) = params.db {
        let (catalog, schema) = parse_catalog_and_schema_from_client_database_name(&db);
        QueryContext::with(catalog, schema)
    } else {
        QueryContext::arc()
    };

    // TODO(shuiyisong): add more error log
    handler.read(request, ctx).await
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
