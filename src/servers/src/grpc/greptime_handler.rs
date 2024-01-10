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

//! Handler for Greptime Database service. It's implemented by frontend.

use std::sync::Arc;
use std::time::Instant;

use api::helper::request_type;
use api::v1::auth_header::AuthScheme;
use api::v1::{Basic, GreptimeRequest, RequestHeader};
use auth::{Identity, Password, UserInfoRef, UserProviderRef};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_runtime::Runtime;
use common_telemetry::logging;
use common_time::timezone::parse_timezone;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{OptionExt, ResultExt};

use crate::error::Error::UnsupportedAuthScheme;
use crate::error::{AuthSnafu, InvalidQuerySnafu, JoinTaskSnafu, NotFoundAuthHeaderSnafu, Result};
use crate::metrics::{METRIC_AUTH_FAILURE, METRIC_SERVER_GRPC_DB_REQUEST_TIMER};
use crate::query_handler::grpc::ServerGrpcQueryHandlerRef;

#[derive(Clone)]
pub struct GreptimeRequestHandler {
    handler: ServerGrpcQueryHandlerRef,
    user_provider: Option<UserProviderRef>,
    runtime: Arc<Runtime>,
}

impl GreptimeRequestHandler {
    pub fn new(
        handler: ServerGrpcQueryHandlerRef,
        user_provider: Option<UserProviderRef>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            handler,
            user_provider,
            runtime,
        }
    }

    pub(crate) async fn handle_request(&self, request: GreptimeRequest) -> Result<Output> {
        let query = request.request.context(InvalidQuerySnafu {
            reason: "Expecting non-empty GreptimeRequest.",
        })?;

        let header = request.header.as_ref();
        let query_ctx = create_query_context(header);
        let user_info = auth(self.user_provider.clone(), header, &query_ctx).await?;
        query_ctx.set_current_user(user_info);

        let handler = self.handler.clone();
        let request_type = request_type(&query).to_string();
        let db = query_ctx.get_db_string();
        let timer = RequestTimer::new(db.clone(), request_type);

        // Executes requests in another runtime to
        // 1. prevent the execution from being cancelled unexpected by Tonic runtime;
        //   - Refer to our blog for the rational behind it:
        //     https://www.greptime.com/blogs/2023-01-12-hidden-control-flow.html
        //   - Obtaining a `JoinHandle` to get the panic message (if there's any).
        //     From its docs, `JoinHandle` is cancel safe. The task keeps running even it's handle been dropped.
        // 2. avoid the handler blocks the gRPC runtime incidentally.
        let handle = self.runtime.spawn(async move {
            handler.do_query(query, query_ctx).await.map_err(|e| {
                if e.status_code().should_log_error() {
                    logging::error!(e; "Failed to handle request");
                } else {
                    // Currently, we still print a debug log.
                    logging::debug!("Failed to handle request, err: {:?}", e);
                }
                e
            })
        });

        handle.await.context(JoinTaskSnafu).map_err(|e| {
            timer.record(e.status_code());
            e
        })?
    }
}

pub(crate) async fn auth(
    user_provider: Option<UserProviderRef>,
    header: Option<&RequestHeader>,
    query_ctx: &QueryContextRef,
) -> Result<Option<UserInfoRef>> {
    let Some(user_provider) = user_provider else {
        return Ok(None);
    };

    let auth_scheme = header
        .and_then(|header| {
            header
                .authorization
                .as_ref()
                .and_then(|x| x.auth_scheme.clone())
        })
        .context(NotFoundAuthHeaderSnafu)?;

    match auth_scheme {
        AuthScheme::Basic(Basic { username, password }) => user_provider
            .auth(
                Identity::UserId(&username, None),
                Password::PlainText(password.into()),
                query_ctx.current_catalog(),
                query_ctx.current_schema(),
            )
            .await
            .context(AuthSnafu),
        AuthScheme::Token(_) => Err(UnsupportedAuthScheme {
            name: "Token AuthScheme".to_string(),
        }),
    }
    .map(Some)
    .map_err(|e| {
        METRIC_AUTH_FAILURE
            .with_label_values(&[e.status_code().as_ref()])
            .inc();
        e
    })
}

pub(crate) fn create_query_context(header: Option<&RequestHeader>) -> QueryContextRef {
    let (catalog, schema) = header
        .map(|header| {
            // We provide dbname field in newer versions of protos/sdks
            // parse dbname from header in priority
            if !header.dbname.is_empty() {
                parse_catalog_and_schema_from_db_string(&header.dbname)
            } else {
                (
                    if !header.catalog.is_empty() {
                        &header.catalog
                    } else {
                        DEFAULT_CATALOG_NAME
                    },
                    if !header.schema.is_empty() {
                        &header.schema
                    } else {
                        DEFAULT_SCHEMA_NAME
                    },
                )
            }
        })
        .unwrap_or((DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME));
    let timezone = parse_timezone(header.map(|h| h.timezone.as_str()));
    let ctx = QueryContextBuilder::default()
        .current_catalog(catalog.to_string())
        .current_schema(schema.to_string())
        .build();
    ctx.set_timezone(timezone);
    ctx
}

/// Histogram timer for handling gRPC request.
///
/// The timer records the elapsed time with [StatusCode::Success] on drop.
pub(crate) struct RequestTimer {
    start: Instant,
    db: String,
    request_type: String,
    status_code: StatusCode,
}

impl RequestTimer {
    /// Returns a new timer.
    pub fn new(db: String, request_type: String) -> RequestTimer {
        RequestTimer {
            start: Instant::now(),
            db,
            request_type,
            status_code: StatusCode::Success,
        }
    }

    /// Consumes the timer and record the elapsed time with specific `status_code`.
    pub fn record(mut self, status_code: StatusCode) {
        self.status_code = status_code;
    }
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        METRIC_SERVER_GRPC_DB_REQUEST_TIMER
            .with_label_values(&[
                self.db.as_str(),
                self.request_type.as_str(),
                self.status_code.as_ref(),
            ])
            .observe(self.start.elapsed().as_secs_f64());
    }
}
