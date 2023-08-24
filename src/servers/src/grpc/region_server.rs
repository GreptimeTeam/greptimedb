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

use api::v1::auth_header::AuthScheme;
use api::v1::region::region_server::Region as RegionServer;
use api::v1::region::{region_request, RegionRequest, RegionResponse};
use api::v1::{Basic, RequestHeader};
use async_trait::async_trait;
use auth::{Identity, Password, UserInfoRef, UserProviderRef};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use common_runtime::Runtime;
use common_telemetry::{debug, error};
use metrics::increment_counter;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response};

use crate::error::{
    AuthSnafu, InvalidQuerySnafu, JoinTaskSnafu, NotFoundAuthHeaderSnafu, Result,
    UnsupportedAuthSchemeSnafu,
};
use crate::grpc::greptime_handler::RequestTimer;
use crate::grpc::TonicResult;
use crate::metrics::{METRIC_AUTH_FAILURE, METRIC_CODE_LABEL};

#[async_trait]
pub trait RegionServerHandler: Send + Sync {
    async fn handle(&self, request: region_request::Body) -> Result<RegionResponse>;
}

pub type RegionServerHandlerRef = Arc<dyn RegionServerHandler>;

#[derive(Clone)]
pub struct RegionServerRequestHandler {
    handler: Arc<dyn RegionServerHandler>,
    user_provider: Option<UserProviderRef>,
    runtime: Arc<Runtime>,
}

impl RegionServerRequestHandler {
    pub fn new(
        handler: Arc<dyn RegionServerHandler>,
        user_provider: Option<UserProviderRef>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            handler,
            user_provider,
            runtime,
        }
    }

    async fn handle(&self, request: RegionRequest) -> Result<RegionResponse> {
        let query = request.body.context(InvalidQuerySnafu {
            reason: "Expecting non-empty GreptimeRequest.",
        })?;

        let header = request.header.as_ref();
        let query_ctx = create_query_context(header);
        let user_info = self.auth(header, &query_ctx).await?;
        query_ctx.set_current_user(user_info);

        let handler = self.handler.clone();
        let request_type = query.as_ref().to_string();
        let timer = RequestTimer::new(query_ctx.get_db_string(), request_type);

        // Executes requests in another runtime to
        // 1. prevent the execution from being cancelled unexpected by Tonic runtime;
        //   - Refer to our blog for the rational behind it:
        //     https://www.greptime.com/blogs/2023-01-12-hidden-control-flow.html
        //   - Obtaining a `JoinHandle` to get the panic message (if there's any).
        //     From its docs, `JoinHandle` is cancel safe. The task keeps running even it's handle been dropped.
        // 2. avoid the handler blocks the gRPC runtime incidentally.
        let handle = self.runtime.spawn(async move {
            handler.handle(query).await.map_err(|e| {
                if e.status_code().should_log_error() {
                    error!(e; "Failed to handle request");
                } else {
                    // Currently, we still print a debug log.
                    debug!("Failed to handle request, err: {}", e);
                }
                e
            })
        });

        handle.await.context(JoinTaskSnafu).map_err(|e| {
            timer.record(e.status_code());
            e
        })?
    }

    async fn auth(
        &self,
        header: Option<&RequestHeader>,
        query_ctx: &QueryContextRef,
    ) -> Result<Option<UserInfoRef>> {
        let Some(user_provider) = self.user_provider.as_ref() else {
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
            AuthScheme::Token(_) => UnsupportedAuthSchemeSnafu {
                name: "Token AuthScheme".to_string(),
            }
            .fail(),
        }
        .map(Some)
        .map_err(|e| {
            increment_counter!(
                METRIC_AUTH_FAILURE,
                &[(METRIC_CODE_LABEL, format!("{}", e.status_code()))]
            );
            e
        })
    }
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

    QueryContextBuilder::default()
        .current_catalog(catalog.to_string())
        .current_schema(schema.to_string())
        .try_trace_id(header.and_then(|h: &RequestHeader| h.trace_id))
        .build()
}

#[async_trait]
impl RegionServer for RegionServerRequestHandler {
    async fn handle(
        &self,
        request: Request<RegionRequest>,
    ) -> TonicResult<Response<RegionResponse>> {
        let request = request.into_inner();
        let response = self.handle(request).await?;
        Ok(Response::new(response))
    }
}
