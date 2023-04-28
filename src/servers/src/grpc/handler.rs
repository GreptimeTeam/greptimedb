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
use api::v1::{Basic, GreptimeRequest, RequestHeader};
use common_query::Output;
use common_runtime::Runtime;
use common_telemetry::timer;
use metrics::increment_counter;
use session::context::{QueryContext, QueryContextRef};
use snafu::OptionExt;
use tonic::Status;

use crate::auth::{Identity, Password, UserProviderRef};
use crate::error::Error::{Auth, UnsupportedAuthScheme};
use crate::error::{InvalidQuerySnafu, NotFoundAuthHeaderSnafu};
use crate::grpc::TonicResult;
use crate::query_handler::grpc::ServerGrpcQueryHandlerRef;

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

    pub(crate) async fn handle_request(&self, request: GreptimeRequest) -> TonicResult<Output> {
        let query = request.request.context(InvalidQuerySnafu {
            reason: "Expecting non-empty GreptimeRequest.",
        })?;

        let header = request.header.as_ref();
        let query_ctx = create_query_context(header);

        self.auth(header, &query_ctx).await?;

        let _timer = timer!(
            crate::metrics::METRIC_SERVER_GRPC_DB_REQUEST_TIMER,
            &[(crate::metrics::METRIC_DB_LABEL, &query_ctx.get_db_string())]
        );
        let handler = self.handler.clone();

        // Executes requests in another runtime to
        // 1. prevent the execution from being cancelled unexpected by Tonic runtime;
        //   - Refer to our blog for the rational behind it:
        //     https://www.greptime.com/blogs/2023-01-12-hidden-control-flow.html
        //   - Obtaining a `JoinHandle` to get the panic message (if there's any).
        //     From its docs, `JoinHandle` is cancel safe. The task keeps running even it's handle been dropped.
        // 2. avoid the handler blocks the gRPC runtime incidentally.
        let handle = self
            .runtime
            .spawn(async move { handler.do_query(query, query_ctx).await });

        let output = handle.await.map_err(|e| {
            if e.is_cancelled() {
                Status::cancelled(e.to_string())
            } else if e.is_panic() {
                Status::internal(format!("{:?}", e.into_panic()))
            } else {
                Status::unknown(e.to_string())
            }
        })??;
        Ok(output)
    }

    async fn auth(
        &self,
        header: Option<&RequestHeader>,
        query_ctx: &QueryContextRef,
    ) -> TonicResult<()> {
        let Some(user_provider) = self.user_provider.as_ref() else { return Ok(()) };

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
                    Password::PlainText(&password),
                    &query_ctx.current_catalog(),
                    &query_ctx.current_schema(),
                )
                .await
                .map_err(|e| Auth { source: e }),
            AuthScheme::Token(_) => Err(UnsupportedAuthScheme {
                name: "Token AuthScheme".to_string(),
            }),
        }
        .map_err(|e| {
            increment_counter!(crate::metrics::METRIC_AUTH_FAILURE);
            Status::unauthenticated(e.to_string())
        })?;
        Ok(())
    }
}

pub(crate) fn create_query_context(header: Option<&RequestHeader>) -> QueryContextRef {
    let ctx = QueryContext::arc();
    if let Some(header) = header {
        // We provide dbname field in newer versions of protos/sdks
        // parse dbname from header in priority
        if !header.dbname.is_empty() {
            let (catalog, schema) =
                crate::parse_catalog_and_schema_from_client_database_name(&header.dbname);
            ctx.set_current_catalog(catalog);
            ctx.set_current_schema(schema);
        } else {
            if !header.catalog.is_empty() {
                ctx.set_current_catalog(&header.catalog);
            }
            if !header.schema.is_empty() {
                ctx.set_current_schema(&header.schema);
            }
        }
    };
    ctx
}
