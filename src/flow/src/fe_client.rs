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

//! Frontend Client for flownode, used for writing result back to database

use api::v1::greptime_database_client::GreptimeDatabaseClient;
use api::v1::greptime_request::Request;
use api::v1::{
    GreptimeRequest, GreptimeResponse, RequestHeader, RowDeleteRequests, RowInsertRequests,
};
use common_error::ext::BoxedError;
use common_frontend::handler::FrontendInvoker;
use common_query::Output;
use common_telemetry::tracing_context::{TracingContext, W3cTrace};
use session::context::{QueryContext, QueryContextRef};
use snafu::IntoError;
use tokio::sync::Mutex;

use crate::{Error, Result};

/// Frontend client for writing result back to database
pub struct FrontendClient {
    client: GreptimeDatabaseClient<tonic::transport::Channel>,
}

impl FrontendClient {
    pub fn new(channel: tonic::transport::Channel) -> Self {
        Self {
            client: GreptimeDatabaseClient::new(channel),
        }
    }
}

fn to_rpc_request(request: Request, ctx: &QueryContext) -> GreptimeRequest {
    let header = RequestHeader {
        catalog: ctx.current_catalog().to_string(),
        schema: ctx.current_schema().to_string(),
        authorization: None,
        // dbname is empty so that header use catalog+schema to determine the database
        // see `create_query_context` in `greptime_handler.rs`
        dbname: "".to_string(),
        timezone: ctx.timezone().to_string(),
        tracing_context: TracingContext::from_current_span().to_w3c(),
    };
    GreptimeRequest {
        header: Some(header),
        request: Some(request),
    }
}

fn from_rpc_error(e: tonic::Status) -> common_frontend::error::Error {
    common_frontend::error::ExternalSnafu {}
        .into_error(BoxedError::new(client::error::Error::from(e)))
}

fn resp_to_output(resp: GreptimeResponse) -> Output {
    let affect_rows = resp
        .response
        .map(|r| match r {
            api::v1::greptime_response::Response::AffectedRows(r) => r.value,
        })
        .unwrap_or(0);

    Output::new_with_affected_rows(affect_rows as usize)
}

#[async_trait::async_trait]
impl FrontendInvoker for FrontendClient {
    async fn row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> common_frontend::error::Result<Output> {
        let req = to_rpc_request(Request::RowInserts(requests), &ctx);
        let resp = self
            .client
            .clone()
            .handle(req)
            .await
            .map_err(from_rpc_error)?;
        Ok(resp_to_output(resp.into_inner()))
    }

    async fn row_deletes(
        &self,
        requests: RowDeleteRequests,
        ctx: QueryContextRef,
    ) -> common_frontend::error::Result<Output> {
        let req = to_rpc_request(Request::RowDeletes(requests), &ctx);
        let resp = self
            .client
            .clone()
            .handle(req)
            .await
            .map_err(from_rpc_error)?;
        Ok(resp_to_output(resp.into_inner()))
    }
}
