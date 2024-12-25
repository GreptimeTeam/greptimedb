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

//! Error definition for flow module

use std::any::Any;

use common_error::ext::BoxedError;
use common_error::{define_into_tonic_status, from_err_code_msg_to_header};
use common_macro::stack_trace_debug;
use common_telemetry::common_error::ext::ErrorExt;
use common_telemetry::common_error::status_code::StatusCode;
use http::HeaderValue;
use snafu::{Location, Snafu};
use tonic::metadata::MetadataMap;

use crate::adapter::FlowId;
use crate::expr::EvalError;

/// This error is used to represent all possible errors that can occur in the flow module.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("External error"))]
    External {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Internal error"))]
    Internal {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    /// TODO(discord9): add detailed location of column
    #[snafu(display("Failed to eval stream"))]
    Eval {
        source: EvalError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table not found: {name}"))]
    TableNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table not found: {msg}, meta error: {source}"))]
    TableNotFoundMeta {
        source: common_meta::error::Error,
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Flow not found, id={id}"))]
    FlowNotFound {
        id: FlowId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to list flows in flownode={id:?}"))]
    ListFlows {
        id: Option<common_meta::FlownodeId>,
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Flow already exist, id={id}"))]
    FlowAlreadyExist {
        id: FlowId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to join task"))]
    JoinTask {
        #[snafu(source)]
        error: tokio::task::JoinError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid query: {reason}"))]
    InvalidQuery {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Not implement in flow: {reason}"))]
    NotImplemented {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Flow plan error: {reason}"))]
    Plan {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported temporal filter: {reason}"))]
    UnsupportedTemporalFilter {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Datatypes error: {source} with extra message: {extra}"))]
    Datatypes {
        source: datatypes::Error,
        extra: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Datafusion error: {raw:?} in context: {context}"))]
    Datafusion {
        #[snafu(source)]
        raw: datafusion_common::DataFusionError,
        context: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected: {reason}"))]
    Unexpected {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to start server"))]
    StartServer {
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to shutdown server"))]
    ShutdownServer {
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to initialize meta client"))]
    MetaClientInit {
        #[snafu(implicit)]
        location: Location,
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to parse address {}", addr))]
    ParseAddr {
        addr: String,
        #[snafu(source)]
        error: std::net::AddrParseError,
    },

    #[snafu(display("Failed to get cache from cache registry: {}", name))]
    CacheRequired {
        #[snafu(implicit)]
        location: Location,
        name: String,
    },
}

/// the outer message is the full error stack, and inner message in header is the last error message that can be show directly to user
pub fn to_status_with_last_err(err: impl ErrorExt) -> tonic::Status {
    let msg = err.to_string();
    let last_err_msg = common_error::ext::StackError::last(&err).to_string();
    let code = err.status_code() as u32;
    let header = from_err_code_msg_to_header(
        code,
        HeaderValue::from_str(&last_err_msg).unwrap_or_else(|_| {
            HeaderValue::from_bytes(
                &last_err_msg
                    .as_bytes()
                    .iter()
                    .flat_map(|b| std::ascii::escape_default(*b))
                    .collect::<Vec<u8>>(),
            )
            .expect("Already escaped string should be valid ascii")
        }),
    );

    tonic::Status::with_metadata(
        tonic::Code::InvalidArgument,
        msg,
        MetadataMap::from_headers(header),
    )
}

/// Result type for flow module
pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Eval { .. } | Self::JoinTask { .. } | Self::Datafusion { .. } => {
                StatusCode::Internal
            }
            Self::FlowAlreadyExist { .. } => StatusCode::TableAlreadyExists,
            Self::TableNotFound { .. }
            | Self::TableNotFoundMeta { .. }
            | Self::FlowNotFound { .. }
            | Self::ListFlows { .. } => StatusCode::TableNotFound,
            Self::Plan { .. } | Self::Datatypes { .. } => StatusCode::PlanQuery,
            Self::InvalidQuery { .. } => StatusCode::EngineExecuteQuery,
            Self::Unexpected { .. } => StatusCode::Unexpected,
            Self::NotImplemented { .. } | Self::UnsupportedTemporalFilter { .. } => {
                StatusCode::Unsupported
            }
            Self::External { source, .. } => source.status_code(),
            Self::Internal { .. } | Self::CacheRequired { .. } => StatusCode::Internal,
            Self::StartServer { source, .. } | Self::ShutdownServer { source, .. } => {
                source.status_code()
            }
            Self::MetaClientInit { source, .. } => source.status_code(),
            Self::ParseAddr { .. } => StatusCode::InvalidArguments,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_into_tonic_status!(Error);
