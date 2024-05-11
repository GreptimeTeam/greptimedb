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
use common_macro::stack_trace_debug;
use common_telemetry::common_error::ext::ErrorExt;
use common_telemetry::common_error::status_code::StatusCode;
use datatypes::value::Value;
use servers::define_into_tonic_status;
use snafu::{Location, Snafu};

use crate::expr::EvalError;

/// This error is used to represent all possible errors that can occur in the flow module.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("External error"))]
    External {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Internal error"))]
    Internal { location: Location, reason: String },

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
        location: Location,
    },

    #[snafu(display("Table already exist: {name}"))]
    TableAlreadyExist {
        name: String,
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

    #[snafu(display("Invalid query plan: {source}"))]
    InvalidQueryPlan {
        source: query::error::Error,
        location: Location,
    },

    #[snafu(display("Invalid query: prost can't decode substrait plan: {inner}"))]
    InvalidQueryProst {
        inner: api::DecodeError,
        location: Location,
    },

    #[snafu(display("Invalid query, can't transform to substrait: {source}"))]
    InvalidQuerySubstrait {
        source: substrait::error::Error,
        location: Location,
    },

    #[snafu(display("Invalid query: {reason}"))]
    InvalidQuery {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No protobuf type for value: {value}"))]
    NoProtoType {
        value: Value,
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
        raw: datafusion_common::DataFusionError,
        context: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected: {reason}"))]
    Unexpected { reason: String, location: Location },
}

/// Result type for flow module
pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Eval { .. } | &Self::JoinTask { .. } | &Self::Datafusion { .. } => {
                StatusCode::Internal
            }
            &Self::TableAlreadyExist { .. } => StatusCode::TableAlreadyExists,
            Self::TableNotFound { .. } | Self::TableNotFoundMeta { .. } => {
                StatusCode::TableNotFound
            }
            Self::InvalidQueryPlan { .. }
            | Self::InvalidQuerySubstrait { .. }
            | Self::InvalidQueryProst { .. }
            | &Self::InvalidQuery { .. }
            | &Self::Plan { .. }
            | &Self::Datatypes { .. } => StatusCode::PlanQuery,
            Self::NoProtoType { .. } | Self::Unexpected { .. } => StatusCode::Unexpected,
            &Self::NotImplemented { .. } | Self::UnsupportedTemporalFilter { .. } => {
                StatusCode::Unsupported
            }
            &Self::External { .. } => StatusCode::Unknown,
            Self::Internal { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_into_tonic_status!(Error);
