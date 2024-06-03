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

use std::any::Any;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datafusion::error::DataFusionError;
use prost::{DecodeError, EncodeError};
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to decode substrait relation"))]
    DecodeRel {
        #[snafu(source)]
        error: DecodeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to encode substrait relation"))]
    EncodeRel {
        #[snafu(source)]
        error: EncodeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Internal error from DataFusion"))]
    DFInternal {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Internal error"))]
    Internal {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Cannot convert plan doesn't belong to GreptimeDB"))]
    UnknownPlan {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to encode DataFusion plan"))]
    EncodeDfPlan {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode DataFusion plan"))]
    DecodeDfPlan {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::UnknownPlan { .. } | Error::EncodeRel { .. } | Error::DecodeRel { .. } => {
                StatusCode::InvalidArguments
            }
            Error::DFInternal { .. }
            | Error::Internal { .. }
            | Error::EncodeDfPlan { .. }
            | Error::DecodeDfPlan { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
