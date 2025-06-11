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

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use tonic::Status;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("External error"))]
    External {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to send gRPC request"))]
    Grpc {
        source: client::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to list nodes from metasrv"))]
    Meta {
        source: meta_client::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl From<Status> for Error {
    fn from(value: Status) -> Self {
        Self::Grpc {
            source: client::error::Error::from(value),
            location: Location::default(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            External { source, .. } => source.status_code(),
            Grpc { source, .. } => source.status_code(),
            Meta { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
