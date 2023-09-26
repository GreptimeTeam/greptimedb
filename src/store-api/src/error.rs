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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

use crate::storage::ColumnDescriptorBuilderError;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Invalid raw region request: {err}"))]
    InvalidRawRegionRequest { err: String, location: Location },

    #[snafu(display("Invalid default constraint: {constraint}"))]
    InvalidDefaultConstraint {
        constraint: String,
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to build column descriptor: "))]
    BuildColumnDescriptor {
        #[snafu(source)]
        error: ColumnDescriptorBuilderError,
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidRawRegionRequest { .. } => StatusCode::InvalidArguments,
            Error::InvalidDefaultConstraint { source, .. } => source.status_code(),
            Error::BuildColumnDescriptor { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
