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
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Missing internal column {} in physical metric table", column))]
    MissingInternalColumn { column: String, location: Location },

    #[snafu(display("Failed to create mito region, region type: {}", region_type))]
    CreateMitoRegion {
        region_type: String,
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Table `{}` already exists", table_name))]
    TableAlreadyExists {
        table_name: String,
        location: Location,
    },

    #[snafu(display("Failed to deserialize semantic type from {}", raw))]
    DeserializeSemanticType {
        raw: String,
        #[snafu(source)]
        error: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Failed to decode base64 column value"))]
    DecodeColumnValue {
        #[snafu(source)]
        error: base64::DecodeError,
        location: Location,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            MissingInternalColumn { .. }
            | DeserializeSemanticType { .. }
            | DecodeColumnValue { .. } => StatusCode::Unexpected,

            CreateMitoRegion { source, .. } => source.status_code(),

            TableAlreadyExists { .. } => StatusCode::TableAlreadyExists,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
