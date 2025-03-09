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

//! Errors for SST conversion.

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Object store error"))]
    ObjectStore {
        #[snafu(source)]
        error: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("I/O error"))]
    Io {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("JSON error"))]
    Json {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing __name__ label"))]
    MissingMetricName {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table not found: {}", table_name))]
    MissingTable {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Column not found: {}", column_name))]
    MissingColumn {
        column_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mito error"))]
    Mito {
        source: mito2::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ObjectStore { .. } => StatusCode::StorageUnavailable,
            Error::Io { .. } => StatusCode::StorageUnavailable,
            Error::Json { .. } => StatusCode::InvalidArguments,
            Error::MissingMetricName { .. } => StatusCode::InvalidArguments,
            Error::MissingTable { .. } => StatusCode::TableNotFound,
            Error::MissingColumn { .. } => StatusCode::TableColumnNotFound,
            Error::Mito { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
