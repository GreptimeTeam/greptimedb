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
use std::io;

use common_error::prelude::{ErrorExt, StatusCode};
use snafu::{Backtrace, ErrorCompat, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid client tls config, {}", msg))]
    InvalidTlsConfig { msg: String },

    #[snafu(display("Invalid config file path, {}", source))]
    InvalidConfigFilePath {
        source: io::Error,
        location: Location,
    },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField { field: String, location: Location },

    #[snafu(display(
        "Write type mismatch, column name: {}, expected: {}, actual: {}",
        column_name,
        expected,
        actual
    ))]
    TypeMismatch {
        column_name: String,
        expected: String,
        actual: String,
        location: Location,
    },

    #[snafu(display("Failed to create gRPC channel, source: {}", source))]
    CreateChannel {
        source: tonic::transport::Error,
        location: Location,
    },

    #[snafu(display("Failed to create RecordBatch, source: {}", source))]
    CreateRecordBatch {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to convert Arrow type: {}", from))]
    Conversion { from: String, location: Location },

    #[snafu(display("Column datatype error, source: {}", source))]
    ColumnDataType {
        #[snafu(backtrace)]
        source: api::error::Error,
    },

    #[snafu(display("Failed to decode FlightData, source: {}", source))]
    DecodeFlightData {
        source: api::DecodeError,
        location: Location,
    },

    #[snafu(display("Invalid FlightData, reason: {}", reason))]
    InvalidFlightData { reason: String, location: Location },

    #[snafu(display("Failed to convert Arrow Schema, source: {}", source))]
    ConvertArrowSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidTlsConfig { .. }
            | Error::InvalidConfigFilePath { .. }
            | Error::MissingField { .. }
            | Error::TypeMismatch { .. }
            | Error::InvalidFlightData { .. } => StatusCode::InvalidArguments,

            Error::CreateChannel { .. }
            | Error::Conversion { .. }
            | Error::DecodeFlightData { .. } => StatusCode::Internal,

            Error::CreateRecordBatch { source } => source.status_code(),
            Error::ColumnDataType { source } => source.status_code(),
            Error::ConvertArrowSchema { source } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use snafu::{OptionExt, ResultExt};

    use super::*;

    type StdResult<E> = std::result::Result<(), E>;

    fn throw_none_option() -> Option<String> {
        None
    }

    #[test]
    fn test_missing_field_error() {
        let e = throw_none_option()
            .context(MissingFieldSnafu { field: "test" })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_type_mismatch_error() {
        let e = throw_none_option()
            .context(TypeMismatchSnafu {
                column_name: "",
                expected: "",
                actual: "",
            })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_create_channel_error() {
        fn throw_tonic_error() -> StdResult<tonic::transport::Error> {
            tonic::transport::Endpoint::new("http//http").map(|_| ())
        }

        let e = throw_tonic_error()
            .context(CreateChannelSnafu)
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }
}
