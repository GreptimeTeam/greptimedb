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

use bigdecimal::BigDecimal;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Decimal out of range, decimal value: {}", value))]
    BigDecimalOutOfRange {
        value: BigDecimal,
        location: Location,
    },

    #[snafu(display("Failed to parse string to rust decimal, raw: {}", raw))]
    ParseRustDecimalStr {
        raw: String,
        #[snafu(source)]
        error: rust_decimal::Error,
    },

    #[snafu(display("Failed to parse string to big decimal, raw: {}", raw))]
    ParseBigDecimalStr {
        raw: String,
        #[snafu(source)]
        error: bigdecimal::ParseBigDecimalError,
    },

    #[snafu(display("Invalid precision or scale, resion: {}", reason))]
    InvalidPrecisionOrScale { reason: String, location: Location },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::BigDecimalOutOfRange { .. } => StatusCode::Internal,
            Error::ParseRustDecimalStr { .. }
            | Error::InvalidPrecisionOrScale { .. }
            | Error::ParseBigDecimalStr { .. } => StatusCode::InvalidArguments,
        }
    }

    fn location_opt(&self) -> Option<common_error::snafu::Location> {
        match self {
            Error::BigDecimalOutOfRange { location, .. } => Some(*location),
            Error::InvalidPrecisionOrScale { location, .. } => Some(*location),
            Error::ParseRustDecimalStr { .. } | Error::ParseBigDecimalStr { .. } => None,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
