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
use std::num::{ParseIntError, TryFromIntError};

use chrono::ParseError;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to parse string to date, raw: {}", raw))]
    ParseDateStr {
        raw: String,
        #[snafu(source)]
        error: ParseError,
    },

    #[snafu(display("Invalid date string, raw: {}", raw))]
    InvalidDateStr {
        raw: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse a string into Timestamp, raw string: {}", raw))]
    ParseTimestamp {
        raw: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Current timestamp overflow"))]
    TimestampOverflow {
        #[snafu(source)]
        error: TryFromIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Timestamp arithmetic overflow, msg: {}", msg))]
    ArithmeticOverflow {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid timezone offset: {hours}:{minutes}"))]
    InvalidTimezoneOffset {
        hours: i32,
        minutes: u32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid offset string {raw}: "))]
    ParseOffsetStr {
        raw: String,
        #[snafu(source)]
        error: ParseIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid timezone string {raw}"))]
    ParseTimezoneName {
        raw: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to format, pattern: {}", pattern))]
    Format {
        pattern: String,
        #[snafu(source)]
        error: std::fmt::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse duration"))]
    ParseDuration {
        #[snafu(source)]
        error: humantime::DurationError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Database's TTL can't be `instant`"))]
    InvalidDatabaseTtl {
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ParseDateStr { .. }
            | Error::ParseDuration { .. }
            | Error::InvalidDatabaseTtl { .. }
            | Error::ParseTimestamp { .. }
            | Error::InvalidTimezoneOffset { .. }
            | Error::Format { .. }
            | Error::ParseOffsetStr { .. }
            | Error::ParseTimezoneName { .. } => StatusCode::InvalidArguments,
            Error::TimestampOverflow { .. } => StatusCode::Internal,
            Error::InvalidDateStr { .. } | Error::ArithmeticOverflow { .. } => {
                StatusCode::InvalidArguments
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
