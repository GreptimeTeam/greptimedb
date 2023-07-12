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
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to parse string to date, raw: {}, source: {}", raw, source))]
    ParseDateStr { raw: String, source: ParseError },

    #[snafu(display("Invalid date string, raw: {}", raw))]
    InvalidDateStr { raw: String, location: Location },

    #[snafu(display("Failed to parse a string into Timestamp, raw string: {}", raw))]
    ParseTimestamp { raw: String, location: Location },

    #[snafu(display("Current timestamp overflow, source: {}", source))]
    TimestampOverflow {
        source: TryFromIntError,
        location: Location,
    },

    #[snafu(display("Timestamp arithmetic overflow, msg: {}", msg))]
    ArithmeticOverflow { msg: String, location: Location },

    #[snafu(display("Invalid time zone offset: {hours}:{minutes}"))]
    InvalidTimeZoneOffset {
        hours: i32,
        minutes: u32,
        location: Location,
    },

    #[snafu(display("Invalid offset string {raw}: {source}"))]
    ParseOffsetStr {
        raw: String,
        source: ParseIntError,
        location: Location,
    },

    #[snafu(display("Invalid time zone string {raw}"))]
    ParseTimeZoneName { raw: String, location: Location },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ParseDateStr { .. }
            | Error::ParseTimestamp { .. }
            | Error::InvalidTimeZoneOffset { .. }
            | Error::ParseOffsetStr { .. }
            | Error::ParseTimeZoneName { .. } => StatusCode::InvalidArguments,
            Error::TimestampOverflow { .. } => StatusCode::Internal,
            Error::InvalidDateStr { .. } | Error::ArithmeticOverflow { .. } => {
                StatusCode::InvalidArguments
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn location_opt(&self) -> Option<common_error::snafu::Location> {
        match self {
            Error::ParseTimestamp { location, .. }
            | Error::TimestampOverflow { location, .. }
            | Error::ArithmeticOverflow { location, .. } => Some(*location),
            Error::ParseDateStr { .. }
            | Error::InvalidTimeZoneOffset { .. }
            | Error::ParseOffsetStr { .. }
            | Error::ParseTimeZoneName { .. } => None,
            Error::InvalidDateStr { location, .. } => Some(*location),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
