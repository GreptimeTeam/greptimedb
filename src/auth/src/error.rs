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

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Invalid config value: {}, {}", value, msg))]
    InvalidConfig { value: String, msg: String },

    #[snafu(display("Illegal param: {}", msg))]
    IllegalParam { msg: String },

    #[snafu(display("Internal state error: {}", msg))]
    InternalState { msg: String },

    #[snafu(display("IO error"))]
    Io {
        #[snafu(source)]
        error: std::io::Error,
        location: Location,
    },

    #[snafu(display("Auth failed"))]
    AuthBackend {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("User not found, username: {}", username))]
    UserNotFound { username: String },

    #[snafu(display("Unsupported password type: {}", password_type))]
    UnsupportedPasswordType { password_type: String },

    #[snafu(display("Username and password does not match, username: {}", username))]
    UserPasswordMismatch { username: String },

    #[snafu(display(
        "Access denied for user '{}' to database '{}-{}'",
        username,
        catalog,
        schema
    ))]
    AccessDenied {
        catalog: String,
        schema: String,
        username: String,
    },

    #[snafu(display("User is not authorized to perform this action"))]
    PermissionDenied { location: Location },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidConfig { .. } => StatusCode::InvalidArguments,
            Error::IllegalParam { .. } => StatusCode::InvalidArguments,
            Error::InternalState { .. } => StatusCode::Unexpected,
            Error::Io { .. } => StatusCode::Internal,
            Error::AuthBackend { .. } => StatusCode::Internal,

            Error::UserNotFound { .. } => StatusCode::UserNotFound,
            Error::UnsupportedPasswordType { .. } => StatusCode::UnsupportedPasswordType,
            Error::UserPasswordMismatch { .. } => StatusCode::UserPasswordMismatch,
            Error::AccessDenied { .. } => StatusCode::AccessDenied,
            Error::PermissionDenied { .. } => StatusCode::PermissionDenied,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
