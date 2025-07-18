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

use std::fmt;

use strum::{AsRefStr, EnumIter, EnumString, FromRepr};
use tonic::Code;

/// Common status code for public API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, AsRefStr, EnumIter, FromRepr)]
pub enum StatusCode {
    // ====== Begin of common status code ==============
    /// Success.
    Success = 0,

    /// Unknown error.
    Unknown = 1000,
    /// Unsupported operation.
    Unsupported = 1001,
    /// Unexpected error, maybe there is a BUG.
    Unexpected = 1002,
    /// Internal server error.
    Internal = 1003,
    /// Invalid arguments.
    InvalidArguments = 1004,
    /// The task is cancelled (typically caller-side).
    Cancelled = 1005,
    /// Illegal state, can be exposed to users.
    IllegalState = 1006,
    /// Caused by some error originated from external system.
    External = 1007,
    /// The request is deadline exceeded (typically server-side).
    DeadlineExceeded = 1008,
    // ====== End of common status code ================

    // ====== Begin of SQL related status code =========
    /// SQL Syntax error.
    InvalidSyntax = 2000,
    // ====== End of SQL related status code ===========

    // ====== Begin of query related status code =======
    /// Fail to create a plan for the query.
    PlanQuery = 3000,
    /// The query engine fail to execute query.
    EngineExecuteQuery = 3001,
    // ====== End of query related status code =========

    // ====== Begin of catalog related status code =====
    /// Table already exists.
    TableAlreadyExists = 4000,
    /// Table not found.
    TableNotFound = 4001,
    /// Table column not found.
    TableColumnNotFound = 4002,
    /// Table column already exists.
    TableColumnExists = 4003,
    /// Database not found.
    DatabaseNotFound = 4004,
    /// Region not found.
    RegionNotFound = 4005,
    /// Region already exists.
    RegionAlreadyExists = 4006,
    /// Region is read-only in current state.
    RegionReadonly = 4007,
    /// Region is not in a proper state to handle specific request.
    RegionNotReady = 4008,
    /// Region is temporarily in busy state.
    RegionBusy = 4009,
    /// Table is temporarily unable to handle the request.
    TableUnavailable = 4010,
    /// Database already exists.
    DatabaseAlreadyExists = 4011,
    // ====== End of catalog related status code =======

    // ====== Begin of storage related status code =====
    /// Storage is temporarily unable to handle the request.
    StorageUnavailable = 5000,
    /// Request is outdated, e.g., version mismatch.
    RequestOutdated = 5001,
    // ====== End of storage related status code =======

    // ====== Begin of server related status code =====
    /// Runtime resources exhausted, like creating threads failed.
    RuntimeResourcesExhausted = 6000,

    /// Rate limit exceeded.
    RateLimited = 6001,
    // ====== End of server related status code =======

    // ====== Begin of auth related status code =====
    /// User not exist.
    UserNotFound = 7000,
    /// Unsupported password type.
    UnsupportedPasswordType = 7001,
    /// Username and password does not match.
    UserPasswordMismatch = 7002,
    /// Not found http authorization header.
    AuthHeaderNotFound = 7003,
    /// Invalid http authorization header.
    InvalidAuthHeader = 7004,
    /// Illegal request to connect catalog-schema.
    AccessDenied = 7005,
    /// User is not authorized to perform the operation.
    PermissionDenied = 7006,
    // ====== End of auth related status code =====

    // ====== Begin of flow related status code =====
    FlowAlreadyExists = 8000,
    FlowNotFound = 8001,
    // ====== End of flow related status code =====

    // ====== Begin of trigger related status code =====
    TriggerAlreadyExists = 9000,
    TriggerNotFound = 9001,
    // ====== End of trigger related status code =====
}

impl StatusCode {
    /// Returns `true` if `code` is success.
    pub fn is_success(code: u32) -> bool {
        Self::Success as u32 == code
    }

    /// Returns `true` if the error with this code is retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            StatusCode::StorageUnavailable
            | StatusCode::RuntimeResourcesExhausted
            | StatusCode::Internal
            | StatusCode::RegionNotReady
            | StatusCode::TableUnavailable
            | StatusCode::RegionBusy => true,

            StatusCode::Success
            | StatusCode::Unknown
            | StatusCode::Unsupported
            | StatusCode::IllegalState
            | StatusCode::Unexpected
            | StatusCode::InvalidArguments
            | StatusCode::Cancelled
            | StatusCode::DeadlineExceeded
            | StatusCode::InvalidSyntax
            | StatusCode::DatabaseAlreadyExists
            | StatusCode::PlanQuery
            | StatusCode::EngineExecuteQuery
            | StatusCode::TableAlreadyExists
            | StatusCode::TableNotFound
            | StatusCode::RegionAlreadyExists
            | StatusCode::RegionNotFound
            | StatusCode::FlowAlreadyExists
            | StatusCode::FlowNotFound
            | StatusCode::TriggerAlreadyExists
            | StatusCode::TriggerNotFound
            | StatusCode::RegionReadonly
            | StatusCode::TableColumnNotFound
            | StatusCode::TableColumnExists
            | StatusCode::DatabaseNotFound
            | StatusCode::RateLimited
            | StatusCode::UserNotFound
            | StatusCode::UnsupportedPasswordType
            | StatusCode::UserPasswordMismatch
            | StatusCode::AuthHeaderNotFound
            | StatusCode::InvalidAuthHeader
            | StatusCode::AccessDenied
            | StatusCode::PermissionDenied
            | StatusCode::RequestOutdated
            | StatusCode::External => false,
        }
    }

    /// Returns `true` if we should print an error log for an error with
    /// this status code.
    pub fn should_log_error(&self) -> bool {
        match self {
            StatusCode::Unknown
            | StatusCode::Unexpected
            | StatusCode::Internal
            | StatusCode::Cancelled
            | StatusCode::DeadlineExceeded
            | StatusCode::IllegalState
            | StatusCode::EngineExecuteQuery
            | StatusCode::StorageUnavailable
            | StatusCode::RuntimeResourcesExhausted
            | StatusCode::External => true,

            StatusCode::Success
            | StatusCode::Unsupported
            | StatusCode::InvalidArguments
            | StatusCode::InvalidSyntax
            | StatusCode::TableAlreadyExists
            | StatusCode::TableNotFound
            | StatusCode::RegionAlreadyExists
            | StatusCode::RegionNotFound
            | StatusCode::PlanQuery
            | StatusCode::FlowAlreadyExists
            | StatusCode::FlowNotFound
            | StatusCode::TriggerAlreadyExists
            | StatusCode::TriggerNotFound
            | StatusCode::RegionNotReady
            | StatusCode::RegionBusy
            | StatusCode::RegionReadonly
            | StatusCode::TableColumnNotFound
            | StatusCode::TableColumnExists
            | StatusCode::DatabaseNotFound
            | StatusCode::RateLimited
            | StatusCode::UserNotFound
            | StatusCode::TableUnavailable
            | StatusCode::DatabaseAlreadyExists
            | StatusCode::UnsupportedPasswordType
            | StatusCode::UserPasswordMismatch
            | StatusCode::AuthHeaderNotFound
            | StatusCode::InvalidAuthHeader
            | StatusCode::AccessDenied
            | StatusCode::PermissionDenied
            | StatusCode::RequestOutdated => false,
        }
    }

    pub fn from_u32(value: u32) -> Option<Self> {
        StatusCode::from_repr(value as usize)
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The current debug format is suitable to display.
        write!(f, "{self:?}")
    }
}

#[macro_export]
macro_rules! define_from_tonic_status {
    ($Error: ty, $Variant: ident) => {
        impl From<tonic::Status> for $Error {
            fn from(e: tonic::Status) -> Self {
                use snafu::location;

                fn metadata_value(e: &tonic::Status, key: &str) -> Option<String> {
                    e.metadata()
                        .get(key)
                        .and_then(|v| String::from_utf8(v.as_bytes().to_vec()).ok())
                }

                let code = metadata_value(&e, $crate::GREPTIME_DB_HEADER_ERROR_CODE)
                    .and_then(|s| {
                        if let Ok(code) = s.parse::<u32>() {
                            StatusCode::from_u32(code)
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| match e.code() {
                        tonic::Code::Cancelled => StatusCode::Cancelled,
                        tonic::Code::DeadlineExceeded => StatusCode::DeadlineExceeded,
                        _ => StatusCode::Internal,
                    });

                let msg = metadata_value(&e, $crate::GREPTIME_DB_HEADER_ERROR_MSG)
                    .unwrap_or_else(|| e.message().to_string());

                // TODO(LFC): Make the error variant defined automatically.
                Self::$Variant {
                    code,
                    msg,
                    tonic_code: e.code(),
                    location: location!(),
                }
            }
        }
    };
}

#[macro_export]
macro_rules! define_into_tonic_status {
    ($Error: ty) => {
        impl From<$Error> for tonic::Status {
            fn from(err: $Error) -> Self {
                use tonic::codegen::http::{HeaderMap, HeaderValue};
                use tonic::metadata::MetadataMap;
                use $crate::GREPTIME_DB_HEADER_ERROR_CODE;

                let mut headers = HeaderMap::<HeaderValue>::with_capacity(2);

                // If either of the status_code or error msg cannot convert to valid HTTP header value
                // (which is a very rare case), just ignore. Client will use Tonic status code and message.
                let status_code = err.status_code();
                headers.insert(
                    GREPTIME_DB_HEADER_ERROR_CODE,
                    HeaderValue::from(status_code as u32),
                );
                let root_error = err.output_msg();

                let metadata = MetadataMap::from_headers(headers);
                tonic::Status::with_metadata(
                    $crate::status_code::status_to_tonic_code(status_code),
                    root_error,
                    metadata,
                )
            }
        }
    };
}

/// Returns the tonic [Code] of a [StatusCode].
pub fn status_to_tonic_code(status_code: StatusCode) -> Code {
    match status_code {
        StatusCode::Success => Code::Ok,
        StatusCode::Unknown | StatusCode::External => Code::Unknown,
        StatusCode::Unsupported => Code::Unimplemented,
        StatusCode::Unexpected
        | StatusCode::IllegalState
        | StatusCode::Internal
        | StatusCode::PlanQuery
        | StatusCode::EngineExecuteQuery => Code::Internal,
        StatusCode::InvalidArguments | StatusCode::InvalidSyntax | StatusCode::RequestOutdated => {
            Code::InvalidArgument
        }
        StatusCode::Cancelled => Code::Cancelled,
        StatusCode::DeadlineExceeded => Code::DeadlineExceeded,
        StatusCode::TableAlreadyExists
        | StatusCode::TableColumnExists
        | StatusCode::RegionAlreadyExists
        | StatusCode::DatabaseAlreadyExists
        | StatusCode::TriggerAlreadyExists
        | StatusCode::FlowAlreadyExists => Code::AlreadyExists,
        StatusCode::TableNotFound
        | StatusCode::RegionNotFound
        | StatusCode::TableColumnNotFound
        | StatusCode::DatabaseNotFound
        | StatusCode::UserNotFound
        | StatusCode::TriggerNotFound
        | StatusCode::FlowNotFound => Code::NotFound,
        StatusCode::TableUnavailable
        | StatusCode::StorageUnavailable
        | StatusCode::RegionNotReady => Code::Unavailable,
        StatusCode::RuntimeResourcesExhausted
        | StatusCode::RateLimited
        | StatusCode::RegionBusy => Code::ResourceExhausted,
        StatusCode::UnsupportedPasswordType
        | StatusCode::UserPasswordMismatch
        | StatusCode::AuthHeaderNotFound
        | StatusCode::InvalidAuthHeader => Code::Unauthenticated,
        StatusCode::AccessDenied | StatusCode::PermissionDenied | StatusCode::RegionReadonly => {
            Code::PermissionDenied
        }
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;

    fn assert_status_code_display(code: StatusCode, msg: &str) {
        let code_msg = format!("{code}");
        assert_eq!(msg, code_msg);
    }

    #[test]
    fn test_display_status_code() {
        assert_status_code_display(StatusCode::Unknown, "Unknown");
        assert_status_code_display(StatusCode::TableAlreadyExists, "TableAlreadyExists");
    }

    #[test]
    fn test_from_u32() {
        for code in StatusCode::iter() {
            let num = code as u32;
            assert_eq!(StatusCode::from_u32(num), Some(code));
        }

        assert_eq!(StatusCode::from_u32(10000), None);
    }

    #[test]
    fn test_is_success() {
        assert!(StatusCode::is_success(0));
        assert!(!StatusCode::is_success(1));
        assert!(!StatusCode::is_success(2));
        assert!(!StatusCode::is_success(3));
    }
}
