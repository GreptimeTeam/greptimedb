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
    /// The task is cancelled.
    Cancelled = 1005,
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
    TableNotFound = 4001,
    TableColumnNotFound = 4002,
    TableColumnExists = 4003,
    DatabaseNotFound = 4004,
    RegionNotFound = 4005,
    RegionAlreadyExists = 4006,
    RegionReadonly = 4007,
    RegionNotReady = 4008,
    // If mutually exclusive operations are reached at the same time,
    // only one can be executed, another one will get region busy.
    RegionBusy = 4009,
    // ====== End of catalog related status code =======

    // ====== Begin of storage related status code =====
    /// Storage is temporarily unable to handle the request
    StorageUnavailable = 5000,
    /// Request is outdated, e.g., version mismatch
    RequestOutdated = 5001,
    // ====== End of storage related status code =======

    // ====== Begin of server related status code =====
    /// Runtime resources exhausted, like creating threads failed.
    RuntimeResourcesExhausted = 6000,

    /// Rate limit exceeded
    RateLimited = 6001,
    // ====== End of server related status code =======

    // ====== Begin of auth related status code =====
    /// User not exist
    UserNotFound = 7000,
    /// Unsupported password type
    UnsupportedPasswordType = 7001,
    /// Username and password does not match
    UserPasswordMismatch = 7002,
    /// Not found http authorization header
    AuthHeaderNotFound = 7003,
    /// Invalid http authorization header
    InvalidAuthHeader = 7004,
    /// Illegal request to connect catalog-schema
    AccessDenied = 7005,
    /// User is not authorized to perform the operation
    PermissionDenied = 7006,
    // ====== End of auth related status code =====
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
            | StatusCode::RegionBusy => true,

            StatusCode::Success
            | StatusCode::Unknown
            | StatusCode::Unsupported
            | StatusCode::Unexpected
            | StatusCode::InvalidArguments
            | StatusCode::Cancelled
            | StatusCode::InvalidSyntax
            | StatusCode::PlanQuery
            | StatusCode::EngineExecuteQuery
            | StatusCode::TableAlreadyExists
            | StatusCode::TableNotFound
            | StatusCode::RegionNotFound
            | StatusCode::RegionAlreadyExists
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
            | StatusCode::RequestOutdated => false,
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
            | StatusCode::PlanQuery
            | StatusCode::EngineExecuteQuery
            | StatusCode::StorageUnavailable
            | StatusCode::RuntimeResourcesExhausted => true,
            StatusCode::Success
            | StatusCode::Unsupported
            | StatusCode::InvalidArguments
            | StatusCode::InvalidSyntax
            | StatusCode::TableAlreadyExists
            | StatusCode::TableNotFound
            | StatusCode::RegionNotFound
            | StatusCode::RegionNotReady
            | StatusCode::RegionBusy
            | StatusCode::RegionAlreadyExists
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
