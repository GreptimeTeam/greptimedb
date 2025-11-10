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

use std::fmt::Debug;
use std::sync::Arc;

use api::v1::greptime_request::Request;
use common_telemetry::debug;
use sql::statements::statement::Statement;

use crate::error::{PermissionDeniedSnafu, Result};
use crate::user_info::DefaultUserInfo;
use crate::{PermissionCheckerRef, UserInfo, UserInfoRef};

#[derive(Debug, Clone)]
pub enum PermissionReq<'a> {
    GrpcRequest(&'a Request),
    SqlStatement(&'a Statement),
    PromQuery,
    LogQuery,
    Opentsdb,
    LineProtocol,
    PromStoreWrite,
    PromStoreRead,
    Otlp,
    LogWrite,
    BulkInsert,
}

impl<'a> PermissionReq<'a> {
    /// Returns true if the permission request is for read operations.
    pub fn is_readonly(&self) -> bool {
        match self {
            PermissionReq::GrpcRequest(Request::Query(_))
            | PermissionReq::PromQuery
            | PermissionReq::LogQuery
            | PermissionReq::PromStoreRead => true,
            PermissionReq::SqlStatement(stmt) => stmt.is_readonly(),

            PermissionReq::GrpcRequest(_)
            | PermissionReq::Opentsdb
            | PermissionReq::LineProtocol
            | PermissionReq::PromStoreWrite
            | PermissionReq::Otlp
            | PermissionReq::LogWrite
            | PermissionReq::BulkInsert => false,
        }
    }

    /// Returns true if the permission request is for write operations.
    pub fn is_write(&self) -> bool {
        !self.is_readonly()
    }
}

#[derive(Debug)]
pub enum PermissionResp {
    Allow,
    Reject,
}

pub trait PermissionChecker: Send + Sync {
    fn check_permission(
        &self,
        user_info: UserInfoRef,
        req: PermissionReq,
    ) -> Result<PermissionResp>;
}

impl PermissionChecker for Option<&PermissionCheckerRef> {
    fn check_permission(
        &self,
        user_info: UserInfoRef,
        req: PermissionReq,
    ) -> Result<PermissionResp> {
        match self {
            Some(checker) => match checker.check_permission(user_info, req) {
                Ok(PermissionResp::Reject) => PermissionDeniedSnafu.fail(),
                Ok(PermissionResp::Allow) => Ok(PermissionResp::Allow),
                Err(e) => Err(e),
            },
            None => Ok(PermissionResp::Allow),
        }
    }
}

/// The default permission checker implementation.
/// It checks the permission mode of [DefaultUserInfo].
pub struct DefaultPermissionChecker;

impl DefaultPermissionChecker {
    /// Returns a new [PermissionCheckerRef] instance.
    pub fn arc() -> PermissionCheckerRef {
        Arc::new(DefaultPermissionChecker)
    }
}

impl PermissionChecker for DefaultPermissionChecker {
    fn check_permission(
        &self,
        user_info: UserInfoRef,
        req: PermissionReq,
    ) -> Result<PermissionResp> {
        if let Some(default_user) = user_info.as_any().downcast_ref::<DefaultUserInfo>() {
            let permission_mode = default_user.permission_mode();

            if req.is_readonly() && !permission_mode.can_read() {
                debug!(
                    "Permission denied: read operation not allowed, user = {}, permission = {}",
                    default_user.username(),
                    permission_mode.as_str()
                );
                return Ok(PermissionResp::Reject);
            }

            if req.is_write() && !permission_mode.can_write() {
                debug!(
                    "Permission denied: write operation not allowed, user = {}, permission = {}",
                    default_user.username(),
                    permission_mode.as_str()
                );
                return Ok(PermissionResp::Reject);
            }
        }

        // default allow all
        Ok(PermissionResp::Allow)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::user_info::PermissionMode;

    #[test]
    fn test_default_permission_checker_allow_all_operations() {
        let checker = DefaultPermissionChecker;
        let user_info =
            DefaultUserInfo::with_name_and_permission("test_user", PermissionMode::ReadWrite);

        let read_req = PermissionReq::PromQuery;
        let write_req = PermissionReq::PromStoreWrite;

        let read_result = checker
            .check_permission(user_info.clone(), read_req)
            .unwrap();
        let write_result = checker.check_permission(user_info, write_req).unwrap();

        assert!(matches!(read_result, PermissionResp::Allow));
        assert!(matches!(write_result, PermissionResp::Allow));
    }

    #[test]
    fn test_default_permission_checker_readonly_user() {
        let checker = DefaultPermissionChecker;
        let user_info =
            DefaultUserInfo::with_name_and_permission("readonly_user", PermissionMode::ReadOnly);

        let read_req = PermissionReq::PromQuery;
        let write_req = PermissionReq::PromStoreWrite;

        let read_result = checker
            .check_permission(user_info.clone(), read_req)
            .unwrap();
        let write_result = checker.check_permission(user_info, write_req).unwrap();

        assert!(matches!(read_result, PermissionResp::Allow));
        assert!(matches!(write_result, PermissionResp::Reject));
    }

    #[test]
    fn test_default_permission_checker_writeonly_user() {
        let checker = DefaultPermissionChecker;
        let user_info =
            DefaultUserInfo::with_name_and_permission("writeonly_user", PermissionMode::WriteOnly);

        let read_req = PermissionReq::LogQuery;
        let write_req = PermissionReq::LogWrite;

        let read_result = checker
            .check_permission(user_info.clone(), read_req)
            .unwrap();
        let write_result = checker.check_permission(user_info, write_req).unwrap();

        assert!(matches!(read_result, PermissionResp::Reject));
        assert!(matches!(write_result, PermissionResp::Allow));
    }
}
