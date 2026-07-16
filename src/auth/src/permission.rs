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

use api::v1::RowInsertRequests;
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use common_telemetry::debug;
use sql::statements::statement::Statement;

use crate::error::{PermissionDeniedSnafu, Result};
use crate::user_info::DefaultUserInfo;
use crate::{PermissionCheckerRef, UserInfo, UserInfoRef};

/// A user-visible table target for permission checks.
///
/// Use [`PermissionTableTargets::resolved`] to validate that all components are
/// non-empty before passing targets to a permission checker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionTableTarget {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl PermissionTableTarget {
    /// Creates a table target candidate.
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }
}

/// The result of resolving every table target in a permission request.
///
/// `Resolved(vec![])` means the request contains no table targets, while
/// [`PermissionTableTargets::Unresolved`] means its targets could not be
/// determined safely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionTableTargets {
    /// Every target was resolved, including the valid empty-target case.
    Resolved(Vec<PermissionTableTarget>),
    /// One or more targets could not be determined safely.
    Unresolved,
}

impl PermissionTableTargets {
    /// Marks targets as resolved only when every component is non-empty.
    pub fn resolved(targets: Vec<PermissionTableTarget>) -> Self {
        if targets.iter().any(|target| {
            target.catalog.is_empty() || target.schema.is_empty() || target.table.is_empty()
        }) {
            Self::Unresolved
        } else {
            Self::Resolved(targets)
        }
    }

    /// Resolves logical table targets from normalized row insert requests.
    pub fn from_row_insert_requests(
        catalog: &str,
        schema: &str,
        requests: &RowInsertRequests,
    ) -> Self {
        Self::resolved(
            requests
                .inserts
                .iter()
                .map(|request| PermissionTableTarget::new(catalog, schema, &request.table_name))
                .collect(),
        )
    }
}

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
    BulkInsert {
        catalog: &'a str,
        schema: &'a str,
        table: &'a str,
    },
}

impl<'a> PermissionReq<'a> {
    /// Returns true if the permission request is for read operations.
    pub fn is_readonly(&self) -> bool {
        match self {
            PermissionReq::GrpcRequest(Request::Query(query_request)) => {
                !matches!(query_request.query, Some(Query::InsertIntoPlan(_)))
            }
            PermissionReq::PromQuery | PermissionReq::LogQuery | PermissionReq::PromStoreRead => {
                true
            }
            PermissionReq::SqlStatement(stmt) => stmt.is_readonly(),

            PermissionReq::GrpcRequest(_)
            | PermissionReq::Opentsdb
            | PermissionReq::LineProtocol
            | PermissionReq::PromStoreWrite
            | PermissionReq::Otlp
            | PermissionReq::LogWrite
            | PermissionReq::BulkInsert { .. } => false,
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

    fn check_permission_with_context(
        &self,
        user_info: UserInfoRef,
        req: PermissionReq,
        _current_schema: Option<&str>,
    ) -> Result<PermissionResp> {
        self.check_permission(user_info, req)
    }

    /// Checks the operation privilege and its resolved user-visible table targets.
    ///
    /// ACL-aware implementations must apply any admin bypass first, then reject
    /// [`PermissionTableTargets::Unresolved`] for non-admin users.
    fn check_permission_with_table_targets(
        &self,
        user_info: UserInfoRef,
        req: PermissionReq,
        targets: PermissionTableTargets,
    ) -> Result<PermissionResp>;
}

fn check_permission_result(result: Result<PermissionResp>) -> Result<PermissionResp> {
    match result {
        Ok(PermissionResp::Reject) => PermissionDeniedSnafu.fail(),
        Ok(PermissionResp::Allow) => Ok(PermissionResp::Allow),
        Err(e) => Err(e),
    }
}

impl PermissionChecker for Option<&PermissionCheckerRef> {
    fn check_permission(
        &self,
        user_info: UserInfoRef,
        req: PermissionReq,
    ) -> Result<PermissionResp> {
        self.check_permission_with_context(user_info, req, None)
    }

    fn check_permission_with_context(
        &self,
        user_info: UserInfoRef,
        req: PermissionReq,
        current_schema: Option<&str>,
    ) -> Result<PermissionResp> {
        match self {
            Some(checker) => check_permission_result(checker.check_permission_with_context(
                user_info,
                req,
                current_schema,
            )),
            None => Ok(PermissionResp::Allow),
        }
    }

    fn check_permission_with_table_targets(
        &self,
        user_info: UserInfoRef,
        req: PermissionReq,
        targets: PermissionTableTargets,
    ) -> Result<PermissionResp> {
        match self {
            Some(checker) => check_permission_result(
                checker.check_permission_with_table_targets(user_info, req, targets),
            ),
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

    fn check_permission_with_table_targets(
        &self,
        user_info: UserInfoRef,
        req: PermissionReq,
        _targets: PermissionTableTargets,
    ) -> Result<PermissionResp> {
        self.check_permission(user_info, req)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, InternalStateSnafu};
    use crate::user_info::PermissionMode;

    struct TargetAwarePermissionChecker;

    impl PermissionChecker for TargetAwarePermissionChecker {
        fn check_permission(
            &self,
            _user_info: UserInfoRef,
            _req: PermissionReq,
        ) -> Result<PermissionResp> {
            Ok(PermissionResp::Reject)
        }

        fn check_permission_with_table_targets(
            &self,
            _user_info: UserInfoRef,
            _req: PermissionReq,
            targets: PermissionTableTargets,
        ) -> Result<PermissionResp> {
            let PermissionTableTargets::Resolved(targets) = targets else {
                return Ok(PermissionResp::Reject);
            };
            if targets.iter().any(|target| target.table == "error") {
                return InternalStateSnafu {
                    msg: "testing".to_string(),
                }
                .fail();
            }
            Ok(
                if !targets.is_empty() && targets.iter().all(|target| target.table == "allowed") {
                    PermissionResp::Allow
                } else {
                    PermissionResp::Reject
                },
            )
        }
    }

    fn resolved_targets(table: &str) -> PermissionTableTargets {
        PermissionTableTargets::resolved(vec![PermissionTableTarget::new(
            "greptime", "public", table,
        )])
    }

    #[test]
    fn test_resolve_permission_table_targets() {
        assert_eq!(
            PermissionTableTargets::Resolved(Vec::new()),
            PermissionTableTargets::resolved(Vec::new())
        );
        assert_eq!(
            resolved_targets("metrics"),
            PermissionTableTargets::Resolved(vec![PermissionTableTarget::new(
                "greptime", "public", "metrics"
            )])
        );

        for target in [
            PermissionTableTarget::new("", "public", "metrics"),
            PermissionTableTarget::new("greptime", "", "metrics"),
            PermissionTableTarget::new("greptime", "public", ""),
        ] {
            assert_eq!(
                PermissionTableTargets::Unresolved,
                PermissionTableTargets::resolved(vec![target])
            );
        }
    }

    #[test]
    fn test_resolve_row_insert_request_targets() {
        let requests = RowInsertRequests {
            inserts: ["cpu", "mem"]
                .into_iter()
                .map(|table_name| api::v1::RowInsertRequest {
                    table_name: table_name.to_string(),
                    ..Default::default()
                })
                .collect(),
        };

        assert_eq!(
            PermissionTableTargets::Resolved(vec![
                PermissionTableTarget::new("greptime", "public", "cpu"),
                PermissionTableTarget::new("greptime", "public", "mem"),
            ]),
            PermissionTableTargets::from_row_insert_requests("greptime", "public", &requests)
        );
        assert_eq!(
            PermissionTableTargets::Unresolved,
            PermissionTableTargets::from_row_insert_requests("", "public", &requests)
        );
        assert_eq!(
            PermissionTableTargets::Unresolved,
            PermissionTableTargets::from_row_insert_requests("greptime", "", &requests)
        );

        let unresolved = RowInsertRequests {
            inserts: vec![api::v1::RowInsertRequest::default()],
        };
        assert_eq!(
            PermissionTableTargets::Unresolved,
            PermissionTableTargets::from_row_insert_requests("greptime", "public", &unresolved)
        );

        let empty = RowInsertRequests::default();
        assert_eq!(
            PermissionTableTargets::Resolved(Vec::new()),
            PermissionTableTargets::from_row_insert_requests("greptime", "public", &empty)
        );
    }

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

    #[test]
    fn test_grpc_insert_into_plan_is_write_request() {
        let request = Request::Query(api::v1::QueryRequest {
            query: Some(Query::InsertIntoPlan(api::v1::InsertIntoPlan::default())),
        });
        let req = PermissionReq::GrpcRequest(&request);

        assert!(req.is_write());
    }

    #[test]
    fn test_bulk_insert_is_write_request() {
        let req = PermissionReq::BulkInsert {
            catalog: "greptime",
            schema: "public",
            table: "metrics",
        };

        assert!(req.is_write());
    }

    #[test]
    fn test_table_target_permission_forwarding() {
        let checker: PermissionCheckerRef = Arc::new(TargetAwarePermissionChecker);
        let checker = Some(&checker);

        let allowed = checker
            .check_permission_with_table_targets(
                crate::userinfo_by_name(None),
                PermissionReq::PromStoreRead,
                resolved_targets("allowed"),
            )
            .unwrap();
        assert!(matches!(allowed, PermissionResp::Allow));

        let rejected = checker.check_permission_with_table_targets(
            crate::userinfo_by_name(None),
            PermissionReq::PromStoreRead,
            resolved_targets("denied"),
        );
        assert!(matches!(rejected, Err(Error::PermissionDenied { .. })));

        let mixed = checker.check_permission_with_table_targets(
            crate::userinfo_by_name(None),
            PermissionReq::PromStoreRead,
            PermissionTableTargets::resolved(vec![
                PermissionTableTarget::new("greptime", "public", "allowed"),
                PermissionTableTarget::new("greptime", "public", "denied"),
            ]),
        );
        assert!(matches!(mixed, Err(Error::PermissionDenied { .. })));

        let error = checker.check_permission_with_table_targets(
            crate::userinfo_by_name(None),
            PermissionReq::PromStoreRead,
            resolved_targets("error"),
        );
        assert!(matches!(error, Err(Error::InternalState { msg }) if msg == "testing"));

        let no_checker: Option<&PermissionCheckerRef> = None;
        let allowed = no_checker
            .check_permission_with_table_targets(
                crate::userinfo_by_name(None),
                PermissionReq::PromStoreRead,
                PermissionTableTargets::Unresolved,
            )
            .unwrap();
        assert!(matches!(allowed, PermissionResp::Allow));
    }

    #[test]
    fn test_default_permission_checker_table_target_parity() {
        let checker = DefaultPermissionChecker;

        for (permission, req) in [
            (PermissionMode::ReadOnly, PermissionReq::PromQuery),
            (PermissionMode::ReadOnly, PermissionReq::PromStoreWrite),
            (PermissionMode::WriteOnly, PermissionReq::PromQuery),
            (PermissionMode::WriteOnly, PermissionReq::PromStoreWrite),
        ] {
            let user = DefaultUserInfo::with_name_and_permission("test_user", permission);
            let direct = checker.check_permission(user.clone(), req.clone()).unwrap();
            let targeted = checker
                .check_permission_with_table_targets(user, req, resolved_targets("metrics"))
                .unwrap();

            assert_eq!(
                matches!(direct, PermissionResp::Allow),
                matches!(targeted, PermissionResp::Allow)
            );
        }
    }
}
