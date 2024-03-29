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

#![feature(assert_matches)]
use std::assert_matches::assert_matches;
use std::sync::Arc;

use api::v1::greptime_request::Request;
use auth::error::Error::InternalState;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq, PermissionResp, UserInfoRef};
use sql::statements::show::{ShowDatabases, ShowKind};
use sql::statements::statement::Statement;

struct DummyPermissionChecker;

impl PermissionChecker for DummyPermissionChecker {
    fn check_permission(
        &self,
        _user_info: Option<UserInfoRef>,
        req: PermissionReq,
    ) -> auth::error::Result<PermissionResp> {
        match req {
            PermissionReq::GrpcRequest(_) => Ok(PermissionResp::Allow),
            PermissionReq::SqlStatement(_) => Ok(PermissionResp::Reject),
            _ => Err(InternalState {
                msg: "testing".to_string(),
            }),
        }
    }
}

#[test]
fn test_permission_checker() {
    let checker: PermissionCheckerRef = Arc::new(DummyPermissionChecker);

    let grpc_result = checker.check_permission(
        None,
        PermissionReq::GrpcRequest(&Request::Query(Default::default())),
    );
    assert_matches!(grpc_result, Ok(PermissionResp::Allow));

    let sql_result = checker.check_permission(
        None,
        PermissionReq::SqlStatement(&Statement::ShowDatabases(ShowDatabases::new(ShowKind::All))),
    );
    assert_matches!(sql_result, Ok(PermissionResp::Reject));

    let err_result = checker.check_permission(None, PermissionReq::Opentsdb);
    assert_matches!(err_result, Err(InternalState { msg }) if msg == "testing");
}
