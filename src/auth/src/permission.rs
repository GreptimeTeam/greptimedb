// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;

use api::v1::greptime_request::Request;
use sql::statements::statement::Statement;

use crate::error::Result;
use crate::{PermissionCheckerRef, UserInfoRef};

#[derive(Debug, Clone)]
pub enum PermissionReq<'a> {
    GrpcRequest(Box<&'a Request>),
    SqlStatement(Box<&'a Statement>),
}

pub trait PermissionChecker: Send + Sync {
    fn check_permission(&self, user_info: Option<UserInfoRef>, req: PermissionReq) -> Result<bool>;
}

impl PermissionChecker for Option<&PermissionCheckerRef> {
    fn check_permission(&self, user_info: Option<UserInfoRef>, req: PermissionReq) -> Result<bool> {
        match self {
            Some(checker) => checker.check_permission(user_info, req),
            None => Ok(true),
        }
    }
}
