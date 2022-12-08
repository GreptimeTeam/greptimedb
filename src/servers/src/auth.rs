// Copyright 2022 Greptime Team
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

pub const DEFAULT_USERNAME: &str = "greptime";

use std::sync::Arc;

use common_error::prelude::ErrorExt;
use common_error::status_code::StatusCode;
use snafu::{Backtrace, ErrorCompat, Snafu};

#[async_trait::async_trait]
pub trait UserProvider: Send + Sync {
    fn name(&self) -> &str;

    async fn auth(&self, id: Identity<'_>, pwd: Password<'_>) -> Result<UserInfo, Error>;
}

pub type UserProviderRef = Arc<dyn UserProvider>;

type Username<'a> = &'a str;
type HostOrIp<'a> = &'a str;

#[derive(Debug, Clone)]
pub enum Identity<'a> {
    UserId(Username<'a>, Option<HostOrIp<'a>>),
}

pub type HashedPwd<'a> = &'a [u8];
pub type Salt<'a> = &'a [u8];
pub type Pwd<'a> = &'a [u8];

/// Authentication information sent by the client.
pub enum Password<'a> {
    PlainText(Pwd<'a>),
    MysqlNativePwd(HashedPwd<'a>, Salt<'a>),
    PgMD5(HashedPwd<'a>, Salt<'a>),
}

pub struct UserInfo {
    username: String,
}

impl Default for UserInfo {
    fn default() -> Self {
        Self {
            username: DEFAULT_USERNAME.to_string(),
        }
    }
}

impl UserInfo {
    pub fn user_name(&self) -> &str {
        &self.username
    }

    #[cfg(test)]
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("User not exist"))]
    UserNotExist { backtrace: Backtrace },

    #[snafu(display("Unsupported Password Type: {}", pwd_type))]
    UnsupportedPwdType {
        pwd_type: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Username and password does not match"))]
    WrongPwd { backtrace: Backtrace },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::UserNotExist { .. } => StatusCode::UserNotFound,
            Error::UnsupportedPwdType { .. } => StatusCode::UnsupportedPwdType,
            Error::WrongPwd { .. } => StatusCode::UserPwdMismatch,
        }
    }

    fn backtrace_opt(&self) -> Option<&common_error::snafu::Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{Identity, Password, UserInfo, UserProvider};
    use crate::auth;

    struct MockUserProvider {}

    #[async_trait::async_trait]
    impl UserProvider for MockUserProvider {
        fn name(&self) -> &str {
            "mock_user_provider"
        }

        async fn auth(
            &self,
            id: Identity<'_>,
            password: Password<'_>,
        ) -> Result<UserInfo, super::Error> {
            match id {
                Identity::UserId(username, _host) => match password {
                    Password::PlainText(pwd) => {
                        if username == "greptime" {
                            if pwd == b"greptime" {
                                return Ok(UserInfo {
                                    username: "greptime".to_string(),
                                });
                            } else {
                                return super::WrongPwdSnafu {}.fail();
                            }
                        } else {
                            return super::UserNotExistSnafu {}.fail();
                        }
                    }
                    _ => super::UnsupportedPwdTypeSnafu {
                        pwd_type: "mysql_native_pwd",
                    }
                    .fail(),
                },
            }
        }
    }

    #[tokio::test]
    async fn test_auth_by_plain_text() {
        let user_provider = MockUserProvider {};
        assert_eq!("mock_user_provider", user_provider.name());

        // auth success
        let auth_result = user_provider
            .auth(
                Identity::UserId("greptime", None),
                Password::PlainText(b"greptime"),
            )
            .await;
        assert!(auth_result.is_ok());
        assert_eq!("greptime", auth_result.unwrap().user_name());

        // auth failed, unsupported pwd type
        let auth_result = user_provider
            .auth(
                Identity::UserId("greptime", None),
                Password::MysqlNativePwd(b"hashed_value", b"salt"),
            )
            .await;
        assert!(auth_result.is_err());
        matches!(
            auth_result.err().unwrap(),
            auth::Error::UnsupportedPwdType { .. }
        );

        // auth failed, err: user not exist.
        let auth_result = user_provider
            .auth(
                Identity::UserId("not_exist_username", None),
                Password::PlainText(b"greptime"),
            )
            .await;
        assert!(auth_result.is_err());
        matches!(auth_result.err().unwrap(), auth::Error::UserNotExist { .. });

        // auth failed, err: wrong password
        let auth_result = user_provider
            .auth(
                Identity::UserId("greptime", None),
                Password::PlainText(b"wrong_pwd"),
            )
            .await;
        assert!(auth_result.is_err());
        matches!(auth_result.err().unwrap(), auth::Error::WrongPwd { .. });
    }
}
