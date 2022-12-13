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

pub mod user_provider;

pub const DEFAULT_USERNAME: &str = "greptime";

use std::sync::Arc;

use common_error::prelude::ErrorExt;
use common_error::status_code::StatusCode;
use snafu::{Backtrace, ErrorCompat, OptionExt, Snafu};

use crate::auth::user_provider::MemUserProvider;

#[async_trait::async_trait]
pub trait UserProvider: Send + Sync {
    fn name(&self) -> &str;

    async fn auth(&self, id: Identity<'_>, password: Password<'_>) -> Result<UserInfo, Error>;
}

pub type UserProviderRef = Arc<dyn UserProvider>;

type Username<'a> = &'a str;
type HostOrIp<'a> = &'a str;

#[derive(Debug, Clone)]
pub enum Identity<'a> {
    UserId(Username<'a>, Option<HostOrIp<'a>>),
}

pub type HashedPassword<'a> = &'a [u8];
pub type Salt<'a> = &'a [u8];

/// Authentication information sent by the client.
pub enum Password<'a> {
    PlainText(&'a str),
    MysqlNativePassword(HashedPassword<'a>, Salt<'a>),
    PgMD5(HashedPassword<'a>, Salt<'a>),
}

#[derive(Clone, Debug)]
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

pub fn user_provider_from_option(opt: &String) -> Result<UserProviderRef, Error> {
    let (name, content) = opt.split_once(':').context(InvalidConfigSnafu {
        value: opt.to_string(),
        msg: "UserProviderOption must be in format `<option>:<value>`",
    })?;
    match name {
        "mem_user_provider" => {
            let provider =
                MemUserProvider::try_from(content).map(|p| Arc::new(p) as UserProviderRef)?;
            Ok(provider)
        }
        _ => InvalidConfigSnafu {
            value: name.to_string(),
            msg: "Invalid UserProviderOption",
        }
        .fail(),
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid config value: {}, {}", value, msg))]
    InvalidConfig {
        value: String,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Encounter IO error, source: {}", source))]
    IOErr { source: std::io::Error },

    #[snafu(display("User not found"))]
    UserNotFound {},

    #[snafu(display("Unsupported password type: {}", password_type))]
    UnsupportedPasswordType {
        password_type: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Username and password does not match"))]
    UserPasswordMismatch {},
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidConfig { .. } => StatusCode::InvalidArguments,
            Error::IOErr { .. } => StatusCode::Internal,

            Error::UserNotFound { .. } => StatusCode::UserNotFound,
            Error::UnsupportedPasswordType { .. } => StatusCode::UnsupportedPasswordType,
            Error::UserPasswordMismatch { .. } => StatusCode::UserPasswordMismatch,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
pub mod test {
    use super::{Identity, Password, UserInfo, UserProvider};

    pub struct MockUserProvider {}

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
                    Password::PlainText(password) => {
                        if username == "greptime" {
                            if password == "greptime" {
                                return Ok(UserInfo {
                                    username: "greptime".to_string(),
                                });
                            } else {
                                return super::UserPasswordMismatchSnafu {}.fail();
                            }
                        } else {
                            return super::UserNotFoundSnafu {}.fail();
                        }
                    }
                    _ => super::UnsupportedPasswordTypeSnafu {
                        password_type: "mysql_native_password",
                    }
                    .fail(),
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test::MockUserProvider;
    use super::{Identity, Password, UserProvider};
    use crate::auth;

    #[tokio::test]
    async fn test_auth_by_plain_text() {
        let user_provider = MockUserProvider {};
        assert_eq!("mock_user_provider", user_provider.name());

        // auth success
        let auth_result = user_provider
            .auth(
                Identity::UserId("greptime", None),
                Password::PlainText("greptime"),
            )
            .await;
        assert!(auth_result.is_ok());
        assert_eq!("greptime", auth_result.unwrap().user_name());

        // auth failed, unsupported password type
        let auth_result = user_provider
            .auth(
                Identity::UserId("greptime", None),
                Password::MysqlNativePassword(b"hashed_value", b"salt"),
            )
            .await;
        assert!(auth_result.is_err());
        matches!(
            auth_result.err().unwrap(),
            auth::Error::UnsupportedPasswordType { .. }
        );

        // auth failed, err: user not exist.
        let auth_result = user_provider
            .auth(
                Identity::UserId("not_exist_username", None),
                Password::PlainText("greptime"),
            )
            .await;
        assert!(auth_result.is_err());
        matches!(auth_result.err().unwrap(), auth::Error::UserNotFound { .. });

        // auth failed, err: wrong password
        let auth_result = user_provider
            .auth(
                Identity::UserId("greptime", None),
                Password::PlainText("wrong_password"),
            )
            .await;
        assert!(auth_result.is_err());
        matches!(
            auth_result.err().unwrap(),
            auth::Error::UserPasswordMismatch { .. }
        );
    }
}
