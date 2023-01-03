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

pub mod user_provider;

use std::sync::Arc;

use common_error::ext::BoxedError;
use common_error::prelude::ErrorExt;
use common_error::status_code::StatusCode;
use session::context::UserInfo;
use snafu::{Backtrace, ErrorCompat, OptionExt, Snafu};

use crate::auth::user_provider::StaticUserProvider;

#[async_trait::async_trait]
pub trait UserProvider: Send + Sync {
    fn name(&self) -> &str;

    async fn auth(&self, id: Identity<'_>, password: Password<'_>) -> Result<UserInfo>;
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

pub fn user_provider_from_option(opt: &String) -> Result<UserProviderRef> {
    let (name, content) = opt.split_once(':').context(InvalidConfigSnafu {
        value: opt.to_string(),
        msg: "UserProviderOption must be in format `<option>:<value>`",
    })?;
    match name {
        user_provider::STATIC_USER_PROVIDER => {
            let provider =
                StaticUserProvider::try_from(content).map(|p| Arc::new(p) as UserProviderRef)?;
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
    InvalidConfig { value: String, msg: String },

    #[snafu(display("IO error, source: {}", source))]
    Io {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Auth failed, source: {}", source))]
    AuthBackend {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("User not found, username: {}", username))]
    UserNotFound { username: String },

    #[snafu(display("Unsupported password type: {}", password_type))]
    UnsupportedPasswordType { password_type: String },

    #[snafu(display("Username and password does not match, username: {}", username))]
    UserPasswordMismatch { username: String },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidConfig { .. } => StatusCode::InvalidArguments,
            Error::Io { .. } => StatusCode::Internal,
            Error::AuthBackend { .. } => StatusCode::Internal,

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

pub type Result<T> = std::result::Result<T, Error>;

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
                                return Ok(UserInfo::new("greptime"));
                            } else {
                                return super::UserPasswordMismatchSnafu {
                                    username: username.to_string(),
                                }
                                .fail();
                            }
                        } else {
                            return super::UserNotFoundSnafu {
                                username: username.to_string(),
                            }
                            .fail();
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
        assert_eq!("greptime", auth_result.unwrap().username());

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
