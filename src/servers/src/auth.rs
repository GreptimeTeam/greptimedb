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

use std::sync::Arc;

use common_error::ext::BoxedError;
use common_error::prelude::ErrorExt;
use common_error::status_code::StatusCode;
use session::context::UserInfo;
use snafu::{Backtrace, ErrorCompat, OptionExt, Snafu};

use crate::auth::user_provider::StaticUserProvider;

pub mod user_provider;

#[async_trait::async_trait]
pub trait UserProvider: Send + Sync {
    fn name(&self) -> &str;

    /// [`authenticate`] checks whether a user is valid and allowed to access the database.
    async fn authenticate(&self, id: Identity<'_>, password: Password<'_>) -> Result<UserInfo>;

    /// [`authorize`] checks whether a connection request
    /// from a certain user to a certain catalog/schema is legal.
    /// This method should be called after [`authenticate`].
    async fn authorize(&self, catalog: &str, schema: &str, user_info: &UserInfo) -> Result<()>;
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

    #[snafu(display("Illegal runtime param: {}", msg))]
    IllegalParam { msg: String },

    #[snafu(display("Internal state error: {}", msg))]
    InternalState { msg: String },

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
