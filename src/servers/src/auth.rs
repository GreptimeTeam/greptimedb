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

use std::ops::Deref;
use std::sync::Arc;

/// Types that can get [ UserInfo ].
#[async_trait::async_trait]
pub trait UserProvider: Send + Sync {
    fn name(&self) -> &str;

    async fn user_info(
        &self,
        id: Identity<'_>,
    ) -> Result<UserInfo, Box<dyn std::error::Error + Send + Sync>>;
}

pub type UserProviderRef = Arc<dyn UserProvider>;

type Username<'a> = &'a str;
type HostOrIp<'a> = &'a str;

#[derive(Debug, Clone)]
pub enum Identity<'a> {
    UserId(Username<'a>, Option<HostOrIp<'a>>),
}

pub struct UserInfo {
    username: String,
    auth_method: Box<dyn Authenticator>,
}

impl UserInfo {
    pub fn user_name(&self) -> &str {
        &self.username
    }

    pub fn auth_method(&self) -> &dyn Authenticator {
        self.auth_method.deref()
    }
}

/// Types that can verify whether the password is correct.
pub trait Authenticator: Send + Sync {
    fn auth(&self, password: Password) -> bool;
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

#[cfg(test)]
mod tests {
    use super::{Authenticator, Identity, Password, UserInfo, UserProvider};

    struct MockUserProvider {}

    #[async_trait::async_trait]
    impl UserProvider for MockUserProvider {
        fn name(&self) -> &str {
            "mock_user_provider"
        }

        async fn user_info(
            &self,
            id: Identity<'_>,
        ) -> Result<UserInfo, Box<dyn std::error::Error + Send + Sync>> {
            match id {
                Identity::UserId(username, _host) => {
                    if username == "greptime" {
                        return Ok(UserInfo {
                            username: "greptime".to_string(),
                            auth_method: Box::new(MockAuthMethod {
                                plain_text: "greptime".to_string(),
                            }),
                        });
                    }
                    todo!()
                }
            }
        }
    }

    struct MockAuthMethod {
        plain_text: String,
    }

    impl Authenticator for MockAuthMethod {
        fn auth(&self, password: Password) -> bool {
            match password {
                Password::PlainText(pwd) => pwd == self.plain_text.as_bytes(),
                Password::MysqlNativePwd(_, _) => unimplemented!(),
                Password::PgMD5(_, _) => unimplemented!(),
            }
        }
    }

    #[tokio::test]
    async fn test_auth_by_plain_text() {
        let username = "greptime";
        let pwd = b"greptime";
        let wrong_pwd = b"wrong";

        let user_provider = MockUserProvider {};

        assert_eq!("mock_user_provider", user_provider.name());

        let user_info = user_provider
            .user_info(Identity::UserId(username, None))
            .await
            .unwrap();

        assert_eq!(username, user_info.user_name());

        let auth_method = user_info.auth_method();
        assert!(auth_method.auth(Password::PlainText(pwd)));
        assert!(!auth_method.auth(Password::PlainText(wrong_pwd)));
    }
}
