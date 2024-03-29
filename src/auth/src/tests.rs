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

use secrecy::ExposeSecret;

use crate::error::{
    AccessDeniedSnafu, Result, UnsupportedPasswordTypeSnafu, UserNotFoundSnafu,
    UserPasswordMismatchSnafu,
};
use crate::user_info::DefaultUserInfo;
use crate::{auth_mysql, Identity, Password, UserInfoRef, UserProvider};

pub struct DatabaseAuthInfo<'a> {
    pub catalog: &'a str,
    pub schema: &'a str,
    pub username: &'a str,
}

pub struct MockUserProvider {
    pub catalog: String,
    pub schema: String,
    pub username: String,
}

impl Default for MockUserProvider {
    fn default() -> Self {
        MockUserProvider {
            catalog: "greptime".to_owned(),
            schema: "public".to_owned(),
            username: "greptime".to_owned(),
        }
    }
}

impl MockUserProvider {
    pub fn set_authorization_info(&mut self, info: DatabaseAuthInfo) {
        self.catalog = info.catalog.to_owned();
        self.schema = info.schema.to_owned();
        self.username = info.username.to_owned();
    }
}

#[async_trait::async_trait]
impl UserProvider for MockUserProvider {
    fn name(&self) -> &str {
        "mock_user_provider"
    }

    async fn authenticate(&self, id: Identity<'_>, password: Password<'_>) -> Result<UserInfoRef> {
        match id {
            Identity::UserId(username, _host) => match password {
                Password::PlainText(password) => {
                    if username == "greptime" {
                        if password.expose_secret() == "greptime" {
                            Ok(DefaultUserInfo::with_name("greptime"))
                        } else {
                            UserPasswordMismatchSnafu {
                                username: username.to_string(),
                            }
                            .fail()
                        }
                    } else {
                        UserNotFoundSnafu {
                            username: username.to_string(),
                        }
                        .fail()
                    }
                }
                Password::MysqlNativePassword(auth_data, salt) => {
                    auth_mysql(auth_data, salt, username, "greptime".as_bytes())
                        .map(|_| DefaultUserInfo::with_name(username))
                }
                _ => UnsupportedPasswordTypeSnafu {
                    password_type: "mysql_native_password",
                }
                .fail(),
            },
        }
    }

    async fn authorize(&self, catalog: &str, schema: &str, user_info: &UserInfoRef) -> Result<()> {
        if catalog == self.catalog && schema == self.schema && user_info.username() == self.username
        {
            Ok(())
        } else {
            AccessDeniedSnafu {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                username: user_info.username().to_string(),
            }
            .fail()
        }
    }
}

#[tokio::test]
async fn test_auth_by_plain_text() {
    use crate::error;

    let user_provider = MockUserProvider::default();
    assert_eq!("mock_user_provider", user_provider.name());

    // auth success
    let auth_result = user_provider
        .authenticate(
            Identity::UserId("greptime", None),
            Password::PlainText("greptime".to_string().into()),
        )
        .await
        .unwrap();
    assert_eq!("greptime", auth_result.username());

    // auth failed, unsupported password type
    let auth_result = user_provider
        .authenticate(
            Identity::UserId("greptime", None),
            Password::PgMD5(b"hashed_value", b"salt"),
        )
        .await;
    assert!(auth_result.is_err());
    assert!(matches!(
        auth_result.err().unwrap(),
        error::Error::UnsupportedPasswordType { .. }
    ));

    // auth failed, err: user not exist.
    let auth_result = user_provider
        .authenticate(
            Identity::UserId("not_exist_username", None),
            Password::PlainText("greptime".to_string().into()),
        )
        .await;
    assert!(auth_result.is_err());
    assert!(matches!(
        auth_result.err().unwrap(),
        error::Error::UserNotFound { .. }
    ));

    // auth failed, err: wrong password
    let auth_result = user_provider
        .authenticate(
            Identity::UserId("greptime", None),
            Password::PlainText("wrong_password".to_string().into()),
        )
        .await;
    assert!(auth_result.is_err());
    assert!(matches!(
        auth_result.err().unwrap(),
        error::Error::UserPasswordMismatch { .. }
    ))
}

#[tokio::test]
async fn test_schema_validate() {
    let mut validator = MockUserProvider::default();
    validator.set_authorization_info(DatabaseAuthInfo {
        catalog: "greptime",
        schema: "public",
        username: "test_user",
    });

    let right_user = DefaultUserInfo::with_name("test_user");
    let wrong_user = DefaultUserInfo::with_name("greptime");

    // check catalog
    let re = validator
        .authorize("greptime_wrong", "public", &right_user)
        .await;
    assert!(re.is_err());
    // check schema
    let re = validator
        .authorize("greptime", "public_wrong", &right_user)
        .await;
    assert!(re.is_err());
    // check username
    let re = validator.authorize("greptime", "public", &wrong_user).await;
    assert!(re.is_err());
    // check ok
    validator
        .authorize("greptime", "public", &right_user)
        .await
        .unwrap();
}
